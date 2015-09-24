#define _GNU_SOURCE

#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <sys/syscall.h>
#include <dlfcn.h>
#include <pthread.h>
#include <signal.h>
#include <sys/time.h>
#include <netinet/in.h>

#ifdef HAS_PAPI
#include <papi.h>
#endif

#include "interpose.h"
#include "interpose_monitoring.h"

static int monitor_running;
static pthread_t monitor_thread;
static pthread_mutex_t monitor_mutex;
static pthread_cond_t monitor_cv;

typedef struct {
    double real_utime;
    double real_stime;
    double real_iowait;
} CpuUtilInfo;

typedef struct {
    unsigned long long vmSize;
    unsigned long long vmRSS;
    unsigned long threads;
} MemUtilInfo;

typedef struct {
    unsigned long long rchar;
    unsigned long long wchar;
    unsigned long syscr;
    unsigned long syscw;
    unsigned long long read_bytes;
    unsigned long long write_bytes;
    unsigned long long cancelled_write_bytes;
} IoUtilInfo;

static int startswith(const char *line, const char *tok) {
    return strstr(line, tok) == line;
}

/* SOCKET-BASED COMMUNICATION WITH KICKSTART */

static int prepare_socket(int *sockfd, char *monitoring_socket_host, char* monitoring_socket_port, struct sockaddr_in *serv_addr) {
    int port_no = atoi(monitoring_socket_port);
    struct sockaddr_in sa_addr;
    struct hostent *server;

    if ((*sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        printerr("Error[getaddrinfo]: %s\n", strerror(errno));
        return -1;
    }

    server = gethostbyname(monitoring_socket_host);
    if (server == NULL) {
        printerr("Error[gethostbyname]: no such host - %s\n", strerror(errno));
        return -1;
    }

    bzero((char *) &sa_addr, sizeof(sa_addr));
    sa_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *)&sa_addr.sin_addr.s_addr, server->h_length);
    sa_addr.sin_port = htons(port_no);

    if (connect(*sockfd, (struct sockaddr *)&sa_addr, sizeof(sa_addr)) < 0) {
        printerr("Error[connect]: %s\n", strerror(errno));
        return -1;
    }

    return 0;
}

static int send_msg_to_kickstart(char *msg, char *host, char *port) {
    int sockfd;
    struct sockaddr_in serv_addr;

    if (prepare_socket(&sockfd, host, port, &serv_addr) == -1) {
        printerr("Some error occured during socket preparation.\n");
        return 1;
    }

    if (send(sockfd, msg, BUFSIZ, 0) < 0) {
        printerr("Error during msg send.\n");
        return 1;
    }

    close(sockfd);
    return 0;
}

/* END SOCKET-BASED COMMUNICATION WITH KICKSTART */

static int set_monitoring_params(int *mpi_rank, int *interval, char **socket_host, char **socket_port, 
    char **kickstart_pid, char *hostname, char **job_id) 
{
    char *envptr = NULL;

    envptr = getenv("OMPI_COMM_WORLD_RANK");
    if (envptr == NULL) {
        envptr = getenv("ALPS_APP_PE");
        if (envptr == NULL) {
            envptr = getenv("PMI_RANK");
            if (envptr == NULL) {
                envptr = getenv("PMI_ID");
                if (envptr == NULL) {
                    envptr = getenv("MPIRUN_RANK");
                }
            }
        }
    }
    if (envptr != NULL) {
        *mpi_rank = atoi(envptr) + 1;
    }

    if ((envptr = getenv("KICKSTART_MON_INTERVAL")) == NULL) {
        printerr("[Thread-%d] Couldn't read KICKSTART_MON_INTERVAL\n", *mpi_rank);
        return 1;
    }
    *interval = atoi(envptr);

    if ((*socket_host = getenv("KICKSTART_MON_HOST")) == NULL) {
        printerr("[Thread-%d] Couldn't read KICKSTART_MON_HOST\n", *mpi_rank);
        return 1;
    }

    if ((*socket_port = getenv("KICKSTART_MON_PORT")) == NULL) {
        printerr("[Thread-%d] Couldn't read KICKSTART_MON_PORT\n", *mpi_rank);
        return 1;
    }

    if ((*kickstart_pid = getenv("KICKSTART_MON_PID")) == NULL) {
        printerr("KICKSTART_MON_PID not set in environment\n");
        return 1;
    }

    if (gethostname(hostname, BUFSIZ)) {
        printerr("[Thread-%d] ERROR: couldn't get hostname: %s\n", *mpi_rank, strerror(errno));
        return 1;
    }

    // we don't really care if this is NULL or not
    *job_id = getenv("CONDOR_JOBID");

    return 0;
}

/* READING PERFORMANCE METRICS FUNCTIONS */

/* Read /proc/self/stat to get CPU usage and returns a structure with this information */
static void read_cpu_status(CpuUtilInfo *info) {
    char statf[] = "/proc/self/stat";

    /* If the stat file is missing, then just skip it */
    if (access(statf, F_OK) < 0) {
        return;
    }

    FILE *f = _interpose_fopen_untraced(statf,"r");
    if (f == NULL) {
        perror("libinterpose: Unable to fopen /proc/self/stat");
        return;
    }

    unsigned long utime, stime = 0;
    unsigned long long iowait = 0; //delayacct_blkio_ticks

    //pid comm state ppid pgrp session tty_nr tpgid flags minflt cminflt majflt
    //cmajflt utime stime cutime cstime priority nice num_threads itrealvalue
    //starttime vsize rss rsslim startcode endcode startstack kstkesp kstkeip
    //signal blocked sigignore sigcatch wchan nswap cnswap exit_signal
    //processor rt_priority policy delayacct_blkio_ticks guest_time cguest_time
    fscanf(f, "%*d %*s %*c %*d %*d %*d %*d %*d %*u %*u %*u %*u %*u %lu "
              "%lu %*d %*d %*d %*d %*d %*d %*u %*u %*d %*u %*u "
              "%*u %*u %*u %*u %*u %*u %*u %*u %*u %*u %*u %*d "
              "%*d %*u %*u %llu %*u %*d",
           &utime, &stime, &iowait);

    _interpose_fclose_untraced(f);

    /* Adjust by number of clock ticks per second */
    long clocks = sysconf(_SC_CLK_TCK);

    double real_utime;
    double real_stime;
    double real_iowait;
    real_utime = ((double)utime) / clocks;
    real_stime = ((double)stime) / clocks;
    real_iowait = ((double)iowait) / clocks;

    info->real_utime = real_utime;
    info->real_stime = real_stime;
    info->real_iowait = real_iowait;
}

/* Read useful information from /proc/self/status and returns a structure with this information */
static void read_mem_status(MemUtilInfo *info) {
    char statf[] = "/proc/self/status";

    /* If the status file is missing, then just skip it */
    if (access(statf, F_OK) < 0) {
        return;
    }

    FILE *f = _interpose_fopen_untraced(statf, "r");
    if (f == NULL) {
        perror("libinterpose: Unable to fopen /proc/self/status");
        return;
    }

    char line[BUFSIZ];

    while (_interpose_fgets_untraced(line, BUFSIZ, f) != NULL) {

        if (startswith(line,"VmSize")) {
            sscanf(line, "VmSize: %llu", &(info->vmSize));
        } 
        else if (startswith(line,"VmRSS")) {
            sscanf(line, "VmRSS: %llu", &(info->vmRSS));
        } 
        else if (startswith(line,"Threads")) {
            sscanf(line, "Threads: %lu", &(info->threads));
        }

    }

    _interpose_fclose_untraced(f);
}

/* Read /proc/self/io to get I/O usage */
static void read_io_status(IoUtilInfo *info) {
    char iofile[] = "/proc/self/io";

    /* This proc file was added in Linux 2.6.20. It won't be
     * there on older kernels, or on kernels without task IO
     * accounting. If it is missing, just bail out.
     */
    if (access(iofile, F_OK) < 0) {
        return;
    }

    FILE *f = _interpose_fopen_untraced(iofile, "r");
    if (f == NULL) {
        perror("libinterpose: Unable to fopen /proc/self/io");
        return;
    }

    char line[BUFSIZ];
    while (_interpose_fgets_untraced(line, BUFSIZ, f) != NULL) {
        if (startswith(line, "rchar")) {
            sscanf(line, "rchar: %llu", &(info->rchar));
        } else if (startswith(line, "wchar")) {
            sscanf(line, "wchar: %llu", &(info->wchar));
        } else if (startswith(line,"syscr")) {
            sscanf(line, "syscr: %lu", &(info->syscr));
        } else if (startswith(line,"syscw")) {
            sscanf(line, "syscw: %lu", &(info->syscw));
        } else if (startswith(line,"read_bytes")) {
            sscanf(line, "read_bytes: %llu", &(info->read_bytes));
        } else if (startswith(line,"write_bytes")) {
            sscanf(line, "write_bytes: %llu", &(info->write_bytes));
        } else if (startswith(line,"cancelled_write_bytes")) {
            sscanf(line, "cancelled_write_bytes: %llu", &(info->cancelled_write_bytes));
        }
    }

    _interpose_fclose_untraced(f);
}

#ifdef HAS_PAPI

/* Read eventset, return 1 on failure, 0 on success */
static int read_hardware_counters(int eventset, int *shared_nevents, int *shared_events, long long *shared_counters) {
    int i, rc;
    int nevents = n_papi_events;
    int events[n_papi_events];
    long long counters[n_papi_events];

    /* Get the events that were actually recorded */
    rc = PAPI_list_events(eventset, events, &nevents);
    if (rc != PAPI_OK) {
        if (rc != PAPI_ENOEVST) {
            printerr("ERROR: PAPI_list_events failed: %s\n", PAPI_strerror(rc));
        }
        return 1;
    }

    /* Initialize shared data structures if this is first read, otherwise
     * verify that they match the shared data structures */
    if (*shared_nevents <= 0) {
        *shared_nevents = nevents;
        for (i = 0; i < nevents; i++) {
            shared_events[i] = events[i];
        }
    } else {
        if (*shared_nevents != nevents) {
            printerr("ERROR: event set %d had a different number of events\n", eventset);
            return 1;
        }
        for (i = 0; i < nevents; i++) {
            if (shared_events[i] != events[i]) {
                printerr("ERROR: the PAPI event in position %d is different than the shared event\n", i);
                return 1;
            }
        }
    }

    /* read and aggregate the counters */
    rc = PAPI_read(eventset, counters);
    if (rc != PAPI_OK) {
        printerr("ERROR: No hardware counters or PAPI not supported: %s\n", PAPI_strerror(rc));
        return 1;
    }
    for (i = 0; i < nevents; i++) {
        /* We are summing up counters from different threads (eventsets) */
        shared_counters[i] = shared_counters[i] + counters[i];
    }

    return 0;
}

#endif

/* END READING PERFORMANCE METRICS FUNCTIONS */

void* _interpose_monitoring_thread_func(void* arg) {
    pthread_mutex_lock(&monitor_mutex);

    int mpi_rank = 0, interval = 60;
    time_t timestamp;
    char exec_name[BUFSIZ] = "", *kickstart_pid = NULL, hostname[BUFSIZ] = "", *job_id = NULL, msg[BUFSIZ] = "";
    char *monitoring_socket_host = NULL, *monitoring_socket_port = NULL;
    CpuUtilInfo cpu_info = { 0.0, 0.0, 0.0 };
    MemUtilInfo mem_info = { 0, 0, 0 };
    IoUtilInfo io_info = { 0, 0, 0, 0, 0, 0, 0 };

    if (set_monitoring_params(&mpi_rank,  &interval,
        &monitoring_socket_host, &monitoring_socket_port,
        &kickstart_pid, hostname, &job_id) ) {
        printerr("Unable to configure monitoring thread\n");
        goto exit;
    }

    while (monitor_running) {

        struct timeval now;
        struct timespec timeout;
        gettimeofday(&now, NULL);
        timeout.tv_sec = now.tv_sec + interval;
        timeout.tv_nsec = now.tv_usec * 1000UL;
        pthread_cond_timedwait(&monitor_cv, &monitor_mutex, &timeout);

        if (!monitor_running) {
            break;
        }

        timestamp = time(NULL);
        read_cpu_status(&cpu_info);
        read_mem_status(&mem_info);
        read_io_status(&io_info);
        _interpose_read_exe(exec_name, BUFSIZ);

        char counters_str[BUFSIZ] = "";
#ifdef HAS_PAPI
        long long shared_counters[n_papi_events] = { 0 };
        int shared_nevents = 0, shared_events[n_papi_events];

        int k = 1;
        while (read_hardware_counters(k, &shared_nevents, shared_events, shared_counters) == 0) {
            k++;
        }

        memset(counters_str, 0, sizeof(counters_str));
        for (int i = 0; i < shared_nevents; i++) {
            char eventname[256];
            int rc = PAPI_event_code_to_name(shared_events[i], eventname);
            if (rc != PAPI_OK) {
                printerr("ERROR: Could not get event name: %s\n", PAPI_strerror(rc));
                break;
            }
            char counter_str[256];
            sprintf(counter_str, "%s=%lld ", eventname, shared_counters[i]);
            strcat(counters_str, counter_str);
        }
#endif

        memset(msg, 0, sizeof(msg));
        sprintf(msg, "ts=%d event=workflow_trace level=INFO status=0 "
                     "job_id=%s kickstart_pid=%s executable=%s hostname=%s mpi_rank=%d utime=%.3f stime=%.3f "
                     "iowait=%.3f vmSize=%llu vmRSS=%llu threads=%lu read_bytes=%llu write_bytes=%llu "
                     "syscr=%lu syscw=%lu %s\n",
                     (int)timestamp, job_id, kickstart_pid, exec_name, hostname, mpi_rank,
                     cpu_info.real_utime, cpu_info.real_stime, cpu_info.real_iowait,
                     mem_info.vmSize, mem_info.vmRSS, mem_info.threads,
                     io_info.rchar, io_info.wchar, io_info.syscr, io_info.syscw, counters_str);
        if (send_msg_to_kickstart(msg, monitoring_socket_host, monitoring_socket_port)) {
            printerr("[Thread-%d] There was a problem sending a message to kickstart...\n", mpi_rank);
        }
    }

exit:
    pthread_mutex_unlock(&monitor_mutex);

    return NULL;
}

void _interpose_spawn_monitoring_thread() {
    /* Only do this if monitoring is enabled */
    if (getenv("KICKSTART_MON") == NULL) {
        return;
    }
    pthread_mutex_init(&monitor_mutex, NULL);
    pthread_cond_init(&monitor_cv, NULL);
    monitor_running = 1;

    typeof(pthread_create) *orig_pthread_create = dlsym(RTLD_NEXT, "pthread_create");
    int rc = (*orig_pthread_create)(&monitor_thread, NULL, _interpose_monitoring_thread_func, NULL);
    if (rc) {
        printerr("ERROR: could not spawn the monitoring thread; return code from pthread_create() is %d\n", rc);
        return;
    }
}

void _interpose_stop_monitoring_thread() {
    /* Only do this if monitoring is enabled */
    if (getenv("KICKSTART_MON") == NULL) {
        return;
    }

    /* Signal the monitoring thread to shutdown */
    pthread_mutex_lock(&monitor_mutex);
    monitor_running = 0;
    pthread_cond_signal(&monitor_cv);
    pthread_mutex_unlock(&monitor_mutex);

    /* Wait for the monitoring thread */
    pthread_join(monitor_thread, NULL);

    pthread_cond_destroy(&monitor_cv);
    pthread_mutex_destroy(&monitor_mutex);
}

