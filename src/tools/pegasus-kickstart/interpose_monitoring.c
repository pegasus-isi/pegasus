#include "interpose_monitoring.h"

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

#ifdef HAS_PAPI
#include <papi.h>
#endif

static int myerr = STDERR_FILENO;

#define printerr(fmt, ...) \
    dprintf(myerr, "libinterpose[%d]: %s[%d]: " fmt, \
            getpid(), __FILE__, __LINE__, ##__VA_ARGS__)

#ifdef DEBUG
#define debug(format, args...) \
    dprintf(myerr, "libinterpose: " format "\n" , ##args)
#else
#define debug(format, args...)
#endif

// SOCKET-BASED COMMUNICATION WITH KICKSTART

int prepare_socket(int *sockfd, char *monitoring_socket_host, char* monitoring_socket_port, struct sockaddr_in *serv_addr) {
    int port_no = atoi(monitoring_socket_port);
    struct sockaddr_in sa_addr;
    struct hostent *server;

    if( (*sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0 ) {
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
    bcopy( (char *)server->h_addr, (char *)&sa_addr.sin_addr.s_addr, server->h_length);
    sa_addr.sin_port = htons(port_no);

    // printerr("haddr: %s\n", inet_ntoa( *( struct in_addr*)( server -> h_addr_list[0])));
    // printerr("port: %d\n", port_no);
    
    if( connect(*sockfd, (struct sockaddr *)&sa_addr, sizeof(sa_addr)) < 0 ) {
        printerr("Error[connect]: %s\n", strerror(errno));
        return -1;
    }

    return 0;
}

int send_msg_to_kickstart(char *msg, char *host, char *port) {
    int sockfd;
    struct sockaddr_in serv_addr;

    if( prepare_socket(&sockfd, host, port, &serv_addr) == -1 ) {
        printerr("Some error occured during socket preparation.\n");
        return 1;
    }
    else {
        // printerr("We are going to write to a socket...\n");        

        if( send(sockfd, msg, BUFSIZ, 0) < 0 ) {
            printerr("Error during msg send.\n");
            return 1;
        }
    }

    close(sockfd);
    return 0;
}

// END SOCKET-BASED COMMUNICATION WITH KICKSTART

// TODO kickstart status file should be used when socket-based communication fails
// Utility function to open the kickstart status file based on environment variable
// static FILE* open_kickstart_status_file() {
//     char *kickstart_status = getenv("KICKSTART_MON_FILE");

//     if (kickstart_status == NULL) {
//         printerr("Unable to open kickstart status file: KICKSTART_MON_FILE not set in environment\n");
//         return NULL;
//     }

//     return fopen(kickstart_status, "a");
// }

int set_monitoring_params(int mpi_rank, int *interval, char **socket_host, char **socket_port, 
    char **kickstart_pid, char *hostname, char **job_id) 
{
    char *envptr = NULL;

    if ( (envptr = getenv("KICKSTART_MON_INTERVAL")) == NULL ) {
        printerr("[Thread-%d] Couldn't read KICKSTART_MON_INTERVAL\n", mpi_rank);
        return 1;        
    }
    else {
        *interval = atoi(envptr);
    }

    if( (*socket_host = getenv("KICKSTART_MON_HOST")) == NULL) {
        printerr("[Thread-%d] Couldn't read KICKSTART_MON_HOST\n", mpi_rank);
        return 1;
    }

    if( (*socket_port = getenv("KICKSTART_MON_PORT")) == NULL ) {
        printerr("[Thread-%d] Couldn't read KICKSTART_MON_PORT\n", mpi_rank);
        return 1;
    } 

    if ( (*kickstart_pid = getenv("KICKSTART_MON_PID")) == NULL) {
        printerr("KICKSTART_MON_PID not set in environment\n");
        return 1;
    }

    if( gethostname(hostname, BUFSIZ) ) {
        printerr("[Thread-%d] ERROR: couldn't get hostname: %s\n", mpi_rank, strerror(errno));
        return 1;
    }

    // we don't really care if this is NULL or not
    *job_id = getenv("CONDOR_JOBID");

    return 0;
}

/* Read /proc/self/stat to get CPU usage and returns a structure with this information */
void read_cpu_status(CpuUtilInfo *info) {
    debug("Reading stat file");

    char statf[] = "/proc/self/stat";

    /* If the stat file is missing, then just skip it */
    if (access(statf, F_OK) < 0) {
        return;
    }

    FILE *f = fopen_untraced(statf,"r");
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

    fclose_untraced(f);

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
void read_mem_status(MemUtilInfo *info) {
    debug("Reading status file");    
    char statf[] = "/proc/self/status";

    /* If the status file is missing, then just skip it */
    if (access(statf, F_OK) < 0) {
        return;
    }

    FILE *f = fopen_untraced(statf, "r");
    if (f == NULL) {
        perror("libinterpose: Unable to fopen /proc/self/status");
        return;
    }

    char line[BUFSIZ];
    
    while (fgets_untraced(line, BUFSIZ, f) != NULL) {

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

    fclose_untraced(f);
}

/* Read /proc/self/io to get I/O usage */
void read_io_status(IoUtilInfo *info) {
    debug("Reading io file");

    pthread_mutex_lock(&io_mut);
    info->rchar = io_util_info.rchar;
    info->wchar = io_util_info.wchar;
    info->syscw = io_util_info.syscw;
    info->syscr = io_util_info.syscr;
    pthread_mutex_unlock(&io_mut);

    char iofile[] = "/proc/self/io";

    /* This proc file was added in Linux 2.6.20. It won't be
     * there on older kernels, or on kernels without task IO
     * accounting. If it is missing, just bail out.
     */
    if (access(iofile, F_OK) < 0) {
        return;
    }

    FILE *f = fopen_untraced(iofile, "r");
    if (f == NULL) {
        perror("libinterpose: Unable to fopen /proc/self/io");
        return;
    }

    char line[BUFSIZ];
//    printerr("Reading io status...\n");
    while (fgets_untraced(line, BUFSIZ, f) != NULL) {
//        printerr("Our line: '%s'\n", line);
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

    fclose_untraced(f);
}


void spawn_monitoring_thread() {

    // spawning a timer thread only when
    char* mpi_rank = getenv("OMPI_COMM_WORLD_RANK");
    // printerr("Spawning thread in process: %d\n", (int)current_pid);
     // printerr("Setting mpi rank based on OMPI_COMM_WORLD_RANK\n");

    if(mpi_rank == NULL) {
        mpi_rank = getenv("ALPS_APP_PE");        
         // printerr("Setting mpi rank based on MPIRUN_RANK\n");

        if(mpi_rank == NULL) {
            mpi_rank = getenv("PMI_RANK");
             // printerr("Setting mpi rank based on PMI_RANK\n");

            if(mpi_rank == NULL) {
                mpi_rank = getenv("PMI_ID");
                 // printerr("Setting mpi rank based on PMI_ID\n");

                if(mpi_rank == NULL) {
                    mpi_rank = getenv("MPIRUN_RANK");
                     // printerr("Setting mpi rank based on MPIRUN_RANK\n");
                }
            }
        }
    }

    if(mpi_rank == NULL) {
        // printerr("MPI rank is not set in environment\n");
        mpi_rank = (char*) calloc(1024, sizeof(char));
        strcpy(mpi_rank, "-1");
    }

    int rc = pthread_create(&timer_thread, NULL, monitoring_thread_func, (void *)mpi_rank);
    if (rc) {
        printerr("ERROR; return code from pthread_create() is %d\n", rc);
        exit(-1);
    }    
}

/*
 * It is a timer thread function, which dumps monitoring information to a global trace file 
 * - a full path is stored in KICKSTART_PREFIX
 * - it stores a single entry each time which follows the pattern:
 * <mpi_rank> <timestamp> <utime> <stime> <io_wait> <vm_peak> <pm_peak> <threads> <read_bytes> <write_bytes> <syscr> <syscw>
 */
void* monitoring_thread_func(void* mpi_rank_void) {
    // TODO kickstart status file should be used when socket-based communication fails
    // FILE* kickstart_status;
    time_t timestamp;
    int interval;
    int mpi_rank = atoi( (char*) mpi_rank_void ) + 1;
    char exec_name[BUFSIZ], *kickstart_pid = NULL, hostname[BUFSIZ], *job_id = NULL, msg[BUFSIZ];
    char *monitoring_socket_host = NULL, *monitoring_socket_port = NULL;
    CpuUtilInfo cpu_info = { 0.0, 0.0 };
    MemUtilInfo mem_info = { 0, 0, 0 };
    IoUtilInfo io_info = { 0, 0, 0, 0, 0, 0, 0 };

    printerr("[Thread-%d] starting a thread...\n", mpi_rank);

    if( set_monitoring_params(mpi_rank,  &interval, 
        &monitoring_socket_host, &monitoring_socket_port,
        &kickstart_pid, hostname, &job_id) ) 
    {
        return NULL;
    }

    while(library_loaded) {
        char counters_str[BUFSIZ] = "", eventname[256], counter_str[256];

        #ifdef HAS_PAPI

        long long shared_counters[papi_max_events] = { 0 };
        int shared_nevents = 0, rc, shared_events[papi_max_events];

        #endif

        timestamp = time(NULL);

        read_cpu_status(&cpu_info);
        read_mem_status(&mem_info);
        read_io_status(&io_info);
        read_exe(exec_name);

        #ifdef HAS_PAPI

        printerr("[Thread-%d] Reading all possible hardware counters...\n", mpi_rank);

        int k = 1;
        while( read_hardware_counters(k, &shared_nevents, shared_events, shared_counters) == 0 ) { 
            k++;
        }

        if(shared_nevents > 0) {
            // printerr("[Thread-%d] Concatenating hardware counters into string from %d threads...\n", mpi_rank, k - 1);
            for(int i = 0; i < shared_nevents; i++) {
                rc = PAPI_event_code_to_name(shared_events[i], eventname);

                if(rc != PAPI_OK) {
                    printerr("ERROR: Could not get event name: %s\n", PAPI_strerror(rc));
                    break;
                }
                else {
                    // printerr("INFO: %s=%lld\n", eventname, shared_counters[i]);
                    sprintf(counter_str, "%s=%lld ", eventname, shared_counters[i]);
                    strcat(counters_str, counter_str);
                }
            }
        }
        // printerr("[Thread-%d] Concatenated hardware counters string is %s...\n", mpi_rank, counters_str);

        #endif

        memset(msg, 0, BUFSIZ);

        sprintf(msg, "ts=%d event=workflow_trace level=INFO status=0 "
                   "job_id=%s kickstart_pid=%s executable=%s hostname=%s mpi_rank=%d utime=%.3f stime=%.3f "
                   "iowait=%.3f vmSize=%llu vmRSS=%llu threads=%lu read_bytes=%llu write_bytes=%llu "
                   "syscr=%lu syscw=%lu %s\n",

            (int)timestamp, job_id, kickstart_pid, exec_name, hostname, mpi_rank, 
            cpu_info.real_utime, cpu_info.real_stime, cpu_info.real_iowait,
            mem_info.vmSize, mem_info.vmRSS, mem_info.threads,
            io_info.rchar, io_info.wchar, io_info.syscr, io_info.syscw, counters_str);

        printerr("[Thread-%d] Sending: %s\n", mpi_rank, msg);

        if( send_msg_to_kickstart(msg, monitoring_socket_host, monitoring_socket_port) ) {
            printerr("[Thread-%d] There was a problem sending a message to kickstart...\n", mpi_rank);

            // TODO we should try to open a file and log monitoring stuff

            // if( (kickstart_status = open_kickstart_status_file()) == NULL ) {
            //     printerr("[Thread-%d] ERROR during kickstart status open: %s\n", mpi_rank, strerror(errno)));
            // }
            // else {
            //     fprintf_untraced(kickstart_status, "%s\n", msg);
            //     fflush(kickstart_status);
            //     fclose(kickstart_status);
            // }

        }

        sleep(interval);
    }

    printerr("[Thread-%d] We are finishing our work...\n", mpi_rank);

    return NULL;
}

#ifdef HAS_PAPI

/* Read eventset, return 1 on failure, 0 on success */
int read_hardware_counters(int eventset, int *shared_nevents, int *shared_events, long long *shared_counters) {
	// printerr("INFO: checking eventset %d\n", eventset);	

	int i, rc, nevents = papi_max_events, events[papi_max_events];
	long long counters[papi_max_events];
	char eventname[256];

	/* Get the events that were actually recorded */
    /* Initialization shared data structures if this is first read */
    if(*shared_nevents <= 0) {
    	rc = PAPI_list_events(eventset, events, &nevents);
    	// printerr("INFO: rc is: %d, shared_nevents: %d\n", rc, nevents);    

    	if (rc != PAPI_OK) {
      		printerr("ERROR: PAPI_list_events failed: %s\n", PAPI_strerror(rc));
      		*shared_nevents = 0;

      		return 1;
    	}

    	*shared_nevents = nevents;
        for (i = 0; i < *shared_nevents; i++) {
        	rc = PAPI_event_code_to_name(events[i], eventname);

        	if(rc != PAPI_OK) {
        		printerr("ERROR: Could not get event name: %s\n", PAPI_strerror(rc));
        		*shared_nevents = 0;

        		return 1;
        	}
        	else {
        		// printerr("INFO: shared_events[%d] = %s\n", i, eventname);
        		shared_events[i] = events[i];
        	}
        }

    }
    else {
	    rc = PAPI_list_events(eventset, events, &nevents);
	    // printerr("INFO: rc is: %d, nevents: %d\n", rc, nevents);    

	    if (rc != PAPI_OK) {
	      printerr("ERROR: PAPI_list_events failed: %s\n", PAPI_strerror(rc));
	      return 1;
	    }

        for (i = 0; i < nevents; i++) {
        	rc = PAPI_event_code_to_name(events[i], eventname);

        	if(rc != PAPI_OK) {
        		printerr("ERROR: Could not get event name: %s\n", PAPI_strerror(rc));

        		return 1;
        	}
        	else {
        		// printerr("INFO: events[%d] = %s\n", i, eventname);
        	}
        }	    

	    for(i = 0; i < nevents; i++) {
	    	if(shared_events[i] != events[i]) {
	    		printerr("ERROR: an event on position %d is different than the shared event\n", i);
	    		return 1;
	    	}
	    }
    }

    rc = PAPI_read(eventset, counters);
    
    if(rc != PAPI_OK) {
      printerr("ERROR: No hardware counters or PAPI not supported: %s\n", PAPI_strerror(rc));
      return 1;
    }
    else {
        // printerr("INFO: reporting hardware counters values...\n");
        
        for (i = 0; i < nevents; i++) {
        	rc = PAPI_event_code_to_name(events[i], eventname);
        	if(rc != PAPI_OK) {
        		printerr("ERROR: Could not get event name: %s\n", PAPI_strerror(rc));
        	}
        	else {
        		// printerr("INFO: %s=%lld\n", eventname, counters[i]);
        		/* We are summing up counters from different threads (eventsets) */
        		shared_counters[i] = shared_counters[i] + counters[i];
        	}
        }
    }

    return 0;
}

#endif
