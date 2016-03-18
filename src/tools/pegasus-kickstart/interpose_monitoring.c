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
#include <pthread.h>
#include <signal.h>
#include <sys/time.h>
#include <netinet/in.h>

#include "error.h"
#include "interpose_monitoring.h"
#include "procfs.h"

static int monitor_running;
static pthread_t monitor_thread;
static pthread_mutex_t monitor_mutex;
static pthread_cond_t monitor_cv;

static int send_msg_to_kickstart(char *msg, int size, char *host, char *port) {
    struct addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    struct addrinfo *servinfo;
    int gaierr = getaddrinfo(host, port, &hints, &servinfo);
    if (gaierr != 0) {
        printerr("getaddrinfo: %s\n", gai_strerror(gaierr));
        return -1;
    }

    int sockfd;
    struct addrinfo *p;
    for (p = servinfo; p != NULL; p = p->ai_next) {
        sockfd = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
        if (sockfd == -1) {
            printerr("socket: %s\n", strerror(errno));
            continue;
        }

        if (connect(sockfd, p->ai_addr, p->ai_addrlen) == -1) {
            printerr("connect: %s", strerror(errno));
            close(sockfd);
            continue;
        }

        // Successful connection
        break;
    }

    freeaddrinfo(servinfo);

    if (p == NULL) {
        printerr("failed to connect to kickstart monitor: no suitable addr\n");
        return -1;
    }

    if (send(sockfd, msg, size, 0) < 0) {
        printerr("Error during msg send: %s\n", strerror(errno));
        return -1;
    }

    close(sockfd);

    return 0;
}

static int set_monitoring_params(int *mpi_rank, int *interval,
        char **socket_host, char **socket_port, char *hostname) {

    char *envptr = getenv("OMPI_COMM_WORLD_RANK");
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
    if (envptr == NULL) {
        *mpi_rank = 0;
    } else {
        *mpi_rank = atoi(envptr);
    }

    if ((envptr = getenv("KICKSTART_MON_INTERVAL")) == NULL) {
        printerr("ERROR: KICKSTART_MON_INTERVAL not set in environment\n");
        return 1;
    }
    *interval = atoi(envptr);

    if ((*socket_host = getenv("KICKSTART_MON_HOST")) == NULL) {
        printerr("ERROR: KICKSTART_MON_HOST not set in environment\n");
        return 1;
    }

    if ((*socket_port = getenv("KICKSTART_MON_PORT")) == NULL) {
        printerr("ERROR: KICKSTART_MON_PORT not set in environment\n");
        return 1;
    }

    if (gethostname(hostname, BUFSIZ)) {
        printerr("ERROR: gethostname() failed: %s\n", strerror(errno));
        return 1;
    }

    return 0;
}

void* _interpose_monitoring_thread_func(void* arg) {
    pthread_mutex_lock(&monitor_mutex);

    int mpi_rank = 0;
    int interval = 60;
    unsigned long sequence = 0;
    char hostname[BUFSIZ] = "";
    char msg[BUFSIZ] = "";
    char *mon_host = NULL;
    char *mon_port = NULL;
    ProcStats stats;
    ProcStats diff;

    procfs_stats_init(&stats);
    procfs_stats_init(&diff);

    if (set_monitoring_params(&mpi_rank, &interval, &mon_host, &mon_port, hostname)) {
        printerr("ERROR: Unable to configure monitoring thread\n");
        goto exit;
    }

    while (monitor_running) {

        struct timeval now;
        struct timespec timeout;
        gettimeofday(&now, NULL);
        timeout.tv_sec = now.tv_sec + interval;
        timeout.tv_nsec = now.tv_usec * 1000UL;
        pthread_cond_timedwait(&monitor_cv, &monitor_mutex, &timeout);

        gettimeofday(&now, NULL);
        double timestamp = now.tv_sec + ((double)now.tv_usec * 1e-6);
        procfs_read_stats_diff(getpid(), &stats, &diff);

        sprintf(msg, "ts=%.3f pid=%d seq=%lu exe=%s host=%s rank=%d "
                     "utime=%.3f stime=%.3f iowait=%.3f vmpeak=%llu rsspeak=%llu "
                     "threads=%d bread=%llu bwrite=%llu rchar=%llu wchar=%llu "
                     "syscr=%lu syscw=%lu\n",
                     timestamp, getpid(), sequence++, stats.exe,
                     hostname, mpi_rank, diff.utime, diff.stime,
                     diff.iowait, diff.vmpeak, diff.rsspeak, diff.threads,
                     diff.read_bytes, diff.write_bytes, diff.rchar, diff.wchar,
                     diff.syscr, diff.syscw);
        if (send_msg_to_kickstart(msg, strlen(msg), mon_host, mon_port)) {
            printerr("Process rank=%d pid=%d failed to send message to kickstart\n",
                     mpi_rank, getpid());
        }

        /* Move this down here so that we always send one event before exiting */
        if (!monitor_running) {
            break;
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

    int rc = pthread_create(&monitor_thread, NULL, _interpose_monitoring_thread_func, NULL);
    if (rc) {
        printerr("Could not spawn the monitoring thread: %d %s\n", rc, strerror(errno));
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

