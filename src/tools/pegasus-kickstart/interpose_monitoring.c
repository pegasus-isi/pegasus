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

static int mon_interval = 10;
static char *mon_host = NULL;
static char *mon_port = NULL;
static int monitor_running;
static pthread_t monitor_thread;
static pthread_mutex_t monitor_mutex;
static pthread_cond_t monitor_cv;

static int send_msg_to_kickstart(ProcStats *stats) {
    if (mon_host == NULL || mon_port == NULL) {
        return -1;
    }

    struct addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    struct addrinfo *servinfo;
    int gaierr = getaddrinfo(mon_host, mon_port, &hints, &servinfo);
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
            printerr("connect: %s\n", strerror(errno));
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

    if (send(sockfd, stats, sizeof(ProcStats), 0) < 0) {
        printerr("Error during msg send: %s\n", strerror(errno));
        return -1;
    }

    close(sockfd);

    return 0;
}

void interpose_send_stats(ProcStats *stats) {
    send_msg_to_kickstart(stats);
}

void *interpose_monitoring_thread_func(void* arg) {
    pthread_mutex_lock(&monitor_mutex);

    ProcStats stats;
    procfs_stats_init(&stats);

    while (monitor_running) {

        struct timeval now;
        struct timespec timeout;
        gettimeofday(&now, NULL);
        timeout.tv_sec = now.tv_sec + mon_interval;
        timeout.tv_nsec = now.tv_usec * 1000UL;
        pthread_cond_timedwait(&monitor_cv, &monitor_mutex, &timeout);

        procfs_read_stats(getpid(), &stats);

        /* TODO Get PAPI counters */

        if (send_msg_to_kickstart(&stats)) {
            printerr("Process %d failed to send message to kickstart\n",
                     getpid());
        }

        /* Move this down here so that we always send one event before exiting */
        if (!monitor_running) {
            break;
        }
    }

    pthread_mutex_unlock(&monitor_mutex);

    return NULL;
}

void interpose_spawn_monitoring_thread() {
    pthread_mutex_init(&monitor_mutex, NULL);
    pthread_cond_init(&monitor_cv, NULL);

    /* Only proceed if monitoring is enabled */
    if (getenv("KICKSTART_MON") == NULL) {
        return;
    }

    pthread_mutex_lock(&monitor_mutex);

    /* XXX Save all the environment variables because the application might
       change them later. */

    char *env = getenv("KICKSTART_MON_HOST");
    if (env == NULL) {
        printerr("KICKSTART_MON_HOST not set\n");
        exit(1);
    }
    mon_host = strdup(env);

    env = getenv("KICKSTART_MON_PORT");
    if (env == NULL) {
        printerr("KICKSTART_MON_PORT not set\n");
        exit(1);
    }
    mon_port = strdup(env);

    env = getenv("KICKSTART_MON_INTERVAL");
    if (env == NULL) {
        printerr("KICKSTART_MON_INTERVAL not set\n");
        exit(1);
    }
    mon_interval = atoi(env);

    monitor_running = 1;
    int rc = pthread_create(&monitor_thread, NULL, interpose_monitoring_thread_func, NULL);
    if (rc) {
        printerr("Could not spawn the monitoring thread: %d %s\n", rc, strerror(errno));
        monitor_running = 0;
    }

    pthread_mutex_unlock(&monitor_mutex);
}

void interpose_stop_monitoring_thread() {
    pthread_mutex_lock(&monitor_mutex);

    if (monitor_running) {
        /* Signal the monitoring thread to shutdown */
        monitor_running = 0;
        pthread_cond_signal(&monitor_cv);

        pthread_mutex_unlock(&monitor_mutex);

        /* Wait for the monitoring thread */
        pthread_join(monitor_thread, NULL);
    } else {
        pthread_mutex_unlock(&monitor_mutex);
    }

    pthread_cond_destroy(&monitor_cv);
    pthread_mutex_destroy(&monitor_mutex);
}

