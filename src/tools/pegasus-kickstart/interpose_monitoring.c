#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include <signal.h>
#include <sys/time.h>

#include "error.h"
#include "interpose.h"
#include "interpose_monitoring.h"
#include "procfs.h"
#include "monitoring.h"

static int mon_interval = 10;
static char mon_host[128];
static char mon_port[16];
static int monitor_running;
static pthread_t monitor_thread;
static pthread_mutex_t monitor_mutex;
static pthread_cond_t monitor_cv;

void interpose_send_stats(ProcStats *stats) {
    send_msg_to_kickstart(mon_host, mon_port, stats);
}

void *interpose_monitoring_thread_func(void* arg) {
    pthread_mutex_lock(&monitor_mutex);

    ProcStats stats;

    while (monitor_running) {

        struct timeval now;
        struct timespec timeout;
        gettimeofday(&now, NULL);
        timeout.tv_sec = now.tv_sec + mon_interval;
        timeout.tv_nsec = now.tv_usec * 1000UL;
        pthread_cond_timedwait(&monitor_cv, &monitor_mutex, &timeout);

        gather_stats(&stats);

        if (send_msg_to_kickstart(mon_host, mon_port, &stats)) {
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
    char *env = getenv("KICKSTART_MON_INTERVAL");
    if (env == NULL) {
        return;
    }

    pthread_mutex_lock(&monitor_mutex);

    /* Note: It is important to save all the environment variables because
     * the application might change them later. For example, the application
     * might be pegasus-monitor or pegasus-kickstart. 
     */

    mon_interval = atoi(env);

    env = getenv("KICKSTART_MON_URL");
    if (env == NULL) {
        printerr("KICKSTART_MON_URL not set\n");
        exit(1);
    }
    if (sscanf(env, "kickstart://%127[^:]:%15[0-9]", mon_host, mon_port) != 2) {
        error("Unable to parse kickstart monitor URL: %s", env);
        exit(1);
    }

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

