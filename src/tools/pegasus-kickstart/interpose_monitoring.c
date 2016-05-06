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

static int monitor_running;
static pthread_t monitor_thread;
static pthread_mutex_t monitor_mutex;
static pthread_cond_t monitor_cv;

void *interpose_monitoring_thread_func(void* arg) {
    pthread_mutex_lock(&monitor_mutex);

    MonitoringContext *ctx = (MonitoringContext *)arg;

    while (monitor_running) {
        struct timeval now;
        struct timespec timeout;
        gettimeofday(&now, NULL);
        timeout.tv_sec = now.tv_sec + ctx->interval;
        timeout.tv_nsec = now.tv_usec * 1000UL;
        pthread_cond_timedwait(&monitor_cv, &monitor_mutex, &timeout);

        ProcStats stats;
        gather_stats(&stats);

        if (send_monitoring_report(ctx, &stats)) {
            printerr("Process %d failed to send message to kickstart\n",
                     getpid());
        }
    }

    release_monitoring_context(ctx);

    pthread_mutex_unlock(&monitor_mutex);

    return NULL;
}

void interpose_start_monitoring_thread() {
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
    MonitoringContext *ctx = calloc(1, sizeof(MonitoringContext));
    if (initialize_monitoring_context(ctx) < 0) {
        return;
    }

    monitor_running = 1;
    int rc = pthread_create(&monitor_thread, NULL, interpose_monitoring_thread_func, ctx);
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

