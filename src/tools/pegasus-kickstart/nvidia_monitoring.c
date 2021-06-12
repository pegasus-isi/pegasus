#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <stdlib.h>
#include "nvidia/nvml.h"
#include <sys/time.h>
#include <pthread.h>
#include <poll.h>
#include <sys/timerfd.h>
#include <fcntl.h>
#include "nvidia/nvml_wrapper.h"
#include "nvidia_monitoring.h"
#include "error.h"
#include "log.h"

#define BUFFER_DEFAULT_LIMIT 1024
#define BUFFER_HARD_LIMIT 32768

static int nvidia_signal_pipe[2];
static pthread_t nvidia_monitoring_thread;
static int nvidia_monitor_running;

nvmlReturn_t update_gpu_stats(gpu_dev_info_struct *device, int monitor_pcie_usage, int monitor_graphics_procs, int monitor_process_util) {
    nvmlReturn_t result;
    
    if (monitor_graphics_procs) {
        result = getGpuGraphicsProcesses(device);
        if (result != NVML_SUCCESS)
            return result;
    }
    
    if (monitor_process_util) {
        result = getGpuProcessUtilization(device);
        if (result != NVML_SUCCESS)
            return result;
    }

    result = getGpuComputeProcesses(device);
    if (result != NVML_SUCCESS)
        return result;
    
    result = getGpuStatistics(device, monitor_pcie_usage);
    if (result != NVML_SUCCESS)
        return result;
    
    return result;
}

void* nvidia_monitoring_thread_func(void *args) {
    MonitoringContext *ctx = (MonitoringContext *) args;
    unsigned int i;
    int monitor_graphics_procs = 0;
    int monitor_process_util = 0;
    int monitor_pcie_usage = 0;
    nvmlReturn_t result;
    gpu_env_struct env;
    json_doc ctx_doc; ctx_doc.buffer = NULL;
    json_doc tmp_doc; tmp_doc.buffer = NULL;
    json_doc gpu_doc; gpu_doc.buffer = NULL;
    
    struct timeval begin, end;
    long seconds, microseconds;

    info("Nvidia Monitoring thread starting...");
    debug("url: %s", ctx->url);
    debug("wf uuid: %s", ctx->wf_uuid);
    debug("wf label: %s", ctx->wf_label);
    debug("dag job id: %s", ctx->dag_job_id);
    debug("condor job id: %s", ctx->condor_job_id);
    debug("xformation: %s", ctx->xformation);
    debug("task id: %s", ctx->task_id);
    debug("process group: %d", getpgid(0));

    char* envptr;

    envptr = getenv("KICKSTART_MON_GRAPHICS_PROCS");
    if (envptr == NULL)
        warn("KICKSTART_MON_GRAPHICS_PROCS not specified\n");
    else
        monitor_graphics_procs = 1;
    
    envptr = getenv("KICKSTART_MON_GRAPHICS_UTIL");
    if (envptr == NULL)
        warn("KICKSTART_MON_GRAPHICS_UTIL not specified\n");
    else
        monitor_process_util = 1;

    envptr = getenv("KICKSTART_MON_GRAPHICS_PCIE");
    if (envptr == NULL)
        warn("KICKSTART_MON_GRAPHICS_PCIE not specified\n");
    else
        monitor_pcie_usage = 1;
    
    // First initialize NVML library
    result = nvmlInit();
    if (result != NVML_SUCCESS) { 
        printerr("Failed to initialize NVML: %s\n", nvmlErrorString(result));
        goto Error;
    }
    
    int timer = timerfd_create(CLOCK_MONOTONIC, TFD_CLOEXEC);
    ctx_doc.buffer_limit = BUFFER_DEFAULT_LIMIT;
    ctx_doc.buffer = (char *) malloc(BUFFER_DEFAULT_LIMIT * sizeof(char));
    gpu_doc.buffer_limit = BUFFER_DEFAULT_LIMIT;
    gpu_doc.buffer = (char *) malloc(BUFFER_DEFAULT_LIMIT * sizeof(char));
    tmp_doc.buffer_limit = BUFFER_HARD_LIMIT;
    tmp_doc.buffer = (char *) malloc(BUFFER_HARD_LIMIT * sizeof(char));

    ctx_doc.buffer_size = json_encode_context(ctx, ctx_doc.buffer, ctx_doc.buffer_limit);
    if (ctx_doc.buffer_size < 0) {
        printerr("Nvidia context buffer reached limit\n");
        goto Error;
    }

    ctx_doc.buffer[ctx_doc.buffer_size-1] = '\0';
    ctx_doc.buffer_size -= 1;

    result = getGpuEnvironment(&env);
    if (result != NVML_SUCCESS)
      goto Error;
    
    while ((gpu_doc.buffer_size = json_encode_environment(env, gpu_doc.buffer, gpu_doc.buffer_limit)) < 0) {
        if (gpu_doc.buffer_limit >= BUFFER_HARD_LIMIT) {
            printerr("Nvidia monitor buffer reached limit\n");
            goto Error;
        }
        gpu_doc.buffer_limit = gpu_doc.buffer_limit * 2;
        gpu_doc.buffer = (char *) realloc(gpu_doc.buffer, gpu_doc.buffer_limit * sizeof(char));
    }

    tmp_doc.buffer_size = snprintf(tmp_doc.buffer, BUFFER_HARD_LIMIT, "%s,%s", ctx_doc.buffer, gpu_doc.buffer+1);
    send_monitoring_report_json(ctx, tmp_doc);

    /* Create timer for monitoring interval */
    struct itimerspec timercfg;
    timercfg.it_value.tv_sec = ctx->interval;
    timercfg.it_value.tv_nsec = 0;
    timercfg.it_interval.tv_sec = ctx->interval; /* Fire every interval seconds */
    timercfg.it_interval.tv_nsec = 0;
    if (timerfd_settime(timer, 0, &timercfg, NULL) < 0) {
        printerr("Error setting timerfd time: %s\n", strerror(errno));
        pthread_exit(NULL);
    }

    struct pollfd fds[2];
    fds[0].fd = nvidia_signal_pipe[0];
    fds[0].events = POLLIN;
    fds[1].fd = timer;
    fds[1].events = POLLIN;

    int signalled = 0;
    while (!signalled) {
        if (poll(fds, 2, -1) <= 0) {
            printerr("Error polling: %s\n", strerror(errno));
            break;
        }
        
        //caught shutdown signal
        if (fds[0].revents & POLLIN) {
            debug("Nvidia Monitoring thread caught signal");
            signalled = 1;
        }//caught timer event
        else if (fds[1].revents & POLLIN) {
            unsigned long long expirations = 0;
            if (read(timer, &expirations, sizeof(expirations)) < 0) {
                error("nvidia timerfd read failed: %s\n", strerror(errno));
            } else if (expirations > 1) {
                warn("nvidia timer expired %llu times\n", expirations);
            }

            trace("Timer expired");

            /* retrieve gpu stats */
            for (i = 0; i < env.device_count; i++) {
                gettimeofday(&begin, 0);
                
                update_gpu_stats(&env.devices[i], monitor_pcie_usage, monitor_graphics_procs, monitor_process_util);
                
                gettimeofday(&end, 0);
                seconds = end.tv_sec - begin.tv_sec;
                microseconds = end.tv_usec - begin.tv_usec;
            }

            /* Send a monitoring message */
            for (i = 0; i < env.device_count; i++) {
                while ((gpu_doc.buffer_size = json_encode_device_stats(env.devices[i], 0, gpu_doc.buffer, gpu_doc.buffer_limit)) < 0) {
                    if (gpu_doc.buffer_limit >= BUFFER_HARD_LIMIT)
                    {
                        printerr("Nvidia monitor buffer reached limit\n");
                        goto Error;
                    }
                    gpu_doc.buffer_limit *= 2;
                    gpu_doc.buffer = (char *) realloc(gpu_doc.buffer, gpu_doc.buffer_limit * sizeof(char));
                }
                
                tmp_doc.buffer_size = snprintf(tmp_doc.buffer, BUFFER_HARD_LIMIT, "%s,%s", ctx_doc.buffer, gpu_doc.buffer+1);
                send_monitoring_report_json(ctx, tmp_doc);
            }
        }
    }
    
    while ((gpu_doc.buffer_size = json_encode_device_stats_max(env, gpu_doc.buffer, gpu_doc.buffer_limit)) < 0) {
        if (gpu_doc.buffer_limit >= BUFFER_HARD_LIMIT)
        {
            printerr("Nvidia monitor buffer reached limit\n");
            goto Error;
        }
        gpu_doc.buffer_limit = gpu_doc.buffer_limit * 2;
        gpu_doc.buffer = (char *) realloc(gpu_doc.buffer, gpu_doc.buffer_limit * sizeof(char));
    }
 
    tmp_doc.buffer_size = snprintf(tmp_doc.buffer, BUFFER_HARD_LIMIT, "%s,%s", ctx_doc.buffer, gpu_doc.buffer+1);
    send_monitoring_report_json(ctx, tmp_doc);

Error:
    info("Nvidia Monitoring thread exiting");
    
    result = nvmlShutdown();
    if (result != NVML_SUCCESS)
        fprintf(stderr, "Failed to shutdown NVML: %s\n", nvmlErrorString(result));

    if (ctx_doc.buffer != NULL) free(ctx_doc.buffer);
    if (tmp_doc.buffer != NULL) free(tmp_doc.buffer);
    if (gpu_doc.buffer != NULL) free(gpu_doc.buffer);
    if (timer) close(timer);
    
    nvml_monitoring_cleanup(&env);
   
    release_monitoring_context(ctx);

    nvidia_monitor_running = 0;
    
    pthread_exit(NULL);
}

int start_nvidia_monitoring_thread() {
    nvidia_monitor_running = 0;

    /* Make sure the calling process is in its own process group */
    setpgid(0, 0);
    
    /* Set up parameters for the thread */
    MonitoringContext *ctx = calloc(1, sizeof(MonitoringContext));
    if (initialize_monitoring_context(ctx) < 0) {
        return -1;
    }
    
    /* Create a pipe to signal between the main thread and the nvidia monitor thread */
    int rc = pipe(nvidia_signal_pipe);
    if (rc < 0) {
        printerr("ERROR: Unable to create signal pipe: %s\n", strerror(errno));
        return rc;
    }
    rc = fcntl(nvidia_signal_pipe[0], F_SETFD, FD_CLOEXEC);
    if (rc < 0) {
        printerr("WARNING: Unable to set CLOEXEC on pipe: %s\n", strerror(errno));
    }
    rc = fcntl(nvidia_signal_pipe[1], F_SETFD, FD_CLOEXEC);
    if (rc < 0) {
        printerr("WARNING: Unable to set CLOEXEC on pipe: %s\n", strerror(errno));
    }

    /* Start and detach the monitoring thread */
    rc = pthread_create(&nvidia_monitoring_thread, NULL, nvidia_monitoring_thread_func, (void*)ctx);
    if (rc) {
        printerr("ERROR: return code from pthread_create() is %d: %s\n", rc, strerror(errno));
        return rc;
    }

    nvidia_monitor_running = 1;

    return 0;
}

int stop_nvidia_monitoring_thread() {
    char msg = 1;
    int rc = write(nvidia_signal_pipe[1], &msg, 1);

    if (rc <= 0) {
        printerr("ERROR: Problem signalling monitoring thread: %s\n", strerror(errno));
        return rc;
    }

    if (nvidia_monitor_running) {
        pthread_join(nvidia_monitoring_thread, NULL);
    }

    close(nvidia_signal_pipe[0]);
    close(nvidia_signal_pipe[1]);

    nvidia_monitor_running = 0;

    return 0;
}
