#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <getopt.h>
#include <signal.h>
#include <nvml.h>
#include <sys/time.h>
#include <pthread.h>
#include <poll.h>
#include <sys/timerfd.h>
#include "nvml_wrapper.h"
#include "nvidia_monitor.h"

#define BUFFER_DEFAULT_LIMIT 1024
#define BUFFER_HARD_LIMIT 32768

static volatile int exit_guard = 1;


void signal_handler(int signum) {
    exit_guard = 0;
}


void publish_stats() {
    return;
}


void* collect_gpu_stats(void *args) {
    struct timeval begin, end;
    long seconds, microseconds;
    
    thread_data *data = (thread_data *)args;
    
    gettimeofday(&begin, 0);

    data->result = getGpuStatistics(&data->env->devices[data->i], data->monitor_pcie_usage);
    if (data->result != NVML_SUCCESS)
        pthread_exit(NULL);

    if (data->monitor_compute_procs) {
        data->result = getGpuComputeProcesses(&data->env->devices[data->i]);
        if (data->result != NVML_SUCCESS)
            pthread_exit(NULL);
    }
    
    if (data->monitor_graphics_procs) {
        data->result = getGpuGraphicsProcesses(&data->env->devices[data->i]);
        if (data->result != NVML_SUCCESS)
            pthread_exit(NULL);
    }
    
    if (data->monitor_process_util) {
        data->result = getGpuProcessUtilization(&data->env->devices[data->i]);
        if (data->result != NVML_SUCCESS)
            pthread_exit(NULL);
    }

    gettimeofday(&end, 0);
    seconds = end.tv_sec - begin.tv_sec;
    microseconds = end.tv_usec - begin.tv_usec;
    data->last_collection_duration = seconds + microseconds*1e-6;

    if (data->publish_url != NULL)
    
    pthread_exit(NULL);
}

nvmlReturn_t multi_threaded_collection(FILE *out, gpu_env_struct *env, pthread_t *threads, thread_data *threads_data, unsigned char output_json, unsigned int interval) {
    unsigned int i;
    json_doc doc;

    if (output_json) {
        doc.buffer_limit = BUFFER_DEFAULT_LIMIT;
        doc.buffer = (char *) malloc(BUFFER_DEFAULT_LIMIT * sizeof(char));
    }
    else {
        doc.buffer_size = 0;
        doc.buffer_limit = 0;
        doc.buffer = NULL;
    }
    
    while(exit_guard) {
        for (i=0; i<env->device_count; i++)
            pthread_create(&threads[i], NULL, collect_gpu_stats, &threads_data[i]);

        for (i=0; i<env->device_count; i++) {
            pthread_join(threads[i], NULL);
            if (threads_data[i].result != NVML_SUCCESS)
                return threads_data[i].result;

            if (output_json) {
                while ((doc.buffer_size = json_encode_device_stats(threads_data[i].env->devices[i], threads_data[i].last_collection_duration, doc.buffer, doc.buffer_limit)) < 0) {
                    free(doc.buffer);
                    if (doc.buffer_limit >= BUFFER_HARD_LIMIT)
                        return -1;
                    doc.buffer_limit *= 2;
                    doc.buffer = (char *) malloc(doc.buffer_limit * sizeof(char));
                }
                fprintf(out, "%s,\n", doc.buffer);
            }
            else {
                printGpuStatistics(out, threads_data[i].env->devices[i]);
                if (threads_data[i].monitor_compute_procs)
                    printGpuComputeProcessInfos(out, threads_data[i].env->devices[i]);
                if (threads_data[i].monitor_graphics_procs)
                    printGpuGraphicsProcessInfos(out, threads_data[i].env->devices[i]);
                if (threads_data[i].monitor_process_util)
                    printGpuProcessSamples(out, threads_data[i].env->devices[i]);
            }
        }
        
        sleep(interval);
    }

    free(doc.buffer);
    return NVML_SUCCESS;
}

nvmlReturn_t single_threaded_collection(FILE *out, gpu_env_struct *env, int monitor_compute_procs, int monitor_graphics_procs, int monitor_pcie_usage, int monitor_process_util, int output_json, unsigned int interval) {
    unsigned int i;
    double last_collection_duration;
    nvmlReturn_t result ;
    struct timeval begin, end;
    long seconds, microseconds;
    json_doc doc;

    //
    int ret;
    int fd = -1;
    struct itimerspec timeout;

    /* Create timer for monitoring interval */
    int timer = timerfd_create(CLOCK_MONOTONIC, TFD_CLOEXEC);
    struct itimerspec timercfg;
    timercfg.it_value.tv_sec = interval;
    timercfg.it_value.tv_nsec = 0;
    timercfg.it_interval.tv_sec = interval; /* Fire every interval seconds */
    timercfg.it_interval.tv_nsec = 0;
    if (timerfd_settime(timer, 0, &timercfg, NULL) < 0) {
        fprintf(stderr, "Error setting timerfd time");
        return -1;
    }
    //

    if (output_json) {
        doc.buffer_limit = BUFFER_DEFAULT_LIMIT;
        doc.buffer = (char *) malloc(BUFFER_DEFAULT_LIMIT * sizeof(char));
    }
    else {
        doc.buffer_size = 0;
        doc.buffer_limit = 0;
        doc.buffer = NULL;
    }

    while(exit_guard) {
        gettimeofday(&begin, 0);
        
        for (i=0; i< env->device_count; i++) {
            result = getGpuStatistics(&env->devices[i], monitor_pcie_usage);
            if (result != NVML_SUCCESS)
                return result;

            if (monitor_compute_procs) {
                result = getGpuComputeProcesses(&env->devices[i]);
                if (result != NVML_SUCCESS)
                    return result;
            }
            
            if (monitor_graphics_procs) {
                result = getGpuGraphicsProcesses(&env->devices[i]);
                if (result != NVML_SUCCESS)
                    return result;
            }

            if (monitor_process_util) {
                result = getGpuProcessUtilization(&env->devices[i]);
                if (result != NVML_SUCCESS)
                    return result;
            }
        }
        
        gettimeofday(&end, 0);
        seconds = end.tv_sec - begin.tv_sec;
        microseconds = end.tv_usec - begin.tv_usec;
        last_collection_duration = seconds + microseconds*1e-6;
            
        for (i=0; i<env->device_count; i++)
        {
            if (output_json) {
                while ((doc.buffer_size = json_encode_device_stats(env->devices[i], last_collection_duration, doc.buffer, doc.buffer_limit)) < 0) {
                    free(doc.buffer);
                    if (doc.buffer_limit >= BUFFER_HARD_LIMIT)
                        return -1;
                    doc.buffer_limit = doc.buffer_limit * 2;
                    doc.buffer = (char *) malloc(doc.buffer_limit * sizeof(char));
                }
                fprintf(out, "%s,\n", doc.buffer);
            }
            else {
                printGpuStatistics(out, env->devices[i]);
                if (monitor_compute_procs)
                    printGpuComputeProcessInfos(out, env->devices[i]);
                if (monitor_graphics_procs)
                    printGpuGraphicsProcessInfos(out, env->devices[i]);
                if (monitor_process_util)
                    printGpuProcessSamples(out, env->devices[i]);
            }
            //printf("Time took to finish sampling: %.3f seconds.\n\n", last_collection_duration);
        }

        struct pollfd fd_t[1];
        fd_t[0].fd = timer;
        fd_t[0].events = POLLIN;
        //sleep(interval);
        if (poll(fd_t, 1, -1) <= 0) {
            fprintf(stderr, "Error polling:");
            return -1;
        }
        if ((fd_t[0].revents & POLLIN) == 0) {
            fprintf(stderr, "TIMER IS NOT READY");
            return -1;
        }
        else {
            unsigned long long expirations = 0;
            if (read(timer, &expirations, sizeof(expirations)) < 0) {
                fprintf(stderr, "timerfd read failed");
            } else if (expirations > 1) {
                fprintf(stderr, "timer expired %llu times\n", expirations);
            }
        }
    }

    close(timer);
    free(doc.buffer);
    return NVML_SUCCESS;
}

int main(int argc, char *argv[]) {
    unsigned int i;
    nvmlReturn_t result;
    gpu_env_struct env;
    unsigned int interval = 10;
    static int monitor_compute_procs = 0;
    static int monitor_graphics_procs = 0;
    static int monitor_process_util = 0;
    static int monitor_pcie_usage = 0;
    static int is_multi_threaded = 0;
    static int is_output_json = 0;
    static int suppress_output = 0;
    FILE *out = NULL;
    char *output_file = NULL;
    char *publish_url = NULL;
    json_doc doc;

    pthread_t *threads = NULL;
    thread_data *threads_data = NULL;

    signal(SIGINT, signal_handler);

    int opt;
    int option_index;
    static struct option long_options[] = {
        {"bus", 0, &monitor_pcie_usage, 1},
        {"compute-procs", 0, &monitor_compute_procs, 1},
        {"graphics-procs", 0, &monitor_graphics_procs, 1},
        {"process-util", 0, &monitor_process_util, 1},
        {"multi-threaded", 0, &is_multi_threaded, 1},
        {"suppress", 0, &suppress_output, 1},
        {"json", 0, &is_output_json, 1},
        {"output", 1, 0, 'o'},
        {"publish", 1, 0, 'p'},
        {"interval", 1, 0, 'i'},
        {"help", 0, 0, 'h'},
        {0, 0, 0, 0}
    };

    while((opt = getopt_long(argc, argv, ":o:p:i:bcgmujh", long_options, &option_index)) != -1) { 
        switch(opt) {
            case 0:
                break;
            case 'i':
                interval = atoi(optarg);
                break;
            case 'o':
                output_file = optarg;
                break;
            case 'p':
                publish_url = optarg;
                break;
            case 'b':
                monitor_pcie_usage = 1;
                break;
            case 'c':
                monitor_compute_procs = 1;
                break;
            case 'g':
                monitor_graphics_procs = 1;
                break;
            case 'u':
                monitor_process_util = 1;
                break;
            case 'm':
                is_multi_threaded = 1;
                break;
            case 's':
                suppress_output = 1;
                break;
            case 'j':
                is_output_json = 1;
                break;
            case 'h': 
                printf("Usage: ./nvidia-monitor [-bcgmuj] [-o <filename>] [-p <publish_url>] [-i <interval>]\n");
                return 0;
                break;
            case ':': 
                printf("Option needs a value\n"); 
                printf("Usage: ./nvidia-monitor [-bcgmuj] [-o <filename>] [-p <publish_url>] [-i <interval>]\n");
                return 1;
            case '?': 
                printf("Unknown option: %c\n", optopt);
                printf("Usage: ./nvidia-monitor [-bcgmuj] [-o <filename>] [-p <publish_url>] [-i <interval>]\n");
                return 1;
        }
    }

    // Open output file if specified, otherwise send output to stdout
    if (output_file == NULL)
        out = stdout;
    else
        out = fopen(output_file, "w+");
    
    if (is_output_json) {
        doc.buffer_limit = BUFFER_DEFAULT_LIMIT;
        doc.buffer = (char *) malloc(BUFFER_DEFAULT_LIMIT * sizeof(char));
    }
    else {
        doc.buffer_size = 0;
        doc.buffer_limit = 0;
        doc.buffer = NULL;
    }

    // First initialize NVML library
    result = nvmlInit();
    if (result != NVML_SUCCESS) { 
        fprintf(stderr, "Failed to initialize NVML: %s\n", nvmlErrorString(result));
        return 1;
    }

    result = getGpuEnvironment(&env);
    if (result != NVML_SUCCESS)
      goto Error;
    
    if (is_output_json) {
        while ((doc.buffer_size = json_encode_environment(env, doc.buffer, doc.buffer_limit)) < 0) {
            free(doc.buffer);
            if (doc.buffer_limit >= BUFFER_HARD_LIMIT)
                goto Error;
            doc.buffer_limit = doc.buffer_limit * 2;
            doc.buffer = (char *) malloc(doc.buffer_limit * sizeof(char));
        }
        fprintf(out, "[%s,\n", doc.buffer);
    }
    else
        printGpuEnvironment(out, env);
    
    //Sample gpus info
    if (is_multi_threaded) {
        threads = (pthread_t*) malloc(env.device_count * sizeof(pthread_t));
        threads_data = (thread_data*) malloc(env.device_count * sizeof(thread_data));
        
        if (threads == NULL || threads_data == NULL) {
            fprintf(stderr, "Not enough memory for threads\n");
            goto Error;
        }

        for (i=0; i<env.device_count; i++) {
            threads_data[i].i = i;
            threads_data[i].env = &env;
            threads_data[i].last_collection_time = 0;
            threads_data[i].last_collection_duration = 0;
            threads_data[i].monitor_compute_procs = monitor_compute_procs;
            threads_data[i].monitor_graphics_procs = monitor_graphics_procs;
            threads_data[i].monitor_process_util = monitor_process_util;
            threads_data[i].monitor_pcie_usage = monitor_pcie_usage;
            threads_data[i].output_json = is_output_json;
            threads_data[i].output = output_file;
            threads_data[i].publish_url = publish_url;
            if (is_output_json) {
                threads_data[i].doc.buffer_limit = BUFFER_DEFAULT_LIMIT;
                threads_data[i].doc.buffer = (char*) malloc(BUFFER_DEFAULT_LIMIT * sizeof(char));
            }
        }
        
        result = multi_threaded_collection(out, &env, threads, threads_data, is_output_json, interval);
        
        if (threads != NULL) free(threads);
        if (threads_data != NULL) {
            for (i=0; i<env.device_count; i++)
                if (threads_data[i].doc.buffer != NULL) free(threads_data[i].doc.buffer);
            free(threads_data);
        }

        if (result != NVML_SUCCESS)
            goto Error;
    }
    else {
        result = single_threaded_collection(out, &env, monitor_compute_procs, monitor_graphics_procs, monitor_pcie_usage, monitor_process_util, is_output_json, interval);
        if (result != NVML_SUCCESS)
            goto Error;
    }

    //Output max measurements
    if (is_output_json) {
        while ((doc.buffer_size = json_encode_device_stats_max(env, doc.buffer, doc.buffer_limit)) < 0) {
            free(doc.buffer);
            if (doc.buffer_limit >= BUFFER_HARD_LIMIT)
                goto Error;
            doc.buffer_limit = doc.buffer_limit * 2;
            doc.buffer = (char *) malloc(doc.buffer_limit * sizeof(char));
        }
        fprintf(out, "%s]\n", doc.buffer);
    }
    else
        printGpuMaxMeasurements(out, env);

    if (output_file != NULL) fclose(out);
    if (doc.buffer != NULL) free(doc.buffer);
    nvml_monitoring_cleanup(&env);

    result = nvmlShutdown();
    if (result != NVML_SUCCESS)
        fprintf(stderr, "Failed to shutdown NVML: %s\n", nvmlErrorString(result));

    return 0;

Error:
    if (output_file != NULL) fclose(out);
    if (doc.buffer != NULL) free(doc.buffer);
    nvml_monitoring_cleanup(&env);
   
    result = nvmlShutdown();
    if (result != NVML_SUCCESS)
        fprintf(stderr, "Failed to shutdown NVML: %s\n", nvmlErrorString(result));

    return 1;
}
