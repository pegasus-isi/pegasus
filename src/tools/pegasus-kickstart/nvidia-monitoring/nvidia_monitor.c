#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <getopt.h>
#include <signal.h>
#include <nvml.h>
#include <sys/time.h>
#include <pthread.h>
#include "nvidia_monitor.h"

static volatile int exit_guard = 1;

void signal_handler(int signum) {
    exit_guard = 0;
}

typedef struct thread_data {
    unsigned int i;
    double last_collection_duration;
    unsigned long int last_collection_time;
    unsigned char monitor_compute_procs;
    unsigned char monitor_graphics_procs;
    unsigned char monitor_pcie_usage;
    gpu_env_struct *env;
    char *output;
    char *publish_url;
    nvmlReturn_t result;
} thread_data;

void *collect_gpu_stats(void *args) {
    struct timeval begin, end;
    long seconds, microseconds;
    
    //thread_detach(pthread_self());
    thread_data *data = (thread_data *)args;
    
    gettimeofday(&begin, 0);
    data->last_collection_time = begin.tv_sec;

    data->result = getGpuStatistics(&data->env->devices[data->i], data->monitor_pcie_usage);
    if (data->result != NVML_SUCCESS)
        pthread_exit(NULL);

    if (data->monitor_compute_procs) {
        data->result = getGpuComputeProcesses(&data->env->devices[data->i]);
        if (data->result != NVML_SUCCESS)
            pthread_exit(NULL);
    }
    
    if (data->monitor_graphics_procs) {
        data->result = getGpuProcessStatistics(&data->env->devices[data->i]);
        if (data->result != NVML_SUCCESS)
            pthread_exit(NULL);
    }

    gettimeofday(&end, 0);
    seconds = end.tv_sec - begin.tv_sec;
    microseconds = end.tv_usec - begin.tv_usec;
    data->last_collection_duration = seconds + microseconds*1e-6;
    
    //TODO: publish and write to file
    /*if (data->output != NULL)
        printf("output: %s\n", output);
    if (data->publish_url != NULL)
        printf("publish: %s\n", publish_url);
    */

    pthread_exit(NULL);
}

nvmlReturn_t multi_threaded_collection(gpu_env_struct *env, pthread_t *threads, thread_data *threads_data, unsigned int interval) {
    unsigned int i;

    while(exit_guard) {
        for (i=0; i<env->device_count; i++)
            pthread_create(&threads[i], NULL, collect_gpu_stats, &threads_data[i]);

        sleep(interval);

        for (i=0; i<env->device_count; i++) {
            pthread_join(threads[i], NULL);
            if (threads_data[i].result != NVML_SUCCESS)
                return threads_data[i].result;
        }
    }

    return NVML_SUCCESS;
}

nvmlReturn_t single_threaded_collection(gpu_env_struct *env, unsigned char monitor_compute_procs, unsigned char monitor_graphics_procs, unsigned char monitor_pcie_usage, unsigned int interval) {
    unsigned int i;
    unsigned long last_collection_time;
    double last_collection_duration;
    nvmlReturn_t result ;
    struct timeval begin, end;
    long seconds, microseconds;

    while(exit_guard) {
        gettimeofday(&begin, 0);
        last_collection_time = begin.tv_sec;
        
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
                result = getGpuProcessStatistics(&env->devices[i]);
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
            printf("==================================== GPU GENERAL STATS ================================================\n");
            printGpuStatistics(env->devices[i]);
            if (monitor_compute_procs) {
                printf("==================================== GPU COMPUTE PROCESSES ============================================\n");
                printGpuComputeProcessInfos(env->devices[i]);
            }
            if (monitor_graphics_procs){
                printf("==================================== GPU PROCESS STATS ================================================\n");
                printGpuProcessStatistics(env->devices[i]);
            }
            printf("Time took to finish sampling: %.3f seconds.\n\n", last_collection_duration);
            printf("=======================================================================================================\n");
        }

        sleep(interval);
        
        //TODO: publish and write to file
        /*if (data->output != NULL)
            printf("output: %s\n", output);
        if (data->publish_url != NULL)
            printf("publish: %s\n", publish_url);
        */
    }

    return NVML_SUCCESS;
}

int main(int argc, char *argv[]) {
    unsigned int i;
    nvmlReturn_t result;
    gpu_env_struct env;
    unsigned int interval = 10;
    static unsigned char monitor_compute_procs = 0;
    static unsigned char monitor_graphics_procs = 0;
    static unsigned char monitor_pcie_usage = 0;
    static unsigned char is_multi_threaded = 0;
    char *output = NULL;
    char *publish_url = NULL;

    pthread_t *threads = NULL;
    thread_data *threads_data = NULL;

    signal(SIGINT, signal_handler);

    int opt;
    int option_index;
    static struct option long_options[] = {
        {"bus", 0, &monitor_pcie_usage, 1},
        {"compute_procs", 0, &monitor_compute_procs, 1},
        {"graphics_procs", 0, &monitor_graphics_procs, 1},
        {"multi_threaded", 0, &is_multi_threaded, 1},
        {"output", 1, 0, 'o'},
        {"publish", 1, 0, 'p'},
        {"interval", 1, 0, 'i'},
        {"help", 0, 0, 'h'},
        {0, 0, 0, 0}
    };

    while((opt = getopt_long(argc, argv, ":o:p:i:bcgmh", long_options, &option_index)) != -1) { 
        switch(opt) {
            case 0:
                break;
            case 'i':
                interval = atoi(optarg);
                break;
            case 'o': 
                output = optarg;
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
            case 'm': 
                is_multi_threaded = 1;
                break; 
            case 'h': 
                printf("Usage: ./nvidia-monitor [-bcgm] [-o <filename>] [-p <publish_url>] [-i <interval>]\n");
                return 0;
                break; 
            case ':': 
                printf("Option needs a value\n"); 
                printf("Usage: ./nvidia-monitor [-bcgm] [-o <filename>] [-p <publish_url>] [-i <interval>]\n");
                return 1;
            case '?': 
                printf("Unknown option: %c\n", optopt);
                printf("Usage: ./nvidia-monitor [-bcgm] [-o <filename>] [-p <publish_url>] [-i <interval>]\n");
                return 1;
        }
    }

    /*printf("bus: %u\n", monitor_pcie_usage);
    printf("cuda: %u\n", monitor_compute_procs);
    printf("graphx: %u\n", monitor_graphics_procs);
    printf("multi threaded: %u\n", is_multi_threaded);
    printf("interval: %u\n", interval);
    if (output != NULL)
        printf("output: %s\n", output);
    if (publish_url != NULL)
        printf("publish: %s\n", publish_url);
    */

    // First initialize NVML library
    result = nvmlInit();
    if (result != NVML_SUCCESS) { 
        printf("Failed to initialize NVML: %s\n", nvmlErrorString(result));
        return 1;
    }

    env.devices = NULL;
    result = getGpuEnvironment(&env);
    if (result != NVML_SUCCESS)
      goto Error;
    
    printf("============================================= GPU ENV =====================================================\n");
    printGpuEnvironment(env);
    printf("===========================================================================================================\n\n");
    
    if (is_multi_threaded) {
        threads = (pthread_t*) malloc(env.device_count * sizeof(pthread_t));
        threads_data = (thread_data*) malloc(env.device_count * sizeof(thread_data));
        
        if (threads == NULL || threads_data == NULL) {
            printf("Not enough memory for threads\n");
            goto Error;
        }

        for (i=0; i<env.device_count; i++) {
            threads_data[i].i = i;
            threads_data[i].env = &env;
            threads_data[i].last_collection_time = 0;
            threads_data[i].last_collection_duration = 0;
            threads_data[i].monitor_compute_procs = monitor_compute_procs;
            threads_data[i].monitor_graphics_procs = monitor_graphics_procs;
            threads_data[i].monitor_pcie_usage = monitor_pcie_usage;
            threads_data[i].output = output;
            threads_data[i].publish_url = publish_url;
        }
        
        result = multi_threaded_collection(&env, threads, threads_data, interval);
        
        if (threads != NULL) free(threads);
        if (threads_data != NULL) free(threads_data);

        if (result != NVML_SUCCESS)
            goto Error;
    }
    else {
        result = single_threaded_collection(&env, monitor_compute_procs, monitor_graphics_procs, monitor_pcie_usage, interval);
        if (result != NVML_SUCCESS)
            goto Error;
    }

    printf("==================================== GPU MAX STATS ================================================\n");
    for (i=0; i<env.device_count; i++)
        printGpuMaxMeasurements(env.devices[i]);
    printf("=======================================================================================================\n\n");
    
    if (output != NULL) free(output);
    if (publish_url != NULL) free(publish_url);
    nvml_monitoring_cleanup(&env);

    result = nvmlShutdown();
    if (result != NVML_SUCCESS)
        printf("Failed to shutdown NVML: %s\n", nvmlErrorString(result));

    printf("All done.\n");

    return 0;

Error:
    if (output != NULL) free(output);
    if (publish_url != NULL) free(publish_url);
    nvml_monitoring_cleanup(&env);
   
    result = nvmlShutdown();
    if (result != NVML_SUCCESS)
        printf("Failed to shutdown NVML: %s\n", nvmlErrorString(result));

    return 1;
}
