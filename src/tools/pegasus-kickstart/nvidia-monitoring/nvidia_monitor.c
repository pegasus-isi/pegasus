#include <stdio.h>
#include <unistd.h>
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
    gpu_env_struct *env;
    nvmlReturn_t result;
} thread_data;

void *collect_gpu_stats(void *args) {
    thread_data *data = (thread_data *)args;
    data->result = getGpuStatistics(&data->env->devices[data->i]);
    if (data->result != NVML_SUCCESS)
        pthread_exit(NULL);
        
    data->result = getGpuComputeProcesses(&data->env->devices[data->i]);
    if (data->result != NVML_SUCCESS)
        pthread_exit(NULL);
        
    data->result = getGpuProcessStatistics(&data->env->devices[data->i]);
    if (data->result != NVML_SUCCESS)
        pthread_exit(NULL);

    pthread_exit(NULL);
}

int main(void) {
    unsigned int i;
    nvmlReturn_t result;
    gpu_env_struct env;
    pthread_t *threads;
    thread_data *threads_data;

    signal(SIGINT, signal_handler);

    // First initialize NVML library
    result = nvmlInit();
    if (result != NVML_SUCCESS)
    { 
        printf("Failed to initialize NVML: %s\n", nvmlErrorString(result));

        printf("Press ENTER to continue...\n");
        getchar();
        return 1;
    }

    result = getGpuEnvironment(&env);
    if (result != NVML_SUCCESS)
      goto Error;
    
    printf("============================================= GPU ENV =====================================================\n");
    printGpuEnvironment(env);
    printf("===========================================================================================================\n\n");
    
    threads = (pthread_t*) malloc(env.device_count * sizeof(pthread_t));
    threads_data = (thread_data*) malloc(env.device_count * sizeof(thread_data));

    for (i=0; i<env.device_count; i++)
    {
        threads_data[i].i = i;
        threads_data[i].env = &env;
    }

    while(exit_guard)
    {
        struct timeval begin, end;
        gettimeofday(&begin, 0);
        
        for (i=0; i<env.device_count; i++)
            pthread_create(&threads[i], NULL, collect_gpu_stats, &threads_data[i]);

        for (i=0; i<env.device_count; i++)
        {
            pthread_join(threads[i], NULL);
            if (threads_data[i].result != NVML_SUCCESS)
                goto Error;
        }

        for (i=0; i<env.device_count; i++)
        {
            printf("==================================== GPU GENERAL STATS ================================================\n");
            printGpuStatistics(env.devices[i]);  
            printf("==================================== GPU COMPUTE PROCESSES ============================================\n");
            printGpuComputeProcessInfos(env.devices[i]);
            printf("==================================== GPU PROCESS STATS ================================================\n");
            printGpuProcessStatistics(env.devices[i]);
            printf("=======================================================================================================\n");
        }

        gettimeofday(&end, 0);
        long seconds = end.tv_sec - begin.tv_sec;
        long microseconds = end.tv_usec - begin.tv_usec;
        double elapsed = seconds + microseconds*1e-6;
        printf("Time took to finish sampling: %.3f seconds.\n\n", elapsed);

        sleep(5);
    }

    printf("==================================== GPU MAX STATS ================================================\n");
    for (i=0; i<env.device_count; i++)
        printGpuMaxMeasurements(env.devices[i]);
    printf("=======================================================================================================\n\n");
    
    result = nvmlShutdown();
    if (result != NVML_SUCCESS)
        printf("Failed to shutdown NVML: %s\n", nvmlErrorString(result));

    printf("All done.\n");

//    printf("Press ENTER to continue...\n");
//    getchar();
    return 0;

Error:
    result = nvmlShutdown();
    if (result != NVML_SUCCESS)
        printf("Failed to shutdown NVML: %s\n", nvmlErrorString(result));

    printf("Press ENTER to continue...\n");
    getchar();
    return 1;
}
