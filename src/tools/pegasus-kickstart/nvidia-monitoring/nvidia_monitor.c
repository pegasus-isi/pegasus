#include <stdio.h>
#include <unistd.h>
#include <signal.h>
#include <nvml.h>
#include "nvidia_monitor.h"

static volatile int exit_guard = 1;

void signal_handler(int signum) {
    exit_guard = 0;
}

int main(void) {
    unsigned int i;
    nvmlReturn_t result;
    gpu_env_struct env;

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
    
    printf("============================== GPU ENV =============================\n");
    printGpuEnvironment(env);
    printf("====================================================================\n\n\n");
    
    while(exit_guard)
    {
        result = getGpuStatistics(&env.devices[0]);
        if (result != NVML_SUCCESS)
          goto Error;
        printf("============================== GPU GENERAL STATS =============================\n");
        printGpuStatistics(env.devices[0]);
        
        /*result = getGpuComputeStatistics(&env.devices[0]);
        if (result != NVML_SUCCESS)
          goto Error;
        printf("============================== GPU COMPUTE STATS =============================\n");
        printGpuComputeStatistics(env.devices[0]);
        printf("==============================================================================\n");
        */
        result = getGpuProcessStatistics(&env.devices[0]);
        if (result != NVML_SUCCESS)
          goto Error;
        printf("============================== GPU PROCESS STATS =============================\n");
        printGpuProcessStatistics(env.devices[0]);
        printf("==============================================================================\n\n");
        
        sleep(2);
    }

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
