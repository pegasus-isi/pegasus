#ifndef NVIDIA_MONITOR_H
#define NVIDIA_MONITOR_H

typedef struct {
    unsigned int index;
    nvmlPciInfo_t pci;
    nvmlDevice_t device;
    nvmlMemory_t memory;
    nvmlUtilization_t utilization;
    nvmlComputeMode_t compute_mode;
    int cuda_capability_major;
    int cuda_capability_minor;
    unsigned int temp;
    unsigned int power_limit;
    unsigned int power_usage;
    unsigned short int is_cuda_capable;
    int clocks[NVML_CLOCK_COUNT];
    int max_clocks[NVML_CLOCK_COUNT];
    char name[NVML_DEVICE_NAME_BUFFER_SIZE];
} gpu_dev_info_struct;

typedef struct {
    unsigned int device_count;
    int cuda_version;
    char driver_version[NVML_SYSTEM_DRIVER_VERSION_BUFFER_SIZE];
    gpu_dev_info_struct devices[20];
} gpu_env_struct;

/*static const char * convertToComputeModeString(nvmlComputeMode_t mode) {
    switch (mode)
    {
        case NVML_COMPUTEMODE_DEFAULT:
            return "Default";
        case NVML_COMPUTEMODE_EXCLUSIVE_THREAD:
            return "Exclusive_Thread";
        case NVML_COMPUTEMODE_PROHIBITED:
            return "Prohibited";
        case NVML_COMPUTEMODE_EXCLUSIVE_PROCESS:
            return "Exclusive Process";
        default:
            return "Unknown";
    }
}*/

nvmlReturn_t getGpuEnvironment(gpu_env_struct *env) {
    nvmlReturn_t result;
    unsigned int i, k;

    /*CUDA Version*/
    result=nvmlSystemGetCudaDriverVersion_v2(&env->cuda_version);
    if (result != NVML_SUCCESS)
    { 
        printf("Failed to get cuda version: %s\n", nvmlErrorString(result));
        return result;
    }
    
    /*Driver Version*/
    result=nvmlSystemGetDriverVersion(env->driver_version, NVML_SYSTEM_DRIVER_VERSION_BUFFER_SIZE);
    if (result != NVML_SUCCESS)
    { 
        printf("Failed to get system driver version: %s\n", nvmlErrorString(result));
        return result;
    }

    result = nvmlDeviceGetCount(&env->device_count);
    if (result != NVML_SUCCESS)
    { 
        printf("Failed to query device count: %s\n", nvmlErrorString(result));
        return result;
    }
    
    for (i = 0; i < env->device_count; i++)
    {
        env->devices[i].index = i;
        env->devices[i].cuda_capability_major = 0;
        env->devices[i].cuda_capability_minor = 0;
        env->devices[i].is_cuda_capable = 1;

        // Query for device handle to perform operations on a device
        // You can also query device handle by other features like:
        // nvmlDeviceGetHandleBySerial
        // nvmlDeviceGetHandleByPciBusId
        result = nvmlDeviceGetHandleByIndex(i, &env->devices[i].device);
        if (result != NVML_SUCCESS)
        { 
            printf("Failed to get handle for device %u: %s\n", i, nvmlErrorString(result));
            return result;
        }

        result = nvmlDeviceGetName(env->devices[i].device, env->devices[i].name, NVML_DEVICE_NAME_BUFFER_SIZE);
        if (result != NVML_SUCCESS)
        { 
            printf("Failed to get name of device %u: %s\n", i, nvmlErrorString(result));
            return result;
        }
        
        // pci.busId is very useful to know which device physically you're talking to
        // Using PCI identifier you can also match nvmlDevice handle to CUDA device.
        result = nvmlDeviceGetPciInfo(env->devices[i].device, &env->devices[i].pci);
        if (result != NVML_SUCCESS)
        { 
            printf("Failed to get pci info for device %u: %s\n", i, nvmlErrorString(result));
            return result;
        }

        result = nvmlDeviceGetComputeMode(env->devices[i].device, &env->devices[i].compute_mode);
        if (NVML_ERROR_NOT_SUPPORTED == result)
            env->devices[i].is_cuda_capable = 0;
        else if (result != NVML_SUCCESS)
        { 
            printf("Failed to get compute mode for device %u: %s\n", i, nvmlErrorString(result));
            return result;
        }

        if (env->devices[i].is_cuda_capable == 1)
        {
            result = nvmlDeviceGetCudaComputeCapability(env->devices[i].device, &env->devices[i].cuda_capability_major, &env->devices[i].cuda_capability_minor);
            if (result != NVML_SUCCESS)
            { 
                printf("Failed to get major and minor cuda capability for device %u: %s\n", i, nvmlErrorString(result));
                return result;
            }
        }

        result = nvmlDeviceGetMemoryInfo(env->devices[i].device, &env->devices[i].memory);
        if (result != NVML_SUCCESS)
        { 
            printf("Failed to get memory info for device %u: %s\n", i, nvmlErrorString(result));
            return result;
        }
        
        result = nvmlDeviceGetEnforcedPowerLimit(env->devices[i].device, &env->devices[i].power_limit);
        if (result != NVML_SUCCESS)
        { 
            printf("Failed to get power limit for device %u: %s\n", i, nvmlErrorString(result));
            return result;
        }
        
        result = nvmlDeviceGetTemperature(env->devices[i].device, NVML_TEMPERATURE_GPU, &env->devices[i].temp);
        if (result != NVML_SUCCESS)
        { 
            printf("Failed to get temperature for device %u: %s\n", i, nvmlErrorString(result));
            return result;
        }
        
        for (k = 0; k < NVML_CLOCK_COUNT; k++)
        {
            result = nvmlDeviceGetMaxClockInfo(env->devices[i].device, k, &env->devices[i].max_clocks[k]);
            if (result != NVML_SUCCESS)
            { 
                printf("Failed to get max clock speeds for device %u: %s\n", i, nvmlErrorString(result));
                return result;
            }
        }


        return result;
    }
}


nvmlReturn_t getGpuStatistics(gpu_dev_info_struct *device) {
    nvmlReturn_t result;
    unsigned int k;

    result = nvmlDeviceGetTemperature(device->device, NVML_TEMPERATURE_GPU, &device->temp);
    if (result != NVML_SUCCESS)
    { 
        printf("Failed to get temperature for device %u: %s\n", device->index, nvmlErrorString(result));
        return result;
    }
        
    result = nvmlDeviceGetMemoryInfo(device->device, &device->memory);
    if (result != NVML_SUCCESS)
    { 
        printf("Failed to get memory info for device %u: %s\n", device->index, nvmlErrorString(result));
        return result;
    }
        
    result = nvmlDeviceGetPowerUsage(device->device, &device->power_usage);
    if (result != NVML_SUCCESS)
    { 
        printf("Failed to get power usage for device %u: %s\n", device->index, nvmlErrorString(result));
        return result;
    }

    result = nvmlDeviceGetUtilizationRates(device->device, &device->utilization);
    if (result != NVML_SUCCESS)
    { 
        printf("Failed to get utilization rates for device %u: %s\n", device->index, nvmlErrorString(result));
        return result;
    }

    for (k = 0; k < NVML_CLOCK_COUNT; k++)
    {
        result = nvmlDeviceGetClockInfo(device->device, k, &device->clocks[k]);
        if (result != NVML_SUCCESS)
        { 
            printf("Failed to get max clock speeds for device %u: %s\n", device->index, nvmlErrorString(result));
            return result;
        }
    }
    
    return result;
}


nvmlReturn_t getGpuStatisticsByID(unsigned int i, gpu_env_struct *env) {
    nvmlReturn_t result = getGpuStatistics(&env->devices[i]);

    return result;
}


nvmlReturn_t getGpuStatisticsAll(gpu_env_struct *env) {
    unsigned int i;
    nvmlReturn_t result;

    for (i = 0; i < env->device_count; i++)
    {
        result = getGpuStatistics(&env->devices[i]);
        if (result != NVML_SUCCESS)
            break;
    }

    return result;
}


void printGpuStatistics(gpu_dev_info_struct device) {
    printf("%u. %s [%s]\n", device.index, device.name, device.pci.busId);
    printf("\t Tempearture %d C\n", device.temp);
    printf("\t Power Usage %d Watt\n", device.power_usage/1000);
    printf("\t GPU Utilization %u%%, Memory Utilization %u%%\n", device.utilization.gpu, device.utilization.memory);
    printf("\t Memory Used %lld MBytes, Memory Total %lld MBytes\n", device.memory.used/(1024*1024), device.memory.total/(1024*1024));
    printf("\t GPU Clock %dMHz, SM Clock %dMHz, Mem Clock %dMHz, Video Clock %dMHz\n", device.clocks[NVML_CLOCK_GRAPHICS], device.clocks[NVML_CLOCK_SM], device.clocks[NVML_CLOCK_MEM], device.clocks[NVML_CLOCK_VIDEO]);

    return;
}


void printGpuStatisticsByID(unsigned int i, gpu_env_struct env) {
    printGpuStatistics(env.devices[i]);

    return;
}

void printGpuEnvironment(gpu_env_struct env) {
    unsigned int i;

    printf("Cuda version is %d.%d\n", env.cuda_version/1000, env.cuda_version%1000/10);
    printf("System driver version is %s\n", env.driver_version);
    printf("Found %u device%s\n", env.device_count, env.device_count != 1 ? "s" : "");

    printf("Listing devices:\n");    
    for (i = 0; i < env.device_count; i++)
    {
        printf("%u. %s [%s]\n", i, env.devices[i].name, env.devices[i].pci.busId);
        if (env.devices[i].is_cuda_capable == 0)
            printf("\t This is not CUDA capable device\n");
        else {
            printf("\t Cuda Capability %d.%d\n", env.devices[i].cuda_capability_major, env.devices[i].cuda_capability_minor);
        }
        printf("\t Tempearture %d C\n", env.devices[i].temp);
        printf("\t Power limit %d Watt\n", env.devices[i].power_limit/1000);
        printf("\t Total Memory %lld MBytes\n", env.devices[i].memory.total/(1024*1024));
        printf("\t Max GPU Clock %dMHz, Max SM Clock %dMHz, Max Mem Clock %dMHz, Max Video Clock %dMHz\n", env.devices[i].max_clocks[NVML_CLOCK_GRAPHICS], env.devices[i].max_clocks[NVML_CLOCK_SM], env.devices[i].max_clocks[NVML_CLOCK_MEM], env.devices[i].max_clocks[NVML_CLOCK_VIDEO]);
    }

    return;
}

#endif
