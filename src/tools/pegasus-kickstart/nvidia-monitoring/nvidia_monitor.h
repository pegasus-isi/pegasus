#ifndef NVIDIA_MONITOR_H
#define NVIDIA_MONITOR_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <nvml.h>


typedef struct {
    nvmlProcessInfo_t *infos;
    unsigned int count;
    unsigned long long last_ts;
} gpu_process_infos;


typedef struct {
    nvmlProcessUtilizationSample_t *samples;
    unsigned int count;
    unsigned long long last_ts;
} gpu_process_samples;


typedef struct {
    unsigned int max_temp;
    unsigned int max_power_usage;
    unsigned int max_gpu_utilization;
    unsigned long long max_mem_usage;
    unsigned long long max_bar1mem_usage;
} gpu_max_measurements;


typedef struct {
    unsigned int index;
    unsigned long long last_ts;
    nvmlPciInfo_t pci;
    nvmlDevice_t device;
    nvmlMemory_t memory;
    nvmlBAR1Memory_t bar1memory;
    nvmlUtilization_t utilization;
    nvmlComputeMode_t compute_mode;
    gpu_process_infos compute_proc_infos;
    gpu_process_infos graphics_proc_infos;
    gpu_process_samples proc_samples;
    gpu_max_measurements max_measurements;
    int cuda_capability_major;
    int cuda_capability_minor;
    unsigned int pcie_tx;
    unsigned int pcie_rx;
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
    gpu_dev_info_struct *devices;
} gpu_env_struct;


static const char* convertToComputeModeString(nvmlComputeMode_t mode)
{
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
}


nvmlReturn_t getGpuEnvironment(gpu_env_struct *env) {
    nvmlReturn_t result;
    unsigned int i, k;

    env->devices = NULL;
    
    /*CUDA Version*/
    result=nvmlSystemGetCudaDriverVersion_v2(&env->cuda_version);
    if (result != NVML_SUCCESS) { 
        fprintf(stderr, "Failed to get cuda version: %s\n", nvmlErrorString(result));
        return result;
    }
    
    /*Driver Version*/
    result=nvmlSystemGetDriverVersion(env->driver_version, NVML_SYSTEM_DRIVER_VERSION_BUFFER_SIZE);
    if (result != NVML_SUCCESS) { 
        fprintf(stderr, "Failed to get system driver version: %s\n", nvmlErrorString(result));
        return result;
    }

    result = nvmlDeviceGetCount(&env->device_count);
    if (result != NVML_SUCCESS) { 
        fprintf(stderr, "Failed to query device count: %s\n", nvmlErrorString(result));
        return result;
    }

    env->devices = (gpu_dev_info_struct*) malloc(env->device_count * sizeof(gpu_dev_info_struct));
    if (env->devices == NULL) {
        fprintf(stderr, "Failed to allocate memory for devices\n");
        return -1;
    }
        
    
    for (i = 0; i < env->device_count; i++) {
        env->devices[i].index = i;
        env->devices[i].last_ts = 0;

        env->devices[i].pcie_rx = 0;
        env->devices[i].pcie_tx = 0;

        env->devices[i].compute_proc_infos.infos = NULL;
        env->devices[i].compute_proc_infos.count = 0;
        env->devices[i].compute_proc_infos.last_ts = 0;
        
        env->devices[i].graphics_proc_infos.infos = NULL;
        env->devices[i].graphics_proc_infos.count = 0;
        env->devices[i].graphics_proc_infos.last_ts = 0;
      
        env->devices[i].proc_samples.samples = NULL;
        env->devices[i].proc_samples.count = 0;
        env->devices[i].proc_samples.last_ts = 0;

        env->devices[i].cuda_capability_major = 0;
        env->devices[i].cuda_capability_minor = 0;
        env->devices[i].is_cuda_capable = 1;

        env->devices[i].max_measurements.max_temp = 0;
        env->devices[i].max_measurements.max_power_usage = 0;
        env->devices[i].max_measurements.max_mem_usage = 0;
        env->devices[i].max_measurements.max_bar1mem_usage = 0;
        env->devices[i].max_measurements.max_gpu_utilization = 0;

        // Query for device handle to perform operations on a device
        // You can also query device handle by other features like:
        // nvmlDeviceGetHandleBySerial
        // nvmlDeviceGetHandleByPciBusId
        result = nvmlDeviceGetHandleByIndex(i, &env->devices[i].device);
        if (result != NVML_SUCCESS) { 
            fprintf(stderr, "Failed to get handle for device %u: %s\n", i, nvmlErrorString(result));
            return result;
        }

        result = nvmlDeviceGetName(env->devices[i].device, env->devices[i].name, NVML_DEVICE_NAME_BUFFER_SIZE);
        if (result != NVML_SUCCESS) { 
            fprintf(stderr, "Failed to get name of device %u: %s\n", i, nvmlErrorString(result));
            return result;
        }
        
        // pci.busId is very useful to know which device physically you're talking to
        // Using PCI identifier you can also match nvmlDevice handle to CUDA device.
        result = nvmlDeviceGetPciInfo(env->devices[i].device, &env->devices[i].pci);
        if (result != NVML_SUCCESS) { 
            fprintf(stderr, "Failed to get pci info for device %u: %s\n", i, nvmlErrorString(result));
            return result;
        }

        result = nvmlDeviceGetComputeMode(env->devices[i].device, &env->devices[i].compute_mode);
        if (NVML_ERROR_NOT_SUPPORTED == result)
            env->devices[i].is_cuda_capable = 0;
        else if (result != NVML_SUCCESS) { 
            fprintf(stderr, "Failed to get compute mode for device %u: %s\n", i, nvmlErrorString(result));
            return result;
        }

        if (env->devices[i].is_cuda_capable == 1) {
            result = nvmlDeviceGetCudaComputeCapability(env->devices[i].device, &env->devices[i].cuda_capability_major, &env->devices[i].cuda_capability_minor);
            if (result != NVML_SUCCESS)
            { 
                fprintf(stderr, "Failed to get major and minor cuda capability for device %u: %s\n", i, nvmlErrorString(result));
                return result;
            }
        }

        result = nvmlDeviceGetMemoryInfo(env->devices[i].device, &env->devices[i].memory);
        if (result != NVML_SUCCESS) { 
            fprintf(stderr, "Failed to get memory info for device %u: %s\n", i, nvmlErrorString(result));
            return result;
        }
        
        result = nvmlDeviceGetEnforcedPowerLimit(env->devices[i].device, &env->devices[i].power_limit);
        if (result != NVML_SUCCESS) { 
            fprintf(stderr, "Failed to get power limit for device %u: %s\n", i, nvmlErrorString(result));
            return result;
        }
        
        result = nvmlDeviceGetTemperature(env->devices[i].device, NVML_TEMPERATURE_GPU, &env->devices[i].temp);
        if (result != NVML_SUCCESS) { 
            fprintf(stderr, "Failed to get temperature for device %u: %s\n", i, nvmlErrorString(result));
            return result;
        }
        
        for (k = 0; k < NVML_CLOCK_COUNT; k++) {
            result = nvmlDeviceGetMaxClockInfo(env->devices[i].device, k, &env->devices[i].max_clocks[k]);
            if (result != NVML_SUCCESS) { 
                fprintf(stderr, "Failed to get max clock speeds for device %u: %s\n", i, nvmlErrorString(result));
                return result;
            }
        }
    }

    return result;
}


nvmlReturn_t getGpuStatistics(gpu_dev_info_struct *device, unsigned char monitor_pcie_usage) {
    nvmlReturn_t result;
    unsigned int k;

    device->last_ts = (unsigned long long) time(NULL);
    
    result = nvmlDeviceGetTemperature(device->device, NVML_TEMPERATURE_GPU, &device->temp);
    if (result != NVML_SUCCESS) { 
        fprintf(stderr, "Failed to get temperature for device %u: %s\n", device->index, nvmlErrorString(result));
        return result;
    }
    if (device->temp > device->max_measurements.max_temp)
        device->max_measurements.max_temp = device->temp;
        
    result = nvmlDeviceGetPowerUsage(device->device, &device->power_usage);
    if (result != NVML_SUCCESS) { 
        fprintf(stderr, "Failed to get power usage for device %u: %s\n", device->index, nvmlErrorString(result));
        return result;
    }
    if (device->power_usage > device->max_measurements.max_power_usage)
        device->max_measurements.max_power_usage = device->power_usage;
    
    result = nvmlDeviceGetBAR1MemoryInfo (device->device, &device->bar1memory);
    if (result != NVML_SUCCESS) { 
        fprintf(stderr, "Failed to get bar1Memory info for device %u: %s\n", device->index, nvmlErrorString(result));
        return result;
    }
    if (device->bar1memory.bar1Used > device->max_measurements.max_bar1mem_usage)
        device->max_measurements.max_bar1mem_usage = device->bar1memory.bar1Used;

    result = nvmlDeviceGetMemoryInfo(device->device, &device->memory);
    if (result != NVML_SUCCESS) { 
        fprintf(stderr, "Failed to get memory info for device %u: %s\n", device->index, nvmlErrorString(result));
        return result;
    }
    if (device->memory.used > device->max_measurements.max_mem_usage)
        device->max_measurements.max_mem_usage = device->memory.used;
        
    result = nvmlDeviceGetUtilizationRates(device->device, &device->utilization);
    if (result != NVML_SUCCESS) { 
        fprintf(stderr, "Failed to get utilization rates for device %u: %s\n", device->index, nvmlErrorString(result));
        return result;
    }
    if (device->utilization.gpu > device->max_measurements.max_gpu_utilization)
        device->max_measurements.max_gpu_utilization = device->utilization.gpu;

    for (k = 0; k < NVML_CLOCK_COUNT; k++) {
        result = nvmlDeviceGetClockInfo(device->device, k, &device->clocks[k]);
        if (result != NVML_SUCCESS) { 
            fprintf(stderr, "Failed to get max clock speeds for device %u: %s\n", device->index, nvmlErrorString(result));
            return result;
        }
    }

    if (monitor_pcie_usage) {
        result = nvmlDeviceGetPcieThroughput(device->device, NVML_PCIE_UTIL_TX_BYTES, &device->pcie_tx);
        if (result != NVML_SUCCESS) { 
            fprintf(stderr, "Failed to get pcie tx utilization rates for device %u: %s\n", device->index, nvmlErrorString(result));
            return result;
        }
    
        result = nvmlDeviceGetPcieThroughput(device->device, NVML_PCIE_UTIL_RX_BYTES, &device->pcie_rx);
        if (result != NVML_SUCCESS) { 
            fprintf(stderr, "Failed to get pcie rx utilization rates for device %u: %s\n", device->index, nvmlErrorString(result));
            return result;
        }
    }

    return result;
}


nvmlReturn_t getGpuStatisticsByID(unsigned int i, gpu_env_struct *env, unsigned char monitor_pcie_usage) {
    nvmlReturn_t result = getGpuStatistics(&env->devices[i], monitor_pcie_usage);

    return result;
}


nvmlReturn_t getGpuStatisticsAll(gpu_env_struct *env, unsigned char monitor_pcie_usage) {
    unsigned int i;
    nvmlReturn_t result;

    for (i = 0; i < env->device_count; i++) {
        result = getGpuStatistics(&env->devices[i], monitor_pcie_usage);
        if (result != NVML_SUCCESS)
            break;
    }

    return result;
}


nvmlReturn_t getGpuProcessUtilization(gpu_dev_info_struct *device) {
    nvmlReturn_t result;
    unsigned int i;
    unsigned int tmp_cnt = 0;
    unsigned long long tmp_last_ts = device->proc_samples.last_ts;
    nvmlProcessUtilizationSample_t *tmp_samples = NULL;
    
    //determine size of buffer
    result = nvmlDeviceGetProcessUtilization(device->device, NULL, &tmp_cnt, tmp_last_ts);
    if (result != NVML_SUCCESS && result != NVML_ERROR_INSUFFICIENT_SIZE) { 
        fprintf(stderr, "Failed to get num of process samples for device %u: %s\n", device->index, nvmlErrorString(result));
        return result;
    }
    
    if (tmp_cnt > 0) {
        //allocate space to store them
        tmp_samples = (nvmlProcessUtilizationSample_t*) malloc(tmp_cnt * sizeof(nvmlProcessUtilizationSample_t));
        if (tmp_samples == NULL) {
            fprintf(stderr, "Failed to allocate memory for samples\n");
            return -1;
        }
    
        //retrieve new samples
        result = nvmlDeviceGetProcessUtilization(device->device, tmp_samples, &tmp_cnt, tmp_last_ts);
        if (result != NVML_SUCCESS) { 
            fprintf(stderr, "Failed to get process samples for device %u: %s\n", device->index, nvmlErrorString(result));
            return result;
        }

        for (i = 0; i < tmp_cnt; i++){
            if ((tmp_samples[i].pid > 0) && (tmp_last_ts < tmp_samples[i].timeStamp))
                tmp_last_ts = tmp_samples[i].timeStamp;
        }
    }

    if (device->proc_samples.samples != NULL)
        free(device->proc_samples.samples);
    
    device->proc_samples.samples = tmp_samples;
    device->proc_samples.count = tmp_cnt;
    device->proc_samples.last_ts = tmp_last_ts;
    
    return result;
}


nvmlReturn_t getGpuProcessUtilizationByID(unsigned int i, gpu_env_struct *env) {
    nvmlReturn_t result = getGpuProcessUtilization(&env->devices[i]);

    return result;
}


nvmlReturn_t getGpuProcessUtilizationAll(gpu_env_struct *env) {
    unsigned int i;
    nvmlReturn_t result;

    for (i = 0; i < env->device_count; i++) {
        result = getGpuProcessUtilization(&env->devices[i]);
        if (result != NVML_SUCCESS)
            break;
    }

    return result;
}


nvmlReturn_t getGpuComputeProcesses(gpu_dev_info_struct *device) {
    nvmlReturn_t result;
    unsigned int i;
    unsigned int tmp_cnt = 0;
    nvmlProcessInfo_t *tmp_infos = NULL;
    
    //determine size of buffer
    result = nvmlDeviceGetComputeRunningProcesses(device->device, &tmp_cnt, NULL);
    if (result != NVML_SUCCESS && result != NVML_ERROR_INSUFFICIENT_SIZE) { 
        fprintf(stderr, "Failed to get num of compute processes for device %u: %s\n", device->index, nvmlErrorString(result));
        return result;
    }
    
    if (tmp_cnt > 0) {
        //allocate space to store them
        tmp_infos = (nvmlProcessInfo_t*) malloc(tmp_cnt * sizeof(nvmlProcessInfo_t));
        if (tmp_infos == NULL) {
            fprintf(stderr, "Failed to allocate memory for infos\n");
            return -1;
        }
    
        //retrieve compute processes
        result = nvmlDeviceGetComputeRunningProcesses(device->device, &tmp_cnt, tmp_infos);
        if (result != NVML_SUCCESS) { 
            fprintf(stderr, "Failed to get compute processes for device %u: %s\n", device->index, nvmlErrorString(result));
            return result;
        }
    }
    
    if (device->compute_proc_infos.infos != NULL)
        free(device->compute_proc_infos.infos);
    
    device->compute_proc_infos.infos = tmp_infos;
    device->compute_proc_infos.count = tmp_cnt;
    device->compute_proc_infos.last_ts = (unsigned long long) time(NULL);
    
    return result;
}


nvmlReturn_t getGpuComputeProcessesByID(unsigned int i, gpu_env_struct *env) {
    nvmlReturn_t result = getGpuComputeProcesses(&env->devices[i]);
    
    return result;
}


nvmlReturn_t getGpuComputeProcessesAll(gpu_env_struct *env) {
    unsigned int i;
    nvmlReturn_t result;

    for (i = 0; i < env->device_count; i++) {
        result = getGpuComputeProcesses(&env->devices[i]);
        if (result != NVML_SUCCESS)
            break;
    }

    return result;
}


nvmlReturn_t getGpuGraphicsProcesses(gpu_dev_info_struct *device) {
    nvmlReturn_t result;
    unsigned int i;
    unsigned int tmp_cnt = 0;
    nvmlProcessInfo_t *tmp_infos = NULL;
    
    //determine size of buffer
    result = nvmlDeviceGetGraphicsRunningProcesses(device->device, &tmp_cnt, NULL);
    if (result != NVML_SUCCESS && result != NVML_ERROR_INSUFFICIENT_SIZE) { 
        fprintf(stderr, "Failed to get num of graphics processes for device %u: %s\n", device->index, nvmlErrorString(result));
        return result;
    }
    
    if (tmp_cnt > 0) {
        //allocate space to store them
        tmp_infos = (nvmlProcessInfo_t*) malloc(tmp_cnt * sizeof(nvmlProcessInfo_t));
        if (tmp_infos == NULL) {
            fprintf(stderr, "Failed to allocate memory for infos\n");
            return -1;
        }
    
        //retrieve compute processes
        result = nvmlDeviceGetGraphicsRunningProcesses(device->device, &tmp_cnt, tmp_infos);
        if (result != NVML_SUCCESS) { 
            fprintf(stderr, "Failed to get graphics processes for device %u: %s\n", device->index, nvmlErrorString(result));
            return result;
        }
    }
    
    if (device->graphics_proc_infos.infos != NULL)
        free(device->graphics_proc_infos.infos);
    
    device->graphics_proc_infos.infos = tmp_infos;
    device->graphics_proc_infos.count = tmp_cnt;
    device->graphics_proc_infos.last_ts = (unsigned long long) time(NULL);
    
    return result;
}


nvmlReturn_t getGpuGraphicsProcessesByID(unsigned int i, gpu_env_struct *env) {
    nvmlReturn_t result = getGpuGraphicsProcesses(&env->devices[i]);
    
    return result;
}


nvmlReturn_t getGpuGraphicsProcessesAll(gpu_env_struct *env) {
    unsigned int i;
    nvmlReturn_t result;

    for (i = 0; i < env->device_count; i++) {
        result = getGpuGraphicsProcesses(&env->devices[i]);
        if (result != NVML_SUCCESS)
            break;
    }

    return result;
}


void printGpuStatistics(FILE *out, gpu_dev_info_struct device) {
    if (out == NULL)
        out = stdout;

    fprintf(out, "==================================== GPU GENERAL STATS ====================================================\n");
    fprintf(out, "%u. %s [%s]\n", device.index, device.name, device.pci.busId);
    fprintf(out, "\t Tempearture %d C\n", device.temp);
    fprintf(out, "\t Power Usage %d Watt\n", device.power_usage/1000);
    fprintf(out, "\t GPU Utilization %u%%, Memory Utilization %u%%\n", device.utilization.gpu, device.utilization.memory);
    fprintf(out, "\t PCIe RX Utilization %u KB/s, PCIe TX Utilization %u KB/s\n", device.pcie_rx, device.pcie_tx);
    fprintf(out, "\t Memory Used %llu MBytes, Memory Total %llu MBytes\n", device.memory.used/(1024*1024), device.memory.total/(1024*1024));
    fprintf(out, "\t Bar1Memory Used %llu MBytes, Bar1Memory Total %llu MBytes\n", device.bar1memory.bar1Used/(1024*1024), device.bar1memory.bar1Total/(1024*1024));
    fprintf(out, "\t GPU Clock %dMHz, SM Clock %dMHz, Mem Clock %dMHz, Video Clock %dMHz\n", device.clocks[NVML_CLOCK_GRAPHICS], device.clocks[NVML_CLOCK_SM], device.clocks[NVML_CLOCK_MEM], device.clocks[NVML_CLOCK_VIDEO]);

    return;
}


void printGpuStatisticsByID(FILE *out, unsigned int i, gpu_env_struct env) {
    printGpuStatistics(out, env.devices[i]);

    return;
}


void printGpuProcessSamples(FILE *out, gpu_dev_info_struct device) {
    unsigned int i;

    if (out == NULL)
        out = stdout;

    fprintf(out, "==================================== GPU PROCESS SAMPLES =================================================\n");
    fprintf(out, "%u. %s [%s]\n", device.index, device.name, device.pci.busId);
    
    for (i = 0; i < device.proc_samples.count; i++)
        fprintf(out, "\t PID %u (%llu): SM Util %u%% | Mem Util %u%% | Enc Util %u%% | Dec Util %u%%\n", 
                  device.proc_samples.samples[i].pid,
                  device.proc_samples.samples[i].timeStamp,
                  device.proc_samples.samples[i].smUtil,
                  device.proc_samples.samples[i].memUtil,
                  device.proc_samples.samples[i].encUtil, 
                  device.proc_samples.samples[i].decUtil
              );

    return;
}


void printGpuProcessSamplesByID(FILE *out, unsigned int i, gpu_env_struct env) {
    printGpuProcessSamples(out, env.devices[i]);

    return;
}


void printGpuComputeProcessInfos(FILE *out, gpu_dev_info_struct device) {
    unsigned int i;
    
    if (out == NULL)
        out = stdout;

    fprintf(out, "==================================== GPU COMPUTE PROCESSES ===============================================\n");
    fprintf(out, "%u. %s [%s]\n", device.index, device.name, device.pci.busId);
    
    for (i = 0; i < device.compute_proc_infos.count; i++)
        fprintf(out, "\t PID %u (%llu): Memory Utilization %llu Bytes\n", device.compute_proc_infos.infos[i].pid, device.compute_proc_infos.last_ts, device.compute_proc_infos.infos[i].usedGpuMemory);

    return;
}


void printGpuComputeProcessInfosByID(FILE *out, unsigned int i, gpu_env_struct env) {
    printGpuComputeProcessInfos(out, env.devices[i]);

    return;
}


void printGpuGraphicsProcessInfos(FILE *out, gpu_dev_info_struct device) {
    unsigned int i;

    if (out == NULL)
        out = stdout;

    fprintf(out, "==================================== GPU GRAPHICS PROCESSES ==============================================\n");
    fprintf(out, "%u. %s [%s]\n", device.index, device.name, device.pci.busId);
    
    for (i = 0; i < device.graphics_proc_infos.count; i++)
        fprintf(out, "\t PID %u (%llu): Memory Utilization %llu Bytes\n", device.graphics_proc_infos.infos[i].pid, device.graphics_proc_infos.last_ts, device.graphics_proc_infos.infos[i].usedGpuMemory);

    return;
}


void printGpuGraphicsProcessInfosByID(FILE *out, unsigned int i, gpu_env_struct env) {
    printGpuGraphicsProcessInfos(out, env.devices[i]);

    return;
}


void printGpuMaxMeasurements(FILE *out, gpu_env_struct env) {
    unsigned int i;

    if (out == NULL)
        out = stdout;

    fprintf(out, "==================================== GPU MAX STATS =======================================================\n");
    for (i = 0; i < env.device_count; i++) {
        fprintf(out, "%u. %s [%s]\n", env.devices[i].index, env.devices[i].name, env.devices[i].pci.busId);
        fprintf(out, "\t Max Temperature %d C\n", env.devices[i].max_measurements.max_temp);
        fprintf(out, "\t Max Power Usage %d Watt\n", env.devices[i].max_measurements.max_power_usage/1000);
        fprintf(out, "\t Max Bar1 Memory Usage %llu MBytes\n", env.devices[i].max_measurements.max_bar1mem_usage/(1024*1024));
        fprintf(out, "\t Max Memory Usage %llu MBytes\n", env.devices[i].max_measurements.max_mem_usage/(1024*1024));
        fprintf(out, "\t Max GPU Utilizaion %u%%\n", env.devices[i].max_measurements.max_gpu_utilization);
    }
    
    return;
}


void printGpuEnvironment(FILE *out, gpu_env_struct env) {
    unsigned int i;

    if (out == NULL)
        out = stdout;

    fprintf(out, "============================================= GPU ENV =====================================================\n");
    fprintf(out, "Cuda version is %d.%d\n", env.cuda_version/1000, env.cuda_version%1000/10);
    fprintf(out, "System driver version is %s\n", env.driver_version);
    fprintf(out, "Found %u device%s\n", env.device_count, env.device_count != 1 ? "s" : "");

    fprintf(out, "Listing devices:\n");    
    for (i = 0; i < env.device_count; i++) {
        fprintf(out, "%u. %s [%s] - %s\n", i, env.devices[i].name, env.devices[i].pci.busId, convertToComputeModeString(env.devices[i].compute_mode));
        if (env.devices[i].is_cuda_capable == 0)
            fprintf(out, "\t This is not CUDA capable device\n");
        else {
            fprintf(out, "\t Cuda Capability %d.%d\n", env.devices[i].cuda_capability_major, env.devices[i].cuda_capability_minor);
        }
        fprintf(out, "\t Temperature %d C\n", env.devices[i].temp);
        fprintf(out, "\t Power limit %d Watt\n", env.devices[i].power_limit/1000);
        fprintf(out, "\t Total Memory %llu MBytes\n", env.devices[i].memory.total/(1024*1024));
        fprintf(out, "\t Max GPU Clock %dMHz, Max SM Clock %dMHz, Max Mem Clock %dMHz, Max Video Clock %dMHz\n", env.devices[i].max_clocks[NVML_CLOCK_GRAPHICS], env.devices[i].max_clocks[NVML_CLOCK_SM], env.devices[i].max_clocks[NVML_CLOCK_MEM], env.devices[i].max_clocks[NVML_CLOCK_VIDEO]);
    }

    return;
}

void nvml_monitoring_cleanup(gpu_env_struct *env) {
    unsigned int i;

    if (env->devices != NULL) {
        for (i=0; i<env->device_count; i++) {   
            if (env->devices[i].proc_samples.samples != NULL)
                free(env->devices[i].proc_samples.samples);
            if (env->devices[i].compute_proc_infos.infos != NULL)
                free(env->devices[i].compute_proc_infos.infos);
            if (env->devices[i].graphics_proc_infos.infos != NULL)
                free(env->devices[i].graphics_proc_infos.infos);
        }

        free(env->devices);
    }

    return;
}

int json_encode_environment(gpu_env_struct env, char *doc_buffer, size_t maxsize) {
    unsigned int i;
    size_t dev_size;
    char dev_buffer[1024];

    size_t size = snprintf(doc_buffer, maxsize,
            "{\"event\":\"kickstart.inv.gpu.environment\","
            "\"timestamp\":%llu,"
            "\"cuda_version\":%d.%d,"
            "\"nvidia_driver_version\":\"%s\","
            "\"device_count\":%u,"
            "\"devices\":[",
            (unsigned long long) time(NULL),
            env.cuda_version/1000,
            env.cuda_version%1000/10,
            env.driver_version,
            env.device_count
    );
    
    for (i = 0; i < env.device_count; i++) {
        dev_size = snprintf(dev_buffer, 1024,
                    "{\"id\":%u,"
                    "\"name\":\"%s\","
                    "\"pci_bus_id\":\"%s\","
                    "\"compute_mode\":\"%s\","
                    "\"is_cuda_capable\":%s,"
                    "\"cuda_capability\":%d.%d,"
                    "\"power_limit\":%d,"
                    "\"total_bar1_memory\":%llu,"
                    "\"total_memory\":%llu,"
                    "\"max_gpu_clock\":%d,"
                    "\"max_sm_clock\":%d,"
                    "\"max_mem_clock\":%d,"
                    "\"max_video_clock\":%d"
                    "%s",
                    env.devices[i].index,
                    env.devices[i].name,
                    env.devices[i].pci.busId,
                    convertToComputeModeString(env.devices[i].compute_mode),
                    (env.devices[i].is_cuda_capable) ? "true" : "false",
                    env.devices[i].cuda_capability_major, env.devices[i].cuda_capability_minor,
                    env.devices[i].power_limit,
                    env.devices[i].bar1memory.bar1Total,
                    env.devices[i].memory.total,
                    env.devices[i].max_clocks[NVML_CLOCK_GRAPHICS],
                    env.devices[i].max_clocks[NVML_CLOCK_SM],
                    env.devices[i].max_clocks[NVML_CLOCK_MEM],
                    env.devices[i].max_clocks[NVML_CLOCK_VIDEO],
                    (i == (env.device_count - 1)) ? "}" : "},"
        );
        
        if ((dev_size < 0) || (dev_size >= 1024)) {
            fprintf(stderr, "An encoding error occured or JSON too large for local buffer: %ld > %d\n", dev_size, 1024);
            return -1;
        }
        
        //append device at the end of the string
        if ((size + dev_size) < maxsize) {
            strncat(doc_buffer, dev_buffer, dev_size);
            size += dev_size;
        }
        else {
            fprintf(stderr, "JSON too large for buffer: %ld >= %ld\n", size+dev_size, maxsize);
            return -1;
        }
    }

    if ((size + 2) >= maxsize) {
        fprintf(stderr, "JSON too large for buffer: %ld > %ld\n", size + 2, maxsize);
        return -1;
    }
    else {
        strncat(doc_buffer, "]}", 2);
        size += 2;
    }

    return size;
}

int json_encode_device_stats_max(gpu_env_struct env, char *doc_buffer, size_t maxsize) {
    unsigned int i;
    size_t dev_size;
    char dev_buffer[1024];

    size_t size = snprintf(doc_buffer, maxsize,
            "{\"event\":\"kickstart.inv.gpu.stats.max\","
            "\"timestamp\":%llu,"
            "\"devices\":[",
            (unsigned long long) time(NULL)
    );
    
    for (i = 0; i < env.device_count; i++) {
        dev_size = snprintf(dev_buffer, 1024,
                    "{\"id\":%u,"
                    "\"name\":\"%s\","
                    "\"pci_bus_id\":\"%s\","
                    "\"compute_mode\":\"%s\","
                    "\"max_temp\":%d,"
                    "\"max_power_usage\":%d,"
                    "\"max_bar1_mem_usage\":%llu,"
                    "\"max_mem_usage\":%llu,"
                    "\"max_gpu_utilization\":%u"
                    "%s",
                    env.devices[i].index,
                    env.devices[i].name,
                    env.devices[i].pci.busId,
                    convertToComputeModeString(env.devices[i].compute_mode),
                    env.devices[i].max_measurements.max_temp,
                    env.devices[i].max_measurements.max_power_usage,
                    env.devices[i].max_measurements.max_bar1mem_usage,
                    env.devices[i].max_measurements.max_mem_usage,
                    env.devices[i].max_measurements.max_gpu_utilization,
                    (i == (env.device_count - 1)) ? "}" : "},"
        );
        
        if ((dev_size < 0) || (dev_size >= 1024)) {
            fprintf(stderr, "An encoding error occured or JSON too large for local buffer: %ld > %d\n", dev_size, 1024);
            return -1;
        }
        
        //append device at the end of the string
        if ((size + dev_size) < maxsize) {
            strncat(doc_buffer, dev_buffer, dev_size);
            size += dev_size;
        }
        else {
            fprintf(stderr, "JSON too large for buffer: %ld >= %ld", size + dev_size, maxsize);
            return -1;
        }
    }

    if ((size + 2) >= maxsize) {
        fprintf(stderr, "JSON too large for buffer: %ld > %ld", size + 2, maxsize);
        return -1;
    }
    else {
        strncat(doc_buffer, "]}", 2);
        size += 2;
    }

    return size;
}

int json_encode_device_stats(gpu_dev_info_struct device, double sampling_duration, char *doc_buffer, size_t maxsize) {
    unsigned int i;
    size_t tmp_size;
    char tmp_buffer[256];

    size_t size = snprintf(doc_buffer, maxsize,
            "{\"event\":\"kickstart.inv.gpu.stats\","
            "\"timestamp\":%llu,"
            "\"id\":%u,"
            "\"name\":\"%s\","
            "\"pci_bus_id\":\"%s\","
            "\"compute_mode\":\"%s\","
            "\"sampling_duration\":%.3f,"
            "\"temp\":%d,"
            "\"power_usage\":%d,"
            "\"pcie_rx\":%u,"
            "\"pcie_tx\":%u,"
            "\"bar1_mem_usage\":%llu,"
            "\"mem_usage\":%llu,"
            "\"mem_utilization\":%u,"
            "\"gpu_utilization\":%u,"
            "\"gpu_clock\":%d,"
            "\"sm_clock\":%d,"
            "\"mem_clock\":%d,"
            "\"video_clock\":%d,"
            "\"compute_procs\":[",
            device.last_ts,
            device.index,
            device.name,
            device.pci.busId,
            convertToComputeModeString(device.compute_mode),
            sampling_duration,
            device.temp,
            device.power_usage,
            device.pcie_rx,
            device.pcie_tx,
            device.bar1memory.bar1Used,
            device.memory.used,
            device.utilization.memory,
            device.utilization.gpu,
            device.clocks[NVML_CLOCK_GRAPHICS],
            device.clocks[NVML_CLOCK_SM],
            device.clocks[NVML_CLOCK_MEM],
            device.clocks[NVML_CLOCK_VIDEO]
    );
    
    for (i = 0; i < device.compute_proc_infos.count; i++) {
        tmp_size = snprintf(tmp_buffer, 256,
                    "{\"pid\":%u,"
                    "\"mem_usage\":%llu"
                    "%s",
                    device.compute_proc_infos.infos[i].pid,
                    device.compute_proc_infos.infos[i].usedGpuMemory,
                    (i == (device.compute_proc_infos.count - 1)) ? "}" : "},"
        );
        
        if ((tmp_size < 0) || (tmp_size >= 256)) {
            fprintf(stderr, "An encoding error occured or JSON too large for local buffer: %ld > %d\n", tmp_size, 256);
            return -1;
        }
        
        //append compute task at the end of the string
        if ((size + tmp_size) < maxsize) {
            strncat(doc_buffer, tmp_buffer, tmp_size);
            size += tmp_size;
        }
        else {
            fprintf(stderr, "JSON too large for buffer: %ld >= %ld\n", size + tmp_size, maxsize);
            return -1;
        }
    }
    
    if (size + 20 >= maxsize) {
        fprintf(stderr, "JSON too large for buffer: %ld > %ld\n", size + 20, maxsize);
        return -1;
    }
    else {
        strncat(doc_buffer, "],\"graphics_procs\":[", 20);
        size += 20;
    }

    for (i = 0; i < device.graphics_proc_infos.count; i++) {
        tmp_size = snprintf(tmp_buffer, 256,
                    "{\"pid\":%u,"
                    "\"mem_usage\":%llu"
                    "%s",
                    device.graphics_proc_infos.infos[i].pid,
                    device.graphics_proc_infos.infos[i].usedGpuMemory,
                    (i == (device.graphics_proc_infos.count - 1)) ? "}" : "},"
        );
        
        if ((tmp_size < 0) || (tmp_size >= 256)) {
            fprintf(stderr, "An encoding error occured or JSON too large for local buffer: %ld > %d\n", tmp_size, 256);
            return -1;
        }
        
        //append graphics task at the end of the string
        if ((size + tmp_size) < maxsize) {
            strncat(doc_buffer, tmp_buffer, tmp_size);
            size += tmp_size;
        }
        else {
            fprintf(stderr, "JSON too large for buffer: %ld >= %ld\n", size + tmp_size, maxsize);
            return -1;
        }
    }
  
    if (size + 18 >= maxsize) {
        fprintf(stderr, "JSON too large for buffer: %ld > %ld\n", size + 18, maxsize);
        return -1;
    }
    else {
        strncat(doc_buffer, "],\"proc_samples\":[", 18);
        size += 20;
    }

    for (i = 0; i < device.proc_samples.count; i++) {
        if (device.proc_samples.samples[i].pid == 0)
            continue;

        tmp_size = snprintf(tmp_buffer, 256,
                    "{\"pid\":%u,"
                    "\"sample_ts\":%llu,"
                    "\"sm_util\":%u,"
                    "\"mem_util\":%u,"
                    "\"enc_util\":%u,"
                    "\"dec_util\":%u"
                    "%s",
                    device.proc_samples.samples[i].pid,
                    device.proc_samples.samples[i].timeStamp,
                    device.proc_samples.samples[i].smUtil,
                    device.proc_samples.samples[i].memUtil,
                    device.proc_samples.samples[i].encUtil,
                    device.proc_samples.samples[i].decUtil,
                    (i == (device.proc_samples.count - 1)) ? "}" : "},"
        );
        
        if ((tmp_size < 0) || (tmp_size >= 256)) {
            fprintf(stderr, "An encoding error occured or JSON too large for local buffer: %ld > %d\n", tmp_size, 256);
            return -1;
        }
        
        //append process sample at the end of the string
        if ((size + tmp_size) < maxsize) {
            strncat(doc_buffer, tmp_buffer, tmp_size);
            size += tmp_size;
        }
        else {
            fprintf(stderr, "JSON too large for buffer: %ld >= %ld\n", size + tmp_size, maxsize);
            return -1;
        }
    }

    if (size + 2 >= maxsize) {
        fprintf(stderr, "JSON too large for buffer: %ld > %ld\n", size + 2, maxsize);
        return -1;
    }
    else {
        strncat(doc_buffer, "]}", 2);
        size += 2;
    }
    
    return size;
}

#endif
