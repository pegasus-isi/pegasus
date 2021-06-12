#ifndef NVML_WRAPPER_H
#define NVML_WRAPPER_H

//#include <nvml.h>
#include "nvml.h"

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
    unsigned int clocks[NVML_CLOCK_COUNT];
    unsigned int max_clocks[NVML_CLOCK_COUNT];
    char name[NVML_DEVICE_NAME_BUFFER_SIZE];
} gpu_dev_info_struct;

typedef struct {
    unsigned int device_count;
    int cuda_version;
    char driver_version[NVML_SYSTEM_DRIVER_VERSION_BUFFER_SIZE];
    gpu_dev_info_struct *devices;
} gpu_env_struct;

nvmlReturn_t getGpuEnvironment(gpu_env_struct *env);

nvmlReturn_t getGpuStatistics(gpu_dev_info_struct *device, unsigned char monitor_pcie_usage);
nvmlReturn_t getGpuStatisticsByID(unsigned int i, gpu_env_struct *env, unsigned char monitor_pcie_usage);
nvmlReturn_t getGpuStatisticsAll(gpu_env_struct *env, unsigned char monitor_pcie_usage);

nvmlReturn_t getGpuProcessUtilization(gpu_dev_info_struct *device);
nvmlReturn_t getGpuProcessUtilizationByID(unsigned int i, gpu_env_struct *env);
nvmlReturn_t getGpuProcessUtilizationAll(gpu_env_struct *env);

nvmlReturn_t getGpuComputeProcesses(gpu_dev_info_struct *device);
nvmlReturn_t getGpuComputeProcessesByID(unsigned int i, gpu_env_struct *env);
nvmlReturn_t getGpuComputeProcessesAll(gpu_env_struct *env);

nvmlReturn_t getGpuGraphicsProcesses(gpu_dev_info_struct *device);
nvmlReturn_t getGpuGraphicsProcessesByID(unsigned int i, gpu_env_struct *env);
nvmlReturn_t getGpuGraphicsProcessesAll(gpu_env_struct *env);

void printGpuStatistics(FILE *out, gpu_dev_info_struct device);
void printGpuStatisticsByID(FILE *out, unsigned int i, gpu_env_struct env);

void printGpuProcessSamples(FILE *out, gpu_dev_info_struct device);
void printGpuProcessSamplesByID(FILE *out, unsigned int i, gpu_env_struct env);

void printGpuComputeProcessInfos(FILE *out, gpu_dev_info_struct device);
void printGpuComputeProcessInfosByID(FILE *out, unsigned int i, gpu_env_struct env);

void printGpuGraphicsProcessInfos(FILE *out, gpu_dev_info_struct device);
void printGpuGraphicsProcessInfosByID(FILE *out, unsigned int i, gpu_env_struct env);

void printGpuEnvironment(FILE *out, gpu_env_struct env);
void printGpuMaxMeasurements(FILE *out, gpu_env_struct env);

void nvml_monitoring_cleanup(gpu_env_struct *env);

int json_encode_environment(gpu_env_struct env, char *doc_buffer, size_t maxsize);
int json_encode_device_stats_max(gpu_env_struct env, char *doc_buffer, size_t maxsize);
int json_encode_device_stats(gpu_dev_info_struct device, double sampling_duration, char *doc_buffer, size_t maxsize);

#endif
