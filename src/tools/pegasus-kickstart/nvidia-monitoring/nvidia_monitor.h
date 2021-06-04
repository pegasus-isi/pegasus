#ifndef NVIDIA_MONITOR_H
#define NVIDIA_MONITOR_H

typedef struct json_doc {
    int buffer_size;
    size_t buffer_limit;
    char* buffer;
} json_doc;

typedef struct thread_data {
    unsigned int i;
    double last_collection_duration;
    unsigned long int last_collection_time;
    int monitor_compute_procs;
    int monitor_graphics_procs;
    int monitor_process_util;
    int monitor_pcie_usage;
    int output_json;
    gpu_env_struct *env;
    char *output;
    char *publish_url;
    json_doc doc;
    nvmlReturn_t result;
} thread_data;

void* collect_gpu_stats(void *args);

nvmlReturn_t multi_threaded_collection(FILE *out, gpu_env_struct *env, pthread_t *threads, thread_data *threads_data, unsigned char output_json, unsigned int interval);
nvmlReturn_t single_threaded_collection(FILE *out, gpu_env_struct *env, int monitor_compute_procs, int monitor_graphics_procs, int monitor_pcie_usage, int monitor_process_util, int output_json, unsigned int interval);

int start_nvidia_monitoring_thread();
int stop_nviidia_monitoring_thread();

#endif
