# GPU Events

## kickstart.inv.online.gpu.env

| Field | Type | Description |
| ------ | ------ | -------- |
| ts | int | seconds since epoch of when the event was created |
| hostname | string | hostname of the compute node |
| wf_label | string | pegasus workflow label |
| nvidia_driver_version | string | MAJOR.MINOR driver of the execution environment |
| site | string | pegasus execution site |
| xformation | string | pegasus transformation |
| task_id | string |pegasus task id |
| cuda_version | float | MAJOR.MINOR cuda version available on the execution environment |
| dag_job_id | string | pegasus dag job id |
| wf_uuid | string | pegasus workflow uuid |
| condor_job_id | string | condor job id |
| device_count | int | number of detected gpu devices |
| devices.compute_mode | string | device compute mode set by admin |
| devices.id | int | gpu device id in the environment |
| devices.is_cuda_capable | bool | has cuda capability |
| devices.max_sm_clock | int (Mhz) | max streaming multiprocessor clock speed |
| devices.max_mem_clock | int (Mhz) | max gpu memory clock speed |
| devices.power_limit | int (milliwatt) | gpu power limit |
| devices.total_memory | int (bytes) | total gpu memory |
| devices.total_bar1_memory | int (bytes) | total shared memory allocated for 3rd party device communication |
| devices.name | string | gpu product name |
| devices.cuda_capability | float | MAJOR.MINOR |
| devices.max_gpu_clock | int (Mhz) | max gpu clock |
| devices.max_video_clock | int (Mhz) | max video enc/dec speed |
| devices.pci_bus_id | string | unique bus id |

## kickstart.inv.online.gpu.stats.max

| Field | Type | Description |
| ------ | ------ | -------- |
| ts | int | seconds since epoch of when the event was created |
| hostname | string | hostname of the compute node |
| wf_label | string | pegasus workflow label |
| site | string | pegasus execution site |
| xformation | string | pegasus transformation |
| task_id | string | pegasus task id |
| dag_job_id | string | pegasus dag job id |
| wf_uuid | string | pegasus workflow uuid |
| condor_job_id | string | condor job id |
| devices.id | int | gpu device id in the environment |
| devices.max_bar1_mem_usage | int (bytes) | maximum memory used for communication with 3rd party devices |
| devices.max_temp | int (celsius) | maximum observed temperature |
| devices.max_gpu_utilization | int (%) | max graphics core utilization |
| devices.name | string | gpu product name |
| devices.max_power_usage | int (milliwatt) | max power usage |
| devices.max_mem_usage | int (bytes) | max gpu memory usage |
| devices.pci_bus_id | string | unique bus id |

## kickstart.inv.online.gpu.stats

| Field | Type | Description |
| ------ | ------ | -------- |
| ts | int | seconds since epoch of when the event was created |
| hostname | string | hostname of the compute node |
| wf_label | string | pegasus workflow label |
| site | string | pegasus execution site |
| xformation | string | pegasus transformation |
| task_id | string | pegasus task id |
| dag_job_id | string | pegasus dag job id |
| wf_uuid | string | pegasus workflow uuid |
| condor_job_id | string | condor job id |
| id | int | gpu device id in the environment |
| compute_mode | string | device compute mode set by admin |
| name | string | gpu product name |
| pci_bus_id | string | unique bus id |
| temp | int (celsius) | temperature |
| video_clock | int (Mhz) | video enc/dec speed |
| gpu_clock | int (Mhz) | graphics clock speed |
| sm_clock | int (Mhz) | shared multiprocessor clock speed |
| mem_clock | int (Mhz) | memory clock speed |
| power_usage | int (milliwatts) | power usage |
| mem_usage | int (bytes) | gpu memory usage |
| mem_utilization | int (%) | gpu memory utilization - percent of time spent in memory ops since last driver sample |
| gpu_utilization | int (%) | gpu utilization - percent of time spent in gpu ops |
| bar1_mem_usage | int (bytes) | memory used for communication with 3rd party devices |
| sampling_duration | float | seconds took kickstart to collect this gpu event |
| pcie_rx (optional) | int (bytes) | bytes received on the pcie bus during a 20ms sample period |
| pcie_tx (optional) | int (bytes) | bytes sent on the pcie bus during a 20ms sample period |
| compute_procs.pid (optional) | int | process id |
| compute_procs.mem_usage (optional) | int (bytes) | memory usage of this cuda process |
| graphics_procs.pid (optional) | int | process id |
| graphics_procs.mem_usage (optional) | int (bytes) | memory usage of this graphics process |
| proc_samples.pid (optional) | int | process id |
| proc_samples.decUtil (optional) | int (%) | process id |
| proc_samples.encUtil (optional) | int (%) | decoder utilization |
| proc_samples.memUtil (optional) | int (%) | encoder utilization |
| proc_samples.smUtil (optional) | int (%) | memory utilization |
| proc_samples.timeStamp (optional) | int (microseconds) | cpu timestamp of when the sample for the pid was collected in the driver structs |

### PCI-E fields are populated when KICKSTART_MON_GRAPHICS_PCIE is set
### Graphic processes are populated when KICKSTART_MON_GRAPHICS_PROCS is set
### Process utilization fields are populated when KICKSTART_MON_GRAPHICS_UTIL is set
