.. _cli-pegasus-globus-online-init:

==========================
pegasus-globus-online-init
==========================

Initializes OAuth tokens for Globus Online authentication.
::

      pegasus-globus-online-init  [-h]
                                  [--permanent]



Description
===========

**pegasus-globus-online-init** initializes OAuth tokens, to be used with
Globus Online transfers. It redirects the user to globus website, in
order to authorize Pegasus wms to perform transfers with the user’s
Globus account. By default this tool requests tokens that cannot be
refreshed and could potentially expire within a couple of days. In order
to provide pegasus with refreshable tokens please use --permanent
option. The acquired tokens are placed in globus.conf inside .pegasus
folder of the user’s home directory.

Note this tool should be used before starting a workflow that relies on
Globus Online transfers, unless the user has initialized the tokens with
another way or has acquired refreshable tokens previously.



Options
=======

**-h**; \ **--help**
   Prints a usage summary with all the available command-line options.

**--permanent**
   Requests a refresh token that can be used indefinetely. Access can be
   revoked from globus web interface (manage consents)


JSON Output Documentation
=========================
[
  {
    "event": "kickstart.inv.gpu.environment",
    "timestamp": 1621975512,
    "cuda_version": 10.2,
    "nvidia_driver_version": "440.100",
    "device_count": 1,
    "devices": [
      {
        "id": 0,
        "name": "GeForce GTX 1080",
        "pci_bus_id": "00000000:01:00.0",
        "compute_mode": "Default",
        "is_cuda_capable": true,
        "cuda_capability": 6.1,
        "power_limit": 180000,
        "total_bar1_memory": 140290443800256,
        "total_memory": 8513978368,
        "max_gpu_clock": 1911,
        "max_sm_clock": 1911,
        "max_mem_clock": 5005,
        "max_video_clock": 1708
      }
    ]
  },
  {
    "event": "kickstart.inv.gpu.stats",
    "timestamp": 1621975512,
    "id": 0,
    "name": "GeForce GTX 1080",
    "pci_bus_id": "00000000:01:00.0",
    "compute_mode": "Default",
    "sampling_duration": 0.205,
    "temp": 43,
    "power_usage": 5661,
    "pcie_rx": 0,
    "pcie_tx": 0,
    "bar1_mem_usage": 6033408,
    "mem_usage": 4866375680,
    "mem_utilization": 0,
    "gpu_utilization": 0,
    "gpu_clock": 139,
    "sm_clock": 139,
    "mem_clock": 405,
    "video_clock": 544,
    "compute_procs": [
      {
        "pid": 2116725,
        "mem_usage": 4837081088
      }
    ],
    "graphics_procs": [
      {
        "pid": 1902,
        "mem_usage": 10309632
      },
      {
        "pid": 2165,
        "mem_usage": 5197824
      }
    ],
    "proc_samples": [
      {
        "pid": 1902,
        "sm_util": 0,
        "mem_util": 0,
        "enc_util": 0,
        "dec_util": 0
      }
    ]
  },
  {
    "event": "kickstart.inv.gpu.stats",
    "timestamp": 1621975518,
    "id": 0,
    "name": "GeForce GTX 1080",
    "pci_bus_id": "00000000:01:00.0",
    "compute_mode": "Default",
    "sampling_duration": 0.043,
    "temp": 43,
    "power_usage": 38882,
    "pcie_rx": 60000,
    "pcie_tx": 633000,
    "bar1_mem_usage": 6033408,
    "mem_usage": 5835259904,
    "mem_utilization": 0,
    "gpu_utilization": 0,
    "gpu_clock": 1607,
    "sm_clock": 1607,
    "mem_clock": 4513,
    "video_clock": 1442,
    "compute_procs": [
      {
        "pid": 2116725,
        "mem_usage": 5805965312
      }
    ],
    "graphics_procs": [
      {
        "pid": 1902,
        "mem_usage": 10309632
      },
      {
        "pid": 2165,
        "mem_usage": 5197824
      }
    ],
    "proc_samples": [
      {
        "pid": 2116725,
        "sm_util": 0,
        "mem_util": 0,
        "enc_util": 0,
        "dec_util": 0
      },
      {
        "pid": 1902,
        "sm_util": 0,
        "mem_util": 0,
        "enc_util": 0,
        "dec_util": 0
      }
    ]
  },
  {
    "event": "kickstart.inv.gpu.stats.max",
    "timestamp": 1621975521,
    "devices": [
      {
        "id": 0,
        "name": "GeForce GTX 1080",
        "pci_bus_id": "00000000:01:00.0",
        "compute_mode": "Default",
        "max_temp": 43,
        "max_power_usage": 38882,
        "max_bar1_mem_usage": 6033408,
        "max_mem_usage": 5835259904,
        "max_gpu_usage": 0
      }
    ]
  }
]
