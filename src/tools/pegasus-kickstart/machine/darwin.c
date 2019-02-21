/*
 * This file or a portion of this file is licensed under the terms of
 * the Globus Toolkit Public License, found in file GTPL, or at
 * http://www.globus.org/toolkit/download/license.html. This notice must
 * appear in redistributions of this file, with or without modification.
 *
 * Redistributions of this Software, with or without modification, must
 * reproduce the GTPL in: (1) the Software, or (2) the Documentation or
 * some other similar material which is provided with the Software (if
 * any).
 *
 * Copyright 1999-2004 University of Chicago and The University of
 * Southern California. All rights reserved.
 */
#include "basic.h"
#include "darwin.h"
#include "../utils.h"
#include "../error.h"

#include <ctype.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <inttypes.h>

#include <sys/sysctl.h>

#include <mach/mach.h>
#include <mach/host_info.h>
#include <mach/vm_statistics.h>

static void gather_darwin_cpuinfo(MachineDarwinInfo* machine) {
    int i;
    size_t len;
    unsigned long freq;
    char model[128];

    len = sizeof(i);
    if (sysctlbyname("hw.ncpu", &i, &len, NULL, 0) == 0) {
        machine->cpu_count = i;
    }

    len = sizeof(i);
    if (sysctlbyname("hw.activecpu", &i, &len, NULL, 0) == 0) {
        machine->cpu_online = i;
    }

    len = sizeof(freq);
    if (sysctlbyname("hw.cpufrequency", &freq, &len, NULL, 0) == 0) {
        machine->megahertz = freq / 1000000;
    }

    len = sizeof(machine->vendor_id);
    if (sysctlbyname("machdep.cpu.vendor", machine->vendor_id, &len, NULL, 0) != 0) {
        memset(machine->vendor_id, 0, sizeof(machine->vendor_id));
    }

    len = sizeof(model);
    if (sysctlbyname("machdep.cpu.brand_string", model, &len, NULL, 0) == 0) {
        char* s = model;
        char* d = machine->model_name;

        while (*s && d - machine->model_name < sizeof(machine->model_name)) {
            while (*s && ! isspace(*s)) *d++ = *s++;
            if (*s && *s == ' ') *d++ = *s++;
            while (*s && isspace(*s)) ++s;
        }
        *d = 0;
    } else {
        memset(machine->model_name, 0, sizeof(machine->model_name));
    }
}

static void gather_darwin_uptime(MachineDarwinInfo* machine) {
    size_t len = sizeof(machine->boottime);
    if (sysctlbyname("kern.boottime", &machine->boottime, &len, NULL, 0) == -1) {
        memset(&machine->boottime, 0, sizeof(machine->boottime));
    }
}

void gather_loadavg(float load[3]) {
    /* purpose: collect load averages
     * primary: provide functionality for monitoring
     * paramtr: load (OUT): array of 3 floats
     */
    struct loadavg l;
    size_t len = sizeof(l);
    if (sysctlbyname("vm.loadavg", &l, &len, NULL, 0) == 0) {
        int i;
        for (i=0; i<3; ++i)
            load[i] = l.ldavg[i] / ((float) l.fscale);
    } else {
        load[0] = load[1] = load[2] = 0.0;
    }
}

static void gather_darwin_meminfo(MachineDarwinInfo* machine) {
    vm_statistics_data_t vm;
    mach_msg_type_number_t ic = HOST_VM_INFO_COUNT;
    uint64_t pagesize = getpagesize();
    struct xsw_usage s;
    size_t len = sizeof(s);
    if (sysctlbyname("vm.swapusage", &s, &len, NULL, 0) == 0) {
        machine->swap_total = s.xsu_total;
        machine->swap_avail = s.xsu_avail;
        machine->swap_used  = s.xsu_used;
    }

    len = sizeof(machine->ram_total);
    if (sysctlbyname("hw.memsize", &machine->ram_total, &len, NULL, 0) == -1) {
        machine->ram_total = 0;
    }

    host_statistics(mach_host_self(), HOST_VM_INFO, (host_info_t) &vm, &ic);
    machine->ram_avail = pagesize * vm.free_count;
    machine->ram_active = pagesize * vm.active_count;
    machine->ram_inactive = pagesize * vm.inactive_count;
    machine->ram_wired = pagesize * vm.wire_count;
}

static void gather_darwin_procstate(unsigned state[MAX_STATE]) {
    int mib[4];
    size_t len;

    mib[0] = CTL_KERN;
    mib[1] = KERN_PROC;
    mib[2] = KERN_PROC_ALL;
    if (sysctl(mib, 3, NULL, &len, NULL, 0) != -1 && len > 0) {
        void* buffer = malloc(len + sizeof(struct kinfo_proc));
        if (buffer == NULL) {
            printerr("malloc: %s\n", strerror(errno));
            return;
        }
        if (sysctl(mib, 3, buffer, &len, NULL, 0) != -1 && len > 0) {
            struct extern_proc* p;
            struct kinfo_proc* kp;
            struct kinfo_proc* end = ((struct kinfo_proc*) (((char*) buffer) + len));
            for (kp = (struct kinfo_proc*) buffer;
                 kp->kp_proc.p_pid && kp < end;
                 kp++) {

                p = &kp->kp_proc;
                state[STATE_TOTAL]++;

                switch (p->p_stat) {
                    case SIDL:
                        state[STATE_IDLE]++;
                        break;
                    case SRUN:
                        /* SRUN is (runnable), not running. Need to dig deeper to find those procs
                         * which are actually running, and those that are just runnable.
                         */
                        state[(p->p_realtimer.it_interval.tv_sec |
                               p->p_realtimer.it_interval.tv_usec |
                               p->p_realtimer.it_value.tv_sec |
                               p->p_realtimer.it_value.tv_usec |
                               p->p_rtime.tv_sec |
                               p->p_rtime.tv_usec) != 0 ? STATE_RUNNING : STATE_SLEEPING]++;
                        break; 
                    case SSLEEP:
                        state[STATE_WAITING]++;
                        break;
                    case SSTOP:
                        state[STATE_STOPPED]++;
                        break;
                    case SZOMB:
                        state[STATE_ZOMBIE]++;
                        break;
                    default:
                        state[STATE_OTHER]++;
                        break;
                }
            }
        }
        free(buffer);
    }
}

void* initMachine(void) {
    /* purpose: initialize the data structure.
     * returns: initialized MachineDarwinInfo structure.
     */
    MachineDarwinInfo* p = (MachineDarwinInfo*) calloc(1, sizeof(MachineDarwinInfo));
    if (p == NULL) {
        printerr("calloc: %s\n", strerror(errno));
        return NULL;
    }

    /* name of this provider -- overwritten by importers */
    p->basic = initBasicMachine();
    p->basic->provider = "darwin";

    /* gather loadavg */
    gather_darwin_meminfo(p);
    gather_loadavg(p->load);
    gather_darwin_uptime(p);
    gather_darwin_cpuinfo(p);
    gather_darwin_procstate(p->pid_state);

    return p;
}

int printDarwinInfo(FILE *out, int indent, const MachineDarwinInfo *ptr) {
    static const char* state_names[MAX_STATE] = {
        "total",
        "idle",
        "running",
        "sleeping",
        "waiting",
        "stopped",
        "zombie",
        "other"
    };

    /* <ram .../> tag */
    fprintf(out, "%*sram_total: %"PRIu64"\n%*sram_avail: %"PRIu64"\n%*sram_active: %"PRIu64"\n%*sram_inactive: %"PRIu64"\n%*sram_wired: %"PRIu64"\n",
            indent, "", ptr->ram_total / 1024,
            indent, "", ptr->ram_avail / 1024,
            indent, "", ptr->ram_active / 1024,
            indent, "", ptr->ram_inactive / 1024,
            indent, "", ptr->ram_wired / 1024);

    /* <swap .../> tag */
    fprintf(out, "%*sswap_total: %"PRIu64"\n%*sswap_avail: %"PRIu64"\n%*sswap_used: %"PRIu64"\n",
            indent, "", ptr->swap_total / 1024,
            indent, "", ptr->swap_avail / 1024,
            indent, "", ptr->swap_used / 1024);

    /* <boot> element */
    fprintf(out, "%*sboot: %s\n", indent, "",
            fmtisodate(ptr->boottime.tv_sec, ptr->boottime.tv_usec));

    /* <cpu> element */
    fprintf(out, "%*scpu_count: %hu\n%*scpu_speed: %lu\n%*scpu_vendor: %s\n%*scpu_name: %s\n",
            indent, "", ptr->cpu_count, 
            indent, "", ptr->megahertz,
            indent, "", ptr->vendor_id,
            indent, "", ptr->model_name);

    /* loadavg data */
    fprintf(out, "%*sload_min1: %.2f\n%*sload_min5: %.2f\n%*sload_min15: %.2f\n",
            indent, "", ptr->load[0],
            indent, "", ptr->load[1],
            indent, "", ptr->load[2]);

    /* <proc> element */
    for (DarwinState s = STATE_TOTAL; s < MAX_STATE; ++s) {
        if (ptr->pid_state[s]) {
            fprintf(out, "%*sproc_%s=: %u\n", indent, "", state_names[s], ptr->pid_state[s]);
        }
    }

    return 0;
}

int printMachine(FILE *out, int indent, const char* tag, const void* data) {
    /* purpose: format the information into the given stream as XML.
     * paramtr: out (IO): the buffer
     *          indent (IN): indentation level
     *          tag (IN): name to use for element tags.
     *          data (IN): MachineDarwinInfo info to print.
     * returns: 0 if no error
     */

    /* sanity check */
    if (data == NULL) {
        return 0;
    }

    const MachineDarwinInfo* ptr = (const MachineDarwinInfo*) data;

    /* start basic info */
    startBasicMachine(out, indent, tag, ptr->basic);

    /* Print contents of <darwin> */
    printDarwinInfo(out, indent+2, ptr);

    /* finish tag */
    finalBasicMachine(out, indent, tag, ptr->basic);

    return 0;
}

void deleteMachine(void* data) {
    /* purpose: destructor
     * paramtr: data (IO): valid MachineDarwinInfo structure to destroy.
     */
    MachineDarwinInfo* ptr = (MachineDarwinInfo*) data;

    if (ptr) {
        deleteBasicMachine(ptr->basic);
        free((void*) ptr);
    }
}

