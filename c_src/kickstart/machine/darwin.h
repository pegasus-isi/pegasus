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
 * Copyright 1999-2008 University of Chicago and The University of
 * Southern California. All rights reserved.
 */
#ifndef _MACHINE_DARWIN_H
#define _MACHINE_DARWIN_H

#include <sys/types.h>
#include <sys/time.h>
#include <sys/utsname.h>
#include <stdint.h>
#include "basic.h"

typedef enum {
    STATE_TOTAL = 0,
    STATE_IDLE,     /* Darwin SIDL */
    STATE_RUNNING,  /* Darwin SRUN if realtime != 0 */
    STATE_SLEEPING, /* Darwin SRUN if realtime == 0 */
    STATE_WAITING,  /* Darwin SSLEEP */
    STATE_STOPPED,  /* Darwin SSTOP */
    STATE_ZOMBIE,   /* Darwin SZOMB */
    STATE_OTHER,    /* future versions */
    MAX_STATE
} DarwinState;

typedef struct {
    /* common (shared) portion */
    MachineBasicInfo* basic;

    /*
     * provider-specific portion
     */

    /* memory statistics */
    uint64_t          ram_total;        /* sysctl hw.memsize */
    uint64_t          ram_avail;        /* mach vm_statistics:free_count */
    uint64_t          ram_active;       /* mach vm_statistics:active_count */
    uint64_t          ram_inactive;     /* mach vm_statistics:inactive_count */
    uint64_t          ram_wired;        /* mach vm_statistics:wire_count */
    uint64_t          swap_total;       /* sysctl vm.swapusage:xsu_total */
    uint64_t          swap_avail;       /* sysctl vm.swapusage:xsu_avail */
    uint64_t          swap_used;        /* sysctl vm.swapusage:xsu_used */

    /* boot time stats */
    struct timeval    boottime;         /* kern.boottime */

    /* cpu information */
    unsigned short    cpu_count;        /* hw.ncpu */
    unsigned short    cpu_online;       /* hw.activecpu */
    unsigned long     megahertz;        /* hw.cpufrequency */
    char              vendor_id[16];    /* machdep.cpu.vendor */
    char              model_name[80];   /* machdep.cpu.brand_string */

    /* system load */
    float             load[3];          /* vm.loadavg */

    /* process count */
    unsigned          pid_state[MAX_STATE];
} MachineDarwinInfo;

extern void gather_loadavg(float load[3]);
extern void* initMachine(void);
extern int printMachine(FILE *out, int indent, const char* tag, const void* data);
extern void deleteMachine(void* data);

#endif /* _MACHINE_DARWIN_H */
