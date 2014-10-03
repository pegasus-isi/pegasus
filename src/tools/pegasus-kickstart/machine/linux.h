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
#ifndef _MACHINE_LINUX_H
#define _MACHINE_LINUX_H

#include <sys/types.h>
#include <sys/time.h>
#include "basic.h"
#include <stdint.h>

typedef enum {
    S_RUNNING, S_SLEEPING, S_WAITING, S_STOPPED, S_ZOMBIE, S_OTHER,
    MAX_STATE
} LinuxState;

typedef struct {
    /* summaries from procfs status file */
    uint64_t          size;
    uint64_t          rss;
    unsigned          total;
    unsigned          state[MAX_STATE];
} LinuxStatus;

typedef struct {
    /* common (shared) portion */
    MachineBasicInfo* basic;

    /*
     * provider-specific portion
     */

    /* from sysinfo(2) call */
    uint64_t          ram_total;
    uint64_t          ram_free;
    uint64_t          ram_shared;
    uint64_t          ram_buffer;
    uint64_t          swap_total;
    uint64_t          swap_free;

    /* from /proc/loadavg */
    float             load[3];

    /* from /proc/cpuinfo */
    unsigned short    cpu_count;
    unsigned short    cpu_online;
    unsigned long     megahertz;
    char              vendor_id[16];
    char              model_name[80];

    /* from /proc/uptime */
    double            idletime;
    struct timeval    boottime;

    /* from /proc/ ** /status */
    LinuxStatus       procs;
    LinuxStatus       tasks;

} MachineLinuxInfo;

extern void gather_loadavg(float load[3]);

extern void gather_meminfo(uint64_t* ram_total, uint64_t* ram_free,
                           uint64_t* ram_shared, uint64_t* ram_buffer,
                           uint64_t* swap_total, uint64_t* swap_free);

extern void* initMachine();
extern int printMachine(FILE *out, int indent, const char* tag, const void* data);
extern void deleteMachine(void* data);

#endif /* _MACHINE_LINUX_H */
