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
#include "linux.h"
#include "../utils.h"
#include "../error.h"

#include <ctype.h>
#include <errno.h>
#include <math.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>

#include <signal.h> /* signal names */

#include <sys/sysinfo.h>

static uint64_t unscale(unsigned long value, char scale) {
    uint64_t result = value;

    switch (scale) {
        case 'B': /* just bytes */
            break;
        case 'k':
            result <<= 10;
            break;
        case 'M':
            result <<= 20;
            break;
        case 'G':
            result <<= 30;
            break;
    }

    return result;
}

static void parse_status_file(const char* fn, LinuxStatus* status) {
    char line[256];
    FILE* f;
    if ((f = fopen(fn, "r"))) {
        while (fgets(line, sizeof(line), f)) {
            if (strncmp(line, "State:", 6) == 0) {
                char* s = line+7;
                while (*s && isspace(*s)) ++s;
                switch (*s) {
                    case 'R':
                        status->state[S_RUNNING]++;
                        break;
                    case 'S':
                        status->state[S_SLEEPING]++;
                        break;
                    case 'D':
                        status->state[S_WAITING]++;
                        break;
                    case 'T':
                        status->state[S_STOPPED]++;
                        break;
                    case 'Z':
                        status->state[S_ZOMBIE]++;
                        break;
                    default:
                        status->state[S_OTHER]++;
                        break;
                }
            } else if (line[0] == 'V') {
                unsigned long value;
                char scale[4];
                if (strncmp(line, "VmSize:", 7) == 0) {
                    char* s = line+8;
                    while (*s && isspace(*s)) ++s;
                    sscanf(s, "%lu %4s", &value, scale);
                    status->size += unscale(value, scale[0]);
                } else if (strncmp(line, "VmRSS:", 6) == 0) {
                    char* s = line+7;
                    while (*s && isspace(*s)) ++s;
                    sscanf(s, "%lu %4s", &value, scale);
                    status->rss += unscale(value, scale[0]);
                }
            }
        }
        fclose(f);
    }
}


void gather_linux_proc26(LinuxStatus* procs, LinuxStatus* tasks) {
    /* purpose: collect proc information on Linux 2.6 kernel
     * paramtr: procs (OUT): aggregation on process level
     *          tasks (OUT): aggregation on task level
     */
    struct dirent* dp;
    struct dirent* dt;
    DIR* taskdir;
    DIR* procdir;

    /* assume procfs is mounted at /proc */
    if ((procdir = opendir("/proc"))) {
        char procinfo[128];
        while ((dp = readdir(procdir))) {
            /* real proc files start with digit in 2.6 */
            if (isdigit(dp->d_name[0])) {
                procs->total++;
                snprintf(procinfo, sizeof(procinfo), "/proc/%s/task", dp->d_name);
                if ((taskdir=opendir(procinfo))) {
                    while ((dt = readdir(taskdir))) {
                        if (isdigit(dt->d_name[0])) {
                            char taskinfo[128];
                            tasks->total++;
                            snprintf(taskinfo, sizeof(taskinfo),
                                     "%s/%s/status", procinfo, dt->d_name);
                            parse_status_file(taskinfo, tasks);
                        }
                    }
                    closedir(taskdir);
                }
                snprintf(procinfo, sizeof(procinfo), "/proc/%s/status", dp->d_name);
                parse_status_file(procinfo, procs);
            }
        }
        closedir(procdir);
    }
}

static void parse_stat_file(const char* fn, LinuxStatus* proc, LinuxStatus* task) {
    char line[256];
    FILE* f;
    if ((f = fopen(fn, "r"))) {
        if (fgets(line, sizeof(line), f)) {
            pid_t pid, ppid;
            char state;
            unsigned long flags, vmsize, text, stack;
            signed long rss;
            int exitsignal, notatask = 0;

            sscanf(line,
                   "%d %*s %c %d %*d %*d %*d %*d %lu %*u "     /*  1 - 10 */
                   "%*u %*u %*u %*u %*u %*d %*d %*d %*d %*d "  /* 11 - 20 */
                   "%*d %*u %lu %ld %*u %lu %*u %lu %*u %*u "  /* 21 - 30 */
                   "%*u %*u %*u %*u %*u %*u %*u %d %*d %*d "   /* 31 - 40 */
                   , &pid, &state, &ppid, &flags
                   , &vmsize, &rss, &text, &stack
                   , &exitsignal
                   /* SIGCHLD == normal process
                    * SIGRTxx == threaded task
                    */
                  );
            rss *= getpagesize();

            if (exitsignal == SIGCHLD) {
                /* regular process */
                notatask = 1;
            } else if (exitsignal == SIGRTMIN) {
                /* Do we need to check ancient LinuxThreads, which on 2.0 kernels
                 * were forced to use SIGUSR1 and SIGUSR2 for communication? */
                /* regular thread */
                notatask = 0;
            } else if (exitsignal == 0) {
                if (text == 0 && stack == 0) {
                    /* kernel magic task -- count as process */
                    notatask = 1;
                } else {
                    /* thread manager task -- count as thread except (init) */
                    notatask = (ppid == 0);
                }
            }

            switch (state) {
                case 'R':
                    task->state[S_RUNNING]++;
                    if (notatask) proc->state[S_RUNNING]++;
                    break;
                case 'S':
                    task->state[S_SLEEPING]++;
                    if (notatask) proc->state[S_SLEEPING]++;
                    break;
                case 'D':
                    task->state[S_WAITING]++;
                    if (notatask) proc->state[S_WAITING]++;
                    break;
                case 'T':
                    task->state[S_STOPPED]++;
                    if (notatask) proc->state[S_STOPPED]++;
                    break;
                case 'Z':
                    task->state[S_ZOMBIE]++;
                    if (notatask) proc->state[S_ZOMBIE]++;
                    break;
                default:
                    task->state[S_OTHER]++;
                    if (notatask) proc->state[S_OTHER]++;
                    break;
            }

            task->size += vmsize;
            if (notatask) proc->size += vmsize;
            task->rss  += rss;
            if (notatask) proc->rss += rss;

            task->total++;
            if (notatask) proc->total++;
        }
        fclose(f);
    } else {
        printerr("open %s: %s\n", fn, strerror(errno));
    }
}

void gather_linux_proc24(LinuxStatus* procs, LinuxStatus* tasks) {
    /* purpose: collect proc information on Linux 2.4 kernel
     * paramtr: procs (OUT): aggregation on process level
     *          tasks (OUT): aggregation on task level
     * grmblftz Linux uses multiple schemes for threads/tasks.
     */
    struct dirent* dp;
    DIR* procdir;

    /* assume procfs is mounted at /proc */
    if ((procdir=opendir("/proc"))) {
        char procinfo[128];
        while ((dp = readdir(procdir))) {
            /* real processes start with digit, tasks *may* start with dot-digit */
            if (isdigit(dp->d_name[0]) || (dp->d_name[0] == '.' && isdigit(dp->d_name[1]))) {
                snprintf(procinfo, sizeof(procinfo), "/proc/%s/stat", dp->d_name);
                parse_stat_file(procinfo, procs, tasks);
            }
        }
        closedir(procdir);
    }
}

void gather_loadavg(float load[3]) {
    /* purpose: collect load averages
     * primary: provide functionality for monitoring
     * paramtr: load (OUT): array of 3 floats
     */
    FILE* f = fopen("/proc/loadavg", "r");
    if (f != NULL) {
        fscanf(f, "%f %f %f", load+0, load+1, load+2);
        fclose(f);
    }
}

void gather_meminfo(uint64_t* ram_total, uint64_t* ram_free,
                    uint64_t* ram_shared, uint64_t* ram_buffer,
                    uint64_t* swap_total, uint64_t* swap_free) {
    /* purpose: collect system-wide memory usage
     * primary: provide functionality for monitoring
     * paramtr: ram_total (OUT): all RAM
     *          ram_free (OUT): free RAM
     *          ram_shared (OUT): unused?
     *          ram_buffer (OUT): RAM used for buffers by kernel
     *          swap_total (OUT): all swap space
     *          swap_free (OUT): free swap space
     */
    struct sysinfo si;

    /* remaining information */
    if (sysinfo(&si) != -1) {
        uint64_t pagesize = si.mem_unit;
        *ram_total  = si.totalram * pagesize;
        *ram_free   = si.freeram * pagesize;
        *ram_shared = si.sharedram * pagesize;
        *ram_buffer = si.bufferram * pagesize;
        *swap_total = si.totalswap * pagesize;
        *swap_free  = si.freeswap * pagesize;
    }
}

static void gather_proc_uptime(struct timeval* boottime, double* idletime) {
    FILE* f = fopen("/proc/uptime", "r");
    if (f != NULL) {
        double uptime, r, sec;
        struct timeval tv;
        now(&tv);
        fscanf(f, "%lf %lf", &uptime, idletime);
        fclose(f);
        r = (tv.tv_sec + tv.tv_usec * 1E-6) - uptime;
        boottime->tv_sec = sec = (time_t) floor(r);
        boottime->tv_usec = (time_t) floor(1E6 * (r - sec));
    }
}

static void gather_proc_cpuinfo(MachineLinuxInfo* machine) {
    FILE* f = fopen("/proc/cpuinfo", "r");
    if (f != NULL) {
        char line[256];
        while (fgets(line, 256, f)) {
            if (*(machine->vendor_id) == 0 &&
                    strncmp(line, "vendor_id", 9) == 0) {
                char* s = strchr(line, ':')+1;
                char* d = machine->vendor_id;
                while (*s && isspace(*s)) ++s;
                while (*s && ! isspace(*s) && d - machine->vendor_id < sizeof(machine->vendor_id))
                    *d++ = *s++;
                *d = 0;
            } else if (*(machine->model_name) == 0 && strncmp(line, "model name", 10) == 0) {
                char* s = strchr(line, ':')+2;
                char* d = machine->model_name;
                while (*s && d - machine->model_name < sizeof(machine->model_name)) {
                    while (*s && ! isspace(*s)) *d++ = *s++;
                    if (*s && *s == ' ') *d++ = *s++;
                    while (*s && isspace(*s)) ++s;
                }
                *d = 0;
            } else if (machine->megahertz == 0.0 && strncmp(line, "cpu MHz", 7) == 0) {
                char* s = strchr(line, ':')+2;
                float mhz;
                sscanf(s, "%f", &mhz);
                machine->megahertz = (unsigned long) (mhz + 0.5);
            } else if (strncmp(line, "processor", 9) == 0) {
                machine->cpu_count += 1;
            }
        }
        fclose(f);
    }
}

static unsigned long extract_version(const char* release) {
    /* purpose: extract a.b.c version from release string, ignoring extra junk
     * paramtr: release (IN): pointer to kernel release string (with junk)
     * returns: integer representation of a version
     *          version := major * 1,000,000 + minor * 1,000 + patch
     */
    unsigned major = 0;
    unsigned minor = 0;
    unsigned patch = 0;
    sscanf(release, "%u.%u.%u", &major, &minor, &patch);
    return major * 1000000ul + minor * 1000 + patch;
}

void* initMachine(void) {
    /* purpose: initialize the data structure.
     * returns: initialized MachineLinuxInfo structure.
     */
    unsigned long version;
    MachineLinuxInfo* p = (MachineLinuxInfo*) calloc(1, sizeof(MachineLinuxInfo));
    if (p == NULL) {
        printerr("calloc: %s\n", strerror(errno));
        return NULL;
    }

    /* name of this provider -- overwritten by importers */
    p->basic = initBasicMachine();
    p->basic->provider = "linux";

    gather_meminfo(&p->ram_total, &p->ram_free,
                   &p->ram_shared, &p->ram_buffer,
                   &p->swap_total, &p->swap_free);
    gather_loadavg(p->load);
    gather_proc_cpuinfo(p);
    gather_proc_uptime(&p->boottime, &p->idletime);

    version = extract_version(p->basic->uname.release);
    /* This used to have an upper limit of 3.2 from PM-571, but it was 
     * removed because the Linux kernel is changing version numbers too
     * fast to keep updating it.
     */
    if (version >= 2006000) {
        gather_linux_proc26(&p->procs, &p->tasks);
    } else if (version >= 2004000 && version <= 2004999) {
        gather_linux_proc24(&p->procs, &p->tasks);
    } else {
        printerr("Info: Kernel v%lu.%lu.%lu is not supported for proc stats gathering\n",
                 version / 1000000, (version % 1000000) / 1000, version % 1000);
    }

    return p;
}

int printLinuxInfo(FILE *out, int indent, const MachineLinuxInfo *ptr) {
    static const char* state_names[MAX_STATE] = {
        "running",
        "sleeping",
        "waiting",
        "stopped",
        "zombie",
        "other"
    };

    char b[4][32];

    /* <ram .../> tag */
    fprintf(out, "%*s<ram total=\"%s\" free=\"%s\" shared=\"%s\" buffer=\"%s\"/>\n",
            indent, "",
            sizer(b[0], 32, sizeof(ptr->ram_total), &(ptr->ram_total)),
            sizer(b[1], 32, sizeof(ptr->ram_free), &(ptr->ram_free)),
            sizer(b[2], 32, sizeof(ptr->ram_total), &(ptr->ram_shared)),
            sizer(b[3], 32, sizeof(ptr->ram_free), &(ptr->ram_buffer)));

    /* <swap .../> tag */
    fprintf(out, "%*s<swap total=\"%s\" free=\"%s\"/>\n", indent, "",
            sizer(b[0], 32, sizeof(ptr->swap_total), &(ptr->swap_total)),
            sizer(b[1], 32, sizeof(ptr->swap_free), &(ptr->swap_free)));

    /* <boot> element */
    fprintf(out, "%*s<boot idle=\"%.3f\">%s</boot>\n", indent, "",
            ptr->idletime,
            fmtisodate(ptr->boottime.tv_sec, ptr->boottime.tv_usec));

    /* <cpu> element */
    fprintf(out, "%*s<cpu count=\"%hu\" speed=\"%lu\" vendor=\"%s\">%s</cpu>\n",
            indent, "", ptr->cpu_count, ptr->megahertz, ptr->vendor_id,
            ptr->model_name);

    /* <load> element */
    fprintf(out, "%*s<load min1=\"%.2f\" min5=\"%.2f\" min15=\"%.2f\"/>\n",
            indent, "", ptr->load[0], ptr->load[1], ptr->load[2]);

    if (ptr->procs.total && ptr->tasks.total) {
        /* <procs> element */
        fprintf(out, "%*s<procs total=\"%u\"", indent, "", ptr->procs.total);
        for (LinuxState s=S_RUNNING; s<=S_OTHER; ++s) {
            if (ptr->procs.state[s]) {
                fprintf(out, " %s=\"%hu\"", state_names[s], ptr->procs.state[s]);
            }
        }
        fprintf(out, " vmsize=\"%s\" rss=\"%s\"/>\n",
                sizer(b[0], 32, sizeof(ptr->procs.size), &ptr->procs.size),
                sizer(b[1], 32, sizeof(ptr->procs.rss), &ptr->procs.rss));

        /* <task> element */
        fprintf(out, "%*s<task total=\"%u\"", indent, "", ptr->tasks.total);
        for (LinuxState s=S_RUNNING; s<=S_OTHER; ++s) {
            if (ptr->tasks.state[s]) {
                fprintf(out, " %s=\"%hu\"", state_names[s], ptr->tasks.state[s]);
            }
        }

        /* vmsize and rss do not make sense for threads b/c they share memory */

        fprintf(out, "/>\n");
    }

    return 0;
}

int printMachine(FILE *out, int indent, const char* tag, const void* data) {
    /* purpose: format the information into the given stream as XML.
     * paramtr: out (IO): The stream
     *          indent (IN): indentation level
     *          tag (IN): name to use for element tags.
     *          data (IN): MachineLinuxInfo info to print.
     */

    /* sanity check */
    if (data == NULL) {
        return 0;
    }

    const MachineLinuxInfo* ptr = (const MachineLinuxInfo*) data;
    startBasicMachine(out, indent, tag, ptr->basic);
    printLinuxInfo(out, indent+2, ptr);
    finalBasicMachine(out, indent, tag, ptr->basic);

    return 0;
}

void deleteMachine(void* data) {
    /* purpose: destructor
     * paramtr: data (IO): valid MachineLinuxInfo structure to destroy.
     */
    MachineLinuxInfo* ptr = (MachineLinuxInfo*) data;

    if (ptr) {
        deleteBasicMachine(ptr->basic);
        free((void*) ptr);
    }
}

