#include <stdio.h>
#include <string.h>
#include <errno.h>

#include "procfs.h"

static int startswith(const char *line, const char *tok) {
    return strstr(line, tok) == line;
}

/* Read /proc/self/stat to get CPU usage and returns a structure with this information */
static int procfs_read_cpu_stats(pid_t pid, ProcStats *stats) {
    char statf[1024];
    snprintf(statf, 1024, "/proc/%d/stat", pid);

    /* If the stat file is missing, then just skip it */
    if (access(statf, F_OK) < 0) {
        return -1;
    }

    FILE *f = fopen(statf,"r");
    if (f == NULL) {
        fprintf(stderr, "Unable to fopen %s: %s", statf, strerror(errno));
        return -1;
    }

    unsigned long utime, stime = 0;
    unsigned long long iowait = 0; //delayacct_blkio_ticks

    //pid comm state ppid pgrp session tty_nr tpgid flags minflt cminflt majflt
    //cmajflt utime stime cutime cstime priority nice num_threads itrealvalue
    //starttime vsize rss rsslim startcode endcode startstack kstkesp kstkeip
    //signal blocked sigignore sigcatch wchan nswap cnswap exit_signal
    //processor rt_priority policy delayacct_blkio_ticks guest_time cguest_time
    fscanf(f, "%*d %*s %*c %*d %*d %*d %*d %*d %*u %*u %*u %*u %*u %lu "
              "%lu %*d %*d %*d %*d %*d %*d %*u %*u %*d %*u %*u "
              "%*u %*u %*u %*u %*u %*u %*u %*u %*u %*u %*u %*d "
              "%*d %*u %*u %llu %*u %*d",
           &utime, &stime, &iowait);

    fclose(f);

    /* Adjust by number of clock ticks per second */
    long clocks = sysconf(_SC_CLK_TCK);

    stats->utime = ((double)utime) / clocks;
    stats->stime = ((double)stime) / clocks;
    stats->iowait = ((double)iowait) / clocks;

    return 0;
}

/* Read useful information from /proc/self/status and returns a structure with this information */
static int procfs_read_mem_stats(pid_t pid, ProcStats *stats) {
    char statf[1024];
    snprintf(statf, 1024, "/proc/%d/status", pid);

    /* If the status file is missing, then just skip it */
    if (access(statf, F_OK) < 0) {
        return -1;
    }

    FILE *f = fopen(statf, "r");
    if (f == NULL) {
        fprintf(stderr, "Unable to fopen %s: %s", statf, strerror(errno));
        return -1;
    }

    char line[BUFSIZ];
    while (fgets(line, BUFSIZ, f) != NULL) {
        if (startswith(line, "PPid")) {
            sscanf(line,"PPid:%d\n",&(stats->ppid));
        } else if (startswith(line,"VmSize")) {
            sscanf(line, "VmSize: %llu", &(stats->vmpeak));
        } else if (startswith(line,"VmRSS")) {
            sscanf(line, "VmRSS: %llu", &(stats->rsspeak));
        } else if (startswith(line,"Threads")) {
            sscanf(line, "Threads: %d", &(stats->threads));
        }
    }

    fclose(f);

    return 0;
}

/* Read /proc/self/io to get I/O usage */
static int procfs_read_io_stats(pid_t pid, ProcStats *stats) {
    char iofile[1024];
    snprintf(iofile, 1024, "/proc/%d/io", pid);

    /* This proc file was added in Linux 2.6.20. It won't be
     * there on older kernels, or on kernels without task IO
     * accounting. If it is missing, just bail out.
     */
    if (access(iofile, F_OK) < 0) {
        return -1;
    }

    FILE *f = fopen(iofile, "r");
    if (f == NULL) {
        fprintf(stderr, "Unable to fopen %s: %s", iofile, strerror(errno));
        return -1;
    }

    char line[BUFSIZ];
    while (fgets(line, BUFSIZ, f) != NULL) {
        if (startswith(line, "rchar")) {
            sscanf(line, "rchar: %llu", &(stats->rchar));
        } else if (startswith(line, "wchar")) {
            sscanf(line, "wchar: %llu", &(stats->wchar));
        } else if (startswith(line,"syscr")) {
            sscanf(line, "syscr: %lu", &(stats->syscr));
        } else if (startswith(line,"syscw")) {
            sscanf(line, "syscw: %lu", &(stats->syscw));
        } else if (startswith(line,"read_bytes")) {
            sscanf(line, "read_bytes: %llu", &(stats->read_bytes));
        } else if (startswith(line,"write_bytes")) {
            sscanf(line, "write_bytes: %llu", &(stats->write_bytes));
        } else if (startswith(line,"cancelled_write_bytes")) {
            sscanf(line, "cancelled_write_bytes: %llu", &(stats->cancelled_write_bytes));
        }
    }

    fclose(f);

    return 0;
}

void procfs_stats_init(ProcStats *stats) {
    if (stats == NULL) {
        return;
    }
    memset(stats, 0, sizeof(ProcStats));
}

int procfs_read_stats(pid_t pid, ProcStats *stats) {
    int a = procfs_read_cpu_stats(pid, stats);
    int b = procfs_read_io_stats(pid, stats);
    int c = procfs_read_mem_stats(pid, stats);
    if ((a + b + c) < 0) {
        return -1;
    }
    return 0;
}

int procfs_read_stats_diff(pid_t pid, ProcStats *prev, ProcStats *diff) {
    ProcStats new;
    procfs_stats_init(&new);

    /* Read the latest values */
    int result = procfs_read_stats(pid, &new);

    /* Compute the delta */
    diff->utime = new.utime - prev->utime;
    diff->stime = new.stime - prev->stime;
    diff->iowait = new.iowait - prev->iowait;
    diff->vmpeak = new.vmpeak - prev->vmpeak;
    diff->rsspeak = new.rsspeak - prev->rsspeak;
    diff->threads = new.threads - prev->threads;
    diff->rchar = new.rchar - prev->rchar;
    diff->wchar = new.wchar - prev->wchar;
    diff->syscr = new.syscr - prev->syscr;
    diff->syscw = new.syscw - prev->syscw;
    diff->read_bytes = new.read_bytes - prev->read_bytes;
    diff->write_bytes = new.write_bytes - prev->write_bytes;
    diff->cancelled_write_bytes = new.cancelled_write_bytes - prev->cancelled_write_bytes;

    /* Save the new values */
    prev->utime = new.utime;
    prev->stime = new.stime;
    prev->iowait = new.iowait;
    prev->vmpeak = new.vmpeak;
    prev->rsspeak = new.rsspeak;
    prev->threads = new.threads;
    prev->rchar = new.rchar;
    prev->wchar = new.wchar;
    prev->syscr = new.syscr;
    prev->syscw = new.syscw;
    prev->read_bytes = new.read_bytes;
    prev->write_bytes = new.write_bytes;
    prev->cancelled_write_bytes = new.cancelled_write_bytes;

    return result;
}

int procfs_read_exe(pid_t pid, char *exe, int maxsize) {
    char exefile[1024];
    snprintf(exefile, 1024, "/proc/%d/exe", pid);
    int size = readlink(exefile, exe, maxsize);
    if (size < 0) {
        fprintf(stderr, "Unable to readlink %s: %s", exefile, strerror(errno));
        exe[0] = '\0';
        return -1;
    }
    if (size == maxsize) {
        fprintf(stderr, "Unable to readlink %s: Real path is too long", exefile);
        exe[maxsize-1] = '\0';
        return -1;
    }
    exe[size] = '\0';
    return 0;
}

