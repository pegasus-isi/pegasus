#ifndef KICKSTART_PROCFS_H
#define KICKSTART_PROCFS_H

#include <unistd.h>

typedef struct {
    pid_t ppid;
    unsigned long long rchar;
    unsigned long long wchar;
    unsigned long syscr;
    unsigned long syscw;
    unsigned long long read_bytes;
    unsigned long long write_bytes;
    unsigned long long cancelled_write_bytes;
    double utime;
    double stime;
    double iowait;
    unsigned long long vmpeak; /* peak vm size */
    unsigned long long rsspeak; /* peak RSS */
    int threads; /* diff of threads could be negative */
} ProcStats;

void procfs_stats_init(ProcStats *stats);
int procfs_read_stats(pid_t process, ProcStats *stats);
int procfs_read_stats_diff(pid_t pid, ProcStats *prev, ProcStats *diff);
int procfs_read_exe(pid_t pid, char *exe, int maxsize);

#endif
