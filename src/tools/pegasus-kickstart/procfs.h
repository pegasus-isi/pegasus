#ifndef KICKSTART_PROCFS_H
#define KICKSTART_PROCFS_H

#include <unistd.h>

typedef struct _ProcStats {
    pid_t pid;
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
    unsigned long long vm; /* current VM size */
    unsigned long long rsspeak; /* peak RSS */
    unsigned long long rss; /* current RSS */
    int threads; /* diff of threads could be negative */
    char exe[255];
    char state;
} ProcStats;

typedef struct _ProcStatsList {
    ProcStats stats;
    struct _ProcStatsList *next;
} ProcStatsList;

void procfs_stats_init(ProcStats *stats);
int procfs_read_stats(pid_t process, ProcStats *stats);
int procfs_read_stats_diff(pid_t pid, ProcStats *prev, ProcStats *diff);
void procfs_read_stats_group(ProcStatsList **listptr);
void procfs_add_stats_list(ProcStatsList *list, ProcStats *result);
void procfs_free_stats_list(ProcStatsList *list);

#endif
