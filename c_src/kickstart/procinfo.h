#ifndef _PROC_H
#define _PROC_H

#include <sys/resource.h> /* struct rusage */
#include <sys/types.h> /* pid_t */
#include <stdint.h>
#include <inttypes.h>

#include "ptrace.h"

#define SC_ARGS 6

typedef struct _FileInfo {
    char *filename;         /* Name of the file */
    uint64_t bread;         /* Number of bytes read */
    uint64_t bwrite;        /* Number of bytes written */
    uint64_t size;          /* Size of the file at exit */
    uint64_t nread;         /* Number of read operations */
    uint64_t nwrite;        /* Number of write operations */
    uint64_t bseek;         /* Total seek distance */
    uint64_t nseek;         /* Number of seek operations */
    struct _FileInfo *next;
} FileInfo;

typedef struct _SockInfo {
    char *address;          /* Address of peer */
    int port;               /* Port number on peer */
    uint64_t bsend;         /* bytes sent on socket */
    uint64_t brecv;         /* bytes recieved on socket */
    uint64_t nsend;         /* Number of send ops */
    uint64_t nrecv;         /* Number of recv ops */
    struct _SockInfo *next;
} SockInfo;

typedef struct _ProcInfo {
    pid_t pid;              /* Process ID */
    pid_t ppid;             /* Parent pid */
    char *exe;              /* Executable path */
    double start;           /* start time in seconds from epoch */
    double stop;            /* stop time in seconds from epoch */
    double utime;           /* time spent in user mode */
    double stime;           /* time spent in kernel mode */
    double iowait;          /* time spent waiting on I/O */
    int vmpeak;             /* peak virtual memory size in KB */
    int rsspeak;            /* peak physical memory usage in KB */
    uint64_t rchar;         /* characters read by the process */
    uint64_t wchar;         /* characters written by the process */
    uint64_t syscr;         /* number read system calls */
    uint64_t syscw;         /* number write system calls */
    uint64_t read_bytes;    /* file bytes read */
    uint64_t write_bytes;   /* file bytes written */
    uint64_t cancelled_write_bytes; /* bytes written, then deleted before flush */
    int max_threads;        /* Peak number of threads */
    int tot_threads;        /* Total number of threads */
    int fin_threads;        /* Number of threads when process exited */

    /* Keeping track of system calls in progress */
    int insyscall;          /* in a system call? */
    int sc_nr;              /* system call number */
    long sc_args[SC_ARGS];  /* system call arguments */
    long sc_rval;           /* system call return value */

    FileInfo *fds[1024];    /* File descriptor table TODO Make this dynamic. */
    FileInfo *files;        /* Linked list of files accessed */

    SockInfo *sockets;      /* Linked list of sockets */

    long long PAPI_TOT_INS; /* Total instructions */
    long long PAPI_LD_INS;  /* Load instructions */
    long long PAPI_SR_INS;  /* Store instructions */
    long long PAPI_FP_INS;  /* Floating point instructions */
    long long PAPI_FP_OPS;  /* Floating point ops */
    long long PAPI_L3_TCM;  /* L3 cache misses */
    long long PAPI_L2_TCM;  /* L2 cache misses */
    long long PAPI_L1_TCM;  /* L1 cache misses */

    char *cmd;              /* Command line */

    struct _ProcInfo *next;
    struct _ProcInfo *prev;
} ProcInfo;

int procChild();
int procParentTrace(pid_t main, int* main_status, struct rusage* main_usage, ProcInfo** procs, int interpose);
int procParentWait(pid_t main, int* main_status, struct rusage* main_usage, ProcInfo** procs);
int printYAMLProcInfo(FILE *out, int indent, ProcInfo* procs);
void deleteProcInfo(ProcInfo *list);

#endif /* _PROC_H */
