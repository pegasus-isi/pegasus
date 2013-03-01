#ifndef _PROC_H
#define _PROC_H

#include <stdint.h>
#include <inttypes.h>

#include "ptrace.h"

typedef struct _ProcInfo {
    int pid; /* Thread ID (main tid==pid) */
    int ppid; /* Parent pid */
    int tgid; /* Thread group ID (i.e. pid) */
    char *exe; /* Executable path */
    double start; /* start time in seconds from epoch */
    double stop; /* stop time in seconds from epoch */
    double utime; /* time spent in user mode */
    double stime; /* time spent in kernel mode */
    int vmpeak; /* peak virtual memory size in KB */
    int rsspeak; /* peak physical memory usage in KB */
    uint64_t rchar; /* characters read by the process */
    uint64_t wchar; /* characters written by the process */
    uint64_t syscr; /* number read system calls */
    uint64_t syscw; /* number write system calls */
    uint64_t read_bytes; /* file bytes read */
    uint64_t write_bytes; /* file bytes written */
    uint64_t cancelled_write_bytes; /* bytes written, then deleted before flush */
    struct _ProcInfo *next;
    struct _ProcInfo *prev;
} ProcInfo;

int procChild();
int procParentTrace(pid_t main, int* main_status, struct rusage* main_usage, ProcInfo** procs);
int procParentWait(pid_t main, int* main_status, struct rusage* main_usage, ProcInfo** procs);
int printXMLProcInfo(char* buffer, size_t size, size_t* len, size_t indent, ProcInfo* procs);
void deleteProcInfo(ProcInfo *list);

#endif /* _PROC_H */
