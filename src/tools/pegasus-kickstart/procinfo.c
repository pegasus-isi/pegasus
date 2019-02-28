/* This module collects I/O, memory and CPU usage info about a job and all 
 * of its child processes. Each child (and grandchild) is traced using 
 * ptrace. When the child is about to exit the tracing proces (kickstart)
 * looks it up in the /proc file system and determines: what the maximum 
 * virtual memory size was (vmpeak), what the maximum physical memory size 
 * was (rsspeak), how much time the process spent in the kernel (stime), 
 * how much time the process spent in user mode (utime) and how much 
 * wall-clock time elapsed between when the process was launched and when 
 * it exited (wtime), how many bytes were read and written, how many
 * characters were read and written, how many read and write system calls
 * were made. The data is added to the invocation record as a series of
 * <proc> entries.
 *
 * NOTE:
 * This won't work if the job requires any executable to be notified when 
 * one of its children stops (i.e. some process needs to wait() for a 
 * child to get a SIGSTOP and then deliver a SIGCONT). See the man page 
 * for ptrace() for more info.
 */

#include <sys/wait.h>
#include <sys/resource.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <limits.h>
#include <errno.h>

#include "procinfo.h"
#include "utils.h"
#include "syscall.h"
#include "error.h"

#ifdef HAS_PTRACE

#include <sys/user.h> /* struct user_regs_struct */

/* Find ProcInfo in a list by pid */
static ProcInfo *proc_lookup(ProcInfo **list, pid_t pid) {
    ProcInfo *cur;
    for (cur = *list; cur!=NULL; cur = cur->next) {
        if (cur->pid == pid) return cur;
    }
    return NULL;
}

/* Create a ProcInfo object */
static ProcInfo *initProcInfo() {
    ProcInfo *new = (ProcInfo *)calloc(1, sizeof(ProcInfo));
    if (new == NULL) {
        printerr("calloc: %s\n", strerror(errno));
        return NULL;
    }
    new->next = NULL;
    new->prev = NULL;
    new->exe = NULL;
    return new;
}

/* Add a new ProcInfo object to the list */
static ProcInfo *proc_add(ProcInfo **list, pid_t pid) {
    ProcInfo *new = initProcInfo();
    if (new == NULL) return NULL;
    new->pid = pid;
    if (*list == NULL) {
        *list = new;
    } else {
        ProcInfo *cur;
        for (cur = *list; cur->next!=NULL; cur = cur->next);
        cur->next = new;
        new->prev = cur;
    }

    return new;
}

/* Get the current time in seconds since the epoch */
static double get_time() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec + ((double)tv.tv_usec / 1e6);
}

/* Read /proc/[pid]/exe */
static int proc_read_exe(ProcInfo *item) {
    char link[PATH_MAX];
    char exe[PATH_MAX];
    int size;
    sprintf(link, "/proc/%d/exe", item->pid);
    size = readlink(link, exe, PATH_MAX);
    if (size < 0) {
        printerr("readlink: %s\n", strerror(errno));
        return -1;
    }
    exe[size] = '\0';
    item->exe = strdup(exe);
    if (item->exe == NULL) {
        printerr("strdup: %s\n", strerror(errno));
        return -1;
    }
    return size;
}

/* Return 1 if line begins with tok */
static int startswith(const char *line, const char *tok) {
    return strncmp(line, tok, strlen(tok)) == 0;
}

/* Read /proc/[pid]/status to get memory usage */
static int proc_read_meminfo(ProcInfo *item) {
    char statf[BUFSIZ];
    if (sprintf(statf,"/proc/%d/status", item->pid) < 0) {
        perror("sprintf");
        return -1;
    }

    /* If the status file is missing, then just skip it */
    if (access(statf, F_OK) < 0) {
        return 0;
    }

    FILE *f = fopen(statf,"r");
    if (f == NULL) {
        perror("fopen");
        return -1;
    }

    char line[BUFSIZ];
    while (fgets(line, BUFSIZ, f) != NULL) {
        if (startswith(line, "PPid")) {
            sscanf(line,"PPid:%d\n",&(item->ppid));
        } else if (startswith(line, "Threads")) {
            sscanf(line,"Threads:%d\n",&(item->fin_threads));
        } else if (startswith(line,"VmPeak")) {
            sscanf(line,"VmPeak:%d kB\n",&(item->vmpeak));
        } else if (startswith(line,"VmHWM")) {
            sscanf(line,"VmHWM:%d kB\n",&(item->rsspeak));
        }
    }

    if (ferror(f)) {
        fclose(f);
        return -1;
    }

    return fclose(f);
}

/* Read /proc/[pid]/stat to get CPU usage */
static int proc_read_statinfo(ProcInfo *item) {
    char statf[BUFSIZ];
    if (sprintf(statf,"/proc/%d/stat", item->pid) < 0) {
        perror("sprintf");
        return -1;
    }

    /* If the stat file is missing, then just skip it */
    if (access(statf, F_OK) < 0) {
        return 0;
    }

    FILE *f = fopen(statf,"r");
    if (f == NULL) {
        perror("fopen");
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

    /* Adjust by number of clock ticks per second */
    long clocks = sysconf(_SC_CLK_TCK);
    item->utime = ((double)utime) / clocks;
    item->stime = ((double)stime) / clocks;
    item->iowait = ((double)iowait) / clocks;

    if (ferror(f)) {
        fclose(f);
        return -1;
    }

    return fclose(f);
}

/* Read /proc/[pid]/io to get I/O usage */
static int proc_read_io(ProcInfo *item) {
    char iofile[BUFSIZ];
    if (sprintf(iofile, "/proc/%d/io", item->pid) < 0) {
        perror("sprintf");
        return -1;
    }

    /* This proc file was added in Linux 2.6.20. It won't be
     * there on older kernels, or on kernels without task IO 
     * accounting. If it is missing, just bail out.
     */
    if (access(iofile, F_OK) < 0) {
        return 0;
    }

    FILE *f = fopen(iofile, "r");
    if (f == NULL) {
        perror("fopen");
        return -1;
    }

    char line[BUFSIZ];
    while (fgets(line, BUFSIZ, f) != NULL) {
        if (startswith(line, "rchar")) {
            sscanf(line,"rchar: %"SCNu64"\n", &(item->rchar));
        } else if (startswith(line, "wchar")) {
            sscanf(line,"wchar: %"SCNu64"\n", &(item->wchar));
        } else if (startswith(line,"syscr")) {
            sscanf(line,"syscr: %"SCNu64"\n", &(item->syscr));
        } else if (startswith(line,"syscw")) {
            sscanf(line,"syscw: %"SCNu64"\n", &(item->syscw));
        } else if (startswith(line,"read_bytes")) {
            sscanf(line,"read_bytes: %"SCNu64"\n",&(item->read_bytes));
        } else if (startswith(line,"write_bytes")) {
            sscanf(line,"write_bytes: %"SCNu64"\n",&(item->write_bytes));
        } else if (startswith(line,"cancelled_write_bytes")) {
            sscanf(line,"cancelled_write_bytes: %"SCNu64"\n",&(item->cancelled_write_bytes));
        }
    }

    if (ferror(f)) {
        fclose(f);
        return -1;
    }

    return fclose(f);
}
#endif

int procChild() {
#ifdef HAS_PTRACE
    return ptrace(PTRACE_TRACEME, 0, NULL, NULL);
#else
    return 0;
#endif
}

/* Do the parent part of fork() */
int procParentTrace(pid_t main, int *main_status, struct rusage *main_usage, ProcInfo **procs, int interpose) {
#ifndef HAS_PTRACE
    return procParentWait(main, main_status, main_usage, procs);
#else

    /* If we are interposing system calls, then we need to call PTRACE_SYSCALL */
    int PTRACE_NEXTSTOP = PTRACE_CONT;
    if (interpose) {
        PTRACE_NEXTSTOP = PTRACE_SYSCALL;
    }

    /* TODO We need to find a way to stop tracing all of our children 
     * if we encounter an error so that we don't leave a lot of processes
     * hanging around in the t state
     */

    /* Event loop */
    while (1) {

        /* Wait for a child to stop or exit */
        int status = 0;
        struct rusage usage;
        /* __WALL is needed so that we can wait on threads too */
        pid_t cpid = wait4(0, &status, __WALL, &usage);
        if (cpid < 0) {
            if (errno == ECHILD) {
                /* No more children, then we are done */
                break;
            } else if (errno == EINTR) {
                /* If interrupted, then go again */
                continue;
            } else {
                perror("wait4");
                return -1;
            }
        }

        /* find the child */
        ProcInfo *child = proc_lookup(procs, cpid);

        /* if not found, then it is new, so add it */
        if (child == NULL) {
            child = proc_add(procs, cpid);
            if (child == NULL) return -1;
            child->start = get_time();

            /* TODO Trace exec so we can get the original exe path.
             * Right now shell scripts are reported as the shell, not
             * as the original script
             */
            unsigned long options = PTRACE_O_TRACEEXIT|
                PTRACE_O_TRACEFORK|PTRACE_O_TRACEVFORK|
                PTRACE_O_TRACECLONE;

            /* If system call interposition is enabled, then tell
             * the kernel to set bit 7 in the signal number so we can
             * distinguish system calls from other events.
             */
            if (interpose) {
                options |= PTRACE_O_TRACESYSGOOD;
            }

            /* Set the tracing options for this child so that we
             * can see when it creates children and when it exits
             */
            if (ptrace(PTRACE_SETOPTIONS, cpid, NULL, options)) {
                perror("ptrace(PTRACE_SETOPTIONS)");
                return -1;
            }

            /* The new child may have inherited some file descriptors
             * Fill in the initial descriptor table for the process
             */
            if (interpose) {
                initFileInfo(child);
            }
        }

        /* child exited */
        if (WIFEXITED(status)) {
            /* If the exiting child was the main process, then
             * store its status and usage
             */
            if (main == cpid) {
                memcpy(main_usage, &usage, sizeof(struct rusage));
            }

            /* Now that the child is done, stat all the files it accessed */
            if (interpose) {
                finiFileInfo(child);
            }
        }

        /* child stopped */
        if (WIFSTOPPED(status)) {

            /* because of an event we wanted to see */
            if(WSTOPSIG(status) == SIGTRAP) {
                int event = status >> 16;
                if (event == PTRACE_EVENT_EXIT) {
                    /* Child exited, grab its final stats */
                    child->stop = get_time();
                    if (proc_read_exe(child) < 0) {
                        perror("proc_read_exe");
                    }
                    if (proc_read_meminfo(child) < 0) {
                        perror("proc_read_meminfo");
                    }
                    if (proc_read_statinfo(child) < 0) {
                        perror("proc_read_statinfo");
                    }
                    if (proc_read_io(child) < 0) {
                        perror("proc_read_io");
                    }

                    /* If this is the main process, then get the exit status.
                     * We have to do this here because the normal exit status
                     * we get from wait4 above does not properly capture the 
                     * exit status of signalled processes.
                     */
                    if (cpid == main) {
                        unsigned long event_status;
                        if (ptrace(PTRACE_GETEVENTMSG, cpid, NULL, &event_status) < 0) {
                            perror("ptrace(PTRACE_GETEVENTMSG)");
                            return -1;
                        }
                        *main_status = event_status;
                    }
                }

                /* tell child to continue */
                if (ptrace(PTRACE_NEXTSTOP, cpid, NULL, NULL)) {
                    perror("ptrace(PTRACE_NEXTSTOP)");
                    return -1;
                }
            }

            /* stopped because it is entering or leaving a system call.
             * bit 7 is set because of PTRACE_O_TRACESYSGOOD.
             */
            else if(WSTOPSIG(status) == (SIGTRAP|0x80)) {
                struct user_regs_struct regs;

                if (ptrace(PTRACE_GETREGS, cpid, NULL, &regs)) {
                    perror("PTRACE_GETREGS");
                    return -1;
                }

                if (child->insyscall) {
                    child->sc_rval = SC_RVAL(regs);
                    if (child->sc_nr <= MAX_SYSCALL) {
                        int (*handler)(ProcInfo *c) = syscalls[child->sc_nr].handler;
                        if (handler) {
                            handler(child);
                        }
                    }
                    child->insyscall = 0;
                } else {
                    child->sc_nr = SC_NR(regs);
                    child->sc_args[0] = SC_ARG0(regs);
                    child->sc_args[1] = SC_ARG1(regs);
                    child->sc_args[2] = SC_ARG2(regs);
                    child->sc_args[3] = SC_ARG3(regs);
                    child->sc_args[4] = SC_ARG4(regs);
                    child->sc_args[5] = SC_ARG5(regs);
                    child->insyscall = 1;
                }

                if (ptrace(PTRACE_NEXTSTOP, cpid, NULL, NULL)) {
                    perror("ptrace(PTRACE_NEXTSTOP)");
                    return -1;
                }
            }

            /* stopped because it got a signal */
            else {
                int signal = WSTOPSIG(status);

                /* Mask the STOP signal. Since we are running a batch job
                 * we should assume that the children never need to be sent
                 * SIGSTOP. It looks like shells try to send SIGSTOP to all
                 * the processes they fork so that they can do something
                 * and send them SIGCONT. The problem is that this does not
                 * work under ptrace because wait() does not return in the
                 * parent, rather it returns in the tracing process so there
                 * is no way to tell the parent that the child stopped, and
                 * as a result the parent never sends SIGCONT and the job
                 * hangs. It is not entirely clear if that explanation is
                 * correct, but blocking STOP (and for completeness TSTP)
                 * fixes the problem. XXX Maybe it is possible to send
                 * SIGSTOP to the parent directly?
                 */
                if (signal == SIGSTOP || signal == SIGTSTP) {
                    signal = 0;
                }

                /* pass the signal on to the child */
                if (ptrace(PTRACE_NEXTSTOP, cpid, NULL, signal)) {
                    perror("ptrace(PTRACE_NEXTSTOP)");
                    return -1;
                }
            }
        }
    }

    return 0;
#endif
}

int procParentWait(pid_t main, int *main_status,  struct rusage *main_usage, ProcInfo **procs) {
    /* Just wait for the child */
    while (wait4(main, main_status, 0, main_usage) < 0) {
        if (errno != EINTR) {
            perror("wait4");
            *main_status = -42;
        }
    }
    return *main_status;
}

static int printXMLFileInfo(FILE *out, int indent, FileInfo *files) {
    FileInfo *i;
    for (i = files; i != NULL; i = i->next) {
        fprintf(out, "%*s<file name=\"%s\" "
                "bread=\"%"PRIu64"\" nread=\"%"PRIu64"\" "
                "bwrite=\"%"PRIu64"\" nwrite=\"%"PRIu64"\" "
                "bseek=\"%"PRIu64"\" nseek=\"%"PRIu64"\" "
                "size=\"%"PRIu64"\"/>\n",
                indent, "", i->filename, i->bread, i->nread,
                i->bwrite, i->nwrite, i->bseek, i->nseek, i->size);
    }
    return 0;
}

static int printXMLSockInfo(FILE *out, int indent, SockInfo *sockets) {
    SockInfo *i;
    for (i = sockets; i != NULL; i = i->next) {
        fprintf(out, "%*s<socket address=\"%s\" port=\"%d\" "
                "brecv=\"%"PRIu64"\" bsend=\"%"PRIu64"\" nrecv=\"%"PRIu64"\" nsend=\"%"PRIu64"\"/>\n",
                indent, "", i->address, i->port, i->brecv, i->bsend, i->nrecv, i->nsend);
    }
    return 0;
}

/* Write <proc> records to buffer */
int printYAMLProcInfo(FILE *out, int indent, ProcInfo* procs) {
    fprintf(out, "%*sprocs:\n", indent, "");
    ProcInfo *i;
    for (i = procs; i; i = i->next) {
        /* This means that the trace file was probably incomplete */
        if (i->pid == 0) {
            printerr("Bad <proc> record: trace file may be incomplete");
        }

        fprintf(out, "%*s  %d:\n"
                     "%*s    ppid: %d\n"
                     "%*s    pid: %d\n"
                     "%*s    exe: %s\n"
                     "%*s    start: %lf\n"
                     "%*s    stop: %lf\n"
                     "%*s    utime: %.3lf\n"
                     "%*s    stime: %.3lf\n"
                     "%*s    iowait: %.3lf\n"
                     "%*s    finthreads: %d\n"
                     "%*s    maxthreads: %d\n"
                     "%*s    totthreads: %d\n"
                     "%*s    vmpeak: %d\n"
                     "%*s    rsspeak: %d\n"
                     "%*s    rchar: %"PRIu64"\n"
                     "%*s    wchar: %"PRIu64"\n"
                     "%*s    rbytes: %"PRIu64"\n"
                     "%*s    wbytes: %"PRIu64"\n"
                     "%*s    cwbytes: %"PRIu64"\n"
                     "%*s    syscr: %"PRIu64"\n"
                     "%*s    syscw: %"PRIu64"\n",
                     indent, "", i->pid,
                     indent, "", i->ppid, 
                     indent, "", i->pid, 
                     indent, "", i->exe,
                     indent, "", i->start,
                     indent, "", i->stop,
                     indent, "", i->utime,
                     indent, "", i->stime,
                     indent, "", i->iowait,
                     indent, "", i->fin_threads, 
                     indent, "", i->max_threads, 
                     indent, "", i->tot_threads,
                     indent, "", i->vmpeak, 
                     indent, "", i->rsspeak,
                     indent, "", i->rchar,
                     indent, "", i->wchar,
                     indent, "", i->read_bytes,
                     indent, "", i->write_bytes,
                     indent, "", i->cancelled_write_bytes,
                     indent, "", i->syscr,
                     indent, "", i->syscw
        );
#ifdef HAS_PAPI
        if (i->PAPI_TOT_INS > 0) {
            fprintf(out, " totins=\"%lld\"", i->PAPI_TOT_INS);
        }
        if (i->PAPI_LD_INS > 0) {
            fprintf(out, " ldins=\"%lld\"", i->PAPI_LD_INS);
        }
        if (i->PAPI_SR_INS > 0) {
            fprintf(out, " srins=\"%lld\"", i->PAPI_SR_INS);
        }
        if (i->PAPI_FP_INS > 0) {
            fprintf(out, " fpins=\"%lld\"", i->PAPI_FP_INS);
        }
        if (i->PAPI_FP_OPS > 0) {
            fprintf(out, " fpops=\"%lld\"", i->PAPI_FP_OPS);
        }
        if (i->PAPI_L3_TCM > 0) {
            fprintf(out, " l3misses=\"%lld\"", i->PAPI_L3_TCM);
        }
        if (i->PAPI_L2_TCM > 0) {
            fprintf(out, " l2misses=\"%lld\"", i->PAPI_L2_TCM);
        }
        if (i->PAPI_L1_TCM > 0) {
            fprintf(out, " l1misses=\"%lld\"", i->PAPI_L1_TCM);
        }
#endif
        if ( ! (i->cmd == NULL && i->files == NULL && i->sockets == NULL)) {
            fprintf(out, ">\n");
            if (i->cmd != NULL) {
                fprintf(out, "%*s<cmd>", indent+2, "");
                yamlquote(out, i->cmd, strlen(i->cmd));
                fprintf(out, "</cmd>\n");
            }
            printXMLFileInfo(out, indent+2, i->files);
            printXMLSockInfo(out, indent+2, i->sockets);
        }
    }
    return 0;
}

/* Delete all the ProcInfo objects in a list */
void deleteProcInfo(ProcInfo *procs) {
    while (procs != NULL) {
        ProcInfo *p = procs;
        if (p->cmd != NULL) {
            free(p->cmd);
        }
        FileInfo *files = p->files;
        while (files != NULL) {
            FileInfo *f = files;
            files = files->next;
            free(f);
        }
        SockInfo *sockets = p->sockets;
        while (sockets != NULL) {
            SockInfo *s = sockets;
            sockets = sockets->next;
            free(s);
        }
        procs = procs->next;
        free(p);
    }
}

