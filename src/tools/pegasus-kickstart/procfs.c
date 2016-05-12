#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <dirent.h>
#include <ctype.h>
#include <stdlib.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

#include "error.h"
#include "procfs.h"

static int mpi_rank = -1;
static in_addr_t hostaddr = 0;

static in_addr_t gethostaddr() {
    /* FIXME This should probably support ipv6 */
    if (hostaddr != 0) {
        return hostaddr;
    }
    char host[256];
    if (gethostname(host, 256) < 0) {
        printerr("gethostname: %s\n", strerror(errno));
        return -1;
    }

    /* XXX Get IPv4 address */
    struct addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET; 
    hints.ai_socktype = SOCK_STREAM;

    struct addrinfo *servinfo;
    int gaierr = getaddrinfo(host, NULL, &hints, &servinfo);
    if (gaierr != 0) {
        printerr("getaddrinfo: %s\n", gai_strerror(gaierr));
        return -1;
    }

    /* XXX getaddrinfo returns a list, just get the first one */
    struct sockaddr_in *p = (struct sockaddr_in *)(servinfo->ai_addr);
    hostaddr = ntohl(p->sin_addr.s_addr);

    freeaddrinfo(servinfo);

    return hostaddr;
}

static int getmpirank() {
    /* If we already looked, then return the value we got before */
    if (mpi_rank >= 0) {
        return mpi_rank;
    }

    /* Try to find a rank environment variable */
    char *envptr = getenv("OMPI_COMM_WORLD_RANK");
    if (envptr == NULL) {
        envptr = getenv("ALPS_APP_PE");
        if (envptr == NULL) {
            envptr = getenv("PMI_RANK");
            if (envptr == NULL) {
                envptr = getenv("PMI_ID");
                if (envptr == NULL) {
                    envptr = getenv("MPIRUN_RANK");
                }
            }
        }
    }

    if (envptr != NULL) {
        mpi_rank = atoi(envptr);
    } else {
        /* If we didn't find anything, then don't look again */
        mpi_rank = 0;
    }

    return mpi_rank;
}

static int startswith(const char *line, const char *tok) {
    return strstr(line, tok) == line;
}

static double get_time() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec + ((double)tv.tv_usec / 1e6);
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
        printerr("Unable to fopen %s: %s\n", statf, strerror(errno));
        return -1;
    }

    unsigned long utime, stime = 0;
    unsigned long long iowait = 0; //delayacct_blkio_ticks

    //pid comm state ppid pgrp session tty_nr tpgid flags minflt cminflt majflt
    //cmajflt utime stime cutime cstime priority nice num_threads itrealvalue
    //starttime vsize rss rsslim startcode endcode startstack kstkesp kstkeip
    //signal blocked sigignore sigcatch wchan nswap cnswap exit_signal
    //processor rt_priority policy delayacct_blkio_ticks guest_time cguest_time
    fscanf(f, "%*d %*s %*c %d %*d %*d %*d %*d %*u %*u %*u %*u %*u %lu "
              "%lu %*d %*d %*d %*d %u %*d %*u %*u %*d %*u %*u "
              "%*u %*u %*u %*u %*u %*u %*u %*u %*u %*u %*u %*d "
              "%*d %*u %*u %llu %*u %*d",
           &(stats->ppid), &utime, &stime, &(stats->threads), &iowait);

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
        printerr("Unable to fopen %s: %s\n", statf, strerror(errno));
        return -1;
    }

    char line[BUFSIZ];
    while (fgets(line, BUFSIZ, f) != NULL) {
        if (startswith(line, "VmPeak")) {
            sscanf(line, "VmPeak: %llu", &(stats->vmpeak));
        } else if (startswith(line, "VmSize")) {
            sscanf(line, "VmSize: %llu", &(stats->vm));
        } else if (startswith(line, "VmHWM")) {
            sscanf(line, "VmHWM: %llu", &(stats->rsspeak));
        } else if (startswith(line, "VmRSS")) {
            sscanf(line, "VmRSS: %llu", &(stats->rss));
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
        printerr("Unable to fopen %s: %s\n", iofile, strerror(errno));
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

static int procfs_read_exe(pid_t pid, char *exe, int maxsize) {
    char exefile[1024];
    snprintf(exefile, 1024, "/proc/%d/exe", pid);
    int size = readlink(exefile, exe, maxsize);
    if (size < 0) {
        printerr("Unable to readlink %s: %s\n", exefile, strerror(errno));
        exe[0] = '\0';
        return -1;
    }
    if (size == maxsize) {
        printerr("Unable to readlink %s: Real path is too long\n", exefile);
        exe[maxsize-1] = '\0';
        return -1;
    }
    exe[size] = '\0';
    return 0;
}


void procfs_stats_init(ProcStats *stats) {
    if (stats == NULL) {
        return;
    }
    memset(stats, 0, sizeof(ProcStats));
}

int procfs_read_stats(pid_t pid, ProcStats *stats) {
    stats->ts = get_time();
    stats->pid = pid;
    stats->ppid = getppid();
    stats->rank = getmpirank();
    stats->host = gethostaddr();
    stats->procs = 1;
    int a = procfs_read_cpu_stats(pid, stats);
    int b = procfs_read_io_stats(pid, stats);
    int c = procfs_read_mem_stats(pid, stats);
    int d = procfs_read_exe(pid, stats->exe, sizeof(stats->exe));
    if ((a + b + c + d) < 0) {
        return -1;
    }
    return 0;
}

/* check to see if str is all decimal digits */
static int isdigits(char *str) {
    for (int i=0; str[i] != '\0'; i++) {
        if (!isdigit(str[i])) {
            return 0;
        }
    }
    return 1;
}

void procfs_read_stats_group(ProcStatsList **listptr) {
    DIR *procdir = opendir("/proc");
    if (procdir == NULL) {
        printerr("Unable to open /proc: %s\n", strerror(errno));
        return;
    }

    in_addr_t host = gethostaddr();
    int mypgid = getpgid(0);

    ProcStatsList *cur = *listptr;

    struct dirent *d;
    for (d = readdir(procdir); d != NULL; d = readdir(procdir)) {
        /* Make sure the name is all digits, indicating a process number */
        if (!isdigits(d->d_name)) {
            continue;
        }

        int pid = atoi(d->d_name);

        /* Check to see if the process is in my process group */
        int pgid = getpgid(pid);
        if (pgid != mypgid) {
            continue;
        }

        /* XXX Since the pids appear in order, we save the starting point for the search */
        cur = procfs_list_find(cur, host, pid);
        if (*listptr == NULL) {
            *listptr = cur;
        }

        assert(cur != NULL);

        /* Get the stats for this process */
        procfs_read_stats(pid, &(cur->stats));
    }

    closedir(procdir);
}

/* Add up all the values in list and store them in result */
void procfs_merge_stats_list(ProcStatsList *list, ProcStats *result, int interval) {
    assert(result != NULL);
    memset(result, 0, sizeof(ProcStats));

    /* Use current process for all the identifying information */
    result->ts = get_time();
    result->host = gethostaddr();
    result->pid = getpid();
    result->ppid = getppid();
    result->rank = getmpirank();
    procfs_read_exe(result->pid, result->exe, sizeof(result->exe));

    for (ProcStatsList *cur = list; cur != NULL; cur = cur->next) {
        ProcStats *stats = &(cur->stats);
        trace("MERGING stats for %s into %s", stats->exe, result->exe);
        result->utime += stats->utime;
        result->stime += stats->stime;
        result->iowait += stats->iowait;
        result->read_bytes += stats->read_bytes;
        result->write_bytes += stats->write_bytes;
        result->rchar += stats->rchar;
        result->wchar += stats->wchar;
        result->syscr += stats->syscr;
        result->syscw += stats->syscw;
#ifdef HAS_PAPI
        result->totins += stats->totins;
        result->ldins += stats->ldins;
        result->srins += stats->srins;
        result->fpins += stats->fpins;
        result->fpops += stats->fpops;
        result->l3misses += stats->l3misses;
        result->l2misses += stats->l2misses;
        result->l1misses += stats->l1misses;
#endif
        result->bsend += stats->bsend;
        result->brecv += stats->brecv;
        /* Only add memory, threads and processes for processes we have seen recently */
        if (stats->ts > result->ts - interval - interval) {
            trace("MERGING memory and threads/procs");
            result->vm += stats->vm;
            result->rss += stats->rss;
            result->procs += stats->procs;
            result->threads += stats->threads;
        }
        /* NOTE: vmpeak and rsspeak don't make sense to add in any case */
    }
}

/* Find list entry for (host,pid) or add one */
ProcStatsList *procfs_list_find(ProcStatsList *list, in_addr_t host, pid_t pid) {
    ProcStatsList *prev = NULL;
    ProcStatsList *cur = list;

    /* Find the right host, or the one this host comes before */
    while (cur != NULL && cur->stats.host < host) {
        prev = cur;
        cur = cur->next;
    }

    /* Find the right process, or the one this process comes before */
    while (cur != NULL && cur->stats.host == host && cur->stats.pid <= pid) {
        if (cur->stats.pid == pid) {
            /* Found it */
            trace("Found monitoring entry for process %lu on %lu", pid, host);
            return cur;
        }
        prev = cur;
        cur = cur->next;
    }

    trace("Creating new monitoring entry for process %lu on %lu", pid, host);

    /* We need to create a new entry for this process */
    ProcStatsList *new = (ProcStatsList *)calloc(1, sizeof(ProcStatsList));
    if (new == NULL) {
        fatal("Unable to allocate monitoring stats list node: %s\n", strerror(errno));
        exit(1);
    }

    /* Add to the list */
    new->next = cur;
    if (prev != NULL) {
        prev->next = new;
    }

    /* Return the entry we created */
    return new;
}

void procfs_list_update(ProcStatsList **list, ProcStats *stats) {
    ProcStatsList *item = procfs_list_find(*list, stats->host, stats->pid);
    if (*list == NULL) {
        *list = item;
    }
    item->stats.pid = stats->pid;
    item->stats.ts = stats->ts;
    item->stats.host = stats->host;
    item->stats.pid = stats->pid;
    item->stats.ppid = stats->ppid;
    item->stats.rank = stats->rank;
    item->stats.rchar = stats->rchar;
    item->stats.wchar = stats->wchar;
    item->stats.syscr = stats->syscr;
    item->stats.syscw = stats->syscw;
    item->stats.read_bytes = stats->read_bytes;
    item->stats.write_bytes = stats->write_bytes;
    item->stats.cancelled_write_bytes = stats->cancelled_write_bytes;
    item->stats.utime = stats->utime;
    item->stats.stime = stats->stime;
    item->stats.iowait = stats->iowait;
    item->stats.vmpeak = stats->vmpeak;
    item->stats.vm = stats->vm;
    item->stats.rsspeak = stats->rsspeak;
    item->stats.rss = stats->rss;
    item->stats.procs = stats->procs;
    item->stats.threads = stats->threads;
#ifdef HAS_PAPI
    item->stats.totins = stats->totins;
    item->stats.ldins = stats->ldins;
    item->stats.srins = stats->srins;
    item->stats.fpins = stats->fpins;
    item->stats.fpops = stats->fpops;
    item->stats.l3misses = stats->l3misses;
    item->stats.l2misses = stats->l2misses;
    item->stats.l1misses = stats->l1misses;
#endif
    item->stats.bsend = stats->bsend;
    item->stats.brecv = stats->brecv;
    strncpy(item->stats.exe, stats->exe, 128);
}

void procfs_free_stats_list(ProcStatsList *list) {
    while (list != NULL) {
        ProcStatsList *tmp = list;
        list = list->next;
        free(tmp);
    }
}

