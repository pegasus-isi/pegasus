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
#include <sys/types.h>
#include <errno.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <signal.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <fcntl.h>
#include <stdio.h>
#include <libgen.h>
#include <dirent.h>

#include "utils.h"
#include "appinfo.h"
#include "statinfo.h"
#include "mysystem.h"
#include "procinfo.h"
#include "error.h"

/* Find the path to the interposition library */
static int findInterposeLibrary(char *path, int pathsize) {
    char kickstart[BUFSIZ];
    char lib[BUFSIZ];

    // Get the full path to the kickstart executable
    int size = readlink("/proc/self/exe", kickstart, BUFSIZ);
    if (size < 0) {
        printerr("Unable to readlink /proc/self/exe");
        return -1;
    }

    // Find the directory containing kickstart
    char *dir = dirname(kickstart);

    // Check if the library is in the kickstart dir
    strcpy(lib, dir);
    strcat(lib, "/libinterpose.so");
    if (access(lib, R_OK) == 0) {
        strncpy(path, lib, pathsize);
        return 0;
    }

    char *homedir = dirname(dir);

    // Try the ../lib/pegasus/ dir instead
    strcpy(lib, homedir);
    strcat(lib, "/lib/pegasus/libinterpose.so");
    if (access(lib, R_OK) == 0) {
        strncpy(path, lib, pathsize);
        return 0;
    }

    // Try the ../lib64/pegasus/ dir
    strcpy(lib, homedir);
    strcat(lib, "/lib64/pegasus/libinterpose.so");
    if (access(lib, R_OK) == 0) {
        strncpy(path, lib, pathsize);
        return 0;
    }

    // Not found
    return -1;
}

static FileInfo *readTraceFileRecord(const char *buf, FileInfo *files) {
    char filename[BUFSIZ];
    size_t size = 0;
    size_t bread = 0;
    size_t bwrite = 0;
    size_t nread = 0;
    size_t nwrite = 0;
    size_t bseek = 0;
    size_t nseek = 0;

    if (sscanf(buf, "file: '%[^']' %lu %lu %lu %lu %lu %lu %lu\n",
               filename, &size, &bread, &bwrite, &nread, &nwrite, &bseek, &nseek) != 8) {
        printerr("Invalid file record: %s", buf);
        return files;
    }

    /* Look for a duplicate file in the list of files */
    FileInfo *file = NULL;
    FileInfo *last = NULL;
    for (file = files; file != NULL; file = file->next) {
        if (strcmp(filename, file->filename) == 0) {
            /* Found a duplicate */
            break;
        }

        /* Keep track of the last file in the list */
        last = file;
    }

    if (file == NULL) {
        /* No duplicate found */
        file = (FileInfo *)calloc(sizeof(FileInfo), 1);
        if (file == NULL) {
            printerr("calloc: %s\n", strerror(errno));
            return files;
        }
        char *temp = strdup(filename);
        if (temp == NULL) {
            free(file);
            printerr("strdup: %s\n", strerror(errno));
            return files;
        }
        file->filename = temp;
        file->size = size;
        file->bread = bread;
        file->bwrite = bwrite;
        file->nread = nread;
        file->nwrite = nwrite;
        file->bseek = bseek;
        file->nseek = nseek;

        if (files == NULL) {
            /* List was empty */
            files = file;
        } else {
            /* Add to end of list */
            last->next = file;
        }
    } else {
        /* Duplicate found, increment counters */
        file->size = file->size > size ? file->size : size; /* max */
        file->bread += bread;
        file->bwrite += bwrite;
        file->nread += nread;
        file->nwrite += nwrite;
        file->bseek += bseek;
        file->nseek += nseek;
    }

    return files;
}

static SockInfo *readTraceSocketRecord(const char *buf, SockInfo *sockets) {
    char address[BUFSIZ];
    int port = 0;
    size_t brecv = 0;
    size_t bsend = 0;
    size_t nrecv = 0;
    size_t nsend = 0;
    if (sscanf(buf, "socket: %s %d %lu %lu %lu %lu\n", address, &port, &brecv, &bsend, &nrecv, &nsend) != 6) {
        printerr("Invalid socket record: %s", buf);
        return sockets;
    }

    /* Look for a duplicate socket in list of sockets */
    SockInfo *sock = NULL;
    SockInfo *last = NULL;
    for (sock = sockets; sock != NULL; sock = sock->next) {
        if (port == sock->port && strcmp(address, sock->address) == 0) {
            /* Found a duplicate */
            break;
        }

        /* Keep track of the last socket in the list */
        last = sock;
    }

    if (sock == NULL) {
        /* No duplicate found */
        sock = (SockInfo *)calloc(sizeof(SockInfo), 1);
        if (sock == NULL) {
            printerr("calloc: %s\n", strerror(errno));
            return sockets;
        }
        char *temp = strdup(address);
        if (temp == NULL) {
            free(sock);
            printerr("strdup: %s\n", strerror(errno));
            return sockets;
        }
        sock->address = temp;
        sock->port = port;
        sock->brecv = brecv;
        sock->bsend = bsend;
        sock->nrecv = nrecv;
        sock->nsend = nsend;

        if (sockets == NULL) {
            /* List was empty */
            sockets = sock;
        } else {
            /* Add to end of list */
            last->next = sock;
        }
    } else {
        /* Duplicate found, increment counters */
        sock->brecv += brecv;
        sock->bsend += bsend;
        sock->nrecv += nrecv;
        sock->nsend += nsend;
    }

    return sockets;
}

/* Return 1 if line begins with tok */
static int startswith(const char *line, const char *tok) {
    return strstr(line, tok) == line;
}

static ProcInfo *processTraceFile(const char *fullpath) {
    FILE *trace = fopen(fullpath, "r");
    if (trace == NULL) {
        printerr("Unable to open trace file '%s': %s\n",
                fullpath, strerror(errno));
        return NULL;
    }

    ProcInfo *procs = NULL;
    ProcInfo *proc = NULL;
    ProcInfo *lastproc = NULL;

    int fork = 0;

    /* Read data from the trace file */
    int lines = 0;
    char line[BUFSIZ];
    long long llval;
    while (fgets(line, BUFSIZ, trace) != NULL) {
        lines++;

        if (proc == NULL) {
            proc = (ProcInfo *)calloc(sizeof(ProcInfo), 1);
            if (proc == NULL) {
                printerr("calloc: %s\n", strerror(errno));
                goto exit;
            }
            fork = 0;
        }

        if (proc != lastproc) {
            if (procs == NULL) {
                procs = proc;
            }
            if (lastproc != NULL) {
                lastproc->next = proc;
            }
            proc->prev = lastproc;
            lastproc = proc;
        }

        if (startswith(line, "file:")) {
            proc->files = readTraceFileRecord(line, proc->files);
        } else if (startswith(line, "socket:")) {
            proc->sockets = readTraceSocketRecord(line, proc->sockets);
        } else if (startswith(line, "exe:")) {
            char exe[BUFSIZ];
            sscanf(line, "exe: %s\n", exe);
            proc->exe = strdup(exe);
            if (proc->exe == NULL) {
                printerr("strdup: %s\n", strerror(errno));
                break;
            }
        } else if (startswith(line, "Pid")) {
            sscanf(line, "Pid:%d\n", &(proc->pid));
        } else if (startswith(line, "PPid")) {
            sscanf(line,"PPid:%d\n", &(proc->ppid));
        } else if (startswith(line,"VmPeak")) {
            sscanf(line,"VmPeak:%d kB\n", &(proc->vmpeak));
        } else if (startswith(line,"VmHWM")) {
            sscanf(line,"VmHWM:%d kB\n", &(proc->rsspeak));
        } else if (startswith(line, "threads")) {
            sscanf(line,"threads: %d %d %d\n", &(proc->fin_threads),
                    &(proc->max_threads), &(proc->tot_threads));
        } else if (startswith(line, "utime:")) {
            sscanf(line,"utime:%lf\n", &(proc->utime));
        } else if (startswith(line, "stime:")) {
            sscanf(line,"stime:%lf\n", &(proc->stime));
        } else if (startswith(line, "iowait:")) {
            sscanf(line,"iowait:%lf\n", &(proc->iowait));
        } else if (startswith(line, "rchar")) {
            sscanf(line,"rchar: %"SCNu64"\n", &(proc->rchar));
        } else if (startswith(line, "wchar")) {
            sscanf(line,"wchar: %"SCNu64"\n", &(proc->wchar));
        } else if (startswith(line,"syscr")) {
            sscanf(line,"syscr: %"SCNu64"\n", &(proc->syscr));
        } else if (startswith(line,"syscw")) {
            sscanf(line,"syscw: %"SCNu64"\n", &(proc->syscw));
        } else if (startswith(line,"read_bytes")) {
            sscanf(line,"read_bytes: %"SCNu64"\n",&(proc->read_bytes));
        } else if (startswith(line,"write_bytes")) {
            sscanf(line,"write_bytes: %"SCNu64"\n",&(proc->write_bytes));
        } else if (startswith(line,"cancelled_write_bytes")) {
            sscanf(line,"cancelled_write_bytes: %"SCNu64"\n",&(proc->cancelled_write_bytes));
        } else if (startswith(line, "start:")) {
            /* Only set the start time if it is not already set.
             * This handles cases where fork() is called. */
            if (proc->start == 0) {
                sscanf(line, "start:%lf\n", &(proc->start));
            }
        } else if (startswith(line, "stop:")) {
            sscanf(line,"stop:%lf\n", &(proc->stop));
            if (fork == 0) {
                /* Reset the pointer so that it creates a new object */
                proc = NULL;
            } else {
                /* We skipped one exec, reset fork so we don't skip another */
                fork = 0;
            }
        } else if (startswith(line, "PAPI_TOT_INS:")) {
            sscanf(line,"PAPI_TOT_INS:%lld\n", &llval);
            proc->PAPI_TOT_INS += llval;
        } else if (startswith(line, "PAPI_LD_INS:")) {
            sscanf(line,"PAPI_LD_INS:%lld\n", &llval);
            proc->PAPI_LD_INS += llval;
        } else if (startswith(line, "PAPI_SR_INS:")) {
            sscanf(line,"PAPI_SR_INS:%lld\n", &llval);
            proc->PAPI_SR_INS += llval;
        } else if (startswith(line, "PAPI_FP_INS:")) {
            sscanf(line,"PAPI_FP_INS:%lld\n", &llval);
            proc->PAPI_FP_INS += llval;
        } else if (startswith(line, "PAPI_FP_OPS:")) {
            sscanf(line,"PAPI_FP_OPS:%lld\n", &llval);
            proc->PAPI_FP_OPS += llval;
        } else if (startswith(line, "PAPI_L3_TCM:")) {
            sscanf(line,"PAPI_L3_TCM:%lld\n", &llval);
            proc->PAPI_L3_TCM += llval;
        } else if (startswith(line, "PAPI_L2_TCM:")) {
            sscanf(line,"PAPI_L2_TCM:%lld\n", &llval);
            proc->PAPI_L2_TCM += llval;
        } else if (startswith(line, "PAPI_L1_TCM:")) {
            sscanf(line,"PAPI_L1_TCM:%lld\n", &llval);
            proc->PAPI_L1_TCM += llval;
        } else if (startswith(line, "cmd:")) {
            proc->cmd = strdup(line+4);
            proc->cmd[strlen(proc->cmd)-1] = '\0';
        } else if (startswith(line, "fork")) {
            fork = 1;
        } else {
            printerr("Unrecognized libinterpose record: %s", line);
        }
    }

exit:
    fclose(trace);

    /* Remove the file */
    unlink(fullpath);

    /* Empty file? */
    if (lines == 0) {
        printerr("Empty trace file: %s\n", fullpath);
        return NULL;
    }

    return procs;
}

/* Go through all the files in tempdir and read all of the traces that begin with trace_file_prefix */
static ProcInfo *processTraceFiles(const char *tempdir, const char *trace_file_prefix) {
    DIR *tmp = opendir(tempdir);
    if (tmp == NULL) {
        printerr("Unable to open trace file directory: %s", tempdir);
        return NULL;
    }

    ProcInfo *procs = NULL;
    ProcInfo *lastproc = NULL;

    struct dirent *d;
    for (d = readdir(tmp); d!=NULL; d = readdir(tmp)) {
        /* If the file name starts with the prefix */
        if (strstr(d->d_name, trace_file_prefix) == d->d_name) {
            char fullpath[BUFSIZ];
            snprintf(fullpath, BUFSIZ, "%s/%s", tempdir, d->d_name);
            ProcInfo *p = processTraceFile(fullpath);
            if (p == NULL) {
                continue;
            }
            p->prev = lastproc;
            if (procs == NULL) {
                procs = p;
            } else {
                lastproc->next = p;
            }
            lastproc = p;
            /* If processTraceFile retuns a list of several procs */
            while (lastproc->next != NULL) {
                lastproc = lastproc->next;
            }
        }
    }

    closedir(tmp);

    return procs;
}

/* Try to get a new environment for the child process that has the tracing vars */
static void set_tracing_environment(const char *tempdir, const char *trace_file_prefix) {
    /* If KICKSTART_PREFIX or LD_PRELOAD are already set then we can't trace */
    if (getenv("KICKSTART_PREFIX") != NULL || getenv("LD_PRELOAD") != NULL) {
        return;
    }

    /* Set LD_PRELOAD to the interpose library */
    /* If the interpose library can't be found, then we can't trace */
    char ld_preload[BUFSIZ];
    if (findInterposeLibrary(ld_preload, BUFSIZ) < 0) {
        printerr("Cannot locate libinterpose.so\n");
        return;
    }
    setenv("LD_PRELOAD", ld_preload, 1);

    /* Set KICKSTART_PREFIX to be tempdir/prefix */
    char kickstart_prefix[BUFSIZ];
    snprintf(kickstart_prefix, BUFSIZ, "%s/%s", tempdir, trace_file_prefix);
    setenv("KICKSTART_PREFIX", kickstart_prefix, 1);
}

/* Defined in pegasus-kickstart.c */
extern AppInfo appinfo;

/* Signal handler to pass the signal on to the currently running child, if any */
void propagate_signal(int sig) {
    if (appinfo.currentChild > 0) {
        kill(appinfo.currentChild, sig);
    }
}

int mysystem(AppInfo* appinfo, JobInfo* jobinfo) {
    /* purpose: emulate the system() libc call, but save utilization data.
     * paramtr: appinfo (IO): shared record of information
     *                        isPrinted (IO): reset isPrinted in child process!
     *                        input (IN): connect to stdin or share
     *                        output (IN): connect to stdout or share
     *                        error (IN): connect to stderr or share
     *          jobinfo (IO): updated record of job-specific information
     *                        argv (IN): assembled commandline
     *                        child (OUT): pid of child process
     *                        status (OUT): also returned as function result
     *                        saverr (OUT): will be set to value of errno
     *                        start (OUT): will be set to startup time
     *                        final (OUT): will be set to finish time after reap
     *                        use (OUT): rusage record from application call
     *          input (IN): connect to stdin or share
     *          output (IN): connect to stdout or share
     *          error (IN): connect to stderr or share
     * returns:   -1: failure in mysystem processing, check errno
     *           126: connecting child to its new stdout failed
     *           127: execv() call failed
     *          else: status of child
     */

    /* sanity checks first */
    if (!jobinfo->isValid) {
        errno = ENOEXEC; /* no executable */
        return -1;
    }

    /* Pass signals on to child */
    struct sigaction propagate, saveintr, saveterm, savequit;
    memset(&propagate, 0, sizeof(struct sigaction));
    propagate.sa_handler = propagate_signal;
    sigemptyset(&propagate.sa_mask);
    propagate.sa_flags = 0;
    if (sigaction(SIGINT, &propagate, &saveintr) < 0) {
        return -1;
    }
    if (sigaction(SIGTERM, &propagate, &saveterm) < 0) {
        return -1;
    }
    if (sigaction(SIGQUIT, &propagate, &savequit) < 0) {
        return -1;
    }

    /* Prefix for trace files generated by this job */
    char trace_file_prefix[128];
    snprintf(trace_file_prefix, 128, "ks.trace.%d", getpid());

    /* Temp dir where trace files are stored for this job */
    const char *tempdir = getTempDir();
    if (tempdir == NULL) {
        tempdir = "/tmp";
    }

    /* start wall-clock */
    now(&(jobinfo->start));

    if ((jobinfo->child=fork()) < 0) {
        /* no more process table space */
        jobinfo->status = -1;
    } else if (jobinfo->child == 0) {
        /* child */
        appinfo->isPrinted=1;

        /* If we are using library tracing, try to set the necessary
           environment variables */
        if (appinfo->enableLibTrace) {
            set_tracing_environment(tempdir, trace_file_prefix);
        }

        /* connect jobs stdio */
        if (forcefd(&appinfo->input, STDIN_FILENO)) _exit(126);
        if (forcefd(&appinfo->output, STDOUT_FILENO)) _exit(126);
        if (forcefd(&appinfo->error, STDERR_FILENO)) _exit(126);

        /* restore signal handlers */
        sigaction(SIGINT, &saveintr, NULL);
        sigaction(SIGTERM, &saveterm, NULL);
        sigaction(SIGQUIT, &savequit, NULL);

        /* If we are tracing, then hand over control to the proc module */
        if (appinfo->enableTracing) {
            if (procChild()) _exit(126);
        }

        execv(jobinfo->argv[0], (char* const*) jobinfo->argv);
        perror("execv");
        _exit(127); /* executed in child process */
    } else {
        /* Track the current child process */
        appinfo->currentChild = jobinfo->child;

        /* parent */
        if (appinfo->enableTracing) {
            /* TODO If this returns an error, then we need to untrace all the children and try the wait instead */
            procParentTrace(jobinfo->child, &jobinfo->status, &jobinfo->use, &(jobinfo->children), appinfo->enableSysTrace);
        } else {
            procParentWait(jobinfo->child, &jobinfo->status, &jobinfo->use, &(jobinfo->children));
        }

        /* sanity check */
        if (kill(jobinfo->child, 0) == 0) {
            printerr("ERROR: job %d is still running!\n", jobinfo->child);
            if (!errno) errno = EINPROGRESS;
        }

        /* Child is no longer running */
        appinfo->currentChild = 0;
    }

    /* save any errors before anybody overwrites this */
    jobinfo->saverr = errno;

    /* stop wall-clock */
    now(&(jobinfo->finish));

    /* restore signal handlers */
    sigaction(SIGINT, &saveintr, NULL);
    sigaction(SIGTERM, &saveterm, NULL);
    sigaction(SIGQUIT, &savequit, NULL);

    /* Look for trace files from libinterpose and add trace data to jobinfo */
    if (appinfo->enableLibTrace) {
        jobinfo->children = processTraceFiles(tempdir, trace_file_prefix);
    }

    /* finalize */
    return jobinfo->status;
}

