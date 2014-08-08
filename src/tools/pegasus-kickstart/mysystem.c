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

/* The name of the program (argv[0]) set in pegasus-kickstart.c:main */
char *programname;

static int isRelativePath(char *path) {
    // Absolute path
    if (path[0] == '/') {
        return 0;
    }

    // Relative to current directory
    if (path[0] == '.') {
        return 1;
    }

    // Relative to current directory
    for (char *c = path; *c != '\0'; c++) {
        if (*c == '/') {
            return 1;
        }
    }

    return 0;
}

/* Find the path to the interposition library */
static int findInterposeLibrary(char *path, int pathsize) {
    char kickstart[BUFSIZ];
    char lib[BUFSIZ];

    // If the path is not relative, then look it up in the PATH
    if (!isRelativePath(programname)) {
        programname = findApp(programname);
        if (programname == NULL) {
            // Not found in PATH
            return -1;
        }
    }

    // Find the real path of kickstart
    if (realpath(programname, kickstart) < 0) {
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
    if (sscanf(buf, "file: '%[^']' %lu %lu %lu\n", filename, &size, &bread, &bwrite) != 4) {
        fprintf(stderr, "Invalid file record: %s", buf);
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
        file->filename = strdup(filename);
        file->size = size;
        file->bread = bread;
        file->bwrite = bwrite;

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
    }

    return files;
}

static SockInfo *readTraceSocketRecord(const char *buf, SockInfo *sockets) {
    char address[BUFSIZ];
    int port = 0;
    size_t brecv = 0;
    size_t bsend = 0;
    if (sscanf(buf, "socket: %s %d %lu %lu\n", address, &port, &brecv, &bsend) != 4) {
        fprintf(stderr, "Invalid socket record: %s", buf);
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
        sock->address = strdup(address);
        sock->port = port;
        sock->brecv = brecv;
        sock->bsend = bsend;

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
        fprintf(stderr, "Unable to open trace file '%s': %s\n",
                fullpath, strerror(errno));
        return NULL;
    }

    ProcInfo *proc = (ProcInfo *)calloc(sizeof(ProcInfo), 1);

    /* Read data from the trace file */
    char line[BUFSIZ];
    while (fgets(line, BUFSIZ, trace) != NULL) {
        if (startswith(line, "file:")) {
            proc->files = readTraceFileRecord(line, proc->files);
        } else if (startswith(line, "socket:")) {
            proc->sockets = readTraceSocketRecord(line, proc->sockets);
        } else if (startswith(line, "exe:")) {
            char exe[BUFSIZ];
            sscanf(line, "exe: %s\n", exe);
            proc->exe = strdup(exe);
        } else if (startswith(line, "Pid")) {
            sscanf(line, "Pid:%d\n", &(proc->pid));
        } else if (startswith(line, "PPid")) {
            sscanf(line,"PPid:%d\n", &(proc->ppid));
        } else if (startswith(line, "Tgid")) {
            sscanf(line,"Tgid:%d\n", &(proc->tgid));
        } else if (startswith(line,"VmPeak")) {
            sscanf(line,"VmPeak:%d kB\n", &(proc->vmpeak));
        } else if (startswith(line,"VmHWM")) {
            sscanf(line,"VmHWM:%d kB\n", &(proc->rsspeak));
        } else if (startswith(line, "Threads")) {
            sscanf(line,"Threads:%d\n", &(proc->threads));
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
            sscanf(line,"start:%lf\n", &(proc->start));
        } else if (startswith(line, "stop:")) {
            sscanf(line,"stop:%lf\n", &(proc->stop));
        } else {
            fprintf(stderr, "Unrecognized libinterpose record: %s", line);
        }
    }

    fclose(trace);

    /* Remove the file */
    unlink(fullpath);

    return proc;
}

/* Go through all the files in tempdir and read all of the traces that begin with trace_file_prefix */
static ProcInfo *processTraceFiles(const char *tempdir, const char *trace_file_prefix) {
    DIR *tmp = opendir(tempdir);
    if (tmp == NULL) {
        fprintf(stderr, "Unable to open trace file directory: %s", tempdir);
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
        }
    }

    closedir(tmp);

    return procs;
}

/* Try to get a new environment for the child process that has the tracing vars */
static char **tryGetNewEnvironment(char **envp, const char *tempdir, const char *trace_file_prefix) {
    int vars;

    /* If KICKSTART_PREFIX or LD_PRELOAD are already set then we can't trace */
    for (vars=0; envp[vars] != NULL; vars++) {
        if (startswith(envp[vars], "KICKSTART_PREFIX=")) {
            return envp;
        }
        if (startswith(envp[vars], "LD_PRELOAD=")) {
            return envp;
        }
    }

    /* If the interpose library can't be found, then we can't trace */
    char libpath[BUFSIZ];
    if (findInterposeLibrary(libpath, BUFSIZ) < 0) {
        fprintf(stderr, "kickstart: Cannot locate libinterpose.so\n");
        return envp;
    }

    /* Set LD_PRELOAD to the intpose library */
    char ld_preload[BUFSIZ];
    snprintf(ld_preload, BUFSIZ, "LD_PRELOAD=%s", libpath);

    /* Set KICKSTART_PREFIX to be tempdir/prefix */
    char kickstart_prefix[BUFSIZ];
    snprintf(kickstart_prefix, BUFSIZ, "KICKSTART_PREFIX=%s/%s",
             tempdir, trace_file_prefix);

    /* Copy the environment variables to a new array */
    char **newenvp = (char **)malloc(sizeof(char **)*(vars+3));
    for (vars=0; envp[vars] != NULL; vars++) {
        newenvp[vars] = envp[vars];
    }

    /* Set the new variables */
    newenvp[vars] = ld_preload;
    newenvp[vars+1] = kickstart_prefix;
    newenvp[vars+2] = NULL;

    return newenvp;
}

int mysystem(AppInfo* appinfo, JobInfo* jobinfo, char* envp[]) {
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
     *          envp (IN): vector with the parent's environment
     * returns:   -1: failure in mysystem processing, check errno
     *           126: connecting child to its new stdout failed
     *           127: execve() call failed
     *          else: status of child
     */

    /* sanity checks first */
    if (!jobinfo->isValid) {
        errno = ENOEXEC; /* no executable */
        return -1;
    }

    /* Ignore SIGINT and SIGQUIT */
    struct sigaction ignore, saveintr, savequit;
    memset(&ignore, 0, sizeof(ignore));
    ignore.sa_handler = SIG_IGN;
    sigemptyset(&ignore.sa_mask);
    ignore.sa_flags = 0;
    if (sigaction(SIGINT, &ignore, &saveintr) < 0) {
        return -1;
    }
    if (sigaction(SIGQUIT, &ignore, &savequit) < 0) {
        return -1;
    }

    /* Prefix for trace files generated by this job */
    char trace_file_prefix[128];
    snprintf(trace_file_prefix, 128, "gs.trace.%d", getpid());

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

        // If we are using library tracing, try to set the necessary
        // environment variables
        if (appinfo->enableLibTrace) {
            envp = tryGetNewEnvironment(envp, tempdir, trace_file_prefix);
        }

        /* connect jobs stdio */
        if (forcefd(&appinfo->input, STDIN_FILENO)) _exit(126);
        if (forcefd(&appinfo->output, STDOUT_FILENO)) _exit(126);
        if (forcefd(&appinfo->error, STDERR_FILENO)) _exit(126);

        /* undo signal handlers */
        sigaction(SIGINT, &saveintr, NULL);
        sigaction(SIGQUIT, &savequit, NULL);

        /* If we are tracing, then hand over control to the proc module */
        if (appinfo->enableTracing) {
            if (procChild()) _exit(126);
        }

        execve(jobinfo->argv[0], (char* const*) jobinfo->argv, envp);
        perror("execve");
        _exit(127); /* executed in child process */
    } else {
        /* parent */
        if (appinfo->enableTracing) {
            /* TODO If this returns an error, then we need to untrace all the children and try the wait instead */
            procParentTrace(jobinfo->child, &jobinfo->status, &jobinfo->use, &(jobinfo->children), appinfo->enableSysTrace);
        } else {
            procParentWait(jobinfo->child, &jobinfo->status, &jobinfo->use, &(jobinfo->children));
        }

        /* sanity check */
        if (kill(jobinfo->child, 0) == 0) {
            fprintf(stderr, "ERROR: job %d is still running!\n", jobinfo->child);
            if (!errno) errno = EINPROGRESS;
        }
    }

    /* save any errors before anybody overwrites this */
    jobinfo->saverr = errno;

    /* stop wall-clock */
    now(&(jobinfo->finish));

    /* ignore errors on these, too. */
    sigaction(SIGINT, &saveintr, NULL);
    sigaction(SIGQUIT, &savequit, NULL);

    /* Look for trace files from libinterpose and add trace data to jobinfo */
    if (appinfo->enableLibTrace) {
        jobinfo->children = processTraceFiles(tempdir, trace_file_prefix);
    }

    /* finalize */
    return jobinfo->status;
}

