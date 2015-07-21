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

#ifdef __APPLE__
#include <sys/event.h> /* For kqueue */
#endif

#ifdef __linux__
#include <sys/signalfd.h>
#include <poll.h>
#endif

#include "utils.h"
#include "appinfo.h"
#include "statinfo.h"
#include "mysystem.h"
#include "procinfo.h"
#include "error.h"

/* How long should the polling interval be, in seconds */
#define POLL_TIMEOUT 60

struct event_loop_ctx {
    AppInfo *appinfo;
    JobInfo *jobinfo;
#ifdef __APPLE__
    int kqfd;
#endif
#ifdef __linux__
    int sigfd;
#endif
};

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

    ProcInfo *proc = (ProcInfo *)calloc(sizeof(ProcInfo), 1);
    if (proc == NULL) {
        printerr("calloc: %s\n", strerror(errno));
        return NULL;
    }

    /* Read data from the trace file */
    int lines = 0;
    char line[BUFSIZ];
    while (fgets(line, BUFSIZ, trace) != NULL) {
        lines++;
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
            printerr("Unrecognized libinterpose record: %s", line);
        }
    }

    fclose(trace);

    /* Remove the file */
    unlink(fullpath);

    /* Empty file? */
    if (lines == 0) {
        printerr("Empty trace file: %s\n", fullpath);
        return NULL;
    }

    return proc;
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
        printerr("Cannot locate libinterpose.so\n");
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
    if (newenvp == NULL) {
        printerr("malloc: %s\n", strerror(errno));
        return envp;
    }
    for (vars=0; envp[vars] != NULL; vars++) {
        newenvp[vars] = envp[vars];
    }

    /* Set the new variables */
    newenvp[vars] = ld_preload;
    newenvp[vars+1] = kickstart_prefix;
    newenvp[vars+2] = NULL;

    return newenvp;
}

static int setup_event_loop(struct event_loop_ctx *ctx, AppInfo *appinfo, JobInfo *jobinfo) {
#ifdef __APPLE__
    /* DON'T block SIGCHLD or kqueue won't work */

    /* Setup the queue descriptor */
    int kq = kqueue();
    if (kq == -1) {
        printerr("Error creating kqueue descriptor: %s\n", strerror(errno));
        return -1;
    }

    /* set the kevent, can only receive signals for this process */
    struct kevent ke;
    EV_SET(&ke, SIGCHLD, EVFILT_SIGNAL, EV_ADD, 0, 0, NULL);
    if (kevent(kq, &ke, 1, NULL, 0, NULL) < 0) {
        printerr("Error setting kevent filter: %s\n", strerror(errno));
        return -1;
    }

    ctx->kqfd = kq;
#endif

#ifdef __linux__
    /* Block SIGCHLD so that we can handle it with the signalfd instead */
    sigset_t mask;
    sigemptyset(&mask);
    sigaddset(&mask, SIGCHLD);
    if (sigprocmask(SIG_BLOCK, &mask, NULL) == -1) {
        printerr("Error blocking SIGCHLD: %s", strerror(errno));
        return -1;
    }

    /* Create a signal file descriptor */
    int sigfd = signalfd(-1, &mask, SFD_CLOEXEC|SFD_NONBLOCK);
    if (sigfd == -1) {
        printerr("Error creating signalfd: %s", strerror(errno));
        return -1;
    }

    ctx->sigfd = sigfd;
#endif

    ctx->appinfo = appinfo;
    ctx->jobinfo = jobinfo;

    return 0;
}

static int teardown_event_loop(struct event_loop_ctx *ctx) {
#ifdef __APPLE__
    return close(ctx->kqfd);
#else
#ifdef __linux__
    return close(ctx->sigfd);
#else
    return 0;
#endif
#endif
}

static int handle_timeout(struct event_loop_ctx *ctx) {
    /* FIXME Do something when the timeout occurs? */
    return 0;
}

static int handle_sigchld(struct event_loop_ctx *ctx) {
    JobInfo *jobinfo = ctx->jobinfo;
    return procParentWait(jobinfo->child, &jobinfo->status, &jobinfo->use, &(jobinfo->children));
}

static int event_loop(struct event_loop_ctx *ctx) {
#ifdef __APPLE__
    while (1) {

        struct kevent ke;
        memset(&ke, 0x00, sizeof(ke));

        struct timespec timeout;
        timeout.tv_sec = POLL_TIMEOUT;
        timeout.tv_nsec = 0;

        int rc = kevent(ctx->kqfd, NULL, 0, &ke, 1, &timeout);
        if (rc < 0) {
            if (errno == EINTR) {
                continue;
            } else {
                printerr("kevent failed: %s\n", strerror(errno));
            }
        } else if (rc == 0) {
            handle_timeout(ctx);
        } else if (ke.filter == EVFILT_SIGNAL) {
            if (ke.ident == SIGCHLD) {
                return handle_sigchld(ctx);
            } else {
                printerr("Error: Unexpected signal from kevent: %lu\n", ke.ident);
                return -1;
            }
        } else {
            printerr("Error: Unexpected filter from kevent: %hd\n", ke.filter);
            return -1;
        }
    }
#else
#ifdef LINUX
    /* TODO Fix TRACING CASE */
    if (ctx->appinfo->enableTracing) {
        /* TODO If this returns an error, then we need to untrace all the children and try the wait instead */
        JobInfo *jobinfo = ctx->jobinfo;
        procParentTrace(jobinfo->child, &jobinfo->status, &jobinfo->use, &(jobinfo->children), ctx->appinfo->enableSysTrace);
        return jobinfo->status;
    }

    struct pollfd ufds[1];
    ufds[0].fd = ctx->sigfd;
    ufds[0].events = POLLIN;

    while (1) {
        int rv = poll(ufds, 1, POLL_TIMEOUT*1000);
        if (rv == -1) {
            if (errno == EINTR) {
                continue;
            }
            printerr("Error polling for updates: %s\n", strerror(errno));
            return -1;
        } else if (rv == 0) {
            handle_timeout(ctx);
        } else {
            if (ufds[0].revents & POLLIN) {
                struct signalfd_siginfo fdsi;
                ssize_t s = read(ufds[0].fd, &fdsi, sizeof(struct signalfd_siginfo));
                if (s != sizeof(struct signalfd_siginfo)) {
                    printerr("Error reading signal from signalfd: %s\n", strerror(errno));
                    return -1;
                }

                if (fdsi.ssi_signo == SIGCHLD) {
                    return handle_sigchld(ctx);
                } else {
                    printerr("Read unexpected signal from signalfd: %d\n", fdsi.ssi_signo);
                    return -1;
                }
            } else {
                printerr("Read unexpected event from poll(): %d\n", ufds[0].revents);
                return -1;
            }
        }
    }
#else /* !__linux__ and !__APPLE__ */

    /* In all other cases, just wait on the child */
    return procParentWait(jobinfo->child, &jobinfo->status, &jobinfo->use, &(jobinfo->children));
#endif /* ifdef __linux__ */
#endif /* ifdef __APPLE__ */
}

int mysystem(AppInfo* appinfo, JobInfo* jobinfo, char* envp[]) {
    /* purpose: Run the task described by jobinfo
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

    struct event_loop_ctx evctx;
    if (setup_event_loop(&evctx, appinfo, jobinfo) < 0) {
        printerr("Error setting up event loop\n");
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
        /* Track the current child process */
        appinfo->currentChild = jobinfo->child;

        if (event_loop(&evctx) < 0) {
            printerr("Event loop error\n");
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

    teardown_event_loop(&evctx);

    /* restore default handlers */
    sigaction(SIGINT, &saveintr, NULL);
    sigaction(SIGQUIT, &savequit, NULL);

    /* Look for trace files from libinterpose and add trace data to jobinfo */
    if (appinfo->enableLibTrace) {
        jobinfo->children = processTraceFiles(tempdir, trace_file_prefix);
    }

    /* finalize */
    return jobinfo->status;
}

