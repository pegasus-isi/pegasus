#define _GNU_SOURCE

#include <stdio.h>
#include <dlfcn.h>
#include <unistd.h>
#include <sys/types.h>
#include <fcntl.h>
#include <stdarg.h>
#include <dirent.h>
#include <stdlib.h>

/* TODO Interpose *64 functions for 32 bit machines (e.g. open64) */
/* TODO Interpose truncate */
/* TODO Interpose mkstemp and tmpfile */
/* TODO Interpose unlink */
/* TODO Use atexit or on_exit to do some final stats? */
/* TODO Interpose network functions (connect, accept) */

/* These are all the functions we are interposing */
static typeof(open) *orig_open = NULL;
static typeof(openat) *orig_openat = NULL;
static typeof(creat) *orig_creat = NULL;
static typeof(fopen) *orig_fopen = NULL;
static typeof(freopen) *orig_freopen = NULL;
static typeof(close) *orig_close = NULL;
static typeof(fclose) *orig_fclose = NULL;

/* This is the trace file where we write information about the process */
static FILE* trace = NULL;

/* Open the trace file */
static int topen() {

    char *kickstart_prefix = getenv("KICKSTART_PREFIX");
    if (kickstart_prefix == NULL) {
        fprintf(stderr, "libinterpose: Unable to open trace file: KICKSTART_PREFIX not set in environment");
        return -1;
    }

    char filename[BUFSIZ];
    snprintf(filename, BUFSIZ, "%s.%d", kickstart_prefix, getpid());

    trace = (*orig_fopen)(filename, "w+");
    if (trace == NULL) {
        fprintf(stderr, "libinterpose: Unable to open trace file");
        return -1;
    }

    return 0;
}

/* Print something to the trace file if it is open */
static int tprintf(const char *format, ...) {
    if (trace == NULL) {
        return 0;
    }
    va_list args;
    va_start(args, format);
    int rc = vfprintf(trace, format, args);
    va_end(args);
    return rc;
}

/* Close trace file */
static int tclose() {
    if (trace == NULL) {
        return 0;
    }

    return (*orig_fclose)(trace);
}

/* Library initialization function */
static __attribute__((constructor)) void init(void) {
    /* Locate the real functions we are interposing */
    orig_open = dlsym(RTLD_NEXT, "open");
    orig_openat = dlsym(RTLD_NEXT, "openat");
    orig_creat = dlsym(RTLD_NEXT, "creat");
    orig_fopen = dlsym(RTLD_NEXT, "fopen");
    orig_freopen = dlsym(RTLD_NEXT, "freopen");
    orig_close = dlsym(RTLD_NEXT, "close");
    orig_fclose = dlsym(RTLD_NEXT, "fclose");

    /* Open the trace file */
    topen();
}

/* Library finalizer function */
static __attribute__((destructor)) void fini(void) {
    /* Close trace file */
    tclose();
}


/** INTERPOSED FUNCTIONS **/

static void trace_open(const char *path) {
    char fullpath[BUFSIZ];
    if (realpath(path, fullpath) == NULL) {
        fprintf(stderr, "libinterpose: Unable to get real path for '%s'", path);
        return;
    }
    tprintf("%s\n", fullpath);
}

static void trace_openat(int fd) {
    char linkpath[64];
    char fullpath[BUFSIZ];
    snprintf(linkpath, 64, "/proc/%d/fd/%d", getpid(), fd);
    if (readlink(linkpath, fullpath, BUFSIZ) <= 0) {
        fprintf(stderr, "libinterpose: Unable to get real path for fd %d", fd);
        return;
    }
    tprintf("%s\n", fullpath);
}

int open(const char *path, int oflag, ...) {
    mode_t mode = 0700;
    if (oflag & O_CREAT) {
        va_list list;
        va_start(list, oflag);
        mode = va_arg(list, int);
        va_end(list);
    }

    int rc = (*orig_open)(path, oflag, mode);

    if (rc >= 0) {
        trace_open(path);
    }

    return rc;
}

int openat(int dirfd, const char *path, int oflag, ...) {
    mode_t mode = 0700;
    if (oflag & O_CREAT) {
        va_list list;
        va_start(list, oflag);
        mode = va_arg(list, int);
        va_end(list);
    }

    int rc = (*orig_openat)(dirfd, path, oflag, mode);

    if (rc >= 0) {
        trace_openat(rc);
    }

    return rc;
}

int creat(const char *path, mode_t mode) {
    int rc = (*orig_creat)(path, mode);

    if (rc >= 0) {
        trace_open(path);
    }

    return rc;
}

FILE *fopen(const char *path, const char *mode) {
    FILE *f = orig_fopen(path, mode);

    if (f != NULL) {
        trace_open(path);
    }

    return f;
}

FILE *freopen(const char *path, const char *mode, FILE *stream) {
    FILE *f = orig_freopen(path, mode, stream);

    if (f != NULL) {
        trace_open(path);
    }

    return f;
}

int close(int filedes) {
    return (*orig_close)(filedes);
}

int fclose(FILE *fp) {
    return (*orig_fclose)(fp);
}

