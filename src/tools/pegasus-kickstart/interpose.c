#define _GNU_SOURCE

#include <stdio.h>
#include <dlfcn.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdarg.h>
#include <dirent.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>

/* TODO Thread safety? */
/* TODO Add r/w/a mode support */
/* TODO Trace rename? */
/* TODO Interpose *64 functions for 32 bit machines (e.g. open64) */
/* TODO Interpose truncate */
/* TODO Interpose mkstemp and tmpfile */
/* TODO Interpose unlink */
/* TODO Use atexit or on_exit to do some final stats? Or interpose exit? */
/* TODO Interpose network functions (connect, accept) */

/* These are all the functions we are interposing */
static typeof(open) *orig_open = NULL;
static typeof(openat) *orig_openat = NULL;
static typeof(creat) *orig_creat = NULL;
static typeof(fopen) *orig_fopen = NULL;
static typeof(freopen) *orig_freopen = NULL;

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

    /* We have to do this here because we don't want to trace our own fopen() */
    if (orig_fopen == NULL) {
        orig_fopen = dlsym(RTLD_NEXT, "fopen");
    }

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

    return fclose(trace);
}

/* Library initialization function */
void __attribute__((constructor)) interpose_init(void) {
    /* Locate the real functions we are interposing */

    /* Open the trace file */
    topen();
}

/* Library finalizer function */
void __attribute__((destructor)) interpose_fini(void) {
    /* Close trace file */
    tclose();
}


/** INTERPOSED FUNCTIONS **/

static void trace_file(const char *path) {
    struct stat st;
    if (stat(path, &st) < 0) {
        fprintf(stderr, "libinterpose: Unable to stat file: %s\n", strerror(errno));
        return;
    }

    /* We only want to report regular files */
    if (!S_ISREG(st.st_mode)) {
        return;
    }

    tprintf("%s %lu\n", path, st.st_size);
}

static void trace_open(const char *path) {
    char fullpath[BUFSIZ];
    if (realpath(path, fullpath) == NULL) {
        fprintf(stderr, "libinterpose: Unable to get real path for '%s'", path);
        return;
    }

    trace_file(fullpath);
}

static void trace_openat(int fd) {
    char linkpath[64];
    snprintf(linkpath, 64, "/proc/%d/fd/%d", getpid(), fd);

    char fullpath[BUFSIZ];
    int len = readlink(linkpath, fullpath, BUFSIZ);
    if (len <= 0) {
        fprintf(stderr, "libinterpose: Unable to get real path for fd %d", fd);
        return;
    }
    if (len == BUFSIZ) {
        fprintf(stderr, "libinterpose: Path too long for fd %d", fd);
        return;
    }
    /* readlink doesn't add a null byte */
    fullpath[len] = '\0';

    trace_file(fullpath);
}

int open(const char *path, int oflag, ...) {
    if (orig_open == NULL) {
        orig_open = dlsym(RTLD_NEXT, "open");
    }

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
    if (orig_openat == NULL) {
        orig_openat = dlsym(RTLD_NEXT, "openat");
    }

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
    if (orig_creat == NULL) {
        orig_creat = dlsym(RTLD_NEXT, "creat");
    }

    int rc = (*orig_creat)(path, mode);

    if (rc >= 0) {
        trace_open(path);
    }

    return rc;
}

FILE *fopen(const char *path, const char *mode) {
    if (orig_fopen == NULL) {
        orig_fopen = dlsym(RTLD_NEXT, "fopen");
    }

    FILE *f = (*orig_fopen)(path, mode);

    if (f != NULL) {
        trace_open(path);
    }

    return f;
}

FILE *freopen(const char *path, const char *mode, FILE *stream) {
    if (orig_freopen == NULL) {
        orig_freopen = dlsym(RTLD_NEXT, "freopen");
    }

    FILE *f = orig_freopen(path, mode, stream);

    if (f != NULL) {
        trace_open(path);
    }

    return f;
}

