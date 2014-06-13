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

/* TODO Fix up status and io records */
/* TODO Filter out system paths like /lib /sys /proc /dev and /etc? */
/* TODO Thread safety? */
/* TODO Add r/w/a mode support */
/* TODO Interpose rename? */
/* TODO Interpose truncate? */
/* TODO Interpose mkstemp and tmpfile? */
/* TODO Interpose unlink? */
/* TODO Interpose network functions (connect, accept) */

/* These are all the functions we are interposing */
static typeof(open) *orig_open = NULL;
static typeof(open64) *orig_open64 = NULL;
static typeof(openat) *orig_openat = NULL;
static typeof(openat64) *orig_openat64 = NULL;
static typeof(creat) *orig_creat = NULL;
static typeof(creat64) *orig_creat64 = NULL;
static typeof(fopen) *orig_fopen = NULL;
static typeof(fopen64) *orig_fopen64 = NULL;
static typeof(freopen) *orig_freopen = NULL;
static typeof(freopen64) *orig_freopen64 = NULL;

/* This is the trace file where we write information about the process */
static FILE* trace = NULL;

static FILE *fopen_untraced(const char *path, const char *mode);

/* Open the trace file */
static int topen() {

    char *kickstart_prefix = getenv("KICKSTART_PREFIX");
    if (kickstart_prefix == NULL) {
        fprintf(stderr, "libinterpose: Unable to open trace file: KICKSTART_PREFIX not set in environment");
        return -1;
    }

    char filename[BUFSIZ];
    snprintf(filename, BUFSIZ, "%s.%d", kickstart_prefix, getpid());

    trace = fopen_untraced(filename, "w+");
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

static void read_status() {
    FILE *statfile = fopen_untraced("/proc/self/status", "r");
    if (statfile == NULL) {
        fprintf(stderr, "libinterpose: Unable to read /proc/self/status\n");
        return;
    }

    char buf[BUFSIZ];
    while (fgets(buf, BUFSIZ, statfile) != NULL) {
        tprintf("S %s", buf);
    }

    fclose(statfile);
}

static void read_io() {
    FILE *iofile = fopen_untraced("/proc/self/io", "r");
    if (iofile == NULL) {
        fprintf(stderr, "libinterpose: Unable to read /proc/self/io\n");
        return;
    }

    char buf[BUFSIZ];
    while (fgets(buf, BUFSIZ, iofile) != NULL) {
        tprintf("I %s", buf);
    }

    fclose(iofile);
}

static void read_proc() {
    read_status();
    read_io();
}

/* Library initialization function */
static void __attribute__((constructor)) interpose_init(void) {
    /* Open the trace file */
    topen();
}

/* Library finalizer function */
static void __attribute__((destructor)) interpose_fini(void) {

    read_proc();

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

    tprintf("F %s %lu\n", path, st.st_size);
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

int open64(const char *path, int oflag, ...) {
    if (orig_open64 == NULL) {
        orig_open64 = dlsym(RTLD_NEXT, "open64");
    }

    mode_t mode = 0700;
    if (oflag & O_CREAT) {
        va_list list;
        va_start(list, oflag);
        mode = va_arg(list, int);
        va_end(list);
    }

    int rc = (*orig_open64)(path, oflag, mode);

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

int openat64(int dirfd, const char *path, int oflag, ...) {
    if (orig_openat64 == NULL) {
        orig_openat64 = dlsym(RTLD_NEXT, "openat64");
    }

    mode_t mode = 0700;
    if (oflag & O_CREAT) {
        va_list list;
        va_start(list, oflag);
        mode = va_arg(list, int);
        va_end(list);
    }

    int rc = (*orig_openat64)(dirfd, path, oflag, mode);

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

int creat64(const char *path, mode_t mode) {
    if (orig_creat64 == NULL) {
        orig_creat64 = dlsym(RTLD_NEXT, "creat64");
    }

    int rc = (*orig_creat64)(path, mode);

    if (rc >= 0) {
        trace_open(path);
    }

    return rc;
}

static FILE *fopen_untraced(const char *path, const char *mode) {
    if (orig_fopen == NULL) {
        orig_fopen = dlsym(RTLD_NEXT, "fopen");
    }

    return (*orig_fopen)(path, mode);
}

FILE *fopen(const char *path, const char *mode) {
    FILE *f = fopen_untraced(path, mode);

    if (f != NULL) {
        trace_open(path);
    }

    return f;
}

FILE *fopen64(const char *path, const char *mode) {
    if (orig_fopen64 == NULL) {
        orig_fopen64 = dlsym(RTLD_NEXT, "fopen64");
    }

    FILE *f = (*orig_fopen64)(path, mode);

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

FILE *freopen64(const char *path, const char *mode, FILE *stream) {
    if (orig_freopen64 == NULL) {
        orig_freopen64 = dlsym(RTLD_NEXT, "freopen64");
    }

    FILE *f = orig_freopen64(path, mode, stream);

    if (f != NULL) {
        trace_open(path);
    }

    return f;
}

