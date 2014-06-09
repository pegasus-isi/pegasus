#define _GNU_SOURCE

#include <stdio.h>
#include <dlfcn.h>
#include <unistd.h>
#include <sys/types.h>
#include <fcntl.h>
#include <stdarg.h>
#include <dirent.h>
#include <stdlib.h>

/* These are all the functions we are interposing */
static typeof(open) *orig_open = NULL;
static typeof(openat) *orig_openat = NULL;
static typeof(creat) *orig_creat = NULL;
static typeof(fopen) *orig_fopen = NULL;
static typeof(freopen) *orig_freopen = NULL;
static typeof(close) *orig_close = NULL;
static typeof(fclose) *orig_fclose = NULL;

/* This is the log file where we write information about the process */
static FILE* log = NULL;

/* Open the log file */
static int lopen() {

    char *kickstart_prefix = getenv("KICKSTART_PREFIX");
    if (kickstart_prefix == NULL) {
        fprintf(stderr, "KICKSTART_PREFIX not set in environment");
        return -1;
    }

    char filename[BUFSIZ];
    snprintf(filename, BUFSIZ, "%s.%d", kickstart_prefix, getpid());

    log = (*orig_fopen)(filename, "w+");
    if (log == NULL) {
        fprintf(stderr, "Unable to open log file");
        return -1;
    }

    return 0;
}

/* Print something to the log file if it is open */
static int lprintf(const char *format, ...) {
    if (log == NULL) {
        return 0;
    }
    va_list args;
    va_start(args, format);
    int rc = vfprintf(log, format, args);
    va_end(args);
    return rc;
}

/* Close log file */
static int lclose() {
    if (log == NULL) {
        return 0;
    }

    return (*orig_fclose)(log);
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

    /* Open the log file */
    lopen();
}

/* Library finalizer function */
static __attribute__((destructor)) void fini(void) {
    fprintf(stderr, "Unloading Kickstart interposition library...\n");

    /* Close the log file */
    lclose();
}


/** INTERPOSED FUNCTIONS **/


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
        lprintf("open %s", path);
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

    // TODO Log

    return rc;
}

int creat(const char *path, mode_t mode) {
    int rc = (*orig_creat)(path, mode);

    // TODO Log

    return rc;
}

FILE *fopen(const char *path, const char *mode) {
    FILE *f = orig_fopen(path, mode);

    if (f != NULL) {
        lprintf("open %s\n", path);
    }

    return f;
}

FILE *freopen(const char *path, const char *mode, FILE *stream) {
    FILE *f = orig_freopen(path, mode, stream);

    if (f != NULL) {
        printf("open %s\n", path);
    }

    return f;
}

int close(int filedes) {
    int rc = (*orig_close)(filedes);

    // TODO Log size?

    if (rc == 0) {
        lprintf("closed %d\n", filedes);
    }

    return rc;
}

int fclose(FILE *fp) {
    int rc = (*orig_fclose)(fp);

    // TODO Log size?

    return rc;
}

