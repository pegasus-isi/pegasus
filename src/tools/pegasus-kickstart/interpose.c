#define _GNU_SOURCE

#include <stdio.h>
#include <dlfcn.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/uio.h>
#include <sys/socket.h>
#include <sys/sendfile.h>
#include <fcntl.h>
#include <stdarg.h>
#include <dirent.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/syscall.h>
#include <pthread.h>
#ifdef HAS_PAPI
#include <papi.h>
#endif

/* TODO Handle directories */
/* TODO Interpose accept (for network servers) */
/* TODO Is it necessary to interpose shutdown? Would that help the DNS issue? */
/* TODO Interpose unlink, unlinkat, remove */
/* TODO Interpose rename */
/* TODO Figure out a way to avoid the hack for file names with spaces */
/* TODO Are the _untraced functions necessary? */
/* TODO Create extensive test cases */
/* TODO Interpose fcntl(..., F_DUPFD, ...) */
/* TODO Interpose mknod for S_IFREG */
/* TODO Interpose wide character I/O functions */
/* TODO Handle I/O for stdout/stderr? */
/* TODO asynchronous I/O from librt? */
/* TODO Add r/w/a mode support? */
/* TODO What happens if one interposed library function calls another (e.g.
 *      fopen calls fopen64)? I think internal calls are not traced.
 */
/* TODO What about mmap? Probably nothing we can do besides interpose
 *      mmap and assume that the total size being mapped is read/written
 */
/* TODO Thread safety? */
/* TODO Figure out a way to collect PAPI profiles for unterminated threads */

#define printerr(fmt, ...) \
    fprintf_untraced(stderr, "libinterpose[%d]: %s[%d]: " fmt, \
                     getpid(), __FILE__, __LINE__, ##__VA_ARGS__)

#ifdef DEBUG
#define debug(format, args...) \
    fprintf_untraced(stderr, "libinterpose: " format "\n" , ##args)
#else
#define debug(format, args...)
#endif

/* These are all the functions we are interposing */
static typeof(dup) *orig_dup = NULL;
static typeof(dup2) *orig_dup2 = NULL;
#ifdef dup3
static typeof(dup3) *orig_dup3 = NULL;
#endif
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
static typeof(close) *orig_close = NULL;
static typeof(fclose) *orig_fclose = NULL;
static typeof(read) *orig_read = NULL;
static typeof(write) *orig_write = NULL;
static typeof(fread) *orig_fread = NULL;
static typeof(fwrite) *orig_fwrite = NULL;
static typeof(pread) *orig_pread = NULL;
static typeof(pread64) *orig_pread64 = NULL;
static typeof(pwrite) *orig_pwrite = NULL;
static typeof(pwrite64) *orig_pwrite64 = NULL;
static typeof(readv) *orig_readv = NULL;
#ifdef preadv
static typeof(preadv) *orig_preadv = NULL;
#endif
#ifdef preadv64
static typeof(preadv64) *orig_preadv64 = NULL;
#endif
static typeof(writev) *orig_writev = NULL;
#ifdef pwritev
static typeof(pwritev) *orig_pwritev = NULL;
#endif
#ifdef pwritev64
static typeof(pwritev64) *orig_pwritev64 = NULL;
#endif
static typeof(fgetc) *orig_fgetc = NULL;
static typeof(fputc) *orig_fputc = NULL;
static typeof(fgets) *orig_fgets = NULL;
static typeof(fputs) *orig_fputs = NULL;
/* Implemented using vfscanf */
/*static typeof(fscanf) *orig_fscanf = NULL;*/
static typeof(vfscanf) *orig_vfscanf = NULL;
/* Implemented using vfprintf */
/*static typeof(fprintf) *orig_fprintf = NULL;*/
static typeof(vfprintf) *orig_vfprintf = NULL;
static typeof(connect) *orig_connect = NULL;
static typeof(send) *orig_send = NULL;
static typeof(sendfile) *orig_sendfile = NULL;
static typeof(sendto) *orig_sendto = NULL;
static typeof(sendmsg) *orig_sendmsg = NULL;
static typeof(recv) *orig_recv = NULL;
static typeof(recvfrom) *orig_recvfrom = NULL;
static typeof(recvmsg) *orig_recvmsg = NULL;
static typeof(truncate) *orig_truncate = NULL;
/* It is not necessary to interpose ftruncate because we should already
 * have a record for the file descriptor.
 */
static typeof(mkstemp) *orig_mkstemp = NULL;
#ifdef mkostemp
static typeof(mkostemp) *orig_mkostemp = NULL;
#endif
#ifdef mkstemps
static typeof(mkstemps) *orig_mkstemps = NULL;
#endif
#ifdef mkostemps
static typeof(mkostemps) *orig_mkostemps = NULL;
#endif
static typeof(tmpfile) *orig_tmpfile = NULL;
/* It is not necessary to interpose other tmp functions because
 * they just generate names that need to be passed to open()
 */
static typeof(lseek) *orig_lseek = NULL;
#ifdef lseek64
static typeof(lseek64) *orig_lseek64 = NULL;
#endif
static typeof(fseek) *orig_fseek = NULL;
static typeof(fseeko) *orig_fseeko = NULL;
static typeof(pthread_create) *orig_pthread_create = NULL;

typedef struct {
    char type;
    char *path;
    size_t bread;
    size_t bwrite;
    size_t nread;
    size_t nwrite;
    size_t bseek;
    size_t nseek;
} Descriptor;

const char DTYPE_NONE = 0;
const char DTYPE_FILE = 1;
const char DTYPE_SOCK = 2;

/* File descriptor table */
static Descriptor *descriptors = NULL;
static int max_descriptors = 0;

/* This is the trace file where we write information about the process */
static FILE* trace = NULL;

#ifdef HAS_PAPI
int papi_ok = 0;

char *papi_events[] = {
    "PAPI_TOT_INS",
    "PAPI_LD_INS",
    "PAPI_SR_INS",
    "PAPI_FP_INS",
    "PAPI_FP_OPS"
};

#define n_papi_events (sizeof(papi_events) / sizeof(char *))

typedef struct {
    int eventset;
} interpose_papi_ctx;

#endif

typedef struct {
    void *(*start_routine)(void *);
    void *arg;
    pthread_key_t cleanup;
} interpose_pthread_wrapper_arg;

static FILE *fopen_untraced(const char *path, const char *mode);
static int fprintf_untraced(FILE *stream, const char *format, ...);
static int vfprintf_untraced(FILE *stream, const char *format, va_list ap);
static char *fgets_untraced(char *s, int size, FILE *stream);
static int fclose_untraced(FILE *fp);

static long unsigned int interpose_gettid(void) {
    return (long unsigned int)syscall(SYS_gettid);
}

/* Open the trace file */
static int topen() {
    debug("Open trace file");

    char *kickstart_prefix = getenv("KICKSTART_PREFIX");
    if (kickstart_prefix == NULL) {
        printerr("Unable to open trace file: KICKSTART_PREFIX not set in environment");
        return -1;
    }

    char filename[BUFSIZ];
    snprintf(filename, BUFSIZ, "%s.%d", kickstart_prefix, getpid());

    trace = fopen_untraced(filename, "w+");
    if (trace == NULL) {
        printerr("Unable to open trace file");
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
    int rc = vfprintf_untraced(trace, format, args);
    va_end(args);
    return rc;
}

/* Close trace file */
static int tclose() {
    if (trace == NULL) {
        return 0;
    }

    debug("Close trace file");

    return fclose_untraced(trace);
}

/* Get the current time in seconds since the epoch */
static double get_time() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec + ((double)tv.tv_usec / 1e6);
}

/* Get network address as a string */
static char *get_addr(const struct sockaddr *addr, socklen_t addrlen) {
    static char addrstr[128];
    if (addr->sa_family == AF_INET) {
        /* IPv4 */
        struct sockaddr_in *in4addr = (struct sockaddr_in *)addr;
        char ipstr[INET_ADDRSTRLEN];
        if (inet_ntop(AF_INET, &(in4addr->sin_addr.s_addr), ipstr, INET_ADDRSTRLEN) != NULL) {
            sprintf(addrstr, "%s %d", ipstr, ntohs(in4addr->sin_port));
            return addrstr;
        }
    } else if (addr->sa_family == AF_INET6) {
        /* TODO IPv6 */
    }

    return NULL;
}

/* Get a reference to the given descriptor if it exists */
static Descriptor *get_descriptor(int fd) {
    /* Sometimes we try to access a descriptor before the 
     * constructor has been called where the descriptor array
     * is allocated. That can happen, for example, if another library
     * constructor calls an interposed function in its constructor and
     * it is loaded before this library. This check will make sure
     * that any descriptor we try to access is valid.
     */
    if (descriptors == NULL || fd > max_descriptors) {
        return NULL;
    }
    return &(descriptors[fd]);
}

/* Read /proc/self/exe to get path to executable */
static void read_exe() {
    debug("Reading exe");
    char name[BUFSIZ];
    snprintf(name, BUFSIZ, "/proc/%d/exe", getpid());
    char exe[BUFSIZ];
    int size = readlink(name, exe, BUFSIZ);
    if (size < 0) {
        printerr("libinterpose: Unable to readlink %s: %s\n", name, strerror(errno));
        return;
    }
    exe[size] = '\0';
    tprintf("exe: %s\n", exe);
}

/* Return 1 if line begins with tok */
static int startswith(const char *line, const char *tok) {
    return strstr(line, tok) == line;
}

/* Return 1 if line ends with tok */
static int endswith(const char *line, const char *tok) {
    int n = strlen(line);
    int m = strlen(tok);
    if (n < m) {
        return 0;
    }

    for(int i=0; i<m; i++) {
        if (line[n-i-1] != tok[m-i-1]) {
            return 0;
        }
    }

    return 1;
}

/* Read useful information from /proc/self/status */
static void read_status() {
    debug("Reading status file");

    char statf[] = "/proc/self/status";

    /* If the status file is missing, then just skip it */
    if (access(statf, F_OK) < 0) {
        return;
    }

    FILE *f = fopen_untraced(statf, "r");
    if (f == NULL) {
        perror("libinterpose: Unable to fopen /proc/self/status");
        return;
    }

    char line[BUFSIZ];
    while (fgets_untraced(line, BUFSIZ, f) != NULL) {
        if (startswith(line, "Pid")) {
            tprintf(line);
        } else if (startswith(line, "PPid")) {
            tprintf(line);
        } else if (startswith(line, "Tgid")) {
            tprintf(line);
        } else if (startswith(line,"VmPeak")) {
            tprintf(line);
        } else if (startswith(line,"VmHWM")) {
            tprintf(line);
        } else if (startswith(line,"Threads")) {
            tprintf(line);
        }
    }

    fclose_untraced(f);
}

/* Read /proc/self/stat to get CPU usage */
static void read_stat() {
    debug("Reading stat file");

    char statf[] = "/proc/self/stat";

    /* If the stat file is missing, then just skip it */
    if (access(statf, F_OK) < 0) {
        return;
    }

    FILE *f = fopen_untraced(statf,"r");
    if (f == NULL) {
        perror("libinterpose: Unable to fopen /proc/self/stat");
        return;
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

    fclose_untraced(f);

    /* Adjust by number of clock ticks per second */
    long clocks = sysconf(_SC_CLK_TCK);
    double real_utime;
    double real_stime;
    double real_iowait;
    real_utime = ((double)utime) / clocks;
    real_stime = ((double)stime) / clocks;
    real_iowait = ((double)iowait) / clocks;

    tprintf("utime: %lf\n", real_utime);
    tprintf("stime: %lf\n", real_stime);
    tprintf("iowait: %lf\n", real_iowait);
}

/* Read /proc/self/io to get I/O usage */
static void read_io() {
    debug("Reading io file");

    char iofile[] = "/proc/self/io";

    /* This proc file was added in Linux 2.6.20. It won't be
     * there on older kernels, or on kernels without task IO 
     * accounting. If it is missing, just bail out.
     */
    if (access(iofile, F_OK) < 0) {
        return;
    }

    FILE *f = fopen_untraced(iofile, "r");
    if (f == NULL) {
        perror("libinterpose: Unable to fopen /proc/self/io");
        return;
    }

    char line[BUFSIZ];
    while (fgets_untraced(line, BUFSIZ, f) != NULL) {
        if (startswith(line, "rchar")) {
            tprintf(line);
        } else if (startswith(line, "wchar")) {
            tprintf(line);
        } else if (startswith(line,"syscr")) {
            tprintf(line);
        } else if (startswith(line,"syscw")) {
            tprintf(line);
        } else if (startswith(line,"read_bytes")) {
            tprintf(line);
        } else if (startswith(line,"write_bytes")) {
            tprintf(line);
        } else if (startswith(line,"cancelled_write_bytes")) {
            tprintf(line);
        }
    }

    fclose_untraced(f);
}

/* Determine which paths should be traced */
static int should_trace(const char *path) {
    /* Trace all files */
    if (getenv("KICKSTART_TRACE_ALL") != NULL) {
        return 1;
    }

    /* Trace files in the current working directory */
    if (getenv("KICKSTART_TRACE_CWD") != NULL) {
        char *wd = getcwd(NULL, 0);
        int incwd = startswith(path, wd);
        free(wd);

        return incwd;
    }

    /* Skip files with known extensions that we don't care about */
    if (endswith(path, ".py") ||
        endswith(path, ".pyc") ||
        endswith(path, ".jar")) {
        return 0;
    }

    /* Skip all the common system paths, which we don't care about */
    if (startswith(path, "/lib") ||
        startswith(path, "/usr") ||
        startswith(path, "/dev") ||
        startswith(path, "/etc") ||
        startswith(path, "/proc")||
        startswith(path, "/sys") ||
        startswith(path, "/selinux")) {
        return 0;
    }

    return 1;
}

static void trace_file(const char *path, int fd) {
    debug("trace_file %s %d", path, fd);

    Descriptor *f = get_descriptor(fd);
    if (f == NULL) {
        return;
    }

    if (!should_trace(path)) {
        return;
    }

    struct stat s;
    if (fstat(fd, &s) != 0) {
        printerr("fstat: %s\n", strerror(errno));
        return;
    }

    /* Skip directories */
    if (s.st_mode & S_IFDIR) {
        return;
    }

    char *temp = strdup(path);
    if (temp == NULL) {
        printerr("strdup: %s\n", strerror(errno));
        return;
    }

    f->type = DTYPE_FILE;
    f->path = temp;
    f->bread = 0;
    f->bwrite = 0;
    f->nread = 0;
    f->nwrite = 0;
    f->bseek = 0;
    f->nseek = 0;
}

static void trace_open(const char *path, int fd) {
    debug("trace_open %s %d", path, fd);

    char *fullpath = realpath(path, NULL);
    if (fullpath == NULL) {
        printerr("Unable to get real path for '%s': %s\n",
                 path, strerror(errno));
        return;
    }

    trace_file(fullpath, fd);

    free(fullpath);
}

static void trace_openat(int fd) {
    debug("trace_openat %d", fd);

    char linkpath[64];
    snprintf(linkpath, 64, "/proc/%d/fd/%d", getpid(), fd);

    char fullpath[BUFSIZ];
    int len = readlink(linkpath, fullpath, BUFSIZ);
    if (len <= 0) {
        printerr("Unable to get real path for fd %d: %s\n",
                 fd, strerror(errno));
        return;
    }
    if (len == BUFSIZ) {
        printerr("Path too long for fd %d: %s\n",
                 fd, strerror(errno));
        return;
    }
    /* readlink doesn't add a null byte */
    fullpath[len] = '\0';

    trace_file(fullpath, fd);
}

static void trace_read(int fd, ssize_t amount) {
    debug("trace_read %d %lu", fd, amount);

    Descriptor *f = get_descriptor(fd);
    if (f == NULL) {
        return;
    }
    f->bread += amount;
    f->nread += 1;
}

static void trace_write(int fd, ssize_t amount) {
    debug("trace_write %d %lu", fd, amount);

    Descriptor *f = get_descriptor(fd);
    if (f == NULL) {
        return;
    }
    f->bwrite += amount;
    f->nwrite += 1;
}

static void trace_seek(int fd, off_t offset) {
    debug("trace_seek %d %ld", fd, offset);

    Descriptor *f = get_descriptor(fd);
    if (f == NULL) {
        return;
    }
    f->bseek += offset > 0 ? offset : -offset;
    f->nseek += 1;
}

static void trace_close(int fd) {
    Descriptor *f = get_descriptor(fd);
    if (f == NULL) {
        return;
    }

    if (f->path == NULL) {
        /* If the path is null, then it is a descriptor we aren't tracking */
        return;
    }

    debug("trace_close %d", fd);

    if (f->type == DTYPE_FILE) {
        /* Try to get the final size of the file */
        size_t size = 0;
        struct stat st;
        if (stat(f->path, &st) == 0) {
            size = st.st_size;
        }

        tprintf("file: '%s' %lu %lu %lu %lu %lu %lu %lu\n",
                f->path, size, f->bread, f->bwrite, f->nread, f->nwrite, f->bseek, f->nseek);
    } else if (f->type == DTYPE_SOCK) {
        tprintf("socket: %s %lu %lu %lu %lu\n", f->path, f->bread, f->bwrite, f->nread, f->nwrite);
    }

    /* Reset the entry */
    free(f->path);
    f->type = DTYPE_NONE;
    f->path = NULL;
    f->bread = 0;
    f->bwrite = 0;
    f->nread = 0;
    f->nwrite = 0;
    f->bseek = 0;
    f->nseek = 0;
}

static void trace_sock(int sockfd, const struct sockaddr *addr, socklen_t addrlen) {
    debug("trace_sock %d");

    Descriptor *d = get_descriptor(sockfd);
    if (d == NULL) {
        return;
    }

    char *addrstr = get_addr(addr, addrlen);
    if (addrstr == NULL) {
        /* It is not a type of socket we understand */
        return;
    }

    debug("sock addr %s", addrstr);

    if (d->path == NULL || strcmp(addrstr, d->path) != 0) {
        /* This is here to handle the case where a socket is reused to connect
         * to another address without being closed first. This happens, for example,
         * with DNS lookups in curl.
         */
        trace_close(sockfd);

        /* Reset the descriptor */
        d->type = DTYPE_NONE;
        d->path = NULL;
        d->bread = 0;
        d->bwrite = 0;
        d->nread = 0;
        d->nwrite = 0;
        d->bseek = 0;
        d->nseek = 0;

        char *temp = strdup(addrstr);
        if (temp == NULL) {
            printerr("strdup: %s\n", strerror(errno));
            return;
        }

        d->type = DTYPE_SOCK;
        d->path = temp;
    }
}

static void trace_dup(int oldfd, int newfd) {
    debug("trace_dup %d %d", oldfd, newfd);

    if (oldfd == newfd) {
        printerr("trace_dup: duplicating the same fd %d\n", oldfd);
        return;
    }

    Descriptor *o = get_descriptor(oldfd);
    if (o == NULL) {
        return;
    }

    /* Not a descriptor we are tracing */
    if (o->path == NULL) {
        return;
    }

    /* Just in case newfd is already open */
    trace_close(newfd);

    char *temp = strdup(o->path);
    if (temp == NULL) {
        printerr("strdup: %s\n", strerror(errno));
        return;
    }

    /* Copy the old descriptor into the new */
    Descriptor *n = get_descriptor(newfd);
    n->type = o->type;
    n->path = temp;
    n->bread = 0;
    n->bwrite = 0;
    n->nread = 0;
    n->nwrite = 0;
    n->bseek = 0;
    n->nseek = 0;
}

static void trace_truncate(const char *path, off_t length) {
    debug("trace_truncate %s %lu", path, length);

    char *fullpath = realpath(path, NULL);
    if (fullpath == NULL) {
        printerr("Unable to get real path for '%s': %s\n",
                 path, strerror(errno));
        return;
    }

    tprintf("file: '%s' %lu 0 0 0 0\n", fullpath, length);

    free(fullpath);
}

#ifdef HAS_PAPI

void init_papi() {
    int err;

    papi_ok = 0;

    err = PAPI_library_init(PAPI_VER_CURRENT);
    if (err != PAPI_VER_CURRENT) {
        printerr("PAPI_library_init failed: %s\n", PAPI_strerror(err));
        return;
    }

    err = PAPI_thread_init(interpose_gettid);
    if (err < 0) {
        printerr("PAPI_thread_init failed: %s\n", PAPI_strerror(err));
        return;
    }

    if (PAPI_num_counters() <= 0) {
        printerr("No hardware counters or PAPI not supported\n");
        return;
    }

    papi_ok = 1;
}

/* Start papi counters for a thread */
void start_papi() {
    int err;

    if (!papi_ok) {
        return;
    }

    interpose_papi_ctx *ctx = malloc(sizeof(interpose_papi_ctx));
    if (ctx == NULL) {
        printerr("Error allocating PAPI thread context: %s\n", strerror(errno));
        return;
    }
    ctx->eventset = PAPI_NULL;

    err = PAPI_register_thread();
    if (err < 0) {
        printerr("Error registering PAPI thread: %s\n", PAPI_strerror(err));
    }

    err = PAPI_create_eventset(&ctx->eventset);
    if (err < 0) {
        printerr("Unable to create PAPI event set: %s\n", PAPI_strerror(err));
        goto cleanup;
    }

    /* Make sure all the events are available and add them to the set */
    for (int i=0; i<n_papi_events; i++) {
        int event;
        err = PAPI_event_name_to_code(papi_events[i], &event);
        if (err < 0) {
            printerr("Error getting PAPI event code for %s: %s\n", papi_events[i], PAPI_strerror(err));
            goto cleanup;
        }

        PAPI_event_info_t info;
        err = PAPI_get_event_info(event, &info);
        if (err < 0) {
            printerr("Error getting PAPI event info for %s: %s\n", papi_events[i], PAPI_strerror(err));
            goto cleanup;
        }
        if (info.count == 0) {
            printerr("PAPI event %s not available: %s\n", papi_events[i], info.long_descr);
            goto cleanup;
        }

        err = PAPI_add_event(ctx->eventset, event);
        if (err < 0) {
            printerr("Error adding PAPI event %d to event set: %s\n", i, PAPI_strerror(err));
            goto cleanup;
        }
    }

    err = PAPI_start(ctx->eventset);
    if (err < 0) {
        printerr("PAPI_start failed: %s\n", PAPI_strerror(err));
        goto cleanup;
    }

    err = PAPI_set_thr_specific(PAPI_USR1_TLS, ctx);
    if (err < 0) {
        printerr("Unable to set PAPI thread context: %s\n", PAPI_strerror(err));
        goto cleanup;
    }

    return;

cleanup:
    free(ctx);
    return;
}

/* Stop an report papi counters for a thread */
void stop_papi() {
    int err;

    if (!papi_ok) {
        return;
    }

    interpose_papi_ctx *ctx = NULL;
    err = PAPI_get_thr_specific(PAPI_USR1_TLS, (void **)&ctx);
    if (err < 0) {
        printerr("Unable to get PAPI thread context in stop_papi: %s\n", PAPI_strerror(err));
        return;
    }
    if (ctx == NULL) {
        return;
    }

    /* Collect counter values */
    int have_counters = 0;
    long long counters[n_papi_events];
    err = PAPI_stop(ctx->eventset, counters);
    if (err < 0) {
        printerr("PAPI_stop failed: %s\n", PAPI_strerror(err));
    } else {
        have_counters = 1;
    }

    /* Unset the context value so that we don't try to stop the counters again */
    err = PAPI_set_thr_specific(PAPI_USR1_TLS, NULL);
    if (err < 0) {
        printerr("Unable to unset PAPI thread context: %s\n", PAPI_strerror(err));
    }

    free(ctx);

    err = PAPI_unregister_thread();
    if (err < 0) {
        printerr("Error unregistering PAPI thread: %s\n", PAPI_strerror(err));
    }

    /* Report counter values if we have them */
    if (have_counters) {
        for (int i=0; i<n_papi_events; i++) {
            tprintf("%s: %lld\n", papi_events[i], counters[i]);
        }
    }
}

#endif

/* Library initialization function */
static void __attribute__((constructor)) interpose_init(void) {
    /* XXX Note that this might be called twice in one program. Java
     * seems to do this, for example.
     */

    /* Open the trace file */
    topen();

    /* Get file descriptor limit and allocate descriptor table */
    struct rlimit nofile_limit;
    getrlimit(RLIMIT_NOFILE, &nofile_limit);
    max_descriptors = nofile_limit.rlim_max;
    descriptors = (Descriptor *)calloc(sizeof(Descriptor), max_descriptors);
    if (descriptors == NULL) {
        printerr("calloc: %s\n", strerror(errno));
    }

    debug("Max descriptors: %d", max_descriptors);

    tprintf("start: %lf\n", get_time());

#ifdef HAS_PAPI
    init_papi();
    /* Start papi for main thread */
    start_papi();
#endif
}

/* Library finalizer function */
static void __attribute__((destructor)) interpose_fini(void) {
    /* Look for descriptors not explicitly closed */
    for(int i=0; i<max_descriptors; i++) {
        trace_close(i);
    }

    read_exe();
    read_status();
    read_stat();
    read_io();

#ifdef HAS_PAPI
    /* Stop papi for (hopefully) main thread */
    stop_papi();
#endif

    tprintf("stop: %lf\n", get_time());

    /* Close trace file */
    tclose();
}


/** INTERPOSED FUNCTIONS **/

int dup(int oldfd) {
    debug("dup");

    if (orig_dup == NULL) {
        orig_dup = dlsym(RTLD_NEXT, "dup");
    }

    int rc = (*orig_dup)(oldfd);

    if (rc >= 0) {
        trace_dup(oldfd, rc);
    }

    return rc;
}

int dup2(int oldfd, int newfd) {
    debug("dup2");

    if (orig_dup2 == NULL) {
        orig_dup2 = dlsym(RTLD_NEXT, "dup2");
    }

    int rc = (*orig_dup2)(oldfd, newfd);

    if (rc >= 0) {
        trace_dup(oldfd, rc);
    }

    return rc;
}

#ifdef dup3
int dup3(int oldfd, int newfd, int flags) {
    debug("dup3");

    if (orig_dup3 == NULL) {
        orig_dup3 = dlsym(RTLD_NEXT, "dup3");
    }

    int rc = (*orig_dup3)(oldfd, newfd, flags);

    if (rc >= 0) {
        trace_dup(oldfd, newfd);
    }

    return rc;
}
#endif

int open(const char *path, int oflag, ...) {
    debug("open");

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
        trace_open(path, rc);
    }

    return rc;
}

int open64(const char *path, int oflag, ...) {
    debug("open64");

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
        trace_open(path, rc);
    }

    return rc;
}

int openat(int dirfd, const char *path, int oflag, ...) {
    debug("openat");

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
    debug("openat64");

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
    debug("creat");

    if (orig_creat == NULL) {
        orig_creat = dlsym(RTLD_NEXT, "creat");
    }

    int rc = (*orig_creat)(path, mode);

    if (rc >= 0) {
        trace_open(path, rc);
    }

    return rc;
}

int creat64(const char *path, mode_t mode) {
    debug("creat64");

    if (orig_creat64 == NULL) {
        orig_creat64 = dlsym(RTLD_NEXT, "creat64");
    }

    int rc = (*orig_creat64)(path, mode);

    if (rc >= 0) {
        trace_open(path, rc);
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
    debug("fopen");

    FILE *f = fopen_untraced(path, mode);

    if (f != NULL) {
        trace_open(path, fileno(f));
    }

    return f;
}

FILE *fopen64(const char *path, const char *mode) {
    debug("fopen64");

    if (orig_fopen64 == NULL) {
        orig_fopen64 = dlsym(RTLD_NEXT, "fopen64");
    }

    FILE *f = (*orig_fopen64)(path, mode);

    if (f != NULL) {
        trace_open(path, fileno(f));
    }

    return f;
}

FILE *freopen(const char *path, const char *mode, FILE *stream) {
    debug("freopen");

    if (orig_freopen == NULL) {
        orig_freopen = dlsym(RTLD_NEXT, "freopen");
    }

    FILE *f = orig_freopen(path, mode, stream);

    if (f != NULL) {
        trace_open(path, fileno(f));
    }

    return f;
}

FILE *freopen64(const char *path, const char *mode, FILE *stream) {
    debug("freopen64");

    if (orig_freopen64 == NULL) {
        orig_freopen64 = dlsym(RTLD_NEXT, "freopen64");
    }

    FILE *f = orig_freopen64(path, mode, stream);

    if (f != NULL) {
        trace_open(path, fileno(f));
    }

    return f;
}

int close(int fd) {
    debug("close");

    if (orig_close == NULL) {
        orig_close = dlsym(RTLD_NEXT, "close");
    }

    int rc = (*orig_close)(fd);

    if (fd >= 0) {
        trace_close(fd);
    }

    return rc;
}

static int fclose_untraced(FILE *fp) {
    if (orig_fclose == NULL) {
        orig_fclose = dlsym(RTLD_NEXT, "fclose");
    }

    return (*orig_fclose)(fp);
}

int fclose(FILE *fp) {
    debug("fclose");

    int fd = -1;
    if (fp != NULL) {
        fd = fileno(fp);
    }

    int rc = fclose_untraced(fp);

    if (fd >= 0) {
        trace_close(fd);
    }

    return rc;
}

ssize_t read(int fd, void *buf, size_t count) {
    debug("read");

    if (orig_read == NULL) {
        orig_read = dlsym(RTLD_NEXT, "read");
    }

    ssize_t rc = (*orig_read)(fd, buf, count);

    if (rc > 0) {
        trace_read(fd, rc);
    }

    return rc;
}

ssize_t write(int fd, const void *buf, size_t count) {
    debug("write");

    if (orig_write == NULL) {
        orig_write = dlsym(RTLD_NEXT, "write");
    }

    ssize_t rc = (*orig_write)(fd, buf, count);

    if (rc > 0) {
        trace_write(fd, rc);
    }

    return rc;
}

size_t fread(void *ptr, size_t size, size_t nmemb, FILE *stream) {
    debug("fread");

    if (orig_fread == NULL) {
        orig_fread = dlsym(RTLD_NEXT, "fread");
    }

    size_t rc = (*orig_fread)(ptr, size, nmemb, stream);

    if (rc > 0) {
        trace_read(fileno(stream), rc);
    }

    return rc;
}

size_t fwrite(const void *ptr, size_t size, size_t nmemb, FILE *stream) {
    debug("fwrite");

    if (orig_fwrite == NULL) {
        orig_fwrite = dlsym(RTLD_NEXT, "fwrite");
    }

    size_t rc = (*orig_fwrite)(ptr, size, nmemb, stream);

    if (rc > 0) {
        trace_write(fileno(stream), rc);
    }

    return rc;
}

ssize_t pread(int fd, void *buf, size_t count, off_t offset) {
    debug("pread");

    if (orig_pread == NULL) {
        orig_pread = dlsym(RTLD_NEXT, "pread");
    }

    ssize_t rc = (*orig_pread)(fd, buf, count, offset);

    if (rc > 0) {
        trace_read(fd, rc);
    }

    return rc;
}

ssize_t pread64(int fd, void *buf, size_t count, off_t offset) {
    debug("pread64");

    if (orig_pread64 == NULL) {
        orig_pread64 = dlsym(RTLD_NEXT, "pread64");
    }

    ssize_t rc = (*orig_pread64)(fd, buf, count, offset);

    if (rc > 0) {
        trace_read(fd, rc);
    }

    return rc;
}

ssize_t pwrite(int fd, const void *buf, size_t count, off_t offset) {
    debug("pwrite");

    if (orig_pwrite == NULL) {
        orig_pwrite = dlsym(RTLD_NEXT, "pwrite");
    }

    ssize_t rc = (*orig_pwrite)(fd, buf, count, offset);

    if (rc > 0) {
        trace_write(fd, rc);
    }

    return rc;
}

ssize_t pwrite64(int fd, const void *buf, size_t count, off_t offset) {
    debug("pwrite64");

    if (orig_pwrite64 == NULL) {
        orig_pwrite64 = dlsym(RTLD_NEXT, "pwrite64");
    }

    ssize_t rc = (*orig_pwrite64)(fd, buf, count, offset);

    if (rc > 0) {
        trace_write(fd, rc);
    }

    return rc;
}

ssize_t readv(int fd, const struct iovec *iov, int iovcnt) {
    debug("readv");

    if (orig_readv == NULL) {
        orig_readv = dlsym(RTLD_NEXT, "readv");
    }

    ssize_t rc = (*orig_readv)(fd, iov, iovcnt);

    if (rc > 0) {
        trace_read(fd, rc);
    }

    return rc;
}

#ifdef preadv
ssize_t preadv(int fd, const struct iovec *iov, int iovcnt, off_t offset) {
    debug("preadv");

    if (orig_preadv == NULL) {
        orig_preadv = dlsym(RTLD_NEXT, "preadv");
    }

    ssize_t rc = (*orig_preadv)(fd, iov, iovcnt, offset);

    if (rc > 0) {
        trace_read(fd, rc);
    }

    return rc;
}
#endif

#ifdef preadv64
ssize_t preadv64(int fd, const struct iovec *iov, int iovcnt, off_t offset) {
    debug("preadv64");

    if (orig_preadv64 == NULL) {
        orig_preadv64 = dlsym(RTLD_NEXT, "preadv64");
    }

    ssize_t rc = (*orig_preadv64)(fd, iov, iovcnt, offset);

    if (rc > 0) {
        trace_read(fd, rc);
    }

    return rc;
}
#endif

ssize_t writev(int fd, const struct iovec *iov, int iovcnt) {
    debug("writev");

    if (orig_writev == NULL) {
        orig_writev = dlsym(RTLD_NEXT, "writev");
    }

    ssize_t rc = (*orig_writev)(fd, iov, iovcnt);

    if (rc > 0) {
        trace_write(fd, rc);
    }

    return rc;
}

#ifdef pwritev
ssize_t pwritev(int fd, const struct iovec *iov, int iovcnt, off_t offset) {
    debug("pwritev");

    if (orig_pwritev == NULL) {
        orig_pwritev = dlsym(RTLD_NEXT, "pwritev");
    }

    ssize_t rc = (*orig_pwritev)(fd, iov, iovcnt, offset);

    if (rc > 0) {
        trace_write(fd, rc);
    }

    return rc;
}
#endif

#ifdef pwritev64
ssize_t pwritev64(int fd, const struct iovec *iov, int iovcnt, off_t offset) {
    debug("pwritev64");

    if (orig_pwritev64 == NULL) {
        orig_pwritev64 = dlsym(RTLD_NEXT, "pwritev64");
    }

    ssize_t rc = (*orig_pwritev64)(fd, iov, iovcnt, offset);

    if (rc > 0) {
        trace_write(fd, rc);
    }

    return rc;
}
#endif

int fgetc(FILE *stream) {
    debug("fgetc");

    if (orig_fgetc == NULL) {
        orig_fgetc = dlsym(RTLD_NEXT, "fgetc");
    }

    int rc = (*orig_fgetc)(stream);

    if (rc > 0) {
        trace_read(fileno(stream), 1);
    }

    return rc;
}

int fputc(int c, FILE *stream) {
    debug("fputc");

    if (orig_fputc == NULL) {
        orig_fputc = dlsym(RTLD_NEXT, "fputc");
    }

    int rc = (*orig_fputc)(c, stream);

    if (rc > 0) {
        trace_write(fileno(stream), 1);
    }

    return rc;
}

static char *fgets_untraced(char *s, int size, FILE *stream) {
    if (orig_fgets == NULL) {
        orig_fgets = dlsym(RTLD_NEXT, "fgets");
    }

    return (*orig_fgets)(s, size, stream);
}

char *fgets(char *s, int size, FILE *stream) {
    debug("fgets");

    char *ret = fgets_untraced(s, size, stream);

    if (ret != NULL) {
        trace_read(fileno(stream), strlen(ret));
    }

    return ret;
}

int fputs(const char *s, FILE *stream) {
    debug("fputs");

    if (orig_fputs == NULL) {
        orig_fputs = dlsym(RTLD_NEXT, "fputs");
    }

    int rc = (*orig_fputs)(s, stream);

    if (rc > 0) {
        trace_write(fileno(stream), strlen(s));
    }

    return rc;
}

int vfscanf(FILE *stream, const char *format, va_list ap) {
    debug("vfscanf");

    if (orig_vfscanf == NULL) {
        orig_vfscanf = dlsym(RTLD_NEXT, "vfscanf");
    }

    /* We need to get the offset because (v)fscanf returns
     * the number of items matched, not the number of bytes
     * read
     */
    long before = ftell(stream);

    int rc = (*orig_vfscanf)(stream, format, ap);

    if (rc > 0) {
        long after = ftell(stream);
        trace_read(fileno(stream), (after-before));
    }

    return rc;
}

int fscanf(FILE *stream, const char *format, ...) {
    debug("fscanf");

    va_list ap;
    va_start(ap, format);
    int rc = vfscanf(stream, format, ap);
    va_end(ap);
    return rc;
}

static int vfprintf_untraced(FILE *stream, const char *format, va_list ap) {
    if (orig_vfprintf == NULL) {
        orig_vfprintf = dlsym(RTLD_NEXT, "vfprintf");
    }

    return (*orig_vfprintf)(stream, format, ap);
}

int vfprintf(FILE *stream, const char *format, va_list ap) {
    debug("vfprintf");

    int rc = vfprintf_untraced(stream, format, ap);

    if (rc > 0) {
        trace_write(fileno(stream), rc);
    }

    return rc;
}

static int fprintf_untraced(FILE *stream, const char *format, ...) {
    va_list ap;
    va_start(ap, format);
    int rc = vfprintf_untraced(stream, format, ap);
    va_end(ap);
    return rc;
}

int fprintf(FILE *stream, const char *format, ...) {
    debug("fprintf");

    va_list ap;
    va_start(ap, format);
    int rc = vfprintf(stream, format, ap);
    va_end(ap);
    return rc;
}

int connect(int sockfd, const struct sockaddr *addr, socklen_t addrlen) {
    debug("connect");

    if (orig_connect == NULL) {
        orig_connect = dlsym(RTLD_NEXT, "connect");
    }

    int rc = (*orig_connect)(sockfd, addr, addrlen);

    /* FIXME There are potential issues with non-blocking sockets here */
    if (rc < 0 && errno != EINPROGRESS) {
        return rc;
    }

    trace_sock(sockfd, addr, addrlen);

    return rc;
}

ssize_t send(int sockfd, const void *buf, size_t len, int flags) {
    debug("send");

    if (orig_send == NULL) {
        orig_send = dlsym(RTLD_NEXT, "send");
    }

    ssize_t rc = (*orig_send)(sockfd, buf, len, flags);

    if (rc > 0) {
        trace_write(sockfd, rc);
    }

    return rc;
}

ssize_t sendfile(int out_fd, int in_fd, off_t *offset, size_t count) {
    debug("sendfile");

    if (orig_sendfile == NULL) {
        orig_sendfile = dlsym(RTLD_NEXT, "sendfile");
    }

    ssize_t rc = (*orig_sendfile)(out_fd, in_fd, offset, count);

    if (rc > 0) {
        trace_read(in_fd, rc);
        trace_write(out_fd, rc);
    }

    return rc;
}

ssize_t sendto(int sockfd, const void *buf, size_t len, int flags,
               const struct sockaddr *dest_addr, socklen_t addrlen) {
    debug("sendto");

    if (orig_sendto == NULL) {
        orig_sendto = dlsym(RTLD_NEXT, "sendto");
    }

    ssize_t rc = (*orig_sendto)(sockfd, buf, len, flags, dest_addr, addrlen);

    if (rc > 0) {
        /* Make sure socket has the right address */
        trace_sock(sockfd, dest_addr, addrlen);
        trace_write(sockfd, rc);
    }

    return rc;
}

ssize_t sendmsg(int sockfd, const struct msghdr *msg, int flags) {
    debug("sendmsg");

    if (orig_sendmsg == NULL) {
        orig_sendmsg = dlsym(RTLD_NEXT, "sendmsg");
    }

    ssize_t rc = (*orig_sendmsg)(sockfd, msg, flags);

    if (rc > 0) {
        /* msg might have an address we need to use */
        if (msg->msg_name != NULL) {
            trace_sock(sockfd, (const struct sockaddr *)msg->msg_name, msg->msg_namelen);
        }
        trace_write(sockfd, rc);
    }

    return rc;
}

ssize_t recv(int sockfd, void *buf, size_t len, int flags) {
    debug("recv");

    if (orig_recv == NULL) {
        orig_recv = dlsym(RTLD_NEXT, "recv");
    }

    ssize_t rc = (*orig_recv)(sockfd, buf, len, flags);

    if (rc > 0) {
        trace_read(sockfd, rc);
    }

    return rc;
}

ssize_t recvfrom(int sockfd, void *buf, size_t len, int flags,
                 struct sockaddr *src_addr, socklen_t *addrlen) {
    debug("recvfrom");

    if (orig_recvfrom == NULL) {
        orig_recvfrom = dlsym(RTLD_NEXT, "recvfrom");
    }

    ssize_t rc = (*orig_recvfrom)(sockfd, buf, len, flags, src_addr, addrlen);

    if (rc > 0) {
        /* Make sure that the socket has the right address */
        trace_sock(sockfd, src_addr, *addrlen);
        trace_read(sockfd, rc);
    }

    return rc;
}

ssize_t recvmsg(int sockfd, struct msghdr *msg, int flags) {
    debug("recvmsg");

    if (orig_recvmsg == NULL) {
        orig_recvmsg = dlsym(RTLD_NEXT, "recvmsg");
    }

    ssize_t rc = (*orig_recvmsg)(sockfd, msg, flags);

    if (rc > 0) {
        /* msg might contain an address we need to get */
        if (msg->msg_name != NULL) {
            trace_sock(sockfd, (const struct sockaddr *)msg->msg_name, msg->msg_namelen);
        }
        trace_read(sockfd, rc);
    }

    return rc;
}

int truncate(const char *path, off_t length) {
    debug("truncate");

    if (orig_truncate == NULL) {
        orig_truncate = dlsym(RTLD_NEXT, "truncate");
    }

    int rc = (*orig_truncate)(path, length);

    if (rc == 0) {
        trace_truncate(path, length);
    }

    return rc;
}

int mkstemp(char *template) {
    debug("mkstemp");

    if (orig_mkstemp == NULL) {
        orig_mkstemp = dlsym(RTLD_NEXT, "mkstemp");
    }

    int rc = (*orig_mkstemp)(template);

    if (rc >= 0) {
        trace_openat(rc);
    }

    return rc;
}

#ifdef mkostemp
int mkostemp(char *template, int flags) {
    debug("mkostemp");

    if (orig_mkostemp == NULL) {
        orig_mkostemp = dlsym(RTLD_NEXT, "mkostemp");
    }

    int rc = (*orig_mkostemp)(template, flags);

    if (rc >= 0) {
        trace_openat(rc);
    }

    return rc;
}
#endif

#ifdef mkstemps
int mkstemps(char *template, int suffixlen) {
    debug("mkstemps");

    if (orig_mkstemps == NULL) {
        orig_mkstemps = dlsym(RTLD_NEXT, "mkstemps");
    }

    int rc = (*orig_mkstemps)(template, suffixlen);

    if (rc >= 0) {
        trace_openat(rc);
    }

    return rc;
}
#endif

#ifdef mkostemps
int mkostemps(char *template, int suffixlen, int flags) {
    debug("mkostemps");

    if (orig_mkostemps == NULL) {
        orig_mkostemps = dlsym(RTLD_NEXT, "mkostemps");
    }

    int rc = (*orig_mkostemps)(template, suffixlen, flags);

    if (rc >= 0) {
        trace_openat(rc);
    }

    return rc;
}
#endif

FILE *tmpfile(void) {
    debug("tmpfile");

    if (orig_tmpfile == NULL) {
        orig_tmpfile = dlsym(RTLD_NEXT, "tmpfile");
    }

    FILE *f = (*orig_tmpfile)();

    if (f != NULL) {
        trace_openat(fileno(f));
    }

    return f;
}

off_t lseek(int fd, off_t offset, int whence) {
    debug("lseek %d %ld %d", fd, offset, whence);

    if (orig_lseek == NULL) {
        orig_lseek = dlsym(RTLD_NEXT, "lseek");
    }

    off_t result = (*orig_lseek)(fd, offset, whence);

    if (result >= 0) {
        trace_seek(fd, offset);
    }

    return result;
}

#ifdef lseek64
off64_t lseek64(int fd, off64_t offset, int whence) {
    debug("lseek64");

    if (orig_lseek64 == NULL) {
        orig_lseek64 = dlsym(RTLD_NEXT, "lseek64");
    }

    off64_t result = (*orig_lseek64)(fd, offset, whence);

    if (result >= 0) {
        trace_seek(fd, offset);
    }

    return result;
}
#endif

int fseek(FILE *stream, long offset, int whence) {
    debug("fseek");

    if (orig_fseek == NULL) {
        orig_fseek = dlsym(RTLD_NEXT, "fseek");
    }

    int result = (*orig_fseek)(stream, offset, whence);

    if (result == 0) {
        trace_seek(fileno(stream), offset);
    }

    return result;
}

int fseeko(FILE *stream, off_t offset, int whence) {
    debug("fseeko");

    if (orig_fseeko == NULL) {
        orig_fseeko = dlsym(RTLD_NEXT, "fseeko");
    }

    int result = (*orig_fseeko)(stream, offset, whence);

    if (result == 0) {
        trace_seek(fileno(stream), offset);
    }

    return result;
}

void interpose_pthread_cleanup(void *arg) {
#ifdef HAS_PAPI
    stop_papi();
#endif
    free(arg);
}

/* This function wraps the start_routine of the thread provided by the user */
void *interpose_pthread_wrapper(void *arg) {
    debug("pthread_wrapper");

    interpose_pthread_wrapper_arg *info = (interpose_pthread_wrapper_arg *)arg;
    if (info == NULL) {
        /* Probably won't ever happen */
        printerr("FATAL ERROR: interpose_pthread_wrapper argument was NULL: pthread_create start_routine lost\n");
        exit(1);
    }

    /* This sets up a key whose destructor cleans up the thread wrapper */
    if (pthread_key_create(&info->cleanup, interpose_pthread_cleanup) != 0) {
        printerr("Error creating cleanup key for thread %lu\n", interpose_gettid());
    }
    if (pthread_setspecific(info->cleanup, arg) != 0) {
        printerr("Unable to set cleanup key for thread %lu\n", interpose_gettid());
    }

#ifdef HAS_PAPI
    start_papi();
#endif

    return info->start_routine(info->arg);
}

int pthread_create(pthread_t *thread, const pthread_attr_t *attr, void *(*start_routine)(void *), void *arg) {
    debug("pthread_create");

    if (orig_pthread_create == NULL) {
        orig_pthread_create = dlsym(RTLD_NEXT, "pthread_create");
    }

    interpose_pthread_wrapper_arg *info = malloc(sizeof(interpose_pthread_wrapper_arg));
    if (info == NULL) {
        printerr("Error creating pthread wrapper: %s\n", strerror(errno));
        return (*orig_pthread_create)(thread, attr, start_routine, arg);
    }

    info->start_routine = start_routine;
    info->arg = arg;

    return (*orig_pthread_create)(thread, attr, interpose_pthread_wrapper, info);
}

