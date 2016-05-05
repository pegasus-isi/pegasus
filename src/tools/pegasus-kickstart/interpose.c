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
#include <signal.h>
#ifdef HAS_PAPI
#include <papi.h>
#endif
#include <fnmatch.h>

#include "interpose.h"
#include "interpose_monitoring.h"
#include "error.h"
#include "log.h"

/* TODO Unlocked I/O (e.g. fwrite_unlocked) */
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
static pthread_mutex_t descriptor_mutex = PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP;
static unsigned long long bsend = 0;
static unsigned long long brecv = 0;

#define lock_descriptors() do { \
    trace("lock_descriptors"); \
    if (pthread_mutex_lock(&descriptor_mutex) != 0) { \
        printerr("Error locking descriptor mutex\n"); \
        abort(); \
    } \
} while (0);

#define unlock_descriptors() do { \
    trace("unlock_descriptors"); \
    if (pthread_mutex_unlock(&descriptor_mutex) != 0) { \
        printerr("Error unlocking descriptor mutex\n"); \
        abort(); \
    } \
} while (0);

/* Thread tracking */
static int cur_threads;
static int tot_threads;
static int max_threads;
static pthread_mutex_t thread_track_mutex = PTHREAD_MUTEX_INITIALIZER;

#define lock_threads() do { \
    if (pthread_mutex_lock(&thread_track_mutex) != 0) { \
        printerr("Error locking thread tracking mutex\n"); \
        abort(); \
    } \
} while (0);

#define unlock_threads() do { \
    if (pthread_mutex_unlock(&thread_track_mutex) != 0) { \
        printerr("Error unlocking thread tracking mutex\n"); \
        abort(); \
    } \
} while (0);

static int mypid = 0;

/* This is the trace file where we write information about the process */
static FILE* trace = NULL;

#ifdef HAS_PAPI
static int papi_ok = 0;

char *papi_events[] = {
    "PAPI_TOT_INS",
    "PAPI_LD_INS",
    "PAPI_SR_INS",
    "PAPI_FP_OPS",
    "PAPI_FP_INS",
    "PAPI_L3_TCM",
    "PAPI_L2_TCM",
    "PAPI_L1_TCM"
};

#define n_papi_events (sizeof(papi_events) / sizeof(char *))

#endif

typedef struct {
    void *(*start_routine)(void *);
    void *arg;
    pthread_key_t cleanup;
} interpose_pthread_wrapper_arg;

static FILE *fopen_untraced(const char *path, const char *mode);
static int vfprintf_untraced(FILE *stream, const char *format, va_list ap);
static char *fgets_untraced(char *s, int size, FILE *stream);
static size_t fread_untraced(void *ptr, size_t size, size_t nmemb, FILE *stream);
static int fclose_untraced(FILE *fp);
static int dup_untraced(int fd);

static pid_t gettid(void) {
    return (pid_t)syscall(SYS_gettid);
}

/* Open the trace file */
static int topen() {
    trace("Open trace file");

    char *kickstart_prefix = getenv("KICKSTART_PREFIX");
    if (kickstart_prefix == NULL) {
        printerr("Unable to open trace file: KICKSTART_PREFIX not set in environment\n");
        return -1;
    }

    char filename[BUFSIZ];
    snprintf(filename, BUFSIZ, "%s.%d", kickstart_prefix, getpid());

    trace = fopen_untraced(filename, "a");
    if (trace == NULL) {
        printerr("Unable to open trace file\n");
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

    trace("Close trace file");

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

static void trace_file(const char *path, int fd);

/* Initialize the descriptor table */
static void init_descriptors() {

    lock_descriptors();

    /* Reset bandwidth counters */
    bsend = 0;
    brecv = 0;

    /* Get file descriptor limit and allocate descriptor table */
    max_descriptors = 256;
    descriptors = (Descriptor *)calloc(sizeof(Descriptor), max_descriptors);
    if (descriptors == NULL) {
        printerr("Error allocating descriptor table: calloc: %s\n", strerror(errno));
        abort();
    }

    /* For each open descriptor, initialize the entry */
    DIR *fddir = opendir("/proc/self/fd");
    if (fddir == NULL) {
        printerr("Unable to open /proc/self/fd: %s\n", strerror(errno));
        goto unlock;
    }

    struct dirent *d;
    for (d = readdir(fddir); d != NULL; d = readdir(fddir)) {
        if (d->d_name[0] == '.') {
            continue;
        }

        char path[64];
        snprintf(path, 64, "/proc/self/fd/%s", d->d_name);

        int fd = atoi(d->d_name);

        char linkpath[BUFSIZ];
        int size = readlink(path, linkpath, BUFSIZ);
        if (size < 0) {
            printerr("Unable to readlink %s: %s\n", path, strerror(errno));
            continue;
        }
        if (size == BUFSIZ) {
            printerr("Unable to readlink %s: Real path is too long\n", path);
            continue;
        }
        linkpath[size] = '\0';

        /* Only handle paths */
        if (linkpath[0] != '/') {
            continue;
        }

        trace_file(linkpath, fd);
    }

    closedir(fddir);

unlock:
    unlock_descriptors();
}

/* Make sure the descriptor table is large enough to hold fd */
/* Note: You must be holding the descriptor mutex when you call this */
static void ensure_descriptor(int fd) {
    trace("ensure_descriptor %d", fd);

    if (descriptors == NULL) {
        printerr("Descriptor table not initialized\n");
        abort();
    }

    if (fd < 0) {
        printerr("Invalid descriptor: %d\n", fd);
        abort();
    }

    if (fd < max_descriptors) {
        return;
    }

    /* Determine what the new size of the table should be */
    int newmax = max_descriptors * 2;
    while (fd >= newmax) {
        newmax = newmax * 2;
    }

    /* Allocate a new descriptor table */
    Descriptor *newdescriptors = realloc(descriptors, sizeof(Descriptor) * newmax);
    if (newdescriptors == NULL) {
        printerr("Error reallocating new descriptor table with %d entries: realloc: %s\n",
                 newmax, strerror(errno));
        /* This is a fatal error */
        abort();
    }

    /* Clear the newly allocated entries */
    bzero(&(newdescriptors[max_descriptors]), (newmax-max_descriptors)*sizeof(Descriptor));

    /* Use the new table */
    descriptors = newdescriptors;
    max_descriptors = newmax;
}

/* Get a reference to the given descriptor if it exists */
/* Note: You must be holding the descriptor mutex when you call this */
static Descriptor *get_descriptor(int fd) {
    trace("get_descriptor %d", fd);

    /* Sometimes we try to access a descriptor before the 
     * constructor has been called where the descriptor array
     * is allocated. That can happen, for example, if another library
     * constructor calls an interposed function in its constructor and
     * it is loaded before this library. This check will make sure
     * that any descriptor we try to access is valid.
     */
    if (descriptors == NULL || fd < 0) {
        return NULL;
    }

    /* Make sure that the descriptor table contains this object */
    ensure_descriptor(fd);

    return &(descriptors[fd]);
}

static void read_cmdline() {
    char cmdline[] = "/proc/self/cmdline";

    /* If the cmdline file is missing, then just skip it */
    if (access(cmdline, F_OK) < 0) {
        return;
    }

    FILE *f = fopen_untraced(cmdline, "r");
    if (f == NULL) {
        printerr("Unable to fopen /proc/self/cmdline: %s\n", strerror(errno));
        return;
    }

    /* Record the first 1024 characters of the command line, separated by
     * spaces, and with arguments containing spaces quoted. If the command is
     * longer than 1024 characters, then add '...' at the end. */
    char args[1024];
    size_t asize = fread_untraced(args, 1, 1024, f);
    if (asize <= 0) {
        printerr("Error reading /proc/self/cmdline: %s\n", strerror(errno));
    } else {
        int rsize = asize;
        char *result = malloc(rsize);
        int j = 0;
        int quote = 0;
        for (int i=0; i<asize; i++) {
            /* Handle the case when the output gets too large */
            if (j+5 >= rsize) {
                rsize = rsize * 2;
                char *new = realloc(result, rsize);
                if (new == NULL) {
                    printerr("Error reallocating cmdline array: %s\n", strerror(errno));
                    result[j] = '\0';
                    break;
                }
                result = new;
            }

            if (i == asize-1) {
                if (asize == 1024) {
                    result[j++] = '.';
                    result[j++] = '.';
                    result[j++] = '.';
                } else if (quote) {
                    result[j++] = '"';
                }
                result[j++] = '\0';
            } else if (args[i] == '\0') {
                if (quote) {
                    result[j++] = '"';
                }
                result[j++] = ' ';
                if (strstr(&args[i+1], " ") == NULL) {
                    quote = 0;
                } else {
                    result[j++] = '"';
                    quote = 1;
                }
            } else {
                result[j++] = args[i];
            }
        }
        tprintf("cmd:%s\n", result);
        free(result);
    }

    fclose_untraced(f);
}

/* Return 1 if line begins with tok */
static int startswith(const char *line, const char *tok) {
    return strstr(line, tok) == line;
}

#ifdef HAS_PAPI
static int read_papi(int eventset, ProcStats *stats) {
    if (!papi_ok) {
        return 1;
    }

    int i, rc;
    int nevents = n_papi_events;
    int events[n_papi_events];
    long long counters[n_papi_events];

    /* Get the events that were actually recorded */
    rc = PAPI_list_events(eventset, events, &nevents);
    if (rc != PAPI_OK) {
        if (rc != PAPI_ENOEVST) {
            printerr("ERROR: PAPI_list_events failed: %s\n", PAPI_strerror(rc));
        }
        return 1;
    }

    /* read and aggregate the counters */
    rc = PAPI_read(eventset, counters);
    if (rc != PAPI_OK) {
        printerr("ERROR: No hardware counters or PAPI not supported: %s\n", PAPI_strerror(rc));
        return 1;
    }

    /* Store in stats record */
    for (i = 0; i < nevents; i++) {
        if (events[i] == PAPI_TOT_INS) {
            stats->totins = stats->totins + counters[i];
        } else if (events[i] == PAPI_LD_INS) {
            stats->ldins = stats->ldins + counters[i];
        } else if (events[i] == PAPI_SR_INS) {
            stats->srins = stats->srins + counters[i];
        } else if (events[i] == PAPI_FP_INS) {
            stats->fpins = stats->fpins + counters[i];
        } else if (events[i] == PAPI_FP_OPS) {
            stats->fpops = stats->fpops + counters[i];
        } else if (events[i] == PAPI_L3_TCM) {
            stats->l3misses = stats->l3misses + counters[i];
        } else if (events[i] == PAPI_L2_TCM) {
            stats->l2misses = stats->l2misses + counters[i];
        } else if (events[i] == PAPI_L1_TCM) {
            stats->l1misses = stats->l1misses + counters[i];
        }
    }

    return 0;
}
#endif

void gather_stats(ProcStats *stats) {
    procfs_stats_init(stats);
    procfs_read_stats(getpid(), stats);

    /* This is better than what we get from /proc */
    struct rusage ru;
    if (getrusage(RUSAGE_SELF, &ru) < 0) {
        printerr("Error getting resource usage: %s\n", strerror(errno));
        return;
    }
    stats->utime = (double)ru.ru_utime.tv_sec + (double)ru.ru_utime.tv_usec/1.0e6;
    stats->stime = (double)ru.ru_stime.tv_sec + (double)ru.ru_stime.tv_usec/1.0e6;

    /* Get bandwidth counters */
    lock_descriptors();
    stats->bsend = bsend;
    stats->brecv = brecv;
    unlock_descriptors();

    /* Get PAPI counters */
#ifdef HAS_PAPI
    for (int i=1; i<=tot_threads; i++) {
        read_papi(i, stats);
    }
#endif
}

/* Read stats from procfs */
static void report_stats() {
    ProcStats stats;
    gather_stats(&stats);

    tprintf("exe: %s\n", stats.exe);
    tprintf("VmPeak: %llu\n", stats.vmpeak);
    tprintf("VmHWM: %llu\n", stats.rsspeak);
    tprintf("utime: %.3lf\n", stats.utime);
    tprintf("stime: %.3lf\n", stats.stime);
    tprintf("iowait: %.3lf\n", stats.iowait);
    tprintf("rchar: %llu\n", stats.rchar);
    tprintf("wchar: %llu\n", stats.wchar);
    tprintf("syscr: %lu\n", stats.syscr);
    tprintf("syscw: %lu\n", stats.syscw);
    tprintf("read_bytes: %llu\n", stats.read_bytes);
    tprintf("write_bytes: %llu\n", stats.write_bytes);
    tprintf("cancelled_write_bytes: %llu\n", stats.cancelled_write_bytes);

    interpose_send_stats(&stats);
}

static int path_matches_patterns(const char *path, const char *patterns) {
    char buf[BUFSIZ];
    strncpy(buf, patterns, BUFSIZ);

    char *sav;
    char *token = strtok_r(buf, ":", &sav);
    while (token != NULL) {
        int result = fnmatch(token, path, 0);
        if (result == 0) {
            return 1;
        } else if (result == FNM_NOMATCH) {
            /* No match, do nothing */
        } else {
            printerr("fnmatch('%s', '%s', 0) failed: %s\n", token, path, strerror(errno));
        }
        token = strtok_r(NULL, ":", &sav);
    }

    return 0;
}

/* Determine which paths should be traced */
static int should_trace(int fd, const char *path) {
    /* Trace all files */
    if (getenv("KICKSTART_TRACE_ALL") != NULL) {
        return 1;
    }

    /* Only trace files in the current working directory */
    if (getenv("KICKSTART_TRACE_CWD") != NULL) {
        char *wd = getcwd(NULL, 0);
        int incwd = startswith(path, wd);
        free(wd);
        return incwd;
    }

    /* Ignore a list of patterns */
    char *ignore = getenv("KICKSTART_TRACE_IGNORE");
    if (ignore != NULL) {
        if (path_matches_patterns(path, ignore)) {
            return 0;
        } else {
            return 1;
        }
    }

    /* Match a list of patterns */
    char *match = getenv("KICKSTART_TRACE_MATCH");
    if (match != NULL) {
        if (path_matches_patterns(path, match)) {
            return 1;
        } else {
            return 0;
        }
    }

    /* Don't trace stdio */
    if (fd <= 2) {
        return 0;
    }

    /* Don't trace the trace log! */
    char *prefix = getenv("KICKSTART_PREFIX");
    if (startswith(path, prefix)) {
        return 0;
    }

    /* Skip directories */
    struct stat s;
    if (fstat(fd, &s) != 0) {
        printerr("fstat: %s\n", strerror(errno));
        return 0;
    }
    if (s.st_mode & S_IFDIR) {
        return 0;
    }

    return 1;
}

static void trace_file(const char *path, int fd) {
    trace("trace_file %s %d", path, fd);

    lock_descriptors();

    Descriptor *f = get_descriptor(fd);
    if (f == NULL) {
        goto unlock;
    }

    if (!should_trace(fd, path)) {
        goto unlock;
    }

    char *temp = strdup(path);
    if (temp == NULL) {
        printerr("strdup: %s\n", strerror(errno));
        goto unlock;
    }

    f->type = DTYPE_FILE;
    f->path = temp;
    f->bread = 0;
    f->bwrite = 0;
    f->nread = 0;
    f->nwrite = 0;
    f->bseek = 0;
    f->nseek = 0;

unlock:
    unlock_descriptors();
}

static void trace_open(const char *path, int fd) {
    trace("trace_open %s %d", path, fd);

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
    trace("trace_openat %d", fd);

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
    trace("trace_read %d %lu", fd, amount);

    lock_descriptors();

    Descriptor *f = get_descriptor(fd);
    if (f == NULL) {
        goto unlock;
    }
    f->bread += amount;
    f->nread += 1;

    if (f->type == DTYPE_SOCK) {
        brecv += amount;
    }

unlock:
    unlock_descriptors();
}

static void trace_write(int fd, ssize_t amount) {
    trace("trace_write %d %lu", fd, amount);

    lock_descriptors();

    Descriptor *f = get_descriptor(fd);
    if (f == NULL) {
        goto unlock;
    }
    f->bwrite += amount;
    f->nwrite += 1;

    if (f->type == DTYPE_SOCK) {
        bsend += amount;
    }

unlock:
    unlock_descriptors();
}

static void trace_seek(int fd, off_t offset) {
    trace("trace_seek %d %ld", fd, offset);

    lock_descriptors();

    Descriptor *f = get_descriptor(fd);
    if (f == NULL) {
        goto unlock;
    }
    f->bseek += offset > 0 ? offset : -offset;
    f->nseek += 1;

unlock:
    unlock_descriptors();
}

static void trace_close(int fd) {
    lock_descriptors();

    Descriptor *f = get_descriptor(fd);
    if (f == NULL) {
        goto unlock;
    }

    if (f->path == NULL) {
        /* If the path is null, then it is a descriptor we aren't tracking */
        goto unlock;
    }

    trace("trace_close %d", fd);

    /* Only report files that have ops on them */
    if (f->type == DTYPE_FILE && (f->nread+f->nwrite+f->nseek) > 0) {
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

unlock:
    unlock_descriptors();
}

static void trace_sock(int sockfd, const struct sockaddr *addr, socklen_t addrlen) {
    trace("trace_sock %d", sockfd);

    lock_descriptors();

    Descriptor *d = get_descriptor(sockfd);
    if (d == NULL) {
        goto unlock;
    }

    char *addrstr = get_addr(addr, addrlen);
    if (addrstr == NULL) {
        /* It is not a type of socket we understand */
        goto unlock;
    }

    trace("sock addr %s", addrstr);

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
            goto unlock;
        }

        d->type = DTYPE_SOCK;
        d->path = temp;
    }

unlock:
    unlock_descriptors();
}

static void trace_dup(int oldfd, int newfd) {
    trace("trace_dup %d %d", oldfd, newfd);

    if (oldfd == newfd) {
        printerr("trace_dup: duplicating the same fd %d\n", oldfd);
        return;
    }

    lock_descriptors();

    /* XXX Be careful in this function. Calling get_descriptor with
     * two different file descriptors can cause problems because
     * the second get_descriptor can invalidate the pointer returned by
     * the first! To avoid that, ensure that the descriptor table is
     * large enough to contain both descriptors first.
     */
    ensure_descriptor(newfd);
    ensure_descriptor(oldfd);

    Descriptor *o = get_descriptor(oldfd);
    if (o == NULL) {
        goto unlock;
    }

    /* Not a descriptor we are tracing */
    if (o->path == NULL) {
        goto unlock;
    }

    /* Just in case newfd is already open */
    trace_close(newfd);

    char *temp = strdup(o->path);
    if (temp == NULL) {
        printerr("strdup: %s\n", strerror(errno));
        goto unlock;
    }

    /* Copy the old descriptor into the new */
    Descriptor *n = get_descriptor(newfd);
    if (n == NULL) {
        goto unlock;
    }
    n->type = o->type;
    n->path = temp;
    n->bread = 0;
    n->bwrite = 0;
    n->nread = 0;
    n->nwrite = 0;
    n->bseek = 0;
    n->nseek = 0;

unlock:
    unlock_descriptors();
}

static void trace_truncate(const char *path, off_t length) {
    trace("trace_truncate %s %lu", path, length);

    char *fullpath = realpath(path, NULL);
    if (fullpath == NULL) {
        printerr("Unable to get real path for '%s': %s\n",
                 path, strerror(errno));
        return;
    }

    tprintf("file: '%s' %lu 0 0 0 0\n", fullpath, length);

    free(fullpath);
}

static void report_thread_counters() {
    lock_threads();
    tprintf("threads: %d %d %d\n", cur_threads, max_threads, tot_threads);
    unlock_threads();
}

static void thread_started() {
    lock_threads();

    /* Increment thread counters */
    cur_threads += 1;
    tot_threads += 1;
    if (cur_threads > max_threads) {
        max_threads = cur_threads;
    }

    unlock_threads();
}

static void thread_finished() {
    lock_threads();

    /* Decrement thread counter */
    cur_threads -= 1;

    unlock_threads();
}

static void init_threads() {
    lock_threads();

    cur_threads = 0;
    max_threads = 0;
    tot_threads = 0;

    unlock_threads();

    /* This thread started */
    thread_started();
}

#ifdef HAS_PAPI

static long unsigned int papi_gettid() {
    return (long unsigned int)gettid();
}

static void init_papi() {
    int err;

    papi_ok = 0;

    err = PAPI_library_init(PAPI_VER_CURRENT);
    if (err != PAPI_VER_CURRENT) {
        printerr("PAPI_library_init failed: %s\n", PAPI_strerror(err));
        return;
    }

    err = PAPI_thread_init(papi_gettid);
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
static void start_papi() {
    int err;

    if (!papi_ok) {
        return;
    }

    err = PAPI_register_thread();
    if (err < 0) {
        printerr("Error registering PAPI thread: %s\n", PAPI_strerror(err));
        return;
    }

    int eventset = PAPI_NULL;
    err = PAPI_create_eventset(&eventset);
    if (err < 0) {
        printerr("Unable to create PAPI event set: %s\n", PAPI_strerror(err));
        return;
    }

    /* Make sure all the events are available and add them to the set */
    for (int i=0; i<n_papi_events; i++) {
        int event;
        err = PAPI_event_name_to_code(papi_events[i], &event);
        if (err < 0) {
            printerr("Error getting PAPI event code for %s: %s\n", papi_events[i], PAPI_strerror(err));
            continue;
        }

        PAPI_event_info_t info;
        err = PAPI_get_event_info(event, &info);
        if (err < 0) {
            printerr("Error getting PAPI event info for %s: %s\n", papi_events[i], PAPI_strerror(err));
            continue;
        }
        if (info.count == 0) {
            /* Event is not available */
            //printerr("PAPI event %s not available: %s\n", papi_events[i], info.long_descr);
            continue;
        }

        err = PAPI_add_event(eventset, event);
        if (err < 0) {
            if (err == PAPI_ECNFLCT) {
                /* Event conflicts with another event */
                continue;
            }
            printerr("Error adding PAPI event %d to event set: %s\n", i, PAPI_strerror(err));
            continue;
        }
    }

    err = PAPI_start(eventset);
    if (err < 0) {
        printerr("PAPI_start failed: %s\n", PAPI_strerror(err));
        return;
    }
}

/* Stop and report papi counters for a given eventset */
static void stop_papi(int eventset) {
    int err;

    if (!papi_ok) {
        return;
    }

    /* Collect counter values */
    long long counters[n_papi_events];
    err = PAPI_stop(eventset, counters);
    if (err < 0) {
        printerr("PAPI_stop failed: %s\n", PAPI_strerror(err));
        return;
    }

    /* Get the events that were actually recorded */
    int nevents = n_papi_events;
    int events[n_papi_events];
    err = PAPI_list_events(eventset, events, &nevents);
    if (err < 0) {
        printerr("PAPI_list_events failed: %s\n", PAPI_strerror(err));
        return;
    }

    /* Report counters */
    char eventname[256];
    for (int i=0; i<nevents; i++) {
        PAPI_event_code_to_name(events[i], eventname);
        tprintf("%s: %lld\n", eventname, counters[i]);
    }
}

static void fini_papi() {
    /* It turns out that the eventsets are ID numbers that are
     * allocated sequentially for each thread starting at 1,
     * so we can just * call stop on every one from 1 to
     * tot_threads */
    for (int i=1; i<=tot_threads; i++) {
        stop_papi(i);
    }

    PAPI_shutdown();
}

#endif

/* Library initialization function */
static void __attribute__((constructor)) interpose_init(void) {
    mypid = getpid();

    log_set_name("libinterpose");
    log_set_default_level();

    /* dup stderr because the program might close it. This is
     * untraced because the descriptor table has not been
     * initialized yet */
    int myerr = dup_untraced(STDERR_FILENO);
    log_set_output(myerr);

    /* Open the trace file */
    topen();

    init_descriptors();
    init_threads();

    tprintf("start: %lf\n", get_time());

    tprintf("Pid: %d\n", getpid());
    tprintf("PPid: %d\n", getppid());
    read_cmdline();

#ifdef HAS_PAPI
    init_papi();
    /* Start papi counters for main thread */
    start_papi();
#endif

    /* online monitoring */
    interpose_spawn_monitoring_thread();
}

/* Library finalizer function */
static void __attribute__((destructor)) interpose_fini(void) {
    /* FIXME Prevent a process that calls fork->exec from shutting down libinterpose */
    if (getpid() != mypid) {
        return;
    }

    /* online monitoring */
    interpose_stop_monitoring_thread();

    /* Look for descriptors not explicitly closed */
    for(int i=0; i<max_descriptors; i++) {
        trace_close(i);
    }

    report_thread_counters();

#ifdef HAS_PAPI
    fini_papi();
#endif

    report_stats();

    tprintf("stop: %lf\n", get_time());

    /* Close trace file */
    tclose();

    mypid = 0;
}

static inline void *osym(const char *name) {
    void *orig_symbol = dlsym(RTLD_NEXT, name);
    if (orig_symbol == NULL) {
        printerr("FATAL ERROR: Unable to locate symbol %s: %s\n", name, dlerror());
        abort();
    }
    return orig_symbol;
}

/** INTERPOSED FUNCTIONS **/
#pragma GCC visibility push(default)

static int dup_untraced(int oldfd) {
    typeof(dup) *orig_dup = osym("dup");
    return (*orig_dup)(oldfd);
}

int dup(int oldfd) {
    trace("dup");

    int rc = dup_untraced(oldfd);

    if (rc >= 0) {
        trace_dup(oldfd, rc);
    }

    return rc;
}

int dup2(int oldfd, int newfd) {
    trace("dup2");

    typeof(dup2) *orig_dup2 = osym("dup2");

    int rc = (*orig_dup2)(oldfd, newfd);

    if (rc >= 0) {
        trace_dup(oldfd, rc);
    }

    return rc;
}

#ifdef dup3
int dup3(int oldfd, int newfd, int flags) {
    trace("dup3");

    typeof(dup3) *orig_dup3 = osym("dup3");

    int rc = (*orig_dup3)(oldfd, newfd, flags);

    if (rc >= 0) {
        trace_dup(oldfd, newfd);
    }

    return rc;
}
#endif

int open(const char *path, int oflag, ...) {
    trace("open");

    typeof(open) *orig_open = osym("open");

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
    trace("open64");

    typeof(open64) *orig_open64 = osym("open64");

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
    trace("openat");

    typeof(openat) *orig_openat = osym("openat");

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
    trace("openat64");

    typeof(openat64) *orig_openat64 = osym("openat64");

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
    trace("creat");

    typeof(creat) *orig_creat = osym("creat");

    int rc = (*orig_creat)(path, mode);

    if (rc >= 0) {
        trace_open(path, rc);
    }

    return rc;
}

int creat64(const char *path, mode_t mode) {
    trace("creat64");

    typeof(creat64) *orig_creat64 = osym("creat64");

    int rc = (*orig_creat64)(path, mode);

    if (rc >= 0) {
        trace_open(path, rc);
    }

    return rc;
}

static FILE *fopen_untraced(const char *path, const char *mode) {
    typeof(fopen) *orig_fopen = osym("fopen");
    return (*orig_fopen)(path, mode);
}

FILE *fopen(const char *path, const char *mode) {
    trace("fopen");

    FILE *f = fopen_untraced(path, mode);

    if (f != NULL) {
        trace_open(path, fileno(f));
    }

    return f;
}

FILE *fopen64(const char *path, const char *mode) {
    trace("fopen64");

    typeof(fopen64) *orig_fopen64 = osym("fopen64");
    FILE *f = (*orig_fopen64)(path, mode);

    if (f != NULL) {
        trace_open(path, fileno(f));
    }

    return f;
}

FILE *freopen(const char *path, const char *mode, FILE *stream) {
    trace("freopen");

    typeof(freopen) *orig_freopen = osym("freopen");
    FILE *f = orig_freopen(path, mode, stream);

    if (f != NULL) {
        trace_open(path, fileno(f));
    }

    return f;
}

FILE *freopen64(const char *path, const char *mode, FILE *stream) {
    trace("freopen64");

    typeof(freopen64) *orig_freopen64 = osym("freopen64");
    FILE *f = orig_freopen64(path, mode, stream);

    if (f != NULL) {
        trace_open(path, fileno(f));
    }

    return f;
}

int close(int fd) {
    trace("close");

    typeof(close) *orig_close = osym("close");
    int rc = (*orig_close)(fd);

    if (fd >= 0) {
        trace_close(fd);
    }

    return rc;
}

static int fclose_untraced(FILE *fp) {
    typeof(fclose) *orig_fclose = osym("fclose");
    return (*orig_fclose)(fp);
}

int fclose(FILE *fp) {
    trace("fclose");

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
    trace("read");

    typeof(read) *orig_read = osym("read");
    ssize_t rc = (*orig_read)(fd, buf, count);

    if (rc > 0) {
        trace_read(fd, rc);
    }

    return rc;
}

ssize_t write(int fd, const void *buf, size_t count) {
    trace("write");

    typeof(write) *orig_write = osym("write");
    ssize_t rc = (*orig_write)(fd, buf, count);

    if (rc > 0) {
        trace_write(fd, rc);
    }

    return rc;
}

static size_t fread_untraced(void *ptr, size_t size, size_t nmemb, FILE *stream) {
    typeof(fread) *orig_fread = osym("fread");
    return (*orig_fread)(ptr, size, nmemb, stream);
}

size_t fread(void *ptr, size_t size, size_t nmemb, FILE *stream) {
    trace("fread");

    size_t rc = fread_untraced(ptr, size, nmemb, stream);

    if (rc > 0) {
        /* rc is the number of objects written */
        trace_read(fileno(stream), rc*size);
    }

    return rc;
}

size_t fwrite(const void *ptr, size_t size, size_t nmemb, FILE *stream) {
    trace("fwrite");

    typeof(fwrite) *orig_fwrite = osym("fwrite");
    size_t rc = (*orig_fwrite)(ptr, size, nmemb, stream);

    if (rc > 0) {
        /* rc is the number of objects written */
        trace_write(fileno(stream), rc*size);
    }

    return rc;
}

ssize_t pread(int fd, void *buf, size_t count, off_t offset) {
    trace("pread");

    typeof(pread) *orig_pread = osym("pread");
    ssize_t rc = (*orig_pread)(fd, buf, count, offset);

    if (rc > 0) {
        trace_read(fd, rc);
    }

    return rc;
}

ssize_t pread64(int fd, void *buf, size_t count, off_t offset) {
    trace("pread64");

    typeof(pread64) *orig_pread64 = osym("pread64");
    ssize_t rc = (*orig_pread64)(fd, buf, count, offset);

    if (rc > 0) {
        trace_read(fd, rc);
    }

    return rc;
}

ssize_t pwrite(int fd, const void *buf, size_t count, off_t offset) {
    trace("pwrite");

    typeof(pwrite) *orig_pwrite = osym("pwrite");
    ssize_t rc = (*orig_pwrite)(fd, buf, count, offset);

    if (rc > 0) {
        trace_write(fd, rc);
    }

    return rc;
}

ssize_t pwrite64(int fd, const void *buf, size_t count, off_t offset) {
    trace("pwrite64");

    typeof(pwrite64) *orig_pwrite64 = osym("pwrite64");
    ssize_t rc = (*orig_pwrite64)(fd, buf, count, offset);

    if (rc > 0) {
        trace_write(fd, rc);
    }

    return rc;
}

ssize_t readv(int fd, const struct iovec *iov, int iovcnt) {
    trace("readv");

    typeof(readv) *orig_readv = osym("readv");
    ssize_t rc = (*orig_readv)(fd, iov, iovcnt);

    if (rc > 0) {
        trace_read(fd, rc);
    }

    return rc;
}

#ifdef preadv
ssize_t preadv(int fd, const struct iovec *iov, int iovcnt, off_t offset) {
    trace("preadv");

    typeof(preadv) *orig_preadv = osym("preadv");
    ssize_t rc = (*orig_preadv)(fd, iov, iovcnt, offset);

    if (rc > 0) {
        trace_read(fd, rc);
    }

    return rc;
}
#endif

#ifdef preadv64
ssize_t preadv64(int fd, const struct iovec *iov, int iovcnt, off_t offset) {
    trace("preadv64");

    typeof(preadv64) *orig_preadv64 = osym("preadv64");
    ssize_t rc = (*orig_preadv64)(fd, iov, iovcnt, offset);

    if (rc > 0) {
        trace_read(fd, rc);
    }

    return rc;
}
#endif

ssize_t writev(int fd, const struct iovec *iov, int iovcnt) {
    trace("writev");

    typeof(writev) *orig_writev = osym("writev");
    ssize_t rc = (*orig_writev)(fd, iov, iovcnt);

    if (rc > 0) {
        trace_write(fd, rc);
    }

    return rc;
}

#ifdef pwritev
ssize_t pwritev(int fd, const struct iovec *iov, int iovcnt, off_t offset) {
    trace("pwritev");

    typeof(pwritev) *orig_pwritev = osym("pwritev");
    ssize_t rc = (*orig_pwritev)(fd, iov, iovcnt, offset);

    if (rc > 0) {
        trace_write(fd, rc);
    }

    return rc;
}
#endif

#ifdef pwritev64
ssize_t pwritev64(int fd, const struct iovec *iov, int iovcnt, off_t offset) {
    trace("pwritev64");

    typeof(pwritev64) *orig_pwritev64 = osym("pwritev64");
    ssize_t rc = (*orig_pwritev64)(fd, iov, iovcnt, offset);

    if (rc > 0) {
        trace_write(fd, rc);
    }

    return rc;
}
#endif

int fgetc(FILE *stream) {
    trace("fgetc");

    typeof(fgetc) *orig_fgetc = osym("fgetc");
    int rc = (*orig_fgetc)(stream);

    if (rc > 0) {
        trace_read(fileno(stream), 1);
    }

    return rc;
}

int fputc(int c, FILE *stream) {
    trace("fputc");

    typeof(fputc) *orig_fputc = osym("fputc");
    int rc = (*orig_fputc)(c, stream);

    if (rc > 0) {
        trace_write(fileno(stream), 1);
    }

    return rc;
}

static char *fgets_untraced(char *s, int size, FILE *stream) {
    typeof(fgets) *orig_fgets = osym("fgets");
    return (*orig_fgets)(s, size, stream);
}

char *fgets(char *s, int size, FILE *stream) {
    trace("fgets");

    char *ret = fgets_untraced(s, size, stream);

    if (ret != NULL) {
        trace_read(fileno(stream), strlen(ret));
    }

    return ret;
}

int fputs(const char *s, FILE *stream) {
    trace("fputs");

    typeof(fputs) *orig_fputs = osym("fputs");
    int rc = (*orig_fputs)(s, stream);

    if (rc > 0) {
        trace_write(fileno(stream), strlen(s));
    }

    return rc;
}

int vfscanf(FILE *stream, const char *format, va_list ap) {
    trace("vfscanf");

    typeof(vfscanf) *orig_vfscanf = osym("vfscanf");

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
    trace("fscanf");

    va_list ap;
    va_start(ap, format);
    int rc = vfscanf(stream, format, ap);
    va_end(ap);
    return rc;
}

static int vfprintf_untraced(FILE *stream, const char *format, va_list ap) {
    typeof(vfprintf) *orig_vfprintf = osym("vfprintf");
    return (*orig_vfprintf)(stream, format, ap);
}

int vfprintf(FILE *stream, const char *format, va_list ap) {
    trace("vfprintf");

    int rc = vfprintf_untraced(stream, format, ap);

    if (rc > 0) {
        trace_write(fileno(stream), rc);
    }

    return rc;
}

int fprintf(FILE *stream, const char *format, ...) {
    trace("fprintf");

    va_list ap;
    va_start(ap, format);
    int rc = vfprintf(stream, format, ap);
    va_end(ap);
    return rc;
}

int connect(int sockfd, const struct sockaddr *addr, socklen_t addrlen) {
    trace("connect");

    typeof(connect) *orig_connect = osym("connect");
    int rc = (*orig_connect)(sockfd, addr, addrlen);

    /* FIXME There are potential issues with non-blocking sockets here */
    if (rc < 0 && errno != EINPROGRESS) {
        return rc;
    }

    trace_sock(sockfd, addr, addrlen);

    return rc;
}

ssize_t send(int sockfd, const void *buf, size_t len, int flags) {
    trace("send");

    typeof(send) *orig_send = osym("send");
    ssize_t rc = (*orig_send)(sockfd, buf, len, flags);

    if (rc > 0) {
        trace_write(sockfd, rc);
    }

    return rc;
}

ssize_t sendfile(int out_fd, int in_fd, off_t *offset, size_t count) {
    trace("sendfile");

    typeof(sendfile) *orig_sendfile = osym("sendfile");
    ssize_t rc = (*orig_sendfile)(out_fd, in_fd, offset, count);

    if (rc > 0) {
        trace_read(in_fd, rc);
        trace_write(out_fd, rc);
    }

    return rc;
}

ssize_t sendto(int sockfd, const void *buf, size_t len, int flags,
               const struct sockaddr *dest_addr, socklen_t addrlen) {
    trace("sendto");

    typeof(sendto) *orig_sendto = osym("sendto");
    ssize_t rc = (*orig_sendto)(sockfd, buf, len, flags, dest_addr, addrlen);

    if (rc > 0) {
        /* Make sure socket has the right address */
        trace_sock(sockfd, dest_addr, addrlen);
        trace_write(sockfd, rc);
    }

    return rc;
}

ssize_t sendmsg(int sockfd, const struct msghdr *msg, int flags) {
    trace("sendmsg");

    typeof(sendmsg) *orig_sendmsg = osym("sendmsg");
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
    trace("recv");

    typeof(recv) *orig_recv = osym("recv");
    ssize_t rc = (*orig_recv)(sockfd, buf, len, flags);

    if (rc > 0) {
        trace_read(sockfd, rc);
    }

    return rc;
}

ssize_t recvfrom(int sockfd, void *buf, size_t len, int flags,
                 struct sockaddr *src_addr, socklen_t *addrlen) {
    trace("recvfrom");

    typeof(recvfrom) *orig_recvfrom = osym("recvfrom");
    ssize_t rc = (*orig_recvfrom)(sockfd, buf, len, flags, src_addr, addrlen);

    if (rc > 0) {
        /* Make sure that the socket has the right address */
        trace_sock(sockfd, src_addr, *addrlen);
        trace_read(sockfd, rc);
    }

    return rc;
}

ssize_t recvmsg(int sockfd, struct msghdr *msg, int flags) {
    trace("recvmsg");

    typeof(recvmsg) *orig_recvmsg = osym("recvmsg");
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
    trace("truncate");

    typeof(truncate) *orig_truncate = osym("truncate");
    int rc = (*orig_truncate)(path, length);

    if (rc == 0) {
        trace_truncate(path, length);
    }

    return rc;
}

int mkstemp(char *template) {
    trace("mkstemp");

    typeof(mkstemp) *orig_mkstemp = osym("mkstemp");
    int rc = (*orig_mkstemp)(template);

    if (rc >= 0) {
        trace_openat(rc);
    }

    return rc;
}

#ifdef mkostemp
int mkostemp(char *template, int flags) {
    trace("mkostemp");

    typeof(mkostemp) *orig_mkostemp = osym("mkostemp");
    int rc = (*orig_mkostemp)(template, flags);

    if (rc >= 0) {
        trace_openat(rc);
    }

    return rc;
}
#endif

#ifdef mkstemps
int mkstemps(char *template, int suffixlen) {
    trace("mkstemps");

    typeof(mkstemps) *orig_mkstemps = osym("mkstemps");
    int rc = (*orig_mkstemps)(template, suffixlen);

    if (rc >= 0) {
        trace_openat(rc);
    }

    return rc;
}
#endif

#ifdef mkostemps
int mkostemps(char *template, int suffixlen, int flags) {
    trace("mkostemps");

    typeof(mkostemps) *orig_mkostemps = osym("mkostemps");
    int rc = (*orig_mkostemps)(template, suffixlen, flags);

    if (rc >= 0) {
        trace_openat(rc);
    }

    return rc;
}
#endif

FILE *tmpfile(void) {
    trace("tmpfile");

    typeof(tmpfile) *orig_tmpfile = osym("tmpfile");
    FILE *f = (*orig_tmpfile)();

    if (f != NULL) {
        trace_openat(fileno(f));
    }

    return f;
}

off_t lseek(int fd, off_t offset, int whence) {
    trace("lseek %d %ld %d", fd, offset, whence);

    typeof(lseek) *orig_lseek = osym("lseek");
    off_t result = (*orig_lseek)(fd, offset, whence);

    if (result >= 0) {
        trace_seek(fd, offset);
    }

    return result;
}

#ifdef lseek64
off64_t lseek64(int fd, off64_t offset, int whence) {
    trace("lseek64");

    typeof(lseek64) *orig_lseek64 = osym("lseek64");
    off64_t result = (*orig_lseek64)(fd, offset, whence);

    if (result >= 0) {
        trace_seek(fd, offset);
    }

    return result;
}
#endif

int fseek(FILE *stream, long offset, int whence) {
    trace("fseek");

    typeof(fseek) *orig_fseek = osym("fseek");
    int result = (*orig_fseek)(stream, offset, whence);

    if (result == 0) {
        trace_seek(fileno(stream), offset);
    }

    return result;
}

int fseeko(FILE *stream, off_t offset, int whence) {
    trace("fseeko");

    typeof(fseeko) *orig_fseeko = osym("fseeko");
    int result = (*orig_fseeko)(stream, offset, whence);

    if (result == 0) {
        trace_seek(fileno(stream), offset);
    }

    return result;
}

static void interpose_pthread_cleanup(void *arg) {
    /* Update thread counters */
    thread_finished();

    /* Free the pthread wrapper */
    free(arg);
}

/* This function wraps the start_routine of the thread provided by the user */
static void *interpose_pthread_wrapper(void *arg) {
    trace("pthread_wrapper");

#ifdef HAS_PAPI
    /* Tell papi to start recording events for this thread */
    start_papi();
#endif

    /* Update thread counters */
    thread_started();

    interpose_pthread_wrapper_arg *info = (interpose_pthread_wrapper_arg *)arg;
    if (info == NULL) {
        /* Probably won't ever happen */
        printerr("FATAL ERROR: interpose_pthread_wrapper argument was NULL: pthread_create start_routine lost\n");
        abort();
    }

    /* This sets up a key whose destructor cleans up the thread wrapper */
    if (pthread_key_create(&info->cleanup, interpose_pthread_cleanup) != 0) {
        printerr("Error creating cleanup key for thread %d\n", gettid());
    }
    if (pthread_setspecific(info->cleanup, arg) != 0) {
        printerr("Unable to set cleanup key for thread %d\n", gettid());
    }

    return info->start_routine(info->arg);
}

int pthread_create(pthread_t *thread, const pthread_attr_t *attr, void *(*start_routine)(void *), void *arg) {
    trace("pthread_create");

    typeof(pthread_create) *orig_pthread_create = osym("pthread_create");

    interpose_pthread_wrapper_arg *info = malloc(sizeof(interpose_pthread_wrapper_arg));
    if (info == NULL) {
        printerr("Error creating pthread wrapper: %s\n", strerror(errno));
        return (*orig_pthread_create)(thread, attr, start_routine, arg);
    }

    info->start_routine = start_routine;
    info->arg = arg;

    return (*orig_pthread_create)(thread, attr, interpose_pthread_wrapper, info);
}

int execl(const char *path, const char *arg, ...) {
    trace("execl");

    int nargs;
    va_list argp;
    const char *p;

    /* Count arguments */
    nargs = 0;
    p = arg;
    va_start(argp, arg);
    while (p != NULL) {
        nargs++;
        p = va_arg(argp, char *);
    }
    va_end(argp);

    /* Construct argument array */
    char **argv = malloc(sizeof(char *) * (nargs+1));

    /* Populate argument array */
    nargs = 0;
    p = arg;
    va_start(argp, arg);
    while (p != NULL) {
        argv[nargs++] = (char *)p;
        p = va_arg(argp, char *);
    }
    argv[nargs++] = NULL;
    va_end(argp);

    return execv(path, argv);
}

int execlp(const char *file, const char *arg, ...) {
    trace("execlp");

    int nargs;
    va_list argp;
    const char *p;

    /* Count arguments */
    nargs = 0;
    p = arg;
    va_start(argp, arg);
    while (p != NULL) {
        nargs++;
        p = va_arg(argp, char *);
    }
    va_end(argp);

    /* Construct argument array */
    char **argv = malloc(sizeof(char *) * (nargs+1));

    /* Populate argument array */
    nargs = 0;
    p = arg;
    va_start(argp, arg);
    while (p != NULL) {
        argv[nargs++] = (char *)p;
        p = va_arg(argp, char *);
    }
    argv[nargs++] = NULL;
    va_end(argp);

    return execvp(file, argv);
}

int execle(const char *path, const char *arg, ... /*, char * const envp[]*/) {
    trace("execle");

    int nargs;
    va_list argp;
    const char *p;

    /* Count arguments */
    nargs = 0;
    p = arg;
    va_start(argp, arg);
    while (p != NULL) {
        nargs++;
        p = va_arg(argp, char *);
    }
    va_end(argp);

    /* Construct argument array */
    char **argv = malloc(sizeof(char *) * (nargs+1));

    /* Populate argument array */
    nargs = 0;
    p = arg;
    va_start(argp, arg);
    while (p != NULL) {
        argv[nargs++] = (char *)p;
        p = va_arg(argp, char *);
    }
    argv[nargs++] = NULL;
    char **envp = va_arg(argp, char **);
    va_end(argp);

    return execve(path, argv, envp);
}

int execv(const char *path, char *const argv[]) {
    trace("execv");
    typeof(execv) *orig_execv = osym("execv");
    interpose_fini();
    int rc = (*orig_execv)(path, argv);
    interpose_init();
    return rc;
}

int execvp(const char *file, char *const argv[]) {
    trace("execvp");
    typeof(execvp) *orig_execvp = osym("execvp");
    interpose_fini();
    int rc = (*orig_execvp)(file, argv);
    interpose_init();
    return rc;
}

int execve(const char *filename, char *const argv[], char *const envp[]) {
    trace("execve");
    typeof(execve) *orig_execve = osym("execve");
    interpose_fini();
    int rc = (*orig_execve)(filename, argv, envp);
    interpose_init();
    return rc;
}

pid_t fork(void) {
    /* We have to intercept fork so that we can reinit libinterpose in the
     * child. vfork does not have this problem because a process created
     * with vfork basically can't do anything except call exec, in which
     * case libinterpose is going to be reinitialized anyway. */

    typeof(fork) *orig_fork = osym("fork");
    pid_t rc = (*orig_fork)();

    if (rc == 0) {
        /* Close the trace file since we inherited it */
        tclose();

        /* Reinitialize libinterpose on a successful fork */
        interpose_init();

        tprintf("fork\n");
    }

    return rc;
}

void _exit(int rc) {
    /* Regular exit() will call the destructor, but if the app calls _exit we
     * have to do this manually */
    interpose_fini();

    typeof(_exit) *orig__exit = osym("_exit");
    (*orig__exit)(rc);

    /* unreachable */
    abort();
}

#pragma GCC visibility pop
