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

/* TODO Interpose accept (for network servers) */
/* TODO Is it necessary to interpose shutdown? Would that help the DNS issue? */
/* TODO Interpose dup and dup2 */
/* TODO Interpose mkstemp family and tmpfile */
/* TODO Interpose rename */
/* TODO Interpose truncate */
/* TODO Interpose unlink, unlinkat, remove */
/* TODO Interpose mknod for S_IFREG */
/* TODO Interpose wide character I/O functions */
/* TODO Handle I/O for stdout/stderr? */
/* TODO asynchronous I/O from librt? */
/* TODO Thread safety? */
/* TODO Add r/w/a mode support? */
/* TODO What happens if one interposed library function calls another (e.g.
 *      fopen calls fopen64)? I think internal calls are not traced.
 */
/* TODO What about mmap? Probably nothing we can do besides interpose
 *      mmap and assume that the total size being mapped is read/written
 */

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
static typeof(preadv) *orig_preadv = NULL;
static typeof(preadv64) *orig_preadv64 = NULL;
static typeof(writev) *orig_writev = NULL;
static typeof(pwritev) *orig_pwritev = NULL;
static typeof(pwritev64) *orig_pwritev64 = NULL;
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

typedef struct {
    char type;
    char *path;
    size_t bread;
    size_t bwrite;
} Descriptor;

const char DTYPE_NONE = 0;
const char DTYPE_FILE = 1;
const char DTYPE_SOCK = 2;

/* File descriptor table */
static Descriptor *descriptors = NULL;
static int max_descriptors = 0;

/* This is the trace file where we write information about the process */
static FILE* trace = NULL;

static FILE *fopen_untraced(const char *path, const char *mode);
static int fprintf_untraced(FILE *stream, const char *format, ...);
static int vfprintf_untraced(FILE *stream, const char *format, va_list ap);
static char *fgets_untraced(char *s, int size, FILE *stream);
static int fclose_untraced(FILE *fp);

/* Open the trace file */
static int topen() {

    char *kickstart_prefix = getenv("KICKSTART_PREFIX");
    if (kickstart_prefix == NULL) {
        fprintf_untraced(stderr, "libinterpose: Unable to open trace file: KICKSTART_PREFIX not set in environment");
        return -1;
    }

    char filename[BUFSIZ];
    snprintf(filename, BUFSIZ, "%s.%d", kickstart_prefix, getpid());

    trace = fopen_untraced(filename, "w+");
    if (trace == NULL) {
        fprintf_untraced(stderr, "libinterpose: Unable to open trace file");
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

/* Read /proc/self/exe to get path to executable */
static void read_exe() {
    char exe[BUFSIZ];
    int size = readlink("/proc/self/exe", exe, BUFSIZ);
    if (size < 0) {
        perror("libinterpose: Unable to readlink /proc/self/exe");
        return;
    }
    exe[size] = '\0';
    tprintf("exe: %s\n", exe);
}

/* Return 1 if line begins with tok */
static int startswith(const char *line, const char *tok) {
    return strstr(line, tok) == line;
}

/* Read useful information from /proc/self/status */
static void read_status() {
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

    unsigned long utime, stime;
    fscanf(f, "%*d %*s %*c %*d %*d %*d %*d %*d "
              "%*u %*u %*u %*u %*u %lu %lu %*d %*d",
           &utime, &stime);

    fclose_untraced(f);

    /* Adjust by number of clock ticks per second */
    long clocks = sysconf(_SC_CLK_TCK);
    double real_utime;
    double real_stime;
    real_utime = ((double)utime) / clocks;
    real_stime = ((double)stime) / clocks;

    tprintf("utime: %lf\n", real_utime);
    tprintf("stime: %lf\n", real_stime);
}

/* Read /proc/self/io to get I/O usage */
static void read_io() {
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

static void trace_file(const char *path, int fd) {
    /* Skip all the common system paths, which we don't care about */
    if (startswith(path, "/lib") ||
        startswith(path, "/usr") ||
        startswith(path, "/dev") ||
        startswith(path, "/etc") ||
        startswith(path, "/proc")||
        startswith(path, "/sys")) {
        return;
    }

    Descriptor *f = &(descriptors[fd]);
    f->type = DTYPE_FILE;
    f->path = strdup(path);
    f->bread = 0;
    f->bwrite = 0;
}

static void trace_open(const char *path, int fd) {
    char fullpath[BUFSIZ];
    if (realpath(path, fullpath) == NULL) {
        fprintf_untraced(stderr, "libinterpose: Unable to get real path for '%s': %s\n",
                path, strerror(errno));
        return;
    }

    trace_file(fullpath, fd);
}

static void trace_openat(int fd) {
    char linkpath[64];
    snprintf(linkpath, 64, "/proc/%d/fd/%d", getpid(), fd);

    char fullpath[BUFSIZ];
    int len = readlink(linkpath, fullpath, BUFSIZ);
    if (len <= 0) {
        fprintf_untraced(stderr, "libinterpose: Unable to get real path for fd %d: %s\n",
                fd, strerror(errno));
        return;
    }
    if (len == BUFSIZ) {
        fprintf_untraced(stderr, "libinterpose: Path too long for fd %d: %s\n",
                fd, strerror(errno));
        return;
    }
    /* readlink doesn't add a null byte */
    fullpath[len] = '\0';

    trace_file(fullpath, fd);
}

static void trace_read(int fd, ssize_t amount) {
    descriptors[fd].bread += amount;
}

static void trace_write(int fd, ssize_t amount) {
    descriptors[fd].bwrite += amount;
}

static void trace_close(int fd) {
    Descriptor *f = &(descriptors[fd]);

    if (f->path == NULL) {
        /* If the path is null, then it is a file we aren't tracing */
        return;
    }

    if (f->type == DTYPE_FILE) {
        struct stat st;
        if (stat(f->path, &st) < 0) {
            fprintf_untraced(stderr, "libinterpose: Unable to stat '%s': %s\n",
                    f->path, strerror(errno));
            return;
        }

        tprintf("file: %s %lu %lu %lu\n", f->path, st.st_size, f->bread, f->bwrite);
    } else if (f->type == DTYPE_SOCK) {
        tprintf("socket: %s %lu %lu\n", f->path, f->bread, f->bwrite);
    }

    /* Reset the entry */
    free(f->path);
    f->type = DTYPE_NONE;
    f->path = NULL;
    f->bread = 0;
    f->bwrite = 0;
}

static void trace_sock(int sockfd, const struct sockaddr *addr, socklen_t addrlen) {
    char *addrstr = get_addr(addr, addrlen);

    Descriptor *d = &descriptors[sockfd];
    if (d->path == NULL || strcmp(addrstr, d->path) != 0) {
        /* This is here to handle the case where a socket is reused to connect
         * to another address without being closed first. This happens, for example,
         * with DNS lookups in curl.
         */
        trace_close(sockfd);

        d->type = DTYPE_SOCK;
        d->path = strdup(addrstr);
    }
}


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

    tprintf("start: %lf\n", get_time());
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

    tprintf("stop: %lf\n", get_time());

    /* Close trace file */
    tclose();
}


/** INTERPOSED FUNCTIONS **/


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
        trace_open(path, rc);
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
        trace_open(path, rc);
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
        trace_open(path, rc);
    }

    return rc;
}

int creat64(const char *path, mode_t mode) {
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
    FILE *f = fopen_untraced(path, mode);

    if (f != NULL) {
        trace_open(path, fileno(f));
    }

    return f;
}

FILE *fopen64(const char *path, const char *mode) {
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
    if (orig_readv == NULL) {
        orig_readv = dlsym(RTLD_NEXT, "readv");
    }

    ssize_t rc = (*orig_readv)(fd, iov, iovcnt);

    if (rc > 0) {
        trace_read(fd, rc);
    }

    return rc;
}

ssize_t preadv(int fd, const struct iovec *iov, int iovcnt, off_t offset) {
    if (orig_preadv == NULL) {
        orig_preadv = dlsym(RTLD_NEXT, "preadv");
    }

    ssize_t rc = (*orig_preadv)(fd, iov, iovcnt, offset);

    if (rc > 0) {
        trace_read(fd, rc);
    }

    return rc;
}

ssize_t preadv64(int fd, const struct iovec *iov, int iovcnt, off_t offset) {
    if (orig_preadv64 == NULL) {
        orig_preadv64 = dlsym(RTLD_NEXT, "preadv64");
    }

    ssize_t rc = (*orig_preadv64)(fd, iov, iovcnt, offset);

    if (rc > 0) {
        trace_read(fd, rc);
    }

    return rc;
}

ssize_t writev(int fd, const struct iovec *iov, int iovcnt) {
    if (orig_writev == NULL) {
        orig_writev = dlsym(RTLD_NEXT, "writev");
    }

    ssize_t rc = (*orig_writev)(fd, iov, iovcnt);

    if (rc > 0) {
        trace_write(fd, rc);
    }

    return rc;
}

ssize_t pwritev(int fd, const struct iovec *iov, int iovcnt, off_t offset) {
    if (orig_pwritev == NULL) {
        orig_pwritev = dlsym(RTLD_NEXT, "pwritev");
    }

    ssize_t rc = (*orig_pwritev)(fd, iov, iovcnt, offset);

    if (rc > 0) {
        trace_write(fd, rc);
    }

    return rc;
}

ssize_t pwritev64(int fd, const struct iovec *iov, int iovcnt, off_t offset) {
    if (orig_pwritev64 == NULL) {
        orig_pwritev64 = dlsym(RTLD_NEXT, "pwritev64");
    }

    ssize_t rc = (*orig_pwritev64)(fd, iov, iovcnt, offset);

    if (rc > 0) {
        trace_write(fd, rc);
    }

    return rc;
}

int fgetc(FILE *stream) {
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
    char *ret = fgets_untraced(s, size, stream);

    if (ret != NULL) {
        trace_read(fileno(stream), strlen(ret));
    }

    return ret;
}

int fputs(const char *s, FILE *stream) {
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
    va_list ap;
    va_start(ap, format);
    int rc = vfprintf(stream, format, ap);
    va_end(ap);
    return rc;
}

int connect(int sockfd, const struct sockaddr *addr, socklen_t addrlen) {
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
    if (orig_sendmsg == NULL) {
        orig_sendmsg = dlsym(RTLD_NEXT, "sendmsg");
    }

    ssize_t rc = (*orig_sendmsg)(sockfd, msg, flags);

    if (rc > 0) {
        // msg might have an address we need to use
        if (msg->msg_name != NULL) {
            trace_sock(sockfd, (const struct sockaddr *)msg->msg_name, msg->msg_namelen);
        }
        trace_write(sockfd, rc);
    }

    return rc;
}

ssize_t recv(int sockfd, void *buf, size_t len, int flags) {
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
    if (orig_recvmsg == NULL) {
        orig_recvmsg = dlsym(RTLD_NEXT, "recvmsg");
    }

    ssize_t rc = (*orig_recvmsg)(sockfd, msg, flags);

    if (rc > 0) {
        // TODO msg might contain an address we need to get
        if (msg->msg_name != NULL) {
            trace_sock(sockfd, (const struct sockaddr *)msg->msg_name, msg->msg_namelen);
        }
        trace_read(sockfd, rc);
    }

    return rc;
}

