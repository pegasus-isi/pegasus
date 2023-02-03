#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <sys/stat.h>
#include <errno.h>

#include "syscall.h"
#include "error.h"

#ifdef HAS_PTRACE

#define DEBUG_SYSCALL 0

/* On i386 we have to do something special because there
 * is only one system call, socketcall, for sockets.
 */
#ifdef __i386__
# include <linux/net.h> /* for SYS_SOCKET, etc subcalls */
#endif

/* TODO Unit tests
 * Multiple threads (verify I/O correct and reported)
 * Lots of files (verify data structure performance)
 * Sockets and other funny descriptors (verify no errors)
 * Open the same file twice (with and without closing in-between)
 */

/* TODO
 * Improve error handling for each function
 * Handle failures gracefully by untracing the children
 * Develop better data structures for the file lookup and descriptor table
 * Stat on file open and close (verify reopens too)?
 * Handle file access modes (read, write, append, create, delete)
 * Check max file path length
 * Handle directories?
 * Handle unlinks?
 * Handle files accessed by multiple sub-processes or keep separate?
 * Filter out /proc, /lib*, /usr/lib*, /etc, ... ? Or optionally?
 */

static void addFileInfo(ProcInfo *c, FileInfo *f) {
    if (c->files == NULL) {
        c->files = f;
        return;
    }

    FileInfo *i;
    for (i=c->files; i->next!=NULL; i=i->next);
    i->next = f;
    f->next = NULL;
}

static FileInfo *findFileInfo(ProcInfo *c, char *filename) {
    FileInfo *i;
    for (i=c->files; i!=NULL; i=i->next) {
        if (strcmp(i->filename, filename) == 0) {
            return i;
        }
    }
    return NULL;
}

static FileInfo *openFileInfo(ProcInfo *c, int fd, char *filename) {
    // If we have opened it before, then get it
    FileInfo *file = findFileInfo(c, filename);

    // Otherwise, create a new FileInfo entry
    if (file == NULL) {
        file = (FileInfo *)calloc(1, sizeof(FileInfo));
        if (file == NULL) {
            printerr("calloc: %s\n", strerror(errno));
            exit(1);
        }
        file->filename = strdup(filename);
        if (file->filename == NULL) {
            printerr("strdup: %s\n", strerror(errno));
            exit(1);
        }
        addFileInfo(c, file);
    }

    // XXX Fix with dynamic array
    if (fd >= 1024) {
        printerr("ERROR: Too many file descriptors (>1024) for process %d\n", c->pid);
        exit(1);
    }

    // Update the descriptor table
    c->fds[fd] = file;
    return file;
}

static FileInfo *getFileInfo(ProcInfo *c, int fd) {
    // XXX Fix with dynamic array
    if (fd >= 1024) {
        printerr("ERROR: Too many file descriptors (>1024) for process %d\n", c->pid);
        exit(1);
    }

    return c->fds[fd];
}

static FileInfo *closeFileInfo(ProcInfo *c, int fd) {
    FileInfo *file = getFileInfo(c, fd);
    c->fds[fd] = NULL;
    return file;
}

static int readProcFilename(pid_t pid, long fd, char *filename, int fnsize) {
    char linkname[1024];
    snprintf(linkname, 1024, "/proc/%d/fd/%ld", pid, fd);
    int len = readlink(linkname, filename, fnsize);
    filename[len] = '\0';
    return len;
}

int initFileInfo(ProcInfo *c) {
    /* Read /proc/[pid]/fd directory entries and open a file for each link */

    /* Get the dir name */
    char dirname[1024];
    snprintf(dirname, 1024, "/proc/%d/fd", c->pid);

    /* Open the fd dir */
    DIR *fddir = opendir(dirname);
    if (fddir == NULL) {
        return -1;
    }

    /* For each fd entry in the dir, open a FileInfo */
    char filename[8192];
    struct dirent *i;
    for (i = readdir(fddir); i != NULL; i = readdir(fddir)) {
        if (i->d_name[0] == '.') {
            continue;
        }
        // XXX This assumes all of the entries are numbers
        int fd = atoi(i->d_name);
        readProcFilename(c->pid, fd, filename, 8192);
        openFileInfo(c, fd, filename);
    }
    closedir(fddir);
    return 0;
}

int finiFileInfo(ProcInfo *c) {
    FileInfo *i;
    struct stat s;
    for (i = c->files; i != NULL; i = i->next) {
        int rc = stat(i->filename, &s);
        if (rc == 0) {
            i->size = s.st_size;
        }
    }
    return 0;
}

static int common_open(ProcInfo *c) {
    /* If the operation was successful, then add the FileInfo entry */
    if (c->sc_rval >= 0) {
        char filename[8192];
        readProcFilename(c->pid, c->sc_rval, filename, 8192);
        FileInfo *file = openFileInfo(c, c->sc_rval, filename);
        if (DEBUG_SYSCALL) fprintf(stderr, "Opening %s\n", file->filename);
    }
    return 0;
}

static int handle_open(ProcInfo *c) {
    // TODO Handle mode if it is supplied
    return common_open(c);
}

static int handle_creat(ProcInfo *c) {
    // XXX This works right now because we only look at sc_rval
    // TODO Handle mode
    return common_open(c);
}

static int handle_openat(ProcInfo *c) {
    // XXX This works right now because we only look at sc_rval
    // TODO Handle mode if it is supplied
    return common_open(c);
}

static int handle_close(ProcInfo *c) {
    long fd = c->sc_args[0];

    if (DEBUG_SYSCALL) fprintf(stderr, "PID %d: close(%ld) = %ld\n", c->pid, fd, c->sc_rval);

    FileInfo *file = closeFileInfo(c, fd);
    if (file != NULL) {
        if (DEBUG_SYSCALL) fprintf(stderr, "Closing %s\n", file->filename);
    }
    return 0;
}

static int handle_read(ProcInfo *c) {
    long fd = c->sc_args[0];
    long bread = c->sc_rval;

    if (DEBUG_SYSCALL) fprintf(stderr, "PID %d: read(%ld, ...) = %ld\n", c->pid, fd, bread);

    FileInfo *file = getFileInfo(c, fd);
    if (file != NULL && bread > 0) {
        file->bread += bread;
        file->nread += 1;
    }
    return 0;
}

static int handle_write(ProcInfo *c) {
    long fd = c->sc_args[0];
    long bwrite = c->sc_rval;

    if (DEBUG_SYSCALL) fprintf(stderr, "PID %d: write(%ld, ...) = %ld\n", c->pid, fd, bwrite);

    FileInfo *file = getFileInfo(c, fd);
    if (file != NULL && bwrite > 0) {
        file->bwrite += bwrite;
        file->nwrite += 1;
    }
    return 0;
}

static int handle_dup(ProcInfo *c) {
    long oldfd = c->sc_args[0];
    long newfd = c->sc_rval;
    long rc = c->sc_rval;
    if (DEBUG_SYSCALL) fprintf(stderr, "PID %d: dup(%ld) = %ld\n", c->pid, oldfd, newfd);
    if (rc >= 0) {
        c->fds[newfd] = closeFileInfo(c, oldfd);
    }
    return 0;
}

static int handle_dup2(ProcInfo *c) {
    long oldfd = c->sc_args[0];
    long newfd = c->sc_args[1];
    long rc = c->sc_rval;
    if (DEBUG_SYSCALL) fprintf(stderr, "PID %d: dup2(%ld, %ld) = %ld\n", c->pid, oldfd, newfd, rc);
    if (rc >= 0) {
        c->fds[newfd] = closeFileInfo(c, oldfd);
    }
    return 0;
}

/* System call handler table */
const struct syscallent syscalls[] = {
#ifdef __i386__
# include "syscall_32.h"
#else
# include "syscall_64.h"
#endif
};

#endif
