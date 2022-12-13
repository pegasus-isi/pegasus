/**
 *  Copyright 2007-2016 University Of Southern California
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

#include <sys/types.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/poll.h>
#include <sys/utsname.h>
#include <unistd.h>
#include <string.h>
#include <fnmatch.h>

#include "tools.h"
#include "report.h"

static char* identifier;
struct utsname uname_cache;

#define KS_FLAGS_ARG "ioelnNRBLTIwWSsKk"
#define KS_FLAGS_NOARG "HVXFfqctzZ"

static char* create_identifier() {
    char buffer[128];

    if (uname(&uname_cache) != -1) {
        char* s = strchr(uname_cache.nodename, '.');
        if (s != NULL) *s = 0;
        snprintf(buffer, sizeof(buffer), "%s:%d", uname_cache.nodename, getpid());
    } else {
        fprintf(stderr, "uname: %d: %s\n", errno, strerror(errno));
        memset(&uname_cache, 0, sizeof(uname_cache));
        snprintf(buffer, sizeof(buffer), "unknown:%d", getpid());
    }

    return strdup(buffer);
}

/* purpose: find start of argv excluding kickstart
 * paramtr: argv (IN): invocation argument vector
 * returns: start of argv. Returns 0 if unsure.
 */
int find_application(char *argv[]) {
    int uses_kickstart = 0;

    for (int i=0; argv[i]; i++) {
        char *a = argv[i];

        /* detect presence of kickstart */
        if (i == 0 && fnmatch("*kickstart", a, 0) == 0) {
            uses_kickstart = 1;
            continue;
        }

        if (uses_kickstart) {
            /* in kickstart mode, skip options of kickstart */
            if (a[0] == '-' && strchr(KS_FLAGS_ARG, a[1]) != NULL) {
                /* option with argument */
                /* if a[2] is 0, then it is not like '-e/path/to/file' */
                if (a[2] == 0) i++;
            } else if (a[0] == '-' && strchr(KS_FLAGS_NOARG, a[1]) != NULL) {
                /* option without argument */
            } else {
                uses_kickstart = 0;
            }
        }

        if (!uses_kickstart) {
            if (argv[i] == NULL) {
                /* Maybe it is just kickstart? */
                return 0;
            } else {
                return i;
            }
        }
    }

    /* invalid arguments? */
    return 0;
}

/* purpose: append invocation to logging buffer, but skip kickstart
 * paramtr: msg (IO): initialized buffer to append to
 *          size (IN): capacity of buffer
 *          len (IN): current length of the buffer
 *          argv (IN): invocation argument vector
 * returns: new length of modified buffer
 */
static size_t append_argument(char* msg, size_t size, size_t len, char* argv[]) {
    int uses_kickstart = 0;
    int alen = 0;
    char* extra[3] = { NULL, NULL, NULL };

    for (int i=0 ; argv[i]; ++i ) {
        char* a = argv[i];
        alen = strlen(a);

        /* detect presence of kickstart */
        if (i == 0 && fnmatch("*kickstart", a, 0) == 0) {
            uses_kickstart = 1;
            continue;
        }

        if (uses_kickstart) {
            /* in kickstart mode, skip options of kickstart */
            if (a[0] == '-' && strchr(KS_FLAGS_ARG, a[1]) != NULL) {
                /* option with argument */
                /* if a[2] is 0, then it is not like '-e/path/to/file' */
                /* Need to save -i -o and -e arguments for printing < > and 2> */
                switch (a[1]) {
                case 'i':
                    if (a[2] == 0) extra[0] = argv[++i];
                    else extra[0] = &a[2];
                    break;
                case 'o':
                    if (a[2] == 0) extra[1] = argv[++i];
                    else extra[1] = &a[2];
                    break;
                case 'e':
                    if (a[2] == 0) extra[2] = argv[++i];
                    else extra[2] = &a[2];
                    break;
                default:
                    if (a[2] == 0) ++i;
                    break;
                }
                continue;
            } else if (a[0] == '-' && strchr(KS_FLAGS_NOARG, a[1]) != NULL) {
                /* option without argument */
                continue;
            } else {
                uses_kickstart = 0;
            }
        }

        if (!uses_kickstart) {
            /* in regular mode, add argument to output */
            if (len + alen + 1 > size) {
                /* continuation dots */
                static const char* dots = " ...";
                if (len < size-strlen(dots)-1) {
                    strncat(msg+len, dots, size-len);
                    len += strlen(dots);
                }
                break;
            }

            /* append argument */
            strncat(msg+len, " ", size-len);
            strncat(msg+len, a, size-len);
            len += alen + 1;
        }
    }

    /* simulate stdio redirection */
    for (int i=0; i<3; ++i) {
        if (extra[i] == NULL) {
            continue;
        }

        int skip = 0;
        char* a = extra[i];
        alen = strlen(a);
        if (len + alen + 4 >= size) {
            /* No more space */
            break;
        }

        switch (i) {
        case 0:
            strncat(msg+len, " < ", size-len);
            break;
        case 1:
            strncat(msg+len, " > ", size-len);
            break;
        case 2:
            strncat(msg+len, " 2> ", size-len);
            break;
        }
        skip = (*a == '!' || *a == '^');
        strncat(msg+len, a+skip, size-len);
        len += alen + 3 + (i == 2) - skip;
    }

    return len;
}

/* purpose: fill in POSIX lock structure and attempt lock (or unlock)
 * paramtr: fd (IN): which file descriptor to lock
 *          cmd (IN): F_SETLK, F_GETLK, F_SETLKW
 *          type (IN): F_WRLCK, F_RDLCK, F_UNLCK
 * returns: result from fcntl call
 */
static int lockit(int fd, short cmd, short type) {
    struct flock l;

    memset(&l, 0, sizeof(l));
    l.l_type = type;

    /* full file */
    l.l_whence = SEEK_SET;
    l.l_start = 0;
    l.l_len = 0;

    return fcntl(fd, cmd, &l);
}

/* purpose: Try to lock the file
 * paramtr: fd (IN): open file descriptor
 * returns: -1: fatal error while locking the file, file not locked
 *           0: all backoff attempts failed, file is not locked
 *           1: file is locked
 */
static int mytrylock(int fd) {
    int backoff = 50; /* milliseconds, increasing */
    int retries = 10; /* 2.2 seconds total */
    while (lockit(fd, F_SETLK, F_WRLCK) == -1) {
        if (errno != EACCES && errno != EAGAIN) {
            return -1;
        }
        if (--retries == 0) {
            return 0;
        }
        backoff += 50;
        poll(NULL, 0, backoff);
    }

    return 1;
}

/* purpose: report what has just finished.
 * paramtr: progress (IN): file descriptor open for writing
 *          time (IN): time to report (no millisecond resolution)
 *          duration (IN): duration with millisecond resolution
 *          status (IN): return value from wait() family 
 *          argv (IN): NULL-delimited argument vector of app
 *          use (IN): resource usage from wait4() call
 *          special (IN): set for setup/cleanup jobs.
 *          taskid (IN): task number from input file. 
 * returns: number of bytes written onto "progress"
 */
ssize_t report(int progress, double time, double duration, int status,
               char *argv[], struct rusage *use, const char *special,
               size_t taskid) {

    /* sanity checks */
    if (progress == -1 || argv == NULL) {
        return 0;
    }

    /* singleton */
    if (identifier == NULL) {
        identifier = create_identifier();
    }

    char date[32];
    iso2date(time, date, sizeof(date));

    size_t size = getpagesize();
    char *msg = (char*) malloc(size<<1);

    /* message start */
    if (status == -1 && duration == 0.0 && use == NULL) {
        /* report of seqexec itself */
        snprintf(msg, size, "%s %s 0 0/0 START", date, identifier);
    } else if (special != NULL) {
        /* report from setup/cleanup invocations */
        snprintf(msg, size, "%s %s %s %d/%d %.3f", date, identifier, special,
                 (status >> 8), (status & 127), duration);
    } else {
        /* report from child invocations */
        snprintf(msg, size, "%s %s %lu %d/%d %.3f", date, identifier, taskid,
                 (status >> 8), (status & 127), duration);
    }

    /* add program arguments */
    size_t len = append_argument(msg, size-2, strlen(msg), argv);

    /* optionally add uname (seqexec) or rusage (children) info */
    if (status == -1 && duration == 0.0 && use == NULL) {
        /* report uname info for seqexec itself */
        snprintf(msg+len, size-len,
                 " ### sysname=%s machine=%s release=%s",
                 uname_cache.sysname, uname_cache.machine,
                 uname_cache.release);
        len += strlen(msg+len);
    } else if (use != NULL) {
        double utime = use->ru_utime.tv_sec + use->ru_utime.tv_usec / 1E6;
        double stime = use->ru_stime.tv_sec + use->ru_stime.tv_usec / 1E6;
        snprintf(msg+len, size-len,
                 " ### utime=%.3f stime=%.3f minflt=%ld majflt=%ld"
                 " maxrss=%ld idrss=%ld inblock=%ld oublock=%ld"
                 " nswap=%ld nsignals=%ld nvcws=%ld nivcsw=%ld",
                 utime, stime, use->ru_minflt, use->ru_majflt,
                 use->ru_maxrss, use->ru_idrss, use->ru_inblock,
                 use->ru_oublock, use->ru_nswap, use->ru_nsignals,
                 use->ru_nvcsw, use->ru_nivcsw);
        len += strlen(msg+len);
    }

    /* terminate line */
    strncat(msg+len, "\n", size-len);

    /* Atomic append -- will still garble on Linux NFS */
    /* Warning: Fcntl-locking may block in syscall on broken Linux kernels */
    int locked = mytrylock(progress);
    ssize_t wsize = write(progress, msg, len+1);
    int save = errno;
    if (locked) {
        lockit(progress, F_SETLK, F_UNLCK);
    }

    free((void*) msg );
    errno = save;
    return wsize;
}

