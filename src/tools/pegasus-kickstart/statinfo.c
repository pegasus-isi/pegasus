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
#include <sys/param.h>
#include <limits.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include <unistd.h>
#include <fcntl.h>
#include <grp.h>
#include <pwd.h>

#include "statinfo.h"
#include "utils.h"
#include "checksum.h"
#include "error.h"

size_t data_section_size = 262144ul;

int forcefd(const StatInfo* info, int fd) {
    /* purpose: force open a file on a certain fd
     * paramtr: info (IN): is the StatInfo of the file to connect to (fn or fd)
     *          the mode for potential open() is determined from this, too.
     *          fd (IN): is the file descriptor to plug onto. If this fd is
     *          the same as the descriptor in info, nothing will be done.
     * returns: 0 if all is well, or fn was NULL or empty.
     *          1 if opening a filename failed,
     *          2 if dup2 call failed
     */
    /* is this a regular file with name, or is this a descriptor to copy from? */
    int isHandle = (info->source == IS_HANDLE || info->source == IS_TEMP);
    int mode = info->file.descriptor; /* openmode for IS_FILE */

    /* initialize the newHandle variable by opening regular files, or copying the fd */
    int newfd = isHandle ?
        info->file.descriptor :
        (((mode & O_ACCMODE) == O_RDONLY) ?
          open(info->file.name, mode) :
          /* FIXME: as long as stdout/stderr is shared between jobs,
           * we must always use append mode. Truncation happens during
           * initialization of the shared stdio. */
          open(info->file.name, mode | O_APPEND, 0666));

    /* this should only fail in the open() case */
    if (newfd == -1) {
        return 1;
    }

    /* create a duplicate of the new fd onto the given (stdio) fd. This operation
     * is guaranteed to close the given (stdio) fd first, if open. */
    if (newfd != fd) {
        /* FIXME: Does dup2 guarantee noop for newfd==fd on all platforms ? */
        if (dup2(newfd, fd) == -1) {
            return 2;
        }
    }

    /* if we opened a file, we need to close it again. */
    if (! isHandle) {
        close(newfd);
    }

    return 0;
}

int initStatInfoAsTemp(StatInfo* statinfo, char* pattern) {
    /* purpose: Initialize a stat info buffer with a temporary file
     * paramtr: statinfo (OUT): the newly initialized buffer
     *          pattern (IO): is the input pattern to mkstemp(), will be modified!
     * returns: a value of -1 indicates an error
     */
    memset(statinfo, 0, sizeof(StatInfo));

    int fd = mkstemp(pattern);
    if (fd < 0) {
        printerr("mkstemp: %s\n", strerror(errno));
        goto error;
    }

    char *filename = strdup(pattern);
    if (filename == NULL) {
        printerr("strdup: %s\n", strerror(errno));
        goto error;
    }

    /* try to ensure append mode for the file, because it is shared
     * between jobs. If the SETFL operation fails, well there is nothing
     * we can do about that. */
    int flags = fcntl(fd, F_GETFL);
    if (flags != -1) {
        fcntl(fd, F_SETFL, flags | O_APPEND);
    }

    /* this file descriptor is NOT to be passed to the jobs? So far, the
     * answer is true. We close this fd on exec of sub jobs, so it will
     * be invisible to them. */
    flags = fcntl(fd, F_GETFD);
    if (flags != -1) {
        fcntl(fd, F_SETFD, flags | FD_CLOEXEC);
    }

    /* the return is the chosen filename as well as the opened descriptor.
     * we *could* unlink the filename right now, and be truly private, but
     * users may want to look into the log files of long persisting operations. */
    statinfo->source = IS_TEMP;
    statinfo->file.descriptor = fd;
    statinfo->file.name = filename;
    statinfo->error = 0;

    errno = 0;
    int result = fstat(fd, &statinfo->info);
    if (result < 0) {
        printerr("fstat: %s\n", strerror(errno));
        goto error;
    }

    return 0;

error:
    statinfo->source = IS_INVALID;
    statinfo->error = errno;

    return -1;
}

static int preserveFile(const char* fn) {
    /* purpose: preserve the given file by renaming it with a backup extension.
     * paramtr: fn (IN): name of the file
     * returns: 0: ok; -1: error, check errno
     */
    int i, fd = open(fn, O_RDONLY);
    if (fd != -1) {
        /* file exists, do something */
        size_t size = strlen(fn)+8;
        char* newfn = malloc(size);
        if (newfn == NULL) {
            printerr("malloc: %s\n", strerror(errno));
            return -1;
        }

        close(fd);
        strncpy(newfn, fn, size);
        for (i=0; i<1000; ++i) {
            snprintf(newfn + size-8, 8, ".%03d", i);
            if ((fd = open(newfn, O_RDONLY)) == -1) {
                if (errno == ENOENT) break;
                else return -1;
            }
            close(fd);
        }

        if (i < 1000) {
            return rename(fn, newfn);
        } else {
            /* too many backups */
            errno = EEXIST;
            return -1;
        }
    } else {
        /* file does not exist, nothing to backup */
        errno = 0;
        return 0;
    }
}

int initStatInfoFromName(StatInfo* statinfo, const char* filename,
                         int openmode, int flag) {
    /* purpose: Initialize a stat info buffer with a filename to point to
     * paramtr: statinfo (OUT): the newly initialized buffer
     *          filename (IN): the filename to memorize (deep copy)
     *          openmode (IN): are the fcntl O_* flags to later open calls
     *          flag (IN): bit#0 truncate: whether to reset the file size to zero
     *                     bit#1 defer op: whether to defer opening the file for now
     *                     bit#2 preserve: whether to backup existing target file
     * returns: the result of the stat() system call on the provided file */
    int result = -1;
    memset(statinfo, 0, sizeof(StatInfo));
    statinfo->source = IS_FILE;
    statinfo->file.descriptor = openmode;
    statinfo->file.name = strdup(filename);
    if (statinfo->file.name == NULL) {
        printerr("strdup: %s\n", strerror(errno));
        statinfo->error = errno;
        return -1;
    }

    if ((flag & 0x01) == 1) {
        /* FIXME: As long as we use shared stdio for stdout and stderr, we need
         * to explicitely truncate (and create) file to zero, if not appending.
         */
        if ((flag & 0x02) == 0) {
            int fd;
            if ((flag & 0x04) == 4) {
                preserveFile(filename);
            }
            fd = open(filename, (openmode & O_ACCMODE) | O_CREAT | O_TRUNC, 0666);
            if (fd != -1) {
                close(fd);
            }
        } else {
            statinfo->deferred = 1 | (flag & 0x04);
        }
    }
    /* POST-CONDITION: statinfo->deferred == 1, iff (flag & 3) == 3 */

    errno = 0;
    result = stat(filename, &statinfo->info);
    statinfo->error = errno;

    /* special case, read the start of file (for magic) */
    if ((flag & 0x02) == 0 &&
        result != -1 &&
        S_ISREG(statinfo->info.st_mode) &&
        statinfo->info.st_size > 0) {

        int fd = open(filename, O_RDONLY);
        if (fd != -1) {
            read(fd, (char*) statinfo->client.header, sizeof(statinfo->client.header));
            close(fd);
        }
    }

    return result;
}

int updateStatInfo(StatInfo* statinfo) {
    /* purpose: update existing and initialized statinfo with latest info
     * paramtr: statinfo (IO): stat info pointer to update
     * returns: the result of the stat() or fstat() system call. */
    int result = -1;

    if (statinfo->source == IS_FILE && (statinfo->deferred & 1) == 1) {
        /* FIXME: As long as we use shared stdio for stdout and stderr, we need
         * to explicitely truncate (and create) file to zero, if not appending.
         */
        int fd;
        if ((statinfo->deferred & 4) == 4) {
            preserveFile(statinfo->file.name);
        }
        fd = open(statinfo->file.name,
                  (statinfo->file.descriptor & O_ACCMODE) | O_CREAT | O_TRUNC,
                  0666);
        if (fd != -1) {
            close(fd);
        }

        /* once only */
        statinfo->deferred &= ~1;  /* remove deferred bit */
        statinfo->deferred |=  2;  /* mark as having gone here */
    }

    if (statinfo->source == IS_FILE ||
        statinfo->source == IS_HANDLE ||
        statinfo->source == IS_TEMP ||
        statinfo->source == IS_FIFO) {

        errno = 0;
        if (statinfo->source == IS_FILE) {
            result = stat(statinfo->file.name, &(statinfo->info));
        } else {
            result = fstat(statinfo->file.descriptor, &(statinfo->info));
        }
        statinfo->error = errno;

        if (result != -1 &&
            statinfo->source == IS_FILE &&
            S_ISREG(statinfo->info.st_mode) &&
            statinfo->info.st_size > 0) {

            int fd = open(statinfo->file.name, O_RDONLY);
            if (fd != -1) {
                read(fd, (char*) statinfo->client.header, sizeof(statinfo->client.header));
                close(fd);
            }
        }
    }

    return result;
}

int initStatInfoFromHandle(StatInfo* statinfo, int descriptor) {
    /* purpose: Initialize a stat info buffer with a filename to point to
     * paramtr: statinfo (OUT): the newly initialized buffer
     *          descriptor (IN): the handle to attach to
     * returns: the result of the fstat() system call on the provided handle */
    int result = -1;
    memset(statinfo, 0, sizeof(StatInfo));
    statinfo->source = IS_HANDLE;
    statinfo->file.descriptor = descriptor;

    errno = 0;
    result = fstat(descriptor, &statinfo->info);
    statinfo->error = errno;

    return result;
}

int addLFNToStatInfo(StatInfo* info, const char* lfn) {
    /* purpose: optionally replaces the LFN field with the specified LFN
     * paramtr: info (IO): stat info pointer to update
     *          lfn (IN): LFN to store, use NULL to free
     * returns: errno in case of error, 0 if OK.
     */

    /* sanity check */
    if (info->source == IS_INVALID) {
        return EINVAL;
    }

    if (info->lfn != NULL) {
        free((void*) info->lfn);
    }

    if (lfn == NULL) {
        info->lfn = NULL;
    } else {
        info->lfn = strdup(lfn);
        if (info->lfn == NULL) {
            printerr("strdup: %s\n", strerror(errno));
            return ENOMEM;
        }
    }

    return 0;
}

size_t printXMLStatInfo(FILE *out, int indent, const char* tag, const char* id,
                        const StatInfo* info, int includeData, int useCDATA,
                        int allowTruncate) {
    char *real = NULL;

    /* sanity check */
    if (info->source == IS_INVALID) {
        return 0;
    }

    /* start main tag */
    fprintf(out, "%*s<%s error=\"%d\"", indent, "", tag, info->error);
    if (id != NULL) {
        fprintf(out, " id=\"%s\"", id);
    }
    if (info->lfn != NULL) {
        fprintf(out, " lfn=\"%s\"", info->lfn);
    }
    fprintf(out, ">\n");

    /* NEW: ignore "file not found" error for "kickstart" */
    if (id != NULL && info->error == 2 && strcmp(id, "kickstart") == 0) {
        fprintf(out, "%*s<!-- ignore above error -->\n", indent+2, "");
    }

    /* either a <name> or <descriptor> sub element */
    switch (info->source) {
        case IS_TEMP:   /* preparation for <temporary> element */
            /* late update for temp files */
            errno = 0;
            if (fstat(info->file.descriptor, (struct stat*) &info->info) != -1 &&
                (((StatInfo*) info)->error = errno) == 0) {

                /* obtain header of file */
                int fd = dup(info->file.descriptor);
                if (fd != -1) {
                    if (lseek(fd, 0, SEEK_SET) != -1) {
                        read(fd, (char*) info->client.header, sizeof(info->client.header));
                    }
                    close(fd);
                }
            }

            fprintf(out, "%*s<temporary name=\"%s\" descriptor=\"%d\"/>\n",
                    indent+2, "", info->file.name, info->file.descriptor);
            break;

        case IS_FIFO: /* <fifo> element */
            fprintf(out, "%*s<fifo name=\"%s\" descriptor=\"%d\" count=\"%zu\" rsize=\"%zu\" wsize=\"%zu\"/>\n",
                    indent+2, "", info->file.name, info->file.descriptor,
                    info->client.fifo.count, info->client.fifo.rsize,
                    info->client.fifo.wsize);
            break;

        case IS_FILE: /* <file> element */
            real = realpath(info->file.name, NULL);
            fprintf(out, "%*s<file name=\"%s\"", indent+2, "", real ? real : info->file.name);
            if (real) {
                free((void*) real);
            }

            if (info->error == 0 &&
                S_ISREG(info->info.st_mode) &&
                info->info.st_size > 0) {

                /* optional hex information */
                size_t i, end = sizeof(info->client.header);
                if (info->info.st_size < end) end = info->info.st_size;

                fprintf(out, ">");
                for (i=0; i<end; ++i) {
                    fprintf(out, "%02X", info->client.header[i]);
                }
                fprintf(out, "</file>\n");
            } else {
                fprintf(out, "/>\n");
            }
            break;

        case IS_HANDLE: /* <descriptor> element */
            fprintf(out, "%*s<descriptor number=\"%u\"/>\n", indent+2, "",
                    info->file.descriptor);
            break;

        default: /* this must not happen! */
            fprintf(out, "%*s<!-- ERROR: No valid file info available -->\n",
                    indent+2, "");
            break;
    }

    if (info->error == 0 && info->source != IS_INVALID) {
        /* <stat> subrecord */
        char my[32];
        struct passwd* user = getpwuid(info->info.st_uid);
        struct group* group = getgrgid(info->info.st_gid);

        fprintf(out, "%*s<statinfo mode=\"0%o\"", indent+2, "",
                info->info.st_mode);

        /* Grmblftz, are we in 32bit, 64bit LFS on 32bit, or 64bit on 64 */
        sizer(my, sizeof(my), sizeof(info->info.st_size), &info->info.st_size);
        fprintf(out, " size=\"%s\"", my);

        sizer(my, sizeof(my), sizeof(info->info.st_ino), &info->info.st_ino);
        fprintf(out, " inode=\"%s\"", my);

        sizer(my, sizeof(my), sizeof(info->info.st_nlink), &info->info.st_nlink);
        fprintf(out, " nlink=\"%s\"", my);

        sizer(my, sizeof(my), sizeof(info->info.st_blksize), &info->info.st_blksize);
        fprintf(out, " blksize=\"%s\"", my);

        /* st_blocks is new in iv-1.8 */
        sizer(my, sizeof(my), sizeof(info->info.st_blocks), &info->info.st_blocks);
        fprintf(out, " blocks=\"%s\"", my);

        fprintf(out, " mtime=\"%s\"", fmtisodate(info->info.st_mtime, -1));
        fprintf(out, " atime=\"%s\"", fmtisodate(info->info.st_atime, -1));
        fprintf(out, " ctime=\"%s\"", fmtisodate(info->info.st_ctime, -1));

        fprintf(out, " uid=\"%d\"", info->info.st_uid);
        if (user) {
            fprintf(out, " user=\"%s\"", user->pw_name);
        }
        fprintf(out, " gid=\"%d\"", info->info.st_gid);
        if (group) {
            fprintf(out, " group=\"%s\"", group->gr_name);
        }

        fprintf(out, "/>\n");
    }

    /* checksum the files if the checksum tools are available
     * and it is a "final" entry
     */
    if (id != NULL && info->error == 0 && strcmp(id, "final") == 0) {
        char chksum[65];
        real = realpath(info->file.name, NULL);
        if (sha256(real, chksum)) {
            fprintf(out, "%*s<checksum type=\"sha256\" value=\"%s\"/>\n",
                    indent+2, "",  chksum);
        }
        if (real) {
            free((void*) real);
        }
    }

    /* if truncation is allowed, then the maximum amount of
     * data that can be put into the invocation record is
     * data_section_size, otherwise add the whole file
     */
    size_t fsize = info->info.st_size;
    size_t dsize = fsize;
    if (allowTruncate) {
        dsize = data_section_size;
    }

    /* data section from stdout and stderr of application */
    if (includeData &&
        info->source == IS_TEMP &&
        info->error == 0 &&
        fsize > 0 && dsize > 0) {

        fprintf(out, "%*s<data%s", indent+2, "",
                (fsize > dsize ? " truncated=\"true\"" : ""));
        if (fsize > 0) {
            char buf [BUFSIZ];
            int fd = dup(info->file.descriptor);

            fprintf(out, ">");
            if (fd != -1) {

                if (useCDATA) {
                    fprintf(out, "<![CDATA[");
                }

                /* Get the last dsize bytes of the file */
                size_t offset = 0;
                if (fsize > dsize) {
                    offset = fsize - dsize;
                }
                if (lseek(fd, offset, SEEK_SET) != -1) {
                    ssize_t total = 0;
                    while (total < dsize) {
                        ssize_t rsize = read(fd, buf, BUFSIZ);
                        if (rsize == 0) {
                            break;
                        } else if (rsize < 0) {
                            printerr("ERROR reading %s: %s",
                                    info->file.name, strerror(errno));
                            break;
                        }
                        if (useCDATA) {
                            fwrite(buf, rsize, 1, out);
                        } else {
                            xmlquote(out, buf, rsize);
                        }
                        total += rsize;
                    }
                }
                close(fd);

                if (useCDATA) {
                    fprintf(out, "]]>");
                }
            }

            fprintf(out, "</data>\n");
        } else {
            fprintf(out, "/>\n");
        }
    }

    fprintf(out, "%*s</%s>\n", indent, "", tag);

    return 0;
}

void deleteStatInfo(StatInfo* statinfo) {
    /* purpose: clean up and invalidates structure after being done.
     * paramtr: statinfo (IO): clean up record. */

    if (statinfo->source == IS_FILE ||
        statinfo->source == IS_TEMP ||
        statinfo->source == IS_FIFO) {

        if (statinfo->source == IS_TEMP || statinfo->source == IS_FIFO) {
            close(statinfo->file.descriptor);
            unlink(statinfo->file.name);
        }

        if (statinfo->file.name) {
            free((void*) statinfo->file.name);
            statinfo->file.name = NULL; /* avoid double free */
        }
    }

    /* invalidate */
    statinfo->source = IS_INVALID;
}

