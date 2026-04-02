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
#ifndef _STATINFO_H
#define _STATINFO_H

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

typedef enum {
    IS_INVALID    = 0,
    IS_FILE       = 1,
    IS_HANDLE     = 2,
    IS_TEMP       = 3,
    IS_FIFO       = 4
} StatSource;

typedef struct {
    StatSource source;
    struct {
        int descriptor;           /* IS_HANDLE, IS_TEMP|FIFO, openmode IS_FILE */
        const char* name;         /* IS_FILE, IS_TEMP|FIFO */
    } file;
    int error;
    int deferred;                 /* IS_FILE: truncate was deferred */
    union {
        unsigned char header[16]; /* IS_FILE regular init */
        struct {
            size_t count;         /* IS_FIFO msg count */
            size_t rsize;         /* IS_FIFO input byte count */
            size_t wsize;         /* IS_FIFO output byte count */
        } fifo;
    } client;
    struct stat info;
    const char* lfn;              /* from -s/-S option */
} StatInfo;

/* size of the <data> section returned for stdout and stderr. */
extern size_t data_section_size;

extern int forcefd(const StatInfo* info, int fd);
extern int initStatInfoAsTemp(StatInfo* statinfo, char* pattern);
extern int initStatInfoFromName(StatInfo* statinfo, const char* filename,
                                int openmode, int flag);
extern int initStatInfoFromHandle(StatInfo* statinfo, int descriptor);
extern int updateStatInfo(StatInfo* statinfo);
extern int addLFNToStatInfo(StatInfo* info, const char* lfn);
extern size_t printYAMLStatInfo(FILE *out, int indent, const char* id,
                               const StatInfo* info, int includeData, int useCDATA,
                               int allowTruncate);
extern void deleteStatInfo(StatInfo* statinfo);

#endif /* _STATINFO_H */
