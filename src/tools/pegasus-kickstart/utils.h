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
#ifndef _UTILS_H
#define _UTILS_H

#include <stdio.h>
#include <sys/types.h>
#include <sys/time.h>
#include <time.h>

extern void yamlquote(FILE *out, const char* msg, size_t msglen);
extern void yamldump(FILE *in, FILE *out, const int indent);
extern char* fmtisodate(time_t seconds, long micros);
extern double doubletime(const struct timeval t);
extern void now(struct timeval* t);
extern const char* getTempDir(void);
extern char* sizer(char* buffer, size_t capacity, size_t vsize, const void* value);

#endif /* _UTILS_H */
