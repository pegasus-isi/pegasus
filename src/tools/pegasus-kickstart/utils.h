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


extern ssize_t debugmsg(char* fmt, ...);
/* purpose: create a log line on stderr.
 * paramtr: fmt (IN): printf-style format string
 *          ... (IN): other arguments according to format
 * returns: number of bytes written to STDERR via write()
 */

extern void xmlquote(FILE *out, const char* msg, size_t msglen);
/* purpose: write a possibly binary message to the stream while XML
 *          quoting
 * paramtr: out (IO): stream to write the quoted xml to
 *          msg (IN): message to append to buffer
 *          mlen (IN): length of message area to append
 * returns: nada
 */

extern char * fmtisodate(int isLocal, int isExtended, time_t seconds, long micros);
/* purpose: return an ISO-formatted string for a given timestamp
 * paramtr: isLocal (IN): flag, if 0 use UTC, otherwise use local time
 *          isExtd (IN): flag, if 0 use concise format, otherwise extended
 *          seconds (IN): tv_sec part of timeval
 *          micros (IN): if negative, don't show micros.
 * returns: a pointer to the formatted string
 */

extern double doubletime(const struct timeval t);
/* purpose: convert a structured timeval into seconds with fractions.
 * paramtr: t (IN): a timeval as retured from gettimeofday().
 * returns: the number of seconds with microsecond fraction.
 */

extern void now(struct timeval* t);
/* purpose: capture a point in time with microsecond extension 
 * paramtr: t (OUT): where to store the captured time
 */

extern const char* getTempDir(void);
/* purpose: determine a suitable directory for temporary files.
 * returns: a string with a temporary directory, may still be NULL.
 */

extern char* sizer(char* buffer, size_t capacity, size_t vsize, const void* value);
/* purpose: format an unsigned integer of less-known size. Note that
 *          64bit ints on 32bit systems need %llu, but 64/64 uses %lu
 * paramtr: buffer (IO): area to output into
 *          capacity (IN): extent of the buffer to store things into
 *          vsize (IN): size of the value
 *          value (IN): value to format
 * returns: buffer
 */

#endif /* _UTILS_H */
