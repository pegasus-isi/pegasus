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
#ifndef _EVENT_H
#define _EVENT_H

#include <sys/types.h>
#include <signal.h>
#include "statinfo.h"

extern
ssize_t
send_message( int outfd, char* msg, ssize_t msize, unsigned channel );
/* purpose: sends a XML-encoded message chunk back to the application
 * paramtr: outfd (IN): output file descriptor, writable (STDERR_FILENO)
 *          msg (IN): pointer to message
 *          msize (IN): length of message content
 *          channel (IN): which channel to send upon (0 - app)
 */

extern
int
eventLoop( int outfd, StatInfo* fifo, volatile sig_atomic_t* terminate );
/* purpose: copy from input file(s) to output fd while not interrupted.
 * paramtr: outfd (IN): output file descriptor, ready for writing.
 *          fifo (IO): contains input fd, and maintains statistics.
 *          terminate (IN): volatile flag, set in signal handlers.
 * returns: -1 in case of error, 0 for o.k.
 */

#endif /* _EVENT_H */
