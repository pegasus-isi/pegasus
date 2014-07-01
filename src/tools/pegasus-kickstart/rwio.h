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
#ifndef _RWIO_H
#define _RWIO_H

#include <sys/types.h>

#ifndef DEFAULT_SYNC_IDLE
#define DEFAULT_SYNC_IDLE 100
#endif

extern ssize_t writen(int fd, const char* buffer, ssize_t n, unsigned restart);
extern int lockit(int fd, int cmd, int type);
extern int mytrylock(int fd);
extern int nfs_sync(int fd, unsigned idle);

#endif /* _RWIO_H */
