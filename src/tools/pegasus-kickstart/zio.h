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
#ifndef _ZIO_H
#define _ZIO_H

#include <sys/types.h>

#ifndef GZIP_PATH
#define GZIP_PATH "/bin/gzip"
#endif /* GZIP_PATH */

int 
zopen( const char* pathname, int flags, mode_t mode );
/* purpose: open a file, but put gzip into the io-path
 * paramtr: pathname (IN): file to read or create
 *          flags (IN): if O_RDONLY, use gunzip on file
 *                      if O_WRONLY, use gzip on file
 *          mode (IN): file mode, see open(2)
 * returns: -1 in case of error, or an open file descriptor
 */

int
zclose( int fd );
/* purpose: close a file that has a gzip in its io path
 * returns: process status from gzip
 */

#endif /* _ZIO_H */
