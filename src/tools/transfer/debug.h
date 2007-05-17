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
#ifndef _DEBUG_H
#define _DEBUG_H

#include <sys/types.h>
#include <stdio.h>

extern
ssize_t
debug( const char* fmt, ... );
/* purpose: write a debug message, prefixed by the current thread id, 
 *          as atomic write operation onto stderr (no locking). 
 * warning: message total is limited to and cut off at 4096 bytes. 
 * paramtr: fmt (IN): printf format to be passed on
 * returns: whatever write returns. 
 */

extern
void
hexdump( FILE* out, const char* prefix, 
	 void* area, size_t len, size_t offset );
/* purpose: hex dumps a given memory area into a given file.
 * paramtr: out (IO): file pointer to dump into
 *          prefix (IN): optional message to prefix lines with, NULL permitted
 *          area (IN): start of the memory area to dump
 *          len (IN): length of the memory area not to exceed
 *          offset (IN): offset into the memory area for partial prints
 */

#endif /* _DEBUG_H */
