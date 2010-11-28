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

extern
int
debugmsg( char* fmt, ... );
/* purpose: create a log line on stderr.
 * paramtr: fmt (IN): printf-style format string
 *          ... (IN): other arguments according to format
 * returns: number of bytes written to STDERR via write()
 */

extern
int 
hexdump( void* area, size_t size );
/* purpose: dump a memory area in old-DOS style hex chars and printable ASCII
 * paramtr: area (IN): pointer to area start
 *          size (IN): extent of area to print
 * returns: number of byte written 
 */ 

#endif /* _DEBUG_H */
