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
#ifndef _PEGASUS_DARWIN_HH
#define _PEGASUS_DARWIN_HH

#define _DARWIN_FEATURE_64_BIT_INODE 1
#include <sys/types.h>

extern 
void 
pegasus_statfs( char* buffer, size_t capacity );

extern 
void
pegasus_loadavg( char* buffer, size_t capacity );

extern
void
pegasus_meminfo( char* buffer, size_t capacity ); 

extern
void
pegasus_cpuinfo( char* buffer, size_t capacity ); 

#endif // _PEGASUS_DARWIN_HH

