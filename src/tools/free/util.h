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
#ifndef _UTIL_H
#define _UTIL_H

#include <sys/types.h>

#if ! defined(MAX)
#define MAX(a,b) ((a) > (b) ? (a) : (b))
#endif

#if ! defined(MIN)
#define MIN(a,b) ((a) < (b) ? (a) : (b))
#endif

extern
void 
show( unsigned megs );
/* purpose: Create the necessary command line information on stdout.
 * paramtr: megs (IN): is the size in MB of true RAM of the host
 * environ: VDS_JAVA_HEAPMAX: maximum size of heap in MB, or 0 for don't set
 *          VDS_JAVA_HEADMIN: minimum size of heap in MB, or 0 for don't set
 */

extern
void
help( int argc, char* argv[], const char* mainid );
/* purpose: Check for the presence of -h, -? or --help, and help.
 * paramtr: argc (IN): see main()
 *          argv (IN): see main()
 *          mainid (IN): main's RCS Id string
 * returns: only in the absence of options
 */

#endif /* _UTIL_H */
