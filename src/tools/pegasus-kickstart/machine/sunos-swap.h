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
 * Copyright 1999-2008 University of Chicago and The University of
 * Southern California. All rights reserved.
 */
#ifndef _MACHINE_SUNOS_SWAP_H
#define _MACHINE_SUNOS_SWAP_H

#include <sys/types.h>

int
gather_sunos_swap( uint64_t* total, uint64_t* free );
/* purpose: collect swap information from solaris
 * warning: This compilation unit MUST be compiled WITHOUT LFS support!
 * paramtr: total (OUT): total size of all swapping
 *          free (OUT): free size of all swapping
 */

void
gather_sunos_proc( unsigned* total, unsigned* good,
		   unsigned* active, unsigned* zombie,
		   uint64_t* size, uint64_t* rss );
/* purpose: collect proc information from solaris
 * warning: This compilation unit MUST be compiled WITHOUT LFS support!
 * paramtr: total (OUT): all eligible entries found in /proc
 *          good (OUT): portion of total we were able to read from
 *          active (OUT): number of active THREADS (LWP)
 *          zombie (OUT): number of zombie THREADS (LWP)
 *          size (OUT): sum of every process's SIZE
 *          rss (OUT): sum of every process's RSS
 */

#endif /* _MACHINE_SUNOS_SWAP_H */
