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
#ifndef _PEGASUS_SUNOS_SWAP_HH
#define _PEGASUS_SUNOS_SWAP_HH

extern
int
gather_sunos_swap( unsigned long long* total, 
		   unsigned long long* free );
/* purpose: collect swap information from solaris
 * warning: This compilation unit MUST be compiled WITHOUT LFS support!
 * paramtr: total (OUT): total size of all swapping
 *          free (OUT): free size of all swapping
 */

#endif // _PEGASUS_SUNOS_SWAP_HH
