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
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/swap.h>

#include <memory.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>

#include "sunos-swap.hh"

static const char* RCS_ID =
  "$Id$";

int
gather_sunos_swap( unsigned long long* total, 
		   unsigned long long* free )
/* purpose: collect swap information from solaris
 * warning: This compilation unit MUST be compiled WITHOUT LFS support!
 * paramtr: total (OUT): total size of all swapping
 *          free (OUT): free size of all swapping
 */
{
  unsigned long long pagesize = getpagesize();
  int num = swapctl( SC_GETNSWP, 0 ); 

  *total = *free = 0ull; 
  if ( num > 0 ) {
    size_t size = (num+1) * sizeof(swapent_t) + sizeof(swaptbl_t);
    swaptbl_t* table = static_cast<swaptbl_t*>( ::malloc(size) ); 
    char dummy[80];
    int i; 

    // we don't care for the path, so init all to the same
    memset( table, 0, size );
    for ( i=0; i<num+1; ++i ) table->swt_ent[i].ste_path = dummy;
    table->swt_n = num+1;

    if ( swapctl( SC_LIST, table ) > 0 ) {
      for ( i=0; i<num; ++i ) {
        // only pages that are not in the process of being deleted
        if ( (table->swt_ent[i].ste_flags & ( ST_INDEL | ST_DOINGDEL) ) == 0 ) {
          *total += ( table->swt_ent[i].ste_pages * pagesize );
          *free  += ( table->swt_ent[i].ste_free  * pagesize );
        }
      }
    }

    ::free( static_cast<void*>(table) );
    return num;
  } else {
    return 0;
  }
}
