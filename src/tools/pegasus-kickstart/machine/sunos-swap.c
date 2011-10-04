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
#include "sunos-swap.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/swap.h>

#include <memory.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <procfs.h>

static const char* RCS_ID =
  "$Id$";

int
gather_sunos_swap( uint64_t* stotal, uint64_t* sfree )
/* purpose: collect swap information from solaris
 * warning: This compilation unit MUST be compiled WITHOUT LFS support!
 * paramtr: total (OUT): total size of all swapping
 *          free (OUT): free size of all swapping
 * returns: number of swap partitions
 */
{
  uint64_t pagesize = getpagesize(); 
  int num = swapctl( SC_GETNSWP, 0 ); 

  *stotal = *sfree = 0; 
  if ( num > 0 ) {
    size_t size = (num+1) * sizeof(swapent_t) + sizeof(swaptbl_t); 
    swaptbl_t* t = malloc(size);
    char dummy[80];
    int i; 

    /* we don't care for the path, so init all to the same */
    memset( t, 0, size ); 
    for ( i=0; i<num+1; ++i ) t->swt_ent[i].ste_path = dummy; 
    t->swt_n = num+1; 
    
    if ( swapctl( SC_LIST, t ) > 0 ) {
      for ( i=0; i<num; ++i ) {
	/* only pages that are not int he process of being deleted */
	if ( (t->swt_ent[i].ste_flags & ( ST_INDEL | ST_DOINGDEL) ) == 0 ) {
	  *stotal += t->swt_ent[i].ste_pages * pagesize; 
	  *sfree  += t->swt_ent[i].ste_free  * pagesize;
	}
      }
    }

    free((void*) t); 
    return num;
  } else {
    return 0; 
  }
}



void
gather_sunos_proc( unsigned* total, unsigned* good,
		   unsigned* active, unsigned* zombie,
		   uint64_t* size, uint64_t* rss )
/* purpose: collect proc information from solaris
 * warning: This compilation unit MUST be compiled WITHOUT LFS support!
 * paramtr: total (OUT): all eligible entries found in /proc
 *          good (OUT): portion of total we were able to read from
 *          active (OUT): number of active THREADS (LWP)
 *          zombie (OUT): number of zombie THREADS (LWP)
 *          size (OUT): sum of every process's SIZE
 *          rss (OUT): sum of every process's RSS
 */
{
  struct dirent* dp; 
  DIR* proc = opendir("/proc");
  if ( proc ) {
    char psinfo[128];
    struct dirent* dp;
    while ( (dp = readdir(proc)) != NULL ) {
      /* assume proc files start with digit */
      if ( dp->d_name[0] >= '0' && dp->d_name[0] <= '9' ) {
	int fd; 
	snprintf( psinfo, sizeof(psinfo), "/proc/%s/psinfo", dp->d_name );
	(*total)++;
	if ( (fd = open( psinfo, O_RDONLY )) != -1 ) {
	  psinfo_t ps;
	  if ( read( fd, &ps, sizeof(ps) ) >= sizeof(ps) ) {
	    (*good)++;
	    *active += ps.pr_nlwp;
#if OSMINOR > 9
	    /* 20100728 (jsv): bug fix: Only Solaris 10 offers pr_nzomb */ 
	    *zombie += ps.pr_nzomb; 
#else
	    /* "man -s 4 proc": If the process is a zombie, pr_nlwp and
	     * pr_lwp.pr_lwpid are zero and the other fields of pr_lwp
	     * are undefined */
	    if ( ps.pr_nlwp ==0 && ps.pr_lwp.pr_lwpid == 0 )
	      *zombie++; 
#endif
	    *size += ps.pr_size;
	    *rss  += ps.pr_rssize; 
	  }
	  close(fd); 
	}
      }
    }
    closedir(proc);

    /* turn kbyte to byte */
    (*size) <<= 10;
    (*rss ) <<= 10; 
  }
}
