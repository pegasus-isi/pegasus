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
#include "sunos.hh"

static const char* RCS_ID =
  "$Id$";

#include <stdio.h>
#include <sys/vfstab.h>
#include <sys/statvfs.h>
#include <string.h>
#include "basic.hh"
#include <stdlib.h>
#include <unistd.h>

#include <sys/stat.h>
#if defined(_LP64) || _FILE_OFFSET_BITS != 64
#include <sys/swap.h>
#endif

#include <sys/sysinfo.h>
#include <sys/param.h> /* FSCALE */
#include <kstat.h>


void
pegasus_statfs( char* buffer, size_t capacity )
{
  FILE* vfstab = fopen( VFSTAB, "r" ); 

  if ( vfstab ) { 
    struct vfstab mtab; 
    char line[1024];
    while ( getvfsent(vfstab,&mtab) == 0 ) {
      struct statvfs vfs; 
      if ( mtab.vfs_special[0] == '/' && statvfs( mtab.vfs_mountp, &vfs ) != -1 ) {
	if ( vfs.f_bsize > 0 && vfs.f_blocks > 0 ) { 
	  char units = 'G'; 
	  unsigned long long size = vfs.f_frsize;
	  unsigned long total = gigs(size * vfs.f_blocks);
	  unsigned long avail = gigs(size * vfs.f_bavail); 
	  if ( total == 0 ) {
	    units = 'M';
	    total = megs(size * vfs.f_blocks);
	    avail = megs(size * vfs.f_bavail); 
	  }

	  snprintf( line, sizeof(line),
		    "Filesystem Info: %-24s %s %5lu%cB total, %5lu%cB avail\n",
		    mtab.vfs_mountp, mtab.vfs_fstype,
		    total, units, avail, units ); 
	  strncat( buffer, line, capacity );
	}
      }
    }
    fclose(vfstab); 
  }
}

static
void
assign( void* dst, size_t size, kstat_named_t* knp )
{
  switch ( size ) {
  case 4: // 32 bit target
    switch ( knp->data_type ) {
    case KSTAT_DATA_INT32:
    case KSTAT_DATA_UINT32:
      *((uint32_t*) dst) = knp->value.ui32;
      break;
    case KSTAT_DATA_INT64:
    case KSTAT_DATA_UINT64:
      *((uint32_t*) dst) = (uint32_t) ( knp->value.ui64 & 0xFFFFFFFFFull ); 
      break;
    }
    break;
  case 8: // 64 bit target
    switch ( knp->data_type ) {
    case KSTAT_DATA_INT32:
      *((int64_t*) dst) = knp->value.i32;
      break;
    case KSTAT_DATA_UINT32:
      *((uint64_t*) dst) = knp->value.ui32;
      break;
    case KSTAT_DATA_INT64:
    case KSTAT_DATA_UINT64:
      *((uint64_t*) dst) = knp->value.ui64;
      break;
    }
    break;
  }
}

struct SunOSMachineInfo {
  uint64_t swap_total;
  uint64_t swap_free;
  uint64_t ram_total;
  uint64_t ram_free; 

  unsigned short cpu_count;
  unsigned short cpu_online; 

  unsigned long megahertz;
  char          brand_id[20];
  char          cpu_type[20];
  char          model_name[20];

  double  load[3]; 
}; 

static struct SunOSMachineInfo info; 
static int init_flag = 1; 

static
void
initMachine()
{
  uint64_t pagesize = getpagesize();
  kstat_ctl_t* kc; 
  
}

void
pegasus_loadavg( char* buffer, size_t capacity )
{
}

void
pegasus_meminfo( char* buffer, size_t capacity )
{
}

void
pegasus_cpuinfo( char* buffer, size_t capacity )
{
}
