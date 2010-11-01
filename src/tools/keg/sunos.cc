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
#include "sunos-swap.hh"

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

#include <sys/loadavg.h>

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

static int load_flag = -2;
static double load_avg[3]; 

void
pegasus_loadavg( char* buffer, size_t capacity )
{
  if ( load_flag == -2 ) {
    load_flag = getloadavg( load_avg, 3 );
  }

  if ( load_flag >= 0 ) { 
    char line[128];
    snprintf( line, sizeof(line), 
	      "Load Averages  : %.3f %.3f %.3f\n", 
	      load_avg[0], load_avg[1], load_avg[2] ); 
    strncat( buffer, line, capacity );     
  }
}

static int swap_flag = -1; 
static unsigned long long swap_total;
static unsigned long long swap_free;
static unsigned long long ram_total;
static unsigned long long ram_free; 

void
pegasus_meminfo( char* buffer, size_t capacity )
{
  if ( ram_total == 0 ) {
    unsigned long long pagesize = getpagesize(); 
    long tmp; 
    if ( (tmp=sysconf(_SC_PHYS_PAGES)) != -1 )
      ram_total = (tmp * pagesize);
    if ( (tmp=sysconf(_SC_AVPHYS_PAGES)) != -1 )
      ram_free = (tmp * pagesize);
  }

  if ( ram_total > 0 ) {
    char line[128];

    snprintf( line, sizeof(line), 
	      "Memory Usage MB: %lu total, %lu free\n",
	      megs(ram_total), megs(ram_free) ); 
    strncat( buffer, line, capacity ); 
  }

  if ( swap_flag == -1 ) {
    swap_flag = gather_sunos_swap( &swap_total, &swap_free ); 
  }

  if ( swap_flag > 0 ) {
    char line[128];
    snprintf( line, sizeof(line),
	      "Swap Usage   MB: %lu total, %lu free\n",
	      megs(swap_total), megs(swap_free) );
    strncat( buffer, line, capacity ); 
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
  unsigned short cpu_count;
  unsigned short cpu_online; 

  unsigned long megahertz;
  char          brand_id[20];
  char          cpu_type[20];
  char          model_name[20];
}; 

static struct SunOSMachineInfo info; 
static int init_flag = -1; 

static
int
initMachine( struct SunOSMachineInfo* p )
{
  unsigned long long pagesize = getpagesize();
  kstat_ctl_t* kc; 
  int result = -1; 

  memset( p, 0, sizeof(struct SunOSMachineInfo) ); 
  /* access kernel statistics API
   * run /usr/sbin/kstat -p to see most of the things available.
   */
  if ( (kc = kstat_open()) != NULL ) {
    kstat_t*       ksp;
    size_t         j;

    // iterate over kernel statistics chain, module by module
    for ( ksp = kc->kc_chain; ksp != NULL; ksp = ksp->ks_next ) {
      if ( strcmp( ksp->ks_module, "cpu_info" ) == 0 ) {
        kstat_read( kc, ksp, NULL );
        /*
         * module == "cpu_info"
         */
        p->cpu_count++;
        for ( j=0; j < ksp->ks_ndata; ++j ) {
          kstat_named_t* knp = ((kstat_named_t*) ksp->ks_data) + j;

          if ( strcmp( knp->name, "state" ) == 0 ) {
            if ( knp->data_type == KSTAT_DATA_CHAR &&
                 strcmp( knp->value.c, "on-line") == 0 ||
                 knp->data_type == KSTAT_DATA_STRING &&
                 strcmp( KSTAT_NAMED_STR_PTR(knp), "on-line" ) == 0 )
              p->cpu_online++;
          } else if ( strcmp( knp->name, "clock_MHz" ) == 0 ) {
            assign( &p->megahertz, sizeof(p->megahertz), knp );
          } else if ( strcmp( knp->name, "brand" ) == 0 ) {
            strncpy( p->brand_id,
                     ( knp->data_type == KSTAT_DATA_STRING ?
                       KSTAT_NAMED_STR_PTR(knp) : knp->value.c ),
                     sizeof(p->brand_id) );
          } else if ( strcmp( knp->name, "cpu_type" ) == 0 ) {
            strncpy( p->cpu_type,
                     ( knp->data_type == KSTAT_DATA_STRING ?
                       KSTAT_NAMED_STR_PTR(knp) : knp->value.c ),
                     sizeof(p->cpu_type) );
          } else if ( strcmp( knp->name, "implementation" ) == 0 ) {
            strncpy( p->model_name,
                     ( knp->data_type == KSTAT_DATA_STRING ?
                       KSTAT_NAMED_STR_PTR(knp) : knp->value.c ),
                     sizeof(p->model_name) );
          }
        } /* for j */
      } /* module == "unix" */
    } /* for */

    kstat_close(kc);
    result = 0; 
  }

  return result; 
}

void
pegasus_cpuinfo( char* buffer, size_t capacity )
{
  if ( init_flag == -1 )
    init_flag = initMachine( &info ); 

  if ( init_flag != -1 ) { 
    char line[1024]; 
    snprintf( line, sizeof(line), 
	      "Processor Info.: %d x %s [%s] @ %ld\n",
	      info.cpu_online, info.model_name, info.cpu_type, 
	      info.megahertz ); 
    strncat( buffer, line, capacity ); 
  }
}
