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
#include "darwin.hh"

static const char* RCS_ID =
  "$Id$";

#include <ctype.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include <sys/param.h>
#include <sys/ucred.h>
#include <sys/mount.h>

#include "basic.hh"

#include <sys/sysctl.h>

#include <mach/mach.h>
#include <mach/host_info.h>
#include <mach/vm_statistics.h>

void
pegasus_statfs( char* buffer, size_t capacity )
{
  struct statfs* mtab; 
  int i, n = getmntinfo( &mtab, MNT_NOWAIT ); // possibly inaccurate
  char line[1024]; 

  if ( n > 1 ) { 
    for ( i=0; i<n; ++i ) { 
      struct statfs* e = mtab + i; 
      if ( mtab[i].f_mntfromname[0] == '/' && mtab[i].f_blocks > 0 ) {
	char total[16], avail[16]; 
	unsigned long long size = mtab[i].f_bsize; 
	smart_units( total, sizeof(total), (size * mtab[i].f_blocks) );
	smart_units( avail, sizeof(avail), (size * mtab[i].f_bavail) );

	snprintf( line, sizeof(line),
		  "Filesystem Info: %-24s %s %s total, %s avail\n",
		  mtab[i].f_mntonname, mtab[i].f_fstypename, 
		  total, avail ); 
	strncat( buffer, line, capacity );
      }
    }
  }
}

void
pegasus_loadavg( char* buffer, size_t capacity )
{
  struct loadavg l; 
  size_t len = sizeof(l);

  if ( sysctlbyname( "vm.loadavg", &l, &len, NULL, 0 ) == 0 ) { 
    float load[3]; 
    for ( int i=0; i<3; ++i ) load[i] = l.ldavg[i] / ((float) l.fscale); 

    char line[128];
    snprintf( line, sizeof(line), 
	      "Load Averages  : %.3f %.3f %.3f\n",
	      load[0], load[1], load[2] ); 
    strncat( buffer, line, capacity ); 
  }
}

void
pegasus_meminfo( char* buffer, size_t capacity )
{
  char line[128]; 
  unsigned long long pagesize = getpagesize(); 
  vm_statistics_data_t vm; 
  mach_msg_type_number_t ic = HOST_VM_INFO_COUNT;
  struct xsw_usage s; 

  unsigned long long ram_total; 
  size_t len = sizeof(ram_total);
  if ( sysctlbyname( "hw.memsize", &ram_total, &len, NULL, 0 ) == 0 ) {
    host_statistics( mach_host_self(), HOST_VM_INFO, (host_info_t) &vm, &ic ); 
    unsigned long ram_avail = megs( pagesize * vm.free_count );
    unsigned long ram_active = megs( pagesize * vm.active_count );
    unsigned long ram_inactive = megs( pagesize * vm.inactive_count );
    unsigned long ram_wired = megs( pagesize * vm.wire_count ); 

    snprintf( line, sizeof(line),
	      "Memory Usage MB: %lu total, %lu avail, %lu active, %lu inactive, %lu wired\n",
	      megs(ram_total), ram_avail, ram_active, ram_inactive, ram_wired ); 
    strncat( buffer, line, capacity ); 
  }

  len = sizeof(s); 
  if ( sysctlbyname( "vm.swapusage", &s, &len, NULL, 0 ) == 0 ) {
    unsigned long swap_total = megs( s.xsu_total );
    unsigned long swap_avail = megs( s.xsu_avail ); 

    snprintf( line, sizeof(line),
	      "Swap Usage   MB: %lu total, %lu free\n", 
	      swap_total, swap_avail ); 
    strncat( buffer, line, capacity ); 
  }
}

void
pegasus_cpuinfo( char* buffer, size_t capacity )
{
  int i;
  size_t len;
  unsigned long freq; 
  char model[128]; 

  unsigned short cpu_online = 0; 
  len = sizeof(i);
  if ( sysctlbyname( "hw.activecpu", &i, &len, NULL, 0 ) == 0 ) 
    cpu_online = i; 

  char cpu_model[80];
  len = sizeof(model);
  if ( sysctlbyname( "machdep.cpu.brand_string", model, &len, NULL, 0 ) == 0 ) {
    char* s = model;
    char* d = cpu_model;
    while ( *s && d - cpu_model < sizeof(cpu_model) ) { 
      while ( *s && ! isspace(*s) ) *d++ = *s++;
      if ( *s && *s == ' ' ) *d++ = *s++;
      while ( *s && isspace(*s) ) ++s;
    }
    *d = 0;
  } else {
    memset( cpu_model, 0, sizeof(cpu_model) ); 
  }

  char line[256];
  snprintf( line, sizeof(line), "Processor Info.: %hu x %s\n",
	    cpu_online, cpu_model ); 
  strncat( buffer, line, capacity ); 
}
