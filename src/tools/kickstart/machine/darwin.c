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
#include "basic.h"
#include "darwin.h"
#include "../tools.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <sys/sysctl.h>

#include <mach/mach.h>
#include <mach/host_info.h>
#include <mach/vm_statistics.h>

static const char* RCS_ID =
  "$Id$";

extern int isExtended; /* timestamp format concise or extended */
extern int isLocal;    /* timestamp time zone, UTC or local */

static
void
gather_darwin_cpuinfo( MachineDarwinInfo* machine ) 
{
  int i; 
  size_t len; 
  unsigned long freq; 
  char model[128]; 

  len = sizeof(i);
  if ( sysctlbyname( "hw.ncpu", &i, &len, NULL, 0 ) == 0 ) 
    machine->cpu_count = i; 

  len = sizeof(i); 
  if ( sysctlbyname( "hw.activecpu", &i, &len, NULL, 0 ) == 0 )
    machine->cpu_online = i; 

  len = sizeof(freq);
  if ( sysctlbyname( "hw.cpufrequency", &freq, &len, NULL, 0 ) == 0 )
    machine->megahertz = freq / 1000000; 

  len = sizeof(machine->vendor_id);
  if ( sysctlbyname( "machdep.cpu.vendor", machine->vendor_id, &len, NULL, 0 ) != 0 ) 
    memset( machine->vendor_id, 0, sizeof(machine->vendor_id) ); 

  len = sizeof(model); 
  if ( sysctlbyname( "machdep.cpu.brand_string", model, &len, NULL, 0 ) == 0 ) {
    char* s = model;
    char* d = machine->model_name; 

    while ( *s && d - machine->model_name < sizeof(machine->model_name) ) {
      while ( *s && ! isspace(*s) ) *d++ = *s++; 
      if ( *s && *s == ' ' ) *d++ = *s++; 
      while ( *s && isspace(*s) ) ++s; 
    }
    *d = 0; 
  } else {
    memset( machine->model_name, 0, sizeof(machine->model_name) ); 
  }
}

static
void
gather_darwin_uptime( MachineDarwinInfo* machine )
{
  size_t len = sizeof(machine->boottime); 
  if ( sysctlbyname( "kern.boottime", &machine->boottime, &len, NULL, 0 ) == -1 )
    memset( &machine->boottime, 0, sizeof(machine->boottime) ); 
}

void
gather_loadavg( float load[3] )
/* purpose: collect load averages
 * primary: provide functionality for monitoring
 * paramtr: load (OUT): array of 3 floats
 */
{
  struct loadavg l; 
  size_t len = sizeof(l);
  if ( sysctlbyname( "vm.loadavg", &l, &len, NULL, 0 ) == 0 ) {
    int i; 
    for ( i=0; i<3; ++i )
      load[i] = l.ldavg[i] / ((float) l.fscale);
  } else {
    load[0] = load[1] = load[2] = 0.0; 
  }
}

static
void
gather_darwin_meminfo( MachineDarwinInfo* machine )
{
  vm_statistics_data_t vm; 
  mach_msg_type_number_t ic = HOST_VM_INFO_COUNT;
  uint64_t pagesize = getpagesize(); 
  struct xsw_usage s; 
  size_t len = sizeof(s); 
  if ( sysctlbyname( "vm.swapusage", &s, &len, NULL, 0 ) == 0 ) {
#if 0
    fprintf( stderr, "# xsu_total %lu\n", s.xsu_total );
    fprintf( stderr, "# xsu_avail %lu\n", s.xsu_avail );
    fprintf( stderr, "# xsu_used %lu\n", s.xsu_used );
    fprintf( stderr, "# xsu_pagesize %u\n", s.xsu_pagesize );
    fprintf( stderr, "# xsu_encrypted %d\n", s.xsu_encrypted );
#endif
    machine->swap_total = s.xsu_total;
    machine->swap_avail = s.xsu_avail;
    machine->swap_used  = s.xsu_used; 
  }

  len = sizeof(machine->ram_total); 
  if ( sysctlbyname( "hw.memsize", &machine->ram_total, &len, NULL, 0 ) == -1 )
    machine->ram_total = 0; 

  host_statistics( mach_host_self(), HOST_VM_INFO, (host_info_t) &vm, &ic ); 
  machine->ram_avail = pagesize * vm.free_count;
  machine->ram_active = pagesize * vm.active_count;
  machine->ram_inactive = pagesize * vm.inactive_count;
  machine->ram_wired = pagesize * vm.wire_count; 
}

static
void
gather_darwin_procstate( unsigned state[MAX_STATE] )
{
  int mib[4]; 
  size_t len; 
  
  mib[0] = CTL_KERN;
  mib[1] = KERN_PROC;
  mib[2] = KERN_PROC_ALL;
  if ( sysctl( mib, 3, NULL, &len, NULL, 0 ) != -1 && len > 0 ) {
    void* buffer = malloc( len + sizeof(struct kinfo_proc) ); 
    if ( sysctl( mib, 3, buffer, &len, NULL, 0 ) != -1 && len > 0 ) {
      struct extern_proc* p; 
      struct kinfo_proc* kp;
      struct kinfo_proc* end = ((struct kinfo_proc*) (((char*) buffer) + len)); 
      for ( kp = (struct kinfo_proc*) buffer; 
	    kp->kp_proc.p_pid && kp < end; 
	    kp++ ) {
	p = &kp->kp_proc;
	state[STATE_TOTAL]++; 

	switch ( p->p_stat ) {
	case SIDL: 
	  state[STATE_IDLE]++;
	  break;
	case SRUN: 
	  /* SRUN is (runnable), not running. Need to dig deeper to find those procs
	   * which are actually running, and those that are just runnable. 
	   */ 
	  state[ (p->p_realtimer.it_interval.tv_sec | 
		  p->p_realtimer.it_interval.tv_usec |
		  p->p_realtimer.it_value.tv_sec | 
		  p->p_realtimer.it_value.tv_usec |
		  p->p_rtime.tv_sec | 
		  p->p_rtime.tv_usec) != 0 ? STATE_RUNNING : STATE_SLEEPING]++; 
	  break; 
	case SSLEEP:
	  state[STATE_WAITING]++; 
	  break;
	case SSTOP:
	  state[STATE_STOPPED]++; 
	  break;
	case SZOMB:
	  state[STATE_ZOMBIE]++;
	  break;
	default:
	  state[STATE_OTHER]++;
	  break;
	}
      }
    }
    free(buffer);
  }
}

void*
initMachine( void )
/* purpose: initialize the data structure. 
 * returns: initialized MachineDarwinInfo structure. 
 */
{
  MachineDarwinInfo* p = (MachineDarwinInfo*) malloc(sizeof(MachineDarwinInfo));

  /* extra sanity check */
  if ( p == NULL ) {
    fputs( "initMachine c'tor failed\n", stderr ); 
    return NULL;
  } else memset( p, 0, sizeof(MachineDarwinInfo) );

  /* name of this provider -- overwritten by importers */
  p->basic = initBasicMachine(); 
  p->basic->provider = "DARWIN"; 
  
  /* gather loadavg */
  gather_darwin_meminfo( p );
  gather_loadavg( p->load ); 
  gather_darwin_uptime( p ); 
  gather_darwin_cpuinfo( p ); 
  gather_darwin_procstate( p->pid_state ); 

  return p;
}

int
printMachine( char* buffer, size_t size, size_t* len, size_t indent,
	      const char* tag, const void* data )
/* purpose: format the information into the given buffer as XML.
 * paramtr: buffer (IO): area to store the output in
 *          size (IN): capacity of character area
 *          len (IO): current position within area, will be adjusted
 *          indent (IN): indentation level
 *          tag (IN): name to use for element tags.
 *          data (IN): MachineDarwinInfo info to print.
 * returns: number of characters put into buffer (buffer length)
 */
{
  static const char* c_state[MAX_STATE] = 
    { "total", "idle", "running", "sleeping", "waiting", "stopped", "zombie", 
      "other" }; 
  char b[3][32];
  const MachineDarwinInfo* ptr = (const MachineDarwinInfo*) data; 
  DarwinState s; 

  /* sanity check */ 
  if ( ptr == NULL ) return *len; 

  /* start basic info */
  startBasicMachine( buffer, size, len, indent+2, tag, ptr->basic ); 

  /* <ram .../> tag */
  myprint( buffer, size, len,
           "%*s<ram total=\"%s\" avail=\"%s\"", 
           indent+2, "", 
	   sizer( b[0], 32, sizeof(ptr->ram_total), &(ptr->ram_total) ),
	   sizer( b[1], 32, sizeof(ptr->ram_avail), &(ptr->ram_avail) ) );
  myprint( buffer, size, len, 
	   " active=\"%s\" inactive=\"%s\" wired=\"%s\"/>\n",
	   sizer( b[0], 32, sizeof(ptr->ram_active), &(ptr->ram_active) ),
	   sizer( b[1], 32, sizeof(ptr->ram_inactive), &(ptr->ram_inactive) ),
	   sizer( b[2], 32, sizeof(ptr->ram_wired), &(ptr->ram_wired) ) ); 

  /* <swap .../> tag */
  myprint( buffer, size, len,
           "%*s<swap total=\"%s\" avail=\"%s\" used=\"%s\"/>\n",
           indent+2, "", 
	   sizer( b[0], 32, sizeof(ptr->swap_total), &(ptr->swap_total) ),
	   sizer( b[1], 32, sizeof(ptr->swap_avail), &(ptr->swap_avail) ),
	   sizer( b[2], 32, sizeof(ptr->swap_used), &(ptr->swap_used) ) );

  /* <boot> element */
  myprint( buffer, size, len, "%*s<boot>", indent+2, "" );
  mydatetime( buffer, size, len, isLocal, isExtended,
              ptr->boottime.tv_sec, ptr->boottime.tv_usec );
  append( buffer, size, len, "</boot>\n" );

  /* <cpu> element */
  myprint( buffer, size, len,
           "%*s<cpu count=\"%s\" speed=\"%s\" vendor=\"%s\">%s</cpu>\n",
           indent+2, "",
           sizer( b[0], 32, sizeof(ptr->cpu_count), &(ptr->cpu_count) ), 
	   sizer( b[1], 32, sizeof(ptr->megahertz), &(ptr->megahertz) ), 
           ptr->vendor_id, ptr->model_name );

  /* loadavg data */
  myprint( buffer, size, len,
	   "%*s<load min1=\"%.2f\" min5=\"%.2f\" min15=\"%.2f\"/>\n",
	   indent+2, "", 
	   ptr->load[0], ptr->load[1], ptr->load[2] );

  /* <proc> element */
  myprint( buffer, size, len, "%*s<proc", indent+2, "" ); 
  for ( s=STATE_TOTAL; s < MAX_STATE; ++s ) {
    if ( ptr->pid_state[s] )
      myprint( buffer, size, len, " %s=\"%u\"", 
	       c_state[s], ptr->pid_state[s] ); 
  }
  append( buffer, size, len, "/>\n" ); 

  /* finish tag */
  finalBasicMachine( buffer, size, len, indent+2, tag, ptr->basic ); 
  
  return *len; 
}

void
deleteMachine( void* data )
/* purpose: destructor
 * paramtr: data (IO): valid MachineDarwinInfo structure to destroy. 
 */
{
  MachineDarwinInfo* ptr = (MachineDarwinInfo*) data; 

#ifdef EXTRA_DEBUG
  fprintf( stderr, "# deleteDarwinMachineInfo(%p)\n", data );
#endif

  if ( ptr ) {
    deleteBasicMachine( ptr->basic );
    free((void*) ptr); 
  }
}
