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
#ifndef _MACHINE_DARWIN_H
#define _MACHINE_DARWIN_H

#include <sys/types.h>
#include <sys/time.h>
#include <sys/utsname.h>
#include <stdint.h>
#include "basic.h"

typedef enum {
  STATE_TOTAL = 0, 
  STATE_IDLE,     /* Darwin SIDL */
  STATE_RUNNING,  /* Darwin SRUN if realtime != 0 */
  STATE_SLEEPING, /* Darwin SRUN if realtime == 0 */ 
  STATE_WAITING,  /* Darwin SSLEEP */ 
  STATE_STOPPED,  /* Darwin SSTOP */ 
  STATE_ZOMBIE,   /* Darwin SZOMB */ 
  STATE_OTHER,    /* future versions */ 
  MAX_STATE 
} DarwinState;

typedef struct {
  /* common (shared) portion */
  MachineBasicInfo* basic; 

  /* 
   * provider-specific portion 
   */

  /* memory statistics */
  uint64_t          ram_total;		/* sysctl hw.memsize */
  uint64_t          ram_avail;		/* mach vm_statistics:free_count */
  uint64_t          ram_active;		/* mach vm_statistics:active_count */
  uint64_t          ram_inactive;	/* mach vm_statistics:inactive_count */
  uint64_t          ram_wired;		/* mach vm_statistics:wire_count */
  uint64_t          swap_total;		/* sysctl vm.swapusage:xsu_total */
  uint64_t          swap_avail;		/* sysctl vm.swapusage:xsu_avail */
  uint64_t          swap_used; 		/* sysctl vm.swapusage:xsu_used */

#if 0
  /* 
   * future lab memory stats -- these are very interesting for monitoring 
   */
  natural_t         reactivate;		/* The number of reactivated pages. */
  natural_t         pageins;            /* The number of requests for pages from a pager */
  natural_t         pageouts;           /* The number of pages that have been paged out. */
  natural_t         faults;             /* The number of times the vm_fault routine has been called. */
  natural_t         cow_fault;          /* The number of copy-on-write faults */
  float             hit_rate;           /* object cache lookup hit rate */ 
#endif

  /* boot time stats */
  struct timeval    boottime;		/* kern.boottime */

  /* cpu information */
  unsigned short    cpu_count;		/* hw.ncpu */
  unsigned short    cpu_online;		/* hw.activecpu */
  unsigned long     megahertz;		/* hw.cpufrequency */
  char              vendor_id[16];	/* machdep.cpu.vendor */
  char              model_name[80];	/* machdep.cpu.brand_string */

  /* system load */
  float             load[3];		/* vm.loadavg */

  /* process count */
  unsigned          pid_state[MAX_STATE];
} MachineDarwinInfo; 

extern
void
gather_loadavg( float load[3] );
/* purpose: collect load averages
 * primary: provide functionality for monitoring
 * paramtr: load (OUT): array of 3 floats
 */

extern
void*
initMachine( void ); 
/* purpose: initialize the data structure. 
 * returns: initialized MachineDarwinInfo structure. 
 */

extern
int
printMachine( char* buffer, size_t size, size_t* len, size_t indent,
	      const char* tag, const void* data );
/* purpose: format the information into the given buffer as XML.
 * paramtr: buffer (IO): area to store the output in
 *          size (IN): capacity of character area
 *          len (IO): current position within area, will be adjusted
 *          indent (IN): indentation level
 *          tag (IN): name to use for element tags.
 *          data (IN): MachineDarwinInfo info to print.
 * returns: number of characters put into buffer (buffer length)
 */

extern
void
deleteMachine( void* data );
/* purpose: destructor
 * paramtr: data (IO): valid MachineDarwinInfo structure to destroy. 
 */

#endif /* _MACHINE_DARWIN_H */
