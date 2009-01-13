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
#ifndef _MACHINE_SUNOS_H
#define _MACHINE_SUNOS_H

#include <sys/types.h>
#include <sys/time.h>
#include <sys/utsname.h>
#include <sys/sysinfo.h>
#include "basic.h"

typedef struct {
  /* common (shared) portion */
  MachineBasicInfo* basic; 

  /* 
   * provider-specific portion 
   */

  /* from swapctl(2) system call */
  uint64_t          swap_total; 
  uint64_t          swap_free; 

  /* from proc(4) access */
  unsigned          ps_total;
  unsigned          ps_good; 
  unsigned          ps_thr_active;
  unsigned          ps_thr_zombie; 
  uint64_t          ps_size;
  uint64_t          ps_rss; 

  /* from kstat API cpuinfo:<instance> */
  unsigned short    cpu_count;
  unsigned short    cpu_online; 

  unsigned long     megahertz;
  char              brand_id[20];
  char              cpu_type[20];
  char              model_name[80];

  /* from kstat API cpu_stat:*:*  */
  unsigned long     cpu_state[CPU_STATES];

  /* from kstat API unix:system_pages */
  uint64_t          ram_avail;
  uint64_t          ram_free; 

  /* from kstat API unix:system_misc */
  double            load[3]; 
  time_t            boottime; 
  unsigned          pid_total; 

} MachineSunosInfo; 

extern
void*
initMachine( void ); 
/* purpose: initialize the data structure. 
 * returns: initialized MachineSunosInfo structure. 
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
 *          data (IN): MachineSunosInfo info to print.
 * returns: number of characters put into buffer (buffer length)
 */

extern
void
deleteMachine( void* data );
/* purpose: destructor
 * paramtr: data (IO): valid MachineSunosInfo structure to destroy. 
 */

#endif /* _MACHINE_SUNOS_H */
