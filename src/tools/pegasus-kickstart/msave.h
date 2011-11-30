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
#ifndef _MACHINE_H
#define _MACHINE_H

#include <sys/types.h>
#include <sys/utsname.h>
#include <sys/time.h>
#include <unistd.h>

#ifndef SYS_NMLN
#ifdef _SYS_NAMELEN /* DARWIN */
#define SYS_NMLN 65
#else
#error "No SYS_NMLN nor _SYS_NAMELEN: check <sys/utsname.h>"
#endif /* _SYS_NAMELEN */
#endif /* SYS_NMLN */

#ifndef SOLARIS
#include <stdint.h> /* uint64_t */
#endif

typedef struct {
  /* from utsname(2) */
  struct utsname uname; 
  struct timeval now; 

  /* from getpagesize(2) */
  unsigned long pagesize; 

#if defined(_SC_NPROCESSORS_CONF) || defined(DARWIN) || defined(LINUX)
  unsigned short cpu_count; 
  unsigned short cpu_online; 
#endif

#if defined(LINUX) || defined(DARWIN) || defined(_SC_PHYS_PAGES)
  /* from sysinfo(2) or sysctl(3) or sysconf(2) */
  uint64_t  ram_total;
#endif
#if defined(LINUX) || defined(DARWIN) || defined(_SC_AVPHYS_PAGES)
  /* from sysinfo(2) or sysctl(3) or sysconf(2) */
  uint64_t  ram_free;
#endif

#if defined(LINUX) || defined(DARWIN)
  /* from sysinfo(2) or sysctl(3) */
  uint64_t  swap_total;
  uint64_t  swap_free;

  /* from /proc/cpuinfo or sysctl(3) */
  unsigned long  megahertz; 
  char           vendor_id[16]; 
  char           model_name[80]; 

  /* from /proc/uptime or sysctl(3) */
  double         idletime; 
  struct timeval boottime; 
#endif

#if defined(LINUX) || defined(DARWIN) || defined(SUNOS)
  float          load[3]; 
#endif

#ifdef LINUX
  /* from /proc/loadavg */
  unsigned      pid_running; 
  unsigned      pid_total; 
#endif
} MachineInfo; 

extern
void
initMachineInfo( MachineInfo* machine ); 
/* purpose: initialize the data structure. 
 * paramtr: machine (OUT): initialized MachineInfo structure. 
 */

extern
int
printXMLMachineInfo( char* buffer, size_t size, size_t* len, size_t indent,
		     const char* tag, const MachineInfo* machine );
/* purpose: format the job information into the given buffer as XML.
 * paramtr: buffer (IO): area to store the output in
 *          size (IN): capacity of character area
 *          len (IO): current position within area, will be adjusted
 *          indent (IN): indentation level
 *          tag (IN): name to use for element tags.
 *          machine (IN): machine info to print.
 * returns: number of characters put into buffer (buffer length)
 */

extern
void
deleteMachineInfo( MachineInfo* machine );
/* purpose: destructor
 * paramtr: machine (IO): valid MachineInfo structure to destroy. 
 */

#endif /* _MACHINE_H */
