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
#include "machine.h"
#include "debug.h"
#include "tools.h"

#include <sys/types.h>
#include <sys/utsname.h>
#include <ctype.h>
#include <math.h>

#ifdef LINUX
#include <sys/sysinfo.h>
#endif /* LINUX */

#ifdef DARWIN
#include <sys/sysctl.h>
#endif /* DARWIN */

#ifdef SUNOS
#include <sys/loadavg.h>
#endif /* SOLARIS */

#include <string.h>
#include <stdio.h>
#include <time.h>

static const char* RCS_ID =
  "$Id$";

extern int isExtended; /* timestamp format concise or extended */
extern int isLocal;    /* timestamp time zone, UTC or local */

static
size_t
mystrlen( const char* s, size_t max )
{
  /* array version */
  size_t i = 0;
  while ( i < max && s[i] ) ++i;
  return i;
}

static
char*
mytolower( char* s, size_t max )
{
  /* array version */
  size_t i;
  for ( i=0; i < max && s[i]; ++i ) s[i] = tolower(s[i]);
  return s;
}

#ifdef SOLARIS

static
void
gather_sunos_loadavg( MachineInfo* machine )
{
  double load[3]; 
  if ( getloadavg( load, 3 ) != -1 ) {
    int i; 
    for ( i=0; i<3; ++i ) {
      machine->load[i] = load[i]; 
    }
  }
}

#endif /* SOLARIS */

/* -------------------------------------------------------------- */

#ifdef DARWIN

static
void
gather_darwin_cpuinfo( MachineInfo* machine ) 
{
  size_t len; 
  unsigned long freq; 
  char model[128]; 

  len = sizeof(machine->cpu_count); 
  if ( sysctlbyname( "hw.ncpu", &machine->cpu_count, &len, NULL, 0 ) != 0 )
    machine->cpu_count = -1; 

  len = sizeof(machine->cpu_online); 
  if ( sysctlbyname( "hw.availcpu", &machine->cpu_online, &len, NULL, 0 ) != 0 )
    machine->cpu_online = -1; 

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
gather_darwin_uptime( MachineInfo* machine )
{
  size_t len = sizeof(machine->boottime); 
  if ( sysctlbyname( "kern.boottime", &machine->boottime, &len, NULL, 0 ) == -1 )
    machine->idletime = 0.0;
}

static
void
gather_darwin_loadavg( MachineInfo* machine )
{
  struct loadavg l; 
  size_t len = sizeof(l);
  if ( sysctlbyname( "vm.loadavg", &l, &len, NULL, 0 ) == 0 ) {
    int i; 
    for ( i=0; i<3; ++i )
      machine->load[i] = l.ldavg[i] / ((float) l.fscale);
  }
}

static
void
gather_darwin_meminfo( MachineInfo* machine )
{
  struct xsw_usage s; 
  size_t len = sizeof(s); 
  if ( sysctlbyname( "vm.swapusage", &s, &len, NULL, 0 ) == 0 ) {
    machine->swap_total = s.xsu_avail;
    machine->swap_free  = s.xsu_used; 
  }

  len = sizeof(machine->ram_total); 
  if ( sysctlbyname( "hw.memsize", &machine->ram_total, &len, NULL, 0 ) == -1 )
    machine->ram_total = 0; 
}

#endif /* DARWIN */

/* -------------------------------------------------------------- */

#ifdef LINUX

static
void
gather_proc_cpuinfo( MachineInfo* machine )
{
  FILE* f = fopen( "/proc/cpuinfo", "r" );
  if ( f != NULL ) {
    char line[256]; 
    while ( fgets( line, 256, f ) ) {
      if ( *(machine->vendor_id) == 0 && 
	   strncmp( line, "vendor_id", 9 ) == 0 ) {
	char* s = strchr( line, ':' )+1; 
	char* d = machine->vendor_id; 
	while ( *s && isspace(*s) ) ++s; 
	while ( *s && ! isspace(*s) && 
		d - machine->vendor_id < sizeof(machine->vendor_id) ) *d++ = *s++; 
	*d = 0; 
      } else if ( *(machine->model_name) == 0 && 
		  strncmp( line, "model name", 10 ) == 0 ) {
	char* s = strchr( line, ':' )+2; 
	char* d = machine->model_name; 
	while ( *s && d - machine->model_name < sizeof(machine->model_name) ) {
	  while ( *s && ! isspace(*s) ) *d++ = *s++; 
	  if ( *s && *s == ' ' ) *d++ = *s++; 
	  while ( *s && isspace(*s) ) ++s; 
	}
	*d = 0; 
      } else if ( machine->megahertz == 0.0 && 
		  strncmp( line, "cpu MHz", 7 ) == 0 ) {
	char* s = strchr( line, ':' )+2; 
	float mhz; 
	sscanf( s, "%f", &mhz );
	machine->megahertz = (unsigned long) mhz;
      } else if ( strncmp( line, "processor ", 10 ) == 0 ) {
	machine->cpu_count += 1; 
      }
    }
    fclose(f); 
  }
}

static
void
gather_proc_loadavg( MachineInfo* machine )
{
  FILE* f = fopen( "/proc/loadavg", "r" );
  if ( f != NULL ) {
    int maxpid; 
    fscanf( f, "%f %f %f %d/%d %d",
	    &(machine->load[0]), &(machine->load[1]), &(machine->load[2]),
	    &(machine->pid_running), &(machine->pid_total), &maxpid ); 
    fclose(f);
  }
}


static
void
gather_proc_uptime( MachineInfo* machine )
{
  FILE* f = fopen( "/proc/uptime", "r" );
  if ( f != NULL ) {
    double uptime, r, sec; 
    struct timeval tv; 
    now( &tv ); 
    fscanf( f, "%lf %lf", &uptime, &(machine->idletime) ); 
    fclose(f);
    r = ( tv.tv_sec + tv.tv_usec * 1E-6 ) - uptime;
    machine->boottime.tv_sec = sec = (time_t) floor(r);
    machine->boottime.tv_usec = (time_t) floor(1E6 * (r - sec));
  }
}

#endif /* LINUX */

/* -------------------------------------------------------------- */

void
initMachineInfo( MachineInfo* machine )
/* purpose: initialize the data structure. 
 * paramtr: machine (OUT): initialized MachineInfo structure. 
 */
{
#ifdef LINUX
  struct sysinfo si; 
#endif
  uint64_t pagesize; 
  long   ppages; /* some paths may not use it */

  /* extra sanity check */
  if ( machine == NULL ) return ;
  memset( machine, 0, sizeof(MachineInfo) ); 

  now( &machine->now ); 
  if ( uname( &machine->uname ) == -1 ) {
    memset( &machine->uname, 0, sizeof(machine->uname) ); 
  } else {
    /* remove mixed case */
    mytolower( machine->uname.sysname, SYS_NMLN ); 
    mytolower( machine->uname.nodename, SYS_NMLN ); 
    mytolower( machine->uname.machine, SYS_NMLN ); 
  }
  pagesize = machine->pagesize = getpagesize(); 

#ifndef LINUX
#if defined(_SC_PHYS_PAGES)
  if ( (ppages = sysconf(_SC_PHYS_PAGES)) != -1 )
    machine->ram_total = pagesize * ppages; 
#endif
#if defined(_SC_AVPHYS_PAGES)
  if ( (ppages = sysconf(_SC_AVPHYS_PAGES)) != -1 )
    machine->ram_free = pagesize * ppages; 
#endif
#endif 

#if defined(_SC_NPROCESSORS_CONF)
  if ( (ppages = sysconf(_SC_NPROCESSORS_CONF)) != -1 )
    machine->cpu_count = ppages; 
#endif
#if defined(_SC_NPROCESSORS_ONLN)
  if ( (ppages = sysconf(_SC_NPROCESSORS_ONLN)) != -1 )
    machine->cpu_online = ppages; 
#endif


#ifdef DARWIN
  gather_darwin_meminfo( machine ); 
  gather_darwin_uptime( machine ); 
  gather_darwin_loadavg( machine ); 
  gather_darwin_cpuinfo( machine );
#endif /* DARWIN */

#ifdef LINUX
  /* obtain some memory information */
  if ( sysinfo(&si) != -1 ) {
    pagesize = si.mem_unit; 
    machine->ram_total = si.totalram * pagesize;
    machine->ram_free  = si.freeram * pagesize;
    machine->swap_total = si.totalswap * pagesize;
    machine->swap_free  = si.freeswap * pagesize;
  }

  gather_proc_uptime(machine); 
  gather_proc_loadavg(machine); 
  gather_proc_cpuinfo(machine); 
#endif /* LINUX */ 
}

int
printXMLMachineInfo( char* buffer, size_t size, size_t* len, size_t indent,
		     const char* tag, const MachineInfo* machine )
/* purpose: format the job information into the given buffer as XML.
 * paramtr: buffer (IO): area to store the output in
 *          size (IN): capacity of character area
 *          len (IO): current position within area, will be adjusted
 *          indent (IN): indentation level
 *          tag (IN): name to use for element tags.
 *          machine (IN): machine info to print.
 * returns: number of characters put into buffer (buffer length)
 */
{
  /* <machine> open tag */
  myprint( buffer, size, len, "%*s<%s start=\"", indent, "", tag );
  mydatetime( buffer, size, len, isLocal, isExtended,
	      machine->now.tv_sec, machine->now.tv_usec ); 
  append( buffer, size, len, "\">\n" ); 

  /* <uname> */
  myprint( buffer, size, len, "%*s<uname system=\"", indent+2, "" );
  full_append( buffer, size, len, machine->uname.sysname, 
	       mystrlen(machine->uname.sysname,SYS_NMLN) );
  append( buffer, size, len, "\" nodename=\"" );
  full_append( buffer, size, len, machine->uname.nodename, 
	       mystrlen(machine->uname.nodename,SYS_NMLN) );
  append( buffer, size, len, "\" release=\"" );
  full_append( buffer, size, len, machine->uname.release, 
	       mystrlen(machine->uname.release,SYS_NMLN) );
  append( buffer, size, len, "\" machine=\"" );
  full_append( buffer, size, len, machine->uname.machine, 
	       mystrlen(machine->uname.machine,SYS_NMLN) );
  append( buffer, size, len, "\">" );
  full_append( buffer, size, len, machine->uname.version, 
	       mystrlen(machine->uname.version,SYS_NMLN) );
  append( buffer, size, len, "</uname>\n" );

  /* <ram .../> tag */
  myprint( buffer, size, len, 
	   "%*s<ram page-size=\"%lu\" total=\"%lu\"",
	   indent+2, "", 
	   machine->pagesize, machine->ram_total );
  if ( machine->ram_free )
    myprint( buffer, size, len, " free=\"%lu\"", machine->ram_free );
  append( buffer, size, len, "/>\n" ); 

#if defined(LINUX) || defined(DARWIN)
  /* <boot> element */
  myprint( buffer, size, len, "%*s<boot", indent+2, "" );
  if ( machine->idletime > 0.0 ) 
    myprint( buffer, size, len, " idle=\"%.3f\"", machine->idletime );
  append( buffer, size, len, ">" ); 
  mydatetime( buffer, size, len, isLocal, isExtended, 
	      machine->boottime.tv_sec, machine->boottime.tv_usec ); 
  append( buffer, size, len, "</boot>\n" ); 
#endif /* LINUX || DARWIN */

#if defined(LINUX) || defined(DARWIN)
  /* <swap .../> tag */
  myprint( buffer, size, len, 
	   "%*s<swap total=\"%lu\" free=\"%lu\"/>\n",
	   indent+2, "", machine->swap_total, machine->swap_free );
#endif 

#if defined(_SC_NPROCESSORS_CONF) || defined(DARWIN)
  /* <cpu> element */
  myprint( buffer, size, len, 
	   "%*s<cpu count=\"%u\" speed=\"%.0f\" vendor=\"%s\">%s</cpu>\n", 
	   indent+2, "", 
	   machine->cpu_count,
	   machine->megahertz + 0.5,
	   machine->vendor_id, machine->model_name ); 
#endif

#if defined(LINUX) || defined(DARWIN)
  /* <load .../> tag */
  myprint( buffer, size, len, 
	   "%*s<load min1=\"%.2f\" min5=\"%.2f\" min15=\"%.2f\""
#ifdef LINUX
	   " running=\"%u\" total-pid=\"%u\""
#endif
	   "/>\n", 
	   indent+2, "", 
	   machine->load[0], machine->load[1], machine->load[2]
#ifdef LINUX
	   , machine->pid_running, machine->pid_total
#endif
	   ); 
#endif

  /* </machine> close tag */
  myprint( buffer, size, len, "%*s</%s>\n", indent, "", tag ); 
  
  return *len; 
}

void
deleteMachineInfo( MachineInfo* machine )
/* purpose: destructor
 * paramtr: machine (IO): valid MachineInfo structure to destroy. 
 */
{
#ifdef EXTRA_DEBUG
  debugmsg( "# deleteAppInfo(%p)\n", runinfo );
#endif

  machine->pagesize = 0; 
}
