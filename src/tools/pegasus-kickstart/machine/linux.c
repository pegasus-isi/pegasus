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
#include "linux.h"
#include "../tools.h"
#include "../debug.h"

#include <ctype.h>
#include <errno.h>
#include <math.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>

#include <signal.h>	/* signal names */

#include <sys/sysinfo.h>

static const char* RCS_ID =
  "$Id$";

extern int isExtended; /* timestamp format concise or extended */
extern int isLocal;    /* timestamp time zone, UTC or local */

static
uint64_t 
unscale( unsigned long value, char scale )
{
  uint64_t result = value; 

  switch ( scale ) {
  case 'B': /* just bytes */
    break;
  case 'k': 
    result <<= 10; 
    break;
  case 'M':
    result <<= 20;
    break;
  case 'G': 
    result <<= 30; 
    break;
  }

  return result; 
}

static
void
parse_status_file( const char* fn, LinuxStatus* status )
{
  char line[256];
  FILE* f;
  if ( (f = fopen( fn, "r" )) ) {
    while ( fgets( line, sizeof(line), f ) ) {
      if ( strncmp( line, "State:", 6 ) == 0 ) {
	char* s = line+7;
	while ( *s && isspace(*s) ) ++s;
	switch ( *s ) {
	case 'R':
	  status->state[S_RUNNING]++; 
	  break;
	case 'S':
	  status->state[S_SLEEPING]++;
	  break;
	case 'D':
	  status->state[S_WAITING]++;
	  break;
	case 'T':
	  status->state[S_STOPPED]++;
	  break;
	case 'Z':
	  status->state[S_ZOMBIE]++;
	  break;
	default: 
	  status->state[S_OTHER]++; 
	  break;
	}
      } else if ( line[0] == 'V' ) {
	unsigned long value;
	char scale[4]; 
	if ( strncmp( line, "VmSize:", 7 ) == 0 ) {
	  char* s = line+8; 
	  while ( *s && isspace(*s) ) ++s; 
	  sscanf( s, "%lu %4s", &value, scale ); 
	  status->size += unscale( value, scale[0] );
	} else if ( strncmp( line, "VmRSS:", 6 ) == 0 ) {
	  char* s = line+7; 
	  while ( *s && isspace(*s) ) ++s; 
	  sscanf( s, "%lu %4s", &value, scale ); 
	  status->rss += unscale( value, scale[0] );
	}
      }
    }
    fclose(f); 
#ifdef DEBUG_PROCFS
  } else {
    fprintf( stderr, "open %s: %s\n", fn, strerror(errno) ); 
#endif
  }
}


#if 0  /*** currently unused ***/

static
void
addtoinfo( LinuxStatus* io, LinuxStatus* summand )
{
  LinuxState i; 

  io->size  += summand->size;
  io->rss   += summand->rss;
  io->total += summand->total;
  for ( i=0; i < MAX_STATE; ++i ) {
    io->state[i] += summand->state[i]; 
  }
}

#endif /*** unused ***/ 

void
gather_linux_proc26( LinuxStatus* procs, LinuxStatus* tasks )
/* purpose: collect proc information on Linux 2.6 kernel
 * paramtr: procs (OUT): aggregation on process level
 *          tasks (OUT): aggregation on task level
 */
{
  struct dirent* dp;
  struct dirent* dt; 
  DIR* taskdir;
  DIR* procdir;

  /* assume procfs is mounted at /proc */
  if ( (procdir=opendir("/proc")) ) {
    char procinfo[128];
    while ( (dp = readdir(procdir)) ) {
      /* real proc files start with digit in 2.6 */
      if ( isdigit(dp->d_name[0]) ) {
	procs->total++; 
	snprintf( procinfo, sizeof(procinfo), "/proc/%s/task", dp->d_name );
	if ( (taskdir=opendir(procinfo)) ) {
	  while ( (dt = readdir(taskdir)) ) {
	    if ( isdigit(dt->d_name[0]) ) {
	      char taskinfo[128];
	      tasks->total++; 
	      snprintf( taskinfo, sizeof(taskinfo), 
			"%s/%s/status", procinfo, dt->d_name ); 
	      parse_status_file( taskinfo, tasks ); 
	    }
	  }
	  closedir(taskdir); 
#ifdef DEBUG_PROCFS
	} else {
	  fprintf( stderr, "opendir %s: %s\n", procinfo, strerror(errno) );
#endif
	}
	snprintf( procinfo, sizeof(procinfo), "/proc/%s/status", dp->d_name ); 
	parse_status_file( procinfo, procs ); 
      }
    }
    closedir(procdir); 
#ifdef DEBUG_PROCFS
  } else {
    perror( "opendir /proc" );
#endif
  }
}

static
void
parse_stat_file( const char* fn, LinuxStatus* proc, LinuxStatus* task )
{
  char line[256];
  FILE* f;
  if ( (f = fopen( fn, "r" )) ) {
    if ( fgets( line, sizeof(line), f ) ) {
      pid_t pid, ppid; 
      char state;
      unsigned long flags, vmsize, text, stack; 
        signed long rss;
	int         exitsignal, notatask = 0; 

      sscanf( line, 
	      "%d %*s %c %d %*d %*d %*d %*d %lu %*u "     /*  1 - 10 */
	      "%*u %*u %*u %*u %*u %*d %*d %*d %*d %*d "  /* 11 - 20 */
	      "%*d %*u %lu %ld %*u %lu %*u %lu %*u %*u "  /* 21 - 30 */
	      "%*u %*u %*u %*u %*u %*u %*u %d %*d %*d "   /* 31 - 40 */
#if 0
	      "%*d %*d %*d %*d %*d"                       /* 41 - 45 */
#endif
	      , &pid, &state, &ppid, &flags
	      , &vmsize, &rss, &text, &stack
	      , &exitsignal
	      /* SIGCHLD == normal process
	       * SIGRTxx == threaded task
	       */
	      ); 
      rss *= getpagesize();
     
      if ( exitsignal == SIGCHLD ) {
	/* regular process */
	notatask = 1;
      } else if ( exitsignal == SIGRTMIN ) {
	/* Do we need to check ancient LinuxThreads, which on 2.0 kernels
	 * were forced to use SIGUSR1 and SIGUSR2 for communication? */
	/* regular thread */
	notatask = 0;
      } else if ( exitsignal == 0 ) {
	if ( text == 0 && stack == 0 ) {
	  /* kernel magic task -- count as process */
	  notatask = 1; 
	} else {
	  /* thread manager task -- count as thread except (init) */
	  notatask = ( ppid == 0 ); 
	}
      }
    
      switch ( state ) {
      case 'R':
	task->state[S_RUNNING]++; 
	if ( notatask ) proc->state[S_RUNNING]++;
	break;
      case 'S':
	task->state[S_SLEEPING]++;
	if ( notatask) proc->state[S_SLEEPING]++;
	break;
      case 'D':
	task->state[S_WAITING]++;
	if ( notatask ) proc->state[S_WAITING]++;
	break;
      case 'T':
	task->state[S_STOPPED]++;
	if ( notatask ) proc->state[S_STOPPED]++;
	break;
      case 'Z':
	task->state[S_ZOMBIE]++;
	if ( notatask ) proc->state[S_ZOMBIE]++;
	break;
      default: 
	task->state[S_OTHER]++; 
	if ( notatask ) proc->state[S_OTHER]++; 
	break;
      }

      task->size += vmsize; 
      if ( notatask ) proc->size += vmsize; 
      task->rss  += rss;
      if ( notatask ) proc->rss += rss; 

      task->total++; 
      if ( notatask ) proc->total++; 
    }
    fclose(f); 
#ifdef DEBUG_PROCFS
  } else {
    fprintf( stderr, "open %s: %s\n", fn, strerror(errno) ); 
#endif
  }
}

void
gather_linux_proc24( LinuxStatus* procs, LinuxStatus* tasks )
/* purpose: collect proc information on Linux 2.4 kernel
 * paramtr: procs (OUT): aggregation on process level
 *          tasks (OUT): aggregation on task level
 * grmblftz Linux uses multiple schemes for threads/tasks. 
 */
{
  struct dirent* dp;
  DIR* procdir;

  /* assume procfs is mounted at /proc */
  if ( (procdir=opendir("/proc")) ) {
    char procinfo[128];
    while ( (dp = readdir(procdir)) ) {
      /* real processes start with digit, tasks *may* start with dot-digit */
      if ( isdigit(dp->d_name[0]) ||
	   ( dp->d_name[0] == '.' && isdigit(dp->d_name[1]) ) ) {
	snprintf( procinfo, sizeof(procinfo), "/proc/%s/stat", dp->d_name );
	parse_stat_file( procinfo, procs, tasks ); 
      }
    }
    closedir(procdir); 

#ifdef DEBUG_PROCFS
  } else {
    perror( "opendir /proc" );
#endif
  }
}

void
gather_loadavg( float load[3] )
/* purpose: collect load averages
 * primary: provide functionality for monitoring
 * paramtr: load (OUT): array of 3 floats
 */
{
  FILE* f = fopen( "/proc/loadavg", "r" );
  if ( f != NULL ) {
    fscanf( f, "%f %f %f", load+0, load+1, load+2 ); 
    fclose(f);
  }
}

void
gather_meminfo( uint64_t* ram_total, uint64_t* ram_free,
		uint64_t* ram_shared, uint64_t* ram_buffer,
		uint64_t* swap_total, uint64_t* swap_free )
/* purpose: collect system-wide memory usage
 * primary: provide functionality for monitoring
 * paramtr: ram_total (OUT): all RAM
 *          ram_free (OUT): free RAM
 *          ram_shared (OUT): unused?
 *          ram_buffer (OUT): RAM used for buffers by kernel
 *          swap_total (OUT): all swap space
 *          swap_free (OUT): free swap space
 */
{
  struct sysinfo si;
  
  /* remaining information */
  if ( sysinfo(&si) != -1 ) {
    uint64_t pagesize = si.mem_unit; 
    *ram_total  = si.totalram * pagesize;
    *ram_free   = si.freeram * pagesize; 
    *ram_shared = si.sharedram * pagesize;
    *ram_buffer = si.bufferram * pagesize; 
    *swap_total = si.totalswap * pagesize;
    *swap_free  = si.freeswap * pagesize;     
  }
}

static
void
gather_proc_uptime( struct timeval* boottime, double* idletime )
{
  FILE* f = fopen( "/proc/uptime", "r" );
  if ( f != NULL ) {
    double uptime, r, sec;
    struct timeval tv;
    now( &tv );
    fscanf( f, "%lf %lf", &uptime, idletime );
    fclose(f);
    r = ( tv.tv_sec + tv.tv_usec * 1E-6 ) - uptime;
    boottime->tv_sec = sec = (time_t) floor(r);
    boottime->tv_usec = (time_t) floor(1E6 * (r - sec));
  }
}

static
void
gather_proc_cpuinfo( MachineLinuxInfo* machine )
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
                d - machine->vendor_id < sizeof(machine->vendor_id) ) 
	  *d++ = *s++;
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
        machine->megahertz = (unsigned long) (mhz + 0.5);
      } else if ( strncmp( line, "processor", 9 ) == 0 ) {
        machine->cpu_count += 1;
      }
    }
    fclose(f);
  }
}

static
unsigned long
extract_version( const char* release ) 
/* purpose: extract a.b.c version from release string, ignoring extra junk
 * paramtr: release (IN): pointer to kernel release string (with junk)
 * returns: integer representation of a version
 *          version := major * 1,000,000 + minor * 1,000 + patch 
 */ 
{
  unsigned major = 0;
  unsigned minor = 0;
  unsigned patch = 0; 
  sscanf( release, "%u.%u.%u", &major, &minor, &patch );
  return major * 1000000ul + minor * 1000 + patch; 
}

/*
 * -------------------------------------------------------------- 
 */

void*
initMachine( void )
/* purpose: initialize the data structure. 
 * returns: initialized MachineLinuxInfo structure. 
 */
{
  unsigned long version;
  MachineLinuxInfo* p = (MachineLinuxInfo*) malloc(sizeof(MachineLinuxInfo));

  /* extra sanity check */
  if ( p == NULL ) {
    fputs( "initMachine c'tor failed\n", stderr ); 
    return NULL;
  } else memset( p, 0, sizeof(MachineLinuxInfo) );

  /* name of this provider -- overwritten by importers */
  p->basic = initBasicMachine(); 
  p->basic->provider = "linux"; 

  gather_meminfo( &p->ram_total, &p->ram_free,
		  &p->ram_shared, &p->ram_buffer, 
		  &p->swap_total, &p->swap_free ); 
  gather_loadavg( p->load ); 
  gather_proc_cpuinfo( p ); 
  gather_proc_uptime( &p->boottime, &p->idletime ); 

  version = extract_version( p->basic->uname.release ); 
  if ( version >= 2006000 && version <= 2006999 ) {
    gather_linux_proc26( &p->procs, &p->tasks ); 
  } else if ( version >= 2004000 && version <= 2004999 ) {
    gather_linux_proc24( &p->procs, &p->tasks ); 
  } else {
    fprintf( stderr, "Info: Kernel v%lu.%lu.%lu is not supported for proc stats gathering\n",
	     version / 1000000, (version % 1000000) / 1000, version % 1000 ); 
  }

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
 *          data (IN): MachineLinuxInfo info to print.
 * returns: number of characters put into buffer (buffer length)
 */
{
  static const char* c_state[MAX_STATE] =
    { "running", "sleeping", "waiting", "stopped", "zombie", "other" }; 
  char b[4][32];
  const MachineLinuxInfo* ptr = (const MachineLinuxInfo*) data; 
  LinuxState s; 

  /* sanity check */ 
  if ( ptr == NULL ) return *len; 

  /* start basic info */
  startBasicMachine( buffer, size, len, indent+2, tag, ptr->basic ); 

  /* <ram .../> tag */
  myprint( buffer, size, len,
           "%*s<ram total=\"%s\" free=\"%s\" shared=\"%s\" buffer=\"%s\"/>\n",
           indent+2, "",
           sizer( b[0], 32, sizeof(ptr->ram_total), &(ptr->ram_total) ),
           sizer( b[1], 32, sizeof(ptr->ram_free), &(ptr->ram_free) ),
           sizer( b[2], 32, sizeof(ptr->ram_total), &(ptr->ram_shared) ),
           sizer( b[3], 32, sizeof(ptr->ram_free), &(ptr->ram_buffer) ) );

  /* <swap .../> tag */
  myprint( buffer, size, len,
           "%*s<swap total=\"%s\" free=\"%s\"/>\n",
           indent+2, "", 
	   sizer( b[0], 32, sizeof(ptr->swap_total), &(ptr->swap_total) ),
	   sizer( b[1], 32, sizeof(ptr->swap_free), &(ptr->swap_free) ) );

  /* <boot> element */
  myprint( buffer, size, len, "%*s<boot idle=\"%.3f\">", 
	   indent+2, "",
	   ptr->idletime ); 
  mydatetime( buffer, size, len, isLocal, isExtended,
              ptr->boottime.tv_sec, ptr->boottime.tv_usec );
  append( buffer, size, len, "</boot>\n" );

  /* <cpu> element */
  myprint( buffer, size, len,
           "%*s<cpu count=\"%hu\" speed=\"%lu\" vendor=\"%s\">%s</cpu>\n",
           indent+2, "",
           ptr->cpu_count, ptr->megahertz, ptr->vendor_id, ptr->model_name );

  /* <load> element */
  myprint( buffer, size, len,
	   "%*s<load min1=\"%.2f\" min5=\"%.2f\" min15=\"%.2f\"/>\n",
	   indent+2, "", 
	   ptr->load[0], ptr->load[1], ptr->load[2] );

  if ( ptr->procs.total && ptr->tasks.total ) {
    /* <proc> element */
    myprint( buffer, size, len, "%*s<proc total=\"%u\"",
	     indent+2, "", ptr->procs.total ); 
    for ( s=S_RUNNING; s<=S_OTHER; ++s ) {
      if ( ptr->procs.state[s] ) 
	myprint( buffer, size, len, " %s=\"%hu\"", 
		 c_state[s], ptr->procs.state[s] );
    }
    myprint( buffer, size, len, " vmsize=\"%s\" rss=\"%s\"/>\n",
	     sizer( b[0], 32, sizeof(ptr->procs.size), &ptr->procs.size ), 
	     sizer( b[1], 32, sizeof(ptr->procs.rss), &ptr->procs.rss ) ); 
    
    /* <task> element */
    myprint( buffer, size, len, "%*s<task total=\"%u\"",
	     indent+2, "", ptr->tasks.total ); 
    for ( s=S_RUNNING; s<=S_OTHER; ++s ) {
      if ( ptr->tasks.state[s] ) 
	myprint( buffer, size, len, " %s=\"%hu\"", 
		 c_state[s], ptr->tasks.state[s] );
    }
#if 0
    /* does not make sense for threads, since they share memory */
    myprint( buffer, size, len, " vmsize=\"%s\" rss=\"%s\"/>\n",
	     sizer( b[0], 32, sizeof(ptr->tasks.size), &ptr->tasks.size ), 
	     sizer( b[1], 32, sizeof(ptr->tasks.rss), &ptr->tasks.rss ) ); 
#else
    append( buffer, size, len, "/>\n" ); 
#endif
  }

  /* finish tag */
  finalBasicMachine( buffer, size, len, indent+2, tag, ptr->basic ); 
  
  return *len; 
}

void
deleteMachine( void* data )
/* purpose: destructor
 * paramtr: data (IO): valid MachineLinuxInfo structure to destroy. 
 */
{
  MachineLinuxInfo* ptr = (MachineLinuxInfo*) data; 

#ifdef EXTRA_DEBUG
  fprintf( stderr, "# deleteLinuxMachineInfo(%p)\n", data );
#endif

  if ( ptr ) {
    deleteBasicMachine( ptr->basic );
    free((void*) ptr); 
  }
}
