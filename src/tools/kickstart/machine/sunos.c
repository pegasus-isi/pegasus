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
#include "sunos.h"
#include "sunos-swap.h"
#include "../tools.h"
#include "../debug.h"

#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#include <sys/stat.h>
#if defined(_LP64) || _FILE_OFFSET_BITS != 64
#include <sys/swap.h>
#endif

#include <sys/sysinfo.h>
#include <sys/param.h> /* FSCALE */
#include <kstat.h>

static const char* RCS_ID =
  "$Id$";

extern int isExtended; /* timestamp format concise or extended */
extern int isLocal;    /* timestamp time zone, UTC or local */

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

void*
initMachine( void )
/* purpose: initialize the data structure. 
 * returns: initialized MachineSunosInfo structure. 
 */
{
  uint64_t pagesize = getpagesize(); 
  kstat_ctl_t* kc; /* kernel statistics context handle */
  MachineSunosInfo* p = (MachineSunosInfo*) malloc(sizeof(MachineSunosInfo));

  /* extra sanity check */
  if ( p == NULL ) {
    fputs( "initMachine c'tor failed\n", stderr ); 
    return NULL;
  } else memset( p, 0, sizeof(MachineSunosInfo) );

  /* name of this provider -- overwritten by importers */
  p->basic = initBasicMachine(); 
  p->basic->provider = "SUNOS"; 
  
  gather_sunos_swap( &p->swap_total, &p->swap_free );
  gather_sunos_proc( &p->ps_total, &p->ps_good,
		     &p->ps_thr_active, &p->ps_thr_zombie,
		     &p->ps_size, &p->ps_rss ); 

  /* access kernel statistics API
   * run /usr/sbin/kstat -p to see most of the things available.
   */
  if ( (kc = kstat_open()) != NULL ) {
    kstat_t*       ksp; 
    size_t         j; 

    /* iterate over kernel statistics chain, module by module */
    for ( ksp = kc->kc_chain; ksp != NULL; ksp = ksp->ks_next ) {
      if ( strcmp( ksp->ks_module, "cpu_stat" ) == 0 ) {
	/* 
	 * module == "cpu_stat"
	 */
	cpu_stat_t cpu;
	if ( kstat_read( kc, ksp, &cpu ) != -1 ) {
	  int i; 
	  cpu_sysinfo_t* si = &cpu.cpu_sysinfo; 
	  for ( i=0; i<CPU_STATES; ++i ) 
	    p->cpu_state[i] += si->cpu[i]; 
	}
      } else if ( strcmp( ksp->ks_module, "cpu_info" ) == 0 ) {
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

      } else if ( strcmp( ksp->ks_module, "unix" ) == 0 ) {
	if ( strcmp( ksp->ks_name, "system_misc" ) == 0 ) {
	  double scale = (FSCALE);
	  kstat_read( kc, ksp, NULL ); 
	  /*
	   * module == "unix" && name == "system_misc"
	   */
	  for ( j=0; j < ksp->ks_ndata; ++j ) {
	    kstat_named_t* knp = ((kstat_named_t*) ksp->ks_data) + j;
	    
	    if ( strcmp( knp->name, "avenrun_1min" ) == 0 ) {
	      p->load[0] = knp->value.ui32 / scale;
	    } else if ( strcmp( knp->name, "avenrun_5min" ) == 0 ) {
	      p->load[1] = knp->value.ui32 / scale; 
	    } else if ( strcmp( knp->name, "avenrun_15min" ) == 0 ) {
	      p->load[2] = knp->value.ui32 / scale; 
	    } else if ( strcmp( knp->name, "boot_time" ) == 0 ) {
	      p->boottime = (time_t) knp->value.ui32; 
	    } else if ( strcmp( knp->name, "nproc" ) == 0 ) {
	      p->pid_total = knp->value.ui32; 
	    }
	  } /* for j */
	} else if ( strcmp( ksp->ks_name, "system_pages" ) == 0 ) {
	  /*
	   * module == "unix" && name == "system_pages"
	   */
	  kstat_read( kc, ksp, NULL ); 
	  for ( j=0; j < ksp->ks_ndata; ++j ) {
	    kstat_named_t* knp = ((kstat_named_t*) ksp->ks_data) + j;
	    
	    if ( strcmp( knp->name, "physmem" ) == 0 ) {
	      assign( &p->ram_avail, sizeof(p->ram_avail), knp );
	      p->ram_avail *= pagesize; 
	    } else if ( strcmp( knp->name, "freemem" ) == 0 ) {
	      assign( &p->ram_free, sizeof(p->ram_free), knp ); 
	      p->ram_free *= pagesize; 
	    }
	  }
	}
      } /* module == "unix" */

    } /* for */ 

    kstat_close(kc); 
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
 *          data (IN): MachineSunosInfo info to print.
 * returns: number of characters put into buffer (buffer length)
 */
{
  char b[2][32]; 
  const MachineSunosInfo* ptr = (const MachineSunosInfo*) data; 

  /* sanity check */ 
  if ( ptr == NULL ) return *len; 

  /* start basic info */
  startBasicMachine( buffer, size, len, indent+2, tag, ptr->basic ); 

  /* <ram> element */
  myprint( buffer, size, len, "%*s<ram", indent+2, "" );
#ifdef _SC_PHYS_PAGES
  myprint( buffer, size, len, " total=\"%s\"",
	   sizer( b[0], 32, sizeof(ptr->basic->ram_total),
		  &(ptr->basic->ram_total) ) );
#endif /* _SC_PHYS_PAGES */
  myprint( buffer, size, len, " avail=\"%s\" free=\"%s\"/>\n", 
	   sizer( b[0], 32, sizeof(ptr->ram_avail), &(ptr->ram_avail) ),
	   sizer( b[1], 32, sizeof(ptr->ram_free), &(ptr->ram_free) ) );

  /* <swap> element -- only in 64bit environments */
  myprint( buffer, size, len, "%*s<swap total=\"%s\" free=\"%s\"/>\n",
	   indent+2, "",
	   sizer( b[0], 32, sizeof(ptr->swap_total), &(ptr->swap_total) ),
	   sizer( b[1], 32, sizeof(ptr->swap_free), &(ptr->swap_free) ) );

  /* <boot> element */
  myprint( buffer, size, len, "%*s<boot>", indent+2, "" );
  mydatetime( buffer, size, len, isLocal, isExtended, ptr->boottime, -1 ); 
  append( buffer, size, len, "</boot>\n" );

  /* <cpu> element */
  myprint( buffer, size, len,
           "%*s<cpu count=\"%hu\" online=\"%hu\" speed=\"%lu\""
	   " type=\"%s\" brand=\"%s\">%s</cpu>\n",
           indent+2, "",
           ptr->cpu_count, ptr->cpu_online,
	   ptr->megahertz, 
	   ptr->cpu_type, ptr->brand_id, 
	   ptr->model_name );

  /* load average data */
  myprint( buffer, size, len, 
	   "%*s<load min1=\"%.2f\" min5=\"%.2f\" min15=\"%.2f\"/>\n",
	   indent+2, "", 
	   ptr->load[0], ptr->load[1], ptr->load[2] );

  /* <proc> element */
  myprint( buffer, size, len, 
	   "%*s<proc total=\"%u\" found=\"%u\" size=\"%s\" rss=\"%s\"/>\n", 
	   indent+2, "", ptr->pid_total, ptr->ps_good,
	   sizer( b[0], 32, sizeof(ptr->ps_size), &ptr->ps_size ),
	   sizer( b[1], 32, sizeof(ptr->ps_rss), &ptr->ps_rss ) );

  /* <lwp> element */
  myprint( buffer, size, len, 
	   "%*s<lwp active=\"%u\" zombie=\"%u\"/>\n",
	   indent+2, "", ptr->ps_thr_active, ptr->ps_thr_zombie ); 

  /* finish tag */
  finalBasicMachine( buffer, size, len, indent+2, tag, ptr->basic ); 
  
  return *len; 
}

void
deleteMachine( void* data )
/* purpose: destructor
 * paramtr: data (IO): valid MachineSunosInfo structure to destroy. 
 */
{
  MachineSunosInfo* ptr = (MachineSunosInfo*) data; 

#ifdef EXTRA_DEBUG
  fprintf( stderr, "# deleteSunosMachineInfo(%p)\n", data );
#endif

  if ( ptr ) {
    deleteBasicMachine( ptr->basic );
    free((void*) ptr); 
  }
}
