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
#include <errno.h>
#ifdef sun
#include <memory.h>
#endif
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include "tools.h"
#include "limitinfo.h"

static const char* RCS_ID =
"$Id: limitinfo.c,v 1.1 2007/01/24 00:03:54 voeckler Exp $";

#ifndef RLIMIT_NLIMITS
#ifdef RLIM_NLIMITS
#define RLIMIT_NLIMITS RLIM_NLIMITS
#endif
#endif

extern
void
initLimitInfo( LimitInfo* limits )
/* purpose: initializes the data structure with current limits
 * paramtr: limits (OUT): initialized memory block
 */
{
#ifdef RLIMIT_NLIMITS
  limits->size = RLIMIT_NLIMITS;
#else
  #error "Need to write a fragment to guesstimate max# of resources"
#endif
  limits->limits = (SingleLimitInfo*) calloc( sizeof(SingleLimitInfo), limits->size );
}

extern
void
updateLimitInfo( LimitInfo* limits )
{ 
  int i;

  if ( limits == NULL || limits->limits == NULL ) return;

  for ( i=0; i<limits->size; ++i ) {
    limits->limits[i].resource = i;
    getrlimit( i, &(limits->limits[i].limit) );
    limits->limits[i].error = errno;
  }      
}

extern
void
deleteLimitInfo( LimitInfo* limits )
/* purpose: destructor
 * paramtr: limits (IO): valid LimitInfo structure to destroy. 
 */
{
#ifdef EXTRA_DEBUG
  fprintf( stderr, "# deleteLimitInfo(%p)\n", limits );
#endif

  if ( limits != NULL ) {
    if ( limits->limits != NULL ) free((void*) limits->limits );
    memset( limits, 0, sizeof(LimitInfo) );
  }
}

static
char*
resource2string( char* buffer, size_t capacity, int resource ) 
/* purpose: converts the resource integer into a string
 * paramtr: resource (IN): resource integer
 * returns: string with name of resource, or NULL if unknown
 */
{
  switch ( resource ) {
#ifdef RLIMIT_CPU
  case RLIMIT_CPU:
    return strncpy( buffer, "RLIMIT_CPU", capacity );
#endif

#ifdef RLIMIT_FSIZE
  case RLIMIT_FSIZE:
    return strncpy( buffer, "RLIMIT_FSIZE", capacity );
#endif

#ifdef RLIMIT_DATA
  case RLIMIT_DATA:
    return strncpy( buffer, "RLIMIT_DATA", capacity );
#endif

#ifdef RLIMIT_STACK
  case RLIMIT_STACK:
    return strncpy( buffer, "RLIMIT_STACK", capacity );
#endif

#ifdef RLIMIT_NOFILE
  case RLIMIT_NOFILE:
    return strncpy( buffer, "RLIMIT_NOFILE", capacity );
#endif

#if defined(RLIMIT_OFILE) && ! defined(RLIMIT_NOFILE)
  case RLIMIT_OFILE:
    return strncpy( buffer, "RLIMIT_OFILE", capacity );
#endif

#ifdef RLIMIT_AS
  case RLIMIT_AS:
    return strncpy( buffer, "RLIMIT_AS", capacity );
#endif

#ifdef RLIMIT_NPROC
  case RLIMIT_NPROC:
    return strncpy( buffer, "RLIMIT_NPROC", capacity );
#endif

#ifdef RLIMIT_LOCKS
  case RLIMIT_LOCKS:
    return strncpy( buffer, "RLIMIT_LOCKS", capacity );
#endif

#ifdef RLIMIT_SIGPENDING
  case RLIMIT_SIGPENDING:
    return strncpy( buffer, "RLIMIT_SIGPENDING", capacity );
#endif

#ifdef RLIMIT_MSGQUEUE
  case RLIMIT_MSGQUEUE:
    return strncpy( buffer, "RLIMIT_MSGQUEUE", capacity );
#endif

#ifdef RLIMIT_NICE
  case RLIMIT_NICE:
    return strncpy( buffer, "RLIMIT_NICE", capacity );
#endif

#ifdef RLIMIT_RTPRIO
  case RLIMIT_RTPRIO:
    return strncpy( buffer, "RLIMIT_RTPRIO", capacity );
#endif

#ifdef RLIMIT_VMEM
#if RLIMIT_AS != RLIMIT_VMEM
  case RLIMIT_VMEM:
    return strncpy( buffer, "RLIMIT_VMEM", capacity );
#endif
#endif

#ifdef RLIMIT_CORE
  case RLIMIT_CORE:
    return strncpy( buffer, "RLIMIT_CORE", capacity );
#endif

#ifdef RLIMIT_MEMLOCK
  case RLIMIT_MEMLOCK:
    return strncpy( buffer, "RLIMIT_MEMLOCK", capacity );
#endif

#ifdef RLIMIT_RSS
#if RLIMIT_AS != RLIMIT_RSS
  case RLIMIT_RSS:
    return strncpy( buffer, "RLIMIT_RSS", capacity );
#endif
#endif

  default:
    snprintf( buffer, capacity, "RESOURCE_%d", resource );
    return buffer;
  }

  /* never reached */
  return NULL;
}

static
char*
value2string( char* buffer, size_t capacity, rlim_t value )
{
  if ( value == RLIM_INFINITY ) strncpy( buffer, "unlimited", capacity );
  else sizer( buffer, capacity, sizeof(rlim_t), &value );
  return buffer;
}

static
int
formatLimit( char* buffer, size_t size, size_t* len, size_t indent, 
	     const SingleLimitInfo* l )
{
  char id[32],value[32];

  if ( l->error != 0 ) 
    return *len;
  if ( resource2string( id, sizeof(id), l->resource ) == NULL ) 
    return *len;

#if 1
  /* Gaurang prefers this one */
  myprint( buffer, size, len, "%*s<soft id=\"%s\">%s</soft>\n",
	   indent, "", id, 
	   value2string(value,sizeof(value),l->limit.rlim_cur) );
    
  myprint( buffer, size, len, "%*s<hard id=\"%s\">%s</hard>\n",
	   indent, "", id,
	   value2string(value,sizeof(value),l->limit.rlim_max) );
#else
  /* I like concise */
  myprint( buffer, size, len, "%*s<limit id=\"%s\" soft=\"%s\"",
	   indent, "", id, 
	   value2string(value,sizeof(value),l->limit.rlim_cur) );
  myprint( buffer, size, len, " hard=\"%s\"/>\n", 
	   value2string(value,sizeof(value),l->limit.rlim_max) );
#endif

  return *len;
}

extern
int
printXMLLimitInfo( char* buffer, size_t size, size_t* len, size_t indent,
		   const LimitInfo* limits )
/* purpose: format the rusage record into the given buffer as XML.
 * paramtr: buffer (IO): area to store the output in
 *          size (IN): capacity of character area
 *          len (IO): current position within area, will be adjusted
 *          indent (IN): indentation level
 *          limits (IN): observed resource limits
 * returns: number of characters put into buffer (buffer length)
 */
{
  int i;

  /* sanity check */
  if ( limits == NULL || limits->limits == NULL ) return *len;

  myprint( buffer, size, len, "%*s<resource>\n", indent, "" );
  for ( i=0; i<limits->size; ++i )
    formatLimit( buffer, size, len, indent+2, &limits->limits[i] );
  myprint( buffer, size, len, "%*s</resource>\n", indent, "" );

  return *len;
}
