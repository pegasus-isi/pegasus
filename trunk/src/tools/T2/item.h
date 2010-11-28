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
 *
 * based on examples in David Butenhof, "Programming with POSIX threads",
 * Addison-Wesley, 1997 
 */
#ifndef _ITEM_H
#define _ITEM_H

#ifndef DEFAULT_STREAMS
#define DEFAULT_STREAMS 1
#endif

#ifndef DEFAULT_BUFSIZE
#define DEFAULT_BUFSIZE 0
#endif

#ifndef DEFAULT_RETRIES
#define DEFAULT_RETRIES 5 /* JSV: up'ed from 3 */
#endif

#ifndef DEFAULT_BACKOFF
#define DEFAULT_BACKOFF 5.0
#endif

#ifndef DEFAULT_INITIAL
#define DEFAULT_INITIAL 0.2
#endif

#include "xfer.h"

typedef struct item_tag {
  struct item_tag*	m_next;		/* next piece of work */
  size_t		m_magic; 	/* valid */
  xfer_p		m_xfer;		/* transfer request */
  unsigned		m_bufsize; 	/* TCP buffer size, 0 is default */
  unsigned		m_streams; 	/* number of parallel data streams */
  unsigned		m_retries;	/* number of retry attempts */
  double                m_initial;      /* initial wait */
  double                m_backoff;      /* exponential backoff time */
  
  /* statistics */
  double		m_timesum;	/* time taken for processing */
  double		m_queued;	/* start time of being queued */
} item_t, *item_p;

#define ITEM_MAGIC 0xa7126def

extern
int
item_init( item_p item, xfer_p xfer );
/* purpose: initializes the work item request.
 * paramtr: item (IO): location of an item to initialize
 * paramtr: xfer (IN): description of what to do
 * returns: 0 for ok, error code for an error 
 */

extern
int
item_full_init( item_p item, xfer_p xfer, 
		unsigned bufsize, unsigned streams, unsigned retries,
		double initial, double backoff );
/* purpose: initializes the work item request.
 * paramtr: item (IO): location of an item to initialize
 * paramtr: xfer (IN): description of what to do
 * paramtr: bufsize (IN): TCP buffer size to use for copy
 * paramtr: streams (IN): number of concurrent data channels
 * paramtr: retries (IN): maximum number of retry attempts
 * paramtr: initial (IN): initial sleep time
 * paramtr: backoff (IN): first exponential backoff time
 * returns: 0 for ok, error code for an error 
 */

extern
int
item_destroy( item_p item );
/* purpose: destroys a work item and frees its resources
 * paramtr: item (IO): location of an item to initialize
 * returns: 0 for ok, error code for an error 
 */

#endif /* _ITEM_H */
