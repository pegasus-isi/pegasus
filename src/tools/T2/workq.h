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
#ifndef _WORKQ_H
#define _WORKQ_H

#include <sys/types.h>
#include <pthread.h>
#include <signal.h>
#include "xfer.h"
#include "item.h"

#ifndef DEFAULT_PARALLEL
#define DEFAULT_PARALLEL 4
#endif

typedef struct workq_tag {
  size_t		m_magic;	/* valid */
  pthread_mutex_t	m_mutex;	/* our mutex */
  pthread_cond_t	m_cv;		/* the condition variable */
  pthread_attr_t	m_attr;		/* detachement */

  pthread_cond_t	m_go;		/* prohibit Q overflow */
  item_p		m_head;		/* head of queue */
  item_p		m_tail;		/* tail of queue */
  size_t		m_limit;	/* maximum Q length */
  size_t		m_qsize;	/* current Q length */

  size_t		m_parallel;	/* maximum number of threads */
  size_t		m_count;	/* current number of busy threads */
  size_t		m_idle;		/* current number of idle threads */
  sig_atomic_t		m_quit;		/* termination flag */
  int		(*m_engine)(item_p);	/* user engine */

  /* statistics department */
  size_t		m_threads;	/* total number of threads used */
  size_t		m_request;	/* total number of requests */
  size_t		m_success;	/* number of successful copies */
  size_t		m_failure;	/* number of failed copies */
  double		m_timesum;	/* time of processing */
  double		m_waitsum;	/* time including queue wait */
} workq_t, *workq_p;

#define WORKQ_MAGIC 0xcafebabe

extern
int
workq_init( workq_p wq, size_t size, int (*engine)(item_p) );
/* purpose: initialize the workq data structure without starting children.
 * paramtr: wq (IO): pointer to datastructure to be initialized
 *          size (IN): maximum number of threads to start
 *          engine (IN): user thread handler to call
 * returns: 0 for success, the error in case of failure.
 */

extern
int 
workq_destroy( workq_p wq );
/* purpose: wait for all to finish and be done with it.
 * paramtr: wq (IO): valid workq data structure.
 * returns: 0 for success, or the error.
 */

extern
int
workq_add( workq_p wq, xfer_p section,
	   unsigned bufsize, unsigned streams, unsigned retries,
	   double initial, double backoff );
/* purpose: add a request to the queue for tasking, may block
 * paramtr: wq (IN): is a valid workq data structure
 *          section (IN): description of the transfer request section
 *          bufsize (IN): TCP buffer size
 *          streams (IN): parallel data channels
 *          retries (IN): retry attempts
 *          initial (IN): initial sleep time
 *          backoff (IN): first exponential backoff time
 * returns: 0 for success, or the error.
 */

#endif /* _WORKQ_H */
