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
#ifdef sun
#include <thread.h>
#include <memory.h>
#endif

#include <string.h>
#include <stdlib.h>
#include <time.h>

#include "error.h"
#include "item.h"
#include "util.h"
#include "workq.h"

static const char* RCS_ID =
"$Id: workq.c,v 1.2 2004/10/22 00:10:47 griphyn Exp $";

int
workq_init( workq_p wq, size_t size, int (*engine)(item_p) )
/* purpose: initialize the workq data structure including starting children.
 * paramtr: wq (IO): pointer to datastructure to be initialized
 *          size (IN): maximum number of threads to start
 *          engine (IN): pointer to user thread function
 * returns: 0 for success, the error in case of failure.
 */
{
  int status;

  /* sanity checks first */
  if ( wq == NULL ) return EINVAL;
  else memset( wq, 0, sizeof(workq_t) );

  /* create detached worker threads */
  if ( (status = pthread_attr_init( &wq->m_attr )) )
    return status;
  status = pthread_attr_setdetachstate( &wq->m_attr, PTHREAD_CREATE_DETACHED );
  if ( status ) {
    pthread_attr_destroy( &wq->m_attr );
    return status;
  }

  if ( size > 1024 ) {
    pthread_attr_destroy( &wq->m_attr );
    return EINVAL;
  } else {
    wq->m_parallel = size;
  }

#ifdef sun
  if ( (status = thr_setconcurrency( size )) ) {
    pthread_attr_destroy( &wq->m_attr );
    return status;
  }
#endif

  /* pthreads init */
  if ( (status = pthread_mutex_init( &wq->m_mutex, NULL )) ) {
    pthread_attr_destroy( &wq->m_attr );
    return status;
  }

  if ( (status = pthread_cond_init( &wq->m_cv, NULL )) ) {
    pthread_mutex_destroy( &wq->m_mutex );
    pthread_attr_destroy( &wq->m_attr );
    return status;
  }

  if ( (status = pthread_cond_init( &wq->m_go, NULL )) ) {
    pthread_cond_destroy( &wq->m_cv );
    pthread_mutex_destroy( &wq->m_mutex );
    pthread_attr_destroy( &wq->m_attr );
    return status;
  }

  /* the rest is auto-memset to zero */
  wq->m_engine = engine;
  wq->m_magic = WORKQ_MAGIC;
  wq->m_limit = 2*size;

  return 0;
}

int 
workq_destroy( workq_p wq )
/* purpose: wait for all to finish and be done with it.
 * paramtr: wq (IO): valid workq data structure.
 * returns: 0 for success, or the error.
 */
{
  int status, s2, s3, s4;

  /* sanity checks first */
  if ( wq == NULL || wq->m_magic != WORKQ_MAGIC ) return EINVAL;
  
  /* get lock to prevent other accesses */
  if ( (status = pthread_mutex_lock( &wq->m_mutex )) )
    return status;

  /* this is it */
  wq->m_magic = -1ul;
  wq->m_quit = 1;

  /* run down any active threads: broadcast wake-up call and wait for all */
  while ( wq->m_count > 0 ) {
    /* broadcast, if idles are lying around */
    if ( wq->m_idle > 0 ) {
      if ( (status = pthread_cond_broadcast( &wq->m_cv )) ) {
	pthread_mutex_unlock( &wq->m_mutex );
	return status;
      }
    }

    /* wait for all others to complete */
    if ( wq->m_count > 0 ) {
      if ( (status = pthread_cond_wait( &wq->m_cv, &wq->m_mutex )) ) {
	pthread_mutex_unlock( &wq->m_mutex );
	return status;
      }
    }
  }

  if ( (status = pthread_mutex_unlock( &wq->m_mutex )) )
    return status;
  status = pthread_mutex_destroy( &wq->m_mutex );
  s2 = pthread_cond_destroy( &wq->m_cv );
  s3 = pthread_attr_destroy( &wq->m_attr );
  s4 = pthread_cond_destroy( &wq->m_go );

  return ( status ? status : ( s2 ? s2 : (s3 ? s3 : s4) ) );
}

static
void*
workq_server( void* arg )
{
  struct timespec timeout;
  workq_p wq = (workq_p) arg;
  item_p item;
  int status, timedout;

  if ( (status = pthread_mutex_lock( &wq->m_mutex )) )
    return NULL;

  /* forever */
  for (;;) {
    /* if there are many threads, allow for more time (host sweating) */
    if ( (timedout = (wq->m_parallel >> 4)) < 5 ) timedout = 5;
    timeout.tv_sec = time(NULL) + timedout;
    timeout.tv_nsec = 0;

    timedout = 0;
    while ( wq->m_head == NULL && ! wq->m_quit ) {
      /* server thread times out after a short while w/o work */
      wq->m_idle++;
      status = pthread_cond_timedwait( &wq->m_cv, &wq->m_mutex, &timeout );
      wq->m_idle--;

      if ( status == ETIMEDOUT ) {
	/* timeout, this is it */
	flockfile( stderr );
	fprintf( stderr, "# worker timed out\n" );
	funlockfile( stderr );
	timedout = 1;
	break;
      } else if ( status != 0 ) {
	/* failure that should not happen */
	flockfile( stderr );
	fprintf( stderr, "worker wait failed: %d: %s\n", 
		 status, strerror(status) );
	funlockfile( stderr );

	wq->m_count--;
	pthread_mutex_unlock( &wq->m_mutex );
	return NULL;
      }
    }

    /* retrieve request from queue */
    item = wq->m_head;
    if ( item != NULL ) {
      double temp0, temp1;

      wq->m_qsize--;
      if ( (wq->m_head = item->m_next) == NULL ) wq->m_tail = NULL;
      if ( (wq->m_head == NULL && wq->m_qsize != 0) ||
	   (wq->m_head != NULL && wq->m_qsize == 0) ) {
	/* small sanity check */
	fprintf( stderr, "mismatch between Q and Q-length\n" );
      }

      if ( (status = pthread_mutex_unlock( &wq->m_mutex )) ) return NULL;
      if ( (status = wq->m_engine(item)) == 0 ) wq->m_success++;
      else wq->m_failure++;

      temp0 = item->m_timesum;
      temp1 = now() - item->m_queued;
      item_destroy(item);
      free((void*) item);
      if ( (status = pthread_mutex_lock( &wq->m_mutex )) ) return NULL;
      wq->m_timesum += temp0;
      wq->m_waitsum += temp1;

      /* we may be able to add another item to the queue */
      if ( wq->m_qsize < wq->m_limit ) pthread_cond_signal( &wq->m_go );
    }

    /* are we done yet */
    if ( wq->m_head == NULL && wq->m_quit ) {
      wq->m_count--;
      if ( wq->m_count == 0 ) pthread_cond_broadcast( &wq->m_cv );
      pthread_mutex_unlock( &wq->m_mutex );
      return NULL;
    }

    /* are we really done */
    if ( wq->m_head == NULL && timedout ) {
      wq->m_count--;
      break;
    }
  } /* forever */

  pthread_mutex_unlock( &wq->m_mutex );
  return NULL;
}

int
workq_add( workq_p wq, xfer_p xfer,
	   unsigned bufsize, unsigned streams, unsigned retries,
	   double initial, double backoff )
{
  int status;
  item_p item;

  /* sanity checks */
  if ( wq == NULL || wq->m_magic != WORKQ_MAGIC ) return EINVAL;

  /* create item */
  if ( (item = (item_p) malloc(sizeof(item_t))) == NULL ) return ENOMEM;
  if ( (status = item_full_init( item, xfer, bufsize, streams, retries,
				 initial, backoff )) ) {
    free((void*) item);
    return status;
  }

  /* get lock */
  if ( (status = pthread_mutex_lock( &wq->m_mutex )) ) {
    item_destroy(item);
    free((void*) item);
    return status;
  }

  /* check the Q length */
  while ( wq->m_qsize >= wq->m_limit ) {
    if ( (status = pthread_cond_wait( &wq->m_go, &wq->m_mutex )) ) {
      item_destroy(item);
      free((void*) item);
      return status;
    }
  }
       
  /* add the request to the queue of work */
  if ( wq->m_head == NULL ) wq->m_head = item;
  else wq->m_tail->m_next = item;
  wq->m_tail = item;
  wq->m_request++;
  wq->m_qsize++;
#if 0
  fprintf( stderr, "# [master] adding item %u to Q\n", wq->m_qsize );
#endif

  if ( wq->m_idle > 0 ) {
    /* wake-up any idle thread */
    if ( (status = pthread_cond_signal( &wq->m_cv )) ) {
      pthread_mutex_unlock( &wq->m_mutex );
      return status;
    }
  } else if ( wq->m_count < wq->m_parallel ) {
    /* add some more threads to work on the task */
    pthread_t id;
    status = pthread_create( &id, &wq->m_attr, workq_server, (void*) wq );
    if ( status != 0 ) {
      pthread_mutex_unlock( &wq->m_mutex );
      return status;
    } else {
      wq->m_threads++;
      wq->m_count++;
    }
  }

  /* done */
  return pthread_mutex_unlock( &wq->m_mutex );
}
