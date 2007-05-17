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
#include <memory.h>
#endif

#include <errno.h>
#include <stdlib.h>
#include <string.h>

#include "item.h"
#include "util.h"

static const char* RCS_ID =
"$Id: item.c,v 1.2 2004/10/22 00:10:47 griphyn Exp $";

int
item_init( item_p item, xfer_p xfer )
{
  /* sanity check */
  if ( item == NULL ) return EINVAL;
  else memset( item, 0, sizeof(item_t) );

  /* invisible parameters */
  item->m_next = NULL;
  item->m_magic = ITEM_MAGIC;

  /* settable parameters */
  item->m_xfer = xfer;

  /* defaults */
  item->m_bufsize = DEFAULT_BUFSIZE;
  if ( (item->m_streams = DEFAULT_STREAMS) < 1 ) item->m_streams = 1;
  if ( (item->m_retries = DEFAULT_RETRIES) < 1 ) item->m_retries = 1;
  if ( (item->m_initial = DEFAULT_INITIAL) < 0 ) item->m_initial = 0.0;
  if ( (item->m_backoff = DEFAULT_BACKOFF) < 0 ) item->m_backoff = 0.0;
  
  item->m_queued = now();
  return 0;
}

int
item_full_init( item_p item, xfer_p xfer,
		unsigned bufsize, unsigned streams, unsigned retries,
		double initial, double backoff )
{
  /* sanity check */
  if ( item == NULL ) return EINVAL;
  else memset( item, 0, sizeof(item_t) );

  /* invisible parameters */
  item->m_next = NULL;
  item->m_magic = ITEM_MAGIC;

  /* settable parameters */
  item->m_xfer = xfer;
  item->m_bufsize = bufsize;
  if ( (item->m_streams = streams) < 1 ) item->m_streams = 1;
  if ( (item->m_retries = retries) < 1 ) item->m_retries = 1;
  if ( (item->m_initial = initial) < 0 ) item->m_initial = 0.0;
  if ( (item->m_backoff = backoff) < 0 ) item->m_backoff = 0.0;
  
  item->m_queued = now();
  return 0;
}

int
item_destroy( item_p item )
{
  /* sanity checks */
  if ( item == NULL || item->m_magic != ITEM_MAGIC ) return EINVAL;

  if ( item->m_xfer ) {
    xfer_done( item->m_xfer );
    free((void*) item->m_xfer);
    item->m_xfer = NULL;
  }

  item->m_magic = -1ul;
  return 0;
}
