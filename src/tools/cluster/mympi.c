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
 */

/*
 * Author : Mei-hui Su
 * Revision : $REVISION$
 */
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <math.h>
#include <sys/types.h>
#include <pthread.h>

#include "mympi.h"


#define  FREE     0
#define  NOT_FREE 1

/* master node is not tracked in the node_list,
   so be careful with off by one thing */

static int *node_list=NULL;
static int numproc;
static int pidx;
static int free_cnt;

pthread_mutex_t       m_mutex;        /* our mutex */
pthread_cond_t        m_cond;           /* the condition variable */

int *init_my_list(int num)
{
   int i;

   numproc=num;
   pidx=0;
   node_list = (int *) malloc( sizeof(int) * numproc);
   for(i=0;i<numproc;i++) node_list[i]=FREE;
   free_cnt=(numproc);
   pthread_mutex_init( &m_mutex, NULL );
   pthread_cond_init( &m_cond, NULL );

   return node_list;
}

void free_my_list()
{
  if(node_list==NULL) return;
  free(node_list);   
  pthread_mutex_destroy( &m_mutex );
  pthread_cond_destroy( &m_cond );
  node_list=NULL;
}

int next_idle_node()
{
  int idx=-1;
  int oidx;

  pthread_mutex_lock(&m_mutex);
  if(free_cnt!=0) {
      while(1) { 
        pidx=(pidx+1) % numproc;
        if(node_list[pidx]==FREE) {
          node_list[pidx]=NOT_FREE;
          free_cnt--;
          idx=pidx;
          break;
        }
      }
  }
  pthread_mutex_unlock(&m_mutex);
  pthread_cond_signal(&m_cond);
  if(idx == -1) return -1;
  oidx=idx+1;
  
  return oidx;
}


void reset_idle_node(int oidx)
{
   int idx=oidx-1;
   pthread_mutex_lock(&m_mutex);
   node_list[idx]=NOT_FREE;
   free_cnt--;
   pthread_mutex_unlock(&m_mutex);
}

void set_idle_node(int oidx)
{
   int idx=oidx-1;
   pthread_mutex_lock(&m_mutex);
   node_list[idx]=FREE;
   free_cnt++;
   pthread_mutex_unlock(&m_mutex);
   pthread_cond_signal(&m_cond);
}

int all_idling()
{
   int ret=0;

   pthread_mutex_lock(&m_mutex);
   if(numproc == free_cnt)
     ret=1;
   pthread_mutex_unlock(&m_mutex);

   return ret;

}
