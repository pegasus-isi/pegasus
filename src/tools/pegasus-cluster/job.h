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
#ifndef _JOB_H
#define _JOB_H

#include <sys/types.h>

typedef enum {
  EMPTY, 
  RUNNING,
} JobState; 

typedef struct {
  int    argc;   /* number of arguments */
  char** argv;   /* argument vector */
  char** envp;   /* environment settings -- future lab per job */

  JobState state; /* where in the life cycle of a job are we */
  pid_t  child;	  /* pid of process -- when in state running */ 
  double start;   /* starting time */ 
  time_t when;    /* start time_t */
  unsigned long count;   /* copy from job counter */ 
  unsigned long lineno;  /* copy from lineno */ 
} Job;

extern
void
job_done( Job* job ); 
/* purpose: free up the argv vector
 * paramtr: job (IO): job to free and initialize to 0
 * warning: does not touch envp (for now)
 */ 



typedef struct {
  Job*   jobs; 
  size_t cpus; 
} Jobs; 

extern
int
jobs_init( Jobs* jobs, int cpus ); 
/* purpose: Initialize maintainance data structure
 * paramtr: jobs (IO): pointer to Jobs data structure
 *          cpus (IN): how many job slots to allocate
 * returns: 0 on success, -1 on error.
 */ 

extern
void
jobs_done( Jobs* jobs ); 
/* purpose: d'tor for Jobs structure
 * paramtr: jobs (IO): pointer to Jobs data structure
 */ 

extern
size_t 
jobs_in_state( Jobs* jobs, JobState state ); 
/* purpose: count number of jobs having a certain job state
 * paramtr: jobs (IN): pointer to maintenance structure
 *          state (IN): job state to compare to
 * returns: count
 */ 

extern
size_t
jobs_first_slot( Jobs* jobs, JobState state ); 
/* purpose: find first slot of a job with state state
 * paramtr: jobs (IN): pointer to maintanance structure
 *          state (IN): job state to search
 * returns: 0 .. cpus-1: valid job slot
 *          cpus: no such slot found
 */ 

#endif /* _JOB_H */
