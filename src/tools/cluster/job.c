/**
 *  Copyright 2007-2010 University Of Southern California
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

#include <sys/types.h>
#include <stdlib.h>
#include <string.h>

#include "job.h"

static const char* RCS_ID =
  "$Id$";

void
job_done( Job* job )
/* purpose: free up the argv vector
 * paramtr: job (IO): job to free and initialize to 0
 * warning: does not touch envp (for now)
 */ 
{
  if ( job ) { 
    int i;
    for ( i=0; i<job->argc; ++i ) {
      if ( job->argv[i] ) { 
	free((void*) job->argv[i]); 
	job->argv[i] = NULL;
      }
    }
    free((void*) job->argv); 
    memset( job, 0, sizeof(Job) ); 
  }
}

int
jobs_init( Jobs* jobs, int cpus )
/* purpose: Initialize maintainance data structure
 * paramtr: jobs (IO): pointer to Jobs data structure
 *          cpus (IN): how many job slots to allocate
 * returns: 0 on success, -1 on error.
 */ 
{
  if ( jobs ) { 
    jobs->cpus = cpus; 
    return ( (jobs->jobs = calloc( sizeof(Job), cpus )) == NULL ) ? -1 : 0;
  } else {
    return -1; 
  }
}

void
jobs_done( Jobs* jobs )
/* purpose: d'tor for Jobs structure
 * paramtr: jobs (IO): pointer to Jobs data structure
 */ 
{
  if ( jobs ) {
    if ( jobs->jobs ) free((void*) jobs->jobs); 
    memset( jobs, 0, sizeof(Jobs) ); 
  }
}

size_t
jobs_in_state( Jobs* jobs, JobState state )
/* purpose: count number of jobs having a certain job state
 * paramtr: jobs (IN): pointer to maintenance structure
 *          state (IN): job state to compare to
 * returns: count
 */ 
{
  size_t i, result = 0; 
  for ( i=0; i < jobs->cpus; ++i ) {
    if ( jobs->jobs[i].state == state ) result++; 
  }
  return result; 
}

size_t
jobs_first_slot( Jobs* jobs, JobState state )
/* purpose: find first slot of a job with state state
 * paramtr: jobs (IN): pointer to maintanance structure
 *          state (IN): job state to search
 * returns: 0 .. cpus-1: valid job slot
 *          cpus: no such slot found
 */ 
{
  size_t result; 
  for ( result=0; result < jobs->cpus; ++result )
    if ( jobs->jobs[result].state == state ) return result;

  return result; /* == jobs->cpus */ 
}
