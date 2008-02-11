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
#include "getif.h"
#include "tools.h"
#include "useinfo.h"
#include "jobinfo.h"
#include <ctype.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdarg.h>

#include <sys/wait.h>
#include <unistd.h>
#include <signal.h>
#include <fcntl.h>

#include "parse.h"

extern int isExtended; /* timestamp format concise or extended */
extern int isLocal;    /* timestamp time zone, UTC or local */

static const char* RCS_ID =
"$Id$";

#ifdef sun
#define sys_siglist _sys_siglist
#endif

#if defined(AIX)
extern const char* const sys_siglist[64];
#endif

#ifndef USE_PARSE
static
size_t
countArguments( const char* cmdline )
/* purpose: count the number of arguments in a commandline
 * warning: any quoting or variable substitution is ignored
 * paramtr: cmdline (IN): string containing the concatenated commandline
 * returns: the number of arguments, 0 for an empty commandline.
 */
{
  size_t result = 0;
  const char* t, *s = cmdline;

  /* sanity check */
  if ( cmdline == NULL || *cmdline == '\0' ) return 0;

  /* skip possible initial whitespace */
  while ( *s && isspace(*s) ) ++s;

  while ( *s ) {
    /* save start position */
    t = s;

    /* advance non whitespace characters */
    while ( *s && ! isspace(*s) ) ++s;

    /* count only full arguments */
    if ( s != t ) result++;

    /* move over whitespace */
    while ( *s && isspace(*s) ) ++s; 
  }
  
  return result;
}
#endif /* ! USE_PARSE */

void
initJobInfoFromString( JobInfo* jobinfo, const char* commandline )
/* purpose: initialize the data structure with default
 * paramtr: jobinfo (OUT): initialized memory block
 *          commandline (IN): commandline concatenated string to separate
 */
{
  size_t i;
  char* t;
#ifdef USE_PARSE
  int state = 0;
  Node* head = parseCommandLine( commandline, &state );
#else
  char* s;
#endif

  /* reset everything */
  memset( jobinfo, 0, sizeof(JobInfo) );

#ifdef USE_PARSE
  /* only continue in ok state AND if there is anything to do */
  if ( state == 32 && head ) {
    size_t size, argc = size = 0;
    Node* temp = head;
    while ( temp ) {
      size += (strlen(temp->data) + 1);
      argc++;
      temp = temp->next;
    }

    /* prepare copy area */
    jobinfo->copy = (char*) malloc( size+argc );

    /* prepare argument vector */
    jobinfo->argc = argc;
    jobinfo->argv = (char* const*) calloc( argc+1, sizeof(char*) );

    /* copy list while updating argument vector and freeing lose arguments */
    t = jobinfo->copy;
    for ( i=0; i < argc && (temp=head); ++i ) {
      /* append string to copy area */
      size_t len = strlen(temp->data)+1;
      memcpy( t, temp->data, len );
      /* I hate nagging compilers which think they know better */
      memcpy( (void*) &jobinfo->argv[i], &t, sizeof(char*) );
      t += len;

      /* clear parse list while we are at it */
      head = temp->next;
      free((void*) temp->data );
      free((void*) temp );
    }
  }
   
  /* free list of (partial) argv */
  if ( head ) deleteNodes(head);

#else
  /* activate copy area */
  jobinfo->copy = strdup( commandline ? commandline : "" );

  /* prepare argv buffer for arguments */
  jobinfo->argc = countArguments(commandline);
  jobinfo->argv = (char* const*) calloc( 1+jobinfo->argc, sizeof(char*) );

  /* copy argument positions into pointer vector */
  for ( i=0, s=jobinfo->copy; *s && i < jobinfo->argc; i++ ) {
    while ( *s && isspace(*s) ) *s++ = '\0';
    t = s;
    while ( *s && ! isspace(*s) ) ++s;
    jobinfo->argv[i] = t; 
  }

  /* remove possible trailing whitespaces */
  while ( *s && isspace(*s) ) *s++ = '\0';
  
  /* finalize vector */
  jobinfo->argv[i] = NULL;
#endif

  /* this is a valid (and initialized) entry */
  if ( jobinfo->argc > 0 ) {
    /* check out path to job */
    char* realpath = findApp( jobinfo->argv[0] );

    if ( realpath ) {
      /* I hate nagging compilers which think they know better */
      memcpy( (void*) &jobinfo->argv[0], &realpath, sizeof(char*) );
      jobinfo->isValid = 1;
    } else {
      jobinfo->status = -127;
      jobinfo->saverr = errno;
      jobinfo->isValid = 2;
    }
    /* initialize some data for myself */
    initStatInfoFromName( &jobinfo->executable, jobinfo->argv[0], O_RDONLY, 0 );
  }
}



void
initJobInfo( JobInfo* jobinfo, int argc, char* const* argv )
/* purpose: initialize the data structure with defaults
 * paramtr: jobinfo (OUT): initialized memory block
 *          argc (IN): adjusted argc string (maybe from main())
 *          argv (IN): adjusted argv string to point to executable
 */
{
#ifdef USE_PARSE
  size_t i;
  char* t;
  int state = 0;
  Node* head = parseArgVector( argc, argv, &state );
#endif

  /* initialize memory */
  memset( jobinfo, 0, sizeof(JobInfo) );

#ifdef USE_PARSE
  /* only continue in ok state AND if there is anything to do */
  if ( state == 32 && head ) {
    size_t size, argc = size = 0;
    Node* temp = head;

    while ( temp ) {
      size += (strlen(temp->data) + 1);
      argc++;
      temp = temp->next;
    }

    /* prepare copy area */
    jobinfo->copy = (char*) malloc( size+argc );

    /* prepare argument vector */
    jobinfo->argc = argc;
    jobinfo->argv = (char* const*) calloc( argc+1, sizeof(char*) );

    /* copy list while updating argument vector and freeing lose arguments */
    t = jobinfo->copy;
    for ( i=0; i < argc && (temp=head); ++i ) {
      /* append string to copy area */
      size_t len = strlen(temp->data)+1;
      memcpy( t, temp->data, len );
      /* I hate nagging compilers which think they know better */
      memcpy( (void*) &jobinfo->argv[i], &t, sizeof(char*) );
      t += len;

      /* clear parse list while we are at it */
      head = temp->next;
      free((void*) temp->data );
      free((void*) temp );
    }
  }
   
  /* free list of (partial) argv */
  if ( head ) deleteNodes(head);

#else
  /* this may require overwriting after CLI parsing */
  jobinfo->argc = argc;
  jobinfo->argv = argv;

#endif 

  /* this is a valid (and initialized) entry */
  if ( jobinfo->argc > 0 ) {
    /* check out path to job */
    char* realpath = findApp( jobinfo->argv[0] );

    if ( realpath ) {
      /* I hate nagging compilers which think they know better */
      memcpy( (void*) &jobinfo->argv[0], &realpath, sizeof(char*) );
      jobinfo->isValid = 1;
    } else {
      jobinfo->status = -127;
      jobinfo->saverr = errno;
      jobinfo->isValid = 2;
    }
      
    /* initialize some data for myself */
    initStatInfoFromName( &jobinfo->executable, jobinfo->argv[0], O_RDONLY, 0 );
  }
}



int
printXMLJobInfo( char* buffer, size_t size, size_t* len, size_t indent,
		 const char* tag, const JobInfo* job )
/* purpose: format the job information into the given buffer as XML.
 * paramtr: buffer (IO): area to store the output in
 *          size (IN): capacity of character area
 *          len (IO): current position within area, will be adjusted
 *          indent (IN): indentation level
 *          tag (IN): name to use for element tags.
 *          job (IN): job info to print.
 * returns: number of characters put into buffer (buffer length) */
{
  int status;	/* $#@! broken Debian headers */

  /* sanity check */
  if ( ! job->isValid ) return *len;

  /* start tag with indentation */
  myprint( buffer, size, len, "%*s<%s start=\"", indent, "", tag );

  /* start time and duration */
  mydatetime( buffer, size, len, isLocal, isExtended,
	      job->start.tv_sec, job->start.tv_usec );
  myprint( buffer, size, len, "\" duration=\"%.3f\"",
	   mymaketime(job->finish) - mymaketime(job->start) );

  /* optional attribute: application process id */
  if ( job->child != 0 )
    myprint( buffer, size, len, " pid=\"%d\"", job->child );

  /* finalize open tag of element */
  append( buffer, size, len, ">\n" );

  /* <usage> */
  printXMLUseInfo( buffer, size, len, indent+2, "usage", &job->use );

  /* <status>: open tag */
  myprint( buffer, size, len, "%*s<status raw=\"%d\">", indent+2, "", 
	   job->status );

  /* <status>: cases of completion */
  status = (int) job->status;	/* $#@! broken Debian headers */
  if ( job->status < 0 ) {
    /* <failure> */
    myprint( buffer, size, len, "<failure error=\"%d\">%s%s</failure>",
	     job->saverr, 
	     job->prefix && job->prefix[0] ? job->prefix : "", 
	     strerror(job->saverr) );
  } else if ( WIFEXITED(status) ) {
    myprint( buffer, size, len, "<regular exitcode=\"%d\"/>", 
	     WEXITSTATUS(status) );
  } else if ( WIFSIGNALED(status) ) {
    /* result = 128 + WTERMSIG(status); */
    myprint( buffer, size, len, "<signalled signal=\"%u\"", 
	     WTERMSIG(status) );
#ifdef WCOREDUMP
    myprint( buffer, size, len, " corefile=\"%s\"", 
	     WCOREDUMP(status) ? "true" : "false" );
#endif
    myprint( buffer, size, len, ">%s</signalled>", 
#if defined(CYGWINNT50) || defined(CYGWINNT51)
	     "unknown"
#else
	     sys_siglist[WTERMSIG(status)]
#endif
	     );
  } else if ( WIFSTOPPED(status) ) {
    myprint( buffer, size, len, "<suspended signal=\"%u\">%s</suspended>",
	     WSTOPSIG(status),
#if defined(CYGWINNT50) || defined(CYGWINNT51)
	     "unknown"
#else
	     sys_siglist[WSTOPSIG(status)]
#endif
	     );
  } /* FIXME: else? */
  append( buffer, size, len, "</status>\n" );

  /* <executable> */
  printXMLStatInfo( buffer, size, len, indent+2, 
		    "statcall", NULL, &job->executable );

#ifdef WITH_NEW_ARGS
  /* alternative 1: new-style <argument-vector> */
  myprint( buffer, size, len, "%*s<argument-vector", indent+2, "" );
  if ( job->argc == 1 ) {
    /* empty element */
    append( buffer, size, len, "/>\n" );
  } else {
    /* content are the CLI args */
    int i=1;

    append( buffer, size, len, ">\n" );
    for ( ; i < job->argc; ++i ) {
      myprint( buffer, size, len, "%*s<arg nr=\"%d\">", indent+4, "", i );
      xmlquote( buffer, size, len, job->argv[i], strlen(job->argv[i]) ); 
      append( buffer, size, len, "</arg>\n" );
    }

    /* end tag */
    myprint( buffer, size, len, "%*s</argument-vector>\n", indent+2, "" );
  }
#else 
  /* alternative 2: old-stlye <arguments> */
  myprint( buffer, size, len, "%*s<arguments", indent+2, "" );
  if ( job->argc == 1 ) {
    /* empty element */
    append( buffer, size, len, "/>\n" );
  } else {
    /* content are the CLI args */
    int i=1;

    append( buffer, size, len, ">" );
    while ( i < job->argc ) {
      xmlquote( buffer, size, len, job->argv[i], strlen(job->argv[i]) );
      if ( ++i < job->argc ) append( buffer, size, len, " " );
    }

    /* end tag */
    append( buffer, size, len, "</arguments>\n" );
  }
#endif /* WITH_NEW_ARGS */
  
  /* finalize close tag of outmost element */
  myprint( buffer, size, len, "%*s</%s>\n", indent, "", tag );

  return *len;
}

void
deleteJobInfo( JobInfo* jobinfo )
/* purpose: destructor
 * paramtr: runinfo (IO): valid AppInfo structure to destroy. */
{
  /* paranoia */
  if ( jobinfo == NULL ) 
    return;

#ifdef EXTRA_DEBUG
  fprintf( stderr, "# deleteJobInfo(%p)\n", jobinfo );
#endif

  if ( jobinfo->isValid ) {
    if ( jobinfo->argv[0] != NULL && jobinfo->argv[0] != jobinfo->copy )
      free((void*) jobinfo->argv[0]); /* from findApp() allocation */
    deleteStatInfo( &jobinfo->executable );
  }

  if ( jobinfo->copy != NULL ) {
    free( (void*) jobinfo->copy );
    free( (void*) jobinfo->argv );
    jobinfo->copy = 0;
  }

  /* final invalidation */
  jobinfo->isValid = 0;
}
