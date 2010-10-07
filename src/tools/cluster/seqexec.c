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
#include <sys/types.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <sys/time.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <unistd.h>

#include "tools.h"
#include "parser.h"
#include "report.h"
#include "mysystem.h"

static const char* RCS_ID =
"$Id$";

extern char *optarg;
extern int optind, opterr, optopt;
int debug = 0;
int progress = -1;
char* application = "seqexec";
static char success[257];

static
void
helpMe( const char* programname, int rc )
/* purpose: write help message and exit
 * paramtr: programname (IN): application of the program (us)
 *           rc (IN): exit code to exit with
 * returns: procedure does not return. */
{
  printf( "%s\nUsage:\t%s [options] [inputfile]\n\n", 
	  RCS_ID, programname );
  printf( "Optional arguments:\n"
" -d\tIncrease debug mode.\n"
" -f\tFail hard on first error (non-zero exit code or signal death), default\n"
"\tis to execute all entries in the input file regardless of their exit.\n"
" -s fn\tProtocol anything to given status file, default stdout.\n"
" -R fn\tRecords progress into the given file, see also SEQEXEC_PROGRESS_REPORT.\n"
" -S ec\tMulti-option: Mark non-zero exit-code ec as success (for -f mode)\n"
" input\tFile with list of applications and args to execute, default stdin.\n" );
  exit(rc);
}

static
void
parseCommandline( int argc, char* argv[], int* fail_hard )
{
  char *s, *ptr = strrchr( argv[0], '/' );
  int option, tmp;

  /* exit code 0 is always good, just in case */
  memset( success, 0, sizeof(success) );
  success[0] = 1;

  if ( ptr == NULL ) ptr = argv[0];
  else ptr++;
  application = ptr;

  /* default progress report location from environment variable */
  if ( (s = getenv("SEQEXEC_PROGRESS_REPORT")) != NULL ) {
    if ( (progress = open( s, O_WRONLY | O_APPEND | O_CREAT, 0666 )) == -1 ) {
      showerr( "%s: open progress %s: %d: %s\n",
	       application, s, errno, strerror(errno) );
    }
  }

  opterr = 0;
  while ( (option = getopt( argc, argv, "R:S:dfhs:" )) != -1 ) {
    switch ( option ) {
    case 'R':
      if ( progress != -1 ) close(progress);
      if ( (progress = open( optarg, O_WRONLY | O_APPEND | O_CREAT, 0666 )) == -1 ) {
	showerr( "%s: open progress %s: %d: %s\n",
		 application, optarg, errno, strerror(errno) );
      }
      break;

    case 'S':
      tmp = atoi(optarg);
      if ( tmp > 0 && tmp < sizeof(success) ) success[tmp] = 1;
      else showerr( "%s: Ignoring unreasonable success code %d\n", application, tmp );
      break;

    case 'd':
      debug++;
      break;

    case 'f':
      (*fail_hard)++;
      break;

    case 's':
      if ( freopen( optarg, "w", stdout ) == NULL ) {
	showerr( "%s: open status %s: %d: %s\n",
		 application, optarg, errno, strerror(errno) );
	exit(2);
      }
      break;

    case 'h':
    case '?':
      helpMe( ptr, 0 );
      break;

    default:
      helpMe( ptr, 1 );
      break;
    }
  }

  if ( (argc - optind) > 1 ) helpMe( ptr, 1 );
  if ( argc != optind ) {
    if ( (freopen( argv[optind], "r", stdin )) == NULL ) {
      showerr( "%s: open input %s: %d: %s\n",
	       application, argv[optind], errno, strerror(errno) );
      exit(3);
    }
  }
}

static
char*
merge( char* s1, char* s2, int use_space )
/* purpose: merge two strings and return the result
 * paramtr: s1 (IN): first string, may be NULL
 *          s2 (IN): second string, must not be null
 *          use_space (IN): flag, if true, separate by space
 * returns: merge of strings into newly allocated area.
 */
{
  if ( s1 == NULL ) {
    return strdup(s2);
  } else {
    size_t l1 = strlen(s1);
    size_t l2 = strlen(s2);
    size_t len = l1 + l2 + 2;
    char* temp = (char*) malloc(len);
    strncpy( temp, s1, len );
    if ( use_space ) strncat( temp, " ", len );
    strncat( temp, s2, len );

    return temp;
  }
}

int
isafailure( int status )
{
  return ( WIFEXITED(status) && success[ WEXITSTATUS(status) ] == 1 ) ? 0 : 1;
}

int
main( int argc, char* argv[], char* envp[] )
{
  size_t len;
  char line[MAXSTR];
  int appc, other, status = 0;
  int fail_hard = 0;
  char* cmd;
  char** appv = NULL;

  char* save = NULL;
  unsigned long total = 0;
  unsigned long failure = 0;
  unsigned long lineno = 0;
  time_t when;
  double diff, start = now(&when);
  parseCommandline( argc, argv, &fail_hard );

  /* progress report finish */
  if ( progress != -1 ) report( progress, time(NULL), 0.0, -1, argv, NULL, NULL );

  /* NEW: unconditionally run a setup job */
  if ( (cmd = getenv("SEQEXEC_SETUP")) != NULL ) { 
#ifndef USE_SYSTEM_SYSTEM
    if ( (appc = interpreteArguments( cmd, &appv )) > 0 ) {
      other = mysystem( appv, envp, "setup" ); 
      if ( other || debug )
	showerr( "%s: setup returned %d/%d\n", /* application */ argv[0],
		 (other >> 8), (other & 127) ); 
      for ( len=0; len<appc; len++ ) free((void*) appv[len]);
      free((void*) appv); 
    } else {
      /* unparsable cleanup argument string */
      showerr( "%s: unparsable setup string, ignoring\n", application ); 
    }
#else
    other = system( cmd ); 
    if ( other || debug )
      showerr( "%s: setup returned %d/%d\n", application,
	       (other >> 8), (other & 127) ); 
#endif /* USE_SYSTEM_SYSTEM */
  }

  /* Read the commands and call each sequentially */
  while ( fgets(line,sizeof(line),stdin) != (char*) NULL ) {
    ++lineno;

    /* check for skippable line */
    if ( line[0] == 0 || /* empty line */
	 line[0] == '\r' || /* CR */
	 line[0] == '\n' || /* LF */
	 line[0] == '#' /* comment */ ) continue;

    /* check for unterminated line (line larger than buffer) */
    len = strlen(line);
    if ( line[len-1] != '\r' && line[len-1] != '\n' ) {
      /* read buffer was too small, save and append */
      char* temp = merge( save, line, 0 );
      if ( save != NULL ) free((void*) save);
      save = temp;

      lineno--;
      fprintf( stderr, "# continuation line %lu\n", lineno );
      continue;
    } else {
      /* remove line termination character(s) */
      do { 
	line[len-1] = 0;
	len--;
      } while ( len > 0 && (line[len-1] == '\r' || line[len-1] == '\n') );
    }
	
    /* assemble command */
    if ( save != NULL ) {
      cmd = merge( save, line, 0 );
      free((void*) save);
      save = NULL;
    } else {
      cmd = line;
    }

    /* and run it */
    if ( (appc = interpreteArguments( cmd, &appv )) > 0 ) {
      total++;
      if ( (status = mysystem( appv, envp, NULL )) ) failure++;
      /* free resource -- we must free argv[] elements */
      for ( len=0; len<appc; len++ ) free((void*) appv[len]);
      free((void*) appv);
    }

    if ( cmd != line ) free((void*) cmd);

    /* fail hard mode, if requested */
    if ( fail_hard && status && isafailure(status) ) break;
  }

  /* NEW: unconditionally run a clean-up job */
  if ( (cmd = getenv("SEQEXEC_CLEANUP")) != NULL ) { 
#ifndef USE_SYSTEM_SYSTEM
    if ( (appc = interpreteArguments( cmd, &appv )) > 0 ) {
      other = mysystem( appv, envp, "cleanup" ); 
      if ( other || debug )
	showerr( "%s: cleanup returned %d/%d\n", /* application */ argv[0],
		 (other >> 8), (other & 127) ); 
      for ( len=0; len<appc; len++ ) free((void*) appv[len]);
      free((void*) appv); 
    } else {
      /* unparsable cleanup argument string */
      showerr( "%s: unparsable cleanup string, ignoring\n", application ); 
    }
#else
    other = system( cmd ); 
    if ( other || debug )
      showerr( "%s: cleanup returned %d/%d\n", application,
	       (other >> 8), (other & 127) ); 
#endif /* USE_SYSTEM_SYSTEM */
  }

  /* provide final statistics */
  diff = now(NULL) - start;
  printf( "[struct stat=\"OK\", lines=%lu, count=%lu, failed=%lu, "
	  "duration=%.3f, start=\"%s\"]\n",
	  lineno, total, failure, diff, isodate(when,line,sizeof(line)) );

  fflush(stdout);
  exit( (fail_hard && status && isafailure(status)) ? 5 : 0 );
}
