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
#include <sys/types.h>
#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <time.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <signal.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <stdlib.h>

#include "appinfo.h"
#include "mysystem.h"
#include "rwio.h"
#include "mylist.h"
#include "invoke.h"

/* truely shared globals */
int isExtended = 1;     /* timestamp format concise or extended */
int isLocal = 1;        /* timestamp time zone, UTC or local */
extern int make_application_executable;
extern size_t data_section_size;

/* module local globals */
static AppInfo appinfo; /* sigh, needs to be global for atexit handler */
static volatile sig_atomic_t global_no_atexit;

static const char* RCS_ID =
"$Id: kickstart.c,v 1.27 2007/01/23 23:55:34 voeckler Exp $";

static
int
obtainStatusCode( int raw ) 
/* purpose: convert the raw result from wait() into a status code
 * paramtr: raw (IN): the raw exit code
 * returns: a cooked exit code */
{
  int result = 127;

  if ( raw < 0 ) {
    /* nothing to do to result */
  } else if ( WIFEXITED(raw) ) {
    result = WEXITSTATUS(raw);
  } else if ( WIFSIGNALED(raw) ) {
    result = 128 + WTERMSIG(raw); 
  } else if ( WIFSTOPPED(raw) ) {
    /* nothing to do to result */
  }

  return result;
}

static
int
prepareSideJob( JobInfo* scripting, const char* value )
/* purpose: prepare a side job from environment string
 * paramtr: scripting (OUT): side job info structure
 *          value (IN): value of the environment setting
 * returns: 0 if there is no job to execute
 *          1 if there is a job to run thru mysystem next
 */
{
  /* no value, no job */
  if ( value == NULL ) return 0;

  /* set-up scripting structure (which is part of the appinfo) */
  initJobInfoFromString( scripting, value );

  /* execute process, if there is any */
  if ( scripting->isValid != 1 ) return 0;

  return 1;
}

StatInfo*
initStatFromList( mylist_p list, size_t* size )
/* purpose: Initialize the statlist and statlist size in appinfo.
 * paramtr: list (IN): list of filenames
 *          size (OUT): statlist size to be set
 * returns: a vector of initialized statinfo records, or NULL
 */
{
  StatInfo* result = NULL;

  if ( (*size = list->count) ) {
    size_t i = 0;
    mylist_item_p item = list->head;

    if ( (result = (StatInfo*) calloc( sizeof(StatInfo), *size )) ) {
      while ( item && i < *size ) {
	initStatInfoFromName( result+i, item->pfn, O_RDONLY, 0 );
	if ( item->lfn != NULL ) addLFNToStatInfo( result+i, item->lfn );
	item = item->next;
	++i;
      }
    }
  }

  return result;
}

#define show( s ) ( s ? s : "(undefined)" )

static
const char*
xlate( const StatInfo* info )
/* purpose: small helper for helpMe() function.
 * paramtr: info (IN): is a record about a file
 * returns: a pointer to the filename, or a local or static buffer w/ info
 * warning: Returns static buffer pointer
 */
{
  static char buffer[16];

  switch ( info->source ) {
  case IS_HANDLE:
    snprintf( buffer, sizeof(buffer), "&%d", info->file.descriptor );
    return buffer;
  case IS_FIFO:
  case IS_TEMP:
  case IS_FILE:
    return show(info->file.name);
  default:
    return "[INVALID]";
  }
}

static
void
helpMe( const AppInfo* run )
/* purpose: print invocation quick help with currently set parameters and
 *          exit with error condition.
 * paramtr: run (IN): constitutes the set of currently set parameters. */
{
  const char* p = strrchr( run->argv[0], '/' );
  if ( p ) ++p;
  else p=run->argv[0];

  fprintf( stderr, "%s\n", RCS_ID );
  fprintf( stderr, 
"Usage:\t%s [-i asi] [-o aso] [-e ase] [-l log] [-n xid] [-N did] \\\n"
"\t[-w|-W cwd] [-R res] [-s [l=]p] [-S [l=]p] [-X] [-H] [-L lbl -T iso] \\\n" 
"\t[-B sz] (-I fn | app [appflags])\n", p );
  fprintf( stderr, 
" -i asi\tConnects stdin of app to asi, default is \"%s\".\n", 
	   xlate(&run->input) );
  fprintf( stderr, 
" -o aso\tConnects stdout of app to aso, default is \"%s\".\n",
	   xlate(&run->output) );
  fprintf( stderr, 
" -e ase\tConnects stderr of app to ase, default is \"%s\".\n", 
	   xlate(&run->error) );
  fprintf( stderr, 
" -l log\tProtocols invocation record into log, default is \"%s\".\n",
	   xlate(&run->logfile) );

  fprintf( stderr, 
" -n xid\tProvides the TR name, default is \"%s\".\n"
" -N did\tProvides the DV name, default is \"%s\".\n" 
" -R res\tReflects the resource handle, default is \"%s\".\n"
" -B sz\tResizes the data section size, default is %u.\n",
	   show(run->xformation), show(run->derivation), 
	   show(run->sitehandle), data_section_size );
  fprintf( stderr,
" -L lbl\tReflects the workflow label, no default.\n"
" -T iso\tReflects the workflow time stamp, no default.\n"
" -H\tOmits the <?xml ...?> header from generated records.\n"
" -I fn\tReads job and args from the file fn, one arg per line.\n"
" -V\tDisplays the version and exit.\n"
" -X\tMakes the application executable, no matter what.\n"
" -w cwd\tSets a different working directory cwd for jobs.\n" 
" -W cwd\tLike -w, but also creates the directory if necessary.\n"
" -S l=p\tProvides filename pairs to stat after start, multi-option.\n"
" \tIf the arg is prefixed with '@', it is a list-of-filenames file.\n"
" -s l=p\tProvides filename pairs to stat before exit, multi-option.\n"
" \tIf the arg is prefixed with '@', it is a list-of-filenames file.\n" );

  /* avoid printing of results in exit handler */
  ((AppInfo*) run)->isPrinted = 1;

  /* exit with error condition */
  exit(127);
}

static
void
finish( void )
{
  if ( ! global_no_atexit ) {
    /* log the output here in case of abnormal termination */
    if ( ! appinfo.isPrinted ) { 
      printAppInfo( &appinfo );
    }
    deleteAppInfo( &appinfo );
  }
}

#ifdef DEBUG_ARGV
static
void
show_args( const char* prefix, char** argv, int argc )
{
  int i;
  fprintf( stderr, "argc=%d\n", argc );
  for ( i=0; i<argc; ++i )
    fprintf( stderr, "%s%2d: %s\n", (prefix ? prefix : ""), i, 
	     (argv[i] ? argv[i] : "(null)" ) );
}
#endif

static
int
readFromFile( const char* fn, char*** argv, int* argc, int* i, int j )
{
  size_t newc = 2;
  size_t index = 0;
  char** newv = calloc( sizeof(char*), newc+1 );
  if ( expand_arg( fn, &newv, &index, &newc, 0 ) == 0 ) {
#if 0
    /* insert newv into argv at position i */
    char** result = calloc( sizeof(char*), j + index + 1 );
    memcpy( result, *argv, sizeof(char*) * j );
    memcpy( result+j, newv, sizeof(char*) * index );
    *argv = result;
    *argc = j + index;
    *i = j-1;
#else
    /* replace argv with newv */
    *argv = newv;
    *argc = index;
    *i = -1;
#endif

#ifdef DEBUG_ARGV
    show_args( "result", *argv, *argc );
#endif

    return 0;
  } else {
    /* error parsing */
    return -1;
  }
}

static
void
handleOutputStream( StatInfo* stream, const char* temp, int std_fileno )
/* purpose: Initialize stdout or stderr from commandline arguments
 * paramtr: stream (IO): pointer to the statinfo record for stdout or stderr
 *          temp (IN): command-line argument
 *          std_fileno (IN): STD(OUT|ERR)_FILENO matching to the stream
 */
{
  if ( temp[0] == '-' && temp[1] == '\0' ) {
    initStatInfoFromHandle( stream, std_fileno );
  } else if ( temp[0] == '!' ) {
    if ( temp[1] == '^' ) {
      initStatInfoFromName( stream, temp+2, O_WRONLY | O_CREAT | O_APPEND, 6 );
    } else {
      initStatInfoFromName( stream, temp+1, O_WRONLY | O_CREAT | O_APPEND, 2 );
    }
  } else if ( temp[0] == '^' ) {
    if ( temp[1] == '!' ) {
      initStatInfoFromName( stream, temp+2, O_WRONLY | O_CREAT | O_APPEND, 6 );
    } else {
      initStatInfoFromName( stream, temp+1, O_WRONLY | O_CREAT, 7 );
    }
  } else {
    initStatInfoFromName( stream, temp, O_WRONLY | O_CREAT, 3 );
  }
}


extern char** environ;

static
int
areWeSane( const char* what )
/* purpose: count the number of occurances of a specific environment variable
 * paramtr: what (IN): environment variable name
 * returns: the count. 
 * warning: Produces a warning on stderr if count==0
 */
{
  size_t len = strlen(what);
  int count = 0;
  char** s = environ;

  while ( s && *s ) {
    if ( strncmp( *s, what, len ) == 0 && (*s)[len] == '=' ) count++;
    ++s;
  }

  if ( ! count ) 
    fprintf( stderr, "Warning! Did not find %s in environment!\n", what );
  return count;
}

static
char*
noquote( char* s )
{
  size_t len;

  /* sanity check */
  if ( ! s ) return NULL;
  else if ( ! *s ) return s;
  else len = strlen(s);

  if ( ( s[0] == '\'' && s[len-1] == '\'' ) || 
       ( s[0] == '"' && s[len-1] == '"' ) ) {
    char* tmp = calloc( sizeof(char), len );
    memcpy( tmp, s+1, len-2 );
    return tmp;
  } else {
    return s;
  }
}

int
main( int argc, char* argv[] )
{
  size_t m, cwd_size = getpagesize();
  int status, result;
  int i, j, keeploop;
  int createDir = 0;
  const char* temp;
  const char* workdir = NULL;
  mylist_t initial;
  mylist_t final;
 
  /* premature init with defaults */
  if ( mylist_init( &initial ) ) return 43;
  if ( mylist_init( &final ) ) return 43;
  initAppInfo( &appinfo, argc, argv );

#if 0
  fprintf( stderr, "# appinfo=%d, jobinfo=%d, statinfo=%d, useinfo=%d\n",
	   sizeof(AppInfo), sizeof(JobInfo), sizeof(StatInfo),
	   sizeof(struct rusage) );
#endif

  /* register emergency exit handler */
  if ( atexit( finish ) == -1 ) {
    appinfo.application.status = -1;
    appinfo.application.saverr = errno;
    fputs( "unable to register an exit handler\n", stderr );
    return 127;
  } else {
    global_no_atexit = 0;
  }

  /* no arguments whatsoever, print help and exit */
  if ( argc == 1 ) helpMe( &appinfo );

  /*
   * read commandline arguments
   * DO NOT use getopt to avoid cluttering flags to the application 
   */
  for ( keeploop=i=1; i < argc && argv[i][0] == '-' && keeploop; ++i ) {
    j = i;
    switch ( argv[i][1] ) {
    case 'B':
      temp = argv[i][2] ? &argv[i][2] : argv[++i];
      m = strtoul( temp, 0, 0 );
      if ( m < 67108863ul ) data_section_size = m;
      break;
#if 0
    case 'c':
      if ( appinfo.channel.source != IS_INVALID )
	deleteStatInfo( &appinfo.channel );
      temp = argv[i][2] ? &argv[i][2] : argv[++i];
      initStatInfoAsFifo( &appinfo.channel, temp, "GRIDSTART_CHANNEL" );
      break;
#endif
    case 'e':
      if ( appinfo.error.source != IS_INVALID )
	deleteStatInfo( &appinfo.error );
      temp = ( argv[i][2] ? &argv[i][2] : argv[++i] );
      handleOutputStream( &appinfo.error, temp, STDERR_FILENO );
      break;
    case 'h':
    case '?':
      helpMe( &appinfo );
      break; /* unreachable */
    case 'V':
      puts( RCS_ID );
      appinfo.isPrinted=1;
      return 0;
    case 'i':
      if ( appinfo.input.source != IS_INVALID )
	deleteStatInfo( &appinfo.input );
      temp = argv[i][2] ? &argv[i][2] : argv[++i];
      if ( temp[0] == '-' && temp[1] == '\0' )
	initStatInfoFromHandle( &appinfo.input, STDIN_FILENO );
      else
	initStatInfoFromName( &appinfo.input, temp, O_RDONLY, 2 );
      break;
    case 'H':
      appinfo.noHeader++;
      break;
    case 'I':
      /* invoke application and args from given file */
      temp = argv[i][2] ? &argv[i][2] : argv[++i];
      if ( readFromFile( temp, &argv, &argc, &i, j ) == -1 ) {
	int saverr = errno;
	fprintf( stderr, "ERROR: While parsing -I %s: %d: %s\n",
		 temp, errno, strerror(saverr) );
	appinfo.application.prefix = strerror(saverr);
	appinfo.application.status = -1;
	return 127;
      }
      keeploop = 0;
      break;
    case 'l':
      if ( appinfo.logfile.source != IS_INVALID )
	deleteStatInfo( &appinfo.logfile );
      temp = argv[i][2] ? &argv[i][2] : argv[++i];
      if ( temp[0] == '-' && temp[1] == '\0' )
	initStatInfoFromHandle( &appinfo.logfile, STDOUT_FILENO );
      else
	initStatInfoFromName( &appinfo.logfile, temp, O_WRONLY | O_CREAT | O_APPEND, 2 );
      break;
    case 'L':
      appinfo.wf_label = noquote( argv[i][2] ? &argv[i][2] : argv[++i] );
      break;
    case 'n':
      appinfo.xformation = noquote( argv[i][2] ? &argv[i][2] : argv[++i] );
      break;
    case 'N':
      appinfo.derivation = noquote( argv[i][2] ? &argv[i][2] : argv[++i] );
      break;
    case 'o':
      if ( appinfo.output.source != IS_INVALID )
	deleteStatInfo( &appinfo.output );
      temp = ( argv[i][2] ? &argv[i][2] : argv[++i] );
      handleOutputStream( &appinfo.output, temp, STDOUT_FILENO );
      break;
    case 'R':
      appinfo.sitehandle = noquote( argv[i][2] ? &argv[i][2] : argv[++i] );
      break;
    case 'S':
      temp = argv[i][2] ? &argv[i][2] : argv[++i];
      if ( temp[0] == '@' ) {
	/* list-of-filenames file */
	if ( (result=mylist_fill( &initial, temp+1 )) )
	  fprintf( stderr, "ERROR: initial %s: %d: %s\n", 
		   temp+1, result, strerror(result) );
      } else {
	/* direct filename */
	if ( (result=mylist_add( &initial, temp )) )
	  fprintf( stderr, "ERROR: initial %s: %d: %s\n", 
		   temp, result, strerror(result) );
      }
      break;
    case 's':
      temp = argv[i][2] ? &argv[i][2] : argv[++i];
      if ( temp[0] == '@' ) {
	/* list-of-filenames file */
	if ( (result=mylist_fill( &final, temp+1 )) )
	  fprintf( stderr, "ERROR: final %s: %d: %s\n", 
		   temp+1, result, strerror(result) );
      } else {
	/* direct filename */
	if ( (result=mylist_add( &final, temp )) )
	  fprintf( stderr, "ERROR: final %s: %d: %s\n", 
		   temp, result, strerror(result) );
      }
      break;
    case 'T':
      appinfo.wf_stamp = noquote( argv[i][2] ? &argv[i][2] : argv[++i] );
      break;
    case 'w':
      workdir = noquote( argv[i][2] ? &argv[i][2] : argv[++i] );
      createDir = 0;
      break;
    case 'W':
      workdir = noquote( argv[i][2] ? &argv[i][2] : argv[++i] );
      createDir = 1;
      break;
    case 'X':
      make_application_executable++;
      break;
    case '-':
      keeploop = 0;
      break;
    default:
      i -= 1;
      keeploop = 0;
      break;
    }
  }

  /* sanity check -- for FNAL */
  areWeSane("GRIDSTART_CHANNEL");

  /* initialize app info and register CLI parameters with it */
  if ( argc-i > 0 ) {
    /* there is an application to run */
    initJobInfo( &appinfo.application, argc-i, argv+i );
    
    /* is there really something to run? */
    if ( appinfo.application.isValid != 1 ) {
      appinfo.application.status = -1;
      appinfo.application.saverr = 2;
      fputs( "The main job specification is invalid or missing\n", stderr );
      return 127;
    }
  } else {
    /* there is not even an application to run */
    helpMe( &appinfo );
  }

  /* make/change into new workdir now */
 REDIR:
  if ( workdir != NULL && chdir(workdir) != 0 ) {
    /* shall we try to make the directory */
    if ( createDir ) {
      createDir = 0; /* once only */

      if ( mkdir( workdir, 0777 ) == 0 ) goto REDIR;
      /* else */
      appinfo.application.saverr = errno;
      fprintf( stderr, "Unable to mkdir %s: %d: %s\n", 
	       workdir, errno, strerror(errno) );
      appinfo.application.prefix = "Unable to mkdir: ";
      appinfo.application.status = -1;
      return 127;
    }

    /* unable to use alternate workdir */
    appinfo.application.saverr = errno;
    fprintf( stderr, "Unable to chdir %s: %d: %s\n", 
	     workdir, errno, strerror(errno) );
    appinfo.application.prefix = "Unable to chdir: ";
    appinfo.application.status = -1;
    return 127;
  }

  /* record the current working directory */
  appinfo.workdir = calloc(cwd_size,sizeof(char));
  if ( getcwd( appinfo.workdir, cwd_size ) == NULL && errno == ERANGE ) {
    /* error allocating sufficient space */
    free((void*) appinfo.workdir );
    appinfo.workdir = NULL;
  }

  /* update stdio and logfile *AFTER* we arrived in working directory */
  updateStatInfo( &appinfo.input );
  updateStatInfo( &appinfo.output );
  updateStatInfo( &appinfo.error );
  updateStatInfo( &appinfo.logfile );

  /* stat pre files */
  appinfo.initial = initStatFromList( &initial, &appinfo.icount );
  mylist_done( &initial );

  /* remember environment that all jobs will see */
  envIntoAppInfo( &appinfo, environ );

  /* Our own initially: an independent setup job */
  if ( prepareSideJob( &appinfo.setup, getenv("GRIDSTART_SETUP") ) )
    mysystem( &appinfo, &appinfo.setup, environ );

  /* possible prae job */
  result = 0;
  if ( prepareSideJob( &appinfo.prejob, getenv("GRIDSTART_PREJOB") ) ) {
    /* there is a prejob to be executed */
    status = mysystem( &appinfo, &appinfo.prejob, environ );
    result = obtainStatusCode(status);
  }

  /* start main application */
  if ( result == 0 ) {
    status = mysystem( &appinfo, &appinfo.application, environ );
    result = obtainStatusCode(status);
  } else {
    /* actively invalidate main record */
    appinfo.application.isValid = 0;
  }

  /* possible post job */
  if ( result == 0 ) {
    if ( prepareSideJob( &appinfo.postjob, getenv("GRIDSTART_POSTJOB") ) ) {
      status = mysystem( &appinfo, &appinfo.postjob, environ );
      result = obtainStatusCode(status);
    }
  }

  /* Java's finally: an independent clean-up job */
  if ( prepareSideJob( &appinfo.cleanup, getenv("GRIDSTART_CLEANUP") ) )
    mysystem( &appinfo, &appinfo.cleanup, environ );

  /* stat post files */
  appinfo.final = initStatFromList( &final, &appinfo.fcount );
  mylist_done( &final );

  /* append results to log file */
  printAppInfo( &appinfo );

  /* clean up and close FDs */
  global_no_atexit = 1; /* disable atexit handler */
  deleteAppInfo( &appinfo );

  /* force NFS sync for gatekeeper */
#if 0
  /* FIXME: No locking on stdout, because printAppInfo will have done so */
  nfs_sync( STDOUT_FILENO, DEFAULT_SYNC_IDLE );
#endif
  nfs_sync( STDERR_FILENO, DEFAULT_SYNC_IDLE );

  /* done */
  return result;
}
