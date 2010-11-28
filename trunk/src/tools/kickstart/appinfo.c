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
#include "rwio.h"
#include "tools.h"
#include "useinfo.h"
#include "machine.h"
#include "jobinfo.h"
#include "statinfo.h"
#include "appinfo.h"
#include "mynss.h"
#include <ctype.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdarg.h>

#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <fcntl.h>
#include <grp.h>
#include <pwd.h>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>

extern int isExtended; /* timestamp format concise or extended */
extern int isLocal;    /* timestamp time zone, UTC or local */

static const char* RCS_ID =
"$Id$";

static
int
mycompare( const void* a, const void* b )
{
  return strcmp( ( a ? *((const char**) a) : "" ), 
		 ( b ? *((const char**) b) : "" ) );
}

static
size_t
convert2XML( char* buffer, size_t size, const AppInfo* run )
{
  size_t i;
  struct passwd* user = wrap_getpwuid( getuid() );
  struct group* group = wrap_getgrgid( getgid() );

  size_t len = 0;
#define XML_SCHEMA_URI "http://pegasus.isi.edu/schema/invocation"
#define XML_SCHEMA_VERSION "2.1"

  /* default is to produce XML preamble */
  if ( ! run->noHeader )
    append( buffer, size, &len, "<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?>\n" );

  /* generate the XML header and start of root element */
  append( buffer, size, &len,
	  "<invocation xmlns=\"" XML_SCHEMA_URI "\""
	  " xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\""
	  " xsi:schemaLocation=\"" XML_SCHEMA_URI
	  " http://pegasus.isi.edu/schema/iv-" XML_SCHEMA_VERSION ".xsd\""
	  " version=\"" XML_SCHEMA_VERSION "\""
	  " start=\"" );

  /* mandatory attributes for root element */
  mydatetime( buffer, size, &len, isLocal, isExtended,
	      run->start.tv_sec, run->start.tv_usec );
  myprint( buffer, size, &len, "\" duration=\"%.3f\"",
	   mymaketime(run->finish) - mymaketime(run->start) );

  /* optional attributes for root element: transformation fqdn */
  if ( run->xformation && strlen(run->xformation) ) {
    append( buffer, size, &len, " transformation=\"" );
    xmlquote( buffer, size, &len, run->xformation, strlen(run->xformation) );
    append( buffer, size, &len, "\"" );
  }

  /* optional attributes for root element: derivation fqdn */
  if ( run->derivation && strlen(run->derivation) ) {
    append( buffer, size, &len, " derivation=\"" );
    xmlquote( buffer, size, &len, run->derivation, strlen(run->derivation) );
    append( buffer, size, &len, "\"" );
  }

  /* optional attributes for root element: name of remote site */
  if ( run->sitehandle && strlen(run->sitehandle) ) {
    append( buffer, size, &len, " resource=\"" );
    xmlquote( buffer, size, &len, run->sitehandle, strlen(run->sitehandle) );
    append( buffer, size, &len, "\"" );
  }

  /* optional attribute for workflow label: name of workflow */
  if ( run->wf_label && strlen(run->wf_label) ) {
    append( buffer, size, &len, " wf-label=\"" );
    xmlquote( buffer, size, &len, run->wf_label, strlen(run->wf_label) );
    append( buffer, size, &len, "\"" );
  }
  if ( run->wf_stamp && strlen(run->wf_stamp) ) {
    append( buffer, size, &len, " wf-stamp=\"" );
    xmlquote( buffer, size, &len, run->wf_stamp, strlen(run->wf_stamp) );
    append( buffer, size, &len, "\"" );
  }

  /* optional attributes for root element: host address dotted quad */
  if ( isdigit( run->ipv4[0] ) ) {
    struct hostent* h;
    in_addr_t address = inet_addr( run->ipv4 );
    myprint( buffer, size, &len, " interface=\"%s\"", run->prif ); 
    myprint( buffer, size, &len, " hostaddr=\"%s\"", run->ipv4 );
    if ( (h = wrap_gethostbyaddr( (const char*) &address, sizeof(in_addr_t), AF_INET )) )
      myprint( buffer, size, &len, " hostname=\"%s\"", h->h_name );
  }

  /* optional attributes for root element: application process id */
  if ( run->child != 0 )
    myprint( buffer, size, &len, " pid=\"%d\"", run->child );

  /* user info about who ran this thing */
  myprint( buffer, size, &len, " uid=\"%d\"", getuid() );
  if ( user ) myprint( buffer, size, &len, " user=\"%s\"", user->pw_name );

  /* group info about who ran this thing */
  myprint( buffer, size, &len, " gid=\"%d\"", getgid() );
  if ( group ) myprint( buffer, size, &len, " group=\"%s\"", group->gr_name );

  /* currently active umask settings */
  myprint( buffer, size, &len, " umask=\"0%03o\"", run->umask );

  /* finalize open tag of root element */
  append( buffer, size, &len, ">\n" );

  /* <setup>, <prejob>, <application>, <postjob>, <cleanup> */
  printXMLJobInfo( buffer, size, &len, 2, "setup", &run->setup );
  printXMLJobInfo( buffer, size, &len, 2, "prejob", &run->prejob );
  printXMLJobInfo( buffer, size, &len, 2, "mainjob", &run->application );
  printXMLJobInfo( buffer, size, &len, 2, "postjob", &run->postjob );
  printXMLJobInfo( buffer, size, &len, 2, "cleanup", &run->cleanup );

  /* <cwd> */
  if ( run->workdir != NULL ) {
    append( buffer, size, &len, "  <cwd>" );
    append( buffer, size, &len, run->workdir );
    append( buffer, size, &len, "</cwd>\n" );
  } else {
#if 0
    append( buffer, size, &len, "  <cwd xmlns:xsi=\"http://www.w3.org/2001/"
	    "XMLSchema-instance\" xsi:nil=\"true\"/>\n" );
#else
    append( buffer, size, &len, "  <cwd/>\n" );
#endif
  }

  /* <usage> own resources */
  printXMLUseInfo( buffer, size, &len, 2, "usage", &run->usage );

  if ( ! run->noHeader )
    printXMLMachineInfo( buffer, size, &len, 2, "machine", &run->machine ); 

  /* <statcall> records */
  printXMLStatInfo( buffer, size, &len, 2, "statcall", "stdin", &run->input );
  updateStatInfo( &(((AppInfo*) run)->output) );
  printXMLStatInfo( buffer, size, &len, 2, "statcall", "stdout", &run->output );
  updateStatInfo( &(((AppInfo*) run)->error) );
  printXMLStatInfo( buffer, size, &len, 2, "statcall", "stderr", &run->error );
  updateStatInfo( &(((AppInfo*) run)->logfile) );
  printXMLStatInfo( buffer, size, &len, 2, "statcall", "gridstart", &run->gridstart );
  printXMLStatInfo( buffer, size, &len, 2, "statcall", "logfile", &run->logfile );
  printXMLStatInfo( buffer, size, &len, 2, "statcall", "channel", &run->channel );

  /* initial and final arbitrary <statcall> records */
  if ( run->icount && run->initial )
    for ( i=0; i<run->icount; ++i )
      printXMLStatInfo( buffer, size, &len, 2, "statcall", "initial", &run->initial[i] );
  if ( run->fcount && run->final )
    for ( i=0; i<run->fcount; ++i )
      printXMLStatInfo( buffer, size, &len, 2, "statcall", "final", &run->final[i] );

  if ( ! run->noHeader ) {

    /* <environment> */
    if ( run->envp && run->envc ) {
      char* s; 

      /* attempt a sorted version */
      char** keys = malloc( sizeof(char*) * run->envc );
      for ( i=0; i < run->envc; ++i ) {
	keys[i] = run->envp[i] ? strdup(run->envp[i]) : "";
      }
      qsort( (void*) keys, run->envc, sizeof(char*), mycompare );

      append( buffer, size, &len, "  <environment>\n" );
      for ( i=0; i < run->envc; ++i ) {
	if ( keys[i] && (s = strchr( keys[i], '=' )) ) {
	  *s = '\0'; /* temporarily cut string here */
	  append( buffer, size, &len, "    <env key=\"" );
	  append( buffer, size, &len, keys[i] );
	  append( buffer, size, &len, "\">" );
	  xmlquote( buffer, size, &len, s+1, strlen(s+1) );
	  append( buffer, size, &len, "</env>\n" );
	  *s = '='; /* reset string to original */
	}
      }
      free((void*) keys);
      append( buffer, size, &len, "  </environment>\n" );
    }

    /* <resource>  limits */
    printXMLLimitInfo( buffer, size, &len, 2, &run->limits );

  } /* ! run->noHeader */

  /* finish root element */
  append( buffer, size, &len, "</invocation>\n" );
  return len;
}

static
char*
pattern( char* buffer, size_t size,
	 const char* dir, const char* sep, const char* file ) 
{
  --size;
  buffer[size] = '\0'; /* reliably terminate string */
  strncpy( buffer, dir, size );
  strncat( buffer, sep, size );
  strncat( buffer, file, size );
  return buffer;
}

void
initAppInfo( AppInfo* appinfo, int argc, char* const* argv )
/* purpose: initialize the data structure with defaults
 * paramtr: appinfo (OUT): initialized memory block
 *          argc (IN): from main()
 *          argv (IN): from main()
 */
{
  size_t tempsize = getpagesize();
  char* tempname = (char*) malloc(tempsize);

  /* find a suitable directory for temporary files */
  const char* tempdir = getTempDir();

  /* reset everything */
  memset( appinfo, 0, sizeof(AppInfo) );

  /* init timestamps with defaults */
  now( &appinfo->start );
  appinfo->finish = appinfo->start;

  /* obtain umask */
  appinfo->umask = umask(0);
  umask(appinfo->umask);

  /* obtain system information */
  initMachineInfo( &appinfo->machine ); 

  /* initialize some data for myself */
  initStatInfoFromName( &appinfo->gridstart, argv[0], O_RDONLY, 0 );

  /* default for stdin */
  initStatInfoFromName( &appinfo->input, "/dev/null", O_RDONLY, 0 );

  /* default for stdout */
#if 1
  pattern( tempname, tempsize, tempdir, "/", "gs.out.XXXXXX" );
  initStatInfoAsTemp( &appinfo->output, tempname );
#else
  initStatInfoFromName( &appinfo->output, "/dev/null", O_WRONLY | O_CREAT, 1 );
#endif

  /* default for stderr */
#if 1
  pattern( tempname, tempsize, tempdir, "/", "gs.err.XXXXXX" );
  initStatInfoAsTemp( &appinfo->error, tempname );
#else
  initStatInfoFromHandle( &appinfo->error, STDERR_FILENO );
#endif

  /* default for stdlog */
  initStatInfoFromHandle( &appinfo->logfile, STDOUT_FILENO );

  /* default for application-level feedback-channel */
  pattern( tempname, tempsize, tempdir, "/", "gs.app.XXXXXX" );
  initStatInfoAsFifo( &appinfo->channel, tempname, "GRIDSTART_CHANNEL" );

  /* free pattern space */
  free((void*) tempname );

  /* original argument vector */
  appinfo->argc = argc;
  appinfo->argv = argv;

  /* where do I run -- guess the primary interface IPv4 dotted quad */
  /* find out where we run at (might stall LATER for some time on DNS) */
  whoami( appinfo->ipv4, sizeof(appinfo->ipv4),
	  appinfo->prif, sizeof(appinfo->prif) );

  /* record resource limits */
  initLimitInfo( &appinfo->limits );

  /* which process is me */
  appinfo->child = getpid();
}

static
size_t
safe_strlen( const char* s )
{
  return ( s == NULL ? 0 : strlen(s) );
}

int
printAppInfo( const AppInfo* run )
/* purpose: output the given app info onto the given fd
 * paramtr: run (IN): is the collective information about the run
 * returns: the number of characters actually written (as of write() call).
 *          if negative, check with errno for the cause of write failure. */
{
  int i, result = -1;
  int fd = run->logfile.source == IS_HANDLE ?
    run->logfile.file.descriptor :
    open( run->logfile.file.name, O_WRONLY | O_APPEND | O_CREAT, 0644 );

  if ( fd != -1 ) {
    int locked;
    size_t wsize, size = getpagesize() << 5; /* initial assumption */
    char* buffer = NULL;

    /* Adjust for final/initial sections */
    if ( run->icount && run->initial )
      for ( i=0; i<run->icount; ++i )
	size += 256 + safe_strlen( run->initial[i].lfn ) +
	  safe_strlen( run->initial[i].file.name );
    if ( run->fcount && run->final )
      for ( i=0; i<run->fcount; ++i )
	size += 256 + safe_strlen( run->final[i].lfn ) +
	  safe_strlen( run->final[i].file.name );

    /* Adjust for <data> sections in stdout and stderr */
    size += ( data_section_size << 1 );

    /* Allocate buffer -- this may fail? */
    buffer = (char*) calloc( size, sizeof(char) );

    /* what about myself? Update stat info on log file */
    updateStatInfo( &((AppInfo*) run)->logfile );

    /* obtain resource usage for xxxx */
#if 0
    struct rusage temp;
    getrusage( RUSAGE_SELF, &temp );
    addUseInfo( (struct rusage*) &run->usage, &temp );
    getrusage( RUSAGE_CHILDREN, &temp );
    addUseInfo( (struct rusage*) &run->usage, &temp );
#else
    getrusage( RUSAGE_SELF, (struct rusage*) &run->usage );
#endif

    /* FIXME: is this true and necessary? */
    updateLimitInfo( (LimitInfo*) &run->limits );

    /* stop the clock */
    now( (struct timeval*) &run->finish );

    wsize = convert2XML( buffer, size, run );
    locked = mytrylock(fd);
    result = writen( fd, buffer, wsize, 3 );
    /* FIXME: what about wsize != result */
    if ( locked==1 ) lockit( fd, F_SETLK, F_UNLCK );

    free( (void*) buffer );

    ((AppInfo*) run)->isPrinted = 1;
    if ( run->logfile.source == IS_FILE ) close(fd);
  }

  return result;
}

void
envIntoAppInfo( AppInfo* runinfo, char* envp[] )
/* purpose: save a deep copy of the current environment
 * paramtr: appinfo (IO): place to store the deep copy
 *          envp (IN): current environment pointer */
{
  /* only do something for an existing environment */
  if ( envp ) {
    char** dst;
    char* const* src = envp;
    size_t size = 0;
    while ( *src++ ) ++size;
    runinfo->envc = size;
    runinfo->envp = (char**) calloc( size+1, sizeof(char*) );

    dst = (char**) runinfo->envp;
    for ( src = envp; dst - runinfo->envp <= size; ++src ) { 
      *dst++ = *src ? strdup(*src) : NULL;
    }
  }
}

void
deleteAppInfo( AppInfo* runinfo )
/* purpose: destructor
 * paramtr: runinfo (IO): valid AppInfo structure to destroy. */
{
  size_t i;

#ifdef EXTRA_DEBUG
  fprintf( stderr, "# deleteAppInfo(%p)\n", runinfo );
#endif

  deleteLimitInfo( &runinfo->limits );

  deleteStatInfo( &runinfo->input );
  deleteStatInfo( &runinfo->output );
  deleteStatInfo( &runinfo->error );
  deleteStatInfo( &runinfo->logfile );
  deleteStatInfo( &runinfo->gridstart );
  deleteStatInfo( &runinfo->channel );

  if ( runinfo->icount && runinfo->initial )
    for ( i=0; i<runinfo->icount; ++i )
      deleteStatInfo( &runinfo->initial[i] );
  if ( runinfo->fcount && runinfo->final )
    for ( i=0; i<runinfo->fcount; ++i )
      deleteStatInfo( &runinfo->final[i] );

  deleteJobInfo( &runinfo->setup );
  deleteJobInfo( &runinfo->prejob );
  deleteJobInfo( &runinfo->application );
  deleteJobInfo( &runinfo->postjob );
  deleteJobInfo( &runinfo->cleanup );

  if ( runinfo->envc && runinfo->envp ) {
    char** p;
    for ( p = (char**) runinfo->envp; *p; p++ ) { 
      if ( *p ) free((void*) *p );
    }

    free((void*) runinfo->envp);
    runinfo->envp = NULL;
    runinfo->envc = 0;
  }

  /* release system information */
  deleteMachineInfo( &runinfo->machine ); 

  memset( runinfo, 0, sizeof(AppInfo) );
}
