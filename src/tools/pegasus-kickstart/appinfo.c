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
#include "debug.h"
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

static
int
mycompare( const void* a, const void* b )
{
  return strcmp( ( a ? *((const char**) a) : "" ), 
                 ( b ? *((const char**) b) : "" ) );
}

static
size_t
convert2XML( FILE *out, const AppInfo* run )
{
  size_t i;
  struct passwd* user = wrap_getpwuid(getuid());
  struct group* group = wrap_getgrgid(getgid());

#define XML_SCHEMA_URI "http://pegasus.isi.edu/schema/invocation"
#define XML_SCHEMA_VERSION "2.2"

  /* default is to produce XML preamble */
  if (!run->noHeader) {
    fprintf(out, "<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?>\n" );
  }

  /* generate the XML header and start of root element */
  fprintf(out, "<invocation xmlns=\"" XML_SCHEMA_URI "\""
          " xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\""
          " xsi:schemaLocation=\"" XML_SCHEMA_URI
          " http://pegasus.isi.edu/schema/iv-" XML_SCHEMA_VERSION ".xsd\""
          " version=\"" XML_SCHEMA_VERSION "\"");

  /* start */
  fprintf(out, " start=\"%s\"",
          fmtisodate(isLocal, isExtended, run->start.tv_sec, 
                     run->start.tv_usec));

  /* duration */
  fprintf(out, " duration=\"%.3f\"",
          mymaketime(run->finish) - mymaketime(run->start));

  /* optional attributes for root element: transformation fqdn */
  if (run->xformation && strlen(run->xformation)) {
    fprintf(out, " transformation=\"");
    xmlquote(out, run->xformation, strlen(run->xformation));
    fprintf(out, "\"");
  }

  /* optional attributes for root element: derivation fqdn */
  if (run->derivation && strlen(run->derivation)) {
    fprintf(out, " derivation=\"");
    xmlquote(out, run->derivation, strlen(run->derivation));
    fprintf(out, "\"");
  }

  /* optional attributes for root element: name of remote site */
  if (run->sitehandle && strlen(run->sitehandle)) {
    fprintf(out, " resource=\"");
    xmlquote(out, run->sitehandle, strlen(run->sitehandle));
    fprintf(out, "\"");
  }

  /* optional attribute for workflow label: name of workflow */
  if (run->wf_label && strlen(run->wf_label)) {
    fprintf(out, " wf-label=\"");
    xmlquote(out, run->wf_label, strlen(run->wf_label));
    fprintf(out, "\"");
  }
  if (run->wf_stamp && strlen(run->wf_stamp)) {
    fprintf(out, " wf-stamp=\"");
    xmlquote(out, run->wf_stamp, strlen(run->wf_stamp));
    fprintf(out, "\"");
  }

  /* optional attributes for root element: host address dotted quad */
  if (isdigit(run->ipv4[0])) {
    struct hostent* h;
    in_addr_t address = inet_addr(run->ipv4);
    fprintf(out, " interface=\"%s\"", run->prif); 
    fprintf(out, " hostaddr=\"%s\"", run->ipv4);
    if ((h = wrap_gethostbyaddr((const char*) &address, sizeof(in_addr_t), AF_INET)))
      fprintf(out, " hostname=\"%s\"", h->h_name);
  }

  /* optional attributes for root element: application process id */
  if (run->child != 0)
    fprintf(out, " pid=\"%d\"", run->child);

  /* user info about who ran this thing */
  fprintf(out, " uid=\"%d\"", getuid());
  if (user) fprintf(out, " user=\"%s\"", user->pw_name);

  /* group info about who ran this thing */
  fprintf(out, " gid=\"%d\"", getgid());
  if (group) fprintf(out, " group=\"%s\"", group->gr_name);

  /* currently active umask settings */
  fprintf(out, " umask=\"0%03o\"", run->umask);

  /* finalize open tag of root element */
  fprintf(out, ">\n");

  /* <setup>, <prejob>, <application>, <postjob>, <cleanup> */
  printXMLJobInfo(out, 2, "setup", &run->setup);
  printXMLJobInfo(out, 2, "prejob", &run->prejob);
  printXMLJobInfo(out, 2, "mainjob", &run->application);
  printXMLJobInfo(out, 2, "postjob", &run->postjob);
  printXMLJobInfo(out, 2, "cleanup", &run->cleanup);

  /* <cwd> */
  if (run->workdir != NULL) {
    fprintf(out, "  <cwd>%s</cwd>\n", run->workdir);
  } else {
    fprintf(out, "  <cwd/>\n");
  }

  /* <usage> own resources */
  printXMLUseInfo(out, 2, "usage", &run->usage);

  if (!run->noHeader)
    printXMLMachineInfo(out, 2, "machine", &run->machine);

  /* User-specified initial and final arbitrary <statcall> records */
  if (run->icount && run->initial)
    for (i=0; i<run->icount; ++i)
      printXMLStatInfo(out, 2, "statcall", "initial", &run->initial[i]);
  if (run->fcount && run->final)
    for (i=0; i<run->fcount; ++i)
      printXMLStatInfo(out, 2, "statcall", "final", &run->final[i]);

  /* Default <statcall> records */
  printXMLStatInfo(out, 2, "statcall", "stdin", &run->input);
  updateStatInfo(&(((AppInfo*) run)->output));
  printXMLStatInfo(out, 2, "statcall", "stdout", &run->output);
  updateStatInfo(&(((AppInfo*) run)->error));
  printXMLStatInfo(out, 2, "statcall", "stderr", &run->error);

  /* If the job failed, or if the user requested the full kickstart record */
  if (run->status || run->fullInfo) {
    /* Extra <statcall> records */
    printXMLStatInfo(out, 2, "statcall", "gridstart", &run->gridstart);
    updateStatInfo(&(((AppInfo*) run)->logfile));
    printXMLStatInfo(out, 2, "statcall", "logfile", &run->logfile);

    /* <environment> */
    if (run->envp && run->envc) {
      char* s;

      /* attempt a sorted version */
      char** keys = malloc(sizeof(char*) * run->envc);
      for (i=0; i < run->envc; ++i) {
        keys[i] = run->envp[i] ? strdup(run->envp[i]) : "";
      }
      qsort((void*) keys, run->envc, sizeof(char*), mycompare);

      fprintf(out, "  <environment>\n");
      for (i=0; i < run->envc; ++i) {
        if (keys[i] && (s = strchr(keys[i], '='))) {
          *s = '\0'; /* temporarily cut string here */
          fprintf(out, "    <env key=\"%s\">", keys[i]);
          xmlquote(out, s+1, strlen(s+1));
          fprintf(out, "</env>\n");
          *s = '='; /* reset string to original */
        }
      }
      free((void*) keys);
      fprintf(out, "  </environment>\n");
    }

    /* <resource>  limits */
    printXMLLimitInfo(out, 2, &run->limits);

  } /* run->status || run->fullInfo */

  /* finish root element */
  fprintf(out, "</invocation>\n");

  return 0;
}

static
char*
pattern( char* buffer, size_t size,
         const char* dir, const char* sep, const char* file)
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

int countProcs(JobInfo *job) {
    int procs = 0;
    ProcInfo *i;
    for (i=job->children; i; i=i->next){
        procs++;
    }
    return procs;
}

int
printAppInfo(AppInfo* run)
/* purpose: output the given app info onto the given fd
 * paramtr: run (IN): is the collective information about the run
 * returns: the number of characters actually written (as of write() call).
 *          if negative, check with errno for the cause of write failure. */
{
  int result = -1;

  /* Get the descriptor to write to */
  int fd;
  if (run->logfile.source == IS_FILE) {
    fd = open(run->logfile.file.name, O_WRONLY | O_APPEND | O_CREAT, 0644);
  } else {
    fd = run->logfile.file.descriptor;
  }

  if (fd < 0) {
    debugmsg("ERROR: Unable to open output file\n");
    return -1;
  }

  /* Create a stream for the file. We use dup so that we can call
   * fclose later regardless of whether fd is stdout/stderr.
   */
  FILE *out = fdopen(dup(fd), "w");
  if (out == NULL) {
    debugmsg("ERROR: Unable to output stream\n");
    goto exit;
  }

  /* what about myself? Update stat info on log file */
  updateStatInfo(&run->logfile);

  /* obtain resource usage for xxxx */
  getrusage(RUSAGE_SELF, &run->usage);

  /* FIXME: is this true and necessary? */
  updateLimitInfo(&run->limits);

  /* stop the clock */
  now(&run->finish);

  int locked = mytrylock(fd);

  /* print the invocation record */
  result = convert2XML(out, run);

  fflush(out);

  if (locked==1) lockit(fd, F_SETLK, F_UNLCK);

  run->isPrinted = 1;

  fclose(out);
exit:
  if (run->logfile.source == IS_FILE)
    close(fd);

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
  debugmsg( "# deleteAppInfo(%p)\n", runinfo );
#endif

  deleteLimitInfo( &runinfo->limits );

  deleteStatInfo( &runinfo->input );
  deleteStatInfo( &runinfo->output );
  deleteStatInfo( &runinfo->error );
  deleteStatInfo( &runinfo->logfile );
  deleteStatInfo( &runinfo->gridstart );

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
