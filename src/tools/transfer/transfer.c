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
#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <math.h>

#include <sys/wait.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <fcntl.h>
#include <unistd.h>
#include <pthread.h>

#include "capabilities.h"
#include "batch.h"
#include "item.h"
#include "workq.h"
#include "util.h"
#include "mypopen.h"


static const char* RCS_ID =
"$Id$";

struct {
  workq_t  crew;	/* worker node management system */
  char*	   guc; 	/* absolute path to globus-url-copy */
  unsigned long guc_caps; /* capabilities of this globus-url-copy */
  unsigned long version; /* version number of globus-url-copy */
  char*    tempfn;      /* temporary file for new guc transfers */
  void*	   envp;	/* environment provided to main() */
  int      quiet;	/* quietness level for informational logging */
  int      retry;       /* if set, retry as often as necessary */
  int      passive;     /* if set, allows g-u-c flags for passive mode */
  size_t   argsize;     /* length of extra guc args */
  char**   args;        /* extra guc args */
} global;

static
void
atfork_child( void )
{
  if ( global.crew.m_magic == WORKQ_MAGIC )
    pthread_mutex_init( &global.crew.m_mutex, NULL );
}

static
char*
parse_file_url( char* url )
/* purpose: parse a file url to determine the start of the path component.
 * paramtr: file url (IN): the file:// URL
 * returns: pointer to the start of the path, or NULL if invalid (oopsa)
 */
{
  char* s = url + strlen("file:");
  char* e = url + strlen(url) - 1;

  /* sanity check */
  if ( s > e ) return NULL;

  /* point to the correct position inside the URL. Note the URL
   * may look like file:/path/to/file and file:///path/to/file.
   * While g-u-c does not parse file://bogus.host/path/to/file,
   * it is still a potential candidate. bogus.host is usually
   * "xx" or "localhost". */

  if ( *s != '/' ) return s; /* be nice: file:path/to/file */
  else s++;
  if ( s > e ) return NULL;
  /* postcondition: we matched "file:/" up to here */

  if ( *s != '/' ) return s-1; /* be nice: file:/path/to/file */
  else s++;
  if ( s > e ) return NULL;
  /* postcondition: we matched "file://" up to here */

  if ( *s != '/' ) {
    /* uncommon case: file://bogus.host/path/to/file */
    char* save = s;
    while ( s <= e && *s != '/' ) s++; /* forward to next slash */
    if ( s > e ) s = save;
  }
  /* postcondition: s points to
   * [a] the 3rd slash in file:///path/to/url and file:///////hi/hi
   * [b] the absolute paths start slash in file://bogus.host/path
   * [c] or the 'n' in the erraneous case file://nopathhere
   */

  return s;
}

static
int
engine( item_p item )
{
  double start, diff;
  char*  s, *tag;
  size_t retry, size = getpagesize();
  char*  line = (char*) malloc(size);
  char** arg = (char**) calloc( 20+global.argsize, sizeof(char*) );
  char   s_bufsize[16], s_streams[16];
  pthread_t id = pthread_self();
  int i, j, status = -1;
  int batching = 0;
  double backoff = item->m_backoff;
  struct timeval tv;

  if ( arg == NULL || line == NULL ) {
    /* out of memory ? */
    flockfile( stderr );
    fputs( "FATAL: out of memory\n", stderr );
    funlockfile( stderr );
    if ( arg != NULL ) free((void*) arg);
    if ( line != NULL ) free((void*) line);
    return ENOMEM;
  } 

  for ( retry = 1; retry <= item->m_retries && ! batching; ++retry ) {
    /* do at least once loop */
    batching = ( item->m_src != NULL && item->m_dst == NULL );

    if ( retry == 1 ) {
      /* initial sleep -- way shorter than retry sleeps */
      double2timeval( &tv, item->m_initial );
    } else {
      /* implement exponential back-off */
      double2timeval( &tv, backoff );
      backoff *= 2.0;
    }

    if ( ! ( tv.tv_sec == 0 && tv.tv_usec < 1000 ) ) {
      /* report and sleep anything longer than 1 ms */
      if ( global.quiet < 1 ) {
	flockfile( stdout );
	printf( "# [%#010lx] %u sleeping for %.3f s\n", id, retry, 
		timeval2double(tv) );
	funlockfile( stdout );
      }
      select( 0, NULL, NULL, NULL, &tv );
    }

    /* assemble commdline for g-u-c */
    i = 0;

    /* regular calls to g-u-c */
    tag = "g-u-c";
    arg[i++] = global.guc;

    /* 20050419: Add extra arguments */
    for ( j=0; j < global.argsize; ++j )
      arg[i++] = global.args[j];

    if ( item->m_retries == 1 ||
	 (item->m_retries > 1 && retry < item->m_retries) ) {
      /* only assemble these, if more than one retry permitted, and this
       * is not the last retry. For single retries, use what the user
       * said. */
      if ( item->m_bufsize > 1024 ) {
	snprintf( s_bufsize, sizeof(s_bufsize), "%u", item->m_bufsize );
	arg[i++] = "-tcp-bs";
	arg[i++] = s_bufsize;
      }
      if ( global.passive && item->m_streams > 1 && item->m_streams < 256 ) {
	snprintf( s_streams, sizeof(s_streams), "%u", item->m_streams );
	arg[i++] = "-p";
	arg[i++] = s_streams;
      }
    }

    if ( global.quiet < 0 && (global.guc_caps & GUC_PERFDATA) != 0 )
      arg[i++] = "-vb";
    if ( global.quiet < -1 && (global.guc_caps & GUC_DEBUG) != 0 )
      arg[i++] = "-dbg";
    if ( (global.guc_caps & GUC_CREATEDIR) != 0 ) 
      arg[i++] = "-cd";
    
    if ( batching ) {
      /* sanity assertion */
      if ( (global.guc_caps & GUC_FROMFILE) == 0 ) {
	/* FATAL */
	flockfile( stderr );
	fputs( "FATAL: I am confused about the guc capabilities. I was expecting to be\n"
	       "able to use the -f option, but suddenly I cannot.\n", stderr );
	funlockfile( stderr );
	free((void*) arg);
	free((void*) line);
	return ENOSYS;
      }

      if ( global.passive && (global.guc_caps & GUC_FAST) != 0 )     arg[i++] = "-fast";
      if ( (global.guc_caps & GUC_CONTINUE) != 0 ) arg[i++] = "-c";
      if ( (global.guc_caps & GUC_RESTART) != 0 )  arg[i++] = "-rst";

      /* run new g-u-c */
      arg[i++] = "-f";
      arg[i++] = item->m_src;
    } else {
      /* finish commandline argument vector */
      arg[i++] = item->m_src;
      arg[i++] = item->m_dst;
    }

    /* finish arg vector */
    arg[i] = NULL;

    if ( global.quiet < 0 ) {
      int k;
      /* report */
      flockfile( stdout );
      printf( "# [%#010lx] %u", id, retry );
      for ( k=0; k<i; ++k ) {
	fputc( ' ', stdout );
	fputs( arg[k], stdout );
      }
      fputc( '\n', stdout );
      funlockfile( stdout );
    }

    /* run g-u-c while capturing its output for later examination */
    memset( line, 0, size );
    start = now();
    if ( batching ) {
      /* batching mode */
      char* p;
      ssize_t rsize;
      PipeCmd* cmd = mypopen( tag, arg, global.envp );

      if ( cmd != NULL ) {
	while ( (rsize=read( cmd->readfd, line, size )) > 0 ) {
	  for ( p = line + rsize - 1; p >= line && isspace(*p); *p-- = '\0' ) ;
	  if ( global.quiet < 2 ) {
	    flockfile(stdout);
	    printf( "# [%#010lx] %u %.3fs %s\n", id, retry, now()-start, line );
	    funlockfile( stdout );
	  }
	}
	status = mypclose(cmd);
      } else {
	status = -1;
      }
    } else {
      /* non-batching mode */
      status = pipe_out_cmd( tag, arg, global.envp, line, size );
    }
    diff = item->m_timesum = now() - start;

    if ( batching ) s=item->m_src;
    else {
      /* poor man's basename */
      if ( (s=strrchr( item->m_dst, '/' )) == NULL ) s=item->m_dst;
      else s++;
    }

    /* report */
    if ( global.quiet < 2 ) {
      flockfile( stdout );
      if ( batching ) {
	printf( "# [%#010lx] %u %d/%d %.3fs \"%s\"\n", 
		id, retry, status >> 8, (status & 127), diff, s );
      } else {
	printf( "# [%#010lx] %u %d/%d %.3fs \"%s\" %s\n", 
		id, retry, status >> 8, (status & 127), diff, s, line );
      }

      /* queue time is (start - item->m_queued) + diff */
      funlockfile( stdout );
    }

    /*
     * Error arbitration between hard and soft errors. Hard errors exit
     * the loop without further retries. Soft errors permit further
     * retries (due to lack of knowledge).
     */
    if ( WIFEXITED(status) ) {
      char* msg;

      /* case: it worked! Off we leave */
      if ( WEXITSTATUS(status) == 0 ) break;

      /* case: g-u-c terminated with 126 or 127 --> pipe_out_cmd error, hard */
      if ( WEXITSTATUS(status) == 126 || WEXITSTATUS(status) == 127 ) break;

      /* case: always retry in face of other errors */
      if ( global.retry ) continue;

      /* FIXME: not zero, but regular exit - what now? must parse <line>
       * content. Assume for now that these errors are all retryable,
       * which is not true */

      /* ERROR: too many url strings specified */
      msg = "ERROR: too many url strings specified";
      if ( strncmp( line, msg, strlen(msg) ) == 0 ) break;

      /* error: a system call failed (No such file or directory) */
      msg = "error: a system call failed";
      if ( strncmp( line, msg, strlen(msg) ) == 0 ) break;

      /* error: probably an unknown file */
      msg ="error: the server sent an error response: 550 550";
      if ( strncmp( line, msg, strlen(msg) ) == 0 ) break;

    } else if ( WIFSIGNALED(status) ) {
      /* case: g-u-c terminated on a signal --> hard error */
      break;
    }
  }

  /* done, return last status */
  free((void*) arg);
  free((void*) line );
  return status;
}

void
add_to_args( char* s, int hyphen ) 
     /* purpose: adds the string to the global guc option list
      * paramtr: s (IO): string containing the option.
      *          hyphen (IN): add this many hyphens before option
      * returns: ?
      */
{
  int i = 0;
  size_t where = global.argsize;
  global.argsize++;
  global.args = realloc( global.args, global.argsize * sizeof(char*) );
  global.args[where] = (char*) malloc( strlen(s) + 1 + hyphen );
  while ( i < hyphen ) global.args[where][i++] = '-';
  strcpy( global.args[where]+i, s );
}

void
helpMe( const char* programname, int rc )
/* purpose: write help message and exit */
{
  printf( "%s\nUsage:\t%s [options] baseuri basemnt [fof]\n\n", 
	  RCS_ID, programname );
  printf(
"Optional arguments:\n"
" -g fn\tUse fn as the path to your version of globus-url-copy\n"
"\tNote: The globus-url-copy actually used depends on its capabilities\n"
" -G o,v\tPasses option o, prefixed with hyphen, to g-u-c with value v\n"
"\tNote: Use just -G o for an option without value. Use multiple times\n"
" -N\tAvoid the batch mode, even if it is available (debugging)\n"
" -n\tDo not add -fast and -p to globus-url-copy (to avoid passive mode)\n"
" -P n\tUse n as maximum number of parallel processes for g-u-c, default %d\n"
" -f\tUse the -f option with ln -s for local files, default is not\n"
" -t n\tUse n as TCP buffer size for g-u-c\'s -tcp-bs option, default %u\n"
" -p n\tUse n as number of streams for g-u-c\'s -p option, default %u\n"
" -r n\tUse n as the maximum number of g-u-c retry attempts, default %u\n"
" -q\tUse multiple times to be less noisy, default is somewhat noisy\n"
" -v\tUse a more verbose mode, opposite effect of using -q\n"
" -R\tForce retries for almost all server-side errors\n"
" -T iv\tUse interval iv for the initial exponential back-off, default %.1f s\n"
" -i iv\tUse initial iv to sleep between transfers, default %.1f s\n"
" -s\tIs a debug option to show the values of all options after parsing\n"
" -S\tDistribute pairs by source-host over slots instead of round-robin\n"
"\n"
"Mandatory arguments:\n"
" baseuri For optimizations, this is the base URI of the gatekeeper\n"
" basemnt For optimizations, this is the corresponding storage mount point\n"
" fof     List of filename pairs, one filename per line, default is stdin\n", 
  DEFAULT_PARALLEL, DEFAULT_BUFSIZE, DEFAULT_STREAMS, DEFAULT_RETRIES,
  DEFAULT_BACKOFF, DEFAULT_INITIAL );
  exit(rc);
}

void
parseCommandline( int argc, char* argv[], char* envp[], unsigned* parallel, 
		  char** basemnt, char** baseuri, FILE** input,
		  unsigned* bufsize, unsigned* streams, unsigned* retries,
		  int* force, int* sorted, double* initial, double* backoff,
		  int* nobatch )
{
  size_t len;
  int option, showme = 0;
  char* e, *ptr = strrchr(argv[0],'/');
  double temp;
  unsigned long capabilities;
  
  /* basename */
  if ( ptr == NULL ) ptr = argv[0];
  else ptr++;
  *parallel = DEFAULT_PARALLEL;
  *streams = DEFAULT_STREAMS;
  *bufsize = DEFAULT_BUFSIZE;
  *retries = DEFAULT_RETRIES;
  *initial = DEFAULT_INITIAL;
  *backoff = DEFAULT_BACKOFF;
  global.envp = envp;
  *force = *sorted = global.quiet = global.retry = 0;
  global.passive = 1;
  global.guc = NULL;

  opterr = 0;
  while ( (option = getopt( argc, argv, "?G:NnP:RT:fg:hi:p:qr:sSt:v" )) != -1 ) {
    switch ( option ) {
    case 'G':
      if ( optarg && *optarg ) {
	char* arg = strdup(optarg);
	char* s = strchr( arg, ',' );
	if ( s == NULL ) {
	  /* option without value */
	  add_to_args( arg, 1 );
	} else {
	  /* option with value */
	  *s++ = '\0';
	  add_to_args( arg, 1 );
	  add_to_args( s, 0 );
	}
      }
      break;
    case 'N':
      *nobatch = 1;
      break;
    case 'n':
      /* Remove -fast and -p flags from g-u-c */
      global.passive = 0;
      break;
    case 'P':
      *parallel = max_procs( strtoul( optarg, 0, 0 ) );
      break;
    case 'R':
      global.retry++;
      break;
    case 'T':
      temp = strtod( optarg, &e );
      if ( e != optarg && temp >= 0.0 )	*backoff = temp;
      break;
    case 'i':
      temp = strtod( optarg, &e );
      if ( e != optarg && temp >= 0.0 )	*initial = temp;
      break;
    case 'f':
      *force = 1;
      break;
    case 'g':
      if ( (capabilities = guc_capabilities( optarg, envp )) != 0ul ) {
	if ( global.guc != NULL ) free((void*) global.guc);
	global.guc = strdup(optarg);
	global.guc_caps = capabilities;
	global.version = check_globus_url_copy( optarg, envp );
      } else {
	fprintf( stderr, "ERROR! Unable to use %s\n", optarg );
	exit(1);
      }
      break;
    case 'p':
      *streams = max_files( strtoul( optarg, 0, 0 ) );
      break;
    case 'q':
      global.quiet++;
      break;
    case 'v':
      global.quiet--;
      break;
    case 'r':
      *retries = strtoul( optarg, 0, 0 );
      break;
    case 's':
      showme = 1;
      break;
    case 'S':
      *sorted = 1;
      break;
    case 't':
      *bufsize = strtoul( optarg, 0, 0 );
      break;
    case 'h':
    case '?':
      helpMe(ptr,0);
      break;
    default:
      helpMe(ptr,1);
      break;
    }
  }

  /* extract mandatory parameters */
  if ( optind > argc-2 || optind < argc-3 ) helpMe(ptr,1);
  *baseuri = argv[optind+0];
  len = strlen(argv[optind+1])+12;
  memset( (*basemnt = (char*) malloc(len)), 0, len );
  if ( argv[optind+1][0] == '/' ) strncpy( *basemnt, "file://", len );
  else strncpy( *basemnt, "file:///", len );
  strncat( *basemnt, argv[optind+1], len );
  if ( optind+2 >= argc ) *input = stdin;
  else {
    if ( (*input = fopen(argv[optind+2],"r")) == NULL ) {
      fprintf( stderr, "open %s: %s\n", argv[optind+2], strerror(errno) );
      exit(1);
    }
  }

  /* compare with shipped version, unless -g was present */
  if ( global.guc == NULL ) {
    char* our;
    global.guc = default_globus_url_copy();
    global.guc_caps = guc_capabilities( global.guc, envp );
    global.version = check_globus_url_copy( global.guc, envp );

    if ( (our=alter_globus_url_copy(argv[0])) != NULL ) {
      long our_version;
      unsigned long our_caps = guc_capabilities( our, envp );
      if ( our_caps > global.guc_caps || 
	 ( our_caps == global.guc_caps && 
	   (our_version=check_globus_url_copy(our,envp)) > global.version ) ) {
	/* prefer our version -- it's richer */
	free((void*) global.guc);
	global.guc = our;
	global.guc_caps = our_caps;
	global.version = our_version;
      } else {
	/* other version is newer -- prefer the other version */
	free((void*) our);
      }
    }
  }

#if 0
  /* determine version number of chosen guc itself */
  /* Warning: This has a side effect of initializing the cache */
  global.version = guc_versions( NULL, global.guc, envp );
#endif

  if ( global.guc_caps == 0x0fff ) {
    /* Remove FAST mode as bug-fix for 3.2.* */
    if ( global.quiet < 0 ) 
      printf( "# work-around for GT 3.2.1 -- removing -fast capability\n" );
    global.guc_caps &= ~GUC_FAST;
  } else if ( global.guc_caps == 0x7fff ) {
    /* bug#3919: Remove RESTART modes as bug-fix for unpatched 4.0.* */
    unsigned long version = 
      guc_versions( "globus_ftp_client_restart_plugin", global.guc, envp );
    if ( version > 0 && version < 3003ul ) {
      if ( global.quiet < 0 ) 
	printf( "# work-around for bug#3919 -- removing restart capabilities\n" );
      global.guc_caps &= ~( GUC_RESTART | GUC_REST_IV | GUC_REST_TO );
    }
  }
  if ( (global.guc_caps & GUC_CREATEDIR) > 0 ) {
    /* bug#3769: Create deep directory works in globus_gass_copy >= 3.20 */
    unsigned long version = 
      guc_versions( "globus_gass_copy", global.guc, envp );
    if ( version > 0 && version < 3020ul ) {
      if ( global.quiet < 0 )
	printf( "# work-around for bug#3769 -- removing -cd capability\n" );
      global.guc_caps &= ~GUC_CREATEDIR;
    }
  }

  /* show results */
  if ( showme ) {
    static char* notset = "(not set)";
    char* gtpr = getenv("GLOBUS_TCP_PORT_RANGE");
    char* gtsr = getenv("GLOBUS_TCP_SOURCE_RANGE");
    char* g_l = getenv("GLOBUS_LOCATION");

    if ( gtpr == NULL ) gtpr=notset;
    if ( gtsr == NULL ) gtsr=notset;
    if ( g_l == NULL ) g_l=notset;

    printf( "#\n# Currently active values for %s:\n# %s\n", ptr, RCS_ID );
    printf( "# GLOBUS_TCP_PORT_RANGE=%s\n", gtpr );
    printf( "# GLOBUS_TCP_SOURCE_RANGE=%s\n", gtsr );
    printf( "# GLOBUS_LOCATION=%s\n", g_l );
    printf( "# max. g-u-c bufsize: %u\n", *bufsize );
    printf( "# max. g-u-c streams: %u\n", *streams );
    printf( "# max. g-u-c retries: %u\n", *retries );
    printf( "# xfer pair slotting: %s\n", ( *sorted ? "src-host-sorted" : "round-robin" ) );
    printf( "# location of g-u-c: %s\n", global.guc );
    printf( "# g-u-c capabilit's: %04lx (%lu.%lu)\n", global.guc_caps,
	    global.version / 1000, global.version % 1000 );
    printf( "# g-u-c addit. args:" );
    if ( global.argsize ) {
      int i = 0;
      while ( i < global.argsize ) printf( " %s", global.args[i++] );
      putchar( '\n' );
    } else {
      puts( " (none)" );
    }
    printf( "# max. forked g-u-c: %u\n", *parallel );
    printf( "# chosen quientness: %d\n", global.quiet );
    printf( "# use -f w/ symlink: %s\n", *force ? "true" : "false" );
    printf( "# forceful retries: %s\n", global.retry ? "true" : "false" );
    printf( "# initial interval: %.3f s\n", *initial );
    printf( "# backoff interval: %.3f s\n", *backoff );
    printf( "# external TFN base: %s\n", *baseuri );
    printf( "# internal SFN base: %s\n", *basemnt );
    printf( "# list of file base: %s\n", 
	    ( optind+2>=argc ? "stdin" : argv[optind+2] ) );
    fflush( stdout );
  }
}

int
main( int argc, char* argv[], char* envp[] )
{
  int   status, flag, force = 0, sorted = 0, nobatch = 0;
  char* baseuri;
  char* basemnt;
  char* source = 0;
  char* gpi = NULL;
  FILE* input;
  long timeleft;
  size_t sizeuri, sizemnt, lineno = 0;
  size_t linesize = getpagesize();
  unsigned parallel, streams, bufsize, retries;
  unsigned long int linkage[3] = { 0, 0, 0 };
  char* s, *line = malloc(linesize);
  double start, diff, initial, backoff;
  BatchingFunction* queue_batching;
  Batch batch = { 0, NULL };

  parseCommandline( argc, argv, envp, &parallel, 
		    &baseuri, &basemnt, &input,
		    &bufsize, &streams, &retries,
		    &force, &sorted, &initial, &backoff,
		    &nobatch );
  sizeuri = strlen(baseuri);
  sizemnt = strlen(basemnt);
  queue_batching = sorted ? queue_batching_ss : queue_batching_rr;

  if ( global.guc_caps == 0 ) {
    fprintf( stderr, "Error while checking usability of globus-url-copy\n" );
    return 2;
  } else {
    /* must we use old-style copies, or can we more efficiently 
     * batch multiple files into one guc invocation? */
    if ( global.guc_caps <= 0x0fff ) {
      flag = ( (global.guc_caps & GUC_FROMFILE) != 0 &&
	       (global.guc_caps & GUC_RESTART) != 0 );
    } else {
      flag = ( (global.guc_caps & GUC_FROMFILE) != 0 );
    }
  }

  /* check our grid certificate */
  if ( (gpi=default_grid_proxy_info()) != NULL ) {
    if ( (timeleft = check_grid_proxy_info(gpi,envp)) <= 3600 ) {
      if ( timeleft == -1 ) 
	fprintf( stderr, "Error while executing %s\n", gpi );
      else
	fprintf( stderr, "Error: Too little time left %ld s\n", timeleft );
      return 3;
    }
    free((void*) gpi);
  } else {
    fputs( "Warning: Unable to locate grid-proxy-info, continuing.\n", stderr );
  }
    
  /* create the crew of worker threads */
  if ( (status = workq_init( &global.crew, parallel, engine )) ) {
    fprintf( stderr, "Error while creating worker threads: %d: %s\n", 
	     status, strerror(status) );
    return 4;
  } else if ( global.quiet < 0 ) {
    printf( "## workq_init( %p, %d, %p )\n", &global.crew, parallel, engine );
  }

  /* deal safely with mutexes during forks */
  if ( (status = pthread_atfork( NULL, NULL, atfork_child )) ) {
    fprintf( stderr, "Error while registering fork handler: %d: %s\n", 
	     status, strerror(status) );
    return 5;
  } else if ( global.quiet < 0 ) {
    printf( "## pthread_atfork( (nil), (nil), %p )\n", atfork_child );
  }

  /* prepare batching, if at all possible */
  if ( nobatch ) flag = 0;
  if ( flag ) init_batching( &batch, parallel);
  flag = ( ! nobatch && batch.batch != NULL );
  if ( flag && global.quiet < 0 )
    puts( "## using batching" );

  /*
   * the big loop over our input file
   */
  start = now();
  while ( fgets( line, linesize, input ) ) {
    /* FIXME: unhandled overly long lines */

    /* comment */
    if ( (s = strchr( line, '#' )) ) *s-- = '\0';
    else s = line + strlen(line) - 1;

    /* chomp */
    while ( s > line && isspace(*s) ) *s-- = '\0';

    /* skip empty (or meaningless) lines */
    if ( strlen(line) < 10 ) continue;
    else lineno++;

    /* match prefix (and replace) */
    if ( sizeuri && strncasecmp( line, baseuri, sizeuri ) == 0 ) {
      memmove( line+sizemnt, line+sizeuri, strlen(line+sizeuri)+1 );
      memcpy( line, basemnt, sizemnt );
    }

    if ( (lineno & 1) ) {
      /* source */
      if ( source ) free((void*) source);
      source = strdup(line);
    } else {
      /* destination */
      
      if ( strncmp( source, "file:", 5 ) == 0 &&
	   strncmp( line, "file:", 5 ) == 0 ) {
	/* symlink files: right here, right now, no threads */

	/* point to the correct position inside the URL. Note the URL
	 * may look like file:/path/to/file and file:///path/to/file. */
	char* src = parse_file_url( source );
	char* dst = parse_file_url( line );
	double symdiff = now();
	int result = full_symlink( src, dst, force );
	int saverr = errno;
	symdiff = now() - symdiff;

	/* report */
	if ( global.quiet < 2 ) {
	  /* poor man's basename */
	  if ( (s=strrchr( dst, '/' )) == NULL ) s=dst;
	  else s++;

	  flockfile( stdout );
	  printf( "# [%#010lx] %u %d/%d %.3fs \"%s\" %s\n", 
		  pthread_self(), 1, (result ? 1 : 0), 0, symdiff, s, 
		  (result ? strerror(saverr) : "") );
	  funlockfile( stdout );
	}

	/* update status counters -- no threads to worry */
	linkage[0]++;
	linkage[ result==0 ? 1 : 2 ]++;

      } else if ( flag ) {
	/* batch mode: queue filenames for guc -f */
	queue_batching( &batch, source, line );

      } else {
	/* really copy files: use the threads */
	if ( (status = workq_add( &global.crew, source, line, 
				  bufsize, streams, retries,
				  initial, backoff )) ) {
	  fprintf( stderr, "Error queuing dst-URI %s, continuing: %d: %s\n",
		   line, status, strerror(status) );
	  
	  /* obtain mutex to increment failures */
	  if ( (status = pthread_mutex_lock( &global.crew.m_mutex )) ) {
	    /* ok, this becomes serious now */
	    fprintf( stderr, "lock mutex: %d: %s\n", 
		     status, strerror(status) ); 
	    return 6;
	  }
	  global.crew.m_failure++;
	  if ( (status = pthread_mutex_unlock( &global.crew.m_mutex )) ) {
	    /* ok, this becomes serious now */
	    fprintf( stderr, "unlock mutex: %d: %s\n", 
		     status, strerror(status) );
	    return 6;
	  } 
	} /* guc version */
      } /* if strncmp */
    }
  } /* while */

  if ( source ) free((void*) source);
  if ( input != stdin ) fclose(input);

  /* if we batched, now is the time to start threads */
  if ( flag ) {
    unsigned i;
    struct stat st;

    for ( i=0; i<parallel; ++i ) {
      if ( fstat( batch.batch[i].descriptor, &st ) == 0 && st.st_size > 0 ) {
	/* WARNING! After close() you MUST invalidate fd, or
	 * Linux pthreads will suffer dire consequences !!! */
	close( batch.batch[i].descriptor );
	batch.batch[i].descriptor = -1;

	if ( global.quiet < 0 ) 
	  printf( "# adding batch temp file %s\n", batch.batch[i].filename );

	if ( (status = workq_add( &global.crew, batch.batch[i].filename, NULL,
				  bufsize, streams, retries,
				  initial, backoff )) ) {
	  fprintf( stderr, "Error queuing batch %u (%d:%s), "
		   "continuing: %d: %s\n", i, batch.batch[i].descriptor,
		   batch.batch[i].filename, status, strerror(status) );
	  
	  /* obtain mutex to increment failures */
	  if ( (status = pthread_mutex_lock( &global.crew.m_mutex )) ) {
	    /* ok, this becomes serious now */
	    fprintf( stderr, "lock mutex: %d: %s\n", 
		     status, strerror(status) ); 
	    return 6;
	  }
	  global.crew.m_failure++;
	  if ( (status = pthread_mutex_unlock( &global.crew.m_mutex )) ) {
	    /* ok, this becomes serious now */
	    fprintf( stderr, "unlock mutex: %d: %s\n", 
		     status, strerror(status) );
	    return 6;
	  } 
	} /* workq_batch */
      }	/* fstat */
    } /* for */
  } /* if flag */

  /* wait for children to join */
  if ( (status = workq_destroy( &global.crew )) ) {
    fprintf( stderr, "while waiting for threads to exit: %d: %s\n",
	     errno, strerror(errno) );
    return 7;
  }

  /* this is actually the right place -- wait for threads to conclude */
  diff = now() - start; 

  /* release batching, if at all possible */
  if ( flag ) done_batching( &batch );

  /* Report some statistics. 
   * No more threads, safe to access crew and timers directly w/o mutex */
  if ( global.quiet < 2 ) {
    printf( "# %lu threads, %lu+%lu messages, %lu+%lu successes, "
	    "%lu+%lu failures\n", global.crew.m_threads, 
	    global.crew.m_request, linkage[0], 
	    global.crew.m_success, linkage[1], 
	    global.crew.m_failure, linkage[2] );
 
    if ( global.quiet < 1 ) {
      double temp;

      if ( ! global.crew.m_request ) temp = 0.0;
      else temp = global.crew.m_timesum / global.crew.m_request;
      printf( "# %.3f s for %lu requests = %.3f s per request, ",
	      global.crew.m_timesum, global.crew.m_request, temp );
  
      if ( global.crew.m_timesum <= 1E-6 ) temp = 0.0;
      else temp = global.crew.m_request / global.crew.m_timesum;
      printf( "%.1f/s spawn rate\n", temp );

      if ( ! global.crew.m_request ) temp = 0.0;
      else temp = global.crew.m_waitsum / global.crew.m_request;
      printf( "# %.3f s in Q of which %.3f s pure wait; %.3f s Qtime per request\n",
	      global.crew.m_waitsum, 
	      global.crew.m_waitsum - global.crew.m_timesum,
	      temp );

      printf( "# %.3f s wall time, speed-up ", diff );
      if ( diff <= 1E-3 ) puts( "unknown" );
      else printf( "of %.1f\n", global.crew.m_timesum /* - diff */ / diff );
    }
  }

  /* done */
  fflush( stdout );
  return ( (global.crew.m_failure + linkage[2]) > 0 ? 42 : 0 );
}
