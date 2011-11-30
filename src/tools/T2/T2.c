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
#include <regex.h>

#include "error.h"
#include "item.h"
#include "workq.h"
#include "util.h"
#include "mypopen.h"

static const char* RCS_ID =
"$Id$";

static const char* REGEX_RFC2396 =
"^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)";

struct {
  workq_t  crew;	/* worker node management system */
  char*	   guc; 	/* absolute path to globus-url-copy */
  unsigned long version; /* version of globus-url-copy */
  char*    ln;		/* absolute path to Unix ln tool for symlinks */
  int      force;       /* set to not zero to use -f for ln -s */
  void*	   envp;	/* environment provided to main() */
  int      quiet;	/* quietness level for informational logging */
  int      retry;       /* if set, retry as often as necessary */
  size_t   argsize;     /* length of extra guc args */
  char**   args;        /* extra guc args */
  regex_t  rfc2396;     /* URI regular expression matcher from RFC 2396 */
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

#if 0
static
char*
parse_host_url( char* url ) 
/* purpose: Extract the hostname[:port] from the URL, if there is any
 * paramtr: supported URL (IN): gsiftp:// or file:// URL -- must not be NULL
 * returns: freshly allocated memarea containing the hostname
 */
{
  char* result = NULL;
#if 0
  char* s = url;
  char* e = url + strlen(url) - 1;
  char* host;

  /* sanity check */
  if ( s > e ) return result;

  /* find first colon */
  while ( s <= e && *s != ':' ) ++s;
  if ( s-url < 2 ) return result;

  /* early bail-out for file:// URIs */
  if ( strncmp( url, "file", s-url ) == 0 )
    return strdup("localhost");

  /* skip "//" after ":" */
  if ( s>e || *s++ != '/' ) return result;
  if ( s>e || *s++ != '/' ) return result;
  host = s;

  /* find next "/" after "://" */
  while ( s <= e && *s != '/' ) ++s;
  if ( s-host < 2 ) return result;

  result = (char*) calloc( s-host+2, 1 );
  strncpy( result, host, s-host );
#else
  regmatch_t pmatch[8];
  if ( regexec( &global.rfc2396, url, sizeof(pmatch), pmatch, 0 ) == 0 ) {
    size_t n = pmatch[4].rm_eo - pmatch[4].rm_so;
    result = calloc( n+2, 1 );
    strncpy( result, url+pmatch[4].rm_so, n );
  }
#endif
  return result;
}
#endif

static
char*
err_msg[] = {
  /* 2: missing source errors, local and remote, both in 2 parts */
  "error: a system call failed",
  "o such file or directory",
  "error: the server sent an error response: 550 550",
  "not a plain file",
  /* 1: URI with whitespace */
  "ERROR: too many url strings specified",
  /* 1: something apart from a missing source file */
  "error: a system call failed",

  /* 3: no gridftp server running */
  "error: a system call failed (Connection refused)",
  /* 1: credential problems with server */
  "Error with GSI credential",
  /* 0: timed out */
  "timed out"
};

static
int
arbitrate( int status, const char* line ) 
/* purpose: Arbitrate between hard error, soft errors and missing src
 * paramtr: status (IN): result code from child process
 *          line (IN): stdout of the child process
 * returns: -1: done -- don't retry
 *           0: soft error -- retry
 *           1: hard error -- don't retry
 *           2: missing source file -- don't retry
 *           3: hard server error -- don't retry
 */
{
  if ( WIFEXITED(status) ) {
    /* case: it worked! Off we leave */
    if ( WEXITSTATUS(status) == 0 )
      return -1;
    
    /* case: g-u-c terminated with 126 or 127 
     * --> pipe_out_cmd error, wrong path to app, hard break */
    if ( WEXITSTATUS(status) == 126 || WEXITSTATUS(status) == 127 )
      return 1;

    /* case: always retry in face of other errors */
    if ( global.retry ) return 0;

    /* FIXME: not zero, but regular exit - what now? must parse
     * <line> content. Assume for now that these errors are mostly
     * retryable, which is not true. */
    if ( WEXITSTATUS(status) == 1 ) {
      /* source file not found has two possible error messages,
       * errcode==1:
       *
       * [1] "error: a system call failed (No such file or directory)" 
       * [2] "error: the server sent an error response: 550 550 <SRC>: 
       * not a plain file." 
       */
      if ( ( strncmp( line, err_msg[0], strlen(err_msg[0]) ) == 0 && 
	     strstr(line,err_msg[1]) != NULL ) || 
	   ( strncmp( line, err_msg[2], strlen(err_msg[2]) ) == 0 && 
	     strstr(line,err_msg[3]) != NULL ) )
	return 2;
    }

    /* any kind of time-out should be considered soft */
    if ( strstr( line, err_msg[8] ) != NULL ) 
      return 0;

    /* no up and running gridftp server yields:
     * "error: a system call failed (Connection refused)" 
     * May also be returned by an overburdened server, sigh! */
    if ( strncmp( line, err_msg[6], strlen(err_msg[6]) ) == 0 )
      return 3;

    /* no account on remote gridftp server yields (auth probs):
     * "Error with GSI credential" somewhere within. This error
     * can be issued for src as well as for dst, sigh! */
    if ( strstr( line, err_msg[7] ) != NULL ) 
      return 1;

    /* ERROR: too many url strings specified */
    if ( strncmp( line, err_msg[4], strlen(err_msg[4]) ) == 0 )
      return 1;

    /* error: a system call failed -- apart from missing src */
    if ( strncmp( line, err_msg[5], strlen(err_msg[5]) ) == 0 )
      return 1;

    /* server error or misc local error: hard */
    if ( strncmp( line, err_msg[2], strlen(err_msg[2]) ) == 0 ||
	 strncmp( line, err_msg[0], strlen(err_msg[0]) ) == 0 ) 
      return 1;

    /* assume soft error on rest for now */
    return 0;
  } else if ( WIFSIGNALED(status) ) {
    /* case: g-u-c terminated on a signal --> hard error */
    return 1;
  }

  /* should not be reached */
  return 1;
}

static
int
sub_engine( item_p item, char* line, size_t size,
	    int* status, char* src, char* dst )
/* purpose: the true engine to start whatever needs to be done.
 * paramtr: item (IO): pointer to structure
 *          line (IN): area to capture things
 *          size (IN): capacity of line area
 *          src (IO): source URI
 *          dst (IO): destination URI
 * returns: arbitration
 */
{
  double start, diff;
  size_t retry;
  char*  s;
  char*  tag;
  char** arg = (char**) calloc( 16+global.argsize, sizeof(char*) );
  char   s_bufsize[16];
  char   s_streams[16];
  int    i, j, arbit = 0;
  unsigned long backoff = item->m_backoff;
  pthread_t id = pthread_self();
  struct timeval tv;

  for ( retry = 1; retry <= item->m_retries; ++retry ) {
    if ( retry == 1 ) {
      /* initial sleep -- way shorter than retry sleeps */
      double2timeval( &tv, item->m_initial );
    } else {
      /* implement exponential back-off */
      double2timeval( &tv, backoff );
      backoff *= 2.0;
    }

    /* report */
    if ( global.quiet < 2 ) {
      flockfile( stdout );
      printf( "# [%#010lx] %u sleeping for %.3f s\n", id, retry, 
	      timeval2double(tv) );
      funlockfile( stdout );
    }

    select( 0, NULL, NULL, NULL, &tv );

    /* assemble commdline for g-u-c */
    i = 0;
    if ( strncmp( src, "file:", 5 ) == 0 && strncmp( dst, "file:", 5 ) == 0 ) {
      /* symlink target instead of calling g-u-c */
      tag = "ln";
      arg[i++] = global.ln;
      if ( global.force ) arg[i++] = "-f";
      arg[i++] = "-s";

      /* point to the correct position inside the URL. Note the URL
       * may look like file:/path/to/file and file:///path/to/file. */
      arg[i++] = parse_file_url( src );
      arg[i++] = parse_file_url( dst );
      arg[i] = NULL; 

    } else {
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
	if ( item->m_streams > 1 ) {
	  snprintf( s_streams, sizeof(s_streams), "%u", item->m_streams );
	  arg[i++] = "-p";
	  arg[i++] = s_streams;
	}
      }

      if ( global.quiet < 0 ) 
	arg[i++] = "-vb";
      if ( global.quiet < -1 )
	arg[i++] = "-dbg";

      /* NEW: create directories with latest guc */
      if ( global.version >= 3020 ) 
	arg[i++] = "-cd";

      /* finish commandline argument vector */
      arg[i++] = src;
      arg[i++] = dst;
      arg[i] = NULL;
    }

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
    *status = pipe_out_cmd( tag, arg, global.envp, line, size );
    diff = item->m_timesum = now() - start;

    /* poor man's basename */
    if ( (s=strrchr( dst, '/' )) == NULL ) s=dst;
    else s++;

    /* report */
    if ( global.quiet < 2 ) {
      flockfile( stdout );
      printf( "# [%#010lx] %u %d/%d %.3fs 0x%02x \"%s\" %s\n", 
	      id, retry, *status >> 8, (*status & 127), diff, 
	      item->m_xfer->m_flags, s, line );
      /* queue time is (start - item->m_queued) + diff */
      funlockfile( stdout );
    }

    /* Error arbitration: Exit loop on anything but soft errors */
    if ( (arbit = arbitrate( *status, line )) ) break;
  }

  free((void*) arg);
  return arbit;
}

static
void
noop( void )
{
  /* noop */
}

static
int
engine( item_p item )
{
  size_t size = getpagesize();
  char* line = (char*) malloc(size);
  int transfer = (item->m_xfer->m_flags & 0x0003);
  int arbit = 0;
  int status = -1;
  dll_item_p src;
  dll_item_p dst;

  for ( src = item->m_xfer->m_src.m_head; src; src = src->m_next ) {
    for ( dst = item->m_xfer->m_dst.m_head; dst; dst = dst->m_next ) {

      /* skip trouble-burdened bad server from previous loop */
      if ( (dst->m_flag & 0x0001) == 1 ) continue;

      /* skip destinations we already successfully transferred to */
      if ( transfer == XFER_ALL && (dst->m_flag & 0x0002) == 2 ) continue;

      /* invoke sub engine */
      arbit = sub_engine(item,line,size,&status,src->m_data,dst->m_data);

      if ( arbit == -1 ) {
	/* mark this dst as done */
	dst->m_flag |= 0x0002;

	/* done -- ok */
	if ( transfer != XFER_ALL ) goto done;

      } else if ( arbit == 2 ) {
	if ( transfer == XFER_OPTIONAL ) status = 0;

	/* mark this source as invalid */
	src->m_flag |= 0x0001;

	/* source not found -- try next source */
	goto next_src;

      } else if ( arbit == 3 ) {
	/* unrecoverable server error, never try this dst again */
	dst->m_flag |= 0x0001;
      }
    }
  next_src:
    /* try next src */
    noop();
  }

  /* all tried, all failed -- don't fail for optional transfers */
  if ( arbit == 2 && transfer == XFER_OPTIONAL ) status = 0;
  /* and don't overwrite last status for "all" transfers */
  else if ( transfer != XFER_ALL ) status = -1;

 done:
  /* done, return last status */
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
helpMe( const char* programname )
/* purpose: write help message and exit */
{
  printf( "%s\nUsage:\t%s [options] baseuri basemnt [fof]\n",
          RCS_ID, programname );
  printf(
" -g guc\tUse guc as the path to your version of globus-url-copy\n"
" -G o,v\tPasses option o, prefixed with hyphen, to g-u-c with value v\n"
"\tNote: Use just -G o for an option without value. Use multiple times\n"
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
" baseuri For optimizations, this is the base URI of the gatekeeper\n"
" basemnt For optimizations, this is the corresponding storage mount point\n"
" fof\tList of filename pairs, one filename per line, default stdin\n", 
  DEFAULT_PARALLEL, DEFAULT_BUFSIZE, DEFAULT_STREAMS, DEFAULT_RETRIES,
  DEFAULT_BACKOFF, DEFAULT_INITIAL );
  exit(1);
}

void
parseCommandline( int argc, char* argv[], char* envp[], unsigned* parallel, 
		  char** basemnt, char** baseuri, FILE** input,
		  unsigned* bufsize, unsigned* streams, unsigned* retries,
		  double* initial, double* backoff )
{
  unsigned long guc_version;
  int status, option, showme = 0;
  char*  e, *ptr = strrchr(argv[0],'/');
  double temp;

  /* basename */
  if ( ptr == NULL ) ptr = argv[0];
  else ptr++;
  *parallel = DEFAULT_PARALLEL;
  *streams = DEFAULT_STREAMS;
  *bufsize = DEFAULT_BUFSIZE;
  *retries = DEFAULT_RETRIES;
  *backoff = DEFAULT_BACKOFF;
  *initial = DEFAULT_INITIAL;
  global.envp = envp;
  global.force = global.quiet = global.retry = 0;
  global.guc = NULL;

  if ( (status=regcomp( &global.rfc2396, REGEX_RFC2396, REG_EXTENDED )) != 0 ) {
    char buffer[512];
    regerror( status, &global.rfc2396, buffer, sizeof(buffer) );
    fprintf( stderr, "ERROR: Compling RFC 2396 regular expression: %s\n", buffer );
    exit(1);
  }

  opterr = 0;
  while ( (option = getopt( argc, argv, "?P:RT:fg:hi:p:qr:st:v" )) != -1 ) {
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
    case 'P':
      *parallel = strtoul( optarg, 0, 0 );
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
      if ( e != optarg && temp >= 0.0 ) *initial = temp;
      break;
    case 'f':
      global.force++;
      break;
    case 'g':
      if ( (guc_version = check_globus_url_copy( optarg, envp )) > 2000 ) {
	if ( global.guc != NULL ) free((void*) global.guc);
	global.guc = strdup(optarg);
	global.version = guc_version;
      } else {
	fprintf( stderr, "ERROR! Unable to use %s\n", optarg );
	exit(1);
      }
      break;
    case 'p':
      *streams = strtoul( optarg, 0, 0 );
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
    case 't':
      *bufsize = strtoul( optarg, 0, 0 );
      break;
    case 'h':
    case '?':
      helpMe(ptr);
      break;
    default:
      helpMe(ptr);
      break;
    }
  }

  /* extract mandatory parameters */
  if ( optind > argc-2 || optind < argc-3 ) helpMe(ptr);
  *baseuri = argv[optind+0];
  *basemnt = (char*) malloc( strlen(argv[optind+1]) + 8 );
  if ( argv[optind+1][0] == '/' ) strcpy( *basemnt, "file://" );
  else strcpy( *basemnt, "file:///" );
  strcat( *basemnt, argv[optind+1] );
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
    global.version = check_globus_url_copy( global.guc, envp );
    
    if ( (our=alter_globus_url_copy(argv[0])) != NULL ) {
      unsigned long our_version = check_globus_url_copy( our, envp );
      if ( our_version > global.version ) {
	/* prefer our version -- it's newer */
	free((void*) global.guc);
	global.guc = our;
	global.version = our_version;
      } else {
	/* other version is newer -- prefer the other version */
	free((void*) our);
      }
    }
  }

  /* show results */
  if ( showme ) {
    printf( "#\n# Currently active values for %s:\n# %s\n", ptr, RCS_ID );
    printf( "# max. g-u-c streams: %u\n", *streams );
    printf( "# max. g-u-c bufsize: %u\n", *bufsize );
    printf( "# max. g-u-c retries: %u\n", *retries );
    printf( "# location of g-u-c: %s\n", global.guc );
    printf( "# version# of g-u-c: %lu.%lu\n", 
	    global.version / 1000, global.version % 1000 );
    printf( "# max. forked g-u-c: %u\n", *parallel );
    printf( "# chosen quietness : %d\n", global.quiet );
    printf( "# use -f w/ symlink: %s\n", global.force ? "true" : "false" );
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
manage( xfer_p section, 
	unsigned bufsize, unsigned streams, unsigned retries,
	double initial, double backoff )
{
  int status;

  if ( (status = workq_add( &global.crew, section,
			    bufsize, streams, retries,
			    initial, backoff )) ) {
    fprintf( stderr, "Error queuing LFN %s, continuing: %d: %s\n",
	     section->m_lfn, status, strerror(status) );
	  
    /* obtain mutex to increment failures */
    if ( (status = pthread_mutex_lock( &global.crew.m_mutex )) ) {
      /* ok, this becomes serious now */
      fprintf( stderr, "lock mutex: %d: %s\n", status, strerror(status) ); 
      return 6;
    }
    global.crew.m_failure++;
    if ( (status = pthread_mutex_unlock( &global.crew.m_mutex )) ) {
      /* ok, this becomes serious now */
      fprintf( stderr, "unlock mutex: %d: %s\n", status, strerror(status) );
      return 6;
    }
  }

  return 0;
}

int
main( int argc, char* argv[], char* envp[] )
{
  int status;
  char* baseuri;
  char* basemnt;
  char* source = NULL;
  FILE* input;
  long timeleft;
  size_t sizeuri;
  size_t sizemnt;
  size_t lineno = 0;
  size_t linesize = getpagesize() << 1;
  unsigned parallel, streams, bufsize, retries;
  char* s, *line = malloc(linesize);
  double start, diff, initial, backoff;
  xfer_p section = NULL;

  /* check that guc is set up and runnable */
  global.envp = envp;
  parseCommandline( argc, argv, envp, &parallel, 
		    &baseuri, &basemnt, &input,
		    &bufsize, &streams, &retries,
		    &initial, &backoff );
  sizeuri = strlen(baseuri);
  sizemnt = strlen(basemnt);

  if ( global.version < 1000 ) {
    fprintf( stderr, "Error while checking usability of globus-url-copy\n" );
    return 2;
  }

  /* check for symlink capabilities */
  if ( (global.ln = check_link()) == 0 ) {
    fprintf( stderr, "Error while checking accessibility of link tool ln\n" );
    return 2;
  }

  /* check our grid certificate */
  if ( (timeleft = check_grid_proxy_info(envp)) <= 3600 ) {
    if ( timeleft == -1 ) 
      fprintf( stderr, "Error while executing grid-proxy-info\n" );
    else
      fprintf( stderr, "Error: Too little time left %ld s\n", timeleft );
    return 3;
  }
    
  /* create the crew of worker threads */
  if ( (status = workq_init( &global.crew, parallel, engine )) ) {
    fprintf( stderr, "Error while creating worker threads: %d: %s\n", 
	     status, strerror(status) );
    return 4;
  }

  /* deal safely with mutexes during forks */
  if ( (status = pthread_atfork( NULL, NULL, atfork_child )) ) {
    fprintf( stderr, "Error while registering fork handler: %d: %s\n", 
	     status, strerror(status) );
    return 5;
  }

  /* 
   * the big loop 
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
    if ( strlen(line) == 0 ) continue;
    else lineno++;

    /* count initial whitespaces */
    if ( ! isspace(line[0]) ) {
      unsigned flags = 0;
      char* lfn;
      char* s;

      /* new LFN, new section */
      if ( section && section->m_magic == T2_SECTION_MAGIC ) {
	/* add section to work queue */
	if ( (status = manage( section, bufsize, streams, retries,
			       initial, backoff )) )
	  return status;
      }

      /* create new section -- deleted inside "engine" */
      section = (xfer_p) malloc( sizeof(xfer_t) );
      lfn = strtok( line, " \t\r\n\v" ); 
      while ( (s = strtok( NULL, " \t\r\n\v" )) != NULL ) {
	if ( strcasecmp( s, "optional" ) == 0 ) 
	  flags = (flags & 0xFFFC) | XFER_OPTIONAL;
	if ( strcasecmp( s, "all" ) == 0 ) 
	  flags = (flags & 0xFFFC) | XFER_ALL;
	if ( strcasecmp( s, "any" ) == 0 ) 
	  flags = (flags & 0xFFFC) | XFER_ANY;
      }

      xfer_init( section, lfn, flags );
    } else if ( ! isspace(line[1]) ) {
      /* isa src TFN */
      s = line+1;

      /* match prefix (and replace) */
      if ( sizeuri && strncasecmp( s, baseuri, sizeuri ) == 0 ) {
	memmove( s+sizemnt, s+sizeuri, strlen(s+sizeuri)+1 );
	memcpy( s, basemnt, sizemnt );
      }

      xfer_add_src( section, s );
    } else {
      /* isa dst TFN */
      s = line+2;

      /* match prefix (and replace) */
      if ( sizeuri && strncasecmp( s, baseuri, sizeuri ) == 0 ) {
	memmove( s+sizemnt, s+sizeuri, strlen(s+sizeuri)+1 );
	memcpy( s, basemnt, sizemnt );
      }

      xfer_add_dst( section, s );
    }
  } /* while */

  if ( section && section->m_magic == T2_SECTION_MAGIC ) {
    /* add section to work queue */
    if ( (status=manage( section, bufsize, streams, retries,
			 initial, backoff )) )
      return status;
  }

  if ( source ) free((void*) source);
  if ( input != stdin ) fclose(input);

  /* wait for children to join */
  if ( (status = workq_destroy( &global.crew )) ) {
    fprintf( stderr, "while waiting for threads to exit: %d: %s\n",
	     errno, strerror(errno) );
    return 7;
  }

  /* this is actually the right place -- wait for threads to conclude */
  diff = now() - start; 

  /* post-condition: no more threads, safe to access crew and timers
   * directly */
  if ( global.quiet < 2 ) {
    printf( "# %lu threads, %lu messages, %lu successes, %lu failures\n", 
	    global.crew.m_threads, global.crew.m_request, 
	    global.crew.m_success, global.crew.m_failure );
 
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
	      global.crew.m_waitsum, global.crew.m_waitsum - global.crew.m_timesum,
	      temp );

      printf( "# %.3f s wall time, speed-up ", diff );
      if ( diff <= 1E-3 ) puts( "unknown" );
      else printf( "of %.1f\n", global.crew.m_timesum /* - diff */ / diff );
    }
  }

  /* done */
  fflush( stdout );
  regfree( &global.rfc2396 );
  return ( global.crew.m_failure > 0 ? 42 : 0 );
}
