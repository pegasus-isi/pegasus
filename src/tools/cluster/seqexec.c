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
#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <time.h>

#include <sys/time.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <signal.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/utsname.h>
#include <sys/poll.h>

static const char* RCS_ID =
"$Id$";

#define MAXSTR 4096

extern char *optarg;
extern int optind, opterr, optopt;
int debug = 0;
int progress = -1;
char* application = "seqexec";
char* identifier = NULL;
static struct utsname uname_cache;

static
ssize_t
showerr( const char* fmt, ... )
{
  char line[MAXSTR];
  va_list ap;

  va_start( ap, fmt );
  vsnprintf( line, sizeof(line), fmt, ap );
  va_end(ap);

  /* atomic write */
  return write( STDOUT_FILENO, line, strlen(line) );
}

static 
double
now( time_t* when )
/* purpose: obtains an UTC timestamp with microsecond resolution.
 * returns: the timestamp, or -1.0 if it was completely impossible.
 */
{
  int timeout = 0;
  struct timeval t = { -1, 0 };
  while ( gettimeofday( &t, NULL ) == -1 && timeout < 10 ) timeout++;
  if ( when != NULL ) *when = t.tv_sec;
  return (t.tv_sec + t.tv_usec/1E6);
}

static
char*
isodate( time_t seconds, char* buffer, size_t size )
/* purpose: formats ISO 8601 timestamp into given buffer (simplified)
 * paramtr: seconds (IN): time stamp
 *          buffer (OUT): where to put the results
 *          size (IN): capacity of buffer
 * returns: pointer to start of buffer for convenience. 
 */
{
  struct tm zulu = *gmtime(&seconds);
  struct tm local = *localtime(&seconds);
  zulu.tm_isdst = local.tm_isdst;
  {
    time_t distance = (seconds - mktime(&zulu)) / 60;
    int hours = distance / 60;
    int minutes = distance < 0 ? -distance % 60 : distance % 60;
    size_t len = strftime( buffer, size, "%Y-%m-%dT%H:%M:%S", &local );
    snprintf( buffer+len, size-len, "%+03d:%02d", hours, minutes );
  }
  return buffer;
}

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
" input\tFile with list of applications and args to execute, default stdin.\n" );
  exit(rc);
}

static
void
parseCommandline( int argc, char* argv[], int* fail_hard )
{
  char *s, *ptr = strrchr( argv[0], '/' );
  int option;

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
  while ( (option = getopt( argc, argv, "R:dfhs:" )) != -1 ) {
    switch ( option ) {
    case 'R':
      if ( progress != -1 ) close(progress);
      if ( (progress = open( optarg, O_WRONLY | O_APPEND | O_CREAT, 0666 )) == -1 ) {
	showerr( "%s: open progress %s: %d: %s\n",
		 application, optarg, errno, strerror(errno) );
      }
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

/* create finite state automaton to remove one level of quoting in the
 * same manner as a shell. This means in particular, in case you are not
 * aware (see man of your shell): 
 *
 * o backslashes are meaningless inside single quotes
 * o single quotes are meaningless inside double quotes
 *
 *     |  sq |  dq |  bs | lws | otr | EOS
 * ----+-----+-----+-----+-----+-----+-----
 *   0 | 3,- | 4,- | 2,- | 0,- | 1,s | F,-
 *   1 | 3,- | 4,- | 2,- | 0,F | 1,s | F,F
 *   2 | 1,s | 1,s | 1,s | 1,s | 1,s | E1
 *   3 | 1,- | 3,s | 3,s | 3,s | 3,s | E2
 *   4 | 4,s | 1,- | 5,- | 4,s | 4,s | E3
 *   5 | 5,s | 5,s | 5,s | 5,s | 5,s | E1
 * ----+-----------------------------------
 *   6 | F   | final state, done with success
 *   7 | E1  | error: premature end of string
 *   8 | E2  | error: missing single quote
 *   9 | E3  | error: missing double quote
 */
static char c_state[6][6] =
  { { 3, 4, 2, 0, 1, 6 },    /* 0: skip linear whitespace */
    { 3, 4, 2, 0, 1, 6 },    /* 1: gobble unquoted nonspaces */
    { 1, 1, 1, 1, 1, 7 },    /* 2: unquoted backslash */
    { 1, 3, 3, 3, 3, 8 },    /* 3: single quote mode */
    { 4, 1, 5, 4, 4, 9 },    /* 4: double quote mode */
    { 4, 4, 4, 4, 4, 7 } };  /* 5: double quote backslash mode */

static char c_action[6][6] = 
  { { 0, 0, 0, 0, 1, 0 },    /* 0: skip linear whitespace */
    { 0, 0, 0, 2, 1, 2 },    /* 1: gobble unquoted nonspaces */
    { 1, 1, 1, 1, 1, 0 },    /* 2: unquoted backslash */
    { 0, 1, 1, 1, 1, 0 },    /* 3: single quote mode */
    { 1, 0, 0, 1, 1, 0 },    /* 4: double quote mode */
    { 1, 1, 1, 1, 1, 0 } };  /* 5: double quote backslash mode */

static
int
charclass( char input )
{
  if ( input == 0 ) return 5;
  else switch ( input ) {
  case '\'': return 0;
  case '"' : return 1;
  case '\\': return 2;
  case ' ' : return 3;
  case '\t': return 3;
  default: return 4;
  }
}

typedef struct s_node {
  char*           data;
  struct s_node*  next;
} t_node;

static
size_t
interpreteArguments( char* cmd, char*** argv )
/* purpose: removes one layer of quoting and escaping, shell-style
 * paramtr: cmd (IO): commandline to split
 * paramtr: argv (OUT): argv[] vector, newly allocated vector
 * returns: argc 
 */
{
  t_node* head = NULL;
  t_node* tail = NULL;
  char* s = cmd;
  size_t capacity = getpagesize();
  size_t size = 0;
  size_t argc = 0;
  char* store = (char*) malloc( capacity );
  int   class, state = 0;
  char  ch;

  while ( state < 6 ) {
    if ( (class = charclass((ch=*s))) != 5 ) s++;
    if ( debug > 2 ) printf( "[debug state=\"%d\" class=\"%d\" ch=\"%c\"]\n",
			     state, class, ch );

    /* handle action */
    switch ( c_action[state][class] ) {
    case 0: /* noop */
      break;

    case 1: /* save char */
      if ( size+1 >= capacity ) {
	/* need to increate buffer to accomodate longer string */
	size_t c = capacity << 1;
	char* t = (char*) malloc(c);
	memcpy( t, store, size );
	free((void*) store );
	capacity = c;
	store = t;
      }

      /* put character into buffer, and finish the C-string */
      store[size++] = ch;
      store[size] = 0;
      break;

    case 2: /* finalize this argument */
      if ( head == NULL && tail == NULL ) {
	/* initially */
	head = tail = (t_node*) malloc( sizeof(t_node) );
      } else {
	/* later */
	tail->next = (t_node*) malloc( sizeof(t_node) );
	tail = tail->next;
      }

      /* copy string so far into data section, and reset string */
      tail->data = strdup( store );
      tail->next = NULL;
      size = 0;
      store[size] = 0;

      /* counts number of arguments in vector we later must allocate */
      argc++;
      break;

    default: /* must not happen - FIXME: Complain bitterly */
      break;
    }

    /* advance state */
    state = c_state[state][class];
  }

  /* FIXME: What if state is not 6 ? */
  if ( state != 6 ) {
    printf( "[syntax-error state=\"%u\" argc=\"%u\" cmd=\"%s\"]\n",
	    state, argc, cmd );
    free((void*) store);
    return 0;
  }

  /* create result vector from list while freeing list */
  *argv = (char**) calloc( sizeof(char*), argc+1 );
  for ( size=0; head != NULL; ) {
    (*argv)[size++] = head->data;
    tail = head;
    head = head->next;
    free((void*) tail);
  }

  /* finalize argument vector */
  (*argv)[size] = NULL;

  free((void*) store);
  return argc;
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
  if ( s1 == NULL ) return strdup(s2);
  else {
#if 0
    static const char hexdigit[16] = "0123456789ABCDEF";
    char a[80], b[24];
    size_t i;
#endif

    size_t l1 = strlen(s1);
    size_t l2 = strlen(s2);
    size_t len = l1 + l2 + 2;
    char* temp = (char*) malloc(len);
    strncpy( temp, s1, len );
    if ( use_space ) strncat( temp, " ", len );
    strncat( temp, s2, len );

#if 0
    memset( a, 0, sizeof(a) );
    memset( b, 0, sizeof(b) );
    for ( i=0; i<8; ++i ) {
      size_t where = l1+(8>>1) - (8-i);
      b[i] = temp[where];
      a[i*3+0] = hexdigit[ (b[i] >> 4) ];
      a[i*3+1] = hexdigit[ (b[i] & 15) ];
      a[i*3+2] = (where==l1 ? '*' : ' ' );
    }
    fprintf( stderr, "# merge %u+%u=%u: %s %s\n",
	     l1, l2, len, a, b );
#endif

    return temp;
  }
}

static
char*
create_identifier( void )
{
  char buffer[128];

  if ( uname(&uname_cache) != -1 ) {
    char* s = strchr( uname_cache.nodename, '.' );
    if ( s != NULL ) *s = 0;
    snprintf( buffer, sizeof(buffer), "%s:%d", uname_cache.nodename, getpid() );
  } else {
    fprintf( stderr, "uname: %d: %s\n", errno, strerror(errno) );
    memset( &uname_cache, 0, sizeof(uname_cache) );
    snprintf( buffer, sizeof(buffer), "unknown:%d", getpid() );
  }

  return strdup(buffer);
}

static
size_t
append_argument( char* msg, size_t size, size_t len, char* argv[] )
/* purpose: append invocation to logging buffer, but skip kickstart
 * paramtr: msg (IO): initialized buffer to append to
 *          size (IN): capacity of buffer
 *          len (IN): current length of the buffer
 *          argv (IN): invocation argument vector
 * returns: new length of modified buffer
 */
{
  size_t slen;
  int i, flag = 0;
  static const char* ks = "kickstart";
  char* extra[3] = { NULL, NULL, NULL };

  for ( i=0 ; argv[i]; ++i ) { 
    char* s = argv[i];
    slen = strlen(s);

    /* detect presence of kickstart */
    if ( i == 0 && strcmp( s+slen-strlen(ks), ks ) == 0 ) {
      flag = 1;
      continue;
    }

    if ( flag ) {
      /* in kickstart mode, skip options of kickstart */
      if ( s[0] == '-' && strchr( "ioelnNRBLTIwWSs", s[1] ) != NULL ) {
	/* option with argument */
	switch ( s[1] ) {
	case 'i':
	  if ( s[2] == 0 ) extra[0] = argv[++i];
	  else extra[0] = &s[2];
	  break;
	case 'o':
	  if ( s[2] == 0 ) extra[1] = argv[++i];
	  else extra[1] = &s[2];
	  break;
	case 'e':
	  if ( s[2] == 0 ) extra[2] = argv[++i];
	  else extra[2] = &s[2];
	  break;
	default:
	  if ( s[2] == 0 ) ++i;
	  break;
	}
	continue;
      } else if ( s[0] == '-' && strchr( "HVX", s[1] ) != NULL ) {
	/* option without argument */
	continue;
      } else {
	flag = 0;
      }
    }

    if ( ! flag ) {
      /* in regular mode, add argument to output */
      if ( len + slen + 1 > size ) {
	/* continuation dots */
	static const char* dots = " ...";
	if ( len < size-strlen(dots)-1 ) { 
	  strncat( msg+len, dots, size-len );
	  len += strlen(dots);
	}
	break;
      }

      /* append argument */
      strncat( msg+len, " ", size-len );
      strncat( msg+len, s, size-len );
      len += slen + 1; 
    }
  }

  /* simulate stdio redirection */
  for ( i=0; i<3; ++i ) {
    if ( extra[i] != NULL ) {
      int skip = 0;
      char* s = extra[i];
      if ( len + (slen=strlen(s)) + 4 < size ) {
	switch ( i ) {
	case 0:
	  strncat( msg+len, " < ", size-len );
	  break;
	case 1:
	  strncat( msg+len, " > ", size-len );
	  break;
	case 2: 
	  strncat( msg+len, " 2> ", size-len );
	  break;
	}
        skip = ( *s == '!' || *s == '^' );
	strncat( msg+len, s+skip, size-len );
	len += slen + 3 + ( i == 2 );
      } else {
	break;
      }
    }
  }

  return len;
}

static
int
lockit( int fd, short cmd, short type )
/* purpose: fill in POSIX lock structure and attempt lock (or unlock)
 * paramtr: fd (IN): which file descriptor to lock
 *          cmd (IN): F_SETLK, F_GETLK, F_SETLKW
 *          type (IN): F_WRLCK, F_RDLCK, F_UNLCK
 * returns: result from fcntl call
 */
{
  struct flock l;

  memset( &l, 0, sizeof(l) );
  l.l_type = type;

  /* full file */
  l.l_whence = SEEK_SET;
  l.l_start = 0;
  l.l_len = 0;

  /* run it */
  return fcntl( fd, cmd, &l );
}

static
int
mytrylock( int fd )
/* purpose: Try to lock the file
 * paramtr: fd (IN): open file descriptor
 * returns: -1: fatal error while locking the file, file not locked
 *           0: all backoff attempts failed, file is not locked
 *           1: file is locked
 */
{
  int backoff = 50; /* milliseconds, increasing */
  int retries = 10; /* 2.2 seconds total */
  while ( lockit( fd, F_SETLK, F_WRLCK ) == -1 ) {
    if ( errno != EACCES && errno != EAGAIN ) return -1;
    if ( --retries == 0 ) return 0;
    backoff += 50;
    poll( NULL, 0, backoff );
  }

  return 1;
}

static
ssize_t
report( time_t start, double duration,
	int status, char* argv[], struct rusage* use )
{
  static unsigned long counter = 0;
  int save, locked;
  char date[32];
  size_t len, size = getpagesize();
  char* msg = (char*) malloc( size<<1 );
  ssize_t wsize = -1;

  /* sanity checks */
  if ( progress == -1 || argv == NULL ) return 0;

  /* message start */
  if ( status == -1 && duration == 0.0 && use == NULL ) {
    /* report of seqexec itself */
    snprintf( msg, size, "%s %s %lu 0/0 START", 
	      isodate(start,date,sizeof(date)), identifier, counter++ );
  } else {
    /* report from child invocations */
    snprintf( msg, size, "%s %s %lu %d/%d %.3f",
	      isodate(start,date,sizeof(date)), identifier, counter++,
	      (status >> 8), (status & 127), duration );
  }

  /* add program arguments */
  len = append_argument( msg, size-2, strlen(msg), argv );

  /* optionally add uname (seqexec) or rusage (children) info */
  if ( status == -1 && duration == 0.0 && use == NULL ) {
    /* report uname info for seqexec itself */
    snprintf( msg+len, size-len,
	      " ### sysname=%s machine=%s release=%s",
	      uname_cache.sysname, uname_cache.machine, uname_cache.release );
    len += strlen(msg+len);
  } else if ( use != NULL ) {
    double utime = use->ru_utime.tv_sec + use->ru_utime.tv_usec / 1E6;
    double stime = use->ru_stime.tv_sec + use->ru_stime.tv_usec / 1E6;
    snprintf( msg+len, size-len, 
	      " ### utime=%.3f stime=%.3f minflt=%ld majflt=%ld"
#ifndef linux
	      /* Linux is broken and does not fill in these values */
	      " maxrss=%ld idrss=%ld inblock=%ld oublock=%ld"
	      " nswap=%ld nsignals=%ld nvcws=%ld nivcsw=%ld"
#endif
	      ,utime, stime, use->ru_minflt, use->ru_majflt
#ifndef linux
	      /* Linux is broken and does not fill in these values */
	      ,use->ru_maxrss, use->ru_idrss, use->ru_inblock, use->ru_oublock,
	      use->ru_nswap, use->ru_nsignals, use->ru_nvcsw, use->ru_nivcsw
#endif
	      );
    len += strlen(msg+len);
  }

  /* terminate line */
  strncat( msg+len, "\n", size-len );

  /* Atomic append -- will still garble on Linux NFS */
  /* Warning: Fcntl-locking may block in syscall on broken Linux kernels */
  locked = mytrylock( progress );
  wsize = write( progress, msg, len+1 ); 
  save = errno;
  if ( locked==1 ) lockit( progress, F_SETLK, F_UNLCK );

  free((void*) msg );
  errno = save;
  return wsize;
}

static 
int
mysystem( char* argv[], char* envp[] )
{
  char date[32];
  struct rusage usage;
  struct sigaction ignore, saveintr, savequit;
  sigset_t childmask, savemask;
  pid_t child;
  time_t when, then;
  double diff, start = now(&when);
  int saverr = 0;
  int status = -1;

  ignore.sa_handler = SIG_IGN;
  sigemptyset( &ignore.sa_mask );
  ignore.sa_flags = 0;
  
  if ( sigaction( SIGINT, &ignore, &saveintr ) < 0 )
    return -1;
  if ( sigaction( SIGQUIT, &ignore, &savequit ) < 0 )
    return -1;

  sigemptyset( &childmask );
  sigaddset( &childmask, SIGCHLD );
  memset( &usage, 0, sizeof(usage) );
  if ( sigprocmask( SIG_BLOCK, &childmask, &savemask ) < 0 )
    return -1;

  if ( (child=fork()) < 0 ) {
    /* no more process table space */
    return -1;
  } else if ( child == (pid_t) 0 ) {
    /* child */
    int null = open( "/dev/null", O_RDONLY );
    if ( null != -1 ) { 
      if ( dup2( null, STDIN_FILENO ) == -1 && debug )
	showerr( "%s: dup2 stdin: %d: %s\n",
		 application, errno, strerror(errno) );
    } else {
      if ( debug ) 
	showerr( "%s: open /dev/null: %d: %s\n", 
		 application, errno, strerror(errno) );
    }

    /* undo signal handlers */
    sigaction( SIGINT, &saveintr, NULL );
    sigaction( SIGQUIT, &savequit, NULL );
    sigprocmask( SIG_SETMASK, &savemask, NULL );

    execve( argv[0], (char* const*) argv, envp );
    showerr( "%s: exec %s: %d: %s\n", 
	     application, argv[0], errno, strerror(errno) );
    _exit(127); /* never reached unless error */
  } else {
    /* parent */
    
    /* wait for child */
    while ( wait4( child, &status, 0, &usage ) < 0 ) {
      if ( errno != EINTR ) {
	status = -1;
	break;
      }
    }
    
    /* remember why/how we did exit */
    saverr = errno;
    
    /* sanity check */
    if ( kill( 0, child ) == 0 )
      showerr( "Warning: job %d (%s) is still running!\n", 
	       child, argv[0] );
  }

  /* ignore errors on these, too. */
  sigaction( SIGINT, &saveintr, NULL );
  sigaction( SIGQUIT, &savequit, NULL );
  sigprocmask( SIG_SETMASK, &savemask, NULL );

  /* say hi */
  diff = now(&then) - start;
  if ( debug > 1 ) {
    printf( "<job app=\"%s\" start=\"%s\" duration=\"%.3f\" status=\"%d\"/>\n",
	    argv[0], isodate(when,date,sizeof(date)), diff, status );
  }

  /* progress report finish */
  if ( progress != -1 ) report( when, diff, status, argv, &usage );

  errno = saverr;
  return status;
}

int
main( int argc, char* argv[], char* envp[] )
{
  size_t len;
  char line[MAXSTR];
  int appc, status = 0;
  int fail_hard = 0;
  char* cmd;
  char** appv;

  char* save = NULL;
  unsigned long total = 0;
  unsigned long failure = 0;
  unsigned long lineno = 0;
  time_t when;
  double diff, start = now(&when);
  parseCommandline( argc, argv, &fail_hard );

  /* progress report finish */
  identifier = create_identifier();
  if ( progress != -1 ) report( time(NULL), 0.0, -1, argv, NULL );

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
      if ( (status = mysystem( appv, envp )) ) failure++;
      /* free resource -- we must free argv[] elements */
      for ( len=0; len<appc; len++ ) free((void*) appv[len]);
      free((void*) appv);
    }

    if ( cmd != line ) free((void*) cmd);

    /* fail hard mode, if requested */
    if ( fail_hard && status ) break;
  }

  /* provide final statistics */
  diff = now(NULL) - start;
  printf( "[struct stat=\"OK\", lines=%lu, count=%lu, failed=%lu, "
	  "duration=%.3f, start=\"%s\"]\n",
	  lineno, total, failure, diff, isodate(when,line,sizeof(line)) );

  fflush(stdout);
  exit( (fail_hard && status) ? 5 : 0 );
}
