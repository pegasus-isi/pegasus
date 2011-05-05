#include <sys/types.h>
#include <ctype.h>
#include <errno.h>
#include <math.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#include <sys/utsname.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/ioctl.h>
#include <net/if.h>
#include <netdb.h>

#ifdef HAS_SYS_SOCKIO
#include <sys/sockio.h>
#endif

#ifdef MACHINE_SPECIFIC
#ifdef DARWIN
#include "darwin.hh"
#endif // DARWIN

#if defined(SUNOS) || defined(SOLARIS)
#include "sunos.hh"
#endif // SUNOS || SOLARIS

#ifdef LINUX
#include "basic.hh"
#include "linux.hh"
#endif // LINUX
#endif // MACHINE_SPECIFIC

static char output[4096];
static char pattern[] = 
"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz\r\n";

class DirtyVector {
  // using malloc/free instead of new/delete to avoid linking
  // in new libgcc_s and libstdc++
public:
  typedef const char* CharP;

  DirtyVector()
    :m_save(0),m_size(0),m_capacity(0) 
  { }

  ~DirtyVector()
  { if ( m_capacity ) free( static_cast<void*>(m_save) ); }

  size_t size() const
  { return m_size; }

  const char* operator[]( unsigned i ) const
  { return ( i <= m_size ? m_save[i] : 0 ); }

  const char* at( unsigned i ) const
  { return ( i <= m_size ? m_save[i] : 0 ); }

  void push_back( const char* s ) 
  {
    if ( m_size+1 >= m_capacity ) {
      if ( m_capacity ) m_capacity <<= 1;
      else m_capacity = 5;
      CharP* newsave = static_cast<CharP*>( malloc(m_capacity*sizeof(CharP)) );
      memset( newsave, 0, m_capacity*sizeof(CharP) );
      memcpy( newsave, m_save, m_size*sizeof(CharP) );
      free( static_cast<void*>(m_save) );
      m_save = newsave;
    }
    m_save[m_size] = s;
    m_size++;
  }

private:
  CharP*   m_save;
  size_t   m_size;
  size_t   m_capacity;
};

double
now( void )
  // purpose: determine the current time in microsecond resolution
  // returns: the current timestamp as seconds w/ us fraction, or -1.0
{
  int timeout = 0;
  struct timeval now = { -1, 0 };
  while ( gettimeofday( &now, 0 ) == -1 && timeout < 10 ) timeout++;
  return ( now.tv_sec + now.tv_usec / 1E6 );
}

template <class T> 
inline 
T 
sqr( T x ) 
  // purpose: function to calculate the square
  // paramtr: x (IN): value to square
  // returns: the square of the value
  // warning: might run into exception, if x is too large
{ 
  return x*x; 
}

size_t
fractal( double x, double y, double a, double b, size_t max )
  // purpose: calculate z := z^2 + c repeatedly
  // paramtr: x (IN): real part of z
  //          y (IN): imag part of z
  //          a (IN): real part of c
  //          b (IN): imag part of c
  //          max (IN): maximum number of iterations
  // returns: iterations until |z| >= 2.0 or maximum iterations reached
  // abbrev1: since |z| = sqrt(x^2 + y^2) squaring both sides of |z| >= 2.0
  //          will result in x^2 + y^2 >= 4.0
  // abbrev2: z := z^2 + c = (x^2 - y^2 + a) + i(2xy + b)
  //          squares of x and y are kept for the loop condition
{
  double xx; // temp
  double qx = sqr(x);
  double qy = sqr(y);
  size_t n = 0;
  
  for ( ; n < max && qx+qy < 4.0; ++n ) {
    xx = qx - qy + a;
    qy = sqr((y=2.0 * x * y + b));
    qx = sqr((x=xx));
  }

  return n;
}

unsigned long
spin( unsigned long interval )
{
  double stop = now() + interval;
  unsigned long count = 0;

  seed48( (unsigned short*) ((void*) &stop) );
  double julia_x = 1.0-2.0*drand48();
  double julia_y = 1.0-2.0*drand48();
  do {
    for ( int i=0; i<16; ++i )
      fractal( 1.0-2.0*drand48(), 1.0-2.0*drand48(), julia_x, julia_y, 1024 );
    ++count;
  } while ( now() < stop );

  return count;
}

char*
append( char* buffer, size_t capacity, const char* fmt, ... )
{
  va_list ap;
  char line[4096];	// many systems don't like alloca()
  va_start( ap, fmt );
  vsnprintf( line, 4096-4, fmt, ap );
  va_end(ap);
  return strncat( buffer, line, capacity );
}

char*
append( char* buffer, size_t capacity, char ch )
{
  size_t size = strlen(buffer);
  if ( size+1 < capacity ) {
    buffer[size] = ch;
    buffer[size+1] = '\0';
  }
  return buffer;
}

static
int
debug( const char* fmt, ... )
{
  int result;
  va_list ap;
  char buffer[4096];
  int saverr = errno;

  va_start( ap, fmt );
  vsnprintf( buffer, sizeof(buffer), fmt, ap );
  va_end( ap );

  result = write( STDERR_FILENO, buffer, strlen(buffer) );
  errno = saverr;
  return result;
}


 static unsigned long vpn_network[5] = { 0, 0, 0, 0, 0 };
 static unsigned long vpn_netmask[5] = { 0, 0, 0, 0, 0 };

static
void
singleton_init( void )
{
  /* singleton init */
  if ( vpn_network[0] == 0ul ) {
    vpn_network[0] = inet_addr("127.0.0.0");   /* loopbacknet */
    vpn_network[1] = inet_addr("10.0.0.0");    /* class A VPN net */
    vpn_network[2] = inet_addr("172.16.0.0");  /* class B VPN nets */
    vpn_network[3] = inet_addr("192.168.0.0"); /* class C VPN nets */
    vpn_network[4] = inet_addr("169.254.0.0"); /* link-local M$ junk */
  }

  /* singleton init */
  if ( vpn_netmask[0] == 0ul ) {
    vpn_netmask[0] = inet_addr("255.0.0.0");   /* loopbackmask */
    vpn_netmask[1] = inet_addr("255.0.0.0");   /* class A mask */
    vpn_netmask[2] = inet_addr("255.240.0.0"); /* class B VPN mask */
    vpn_netmask[3] = inet_addr("255.255.0.0"); /* class C VPN mask */
    vpn_netmask[4] = inet_addr("255.254.0.0"); /* link-local M$ junk */ 
  }
}

struct ifreq*
primary_interface( void )
/* purpose: obtain the primary interface information
 * returns: a newly-allocated structure containing the interface info,
 *          or NULL to indicate an error. 
 */
{
#if defined(SIOCGLIFNUM)
  struct lifnum ifnr;
#endif
  struct sockaddr_in sa;
  struct ifconf ifc;
  struct ifreq  result, primary;
  struct ifreq* ifrcopy = NULL;
  char *ptr, *buf = 0;
  int lastlen, len, sockfd, flag = 0;

  /*
   * phase 0: init
   */
  memset( &result, 0, sizeof(result) );
  memset( &primary, 0, sizeof(primary) );
  singleton_init();

  /* create a socket */
  if ( (sockfd = socket( AF_INET, SOCK_DGRAM, 0 )) == -1 ) { 
    debug( "ERROR: socket DGRAM: %d: %s\n", errno, strerror(errno) );
    return ifrcopy;
  }

  /*
   * phase 1: guestimate size of buffer necessary to contain all interface
   * information records. 
   */
#if defined(SIOCGLIFNUM)
  /* API exists to determine the correct buffer size */
  memset( &ifnr, 0, sizeof(ifnr) );
  ifnr.lifn_family = AF_INET;
  if ( ioctl( sockfd, SIOCGLIFNUM, &ifnr ) < 0 ) {
    debug( "ERROR: ioctl SIOCGLIFNUM: %d: %s\n", errno, strerror(errno) );

    if ( errno != EINVAL ) {
      close(sockfd);
      return ifrcopy;
    }
  } else {
    len = lastlen = ifnr.lifn_count * sizeof(struct ifreq);
  }
#else /* does not have SIOCGLIFNUM */
  /* determine by repetitive guessing a buffer size */
  lastlen = len = 4 * sizeof(struct ifreq); /* 1st guesstimate */
#endif
  /* POST CONDITION: some buffer size determined */

  /* FIXME: Missing upper bound */
  for (;;) {
    /* guestimate correct buffer length */
    buf = (char*) malloc(len);
    memset( buf, 0, len );
    ifc.ifc_len = len;
    ifc.ifc_buf = buf;
    if ( ioctl( sockfd, SIOCGIFCONF, &ifc ) < 0 ) {
      debug( "WARN: ioctl SIOCGIFCONF: %d: %s\n", errno, strerror(errno) );
      if ( errno != EINVAL || lastlen != 0 ) {
	close(sockfd);
	return ifrcopy;
      }
    } else {
      if ( ifc.ifc_len == lastlen ) break; /* success */
      lastlen = ifc.ifc_len;
    }
    len <<= 1;
    free((void*) buf);
  }
  /* POST CONDITION: Now the buffer contains list of all interfaces */

  /*
   * phase 2: walk interface list until a good interface is reached
   */ 
  /* Notice: recycle meaning of "len" in here */
  for ( ptr = buf; ptr < buf + ifc.ifc_len; ) {
    struct ifreq* ifr = (struct ifreq*) ptr;
    len = sizeof(*ifr);
    ptr += len;

    /* interested in IPv4 interfaces only */
    if ( ifr->ifr_addr.sa_family != AF_INET )
      continue;

    memcpy( &sa, &(ifr->ifr_addr), sizeof(struct sockaddr_in) );

    /* Do not use localhost aka loopback interfaces. While loopback
     * interfaces traditionally start with "lo", this is not mandatory.
     * It is safer to check that the address is in the 127.0.0.0 class A
     * network. */
    if ( (sa.sin_addr.s_addr & vpn_netmask[0]) == vpn_network[0] )
      continue;

    /* prime candidate - check, if interface is UP */
    result = *ifr;
    ioctl( sockfd, SIOCGIFFLAGS, &result );

    /* interface is up - our work is done. Or is it? */
    if ( (result.ifr_flags & IFF_UP) ) {
      if ( ! flag ) {
	/* remember first found primary interface */
	primary = result;
	flag = 1;
      }

      /* check for VPNs */
      if ( (sa.sin_addr.s_addr & vpn_netmask[1]) != vpn_network[1] &&
	   (sa.sin_addr.s_addr & vpn_netmask[2]) != vpn_network[2] &&
	   (sa.sin_addr.s_addr & vpn_netmask[3]) != vpn_network[3] ) {
	flag = 2;
	break;
      }
    }
  }

  /* check for loop exceeded - if yes, fall back on first primary */
  if ( flag == 1 && ptr >= buf + ifc.ifc_len ) result = primary;

  /* clean up */
  free((void*) buf);
  close(sockfd);

  /* create a freshly allocated copy */
  ifrcopy = (struct ifreq*) malloc( sizeof(struct ifreq) );
  memcpy( ifrcopy, &result, sizeof(struct ifreq) );
  return ifrcopy;
}

struct in_addr
whoami( char* buffer, size_t size )
/* purpose: copy the primary interface's IPv4 dotted quad into the given buffer
 * paramtr: buffer (IO): start of buffer
 *          size (IN): maximum capacity the buffer is willing to accept
 * returns: the modified buffer. */
{
  /* enumerate interfaces, and guess primary one */
  struct sockaddr_in sa;
  struct ifreq* ifr = primary_interface();
  if ( ifr != NULL ) {
    memcpy( &sa, &(ifr->ifr_addr), sizeof(struct sockaddr) );
    strncpy( buffer, inet_ntoa(sa.sin_addr), size );
    free((void*) ifr);
  } else {
    /* error while trying to determine address of primary interface */
    memset( &sa, 0, sizeof(sa) );
#if 0
    /* future lab */
    strncpy( buffer, "xsi:null", size );
#else
    /* for now */
    strncpy( buffer, "0.0.0.0", size );
#endif
  }
  return sa.sin_addr;
}

void
identify( char* result, size_t size, const char* arg0,
	  double start, bool condor,
	  const DirtyVector iox[4], const char* outfn )
{
  size_t linsize = getpagesize();
  char* line = static_cast<char*>( malloc(linsize) );

  // phase 2: where am i
  struct utsname uts;
  char* release = const_cast<char*>("");
  char* machine = const_cast<char*>("");
  char* sysname = const_cast<char*>("");
  char* hostname = getenv("HOSTNAME");
  if ( uname(&uts) >= 0 ) {
    release = strdup( uts.release );
    machine = strdup( uts.machine );
    sysname = strdup( uts.sysname );
    if ( hostname == 0 ) hostname = uts.nodename;
  }

  // who am i
  char ifaddr[20];
  struct in_addr me = whoami( ifaddr, sizeof(ifaddr) );

  singleton_init();
  if ( (me.s_addr & vpn_netmask[1]) == vpn_network[1] || 
       (me.s_addr & vpn_netmask[2]) == vpn_network[2] ||
       (me.s_addr & vpn_netmask[3]) == vpn_network[3] ) {
    // private network, no loopkup
    hostname = static_cast<char*>( malloc(32) );
    sprintf( hostname, "%s (VPN)", ifaddr );
  } else {
    struct hostent* h = ( me.s_addr == 0ul || me.s_addr == 0xFFFFFFFFul ) ?
      gethostbyname(hostname) : 
      gethostbyaddr( (const char*) &me.s_addr, sizeof(me.s_addr), AF_INET );

    if ( h ) {
      struct in_addr ipv4;
      hostname = static_cast<char*>( malloc(strlen(h->h_name)+20) );
      memcpy( &ipv4.s_addr, h->h_addr, 4 );
      sprintf( hostname, "%s (%s)", inet_ntoa(ipv4), h->h_name );
    } else {
      hostname = static_cast<char*>( malloc(strlen(ifaddr)+2) );
      strcpy( hostname, ifaddr );
    }
  }

  // timestamp stuff
  double integral, that = now();
  modf( start, &integral );
  time_t intint = (time_t) integral;
  
  // determine timezone offset
  struct tm tm0, tm1;
  memcpy( &tm0, gmtime(&intint), sizeof(struct tm) );
  memcpy( &tm1, localtime(&intint), sizeof(struct tm) );
  tm0.tm_isdst = tm1.tm_isdst;
  time_t offset = intint - mktime(&tm0);
  int hours = offset / 3600;
#if defined (__GNUC__)
  int minutes = abs(offset) % 60;
#else
  // Solaris has overloading ambiguity between std::abs(int|double) problems
  int minutes = ( offset < 0 ? -offset : offset ) % 60;
#endif
  
  // time stamp ISO format
  strftime( line, linsize, "%Y%m%dT%H%M%S", &tm1 );
  char ms[8]; 
  snprintf( ms, sizeof(ms), "%.3f", start - floor(start) ); 
  append( result, size, "Timestamp Today: %s%s%+03d:%02d (%.3f;%.3f)\n", 
	  line, ms+1, hours, minutes, start, that-start );

  // phase 1: Say hi
#ifdef HAS_SVNVERSION
  append( result, size, "Applicationname: %s [%s] @ %s\n", 
	  arg0, HAS_SVNVERSION, hostname );
#else
  append( result, size, "Applicationname: %s @ %s\n", arg0, hostname );
#endif // HAS_SVNVERSION

  if ( getcwd( line, linsize ) == 0 ) strcpy( line, "(n.a.)" );
  append( result, size, "Current Workdir: %s\n", line );

  // phase 2: this machine?
  append( result, size, "Systemenvironm.: %s-%s %s\n", machine, sysname, release );
#ifdef MACHINE_SPECIFIC
  pegasus_cpuinfo( result, size ); 
  pegasus_loadavg( result, size ); 
  pegasus_meminfo( result, size ); 
  pegasus_statfs( result, size ); 
#endif // MACHINE_SPECIFIC

  if ( condor ) { 
    for ( char** p = environ; *p; p++ ) {
      if ( strncmp( *p, "_CONDOR", 7 ) == 0 ) {
	append( result, size, "Condor Variable: %s\n", *p ); 
      }
    }
  }

  append( result, size, "Output Filename: %s\n", outfn );
  if ( iox[1].size() ) {
    append( result, size, "Input Filenames:" );
    for ( unsigned j=0; j<iox[1].size(); ++j ) {
      append( result, size, ' ' );
      strncat( result, iox[1][j], size );
    }
    append( result, size, '\n' );
  }

  if ( iox[0].size() ) {
    append( result, size, "Other Arguments:" );
    for ( unsigned j=0; j<iox[0].size(); ++j ) {
      append( result, size, ' ' );
      strncat( result, iox[0][j], size );
    }
    append( result, size, '\n' );
  }
  
  for ( unsigned j=0; j<iox[3].size(); ++j ) {
    append( result, size, "Environmentvar.: %s=", iox[3][j] );
    char* e = getenv(iox[3][j]);
    if ( e && *e ) strncat( result, e, size );
    append( result, size, '\n' );
  }

  free( static_cast<void*>(line) );
}

void
helpMe( const char* ptr, unsigned long timeout, unsigned long spinout, 
	const char* prefix )
{
  printf( "Usage\t%s [-a appname] [(-t|-T) thinktime] [-l fn] [-o fn [..]]\n"
	  "\t[-i fn [..] | -G size] [-e env [..]] [-p p [..]] [-P ps]\n",
	  ptr );
  printf( " -a app\tset name of application to something else, default %s\n", ptr );
  printf( " -t to\tsleep for 'to' seconds during execution, default %lu\n", timeout );
  printf( " -T to\tspin for 'to' seconds during execution, default %lu\n", spinout );
  printf( " -l fn\tappend own information atomically to a logfile\n" );
  printf( " -o ..\tenumerate space-separated list output files to create\n" );
  printf( " -i ..\tenumerate space-separated list input to read and copy\n" );
  printf( " -G sz\tuse the generated size pattern instead of input files\n" );
  printf( " -p ..\tenumerate space-separated parameters to mention\n" );
  printf( " -e ..\tenumerate space-separated environment values to print\n" );
  printf( " -C\tprint all environment variables starting with _CONDOR\n" ); 
  printf( " -P ps\tprefix input file lines with 'ps', default \"%s\"\n", prefix );
}

int
main( int argc, char* argv[] ) 
{
  int state = 0;
  bool condor = false; 
  unsigned long timeout = 0;
  unsigned long spinout = 0;
  unsigned long gensize = 0;
  DirtyVector iox[4];

  // when did we start
  double start = now();
  char* prefix = strdup("  ");

  // determine base name of input file
  char* logfile = 0;
  char* ptr = 0;
  if ( (ptr=strrchr(argv[0],'/')) == 0 ) ptr = argv[0];
  else ptr++;

  // complain, if no parameters were given
  if ( argc == 1 ) {
    helpMe( ptr, timeout, spinout, prefix ); 
    return 0;
  }
  
  // prepare generator pattern
  for ( size_t i=0; i<sizeof(output); i++ ) output[i] = pattern[i & 63];
  
  for ( int i=1; i<argc; ++i ) {
    char* s = argv[i];
    if ( s[0] == '-' && s[1] != 0 ) {
      if ( strchr( "iotTGaepPlC\0", s[1] ) != NULL ) {
	switch (s[1]) {
	case 'i':
	  state = 1;
	  break;
	case 'o':
	  state = 2;
	  break;
	case 'e':
	  state = 3;
	  break;
	case 'a':
	  state = 10;
	  break;
	case 't':
	  state = 11;
	  break;
	case 'l':
	  state = 12;
	  break;
	case 'T':
	  state = 13;
	  break;
	case 'p':
	  state = 0;
	  break;
	case 'P':
	  state = 14;
	  break;
	case 'G':
	  state = 15;
	  break;
	case 'C':
	  condor = true;
	  continue; 
	}
	s += 2;
      }
    }
    if ( strlen(s) == 0 ) continue;
    if ( state >= 10 ) {
      switch ( state ) {
      case 10:
	ptr = s;
	break;
      case 11:
	timeout = strtoul(s,0,10);
	break;
      case 12:
	logfile = s;
	break;
      case 13:
	spinout = strtoul(s,0,10);
	break;
      case 14:
	free( static_cast<void*>(prefix) );
	prefix = strdup(s);
	break;
      case 15:
	gensize = strtoul(s,0,10);
	break;
      }
      state = 0;
    } else {
      iox[state].push_back(s);
    }
  }

  // thinktime
  if ( timeout ) sleep(timeout);
  if ( spinout ) spin(spinout);

  // output buffer
  size_t bufsize = getpagesize() << 4;
  if ( bufsize < 16384 ) bufsize = 16384;
  char* buffer = static_cast<char*>( malloc(bufsize) );

#ifndef MIN
#define MIN(a,b) ((a) < (b) ? (a) : (b))
#endif // MIN

  // all input, all output files
  FILE *out, *in;
  for ( unsigned i=0; i<iox[2].size(); ++i ) {
    out = ( iox[2][i][0] == '-' && iox[2][i][1] == '\0' ) ? 
      fdopen( STDOUT_FILENO, "a" ) :
      fopen( iox[2][i], "w" );
    if ( out ) {
      if ( gensize > 0 ) {
	unsigned long xsize = gensize-1; // final LF counts
	while ( xsize > 0 ) {
	  ssize_t wsize = fwrite( output, sizeof(char),
				  MIN(xsize,sizeof(output)), out );
	  if ( wsize > 0 ) xsize -= wsize;
	  else break;
	}
	fputc( '\n', out );
      } else {
	// copy input files
	for ( unsigned j=0; j<iox[1].size(); ++j ) {
	  in = ( iox[1][j][0] == '-' && iox[1][j][1] == '\0' ) ? 
	    fdopen( STDIN_FILENO, "r" ) : 
	    fopen( iox[1][j], "r" );
	  fprintf( out, "--- start %s ----\n", iox[1][j] );
	  if ( in ) {
	    while ( fgets( buffer, bufsize, in ) ) {
	      fputs( prefix, out );
	      fputs( buffer, out );
	    }
	    fclose(in);
	  } else {
	    fprintf( out, "  open \"%s\": %d: %s\n", iox[1][j], 
		     errno, strerror(errno) );
	  }
	  fprintf( out, "--- final %s ----\n", iox[1][j] );
	}
      }

      // create buffer, and fill with content
      memset( buffer, 0, bufsize );
      identify( buffer, bufsize, ptr, start, condor, iox, iox[2][i] );
      fputs( buffer, out );
      fclose(out);
    } else {
      fprintf( stderr, "open(%s): %s\n", iox[2][i], strerror(errno) );
      free( static_cast<void*>(buffer) );
      return 2;
    }
  }

  // append atomically to logfile
  if ( logfile != 0 ) {
    int fd = -1;
    if ( (fd=open( logfile, O_WRONLY | O_CREAT | O_APPEND, 0666 )) == -1 ) {
      fprintf( stderr, "WARNING: open(%s): %s\n", logfile, strerror(errno) );
    } else {
      memset( buffer, 0, bufsize );
      identify( buffer, bufsize, ptr, start, condor, iox, logfile );
      append( buffer, bufsize, '\n' );
      write( fd, buffer, strlen(buffer) ); // atomic write
      close(fd);
    }
  }

  free( static_cast<void*>(buffer) );
  return 0;
}
