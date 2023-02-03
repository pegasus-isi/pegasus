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
#include <sys/mman.h>

#ifdef HAS_SYS_SOCKIO
#include <sys/sockio.h>
#endif

#ifdef MACHINE_SPECIFIC
#ifdef DARWIN
#include "darwin.hh"
extern char **environ;
#endif // DARWIN

#if defined(SUNOS) || defined(SOLARIS)
#include "sunos.hh"
#endif // SUNOS || SOLARIS

#ifdef LINUX
#include "basic.hh"
#include "linux.hh"
#endif // LINUX

#ifdef GNUKFREEBSD
#include "basic.hh"
#include "linux.hh"
#endif // GNUKFREEBSD
#endif // MACHINE_SPECIFIC

#include "version.h"

#ifdef WITH_MPI
#include <mpi.h>
#endif

static char output[4096];
static char pattern[] =
    "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz\r\n";

class DirtyVector
{
    // using malloc/free instead of new/delete to avoid linking
    // in new libgcc_s and libstdc++
public:
    typedef const char *CharP;

    DirtyVector()
        : m_save(0), m_size(0), m_capacity(0)
    { }

    ~DirtyVector()
    {
        if ( m_capacity ) free( static_cast<void *>(m_save) );
    }

    size_t size() const
    {
        return m_size;
    }

    const char *operator[]( unsigned i ) const
    {
        return ( i <= m_size ? m_save[i] : 0 );
    }

    const char *at( unsigned i ) const
    {
        return ( i <= m_size ? m_save[i] : 0 );
    }

    void push_back( const char *s )
    {
        if ( m_size + 1 >= m_capacity )
        {
            if ( m_capacity ) m_capacity <<= 1;
            else m_capacity = 5;
            CharP *newsave = static_cast<CharP *>( malloc(m_capacity * sizeof(CharP)) );
            memset( newsave, 0, m_capacity * sizeof(CharP) );
            memcpy( newsave, m_save, m_size * sizeof(CharP) );
            free( static_cast<void *>(m_save) );
            m_save = newsave;
        }
        m_save[m_size] = s;
        m_size++;
    }

private:
    CharP   *m_save;
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
    return x * x;
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

    for ( ; n < max && qx + qy < 4.0; ++n )
    {
        xx = qx - qy + a;
        qy = sqr((y = 2.0 * x * y + b));
        qx = sqr((x = xx));
    }

    return n;
}

unsigned long
spin( unsigned long interval )
{
    double stop = now() + interval;
    unsigned long count = 0;

    seed48( (unsigned short *) ((void *) &stop) );
    double julia_x = 1.0 - 2.0 * drand48();
    double julia_y = 1.0 - 2.0 * drand48();
    do
    {
        for ( int i = 0; i < 16; ++i )
            fractal( 1.0 - 2.0 * drand48(), 1.0 - 2.0 * drand48(), julia_x, julia_y, 1024 );
        ++count;
    }
    while ( now() < stop );

    return count;
}

char *
append( char *buffer, size_t capacity, const char *fmt, ... )
{
    va_list ap;
    char line[4096];  // many systems don't like alloca()
    va_start( ap, fmt );
    vsnprintf( line, 4096 - 4, fmt, ap );
    va_end(ap);
    return strncat( buffer, line, capacity );
}

char *
append( char *buffer, size_t capacity, char ch )
{
    size_t size = strlen(buffer);
    if ( size + 1 < capacity )
    {
        buffer[size] = ch;
        buffer[size + 1] = '\0';
    }
    return buffer;
}

static
int
debug( const char *fmt, ... )
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
    if ( vpn_network[0] == 0ul )
    {
        vpn_network[0] = inet_addr("127.0.0.0");   /* loopbacknet */
        vpn_network[1] = inet_addr("10.0.0.0");    /* class A VPN net */
        vpn_network[2] = inet_addr("172.16.0.0");  /* class B VPN nets */
        vpn_network[3] = inet_addr("192.168.0.0"); /* class C VPN nets */
        vpn_network[4] = inet_addr("169.254.0.0"); /* link-local M$ junk */
    }

    /* singleton init */
    if ( vpn_netmask[0] == 0ul )
    {
        vpn_netmask[0] = inet_addr("255.0.0.0");   /* loopbackmask */
        vpn_netmask[1] = inet_addr("255.0.0.0");   /* class A mask */
        vpn_netmask[2] = inet_addr("255.240.0.0"); /* class B VPN mask */
        vpn_netmask[3] = inet_addr("255.255.0.0"); /* class C VPN mask */
        vpn_netmask[4] = inet_addr("255.254.0.0"); /* link-local M$ junk */
    }
}

struct ifreq *
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
    struct ifreq *ifrcopy = NULL;
    char *ptr, *buf = 0;
    int lastlen, len, sockfd, flag = 0;

    /*
     * phase 0: init
     */
    memset( &result, 0, sizeof(result) );
    memset( &primary, 0, sizeof(primary) );
    singleton_init();

    /* create a socket */
    if ( (sockfd = socket( AF_INET, SOCK_DGRAM, 0 )) == -1 )
    {
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
    if ( ioctl( sockfd, SIOCGLIFNUM, &ifnr ) < 0 )
    {
        debug( "ERROR: ioctl SIOCGLIFNUM: %d: %s\n", errno, strerror(errno) );

        if ( errno != EINVAL )
        {
            close(sockfd);
            return ifrcopy;
        }
    }
    else
    {
        len = lastlen = ifnr.lifn_count * sizeof(struct ifreq);
    }
#else /* does not have SIOCGLIFNUM */
    /* determine by repetitive guessing a buffer size */
    lastlen = len = 4 * sizeof(struct ifreq); /* 1st guesstimate */
#endif
    /* POST CONDITION: some buffer size determined */

    /* FIXME: Missing upper bound */
    for (;;)
    {
        /* guestimate correct buffer length */
        buf = (char *) malloc(len);
        memset( buf, 0, len );
        ifc.ifc_len = len;
        ifc.ifc_buf = buf;
        if ( ioctl( sockfd, SIOCGIFCONF, &ifc ) < 0 )
        {
            debug( "WARN: ioctl SIOCGIFCONF: %d: %s\n", errno, strerror(errno) );
            if ( errno != EINVAL || lastlen != 0 )
            {
                close(sockfd);
                return ifrcopy;
            }
        }
        else
        {
            if ( ifc.ifc_len == lastlen ) break; /* success */
            lastlen = ifc.ifc_len;
        }
        len <<= 1;
        free((void *) buf);
    }
    /* POST CONDITION: Now the buffer contains list of all interfaces */

    /*
     * phase 2: walk interface list until a good interface is reached
     */
    /* Notice: recycle meaning of "len" in here */
    for ( ptr = buf; ptr < buf + ifc.ifc_len; )
    {
        struct ifreq *ifr = (struct ifreq *) ptr;
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
        if ( (result.ifr_flags & IFF_UP) )
        {
            if ( ! flag )
            {
                /* remember first found primary interface */
                primary = result;
                flag = 1;
            }

            /* check for VPNs */
            if ( (sa.sin_addr.s_addr & vpn_netmask[1]) != vpn_network[1] &&
                    (sa.sin_addr.s_addr & vpn_netmask[2]) != vpn_network[2] &&
                    (sa.sin_addr.s_addr & vpn_netmask[3]) != vpn_network[3] )
            {
                flag = 2;
                break;
            }
        }
    }

    /* check for loop exceeded - if yes, fall back on first primary */
    if ( flag == 1 && ptr >= buf + ifc.ifc_len ) result = primary;

    /* clean up */
    free((void *) buf);
    close(sockfd);

    /* create a freshly allocated copy */
    ifrcopy = (struct ifreq *) malloc( sizeof(struct ifreq) );
    memcpy( ifrcopy, &result, sizeof(struct ifreq) );
    return ifrcopy;
}

struct in_addr
whoami( char *buffer, size_t size )
/* purpose: copy the primary interface's IPv4 dotted quad into the given buffer
 * paramtr: buffer (IO): start of buffer
 *          size (IN): maximum capacity the buffer is willing to accept
 * returns: the modified buffer. */
{
    /* enumerate interfaces, and guess primary one */
    struct sockaddr_in sa;
    struct ifreq *ifr = primary_interface();
    if ( ifr != NULL )
    {
        memcpy( &sa, &(ifr->ifr_addr), sizeof(struct sockaddr) );
        strncpy( buffer, inet_ntoa(sa.sin_addr), size );
        free((void *) ifr);
    }
    else
    {
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
identify( char *result, size_t size, const char *arg0,
          double start, bool condor,
          const DirtyVector iox[5], const char *outfn )
{
    size_t linsize = getpagesize();
    char *line = static_cast<char *>( malloc(linsize) );

    // phase 2: where am i
    struct utsname uts;
    // char *release = const_cast<char *>("");
    // char *machine = const_cast<char *>("");
    // char *sysname = const_cast<char *>("");
    char *hostname = getenv("HOSTNAME");
    if ( uname(&uts) >= 0 )
    {
        // release = strdup( uts.release );
        // machine = strdup( uts.machine );
        // sysname = strdup( uts.sysname );
        if ( hostname == 0 ) hostname = uts.nodename;
    }

    // who am i
    char ifaddr[20];
    struct in_addr me = whoami( ifaddr, sizeof(ifaddr) );

    singleton_init();
    if ( (me.s_addr & vpn_netmask[1]) == vpn_network[1] ||
            (me.s_addr & vpn_netmask[2]) == vpn_network[2] ||
            (me.s_addr & vpn_netmask[3]) == vpn_network[3] )
    {
        // private network, no loopkup
        hostname = static_cast<char *>( malloc(32) );
        sprintf( hostname, "%s (VPN)", ifaddr );
    }
    else
    {
        struct hostent *h = ( me.s_addr == 0ul || me.s_addr == 0xFFFFFFFFul ) ?
                            gethostbyname(hostname) :
                            gethostbyaddr( (const char *) &me.s_addr, sizeof(me.s_addr), AF_INET );

        if ( h )
        {
            struct in_addr ipv4;
            hostname = static_cast<char *>( malloc(strlen(h->h_name) + 20) );
            memcpy( &ipv4.s_addr, h->h_addr, 4 );
            sprintf( hostname, "%s (%s)", inet_ntoa(ipv4), h->h_name );
        }
        else
        {
            hostname = static_cast<char *>( malloc(strlen(ifaddr) + 2) );
            strcpy( hostname, ifaddr );
        }
    }
    
    append( result, size, "IP addr and hostname: %s\n", hostname );

    free( static_cast<void *>(line) );
}

void helpMe(const char *ptr, unsigned long timeout, unsigned long spinout,
            unsigned long sleeptime, const char *prefix)
{
    printf( "Usage:\t%s [-a appname] [(-s|-t|-T) thinktime] [-l fn] [-o fn [..]]\n"
            "\t[-i fn [..] | -G size] [-e env [..]] [-p p [..]] [-P ps] [-h]\n",
            ptr );
    printf( " -a app\tset name of application to something else, default %s\n", ptr );
    printf( " -m me\tallocate 'me' MB of memory\n" );
#ifdef WITH_MPI
    printf( " -r \tallocate memory specified with the '-m' switch only in the root process\n" );
#endif
    printf( " -t to\tsleep for 'to' seconds during execution, default %lu\n", timeout );
    printf( " -s to\tsleep for 'to' seconds after the I/O phase, default %lu\n", sleeptime);
    printf( " -T to\tspin for 'to' seconds during execution, default %lu\n", spinout );
    printf( " -l fn\tappend own information atomically to a logfile\n" );
    printf( " -o ..\tenumerate space-separated list output files to create\n\
        Accept also '<filename>=<filesize><data_unit>' form, where <data_unit>\n\
        is a character supported by '-u' switch.\n\
        If you don't specify input files you have to specify output file size.\n" );
    printf( " -i ..\tenumerate space-separated list input to read and copy\n" );
    printf( " -G ..\tenumerate space-separated list of output file sizes\n" );
    printf( " -u un\tdata unit for output files generator - accepted values includes [ B K M G ], default is B\n" );
    printf( " -p ..\tenumerate space-separated parameters to mention\n" );
    printf( " -e ..\tenumerate space-separated environment values to print\n" );
    printf( " -C\tprint all environment variables starting with _CONDOR\n" );
    printf( " -P ps\tprefix input file lines with 'ps', default \"%s\"\n", prefix );
    printf( " -h\tshows this help message\n" );
}

unsigned long long
data_unit_multiplier( char data_unit )
/* purpose: convert a description of a data unit to a coresponding number of bytes
 * paramtr: data_unit (char): a character indicating data unit, accepting only B K M G
 * returns: the amount of bytes in the specified data unit */
{
    unsigned long long data_unit_multiplier = 1;

    switch ( data_unit )
    {
    case 'B':
        data_unit_multiplier = 1;
        break;
    case 'K':
        data_unit_multiplier = 1024;
        break;
    case 'M':
        data_unit_multiplier = 1024 * 1024;
        break;
    case 'G':
        data_unit_multiplier = 1024 * 1024 * 1024;
        break;
    }

    return data_unit_multiplier;
}

#ifndef MIN
#define MIN(a,b) ((a) < (b) ? (a) : (b))
#endif // MIN

char*
allocate_mem_buffer( size_t mem_buf_size )
/* purpose: allocate a memory buffor on heap and prevent it from being paged out
 * paramtr: mem_buf_size (size_t): the memory size to be allocated 
 * returns: a pointer to the allocated buffer */
{
    char *memory_buffer = static_cast<char *>( malloc(mem_buf_size) );

    if ( memory_buffer == NULL )
    {
        printf( "Memory allocation failure:  %s\n", strerror(errno) );
    }
    else
    {
        // printf( "Memory allocation was successfull\n" );
        for (unsigned int i = 0; i < mem_buf_size; i += getpagesize())
            memory_buffer[i] = 'Z';
    }

    return memory_buffer;
}

unsigned long
calculate_input_file_size( DirtyVector iox[5], char *buffer )
/* purpose: sum up input file sizes
 * paramtr: iox (DirtyVector[]): a data structure with information about all input/output files
 *         buffer (char*): already allocated auxiliary buffer
 * returns: a total size of all input files */
{
    unsigned long input_files_size = 0;
    FILE *in;
    int fd, error;
    struct stat file_stat;

    for ( unsigned int j = 0; j < iox[1].size(); j++ )
    {
        in = ( iox[1][j][0] == '-' && iox[1][j][1] == '\0' ) ?
             fdopen( STDIN_FILENO, "r" ) :
             fopen( iox[1][j], "r" );

        if ( in )
        {
            fd = fileno( in );
            error = fstat( fd, &file_stat );

            if ( error != 0 )
            {
                printf( "[error] couldn't fstat of \"%s\": %d: %s\n", iox[1][j], errno, strerror(errno) );
            }
            else
            {
                // buffer for start and final lines
                sprintf( buffer, "--- start %s ----\n", iox[1][j] );
                input_files_size += 2 * strlen( buffer );
                input_files_size += file_stat.st_size;
            }

            fclose(in);
        }
        else
        {
            printf( "[error] open \"%s\": %d: %s\n", iox[1][j], errno, strerror(errno) );
        }
    }

    return input_files_size;
}

int
read_input_files( DirtyVector iox[5], char *buffer, size_t bufsize, char *memory_buffer )
/* purpose: read input file content to a memory buffer
 * paramtr: iox (DirtyVector[]): a data structure with information about all input/output files
 *         buffer (char*): an already allocated auxiliary buffer
 *         bufsize (size_t): size of the auxiliary buffer
 *         memory_buffer (char*): destination point where the input files content should be placed
 */
{
    FILE *in;
    size_t mem_buf_offset = 0;

    for ( unsigned int j = 0; j < iox[1].size(); j++ )
    {
        in = ( iox[1][j][0] == '-' && iox[1][j][1] == '\0' ) ?
             fdopen( STDIN_FILENO, "r" ) :
             fopen( iox[1][j], "r" );

        if ( in )
        {
            sprintf( buffer, "--- start %s ----\n", iox[1][j] );
            memcpy( memory_buffer + mem_buf_offset, buffer, strlen( buffer ) );
            mem_buf_offset += strlen( buffer );

            while ( fgets( buffer, bufsize, in ) )
            {
                memcpy( memory_buffer + mem_buf_offset, buffer, strlen( buffer ) );
                mem_buf_offset += strlen( buffer );
            }

            sprintf( buffer, "--- final %s ----\n", iox[1][j] );
            memcpy( memory_buffer + mem_buf_offset, buffer, strlen( buffer ) );
            mem_buf_offset += strlen( buffer );

            fclose(in);
        }
        else
        {
            printf( "[error] open \"%s\": %d: %s\n", iox[1][j], errno, strerror(errno) );
            return 1;
        }
    }

    return 0;
}

void
generate_output_file( FILE *out, unsigned long xsize )
/* purpose: write the specified amount of 'random' data to a file
 * paramtr: out (FILE*): where the generated data should be written
 *         xsize (ulong): how much data (in bytes) should be generated
 */
{
    while ( xsize > 0 )
    {
        ssize_t wsize = fwrite( output, sizeof(char),
                                MIN(xsize, sizeof(output)), out );
        // printf( "[%d] writing %ld bytes\n", i, wsize );
        if ( wsize > 0 ) xsize -= wsize;
        else break;
    }
    fputc( '\n', out );
}

/**
 * create a deep directory if it does not exist
 * paramtr: file_path  the directory to be created. must end in /
 * mode:    mode to assign to the directory 
 */
int
mkpath(char* file_path, mode_t mode) {
    for (char* p = strchr(file_path + 1, '/'); p; p = strchr(p + 1, '/')) {
        *p = '\0';
	fprintf( stdout, "trying to create dir %s\n", file_path);
        if (mkdir(file_path, mode) == -1) {
            if (errno != EEXIST) {
                *p = '/';
                return -1;
            }
        }
        *p = '/';
    }
    return 0;
}

int
main( int argc, char *argv[] )
{
    // process rank - altered only in the MPI version
    int rank = 0;

#ifdef WITH_MPI
    MPI_Init( &argc, &argv );

    MPI_Comm_rank( MPI_COMM_WORLD, &rank );
#endif

    // required CPU time
    unsigned long spinout = 0;
    // required wall time
    unsigned long timeout = 0;
    // required sleep time
    unsigned long sleeptime = 0;

    // buffer for mock memory or input files content
    char *memory_buffer = NULL;
    // auxiliary variables for memory management
    unsigned long memory_size = 0;
    size_t mem_buf_size = 0;
    bool root_only_memory_allocation = false;
    size_t bufsize = getpagesize() << 4;
    if ( bufsize < 16384 ) bufsize = 16384;
    char *buffer = static_cast<char *>( malloc(bufsize) );    

    int state = 0;
    bool condor = false;
    // DK: DEPRACTED
    // unsigned long gensize = 0;
    char data_unit = 'B';
    DirtyVector iox[5];

    // when did we start
    double start = now();
    char *prefix = strdup("  ");

    // determine base name of input file
    char *logfile = 0;
    char *ptr = 0;
    if ( (ptr = strrchr(argv[0], '/')) == 0 ) ptr = argv[0];
    else ptr++;

    // complain, if no parameters were given
    if ( rank == 0 && argc == 1 )
    {
        helpMe( ptr, timeout, spinout, sleeptime, prefix );
        return 0;
    }

    // prepare generator pattern
    for ( size_t i = 0; i < sizeof(output); i++ ) output[i] = pattern[i & 63];

    for ( int i = 1; i < argc; ++i )
    {
        char *s = argv[i];
        if ( s[0] == '-' && s[1] != 0 )
        {
            if ( strchr( "iotTGaepPlCmruhs\0", s[1] ) != NULL )
            {
                switch (s[1])
                {
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
                    state = 4;
                    break;
                case 'm':
                    state = 16;
                    break;
                case 'u':
                    state = 17;
                    break;
                case 's':
                    state = 19;
                    break;
#ifdef WITH_MPI
                case 'r':
                    root_only_memory_allocation = true;
                    continue;
#endif
                case 'C':
                    condor = true;
                    continue;

                case 'h':
                    helpMe( ptr, timeout, spinout, sleeptime, prefix );
                    return 0;
                }
                s += 2;
            }
        }
        if ( strlen(s) == 0 ) continue;
        if ( state >= 10 )
        {
            switch ( state )
            {
            case 10:
                ptr = s;
                break;
            case 11:
                timeout = strtoul(s, 0, 10);
                break;
            case 12:
                logfile = s;
                break;
            case 13:
                spinout = strtoul(s, 0, 10);
                break;
            case 14:
                free( static_cast<void *>(prefix) );
                prefix = strdup(s);
                break;
            // DK: DEPRECATED not used since we allow to specify size of each output file
            // case 15:
            //     gensize = strtoul(s, 0, 10);
            //     break;
            case 16:
                memory_size = strtoul(s, 0, 10);
                mem_buf_size = sizeof(char) * memory_size * data_unit_multiplier( 'M' );
                break;
            case 17:
                data_unit = s[0];
                break;
            case 19:
                sleeptime = strtoul(s, 0, 10);
                break;
            }
            state = 0;
        }
        else
        {
            iox[state].push_back(s);
        }
    }

    if (sleeptime > 0 && timeout > 0) 
    {
        sleeptime = fabs(sleeptime - timeout);
    }

    if (sleeptime > 0 && spinout > 0)
    {
        sleeptime = fabs(sleeptime - spinout);
    }

    if (timeout > sleeptime || spinout > sleeptime)
    {
        sleeptime = 0;
    }

    if ( memory_size )
    {
        if ( (rank == 0) || (! root_only_memory_allocation) )
        {
            memory_buffer = allocate_mem_buffer( mem_buf_size );
        }
    }

    if (rank == 0)
    {
        // PHASE 1 - reading input files to memory if any; use the memory_buffer for storing all the file content
        // 1. check how much memory do we need
        unsigned long input_files_size = 0;
        FILE *out;

        input_files_size = calculate_input_file_size( iox, buffer );

        // 2. allocate memory buffer
        if ( input_files_size > mem_buf_size )
        {
            if ( memory_buffer == NULL )
            {
                memory_buffer = static_cast<char *>( malloc( sizeof(char) * input_files_size ) );
            }
            else
            {
                memory_buffer = static_cast<char *>( realloc( memory_buffer, sizeof(char) * input_files_size ) );
            }

            strcpy(memory_buffer, "");
        }

        // 3. read the input files content
        if( read_input_files( iox, buffer, bufsize, memory_buffer ) ) {
            free( static_cast<void *>(buffer) );
            return 2;
        }
        // printf( "%s\n", memory_buffer );

        // PHASE 2 - writing output files if any; the -G switch has higher priority than input files
        for ( unsigned i = 0; i < iox[2].size(); ++i )
        {
            unsigned long xsize = 0;

            if ( iox[2][i][0] == '-' && iox[2][i][1] == '\0' )
            {
                out = fdopen( STDOUT_FILENO, "a" );
            }
            else 
            {
                char *filesize = strrchr( (char*)iox[2][i], '=' );
                char filename[256];
                
                if ( filesize != NULL )
                {
                    memcpy( filename, iox[2][i], sizeof(char) * ( filesize - iox[2][i] ) );
                    filename[( filesize - iox[2][i] )] = '\0';

                    out = fopen( filename, "w" );

                    unsigned long long unit_multiplier = 1;

                    if ( strchr( "BKMG\0", filesize[ strlen( filesize ) - 1 ] ) != NULL ) 
                    {
                        unit_multiplier = data_unit_multiplier( filesize[ strlen( filesize ) - 1 ] );
                    }

                    filesize[ strlen( filesize ) - 1 ] = '\0';
                    xsize = strtoul(filesize + 1, 0, 10) * unit_multiplier;
                }
                else 
                {
		    strcpy(filename, iox[2][i]);
		    fprintf(stdout, "output file to be generated is %s\n", filename) ;
		    // is there a directory path specified
		    char* last_index =  strrchr( filename, '/' );  
		    if (last_index != NULL){
			 char directory[256];
			 /* ensure that directory has the trailing */
			 memcpy(directory, filename, sizeof(char) * (last_index - filename + 1));
			 directory[ last_index - filename + 1 ] = '\0';
		         fprintf(stdout, "need to create directory is %s\n", directory) ;
			 if (mkpath(directory, 0777) != 0) {
			   fprintf( stderr, "[error] Unable to mkdir %s: %d: %s\n", directory, errno, strerror(errno));
			   return 1;
			 }
		    }
		    out = fopen( iox[2][i], "w" );
                }
            }

            if ( out )
            {
                if ( iox[4].size() > 0 || xsize > 0 )
                {
                    if ( xsize <= 0 )
                    {
                        const char *xsize_str = iox[4][ i % iox[4].size() ];  
                        xsize = strtoul(xsize_str, 0, 10) * data_unit_multiplier( data_unit );
                    }
                    generate_output_file( out, xsize );
                }
                else
                {
                    fputs( prefix, out );
                    
                    if(memory_buffer != NULL) {
                        fputs( memory_buffer, out );
                    }                    
                }

                // create buffer, and fill with content
                memset( buffer, 0, bufsize );
                identify( buffer, bufsize, ptr, start, condor, iox, iox[2][i] );
                fputs( buffer, out );
                fclose(out);
            }
            else
            {
                fprintf( stderr, "open(%s): %s\n", iox[2][i], strerror(errno) );
                free( static_cast<void *>(buffer) );
                return 2;
            }
        }
    }

    // printf( "Start time: %f - Current timestamp: %f - Difference: %f\n", start, timestamp, timestamp - start);
    // printf( "[debug] we spent %d [s] on IO stuff\n", (int) (timestamp - start) );

    // PHASE 3 - spinning out
    if( spinout ) {
        spin(spinout);
    }

    // PHASE 4 - sleeping
    if ( timeout )
    {
        double timestamp = now();
        int time_diff = timeout - ( (int) (timestamp - start) );

        if ( time_diff < 0 )
        {
            printf("[error] you specified %lu [s] to sleep but you've already exceeded this value by %d [s]\n", timeout, -time_diff );

            if ( memory_buffer != NULL )
                free( static_cast<void *>(memory_buffer) );

            if ( buffer != NULL )
                free( static_cast<void *>(buffer) );

            return 3;

        }
        else
        {
            // printf( "[debug] you specified %lu [s] to sleep so we will sleep for %d [s]\n", timeout, time_diff );
            sleep(time_diff);
        }
    }

    if ( sleeptime )
    {
        sleep(sleeptime);
    }

    // append atomically to logfile
    if ( rank == 0 && logfile != 0 )
    {
        int fd = -1;
        if ( (fd = open( logfile, O_WRONLY | O_CREAT | O_APPEND, 0666 )) == -1 )
        {
            fprintf( stderr, "WARNING: open(%s): %s\n", logfile, strerror(errno) );
        }
        else
        {
            memset( buffer, 0, bufsize );
            identify( buffer, bufsize, ptr, start, condor, iox, logfile );
            append( buffer, bufsize, '\n' );
            write( fd, buffer, strlen(buffer) ); // atomic write
            close(fd);
        }
    }

    if ( memory_buffer != NULL )
        free( static_cast<void *>(memory_buffer) );

    if ( buffer != NULL )
        free( static_cast<void *>(buffer) );

#ifdef WITH_MPI
    // else     // worker
    // {
    // }
#endif

#ifdef WITH_MPI
    MPI_Finalize();
#endif

    return 0;
}