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

#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdarg.h>

#include <sys/utsname.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <sys/ioctl.h>

#ifdef HAS_SYS_SOCKIO
#include <sys/sockio.h>
#endif

static const char* RCS_ID =
"$Id: getif.c,v 1.8 2005/10/19 18:37:20 griphyn Exp $";

int getif_debug = 0; /* enable debugging code paths */

#include <stdarg.h>

static
int
debug( char* fmt, ... )
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


static 
int 
hexdump( char* area, size_t size )
{
  static const char digit[16] = "0123456789ABCDEF";
  char a[82];
  unsigned char b[18];
  size_t i, j;
  unsigned char c;
  ssize_t result = 0;

  for ( i=0; i<size; i+=16 ) {
    memset( a, 0, sizeof(a) );
    memset( b, 0, sizeof(b) );
    sprintf( a, "%04X: ", i );
    for ( j=0; j<16 && j+i<size; ++j ) {
      c = (unsigned char) area[i+j];

      a[6+j*3] = digit[ c >> 4 ];
      a[7+j*3] = digit[ c & 15 ];
      a[8+j*3] = ( j == 7 ? '-' : ' ' );
      b[j] = (char) (c < 32 || c >= 127 ? '.' : c);
    }
    for ( ; j<16; ++j ) {
      a[6+j*3] = a[7+j*3] = a[8+j*3] = b[j] = ' ';
    }
    strncat( a, (char*) b, sizeof(a) );
    strncat( a, "\n", sizeof(a) );
    result += write( STDERR_FILENO, a, strlen(a) );
  }

  return result;
}

static unsigned long vpn_network[4] = { 0, 0, 0, 0 };
static unsigned long vpn_netmask[4] = { 0, 0, 0, 0 };

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
  }

  /* singleton init */
  if ( vpn_netmask[0] == 0ul ) {
    vpn_netmask[0] = inet_addr("255.0.0.0");   /* loopbackmask */
    vpn_netmask[1] = inet_addr("255.0.0.0");   /* class A mask */
    vpn_netmask[2] = inet_addr("255.240.0.0"); /* class B VPN mask */
    vpn_netmask[3] = inet_addr("255.255.0.0"); /* class C VPN mask */
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
  if ( getif_debug ) debug( "DEBUG: SIOCGLIFNUM ioctl supported\n" );

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
  if ( getif_debug ) debug( "DEBUG: SIOCGLIFNUM ioctl *not* supported\n" );

  lastlen = len = 4 * sizeof(struct ifreq); /* 1st guesstimate */
#endif
  /* POST CONDITION: some buffer size determined */

  /* FIXME: Missing upper bound */
  for (;;) {
    /* guestimate correct buffer length */
    if ( getif_debug ) debug( "DEBUG: lastlen=%d, len=%d\n", lastlen, len );

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
      if ( getif_debug ) debug( "DEBUG: size mismatch, next round\n" );
      lastlen = ifc.ifc_len;
    }
    len <<= 1;
    free((void*) buf);
  }
  /* POST CONDITION: Now the buffer contains list of all interfaces */
  if ( getif_debug ) {
    debug( "DEBUG: correct buffer length %d\n", ifc.ifc_len );
    hexdump( buf, ifc.ifc_len );
  }

  /*
   * phase 2: walk interface list until a good interface is reached
   */ 
  /* Notice: recycle meaning of "len" in here */
  for ( ptr = buf; ptr < buf + ifc.ifc_len; ) {
    struct ifreq* ifr = (struct ifreq*) ptr;
    len = sizeof(*ifr);
    if ( getif_debug ) debug( "DEBUG: stepping by %d\n", len );
    ptr += len;

    /* report current entry's interface name */
    if ( getif_debug ) debug( "DEBUG: interface %s\n", ifr->ifr_name );

    /* interested in IPv4 interfaces only */
    if ( ifr->ifr_addr.sa_family != AF_INET ) {
      if ( getif_debug ) 
	debug( "DEBUG: interface has wrong family, skipping\n" );
      continue;
    }

    memcpy( &sa, &(ifr->ifr_addr), sizeof(struct sockaddr_in) );
    if ( getif_debug ) debug( "DEBUG: address %s\n", inet_ntoa(sa.sin_addr) );

    /* Do not use localhost aka loopback interfaces. While loopback
     * interfaces traditionally start with "lo", this is not mandatory.
     * It is safer to check that the address is in the 127.0.0.0 class A
     * network. */
    if ( (sa.sin_addr.s_addr & vpn_netmask[0]) == vpn_network[0] ) {
      if ( getif_debug ) 
	debug( "DEBUG: interface is localhost, skipping\n" );
      continue;
    }

    /* prime candidate - check, if interface is UP */
    result = *ifr;
    if ( ioctl( sockfd, SIOCGIFFLAGS, &result ) < 0 ) {
      if ( getif_debug ) 
	debug( "DEBUG: ioctl SIOCGIFFLAGS %s: %s\n", 
	       ifr->ifr_name, strerror(errno) );
    }

    /* interface is up - our work is done. Or is it? */
    if ( (result.ifr_flags & IFF_UP) ) {
      if ( ! flag ) {
	/* remember first found primary interface */
	if ( getif_debug )
	  debug( "DEBUG: first primary interface %s\n", ifr->ifr_name );
	primary = result;
	flag = 1;
      }

      /* check for VPNs */
      if ( (sa.sin_addr.s_addr & vpn_netmask[1]) == vpn_network[1] ||
	   (sa.sin_addr.s_addr & vpn_netmask[2]) == vpn_network[2] ||
	   (sa.sin_addr.s_addr & vpn_netmask[3]) == vpn_network[3] ) {
	if ( getif_debug )
	  debug( "DEBUG: interface has VPN address, trying next\n" );
      } else {
	if ( getif_debug ) 
	  debug( "DEBUG: interface is good\n" );
	flag = 2;
	break;
      }
    } else {
      if ( getif_debug ) debug( "DEBUG: interface is down\n" );
    }
  }

  /* check for loop exceeded - if yes, fall back on first primary */
  if ( flag == 1 && ptr >= buf + ifc.ifc_len ) {
    if ( getif_debug ) 
      debug( "DEBUG: no better interface found, falling back\n" );
    result = primary;
  }

  /* clean up */
  free((void*) buf);
  close(sockfd);

  /* create a freshly allocated copy */
  ifrcopy = (struct ifreq*) malloc( sizeof(struct ifreq) );
  memcpy( ifrcopy, &result, sizeof(struct ifreq) );
  return ifrcopy;
}

void
whoami( char* buffer, size_t size )
/* purpose: copy the primary interface's IPv4 dotted quad into the given buffer
 * paramtr: buffer (IO): start of buffer
 *          size (IN): maximum capacity the buffer is willing to accept
 * returns: the modified buffer. */
{
  /* enumerate interfaces, and guess primary one */
  struct ifreq* ifr = primary_interface();
  if ( ifr != NULL ) {
    struct sockaddr_in sa;
    memcpy( &sa, &(ifr->ifr_addr), sizeof(struct sockaddr) );
    strncpy( buffer, inet_ntoa(sa.sin_addr), size );
    free((void*) ifr);
  } else {
    /* error while trying to determine address of primary interface */
#if 0
    /* future lab */
    strncpy( buffer, "xsi:null", size );
#else
    /* for now */
    strncpy( buffer, "0.0.0.0", size );
#endif
  }
}
