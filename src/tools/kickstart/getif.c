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
#include "debug.h"
#include "getif.h"

#include <errno.h>
#include <string.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>

#include <sys/utsname.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <sys/ioctl.h>

#ifdef HAS_SYS_SOCKIO
#include <sys/sockio.h>
#endif

static const char* RCS_ID =
  "$Id$";

int getif_debug = 0; /* enable debugging code paths */

static unsigned long vpn_network[5] = { 0, 0, 0, 0 };
static unsigned long vpn_netmask[5] = { 0, 0, 0, 0 };

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
    vpn_network[4] = inet_addr("169.254.0.0"); /* link-local junk */
  }

  /* singleton init */
  if ( vpn_netmask[0] == 0ul ) {
    vpn_netmask[0] = inet_addr("255.0.0.0");   /* loopbackmask */
    vpn_netmask[1] = inet_addr("255.0.0.0");   /* class A mask */
    vpn_netmask[2] = inet_addr("255.240.0.0"); /* class B VPN mask */
    vpn_netmask[3] = inet_addr("255.255.0.0"); /* class C VPN mask */
    vpn_netmask[4] = inet_addr("255.254.0.0"); /* link-local junk */
  }
}

int
interface_list( struct ifconf* ifc )
/* purpose: returns the list of interfaces
 * paramtr: ifc (IO): initializes structure with buffer and length
 * returns: sockfd for further queries, or -1 to indicate an error. 
 * warning: caller must free memory in ifc.ifc_buf
 *          caller must close sockfd (result value)
 */
{
#if defined(SIOCGLIFNUM)
  struct lifnum ifnr;
#endif
  char *buf = 0;
  int lastlen, len, sockfd = 0;

  /* create a socket */
  if ( (sockfd = socket( AF_INET, SOCK_DGRAM, 0 )) == -1 ) { 
    int saverr = errno; 
    debugmsg( "ERROR: socket DGRAM: %d: %s\n", errno, strerror(errno) );
    errno = saverr; 
    return -1;
  }

  /*
   * phase 1: guestimate size of buffer necessary to contain all interface
   * information records. 
   */
#if defined(SIOCGLIFNUM)
  /* API exists to determine the correct buffer size */
  if ( getif_debug ) debugmsg( "DEBUG: SIOCGLIFNUM ioctl supported\n" );

  memset( &ifnr, 0, sizeof(ifnr) );
  ifnr.lifn_family = AF_INET;
  if ( ioctl( sockfd, SIOCGLIFNUM, &ifnr ) < 0 ) {
    debugmsg( "ERROR: ioctl SIOCGLIFNUM: %d: %s\n", errno, strerror(errno) );

    if ( errno != EINVAL ) {
      int saverr = errno;
      close(sockfd);
      errno = saverr; 
      return -1; 
    }
  } else {
    len = lastlen = ifnr.lifn_count * sizeof(struct ifreq);
  }
#else /* does not have SIOCGLIFNUM */
  /* determine by repetitive guessing a buffer size */
  if ( getif_debug ) debugmsg( "DEBUG: SIOCGLIFNUM ioctl *not* supported\n" );

  lastlen = len = 3.5 * sizeof(struct ifreq); /* 1st guesstimate */
#endif
  /* POST CONDITION: some buffer size determined */

  /* FIXME: Missing upper bound */
  for (;;) {
    /* guestimate correct buffer length */
    if ( getif_debug ) debugmsg( "DEBUG: lastlen=%d, len=%d\n", lastlen, len );

    buf = (char*) malloc(len);
    memset( buf, 0, len );
    ifc->ifc_len = len;
    ifc->ifc_buf = buf;
    if ( ioctl( sockfd, SIOCGIFCONF, ifc ) < 0 ) {
      debugmsg( "WARN: ioctl SIOCGIFCONF: %d: %s\n", errno, strerror(errno) );
      if ( errno != EINVAL || lastlen != 0 ) {
	int saverr = errno; 
	close(sockfd);
	errno = saverr; 
	return -1; 
      }
    } else {
      if ( ifc->ifc_len == lastlen ) break; /* success */
      if ( getif_debug ) debugmsg( "DEBUG: size mismatch, next round\n" );
      lastlen = ifc->ifc_len;
    }
    len <<= 1;
    free((void*) buf);
  }
  /* POST CONDITION: Now the buffer contains list of all interfaces */
  if ( getif_debug ) {
    debugmsg( "DEBUG: correct buffer length %d\n", ifc->ifc_len );
    hexdump( ifc->ifc_buf, ifc->ifc_len );
  }

  return sockfd; 
}



struct ifreq*
primary_interface( void )
/* purpose: obtain the primary interface information
 * returns: a newly-allocated structure containing the interface info,
 *          or NULL to indicate an error. 
 */
{
  struct sockaddr_in sa;
  struct ifconf ifc;
  struct ifreq  result, primary;
  struct ifreq* ifrcopy = NULL;
  char   *ptr; 
  int    sockfd, flag = 0;

  /*
   * phase 0: init
   */
  memset( &result, 0, sizeof(result) );
  memset( &primary, 0, sizeof(primary) );
  singleton_init();

  /* 
   * phase 1: obtain list of interfaces 
   */
  if ( (sockfd=interface_list( &ifc )) == -1 ) return NULL; 

  /*
   * phase 2: walk interface list until a good interface is reached
   */ 
  /* Notice: recycle meaning of "len" in here */
  for ( ptr = ifc.ifc_buf; ptr < ifc.ifc_buf + ifc.ifc_len; ) {
    struct ifreq* ifr = (struct ifreq*) ptr;
    size_t len = sizeof(*ifr);
    if ( getif_debug ) debugmsg( "DEBUG: stepping by %d\n", len );
    ptr += len;

    /* report current entry's interface name */
    if ( getif_debug ) debugmsg( "DEBUG: interface %s\n", ifr->ifr_name );

    /* interested in IPv4 interfaces only */
    if ( ifr->ifr_addr.sa_family != AF_INET ) {
      if ( getif_debug ) 
	debugmsg( "DEBUG: interface has wrong family, skipping\n" );
      continue;
    }

    memcpy( &sa, &(ifr->ifr_addr), sizeof(struct sockaddr_in) );
    if ( getif_debug ) debugmsg( "DEBUG: address %s\n", inet_ntoa(sa.sin_addr) );

    /* Do not use localhost aka loopback interfaces. While loopback
     * interfaces traditionally start with "lo", this is not mandatory.
     * It is safer to check that the address is in the 127.0.0.0 class A
     * network. */
    if ( (sa.sin_addr.s_addr & vpn_netmask[0]) == vpn_network[0] ) {
      if ( getif_debug ) 
	debugmsg( "DEBUG: interface is localhost, skipping\n" );
      continue;
    }

    /* prime candidate - check, if interface is UP */
    result = *ifr;
    if ( ioctl( sockfd, SIOCGIFFLAGS, &result ) < 0 ) {
      if ( getif_debug ) 
	debugmsg( "DEBUG: ioctl SIOCGIFFLAGS %s: %s\n", 
	       ifr->ifr_name, strerror(errno) );
    }

    /* interface is up - our work is done. Or is it? */
    if ( (result.ifr_flags & IFF_UP) ) {
      if ( ! flag ) {
	/* remember first found primary interface */
	if ( getif_debug )
	  debugmsg( "DEBUG: first primary interface %s\n", ifr->ifr_name );
	primary = result;
	flag = 1;
      }

      /* check for VPNs */
      if ( (sa.sin_addr.s_addr & vpn_netmask[1]) == vpn_network[1] ||
	   (sa.sin_addr.s_addr & vpn_netmask[2]) == vpn_network[2] ||
	   (sa.sin_addr.s_addr & vpn_netmask[3]) == vpn_network[3] ) {
	if ( getif_debug )
	  debugmsg( "DEBUG: interface has VPN address, trying next\n" );
      } else {
	if ( getif_debug ) 
	  debugmsg( "DEBUG: interface is good\n" );
	flag = 2;
	break;
      }
    } else {
      if ( getif_debug ) debugmsg( "DEBUG: interface is down\n" );
    }
  }

  /* check for loop exceeded - if yes, fall back on first primary */
  if ( flag == 1 && ptr >= ifc.ifc_buf + ifc.ifc_len ) {
    if ( getif_debug ) 
      debugmsg( "DEBUG: no better interface found, falling back\n" );
    result = primary;
  }

  /* clean up */
  free((void*) ifc.ifc_buf);
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
