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
#include "getif.hh"

#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdlib.h>

#include <sys/utsname.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdarg.h>
#include <sys/ioctl.h>

#ifdef HAS_SYS_SOCKIO
#include <sys/sockio.h>
#endif

static const char* RCS_ID =
"$Id$";

const unsigned long 
PrimaryInterface::vpn_netmask[4] = {
  inet_addr("255.0.0.0"),   /* loopbackmask */
  inet_addr("255.0.0.0"),   /* class A mask */
  inet_addr("255.240.0.0"), /* class B VPN mask */
  inet_addr("255.255.0.0")  /* class C VPN mask */
};

const unsigned long 
PrimaryInterface::vpn_network[4] = {
  inet_addr("127.0.0.0"),   /* loopbacknet */
  inet_addr("10.0.0.0"),    /* class A VPN net */
  inet_addr("172.16.0.0"),  /* class B VPN nets */
  inet_addr("192.168.0.0")  /* class C VPN nets */
};

bool
PrimaryInterface::isVPN( const unsigned long host )
  // purpose: Determines if an IPv4 address is from a VPN
  // paramtr: host (IN): network byte ordered IPv4 host address
  // returns: true, if the host is in a VPN address space
{
  return ( (host & vpn_netmask[1]) == vpn_network[1] ||
	   (host & vpn_netmask[2]) == vpn_network[2] ||
	   (host & vpn_netmask[3]) == vpn_network[3] );
}

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

struct ifreq*
PrimaryInterface::primary_interface( )
  // purpose: obtain the primary interface information
  // returns: a structure containing the if info, or NULL for error
{
#if defined(SIOCGLIFNUM)
  struct lifnum ifnr;
#endif
  struct sockaddr_in sa;
  struct ifconf ifc;
  struct ifreq  result, primary;
  struct ifreq* ifrcopy = 0;
  char *ptr, *buf = 0;
  int lastlen, len, sockfd, flag = 0;

  /*
   * phase 0: init
   */
  memset( &result, 0, sizeof(result) );
  memset( &primary, 0, sizeof(primary) );

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
      if ( ! isVPN(sa.sin_addr.s_addr) ) {
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



PrimaryInterface::PrimaryInterface()
  // purpose: protected singleton ctor
  :m_interface( primary_interface() )
{
  // empty
}

PrimaryInterface::~PrimaryInterface()
  // purpose: protected singleton destructor
{
  if ( m_interface ) free((void*) m_interface);
}

PrimaryInterface*
PrimaryInterface::m_instance = 0;

const PrimaryInterface&
PrimaryInterface::instance()
  // purpose: Obtains access to the singleton
  // returns: The single instance of the Singleton.
{
  if ( m_instance == 0 ) m_instance = new PrimaryInterface();
  return *m_instance;
}

std::string
PrimaryInterface::whoami() const
  // purpose: Obtains the primary interface's IPv4 as dotted quad
  // returns: The IPv4 as dotted quad - or an empty string on failure
{
  std::string result;

  if ( m_interface ) {
    struct sockaddr_in sa;
    // type-ignoring copy
    memcpy( &sa, &(m_interface->ifr_addr), sizeof(struct sockaddr) );
    result = inet_ntoa(sa.sin_addr);
  } else {
#if 0
    result = "xsi:null";
#else
    result = "0.0.0.0";
#endif
  }

  return result;
}

const char* 
PrimaryInterface::whoami( char* area, size_t size ) const
  // purpose: Obtains the primary interface's IPv4 as dotted quad
  // paramtr: area (OUT): The IPv4 as dotted quad - sizeof >= 16!
  // returns: area
{
  memset( area, 0, size );
  if ( m_interface ) {
    struct sockaddr_in sa;

    // type-ignoring copy
    memcpy( &sa, &(m_interface->ifr_addr), sizeof(struct sockaddr) );
    // add to result
    return strncpy( area, inet_ntoa(sa.sin_addr), size );
  } else {
#if 0
    /* future lab */
    return strncpy( area, "xsi:null", size );
#else
    /* for now */
    return strncpy( area, "0.0.0.0", size );
#endif
  }
}
