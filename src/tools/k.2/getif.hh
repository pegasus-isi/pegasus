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
#ifndef _GETIF_HH
#define _GETIF_HH

#include <sys/types.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <net/if.h>

#include <string>

class PrimaryInterface {
  // This class obtains the IPv4 address of the primary interface.
  // It is implemented using the Singleton pattern.
private:
  static const unsigned long    vpn_netmask[4];
  static const unsigned long    vpn_network[4];
  struct ifreq*			m_interface;
  static PrimaryInterface*	m_instance;

protected:
  PrimaryInterface();
    // purpose: protected singleton ctor
  ~PrimaryInterface();
    // purpose: protected singleton destructor
  static struct ifreq* primary_interface();
    // purpose: obtain the primary interface information
    // returns: a structure containing the if info, or NULL for error

public:
  static const PrimaryInterface& instance();
    // purpose: Obtains access to the singleton
    // returns: The single instance of the Singleton.

  std::string whoami() const;
    // purpose: Obtains the primary interface's IPv4 as dotted quad
    // returns: The IPv4 as dotted quad - or an empty string on failure

  const char* whoami( char* area, size_t size ) const;
    // purpose: Obtains the primary interface's IPv4 as dotted quad
    // paramtr: area (OUT): The IPv4 as dotted quad - sizeof >= 16!
    // returns: area

  static bool isVPN( const unsigned long host );
    // purpose: Determines if an IPv4 address is from a VPN
    // paramtr: host (IN): network byte ordered IPv4 host address
    // returns: true, if the host is in a VPN address space
}; 

#endif // _GETIF_HH
