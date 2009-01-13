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
#ifndef _GETIF_H
#define _GETIF_H

#include <sys/types.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <net/if.h>

extern int getif_debug; /* enable debugging code paths */

extern
int
interface_list( struct ifconf* ifc );
/* purpose: returns the list of interfaces
 * paramtr: ifc (IO): initializes structure with buffer and length
 * returns: sockfd for further queries, or -1 to indicate an error. 
 * warning: caller must free memory in ifc.ifc_buf
 *          caller must close sockfd (result value)
 */

extern 
struct ifreq* 
primary_interface();
/* purpose: obtain the primary interface information
 * returns: a newly-allocated structure containing the interface info,
 *          or NULL to indicate an error. 
 */

extern 
void 
whoami( char* buffer, size_t size );
/* purpose: copy the primary interface's IPv4 dotted quad into the given buffer
 * paramtr: buffer (IO): start of buffer
 *          size (IN): maximum capacity the buffer is willing to accept
 * returns: the modified buffer. 
 */

#endif /* _GETIF_H */
