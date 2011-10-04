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
#ifndef _MYNSS_H
#define _MYNSS_H

#ifdef LINUX
/* $@#! Linux */
#include <sys/types.h>
#include <signal.h>
#include <pwd.h>
#include <grp.h>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>

extern volatile sig_atomic_t noSymbolicLookups;

extern struct passwd*  wrap_getpwuid( uid_t uid );
extern struct group*   wrap_getgrgid( gid_t gid );
extern struct hostent* wrap_gethostbyaddr( const char* addr, int len, int type );

#else
/* These are _sane_ systems like Solaris */
#define wrap_getpwuid(uid) getpwuid((uid))
#define wrap_getgrgid(gid) getgrgid((gid))
#define wrap_gethostbyaddr(a,b,c) gethostbyaddr((a),(b),(c))
#endif

#endif /* _MYNSS_H */
