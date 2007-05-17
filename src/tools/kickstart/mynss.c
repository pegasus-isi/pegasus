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
#include "mynss.h"

static const char* RCS_ID =
"$Id: mynss.c,v 1.2 2005/08/05 20:28:33 griphyn Exp $";

#ifdef LINUX
#include <setjmp.h>
#include <errno.h>
#include <memory.h>
#include <string.h>

#include "mysignal.h"

/*
 * Linux statically linked binaries do not like libnss function calls.
 */
volatile sig_atomic_t noSymbolicLookups;
static volatile sig_atomic_t canJump;
static sigjmp_buf jump;

static 
SIGRETTYPE
sigsegv_handler( SIGPARAM signo )
{
  if ( canJump == 0 ) return; /* unexpected signal */
  noSymbolicLookups = signo;
  canJump = 0;
  siglongjmp( jump, 1 );
}

static
int
setup_sigsegv( struct sigaction* old, struct sigaction* new )
{
  memset( old, 0, sizeof(*old) );
  memset( new, 0, sizeof(*new) );
  new->sa_handler = sigsegv_handler;
  sigemptyset( &(new->sa_mask) );
#ifdef SA_INTERRUPT
  new->sa_flags |= SA_INTERRUPT; /* SunOS, obsoleted by POSIX */
#endif

  return sigaction( SIGSEGV, new, old );
}

struct passwd* 
wrap_getpwuid( uid_t uid )
{
  struct sigaction old_segv, new_segv;
  struct passwd* result;

  if ( noSymbolicLookups ) return NULL;
  if ( sigsetjmp( jump, 1 ) ) {
    errno = ELIBACC;
    return NULL;
  }
  canJump = 1;
  setup_sigsegv( &old_segv, &new_segv );
  result = getpwuid(uid);
  sigaction( SIGSEGV, &old_segv, NULL );
  return ( noSymbolicLookups ? NULL : result );
}

struct group* 
wrap_getgrgid( gid_t gid )
{
  struct sigaction old_segv, new_segv;
  struct group* result;

  if ( noSymbolicLookups ) return NULL;
  if ( sigsetjmp( jump, 1 ) ) {
    errno = ELIBACC;
    return NULL;
  }
  canJump = 1;
  setup_sigsegv( &old_segv, &new_segv );
  result = getgrgid(gid);
  sigaction( SIGSEGV, &old_segv, NULL );
  return ( noSymbolicLookups ? NULL : result );
}

struct hostent* 
wrap_gethostbyaddr( const char* addr, int len, int type )
{
  struct sigaction old_segv, new_segv;
  struct hostent* result;

  if ( noSymbolicLookups ) return NULL;
  if ( sigsetjmp( jump, 1 ) ) {
    h_errno = NETDB_INTERNAL;
    errno = ELIBACC;
    return NULL;
  }
  canJump = 1;
  setup_sigsegv( &old_segv, &new_segv );
  result = gethostbyaddr(addr,len,type);
  sigaction( SIGSEGV, &old_segv, NULL );
  return ( noSymbolicLookups ? NULL : result );
}

#endif /* LINUX */

