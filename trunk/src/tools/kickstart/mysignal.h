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
#ifndef _MYSIGNAL_H
#define _MYSIGNAL_H

#include <signal.h>

#if 1 /* so far, all systems I know use void */
# define SIGRETTYPE void
#else
# define SIGRETTYPE int
#endif

#if defined(SUNOS) && defined(SUN)
# define SIGPARAM void
#else /* SOLARIS, LINUX, IRIX, AIX, SINIXY */
# define SIGPARAM int
#endif

typedef SIGRETTYPE SigFunc( SIGPARAM );

#endif /* _MYSIGNAL_H */
