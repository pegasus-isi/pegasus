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
#ifndef _CAPABILITIES_H
#define _CAPABILITIES_H

enum {
  GUC_TCP_BS    = 0x0001, /* -tcp-bs */
  GUC_BLOCKSIZE = 0x0002, /* -bs, -block-size */
  GUC_PARALLEL  = 0x0004, /* -p, -parallel */
  GUC_PERFDATA  = 0x0008, /* -vb */
  GUC_DEBUG     = 0x0010, /* -dbg */

  /* GT2 has capabilities 001f */

  GUC_CONTINUE  = 0x0020, /* -c, -continue-on-error */
  GUC_FAST      = 0x0040, /* -fast */
  GUC_FROMFILE  = 0x0080, /* -f fn */
  GUC_RECURSIVE = 0x0100, /* -r, -recurse*/
  GUC_RESTART   = 0x0200, /* -rst, -restart */
  GUC_REST_IV   = 0x0400, /* -rst-interval */
  GUC_REST_TO   = 0x0800, /* -rst-timeout */

  /* GT3.2 has capabilities 0fff */

  GUC_STRIPE    = 0x1000, /* -stripe */
  GUC_STRIPE_BS = 0x2000, /* -sbs, -striped-block-size */
  GUC_CREATEDIR = 0x4000  /* -cd */

  /* GT3.9 + Link-patch has capabilities 7fff */
};

extern
unsigned long
guc_capabilities( char* app, char* envp[] );
/* purpose: Obtains the capabilties of a given g-u-c
 * paramtr: app (IN): fully-qualified path name pointing to a guc
 *          envp (IN): environment pointer to pass to pipe_out_cmd
 * returns: capabilities of guc. Return value of 0 is suspicious of
 *          problems with the guc.
 */

extern
unsigned long
guc_versions( const char* prefix, char* app, char* envp[] );
/* purpose: Obtains the versions of a given g-u-c
 * paramtr: prefix (IN): prefix matching component to look for, 
 *                       or NULL to obtain g-u-c's own version number
 *          app (IN): fully-qualified path name pointing to a guc
 *          envp (IN): environment pointer to pass to pipe_out_cmd
 * returns: the version number of the sub component, as major*1000 + minor,
 *          or 0 to indicate that it was not found. 
 * warning: This will invoke g-u-c -versions only once, and cache output. 
 */


#endif /* _CAPABILITIES_H */
