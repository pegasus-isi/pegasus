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
 *
 */

/*
 * Author : Mei-hui Su
 * Revision : $REVISION$
 */

#ifndef mympi_H
#define  mympi_H

extern int *init_my_list(int);
extern void reset_idle_node(int);
extern void set_idle_node(int);
extern int next_idle_node();
extern void free_my_list();
extern int all_idling();

#endif
