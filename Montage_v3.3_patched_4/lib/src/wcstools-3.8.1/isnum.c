/* File isnum.c
 * January 10, 2007
 * By Doug Mink, Harvard-Smithsonian Center for Astrophysics
 * Send bug reports to dmink@cfa.harvard.edu

   Copyright (C) 2001-2007
   Smithsonian Astrophysical Observatory, Cambridge, MA USA

   This program is free software; you can redistribute it and/or
   modify it under the terms of the GNU General Public License
   as published by the Free Software Foundation; either version 2
   of the License, or (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software Foundation,
   Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA
 *
 * Return 1 if argument is an integer, 2 if it is floating point, else 0
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "libwcs/fitshead.h"

static char *RevMsg = "ISNUM WCSTools 3.8.1, 14 December 2009, Doug Mink (dmink@cfa.harvard.edu)";

int
main (ac, av)
int ac;
char **av;
{
    char *str;

    /* Check for version or help command first */
    str = *(av+1);
    if (!str || !strcmp (str, "help") || !strcmp (str, "-help")) {
	fprintf (stderr,"%s\n",RevMsg);
	fprintf (stderr,"Usage: Return 1 if argument is an integer, ");
	fprintf (stderr,"2 if it is floating point, else 0\n");
	exit (1);
	}
    else if (!strcmp (str, "version") || !strcmp (str, "-version")) {
	fprintf (stderr,"%s\n",RevMsg);
	exit (1);
	}

    /* check to see if this is a number */
    else
	printf ("%d\n", isnum (str));

    exit (0);
}
/* Nov  7 2001	New program
 *
 * Apr 11 2005	Print version
 *
 * Apr  3 2006	Declare main to be int
 *
 * Jan 10 2007	Drop unused variable fn
 */
