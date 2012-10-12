/* File isrange.c
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
 * Return 1 if argument is a range
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static char *RevMsg = "ISRANGE WCSTools 3.8.1, 14 December 2009, Doug Mink (dmink@cfa.harvard.edu)";

static int isrange();

int
main (ac, av)
int ac;
char **av;
{
    char *str;

    /* Check for help or version command first */
    str = *(av+1);
    if (!str || !strcmp (str, "help") || !strcmp (str, "-help")) {
	fprintf (stderr,"%s\n",RevMsg);
	fprintf (stderr,"Usage:  Return 1 if argument is a range of numbers: n1[-n2[xs]],...\n");
	fprintf (stderr,"        where n1=first number, n2=last number, and s=step size.\n");
	exit (1);
	}

    else if (!strcmp (str, "version") || !strcmp (str, "-version")) {
	fprintf (stderr,"%s\n",RevMsg);
	exit (1);
	}

    /* check to see if this is a range */
    else
	printf ("%d\n", isrange (str));

    exit (0);
}


/* ISRANGE -- Return 1 if string is a range, else 0 */

static int
isrange (string)

char *string;		/* String which might be a range of numbers */

{
    int i, lstr;

    /* If range separators present, check to make sure string is range */
    if (strchr (string+1, '-') || strchr (string+1, ',')) {
	lstr = strlen (string);
	for (i = 0; i < lstr; i++) {
	    if (strchr ("0123456789-,.x", (int)string[i]) == NULL)
		return (0);
	    }
	return (1);
	}
    else
	return (0);
}
/* Dec 14 2001	New program
 *
 * Apr 11 2005	Print version; improve online documentation
 *
 * Mar  3 2006	Declare main to be int
 * Jun 20 2006	Drop unused variable fn
 *
 * Jan 10 2007	Declare RevMsg static, not const
 */
