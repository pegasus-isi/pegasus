/* File filext.c
 * January 10, 2007
 * By Doug Mink, Harvard-Smithsonian Center for Astrophysics
 * Send bug reports to dmink@cfa.harvard.edu

   Copyright (C) 2002-2007
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
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>

static int verbose = 0;         /* verbose/debugging flag */
static void usage();

int
main (ac, av)
int ac;
char **av;
{
    char *fn;
    char *str;
    char *ext, *ext2;

    /* crack arguments */
    for (av++; --ac > 0 && *(str = *av) == '-'; av++) {
        char c;
        while ((c = *++str))
        switch (c) {

        case 'v':       /* more verbosity */
            verbose++;
            break;

        default:
            usage();
            break;
        }
    }

    /* There are ac remaining file names starting at av[0] */
    if (ac == 0)
        usage ();

    while (ac-- > 0) {
	fn = *av++;
	if (verbose)
    	    printf ("%s -> ", fn);
	ext = strrchr (fn, '[');
	if (ext != NULL) {
	    ext = ext + 1;
	    ext2 = strrchr (fn, ']');
	    if (ext2 != NULL)
		*ext2 = (char) 0;
	    }
	else {
	    ext = strrchr (fn, ',');
	    if (ext != NULL)
		ext = ext + 1;
	    else {
		ext = strrchr (fn, '.');
		if (ext != NULL)
		    ext = ext + 1;
		}
	    }
	printf ("%s\n", ext);
	}

    return (0);
}

static void
usage ()
{
    fprintf (stderr,"FILEXT: Print file name extension\n");
    fprintf(stderr,"Usage:  filext file1 file2 file3 ...\n");
    exit (1);
}
/* Apr 29 2002	New program
 *
 * Jan 10 2007	Drop unused variables
 */
