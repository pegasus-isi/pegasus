/* File filename.c
 * February 6, 2008
 * By Doug Mink, Harvard-Smithsonian Center for Astrophysics
 * Send bug reports to dmink@cfa.harvard.edu

   Copyright (C) 2006-2008
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

static int verbose = 0;         /* Verbose/debugging flag */
static int getroot = 0;         /* Return file root */
static int nslash = 1;		/* Start this many slashes from end of name */
static int keepdir = 0;		/* Return directory(ies) as part of name */
static void usage();

int
main (ac, av)
int ac;
char **av;
{
    char *fn, *fn0;
    char *str;
    char *name;
    char *is[10];
    char *endroot;
    int i, n;

    /* crack arguments */
    for (av++; --ac > 0 && *(str = *av) == '-'; av++) {
        char c;
        while ((c = *++str))
        switch (c) {

        case 'v':       /* More verbosity */
            verbose++;
            break;

        case 'r':       /* Return root of filename */
            getroot++;
            break;

        case '/':       /* Keep one more directory in path */
            nslash++;
            break;

        case 'n':       /* Prepend directory to name with . */
            keepdir++;
	    nslash++;
            break;

        default:
            usage();
            break;
        }
    }

    /* There are ac remaining file names starting at av[0] */
    if (ac == 0)
        usage ();

    if (nslash > 10)
	nslash = 10;

    while (ac-- > 0) {
	fn0 = *av++;
	fn = fn0;

	for (n = 0; n < 10; n++)
	    is[n] = NULL;
	if (verbose)
    	    printf ("%s -> ", fn);
	endroot = NULL;
	for (n = 0; n < nslash; n++) {
	    name = strrchr (fn, '/');
	    if (n == 0 && getroot) {
		if (name != NULL)
		    endroot = strrchr (name, '.');
		else
		    endroot = strrchr (fn, '.');
		if (endroot != NULL) {
		    *endroot = (char) 0;
		    if (getroot > 1) {
			if (name != NULL)
			    endroot = strrchr (name, '.');
			else
			    endroot = strrchr (fn, '.');
			if (endroot != NULL)
			    *endroot = (char) 0;
			}
		    }
		}
	    if (name == NULL) {
		name = fn;
		break;
		}
	    else {
		if (name > fn0) {
		    is[n] = name;
		    if (keepdir)
			*name = '.';
		    else
			*name = (char) 0;
		    }
		else
		    break;
		name = name + 1;
		if (verbose) {
		    for (i = 0; i < nslash; i++) {
			if (is[i] != NULL) {
			    if (keepdir)
			 	*(is[i]) = '.';
			    else
			 	*(is[i]) = '/';
			    }
			}
		    printf ("%s\n", name);
		    for (i = 0; i < nslash; i++) {
			if (is[i] != NULL) {
			    if (keepdir)
				*(is[i]) = '.';
			    else
				*(is[i]) = (char) 0;
			    }
			}
		    }
		}
	    }

	for (n = 0; n < nslash; n++) {
	    if (is[n] != NULL) {
		if (keepdir)
		    *(is[n]) = '.';
		else
		    *(is[n]) = '/';
		}
	    }

	printf ("%s\n", name);
	}

    return (0);
}

static void
usage ()
{
    fprintf (stderr,"FILENAME: Drop directory from pathname\n");
    fprintf(stderr,"Usage:  filename [-v/] path1 path2 path3 ...\n");
    fprintf(stderr,"  -/: Keep one more end directory for each /\n");
    fprintf(stderr,"  -n: Prepend one more end directory with .\n");
    fprintf(stderr,"  -r: Root of file name (before first .)\n");
    fprintf(stderr,"  -v: Verbose\n");
    exit (1);
}
/* Aug  3 1998	New program
 *
 * Jun 21 2006	Clean up code
 *
 * Jun  1 2007	Add option to keep directories up from file
 * Jun 11 2007	Add -r option to keep only root of file name (before first .)
 *
 * Feb  6 2008	Add -n option to keep directory with "."
 * Feb  6 2008	Allow more than one -r command
 */
