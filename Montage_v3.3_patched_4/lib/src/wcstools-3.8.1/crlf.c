/* File crlf.c
 * June 20, 2006
 * By Doug Mink, Harvard-Smithsonian Center for Astrophysics
 * Send bug reports to dmink@cfa.harvard.edu

   Copyright (C) 2006 
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
static void CRFix();

int
main (ac, av)
int ac;
char **av;
{
    char *fn;
    char *str;

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
    	    printf ("%s:\n", fn);
	CRFix (fn);
	if (verbose)
	    printf ("\n");
	}

    return (0);
}

static void
usage ()
{
    fprintf (stderr,"CRLF: Change carriage returns to linefeeds in a file\n");
    fprintf(stderr,"Usage:  crlf [-v] file1 file2 ... filen\n");
    fprintf(stderr,"  -v: verbose\n");
    exit (1);
}

static void
CRFix (name)

char *name;

{
    char buffer[1000];
    int fd;
    int nbr, i;

    fd = open (name, O_RDONLY);
    nbr = 1000;
    while (nbr > 0) {
	nbr = read (fd, buffer, 1000);
	if (nbr > 0) {
	    for (i = 0; i < nbr; i++) {
		if (buffer[i] == (char) 13)
		    buffer[i] = (char) 10;
		}
	    (void) write (1, buffer, nbr);
	    }
	}
   return;
}
/* Feb 10 1998	New program
 *
 * Apr  3 2005	Declare main to be int
 *
 * Jun 20 2006	Clean up code
 */
