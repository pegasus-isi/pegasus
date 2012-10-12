/* File matchcat.c
 * January 10, 2007
 * By Doug Mink, Harvard-Smithsonian Center for Astrophysics
 * Send bug reports to dmink@cfa.harvard.edu

   Copyright (C) 2006-2007
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
#include "libwcs/wcscat.h"
#include "libwcs/wcs.h"

#define NMAXCAT 1500000

static int verbose = 0;         /* verbose/debugging flag */
static double matchrad = 5.0;	/* Initial match radius is 5 arcseconds */
int version = 0;		/* If 1, print only program name and version */
static void usage();

static char *RevMsg = "MATCHCAT WCSTools 3.8.1, 14 December 2009, Doug Mink SAO";

main (ac, av)
int ac;
char **av;
{
    char *fn;
    char *str;
    char outfile[256];
    FILE *fcat;
    double delta, dist;

    double ra[NMAXCAT], dec[NMAXCAT];
    double ra2[NMAXCAT], dra[NMAXCAT], ddec[NMAXCAT];
    float as[NMAXCAT], v[NMAXCAT], r[NMAXCAT];
    float dv[NMAXCAT], dr[NMAXCAT], class[NMAXCAT];
    int id[NMAXCAT];
    char line[1000];
    char line1[1000];
    char line2[1000];
    int ncmax = 1000;
    struct Tokens *tokens;
    int ltok, ntok;
    int i, num, j, ij, nid;
    char rastr[32], decstr[32], mvstr[8], mrstr[8], clstr[8];

    /* Check for help or version command first */
    str = *(av+1);
    if (!str || !strcmp (str, "help") || !strcmp (str, "-help"))
	usage();
    if (!strcmp (str, "version") || !strcmp (str, "-version")) {
        version = 1;
	usage();
	}

    /* crack arguments */
    for (av++; --ac > 0 && *(str = *av) == '-'; av++) {
        char c;
        while (c = *++str)
        switch (c) {

        case 'v':       /* more verbosity */
            verbose++;
            break;

        case 'r':       /* match radius in arcseconds */
            if (ac > 1)
		matchrad = atof (*++av);
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

	/* Open ASCII catalog */
	if (!(fcat = fopen (fn, "r"))) {
	    fprintf (stderr,"ASCII catalog %s cannot be read\n", fn);
	    exit (1);
	    }

	/* Ignore first two lines */
	if (fgets (line1, ncmax, fcat) == NULL) {
	    fprintf (stderr,"Cannot read 1st line of ASCII catalog %sd\n", fn);
	    fclose (fcat);
	    exit (1);
	    }
	if (fgets (line2, ncmax, fcat) == NULL) {
	    fprintf (stderr,"Cannot read 2nd line of ASCII catalog %sd\n", fn);
	    fclose (fcat);
	    exit (1);
	    }

	/* Read position information into arrays */
	i = 0;
	while (fgets (line, ncmax, fcat) != NULL) {
	    ntok = setoken (tokens, line, NULL);
	    ltok = getoken (tokens, 1, rastr, 32);
	    ltok = getoken (tokens, 2, decstr, 32);
	    ltok = getoken (tokens, 3, mvstr, 8);
	    ltok = getoken (tokens, 4, mrstr, 8);
	    ltok = getoken (tokens, 5, clstr, 8);
	    as[i] = -1;
	    v[i] = atof (mvstr);
	    r[i] = atof (mrstr);
	    class[i] = atoi (clstr);
	    dec[i] = str2dec (decstr);
	    ra[i] = str2ra (rastr) * cosdeg (dec[i]);
	    ra2[i] = str2ra (rastr);
	    i++;
	    if (i > NMAXCAT)
		break;
	    }
	fclose (fcat);

	/* Set match radius in degrees */
	delta = 5.0 / 3600.0;
	num = i - 1;
	nid = 0;

	/* Read through file */
	for (i = 0; i < num; i++) {

	    /* If current star has not been matched */
	    if (id[i] <= 0) {
		nid = nid + 1;
		id[i] = nid;

		/* Check for matching stars */
		for (j = 1; j < 1000; j++) {
		    ij = i + j;
		    if (ij >= num)
			break;

		    /* Check RA distance first */
		    if ((ra[ij] - ra[i]) > delta)
			break;

		    /* If RA close enough, check radial distance */
		    dist = wcsdist (ra2[i],dec[i],ra2[ij],dec[ij]);
		    if (dist <= delta) {
			id[ij] = nid;
			as[ij] = 3600.0 * dist;
			dv[ij] = v[i] - v[ij];
			dr[ij] = r[i] - r[ij];
			dra[ij] = 3600.0 * (ra[i] - ra[ij]);
			ddec[ij] = 3600.0 * (dec[i] - dec[ij]);
			}
		    }
		}
	    }

	/* Open output file */
	strcpy (outfile, fn);
	strcat (outfile, ".match");
	fcat = fopen (outfile, "w");
        if (fcat == NULL) {
            fprintf (stderr, "Cannot write file %s\n", outfile);
	    exit (1);
	    }

	/* Print header lines */
	fprintf (fcat, "%s", line1);
	fprintf (fcat, "%s", line2);

	/* Print catalog */
	for (i = 0; i < num; i++) {
	    ra2str (rastr, 32, ra2[i], 3);
	    dec2str (decstr, 32, dec[i], 2);
	    fprintf (fcat, "%7d %s %s %6.3f %6.3f %6.3f %6.3f %6.3f ",
		     id[i], rastr, decstr, r[i], v[i], dr[i], dv[i], class[i]);
	    fprintf (fcat, "%9.6f %9.6f %9.6f %9.6f %9.6f %7d\n",
		     as[i], ra2[i], dec[i], dra[i], ddec[i], i);
	    }

	fclose (fcat);
	}
    exit (0);
}

static void
usage ()
{
    fprintf (stderr,"%s\n",RevMsg);
    if (version)
        exit (-1);
    fprintf (stderr,"Match catalog entries after combining, RA-sorting them\n");
    fprintf (stderr,"usage: matchcat [-v][-r rad] file.cat ...\n");
    fprintf (stderr,"  -r rad: Combine entries within this arcsecond distance\n");
    fprintf (stderr,"  -v: Verbose\n");
    exit (1);
}
/*
 * Jan 10 2007	Declare RevMsg static, not const
 */
