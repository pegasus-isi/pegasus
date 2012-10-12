/*** File gsc2cat.c
 *** November 28, 2007
 *** By Doug Mink, SAO

   Copyright (C) 2007 
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

#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include "libwcs/fitsfile.h"

static char rootdir[128] = "/data/astrocat2/gsc2";
static int n = 0;
static char *tbuff = NULL;	/* FITS table buffer */
static int lbuff = 0;		/* Length of FITS table buffer */
static int version = 0;
static int verbose = 0;
static int nlog = 10000;
static void usage();
void SaveGSC2();

static char *RevMsg = "GSC2CAT WCSTools 3.8.1, 14 December 2009, Doug Mink (dmink@cfa.harvard.edu)";

main (ac, av)
int ac;
char **av;
{
    char *str, *str1;
    int i, j;
    int readlist = 0;
    char dir[256];
    char filename[128];
    char *lastchar;
    FILE *flist;
    char *listfile = NULL;

    /* Check for help or version command first */
    str = *(av+1);
    if (!str || !strcmp (str, "help") || !strcmp (str, "-help"))
	usage();
    if (!strcmp (str, "version") || !strcmp (str, "-version")) {
	version = 1;
	usage();
	}

    /* crack arguments */
    for (av++; --ac > 0 && (*(str = *av)=='-' || *str == '@'); av++) {
	char c;
	if (*str == '@')
	    str = str - 1;
	while ((c = *++str)) {
	    switch (c) {

		case 'v':	/* More verbosity */
		    verbose++;
		    break;

		case 'd':	/* Set new root directory */
		    av++;
		    strcpy (rootdir, *av);
		    if (verbose)
			fprintf (stderr,"Writing GSC2.3 to %s\n", rootdir);
		    ac--;
		    break;

		case 'l':	/* Set new root directory */
		    av++;
		    nlog = (int) atof (*av);
		    if (verbose)
			fprintf (stderr,"Logging every %d entries\n", nlog);
		    ac--;
		    break;

		case '@':	/* List of files to be read */
		    readlist++;
		    listfile = ++str;
		    str = str + strlen (str) - 1;
		    av++;
		    ac--;
		    break;

		default:
		    usage();
		    break;
		}
	    }
	}

    if (ac == 0 && !readlist)
	usage ();

    /* Allocate FITS table buffer which is saved between calls */
    if (lbuff < 1) {
	lbuff = 10000;
	tbuff = (char *)calloc (lbuff, sizeof (char));
	if (tbuff == NULL) {
	    fprintf (stderr, "GSCREAD: cannot allocate FITS table buffer\n");
	    return (0);
	    }
	}

    /* Find number of images to search and leave listfile open for reading */
    if (readlist) {
	if ((flist = fopen (listfile, "r")) == NULL) {
	    fprintf (stderr,"GSC2CAT: List file %s cannot be read\n",
		     listfile);
	    usage ();
	    }
	while (fgets (filename, 128, flist) != NULL) {
	    lastchar = filename + strlen (filename) - 1;
	    if (*lastchar < 32) *lastchar = 0;
	    (void) SaveGSC2 (filename);
	    if (verbose)
		printf ("\n");
	    }
	fclose (flist);
	}

    /* If no arguments left, print usage */
    if (ac == 0)
	usage ();

    while (ac-- > 0) {
	char *fn = *av++;
	(void) SaveGSC2 (fn);
	if (verbose)
	    printf ("\n");
	}

    return (0);
}
    

static void
usage ()
{
    fprintf (stderr,"%s\n",RevMsg);
    if (version)
        exit (-1);
    fprintf (stderr,"Resort the GSC2 file from FITS tables\n");
    fprintf (stderr,"Usage: [-d directory] [-l nlog] [-v] [@filelist] FITS table file(s)\n");
    fprintf (stderr,"  -d dir: Set new root directory\n");
    fprintf (stderr,"  -l num: Set new logging interval\n");
    fprintf (stderr,"  -v: Verbose\n");
    exit (1);
}



void
SaveGSC2 (infile)

char *infile; 	/* Relative pathname of input file */

{
    double ra, dec, spd, mf, mj, mn, mb, mv, ef, ej, en, eb, ev;
    int id, class, fvar, fmult;
    int nrows, nk, irow, nbline, nbhead;
    int fd;
    int i, j;
    char inpath[256];
    FILE *outfile = NULL;
    FILE *sortfile = NULL;
    char file[256], file0[256], dir[256];
    char line[512];
    int ni = 0;
    int nt = 0;
    int nz = 0;
    int z;
    struct Keyword kw[60];	/* Structure for desired entries */
    struct Keyword *kwn;

    /* When first called, set up south polar distance zone directories */
    if (n == 0) {
	for (j = 0; j < 180; j = j + 1) {
	    sprintf (dir,"%s/%03d", rootdir, j);
	    if (access (dir, F_OK&W_OK)) {
		if (mkdir (dir, 00777))
		   fprintf (stderr, "GSC2CAT: Cannot make directory %s\n", dir);
		}
	    }
	n++;
	}

    for (i = 0; i < 512; i++)
	line[i] = 0;

    strcpy (inpath, infile);
    strcat (inpath, ",2");

    /* Initialize file name to all zeroes */
    for (i = 0; i < 256; i++)
	file[i] = (char) 0;
    strcpy (file0, file);

    kwn = kw;
    nk = 0;
    if ((fd = fitsrtopen (inpath, &nk, &kwn, &nrows, &nbline, &nbhead)) < 1) {
	fprintf (stderr,"GSC2CAT: %s is not a FITS table file\n", inpath);
	return;
	}

    /* Read through this file */
    for (irow = 1; irow <= nrows; irow++) {
	if (fitsrtline (fd, nbhead, lbuff, tbuff, irow, nbline, line) < nbline) {
	    continue;
	    }

	/*  Compute South Polar Distance from declination */
	dec = ftgetr8 (line, &kw[4]);
	spd = dec + 90.0;
	ni = ni + 1;

	/* Compute zone number and skip if out of range */
	z = (int) spd;
	if (z > 180)
	    continue;

	/* Set output directory name */
	sprintf (dir,"%s/%03d", rootdir, z);

	/* Set output file name */
	i = (int) (((spd + 0.0000001) * 10.0) - (z * 10));
	sprintf (file, "%s/g%04d.cat", dir, i);

	/* If not the same file as last, close old file; open new */
	if (strcmp (file, file0)) {
	    /* if (nz > 1)
		printf (" GSC2CAT: %7d stars written to %s\n", nz, file0); */
	    if (nz == 1) {
	/* 	printf (" GSC2CAT: %7d stars written to %s", nz, file0);
		printf (" %10.6f %10.6f %d\n", ra, dec, id); */
		sortfile = fopen ("resort", "a");
		if (sortfile == NULL)
		    sortfile = fopen ("resort", "w");
		if (sortfile != NULL) {
		    fprintf (sortfile,"%s\n", file0);
		    fclose (sortfile);
		    }
		}

	    if (outfile != NULL)
		fclose (outfile);
	    outfile = fopen (file, "a");
	    if (outfile == NULL) {
		outfile = fopen (file, "w");
		if (outfile == NULL) {
		    printf ("GSC2CAT: Cannot write to file %s\n", file);
		    continue;
		    }
		}
	    strcpy (file0, file);
	    nz = 0;
	    }

	/* Right ascension */
	ra = ftgetr8 (line, &kw[3]);

	/* ID */
	id = ftgeti4 (line, &kw[0]);

	/* F, J, N, B, V magnitudes */
	mf = ftgetr8 (line, &kw[13]);
	mj = ftgetr8 (line, &kw[16]);
	mn = ftgetr8 (line, &kw[22]);
	mb = ftgetr8 (line, &kw[28]);
	mv = ftgetr8 (line, &kw[19]);

	/* F, J, N, B, V magnitude errors */
	ef = ftgetr8 (line, &kw[14]);
	ej = ftgetr8 (line, &kw[17]);
	en = ftgetr8 (line, &kw[23]);
	eb = ftgetr8 (line, &kw[29]);
	ev = ftgetr8 (line, &kw[20]);

	/* Classification */
	class = ftgeti4 (line, &kw[46]);

	/* Variability flag */
	fvar = ftgeti4 (line, &kw[51]);

	/* Multiplicity flag */
	fmult = ftgeti4 (line, &kw[52]);

	nz = nz + 1;

	/* Append position and magnitudes from this line to current file */
	fprintf (outfile,"%10.6f %10.6f %8d %6.3f %6.3f %6.3f %6.3 %6.3 %5.3f %5.3f %5.3f %5.3f %5.3f %1d\n",
		 ra, dec, id, mf, mj, mn, mb, mv, mf, mj, mn, mb, mv, class);
	nt = nt + 1;
	if (nt % nlog == 0)
	    fprintf (stderr, "%10d stars read to %10.6f %10.6f (%s)\n",
		     nt, ra, dec, file);
	}

    /* Close output file */
    fclose (outfile);

    return;
}
/* Nov 28 2007	New program, based on tmcat.c
 */
