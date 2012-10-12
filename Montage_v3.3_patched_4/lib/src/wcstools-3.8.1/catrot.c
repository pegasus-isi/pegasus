/* File catrot.c
 * April 6, 2007
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
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>
#include <math.h>
#include "libwcs/fitsfile.h"

static void usage();
static void imRot ();
extern char *RotFITS();

#define MAXFILES 1000
static int maxnfile = MAXFILES;

static int verbose = 0;	/* verbose/debugging flag */
static int mirror = 0;	/* reflect image across vertical axis */
static int automirror = 0;	/* reflect image if IRAF header says to */
static int rotate = 0;	/* rotation in degrees, degrees counter-clockwise */
char outname[128];		/* Name for output image */
static int bitpix = 0;	/* number of bits per pixel (FITS code) */
static int fitsout = 0;	/* Output FITS file from IRAF input if 1 */
static int nsplit = 0;	/* Output multiple FITS files from n-extension file */
static int overwrite = 0;	/* allow overwriting of input image file */
static char *RevMsg = "IMROT WCSTools 3.8.1, 14 December 2009, Doug Mink (dmink@cfa.harvard.edu)";
static int version = 0;		/* If 1, print only program name and version */
static int xshift = 0;
static int yshift = 0;
static int shifted = 0;
static char newline = 10;

static int nlines;	/* Number of lines in catalog */
static char *daobuff;

main (ac, av)
int ac;
char **av;
{
    char *str;
    char c;
    int readlist = 0;
    char *lastchar;
    char filename[128];
    FILE *flist;
    char **fn, *fname;
    char *listfile;
    char *fni;
    int lfn, i, ifile, nfile;
    double dx, dy;

    nfile = 0;
    fn = (char **)calloc (maxnfile, sizeof(char *));
    outname[0] = 0;

    /* Check for help or version command first */
    str = *(av+1);
    if (!str || !strcmp (str, "help") || !strcmp (str, "-help"))
	usage();
    if (!strcmp (str, "version") || !strcmp (str, "-version")) {
	version = 1;
	usage();
	}

    /* Loop through the arguments */
    for (av++; --ac > 0; av++) {
	str = *av;

	/* List of files to be read */
	if (*str == '@') {
	    readlist++;
	    listfile = ++str;
	    str = str + strlen (str) - 1;
	    av++;
	    ac--;
	    str = str - 1;
	    }

	/* Set image shifts */
	else if (isnum (str)) {
	    if (shifted) {
		dy = atof (str);
		if (dy < 0)
		    yshift = (int) (dy - 0.5);
		else
		    yshift = (int) (dy + 0.5);
		}
	    else {
		shifted = 1;
		dx = atof (str);
		if (dx < 0)
		    xshift = (int) (dx - 0.5);
		else
		    xshift = (int) (dx + 0.5);
		}
	    }

	/* Parameters */
	else if (str[0] == '-') {
	    while ((c = *++str) != 0) {
		switch (c) {

		case 'l':	/* image flipped around N-S axis */
		    mirror = 1;
		    break;

		case 'r':	/* Rotation angle in degrees */
		    if (ac < 2)
			usage ();
		    rotate = (int) atof (*++av);
		    ac--;
		    break;

		case 'v':	/* more verbosity */
		    verbose++;
		    break;

		default:
		    usage();
		    break;
		}
		}
	    }

        /* Image file */
        else if (isfile (str)) {
            if (nfile >= maxnfile) {
                maxnfile = maxnfile * 2;
                fn = (char **) realloc ((void *)fn, maxnfile);
                }
            fn[nfile] = str;
            nfile++;
            }

        else {
	    fprintf (stderr,"CATROT: %s is not an image catalog file \n",str);
            usage();
            }

    }

    /* Process files in file of filenames */
    if (readlist) {
	if ((flist = fopen (listfile, "r")) == NULL) {
	    fprintf (stderr,"CATROT: List file %s cannot be read\n",
		     listfile);
	    usage ();
	    }
	while (fgets (filename, 128, flist) != NULL) {
	    lastchar = filename + strlen (filename) - 1;
	    if (*lastchar < 32) *lastchar = 0;
	    CatRot (filename);
	    if (verbose)
		printf ("\n");
	    }
	fclose (flist);
	}

    /* Process files on command line */
    else if (nfile > 0) {
	for (ifile = 0; ifile < nfile; ifile++) {
	    fname = fn[ifile];
	    lfn = strlen (fname);
	    if (lfn < 8)
		lfn = 8;
	    if (verbose)
		printf ("%s:\n", fname);
	    CatRot (fname);
	    if (verbose)
  		printf ("\n");
	    }
	}

    /* If no files processed, print usage */
    else
	usage ();

    return (0);
}

static void
usage ()
{
    fprintf (stderr,"%s\n",RevMsg);
    if (version)
	exit (-1);
    fprintf (stderr,"Rotate and/or Reflect catalog of objects in an image\n");
    fprintf(stderr,"Usage: [-vl][-o outputfile][-r rot] file ...\n");
    fprintf(stderr,"  -l: reflect image across vertical axis\n");
    fprintf(stderr,"  -r: image rotation angle in degrees (default 0)\n");
    fprintf(stderr,"  -v: verbose\n");
    exit (1);
}

static void
CatRot (name)
char *name;
{
    char newname[256];		/* Name for revised image */
    char *ext;
    char *fname;
    char extname[16];
    int lext, lroot;
    int bitpix0;
    char echar;
    char temp[8];
    char history[64];
    char pixname[256];
    double ctemp;

    /* If not overwriting input file, make up a name for the output file */
    fname = strrchr (name, '/');
    if (fname)
	fname = fname + 1;
    else
	fname = name;
    ext = strrchr (fname, '.');
    if (ext != NULL) {
	lext = (fname + strlen (fname)) - ext;
	lroot = ext - fname;
	strncpy (newname, fname, lroot);
	*(newname + lroot) = 0;
	}
    else {
	lext = 0;
	lroot = strlen (fname);
	strcpy (newname, fname);
	}

    /* Create new file name */
    if (shifted)
	strcat (newname, "s");
    if (mirror)
	strcat (newname, "m");
    if (rotate != 0) {
	strcat (newname, "r");
	if (rotate < 10 && rotate > -1)
	    sprintf (temp,"%1d",rotate);
	else if (rotate < 100 && rotate > -10)
	    sprintf (temp,"%2d",rotate);
	else if (rotate < 1000 && rotate > -100)
	    sprintf (temp,"%3d",rotate);
	else
	    sprintf (temp,"%4d",rotate);
	strcat (newname, temp);
	}
    if (lext > 0)
	strcat (newname, ext);

    if (daoopen (newname) > 0) {
	line = daobuff;

    /* Loop through catalog */
	for (iline = 1; iline <= nlines; iline++) {
	    line = daoline (iline, line);
	    if (line == NULL) {
		fprintf (stderr,"CATROT: Cannot read line %d\n", iline);
		break;
		}
	    else if (line[0] == '#') {
		printf ("%s", line);
		}
	    else {

		/* Extract X, Y, magnitude  */
		sscanf (line,"%lg %lg %lg", &xi, &yi, &magi);

		/* Rotate star position */
		nstars++;
		rotstar (&xi, *yi, nxpix, nypix, rotate, mirror);

		if (nlog == 1)
		    fprintf (stderr,"CATROT: %6d: %9.5f %9.5f %6.2f\n",
			   nstars,xi,yi,magi);
		}

	    /* Log operation */
	    if (nlog > 0 && iline%nlog == 0)
		fprintf (stderr,"CATROT: %5d / %5d / %5d stars from catalog %s\r",
			nstars, iline, nlines, daocat);

	    /* End of star loop */
	    }

	/* End of open catalog file */
	}

/* Summarize search */
    if (nlog > 0)
	fprintf (stderr,"DAOREAD: Catalog %s : %d / %d / %d found\n",
		 daocat, nstars, iline, nlines);

    free (daobuff);

    return (nstars);
}

/* Apr 15 1996	New program
 * Apr 18 1996	Add option to write to current working directory
 * May  2 1996	Pass filename to rotFITS
 * May 28 1996	Change rotFITS to RotFITS
 * Jun  6 1996	Always write to current working directory
 * Jun 14 1996	Use single image buffer
 * Jul  3 1996	Allow optional overwriting of input image
 * Jul 16 1996	Update header reading and allocation
 * Aug 26 1996	Change HGETC call to HGETS; pass LHEAD in IRAFWIMAGE
 * Aug 27 1996	Remove unused variables after lint
 *
 * Feb 21 1997  Check pointers against NULL explicitly for Linux
 * Aug 13 1997	Fix bug when overwriting an image
 * Dec 15 1997	Add capability of reading and writing IRAF 2.11 images
 *
 * Feb 24 1998	Add ext. to filename if writing part of multi-ext. file
 * May 26 1998	Fix bug when writing .imh images
 * May 27 1998	Include fitsio.h instead of fitshead.h
 * Jul 24 1998	Make irafheader char instead of int
 * Aug  6 1998	Change fitsio.h to fitsfile.h
 * Oct 14 1998	Use isiraf() to determine file type
 * Nov 30 1998	Add version and help commands for consistency
 * Jun  8 1999  Return image pointer from RotFITS, not flag
 * Oct 22 1999	Drop unused variables after lint
 *
 * Jan 24 2000	Add to name if BITPIX is changed
 * Mar 23 2000	Use hgetm() to get the IRAF pixel file name, not hgets()
 * Jul 20 2000	Use .fits, not .fit, extension on output FITS file names
 * Jul 20 2000	Add -s option to split multi-extension FITS files
 * Sep 12 2000	Echo new file name to standard output, if not verbose
 * Sep 12 2000	Use .extname if multi-extension extraction, not _number
 *
 * Jan 18 2001	Add automirror -a option
 * Feb 13 2001	Add -o name option to -o argument (from imwcs)
 *
 * Jan 28 2004	Add option to shift file data within file (before rotating)
 * May  6 2004	Add -i to avoid appending primary header to extension header
 * Jun 14 2004	Write HISTORY only if BITPIX is changed
 */
