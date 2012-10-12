/* File delwcs.c
 * June 21, 2006
 * By Doug Mink, after University of Iowa code
 * (Harvard-Smithsonian Center for Astrophysics)
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
static void DelWCS ();
extern int DelWCSFITS ();

static int verbose = 0;		/* Verbose/debugging flag */
static int newimage = 0;	/* New image flag */
static int readimage = 1;	/* Read and write image as well as header */
static char *RevMsg = "DELWCS WCSTools 3.8.1, 14 December 2009, Doug Mink (dmink@cfa.harvard.edu)";
static int version = 0;		/* If 1, print only program name and version */

int
main (ac, av)
int ac;
char **av;
{
    char *str;

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
	while ((c = *++str))
	switch (c) {
	case 'b':	/* Leave blank lines at end after removing keywords */
	    setheadshrink (0);
	    readimage = 0;
	    break;
	case 'n':	/* New image for output */
	    newimage++;
	    break;
	case 'v':	/* more verbosity */
	    verbose++;
	    break;
	default:
	    usage();
	    break;
	}
    }

    /* now there are ac remaining file names starting at av[0] */
    if (ac == 0)
	usage ();

    while (ac-- > 0) {
	char *fn = *av++;
	DelWCS (fn);
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
    fprintf (stderr,"Delete WCS in FITS and IRAF image files\n");
    fprintf(stderr,"usage: delwcs [-nv] file.fits ...\n");
    fprintf(stderr,"  -b: When overwriting image header, leave blank lines \n");
    fprintf(stderr,"  -n: write new file, else overwrite \n");
    fprintf(stderr,"  -v: verbose\n");
    exit (1);
}


static void
DelWCS (filename)

char *filename;

{
    char *header;	/* FITS image header */
    char *image;	/* Image pixels */
    int lhead;		/* Maximum number of bytes in FITS header */
    int nbhead;		/* Actual number of bytes in FITS header */
    int iraffile;	/* 1 if IRAF image */
    char *irafheader = NULL;	/* IRAF image header */
    char pixname[256];	/* IRAF pixel file name */
    char newname[256];
    int lext, lroot;
    char *ext, *fname, *imext, *imext1;
    char echar;

    if (strchr (filename, ',') || strchr (filename,'[')) {
	setheadshrink (0);
	readimage = 0;
	}
    if (isiraf (filename))
	readimage = 0;
    if (newimage)
	readimage = 1;

    /* Open image if IRAF .imh image */
    if (isiraf (filename)) {
	iraffile = 1;
	if ((irafheader = irafrhead (filename, &lhead)) != NULL) {
	    if ((header = iraf2fits (filename, irafheader, lhead, &nbhead))==NULL) {
		fprintf (stderr, "Cannot translate IRAF header %s/n",filename);
		free (irafheader);
		return;
		}
	    if (readimage) {
		if ((image = irafrimage (header)) == NULL) {
                    hgetm (header,"PIXFIL", 255, pixname);
                    fprintf (stderr, "Cannot read IRAF pixel file %s\n", pixname);
                    free (irafheader);
                    free (header);
                    return;
		    }
                }
	    }
	else {
	    fprintf (stderr, "Cannot read IRAF header file %s\n", filename);
	    return;
	    }
	}

    /* Open FITS file if not an IRAF .imh image */
    else {
	iraffile = 0;
	if ((header = fitsrhead (filename, &lhead, &nbhead)) != NULL) {
	    if (readimage) {
		if ((image = fitsrimage (filename, nbhead, header)) == NULL) {
		    fprintf (stderr, "Cannot read FITS image %s\n", filename);
		    free (header);
		    return;
		    }
		}
	    }
	else {
	    fprintf (stderr, "Cannot read FITS file %s\n", filename);
	    return;
	    }
	}

    /* Make up name for new FITS or IRAF output file */
    if (newimage) {

    /* Remove directory path and extension from file name */
	fname = strrchr (filename, '/');
	if (fname)
	    fname = fname + 1;
	else
	    fname = filename;
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
	imext = strchr (fname, ',');
	imext1 = NULL;
	if (imext == NULL) {
	    imext = strchr (fname, '[');
	    if (imext != NULL) {
		imext1 = strchr (fname, ']');
		*imext1 = (char) 0;
		}
	    }
	if (imext != NULL) {
	    strcat (newname, "_");
	    strcat (newname, imext+1);
	    }
	if (fname)
	    fname = fname + 1;
	else
	    fname = filename;
	strcat (newname, "e");
	if (lext > 0) {
	    if (imext != NULL) {
		echar = *imext;
		*imext = (char) 0;
		strcat (newname, ext);
		*imext = echar;
		if (imext1 != NULL)
		    *imext1 = ']';
		}
	    else
		strcat (newname, ext);
	    }
	}
    else {
	if (strchr (filename, ',') || strchr (filename,'['))
	    setheadshrink (0);
	strcpy (newname, filename);
	}

    if (verbose) {
	fprintf (stderr,"%s\n",RevMsg);
	fprintf (stderr,"Remove World Coordinate System from ");
	if (iraffile)
	    fprintf (stderr,"IRAF image file %s\n", filename);
	else
	    fprintf (stderr,"FITS image file %s\n", filename);
	}

    if (DelWCSFITS (header, verbose) < 1) {
	if (verbose)
	    printf ("%s: no WCS fields found -- file unchanged\n", filename);
	}
    else  {
	if (iraffile) {
	    if (irafwhead (newname, lhead, irafheader, header) < 1)
		fprintf (stderr, "%s: Could not write FITS file\n", newname);
	    else {
		if (verbose)
		    printf ("%s: written successfully without WCS.\n", newname);
		}
	    }
	else if (readimage) {
	    if (fitswimage (newname, header, image) < 1)
		fprintf (stderr, "%s: Could not write FITS file\n", newname);
	    else {
		if (verbose)
		    printf ("%s: written successfully without WCS.\n", newname);
		}
	    }
	else {
	    if (fitswhead (newname, header) < 1)
		fprintf (stderr, "%s: Could not overwrite FITS header\n",
			 newname);
	    else {
		if (verbose)
		    printf ("%s: rewritten successfully without WCS.\n",
			    newname);
		}
	    }
	}

    free (header);
    free (image);
    return;
}
/*
 * Feb 23 1996	New program split off from SETWCS
 * Apr 15 1996	Move delWCSFITS subroutine to libwcs imdelwcs.c
 * Apr 15 1996	Drop name as argument to delWCSFITS
 * May 31 1996	Rename delPos to DelWCS
 * Aug 26 1996	Change HGETC call to HGETS
 * Aug 27 1996	Fix IRAFRHEAD arguments after lint
 * Oct 16 1996	Add newlines to heading
 *
 * Feb 21 1997  Check pointers against NULL explicitly for Linux
 *
 * Apr 14 1998	Version 2.2: deletes more parameters
 * May 27 1998	Include fitsio.h instead of fitshead.h
 * Jul 24 1998	Make irafheader char instead of int
 * Aug  6 1998	Change fitsio.h to fitsfile.h
 * Oct 28 1998	Add option to write a new file
 * Nov 30 1998	Add version and help commands for consistency
 *
 * Oct 21 1999	Drop unused variables after lint
 *
 * Mar 23 2000	Use hgetm() to get the IRAF pixel file name, not hgets()

 * Jul  1 2004	Call setheadshrink() to keep blank lines if FITS extension
 *
 * May 22 2006	Add -b to force setheadshrink
 * Jun 21 2006	Clean up code
 */
