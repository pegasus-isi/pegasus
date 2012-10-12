/* File subpix.c
 * June 21, 2006
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
#include "libwcs/wcs.h"

static void usage();
static void SubPix();

static int newimage = 0;
static int verbose = 0;		/* verbose flag */
static int version = 0;		/* If 1, print only program name and version */

static char *RevMsg = "SUBPIX WCSTools 3.8.1, 14 December 2009, Doug Mink (dmink@cfa.harvard.edu)";

int
main (ac, av)
int ac;
char **av;
{
    char *str;
    char *fn;
    char *value[100];
    int i, x[100], y[100];

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
	case 'v':	/* more verbosity */
	    verbose++;
	    break;
	case 'n':	/* ouput new file */
	    newimage++;
	    break;
	default:
	    usage ();
	    break;
	}
    }

    /* now there are ac remaining arguments starting at av[0] */
    if (ac == 0)
	usage();

    fn = *av++;

    i = 0;
    while (--ac > 2) {
	x[i] = atoi (*av++);
	ac--;
	y[i] = atoi (*av++);
	ac--;
	value[i] = *av++;
	i++;
	}
    if (i > 0)
        SubPix (fn,i,x,y,value);

    return (0);
}

static void
usage ()
{
    fprintf (stderr,"%s\n",RevMsg);
    if (version)
	exit (-1);
    fprintf (stderr,"Subtract from pixel of FITS or IRAF image file\n");
    fprintf(stderr,"Usage: subpix [-vn] file.fts x y value...\n");
    fprintf(stderr,"  -n: write new file, else overwrite \n");
    fprintf(stderr,"  -v: verbose\n");
    exit (1);
}


static void
SubPix (filename, n, x, y, value)

char	*filename;	/* FITS or IRAF file filename */
int	n;		/* number of pixels to change */
int	*x, *y;		/* Horizontal and vertical coordinates of pixel */
			/* (1-based) */
char	**value;	/* value to insert into pixel */

{
    char *image;		/* FITS image */
    char *header;		/* FITS header */
    int lhead;			/* Maximum number of bytes in FITS header */
    int nbhead;			/* Actual number of bytes in FITS header */
    int iraffile;		/* 1 if IRAF image */
    char *irafheader = NULL;	/* IRAF image header */
    int i, lext, lroot;
    char *imext, *imext1;
    char newname[256];
    char pixname[256];
    char tempname[256];
    char history[64];
    char *ext, *fname;
    char echar;
    char newline[1];
    double dpix, dpix0, dpix1;
    double bzero;		/* Zero point for pixel scaling */
    double bscale;		/* Scale factor for pixel scaling */
    int bitpix,xdim,ydim;

    newline[0] = 10;
    strcpy (tempname, "fitshead.temp");

    /* Open IRAF image and header */
    if (isiraf (filename)) {
	iraffile = 1;
	if ((irafheader = irafrhead (filename, &lhead)) != NULL) {
	    if ((header = iraf2fits (filename, irafheader, lhead, &nbhead)) == NULL) {
		free (irafheader);
                fprintf (stderr, "Cannot translate IRAF header %s/n", filename);
                return;
                }
	    if ((image = irafrimage (header)) == NULL) {
		hgetm (header,"PIXFIL", 255, pixname);
		fprintf (stderr, "Cannot read IRAF pixel file %s\n", pixname);
		free (irafheader);
		free (header);
		return;
		}
	    }
	else {
	    fprintf (stderr, "Cannot read IRAF header file %s\n", filename);
	    return;
	    }
	}

    /* Read FITS image and header */
    else {
	iraffile = 0;
	if ((header = fitsrhead (filename, &lhead, &nbhead)) != NULL) {
	    if ((image = fitsrimage (filename, nbhead, header)) == NULL) {
		fprintf (stderr, "Cannot read FITS image %s\n", filename);
		free (header);
		return;
		}
	    }
	else {
	    fprintf (stderr, "Cannot read FITS file %s\n", filename);
	    return;
	    }
	}
    if (verbose)
	fprintf (stderr,"%s\n",RevMsg);

    /* Add specified value to specified pixel */
    hgeti4 (header,"BITPIX",&bitpix);
    hgeti4 (header,"NAXIS1",&xdim);
    hgeti4 (header,"NAXIS2",&ydim);
    bzero = 0.0;
    hgetr8 (header,"BZERO",&bzero);
    bscale = 1.0;
    hgetr8 (header,"BSCALE",&bscale);

    for (i = 0; i < n; i++) {
	if (strchr (value[i],(int)'.'))
	    dpix = (double) atoi (value[i]);
	else
	    dpix = atof (value[i]);
	dpix0 = getpix (image,bitpix,xdim,ydim,bzero,bscale,x[i]-1,y[i]-1);
	dpix1 = dpix0 - dpix;
	putpix (image,bitpix,xdim,ydim,bzero,bscale,x[i]-1,y[i]-1,dpix1);

	/* Note addition as history line in header */
	if (bitpix > 0) {
	    int ipix = (int)dpix;
	    sprintf (history, "SUBPIX: %d subtracted from pixel at row %d, column %d",
		     ipix,x[i],y[i]);
	    }
	else if (dpix < 1.0 && dpix > -1.0)
	    sprintf (history, "SUBPIX: %f subtracted from pixel at row %d, column %d",
		     dpix,x[i],y[i]);
	else
	    sprintf (history,"SUBPIX: %.2f subtracted from pixel at row %d, column %d",
		     dpix,x[i],y[i]);
	hputc (header,"HISTORY",history);
	if (verbose)
	    printf ("%s\n", history);
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
    else
	strcpy (newname, filename);

    /* Write fixed header to output file */
    if (iraffile) {
	if (irafwimage (newname,lhead,irafheader,header,image) > 0 && verbose)
	    printf ("%s rewritten successfully.\n", newname);
	else if (verbose)
	    printf ("%s could not be written.\n", newname);
	free (irafheader);
	}
    else {
	if (fitswimage (newname, header, image) > 0 && verbose)
	    printf ("%s: rewritten successfully.\n", newname);
	else if (verbose)
	    printf ("%s could not be written.\n", newname);
	}

    free (image);
    free (header);
    return;
}

/* Dec  6 1996	New program
 *
 * Jan 15 1997	Print subtracted value rather than result in verbose mode
 * Feb 21 1997  Check pointers against NULL explicitly for Linux
 * Dec 15 1997	Add capability of reading and writing IRAF 2.11 images
 *
 * May 27 1998	Include fitsio.h instead of fitshead.h
 * Jul 24 1998	Make irafheader char instead of int
 * Aug  6 1998	Change fitsio.h to fitsfile.h
 * Aug 14 1998	Preserve extension when creating new file name
 * Oct 14 1998	Use isiraf() to determine file type
 * Nov 30 1998	Add version and help commands for consistency
 *
 * Apr 29 1999	Add BZERO and BSCALE
 * Jun 29 1999	Fix typo in BSCALE setting
 * Oct 22 1999	Drop unused variables after lint
 *
 * Mar 23 2000	Use hgetm() to get the IRAF pixel file name, not hgets()
 *
 * Apr  9 2002	Do not free unallocated header
 *
 * Jun 21 2006	Clean up code
 */
