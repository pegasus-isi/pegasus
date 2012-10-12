/* File fixpix.c
 * October 9, 2007
 * By Doug Mink, Harvard-Smithsonian Center for Astrophysics
 * Send bug reports to dmink@cfa.harvard.edu

   Copyright (C) 1997-2007
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
#include "libwcs/fitsfile.h"
#include "libwcs/wcs.h"

#define MAXFIX 10
#define MAXFILES 50

static void FixPix();
static void FixReg();
static void usage();
static int newimage = 0;
static int verbose = 0;		/* verbose flag */
static int nfix = 0;		/* Number of regions to fix
				   If < 0 read regions from a file */
static int version = 0;		/* If 1, print only program name and version */
static int xl[MAXFIX],yl[MAXFIX]; /* Lower left corners of regions (1 based) */
static int xr[MAXFIX],yr[MAXFIX]; /* Upper right corners of regions (1 based) */
static char *RevMsg = "FIXPIX WCSTools 3.8.1, 14 December 2009, Doug Mink (dmink@cfa.harvard.edu)";

int
main (ac, av)
int ac;
char **av;
{
    char *str;
    char *fn[MAXFILES];
    int readlist = 0;
    int nfile;
    char *lastchar;
    char filename[128];
    int ifile;
    FILE *flist;
    char *listfile;
    char *regionlist;

    regionlist = NULL;
    listfile = NULL;

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

	case '@':	/* List of files to be read */
	    readlist++;
	    listfile = ++str;
	    str = str + strlen (str) - 1;
	    av++;
	    ac--;

	default:
	    usage ();
	    break;
	}
    }

    /* If there are no remaining arguments, print usage */
    if (ac == 0)
	usage ();

    /* Crack arguments */
    nfix = 0;
    nfile = 0;
    while (ac-- > 0  && nfile < MAXFILES && nfix < MAXFIX) {
	if (isiraf (*av) || isfits (*av)) {
	    fn[nfile] = *av;
	    nfile++;
	    av++;
	    }
	else if (*av[0] == '@') {
	    nfix = -1;
	    regionlist = *av + 1;
	    av++;
	    }
	else if (ac > 2) {
	    xl[nfix] = atoi (*av++);
	    ac--;
	    yl[nfix] = atoi (*av++);
	    ac--;
	    xr[nfix] = atoi (*av++);
	    ac--;
	    yr[nfix] = atoi (*av++);
	    nfix++;
	    }
	}

    /* Process only if a list of regions to fix has been found */
    if (nfix != 0) {

	/* Read through headers of images in listfile */
	if (readlist) {
	    if ((flist = fopen (listfile, "r")) == NULL) {
		fprintf (stderr,"FIXPIX: List file %s cannot be read\n",
		     listfile);
		usage ();
		}
	    while (fgets (filename, 128, flist) != NULL) {
		lastchar = filename + strlen (filename) - 1;
		if (*lastchar < 32) *lastchar = 0;
		FixPix (filename, regionlist);
		if (verbose)
		    printf ("\n");
		}
	    fclose (flist);
	    }

	/* Read image headers from command line list */
	else {
	    for (ifile = 0; ifile < nfile; ifile++)
		FixPix (fn[ifile], regionlist);
	    }
	}

    return (0);
}

static void
usage ()
{
    fprintf (stderr,"%s\n",RevMsg);
    if (version)
	exit (-1);
    fprintf (stderr,"Fix pixel regions of FITS or IRAF image file\n");
    fprintf(stderr,"Usage: fixpix [-vn] file.fits xl yl xr yr...\n");
    fprintf(stderr,"  or : fixpix [-vn] file.fits @regionlist\n");
    fprintf(stderr,"  or : fixpix [-vn] @filelist xl yl xr yr...\n");
    fprintf(stderr,"  or : fixpix [-vn] @filelist @regionlist\n");
    fprintf(stderr,"  -n: write new file, else overwrite \n");
    fprintf(stderr,"  -v: verbose\n");
    exit (1);
}


static void
FixPix (filename, regionlist)

char	*filename;	/* FITS or IRAF file filename */
char	*regionlist;	/* Name of file of regions to fix, if nfix < 0 */

{
    char *image;		/* FITS image */
    char *header;		/* FITS header */
    int lhead;			/* Maximum number of bytes in FITS header */
    int nbhead;			/* Actual number of bytes in FITS header */
    int iraffile;		/* 1 if IRAF image */
    char *irafheader = NULL;	/* IRAF image header */
    int i, lext, lroot;
    double bzero;		/* Zero point for pixel scaling */
    double bscale;		/* Scale factor for pixel scaling */
    char *imext, *imext1;
    char newname[256];
    char pixname[256];
    char tempname[256];
    char line[128];
    char history[64];
    char echar;
    FILE *freg;
    char *ext, *fname;
    char newline[1];
    int bitpix,xdim,ydim;

    newline[0] = 10;
    strcpy (tempname, "fitshead.temp");

    /* Open IRAF image and header if .imh extension is present */
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

    /* Read FITS image and header if .imh extension is not present */
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

    /* Fix values of specified area */
    hgeti4 (header,"BITPIX",&bitpix);
    hgeti4 (header,"NAXIS1",&xdim);
    hgeti4 (header,"NAXIS2",&ydim);
    bzero = 0.0;
    hgetr8 (header,"BZERO",&bzero);
    bscale = 1.0;
    hgetr8 (header,"BSCALE",&bscale);

    /* Fix pixels over regions from a command line coordinate list */
    if (nfix > 0) {
	for (i = 0; i < nfix; i++) {
	    FixReg (image,bitpix,xdim,ydim,bzero,bscale,xl[i],yl[i],xr[i],yr[i]);

	    /* Note addition as history line in header */
	    sprintf (history, "FIXPIX: region x: %d-%d, y: %d-%d replaced",
		     xl[i],xr[i],yl[i],yr[i]);
	    hputc (header,"HISTORY",history);
	    if (verbose)
		printf ("%s\n", history);
	    }
	}

    /* Fix pixels over regions from a file */
    else {
	if ((freg = fopen (regionlist, "r")) == NULL) {
		fprintf (stderr,"FIXPIX: Region file %s cannot be read\n",
		     regionlist);
		usage ();
		}
	while (fgets (line, 128, freg) != NULL) {
	    i = 0;
	    sscanf (line,"%d %d %d %d", &xl[i], &yl[i], &xr[i], &yr[i]);
	    FixReg (image,bitpix,xdim,ydim,bzero,bscale,xl[i],yl[i],xr[i],yr[i]);

	    /* Note addition as history line in header */
	    sprintf (history, "FIXPIX: region x: %d-%d, y: %d-%d replaced",
			 xl[i],xr[i],yl[i],yr[i]);
	    hputc (header,"HISTORY",history);
	    if (verbose)
		printf ("%s\n", history);
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
    else
	strcpy (newname, filename);

    /* Write fixed image to output file */
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

    free (header);
    free (image);
    return;
}

static void
FixReg (image, bitpix, xdim, ydim, bzero, bscale, ixl, iyl, ixr, iyr)

char	*image;		/* FITS image */
int	bitpix;		/* Number of bits in each pixel */
int	xdim;		/* Number of pixels in image horizontally */
int	ydim;		/* Number of pixels in image vertically */
double  bzero;		/* Zero point for pixel scaling */
double  bscale;		/* Scale factor for pixel scaling */
int	ixl, iyl;	/* Lower left corner of region (1 based) */
int	ixr, iyr;	/* Upper right corner of region (1 based) */

{
    int xdiff, ydiff, it, ix, iy;
    double pixl, pixr, dpix;

    /* Find dimensions of region to fix */
    if (ixl > ixr) {
	it = ixr;
	ixr = ixl;
	ixl = it;
	}
    xdiff = ixr - ixl + 1;
    if (iyl > iyr) {
	it = iyr;
	iyr = iyl;
	iyl = it;
	}
    ydiff = iyr - iyl + 1;

    /* Return if region contains no points */
    if (xdiff < 1 || ydiff < 1)
	return;

    /* If more horizontal than vertical, interpolate vertically */
    if (xdiff > ydiff) {
	if (iyl - 1 < 0 || iyr + 1 > ydim - 1)
	    return;
	for (ix = ixl; ix <= ixr; ix++) {
	    pixl = getpix1 (image,bitpix,xdim,ydim,bzero,bscale,ix,iyl-1);
	    pixr = getpix1 (image,bitpix,xdim,ydim,bzero,bscale,ix,iyr+1);
	    dpix = (pixr - pixl) / (double)ydiff;
	    for (iy = iyl; iy <= iyr; iy++) {
		pixl = pixl + dpix;
		putpix1 (image,bitpix,xdim,ydim,bzero,bscale,ix,iy,pixl);
		}
	    }
	}

    /* If more vertical than horizontal, interpolate horizontally */
    else {
	if (ixl - 1 < 0 || ixr + 1 > xdim - 1)
	    return;
	for (iy = iyl; iy <= iyr; iy++) {
	    pixl = getpix1 (image,bitpix,xdim,ydim,bzero,bscale,ixl-1,iy);
	    pixr = getpix1 (image,bitpix,xdim,ydim,bzero,bscale,ixr+1,iy);
	    dpix = (pixr - pixl) / (double)xdiff;
	    for (ix = ixl; ix <= ixr; ix++) {
		pixl = pixl + dpix;
		putpix1 (image,bitpix,xdim,ydim,bzero,bscale,ix,iy,pixl);
		}
	    }
	}

    return;
}

/* Jul 12 1997	New program
 * Dec 15 1997	Add capability of reading and writing IRAF 2.11 images
 *
 * Apr 14 1998	Change xn, yn variable names due to a header conflict
 * May 27 1998	Include fitsio.h instead of fitshead.h
 * Jul 24 1998	Make irafheader char instead of int
 * Aug  6 1998	Change fitsio.h to fitsfile.h
 * Aug 14 1998	Preserve extension when creating new file name
 * Oct 14 1998	Use isiraf() to determine file type
 * Nov 30 1998	Add version and help commands for consistency
 *
 * Apr 29 1999	Add BZERO and BSCALE
 * Jun 29 1999	Fix typo in BSCALE setting
 * Sep 27 1999	Use new 1-based-coordinate image access subroutines
 * Oct 15 1999	Fix input from list file
 * Oct 22 1999	Drop unused variables after lint
 *
 * Mar 23 2000	Use hgetm() to get the IRAF pixel file name, not hgets()
 *
 * Apr  9 2002	Do not free unallocated header
 *
 * Jun 20 2006	Clean up code
 *
 * Oct  9 2007	Fix bug reading coordinates from file found by Saurabh Jha
 */
