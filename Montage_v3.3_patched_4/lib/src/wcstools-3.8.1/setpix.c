/* File setpix.c
 * January 10, 2007
 * By Doug Mink, Harvard-Smithsonian Center for Astrophysics
 * Send bug reports to dmink@cfa.harvard.edu

   Copyright (C) 1996-2007
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
#include "libwcs/wcscat.h"

#define PIX_ADD 1
#define PIX_SUB 2
#define PIX_MUL 3
#define PIX_DIV 4
#define PIX_SET 5

static void usage();
static void SetPix();

static int newimage = 0;
static int verbose = 0;		/* verbose flag */
static int eachpix = 0;		/* If 1, print each pixel change */
static int version = 0;		/* If 1, print only program name and version */
static int logrange = 1;	/* Log pixel change in image header */
static char *pform = NULL;	/* Format in which to print pixels */

static char *RevMsg = "SETPIX WCSTools 3.8.1, 14 December 2009, Doug Mink (dmink@cfa.harvard.edu)";

int
main (ac, av)
int ac;
char **av;
{
    char *str;
    char *fn[500];
    char **crange, **rrange, **value, **rtemp;
    char cr[64], rr[64], vr[64];
    char *listfile = NULL;
    char nextline[128];
    FILE *flist;
    int j;
    int i = 0;
    int iv = 0;
    int nrange, nrange0;
    int op;

    fn[0] = NULL;
    nrange = 500;
    crange = (char **) calloc (nrange, sizeof (char *));
    rrange = (char **) calloc (nrange, sizeof (char *));
    value  = (char **) calloc (nrange, sizeof (char *));

    /* Check for help or version command first */
    str = *(av+1);
    if (!str || !strcmp (str, "help") || !strcmp (str, "-help"))
	usage();
    if (!strcmp (str, "version") || !strcmp (str, "-version")) {
	version = 1;
	usage();
	}
    op = PIX_SET;

    /* crack arguments */
    for (av++; --ac > 0; av++) {
	str = *av;

	/* Set up pixel changes from a file */
	if (str[0] == '@') {
	    listfile = str+1;
	    if ((flist = fopen (listfile, "r")) == NULL) {
		fprintf (stderr,"CONPIX: List file %s cannot be read\n",
			 listfile);
		usage ();
		}
	    while (fgets (nextline, 64, flist) != NULL) { 
		sscanf (nextline, "%s %s %s", cr, rr, vr);
		crange[iv] = (char *) malloc (strlen (cr) + 2);
		rrange[iv] = (char *) malloc (strlen (rr) + 2);
		value[iv] = (char *) malloc (strlen (vr) + 2);
		strcpy (crange[iv], cr);
		strcpy (rrange[iv], rr);
		strcpy (value[iv], vr);
		/* if (verbose)
		    printf ("%4d: %s %s %s\n",
			    iv, crange[iv], rrange[iv], value[iv]); */
		iv++;
		if (iv >= nrange) {
		    nrange0 = nrange;
		    nrange = nrange + 500;
		    rtemp = (char **) calloc (nrange, sizeof (char **));
		    for (j = 0; j < nrange0; j++)
			rtemp[j] = rrange[j];
		    free (rrange);
		    rrange = rtemp;
		    rtemp = (char **) calloc (nrange, sizeof (char **));
		    for (j = 0; j < nrange0; j++)
			rtemp[j] = crange[j];
		    free (crange);
		    crange = rtemp;
		    rtemp = (char **) calloc (nrange, sizeof (char **));
		    for (j = 0; j < nrange0; j++)
			rtemp[j] = value[j];
		    free (value);
		    value = rtemp;
		    }
		rrange[iv] = NULL;
		crange[iv] = NULL;
		value[iv] = NULL;
		}
	    }

	/* Set x or y range or new pixel value */
	else if (isnum (str) || isrange (str)) {
	    if (crange[iv] == NULL)
		crange[iv] = str;
	    else if (rrange[iv] == NULL)
		rrange[iv] = str;
	    else if (isnum (str)) {
		value[iv] = str;
		iv++;
		if (iv >= nrange) {
		    nrange0 = nrange;
		    nrange = nrange + 500;
		    rtemp = (char **) calloc (nrange, sizeof (char **));
		    for (j = 0; j < nrange0; i++)
			rtemp[j] = rrange[j];
		    free (rrange);
		    rrange = rtemp;
		    rtemp = (char **) calloc (nrange, sizeof (char **));
		    for (j = 0; j < nrange0; i++)
			rtemp[j] = crange[j];
		    free (crange);
		    crange = rtemp;
		    rtemp = (char **) calloc (nrange, sizeof (char **));
		    for (j = 0; j < nrange0; i++)
			rtemp[j] = value[j];
		    free (value);
		    value = rtemp;
		    }
		rrange[iv] = NULL;
		crange[iv] = NULL;
		value[iv] = NULL;
		}
	    }

	else if (str[0] == '-') {
	    char c;
	    while ((c = *++str)) {
		switch (c) {
		    case 'a':	/* Add constant to image section */
			op = PIX_ADD;
			break;
		    case 'd':	/* Divide image section by constant */
			op = PIX_DIV;
			break;
		    case 'i':	/* print each change */
			eachpix++;
			break;
		    case 'm':	/* Multiply image section by constant */
			op = PIX_MUL;
			break;
		    case 'n':	/* ouput new file */
			newimage++;
			break;
		    case 's':	/* Subtract constant from image section */
			op = PIX_SUB;
			break;
		    case 'v':	/* some verbosity */
			verbose++;
			break;
		    default:
			usage ();
			break;
		    }
		}
	    }

	else {
	    fn[i] = str;
	    i++;
	    fn[i] = NULL;
	    }
	}

    if (i == 0 || iv == 0)
	usage();

    /* If number of ranges being set is more than 10 do not login header */
    if (iv > 10)
	logrange = 0;

    /* Loop through pixel changes for each image */
    i = 0;
    while (fn[i] != NULL) {
	SetPix (fn[i], op, crange, rrange, value);
	i++;
	}

    return (0);
}

static void
usage ()
{
    fprintf (stderr,"%s\n",RevMsg);
    if (version)
	exit (-1);
    fprintf (stderr,"Edit pixels of FITS or IRAF image file\n");
    fprintf(stderr,"Usage: setpix [-vn] file.fts x_range y_range value ...\n");
    fprintf(stderr,"  or : setpix [-vn] file.fts @valuefile ...\n");
    fprintf(stderr,"  -a: Add constant to pixels\n");
    fprintf(stderr,"  -d: Divide pixels by constant\n");
    fprintf(stderr,"  -i: List each line which is dropped \n");
    fprintf(stderr,"  -m: Multiply pixels by constant\n");
    fprintf(stderr,"  -n: write new file, else overwrite \n");
    fprintf(stderr,"  -s: Subtract constant from pixels\n");
    fprintf(stderr,"  -v: verbose\n");
    exit (1);
}


static void
SetPix (filename, op, crange, rrange, value)

char	*filename;	/* FITS or IRAF file filename */
int	op;		/* Operation to perform */
char	**crange;	/* Column range string */
char	**rrange;	/* Row range string */
char	**value;	/* value to insert into pixel */

{
    char *image;		/* FITS image */
    char *header;		/* FITS header */
    int lhead;			/* Maximum number of bytes in FITS header */
    int nbhead;			/* Actual number of bytes in FITS header */
    int iraffile;		/* 1 if IRAF image */
    char *irafheader;		/* IRAF image header */
    int i, lext, lroot;
    int nx, ny, ix, iy, x, y, ipix;
    char *imext, *imext1;
    double bzero;		/* Zero point for pixel scaling */
    double bscale;		/* Scale factor for pixel scaling */
    char newname[256];
    char pixname[256];
    char history[64];
    char *ext, *fname;
    char newline[1];
    char echar;
    double dpix, dpi;
    char *c;
    char opstring[32];
    int bitpix,xdim,ydim;
    struct Range *xrange;    /* X range structure */
    struct Range *yrange;    /* Y range structure */

    newline[0] = 10;

    if (op == PIX_SET)
	strcpy (opstring, "set to");
    else if (op == PIX_MUL)
	strcpy (opstring, "multiplied by");
    else if (op == PIX_DIV)
	strcpy (opstring, "divided by");
    else if (op == PIX_SUB)
	strcpy (opstring, "minus");
    else if (op == PIX_ADD)
	strcpy (opstring, "plus");

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

    /* Change value of specified pixel */
    hgeti4 (header,"BITPIX",&bitpix);
    xdim = 1;
    hgeti4 (header,"NAXIS1",&xdim);
    ydim = 1;
    hgeti4 (header,"NAXIS2",&ydim);
    bzero = 0.0;
    hgetr8 (header,"BZERO",&bzero);
    bscale = 1.0;
    hgetr8 (header,"BSCALE",&bscale);

    i = 0;
    while (value[i] != NULL) {

	/* Extract new value from command line string */
	if (strchr (value[i], (int)'.'))
	    dpix = atof (value[i]);
	else
	    dpix = (double) atoi (value[i]);

	/* Set format if not already set */
	if (pform == NULL) {
	    pform = (char *) calloc (8,1);
	    if (bitpix > 0)
		strcpy (pform, "%d");
	    else
		strcpy (pform, "%.2f");
	    }

	/* Set entire image */
	if (!strcmp (rrange[i], "0") && !strcmp (crange[i], "0")) {
	    nx = xdim;
	    ny = ydim;
	    for (x = 0; x < nx; x++) {
		for (y = 0; y < ny; y++) {
		    if (op == PIX_SET) {
			putpix (image,bitpix,xdim,ydim,bzero,bscale,x,y,dpix);
			dpi = dpix;
			}
		    else {
			dpi = getpix (image,bitpix,xdim,ydim,bzero,bscale,x,y);
			if (op == PIX_MUL)
			    dpi = dpi * dpix;
			else if (op == PIX_ADD)
			    dpi = dpi + dpix;
			else if (op == PIX_SUB)
			    dpi = dpi - dpix;
			else if (op == PIX_DIV)
			    dpi = dpi / dpix;
			putpix (image,bitpix,xdim,ydim,bzero,bscale,x,y,dpi);
			}
	            if (bitpix > 0) {
			if (dpi > 0)
	 		    ipix = (int) (dpi + 0.5);
			else if (dpi < 0)
		 	    ipix = (int) (dpi - 0.5);
			else
			    ipix = 0;
			}
		    if (eachpix) {
			printf ("%s[%d,%d] = ", filename,x+1,y+1);
		        if (bitpix > 0)
			    printf (pform, ipix);
			else
			    printf (pform, dpi);
			printf ("\n");
			}
		    }
		}
	    sprintf (history,"SETPIX: pixels in image %s %s",
		     opstring, value[i]);
	    }

	/* Set entire columns */
	if (!strcmp (rrange[i], "0")) {
	    xrange = RangeInit (crange[i], xdim);
	    nx = rgetn (xrange);
	    ny = ydim;
	    for (ix = 0; ix < nx; ix++) {
		rstart (xrange);
		x = rgeti4 (xrange) - 1;
		for (y = 0; y < ny; y++) {
		    if (op == PIX_SET) {
			putpix (image,bitpix,xdim,ydim,bzero,bscale,x,y,dpix);
			dpi = dpix;
			}
		    else {
			dpi = getpix (image,bitpix,xdim,ydim,bzero,bscale,x,y);
			if (op == PIX_MUL)
			    dpi = dpi * dpix;
			else if (op == PIX_ADD)
			    dpi = dpi + dpix;
			else if (op == PIX_SUB)
			    dpi = dpi - dpix;
			else if (op == PIX_DIV)
			    dpi = dpi / dpix;
			putpix (image,bitpix,xdim,ydim,bzero,bscale,x,y,dpi);
			}
	            if (bitpix > 0) {
			if (dpi > 0)
	 		    ipix = (int) (dpi + 0.5);
			else if (dpix < 0)
		 	    ipix = (int) (dpi - 0.5);
			else
			    ipix = 0;
			}
		    if (eachpix) {
			printf ("%s[%d,%d] = ", filename,x+1,y+1);
		        if (bitpix > 0)
			    printf (pform, ipix);
			else
			    printf (pform, dpi);
			printf ("\n");
			}
		    }
		}
	    if (isnum (crange[i]))
		sprintf (history, "SETPIX: pixels in column %s %s %s",
		     crange[i],opstring,value[i]);
	    else
		sprintf (history, "SETPIX: pixels in columns %s %s %s",
		     crange[i],opstring,value[i]);
	    free (xrange);
	    }

	/* Set entire rows */
	else if (!strcmp (crange[i], "0")) {
	    yrange = RangeInit (rrange[i], xdim);
	    ny = rgetn (yrange);
	    nx = xdim;
	    for (iy = 0; iy < ny; iy++) {
		y = rgeti4 (yrange) - 1;
		for (x = 0; x < nx; x++) {
		    if (op == PIX_SET) {
			putpix (image,bitpix,xdim,ydim,bzero,bscale,x,y,dpix);
			dpi = dpix;
			}
		    else {
			dpi = getpix (image,bitpix,xdim,ydim,bzero,bscale,x,y);
			if (op == PIX_MUL)
			    dpi = dpi * dpix;
			else if (op == PIX_ADD)
			    dpi = dpi + dpix;
			else if (op == PIX_SUB)
			    dpi = dpi - dpix;
			else if (op == PIX_DIV)
			    dpi = dpi / dpix;
			putpix (image,bitpix,xdim,ydim,bzero,bscale,x,y,dpi);
			}
		    if (eachpix) {
			if (bitpix > 0) {
			    if (dpi > 0)
	 			ipix = (int) (dpi + 0.5);
			    else if (dpi < 0)
		 		ipix = (int) (dpi - 0.5);
			    else
				ipix = 0;
			    }
			printf ("%s[%d,%d] = ", filename,x+1,y+1);
			if (bitpix > 0)
			    printf (pform, ipix);
			else
			    printf (pform, dpi);
			printf ("\n");
			}
		    }
		}
	    if (isnum (rrange[i]))
		sprintf (history, "SETPIX: pixels in row %s %s %s",
		     rrange[i],opstring,value[i]);
	    else
		sprintf (history, "SETPIX: pixels in rows %s %s %s",
		     rrange[i],opstring,value[i]);
	    free (yrange);
	    }

	/* Set a region of a two-dimensional image */
	else {
	    xrange = RangeInit (crange[i], xdim);
	    nx = rgetn (xrange);

	    /* Make list of y coordinates */
	    yrange = RangeInit (rrange[i], ydim);
	    ny = rgetn (yrange);

	    /* Loop through rows starting with the last one */
	    for (iy = 0; iy < ny; iy++) {
		y = rgeti4 (yrange);
		rstart (xrange);

		/* Loop through columns */
		for (ix = 0; ix < nx; ix++) {
		    x = rgeti4 (xrange);
		    if (op == PIX_SET) {
			putpix1 (image,bitpix,xdim,ydim,bzero,bscale,x,y,dpix);
			dpi = dpix;
			}
		    else {
			dpi = getpix1 (image,bitpix,xdim,ydim,bzero,bscale,x,y);
			if (op == PIX_MUL)
			    dpi = dpi * dpix;
			else if (op == PIX_ADD)
			    dpi = dpi + dpix;
			else if (op == PIX_SUB)
			    dpi = dpi - dpix;
			else if (op == PIX_DIV)
			    dpi = dpi / dpix;
			putpix1 (image,bitpix,xdim,ydim,bzero,bscale,x,y,dpi);
			}
		    if (eachpix) {
	        	if (bitpix > 0) {
			    if ((c = strchr (pform,'f')) != NULL)
				*c = 'd';
			    if (dpi > 0)
	 			ipix = (int) (dpi + 0.5);
			    else if (dpi < 0)
		 		ipix = (int) (dpi - 0.5);
			    else
				ipix = 0;
			    }
			else {
			    if ((c = strchr (pform,'d')) != NULL)
				*c = 'f';
			    }
			printf ("%s[%d,%d] = ", filename,x+1,y+1);
			if (bitpix > 0)
			    printf (pform, ipix);
			else
			    printf (pform, dpi);
			printf ("\n");
			}
		    }
		}

	    /* Note addition as history line in header */
	    if (isnum (crange[i]) && isnum (rrange[i]))
		sprintf (history, "SETPIX: pixel at row %s, column %s %s %s",
		     rrange[i], crange[i], opstring, value[i]);
	    else if (isnum (rrange[i]))
		sprintf (history, "SETPIX: pixels in row %s, columns %s %s %s",
		     rrange[i], crange[i], opstring, value[i]);
	    else if (isnum (crange[i]))
		sprintf (history, "SETPIX: pixels in column %s, rows %s %s %s",
		     crange[i], rrange[i], opstring, value[i]);
	    else
		sprintf (history, "SETPIX: pixels in rows %s, columns %s %s %s",
		     rrange[i], crange[i], opstring, value[i]);
	    free (xrange);
	    free (yrange);
	    }
	if (logrange) {
	    if (hputc (header,"HISTORY",history)) {
		lhead = gethlength (header);
		lhead = lhead + 14400;
		if ((header = (char *) realloc (header, (unsigned int) lhead)) != NULL) {
		    hlength (header, lhead);
		    hputc (header,"HISTORY",history);
		    }
		}
	    }
	if (verbose)
	    printf ("%s\n", history);
	i++;
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

    free (header);
    free (image);
    return;
}

/* Dec  6 1996	New program
 *
 * Feb 21 1997  Check pointers against NULL explicitly for Linux
 * Dec 15 1997	Add capability of reading and writing IRAF 2.11 images
 *
 * May 28 1998	Include fitsio.h instead of fitshead.h
 * Jul 24 1998	Make irafheader char instead of int
 * Aug  6 1998	Change fitsio.h to fitsfile.h
 * Aug 14 1998	Preserve extension when creating new file name
 * Oct 14 1998	Use isiraf() to determine file type
 * Nov 30 1998	Add version and help commands for consistency
 *
 * Feb 12 1999	Initialize dxisn to 1 so it works for 1-D images
 * Apr 29 1999	Add BZERO and BSCALE
 * Jun 29 1999	Fix typo in BSCALE setting
 * Jul 12 1999	Add ranges
 * Sep 14 1999	Add file of values to usage
 * Oct  5 1999	Bump maximum number of ranges and files from 100 to 500
 * Oct  7 1999	Add -i option to usage()
 * Oct 14 1999	Reallocate header and try again if history writing unsuccessful
 * Oct 22 1999	Drop unused variables after lint
 * Oct 28 1999	Fix bug which always tried to free both ranges
 * Dec 13 1999	Fix bug with region setting; add option to set entire image
 *
 * Mar 23 2000	Use hgetm() to get the IRAF pixel file name, not hgets()
 * Jun 21 2000	Add options to operate on existing image
 *
 * Apr  9 2002	Do not free unallocated header
 * Dec  5 2002	Allocate ranges so number of them can be infinite
 * Dec  5 2002	Drop header HISTORY if more than 10 ranges of pixels set
 *
 * Feb 19 2003	Fix bug which caused pixels to always be set as integers
 *
 * Nov 17 2004	Check for arguments after numbers so negative pixel values work
 */
