/* File sumpix.c
 * January 11, 2007
 * By Doug Mink Harvard-Smithsonian Center for Astrophysics)
 * Send bug reports to dmink@cfa.harvard.edu

   Copyright (C) 1999-2007
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
#include "libwcs/wcs.h"
#include "libwcs/fitsfile.h"
#include "libwcs/wcscat.h"

static void usage();
static void SumPix();
extern double PhotPix();

static char *RevMsg = "SUMPIX WCSTools 3.8.1, 14 December 2009, Doug Mink SAO";

static int verbose = 0;		/* verbose/debugging flag */
static int version = 0;	/* If 1, print only program name and version */
static int complim = 0;	/* If 1, compute limiting values over range */
static int compsum = 0;	/* If 1, compute sum over range */
static int compmean = 0;/* If 1, compute mean over range */
static int compvar = 0;	/* If 1, compute variance over range */
static int compstd = 0;	/* If 1, compute standard deviation over range */
static int compphot = 0; /* If 1, compute photometry in circular aperture */
static char *xstr;	/* X coordinate of center of aperture */
static char *ystr;	/* Y coordinate of center of aperture */
static double rad;	/* Radius of aperture in pixels */
static int printfile = 0;/* If 1, print file name at start of line */
static int ndec = -1;	/* Number of decimal places in outout */

int
main (ac, av)
int ac;
char **av;
{
    char *str;
    char *fn;
    char *rrange;	/* Row range string */
    char *crange;	/* Column range string */

    /* Check for help or version command first */
    str = *(av+1);
    if (!str || !strcmp (str, "help") || !strcmp (str, "-help"))
	usage();
    if (!strcmp (str, "version") || !strcmp (str, "-version")) {
	version = 1;
	usage();
	}

    crange = NULL;
    rrange = NULL;
    fn = NULL;

    /* crack arguments */
    for (av++; --ac > 0; av++) {
	str = *av;
	if (str[0] == '-') {
	    char c;
	    while ((c = *++str))
	    switch (c) {

		case 'c':	/* Compute circular aperture */
		    if (ac < 4)
			usage();
		    xstr = *++av;
		    ac--;
		    ystr = *++av;
		    ac--;
		    rad = atof (*++av);
		    ac--;
		    compstd++;
		    break;

		case 'd':	/* Compute standard deviation */
		    compstd++;
		    break;

		case 'l':	/* Compute limits */
		    complim++;
		    break;

		case 'm':	/* Compute mean */
		    compmean++;
		    break;

		case 'n': /* Number of decimal places in output */
		    if (ac < 2)
			usage();
		    ndec = (int) (atof (*++av));
		    ac--;
		    break;

		case 'p':	/* Print filename at start of line */
		    printfile++;
		    break;
		case 'r':	/* Compute variance */
		    compvar++;
		    break;
		case 's':	/* Compute sum */
		    compsum++;
		    break;
		case 'v':	/* more verbosity */
		    verbose++;
		    break;
		default:
		    usage();
		    break;
		}
	    }

	/* Read range in x or y */
        else if (isnum (*av) || isrange (*av)) {
	    if (crange == NULL)
		crange = *av;
	    else
		rrange = *av;
	    }

	/* read filename */
	else {
	    fn = *av;

	    if (!compmean && !compvar && !compstd)
		compsum++;
	    if (crange && rrange)
		SumPix (fn, crange, rrange);
	    else if (crange)
		SumPix (fn, crange, "0");
	    else if (rrange)
		SumPix (fn, "0", rrange);
	    else
		SumPix (fn, "0", "0");
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
    fprintf (stderr,"Sum row, column, or region of a FITS or IRAF image\n");
    fprintf(stderr,"Usage: sumpix [-dmrsv][-n num] x_range  y_range file.fit ...\n");
    fprintf(stderr,"  -c x y r: compute total counts in circle r from (x,y)\n");
    fprintf(stderr,"  -d: compute and print standard deviation\n");
    fprintf(stderr,"  -l: compute and print min and max values\n");
    fprintf(stderr,"  -m: compute and print mean\n");
    fprintf(stderr,"  -n: number of decimal places in output\n");
    fprintf(stderr,"  -p: Print file name at start of line\n");
    fprintf(stderr,"  -r: compute and print variance (sum of squares)\n");
    fprintf(stderr,"  -s: compute and print sum (default)\n");
    fprintf(stderr,"  -v: verbose\n");
    fprintf(stderr,"  a range of 0 implies the full dimension\n");
    fprintf(stderr,"  an absence of ranges uses the entire image\n");
    exit (1);
}


static void
SumPix (name, crange, rrange)

char *name;	/* FITS or IRAF .imh file name */
char *crange;	/* Column range string */
char *rrange;	/* Row range string */

{
    char *header;	/* FITS image header */
    int lhead;		/* Maximum number of bytes in FITS header */
    int nbhead;		/* Actual number of bytes in FITS header */
    char *irafheader;	/* IRAF image header */
    char *image;	/* FITS or IRAF image */
    double bzero;	/* Zero point for pixel scaling */
    double bscale;	/* Scale factor for pixel scaling */
    double counts;
    double wt;
    int iraffile;
    double dpix, sum;
    double ra, dec, xp, yp;
    int bitpix,xdim,ydim;
    int nx, ny, ix, iy, x, y;
    int offscl;
    int np;
    double dnp, mean, variance, std, sumsq, dmin, dmax;
    char pixname[256];
    char numform[8];
    char numforme[8];
    struct Range *xrange = NULL;    /* X range structure */
    struct Range *yrange = NULL;    /* Y range structure */
    struct WorldCoor *wcs, *GetWCSFITS();

    /* Open IRAF image if .imh extension is present */
    if (isiraf (name)) {
	iraffile = 1;
	if ((irafheader = irafrhead (name, &lhead)) != NULL) {
	    header = iraf2fits (name, irafheader, lhead, &nbhead);
	    free (irafheader);
	    if (header == NULL) {
		fprintf (stderr, "Cannot translate IRAF header %s/n",name);
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
	    fprintf (stderr, "Cannot read IRAF file %s\n", name);
	    return;
	    }
	}

    /* Open FITS file if .imh extension is not present */
    else {
	iraffile = 0;
	if ((header = fitsrhead (name, &lhead, &nbhead)) != NULL) {
	    if ((image = fitsrimage (name, nbhead, header)) == NULL) {
		fprintf (stderr, "Cannot read FITS image %s\n", name);
		free (header);
		return;
		}
	    }
	else {
	    fprintf (stderr, "Cannot read FITS file %s\n", name);
	    return;
	    }
	}
    if (verbose) {
	fprintf (stderr,"%s\n",RevMsg);
	if (!strcmp (crange, "0"))
	    fprintf (stderr,"Sum rows %s in ", rrange);
	else if (!strcmp (rrange, "0"))
	    fprintf (stderr,"Sum columns %s in ", crange);
	else
	    fprintf (stderr,"Sum rows %s, columns %s in ", rrange, crange);
	if (iraffile)
	    fprintf (stderr,"IRAF image file %s\n", name);
	else
	    fprintf (stderr,"FITS image file %s\n", name);
	}

    if (ndec > -1) {
	sprintf (numform, "%%.%df ", ndec);
	sprintf (numforme, "%%.%df", ndec);
	}
    else {
	sprintf (numform, "%%.2f ");
	sprintf (numforme, "%%.2f");
	}

/* Get information about the image */
    hgeti4 (header,"BITPIX",&bitpix);
    xdim = 1;
    hgeti4 (header,"NAXIS1",&xdim);
    ydim = 1;
    hgeti4 (header,"NAXIS2",&ydim);
    bzero = 0.0;
    hgetr8 (header,"BZERO",&bzero);
    bscale = 1.0;
    hgetr8 (header,"BSCALE",&bscale);

    if (printfile)
	printf ("%s ", name);

    /* Compute counts in circular area */
    if (compphot) {
	if (strchr (xstr, ':') != NULL || strchr (ystr, ':') != NULL) {
	    wcs = GetWCSFITS (name, header, verbose);
	    ra = str2ra (xstr);
	    dec = str2dec (ystr);
	    wcs2pix (wcs, ra, dec, &xp, &yp, &offscl);
	    if (verbose)
		printf ("%s %s (%.3f,%.3f) %.1f = ", xstr, ystr, xp, yp, rad);
	    }
	else {
	    xp = atof (xstr);
	    yp = atof (ystr);
	    if (verbose)
		printf ("(%.3f,%.3f) %.1f = ", xp, yp, rad);
	    }
	counts = PhotPix (image, header, xp, yp, rad, &wt);
	printf ("%f", counts);
	if (verbose)
	    printf (" (%f pixels)", wt);
	printf ("\n");
	}

    /* Sum entire image */
    else if (!strcmp (crange, "0") && !strcmp (rrange, "0")) {
	nx = xdim;
	ny = ydim;
	sum = 0.0;
	sumsq = 0.0;
	np = 0;
	dmin = 1000000000000000.0;
	dmax = -10000000000000000.0;
	for (x = 0; x < nx; x++) {
	    for (y = 0; y < ny; y++) {
        	dpix = getpix (image,bitpix,xdim,ydim,bzero,bscale,x,y);
		sum = sum + dpix;
		sumsq = sumsq + (dpix * dpix);
		if (dpix < dmin)
		    dmin = dpix;
		if (dpix > dmax)
		    dmax = dpix;
		np++;
		}
	    }
	dnp = (double) np;
	if (compsum)
	    printf (numform, sum);
	mean = sum / dnp;
	if (compmean)
	    printf (numform, mean);
	variance = sumsq;
	if (compvar)
	    printf (numform, variance);
	if (compstd) {
	    std = sqrt ((sumsq / dnp) - (mean * mean));
	    printf (numforme, std);
	    }
	if (complim) {
	    printf (" ");
	    printf (numform, dmin);
	    printf ("- ");
	    printf (numform, dmax);
	    }
	printf ("\n");
	}

    /* Sum entire columns */
    else if (!strcmp (rrange, "0")) {
	xrange = RangeInit (crange, xdim);
	nx = rgetn (xrange);
	ny = ydim;
	for (ix = 0; ix < nx; ix++) {
	    x = rgeti4 (xrange) - 1;
	    sum = 0.0;
	    sumsq = 0.0;
	    dmin = 1000000000000000.0;
	    dmax = -10000000000000000.0;
	    np = 0;
	    for (y = 0; y < ny; y++) {
        	dpix = getpix (image,bitpix,xdim,ydim,bzero,bscale,x,y);
		sum = sum + dpix;
		sumsq = sumsq + (dpix * dpix);
		if (dpix < dmin)
		    dmin = dpix;
		if (dpix > dmax)
		    dmax = dpix;
		np++;
		}
	    dnp = (double) np;
	    if (compsum)
		printf (numform, sum);
	    mean = sum / dnp;
	    if (compmean)
		printf (numform, mean);
	    variance = sumsq;
	    if (compvar)
		printf (numform, variance);
	    if (compstd) {
		std = sqrt ((sumsq / dnp) - (mean * mean));
		printf (numforme, std);
		}
	    if (complim) {
		printf (" ");
		printf (numform, dmin);
		printf ("- ");
		printf (numform, dmax);
		}
	    printf ("\n");
	    }
	free (xrange);
	}

    /* Sum entire rows */
    else if (!strcmp (crange, "0")) {
	yrange = RangeInit (rrange, xdim);
	ny = rgetn (yrange);
	nx = xdim;
	for (iy = 0; iy < ny; iy++) {
	    y = rgeti4 (yrange) - 1;
	    sum = 0.0;
	    sumsq = 0.0;
	    dmin = 1000000000000000.0;
	    dmax = -10000000000000000.0;
	    np = 0;
	    for (x = 0; x < nx; x++) {
        	dpix = getpix (image,bitpix,xdim,ydim,bzero,bscale,x,y);
		sum = sum + dpix;
		sumsq = sumsq + (dpix * dpix);
		if (dpix < dmin)
		    dmin = dpix;
		if (dpix > dmax)
		    dmax = dpix;
		np++;
		}
	    dnp = (double) np;
	    if (compsum)
		printf (numform, sum);
	    mean = sum / dnp;
	    if (compmean)
		printf (numform, mean);
	    variance = sumsq;
	    if (compvar)
		printf (numform, variance);
	    if (compstd) {
		std = sqrt ((sumsq / dnp) - (mean * mean));
		printf (numforme, std);
		}
	    if (complim) {
		printf (" ");
		printf (numform, dmin);
		printf ("- ");
		printf (numform, dmax);
		}
	    printf ("\n");
	    }
	free (yrange);
	}

    /* Sum a region of a two-dimensional image */
    else {
	yrange = RangeInit (rrange, ydim);
	ny = rgetn (yrange);
	sum = 0.0;
	sumsq = 0.0;
	np = 0;
	dmin = 1000000000000000.0;
	dmax = -10000000000000000.0;
	for (iy = 0; iy < ny; iy++) {
	    y = rgeti4 (yrange) - 1;
	    xrange = RangeInit (crange, xdim);
	    nx = rgetn (xrange);
	    for (ix = 0; ix < nx; ix++) {
		x = rgeti4 (xrange) - 1;
        	dpix = getpix (image,bitpix,xdim,ydim,bzero,bscale,x,y);
		sum = sum + dpix;
		sumsq = sumsq + (dpix * dpix);
		if (dpix < dmin)
		    dmin = dpix;
		if (dpix > dmax)
		    dmax = dpix;
		np++;
		if (verbose)
		    printf ("%s[%d,%d] = %f\n",name,x,y,dpix);
		}
	    }
	dnp = (double) np;
	if (compsum)
	    printf ("%f ", sum);
	mean = sum / dnp;
	if (compmean)
	    printf ("%f ", mean);
	variance = sumsq;
	if (compvar)
	    printf ("%f ", variance);
	if (compstd) {
	    std = sqrt ((sumsq / dnp) - (mean * mean));
	    printf ("%f", std);
	    }
	if (complim) {
	    printf (" ");
	    printf (numform, dmin);
	    printf ("- ");
	    printf (numform, dmax);
	    }
	printf ("\n");
	if (xrange != NULL)
	    free (xrange);
	free (yrange);
	}

    free (header);
    free (image);
    return;
}
/* Jul  2 1999	New program
 * Jul  6 1999	Fix bug with x computation in patch adding section
 * Oct 22 1999	Drop unused variables after lint
 * Oct 29 1999	Add option to compute and print mean, rms, std, and/or sum
 * Dec 10 1999	Add option -n to set number of decimal places in output
 * Dec 14 1999	Change rms to variance
 *
 * Mar 23 2000	Use hgetm() to get the IRAF pixel file name, not hgets()
 *
 * Dec  6 2002	Add option to sum over entire image if both ranges are 0
 * Dec  6 2002	Add -l option to print range of values
 *
 * Jun 21 2006	Clean up code
 *
 * Jan 10 2007	Declare RevMsg static, not const
 * Jan 10 2007	Include wcs.h
 * Jan 11 2007	Add PhotPix to compute circular aperture photometry
 */
