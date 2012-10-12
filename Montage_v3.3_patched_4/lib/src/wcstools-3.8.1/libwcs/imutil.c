/*** File libwcs/imutil.c
 *** January 11, 2007
 *** By Doug Mink, dmink@cfa.harvard.edu
 *** Harvard-Smithsonian Center for Astrophysics
 *** Copyright (C) 2006-2007
 *** Smithsonian Astrophysical Observatory, Cambridge, MA, USA
 */

/* Smooth, fill, or shrink an image */

/* char *FiltFITS(header, image, filter, xsize, ysize, nlog)
 *	Return filtered image buffer
 * char *FillFITS(header, image, filter, xsize, ysize, nlog)
 *	Return image bufer with bad pixels replaced by filter value
 * SetBadFITS (header, image, badheader, badimage, nlog)
 *	Set bad pixels in image to BLANK using bad pixel file
 * SetBadVal (header, image, minpixval, maxpixval, nlog)
 *	Set bad pixels in image to BLANK if value less than minpixval
 *
 * char *medfilt (buff, header, ndx, ndy, nlog)
 *	Median filter an image
 * char *medfill (buff, header, ndx, ndy, nlog)
 *	Set blank pixels to the median of a box around each one
 * medpixi2 (x, ival, ix, iy, nx, ny, ndx, ndy)
 * medpixi4 (x, ival, ix, iy, nx, ny, ndx, ndy)
 * medpixr4 (x, ival, ix, iy, nx, ny, ndx, ndy)
 * medpixr8 (x, ival, ix, iy, nx, ny, ndx, ndy)
 *	Compute median of rectangular group of pixels
 *
 * char *meanfilt (buff, header, ndx, ndy, nlog)
 *	Mean filter an image
 * char *meanfill (buff, header, ndx, ndy, nlog)
 *	Set blank pixels to the mean of a box around each one
 * meanpixi2 (x, ival, ix, iy, nx, ny, ndx, ndy)
 * meanpixi4 (x, ival, ix, iy, nx, ny, ndx, ndy)
 * meanpixr4 (x, ival, ix, iy, nx, ny, ndx, ndy)
 * meanpixr8 (x, ival, ix, iy, nx, ny, ndx, ndy)
 *	Compute mean of rectangular group of pixels
 *
 * char *gaussfilt (buff, header, ndx, ndy, nlog)
 *	Gaussian filter an image
 * char *gaussfill (buff, header, ndx, ndy, nlog)
 *	Set blank pixels to the Gaussian weighted sum of a box around each one
 * gausswt (mx, my, nx)
 *	Compute Gaussian weights for a square region
 * gausspixi2 (image, ival, ix, iy, nx, ny)
 * gausspixi4 (image, ival, ix, iy, nx, ny)
 * gausspixr4 (image, ival, ix, iy, nx, ny)
 * gausspixr8 (image, ival, ix, iy, nx, ny)
 *	Compute Gaussian-weighted mean of a square group of pixels
 *
 * char *ShrinkFITSImage (header, image, xfactor, yfactor, mean, bitpix, nlog)
 *	Return image buffer reduced by a given factor
 * char *ShrinkFITSHeader (filename, header, xfactor, yfactor, mean, bitpix)
 		Return image header with dimensions reduced by a given factor
 * double PhotPix (imbuff,header,cx,cy,rad,sumw)
 *	Compute counts within a strictly defined circular aperture
 */

#include <string.h>             /* NULL, strlen, strstr, strcpy */
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include "fitsfile.h"

#define MEDIAN 1
#define MEAN 2
#define GAUSSIAN 3

char *medfilt();
char *meanfilt();
char *gaussfilt();
char *badfill();
char *medfill();
char *meanfill();
char *gaussfill();
void gausswt();

short medpixi2();
short meanpixi2();
short gausspixi2();
int medpixi4();
int meanpixi4();
int gausspixi4();
float medpixr4();
float meanpixr4();
float gausspixr4();
double medpixr8();
double meanpixr8();
double gausspixr8();

static int bpvalset = 0;
static double bpval = -9999.0;
static short bpvali2;
static int bpvali4;
static float bpvalr4;
static int nfilled;

int
getnfilled ()
{ return (nfilled); }

void
setbadpix (bpval0)
double bpval0;
{ bpvalset = 1; bpval = bpval0; return; }


/* Set all pixels to a value computed from a box around each one */

char *
FiltFITS (header, image, filter, xsize, ysize, nlog)

char	*header;	/* Image header */
char	*image;		/* Image bytes to be filtered */
int	filter;		/* Smoothing filter (median,mean,gaussian) */
int	xsize;		/* Number of pixels in x (odd, horizontal) */
int	ysize;		/* Number of pixels in y (odd, vertical) */
int	nlog;		/* Logging interval in lines */

{
    if (filter == MEDIAN)
	return (medfilt (image, header, xsize, ysize, nlog));
    else if (filter == GAUSSIAN)
	return (gaussfilt (image, header, xsize, ysize, nlog));
    else
	return (meanfilt (image, header, xsize, ysize, nlog));
}

/* Set BLANK pixels to a value computed from a box around each one */

char *
FillFITS (header, image, filter, xsize, ysize, nlog)

char	*header;	/* Image header */
char	*image;		/* Image bytes to be filtered */
int	filter;		/* Smoothing filter (median,mean,gaussian) */
int	xsize;		/* Number of pixels in x (odd, horizontal) */
int	ysize;		/* Number of pixels in y (odd, vertical) */
int	nlog;		/* Logging interval in lines */

{
    if (filter == MEDIAN)
	return (medfill (image, header, xsize, ysize, nlog));
    else if (filter == GAUSSIAN)
	return (gaussfill (image, header, xsize, ysize, nlog));
    else
	return (meanfill (image, header, xsize, ysize, nlog));
}


/*	Set bad pixels in image to BLANK using bad pixel file */

char *
SetBadFITS (header, image, badheader, badimage, nlog)

char	*header;	/* FITS image header */
char	*image;		/* Image buffer */
char	*badheader;	/* FITS bad pixel file image header */
			/* Bad pixels have non-zero values */
char	*badimage;	/* Bad pixel file image buffer */
int	nlog;		/* Logging interval in pixels */

{
char	*buffret;	/* Modified image buffer (returned) */
int	nx,ny;		/* Number of columns and rows in image */
int	ix,iy;		/* Pixel around which to compute mean */
int	npix;		/* Number of pixels in image */
int	bitpix;		/* Number of bits per pixel (<0=floating point) */
int	bitpixb;	/* Number of bits per pixel in bad pixel file */
int	nxb,nyb;	/* Number of columns and rows in bad pixel image */
int	naxes;
char	*buff;
char	*buffbad;

    hgeti4 (header, "BITPIX", &bitpix);
    hgeti4 (header, "NAXIS", &naxes);
    hgeti4 (header, "NAXIS1", &nx);
    if (naxes > 1)
	hgeti4 (header, "NAXIS2", &ny);
    else
	ny = 1;
    npix = nx * ny;
    if (!bpvalset)
	hgetr8 (header, "BLANK", &bpval);

    hgeti4 (badheader, "BITPIX", &bitpixb);
    hgeti4 (badheader, "NAXIS", &naxes);
    hgeti4 (badheader, "NAXIS1", &nxb);
    if (naxes > 1)
	hgeti4 (badheader, "NAXIS2", &nyb);
    else
	nyb = 1;
    if (nx != nxb || ny != nyb) {
	fprintf (stderr, "SetBadFITS: Data file and bad pixel file mismatch %dx%d not %dx%d\n",
		 nx,ny,nxb,nyb);
	return (NULL);
	}

    nfilled = 0;

    buff = image;
    buffbad = badimage;
    buffret = NULL;
    if (bitpix == 16) {
	short *b1, *b2, *buffout;
	bpvali2 = (short) bpval;
	buffret = (char *) calloc (npix, sizeof (short));
	buffout = (short *) buffret;
	b1 = (short *) buff;
	b2 = buffout;
	if (bitpixb == 16) {
	    short *bb;
	    bb = (short *) buffbad;
	    for (iy = 0; iy < ny; iy++) {
		for (ix = 0; ix < nx; ix++) {
		    if (*bb++) {
			*b2++ = bpvali2;
			nfilled++;
			*b1++;
			}
		    else {
			*b2++ = *b1++;
			}
		    }
		if (nlog > 0 && (iy+1)%nlog == 0)
		    fprintf (stderr,"SetBadFITS: %d lines, %d bad pixels set\r",
			     iy+1, nfilled);
		}
	    }
	else if (bitpixb == 32) {
	    int *bb;
	    bb = (int *) buffbad;
	    for (iy = 0; iy < ny; iy++) {
		for (ix = 0; ix < nx; ix++) {
		    if (*bb++) {
			*b2++ = bpvali2;
			nfilled++;
			*b1++;
			}
		    else {
			*b2++ = *b1++;
			}
		    }
		if (nlog > 0 && (iy+1)%nlog == 0)
		    fprintf (stderr,"SetBadFITS: %d lines, %d bad pixels set\r",
			     iy+1, nfilled);
		}
	    }
	}
    else if (bitpix == 32) {
	int *b1, *b2, *buffout;
	bpvali4 = (int) bpval;
	buffret = (char *) calloc (npix, sizeof (int));
	buffout = (int *) buffret;
	b1 = (int *) buff;
	b2 = buffout;
	if (bitpixb == 16) {
	    short *bb;
	    bb = (short *) buffbad;
	    for (iy = 0; iy < ny; iy++) {
		for (ix = 0; ix < nx; ix++) {
		    if (*bb++) {
			*b2++ = bpvali4;
			nfilled++;
			*b1++;
			}
		    else {
			*b2++ = *b1++;
			}
		    }
		if (nlog > 0 && (iy+1)%nlog == 0)
		    fprintf (stderr,"SetBadFITS: %d lines, %d bad pixels set\r",
			     iy+1, nfilled);
		}
	    }
	else if (bitpixb == 32) {
	    int *bb;
	    bb = (int *) buffbad;
	    for (iy = 0; iy < ny; iy++) {
		for (ix = 0; ix < nx; ix++) {
		    if (*bb++) {
			*b2++ = bpvali4;
			nfilled++;
			*b1++;
			}
		    else {
			*b2++ = *b1++;
			}
		    }
		if (nlog > 0 && (iy+1)%nlog == 0)
		    fprintf (stderr,"SetBadFITS: %d lines, %d bad pixels set\r",
			     iy+1, nfilled);
		}
	    }
	fprintf (stderr,"\n");
	}
    else if (bitpix == -32) {
	float *b1, *b2, *buffout;
	buffret = (char *) calloc (npix, sizeof (float));
	buffout = (float *) buffret;
	bpvalr4 = (float) bpval;
	b1 = (float *) buff;
	b2 = buffout;
	if (bitpixb == 16) {
	    short *bb;
	    bb = (short *) buffbad;
	    for (iy = 0; iy < ny; iy++) {
		for (ix = 0; ix < nx; ix++) {
		    if (*bb++) {
			*b2++ = bpvalr4;
			nfilled++;
			*b1++;
			}
		    else {
			*b2++ = *b1++;
			}
		    }
		if (nlog > 0 && (iy+1)%nlog == 0)
		    fprintf (stderr,"SetBadFITS: %d lines, %d bad pixels set\r",
			     iy+1, nfilled);
		}
	    }
	else if (bitpixb == 32) {
	    int *bb;
	    bb = (int *) buffbad;
	    for (iy = 0; iy < ny; iy++) {
		for (ix = 0; ix < nx; ix++) {
		    if (*bb++) {
			*b2++ = bpvalr4;
			nfilled++;
			*b1++;
			}
		    else {
			*b2++ = *b1++;
			}
		    }
		if (nlog > 0 && (iy+1)%nlog == 0)
		    fprintf (stderr,"SetBadFITS: %d lines, %d bad pixels set\r",
			     iy+1, nfilled);
		}
	    }
	}
    else if (bitpix == -64) {
	double *b1, *b2, *buffout;
	buffret = (char *) calloc (npix, sizeof (double));
	buffout = (double *) buffret;
	b1 = (double *) buff;
	b2 = buffout;
	if (bitpixb == 16) {
	    short *bb;
	    bb = (short *) buffbad;
	    for (iy = 0; iy < ny; iy++) {
		for (ix = 0; ix < nx; ix++) {
		    if (*bb++) {
			*b2++ = bpval;
			nfilled++;
			*b1++;
			}
		    else {
			*b2++ = *b1++;
			}
		    }
		if (nlog > 0 && (iy+1)%nlog == 0)
		    fprintf (stderr,"SetBadFITS: %d lines, %d bad pixels set\r",
			     iy+1, nfilled);
		}
	    }
	else if (bitpixb == 32) {
	    int *bb;
	    bb = (int *) buffbad;
	    for (iy = 0; iy < ny; iy++) {
		for (ix = 0; ix < nx; ix++) {
		    if (*bb++) {
			*b2++ = bpval;
			nfilled++;
			*b1++;
			}
		    else {
			*b2++ = *b1++;
			}
		    }
		if (nlog > 0 && (iy+1)%nlog == 0)
		    fprintf (stderr,"SetBadFITS: %d lines, %d bad pixels set\r",
			     iy+1, nfilled);
		}
	    }
	}
    if (nlog > 0)
	fprintf (stderr,"SetBadFITS: %d lines, %d bad pixels set\n",
		 iy, nfilled);
    return (buffret);
}


/* Set bad pixels in image to BLANK if less than minpixval */

char *
SetBadVal (header, image, minval, maxval, nlog)

char	*header;	/* FITS image header */
char	*image;		/* Image buffer */
double	minval;		/* Minimum good pixel value */
double	maxval;		/* Maximum good pixel value */
int	nlog;		/* Logging interval in pixels */

{
char	*buffret;	/* Modified image buffer (returned) */
int	nx,ny;		/* Number of columns and rows in image */
int	ix,iy;		/* Pixel around which to compute mean */
int	npix;		/* Number of pixels in image */
int	bitpix;		/* Number of bits per pixel (<0=floating point) */
int	checkmin = 0;
int	checkmax = 0;
int	naxes;
char	*buff;

    hgeti4 (header, "BITPIX", &bitpix);
    hgeti4 (header, "NAXIS", &naxes);
    hgeti4 (header, "NAXIS1", &nx);
    if (naxes > 1)
	hgeti4 (header, "NAXIS2", &ny);
    else
	ny = 1;
    npix = nx * ny;
    if (!bpvalset)
	hgetr8 (header, "BLANK", &bpval);

    if (minval != -9999.0)
	checkmin++;
    if (maxval != -9999.0)
	checkmax++;

    nfilled = 0;

    buff = image;
    buffret = NULL;
    if (bitpix == 16) {
	short *bb, *b1, *b2, *buffout, bpvali2, mnvali2,mxvali2;
	bpvali2 = (short) bpval;
	mnvali2 = (short) minval;
	mxvali2 = (short) maxval;
	buffret = (char *) calloc (npix, sizeof (short));
	buffout = (short *) buffret;
	bb = (short *) buff;
	b1 = (short *) buff;
	b2 = buffout;
	for (iy = 0; iy < ny; iy++) {
	    for (ix = 0; ix < nx; ix++) {
		if (checkmin && *bb < mnvali2) {
		    *b2++ = bpvali2;
		    nfilled++;
		    *b1++;
		    }
		else if (checkmax && *bb > mxvali2) {
		    *b2++ = bpvali2;
		    nfilled++;
		    *b1++;
		    }
		else {
		    *b2++ = *b1++;
		    }
		bb++;
		}
	    if (nlog > 0 && (iy+1)%nlog == 0)
		fprintf (stderr,"SetBadVal: %d lines, %d bad pixels set\r",
			 iy+1, nfilled);
	    }
	}
    else if (bitpix == 32) {
	int *bb, *b1, *b2, *buffout, mnvali4, mxvali4, bpvali4;
	bpvali4 = (int) bpval;
	mnvali4 = (int) minval;
	mxvali4 = (int) maxval;
	buffret = (char *) calloc (npix, sizeof (int));
	buffout = (int *) buffret;
	bb = (int *) buff;
	b1 = (int *) buff;
	b2 = buffout;
	for (iy = 0; iy < ny; iy++) {
	    for (ix = 0; ix < nx; ix++) {
		if (checkmin && *bb < mnvali4) {
		    *b2++ = bpvali4;
		    nfilled++;
		    *b1++;
		    }
		else if (checkmax && *bb > mxvali4) {
		    *b2++ = bpvali4;
		    nfilled++;
		    *b1++;
		    }
		else {
		    *b2++ = *b1++;
		    }
		bb++;
		}
	    if (nlog > 0 && (iy+1)%nlog == 0)
		fprintf (stderr,"SetBadVal: %d lines, %d bad pixels set\r",
			 iy+1, nfilled);
	    }
	}
    else if (bitpix == -32) {
	float *bb, *b1, *b2, *buffout, bpvalr4, mnvalr4, mxvalr4;
	buffret = (char *) calloc (npix, sizeof (float));
	buffout = (float *) buffret;
	bpvalr4 = (float) bpval;
	mnvalr4 = (float) minval;
	mxvalr4 = (float) maxval;
	bb = (float *) buff;
	b1 = (float *) buff;
	b2 = buffout;
	for (iy = 0; iy < ny; iy++) {
	    for (ix = 0; ix < nx; ix++) {
		if (checkmin && *bb < mnvalr4) {
		    *b2++ = bpvalr4;
		    nfilled++;
		    *b1++;
		    }
		else if (checkmax && *bb > mxvalr4) {
		    *b2++ = bpvalr4;
		    nfilled++;
		    *b1++;
		    }
		else {
		    *b2++ = *b1++;
		    }
		bb++;
		}
	    if (nlog > 0 && (iy+1)%nlog == 0)
		fprintf (stderr,"SetBadVal: %d lines, %d bad pixels set\r",
			 iy+1, nfilled);
	    }
	}
    else if (bitpix == -64) {
	double *bb, *b1, *b2, *buffout, bpvalr8, mnvalr8, mxvalr8;
	buffret = (char *) calloc (npix, sizeof (double));
	buffout = (double *) buffret;
	bpvalr8 = (double) bpval;
	mnvalr8 = (double) minval;
	mxvalr8 = (double) maxval;
	bb = (double *) buff;
	b1 = (double *) buff;
	b2 = buffout;
	for (iy = 0; iy < ny; iy++) {
	    for (ix = 0; ix < nx; ix++) {
		if (checkmin && *bb < mnvalr8) {
		    *b2++ = bpvalr8;
		    nfilled++;
		    *b1++;
		    }
		else if (checkmax && *bb > mxvalr8) {
		    *b2++ = bpvalr8;
		    nfilled++;
		    *b1++;
		    }
		else {
		    *b2++ = *b1++;
		    }
		bb++;
		}
	    if (nlog > 0 && (iy+1)%nlog == 0)
		fprintf (stderr,"SetBadVal: %d lines, %d bad pixels set\r",
			 iy+1, nfilled);
	    }
	}
    if (nlog > 0)
	fprintf (stderr,"SetBadVal: %d lines, %d bad pixels set\n",
		 iy, nfilled);
    return (buffret);
}


/* Median filter an image */

static short *vi2;	/* Working vector to sort for median */
static int *vi4;	/* Working vector to sort for median */
static float *vr4;	/* Working vector to sort for median */
static double *vr8;	/* Working vector to sort for median */

char *
medfilt (buff, header, ndx, ndy, nlog)

char	*buff;	/* Image buffer */
char	*header; /* FITS image header */
int	ndx;	/* Number of columns over which to compute the median */
int	ndy;	/* Number of rows over which to compute the median */
int	nlog;	/* Logging interval in pixels */

{
char	*buffret;	/* Modified image buffer (returned) */
int	nx,ny;	/* Number of columns and rows in image */
int	ix,iy;	/* Pixel around which to compute median */
int	npix;	/* Number of pixels in image */
int	bitpix;	/* Number of bits per pixel (<0=floating point) */
int	naxes;

    hgeti4 (header, "BITPIX", &bitpix);
    hgeti4 (header, "NAXIS", &naxes);
    hgeti4 (header, "NAXIS1", &nx);
    if (naxes > 1)
	hgeti4 (header, "NAXIS2", &ny);
    else
	ny = 1;
    npix = nx * ny;
    hgetr8 (header, "BLANK", &bpval);

    buffret = NULL;
    if (bitpix == 16) {
	short *b1,*b2, *buffout;
	bpvali2 = (short) bpval;
	vi2 = NULL;
	buffret = (char *) calloc (npix, sizeof (short));
	buffout = (short *) buffret;
	b1 = (short *) buff;
	b2 = buffout;
	for (iy = 0; iy < ny; iy++) {
	    for (ix = 0; ix < nx; ix++) {
		*b2++ = medpixi2 (buff, *b1++, ix, iy, nx, ny, ndx, ndy);
		}
	    if (nlog > 0 && (iy+1)%nlog == 0)
		fprintf (stderr,"MEDFILT: %d lines filtered\r", iy+1);
	    }
	fprintf (stderr,"\n");
	free (vi2);
	vi2 = NULL;
	}
    else if (bitpix == 32) {
	int *b1, *b2, *buffout;
	bpvali4 = (int) bpval;
	vi4 = NULL;
	buffret = (char *) calloc (npix, sizeof (int));
	buffout = (int *) buffret;
	b1 = (int *) buff;
	b2 = buffout;
	for (iy = 0; iy < ny; iy++) {
	    for (ix = 0; ix < nx; ix++) {
		*b2++ = medpixi4 (buff, *b1++, ix, iy, nx, ny, ndx, ndy);
		}
	    if (nlog > 0 && (iy+1)%nlog == 0)
		fprintf (stderr,"MEDFILT: %d lines filtered\r", iy+1);
	    }
	fprintf (stderr,"\n");
	free (vi4);
	vi4 = NULL;
	}
    else if (bitpix == -32) {
	float *b1, *b2, *buffout;
	buffret = (char *) calloc (npix, sizeof (float));
	buffout = (float *) buffret;
	b1 = (float *) buff;
	b2 = buffout;
	bpvalr4 = (float) bpval;
	for (iy = 0; iy < ny; iy++) {
	    for (ix = 0; ix < nx; ix++) {
		*b2++ = medpixr4 (buff, *b1++, ix, iy, nx, ny, ndx, ndy);
		}
	    if (nlog > 0 && (iy+1)%nlog == 0)
		fprintf (stderr,"MEDFILT: %d lines filtered\r", iy+1);
	    }
	fprintf (stderr,"\n");
	free (vr4);
	vr4 = NULL;
	}
    else if (bitpix == -64) {
	double *b1, *b2, *buffout;
	buffret = (char *) calloc (npix, sizeof (double));
	buffout = (double *) buffret;
	b1 = (double *) buff;
	b2 = buffout;
	for (iy = 0; iy < ny; iy++) {
	    for (ix = 0; ix < nx; ix++) {
		*b2++ = medpixr8 (buff, *b1++, ix, iy, nx, ny, ndx, ndy);
		}
	    if (nlog > 0 && (iy+1)%nlog == 0)
		fprintf (stderr,"MEDFILT: %d lines filtered\r", iy+1);
	    }
	fprintf (stderr,"\n");
	free (vr8);
	vr8 = NULL;
	}
    if (nlog > 0)
	fprintf (stderr,"MEDFILT: %d lines filtered\n", iy);
    return (buffret);
}


/* Set blank pixels to the median of a box around each one */

char *
medfill (buff, header, ndx, ndy, nlog)

char	*buff;	/* Image buffer */
char	*header; /* FITS image header */
int	ndx;	/* Number of columns over which to compute the median */
int	ndy;	/* Number of rows over which to compute the median */
int	nlog;	/* Logging interval in pixels */

{
char	*buffret;	/* Modified image buffer (returned) */
int	nx,ny;	/* Number of columns and rows in image */
int	ix,iy;	/* Pixel around which to compute median */
int	npix;	/* Number of pixels in image */
int	bitpix;	/* Number of bits per pixel (<0=floating point) */
int	naxes;

    hgeti4 (header, "BITPIX", &bitpix);
    hgeti4 (header, "NAXIS", &naxes);
    hgeti4 (header, "NAXIS1", &nx);
    if (naxes > 1)
	hgeti4 (header, "NAXIS2", &ny);
    else
	ny = 1;
    npix = nx * ny;
    hgetr8 (header, "BLANK", &bpval);

    nfilled = 0;

    buffret = NULL;
    if (bitpix == 16) {
	short *b1,*b2, *buffout;
	bpvali2 = (short) bpval;
	vi2 = NULL;
	buffret = (char *) calloc (npix, sizeof (short));
	buffout = (short *) buffret;
	b1 = (short *) buff;
	b2 = buffout;
	for (iy = 0; iy < ny; iy++) {
	    for (ix = 0; ix < nx; ix++) {
		if (*b1 != bpvali2)
		    *b2++ = *b1++;
		else {
		    *b2++ = medpixi2 (buff, *b1++, ix, iy, nx, ny, ndx, ndy);
		    nfilled++;
		    }
		}
	    if (nlog > 0 && (iy+1)%nlog == 0)
		fprintf (stderr,"MEDFILL: %d lines, %d pixels filled\r", iy+1, nfilled);
	    }
	free (vi2);
	vi2 = NULL;
	}
    else if (bitpix == 32) {
	int *b1, *b2, *buffout;
	bpvali4 = (int) bpval;
	vi4 = NULL;
	buffret = (char *) calloc (npix, sizeof (int));
	buffout = (int *) buffret;
	b1 = (int *) buff;
	b2 = buffout;
	for (iy = 0; iy < ny; iy++) {
	    for (ix = 0; ix < nx; ix++) {
		if (*b1 != bpvali4)
		    *b2++ = *b1++;
		else {
		    *b2++ = medpixi4 (buff, *b1++, ix, iy, nx, ny, ndx, ndy);
		    nfilled++;
		    }
		}
	    if (nlog > 0 && (iy+1)%nlog == 0)
		fprintf (stderr,"MEDFILL: %d lines, %d pixels filled\r", iy+1, nfilled);
	    }
	free (vi4);
	vi4 = NULL;
	}
    else if (bitpix == -32) {
	float *b1, *b2, *buffout;
	buffret = (char *) calloc (npix, sizeof (float));
	buffout = (float *) buffret;
	bpvalr4 = (float) bpval;
	b1 = (float *) buff;
	b2 = buffout;
	for (iy = 0; iy < ny; iy++) {
	    for (ix = 0; ix < nx; ix++) {
		if (*b1 != bpvalr4)
		    *b2++ = *b1++;
		else {
		    *b2++ = medpixr4 (buff, *b1++, ix, iy, nx, ny, ndx, ndy);
		    nfilled++;
		    }
		}
	    if (nlog > 0 && (iy+1)%nlog == 0)
		fprintf (stderr,"MEDFILL: %d lines, %d pixels filled\r", iy+1, nfilled);
	    }
	free (vr4);
	vr4 = NULL;
	}
    else if (bitpix == -64) {
	double *b1, *b2, *buffout;
	buffret = (char *) calloc (npix, sizeof (double));
	buffout = (double *) buffret;
	b1 = (double *) buff;
	b2 = buffout;
	for (iy = 0; iy < ny; iy++) {
	    for (ix = 0; ix < nx; ix++) {
		if (*b1 != bpval)
		    *b2++ = *b1++;
		else {
		    *b2++ = medpixr8 (buff, *b1++, ix, iy, nx, ny, ndx, ndy);
		    nfilled++;
		    }
		}
	    if (nlog > 0 && (iy+1)%nlog == 0)
		fprintf (stderr,"MEDFILL: %d lines, %d pixels filled\r", iy+1, nfilled);
	    }
	free (vr8);
	vr8 = NULL;
	}
    if (nlog > 0)
	fprintf (stderr,"MEDFILL: %d lines, %d pixels filled\n", iy, nfilled);
    return (buffret);
}


/* Compute median of rectangular group of pixels */

short
medpixi2 (x, ival, ix, iy, nx, ny, ndx, ndy)

short	*x;	/* Image buffer */
short	ival;	/* Value at this pixel */
int	ix,iy;	/* Pixel around which to compute median */
int	nx,ny;	/* Number of columns and rows in image */
int	ndx;	/* Number of columns over which to compute the median */
int	ndy;	/* Number of rows over which to compute the median */

{
    short xx, *vecj, *img;
    int n, i, j;
    int  nx2, ny2, npix;
    int jx, jx1, jx2, jy, jy1, jy2;

    /* Allocate working buffer if it hasn't already been allocated */
    npix = ndx * ndy;
    if (vi2 == NULL) {
	vi2 = (short *) calloc (npix, sizeof (short));
	if (vi2 == NULL) {
	    fprintf (stderr, "MEDPIXI2: Could not allocate %d-pixel buffer\n",npix);
	    return (0);
	    }
	}

    n = ndx * ndy;
    if (n <= 0)
	return (0.0);
    else if (n == 1)
	return (*(x + (iy * ny) + ix));

    /* Compute limits for this pixel */
    nx2 = ndx / 2;
    jx1 = ix - nx2;
    if (jx1 < 0)
	jx1 = 0;
    jx2 = ix + nx2 + 1;
    if (jx2 > nx)
	jx2 = nx;
    ny2 = ndy / 2;
    jy1 = iy - ny2;
    if (jy1 < 0)
	jy1 = 0;
    jy2 = iy + ny2 + 1;
    if (jy2 > ny)
	jy2 = ny;

    /* Initialize actual number of pixels used for this pixel */
    n = 0;

    /* Set up working vector for this pixel */
    vecj = vi2;
    for (jy = jy1; jy < jy2; jy++) {
	img = x + (jy * nx) + jx1;
	for (jx = jx1; jx < jx2; jx++) {
	    if (*img != bpvali2) {
		*vecj++ = *img;
		n++;
		}
	    img++;
	    }
	}

    /* If no good pixels, return old value */
    if (n < 1)
	return (ival);

    /* Sort numbers in working vector */
    else {
	for (j = 2; j <= n; j++) {
	    xx = vi2[j];
	    i = j - 1;
	    while (i > 0 && vi2[i] > xx) {
		vi2[i+1] = vi2[i];
		i--;
		}
	    vi2[i+1] = xx;
	    }

	/* Middle number is the median */
	return (vi2[n/2]);
	}
}


/* Compute median of rectangular group of pixels */

int
medpixi4 (x, ival, ix, iy, nx, ny, ndx, ndy)

int	*x;	/* Image buffer */
int	ival;	/* Current pixel */
int	ix,iy;	/* Pixel around which to compute median */
int	nx,ny;	/* Number of columns and rows in image */
int	ndx;	/* Number of columns over which to compute the median */
int	ndy;	/* Number of rows over which to compute the median */

{
    int xx, *vecj, *img;
    int n, i, j;
    int  nx2, ny2, npix;
    int jx, jx1, jx2, jy, jy1, jy2;

    /* Allocate working buffer if it hasn't already been allocated */
    npix = ndx * ndy;
    if (vi4 == NULL) {
	vi4 = (int *) calloc (npix, sizeof (int));
	if (vi4 == NULL) {
	    fprintf (stderr, "MEDIANI4: Could not allocate %d-pixel buffer\n",npix);
	    return (0);
	    }
	}

    n = ndx * ndy;
    if (n <= 0)
	return (0.0);
    else if (n == 1)
	return (*(x + (iy * ny) + ix));

    /* Compute limits for this pixel */
    nx2 = ndx / 2;
    jx1 = ix - nx2;
    if (jx1 < 0)
	jx1 = 0;
    jx2 = ix + nx2 + 1;
    if (jx2 > nx)
	jx2 = nx;
    ny2 = ndy / 2;
    jy1 = iy - ny2;
    if (jy1 < 0)
	jy1 = 0;
    jy2 = iy + ny2 + 1;
    if (jy2 > ny)
	jy2 = ny;

    /* Intitialize actual number of pixels used for this pixel */
    n = 0;

    /* Set up working vector for this pixel */
    vecj = vi4;
    for (jy = jy1; jy < jy2; jy++) {
	img = x + (jy * nx) + jx1;
	for (jx = jx1; jx < jx2; jx++) {
	    if (*img != bpvali4) {
		*vecj++ = *img;
		n++;
		}
	    img++;
	    }
	}

    /* If no good pixels, return old value */
    if (n < 1)
	return (ival);

    /* Sort numbers in working vector */
    else {
	for (j = 2; j <= n; j++) {
	    xx = vi4[j];
	    i = j - 1;
	    while (i > 0 && vi4[i] > xx) {
		vi4[i+1] = vi4[i];
		i--;
		}
	    vi4[i+1] = xx;
	    }

	/* Middle number is the median */
	return (vi4[n/2]);
	}
}


float
medpixr4 (x, rval, ix, iy, nx, ny, ndx, ndy)

float	*x;	/* Image buffer */
float	rval;	/* Image value at this pixel */
int	ix,iy;	/* Pixel around which to compute median */
int	nx,ny;	/* Number of columns and rows in image */
int	ndx;	/* Number of columns over which to compute the median */
int	ndy;	/* Number of rows over which to compute the median */

{
    float xx, *vecj, *img;
    int n, i, j;
    int  nx2, ny2, npix;
    int jx, jx1, jx2, jy, jy1, jy2;

    /* Allocate working buffer if it hasn't already been allocated */
    npix = ndx * ndy;
    if (vr4 == NULL) {
	vr4 = (float *) calloc (npix, sizeof (float));
	if (vr4 == NULL) {
	    fprintf (stderr, "MEDIANR4: Could not allocate %d-pixel buffer\n",npix);
	    return (0);
	    }
	}

    n = ndx * ndy;
    if (n <= 0)
	return (0.0);
    else if (n == 1)
	return (*(x + (iy * ny) + ix));

    /* Compute limits for this pixel */
    nx2 = ndx / 2;
    jx1 = ix - nx2;
    if (jx1 < 0)
	jx1 = 0;
    jx2 = ix + nx2 + 1;
    if (jx2 > nx)
	jx2 = nx;
    ny2 = ndy / 2;
    jy1 = iy - ny2;
    if (jy1 < 0)
	jy1 = 0;
    jy2 = iy + ny2 + 1;
    if (jy2 > ny)
	jy2 = ny;

    /* Initialize actual number of pixels used for this pixel */
    n = 0;

    /* Set up working vector for this pixel */
    vecj = vr4;
    for (jy = jy1; jy < jy2; jy++) {
	img = x + (jy * nx) + jx1;
	for (jx = jx1; jx < jx2; jx++) {
	    if (*img != bpvalr4) {
		*vecj++ = *img;
		n++;
		}
	    img++;
	    }
	}

    /* If no good pixels, return old value */
    if (n < 1)
	return (rval);

    /* Sort numbers in working vector */
    else {
	for (j = 2; j <= n; j++) {
	    xx = vr4[j];
	    i = j - 1;
	    while (i > 0 && vr4[i] > xx) {
		vr4[i+1] = vr4[i];
		i--;
		}
	    vr4[i+1] = xx;
	    }

	/* Middle number is the median */
	return (vr4[n/2]);
	}
}


double
medpixr8 (x, dval, ix, iy, nx, ny, ndx, ndy)

double	*x;	/* Image buffer */
double	dval;	/* Image value of this pixel */
int	ix,iy;	/* Pixel around which to compute median */
int	nx,ny;	/* Number of columns and rows in image */
int	ndx;	/* Number of columns over which to compute the median */
int	ndy;	/* Number of rows over which to compute the median */

{
    double xx, *vecj, *img;
    int n, i, j;
    int  nx2, ny2, npix;
    int jx, jx1, jx2, jy, jy1, jy2;

    /* Allocate working buffer if it hasn't already been allocated */
    npix = ndx * ndy;
    if (vr8 == NULL) {
	vr8 = (double *) calloc (npix, sizeof (double));
	if (vr8 == NULL) {
	    fprintf (stderr, "MEDIANR8: Could not allocate %d-pixel buffer\n",npix);
	    return (0);
	    }
	}

    n = ndx * ndy;
    if (n <= 0)
	return (0.0);
    else if (n == 1)
	return (*(x + (iy * ny) + ix));

    /* Compute limits for this pixel */
    nx2 = ndx / 2;
    jx1 = ix - nx2;
    if (jx1 < 0)
	jx1 = 0;
    jx2 = ix + nx2 + 1;
    if (jx2 > nx)
	jx2 = nx;
    ny2 = ndy / 2;
    jy1 = iy - ny2;
    if (jy1 < 0)
	jy1 = 0;
    jy2 = iy + ny2 + 1;
    if (jy2 > ny)
	jy2 = ny;

    /* Initialize actual number of pixels used for this pixel */
    n = 0;

    /* Set up working vector for this pixel */
    vecj = vr8;
    for (jy = jy1; jy < jy2; jy++) {
	img = x + (jy * nx) + jx1;
	for (jx = jx1; jx < jx2; jx++) {
	    if (*img != bpval) {
		*vecj++ = *img;
		n++;
		}
	    img++;
	    }
	}

    /* If no good pixels, return old value */
    if (n < 1)
	return (dval);

    /* Sort numbers in working vector */
    else {
	for (j = 2; j <= n; j++) {
	    xx = vr8[j];
	    i = j - 1;
	    while (i > 0 && vr8[i] > xx) {
		vr8[i+1] = vr8[i];
		i--;
		}
	    vr8[i+1] = xx;
	    }

	/* Middle number is the median */
	return (vr8[n/2]);
	}
}


/* Mean filter an image */

char *
meanfilt (buff, header, ndx, ndy, nlog)

char	*buff;	/* Image buffer */
char	*header; /* FITS image header */
int	ndx;	/* Number of columns over which to compute the mean */
int	ndy;	/* Number of rows over which to compute the mean */
int	nlog;	/* Logging interval in pixels */

{
char	*buffret;	/* Modified image buffer (returned) */
int	nx,ny;	/* Number of columns and rows in image */
int	ix,iy;	/* Pixel around which to compute mean */
int	npix;	/* Number of pixels in image */
int	bitpix;	/* Number of bits per pixel (<0=floating point) */
int	naxes;

    hgeti4 (header, "BITPIX", &bitpix);
    hgeti4 (header, "NAXIS", &naxes);
    hgeti4 (header, "NAXIS1", &nx);
    if (naxes > 1)
	hgeti4 (header, "NAXIS2", &ny);
    else
	ny = 1;
    npix = nx * ny;
    hgetr8 (header, "BLANK", &bpval);

    buffret = NULL;
    if (bitpix == 16) {
	short *b1, *b2, *buffout;
	bpvali2 = (short) bpval;
	vi2 = NULL;
	buffret = (char *) calloc (npix, sizeof (short));
	buffout = (short *) buffret;
	b1 = (short *) buff;
	b2 = buffout;
	for (iy = 0; iy < ny; iy++) {
	    for (ix = 0; ix < nx; ix++) {
		*b2++ = meanpixi2 (buff, *b1++, ix, iy, nx, ny, ndx, ndy);
		}
	    if (nlog > 0 && (iy+1)%nlog == 0)
		fprintf (stderr,"MEANFILT: %d lines filtered\r", iy+1);
	    }
	free (vi2);
	vi2 = NULL;
	}
    else if (bitpix == 32) {
	int *b1, *b2, *buffout;
	bpvali4 = (int) bpval;
	vi4 = NULL;
	buffret = (char *) calloc (npix, sizeof (int));
	buffout = (int *) buffret;
	b1 = (int *) buff;
	b2 = buffout;
	for (iy = 0; iy < ny; iy++) {
	    for (ix = 0; ix < nx; ix++) {
		*b2++ = meanpixi4 (buff, *b1++, ix, iy, nx, ny, ndx, ndy);
		}
	    if (nlog > 0 && (iy+1)%nlog == 0)
		fprintf (stderr,"MEANFILT: %d lines filtered\r", iy+1);
	    }
	free (vi4);
	vi4 = NULL;
	}
    else if (bitpix == -32) {
	float *b1, *b2, *buffout;
	buffret = (char *) calloc (npix, sizeof (float));
	buffout = (float *) buffret;
	bpvalr4 = (float) bpval;
	b1 = (float *) buff;
	b2 = buffout;
	for (iy = 0; iy < ny; iy++) {
	    for (ix = 0; ix < nx; ix++) {
		*b2++ = meanpixr4 (buff, *b1++, ix, iy, nx, ny, ndx, ndy);
		}
	    if (nlog > 0 && (iy+1)%nlog == 0)
		fprintf (stderr,"MEANFILT: %d lines filtered\r", iy+1);
	    }
	free (vr4);
	vr4 = NULL;
	}
    else if (bitpix == -64) {
	double *b1, *b2, *buffout;
	buffret = (char *) calloc (npix, sizeof (double));
	buffout = (double *) buffret;
	b1 = (double *) buff;
	b2 = buffout;
	for (iy = 0; iy < ny; iy++) {
	    for (ix = 0; ix < nx; ix++) {
		*b2++ = meanpixr8 (buff, *b1++, ix, iy, nx, ny, ndx, ndy);
		}
	    if (nlog > 0 && (iy+1)%nlog == 0)
		fprintf (stderr,"MEANFILT: %d lines filtered\r", iy+1);
	    }
	free (vr8);
	vr8 = NULL;
	}
    if (nlog > 0)
	fprintf (stderr,"MEANFILT: %d lines filtered\n", iy);
    return (buffret);
}


/* Set blank pixels to the mean of a box around each one */

char *
meanfill (buff, header, ndx, ndy, nlog)

char	*buff;	/* Image buffer */
char	*header; /* FITS image header */
int	ndx;	/* Number of columns over which to compute the mean */
int	ndy;	/* Number of rows over which to compute the mean */
int	nlog;	/* Logging interval in pixels */

{
char	*buffret;	/* Modified image buffer (returned) */
int	nx,ny;	/* Number of columns and rows in image */
int	ix,iy;	/* Pixel around which to compute mean */
int	npix;	/* Number of pixels in image */
int	bitpix;	/* Number of bits per pixel (<0=floating point) */
int	naxes;

    hgeti4 (header, "BITPIX", &bitpix);
    hgeti4 (header, "NAXIS", &naxes);
    hgeti4 (header, "NAXIS1", &nx);
    if (naxes > 1)
	hgeti4 (header, "NAXIS2", &ny);
    else
	ny = 1;
    npix = nx * ny;
    hgetr8 (header, "BLANK", &bpval);

    nfilled = 0;

    buffret = NULL;
    if (bitpix == 16) {
	short *b1, *b2, *buffout;
	bpvali2 = (short) bpval;
	vi2 = NULL;
	buffret = (char *) calloc (npix, sizeof (short));
	buffout = (short *) buffret;
	b1 = (short *) buff;
	b2 = buffout;
	for (iy = 0; iy < ny; iy++) {
	    for (ix = 0; ix < nx; ix++) {
		if (*b1 != bpvali2)
		    *b2++ = *b1++;
		else {
		    *b2++ = meanpixi2 (buff, *b1++, ix, iy, nx, ny, ndx, ndy);
		    nfilled++;
		    }
		}
	    if (nlog > 0 && (iy+1)%nlog == 0)
		fprintf (stderr,"MEANFILL: %d lines, %d pixels filled\r", iy+1, nfilled);
	    }
	free (vi2);
	vi2 = NULL;
	}
    else if (bitpix == 32) {
	int *b1, *b2, *buffout;
	bpvali4 = (int) bpval;
	vi4 = NULL;
	buffret = (char *) calloc (npix, sizeof (int));
	buffout = (int *) buffret;
	b1 = (int *) buff;
	b2 = buffout;
	for (iy = 0; iy < ny; iy++) {
	    for (ix = 0; ix < nx; ix++) {
		if (*b1 != bpvali4)
		    *b2++ = *b1++;
		else {
		    *b2++ = meanpixi4 (buff, *b1++, ix, iy, nx, ny, ndx, ndy);
		    nfilled++;
		    }
		}
	    if (nlog > 0 && (iy+1)%nlog == 0)
		fprintf (stderr,"MEANFILL: %d lines, %d pixels filled\r", iy+1, nfilled);
	    }
	free (vi4);
	vi4 = NULL;
	}
    else if (bitpix == -32) {
	float *b1, *b2, *buffout;
	buffret = (char *) calloc (npix, sizeof (float));
	buffout = (float *) buffret;
	bpvalr4 = (float) bpval;
	b1 = (float *) buff;
	b2 = buffout;
	for (iy = 0; iy < ny; iy++) {
	    for (ix = 0; ix < nx; ix++) {
		if (*b1 != bpvalr4)
		    *b2++ = *b1++;
		else {
		    *b2++ = meanpixr4 (buff, *b1++, ix, iy, nx, ny, ndx, ndy);
		    nfilled++;
		    }
		}
	    if (nlog > 0 && (iy+1)%nlog == 0)
		fprintf (stderr,"MEANFILL: %d lines, %d pixels filled\r", iy+1, nfilled);
	    }
	free (vr4);
	vr4 = NULL;
	}
    else if (bitpix == -64) {
	double *b1, *b2, *buffout;
	buffret = (char *) calloc (npix, sizeof (double));
	buffout = (double *) buffret;
	b1 = (double *) buff;
	b2 = buffout;
	for (iy = 0; iy < ny; iy++) {
	    for (ix = 0; ix < nx; ix++) {
		if (*b1 != bpval)
		    *b2++ = *b1++;
		else {
		    *b2++ = meanpixr8 (buff, *b1++, ix, iy, nx, ny, ndx, ndy);
		    nfilled++;
		    }
		}
	    if (nlog > 0 && (iy+1)%nlog == 0)
		fprintf (stderr,"MEANFILL: %d lines, %d pixels filled\r", iy+1, nfilled);
	    }
	free (vr8);
	vr8 = NULL;
	}
    if (nlog > 0)
	fprintf (stderr,"MEANFILL: %d lines, %d pixels filled\n", iy, nfilled);
    return (buffret);
}


/* Compute mean of rectangular group of pixels */

short
meanpixi2 (x, ival, ix, iy, nx, ny, ndx, ndy)

short	*x;	/* Image buffer */
short	ival;	/* Image pixel value */
int	ix,iy;	/* Pixel around which to compute median */
int	nx,ny;	/* Number of columns and rows in image */
int	ndx;	/* Number of columns over which to compute the median */
int	ndy;	/* Number of rows over which to compute the median */

{
    double sum;
    short *img;
    int n, nx2, ny2;
    int jx, jx1, jx2, jy, jy1, jy2;

    n = ndx * ndy;
    if (n <= 0)
	return (0.0);
    else if (n == 1)
	return (*(x + (iy * ny) + ix));

    /* Compute limits for this pixel */
    nx2 = ndx / 2;
    jx1 = ix - nx2;
    if (jx1 < 0)
	jx1 = 0;
    jx2 = ix + nx2 + 1;
    if (jx2 > nx)
	jx2 = nx;
    ny2 = ndy / 2;
    jy1 = iy - ny2;
    if (jy1 < 0)
	jy1 = 0;
    jy2 = iy + ny2 + 1;
    if (jy2 > ny)
	jy2 = ny;

    /* Initialize actual number of pixels used for this pixel */
    n = 0;

    /* Compute total counts around this pixel */
    sum = 0.0;
    for (jy = jy1; jy < jy2; jy++) {
	img = x + (jy * nx) + jx1;
	for (jx = jx1; jx < jx2; jx++) {
	    if (*img != bpvali2) {
		sum = sum + (double) *img;
		n++;
		}
	    img++;
	    }
	}

    if (n < 1)
	return (ival);
    else
	return ((short) (sum / (double) n));
}


int
meanpixi4 (x, ival, ix, iy, nx, ny, ndx, ndy)

int	*x;	/* Image buffer */
int	ival;	/* Image pixel value */
int	ix,iy;	/* Pixel around which to compute median */
int	nx,ny;	/* Number of columns and rows in image */
int	ndx;	/* Number of columns over which to compute the median */
int	ndy;	/* Number of rows over which to compute the median */

{
    double sum;
    int *img;
    int n, nx2, ny2;
    int jx, jx1, jx2, jy, jy1, jy2;

    n = ndx * ndy;
    if (n <= 0)
	return (0.0);
    else if (n == 1)
	return (*(x + (iy * ny) + ix));

    /* Compute limits for this pixel */
    nx2 = ndx / 2;
    jx1 = ix - nx2;
    if (jx1 < 0)
	jx1 = 0;
    jx2 = ix + nx2 + 1;
    if (jx2 > nx)
	jx2 = nx;
    ny2 = ndy / 2;
    jy1 = iy - ny2;
    if (jy1 < 0)
	jy1 = 0;
    jy2 = iy + ny2 + 1;
    if (jy2 > ny)
	jy2 = ny;

    /* Initialize actual number of pixels used for this pixel */
    n = 0;

    /* Set up working vector for this pixel */
    sum = 0.0;
    for (jy = jy1; jy < jy2; jy++) {
	img = x + (jy * nx) + jx1;
	for (jx = jx1; jx < jx2; jx++) {
	    if (*img != bpvali4) {
		sum = sum + (double) *img;
		n++;
		}
	    img++;
	    }
	}

    if (n < 1)
	return (ival);
    else
	return ((int) (sum / (double) n));
}


float
meanpixr4 (x, rval, ix, iy, nx, ny, ndx, ndy)

float	*x;	/* Image buffer */
float	rval;	/* Image pixel value */
int	ix,iy;	/* Pixel around which to compute median */
int	nx,ny;	/* Number of columns and rows in image */
int	ndx;	/* Number of columns over which to compute the median */
int	ndy;	/* Number of rows over which to compute the median */

{
    double sum;
    float *img;
    int n, nx2, ny2;
    int jx, jx1, jx2, jy, jy1, jy2;

    n = ndx * ndy;
    if (n <= 0)
	return (0.0);
    else if (n == 1)
	return (*(x + (iy * ny) + ix));

    /* Compute limits for this pixel */
    nx2 = ndx / 2;
    jx1 = ix - nx2;
    if (jx1 < 0)
	jx1 = 0;
    jx2 = ix + nx2 + 1;
    if (jx2 > nx)
	jx2 = nx;
    ny2 = ndy / 2;
    jy1 = iy - ny2;
    if (jy1 < 0)
	jy1 = 0;
    jy2 = iy + ny2 + 1;
    if (jy2 > ny)
	jy2 = ny;

    /* Initialize actual number of pixels used for this pixel */
    n = 0;

    /* Set up working vector for this pixel */
    sum = 0.0;
    for (jy = jy1; jy < jy2; jy++) {
	img = x + (jy * nx) + jx1;
	for (jx = jx1; jx < jx2; jx++) {
	    if (*img != bpvalr4) {
		sum = sum + (double) *img;
		n++;
		}
	    img++;
	    }
	}

    if (n < 1)
	return (rval);
    else
	return ((float) (sum / (double) n));
}


double
meanpixr8 (x, dval, ix, iy, nx, ny, ndx, ndy)

double	*x;	/* Image buffer */
double	dval;	/* Image pixel value */
int	ix,iy;	/* Pixel around which to compute median */
int	nx,ny;	/* Number of columns and rows in image */
int	ndx;	/* Number of columns over which to compute the median */
int	ndy;	/* Number of rows over which to compute the median */

{
    double *img;
    double sum;
    int n, nx2, ny2;
    int jx, jx1, jx2, jy, jy1, jy2;

    n = ndx * ndy;
    if (n <= 0)
	return (0.0);
    else if (n == 1)
	return (*(x + (iy * ny) + ix));

    /* Compute limits for this pixel */
    nx2 = ndx / 2;
    jx1 = ix - nx2;
    if (jx1 < 0)
	jx1 = 0;
    jx2 = ix + nx2 + 1;
    if (jx2 > nx)
	jx2 = nx;
    ny2 = ndy / 2;
    jy1 = iy - ny2;
    if (jy1 < 0)
	jy1 = 0;
    jy2 = iy + ny2 + 1;
    if (jy2 > ny)
	jy2 = ny;

    /* Initialize actual number of pixels used for this pixel */
    n = 0;

    /* Set up working vector for this pixel */
    sum = 0.0;
    for (jy = jy1; jy < jy2; jy++) {
	img = x + (jy * nx) + jx1;
	for (jx = jx1; jx < jx2; jx++) {
	    if (*img != bpval) {
		sum = sum + (double) *img;
		n++;
		}
	    img++;
	    }
	}

    if (n < 1)
	return (dval);
    else
	return (sum / (double) n);
}


/* Gaussian filter an image */

char *
gaussfilt (buff, header, ndx, ndy, nlog)

char	*buff;		/* Image buffer */
char	*header;	/* FITS image header */
int	ndx;		/* Number of columns over which to compute the Gaussian */
int	ndy;		/* Number of rows over which to compute the Gaussian */
int	nlog;		/* Logging interval in pixels */

{
    char *buffret;	/* Modified image buffer (returned) */
    int nx,ny;		/* Number of columns and rows in image */
    int ix,iy;		/* Pixel around which to compute median */
    int npix;		/* Number of pixels in image */
    int bitpix;		/* Number of bits per pixel (<0=floating point) */
    int naxes;

    hgeti4 (header, "BITPIX", &bitpix);
    hgeti4 (header, "NAXIS", &naxes);
    hgeti4 (header, "NAXIS1", &nx);
    if (naxes > 1)
	hgeti4 (header, "NAXIS2", &ny);
    else
	ny = 1;
    npix = nx * ny;
    hgetr8 (header, "BLANK", &bpval);

    gausswt (ndx, ndy, nx);

    buffret = NULL;
    if (bitpix == 16) {
	short *b1, *b2, *buffout;
	bpvali2 = (short) bpval;
	buffret = (char *) calloc (npix, sizeof (short));
	buffout = (short *) buffret;
	b1 = (short *) buff;
	b2 = buffout;
	for (iy = 0; iy < ny; iy++) {
	    for (ix = 0; ix < nx; ix++) {
		*b2++ = gausspixi2 (buff, *b1++, ix, iy, nx, ny);
		}
	    if (nlog > 0 && (iy+1)%nlog == 0)
		fprintf (stderr,"GAUSSFILT: %d/%d lines filtered\r", iy+1,ny);
	    }
	fprintf (stderr,"\n");
	}
    else if (bitpix == 32) {
	int *b1, *b2, *buffout;
	bpvali4 = (int) bpval;
	buffret = (char *) calloc (npix, sizeof (int));
	buffout = (int *) buffret;
	b1 = (int *) buff;
	b2 = buffout;
	for (iy = 0; iy < ny; iy++) {
	    for (ix = 0; ix < nx; ix++) {
		*b2++ = gausspixi4 (buff, *b1++, ix, iy, nx, ny);
		}
	    if (nlog > 0 && (iy+1)%nlog == 0)
		fprintf (stderr,"GAUSSFILT: %d/%d lines filtered\r", iy+1,ny);
	}
	fprintf (stderr,"\n");
	}
    else if (bitpix == -32) {
	float *b1, *b2, *buffout;
	bpvalr4 = (float) bpval;
	buffret = (char *) calloc (npix, sizeof (float));
	buffout = (float *) buffret;
	b1 = (float *) buff;
	b2 = buffout;
	for (iy = 0; iy < ny; iy++) {
	    for (ix = 0; ix < nx; ix++) {
		*b2++ = gausspixr4 (buff, *b1++, ix, iy, nx, ny);
		}
	    if (nlog > 0 && (iy+1)%nlog == 0)
		fprintf (stderr,"GAUSSFILT: %d/%d lines filtered\r", iy+1,ny);
	    }
	fprintf (stderr,"\n");
	}
    else if (bitpix == -64) {
	double *b1, *b2, *buffout;
	buffret = (char *) calloc (npix, sizeof (double));
	buffout = (double *) buffret;
	b1 = (double *) buff;
	b2 = buffout;
	for (iy = 0; iy < ny; iy++) {
	    for (ix = 0; ix < nx; ix++) {
		*b2++ = gausspixr8 (buff, *b1++, ix, iy, nx, ny);
		}
	    if (nlog > 0 && (iy+1)%nlog == 0)
		fprintf (stderr,"GAUSSFILT: %d/%d lines filtered\r", iy+1,ny);
	    }
	fprintf (stderr,"\n");
	}
    if (nlog > 0 && (iy+1)%nlog == 0)
	fprintf (stderr,"GAUSSFILT: %d/%d lines filtered\n", iy,ny);
    return (buffret);
}


/* Set blank pixels to the Gaussian weighted sum of a box around each one */

char *
gaussfill (buff, header, ndx, ndy, nlog)

char	*buff;	/* Image buffer */
char	*header; /* FITS image header */
int	ndx;	/* Number of columns over which to compute the mean */
int	ndy;	/* Number of rows over which to compute the mean */
int	nlog;	/* Logging interval in pixels */

{
char	*buffret;	/* Modified image buffer (returned) */
int	nx,ny;	/* Number of columns and rows in image */
int	ix,iy;	/* Pixel around which to compute median */
int	npix;	/* Number of pixels in image */
int	bitpix;	/* Number of bits per pixel (<0=floating point) */
int	naxes;

    hgeti4 (header, "BITPIX", &bitpix);
    hgeti4 (header, "NAXIS", &naxes);
    hgeti4 (header, "NAXIS1", &nx);
    if (naxes > 1)
	hgeti4 (header, "NAXIS2", &ny);
    else
	ny = 1;
    npix = nx * ny;
    hgetr8 (header, "BLANK", &bpval);

    nfilled = 0;

    gausswt (ndx, nx);

    buffret = NULL;
    if (bitpix == 16) {
	short *b1, *b2, *buffout;
	bpvali2 = (short) bpval;
	buffret = (char *) calloc (npix, sizeof (short));
	buffout = (short *) buffret;
	b1 = (short *) buff;
	b2 = buffout;
	for (iy = 0; iy < ny; iy++) {
	    for (ix = 0; ix < nx; ix++) {
		if (*b1 != bpvali2)
		    *b2++ = *b1++;
		else {
		    *b2++ = gausspixi2 (buff, *b1++, ix, iy, nx, ny);
		    nfilled++;
		    }
		}
	    if (nlog > 0 && (iy+1)%nlog == 0)
		fprintf (stderr,"GAUSSFILL: %d lines, %d pixels filled\r", iy+1, nfilled);
	    }
	}
    else if (bitpix == 32) {
	int *b1, *b2, *buffout;
	bpvali4 = (int) bpval;
	buffret = (char *) calloc (npix, sizeof (int));
	buffout = (int *) buffret;
	b1 = (int *) buff;
	b2 = buffout;
	for (iy = 0; iy < ny; iy++) {
	    for (ix = 0; ix < nx; ix++) {
		if (*b1 != bpvali4)
		    *b2++ = *b1++;
		else {
		    *b2++ = gausspixi4 (buff, *b1++, ix, iy, nx, ny);
		    nfilled++;
		    }
		}
	    if (nlog > 0 && (iy+1)%nlog == 0)
		fprintf (stderr,"GAUSSFILL: %d lines, %d pixels filled\r", iy+1, nfilled);
	    }
	}
    else if (bitpix == -32) {
	float *b1, *b2, *buffout;
	buffret = (char *) calloc (npix, sizeof (float));
	buffout = (float *) buffret;
	bpvalr4 = (float) bpval;
	b1 = (float *) buff;
	b2 = buffout;
	for (iy = 0; iy < ny; iy++) {
	    for (ix = 0; ix < nx; ix++) {
		if (*b1 != bpvalr4)
		    *b2++ = *b1++;
		else {
		    *b2++ = gausspixr4 (buff, *b1++, ix, iy, nx, ny);
		    nfilled++;
		    }
		}
	    if (nlog > 0 && (iy+1)%nlog == 0)
		fprintf (stderr,"GAUSSFILL: %d lines, %d pixels filled\r", iy+1, nfilled);
	    }
	}
    else if (bitpix == -64) {
	double *b1, *b2, *buffout;
	buffret = (char *) calloc (npix, sizeof (double));
	buffout = (double *) buffret;
	b1 = (double *) buff;
	b2 = buffout;
	for (iy = 0; iy < ny; iy++) {
	    for (ix = 0; ix < nx; ix++) {
		if (*b1 != bpval)
		    *b2++ = *b1++;
		else {
		    *b2++ = gausspixr8 (buff, *b1++, ix, iy, nx, ny);
		    nfilled++;
		    }
		}
	    if (nlog > 0 && (iy+1)%nlog == 0)
		fprintf (stderr,"GAUSSFILL: %d lines, %d pixels filled\r", iy+1, nfilled);
	    }
	}
    if (nlog > 0)
	fprintf (stderr,"GAUSSFILL: %d lines, %d pixels filled\n", iy, nfilled);
    return (buffret);
}

/* Compute Gaussian weighting function */

static double *gwt = NULL;
static int *ixbox;	/* Vector of x offsets in image */
static int *iybox;	/* Vector of y offsets in image */
static int *ipbox;	/* Vector of pixel offsets in image */
static int npbox;	/* Number of pixels in replacement vector */

static int mpbox = 1;	/* Minimum number of good pixels to use value */
void
setminpix (minpix)
int minpix;
{ mpbox = minpix; return; }

static double hwidth = 1.0;	/* Half-width at half-height of Gaussian */
void
setghwidth (ghwidth)
double ghwidth;
{ hwidth = ghwidth; return; }

static int nsub = 1;	/* Number of sub-computations per pixel */
void
setsubpix (nsubpix)
double nsubpix;
{ nsub = nsubpix; return; }

/* Compute Gaussian weights for a square region */

void 
gausswt (mx, my, nx)

int	mx;	/* Width in pixels over which to compute the Gaussian */
int	my;	/* Height in pixels over which to compute the Gaussian */
int	nx;	/* Number of columns (naxis1) in image */
{
    int	i, idr, idc, jx, jy, jr;
    double dsub, xd0, xd, xdr, xdc, twt, rad2;
    extern void setscale();

    setscale (0);

    dsub = (double) nsub;
    xd0 = (dsub - 1.0) / (dsub * 2.0);
    xd = 1.0 / (hwidth * dsub);
    twt = 0.0;

    if (gwt != NULL) {
	free (gwt);
	free (ixbox);
	free (iybox);
	free (ipbox);
	}
    npbox = mx * my;
    gwt = (double *) calloc (npbox, sizeof(double));
    ixbox = (int *) calloc (npbox, sizeof(int));
    iybox = (int *) calloc (npbox, sizeof(int));
    ipbox = (int *) calloc (npbox, sizeof(int));
    idr = (-my / 2) - 1;
    i = 0;
    for (jy = 0; jy < my; jy++) {
	idr++;
	idc = (-mx / 2) - 1;
	for (jx = 0; jx < mx; jx++) {
	    idc++;
	    gwt[i] = 0.0;
	    xdr = ((double) idr - xd0) / hwidth;
	    for (jr = 0; jr < nsub; jr++) {
		xdc = ((double)(idc) - xd0) / hwidth;
		for (jr = 0; jr < nsub; jr++) {
		    rad2 = (xdc * xdc) + (xdr * xdr);
		    gwt[i] = gwt[i] + exp (-rad2 / 2.0);
		    xdc = xdc + xd;
		    }
		xdr = xdr + xd;
		}
	    twt = twt + gwt[i];
	    iybox[i] = idc;
	    ixbox[i] = idr;
	    ipbox[i] = (idr * nx) + idc;
	    i++;
	    }
	}

    /* Normalize to 1.0 */
    for (i = 0; i < npbox; i++)
	gwt[i] = gwt[i] / twt;

    return;
}


/* Compute Gaussian-weighted mean of a square group of pixels */
/* gausswt() must be called first to set up weighting vector */

short
gausspixi2 (image, ival, ix, iy, nx, ny)

short	*image;	/* Image buffer */
short	ival;	/* Pixel value */
int	ix,iy;	/* Pixel around which to compute Gaussian-weighted mean */
int	nx,ny;	/* Number of columns and rows in image */
{
    short *img;
    double twt, tpix;
    double flux;
    int i, ixi, iyi;
    int  np;

    if (npbox <= 1)
	return (ival);

    twt = 0.0;
    tpix = 0.0;
    np = 0;
    for (i = 0; i < npbox; i++) {
	ixi = ix + ixbox[i];
	iyi = iy + iybox[i];
	if (ixi > -1 && iyi > -1 && ixi < nx && iyi < ny) {
	    img = image + (iyi * ny) + ixi;
	    if (*img != bpvali2) {
		flux = (double) *img;
		twt = twt + gwt[i];
		tpix = tpix + gwt[i] * flux;
		np++;
		}
	    }
	}

    /* If enough surrounding pixels are non-zero, replace the current pixel */
    if (np > mpbox && twt > 0.0) {
	if (twt < 1.0)
	    tpix = tpix / twt;
	return ((short) tpix);
	}
    else
	return (ival);
}


int
gausspixi4 (image, ival, ix, iy, nx, ny)

int	*image;	/* Image buffer */
int	ival;	/* Pixel value */
int	ix,iy;	/* Pixel around which to compute Gaussian-weighted mean */
int	nx,ny;	/* Number of columns and rows in image */
{
    double twt, tpix;
    double flux;
    int *img;
    int i, ixi, iyi;
    int  np;

    if (npbox <= 1)
	return (ival);

    twt = 0.0;
    tpix = 0.0;
    np = 0;
    for (i = 0; i < npbox; i++) {
	ixi = ix + ixbox[i];
	iyi = iy + iybox[i];
	if (ixi > -1 && iyi > -1 && ixi < nx && iyi < ny) {
	    img = image + (iyi * ny) + ixi;
	    if (*img != bpvali4) {
		flux = (double) *img;
		twt = twt + gwt[i];
		tpix = tpix + gwt[i] * flux;
		np++;
		}
	    }
	}

    /* If enough surrounding pixels are non-zero, replace the current pixel */
    if (np > mpbox && twt > 0.0) {
	if (twt < 1.0)
	    tpix = tpix / twt;
	return ((int) tpix);
	}
    else
	return (ival);
}


float
gausspixr4 (image, rval, ix, iy, nx, ny)

float	*image;	/* Image buffer */
float	rval;	/* Pixel value */
int	ix,iy;	/* Pixel around which to compute Gaussian-weighted mean */
int	nx,ny;	/* Number of columns and rows in image */
{
    double twt, tpix;
    double flux;
    float *img;
    int i, ixi, iyi;
    int  np;

    if (npbox <= 1)
	return (rval);

    twt = 0.0;
    tpix = 0.0;
    np = 0;
    for (i = 0; i < npbox; i++) {
	ixi = ix + ixbox[i];
	iyi = iy + iybox[i];
	if (ixi > -1 && iyi > -1 && ixi < nx && iyi < ny) {
	    img = image + (iyi * ny) + ixi;
	    if (*img != bpvalr4) {
		flux = (double) image[ixi + (iyi * ny)];
		twt = twt + gwt[i];
		tpix = tpix + gwt[i] * flux;
		np++;
		}
	    }
	}

    /* If enough surrounding pixels are non-zero, replace the current pixel */
    if (np > mpbox && twt > 0.0) {
	if (twt < 1.0)
	    tpix = tpix / twt;
	return ((float) tpix);
	}
    else
	return (rval);
}


double
gausspixr8 (image, dval, ix, iy, nx, ny)

double	*image;	/* Image buffer */
double	dval;	/* Pixel value */
int	ix,iy;	/* Pixel around which to compute Gaussian-weighted mean */
int	nx,ny;	/* Number of columns and rows in image */
{
    double *img;
    double twt, tpix;
    double flux;
    int i, ixi, iyi;
    int  np;

    if (npbox <= 1)
	return (dval);

    twt = 0.0;
    tpix = 0.0;
    np = 0;
    for (i = 0; i < npbox; i++) {
	ixi = ix + ixbox[i];
	iyi = iy + iybox[i];
	if (ixi > -1 && iyi > -1 && ixi < nx && iyi < ny) {
	    img = image + (iyi * ny) + ixi;
	    if (*img != bpval) {
		flux = image[ixi + (iyi * ny)];
		twt = twt + gwt[i];
		tpix = tpix + gwt[i] * flux;
		np++;
		}
	    }
	}

    /* If enough surrounding pixels are non-zero, replace the current pixel */
    if (np > mpbox && twt > 0.0) {
	if (twt < 1.0)
	    tpix = tpix / twt;
	return (tpix);
	}
    else
	return (dval);
}


/* Return image buffer reduced by a given factor */

char *
ShrinkFITSImage (header, image, xfactor, yfactor, mean, bitpix, nlog)

char	*header;	/* Image header */
char	*image;		/* Image bytes to be filtered */
int	xfactor;	/* Factor by which to reduce horizontal size of image */
int	yfactor;	/* Factor by which to reduce vertical size of image */
int	mean;		/* If 0, sum pixels, else substitute mean */
int	bitpix;		/* Number of bits per output pixel (neg=f.p.) */
int	nlog;		/* Logging interval in lines */

{

char	*image1;
int	nx,ny;		/* Number of columns and rows in input image */
int	nx1,ny1;	/* Number of columns and rows in input image */
int	ix,iy;		/* Output pixel coordinates */
int	jx,jy;		/* Input pixel coordinates */
int	kx, ky;
int	nxf, nyf;
int	npix1;		/* Number of pixels in output image */
int	bitsin;		/* Number of bits per input pixel (<0=floating point) */
int	naxes;
double	pixij;		/* Summed value of rebinned pixel */
double	bzero, bscale;
double	pixval, dnp;
short	*buffi2 = NULL;
int	*buffi4 = NULL;
float	*buffr4 = NULL;
double	*buffr8 = NULL;

    /* Get bits per pixel in input image */
    hgeti4 (header, "BITPIX", &bitsin);
    if (bitpix == 0) {
	bitpix = bitsin;
	mean = 1;
	}

    /* Get scaling of input image, if any */
    bzero = 0.0;
    hgetr8 (header, "BZERO", &bzero);
    bscale = 1.0;
    hgetr8 (header, "BSCALE", &bscale);

    /* Get size of input horizontal axis */
    hgeti4 (header, "NAXIS1", &nx);

    /* Set horizontal axis for output image */
    if (nx > xfactor)
	nx1 = nx / xfactor;
    else
	nx1 = nx;

    /* Get number of axes */
    hgeti4 (header, "NAXIS", &naxes);

    /* Set vertical axis for output image */
    if (naxes > 1) {
	hgeti4 (header, "NAXIS2", &ny);
	if (ny > yfactor)
	    ny1 = ny / yfactor;
	else
	    ny1 = ny;
	}
    else {
	ny = 1;
	ny1 = 1;
	}
    npix1 = nx1 * ny1;

    /* Allocate output image buffer and initialize pointer into it */
    image1 = NULL;
    if (bitpix == 16) {
	image1 = (char *) calloc (npix1, sizeof (short));
	buffi2 = (short *) image1;
	}
    else if (bitpix == 32) {
	image1 = (char *) calloc (npix1, sizeof (int));
	buffi4 = (int *) image1;
	}
    else if (bitpix == -32) {
	image1 = (char *) calloc (npix1, sizeof (float));
	buffr4 = (float *) image1;
	}
    else if (bitpix == -64) {
	image1 = (char *) calloc (npix1, sizeof (double));
	buffr8 = (double *) image1;
	}

    /* Fill output buffer */
    for (jy = 0; jy < ny1; jy++) {
	for (jx = 0; jx < nx1; jx++) {
	    pixij = 0.0;
	    ky = (jy * yfactor);
	    if (ky + yfactor > ny)
		nyf = ny - ky + 1;
	    else
		nyf = yfactor;
	    dnp = 0.0;
	    for (iy = 0; iy < nyf; iy++) {
		kx = (jx * xfactor);
		if (kx + xfactor > nx)
		    nxf = nx - kx + 1;
		else
		    nxf = xfactor;
		for (ix = 0; ix < nxf; ix++) {
		    pixval = getpix (image, bitsin, nx,ny,bzero,bscale,kx++,ky);
		    pixij = pixij + pixval;
		    dnp++;
		    }
		ky++;
		}
	    if (mean) {
	 	switch (bitpix) {
		    case 16:
			*buffi2++ = (short) (pixij / dnp);
			break;
		    case 32:
			*buffi4++ = (int) (pixij / dnp);
			break;
		    case -32:
			*buffr4++ = (float) (pixij / dnp);
			break;
		    case -64:
			*buffr8++ = (pixij / dnp);
			break;
		    }
		}
	    else {
	 	switch (bitpix) {
		    case 16:
			if (pixij < 32768.0)
			    *buffi2++ = (short) pixij;
			else
			    *buffi2++ = 32767;
			break;
		    case 32:
			*buffi4++ = (int) pixij;
			break;
		    case -32:
			*buffr4++ = (float) pixij;
			break;
		    case -64:
			*buffr8++ = pixij;
			break;
		    }
		}
	    }
	if ((jy+1)%nlog == 0)
	    fprintf (stderr,"IMRESIZE: %d/%d lines created\r", jy+1, ny1);
	}
    if (nlog > 0)
	fprintf (stderr,"\n");

    return (image1);
}

/* Return image header with dimensions reduced by a given factor */

char *
ShrinkFITSHeader (filename, header, xfactor, yfactor, mean, bitpix)

char	*filename;	/* Name of image file before shrinking */
char	*header;	/* Image header */
int	xfactor;	/* Factor by which to reduce horizontal size of image */
int	yfactor;	/* Factor by which to reduce vertical size of image */
int	mean;		/* If 0, sum pixels, else substitute mean */
int	bitpix;		/* Number of bits per output pixel (neg=f.p.) */

{
    char *newhead;	/* New header for shrunken file */
    char history[64];
    int nbhead;		/* Number of bytes in header */
    int naxes;
    int nblocks;
    int nx, ny, nx1, ny1;
    double crpix1, crpix2, cdelt1, cdelt2;
    double dfac;

    nbhead = strlen (header);
    nblocks = nbhead / 2880;
    if (nblocks * 2880 < nbhead)
	nblocks = nblocks + 1;
    nbhead = 2880 * (nblocks +1);
    newhead = (char *) calloc (nbhead, 1);
    strcpy (newhead, header);

    /* Set pixel size in bits */
    if (bitpix == 0) {
	hgeti4 (header, "BITPIX", &bitpix);
	mean = 1;
	}
    hputi4 (newhead, "BITPIX", bitpix);

    /* Set new image horizontal dimension */
    hgeti4 (header, "NAXIS1", &nx);
    if (nx > xfactor)
	nx1 = nx / xfactor;
    else
	nx1 = nx;
    hputi4 (newhead, "NAXIS1", nx1);

    /* Set new image vertical dimension */
    hgeti4 (header, "NAXIS", &naxes);
    if (naxes > 1) {
	hgeti4 (header, "NAXIS2", &ny);
	if (ny > yfactor)
	    ny1 = ny / yfactor;
	else
	    ny1 = ny;
	hputi4 (newhead, "NAXIS2", ny1);
	}
    else {
	ny = 1;
	ny1 = 1;
	}

    /* Fix WCS */
    dfac = (double) xfactor;
    if (hgetr8 (header, "CRPIX1", &crpix1)) {
	crpix1 = (crpix1 / dfac) + 0.5;
	hputr8 (newhead, "CRPIX1", crpix1);
	}
    if (hgetr8 (header, "CDELT1", &cdelt1)) {
	cdelt1 = (cdelt1 * dfac);
	hputr8 (newhead, "CDELT1", cdelt1);
	}
    if (hgetr8 (header, "CD1_1", &cdelt1)) {
	cdelt1 = (cdelt1 * dfac);
	hputr8 (newhead, "CD1_1", cdelt1);
	}
    if (hgetr8 (header, "CD1_2", &cdelt1)) {
	cdelt1 = (cdelt1 * dfac);
	hputr8 (newhead, "CD1_2", cdelt1);
	}
    dfac = (double) yfactor;
    if (hgetr8 (header, "CRPIX2", &crpix2)) {
	crpix2 = (crpix2 / dfac) + 0.5;
	hputr8 (newhead, "CRPIX2", crpix2);
	}
    if (hgetr8 (header, "CDELT2", &cdelt2)) {
	cdelt2 = (cdelt2 * dfac);
	hputr8 (newhead, "CDELT2", cdelt2);
	}
    if (hgetr8 (header, "CD2_1", &cdelt2)) {
	cdelt2 = (cdelt2 * dfac);
	hputr8 (newhead, "CD2_1", cdelt2);
	}
    if (hgetr8 (header, "CD2_2", &cdelt2)) {
	cdelt2 = (cdelt2 * dfac);
	hputr8 (newhead, "CD2_2", cdelt2);
	}

    /* Add keyword to denote this operation */
    if (strlen (filename) < 40)
	sprintf (history, "%s blocked %dx%d", filename, xfactor, yfactor);
    else
	sprintf (history, "%40s blocked / %dx%d", filename, xfactor, yfactor);
    if (mean)
	strcat (history, " mean");
    else
	strcat (history, " sum");
    hputs (newhead, "IMSHRINK", history);
    return (newhead);
}

static double apint();
static double imapfr();

/* PhotPix -- Compute counts within a strictly defined circular aperture
 *	      returns sum of pixel flux
 *	      Jul  4 1984 Original Fortran program by Sam Conner at MIT
 *	      Mar  2 1987 Ported to Unix by Doug Mink at SAO
 *	      Jan 30 2002 translated from Fortran to C by Doug Mink at SAO
 */

double
PhotPix (imbuff,header,cx,cy,rad,sumw)

char	*imbuff;		/* address of start of image buffer */
char	*header;	/* image FITS header */
double	cx,cy;		/* center x,y of aperture */
double	rad;		/* radius of aperture */
double	*sumw;		/* sum of values of pixel weights (returned) */
{
    double x, y, factor, flux, bs, bz, wsum;
    int ix1, ix2, iy1, iy2, ix, iy, bitpix, nx, ny;

    *sumw = 0.0;
    wsum = 0.0;
    hgeti4 (header, "BITPIX", &bitpix);
    hgeti4 (header, "NAXIS1", &nx);
    hgeti4 (header, "NAXIS2", &ny);
    hgetr8 (header, "BSCALE", &bs);
    hgetr8 (header, "BZERO", &bz);

    /* Find range of ys to check */
    iy1 = (int) (cy - rad);
    iy2 = (int) (cy + rad + 0.99999);
    if (iy1 < 1)
	iy1 = 1;
    if (iy2 > ny)
	iy2 = ny;

    /* Find range of xs to check */
    ix1 = (int) (cx - rad);
    ix2 = (int) (cx + rad + 0.99999);
    if (ix1 < 1)
	ix1 = 1;
    if (ix2 > nx)
	ix2 = nx;

    for (iy = iy1; iy <= iy2; iy++) {
	y = (double) iy;
	for (ix = ix1; ix <= ix2; ix++) {
	    x = (double) ix;
	    factor = imapfr (x,y,cx,cy,rad);
	    *sumw = *sumw + factor;
	    flux = getpix1 (imbuff, bitpix, nx, ny, bz, bs, ix, iy);
	    wsum = wsum + (factor * flux);

	    /* fprintf (stderr, "IMAPSB: (%d,%d)= %f weight= %f\n",
		    ix,iy,flux,factor); */
	    }
	}

 /* fprintf (stderr, "IMAPSB: sum of weights = %f\n", *sumw);
    fprintf (stderr, "IMAPSB: sum of weighted intensity = %f\n", wsum);
 */

    return (wsum);
}

/* IMAPFR -- Determine the fraction of a square pixel that is included
 *	     within the boundary of a circle of arbitrary center and radius
 */

static double
imapfr (pcol,prow,ccol,crow,rad)

double	pcol,prow;	/* column, row of pixel */
double	ccol,crow;	/* column, row of center of circle */
double	rad;		/* radius of circle in pixels */

{
    int icorn[4], ncorn, j, i;
    double dist, xdiff, y0, y1, ydiff, x0, x1, x, y, dc5, dr5;
    double cterm, ulim, llim, dc, dr, dx, dy;
    double frac;		/* fraction of pixel within circle */

    /* Compute distance to center of circle from most distant corner of pixel */
    dc = fabs (pcol - ccol);
    dr = fabs (prow - crow);
    dc5 = dc + 0.5;
    dr5 = dr + 0.5;
    dist = sqrt ((dc5 * dc5) + (dr5 * dr5));
    dc5 = dc - 0.5;
    dr5 = dr - 0.5;

    /* if the pixel is completely enclosed by the aperture, return 1 */
    if (dist <= rad)
	return (1.0);

    /* if the pixel is not completely included, compute distance from the
       center of the circle to the nearest point on the periphery of the pixel*/
    if ((pcol-0.5) < ccol && ccol < (pcol+.5)) {
	dist = dr5;

	/* the pixel is completely excluded from the aperture */
	if (dist >= rad)
	      return (0.0);

	else if ((prow - 0.5) < crow && crow < (prow + 0.5)) {
	    dist = dc5;

	    /* the pixel is completely excluded from the aperture */
	    if (dist >= rad)
	      return (0.0);
	    }

	else
	    dist = sqrt (dc5*dc5 + dr5*dr5);

	/* the pixel is completely excluded from the aperture */
	if (dist >= rad)
	    return (0.0);

	}

    /* If the pixel is partially included in the aperture, determine how
       many corners are enclosed */
    ncorn = 1;
    y = prow - 1.5;
    for (i = 0; i < 2; i++) {
	y = y + 1.0;
	x = pcol - 1.50;
	for (j = 0; j < 2; j++) {
	    icorn[j+(i*2)] = 0;
	    x = x + 1.0;
	    dx = x - ccol;
	    dy = y - crow;
	    dist = sqrt (dx*dx + dy*dy);
	    if (dist < rad) {
	        ncorn = ncorn + 1;
	        icorn[j+(i*2)] = 1;
	        }
	    }
	}

    /* Depending on number of corners enclosed,
       branch to appropriate computation */

    /* If no corners are enclosed, determine whether the slice is vertical or
       horizontal */
    if (ncorn < 1) {

	/* If the slice is vertical (at the sides of the aperture,
	   at extreme x), determine the limits of integration */
	if ((pcol - 0.5) >= ccol || ccol >= (pcol + 0.5)) {
	    xdiff = fabs (pcol - ccol) - 0.5;
	    dx = sqrt (rad*rad - xdiff*xdiff);
	    y0 = crow - dx;
	    y1 = crow + dx;
	    if (ccol >= pcol)
		x = pcol + 0.5;
	    else
		x = pcol - 0.5;

	    /* Evaluate integral */
	    frac = -xdiff * (y1 - y0) + apint ((y1 - crow), rad) -
		   apint ((y0 - crow), rad);
	    return (frac);
	    }

	/* If the slice is horizontal (at top or bottom of the aperture, at
	   extreme y), determine the limits of integration. */
	else {
	    ydiff = fabs (prow - crow) - 0.5;
	    dy = sqrt (rad*rad - ydiff*ydiff);
	    x0 = ccol - dy;
	    x1 = ccol + dy;
	    if (crow >= prow)
		y = prow + 0.5;
	    else
		y = prow - 0.5;

	    /* Evaluate integral */
	    frac = -ydiff*(x1 - x0) + apint ((x1 - ccol),rad) -
		   apint ((x0 - ccol),rad);
	    return (frac);
	    }
	}

    /* if one corner is enclosed, find direction (ne,nw,se,sw) from the center*/
    else if (ncorn < 2) {

	if (pcol < ccol) {

	    /* Determine the limits of integration */
	    ydiff = fabs (prow - crow) - 0.5;;
	    x0 = ccol - sqrt (rad*rad - ydiff*ydiff);
	    if (crow >= prow)
		y = prow + 0.5;
	    else
		y = prow - 0.5;

	    /* Evaluate integral */
	    frac = -ydiff*(pcol + 0.5 - x0) + apint ((pcol + 0.5 - ccol),rad) -
		   apint ((x0 - ccol),rad);
	    return (frac);
	    }

	else {

	    /* Determine the limits of integration */
	    ydiff = fabs (crow - prow) - 0.5;
	    x1 = ccol + sqrt (rad*rad - ydiff*ydiff);
	    if (crow >= prow)
		y = prow + 0.5;
	    else
		y = prow - 0.5;

	    /* Evaluate integral */
	    frac = -ydiff*(x1 - pcol + 0.5) + apint ((x1 - ccol),rad) -
		   apint ((pcol - .5 - ccol),rad);
	    return (frac);
	    }
	}

    /* If two corners are enclosed, this problem has two cases: one in
       which the aperture boundary intersects the pixel boundary twice,
       and another in which it intersects the pixel boundaries 4 times */

    /* determine whether this may be a 4-intersection configuration */
    else if (ncorn < 3) {

	if (((pcol - 0.5) < ccol && ccol < (pcol + .5) &&
	    (fabs(crow - prow) + 0.5) < rad) ||
	    ((prow - 0.5) < crow && crow < (prow + 0.5) &&
	    (fabs(ccol - pcol) + 0.5) < rad)) {

/* if the pixel boundary intersects the aperture boundary in four places,
   determine whether the pixel is east-west or north-south. */

	    /* pixel is east-west */
	    if ((pcol - 0.5) >= ccol || (pcol + 0.5) <= ccol) {
		xdiff = dc + 0.5;

		/* Determine the limits of integration */
		dx = sqrt (rad*rad - xdiff*xdiff);
		y0 = crow - dx;
		y1 = crow + dx;
		if (pcol >= ccol)
		    x = pcol + .5;
		else
		    x = pcol - .5;
		frac = 1. - xdiff*(y0 - y1 + 1.0) +
			apint ((y0 - crow), rad) -
			apint ((prow - 0.5 - crow), rad) +
			apint ((prow + 0.5 - crow), rad) -
			apint ((y1 - crow), rad);
		return (frac);
		}

	    /* Pixel is north-south */

	    else {
		ydiff = dr + 0.5;

		/* determine the limits of integration */
		dy = sqrt (rad*rad - ydiff*ydiff);

		/* x0 is the x-coordinate of the small x intercept */
		x0 = ccol - dy;

		/* x1 is the x-coordinate of the large x intercept */
		x1 = ccol + dy;
		if (prow >= crow)
		    y = prow + 0.5;
		else
		    y = prow - 0.5;
		frac = 1. - ydiff * (x0 - x1 + 1.0) +
			apint ((x0 - ccol), rad) -
			apint ((pcol - 0.5 - ccol), rad) +
			apint ((pcol + 0.5 - ccol), rad) -
			apint ((x1 - ccol), rad);
		return (frac);
		}
	    }

	/* in the two-intersection case, determine which corners are included */
	else {

	    /* the pixel is north or south of the center of the aperture */
	    if (icorn[0]*icorn[1] == 1 || icorn[2]*icorn[3] == 1) {
		cterm = 0.5 - dr;
		ulim = apint ((pcol + 0.5 - ccol) ,rad);
		llim = apint ((pcol - 0.5 - ccol) ,rad);
		frac = cterm + ulim - llim;
		return (frac);
		}

	    /* the pixel is east or west of the center */
	    else {
		cterm = 0.5 - dc;
		ulim = apint ((prow + 0.5 - crow), rad);
		llim = apint ((prow - 0.5 - crow), rad);
		frac = cterm + ulim - llim;
		return (frac);
		}
	    }
	}

    /* if three corners are enclosed, determine whether the corner in question
       is east or west of the center of the aperture */

    else {
	if (pcol > ccol) {
	    ydiff = dr + 0.5;
	    if (prow >= crow)
		y = prow + 0.5;
	    else
		y = prow - 0.5;

	    x0 = ccol + sqrt (rad*rad - ydiff*ydiff);
	    frac = 1.0 - (ydiff * (pcol + 0.5 - x0)) +
		   apint ((pcol + 0.5 - ccol), rad) -
		   apint ((x0 - ccol), rad);
	    return (frac);
	    }
	else {
	    ydiff = dr + 0.5;
	    if (prow >= crow)
		y = prow + 0.5;
	    else
		y = prow - 0.5;
	    x0 = ccol - sqrt (rad*rad - ydiff*ydiff);
	    frac = 1. - ydiff*(x0 - pcol + 0.5) +
		   apint ((x0 - ccol),rad) -
		   apint ((pcol - 0.5 - ccol),rad);
	    return (frac);
	    }
	}
}


/* APINT -- Evaluate the integral of sqrt (rad**2 - x**2) dx at one limit */

static double
apint (x, rad)

double	x;
double	rad;
{
    double x2,rad2,arg,arg2,arcsin,pi;

    pi = 3.141592654;

    arg = x / rad;
    x2 = x * x;
    rad2 = rad * rad;
    arg2 = x2 / rad2;

    arcsin = atan2 (arg, sqrt (1.0 - arg2));

    if ((1. - fabs (arg)) < 0.000001) {
	if (arg >= 0)
	    arcsin = pi / 2.0;
	else
	    arcsin = -pi / 2.0;
	}

    return (0.5 * (x * sqrt (rad2 - x2) + rad2 * arcsin));
}


/* Oct 25 2005	New subroutine translated from Fortran imlib/smooth.f
 *
 * Jan 25 2006	Add subroutines to shrink an image
 * Mar  1 2006	Add subroutines for Gaussian smoothing/filling
 * Apr  3 2006	Fix error return in gausspix()
 * Apr  3 2006	Include math.h and fitsfile.h (instead of fitshead.h)
 * Apr  7 2006	Add filling subroutines medfill(), meanfill(), gaussfill()
 * Apr  7 2006	Add subtroutine to set bad pixels from second image
 * Apr 27 2006	Add and correct comments
 * May  8 2006	Print final number of pixels filled; add getnfilled()
 * May 16 2006	Do not use BLANK from bad pixel file if set on command line 
 * Jun 20 2006	Drop unused variables
 * Jun 30 2006	Add number of lines in output image to log message
 * Jul  5 2006	Add SetBadVal() to set pixels less than min to bad
 * Jul  6 2006	Make both dimensions variable for Gaussian
 * Jul  7 2006	SetBadVal() now checks min and max allowable values
 * Sep 25 2006	Fix bug so CDELT1 and CDELT2 are scaled correctly
 *
 * Jan  8 2007	Drop unused variables from SetBadVal()
 * Jan 11 2007	Add circular aperture photometry in PhotPix()
 */
