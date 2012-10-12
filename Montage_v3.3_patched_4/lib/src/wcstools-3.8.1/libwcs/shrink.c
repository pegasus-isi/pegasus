/* File libwcs/shrink.c
 * January 24, 2006
 * By Doug Mink, Harvard-Smithsonian Center for Astrophysics
 */

/* Return image buffer reduced by a given factor */

#include <string.h>             /* NULL, strlen, strstr, strcpy */
#include <stdio.h>
#include <stdlib.h>
#include "fitshead.h"

char *
ShrinkFITSImage (header, image, factor, floor, bitpix, nlog)

char	*header;	/* Image header */
char	*image;		/* Image bytes to be filtered */
int	factor;		/* Factor by which to reduce size of image */
int	floor;		/* Subtract this number from every value */
int	bitpix;		/* Number of bits per output pixel (neg=f.p.) */
int	nlog;		/* Logging interval in lines */

{

char	*image1;
int	nx,ny;		/* Number of columns and rows in input image */
int	nx1,ny1;	/* Number of columns and rows in input image */
int	ix,iy;		/* Output pixel coordinates */
int	jx,jy;		/* Input pixel coordinates */
int	npix;		/* Number of pixels in input image */
int	npix1;		/* Number of pixels in output image */
int	bitsin;		/* Number of bits per input pixel (<0=floating point) */
int	bytesin;	/* Number of bytes per input pixel */
int	naxes;
double	pixij;		/* Summed value of rebinned pixel */
double	bzero, bscale;
double	pixval, pixij, dnp;
short	*buffi2;
int	*buffi4;
float	*buffr4;
double	*buffr8;

    hgeti4 (header, "BITPIX", &bitsin);
    bytesin = bitsin / 8;
    if (bytesin < 0)
	bytesin = -bytesin;
    hgeti4 (header, "NAXIS", &naxes);
    hgeti4 (header, "NAXIS1", &nx);
    if (naxes > 1)
	hgeti4 (header, "NAXIS2", &ny);
    else
	ny = 1;
    bzero = 0.0;
    hgetr8 (header, "BZERO", &bzero);
    bscale = 1.0;
    hgetr8 (header, "BSCALE", &bscale);
    npix = nx * ny;
    nx1 = nx1 / factor;
    ny1 = ny / factor;
    npix1 = nx1 * ny1;

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

		if (mean)
		    *buffout++ = (short) (pixij / dnp);
		else {
		    if (pixij < 32768.0)
			*buffout++ = (short) pixij;
		    else
			*buffout++ = 32767;
		    }
		}
	    if ((jy+1)%nlog == 0)
		fprintf (stderr,"SHRINK: %d lines created\r", jy+1);
	    }
	if (nlog > 0)
	    fprintf (stderr,"\n");
	}


    for (jy = 0; jy < ny1; jy++) {
	for (jx = 0; jx < nx1; jx++) {
	    pixij = 0.0;
	    ky = (jy * factor);
	    if (ky + factor > ny)
		nyf = ny - ky + 1;
	    else
		nyf = factor;
	    dnp = 0.0;
	    for (iy = 0; iy < nyf; iy++) {
		kx = (jx * factor);
		if (kx + factor > nx)
		    nxf = nx - kx + 1;
		else
		    nxf = factor;
		for (ix = 0; ix < nxf; ix++) {
		    pixval = getpix (image, bitsin, nx,ny,bzero,bscale,kx++,ky);
		    pixfal = pixval - floor;
		    if (pixval < 0.0)
			pixval = 0.0;
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
	    fprintf (stderr,"SHRINK: %d lines created\r", jy+1);
	}
    if (nlog > 0)
	fprintf (stderr,"\n");

    return (image1);
}

/* Jan 25 2006	New subroutine based on libwcs/filter.c
 */
