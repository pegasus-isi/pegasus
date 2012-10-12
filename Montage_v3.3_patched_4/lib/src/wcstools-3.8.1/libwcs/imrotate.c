/*** File libwcs/imrotate.c
 *** June 26, 2008
 *** By Doug Mink, dmink@cfa.harvard.edu
 *** Harvard-Smithsonian Center for Astrophysics
 *** Copyright (C) 1996-2008
 *** Smithsonian Astrophysical Observatory, Cambridge, MA, USA

    This library is free software; you can redistribute it and/or
    modify it under the terms of the GNU Lesser General Public
    License as published by the Free Software Foundation; either
    version 2 of the License, or (at your option) any later version.

    This library is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
    Lesser General Public License for more details.
    
    You should have received a copy of the GNU Lesser General Public
    License along with this library; if not, write to the Free Software
    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

    Correspondence concerning WCSTools should be addressed as follows:
           Internet email: dmink@cfa.harvard.edu
           Postal address: Doug Mink
                           Smithsonian Astrophysical Observatory
                           60 Garden St.
                           Cambridge, MA 02138 USA
 */

#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <string.h>
#include "fitsfile.h"

static void RotWCSFITS();	/* rotate all the C* fields */

/* Rotate an image by 90, 180, or 270 degrees, with an optional
 * reflection across the vertical or horizontal axis.
 * verbose generates extra info on stdout.
 * return NULL if successful or rotated image.
 */

char *
RotFITS (pathname,header,image0,xshift,yshift,rotate,mirror,bitpix2,rotwcs,verbose)

char	*pathname;	/* Name of file which is being changed */
char	*header;	/* FITS header */
char	*image0;	/* Unrotated image pixels */
int	xshift;		/* Number of pixels to shift image horizontally, +=right */
int	yshift;		/* Number of pixels to shift image vertically, +=right */
int	rotate;		/* Angle to by which to rotate image (90, 180, 270) */
int	mirror;		/* Reflect image around 1=vertical, 2=horizontal axis */
int	bitpix2;	/* Number of bits per pixel in output image */
int	rotwcs;		/* If not =0, rotate WCS keywords, else leave them */
int	verbose;

{
    int bitpix1, ny, nx, nax;
    int x1, y1, x2, y2, nbytes;
    char *rotimage;
    char *image;
    char history[128];
    char *filename;
    double crpix;

    image = NULL;
    rotimage = NULL;

    if (rotate == 1)
	rotate = 90;
    else if (rotate == 2)
	rotate = 180;
    else if (rotate == 3)
	rotate = 270;
    else if (rotate < 0)
	rotate = rotate + 360;

    filename = strrchr (pathname,'/');
    if (filename)
	filename = filename + 1;
    else
	filename = pathname;

    /* Get image size */
    nax = 0;
    if (hgeti4 (header,"NAXIS",&nax) < 1) {
	if (verbose)
	    printf ("RotFITS: Not an image (NAXIS=%d)\n",nax);
	return (NULL);
	}
    else {
	if (hgeti4 (header,"NAXIS1",&nx) < 1) {
	    if (verbose)
		printf ("RotFITS: Not an image (NAXIS1=%d)\n",nx);
	    return (NULL);
	    }
	else {
	    if (hgeti4 (header,"NAXIS2",&ny) < 1) {
		if (verbose)
		    printf ("RotFITS: Not an image (NAXIS2=%d)\n",ny);
		return (NULL);
		}
	    }
	}
    bitpix1 = 16;
    hgeti4 (header,"BITPIX", &bitpix1);
    if (bitpix2 == 0)
	bitpix2 = bitpix1;

    /* Shift WCS fields in header */
    if (rotwcs && hgetr8 (header, "CRPIX1", &crpix)) {
	crpix = crpix + xshift;
	hputr8 (header, "CRPIX1", crpix);
	}
    if (rotwcs && hgetr8 (header, "CRPIX2", &crpix)) {
	crpix = crpix + yshift;
	hputr8 (header, "CRPIX2", crpix);
	}

    /* Rotate WCS fields in header */
    if (rotwcs && (rotate != 0 || mirror))
	RotWCSFITS (header, rotate, mirror, verbose);

    /* Compute size of image in bytes */
    switch (bitpix2) {
	case 8:
	    nbytes = nx * ny;
	    break;
	case 16:
	    nbytes = nx * ny * 2;
	    break;
	case 32:
	    nbytes = nx * ny * 4;
	    break;
	case -16:
	    nbytes = nx * ny * 2;
	    break;
	case -32:
	    nbytes = nx * ny * 4;
	    break;
	case -64:
	    nbytes = nx * ny * 8;
	    break;
	default:
	    if (verbose)
		printf ("RotFITS: Illegal BITPIX (%d)\n", bitpix2);
	    return (NULL);
	}

    if (bitpix1 != bitpix2) {
	sprintf (history,"Copy of image %s bits per pixel %d -> %d",
		filename, bitpix1, bitpix2);
	hputc (header,"HISTORY",history);
	if (verbose)
	    fprintf (stderr,"%s\n",history);
	}

    /* Shift image first */
    if (xshift != 0 || yshift != 0) {

	/* Allocate buffer for shifted image */
	image = (char *) calloc (nbytes, 1);
	if (image == NULL) {
	    if (verbose)
		printf ("RotFITS: Cannot allocate %d bytes for shifted image\n", nbytes);
	    return (NULL);
	    }

	for (x1 = 0; x1 < nx; x1++) {
	    x2 = x1 + xshift;
	    for (y1 = 0; y1 < ny; y1++) {
		y2 = y1 + yshift;
		if (y2 < ny)
		    movepix (image0,bitpix1,nx,x1,y1,image,bitpix2,nx,x2,y2);
		}
	    }
	sprintf (history,"Copy of image %s shifted by dx=%d dy=%d",
		 filename, xshift, yshift);
	hputc (header,"HISTORY",history);
	if (rotate == 0 && !mirror)
	    return (image);
	}
    else
	image = image0;

    /* Allocate buffer for rotated image */
    rotimage = (char *) calloc (nbytes, 1);
    if (rotimage == NULL) {
	if (verbose)
	    printf ("RotFITS: Cannot allocate %d bytes for new image\n", nbytes);
	return (NULL);
	}

    /* Mirror image without rotation */
    if (rotate < 45 && rotate > -45) {
	if (mirror == 1) {
	    for (x1 = 0; x1 < nx; x1++) {
		x2 = nx - x1 - 1;
		for (y1 = 0; y1 < ny; y1++) {
		    movepix (image,bitpix1,nx,x1,y1,rotimage,bitpix2,nx,x2,y1);
		    }
		}
	    sprintf (history,"Copy of image %s reflected",filename);
	    hputc (header,"HISTORY",history);
	    }
	else if (mirror == 2) {
	    for (y1 = 0; y1 < ny; y1++) {
		y2 = ny - y1 - 1;
		for (x1 = 0; x1 < nx; x1++) {
		    movepix (image,bitpix1,nx,x1,y1,rotimage,bitpix2,nx,x1,y2);
		    }
		}
	    sprintf (history,"Copy of image %s flipped",filename);
	    hputc (header,"HISTORY",history);
	    }
	else {
	    for (y1 = 0; y1 < ny; y1++) {
		for (x1 = 0; x1 < nx; x1++) {
		    movepix (image,bitpix1,nx,x1,y1,rotimage,bitpix2,nx,x1,y1);
		    }
		}
	    }
	}

    /* Rotate by 90 degrees */
    else if (rotate >= 45 && rotate < 135) {
	if (mirror == 1) {
	    for (y1 = 0; y1 < ny; y1++) {
		x2 = ny - y1 - 1;
		for (x1 = 0; x1 < nx; x1++) {
		    y2 = nx - x1 - 1;
		    movepix (image,bitpix1,nx,x1,y1,rotimage,bitpix2,ny,x2,y2);
		    }
		}
	    sprintf (history,"Copy of image %s reflected, rotated 90 degrees",
		     filename);
            hputc (header,"HISTORY",history);
	    }
	else if (mirror == 2) {
	    for (y1 = 0; y1 < ny; y1++) {
		for (x1 = 0; x1 < nx; x1++) {
		    movepix (image,bitpix1,nx,x1,y1,rotimage,bitpix2,ny,y1,x1);
		    }
		}
	    sprintf (history,"Copy of image %s flipped, rotated 90 degrees",
		     filename);
            hputc (header,"HISTORY",history);
	    }
	else {
	    for (y1 = 0; y1 < ny; y1++) {
		x2 = ny - y1 - 1;
		for (x1 = 0; x1 < nx; x1++) {
		    y2 = x1;
		    movepix (image,bitpix1,nx,x1,y1,rotimage,bitpix2,ny,x2,y2);
		    }
		}
	    sprintf (history,"Copy of image %s rotated 90 degrees",filename);
            hputc (header,"HISTORY",history);
	    }
	hputi4 (header,"NAXIS1",ny);
	hputi4 (header,"NAXIS2",nx);
	}

    /* Rotate by 180 degrees */
    else if (rotate >= 135 && rotate < 225) {
	if (mirror == 1) {
	    for (y1 = 0; y1 < ny; y1++) {
		y2 = ny - y1 - 1;
		for (x1 = 0; x1 < nx; x1++) {
		    movepix (image,bitpix1,nx,x1,y1,rotimage,bitpix2,nx,x1,y2);
		    }
		}
	    sprintf (history,"Copy of image %s reflected, rotated 180 degrees",
		     filename);
            hputc (header,"HISTORY",history);
	    }
	else if (mirror == 2) {
	    for (x1 = 0; x1 < nx; x1++) {
		x2 = nx - x1 - 1;
		for (y1 = 0; y1 < ny; y1++) {
		    movepix (image,bitpix1,nx,x1,y1,rotimage,bitpix2,nx,x2,y1);
		    }
		}
	    sprintf (history,"Copy of image %s flipped, rotated 180 degrees",
		     filename);
            hputc (header,"HISTORY",history);
	    }
	else {
	    for (y1 = 0; y1 < ny; y1++) {
		y2 = ny - y1 - 1;
		for (x1 = 0; x1 < nx; x1++) {
		    x2 = nx - x1 - 1;
		    movepix (image,bitpix1,nx,x1,y1,rotimage,bitpix2,nx,x2,y2);
		    }
		}
	    sprintf (history,"Copy of image %s rotated 180 degrees",filename);
            hputc (header,"HISTORY",history);
	    }
	}

    /* Rotate by 270 degrees */
    else if (rotate >= 225 && rotate < 315) {
	if (mirror == 1) {
	    for (y1 = 0; y1 < ny; y1++) {
		for (x1 = 0; x1 < nx; x1++) {
		    movepix (image,bitpix1,nx,x1,y1,rotimage,bitpix2,ny,y1,x1);
		    }
		}
	    sprintf (history,"Copy of image %s reflected, rotated 270 degrees",
		     filename);
            hputc (header,"HISTORY",history);
	    }
	else if (mirror == 2) {
	    for (y1 = 0; y1 < ny; y1++) {
		x2 = ny - y1 - 1;
		for (x1 = 0; x1 < nx; x1++) {
		    y2 = nx - x1 - 1;
		    movepix (image,bitpix1,nx,x1,y1,rotimage,bitpix2,ny,x2,y2);
		    }
		}
	    sprintf (history,"Copy of image %s flipped, rotated 270 degrees",
		     filename);
            hputc (header,"HISTORY",history);
	    }
	else {
	    for (y1 = 0; y1 < ny; y1++) {
		x2 = y1;
		for (x1 = 0; x1 < nx; x1++) {
		    y2 = nx - x1 - 1;
		    movepix (image,bitpix1,nx,x1,y1,rotimage,bitpix2,ny,x2,y2);
		    }
		}
	    sprintf (history,"Copy of image %s rotated 270 degrees",filename);
            hputc (header,"HISTORY",history);
	    }
	hputi4 (header,"NAXIS1",ny);
	hputi4 (header,"NAXIS2",nx);
	}

    /* If rotating by more than 315 degrees, assume top-bottom reflection */
    else if (rotate >= 315 && mirror) {
	for (y1 = 0; y1 < ny; y1++) {
	    for (x1 = 0; x1 < nx; x1++) {
		x2 = y1;
		y2 = x1;
		movepix (image,bitpix1,nx,x1,y1,rotimage,bitpix2,ny,x2,y2);
		}
	    }
	sprintf (history,"Copy of image %s reflected top to bottom",filename);
        hputc (header,"HISTORY",history);
	}
    
    if (verbose)
	fprintf (stderr,"%s\n",history);

    return (rotimage);
}


/* rotate all the C* fields.
 * return 0 if at least one such field is found, else -1.  */

static void
RotWCSFITS (header, angle, mirror, verbose)

char	*header;	/* FITS header */
int	angle;		/* Angle to be rotated (0, 90, 180, 270) */
int	mirror;		/* 1 if mirrored left to right, else 0 */
int	verbose;	/* Print progress if 1 */

{
    static char flds[15][8];
    char ctype1[16], ctype2[16];
    double ctemp1, ctemp2, ctemp3, ctemp4, naxis1, naxis2;
    int i, n, ndec1, ndec2, ndec3, ndec4;

    strcpy (flds[0], "CTYPE1");
    strcpy (flds[1], "CTYPE2");
    strcpy (flds[2], "CRVAL1");
    strcpy (flds[3], "CRVAL2");
    strcpy (flds[4], "CDELT1");
    strcpy (flds[5], "CDELT2");
    strcpy (flds[6], "CRPIX1");
    strcpy (flds[7], "CRPIX2");
    strcpy (flds[8], "CROTA1");
    strcpy (flds[9], "CROTA2");
    strcpy (flds[10], "IMWCS");
    strcpy (flds[11], "CD1_1");
    strcpy (flds[12], "CD1_2");
    strcpy (flds[13], "CD2_1");
    strcpy (flds[14], "CD2_2");

    n = 0;
    hgetr8 (header, "NAXIS1", &naxis1);
    hgetr8 (header, "NAXIS2", &naxis2);

    /* Find out if there any WCS keywords in this header */
    for (i = 0; i < sizeof(flds)/sizeof(flds[0]); i++) {
	if (ksearch (header, flds[i]) != NULL) {
	    n++;
	    if (verbose)
		fprintf (stderr,"%s: found\n", flds[i]);
	    }
	}

    /* Return if no WCS keywords to change */
    if (n == 0) {
	if (verbose)
	    fprintf (stderr,"RotWCSFITS: No WCS in header\n");
	return;
	}

    /* Reset CROTAn and CD matrix if axes have been exchanged */
    if (angle == 90) {
	if (hgetr8 (header, "CROTA1", &ctemp1)) {
	    hgetndec (header, "CROTA1", &ndec1);
	    hputnr8 (header, "CROTA1", ndec1, ctemp1+90.0);
	    }
	if (hgetr8 (header, "CROTA2", &ctemp2)) {
	    hgetndec (header, "CROTA2", &ndec2);
	    hputnr8 (header, "CROTA2", ndec2, ctemp2+90.0);
	    }
	}

    /* Negate rotation angle if mirrored */
    if (mirror) {
	if (hgetr8 (header, "CROTA1", &ctemp1)) {
	    hgetndec (header, "CROTA1", &ndec1);
	    hputnr8 (header, "CROTA1", ndec1, -ctemp1);
	    }
	if (hgetr8 (header, "CROTA2", &ctemp2)) {
	    hgetndec (header, "CROTA2", &ndec2);
	    hputnr8 (header, "CROTA2", ndec2, -ctemp2);
	    }
	if (hgetr8 (header, "LTM1_1", &ctemp1)) {
	    hgetndec (header, "LTM1_1", &ndec1);
	    hputnr8 (header, "LTM1_1", ndec1, -ctemp1);
	    }
	if (hgetr8 (header, "CD1_1", &ctemp1))
	    hputr8 (header, "CD1_1", -ctemp1);
	if (hgetr8 (header, "CD1_2", &ctemp1))
	    hputr8 (header, "CD1_2", -ctemp1);
	if (hgetr8 (header, "CD2_1", &ctemp1))
	    hputr8 (header, "CD2_1", -ctemp1);
	}

    /* Unbin CRPIX and CD matrix */
    if (hgetr8 (header, "LTM1_1", &ctemp1)) {
	if (ctemp1 != 1.0) {
	    if (hgetr8 (header, "LTM2_2", &ctemp2)) {
		if (ctemp1 == ctemp2) {
		    double ltv1 = 0.0;
		    double ltv2 = 0.0;
		    if (hgetr8 (header, "LTV1", &ltv1))
			hdel (header, "LTV1");
		    if (hgetr8 (header, "LTV2", &ltv1))
			hdel (header, "LTV2");
		    if (hgetr8 (header, "CRPIX1", &ctemp3))
			hputr8 (header, "CRPIX1", (ctemp3-ltv1)/ctemp1);
		    if (hgetr8 (header, "CRPIX2", &ctemp3))
			hputr8 (header, "CRPIX2", (ctemp3-ltv2)/ctemp1);
		    if (hgetr8 (header, "CD1_1", &ctemp3))
			hputr8 (header, "CD1_1", ctemp3/ctemp1);
		    if (hgetr8 (header, "CD1_2", &ctemp3))
			hputr8 (header, "CD1_2", ctemp3/ctemp1);
		    if (hgetr8 (header, "CD2_1", &ctemp3))
			hputr8 (header, "CD2_1", ctemp3/ctemp1);
		    if (hgetr8 (header, "CD2_2", &ctemp3))
			hputr8 (header, "CD2_2", ctemp3/ctemp1);
		    hdel (header, "LTM1_1");
		    hdel (header, "LTM2_2");
		    }
		}
	    }
	}

    /* Reset CRPIXn */
    if (hgetr8 (header, "CRPIX1", &ctemp1) &&
	hgetr8 (header, "CRPIX2", &ctemp2)) { 
	hgetndec (header, "CRPIX1", &ndec1);
	hgetndec (header, "CRPIX2", &ndec2);
	if (mirror) {
	    if (angle == 0)
		hputnr8 (header, "CRPIX1", ndec1, naxis1-ctemp1);
	    else if (angle == 90) {
		hputnr8 (header, "CRPIX1", ndec2, naxis2-ctemp2);
		hputnr8 (header, "CRPIX2", ndec1, naxis1-ctemp1);
		}
	    else if (angle == 180) {
		hputnr8 (header, "CRPIX1", ndec1, ctemp1);
		hputnr8 (header, "CRPIX2", ndec2, naxis2-ctemp2);
		}
	    else if (angle == 270) {
		hputnr8 (header, "CRPIX1", ndec2, ctemp2);
		hputnr8 (header, "CRPIX2", ndec1, ctemp1);
		}
	    }
	else {
	    if (angle == 90) {
		hputnr8 (header, "CRPIX1", ndec2, naxis2-ctemp2);
		hputnr8 (header, "CRPIX2", ndec1, ctemp1);
		}
	    else if (angle == 180) {
		hputnr8 (header, "CRPIX1", ndec1, naxis1-ctemp1);
		hputnr8 (header, "CRPIX2", ndec2, naxis2-ctemp2);
		}
	    else if (angle == 270) {
		hputnr8 (header, "CRPIX1", ndec2, ctemp2);
		hputnr8 (header, "CRPIX2", ndec1, naxis1-ctemp1);
		}
	    }
	}

    /* Reset CDELTn (degrees per pixel) */
    if (hgetr8 (header, "CDELT1", &ctemp1) &&
	hgetr8 (header, "CDELT2", &ctemp2)) { 
	hgetndec (header, "CDELT1", &ndec1);
	hgetndec (header, "CDELT2", &ndec2);
	if (mirror) {
	    if (angle == 0)
		hputnr8 (header, "CDELT1", ndec1, -ctemp1);
	    else if (angle == 90) {
		hputnr8 (header, "CDELT1", ndec2, -ctemp2);
		hputnr8 (header, "CDELT2", ndec1, -ctemp1);
		}
	    else if (angle == 180) {
		hputnr8 (header, "CDELT1", ndec1, ctemp1);
		hputnr8 (header, "CDELT2", ndec2, -ctemp2);
		}
	    else if (angle == 270) {
		hputnr8 (header, "CDELT1", ndec2, ctemp2);
		hputnr8 (header, "CDELT2", ndec1, ctemp1);
		}
	    }
	else {
	    if (angle == 90) {
		hputnr8 (header, "CDELT1", ndec2, -ctemp2);
		hputnr8 (header, "CDELT2", ndec1, ctemp1);
		}
	    else if (angle == 180) {
		hputnr8 (header, "CDELT1", ndec1, -ctemp1);
		hputnr8 (header, "CDELT2", ndec2, -ctemp2);
		}
	    else if (angle == 270) {
		hputnr8 (header, "CDELT1", ndec2, ctemp2);
		hputnr8 (header, "CDELT2", ndec1, -ctemp1);
		}
	    }
	}

    /* Reset CD matrix, if present */
    ctemp1 = 0.0;
    ctemp2 = 0.0;
    ctemp3 = 0.0;
    ctemp4 = 0.0;
    if (hgetr8 (header, "CD1_1", &ctemp1)) {
	hgetr8 (header, "CD1_2", &ctemp2);
	hgetr8 (header, "CD2_1", &ctemp3);
	hgetr8 (header, "CD2_2", &ctemp4);
	hgetndec (header, "CD1_1", &ndec1);
	hgetndec (header, "CD1_2", &ndec2);
	hgetndec (header, "CD2_1", &ndec3);
	hgetndec (header, "CD2_2", &ndec4);
	if (mirror) {
	    if (angle == 0) {
		hputnr8 (header, "CD1_2", ndec2, -ctemp2);
		hputnr8 (header, "CD2_1", ndec3, -ctemp3);
		}
	    else if (angle == 90) {
		hputnr8 (header, "CD1_1", ndec4, -ctemp4);
		hputnr8 (header, "CD1_2", ndec3, -ctemp3);
		hputnr8 (header, "CD2_1", ndec2, -ctemp2);
		hputnr8 (header, "CD2_2", ndec1, -ctemp1);
		}
	    else if (angle == 180) {
		hputnr8 (header, "CD1_1", ndec1, ctemp1);
		hputnr8 (header, "CD1_2", ndec2, ctemp2);
		hputnr8 (header, "CD2_1", ndec3, -ctemp3);
		hputnr8 (header, "CD2_2", ndec4, -ctemp4);
		}
	    else if (angle == 270) {
		hputnr8 (header, "CD1_1", ndec4, ctemp4);
		hputnr8 (header, "CD1_2", ndec3, ctemp3);
		hputnr8 (header, "CD2_1", ndec2, ctemp2);
		hputnr8 (header, "CD2_2", ndec1, ctemp1);
		}
	    }
	else {
	    if (angle == 90) {
		hputnr8 (header, "CD1_1", ndec4, -ctemp4);
		hputnr8 (header, "CD1_2", ndec3, -ctemp3);
		hputnr8 (header, "CD2_1", ndec2, ctemp2);
		hputnr8 (header, "CD2_2", ndec1, ctemp1);
		}
	    else if (angle == 180) {
		hputnr8 (header, "CD1_1", ndec1, -ctemp1);
		hputnr8 (header, "CD1_2", ndec2, -ctemp2);
		hputnr8 (header, "CD2_1", ndec3, -ctemp3);
		hputnr8 (header, "CD2_2", ndec4, -ctemp4);
		}
	    else if (angle == 270) {
		hputnr8 (header, "CD1_1", ndec4, ctemp4);
		hputnr8 (header, "CD1_2", ndec3, ctemp3);
		hputnr8 (header, "CD2_1", ndec2, -ctemp2);
		hputnr8 (header, "CD2_2", ndec1, -ctemp1);
		}
	    }
	}

    /* Delete any polynomial solution */
    /* (These could maybe be switched, but I don't want to work them out yet */
    if (ksearch (header, "CO1_1")) {
	int i;
	char keyword[16];

	for (i = 1; i < 13; i++) {
	    sprintf (keyword,"CO1_%d", i);
	    hdel (header, keyword);
	    }
	for (i = 1; i < 13; i++) {
	    sprintf (keyword,"CO2_%d", i);
	    hdel (header, keyword);
	    }
	}

    return;
}

/* May 29 1996	Change name from rotFITS to RotFITS
 * Jun  4 1996	Fix bug when handling assymetrical images
 * Jun  5 1996	Print filename, not pathname, in history
 * Jun 10 1996	Remove unused variables after running lint
 * Jun 13 1996	Replace image with rotated image
 * Jun 18 1996	Fix formatting bug in history
 *
 * Jul 11 1997	If rotation is 360, flip top bottom if mirror flat is set
 *
 * Feb 23 1998	Do not delete WCS if image not rotated or mirrored
 * May 26 1998	Rotate WCS instead of deleting it
 * May 27 1998	Include imio.h

 * Jun  8 1999	Return new image pointer instead of flag; do not free old image
 * Jun  9 1999	Make history buffer 128 instead of 72 to avoid overflows
 * Jun 10 1999	Drop image0; use image
 * Oct 21 1999	Fix hputnr8() calls after lint
 *
 * Jan 11 2001	Print all messages to stderr
 * Jan 17 2001	Reset coordinate direction if image is mirrored
 * Jan 18 2001	Reset WCS scale if image is binned
 * Nov 27 2001	Add error messages for all null returns
 * Nov 27 2001	Add bitpix=8
 *
 * Jan 28 2004	Add xshift and yshift arguments to shift image
 * Sep 15 2004	Fix bugs in calls to hgetr8 for crpix (found by Rob Creager)
 *
 * Aug 17 2005	Add mirror = 2 flag indicating a flip across x axis
 *
 * Jun 26 2008	Shift pixels if either xshift or yshift is not zero
 */
