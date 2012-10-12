/*** File caphot.c
 *** January 30, 2002
 *** By Doug Mink from Fortran code by Sam Conner (MIT, 1984)
 *** Copyright (C) 2002
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

#include <math.h>
#include "fitshead.h"
#include "imio.h"

static double apint();
static double imapfr();

/* CAPHOT -- Perform rigorous circular aperture photometry using the imapfr()
 *	     subroutine to determine the fraction of a square pixel enclosed
 *	     by a circular aperture.
 */

void
caphot (imbuff,header,cx,cy,rad,sumw,wsum)

char	*imbuff;		/* address of start of image buffer */
char	*header;	/* image FITS header */
double	cx,cy;		/* center x,y of aperture */
double	rad;		/* radius of aperture */
double	sumw;		/* sum of values of pixel weights (returned) */
double	wsum;		/* sum of weighted pixel fluxes (returned) */
{
    double x, y, factor, flux, bs, bz;
    int ix1, ix2, iy1, iy2, ix, iy, bitpix, nx, ny;

    sumw = 0.0;
    wsum = 0.0;
    hgeti4 (header, "BITPIX", &bitpix);
    hgeti4 (header, "NAXIS1", &nx);
    hgeti4 (header, "NAXIS2", &ny);
    hgetr8 (header, "BSCALE", &bs);
    hgeti4 (header, "BZERO", &bz);

    /* Find range of ys to check */
    iy1 = (int) (cy - rad);
    iy2 = (int) (cy + rad + 0.99999);
    if (iy1 < 1)
	iy1 = 1;
    if (iy2 > ny)
	iy2 = ny;

    /* Find range of xumns to check */
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
	    sumw = sumw + factor;
	    flux = getpix1 (imbuff, bitpix, nx, ny, bz, bs, ix, iy);
	    wsum = wsum + (factor * flux);

	    /* fprintf (stderr, "IMAPSB: (%d,%d)= %f weight= %f\n",
		    ix,iy,flux,factor); */
	    }
	}

 /* fprintf (stderr, "IMAPSB: sum of weights = %f\n", sumw);
    fprintf (stderr, "IMAPSB: sum of weighted intensity = %f\n", wsum);
 */

    return;
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

/* Jul  4 1984	Original Fortran program by Sam Conner at MIT
 * Mar  2 1987	Original VAX Unix version
 * Mar  2 1987	declare undecldared variables x, y, and j
 *
 * Jan  8 1993	Modify to handle arbitrary byte-per-pixel images
 * Apr 16 1993	Declare undeclared variables iy, ic0, and ir0
 *
 * Jan 30 2002	Translate from Fortran to C
 */
