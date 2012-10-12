/*** File saoimage/wcslib/dsspos.c
 *** October 21, 1999
 *** By Doug Mink, dmink@cfa.harvard.edu
 *** Harvard-Smithsonian Center for Astrophysics
 *** Copyright (C) 1995-2002
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

 * Module:	dsspos.c (Plate solution WCS conversion)
 * Purpose:	Compute WCS from Digital Sky Survey plate fit
 * Subroutine:	dsspos() converts from pixel location to RA,Dec 
 * Subroutine:	dsspix() converts from RA,Dec to pixel location   

    These functions are based on the astrmcal.c portion of GETIMAGE by
    J. Doggett and the documentation distributed with the Digital Sky Survey.

*/

#include <math.h>
#include <string.h>
#include <stdio.h>
#include "wcs.h"

int
dsspos (xpix, ypix, wcs, xpos, ypos)

/* Routine to determine accurate position for pixel coordinates */
/* returns 0 if successful otherwise 1 = angle too large for projection; */
/* based on amdpos() from getimage */

/* Input: */
double	xpix;		/* x pixel number  (RA or long without rotation) */
double	ypix;		/* y pixel number  (dec or lat without rotation) */
struct WorldCoor *wcs;	/* WCS parameter structure */

/* Output: */
double	*xpos;		/* Right ascension or longitude in degrees */
double	*ypos;		/* Declination or latitude in degrees */

{
  double x, y, xmm, ymm, xmm2, ymm2, xmm3, ymm3, x2y2;
  double xi, xir, eta, etar, raoff, ra, dec;
  double cond2r = 1.745329252e-2;
  double cons2r = 206264.8062470964;
  double twopi = 6.28318530717959;
  double ctan, ccos;

/*  Ignore magnitude and color terms 
  double mag = 0.0;
  double color = 0.0; */

/* Convert from image pixels to plate pixels */
  x = xpix + wcs->x_pixel_offset - 1.0 + 0.5;
  y = ypix + wcs->y_pixel_offset - 1.0 + 0.5;

/* Convert from pixels to millimeters */
  xmm = (wcs->ppo_coeff[2] - x * wcs->x_pixel_size) / 1000.0;
  ymm = (y * wcs->y_pixel_size - wcs->ppo_coeff[5]) / 1000.0;
  xmm2 = xmm * xmm;
  ymm2 = ymm * ymm;
  xmm3 = xmm * xmm2;
  ymm3 = ymm * ymm2;
  x2y2 = xmm2 + ymm2;

/*  Compute coordinates from x,y and plate model */

  xi =  wcs->x_coeff[ 0]*xmm	+ wcs->x_coeff[ 1]*ymm +
	wcs->x_coeff[ 2]		+ wcs->x_coeff[ 3]*xmm2 +
	wcs->x_coeff[ 4]*xmm*ymm	+ wcs->x_coeff[ 5]*ymm2 +
	wcs->x_coeff[ 6]*(x2y2)	+ wcs->x_coeff[ 7]*xmm3 +
	wcs->x_coeff[ 8]*xmm2*ymm	+ wcs->x_coeff[ 9]*xmm*ymm2 +
	wcs->x_coeff[10]*ymm3	+ wcs->x_coeff[11]*xmm*(x2y2) +
	wcs->x_coeff[12]*xmm*x2y2*x2y2;

/*  Ignore magnitude and color terms 
	+ wcs->x_coeff[13]*mag	+ wcs->x_coeff[14]*mag*mag +
	wcs->x_coeff[15]*mag*mag*mag + wcs->x_coeff[16]*mag*xmm +
	wcs->x_coeff[17]*mag*x2y2	+ wcs->x_coeff[18]*mag*xmm*x2y2 +
	wcs->x_coeff[19]*color; */

  eta =	wcs->y_coeff[ 0]*ymm	+ wcs->y_coeff[ 1]*xmm +
	wcs->y_coeff[ 2]		+ wcs->y_coeff[ 3]*ymm2 +
	wcs->y_coeff[ 4]*xmm*ymm	+ wcs->y_coeff[ 5]*xmm2 +
	wcs->y_coeff[ 6]*(x2y2)	+ wcs->y_coeff[ 7]*ymm3 +
	wcs->y_coeff[ 8]*ymm2*xmm	+ wcs->y_coeff[ 9]*ymm*xmm2 +
	wcs->y_coeff[10]*xmm3	+ wcs->y_coeff[11]*ymm*(x2y2) +
	wcs->y_coeff[12]*ymm*x2y2*x2y2;

/*  Ignore magnitude and color terms 
	+ wcs->y_coeff[13]*mag	+ wcs->y_coeff[14]*mag*mag +
	wcs->y_coeff[15]*mag*mag*mag + wcs->y_coeff[16]*mag*ymm +
	wcs->y_coeff[17]*mag*x2y2)	+ wcs->y_coeff[18]*mag*ymm*x2y2 +
	wcs->y_coeff[19]*color; */

/* Convert to radians */

  xir = xi / cons2r;
  etar = eta / cons2r;

/* Convert to RA and Dec */

  ctan = tan (wcs->plate_dec);
  ccos = cos (wcs->plate_dec);
  raoff = atan2 (xir / ccos, 1.0 - etar * ctan);
  ra = raoff + wcs->plate_ra;
  if (ra < 0.0) ra = ra + twopi;
  *xpos = ra / cond2r;

  dec = atan (cos (raoff) * ((etar + ctan) / (1.0 - (etar * ctan))));
  *ypos = dec / cond2r;
  return 0;
}


int
dsspix (xpos, ypos, wcs, xpix, ypix)

/* Routine to determine pixel coordinates for sky position */
/* returns 0 if successful otherwise 1 = angle too large for projection; */
/* based on amdinv() from getimage */

/* Input: */
double	xpos;		/* Right ascension or longitude in degrees */
double	ypos;		/* Declination or latitude in degrees */
struct WorldCoor *wcs;	/* WCS parameter structure */

/* Output: */
double	*xpix;		/* x pixel number  (RA or long without rotation) */
double	*ypix;		/* y pixel number  (dec or lat without rotation) */

{
  double div,xi,eta,x,y,xy,x2,y2,x2y,y2x,x3,y3,x4,y4,x2y2,cjunk,dx,dy;
  double sypos,cypos,syplate,cyplate,sxdiff,cxdiff;
  double f,fx,fy,g,gx,gy, xmm, ymm;
  double conr2s = 206264.8062470964;
  double tolerance = 0.0000005;
  int    max_iterations = 50;
  int    i;
  double xr, yr; 	/* position in radians */

  *xpix = 0.0;
  *ypix = 0.0;

/* Convert RA and Dec in radians to standard coordinates on a plate */
  xr = degrad (xpos);
  yr = degrad (ypos);
  sypos = sin (yr);
  cypos = cos (yr);
  if (wcs->plate_dec == 0.0)
    wcs->plate_dec = degrad (wcs->yref);
  syplate = sin (wcs->plate_dec);
  cyplate = cos (wcs->plate_dec);
  if (wcs->plate_ra == 0.0)
    wcs->plate_ra = degrad (wcs->yref);
  sxdiff = sin (xr - wcs->plate_ra);
  cxdiff = cos (xr - wcs->plate_ra);
  div = (sypos * syplate) + (cypos * cyplate * cxdiff);
  if (div == 0.0)
    return (1);
  xi = cypos * sxdiff * conr2s / div;
  eta = ((sypos * cyplate) - (cypos * syplate * cxdiff)) * conr2s / div;

/* Set initial value for x,y */
  if (wcs->plate_scale == 0.0)
    return (1);
  xmm = xi / wcs->plate_scale;
  ymm = eta / wcs->plate_scale;

/* Iterate by Newton's method */
  for (i = 0; i < max_iterations; i++) {

    /* X plate model */
    xy = xmm * ymm;
    x2 = xmm * xmm;
    y2 = ymm * ymm;
    x2y = x2 * ymm;
    y2x = y2 * xmm;
    x2y2 = x2 + y2;
    cjunk = x2y2 * x2y2;
    x3 = x2 * xmm;
    y3 = y2 * ymm;
    x4 = x2 * x2;
    y4 = y2 * y2;
    f = wcs->x_coeff[0]*xmm      + wcs->x_coeff[1]*ymm +
        wcs->x_coeff[2]          + wcs->x_coeff[3]*x2 +
        wcs->x_coeff[4]*xy       + wcs->x_coeff[5]*y2 +
        wcs->x_coeff[6]*x2y2     + wcs->x_coeff[7]*x3 +
        wcs->x_coeff[8]*x2y      + wcs->x_coeff[9]*y2x +
        wcs->x_coeff[10]*y3      + wcs->x_coeff[11]*xmm*x2y2 +
        wcs->x_coeff[12]*xmm*cjunk;
    /* magnitude and color terms ignored
      + wcs->x_coeff[13]*mag +
        wcs->x_coeff[14]*mag*mag   + wcs->x_coeff[15]*mag*mag*mag +
        wcs->x_coeff[16]*mag*xmm   + wcs->x_coeff[17]*mag*(x2+y2) +
        wcs->x_coeff[18]*mag*xmm*(x2+y2)  + wcs->x_coeff[19]*color;
    */

    /*  Derivative of X model wrt x */
    fx = wcs->x_coeff[0]           + wcs->x_coeff[3]*2.0*xmm +
         wcs->x_coeff[4]*ymm       + wcs->x_coeff[6]*2.0*xmm +
         wcs->x_coeff[7]*3.0*x2    + wcs->x_coeff[8]*2.0*xy +
         wcs->x_coeff[9]*y2        + wcs->x_coeff[11]*(3.0*x2+y2) +
         wcs->x_coeff[12]*(5.0*x4 +6.0*x2*y2+y4);
    /* magnitude and color terms ignored
         wcs->x_coeff[16]*mag      + wcs->x_coeff[17]*mag*2.0*xmm +
         wcs->x_coeff[18]*mag*(3.0*x2+y2);
    */

    /* Derivative of X model wrt y */
    fy = wcs->x_coeff[1]           + wcs->x_coeff[4]*xmm +
         wcs->x_coeff[5]*2.0*ymm   + wcs->x_coeff[6]*2.0*ymm +
         wcs->x_coeff[8]*x2        + wcs->x_coeff[9]*2.0*xy +
         wcs->x_coeff[10]*3.0*y2   + wcs->x_coeff[11]*2.0*xy +
         wcs->x_coeff[12]*4.0*xy*x2y2;
    /* magnitude and color terms ignored
         wcs->x_coeff[17]*mag*2.0*ymm +
         wcs->x_coeff[18]*mag*2.0*xy;
    */

    /* Y plate model */
    g = wcs->y_coeff[0]*ymm       + wcs->y_coeff[1]*xmm +
       wcs->y_coeff[2]            + wcs->y_coeff[3]*y2 +
       wcs->y_coeff[4]*xy         + wcs->y_coeff[5]*x2 +
       wcs->y_coeff[6]*x2y2       + wcs->y_coeff[7]*y3 +
       wcs->y_coeff[8]*y2x        + wcs->y_coeff[9]*x2y +
       wcs->y_coeff[10]*x3        + wcs->y_coeff[11]*ymm*x2y2 +
       wcs->y_coeff[12]*ymm*cjunk;
    /* magnitude and color terms ignored
       wcs->y_coeff[13]*mag        + wcs->y_coeff[14]*mag*mag +
       wcs->y_coeff[15]*mag*mag*mag + wcs->y_coeff[16]*mag*ymm +
       wcs->y_coeff[17]*mag*x2y2 +
       wcs->y_coeff[18]*mag*ymm*x2y2 + wcs->y_coeff[19]*color;
    */

    /* Derivative of Y model wrt x */
    gx = wcs->y_coeff[1]           + wcs->y_coeff[4]*ymm +
         wcs->y_coeff[5]*2.0*xmm   + wcs->y_coeff[6]*2.0*xmm +
         wcs->y_coeff[8]*y2       + wcs->y_coeff[9]*2.0*xy +
         wcs->y_coeff[10]*3.0*x2  + wcs->y_coeff[11]*2.0*xy +
         wcs->y_coeff[12]*4.0*xy*x2y2;
    /* magnitude and color terms ignored
         wcs->y_coeff[17]*mag*2.0*xmm +
         wcs->y_coeff[18]*mag*ymm*2.0*xmm;
    */

    /* Derivative of Y model wrt y */
    gy = wcs->y_coeff[0]            + wcs->y_coeff[3]*2.0*ymm +
         wcs->y_coeff[4]*xmm        + wcs->y_coeff[6]*2.0*ymm +
         wcs->y_coeff[7]*3.0*y2     + wcs->y_coeff[8]*2.0*xy +
         wcs->y_coeff[9]*x2         + wcs->y_coeff[11]*(x2+3.0*y2) +
         wcs->y_coeff[12]*(5.0*y4 + 6.0*x2*y2 + x4);
    /* magnitude and color terms ignored
         wcs->y_coeff[16]*mag       + wcs->y_coeff[17]*mag*2.0*ymm +
         wcs->y_coeff[18]*mag*(x2+3.0*y2);
    */

    f = f - xi;
    g = g - eta;
    dx = ((-f * gy) + (g * fy)) / ((fx * gy) - (fy * gx));
    dy = ((-g * fx) + (f * gx)) / ((fx * gy) - (fy * gx));
    xmm = xmm + dx;
    ymm = ymm + dy;
    if ((fabs(dx) < tolerance) && (fabs(dy) < tolerance)) break;
    }

/* Convert mm from plate center to plate pixels */
  if (wcs->x_pixel_size == 0.0 || wcs->y_pixel_size == 0.0)
    return (1);
  x = (wcs->ppo_coeff[2] - xmm*1000.0) / wcs->x_pixel_size;
  y = (wcs->ppo_coeff[5] + ymm*1000.0) / wcs->y_pixel_size;

/* Convert from plate pixels to image pixels */
  *xpix = x - wcs->x_pixel_offset + 1.0 - 0.5;
  *ypix = y - wcs->y_pixel_offset + 1.0 - 0.5;

/* If position is off of the image, return offscale code */
  if (*xpix < 0.5 || *xpix > wcs->nxpix+0.5)
    return -1;
  if (*ypix < 0.5 || *ypix > wcs->nypix+0.5)
    return -1;

  return 0;
}
/* Mar  6 1995	Original version of this code
 * May  4 1995	Fix eta cross terms which were all in y
 * Jun 21 1995	Add inverse routine
 * Oct 17 1995	Fix inverse routine (degrees -> radians)
 * Nov  7 1995	Add half pixel to image coordinates to get astrometric
 *                plate coordinates
 * Feb 26 1996	Fix plate to image pixel conversion error
 *
 * Mar 23 1998	Change names from plate*() to dss*()
 * Apr  7 1998	Change amd_i_coeff to i_coeff
 * Sep  4 1998	Fix possible divide by zero in dsspos() from Allen Harris, SAO
 * Sep 10 1998	Fix possible divide by zero in dsspix() from Allen Harris, SAO
 *
 * Oct 21 1999	Drop declaration of cond2r in dsspix()
 */
