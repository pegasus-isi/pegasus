/*  worldpos.c -- WCS Algorithms from Classic AIPS.
 *  June 20, 2006
 *  Copyright (C) 1994-2006
 *  Associated Universities, Inc. Washington DC, USA.
 *  With code added by Doug Mink, Smithsonian Astrophysical Observatory
 *                 and Allan Brighton and Andreas Wicenec, ESO

 * Module:	worldpos.c
 * Purpose:	Perform forward and reverse WCS computations for 8 projections
 * Subroutine:	worldpos() converts from pixel location to RA,Dec 
 * Subroutine:	worldpix() converts from RA,Dec         to pixel location   

    -=-=-=-=-=-=-

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
   
    Correspondence concerning AIPS should be addressed as follows:
	   Internet email: aipsmail@nrao.edu
	   Postal address: AIPS Group
	                   National Radio Astronomy Observatory
	                   520 Edgemont Road
	                   Charlottesville, VA 22903-2475 USA

    -=-=-=-=-=-=-

    These two ANSI C functions, worldpos() and worldpix(), perform
    forward and reverse WCS computations for 8 types of projective
    geometries ("-SIN", "-TAN", "-ARC", "-NCP", "-GLS" or "-SFL", "-MER",
     "-AIT", "-STG", "CAR", and "COE"):

	worldpos() converts from pixel location to RA,Dec 
	worldpix() converts from RA,Dec         to pixel location   

    where "(RA,Dec)" are more generically (long,lat). These functions
    are based on the WCS implementation of Classic AIPS, an
    implementation which has been in production use for more than ten
    years. See the two memos by Eric Greisen

	ftp://fits.cv.nrao.edu/fits/documents/wcs/aips27.ps.Z
	ftp://fits.cv.nrao.edu/fits/documents/wcs/aips46.ps.Z

    for descriptions of the 8 projective geometries and the
    algorithms.  Footnotes in these two documents describe the
    differences between these algorithms and the 1993-94 WCS draft
    proposal (see URL below). In particular, these algorithms support
    ordinary field rotation, but not skew geometries (CD or PC matrix
    cases). Also, the MER and AIT algorithms work correctly only for
    CRVALi=(0,0). Users should note that GLS projections with yref!=0
    will behave differently in this code than in the draft WCS
    proposal.  The NCP projection is now obsolete (it is a special
    case of SIN).  WCS syntax and semantics for various advanced
    features is discussed in the draft WCS proposal by Greisen and
    Calabretta at:
    
	ftp://fits.cv.nrao.edu/fits/documents/wcs/wcs.all.ps.Z
    
	        -=-=-=-

    The original version of this code was Emailed to D.Wells on
    Friday, 23 September by Bill Cotton <bcotton@gorilla.cv.nrao.edu>,
    who described it as a "..more or less.. exact translation from the
    AIPSish..". Changes were made by Don Wells <dwells@nrao.edu>
    during the period October 11-13, 1994:
    1) added GNU license and header comments
    2) added testpos.c program to perform extensive circularity tests
    3) changed float-->double to get more than 7 significant figures
    4) testpos.c circularity test failed on MER and AIT. B.Cotton
       found that "..there were a couple of lines of code [in] the wrong
       place as a result of merging several Fortran routines." 
    5) testpos.c found 0h wraparound in worldpix() and worldpos().
    6) E.Greisen recommended removal of various redundant if-statements,
       and addition of a 360d difference test to MER case of worldpos(). 
    7) D.Mink changed input to data structure and implemented rotation matrix.
*/
#include <math.h>
#include <string.h>
#include <stdio.h>
#include "wcs.h"

int
worldpos (xpix, ypix, wcs, xpos, ypos)

/* Routine to determine accurate position for pixel coordinates */
/* returns 0 if successful otherwise 1 = angle too large for projection; */
/* does: -SIN, -TAN, -ARC, -NCP, -GLS or -SFL, -MER, -AIT projections */
/* anything else is linear */

/* Input: */
double	xpix;		/* x pixel number  (RA or long without rotation) */
double	ypix;		/* y pixel number  (Dec or lat without rotation) */
struct WorldCoor *wcs;		/* WCS parameter structure */

/* Output: */
double	*xpos;		/* x (RA) coordinate (deg) */
double	*ypos;		/* y (dec) coordinate (deg) */

{
  double cosr, sinr, dx, dy, dz, tx;
  double sins, coss, dt, l, m, mg, da, dd, cos0, sin0;
  double rat = 0.0;
  double dect = 0.0;
  double mt, a, y0, td, r2;  /* allan: for COE */
  double dec0, ra0, decout, raout;
  double geo1, geo2, geo3;
  double cond2r=1.745329252e-2;
  double twopi = 6.28318530717959;
  double deps = 1.0e-5;

  /* Structure elements */
  double xref;		/* X reference coordinate value (deg) */
  double yref;		/* Y reference coordinate value (deg) */
  double xrefpix;	/* X reference pixel */
  double yrefpix;	/* Y reference pixel */
  double xinc;		/* X coordinate increment (deg) */
  double yinc;		/* Y coordinate increment (deg) */
  double rot;		/* Optical axis rotation (deg)  (N through E) */
  int itype = wcs->prjcode;

  /* Set local projection parameters */
  xref = wcs->xref;
  yref = wcs->yref;
  xrefpix = wcs->xrefpix;
  yrefpix = wcs->yrefpix;
  xinc = wcs->xinc;
  yinc = wcs->yinc;
  rot = degrad (wcs->rot);
  cosr = cos (rot);
  sinr = sin (rot);

  /* Offset from ref pixel */
  dx = xpix - xrefpix;
  dy = ypix - yrefpix;

  /* Scale and rotate using CD matrix */
  if (wcs->rotmat) {
    tx = dx * wcs->cd[0] + dy * wcs->cd[1];
    dy = dx * wcs->cd[2] + dy * wcs->cd[3];
    dx = tx;
    }

  /* Scale and rotate using CDELTn and CROTA2 */
  else {

    /* Check axis increments - bail out if either 0 */
    if ((xinc==0.0) || (yinc==0.0)) {
      *xpos=0.0;
      *ypos=0.0;
      return 2;
      }

    /* Scale using CDELT */
    dx = dx * xinc;
    dy = dy * yinc;

    /* Take out rotation from CROTA */
    if (rot != 0.0) {
      tx = dx * cosr - dy * sinr;
      dy = dx * sinr + dy * cosr;
      dx = tx;
      }
    }

  /* Flip coordinates if necessary */
  if (wcs->coorflip) {
    tx = dx;
    dx = dy;
    dy = tx;
    }

  /* Default, linear result for error or pixel return  */
  *xpos = xref + dx;
  *ypos = yref + dy;
  if (itype <= 0)
    return 0;

  /* Convert to radians  */
  if (wcs->coorflip) {
    dec0 = degrad (xref);
    ra0 = degrad (yref);
    }
  else {
    ra0 = degrad (xref);
    dec0 = degrad (yref);
    }
  l = degrad (dx);
  m = degrad (dy);
  sins = l*l + m*m;
  decout = 0.0;
  raout = 0.0;
  cos0 = cos (dec0);
  sin0 = sin (dec0);

  /* Process by case  */
  switch (itype) {

    case WCS_CAR:   /* -CAR Cartesian (was WCS_PIX pixel and WCS_LIN linear) */
      rat =  ra0 + l;
      dect = dec0 + m;
      break;

    case WCS_SIN: /* -SIN sin*/ 
      if (sins>1.0) return 1;
      coss = sqrt (1.0 - sins);
      dt = sin0 * coss + cos0 * m;
      if ((dt>1.0) || (dt<-1.0)) return 1;
      dect = asin (dt);
      rat = cos0 * coss - sin0 * m;
      if ((rat==0.0) && (l==0.0)) return 1;
      rat = atan2 (l, rat) + ra0;
      break;

    case WCS_TAN:   /* -TAN tan */
    case WCS_TNX:   /* -TNX tan with polynomial correction */
      if (sins>1.0) return 1;
      dect = cos0 - m * sin0;
      if (dect==0.0) return 1;
      rat = ra0 + atan2 (l, dect);
      dect = atan (cos(rat-ra0) * (m * cos0 + sin0) / dect);
      break;

    case WCS_ARC:   /* -ARC Arc*/
      if (sins>=twopi*twopi/4.0) return 1;
      sins = sqrt(sins);
      coss = cos (sins);
      if (sins!=0.0) sins = sin (sins) / sins;
      else
	sins = 1.0;
      dt = m * cos0 * sins + sin0 * coss;
      if ((dt>1.0) || (dt<-1.0)) return 1;
      dect = asin (dt);
      da = coss - dt * sin0;
      dt = l * sins * cos0;
      if ((da==0.0) && (dt==0.0)) return 1;
      rat = ra0 + atan2 (dt, da);
      break;

    case WCS_NCP:   /* -NCP North celestial pole*/
      dect = cos0 - m * sin0;
      if (dect==0.0) return 1;
      rat = ra0 + atan2 (l, dect);
      dt = cos (rat-ra0);
      if (dt==0.0) return 1;
      dect = dect / dt;
      if ((dect>1.0) || (dect<-1.0)) return 1;
      dect = acos (dect);
      if (dec0<0.0) dect = -dect;
      break;

    case WCS_GLS:   /* -GLS global sinusoid */
    case WCS_SFL:   /* -SFL Samson-Flamsteed */
      dect = dec0 + m;
      if (fabs(dect)>twopi/4.0) return 1;
      coss = cos (dect);
      if (fabs(l)>twopi*coss/2.0) return 1;
      rat = ra0;
      if (coss>deps) rat = rat + l / coss;
      break;

    case WCS_MER:   /* -MER mercator*/
      dt = yinc * cosr + xinc * sinr;
      if (dt==0.0) dt = 1.0;
      dy = degrad (yref/2.0 + 45.0);
      dx = dy + dt / 2.0 * cond2r;
      dy = log (tan (dy));
      dx = log (tan (dx));
      geo2 = degrad (dt) / (dx - dy);
      geo3 = geo2 * dy;
      geo1 = cos (degrad (yref));
      if (geo1<=0.0) geo1 = 1.0;
      rat = l / geo1 + ra0;
      if (fabs(rat - ra0) > twopi) return 1; /* added 10/13/94 DCW/EWG */
      dt = 0.0;
      if (geo2!=0.0) dt = (m + geo3) / geo2;
      dt = exp (dt);
      dect = 2.0 * atan (dt) - twopi / 4.0;
      break;

    case WCS_AIT:   /* -AIT Aitoff*/
      dt = yinc*cosr + xinc*sinr;
      if (dt==0.0) dt = 1.0;
      dt = degrad (dt);
      dy = degrad (yref);
      dx = sin(dy+dt)/sqrt((1.0+cos(dy+dt))/2.0) -
	  sin(dy)/sqrt((1.0+cos(dy))/2.0);
      if (dx==0.0) dx = 1.0;
      geo2 = dt / dx;
      dt = xinc*cosr - yinc* sinr;
      if (dt==0.0) dt = 1.0;
      dt = degrad (dt);
      dx = 2.0 * cos(dy) * sin(dt/2.0);
      if (dx==0.0) dx = 1.0;
      geo1 = dt * sqrt((1.0+cos(dy)*cos(dt/2.0))/2.0) / dx;
      geo3 = geo2 * sin(dy) / sqrt((1.0+cos(dy))/2.0);
      rat = ra0;
      dect = dec0;
      if ((l==0.0) && (m==0.0)) break;
      dz = 4.0 - l*l/(4.0*geo1*geo1) - ((m+geo3)/geo2)*((m+geo3)/geo2) ;
      if ((dz>4.0) || (dz<2.0)) return 1;;
      dz = 0.5 * sqrt (dz);
      dd = (m+geo3) * dz / geo2;
      if (fabs(dd)>1.0) return 1;;
      dd = asin (dd);
      if (fabs(cos(dd))<deps) return 1;;
      da = l * dz / (2.0 * geo1 * cos(dd));
      if (fabs(da)>1.0) return 1;;
      da = asin (da);
      rat = ra0 + 2.0 * da;
      dect = dd;
      break;

    case WCS_STG:   /* -STG Sterographic*/
      dz = (4.0 - sins) / (4.0 + sins);
      if (fabs(dz)>1.0) return 1;
      dect = dz * sin0 + m * cos0 * (1.0+dz) / 2.0;
      if (fabs(dect)>1.0) return 1;
      dect = asin (dect);
      rat = cos(dect);
      if (fabs(rat)<deps) return 1;
      rat = l * (1.0+dz) / (2.0 * rat);
      if (fabs(rat)>1.0) return 1;
      rat = asin (rat);
      mg = 1.0 + sin(dect) * sin0 + cos(dect) * cos0 * cos(rat);
      if (fabs(mg)<deps) return 1;
      mg = 2.0 * (sin(dect) * cos0 - cos(dect) * sin0 * cos(rat)) / mg;
      if (fabs(mg-m)>deps) rat = twopi/2.0 - rat;
      rat = ra0 + rat;
      break;

    case WCS_COE:    /* COE projection code from Andreas Wicenic, ESO */
      td = tan (dec0);
      y0 = 1.0 / td;
      mt = y0 - m;
      if (dec0 < 0.)
	a = atan2 (l,-mt);
      else
	a = atan2 (l, mt);
      rat = ra0 - (a / sin0);
      r2 = (l * l) + (mt * mt);
      dect = asin (1.0 / (sin0 * 2.0) * (1.0 + sin0*sin0 * (1.0 - r2)));
      break;
  }

  /* Return RA in range  */
  raout = rat;
  decout = dect;
  if (raout-ra0>twopi/2.0) raout = raout - twopi;
  if (raout-ra0<-twopi/2.0) raout = raout + twopi;
  if (raout < 0.0) raout += twopi; /* added by DCW 10/12/94 */

  /* Convert units back to degrees  */
  *xpos = raddeg (raout);
  *ypos = raddeg (decout);

  return 0;
}  /* End of worldpos */


int
worldpix (xpos, ypos, wcs, xpix, ypix)

/*-----------------------------------------------------------------------*/
/* routine to determine accurate pixel coordinates for an RA and Dec     */
/* returns 0 if successful otherwise:                                    */
/*  1 = angle too large for projection;                                  */
/*  2 = bad values                                                       */
/* does: SIN, TAN, ARC, NCP, GLS or SFL, MER, AIT, STG, CAR, COE projections    */
/* anything else is linear                                               */

/* Input: */
double	xpos;		/* x (RA) coordinate (deg) */
double	ypos;		/* y (dec) coordinate (deg) */
struct WorldCoor *wcs;	/* WCS parameter structure */

/* Output: */
double	*xpix;		/* x pixel number  (RA or long without rotation) */
double	*ypix;		/* y pixel number  (dec or lat without rotation) */
{
  double dx, dy, ra0, dec0, ra, dec, coss, sins, dt, da, dd, sint;
  double l, m, geo1, geo2, geo3, sinr, cosr, tx, x, a2, a3, a4;
  double rthea,gamby2,a,b,c,phi,an,rap,v,tthea,co1,co2,co3,co4,ansq; /* COE */
  double cond2r=1.745329252e-2, deps=1.0e-5, twopi=6.28318530717959;

/* Structure elements */
  double xref;		/* x reference coordinate value (deg) */
  double yref;		/* y reference coordinate value (deg) */
  double xrefpix;	/* x reference pixel */
  double yrefpix;	/* y reference pixel */
  double xinc;		/* x coordinate increment (deg) */
  double yinc;		/* y coordinate increment (deg) */
  double rot;		/* Optical axis rotation (deg)  (from N through E) */
  int itype;

  /* Set local projection parameters */
  xref = wcs->xref;
  yref = wcs->yref;
  xrefpix = wcs->xrefpix;
  yrefpix = wcs->yrefpix;
  xinc = wcs->xinc;
  yinc = wcs->yinc;
  rot = degrad (wcs->rot);
  cosr = cos (rot);
  sinr = sin (rot);

  /* Projection type */
  itype = wcs->prjcode;

  /* Nonlinear position */
  if (itype > 0) {
    if (wcs->coorflip) {
      dec0 = degrad (xref);
      ra0 = degrad (yref);
      dt = xpos - yref;
      }
    else {
      ra0 = degrad (xref);
      dec0 = degrad (yref);
      dt = xpos - xref;
      }

    /* 0h wrap-around tests added by D.Wells 10/12/1994: */
    /* Modified to exclude weird reference pixels by D.Mink 2/3/2004 */
    if (xrefpix*xinc > 180.0 || xrefpix*xinc < -180.0) {
	if (dt > 360.0) xpos -= 360.0;
	if (dt < 0.0) xpos += 360.0;
	}
    else {
	if (dt > 180.0) xpos -= 360.0;
	if (dt < -180.0) xpos += 360.0;
	}
    /* NOTE: changing input argument xpos is OK (call-by-value in C!) */

    ra = degrad (xpos);
    dec = degrad (ypos);

    /* Compute direction cosine */
    coss = cos (dec);
    sins = sin (dec);
    l = sin(ra-ra0) * coss;
    sint = sins * sin(dec0) + coss * cos(dec0) * cos(ra-ra0);
    }
  else {
    l = 0.0;
    sint = 0.0;
    sins = 0.0;
    coss = 0.0;
    ra = 0.0;
    dec = 0.0;
    ra0 = 0.0;
    dec0 = 0.0;
    m = 0.0;
    }

  /* Process by case  */
  switch (itype) {

    case WCS_CAR:   /* -CAR Cartesian */
      l = ra - ra0;
      m = dec - dec0;
      break;

    case WCS_SIN:   /* -SIN sin*/ 
	if (sint<0.0) return 1;
	m = sins * cos(dec0) - coss * sin(dec0) * cos(ra-ra0);
	break;

    case WCS_TAN:   /* -TAN tan */
	if (sint<=0.0) return 1;
 	m = sins * sin(dec0) + coss * cos(dec0) * cos(ra-ra0);
	l = l / m;
	m = (sins * cos(dec0) - coss * sin(dec0) * cos(ra-ra0)) / m;
	break;

    case WCS_ARC:   /* -ARC Arc*/
	m = sins * sin(dec0) + coss * cos(dec0) * cos(ra-ra0);
	if (m<-1.0) m = -1.0;
	if (m>1.0) m = 1.0;
	m = acos (m);
	if (m!=0) 
	    m = m / sin(m);
	else
	    m = 1.0;
	l = l * m;
	m = (sins * cos(dec0) - coss * sin(dec0) * cos(ra-ra0)) * m;
	break;

    case WCS_NCP:   /* -NCP North celestial pole*/
	if (dec0==0.0) 
	    return 1;  /* can't stand the equator */
	else
	    m = (cos(dec0) - coss * cos(ra-ra0)) / sin(dec0);
	break;

    case WCS_GLS:   /* -GLS global sinusoid */
    case WCS_SFL:   /* -SFL Samson-Flamsteed */
	dt = ra - ra0;
	if (fabs(dec)>twopi/4.0) return 1;
	if (fabs(dec0)>twopi/4.0) return 1;
	m = dec - dec0;
	l = dt * coss;
	break;

    case WCS_MER:   /* -MER mercator*/
	dt = yinc * cosr + xinc * sinr;
	if (dt==0.0) dt = 1.0;
	dy = degrad (yref/2.0 + 45.0);
	dx = dy + dt / 2.0 * cond2r;
	dy = log (tan (dy));
	dx = log (tan (dx));
	geo2 = degrad (dt) / (dx - dy);
	geo3 = geo2 * dy;
	geo1 = cos (degrad (yref));
	if (geo1<=0.0) geo1 = 1.0;
	dt = ra - ra0;
	l = geo1 * dt;
	dt = dec / 2.0 + twopi / 8.0;
	dt = tan (dt);
	if (dt<deps) return 2;
	m = geo2 * log (dt) - geo3;
	break;

    case WCS_AIT:   /* -AIT Aitoff*/
	l = 0.0;
	m = 0.0;
	da = (ra - ra0) / 2.0;
	if (fabs(da)>twopi/4.0) return 1;
	dt = yinc*cosr + xinc*sinr;
	if (dt==0.0) dt = 1.0;
	dt = degrad (dt);
	dy = degrad (yref);
	dx = sin(dy+dt)/sqrt((1.0+cos(dy+dt))/2.0) -
	     sin(dy)/sqrt((1.0+cos(dy))/2.0);
	if (dx==0.0) dx = 1.0;
	geo2 = dt / dx;
	dt = xinc*cosr - yinc* sinr;
	if (dt==0.0) dt = 1.0;
	dt = degrad (dt);
	dx = 2.0 * cos(dy) * sin(dt/2.0);
	if (dx==0.0) dx = 1.0;
	geo1 = dt * sqrt((1.0+cos(dy)*cos(dt/2.0))/2.0) / dx;
	geo3 = geo2 * sin(dy) / sqrt((1.0+cos(dy))/2.0);
	dt = sqrt ((1.0 + cos(dec) * cos(da))/2.0);
	if (fabs(dt)<deps) return 3;
	l = 2.0 * geo1 * cos(dec) * sin(da) / dt;
	m = geo2 * sin(dec) / dt - geo3;
	break;

    case WCS_STG:   /* -STG Sterographic*/
	da = ra - ra0;
	if (fabs(dec)>twopi/4.0) return 1;
	dd = 1.0 + sins * sin(dec0) + coss * cos(dec0) * cos(da);
	if (fabs(dd)<deps) return 1;
	dd = 2.0 / dd;
	l = l * dd;
	m = dd * (sins * cos(dec0) - coss * sin(dec0) * cos(da));
	break;

    case WCS_COE:    /* allan: -COE projection added, AW, ESO*/
	gamby2 = sin (dec0);
	tthea = tan (dec0);
	rthea = 1. / tthea;
	a = -2. * tthea;
	b = tthea * tthea;
	c = tthea / 3.;
	a2 = a * a;
	a3 = a2 * a;
	a4 = a2 * a2;
	co1 = a/2.;
	co2 = -0.125 * a2 + b/2.;
	co3 = -0.25 * a*b + 0.0625 * a3 + c/2.0;
	co4 = -0.125 * b*b - 0.25 * a*c + 0.1875 * b*a2 - (5.0/128.0)*a4;
	phi = ra0 - ra;
	an = phi * gamby2;
	v = dec - dec0;
	rap = rthea * (1.0 + v * (co1+v * (co2+v * (co3+v * co4))));
	ansq = an * an;
	if (wcs->rotmat)
	    l = rap * an * (1.0 - ansq/6.0) * (wcs->cd[0] / fabs(wcs->cd[0]));
	else
	    l = rap * an * (1.0 - ansq/6.0) * (xinc / fabs(xinc));
	m = rthea - (rap * (1.0 - ansq/2.0));
	break;

    }  /* end of itype switch */

  /* Convert back to degrees  */
  if (itype > 0) {
    dx = raddeg (l);
    dy = raddeg (m);
    }

  /* For linear or pixel projection */
  else {
    dx = xpos - xref;
    dy = ypos - yref;
    }

  if (wcs->coorflip) {
    tx = dx;
    dx = dy;
    dy = tx;
    }

  /* Scale and rotate using CD matrix */
  if (wcs->rotmat) {
    tx = dx * wcs->dc[0] + dy * wcs->dc[1];
    dy = dx * wcs->dc[2] + dy * wcs->dc[3];
    dx = tx;
    }

  /* Scale and rotate using CDELTn and CROTA2 */
  else {

    /* Correct for rotation */
    if (rot!=0.0) {
      tx = dx*cosr + dy*sinr;
      dy = dy*cosr - dx*sinr;
      dx = tx;
      }

    /* Scale using CDELT */
    if (xinc != 0.)
      dx = dx / xinc;
    if (yinc != 0.)
      dy = dy / yinc;
    }

  /* Convert to pixels  */
  *xpix = dx + xrefpix;
  if (itype == WCS_CAR) {
    if (*xpix > wcs->nxpix) {
      x = *xpix - (360.0 / xinc);
      if (x > 0.0) *xpix = x;
      }
    else if (*xpix < 0) {
      x = *xpix + (360.0 / xinc);
      if (x <= wcs->nxpix) *xpix = x;
      }
    }
  *ypix = dy + yrefpix;

  return 0;
}  /* end worldpix */

 
/* Oct 26 1995	Fix bug which interchanged RA and Dec twice when coorflip
 *
 * Oct 31 1996	Fix CD matrix use in WORLDPIX
 * Nov  4 1996	Eliminate extra code for linear projection in WORLDPIX
 * Nov  5 1996	Add coordinate flip in WORLDPIX
 *
 * May 22 1997	Avoid angle wraparound when CTYPE is pixel
 * Jun  4 1997	Return without angle conversion from worldpos if type is PIXEL
 *
 * Oct 20 1997	Add chip rotation; compute rotation angle trig functions
 * Jan 23 1998	Change PCODE to PRJCODE
 * Jan 26 1998	Remove chip rotation code
 * Feb  5 1998	Make cd[] and dc[] vectors; use xinc, yinc, rot from init
 * Feb 23 1998	Add NOAO TNX projection as TAN
 * Apr 28 1998  Change projection flags to WCS_*
 * May 27 1998	Skip limit checking for linear projection
 * Jun 25 1998	Fix inverse for CAR projection
 * Aug  5 1998	Allan Brighton: Added COE projection (code from A. Wicenec, ESO)
 * Sep 30 1998	Fix bug in COE inverse code to get sign correct
 *
 * Oct 21 1999	Drop unused y from worldpix()
 *
 * Apr  3 2002	Use GLS and SFL interchangeably
 *
 * Feb  3 2004	Let ra be >180 in worldpix() if ref pixel is >180 deg away
 *
 * Jun 20 2006	Initialize uninitialized variables
 */
