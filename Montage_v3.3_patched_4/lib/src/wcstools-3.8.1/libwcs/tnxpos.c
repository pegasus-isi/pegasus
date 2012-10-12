/*** File wcslib/tnxpos.c
 *** September 17, 2008
 *** By Doug Mink, dmink@cfa.harvard.edu
 *** Harvard-Smithsonian Center for Astrophysics
 *** After IRAF mwcs/wftnx.x and mwcs/wfgsurfit.x
 *** Copyright (C) 1998-2008
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
#include "wcs.h"

#define SPHTOL 0.00001
#define BADCVAL 0.0
#define MAX(a,b) (((a) > (b)) ? (a) : (b))
#define MIN(a,b) (((a) < (b)) ? (a) : (b))

/* wftnx -- wcs function driver for the gnomonic projection with correction.
 *    tnxinit (header, wcs)
 *    tnxclose (wcs)
 *    tnxfwd (xpix, ypix, wcs, xpos, ypos)	Pixels to WCS
 *    tnxrev (xpos, ypos, wcs, xpix, ypix)	WCS to pixels
 */

#define	max_niter	500
#define	SZ_ATSTRING	2000
static void wf_gsclose();
static void wf_gsb1pol();
static void wf_gsb1leg();
static void wf_gsb1cheb();

/* tnxinit -- initialize the gnomonic forward or inverse transform.
 * initialization for this transformation consists of, determining which
 * axis is ra / lon and which is dec / lat, computing the celestial longitude
 * and colatitude of the native pole, reading in the the native longitude
 * of the pole of the celestial coordinate system longpole from the attribute
 * list, precomputing euler angles and various intermediaries derived from the
 * coordinate reference values, and reading in the projection parameter ro
 * from the attribute list. if longpole is undefined then a value of 180.0
 * degrees is assumed. if ro is undefined a value of 180.0 / pi is assumed.
 * the tan projection is equivalent to the azp projection with mu set to 0.0.
 * in order to determine the axis order, the parameter "axtype={ra|dec}
 * {xlon|glat}{xlon|elat}" must have been set in the attribute list for the
 * function. the longpole and ro parameters may be set in either or both of
 * the axes attribute lists, but the value in the ra axis attribute list takes
 * precedence. 
 */

int
tnxinit (header, wcs)

const char *header;	/* FITS header */
struct WorldCoor *wcs;	/* pointer to WCS structure */
{
    struct IRAFsurface *wf_gsopen();
    char *str1, *str2, *lngstr, *latstr;
    extern void wcsrotset();

    /* allocate space for the attribute strings */
    str1 = malloc (SZ_ATSTRING);
    str2 = malloc (SZ_ATSTRING);
    hgetm (header, "WAT1", SZ_ATSTRING, str1);
    hgetm (header, "WAT2", SZ_ATSTRING, str2);

    lngstr = malloc (SZ_ATSTRING);
    latstr = malloc (SZ_ATSTRING);

    /* determine the native longitude of the pole of the celestial
	coordinate system corresponding to the FITS keyword longpole.
	this number has no default and should normally be set to 180
	degrees. search both axes for this quantity. */

    if (wcs->longpole > 360.0) {
	if (!igetr8 (str1, "longpole", &wcs->longpole)) {
	    if (!igetr8 (str2, "longpole", &wcs->longpole))
		wcs->longpole = 180.0;
	    }
	}

    /*  Fetch the ro projection parameter which is the radius of the
	generating sphere for the projection. if ro is absent which
	is the usual case set it to 180 / pi. search both axes for
	this quantity. */

    if (!igetr8 (str1, "ro", &wcs->rodeg)) {
	if (!igetr8 (str2, "ro", &wcs->rodeg))
	    wcs->rodeg = 180.0 / PI;
	}

    /*  Fetch the longitude correction surface. note that the attribute
	string may be of any length so the length of atvalue may have
	to be adjusted. */

    if (!igets (str1, "lngcor", SZ_ATSTRING, lngstr)) {
	if (!igets (str2, "lngcor", SZ_ATSTRING, lngstr))
	    wcs->lngcor = NULL;
	else
	    wcs->lngcor = wf_gsopen (lngstr);
	}
    else
	wcs->lngcor = wf_gsopen (lngstr);

    /*  Fetch the latitude correction surface. note that the attribute
	string may be of any length so the length of atvalue may have
	to be adjusted. */

    if (!igets (str2, "latcor", SZ_ATSTRING, latstr)) {
	if (!igets (str1, "latcor", SZ_ATSTRING, latstr))
	    wcs->latcor = NULL;
	else
	    wcs->latcor = wf_gsopen (latstr);
	}
    else
	wcs->latcor = wf_gsopen (latstr);

    /* Compute image rotation */
    wcsrotset (wcs);

    /* free working space. */
    free (str1);
    free (str2);
    free (lngstr);
    free (latstr);

    /* Return 1 if there are no correction coefficients */
    if (wcs->latcor == NULL && wcs->lngcor == NULL)
	return (1);
    else
	return (0);
}


/* tnxpos -- forward transform (physical to world) gnomonic projection. */

int
tnxpos (xpix, ypix, wcs, xpos, ypos)

double	xpix, ypix;	/*i physical coordinates (x, y) */
struct WorldCoor *wcs;	/*i pointer to WCS descriptor */
double	*xpos, *ypos;	/*o world coordinates (ra, dec) */
{
    int	ira, idec;
    double x, y, r, phi, theta, costhe, sinthe, dphi, cosphi, sinphi, dlng, z;
    double colatp, coslatp, sinlatp, longp;
    double xs, ys, ra, dec, xp, yp;
    double wf_gseval();

    /* Convert from pixels to image coordinates */
    xpix = xpix - wcs->crpix[0];
    ypix = ypix - wcs->crpix[1];

    /* Scale and rotate using CD matrix */
    if (wcs->rotmat) {
	x = xpix * wcs->cd[0] + ypix * wcs->cd[1];
	y = xpix * wcs->cd[2] + ypix * wcs->cd[3];
	}

    else {

	/* Check axis increments - bail out if either 0 */
	if (wcs->cdelt[0] == 0.0 || wcs->cdelt[1] == 0.0) {
	    *xpos = 0.0;
	    *ypos = 0.0;
	    return 2;
	    }

	/* Scale using CDELT */
	xs = xpix * wcs->cdelt[0];
	ys = ypix * wcs->cdelt[1];

	/* Take out rotation from CROTA */
	if (wcs->rot != 0.0) {
	    double cosr = cos (degrad (wcs->rot));
	    double sinr = sin (degrad (wcs->rot));
	    x = xs * cosr - ys * sinr;
	    y = xs * sinr + ys * cosr;
    	    }
	else {
	    x = xs;
	    y = ys;
	    }
	}

    /* get the axis numbers */
    if (wcs->coorflip) {
	ira = 1;
	idec = 0;
	}
    else {
	ira = 0;
	idec = 1;
	}
    colatp = degrad (90.0 - wcs->crval[idec]);
    coslatp = cos(colatp);
    sinlatp = sin(colatp);
    longp = degrad(wcs->longpole);

    /*  Compute native spherical coordinates phi and theta in degrees from the
	projected coordinates. this is the projection part of the computation */
    if (wcs->lngcor != NULL)
	xp = x + wf_gseval (wcs->lngcor, x, y);
    else
	xp = x;
    if (wcs->latcor != NULL)
	yp = y + wf_gseval (wcs->latcor, x, y);
    else
	yp = y;
    x = xp;
    y = yp;
    r = sqrt (x * x + y * y);

    /* Compute phi */
    if (r == 0.0)
	phi = 0.0;
    else
	phi = atan2 (x, -y);

    /* Compute theta */
    theta = atan2 (wcs->rodeg, r);

    /*  Compute the celestial coordinates ra and dec from the native
	coordinates phi and theta. this is the spherical geometry part
	of the computation */

    costhe = cos (theta);
    sinthe = sin (theta);
    dphi = phi - longp;
    cosphi = cos (dphi);
    sinphi = sin (dphi);

    /* Compute the ra */
    x = sinthe * sinlatp - costhe * coslatp * cosphi;
    if (fabs (x) < SPHTOL)
	x = -cos (theta + colatp) + costhe * coslatp * (1.0 - cosphi);
    y = -costhe * sinphi;
    if (x != 0.0 || y != 0.0)
	dlng = atan2 (y, x);
    else
	dlng = dphi + PI ;
    ra =  wcs->crval[ira] + raddeg(dlng);

    /* normalize ra */
    if (wcs->crval[ira] >= 0.0) {
	if (ra < 0.0)
	    ra = ra + 360.0;
	}
    else {
	if (ra > 0.0)
	    ra = ra - 360.0;
	}
    if (ra > 360.0)
	ra = ra - 360.0;
    else if (ra < -360.0)
	ra = ra + 360.0;

    /* compute the dec */
    if (fmod (dphi, PI) == 0.0) {
	dec = raddeg(theta + cosphi * colatp);
	if (dec > 90.0)
	    dec = 180.0 - dec;
	if (dec < -90.0)
	    dec = -180.0 - dec;
	}
    else {
	z = sinthe * coslatp + costhe * sinlatp * cosphi;
	if (fabs(z) > 0.99) {
	    if (z >= 0.0)
		dec = raddeg(acos (sqrt(x * x + y * y)));
	    else
		dec = raddeg(-acos (sqrt(x * x + y * y)));
	    }
	else
		dec = raddeg(asin (z));
	}

    /* store the results */
    *xpos  = ra;
    *ypos = dec;
    return (0);
}


/* tnxpix -- inverse transform (world to physical) gnomonic projection */

int
tnxpix (xpos, ypos, wcs, xpix, ypix)

double	xpos, ypos;	/*i world coordinates (ra, dec) */
struct WorldCoor *wcs;	/*i pointer to WCS descriptor */
double	*xpix, *ypix;	/*o physical coordinates (x, y) */
{
    int	ira, idec, niter;
    double ra, dec, cosdec, sindec, cosra, sinra, x, y, phi, theta;
    double s, r, dphi, z, dpi, dhalfpi, twopi, tx;
    double xm, ym, f, fx, fy, g, gx, gy, denom, dx, dy;
    double colatp, coslatp, sinlatp, longp, sphtol;
    double wf_gseval(), wf_gsder();

    /* get the axis numbers */
    if (wcs->coorflip) {
	ira = 1;
	idec = 0;
	}
    else {
	ira = 0;
	idec = 1;
	}

    /*  Compute the transformation from celestial coordinates ra and
	dec to native coordinates phi and theta. this is the spherical
	geometry part of the transformation */

    ra  = degrad (xpos - wcs->crval[ira]);
    dec = degrad (ypos);
    cosra = cos (ra);
    sinra = sin (ra);
    cosdec = cos (dec);
    sindec = sin (dec);
    colatp = degrad (90.0 - wcs->crval[idec]);
    coslatp = cos (colatp);
    sinlatp = sin (colatp);
    if (wcs->longpole == 999.0)
	longp = degrad (180.0);
    else
	longp = degrad(wcs->longpole);
    dpi = PI;
    dhalfpi = dpi * 0.5;
    twopi = PI + PI;
    sphtol = SPHTOL;

    /* Compute phi */
    x = sindec * sinlatp - cosdec * coslatp * cosra;
    if (fabs(x) < sphtol)
	x = -cos (dec + colatp) + cosdec * coslatp * (1.0 - cosra);
    y = -cosdec * sinra;
    if (x != 0.0 || y != 0.0)
	dphi = atan2 (y, x);
    else
	dphi = ra - dpi;
    phi = longp + dphi;
    if (phi > dpi)
	phi = phi - twopi;
    else if (phi < -dpi)
	phi = phi + twopi;

    /* Compute theta */
    if (fmod (ra, dpi) == 0.0) {
	theta = dec + cosra * colatp;
	if (theta > dhalfpi)
	    theta = dpi - theta;
	if (theta < -dhalfpi)
	    theta = -dpi - theta;
	}
    else {
	z = sindec * coslatp + cosdec * sinlatp * cosra;
	if (fabs (z) > 0.99) {
	    if (z >= 0.0)
		theta = acos (sqrt(x * x + y * y));
	    else
		theta = -acos (sqrt(x * x + y * y));
	    }
	else
	    theta = asin (z);
	}

    /*  Compute the transformation from native coordinates phi and theta
	to projected coordinates x and y */

    s = sin (theta);
    if (s == 0.0) {
	x = BADCVAL;
	y = BADCVAL;
	}
    else {
	r = wcs->rodeg * cos (theta) / s;
	if (wcs->lngcor == NULL && wcs->latcor == NULL) {
	    if (wcs->coorflip) {
		y  = r * sin (phi);
		x = -r * cos (phi);
		}
	    else {
		x  = r * sin (phi);
		y = -r * cos (phi);
		}
	    }
	else {
	    xm  = r * sin (phi);
	    ym = -r * cos (phi);
	    x = xm;
	    y = ym;
	    niter = 0;
	    while (niter < max_niter) {
		if (wcs->lngcor != NULL) {
		    f = x + wf_gseval (wcs->lngcor, x, y) - xm;
		    fx = wf_gsder (wcs->lngcor, x, y, 1, 0);
		    fx = 1.0 + fx;
		    fy = wf_gsder (wcs->lngcor, x, y, 0, 1);
		    }
		else {
		    f = x - xm;
		    fx = 1.0 ;
		    fy = 0.0;
		    }
		if (wcs->latcor != NULL) {
		    g = y + wf_gseval (wcs->latcor, x, y) - ym;
		    gx = wf_gsder (wcs->latcor, x, y, 1, 0);
		    gy = wf_gsder (wcs->latcor, x, y, 0, 1);
		    gy = 1.0 + gy;
		    }
		else {
		    g = y - ym;
		    gx = 0.0 ;
		    gy = 1.0;
		    }

		denom = fx * gy - fy * gx;
		if (denom == 0.0)
		    break;
		dx = (-f * gy + g * fy) / denom;
		dy = (-g * fx + f * gx) / denom;
		x = x + dx;
		y = y + dy;
		if (MAX(MAX(fabs(dx),fabs(dy)),MAX(fabs(f),fabs(g))) < 2.80e-8)
		    break;

		niter = niter + 1;
		}

	    /* Reverse x and y if axes flipped */
	    if (wcs->coorflip) {
		tx = x;
		x = y;
		y = tx;
		}
	    }
	}

    /* Scale and rotate using CD matrix */
    if (wcs->rotmat) {
	*xpix = x * wcs->dc[0] + y * wcs->dc[1];
	*ypix = x * wcs->dc[2] + y * wcs->dc[3];
	}

    else {

	/* Correct for rotation */
	if (wcs->rot!=0.0) {
	    double cosr = cos (degrad (wcs->rot));
	    double sinr = sin (degrad (wcs->rot));
	    *xpix = x * cosr + y * sinr;
	    *ypix = y * cosr - x * sinr;
	    }
	else {
	    *xpix = x;
	    *ypix = y;
	    }

	/* Scale using CDELT */
	if (wcs->xinc != 0.)
	    *xpix = *xpix / wcs->xinc;
	if (wcs->yinc != 0.)
	    *ypix = *ypix / wcs->yinc;
	}

    /* Convert to pixels  */
    *xpix = *xpix + wcs->xrefpix;
    *ypix = *ypix + wcs->yrefpix;

    return (0);
}


/* TNXCLOSE -- free up the distortion surface pointers */

void
tnxclose (wcs)

struct WorldCoor *wcs;		/* pointer to the WCS descriptor */

{
    if (wcs->lngcor != NULL)
	wf_gsclose (wcs->lngcor);
    if (wcs->latcor != NULL)
	wf_gsclose (wcs->latcor);
    return;
}

/* copyright(c) 1986 association of universities for research in astronomy inc.
 * wfgsurfit.x -- surface fitting package used by wcs function drivers.
 * Translated to C from SPP by Doug Mink, SAO, May 26, 1998
 *
 * the following routines are used by the experimental function drivers tnx
 * and zpx to decode polynomial fits stored in the image header in the form
 * of a list of parameters and coefficients into surface descriptors in
 * ra / dec or longitude latitude. the polynomial surfaces so encoded consist
 * of corrections to function drivers tan and zpn. the package routines are
 * modelled after the equivalent gsurfit routines and are consistent with them.
 * the routines are:
 *
 *                 sf = wf_gsopen (wattstr)
 *                     wf_gsclose (sf)
 *
 *                  z = wf_gseval (sf, x, y)
 *             ncoeff = wf_gscoeff (sf, coeff)
 *               zder = wf_gsder (sf, x, y, nxder, nyder)
 *
 * wf_gsopen is used to open a surface fit encoded in a wcs attribute, returning
 * the sf surface fitting descriptor.  wf_gsclose should be called later to free
 * the descriptor.  wf_gseval is called to evaluate the surface at a point.
 */


#define  SZ_GSCOEFFBUF     20

/* define the structure elements for the wf_gsrestore task */
#define  TNX_SAVETYPE     0
#define  TNX_SAVEXORDER   1
#define  TNX_SAVEYORDER   2
#define  TNX_SAVEXTERMS   3
#define  TNX_SAVEXMIN     4
#define  TNX_SAVEXMAX     5
#define  TNX_SAVEYMIN     6
#define  TNX_SAVEYMAX     7
#define  TNX_SAVECOEFF    8


/* wf_gsopen -- decode the longitude / latitude or ra / dec mwcs attribute
 * and return a gsurfit compatible surface descriptor.
 */

struct IRAFsurface *
wf_gsopen (astr)

char    *astr;		/* the input mwcs attribute string */

{
    double dval;
    char *estr;
    int npar, szcoeff;
    double *coeff;
    struct IRAFsurface *gs;
    struct IRAFsurface *wf_gsrestore();

    if (astr[1] == 0)
	return (NULL);

    gs = NULL;
    npar = 0;
    szcoeff = SZ_GSCOEFFBUF;
    coeff = (double *) malloc (szcoeff * sizeof (double));

    estr = astr;
    while (*estr != (char) 0) {
	dval = strtod (astr, &estr);
	if (*estr == '.')
	    estr++;
	if (*estr != (char) 0) {
	    npar++;
	    if (npar >= szcoeff) {
		szcoeff = szcoeff + SZ_GSCOEFFBUF;
		coeff = (double *) realloc (coeff, (szcoeff * sizeof (double)));
		}
	    coeff[npar-1] = dval;
	    astr = estr;
	    while (*astr == ' ') astr++;
	    }
        }

    gs = wf_gsrestore (coeff);

    free (coeff);

    if (npar == 0)
	return (NULL);
    else
	return (gs);
}


/* wf_gsclose -- procedure to free the surface descriptor */

static void
wf_gsclose (sf)

struct IRAFsurface *sf;	/* the surface descriptor */

{
    if (sf != NULL) {
	if (sf->xbasis != NULL)
	    free (sf->xbasis);
	if (sf->ybasis != NULL)
	    free (sf->ybasis);
	if (sf->coeff != NULL)
	    free (sf->coeff);
	free (sf);
	}
    return;
}


/* wf_gseval -- procedure to evaluate the fitted surface at a single point.
 * the wf->ncoeff coefficients are stored in the vector pointed to by sf->coeff.
 */

double
wf_gseval (sf, x, y)

struct IRAFsurface *sf;	/* pointer to surface descriptor structure */
double  x;		/* x value */
double  y;		/* y value */
{
    double sum, accum;
    int i, ii, k, maxorder, xorder;

    /* Calculate the basis functions */
    switch (sf->type) {
        case TNX_CHEBYSHEV:
            wf_gsb1cheb (x, sf->xorder, sf->xmaxmin, sf->xrange, sf->xbasis);
            wf_gsb1cheb (y, sf->yorder, sf->ymaxmin, sf->yrange, sf->ybasis);
	    break;
        case TNX_LEGENDRE:
            wf_gsb1leg (x, sf->xorder, sf->xmaxmin, sf->xrange, sf->xbasis);
            wf_gsb1leg (y, sf->yorder, sf->ymaxmin, sf->yrange, sf->ybasis);
	    break;
        case TNX_POLYNOMIAL:
            wf_gsb1pol (x, sf->xorder, sf->xbasis);
            wf_gsb1pol (y, sf->yorder, sf->ybasis);
	    break;
        default:
            fprintf (stderr,"TNX_GSEVAL: unknown surface type\n");
	    return (0.0);
        }

    /* Initialize accumulator basis functions */
    sum = 0.0;

    /* Loop over y basis functions */
    if (sf->xorder > sf->yorder)
	maxorder = sf->xorder + 1;
    else
	maxorder = sf->yorder + 1;
    xorder = sf->xorder;
    ii = 0;

    for (i = 0; i < sf->yorder; i++) {

	/* Loop over the x basis functions */
	accum = 0.0;
	for (k = 0; k < xorder; k++) {
	    accum = accum + sf->coeff[ii] * sf->xbasis[k];
	    ii = ii + 1;
	    }
	accum = accum * sf->ybasis[i];
	sum = sum + accum;

        /* Elements of the coefficient vector where neither k = 1 or i = 1
           are not calculated if sf->xterms = no. */
        if (sf->xterms == TNX_XNONE)
            xorder = 1;
        else if (sf->xterms == TNX_XHALF) {
            if ((i + 1 + sf->xorder + 1) > maxorder)
                xorder = xorder - 1;
	    }
        }

    return (sum);
}


/* TNX_GSCOEFF -- procedure to fetch the number and magnitude of the coefficients
 * if the sf->xterms = wf_xbi (yes) then the number of coefficients will be
 * (sf->xorder * sf->yorder); if wf_xterms is wf_xtri then the number
 * of coefficients will be (sf->xorder *  sf->yorder - order *
 * (order - 1) / 2) where order is the minimum of the x and yorders;  if
 * sf->xterms = TNX_XNONE then the number of coefficients will be
 * (sf->xorder + sf->yorder - 1).
 */

int
wf_gscoeff (sf, coeff)

struct IRAFsurface *sf;	/* pointer to the surface fitting descriptor */
double	*coeff;		/* the coefficients of the fit */

{
    int ncoeff;		/* the number of coefficients */
    int i;

    /* Exctract coefficients from data structure and calculate their number */
    ncoeff = sf->ncoeff;
    for (i = 0; i < ncoeff; i++)
	coeff[i] = sf->coeff[i];
    return (ncoeff);
}


static double *coeff = NULL;
static int nbcoeff = 0;

/* wf_gsder -- procedure to calculate a new surface which is a derivative of
 * the input surface.
 */

double
wf_gsder (sf1, x, y, nxd, nyd)

struct IRAFsurface *sf1; /* pointer to the previous surface */
double	x;		/* x values */
double	y;		/* y values */
int	nxd, nyd;	/* order of the derivatives in x and y */
{
    int nxder, nyder, i, j, k, nbytes;
    int order, maxorder1, maxorder2, nmove1, nmove2;
    struct IRAFsurface *sf2 = 0;
    double *ptr1, *ptr2;
    double zfit, norm;
    double wf_gseval();

    if (sf1 == NULL)
	return (0.0);

    if (nxd < 0 || nyd < 0) {
	fprintf (stderr, "TNX_GSDER: order of derivatives cannot be < 0\n");
	return (0.0);
	}

    if (nxd == 0 && nyd == 0) {
	zfit = wf_gseval (sf1, x, y);
	return (zfit);
	}

    /* Allocate space for new surface */
    sf2 = (struct IRAFsurface *) malloc (sizeof (struct IRAFsurface));

    /* Check the order of the derivatives */
    nxder = MIN (nxd, sf1->xorder - 1);
    nyder = MIN (nyd, sf1->yorder - 1);

    /* Set up new surface */
    sf2->type = sf1->type;

    /* Set the derivative surface parameters */
    if (sf2->type == TNX_LEGENDRE ||
	sf2->type == TNX_CHEBYSHEV ||
	sf2->type == TNX_POLYNOMIAL) {

	sf2->xterms = sf1->xterms;

	/* Find the order of the new surface */
	switch (sf2->xterms) {
	    case TNX_XNONE: 
		if (nxder > 0 && nyder > 0) {
		    sf2->xorder = 1;
		    sf2->yorder = 1;
		    sf2->ncoeff = 1;
		    }
		else if (nxder > 0) {
		    sf2->xorder = MAX (1, sf1->xorder - nxder);
		    sf2->yorder = 1;
		    sf2->ncoeff = sf2->xorder;
		    }
		else if (nyder > 0) {
		    sf2->xorder = 1;
		    sf2->yorder = MAX (1, sf1->yorder - nyder);
		    sf2->ncoeff = sf2->yorder;
		    }
		break;

	    case TNX_XHALF:
		maxorder1 = MAX (sf1->xorder+1, sf1->yorder+1);
		order = MAX(1, MIN(maxorder1-1-nyder-nxder,sf1->xorder-nxder));
		sf2->xorder = order;
		order = MAX(1, MIN(maxorder1-1-nyder-nxder,sf1->yorder-nyder));
		sf2->yorder = order;
		order = MIN (sf2->xorder, sf2->yorder);
		sf2->ncoeff = sf2->xorder * sf2->yorder - (order*(order-1)/2);
		break;

	    default:
		sf2->xorder = MAX (1, sf1->xorder - nxder);
		sf2->yorder = MAX (1, sf1->yorder - nyder);
		sf2->ncoeff = sf2->xorder * sf2->yorder;
	    }

	/* define the data limits */
	sf2->xrange = sf1->xrange;
	sf2->xmaxmin = sf1->xmaxmin;
	sf2->yrange = sf1->yrange;
	sf2->ymaxmin = sf1->ymaxmin;
	}

    else {
	fprintf (stderr, "TNX_GSDER: unknown surface type %d\n", sf2->type);
	return (0.0);
	}

    /* Allocate space for coefficients and basis functions */
    nbytes = sf2->ncoeff * sizeof(double);
    sf2->coeff = (double *) malloc (nbytes);
    nbytes = sf2->xorder * sizeof(double);
    sf2->xbasis = (double *) malloc (nbytes);
    nbytes = sf2->yorder * sizeof(double);
    sf2->ybasis = (double *) malloc (nbytes);

    /* Get coefficients */
    nbytes = sf1->ncoeff * sizeof(double);
    if (nbytes > nbcoeff) {
	if (nbcoeff > 0)
	    coeff = (double *) realloc (coeff, nbytes);
	else
	    coeff = (double *) malloc (nbytes);
	nbcoeff = nbytes;
	}
    (void) wf_gscoeff (sf1, coeff);

    /* Compute the new coefficients */
    switch (sf2->xterms) {
	case TNX_XFULL:
	    ptr2 = sf2->coeff + (sf2->yorder - 1) * sf2->xorder;
	    ptr1 = coeff + (sf1->yorder - 1) * sf1->xorder;
	    for (i = sf1->yorder - 1; i >= nyder; i--) {
		for (j = i; j >= i-nyder+1; j--) {
		    for (k = 0; k < sf2->xorder; k++)
			ptr1[nxder+k] = ptr1[nxder+k] * (double)(j);
		    }
		for (j = sf1->xorder; j >= nxder+1; j--) {
		    for (k = j; k >= j-nxder+1; k--)
			ptr1[j-1] = ptr1[j-1] * (double)(k - 1);
		    }
		for (j = 0; j < sf2->xorder; j++)
		    ptr2[j] = ptr1[nxder+j];
		ptr2 = ptr2 - sf2->xorder;
		ptr1 = ptr1 - sf1->xorder;
		}
	    break;

	case TNX_XHALF:
	    maxorder1 = MAX (sf1->xorder + 1, sf1->yorder + 1);
	    maxorder2 = MAX (sf2->xorder + 1, sf2->yorder + 1);
	    ptr2 = sf2->coeff + sf2->ncoeff;
	    ptr1 = coeff + sf1->ncoeff;
	    for (i = sf1->yorder; i >= nyder+1; i--) {
		nmove1 = MAX (0, MIN (maxorder1 - i, sf1->xorder));
		nmove2 = MAX (0, MIN (maxorder2 - i + nyder, sf2->xorder));
		ptr1 = ptr1 - nmove1;
		ptr2 = ptr2 - nmove2;
		for (j = i; j > i - nyder + 1; j--) {
		    for (k = 0; k < nmove2; k++)
			ptr1[nxder+k] = ptr1[nxder+k] * (double)(j-1);
		    }
		for (j = nmove1; j >= nxder+1; j--) {
		    for (k = j;  k >= j-nxder+1; k--)
			ptr1[j-1] = ptr1[j-1] * (double)(k - 1);
		    }
		for (j = 0; j < nmove2; j++)
		    ptr2[j] = ptr1[nxder+j];
		}
	    break;

	default:
	    if (nxder > 0 && nyder > 0)
		sf2->coeff[0] = 0.0;

	    else if (nxder > 0) { 
		ptr1 = coeff;
		ptr2 = sf2->coeff + sf2->ncoeff - 1;
		for (j = sf1->xorder; j >= nxder+1; j--) {
		    for (k = j; k >= j - nxder + 1; k--)
			ptr1[j-1] = ptr1[j-1] * (double)(k - 1);
		    ptr2[0] = ptr1[j-1];
		    ptr2 = ptr2 - 1;
		    }
		}

	    else if (nyder > 0) {
		ptr1 = coeff + sf1->ncoeff - 1;
		ptr2 = sf2->coeff;
		for (i = sf1->yorder; i >= nyder + 1; i--) {
		    for (j = i; j >= i - nyder + 1; j--)
			*ptr1 = *ptr1 * (double)(j - 1);
		    ptr1 = ptr1 - 1;
		    }
		for (i = 0; i < sf2->ncoeff; i++)
		    ptr2[i] = ptr1[i+1];
		}
	}

    /* evaluate the derivatives */
    zfit = wf_gseval (sf2, x, y);

    /* normalize */
    if (sf2->type != TNX_POLYNOMIAL) { 
	norm = pow (sf2->xrange, (double)nxder) *
	       pow (sf2->yrange, (double)nyder);
	zfit = norm * zfit;
	}

    /* free the space */
    wf_gsclose (sf2);

    return (zfit);
}


/* wf_gsrestore -- procedure to restore the surface fit encoded in the
   image header as a list of double precision parameters and coefficients
   to the surface descriptor for use by the evaluating routines. the
   surface parameters, surface type, xorder (or number of polynomial
   terms in x), yorder (or number of polynomial terms in y), xterms,
   xmin, xmax and ymin and ymax, are stored in the first eight elements
   of the double array fit, followed by the wf->ncoeff surface coefficients.
 */

struct IRAFsurface *
wf_gsrestore (fit)

double	*fit;			/* array containing the surface parameters
				   and coefficients */
{
    struct IRAFsurface	*sf;	/* surface descriptor */
    int	surface_type, xorder, yorder, order, i;
    double xmin, xmax, ymin, ymax;

    xorder = (int) (fit[TNX_SAVEXORDER] + 0.5);
    if (xorder < 1) {
	fprintf (stderr, "wf_gsrestore: illegal x order %d\n", xorder);
	return (NULL);
	}

    yorder = (int) (fit[TNX_SAVEYORDER] + 0.5);
    if (yorder < 1) {
	fprintf (stderr, "wf_gsrestore: illegal y order %d\n", yorder);
	return (NULL);
	}

    xmin = fit[TNX_SAVEXMIN];
    xmax = fit[TNX_SAVEXMAX];
    if (xmax <= xmin) {
	fprintf (stderr, "wf_gsrestore: illegal x range %f-%f\n",xmin,xmax);
	return (NULL);
	}
    ymin = fit[TNX_SAVEYMIN];
    ymax = fit[TNX_SAVEYMAX];
    if (ymax <= ymin) {
	fprintf (stderr, "wf_gsrestore: illegal y range %f-%f\n",ymin,ymax);
	return (NULL);
	}

    /* Set surface type dependent surface descriptor parameters */
    surface_type = (int) (fit[TNX_SAVETYPE] + 0.5);

    if (surface_type == TNX_LEGENDRE ||
	surface_type == TNX_CHEBYSHEV ||
	surface_type == TNX_POLYNOMIAL) {

	/* allocate space for the surface descriptor */
	sf = (struct IRAFsurface *) malloc (sizeof (struct IRAFsurface));
	sf->xorder = xorder;
	sf->xrange = 2.0 / (xmax - xmin);
	sf->xmaxmin =  - (xmax + xmin) / 2.0;
	sf->yorder = yorder;
	sf->yrange = 2.0 / (ymax - ymin);
	sf->ymaxmin =  - (ymax + ymin) / 2.0;
	sf->xterms = fit[TNX_SAVEXTERMS];
	switch (sf->xterms) {
	    case TNX_XNONE:
		sf->ncoeff = sf->xorder + sf->yorder - 1;
		break;
	    case TNX_XHALF:
		order = MIN (xorder, yorder);
		sf->ncoeff = sf->xorder * sf->yorder - order * (order-1) / 2;
		break;
	    case TNX_XFULL:
		sf->ncoeff = sf->xorder * sf->yorder;
		break;
	    }
	}
    else {
	fprintf (stderr, "wf_gsrestore: unknown surface type %d\n", surface_type);
	return (NULL);
	}

    /* Set remaining curve parameters */
    sf->type = surface_type;

    /* Restore coefficient array */
    sf->coeff = (double *) malloc (sf->ncoeff*sizeof (double));
    for (i = 0; i < sf->ncoeff; i++)
	sf->coeff[i] = fit[TNX_SAVECOEFF+i];

    /* Allocate space for basis vectors */
    sf->xbasis = (double *) malloc (sf->xorder*sizeof (double));
    sf->ybasis = (double *) malloc (sf->yorder*sizeof (double));

    return (sf);
}


/* wf_gsb1pol -- procedure to evaluate all the non-zero polynomial functions
   for a single point and given order. */

static void
wf_gsb1pol (x, order, basis)

double  x;		/*i data point */
int     order;		/*i order of polynomial, order = 1, constant */
double  *basis;		/*o basis functions */
{
    int     i;

    basis[0] = 1.0;
    if (order == 1)
	return;

    basis[1] = x;
    if (order == 2)
	return;

    for (i = 2; i < order; i++)
	basis[i] = x * basis[i-1];

    return;
}


/* wf_gsb1leg -- procedure to evaluate all the non-zero legendre functions for
   a single point and given order. */

static void
wf_gsb1leg (x, order, k1, k2, basis)

double  x;		/*i data point */
int     order;		/*i order of polynomial, order = 1, constant */
double  k1, k2;		/*i normalizing constants */
double	*basis;		/*o basis functions */
{
    int i;
    double ri, xnorm;

    basis[0] = 1.0;
    if (order == 1)
	return;

    xnorm = (x + k1) * k2 ;
    basis[1] = xnorm;
    if (order == 2)
        return;

    for (i = 2; i < order; i++) {
	ri = i;
        basis[i] = ((2.0 * ri - 1.0) * xnorm * basis[i-1] -
                       (ri - 1.0) * basis[i-2]) / ri;
        }

    return;
}


/* wf_gsb1cheb -- procedure to evaluate all the non-zero chebyshev function
   coefficients for a given x and order. */

static void
wf_gsb1cheb (x, order, k1, k2, basis)

double	x;		/*i number of data points */
int	order;		/*i order of polynomial, 1 is a constant */
double	k1, k2;		/*i normalizing constants */
double	*basis;		/*o array of basis functions */
{
    int i;
    double xnorm;

    basis[0] = 1.0;
    if (order == 1)
	return;

    xnorm = (x + k1) * k2;
    basis[1] = xnorm;
    if (order == 2)
	return;

    for (i = 2; i < order; i++)
	basis[i] = 2. * xnorm * basis[i-1] - basis[i-2];

    return;
}

/* Set surface polynomial from arguments */

int
tnxpset (wcs, xorder, yorder, xterms, coeff)

struct WorldCoor *wcs;  /* World coordinate system structure */
int	xorder;		/* Number of x coefficients (same for x and y) */
int	yorder;		/* Number of y coefficients (same for x and y) */
int	xterms;		/* Number of xy coefficients (same for x and y) */
double	*coeff;		/* Plate fit coefficients */

{
    double *ycoeff;
    struct IRAFsurface *wf_gspset ();

    wcs->prjcode = WCS_TNX;

    wcs->lngcor = wf_gspset (xorder, yorder, xterms, coeff);
    ycoeff = coeff + wcs->lngcor->ncoeff;
    wcs->latcor = wf_gspset (xorder, yorder, xterms, ycoeff);

    return 0;
}


/* wf_gspset -- procedure to set the surface descriptor for use by the
   evaluating routines.  from arguments.  The surface parameters are
   surface type, xorder (number of polynomial terms in x), yorder (number
   of polynomial terms in y), xterms, and the surface coefficients.
 */

struct IRAFsurface *
wf_gspset (xorder, yorder, xterms, coeff)

int	xorder;
int	yorder;
int	xterms;
double	*coeff;
{
    struct IRAFsurface	*sf;	/* surface descriptor */
    int	surface_type, order, i;
    double xmin, xmax;
    double ymin, ymax;

    surface_type = TNX_POLYNOMIAL;
    xmin = 0.0;
    xmax = 0.0;
    ymin = 0.0;
    ymax = 0.0;

    if (surface_type == TNX_LEGENDRE ||
	surface_type == TNX_CHEBYSHEV ||
	surface_type == TNX_POLYNOMIAL) {

	/* allocate space for the surface descriptor */
	sf = (struct IRAFsurface *) malloc (sizeof (struct IRAFsurface));
	sf->xorder = xorder;
	sf->xrange = 2.0 / (xmax - xmin);
	sf->xmaxmin =  -(xmax + xmin) / 2.0;
	sf->yorder = yorder;
	sf->yrange = 2.0 / (ymax - ymin);
	sf->ymaxmin =  - (ymax + ymin) / 2.0;
	sf->xterms = xterms;
	switch (sf->xterms) {
	    case TNX_XNONE:
		sf->ncoeff = sf->xorder + sf->yorder - 1;
		break;
	    case TNX_XHALF:
		order = MIN (xorder, yorder);
		sf->ncoeff = sf->xorder * sf->yorder - order * (order-1) / 2;
		break;
	    case TNX_XFULL:
		sf->ncoeff = sf->xorder * sf->yorder;
		break;
	    }
	}
    else {
	fprintf (stderr, "TNX_GSSET: unknown surface type %d\n", surface_type);
	return (NULL);
	}

    /* Set remaining curve parameters */
    sf->type = surface_type;

    /* Restore coefficient array */
    sf->coeff = (double *) malloc (sf->ncoeff*sizeof (double));
    for (i = 0; i < sf->ncoeff; i++)
	sf->coeff[i] = coeff[i];

    /* Allocate space for basis vectors */
    sf->xbasis = (double *) malloc (sf->xorder*sizeof (double));
    sf->ybasis = (double *) malloc (sf->yorder*sizeof (double));

    return (sf);
}

/* Mar 26 1998	New subroutines, translated from SPP
 * Apr 28 1998  Change all local flags to TNX_* and projection flag to WCS_TNX
 * May 11 1998	Fix use of pole longitude default
 * Sep  4 1998	Fix missed assignment in tnxpos from Allen Harris, SAO
 * Sep 10 1998	Fix bugs in tnxpix()
 * Sep 10 1998	Fix missed assignment in tnxpix from Allen Harris, SAO
 *
 * Oct 22 1999	Drop unused variables, fix case statements after lint
 * Dec 10 1999	Fix bug in gsder() which failed to allocate enough memory
 * Dec 10 1999	Compute wcs->rot using wcsrotset() in tnxinit()
 *
 * Feb 14 2001	Fixed off-by-one bug in legendre evaluation (Mike Jarvis)
 *
 * Apr 11 2002	Fix bug when .-terminated substring in wf_gsopen()
 * Apr 29 2002	Clean up code
 * Jun 26 2002	Increase size of WAT strings from 500 to 2000
 *
 * Jun 27 2005	Drop unused arguments k1 and k2 from wf_gsb1pol()
 *
 * Jan  8 2007	Drop unused variable ncoeff in wf_gsder()
 * Jan  9 2007	Declare header const char in tnxinit()
 * Apr  3 2007	Fix offsets to hit last cooefficient in wf_gsder()
 *
 * Sep  5 2008	Fix wf_gseval() call in tnxpos() so unmodified x and y are used
 * Sep  9 2008	Fix loop in TNX_XFULL section of wf_gsder()
 * 		(last two bugs found by Ed Los)
 * Sep 17 2008	Fix tnxpos for null correction case (fix by Ed Los)
 */
