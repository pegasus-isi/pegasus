/*** File libwcs/findstar.c
 *** October 19, 2007
 *** By Doug Mink, after Elwood Downey
 *** Copyright (C) 1996-2007
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
#include "wcs.h"
#include "wcscat.h"
#include "lwcs.h"

#define ABS(a) ((a) < 0 ? (-(a)) : (a))

static int HotPixel();
static int starRadius();
static void starCentroid();
static int BrightWalk ();
static double FindFlux ();
static void mean2d();
static void mean1d();
static void rotstars();
extern void setminmatch();
extern void setnitmax();
extern void setminstars();
extern void setminpmqual();
extern void setminid();
extern void setnxydec();

/* Set input catalog for image stars */
static char imcatname[256] = "";
void setimcat (cat)
char *cat;
{strcpy (imcatname, cat); return; }

/* Get input catalog for image stars */
char *getimcat ()
{return (imcatname); }

static int nspix = NSTATPIX;	/* Stats are computed for +- this many pixels */
void setnspix (nsp)
int nsp;
{ nspix = nsp; return; }

static int ispix = ISTATPIX;	/* Stats are computed every this many pixels */
void setispix (isp)
int isp;
{ ispix = isp; return; }

static int maxw = MAXWALK;	/* Farthest distance to walk from seed */
void setmaxwalk (wmax)
int wmax;
{ maxw = wmax; return; }

static double burnedout = BURNEDOUT; /* Clamp pixels brighter than this */
void setburnedout (bmax)
double bmax;
{ burnedout = bmax; return; }

static int niterate = NITERATE;	/* Number of iterations for sigma clipping */
void setniterate (nit)
int nit;
{ niterate = nit; return;}

/* Stars must be at least this many standard deviations above the mean */
static double starsig = STARSIGMA;
void setstarsig (sig)
double sig;
{ starsig = sig; return; }

static int fsborder = BORDER;	/* Ignore this much of the edge */
void setborder (brd)
int brd;
{ fsborder = brd; return; }

static int rnoise = RNOISE;	/* Mean noise is from center +- this many pixels */
void setrnoise (rn)
int rn;
{ rnoise = rn; return; }

static int maxrad = MAXRAD;	/* Maximum radius for a star */
void setmaxrad (rmax)
int rmax;
{ maxrad = rmax; return; }

static int minrad = MINRAD;	/* Minimum radius for a star */
void setminrad (rmin)
int rmin;
{ minrad = rmin; return; }

static double bmin = MINPEAK;	/* Minimum peak for a star */
void setbmin (min)
double min;
{ bmin = min; return; }

static int minsep = MINSEP;	/* Minimum separation for stars */
void setminsep (smin)
int smin;
{ minsep = smin; return; }

static int mirror = 0;
void setmirror (mirror1)
int mirror1;
{ mirror = mirror1; return;}

static int rotate = 0;
void setrotate (rotate1)
int rotate1;
{ rotate = rotate1; return;}


/* Find the location and brightest pixel of stars in the given image.
 * Return malloced arrays of x and y and b.
 * N.B. Caller must free *xa and *ya and *ba even if 0 stars are returned.
 * N.B. Pixels outside fsborder are ignored.
 * N.B. Isolated hot pixels are ignored.
 * return number of stars (might well be 0 :-), or -1 if trouble.
 */

int
FindStars (header, image, xa, ya, ba, pa, verbose, zap)

char	*header;	/* FITS header */
char	*image;		/* image pixels */
double	**xa, **ya;	/* X and Y coordinates of stars, array returned */
double	**ba;		/* Fluxes of stars in counts, array returned */
int	**pa;		/* Peak counts of stars in counts, array returned */
int	verbose;	/* 1 to print each star's position */
int	zap;		/* If 1, set star to background after reading */

{
    double noise, nsigma;
    int nstars;
    double minll;
    int bitpix;
    int w, h, ilp, irp, i, idx, idy;
    int x, y, x1, x2, y1, y2;
    double xai, yai, bai;
    double minsig, sigma;
    double *svec, *svb, *sv, *sv1, *sv2, *svlim;
    double rmax;
    double bz, bs;		/* Pixel value scaling */
    int *ixa, *iya;
    int lwidth;
    int nextline;
    int xborder1, xborder2, yborder1, yborder2;
    char trimsec[32];
    int nstarmax = 100;
    extern void setscale();

    hgeti4 (header,"NAXIS1", &w);
    hgeti4 (header,"NAXIS2", &h);
    hgeti4 (header,"BITPIX", &bitpix);
    bz = 0.0;
    hgetr8 (header,"BZERO", &bz);
    bs = 1.0;
    hgetr8 (header,"BSCALE", &bs);
    if (bz == 0.0 && bs == 1.0)
	setscale (0);

    /* Allocate the position, flux, and peak intensity arrays
     * it's ok to do now because we claim caller should always free these.
     */
    *xa = (double *) calloc (nstarmax, sizeof(double));
    *ya = (double *) calloc (nstarmax, sizeof(double));
    *ba = (double *) calloc (nstarmax, sizeof(double));
    *pa = (int *) calloc (nstarmax, sizeof(int));
    ixa = (int *) calloc (nstarmax, sizeof (int));
    iya = (int *) calloc (nstarmax, sizeof (int));

    /* Read star list from file */
    if (imcatname[0] != 0) {
	int nlog = 0;
	if (verbose) nlog = 10;
	if (istab (imcatname))
	    nstars = tabxyread (imcatname, xa, ya, ba, pa, nlog);
	else
	    nstars = daoread (imcatname, xa, ya, ba, pa, nlog);
	if (rotate != 0 || mirror)
	    rotstars (nstars, *xa, *ya, w, h);
	return (nstars);
	}

    /* Set trim section for star searching */
    if (hgets (header, "TRIMSEC", 32, trimsec)) {
	char *tx1, *tx2, *tx3, *tx4, *tx5;
	tx1 = trimsec + 1;
	tx2 = strchr (trimsec, ':');
	*tx2 = (char) 0;
	xborder1 = atoi (tx1+1);
	tx2 = tx2 + 1;
	tx3 = strchr (tx2, ',');
	*tx3 = (char) 0;
	xborder2 = w - atoi (tx2);
	tx3 = tx3 + 1;
	tx4 = strchr (tx3, ':');
	*tx4 = (char) 0;
	yborder1 = atoi (tx3);
	tx4= tx4 + 1;
	tx5 = strchr (tx4, ']');
	*tx5 = (char) 0;
	yborder2 = atoi (tx4) - h;
	}
    else {
	xborder1 = fsborder;
	xborder2 = fsborder;
	yborder1 = fsborder;
	yborder2 = fsborder;
	}

    /* Allocate a buffer to hold one image line */
    svec = (double *) malloc (w * sizeof (double));

    /* Compute image noise from a central swath */
    x1 = (w / 2) - rnoise;
    if (x1 < 1)
	x1 = 1;
    x2 = (w / 2) + rnoise;
    if (x2 > w)
	x2 = w;
    y1 = (h / 2) - rnoise;
    if (y1 < 1)
	y1 = 1;
    y2 = (h / 2) + rnoise;
    if (y2 > h)
	y2 = h;
    mean2d (image,bitpix,w,h,bz,bs, x1, x2, y1, y2, &noise, &nsigma);
    if (verbose)
	fprintf (stderr, "FindStar mean is %.2f, sigma is %.2f\n",
		 noise, nsigma);

    /* Fill in borders of the image line buffer with noise */
    svlim = svec + w;
    svb = svec + xborder1;
    for (sv = svec; sv < svb; sv++)
	*sv = noise;
    for (sv = svlim - xborder2; sv < svlim; sv++)
	*sv = noise;
    if (verbose) {
	fprintf (stderr, "FindStar x=1-%d, %d-%d set to noise\n",
		 xborder1, w-xborder2+1, w);
	fprintf (stderr, "FindStar y=1-%d, %d-%d set to noise\n",
		 yborder1, h-yborder2+1, h);
	}
    if (bmin > 0)
	minll = noise + bmin;
    else
	minll = noise + (starsig * nsigma);
    sigma = sqrt (minll);
    if (nsigma < sigma)
	minsig = sigma;
    else
	minsig = nsigma;

    /* Scan for stars based on surrounding local noise figure */
    nstars = 0;
    lwidth = w - xborder2 - xborder1 + 1;
    for (y = yborder1; y < h-yborder1; y++) {
        int ipix = 0;

	/* Get one line of the image minus the noise-filled borders */
	nextline = (w * (y-1)) + xborder1 - 1;
	getvec (image, bitpix, bz, bs, nextline, lwidth, svb);
	if (verbose)
	    fprintf (stderr, "Row %5d Col     0:\r", y+1);

	/* Search row for bright pixels */
	for (x = xborder1; x < w-xborder2; x++) {

	    if (verbose && x%100 == 0)
		fprintf (stderr, "Row %5d Col %5d:\r", y+1, x+1);

	    /* Redo stats once for every several pixels */
	    if (ispix > 0 && nspix > 0 && ipix++ % ispix == 0) {

		/* Find stats to the left */
		ilp = x - (nspix / 2);
		if (ilp < 0)
		    ilp = 0;
		sv1 = svec + ilp;
		irp = ilp + nspix;
		if (irp < w)
		    sv2 = svec + irp;
		else
		    sv2 = svlim;
		minsig = 0.0;
		if (sv2 > sv1+1)
		    mean1d (sv1, sv2, &noise, &minsig);
		sigma = sqrt (noise);
		if (minsig < sigma)
		    minsig = sigma;
		minll = noise + (starsig * minsig);
		}

	    /* Pixel is a candidate if above the noise */
	    if (svec[x] > minll) {
		int sx, sy, r, rf;
		double b;
		int i;

		/* Ignore faint stars */
		if (svec[x] < bmin)
		    continue;

		/* Ignore hot pixels */
		if (!HotPixel (image,bitpix,w,h,bz,bs, x, y, minll))
		    continue;

		/* Walkabout to find brightest pixel in neighborhood */
		if (BrightWalk (image,bitpix,w,h,bz,bs,x,y,maxw,&sx,&sy,&b) < 0)
		    continue;

		/* Ignore really bright stars */
		if (burnedout > 0 && b >= burnedout)
		    continue;

		/* Skip star if already in list */
		for (i = 0; i < nstars; i++) {
		    idy = iya[i] - sy;
		    if (idy < 0)
			idy = -idy;
		    if (idy <= minsep) {
			idx = ixa[i] - sx;
			if (idx < 0)
			    idx = -idx;
			if (idx <= minsep)
			    break;
			}
		    }
		if (i < nstars)
		    continue;

		/* Keep it if it is within the size range for stars */
		rmax = maxrad;
		r = starRadius (image,bitpix,w,h,bz,bs, sx, sy, rmax,
				minsig, noise);
		if (r > minrad && r <= maxrad) {

		/* Centroid star */
		    nstars++;
		    if (nstars > nstarmax) {
			nstarmax = nstarmax * 2;
			*xa= (double *) realloc(*xa, nstarmax*sizeof(double));
			*ya= (double *) realloc(*ya, nstarmax*sizeof(double));
			ixa= (int *) realloc(ixa, nstarmax*sizeof(int));
			iya= (int *) realloc(iya, nstarmax*sizeof(int));
			*ba= (double *) realloc(*ba, nstarmax*sizeof(double));
			*pa= (int *) realloc(*pa, nstarmax*sizeof(int));
			}
		    starCentroid (image,bitpix,w,h,bz,bs, sx, sy, &xai, &yai); 
		    (*xa)[nstars-1] = xai;
		    (*ya)[nstars-1] = yai;
		    ixa[nstars-1] = (int) (xai + 0.5);
		    iya[nstars-1] = (int) (yai + 0.5);
		    (*pa)[nstars-1] = (int) b;

		/* Find radius of star for photometry */
		/* Outermost 1-pixel radial band is one sigma above background */
		    sx = (int) (xai + 0.5);
		    sy = (int) (yai + 0.5);
		    rmax = 2.0 * (double) maxrad;
		    rf = starRadius (image,bitpix,w,h,bz,bs, sx, sy, rmax,
				    minsig, noise);

		/* Find flux from star */
		    bai = FindFlux (image,bitpix,w,h,bz,bs,sx,sy,rf,noise,zap);
		    (*ba)[nstars-1] = bai;
		    if (verbose) {
			fprintf (stderr, "Row %5d Col %5d: ", y+1, x+1);
			fprintf (stderr," %d: (%d %d) -> (%7.3f %7.3f)",
				 nstars, sx, sy, xai, yai);
			fprintf (stderr," %8.1f -> %10.1f  %d -> %d    ",
				 b, bai, r, rf);
			(void)putc (13,stderr);
			}
		    }
		/* else {
		    fprintf (stderr," %d: (%d %d) %d > %d\n",
			     nstars, sx, sy, r, maxrad);
		    } */
		}
	    }
	}

    /* Turn fluxes into instrument magnitudes */
    (void) FluxSortStars (*xa, *ya, *ba, *pa, nstars);
    if (nstars > 0) {
	double *flux;
	for (i = 0; i < nstars; i++) {
	    flux = (*ba)+i;
	    *flux = -2.5 * log10 (*flux);
	    }
	}

    free ((char *)svec);
    return (nstars);
}


/* Check pixel at x/y for being "hot", ie, a pixel surrounded by noise.
 * If any are greater than pixel at x/y then return -1.
 * Else set the pixel at x/y to llimit and return 0.
 */

static int
HotPixel (image, bitpix, w, h, bz, bs, x, y, llimit)

char	*image;		/* Image array origin pointer */
int	bitpix;		/* Bits per pixel, negative for floating point or unsigned int */
int	w;		/* Image width in pixels */
int	h;		/* Image height in pixels */
double	bz;		/* Zero point for pixel scaling */
double	bs;		/* Scale factor for pixel scaling */
int	x, y;
double	llimit;

{
    double pix1, pix2, pix3;

    /* Check for hot row */
    pix1 = getpix (image,bitpix,w,h,bz,bs,x-1,y-1);
    pix2 = getpix (image,bitpix,w,h,bz,bs,x,y-1);
    pix3 = getpix (image,bitpix,w,h,bz,bs,x+1,y-1);
    if (pix1 > llimit || pix2 > llimit || pix3 > llimit)
	return (-1);
    pix1 = getpix (image,bitpix,w,h,bz,bs,x-1,y+1);
    pix2 = getpix (image,bitpix,w,h,bz,bs,x,y+1);
    pix3 = getpix (image,bitpix,w,h,bz,bs,x+1,y+1);
    if (pix1 > llimit || pix2 > llimit || pix3 > llimit)
	return (-1);

    /* Check for hot column */
    pix1 = getpix (image,bitpix,w,h,bz,bs,x-1,y-1);
    pix2 = getpix (image,bitpix,w,h,bz,bs,x-1,y);
    pix3 = getpix (image,bitpix,w,h,bz,bs,x-1,y+1);
    if (pix1 > llimit || pix2 > llimit || pix3 > llimit)
	return (-1);
    pix1 = getpix (image,bitpix,w,h,bz,bs,x+1,y-1);
    pix2 = getpix (image,bitpix,w,h,bz,bs,x+1,y);
    pix3 = getpix (image,bitpix,w,h,bz,bs,x+1,y+1);
    if (pix1 > llimit || pix2 > llimit || pix3 > llimit)
	return (-1);

    /* Check for hot pixel */
    pix1 = getpix (image,bitpix,w,h,bz,bs,x-1,y);
    pix3 = getpix (image,bitpix,w,h,bz,bs,x+1,y);
    if (pix1 > llimit || pix3 > llimit)
	return (-1);
    pix1 = getpix (image,bitpix,w,h,bz,bs,x,y-1);
    pix3 = getpix (image,bitpix,w,h,bz,bs,x,y+1);
    if (pix1 > llimit || pix3 > llimit)
	return (-1);

    putpix (image, bitpix, w, h, bz, bs, x, y, llimit);
    return (0);
}


/* Compute and return the radius of the star centered at x0, y0.
 * A guard band is assumed to exist on the image.
 * Calling program is assumed to reject object if r > rmax.
 */

static int
starRadius (imp, bitpix, w, h, bz, bs, x0, y0, rmax, minsig, background)

char	*imp;		/* Image array origin pointer */
int	bitpix;		/* Bits per pixel, negative for floating point or unsigned int */
int	w;		/* Image width in pixels */
int	h;		/* Image height in pixels */
double	bz;		/* Zero point for pixel scaling */
double	bs;		/* Scale factor for pixel scaling */
int	x0, y0;		/* Coordinates of center pixel of star */
double	rmax;		/* Maximum allowable radius of star */
double	minsig;		/* Minimum level for signal */
double	background;	/* Mean background level */

{
    int r, irmax;
    double dp, sum, mean;
    int xyrr, yrr, np;
    int inrr, outrr;
    int x, y;
    irmax = (int) rmax;

    /* Compute star's radius.
     * Scan in ever-greater circles until find one such that the mean at
     * that radius is less than one sigma above the background level.
     */
    for (r = 2; r <= irmax; r++) {
	inrr = r*r;
	outrr = (r+1)*(r+1);
	np = 0;
	sum = 0.0;

	for (y = -r; y <= r; y++) { 
	    yrr = y*y;
	    for (x = -r; x <= r; x++) {
		xyrr = x*x + yrr;
		if (xyrr >= inrr && xyrr < outrr) {
		    dp = getpix (imp,bitpix,w,h,bz,bs,x0+x,y0+y);
		    sum += dp;
		    np++;
		    }
		}
	    }

	mean = (sum / np) - background;
	if (mean < minsig)
	    break;
	}

    return (r);
}

/* Compute the fine location of the star peaking at [x0,y0] */

static void
starCentroid (imp, bitpix, w, h, bz, bs, x0, y0, xp, yp)

char	*imp;
int	bitpix;
int	w;
int	h;
double	bz;		/* Zero point for pixel scaling */
double	bs;		/* Scale factor for pixel scaling */
int	x0, y0;
double	*xp, *yp;

{
    double p1, p2, p22, p3, d;

    /* Find maximum of best-fit parabola in each direction.
     * see Bevington, page 210
     */

    p1 = getpix (imp,bitpix,w,h,bz,bs,x0-1,y0);
    p2 = getpix (imp,bitpix,w,h,bz,bs,x0,y0);
    p22 = 2*p2;
    p3 = getpix (imp,bitpix,w,h,bz,bs,x0+1,y0);
    d = p3 - p22 + p1;
    *xp = (d == 0) ? x0 : x0 + 0.5 - (p3 - p2)/d;
    *xp = *xp + 1.0;

    p1 = getpix (imp,bitpix,w,h,bz,bs,x0,y0-1);
    p3 = getpix (imp,bitpix,w,h,bz,bs,x0,y0+1);
    d = p3 - p22 + p1;
    *yp = (d == 0) ? y0 : y0 + 0.5 - (p3 - p2)/d;
    *yp = *yp + 1.0;
}


/* Given an image and a starting point, walk the gradient to the brightest
 * pixel and return its location, never going more than maxrad away.
 * Return 0 if brightest pixel found within maxsteps, else -1 */

static int dx[8]={1,0,-1,1,-1,1,0,-1};
static int dy[8]={1,1,1,0,0,-1,-1,-1};

static int
BrightWalk (image, bitpix, w, h, bz, bs, x0, y0, maxr, xp, yp, bp)

char	*image;
int	bitpix;
int	w;
int	h;
double	bz;		/* Zero point for pixel scaling */
double	bs;		/* Scale factor for pixel scaling */
int	x0;
int	y0;
int	maxr;
int	*xp;
int	*yp;
double	*bp;

{

    double b, tmpb, newb;
    int x, y, x1, y1, i, xa, ya;

    /* start by assuming seed point is brightest */
    b = getpix (image,bitpix,w,h,bz,bs, x0,y0);
    x = x0;
    y = y0;
    xa = x0;
    ya = y0;

    /* walk towards any brighter pixel */
    for (;;) {
	int newx = 0;
	int newy = 0;

	/* Find brightest pixel in 3x3 region */
	newb = b;
	for (i = 0; i < 8; i++) {
	    x1 = x + dx[i];
	    y1 = y + dy[i];
	    tmpb = getpix (image,bitpix,w,h,bz,bs, x1, y1);
	    if (tmpb >= newb) {
		if (x1 == xa && y1 == ya)
		    break;
		xa = x;
		ya = y;
		newx = x1;
		newy = y1;
		newb = tmpb;
		}
	    }
 
	/* If brightest pixel is one in center of region, quit */
	if (newb == b)
	    break;

	/* Otherwise, set brightest pixel to new center */
	x = newx;
	y = newy;
	b = newb;
	if (abs(x-x0) > maxr || abs(y-y0) > maxr)
	    return (-1);
	}

    *xp = x;
    *yp = y;
    *bp = b;
    return (0);
}

/* Compute stats in the give region of the image of width w pixels.
 * Bounds are not checked.
 */

static void
mean2d (image, bitpix, w, h, bz, bs, x1, x2, y1, y2, mean, sigma)

char	*image;
int	bitpix;
int	w;
int	h;
double	bz;		/* Zero point for pixel scaling */
double	bs;		/* Scale factor for pixel scaling */
int	x1,x2;
int	y1, y2;
double	*mean;
double	*sigma;

{
    double p, pmin, pmax;
    double pmean = 0.0;
    double sd = 0.0;
    int x, y;
    int i;
    double sum;
    double dnpix;
    int npix;

    pmin = -1.0e20;
    pmax = 1.0e20;

    for (i = 0; i < niterate; i++ ) {
	sum = 0.0;
	npix = 0;

    /* Compute mean */
	if (i == 0) {
	    for (y = y1; y < y2; y++) {
		for (x = x1; x < x2; x++) {
		    p = getpix (image,bitpix,w,h,bz,bs, x, y);
		    sum += p;
		    npix++;
		    }
		}
	    }
	else {
	    for (y = y1; y < y2; y++) {
		for (x = x1; x < x2; x++) {
		    p = getpix (image,bitpix,w,h,bz,bs, x, y);
		    if (p > pmin && p < pmax) {
			sum += p;
			npix++;
			}
		    }
		}
	    }
	dnpix = (double) npix;
	pmean = sum / dnpix;

    /* Compute average deviation */
	npix = 0;
	sum = 0.0;
	for (y = y1; y < y2; y++) {
	    for (x = x1; x < x2; x++) {
		p = getpix (image,bitpix,w,h,bz,bs, x, y);
		if (p > pmin && p < pmax) {
		    sum += fabs (p - pmean);
		    npix++;
		    }
		}
	    }

	dnpix = (double) npix;
	if (npix > 0)
	    sd = sum / dnpix;
	else
	    sd = 0.0;
	pmin = pmean - sd * starsig;
	pmax = pmean + sd * starsig;
	}

    *mean = pmean;
    *sigma = sd;
    return;
}


static void
mean1d (sv1, sv2, mean, sigma)

double *sv1, *sv2;	/* starting and ending pixels for statistics */
double *mean;		/* Mean value of pixels (returned) */
double *sigma;		/* Average deviation of pixels (returned) */
{
    double *sv;
    double p, pmin, pmax;
    double pmean = 0.0;
    double sd = 0.0;
    int i;
    int npix;
    double dnpix;
    double sum;

    pmin = -1.0e20;
    pmax = 1.0e20;

    /* Iterate with sigma-clipping */
    for (i = 0; i < niterate; i++ ) {
	npix = 0;
	sum = 0.0;

	/* Compute mean */
	for (sv = sv1; sv < sv2; sv++) {
	    p = *sv;
	    if (p > pmin && p < pmax) {
		sum += p;
		npix++;
		}
	    }
	if (npix > 0) {
	    dnpix = (double) npix;
	    pmean = sum / dnpix;
	    }
	else
	    pmean = 0.0;

	/* Compute average deviation */
	npix = 0;
	sum = 0.0;
	for (sv = sv1; sv < sv2; sv++) {
	    p = *sv;
	    if (p > pmin && p < pmax) {
		sum += fabs (p - pmean);
		npix++;
		}
	    }
	if (npix > 0)
	    sd = sum / dnpix;
	else
	    sd = 0.0;
	pmin = pmean - (sd * starsig);
	pmax = pmean + (sd * starsig);
	}
    *mean = pmean;
    *sigma = sd;
    return;
}


/* Find total flux within a circular region minus a mean background level */

static double
FindFlux (image, bitpix, w, h, bz, bs, x0, y0, r, background, zap)

char	*image;
int	bitpix;
int	w;
int	h;
double	bz;		/* Zero point for pixel scaling */
double	bs;		/* Scale factor for pixel scaling */
int	x0;
int	y0;
int	r;
double	background;	/* Background level (subtracted for flux) */
int	zap;		/* If 1, set star to background after reading */
{
    double sum = 0.0;
    int x, y, x1, x2, y1, y2, yy, xxyy, xi, yi;
    int rr = r * r;
    double dp;

/* Keep X within image */
    x1 = -r;
    if (x0-r < 0)
	x1 = 0;
    x2 = r;
    if (x0+r > 0)
	x2 = w;

/* Keep Y within image */
    y1 = -r;
    if (y0-r < 0)
	y1 = 0;
    y2 = r;
    if (y0+r > 0)
	y2 = h;

/* Integrate circular region around a star */
    for (y = y1; y <= y2; y++) { 
	yy = y*y;
	for (x = x1; x <= x2; x++) {
	    xxyy = x*x + yy;
	    if (xxyy <= rr) {
		xi = x0 + x;
		yi = y0 + x;
		dp = getpix (image, bitpix, w,h,bz,bs, xi, yi);
		if (dp > background) {
		    sum += dp - background;
		    if (zap)
		        putpix (image, bitpix, w,h,bz,bs, xi, yi,background);
		    }
		}
	    }
	}

    return (sum);
}


/* Reset parameter values from the command line */
void
setparm (parstring)
char *parstring;
{
    char *parname;
    char *parvalue;

    parname = parstring;
    if ((parvalue = strchr (parname,'=')) == NULL)
	return;
    *parvalue = (char) 0;
    parvalue++;
    if (!strcmp (parname, "nstatpix") ||
	!strcmp (parname, "nspix"))
	setnspix (atoi (parvalue));
    else if (!strcmp (parname, "istatpix") ||
	!strcmp (parname, "ispix"))
	setispix (atoi (parvalue));
    else if (!strcmp (parname, "niterate") ||
	!strcmp (parname, "niter"))
	setniterate (atoi (parvalue));
    else if (!strcmp (parname, "border"))
	setborder (atoi (parvalue));
    else if (!strcmp (parname, "maxrad"))
	setmaxrad (atoi (parvalue));
    else if (!strcmp (parname, "minrad"))
	setminrad (atoi (parvalue));
    else if (!strcmp (parname, "starsig"))
	setstarsig (atof (parvalue));
    else if (!strcmp (parname, "maxwalk"))
	setmaxwalk (atoi (parvalue));
    else if (!strcmp (parname, "minsep"))
	setminsep (atoi (parvalue));
    else if (!strcmp (parname, "minpeak"))
	setbmin (atof (parvalue));
    else if (!strcmp (parname, "minmatch"))
	setminmatch ((int) atof (parvalue));
    else if (!strcmp (parname, "nmax"))
	setnitmax ((int) atof (parvalue));
    else if (!strcmp (parname, "nitmax"))
	setnitmax ((int) atof (parvalue));
    else if (!strcmp (parname, "minstars"))
	setminstars ((int) atof (parvalue));
    else if (!strcmp (parname, "minpmqual"))
	setminpmqual ((int) atof (parvalue));
    else if (!strcmp (parname, "minid"))
	setminid ((int) atof (parvalue));
    else if (!strcmp (parname, "nxydec"))
	setnxydec ((int) atof (parvalue));
    else if (!strcmp (parname, "rnoise"))
	setrnoise ((int) atof (parvalue));
    return;
}

static void
rotstars (nstars, xa, ya, w, h)

int	nstars;	/* Number of stars found in image */
double	*xa;	/* X coordinates of stars */
double	*ya;	/* Y coordinates of stars */
int	w;	/* Original width of image */
int	h;	/* Original height of image */

{
    int istar;
    void rotstar();

    if (rotate == 1)
	rotate = 90;
    else if (rotate == 2)
	rotate = 180;
    else if (rotate == 3)
	rotate = 270;
    else if (rotate < 0)
	rotate = rotate + 360;
    else if (rotate > 360)
	rotate = rotate - 360;

    /* Rotate star postions one at a time */
    for (istar = 0; istar < nstars; istar++)
	rotstar (&xa[istar], &ya[istar], w, h);

    return;
}

void
rotstar (x, y, w, h)

double	*x;	/* X coordinates of stars */
double	*y;	/* Y coordinates of stars */
int	w;	/* Original width of image */
int	h;	/* Original height of image */

{
    double x1, y1, x2, y2;
    double xn = (double) w;
    double yn = (double) h;
    int reflect=mirror;		/* 1 if image is reflected, else 0 */

    x1 = *x;
    y1 = *y;
    x2 = x1;
    y2 = y1;

    /* Rotate star postions one at a time */

    /* Mirror coordinates without rotation */
    if (rotate < 45.0 && rotate > -45.0) {
	if (reflect == 1)
	    x2 = xn - x1 - 1.0;
	else if (reflect == 2)
	    y2 = yn - y1 - 1.0;
	}

    /* Rotate by 90 degrees */
    else if (rotate >= 45 && rotate < 135) {
	if (reflect == 1) {
	    x2 = yn - y1 - 1.0;
	    y2 = xn - x1 - 1.0;
	    }
	else if (reflect == 2) {
	    x2 = y1;
	    y2 = x1;
	    }
	else {
	    x2 = yn - y1 - 1.0;
	    y2 = x1;
	    }
	}

    /* Rotate by 180 degrees */
    else if (rotate >= 135 && rotate < 225) {
	if (reflect == 1)
	    y2 = yn - y1 - 1.0;
	else if (reflect == 2)
	    x2 = xn - x1 - 1.0;
	else {
	    x2 = xn - x1 - 1.0;
	    y2 = yn - y1 - 1.0;
	    }
	}

    /* Rotate by 270 degrees */
    else if (rotate >= 225 && rotate < 315) {
	if (reflect == 1) {
	    x2 = y1;
	    y2 = x1;
	    }
	else if (reflect == 2) {
	    x2 = yn - y1 - 1.0;
	    y2 = xn - x1 - 1.0;
	    }
	else {
	    x2 = y1;
	    y2 = xn - x1 - 1.0;
	    }
	}

    /* If rotating by more than 315 degrees, flip across both axes */
    else if (rotate >= 315 && mirror) {
	x2 = y1;
	y2 = x1;
	}

    *x = x2;
    *y = y2;

    return;
}

/* May 21 1996	Return peak flux in counts
 * May 22 1996	Add arguments so GETPIX and PUTPIX can check coordinates
 * Jun  6 1996	Change name from findStars to FindStars
 * Jun 12 1996	Remove unused variables after using lint
 * Jun 13 1996	Removed leftover free of image
 * Aug  6 1996	Fixed small defects after lint
 * Aug 26 1996	Drop unused variables NH and NW
 * Aug 30 1996	Modify sigma computation; allow border to be set
 * Sep  1 1996	Set constants in lwcs.h
 * Oct 15 1996	Drop unused variables
 * Dec 10 1996	Check for hot columns as well as hot rows
 * Dec 10 1996	Add option to read image stars from DAOFIND file
 *
 * Mar 20 1997	Declare external subroutine DAOREAD
 * Nov  6 1997	Add subroutine to return image catalog filename
 * Dec 15 1997	Change calls to ABS to FABS when doubles are involved
 *
 * May 27 1998	Include imio.h
 * Jul 30 1998	Deal with too-small sigmas
 *
 * Feb  4 1999	Keep overexposed (pixel value > BURNEDOUT) stars
 * Apr 26 1999	Fix Bright Walk() to slide along bleeding rows or columns
 * Apr 28 1999	Add scaling to getpix and getvec
 * Jun 11 1999	Add parameter setting subroutine
 * Oct 21 1999	Drop unused variables and fix sigma usage after lint
 * Oct 25 1999	Fix mean loop to avoid bad pointer creation
 * Oct 29 1999	Read image star positions from tab table or DAOPHOT table
 * Nov 23 1999	Lengthen imcatname from 32 to 256 for long pathnames
 *
 * Mar 27 2000	Drop unused variable imtab
 *
 * Jan 18 2001	Use trim section from image header, if not trimmed
 * Jul 25 2001	Return plate magnitudes instead of fluxes
 * Oct 31 2001	Add minmatch from matchstar.c to setparm() options
 * Nov  6 2001	Add setnitmax() to setparm()
 * Nov  7 2001	Add setminstars() to setparm()
 * Dec 19 2001	Add setmirror() and setrotate() to rotate input catalog
 *
 * Jan 23 2002	If zap, set pixels in star to background after adding up flux
 * Jan 23 2002	Skip recomputation of noise if istat is zero
 * Jan 23 2002	Set scale flag if BSCALE and BZERO not used
 * May 13 2002	Fix bugs found by lint
 *
 * Jan 23 2003	Add setminpmqual() to setparm() for USNO-B1.0
 * Jan 29 2003	Add setminid() to setparm() for USNO-B1.0
 * Apr  3 2003	Fix bug setting minll if bmin is less than 0
 * Jun  2 2003	Fix bug to setcale(0) if bscale == 1, not 0 (J-B Marquette)
 *
 * Aug  3 2004	Move single star image position rotation into rotstar()
 * Aug  3 2004	Move daoread() declaration to wcscat.h
 * Sep 24 2004	Fix rotstar() to separate output values from input values
 *
 * Mar 30 2006	Add nxydec=num. decimal places in image coordinates to setparm()
 * Apr 25 2006	Change MINPEAK to mean counts above noise background
 *              Suggested by Hill & Biddick for high background situations
 * Jun 19 2006	Initialized uninitialized variables
 * Oct 24 2006	Add reflection across horizontal as well as vertical axis
 *
 * Jan  8 2007	Include fitsfile.h instead of fitshead.h and imio.h
 * Jan  8 2007	Drop unused variables
 * Jan 10 2007	Include wcs.h
 * Oct 19 2007	Fix pointers in trim section processing
 */
