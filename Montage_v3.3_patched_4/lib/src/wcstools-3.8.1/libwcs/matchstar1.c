/*** File libwcs/matchstar.c
 *** June 19, 2006
 *** By Doug Mink, dmink@cfa.harvard.edu
 *** Harvard-Smithsonian Center for Astrophysics
 *** Copyright (C) 1996-2006
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

/* StarMatch (ns, sx, sy, ng, gra, gdec, goff, gx, gy, tol, wcs, nfit, debug)
 *  Find shift, scale, and rotation of image stars to best-match reference stars
 *
 * ReadMatch (filename, sx, sy, gra, gdec, debug)
 *  Read in x, y, RA, and Dec of pre-match stars in image
 *
 * WCSMatch (nmatch, sbx, sby, gbra, gbdec, debug)
 *  Find shift, scale, and rotation of image stars to best-match reference stars
 *
 * FitMatch (ns, sx, sy, ng, gra, gdec, gx, gy, tol, wcs, nfit, debug)
 *  Fit shift, scale, and rotation of image stars to RA/Dec/X/Y matches
 *
 * wcs_amoeba (wcs0) Set up temp arrays and call multivariate solver
 * chisqr (v) Compute the chisqr of the vector v
 * amoeba (p, y, ndim, ftol, itmax, funk, nfunk)
 *    Multivariate solver from Numerical Recipes
 */

#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <string.h>
#include "wcs.h"
#include "lwcs.h"
#include "wcscat.h"

#define NPAR 8
#define NPAR1 9

#define ABS(a) ((a) < 0 ? (-(a)) : (a))

static void wcs_amoeba ();
extern void setnofit();
extern int getfilelines();

/* Statics used by the chisqr evaluator */
static double	*sx_p;
static double	*sy_p;
static double	*gra_p;
static double	*gdec_p;
static double	xref_p, yref_p;
static double	xrefpix, yrefpix;
static int	nbin_p;
static int	nfit;	/* Number of parameters to fit */
static int	pfit0 = 0;	/* List of parameters to fit, 1 per digit */
static int	cdfit = 0;	/* 1 if CD matrix has been fit */
static int	resid_refine = 0;
static int	minbin=2;	/* Minimum number of coincidence hits needed */
static int	minmatch0 = MINMATCH;	/* matches to drop out of loop */
static int	nitmax0 = NMAX;		/* max iterations to stop fit */
static int	vfit[NPAR1]; /* Parameters being fit: index to value vector
				1= RA,		  2= Dec,
				3= X plate scale, 4= Y plate scale
				5= rotation,	  6= second rotation (skew),
				7= optical axis X,8= optical axis Y */

/* Find shift, scale, and rotation of image stars to best-match reference stars
 * Get best match by finding which offsets between pairs of s's and g's
 * work for the most other pairs of s's and g's
 * N.B. we assume rotation will be "small enough" so that initial guesses can
 *   be done using just shifts.
 * Return count of total coincidences found, else 0 if none or -1 if trouble.
 */

int
StarMatch (ns,sx,sy,refcat,ng,gnum,gra,gdec,goff,gx,gy,tol,wcs,debug)

int	ns;		/* Number of image stars */
double	*sx;		/* Image star X coordinates in pixels */
double	*sy;		/* Image star Y coordinates in pixels */
int	refcat;		/* Reference Catalog code */
int	ng;		/* Number of reference stars */
double	*gnum;		/* Reference star catalog numbers */
double	*gra;		/* Reference star right ascensions in degrees */
double	*gdec;		/* Reference star right ascensions in degrees */
int	*goff;		/* Reference star offscale flags */
double	*gx;		/* Reference star X coordinates in pixels */
double	*gy;		/* Reference star Y coordinates in pixels */
double	tol;		/* +/- this many pixels is a hit */
struct WorldCoor *wcs;	/* World coordinate structure (fit returned) */
int	debug;

{
    double dx, bestdx, dxi;
    double dy, bestdy, dyi;
    double dx2, dy2, dxy, dxys, dxs, dys, dxsum, dysum;
    double *mx, *my, *mxy;
    int nmatch;
    int s, g, si, gi, igs;
    int nbin;
    double *sbx, *sby;	/* malloced array of s stars in best bin */
    double *gbra, *gbdec;	/* malloced array of g stars in best bin */
    int peaks[NPEAKS+1];	/* history of bin counts */
    int dxpeaks[NPEAKS+1], dypeaks[NPEAKS+1]; /* history of dx/dy at peaks */
    int npeaks;		/* entries in use in peaks[] */
    int maxnbin, i, nmatchd;
    int minmatch;
    int *is, *ig, *ibs, *ibg;
    char rastr[16], decstr[16], numstr[16];
    double xref0, yref0, xinc0, yinc0, rot0, xrefpix0, yrefpix0, cd0[4];
    int bestbin;	/* Number of coincidences for refit */
    int pfit;		/* List of parameters to fit, 1 per digit */
    char vpar[16];	/* List of parameters to fit */
    char *vi;
    char vc;
    int ParamFit();
    double tol2 = tol * tol;
    double maxnum;
    int nnfld = 0;

    /* Set minimum number of matches between image and reference stars to fit */
    if (ns > ng) {
	minmatch = 0.5 * ng;
	if (minmatch > minmatch0)
	    minmatch = 0.25 * ng;
	if (minmatch > minmatch0)
	    minmatch = minmatch0;
	}
    else {
	minmatch = 0.5 * ns;
	if (minmatch > minmatch0)
	    minmatch = 0.25 * ns;
	if (minmatch > minmatch0)
	    minmatch = minmatch0;
	}

    /* Set format for numbers, if listed */
    if (debug) {
	maxnum = gnum[0];
	for (gi = 1; gi < ng; gi++) {
	    if (gnum[gi] > maxnum)
		maxnum = gnum[gi];
	    }
	nnfld = CatNumLen (refcat, maxnum, 0);
        }

    /* Set maximum number of matches and allocate match indices */
    if (ng > ns)
	maxnbin = (int) ((double) ng * 1.25);
    else
	maxnbin = (int) ((double) ns * 1.25);
    if (debug)
	fprintf (stderr,"Match history: nim=%d nref=%d tol=%3.0f minbin=%d minmatch=%d):\n",
		 ns, ng, tol, minbin, minmatch);

    /* Allocate arrays in which to save match information */
    mx = (double *) calloc (maxnbin, sizeof(double));
    my = (double *) calloc (maxnbin, sizeof(double));
    mxy = (double *) calloc (maxnbin, sizeof(double));
    is = (int *) calloc (maxnbin, sizeof(int));
    ig = (int *) calloc (maxnbin, sizeof(int));
    ibs = (int *) calloc (maxnbin, sizeof(int));
    ibg = (int *) calloc (maxnbin, sizeof(int));

    /* Try matching stars using the current WCS first */
    nmatch = 0;
    bestdx = 0.0;
    bestdy = 0.0;
    dxsum = 0.0;
    dysum = 0.0;
    dxs = 0.0;
    dys = 0.0;

    /* Loop through image stars */
    nbx = nxpix / (int) tol;
    nby = nypix / (int) tol;
    dbin = calloc (4 * nbx * nby, sizeof(double));
    for (s = 0; s < ns; s++) {

	/* Loop through reference catalog stars */
	for (g = 0; g < ng; g++) {
	    dx  = gx[g] - sx[s];
	    dy = gy[g] - sy[s];

	    /* Add to number in this offset bin */
	    idx = (dx / tol)
	    idy = (dy / tol)
	    if ((idx > -nbx && idx < nbx && idy > -nby && idy < nby)
		dbin[((idy+nby) * nbx) + idx + nbx]++;
	    }
	}

    /* Find offset bin with maximim number of entries
    idxmax = 0;
    idymax = 0;
    nmatch = 0;
    dbini = dbin
    for (idy = -nby; idy < nby; idy++) {
	for (idx = -nbx; idx < nbx; idx++) {
	    if (*dbini > ndmax) {
		idxmax = idx;
		idymax - idy;
		nmatch = *dbini;
		}
	    dbini++;
	    }
	}
    bestdx = tol * (double) (idxmax + nbx);
    bestdy = tol * (double) (idymax + nby);

    /* If we found enough matches, we can proceed with this offset */
    if (nmatch > minmatch) {
	if (debug)
	    fprintf (stderr, "%d matches found at mean offset %6.3f %6.3f\n",
		nmatch, bestdx, bestdy);
	}

    /* Otherwise, look for a coarse alignment assuming no additional rotation.
     * This will collect a set of stars that correspond and
     * establish an initial guess of the solution.
     */
    else {
	npeaks = 0;
	nmatch = 0;
	for (i = 0; i < NPEAKS; i++) {
	    peaks[i] = 0;
	    dxpeaks[i] = 0;
	    dypeaks[i] = 0;
	    }
	bestdx = 0.0;
	bestdy = 0.0;
	for (s = 0; s < ns; s++) {
	    for (g = 0; g < ng; g++) {
		dx = gx[g] - sx[s];
		dy = gy[g] - sy[s];
		nbin = 0;
		for (gi = 0; gi < ng; gi++) {
		    for (si = 0; si < ns; si++) {
			dxi = gx[gi] - sx[si] - dx;
			if (dxi < 0)
			    dxi = -dxi;
			dyi = gy[gi] - sy[si] - dy;
			if (dyi < 0)
			    dyi = -dyi;
			if (dxi <= tol && dyi <= tol) {
			/* if (debug)
			    fprintf (stderr,"%d %d %d %d %5.1f %5.1f %5.1f %5.1f\n",
				     g,s,gi,si,dx,dy,dxi,dyi); */
			    is[nbin] = si;
			    ig[nbin] = gi;
			    nbin++;
			    }
			}
		    }
		/* if (debug)
		    fprintf (stderr,"%d %d %d %d %d\n", g,s,gi,si,nbin); */
		if (nbin > 1 && nbin >= nmatch) {
		    int i;
		    nmatch = nbin;
		    bestdx = (double) dx;
		    bestdy = (double) dy;
		    for (i = 0; i < nbin; i++) {
			ibs[i] = is[i];
			ibg[i] = ig[i];
			}
	
		    /* keep last NPEAKS nmatchs, dx and dy;
		     * put newest first in arrays */
		    if (npeaks > 0) {
			for (i = npeaks; i > 0; i--) {
			    peaks[i] = peaks[i-1];
			    dxpeaks[i] = dxpeaks[i-1];
			    dypeaks[i] = dypeaks[i-1];
			    }
			}
		    peaks[0] = nmatch;
		    if (bestdx > 0.0)
			dxpeaks[0] = (int) (bestdx + 0.5);
		    else
			dxpeaks[0] = (int) (bestdx - 0.5);
		    if (bestdy > 0)
			dypeaks[0] = (int) (bestdy + 0.5);
		    else
			dypeaks[0] = (int) (bestdy - 0.5);
		    if (npeaks < NPEAKS)
			npeaks++;
		    if (debug)
			fprintf (stderr,"%d: %d/%d matches at image %d cat %d: dx= %d dy= %d\n",
				npeaks, nmatch, minmatch, s, g, dxpeaks[0], dypeaks[0]);
		    }
		if (nmatch > minmatch)
		    break;
		}
	    if (nmatch > minmatch)
		break;
	    }

	/* if (debug) {
	    int i;
	    for (i = 0; i < npeaks; i++)
		fprintf (stderr," %d bins at dx=%d dy=%d\n",
			 peaks[i], dxpeaks[i], dypeaks[i]);
	    } */

	/* peak is broad */
	if (npeaks < 2 || peaks[1] == peaks[0]) {
	    if (debug)
		fprintf (stderr,"  Broad peak of %d bins at dx=%.0f dy=%.0f\n",
		         peaks[0], bestdx, bestdy);
	    }
	}

    /* too few hits */
    if (nmatch < minbin)
	return (nmatch);

    /* Get X and Y coordinates of matches from best binning */
    nmatchd = nmatch * sizeof (double);
    if (!(sbx = (double *) malloc (nmatchd)))
	fprintf (stderr," Could not allocate %d bytes for SBX\n", nmatchd);
    if (!(sby = (double *) malloc (nmatchd)))
	fprintf (stderr," Could not allocate %d bytes for SBY\n", nmatchd);
    if (!(gbra = (double *) malloc (nmatchd)))
	fprintf (stderr," Could not allocate %d bytes for GBRA\n", nmatchd);
    if (!(gbdec = (double *) malloc (nmatchd)))
	fprintf (stderr," Could not allocate %d bytes for GBDEC\n", nmatchd);
    for (i = 0; i < nmatch; i++) {
	sbx[i] = sx[ibs[i]];
	sby[i] = sy[ibs[i]];
	gbra[i] = gra[ibg[i]];
	gbdec[i] = gdec[ibg[i]];
	}

    /* Reset image center based on star matching */
    wcs->xref = wcs->xref + (bestdx * wcs->xinc);
    if (wcs->xref < 0.0) wcs->xref = 360.0 + wcs->xref;
    wcs->yref = wcs->yref + (bestdy * wcs->yinc);

    /* Fit WCS to matched stars */

    /* Provide non-parametric access to the star lists */
    sx_p = sbx;
    sy_p = sby;
    gra_p = gbra;
    gdec_p = gbdec;
    xref_p = wcs->xref;
    yref_p = wcs->yref;
    xrefpix = wcs->xrefpix;
    yrefpix = wcs->yrefpix;
    nbin_p = nmatch;

    /* Number of parameters to fit from command line or number of matches */
    pfit = ParamFit (nmatch);

    /* Get parameters to fit from digits of pfit */
    sprintf (vpar, "%d", pfit);
    nfit = 0;
    vfit[0] = -1;
    for (i = 1; i < NPAR1; i++) {
	vc = i + 48;
	vi = strchr (vpar, vc);
	if (vi != NULL) {
	    vfit[i] = vi - vpar;
	    nfit++;
	    }
	else
	    vfit[i] = -1;
	}

    /* Set initial guesses for parameters which are being fit */
    xref0 = wcs->xref;
    yref0 = wcs->yref;
    xinc0 = wcs->xinc;
    yinc0 = wcs->yinc;
    rot0 = wcs->rot;
    xrefpix0 = wcs->xrefpix;
    yrefpix0 = wcs->yrefpix;
    cd0[0] = wcs->cd[0];
    cd0[1] = wcs->cd[1];
    cd0[2] = wcs->cd[2];
    cd0[3] = wcs->cd[3];
    if (vfit[6] > -1)
	cdfit = 1;
    else
	cdfit = 0;

    /* Fit image star coordinates to reference star positions */
    wcs_amoeba (wcs);

    if (debug) {
	fprintf (stderr,"\nAmoeba fit:\n");
	ra2str (rastr, 16, xref0, 3);
	dec2str (decstr, 16, yref0, 2);
	fprintf (stderr,"   initial guess:\n");
	if (vfit[6] > -1)
	    fprintf (stderr," cra= %s cdec= %s cd = %9.7f,%9.7f,%9.7f,%9.7f ",
		     rastr, decstr, cd0[0], cd0[1], cd0[2], cd0[3]);
	else
	    fprintf (stderr," cra= %s cdec= %s del=%7.4f,%7.4f rot=%7.4f ",
		     rastr, decstr, xinc0*3600.0, yinc0*3600.0, rot0);
	fprintf (stderr,"(%8.2f,%8.2f\n", xrefpix0, yrefpix0);

	ra2str (rastr, 16, wcs->xref, 3);
	dec2str (decstr, 16, wcs->yref, 2);
	fprintf (stderr,"\nfirst solution:\n");
	if (vfit[6] > -1)
	    fprintf (stderr," cra= %s cdec= %s cd = %9.7f,%9.7f,%9.7f,%9.7f ",
		     rastr,decstr,wcs->cd[0],wcs->cd[1],wcs->cd[2],wcs->cd[3]);
	else
	    fprintf (stderr," cra= %s cdec= %s del=%7.4f,%7.4f rot=%7.4f ",
		     rastr,decstr,3600.0*wcs->xinc,3600.0*wcs->yinc,wcs->rot);
	fprintf (stderr,"(%8.2f,%8.2f)\n", wcs->xrefpix, wcs->yrefpix);
	}

    /* If we have extra bins, repeat with the best ones */
    bestbin = nfit + 1;
    if (resid_refine && nmatch > bestbin) {
	double *resid = (double *) malloc (nmatch * sizeof(double));
	double *xe = (double *) malloc (nmatch * sizeof(double));
	double *ye = (double *) malloc (nmatch * sizeof(double));
	int i, j;
	double xmean, ymean, rmean, xsumsq, ysumsq, diff;
	double mx, my, xsig, ysig, rsig, siglim;
	char wcstring[64];
	double xsum = 0.0;
	double ysum = 0.0;
	double rsum = 0.0;
	double dmatch = (double)nmatch;
	double dmatch1 = (double)(nmatch - 1);

	/* Compute residuals at each star location */
	for (i = 0; i < nmatch; i++) {
	    pix2wcs (wcs, sbx[i], sby[i], &mx, &my);
	    xe[i] = (mx - gbra[i]) * 3600.0;
	    ye[i] = (my - gbdec[i]) * 3600.0;
	    resid[i] = sqrt (xe[i]*xe[i] + ye[i]*ye[i]);
	    if (debug) {
		pix2wcst (wcs, sbx[i], sby[i], wcstring, 64);
		fprintf (stderr,"%3d (%8.3f,%8.3f) -> %s %6.3f %6.3f %6.3f\n",
		    i, sbx[i], sby[i], wcstring, xe[i], ye[i], resid[i]);
		}
	    xsum = xsum + xe[i];
	    ysum = ysum + ye[i];
	    rsum = rsum + resid[i];
	    }

	/* Compute means and standard deviations */
	xmean = xsum / dmatch;
	ymean = ysum / dmatch;
	rmean = rsum / dmatch;
	xsumsq = 0.0;
	ysumsq = 0.0;
	for (i = 0; i < nmatch; i++) {
	    diff = xe[i] - xmean;
	    xsumsq = xsumsq + (diff * diff);
	    diff = ye[i] - ymean;
	    ysumsq = ysumsq + (diff * diff);
	    }
	xsig = sqrt (xsumsq / dmatch1);
	ysig = sqrt (ysumsq / dmatch1);
	rsig = sqrt ((xsumsq + ysumsq)/ dmatch1);
	siglim = 2.0 * rsig;
	if (debug) {
	    fprintf (stderr,"Mean x: %6.3f/%6.3f y: %6.3f/%6.3f r: %6.3f/%6.3f\n",
		    xmean, xsig, ymean, ysig, rmean, rsig);
	    }

	/* sort by increasing total residual */
	for (i = 0; i < nmatch-1; i++) {
	    for (j = i+1; j < nmatch; j++) {
		if (resid[j] < resid[i]) {
		    double tmp;

		    tmp = sbx[i]; sbx[i] = sbx[j]; sbx[j] = tmp;
		    tmp = sby[i]; sby[i] = sby[j]; sby[j] = tmp;
		    tmp = gbra[i]; gbra[i] = gbra[j]; gbra[j] = tmp;
		    tmp = gbdec[i]; gbdec[i] = gbdec[j]; gbdec[j] = tmp;
		    tmp = resid[i]; resid[i] = resid[j]; resid[j] = tmp;
		    }
		}
	    }

	/* Cut off points at residual of two sigma */
	for (i = 0; i < nmatch; i++) {
	    if (resid[i] > siglim) {
		if (i > bestbin) bestbin = i - 1;
		break;
		}
	    }

	xref_p = wcs->xref;
	if (xref_p < 0.0) xref_p = 360.0 + xref_p;
	yref_p = wcs->yref;
	xrefpix = wcs->xrefpix;
	yrefpix = wcs->yrefpix;
	nbin_p = bestbin;
	wcs_amoeba (wcs);

	if (debug) {
	    ra2str (rastr, 16, wcs->xref, 3);
	    dec2str (decstr, 16, wcs->yref, 2);
	    fprintf (stderr,"\nresid solution:\n");
	    fprintf (stderr,"\n%d points < %.3f arcsec residuals refit\n",
			    bestbin, siglim);
	    fprintf (stderr," cra= %s cdec= %s del=%7.4f,%7.4f rot=%7.4f ",
		 rastr, decstr, 3600.0*wcs->xinc, 3600.0*wcs->yinc, wcs->rot);
	    fprintf (stderr,"(%8.2f,%8.2f)\n", wcs->xrefpix, wcs->yrefpix);
	    }
	free (resid);
	free (xe);
	free (ye);
	}

    free (sbx);
    free (sby);
    free (gbra);
    free (gbdec);
    free (is);
    free (ig);
    free (ibs);
    free (ibg);

    return (nmatch);
}

int
ParamFit (nbin)

int	nbin;	/* Number of point to be fit */
{
    int pfit;

    if (pfit0 != 0) {
	if (pfit0 < 3)
	    pfit = 12;
	else if (pfit0 == 3)	/* Fit center and plate scale */
	    pfit = 123;
	else if (pfit0 == 4)	/* Fit center, plate scale, rotation */
	    pfit = 1235;
	else if (pfit0 == 5)	/* Fit center, x&y plate scales, rotation */
	    pfit = 12345;
	else if (pfit0 == 6)	/* Fit center, x&y plate scales, x&y rotations */
	    pfit = 123456;
	else if (pfit0 == 7)	/* Fit center, x&y plate scales, rotation, refpix */
	    pfit = 1234578;
	else if (pfit0 == 8)	/* Fit center, x&y plate scales, x&y rotation, refpix */
	    pfit = 12345678;
	else
	    pfit = pfit0;
	}
    else if (nbin < 4)
	pfit = 12;
    else if (nbin < 6)
	pfit = 123;
    else
	pfit = 12345;
    return (pfit);
}


int
NParamFit (nbin)

int	nbin;	/* Number of point to be fit */
{
    int pfit;

    pfit = ParamFit (nbin);
    if (pfit < 1)
	return (0);
    else if (pfit < 10)
	return (1);
    else if (pfit < 100)
	return (2);
    else if (pfit < 1000)
	return (3);
    else if (pfit < 10000)
	return (4);
    else if (pfit < 100000)
	return (5);
    else if (pfit < 1000000)
	return (6);
    else if (pfit < 10000000)
	return (7);
    else
	return (8);
}


int
ReadMatch (filename, sx, sy, sra, sdec, debug)

char	*filename;	/* Name of file containing matches */
double	**sx;		/* Image star X coordinates in pixels */
double	**sy;		/* Image star Y coordinates in pixels */
double	**sra;		/* Probable image star right ascensions in degrees */
double	**sdec;		/* Probable image star declinations in degrees */
int	debug;		/* Printed debugging information if not zero */

{
    int nbytes, nread, ir, ntok, itok, iytok;
    double *tx, *ty, *tra, *tdec, ra, dec, x, y;
    int ndec;
    int nmatch = 0;	/* Number of matches read from file */
    char rastr[32], decstr[32];

    /* If tab file, read from ra, dec, x, y  columns */
    if (istab (filename)) {
	}

    /* Otherwise, assume first 4 columns are x, y, ra, dec */
    else {
	char line[1025];
	char *nextline, *lastchar;
	FILE *fd;
	struct Tokens tokens;	/* Token structure */
	char *cwhite;		/* additional whitespace characters */
	char token[256];

	cwhite = NULL;

	/* Open input file */
	if (!strcmp (filename, "stdin")) {
	    fd = stdin;
	    nread = 1000;
	    }
	else {
	    nread = getfilelines (filename);
	    if (!(fd = fopen (filename, "r"))) {
	    fprintf (stderr, "SetWCSFITS: Match file %s could not be opened\n",
		     filename);
		return (0);
		}
	    }
	nbytes = nread * sizeof (double);
	if (!(tra = (double *) calloc (nread, sizeof(double))))
	    fprintf (stderr, "Could not calloc %d bytes for gra\n", nbytes);
	if (!(tdec = (double *) calloc (nread, sizeof(double))))
	    fprintf (stderr, "Could not calloc %d bytes for gdec\n", nbytes);
	if (!(tx = (double *) calloc (nread, sizeof(double))))
	    fprintf (stderr, "Could not calloc %d bytes for sx\n", nbytes);
	if (!(ty = (double *) calloc (nread, sizeof(double))))
	    fprintf (stderr, "Could not calloc %d bytes for sy\n", nbytes);
	*sra = tra;
	*sdec = tdec;
	*sx = tx;
	*sy = ty;

	nmatch = 0;
	nextline = line;
        for (ir = 0; ir < nread; ir++) {
	    if (fgets (line, 1024, fd) == NULL)
		break;

	    /* Skip lines with comments */
	    if (line[0] == '#')
		continue;

	    /* Drop linefeeds */
	    lastchar = nextline + strlen(nextline) - 1;
	    if (*lastchar < 32)
		*lastchar = (char) 0;

	    /* Read X, Y, RA, and Dec from each line,
		skipping line if all four are not present and numbers */
	    ntok = setoken (&tokens, line, cwhite);
	    if (ntok < 1)
		break;
	    if (ntok < 4)
		continue;

	    /* if (debug)
		fprintf (stderr, "%d: %s\n", nmatch, line); */

	    /* Image X coordinate or RA */
	    itok = 1;
	    if (getoken(&tokens, itok, token, 256)) {

		/* Read RA, Dec, X, Y if first token has : in it */
		if (strchr (token, ':') != NULL) {
		    ra = str2ra (token);
		    iytok = 4;
		    if (getoken(&tokens, 2, token, 256))
			dec = str2dec (token);
		    if (getoken(&tokens, 3, token, 256)) {
			if (isnum (token))
			    x = atof (token);
			else {
			    iytok = 5;
			    if (getoken(&tokens, 4, token, 256)) {
				if (isnum (token))
				    x = atof (token);
				else
				    continue;
				}
			    }
			}
		    if (getoken(&tokens, iytok, token, 256)) {
			if (isnum (token))
			    y = atof (token);
			else
			    continue;
			}
		    tx[nmatch] = x;
		    ty[nmatch] = y;
		    tra[nmatch] = ra;
		    tdec[nmatch] = dec;
		    nmatch++;
		    continue;
		    }
		if (isnum (token))
		    x = atof (token);
		else
		    continue;
		}
	    else
		continue;

	    /* Image Y coordinate */
	    itok++;
	    if (getoken(&tokens, itok, token, 256)) {
		if (isnum (token))
		    y = atof (token);
		else
		    continue;
		}
	    else
		continue;

	    /* Right ascension */
	    itok++;
	    if (getoken(&tokens, itok, token, 256)) {
		if (isnum (token) == 1) {
		    ra = atof (token);
		    itok++;
		    if (getoken(&tokens, itok, token, 256)) {
			if (isnum (token) == 2)
			    ra = ra + (atof (token) / 60.0);
			else if (isnum (token) == 1) {
			    ra = ra + (atof (token) / 60.0);
			    itok++;
			    if (getoken(&tokens, itok, token, 256)) {
				if (isnum (token))
				    ra = ra + (atof (token) / 3600.0);
				}
			    }
			}
		    ra = ra * 15.0;
		    }
		else
		    ra = str2ra (token);
		}
	    else
		continue;

	    /* Declination */
	    itok++;
	    if (getoken(&tokens, itok, token, 256)) {
		if (isnum (token) == 1) {
		    dec = atof (token);
		    itok++;
		    if (strchr (token, '-') != NULL)
			ndec = 1;
		    else
			ndec = 0;
		    if (getoken(&tokens, itok, token, 256)) {
			if (isnum (token) == 2) {
			    if (ndec)
				dec = dec - (atof (token) / 60.0);
			    else
				dec = dec + (atof (token) / 60.0);
			    }
			else if (isnum (token) == 1) {
			    if (ndec)
				dec = dec - (atof (token) / 60.0);
			    else
				dec = dec + (atof (token) / 60.0);
			    itok++;
			    if (getoken(&tokens, itok, token, 256)) {
				if (isnum (token)) {
				    if (ndec)
					dec = dec - (atof (token) / 3600.0);
				    else
					dec = dec + (atof (token) / 3600.0);
				    }
				}
			    }
			}
		    }
		else
		    dec = str2dec (token);
		}
	    else
		continue;
	    tx[nmatch] = x;
	    ty[nmatch] = y;
	    tra[nmatch] = ra;
	    tdec[nmatch] = dec;
	    if (debug) {
		ra2str (rastr, 32, tra[nmatch], 3);
		dec2str (decstr, 32, tdec[nmatch], 2);
		fprintf (stderr, "%d: %8.3f %8.3f %s %s\n", nmatch,
			 tx[nmatch], ty[nmatch], rastr, decstr);
		}
	    nmatch++;
	    }
	}

    return (nmatch);
}


/* Find shift, scale, and rotation of image stars to best-match reference stars
 */

void
WCSMatch (nmatch, sbx, sby, gbra, gbdec, debug)

int	nmatch;		/* Number of matched stars */
double	*sbx;		/* Image star X coordinates in pixels */
double	*sby;		/* Image star Y coordinates in pixels */
double	*gbra;		/* Reference star right ascensions in degrees */
double	*gbdec;		/* Reference star right ascensions in degrees */
int	debug;		/* Printed debugging information if not zero */

{
    int i;
    double xdiff, ydiff;
    int nsc, j;
    double tx = 0.0;
    double ty = 0.0;
    double tra = 0.0;
    double tdec = 0.0;
    double tdiff = 0.0;
    double cra, cdec, cx, cy, scale;
    double dmatch;
    double skydiff, imdiff;
    extern double getsecpix();
    extern void getcenter(),getrefpix(),setdcenter(),setrefpix(),setsecpix();

    dmatch = (double) nmatch;

    if (debug) {
	fprintf (stderr,"%d matched stars:\n", nmatch);
	}

    /* too few hits */
    if (nmatch < 2)
	return;

    /* Compute plate scale and center of stars */
    nsc = 0;
    for (i = 0; i < nmatch; i++) {
	tx = tx + sbx[i];
	ty = ty + sby[i];
	tra = tra + gbra[i];
	tdec = tdec + gbdec[i];
	for (j = i+1; j < nmatch; j++) {
	    skydiff = wcsdist (gbra[i], gbdec[i], gbra[j], gbdec[j]);
	    xdiff = sbx[j] - sbx[i];
	    ydiff = sby[j] - sby[i];
	    imdiff = sqrt ((xdiff * xdiff) + (ydiff * ydiff));
	    scale = skydiff / imdiff;
	    tdiff = tdiff + scale;
	    nsc++;
	    if (debug) {
		fprintf (stderr,"%d %d: sky: %8g, image: %8g, %8g deg/pix", 
			i, j, skydiff, imdiff, scale);
		fprintf (stderr," = %8g arcsec/pix\n", scale * 3600.0);
		}
	    }
	}
    
    /* Reset image center based on star matching */
    getcenter (&cra, &cdec);
    if (cra == -99.0 && cdec == -99.0) {
	cra = tra / dmatch;
	cdec = tdec / dmatch;
	setdcenter (cra, cdec);
	}
    getrefpix (&cx, &cy);
    if (cx == -99999.0) {
	cx = tx / dmatch;
	cy = ty / dmatch;
	setrefpix (cx, cy);
	}
    scale = getsecpix();
    if (scale == 0.0) {
	scale = tdiff / (double) nsc;
	setsecpix (3600.0 * scale);
	}
    if (debug)
	fprintf (stderr,"scale = %8g deg/pix = %8g arcsec/pix\n", scale, scale*3600.0);
    return;
}


/* Find shift, scale, and rotation of image stars to best-match reference stars
 * Return count of total coincidences found, else 0 if none or -1 if trouble.
 */

int
FitMatch (nmatch, sbx, sby, gbra, gbdec, wcs, debug)

int	nmatch;		/* Number of matched stars */
double	*sbx;		/* Image star X coordinates in pixels */
double	*sby;		/* Image star Y coordinates in pixels */
double	*gbra;		/* Reference star right ascensions in degrees */
double	*gbdec;		/* Reference star right ascensions in degrees */
struct WorldCoor *wcs;	/* World coordinate structure (fit returned) */
int	debug;		/* Printed debugging information if not zero */

{
    int i;
    char rastr[16], decstr[16];
    double xref0, yref0, xinc0, yinc0, rot0, xrefpix0, yrefpix0, cd0[4];
    int bestbin;	/* Number of coincidences for refit */
    int pfit;		/* List of parameters to fit, 1 per digit */
    char vpar[16];	/* List of parameters to fit */
    double xdiff, ydiff;
    char *vi;
    char vc;
    int nsc, j;
/*    double equinox = wcs->equinox; */
    double tx = 0.0;
    double ty = 0.0;
    double tra = 0.0;
    double tdec = 0.0;
    double tdiff = 0.0;
    double scale;
/*    double dmatch; */
    double skydiff, imdiff;

/*    dmatch = (double) nmatch; */

    if (debug) {
	fprintf (stderr,"%d matched stars:\n", nmatch);
	}

    /* too few hits */
    if (nmatch < minbin)
	return (nmatch);

    /* Compute plate scale and center of stars */
    nsc = 0;
    for (i = 0; i < nmatch; i++) {
	tx = tx + sbx[i];
	ty = ty + sby[i];
	tra = tra + gbra[i];
	tdec = tdec + gbdec[i];
	for (j = i+1; j < nmatch; j++) {
	    skydiff = wcsdist (gbra[i], gbdec[i], gbra[j], gbdec[j]);
	    xdiff = sbx[j] - sbx[i];
	    ydiff = sby[j] - sby[i];
	    imdiff = sqrt ((xdiff * xdiff) + (ydiff * ydiff));
	    scale = skydiff / imdiff;
	    tdiff = tdiff + scale;
	    nsc++;
	    if (debug) {
		fprintf (stderr,"%d %d: sky: %8g, image: %8g, %8g deg/pix", 
			i, j, skydiff, imdiff, scale);
		fprintf (stderr," = %8g arcsec/pix\n", scale * 3600.0);
		}
	    }
	}
    
    /* Reset image center in WCS data structure based on star matching */
    /* cra = tra / dmatch;
    cdec = tdec / dmatch;
    cx = tx / dmatch;
    cy = ty / dmatch;
    scale = tdiff / (double) nsc;
    if (debug)
	fprintf (stderr,"scale = %8g deg/pix = %8g arcsec/pix\n", scale, scale*3600.0);
    wcsreset (wcs, cx, cy, cra, cdec, scale, 0.0, 0.0, NULL, equinox); */

    /* Provide non-parametric access to the star lists */
    sx_p = sbx;
    sy_p = sby;
    gra_p = gbra;
    gdec_p = gbdec;
    xref_p = wcs->xref;
    if (xref_p < 0.0) xref_p = 360.0 + xref_p;
    yref_p = wcs->yref;
    xrefpix = wcs->xrefpix;
    yrefpix = wcs->yrefpix;
    nbin_p = nmatch;

    /* Number of parameters to fit from command line or number of matches */
    if (pfit0 != 0) {
	if (pfit0 < 3)
	    pfit = 12;
	else if (pfit0 == 3)	/* Fit center and plate scale */
	    pfit = 123;
	else if (pfit0 == 4)	/* Fit center, plate scale, rotation */
	    pfit = 1235;
	else if (pfit0 == 5)	/* Fit center, x&y plate scales, rotation */
	    pfit = 12345;
	else if (pfit0 == 6)	/* Fit center, x&y plate scales, x&y rotations */
	    pfit = 123456;
	else if (pfit0 == 7)	/* Fit center, x&y plate scales, rotation, refpix */
	    pfit = 1234578;
	else if (pfit0 == 8)	/* Fit center, x&y plate scales, x&y rotation, refpix */
	    pfit = 12345678;
	else
	    pfit = pfit0;
	}
    else if (nmatch < 4)
	pfit = 12;
    else if (nmatch < 6)
	pfit = 123;
    else
	pfit = 12345;

    /* Get parameters to fit from digits of pfit */
    sprintf (vpar, "%d", pfit);
    nfit = 0;
    vfit[0] = -1;
    for (i = 1; i < NPAR1; i++) {
	vc = i + 48;
	vi = strchr (vpar, vc);
	if (vi != NULL) {
	    vfit[i] = vi - vpar;
	    nfit++;
	    }
	else
	    vfit[i] = -1;
	}

    /* Set initial guesses for parameters which are being fit */
    xref0 = wcs->xref;
    yref0 = wcs->yref;
    xinc0 = wcs->xinc;
    yinc0 = wcs->yinc;
    rot0 = wcs->rot;
    xrefpix0 = wcs->xrefpix;
    yrefpix0 = wcs->yrefpix;
    cd0[0] = wcs->cd[0];
    cd0[1] = wcs->cd[1];
    cd0[2] = wcs->cd[2];
    cd0[3] = wcs->cd[3];
    if (vfit[6] > -1)
	cdfit = 1;
    else
	cdfit = 0;

    /* Fit image star coordinates to reference star positions */
    wcs_amoeba (wcs);

    if (debug) {
	fprintf (stderr,"\nAmoeba fit:\n");
	ra2str (rastr, 16, xref0, 3);
	dec2str (decstr, 16, yref0, 2);
	fprintf (stderr,"   initial guess:\n");
	if (vfit[6] > -1)
	    fprintf (stderr," cra= %s cdec= %s cd = %9.7f,%9.7f,%9.7f,%9.7f ",
		     rastr, decstr, cd0[0], cd0[1], cd0[2], cd0[3]);
	else
	    fprintf (stderr," cra= %s cdec= %s del=%7.4f,%7.4f rot=%7.4f ",
		     rastr, decstr, xinc0*3600.0, yinc0*3600.0, rot0);
	fprintf (stderr,"(%8.2f,%8.2f\n", xrefpix0, yrefpix0);

	ra2str (rastr, 16, wcs->xref, 3);
	dec2str (decstr, 16, wcs->yref, 2);
	fprintf (stderr,"\nfirst solution:\n");
	if (vfit[6] > -1)
	    fprintf (stderr," cra= %s cdec= %s cd = %9.7f,%9.7f,%9.7f,%9.7f ",
		     rastr,decstr,wcs->cd[0],wcs->cd[1],wcs->cd[2],wcs->cd[3]);
	else
	    fprintf (stderr," cra= %s cdec= %s del=%7.4f,%7.4f rot=%7.4f ",
		     rastr,decstr,3600.0*wcs->xinc,3600.0*wcs->yinc,wcs->rot);
	fprintf (stderr,"(%8.2f,%8.2f)\n", wcs->xrefpix, wcs->yrefpix);
	}

    /* If we have extra bins, repeat with the best ones */
    bestbin = nfit + 1;
    if (resid_refine && nmatch > bestbin) {
	double *resid = (double *) malloc (nmatch * sizeof(double));
	double *xe = (double *) malloc (nmatch * sizeof(double));
	double *ye = (double *) malloc (nmatch * sizeof(double));
	int i, j;
	double xmean, ymean, rmean, xsumsq, ysumsq, diff;
	double mra, mdec, xsig, ysig, rsig, siglim;
	char wcstring[64];
	double xsum = 0.0;
	double ysum = 0.0;
	double rsum = 0.0;
	double dmatch = (double)nmatch;
	double dmatch1 = (double)(nmatch - 1);

	/* Compute residuals at each star location */
	for (i = 0; i < nmatch; i++) {
	    pix2wcs (wcs, sbx[i], sby[i], &mra, &mdec);
	    xe[i] = (mra - gbra[i]) * 3600.0;
	    ye[i] = (mdec - gbdec[i]) * 3600.0;
	    resid[i] = sqrt (xe[i]*xe[i] + ye[i]*ye[i]);
	    if (debug) {
		pix2wcst (wcs, sbx[i], sby[i], wcstring, 64);
		fprintf (stderr,"%3d (%8.3f,%8.3f) -> %s %6.3f %6.3f %6.3f\n",
		    i, sbx[i], sby[i], wcstring, xe[i], ye[i], resid[i]);
		}
	    xsum = xsum + xe[i];
	    ysum = ysum + ye[i];
	    rsum = rsum + resid[i];
	    }

	/* Compute means and standard deviations */
	xmean = xsum / dmatch;
	ymean = ysum / dmatch;
	rmean = rsum / dmatch;
	xsumsq = 0.0;
	ysumsq = 0.0;
	for (i = 0; i < nmatch; i++) {
	    diff = xe[i] - xmean;
	    xsumsq = xsumsq + (diff * diff);
	    diff = ye[i] - ymean;
	    ysumsq = ysumsq + (diff * diff);
	    }
	xsig = sqrt (xsumsq / dmatch1);
	ysig = sqrt (ysumsq / dmatch1);
	rsig = sqrt ((xsumsq + ysumsq)/ dmatch1);
	siglim = 2.0 * rsig;
	if (debug) {
	    fprintf (stderr,"Mean x: %6.3f/%6.3f y: %6.3f/%6.3f r: %6.3f/%6.3f\n",
		    xmean, xsig, ymean, ysig, rmean, rsig);
	    }

	/* sort by increasing total residual */
	for (i = 0; i < nmatch-1; i++) {
	    for (j = i+1; j < nmatch; j++) {
		if (resid[j] < resid[i]) {
		    double tmp;

		    tmp = sbx[i]; sbx[i] = sbx[j]; sbx[j] = tmp;
		    tmp = sby[i]; sby[i] = sby[j]; sby[j] = tmp;
		    tmp = gbra[i]; gbra[i] = gbra[j]; gbra[j] = tmp;
		    tmp = gbdec[i]; gbdec[i] = gbdec[j]; gbdec[j] = tmp;
		    tmp = resid[i]; resid[i] = resid[j]; resid[j] = tmp;
		    }
		}
	    }

	/* Cut off points at residual of two sigma */
	for (i = 0; i < nmatch; i++) {
	    if (resid[i] > siglim) {
		if (i > bestbin) bestbin = i - 1;
		break;
		}
	    }

	xref_p = wcs->xref;
	if (xref_p < 0.0) xref_p = 360.0 + xref_p;
	yref_p = wcs->yref;
	xrefpix = wcs->xrefpix;
	yrefpix = wcs->yrefpix;
	nbin_p = bestbin;
	wcs_amoeba (wcs);

	if (debug) {
	    ra2str (rastr, 16, wcs->xref, 3);
	    dec2str (decstr, 16, wcs->yref, 2);
	    fprintf (stderr,"\nresid solution:\n");
	    fprintf (stderr,"\n%d points < %.3f arcsec residuals refit\n",
			    bestbin, siglim);
	    fprintf (stderr," cra= %s cdec= %s del=%7.4f,%7.4f rot=%7.4f ",
		 rastr, decstr, 3600.0*wcs->xinc, 3600.0*wcs->yinc, wcs->rot);
	    fprintf (stderr,"(%8.2f,%8.2f)\n", wcs->xrefpix, wcs->yrefpix);
	    }
	free (resid);
	free (xe);
	free (ye);
	}

    return (nmatch);
}

struct WorldCoor *wcsf;

static double wcs_chisqr ();

/* From Numerical Recipes */
void amoeba();
static double amotry();


/* Set up the necessary temp arrays and call the amoeba() multivariate solver */

static void
wcs_amoeba (wcs0)

struct WorldCoor *wcs0;

{
    double *p[NPAR1];				  /* used as p[NPAR1][NPAR] */
    double vguess[NPAR], vp[NPAR], vdiff[NPAR];
    double p0[NPAR], p1[NPAR], p2[NPAR], p3[NPAR], p4[NPAR],
	   p5[NPAR], p6[NPAR], p7[NPAR], p8[NPAR]; /* used as px[0..NPAR-1] */
    double y[NPAR1];				  /* used as y[1..NPAR] */
    double xinc1, yinc1, xrefpix1, yrefpix1, rot, cd[4];
    double sumx, sumy, sumr;
    int iter;
    int i, j;
    int nfit1;
    char rastr[16],decstr[16];
    int nitmax;

    nitmax = nitmax0;
    if (nfit > NPAR)
	nfit = NPAR;
    nfit1 = nfit + 1;
    wcsf = wcs0;

/* Initialize guess and difference vectors to zero */
    for (i = 0; i < NPAR; i++) {
	vguess[i] = 0.0;
	vdiff[i] = 0.0;
	}

    /* Optical axis center (RA and Dec degrees) */
    if (vfit[1] > -1) {
	vguess[vfit[1]] = 0.0;
	vdiff[vfit[1]] = 5.0 * wcsf->xinc;
	}
    if (vfit[2] > -1) {
	vguess[vfit[2]] = 0.0;
	vdiff[vfit[2]] = 5.0 * wcsf->yinc;
	}
    /* Second rotation about optical axis (degrees) -> CD matrix */
    if (vfit[6] > -1) {
	wcsf->rotmat = 1;
	vguess[vfit[3]] = wcsf->cd[0];
	vdiff[vfit[3]] = wcsf->xinc * 0.03;
	vguess[vfit[4]] = wcsf->cd[1];
	vdiff[vfit[4]] = wcsf->yinc * 0.03;
	vguess[vfit[5]] = wcsf->cd[2];
	vdiff[vfit[5]] = wcsf->xinc * 0.03;
	vguess[vfit[6]] = wcsf->cd[3];
	vdiff[vfit[6]] = wcsf->yinc * 0.03;
	}

    else {

    /* Plate scale at optical axis right ascension or both (degrees/pixel) */
	if (vfit[3] > -1) {
	    vguess[vfit[3]] = wcsf->xinc;
	    vdiff[vfit[3]] = wcsf->xinc * 0.03;
	    }

    /* Plate scale in declination at optical axis (degrees/pixel) */
	if (vfit[4] > -1) {
	    vguess[vfit[4]] = wcsf->yinc;
	    vdiff[vfit[4]] = wcsf->yinc * 0.03;
	    }

    /* Rotation about optical axis in degrees */
	if (vfit[5] > -1) {
	    vguess[vfit[5]] = wcsf->rot;
	    vdiff[vfit[5]] = 0.5;
	    }
	}

/* Reference pixel (optical axis) */
    if (vfit[7] > -1) {
	vguess[vfit[7]] = 0.0;
	vdiff[vfit[7]] = 10.0;
	}
    if (vfit[8] > -1) {
	vguess[vfit[8]] = 0.0;
	vdiff[vfit[8]] = 10.0;
	}

/* Set up matrix of nfit+1 initial guesses.
 * The supplied guess, plus one for each parameter altered by a small amount
 */
    p[0] = p0;
    if (nfit > 0) p[1] = p1;
    if (nfit > 1) p[2] = p2;
    if (nfit > 2) p[3] = p3;
    if (nfit > 3) p[4] = p4;
    if (nfit > 4) p[5] = p5;
    if (nfit > 5) p[6] = p6;
    if (nfit > 6) p[7] = p7;
    if (nfit > 7) p[8] = p8;
    for (i = 0; i <= nfit; i++) {
	for (j = 0; j < nfit; j++)
	    p[i][j] = vguess[j];
	if (i > 0 && i <= nfit)
	    p[i][i-1] = vguess[i-1] + vdiff[i-1];
	 y[i] = wcs_chisqr (p[i], -i);
	}

#define	PDUMP
#ifdef	PDUMP
    fprintf (stderr,"Before:\n");
    for (i = 0; i < nfit1; i++) {
	if (vfit[1] > -1)
	    ra2str (rastr, 16, p[i][vfit[1]] + xref_p, 3);
	else
	    ra2str (rastr, 16, wcsf->xref, 3);
	if (vfit[2] > -1)
	    dec2str (decstr, 16, p[i][vfit[2]]+yref_p, 2);
	else
	    dec2str (decstr, 16, wcsf->yref, 2);
	if (vfit[6] > -1) {
	    cd[0] = p[i][vfit[3]];
	    cd[1] = p[i][vfit[4]];
	    cd[2] = p[i][vfit[5]];
	    cd[3] = p[i][vfit[6]];
	    fprintf (stderr,"%d: %s %s CD: %7.5f,%7.5f,%7.5f,%7.5f ",
		    i, rastr, decstr, cd[0],cd[1],cd[2],cd[3]);
	    }
	else {
	    if (vfit[3] > -1)
		xinc1 = p[i][vfit[3]];
	    else
		xinc1 = wcsf->xinc;
	    if (vfit[4] > -1)
		yinc1 = p[i][vfit[4]];
	    else if (vfit[3] > -1) {
		if (xinc1 < 0)
		    yinc1 = -xinc1;
		else
		    yinc1 = xinc1;
		}
	    else
		yinc1 = wcsf->yinc;
	    if (vfit[5] > -1)
		rot = p[i][vfit[5]];
	    else
		rot = wcsf->rot;
	    fprintf (stderr,"%d: %s %s del=%6.4f,%6.4f rot=%5.3f ",
		    i, rastr, decstr, 3600.0*xinc1, 3600.0*yinc1, rot);
	    }

	if (vfit[7] > -1)
	    xrefpix1 = xrefpix + p[i][vfit[7]];
	else
	    xrefpix1 = wcsf->xrefpix;
	if (vfit[8] > -1)
	    yrefpix1 = yrefpix + p[i][vfit[8]];
	else
	    yrefpix1 = wcsf->yrefpix;
	fprintf (stderr,"(%8.2f,%8.2f) y=%g\n", xrefpix1, yrefpix1, y[i]);
	}
#endif

    amoeba (p, y, nfit, FTOL, nitmax, wcs_chisqr, &iter);

#define	PDUMP
#ifdef	PDUMP
    fprintf (stderr,"\nAfter:\n");
    for (i = 0; i < nfit1; i++) {
	if (vfit[1] > -1)
	    ra2str (rastr, 16, p[i][vfit[1]] + xref_p, 3);
	else
	    ra2str (rastr, 16, wcsf->xref, 3);
	if (vfit[2] > -1)
	    dec2str (decstr, 16, p[i][vfit[2]]+yref_p, 2);
	else
	    dec2str (decstr, 16, wcsf->yref, 2);
	if (vfit[6] > -1) {
	    cd[0] = p[i][vfit[3]];
	    cd[1] = p[i][vfit[4]];
	    cd[2] = p[i][vfit[5]];
	    cd[3] = p[i][vfit[6]];
	    fprintf (stderr,"%d: %s %s CD: %7.5f,%7.5f,%7.5f,%7.5f ",
		    i, rastr, decstr, cd[0],cd[1],cd[2],cd[3]);
	    }
	else {
	    if (vfit[3] > -1)
		xinc1 = p[i][vfit[3]];
	    else
		xinc1 = wcsf->xinc;
	    if (vfit[4] > -1)
		yinc1 = p[i][vfit[4]];
	    else if (vfit[3] > -1) {
		if (xinc1 < 0)
		    yinc1 = -xinc1;
		else
		    yinc1 = xinc1;
		}
	    else
		yinc1 = wcsf->yinc;
	    if (vfit[5] > -1)
		rot = p[i][vfit[5]];
	    else
		rot = wcsf->rot;
	    fprintf (stderr,"%d: %s %s del=%6.4f,%6.4f rot=%5.3f ",
		    i,rastr,decstr, 3600.0*xinc1, 3600.0*yinc1, rot);
	    }
	if (vfit[7] > -1)
	    xrefpix1 = xrefpix + p[i][vfit[7]];
	else
	    xrefpix1 = wcsf->xrefpix;
	if (vfit[8] > -1)
	    yrefpix1 = yrefpix + p[i][vfit[8]];
	else
	    yrefpix1 = wcsf->yrefpix;
	fprintf (stderr,"(%8.2f,%8.2f) y=%g\n", xrefpix1, yrefpix1, y[i]);
	}
#endif

    /* On return, all entries in p[1..NPAR] are within FTOL;
     * Return the average, though you could just pick the first one
     */
    for (j = 0; j < nfit; j++) {
	double sum = 0.0;
	for (i = 0; i < nfit1; i++)
	    sum += p[i][j];
	vp[j] = sum / (double)nfit1;
	}
    if (vfit[1] > -1) {
	wcsf->xref = xref_p + vp[vfit[1]];
	if (wcsf->xref < 0.0) wcsf->xref = 360.0 + wcsf->xref;
	}
    if (vfit[2] > -1)
	wcsf->yref = yref_p + vp[vfit[2]];
    if (vfit[6] > -1) {
	wcsf->cd[0] = vp[vfit[3]];
	wcsf->cd[1] = vp[vfit[4]];
	wcsf->cd[2] = vp[vfit[5]];
	wcsf->cd[3] = vp[vfit[6]];
	}
    else {
	if (vfit[3] > -1)
	    wcsf->xinc = vp[vfit[3]];
	if (vfit[4] > -1)
	    wcsf->yinc = vp[vfit[4]];
	else if (vfit[3] > -1) {
	    if (wcsf->xinc < 0)
		wcsf->yinc = -wcsf->xinc;
	    else
		wcsf->yinc = wcsf->xinc;
	    }
	if (vfit[5] > -1)
	    wcsf->rot = vp[vfit[5]];
	}
    if (vfit[7] > -1)
	wcsf->xrefpix = xrefpix + vp[vfit[7]];
    if (vfit[8] > -1)
	wcsf->yrefpix = yrefpix + vp[vfit[8]];

#define RESIDDUMP
#ifdef RESIDDUMP
    ra2str (rastr, 16, wcsf->xref, 3);
    dec2str (decstr, 16, wcsf->yref, 2);

    if (vfit[6] > -1)
	fprintf (stderr,"iter=%d\n cra= %s cdec= %s CD=%9.7f,%9.7f,%9.7f,%9.7f ", iter,
		rastr, decstr, wcsf->cd[0], wcsf->cd[1], wcsf->cd[2],
		wcsf->cd[3]);
    else
	fprintf (stderr,"iter=%d\n cra= %s cdec= %s del=%7.4f,%7.4f rot=%7.4f ", iter,
		rastr, decstr, wcsf->xinc*3600.0, wcsf->yinc*3600.0, wcsf->rot);
    fprintf (stderr,"(%8.2f,%8.2f)\n", wcsf->xrefpix, wcsf->yrefpix);
    sumx = 0.0;
    sumy = 0.0;
    sumr = 0.0;
    for (i = 0; i < nbin_p; i++) {
	double mra, mdec, ex, ey, er;
	char rastr[16], decstr[16];

	pix2wcs (wcsf, sx_p[i], sy_p[i], &mra, &mdec);
	ex = 3600.0 * (mra - gra_p[i]);
	ey = 3600.0 * (mdec - gdec_p[i]);
	er = sqrt (ex * ex + ey * ey);
	sumx = sumx + ex;
	sumy = sumy + ey;
	sumr = sumr + er;

	ra2str (rastr, 16, gra_p[i], 3);
	dec2str (decstr, 16, gdec_p[i], 2);
	fprintf (stderr,"%2d: c: %s %s ", i+1, rastr, decstr);
	ra2str (rastr, 16, mra, 3);
	dec2str (decstr, 16, mdec, 2);
	fprintf (stderr, "i: %s %s %6.3f %6.3f %6.3f\n",
		rastr, decstr, 3600.0*ex, 3600.0*ey,
		3600.0*sqrt(ex*ex + ey*ey));
	}
    sumx = sumx / (double)nbin_p;
    sumy = sumy / (double)nbin_p;
    sumr = sumr / (double)nbin_p;
    fprintf (stderr,"mean dra: %6.3f, ddec: %6.3f, dr = %6.3f\n", sumx, sumy, sumr);
#endif
}


/* Compute the chisqr of the vector v, where
 * v[0]=cra, v[1]=cdec, v[2]=ra deg/pix, v[3]=dec deg/pix,
 * v[4]=rotation, v[5]=2nd rotation->CD matrix, v[6]=ref x, and v[7] = ref y
 * chisqr is in arcsec^2
 */

static double
wcs_chisqr (v, iter)

double	*v;	/* Vector of parameter values */
int	iter;	/* Number of iterations */

{
    double chsq;
    char rastr[16],decstr[16];
    double xmp, ymp, dx, dy, cd[4], *cdx;
    double crval1, crval2, cdelt1, cdelt2, crota, crpix1, crpix2;
    int i, offscale;

    /* Set WCS parameters from fit parameter vector */

    /* Sky coordinates at optical axis (degrees) */
    if (vfit[1] > -1)
	crval1 = xref_p + v[vfit[1]];
    else
	crval1 = wcsf->xref;
    if (vfit[2] > -1)
	crval2 = yref_p + v[vfit[2]];
    else
	crval2 = wcsf->yref;

    /* CD matrix */
    if (vfit[6] > -1) {
	cdelt1 = 0.0;
	cdelt2 = 0.0;
	crota = 0.0;
	cd[0] = v[vfit[3]];
	cd[1] = v[vfit[4]];
	cd[2] = v[vfit[5]];
	cd[3] = v[vfit[6]];
	cdx = cd;
	}

    else {
	/* Plate scale (degrees/pixel) */
	if (vfit[3] > -1)
	    cdelt1 = v[vfit[3]];
	else
	    cdelt1 = wcsf->xinc;
	if (vfit[4] > -1)
	    cdelt2 = v[vfit[4]];
	else if (vfit[3] > -1) {
	    if (cdelt1 < 0)
		cdelt2 = -cdelt1;
	    else
		cdelt2 = cdelt1;
	    }
	else
	    cdelt2 = wcsf->yinc;

	/* Rotation angle (degrees) */
	if (vfit[5] > -1)
	    crota = v[vfit[5]];
	else
	    crota = wcsf->rot;
	cdx = NULL;
	}

    /* Optical axis pixel coordinates */
    if (vfit[7] > -1)
	crpix1 = xrefpix + v[vfit[7]];
    else
	crpix1 = wcsf->xrefpix;
    if (vfit[8] > -1)
	crpix2 = yrefpix + v[vfit[8]];
    else
	crpix2 = wcsf->yrefpix;
    if (wcsreset (wcsf,crpix1,crpix2,crval1,crval2,cdelt1,cdelt2,crota,cdx)) {
	fprintf (stderr,"CHISQR: Cannot reset WCS!\n");
	return (0.0);
	}

    /* Compute sum of squared residuals for these parameters */
    chsq = 0.0;
    for (i = 0; i < nbin_p; i++) {
	wcs2pix (wcsf, gra_p[i], gdec_p[i], &xmp, &ymp, &offscale);
	/* if (!offscale) { */
	    dx = xmp - sx_p[i];
	    dy = ymp - sy_p[i];
	    chsq += dx*dx + dy*dy;
	    /* } */
	}

#define TRACE_CHSQR
#ifdef TRACE_CHSQR
    ra2str (rastr, 16, wcsf->xref, 3);
    dec2str (decstr, 16, wcsf->yref, 2);
    if (vfit[6] > -1)
	fprintf (stderr,"%4d: %s %s CD: %9.7f,%9.7f,%9.7f,%9.7f ",
		iter, rastr, decstr, wcsf->cd[0],wcsf->cd[1],wcsf->cd[2],
		wcsf->cd[3]);
    else
	fprintf (stderr,"%4d: %s %s %9.7f,%9.7f %8.5f ",
		iter, rastr, decstr, wcsf->xinc*3600.0, wcsf->yinc*3600.0,
		wcsf->rot);
    fprintf (stderr,"(%8.2f,%8.2f) -> %f\r",
	     wcsf->xrefpix, wcsf->yrefpix, chsq);
#endif
    return (chsq);
}

/* The following subroutines are based on those in Numerical Recipes in C */

/* amoeba.c */

#define ALPHA 1.0
#define BETA 0.5
#define GAMMA 2.0

void
amoeba (p, y, ndim, ftol, itmax, funk, nfunk)

double	**p;
double	y[];
double	ftol;
int	itmax;
double	(*funk)();
int	ndim;
int	*nfunk;

{
int i,j,ilo,ihi,inhi,ndim1=ndim+1;
double ytry,ysave,sum,rtol,*psum;

    psum = (double *) malloc ((unsigned)ndim * sizeof(double));
    *nfunk = 0;
    for (j=0; j<ndim; j++) {
	for (i=0,sum=0.0; i<ndim1; i++)
	    sum += p[i][j]; psum[j]=sum;
	}
    for (;;) {
	ilo=1;
	if (y[0] > y[1]) {
	    inhi = 1;
	    ihi = 0;
	    }
	else {
	    inhi = 0;
	    ihi = 1;
	    }
	for (i = 0; i < ndim1; i++) {
	    if (y[i] < y[ilo])
		ilo=i;
	    if (y[i] > y[ihi]) {
		inhi=ihi;
		ihi=i;
		}
	    else if (y[i] > y[inhi])
		if (i != ihi)
		    inhi=i;
	    }
	rtol = 2.0 * fabs(y[ihi]-y[ilo]) / (fabs(y[ihi]) + fabs(y[ilo]));
	if (rtol < ftol)
	    break;
	if (*nfunk >= itmax) {
	    fprintf (stderr,"Too many iterations in amoeba fit %d > %d",*nfunk,itmax);
	    return;
	    }
	ytry = amotry (p, y, psum, ndim, funk, ihi, nfunk, -ALPHA);
	if (ytry <= y[ilo])
	    ytry = amotry (p, y, psum, ndim, funk, ihi, nfunk, GAMMA);
	else if (ytry >= y[inhi]) {
	    ysave = y[ihi];
	    ytry = amotry (p,y,psum,ndim,funk,ihi,nfunk,BETA);
	    if (ytry >= ysave) {
		for (i = 0; i < ndim1; i++) {
		    if (i != ilo) {
			for (j = 0; j < ndim; j++) {
			    psum[j] = 0.5 * (p[i][j] + p[ilo][j]);
			    p[i][j] = psum[j];
			    }
			y[i]=(*funk)(psum, *nfunk);
			}
		    }
		*nfunk += ndim;
		for (j=0; j<ndim; j++) {
		    for (i=0,sum=0.0; i<ndim1; i++)
			sum += p[i][j]; psum[j]=sum;
		    }
		}
	    }
	}
    free (psum);
    return;
}


static double
amotry (p, y, psum, ndim, funk, ihi, nfunk, fac)

double	**p;
double	*y;
double	*psum;
double	(*funk)();
double	fac;
int	ndim;
int	ihi;
int	*nfunk;

{
    int j;
    double fac1,fac2,ytry,*ptry;

    ptry = (double *) malloc ((unsigned) ndim * sizeof(double));
    fac1 = (1.0 - fac) / ndim;
    fac2 = fac1 - fac;
    for (j = 0; j < ndim; j++)
	ptry[j] = psum[j] * fac1 - p[ihi][j] * fac2;
    ytry = (*funk)(ptry, *nfunk);
    ++(*nfunk);
    if (ytry < y[ihi]) {
	y[ihi] = ytry;
	for (j = 0; j < ndim; j++) {
    	    psum[j] +=  ptry[j] - p[ihi][j];
    	    p[ihi][j] = ptry[j];
	    }
	}
    free (ptry);
    return ytry;
}

void
setresid_refine (refine)
int refine;
{ resid_refine = refine; return; }

int
getresid_refine ()
{ return (resid_refine); }

void
setnfit (nfit)
int nfit;
{
    if (nfit == 0)
	setnofit();
    else if (nfit < 0) {
	pfit0 = -nfit;
	resid_refine = 1;
	}
    else {
	pfit0 = nfit;
	resid_refine = 0;
	}
    return;
}

int
getnfit ()
{ return (pfit0); }

int
iscdfit ()
{ return (cdfit); }

void
setminmatch (minmatch)
int minmatch;
{ minmatch0 = minmatch; return; }

void
setminbin (minbin1)
int minbin1;
{ minbin = minbin1; return; }

void
setnitmax (nitmax)
int nitmax;
{ nitmax0 = nitmax; return; }

/* Aug  6 1996	New subroutine
 * Sep  1 1996	Move constants to lwcs.h
 * Sep  3 1996	Use offscale pixels for chi^2 computation
 * Sep  3 1996	Overprint chi^2 in verbose mode
 * Oct 15 1996	Fix am* subroutine declarations
 * Nov 19 1996	Fix bug regarding rotation
 *
 * Jul 21 1997	Add reference pixel position fitting
 * Aug  4 1997	Increase maximum iterations from 750 to 1000 in lwcs.h
 * Aug 28 1997	Fix VGUESS dimension bug
 * Sep  9 1997	Print RA and Dec offsets in residual listing
 * Sep  9 1997	Turn on resid_refinement if number of parameters to fit negated
 * Sep  9 1997	Fit separate horizontal and vertical plate scales if nfit=5
 * Sep  9 1997	Fix bugs associated with fitting optical axis
 * Sep 12 1997	Add chip rotation instead of second plate scale
 * Oct  2 1997	Keep second plate scale AND chip rotation
 * Oct 16 1997	Try to deal with reference pixel position correctly
 * Nov  5 1997	Select parameters one at a time, in any order
 * Nov 12 1997	Add PFIT=3 to fit center and plate scale only
 * Dec 15 1997	Fix minor bugs after lint
 *
 * Jan 26 1998	Remove chip rotation code
 * Jan 29 1998	Streamline initialization code
 * Feb 19 1998	Fix bug in initialization code
 * Mar  3 1998	Fix residual-refining code
 * Mar 20 1998	Add option to fit CD matrix
 * Mar 25 1998	Make amoeba() externally callable
 * Mar 26 1998	Return instead of crashing when too many iterations
 * Apr 21 1998	Drop out of loop if more than half of stars are matched
 * Apr 27 1998	Fix bug handling nfit=8
 * Jun 24 1998	Fix bug summing unitialized values for mean after fit
 * Jun 24 1998	Add string lengths to ra2str() and dec2str() calls
 * Oct  8 1998	Initialize bestdx and bestdy to zero
 * Dec  8 1998	Fix declaration of amotry()
 *
 * Apr 21 1999	Add subroutines to set and retrieve resid_refine independently
 * Jul 21 1999	Add FitMatch() to fit WCS to already-matched stars
 * Sep  8 1999	Fix bug found by Jean-Baptiste Marquette
 * Oct  1 1999	Add ReadMatch() to read a set of matches from a file
 * Oct 20 1999	Include wcscat.h
 *
 * Feb 15 2000	Add iscdfit() to return whether CD matrix is being fit
 * Mar 10 2000	Add debug statement to list max matches as they are found
 * Mar 10 2000	Change loop order to image stars first
 * Dec 18 2000	Write half of ReadMatch() to deal with ASCII files
 *
 * Jan  2 2001	Modify ReadMatch() to read hh mm ss dd mm ss, too
 * Jan  9 2001	Work on FitMatch()
 * Jan 11 2001	All diagnostic printing goes to stderr
 * Feb 28 2001	Ignore coordinate system if present after match file coordinates
 * Jun 18 2001	Add maximum length of returned string to getoken()
 * Aug  2 2001	Separate parameter listing and counting into subroutines
 * Sep 19 2001	Drop fitshead.h; it is in wcs.h
 * Sep 24 2001	Ease match numeric criterium if half num is > 40
 * Oct 15 2001	Simplify error message
 * Oct 16 2001	Read minimum match to drop out of loop from lwcs.h
 * Oct 31 2001	Simplify innermost loop to try for more speed
 * Nov  1 2001	Add goff to StarMatch() arguments
 * Nov  5 2001	Use current WCS with no offset before trying offset matching
 * Nov  6 2001	Add setnitmax() to set maximum number of amoeba iterations
 * Nov  7 2001	Add setminbin to set minimum number of matches for fit
 * Nov 16 2001	Allocate slightly more than maxbin to handle dense fields
 *
 * Jul 31 2002	Add getnfit() to return current number of parameters being fit
 * Aug 30 2002	Fix WCSMatch() to set scale in arcsec, not degrees
 *
 * Jan 30 2003	Remove uninitialized variable in WCSMatch()
 * Mar 13 2003	Do not include malloc.h on Apples and Convexes
 * Apr  3 2003	Clean up code with lint
 * Nov 18 2003	Drop include of malloc.h; it is in stdlib.h
 *
 * Aug 30 2004	Declare void various external set*() calls
 *
 * Jun 19 2006	Initialize unitialized variables dxs and dys
 */ 
