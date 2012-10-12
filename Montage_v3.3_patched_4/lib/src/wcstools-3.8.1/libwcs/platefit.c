/*** File libwcs/platefit.c
 *** September 26, 2006
 *** By Doug Mink, dmink@cfa.harvard.edu
 *** Harvard-Smithsonian Center for Astrophysics
 *** Copyright (C) 1998-2006
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

/*  Nonlinear least squares fitting program using data arrays starting
 *  at x and y to fit array starting at z.
 *  Contains convergence oscillation damping and optional normalization 
 *  Fits up to MAXPAR parameters
 */

#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include "wcs.h"
#include "lwcs.h"

static void plate_amoeba();
static double plate_chisqr();
static int ncoeff=0;
static double   *sx_p;
static double   *sy_p;
static double   *gx_p;
static double   *gy_p;
static int	nbin_p;
extern int SetPlate();

#define MAXPAR 26
#define MAXPAR1 27
#define NITMAX 2500

int
FitPlate (wcs, x, y, x1, y1, np, ncoeff0, debug)

struct WorldCoor *wcs;	/* World coordinate system structure */
double	*x, *y;		/* Image WCS coordinates */
double	*x1, *y1;	/* Image pixel coordinates */
int	np;		/* Number of points to fit */
int	ncoeff0;	/* Order of polynomial terms in x and y */
int	debug;

{
    sx_p = x;
    sy_p = y;
    gx_p = x1;
    gy_p = y1;
    nbin_p = np;
    ncoeff = ncoeff0;

    /* Fit polynomials */
    plate_amoeba (wcs);

    return (0);
}

static struct WorldCoor *wcsp;

/* Set up the necessary temp arrays and call the amoeba() multivariate solver */

static void
plate_amoeba (wcs0)

struct WorldCoor *wcs0;

{
    double *p[MAXPAR1];				  /* used as p[NPAR1][NPAR] */
    double vguess[MAXPAR], vp[MAXPAR], vdiff[MAXPAR];
    double y[MAXPAR1];				  /* used as y[1..NPAR] */
    double sumx, sumy, sumr;
    int nbytes;
    int iter;
    int i, j;
    int nfit, nfit1;
    int nitmax;
    extern void amoeba();

    /* Allocate memory for fit */
    nfit = ncoeff * 2;
    nfit1 = nfit + 1;
    nbytes = nfit * sizeof (double);
    for (i = 0; i < nfit1; i++)
	p[i] = (double *) malloc (nbytes);

    nitmax = 6000;
    wcsp = wcs0;

/* Zero guess and difference vectors */
    for (i = 0; i < MAXPAR; i++) {
	vguess[i] = 0.0;
	vdiff[i] = 0.0;
	vp[i] = 0.0;
	}

    if (nfit > 0) {
	double dra = wcsp->cdelt[0];
	double ddec = wcsp->cdelt[1];
	vguess[0] = 0.0;
	vdiff[0] = 5.0 * dra;
	vguess[1] = wcsp->cd[0];
	vdiff[1] = 0.05 * dra;
	vguess[2] = wcsp->cd[1];
	vdiff[2] = 0.05 * ddec;
	vguess[3] = 0.0;
	vdiff[3] = 0.001 * dra;
	vguess[4] = 0.0;
	vdiff[4] = 0.001 * ddec;
	vguess[5] = 0.0;
	vdiff[5] = 0.001 * ddec;
	if (ncoeff > 6) {
	    vguess[6] = 0.0;
	    vdiff[6] = 0.001 * ddec;
	    vguess[7] = 0.0;
	    vdiff[7] = 0.001 * ddec;
	    }
	vguess[ncoeff+0] = 0.0;
	vdiff[ncoeff+0] = 5.0 * ddec;
	vguess[ncoeff+1] = wcsp->cd[2];
	vdiff[ncoeff+1] = 0.05 * ddec;
	vguess[ncoeff+2] = wcsp->cd[3];
	vdiff[ncoeff+2] = 0.05 * dra;
	vguess[ncoeff+3] = 0.0;
	vdiff[ncoeff+3] = 0.001 * ddec;
	vguess[ncoeff+4] = 0.0;
	vdiff[ncoeff+4] = 0.001 * dra;
	vguess[ncoeff+5] = 0.0;
	vdiff[ncoeff+5] = 0.001 * ddec;
	if (ncoeff > 6) {
	    vguess[ncoeff+6] = 0.0;
	    vdiff[ncoeff+6] = 0.001 * dra;
	    vguess[ncoeff+7] = 0.0;
	    vdiff[ncoeff+7] = 0.001 * ddec;
	    }
	}

    /* Set up matrix of nfit+1 initial guesses.
     * The supplied guess, plus one for each parameter altered by a small amount
     */
    for (i = 0; i < nfit1; i++) {
	for (j = 0; j < nfit; j++)
	    p[i][j] = vguess[j];
	if (i > 0)
	    p[i][i-1] = vguess[i-1] + vdiff[i-1];
	y[i] = plate_chisqr (p[i], -i);
	}

#define	PDUMP
#ifdef	PDUMP
    fprintf (stderr,"Before:\n");
    for (i = 0; i < nfit1; i++) {
	fprintf (stderr,"%3d: ", i);
	for (j = 0; j < ncoeff; j++)
	    fprintf (stderr," %9.7f",p[i][j]);
	fprintf (stderr,"\n     ");
	for (j = 0; j < ncoeff; j++)
	    fprintf (stderr," %9.7f",p[i][ncoeff+j]);
	fprintf (stderr,"\n");
	}
#endif

    amoeba (p, y, nfit, FTOL, nitmax, plate_chisqr, &iter);

#define	PDUMP
#ifdef	PDUMP
    fprintf (stderr,"\nAfter:\n");
    for (i = 0; i < nfit1; i++) {
	fprintf (stderr,"%3d: ", i);
	for (j = 0; j < ncoeff; j++)
	    fprintf (stderr," %9.7f",p[i][j]);
	fprintf (stderr,"\n     ");
	for (j = 0; j < ncoeff; j++)
	    fprintf (stderr," %9.7f",p[i][ncoeff+j]);
	fprintf (stderr,"\n");
	}
#endif

    /* on return, all entries in p[1..NPAR] are within FTOL; average them */
    for (j = 0; j < nfit; j++) {
	double sum = 0.0;
        for (i = 0; i < nfit1; i++)
	    sum += p[i][j];
	vp[j] = sum / (double)nfit1;
	}
    (void)SetPlate (wcsp, ncoeff, ncoeff, vp);

#define RESIDDUMP
#ifdef RESIDDUMP
    fprintf (stderr,"iter=%4d\n  ", iter);
    for (j = 0; j < ncoeff; j++)
	    fprintf (stderr," %9.7f",vp[j]);
    fprintf (stderr,"\n    ");
    for (j = 0; j < ncoeff; j++)
	    fprintf (stderr," %9.7f",vp[j+6]);
    fprintf (stderr,"\n");

    sumx = 0.0;
    sumy = 0.0;
    sumr = 0.0;
    for (i = 0; i < nbin_p; i++) {
	double mx, my, ex, ey, er;
	char rastr[32], decstr[32];

	pix2wcs (wcsp, sx_p[i], sy_p[i], &mx, &my);
	ex = 3600.0 * (mx - gx_p[i]);
	ey = 3600.0 * (my - gy_p[i]);
	er = sqrt (ex * ex + ey * ey);
	sumx = sumx + ex;
	sumy = sumy + ey;
	sumr = sumr + er;

	ra2str (rastr, 31, gx_p[i], 3);
	dec2str (decstr, 31, gy_p[i], 2);
	fprintf (stderr,"%2d: c: %s %s ", i+1, rastr, decstr);
	ra2str (rastr, 31, mx, 3);
	dec2str (decstr, 31, my, 2);
	fprintf (stderr,"i: %s %s %6.3f %6.3f %6.3f\n",
		rastr, decstr, 3600.0*ex, 3600.0*ey,
		3600.0*sqrt(ex*ex + ey*ey));
	}
    sumx = sumx / (double)nbin_p;
    sumy = sumy / (double)nbin_p;
    sumr = sumr / (double)nbin_p;
    fprintf (stderr,"mean dra: %6.3f, ddec: %6.3f, dr = %6.3f\n", sumx, sumy, sumr);
#endif

    for (i = 0; i < nfit1; i++)
	free (p[i]);
    return;
}


/* Compute the chisqr of the vector v, where v[i]=plate fit coeffients
 * chisqr is in arcsec^2
 */

static double
plate_chisqr (v, iter)

double	*v;	/* Vector of parameter values */
int	iter;	/* Number of iterations */

{
    double chsq;
    double xsp, ysp, dx, dy;
    int i, j;
    extern int SetPlate();

    /* Set plate constants from fit parameter vector */
    if (SetPlate (wcsp, ncoeff, ncoeff, v)) {
	fprintf (stderr,"CHISQR: Cannot reset WCS!\n");
	return (0.0);
	}

    /* Compute sum of squared residuals for these parameters */
    chsq = 0.0;
    for (i = 0; i < nbin_p; i++) {
	pix2wcs (wcsp, sx_p[i], sy_p[i], &xsp, &ysp);
	dx =3600.0 * (xsp - gx_p[i]);
	dy = 3600.0 * (ysp - gy_p[i]);
	chsq += dx*dx + dy*dy;
	}

#define TRACE_CHSQR
#ifdef TRACE_CHSQR
    fprintf (stderr,"%4d:", iter);
    for (j = 0; j < ncoeff; j++)
	fprintf (stderr," %9.4g",v[j]);
    for (j = 0; j < ncoeff; j++)
	fprintf (stderr," %9.4g",v[ncoeff+j]);
    fprintf (stderr," -> %f\r", chsq);
#endif
    return (chsq);
}

/* Mar 30 1998	New subroutines
 * Apr  7 1998	Add x^3 and y^3 terms
 * Apr 10 1998	Add second number of coefficients
 * May 14 1998	include stdio.h for stderr
 * Jun 24 1998	Add string lengths to ra2str() and dec2str() calls
 * Oct 15 1999	Include stdlib.h for malloc() declaration
 *
 * Jan 11 2001	Print all messages to stderr
 *
 * Sep 26 2006	Increase length of rastr and destr from 16 to 32
 */
