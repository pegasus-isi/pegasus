/*** File polfit.c
 *** April 3, 2003
 *** By Doug Mink, after Bevington, page 141

 *--- Polynomial least squares fitting program, almost identical to the
 *    one in Bevington, "Data Reduction and Error Analysis for the
 *    Physical Sciences," page 141.  The argument list was changed and
 *    the weighting removed.
 *      y = a(1) + a(2)*(x-x0) + a(3)*(x-x0)**2 + a(3)*(x-x0)**3 + . . .
 */

#include <stdlib.h>
#include <stdio.h>
#include <math.h>

#include "wcscat.h"

static double determ();

void
polfit (x, y, x0, npts, nterms, a, stdev)

double	*x;		/* Array of independent variable points */
double	*y;		/* Array of dependent variable points */
double	x0;		/* Offset to independent variable */
int	npts;		/* Number of data points to fit */
int	nterms;		/* Number of parameters to fit */
double	*a;		/* Vector containing current fit values */
double	*stdev; 	/* Standard deviation of fit (returned) */
{
    double sigma2sum;
    double xterm,yterm,xi,yi;
    double *sumx, *sumy;
    double *array;
    int i,j,k,l,n,nmax;
    double delta;

    /* accumulate weighted sums */
    nmax = 2 * nterms - 1;
    sumx = (double *) calloc (nmax, sizeof(double));
    sumy = (double *) calloc (nterms, sizeof(double));
    for (n = 0; n < nmax; n++)
	sumx[n] = 0.0;
    for (j = 0; j < nterms; j++)
	sumy[j] = 0.0;
    for (i = 0; i < npts; i++) {
	xi = x[i] - x0;
	yi = y[i];
	xterm = 1.0;
	for (n = 0; n < nmax; n++) {
	    sumx[n] = sumx[n] + xterm;
	    xterm = xterm * xi;
	    }
	yterm = yi;
	for (n = 0; n < nterms; n++) {
	    sumy[n] = sumy[n] + yterm;
	    yterm = yterm * xi;
	    }
	}

    /* Construct matrices and calculate coeffients */
    array = (double *) calloc (nterms*nterms, sizeof(double));
    for (j = 0; j < nterms; j++) {
	for (k = 0; k < nterms; k++) {
	    n = j + k;
	    array[j+k*nterms] = sumx[n];
	    }
	}
    delta = determ (array, nterms);
    if (delta == 0.0) {
	*stdev = 0.;
	for (j = 0; j < nterms; j++)
	    a[j] = 0. ;
	free (array);
	free (sumx);
	free (sumy);
	return;
	}

    for (l = 0; l < nterms; l++) {
	for (j = 0; j < nterms; j++) {
	    for (k = 0; k < nterms; k++) {
		n = j + k;
		array[j+k*nterms] = sumx[n];
		}
	    array[j+l*nterms] = sumy[j];
	    }
	a[l] = determ (array, nterms) / delta;
	}

    /* Calculate sigma */
    sigma2sum = 0.0;
    for (i = 0; i < npts; i++) {
	yi = polcomp (x[i], x0, nterms, a);
	sigma2sum = sigma2sum + ((y[i] - yi) * (y[i] - yi));
	}
    *stdev = sqrt (sigma2sum / (double) (npts - 1));

    free (array);
    free (sumx);
    free (sumy);
    return;
}


/*--- Calculate the determinant of a square matrix
 *    This subprogram destroys the input matrix array
 *    From Bevington, page 294.
 */

static double
determ (array, norder)

double	*array;		/* Input matrix array */
int	norder;		/* Order of determinant (degree of matrix) */

{
    double save, det;
    int i,j,k,k1, zero;

    det = 1.0;
    for (k = 0; k < norder; k++) {

	/* Interchange columns if diagonal element is zero */
	if (array[k+k*norder] == 0) {
	    zero = 1;
	    for (j = k; j < norder; j++) {
		if (array[k+j*norder] != 0.0)
		    zero = 0;
		}
	    if (zero)
		return (0.0);

	    for (i = k; i < norder; i++) {
		save = array[i+j*norder]; 
		array[i+j*norder] = array[i+k*norder];
		array[i+k*norder] = save ;
		}
	    det = -det;
	    }

	/* Subtract row k from lower rows to get diagonal matrix */
	det = det * array[k+k*norder];
	if (k < norder - 1) {
	    k1 = k + 1;
	    for (i = k1; i < norder; i++) {
		for (j = k1; j < norder; j++) {
		    array[i+j*norder] = array[i+j*norder] -
				      (array[i+k*norder] * array[k+j*norder] /
				      array[k+k*norder]);
		    }
		}
	    }
	}
	return (det);
}

/* POLCOMP -- Polynomial evaluation
 *	Y = A(1) + A(2)*X + A(3)*X**2 + A(3)*X**3 + . . . */

double
polcomp (xi, x0, norder, a)

double	xi;	/* Independent variable */
double	x0;	/* Offset to independent variable */
int	norder;	/* Number of coefficients */
double	*a;	/* Vector containing coeffiecients */
{
    double xterm, x, y;
    int iterm;

    /* Accumulate polynomial value */
    x = xi - x0;
    y = 0.0;
    xterm = 1.0;
    for (iterm = 0; iterm < norder; iterm++) {
	y = y + a[iterm] * xterm;
	xterm = xterm + x;
	}
    return (y);
}

/* Sep 10 1987	Program written
 *
 * Mar 17 1993	Add x offset
 *
 * Feb 23 1998	Translate to C
 *
 * Jul 25 2001	Add polcomp to return computed values
 *
 * Apr  3 2003	Drop unused variable freedom in polfit()
 */
