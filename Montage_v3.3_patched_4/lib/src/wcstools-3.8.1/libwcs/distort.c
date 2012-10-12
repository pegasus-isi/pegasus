/*** File libwcs/distort.c
 *** January 4, 2007
 *** By Doug Mink, dmink@cfa.harvard.edu, 
 *** Based on code written by Jing Li, IPAC
 *** Harvard-Smithsonian Center for Astrophysics
 *** Copyright (C) 2004-2007
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

 * Module:	distort.c (World Coordinate Systems)
 * Purpose:	Convert focal plane coordinates to pixels and vice versa:
 * Subroutine:  distortinit (wcs, hstring) set distortion coefficients from FITS header
 * Subroutine:  DelDistort (header, verbose) delete distortion coefficients in FITS header
 * Subroutine:	pix2foc (wcs, x, y, u, v) pixel coordinates -> focal plane coordinates
 * Subroutine:	foc2pix (wcs, u, v, x, y) focal plane coordinates -> pixel coordinates
 * Subroutine:  setdistcode (wcs,ctype) sets distortion code from CTYPEi
 * Subroutine:  getdistcode (wcs) returns distortion code string for CTYPEi
 */

#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "wcs.h"

void
distortinit (wcs, hstring)
struct WorldCoor *wcs;  /* World coordinate system structure */
const char *hstring;	/* character string containing FITS header information
			   in the format <keyword>= <value> [/ <comment>] */
{
    int i, j, m;
    char keyword[12];

    /* Read distortion coefficients, if present */
    if (wcs->distcode == DISTORT_SIRTF) {
	if (wcs->wcsproj == WCS_OLD) {
	    wcs->wcsproj = WCS_NEW;
	    wcs->distort.a_order = 0;
	    wcs->distort.b_order = 0;
	    wcs->distort.ap_order = 0;
	    wcs->distort.bp_order = 0;
	    }
	else {
	    if (!hgeti4 (hstring, "A_ORDER", &wcs->distort.a_order)) {
		setwcserr ("DISTINIT: Missing A_ORDER keyword for SIRTF distortion");
		}
	    else {
		m = wcs->distort.a_order;
		for (i = 0; i <= m; i++) {
		    for (j = 0; j <= m; j++) {
			wcs->distort.a[i][j] = 0.0;
			}
		    }
		for (i = 0; i <= m; i++) {
		    for (j = 0; j <= m-i; j++) {
			sprintf (keyword, "A_%d_%d", i, j);
			hgetr8 (hstring, keyword, &wcs->distort.a[i][j]);
			}
		    }
		}
	    if (!hgeti4 (hstring, "B_ORDER", &wcs->distort.b_order)) {
		setwcserr ("DISTINIT: Missing B_ORDER keyword for SIRTF distortion");
		}
	    else {
		m = wcs->distort.b_order;
		for (i = 0; i <= m; i++) {
		    for (j = 0; j <= m; j++) {
			wcs->distort.b[i][j] = 0.0;
			}
		    }
		for (i = 0; i <= m; i++) {
		    for (j = 0; j <= m-i; j++) {
			sprintf (keyword, "B_%d_%d", i, j);
			hgetr8 (hstring, keyword, &wcs->distort.b[i][j]);
			}
		    }
		}
	    if (!hgeti4 (hstring, "AP_ORDER", &wcs->distort.ap_order)) {
		setwcserr ("DISTINIT: Missing AP_ORDER keyword for SIRTF distortion");
		}
	    else {
		m = wcs->distort.ap_order;
		for (i = 0; i <= m; i++) {
		    for (j = 0; j <= m; j++) {
			wcs->distort.ap[i][j] = 0.0;
			}
		    }
		for (i = 0; i <= m; i++) {
		    for (j = 0; j <= m-i; j++) {
			sprintf (keyword, "AP_%d_%d", i, j);
			hgetr8 (hstring, keyword, &wcs->distort.ap[i][j]);
			}
		    }
		}
	    if (!hgeti4 (hstring, "BP_ORDER", &wcs->distort.bp_order)) {
		setwcserr ("DISTINIT: Missing BP_ORDER keyword for SIRTF distortion");
		}
	    else {
		m = wcs->distort.bp_order;
		for (i = 0; i <= m; i++) {
		    for (j = 0; j <= m; j++) {
			wcs->distort.bp[i][j] = 0.0;
			}
		    }
		for (i = 0; i <= m; i++) {
		    for (j = 0; j <= m-i; j++) {
			sprintf (keyword, "BP_%d_%d", i, j);
			hgetr8 (hstring, keyword, &wcs->distort.bp[i][j]);
			}
		    }
		}
	    }
	}
    return;
}


/* Delete all distortion-related fields.
 * return 0 if at least one such field is found, else -1.  */

int
DelDistort (header, verbose)

char *header;
int verbose;

{
    char keyword[16];
    char str[32];
    int i, j, m;
    int lctype;
    int n;

    n = 0;

    if (hgeti4 (header, "A_ORDER", &m)) {
	for (i = 0; i <= m; i++) {
	    for (j = 0; j <= m-i; j++) {
		sprintf (keyword, "A_%d_%d", i, j);
		hdel (header, keyword);
		n++;
		}
	    }
	hdel (header, "A_ORDER");
	n++;
	}

    if (hgeti4 (header, "AP_ORDER", &m)) {
	for (i = 0; i <= m; i++) {
	    for (j = 0; j <= m-i; j++) {
		sprintf (keyword, "AP_%d_%d", i, j);
		hdel (header, keyword);
		n++;
		}
	    }
	hdel (header, "AP_ORDER");
	n++;
	}

    if (hgeti4 (header, "B_ORDER", &m)) {
	for (i = 0; i <= m; i++) {
	    for (j = 0; j <= m-i; j++) {
		sprintf (keyword, "B_%d_%d", i, j);
		hdel (header, keyword);
		n++;
		}
	    }
	hdel (header, "B_ORDER");
	n++;
	}

    if (hgeti4 (header, "BP_ORDER", &m)) {
	for (i = 0; i <= m; i++) {
	    for (j = 0; j <= m-i; j++) {
		sprintf (keyword, "BP_%d_%d", i, j);
		hdel (header, keyword);
		n++;
		}
	    }
	hdel (header, "BP_ORDER");
	n++;
	}

    if (n > 0 && verbose)
	fprintf (stderr,"%d keywords deleted\n", n);

    /* Remove WCS distortion code from CTYPEi in FITS header */
    if (hgets (header, "CTYPE1", 31, str)) {
	lctype = strlen (str);
	if (lctype > 8) {
	    str[8] = (char) 0;
	    hputs (header, "CTYPE1", str);
	    }
	}
    if (hgets (header, "CTYPE2", 31, str)) {
	lctype = strlen (str);
	if (lctype > 8) {
	    str[8] = (char) 0;
	    hputs (header, "CTYPE2", str);
	    }
	}

    return (n);
}

void
foc2pix (wcs, x, y, u, v)

struct WorldCoor *wcs;  /* World coordinate system structure */
double	x, y;		/* Focal plane coordinates */
double	*u, *v;		/* Image pixel coordinates (returned) */
{
    int m, n, i, j, k;
    double s[DISTMAX], sum;
    double temp_x, temp_y;

    /* SIRTF distortion */
    if (wcs->distcode == DISTORT_SIRTF) {
	m = wcs->distort.ap_order;
	n = wcs->distort.bp_order;

	temp_x = x - wcs->xrefpix;
	temp_y = y - wcs->yrefpix;

	/* compute u */
	for (j = 0; j <= m; j++) {
	    s[j] = wcs->distort.ap[m-j][j];
	    for (k = j-1; k >= 0; k--) {
	   	s[j] = (temp_y * s[j]) + wcs->distort.ap[m-j][k];
		}
	    }
  
	sum = s[0];
	for (i=m; i>=1; i--){
	    sum = (temp_x * sum) + s[m-i+1];
	    }
	*u = sum;

	/* compute v*/
	for (j = 0; j <= n; j++) {
	    s[j] = wcs->distort.bp[n-j][j];
	    for (k = j-1; k >= 0; k--) {
		s[j] = temp_y*s[j] + wcs->distort.bp[n-j][k];
		}
	    }
   
	sum = s[0];
	for (i = n; i >= 1; i--)
	    sum = temp_x * sum + s[n-i+1];

	*v = sum;

	*u = x + *u;
	*v = y + *v;
	}

    /* If no distortion, return pixel positions unchanged */
    else {
	*u = x;
	*v = y;
	}

    return;
}


void
pix2foc (wcs, u, v, x, y)

struct WorldCoor *wcs;  /* World coordinate system structure */
double u, v;		/* Image pixel coordinates */
double *x, *y;		/* Focal plane coordinates (returned) */
{
    int m, n, i, j, k;
    double s[DISTMAX], sum;
    double temp_u, temp_v;

    /* SIRTF distortion */
    if (wcs->distcode == DISTORT_SIRTF) {
	m = wcs->distort.a_order;
	n = wcs->distort.b_order;

	temp_u = u - wcs->xrefpix;
	temp_v = v - wcs->yrefpix;

	/* compute u */
	for (j = 0; j <= m; j++) {
	    s[j] = wcs->distort.a[m-j][j];
	    for (k = j-1; k >= 0; k--) {
		s[j] = (temp_v * s[j]) + wcs->distort.a[m-j][k];
		}
	    }
  
	sum = s[0];
	for (i=m; i>=1; i--){
	    sum = temp_u*sum + s[m-i+1];
	    }
	*x = sum;

	/* compute v*/
	for (j=0; j<=n; j++) {
	    s[j] = wcs->distort.b[n-j][j];
	    for (k=j-1; k>=0; k--) {
		s[j] =temp_v*s[j] + wcs->distort.b[n-j][k];
		}
	    }
   
	sum = s[0];
	for (i=n; i>=1; i--)
	    sum = temp_u*sum + s[n-i+1];

	*y = sum;
  
	*x = u + *x;
	*y = v + *y;

/*	*x = u + *x + coeff.crpix1; */
/*	*y = v + *y + coeff.crpix2; */
	}

    /* If no distortion, return pixel positions unchanged */
    else {
	*x = u;
	*y = v;
	}

    return;
}


/* SETDISTCODE -- Set WCS distortion code from CTYPEi in FITS header */

void
setdistcode (wcs, ctype)

struct WorldCoor *wcs;  /* World coordinate system structure */
char *ctype;		/* Value of CTYPEi from FITS header */

{
    char *extension;
    int lctype;

    lctype = strlen (ctype);
    if (lctype < 9)
	wcs->distcode = DISTORT_NONE;
    else {
	extension = ctype + 8;
	if (!strncmp (extension, "-SIP", 4))
	    wcs->distcode = DISTORT_SIRTF;
	else
	    wcs->distcode = DISTORT_NONE;
	}
    return;
}


/* GETDISTCODE -- Return NULL if no distortion or code from wcs.h */

char *
getdistcode (wcs)

struct WorldCoor *wcs;  /* World coordinate system structure */

{
    char *dcode;	/* Distortion string for CTYPEi */

    if (wcs->distcode == DISTORT_SIRTF) {
	dcode = (char *) calloc (8, sizeof (char));
	strcpy (dcode, "-SIP");
	}
    else
	dcode = NULL;
    return (dcode);
}

/* Apr  2 2003	New subroutines
 * Nov  3 2003	Add getdistcode to return distortion code string
 * Nov 10 2003	Include unistd.h to get definition of NULL
 * Nov 18 2003	Include string.h to get strlen()
 *
 * Jan  9 2004	Add DelDistort() to delete distortion keywords
 *
 * Jan  4 2007	Declare header const char*
 */
