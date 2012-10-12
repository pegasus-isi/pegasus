/*** File libwcs/fortcat.c
 *** April 3, 2003
 *** By Doug Mink, dmink@cfa.harvard.edu
 *** Harvard-Smithsonian Center for Astrophysics
 *** Copyright (C) 2001-2003
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

/* Fortran wrappers for subroutines which read astronomical catalogs
 *
 * Catalogs supported include: USNO-A2.0, USNO_SA2.0, ACT, Tycho 2, SAO
 * TDC binary format (SAO, PPM, Yale Bright Star, IRAS Point Source Catalogs)
 * SAO TDC ASCII format, and Starbase tab-delimited format
 *
 * For shell-level searches, use WCSTools scat, documented at
 * http://tdc-www.harvard.edu/software/wcstools/scat/
 *
 * int catread_()	Read catalog stars in specified region of the sky
 * int catrnum_()	Read catalog stars with specified numbers
 */

#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <math.h>
#include <sys/types.h>
#include "wcs.h"
#include "wcscat.h"

/* default pathname for catalog, used if catalog file not found in current
   working directory, but overridden by WCS_CATDIR environment variable */
char catdir[64]="/data/catalogs";

static struct StarCat **starcat; /* Catalog data structure */

/* CATREAD -- Read ASCII stars in specified region using ctgread() */

void
catread_ (catfile, distsort, cra, cdec, dra, ddec, drad,
	  csysout, eqout, epout, mag1, mag2, nsmax, nlog, nstars,
	  xnum, xra, xdec, xpra, xpdec, xmag, xmagb)

char	*catfile;	/* Name of reference star catalog file */
int	distsort;	/* 1 to sort stars by distance from center */
double	cra;		/* Search center J2000 right ascension in degrees */
double	cdec;		/* Search center J2000 declination in degrees */
double	dra;		/* Search half width in right ascension in degrees */
double	ddec;		/* Search half-width in declination in degrees */
double	drad;		/* Limiting separation in degrees (ignore if 0) */
char	*csysout;	/* Search coordinate system */
double	eqout;		/* Search coordinate equinox */
double	epout;		/* Proper motion epoch (0.0 for no proper motion) */
double	mag1,mag2;	/* Limiting magnitudes (none if equal) */
int     nsmax;		/* Maximum number of stars to be returned */
int     nlog;		/* Logging frequency */
int	*nstars;		/* Number of catalog stars found (returned) */
double	*xnum;		/* Array of ID numbers (returned) */
double	*xra;		/* Array of right ascensions (returned) */
double	*xdec;		/* Array of declinations (returned) */
double	*xpra;		/* Array of right ascension proper motions (returned) */
double	*xpdec;		/* Array of declination proper motions (returned) */
double	*xmag;		/* Array of magnitudes (returned) */
double	*xmagb;		/* Array of second magnitudes (returned) */

{
int     refcat;         /* Catalog code from wcscat.h */
char    **tobj;         /* Array of object names (ignored) */
int     *tc;            /* Array of fluxes (ignored) */
int	nread;		/* Number of stars read from catalog */
int     sysout;         /* Search coordinate system */

    tc = NULL;
    tobj = NULL;

    refcat = CatCode (catfile);
    sysout = wcscsys (csysout);

    nread = ctgread (catfile, refcat, distsort, cra, cdec, dra, ddec, drad,
         sysout, eqout, epout, mag1, mag2, nsmax, starcat,
         xnum, xra, xdec, xpra, xpdec, xmag, xmagb, tc, tobj, nlog);

    /* Return number of stars read or maximum, which ever is lower */
    if (nread < nsmax)
	*nstars = nread;
    else
	*nstars = nsmax;

    return;
}


/* CATRNUM -- Read ASCII stars with specified numbers using ctgrnum() */

void
catrnum_ (catfile, nnum, csysout, eqout, epout, match, nlog, nstars,
          xnum, xra, xdec, xpra, xpdec, xmag, xmagb)

char    *catfile;       /* Name of reference star catalog file */
int     nnum;           /* Number of stars to look for */
char	*csysout;	/* Search coordinate system */
double  eqout;          /* Search coordinate equinox */
double  epout;          /* Proper motion epoch (0.0 for no proper motion) */
int     match;          /* 1 to match star number exactly, else sequence num.*/
int     nlog;		/* Logging frequency */
int	*nstars;	/* Number of catalog stars found (returned) */
double  *xnum;          /* Array of star numbers to look for */
double  *xra;           /* Array of right ascensions (returned) */
double  *xdec;          /* Array of declinations (returned) */
double  *xpra;          /* Array of right ascension proper motions (returned) */
double  *xpdec;         /* Array of declination proper motions (returned) */
double  *xmag;          /* Array of magnitudes (returned) */
double  *xmagb;         /* Array of second magnitudes (returned) */

{
int     refcat;         /* Catalog code from wcscat.h */
int     *tc;            /* Array of fluxes (ignore) */
char    **tobj;         /* Array of object names (ignored) */
int     sysout;         /* Search coordinate system */
int	nread;		/* Number of stars read from catalog */

    tc = NULL;
    tobj = NULL;

    sysout = wcscsys (csysout);

    refcat = CatCode (catfile);

    nread = ctgrnum (catfile,refcat, nnum,sysout,eqout,epout,match,starcat,
		     xnum,xra,xdec,xpra,xpdec,xmag,xmagb,tc,tobj,nlog);
    *nstars = nread;

    return;
}
/*
 * Feb 16 2001	New subroutines
 * Sep 19 2001	Drop fitshead.h; it is in wcs.h
 *
 * Apr  3 2003	Use CatCode() instead of RefCat(); add nstars to catrnum_()
 */
