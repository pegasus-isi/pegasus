/*** File libwcs/imgetwcs.c
 *** March 24, 2009
 *** By Doug Mink, dmink@cfa.harvard.edu (remotely based on UIowa code)
 *** Harvard-Smithsonian Center for Astrophysics
 *** Copyright (C) 1996-2009
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

#include "wcs.h"
#include "lwcs.h"

/* Get the C* WCS fields in  a FITS header based on a reference catalog
 * do it by finding stars in the image and in the reference catalog and
 * finding the rotation and offsets which result in a best-fit.
 * verbose generates extra info on stderr.
 * try using deeper reference star catalog searches if there is trouble.
 * return 1 if all ok, else 0
 */

/* These parameters can be set on the command line */
static double secpix0 = PSCALE;		/* Set image scale--override header */
static double secpix2 = PSCALE;		/* Set image scale 2--override header */
static double *cd0 = NULL;		/* Set CD matrix--override header */
static double rot0 = 361.0;		/* Initial image rotation */
static int comsys = WCS_J2000;		/* Command line center coordinte system */
static int wp0 = 0;			/* Initial width of image */
static int hp0 = 0;			/* Initial height of image */
static double ra0 = -99.0;		/* Initial center RA in degrees */
static double dec0 = -99.0;		/* Initial center Dec in degrees */
static double xref0 = -99999.0;		/* Reference pixel X coordinate */
static double yref0 = -99999.0;		/* Reference pixel Y coordinate */
static int ptype0 = -1;			/* Projection type to fit */
static int  nctype = 28;		/* Number of possible projections */
static char ctypes[32][4];		/* 3-letter codes for projections */
static int usecdelt = 0;		/* Use CDELT if 1, else CD matrix */
static char *dateobs0 = NULL;		/* Initial DATE-OBS value in FITS date format */

struct WorldCoor *ChangeFITSWCS();

/* Set a nominal world coordinate system from image header info.
 * If the image center is not FK5 (J2000) equinox, convert it
 * Return a WCS structure if OK, else return NULL
 */

struct WorldCoor *
GetFITSWCS (filename, header, verbose, cra, cdec, dra, ddec, secpix, wp, hp,
	    sysout, eqout)

char	*filename;	/* FITS or IRAF file name */
char	*header;	/* Image FITS header */
int	verbose;	/* Extra printing if =1 */
double	*cra;		/* Center right ascension in degrees (returned) */
double	*cdec;		/* Center declination in degrees (returned) */
double	*dra;		/* Right ascension half-width in degrees (returned) */
double	*ddec;		/* Declination half-width in degrees (returned) */
double	*secpix;	/* Arcseconds per pixel (returned) */
int	*wp;		/* Image width in pixels (returned) */
int	*hp;		/* Image height in pixels (returned) */
int	*sysout;	/* Coordinate system to return (0=image, returned) */
double	*eqout;		/* Equinox to return (0=image, returned) */
{
    int naxes;
    double eq1, x, y;
    double ra1, dec1, dx, dy;
    double xmin, xmax, ymin, ymax, ra2, dec2, ra3, dec3, ra4, dec4;
    double dra0, dra1, dra2, dra3, dra4;
    struct WorldCoor *wcs;
    char rstr[64], dstr[64], cstr[16];

    /* Initialize WCS structure from possibly revised FITS header */
    wcs = ChangeFITSWCS (filename, header, verbose);
    if (wcs == NULL) {
	return (NULL);
	}
    *hp = (int) wcs->nypix;
    *wp = (int) wcs->nxpix;

    /* If incomplete WCS in header, drop out */
    if (nowcs (wcs)) {
	setwcsfile (filename);
	/* wcserr(); */
	if (verbose)
	    fprintf (stderr,"Insufficient information for initial WCS\n");
	return (NULL);
	}

    /* If in linear coordinates, do not print as sexigesimal */
    if (wcs->sysout < 1 || wcs->sysout == 6 || wcs->sysout == 10)
	wcs->degout = 1;

    /* Set flag to get appropriate equinox for catalog search */
    if (!*sysout)
	*sysout = wcs->syswcs;
    if (*eqout == 0.0)
	*eqout = wcs->equinox;
    eq1 = wcs->equinox;
    if (wcs->coorflip) {
	ra1 = wcs->crval[1];
	dec1 = wcs->crval[0];
	}
    else {
	ra1 = wcs->crval[0];
	dec1 = wcs->crval[1];
	}

    /* Print reference pixel position and value */
    if (verbose && (eq1 != *eqout || wcs->syswcs != *sysout)) {
	if (wcs->degout) {
	    deg2str (rstr, 32, ra1, 6);
	    deg2str (dstr, 32, dec1, 6);
	    }
	else {
	    ra2str (rstr, 32, ra1, 3);
	    dec2str (dstr, 32, dec1, 2);
	    }
	wcscstr (cstr, wcs->syswcs, wcs->equinox, wcs->epoch);
	fprintf (stderr,"Reference pixel (%.2f,%.2f) %s %s %s\n",
		 wcs->xrefpix, wcs->yrefpix, rstr, dstr, cstr);
	}

    /* Get coordinates of corners for size for catalog searching */
    dx = wcs->nxpix;
    dy = wcs->nypix;
    xmin = 0.5;
    ymin = 0.5;
    xmax = 0.5 + dx;
    ymax = 0.5 + dy;
    pix2wcs (wcs, xmin, ymin, &ra1, &dec1);
    pix2wcs (wcs, xmin, ymax, &ra2, &dec2);
    pix2wcs (wcs, xmax, ymin, &ra3, &dec3);
    pix2wcs (wcs, xmax, ymax, &ra4, &dec4);

    /* Convert search corners to output coordinate system and equinox */
    if (wcs->syswcs > 0 && wcs->syswcs != 6 && wcs->syswcs != 10) {
	wcscon (wcs->syswcs,*sysout,wcs->equinox,*eqout,&ra1,&dec1,wcs->epoch);
	wcscon (wcs->syswcs,*sysout,wcs->equinox,*eqout,&ra2,&dec2,wcs->epoch);
	wcscon (wcs->syswcs,*sysout,wcs->equinox,*eqout,&ra3,&dec3,wcs->epoch);
	wcscon (wcs->syswcs,*sysout,wcs->equinox,*eqout,&ra4,&dec4,wcs->epoch);
	}

    /* Find center and convert to output coordinate system and equinox */
    x = 0.5 + (dx * 0.5);
    y = 0.5 + (dy * 0.5);
    pix2wcs (wcs, x, y, cra, cdec);
    if (wcs->syswcs > 0 && wcs->syswcs != 6 && wcs->syswcs != 10)
	wcscon (wcs->syswcs,*sysout,wcs->equinox,*eqout,cra,cdec,wcs->epoch);

    /* Find maximum half-width in declination */
    *ddec = fabs (dec1 - *cdec);
    if (fabs (dec2 - *cdec) > *ddec)
	*ddec = fabs (dec2 - *cdec);
    if (fabs (dec3 - *cdec) > *ddec)
	*ddec = fabs (dec3 - *cdec);
    if (fabs (dec4 - *cdec) > *ddec)
	*ddec = fabs (dec4 - *cdec);

    /* Find maximum half-width in right ascension */
    dra0 = (dx / dy) * (*ddec / cos (*cdec));
    dra1 = ra1 - *cra;
    dra2 = ra2 - *cra;
    if (*cra < 0 && *cra + dra0 > 0.0) {
	dra1 = -(dra1 - 360.0);
	dra2 = -(dra2 - 360.0);
	}
    if (dra1 > 180.0)
	dra1 = dra1 - 360.0;
    else if (dra1 < -180.0)
	dra1 = dra1 + 360.0;
    else if (dra1 < 0.0)
	dra1 = -dra1;
    if (dra2 > 180.0)
	dra2 = dra2 - 360.0;
    else if (dra2 < -180.0)
	dra2 = dra2 + 360.0;
    else if (dra2 < 0.0)
	dra2 = -dra2;
    dra3 = *cra - ra3;
    dra4 = *cra - ra4;
    if (*cra > 0 && *cra - dra0 < 0.0) {
	dra3 = dra3 + 360.0;
	dra4 = dra4 + 360.0;
	}
    if (dra3 > 180.0)
	dra3 = dra3 - 360.0;
    else if (dra3 < -180.0)
	dra3 = dra3 + 360.0;
    else if (dra3 < 0.0)
	dra3 = -dra3;
    if (dra4 > 180.0)
	dra4 = dra4 - 360.0;
    else if (dra4 < -180.0)
	dra4 = dra4 + 360.0;
    else if (dra4 < 0.0)
	dra4 = -dra4;
    *dra = dra1;
    if (dra2 > *dra)
	*dra = dra2;
    if (dra3 > *dra)
	*dra = dra3;
    if (dra4 > *dra)
	*dra = dra4;

    /* wcssize (wcs, cra, cdec, dra, ddec); */

    /* Set reference pixel to center of image if it has not been set */
    if (wcs->xref == -999.0 && wcs->yref == -999.0) {
	wcs->xref = *cra;
	wcs->cel.ref[0] = *cra;
	wcs->crval[0] = *cra;
	wcs->yref = *cdec;
	wcs->cel.ref[1] = *cdec;
	wcs->crval[1] = *cdec;
	ra1 = *cra;
	dec1 = *cdec;
	if (wcs->xrefpix == 0.0 && wcs->yrefpix == 0.0) {
	    wcs->xrefpix = 0.5 + (double) wcs->nxpix * 0.5;
	    wcs->yrefpix = 0.5 + (double) wcs->nypix * 0.5;
	    }
	wcs->xinc = *dra * 2.0 / (double) wcs->nxpix;
	wcs->yinc = *ddec * 2.0 / (double) wcs->nypix;
	/* hchange (header,"PLTRAH","PLT0RAH");
	wcs->plate_fit = 0; */
	}

    /* Convert center to desired coordinate system */
    else if (wcs->syswcs != *sysout && wcs->equinox != *eqout) {
	wcscon (wcs->syswcs, *sysout, wcs->equinox, *eqout, &ra1, &dec1, wcs->epoch);
	if (wcs->coorflip) {
	    wcs->yref = ra1;
	    wcs->xref = dec1;
	    }
	else {
	    wcs->xref = ra1;
	    wcs->yref = dec1;
	    }
	}

    /* Compute plate scale to return if it was not set on the command line */
    if (secpix0 <= 0.0) {
	pix2wcs (wcs, wcs->xrefpix-0.5, wcs->yrefpix, &ra1, &dec1);
	pix2wcs (wcs, wcs->xrefpix+0.5, wcs->yrefpix, &ra2, &dec2);
	*secpix = 3600.0 * wcsdist (ra1, dec1, ra2, dec2);
	}

    wcs->crval[0] = wcs->xref;
    wcs->crval[1] = wcs->yref;
    if (wcs->coorflip) {
	wcs->cel.ref[0] = wcs->crval[1];
	wcs->cel.ref[1] = wcs->crval[0];
	}
    else {
	wcs->cel.ref[0] = wcs->crval[0];
	wcs->cel.ref[1] = wcs->crval[1];
	}

    if (wcs->syswcs > 0 && wcs->syswcs != 6 && wcs->syswcs != 10) {
	wcs->cel.flag = 0;
	wcs->wcsl.flag = 0;
	}
    else {
	wcs->lin.flag = LINSET;
	wcs->wcsl.flag = WCSSET;
	}

    wcs->equinox = *eqout;
    wcs->syswcs = *sysout;
    wcs->sysout = *sysout;
    wcs->eqout = *eqout;
    wcs->sysin = *sysout;
    wcs->eqin = *eqout;
    wcscstr (cstr,*sysout,*eqout,wcs->epoch);
    strcpy (wcs->radecsys, cstr);
    strcpy (wcs->radecout, cstr);
    strcpy (wcs->radecin, cstr);
    wcsininit (wcs, wcs->radecsys);
    wcsoutinit (wcs, wcs->radecsys);

    naxes = wcs->naxis;
    if (naxes < 1 || naxes > 9) {
	naxes = wcs->naxes;
	wcs->naxis = naxes;
	}

    if (usecdelt) {
	hputnr8 (header, "CDELT1", 9, wcs->xinc);
	if (naxes > 1) {
	    hputnr8 (header, "CDELT2", 9, wcs->yinc);
	    hputnr8 (header, "CROTA2", 9, wcs->rot);
	    }
	hdel (header, "CD1_1");
	hdel (header, "CD1_2");
	hdel (header, "CD2_1");
	hdel (header, "CD2_2");
	}
    else {
	hputnr8 (header, "CD1_1", 9, wcs->cd[0]);
	if (naxes > 1) {
	    hputnr8 (header, "CD1_2", 9, wcs->cd[1]);
	    hputnr8 (header, "CD2_1", 9, wcs->cd[2]);
	    hputnr8 (header, "CD2_2", 9, wcs->cd[3]);
	    }
	}

    /* Print reference pixel position and value */
    if (verbose) {
	if (wcs->degout) {
	    deg2str (rstr, 32, ra1, 6);
            deg2str (dstr, 32, dec1, 6);
	    }
	else {
	    ra2str (rstr, 32, ra1, 3);
            dec2str (dstr, 32, dec1, 2);
	    }
	wcscstr (cstr,*sysout,*eqout,wcs->epoch);
	fprintf (stderr,"Reference pixel (%.2f,%.2f) %s %s %s\n",
		wcs->xrefpix, wcs->yrefpix, rstr, dstr, cstr);
	}

    /* Image size for catalog search */
    if (verbose) {
	if (wcs->degout) {
	    deg2str (rstr, 32, *cra, 6);
            deg2str (dstr, 32, *cdec, 6);
	    }
	else {
	    ra2str (rstr, 32, *cra, 3);
	    dec2str (dstr, 32, *cdec, 2);
	    }
	wcscstr (cstr, *sysout, *eqout, wcs->epoch);
	fprintf (stderr,"Search at %s %s %s", rstr, dstr, cstr);
	if (wcs->degout) {
	    deg2str (rstr, 32, *dra, 6);
            deg2str (dstr, 32, *ddec, 6);
	    }
	else {
	    ra2str (rstr, 32, *dra, 3);
	    dec2str (dstr, 32, *ddec, 2);
	    }
	fprintf (stderr," +- %s %s\n", rstr, dstr);
	fprintf (stderr,"Image width=%d height=%d, %g arcsec/pixel\n",
				*wp, *hp, *secpix);
	}

    return (wcs);
}


/* ChangeFITSWCS: Modify FITS WCS header from command line values */

struct WorldCoor *
ChangeFITSWCS (filename, header, verbose)

char	*filename;	/* FITS or IRAF file name */
char	*header;	/* Image FITS header */
int	verbose;	/* Extra printing if =1 */
{
    int nax, i, hp, wp;
    double xref, yref, degpix, secpix;
    struct WorldCoor *wcs;
    char temp[16];
    char *cwcs;

    /* Set the world coordinate system from the image header */
    if (strlen (filename) > 0) {
	cwcs = strchr (filename, '%');
	if (cwcs != NULL)
	    cwcs++;
	}
    if (!strncmp (header, "END", 3)) {
	cwcs = NULL;
	for (i = 0; i < 2880; i++)
	    header[i] = (char) 32;
	hputl (header, "SIMPLE", 1);
	hputi4 (header, "BITPIX", 0);
	hputi4 (header, "NAXIS", 2);
	hputi4 (header, "NAXIS1", 1);
	hputi4 (header, "NAXIS2", 1);
	}

    /* Set image dimensions */
    nax = 0;
    if (hp0 > 0 || wp0 > 0) {
	hp = hp0;
	wp = wp0;
	if (hp > 0 && wp > 0)
	    nax = 2;
	else
	    nax = 1;
	hputi4 (header, "NAXIS", nax);
	hputi4 (header, "NAXIS1", wp);
	hputi4 (header, "NAXIS2", hp);
	}
    else if (hgeti4 (header,"NAXIS",&nax) < 1 || nax < 1) {
	if (hgeti4 (header, "WCSAXES", &nax) < 1)
	    return (NULL);
	else {
	    if (hgeti4 (header, "IMAGEW", &wp) < 1)
		return (NULL);
	    if (hgeti4 (header, "IMAGEH", &wp) < 1)
		return (NULL);
	    }
	}
    else {
	if (hgeti4 (header,"NAXIS1",&wp) < 1)
	    return (NULL);
	if (hgeti4 (header,"NAXIS2",&hp) < 1)
	    return (NULL);
	}

    /* Set plate center from command line, if it is there */
    if (ra0 > -99.0 && dec0 > -99.0) {
	hputnr8 (header, "CRVAL1" ,8,ra0);
	hputnr8 (header, "CRVAL2" ,8,dec0);
	hputra (header, "RA", ra0);
	hputdec (header, "DEC", dec0);
	if (comsys == WCS_B1950) {
	    hputi4 (header, "EPOCH", 1950);
	    hputi4 (header, "EQUINOX", 1950);
	    hputs (header, "RADECSYS", "FK4");
	    }
	else {
	    hputi4 (header, "EPOCH", 2000);
	    hputi4 (header, "EQUINOX", 2000);
	    if (comsys == WCS_GALACTIC)
		hputs (header, "RADECSYS", "GALACTIC");
	    else if (comsys == WCS_ECLIPTIC)
		hputs (header, "RADECSYS", "ECLIPTIC");
	    else if (comsys == WCS_ICRS)
		hputs (header, "RADECSYS", "ICRS");
	    else
		hputs (header, "RADECSYS", "FK5");
	    }
	if (hgetr8 (header, "SECPIX", &secpix)) {
	    degpix = secpix / 3600.0;
	    hputnr8 (header, "CDELT1", 8, -degpix);
	    hputnr8 (header, "CDELT2", 8, degpix);
	    hdel (header, "CD1_1");
	    hdel (header, "CD1_2");
	    hdel (header, "CD2_1");
	    hdel (header, "CD2_2");
	    } 
	}
    if (ptype0 > -1 && ptype0 < nctype) {
	strcpy (temp,"RA---");
	strcat (temp, ctypes[ptype0]);
	hputs (header, "CTYPE1", temp);
	strcpy (temp,"DEC--");
	strcat (temp, ctypes[ptype0]);
	hputs (header, "CTYPE2", temp);
	}

    /* Set reference pixel from command line, if it is there */
    if (xref0 > -99999.0 && yref0 > -99999.0) {
	hputr8 (header, "CRPIX1", xref0);
	hputr8 (header, "CRPIX2", yref0);
	}
    else if (hgetr8 (header, "CRPIX1", &xref) < 1) {
	xref = 0.5 + (double) wp / 2.0;
	yref = 0.5 + (double) hp / 2.0;
	hputnr8 (header, "CRPIX1", 3, xref);
	hputnr8 (header, "CRPIX2", 3, yref);
	}

    /* Set plate scale from command line, if it is there */
    if (secpix0 != 0.0 || cd0 != NULL) {
	if (secpix2 != 0.0) {
	    secpix = 0.5 * (secpix0 + secpix2);
	    hputnr8 (header, "SECPIX1", 5, secpix0);
	    hputnr8 (header, "SECPIX2", 5, secpix2);
	    degpix = -secpix0 / 3600.0;
	    hputnr8 (header, "CDELT1", 8, degpix);
	    degpix = secpix2 / 3600.0;
	    hputnr8 (header, "CDELT2", 8, degpix);
	    hdel (header, "CD1_1");
	    hdel (header, "CD1_2");
	    hdel (header, "CD2_1");
	    hdel (header, "CD2_2");
	    }
	else if (secpix0 != 0.0) {
	    secpix = secpix0;
	    hputnr8 (header, "SECPIX", 5, secpix);
	    degpix = secpix / 3600.0;
	    hputnr8 (header, "CDELT1", 8, -degpix);
	    hputnr8 (header, "CDELT2", 8, degpix);
	    hdel (header, "CD1_1");
	    hdel (header, "CD1_2");
	    hdel (header, "CD2_1");
	    hdel (header, "CD2_2");
	    }
	else {
	    hputr8 (header, "CD1_1", cd0[0]);
	    hputr8 (header, "CD1_2", cd0[1]);
	    hputr8 (header, "CD2_1", cd0[2]);
	    hputr8 (header, "CD2_2", cd0[3]);
	    hdel (header, "CDELT1");
	    hdel (header, "CDELT2");
	    hdel (header, "CROTA1");
	    hdel (header, "CROTA2");
	    }
	if (!ksearch (header,"CRVAL1")) {
	    hgetra (header, "RA", &ra0);
	    hgetdec (header, "DEC", &dec0);
	    hputnr8 (header, "CRVAL1", 8, ra0);
	    hputnr8 (header, "CRVAL2", 8, dec0);
	    }
	if (!ksearch (header,"CRPIX1")) {
	    xref = (double) wp / 2.0;
	    yref = (double) hp / 2.0;
	    hputnr8 (header, "CRPIX1", 3, xref);
	    hputnr8 (header, "CRPIX2", 3, yref);
	    }
	if (!ksearch (header,"CTYPE1")) {
	    if (comsys == WCS_GALACTIC) {
		hputs (header, "CTYPE1", "GLON-TAN");
		hputs (header, "CTYPE2", "GLAT-TAN");
		}
	    else {
		hputs (header, "CTYPE1", "RA---TAN");
		hputs (header, "CTYPE2", "DEC--TAN");
		}
	    }
	}

    /* Set rotation angle from command line, if it is there */
    if (rot0 < 361.0) {
	hputnr8 (header, "CROTA1", 5, rot0);
	hputnr8 (header, "CROTA2", 5, rot0);
	}

    /* Set observation date for epoch, if it is there */
    if (dateobs0 != NULL)
	hputs (header, "DATE-OBS", dateobs0);

    /* Initialize WCS structure from FITS header */
    wcs = wcsinitn (header, cwcs);

    /* If incomplete WCS in header, drop out */
    if (nowcs (wcs)) {
	setwcsfile (filename);
	/* wcserr(); */
	if (verbose)
	    fprintf (stderr,"Insufficient information for initial WCS\n");
	return (NULL);
	}
    return (wcs);
}

void
setcdelt()			/* Set flag to use CDELTn, not CD matrix */
{usecdelt = 1; return;}

void
setnpix (nx, ny)		/* Set image size */
int nx, ny;
{ wp0 = nx; hp0 = ny; return; }

void
setrot (rot)
double rot;
{ rot0 = rot; return; }

void
setsecpix (secpix)		/* Set first axis arcseconds per pixel */
double secpix;
{ secpix0 = secpix; return; }

double
getsecpix ()		/* Return first axis arcseconds per pixel */
{ return (secpix0); }

void
setsecpix2 (secpix)		/* Set second axis arcseconds per pixel */
double secpix;
{ secpix2 = secpix; return; }

void
setcd (cd)		/* Set initial CD matrix */
double *cd;
{ int i;
  if (cd0 != NULL) free (cd0);
  cd0 = (double *) calloc (4, sizeof (double));
  for (i = 0; i < 4; i++) cd0[i] = cd[i];
  return; }

void
setsys (comsys0)		/* Set WCS coordinates as FK4 */
int comsys0;
{ comsys = comsys0; return; }

void
setcenter (rastr, decstr)	/* Set center sky coordinates in strings */
char *rastr, *decstr;
{
    ra0 = str2ra (rastr);
    dec0 = str2dec (decstr);
    return;
}

void
setdcenter (ra, dec)		/* Set center sky coordinates in degrees */
double ra, dec;
{ ra0 = ra; dec0 = dec; return; }

void
getcenter (ra, dec)		/* Return initial reference sky coordinates */
double *ra, *dec;
{ *ra = ra0; *dec = dec0; return; }

void
setrefpix (x, y)		/* Set reference pixel image coordinates */
double x, y;
{ xref0 = x; yref0 = y; return; }

void
getrefpix (x, y)		/* Return initial ref pixel image coordinates */
double *x, *y;
{ *x = xref0; *y = yref0; return; }

void
setproj (ptype)
char*	ptype;
{
    int i;

    /* Set up array of projection types */
    strcpy (ctypes[0], "LIN");
    strcpy (ctypes[1], "AZP");
    strcpy (ctypes[2], "SZP");
    strcpy (ctypes[3], "TAN");
    strcpy (ctypes[4], "SIN");
    strcpy (ctypes[5], "STG");
    strcpy (ctypes[6], "ARC");
    strcpy (ctypes[7], "ZPN");
    strcpy (ctypes[8], "ZEA");
    strcpy (ctypes[9], "AIR");
    strcpy (ctypes[10], "CYP");
    strcpy (ctypes[11], "CAR");
    strcpy (ctypes[12], "MER");
    strcpy (ctypes[13], "CEA");
    strcpy (ctypes[14], "COP");
    strcpy (ctypes[15], "COD");
    strcpy (ctypes[16], "COE");
    strcpy (ctypes[17], "COO");
    strcpy (ctypes[18], "BON");
    strcpy (ctypes[19], "PCO");
    strcpy (ctypes[20], "SFL");
    strcpy (ctypes[21], "PAR");
    strcpy (ctypes[22], "AIT");
    strcpy (ctypes[23], "MOL");
    strcpy (ctypes[24], "CSC");
    strcpy (ctypes[25], "QSC");
    strcpy (ctypes[26], "TSC");
    strcpy (ctypes[27], "NCP");
    strcpy (ctypes[28], "GLS");
    strcpy (ctypes[29], "DSS");
    strcpy (ctypes[30], "PLT");
    strcpy (ctypes[31], "TNX");

    ptype0 = -1;
    for (i = 0; i < nctype; i++) {
	if (!strcasecmp (ptype, ctypes[i]))
	    ptype0 = i;
	}
    return;
}

void
setdateobs (dateobs)
char *dateobs;
{ dateobs0 = calloc (strlen (dateobs), sizeof (char));
  strcpy (dateobs0, dateobs);
  return; }


/* Feb 29 1996	New program
 * May 23 1996	Use pre-existing WCS for center, if it is present
 * May 29 1996	Simplify program by always using WCS structure
 * Jun 12 1996	Be more careful with nominal WCS setting
 * Jul  3 1996	Set epoch from old equinox if not already set
 * Jul 19 1996	Set image center in WCS if DSS WCS
 * Aug  5 1996	Check for SECPIX1 as well as SECPIX
 * Aug  7 1996	Save specified number of decimal places in header parameters
 * Aug  7 1996	Rename old center parameters
 * Aug 26 1996	Decrease default pixel tolerance from 20 to 10
 * Sep  1 1996	Set plate scale default in lwcs.h
 * Sep  3 1996	Fix bug to set plate scale from command line
 * Oct 15 1996	Break off from imsetwcs.c
 * Oct 16 1996	Clean up center setting so eqref is used
 * Oct 17 1996	Do not print error messages unless verbose is set
 * Oct 30 1996	Keep equinox from image if EQREF is zero
 * Nov  1 1996	Declare undeclared subroutines; remove unused variables
 * Nov  4 1996	Add reference pixel and projection to wcsset() call
 * Nov 14 1996	Add GetLimits() to deal with search limits around the poles
 * Nov 15 1996	Drop GetLimits(); code moved to individual catalog routines
 * Dec 10 1996	Fix precession and make equinox double

 * Feb 19 1997	If eqout is 0, use equinox of image
 * Feb 24 1997	Always convert center to output equinox (bug fix)
 * Mar 20 1997	Declare EQ2 double instead of int, fixing a bug
 * Jul 11 1997	Allow external (command line) setting of reference pixel coords
 * Sep 26 1997	Set both equinox and epoch if input center coordinates
 * Nov  3 1997	Separate WCS reference pixel from search center
 * Dec  8 1997	Set CDELTn using SECPIX if it is in the header
 *
 * Jan  6 1998	Do not print anything unless verbose is set
 * Jan 29 1998	Use flag to allow AIPS classic WCS subroutines
 * Mar  1 1998	Set x and y plate scales from command line if there are two
 * Mar  2 1998	Do not reset plate solution switch
 * Mar  6 1998	Add option to reset center sky coordinates in degrees
 * Mar  6 1998	Add option to set projection type
 * Mar 18 1998	Initialize reference pixel if CDELTn's being set
 * Mar 20 1998	Only set CTYPEn if CDELTn or CRVALn are set
 * Apr 10 1998	Add option to set search area to circle or box
 * Apr 20 1998	Move GetArea() to scat.c
 * Apr 24 1998	Always convert image reference coordinate to catalog equinox
 * Apr 28 1998	Change coordinate system flags to WCS_*
 * Jun  1 1998	Print error message if WCS cannot be initialized
 * Jun 24 1998	Add string lengths to ra2str() and dec2str() calls
 * Jun 25 1998	Leave use of AIPS wcs to wcs.c file
 * Sep 17 1998	Add sysout to argument list and use scscon() for conversion
 * Sep 25 1998	Make sysout==0 indicate output in image coordinate system
 * Oct 28 1998	Set coordinate system properly to sysout/eqout in GetFITSWCS()
 *
 * Apr  7 1999	Add filename argument to GetFITSWCS
 * Apr 29 1999	Add option to set image size
 * Jun  2 1999	Fix sign of CDELT1 if secpix2 and secpix0 are set
 * Jul  7 1999	Fix conversion of center coordinates to refsys
 * Jul  9 1999	Fix bug which reset command-line-set reference pixel coordinate
 * Oct 21 1999	Fix declarations after lint
 * Nov  1 1999	Add option to write CD matrix
 * Nov  1 1999	If CDELTn set from command line delete previous header CD matrix
 * Nov 12 1999	Add galactic coordinates as command line option
 * Nov 16 1999	Set radecsys correctly for command line galactic
 *
 * Feb 15 2000	Add option to override the header CD matrix (like CDELTs)
 * Feb 29 2000	Fix bug, converting reference pixel WCS coordinates everywhere
 * Mar 27 2000	Drop unused subroutine setradius()
 * May 24 2000	Print degrees in debugging messages if output format
 * Sep 14 2000	Set xinc and yinc correctly if center pixel in header
 *
 * Jan 11 2001	All printing to stderr
 * Sep 19 2001	Drop fitshead.h; it is in wcs.h
 * Oct 19 2001	Allow DATE-OBS to be set
 *
 * Apr  3 2002	Update projection types to match list in wcs.h and wcs.c
 * Aug  2 2002	Add getsecpix(), getrefpix(), getcneter() to return presets
 *
 * Mar 25 2003	Fix GetFITSWCS() to return correct search area for rotated image
 * Mar 25 2003	Write out CTYPEn with quotes
 * Mar 27 2003	Fix half-pixel bug in computation of image center
 * Apr  3 2003	Drop unused variables after lint
 * Jun  6 2003	Set xref and yref to center if -999, not 0
 * Jul 21 2003	Fix bug setting secpix if it was not set on the command line
 *		(found by Takehiko Wada, ISAS)
 * Sep 23 2003	In setproj(), use strcasecmp() instead of strcmp()
 * Sep 26 2003	If reference pixel not set, set center correctly
 * Oct  6 2003	Change wcs->naxes to wcs->naxis to match WCSLIB 3.2
 * Dec  3 2003	Add wcs->naxes back as an alternative
 *
 * Jul 26 2004	Fix image size when wrapping around RA=0:00
 * Sep 16 2004	Fix verbose mode search size in GetFITSWCS()
 * Oct 29 2004	Fix problem setting RA size from limits
 *
 * Jan 20 2005	Fix cel.ref assignment if axis are switched
 * Jul 20 2005	Fix bug which reversed dimensions when setting image size
 * Jul 21 2005	Fix bug which caused bad results at RA ~= 0.0
 * Aug 30 2005	Implement multiple WCS's, though not modification thereof, yet
 * Nov  1 2005	Set RADECSYS to ICRS if appropriate
 *
 * Oct 30 2006	Do not precess LINEAR or XY coordinates
 *
 * Jun  1 2007	In GetFITSWCS, deal with no input file
 * Jun  5 2007	Add ChangeFITSWCS to set header WCS arguments and WCS
 * Jul  3 2007	Fix bug by setting hp and wp
 * Jul 26 2007	If first line of header is END, initialize other needed values
 * Oct 19 2007	Return NULL from GetFITSWCS() immediately if no WCS in header
 *
 * Mar 24 2009	Set dimensions from IMAGEW and IMAGEH if WCSAXES > 0
 */
