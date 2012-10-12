/*** File libwcs/ujcread.c
 *** July 5, 2007
 *** By Doug Mink, dmink@cfa.harvard.edu
 *** Harvard-Smithsonian Center for Astrophysics
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

#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <math.h>
#include "wcs.h"
#include "fitsfile.h"
#include "wcscat.h"

static char cdu[64]="/data/ujcat/catalog"; /* pathname of UJ 1.0 CDROM */

typedef struct {
    int rasec, decsec, magetc;
} UJCstar;

static int nstars;	/* Number of stars in catalog */
static int cswap = 0;	/* Byte reverse catalog to Intel/DEC order if 1 */
static FILE *fcat;
static int refcat;	/* Code for catalog */
static char *catname;

#define ABS(a) ((a) < 0 ? (-(a)) : (a))
#define NZONES 24

static double ujcra();
static double ujcdec();
static double ujcmag();
static int ujcplate();
static int ujczones();
static int ujczone();
static int ujcsra();
static int ujcopen();
static int ujcpath();
static int ujcstar();
static void ujcswap();


/* UJCREAD -- Read USNO J Catalog stars from CDROM or plate catalog from file */

int
ujcread (refcatname,cra,cdec,dra,ddec,drad,dradi,distsort,sysout,eqout,epout,
	 mag1,mag2,nstarmax,unum,ura,udec,umag,uplate,verbose)

char    *refcatname;    /* Name of catalog (UJC, xxxxx.usno) */
double	cra;		/* Search center J2000 right ascension in degrees */
double	cdec;		/* Search center J2000 declination in degrees */
double	dra;		/* Search half width in right ascension in degrees */
double	ddec;		/* Search half-width in declination in degrees */
double	drad;		/* Limiting separation in degrees (ignore if 0) */
double	dradi;		/* Inner edge of annulus in degrees (ignore if 0) */
int	distsort;	/* 1 to sort stars by distance from center */
int	sysout;		/* Search coordinate system */
double	eqout;		/* Search coordinate equinox */
double	epout;		/* Proper motion epoch (0.0 for no proper motion) */
double	mag1,mag2;	/* Limiting magnitudes (none if equal) */
int	nstarmax;	/* Maximum number of stars to be returned */
double	*unum;		/* Array of UJ numbers (returned) */
double	*ura;		/* Array of right ascensions (returned) */
double	*udec;		/* Array of declinations (returned) */
double	**umag;		/* Array of magnitudes (returned) */
int	*uplate;	/* Array of plate numbers (returned) */
int	verbose;	/* 1 for diagnostics */
{
    double ra1,ra2;	/* Limiting right ascensions of region in degrees */
    double dec1,dec2;	/* Limiting declinations of region in degrees */
    double dist = 0.0;  /* Distance from search center in degrees */
    double faintmag=0.0; /* Faintest magnitude */
    double maxdist=0.0; /* Largest distance */
    int	faintstar=0;	/* Faintest star */
    int	farstar=0;	/* Most distant star */
    int magsort=0;
    double *udist;	/* Array of distances to stars */
    int nz;		/* Number of input UJ zone files */
    int zlist[NZONES];	/* List of input UJ zones */
    UJCstar star;	/* UJ catalog entry for one star */
    int sysref=WCS_J2000;	/* Catalog coordinate system */
    double eqref=2000.0;	/* Catalog equinox */
    double epref=2000.0;	/* Catalog epoch */
    char cstr[32];
    double num;		/* UJ number */
    int xplate;		/* If nonzero, use objects only from this plate */

    double rra1, rra2, rdec1, rdec2;
    int wrap, iwrap;
    int znum, itot,iz;
    int nlog,jstar, mprop, nmag;
    int itable = 0;
    int nstar, i;
    int pass;
    double ra,dec;
    double mag;
    double rdist, ddist;
    int istar, istar1, istar2, plate;
    int nzmax = NZONES;	/* Maximum number of declination zones */
    char *str;
    char title[128];

    itot = 0;
    xplate = getuplate ();

    /* Set catalog code and path to catalog */
    catname = refcatname;
    refcat = RefCat (refcatname,title,&sysref,&eqref,&epref,&mprop,&nmag);
    if (refcat == UJC && (str = getenv("UJ_PATH")) != NULL ) {

	/* If pathname is a URL, search and return */
	if (!strncmp (str, "http:",5)) {
	    return (webread (str,"ujc",distsort,cra,cdec,dra,ddec,drad,dradi,
			     sysout,eqout,epout,mag1,mag2,magsort,nstarmax,
			     unum,ura,udec,NULL,NULL,umag,uplate,verbose));
	    }
	else
	    strcpy (cdu,str);
	}

    wcscstr (cstr, sysout, eqout, epout);

    SearchLim (cra,cdec,dra,ddec,sysout,&ra1,&ra2,&dec1,&dec2,verbose);

    /* mag1 is always the smallest magnitude */
    if (mag2 < mag1) {
	mag = mag2;
	mag2 = mag1;
	mag1 = mag;
	}

    rra1 = ra1;
    rra2 = ra2;
    rdec1 = dec1;
    rdec2 = dec2;
    RefLim (cra,cdec,dra,ddec,sysout,sysref,eqout,eqref,epout,epref,0.0,
	    &rra1, &rra2, &rdec1, &rdec2, &wrap, verbose);

    /* Find UJ Star Catalog regions in which to search */
    if (refcat == UJC) {
	nz = ujczones (rra1, rra2, rdec1, rdec2, nzmax, zlist, verbose);
	if (nz <= 0) {
	    fprintf (stderr,"UJCREAD:  no UJ zones found\n");
	    return (0);
	    }
	}
    else
	nz = 1;

    udist = (double *) calloc (nstarmax, sizeof (double));

    /* Logging interval */
    if (verbose)
	nlog = 100;
    else
	nlog = 0;
    nstar = 0;

    /* Loop through region list */
    for (iz = 0; iz < nz; iz++) {

	/* Get path to zone catalog */
	znum = zlist[iz];
	if ((nstars = ujcopen (znum)) != 0) {

	    jstar = 0;
	    itable = 0;
	    for (iwrap = 0; iwrap <= wrap; iwrap++) {

		/* Find first star based on RA */
		if (iwrap == 0 || wrap == 0)
		    istar1 = ujcsra (rra1);
		else
		    istar1 = 1;

		/* Find last star based on RA */
		if (iwrap == 1 || wrap == 0)
		    istar2 = ujcsra (rra2);
		else
		    istar2 = nstars;

		if (istar1 == 0 || istar2 == 0)
		    break;

		/* Loop through zone catalog for this region */
		for (istar = istar1; istar <= istar2; istar++) {
		    itable ++;

		    if (ujcstar (istar, &star)) {
			fprintf (stderr,"UJCREAD: Cannot read star %d\n", istar);
			break;
			}

		    /* Extract selected fields if not probable duplicate */
		    else if (star.magetc > 0) {
			mag = ujcmag (star.magetc);	/* Magnitude */

			/* Check magnitude limits */
			pass = 1;
			if (mag1 != mag2 && (mag < mag1 || mag > mag2))
			    pass = 0;

			/* Check plate number */
			plate = ujcplate (star.magetc);	/* Plate number */
			if (xplate != 0 && plate != xplate)
			    pass = 0;

			/* Check position limits */
			if (pass) {
			    ra = ujcra (star.rasec);	/* RA in degrees */
			    dec = ujcdec (star.decsec);	/* Dec in degrees */

			    /* Get position in output coordinate system */
			    wcscon (sysref,sysout,eqref,eqout,&ra,&dec,epout);

			    /* Compute distance from search center */
			    if (drad > 0 || distsort)
				dist = wcsdist (cra,cdec,ra,dec);
			    else
				dist = 0.0;

			    /* Check radial distance to search center */
			    if (drad > 0) {
				if (dist > drad)
				    pass = 0;
				if (dradi > 0.0 && dist < dradi)
				    pass = 0;
				}

			    /* Check distance along RA and Dec axes */
			    else {
				ddist = wcsdist (cra,cdec,cra,dec);
				if (ddist > ddec)
				    pass = 0;
				rdist = wcsdist (cra,dec,ra,dec);
			        if (rdist > dra)
				   pass = 0;
				}
			    }

			if (pass) {
			    if (refcat == UJC)
				num = (double) znum + (0.0000001*(double)istar);
			    else
				num = (double)istar;

			    /* Save star position and magnitude in table */
			    if (nstar < nstarmax) {
				unum[nstar] = num;
				ura[nstar] = ra;
				udec[nstar] = dec;
				umag[0][nstar] = mag;
				uplate[nstar] = plate;
				udist[nstar] = dist;
				if (dist > maxdist) {
				    maxdist = dist;
				    farstar = nstar;
				    }
				if (mag > faintmag) {
				    faintmag = mag;
				    faintstar = nstar;
				    }
				}

			    /* If too many stars and distance sorting,
				replace furthest star */
			    else if (distsort) {
				if (dist < maxdist) {
				    unum[farstar] = num;
				    ura[farstar] = ra;
				    udec[farstar] = dec;
				    umag[0][farstar] = mag;
				    uplate[farstar] = plate;
				    udist[farstar] = dist;

				/* Find new farthest star */
				    maxdist = 0.0;
				    for (i = 0; i < nstarmax; i++) {
					if (udist[i] > maxdist) {
					    maxdist = udist[i];
					    farstar = i;
					    }
					}
				    }
				}

			    /* If too many stars, replace faintest star */
			    else if (mag < faintmag) {
				unum[faintstar] = num;
				ura[faintstar] = ra;
				udec[faintstar] = dec;
				umag[0][faintstar] = mag;
				uplate[faintstar] = plate;
				udist[faintstar] = dist;
				faintmag = 0.0;

				/* Find new faintest star */
				for (i = 0; i < nstarmax; i++) {
				    if (umag[0][i] > faintmag) {
					faintmag = umag[0][i];
					faintstar = i;
					}
				    }
				}
			    nstar++;
			    jstar++;
			    if (nlog == 1)
				fprintf (stderr,"UJCREAD: %04d.%04d: %9.5f %9.5f %s %5.2f\n",
				    znum,istar,ra,dec,cstr,mag);

			    /* End of accepted star processing */
			    }

			/* End of individual star processing */
			}

		    /* Log operation */
		    if (nlog > 0 && itable%nlog == 0)
			fprintf (stderr,"UJCREAD: zone %d (%4d / %4d) %6d / %6d sources\r",
				znum, iz+1, nz, jstar, itable);

		    /* End of star loop */
		    }

		/* End of wrap loop */
		}

	/* Close zone input file */
	    (void) fclose (fcat);
	    itot = itot + itable;
	    if (nlog > 0)
		fprintf (stderr,"UJCREAD: zone %d (%4d / %4d) %6d / %6d / %8d sources\n",
			znum, iz+1, nz, jstar, itable, nstars);

	/* End of zone processing */
	    }

    /* End of zone loop */
	}

/* Summarize search */
    if (nlog > 0) {
	if (nz > 1)
	    fprintf (stderr,"UJCREAD: %d zone: %d / %d found\n",nz,nstar,itot);
	else
	    fprintf (stderr,"UJCREAD: 1 zone: %d / %d found\n",nstar,itable);
	if (nstar > nstarmax)
	    fprintf (stderr,"UJCREAD: %d stars found; only %d returned\n",
		     nstar,nstarmax);
	}
    free ((char *)udist);
    return (nstar);
}

/* UJCRNUM -- Read USNO J Catalog stars from CDROM or plate catalog from file */

int
ujcrnum (refcatname,nnum,sysout,eqout,epout,unum,ura,udec,umag,uplate,nlog)

char    *refcatname;    /* Name of catalog (UAC, USAC, UAC2, USAC2) */
int	nnum;		/* Number of stars to find */
int	sysout;		/* Search coordinate system */
double	eqout;		/* Search coordinate equinox */
double	epout;		/* Proper motion epoch (0.0 for no proper motion) */
double	*unum;		/* Array of UA numbers to find */
double	*ura;		/* Array of right ascensions (returned) */
double	*udec;		/* Array of declinations (returned) */
double	**umag;		/* Array of red magnitudes (returned) */
int	*uplate;	/* Array of plate numbers (returned) */
int	nlog;		/* Logging interval */
{
    UJCstar star;	/* UJ catalog entry for one star */
    int sysref=WCS_J2000;	/* Catalog coordinate system */
    double eqref=2000.0;	/* Catalog equinox */
    double epref=2000.0;	/* Catalog epoch */

    int znum;
    int jnum;
    int nzone;
    int nfound = 0;
    double ra,dec;
    double mag;
    int istar, plate, mprop, nmag;
    char *str;
    char title[128];

    /* Set catalog code and path to catalog */
    catname = refcatname;
    refcat = RefCat (refcatname,title,&sysref,&eqref,&epref,&mprop,&nmag);

    if (refcat == UJC && (str = getenv("UJ_PATH")) != NULL ) {

	/* If pathname is a URL, search and return */
	if (!strncmp (str, "http:",5)) {
	    return (webrnum (str,"ujc",nnum,sysout,eqout,epout,1,
			     unum,ura,udec,NULL,NULL,umag,uplate,nlog));
	    }
	else
	    strcpy (cdu,str);
	}

/* Loop through star list */
    for (jnum = 0; jnum < nnum; jnum++) {

    /* Get path to zone catalog */
	znum = (int) unum[jnum];
	if ((nzone = ujcopen (znum)) != 0) {

	    if (refcat == UJC)
		istar = (int) (((unum[jnum] - znum) * 100000000.0) + 0.5);
	    else
		istar = (int) (unum[jnum] + 0.5);

	/* Check to make sure star can be in this zone */
	    if (istar > nzone) {
		fprintf (stderr,"UJCRNUM: Star %d > zone max. %d\n",
			 istar, nzone);
		break;
		}

	/* Read star entry from catalog */
	    if (ujcstar (istar, &star)) {
		fprintf (stderr,"UJCRNUM: Cannot read star %d\n", istar);
		break;
		}

	    /* Extract selected fields if not probable duplicate */
	    else if (star.magetc > 0) {
		ra = ujcra (star.rasec); /* Right ascension in degrees */
		dec = ujcdec (star.decsec); /* Declination in degrees */
		wcscon (sysref, sysout, eqref, eqout, &ra, &dec, epout);
		mag = ujcmag (star.magetc); /* Magnitude */
		plate = ujcplate (star.magetc);	/* Plate number */

		/* Save star position and magnitude in table */
		ura[nfound] = ra;
		udec[nfound] = dec;
		umag[0][nfound] = mag;
		uplate[nfound] = plate;

		nfound++;
		if (nlog == 1)
		    fprintf (stderr,"UJCRNUM: %04d.%08d: %9.5f %9.5f %5.2f\n",
			     znum,istar,ra,dec,mag);

		/* Log operation */
		if (nlog > 0 && jnum%nlog == 0)
		    fprintf (stderr,"UJCRNUM: %04d.%08d  %8d / %8d sources\r",
			     znum, istar, jnum, nnum);

		(void) fclose (fcat);
		/* End of star processing */
		}

	    /* End of star */
	    }

	/* End of star loop */
	}

    /* Summarize search */
    if (nlog > 0)
	fprintf (stderr,"UJCRNUM:  %d / %d found\n",nfound,nnum);

    return (nfound);
}


/* UJCBIN -- Fill a FITS WCS image with USNO J Catalog stars */

int
ujcbin (refcatname, wcs, header, image, mag1,mag2, magscale, verbose)

char    *refcatname;    /* Name of catalog (UJC, xxxxx.usno) */
struct WorldCoor *wcs;	/* World coordinate system for image */
char	*header;	/* FITS header for output image */
char	*image;		/* Output FITS image */
double	mag1,mag2;	/* Limiting magnitudes (none if equal) */
double	magscale;	/* Scaling factor for magnitude to pixel flux
			 * (number of catalog objects per bin if 0) */
int	verbose;	/* 1 for diagnostics */
{
    double cra;		/* Search center J2000 right ascension in degrees */
    double cdec;	/* Search center J2000 declination in degrees */
    double dra;		/* Search half width in right ascension in degrees */
    double ddec;	/* Search half-width in declination in degrees */
    int sysout;		/* Search coordinate system */
    double eqout;	/* Search coordinate equinox */
    double epout;	/* Proper motion epoch (0.0 for no proper motion) */
    double ra1,ra2;	/* Limiting right ascensions of region in degrees */
    double dec1,dec2;	/* Limiting declinations of region in degrees */
    int nz;		/* Number of input UJ zone files */
    int zlist[NZONES];	/* List of input UJ zones */
    UJCstar star;	/* UJ catalog entry for one star */
    int sysref=WCS_J2000;	/* Catalog coordinate system */
    double eqref=2000.0;	/* Catalog equinox */
    double epref=2000.0;	/* Catalog epoch */
    char cstr[32];
    int xplate;		/* If nonzero, use objects only from this plate */

    double rra1, rra2, rdec1, rdec2;
    int wrap, iwrap;
    int znum, itot,iz;
    int nlog,jstar, mprop, nmag;
    int itable = 0;
    int nstar;
    int pass;
    int ix, iy;
    double ra,dec;
    double mag;
    double rdist, ddist;
    int istar, istar1, istar2, plate;
    int nzmax = NZONES;	/* Maximum number of declination zones */
    char *str;
    char title[128];
    double xpix, ypix, flux;
    int offscl;
    int bitpix, w, h;   /* Image bits/pixel and pixel width and height */
    double logt = log(10.0);

    itot = 0;
    xplate = getuplate ();

    /* Set image parameters */
    bitpix = 0;
    (void)hgeti4 (header, "BITPIX", &bitpix);
    w = 0;
    (void)hgeti4 (header, "NAXIS1", &w);
    h = 0;
    (void)hgeti4 (header, "NAXIS2", &h);

    /* Set catalog code and path to catalog */
    catname = refcatname;
    refcat = RefCat (refcatname,title,&sysref,&eqref,&epref,&mprop,&nmag);
    if (refcat == UJC && (str = getenv("UJ_PATH")) != NULL )
	strcpy (cdu,str);

    /* Set catalog search limits from image WCS information */
    sysout = wcs->syswcs;
    eqout = wcs->equinox;
    epout = wcs->epoch;
    wcscstr (cstr, sysout, eqout, epout);
    wcssize (wcs, &cra, &cdec, &dra, &ddec);
    SearchLim (cra,cdec,dra,ddec,sysout,&ra1,&ra2,&dec1,&dec2,verbose);

    /* mag1 is always the smallest magnitude */
    if (mag2 < mag1) {
	mag = mag2;
	mag2 = mag1;
	mag1 = mag;
	}

    rra1 = ra1;
    rra2 = ra2;
    rdec1 = dec1;
    rdec2 = dec2;
    RefLim (cra,cdec,dra,ddec,sysout,sysref,eqout,eqref,epout,epref,0.0,
	    &rra1, &rra2, &rdec1, &rdec2, &wrap, verbose);

    /* Find UJ Star Catalog regions in which to search */
    if (refcat == UJC) {
	nz = ujczones (rra1, rra2, rdec1, rdec2, nzmax, zlist, verbose);
	if (nz <= 0) {
	    fprintf (stderr,"UJCBIN:  no UJ zones found\n");
	    return (0);
	    }
	}
    else
	nz = 1;

    /* Logging interval */
    if (verbose)
	nlog = 100;
    else
	nlog = 0;
    nstar = 0;

    /* Loop through region list */
    for (iz = 0; iz < nz; iz++) {

	/* Get path to zone catalog */
	znum = zlist[iz];
	if ((nstars = ujcopen (znum)) != 0) {

	    jstar = 0;
	    itable = 0;
	    for (iwrap = 0; iwrap <= wrap; iwrap++) {

		/* Find first star based on RA */
		if (iwrap == 0 || wrap == 0)
		    istar1 = ujcsra (rra1);
		else
		    istar1 = 1;

		/* Find last star based on RA */
		if (iwrap == 1 || wrap == 0)
		    istar2 = ujcsra (rra2);
		else
		    istar2 = nstars;

		if (istar1 == 0 || istar2 == 0)
		    break;

		/* Loop through zone catalog for this region */
		for (istar = istar1; istar <= istar2; istar++) {
		    itable ++;

		    if (ujcstar (istar, &star)) {
			fprintf (stderr,"UJCBIN: Cannot read star %d\n", istar);
			break;
			}

		    /* Extract selected fields if not probable duplicate */
		    else if (star.magetc > 0) {
			mag = ujcmag (star.magetc);	/* Magnitude */

			/* Check magnitude limits */
			pass = 1;
			if (mag1 != mag2 && (mag < mag1 || mag > mag2))
			    pass = 0;

			/* Check plate number */
			plate = ujcplate (star.magetc);	/* Plate number */
			if (xplate != 0 && plate != xplate)
			    pass = 0;

			/* Check position limits */
			if (pass) {
			    ra = ujcra (star.rasec);	/* RA in degrees */
			    dec = ujcdec (star.decsec);	/* Dec in degrees */

			    /* Get position in output coordinate system */
			    wcscon (sysref,sysout,eqref,eqout,&ra,&dec,epout);

			    /* Check distance along RA and Dec axes */
			    ddist = wcsdist (cra,cdec,cra,dec);
			    if (ddist > ddec)
				pass = 0;
			    rdist = wcsdist (cra,dec,ra,dec);
			    if (rdist > dra)
				pass = 0;
			    }

			/* Save star in FITS image */
			if (pass) {
			    wcs2pix (wcs, ra, dec, &xpix, &ypix, &offscl);
			    if (!offscl) {
				if (magscale > 0.0)
				    flux = magscale * exp (logt * (-mag / 2.5));
				else
				    flux = 1.0;
				ix = (int) (xpix + 0.5);
				iy = (int) (ypix + 0.5);
				addpix1 (image, bitpix, w,h, 0.0,1.0, xpix,ypix, flux);
				nstar++;
				jstar++;
				}
			    else {
				ix = 0;
				iy = 0;
				}
			    if (nlog == 1) {
				fprintf (stderr,"UJCBIN: %04d.%04d: %9.5f %9.5f %s",
					 znum, istar,ra,dec,cstr);
				if (magscale > 0.0)
				    fprintf (stderr, " %5.2f", mag);
				if (!offscl)
				    flux = getpix1 (image, bitpix, w, h, 0.0, 1.0, ix, iy);
				else
				    flux = 0.0;
				fprintf (stderr," (%d,%d): %f\n", ix, iy, flux);
				}

			    /* End of accepted star processing */
			    }

			/* End of individual star processing */
			}

		    /* Log operation */
		    if (nlog > 0 && itable%nlog == 0)
			fprintf (stderr,"UJCBIN: zone %d (%4d / %4d) %6d / %6d sources\r",
				znum, iz+1, nz, jstar, itable);

		    /* End of star loop */
		    }

		/* End of wrap loop */
		}

	/* Close zone input file */
	    (void) fclose (fcat);
	    itot = itot + itable;
	    if (nlog > 0)
		fprintf (stderr,"UJCBIN: zone %d (%4d / %4d) %6d / %6d / %8d sources\n",
			znum, iz+1, nz, jstar, itable, nstars);

	/* End of zone processing */
	    }

    /* End of zone loop */
	}

/* Summarize search */
    if (nlog > 0) {
	if (nz > 1)
	    fprintf (stderr,"UJCBIN: %d zone: %d / %d found\n",nz,nstar,itot);
	else
	    fprintf (stderr,"UJCBIN: 1 zone: %d / %d found\n",nstar,itable);
	}
    return (nstar);
}


/* Declination zone numbers */
int zone[NZONES]={0,75,150,225,300,375,450,525,600,675,750,825,900,
	      975,1050,1125,1200,1275,1350,1425,1500,1575,1650,1725};

/* UJCZONES -- figure out which UJ zones will need to be searched */

static int
ujczones (ra1, ra2, dec1, dec2, nzmax, zones, verbose)

double	ra1, ra2;	/* Right ascension limits in degrees */
double	dec1, dec2; 	/* Declination limits in degrees */
int	nzmax;		/* Maximum number of zones to find */
int	*zones;		/* Region numbers (returned)*/
int	verbose;	/* 1 for diagnostics */

{
    int nrgn;		/* Number of zones found (returned) */
    int iz,iz1,iz2,i;

    for (i = 0; i < nzmax; i++)
	zones[i] = 0;

    nrgn = 0;

/* Find zone range to search based on declination */
    iz1 = ujczone (dec1);
    iz2 = ujczone (dec2);

/* Tabulate zones to search */
    i = 0;
    if (iz2 >= iz1) {
	for (iz = iz1; iz <= iz2; iz++)
	    zones[i++] = zone[iz];
	}
    else {
	for (iz = iz2; iz <= iz1; iz++)
	    zones[i++] = zone[iz];
	}

    nrgn = i;
    if (verbose) {
	fprintf (stderr,"UJCREG:  %d zones: %d - %d\n",nrgn,zones[0],zones[i-1]);
	fprintf(stderr,"UJCREG: RA: %.5f - %.5f, Dec: %.5f - %.5f\n",ra1,ra2,dec1,dec2);
	}

    return (nrgn);
}


/* UJCRA -- returns right ascension in degrees from the UJ star structure */

static double
ujcra (rasec)

int rasec;	/* RA in 100ths of arcseconds from UJ catalog entry */
{
    return ((double) (rasec) / 360000.0);
}


/* UJCDEC -- returns the declination in degrees from the UJ star structure */

static double
ujcdec (decsec)

int decsec;	/* Declination in 100ths of arcseconds from UJ catalog entry */
{
    return ((double) (decsec - 32400000) / 360000.0);
}


/* UJCMAG -- returns the magnitude from the UJ star structure */

static double
ujcmag (magetc)

int magetc;	/* Quality, plate, and magnitude from UJ catalog entry */
{
    if (magetc < 0)
	return ((double) (-magetc % 10000) * 0.01);
    else
	return ((double) (magetc % 10000) * 0.01);
}


/* UJCPLATE -- returns the plate number from the UJ star structure */

static int
ujcplate (magetc)

int magetc;	/* Quality, plate, and magnitude from UJ catalog entry */
{
    if (magetc < 0)
	return ( (-magetc % 10000000) / 10000);
    else
	return ( (magetc % 10000000) / 10000);
}


/* UJCZONE -- find the UJ zone number where a declination can be found */

static int
ujczone (dec)

double dec;	/* declination in degrees */
{
    double zonesize = 7.5;	/* number of degrees per declination zone */

    return ((int) ((dec + 90.0) / zonesize));
}


/* UJCSRA -- Find UJ star closest to specified right ascension */

static int
ujcsra (rax0)

double	rax0;		/* Right ascension for which to search */
{
    int istar, istar1, istar2, nrep;
    double rax, ra1, ra, rdiff, rdiff1, rdiff2, sdiff;
    UJCstar star;	/* UJ catalog entry for one star */
    char rastrx[32];
    int debug = 0;

    rax = rax0;
    ra2str (rastrx, 31, rax, 3);
    istar1 = 1;
    if (ujcstar (istar1, &star))
	return (0);
    ra1 = ujcra (star.rasec);
    istar = nstars;
    nrep = 0;
    while (istar != istar1 && nrep < 20) {
	if (ujcstar (istar, &star))
	    break;
	else {
	    ra = ujcra (star.rasec);
	    if (ra == ra1)
		break;
	    if (debug) {
		char rastr[32];
		ra2str (rastr, 31, ra, 3);
		fprintf (stderr,"UJCSRA %d %d: %s (%s)\n",
			 nrep,istar,rastr,rastrx);
		}
	    rdiff = ra1 - ra;
	    rdiff1 = ra1 - rax;
	    rdiff2 = ra - rax;
	    if (nrep > 20 && ABS(rdiff2) > ABS(rdiff1)) {
		istar = istar1;
		break;
		}
	    nrep++;
	    sdiff = (double)(istar - istar1) * rdiff1 / rdiff;
	    istar2 = istar1 + (int) (sdiff + 0.5);
	    ra1 = ra;
	    istar1 = istar;
	    istar = istar2;
	    if (debug) {
		fprintf (stderr,"UJCSRA: ra1=    %.5f ra=     %.5f rax=    %.5f\n",
			 ra1,ra,rax);
		fprintf (stderr,"UJCSRA: rdiff=  %.5f rdiff1= %.5f rdiff2= %.5f\n",
			 rdiff,rdiff1,rdiff2);
		fprintf (stderr,"UJCSRA: istar1= %d istar= %d istar1= %d\n",
			 istar1,istar,istar2);
		}
	    if (istar < 1)
		istar = 1;
	    if (istar > nstars)
		istar = nstars;
	    if (istar == istar1)
		break;
	    }
	}
    return (istar);
}

/* UJCOPEN -- Open UJ Catalog zone catalog, returning number of entries */

static int
ujcopen (znum)

int znum;	/* UJ Catalog zone */
{
    char zonepath[128];	/* Pathname for input UJ zone file */
    UJCstar star;	/* UJ catalog entry for one star */
    int lfile;

/* Get path to zone catalog */
    if (ujcpath (znum, zonepath)) {
	fprintf (stderr, "UJCOPEN: Cannot find zone catalog for %d\n", znum);
	return (0);
	}

/* Find number of stars in zone catalog by its length */
    lfile = getfilesize (zonepath);
    if (lfile < 2) {
	fprintf (stderr,"UJCOPEN: Zone catalog %s has no entries\n",zonepath);
	return (0);
	}
    else
	nstars = lfile / 12;

/* Open zone catalog */
    if (!(fcat = fopen (zonepath, "rb"))) {
	fprintf (stderr,"UJCOPEN: Zone catalog %s cannot be read\n",zonepath);
	return (0);
	}

/* Check to see if byte-swapping is necessary */
    cswap = 0;
    if (ujcstar (1, &star)) {
	fprintf (stderr,"UJCOPEN: cannot read star 1 from UJ zone catalog %s\n",
		 zonepath);
	return (0);
	}
    else {
	if (star.decsec < 0) {
	    cswap = 1;
	    fprintf (stderr,"UJCOPEN: swapping bytes in UJ zone catalog %s\n",
		     zonepath);
	    }
	else
	    cswap = 0;
	}

    return (nstars);
}


/* UJCPATH -- Get UJ Catalog region file pathname */

static int
ujcpath (zn, path)

int zn;		/* UJ zone number */
char *path;	/* Pathname of UJ zone file */

{
    if (refcat == USNO) {
	strcpy (path, catname);
	return (0);
	}
    else if (zn < 0 || zn > 1725) {
	fprintf (stderr, "UJCPATH: zone %d out of range 0-1725\n",zn);
	return (-1);
	}
    else if (strchr (cdu,'C'))
	sprintf (path,"%s/ZONE%04d.CAT", cdu, zn);
    else
	sprintf (path,"%s/zone%04d.cat", cdu, zn);

    return (0);
}


/* UJCSTAR -- Get UJ catalog entry for one star; return 0 if successful */

static int
ujcstar (istar, star)

int istar;	/* Star sequence number in UJ zone catalog */
UJCstar *star;	/* UJ catalog entry for one star */
{
    int nbs, nbr, nbskip;

    if (istar < 1 || istar > nstars) {
	fprintf (stderr, "UJCstar %d is not in catalog\n",istar);
	return (-1);
	}
    nbskip = 12 * (istar - 1);
    if (fseek (fcat,nbskip,SEEK_SET))
	return (-1);
    nbs = sizeof (UJCstar);
    nbr = fread (star, nbs, 1, fcat) * nbs;
    if (nbr < nbs) {
	fprintf (stderr, "UJCstar %d / %d bytes read\n",nbr, nbs);
	return (-2);
	}
    if (cswap)
	ujcswap ((char *)star);
    return (0);
}


/* UJCSWAP -- Reverse bytes of UJ Catalog entry */

static void
ujcswap (string)

char *string;	/* Start of vector of 4-byte ints */

{
char *sbyte, *slast;
char temp0, temp1, temp2, temp3;
int nbytes = 12; /* Number of bytes to reverse */

    slast = string + nbytes;
    sbyte = string;
    while (sbyte < slast) {
	temp3 = sbyte[0];
	temp2 = sbyte[1];
	temp1 = sbyte[2];
	temp0 = sbyte[3];
	sbyte[0] = temp0;
	sbyte[1] = temp1;
	sbyte[2] = temp2;
	sbyte[3] = temp3;
	sbyte = sbyte + 4;
	}
    return;
}

/* May 20 1996	New subroutine
 * May 23 1996	Add optional plate number check
 * Aug  6 1996	Remove unused variables after lint
 * Oct 15 1996	Add comparison when testing an assignment
 * Oct 17 1996	Fix after lint: cast argument to UJCSWAP
 * Nov 13 1996	Return no more than maximum star number
 * Nov 13 1996	Write all error messages to stderr with subroutine names
 * Nov 15 1996  Implement search radius; change input arguments
 * Dec 12 1996	Improve logging
 * Dec 17 1996	Add code to keep brightest or closest stars if too many found
 * Dec 18 1996	Add code to read a specific star
 * Mar 20 1997	Clean up UJCRNUM after lint
 *
 * Jun 24 1998	Add string lengths to ra2str() and dec2str() calls
 * Jun 24 1998	Initialize byte-swapping flag in UJCOPEN()
 * Sep 22 1998	Convert to desired output coordinate system
 * Oct 29 1998	Correctly assign numbers when too many stars are found
 *
 * Jun 16 1999	Use SearchLim()
 * Aug 16 1999	Add RefLim() to get converted search coordinates right
 * Aug 16 1999  Fix bug to fix failure to search across 0:00 RA
 * Aug 25 1999  Return real number of stars from ujcread()
 * Aug 26 1999	Set nlog to 100 if verbose mode is on
 * Sep 10 1999	Set plate selection with subroutine, not argument
 * Sep 16 1999	Fix bug which didn't always return closest stars
 * Sep 16 1999	Add distsort argument so brightest stars in circle works, too
 * Oct 20 1999	Include wcscat.h
 * Oct 21 1999	Delete unused variables after lint
 *
 * Jun 26 2000	Add coordinate system to SearchLim() arguments
 * Oct 25 2000	Add USNO plate catalogs; fix byte order test
 * Nov 29 2000	Add option to read UJ catalog using HTTP
 *
 * Jan 11 2001	All printing is to stderr
 * Jun  7 2001	Add proper motion flag and number of magnitudes to RefCat()
 * Sep 19 2001	Drop fitshead.h; it is in wcs.h
 *
 * Feb  4 2003	Open catalog file rb instead of r (Martin Ploner, Bern)
 * Mar 11 2003	Improve position filtering
 * May 27 2003	Use getfilesize() to get file length
 * Aug 22 2003	Add radi argument for inner edge of search annulus
 * Sep 25 2003	Add ujcbin() to fill an image with sources
 * Oct  6 2003	Update ujcread() and ujcbin() for improved RefLim()
 * Nov 18 2003	Initialize image size and bits/pixel from header in ujcbin()
 * Dec 12 2003	Fix bug in wcs2pix() call in ujcbin()
 *
 * Aug 30 2004	Include fitsfile.h and math.h
 *
 * Jun 20 2006	Initialize uninitialized variables
 * Sep 26 2006	Increase length of rastr and destr from 16 to 32
 * Nov 16 2006	Fix binning
 *
 * Jan  8 2007	Fix bad format statement in ujcbin()
 * Jan 10 2007	Add match=1 argument to webrnum()
 * Jul  5 2007	Fix bug in ujcread() and ujcbin() which always rejected stars
 */
