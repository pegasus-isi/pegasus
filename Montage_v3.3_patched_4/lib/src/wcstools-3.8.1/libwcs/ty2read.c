/*** File libwcs/ty2read.c
 *** July 9, 2007
 *** By Doug Mink, dmink@cfa.harvard.edu
 *** Harvard-Smithsonian Center for Astrophysics
 *** Copyright (C) 2000-2007
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
#include <stdio.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include "fitsfile.h"
#include "wcs.h"
#include "wcscat.h"

#define MAXREG 10000

/* pathname of Tycho 2 CDROM or catalog search engine URL */
char ty2cd[64]="/data/astrocat/tycho2";

static double *gdist;	/* Array of distances to stars */
static int ndist = 0;

static int ty2reg();
static int ty2regn();
static int ty2zone();
static int ty2size();
struct StarCat *ty2open();
void ty2close();
static int ty2star();
static int ty2size();


/* TY2READ -- Read Tycho 2 Star Catalog stars from CDROM */

int
ty2read (refcat,cra,cdec,dra,ddec,drad,dradi,distsort,sysout,eqout,epout,
	 mag1,mag2,sortmag,nstarmax,gnum,gra,gdec,gpra,gpdec,gmag,gtype,nlog)

int	refcat;		/* Catalog code from wcscat.h */
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
int	sortmag;	/* Magnitude by which to sort (1 or 2) */
int	nstarmax;	/* Maximum number of stars to be returned */
double	*gnum;		/* Array of Guide Star numbers (returned) */
double	*gra;		/* Array of right ascensions (returned) */
double	*gdec;		/* Array of declinations (returned) */
double  *gpra;          /* Array of right ascension proper motions (returned) */
double  *gpdec;         /* Array of declination proper motions (returned) */
double	**gmag;		/* Array of b and v magnitudes (returned) */
int	*gtype;		/* Array of object types (returned) */
int	nlog;		/* 1 for diagnostics */
{
    double ra1,ra2;	/* Limiting right ascensions of region in degrees */
    double dec1,dec2;	/* Limiting declinations of region in degrees */
    double dist = 0.0;  /* Distance from search center in degrees */
    double faintmag=0.0; /* Faintest magnitude */
    double maxdist=0.0; /* Largest distance */
    int	faintstar=0;	/* Faintest star */
    int	farstar=0;	/* Most distant star */
    int nreg = 0;	/* Number of Tycho 2 regions in search */
    int regnum[MAXREG];	/* List of region numbers */
    int rlist[MAXREG];	/* List of first stars in regions */
    int nlist[MAXREG];	/* List of number of stars per region */
    char inpath[128];	/* Pathname for input region file */
    int sysref = WCS_J2000;	/* Catalog coordinate system */
    double eqref = 2000.0;	/* Catalog equinox */
    double epref = 2000.0;	/* Catalog epoch */
    double secmarg = 60.0;	/* Arcsec/century margin for proper motion */
    struct StarCat *starcat;
    struct Star *star;
    int verbose;
    int wrap;
    int ireg;
    int ierr;
    int magsort, magsort1;
    int jstar, iw;
    int nrmax = MAXREG;
    int nstar,i, ntot;
    int istar, istar1, istar2;
/*    int isp; */
    int pass;
    double num, ra, dec, rapm, decpm, mag, magb, magv, magve, magbe;
    double rra1, rra2, rra2a, rdec1, rdec2;
    double rdist, ddist;
    char cstr[32], rastr[32], decstr[32];
    char *str;

    ntot = 0;
    if (nlog > 0)
	verbose = 1;
    else
	verbose = 0;

    /* If pathname is a URL, search and return */
    if ((str = getenv("TY2_PATH")) == NULL )
	str = ty2cd;
    else
	strncpy (ty2cd, str, 64);
    if (!strncmp (str, "http:",5)) {
	return (webread (ty2cd,"tycho2",distsort,cra,cdec,dra,ddec,drad,dradi,
			 sysout,eqout,epout,mag1,mag2,sortmag,nstarmax,
			 gnum,gra,gdec,gpra,gpdec,gmag,gtype,nlog));
	}

    wcscstr (cstr, sysout, eqout, epout);

    SearchLim (cra,cdec,dra,ddec,sysout,&ra1,&ra2,&dec1,&dec2,verbose);

    /* Make mag1 always the smallest magnitude */
    if (mag2 < mag1) {
	mag = mag2;
	mag2 = mag1;
	mag1 = mag;
	}

   if (sortmag == 2) {
	magsort = 0;
	magsort1 = 1;
	}
    else {
	magsort = 1;
	magsort1 = 0;
	}

    /* Allocate table for distances of stars from search center */
    if (nstarmax > ndist) {
	if (ndist > 0)
	    free ((void *)gdist);
	gdist = (double *) malloc (nstarmax * sizeof (double));
	if (gdist == NULL) {
	    fprintf (stderr,"TY2READ:  cannot allocate separation array\n");
	    return (0);
	    }
	ndist = nstarmax;
	}

    /* Allocate catalog entry buffer */
    star = (struct Star *) calloc (1, sizeof (struct Star));
    star->num = 0.0;

    nstar = 0;
    jstar = 0;

    /* Get RA and Dec limits in catalog (J2000) coordinates */
    RefLim (cra,cdec,dra,ddec,sysout,sysref,eqout,eqref,epout,epref,
	    secmarg, &rra1, &rra2, &rdec1, &rdec2, &wrap, verbose);
    if (wrap) {
	rra2a = rra2;
	rra2 = 360.0;
	}
    else {
	rra2a = 0;
	}

    /* Write header if printing star entries as found */
    if (nstarmax < 1) {
	char *revmessage;
	revmessage = getrevmsg();
	printf ("catalog	Tycho-2\n");
	ra2str (rastr, 31, cra, 3);
	printf ("ra	%s\n", rastr);
	dec2str (decstr, 31, cdec, 2);
	printf ("dec	%s\n", decstr);
	printf ("rpmunit	mas/year\n");
	printf ("dpmunit	mas/year\n");
	if (drad != 0.0) {
	    printf ("radmin	%.1f\n", drad*60.0);
	    if (dradi > 0)
		printf ("radimin	%.1f\n", dradi*60.0);
	    }
	else {
	    printf ("dramin	%.1f\n", dra*60.0* cosdeg (cdec));
	    printf ("ddecmin	%.1f\n", ddec*60.0);
	    }
	printf ("radecsys	%s\n", cstr);
	printf ("equinox	%.3f\n", eqout);
	printf ("epoch	%.3f\n", epout);
	printf ("program	scat %s\n", revmessage);
	printf ("tycho2_id	ra          	dec         ");
	printf ("	magb 	magv");
	if (refcat == TYCHO2E)
	    printf (" 	magbe	magve");
	printf ("	ura   	udec  	arcmin\n");
	printf ("----------	------------	------------");
	printf ("	-----	-----");
	if (refcat == TYCHO2E)
	    printf ("	-----	-----");
	printf ("	------	------	------\n");
	}

    /* If searching through RA = 0:00, split search in two */
    for (iw = 0; iw <= wrap; iw++) {

	/* Find Tycho 2 Star Catalog regions in which to search */
	nreg = ty2reg (rra1,rra2,rdec1,rdec2,nrmax,regnum,rlist,nlist,verbose);
	if (nreg <= 0) {
	    fprintf (stderr,"TY2READ:  no Tycho 2 region for %.2f-%.2f %.2f %.2f\n",
		     rra1, rra2, rdec1, rdec2);
	    rra1 = 0.0;
	    rra2 = rra2a;
	    continue;
	    }

	/* Loop through region list */
	for (ireg = 0; ireg < nreg; ireg++) {

	    /* Open catalog file for this region */
	    istar1 = rlist[ireg];
	    istar2 = istar1 + nlist[ireg];
	    if (verbose)
		fprintf (stderr,"TY2READ: Searching stars %d through %d\n",
			istar1, istar2-1);

	    /* Open file for this region of Tycho 2 catalog */
	    starcat = ty2open (rlist[ireg], nlist[ireg]);
	    if (starcat == NULL) {
		fprintf (stderr,"TY2READ: File %s not found\n",inpath);
		return (0);
		}

	    /* Loop through catalog for this region */
	    for (istar = istar1; istar < istar2; istar++) {
		if ((ierr = ty2star (starcat, star, istar))) {
		    /* fprintf (stderr,"TY2READ: Cannot read star %d\n", istar); */
		    if (ierr < 3)
			break;
		    else
			continue;
		    }

		/* ID number */
		num = star->num;

		/* Magnitude */
		magb = star->xmag[0];
		magv = star->xmag[1];
		magbe = star->xmag[2];
		magve = star->xmag[3];
		mag = star->xmag[magsort];

		/* Check magnitude limits */
		pass = 1;
		if (mag1 != mag2 && (mag < mag1 || mag > mag2))
		    pass = 0;

		/* Check position limits */
		if (pass) {

		    /* Get position in output coordinate system */
		    rapm = star->rapm;
		    decpm = star->decpm;
		    ra = star->ra;
		    dec = star->dec;
		    wcsconp (sysref, sysout, eqref, eqout, epref, epout,
		 	     &ra, &dec, &rapm, &decpm);

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

		/* Spectral Type
		isp = (1000 * (int) star->isp[0]) + (int)star->isp[1]; */

		/* Write star position and magnitudes to stdout */
		    if (nstarmax < 1) {
			ra2str (rastr, 31, ra, 3);
			dec2str (decstr, 31, dec, 2);
			dist = wcsdist (cra,cdec,ra,dec) * 60.0;
			printf ("%010.5f	%s	%s", num,rastr,decstr);
			printf ("	%5.2f	%5.2f", magb, magv);
			if (refcat == TYCHO2E)
			    printf ("	%5.2f	%5.2f", magbe, magve);
			printf ("	%6.1f	%6.1f	%.2f\n",
				rapm * 3600000.0 * cosdeg(dec),
				decpm * 3600000.0, dist / 60.0);
			}

		    /* Save star position and magnitude in table */
		    if (nstar < nstarmax) {
			gnum[nstar] = num;
			gra[nstar] = ra;
			gdec[nstar] = dec;
			gpra[nstar] = rapm;
			gpdec[nstar] = decpm;
			gmag[0][nstar] = magb;
			gmag[1][nstar] = magv;
			if (refcat == TYCHO2E) {
			    gmag[2][nstar] = star->xmag[2];
			    gmag[3][nstar] = star->xmag[3];
			    }
			gdist[nstar] = dist;
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
		       replace farthest star */
		    else if (distsort) {
			if (dist < maxdist) {
			    gnum[farstar] = num;
			    gra[farstar] = ra;
			    gdec[farstar] = dec;
			    gpra[farstar] = rapm;
			    gpdec[farstar] = decpm;
			    gmag[0][farstar] = magb;
			    gmag[1][farstar] = magv;
			    if (refcat == TYCHO2E) {
				gmag[2][farstar] = star->xmag[2];
				gmag[3][farstar] = star->xmag[3];
				}
			    gdist[farstar] = dist;

			    /* Find new farthest star */
			    maxdist = 0.0;
			    for (i = 0; i < nstarmax; i++) {
				if (gdist[i] > maxdist) {
				    maxdist = gdist[i];
				    farstar = i;
				    }
				}
			    }
			}

		    /* Else if too many stars, replace faintest star */
		    else if (mag < faintmag) {
			gnum[faintstar] = num;
			gra[faintstar] = ra;
			gdec[faintstar] = dec;
			gpra[faintstar] = rapm;
			gpdec[faintstar] = decpm;
			gmag[0][faintstar] = magb;
			gmag[1][faintstar] = magv;
			if (refcat == TYCHO2E) {
			    gmag[2][faintstar] = star->xmag[2];
			    gmag[3][faintstar] = star->xmag[3];
			    }
			gdist[faintstar] = dist;
			faintmag = 0.0;

			/* Find new faintest star */
			for (i = 0; i < nstarmax; i++) {
			    if (gmag[magsort1][i] > faintmag) {
				faintmag = gmag[magsort1][i];
				faintstar = i;
				}
			    }
			}

		    nstar++;
		    if (nlog == 1)
			fprintf (stderr,"TY2READ: %11.6f: %9.5f %9.5f %5.2f %5.2f\n",
				 num,ra,dec,magb,magv);

		    /* End of accepted star processing */
		    }

		/* Log operation */
		jstar++;
		if (nlog > 0 && istar%nlog == 0)
		    fprintf (stderr,"TY2READ: %5d / %5d / %5d sources\r",
			     nstar,jstar,starcat->nstars);

		/* End of star loop */
		}

	    ntot = ntot + starcat->nstars;
	    if (nlog > 0)
		fprintf (stderr,"TY2READ: %4d / %4d: %5d / %5d  / %5d sources from region %4d    \n",
		 	 ireg+1,nreg,nstar,jstar,starcat->nstars,regnum[ireg]);

	    /* Close region input file */
	    ty2close (starcat);
	    }
	rra1 = 0.0;
	rra2 = rra2a;
	}

/* close output file and summarize transfer */
    if (nlog > 0) {
	if (nreg > 1)
	    fprintf (stderr,"TY2READ: %d regions: %d / %d found\n",nreg,nstar,ntot);
	else
	    fprintf (stderr,"TY2READ: 1 region: %d / %d found\n",nstar,ntot);
	if (nstar > nstarmax)
	    fprintf (stderr,"TY2READ: %d stars found; only %d returned\n",
		     nstar,nstarmax);
	}
    return (nstar);
}

/* TY2RNUM -- Read HST Guide Star Catalog stars from CDROM */

int
ty2rnum (refcat, nstars,sysout,eqout,epout,
	 gnum,gra,gdec,gpra,gpdec,gmag,gtype,nlog)

int	refcat;		/* Catalog code from wcscat.h */
int	nstars;		/* Number of stars to find */
int	sysout;		/* Search coordinate system */
double	eqout;		/* Search coordinate equinox */
double	epout;		/* Proper motion epoch (0.0 for no proper motion) */
double	*gnum;		/* Array of Guide Star numbers (returned) */
double	*gra;		/* Array of right ascensions (returned) */
double	*gdec;		/* Array of declinations (returned) */
double  *gpra;          /* Array of right ascension proper motions (returned) */
double  *gpdec;         /* Array of declination proper motions (returned) */
double	**gmag;		/* Array of B and V magnitudes (returned) */
int	*gtype;		/* Array of object types (returned) */
int	nlog;		/* 1 for diagnostics */
{
    char inpath[128];	/* Pathname for input region file */
    int sysref=WCS_J2000;	/* Catalog coordinate system */
    double eqref=2000.0;	/* Catalog equinox */
    double epref=2000.0;	/* Catalog epoch */
    struct StarCat *starcat;
    struct Star *star;
    char *str;

    int verbose;
    int rnum;
    int ierr;
    int jstar;
    int istar, istar1, istar2, jstar1, jstar2, nstar;
/*    int isp; */
    double num, ra, dec, rapm, decpm, magb, magv;

    if (nlog == 1)
	verbose = 1;
    else
	verbose = 0;

    /* If pathname is a URL, search and return */
    if ((str = getenv("TY2_PATH")) == NULL )
	str = ty2cd;
    if (!strncmp (str, "http:",5))
	return (webrnum (str,"tycho2",nstars,sysout,eqout,epout,1,
			 gnum,gra,gdec,gpra,gpdec,gmag,gtype,nlog));

    /* Allocate catalog entry buffer */
    star = (struct Star *) calloc (1, sizeof (struct Star));
    star->num = 0.0;
    nstar = 0;

/* Loop through star list */
    for (jstar = 0; jstar < nstars; jstar++) {
	rnum = (int) (gnum[jstar] + 0.0000001);
	
	/* Find numbered stars (rrrr.nnnnn) */
	if (gnum[jstar]-(double)rnum > 0.0000001) {
	    ty2regn (rnum, &istar1, &istar2, verbose);
	    nstar = istar2 - istar1 + 1;
	    starcat = ty2open (istar1, nstar);
    	    if (starcat == NULL) {
		fprintf (stderr,"TY2RNUM: File %s not found\n",inpath);
		return (0);
		}
	    for (istar = istar1; istar < istar2; istar++) {
		if ((ierr = ty2star (starcat, star, istar))) {
		    /* fprintf (stderr,"TY2RNUM: Cannot read star %d\n", istar); */
		    gra[jstar] = 0.0;
		    gdec[jstar] = 0.0;
		    gmag[0][jstar] = 0.0;
		    gmag[1][jstar] = 0.0;
		    gmag[2][jstar] = 0.0;
		    gmag[3][jstar] = 0.0;
		    gtype[jstar] = 0;
		    if (ierr < 3)
			break;
		    }
		else {
		    if (fabs (gnum[jstar] - star->num) < 0.0000005)
			break;
		    }
		}
	    ty2close (starcat);
	    }
	/* Find nth sequential stars in catalog (not rrrr.nnnnn) */
	else {
	    /* Find out whether file has CR/LF or LF only at end of lines */
	    rnum = 1;
	    ty2regn (rnum, &jstar1, &jstar2, verbose);

	    istar = (int) (gnum[jstar] + 0.01);
	    starcat = ty2open (istar, 10);
    	    if (starcat == NULL) {
		fprintf (stderr,"TY2RNUM: File %s not found\n",inpath);
		return (0);
		}
	    if ((ierr = ty2star (starcat, star, istar))) {
		/* fprintf (stderr,"TY2RNUM: Cannot read star %d\n", istar); */
		gra[jstar] = 0.0;
		gdec[jstar] = 0.0;
		gmag[0][jstar] = 0.0;
		gmag[1][jstar] = 0.0;
		if (refcat == TYCHO2E) {
		    gmag[2][jstar] = 0.0;
		    gmag[3][jstar] = 0.0;
		    }
		gtype[jstar] = 0;
		if (ierr < 3)
		    break;
		else
		    continue;
		}
	    ty2close (starcat);
	    }

	/* If star has been found in catalog */

	/* ID number */
	num = star->num;

	/* Position in degrees at designated epoch */
	ra = star->ra;
	dec = star->dec;
	rapm = star->rapm;
	decpm = star->decpm;
	wcsconp (sysref, sysout, eqref, eqout, epref, epout,
		     &ra, &dec, &rapm, &decpm);

	/* Magnitude */
	magb = star->xmag[0];
	magv = star->xmag[1];

	/* Spectral Type
	isp = (1000 * (int) star->isp[0]) + (int)star->isp[1]; */

	/* Save star position and magnitude in table */
	gnum[jstar] = num;
	gra[jstar] = ra;
	gdec[jstar] = dec;
	gpra[jstar] = rapm;
	gpdec[jstar] = decpm;
	gmag[0][jstar] = magb;
	gmag[1][jstar] = magv;
	if (refcat == TYCHO2E) {
	    gmag[2][jstar] = star->xmag[2];
	    gmag[3][jstar] = star->xmag[3];
	    }
	/* gtype[jstar] = isp; */
	if (nlog == 1)
	    fprintf (stderr,"TY2RNUM: %11.6f: %9.5f %9.5f %5.2f %5.2f %s  \n",
		     num, ra, dec, magb, magv, star->isp);

	/* End of star loop */
	}

/* Summarize search */
    if (nlog > 0)
	fprintf (stderr,"TY2RNUM: %d / %d found\n", nstar, nstars);

    return (nstars);
}


/* TY2BIN -- Read Tycho 2 Star Catalog stars from CDROM */

int
ty2bin (wcs, header, image, mag1, mag2, sortmag, magscale, nlog)

struct WorldCoor *wcs;	/* World coordinate system for image */
char	*header;	/* FITS header for output image */
char	*image;		/* Output FITS image */
double	mag1,mag2;	/* Limiting magnitudes (none if equal) */
int	sortmag;	/* Magnitude by which to sort (1 or 2) */
double	magscale;	/* Scaling factor for magnitude to pixel flux
			 * (number of catalog objects per bin if 0) */
int	nlog;		/* 1 for diagnostics */
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
    int nreg = 0;	/* Number of Tycho 2 regions in search */
    int regnum[MAXREG];	/* List of region numbers */
    int rlist[MAXREG];	/* List of first stars in regions */
    int nlist[MAXREG];	/* List of number of stars per region */
    char inpath[128];	/* Pathname for input region file */
    int sysref = WCS_J2000;	/* Catalog coordinate system */
    double eqref = 2000.0;	/* Catalog equinox */
    double epref = 2000.0;	/* Catalog epoch */
    double secmarg = 60.0;	/* Arcsec/century margin for proper motion */
    struct StarCat *starcat;
    struct Star *star;
    int verbose;
    int wrap;
    int ireg;
    int ierr;
    int magsort;
    int jstar, iw;
    int nrmax = MAXREG;
    int nstar, ntot;
    int istar, istar1, istar2;
    int pass;
    double num, ra, dec, mag, magb, magv;
    double rra1, rra2, rra2a, rdec1, rdec2;
    char cstr[32];
    char *str;
    double xpix, ypix, flux;
    int ix, iy, offscl;
    int bitpix, w, h;   /* Image bits/pixel and pixel width and height */
    double logt = log(10.0);

    /* Get image dimensions */
    bitpix = 0;
    (void)hgeti4 (header,"BITPIX",&bitpix);
    w = 0;
    (void)hgeti4 (header,"NAXIS1",&w);
    h = 0;
    (void)hgeti4 (header,"NAXIS2",&h);
    if (bitpix * w * h < 1) {
	fprintf (stderr, "TY2BIN: No pixels in image = %d bytes x %d x %d\n",
		 bitpix, w, h);
	return (0);
	}

    ntot = 0;
    if (nlog > 0)
	verbose = 1;
    else
	verbose = 0;

    /* If pathname is a URL, search and return */
    if ((str = getenv("TY2_PATH")) != NULL )
	strncpy (ty2cd, str, 64);

    /* Set catalog search limits from image WCS information */
    sysout = wcs->syswcs;
    eqout = wcs->equinox;
    epout = wcs->epoch;
    wcscstr (cstr, sysout, eqout, epout);
    wcssize (wcs, &cra, &cdec, &dra, &ddec);
    SearchLim (cra,cdec,dra,ddec,sysout,&ra1,&ra2,&dec1,&dec2,verbose);

    /* Make mag1 always the smallest magnitude */
    if (mag2 < mag1) {
	mag = mag2;
	mag2 = mag1;
	mag1 = mag;
	}

   if (sortmag == 2)
	magsort = 0;
    else
	magsort = 1;

    /* Allocate catalog entry buffer */
    star = (struct Star *) calloc (1, sizeof (struct Star));
    star->num = 0.0;

    nstar = 0;
    jstar = 0;

    /* Get RA and Dec limits in catalog (J2000) coordinates */
    RefLim (cra,cdec,dra,ddec,sysout,sysref,eqout,eqref,epout,epref,
	    secmarg, &rra1, &rra2, &rdec1, &rdec2, &wrap, verbose);
    if (wrap) {
	rra2a = rra2;
	rra2 = 360.0;
	}
    else {
	rra2a = 0.0;
	}

    /* If searching through RA = 0:00, split search in two */
    for (iw = 0; iw <= wrap; iw++) {

	/* Find Tycho 2 Star Catalog regions in which to search */
	nreg = ty2reg (rra1,rra2,rdec1,rdec2,nrmax,regnum,rlist,nlist,verbose);
	if (nreg <= 0) {
	    fprintf (stderr,"TY2BIN:  no Tycho 2 region for %.2f-%.2f %.2f %.2f\n",
		     rra1, rra2, rdec1, rdec2);
	    rra1 = 0.0;
	    rra2 = rra2a;
	    continue;
	    }

	/* Loop through region list */
	for (ireg = 0; ireg < nreg; ireg++) {

	    /* Open catalog file for this region */
	    istar1 = rlist[ireg];
	    istar2 = istar1 + nlist[ireg];
	    /* if (verbose)
		fprintf (stderr,"TY2BIN: Searching stars %d through %d\n",
			istar1, istar2-1); */

	    /* Open file for this region of Tycho 2 catalog */
	    starcat = ty2open (rlist[ireg], nlist[ireg]);
	    if (starcat == NULL) {
		fprintf (stderr,"TY2BIN: File %s not found\n",inpath);
		return (0);
		}

	    /* Loop through catalog for this region */
	    for (istar = istar1; istar < istar2; istar++) {
		if ((ierr = ty2star (starcat, star, istar))) {
		    /* fprintf (stderr,"TY2BIN: Cannot read star %d\n", istar); */
		    if (ierr < 3)
			break;
		    else
			continue;
		    }

		/* ID number */
		num = star->num;
		ra = star->ra;
		dec = star->dec;

		/* Magnitude */
		magb = star->xmag[0];
		magv = star->xmag[1];
		mag = star->xmag[magsort];

		/* Check magnitude limits */
		pass = 1;
		if (mag1 != mag2 && (mag < mag1 || mag > mag2))
		    pass = 0;

		/* If this star was searched in first pass, skip it */
		if (iw > 0 && ra > rra2)
		    pass = 0;

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
			fprintf (stderr,"TY2BIN: %11.5f: %9.5f %9.5f %s",
				 num,ra,dec,cstr);
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

		/* Log operation
		jstar++;
		if (nlog > 0 && istar%nlog == 0)
		    fprintf (stderr,"TY2BIN: %5d / %5d / %5d sources\r",
			     nstar,jstar,starcat->nstars);

		   End of star loop */
		}

	    ntot = ntot + starcat->nstars;
	    if (nlog > 0)
		fprintf (stderr,"TY2BIN: %4d / %4d: %5d / %5d  / %5d sources from region %4d    \r",
		 	 ireg+1,nreg,nstar,jstar,starcat->nstars,regnum[ireg]);

	    /* Close region input file */
	    ty2close (starcat);
	    }
	rra1 = 0.0;
	rra2 = rra2a;
	}

/* close output file and summarize transfer */
    if (nlog > 0) {
	if (nreg > 1)
	    fprintf (stderr,"\nTY2BIN: %d regions: %d / %d found\n",nreg,nstar,ntot);
	else
	    fprintf (stderr,"\nTY2BIN: 1 region: %d / %d found\n",nstar,ntot);
	}
    return (nstar);
}


/* Tycho 2 region index for ty2regn() and ty2reg() */

/* First region in each declination zone */
static int treg1[24]={9490,9346,9134,8840,8464,8022,7523,6989,6412,5838,5260,4663,
	       1,594,1178,1729,2259,2781,3246,3652,4014,4294,4492,4615};
 
/* Last region in each declination zone */
static int treg2[24]={9537,9489,9345,9133,8839,8463,8021,7522,6988,6411,5837,5259,
	       593,1177,1728,2258,2780,3245,3651,4013,4293,4491,4614,4662};

static int indnchar = 0;	/* Number of characters per line in table */

/* TY2REGN -- read the range of stars in a region from the Tycho 2 Catalog
 * index table.
 */

static int
ty2regn (region, star1, star2, verbose)

int	region;		/* Region to find */
int	*star1;		/* First star number in region (returned)*/
int	*star2;		/* Last star number in region (returned)*/
int	verbose;	/* 1 for diagnostics */

{
    char *tabpath;	/* Pathname for regions table */
    char *buffer;	/* Buffer to hold index table */
    char *line;
    char *str;
    char lf=(char)10;
    int deczone;
    int lpath;

    *star1 = 0;
    *star2 = 0;

/* Find declination zone(s) in which this region exists */
    for (deczone = 0; deczone < 24; deczone++) {
	if (region >= treg1[deczone] && region <= treg2[deczone])
	    break;
	}
    if (deczone > 24)
	return (0);

/* Set path to Tycho 2 Catalog CDROM */
    if ((str = getenv("TY2_PATH")) != NULL ) {
	lpath = strlen (str) + 16;
	tabpath = (char *) malloc (lpath);
	strcpy (tabpath, str);
	}
    else {
	lpath = strlen (ty2cd) + 16;
	tabpath = (char *) malloc (lpath);
	strcpy (tabpath, ty2cd);
	}

/* Set pathname for index table file */
    strcat (tabpath,"/data/index.dat");

/* Read the index table */
    if ((buffer = getfilebuff (tabpath)) == NULL) {
	fprintf (stderr,"TY2REG:  error reading region table %s\n",tabpath);
	return (0);
	}

/* Figure out whether index file has LF or CRLF at end of lines */
    if (buffer[42] == lf)
	indnchar = 43;
    else
	indnchar = 44;

/* Read first star from regionth line of region table */
    line = buffer + ((region - 1) * indnchar);
    *star1 = atoi (line);

/* Read last star + 1 from region+1th line of region table */
    *star2 = atoi (line+indnchar);
    free (buffer);
    free (tabpath);
    return (1);
}


/* TY2REG -- search the Tycho 2 Catalog index table for fields
 * in the specified range of coordinates and magnitudes.
 * Build lists containing the first star and number of stars for each range.
 */

static int
ty2reg (ra1, ra2, dec1, dec2, nrmax, regnum, rstar, nstar, verbose)

double	ra1, ra2;	/* Right ascension limits in degrees */
double	dec1, dec2; 	/* Declination limits in degrees */
int	nrmax;		/* Maximum number of regions to find */
int	*regnum;	/* Region numbers (returned)*/
int	*rstar;		/* Region first star numbers (returned)*/
int	*nstar;		/* Region numbers of stars (returned)*/
int	verbose;	/* 1 for diagnostics */

{
    int nrgn;		/* Number of regions found (returned) */
    char *tabpath;	/* Pathname for regions table */
    char *buffer;	/* Buffer to hold index table */
    char *line;
    char *str;
    char lf=(char)10;
    int nwrap;		/* 1 if 0h included in RA span*/
    int iwrap;
    int num1, num2;
    int irow,iz1,iz2,jr1,jr2,i;
    int ir1 = 0;
    int ir2 = 0;
    int nsrch,nsrch1;
    double ralow, rahi;
    double declow, dechi, decmin, decmax;

    for (i = 0; i < nrmax; i++) {
	rstar[i] = 0;
	nstar[i] = 0;
	}
    nrgn = 0;

/* Set path to Tycho 2 Catalog CDROM */
    if ((str = getenv("TY2_PATH")) != NULL ) {
	tabpath = (char *) malloc (strlen (str) + 16);
	strcpy (tabpath, str);
	}
    else {
	tabpath = (char *) malloc (strlen (ty2cd) + 16);
	strcpy (tabpath, ty2cd);
	}

/* Set pathname for index table file */
    strcat (tabpath,"/data/index.dat");

/* Read the index table */
    if ((buffer = getfilebuff (tabpath)) == NULL) {
	fprintf (stderr,"TY2REG:  error reading region table %s\n",tabpath);
	return (0);
	}

/* Figure out whether index file has LF or CRLF at end of lines */
    if (buffer[42] == lf)
	indnchar = 43;
    else
	indnchar = 44;

/* Find region range to search based on declination */
    iz1 = ty2zone (dec1);
    iz2 = ty2zone (dec2);
    jr1 = 0;
    jr2 = 0;
    nwrap = 1;

/* Search in only one region */
    if (iz1 == iz2) {
	ir1 = treg1[iz1];
	ir2 = treg2[iz1];
	}
/* Search region in northern hemisphere */
    if (dec1 >= 0 && dec2 >= 0) {
	if (dec1 < dec2) {
	    ir1 = treg1[iz1];
	    ir2 = treg2[iz2];
	    }
	else {
	    ir1 = treg1[iz2];
	    ir2 = treg2[iz1];
	    }
	}

/* Search region in southern hemisphere with multiple regions */
    else if (dec1 < 0 && dec2 < 0) {
	if (dec1 < dec2) {
	    ir1 = treg1[iz2];
	    ir2 = treg2[iz1];
	    }
	else {
	    ir1 = treg1[iz1];
	    ir2 = treg2[iz2];
	    }
	}

/* Search region spans equator */
    else if (dec1 < 0 && dec2 >= 0) {
	nwrap = 2;

	/* southern part */
	jr1 = treg1[11];
	jr2 = treg2[iz1];

	/* northern part */
	ir1 = treg1[12];
	ir2 = treg2[iz2];
	}

    nsrch = ir2 - ir1 + 1;
    if (verbose)
	fprintf (stderr,"TY2REG: searching %d regions: %d - %d\n",nsrch,ir1,ir2);
    if (jr1 > 0) {
	nsrch1 = jr2 - jr1 + 1;
	if (verbose)
	    fprintf (stderr,"TY2REG: searching %d regions: %d - %d\n",nsrch1,jr1,jr2);
	}
    if (verbose)
	fprintf(stderr,"TY2REG: RA: %.5f - %.5f, Dec: %.5f - %.5f\n",ra1,ra2,dec1,dec2);

    nrgn = 0;

    for (iwrap = 0; iwrap < nwrap; iwrap++) {

	for (irow = ir1 - 1; irow < ir2; irow++) {

	/* Read next line of region table */
	    line = buffer + (irow * indnchar);

	/* Declination range of the gs region */
	/* note:  southern dechi and declow are reversed */
	    num1 = atoi (line);
	    num2 = atoi (line+indnchar);
	    dechi = atof (line + 29);
	    declow = atof (line + 36);
	    if (dechi > declow) {
		decmin = declow - 0.1;
		decmax = dechi + 0.1;
		}
	    else {
		decmax = declow + 0.1;
		decmin = dechi - 0.1;
		}

	    if (decmax >= dec1 && decmin <= dec2) {

	    /* Right ascension range of the Guide Star Catalog region */
		ralow = atof (line + 15) - 0.1;
		if (ralow <= 0.0) ralow = 0.0;
		rahi = atof (line + 22) + 0.1;
		if (rahi > 360.0) rahi = 360.0;
		if (rahi <= 0.0) rahi = 360.0;

	    /* Check RA if 0h RA not between region RA limits */
		if (ra1 < ra2) {

		    /* Add this region to list, if there is space */
		    if (ralow <= ra2 && rahi >= ra1) {
			/* if (verbose)
			    fprintf (stderr,"TY2REG: Region %d added to search\n",irow);
			    */

			if (nrgn < nrmax) {
			    regnum[nrgn] = irow;
			    rstar[nrgn] = num1;
			    nstar[nrgn] = num2 - num1;
			    nrgn = nrgn + 1;
			    }
			}
		    }

	    /* Check RA if 0h RA is between region RA limits */
		else {
		    if (ralow > rahi) rahi = rahi + 360.0;
		    if (ralow <= ra2 || rahi >= ra1) {

		    /* Add this region to list, if there is space */
			/* if (verbose)
			    fprintf (stderr,"TY2REG: Region %d added to search\n", irow);
			    */

			if (nrgn < nrmax) {
			    regnum[nrgn] = irow;
			    rstar[nrgn] = num1;
			    nstar[nrgn] = num2 - num1;
			    nrgn = nrgn + 1;
			    }
			}
		    }
		}
	    }

/* Handle wrap-around through the equator */
	ir1 = jr1;
	ir2 = jr2;
	jr1 = 0;
	jr2 = 0;
	}

    free (buffer);
    return (nrgn);
}

 
 
/*  TY2ZONE -- find the zone number where a declination can be found */
 
static int
ty2zone (dec)
 
double dec;		/* declination in degrees */
 
{
int zone;		/* gsc zone (returned) */
double  zonesize;
int ndeczones = 12;	/* number of declination zones per hemisphere */
double zdec = dec + 90.0;
 
/* width of declination zones */
    zonesize = 90.0 / ndeczones;
 
    zone = (int) (zdec / zonesize);
    if (zone < 0)
	zone = 0;
    if (zone > 23)
	zone = 23;
 
    return (zone);
}



/* TY2OPEN -- Open Tycho 2 catalog file, returning number of entries */

struct StarCat *
ty2open (nstar, nread)

int	nstar;	/* Number of first star to read */
int	nread;	/* Number of star entries to read */

{
    FILE *fcat;
    struct StarCat *sc;
    int lfile, lpath;
    int lread, lskip, nr;
    char *str;
    char *ty2file;
    char *ty2path;	/* Full pathname for catalog file */

    /* Set path to Tycho 2 Catalog CDROM */
    if ((str = getenv("TY2_PATH")) != NULL ) {
	lpath = strlen(str) + 18;
	ty2path = (char *) malloc (lpath);
	strcpy (ty2path, str);
	}
    else {
	lpath = strlen(ty2cd) + 18;
	ty2path = (char *) malloc (lpath);
	strcpy (ty2path, ty2cd);
	}

    /* Set pathname for catalog file */
    strcat (ty2path, "/data/catalog.dat");

    /* Find length of Tycho 2 catalog file */
    lfile = ty2size (ty2path);

    /* Check for existence of catalog */
    if (lfile < 2) {
	fprintf (stderr,"TY2OPEN: Binary catalog %s has no entries\n",ty2path);
	free (ty2path);
	return (NULL);
	}

    /* Open Tycho 2 file */
    if (!(fcat = fopen (ty2path, "r"))) {
	fprintf (stderr,"TY2OPEN: Tycho 2 file %s cannot be read\n",ty2path);
	free (ty2path);
	return (0);
	}

    /* Set Tycho 2 catalog header information */
    sc = (struct StarCat *) calloc (1, sizeof (struct StarCat));
    sc->byteswapped = 0;

    if (indnchar == 44)
	sc->nbent = 208;
    else
	sc->nbent = 207;
    sc->nstars = lfile / sc->nbent;

    /* Separate filename from pathname and save in structure */
    ty2file = strrchr (ty2path,'/');
    if (ty2file)
	ty2file = ty2file + 1;
    else
	ty2file = ty2path;
    if (strlen (ty2file) < 24)
	strcpy (sc->isfil, ty2file);
    else
	strncpy (sc->isfil, ty2file, 23);

    /* Set other catalog information in structure */
    sc->inform = 'J';
    sc->coorsys = WCS_J2000;
    sc->epoch = 2000.0;
    sc->equinox = 2000.0;
    sc->ifcat = fcat;
    sc->sptype = 2;

    /* Tycho 2 stars are not RA-sorted within regions */
    sc->rasorted = 0;

    /* Read part of catalog into a buffer */
    lread = nread * sc->nbent;
    lskip = (nstar - 1) * sc->nbent;
    sc->catdata = NULL;
    if ((sc->catdata = calloc (1, lread+1)) != NULL) {
	fseek (fcat, lskip, 0);
	nr = fread (sc->catdata, 1, lread, fcat);
	if (nr < lread) {
	    fprintf (stderr,"TY2OPEN: Read %d / %d bytes\n", nr, lread);
            ty2close (sc);
	    free (ty2path);
            return (NULL);
            }
	sc->catlast = sc->catdata + lread;
	}
    else {
	fprintf (stderr,"TY2OPEN: Cannot allocate %d-byte buffer.\n", lread);
        ty2close (sc);
	free (ty2path);
	return (NULL);
	}
    sc->istar = nstar;
    free (ty2path);
    return (sc);
}


void
ty2close (sc)
struct StarCat *sc;	/* Star catalog descriptor */
{
    fclose (sc->ifcat);
    if (sc->catdata != NULL)
	free (sc->catdata);
    free (sc);
    return;
}


/* TY2STAR -- Get Tycho 2 catalog entry for one star;
              return 0 if successful */

static int
ty2star (sc, st, istar)

struct StarCat *sc;	/* Star catalog descriptor */
struct Star *st;	/* Current star entry */
int istar;	/* Star sequence number in Tycho 2 catalog region file */
{
    char *line;
    double regnum, starnum, multnum;

    /* Drop out if catalog pointer is not set */
    if (sc == NULL) {
	fprintf (stderr, "TY2STAR:  Catalog pointer not set\n");
	return (1);
	}

    /* Drop out if catalog is not open */
    if (sc->ifcat == NULL) {
	fprintf (stderr, "TY2STAR:  Catalog is not open\n");
	return (2);
	}

    /* Drop out if star number is too large */
    if (istar > sc->nstars) {
	fprintf (stderr, "TY2STAR:  %d  > %d is not in catalog\n",
		 istar, sc->nstars);
	return (3);
	}

    /* Move buffer pointer to start of correct star entry */
    if (istar > 0) {
	line = sc->catdata + ((istar - sc->istar) * sc->nbent);
	if (line >= sc->catlast) {
	    fprintf (stderr, "TY2STAR:  star %d past buffer\n", istar);
	    return (4);
	    }
	}
    else {
	line = sc->catdata;
	}

    /* Read catalog entry */
    if (sc->nbent > sc->catlast-line) {
	fprintf (stderr, "TY2STAR:  %d / %d bytes read for star %d\n",
		 sc->catlast - line, sc->nbent, istar);
	return (5);
	}

    regnum = atof (line);
    starnum = atof (line+5);
    multnum = atof (line+11);
    st->num = regnum + (0.0001 * starnum) + (0.00001 * multnum);

    if (line[13] == 'X') {
	fprintf (stderr, "TY2STAR:  No position for star %010.5f\n", st->num);
	return (6);
	}

    /* Read position in degrees */
    st->ra = atof (line+15);
    st->dec = atof (line+28);

    /* Read proper motion and convert it to degrees (of RA and Dec) per year */
    st->rapm = (atof (line+41) / 3600000.0) / cosdeg (st->dec);
    st->decpm = atof (line+49) / 3600000.0;

    /* Set B magnitude and error */
    st->xmag[0] = atof (line+110);
    st->xmag[2] = atof (line+117);

    /* Set V magnitude and error */
    st->xmag[1] = atof (line+123);
    st->xmag[3] = atof (line+130);

    /* Set main sequence spectral type
    st->isp[0] = (char)0;
    st->isp[1] = (char)0;
    bv2sp (NULL, st->xmag[1], st->xmag[0], st->isp); */

    return (0);
}

/* TY2SIZE -- return size of Tycho 2 catalog file in bytes */

static int
ty2size (filename)

char	*filename;	/* Name of file for which to find size */
{
    FILE *diskfile;
    long filesize;

    /* Open file */
    if ((diskfile = fopen (filename, "r")) == NULL)
	return (-1);

    /* Move to end of the file */
    if (fseek (diskfile, 0, 2) == 0)

	/* Position is the size of the file */
	filesize = ftell (diskfile);

    else
	filesize = -1;

    fclose (diskfile);

    return (filesize);
}


/* Jun  2 2000	New program, based on actread.c and gscread.c
 * Jun 13 2000	Correctly order magnitudes: 0=V, 1=B
 * Jun 26 2000	Add coordinate system to SearchLim() arguments
 * Sep 25 2000	Set sc->sptype to 2 to indicate presence of spectral type
 * Nov 29 2000	Add option to read catalog using HTTP
 * Dec 11 2000	Accept catalog search engine URL in ty2cd[]
 *
 * Jan 11 2001	All printing goes to stderr
 * Jun 14 2001	Drop spectral type approximation
 * Jun 15 2001	In ty2reg(), add 0.1 to tabulated region limits
 * Jun 19 2001	When no region found, print RA and Dec limits used
 * Jun 27 2001	Allocate gdist only if needed
 * Sep 11 2001	Change to single magnitude argeument
 * Sep 11 2001	Add sort magnitude argument to uacread()
 * Nov 20 2001	Change cos(degrad()) to cosdeg()
 * Dec  3 2001	Change default directory to /data/astrocat/tycho2
 *
 * Apr  3 2002	Fix bug so magnitude filtering is actually done (all passed)
 * Apr  8 2002	Fix uninitialized variable
 * Apr 10 2002	Separate catalog and output sort mags (in:vb out: bv)
 * Oct  3 2002	Print stars as found in ty2read() if nstarmax < 1
 *
 * Feb  3 2003	Include math.h because of fabs()
 * Feb 27 2003	Add 60 arcsec/century to margins of search box to get moving stars
 * Mar 11 2003	Fix position limit testing
 * Apr  3 2003	Drop unused variables after lint
 * Apr 14 2003	Explicitly get revision date if nstarmax < 1
 * Jun  2 2003	Print proper motion as mas/year
 * Aug  8 2003	Increase MAXREG from 100 to 1000
 * Aug 22 2003	Add radi argument for inner edge of search annulus
 * Sep 26 2003	Add ty2bin() to fill an image with sources
 * Sep 29 2003	Rewrite zone computation to deal with +-90 correctly
 * Oct  1 2003	Use wcs2pix() to decide whether to accept position in ty2bin()
 * Oct  6 2003	Update ty2read() and ty2bin() for improved RefLim()
 * Nov 18 2003	Fix bugs in ty2bin()
 * Dec  1 2003	Add missing tab to n=-1 header
 *
 * Apr 30 2004	Allow either LF or CRLF at end of lines in index and catalog
 *
 * May 18 2005	Add magnitude errors
 * Aug  5 2005	Make magnitude errors an option if refcat is TYCHO2E
 *
 * Apr  3 2006	Add refcat definition to ty2rnum()
 * Jun 20 2006	Initialize uninitialized variables
 * Oct  5 2006	Fix order of magnitudes to Bt-Vt from Vt-Bt
 * Nov 16 2006	Fix binning
 *
 * Jan 10 2007	Add dradi argument to webread() call
 * Jan 10 2007	Add match=1 argument to webrnum()
 * Jan 10 2007	Rewrite web access in ty2rnum() to reduce code
 * Jul  6 2007	Skip stars with no positions (=unfound Guide Stars?)
 * Jul  6 2007	Skip stars with entry read errors; stop if catalog problem
 * Jul  6 2007	Print read errors in ty2star() only
 * Jun  9 2007	Fix bug so that sequential catalog entry reading works
 */
