/*** File libwcs/actread.c
 *** January 10, 2007
 *** By Doug Mink, dmink@cfa.harvard.edu
 *** Copyright (C) 1999-2007
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
#include <stdlib.h>
#include <math.h>
#include <string.h>
#include <stdio.h>
#include "fitsfile.h"
#include "wcs.h"
#include "wcscat.h"

/* pathname of ACT CDROM or catalog search engine URL */
char actcd[64]="/data/act";

#define MAXREG 100

static double *gdist;	/* Array of distances to stars */
static int ndist = 0;

static int actreg();
struct StarCat *actopen();
void actclose();
static int actstar();
static int actsize();
static int actsra();

/* ACTREAD -- Read USNO ACT Star Catalog stars from CDROM */

int
actread (cra,cdec,dra,ddec,drad,dradi,distsort,sysout,eqout,epout,mag1,mag2,
	 sortmag,nstarmax,gnum,gra,gdec,gpra,gpdec,gmag,gtype,nlog)

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
int	sortmag;	/* Magnitude by which to sort (1 to nmag) */
int	nstarmax;	/* Maximum number of stars to be returned */
double	*gnum;		/* Array of Guide Star numbers (returned) */
double	*gra;		/* Array of right ascensions (returned) */
double	*gdec;		/* Array of declinations (returned) */
double  *gpra;          /* Array of right ascension proper motions (returned) */
double  *gpdec;         /* Array of declination proper motions (returned) */
double	**gmag;		/* Array of visual magnitudes (returned) */
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
    int nreg = 0;	/* Number of ACT regions in search */
    int rlist[MAXREG];	/* List of input region files */
    int sysref=WCS_J2000;	/* Catalog coordinate system */
    double eqref=2000.0;	/* Catalog equinox */
    double epref=2000.0;	/* Catalog epoch */
    double secmarg = 60.0;	/* Arcsec/century margin for proper motion */
    struct StarCat *starcat;
    struct Star *star;
    int verbose;
    int wrap;
    int pass;
    int magsort;
    int rnum, ireg;
    int jstar, iw;
    int nrmax,nstar,i, ntot;
    int istar, istar1, istar2;
    double num, ra, dec, rapm, decpm, mag, magb, magv;
    double rra1, rra2, rra2a, rdec1, rdec2;
    double rdist, ddist;
    char *str;
    char cstr[32], decstr[32], rastr[32];
    int nbytes;

    ntot = 0;
    if (nlog == 1)
	verbose = 1;
    else
	verbose = 0;

    /* Set path to ACT Catalog */
    if ((str = getenv("ACT_PATH")) == NULL )
	str = actcd;

    /* If pathname is a URL, search and return */
    if (!strncmp (str, "http:",5)) {
	return (webread (str,"act",distsort,cra,cdec,dra,ddec,drad,dradi,
			 sysout,eqout,epout,mag1,mag2,sortmag,nstarmax,
			 gnum,gra,gdec,gpra,gpdec,gmag,gtype,nlog));
	}

    if (sortmag > 0 && sortmag < 3)
	magsort = sortmag - 1;
    else 
	magsort = 1;

    wcscstr (cstr, sysout, eqout, epout);

    SearchLim (cra,cdec,dra,ddec,sysout,&ra1,&ra2,&dec1,&dec2,verbose);

/* make mag1 always the smallest magnitude */
    if (mag2 < mag1) {
	mag = mag2;
	mag2 = mag1;
	mag1 = mag;
	}

    /* Allocate table for distances of stars from search center */
    if (nstarmax > 10)
	nbytes = nstarmax * sizeof (double);
    else
	nbytes = 10 * sizeof (double);
    
    if (nstarmax > ndist) {
	if (ndist > 0)
	    free ((void *) gdist);
	gdist = (double *) malloc (nbytes);
	if (gdist == NULL) {
	    fprintf (stderr,"ACTREAD:  cannot allocate separation array\n");
	    return (0);
	    }
	ndist = nstarmax;
	}

    /* Allocate catalog entry buffer */
    star = (struct Star *) calloc (1, sizeof (struct Star));
    star->num = 0.0;

    nstar = 0;
    jstar = 0;

    rra1 = ra1;
    rra2 = ra2;
    rdec1 = dec1;
    rdec2 = dec2;
    RefLim (cra,cdec,dra,ddec,sysout,sysref,eqout,eqref,epout,epref,secmarg,
	    &rra1, &rra2, &rdec1, &rdec2, &wrap, verbose);
    if (wrap) {
	rra2a = rra2;
	rra2 = 360.0;
	}
    else
	rra2a = 0.0;
    nrmax = MAXREG;

    /* Write header if printing star entries as found */
    if (nstarmax < 1) {
	char *revmessage;
	revmessage = getrevmsg();
	printf ("catalog	ACT\n");
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
	    printf ("dramin	%.1f\n", dra*60.0 * cosdeg (cdec));
	    printf ("ddecmin	%.1f\n", ddec*60.0);
	    }
	printf ("radecsys	%s\n", cstr);
	printf ("equinox	%.3f\n", eqout);
	printf ("epoch	%.3f\n", epout);
	printf ("program	scat %s\n", revmessage);
	printf ("act_id    	ra          	dec         	");
	printf ("magb 	magv 	ura   	udec  	arcmin\n");
	printf ("----------	------------	------------	");
	printf ("-----	-----	------	------	------\n");
	}

    /* If searching through RA = 0:00, split search in two */
    for (iw = 0; iw <= wrap; iw++) {

	/* Find ACT Star Catalog regions in which to search */
	nreg = actreg (rra1,rra2,rdec1,rdec2,nrmax,rlist,verbose);
	if (nreg <= 0) {
	    fprintf (stderr,"ACTREAD:  no ACT regions found\n");
	    free ((void *)star);
	    return (0);
	    }

	/* Loop through region list */
	for (ireg = 0; ireg < nreg; ireg++) {

	    /* Open catalog file for this region */
	    starcat = actopen (rlist[ireg]);
	    rnum = rlist[ireg];

	    /* Set first and last stars to check */
	    istar1 = actsra (starcat, star, rra1);
	    istar2 = actsra (starcat, star, rra2);
	    if (verbose)
		fprintf (stderr,"ACTREAD: Searching stars %d.%d through %d.%d\n",
			rnum,istar1,rnum,istar2);

	    /* Loop through catalog for this region */
	    for (istar = istar1; istar <= istar2; istar++) {
		if (actstar (starcat, star, istar)) {
		    fprintf (stderr,"ACTREAD: Cannot read star %d\n", istar);
		    break;
		    }

		/* Magnitude */
		magv = star->xmag[0];
		magb = star->xmag[1];
		mag = star->xmag[magsort];

		/* Check magnitude limits */
		pass = 1;
		if (mag1 != mag2 && (mag < mag1 || mag > mag2))
		    pass = 0;

		/* Get position in output coordinate system */
		if (pass) {
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

		    /* ID number */
		    num = (double) rlist[ireg] + (star->num / 100000.0);

		    /* Write star position and magnitudes to stdout */
		    if (nstarmax < 1) {
			ra2str (rastr, 31, ra, 3);
			dec2str (decstr, 31, dec, 2);
			dist = wcsdist (cra,cdec,ra,dec) * 60.0;
			printf ("%010.5f	%s	%s", num,rastr,decstr);
			printf ("	%.2f	%.2f	%6.1f	%6.1f	%.2f\n",
				magb, magv,
				gpra[nstar] * 3600000.0 * cosdeg(dec),
				gpdec[nstar] * 3600000.0, dist / 60.0);
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
			gpra[farstar] = rapm;
			gpdec[farstar] = decpm;
			gmag[0][faintstar] = magb;
			gmag[1][faintstar] = magv;
			gdist[faintstar] = dist;
			faintmag = 0.0;

			/* Find new faintest star */
			for (i = 0; i < nstarmax; i++) {
			    if (gmag[magsort][i] > faintmag) {
				faintmag = gmag[magsort][i];
				faintstar = i;
				}
			    }
			}

		    nstar++;
		    if (nlog == 1)
			fprintf (stderr,"ACTREAD: %11.6f: %9.5f %9.5f %5.2f %5.2f\n",
				 num,ra,dec,magb,mag);

		    /* End of accepted star processing */
		    }

		/* Log operation */
		jstar++;
		if (nlog > 0 && istar%nlog == 0)
		    fprintf (stderr,"ACTREAD: %5d / %5d / %5d sources\r",
			     nstar,jstar,starcat->nstars);

		/* End of star loop */
		}

	    ntot = ntot + starcat->nstars;
	    if (nlog > 0)
		fprintf (stderr,"ACTREAD: %4d / %4d: %5d / %5d  / %5d sources from region %4d    \n",
		 	 ireg+1,nreg,nstar,jstar,starcat->nstars,rlist[ireg]);

	    /* Close region input file */
	    actclose (starcat);
	    }
	rra1 = 0.0;
	rra2 = rra2a;
	}

/* close output file and summarize transfer */
    if (nlog > 0) {
	if (nreg > 1)
	    fprintf (stderr,"ACTREAD: %d regions: %d / %d found\n",nreg,nstar,ntot);
	else
	    fprintf (stderr,"ACTREAD: 1 region: %d / %d found\n",nstar,ntot);
	if (nstar > nstarmax)
	    fprintf (stderr,"ACTREAD: %d stars found; only %d returned\n",
		     nstar,nstarmax);
	}
    free ((void *)star);
    return (nstar);
}

/* ACTRNUM -- Read HST Guide Star Catalog stars from CDROM */

int
actrnum (nstars,sysout,eqout,epout,
	 gnum,gra,gdec,gpra,gpdec,gmag,gtype,nlog)

int	nstars;		/* Number of stars to find */
int	sysout;		/* Search coordinate system */
double	eqout;		/* Search coordinate equinox */
double	epout;		/* Proper motion epoch (0.0 for no proper motion) */
double	*gnum;		/* Array of Guide Star numbers (returned) */
double	*gra;		/* Array of right ascensions (returned) */
double	*gdec;		/* Array of declinations (returned) */
double  *gpra;          /* Array of right ascension proper motions (returned) */
double  *gpdec;         /* Array of declination proper motions (returned) */
double	**gmag;		/* Array of V, B magnitudes (returned) */
int	*gtype;		/* Array of object types (returned) */
int	nlog;		/* 1 for diagnostics */
{
    int sysref;		/* Catalog coordinate system */
    double eqref;	/* Catalog equinox */
    double epref;	/* Catalog epoch */
    struct StarCat *starcat = NULL;
    struct Star *star;

    int rnum;
    int jstar;
    int istar, nstar, snum;
    double num, ra, dec, rapm, decpm, mag, magb;
    char *str;

    /* Set path to ACT Catalog */
    if ((str = getenv("ACT_PATH")) == NULL )
	str = actcd;

    /* If pathname is a URL, search and return */
    if (!strncmp (str, "http:",5)) {
	return (webrnum (str,"act",nstars, sysout,eqout,epout, 1,
			 gnum,gra,gdec,gpra,gpdec,gmag,gtype,nlog));
	}

    /* Allocate catalog entry buffer */
    star = (struct Star *) calloc (1, sizeof (struct Star));
    star->num = 0.0;

    nstar = 0;

/* Loop through star list */
    for (jstar = 0; jstar < nstars; jstar++) {
	rnum = (int) gnum[jstar];
	snum = (int) (((gnum[jstar] - (double)rnum) * 100000.0) + 0.01);

	/* Open file for this region of ACT catalog */
	starcat = actopen (rnum);
	if (starcat == NULL) {
	    free ((void*) star);
	    return (0);
	    }

	sysref = starcat->coorsys;
	eqref = starcat->equinox;
	epref = starcat->epoch;

	/* Find star in catalog */
	istar = snum;
	if (actstar (starcat, star, istar)) {
	    fprintf (stderr,"ACTRNUM: Cannot read star %d\n", istar);
	    gra[nstar] = 0.0;
	    gdec[nstar] = 0.0;
	    gmag[0][nstar] = 0.0;
	    gmag[1][nstar] = 0.0;
	    gtype[nstar] = 0;
	    continue;
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
	mag = star->xmag[0];
	magb = star->xmag[1];

	/* Save star position and magnitude in table */
	gra[nstar] = ra;
	gdec[nstar] = dec;
	gpra[nstar] = rapm;
	gpdec[nstar] = decpm;
	gmag[0][nstar] = magb;
	gmag[1][nstar] = mag;
	nstar++;
	if (nlog == 1)
	    fprintf (stderr,"ACTRNUM: %11.6f: %9.5f %9.5f %5.2f %5.2f \n",
		     num, ra, dec, magb, mag);

	/* End of star loop */
	}

/* Summarize search */
    if (nlog > 0)
	fprintf (stderr,"ACTRNUM: %d / %d found\n",nstar,nstars);

    if (starcat != NULL)
	actclose (starcat);
    free ((void*) star);
    return (nstar);
}


/* ACTBIN -- Fill FITS WCS image with USNO ACT Star Catalog stars */

int
actbin (wcs, header, image, mag1, mag2, sortmag, magscale, nlog)

struct WorldCoor *wcs;	/* World coordinate system for image */
char	*header;	/* FITS header for output image */
char	*image;		/* Output FITS image */
double	mag1,mag2;	/* Limiting magnitudes (none if equal) */
int	sortmag;	/* Magnitude by which to sort (1 to nmag) */
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
    int nreg = 0;	/* Number of ACT regions in search */
    int rlist[MAXREG];	/* List of input region files */
    int sysref=WCS_J2000;	/* Catalog coordinate system */
    double eqref=2000.0;	/* Catalog equinox */
    double epref=2000.0;	/* Catalog epoch */
    double secmarg = 60.0;	/* Arcsec/century margin for proper motion */
    struct StarCat *starcat;
    struct Star *star;
    int verbose;
    int wrap;
    int pass;
    int magsort;
    int rnum, ireg;
    int ix, iy;
    int jstar, iw;
    int nrmax,nstar, ntot;
    int istar, istar1, istar2;
    double num, ra, dec, rapm, decpm, mag, magb, magv;
    double rra1, rra2, rra2a, rdec1, rdec2;
    double rdist, ddist;
    char *str;
    char cstr[32];
    double xpix, ypix, flux;
    int offscl;
    int bitpix, w, h;   /* Image bits/pixel and pixel width and height */
    double logt = log(10.0);

    ntot = 0;
    if (nlog == 1)
	verbose = 1;
    else
	verbose = 0;

    /* Set path to ACT Catalog */
    str = getenv("ACT_PATH");

    if (sortmag > 0 && sortmag < 3)
	magsort = sortmag - 1;
    else 
	magsort = 1;

    /* Set image parameters */
    bitpix = 0;
    (void)hgeti4 (header, "BITPIX", &bitpix);
    w = 0;
    (void)hgeti4 (header, "NAXIS1", &w);
    h = 0;
    (void)hgeti4 (header, "NAXIS2", &h);

    /* Set catalog search limits from image WCS information */
    sysout = wcs->syswcs;
    eqout = wcs->equinox;
    epout = wcs->epoch;
    wcscstr (cstr, sysout, eqout, epout);
    wcssize (wcs, &cra, &cdec, &dra, &ddec);
    SearchLim (cra,cdec,dra,ddec,sysout,&ra1,&ra2,&dec1,&dec2,verbose);

    /* If RA range includes zero, split it in two */
    wrap = 0;
    if (ra1 > ra2)
	wrap = 1;
    else
	wrap = 0;

    /* make mag1 always the smallest magnitude */
    if (mag2 < mag1) {
	mag = mag2;
	mag2 = mag1;
	mag1 = mag;
	}

    /* Allocate catalog entry buffer */
    star = (struct Star *) calloc (1, sizeof (struct Star));
    star->num = 0.0;

    nstar = 0;
    jstar = 0;

    rra1 = ra1;
    rra2 = ra2;
    rdec1 = dec1;
    rdec2 = dec2;
    RefLim (cra,cdec,dra,ddec,sysout,sysref,eqout,eqref,epout,epref,secmarg,
	    &rra1, &rra2, &rdec1, &rdec2, &wrap, verbose);
    if (wrap) {
	rra2a = rra2;
	rra2 = 360.0;
	}
    else
	rra2a = 0.0;

    nrmax = MAXREG;

    /* If searching through RA = 0:00, split search in two */
    for (iw = 0; iw <= wrap; iw++) {

	/* Find ACT Star Catalog regions in which to search */
	nreg = actreg (rra1,rra2,rdec1,rdec2,nrmax,rlist,verbose);
	if (nreg <= 0) {
	    fprintf (stderr,"ACTBIN:  no ACT regions found\n");
	    free ((void *)star);
	    return (0);
	    }

	/* Loop through region list */
	for (ireg = 0; ireg < nreg; ireg++) {

	    /* Open catalog file for this region */
	    starcat = actopen (rlist[ireg]);
	    rnum = rlist[ireg];

	    /* Set first and last stars to check */
	    istar1 = actsra (starcat, star, rra1);
	    istar2 = actsra (starcat, star, rra2);
	    if (verbose)
		fprintf (stderr,"ACTBIN: Searching stars %d.%d through %d.%d\n",
			rnum,istar1,rnum,istar2);

	    /* Loop through catalog for this region */
	    for (istar = istar1; istar <= istar2; istar++) {
		if (actstar (starcat, star, istar)) {
		    fprintf (stderr,"ACTBIN: Cannot read star %d\n", istar);
		    break;
		    }

		/* ID number */
		num = star->num;

		/* Magnitude */
		magv = star->xmag[0];
		magb = star->xmag[1];
		mag = star->xmag[magsort];

		/* Check magnitude limits */
		pass = 1;
		if (mag1 != mag2 && (mag < mag1 || mag > mag2))
		    pass = 0;

		/* Get position in output coordinate system */
		if (pass) {
		    rapm = star->rapm;
		    decpm = star->decpm;
		    ra = star->ra;
		    dec = star->dec;
		    wcsconp (sysref, sysout, eqref, eqout, epref, epout,
			     &ra, &dec, &rapm, &decpm);

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
			fprintf (stderr,"TABBIN: %11.6f: %9.5f %9.5f %s",
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

		/* Log operation */
		jstar++;
		if (nlog > 0 && istar%nlog == 0)
		    fprintf (stderr,"ACTBIN: %5d / %5d / %5d sources\r",
			     nstar,jstar,starcat->nstars);

		/* End of star loop */
		}

	    ntot = ntot + starcat->nstars;
	    if (nlog > 0)
		fprintf (stderr,"ACTBIN: %4d / %4d: %5d / %5d  / %5d sources from region %4d    \n",
		 	 ireg+1,nreg,nstar,jstar,starcat->nstars,rlist[ireg]);

	    /* Close region input file */
	    actclose (starcat);
	    }
	rra1 = 0.0;
	rra2 = rra2a;
	}

/* close output file and summarize transfer */
    if (nlog > 0) {
	if (nreg > 1)
	    fprintf (stderr,"ACTBIN: %d regions: %d / %d found\n",nreg,nstar,ntot);
	else
	    fprintf (stderr,"ACTBIN: 1 region: %d / %d found\n",nstar,ntot);
	}
    free ((void *)star);
    return (nstar);
}


/* ACTREG -- from RA and Dec ranges, figure out which ACT files to search
 * Build a list containing the numeric part of the CDROM file names.
 */

static int regions[48]={   0,  30, 100, 130, 200, 230, 300, 330, 400, 430,
			 500, 530, 600, 630, 700, 730, 800, 830, 900, 930,
			1000,1030,1100,1130,1200,1230,1300,1330,1400,1430,
			1500,1530,1600,1630,1700,1730,1800,1830,1900,1930,
			2000,2030,2100,2130,2200,2230,2300,2330};
static double reghour[49]={ 0.0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5,
			    5.0, 5.5, 6.0, 6.5, 7.0, 7.5, 8.0, 8.5, 9.0, 9.5,
			   10.0,10.5,11.0,11.5,12.0,12.5,13.0,13.5,14.0,14.5,
			   15.0,15.5,16.0,16.5,17.0,17.5,18.0,18.5,19.0,19.5,
			   20.0,20.5,21.0,21.5,22.0,22.5,23.0,23.5,24.0};

static int
actreg (ra1, ra2, dec1, dec2, nrmax, rgns, verbose)

double	ra1, ra2;	/* Right ascension limits in degrees */
double	dec1, dec2; 	/* Declination limits in degrees */
int	nrmax;		/* Maximum number of regions to find */
int	*rgns;		/* Region numbers (returned)*/
int	verbose;	/* 1 for diagnostics */

{
    int nsrch;		/* Number of regions found (returned) */

    int i, ir, irx;
    int ir1 = 0;
    int ir2 = 0;

    /* Zero out regions to be searched */
    for (i = 0; i < nrmax; i++)
	rgns[i] = 0;

    /* Find region range to search based on declination */

    /* Find RA regions to search */
    ra1 = ra1 / 15.0;
    ra2 = ra2 / 15.0;
    irx = 0;

    /* Find first region to search */
    for (ir = 1; ir < 49; ir++) {
	if (ra1 >= reghour[ir-1] && ra1 <= reghour[ir]) {
	    ir1 = ir - 1;
	    break;
	    }
	}

    /* Find last region to search */
    for (ir = 1; ir < 49; ir++) {
	if (ra2 >= reghour[ir-1] && ra2 <= reghour[ir]) {
	    ir2 = ir - 1;
	    break;
	    }
	}

    if (ir2 >= ir1) {
	for (ir = ir1; ir <= ir2; ir++) {
	    if (irx < nrmax)
		rgns[irx++] = regions[ir];
	    }
	}
    else if (ir2 < ir1) {
	for (ir = ir1; ir < 48; ir++) {
	    if (irx < nrmax)
		rgns[irx++] = regions[ir];
	    }
	for (ir = 0; ir <= ir2; ir++) {
	    if (irx < nrmax)
		rgns[irx++] = regions[ir];
	    }
	}
    nsrch = irx;

    if (verbose) {
	fprintf (stderr,"ACTREG: RA: %.5f - %.5f, Dec: %.5f - %.5f\n",
		 ra1,ra2,dec1,dec2);
	fprintf (stderr,"ACTREG: searching %d regions:",nsrch);
	for (ir = 0; ir < nsrch; ir++)
	    fprintf (stderr," %04d",rgns[ir]);
	fprintf (stderr,"\n");
	}
    return (nsrch);
}


/* ACTOPEN -- Open ACT catalog region file, returning number of entries */

struct StarCat *
actopen (regnum)

int regnum;	/* ACT Catalog region number */

{
    FILE *fcat;
    struct StarCat *sc;
    int lfile, lpath;
    char *actfile;
    char *path;		/* Full pathname for catalog file */
    char *cdpath;

    /* Set the pathname using the appropriate ACT CDROM directory */
    if ((cdpath = getenv("ACT_PATH")) == NULL )
	cdpath = actcd;
    lpath = strlen (cdpath) + 32;
    path = (char *) calloc (lpath, 1);

    /* Declination zoned regions */
    if (regnum > 0 && regnum < 5)
	sprintf (path,"%s/data2/act%1d.dat", cdpath, regnum);

    /* Right ascension zoned regions */
    else
	sprintf (path,"%s/data1/act%04d.dat", cdpath, regnum);

    /* Find length of ACT catalog region file */
    lfile = actsize (path);

    /* Check for existence of catalog */
    if (lfile < 2) {
	fprintf (stderr,"ACTOPEN: Binary catalog %s has no entries\n", path);
	free (path);
	return (0);
	}

    /* Open ACT region file */
    if (!(fcat = fopen (path, "r"))) {
	fprintf (stderr,"ACTOPEN: ACT region file %s cannot be read\n",path);
	free (path);
	return (0);
	}

    /* Set ACT catalog header information */
    sc = (struct StarCat *) calloc (1, sizeof (struct StarCat));
    sc->byteswapped = 0;

    sc->nbent = 161;
    sc->nstars = lfile / sc->nbent;

    /* Separate filename from pathname and save in structure */
    actfile = strrchr (path,'/');
    if (actfile)
	actfile = actfile + 1;
    else
	actfile = path;
    if (strlen (actfile) < 24)
	strcpy (sc->isfil, actfile);
    else
	strncpy (sc->isfil, actfile, 23);

    /* Set other catalog information in structure */
    sc->inform = 'J';
    sc->coorsys = WCS_J2000;
    sc->epoch = 2000.0;
    sc->equinox = 2000.0;
    sc->ifcat = fcat;
    sc->sptype = 2;

    /* ACT region files are all RA-sorted */
    sc->rasorted = 1;

    return (sc);
}


void
actclose (sc)
struct StarCat *sc;	/* Star catalog descriptor */
{
    fclose (sc->ifcat);
    free ((void *)sc);
    return;
}


/* ACTSRA -- Find star closest to given RA in ACT catalog file */

static int
actsra (sc, st, dra)

struct StarCat *sc;	/* Star catalog descriptor */
struct Star *st;	/* Current star entry */
double	dra;		/* Right ascension in degrees */

{
    char rastr[32], raxstr[32], ramins[32], ramaxs[32];
    int istar0, istarx, nrep, ismin, ismax;
    double rax, ramin, ramax;
    int verbose = 0;

    /* Keep RA between 0 and 360 degrees */
    if (dra > 360.0)
	rax = dra - 360.0;
    else
	rax = dra;

    ismin = 1;
    if (actstar (sc, st, ismin)) {
	fprintf (stderr,"ACTSRA: Cannot read star %d\n", ismin);
	return (0);
	}
    else
	ramin = st->ra;

    ismax = sc->nstars;
    if (actstar (sc, st, ismax)) {
	fprintf (stderr,"ACTSRA: Cannot read star %d\n", ismax);
	return (0);
	}
    else
	ramax = st->ra;

    istarx = sc->nstars / 2;

    for (nrep = 0; nrep < 32; nrep++) {
	if (actstar (sc, st, istarx)) {
	    fprintf (stderr,"ACTSRA: Cannot read star %d\n", istarx);
            return (0);
	    }

	/* Find next catalog number to read */
	if (st->ra < rax) {
	    ismin = istarx;
	    ramin = st->ra;
	    istar0 = istarx;
	    if (ismax - istarx > 1)
		istarx = istarx + (ismax - istarx) / 2;
	    else if (ismax - istarx > 0)
		istarx = istarx + 1;
	    }
	else if (st->ra > rax) {
	    ismax = istarx;
	    ramax = st->ra;
	    istar0 = istarx;
	    if (istarx - ismin > 1)
		istarx = istarx - ((istarx - ismin) / 2);
	    else if (istarx - ismin > 0)
		istarx = istarx - 1;
	    }
	else
	    break;

	if (verbose) {
	    ra2str (rastr, 16, st->ra, 3);
	    ra2str (raxstr, 16, rax, 3);
	    ra2str (ramins, 16, ramin, 3);
	    ra2str (ramaxs, 16, ramax, 3);
	    fprintf (stderr,"%9d: %s -> %s  %9d: %s  %9d: %s\n",
		    istarx, rastr, raxstr, ismin,ramins,ismax,ramaxs);
	    }
	if (istarx == istar0)
	    break;
	}

    /* Make sure final star is real */
    if (actstar (sc, st, istarx)) {
	fprintf (stderr,"ACTSRA: Cannot read star %d\n", istarx);
        return (0);
	}
    else
	return (istarx);
}


/* ACTSTAR -- Get ACT catalog entry for one star;
              return 0 if successful */

static int
actstar (sc, st, istar)

struct StarCat *sc;	/* Star catalog descriptor */
struct Star *st;	/* Current star entry */
int istar;	/* Star sequence number in ACT catalog region file */
{
    int nbr;
    long offset;
    char dsgn;
    char line[256];
    int irh,irm,idd,idm;
    double rs, ds;
/*    double bvmag; */

    /* Drop out if catalog pointer is not set */
    if (sc == NULL)
	return (1);

    /* Drop out if catalog is not open */
    if (sc->ifcat == NULL)
	return (2);

    /* Drop out if star number is too large */
    if (istar > sc->nstars) {
	fprintf (stderr, "ACTSTAR:  %d  > %d is not in catalog\n",
		 istar, sc->nstars);
	return (3);
	}

    /* Move file pointer to start of correct star entry */
    if (istar > 0) {
	offset = (istar - 1) * sc->nbent;
	if (fseek (sc->ifcat, offset, SEEK_SET))
	    return (4);
	}

    /* Read catalog entry */
    if ((nbr = fread (line, sc->nbent, 1, sc->ifcat)) > sc->nbent) {
	fprintf (stderr, "ACTSTAR:  %d / %d bytes read from %s\n",
		 nbr, sc->nbent, sc->isfil);
	return (5);
	}

    st->num = (double) istar;

    /* Read position for this star */
    irh = atoi (line);
    irm = atoi (line+3);
    rs = atof (line+6);
    dsgn = line[14];
    idd = atoi (line+15);
    idm = atoi (line+18);
    ds = atof (line+21);

    /* Convert position to degrees */
    st->ra = hrdeg ((double)irh + ((double)irm)/60.0 + rs / 3600.0);
    st->dec = (double) idd + ((double)idm) / 60.0 + ds / 3600.0;
    if (dsgn == '-') st->dec = -st->dec;

    /* Read proper motion and convert it to to degrees/year */
    st->rapm = hrdeg (atof (line+28)) / 3600.0;
    st->decpm = atof (line+36) / 3600.0;

    /* Set V, B, B-V magnitudes */
    st->xmag[0] = atof (line+75);
    st->xmag[1] = atof (line+68);
    st->xmag[2] = atof (line+82);
    st->isp[0] = (char) 0;
    st->isp[1] = (char) 0;

    return (0);
}

/* ACTSIZE -- return size of one ACT catalog file in bytes */

static int
actsize (filename)

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


/* Feb 11 1999	New program
 * Apr 13 1999	Fix bugs which caused failure on crossing 0:00 h
 * May 12 1999	Fix bug for all searches
 * May 21 1999	Fix bug with proper motion so it is in deg/yr, not sec/yr
 * Jun 16 1999	Use SearchLim()
 * Aug 16 1999	Add RefLim() to get converted search coordinates right
 * Aug 25 1999	Return real number of stars from actread()
 * Sep 16 1999	Fix bug which didn't always return closest stars
 * Sep 16 1999	Add distsort argument so brightest stars in circle works, too
 * Oct 21 1999	Delete unused varaiables after lint
 *
 * Jan  5 2000	Add 2 to spectral type string so there can be proper termination
 * Mar 15 2000	Add proper motions to returns from actread() and actrnum()
 * May 31 2000	Get spectral type from bv2sp()
 * Jun  2 2000	Free all allocated data structures
 * Jun  9 2000	Fix bug which caused memory overflow if limiting number
 * Jun 26 2000	Add coordinate system to SearchLim() arguments
 * Sep 25 2000	Set sc->sptype to 2 to indicate presence of spectral type
 * Nov 29 2000	Add option to read catalog using HTTP
 * Dec 11 2000	Allow catalog search engine URL in actcd[]
 *
 * Jan 11 2001	All printing is to stderr
 * Jun 14 2001	Drop spectral type approximation
 * Sep 11 2001	Change to single magnitude argeument
 * Sep 11 2001	Add sort magnitude argument to actread()
 *
 * Apr  8 2002	Fix extraneous declaration of actsize()
 * Oct  2 2002	Print stars as found in actread() if nstarmax < 1
 *
 * Mar 11 2003	Fix position limit testing
 * Apr  3 2003	Drop unused type variables in actstar() and actread()
 * Apr 14 2003	Explicitly get revision date if nstarmax < 1
 * Jun  2 2003	Print proper motion as mas/year
 * Aug 22 2003	Add radi argument for inner edge of search annulus
 * Aug 22 2003	Fix bug in actrnum: mags were not in one array
 * Sep 25 2003	Add actbin() to fill an image with sources
 * Oct  6 2003	Update actread() and actbin() for improved RefLim()
 * Nov 18 2003	Initialize image size and bits/pixel from header in actbin()
 * Dec  1 2003	Add missing tab to n=-1 header
 * Dec 12 2003	Fix bug in wcs2pix() call in actbin()
 *
 * Aug 30 2004	Include math.h
 *
 * Jun 20 2006	Initialize uninitialized variables
 * Sep 26 2006	Increase length of rastr and destr from 16 to 32
 * Nov 16 2006	Fix binning
 *
 * Jan 10 2007	Add match=1 argument to webrnum()
 * Jan 10 2007	Rewrite web access in actread() and actrnum() to reduce code
 */
