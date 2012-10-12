/*** File libwcs/tmcread.c
 *** September 28, 2009
 *** By Doug Mink, dmink@cfa.harvard.edu
 *** Harvard-Smithsonian Center for Astrophysics
 *** Copyright (C) 2001-2009
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
#include <string.h>
#include <stdio.h>
#include <math.h>
#include "fitsfile.h"
#include "wcs.h"
#include "wcscat.h"

#define MAXREG 1800

#define MINRA 1
#define MAXRA 2

#define ABS(a) ((a) < 0 ? (-(a)) : (a))

/* pathname of 2MASS point source catalog root directory
   or catalog search engine URL */
char tmc2path[64]="/data/astrocat/2MASS";
char tmcapath[64]="/data/astrocat/tmc";
char tmcepath[64]="/data/astrocat2/tmce";
char tmxpath[64]="/data/astrocat/tmx";
char *tmcpath;

static double *gdist;	/* Array of distances to stars */
static int ndist = 0;
static int linedump = 0;
static char *catfile = NULL;

static int tmcreg();
struct StarCat *tmcopen();
void tmcclose();
static int tmcstar();
static int tmcsdec();
static int tmcsra();

/* TMCREAD -- Read 2MASS catalog stars from disk files */

int
tmcread (refcat,cra,cdec,dra,ddec,drad,dradi,distsort,sysout,eqout,epout,
	 mag1,mag2,sortmag,nstarmax,gnum,gra,gdec,gmag,gtype,nlog)

int	refcat;		/* Code for catalog file from wcscat.h */
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
double	*gnum;		/* Array of catalog numbers (returned) */
double	*gra;		/* Array of right ascensions (returned) */
double	*gdec;		/* Array of declinations (returned) */
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
    int nreg = 0;	/* Number of 2MASS point source regions in search */
    int rlist[MAXREG];	/* List of regions */
    char inpath[128];	/* Pathname for input region file */
    int sysref=WCS_J2000;	/* Catalog coordinate system */
    double eqref=2000.0;	/* Catalog equinox */
    double epref=2000.0;	/* Catalog epoch */
    double size = 0.0;		/* Semi-major axis of extended source */
    struct StarCat *starcat;
    struct Star *star;
    int verbose;
    int wrap;
    int pass;
    int ireg;
    int imag, nmag;
    int jstar, iw;
    int zone;
    int magsort;
    int nrmax = MAXREG;
    int nstar,i, ntot;
    int istar, istar1, istar2;
    double num, ra, dec, mag;
    double rra1, rra2, rra2a, rdec1, rdec2;
    double rdist, ddist;
    char cstr[32], rastr[32], decstr[32], numstr[32];
    char *str;
    char tmcenv[16];

    /* Choose appropriate catalog to read */
    if (refcat == TMIDR2) {
	tmcpath = tmc2path;
	strcpy (tmcenv, "TMCIDR2_PATH");
	nmag = 3;
	}
    else if (refcat == TMXSC) {
	tmcpath = tmxpath;
	strcpy (tmcenv, "TMX_PATH");
	nmag = 3;
	}
    else if (refcat == TMPSCE) {
	tmcpath = tmcepath;
	strcpy (tmcenv, "TMCE_PATH");
	nmag = 6;
	}
    else {
	tmcpath = tmcapath;
	strcpy (tmcenv, "TMC_PATH");
	nmag = 3;
	}

    ntot = 0;
    if (nlog > 0)
	verbose = 1;
    else if (nlog < 0) {
	linedump = 0;
	verbose = 0;
	}
    else
	verbose = 0;

    /* If pathname is set in environment, override local value */
    if ((str = getenv(tmcenv)) != NULL )
	tmcpath = str;

    /* If pathname is a URL, search and return */
    if (!strncmp (tmcpath, "http:",5)) {
	if (catfile == NULL)
	    catfile = (char *) calloc (8, 1);
	if (refcat == TMPSC)
	    strcpy (catfile, "tmc");
	else if (refcat == TMPSCE)
	    strcpy (catfile, "tmce");
	else if (refcat == TMXSC)
	    strcpy (catfile, "tmx");
	else if (refcat == TMIDR2)
	    strcpy (catfile, "tmidr2");
	return (webread (str,catfile,distsort,cra,cdec,dra,ddec,drad,dradi,
			 sysout,eqout,epout,mag1,mag2,sortmag,nstarmax,
			 gnum,gra,gdec,NULL,NULL,gmag,gtype,nlog));
	}
    wcscstr (cstr, sysout, eqout, epout);

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

    /* Allocate table for distances of stars from search center */
    if (nstarmax > ndist) {
	if (ndist > 0)
	    free ((void *)gdist);
	gdist = (double *) malloc (nstarmax * sizeof (double));
	if (gdist == NULL) {
	    fprintf (stderr,"TMCREAD:  cannot allocate separation array\n");
	    return (0);
	    }
	ndist = nstarmax;
	}

    /* Allocate catalog entry buffer */
    star = (struct Star *) calloc (1, sizeof (struct Star));
    star->num = 0.0;

    nstar = 0;
    jstar = 0;

    if (sortmag > 0 && sortmag < 4)
	magsort = sortmag - 1;
    else 
	magsort = 0;

    rra1 = ra1;
    rra2 = ra2;
    rdec1 = dec1;
    rdec2 = dec2;
    RefLim (cra,cdec,dra,ddec,sysout,sysref,eqout,eqref,epout,epref,0.0,
	    &rra1, &rra2, &rdec1, &rdec2, &wrap, verbose);
    if (wrap) {
	rra2a = rra2;
	rra2 = 360.0;
	}
    else
	rra2a = 0.0;

    /* Write header if printing star entries as found */
    if (nstarmax < 1) {
	char *revmessage;
	revmessage = getrevmsg();
	if (refcat == TMXSC)
	    printf ("catalog	2MASS Extended Source Catalog\n");
	else
	    printf ("catalog	2MASS Point Source Catalog\n");
	ra2str (rastr, 31, cra, 3);
	printf ("ra	%s\n", rastr);
	dec2str (decstr, 31, cdec, 2);
	printf ("dec	%s\n", decstr);
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
	if (refcat == TMXSC)
	    printf ("program	stmx %s\n", revmessage);
	else
	    printf ("program	stmc %s\n", revmessage);
	if (refcat == TMXSC)
	    printf ("2mx_id    	ra          	dec         	");
	else
	    printf ("2mass_id  	ra          	dec         	");
	printf ("magj  	magh  	magk  ");
	if (refcat == TMPSCE)
	    printf ("magje  	maghe  	magke ");
	if (refcat == TMXSC)
	    printf (" 	size  ");
	printf (" 	arcmin\n");
	printf ("----------	------------	------------	");
	printf ("------	------	------");
	if (refcat == TMPSCE)
	    printf ("------	------	------");
	if (refcat == TMXSC)
	    printf ("	------");
	printf ("	------\n");
	}

    /* If searching through RA = 0:00, split search in two */
    for (iw = 0; iw <= wrap; iw++) {

	/* Find 2MASS Point Source Catalog regions in which to search */
	nreg = tmcreg (refcat, rra1,rra2,rdec1,rdec2,nrmax,rlist,verbose);
	if (nreg <= 0) {
	    fprintf (stderr,"TMCREAD:  no 2MASS regions found\n");
	    return (0);
	    }

	/* Loop through zone or region list */
	for (ireg = 0; ireg < nreg; ireg++) {

	    /* Open file for this region of 2MASS point source catalog */
	    zone = rlist[ireg];
	    starcat = tmcopen (refcat, zone);
	    if (starcat == NULL) {
		fprintf (stderr,"TMCREAD: File %s not found\n",inpath);
		return (0);
		}

	    /* Find first and last stars in this region */
	    if (refcat == TMPSC || refcat == TMPSCE) {
		istar1 = tmcsra (starcat, star, zone, rra1, MINRA);
		istar2 = tmcsra (starcat, star, zone, rra2, MAXRA);
		}
	    else if (refcat == TMXSC) {
		istar1 = tmcsra (starcat, star, zone, rra1, MINRA);
		istar2 = tmcsra (starcat, star, zone, rra2, MAXRA);
		/* istar1 = 1;
		istar2 = starcat->nstars; */
		}
	    else {
		istar1 = tmcsdec (starcat, star, zone, rdec1);
		istar2 = tmcsdec (starcat, star, zone, rdec2);
		}
	    if (verbose)
		fprintf (stderr,"TMCREAD: Searching stars %d through %d in region %d\n",
			istar1, istar2-1, zone);

	    /* Loop through catalog for this region */
	    for (istar = istar1; istar < istar2; istar++) {
		if (tmcstar (starcat, star, zone, istar)) {
		    fprintf (stderr,"TMCREAD: Cannot read star %d\n", istar);
		    break;
		    }

		/* ID number */
		num = star->num;

		/* Magnitude */
		mag = star->xmag[0];

		/* Semi-major axis of extended source */
		if (refcat == TMXSC)
		    size = star->size;

		/* Check magnitude limits */
		pass = 1;
		if (mag1 != mag2 && (mag < mag1 || mag > mag2))
		    pass = 0;

		if (pass) {

		    /* Get position in output coordinate system */
		    ra = star->ra;
		    dec = star->dec;
		    wcscon (sysref, sysout, eqref, eqout, &ra, &dec, epout);

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

		    /* Write star position and magnitudes to stdout */
		    if (nstarmax < 1) {
			CatNum (TMPSC, -10, 0, num, numstr);
			ra2str (rastr, 31, ra, 3);
			dec2str (decstr, 31, dec, 2);
			dist = wcsdist (cra,cdec,ra,dec) * 60.0;
                        printf ("%s	%s	%s", numstr,rastr,decstr);
			for (imag = 0; imag < 3; imag++) {
			    if (star->xmag[imag] > 100.0)
				printf ("	%.3fL", star->xmag[imag]-100.0);
			    else
				printf ("	%.3f ", star->xmag[imag]);
			    }
			if (refcat == TMPSCE) {
			    for (imag = 3; imag < 6; imag++) {
				printf ("	%.3f ", star->xmag[imag]);
				}
			    }
			if (refcat == TMXSC)
			    printf ("	%.1f", size);
			printf ("	%.2f\n", dist);
			}

		    /* Save star position and magnitudes in table */
		    else if (nstar < nstarmax) {
			gnum[nstar] = num;
			gra[nstar] = ra;
			gdec[nstar] = dec;
			for (imag = 0; imag < nmag; imag++) {
			    if (gmag[imag] != NULL)
				gmag[imag][nstar] = star->xmag[imag];
			    }
			if (refcat == TMXSC)
			    gtype[nstar] = (int) ((size + 0.05) * 10.0);
			else
			    gtype[nstar] = 0;
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
			    for (imag = 0; imag < nmag; imag++) {
				if (gmag[imag] != NULL)
				    gmag[imag][farstar] = star->xmag[imag];
				}
			    if (refcat == TMXSC)
				gtype[farstar] = (int) ((size + 0.05) * 10.0);
			    else
				gtype[farstar] = 0;
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
			for (imag = 0; imag < nmag; imag++) {
			    if (gmag[imag] != NULL)
				gmag[imag][faintstar] = star->xmag[imag];
			    }
			if (refcat == TMXSC)
			    gtype[faintstar] = (int) ((size + 0.05) * 10.0);
			else
			    gtype[faintstar] = 0;
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
			fprintf (stderr,"TMCREAD: %11.6f: %9.5f %9.5f %5.2f %5.2f %5.2f\n",
				 num,ra,dec,star->xmag[0],star->xmag[1],star->xmag[2]);

		    /* End of accepted star processing */
		    }

		/* Log operation */
		jstar++;
		if (nlog > 0 && istar%nlog == 0)
		    fprintf (stderr,"TMCREAD: %5d / %5d / %5d sources\r",
			     nstar,jstar,starcat->nstars);

		/* End of star loop */
		}

	    ntot = ntot + starcat->nstars;
	    if (nlog > 0)
		fprintf (stderr,"TMCREAD: %4d / %4d: %5d / %5d  / %5d sources from region %4d    \n",
		 	 ireg+1,nreg,nstar,jstar,starcat->nstars,zone);

	    /* Close region input file */
	    tmcclose (starcat);
	    }
	rra1 = 0.0;
	rra2 = rra2a;
	}

/* close output file and summarize transfer */
    if (nlog > 0) {
	if (nreg > 1)
	    fprintf (stderr,"TMCREAD: %d regions: %d / %d found\n",nreg,nstar,ntot);
	else
	    fprintf (stderr,"TMCREAD: 1 region: %d / %d found\n",nstar,ntot);
	if (nstar > nstarmax)
	    fprintf (stderr,"TMCREAD: %d stars found; only %d returned\n",
		     nstar,nstarmax);
	}
    return (nstar);
}

/* TMCRNUM -- Read HST Guide Star Catalog stars from CDROM */

int
tmcrnum (refcat,nstars,sysout,eqout,epout,gnum,gra,gdec,gmag,gtype,nlog)

int	refcat;		/* Code for catalog file from wcscat.h */
int	nstars;		/* Number of stars to find */
int	sysout;		/* Search coordinate system */
double	eqout;		/* Search coordinate equinox */
double	epout;		/* Proper motion epoch (0.0 for no proper motion) */
double	*gnum;		/* Array of source numbers for which to search */
double	*gra;		/* Array of right ascensions (returned) */
double	*gdec;		/* Array of declinations (returned) */
double	**gmag;		/* 2-D array of magnitudes (returned) */
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

    int rnum;
    int jstar;
    int imag, nmag;
    int istar, nstar;
    double num, ra, dec, rapm, decpm, dstar;
    char tmcenv[16];

    /* Choose appropriate catalog */
    if (refcat == TMIDR2) {
	tmcpath = tmc2path;
	strcpy (tmcenv, "TMCIDR2_PATH");
	nmag = 3;
	}
    else if (refcat == TMXSC) {
	tmcpath = tmxpath;
	strcpy (tmcenv, "TMX_PATH");
	nmag = 3;
	}
    else if (refcat == TMPSCE) {
	tmcpath = tmcepath;
	strcpy (tmcenv, "TMCE_PATH");
	nmag = 6;
	}
    else {
	tmcpath = tmcapath;
	strcpy (tmcenv, "TMC_PATH");
	nmag = 3;
	}

    if (nlog < 0)
	linedump = 1;

    /* If pathname is set in environment, override local value */
    if ((str = getenv(tmcenv)) != NULL )
	tmcpath = str;

    /* If pathname is a URL, search and return */
    if (!strncmp (tmcpath, "http:",5))
	return (webrnum (tmcpath,"tmc",nstars,sysout,eqout,epout,1,
			 gnum,gra,gdec,NULL,NULL,gmag,gtype,nlog));

    /* Allocate catalog entry buffer */
    star = (struct Star *) calloc (1, sizeof (struct Star));
    star->num = 0.0;
    nstar = 0;

    /* Loop through star list */
    for (jstar = 0; jstar < nstars; jstar++) {
	rnum = (int) (gnum[jstar] + 0.0000000001);
	starcat = tmcopen (refcat, rnum);
    	if (starcat == NULL) {
	    fprintf (stderr,"TMCRNUM: File %s not found\n",inpath);
	    return (0);
	    }
	if (refcat == TMIDR2)
	    dstar = (gnum[jstar] - (double)rnum) * 10000000.0;
	else
	    dstar = (gnum[jstar] - (double)rnum) * 1000000.0;
	istar = (int) (dstar + 0.5);
	if (tmcstar (starcat, star, rnum, istar)) {
	    fprintf (stderr,"TMCRNUM: Cannot read star %d\n", istar);
	    gra[jstar] = 0.0;
	    gdec[jstar] = 0.0;
	    for (imag = 0; imag < nmag; imag++) {
		gmag[imag][jstar] = 0.0;
		}
	    gtype[jstar] = 0;
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

	/* Save star position and magnitude in table */
	gnum[jstar] = num;
	gra[jstar] = ra;
	gdec[jstar] = dec;
	for (imag = 0; imag < nmag; imag++) {
	    gmag[imag][jstar] = star->xmag[imag];
	    }
	if (refcat == TMXSC)
	    gtype[jstar] = (int) ((star->size * 10.0) + 0.5);
	else
	    gtype[jstar] = 0;
	if (nlog == 1) {
	    fprintf (stderr,"TMCRNUM: %11.6f: %9.5f %9.5f %5.2f %5.2f %5.2f",
		     num, ra, dec, star->xmag[0],star->xmag[1],star->xmag[2]);
	    if (nmag > 3) {
		fprintf (stderr," %5.2f %5.2f %5.2f",
			 star->xmag[3],star->xmag[4],star->xmag[5]);
		}
	    fprintf (stderr, "\n");
	    }

	/* End of star loop */
	}

/* Summarize search */
    if (nlog > 0)
	fprintf (stderr,"TMCRNUM: %d / %d found\n",nstar, nstars);

    tmcclose (starcat);
    return (nstars);
}


/* TMCBIN -- Fill FITS WCS image with 2MASS point source catalog objects */

int
tmcbin (refcat, wcs, header, image, mag1, mag2, sortmag, magscale, nlog)

int	refcat;		/* Code for catalog file from wcscat.h */
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
    int	sysout;		/* Search coordinate system */
    double eqout;	/* Search coordinate equinox */
    double epout;	/* Proper motion epoch (0.0 for no proper motion) */
    double ra1,ra2;	/* Limiting right ascensions of region in degrees */
    double dec1,dec2;	/* Limiting declinations of region in degrees */
    int nreg = 0;	/* Number of 2MASS point source regions in search */
    int rlist[MAXREG];	/* List of regions */
    char inpath[128];	/* Pathname for input region file */
    int sysref=WCS_J2000;	/* Catalog coordinate system */
    double eqref=2000.0;	/* Catalog equinox */
    double epref=2000.0;	/* Catalog epoch */
    struct StarCat *starcat;
    struct Star *star;
    int verbose;
    int wrap;
    int pass;
    int ireg;
    int jstar, iw;
    int zone;
    int magsort;
    int nrmax = MAXREG;
    int nstar, ntot;
    int istar, istar1, istar2;
    double num, ra, dec, mag;
    double rra1, rra2, rra2a, rdec1, rdec2;
    double rdist, ddist;
    char cstr[32];
    char *str;
    char tmcenv[16];
    double xpix, ypix, flux;
    int offscl;
    int ix, iy;
    int bitpix, w, h;   /* Image bits/pixel and pixel width and height */
    double logt = log(10.0);

    /* Choose appropriate catalog */
    if (refcat == TMIDR2) {
	tmcpath = tmc2path;
	strcpy (tmcenv, "TMCIDR2_PATH");
	}
    else if (refcat == TMXSC) {
	tmcpath = tmxpath;
	strcpy (tmcenv, "TMX_PATH");
	}
    else if (refcat == TMPSCE) {
	tmcpath = tmcepath;
	strcpy (tmcenv, "TMCE_PATH");
	}
    else {
	tmcpath = tmcapath;
	strcpy (tmcenv, "TMC_PATH");
	}

    ntot = 0;
    if (nlog > 0)
	verbose = 1;
    else if (nlog < 0) {
	linedump = 0;
	verbose = 0;
	}
    else
	verbose = 0;

    /* Set image parameters */
    bitpix = 0;
    (void)hgeti4 (header, "BITPIX", &bitpix);
    w = 0;
    (void)hgeti4 (header, "NAXIS1", &w);
    h = 0;
    (void)hgeti4 (header, "NAXIS2", &h);

    /* If pathname is set in environment, override local value */
    if ((str = getenv(tmcenv)) != NULL )
	tmcpath = str;

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

    if (sortmag > 0 && sortmag < 4)
	magsort = sortmag - 1;
    else 
	magsort = 0;

    rra1 = ra1;
    rra2 = ra2;
    rdec1 = dec1;
    rdec2 = dec2;
    RefLim (cra,cdec,dra,ddec,sysout,sysref,eqout,eqref,epout,epref,0.0,
	    &rra1, &rra2, &rdec1, &rdec2, &wrap, verbose);
    if (wrap) {
	rra2a = rra2;
	rra2 = 360.0;
	}
    else
	rra2a = 0.0;

    /* If searching through RA = 0:00, split search in two */
    for (iw = 0; iw <= wrap; iw++) {

	/* Find 2MASS Point Source Catalog regions in which to search */
	nreg = tmcreg (refcat, rra1,rra2,rdec1,rdec2,nrmax,rlist,verbose);
	if (nreg <= 0) {
	    fprintf (stderr,"TMCBIN:  no 2MASS regions found\n");
	    return (0);
	    }

	/* Loop through zone or region list */
	for (ireg = 0; ireg < nreg; ireg++) {

	    /* Open file for this region of 2MASS point source catalog */
	    zone = rlist[ireg];
	    starcat = tmcopen (refcat, zone);
	    if (starcat == NULL) {
		fprintf (stderr,"TMCBIN: File %s not found\n",inpath);
		return (0);
		}

	    /* Find first and last stars in this region */
	    if (refcat == TMPSC || refcat == TMPSCE || refcat == TMXSC) {
		istar1 = tmcsra (starcat, star, zone, rra1, MINRA);
		istar2 = tmcsra (starcat, star, zone, rra2, MAXRA);
		}
	    else {
		istar1 = tmcsdec (starcat, star, zone, rdec1);
		istar2 = tmcsdec (starcat, star, zone, rdec2);
		}
	    if (verbose)
		fprintf (stderr,"TMCBIN: Searching stars %d through %d\n",
			istar1, istar2-1);

	    /* Loop through catalog for this region */
	    for (istar = istar1; istar < istar2; istar++) {
		if (tmcstar (starcat, star, zone, istar)) {
		    fprintf (stderr,"TMCBIN: Cannot read star %d\n", istar);
		    break;
		    }

		/* ID number */
		num = star->num;

		/* Magnitude */
		mag = star->xmag[0];

		/* Check magnitude limits */
		pass = 1;
		if (mag1 != mag2 && (mag < mag1 || mag > mag2))
		    pass = 0;

		if (pass) {

		    /* Get position in output coordinate system */
		    ra = star->ra;
		    dec = star->dec;
		    wcscon (sysref, sysout, eqref, eqout, &ra, &dec, epout);

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
		    wcs2pix (wcs, ra, dec,&xpix,&ypix,&offscl);
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
			fprintf (stderr,"TMCBIN: %11.6f: %9.5f %9.5f %s",
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
		    fprintf (stderr,"TMCBIN: %5d / %5d / %5d sources\r",
			     nstar,jstar,starcat->nstars);

		/* End of star loop */
		}

	    ntot = ntot + starcat->nstars;
	    if (nlog > 0)
		fprintf (stderr,"TMCBIN: %4d / %4d: %5d / %5d  / %5d sources from region %4d    \n",
		 	 ireg+1,nreg,nstar,jstar,starcat->nstars,zone);

	    /* Close region input file */
	    tmcclose (starcat);
	    }
	rra1 = 0.0;
	rra2 = rra2a;
	}

/* close output file and summarize transfer */
    if (nlog > 0) {
	if (nreg > 1)
	    fprintf (stderr,"TMCBIN: %d regions: %d / %d found\n",nreg,nstar,ntot);
	else
	    fprintf (stderr,"TMCBIN: 1 region: %d / %d found\n",nstar,ntot);
	}
    return (nstar);
}

char rdir[50][4]={"0", "1", "2", "3", "4", "5a", "5b", "6a", "6b", "6c",
	"6d", "7a", "7b", "7c", "7d", "8a", "8b", "9", "10", "11", "12",
	"13", "14", "15", "16a", "16b", "17a", "17b", "17c", "17d", "17e",
	"17f", "17g", "17h", "18a", "18b", "18c", "18d", "19a", "19b",
	"19c", "19d", "20a", "20b", "20c", "20d", "21", "22", "23", ""};
double zmax[50]={00.000, 01.000, 02.000, 03.000, 04.000, 05.000, 05.500,
		 06.000, 06.250, 06.500, 06.750, 07.000, 07.250, 07.500,
		 07.750, 08.000, 08.500, 09.000, 10.000, 11.000, 12.000,
		 13.000, 14.000, 15.000, 16.000, 16.500, 17.000, 17.125,
		 17.250, 17.375, 17.500, 17.625, 17.750, 17.875, 18.000,
		 18.250, 18.500, 18.750, 19.000, 19.250, 19.500, 19.750,
		 20.000, 20.250, 20.500, 20.750, 21.000, 22.000, 23.000,
		 24.000};


/* TMCREG -- find the regions contained by the given RA/Dec limits
 * Build lists containing the first star and number of stars for each range.
 */

static int
tmcreg (refcat, ra1, ra2, dec1, dec2, nrmax, regions, verbose)

int	refcat;		/* Code for catalog file from wcscat.h */
double	ra1, ra2;	/* Right ascension limits in degrees */
double	dec1, dec2; 	/* Declination limits in degrees */
int	nrmax;		/* Maximum number of regions to find */
int	*regions;	/* Region numbers to search (returned)*/
int	verbose;	/* 1 for diagnostics */

{
    int nrgn;		/* Number of regions found (returned) */
    int ir;
    int iz1 = 0;
    int iz2 = 0;
    int ir1,ir2,jr1,jr2,i;
    int ispd1, ispd2, ispd;
    int nsrch;
    double rah1,rah2, spd1, spd2;

    nrgn = 0;

    /* Find region range to search based on right ascension */
    if (refcat == TMIDR2) {
	rah1 = ra1 / 15.0;
	for (i = 1; i < 50; i++) {
	    if (rah1 < zmax[i]) {
		iz1 = i - 1;
		break;
		}
	    }
	rah2 = ra2 / 15.0;
	for (i = 1; i < 50; i++) {
	    if (rah2 < zmax[i]) {
		iz2 = i - 1;
		break;
		}
	    }
	if (iz2 >= iz1) {
	    ir1 = iz1;
	    ir2 = iz2;
	    jr1 = 0;
	    jr2 = 0;
	    nsrch = iz2 - iz1 + 1;
	    }
	else {
	    ir1 = iz1;
	    ir2 = 48;
	    jr1 = 0;
	    jr2 = iz2;
	    nsrch = 48 - iz1 + 1 + iz2 + 1;
	    }

	/* Search region northern hemisphere or only one region */
	if (verbose) {
	    fprintf(stderr,"TMCREG: RA: %.5f - %.5f, Dec: %.5f - %.5f\n",
		ra1,ra2,dec1,dec2);
	    if (nsrch == 1)
		fprintf (stderr,"TMCREG: searching region %d", ir1);
	    else
		fprintf (stderr,"TMCREG: searching %d regions: %d - %d",
		 nsrch, ir1, ir2);
	    if (jr1 > 0 && jr2 > 0)
		fprintf (stderr,", %d - %d", jr1, jr2);
	    fprintf (stderr,"\n");
	    }

	/* Loop through first section of sky */
	nrgn = 0;
	for (ir = ir1; ir <= ir2; ir++) {
	    if (verbose)
		fprintf (stderr,"TMCREG: Region %d (%s) added to search\n",
		    ir, rdir[ir]);

	    /* Add this region to list, if there is space */
	    if (nrgn < nrmax) {
		regions[nrgn] = ir;
		nrgn++;
		}
	    }

	/* Loop through second section of sky */
	for (ir = jr1; ir < jr2; ir++) {
	    if (verbose)
		fprintf (stderr,"TMCREG: Region %d %s) added to search\n",
		     ir, rdir[ir]);

	    /* Add this region to list, if there is space */
	    if (nrgn < nrmax) {
		regions[nrgn] = ir;
		nrgn++;
		}
	    }
	}

    /* Compute SPD regions for all-sky release of point source catalog */
    else {
	if (dec1 < dec2) {
	    spd1 = dec1 + 90.0;
	    spd2 = dec2 + 90.0;
	    }
	else {
	    spd1 = dec2 + 90.0;
	    spd2 = dec1 + 90.0;
	    }
	ispd1 = (int) (spd1 * 10.0);
	ispd2 = (int) (spd2 * 10.0);
	if (ispd2 > 1799) ispd2 = 1799;
	for (ispd = ispd1; ispd <= ispd2; ispd++) {
	    /* Add this region to list, if there is space */
	    if (nrgn < nrmax) {
		regions[nrgn] = ispd;
		nrgn++;
		}
	    }
	}

    return (nrgn);
}


/* TMCOPEN -- Open 2MASS point source catalog file, returning catalog structure */

struct StarCat *
tmcopen (refcat, zone)

int	refcat;		/* Code for catalog file from wcscat.h */
int	zone;		/* RA zone (hours) to read */

{
    FILE *fcat;
    struct StarCat *sc;
    int lfile, lpath, izone, ireg;
    char *zonefile;
    char *zonepath;	/* Full pathname for catalog file */

    /* Set path to 2MASS Point Source Catalog zone */
    if (refcat == TMPSC || refcat == TMPSCE || refcat == TMXSC) {
	izone = zone / 10;
	ireg = zone % 10;
	lpath = strlen (tmcpath) + 18;
	zonepath = (char *) malloc (lpath);
	sprintf (zonepath, "%s/%03d/t%04d.cat", tmcpath, izone, ireg);
	}
    else {
	lpath = strlen (tmcpath) + 18;
	zonepath = (char *) malloc (lpath);
	sprintf (zonepath, "%s/idr2psc%s.tbl", tmcpath, rdir[zone]);
	}

    /* Find length of 2MASS catalog file */
    lfile = getfilesize (zonepath);

    /* Check for existence of catalog */
    if (lfile < 2) {
	fprintf (stderr,"TMCOPEN: Binary catalog %s has no entries\n",zonepath);
	free (zonepath);
	return (NULL);
	}

    /* Open 2MASS point source catalog zone file */
    if (!(fcat = fopen (zonepath, "r"))) {
	fprintf (stderr,"TMCOPEN: 2MASS PSC file %s cannot be read\n",zonepath);
	free (zonepath);
	return (0);
	}

    /* Set 2MASS PSC catalog header information */
    sc = (struct StarCat *) calloc (1, sizeof (struct StarCat));
    sc->byteswapped = 0;
    sc->refcat = refcat;

    if (refcat == TMPSC) {
	sc->entra = 0;
	sc->entdec = 10;
	sc->entname = 0;
	sc->entmag[0] = 39;
	sc->entmag[1] = 46;
	sc->entmag[2] = 53;
	sc->entadd = 61;
	sc->nbent = 69;
	}
    else if (refcat == TMPSCE) {
	sc->entra = 0;
	sc->entdec = 10;
	sc->entname = 0;
	sc->entmag[0] = 39;
	sc->entmag[1] = 46;
	sc->entmag[2] = 53;
	sc->entmag[3] = 60;
	sc->entmag[4] = 66;
	sc->entmag[5] = 72;
	sc->entadd = 79;
	sc->nbent = 87;
	}
    else if (refcat == TMXSC) {
	sc->entra = 0;
	sc->entdec = 10;
	sc->entname = 0;
	sc->entmag[0] = 39;
	sc->entmag[1] = 46;
	sc->entmag[2] = 53;
	sc->entsize = 60;
	sc->nbent = 68;
	}
    else {
	sc->entra = 0;
	sc->entdec = 10;
	sc->entmag[0] = 53;
	sc->entmag[1] = 72;
	sc->entmag[2] = 91;
	sc->entadd = 110;
	sc->nbent = 302;
	}
    sc->nstars = lfile / sc->nbent;

    /* Separate filename from pathname and save in structure */
    zonefile = strrchr (zonepath,'/');
    if (zonefile)
	zonefile = zonefile + 1;
    else
	zonefile = zonepath;
    if (strlen (zonefile) < 24)
	strcpy (sc->isfil, zonefile);
    else
	strncpy (sc->isfil, zonefile, 23);

    /* Set other catalog information in structure */
    sc->inform = 'J';
    sc->coorsys = WCS_J2000;
    sc->epoch = 2000.0;
    sc->equinox = 2000.0;
    sc->ifcat = fcat;
    sc->sptype = 2;

    /* All-sky release 2MASS catalogs are RA-sorted within Dec zones */
    if (refcat == TMPSC || refcat == TMPSCE)
	sc->rasorted = 1;
    /* Pre-release 2MASS catalogs were Dec-sorted within RA zones */
    else
	sc->rasorted = 0;

    free (zonepath);
    return (sc);
}


void
tmcclose (sc)
struct StarCat *sc;	/* Star catalog descriptor */
{
    fclose (sc->ifcat);
    if (sc->catdata != NULL)
	free (sc->catdata);
    free (sc);
    return;
}


/* TMCSDEC -- Find 2MASS star closest to specified declination */

static int
tmcsdec (starcat, star, zone, decx0)

struct StarCat *starcat; /* Star catalog descriptor */
struct Star *star;	/* Current star entry */
int	zone;		/* RA zone in which search is occuring */
double	decx0;		/* Declination in degrees for which to search */
{
    int istar, istar1, istar2, nrep;
    double decx, dec1, dec, rdiff, rdiff1, rdiff2, sdiff;
    char decstrx[32];
    int debug = 0;

    decx = decx0;
    if (debug)
	dec2str (decstrx, 31, decx, 3);
    istar1 = 1;
    if (tmcstar (starcat, star, zone, istar1))
	return (0);
    dec1 = star->dec;
    istar = starcat->nstars;
    nrep = 0;
    while (istar != istar1 && nrep < 20) {
	if (tmcstar (starcat, star, zone, istar))
	    break;
	else {
	    dec = star->dec;
	    if (dec == dec1)
		break;
	    if (debug) {
		char decstr[32];
		dec2str (decstr, 31, dec, 3);
		fprintf (stderr,"TMCSRA %d %d: %s (%s)\n",
			 nrep,istar,decstr,decstrx);
		}
	    rdiff = dec1 - dec;
	    rdiff1 = dec1 - decx;
	    rdiff2 = dec - decx;
	    if (nrep > 20 && ABS(rdiff2) > ABS(rdiff1)) {
		istar = istar1;
		break;
		}
	    nrep++;
	    sdiff = (double)(istar - istar1) * rdiff1 / rdiff;
	    istar2 = istar1 + (int) (sdiff + 0.5);
	    dec1 = dec;
	    istar1 = istar;
	    istar = istar2;
	    if (debug) {
		fprintf (stderr," dec1=    %.5f dec=     %.5f decx=    %.5f\n",
			 dec1,dec,decx);
		fprintf (stderr," rdiff=  %.5f rdiff1= %.5f rdiff2= %.5f\n",
			 rdiff,rdiff1,rdiff2);
		fprintf (stderr," istar1= %d istar= %d istar1= %d\n",
			 istar1,istar,istar2);
		}
	    if (istar < 1)
		istar = 1;
	    if (istar > starcat->nstars)
		istar = starcat->nstars;
	    if (istar == istar1)
		break;
	    }
	}
    return (istar);
}


/* TMCSRA -- Find 2MASS star closest to specified right ascension */

static int
tmcsra (starcat, star, zone, rax0, minmax)

struct StarCat *starcat; /* Star catalog descriptor */
struct Star *star;	/* Current star entry */
int	zone;		/* Declination zone in which search is occurring */
double	rax0;		/* Right ascension in degrees for which to search */
int	minmax;		/* Flag to say whether this is a min or max RA */
{
    int istar, istar0, istar1, nrep, i;
    double rax, ra0, ra1, ra, sdiff;
    char rastrx[32];
    char rastr[32];
    int debug = 0;

    rax = rax0;
    ra0 = -1.0;
    ra1 = star->ra;
    if (debug) {
	ra2str (rastrx, 31, rax, 3);
	ra2str (rastr, 31, ra1, 3);
	nrep = -1;
	istar = (int) star->num;
	fprintf (stderr,"TMCSRA %d %d: %s (%s)\n",
		 nrep,istar,rastr,rastrx);
	}
    istar0 = 1;
    if (tmcstar (starcat, star, zone, istar0))
	return (0);
    ra0 = star->ra;
    istar1 = starcat->nstars;
    if (tmcstar (starcat, star, zone, istar1))
	return (0);
    ra1 = star->ra;
    istar = starcat->nstars / 2;
    if (tmcstar (starcat, star, zone, istar))
	return (0);
    ra = star->ra;
    nrep = 0;
    while (istar != istar1 && nrep < 20) {
	if (ra < rax) {
	    sdiff = 0.5 * (double) (istar1 - istar);
	    if (sdiff < 1.0)
		break;
	    istar0 = istar;
	    istar = istar + (int) (sdiff + 0.5);
	    }
	else if (ra > rax) {
	    sdiff = 0.5 * (double) (istar - istar0);
	    if (sdiff < 1.0)
		break;
	    istar1 = istar;
	    istar = istar - (int) (sdiff + 0.5);
	    }
	else
	    break;
	if (debug) {
	    fprintf (stderr," istar0= %d istar1= %d istar= %d\n",
		     istar0, istar1,istar);
	    fprintf (stderr," ra1=    %.5f ra=     %.5f rax=    %.5f\n",
			 ra0,ra,rax);
	    }
	if (istar == 1 || istar == istar1)
	    break;
	if (tmcstar (starcat, star, zone, istar))
	    break;
	ra = star->ra;
	nrep++;
	}

    /* For small catalogs, linear projection of RA's doesn't work */
    /* Check lower numbers if low end of range is being set */
    if (minmax == MINRA) {
	for (i = 1; i < 5; i++) {
	    istar0 = istar - 1;
	    if (istar0 < 1)
		break;
	    if (tmcstar (starcat, star, zone, istar0))
		    break;
	    if (star->ra < rax)
		break;
	    else
		istar = istar0;
	    }
	}

    /* Check higher numbers if top end of range is being set */
    else {
	for (i = 1; i < 5; i++) {
	    istar0 = istar + 1;
	    if (istar0 > starcat->nstars)
		break;
	    if (tmcstar (starcat, star, zone, istar0))
		break;
	    if (star->ra > rax)
		break;
	    else
		istar = istar0;
	    }
	}
    
    return (istar);
}


/* TMCSTAR -- Get 2MASS Point Source Catalog entry for one star;
              return 0 if successful */

static int
tmcstar (sc, st, zone, istar)

struct StarCat *sc;	/* Star catalog descriptor */
struct Star *st;	/* Current star entry */
int	zone;		/* Zone catalog number (1-49) */
int	istar;		/* Star sequence in 2MASS zone file */
{
    char line[500];
    int nbskip, nbr, iflag;

    /* Drop out if catalog pointer is not set */
    if (sc == NULL)
	return (1);

    /* Drop out if catalog is not open */
    if (sc->ifcat == NULL)
	return (2);

    /* Drop out if star number is too large */
    if (istar > sc->nstars) {
	fprintf (stderr, "TMCSTAR:  %d  > %d is not in catalog\n",
		 istar, sc->nstars);
	return (3);
	}

    /* Read entry for one star */
    nbskip = sc->nbent * (istar - 1);
    if (fseek (sc->ifcat,nbskip,SEEK_SET))
	return (-1);
    nbr = fread (line, sc->nbent, 1, sc->ifcat) * sc->nbent;
    if (nbr < sc->nbent) {
	fprintf (stderr, "tmcstar %d / %d bytes read\n",nbr, sc->nbent);
	return (-2);
	}

    /* Make up source number from zone number and star number */
    if (sc->refcat == TMIDR2)
	st->num = zone + (0.0000001 * (double) istar);
    else
	st->num = zone + (0.000001 * (double) istar);

    /* Read position in degrees */
    st->ra = atof (line);
    st->dec = atof (line+sc->entdec);

    /* No proper motion */
    st->rapm = 0.0;
    st->decpm = 0.0;

    /* Set J magnitude */
    st->xmag[0] = atof (line+sc->entmag[0]);

    /* Set H magnitude */
    st->xmag[1] = atof (line+sc->entmag[1]);

    /* Set K magnitude */
    st->xmag[2] = atof (line+sc->entmag[2]);

    /* Add J, H, K errors if needed */
    if (sc->refcat == TMPSCE) {
	st->xmag[3] = atof (line+sc->entmag[3]);
	st->xmag[4] = atof (line+sc->entmag[4]);
	st->xmag[5] = atof (line+sc->entmag[5]);
	}

    /* Add 100 to magnitude if it isn't a good one */
    if (sc->refcat == TMPSC || sc->refcat == TMPSCE || sc->refcat == TMXSC) {
	if (line[sc->entadd] == 'U')
	    st->xmag[0] = st->xmag[0] + 100.0;
	if (line[sc->entadd + 1] == 'U')
	    st->xmag[1] = st->xmag[1] + 100.0;
	if (line[sc->entadd + 2] == 'U')
	    st->xmag[2] = st->xmag[2] + 100.0;
	}

	/* Preliminary data release data quality flag */
    else {
	iflag = ((int) line[sc->entadd]) - 48;
	if (iflag < 1 || iflag == 3 || iflag > 4)
	    st->xmag[0] = st->xmag[0] + 100.0;
	iflag = ((int) line[sc->entadd + 1]) - 48;
	if (iflag < 1 || iflag == 3 || iflag > 4)
	    st->xmag[1] = st->xmag[1] + 100.0;
	iflag = ((int) line[sc->entadd + 2]) - 48;
	if (iflag < 1 || iflag == 3 || iflag > 4)
	    st->xmag[2] = st->xmag[2] + 100.0;
	}
    if (sc->refcat == TMXSC)
	st->size = atof (line+sc->entsize);
    else
	st->size = 0.0;

    if (linedump)
	printf ("%s\n",line);

    return (0);
}

/* May 29 2001	New program, based on ty2read.c and uacread.c
 * May 30 2001	Round K magnitude to nearest 1000th
 * Jun 13 2001	Round star number up to avoid truncation problem
 * Jun 27 2001	Add code to print one entry at time if nstars < 1
 * Jun 27 2001	Allocate gdist only if larger array is needed
 * Sep 11 2001	Return all three magnitudes
 * Sep 17 2001	Print line from catalog if nlog is < 0
 * Sep 17 2001	Flag bad magnitudes by adding 100 to them
 * Sep 18 2001	Fix bug in magnitudes returned if not distance sorted
 * Nov 20 2001	Change cos(degrad()) to cosdeg()
 * Nov 29 2001	Declare undeclared subroutine tmcsdec
 * Dec  3 2001	Change default catalog directory to /data/astrocat/2MASS
 *
 * Feb 13 2002	Fix catalog name in web access
 * Oct  3 2002	Use global variable for scat version
 *
 * Apr  3 2003	Drop unused variables after lint
 * Apr 14 2003	Explicitly get revision date if nstarmax < 1
 * May 19 2003	Add code to read dec-zoned, ra-sorted All-Sky release
 * May 21 2003	Add catfile argument to read both IDR2 and All-Sky releases
 * May 27 2003	Allow IDR2 and Allsky release to be set by catalog name
 * May 28 2003	Star ID numbers from All-Sky release have 6 decimal places
 * Jul  2 2003	Fix limiting magnitude for All-Sky Release
 * Aug 22 2003	Add radi argument for inner edge of search annulus
 * Sep 25 2003	Add tmcbin() to fill an image with sources
 * Oct  6 2003	Update tmcread() and tmcbin() for improved RefLim()
 * Nov 18 2003	Initialize image size and bits/pixel from header in tmcbin()
 *
 * Jan 13 2004	Add support for 2MASS Extended Source Catalog
 * Jan 14 2004	Add code to fix convergence in tmcsra()
 * Jan 23 2004	Fix search algorith in tmcsra()
 * Nov 10 2004	Fix region computation at north pole
 *
 * Aug  5 2005	Add JHK errors as additional option
 * Aug  5 2005	Avoid extra work by passing refcat catalog code
 *
 * Jun 20 2006	Initialize uninitialized variables
 * Sep 26 2006	Increase length of rastr and destr from 16 to 32
 * Nov 15 2006	Fix binning
 *
 * Jan  8 2007	Drop unused variables
 * Jan  9 2007	Relabel number arrays
 * Jan 10 2007	Add match=1 argument to webrnum()
 * Oct 31 2007	Properly return magnitude errors from tmcrnum(), if present
 * Nov 20 2007	Fix bug which offset limit flag by one (found by Gus Muensch)
 *
 * Sep 28 2009	Print correct heading for n<0 Extended Source tab table
 */
