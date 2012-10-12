/*** File libwcs/ucacread.c
 *** December 14, 2009
 *** By Doug Mink, dmink@cfa.harvard.edu
 *** Harvard-Smithsonian Center for Astrophysics
 *** Copyright (C) 2003-2009
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

 * ucacread()	Read UCAC Star Catalog stars in a rectangle on the sky
 * ucacrnum()	Read UCAC Star Catalog stars by number 
 * ucacbin()	Fill a FITS WECS image with UCAC Star Catalog stars
 * ucaczones()	Make list of zones covered by a range of declinations
 * ucacsra (sc,st,zone,rax0)   Find UCAC star closest to specified right ascension
 * ucacopen(zone, nstars)   Open UCAC catalog file, returning number of entries
 * ucacclose (sc)	    Close UCAC catalog file 
 * ucacstar (sc,st,zone,istar) Get UCAC catalog entry for one star
 */

#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <math.h>
#include "fitsfile.h"
#include "wcs.h"
#include "wcscat.h"

#define MAXZONE 100
#define ABS(a) ((a) < 0 ? (-(a)) : (a))

typedef struct {
    int rasec, decsec;
    short cmag;
    unsigned char era, edec, nobs, rflg, ncat, cflg;
    short epra, epdec;
    int rapm, decpm;
    unsigned char erapm, edecpm, qrapm, qdecpm;
    int id2m;
    short jmag, hmag, kmag, qm, cc;
} UCAC2star;

typedef struct {
    int rasec, decsec;
    short mmag, amag, sigmag;
    char objt, dsf;
    short sigra, sigdec;
    char na1, nu1, us1, cn1;
    short cepra, cepdec;
    int rapm, decpm;
    short sigpmr, sigpmd;
    int id2m;
    short jmag, hmag, kmag;
    char jq, hq, kq;
    char e2mpho[3];
    short bmag, rmag, imag;
    char clbl;
    char bq, rq, iq;
    char catflg[10];
    char g1, c1, leda, x2m;
    int rn;
} UCAC3star;


/* pathname of UCAC1 decompressed data files or search engine URL */
char ucac1path[64]="/data/astrocat/ucac1";

/* pathname of UCAC2 decompressed data files or search engine URL */
char ucac2path[64]="/data/astrocat/ucac2";

/* pathname of UCAC3 decompressed data files or search engine URL */
char ucac3path[64]="/data/astrocat/ucac3";

char *ucacpath;
static int ucat = 0;

static double *gdist;	/* Array of distances to stars */
static int ndist = 0;
static int cswap = 0;   /* Byte reverse catalog to Mac/Sun/network order if 1 */

static int ucaczones();
struct StarCat *ucacopen();
void ucacclose();
static int ucacsra();
static int ucacstar();
static void ucacswap4();
static void ucacswap2();


/* UCACREAD -- Read UCAC Star Catalog stars */

int
ucacread (refcatname,cra,cdec,dra,ddec,drad,dradi,distsort,sysout,eqout,epout,
	  mag1,mag2,sortmag,nstarmax,gnum,gra,gdec,gpra,gpdec,gmag,gtype,nlog)

char	*refcatname;	/* Name of catalog (UAC, USAC, UAC2, USAC2) */
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
double	**gmag;		/* Array of magnitudes (returned) */
int	*gtype;		/* Array of object types (returned) */
int	nlog;		/* 1 for diagnostics */
{
    double ra1,ra2;		/* Limiting right ascensions of region in degrees */
    double dec1,dec2;		/* Limiting declinations of region in degrees */
    double dist = 0.0;		/* Distance from search center in degrees */
    double faintmag=0.0;	/* Faintest magnitude */
    double maxdist=0.0;		/* Largest distance */
    int	faintstar=0;		/* Faintest star */
    int	farstar=0;		/* Most distant star */
    int nz;			/* Number of UCAC regions in search */
    int zlist[MAXZONE];		/* List of region numbers */
    int sysref = WCS_J2000;	/* Catalog coordinate system */
    double eqref = 2000.0;	/* Catalog equinox */
    double epref = 2000.0;	/* Catalog epoch */
    double secmarg = 60.0;	/* Arcsec/century margin for proper motion */
    struct StarCat *starcat;	/* Star catalog data structure */
    struct Star *star;		/* Single star data structure */
    double errra, errdec, errpmr, errpmd;
    int nim, ncat;
    int verbose;
    int wrap;
    int iz;
    int magsort;
    int jstar;
    int nrmax = MAXZONE;
    int nstar,i, ntot, imag;
    int istar, istar1, istar2;
    int jtable,iwrap, nread;
    int pass;
    int zone;
    int nmag;
    double num, ra, dec, rapm, decpm, mag;
    double rra1, rra2, rdec1, rdec2;
    double rdist, ddist;
    char cstr[32], rastr[32], decstr[32];
    char ucacenv[16];
    char *str;

    ntot = 0;
    if (nlog > 0)
	verbose = 1;
    else
	verbose = 0;

    /* Set catalog code and path to catalog */
    if (strncmp (refcatname,"ucac2",5)==0 ||
        strncmp (refcatname,"UCAC2",5)==0) {
	ucat = UCAC2;
	ucacpath = ucac2path;
	strcpy (ucacenv, "UCAC2_PATH");
	nmag = 4;
	}
    else if (strncmp (refcatname,"ucac3",5)==0 ||
        strncmp (refcatname,"UCAC3",5)==0) {
	ucat = UCAC3;
	ucacpath = ucac3path;
	strcpy (ucacenv, "UCAC3_PATH");
	nmag = 8;
	}
    else {
	ucat = UCAC1;
	ucacpath = ucac1path;
	strcpy (ucacenv, "UCAC1_PATH");
	nmag = 1;
	}

    /* If pathname is set in environment, override local value */
    if ((str = getenv (ucacenv)) != NULL )
	ucacpath = str;

    /* If pathname is a URL, search and return */
    if (!strncmp (ucacpath, "http:",5)) {
	return (webread (ucacpath,refcatname,distsort,cra,cdec,dra,ddec,drad,
			 dradi,sysout,eqout,epout,mag1,mag2,sortmag,nstarmax,
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

   if (sortmag < 1)
	magsort = 0;
    else if (sortmag > nmag)
	magsort = nmag - 1;
    else
	magsort = sortmag - 1;

    /* Allocate table for distances of stars from search center */
    if (nstarmax > ndist) {
	if (ndist > 0)
	    free ((void *)gdist);
	gdist = (double *) malloc (nstarmax * sizeof (double));
	if (gdist == NULL) {
	    fprintf (stderr,"UCACREAD:  cannot allocate separation array\n");
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
    rra1 = ra1;
    rra2 = ra2;
    rdec1 = dec1;
    rdec2 = dec2;
    RefLim (cra,cdec,dra,ddec,sysout,sysref,eqout,eqref,epout,epref,secmarg,
	    &rra1, &rra2, &rdec1, &rdec2, &wrap, verbose);

    /* Find UCAC Star Catalog zones in which to search */
    nz = ucaczones (rdec1,rdec2,nrmax,zlist,verbose);
    if (nz <= 0) {
	fprintf (stderr,"UCACREAD:  no UCAC zone for %.2f-%.2f %.2f %.2f\n",
		 rra1, rra2, rdec1, rdec2);
	return (0);
	}

    /* Write header if printing star entries as found */
    if (nstarmax < 1) {
	char *revmessage;
	revmessage = getrevmsg();
	printf ("catalog	UCAC1\n");
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
	printf ("ucac_id   	ra          	dec         	");
	if (ucat == UCAC1) {
	    printf ("mag 	ura   	udec  	arcmin\n");
	    printf ("----------	------------	------------	");
	    printf ("-----	------	------	------\n");
	    }
	else if (ucat == UCAC2) {
	    printf ("raerr 	decerr	magc 	");
	    printf ("magj	magh	magk 	mag	");
	    printf ("pmra  	pmdec  	pmrerr 	pmderr 	");
	    printf ("ni	nc	arcsec\n");
	    printf ("----------	------------	------------    ");
	    printf ("------	------	-----	-----	-----	");
	    printf ("-----	-----	------	------	-----	-----	");
	    printf ("--	--	------\n");
	    }
	else if (ucat == UCAC3) {
	    printf ("raerr 	decerr	magb 	magr 	magi 	");
	    printf ("magj 	magh 	magk 	maga 	magm 	");
	    printf ("mag  	pmra  	pmdec  	pmrerr 	pmderr 	");
	    printf ("ni	nc	arcsec\n");
	    printf ("----------	------------	------------	");
	    printf ("------	------	-----	------	-----	");
	    printf ("-----	-----	-----	-----	-----	");
	    printf ("-----	-----	-----	------	------	");
	    printf ("--	--	------\n");
	    }
	}

    /* Loop through zone list */
    nstar = 0;
    for (iz = 0; iz < nz; iz++) {

	/* Get path to zone catalog */
	zone = zlist[iz];
	if ((starcat = ucacopen (zone)) != 0) {

	    jstar = 0;
	    jtable = 0;
	    for (iwrap = 0; iwrap <= wrap; iwrap++) {

		/* Find first star based on RA */
		if (iwrap == 0 || wrap == 0) {
		    istar1 = ucacsra (starcat, star, zone, rra1);
		    if (istar > 5)
			istar = istar - 5;
		    else
			istar = 1;
		    }
		else
		    istar1 = 1;

		/* Find last star based on RA */
		if (iwrap == 1 || wrap == 0) {
		    istar2 = ucacsra (starcat, star, zone, rra2);
		    if (istar2 < starcat->nstars - 5)
			istar2 = istar2 + 5;
		    else
			istar2 = starcat->nstars;
		    }
		else
		    istar2 = starcat->nstars;

		if (istar1 == 0 || istar2 == 0)
		    break;

		nread = istar2 - istar1 + 1;

		/* Loop through zone catalog for this region */
		for (istar = istar1; istar <= istar2; istar++) {
		    jtable ++;

		    if (ucacstar (starcat, star, zone, istar)) {
			fprintf(stderr,"UCACREAD: Cannot read star %d\n",istar);
			break;
			}

		    /* ID number */
		    num = star->num;

		    /* Magnitude */
		    mag = star->xmag[magsort];

		    /* Check magnitude limits */
		    pass = 1;
		    if (mag1 != mag2 && (mag < mag1 || mag > mag2))
			pass = 0;

		    /* Check position limits */
		    if (pass) {

			/* Get position in output coordinate system */
			ra = star->ra;
			dec = star->dec;
			rapm = star->rapm;
			decpm = star->decpm;
			errra = star->errra;
			errdec = star->errdec;
			errpmr = star->errpmr;
			errpmd = star->errpmd;
			nim = star->nimage;
			ncat = star->ncat;
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

		    /* Write star position and magnitude to stdout */
			if (nstarmax < 1) {
			    ra2str (rastr, 31, ra, 3);
			    dec2str (decstr, 31, dec, 2);
			    dist = wcsdist (cra,cdec,ra,dec) * 3600.0;
			    printf ("%010.6f	%s	%s", num,rastr,decstr);
			    if (ucat == UCAC2 || ucat == UCAC3)
				printf ("	%5.3f	%5.3f",
				errra * 3600.0 * cosdeg (dec), errdec * 3600.0);
			    if (ucat == UCAC1)
				printf ("	%5.2f", mag);
			    else
				printf ("	%5.2f	%5.2f	%5.2f	%5.2f",
					star->xmag[0], star->xmag[1],
					star->xmag[2], star->xmag[3]);
			    if (ucat == UCAC3)
				printf ("	%5.2f	%5.2f	%5.2f	%5.2f",
					star->xmag[4], star->xmag[5],
					star->xmag[6], star->xmag[7]);
			    printf ("	%5.2f	%6.1f	%6.1f",
				mag, rapm*3600000.0*cosdeg (dec), decpm*3600000.0);
			    printf ("	%6.1f	%6.1f",
				errpmr*3600000.0, errpmd*3600000.0);
			    printf ("	%2d	%2d	%.3f\n",
				nim, ncat, dist);
			    }

			/* Save star position and magnitude in table */
			else if (nstar < nstarmax) {
			    gnum[nstar] = num;
			    gra[nstar] = ra;
			    gdec[nstar] = dec;
			    gpra[nstar] = rapm;
			    gpdec[nstar] = decpm;
			    if (ucat == UCAC1)
				gmag[0][nstar] = mag;
			    else {
				for (imag = 0; imag < nmag; imag++)
				    gmag[imag][nstar] = star->xmag[imag];

				gmag[nmag][nstar] = star->errra;
				gmag[nmag+1][nstar] = star->errdec;
				gmag[nmag+2][nstar] = star->errpmr;
				gmag[nmag+3][nstar] = star->errpmd;
				gtype[nstar] = (1000 * nim) + ncat;
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
				if (ucat == UCAC1)
				    gmag[0][farstar] = mag;
				else {
				    for (imag = 0; imag < nmag; imag++)
					gmag[imag][farstar] = star->xmag[imag];
				    gmag[nmag][farstar] = star->errra;
				    gmag[nmag+1][farstar] = star->errdec;
				    gmag[nmag+2][farstar] = star->errpmr;
				    gmag[nmag+3][farstar] = star->errpmd;
				    gtype[farstar] = (1000 * nim) + ncat;
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
			    if (ucat == UCAC1)
				gmag[0][faintstar] = mag;
			    else {
				for (imag = 0; imag < nmag; imag++)
				    gmag[imag][faintstar] = star->xmag[imag];
				gmag[nmag][faintstar] = star->errra;
				gmag[nmag+1][faintstar] = star->errdec;
				gmag[nmag+2][faintstar] = star->errpmr;
				gmag[nmag+3][faintstar] = star->errpmd;
				gtype[faintstar] = (1000 * nim) + ncat;
				}
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
			    fprintf (stderr,"UCACREAD: %11.6f: %9.5f %9.5f %5.2f\n",
				 num,ra,dec,mag);

			/* End of accepted star processing */
			}

		    /* Log operation */
		    jstar++;
		    if (nlog > 0 && istar%nlog == 0)
			fprintf (stderr,"UCACREAD: %5d / %5d / %5d sources\r",
				 nstar,jstar,starcat->nstars);

		    /* End of star loop */
		    }

		/* End of 0:00 RA wrap loop */
		}

	    /* End of successful zone file loop */
	    ntot = ntot + starcat->nstars;
	    if (nlog > 0)
		fprintf (stderr,"UCACREAD: %4d / %4d: %5d / %5d  / %5d sources from zone %4d    \n",
		 	 iz+1,nz,nstar,jstar,starcat->nstars,zlist[iz]);

	    /* Close region input file */
	    ucacclose (starcat);
	    }

	/* End of zone loop */
	}

    /* Summarize transfer */
    if (nlog > 0) {
	if (nz > 1)
	    fprintf (stderr,"UCACREAD: %d zones: %d / %d found\n",nz,nstar,ntot);
	else
	    fprintf (stderr,"UCACREAD: 1 region: %d / %d found\n",nstar,ntot);
	if (nstar > nstarmax)
	    fprintf (stderr,"UCACREAD: %d stars found; only %d returned\n",
		     nstar,nstarmax);
	}
    return (nstar);
}

/* UCACRNUM -- Read HST Guide Star Catalog stars from CDROM */

int
ucacrnum (refcatname,nstars,sysout,eqout,epout,
	 gnum,gra,gdec,gpra,gpdec,gmag,gtype,nlog)

char	*refcatname;	/* Name of catalog (UAC, USAC, UAC2, USAC2) */
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
    int sysref=WCS_J2000;	/* Catalog coordinate system */
    double eqref=2000.0;	/* Catalog equinox */
    double epref=2000.0;	/* Catalog epoch */
    struct StarCat *starcat;
    struct Star *star;
    char *str;
    char ucacenv[16];

    int verbose;
    int zone, zone0;
    int jstar, imag, nmag;
    int istar, nstar;
    double num, ra, dec, rapm, decpm, mag;

    if (nlog == 1)
	verbose = 1;
    else
	verbose = 0;

    /* Set catalog code and path to catalog */
    if (strncmp (refcatname,"ucac3",5)==0 ||
        strncmp (refcatname,"UCAC3",5)==0) {
	ucat = UCAC3;
	ucacpath = ucac3path;
	strcpy (ucacenv, "UCAC3_PATH");
	nmag = 8;
	}
    else if (strncmp (refcatname,"ucac2",5)==0 ||
        strncmp (refcatname,"UCAC2",5)==0) {
	ucat = UCAC2;
	ucacpath = ucac2path;
	strcpy (ucacenv, "UCAC2_PATH");
	nmag = 4;
	}
    else {
	ucat = UCAC1;
	ucacpath = ucac1path;
	strcpy (ucacenv, "UCAC1_PATH");
	nmag = 1;
	}

    /* If pathname is set in environment, override local value */
    if ((str = getenv(ucacenv)) != NULL )
	ucacpath = str;

    /* If pathname is a URL, search and return */
    if (!strncmp (ucacpath, "http:",5))
	return (webrnum (ucacpath,refcatname,nstars,sysout,eqout,epout,1,
			 gnum,gra,gdec,gpra,gpdec,gmag,gtype,nlog));

    /* Allocate catalog entry buffer */
    star = (struct Star *) calloc (1, sizeof (struct Star));
    star->num = 0.0;
    nstar = 0;
    zone0 = 0;
    starcat = NULL;

/* Loop through star list */
    for (jstar = 0; jstar < nstars; jstar++) {
	zone = (int) (gnum[jstar] + 0.0000001);

	/* Find numbered stars (rrr.nnnnnn) */
	istar = (int) ((gnum[jstar] - (double) zone + 0.0000001) * 1000000.0);
	if (istar > 0) {
	    if (zone != zone0) {
		if (starcat != NULL)
		    ucacclose (starcat);
		starcat = ucacopen (zone);
		}
    	    if (starcat == NULL) {
		fprintf (stderr,"UCACRNUM: Zone %d file not found\n", zone);
		return (0);
		}
	    if (ucacstar (starcat, star, zone, istar)) {
		fprintf (stderr,"UCACRNUM: Cannot read star %d.%06d\n", zone, istar);
		gra[jstar] = 0.0;
		gdec[jstar] = 0.0;
		if (ucat == UCAC1)
		    gmag[0][jstar] = 0.0;
		else {
		    for (imag = 0; imag < nmag; imag++)
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

	    /* Magnitude */
	    mag = star->xmag[0];

	    /* Save star position and magnitude in table */
	    gnum[jstar] = num;
	    gra[jstar] = ra;
	    gdec[jstar] = dec;
	    gpra[jstar] = rapm;
	    gpdec[jstar] = decpm;
	    gmag[0][jstar] = mag;
    	    gtype[jstar] = (1000 * star->nimage) + star->ncat;
	    if (ucat == UCAC1)
		gmag[0][jstar] = star->xmag[0];
	    else {
		for (imag = 0; imag < nmag; imag++)
		    gmag[imag][jstar] = star->xmag[imag];
		gmag[nmag][jstar] = star->errra;
		gmag[nmag+1][jstar] = star->errdec;
		gmag[nmag+2][jstar] = star->errpmr;
		gmag[nmag+3][jstar] = star->errpmd;
		gtype[jstar] = (1000 * star->nimage) + star->ncat;
		}
	    if (nlog == 1) {
		if (ucat == UCAC1)
		    fprintf (stderr,"UCACRNUM: %11.6f: %9.5f %9.5f %5.2f %s  \n",
			     num, ra, dec, mag, star->isp);
		else if (ucat == UCAC2) {
		    fprintf (stderr,"UCACRNUM: %11.6f: %9.5f %9.5f %5.2f",
			     num, ra, dec, star->xmag[0]);
		    fprintf (stderr," %5.2f %5.2f %5.2f %d %d",
			     star->xmag[1],star->xmag[2],star->xmag[3]);
		    fprintf (stderr," %d %d\n",star->nimage,star->ncat);
		    }
		else
		    fprintf (stderr,"UCACRNUM: %11.6f: %9.5f %9.5f %5.2f %5.2f",
			     num, ra, dec, star->xmag[0],star->xmag[1]);
		    fprintf (stderr," %5.2f %5.2f %5.2f",
			     star->xmag[2], star->xmag[3], star->xmag[4]);
		    fprintf (stderr," %5.2f %5.2f %5.2f",
			     star->xmag[5], star->xmag[6], star->xmag[7]);
		    fprintf (stderr," %d %d\n",star->nimage,star->ncat);
		}
	    }

	/* End of star loop */
	}

/* Summarize search */
    ucacclose (starcat);
    if (nlog > 0)
	fprintf (stderr,"UCACRNUM: %d / %d found\n",nstar,starcat->nstars);

    return (nstars);
}


/* UCACBIN -- Fill a FITS WCS image with UCAC Star Catalog stars */

int
ucacbin (refcatname, wcs, header, image, mag1, mag2, sortmag, magscale, nlog)

char	*refcatname;	/* Name of catalog (UAC, USAC, UAC2, USAC2) */
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
    double dec1,dec2;		/* Limiting declinations of region in degrees */
    int nz;			/* Number of UCAC regions in search */
    int zlist[MAXZONE];		/* List of region numbers */
    int sysref = WCS_J2000;	/* Catalog coordinate system */
    double eqref = 2000.0;	/* Catalog equinox */
    double epref = 2000.0;	/* Catalog epoch */
    double secmarg = 60.0;	/* Arcsec/century margin for proper motion */
    struct StarCat *starcat;	/* Star catalog data structure */
    struct Star *star;		/* Single star cata structure */
    int verbose;
    int wrap;
    int ix, iy, iz;
    int magsort;
    int jstar;
    int nrmax = MAXZONE;
    int nstar, ntot;
    int istar, istar1, istar2;
    int jtable,iwrap, nread;
    int pass;
    int zone;
    double num, ra, dec, rapm, decpm, mag;
    double rra1, rra2, rdec1, rdec2;
    double rdist, ddist;
    char cstr[32];
    char ucacenv[16];
    char *str;
    double xpix, ypix, flux;
    int offscl;
    int bitpix, w, h;   /* Image bits/pixel and pixel width and height */
    double logt = log(10.0);

    ntot = 0;
    if (nlog > 0)
	verbose = 1;
    else
	verbose = 0;

    /* Set catalog code and path to catalog */
    if (strncmp (refcatname,"ucac3",5)==0 ||
        strncmp (refcatname,"UCAC3",5)==0) {
	ucat = UCAC3;
	ucacpath = ucac3path;
	strcpy (ucacenv, "UCAC3_PATH");
	}
   else if (strncmp (refcatname,"ucac2",5)==0 ||
        strncmp (refcatname,"UCAC2",5)==0) {
	ucat = UCAC2;
	ucacpath = ucac2path;
	strcpy (ucacenv, "UCAC2_PATH");
	}
    else {
	ucat = UCAC1;
	ucacpath = ucac1path;
	strcpy (ucacenv, "UCAC1_PATH");
	}

    /* If pathname is set in environment, override local value */
    if ((str = getenv (ucacenv)) != NULL )
	ucacpath = str;

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

    /* Make mag1 always the smallest magnitude */
    if (mag2 < mag1) {
	mag = mag2;
	mag2 = mag1;
	mag1 = mag;
	}

   if (sortmag < 1)
	magsort = 0;
    else if (sortmag > 4)
	magsort = 3;
    else
	magsort = sortmag - 1;

    /* Allocate catalog entry buffer */
    star = (struct Star *) calloc (1, sizeof (struct Star));
    star->num = 0.0;

    nstar = 0;
    jstar = 0;

    /* Get RA and Dec limits in catalog (J2000) coordinates */
    rra1 = ra1;
    rra2 = ra2;
    rdec1 = dec1;
    rdec2 = dec2;
    RefLim (cra,cdec,dra,ddec,sysout,sysref,eqout,eqref,epout,epref,secmarg,
	    &rra1, &rra2, &rdec1, &rdec2, &wrap, verbose);

    /* Find UCAC Star Catalog zones in which to search */
    nz = ucaczones (rdec1,rdec2,nrmax,zlist,verbose);
    if (nz <= 0) {
	fprintf (stderr,"UCACBIN:  no UCAC zone for %.2f-%.2f %.2f %.2f\n",
		 rra1, rra2, rdec1, rdec2);
	return (0);
	}

    /* Loop through zone list */
    nstar = 0;
    for (iz = 0; iz < nz; iz++) {

	/* Get path to zone catalog */
	zone = zlist[iz];
	if ((starcat = ucacopen (zone)) != 0) {

	    jstar = 0;
	    jtable = 0;
	    for (iwrap = 0; iwrap <= wrap; iwrap++) {

		/* Find first star based on RA */
		if (iwrap == 0 || wrap == 0)
		    istar1 = ucacsra (starcat, star, zone, rra1);
		else
		    istar1 = 1;

		/* Find last star based on RA */
		if (iwrap == 1 || wrap == 0)
		    istar2 = ucacsra (starcat, star, zone, rra2);
		else
		    istar2 = starcat->nstars;

		if (istar1 == 0 || istar2 == 0)
		    break;

		nread = istar2 - istar1 + 1;

		/* Loop through zone catalog for this region */
		for (istar = istar1; istar <= istar2; istar++) {
		    jtable ++;

		    if (ucacstar (starcat, star, zone, istar)) {
			fprintf(stderr,"UCACBIN: Cannot read star %d\n",istar);
			break;
			}

		    /* ID number */
		    num = star->num;

		    /* Magnitude */
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
			    fprintf (stderr,"UCACBIN: %11.6f: %9.5f %9.5f %s",
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
			fprintf (stderr,"UCACBIN: %5d / %5d / %5d sources\r",
				 nstar,jstar,starcat->nstars);

		    /* End of star loop */
		    }

		/* End of 0:00 RA wrap loop */
		}

	    /* End of successful zone file loop */
	    ntot = ntot + starcat->nstars;
	    if (nlog > 0)
		fprintf (stderr,"UCACBIN: %4d / %4d: %5d / %5d  / %5d sources from zone %4d    \n",
		 	 iz+1,nz,nstar,jstar,starcat->nstars,zlist[iz]);

	    /* Close region input file */
	    ucacclose (starcat);
	    }

	/* End of zone loop */
	}

    /* Summarize transfer */
    if (nlog > 0) {
	if (nz > 1)
	    fprintf (stderr,"UCACBIN: %d zones: %d / %d found\n",nz,nstar,ntot);
	else
	    fprintf (stderr,"UCACBIN: 1 region: %d / %d found\n",nstar,ntot);
	}
    return (nstar);
}


/* UCACZONE -- Compute the zones over which to search
 * in the specified range of coordinates.
 * Build lists containing the first star and number of stars for each range.
 */

static int
ucaczones (dec1, dec2, nzmax, zones, verbose)

double	dec1, dec2; 	/* Declination limits in degrees */
int	nzmax;		/* Maximum number of zones to find */
int	*zones;		/* Zone numbers (returned)*/
int	verbose;	/* 1 for diagnostics */

{
    int nz;		/* Number of declination zones found (returned) */
    int iz, iz1, iz2;
    int i;
    double spd1, spd2;

    for (i = 0; i < nzmax; i++)
	zones[i] = 0;

/* Find first and last declination zone to search */
    spd1 = 90.0 + dec1;
    iz1 = (int) ((spd1 * 2.0) + 0.99999);
    if (iz1 < 1) iz1 = 1;
    spd2 = 90.0 + dec2;
    iz2 = (int) ((spd2 * 2.0) + 0.99999);
    if (ucat == UCAC1 && iz2 > 169) iz2 = 169;
    if (ucat == UCAC2 && iz2 > 288) iz2 = 288;
    if (ucat == UCAC3 && iz2 > 360) iz2 = 360;
    if (iz1 > iz2)
	return (0);

    nz = iz2 - iz1 + 1;
    if (verbose) {
	fprintf (stderr,"UCACZONES: searching %d zones: %d - %d\n",nz,iz1,iz2);
	fprintf(stderr,"UCACZONES: Dec: %.5f - %.5f\n", dec1,dec2);
	}

    i = 0;
    for (iz = iz1; iz <= iz2; iz++) {
	zones[i] = iz;
	i++;
	}

    return (nz);
}


/* UCACSRA -- Find UCAC star closest to specified right ascension */

static int
ucacsra (sc, st, zone, rax0)

struct StarCat *sc;	/* Star catalog descriptor */
struct Star *st;	/* Current star entry */
int	zone;		/* Declination zone */
double	rax0;		/* Right ascension in degrees for which to search */
{
    int istar, istar1, istar2, nrep;
    double rax, ra1, ra, rdiff, rdiff1, rdiff2, sdiff;
    char rastrx[32];
    int debug = 0;

    rax = rax0;
    if (debug)
	ra2str (rastrx, 31, rax, 3);
    istar1 = 1;
    ucacstar (sc, st, zone, istar1);
    ra1 = st->ra;
    istar = sc->nstars;
    nrep = 0;
    while (istar != istar1 && nrep < 20) {
	if (ucacstar (sc, st, zone, istar))
	    break;
	else {
	    ra = st->ra;
	    if (ra == ra1)
		break;
	    if (debug) {
		char rastr[32];
		ra2str (rastr, 31, ra, 3);
		fprintf (stderr,"UCACSRA %d %d: %s (%s)\n",
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
		fprintf (stderr," ra1=    %.5f ra=     %.5f rax=    %.5f\n",
			 ra1,ra,rax);
		fprintf (stderr," rdiff=  %.5f rdiff1= %.5f rdiff2= %.5f\n",
			 rdiff,rdiff1,rdiff2);
		fprintf (stderr," istar1= %d istar= %d istar1= %d\n",
			 istar1,istar,istar2);
		}
	    if (istar < 1)
		istar = 1;
	    if (istar > sc->nstars)
		istar = sc->nstars;
	    if (istar == istar1)
		break;
	    }
	}
    return (istar);
}

 
/* UCACOPEN -- Open UCAC catalog file, returning number of entries */

struct StarCat *
ucacopen (zone)

int	zone;	/* Number of catalog zone to read */

{
    FILE *fcat;
    struct StarCat *sc;
    int lfile, lpath;
    char *zonefile;
    char *zonepath;	/* Full pathname for catalog file */

    /* Set pathname for catalog file */
    lpath = strlen (ucacpath) + 16;
    zonepath = (char *) malloc (lpath);
    if (ucat == UCAC1)
	sprintf (zonepath, "%s/u1/z%03d", ucacpath, zone);
    else if (ucat == UCAC2)
	sprintf (zonepath, "%s/u2/z%03d", ucacpath, zone);
    else
	sprintf (zonepath, "%s/z%03d", ucacpath, zone);

    /* Set UCAC catalog header information */
    sc = (struct StarCat *) calloc (1, sizeof (struct StarCat));
    sc->byteswapped = 0;

    /* Set number of stars in this zone catalog */
    if (ucat == UCAC1)
	sc->nbent = 67;
    else if (ucat == UCAC2)
	sc->nbent = 44;
    else
	sc->nbent = 84;
    lfile = getfilesize (zonepath);
    if (lfile < 2) {
        fprintf (stderr,"UCAC zone catalog %s has no entries\n",zonepath);
	free (sc);
	sc = NULL;
        return (NULL);
        }
    else
        sc->nstars = lfile / sc->nbent;

    /* Open UCAC file */
    if (!(fcat = fopen (zonepath, "r"))) {
	fprintf (stderr,"UCACOPEN: UCAC file %s cannot be read\n",zonepath);
	free (sc);
	return (NULL);
	}

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
    sc->sptype = 0;
    if (ucat == UCAC1)
	sc->nmag = 1;
    else if (ucat == UCAC2)
	sc->nmag = 4;
    else
	sc->nmag = 8;

    /* UCAC stars are RA-sorted within declination zones */
    sc->rasorted = 1;

/* Check to see if byte-swapping is necessary */
    cswap = 0;
    if (ucat == UCAC2) {
	UCAC2star us2;	/* UCAC2 catalog entry for one star */
	int nbr;

	nbr = fread (&us2, 1, sc->nbent, sc->ifcat);
	if (nbr < 1) {
	    fprintf (stderr,
		 "UCACOPEN: cannot read star 1 from UCAC2 zone catalog %s\n",
		 zonepath);
	    return (0);
	    }

	/* RA should be between 0 and 360 degrees in milliarcseconds */
	if (us2.rasec > 360 * 3600000 || us2.rasec < 0)
	    cswap = 1;

	/* Dec should be between -90 and +90 degrees in milliarcseconds */
	else if (us2.decsec > 90 * 3600000 || us2.decsec < -90 * 3600000)
	    cswap = 1;

	/* J H K magnitudes should be near-positive */
	else if (us2.jmag < -1000 || us2.hmag < -1000  || us2.kmag < -1000)
	    cswap = 1;
	else
	    cswap = 0;

	/* if (cswap)
	    fprintf (stderr,
		    "UCACOPEN: swapping bytes in UCAC2 zone catalog %s\n",
		     zonepath); */
	}
    else if (ucat == UCAC3) {
	UCAC3star us3;	/* UCAC3 catalog entry for one star */
	int nbr;

	nbr = fread (&us3, 1, sc->nbent, sc->ifcat);
	if (nbr < 1) {
	    fprintf (stderr,
		 "UCACOPEN: cannot read star 1 from UCAC3 zone catalog %s\n",
		 zonepath);
	    return (0);
	    }

	/* RA should be between 0 and 360 degrees in milliarcseconds */
	if (us3.rasec > 360 * 3600000 || us3.rasec < 0)
	    cswap = 1;

	/* Dec should be between -90 and +90 degrees in milliarcseconds */
	else if (us3.decsec > 180 * 3600000 || us3.decsec < -180 * 3600000)
	    cswap = 1;

	/* J H K magnitudes should be near-positive */
	else if (us3.jmag < -1000 || us3.hmag < -1000  || us3.kmag < -1000)
	    cswap = 1;
	else
	    cswap = 0;

	/* if (cswap)
	    fprintf (stderr,
	    "UCACOPEN: swapping bytes in UCAC2 zone catalog %s\n",zomepath); */
	}

    sc->istar = 0;
    free (zonepath);
    return (sc);
}


void
ucacclose (sc)
struct StarCat *sc;	/* Star catalog descriptor */
{
    fclose (sc->ifcat);
    free (sc);
    return;
}


/* UCACSTAR -- Get UCAC catalog entry for one star;
              return 0 if successful */

static int
ucacstar (sc, st, zone, istar)

struct StarCat *sc;	/* Star catalog descriptor */
struct Star *st;	/* Current star entry */
int	zone;		/* Declination zone */
int	istar;		/* Star sequence number in UCAC catalog region file */
{
    char line[256];
    int nbr, nbskip;
    UCAC2star us2;	/* UCAC2 catalog entry for one star */
    UCAC3star us3;	/* UCAC3 catalog entry for one star */

    /* Drop out if catalog pointer is not set */
    if (sc == NULL)
	return (1);

    /* Drop out if catalog is not open */
    if (sc->ifcat == NULL)
	return (2);

    /* Drop out if star number is too small or too large */
    if (istar < 1 || istar > sc->nstars) {
        fprintf (stderr, "UCAC star %d is not in catalog\n",istar);
        return (-1);
        }

    /* Move file pointer to start of correct star entry */
    nbskip = sc->nbent * (istar - 1);
    if (fseek (sc->ifcat,nbskip,SEEK_SET))
        return (-1);

    if (ucat == UCAC1)
	nbr = fread (line, 1, sc->nbent, sc->ifcat);
    else if (ucat == UCAC2)
	nbr = fread (&us2, 1, sc->nbent, sc->ifcat);
    else
	nbr = fread (&us3, 1, sc->nbent, sc->ifcat);
    if (nbr < sc->nbent) {
        fprintf (stderr, "UCACSTAR %d / %d bytes read\n",nbr, sc->nbent);
        return (-2);
        }

    /* Star ID number = region.sequence */
    st->num = (double) zone + (0.000001 * (double) istar);

    /* Read UCAC1 position and proper motion from ASCII file */
    if (ucat == UCAC1) {

	/* Read position in degrees */
	st->ra = atof (line) / 3600000.0;
	st->dec = (atof (line+10) / 3600000.0) - 90.0;

	/* Read proper motion and convert it to to degrees/year */
	st->rapm = (atof (line+41) / 3600000.0) / cosdeg (st->dec);
	st->decpm = atof (line+48) / 3600000.0;

	/* Set V magnitude */
	st->xmag[0] = atof (line+20) * 0.01;
	}

    /* Read UCAC2 position, proper motion, and magnitudes from binary file */
    else if (ucat == UCAC2) {
	if (cswap) {
	    ucacswap4 (&us2.rasec);
	    ucacswap4 (&us2.decsec);
	    ucacswap4 (&us2.rapm);
	    ucacswap4 (&us2.decpm);
	    ucacswap2 (&us2.cmag);
	    ucacswap2 (&us2.jmag);
	    ucacswap2 (&us2.hmag);
	    ucacswap2 (&us2.kmag);
	    }
	st->ra  = (double) us2.rasec  / 3600000.0;
	st->dec = (double) us2.decsec / 3600000.0;
	st->errra  = (double) us2.era / 3600000.0;	/* mas */
	st->errra  = st->errra / cosdeg (st->dec);	/* to RA deg */
	st->errdec = (double) us2.edec / 3600000.0;	/* mas */
	st->rapm  = (double) us2.rapm  / 36000000.0;	/* 0.1mas/yr */
	st->decpm = (double) us2.decpm / 36000000.0;	/* 0.1mas/yr */
	st->errpmr  = (double) us2.erapm / 36000000.0;	/* 0.1mas/yr */
	st->errpmr  = st->errpmr / cosdeg (st->dec);	/* to RA deg */
	st->errpmd = (double) us2.edecpm / 36000000.0;	/* 0.1mas/yr */
	st->xmag[0] = ((double) us2.jmag) / 1000.0;
	st->xmag[1] = ((double) us2.hmag) / 1000.0;
	st->xmag[2] = ((double) us2.kmag) / 1000.0;
	st->xmag[3] = ((double) us2.cmag) / 100.0;
	st->nimage = (int) us2.nobs;
	st->ncat = (int) us2.ncat;
	}

    /* Read UCAC3 position, proper motion, and magnitudes from binary file */
    else {
	if (cswap) {
	    ucacswap4 (&us3.rasec);
	    ucacswap4 (&us3.decsec);
	    ucacswap4 (&us3.rapm);
	    ucacswap4 (&us3.decpm);
	    ucacswap2 (&us3.sigra);
	    ucacswap2 (&us3.sigdec);
	    ucacswap2 (&us3.sigpmr);
	    ucacswap2 (&us3.sigpmd);
	    ucacswap2 (&us3.mmag);
	    ucacswap2 (&us3.amag);
	    ucacswap2 (&us3.jmag);
	    ucacswap2 (&us3.hmag);
	    ucacswap2 (&us3.kmag);
	    ucacswap2 (&us3.bmag);
	    ucacswap2 (&us3.rmag);
	    ucacswap2 (&us3.imag);
	    }
	st->ra  = (double) us3.rasec  / 3600000.0;	/* mas */
	st->dec = (double) us3.decsec / 3600000.0;	/* mas */
	st->dec = st->dec - 90.0;
	st->errra  = (double) us3.sigra / 3600000.0;	/* mas */
	st->errra  = st->errra / cosdeg (st->dec);	/* to RA deg */
	st->errdec = (double) us3.sigdec / 3600000.0;	/* mas */
	st->rapm  = (double) us3.rapm  / 36000000.0;	/* 0.1mas/yr */
	st->rapm  = st->rapm / cosdeg (st->dec);	/* to RA deg */
	st->decpm = (double) us3.decpm / 36000000.0;	/* 0.1mas/yr */
	st->errpmr  = (double) us3.sigpmr / 36000000.0;	/* 0.1mas/yr */
	st->errpmr  = st->errpmr / cosdeg (st->dec);	/* to RA deg */
	st->errpmd = (double) us3.sigpmd / 36000000.0;	/* 0.1mas/yr */
	st->nimage = (int) us3.nu1;
	st->ncat = (int) us3.us1;
	if (us3.bmag == 0)
	    st->xmag[0] = 99.990;
	else
	    st->xmag[0] = ((double) us3.bmag) / 1000.0;
	if (us3.rmag == 0)
	    st->xmag[1] = 99.990;
	else
	    st->xmag[1] = ((double) us3.rmag) / 1000.0;
	if (us3.imag == 0)
	    st->xmag[2] = 99.990;
	else
	    st->xmag[2] = ((double) us3.imag) / 1000.0;
	if (us3.jmag == 0)
	    st->xmag[3] = 99.990;
	else
	    st->xmag[3] = ((double) us3.jmag) / 1000.0;
	if (us3.hmag == 0)
	    st->xmag[4] = 99.990;
	else
	    st->xmag[4] = ((double) us3.hmag) / 1000.0;
	if (us3.kmag == 0)
	    st->xmag[5] = 99.990;
	else
	    st->xmag[5] = ((double) us3.kmag) / 1000.0;
	if (us3.mmag == 0)
	    st->xmag[6] = 99.990;
	else
	    st->xmag[6] = ((double) us3.mmag) / 1000.0;
	if (us3.amag == 0)
	    st->xmag[7] = 99.990;
	else
	    st->xmag[7] = ((double) us3.amag) / 1000.0;
	}

    return (0);
}


/* UCACSWAP2 -- Swap bytes in Integer*2 number in place */

static void
ucacswap2 (string)


char *string;	/* Address of starting point of bytes to swap */

{
    char *sbyte, temp;

    sbyte = string;
    temp = sbyte[0];
    sbyte[0] = sbyte[1];
    sbyte[1] = temp;
    return;
}


/* UCACSWAP4 -- Reverse bytes of Integer*4 or Real*4 number in place */

static void
ucacswap4 (string)

char *string;	/* Address of Integer*4 or Real*4 vector */

{
    char temp0, temp1, temp2, temp3;

    temp3 = string[0];
    temp2 = string[1];
    temp1 = string[2];
    temp0 = string[3];
    string[0] = temp0;
    string[1] = temp1;
    string[2] = temp2;
    string[3] = temp3;

    return;
}

/* Apr 24 2003	New subroutines, based on ty2read.c and uacread.c
 * May 30 2003	Add UCAC2, compute file size rather than reading it from file
 * Jun  2 2003	Print proper motions as mas/year
 * Aug 22 2003	Add radi argument for inner edge of search annulus
 * Sep 25 2003	Add ucacbin() to fill an image with sources
 * Oct  6 2003	Update ubcread() and ubcbin() for improved RefLim()
 * Nov 10 2003	Fix byte-swapping test in ucacopen() found by Ed Beshore
 * Nov 18 2003	Initialize image size and bits/pixel from header in ucacbin()
 * Dec  1 2003	Add missing tab to n=-1 header
 * Dec 12 2003	Fix bug in wcs2pix() call in ucacbin()
 *
 * Jan  4 2005	Fix bug in if statement on line 626 found by Dan Katz at JPL
 *
 * Jun 20 2006	Drop unused variables
 * Sep 26 2006	Increase length of rastr and destr from 16 to 32
 * Nov 16 2006	Fix binning
 *
 * Jan  8 2007	Drop unused variable in ucacbin()
 * Jan 10 2007	Add match=1 argument to webrnum()
 * Jul 11 2007	Add magnitude byte-swap check
 *
 * Oct  5 2009	Add UCAC3
 * Oct 15 2009	Read extra stars in RA
 * Oct 22 2009	Set UCAC3 magnitudes to 99.99 if zero
 * Oct 30 2009	Add position and proper motion error to Star structure
 * Nov  2 2009	Print UCAC3 errors if n = -1
 * Nov  5 2009	Return number of images and catalogs in gtype
 * Nov  5 2009	Return errors in position and proper motion as magnitudes
 * Nov  5 2009	Return UCAC2 and UCAC3 RA proper motion and error as RA degrees
 * Dec 14 2009	Drop nmag1 from ucacread() and ucacrnum(); it wasn't being used
 */
