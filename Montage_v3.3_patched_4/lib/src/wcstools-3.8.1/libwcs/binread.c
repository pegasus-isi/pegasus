/*** File libwcs/binread.c
 *** September 25, 2009
 *** By Doug Mink, dmink@cfa.harvard.edu
 *** Harvard-Smithsonian Center for Astrophysics
 *** Copyright (C) 1998-2009
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

/* int binread()	Read binary catalog sources + names in specified region
 * int binrnum()	Read binary catalog sources + names with specified numbers
 * int binopen()	Open binary catalog, returning number of entries
 * int binstar()	Get binary catalog entry for one source
 * void binclose()	Close binary catalog
 */

/* default pathname for catalog,  used if catalog file not found in current
   working directory, but overridden by the WCS_BINDIR, SAO_PATH (if bincat
   is SAO), PPM_PATH (if bincat is PPM, HIP_PATH, if bincat is HIP, or
   IRAS_PATH is bincat is IRAS) environment variable */

char bindir[64]="/data/stars";

static double *tdist;	/* Array of distances to sources from search center */
static int ndist = 0;

#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <math.h>
#include <string.h>
#include <strings.h>
#include <sys/types.h>
#include <fcntl.h>
#include "wcs.h"
#include "fitsfile.h"
#include "wcscat.h"
#ifndef O_BINARY
#define O_BINARY 0
#endif

static int binsra();
static int binsize();
void binclose();
static void binswap8();
static void binswap4();
static void binswap2();


/* BINREAD -- Read binary catalog sources + names in specified region */

int
binread (bincat,distsort,cra,cdec,dra,ddec,drad,dradi,sysout,eqout,epout,
	 mag1,mag2,sortmag,nstarmax,starcat,
	 tnum,tra,tdec,tpra,tpdec,tmag,tpeak,tobj,nlog)

char	*bincat;	/* Name of reference star catalog file */
int	distsort;	/* 1 to sort stars by distance from center */
double	cra;		/* Search center J2000 right ascension in degrees */
double	cdec;		/* Search center J2000 declination in degrees */
double	dra;		/* Search half width in right ascension in degrees */
double	ddec;		/* Search half-width in declination in degrees */
double	drad;		/* Limiting separation in degrees (ignore if 0) */
double	dradi;		/* Inner edge of annulus in degrees (ignore if 0) */
int	sysout;		/* Search coordinate system */
double	eqout;		/* Search coordinate equinox */
double	epout;		/* Proper motion epoch (0.0 for no proper motion) */
double	mag1,mag2;	/* Limiting magnitudes (none if equal) */
int	sortmag;	/* Magnitude by which to sort (1 to nmag) */
int	nstarmax;	/* Maximum number of sources to be returned */
struct StarCat **starcat; /* Star catalog data structure */
double	*tnum;		/* Array of catalog numbers (returned) */
double	*tra;		/* Array of right ascensions (returned) */
double	*tdec;		/* Array of declinations (returned) */
double  *tpra;		/* Array of right ascension proper motions (returned) */
double  *tpdec;		/* Array of declination proper motions (returned) */
double	**tmag;		/* 2-D Array of magnitudes (returned) */
int	*tpeak;		/* Array of encoded spectral types (returned) */
char	**tobj;		/* Array of object names (returned) */
int	nlog;
{
    double rra1,rra2;	/* Limiting catalog right ascensions of region */
    double rdec1,rdec2;	/* Limiting catalog declinations of region */
    double ra1,ra2;	/* Limiting output right ascensions of region */
    double dec1,dec2;	/* Limiting output declinations of region */
    double dist = 0.0;  /* Distance from search center in degrees */
    double faintmag=0.0; /* Faintest magnitude */
    double maxdist=0.0; /* Largest distance */
    int faintstar=0;    /* Faintest star */
    int farstar=0;      /* Most distant star */
    int sysref;		/* Catalog coordinate system */
    double eqref;	/* Catalog coordinate equinox */
    double epref;	/* Catalog position epoch */
    double secmarg = 60.0; /* Arcsec/century margin for proper motion */
    double ra, dec, rapm, decpm;
    double rra1a, rra2a;
    double rdist, ddist;
    struct StarCat *sc;	/* Star catalog data structure */
    struct Star *star;
    int wrap, iwrap, istar1,istar2;
    int pass;
    int imag;
    char *objname;
    int lname;
    int nmag;		/* Real number of magnitudes per entry (- rv) */
    int jstar;
    int nstar;
    double mag = 0.0;
    double num;
    int i;
    int magsort;
    int istar = 0;
    int isp;
    int verbose;
    int mrv;
    char cstr[16];
    char str[128];

    sc = *starcat;

    if (nlog > 0)
	verbose = 1;
    else
	verbose = 0;

    /* Open catalog */
    if (sc == NULL)
	sc = binopen (bincat);
    *starcat = sc;
    if (sc == NULL)
	return (0);

    /* If pathname is a URL, search and return */
    if (sc->caturl != NULL) {
	*starcat = NULL;
	strcpy (str, sc->caturl);
	free (sc);
	return (webread (str,bincat,distsort,cra,cdec,dra,ddec,drad,dradi,
		sysout,eqout,epout,mag1,mag2,sortmag,nstarmax,
		tnum,tra,tdec,tpra,tpdec,tmag,tpeak,nlog));
	}

    if (sc->nstars <= 0) {
	binclose (sc);
	sc = NULL;
	return (0);
	}

    /* Keep mag1 the smallest magnitude */
    if (mag2 < mag1) {
	mag = mag2;
	mag2 = mag1;
	mag1 = mag;
	}

    if (sortmag > 0 && sortmag <= sc->nmag)
	magsort = sortmag - 1;
    else 
	magsort = 0;

    /* Logging interval */
    nstar = 0;

    /* Allocate space for distances from search center, if necessary */
    if (nstarmax > ndist) {
	if (ndist > 0)
	    free ((void *)tdist);
	if (nstarmax > 10)
	    tdist = (double *) calloc (nstarmax, sizeof(double));
	else
	    tdist = (double *) calloc (10, sizeof(double));
	if (tdist == NULL) {
	    fprintf (stderr,"BINREAD:  cannot allocate separation array\n");
	    ndist = 0;
	    return (0);
	    }
	if (nstarmax > 10)
	    ndist = nstarmax;
	else
	    ndist = 10;
	}

    SearchLim (cra, cdec, dra, ddec, sysout, &ra1, &ra2, &dec1, &dec2, verbose);

    /* Make sure first declination is always the smallest one */
    if (dec1 > dec2) {
	dec = dec1;
	dec1 = dec2;
	dec2 = dec;
	}
  
    sysref = sc->coorsys;
    eqref = sc->equinox;
    epref = sc->epoch;
    RefLim (cra,cdec,dra,ddec,sysout,sysref,eqout,eqref,epout,epref,secmarg,
	    &rra1, &rra2, &rdec1, &rdec2, &wrap, verbose);

    if (verbose) {
	char rstr1[16],rstr2[16],dstr1[16],dstr2[16];
	ra2str (rstr1, 16, rra1, 3);
        dec2str (dstr1, 16, rdec1, 2);
	ra2str (rstr2, 16, rra2, 3);
        dec2str (dstr2, 16, rdec2, 2);

	wcscstr (cstr, sysref,eqref,epref);
	fprintf (stderr,"BINREAD: RA: %s - %s  Dec: %s - %s %s\n",
		 rstr1, rstr2, dstr1, dstr2, cstr);
	}

    /* If catalog RA range includes zero, split search in two */
    if (wrap) {
	rra1a = 0.0;
	rra2a = rra2;
	rra2 = 360.0;
	}
    else {
	rra2a = 0;
	}

    if (sc->entrv > 0) {
	nmag = sc->nmag - 1;
	mrv = nmag;
	}
    else {
	nmag = sc->nmag;
	mrv = 0;
	}

    /* Allocate catalog entry buffer */
    star = (struct Star *) calloc (1, sizeof (struct Star));
    star->num = 0.0;

    jstar = 0;

    /* Loop through wraps (do not cross 360 degrees in search */
    for (iwrap = 0; iwrap <= wrap; iwrap++) {

	/* Set first and last stars to check */
	if (sc->rasorted) {
	    istar1 = binsra (sc, star, rra1);
	    istar2 = binsra (sc, star, rra2);
	    }
	else {
	    istar1 = sc->star1;
	    istar2 = sc->star0 + sc->nstars;
	    }
	if (verbose)
	    fprintf (stderr,"BINREAD: Searching stars %d through %d\n",istar1,istar2);

	/* Loop through catalog */
	for (istar = istar1; istar <= istar2; istar++) {
	    if (binstar (sc, star, istar)) {
		fprintf (stderr,"BINREAD: Cannot read star %d\n", istar);
		break;
		}

	    /* ID number */
	    num = star->num;

	    /* Magnitude */
	    if (sc->entmag[0] > 0)
		mag = star->xmag[magsort];

	    /* Check magnitude limits */
	    pass = 1;
	    if (mag1 != mag2 && (mag < mag1 || mag > mag2))
		pass = 0;

	    /* Get position in output coordinate system, equinox, and epoch */
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

		/* Radial velocity */
		if (sc->entrv > 0)
		    star->xmag[mrv] = star->radvel;

		/* Spectral Type */
		isp = (1000 * (int) star->isp[0]) + (int)star->isp[1];

		/* Save star position and magnitude in table */
		if (nstar < nstarmax) {
		    tnum[nstar] = num;
		    tra[nstar] = ra;
		    tdec[nstar] = dec;
		    if (sc->mprop == 1) {
			tpra[nstar] = rapm;
			tpdec[nstar] = decpm;
			}
		    for (imag = 0; imag < sc->nmag; imag++) {
			if (tmag[imag] != NULL)
			    tmag[imag][nstar] = star->xmag[imag];
			}
		    tpeak[nstar] = isp;
		    tdist[nstar] = dist;
		    if (sc->ncobj > 0 && tobj != NULL) {
			lname = strlen (star->objname) + 1;
			objname = (char *)calloc (lname, 1);
			strcpy (objname, star->objname);
			tobj[nstar] = objname;
			}
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
			tnum[farstar] = num;
			tra[farstar] = ra;
			tdec[farstar] = dec;
			if (sc->mprop == 1) {
			    tpra[farstar] = rapm;
			    tpdec[farstar] = decpm;
			    }
			for (imag = 0; imag < sc->nmag; imag++) {
			    if (tmag[imag] != NULL)
				tmag[imag][farstar] = star->xmag[imag];
			    }
			tpeak[farstar] = isp;
			tdist[farstar] = dist;
			if (sc->ncobj > 0 && tobj != NULL) {
			    free ((void *)tobj[farstar]);
			    lname = strlen (star->objname) + 1;
			    objname = (char *)calloc (lname, 1);
			    strcpy (objname, star->objname);
			    tobj[farstar] = objname;
			    }
			maxdist = 0.0;

		    /* Find new farthest star */
			for (i = 0; i < nstarmax; i++) {
			    if (tdist[i] > maxdist) {
				maxdist = tdist[i];
				farstar = i;
				}
			    }
			}
		    }

		/* Else if too many stars, replace faintest star */
		else if (mag < faintmag) {
		    tnum[faintstar] = num;
		    tra[faintstar] = ra;
		    tdec[faintstar] = dec;
		    if (sc->mprop == 1) {
			tpra[faintstar] = rapm;
			tpdec[faintstar] = decpm;
			}
		    for (imag = 0; imag < sc->nmag; imag++) {
			if (tmag[imag] != NULL)
			    tmag[imag][faintstar] = star->xmag[imag];
			}
		    tpeak[faintstar] = isp;
		    tdist[faintstar] = dist;
		    if (sc->ncobj > 0 && tobj != NULL) {
			free ((void *)tobj[faintstar]);
			lname = strlen (star->objname) + 1;
			objname = (char *)calloc (lname, 1);
			strcpy (objname, star->objname);
			tobj[faintstar] = objname;
			}
		    faintmag = 0.0;

		    /* Find new faintest star */
		    for (i = 0; i < nstarmax; i++) {
			if (tmag[magsort][i] > faintmag) {
			    faintmag = tmag[magsort][i];
			    faintstar = i;
			    }
			}
		    }
		
		nstar++;
		jstar++;
		if (nlog == 1)
		    fprintf (stderr,"BINREAD: %11.6f: %9.5f %9.5f %5.2f\n",
			   num,ra,dec,mag);

	    /* End of accepted star processing */
		}

	/* Log operation */
	    if (nlog > 0 && istar%nlog == 0)
		fprintf (stderr,"BINREAD: %5d / %5d / %5d sources catalog %s\r",
			jstar,istar,sc->nstars,bincat);

	/* End of star loop */
	    }

	/* Set second set of RA limits if passing through 0h */
	rra1 = rra1a;
	rra2 = rra2a;
	}

    /* Summarize search */
    if (nlog > 0) {
	fprintf (stderr,"BINREAD: Catalog %s : %d / %d / %d found\n",
		 bincat,jstar,istar,sc->nstars);
	if (nstar > nstarmax)
	    fprintf (stderr,"BINREAD: %d stars found; only %d returned\n",
		     nstar,nstarmax);
	}

    free ((void *)star);
    free ((void *)tdist);
    return (nstar);
}


/* BINRNUM -- Read binary catalog stars with specified numbers */

int
binrnum (bincat, nnum, sysout, eqout, epout, match,
	 tnum,tra,tdec,tpra,tpdec,tmag,tpeak,tobj,nlog)

char	*bincat;	/* Name of reference star catalog file */
int	nnum;		/* Number of stars to look for */
int	sysout;		/* Search coordinate system */
double	eqout;		/* Search coordinate equinox */
double	epout;		/* Proper motion epoch (0.0 for no proper motion) */
int	match;		/* If 1, match number exactly, else number is sequence*/
double	*tnum;		/* Array of star numbers to look for */
double	*tra;		/* Array of right ascensions (returned) */
double	*tdec;		/* Array of declinations (returned) */
double  *tpra;		/* Array of right ascension proper motions (returned) */
double  *tpdec;		/* Array of declination proper motions (returned) */
double	**tmag;		/* 2-D Array of magnitudes (returned) */
int	*tpeak;		/* Array of peak counts (returned) */
char	**tobj;		/* Array of object names (returned) */
int	nlog;
{
    int sysref;		/* Catalog coordinate system */
    double eqref;	/* Catalog coordinate equinox */
    double epref;	/* Catalog position epoch */
    int jnum;
    int nstar;
    double ra, dec, rapm, decpm;
    double num;
    int istar;
    int isp;
    int imag;
    int lname;
    char *objname;
    struct StarCat *starcat;
    struct Star *star;
    char str[128];
    int nmag;
    int mrv;

    nstar = 0;
    starcat = binopen (bincat);
    if (starcat == NULL)
	return (0);

    /* If pathname is a URL, search and return */
    if (starcat->caturl != NULL) {
	strcpy (str, starcat->caturl);
	free (starcat);
	return (webrnum (str,bincat,nnum,sysout,eqout,epout,1,
		tnum,tra,tdec,tpra,tpdec,tmag,tpeak,nlog));
	}

    /* If no stars in catalog, print error message and return */
    if (starcat->nstars <= 0) {
	free ((void *)starcat);
	fprintf (stderr,"BINRNUM: Cannot read catalog %s\n", bincat);
	return (0);
	}

    sysref = starcat->coorsys;
    eqref = starcat->equinox;
    epref = starcat->epoch;
    if (!sysout)
	sysout = sysref;
    if (!eqout)
	eqout = eqref;
    if (!epout)
	epout = epref;

    /* Allocate catalog entry buffer */
    star = (struct Star *) calloc (1, sizeof (struct Star));
    star->num = 0.0;
    if (starcat->entrv > 0) {
	nmag = starcat->nmag - 1;
	mrv = nmag;
	}
    else
	nmag = starcat->nmag;
    

    /* Loop through star list */
    for (jnum = 0; jnum < nnum; jnum++) {

	/* Find star in catalog */
	istar = (int) tnum[jnum];
	if (match) {
	    istar = 1;
	    while (istar <= starcat->nstars) {
		if (binstar (starcat, star, istar)) {
		    fprintf (stderr,"BINRNUM: Cannot read star %d\n", istar);
		    tra[jnum] = 0.0;
		    tdec[jnum] = 0.0;
		    tpra[jnum] = 0.0;
		    tpdec[jnum] = 0.0;
		    for (imag = 0; imag < starcat->nmag; imag++)
			tmag[imag][jnum] = 0.0;
		    tpeak[jnum] = 0;
		    continue;
		    }
		if (star->num == tnum[jnum])
		    break;
		istar++;
		}
	    if (star->num != tnum[jnum])
		continue;
	    }

	else if (binstar (starcat, star, istar)) {
	    fprintf (stderr,"BINRNUM: Cannot read star %d\n", istar);
	    tra[jnum] = 0.0;
	    tdec[jnum] = 0.0;
	    tpra[jnum] = 0.0;
	    tpdec[jnum] = 0.0;
	    for (imag = 0; imag < starcat->nmag; imag++)
		tmag[imag][jnum] = 0.0;
	    tpeak[jnum] = 0;
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

	/* Spectral Type */
	isp = (1000 * (int) star->isp[0]) + (int)star->isp[1];

	/* Save star position and magnitude in table */
	tnum[jnum] = num;
	tra[jnum] = ra;
	tdec[jnum] = dec;
	if (starcat->mprop == 1) {
	    tpra[jnum] = rapm;
	    tpdec[jnum] = decpm;
	    }

	/* Radial velocity, if present */
	if (starcat->entrv > 0)
	    tmag[mrv][nstar] = star->radvel;

	/* Magnitudes */
	if (nmag > 0) {
	    for (imag = 0; imag < nmag; imag++) {
		if (tmag[imag] != NULL)
		    tmag[imag][nstar] = star->xmag[imag];
		}
	    }

	tpeak[jnum] = isp;
	if (starcat->ncobj > 0 && tobj != NULL) {
	    lname = strlen (star->objname) + 1;
	    objname = (char *)calloc (lname, 1);
	    strcpy (objname, star->objname);
	    tobj[nstar] = objname;
	    }
	nstar++;
	if (nlog == 1) {
	    fprintf (stderr,"BINRNUM: %11.6f: %9.5f %9.5f", num, ra, dec);
	    for (imag = 0; imag < starcat->nmag; imag++)
		fprintf (stderr," %5.2f",tmag[imag][nstar]);
	    fprintf (stderr," %s  \n", star->isp);
	    }

	/* End of star loop */
	}

/* Summarize search */
    if (nlog > 0)
	fprintf (stderr,"BINRNUM: Catalog %s : %d / %d found\n",
		 bincat,nstar,starcat->nstars);

    binclose (starcat);
    free ((void *) star);
    return (nstar);
}


/* BINBIN -- Fill FITS WCS image with stars from binary catalog */

int
binbin (bincat, wcs, header, image, mag1, mag2, sortmag, magscale, nlog)

char	*bincat;	/* Name of reference star catalog file */
struct WorldCoor *wcs;	/* World coordinate system for image */
char	*header;	/* FITS header for output image */
char	*image;		/* Output FITS image */
double	mag1,mag2;	/* Limiting magnitudes (none if equal) */
int	sortmag;	/* Magnitude by which to sort (1 to nmag) */
double	magscale;	/* Scaling factor for magnitude to pixel flux
			 * (number of catalog objects per bin if 0) */
int	nlog;
{
    double cra;		/* Search center J2000 right ascension in degrees */
    double cdec;	/* Search center J2000 declination in degrees */
    double dra;		/* Search half width in right ascension in degrees */
    double ddec;	/* Search half-width in declination in degrees */
    int	sysout;		/* Search coordinate system */
    double eqout;	/* Search coordinate equinox */
    double epout;	/* Proper motion epoch (0.0 for no proper motion) */
    double rra1,rra2;	/* Limiting catalog right ascensions of region */
    double rdec1,rdec2;	/* Limiting catalog declinations of region */
    double ra1,ra2;	/* Limiting output right ascensions of region */
    double dec1,dec2;	/* Limiting output declinations of region */
    int sysref;		/* Catalog coordinate system */
    double eqref;	/* Catalog coordinate equinox */
    double epref;	/* Catalog position epoch */
    double secmarg = 60.0; /* Arcsec/century margin for proper motion */
    double ra, dec, rapm, decpm;
    double rra1a, rra2a;
    double rdist, ddist;
    struct StarCat *sc;	/* Star catalog data structure */
    struct Star *star;
    int wrap, iwrap, istar1,istar2;
    int ix, iy;
    int pass;
    int nmag;		/* Real number of magnitudes per entry (- rv) */
    int jstar;
    int nstar;
    double mag;
    double num;
    int magsort;
    int istar = 0;
    int verbose;
    int mrv;
    char cstr[16];
    double xpix, ypix, flux;
    int offscl;
    int bitpix, w, h;   /* Image bits/pixel and pixel width and height */
    double logt = log(10.0);

    if (nlog > 0)
	verbose = 1;
    else
	verbose = 0;

    /* Set image parameters */
    bitpix = 0;
    (void)hgeti4 (header, "BITPIX", &bitpix);
    w = 0;
    (void)hgeti4 (header, "NAXIS1", &w);
    h = 0;
    (void)hgeti4 (header, "NAXIS2", &h);

    /* Open catalog */
    sc = binopen (bincat);
    if (sc == NULL)
	return (0);

    if (sc->nstars <= 0) {
	binclose (sc);
	sc = NULL;
	return (0);
	}

    /* Keep mag1 the smallest magnitude */
    if (mag2 < mag1) {
	mag = mag2;
	mag2 = mag1;
	mag1 = mag;
	}

    if (sortmag > 0 && sortmag <= sc->nmag)
	magsort = sortmag - 1;
    else 
	magsort = 0;

    /* Logging interval */
    nstar = 0;

    /* Set catalog search limits from image WCS information */
    sysout = wcs->syswcs;
    eqout = wcs->equinox;
    epout = wcs->epoch;
    wcscstr (cstr, sysout, eqout, epout);
    wcssize (wcs, &cra, &cdec, &dra, &ddec);
    SearchLim (cra, cdec, dra, ddec, sysout, &ra1, &ra2, &dec1, &dec2, verbose);

    /* Make sure first declination is always the smallest one */
    if (dec1 > dec2) {
	dec = dec1;
	dec1 = dec2;
	dec2 = dec;
	}
  
    sysref = sc->coorsys;
    eqref = sc->equinox;
    epref = sc->epoch;
    RefLim (cra,cdec,dra,ddec,sysout,sysref,eqout,eqref,epout,epref,secmarg,
	    &rra1, &rra2, &rdec1, &rdec2, &wrap, verbose);

    if (verbose) {
	char rstr1[16],rstr2[16],dstr1[16],dstr2[16];
	ra2str (rstr1, 16, rra1, 3);
        dec2str (dstr1, 16, rdec1, 2);
	ra2str (rstr2, 16, rra2, 3);
        dec2str (dstr2, 16, rdec2, 2);

	wcscstr (cstr, sysref,eqref,epref);
	fprintf (stderr,"BINREAD: RA: %s - %s  Dec: %s - %s %s\n",
		 rstr1, rstr2, dstr1, dstr2, cstr);
	}

    /* If catalog RA range includes zero, split search in two */
    if (wrap) {
	rra1a = 0.0;
	rra2a = rra2;
	rra2 = 360.0;
	}
    else {
	rra2a = 0.0;
	}

    if (sc->entrv > 0) {
	nmag = sc->nmag - 1;
	mrv = nmag;
	}
    else
	nmag = sc->nmag;

    /* Allocate catalog entry buffer */
    star = (struct Star *) calloc (1, sizeof (struct Star));
    star->num = 0.0;

    jstar = 0;

    /* Loop through wraps (do not cross 360 degrees in search */
    for (iwrap = 0; iwrap <= wrap; iwrap++) {

	/* Set first and last stars to check */
	if (sc->rasorted) {
	    istar1 = binsra (sc, star, rra1);
	    istar2 = binsra (sc, star, rra2);
	    }
	else {
	    istar1 = sc->star1;
	    istar2 = sc->star0 + sc->nstars;
	    }
	if (verbose)
	    fprintf (stderr,"BINREAD: Searching stars %d through %d\n",istar1,istar2);

	/* Loop through catalog */
	for (istar = istar1; istar <= istar2; istar++) {
	    if (binstar (sc, star, istar)) {
		fprintf (stderr,"BINREAD: Cannot read star %d\n", istar);
		break;
		}

	    /* ID number */
	    num = star->num;

	    /* Magnitude */
	    if (sc->entmag[0] > 0)
		mag = star->xmag[magsort];

	    /* Check magnitude limits */
	    pass = 1;
	    if (mag1 != mag2 && (mag < mag1 || mag > mag2))
		pass = 0;

	    /* Get position in output coordinate system, equinox, and epoch */
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
		    fprintf (stderr,"BINBIN: %11.6f: %9.5f %9.5f %s",
			     num,ra,dec,cstr);
		    if (magscale > 0.0)
			fprintf (stderr, " %5.2f", mag);
		    if (!offscl)
			flux = getpix1 (image, bitpix, w, h, 0.0, 1.0, ix, iy);
		    else
			flux = 0.0;
		    fprintf (stderr," (%d,%d): %f\n", ix, iy, flux);
		    }
		if (nlog == 1)
		    fprintf (stderr,"BINREAD: %11.6f: %9.5f %9.5f %5.2f\n",
			   num,ra,dec,mag);

	    /* End of accepted star processing */
		}

	/* Log operation */
	    if (nlog > 0 && istar%nlog == 0)
		fprintf (stderr,"BINREAD: %5d / %5d / %5d sources catalog %s\r",
			jstar,istar,sc->nstars,bincat);

	/* End of star loop */
	    }

	/* Set second set of RA limits if passing through 0h */
	rra1 = rra1a;
	rra2 = rra2a;
	}

    /* Summarize search */
    if (nlog > 0) {
	fprintf (stderr,"BINREAD: Catalog %s : %d / %d / %d found\n",
		 bincat,jstar,istar,sc->nstars);
	}

    free ((void *)star);
    free ((void *)tdist);
    return (nstar);
}


/* BINOPEN -- Open binary catalog, returning number of entries */

struct StarCat *
binopen (bincat)

char *bincat;	/* Binary catalog file name */
{
    int fcat;
    struct StarCat *sc;
    int nr, lfile;
    char *binfile;
    char binpath[128];	/* Full pathname for catalog file */
    int lf, nb, binset;
    char *str;

    sc = (struct StarCat *) calloc (1, sizeof (struct StarCat));

    /* Find length of binary catalog, if in working directory */
    lfile = binsize (bincat);

    /* Set catalog directory; if pathname is a URL, return */
    binset = 0;
    if (!strncasecmp (bincat,"PPM",3)) {
	if ((str = getenv("PPM_PATH")) != NULL ) {
	    if (!strncmp (str, "http:",5)) {
		sc->caturl = str;
		}
	    else if (strlen (str) < 64) {
		strcpy (bindir, str);
		binset = 1;
		}
            }
	}
    else if (!strncasecmp (bincat,"SAO",3)) {
	if ((str = getenv("SAO_PATH")) != NULL ) {
	    if (!strncmp (str, "http:",5)) {
		sc->caturl = str;
		}
	    else if (strlen (str) < 64) {
		strcpy (bindir, str);
		binset = 1;
		}
            }
	}
    else if (!strncasecmp (bincat,"SKY2K",3)) {
	if ((str = getenv("SKY2K_PATH")) != NULL ) {
	    if (!strncmp (str, "http:",5)) {
		sc->caturl = str;
		}
	    else if (strlen (str) < 64) {
		strcpy (bindir, str);
		binset = 1;
		}
            }
	}
    else if (!strncasecmp (bincat,"HIP",3)) {
	if ((str = getenv("HIP_PATH")) != NULL ) {
	    if (!strncmp (str, "http:",5)) {
		sc->caturl = str;
		}
	    else if (strlen (str) < 64) {
		strcpy (bindir, str);
		binset = 1;
		}
            }
	}
    else if (!strncasecmp (bincat,"IRAS",4)) {
	if ((str = getenv("IRAS_PATH")) != NULL ) {
	    if (!strncmp (str, "http:",5)) {
		sc->caturl = str;
		}
	    else if (strlen (str) < 64) {
		strcpy (bindir, str);
		binset = 1;
		}
            }
	}
    if (!binset && (str = getenv("WCS_BINDIR")) != NULL ) {
	if (!strncmp (str, "http:",5)) {
	    sc->caturl = str;
            }
	else if (strlen (str) < 64)
	    strcpy (bindir, str);
	}

    /* Set up catalog information and return if over web */
    if (sc->caturl != NULL) {
	sc->coorsys = 0;
	sc->epoch = 0.0;
	sc->equinox = 0.0;
	if (!strncasecmp (bincat, "sao", 3) ||
	    !strncasecmp (bincat, "ppm", 3) ||
	    !strncasecmp (bincat, "sky2k", 3) ||
	    !strncasecmp (bincat, "hip", 3))
	   sc->mprop = 1;
	else
	   sc->mprop = 0;
	return (sc);
	}

    /* Prepend directory name file not in working directory */
    if (lfile < 2) {

	strcpy (binpath, bindir);
	strcat (binpath, "/");
	strcat (binpath, bincat);
	lfile = binsize (binpath);
	if (lfile < 2) {
	    fprintf (stderr,"BINOPEN: Binary catalog %s has no entries\n",bincat);
	    return (NULL);
	    }
	}
    else
	strcpy (binpath, bincat);

    /* Open binary catalog */
    if ((fcat = open (binpath, O_RDONLY+O_BINARY)) < 3) {
	fprintf (stderr,"BINOPEN: Binary catalog %s cannot be read\n",binpath);
	free (sc);
	return (NULL);
	}

    /* Read binary catalog header information */
    nr = (int) read (fcat, sc, 28);
    if (nr < 28) {
	fprintf (stderr,"BINOPEN: read only %d / %d bytes of file %s\n",
		 nr, lfile, binpath);
	(void) close (fcat);
	return (NULL);
	}

    /* Check for byte reversal */
    if (sc->nbent > 80) {
	sc->byteswapped = 1;
	binswap4 (&sc->star0);
	binswap4 (&sc->star1);
	binswap4 (&sc->nstars);
	binswap4 (&sc->stnum);
	binswap4 (&sc->mprop);
	binswap4 (&sc->nmag);
	binswap4 (&sc->nbent);
	}
    else
	sc->byteswapped = 0;

    /* Allocate buffer to read one line of catalog */
    sc->catline = (char *) calloc (sc->nbent, sizeof (char));

    nb = 0;
    sc->ncobj = 0;
    if (sc->stnum < 0)
	sc->ncobj = -sc->stnum;
    else if (sc->stnum > 0)	
	nb = 4;

    sc->entra = nb;
    sc->entdec = nb + 8;
    sc->entpeak = nb + 16;

    /* Set other catalog information in structure */
    if (sc->nmag < 0) {
	sc->inform = 'J';
	sc->coorsys = WCS_J2000;
	sc->epoch = 2000.0;
	sc->equinox = 2000.0;
	sc->nmag = -sc->nmag;
	}
    else if (sc->nstars < 0) {
	sc->inform = 'J';
	sc->coorsys = WCS_J2000;
	sc->epoch = 2000.0;
	sc->equinox = 2000.0;
	sc->nstars = -sc->nstars;
	}
    else {
	sc->inform = 'B';
	sc->coorsys = WCS_B1950;
	sc->epoch = 1950.0;
	sc->equinox = 1950.0;
	}

    if (sc->nmag > 0)
	sc->entmag[0] = nb + 18;
    else
	sc->entmag[0] = 0;
    nb = nb + 18 + (sc->nmag * 2);
    if (sc->mprop == -1)
	sc->mprop = 1;
    if (sc->mprop == 1) {
	sc->entrpm = nb;
	sc->entdpm = nb + 4;
	nb = nb + 8;
	}
    else if (sc->mprop == 2) {
	sc->entrv = nb;
	nb = nb + 8;
	}
    if (sc->ncobj)
	sc->entname = nb;

    /* Set number of decimal places in star numbers */
    if (sc->stnum == 2)
	sc->nndec = 4;
    else if (sc->stnum == 3)
	sc->nndec = 5;
    else
	sc->nndec = 0;

    strcpy (sc->incdir, bindir);
    strcpy (sc->incfile, bincat);

    /* Separate filename from pathname and save in structure */
    binfile = strrchr (binpath,'/');
    if (binfile)
	binfile = binfile + 1;
    else
	binfile = binpath;
    if (strlen (binfile) < 24)
	strcpy (sc->isfil, binfile);
    else
	strncpy (sc->isfil, binfile, 23);

    sc->entadd = fcat;
    sc->sptype = 1;
    if (sc->mprop == 2)
	sc->nmag = sc->nmag + 1;

    /* Check name to see if file is RA-sorted */
    lf = strlen (binfile);
    sc->rasorted = 0;
    if (binfile[lf-2] == 'r' && binfile[lf-1] == 'a')
	sc->rasorted = 1;
    if (!strncmp (binfile, "IRAS", 4))
	sc->rasorted = 1;

    sc->refcat = BINCAT;
    return (sc);
}


void
binclose (sc)
struct StarCat *sc;	/* Star catalog descriptor */
{
    close (sc->entadd);
    free ((void *)sc->catline);
    free ((void *)sc);
    return;
}


/* BINSRA -- Find star closest to given RA in RA-sorted catalog */

static int
binsra (sc, st, dra)

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
    if (binstar (sc, st, ismin)) {
	fprintf (stderr,"BINSRA: Cannot read star %d\n", ismin);
	return (0);
	}
    else
	ramin = st->ra;

    ismax = sc->nstars;
    if (binstar (sc, st, ismax)) {
	fprintf (stderr,"BINSRA: Cannot read star %d\n", ismax);
	return (0);
	}
    else
	ramax = st->ra;

    istarx = sc->nstars / 2;

    for (nrep = 0; nrep < 32; nrep++) {
	if (binstar (sc, st, istarx)) {
	    fprintf (stderr,"BINSRA: Cannot read star %d\n", istarx);
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
    if (binstar (sc, st, istarx)) {
	fprintf (stderr,"BINSRA: Cannot read star %d\n", istarx);
        return (0);
	}
    else
	return (istarx);
}


/* BINSTAR -- Get binary catalog entry for one star;
              return 0 if successful */

int
binstar (sc, st, istar)

struct StarCat *sc;	/* Star catalog descriptor */
struct Star *st;	/* Current star entry */
int istar;	/* Star sequence number in binary catalog */
{
    int ino, i, nmag;
    long offset;
    double radvel;
    float pm[2];

    /* Drop out if catalog pointer is not set */
    if (sc == NULL)
	return (1);

    /* Drop out if catalog is not open */
    if (sc->entadd < 3)
	return (2);

    /* Drop out if star number is too large */
    if (istar > sc->nstars) {
	fprintf (stderr, "BINSTAR:  %d  > %d is not in catalog\n",
		 istar, sc->nstars);
	return (3);
	}

    /* Move file pointer to start of correct star entry */
    if (istar > 0) {
	offset = 28 + (istar - sc->star1) * sc->nbent;
	if (lseek (sc->entadd, offset, SEEK_SET) < offset)
	    return (0);
	}

    /* Read catalog entry */
    if ((int)read (sc->entadd, sc->catline, sc->nbent) < 1)
	return (4);

    /* Read catalog number or object name */
    sc->ncobj = 0;
    if (sc->stnum <= 0) {
	sc->ncobj = -sc->stnum;
	movebuff (sc->catline, st->objname, sc->ncobj, sc->entname, 0);
	}
    else {
	movebuff (sc->catline, (char *) &st->xno, 4, 0, 0);
	if (sc->byteswapped)
	    binswap4 (&st->xno);
	}

    /* Interpret catalog number */
    switch (sc->stnum) {
	case 1:
	    st->num = (double) st->xno;
	    break;
	case 2:
	    bcopy ((char *)&st->xno, (char *) &ino, 4);
	    st->num = 0.0001 * (double) ino;
	    break;
	case 3:
	    bcopy ((char *)&st->xno, (char *) &ino, 4);
	    st->num = 0.00001 * (double) ino;
	    break;
	case 4:
	    bcopy ((char *)&st->xno, (char *) &ino, 4);
	    st->num = (double) ino;
	    break;
	default:
	    if (istar > 0)
		st->num = (double) istar;
	    else
		st->num = st->num + 1.0;
	    break;
	}

    /* Right ascension and declination and convert to degrees */
    movebuff (sc->catline, (char *) &st->ra, 8, sc->entra, 0);
    movebuff (sc->catline, (char *) &st->dec, 8, sc->entdec, 0);
    if (sc->byteswapped) {
	binswap8 (&st->ra);
	binswap8 (&st->dec);
	}
    st->ra = raddeg (st->ra);
    st->dec = raddeg (st->dec);

    /* Proper motion, if present, and convert to degrees/year */
    nmag = sc->nmag;
    if (sc->mprop == 1) {
	movebuff (sc->catline, (char *) &pm[0], 4, sc->entrpm, 0);
	movebuff (sc->catline, (char *) &pm[1], 4, sc->entdpm, 0);
	if (sc->byteswapped) {
	    binswap4 (&pm[0]);
	    binswap4 (&pm[1]);
	    }
	st->rapm = raddeg ((double) pm[0]);
	st->decpm = raddeg ((double) pm[1]);
	}

    /* Radial velocity, if it is present */
    else if (sc->mprop == 2) {
	movebuff (sc->catline, (char *) &radvel, 8, sc->entrv, 0);
	if (sc->byteswapped)
	    binswap8 (&radvel);
	st->radvel = radvel;
	nmag = nmag - 1;
	}

    /* Spectral type */
    movebuff (sc->catline, (char *) st->isp, 2, sc->entpeak, 0);

    /* Magnitudes */
    if (sc->entmag[0] > 0) {
	for (i = 0; i < nmag; i++) {
	    movebuff (sc->catline, (char *) st->mag, 2, sc->entmag[0]+(i*2), i*2);
	    if (sc->byteswapped)
		binswap2 (&st->mag[i], 2);
	    st->xmag[i] = 0.01 * (double) st->mag[i];
	    }
	}
    return (0);
}


/* BINSWAP2 -- Swap bytes in string in place */

static void
binswap2 (string,nbytes)


char *string;	/* Address of starting point of bytes to swap */
int nbytes;	/* Number of bytes to swap */

{
    char *sbyte, temp, *slast;

    slast = string + nbytes;
    sbyte = string;
    while (sbyte < slast) {
	temp = sbyte[0];
	sbyte[0] = sbyte[1];
	sbyte[1] = temp;
	sbyte= sbyte + 2;
	}
    return;
}


/* BINSWAP4 -- Reverse bytes of Integer*4 or Real*4 number in place */

static void
binswap4 (string)

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


/* BINSWAP8 -- Reverse bytes of Real*8 vector in place */

static void
binswap8 (string)

char *string;	/* Address of Real*8 vector */

{
    char temp[8];

    temp[7] = string[0];
    temp[6] = string[1];
    temp[5] = string[2];
    temp[4] = string[3];
    temp[3] = string[4];
    temp[2] = string[5];
    temp[1] = string[6];
    temp[0] = string[7];
    string[0] = temp[0];
    string[1] = temp[1];
    string[2] = temp[2];
    string[3] = temp[3];
    string[4] = temp[4];
    string[5] = temp[5];
    string[6] = temp[6];
    string[7] = temp[7];
    return;
}


/* BINSIZE -- return size of binary catalog file in bytes */

static int
binsize (filename)

char	*filename;	/* Name of file for which to find size */
{
    FILE *diskfile;
    long filesize;

    /* Open file */
    if ((diskfile = fopen (filename, "rb")) == NULL)
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


/* ISBIN -- Return 1 if TDC binary catalog file, else 0 */

int
isbin (filename)

char    *filename;      /* Name of file to check */
{
    FILE *diskfile;
    char line[8];
    int nbr;

    if ((diskfile = fopen (filename, "rb")) == NULL)
	return (0);
    else {
	nbr = fread (line, 1, 4, diskfile);
	fclose (diskfile);
	if (nbr < 4)
	    return (0);
	else if (line[0] == 0 || line[1] == 0 || line[2] == 0 || line[3] == 0)
	    return (1);
	else
	    return (0);
	}
}

/* Sep 10 1998	New subroutines
 * Sep 15 1998	Add byte swapping
 * Sep 16 1998	Use limiting radius correctly; use arbitrary search system
 * Sep 21 1998	Return converted coordinates
 * Sep 23 1998	Set search limits in catalog system, but test in output system
 * Sep 24 1998	Return second magnitude if more than one in catalog
 * Oct 15 1998	Add binsize() to compute file length in bytes
 * Oct 20 1998	Add binrobj() to read object names for specified stars
 * Oct 21 1998	Fix binsra() to get out of 3-in-a-row identical RAs
 * Oct 21 1998	Add isbin() to check whether file could be TDC binary catalog
 * Oct 26 1998	Add object retrieval to binread() and binrnum()
 * Oct 29 1998	Correctly assign numbers when too many stars are found
 * Oct 30 1998	Be more careful with coordinate system of catalog
 * Oct 30 1998	Fix convergence at edges of catalog
 * Nov  9 1998	Add flag to catalog structure for RA-sorted catalogs
 * Dec  8 1998	Have binstar() return 0 instead of null
 *
 * Feb  1 1999	Add match argument to binrnum() 
 * Feb  2 1999	Set number of decimal places in star number
 * Feb 11 1999	Change starcat.insys to starcat.coorsys
 * Jun 16 1999	Use SearchLim()
 * Aug 16 1999	Add RefLim() to get converted search coordinates right
 * Aug 24 1999	Fix declination limit bug which broke search 
 * Aug 25 1999	Return real number of stars from binread()
 * Sep 16 1999	Fix bug which didn't always return closest stars
 * Sep 16 1999	Add distsort argument so brightest stars in circle works, too
 * Oct 21 1999	Fix declarations after lint
 *
 * Mar 15 2000	Add proper motion return to binread() and binrnum()
 * Mar 28 2000	Use moveb() to extract information from entries
 * Mar 30 2000	Use standard i/O instead of stream I/O
 * Jun  2 2000	Minor changes after lint
 * Jun 26 2000	Add coordinate system to SearchLim() arguments
 * Jul 12 2000	Add star catalog structure to binread() argument list
 * Jul 25 2000	Pass star catalog address of data structure address
 * Sep 25 2000	Set sc->sptype to 1 to indicate presence of spectral type
 * Nov 29 2000	Add option to read catalogs using HTTP
 * Dec  1 2000	Add separate paths for SAO and PPM catalogs
 * Dec 18 2000	Drop unused variable str in binopen()
 *
 * Jan 11 2001	Print all messages to stderr
 * Feb 14 2001	Correctly full-circle limits at poles
 * Mar 19 2001	Check WCS_BINDIR if PPM_PATH or SAO_PATH not set in binread()
 * Mar 22 2001	Move path environment variable reading to binopen()
 * Mar 23 2001	Set system, equinox, and epoch to 0 in binopen if using Web
 * Jun 20 2001	Save all four magnitudes if present, dropping spectral type
 * Jun 22 2001	Do not swap spectral type in binstar()
 * Jun 25 2001	Add HIP_PATH and IRAS_PATH; allow 5 digits for magnitudes
 * Jun 27 2001	Allocate separation array only when larger one is needed
 * Jul  6 2001	Fix bug in dealing with proper motions of excess stars
 * Jul 17 2001	Fix bug which prevented use of full catalog pathname on CL
 * Aug  8 2001	Add radial velocity if mprop is 2; return as magnitude
 * Sep 11 2001	Pass array of magnitude vectors to avoid kludges
 * Sep 11 2001	Add sort magnitude argument
 * Sep 18 2001	Fix magnitude number in binrnum()
 * Sep 19 2001	Drop fitshead.h; it is in wcs.h
 *
 * Mar 25 2002	Fix bugs dealing with radial velocity
 * Mar 26 2002	Don't set object name unless header says it is there
 * Aug  6 2002	Make sc->entmag into vector, but only use first position
 *
 * Feb  4 2003	Open catalog file rb instead of r (Martin Ploner, Bern)
 * Feb 19 2003	Increase maximum nbent from 50 to 80
 * Feb 20 2003	Eliminate unused variables after lint
 * Mar 11 2003	Upgrade position testing
 * Mar 11 2003	If proper motion flag is -1, reset it to 1
 * Mar 26 2003	Open binpath with binary flag set
 * Aug 22 2003	Add radi argument for inner edge of search annulus
 * Sep 25 2003	Add binbin() to fill an image with sources
 * Oct  6 2003	Update binread() and binbin() for improved RefLim()
 * Nov 18 2003	Initialize image size and bits/pixel from header in binbin()
 * Dec 12 2003	Fix bug in wcs2pix() call in binbin()
 *
 * Aug 27 2004	Include math.h
 *
 * Jan 19 2006	Fix bug when J2000 system set by negative number of magnitudes
 * Jun 20 2006	Initialize uninitialized variables
 * Sep 26 2006	Increase length of rastr and destr from 16 to 32
 * Nov 15 2006	Fix binning
 *
 * Jan  8 2007	Drop unused variables in binbin()
 * Jan 10 2007	Add match=1 argument to webrnum()
 * Nov 28 2007	Move moveb() to catutil.c
 *
 * Sep 25 2009	Call movebuff() instead of moveb() and move mvebuff() to catutil.c
 */
