/*** File libwcs/tabread.c
 *** September 30, 2009
 *** By Doug Mink, dmink@cfa.harvard.edu
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

/* int tabread()	Read tab table stars in specified region
 * int tabrnum()	Read tab table stars with specified numbers
 * int tabbin ()	Bin tab table stars in specified region into an image
 * int tabxyread()	Read x, y, and magnitude from tab table star list
 * int tabrkey()	Read single keyword from specified tab table stars
 * struct StarCat tabcatopen()	Open tab table catalog, return number of entries
 * struct TabTable *tabopen()	Open tab table, returning number of entries
 * char *tabline()	Get tab table entry for one line
 * double tabgetra()	Return double right ascension in degrees
 * double tabgetdec()	Return double declination in degrees
 * double tabgetr8()	Return 8-byte floating point number from tab table line
 * int tabgeti4()	Return 4-byte integer from tab table line
 * int tabgetk()	Return character entry from tab table line for column
 * int tabgetc()	Return n'th character entry from tab table line
 * int tabhgetr8()	Return 8-byte floating point keyword value from header
 * int tabhgeti4()	Return 4-byte integer keyword value from header
 * int tabhgetc()	Return character keyword value from header
 * int tabparse()	Make a table of column headings
 * int tabcol()		Find entry in a table of column headings (case-dependent)
 * int tabccol()	Find entry in a table of column headings (case-independent)
 * int tabsize()	Return length of file in bytes
 * int istab()		Return 1 if first line of file contains a tab, else 0
 */

#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <math.h>
#include <sys/types.h>
#include "wcs.h"
#include "fitsfile.h"
#include "wcscat.h"

#define ABS(a) ((a) < 0 ? (-(a)) : (a))

static int tabhgetr8();
static int tabhgeti4();
static int tabhgetc();
static int tabcont();
static int tabccont();
static int tabsize();
static int nndec = 0;
static int verbose = 0;
static char *taberr;
static struct Tokens startok;

char *gettaberr ()
{ return (taberr); }

int gettabndec()
{ return (nndec); }

static char *kwo = NULL;	/* Keyword returned by tabread(), tabrnum() */
void settabkey (keyword0)
char *keyword0;
{ kwo = keyword0; return; }


/* TABREAD -- Read tab table stars in specified region */

int
tabread (tabcatname,distsort,cra,cdec,dra,ddec,drad,dradi,
	 sysout,eqout,epout,mag1,mag2,sortmag,nstarmax,starcat,
	 tnum,tra,tdec,tpra,tpdec,tmag,tpeak,tkey,nlog)

char	*tabcatname;	/* Name of reference star catalog file */
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
int	nstarmax;	/* Maximum number of stars to be returned */
struct StarCat **starcat; /* Star catalog data structure */
double	*tnum;		/* Array of UJ numbers (returned) */
double	*tra;		/* Array of right ascensions (returned) */
double	*tdec;		/* Array of declinations (returned) */
double	*tpra;		/* Array of right ascension proper motions (returned) */
double	*tpdec;		/* Array of declination proper motions (returned) */
double	**tmag;		/* 2-D Array of magnitudes (returned) */
int	*tpeak;		/* Array of peak counts (returned) */
char	**tkey;		/* Array of values of additional keyword */
int	nlog;
{
    double ra1,ra2;	/* Limiting right ascensions of region in degrees */
    double dec1,dec2;	/* Limiting declinations of region in degrees */
    double rra1,rra2;	/* Catalog coordinate limiting right ascension (deg) */
    double rdec1,rdec2;	/* Catalog coordinate limiting declination (deg) */
    double dist = 0.0;  /* Distance from search center in degrees */
    double faintmag=0.0; /* Faintest magnitude */
    double maxdist=0.0; /* Largest distance */
    int faintstar=0;    /* Faintest star */
    int farstar=0;      /* Most distant star */
    double *tdist;      /* Array of distances to stars */
    int sysref;		/* Catalog coordinate system */
    double eqref;	/* Catalog equinox */
    double epref;	/* Catalog epoch */
    double secmarg = 0.0; /* Arcsec/century margin for proper motion */
    double magt;
    double rdist, ddist;
    int pass;
    char cstr[32];
    struct Star *star;
    struct StarCat *sc;	/* Star catalog data structure */

    int wrap;
    int jstar;
    int magsort;
    int nstar;
    char *objname;
    int lname;
    int imag;
    double ra,dec, rapm, decpm;
    double mag, parallax, rv;
    double num;
    int peak, i;
    int istar, nstars, lstar;

    sc = *starcat;

    if (nlog > 0)
	verbose = 1;
    else
	verbose = 0;

    SearchLim (cra,cdec,dra,ddec,sysout,&ra1,&ra2,&dec1,&dec2,verbose);

    /* mag1 is always the smallest magnitude limit */
    if (mag2 < mag1) {
	mag = mag2;
	mag2 = mag1;
	mag1 = mag;
	}

    /* Logging interval */
    nstar = 0;
    tdist = (double *) calloc (nstarmax, sizeof (double));

    lstar = sizeof (struct Star);
    star = (struct Star *) calloc (1, lstar);
    if (sc == NULL)
	sc = tabcatopen (tabcatname, NULL, 0);
    *starcat = sc;
    if (sc == NULL || sc->nstars <= 0) {
	if (taberr != NULL)
	    fprintf (stderr,"%s\n", taberr);
	fprintf (stderr,"TABREAD: Cannot read catalog %s\n", tabcatname);
	free (star);
	sc = NULL;
	return (0);
	}

    nstars = sc->nstars;
    jstar = 0;

    if (sortmag > 0 && sortmag <= sc->nmag)
	magsort = sortmag - 1;
    else 
	magsort = 0;

    /* Set catalog coordinate system */
    if (sc->equinox != 0.0)
	eqref = sc->equinox;
    else
	eqref = eqout;
    if (sc->epoch != 0.0)
	epref = sc->epoch;
    else
	epref = epout;
    if (sc->coorsys)
	sysref = sc->coorsys;
    else
	sysref = sysout;
    wcscstr (cstr, sysout, eqout, epout);

    rra1 = ra1;
    rra2 = ra2;
    rdec1 = dec1;
    rdec2 = dec2;
    if (sc->mprop > 0)
	secmarg = 60.0;
    RefLim (cra,cdec,dra,ddec,sysout,sysref,eqout,eqref,epout,epref,secmarg,
	    &rra1, &rra2, &rdec1, &rdec2, &wrap, verbose);

    /* Loop through catalog */
    for (istar = 1; istar <= nstars; istar++) {

	/* Read position of next star */
	if (tabstar (istar, sc, star, verbose)) {
	    if (verbose)
		fprintf (stderr,"TABREAD: Cannot read star %d\n", istar);
	    break;
	    }

	/* Set magnitude to test */
	if (sc->nmag > 0) {
	    magt = star->xmag[magsort];
	    if (sortmag < 1) {
		imag = 0;
		while (magt == 99.90 && imag < sc->nmag)
		    magt = star->xmag[imag++];
		if (magt > 100.0)
		    magt = magt - 100.0;
		}
	    }
	else
	    magt = mag1;

	/* Check magnitude limits */
	pass = 1;
	if (mag1 != mag2 && (magt < mag1 || magt > mag2))
	    pass = 0;

	/* Check rough position limits */
	ra = star->ra;
	dec = star->dec;
	if  ((!wrap && (ra < rra1 || ra > rra2)) ||
	    (wrap && (ra < rra1 && ra > rra2)) ||
	    dec < rdec1 || dec > rdec2)
	    pass = 0;

	/* Convert coordinate system for this star and test it*/
	if (pass) {
	    sysref = star->coorsys;
	    eqref = star->equinox;
	    epref = star->epoch;

	    /* Extract selected fields  */
	    num = star->num;
	    rapm = star->rapm;
	    decpm = star->decpm;
	    parallax = star->parallax;
	    rv = star->radvel;

	    /* Convert from catalog to search coordinate system */
	    if (sc->entpx || sc->entrv)
		wcsconv (sysref, sysout, eqref, eqout, epref, epout,
		     &ra, &dec, &rapm, &decpm, &parallax, &rv);
	    else if (sc->mprop == 1)
		wcsconp (sysref, sysout, eqref, eqout, epref, epout,
		     &ra, &dec, &rapm, &decpm);
	    else
		wcscon (sysref, sysout, eqref, eqout, &ra, &dec, epout);
	    if (sc->sptype)
		peak = (1000 * (int) star->isp[0]) + (int)star->isp[1];
	    else
		peak = star->peak;

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

	/* Add this star's information to the list if still OK */
	if (pass) {

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
		if (tpeak)
		    tpeak[nstar] = peak;
		tdist[nstar] = dist;
		lname = strlen (star->objname);
		if (lname > 0) {
		    objname = (char *)calloc (lname+1, 1);
		    strcpy (objname, star->objname);
		    if (tkey[nstar]) free(tkey[nstar]);
		    tkey[nstar] = objname;
		    }
		if (dist > maxdist) {
		    maxdist = dist;
		    farstar = nstar;
		    }
		if (sc->nmag > 0 && magt > faintmag) {
		    faintmag = magt;
		    faintstar = nstar;
		    }
		}

	    /* If radial search & too many stars, replace furthest star */
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
		    tpeak[farstar] = peak;
		    tdist[farstar] = dist;
		    lname = strlen (star->objname);
		    if (lname > 0) {
			objname = (char *)calloc (lname+1, 1);
			strcpy (objname, star->objname);
			if (tkey[farstar]) free(tkey[farstar]);
			tkey[farstar] = objname;
			}

		    /* Find new farthest star */
		    maxdist = 0.0;
		    for (i = 0; i < nstarmax; i++) {
			if (tdist[i] > maxdist) {
			    maxdist = tdist[i];
			    farstar = i;
			    }
			}
		    }
		}

	    /* Otherwise if too many stars, replace faintest star */
	    else if (sc->nmag > 0 && magt < faintmag) {
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
		tpeak[faintstar] = peak;
		tdist[faintstar] = dist;
		lname = strlen (star->objname);
		if (lname > 0) {
		    objname = (char *)calloc (lname+1, 1);
		    strcpy (objname, star->objname);
		    if (tkey[faintstar]) free(tkey[faintstar]);
		    tkey[faintstar] = objname;
		    }
		faintmag = 0.0;

		/* Find new faintest star */
		for (i = 0; i < nstarmax; i++) {
		    magt = tmag[magsort][i];
		    imag = 0;
		    while (magt == 99.90 && imag < sc->nmag)
			magt = tmag[imag++][i];
		    if (magt > 100.0)
			magt = magt - 100.0;
		    if (magt > faintmag) {
			faintmag = magt;
			faintstar = i;
			}
		    }
		}
		
	    nstar++;
	    jstar++;
	    if (nlog == 1)
		fprintf (stderr,"TABREAD: %11.6f: %9.5f %9.5f %s %5.2f %d    \n",
			 num,ra,dec,cstr,magt,peak);

	    /* End of accepted star processing */
	    }

	/* Log operation */
	if (nlog > 0 && istar%nlog == 0)
		fprintf (stderr,"TABREAD: %5d / %5d / %5d sources catalog %s\r",
			jstar,istar,nstars,tabcatname);

	/* End of star loop */
	}

    /* Summarize search */
    if (nlog > 0) {
	fprintf (stderr,"TABREAD: Catalog %s : %d / %d / %d found\n",tabcatname,
		 jstar,istar,nstars);
	if (nstar > nstarmax)
	    fprintf (stderr,"TABREAD: %d stars found; only %d returned\n",
		     nstar,nstarmax);
	}

    free ((char *) tdist);
    free ((char *) star);
    return (nstar);
}


/* TABRNUM -- Read tab table stars with specified numbers */

int
tabrnum (tabcatname, nnum, sysout, eqout, epout, starcat, match,
	 tnum,tra,tdec,tpra,tpdec,tmag,tpeak,tkey,nlog)

char	*tabcatname;	/* Name of reference star catalog file */
int	nnum;		/* Number of stars to look for */
int	sysout;		/* Search coordinate system */
double	eqout;		/* Search coordinate equinox */
double	epout;		/* Proper motion epoch (0.0 for no proper motion) */
struct StarCat **starcat; /* Star catalog data structure */
int	match;		/* 1 to match star number exactly, else sequence num.*/
double	*tnum;		/* Array of star numbers to look for */
double	*tra;		/* Array of right ascensions (returned) */
double	*tdec;		/* Array of declinations (returned) */
double	*tpra;		/* Array of right ascension proper motions (returned) */
double	*tpdec;		/* Array of declination proper motions (returned) */
double	**tmag;		/* 2-D array of magnitudes (returned) */
int	*tpeak;		/* Array of peak counts (returned) */
char	**tkey;		/* Array of additional keyword values */
int	nlog;
{
    int jnum;
    int nstar;
    double ra,dec, rapm, decpm;
    double mag, parallax, rv;
    double num;
    int peak;
    int istar, istar0, nstars;
    int imag;
    char *line;
    char numstr[32];	/* Catalog number */
    int sysref;		/* Catalog coordinate system */
    double eqref;	/* Catalog equinox */
    double epref;	/* Catalog epoch */
    char cstr[32];
    char str[32];
    char *objname;
    char *datestring;
    int lname, lstar;
    int ireg, inum, nnfld, i;
    char rastr[32], decstr[32];
    struct TabTable *startab;
    struct StarCat *sc;
    struct Star *star;

    line = NULL;
    nnfld = 0;

    nstar = 0;
    nndec = 0;

    /* Allocate catalog entry buffer */
    lstar = sizeof (struct Star);
    star = (struct Star *) calloc (1, lstar);

    /* Open star catalog */
    sc = *starcat;
    if (sc == NULL)
	sc = tabcatopen (tabcatname, NULL, 0);
    *starcat = sc;
    if (sc == NULL || sc->nstars <= 0) {
	if (taberr != NULL)
	    fprintf (stderr,"%s\n", taberr);
	fprintf (stderr,"TABRNUM: Cannot read catalog %s\n", tabcatname);
	free (star);
	return (0);
	}
    startab = sc->startab;
    nstars = sc->nstars;

    /* Set catalog coordinate system */
    if (sc->equinox != 0.0)
	eqref = sc->equinox;
    else
	eqref = eqout;
    if (sc->epoch != 0.0)
	epref = sc->epoch;
    else
	epref = epout;
    if (sc->coorsys)
	sysref = sc->coorsys;
    else
	sysref = sysout;
    wcscstr (cstr, sysout, eqout, epout);

    /* Write header if printing star entries as found */
    if (nlog < 0) {
	char *revmessage;
	revmessage = getrevmsg();
	printf ("catalog	%s\n", tabcatname);
	printf ("radecsys	%s\n", cstr);
	printf ("equinox	%.3f\n", eqout);
	printf ("epoch  	%.3f\n", epout);
	printf ("program	scat %s\n", revmessage);
	if (sc->nnfld > 0)
	    nnfld = sc->nnfld;
	else
	    nnfld = CatNumLen (TABCAT, tnum[nnum-1], sc->nndec);
	printf ("id   	ra          	dec        ");
	for (i = 1; i < sc->nmag+1; i++) {
	    if (i == sc->nmag && sc->entepoch)
		printf ("	epoch  	");
	    else
		printf ("	%s", sc->keymag[i-1]);
	    }
	if (kwo != NULL)
	    printf ("	%s\n", kwo);
	else
	    printf ("\n");
	printf ("-----	------------	------------");
	for (i = 1; i < sc->nmag+1; i++) {
	    if (i == sc->nmag && sc->entepoch)
		printf ("	--------");
	    else
		printf ("	-----");
	    }
	if (kwo != NULL)
	    printf ("	---------\n");
	else
	    printf ("\n");
	}

    star->num = 0.0;
    istar0 = 0;
    num = 0.0;

    /* Loop through star list */
    line = startab->tabdata;
    for (jnum = 0; jnum < nnum; jnum++) {

	/* Read forward from the last star if possible */
	inum = (int) (tnum[jnum] + 0.5);
	if ((double)inum != tnum[jnum] || inum < istar0 || istar0 == 0) {
	    istar0 = 1;
	    }

	/* Loop through catalog to star */
	for (istar = istar0; istar <= nstars; istar++) {
	    if (!match && istar == inum)
		break;
	    if (num < tnum[jnum]) {
		if ((line = gettabline (startab, istar)) == NULL) {
		    if (nlog)
			fprintf (stderr,"TABRNUM: Cannot read star %d\n", istar);
		    break;
		    }
		}

	    /* Check ID number first */
	    (void) setoken (&startok, line, "tab");
	    if (!strcmp (sc->isfil,"gsc-server")) {
   		if (tabgetc (&startok, sc->entid, str, 24))
		    num = 0.0;
		else {
		    num = atof (str+3) * 0.00001;
		    ireg = (int) num;
		    inum = (int) (((num - (double)ireg) * 100000.0) + 0.5);
		    num = (double) ireg + 0.0001 * (double) inum;
		    }
		}
	    else
		num = tabgetr8 (&startok,sc->entid);
	    if (num == 0.0)
		num = (double) istar;
	    if (num > tnum[jnum]) {
		break;
		}
	    if (num == tnum[jnum])
		break;
	    }

	/* If star has been found in table, read rest of entry */
	if ((match && num == tnum[jnum]) || (!match && inum == istar)) {
	    istar0 = istar;
	    sc->istar = startab->iline;
	    if (tabstar (istar, sc, star, nlog))
		fprintf (stderr,"TABRNUM: Cannot read star %d\n", istar);

	    /* If star entry has been read successfully */
	    else {

		/* Set coordinate system for this star */
		sysref = star->coorsys;
		eqref = star->equinox;

		/* Extract selected fields  */
		num = star->num;
		ra = star->ra;
		dec = star->dec;
		rapm = star->rapm;
		decpm = star->decpm;
		parallax = star->parallax;
		rv = star->radvel;
		if (sc->entrv > 0)
		    star->xmag[sc->nmag-1] = rv;
		
		if (sc->entpx || sc->entrv)
		    wcsconv (sysref, sysout, eqref, eqout, epref, epout,
			     &ra, &dec, &rapm, &decpm, &parallax, &rv);
		else if (sc->mprop == 1)
		    wcsconp (sysref, sysout, eqref, eqout, epref, epout,
			     &ra, &dec, &rapm, &decpm);
		else
		    wcscon (sysref, sysout, eqref, eqout, &ra, &dec, epout);
		if (sc->nmag > 0)
		    mag = star->xmag[0];
		else
		    mag = 99.99;
		if (sc->sptype)
		    peak = (1000 * (int) star->isp[0]) + (int)star->isp[1];
		else
		    peak = star->peak;
		if (nlog < 0) {
		    CatNum (TABCAT, -nnfld, sc->nndec, num, numstr);
		    ra2str (rastr, 31, ra, 3);
		    dec2str (decstr, 31, dec, 2);
		    printf ("%s	%s	%s", numstr,rastr,decstr);
		    for (imag = 0; imag < sc->nmag; imag++)
			if (imag == sc->nmag-1 && sc->entepoch) {
			    datestring = DateString (star->xmag[imag], 1);
			    printf ("%s", datestring);
			    free (datestring);
			    }
			else
			    printf ("	%.2f", star->xmag[imag]);
		    if (kwo != NULL)
			printf ("	%s\n", star->objname);
		    else
			printf ("\n");
		    continue;
		    }

		/* Save star position and magnitude in table */
		tnum[jnum] = num;
		tra[jnum] = ra;
		tdec[jnum] = dec;
		if (sc->mprop == 1) {
		    tpra[jnum] = rapm;
		    tpdec[jnum] = decpm;
		    }
		for (imag = 0; imag < sc->nmag; imag++) {
		    if (tmag[imag] != NULL)
			tmag[imag][jnum] = star->xmag[imag];
		    }
		tpeak[jnum] = peak;
		lname = strlen (star->objname);
		if (lname > 0) {
		    objname = (char *)calloc (lname+1, 1);
		    strcpy (objname, star->objname);
		    if (tkey[jnum]) free(tkey[jnum]);
		    tkey[jnum] = objname;
		    }
		nstar++;
		if (nlog == 1)
		    fprintf (stderr,"TABRNUM: %11.6f: %9.5f %9.5f %s %5.2f %d    \n",
			     num,ra,dec,cstr,mag,peak);
		/* End of accepted star processing */
		}
	    }
	else {
	    nstar++;
	    istar0 = istar;
	    if (nlog < 0) {
		CatNum (TABCAT, -nnfld, sc->nndec, tnum[jnum], numstr);
		ra = 0.0;
		ra2str (rastr, 31, ra, 3);
		dec = 0.0;
		dec2str (decstr, 31, dec, 2);
		printf ("%s	%s	%s", numstr,rastr,decstr);
		for (imag = 0; imag < sc->nmag; imag++)
		    printf ("	99.0");
		printf ("\n");
		}
	    }

	/* Log operation */
	if (nlog > 0 && jnum%nlog == 0)
	    fprintf (stderr,"TABRNUM: %5d / %5d / %5d sources catalog %s\r",
		     nstar,jnum,nstars,tabcatname);

	/* End of star loop */
	}

/* Summarize search */
    if (nlog > 0)
	fprintf (stderr,"TABRNUM: Catalog %s : %d / %d found\n",
		 tabcatname,nstar,nstars);

    free ((char *) star);
    return (nstar);
}


/* TABBIN -- Bin tab table stars in specified region into an image */

int
tabbin (tabcatname, wcs, header, image, mag1, mag2, sortmag, magscale, nlog)

char	*tabcatname;	/* Name of reference star catalog file */
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
    int sysout;		/* Search coordinate system */
    double eqout;	/* Search coordinate equinox */
    double epout;	/* Proper motion epoch (0.0 for no proper motion) */
    double ra1,ra2;	/* Limiting right ascensions of region in degrees */
    double dec1,dec2;	/* Limiting declinations of region in degrees */
    double rra1,rra2;	/* Catalog coordinate limiting right ascension (deg) */
    double rdec1,rdec2;	/* Catalog coordinate limiting declination (deg) */
    int sysref;		/* Catalog coordinate system */
    double eqref;	/* Catalog equinox */
    double epref;	/* Catalog epoch */
    double secmarg = 0.0; /* Arcsec/century margin for proper motion */
    double magt;
    double rdist, ddist;
    int ix, iy;
    int pass;
    char cstr[32];
    struct Star *star;
    struct StarCat *sc;	/* Star catalog data structure */

    int wrap;
    int jstar;
    int magsort;
    int nstar;
    int imag;
    double ra,dec, rapm, decpm;
    double mag, parallax, rv;
    double num;
    int peak;
    int istar, nstars, lstar;
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

    /* Set catalog search limits from image WCS information */
    sysout = wcs->syswcs;
    eqout = wcs->equinox;
    epout = wcs->epoch;
    wcscstr (cstr, sysout, eqout, epout);
    wcssize (wcs, &cra, &cdec, &dra, &ddec);
    SearchLim (cra,cdec,dra,ddec,sysout,&ra1,&ra2,&dec1,&dec2,verbose);

    /* mag1 is always the smallest magnitude limit */
    if (mag2 < mag1) {
	mag = mag2;
	mag2 = mag1;
	mag1 = mag;
	}

    /* Logging interval */
    nstar = 0;

    lstar = sizeof (struct Star);
    star = (struct Star *) calloc (1, lstar);
    sc = tabcatopen (tabcatname, NULL, 0);
    if (sc == NULL || sc->nstars <= 0) {
	if (taberr != NULL)
	    fprintf (stderr,"%s\n", taberr);
	fprintf (stderr,"TABBIN: Cannot read catalog %s\n", tabcatname);
	free (star);
	sc = NULL;
	return (0);
	}

    nstars = sc->nstars;
    jstar = 0;

    if (sortmag > 0 && sortmag <= sc->nmag)
	magsort = sortmag - 1;
    else 
	magsort = 0;

    /* Set catalog coordinate system */
    if (sc->equinox != 0.0)
	eqref = sc->equinox;
    else
	eqref = eqout;
    if (sc->epoch != 0.0)
	epref = sc->epoch;
    else
	epref = epout;
    if (sc->coorsys)
	sysref = sc->coorsys;
    else
	sysref = sysout;
    wcscstr (cstr, sysout, eqout, epout);

    rra1 = ra1;
    rra2 = ra2;
    rdec1 = dec1;
    rdec2 = dec2;
    if (sc->mprop > 0)
	secmarg = 60.0;
    RefLim(cra,cdec,dra,ddec,sysout,sysref,eqout,eqref,epout,epref,secmarg,
	    &rra1, &rra2, &rdec1, &rdec2, &wrap, verbose);

    /* If RA range includes zero, split it in two */
    if (rra1 > rra2)
	wrap = 1;
    else
	wrap = 0;

    /* Loop through catalog */
    for (istar = 1; istar <= nstars; istar++) {

	/* Read position of next star */
	if (tabstar (istar, sc, star, verbose)) {
	    if (verbose)
		fprintf (stderr,"TABBIN: Cannot read star %d\n", istar);
	    break;
	    }

	/* Set magnitude to test */
	if (sc->nmag > 0) {
	    magt = star->xmag[magsort];
	    imag = 0;
	    while (magt == 99.90 && imag < sc->nmag)
		magt = star->xmag[imag++];
	    if (magt > 100.0)
		magt = magt - 100.0;
	    }
	else
	    magt = mag1;

	/* Check magnitude limits */
	pass = 1;
	if (mag1 != mag2 && (magt < mag1 || magt > mag2))
	    pass = 0;

	/* Check rough position limits */
	ra = star->ra;
	dec = star->dec;
	if  ((!wrap && (ra < rra1 || ra > rra2)) ||
	    (wrap && (ra < rra1 && ra > rra2)) ||
	    dec < rdec1 || dec > rdec2)
	    pass = 0;

	/* Convert coordinate system for this star and test it*/
	if (pass) {
	    sysref = star->coorsys;
	    eqref = star->equinox;
	    epref = star->epoch;

	    /* Extract selected fields  */
	    num = star->num;
	    rapm = star->rapm;
	    decpm = star->decpm;
	    parallax = star->parallax;
	    rv = star->radvel;

	    /* Convert from catalog to search coordinate system */
	    if (sc->entpx || sc->entrv)
		wcsconv (sysref, sysout, eqref, eqout, epref, epout,
		     &ra, &dec, &rapm, &decpm, &parallax, &rv);
	    else if (sc->mprop == 1)
		wcsconp (sysref, sysout, eqref, eqout, epref, epout,
		     &ra, &dec, &rapm, &decpm);
	    else
		wcscon (sysref, sysout, eqref, eqout, &ra, &dec, epout);
	    if (sc->sptype)
		peak = (1000 * (int) star->isp[0]) + (int)star->isp[1];
	    else
		peak = star->peak;

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
		    flux = magscale * exp (logt * (-magt / 2.5));
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
	if (nlog > 0 && istar%nlog == 0)
		fprintf (stderr,"TABBIN: %5d / %5d / %5d sources catalog %s\r",
			jstar,istar,nstars,tabcatname);

	/* End of star loop */
	}

    /* Summarize search */
    if (nlog > 0) {
	fprintf (stderr,"TABBIN: Catalog %s : %d / %d / %d found\n",tabcatname,
		 jstar,istar,nstars);
	}

    return (nstar);
}


/* TABXYREAD -- Read X, Y, and magnitude of tab table stars */

int
tabxyread (tabcatname, xa, ya, ba, pa, nlog)

char	*tabcatname;	/* Name of reference star catalog file */
double	**xa;		/* Array of x coordinates (returned) */
double	**ya;		/* Array of y coordinates (returned) */
double	**ba;		/* Array of magnitudes (returned) */
int	**pa;		/* Array of fluxes (returned) */
int	nlog;
{
    double xi, yi, magi, flux;
    char *line;
    int istar, nstars;
    struct TabTable *startab;
    int entx, enty, entmag;

    /* Open tab table file */
    nndec = 0;
    startab = tabopen (tabcatname, 0);
    if (startab == NULL || startab->nlines <= 0) {
	fprintf (stderr,"TABXYREAD: Cannot read catalog %s\n", tabcatname);
	return (0);
	}

    /* Find columns for X and Y */
    entx = tabccol (startab, "x");
    enty = tabccol (startab, "y");

    /* Find column for magnitude */
    if (!(entmag = tabccol (startab, "mag"))) {
	if (!(entmag = tabccol (startab, "magv"))) {
	    if (!(entmag = tabccol (startab, "magj")))
		entmag = tabccol (startab, "magr");
	    }
	}

    /* Allocate vectors for x, y, magnitude, and flux */
    nstars = startab->nlines;
    *xa = (double *) realloc(*xa, nstars*sizeof(double));
    if (*xa == NULL) {
	fprintf (stderr,"TABXYREAD: Cannot allocate memory for x\n");
	return (0);
	}
    *ya = (double *) realloc(*ya, nstars*sizeof(double));
    if (*ya == NULL) {
	fprintf (stderr,"TABXYREAD: Cannot allocate memory for y\n");
	return (0);
	}
    *ba = (double *) realloc(*ba, nstars*sizeof(double));
    if (*ba == NULL) {
	fprintf (stderr,"TABXYREAD: Cannot allocate memory for mag\n");
	return (0);
	}
    *pa = (int *) realloc(*pa, nstars*sizeof(int));
    if (*pa == NULL) {
	fprintf (stderr,"TABXYREAD: Cannot allocate memory for flux\n");
	return (0);
	}

    /* Loop through catalog */
    for (istar = 0; istar < nstars; istar++) {

	/* Read line for next star */
	if ((line = gettabline (startab, istar+1)) == NULL) {
	    fprintf (stderr,"TABXYREAD: Cannot read star %d\n", istar);
	    break;
	    }

	/* Extract x, y, and magnitude */
	(void) setoken (&startok, line, "tab");
	xi = tabgetr8 (&startok, entx);
	yi = tabgetr8 (&startok, enty);
	magi = tabgetr8 (&startok, entmag);

	(*xa)[istar] = xi;
	(*ya)[istar] = yi;
	(*ba)[istar] = magi;
	flux = 1000000000.0 * pow (10.0, (-magi / 2.5));
	(*pa)[istar] = (int) flux;

	if (nlog == 1)
	    fprintf (stderr,"DAOREAD: %6d/%6d: %9.5f %9.5f %6.2f %15.4f\n",
		     istar,nstars,xi,yi,magi,flux);

	/* Log operation */
	if (nlog > 1 && istar%nlog == 0)
	    fprintf (stderr,"TABXYREAD: %5d / %5d sources catalog %s\r",
		     istar,nstars,tabcatname);

	/* End of star loop */
	}

    /* Summarize search */
    if (nlog > 0)
	fprintf (stderr,"TABXYREAD: Catalog %s : %d / %d found\n",tabcatname,
		 istar,nstars);

    /* Free table */
    tabclose (startab);
    if (istar < nstars-1)
	return (istar + 1);
    else
	return (nstars);
}


#define TABMAX 64

/* TABRKEY -- Read single keyword from tab table stars with specified numbers */

int
tabrkey (tabcatname, starcat, nnum, tnum, keyword, tval)

char	*tabcatname;	/* Name of reference star catalog file */
struct StarCat **starcat; /* Star catalog data structure */
int	nnum;		/* Number of stars to look for */
double	*tnum;		/* Array of star numbers to look for */
char	*keyword;	/* Keyword for which to return values */
char	**tval;		/* Returned values for specified keyword */
{
    int jnum, lval;
    int nstar;
    int istar, nstars;
    double num;
    char *line;
    char *tvalue;
    char value[TABMAX];
    struct TabTable *startab;
    struct StarCat *sc;	/* Star catalog data structure */

    nstar = 0;

    /* Open star catalog */
    sc = *starcat;
    if (sc == NULL)
	sc = tabcatopen (tabcatname, NULL, 0);
    *starcat = sc;
    if (sc == NULL || sc->nstars <= 0) {
	if (taberr != NULL)
	    fprintf (stderr,"%s\n", taberr);
	fprintf (stderr,"TABRKEY: Cannot read catalog %s\n", tabcatname);
	return (0);
	}
    startab = sc->startab;
    if (startab == NULL || startab->nlines <= 0) {
	fprintf (stderr,"TABRKEY: Cannot read catalog %s\n", tabcatname);
	return (0);
	}

    /* Loop through star list */
    nstars = startab->nlines;
    for (jnum = 0; jnum < nnum; jnum++) {

	/* Loop through catalog to star */
	for (istar = 1; istar <= nstars; istar++) {
	    if ((line = gettabline (startab, istar)) == NULL) {
		fprintf (stderr,"TABRKEY: Cannot read star %d\n", istar);
		num = 0.0;
		break;
		}

	    /* Check ID number */
	    (void) setoken (&startok, line, "tab");
	    if ((num = tabgetr8 (&startok,sc->entid)) == 0.0)
		num = (double) istar;
	    if (num == tnum[jnum])
		break;
	    }

	/* If star has been found in table */
	if (num == tnum[jnum]) {
	    nstar++;

	    /* Extract selected field */
	    (void) tabgetk (startab, &startok, keyword, value, TABMAX);
	    lval = strlen (value);
	    if (lval > 0) {
		tvalue = (char *) calloc (1, lval+1);
		strcpy (tvalue, value);
		}
	    else
		tvalue = NULL;
	    if (tval[jnum]) free(tval[jnum]);
	    tval[jnum] = tvalue;
	    }
	}

    return (nstars);
}

static char newline = 10;
static char tab = 9;


/* TABCATOPEN -- Open tab table catalog, returning number of entries */

struct StarCat *
tabcatopen (tabpath, tabtable, nbbuff)

char *tabpath;		/* Tab table catalog file pathname */
struct TabTable *tabtable;
int	nbbuff;		/* Number of bytes in buffer; 0=read whole file */
{
    char cstr[32];
    char keyword[16];
    char *tabname;
    struct TabTable *startab;
    struct StarCat *sc;
    int i, lnum, ndec, istar, nbsc, icol, j, nnfld;
    int entpmq, entnid;
    char *line;
    double dnum;

    /* Open the tab table file */
    if (tabtable != NULL)
	startab = tabtable;
    else if ((startab = tabopen (tabpath, nbbuff)) == NULL)
	return (NULL);

    /* Allocate catalog data structure */
    nbsc = sizeof (struct StarCat);
    sc = (struct StarCat *) calloc (1, nbsc);
    sc->startab = startab;

    /* Save name of catalog */
    tabname = strrchr (tabpath, '/');
    if (tabname)
	tabname = tabname + 1;
    else
	tabname = tabpath;
    if (strlen (tabname) < 24)
	strcpy (sc->isfil, tabname);
    else {
	strncpy (sc->isfil, tabname, 23);
	sc->isfil[23] = (char) 0;
	}

    /* Find column and name of object identifier */
    sc->entid = -1;
    sc->keyid[0] = (char) 0;
    if ((sc->entid = tabccol (startab, "id"))) {
	i = sc->entid - 1;
	strncpy (sc->keyid, startab->colname[i], startab->lcol[i]);
	}
    else if ((sc->entid = tabccont (startab, "_id"))) {
	i = sc->entid - 1;
	strncpy (sc->keyid, startab->colname[i], startab->lcol[i]);
	}
    else if ((sc->entid = tabccont (startab, "id"))) {
	i = sc->entid - 1;
	strncpy (sc->keyid, startab->colname[i], startab->lcol[i]);
	}
    else if ((sc->entid = tabccont (startab, "num"))) {
	i = sc->entid - 1;
	strncpy (sc->keyid, startab->colname[i], startab->lcol[i]);
	}
    else if ((sc->entid = tabccont (startab, "name"))) {
	i = sc->entid - 1;
	strncpy (sc->keyid, startab->colname[i], startab->lcol[i]);
	}
    else if ((sc->entid = tabccont (startab, "obj"))) {
	i = sc->entid - 1;
	strncpy (sc->keyid, startab->colname[i], startab->lcol[i]);
	}
    sc->nndec = nndec;

    /* Figure out what the coordinate system is */
    if (tabhgetc (startab, "radecsys", cstr)) {
	if (!strcmp (cstr, "galactic"))
	    sc->coorsys = WCS_GALACTIC;
	else if (!strcmp (cstr, "ecliptic"))
	    sc->coorsys = WCS_ECLIPTIC;
	else if (!strcmp (cstr, "fk4"))
	    sc->coorsys = WCS_B1950;
	else
	    sc->coorsys = WCS_J2000;
	}
    else
	sc->coorsys = WCS_J2000;

    /* Find column and name of object right ascension */
    sc->entra = -1;
    sc->keyra[0] = (char) 0;
    if (sc->coorsys == WCS_GALACTIC) {
	if ((sc->entra = tabccol (startab, "long_gal")))
	    strcpy (sc->keyra, "long_gal");
	else if ((sc->entra = tabccol (startab, "long")))
	    strcpy (sc->keyra, "long_gal");
	else if ((sc->entra = tabccont (startab, "long"))) {
	    i = sc->entra - 1;
	    strncpy (sc->keyra, startab->colname[i], startab->lcol[i]);
	    }
	}
    else if (sc->coorsys == WCS_ECLIPTIC) {
	if ((sc->entra = tabccol (startab, "long_ecl")))
	    strcpy (sc->keyra, "long_ecl");
	else if ((sc->entra = tabccol (startab, "long")))
	    strcpy (sc->keyra, "long_ecl");
	else if ((sc->entra = tabccont (startab, "long"))) {
	    i = sc->entra - 1;
	    strncpy (sc->keyra, startab->colname[i], startab->lcol[i]);
	    }
	}
    else {
	if ((sc->entra = tabccol (startab, "ra"))) {
	    i = sc->entra - 1;
	    strncpy (sc->keyra, startab->colname[i], startab->lcol[i]);
	    }
	else if ((sc->entra = tabccont (startab, "ra"))) {
	    i = sc->entra - 1;
	    strncpy (sc->keyra, startab->colname[i], startab->lcol[i]);
	    }
	}

    /* Find column and name of object declination */
    sc->entdec = -1;
    sc->keydec[0] = (char) 0;
    if (sc->coorsys == WCS_GALACTIC) {
	if ((sc->entdec = tabccol (startab, "lat_gal")))
	    strcpy (sc->keydec, "lat_gal");
	else if ((sc->entdec = tabccol (startab, "lat")))
	    strcpy (sc->keydec, "lat_gal");
	else if ((sc->entdec = tabccont (startab, "lat"))) {
	    i = sc->entdec - 1;
	    strncpy (sc->keydec, startab->colname[i], startab->lcol[i]);
	    }
	}
    else if (sc->coorsys == WCS_ECLIPTIC) {
	if ((sc->entdec = tabccol (startab, "lat_ecl")))
	    strcpy (sc->keydec, "lat_ecl");
	else if ((sc->entdec = tabccol (startab, "lat")))
	    strcpy (sc->keydec, "lat_ecl");
	else if ((sc->entdec = tabccont (startab, "lat"))) {
	    i = sc->entdec - 1;
	    strncpy (sc->keydec, startab->colname[i], startab->lcol[i]);
	    }
	}
    else {
	if ((sc->entdec = tabccol (startab, "de(deg)"))) {
	    i = sc->entdec - 1;
	    strcpy (sc->keydec, "dec");
	    }
	else if ((sc->entdec = tabccol (startab, "dec"))) {
	    i = sc->entdec - 1;
	    strncpy (sc->keydec, startab->colname[i], startab->lcol[i]);
	    }
	else if ((sc->entdec = tabccont (startab, "dec"))) {
	    i = sc->entdec - 1;
	    strncpy (sc->keydec, startab->colname[i], startab->lcol[i]);
	    }
	}

    /* Check columns for magnitudes and save columns and names */
    sc->nmag = 0;
    icol = 0;
    for (i = 0; i < startab->ncols; i++) {
	icol = icol + 1;
	if (icol == sc->entid) continue;
	if (icol == sc->entra) continue;
	if (icol == sc->entdec) continue;
	for (j = 0; j < 16; j++) keyword[j] = (char)0;
	if (startab->lcol[i] < 16)
	    strncpy (keyword, startab->colname[i], startab->lcol[i]);
	else
	    strncpy (keyword, startab->colname[i], 15);
	if (strcsrch (keyword, "mag") && !strcsrch (keyword, "err")) {
	    strcpy (sc->keymag[sc->nmag], keyword);
	    sc->entmag[sc->nmag] = icol;
	    sc->nmag++;
	    }
	else if (strcsrch (keyword, "exp")) {
	    strcpy (sc->keymag[sc->nmag], keyword);
	    sc->entmag[sc->nmag] = icol;
	    sc->nmag++;
	    }
	else if (strcsrch (keyword, "dist")) {
	    strcpy (sc->keymag[sc->nmag], keyword);
	    sc->entmag[sc->nmag] = icol;
	    sc->nmag++;
	    }
	else if ((keyword[0] == 'M' || keyword[0] == 'm') &&
		 !strcsrch (keyword, "err") &&
		 !strcsrch (keyword, "flag")) {
	    strcpy (sc->keymag[sc->nmag], keyword);
	    sc->entmag[sc->nmag] = icol;
	    sc->nmag++;
	    }
	}

    /* Find column and name of object right ascension proper motion */
    sc->entrpm = -1;
    sc->keyrpm[0] = (char) 0;
    if ((sc->entrpm = tabccol (startab, "ura")))
	strcpy (sc->keyrpm, "ura");
    else if ((sc->entrpm = tabccol (startab, "rapm")))
	strcpy (sc->keyrpm, "rapm");
    else if ((sc->entrpm = tabccol (startab, "pmra")))
	strcpy (sc->keyrpm, "pmra");
    else if ((sc->entrpm = tabccol (startab, "dra")))
	strcpy (sc->keyrpm, "dra");
    else if ((sc->entrpm = tabccol (startab, "ux")))
	strcpy (sc->keyrpm, "ux");

    /* Find column and name of object declination proper motion */
    sc->entdpm = -1;
    sc->keydpm[0] = (char) 0;
    if ((sc->entdpm = tabccol (startab, "udec")))
	strcpy (sc->keydpm, "udec");
    else if ((sc->entdpm = tabccol (startab, "decpm")))
	strcpy (sc->keydpm, "decpm");
    else if ((sc->entdpm = tabccol (startab, "pmdec")))
	strcpy (sc->keydpm, "pmdec");
    else if ((sc->entrpm = tabccol (startab, "ddec")))
	strcpy (sc->keyrpm, "ddec");
    else if ((sc->entdpm = tabccol (startab, "uy")))
	strcpy (sc->keydpm, "uy");

    /* Find units for RA proper motion */
    sc->mprop = 0;
    cstr[0] = 0;
    if (!tabhgetc (startab,"RPMUNIT", cstr)) {
	if (!tabhgetc (startab,"rpmunit", cstr)) {
	    if (!tabhgetc (startab,"pmunit", cstr)) {
		if (sc->entdpm > 0 && sc->entrpm > 0)
		    strcpy (cstr,"mas/yr");
		}
	    }
	}
    if (strlen (cstr) > 0) {
	sc->mprop = 1;
	if (!strcmp (cstr, "mas/yr") || !strcmp (cstr, "mas/year"))
	    sc->rpmunit = PM_MASYR;
	else if (!strcmp (cstr, "ms/yr") || !strcmp (cstr, "millisec/year"))
	    sc->rpmunit = PM_MTSYR;
	else if (!strcmp (cstr, "arcsec/yr") || !strcmp (cstr, "arcsec/year"))
	    sc->rpmunit = PM_ARCSECYR;
	else if (!strcmp (cstr, "arcsec/hr") || !strcmp (cstr, "arcsec/hour"))
	    sc->rpmunit = PM_ARCSECHR;
	else if (!strcmp (cstr, "arcsec/cen") || !strcmp (cstr, "arcsec/century"))
	    sc->rpmunit = PM_ARCSECCEN;
	else if (!strcmp (cstr, "rad/yr") || !strcmp (cstr, "rad/year"))
	    sc->rpmunit = PM_RADYR;
	else if (!strcmp (cstr, "sec/yr") || !strcmp (cstr, "sec/year"))
	    sc->rpmunit = PM_TSECYR;
	else if (!strcmp (cstr, "tsec/yr") || !strcmp (cstr, "tsec/year"))
	    sc->rpmunit = PM_TSECYR;
	else if (!strcmp (cstr, "tsec/cen") || !strcmp (cstr, "tsec/century"))
	    sc->rpmunit = PM_TSECCEN;
	else
	    sc->rpmunit = PM_DEGYR;
	}
    else if (sc->entrpm > 0)
	sc->rpmunit = PM_TSECCEN;

    /* Find units for Dec proper motion */
    cstr[0] = 0;
    if (!tabhgetc (startab,"DPMUNIT", cstr)) {
	if (!tabhgetc (startab,"dpmunit", cstr)) {
	    if (!tabhgetc (startab,"pmunit", cstr))
		tabhgetc (startab,"pmunit", cstr);
	    }
	}
    if (strlen (cstr) > 0) {
	sc->mprop = 1;
	if (!strcmp (cstr, "mas/yr") || !strcmp (cstr, "mas/year"))
	    sc->dpmunit = PM_MASYR;
	else if (!strcmp (cstr, "sec/yr") || !strcmp (cstr, "sec/year"))
	    sc->dpmunit = PM_ARCSECYR;
	else if (!strcmp (cstr, "tsec/yr") || !strcmp (cstr, "tsec/year"))
	    sc->dpmunit = PM_ARCSECYR;
	else if (!strcmp (cstr, "arcsec/hr") || !strcmp (cstr, "arcsec/hour"))
	    sc->dpmunit = PM_ARCSECHR;
	else if (!strcmp (cstr, "arcsec/cen") || !strcmp (cstr, "arcsec/century"))
	    sc->dpmunit = PM_ARCSECCEN;
	else if (!strcmp (cstr, "arcsec/yr") || !strcmp (cstr, "arcsec/year"))
	    sc->dpmunit = PM_ARCSECYR;
	else if (!strcmp (cstr, "rad/yr") || !strcmp (cstr, "rad/year"))
	    sc->dpmunit = PM_RADYR;
	else
	    sc->dpmunit = PM_DEGYR;
	}
    else if (sc->entdpm > 0)
	sc->dpmunit = PM_ARCSECCEN;

    /* Find column for parallax */
    sc->entpx = 0;
    sc->entpx = tabccol (startab, "px");

    /* Find column for parallax error */
    sc->entpxe = 0;
    sc->entpxe = tabccol (startab, "pxerr");

    /* Find column for radial velocity */
    sc->entrv = 0;
    sc->keyrv[0] = (char) 0;
    if ((sc->entrv = tabccol (startab, "rv")))
	strcpy (sc->keyrv, "rv");
    else if ((sc->entrv = tabccol (startab, "cz")))
	strcpy (sc->keyrv, "cz");
    if (sc->entrv > 0 && sc->nmag < 10) {
	strcpy (sc->keymag[sc->nmag], sc->keyrv);
	sc->entmag[sc->nmag] = sc->entrv;
	sc->nmag++;
	}

    /* Find column for epoch */
    sc->entepoch = 0;
    sc->keyepoch[0] = (char) 0;
    if ((sc->entepoch = tabccol (startab, "epoch")))
	strcpy (sc->keyepoch, "epoch");
    else if ((sc->entepoch = tabccol (startab, "ep")))
	strcpy (sc->keyepoch, "ep");
    if (sc->entepoch > 0 && sc->nmag < 10) {
	strcpy (sc->keymag[sc->nmag], sc->keyepoch);
	sc->entmag[sc->nmag] = sc->entepoch;
	sc->nmag++;
	sc->nepoch = 1;
	}

    /* Find column for date (in FITS format as epoch alternate) */
    sc->entdate = 0;
    sc->entdate = tabccol (startab, "date");

    /* Find column and name of object peak or plate number */
    sc->entpeak = -1;
    sc->keypeak[0] = (char) 0;
    if ((sc->entpeak = tabccol (startab, "PEAK")))
	strcpy (sc->keypeak, "PEAK");
    else if ((sc->entpeak = tabccol (startab, "peak")))
	strcpy (sc->keypeak, "peak");
    else if ((sc->entpeak = tabccol (startab, "plate"))) {
	strcpy (sc->keypeak, "plate");
	sc->plate = 1;
	}
    else if ((sc->entpeak = tabccol (startab, "field"))) {
	strcpy (sc->keypeak, "field");
	sc->plate = 1;
	}
    else if ((sc->entpeak = tabccol (startab, "Class"))) {
	strcpy (sc->keypeak, "class");
	sc->plate = 1;
	}
    else if ((sc->entpeak = tabcol (startab, "c"))) {
	strcpy (sc->keypeak, "class");
	sc->plate = 1;
	}
    else if ((entpmq = tabccol (startab, "pm")) > 0 &&
	     (entnid = tabccol (startab, "ni")) > 0) {
	sc->entpeak = (entpmq * 100) + entnid;
	}

    /* Find column and name of object spectral type */
    sc->enttype = 0;
    sc->keytype[0] = (char) 0;
    if ((sc->enttype = tabccol (startab, "SpT")))
	strcpy (sc->keytype, "spt");
    else if ((sc->enttype = tabccol (startab, "TYPE")))
	strcpy (sc->keytype, "type");
    else if ((sc->enttype = tabccont (startab, "typ"))) {
	i = sc->enttype - 1;
	strncpy (sc->keytype, startab->colname[i], startab->lcol[i]);
	}
    if (sc->enttype > 0)
	sc->sptype = 1;

    sc->entadd = -1;
    sc->keyadd[0] = (char) 0;
    if (kwo != NULL) {
	sc->entadd = tabccol (startab, kwo);
	strcpy (sc->keyadd, kwo);
	}

    /* Set catalog coordinate system */
    sc->coorsys = 0;
    sc->equinox = 0.0;
    sc->epoch = 0.0;
    if (tabhgetc (startab,"RADECSYS", cstr)) {
	sc->coorsys = wcscsys (cstr);
	if (!tabhgetr8 (startab,"EQUINOX", &sc->equinox))
	    sc->equinox = wcsceq (cstr);
	if (!tabhgetr8 (startab,"EPOCH",&sc->epoch))
	    sc->epoch = sc->equinox;
	}
    if (tabhgetc (startab,"radecsys", cstr)) {
	sc->coorsys = wcscsys (cstr);
	if (!tabhgetr8 (startab,"equinox", &sc->equinox))
	    sc->equinox = wcsceq (cstr);
	if (!tabhgetr8 (startab,"epoch",&sc->epoch))
	    sc->epoch = sc->equinox;
	}
    else if (tabhgetr8 (startab,"EQUINOX", &sc->equinox)) {
	if (!tabhgetr8 (startab,"EPOCH",&sc->epoch))
	    sc->epoch = sc->equinox;
	if (sc->equinox == 1950.0)
	    sc->coorsys = WCS_B1950;
	else
	    sc->coorsys = WCS_J2000;
	}
    else if (tabhgetr8 (startab,"equinox", &sc->equinox)) {
	if (!tabhgetr8 (startab,"epoch",&sc->epoch))
	    sc->epoch = sc->equinox;
	if (sc->equinox == 1950.0)
	    sc->coorsys = WCS_B1950;
	else
	    sc->coorsys = WCS_J2000;
	}
    else if (tabhgetr8 (startab,"EPOCH", &sc->epoch)) {
	sc->equinox = sc->epoch;
	if (sc->equinox == 1950.0)
	    sc->coorsys = WCS_B1950;
	else
	    sc->coorsys = WCS_J2000;
	}
    else if (tabhgetr8 (startab,"epoch", &sc->epoch)) {
	sc->equinox = sc->epoch;
	if (sc->equinox == 1950.0)
	    sc->coorsys = WCS_B1950;
	else
	    sc->coorsys = WCS_J2000;
	}

    /* Check whether catalog is sorted by right ascension */
    if (tabhgetc (startab, "rasort", cstr))
	sc->rasorted = 1;
    else
	sc->rasorted = 0;

    /* Set other stuff */
    sc->nstars = startab->nlines;
    if (sc->entid)
	sc->stnum = 1;
    else
	sc->stnum = 0;

    /* Find out if ID is number, and if so, how many decimal places it has */
    if (sc->entid) {

	istar = 1;
	if ((line = gettabline (startab, istar)) == NULL) {
	    fprintf (stderr,"TABCATOPEN: Cannot read first star\n");
	    tabcatclose (sc);
	    return (NULL);
	    }
	if (setoken (&startok, line, "tab") < 2) {
	    fprintf (stderr,"TABCATOPEN: First star has too few columns\n");
	    tabcatclose (sc);
	    return (NULL);
	    }
	tabgetc (&startok, sc->entid, cstr, 32);

	/* Find length of identifier */
	if (tabhgeti4 (startab,"nfield", &nnfld))
	    sc->nnfld = nnfld;
	else
	    sc->nnfld = strlen (cstr);

	/* Find number of decimal places in identifier */
	if (tabhgeti4 (startab, "ndec", &nndec)) {
	    sc->nndec = nndec;
	    if (!isnum (cstr))
		sc->stnum = -nndec;
	    }
	else if (isnum (cstr)) {
	    dnum = tabgetr8 (&startok,sc->entid);
	    sprintf (cstr,"%.0f", (dnum * 100000000.0) + 0.1);
	    lnum = strlen (cstr);
	    for (i = 0; i < 8; i++) {
		if (cstr[lnum-i-1] != '0') {
		    ndec = 8 - i;
		    if (ndec > nndec) {
			nndec = ndec;
			sc->nndec = nndec;
			}
		    break;
		    }
		}
	    sc->nndec = nndec;
	    }
	else {
	    sc->stnum = -strlen (cstr);
	    sc->nndec = nndec;
	    }
	}
    sc->refcat = TABCAT;

    return (sc);
}


/* TABCATCLOSE -- Close tab table catalog and free associated data structures */

void
tabcatclose (sc)
    struct StarCat *sc;
{
    tabclose (sc->startab);
    free (sc);
    return;
}


/* TABSTAR -- Get tab table catalog entry for one star;
   return 0 if successful, else -1 */

int
tabstar (istar, sc, st, verbose)

int	istar;		/* Star sequence number in tab table catalog */
struct StarCat *sc;	/* Star catalog data structure */
struct Star *st;	/* Star data structure, updated on return */
int	verbose;	/* 1 to print error messages */
{
    struct TabTable *startab = sc->startab;
    char *line;
    char *uscore;
    char cnum[64];
    char temp[64];
    double ydate;
    char *cn;
    int ndec, i, imag, ltok;
    int lnum, ireg, inum;
    int pmq, nid;
    int lcn;
    char str[24];

    if ((line = gettabline (startab, istar)) == NULL) {
	if (verbose)
	    fprintf (stderr,"TABSTAR: Cannot read star %d\n", istar);
	return (-1);
	}

    /* Parse line for rapid field extraction */
    if (setoken (&startok, line, "tab") < 2) {
	fprintf (stderr,"TABSTAR: Star %d entry too short\n", istar);
	return (-1);
	}

    /* Extract ID  */
    st->objname[0] = (char) 0;
    if (sc->entid) {
	tabgetc (&startok, sc->entid, cnum, 64);
	if (!strcmp (sc->isfil,"usnoa-server")) {
	    if ((uscore = strchr (cnum, '_')) != NULL)
		*uscore = '.';
	    cn = cnum + 1;
	    }
	else if (!strcmp (sc->isfil,"gsc-server"))
	    cn = cnum + 3;
	else
	    cn = cnum;
	lcn = strlen (cn);
	/* if (lcn < 16 && (isnum (cn) || isnum (cn+1))) { */
	if (lcn < 16 && isnum (cn)) {
	    if (isnum(cnum)) {
		lcn = strlen (cn);
		if (lcn > 15)
		    st->num = atof (cn + lcn - 15);
		else
		    st->num = atof (cn);
		}
	    else {
		if (cnum[0] == 'S')
		    st->num = -atof (cn+1);
		else
		    st->num = atof (cn+1);
		}
	    if (!strcmp (sc->isfil,"gsc-server")) {
		ireg = atoi (cn) / 100000;
		inum = atoi (cn) % 100000;
		st->num = (double) ireg + 0.0001 * (double) inum;
		}

	    /* Find field length of identifier */
	    sc->nnfld = strlen (cn);

	    /* Find number of decimal places in identifier */
	    if (strchr (cn,'.') == NULL) {
		nndec = 0;
		sc->nndec = nndec;
		}
	    else {
		sprintf (cn,"%.0f", (st->num * 100000000.0) + 0.1);
		lnum = strlen (cnum);
		for (i = 0; i < 8; i++) {
		    if (cn[lnum-i-1] != '0') {
			ndec = 8 - i;
			if (ndec > nndec) {
			    nndec = ndec;
			    sc->nndec = nndec;
			    }
			break;
			}
		    }
		}
	    }
	else {
	    strcpy (st->objname, cnum);
	    st->num = st->num + 1.0;
	    }
	}
    else {
	st->num = (double) istar;
	nndec = 0;
	sc->nndec = nndec;
	}

    /* Right ascension */
    st->ra = tabgetra (&startok, sc->entra);

    /* Declination */
    st->dec = tabgetdec (&startok, sc->entdec);

    /* Magnitudes */
    for (imag = 0; imag < sc->nmag; imag++) {
	if (sc->entmag[imag]) {
	    if (tabgetc (&startok, sc->entmag[imag], str, 24))
		return (0);
	    ltok = strlen (str);
	    if (str[ltok-1] == 'L') {
		str[ltok-1] = (char) 0;
		if (isnum (str))
		    st->xmag[imag] = 100.0 + atof (str);
		else
		    st->xmag[imag] = 0.0;
		}
	    else
		st->xmag[imag] = tabgetr8 (&startok, sc->entmag[imag]);
	    }
	else
	    st->xmag[imag] = 0.0;
	}

    /* Convert right ascension proper motion to degrees/year */
    st->rapm = tabgetr8 (&startok, sc->entrpm);
    if (sc->rpmunit == PM_MASYR)
	st->rapm = (st->rapm / 3600000.0) / cosdeg(st->dec);
    else if (sc->rpmunit == PM_MTSYR)
	st->rapm = st->rapm / 240000.0;
    else if (sc->rpmunit == PM_ARCSECYR)
	st->rapm = (st->rapm / 3600.0) / cosdeg (st->dec);
    else if (sc->rpmunit == PM_ARCSECCEN)
	st->rapm = (st->rapm / 360000.0) / cosdeg (st->dec);
    else if (sc->rpmunit == PM_TSECYR)
	st->rapm = st->rapm / 240.0;
    else if (sc->rpmunit == PM_TSECCEN)
	st->rapm = st->rapm / 24000.0;
    else if (sc->rpmunit == PM_RADYR)
	st->rapm = raddeg (st->rapm);
    else
	st->rapm = 0.0;

    /* Convert declination proper motion to degrees/year */
    st->decpm = tabgetr8 (&startok, sc->entdpm);
    if (sc->dpmunit == PM_MASYR)
	st->decpm = st->decpm / 3600000.0;
    else if (sc->dpmunit == PM_ARCSECYR)
	st->decpm = st->decpm / 3600.0;
    else if (sc->dpmunit == PM_ARCSECCEN)
	st->decpm = st->decpm / 360000.0;
    else if (sc->dpmunit == PM_RADYR)
	st->decpm = raddeg (st->decpm);
    else
	st->decpm = 0.0;

    /* Parallax */
    if (sc->entpx)
	st->parallax = tabgetr8 (&startok, sc->entpx);
    else
	st->parallax = 0.0;

    /* Radial velocity */
    if (sc->entrv)
	st->radvel = tabgetr8 (&startok, sc->entrv);
    else
	st->radvel = 0.0;

    /* Epoch */
    if (sc->entepoch) {
	tabgetc (&startok, sc->entepoch, temp, 10);
	if (temp[0] == '_') {
	    if (sc->entdate > 0) {
		tabgetc (&startok, sc->entdate, temp, 10);
		st->epoch = fd2ep (temp);
		}
	    else
		st->epoch = sc->epoch;
	    }
	else if (strchr (temp, '-') != NULL)
	    st->epoch = fd2ep (temp);
	else {
	    ydate = tabgetr8 (&startok, sc->entepoch);
	    if (ydate < 3000.0 && ydate > 0.0)
		st->epoch = dt2ep (ydate, 12.0);
	    else if (ydate < 100000.0)
		st->epoch = mjd2ep (ydate);
	    else
		st->epoch = jd2ep (ydate);
	    if (st->epoch > 2005.000)
		printf ("TABSTAR: %s = %.5f -> %.5f\n", temp, ydate, st->epoch);
	    }
	st->xmag[sc->nmag-1] = st->epoch;
	}
    else
	st->epoch = sc->epoch;

    /* Peak counts */
    if (sc->entpeak > 100) {
	pmq = tabgeti4 (&startok, sc->entpeak/100);
	nid = tabgeti4 (&startok, sc->entpeak%100);
	st->peak = (pmq * 100) + nid;
	}
    else if (sc->entpeak > 0)
	st->peak = tabgeti4 (&startok, sc->entpeak);
    else
	st->peak = 0;

    /* Spectral type */
    if (sc->enttype > 0) {
	strcpy (st->isp, "__");
	tabgetc (&startok, sc->enttype, st->isp, 24);
	}

    /* Extract selected field */
    if (kwo != NULL)
	(void) tabgetk (startab, &startok, kwo, st->objname, 79);

    st->coorsys = sc->coorsys;
    st->equinox = sc->equinox;
    return (0);
}


/* TABOPEN -- Open tab table file, returning number of entries */

struct TabTable *
tabopen (tabfile, nbbuff)

char	*tabfile;	/* Tab table catalog file name */
int	nbbuff;		/* Number of bytes in buffer; 0=read whole file */
{
    FILE *fcat;
    int nr, lfile, lname, lline;
    char *tabnew, *tabline, *lastline;
    char *tabcomma, *nextline;
    char *thisname, *tabname;
    int thistab, itab, nchar, nbtab;
    int formfeed = (char) 12;
    struct TabTable *tabtable;

    tabcomma = NULL;
    if (taberr != NULL) {
	free (taberr);
	taberr = NULL;
	}

    tabname = NULL;
    if (!strcmp (tabfile, "stdin")) {
	lfile = 100000;
	fcat = stdin;
	}
    else {

	/* Separate table name from file name, if necessary */
	if ((tabcomma = strchr (tabfile, ',')) != NULL) {
	    tabname = (char *) calloc (1,64);
	    strcpy (tabname, tabcomma+1);
	    *tabcomma = (char) 0;
	    }

	/* Find length of tab table catalog */
	lfile = tabsize (tabfile);
	if (nbbuff > 1 && nbbuff < lfile)
	    lfile = nbbuff;
	else if (nbbuff == 1 && lfile > 10000)
	    lfile = 10000;
	if (lfile < 1) {
	    taberr = (char *) calloc (64 + strlen (tabfile), 1);
	    sprintf (taberr,"TABOPEN: Tab table file %s has no entries",
		     tabfile);
	    if (tabcomma != NULL) *tabcomma = ',';
	    return (NULL);
	    }

	/* Open tab table catalog */
	if (!(fcat = fopen (tabfile, "r"))) {
	    taberr = (char *) calloc (64 + strlen (tabfile), 1);
	    sprintf (taberr,"TABOPEN: Tab table file %s cannot be read",
		     tabfile);
	    if (tabcomma != NULL) *tabcomma = ',';
	    return (NULL);
	    }
	else if (verbose) {
	    fprintf (stderr,"TABOPEN: tab table %s opened", tabfile);
	}
	}

    /* Allocate tab table structure */
    nbtab = sizeof(struct TabTable);
    if ((tabtable=(struct TabTable *) calloc(1,nbtab)) == NULL){
	taberr = (char *) calloc (64 + strlen (tabfile), 1);
	sprintf (taberr,"TABOPEN: cannot allocate %d bytes for tab table structure for %s",
		 nbtab, tabfile);
	if (tabcomma != NULL) *tabcomma = ',';
	return (NULL);
	}
    else if (verbose) {
	fprintf (stderr,"TABOPEN: allocated %d bytes for tab table structure for %s",
		 nbtab, tabfile);
	}

    tabtable->tabname = tabname;

    /* Allocate space in structure for filename and save it */
    lname = strlen (tabfile) + 2;
    if ((tabtable->filename = (char *)calloc (1, lname)) == NULL) {
	taberr = (char *) calloc (64 + strlen (tabfile), 1);
	sprintf (taberr,"TABOPEN: cannot allocate filename %s in structure",
		 tabfile);
	(void) fclose (fcat);
	tabclose (tabtable);
	if (tabcomma != NULL) *tabcomma = ',';
	return (NULL);
	}
    strcpy (tabtable->filename, tabfile);

    /* Allocate buffer to hold entire catalog (or buffer length) and read it */
    if ((tabtable->tabbuff = (char *) calloc (1, lfile+2)) == NULL) {
	taberr = (char *) calloc (64 + strlen (tabfile), 1);
	sprintf (taberr,"TABOPEN: cannot allocate buffer for tab table %s",
		 tabfile);
	(void) fclose (fcat);
	tabclose (tabtable);
	if (tabcomma != NULL) *tabcomma = ',';
	return (NULL);
	}
    else {
	if (verbose) {
	    fprintf (stderr,"TABOPEN: allocated %d bytes for tab table for %s",
		 lfile+2, tabfile);
	    }
	nr = fread (tabtable->tabbuff, 1, lfile, fcat);
	if (fcat != stdin && nr < lfile) {
	    fprintf (stderr,"TABOPEN: read only %d / %d bytes of file %s\n",
		     nr, lfile, tabfile);
	    (void) fclose (fcat);
	    tabclose (tabtable);
	    if (tabcomma != NULL) *tabcomma = ',';
	    return (NULL);
	    }
	else if (verbose) {
	    fprintf (stderr,"TABOPEN: read %d byte tab table from %s",
		 lfile, tabfile);
	    }
	tabtable->tabbuff[lfile] = (char) 0;

	/* Check for named table within a file */
	if (tabname != NULL) {
	    if (isnum (tabname)) {
		itab = atoi (tabname);
		thisname = tabtable->tabbuff;
		thistab = 1;
		if (itab > 1) {
		    while (thistab < itab && thisname != NULL) {
			thisname = strchr (thisname, formfeed);
			if (thisname != NULL)
			    thisname++;
			thistab++;
			}
		    }
		if (thisname == NULL) {
		    fprintf (stderr, "GETTAB:  There are < %d tables in %s\n",
			itab, tabfile);
		    return (NULL);
		    }
		while (*thisname==' ' || *thisname==newline ||
		       *thisname==formfeed || *thisname==(char)13)
		    thisname++;
		tabline = strchr (thisname, newline);
		if (tabline != NULL) {
		    nchar = tabline - thisname;
		    if (strchr (thisname, tab) > tabline)
			strncpy (tabtable->tabname, thisname, nchar);
		    }
		}
	    else {
		lname = strlen (tabname);
		thisname = tabtable->tabbuff;
		while (*thisname != (char) 0) {
		    while (*thisname==' ' || *thisname==newline ||
		   	   *thisname==formfeed || *thisname==(char)13)
			thisname++;
		    if (!strncmp (tabname, thisname, lname))
			break;
		    else
			thisname = strchr (thisname, formfeed);
		    }
		}
	    if (thisname == NULL) {
		fprintf (stderr, "TABOPEN: table %s in file %s not found\n",
			 tabname, tabfile);
		if (tabcomma != NULL) *tabcomma = ',';
		return (NULL);
		}
	    else
		tabtable->tabheader = strchr (thisname, newline) + 1;
	    }
	else
	    tabtable->tabheader = tabtable->tabbuff;

	tabline = tabtable->tabheader;
	lastline = NULL;
	while (*tabline!='-' && tabline!=NULL && tabline < tabtable->tabbuff+lfile) {
	    lastline = tabline;
	    if ((tabline = strchr (lastline,newline)) != NULL)
		tabline++;
	    else
		break;
	    }
	if (tabline == NULL || *tabline != '-') {
	    taberr = (char *) calloc (64 + strlen (tabfile), 1);
	    sprintf (taberr,"TABOPEN: No - line in tab table %s",tabfile);
	    (void) fclose (fcat);
	    tabclose (tabtable);
	    if (tabcomma != NULL) *tabcomma = ',';
	    return (NULL);
	    }
	tabtable->tabhead = lastline;
	tabtable->tabdash = tabline;
	tabtable->tabdata = strchr (tabline, newline) + 1;

	/* Extract positions of keywords we will want to use */
	if (!tabparse (tabtable)) {
	    fprintf (stderr,"TABOPEN: No columns in tab table %s\n",tabfile);
	    (void) fclose (fcat);
	    tabclose (tabtable);
	    if (tabcomma != NULL) *tabcomma = ',';
	    return (NULL);
	    }
	else if (verbose) {
	    fprintf (stderr,"TABOPEN: tab table %s header parsed", tabfile);
	    }

    /* Enumerate entries in tab table catalog by counting newlines */
	if (nbbuff > 0)
	    tabtable->nlines = 10000000;
	else {
	    tabnew = tabtable->tabdata;
	    tabtable->nlines = 0;
	    while ((tabnew = strchr (tabnew, newline)) != NULL) {
		tabnew = tabnew + 1;
		tabtable->nlines = tabtable->nlines + 1;
		if (*tabnew == formfeed)
		    break;
		}
	    }
	if (verbose) {
	    fprintf (stderr,"TABOPEN: %d lines in tab table %s", tabtable->nlines, tabfile);
	    }
	}

    /* Set up line buffer, and put first line in it */
    if (nbbuff > 0) {
	tabtable->lhead = tabtable->tabdata - tabtable->tabheader;
	tabtable->tcat = fcat;
	nextline = strchr (tabtable->tabdata, newline) + 1;
	tabtable->lline = (nextline - tabtable->tabdata) * 2;
	tabtable->tabline = (char *) calloc (tabtable->lline, 1);
	fseek (tabtable->tcat, (long) tabtable->lhead, SEEK_SET);
	(void) fgets (tabtable->tabline, tabtable->lline, tabtable->tcat);
	lline = strlen (tabtable->tabline);
	if (tabtable->tabline[lline-1] < 32)
	     tabtable->tabline[lline-1] = (char) 0;
	tabtable->tabdata = tabtable->tabline;
	}

    /* Close catalog file if not reading one line at a time */
    else {
	tabtable->lhead = 0;
	(void) fclose (fcat);
	tabtable->tcat = NULL;
	}

    tabtable->tabline = tabtable->tabdata;
    tabtable->iline = 1;
    if (tabcomma != NULL) *tabcomma = ',';
    return (tabtable);
}


void
tabclose (tabtable)

    struct TabTable *tabtable;
{
    if (tabtable != NULL) {
	if (tabtable->filename != NULL) free (tabtable->filename);
	if (tabtable->tabname != NULL) free (tabtable->tabname);
	if (tabtable->tabbuff != NULL) free (tabtable->tabbuff);
	if (tabtable->colname != NULL) free (tabtable->colname);
	if (tabtable->lcol != NULL) free (tabtable->lcol);
	if (tabtable->lcfld != NULL) free (tabtable->lcfld);
	if (tabtable->tcat != NULL) fclose (tabtable->tcat);
	free (tabtable);
	}
    return;
}


/* TABLINE -- Get tab table entry for one line;
	      return NULL if unsuccessful */

char *
gettabline (tabtable, iline)

struct TabTable *tabtable;	/* Tab table structure */
int iline;	/* Line sequence number in tab table */
{
    char *nextline = tabtable->tabline;
    char *next;
    int lline, i;

    /* Return NULL if tab table has not been opened */
    if (tabtable == NULL)
	return (NULL);

    /* Read one line at a time if file is still open */
    if (tabtable->tcat != NULL) {

	/* Return current line from buffer */
	if (iline == tabtable->iline)
	    return (tabtable->tabline);

	/* Read next line from file */
	if (iline < 1 || iline > tabtable->iline) {
	    for (i = tabtable->iline; i < iline; i++) {
		next = fgets (tabtable->tabline, tabtable->lline, tabtable->tcat);
		if (next == NULL || *next == EOF)
		    return (NULL);
		tabtable->iline++;
		}
	    lline = strlen (tabtable->tabline);
	    if (lline < 2)
		return (NULL);
	    if (tabtable->tabline[lline-1] < 32)
		tabtable->tabline[lline-1] = (char) 0;
	    }
	else if (iline < tabtable->iline) {
	    fseek (tabtable->tcat, (long) tabtable->lhead, SEEK_SET);
	    tabtable->iline = 0;
	    for (i = tabtable->iline; i < iline; i++) {
		(void) fgets (tabtable->tabline, tabtable->lline, tabtable->tcat);
		tabtable->iline++;
		}
	    lline = strlen (tabtable->tabline);
	    if (tabtable->tabline[lline-1] < 32)
		tabtable->tabline[lline-1] = (char) 0;
	    }
	return (tabtable->tabline);
	}

    /* Return NULL if trying to read past last line */
    if (iline > tabtable->nlines) {
	fprintf (stderr, "TABLINE:  line %d is not in table\n",iline);
	return (NULL);
	}

    /* If iline is 0 or less, just read next line from table */
    else if (iline < 1 && nextline) {
	tabtable->iline++;
	if (tabtable->iline > tabtable->nlines) {
	    fprintf (stderr, "TABLINE:  line %d is not in table\n",iline);
	    return (NULL);
	    }
	nextline = strchr (nextline, newline) + 1;
	}

    /* If iline is before current line, read from start of file */
    else if (iline < tabtable->iline) {
	tabtable->iline = 1;
	tabtable->tabline = tabtable->tabdata;
	while (tabtable->iline < iline) {
	    tabtable->tabline = strchr (tabtable->tabline, newline) + 1;
	    tabtable->iline ++;
	    }
	}
    /* If iline is after current line, read forward */
    else if (iline > tabtable->iline) {
	while (tabtable->iline < iline) {
	    tabtable->tabline = strchr (tabtable->tabline, newline) + 1;
	    tabtable->iline ++;
	    }
	}

    return (tabtable->tabline);
}


/* TABGETRA -- returns double right ascension in degrees */

double
tabgetra (tabtok, ientry)

struct Tokens *tabtok;	/* Line token structure */
int	ientry;	/* sequence of entry on line */
{
    char str[24];

    strcpy (str, "0.0");
    if (tabgetc (tabtok, ientry, str, 24))
	return (0.0);
    else
	return (str2ra (str));
}


/* TABGETDEC -- returns double declination in degrees */

double
tabgetdec (tabtok, ientry)

struct Tokens *tabtok;	/* Line token structure */
int	ientry;		/* sequence of entry on line */
{
    char str[24];

    strcpy (str, "0.0");
    if (tabgetc (tabtok, ientry, str, 24))
	return (0.0);
    else
	return (str2dec (str));
}


/* TABGETR8 -- returns 8-byte floating point number from tab table line */

double
tabgetr8 (tabtok, ientry)

struct Tokens *tabtok;	/* Line token structure */
int	ientry;		/* sequence of entry on line */
{
    char str[24];

    strcpy (str, "0.0");
    if (tabgetc (tabtok, ientry, str, 24))
	return (0.0);
    else if (isnum (str))
	return (atof (str));
    else
	return (0.0);
}


/* TABGETI4 -- returns a 4-byte integer from tab table line */

int
tabgeti4 (tabtok, ientry)

struct Tokens *tabtok;	/* Line token structure */
int	ientry;		/* sequence of entry on line */
{
    char str[24];

    strcpy (str, "0");
    if (tabgetc (tabtok, ientry, str, 24))
	return (0);
    else if (isnum (str))
	return ((int) atof (str));
    else
	return (0);
}


/* TABGETK -- returns a character entry from tab table line for named column */

int
tabgetk (tabtable, tabtok, keyword, string, maxchar)

struct TabTable *tabtable;	/* Tab table structure */
struct Tokens *tabtok;		/* Line token structure */
char	*keyword;		/* column header of desired value */
char	*string;		/* character string (returned) */
int	maxchar;	/* Maximum number of characters in returned string */
{
    int ientry = tabccol (tabtable, keyword);

    return (tabgetc (tabtok, ientry, string, maxchar));
}


/* TABGETC -- returns n'th entry from tab table line as character string */

int
tabgetc (tabtok, ientry, string, maxchar)

struct Tokens *tabtok;	/* Line token structure */
int	ientry;		/* Sequence of entry on line (1-ncol) */
char	*string;	/* Character string (returned) */
int	maxchar;	/* Maximum number of characters in returned string */
{

    if (ientry > tabtok->ntok)
	return (0);
    else if (getoken (tabtok, ientry, string, maxchar))
	return (0);
    else
	return (-1);
}


/* TABHGETR8 -- read an 8-byte floating point number from a tab table header */

static int
tabhgetr8 (tabtable, keyword, result)

struct TabTable *tabtable;	/* Tab table structure */
char	*keyword;		/* sequence of entry on line */
double	*result;
{
    char value[24];

    if (tabhgetc (tabtable, keyword, value)) {
	*result = atof (value);
	return (1);
	}
    else
	return (0);
}


/* TABHGETI4 -- read a 4-byte integer from a tab table header */

static int
tabhgeti4 (tabtable, keyword, result)

struct TabTable *tabtable;	/* Tab table structure */
char	*keyword;		/* sequence of entry on line */
int	*result;
{
    char value[24];

    if (tabhgetc (tabtable, keyword, value)) {
	*result  = (int) atof (value);
	return (1);
	}
    else
	return (0);
}


/* TABHGETC -- read a string from a tab table header */

static int
tabhgetc (tabtable, keyword, result)

struct TabTable *tabtable;	/* Tab table structure */
char	*keyword;		/* sequence of entry on line */
char	*result;
{
    char *str0, *str1, *line, *head, keylow[24], keyup[24];
    int ncstr, lkey, i;

    head = tabtable->tabbuff;
    str0 = 0;

    /* Make all-upper-case and all-lower-case versions of keyword */
    lkey = strlen (keyword);
    if (lkey > 24) lkey = 24;
    for (i = 0; i < lkey; i++) {
	if (keyword[i] > 96 && keyword[i] < 123)
	    keyup[i] = keyword[i] - 32;
	else
	    keyup[i] = keyword[i];
	if (keyword[i] > 64 && keyword[i] < 91)
	    keylow[i] = keyword[i] + 32;
	else
	    keylow[i] = keyword[i];
	}
    keyup[lkey] = (char) 0;
    keylow[lkey] = (char) 0;

    /* Find keyword or all-upper-case or all-lower-case version in header */
    while (head < tabtable->tabhead) {
	line = strsrch (head, keyword);
	if (line == NULL)
	    line = strsrch (head, keylow);
	if (line == NULL)
	    line = strsrch (head, keyup);
	if (line == NULL)
	    break;
	if (line == tabtable->tabbuff || line[-1] == newline) {
	    str0 = strchr (line, tab) + 1;
	    str1 = strchr (str0, newline);
	    break;
	    }
	else
	    head = line + 1;
	}

    /* Return value as a character string and 1 if found */
    if (str0) {
	ncstr = str1 - str0;
	strncpy (result, str0, ncstr);
	result[ncstr] = (char)0;
	return (1);
	}
    else
	return (0);
}


/* TABPARSE -- Make a table of column headings */

int
tabparse (tabtable)

struct TabTable *tabtable;	/* Tab table structure */
{
    char *colhead;	/* Column heading first character */
    char *endcol;	/* Column heading last character */
    char *headlast;
    char *hyphens;
    char *hyphlast;
    char *nextab;
    int icol;
    int nbytes, nba;

    /* Return if no column names in header */
    headlast = strchr (tabtable->tabhead, newline);
    if (headlast == tabtable->tabhead)
	return (0);

    /* Count columns in table header */
    tabtable->ncols = 1;
    for (colhead = tabtable->tabhead; colhead < headlast; colhead++) {
	if (*colhead == tab)
	    tabtable->ncols++;
	}

    /* Tabulate column names */
    nbytes = tabtable->ncols * sizeof (char *);
    nba = nbytes / 64;
    if (nbytes > nba * 64)
	nba = (nba + 1) * 64;
    else
	nba = nba * 64;
    tabtable->colname = (char **)calloc (tabtable->ncols, sizeof (char *));
    tabtable->lcol = (int *) calloc (tabtable->ncols, sizeof (int));
    colhead = tabtable->tabhead;
    for (icol = 0; icol < tabtable->ncols; icol++) {
	nextab = strchr (colhead, tab);
	if (nextab < headlast)
	    endcol = nextab - 1;
	else
	    endcol = headlast - 1;
	while (*endcol == ' ')
	    endcol = endcol - 1;
	tabtable->lcol[icol] = (int) (endcol - colhead) + 1;
	tabtable->colname[icol] = colhead;
	colhead = nextab + 1;
	if (colhead > headlast)
	    break;
	}

    /* Tabulate field widths */
    hyphens = headlast + 1;
    hyphlast = strchr (hyphens, newline);
    if (hyphlast == hyphens)
	return (0);
    tabtable->lcfld = (int *) calloc (tabtable->ncols, sizeof (int));
    colhead = hyphens;
    for (icol = 0; icol < tabtable->ncols; icol++) {
	if ((nextab = strchr (colhead, tab)) == NULL)
	    endcol = hyphlast - 1;
	else
	    endcol = nextab - 1;
	tabtable->lcfld[icol] = (int) (endcol - colhead) + 1;
	if (nextab != NULL)
	    colhead = nextab + 1;
	else
	    break;
	}

    return (tabtable->ncols);
}


/* Search table of column headings for a particlar entry (case-dependent) */

int
tabcol (tabtable, keyword)

struct TabTable *tabtable;	/* Tab table structure */
char	*keyword;		/* Column heading to find */

{
    int i, lkey, lcol;
    lkey = strlen (keyword);

    for (i = 0; i < tabtable->ncols; i++) {
	lcol = tabtable->lcol[i];
	if (lcol == lkey &&
	    !strncmp (keyword, tabtable->colname[i], lcol)) {
	    return (i + 1);
	    }
	}
    return (0);
}



/* Search table of column headings for a particlar entry (case-independent) */

int
tabccol (tabtable, keyword)

struct TabTable *tabtable;	/* Tab table structure */
char	*keyword;		/* Column heading to find */

{
    int i, lkey, lcol;
    lkey = strlen (keyword);

    for (i = 0; i < tabtable->ncols; i++) {
	lcol = tabtable->lcol[i];
	if (lcol == lkey &&
	    !strncasecmp (keyword, tabtable->colname[i], lcol)) {
	    return (i + 1);
	    }
	}
    return (0);
}


/* Search table of column headings for first with string (case-dependent) */

static int
tabcont (tabtable, keyword)

struct TabTable *tabtable;	/* Tab table structure */
char	*keyword;		/* Part of column heading to find */

{
    int i;

    for (i = 0; i < tabtable->ncols; i++) {
	if (strnsrch (tabtable->colname[i], keyword, tabtable->lcol[i])) {
	    return (i + 1);
	    }
	}
    return (0);
}


/* Search table of column headings for first with string (case-independent) */

static int
tabccont (tabtable, keyword)

struct TabTable *tabtable;	/* Tab table structure */
char	*keyword;		/* Part of column heading to find */

{
    int i;

    for (i = 0; i < tabtable->ncols; i++) {
	if (strncsrch (tabtable->colname[i], keyword, tabtable->lcol[i])) {
	    return (i + 1);
	    }
	}
    return (0);
}



/* TABSIZE -- return size of file in bytes */

static int
tabsize (filename)

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


/* ISTAB -- Return 1 if tab table file, else 0 */

int
istab (filename)

char    *filename;      /* Name of file to check */
{
    struct TabTable *tabtable;

    /* First check file extension */
    if (strsrch (filename, ".tab"))
	return (1);

    /* If no .tab file extension, try opening the file */
    else {
	if ((tabtable = tabopen (filename, 10000)) != NULL) {
	    tabclose (tabtable);
	    return (1);
	    }
	else
	    return (0);
	}
}

/* Jul 18 1996	New subroutines
 * Aug  6 1996	Remove unused variables after lint
 * Aug  8 1996	Fix bugs in entry reading and logging
 * Oct 15 1996  Add comparison when testing an assignment
 * Nov  5 1996	Drop unnecessary static declarations
 * Nov 13 1996	Return no more than maximum star number
 * Nov 13 1996	Write all error messages to stderr with subroutine names
 * Nov 15 1996  Implement search radius; change input arguments
 * Nov 19 1996	Allow lower case column headings
 * Dec 18 1996	Add UJCRNUM to read specified catalog entries
 * Dec 18 1996  Keep closest stars, not brightest, if searching within radius
 *
 * Mar 20 1997	Clean up code in TABRNUM
 * May  7 1997	Set entry number to zero if column not found
 * May 29 1997	Add TABPARSE and TABCOL to more easily extract specific columns
 * May 29 1997	Add TABCLOSE to free memory from outside this file
 * Jun  4 1997	Set ID to sequence number in table if no ID/id entry present
 *
 * Jun  2 1998	Fix bug parsing last column of header
 * Jun 24 1998	Add string lengths to ra2str() and dec2str() calls
 * Sep 16 1998	Use limiting radius correctly
 * Sep 22 1998	Convert to output coordinate system
 * Oct 15 1998	Add tabsize() and istab()
 * Oct 21 1998	Add tabrkey() to read values of keyword for list of stars
 * Oct 29 1998	Correctly assign numbers when too many stars are found
 * Oct 30 1998	Fix istab() to check only first line of file
 * Dec  8 1998	Do not declare tabsize() static

 * Jan 20 1999	Add tabcatopen() and keep table info in structure, not global
 * Jan 25 1999	Add lcfld to structure to keep track of field widths
 * Jan 29 1999	Add current line number and pointer to table structure
 * Feb  2 1999	Allow for equinox other than 2000.0 in tab table header
 * Feb  2 1999	Add tabhgetc() to read char string values from tab table header
 * Feb 17 1999	Increase maximum line in istab() from 80 to 1024
 * Mar  2 1999	Fix bugs calling tabhgetx()
 * Mar  2 1999	Rewrite tabhgetx() to use tabhgetc() for all header reading
 * May 28 1999	Add tabcatopen() and tabstar() and use them
 * Jun  3 1999	Fix bug so header parameters are read correctly
 * Jun 16 1999	Use SearchLim()
 * Aug 16 1999	Fix bug to fix failure to search across 0:00 RA
 * Aug 25 1999  Return real number of stars from tabread()
 * Sep 10 1999	Fix bug setting equinox and coordinate system in tabcatopen()
 * Sep 10 1999	Set additional keyword selection with subroutine
 * Sep 13 1999	Fix comment for tabstar()
 * Sep 16 1999	Fix bug which didn't always return closest stars
 * Sep 16 1999	Add distsort argument so brightest stars in circle works, too
 * Oct 21 1999	Clean up code after lint
 * Oct 25 1999	Fix subroutine declaration inconsistency
 * Oct 25 1999	Replace malloc() calls with calloc()
 * Oct 29 1999	Add tabxyread() for image catalogs
 * Nov 23 1999	Improve error checking on Starbase tables; istab() opens file
 * Nov 30 1999	Fix bugs found when compiling under SunOS 4.1.3
 *
 * Jan  4 2000	Always close file and print error message on tabopen() failures
 * Jan  6 2000	If "id" not found, try heading with "_id" to catch scat output
 * Jan 10 2000	Add second magnitude; save column headers in catalog structure
 * Feb 10 2000	Implement proper motion in source catalogs
 * Feb 10 2000	Accept first mag-containing column as first magnitude
 * Feb 10 2000	Clean up id reading: no. decimals, non-numeric id
 * Feb 14 2000	Save table opening errors in string
 * Feb 16 2000	Lengthen short calloc() lengths
 * Feb 16 2000	Pad tabbuff with 2 nulls so end can be found
 * Mar 10 2000	Return proper motions from tabread() and tabrnum()
 * Mar 13 2000	Do not free tabtable structure if it is null
 * Mar 27 2000	Clean up code after lint
 * May 26 2000	Add ability to read named tables in a multi-table file
 * Jun 26 2000	Add coordinate system to SearchLim() arguments
 * Jul  5 2000	Check for additional column heading variations
 * Jul 10 2000	Deal with number of decimal places and name/number in tabcatopen()
 * Jul 12 2000	Add star catalog data structure to tabread() argument list
 * Jul 13 2000	Use nndec header parameter to optionally set decimal places
 * Jul 25 2000	Pass star catalog address of data structure address
 * Aug  3 2000	Skip first character of ID if rest is number
 * Aug  3 2000	If no decimal point in numeric ID, set ndec to zero
 * Sep 27 2000	Use first column with name if no id column is found
 * Oct 26 2000	Add proper motion in seconds and arcseconds per century
 * Oct 31 2000	Add proper motion in milliseconds of time per year
 * Nov 22 2000	Add tabtable argument to tabcatopen() for URL search returns
 * Nov 28 2000	Add starcat as argument to tabrnum() as well as tabread()
 * Nov 29 2000	Do not set tmagb if it is null; set type if present
 * Nov 30 2000	Add spectral type as possible column
 * Dec  1 2000	Add field as synonym for plate
 * Dec 18 2000	Clean up after lint
 * Dec 29 2000	Clean up after lint
 *
 * May 29 2001	Save length of identifier in catalog structure
 * May 30 2001	Rewrite to deal with up to 3 magnitudes
 * Jun 11 2001	Add one-line-at-a-time catalog reading from tabcatopen()
 * Jun 14 2001	In tabopen(), use actual file size if less than buffer size
 * Jun 14 2001	In tabcol() make sure lengths are equal, too
 * Jun 18 2001	Fix bug finding of length of token string extracted
 * Jun 19 2001	Use setoken() to parse each catalog line
 * Jun 20 2001	Add fourth magnitude for GSC II
 * Jun 25 2001	Check fgets() return in gettabline()
 * Jun 25 2001	Print star read errors only in verbose mode
 * Jun 28 2001	If no proper motion unit is set, set proper motion to 0.0
 * Jun 28 2001	If first magnitude is 99.90, sort by second magnitude
 * Jun 28 2001	Up default line limit from 1 million to 10 million
 * Jul  2 2001	Fix order of 3rd and 4th magnitudes
 * Jul 20 2001	Return magnitude as well as flux from tabxyread()
 * Aug  8 2001	Return radial velocity as additional magnitude
 * Aug 17 2001	Fix bug reading radial velocity
 * Aug 21 2001	Check numbers using isnum() in tabgetr8() and tabgeti4()
 * Aug 21 2001	Add starcat to tabrkey() argument list
 * Aug 21 2001	Read object name into tkey if it is present
 * Sep 11 2001	Allow an arbitrary number of magnitudes
 * Sep 11 2001  Add sort magnitude argument to tabread()
 * Sep 19 2001	Drop fitshead.h; it is in wcs.h
 * Sep 20 2001	Clean up pointers for Alpha and Linux; drop tabgetpm()
 * Sep 20 2001	Change _ to . in ESO server USNO-A2.0 numbers
 * Sep 21 2001	Rearrange ESO server GSC numbers
 * Oct 12 2001	Check for additional column names for magnitude
 * Oct 15 2001	Read first star only once, compute relative flux in tabxyread() (bug fix)
 * Oct 16 2001	Add pointer to line of dashes to table structure
 * Dec  3 2001	Initialize keyword search variables in tabhgetc()
 *
 * May  6 2002	Allow object names to be up to 79 characters long
 * Aug  5 2002	Deal correctly with magnitude-less catalogs
 * Aug  5 2002	Add magu, magb, magv for UBV magnitudes
 * Aug  6 2002	Pass through magnitude keywords
 * Aug  6 2002	Return initial string if token not found by tabgetc()
 * Oct 30 2002	Add code to pass epoch as a final magnitude (but not RV, yet)
 *
 * Jan 26 2003	Add code to pass USNO-B1.0 pm quality and no. of ids
 * Jan 28 2003	Improve spatial position test
 * Feb  4 2003	Compare character to 0, not NULL
 * Mar 11 2003	Fix limit setting
 * Apr  3 2003	Drop unused variables after lint
 * May 28 2003	Read long and lat if radecsys is "galactic" or "ecliptic"
 * Jun  2 003	Divide by cos(dec) for arcsec and mas RA proper motions
 * Aug 22 2003	Add radi argument for inner edge of search annulus
 * Sep 25 2003	Add tabbin() to fill an image with sources
 * Oct  6 2003	Update tabread() and tabbin() for improved RefLim()
 * Nov 18 2003	Initialize image size and bits/pixel from header in tabbin()
 * Nov 22 2003	Add GSC II object class (c) as possible content for tpeak
 * Dec  4 2003	Add default of arcsec/century as proper motion unit
 * Dec 10 2003	If magnitude ends in L and is a number, add 100.0
 * Dec 12 2003	Fix bug in wcs2pix() call in tabbin()
 *
 * Jan  5 2004	If more than 15 digits in numberic ID, drop excess off front
 * Mar 16 2004	Be more clever about reading by number
 * Mar 19 2004	Make verbose flag global
 * Nov 17 2004	Accept SpT and spt before type for spectral type
 *
 * May 12 2005	Add "num" as a possible substring to identify an ID column
 * Jul 26 2005	Fix bug to set magnitudes and keywords correctly in tabrnum()
 * Aug  3 2005	If nlog < 0 in tabrnum(), print objects as found
 * Aug 10 2005	Add "exp" as possible "magnitude" column heading
 * Aug 11 2005	Do not re-read lines if missing object numbers in tabrnum()
 * Aug 17 2005	If nmax is -1, print magnitude keywords from file
 * Sep 29 2005	Read first 10000 characters for istab() to capture long heads
 * Sep 29 2005	Add rasort header flag if catalog is sorted by RA
 *
 * Jun 15 2006	Read ID field length from header, if present
 * Jun 20 2006	Drop unused variables; initialize uninitialized variables
 * Jun 30 2006	Add match argument to tabrnum() to add sequential read option
 *
 * Jan 11 2007	Switch order of wcscat.h and fitsfile.h includes
 * Mar 13 2007	Return object name if first character of ID is non-numeric
 * Mar 13 2007	Check for "Class" for GSC2 object class
 * Jul  9 2007	Reject mflag as a magnitude
 * Jul  9 2007	Sort by designated magnitude only unless sort mag is 0
 * Jul 18 2007	Add tabccol() and tabccont() case-insensitive header searches
 * Jul 19 2007	Add proper motion in arsec/hour for solar system objects
 * Jul 23 2007	Add ...obj... as possible identifier
 * Jul 23 2007	Add ...dist... as possible "magnitude"
 *
 * Aug 17 2009	Fix columns for declination column name
 * Sep 25 2009	Fix memory leaks found by Douglas Burke
 * Sep 30 2009	Fix bugs freeing object names for first pass and farthest star
 */
