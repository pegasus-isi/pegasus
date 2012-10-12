/*** File libwcs/ubcread.c
 *** December 05, 2007
 *** By Doug Mink, dmink@cfa.harvard.edu
 *** Harvard-Smithsonian Center for Astrophysics
 *** Copyright (C) 2003-2007
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

 * Subroutines to read from the USNO-B1.0 catalog
 */

#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <math.h>
#include "wcs.h"
#include "fitsfile.h"
#include "wcscat.h"

static int ucat=UB1;

/* USNO B-1.0 directory pathname; replaced by UB1_PATH environment variable.
 * This may also be a URL to a catalog search engine */
static char ub1path[64]="/data/ub1";

/* USNO YB6 directory pathname; replaced by YB6_PATH environment variable.
 * This may also be a URL to a catalog search engine */
static char yb6path[64]="/data/astrocat2/usnoyb6";

static char *upath;

typedef struct {
    int rasec, decsec, pm, pmerr, poserr, mag[5], magerr[5], index[5];
} UBCstar;

static int nstars;	/* Number of stars in catalog */
static int cswap = 0;	/* Byte reverse catalog to Intel/DEC order if 1 */
static double *udist;	/* Array of distances to stars */
static int ndist = 0;	/* Number of stars in distance array */
static int minpmqual = 3; /* Proper motion quality limit (0=bad, 9=good)*/
void setminpmqual (n)
int n; { minpmqual = n; return; }
int getminpmqual ()
{ return (minpmqual); }

static int minid = 0; /* Minimum number of plate ID's (<0 excludes Tycho-2) */
void setminid (n)
int n; { minid = n; return; }
int getminid ()
{ return (minid); }


static FILE *fcat;
#define ABS(a) ((a) < 0 ? (-(a)) : (a))
#define NZONES 1800

static double ubcra();
static double ubcdec();
static double ubcmag();
static double ubcpra();
static double ubcpdec();
static int ubcpmq();
static int ubcsg();
int ubcmagerr();
static int ubcndet();

static int ubczones();
static int ubczone();
static int ubcsra();
static int ubcopen();
static int ubcpath();
static int ubcstar();
static void ubcswap();
static int nbent = 80;


/* UBCREAD -- Return USNO B1.0 or YB6 sources in specified region */

int
ubcread (refcatname,distsort,cra,cdec,dra,ddec,drad,dradi,sysout,eqout,epout,
	 mag1,mag2,sortmag,nstarmax,unum,ura,udec,upra,updec,umag,upmni,nlog)

char	*refcatname;	/* Name of catalog (UB1 only, for now) */
int	distsort;	/* 1 to sort stars by distance from center */
double	cra;		/* Search center J2000 right ascension in degrees */
double	cdec;		/* Search center J2000 declination in degrees */
double	dra;		/* Search half width in right ascension in degrees */
double	ddec;		/* Search half-width in declination in degrees */
double	drad;		/* Limiting separation in degrees (ignore if 0) */
double	dradi;		/* Inner edge of search annulus in degrees (ignore if 0) */
int	sysout;		/* Search coordinate system */
double	eqout;		/* Search coordinate equinox */
double	epout;		/* Proper motion epoch (0.0 for no proper motion) */
double	mag1,mag2;	/* Limiting magnitudes (none if equal) */
int	sortmag;	/* Magnitude by which to sort (1 or 2) */
int	nstarmax;	/* Maximum number of stars to be returned */
double	*unum;		/* Array of UB numbers (returned) */
double	*ura;		/* Array of right ascensions (returned) */
double	*udec;		/* Array of declinations (returned) */
double	*upra;		/* Array of right ascension proper motions (returned) */
double	*updec;		/* Array of declination proper motions (returned) */
double	**umag;		/* Array of red and blue magnitudes (returned) */
int	*upmni;		/* Array of number of ids and pm quality (returned) */
int	nlog;		/* Logging interval */
{
    double ra1,ra2;	/* Limiting right ascensions of region in degrees */
    double dec1,dec2;	/* Limiting declinations of region in degrees */
    int nz;		/* Number of input UB zone files */
    int zlist[NZONES];	/* List of input UB zones */
    UBCstar star;	/* UB catalog entry for one star */
    double dist = 0.0;	/* Distance from search center in degrees */
    double faintmag=0.0; /* Faintest magnitude */
    double maxdist=0.0; /* Largest distance */
    double rdist, ddist;
    int	faintstar=0;	/* Faintest star */
    int	farstar=0;	/* Most distant star */
    int pmqual;
    int sysref=WCS_J2000;	/* Catalog coordinate system */
    double eqref=2000.0;	/* Catalog equinox */
    double epref=2000.0;	/* Catalog epoch */

    double rra1, rra2, rdec1, rdec2;
    double num;		/* UB numbers */
    double rapm, decpm, rapm0, decpm0;
    int wrap, iwrap;
    int verbose;
    int znum, itot,iz, i;
    int jtable,jstar;
    int itable = 0;
    int objtype = 0;
    int nstar, nread, pass;
    int ubra1, ubra2, ubdec1, ubdec2;
    int nsg, isg, qsg;
    double ra,dec, ra0, dec0;
    double mag, magtest, secmarg;
    int istar, istar1, istar2, pmni, nid;
    int nzmax = NZONES;	/* Maximum number of declination zones */
    int magsort;
    char *str;
    char cstr[32], rastr[32], numstr[32], decstr[32], catid[32];
    char *title;

    itot = 0;
    if (nlog > 0)
	verbose = 1;
    else
	verbose = 0;

    /* Set catalog code and path to catalog */
    if (strncasecmp (refcatname,"ub",2)==0) {
	if ((str = getenv("UB1_PATH")) != NULL)
	    strcpy (ub1path,str);
	ucat = UB1;
	upath = ub1path;
	}
    else if (strncasecmp (refcatname,"yb",2)==0) {
	if ((str = getenv("YB6_PATH")) != NULL)
	    strcpy (yb6path,str);
	ucat = YB6;
	upath = yb6path;
	}
    else {
	fprintf (stderr, "UBCREAD:  %s not a USNO catalog\n", refcatname);
	return (0);
	}

    if (strchr (refcatname, 't'))
	objtype = 1;
    else
	objtype = 0;

    /* If root pathname is a URL, search and return */
    if (!strncmp (upath, "http:",5)) {
	return (webread (upath,refcatname,distsort,cra,cdec,dra,ddec,drad,
			 dradi,sysout,eqout,epout,mag1,mag2,sortmag,nstarmax,
			 unum,ura,udec,upra,updec,umag,upmni,nlog));
	}

    wcscstr (cstr, sysout, eqout, epout);

    SearchLim (cra,cdec,dra,ddec,sysout,&ra1,&ra2,&dec1,&dec2,verbose);

    /* mag1 is always the smallest magnitude */
    if (mag2 < mag1) {
	mag = mag2;
	mag2 = mag1;
	mag1 = mag;
	}

    /* Sort by 4th magnitude = R2 if sort magnitude is not set */
    if (sortmag > 0 && sortmag < 6)
	magsort = sortmag - 1;
    else
	magsort = 3;

    /* Add 60 arcsec/century margins to region to get most stars which move */
    if (minpmqual < 11 && (epout != 0.0 || sysout != sysref))
	secmarg = 60.0;
    else
	secmarg = 0.0;

    /* Find RA and Dec limits in catalog coordinate system */
    RefLim (cra,cdec,dra,ddec,sysout,sysref,eqout,eqref,epout,epref,secmarg,
	    &rra1, &rra2, &rdec1, &rdec2, &wrap, verbose);

    /* Find declination zones to search */
    nz = ubczones (rra1, rra2, rdec1, rdec2, nzmax, zlist, verbose);
    if (nz <= 0) {
	fprintf (stderr, "UBCREAD:  no USNO B zones found\n");
	return (0);
	}

    /* Write header if printing star entries as found */
    if (nstarmax < 1) {
	char *revmessage;
	revmessage = getrevmsg();
	title = CatName (ucat, refcatname);
	printf ("catalog	%s\n", title);
	free ((char *)title);
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
	printf ("epoch  %.3f\n", epout);
	printf ("program        scat %s\n", revmessage);
	CatID (catid, ucat);
	if (objtype) {
	    printf ("%s	ra          	dec         	", catid);
	    printf ("magb1 	magr1 	magb1 	magb2 	magn  	");
	    printf ("sgb1 	sgr1 	sgb1 	sgb2	");
	    printf ("ura  	udec	pm	ni	qsg	arcmin\n");
	    printf ("------------	------------	------------	");
	    printf ("-----	-----	-----	-----	-----	");
	    printf ("----	----	----	----	");
	    printf ("-----	-----	--	--	---	------\n");
	    }
	else {
	    printf ("%s	ra          	dec         	", catid);
	    printf ("magb1 	magr1 	magb1 	magb2 	magn  	ura  	");
	    printf ("udec	pm	ni	qsg	arcmin\n");
	    printf ("------------	------------	------------	");
	    printf ("-----	-----	-----	-----	-----	-----	");
	    printf ("-----	--	--	---	------\n");
	    }
	}

    /* Convert RA and Dec limits to same units as catalog for quick filter */
    ubra1 = (int) (rra1 * 360000.0 + 0.5);
    ubra2 = (int) (rra2 * 360000.0 + 0.5);
    ubdec1 = (int) ((rdec1 * 360000.0) + 32400000.5);
    ubdec2 = (int) ((rdec2 * 360000.0) + 32400000.5);

    /* Convert dra to angular units for rectangular box on sky */
    dra = dra / cos (degrad (cdec));
    
    if (nstarmax > ndist) {
	if (ndist > 0)
	    free ((void *)udist);
	udist = (double *) malloc (nstarmax * sizeof (double));
	if (udist == NULL) {
	    fprintf (stderr,"UBCREAD:  cannot allocate separation array\n");
	    return (0);
	    }
	ndist = nstarmax;
	}

    /* Loop through region list */
    nstar = 0;
    for (iz = 0; iz < nz; iz++) {

    /* Get path to zone catalog */
	znum = zlist[iz];
	if ((nstars = ubcopen (znum)) != 0) {

	    jstar = 0;
	    jtable = 0;
	    for (iwrap = 0; iwrap <= wrap; iwrap++) {

	    /* Find first star based on RA */
		if (iwrap == 0 || wrap == 0) {
		    istar1 = ubcsra (rra1);
		    if (istar1 > 1)
			istar1 = istar1 - 1;
		    }
		else
		    istar1 = 1;

	    /* Find last star based on RA */
		if (iwrap == 1 || wrap == 0) {
		    istar2 = ubcsra (rra2);
		    if (istar2 < nstars)
			istar2 = istar2 + 1;
		    }
		else
		    istar2 = nstars;

		if (istar1 == 0 || istar2 == 0)
		    break;

		nread = istar2 - istar1 + 1;
		itable = 0;

	    /* Loop through zone catalog for this region */
		for (istar = istar1; istar <= istar2; istar++) {
		    itable ++;
		    jtable ++;

		    if (ubcstar (istar, &star)) {
			fprintf (stderr,"UBCREAD: Cannot read star %d\n", istar);
			break;
			}

		/* Extract selected fields */

		/* Check rough position limits */
     		    if ((star.decsec >= ubdec1 && star.decsec <= ubdec2) &&
			((wrap && (star.rasec>=ubra1 || star.rasec<=ubra2)) ||
			(!wrap && (star.rasec>=ubra1 && star.rasec<=ubra2))
			)){

			/* Set magnitude by which to sort and test */
			mag = ubcmag (star.mag[magsort]);
			if (sortmag == 0) {
			    if (mag > 30.0)
				mag = ubcmag (star.mag[1]);
			    if (mag > 30.0)
				mag = ubcmag (star.mag[2]);
			    if (mag > 30.0)
				mag = ubcmag (star.mag[0]);
			    }
			pass = 1;
			if (mag1 != mag2 && (mag < mag1 || mag > mag2))
			    pass = 0;

			if (pass) {
			    nid = ubcndet (star.pmerr);
			    if (nid < minid) {
				if (minid > 0 && nid > 0)
				    pass = 0;
				}
			    if (minid < 0 && nid < -minid)
				pass = 0;
			    }

			/* Test distance limits */
			if (pass) {
			    ra0 = ubcra (star.rasec);
			    dec0 = ubcdec (star.decsec);
			    ra = ra0;
			    dec = dec0;
			    pmqual = ubcpmq (star.pm);
			    if (nid == 0)
				pmqual = 10;
			    nsg = 0;
			    qsg = 0;
			    for (i = 0; i < 4; i++) {
				if (star.mag[i] > 0) {
				    isg = ubcsg (star.mag[i]);
				    if (isg > 0) {
					nsg++;
					qsg = qsg + isg;
					}
				    }
				}
			    if (pmqual == 10 || nsg < 1)
				qsg = 12;
			    else
				qsg = qsg / nsg;
			    pmni = (10000 * qsg) + (100 * pmqual) + nid;

			    /* Convert to search equinox and epoch */
			    if (pmqual < minpmqual) {
				rapm = 0.0;
				decpm = 0.0;
				rapm0 = 0.0;
				decpm0 = 0.0;
				wcscon (sysref,sysout,eqref,eqout,
					&ra,&dec,epout);
				}
			    else {
				rapm0 = ubcpra (star.pm) / cos (degrad (dec));
				decpm0 = ubcpdec (star.pm);
				rapm = rapm0;
				decpm = decpm0;
				wcsconp (sysref,sysout,eqref,eqout,epref,epout,
					 &ra, &dec, &rapm, &decpm);
				}

			    if (distsort || drad > 0.0)
				dist = wcsdist (cra,cdec,ra,dec);
			    else
				dist = 0.0;

			/* Test spatial limits */
			    if (drad > 0.0) {
				if (dist > drad)
				    pass = 0;
				if (dradi > 0.0 && dist < dradi)
				    pass = 0;
				}
			    else {
				rdist = wcsdist (cra,dec,ra,dec);
				if (rdist > dra)
				    pass = 0;
				ddist = wcsdist (ra,cdec,ra,dec);
				if (ddist > ddec)
				    pass = 0;
				}
			    }

			if (pass) {
			    num = (double) znum +
				  (0.0000001 * (double)istar);

			/* Write star position and magnitudes to stdout */
			    if (nstarmax < 1) {
				CatNum (ucat, -12, 0, num, numstr);
				ra2str (rastr, 31, ra, 3);
				dec2str (decstr, 31, dec, 2);
				dist = wcsdist (cra,cdec,ra,dec) * 60.0;
				printf ("%s	%s	%s", numstr,rastr,decstr);
				for (i = 0; i < 5; i++)
				    printf ("	%.2f",ubcmag(star.mag[i]));
				if (objtype) {
				    for (i = 0; i < 4; i++)
					printf ("	%2d",ubcsg(star.mag[i]));
				    }
				printf ("	%6.1f	%6.1f",
					rapm * 3600000.0 * cosdeg(dec),
					decpm * 3600000.0);
				printf ("	%d	%d	%d",
					pmqual, nid, qsg);
				printf ("	%.2f\n", dist/60.0);
				}

			    /* Save star position and magnitude in table */
			    else if (nstar < nstarmax) {
				unum[nstar] = num;
				ura[nstar] = ra;
				udec[nstar] = dec;
				upra[nstar] = rapm;
				updec[nstar] = decpm;
				for (i = 0; i < 5; i++)
				    umag[i][nstar] = ubcmag (star.mag[i]);
				upmni[nstar] = pmni;
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
				    udec[farstar] = dec;
				    upra[farstar] = rapm;
				    for (i = 0; i < 5; i++)
					umag[i][farstar] = ubcmag (star.mag[i]);
				    upmni[farstar] = pmni;
				    udist[farstar] = dist;

				/* Find new farthest star */
				    if (nstarmax > 1) {
					maxdist = 0.0;
					for (i = 0; i < nstarmax; i++) {
					    if (udist[i] > maxdist) {
						maxdist = udist[i];
						farstar = i;
						}
					    }
					}
				    else {
					maxdist = dist;
					farstar = 0;
					}
				    }
				}

			    /* If too many stars, replace faintest star */
			    else if (mag < faintmag) {
				unum[faintstar] = num;
				ura[faintstar] = ra;
				udec[faintstar] = dec;
				upra[faintstar] = rapm;
				updec[faintstar] = decpm;
				for (i = 0; i < 5; i++)
				    umag[i][faintstar] = ubcmag (star.mag[i]);
				upmni[faintstar] = pmni;
				udist[faintstar] = dist;

			    /* Find new faintest star */
				faintmag = 0.0;
				for (i = 0; i < nstarmax; i++) {
				    magtest = umag[magsort][i];
				    if (sortmag == 0) {
					if (faintmag < 30.0)
					    faintmag = umag[1][i];
					if (faintmag < 30.0)
					    faintmag = umag[2][i];
					if (faintmag < 30.0)
					    faintmag = umag[0][i];
					}
				    if (magtest > faintmag) {
					faintmag = magtest;
					faintstar = i;
					}
				    }
				}
			    nstar++;
			    jstar++;
			    if (nlog == 1) {
				fprintf (stderr,"UBCREAD: %04d.%07d: %9.5f %9.5f %s\n",
					znum,istar,ra,dec,cstr);
				for (i = 0; i < 5; i++)
				    fprintf (stderr, " %5.2f", ubcmag(star.mag[i]));
				fprintf (stderr,"\n");
				}

			    /* End of accepted star processing */
			    }

		    /* End of individual star processing */
			}

		/* Log operation */
		    if (nlog > 0 && itable%nlog == 0)
			fprintf (stderr,"UBCREAD: zone %d (%2d / %2d) %8d / %8d / %8d sources\r",
				znum, iz+1, nz, jstar, itable, nread);

		/* End of star loop */
		    }

		/* End of wrap loop */
		}

	/* Close zone input file */
	    (void) fclose (fcat);
	    itot = itot + itable;
	    if (nlog > 0)
		fprintf (stderr,"UBCREAD: zone %d (%2d / %2d) %8d / %8d / %8d sources      \n",
			znum, iz+1, nz, jstar, jtable, nstars);

	/* End of zone processing */
	    }

    /* End of zone loop */
	}

/* Summarize search */
    if (nlog > 0) {
	if (nz > 1)
	    fprintf (stderr,"UBCREAD: %d zones: %d / %d found\n",nz,nstar,itot);
	else
	    fprintf (stderr,"UBCREAD: 1 zone: %d / %d found\n",nstar,itable);
	if (nstar > nstarmax)
	    fprintf (stderr,"UBCREAD: %d stars found; only %d returned\n",
		     nstar,nstarmax);
	}
    return (nstar);
}


/* UBCRNUM -- Return USNO-B1.0 sources with specified ID numbers */

int
ubcrnum (refcatname,nnum,sysout,eqout,epout,unum,ura,udec,upra,updec,umag,upmni,nlog)

char	*refcatname;	/* Name of catalog (UBC, USAC, UBC2, USAC2) */
int	nnum;		/* Number of stars to find */
int	sysout;		/* Search coordinate system */
double	eqout;		/* Search coordinate equinox */
double	epout;		/* Proper motion epoch (0.0 for no proper motion) */
double	*unum;		/* Array of UB numbers to find */
double	*ura;		/* Array of right ascensions (returned) */
double	*udec;		/* Array of declinations (returned) */
double	*upra;		/* Array of right ascension proper motions (returned) */
double	*updec;		/* Array of declination proper motions (returned) */
double	**umag;		/* Array of blue and red magnitudes (returned) */
int	*upmni;		/* Array of number of ids and pm quality (returned) */
int	nlog;		/* Logging interval */
{
    UBCstar star;	/* UB catalog entry for one star */
    int sysref=WCS_J2000;	/* Catalog coordinate system */
    double eqref=2000.0;	/* Catalog equinox */
    double epref=2000.0;	/* Catalog epoch */

    int znum;
    int jnum;
    int i;
    int nzone;
    int nfound = 0;
    int pmqual;
    double ra,dec;
    double rapm, decpm;
    double dstar;
    int istar, pmni, nid;
    int nsg, qsg;
    char *str;

    /* Set catalog code and path to catalog */
    if (strncasecmp (refcatname,"ub",2)==0) {
	if ((str = getenv("UB1_PATH")) != NULL)
	    strcpy (ub1path,str);
	ucat = UB1;
	upath = ub1path;
	}
    else if (strncasecmp (refcatname,"yb",2)==0) {
	if ((str = getenv("YB6_PATH")) != NULL)
	    strcpy (yb6path,str);
	ucat = YB6;
	upath = yb6path;
	}
    else {
	fprintf (stderr, "UBCREAD:  %s not a USNO catalog\n", refcatname);
	return (0);
	}

    /* If root pathname is a URL, search and return */
    if (!strncmp (upath, "http:",5)) {
	return (webrnum (upath,refcatname,nnum,sysout,eqout,epout,1,
			 unum,ura,udec,upra,updec,umag,upmni,nlog));
	}


/* Loop through star list */
    for (jnum = 0; jnum < nnum; jnum++) {

    /* Get path to zone catalog */
	znum = (int) unum[jnum];
	if ((nzone = ubcopen (znum)) != 0) {
	    dstar = (unum[jnum] - znum) * 10000000.0;
	    istar = (int) (dstar + 0.5);
	    if (istar > nzone) {
		fprintf (stderr,"UBCRNUM: Star %d > max. in zone %d\n",
			 istar,nzone);
		break;
		}

	    if (ubcstar (istar, &star)) {
		fprintf (stderr,"UBCRNUM: Cannot read star %d\n", istar);
		break;
		}

	    /* Extract selected fields */
	    else {
		ra = ubcra (star.rasec); /* Right ascension in degrees */
		dec = ubcdec (star.decsec); /* Declination in degrees */
		pmqual = ubcpmq (star.pm);
		nid = ubcndet (star.pmerr);
		if (nid == 0)
		    pmqual = 10;
		nsg = 0;
		qsg = 0;
		for (i = 0; i < 4; i++) {
		    if (star.mag[i] > 0) {
			nsg++;
			qsg = qsg + ubcsg (star.mag[i]);
			}
		    }
		if (pmqual == 10 || nsg < 1)
		    qsg = 12;
		else
		    qsg = qsg / nsg;
		pmni = (10000 * qsg) + (100 * pmqual) + nid;

		/* Convert to desired equinox and epoch */
		if (pmqual < minpmqual) {
		    rapm = 0.0;
		    decpm = 0.0;
		    wcscon (sysref,sysout,eqref,eqout,&ra,&dec,epout);
		    }
		else {
		    rapm = ubcpra (star.pm);
		    decpm = ubcpdec (star.pm);
		    wcsconp (sysref,sysout,eqref,eqout,epref,epout,
			     &ra, &dec, &rapm, &decpm);
		    }

		/* Save star position and magnitude in table */
		ura[nfound] = ra;
		udec[nfound] = dec;
		upra[nfound] = rapm;
		updec[nfound] = decpm;
		upmni[nfound] = pmni;
		for (i = 0; i< 5; i++)
		    umag[i][nfound] = ubcmag (star.mag[i]);

		nfound++;
		if (nlog == 1) {
		    fprintf (stderr,"UBCRNUM: %04d.%08d: %9.5f %9.5f",
			     znum,istar,ra,dec);
		    for (i = 0; i < 5; i++)
			fprintf (stderr, " %5.2f", ubcmag(star.mag[i]));
		    fprintf (stderr, "\n");
		    }

		/* Log operation */
		if (nlog > 0 && jnum%nlog == 0)
		    fprintf (stderr,"UBCRNUM: %4d.%8d  %8d / %8d sources\r",
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
	fprintf (stderr,"UBCRNUM:  %d / %d found\n",nfound,nnum);

    return (nfound);
}


/* UBCBIN -- Fill FITS WCS image with USNO-B1.0 sources */

int
ubcbin (refcatname, wcs, header, image, mag1, mag2, sortmag, magscale, nlog)

char	*refcatname;	/* Name of catalog (UB1 only, for now) */
struct WorldCoor *wcs;	/* World coordinate system for image */
char	*header;	/* FITS header for output image */
char	*image;		/* Output FITS image */
double	mag1,mag2;	/* Limiting magnitudes (none if equal) */
int	sortmag;	/* Magnitude by which to sort (1 or 2) */
double	magscale;	/* Scaling factor for magnitude to pixel flux
			 * (number of catalog objects per bin if 0) */
int	nlog;		/* Logging interval */
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
    int nz;		/* Number of input UB zone files */
    int zlist[NZONES];	/* List of input UB zones */
    UBCstar star;	/* UB catalog entry for one star */
    double rdist, ddist;
    int pmqual;
    int sysref=WCS_J2000;	/* Catalog coordinate system */
    double eqref=2000.0;	/* Catalog equinox */
    double epref=2000.0;	/* Catalog epoch */

    double rra1, rra2, rdec1, rdec2;
    double rapm, decpm, rapm0, decpm0;
    double xpix, ypix, flux;
    int offscl;
    int wrap, iwrap;
    int verbose;
    int znum, itot,iz, i;
    int ix, iy;
    int nsg, qsg;
    int jtable,jstar;
    int itable = 0;
    int nstar, nread, pass;
    int ubra1, ubra2, ubdec1, ubdec2;
    double ra,dec, ra0, dec0;
    double mag, secmarg;
    int istar, istar1, istar2, pmni, nid;
    int nzmax = NZONES;	/* Maximum number of declination zones */
    int bitpix, w, h;	/* Image bits/pixel and pixel width and height */
    int magsort;
    char *str;
    char cstr[32];
    double logt = log(10.0);

    itot = 0;
    if (nlog > 0)
	verbose = 1;
    else
	verbose = 0;

    /* Set catalog code and path to catalog */
    if (strncasecmp (refcatname,"ub",2)==0) {
	if ((str = getenv("UB1_PATH")) != NULL)
	    strcpy (ub1path,str);
	ucat = UB1;
	upath = ub1path;
	}
    else if (strncasecmp (refcatname,"yb",2)==0) {
	if ((str = getenv("YB6_PATH")) != NULL)
	    strcpy (yb6path,str);
	ucat = YB6;
	upath = yb6path;
	}
    else {
	fprintf (stderr, "UBCBIN:  %s not a USNO catalog\n", refcatname);
	return (0);
	}

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

    /* Bin using 4th magnitude = R2 if sort magnitude is not set */
    if (sortmag > 0 && sortmag < 6)
	magsort = sortmag - 1;
    else
	magsort = 3;

    /* Set image parameters */
    bitpix = 0;
    (void)hgeti4 (header, "BITPIX", &bitpix);
    w = 0;
    (void)hgeti4 (header, "NAXIS1", &w);
    h = 0;
    (void)hgeti4 (header, "NAXIS2", &h);

    /* Add 60 arcsec/century margins to region to get most stars which move */
    if (minpmqual < 11 && (epout != 0.0 || sysout != sysref))
	secmarg = 60.0;
    else
	secmarg = 0.0;

    /* Find RA and Dec limits in catalog coordinate system */
    RefLim (cra,cdec,dra,ddec,sysout,sysref,eqout,eqref,epout,epref,secmarg,
	    &rra1, &rra2, &rdec1, &rdec2, &wrap, verbose);

    /* Find declination zones to search */
    nz = ubczones (rra1, rra2, rdec1, rdec2, nzmax, zlist, verbose);
    if (nz <= 0) {
	fprintf (stderr, "UBCBIN:  no USNO B zones found\n");
	return (0);
	}

    /* Convert RA and Dec limits to same units as catalog for quick filter */
    ubra1 = (int) (rra1 * 360000.0 + 0.5);
    ubra2 = (int) (rra2 * 360000.0 + 0.5);
    ubdec1 = (int) ((rdec1 * 360000.0) + 32400000.5);
    ubdec2 = (int) ((rdec2 * 360000.0) + 32400000.5);

    /* Convert dra to angular units for rectangular box on sky */
    dra = dra / cos (degrad (cdec));
    
    /* Loop through region list */
    nstar = 0;
    for (iz = 0; iz < nz; iz++) {

    /* Get path to zone catalog */
	znum = zlist[iz];
	if ((nstars = ubcopen (znum)) != 0) {

	    jstar = 0;
	    jtable = 0;
	    for (iwrap = 0; iwrap <= wrap; iwrap++) {

	    /* Find first star based on RA */
		if (iwrap == 0 || wrap == 0) {
		    istar1 = ubcsra (rra1);
		    if (istar1 > 1)
			istar1 = istar1 - 1;
		    }
		else
		    istar1 = 1;

	    /* Find last star based on RA */
		if (iwrap == 1 || wrap == 0) {
		    istar2 = ubcsra (rra2);
		    if (istar2 < nstars)
			istar2 = istar2 + 1;
		    }
		else
		    istar2 = nstars;

		if (istar1 == 0 || istar2 == 0)
		    break;

		nread = istar2 - istar1 + 1;
		itable = 0;

	    /* Loop through zone catalog for this region */
		for (istar = istar1; istar <= istar2; istar++) {
		    itable ++;
		    jtable ++;

		    if (ubcstar (istar, &star)) {
			fprintf (stderr,"UBCBIN: Cannot read star %d\n", istar);
			break;
			}

		/* Extract selected fields */

		/* Check rough position limits */
     		    if ((star.decsec >= ubdec1 && star.decsec <= ubdec2) &&
			((wrap && (star.rasec>=ubra1 || star.rasec<=ubra2)) ||
			(!wrap && (star.rasec>=ubra1 && star.rasec<=ubra2))
			)){

			/* Set magnitude by which to sort and test */
			mag = ubcmag (star.mag[magsort]);
			if (sortmag == 0) {
			    if (mag > 30.0)
				mag = ubcmag (star.mag[1]);
			    if (mag > 30.0)
				mag = ubcmag (star.mag[2]);
			    if (mag > 30.0)
				mag = ubcmag (star.mag[0]);
			    }
			pass = 1;
			if (mag1 != mag2 && (mag < mag1 || mag > mag2))
			    pass = 0;

			if (pass) {
			    nid = ubcndet (star.pmerr);
			    if (nid < minid) {
				if (minid > 0 && nid > 0)
				    pass = 0;
				}
			    if (minid < 0 && nid < -minid)
				pass = 0;
			    }

			/* Test distance limits */
			if (pass) {
			    ra0 = ubcra (star.rasec);
			    dec0 = ubcdec (star.decsec);
			    ra = ra0;
			    dec = dec0;
			    pmqual = ubcpmq (star.pm);
			    if (nid == 0)
				pmqual = 10;
			    nsg = 0;
			    qsg = 0;
			    for (i = 0; i < 4; i++) {
				if (star.mag[i] > 0) {
				    nsg++;
				    qsg = qsg + ubcsg (star.mag[i]);
				    }
				}
			    if (pmqual == 10 || nsg < 1)
				qsg = 12;
			    else
				qsg = qsg / nsg;
			    pmni = (10000 * qsg) + (100 * pmqual) + nid;

			    /* Convert to search equinox and epoch */
			    if (pmqual < minpmqual) {
				rapm = 0.0;
				decpm = 0.0;
				rapm0 = 0.0;
				decpm0 = 0.0;
				wcscon (sysref,sysout,eqref,eqout,
					&ra,&dec,epout);
				}
			    else {
				rapm0 = ubcpra (star.pm) / cos (degrad (dec));
				decpm0 = ubcpdec (star.pm);
				rapm = rapm0;
				decpm = decpm0;
				wcsconp (sysref,sysout,eqref,eqout,epref,epout,
					 &ra, &dec, &rapm, &decpm);
				}

			/* Test spatial limits */
			    rdist = wcsdist (cra,dec,ra,dec);
			    if (rdist > dra)
				pass = 0;
			    ddist = wcsdist (ra,cdec,ra,dec);
			    if (ddist > ddec)
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
				addpix1 (image, bitpix, w, h, 0.0, 1.0, ix, iy, flux);
				nstar++;
				jstar++;
				if (nlog == 1) {
				    flux = getpix1 (image, bitpix, w, h, 0.0, 1.0, ix, iy);
				    fprintf (stderr,"UBCBIN: %d %04d.%07d: %9.5f %9.5f %s",
					nstar,znum,nstar,ra,dec,cstr);
				    if (magscale > 0.0)
					fprintf (stderr, " %5.2f", mag);
				    fprintf (stderr," %5d %5d: %f\n", ix, iy, flux);
				    }
				}

			    /* End of accepted star processing */
			    }

		    /* End of individual star processing */
			}

		/* Log operation */
		    if (nlog > 0 && itable%nlog == 0)
			fprintf (stderr,"UBCBIN: zone %d (%2d / %2d) %8d / %8d / %8d sources\r",
				znum, iz+1, nz, jstar, itable, nread);

		/* End of star loop */
		    }

		/* End of wrap loop */
		}

	/* Close zone input file */
	    (void) fclose (fcat);
	    itot = itot + itable;
	    if (nlog > 0)
		fprintf (stderr,"UBCBIN: zone %d (%2d / %2d) %8d / %8d / %8d sources      \n",
			znum, iz+1, nz, jstar, jtable, nstars);

	/* End of zone processing */
	    }

    /* End of zone loop */
	}

/* Summarize search */
    if (nlog > 0) {
	if (nz > 1)
	    fprintf (stderr,"UBCBIN: %d zones: %d / %d found\n",nz,nstar,itot);
	else
	    fprintf (stderr,"UBCBIN: 1 zone: %d / %d found\n",nstar,itable);
	}
    return (nstar);
}


/* UBCZONES -- figure out which UB zones will need to be searched */

static int
ubczones (ra1, ra2, dec1, dec2, nzmax, zones, verbose)

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
    iz1 = ubczone (dec1);
    iz2 = ubczone (dec2);

/* Tabulate zones to search */
    i = 0;
    if (iz2 >= iz1) {
	for (iz = iz1; iz <= iz2; iz++)
	    zones[i++] = iz;
	}
    else {
	for (iz = iz2; iz <= iz1; iz++)
	    zones[i++] = iz;
	}

    nrgn = i;
    if (verbose) {
	fprintf(stderr,"UBCZONES:  %d zones: %d - %d\n",nrgn,zones[0],zones[i-1]);
	fprintf(stderr,"UBCZONES: RA: %.5f - %.5f, Dec: %.5f - %.5f\n",ra1,ra2,dec1,dec2);
	}

    return (nrgn);
}


/* UBCRA -- returns right ascension in degrees from the UB star structure */

static double
ubcra (rasec)

int rasec;	/* RA in 100ths of arcseconds from UB catalog entry */
{
    return ((double) (rasec) / 360000.0);
}


/* UBCDEC -- returns the declination in degrees from the UB star structure */

static double
ubcdec (decsec)

int decsec;	/* Declination in 100ths of arcseconds from UB catalog entry */
{
    return ((double) (decsec - 32400000) / 360000.0);
}


/* UBCMAG -- returns a magnitude from the UBC star structure */

static double
ubcmag (magetc)

int magetc;	/* Magnitude 4 bytes from UB catalog entry */
{
    double xmag;

    if (ucat == YB6)
	xmag =  0.001 * (double) magetc;
    else if (magetc < 0)
	xmag = (double) (-magetc % 10000) * 0.01;
    else
	xmag = (double) (magetc % 10000) * 0.01;
    if (xmag == 0.00)
	xmag = 99.99;
    return (xmag);
}


/* UBCPRA -- returns RA proper motion in arcsec/year from UBC star structure */

static double
ubcpra (magetc)

int magetc;	/* Proper motion field from UB catalog entry */
{
    double pm;

    if (magetc < 0)
	pm = (double) (-magetc % 10000);
    else
	pm = (double) (magetc % 10000);
    pm = ((pm * 0.002) - 10.0) / 3600.0;
    return (pm);
}


/* UBCPDEC -- returns Dec proper motion in arcsec/year from UBC star structure */

static double
ubcpdec (magetc)

int magetc;	/* Proper motion field from UB catalog entry */
{
    double pm;
    if (ucat == YB6)
	pm = (double) (magetc / 10000);
    else if (magetc < 0)
	pm = (double) ((-magetc % 100000000) / 10000);
    else
	pm = (double) ((magetc % 100000000) / 10000);
    pm = ((pm * 0.002) - 10.0) / 3600.0;
    return (pm);
}


/* UBCPMQ -- returns proper motion probability (1-9) */

static int
ubcpmq (magetc)

int magetc;	/* Quality, plate, and magnitude from UB catalog entry */
{
    if (magetc < 0)
	return (-magetc / 100000000);
    else
	return (magetc / 100000000);
}


/* UBCMAGERR -- returns 1 if magnitude is uncertain from UB star structure */

int
ubcmagerr (magetc)

int magetc;	/* Quality, plate, and magnitude from UB catalog entry */
{
    if (magetc < 0)
	return ((-magetc / 1000000000) % 10);
    else
	return ((magetc / 1000000000) % 10);
}


/* UBCNDET -- returns number of detections; 0 = Tycho-2 Catalog */

static int
ubcndet (magetc)

int magetc;	/* Quality, plate, and magnitude from UB catalog entry */
{
    if (ucat == YB6)
	return (0);
    else if (magetc < 0)
	return (-magetc / 100000000);
    else
	return (magetc / 100000000);
}


/* UBCSG -- returns closeness to star PSF (0-11) */

static int
ubcsg (magetc)

int magetc;	/* Quality, plate, and magnitude from UB catalog entry */
{
    if (ucat == YB6)
	return (0);
    else if (magetc < 0)
	return (-magetc / 100000000);
    else
	return (magetc / 100000000);
}


/* UBCZONE -- find the UB zone number where a declination can be found */

static int
ubczone (dec)

double dec;	/* declination in degrees */
{
    double zonesize = 0.1;	/* number of degrees per declination zone */
    int zone;

    zone = (int) ((dec + 90.0) / zonesize);
    if (zone > 1799)
	zone = 1799;
    else if (zone < 0)
	zone = 0;
    return (zone);
}


/* UBCSRA -- Find UB star closest to specified right ascension */

static int
ubcsra (rax0)

double	rax0;		/* Right ascension in degrees for which to search */
{
    int istar, istar1, istar2, nrep;
    double rax, ra1, ra, rdiff, rdiff1, rdiff2, sdiff;
    UBCstar star;	/* UB catalog entry for one star */
    char rastrx[32];
    int debug = 0;

    rax = rax0;
    if (debug)
	ra2str (rastrx, 31, rax, 3);
    istar1 = 1;
    if (ubcstar (istar1, &star))
	return (0);
    ra1 = ubcra (star.rasec);
    istar = nstars;
    nrep = 0;
    while (istar != istar1 && nrep < 30) {
	if (ubcstar (istar, &star))
	    break;
	else {
	    ra = ubcra (star.rasec);
	    if (ra == ra1)
		break;
	    if (debug) {
		char rastr[32];
		ra2str (rastr, 31, ra, 3);
		fprintf (stderr,"UBCSRA %d %d: %s (%s)\n",
			 nrep,istar,rastr,rastrx);
		}
	    rdiff = ra1 - ra;
	    rdiff1 = ra1 - rax;
	    rdiff2 = ra - rax;
	    if (nrep > 25 && ABS(rdiff2) > ABS(rdiff1)) {
		istar = istar1;
		break;
		}
	    nrep++;
	    sdiff = (double)(istar - istar1) * rdiff1 / rdiff;
	    istar2 = istar1 + (int) (sdiff + 0.5);
	    ra1 = ra;
	    if (debug) {
		fprintf (stderr," ra1=    %.5f ra=     %.5f rax=    %.5f\n",
			 ra1,ra,rax);
		fprintf (stderr," rdiff=  %.5f rdiff1= %.5f rdiff2= %.5f\n",
			 rdiff,rdiff1,rdiff2);
		fprintf (stderr," istar1= %d istar= %d istar2= %d\n",
			 istar1,istar,istar2);
		}
	    istar1 = istar;
	    istar = istar2;
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

/* UBCOPEN -- Open UB Catalog zone catalog, returning number of entries */

static int
ubcopen (znum)

int znum;	/* UB Catalog zone */
{
    char zonepath[64];	/* Pathname for input UB zone file */
    UBCstar star;	/* UB catalog entry for one star */
    int lfile;
    
/* Get path to zone catalog */
    if (ubcpath (znum, zonepath)) {
	fprintf (stderr, "UBCOPEN: Cannot find zone catalog for %d\n", znum);
	return (0);
	}

/* Find number of stars in zone catalog by its length */
    lfile = getfilesize (zonepath);
    if (lfile < 2) {
	fprintf (stderr,"UB zone catalog %s has no entries\n",zonepath);
	return (0);
	}
    else
	nstars = lfile / nbent;

/* Open zone catalog */
    if (!(fcat = fopen (zonepath, "rb"))) {
	fprintf (stderr,"UB zone catalog %s cannot be read\n",zonepath);
	return (0);
	}

/* Check to see if byte-swapping is necessary */
    cswap = 0;
    if (ubcstar (1, &star)) {
	fprintf (stderr,"UBCOPEN: cannot read star 1 from UB zone catalog %s\n",
		 zonepath);
	return (0);
	}
    else {
	if (star.rasec > 360 * 360000 || star.rasec < 0) {
	    cswap = 1;
	    /* fprintf (stderr,"UBCOPEN: swapping bytes in UB zone catalog %s\n",
		     zonepath); */
	    }
	else if (star.decsec > 180 * 360000 || star.decsec < 0) {
	    cswap = 1;
	    /* fprintf (stderr,"UBCOPEN: swapping bytes in UB zone catalog %s\n",
		     zonepath); */
	    }
	else
	    cswap = 0;
	}

    return (nstars);
}


/* UBCPATH -- Get UB Catalog region file pathname */

static int
ubcpath (zn, path)

int zn;		/* UB zone number */
char *path;	/* Pathname of UB zone file */

{
    /* Return error code and null path if zone is out of range */
    if (zn < 0 || zn > 1799) {
	fprintf (stderr, "UBCPATH: zone %d out of range 0-1799\n",zn);
	path[0] = (char) 0;
	return (-1);
	}

    /* Set path for USNO-B1.0 zone catalog */
    sprintf (path,"%s/%03d/b%04d.cat", upath, zn/10, zn);

    return (0);
}


/* UBCSTAR -- Get UB catalog entry for one star; return 0 if successful */

static int
ubcstar (istar, star)

int istar;	/* Star sequence number in UB zone catalog */
UBCstar *star;	/* UB catalog entry for one star */
{
    int nbs, nbr, nbskip;

    if (istar < 1 || istar > nstars) {
	fprintf (stderr, "UBCstar %d is not in catalog\n",istar);
	return (-1);
	}
    nbskip = nbent * (istar - 1);
    if (fseek (fcat,nbskip,SEEK_SET))
	return (-1);
    nbs = sizeof (UBCstar);
    nbr = fread (star, nbs, 1, fcat) * nbs;
    if (nbr < nbs) {
	fprintf (stderr, "UBCstar %d / %d bytes read\n",nbr, nbs);
	return (-2);
	}
    if (cswap)
	ubcswap ((char *)star);
    return (0);
}


/* UBCSWAP -- Reverse bytes of UB Catalog entry */

static void
ubcswap (string)

char *string;	/* Start of vector of 4-byte ints */

{
char *sbyte, *slast;
char temp0, temp1, temp2, temp3;
int nbytes = nbent; /* Number of bytes to reverse */

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

/* Jan 30 2003	New subroutine based on ubcread.c
 * Feb  4 2003	Open catalog file rb instead of r (Martin Ploner, Bern)
 * Mar 21 2003	Improve search limit test by always using wcsdist()
 * Apr 15 2003	Explicitly get revision date if nstarmax < 1
 * May 27 2003	Use getfilesize() to get file size
 * Jun  2 2003	Print proper motion as mas/year
 * Aug 22 2003	Add radi argument for inner edge of search annulus
 * Sep 25 2003	Add ubcbin() to fill an image with sources
 * Oct  6 2003	Update ubcread() and ubcbin() for improved RefLim()
 * Dec  1 2003	Add missing tab to n=-1 header
 * Dec  4 2003	Add USNO YB6 catalog
 * Dec 12 2003	Fix bug in wcs2pix() call in ubcbin()
 *
 * Aug 27 2004	Include fitsfile.h
 * Nov 19 2004	Return galaxy/star type code (qsg=0-11) in upmni vector
 *
 * Jan 12 2005	Declare ubcsg()
 *
 * Jun 20 2006	Drop unused variables
 * Sep 26 2006	Increase length of rastr and destr from 16 to 32
 * Nov 15 2006	Print coordinates if in verbose mode; convert coords to integer
 *
 * Jan 10 2007	Add match=1 argument to webrnum()
 * Nov 26 2007	Add one at each end of search range in ubcread()
 * Dec 05 2007	Add option to print per magnitude star/galaxy discriminators if ub1t
 * Dec 05 2007	Drop sg flag=0 from average qsg
 */
