/*** File libwcs/gscread.c
 *** September 22, 2009
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

#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <math.h>
#include <string.h>
#include "fitsfile.h"
#include "wcs.h"
#include "wcscat.h"

/* Pathname of northern hemisphere GSC CDROM  or search engine URL */
static char cdn[64]="/data/astrocat/gsc1";

/* Uncomment following line to use ESO GSC server for GSC
static char cdn[64]="http://archive.eso.org/skycat/servers/gsc-server";
 */

/* Pathname of southern hemisphere GSC CDROM */
static char cds[64]="/data/astrocat/gsc2";

/* Pathname of northern hemisphere GSC-ACT CDROM  or search engine URL */
static char cdna[64]="/data/astrocat/gscact1";

/* Pathname of southern hemisphere GSC-ACT CDROM */
static char cdsa[64]="/data/astrocat/gscact2";

static void gscpath();
static int gscreg();

static char *table = NULL;	/* FITS table buffer */
static int ltab = 0;		/* Length of FITS table buffer */
static double *gdist = NULL;	/* Array of distances to stars */
static int ndist = 0;

void
gscfree()
{ free ((void *)table); ltab = 0;
  free ((void *)gdist); ndist = 0;
  return; }

static int classd = -1; /* Desired object class
			   (-2=all objects, -1=all, 0=stars, 3=nonstars) */
void
setgsclass (class)
int class;
{ classd = class; return; }

/* GSCREAD -- Read HST Guide Star Catalog stars from CDROM */

int
gscread (refcat,cra,cdec,dra,ddec,drad,dradi,distsort,sysout,eqout,epout,
	 mag1,mag2,nstarmax,gnum,gra,gdec,gmag,gtype,nlog)

int	refcat;		/* Catalog code (GSC or GSCACT) */
double	cra;		/* Search center J2000 right ascension in degrees */
double	cdec;		/* Search center J2000 declination in degrees */
double	dra;		/* Search half width in right ascension in degrees */
double	ddec;		/* Search half-width in declination in degrees */
double	drad;		/* Limiting separation in degrees (ignore if 0) */
double	dradi;		/* Inner separation in degrees for annulus (ignore if 0) */
int	distsort;	/* 1 to sort stars by distance from center */
int	sysout;		/* Search coordinate system */
double	eqout;		/* Search coordinate equinox */
double	epout;		/* Proper motion epoch (0.0 for no proper motion) */
double	mag1,mag2;	/* Limiting magnitudes (none if equal) */
int	nstarmax;	/* Maximum number of stars to be returned */
double	*gnum;		/* Array of Guide Star numbers (returned) */
double	*gra;		/* Array of right ascensions (returned) */
double	*gdec;		/* Array of declinations (returned) */
double	**gmag;		/* Array of magnitudes (returned) */
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
    int magsort=0;
    int nreg;		/* Number of input FITS tables files */
    double xnum;		/* Guide Star number */
    int rlist[100];	/* List of input FITS tables files */
    char inpath[64];	/* Pathname for input FITS table file */
    char entry[100];	/* Buffer for FITS table row */
    int class;		/* Object class (0>star, 3>other) */
    int sysref=WCS_J2000;	/* Catalog coordinate system */
    double eqref=2000.0;	/* Catalog equinox */
    double epref=2000.0;	/* Catalog epoch */
    struct Keyword kw[8];	/* Keyword structure */
    struct Keyword *kwn;

    int verbose;
    int pass;
    int rnum, num0, num, itot,ireg;
    int ik,nk,itable,ntable,jstar;
    int nbline,npos,nbhead;
    int nbr,nrmax,nstar,i;
    int ift;
    int wrap;
    double ra,rasum,dec,decsum,perr,perr2,perrsum,msum;
    double mag,merr,merr2,merrsum;
    int band0 = 0;
    int band = 0;
    int class0 = 0;
    double merr0 = 0.0;
    double mag0 = 0.0;
    double perr0 = 0.0;
    double ra0 = 0.0;
    double dec0 = 0.0;
    double rra1, rra2, rdec1, rdec2;
    double rdist, ddist;
    char *str;
    char *url;
    char *title;
    char cstr[32], numstr[32], rastr[32], decstr[32], catid[16];

    itot = 0;
    if (nlog == 1)
	verbose = 1;
    else
	verbose = 0;
    verbose = nlog;

    /* If root pathname is a URL, search and return */
    url = cdn;
    if (refcat == GSC) {
	url = cdn;
	if ((str = getenv("GSC_NORTH")) == NULL)
	    str = getenv ("GSC_PATH");
	if (str != NULL) url = str;
	}
    if (refcat == GSCACT) {
	url = cdna;
	if ((str = getenv("GSCACT_NORTH")) == NULL)
	    str = getenv ("GSCACT_PATH");
	if (str != NULL) url = str;
	}
    if (!strncmp (url, "http:",5))
	return (webread (url,"gsc",distsort,cra,cdec,dra,ddec,drad,dradi,
			 sysout,eqout,epout,mag1,mag2,magsort,nstarmax,
			 gnum,gra,gdec,NULL,NULL,gmag,gtype,nlog));

    /* Allocate FITS table buffer which is saved between calls */
    if (ltab < 1) {
	ltab = 10000;
	table = (char *)calloc (ltab, sizeof (char));
	if (table == NULL) {
	    fprintf (stderr, "GSCREAD: cannot allocate FITS table buffer\n");
	    return (0);
	    }
	}

    for (i = 0; i < 100; i++)
	entry[i] = 0;

    /* Set path to Guide Star Catalog */
    if (refcat == GSCACT) {
	if ((str = getenv("GSCACT_NORTH")) != NULL )
	    strcpy (cdna,str);
	if ((str = getenv("GSCACT_SOUTH")) != NULL )
	    strcpy (cdsa,str);
	}
    else {
	if ((str = getenv("GSC_NORTH")) != NULL )
	    strcpy (cdn,str);
	if ((str = getenv("GSC_SOUTH")) != NULL )
	    strcpy (cds,str);
	}

    wcscstr (cstr, sysout, eqout, epout);

    SearchLim (cra,cdec,dra,ddec,sysout,&ra1,&ra2,&dec1,&dec2,verbose);

    /* Make mag1 always the smallest magnitude */
    if (mag2 < mag1) {
	mag = mag2;
	mag2 = mag1;
	mag1 = mag;
	}

    /* Find Guide Star Catalog regions in which to search */
    nrmax = 100;
    rra1 = ra1;
    rra2 = ra2;
    rdec1 = dec1;
    rdec2 = dec2;
    RefLim (cra,cdec,dra,ddec,sysout,sysref,eqout,eqref,epout,epref,0.0,
	    &rra1, &rra2, &rdec1, &rdec2,  &wrap, verbose);
    nreg = gscreg (refcat,rra1,rra2,rdec1,rdec2,table,nrmax,rlist,verbose);
    if (nreg <= 0) {
	fprintf (stderr,"GSCREAD:  no Guide Star regions found\n");
	return (0);
	}

    /* Allocate array for distances from search center */
    if (nstarmax > ndist) {
	if (ndist > 0)
	    free ((void *)gdist);
	gdist = (double *) malloc (nstarmax * sizeof (double));
	if (gdist == NULL) {
	    fprintf (stderr,"GSCREAD:  cannot allocate separation array\n");
	    return (0);
	    }
	ndist = nstarmax;
	}

    /* Set keyword list */
    nk = 8;
    strcpy (kw[0].kname,"GSC_ID");
    strcpy (kw[1].kname,"RA_DEG");
    strcpy (kw[2].kname,"DEC_DEG");
    strcpy (kw[3].kname,"POS_ERR");
    strcpy (kw[4].kname,"MAG");
    strcpy (kw[5].kname,"MAG_ERR");
    strcpy (kw[6].kname,"MAG_BAND");
    strcpy (kw[7].kname,"CLASS");
    for (ik = 0; ik < nk; ik++) {
	kw[ik].lname = (int) strlen (kw[ik].kname);
	kw[ik].kn = 0;
	kw[ik].kf = 0;
	kw[ik].kl = 0;
	}
    nstar = 0;

    /* Write header if printing star entries as found */
    if (nstarmax < 1) {
	char *revmessage;
	revmessage = getrevmsg();
	title = CatName (refcat, NULL);
	printf ("catalog	%s\n", title);
	free (title);
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
	printf ("program	scat %s\n", revmessage);
	CatID (catid, refcat);
	printf ("%s	ra          	dec         	", catid);
	printf ("magv 	class	band	n	arcmin\n");
	printf ("---------	------------	------------	");
	printf ("-----	-----	----	-	------\n");
	}

    /* Loop through region list */
    for (ireg = 0; ireg < nreg; ireg++) {
	gscpath (refcat, rlist[ireg], inpath);

    /* Read size and keyword info from FITS table header */
	kwn = kw;
	ift = fitsrtopen (inpath,&nk,&kwn,&ntable,&nbline,&nbhead);

	rnum = rlist[ireg];
	num0 = 0;
	rasum = 0.0;
	decsum = 0.0;
	msum = 0.0;
	perrsum = 0.0;
	merrsum = 0.0;
	npos = 0;
	num = 0;
	fitsrtlset();
	jstar = 0;
	class = 0;

	/* Loop through FITS table for this region */
	for (itable = 0; itable <= ntable; itable++) {

	    if (itable < ntable) {
		nbr = fitsrtline (ift,nbhead,ltab,table,itable,nbline,entry);
		if (nbr < nbline) {
		    fprintf (stderr,"GSCREAD: %d / %d bytes read, line %d / %d, region %d\n",
			      nbr,nbline,itable,ntable,rnum);
		    break;
		    }

		/* Extract selected fields */

		/* Star number within region */
		num0 = ftgeti4 (entry, &kw[0]);

		/* Right ascension in degrees */
		ra0 = ftgetr8 (entry, &kw[1]);

		/* Declination in degrees */
		dec0 = ftgetr8 (entry, &kw[2]);

		/* Position error */
		perr0 = ftgetr8 (entry, &kw[3]);

		/* Magnitude */
		mag0 = ftgetr8 (entry, &kw[4]);

		/* Magnitude error */
		merr0 = ftgetr8 (entry, &kw[5]);

		/* Bandpass code */
		band0 = ftgeti4 (entry, &kw[6]);

		/* Object class code */
		class0 = ftgeti4 (entry, &kw[7]);
		}
	    else
		num0 = 0;

	/* Compute mean position and magnitude for object */
	    if (itable > 0 && npos > 0 &&
		((classd < -1 && band != band0) ||
		(classd < -1 && class != class0) || 
		num != num0)) {

		pass = 1;
		if (perrsum == 0.0 || merrsum == 0)
		    pass = 0;
		else {
		    ra = rasum / perrsum;
		    dec = decsum / perrsum;
		    mag = msum / merrsum;
		    }

		if (pass > 0 && classd > -1 && class != classd)
		    pass = 0;

		/* Check magnitude and position limits */
		if (pass > 0 && mag1 != mag2 && (mag < mag1 || mag > mag2))
		    pass = 0;

		if (pass) {
		    wcscon (sysref, sysout, eqref, eqout, &ra, &dec, epout);
		
		    /* Compute distance from search center */
		    if (drad > 0 || distsort)
			dist = wcsdist (cra,cdec,ra,dec);
		    else
			dist = 0.0;

		    /* Check position limits */
		    if (drad > 0) {
			if (dist > drad)
			    pass = 0;
			if (dradi > 0.0 && dist < dradi)
			    pass = 0;
			}
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
		    class = class + (band * 100) + (npos * 10000);
		    xnum = (double)rnum + (0.0001 * (double) num);

		    /* Write star position and magnitudes to stdout */
		    if (nstarmax < 1) {
			CatNum (refcat, -9, 0, xnum, numstr);
			ra2str (rastr, 31, ra, 3);
			dec2str (decstr, 31, dec, 2);
			dist = wcsdist (cra,cdec,ra,dec) * 60.0;
			printf ("%s	%s	%s", numstr,rastr,decstr);
			printf ("	%.2f	%d	%d	%d	%.2f\n",
				mag, class-(band*100)-(npos*10000), band, npos, dist);
			}

		    /* Save star position in table */
		    else if (nstar < nstarmax) {
			gnum[nstar] = xnum;
			gra[nstar] = ra;
			gdec[nstar] = dec;
			gmag[0][nstar] = mag;
			gtype[nstar] = class;
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
			replace furthest star */
		    else if (distsort) {
			if (dist < maxdist) {
			    gnum[farstar] = xnum;
			    gra[farstar] = ra;
			    gdec[farstar] = dec;
			    gmag[0][farstar] = mag;
			    gtype[farstar] = class;
			    gdist[farstar] = dist;
			    maxdist = 0.0;

			    /* Find new farthest star */
			    for (i = 0; i < nstarmax; i++) {
				if (gdist[i] > maxdist) {
				    maxdist = gdist[i];
				    farstar = i;
				    }
				}
			    }
			}

		    /* If too many stars, replace faintest star */
		    else if (mag < faintmag) {
			gnum[faintstar] = xnum;
			gra[faintstar] = ra;
			gdec[faintstar] = dec;
			gmag[0][faintstar] = mag;
			gtype[faintstar] = class;
			gdist[faintstar] = dist;
			faintmag = 0.0;

			/* Find new faintest star */
			for (i = 0; i < nstarmax; i++) {
			    if (gmag[0][i] > faintmag) {
				faintmag = gmag[0][i];
				faintstar = i;
				}
			    }
			}
		    nstar++;
		    jstar++;
		    if (nlog == 1)
			fprintf (stderr,"GSCREAD: %04d.%04d: %9.5f %9.5f %s %5.2f %d %d\n",
				rnum,num,ra,dec,cstr,mag,class,npos);
		    }

	/* Reset star position for averaging */
		rasum = 0.0;
		decsum = 0.0;
		msum = 0.0;
		perrsum = 0.0;
		merrsum = 0.0;
		npos = 0;
		}

	/* Add information from current line to current object */

	    /* Check object class */
     	    if ((classd > -1 && class0 == classd) ||
		classd < -2 || (classd < 0 && class0 != 5)) {
		perr = perr0;
		perr2 = perr * perr;
		if (perr2 <= 0.0) perr2 = 0.01;
		rasum = rasum + (ra0 / perr2);
		decsum = decsum + (dec0 / perr2);
		perrsum = perrsum + (1.0 / perr2);
		if (merr0 <= 0.0) merr0 = 0.01;
		merr = merr0;
		merr2 = merr * merr;
		msum = msum + (mag0 / merr2);
		merrsum = merrsum + (1.0 / merr2);
		num = num0;
		class = class0;
		band = band0;
		npos++;
		}

	    /* Log operation */
	    if (nlog > 0 && itable%nlog == 0)
		fprintf (stderr,"GSCREAD: %4d / %4d: %5d / %5d  / %5d sources, region %4d.%04d\r",
			 ireg,nreg,jstar,itable,ntable,rlist[ireg],num0);

	/* End of region */
	    }

	/* Close region input file */
	(void) close (ift);
	itot = itot + itable;
	if (nlog > 0)
	    fprintf (stderr,"GSCREAD: %4d / %4d: %5d / %5d  / %5d sources from region %4d    \n",
		     ireg+1,nreg,jstar,itable,ntable,rlist[ireg]);
	}

    /* Close output file and summarize transfer */
    if (nlog > 0) {
	if (nreg > 1)
	    fprintf (stderr,"GSCREAD: %d regions: %d / %d found\n",nreg,nstar,itot);
	else
	    fprintf (stderr,"GSCREAD: 1 region: %d / %d found\n",nstar,itable);
	if (nstar > nstarmax )
	    fprintf (stderr,"GSCREAD: %d stars found; only %d returned\n",
		     nstar,nstarmax);
	}
    return (nstar);
}

/* GSCRNUM -- Read HST Guide Star Catalog stars from CDROM */

int
gscrnum (refcat, nstars, sysout, eqout, epout, gnum,gra,gdec,gmag,gtype,nlog)

int	refcat;		/* Catalog code (GSC or GSCACT) */
int	nstars;		/* Number of stars to find */
int	sysout;		/* Search coordinate system */
double	eqout;		/* Search coordinate equinox */
double	epout;		/* Proper motion epoch (0.0 for no proper motion) */
double	*gnum;		/* Array of Guide Star numbers (returned) */
double	*gra;		/* Array of right ascensions (returned) */
double	*gdec;		/* Array of declinations (returned) */
double	**gmag;		/* Array of magnitudes (returned) */
int	*gtype;		/* Array of object types (returned) */
int	nlog;		/* 1 for diagnostics */
{
    char *table;		/* FITS table */
    char inpath[64];		/* Pathname for input FITS table file */
    char entry[100];		/* Buffer for FITS table row */
    int class;			/* Object class (0>star, 3>other) */
    int sysref=WCS_J2000;	/* Catalog coordinate system */
    double eqref=2000.0;	/* Catalog equinox */
    struct Keyword kw[8];	/* Keyword structure */
    struct Keyword *kwn;

    int rnum, num0, num, itot;
    int ik,nk,itable,ntable,jstar;
    int nbline,npos,nbhead;
    int nbr,nstar,i, snum;
    int ift;
    double dnum;
    double ra,rasum,dec,decsum,perr,perr2,perrsum,msum;
    double mag,merr,merr2,merrsum;
    int band0 = 0;
    int band = 0;
    int class0 = 0;
    double merr0 = 0.0;
    double mag0 = 0.0;
    double perr0 = 0.0;
    double ra0 = 0.0;
    double dec0 = 0.0;
    char *str;
    char *url;

    itot = 0;

    /* If root pathname is a URL, search and return */
    url = cdn;
    if (refcat == GSC) {
	url = cdn;
	if ((str = getenv("GSC_NORTH")) == NULL)
	    str = getenv ("GSC_PATH");
	if (str != NULL) url = str;
	if (!strncmp (url, "http:",5))
	    return (webrnum (str,"gsc",nstars,sysout,eqout,epout,1,
			     gnum,gra,gdec,NULL,NULL,gmag,gtype,nlog));
	}
    if (refcat == GSCACT) {
	url = cdna;
	if ((str = getenv("GSCACT_NORTH")) == NULL)
	    str = getenv ("GSCACT_PATH");
	if (str != NULL) url = str;
	}
    if (!strncmp (url, "http:",5))
	return (webrnum (str,"gsc",nstars,sysout,eqout,epout,1,
			 gnum,gra,gdec,NULL,NULL,gmag,gtype,nlog));

    if (ltab < 1) {
	ltab = 10000;
	table = (char *)calloc (ltab, sizeof (char));
	}

    for (i = 0; i < 100; i++)
	entry[i] = 0;

    /* Set path to Guide Star Catalog */
    if ((str = getenv("GSC_NORTH")) != NULL )
	strcpy (cdn,str);
    if ((str = getenv("GSC_SOUTH")) != NULL )
	strcpy (cds,str);

    /* Set keyword list */
    nk = 8;
    strcpy (kw[0].kname,"GSC_ID");
    strcpy (kw[1].kname,"RA_DEG");
    strcpy (kw[2].kname,"DEC_DEG");
    strcpy (kw[3].kname,"POS_ERR");
    strcpy (kw[4].kname,"MAG");
    strcpy (kw[5].kname,"MAG_ERR");
    strcpy (kw[6].kname,"MAG_BAND");
    strcpy (kw[7].kname,"CLASS");
    for (ik = 0; ik < nk; ik++) {
	kw[ik].lname = (int) strlen (kw[ik].kname);
	kw[ik].kn = 0;
	kw[ik].kf = 0;
	kw[ik].kl = 0;
	}
    nstar = 0;

    /* Loop through star list */
    for (jstar = 0; jstar < nstars; jstar++) {
	rnum = (int) gnum[jstar];
	gscpath (refcat, rnum, inpath);
	dnum = (gnum[jstar] - (double)rnum) * 10000.0;
	snum = (int) (dnum + 0.5);

    /* Read size and keyword info from FITS table header */
	kwn = kw;
	ift = fitsrtopen (inpath,&nk,&kwn,&ntable,&nbline,&nbhead);
	if (ift < 0) {
	    fprintf (stderr,"GSCRNUM: File %s not found\n",inpath);
	    return (0);
	    }
	num0 = 0;
	rasum = 0.0;
	decsum = 0.0;
	msum = 0.0;
	perrsum = 0.0;
	merrsum = 0.0;
	npos = 0;
	num = 0;
	fitsrtlset();
	class = 0;

	/* Loop through FITS table for this region */
	for (itable = 0; itable <= ntable; itable++) {

	    if (itable < ntable) {
		nbr = fitsrtline (ift,nbhead,ltab,table,itable,nbline,entry);
		if (nbr < nbline) {
		    fprintf (stderr,"GSCRNUM: %d / %d bytes read, line %d / %d, region %d\n",
			      nbr,nbline,itable,ntable,rnum);
		    break;
		    }

	 /* Extract selected fields */

		/* Star number within region */
		num0 = ftgeti4 (entry, &kw[0]);
		if (num0 < snum)
		    continue;
		else if (num == 0)
		    num = num0;

		/* Right ascension in degrees */
		ra0 = ftgetr8 (entry, &kw[1]);

		/* Declination in degrees */
		dec0 = ftgetr8 (entry, &kw[2]);

		/* Position error */
		perr0 = ftgetr8 (entry, &kw[3]);

		/* Magnitude */
		mag0 = ftgetr8 (entry, &kw[4]);

		/* Magnitude error */
		merr0 = ftgetr8 (entry, &kw[5]);

		/* Bandpass code */
		band0 = ftgeti4 (entry, &kw[6]);

		/* Object class code */
		class0 = ftgeti4 (entry, &kw[7]);
		}
	    else
		num0 = 0;

	    /* Compute mean position and magnitude for object */
	    if (num != num0 && itable > 0 && npos > 0) {
		ra = rasum / perrsum;
		dec = decsum / perrsum;
		wcscon (sysref, sysout, eqref, eqout, &ra, &dec, epout);
		mag = msum / merrsum;
		class = class + (band * 100) + (npos * 10000);

		/* Save star position in table */
		gra[nstar] = ra;
		gdec[nstar] = dec;
		gmag[0][nstar] = mag;
		gtype[nstar] = class;

		nstar++;
		if (nlog == 1)
		    fprintf (stderr,"GSCRNUM: %04d.%04d: %9.5f %9.5f %5.2f %d %d\n",
			     rnum,num,ra,dec,mag,class,npos);

		/* Reset star position for averaging */
		rasum = 0.0;
		decsum = 0.0;
		msum = 0.0;
		perrsum = 0.0;
		merrsum = 0.0;
		npos = 0;
		break;
		}

	    /* Add information from current line to current object */
	    perr = perr0;
	    perr2 = perr * perr;
	    if (perr2 <= 0.0) perr2 = 0.01;
	    rasum = rasum + (ra0 / perr2);
	    decsum = decsum + (dec0 / perr2);
	    perrsum = perrsum + (1.0 / perr2);
	    if (merr0 <= 0.0) merr0 = 0.01;
	    merr = merr0;
	    merr2 = merr * merr;
	    msum = msum + (mag0 / merr2);
	    merrsum = merrsum + (1.0 / merr2);
	    num = num0;
	    class = class0;
	    band = band0;
	    npos = npos + 1;

	    /* Log operation */
	    if (nlog > 0 && itable%nlog == 0)
		fprintf (stderr,"GSCRNUM: %4d / %4d: %5d / %5d sources, region %4d.%04d\r",
			 jstar+1,nstars,itable,ntable,rnum,snum);

	/* End of region */
	    }

	/* Close region input file */
	(void) close (ift);
	itot = itot + itable;
	if (nlog > 0)
	    fprintf (stderr,"GSCRNUM: %4d / %4d: %5d / %5d sources, region %4d.%04d\n",
		     jstar+1,nstars,itable,ntable,rnum,snum);
	}

    return (nstars);
}


/* GSCBIN -- Fill FITS WCS image with HST Guide Star Catalog objects */

int
gscbin (refcat, wcs, header, image, mag1, mag2, magscale, nlog)

int	refcat;		/* Catalog code (GSC or GSCACT) */
struct WorldCoor *wcs;	/* World coordinate system for image */
char	*header;	/* FITS header for output image */
char	*image;		/* Output FITS image */
double	mag1,mag2;	/* Limiting magnitudes (none if equal) */
double	magscale;	/* Scaling factor for magnitude to pixel flux
			 * (number of catalog objects per bin if 0) */
int	nlog;		/* 1 for diagnostics */
{
    double cra;		/* Search center J2000 right ascension in degrees */
    double cdec;	/* Search center J2000 declination in degrees */
    double dra;		/* Search half width in right ascension in degrees */
    double ddec;	/* Search half-width in declination in degrees */
    int	sysout = wcs->syswcs;	/* Image coordinate system */
    double eqout = wcs->equinox; /* Image coordinate equinox */
    double epout = wcs->epoch;	/* Image epoch */
    double ra1,ra2;	/* Limiting right ascensions of image in degrees */
    double dec1,dec2;	/* Limiting declinations of image in degrees */
    int nreg;		/* Number of input FITS tables files */
    double xnum;		/* Guide Star number */
    int rlist[100];	/* List of input FITS tables files */
    char inpath[64];	/* Pathname for input FITS table file */
    char entry[100];	/* Buffer for FITS table row */
    int class, class0;	/* Object class (0>star, 3>other) */
    int sysref=WCS_J2000;	/* Catalog coordinate system */
    double eqref=2000.0;	/* Catalog equinox */
    struct Keyword kw[8];	/* Keyword structure */
    struct Keyword *kwn;

    int verbose;
    int pass;
    int wrap;
    int rnum, num0, num, itot,ireg;
    int ik,nk,itable,ntable,jstar;
    int nbline,npos,nbhead;
    int nbr,nrmax,nstar,i;
    int ift, band0, band;
    double ra,ra0,rasum,dec,dec0,decsum,perr,perr0,perr2,perrsum,msum;
    double mag,mag0,merr,merr0,merr2,merrsum;
    double rra1, rra2, rdec1, rdec2;
    double rdist, ddist;
    char *str;
    char cstr[32];
    int bitpix, w, h;	/* Image bits/pixel and pixel width and height */
    double logt = log(10.0);
    double xpix, ypix, flux;
    int ix, iy;
    int offscl;

    itot = 0;
    if (nlog == 1)
	verbose = 1;
    else
	verbose = 0;
    verbose = nlog;

    /* Set image parameters */
    bitpix = 0;
    (void)hgeti4 (header, "BITPIX", &bitpix);
    w = 0;
    (void)hgeti4 (header, "NAXIS1", &w);
    h = 0;
    (void)hgeti4 (header, "NAXIS2", &h);

    if (refcat == GSCACT) {
	if ((str = getenv("GSCACT_NORTH")) == NULL)
	    str = getenv ("GSCACT_PATH");
	}

    /* Allocate FITS table buffer which is saved between calls */
    if (ltab < 1) {
	ltab = 10000;
	table = (char *)calloc (ltab, sizeof (char));
	if (table == NULL) {
	    fprintf (stderr, "GSCBIN: cannot allocate FITS table buffer\n");
	    return (0);
	    }
	}

    for (i = 0; i < 100; i++)
	entry[i] = 0;

    /* Set path to Guide Star Catalog */
    if (refcat == GSCACT) {
	if ((str = getenv("GSCACT_NORTH")) != NULL )
	    strcpy (cdna,str);
	if ((str = getenv("GSCACT_SOUTH")) != NULL )
	    strcpy (cdsa,str);
	}
    else {
	if ((str = getenv("GSC_NORTH")) != NULL )
	    strcpy (cdn,str);
	if ((str = getenv("GSC_SOUTH")) != NULL )
	    strcpy (cds,str);
	}

    wcscstr (cstr, sysout, eqout, epout);

    wcssize (wcs, &cra, &cdec, &dra, &ddec);
    SearchLim (cra,cdec,dra,ddec,sysout,&ra1,&ra2,&dec1,&dec2,verbose);

    /* Make mag1 always the smallest magnitude */
    if (mag2 < mag1) {
	mag = mag2;
	mag2 = mag1;
	mag1 = mag;
	}

    /* Find Guide Star Catalog regions in which to search */
    nrmax = 100;
    rra1 = ra1;
    rra2 = ra2;
    rdec1 = dec1;
    rdec2 = dec2;
    RefLim (cra,cdec,dra,ddec,sysout,sysref,eqout,eqref,epout,epout,0.0,
	    &rra1, &rra2, &rdec1, &rdec2, &wrap, verbose);
    nreg = gscreg (refcat,rra1,rra2,rdec1,rdec2,table,nrmax,rlist,verbose);
    if (nreg <= 0) {
	fprintf (stderr,"GSCBIN:  no Guide Star regions found\n");
	return (0);
	}

    /* Set keyword list */
    nk = 8;
    strcpy (kw[0].kname,"GSC_ID");
    strcpy (kw[1].kname,"RA_DEG");
    strcpy (kw[2].kname,"DEC_DEG");
    strcpy (kw[3].kname,"POS_ERR");
    strcpy (kw[4].kname,"MAG");
    strcpy (kw[5].kname,"MAG_ERR");
    strcpy (kw[6].kname,"MAG_BAND");
    strcpy (kw[7].kname,"CLASS");
    for (ik = 0; ik < nk; ik++) {
	kw[ik].lname = (int) strlen (kw[ik].kname);
	kw[ik].kn = 0;
	kw[ik].kf = 0;
	kw[ik].kl = 0;
	}
    nstar = 0;

    /* Loop through region list */
    for (ireg = 0; ireg < nreg; ireg++) {
	gscpath (refcat, rlist[ireg], inpath);

    /* Read size and keyword info from FITS table header */
	kwn = kw;
	ift = fitsrtopen (inpath,&nk,&kwn,&ntable,&nbline,&nbhead);

	rnum = rlist[ireg];
	num0 = 0;
	rasum = 0.0;
	decsum = 0.0;
	msum = 0.0;
	perrsum = 0.0;
	merrsum = 0.0;
	npos = 0;
	num = 0;
	fitsrtlset();
	jstar = 0;
	class = 0;

	/* Loop through FITS table for this region */
	for (itable = 0; itable <= ntable; itable++) {

	    if (itable < ntable) {
		nbr = fitsrtline (ift,nbhead,ltab,table,itable,nbline,entry);
		if (nbr < nbline) {
		    fprintf (stderr,"GSCBIN: %d / %d bytes read, line %d / %d, region %d\n",
			      nbr,nbline,itable,ntable,rnum);
		    break;
		    }

		/* Extract selected fields */

		/* Star number within region */
		num0 = ftgeti4 (entry, &kw[0]);

		/* Right ascension in degrees */
		ra0 = ftgetr8 (entry, &kw[1]);

		/* Declination in degrees */
		dec0 = ftgetr8 (entry, &kw[2]);

		/* Position error */
		perr0 = ftgetr8 (entry, &kw[3]);

		/* Magnitude */
		mag0 = ftgetr8 (entry, &kw[4]);

		/* Magnitude error */
		merr0 = ftgetr8 (entry, &kw[5]);

		/* Bandpass code */
		band0 = ftgeti4 (entry, &kw[6]);

		/* Object class code */
		class0 = ftgeti4 (entry, &kw[7]);
		}
	    else
		num0 = 0;

	/* Compute mean position and magnitude for object */
	    if (itable > 0 && npos > 0 &&
		((classd < -1 && band != band0) ||
		(classd < -1 && class != class0) || 
		num != num0)) {

		pass = 1;
		if (perrsum == 0.0 || merrsum == 0)
		    pass = 0;
		else {
		    ra = rasum / perrsum;
		    dec = decsum / perrsum;
		    mag = msum / merrsum;
		    }

		if (pass > 0 && classd > -1 && class != classd)
		    pass = 0;

		/* Check magnitude and position limits */
		if (pass > 0 && mag1 != mag2 && (mag < mag1 || mag > mag2))
		    pass = 0;

		if (pass) {
		    wcscon (sysref, sysout, eqref, eqout, &ra, &dec, epout);
		
		    /* Check position limits */
		    ddist = wcsdist (cra,cdec,cra,dec);
		    if (ddist > ddec)
			pass = 0;
		    rdist = wcsdist (cra,dec,ra,dec);
		    if (rdist > dra)
			pass = 0;
		    }

		/* Save star in FITS image */
		if (pass) {
		    class = class + (band * 100) + (npos * 10000);
		    xnum = (double)rnum + (0.0001 * (double) num);
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
			fprintf (stderr,"GSCBIN: %04d.%04d: %9.5f %9.5f %s %d %d",
				 rnum, num, ra, dec, cstr, class, npos);
			if (magscale > 0.0)
			    fprintf (stderr, " %5.2f", mag);
			if (!offscl)
			    flux = getpix1 (image, bitpix, w, h, 0.0, 1.0, ix, iy);
			else
			    flux = 0.0;
			fprintf (stderr," (%d,%d): %f\n", ix, iy, flux);
			}
		    }

	/* Reset star position for averaging */
		rasum = 0.0;
		decsum = 0.0;
		msum = 0.0;
		perrsum = 0.0;
		merrsum = 0.0;
		npos = 0;
		}

	/* Add information from current line to current object */

	    /* Check object class */
     	    if ((classd > -1 && class0 == classd) ||
		classd < -2 || (classd < 0 && class0 != 5)) {
		perr = perr0;
		perr2 = perr * perr;
		if (perr2 <= 0.0) perr2 = 0.01;
		rasum = rasum + (ra0 / perr2);
		decsum = decsum + (dec0 / perr2);
		perrsum = perrsum + (1.0 / perr2);
		if (merr0 <= 0.0) merr0 = 0.01;
		merr = merr0;
		merr2 = merr * merr;
		msum = msum + (mag0 / merr2);
		merrsum = merrsum + (1.0 / merr2);
		num = num0;
		class = class0;
		band = band0;
		npos++;
		}

	    /* Log operation */
	    if (nlog > 0 && itable%nlog == 0)
		fprintf (stderr,"GSCBIN: %4d / %4d: %5d / %5d  / %5d sources, region %4d.%04d\r",
			 ireg,nreg,jstar,itable,ntable,rlist[ireg],num0);

	/* End of region */
	    }

	/* Close region input file */
	(void) close (ift);
	itot = itot + itable;
	if (nlog > 0)
	    fprintf (stderr,"GSCBIN: %4d / %4d: %5d / %5d  / %5d sources from region %4d    \n",
		     ireg+1,nreg,jstar,itable,ntable,rlist[ireg]);
	}

    /* Close output file and summarize transfer */
    if (nlog > 0) {
	if (nreg > 1)
	    fprintf (stderr,"GSCBIN: %d regions: %d / %d found\n",nreg,nstar,itot);
	else
	    fprintf (stderr,"GSCBIN: 1 region: %d / %d found\n",nstar,itable);
	}
    return (nstar);
}


/* First region in each declination zone */
int zreg1[24]={1,594,1178,1729,2259,2781,3246,3652,4014,4294, 4492,4615,
	      4663,5260,5838,6412,6989,7523,8022,8464,8840,9134,9346,9490};

/* Last region in each declination zone */
int zreg2[24]={593,1177,1728,2258,2780,3245,3651,4013,4293,4491,4614,4662,
	       5259,5837,6411,6988,7522,8021,8463,8839,9133,9345,9489,9537};

/* Directory for each declination zone */
char zdir[24][8]={"n0000","n0730","n1500","n2230","n3000","n3730","n4500",
		"n5230","n6000","n6730","n7500","n8230","s0000","s0730",
		"s1500","s2230","s3000","s3730","s4500","s5230","s6000",
		"s6730","s7500","s8230"};

static struct Keyword rkw[15];
static int nrkw = 13;

/* GSCREG -- search the HST Guide Star Catalog index table for fields
 * in the specified range of coordinates and magnitudes.  Build a
 * list containing the pathnames of the files on the cd-rom.
 */

static int
gscreg (refcat, ra1, ra2, dec1, dec2, table, nrmax, rgns, verbose)

int	refcat;		/* Catalog code (GSC or GSCACT) */
double	ra1, ra2;	/* Right ascension limits in degrees */
double	dec1, dec2; 	/* Declination limits in degrees */
char	*table;		/* Table data buffer */
int	nrmax;		/* Maximum number of regions to find */
int	*rgns;		/* Region numbers (returned)*/
int	verbose;	/* 1 for diagnostics */

{
    int nrgn;		/* Number of regions found (returned) */
    char tabpath[64];	/* Pathname for regions table */
    int nrows;		/* Number of entries in table */
    int nchar;		/* Number of characters per line in table */
    int nwrap;		/* 1 if 0h included in RA span*/
    int iwrap;
    struct Keyword *kwn;
    double gscra(), gscdec();
    int gsczone();

    char fitsline[120];
    int irow,iz1,iz2,ir1,ir2,jr1,jr2,i;
    int nsrch,nsrch1,nbhead,nbr;
    int ift;
    double ralow, rahi;
    double declow, dechi, decmin, decmax;
    int regnum;

    /* Set up keyword list for table entries to extract */
    strcpy (rkw[0].kname,"REG_NO");
    strcpy (rkw[1].kname,"RA_H_LOW");
    strcpy (rkw[2].kname,"RA_M_LOW");
    strcpy (rkw[3].kname,"RA_S_LOW");
    strcpy (rkw[4].kname,"RA_H_HI");
    strcpy (rkw[5].kname,"RA_M_HI");
    strcpy (rkw[6].kname,"RA_S_HI");
    strcpy (rkw[7].kname,"DECSI_LO");
    strcpy (rkw[8].kname,"DEC_D_LO");
    strcpy (rkw[9].kname,"DEC_M_LO");
    strcpy (rkw[10].kname,"DECSI_HI");
    strcpy (rkw[11].kname,"DEC_D_HI");
    strcpy (rkw[12].kname,"DEC_M_HI");
    rkw[13].kname[0] = 0;

    /* Add lengths of keywords to keyword structures */
    for (i = 0; i < 13; i++)
	rkw[i].lname = strlen (rkw[i].kname);

    /* Initialize region list to zeroes */
    for (i = 0; i < nrmax; i++)
	rgns[i] = 0;

    /* Initialize FITS table line buffer to zeroes */
    for (i = 0; i < 120; i++)
	fitsline[i] = (char) 0;

    nrgn = 0;

    /* Set pathnames to Guide Star Catalog CDROM */
    if (refcat == GSCACT)
	strcpy (tabpath,cdna);
    else
	strcpy (tabpath,cdn);

    /* Set pathname for region table file */
    strcat (tabpath,"/tables/regions.tbl");

    /* Open the index table */
    kwn = rkw;
    ift = fitsrtopen (tabpath,&nrkw,&kwn, &nrows, &nchar, &nbhead);

    /* If the northern hemisphere CDROM cannot be read, try the southern */
    if (ift < 0) {
	if (refcat == GSCACT)
	    strcpy (tabpath,cdsa);
	else
	    strcpy (tabpath,cds);
	strcat (tabpath,"/tables/regions.tbl");
	ift = fitsrtopen (tabpath,&nrkw,&kwn,&nchar,&nrows,&nbhead);
	if (ift < 0) {
	    fprintf (stderr,"GSCREG:  error reading region table %s\n",tabpath);
	    return (0);
	    }
	}

    /* Find region range to search based on declination */
    iz1 = gsczone (dec1);
    iz2 = gsczone (dec2);
    jr1 = 0;
    jr2 = 0;
    nwrap = 1;

    /* Search region northern hemisphere or only one region */
    if (iz2 >= iz1) {
	ir1 = zreg1[iz1];
	ir2 = zreg2[iz2];
	}

    /* Search region in southern hemisphere with multiple regions */
    else if (dec1 < 0 && dec2 < 0) {
	ir1 = zreg1[iz2];
	ir2 = zreg2[iz1];
	}

    /* Search region spans equator */
    else if (dec1 < 0 && dec2 >= 0) {
	ir1 = zreg1[12];
	ir2 = zreg2[iz1];
	jr1 = zreg1[0];
	jr2 = zreg2[iz2];
	nwrap = 2;
	}
    else {
	ir1 = 1;
	ir2 = 0;
	}

    nsrch = ir2 - ir1 + 1;
    if (verbose)
	fprintf (stderr,"GSCREG: searching %d regions: %d - %d\n",nsrch,ir1,ir2);
    if (jr1 > 0) {
	nsrch1 = jr2 - jr1 + 1;
	if (verbose)
	    fprintf (stderr,"GSCREG: searching %d regions: %d - %d\n",nsrch1,jr1,jr2);
	}
    if (verbose)
	fprintf(stderr,"GSCREG: RA: %.5f - %.5f, Dec: %.5f - %.5f\n",ra1,ra2,dec1,dec2);

    if (nsrch < 1) {
	return (0);
	}

    nrgn = 0;

    for (iwrap = 0; iwrap < nwrap; iwrap++) {

	for (irow = ir1 - 1; irow < ir2; irow++) {

	/* Read next line of region table */
	    nbr = fitsrtline (ift,nbhead,ltab,table,irow,nchar,fitsline);
	    if (nbr < nchar) {
		fprintf (stderr,"GSREG: %d / %d bytes read for row %d\n",nbr,nchar,irow);
		break;
		}

	/* Declination range of the gs region */
	/* note:  southern dechi and declow are reversed */
	    dechi = gscdec (fitsline, 10, 11, 12);
	    declow = gscdec (fitsline, 7, 8, 9);
	    if (dechi > declow) {
		decmin = declow;
		decmax = dechi;
		}
	    else {
		decmax = declow;
		decmin = dechi;
		}

	    if (decmax >= dec1 && decmin <= dec2) {

	    /* Right ascension range of the Guide Star Catalog region */
		ralow = gscra (fitsline, 1, 2, 3);
		rahi = gscra (fitsline, 4, 5, 6);
		if (rahi <= 0.0) rahi = 360.0;

	    /* Check RA if 0h RA not between region RA limits */
		if (ra1 < ra2) {
		    if (ralow <= ra2 && rahi >= ra1) {

			/* Get region number from FITS table */
			regnum = ftgeti4 (fitsline, &rkw[0]);
			if (verbose)
			    fprintf (stderr,"GSCREG: Region %d added to search\n",regnum);

			/* Add this region to list, if there is space */
			if (nrgn < nrmax) {
			    rgns[nrgn] = regnum;
			    nrgn = nrgn + 1;
			    }
			}
		    }

	    /* Check RA if 0h RA is between region RA limits */
		else {
		    if (ralow > rahi) rahi = rahi + 360.0;
		    if (ralow <= ra2 || rahi >= ra1) {

		    /* Get region number from FITS table */
			regnum = ftgeti4 (fitsline, &rkw[0]);
			if (verbose)
			    fprintf (stderr,"GSCREG: Region %d added to search\n",regnum);

		    /* Add this region to list, if there is space */
			if (nrgn < nrmax) {
			    rgns[nrgn] = regnum;
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

    (void) close (ift);
    return (nrgn);
}


/* GSCRA -- returns right ascension in degrees from the GSC index table
 *  This is converted from the hours, minutes, and seconds columns.
 */

double
gscra (fitsline, hcol, mcol, scol)

char *fitsline;		/* Index table line */
int hcol;		/* Column index for hours */
int mcol;		/* Column index for minutes */
int scol;		/* Column index for seconds */

{
    double ra;		/* Right ascension in fractional degrees */
    double hrs;		/* Hours of right ascension */
    double min;		/* Minutes of right ascension */
    double sec;		/* Seconds of right ascension */

/*  hours of right ascension */
    hrs = ftgetr8 (fitsline, &rkw[hcol]);

/* minutes of right ascension */
    min = ftgetr8 (fitsline, &rkw[mcol]);

/* seconds of right ascension */
    sec = ftgetr8 (fitsline, &rkw[scol]);

/* right ascension in degrees */
    ra = hrs + (min / 60.0) + (sec / 3600.0);
    ra = ra * 15.0;

    return (ra);
}


/*  GSCDEC -- returns the declination in degrees from the GSC index table.
 *  This is converted from the sign, degrees, minutes, and seconds columns.
 */

double
gscdec (fitsline, sgncol, dcol, mcol)

char *fitsline;		/* Index table line */
int sgncol;		/* Column index for sign */
int dcol;		/* Column index for degrees */
int mcol;		/* Column index for minutes */

{
double	dec;		/* Declination in fractional degrees */

char sgn[4];		/* Sign of declination */
double deg;		/* Degrees of declination*/
double min;		/* Minutes of declination */

    /* Get declination sign from table */
    (void) ftgetc (fitsline, &rkw[sgncol], sgn, 3);

    /* Get degrees of declination from table */
    deg = ftgetr8 (fitsline, &rkw[dcol]);

    /* Get minutes of declination from table */
    min = ftgetr8 (fitsline, &rkw[mcol]);

    dec = deg + (min / 60.0);

    /* Negative declination */
    if (strchr (sgn, '-') != NULL)
	dec = -dec;

    return (dec);
}


/*  GSCZONE -- find the zone number where a declination can be found */

int
gsczone (dec)

double dec;	/* declination in degrees */

{
int zone;		/* gsc zone (returned) */
double	zonesize;
int ndeczones = 12;	/* number of declination zones per hemisphere */

    /* Width of declination zones */
    zonesize = 90.0 / ndeczones;

    zone = ((int) (dec / zonesize));
    if (dec < 0)
	zone = ndeczones - zone;
	
    return (zone);
}


/* GSCPATH -- Get HST Guide Star Catalog region FITS file pathname */

static void
gscpath (refcat, regnum, path)

int	refcat;		/* Catalog code (GSC or GSCACT) */
int	regnum;		/* Guide Star Catalog region number */
char	*path;		/* Pathname of GSC region FITS file */

{
    int zone = 0;	/* Name of Guide Star Catalog zone directory */
    int i;

    /* Get zone directory name given region number */
    for (i = 0; i < 24; i++) {
	if (regnum >= zreg1[i] && regnum <= zreg2[i]) {
	    zone = i;
	    break;
	    }
	}

    /* Set the pathname using the appropriate GSC CDROM directory */

    /* Northern hemisphere disk (volume 1) */
    if (regnum < zreg1[13]) {
	if (refcat == GSCACT)
	    sprintf (path,"%s/%s/%04d.gsc", cdna, zdir[zone], regnum);
	else
	    sprintf (path,"%s/gsc/%s/%04d.gsc", cdn, zdir[zone], regnum);
	}

    /* Southern hemisphere disk (volume 2) */
    else {
	if (refcat == GSCACT)
	    sprintf (path,"%s/%s/%04d.gsc", cdsa, zdir[zone], regnum);
	else
	    sprintf (path,"%s/gsc/%s/%04d.gsc", cds, zdir[zone], regnum);
	}

    return;
}

/* Feb 16 1996	New program
 * May 14 1996	Change arguments to FITSRTOPEN
 * May 17 1996	Fix bug so equal magnitude limits accepts anything
 * May 17 1996	Use new FITSRTOPEN which internalizes FITS header
 * May 24 1996	Fix string decoding bug and region search
 * May 31 1996	Use stream I/O
 * Jun 10 1996	Remove unused variables after using lint
 * Jul  1 1996	Fix GSC pathname
 * Aug  6 1996	Minor changes after lint
 * Sep 17 1996	Fix bug causing incomplete region access; improve logging
 * Oct 17 1996	Change FITS table entry calls
 * Nov 13 1996	Return no more than maximum star number
 * Nov 13 1996	Write all error messages to stderr with subroutine names
 * Nov 15 1996	Implement search radius; change input arguments
 * Dec 12 1996	Make logging run on single line per region
 * Dec 17 1996  Add code to keep brightest or closest stars if too many found
 * Dec 18 1996	Add code to get star information by number
 *
 * Mar 20 1997	Remove unused variables and fixed logging in GSCRNUM after lint
 * Oct 10 1997	Use standard I/O instead of stream I/O when reading FITS files
 * Nov  6 1997	Do not print overrun unless logging
 *
 * May 13 1998	Do not set classd in gscread()
 * May 13 1998	Print all stars if classd is < -1
 * May 27 1998	Include fitsio.h instead of fitshead.h
 * Jun 24 1998	Add string lengths to ra2str() and dec2str() calls
 * Aug  6 1998	Change fitsio.h to fitsfile.h
 * Sep 16 1998	Use limiting radius, if present
 * Sep 22 1998	Convert to desired output coordinate system
 * Oct 26 1998	Fix bug in region selection
 * Oct 29 1998	Correctly assign numbers when too many stars are found
 *
 * May 25 1999	Allocate table buffer only once
 * Jun 16 1999	Use SearchLim()
 * Aug 16 1999	Add RefLim() to get converted search coordinates right
 * Aug 16 1999	Fix bug to fix failure to search across 0:00 RA
 * Aug 25 1999	Return real number of stars from gscread()
 * Sep 10 1999	Set class selection with subroutine, not argument
 * Sep 16 1999	Fix bug which didn't always return closest stars
 * Sep 16 1999	Add distsort argument so brightest stars in circle works, too
 * Sep 22 1999	Rewrite table allocation so it works; make ltab static
 * Oct 21 1999	Include wcscat.h, unistd.h
 *
 * Mar 27 2000	Drop unused variables after lint
 * Mar 28 2000	Make default to read all classes
 * Jun 26 2000	Add coordinate system to SearchLim() arguments
 * Nov 29 2000	Add option to read cataog across the web
 * Dec 11 2000	Allow search engine URL in cdn[]
 * Dec 12 2000	Fix wrong web subroutine in gscrnum()
 *
 * Apr 24 2001	Add bandpass code and number of entries to object class
 * Apr 24 2001	Return individual entries if class is < -1
 * May 23 2001	Add support for GSC-ACT
 * May 30 2001	Use GSC_NORTH and GSCACT_NORTH instead of *_PATH for consistency
 * Jun 27 2001  Print stars as found in gscread() if nstarmax < 1
 * Jun 27 2001	Allocate distance array only if larger one is needed
 * Jun 27 2001	Add gscfree() to free table buffer and distance array
 * Sep 11 2001	Use single magnitude argument to gscread() and webread()
 * Sep 21 2001	Clean up web interface
 * Nov 20 2001	Change cos(degrad)) to cosdeg()
 *
 * Mar 28 2002	Change pathnames to /data/astrocat
 * Jul 31 2002	Always check for GSCACT_PATH, not GSCA_PATH
 * Oct  2 2002	Fix pass-through tab table header
 *
 * Mar 10 2003	Improve position limits
 * Apr  3 2003	Drop wrap in gscread(); add test for npos in gscread()
 * Apr 14 2003	Explicitly get revision date if nstarmax < 1
 * Aug 22 2003	Add dradi for inner edge of annulus search
 * Sep 25 2003	Add gscbin() to fill an image with sources
 * Oct  6 2003	Update gscread() and gscbin() for improved RefLim()
 * Nov 18 2003	Initialize image size and bits/pixel from header in gscbin()
 * Dec  1 2003	Add missing tab to n=-1 header
 * Dec 12 2003	Fix bug in wcs2pix() call in gscbin()
 *
 * Aug 27 2004	Include math.h
 *
 * Jun 20 2006	Initialize uninitialized variables
 * Nov 16 2006	Fix binning
 *
 * Jan 10 2007	Add match=1 argument to webrnum()
 * Jan 10 2007	Rewrite web access in gscread() and gscrnum() to reduce code
 *
 * Sep 22 2009	Initialize lengths of FITS table columns
 * Sep 22 2009	Change region table keywords from DEC*LOW to DEC*LO
 */
