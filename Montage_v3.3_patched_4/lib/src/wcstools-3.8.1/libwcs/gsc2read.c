/*** File libwcs/gsc2read.c
 *** August 17, 2009
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

static void parsex();

#define ABS(a) ((a) < 0 ? (-(a)) : (a))
#define LINE    1024

/* URL for GSC II search engine at STScI Catalogs and Surveys Branch */
char gsc23url[64]="http://gsss.stsci.edu/webservices/GSC2/GSC2DataReturn.aspx";

/* GSC2READ -- Read GSC II catalog stars over the web */

int
gsc2read (refcatname,cra,cdec,dra,ddec,drad,dradi,distsort,sysout,eqout,epout,
	  mag1,mag2,sortmag,nstarmax,gnum,gobj,gra,gdec,gpra,gpdec,gmag,gtype,nlog)

char	*refcatname;	/* Name of catalog (GSC2 for 2.2; GSC2.3 for 2.3) */
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
char	**gobj;		/* Array of object IDs (mixed characters and numbers) */
double	*gra;		/* Array of right ascensions (returned) */
double	*gdec;		/* Array of declinations (returned) */
double	*gpra;		/* Array of right ascension proper motions (returned) */
double	*gpdec;		/* Array of declination proper motions (returned) */
double	**gmag;		/* 2-D array of magnitudes (returned) */
int	*gtype;		/* Array of object classes (returned) */
int	nlog;		/* 1 for diagnostics */
{
    char srchurl[LINE];
    char temp[64];
    struct TabTable *tabtable;
    double dr;
    struct StarCat *starcat;
    int nstar, i;
    int rah, ram, dd, dm;
    double ras, ds;
    char sr[4], sd[4];
    double ra, dec, mag, ddra;
    char rastr[32], decstr[32];
    char *gsc2url;

    /* Set URL for search command */
    gsc2url = gsc23url;

    if (nstarmax < 1)
	nlog = -1;

/* make mag1 always the smallest magnitude */
    if (mag2 < mag1) {
	mag = mag2;
	mag2 = mag1;
	mag1 = mag;
	}

    /* Set up query for STScI GSC II server */
    ra = cra;
    dec = cdec;
    if (sysout != WCS_J2000)
	wcscon (sysout, WCS_J2000, eqout, 2000.0, &ra, &dec, epout);
    ra2str (rastr, 32, ra, 3);
    dec2str (decstr, 32, dec, 2);

    parsex (rastr, sr, &rah, &ram, &ras);
    sprintf (srchurl, "?RAH=%d&RAM=%d&RAS=%.3f&", rah, ram, ras);
    parsex (decstr, sd, &dd, &dm, &ds);
    sprintf (temp, "DSN=%1s&DD=%d&DM=%d&DS=%.3f&", sd, dd, dm, ds);
    strcat (srchurl, temp);
    if (drad != 0.0) {
	dr = drad * 60.0;
	}
    else {
	ddra = dra * cos (degrad (cdec));
	dr = sqrt (ddra*ddra + ddec*ddec) * 60.0;
	}
    sprintf (temp, "EQ=2000&SIZE=%.3f&SRCH=Radius&FORMAT=TSV&CAT=GSC23&", dr);
    strcat (srchurl, temp);
    sprintf (temp, "HSTID=&GSC1ID=");
    strcat (srchurl, temp);

    if (nlog > 0)
	fprintf (stderr,"%s%s\n", gsc2url, srchurl);

    /* Run search across the web */
    if ((tabtable = webopen (gsc2url, srchurl, nlog)) == NULL) {
	if (nlog > 0)
	    fprintf (stderr, "WEBREAD: %s failed\n", srchurl);
	return (0);
	}

    /* Return if no data */
    if (tabtable->tabdata == NULL || strlen (tabtable->tabdata) == 0 ||
	!strncasecmp (tabtable->tabdata, "[EOD]", 5)) {
	if (nlog > 0)
	    fprintf (stderr, "WEBRNUM: No data returned\n");
	return (0);
	}

    /* Dump returned file and stop */
    if (nlog < 0) {
	(void) fwrite  (tabtable->tabbuff, tabtable->lbuff, 1, stdout);
	exit (0);
	}

    /* Open returned Starbase table as a catalog */
    if ((starcat = tabcatopen (gsc2url, tabtable,0)) == NULL) {
	if (nlog > 0)
	    fprintf (stderr, "WEBREAD: Could not open Starbase table as catalog\n");
	return (0);
	}

    /* Set reference frame, epoch, and equinox of catalog */
    /* starcat->rpmunit = PM_MASYR;
    starcat->dpmunit = PM_MASYR; */
    starcat->rpmunit = 0;
    starcat->dpmunit = 0;
    starcat->coorsys = WCS_J2000;
    starcat->epoch = 2000.0;
    starcat->equinox = 2000.0;

    /* Extract desired sources from catalog  and return them */
    nstar = tabread (gsc2url,distsort,cra,cdec,dra,ddec,drad,dradi,
	     sysout,eqout,epout,mag1,mag2,sortmag,nstarmax,&starcat,
	     gnum,gra,gdec,gpra,gpdec,gmag,gtype,gobj,nlog);

    tabcatclose (starcat);

    /* Zero out any proper motions for GSC 3.3 and earlier */
    if (!strchr (refcatname, '4')) {
	for (i = 0; i < nstar; i++) {
	    if (i < nstarmax) {
		gpra[i] = 0.0;
		gpdec[i] = 0.0;
		}
	    }
	}

    starcat = NULL;

    return (nstar);
}

static void
parsex (str, ss, d, m, s)

char *str;
char *ss;
int *d;
int *m;
double *s;
{
    char *c0, *cstr;
    c0 = str;
    *ss = (char) 0;
    *d = 0;
    *m = 0;
    *s = 0.0;

    /* Skip over blanks at start of number */
    while (*c0 == ' ')
	c0++;
    if (*c0 == '+' || *c0 == '-') {
	ss[0] = *c0;
	ss[1] = (char) 0;
	c0++;
	}
    else {
	ss[0] = '+';
	ss[1] = (char) 0;
	}
    cstr = strchr (c0,':');
    if (cstr > c0) {
        *cstr = (char) 0;
        *d = (int) atof (c0);
        *cstr = ':';
        c0 = cstr + 1;
        cstr = strchr (c0,':');
        if (cstr > c0) {
            *cstr = '\0';
            *m = (int) atof (c0);
            *cstr = ':';
            c0 = cstr + 1;
            *s = atof (c0);
            }
        else
            *m = (int) atof (c0);
        }
    else
        *d = (int) atof (c0);
}


char *
gsc2c2t (csvbuff)

    char *csvbuff;	/* Input comma-separated table */

{
    char *tabbuff;	/* Output tab-separated table */
    char *databuff;
    char *lastbuff;
    char *oldbuff;
    char *colhead, *colsep;
    int lhead, lbuff, i, j;
    char ctab = (char) 9;
    char ccom = ',';
    char clf = '\n';
    char ccr = '\r';
    char csp = ' ';

    /* First line of buffer is header */
    databuff = strchr (csvbuff, clf) + 1;
    lhead = (int) (databuff - csvbuff);

    /* Allocate buffer for tab-separated table with header */
    lbuff = strlen (databuff) + (2 * lhead);
    tabbuff = (char *) calloc (lbuff, 1);

    /* Copy header into new buffer with tabs instead of commas */
    oldbuff = csvbuff;
    i = 0;
    while (oldbuff < databuff) {
	if (*oldbuff == ccom)
	    tabbuff[i++] = ctab;
	else if (*oldbuff != csp && *oldbuff != ccr && *oldbuff != clf)
	    tabbuff[i++] = *oldbuff;
	oldbuff++;
	}
    tabbuff[i++] = clf;

    /* Make separating line from first line of input file */
    oldbuff = csvbuff;
    while (oldbuff < databuff) {
	if (*oldbuff == ccom)
	    tabbuff[i++] = ctab;
	else if (*oldbuff != csp && *oldbuff != ccr && *oldbuff != clf)
	    tabbuff[i++] = '-';
	oldbuff++;
	}
    tabbuff[i++] = clf;

    /* Drop extraneous data after last linefeed */
    lbuff = strlen (databuff);
    if (lbuff > 0) {
	lastbuff = strrchr (databuff, '\n');
	if (lastbuff - databuff < lbuff)
	    *(lastbuff+1) = (char) 0;

	/* Convert commas in data table to tabs and drop spaces */
	for (j = 0; j < lbuff; j++) {
	    if (databuff[j] == ccom)
		tabbuff[i++] = ctab;
	    else if (databuff[j] != csp && databuff[j] != ccr)
		tabbuff[i++] = databuff[j];
	    }
	}

    return (tabbuff);
}


char *
gsc2t2t (tsvbuff)

    char *tsvbuff;	/* Input tab-separated table */

{
    char *tabbuff;	/* Output tab-separated table */
    int lbuff, i, j;
    char ctab = (char) 9;
    char clf = '\n';
    char ccr = '\r';
    char csp = ' ';

    /* Allocate buffer for tab-separated table with header */
    lbuff = strlen (tsvbuff);
    tabbuff = (char *) calloc (lbuff, 1);

    /* Copy input into new buffer dropping extra carriage returns */
    i = 0;
    for (j = 0; j < lbuff; j++) {
	if (tsvbuff[j] != csp && tsvbuff[j] != ccr)
	    tabbuff[i++] = tsvbuff[j];
	}
    tabbuff[i++] = (char) 0;

    return (tabbuff);
}

/* Jun 22 2001	New program
 * Jun 28 2001	Set proper motion to milliarcseconds/year
 * Jun 29 2001	Always set maximum magnitude to 99.9 to get Tycho-2 stars, too
 * Sep 13 2001	Pass array of magnitudes, not vector
 * Sep 14 2001	Add option to print entire returned file if nlog < 0
 * Sep 20 2001	Make argument starcat, not *starcat in tabcatclose()
 *
 * Apr  8 2002	Fix bugs in null subroutine gsc2rnum()
 * Oct  3 2002	If nstarmax is less than 1, print everything returned
 *
 * Feb  6 2003	Reset nmag to 4 because there is an epoch column
 * Mar 11 2003	Fix URL for search
 * Apr  3 2003	Drop unused variables after lint; drop gsc2rnum()
 * Apr 24 2003	Set nmag to 5 to include epoch, which is not printed
 * Aug 22 2003	Add radi argument for inner edge of search annulus
 * Nov 22 2003	Return object class (c column) as gtype
 * Dec  3 2003	Add option to access GSC 2.3 over the Web
 * Dec  4 2003	Add proper motions for GSC 2.3
 * Dec 11 2003	Search to corners of rectangle, not to longest edge
 * Dec 12 2003	Fix call to tabcatopen()
 *
 * Oct 18 2004	Divide RA by cos(Dec) when computing radius for rect. input
 *
 * Jun 20 2006	Cast fwrite to void
 * Sep  8 2006	Fix comment which mentioned wrong catalog

 * Mar 12 2007	Read from copy in STScI MAST GALEX archive
 * Mar 12 2007	Add parsex \() to separate sexigesimal number into components
 * Mar 13 2007	Add gsc2c2v() to convert comma-separated input to tab-separated
 * Apr 11 2007	Return null data buffer from gsc2c2t() if no data
 *
 * Oct 24 2008	Reset to read from new CASB server and drop GALEX server
 * Oct 24 2008	Add gsc2t2t to drop extra characters from returned table
 *
 * Aug 17 2009	Set proper motion to 0.0 for all versions
 */
