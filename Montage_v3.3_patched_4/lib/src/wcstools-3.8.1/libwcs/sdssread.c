/*** File libwcs/sdssread.c
 *** October 22, 2007
 *** By Doug Mink, dmink@cfa.harvard.edu
 *** Harvard-Smithsonian Center for Astrophysics
 *** Copyright (C) 2004-2007
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

#define ABS(a) ((a) < 0 ? (-(a)) : (a))
#define LINE    1024

/* SDSS DR1 search engine URL
char sdssrurl[64]="http://skyserver.sdss.org/cas/en/tools/search/x_radial.asp";
char sdssburl[64]="http://skyserver.sdss.org/cas/en/tools/search/x_rect.asp"; */

/* SDSS DR4 search engine URL
char sdssrurl[64]="http://cas.sdss.org/dr4/en/tools/search/x_radial.asp";
char sdssburl[64]="http://cas.sdss.org/dr4/en/tools/search/x_rect.asp"; */

/* SDSS DR5 search engine URL
char sdssrurl[64]="http://cas.sdss.org/dr5/en/tools/search/x_radial.asp";
char sdssburl[64]="http://cas.sdss.org/dr5/en/tools/search/x_rect.asp"; */

/* SDSS DR6 search engine URL
char sdssrurl[64]="http://cas.sdss.org/dr6/en/tools/search/x_radial.asp";
char sdssburl[64]="http://cas.sdss.org/dr6/en/tools/search/x_rect.asp"; */

/* SDSS DR7 search engine URL */
char sdssrurl[64]="http://cas.sdss.org/dr7/en/tools/search/x_radial.asp";
char sdssburl[64]="http://cas.sdss.org/dr7/en/tools/search/x_rect.asp";

/* SDSS magnitudes */
char sdssmag[6]="ugriz";

/* SDSSREAD -- Read Sloan Digital Sky Survey catalog stars over the web */

int
sdssread (cra,cdec,dra,ddec,drad,dradi,distsort,sysout,eqout,epout,
	  mag1,mag2,sortmag,nstarmax,gnum,gobj,gra,gdec,gmag,gtype,nlog)

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
char	**gobj;		/* Array of object IDs (too long for integer*4) */
double	*gra;		/* Array of right ascensions (returned) */
double	*gdec;		/* Array of declinations (returned) */
double	**gmag;		/* 2-D array of magnitudes (returned) */
int	*gtype;		/* Array of object classes (returned) */
int	nlog;		/* 1 for diagnostics */
{
    char srchurl[LINE];
    char temp[64];
    char cmag;
    struct TabTable *tabtable;
    double dtemp;
    double *gpra, *gpdec;
    struct StarCat *starcat;
    int nstar, nlog0;
    double ra, dec, mag;
    char rastr[32], decstr[32];
    char *sdssurl;

    gpra = NULL;
    gpdec = NULL;

    nlog0 = nlog;
    if (nstarmax < 1)
	nlog = -1;

/* make mag1 always the smallest magnitude */
    if (mag2 < mag1) {
	mag = mag2;
	mag2 = mag1;
	mag1 = mag;
	}
    if (mag1 < 0)
	mag1 = 0.0;

    /* Set up query for STScI GSC II server */
    ra = cra;
    dec = cdec;
    if (sysout != WCS_J2000)
	wcscon (sysout, WCS_J2000, eqout, 2000.0, &ra, &dec, epout);

    sdssurl = sdssrurl;
    deg2str (rastr, 32, ra, 5);
    deg2str (decstr, 32, dec, 5);

    /* Radius or box size */
    if (drad != 0.0) {
	dtemp = drad * 60.0;
	sprintf (srchurl, "?ra=%.5f&dec=%.5f&radius=%.3f",
		 ra, dec, dtemp);
	}
    else {
	dtemp = sqrt (dra*dra + ddec*ddec) * 60.0;
	sprintf (srchurl, "?ra=%.5f&dec=%.5f&radius=%.3f",
		 ra, dec, dtemp);
	}

    /* Magnitude limit, if any */
    if (sortmag < 1)
	cmag = 'g';
    else
	cmag = sdssmag[sortmag - 1];
    if (mag1 < mag2) {
	sprintf (temp, "&check_%c=%c&min_%c=%.2f&max_%c=%.2f",
		 cmag, cmag, cmag, mag1, cmag, mag2);
	strcat (srchurl, temp);
	}
    nstar = 50000;
    sprintf (temp, "&entries=top&topnum=%d&format=csv",nstar);
    strcat (srchurl, temp);
    if (nlog0 > 0)
	fprintf (stderr,"%s%s\n", sdssurl, srchurl);

    /* Run search across the web */
    if ((tabtable = webopen (sdssurl, srchurl, nlog)) == NULL) {
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
    if ((starcat = tabcatopen (sdssurl, tabtable,0)) == NULL) {
	if (nlog > 0)
	    fprintf (stderr, "WEBREAD: Could not open Starbase table as catalog\n");
	return (0);
	}

    /* Set reference frame, epoch, and equinox of catalog */
    starcat->coorsys = WCS_J2000;
    starcat->epoch = 2000.0;
    starcat->equinox = 2000.0;
    starcat->nmag = 5;

    /* Extract desired sources from catalog  and return them */
    nstar = tabread (sdssurl,distsort,cra,cdec,dra,ddec,drad,dradi,
	     sysout,eqout,epout,mag1,mag2,sortmag,nstarmax,&starcat,
	     gnum,gra,gdec,gpra,gpdec,gmag,gtype,gobj,nlog);

    tabcatclose (starcat);

    starcat = NULL;

    return (nstar);
}

char *
sdssc2t (csvbuff)

    char *csvbuff;	/* Input comma-separated table */

{
    char colhead[180]="objID             	run	rerun	camcol	field	obj	type	ra        	dec      	umag	gmag	rmag	imag	zmag	uerr    	gerr   	rerr    	ierr    	zerr   \n";
    char colsep[180]="------------------	---	-----	------	-----	---	----	----------	---------	------	------	------	------	------	--------	------	--------	--------	-------\n";
    char *tabbuff;	/* Output tab-separated table */
    char *databuff;
    char *lastbuff;
    int lbuff, i;
    char ctab = (char) 9;
    char ccom = ',';

    /* Skip first line of returned header */
    databuff = strchr (csvbuff, '\n') + 1;

    /* Drop extraneous data after last linefeed */
    lbuff = strlen (databuff);
    lastbuff = strrchr (databuff, '\n');
    if (lastbuff - databuff < lbuff)
	*(lastbuff+1) = (char) 0;

    /* Convert commas in table to tabs */
    lbuff = strlen (databuff);
    for (i = 0; i < lbuff; i++) {
	if (databuff[i] == ccom)
	    databuff[i] = ctab;
	}

    /* Allocate buffer for tab-separated table with header */
    lbuff = strlen (databuff) + strlen (colhead) + strlen (colsep);
    tabbuff = (char *) calloc (lbuff, 1);

    /* Copy column headings, separator, and data to output buffer */
    strcpy (tabbuff, colhead);
    strcat (tabbuff, colsep);
    strcat (tabbuff, databuff);

    return (tabbuff);
}

/* Jan  5 2004	New program
 *
 * Apr  6 2006	Use different server to get DR4 data
 * Jun 20 2006	Drop unused variables
 * Jul 11 2006	Change path to Data Release 5
 * Oct 30 2006	Print URL in verbose mode when printing web-returned sources
 * Oct 30 2006	Fix bug in buffer length when setting up tab table
 * Nov  3 2006	Drop extra characters from end of data returned from SDSS
 * Nov  6 2006	Pass SDSS ID as character string because it is too long integer
 *
 * Jan  8 2007	Drop unused variables
 * Jan  9 2007	Drop refcatname from argument list; it is not used
 * Jan 10 2007	Drop gnum argument from sdssread(); gobj replaced it
 * Oct 22 2007	Change path to Data Release 6
 */
