/*** File libwcs/skybotread.c
 *** May 5, 2009
 *** By Doug Mink, dmink@cfa.harvard.edu
 *** Harvard-Smithsonian Center for Astrophysics
 *** Copyright (C) 2004-2009
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

static int obscode = 801;	/* Default to Harvard Observatory, Oak Ridge */
void setobs(newobs)
int newobs;
{obscode = newobs; return; }

static char tabhead[500];	/* Starbase header for returned data */

void
setobsname (obsname)
char *obsname;
{
    if (strcsrch (obsname, "mmt"))
	obscode = 696;	/* Whipple Observatory, Mt. Hopkins */
    else if (strcsrch (obsname, "whip"))
	obscode = 696;	/* Whipple Observatory, Mt. Hopkins */
    else if (strcsrch (obsname, "flw"))
	obscode = 696;	/* Whipple Observatory, Mt. Hopkins */
    else if (strcsrch (obsname, "oak"))
	obscode = 801;	/* Harvard Observatory, Oak Ridge */
    else if (strcsrch (obsname, "hco"))
	obscode = 802;	/* Harvard Observatory, Cambridge */
    else if (strcsrch (obsname, "boy"))
	obscode = 074;	/* Boyden Observatory, Bloemfontein */
    else if (strcsrch (obsname, "are"))
	obscode = 800;	/* Harvard Observatory, Arequipa */
    else
	obscode = 500;	/* Geocenter */
    return;
}

char *
getobsname (code)
int code;
{
    char *obsname;
    obsname = (char *) calloc (64, 1);
    if (code == 696)
	strcpy (obsname, "FLWO Whipple Observatory, Mt. Hopkins");
    else if (code == 801)
	strcpy (obsname, "HCO Oak Ridge");
    else if (code == 802)
	strcpy (obsname, "HCO Cambridge");
    else if (code == 074)
	strcpy (obsname, "Boyden Observatory, Bloemfontein");
    else if (code == 800)
	strcpy (obsname, "HCO Arequipa, Peru");
    else if (code == 500)
	strcpy (obsname, "Geocenter");
    else
	sprintf (obsname, "IAU %d", obscode);
    return (obsname);
}

/* SkyBot search engine URL */
static char skyboturl[128]="http://www.imcce.fr/webservices/skybot/skybot_query.php";

/* skybotread -- Read IMCCE SkyBot server astroids over the web */

int
skybotread (cra,cdec,dra,ddec,drad,distsort,sysout,eqout,epout,mag1,mag2,
	    sortmag,nstarmax,gnum,gobj,gra,gdec,gpra,gpdec,gmag,gtype,nlog)

double	cra;		/* Search center J2000 right ascension in degrees */
double	cdec;		/* Search center J2000 declination in degrees */
double	dra;		/* Search half width in right ascension in degrees */
double	ddec;		/* Search half-width in declination in degrees */
double	drad;		/* Limiting separation in degrees (ignore if 0) */
int	distsort;	/* 1 to sort asteroids by distance from center */
int	sysout;		/* Search coordinate system */
double	eqout;		/* Search coordinate equinox */
double	epout;		/* Julian date for positions (current time if zero) */
double	mag1,mag2;	/* Limiting magnitudes (none if equal) */
int	sortmag;	/* Magnitude by which to sort (1 to nmag) */
int	nstarmax;	/* Maximum number of stars to be returned */
double	*gnum;		/* Array of asteroid numbers (returned) */
char	**gobj;		/* Array of object IDs (too long for integer*4) */
double	*gra;		/* Array of right ascensions (returned) */
double	*gdec;		/* Array of declinations (returned) */
double	*gpra;		/* Array of right ascension motions (returned) */
double	*gpdec;		/* Array of declination motions (returned) */
double	**gmag;		/* 2-D array of magnitudes and other info (returned) */
int	*gtype;		/* Array of object classes (returned) */
int	nlog;		/* 1 for diagnostics */
{
    char srchurl[LINE];
    char cmag;
    char *obs;
    char *dstr;
    struct TabTable *tabtable;
    struct StarCat *starcat; /* Star catalog data structure */
    double dtemp, jdout;
    int nstar, nlog0;
    double ra, dec, mag, dradi, dradx;
    char rastr[32], decstr[32], temp[256], tstr[80];

    /* Set up header for returned Starbase table */
    strcpy (tabhead, "catalog\tSkyBot\n");
    strcat (tabhead, "equinox\t2000.0\n");
    strcat (tabhead, "radecsys\tFK5\n");
    obs = getobsname (obscode);
    sprintf (tstr, "obs\t%s\n", obs);
    strcpy (tabhead, tstr);

    dradi = 0.0;

    nlog0 = nlog;
    if (nstarmax < 1)
	nlog = -1;

    /* mag1 is always the smallest magnitude */
    if (mag2 < mag1) {
	mag = mag2;
	mag2 = mag1;
	mag1 = mag;
	}
    if (mag1 < 0)
	mag1 = 0.0;

    /* Set up query for SkyBot server */
    ra = cra;
    dec = cdec;
    if (sysout != WCS_J2000)
	wcscon (sysout, WCS_J2000, eqout, 2000.0, &ra, &dec, epout);

    /* Epoch for positions */
    jdout = ep2jd (epout);
    sprintf (srchurl, "?-ep=%.5f&", jdout);
    dstr = jd2fd (jdout);
    sprintf (tstr, "epoch\t%s\n",dstr);
    strcat (tabhead, tstr);

    /* Search center */
    sprintf (temp, "-ra=%.5f&-dec=%.5f&", ra, dec);
    strcat (srchurl, temp);
    deg2str (rastr, 32, ra, 5);
    deg2str (decstr, 32, dec, 5);
    sprintf (tstr, "sra\t%s\n",rastr);
    strcat (tabhead, tstr);
    sprintf (tstr, "sdec\t%s\n",decstr);
    strcat (tabhead, tstr);

    /* Radius in degrees */
    if (drad != 0.0) {
	if (drad < 0.0) {
	    dradx = -drad * sqrt (2.0);
	    if (dradx > 10.0)
		dradx = 10.0;
	    sprintf (temp, "-rd=%.5f&", dradx);
	    sprintf (tstr, "dra\t%.5f\n", -drad);
	    strcat (tabhead, tstr);
	    sprintf (tstr, "ddec\t%.5f\n", -drad);
	    strcat (tabhead, tstr);
	    }
	else {
	    if (drad > 10.0)
		dradx = 10.0;
	    else
		dradx = drad;
	    sprintf (temp, "-rd=%.5f&", dradx);
	    sprintf (tstr, "rad\t%.5f\n", dradx);
	    strcat (tabhead, tstr);
	    }
	}

    /* Box size in degrees */
    else {
	dradx = sqrt ((dra * dra) + (ddec * ddec));
	sprintf (temp, "-rd=%.5f&", dradx);
	sprintf (tstr, "dra\t%.6f\n", dra);
	strcat (tabhead, tstr);
	sprintf (tstr, "ddec\t%.6f\n", ddec);
	strcat (tabhead, tstr);
	}

    /* Units for motion on the sky */
    strcat (tabhead, "rpmunit\tarcsec/hour\n");
    strcat (tabhead, "dpmunit\tarcsec/hour\n");

    strcat (srchurl, temp);

    /* Output type */
    strcat (srchurl, "-mime=text&");

    /* IAU observatory code*/
    sprintf (temp, "loc=%03d&", obscode);
    strcat (srchurl, temp);

    /* Drop comets to save time */
    strcat (srchurl, "-objFilter=110&");

    /* Drop planets to save time (unused)
    strcat (srchurl, "-objFilter=100&"); */

    /* Source of search */
    strcat (srchurl, "-from=WCSTools");

    if (nlog0 > 0)
	fprintf (stderr,"%s%s\n", skyboturl, srchurl);

    /* Run search across the web */
    if ((tabtable = webopen (skyboturl, srchurl, nlog)) == NULL) {
	if (nlog > 0)
	    fprintf (stderr, "SKYBOTREAD: %s failed\n", srchurl);
	return (0);
	}

    /* Return if no data */
    if (tabtable->tabdata == NULL || strlen (tabtable->tabdata) == 0 ||
	!strncasecmp (tabtable->tabdata, "[EOD]", 5)) {
	if (nlog > 0)
	    fprintf (stderr, "SKYBOTREAD: No data returned\n");
	return (0);
	}

    /* Dump returned file and stop */
    if (nlog < 0) {
	(void) fwrite  (tabtable->tabbuff, tabtable->lbuff, 1, stdout);
	exit (0);
	}

    /* Open returned Starbase table as a catalog */
    if ((starcat = tabcatopen (skyboturl, tabtable,0)) == NULL) {
	if (nlog > 0)
	    fprintf (stderr, "SKYBOTREAD: Could not open Starbase table as catalog\n");
	return (0);
	}

    /* Set reference frame, epoch, and equinox of catalog */
    starcat->coorsys = WCS_J2000;
    starcat->epoch = 2000.0;
    starcat->equinox = 2000.0;
    starcat->nmag = 3;

    /* Extract desired sources from catalog  and return them */
    nstar = tabread (skyboturl,distsort,cra,cdec,dra,ddec,drad,dradi,
	     sysout,eqout,epout,mag1,mag2,sortmag,nstarmax,&starcat,
	     gnum,gra,gdec,gpra,gpdec,gmag,gtype,gobj,nlog);

    tabcatclose (starcat);

    starcat = NULL;

    return (nstar);
}

char *
skybot2tab (skybuff)

    char *skybuff;	/* Input comma-separated table */
{
    char *heading;
    char *colhead;
    char *colsep;
    char *tabbuff;	/* Output tab-separated table */
    char *tbuff;
    char *databuff;
    char *dbuff;
    char *buffer;
    char *lastbuff;
    char *endhead;
    char *colend;
    char *chead;
    char *head;
    char *tbuffi;
    char temp[16], format[16];
    int lbuff, i, lra, icol;
    int lhead, ldata;
    int addname;
    int lobj;
    char cbuff;
    char *buff;
    char cbar = '|';
    char ccom = ',';
    char cminus = '-';
    char cspace = ' ';
    char ctab = (char) 9;
    char clf = (char) 10;
    double ra;

    /* Skip first two lines of returned header */
    buffer = strchr (skybuff, '\n') + 1;
    buffer = strchr (buffer, '\n') + 1;

    /* Skip header line as it is recreated */
    buffer = strchr (buffer, '\n') + 1;

    /* Allocate starbase table for output */
    lbuff = strlen (skybuff) + strlen (tabhead) + 200;
    tabbuff = (char *) calloc (lbuff, 1);

    /* Add metadata */
    strcpy (tabbuff, tabhead);

    /* Set up tabbed column headings */
    i = 0;
    lhead = 0;
    colhead = tabbuff + strlen (tabbuff);

    /* Combine number and name into first column */
    strcpy (colhead, "object          ");
    strcat (colhead,"\t");

    /* Fix heading for RA column, which will be degrees, not hours */
    strcat (colhead, "ra           ");
    strcat (colhead,"\t");

    /* Fix colheading for Dec column */
    strcat (colhead, "dec         ");
    strcat (colhead,"\t");

    strcat (colhead, "class ");
    strcat (colhead,"\t");

    strcat (colhead, "vmag ");
    strcat (colhead,"\t");

    strcat (colhead, "poserr");
    strcat (colhead,"\t");

    strcat (colhead, "offset");
    strcat (colhead,"\t");

    strcat (colhead, "rapm  ");
    strcat (colhead,"\t");

    strcat (colhead, "decpm ");
    strcat (colhead,"\t");

    strcat (colhead, "gdist       ");
    strcat (colhead,"\t");

    strcat (colhead, "hdist       ");
    strcat (colhead,"\n");
    lhead = strlen (colhead);

    /* Set up tabbed separator line */
    colsep = colhead + lhead;
    for (i = 0; i < lhead; i++) {
	cbuff = colhead[i];
	if (cbuff == ctab)
	    colsep[i] = ctab;
	else if (cbuff == (char) 10)
	    colsep[i] = (char) 10;
	else
	    colsep[i] = '-';
	}

    /* Copy input through final linefeed */
    lastbuff = strrchr (buffer, '\n') + 1;

    /* Copy data to output buffer, dropping leading and trailing spaces
       and converting vertical bars in table to tabs */
    dbuff = buffer;
    tbuff = tabbuff + strlen (tabbuff);
    icol = 0;
    while (dbuff < lastbuff) {

	/* Drop out if blank line encountered */
	if (icol == 0 && *dbuff == clf)
	    break;

	/* Combine ID and name in first column */
	if (icol == 0) {
	    tbuffi = tbuff;
	    while (*dbuff == cspace)
		dbuff++;
	    if (*dbuff == cminus) {
		addname = 0;
		dbuff++;
		}
	    else
		addname = 1;
	    while (*dbuff != cbar) {
		if (*dbuff != cspace)
		    *tbuff++ = *dbuff;
		dbuff++;
		}
	    if (*dbuff == cbar) {
		icol++;
		dbuff++;
		}
	    if (addname)
		*tbuff++ = '(';
	    while (*dbuff != cbar) {
		if (*dbuff != cspace)
		    *tbuff++ = *dbuff;
		dbuff++;
		}
	    if (addname)
		*tbuff++ = ')';
	    lobj = tbuff - tbuffi;
	    if (lobj < 16) {
		for (i = lobj; i < 16; i++)
		    *tbuff++ = ' ';
		}
	    }
	if (*dbuff == cbar) {
	    *tbuff++ = ctab;
	    icol++;
	    dbuff++;

	    /* Convert RA from fractional hours to fractional degrees */
	    if (icol == 2) {
		ra = atof (dbuff) * 15.0;
		colend = strchr (dbuff, cbar) - 1;
		lra = colend - dbuff;
		sprintf (format,"%%%d.%df",lra,lra-4);
		sprintf (temp, format, ra);
		for (i = 0; i < lra; i++)
		    dbuff[i] = temp[i];
		dbuff--;
		}

	    /* Shorten "Satellite" to Sat */
	    if (icol == 4) {
		if (!strncmp (dbuff, " Sat", 4)) {
		    *tbuff++ = 'S';
		    *tbuff++ = 'a';
		    *tbuff++ = 't';
		    dbuff = dbuff + 9;
		    }
		else
		    dbuff--;
		}
	    }
	else if (*dbuff == clf) {
	    *tbuff++ = *dbuff;
	    icol = 0;
	    }
	else if (*dbuff != cspace)
	    *tbuff++ = *dbuff;
	dbuff++;
	}

    return (tabbuff);
}

/* Jul 26 2004	New program
 *
 * May  5 2009	Add -objFilter=110 to drop comets (someday, this might be optional)
 */
