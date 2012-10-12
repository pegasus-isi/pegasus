/* File wcshead.c
 * December 21, 2007
 * By Doug Mink Harvard-Smithsonian Center for Astrophysics)
 * Send bug reports to dmink@cfa.harvard.edu

   Copyright (C) 1998-2007
   Smithsonian Astrophysical Observatory, Cambridge, MA USA

   This program is free software; you can redistribute it and/or
   modify it under the terms of the GNU General Public License
   as published by the Free Software Foundation; either version 2
   of the License, or (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software Foundation,
   Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>
#include <math.h>
#include "libwcs/fitsfile.h"
#include "libwcs/wcs.h"
#include "libwcs/wcscat.h"

static void usage();
static int ListWCS();

static int verbose = 0;		/* verbose/debugging flag */
static int tabout = 0;		/* tab table output flag */
static int ndec = 3;		/* maximum number of decimal places for output*/
static int nchar = 24;		/* maximum number of characters for filename */
static int hms = 0;		/* 1 for output in hh:mm:ss dd:mm:ss */
static int nf = 0;		/* Number of files */
static int version = 0;		/* If 1, print only program name and version */
static int wave = 0;		/* If 1, print first dimension limits */
static int restwave = 0;	/* If 1, print first dimension limits */
static int printhead = 1;	/* 1 until header has been printed */
static char *rootdir=NULL;	/* Root directory for input files */

static char *RevMsg = "WCSHEAD WCSTools 3.8.1, 14 December 2009, Doug Mink SAO";

int
main (ac, av)
int ac;
char **av;
{
    char *str;
    int readlist = 0;
    int lfile;
    char *lastchar;
    char filename[256];
    FILE *flist;
    char *listfile = NULL;
    struct Range *erange;
    char *fext = NULL;
    char *fcomma = NULL;
    int nfext=0;
    int i, j, nch;
    char *namext;
    char *extension;         /* Extension number or name to read */
    int nrmax=10;

    erange = NULL;
    extension = NULL;

    /* Check for help or version command first */
    str = *(av+1);
    if (!str || !strcmp (str, "help") || !strcmp (str, "-help"))
	usage();
    if (!strcmp (str, "version") || !strcmp (str, "-version")) {
	version = 1;
	usage();
	}

    /* crack arguments */
    for (av++; --ac > 0 && (*(str = *av)=='-' || *str == '@'); av++) {
	char c;
	if (*str == '@')
	    str = str - 1;
	while ((c = *++str))
	switch (c) {

	case 'd':	/* Root directory for input files (default is cwd) */
	    if (ac < 2)
		usage();
	    rootdir = *++av;
	    ac--;
	    break;

	case 'h':	/* hh:mm:ss output for crval, cdelt in arcsec/pix */
	    hms++;
	    break;

    	case 'n':	/* Number of decimal places in coordinates */
    	    if (ac < 2)
    		usage();
    	    ndec = atoi (*++av);
    	    ac--;
    	    break;

	case 'r':	/* Print first dimension as rest wavelength, first and last values */
	    restwave++;
	    break;

	case 't':	/* tab table output */
	    tabout++;
	    break;

	case 'v':	/* more verbosity */
	    verbose++;
	    break;

	case 'w':	/* Print only first dimension, first and last values */
	    wave++;
	    break;

    	case 'z':	/* Use AIPS classic WCS */
    	    setdefwcs (WCS_ALT);
    	    break;

	case '@':	/* List of files to be read */
	    readlist++;
	    listfile = ++str;
	    str = str + strlen (str) - 1;
	    av++;
	    ac--;
	    break;

	default:
	    usage();
	    break;
	}
    }

    /* Read filenames of images from listfile */
    if (readlist) {

	/* Find maximimum filename length */
	if ((flist = fopen (listfile, "r")) == NULL) {
	    fprintf (stderr,"WCSHEAD: List file %s cannot be read\n",
		     listfile);
	    usage();
	    }
	nchar = 8;
	while (fgets (filename, 256, flist) != NULL) {
	    lfile = strlen (filename) - 1;
	    if (lfile > nchar) nchar = lfile;
	    }
	fclose (flist);

	/* Process files */
	if ((flist = fopen (listfile, "r")) == NULL) {
	    fprintf (stderr,"WCSHEAD: List file %s cannot be read\n",
		     listfile);
	    usage();
	    }
	while (fgets (filename, 256, flist) != NULL) {
	    lastchar = filename + strlen (filename) - 1;
	    *lastchar = 0;
	    if ((fcomma = strchr (filename, ','))) {
		fext = fcomma + 1;
		if (isrange (fext)) {
		    erange = RangeInit (fext, nrmax);
		    nfext = rgetn (erange);
		    extension = calloc (1, 8);
		    *fcomma = (char) 0;
		    }
		else if (!strcmp (fext, "all")) {
		    erange = RangeInit ("1-500", nrmax);
		    nfext = rgetn (erange);
		    extension = calloc (1, 8);
		    *fcomma = (char) 0;
		    }
		else
		    nfext = 1;
		}
	    else
		nfext = 0;
	    if (nfext > 1) {
		rstart (erange);
		for (i = 0; i < nfext; i++) {
		    j = rgeti4 (erange);
		    sprintf (extension, "%d", j);
		    nch = strlen (filename) + 2 + strlen (extension);
		    namext = (char *) calloc (1, nch);
		    strcpy (namext, filename);
		    strcat (namext, ",");
		    strcat (namext, extension);
		    if (ListWCS (namext))
			break;
		    free (namext);
		    }
		}
	    else
		(void) ListWCS (filename);

	    if (verbose)
		printf ("\n");
	    }
	fclose (flist);
	}

    /* If no arguments left, print usage */
    if (ac == 0)
	usage();

    if (verbose)
	fprintf (stderr,"%s\n",RevMsg);

    nf = 0;
    while (ac-- > 0) {
	char *fn = *av++;
	nf++;
	if ((fcomma = strchr (fn, ','))) {
	    fext = fcomma + 1;
	    if (isrange (fext)) {
		erange = RangeInit (fext, nrmax);
		nfext = rgetn (erange);
		extension = calloc (1, 8);
		*fcomma = (char) 0;
		}
	    else if (!strcmp (fext, "all")) {
		erange = RangeInit ("1-500", nrmax);
		nfext = rgetn (erange);
		extension = calloc (1, 8);
		*fcomma = (char) 0;
		}
	    else
		nfext = 1;
	    }
	else
	    nfext = 0;
	if (nfext > 1) {
	    rstart (erange);
	    for (i = 0; i < nfext; i++) {
		j = rgeti4 (erange);
		sprintf (extension, "%d", j);
		nch = strlen (fn) + 2 + strlen (extension);
		namext = (char *) calloc (1, nch);
		strcpy (namext, fn);
		strcat (namext, ",");
		strcat (namext, extension);
		if (ListWCS (namext))
		    break;
		free (namext);
		}
	    }
	else
	    (void) ListWCS (fn);
	}

    return (0);
}

static void
usage ()
{
    fprintf (stderr,"%s\n",RevMsg);
    if (version)
	exit (-1);
    fprintf (stderr,"Print WCS part of FITS or IRAF image header\n");
    fprintf (stderr,"usage: wcshead [-htv] file.fit ...\n");
    fprintf (stderr, " -d dir: Root directory for input files (default is cwd)\n");
    fprintf (stderr,"  -h: Print CRVALs as hh:mm:ss dd:mm:ss\n");
    fprintf (stderr,"  -n num: Number of decimal places in CRVAL output\n");
    fprintf (stderr,"  -r: Print first dimension as rest wavelength limiting values\n");
    fprintf (stderr,"  -t: Print tab table output\n");
    fprintf (stderr,"  -v: Verbose\n");
    fprintf (stderr,"  -w: Print only first dimension limiting values\n");
    fprintf (stderr,"  -z: Use AIPS classic WCS subroutines\n");
    exit (1);
}

static int
ListWCS (filename)

char	*filename;	/* FITS or IRAF image file name */
{
    double w1, w2, dx, dy;
    int i, nxpix, nypix;
    char str[256], temp[256];
    char pathname[256], ctype[16];
    char *header = NULL;
    char *GetFITShead();
    char rastr[32], decstr[32], fform[8];
    struct WorldCoor *wcs, *GetWCSFITS();
    double wfrst, dwl, wlast, dxpix, zvel, vel;
    int logwav = 0;

    if (rootdir){
	strcpy (pathname, rootdir);
	strcat (pathname, "/");
	strcat (pathname, filename);
	}
    else
	strcpy (pathname, filename);
    wcs = GetWCSFITS (pathname, verbose);
    if (nowcs (wcs)) {
	wcsfree (wcs);
	wcs = NULL;
	if ((header = GetFITShead (pathname, verbose)) == NULL)
	    return (-1);
	if (wave) {
	    hgetr8 (header, "NAXIS1", &dxpix);
	    hgeti4 (header, "DC-FLAG", &logwav);
	    hgetr8 (header, "CRVAL1", &wfrst);
	    hgetr8 (header, "CDELT1", &dwl);
	    if (logwav)
		strcpy (ctype, "LOGWAV");
	    else
		strcpy (ctype, "LINEAR");
	    hgets (header, "CTYPE1", 31, ctype);
	    hgetr8 (header, "VELOCITY", &vel);
	    zvel = vel / 299792.5;
	    }
	}
    else {
	dxpix = wcs->nxpix;
	wfrst = wcs->xref;
	dwl = wcs->cdelt[0];
	strcpy (ctype, wcs->ctype[0]);
	zvel = wcs->zvel;
	logwav = wcs->logwcs;
	}

    if (wcs && wcs->ctype[0][0] == (char) 0) {
	wcsfree (wcs);
	wcs = NULL;
	header = GetFITShead (pathname, verbose);
	if (wave) {
	    hgetr8 (header, "NAXIS1", &dxpix);
	    hgeti4 (header, "DC-FLAG", &logwav);
	    hgetr8 (header, "CRVAL1", &wfrst);
	    hgetr8 (header, "CDELT1", &dwl);
	    if (logwav)
		strcpy (ctype, "LOGWAV");
	    else
		strcpy (ctype, "LINEAR");
	    hgetr8 (header, "VELOCITY", &vel);
	    zvel = vel / 299792.5;
	    }
	}
    if (tabout && printhead) {
	strcpy (str, "filename");
	for (i = 1; i < nchar - 7; i++) strcat (str, " ");
	if (wave || restwave) {
	    strcat (str, "	naxis1	ctype1	first    ");
	    strcat (str, "	last     	delt   \n");
	    for (i = 0; i < nchar; i++) strcat (str, "-");
	    strcat (str, "	------	------");
	    strcat (str, "	---------	---------	-------\n");
	    }
	else {
	    strcat (str, "	naxis1	naxis2");
	    strcat (str, "	ctype1  	ctype2  ");
	    strcat (str, "	crval1	crval2	system  ");
	    strcat (str, "	crpix1	crpix2");
	    strcat (str, "	cdelt1	cdelt2");
	    strcat (str, "	crota2\n");
	    for (i = 0; i < nchar; i++) strcat (str, "-");
	    strcat (str, "	------	------");
	    if (ndec <= 2)
		strcat (str, "	-------	-------");
	    else if (ndec == 3)
		strcat (str, "	--------	--------");
	    else if (ndec == 4)
		strcat (str, "	---------	---------");
	    else if (ndec == 5)
		strcat (str, "	----------	----------");
	    else
		strcat (str, "	-----------	-----------");
	    strcat (str, "	-------	-------");
	    strcat (str, "	--------	-------");
	    strcat (str, "	-------	-------");
	    strcat (str, "	-------	-------\n");
	    }
	printf ("%s", str);
	printhead = 0;
	}

    sprintf (fform,"%%%d.%ds",nchar, nchar);
    if (tabout) {
	sprintf (str, fform, filename);
	if (wave) {
	    wlast = wfrst + ((dxpix - 1.0) * dwl);
	    if (logwav) {
		w1 = exp (log(10.0) * wfrst);
		w2 = exp (log(10.0) * wlast);
		dwl = (w2 - w1) / (dxpix - 1.0);
		}
	    else {
		w1 = wfrst;
		w2 = wlast;
		}
	    sprintf (temp, "	%.0f	%s	%9.4f	%9.4f	%7.4f",
			 dxpix, ctype, w1, w2, dwl);
	    strcat (str, temp);
	    }
	else if (restwave) {
	    wlast = wfrst + ((dxpix - 1.0) * dwl);
	    if (logwav) {
		w1 = exp (log (10.0) * wfrst) / (1.0 + zvel);
		w2 = exp (log (10.0) * wlast) / (1.0 + zvel);
		dwl = (w2 - w1) / (dxpix - 1.0);
		}
	    else {
		w1 = wfrst / (1.0 + zvel);
		w2 = wlast / (1.0 + zvel);
		}
	    sprintf (temp, "	%.0f	%s	%9.4f	%9.4f	%7.4f",
		     dxpix, ctype, w1, w2, dwl);
	    strcat (str, temp);
	    }
	else if (!wcs) {
	    if (wave) {
		strcat (str, "	___	___	_________	_________	_______");
		}
	    else if (restwave) {
		strcat (str, "	___	___	_________	_________	_______");
		}
	    else {
		hgeti4 (header, "NAXIS1", &nxpix);
		hgeti4 (header, "NAXIS2", &nypix);
		sprintf (temp, "	%d	%d", nxpix, nypix);
		strcat (str, temp);
		strcat (str, "	________	________	_______	_______	________	_______	_______	_______	_______	_______");
		}
	    }
	else {
	    sprintf (temp, "	%.0f	%.0f", wcs->nxpix, wcs->nypix);
	    strcat (str, temp);
	    if (strlen (wcs->ctype[0]) < 8)
		sprintf (temp, "	%8.8s	%8.8s", wcs->ctype[0], wcs->ctype[1]);
	    else
		sprintf (temp, "	%s	%s", wcs->ctype[0], wcs->ctype[1]);
	    strcat (str, temp);
	    if (hms) {
		if (wcs->coorflip) {
			ra2str (rastr, 32, wcs->yref, ndec);
			dec2str (decstr, 32, wcs->xref, ndec-1);
			}
		else {
			ra2str (rastr, 32, wcs->xref, ndec);
			dec2str (decstr, 32, wcs->yref, ndec-1);
			}
		}
	    else {
		num2str (rastr, wcs->xref, 0, ndec);
		num2str (decstr, wcs->yref, 0, ndec);
		}
	    sprintf (temp, "	%s	%s	%s",rastr,decstr,wcs->radecsys);
	    strcat (str, temp);
	    sprintf (temp, "	%7.2f	%7.2f", wcs->xrefpix, wcs->yrefpix);
	    strcat (str, temp);
	    dx = 3600.0 * wcs->xinc;
	    dy = 3600.0 * wcs->yinc;
	    if (dx >= 10000.0 || dx <= -1000.0)
		sprintf (temp, "	%7.1f	%7.1f", dx, dy);
	    else if (dx >= 1000.0 || wcs->xinc <= -100.0)
		sprintf (temp, "	%7.2f	%7.2f", dx, dy);
	    else if (dx >= 100.0 || dx <= -10.0)
		sprintf (temp, "	%7.3f	%7.3f", dx, dy);
	    else
		sprintf (temp, "	%7.4f	%7.4f", dx, dy);
	    strcat (str, temp);
	    sprintf (temp, "	%7.4f", wcs->rot);
	    strcat (str, temp);
	    }
	}
    else {
	sprintf (str, fform, filename);
	if (wave) {
	    sprintf (temp, " %.0f %s", dxpix, ctype);
	    strcat (str, temp);
	    wlast = wfrst + ((dxpix - 1.0) * dwl);
	    if (logwav) {
		w1 = exp (log(10.0) * wfrst);
		w2 = exp (log(10.0) * wlast);
		dwl = (w2 - w1) / (dxpix - 1.0);
		}
	    else {
		w1 = wfrst;
		w2 = wlast;
		}
	    sprintf (temp, " %9.4f %9.4f %7.4f", w1, w2, dwl);
	    strcat (str, temp);
	    }
	else if (restwave) {
	    wlast = wfrst + ((dxpix - 1.0) * dwl);
	    if (logwav) {
		w1 = exp (log(10.0) * wfrst);
		w2 = exp (log(10.0) * wlast);
		dwl = (w2 - w1) / (dxpix - 1.0);
		}
	    else {
		w1 = wfrst;
		w2 = wlast;
		}
	    w1 = w1 / (1.0 + zvel);
	    w2 = w2 / (1.0 + zvel);
	    sprintf (temp, " %.0f %s %9.4f %9.4f %7.4f",
		     dxpix, ctype, w1, w2, dwl);
	    strcat (str, temp);
	    }
	else if (!wcs) {
	    if (wave) {
		strcpy (temp, " ___ ___ _________ _________ _______");
		}
	    else if (restwave) {
		strcpy (temp, " ___ ___ _________ _________ _______");
		}
	    else {
		hgeti4 (header, "NAXIS1", &nxpix);
		hgeti4 (header, "NAXIS2", &nypix);
		sprintf (temp, " %4d %4d", nxpix, nypix);
		strcat (str, temp);
		strcpy (temp, " ________ ________ _______ _______ ________ _______ _______ _______ _______ _______");
		}
	    strcat (str, temp);
	    }
	else {
	    sprintf (temp, " %4.0f %4.0f", wcs->nxpix, wcs->nypix);
	    strcat (str, temp);
	    if (strlen (wcs->ctype[0]) < 8)
		    sprintf (temp, " %8.8s %8.8s", wcs->ctype[0], wcs->ctype[1]);
	    else
		sprintf (temp, " %s %s", wcs->ctype[0], wcs->ctype[1]);
	    strcat (str, temp);
	    if (hms) {
		if (wcs->coorflip) {
		    ra2str (rastr, 32, wcs->yref, ndec);
		    dec2str (decstr, 32, wcs->xref, ndec-1);
		    }
		else {
		    ra2str (rastr, 32, wcs->xref, ndec);
		    dec2str (decstr, 32, wcs->yref, ndec-1);
		    }
		}
	    else {
		num2str (rastr, wcs->xref, 0, ndec);
		num2str (decstr, wcs->yref, 0, ndec);
		}
	    sprintf (temp, " %s %s %s", rastr, decstr, wcs->radecsys);
	    strcat (str, temp);
	    sprintf (temp, " %7.2f %7.2f", wcs->xrefpix, wcs->yrefpix);
	    strcat (str, temp);
	    dx = 3600.0 * wcs->xinc;
	    dy = 3600.0 * wcs->yinc;
	    if (dx >= 10000.0 || dx <= -1000.0)
		sprintf (temp, " %7.1f %7.1f", dx, dy);
	    else if (dx >= 1000.0 || wcs->xinc <= -100.0)
		sprintf (temp, " %7.2f %7.2f", dx, dy);
	    else if (dx >= 100.0 || dx <= -10.0)
		sprintf (temp, " %7.3f %7.3f", dx, dy);
	    else
		sprintf (temp, " %7.4f %7.4f", dx, dy);
	    strcat (str, temp);
	    sprintf (temp, " %7.4f", wcs->rot);
	    strcat (str, temp);
	    }
	}

    printf ("%s\n", str);

    wcsfree (wcs);
    wcs = NULL;

    return (0);
}
/* Feb 18 1998	New program
 * May 27 1998	Include fitsio.h instead of fitshead.h
 * Jun 24 1998	Add string lengths to ra2str() and dec2str() calls
 * Jul 10 1998	Add option to use AIPS classic WCS subroutines
 * Aug  6 1998	Change fitsio.h to fitsfile.h
 * Nov 30 1998	Add version and help commands for consistency
 *
 * Apr  7 1999	Print lines all at once instead of one variable at a time
 * Jun  3 1999	Change PrintWCS to ListWCS to avoid name conflict
 * Oct 15 1999	Free wcs using wcsfree()
 * Oct 22 1999	Drop unused variables after lint
 * Nov 30 1999	Fix declaration of ListWCS()
 *
 * Jan 28 2000	Call setdefwcs() with WCS_ALT instead of 1
 * Jun 21 2000	Add -w option to print limits for 1-d WCS
 * Aug  4 2000	Add -w option to printed option list
 *
 * Apr  8 2002	Free wcs structure if no WCS is found in file header
 * May  9 2002	Add option to print rest wavelength limits
 * May 13 2002	Set wcs pointer to NULL after freeing data structure
 * Jun 19 2002	Add verbose argument to GetWCSFITS()
 *
 * Jul 19 2004	Print header if flag is set, not if first file
 * Jul 19 2004	Print underscores to fill lines if no WCS
 *
 * Jun 21 2006	Clean up code
 *
 * Jan 18 2007	Add -n option to set number of decimal places in CRVALi output
 * Jan 25 2007	Add -d option to specify a rood directory
 * Jan 25 2007	Handle log wavelength as well as wavelength
 * Jan 26 2007	Deal with incomplete WCS
 * Feb  1 2007	Fix wavelength delta for log wavelength
 * Dec 21 2007	Add option to put range of extensions in filenames
 */
