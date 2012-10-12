/* File imsize.c
 * March 24, 2009
 * By Doug Mink, Harvard-Smithsonian Center for Astrophysics
 * Send bug reports to dmink@cfa.harvard.edu

   Copyright (C) 1996-2009
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
extern void setsys();
extern void setcenter();
extern void setsecpix();
extern struct WorldCoor *GetFITSWCS();
extern char *GetFITShead();
static int PrintWCS();

static char coorsys[8];
static double size = 0.0;
static double frac = 0.0;

static char *RevMsg = "IMSIZE WCSTools 3.8.1, 14 December 2009, Doug Mink (dmink@cfa.harvard.edu)";

static int verbose = 0;		/* verbose/debugging flag */
static int dss = 0;		/* Flag to drop extra stuff for DSS */
static int dssc = 0;		/* Flag to drop extra stuff for DSS */
static int degout = 0;		/* Flag to print center in degrees */
static double eqout = 0.0;
static double eqim = 0.0;
static int sysout0 = 0;
static int sysim = 0;
static int printepoch = 0;	/* If 1, print epoch of image */
static int printyear = 0;	/* If 1, print FITS date of image */
static int printrange = 0;	/* Flag to print range rather than center */
static int version = 0;		/* If 1, print only program name and version */
static int ndec = 3;		/* Number of decimal places in non-angles */
static char *extensions;	/* Extension number(s) or name to read */
static char *extension;		/* Extension number or name to read */

int
main (ac, av)
int ac;
char **av;
{
    char *str, *str1;
    char rastr[32];
    char decstr[32];
    int readlist = 0;
    char *lastchar;
    char filename[128];
    FILE *flist;
    char *listfile = NULL;
    char *fext = NULL;
    char *fcomma = NULL;
    int nfext=0;
    int i, j, nch;
    char *namext;
    int nrmax=10;
    struct Range *erange;

    coorsys[0] = 0;
    extension = NULL;
    extensions = NULL;
    erange = NULL;

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

	case 'v':	/* more verbosity */
	    verbose++;
	    break;

	case 'b':	/* ouput B1950 (B1950) coordinates */
	    eqout = 1950.0;
	    sysout0 = WCS_B1950;
	    str1 = *(av+1);
	    if (*(str+1) || !strchr (str1,':'))
		strcpy (coorsys, "B1950");
	    else if (ac < 3)
		usage ();
	    else {
		setsys (WCS_B1950);
		strcpy (coorsys, "B1950");
		strcpy (rastr, *++av);
		ac--;
		strcpy (decstr, *++av);
		ac--;
		setcenter (rastr, decstr);
		}
	    break;

	case 'c':	/* Change output for DSS */
	    strcpy (coorsys, "J2000");
	    eqout = 2000.0;
	    sysout0 = WCS_J2000;
	    dssc++;
	    str1 = *(av+1);
	    if (!*(str+1) && (strchr (str1,'-') || strchr (str1,'+')) ) {
		size = atof (*++av);
		ac--;
		}
	    else if (!*(str+1) && strchr (str1,'x') ) {
		frac = atof (*++av+1);
		ac--;
		}
	    break;

	case 'd':	/* output center in degrees */
	    degout++;
	    break;

	case 'e':	/* output epoch of plate */
	    printepoch++;
	    break;

	case 'j':	/* output J2000 (J2000) coordinates */
	    str1 = *(av+1);
	    eqout = 2000.0;
	    sysout0 = WCS_J2000;
	    if (*(str+1) || !strchr (str1,':'))
		strcpy (coorsys, "J2000");
	    else if (ac < 3)
		usage ();
	    else {
		setsys (WCS_J2000);
		strcpy (coorsys, "J2000");
		strcpy (rastr, *++av);
		ac--;
		strcpy (decstr, *++av);
		ac--;
		setcenter (rastr, decstr);
		}
	    break;

    	case 'n':	/* Number of decimal places in coordinates */
    	    if (ac < 2)
    		usage();
    	    ndec = atoi (*++av);
    	    ac--;
    	    break;

    	case 'p':	/* Initial plate scale in arcseconds per pixel */
    	    if (ac < 2)
    		usage();
    	    setsecpix (atof (*++av));
    	    ac--;
    	    break;

	case 'r':
	    printrange++;
	    break;

	case 's':	/* Change output for DSS getimage */
	    strcpy (coorsys, "J2000");
	    eqout = 2000.0;
	    sysout0 = WCS_J2000;
	    dss++;
	    str1 = *(av+1);
	    if (!*(str+1) && (strchr (str1,'-') || strchr (str1,'+')) ) {
		size = atof (*++av);
		ac--;
		}
	    else if (!*(str+1) && strchr (str1,'x') ) {
		frac = atof (*++av+1);
		ac--;
		}
	    break;

	case 'x': /* FITS extensions to read */
	    if (ac < 2)
		usage();
	    extensions = *++av;
	    ac--;
	    break;

	case 'y':	/* output date of plate */
	    printyear++;
	    break;

	case 'z':       /* Use AIPS classic WCS */
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

    /* Check extensions for range and set accordingly */
    if (isrange (extensions)) {
	erange = RangeInit (extensions, nrmax);
	nfext = rgetn (erange);
	extension = calloc (1, 8);
	}
    else {
	extension = extensions;
	if (extension)
	    nfext = 1;
	}

    /* Find number of images to search and leave listfile open for reading */
    if (readlist) {
	if ((flist = fopen (listfile, "r")) == NULL) {
	    fprintf (stderr,"IMSIZE: List file %s cannot be read\n",
		     listfile);
	    usage ();
	    }
	while (fgets (filename, 128, flist) != NULL) {
	    lastchar = filename + strlen (filename) - 1;
	    if (*lastchar < 32) *lastchar = 0;
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
		    if (PrintWCS (namext))
			break;
		    free (namext);
		    }
		}
	    else
		(void) PrintWCS (filename);
	    if (verbose)
		printf ("\n");
	    }
	fclose (flist);
	}

    /* If no arguments left, print usage */
    if (ac == 0)
	usage ();

    while (ac-- > 0) {
	char *fn = *av++;
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
		if (PrintWCS (namext))
		    break;
		free (namext);
		}
	    }
	else
	    (void) PrintWCS (fn);
	if (verbose)
	    printf ("\n");
	}

    return (0);
}

static void
usage ()
{
    fprintf (stderr,"%s\n",RevMsg);
    if (version)
	exit (-1);
    fprintf (stderr,"Print size of image in WCS and pixels\n");
    fprintf (stderr,"Usage: [-vcd] [-p scale] [-b ra dec] [-j ra dec] FITS or IRAF file(s)\n");
    fprintf (stderr,"  -b: Output B1950 (B1950) coordinates (optional center)\n");
    fprintf (stderr,"  -c: Format output without pixel dimensions (optional size change)\n");
    fprintf (stderr,"  -d: Output center in degrees\n");
    fprintf (stderr,"  -e: Add epoch of image to output line\n");
    fprintf (stderr,"  -j: Output J2000 (J2000) coordinates (optional center)\n");
    fprintf (stderr,"  -n: Number of decimal places in output (default 3)\n");
    fprintf (stderr,"  -p: Initial plate scale in arcsec per pixel (default 0)\n");
    fprintf (stderr,"  -r: Print range in RA and Dec\n");
    fprintf (stderr,"  -s: Format output as input to DSS getimage (optional size change)\n");
    fprintf (stderr,"  -v: Verbose\n");
    fprintf (stderr,"  -x range: Print size for these extensions\n");
    fprintf (stderr,"  -y: Add FITS date of image to output line\n");
    fprintf (stderr,"  -z: use AIPS classic projections instead of WCSLIB\n");
    exit (1);
}


static int
PrintWCS (name)

char *name;

{
    char *header;		/* FITS image header */
    int nc;
    char fileroot[64];
    char *filename, *ext, *extn;
    int nax, nch;
    int bp, hp, wp, i, lfroot;
    int sysout;
    double cra, cdec, dra, ddec, secpix;
    double xmin, xmax, ymin, ymax, dx, dy;
    struct WorldCoor *wcs;
    char *namext;
    char *colon;
    char *fd;
    char rstr[32], dstr[32], blanks[64];
    char ramin[32], ramax[32], decmin[32], decmax[32];

    /* Add extension number or name to file if not there already */
    namext = NULL;
    extn = strchr (name, ',');
    if (extension && !extn) {
	nch = strlen (name) + 2 + strlen (extension);
	namext = (char *) calloc (1, nch);
	strcpy (namext, name);
	strcat (namext, ",");
	strcat (namext, extension);
	}
    else {
	nch = strlen (name) + 1;
	namext = (char *) calloc (1, nch);
	strcpy (namext, name);
	}

    /* Find root file name */
    if (strrchr (namext,'/'))
	filename = strrchr (namext,'/') + 1;
    else
	filename = namext;
    ext = strrchr (filename, '.');
    if (ext != NULL)
	nc = ext - filename;
    else
	nc = strlen (filename);
    strncpy (fileroot, filename, nc);
    fileroot[nc] = 0;

    if (verbose) {
	fprintf (stderr,"%s\n",RevMsg);
	fprintf (stderr,"Print World Coordinate System from ");
	if (isiraf (name))
	    fprintf (stderr,"IRAF image file %s\n", name);
	else
	    fprintf (stderr,"FITS image file %s\n", namext);
	}

    if ((header = GetFITShead (namext, verbose)) == NULL)
	return (-1);

    /* Set image dimensions */
    nax = 0;
    if (hgeti4 (header,"NAXIS",&nax) < 1 || nax < 1) {
	if (hgeti4 (header, "WCSAXES", &nax) < 1)
	    return (-1);
	else {
	    if (hgeti4 (header, "IMAGEW", &wp) < 1)
		return (-1);
	    if (hgeti4 (header, "IMAGEH", &hp) < 1)
		return (-1);
	    }
	}
    else {
	if (hgeti4 (header,"NAXIS1",&wp) < 1)
	    return (-1);
	if (hgeti4 (header,"NAXIS2",&hp) < 1)
	    return (-1);
	}
    sysim = 0;
    eqim = 0;
    sysout = sysout0;

    /* Read world coordinate system information from the image header */
    wcs = GetFITSWCS (namext, header, verbose, &cra, &cdec, &dra, &ddec,
		      &secpix, &wp, &hp, &sysim, &eqim);
    if (nowcs (wcs)) {
	hgeti4 (header,"NAXIS1", &wp);
	hgeti4 (header,"NAXIS2", &hp);
	hgeti4 (header,"BITPIX", &bp);
	printf ("%s %d x %d,  %d bits/pixel\n", name, wp, hp, bp);
	wcsfree (wcs);
	wcs = NULL;
	return (0);
	}

    /* Convert to desired output coordinates */
    if (sysout == 0)
	sysout = sysim;
    if (sysim != sysout)
	wcscon (sysim, sysout, eqim, eqout, &cra, &cdec, wcs->epoch);
    if (sysout < 3 && degout == 0) {
	ra2str (rstr, 16, cra, ndec);
	dec2str (dstr, 16, cdec, ndec-1);
	}
    else {
	num2str (rstr, cra, 0, ndec);
	num2str (dstr, cdec, 0, ndec);
	}

    /* Image size in degrees or whatever */
    if (sysim > 5) {
	dra = 2 * dra;
	ddec = 2.0 * ddec;
	}

    /* Image size in arcminutes */
    else if (sysim > 4) {
	dra = 2.0 * dra * 60.0 * cos (degrad(cdec));
	ddec = 2.0 * ddec * 60.0;
	}
    else {
	dra = 2.0 * dra * cos (degrad(cdec));
	ddec = 2.0 * ddec;
	}

    /* Set output coordinate system string */
    if (coorsys[0] == 0)
	wcscstr (coorsys, wcs->syswcs, wcs->equinox, 0.0);
    else
	wcsoutinit (wcs, coorsys);

    /* Convert output width to appropriate units */
    if (sysim > 5) {
	dra = dra;
	ddec = ddec;
	}
    else if (sysim > 4) {
	dra = dra * 3600.0;
	ddec = ddec * 3600.0;
	}
    else if (frac > 0.0) {
	dra = dra * frac;
	ddec = ddec * frac;
	}
    else if (size != 0.0) {
	dra = dra + size;
	ddec = ddec + size;
	}

    /* Print coverage of image in right ascension and declination */
    if (printrange) {
	wcsrange (wcs, &xmin, &xmax, &ymin, &ymax);
	if (sysim < 3 && degout == 0) {
	    ra2str (ramin, 32, xmin, ndec);
	    ra2str (ramax, 32, xmax, ndec);
	    dec2str (decmin, 32, ymin, ndec-1);
	    dec2str (decmax, 32, ymax, ndec-1);
	    dx = wcs->xinc * 3600.0;
	    dy = wcs->yinc * 3600.0;
	    }
	else {
	    num2str (ramin, xmin, 0, ndec);
	    num2str (ramax, xmax, 0, ndec);
	    num2str (decmin, ymin, 0, ndec);
	    num2str (decmax, ymax, 0, ndec);
	    dx = wcs->xinc;
	    dy = wcs->yinc;
	    }
	strcpy (blanks, "                                       ");
	lfroot = strlen (fileroot);
	blanks[lfroot-1] = 0;
	if (sysim < 3) {
	    printf ("%s%s RA:  %s -  %s %.4f arcsec/pix \n",
		    filename, ext, ramin, ramax, dx);
	    printf ("%s Dec: %s - %s %.4f arcsec/pix %s\n",
		    blanks, decmin, decmax, dy, coorsys);
	    }
	else {
	    printf ("%s%s X:  %s -  %s %.4f/pix \n",
		    filename, ext, ramin, ramax, dx);
	    printf ("%s Y: %s - %s %.4f/pix %s\n",
		    blanks, decmin, decmax, dy, coorsys);
	    }
	}

    /* Input for DSS GETIMAGE program */
    else if (dss) {
	for (i = 1; i < 3; i++) {
	    colon = strchr (rstr,':');
	    if (colon)
		*colon = ' ';
	    colon = strchr (dstr,':');
	    if (colon)
		*colon = ' ';
	    }
	printf ("%s%s %s %s ", fileroot, ext, rstr, dstr);
	if (secpix > 0.0)
	    printf (" %.3f %.3f\n", dra, ddec);
	else
	    printf (" 10.0 10.0\n");
	}
    else {
	printf ("%s%s %s %s %s", fileroot, ext, rstr, dstr, coorsys);
	if (secpix > 0.0)
	    if (wcs->sysout < 3 && degout == 0)
		printf (" %.3fmx%.3fm", dra*60.0, ddec*60.0);
	    else {
		num2str (rstr, dra, 0, ndec);
		num2str (dstr, ddec, 0, ndec);
		printf (" %sx%s", rstr, dstr);
		}
	else if (dssc)
	    printf (" 10.000\'x10.000\'");
	if (!dssc) {
	    dx = wcs->xinc * 3600.0;
	    dy = wcs->yinc * 3600.0;
	    if (secpix > 0.0) {
		if (dx == dy)
		    printf (" %.4fs/pix", secpix);
		else
		    printf (" %.4f/%.4fs/pix", dx, dy);
		}
	    printf ("  %dx%d pix", wp, hp);
	    }
	if (printepoch)
	    printf (" %.5f",wcs->epoch);
	if (printyear) {
	    fd = ep2fd (wcs->epoch);
	    printf (" %s", fd);
	    }
	printf ("\n");
	}

    wcsfree (wcs);
    wcs = NULL;
    free (header);
    return (0);
}
/* Jul  9 1996	New program
 * Jul 18 1996	Update header reading
 * Jul 19 1996	Add option to change coordinate system
 * Aug  8 1996	Fix coordinate change option
 * Aug  9 1996	Add file name to output string
 * Aug 15 1996	Clean up image header reading code
 * Aug 27 1996	Add output format for DSS getimage
 * Aug 28 1996	Allow size increase or decrease if DSS-format output
 * Sep  3 1996	Add option to print DSS format with colons
 * Oct 16 1996  Rewrite to allow optional new center and use GetWCSFITS
 * Oct 17 1996	Do not print angular size and scale if not set
 * Oct 30 1996	Make equinox default to J2000 if not in image header
 * Dec 10 1996	Change equinox in getfitswcs call to double
 *
 * Feb 21 1997  Add optional epoch of image to output
 * Feb 24 1997  Read header using GetFITShead()
 * May 28 1997  Add option to read a list of filenames from a file
 * Sep  8 1997	Add option to change size by fraction of size or constant
 * Oct 14 1997	Add option to print RA and Dec range
 * Nov 13 1997	Print both plate scales if they are different
 * Dec 15 1997	Read IRAF 2.11 image format; fix bug if no WCS
 *
 * Jan 27 1998  Implement Mark Calabretta's WCSLIB
 * Jan 29 1998  Add -z for AIPS classic WCS projections
 * Feb 18 1998	Version 2.0: Full Calabretta WCS
 * Apr 24 1998	change coordinate setting to setsys() from setfk4()
 * Apr 28 1998	Change coordinate system flags to WCS_*
 * May 27 1998	Include fitsio.h instead of fitshead.h
 * Jun 24 1998	Add string lengths to ra2str() and dec2str() calls
 * Jun 25 1998	Set WCS subroutine choice with SETDEFWCS()
 * Jul 24 1998	Drop unused variable irafheader
 * Aug  6 1998	Change fitsio.h to fitsfile.h
 * Sep 17 1998	Add coordinate system to GetFITSWCS()
 * Sep 17 1998	Set coordinate system string using wcscstr()
 * Sep 29 1998	Change call to GetFITSWCS()
 * Oct 14 1998	Use isiraf() to determine file type
 * Nov 30 1998	Add version and help commands for consistency
 *
 * Apr  7 1999	Add file name argument to GetFITSWCS
 * Jun 17 1999	Fix coordinate conversion
 * Oct 15 1999	Free wcs using wcsfree()
 * Oct 22 1999	Drop unused variables after lint
 *
 * Jan 28 2000	Call setdefwcs() with WCS_ALT instead of 1
 * Feb 15 2000	Print size of image if no WCS
 * Aug 14 2000	Reformat for LINEAR and other non-angular coordinates
 * Aug 15 2000	Add -n option to set number of decimal places
 * Sep 14 2000	Print size in arcminutes if WCS is sky
 *
 * Apr  8 2002	Free wcs structure if no WCS is found in file header
 * Apr  8 2002	Fix bug so list files work
 * Jun  6 2002	Add -x option to print sizes fore multiple file extensions
 * Jun 18 2002	Make coorsys 8 instead of 4, fixing bug
 * Jun 18 2002	Use extn for image extension, ext for filename extension
 * Jun 19 2002	Add verbose argument to GetFITShead()
 * Oct  3 2002	Initialize uninitialized switch nfext
 *
 * Apr 11 2003	Add -d option for degree center
 * Dec 18 2003	Print decimal degrees for longitude/latitude output
 *
 * Jan 12 2005	Check for uppercase filename extensions
 * Jul 20 2005	Make -d and -n options work for both center and dimensions
 * Sep 13 2005	Fix inline documentation to match reality
 *
 * Jun 21 2006	Clean up code
 * Sep 26 2006	Increase length of rastr and destr from 16 to 32
 *
 * Jan 10 2007	Declare RevMsg static, not const
 * Dec 21 2007	Add option to put range of extensions in filenames
 * 
 * May 23 2008	Add y option to print FITS ISO format date of image
 * May 23 2008	Drop quotes from output: use m and s instead
 *
 * Mar 24 2009	Set dimensions from IMAGEW and IMAGEH if WCSAXES > 0
 */
