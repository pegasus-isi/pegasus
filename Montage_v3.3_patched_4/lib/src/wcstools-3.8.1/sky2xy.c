/* File sky2xy.c
 * September 25, 2009
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
#include "libwcs/wcs.h"
#include "libwcs/fitsfile.h"

static void PrintUsage();
extern void setrot(),setsys(),setcenter(),setsecpix(),setrefpix(),setdateobs();
extern void setnpix();
extern struct WorldCoor *GetFITSWCS ();	/* Read WCS from FITS or IRAF header */
extern struct WorldCoor *GetWCSFITS ();	/* Read WCS from FITS or IRAF file */
extern char *GetFITShead();
static int version = 0;		/* If 1, print only program name and version */

static char *RevMsg = "SKY2XY WCSTools 3.8.1, 14 December 2009, Doug Mink (dmink@cfa.harvard.edu)";

int
main (ac, av)
int ac;
char **av;
{
    int verbose = 0;		/* verbose/debugging flag */
    char *str;
    double x, y, ra, dec, ra0, dec0;
    FILE *fd;
    char *ln, *listname;
    char line[80];
    char *fn;
    char csys[16];
    int sysin;
    struct WorldCoor *wcs;
    char rastr[32], decstr[32];
    char xstr[16], ystr[16];
    char *str1;
    int ic;
    int offscale, n;
    int nx, ny, lhead, i, nf;
    int bitpix = 0;
    char *header;
    double cra, cdec, dra, ddec, secpix, drot;
    double eqout = 0.0;
    double eqin = 0.0;
    int sysout = 0;
    int syscoor = 0;
    int degout = 0;
    int wp, hp;
    char coorsys[16];
    int ndec = 3;		/* Number of decimal places in output coords */
    char printonly = 'b';
    int modwcs = 0;		/* 1 if image WCS modified on command line */

    wcs = NULL;

    /* Check for help or version command first */
    str = *(av+1);
    if (!str || !strcmp (str, "help") || !strcmp (str, "-help"))
	PrintUsage(str);
    if (!strcmp (str, "version") || !strcmp (str, "-version")) {
	version = 1;
	PrintUsage(str);
	}

    *coorsys = 0;

    /* Decode arguments */
    for (av++; --ac > 0 && *(str = *av) == '-'; av++) {
	char c;
	while ((c = *++str))
    	switch (c) {

    	    case 'v':	/* more verbosity */
    		verbose++;
    		break;

	    case 'a':       /* Initial rotation angle in degrees */
		if (ac < 2)
		    PrintUsage (str);
		drot = atof (*++av);
                setrot (drot);
		modwcs = 1;
                ac--;
                break;

	    case 'b':	/* image reference coordinates on command line in B1950 */
		str1 = *(av+1);
		ic = (int)str1[0];
		if (*(str+1) || ic < 48 || ic > 58) {
		    strcpy (coorsys,"B1950");
		    }
		else if (ac < 3)
		    PrintUsage (str);
		else {
		    setsys(WCS_B1950);
		    sysout = WCS_B1950;
		    syscoor = WCS_B1950;
		    eqout = 1950.0;
		    if (strlen (*++av) < 32)
			strcpy (rastr, *av);
		    else {
			strncpy (rastr, *av, 31);
			rastr[31] = (char) 0;
			}
		    ac--;
		    if (strlen (*++av) < 32)
			strcpy (decstr, *av);
		    else {
			strncpy (decstr, *av, 31);
			decstr[31] = (char) 0;
			}
		    ac--;
		    setcenter (rastr, decstr);
		    modwcs = 1;
		    }
    		break;

	    case 'e':
		str1 = *(av+1);
		ic = (int)str1[0];
		if (*(str+1) || ic < 48 || ic > 58) {
		    setsys (WCS_ECLIPTIC);
		    sysout = WCS_ECLIPTIC;
		    syscoor = WCS_ECLIPTIC;
		    eqout = 2000.0;
		    }
		else if (ac < 3)
		    PrintUsage (str);
		else {
		    setsys (WCS_ECLIPTIC);
		    sysout = WCS_ECLIPTIC;
		    syscoor = WCS_ECLIPTIC;
		    eqout = 2000.0;
		    strcpy (rastr, *++av);
		    ac--;
		    strcpy (decstr, *++av);
		    ac--;
		    setcenter (rastr, decstr);
		    modwcs = 1;
		    }
        	degout++;
		strcpy (coorsys,"ecliptic");
    		break;

	    case 'g':
		str1 = *(av+1);
		ic = (int)str1[0];
		if (*(str+1) || ic < 48 || ic > 58) {
		    setsys (WCS_GALACTIC);
		    sysout = WCS_GALACTIC;
		    syscoor = WCS_GALACTIC;
		    eqout = 2000.0;
		    }
		else if (ac < 3)
		    PrintUsage (str);
		else {
		    setsys (WCS_GALACTIC);
		    sysout = WCS_GALACTIC;
		    syscoor = WCS_GALACTIC;
		    eqout = 2000.0;
		    strcpy (rastr, *++av);
		    ac--;
		    strcpy (decstr, *++av);
		    ac--;
		    setcenter (rastr, decstr);
		    modwcs = 1;
		    }
        	degout++;
		strcpy (coorsys,"galactic");
    		break;

	    case 'j':	/* image reference coordinates on command line in J2000 */
		str1 = *(av+1);
		ic = (int)str1[0];
		if (*(str+1) || ic < 48 || ic > 58) {
		    strcpy (coorsys,"J2000");
		    }
		else if (ac < 3)
		    PrintUsage (str);
		else {
		    setsys(WCS_J2000);
		    sysout = WCS_J2000;
		    syscoor = WCS_J2000;
		    eqout = 2000.0;
		    if (strlen (*++av) < 32)
			strcpy (rastr, *av);
		    else
			strncpy (rastr, *av, 31);
		    rastr[31] = (char) 0;
		    ac--;
		    if (strlen (*++av) < 32)
			strcpy (decstr, *av);
		    else
			strncpy (decstr, *av, 31);
		    decstr[31] = (char) 0;
		    ac--;
		    setcenter (rastr, decstr);
		    modwcs = 1;
		    }
    		break;

	    case 'n':	/* Number of decimal places in output coordsinates */
		if (ac < 2)
		    PrintUsage (str);
		ndec = (int) atof (*++av);
		ac--;
		break;

	    case 'o':	/* Output only the following part of the coordinates */
		if (ac < 2)
		    PrintUsage (str);
		av++;
		printonly = **av;
		ac--;
		break;

	    case 'p':	/* Initial plate scale in arcseconds per pixel */
		if (ac < 2)
		    PrintUsage (str);
		setsecpix (atof (*++av));
		modwcs = 1;
		ac--;
		break;
	
	    case 's':	/* Size of image in X and Y pixels */
		if (ac < 3)
		    PrintUsage(str);
		nx = atoi (*++av);
		ac--;
		ny = atoi (*++av);
		ac--;
		setnpix (nx, ny);
		modwcs = 1;
    		break;

	    case 'x':	/* X and Y coordinates of reference pixel */
		if (ac < 3)
		    PrintUsage (str);
		x = atof (*++av);
		ac--;
		y = atof (*++av);
		ac--;
		setrefpix (x, y);
		modwcs = 1;
		break;

	    case 'y':	/* Epoch of image in FITS date format */
		if (ac < 2)
		    PrintUsage (str);
		setdateobs (*++av);
		ac--;
		modwcs = 1;
		break;

	    case 'z':       /* Use AIPS classic WCS */
		setdefwcs (WCS_ALT);
		break;

    	    default:
    		PrintUsage(str);
    		break;
    	    }
	}

    /* There are ac remaining file names starting at av[0] */
    if (ac == 0)
	PrintUsage (str);

    /* Read filename, if any */
    fn = *av;
    if (!isnum (*av)) {
	fn = *av;
	av++;
	if (verbose)
	    printf ("%s:\n", fn);
	header = GetFITShead (fn, verbose);
	wcs = GetFITSWCS (fn,header,verbose,&cra,&cdec,&dra,&ddec,&secpix,
			  &wp, &hp, &sysout, &eqout);
	if (nowcs (wcs)) {
	    fprintf (stderr, "No WCS in image file %s\n", fn);
	    wcsfree (wcs);
	    free (header);
	    exit (1);
	    }
	}
    else {
	fn = NULL;
	lhead = 14400;
	header = (char *) calloc (1, lhead);
	strcpy (header, "END ");
	for (i = 4; i < lhead; i++)
            header[i] = ' ';
	hlength (header, 14400);
	hputl (header, "SIMPLE", 1);
	hputi4 (header, "BITPIX", bitpix);
	hputi4 (header, "NAXIS", 2);
	hputi4 (header, "NAXIS1", 100);
	hputi4 (header, "NAXIS2", 100);
	wcs = GetFITSWCS (fn,header,verbose,&cra,&cdec,&dra,&ddec,&secpix,
			  &wp, &hp, &sysout, &eqout);
	if (nowcs (wcs)) {
	    fprintf (stderr, "Incomplete WCS on command line\n");
	    wcsfree (wcs);
	    free (header);
	    exit (1);
	    }
	}

    /* Set size of image coordinate field */
    if (wcs->nxpix < 10 && wcs->nypix < 10)
	nf = 2 + ndec;
    else if (wcs->nxpix < 100 && wcs->nypix < 100)
	nf = 3 + ndec;
    else if (wcs->nxpix < 1000 && wcs->nypix < 1000)
	nf = 4 + ndec;
    else if (wcs->nxpix < 10000 && wcs->nypix < 10000)
	nf = 5 + ndec;
    else
	nf = 6 + ndec;

    if (degout)
	wcs->degout = degout;

    while (ac-- > 1) {
	listname = *av;
	if (listname[0] == '@') {
	    if (*coorsys)
		wcsininit (wcs, coorsys);
	    ln = listname;
	    while (*ln++)
		*(ln-1) = *ln;
	    if ((fd = fopen (listname, "r"))) {
		while (fgets (line, 80, fd)) {
		    csys[0] = (char) 0;
		    n = sscanf (line,"%s %s %s", rastr, decstr, csys);
		    ra = str2ra (rastr);
		    dec = str2dec (decstr);
		    if (n > 2)
			sysin = wcscsys (csys);
		    else if (*coorsys) {
			sysin = wcscsys (coorsys);
			strcpy (csys, coorsys);
			}
		    else {
			sysin = wcs->sysin;
			strcpy (csys, wcs->radecin);
			}
		    wcsc2pix (wcs, ra, dec, csys, &x, &y, &offscale);
		    num2str (xstr, x, nf, ndec);
		    num2str (ystr, y, nf, ndec);

		    if (verbose)
			printf ("%s %s %s -> %.5f %.5f -> %s %s",
				 rastr, decstr, csys, ra, dec, xstr, ystr);
		    else
			printf ("%s %s %s -> %s %s",
				rastr, decstr, csys, xstr, ystr);
		    if (offscale == 2)
			printf (" (off image)\n");
		    else if (offscale)
			printf (" (offscale)\n");
		    else
			printf ("\n");
		    }
		}
	    else {
		fprintf (stderr, "Cannot read file %s\n", listname);
		exit (1);
		}
	    av++;
	    }
	else if (ac > 1) {
	    strcpy (rastr, *av);
	    ac--;
	    av++;
	    strcpy (decstr, *av);
	    ra0 = str2ra (rastr);
	    dec0 = str2dec (decstr);
	    ra = ra0;
	    dec = dec0;
	    av++;

	/* Convert coordinates system to that of image */
	    if (ac > 1 && wcscsys (*av) > -1) {
		strcpy (csys, *av);
		ac--;
		av++;
		}
	    else if (*coorsys)
		strcpy (csys, coorsys);
	    else {
		if (wcs->prjcode < 0)
		    strcpy (csys, "PIXEL");
		else if (wcs->prjcode < 2)
		    strcpy (csys, "LINEAR");
		else
		    strcpy (csys, wcs->radecsys);
		}

	    sysin = wcscsys (csys);
	    eqin = wcsceq (csys);
	    if (wcs->syswcs > 0 && wcs->syswcs != 6 && wcs->syswcs != 10)
		wcscon (sysin, wcs->syswcs, eqin, eqout, &ra, &dec, wcs->epoch);
	    if (sysin != wcs->syswcs && verbose) {
		printf ("%s %s %s -> ", rastr, decstr, csys);
		ra2str (rastr, 32, ra, ndec);
		dec2str (decstr, 32, dec, ndec-1);
		printf ("%s %s %s\n", rastr, decstr, wcs->radecsys);
		}
	    wcsc2pix (wcs, ra0, dec0, csys, &x, &y, &offscale);
	    num2str (xstr, x, nf, ndec);
	    num2str (ystr, y, nf, ndec);
	    if (printonly == 'x')
		printf ("%s", xstr);
	    else if (printonly == 'y')
		printf ("%s", ystr);
	    else
		printf ("%s %s %s -> %s %s",rastr, decstr, csys, xstr, ystr);
	    if (wcs->wcsl.cubeface > -1)
		printf (" %d", wcszout (wcs));
	    if (offscale == 2)
		printf (" (off image)\n");
	    else if (offscale)
		printf (" (offscale)\n");
	    else
		printf ("\n");
	    }
	}

    wcsfree (wcs);
    return (0);
}

static void
PrintUsage (command)
char	*command;
{
    fprintf (stderr,"%s\n",RevMsg);
    if (version)
	exit (-1);
    if (command != NULL) {
	if (command[0] == '*')
	    fprintf (stderr, "%s\n", command);
	else
	    fprintf (stderr, "* Missing argument for command: %c\n", command[0]);
	exit (1);
	}
    fprintf (stderr,"Compute X Y from RA Dec using WCS in FITS and IRAF image files\n");
    fprintf (stderr,"Usage: [-vbjg] file.fts ra1 dec1 sys1 ... ran decn sysn\n");
    fprintf (stderr,"  or : [-vbjg] file.fts @listfile\n");
    fprintf (stderr,"  -a rot: rotation angle (counterclockwise degrees)\n");
    fprintf (stderr,"  -b [ra dec]: input in B1950 [image reference sky position]\n");
    fprintf (stderr,"  -e [long lat]: input in ecliptic [image reference sky position]\n");
    fprintf (stderr,"  -g [long lat]: input in galactic [image reference sky position]\n");
    fprintf (stderr,"  -j [ra dec]: input in J2000 [image reference sky position]\n");
    fprintf (stderr,"  -n num: number of decimal places in output\n");
    fprintf (stderr,"  -o x|y: print only x or y coordinate\n");
    fprintf (stderr,"  -p scale: plate scale in arcsec/pixel\n");
    fprintf (stderr,"  -s nx ny: size of image in pixels\n");
    fprintf (stderr,"  -x x y: reference image position in pixels\n");
    fprintf (stderr,"  -v: verbose\n");
    fprintf (stderr,"  -y date: Epoch as fractional year or FITS date\n");
    fprintf (stderr,"  -z: use AIPS classic projections instead of WCSLIB\n");
    fprintf (stderr,"These flags are best used for files of coordinates in the same system:\n");
    fprintf (stderr,"  -b: B1950 (FK4) input\n");
    fprintf (stderr,"  -e: ecliptic longitude and latitude input\n");
    fprintf (stderr,"  -j: J2000 (FK5) input\n");
    fprintf (stderr,"  -g: galactic longitude and latitude input\n");
    exit (1);
}
/* Feb 23 1996	New program
 * Apr 24 1996	Version 1.1: Add B1950, J2000, or galactic coordinate input options
 * Jun 10 1996	Change name of WCS subroutine
 * Aug 27 1996	Clean up code after lint
 * Oct 29 1996	Allow alternate coordinate systems for input coordinates
 * Oct 30 1996	Exit if image file is not found
 * Nov  1 1996	Fix bug so systemless coordinates do not cause crash
 * Nov  5 1996	Fix multiple sets of coordinates on command line
 *
 * Jun  4 1997	Add PIXEL wcs for linear non-sky projections
 * Nov  4 1997	If verbose mode, always print converted input string
 * Dec 15 1997	Handle new IRAF 2.11 image header format
 *
 * Jan 28 1998  Implement Mark Calabretta's WCSLIB
 * Jan 29 1998  Add -z for AIPS classic WCS projections
 * Feb 17 1998	Add support for galactic coordinates as input
 * Feb 18 1998	Version 2.0: Full Calabretta implementation
 * Mar 27 1998	Version 2.2: Add TNX and polynomial plate fit
 * Apr 13 1998	Compute pixel from galactic coordinates correctly
 * Apr 14 1998	Add ecliptic coordinates
 * Apr 24 1998	Handle linear coodinates
 * Apr 28 1998	Implement separate coordinate system for input
 * May 13 1998	Implement arbitrary equinox for input
 * May 27 1998	Include fitsio.h instead of fitshead.h
 * Jun 24 1998	Add string lengths to ra2str() and dec2str() calls
 * Jun 25 1998	Set WCS subroutine choice with SETDEFWCS()
 * Jul 16 1998	Print face if cube face is returned
 * Aug  6 1998	Change fitsio.h to fitsfile.h
 * Nov 30 1998	Add version and help commands for consistency
 *
 * Mar 17 1999	Add flag for positions off image but within projection
 * Oct 14 1999	Use command line coordinate flag if system not in coordinates
 * Oct 15 1999	Free wcs using wcsfree()
 * Oct 22 1999	Link included files to libwcs
 *
 * Jan 28 2000	Call setdefwcs() with WCS_ALT instead of 1
 *
 * Apr  8 2002	Free wcs structure if no WCS is found in file header
 * Jun 19 2002	Add verbose argument to GetWCSFITS()
 *
 * Apr  4 2003	Add command line WCS setting and number of decimal places
 * Apr  7 2003	Add -o option to print only x or y coordinate
 * Apr 24 2003	Initialize FITS header completely if needed
 * Jul 22 2003	Initialize sysout; move most static variables into main()
 * Jul 22 2003	(bug found and fix suggested by Takehiko Wada, ISAS)
 *
 * Aug 30 2004	Declare undeclared void subroutines
 *
 * Jan 20 2005	Set WCS directly from header if no command line changes
 *
 * Apr 19 2006	Use -n number of decimal places when reading from file, too
 * Jun 21 2006	Clean up code
 * Sep 26 2006	Allow coordinates on command line to be any length
 * Oct 30 2006	Do not precess LINEAR or XY coordinates
 *
 * Jul  5 2007	Parse command line arguments to initialize a WCS without a file
 * Jul  5 2007	Use command line argument if no system with coordinates
 *
 * Sep 25 2009	Declare setnpix()
 */
