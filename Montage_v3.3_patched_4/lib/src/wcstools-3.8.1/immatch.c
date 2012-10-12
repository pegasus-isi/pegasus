/* File immatch.c
 * April 6, 2007
 * By Doug Mink, after Elwood Downey
 * (Harvard-Smithsonian Center for Astrophysics)
 * Send bug reports to dmink@cfa.harvard.edu

   Copyright (C) 1997-2007 
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
#include "libwcs/lwcs.h"

static void PrintUsage();
static void MatchCat();

#define MAXFILES 1000
static int maxnfile = MAXFILES;

static char *RevMsg = "WCSTools 3.8.1, 14 December 2009, Doug Mink (dmink@cfa.harvard.edu)";

static int verbose = 0;		/* verbose/debugging flag */
static int rot = 0;
static int mirror = 0;
static int bitpix = 0;
static int imsearch = 1;	/* set to 0 if image catalog provided */
static int rotatewcs = 1;	/* If 1, rotate FITS WCS keywords in image */
static char *refcatname;	/* Name of reference catalog to match */
static int version = 0;		/* If 1, print only program name and version */
static char *progname;		/* Name of program as executed */

extern char *RotFITS();
extern int SetWCSFITS();
extern int PrintWCS();
extern void settolerance();
extern void setreflim();
extern void setrot();
extern void setnfit();
extern void setsecpix();
extern void setcenter();
extern void setsys();
extern void setminb();
extern void setmaxcat();
extern void setstarsig();
extern void setgsclass();
extern void setuplate();
extern void setimcat();
extern void setbmin();
extern void setfrac();
extern void setrefpix();
extern void setwcsproj();
extern void setfitwcs();
extern void setirafout();
extern void setmagfit();
extern void setimfrac();
extern void setsortmag();
extern void setparm();
extern void setmirror();
extern void setrotate();
extern void setrevmsg();

int
main (ac, av)
int ac;
char **av;
{
    char *str;
    double bmin, maglim1, maglim2, arot, drot;
    char rastr[32];
    char decstr[32];
    int readlist = 0;
    char *lastchar;
    char filename[128];
    char errmsg[256];
    FILE *flist;
    char **fn;
    char *listfile = NULL;
    double x, y;
    int imag;
    int ifile, nfile;
    char c, c1, *ccom;

    setfitwcs (0);
    nfile = 0;
    fn = (char **)calloc (maxnfile, sizeof(char *));
    setrevmsg (RevMsg);

    /* Check name used to execute programe and set catalog name accordingly */
    progname = ProgName (av[0]);
    refcatname = ProgCat (progname);

    if (ac == 1)
	PrintUsage (NULL);

    /* Loop through the arguments */
    for (av++; --ac > 0; av++) {
	str = *av;

	/* Check for help command first */
	if (!str || !strcmp (str, "help") || !strcmp (str, "-help"))
	    PrintUsage (NULL);

	/* Check for version command */
	else if (!strcmp (str, "version") || !strcmp (str, "-version")) {
	    version = 1;
	    PrintUsage (NULL);
	    }

	else if (strchr (str, '='))
	    setparm (str);

	/* Image list file */
	else if (str[0] == '@') {
	    readlist++;
	    listfile = ++str;
	    str = str + strlen (str) - 1;
	    av++;
	    ac--;
	    }

	/* Decode arguments */
	else if (str[0] == '-') {

	    /* Loop through possibly combined arguments */
	    while ((c = *++str) != 0) {
		switch (c) {
		case 'a':	/* Initial rotation angle in degrees */
		    if (ac < 2)
			PrintUsage (str);
		    drot = atof (*++av);
		    arot = fabs (drot);
		    if (arot != 90.0 && arot != 180.0 && arot != 270.0) {
			setrot (rot);
			rot = 0;
			}
		    else
			rot = atoi (*av);
		    ac--;
		    break;

		case 'b':	/* initial coordinates on command line in B1950 */
		    if (ac < 3)
			PrintUsage (str);
		    setsys (WCS_B1950);
		    strcpy (rastr, *++av);
		    ac--;
		    strcpy (decstr, *++av);
		    ac--;
		    setcenter (rastr, decstr);
		    break;

		case 'c':       /* Set reference catalog */
		    if (ac < 2)
			PrintUsage (str);
		    refcatname = *++av;
		    ac--;
		    break;

		case 'd':	/* Read image star positions from DAOFIND file */
		    if (ac < 2)
			PrintUsage (str);
		    setimcat (*++av);
		    imsearch = 0;
		    ac--;
		    break;

		case 'e':	/* Set WCS projection
		    if (ac < 2)
			PrintUsage (str);
		    setwcsproj (*++av);
		    ac--;
		    break; */

		case 'f':	/* Set IRAF output format */
		    setirafout();
		    break;

		case 'g':	/* Guide Star object class */
		    if (ac < 2)
			PrintUsage (str);
		    setgsclass ((int) atof (*++av));
		    ac--;
		    break;

		case 'h':	/* Maximum number of reference stars */
		    if (ac < 2)
			PrintUsage (str);
		    setmaxcat ((int) atof (*++av));
		    ac--;
		    break;

		case 'i':       /* Image star minimum peak value */
		    if (ac < 2)
			PrintUsage (str);
		    bmin = atof (*++av);
		    if (bmin < 0)
			setstarsig (-bmin);
		    else
			setbmin (bmin);
		    ac--;
		    break;

		case 'j':	/* center coordinates on command line in J2000 */
		    if (ac < 3)
			PrintUsage (str);
		    setsys (WCS_J2000);
		    strcpy (rastr, *++av);
		    ac--;
		    strcpy (decstr, *++av);
		    ac--;
		    setcenter (rastr, decstr);
		    break;

		case 'k':	/* select magnitude to use from reference catalog */
		    if (ac < 2)
			PrintUsage (str);
		    av++;
		    c1 = (*av)[0];
		    if (c1 > '9')
			imag = (int) c1;
		    else
			imag = (int) c1 - 48;
		    setsortmag (imag);
		    ac--;
		    break;

 		case 'l':	/* Left-right reflection before rotating */
		    mirror = 1;
		    setmirror (mirror);
		    break;

		case 'm':	/* Limiting reference star magnitude */
		    if (ac < 2)
			PrintUsage (str);

		    /* Select reference catalog magnitude to use */
		    c1 = *(str+1);
		    if (c1 != (char) 0) {
			++str;
			if (c1 > '9')
			    imag = (int) c1;
			else
			    imag = (int) c1 - 48;
			setsortmag (imag);
			}
		    ac--;
		    av++;

		    if ((ccom = strchr (*av, ',')) != NULL) {
			*ccom = (char) 0;
			maglim1 = atof (*av);
			maglim2 = atof (ccom+1);
			}
		    else {
			maglim2 = atof (*av);
			if (ac > 1 && isnum (*(av+1))) {
			    av++;
			    ac--;
			    maglim1 = maglim2;
			    maglim2 = atof (*av);
			    }
			else if (MAGLIM1 == MAGLIM2)
			    maglim1 = -2.0;
			}
		    setreflim (maglim1, maglim2);
		    break;

		case 'p':	/* Initial plate scale in arcseconds per pixel */
		    if (ac < 2)
			PrintUsage (str);
		    setsecpix (atof (*++av));
		    ac--;
		    break;

		case 'q':	/* Set image to catalog magnitude fit */
		    setmagfit();
		    break;

		case 'r':	/* Angle in degrees to rotate before fitting */
		    if (ac < 2)
			PrintUsage (str);
		    rot = (int) atof (*++av);
		    setrotate (rot);
		    ac--;
		    break;

		case 's':   /* This fraction more image stars than reference stars or vice versa */
		    if (ac < 2)
			PrintUsage (str);
		    setfrac (atof (*++av));
		    ac--;
		    break;

		case 't':	/* +/- this many pixels is a hit */
		    if (ac < 2)
			PrintUsage (str);
		    settolerance (atof (*++av));
		    ac--;
		    break;

		case 'u':	/* UJ Catalog plate number */
		    if (ac < 2)
			PrintUsage (str);
		    setuplate ((int) atof (*++av));
		    ac--;
		    break;

		case 'v':	/* more verbosity */
		    verbose++;
		    break;

		case 'w':	/* Do not rotate image WCS with image */
		    rotatewcs = 0;
		    break;

		case 'x':	/* X and Y coordinates of reference pixel */
		    if (ac < 3)
			PrintUsage (str);
		    x = atof (*++av);
		    ac--;
		    y = atof (*++av);
		    ac--;
		    setrefpix (x, y);
		    break;

		case 'y':	/* Multiply dimensions of image by fraction */
		    if (ac < 2)
			PrintUsage (str);
		    setimfrac (atof (*++av));
		    break;

		case 'z':       /* Use AIPS classic WCS */
		    setdefwcs (WCS_ALT);
		    break;

		default:
		    sprintf (errmsg, "* Illegal command -%s-", str);
		    PrintUsage (errmsg);
		    break;
		}
		}
	    }

	/* Image file */
	else if (isfits (str) || isiraf (str)) {
	    if (nfile >= maxnfile) {
		maxnfile = maxnfile * 2;
		fn = (char **) realloc ((void *)fn, maxnfile);
		}
	    fn[nfile] = str;
	    nfile++;
	    }

	else {
	    sprintf (errmsg, "* %s is not a FITS or IRAF file.", str);
    	    PrintUsage (errmsg);
	    }
	}

    /* If reference catalog is not set, exit with an error message */
    if (refcatname == NULL) {
	PrintUsage ("* Must specifiy a reference catalog using -c or alias.");
	}

    /* Process image files from list file */
    if (readlist) {
	if ((flist = fopen (listfile, "r")) == NULL) {
	    sprintf (errmsg,"* List file %s cannot be read\n", listfile);
	    PrintUsage (errmsg);
	    }
	while (fgets (filename, 128, flist) != NULL) {
	    lastchar = filename + strlen (filename) - 1;
	    if (*lastchar < 32) *lastchar = 0;
	    MatchCat (progname, filename);
	    if (verbose)
		printf ("\n");
	    }
	fclose (flist);
	}

    /* Process image files */
    else if (nfile > 0) {
	for (ifile = 0; ifile < nfile; ifile++) {
	    if ( verbose)
    		printf ("%s:\n", fn[ifile]);
	    MatchCat (progname, fn[ifile]);
	    if (verbose)
		printf ("\n");
	    }
	}

    /* Print error message if no image files to process */
    else {
	PrintUsage ("* No files to process.");
	return (0);
	}

    return (0);
}

static void
PrintUsage (command)

char	*command;		/* Name of program being executed */

{
    fprintf (stderr,"%s %s\n", progname, RevMsg);
    if (version)
	exit (-1);

    if (command != NULL) {
	if (command[0] == '*')
	    fprintf (stderr, "%s\n", command);
	else
	    fprintf (stderr, "* Missing argument for command: %c\n", command[0]);
	exit (1);
	}

    if (strsrch (progname,"gsc") != NULL)
	fprintf (stderr,"Match HST Guide Star Catalog to image stars from WCS in image file\n");
    else if (strsrch (progname,"gsca") != NULL)
	fprintf (stderr,"Match GSC-ACT Catalog to image stars from WCS in image file\n");
    else if (strsrch (progname,"tmc") != NULL ||
	     strsrch (progname,"2mp") != NULL)
	fprintf (stderr,"Match 2MASS Point Sources to image stars from WCS in image file\n");
    else if (strsrch (progname,"ujc") != NULL)
	fprintf (stderr,"Match USNO J Catalog to image stars from WCS in image file\n");
    else if (strsrch (progname,"uac") != NULL)
	fprintf (stderr,"Match USNO A Catalog to image stars from WCS in image file\n");
    else if (strsrch (progname,"ua1") != NULL)
	fprintf (stderr,"Match USNO A1.0 Catalog to image stars from WCS in image file\n");
    else if (strsrch (progname,"ua2") != NULL)
	fprintf (stderr,"Match USNO A2.0 Catalog to image stars from WCS in image file\n");
    else if (strsrch (progname,"usac") != NULL)
	fprintf (stderr,"Match USNO SA Catalog to image stars from WCS in image file\n");
    else if (strsrch (progname,"usa1") != NULL)
	fprintf (stderr,"Match USNO SA1.0 Catalog to image stars from WCS in image file\n");
    else if (strsrch (progname,"usa2") != NULL)
	fprintf (stderr,"Match USNO SA2.0 Catalog to image stars from WCS in image file\n");
    else if (strsrch (progname,"ub1") != NULL)
	fprintf (stderr,"Match USNO B1.0 Catalog to image stars from WCS in image file\n");
    else if (strsrch (progname,"act") != NULL)
	fprintf (stderr,"Match USNO ACT Catalog to image stars from WCS in image file\n");
    else if (strsrch (progname,"bsc") != NULL)
	fprintf (stderr,"Match Yale Bright Star Catalog to image stars from WCS in image file\n");
    else if (strsrch (progname,"iras") != NULL)
	fprintf (stderr,"Match IRAS Point Source Catalog to image stars from WCS in image file\n");
    else if (strsrch (progname,"sao") != NULL)
	fprintf (stderr,"Match SAO Catalog to image stars from WCS in image file\n");
    else if (strsrch (progname,"ppm") != NULL)
	fprintf (stderr,"Match PPM Catalog to image stars from WCS in image file\n");
    else if (strsrch (progname,"tycho") != NULL)
	fprintf (stderr,"Match Tycho Catalog to image stars from WCS in image file\n");
    else
	fprintf (stderr,"Match catalog to image stars from WCS in image file\n");
    fprintf(stderr,"Usage: [-vl] [-m mag] [-n frac] [-s mode] [-g class] [-h maxref] [-i peak]\n");
    fprintf(stderr,"       [-c catalog] [-p scale] [-b ra dec] [-j ra dec] [-r deg] [-t tol] [-x x y] [-y frac]\n");
    fprintf(stderr,"       FITS or IRAF file(s)\n");
    fprintf(stderr,"  -a ang: initial rotation angle in degrees (default 0)\n");
    fprintf(stderr,"  -b: initial center in B1950 (FK4) RA and Dec\n");
    fprintf(stderr,"  -c cat: reference catalog (gsc, uac, ujc, tab table file\n");
    fprintf(stderr,"  -d cat: Use following DAOFIND output catalog instead of search\n");
    /* fprintf(stderr,"  -e type: WCS type (TAN default)\n"); */
    fprintf(stderr,"  -f: Write output X Y RA Dec, instead of N RA Dec X Y\n");
    fprintf(stderr,"  -g num: Guide Star Catalog class (-1=all,0,3 (default -1)\n");
    fprintf(stderr,"  -h num: maximum number of reference stars to use (10-200, default 25\n");
    fprintf(stderr,"  -i num: minimum peak value for star in image (<0=-sigma)\n");
    fprintf(stderr,"  -j: initial center in J2000 (FK5) RA and Dec\n");
    fprintf(stderr,"  -k: magnitude to use (1 to nmag)\n");
    fprintf(stderr,"  -l: reflect left<->right before rotating and fitting\n");
    fprintf(stderr,"  -mx m1[,m2]: initial reference catalog magnitude and limits\n");
    fprintf(stderr,"  -p num: initial plate scale in arcsec per pixel (default 0)\n");
    fprintf(stderr,"  -q: fit image to catalog magnitude polynomial(s)\n");
    fprintf(stderr,"  -r ang: rotation angle in degrees before fitting (default 0)\n");
    fprintf(stderr,"  -s frac: use this fraction extra stars (default 1.0)\n");
    fprintf(stderr,"  -t tol: offset tolerance in pixels (default 20)\n");
    fprintf(stderr,"  -u num: USNO catalog single plate number to accept\n");
    fprintf(stderr,"  -v: verbose\n");
    fprintf(stderr,"  -w: rotate image WCS with image\n");
    fprintf(stderr,"  -x x y: X and Y coordinates of reference pixel (default is center)\n");
    fprintf(stderr,"  -y num: multiply image dimensions by this for search (default is 1)\n");
    fprintf(stderr,"  -z: use AIPS classic projections instead of WCSLIB\n");
    exit (1);
}


static void
MatchCat (progname, name)

char	*progname;		/* Name of program being executed */
char	*name;			/* Name of FITS or IRAF image file */

{
    int lhead;			/* Maximum number of bytes in FITS header */
    int nbhead;			/* Actual number of bytes in FITS header */
    int iraffile;		/* 1 if IRAF image */
    char *image;		/* Image */
    char *header;		/* FITS header */
    char *irafheader = NULL;	/* IRAF image header */
    char pixname[256];		/* Pixel file name for revised image */
    char *newimage;

    image = NULL;

    /* Open IRAF image */
    if (isiraf (name)) {
	iraffile = 1;
	if ((irafheader = irafrhead (name, &lhead)) != NULL) {
	    header = iraf2fits (name, irafheader, lhead, &nbhead);
	    if (header == NULL) {
		fprintf (stderr, "Cannot translate IRAF header %s/n",name);
		free (irafheader);
		return;
		}
	    if (imsearch || rot || mirror) {
		if ((image = irafrimage (header)) == NULL) {
		    hgetm (header,"PIXFIL", 255, pixname);
		    fprintf (stderr, "Cannot read IRAF pixel file %s\n", pixname);
		    free (irafheader);
		    free (header);
		    return;
		    }
		}
	    }
	else {
	    fprintf (stderr, "Cannot read IRAF header file %s\n", name);
	    return;
	    }
	}

    /* Open FITS file */
    else {
	iraffile = 0;
	if ((header = fitsrhead (name, &lhead, &nbhead)) != NULL) {
	    if (imsearch || rot || mirror) {
		if ((image = fitsrimage (name, nbhead, header)) == NULL) {
		    fprintf (stderr, "Cannot read FITS image %s\n", name);
		    free (header);
		    return;
		    }
		}
	    }
	else {
	    fprintf (stderr, "Cannot read FITS file %s\n", name);
	    return;
	    }
	}

    if (verbose) {
	fprintf (stderr,"%s %s\n", progname, RevMsg);
	fprintf (stderr,"Matching catalog to ");
	if (iraffile)
	    fprintf (stderr,"IRAF image file %s\n", name);
	else
	    fprintf (stderr,"FITS image file %s\n", name);
	}

    /* Print existing WCS headers and check for permission to overwrite */
    (void) PrintWCS (header, verbose);

    /* Rotate and/or reflect image */
    if (imsearch  && (rot != 0 || mirror)) {
	if ((newimage = RotFITS (name,header,image,0,0,rot,mirror,bitpix,
				 rotatewcs,verbose))
	    == NULL) {
	    fprintf (stderr,"Image %s could not be rotated\n", name);
	    free (header);
	    if (iraffile)
		free (irafheader);
	    if (image != NULL)
		free (image);
	    return;
	    }
	free (image);
	image = newimage;
	}

    if (refcatname != NULL)
	(void) SetWCSFITS (name, header, image, refcatname, verbose);
    else
	printf ("IMMATCH: No reference catalog has been set \n");

    free (header);
    if (iraffile)
	free (irafheader);
    if (image != NULL)
	free (image);
    return;
}

/* Nov  6 1997	New program based on IMWCS
 * Nov 17 1997	Add optional second magnitude limit
 * Dec  8 1997	Fix bug in setting nominal WCS
 * Dec 15 1997	Add capability of reading and writing IRAF 2.11 images
 *
 * Jan 27 1998  Implement Mark Calabretta's WCSLIB
 * Jan 29 1998  Add -z for AIPS classic WCS projections
 * Feb 18 1998	Version 2.0: Full Calabretta implementation
 * Mar 27 1998	Version 2.1: Add IRAF TNX projection
 * Apr 13 1998	Version 2.2: Add polynomial plate fit
 * Apr 24 1998	change coordinate setting to setsys() from setfk4()
 * Apr 28 1998	Change coordinate system flags to WCS_*
 * May 27 1998	Include fitsio.h instead of fitshead.h
 * Jun  2 1998	Fix bugs in hput() and tabread()
 * Jun 11 1998	Change setwcstype() to setwcsproj() to avoid conflict
 * Jun 25 1998	Set WCS subroutine choice with SETDEFWCS()
 * Jul 24 1998	Make irafheader char instead of int
 * Aug  6 1998	Change fitsio.h to fitsfile.h
 * Oct 14 1998	Use isiraf() to determine file type
 * Nov 13 1998	Pass reference catalog name to SetWCSFITS
 * Nov 30 1998	Add version and help commands for consistency
 *
 * Jan 26 1999	Add option to format output for IRAF coord fitting task
 * Apr 13 1999	Fix progname to drop / when full pathname
 * Jun  8 1999	Return image pointer from RotFITS, not flag
 * Jun 10 1999	If -a argument is multiple of 90, rotate image
 * Jul  7 1999	Fix bug setting rotation
 * Aug 25 1999	Add Bright Star Catalog, BSC
 * Oct 22 1999	Drop unused variables after lint
 *
 * Jan 28 2000	Call setdefwcs() with WCS_ALT instead of 1
 * Mar  8 2000	Move catalog selection from executable name to subroutine
 *
 * May 25 2001	Add GSC-ACT and 2MASS Point Source Catalogs
 * Jul 25 2001	Add -q option to fit image to catalog magnitude polynoimial
 * Sep 13 2001	Add -k option to select magnitude by which to sort ref. cat.
 * Oct 25 2001	Allow arbitrary argument order on command line
 * Oct 31 2001	Print complete help message if no arguments
 * Dec 17 2001	Set mirror and rotation in FindStars()
 *
 * Apr 10 2002	Accept letter as well as number for magnitude
 *
 * Jan 23 2003	Add USNO-B1.0 Catalog
 * Apr 13 2003	Set revision message for subroutines using setrevmsg()
 *
 * Jul  1 2004	Drop unused declaration of DelWCSFITS()
 * Aug 30 2004	Fix declarations
 * Sep 15 2004	Add missing 0 shift arguments to RotFITS() call (Rob Creager)
 *
 * Apr  4 2005	Exit with an error message if no catalog is specified
 *
 * May 30 2006	Use -mx to specify magnitude instead of -k
 * Jun 21 2006	Clean up code
 * Sep 26 2006	Increase length of rastr and destr from 16 to 32
 *
 * Jan 10 2007	Call setgsclass() instead of setclass()
 * Jan 10 2007	Call setuplate() instead of setplate()
 * Jan 10 2007	Declare RevMsg static, not const
 * Jan 10 2007	Drop unused variable cs
 * Apr  6 2007	Rotate the image WCS unless -w is set
 */
