/* File bincat.c
 * January 10, 2007
 * By Doug Mink, Harvard-Smithsonian Center for Astrophysics
 * Send bug reports to dmink@cfa.harvard.edu

   Copyright (C) 2003-2007
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

/* Create a FITS image containing either the number of catalogued objects
 * per pixel in a field or the flux per pixel from those catalogued objects */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>
#include <math.h>

#include "libwcs/wcs.h"
#include "libwcs/lwcs.h"
#include "libwcs/fitsfile.h"
#include "libwcs/wcscat.h"

static void usage();
static void MakeFITS();
extern void setcenter();
extern void setsys();
extern void setrot();
extern void setsecpix();
extern void setsecpix2();
extern void setrefpix();
extern void setcdelt();
extern struct WorldCoor *GetFITSWCS();

static char *RevMsg = "BINCAT WCSTools 3.8.1, 14 December 2009, Doug Mink (dmink@cfa.harvard.edu)";
static int verbose = 0;		/* verbose flag */
static int debug = 0;		/* debugging flag */
static int bitpix = 0;	/* number of bits per pixel (FITS code, 0=no image) */
static int version = 0;	/* If 1, print only program name and version */
static int wcshead = 0;	/* If 1, add WCS information from command line */
static int nx = 100;		/* width of image in pixels */
static int ny = 100;		/* height of image in pixels */
static int extend = 0;	/* If 1, write primary header, add other files as ext */
static char *pixfile;		/* Pixel file name */
static char *newname;		/* FITS extension file name */
static int ncat = 0;            /* Number of reference catalogs to search */
static char *refcatname;	/* reference catalog names */
static int nmagmax = 5;
static char *progname;          /* Name of program as executed */
static struct StarCat *starcat;	/* Star catalog data structure */
static char cpname[16];         /* Name of program for error messages */
static int refcat;		/* Catalog code (wcscat.h) */
static int sortmag = 0;		/* Magnitude to bin */
static double maglim1 = MAGLIM1; /* Catalog bright magnitude limit */
static double maglim2 = MAGLIM2; /* Catalog faint magnitude limit */
static int classd = -1;		/* Guide Star Catalog object classes */
static double magscale = 0;	/* Flux scaling factor */
static char *wcsfile;		/* Read WCS from this FITS or IRAF file */

static int bincatparm();

extern void setminpmqual();
extern void setminid();
extern void setrevmsg();
extern void setproj();

int
main (ac, av)
int ac;
char **av;
{
    char *str;
    int readlist = 0;
    char *lastchar;
    char filename[128];
    FILE *flist;
    char *listfile;
    char rastr[32], decstr[32];
    double x, y;
    char cs1;
    char *ccom;
    int i, lcat;
    char *refcatn;
    char title[80];
    int sysref;         /* Coordinate system of reference catalog */
    double eqref;       /* Equinox of catalog to be searched */
    double epref;       /* Epoch of catalog to be searched */
    int nmag, mprop;

    pixfile = NULL;
    newname = NULL;
    starcat = NULL;

    setrevmsg (RevMsg);

    /* Check name used to execute program and set catalog name accordingly */
    progname = ProgName (av[0]);
    for (i = 0; i < strlen (progname); i++) {
        if (progname[i] > 95 && progname[i] < 123)
            cpname[i] = progname[i] - 32;
        else
            cpname[i] = progname[i];
        }
    refcatn = ProgCat (progname);
    if (refcatn != NULL) {
        refcatname = refcatn;
        refcat = RefCat (refcatn,title,&sysref,&eqref,&epref,&mprop,&nmag);
        if (nmag > nmagmax)
            nmagmax = nmag;
        }

    /* Check for help or version command first */
    str = *(av+1);
    if (!str || !strcmp (str, "help") || !strcmp (str, "-help"))
	usage((char) 0, NULL);
    if (!strcmp (str, "version") || !strcmp (str, "-version")) {
	version = 1;
	usage((char)0, NULL);
	}

    /* crack arguments */
    for (av++; --ac > 0; av++) {

	/* Set RA, Dec, and equinox if WCS-generated argument */
	if (strsrch (*av,":") != NULL) {
	    if (ac < 3)
		usage((char)0, "3 arguments needed for center coordinate");
	    else {
		strcpy (rastr, *av);
		ac--;
		strcpy (decstr, *++av);
		setcenter (rastr, decstr);
		ac--;
		setsys (wcscsys (*++av));
		}
	    }

	/* Set decimal degree center */
	else if (isnum (*av)) {
	    if (ac < 3)
		usage((char)0, "3 arguments needed for center coordinate");
	    else {
		strcpy (rastr, *++av);
		ac--;
		strcpy (decstr, *++av);
		setcenter (rastr, decstr);
		ac--;
		setsys (wcscsys (*++av));
		}
	    }

	/* Set parameters from keyword=value arguments */
	else if (strchr (*av, '=')) {
	    if (bincatparm (*av))
		fprintf (stderr, "BINCAT: %s is not a parameter.\n", *av);
	    }

	/* Otherwise, read command */
	else if (*(str = *av) == '-') {
	    char c;
	    while ((c = *++str))
	    switch (c) {

    		case 'a':	/* Initial rotation angle in degrees */
    		    if (ac < 2)
    			usage(c, "needs rotation angle in degrees");
    		    setrot (atof (*++av));
    		    ac--;
		    wcshead++;
    		    break;
	
    		case 'b':	/* Reference pixel coordinates in B1950 */
    		    if (ac < 3)
    			usage(c, "needs B1950 coordinates of output reference pixel");
		    setsys (WCS_B1950);
		    strcpy (rastr, *++av);
		    ac--;
		    strcpy (decstr, *++av);
		    ac--;
		    setcenter (rastr, decstr);
		    wcshead++;
    		    break;

		case 'c':       /* Set reference catalog */
		    if (ac < 2)
			usage (c, "needs a reference catalog file or ID");
		    lcat = strlen (*++av);
		    refcatn = (char *) calloc (1, lcat + 2);
		    strcpy (refcatn, *av);
		    refcatname = refcatn;
		    refcat = RefCat (refcatn,title,&sysref,&eqref,&epref,&mprop,&nmag);
		    if (nmag > nmagmax)
			nmagmax = nmag;
		    ncat = ncat + 1;
		    ac--;
		    break;

    		case 'd':	/* Set CDELTn, CROTAn instead of CD matrix */
		    setcdelt();
    		    break;

    		case 'e':	/* Make an extended FITS file */
		    extend = 1;
		    break;

    		case 'f':	/* Flux scaling factor */
    		    if (ac < 2)
    			usage (c, "needs a flux scaling factor > 0");
    		    magscale = atof (*++av);
    		    ac--;
    		    break;
	
    		case 'g':	/* Reference pixel coordinates in Galactic */
    		    if (ac < 3)
    			usage(c,"needs galactic coordinates of output reference pixel");
		    setsys (WCS_GALACTIC);
		    strcpy (rastr, *++av);
		    ac--;
		    strcpy (decstr, *++av);
		    ac--;
		    setcenter (rastr, decstr);
		    wcshead++;
    		    break;

    		case 'j':	/* Reference pixel coordinates in J2000 */
    		    if (ac < 3)
    			usage(c, "needs J2000 coordinates of output reference pixel");
		    setsys (WCS_J2000);
		    strcpy (rastr, *++av);
		    ac--;
		    strcpy (decstr, *++av);
		    ac--;
		    setcenter (rastr, decstr);
		    wcshead++;
    		    break;

		case 'm':	/* Magnitude limit */
		    if (ac < 2)
			usage(c, "needs a magnitude limit or limits");
		    cs1 = *(str+1);
		    if (cs1 != (char) 0) {
			++str;
			if (cs1 > '9')
			    sortmag = (int) cs1;
			else
			    sortmag = (int) cs1 - 48;
			}
		    av++;
		    ac--;
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
		    break;
	
    		case 'o':	/* Number of bits per pixel */
    		    if (ac < 2)
    			usage (c, "needs a number of bits per image pixel");
    		    bitpix = (int) atof (*++av);
    		    ac--;
    		    break;
	
    		case 'p':	/* Initial plate scale in arcseconds / pixel */
    		    if (ac < 2)
    			usage(c, "needs a plate scale");
    		    setsecpix (atof (*++av));
    		    ac--;
		    if (ac > 1 && isnum (*(av+1))) {
			setsecpix2 (atof (*++av));
			ac--;
			}
		    wcshead++;
    		    break;
	
		case 's':	/* Size of image in X and Y pixels */
		    if (ac < 3)
			usage(c, "needs x and y dimensions of output image");
		    nx = atoi (*++av);
		    ac--;
		    ny = atoi (*++av);
		    ac--;
    		    break;
	
    		case 't':	/* FITS projection code for output image */
    		    if (ac < 2)
    			usage(c, "needs projection type, such as TAN");
    		    setproj (*++av);
    		    ac--;
		    break;
	
    		case 'v':	/* More verbosity */
		    if (verbose)
			debug++;
		    else
    			verbose++;
    		    break;

		case 'w':       /* Read WCS from a FITS or IRAF file */
		    if (ac < 2)
			usage(c, "needs FITS or IRAF file name for header WCS");
		    wcsfile = *++av;
		    ac--;
		    break;
	
		case 'x':	/* Reference pixel X and Y coordinates */
		    if (ac < 3)
			usage(c,"needs X and Y coordinates of reference pixel");
		    x = atof (*++av);
		    ac--;
		    y = atof (*++av);
		    ac--;
    		    setrefpix (x, y);
		    wcshead++;
    		    break;

		case 'z':       /* Use AIPS classic WCS */
		    setdefwcs (WCS_ALT);
		    break;

    		default:
    		    usage(c, "Unknown option");
    		    break;
    		}
	    }
	else {
    	    MakeFITS (*av);
    	    if (verbose)
    		printf ("\n");
	    }
	}

    /* Process files in file of filenames */
    if (readlist) {
	if ((flist = fopen (listfile, "r")) == NULL) {
	    fprintf (stderr,"IMROT: List file %s cannot be read\n",
		     listfile);
	    usage ();
	    }
	while (fgets (filename, 128, flist) != NULL) {
	    lastchar = filename + strlen (filename) - 1;
	    if (*lastchar < 32) *lastchar = 0;
	    MakeFITS (filename);
	    if (verbose)
		printf ("\n");
	    }
	fclose (flist);
	}

    return (0);
}

static void
usage (arg, message)
char	arg;		/* single character command line argument */
char	*message;	/* Error message */
{
    char *catname;
    fprintf (stderr,"%s\n",RevMsg);
    if (version)
	exit (-1);
    if (message != NULL)
	fprintf (stderr, "ERROR: %c %s\n", arg, message);
    catname = CatName (refcat, "catalog");
    fprintf (stderr,"Bin %s into a FITS image\n", catname);
    fprintf(stderr,"Usage: [-v][-a degrees][-p scale][-b ra dec][-j ra dec][-s nx ny] filename\n");
    fprintf(stderr,"       [-x x y] [-c catname] file.fits ...\n");
    fprintf(stderr,"  -a ang: initial rotation angle in degrees (default 0)\n");
    fprintf(stderr,"  -b ra dec: initial center in B1950 (FK4) RA and Dec\n");
    fprintf(stderr,"  -c name: name of source catalog to bin\n");
    fprintf(stderr,"  -d: set CDELTn, CROTAn instead of CD matrix\n");
    fprintf(stderr,"  -f num: Set flux scaling factor (number if 0)\n");
    fprintf(stderr,"  -g long lat: initial center in Galactic longitude and latitude\n");
    fprintf(stderr,"  -j ra dec: initial center in J2000 (FK5) RA and Dec\n");
    fprintf(stderr,"  -mx mag1[,mag2]: Magnitude #x limit(s) (only one set allowed, default none) \n");
    fprintf(stderr,"  -o num: output pixel size in bits (FITS BITPIX, default=0)\n");
    fprintf(stderr,"  -p num: initial plate scale in arcsec per pixel (default 0)\n");
    fprintf(stderr,"  -s num num: size of image in x and y pixels (default 100x100)\n");
    fprintf(stderr,"  -t proj: set FITS CTYPE projection (default TAN)\n");
    fprintf(stderr,"  -v: verbose\n");
    fprintf(stderr,"  -w file: read WCS information from this FITS file\n");
    fprintf(stderr,"  -x x y: X and Y coordinates of reference pixel (default is center)\n");
    fprintf(stderr,"  -z: use AIPS classic projections instead of WCSLIB\n");
    exit (1);
}

static void
MakeFITS (name)
char *name;
{
    char *image;	/* FITS image */
    char *header;	/* FITS header */
    int lhead;		/* Maximum number of bytes in FITS header */
    double cra;		/* Center right ascension in degrees (returned) */
    double cdec;	/* Center declination in degrees (returned) */
    double dra;		/* Right ascension half-width in degrees (returned) */
    double ddec;	/* Declination half-width in degrees (returned) */
    double secpix;	/* Arcseconds per pixel (returned) */
    int wp;		/* Image width in pixels (returned) */
    int hp;		/* Image height in pixels (returned) */
    int sysout=0;	/* Coordinate system to return (0=image, returned) */
    double eqout=0.0;	/* Equinox to return (0=image, returned) */
    int i;
    int nbimage;	/* Number of bytes in image */
    char title[80];
    int sysref;         /* Coordinate system of reference catalog */
    double eqref;       /* Equinox of catalog to be searched */
    double epref;       /* Epoch of catalog to be searched */
    struct WorldCoor *wcs;
    FILE *diskfile;
    int nlog = 0;
    int mprop = 0;
    int nsw;
    int nmag;

    if (verbose)
	fprintf (stderr,"%s\n",RevMsg);

    /* Skip this catalog if no name is given */
    if (refcatname == NULL || strlen (refcatname) == 0) {
	fprintf (stderr, "No file created; catalog not specified\n");
	return;
	}

    /* Figure out which catalog we are searching */
    if (!(refcat = RefCat (refcatname,title,&sysref,&eqref,
			   &epref,&mprop,&nmag))) {
	fprintf (stderr,"No file created; catalog '%s' missing\n", refcatname);
	return;
	}

    if (verbose)
	fprintf (stderr,"Create FITS file %s from %s\n", name, title);

    /* Make sure that no existing file is overwritten */
    if ((diskfile = fopen (name, "r")) != NULL) {
	fprintf (stderr,"BINCAT: FITS file %s exists, no new file written\n",
		     name);
	fclose (diskfile);
	return;
	}

    /* Set logging interval */
    if (debug)
	nlog = 1;
    else if (verbose) {
	if (refcat==UAC || refcat==UA1 || refcat==UA2 || refcat==UB1 ||
	    refcat==USAC || refcat==USA1 || refcat==USA2 || refcat==GSC ||
	    refcat==GSCACT || refcat==TMPSC || refcat==TMIDR2)
	    nlog = 1000;
	else
	    nlog = 100;
	}

    /* Initialized header */
    lhead = 14400;
    header = (char *) calloc (1, lhead);
    strcpy (header, "END ");
    for (i = 4; i < lhead; i++)
	header[i] = ' ';
    hlength (header, 14400);
    hputl (header, "SIMPLE", 1);
    hputi4 (header, "BITPIX", bitpix);
    hputi4 (header, "NAXIS", 2);
    hputi4 (header, "NAXIS1", nx);
    hputi4 (header, "NAXIS2", ny);
    if (bitpix < 0)
	nbimage = (-bitpix / 8) * nx * ny;
    else
	nbimage =  (bitpix / 8) * nx * ny;

    /* Set up blank image, if bitpix is non-zero */
    if (bitpix == 0) {
	printf ("BINCAT: No BITPIX; no FITS image written\n");
	return;
	}
    else {
	if ((image = calloc (nbimage, 1)) == NULL)
	    printf ("*** BINCAT: Could not allocate %d bytes for image\n",
		    nbimage);
	if (verbose)
	    fprintf (stderr, "BINCAT: %d bits/pixel\n", bitpix);
	}

    /* Initialize header */
    if (wcshead) {
	wcs = GetFITSWCS (name,header,verbose,&cra,&cdec,&dra,&ddec,&secpix,
			  &wp,&hp,&sysout,&eqout);
	}
    else {
	printf ("BINCAT: No WCS set; no FITS image written\n");
	return;
	}

    nsw = ctgbin (refcatname,refcat,wcs,header,image,maglim1,maglim2,sortmag,magscale,nlog);

    hputc (header, "COMMENT", "FITS (Flexible Image Transport System) format is defined in 'Astronomy");
    hputc (header, "COMMENT", "and Astrophysics' vol 376, page 359; bibcode 2001A&A...376..359H.");

    if (nsw < 1) {
	printf ("*** No stars written to image\n");
	}
    else if (fitswimage (name, header, image) > 0 && verbose) {
	printf ("%s: %d-byte FITS image written successfully.\n",
		name, nbimage);
	}

    wcsfree (wcs);
    free (header);
    free (image);
    return;
}


/* Set parameter values from the command line as keyword=value
 * Return 1 if successful, else 0 */

static int
bincatparm (parstring)

char *parstring;
{
    char *parname;
    char *parvalue;
    char *parequal;
    int minid;

    /* Separate parameter name and value */
    parname = parstring;
    if ((parequal = strchr (parname,'=')) == NULL)
        return (0);
    *parequal = (char) 0;
    parvalue = parequal + 1;

    /* Minimum proper motion quality for USNO-B1.0 catalog */
    if (!strcasecmp (parname, "minpmq")) {
        if (isnum (parvalue)) {
            minid = atoi (parvalue);
            setminpmqual (minid);
            }
        }

    /* Minimum number of plate ID's for USNO-B1.0 catalog */
    else if (!strcasecmp (parname, "minid")) {
        if (isnum (parvalue)) {
            minid = atoi (parvalue);
            setminid (minid);
            }
        }

    /* Guide Star object class */
    else if (!strncasecmp (parname,"cla",3)) {
        classd = (int) atof (parvalue);
        setgsclass (classd);
        }

    else {
        *parequal = '=';
        return (1);
        }
    *parequal = '=';
    return (0);
}


/* Sep 25 2003	New program based on newfits and scat
 *
 * Nov 16 2006	Do not write image if no catalog objects are found
 *
 * Jan 10 2007	Drop unused variables
 */
