/* File imcat.c
 * December 14, 2009
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
#include "libwcs/wcscat.h"
#include "libwcs/lwcs.h"

#define MAXFILES 1000
static int maxnfile = MAXFILES;
static double *gnum;	/* Catalog numbers */
static double *gra;	/* Catalog right ascensions, degrees */
static double *gdec;	/* Catalog declinations, degrees */
static double *gpra;	/* Catalog right ascension proper motions, degrees/year */
static double *gpdec;	/* Catalog declination proper motions, degrees/year */
static double **gm;		/* Catalog star magnitudes */
static double *gx;	/* Catalog star X positions on image */
static double *gy;	/* Catalog star Y positions on image */
static int *gc;		/* Catalog object classes, plates, etc. */
static char **gobj;	/* Catalog object names */
static char **gobj1;	/* Catalog object names */
static int nalloc = 0;
static int nbuffer = 0;	/* size of buffer */

static void PrintUsage();
static void FreeBuffers();
static int AllocBuffers();
static void ListCat();
extern void fk524e();
extern struct WorldCoor *GetFITSWCS();
extern char *GetFITShead();
extern void setsys();
extern void setcenter();
extern void setsecpix();
extern void setrefpix();
extern void setdateobs();
extern void setparm();
extern void setnpix();

static char *RevMsg = "WCSTools 3.8.1, 14 December 2009, Doug Mink (dmink@cfa.harvard.edu)";

static int verbose = 0;		/* verbose/debugging flag */
static int wfile = 0;		/* True to print output file */
static int refcat = GSC;	/* reference catalog switch */
static int classd = -1;		/* Guide Star Catalog object classes */
static int uplate = 0;		/* UJ Catalog plate number to use */
static double maglim1 = MAGLIM1; /* reference catalog bright magnitude limit */
static double maglim2 = MAGLIM2; /* reference catalog faint magnitude limit */
static int nstars = 0;		/* Number of brightest stars to list */
static int printhead = 0;	/* 1 to print table heading */
static int tabout = 0;		/* 1 for tab table to standard output */
static int catsort = SORT_MAG;	/* Default to sort stars by magnitude */
static int debug = 0;		/* True for extra information */
static int degout0 = 0;		/* True for RA and Dec in fractional degrees */
static char *keyword = NULL;	/* Column to add to tab table output */
static int sysout = 0;		/* Output coordinate system */
static int starcount = 0;	/* 1 to only print number of stars found */
static double eqout = 0.0;	/* Equinox for output coordinates */
static int version = 0;		/* If 1, print only program name and version */
static int nmagmax = MAXNMAG;
static int obname[5];		/* If 1, print object name, else number */
static struct StarCat *starcat[5]; /* Star catalog data structure */
static int sortmag = 0;		/* Magnitude by which to sort stars */
static int webdump = 0;
static char *progname;		/* Name of program as executed */
static int minid = 0;		/* Minimum number of plate IDs for USNO-B1.0 */
static int minpmqual = 0;	/* Minimum USNO-B1.0 proper motion quality */
static int printepoch = 0;	/* 1 to print epoch of entry */
static int region_radius[5];	/* Min Radius for SAOimage region file output */
static int region_radius1[5];	/* Max Radius for SAOimage region file output */
static int region_char[5];	/* Character for SAOimage region file output */
static int lofld = 0;		/* Length of object name field in output */

extern int getminpmqual();
extern int getminid();
extern void setrevmsg();
extern void setrot();

static void ImageLim();

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
    char errmsg[256];
    FILE *flist;
    char *listfile = NULL;
    char *cstr;
    char cs, cs1;
    char **fn;
    int i, ic;
    int nx, ny;
    double x, y;
    char *refcatname[5];	/* reference catalog name */
    int ncat = 0;
    int rcat = 0;
    int region_pixel;
    char *refcatn;
    int ifile, nfile;
    int lcat;
    int scat = 0;
    char c1, c, *ccom;
    double drot;

    /* Ensure global variables are set up so that Alloc/FreeBuffers
       work sensibly */
    nbuffer = 0;
    nalloc = 0;
    gm = NULL;
    gra = NULL;
    gdec = NULL;
    gpra = NULL;
    gpdec = NULL;
    gnum = NULL;
    gc = NULL;
    gx = NULL;
    gy = NULL;
    gobj = NULL;
    gobj1 = NULL;

    nfile = 0;
    fn = (char **)calloc (maxnfile, sizeof(char *));

    for (i = 0; i< 5; i++)
	starcat[i] = NULL;

    for (i = 0; i < 5; i++) {
	region_radius[i] = 0;
	region_radius1[i] = 0;
	region_char[i] = 0;
	obname[i] = 0;
	}
    setrevmsg (RevMsg);

    /* Check name used to execute programe and set catalog name accordingly */
    progname = ProgName (av[0]);
    refcatn = ProgCat (progname);
    if (refcatn != NULL) {
	refcatname[ncat] = refcatn;
	ncat++;
	}

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
	    PrintUsage ("version");
	    }

	else if (strchr (str, '=')) {
	    setparm (str);
	    minid = getminid ();
	    minpmqual = getminpmqual ();
	    }

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

	    while ((c = *++str) != 0) {
    		switch (c) {

		case 'a':       /* Initial rotation angle in degrees */
		    if (ac < 2)
			PrintUsage (str);
		    drot = atof (*++av);
		    setrot (drot);
		    ac--;
		    break;

		case 'b':	/* initial coordinates on command line in B1950 */
		    str1 = *(av+1);
		    ic = (int)str1[0];
		    if (*(str+1) || ic < 48 || ic > 58) {
			setsys(WCS_B1950);
			sysout = WCS_B1950;
			eqout = 1950.0;
			}
		    else if (ac < 3)
			PrintUsage (str);
		    else {
			setsys(WCS_B1950);
			sysout = WCS_B1950;
			eqout = 1950.0;
			strcpy (rastr, *++av);
			ac--;
			strcpy (decstr, *++av);
			ac--;
			setcenter (rastr, decstr);
			}
		    break;

		case 'c':       /* Set reference catalog */
		    if (ac < 2)
			PrintUsage (str);
		    lcat = strlen (*++av);
		    refcatn = (char *) calloc (1, lcat + 1);
		    strcpy (refcatn, *av);
		    refcatname[ncat] = refcatn;
		    ncat = ncat + 1;
		    ac--;
		    break;

		case 'd':
		    degout0++;
		    break;

		case 'e':
		    sysout = WCS_ECLIPTIC;
		    break;

		case 'f':
		    starcount = 1;
		    break;

		case 'g':
		    sysout = WCS_GALACTIC;
		    break;

		case 'h':	/* ouput descriptive header */
		    printhead++;
		    break;

		case 'i':	/* Label region with name, not number */
		    if (ncat > 0)
			obname[ncat-1]++;
		    else
			obname[0]++;
		    break;

		case 'j':	/* center coordinates on command line in J2000 */
		    str1 = *(av+1);
		    ic = (int)str1[0];
		    if (*(str+1) || ic < 48 || ic > 58) {
			setsys(WCS_J2000);
			sysout = WCS_J2000;
			eqout = 2000.0;
			}
		    else if (ac < 3)
			PrintUsage (str);
		    else {
			setsys(WCS_J2000);
			sysout = WCS_J2000;
			eqout = 2000.0;
			strcpy (rastr, *++av);
			ac--;
			strcpy (decstr, *++av);
			ac--;
			setcenter (rastr, decstr);
			}
		    break;

		case 'k':	/* Keyword (column) to add to output from tab table */
		    if (ac < 2)
			PrintUsage (str);
		    keyword = *++av;
		    settabkey (keyword);
		    if (ncat > 0)
			obname[ncat-1]++;
		    else
			obname[0]++;
		    ac--;
		    break;

		case 'l':   /* Size of image in X and Y pixels */
		    if (ac < 3)
			PrintUsage(str);
		    nx = atoi (*++av);
		    ac--;
		    ny = atoi (*++av);
		    ac--;
		    setnpix (nx, ny);
		    break;

		case 'm':	/* Limiting reference star magnitude */
		    if (ac < 2)
			PrintUsage (str);
		    cs1 = *(str+1);
		    if (cs1 != (char) 0) {
			++str;
			if (cs1 > '9')
			    sortmag = (int) cs1;
			else
			    sortmag = (int) cs1 - 48;
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
		    break;

		case 'n':	/* Number of brightest stars to read */
		    if (ac < 2)
			PrintUsage (str);
		    nstars = atoi (*++av);
		    ac--;
		    break;

		case 'o':	/* Guide Star object class */
		    if (ac < 2)
			PrintUsage (str);
		    classd = (int) atof (*++av);
		    setgsclass (classd);
		    ac--;
		    break;

		case 'p':	/* Initial plate scale in arcseconds per pixel */
		    if (ac < 2)
			PrintUsage (str);
		    setsecpix (atof (*++av));
		    ac--;
		    break;

		case 'q':	/* Output region file shape for SAOimage */
		    if (ac < 2)
			PrintUsage (str);
		    cstr = *++av;
		    c1 = cstr[0];
		    region_pixel = 0;
		    if (strlen(cstr) > 1) {
			if (cstr[0] == 'p') {
			    c1 = cstr[1];
			    region_pixel = 10;
			    }
			else if (cstr[1] == 'p')
			    region_pixel = 10;
			}
		    switch (c1){
			case 'c':
			    if (cstr[1] == 'i')
				region_char[scat] = WCS_CIRCLE;
			    else
				region_char[scat] = WCS_CROSS;
			    break;
			case 'd':
			    region_char[scat] = WCS_DIAMOND;
			    break;
			case 'p':
			    region_char[scat] = WCS_PCIRCLE;
			    break;
			case 's':
			    region_char[scat] = WCS_SQUARE;
			    break;
			case 'x':
			    region_char[scat] = WCS_EX;
			    break;
			case 'v':
			    region_char[scat] = WCS_VAR;
			    break;
			case '+':
			    region_char[scat] = WCS_CROSS;
			    break;
			case 'o':
			default:
			    region_char[scat] = WCS_CIRCLE;
			}
		    region_char[scat] = region_char[scat] + region_pixel;
		    if (region_radius[scat] == 0) {
			region_radius[scat] = -1;
			region_radius1[scat] = -1;
			}
		    scat++;
		    wfile++;
		    ac--;
		    break;

		case 'r':	/* Output region file with shape radius for SAOimage */
		    if (ac < 2)
			PrintUsage (str);
		    if ((ccom = strchr (*++av, ',')) != NULL) {
			*ccom = (char) 0;
			region_radius[rcat] = atoi (*av);
			region_radius1[rcat] = atoi (ccom+1);
			}
		    else {
			region_radius[rcat] = atoi (*av);
			region_radius1[rcat] = region_radius[rcat];
			}
		    if (region_radius[rcat] == 0) {
			region_radius[rcat] = -1;
			region_radius1[rcat] = -1;
			}
		    rcat++;
		    wfile++;
		    ac--;
		    break;

		case 's':	/* sort by RA, Dec, magnitude or nothing */
		    catsort = SORT_RA;
		    cs = (char) 0;
		    cs1 = (char) 0;
		    if (ac > 1) {
			str1 = *(av + 1);
			cs = str1[0];
			if (strchr ("dimnrxy",(int)cs)) {
			    cs1 = str1[1];
			    av++;
			    ac--;
			    }
			else
			    cs = 'r';
			}
		    else
			cs = 'r';
		    if (cs) {

			/* Declination */
			if (cs == 'd')
			    catsort = SORT_DEC;

			/* Magnitude (brightest first) */
			else if (cs == 'm') {
			    catsort = SORT_MAG;
			    if (cs1 != (char) 0) {
				if (cs1 > '9')
				    sortmag = (int) cs1;
				else
				    sortmag = (int) cs1 - 48;
				}
			    }

			/* No sorting */
			else if (cs == 'n')
			    catsort = SORT_NONE;

			/* ID number */
			else if (cs == 'i')
			    catsort = SORT_ID;

			/* X coordinate */
			else if (cs == 'x')
			    catsort = SORT_X;

			/* Y coordinate */
			else if (cs == 'y')
			    catsort = SORT_Y;

			/* Right ascension */
			else if (cs == 'r')
			    catsort = SORT_RA;
			else
			    catsort = SORT_RA;
			}
		    else
			catsort = SORT_RA;
		    break;

		case 't':	/* tab table to stdout */
		    tabout = 1;
		    break;

		case 'u':	/* UJ Catalog plate number */
		    if (ac < 2)
			PrintUsage (str);
		    uplate = (int) atof (*++av);
		    setuplate (uplate);
		    ac--;
		    break;

		case 'v':	/* more verbosity */
		    if (debug) {
			webdump++;
			debug = 0;
			verbose = 0;
			}
		    else if (verbose)
			debug++;
		    else
			verbose++;
		    break;

		case 'w':	/* write output file */
		    wfile++;
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

		case 'y':	/* Epoch of image in FITS date format */
		    if (ac < 2)
			PrintUsage (str);
		    setdateobs (*++av);
		    ac--;
		    break;

		case 'z':       /* Use AIPS classic WCS */
		    setdefwcs (WCS_ALT);
		    break;

		default:
		    sprintf (errmsg, "* Illegal command -%c-", c);
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

    /* if (!verbose && !wfile)
	verbose = 1; */

    /* If reference catalog is not set, exit with error message */
    if (refcatname == NULL) {
	PrintUsage ("* Reference catalog must be specified using -c or alias.");
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
	    if (debug)
		printf ("%s:\n", filename);
	    ListCat (progname,filename,ncat,refcatname);
	    }
	fclose (flist);
	}

    /* Process image files */
    else if (nfile > 0) {
	for (ifile = 0; ifile < nfile; ifile++) {
	    if ( verbose)
		printf ("%s:\n", fn[ifile]);
	    ListCat (progname,fn[ifile], ncat, refcatname);
	    if (verbose)
		printf ("\n");
	    }
	}

    /* Create header with WCS information on command line */
    else {
	filename[0] = (char) 0;
	ListCat (progname, filename, ncat, refcatname);
	}

    /* Close source catalogs */
    for (i = 0; i < 5; i++)
	if (starcat[i] != NULL) ctgclose (starcat[i]);

    /* Free memory used for search results and return */
    FreeBuffers();
    free (fn);
    fn = NULL;
    return (0);
}

static void
PrintUsage (command)
char	*command;
{
    int catcode;
    char *srcname;
    char *catname;

    fprintf (stderr,"%s %s\n", progname, RevMsg);
    if (version)
	exit (0);

    if (command != NULL) {
	if (command[0] == '*')
	    fprintf (stderr, "%s\n", command);
	else
	    fprintf (stderr, "* Missing argument for command: %c\n", command[0]);
	exit (1);
	}

    catname = ProgCat (progname);
    catcode = CatCode (catname);
    srcname = CatSource (catcode, NULL);
    fprintf (stderr,"List %s in FITS and IRAF image files\n", srcname);

    if (refcat == GSC || refcat == GSCACT || refcat == GSC2)
	fprintf (stderr,"Usage: [-vhst] [-m [mag1] mag2] [-g class]\n");
    else if (refcat > 0)
	fprintf (stderr,"Usage: [-vhst] [-m [mag1] mag2]\n");
    else if (refcat == UJC|| refcat == UAC || refcat == UA1 || refcat == UA2 ||
	     refcat == USAC || refcat == USA1 || refcat == USA2)
	fprintf (stderr,"Usage: [-vhst] [-m [mag1] mag2] [-u plate]\n");
    else
	fprintf (stderr,"Usage: [-vwhst][-a deg][-m [mag1] mag2][-c catalog][-x x y]\n");
    fprintf (stderr,"       [-p scale][-q osd+x][-b ra dec][-j ra dec][-r arcsec] FITS or IRAF file(s)\n");
    fprintf (stderr,"  -a: initial rotation angle in degrees (default 0)\n");
    fprintf (stderr,"  -b [RA Dec]: Output, (center) in B1950 (FK4) RA and Dec\n");
    fprintf (stderr,"  -c name: Reference catalog (gsc, ua2, local file, etc.\n");
    fprintf (stderr,"  -d: Output RA,Dec positions in fractional degrees\n");
    fprintf (stderr,"  -e: Output in ecliptic longitude and latitude\n");
    fprintf (stderr,"  -f: Print only number of stars found, nothing else\n");
    fprintf (stderr,"  -g: Output in galactic longitude and latitude\n");
    fprintf (stderr,"  -h: Print heading, else do not \n");
    fprintf (stderr,"  -i: Print name instead of number in region file \n");
    fprintf (stderr,"  -j [RA Dec]: Output (center) in J2000 (FK5) RA and Dec\n");
    fprintf (stderr,"  -k keyword: Add this keyword to output from tab table search\n");
    fprintf (stderr,"  -l nx ny: X and Y size of image in pixels\n");
    fprintf (stderr,"  -mx m1[,m2]: Catalog magnitude #x limit(s) (only one set allowed, default none)\n");
    fprintf (stderr,"  -n num: Number of brightest stars to print \n");
    fprintf (stderr,"  -o name: Set HST Guide Star object class to print \n");
    fprintf (stderr,"  -p num: Initial plate scale in arcsec per pixel (default 0)\n");
    fprintf (stderr,"  -q osdv+x[p]: Write SAOimage region file of this shape (filename.cat)\n");
    fprintf (stderr,"  -r num[,num]: Write SAOimage region file of this radius [range] (filename.cat)\n");
    fprintf (stderr,"  -s d|mx|n|r|x|y: Sort by r=RA d=Dec mx=Mag#x n=none x=X y=Y\n");
    fprintf (stderr,"  -t: Tab table to standard output as well as file\n");
    fprintf (stderr,"  -u num: USNO catalog single plate number to accept\n");
    fprintf (stderr,"  -v: Verbose\n");
    fprintf (stderr,"  -w: Write tab table output file [imagename].[catalog]\n");
    fprintf (stderr,"  -x x y: X and Y coordinates of reference pixel (default is center)\n");
    fprintf (stderr,"  -y date: Epoch of image in FITS date format or year\n");
    fprintf (stderr,"  -z: Use AIPS classic projections instead of WCSLIB\n");
    exit (1);
    fprintf (stderr,"   x: Number of magnitude must be same for sort and limits\n");
    fprintf (stderr,"      and x may be omitted from either or both -m and -s m\n");
}


/* Returns 1 if buffers could be allocated, 0 otherwise */
static int
AllocBuffers (ngmax)

int    ngmax;	  /* Number of entries to allocate */

{
    int i, imag;

    FreeBuffers();

    nbuffer = nmagmax;
    nalloc = ngmax;

    if (!(gm = (double **) calloc (nbuffer, sizeof(double *)))) {
	fprintf (stderr, "Could not calloc %d bytes for gm\n",
	    nbuffer*sizeof(double *));
	FreeBuffers();
	return (0);
    }
    for (imag = 0; imag < nbuffer; imag++) {
	if (!(gm[imag] = (double *) calloc (ngmax, sizeof(double)))) {
	   fprintf (stderr, "Could not calloc %d bytes for gm\n",
		    ngmax*sizeof(double));
	    FreeBuffers();
	    return (0);
       }
    }

    if (!(gra = (double *) calloc (ngmax, sizeof(double)))) {
	fprintf (stderr, "Could not calloc %d bytes for gra\n",
	    ngmax*sizeof(double));
	FreeBuffers();
	return (0);
    }
    if (!(gdec = (double *) calloc (ngmax, sizeof(double)))) {
	fprintf (stderr, "Could not calloc %d bytes for gdec\n",
	    ngmax*sizeof(double));
	FreeBuffers();
	return (0);
    }
    if (!(gpra = (double *) calloc (ngmax, sizeof(double)))) {
	fprintf (stderr, "Could not calloc %d bytes for gpra\n",
	    ngmax*sizeof(double));
	FreeBuffers();
	return (0);
    }
    if (!(gpdec = (double *) calloc (ngmax, sizeof(double)))) {
	fprintf (stderr, "Could not calloc %d bytes for gpdec\n",
	    ngmax*sizeof(double));
	FreeBuffers();
	return (0);
    }
    if (!(gnum = (double *) calloc (ngmax, sizeof(double)))) {
	fprintf (stderr, "Could not calloc %d bytes for gnum\n",
	   ngmax*sizeof(double));
       FreeBuffers();
	return (0);
    }
    if (!(gc = (int *) calloc (ngmax, sizeof(int)))) {
	fprintf (stderr, "Could not calloc %d bytes for gc\n",
	    ngmax*sizeof(int));
	FreeBuffers();
	return (0);
    }
    if (!(gx = (double *) calloc (ngmax, sizeof(double)))) {
	fprintf (stderr, "Could not calloc %d bytes for gx\n",
	    ngmax*sizeof(double));
	FreeBuffers();
	return (0);
    }
    if (!(gy = (double *) calloc (ngmax, sizeof(double)))) {
	fprintf (stderr, "Could not calloc %d bytes for gy\n",
	    ngmax*sizeof(double));
	FreeBuffers();
	return (0);
    }
    if (!(gobj = (char **) calloc (ngmax, sizeof(char *)))) {
	fprintf (stderr, "Could not calloc %d bytes for obj\n",
	    ngmax*sizeof(char *));
	FreeBuffers();
	return (0);
    }
    for (i = 0; i < ngmax; i++)
	gobj[i] = NULL;

    return 1;
}


static void
FreeBuffers ()
{
    int i, imag;

    if (gm) {
	for (imag = 0; imag < nbuffer; imag++)
	    if (gm[imag]) {free ((char *) gm[imag]); gm[imag] = NULL; }
	free ((char *)gm);
	gm = NULL;
    }
    if (gra) { free ((char *)gra); gra = NULL; }
    if (gdec) { free ((char *)gdec); gdec = NULL; }
    if (gpra) { free ((char *)gpra); gpra = NULL; }
    if (gpdec) { free ((char *)gpdec); gpdec = NULL; }
    if (gnum) { free ((char *)gnum); gnum = NULL; }
    if (gc) { free ((char *)gc); gc = NULL; }
    if (gx) { free ((char *)gx); gx = NULL; }
    if (gy) { free ((char *)gy); gy = NULL; }
    if (gobj) {
	for (i = 0; i < nalloc; i++) {
	    if (gobj[i] != NULL) {
		free ((char *)gobj[i]);
		gobj[i] = NULL;
		}
	    }
	free ((char *)gobj);
	gobj = NULL;
	}
    nbuffer = 0;
    nalloc = 0;
    return;
}


static void
ListCat (progname, filename, ncat, refcatname)

char	*progname;	/* Name of program being executed */
char	*filename;	/* FITS or IRAF file filename */
int	ncat;		/* Number of catalogs to search */
char	**refcatname;	/* reference catalog name */

{
    char *header;	/* FITS image header */
    int ng;		/* Number of catalog stars */
    int nbg;		/* Number of brightest catalog stars actually used */
    int imh, imw;	/* Image height and width in pixels */
    int i, j, ngmax;
    int degout;
    FILE *fd = NULL;
    struct WorldCoor *wcs = NULL; /* World coordinate system structure */
    double eqref;	/* Equinox of catalog to be searched */
    double epref;	/* Epoch of catalog to be searched */
    double epout;	/* Epoch of catalog to be searched */
    int sysref;		/* Coordinate system of catalog to be searched */
    char rastr[32], decstr[32];	/* coordinate strings */
    char numstr[32];	/* Catalog number */
    double cra, cdec, dra, ddec, ra1, ra2, dec1, dec2, mag1, mag2,secpix;
    double mag, drad, flux;
    double era, edec, epmr, epmd;
    int nim, nct;
    int offscale, nlog, imag;
    char headline[160];
    char temp[80];
    char title[80];
    char outfile[80];
    char *fname;
    char *temp1;
    char magname[8];
    char isp[4];
    int nmag1;
    int icat;
    int printobj = 0;
    int nndec = 0;
    int nnfld;
    char blanks[256];
    int lfn;
    int lobj;
    int lhead;
    int band = 0;
    int ngsc = 0;
    int gcset;
    int mprop;
    int nmag;
    int sptype;
    int nofile = 0;
    int magsort;
    double gxmax, gymax;
    double pra, pdec;
    double maxnum;
    char *catalog;

    /* Drop out if no catalog is specified */
    if (ncat == 0) {
	fprintf (stderr, "No catalog specified\n");
	exit (-1);
	}

    /* Allocate space for returned catalog information */
    if (nstars != 0)
	ngmax = nstars;
    else
	ngmax = MAXCAT;

    /* Allocate or reallocate buffers for current catalog if necessary */
    if (ngmax > nalloc) {
	if (AllocBuffers (ngmax) == 0)
	    return;

	/* Initialize catalog entry values */
	for (i = 0; i < ngmax; i++) {
	    for (imag = 0; imag < nmagmax; imag++)
		gm[imag][i] = 99.0;
	    gra[i] = 0.0;
	    gdec[i] = 0.0;
	    gpra[i] = 0.0;
	    gpdec[i] = 0.0;
	    gnum[i] = 0.0;
	    gc[i] = 0;
	    gx[i] = 0.0;
	    gy[i] = 0.0;
	    }
	}

    if (tabout)
	printhead = 0;

    isp[2] = 0;
    isp[3] = 0;
    if (verbose || printhead)
	printf ("\n%s %s\n", progname, RevMsg);
    for (i = 0; i < 255; i++)
	blanks[i] = ' ';
    blanks[255] = (char) 0;

    if (strlen (filename) < 1) {
	strcpy (filename, "NoFile");
	nofile = 1;
	}

    /* Loop through catalogs */
    for (icat = 0; icat < ncat; icat++) {

	/* Skip this catalog if no name is given */
	if (refcatname[icat] == NULL || strlen (refcatname[icat]) == 0) {
	    fprintf (stderr, "Catalog %d not specified\n", icat);
	    continue;
	    }

    /* Find title and coordinate system for catalog */
    if (!(refcat = RefCat (refcatname[icat],title,&sysref,&eqref,&epref,&mprop,&nmag))) {
	fprintf (stderr,"ListCat: No catalog named %s\n", refcatname[icat]);
	return;
	}

    /* If more magnitudes are needed, allocate space for them */
    if (refcat == UCAC2 || refcat == UCAC3)
	nmag1 = nmag + 4;
    else
	nmag1 = nmag;
    if (nmag1 > nmagmax) {
	nmagmax = nmag1;
	if (AllocBuffers (ngmax) == 0)
	    return;
	}

    if (classd == 0)
	strcat (title, " stars");
    else if (classd == 3)
	strcat (title, " nonstars");

    /* Initialize FITS header from file if one is there or to nothing */
    if (nofile) {
	lhead = 14400;
	header = (char *) calloc (1, lhead);
	strcpy (header, "END ");
	for (i = 4; i < lhead; i++)
	    header[i] = ' ';
	hlength (header, 14400);
	hputl (header, "SIMPLE", 1);
	hputi4 (header, "BITPIX", 0);
	hputi4 (header, "NAXIS", 2);
	hputi4 (header, "NAXIS1", 1);
	hputi4 (header, "NAXIS2", 1);
	}
    else if ((header = GetFITShead (filename, verbose)) == NULL)
	return;

    /* Read world coordinate system information from the image header */
    wcs = GetFITSWCS (filename, header, verbose, &cra, &cdec, &dra, &ddec,
		      &secpix, &imw, &imh, &sysout, &eqout);
    gxmax = (double) imw + 0.5;
    gymax = (double) imh + 0.5;
    free (header);
    if (nowcs (wcs)) {
	if (nofile)
	    PrintUsage ("* No files to process or incomplete command line WCS.");
	else
	    fprintf (stderr, "Incomplete WCS in image file\n");
	wcsfree (wcs);
	return;
	}

    /* Set up limits for search, taking into account image rotation */
    ImageLim (wcs,&cra, &cdec, &dra, &ddec, &ra1, &ra2, &dec1, &dec2);
    epout = wcs->epoch;
    if (verbose || printhead) {
	char rastr1[32],rastr2[32],decstr1[32],decstr2[32], cstr[16];
	wcscstr (cstr, sysout, eqout, epout);
	ra2str (rastr1, 16, ra1, 3);
	ra2str (rastr2, 16, ra2, 3);
	printf ("%s: RA:  %s - %s %s\n",filename,rastr1, rastr2, cstr);
	dec2str (decstr1, 16, dec1, 2);
	dec2str (decstr2, 16, dec2, 2);
	lfn = strlen (filename);
	blanks[lfn] = (char) 0;
	printf ("%s  Dec: %s - %s %s\n", blanks, decstr1, decstr2, cstr);
	blanks[lfn] = ' ';
	}

/* Set the magnitude limits for the search */
    if (maglim2 == 0.0) {
	mag1 = 0.0;
	mag2 = 0.0;
	}
    else {
	mag1 = maglim1;
	mag2 = maglim2;
	}
    if (mag2 < mag1) {
	mag = mag1;
	mag1 = mag2;
	mag2 = mag;
	}
    if (sortmag > 9)
	sortmag = CatMagNum (sortmag, refcat);

    if (nstars > 0)
	ngmax = nstars;
    else if (nstars < 0)
	ngmax = 1;
    else
	ngmax = MAXCAT;

    if (AllocBuffers (ngmax) != 1) {
        wcsfree (wcs);
        return;
	}

    if (webdump)
	nlog = -1;
    else if (verbose) {
	if (refcat==UAC  || refcat==UA1  || refcat==UA2 || refcat==UB1 ||
	    refcat==USAC || refcat==USA1 || refcat==USA2 || refcat==GSC  ||
	    refcat==GSCACT || refcat==TMPSC || refcat==TMIDR2 ||
	    refcat== SDSS || refcat == YB6)
	    nlog = 1000;
	else
	    nlog = 100;
	}
    else
	nlog = 0;
    if (nstars < 0)
	ngmax = -1;

    /* Find the nearby reference stars, in ra/dec */
    drad = 0.0;
    ng = ctgread (refcatname[icat], refcat, 0,
		  cra,cdec,dra,ddec,drad,0.0,sysout,eqout,epout,mag1,mag2,
		  sortmag,ngmax,&starcat[icat],
		  gnum,gra,gdec,gpra,gpdec,gm,gc,gobj,nlog);
    if (ngmax < 0)
	return;

    if (starcount) {
	printf ("%s %d\n", filename, ng);
	return;
	}

    /* Set flag if any proper motions are non-zero
    mprop = 0;
    for (i = 0; i < ng; i++) {
	if (gpra[i] != 0.0 || gpdec[i] != 0.0) {
	    mprop = 1;
	    break;
	    }
	} */

    /* Set flag if spectral type is present */
    if (refcat==SAO || refcat==PPM || refcat==IRAS ||
	refcat==HIP || refcat==BSC || refcat==SDSS)
	sptype = 1;
    else if (refcat == TMPSC || refcat == TMIDR2)
	sptype = 2;
    else if (starcat[icat] != NULL && starcat[icat]->sptype > 0)
	sptype = 1;
    else
	sptype = 0;

    if (refcat == BINCAT || refcat == TABCAT || refcat == TXTCAT)
	nndec = starcat[icat]->nndec;

    /* Find out whether object names are set */
    if (gobj[0] == NULL)
	gobj1 = NULL;
    else
	gobj1 = gobj;

    if (ng > ngmax)
	nbg = ngmax;
    else
	nbg = ng;

    /* Find largest catalog number printed */
    if (starcat[icat] != NULL && starcat[icat]->nnfld != 0)
	nnfld = starcat[icat]->nnfld;
    else if (refcat == SKYBOT) {
	nnfld = 6;
	for (i = 0; i < ng; i++) {
	    lobj = strlen (gobj[i]);
	    if (lobj > nnfld)
		nnfld = lobj;
	    }
	}
    else {
	maxnum = 0.0;
	for (i = 0; i < nbg; i++ ) {
	    if (gnum[i] > maxnum)
		maxnum = gnum[i];
	    }
	nnfld = CatNumLen (refcat, maxnum, nndec);
	}

    /* Check to see if epoch is contained in entries */
    if (starcat[icat] != NULL && starcat[icat]->nepoch != 0)
	printepoch = 1;
    else
	printepoch = 0;

    /* Get image pixel coordinates for each star found in reference catalog */
    for (i = 0; i < nbg; i++ ) {
	offscale = 0;
	wcs2pix (wcs, gra[i], gdec[i], &gx[i], &gy[i], &offscale);
	if (offscale) {
	    gx[i] = 0.0;
	    gy[i] = 0.0;
	    }
	if (debug) {
	    if (starcat[icat] != NULL) {
		if (starcat[icat]->stnum < 0 && gobj1 != NULL) {
		    strncpy (numstr, gobj1[i], 32);
		    if (lofld > 0) {
			for (j = 0; j < lofld; j++) {
			    if (!numstr[j])
				numstr[j] = ' ';
			    }
			}
		    }
		else
		    CatNum (refcat,-nnfld,starcat[icat]->nndec,gnum[i],numstr);
		}
	    else if (refcat == SDSS || refcat == GSC2 || refcat == SKYBOT) {
		strcpy (numstr, gobj[i]);
		lobj = strlen (gobj[i]);
		if (lobj < nnfld) {
		    for (j = lobj; j < nnfld; j++)
			strcat (numstr, " ");
		    }
		}
	    else
		CatNum (refcat, -nnfld, nndec, gnum[i], numstr);
	    ra2str (rastr, 32, gra[i], 3);
	    dec2str (decstr, 32, gdec[i], 2);
	    fprintf (stderr, "%s	%s	%s	%5.2f	%5.2f	%8.2f	%8.2f\n",
		     numstr,rastr,decstr,gm[0][i],gm[1][i],gx[i],gy[i]);
	    }
	}

    /* Check to see whether gc is set at all */
    gcset = 0;
    for (i = 0; i < nbg; i++ ) {
	if (gc[i] != 0) {
	    gcset = 1;
	    break;
	    }
	}

    /* Sort reference stars by brightness (magnitude) */
    MagSortStars (gnum, gra, gdec, gpra, gpdec, gx, gy, gm, gc, gobj1, nbg,
		  nmagmax, sortmag);

    /* List the brightest reference stars */
    CatMagName (sortmag, refcat, magname);
    if (sortmag > 0 && sortmag <= nmag)
	magsort = sortmag - 1;
    else
	magsort = 0;
    if (ng > ngmax) {
	if (verbose || printhead) {
	    if (mag2 > 0.0)
		printf ("%d / %d %s %.1f < %s < %.1f",
			nbg,ng,title,gm[magsort][0],magname,gm[magsort][nbg-1]);
	    else
		printf ("%d / %d %s %s <= %.1f",
			nbg, ng, title, magname, gm[magsort][nbg-1]);
	    }
	}
    else {
	if (verbose || printhead) {
	    if (maglim1 > 0.0)
		printf ("%d %s %.1f < %s < %.1f",
			ng,title,maglim1,magname,maglim2);
	    else if (maglim2 > 0.0)
		printf ("%d %s %s < %.1f",ng, title, magname, maglim2);
	    else
		printf ("%d %s", ng, title);
	    }
	}
    if (printhead || verbose) {
	if (mprop) {
	    if (wcs->epoch != wcs->equinox)
		printf (" at %9.4f",wcs->epoch);
	    if (isiraf (filename))
		printf (" in IRAF image %s\n",filename);
	    else
		printf (" in FITS image %s\n", filename);
	    }
	else
	    printf ("\n");
	}

    /* Sort catalogued objects, if requested */
    if (nbg > 1) {

	/* Sort reference stars by image X coordinate */
	if (catsort == SORT_X)
	    XSortStars (gnum,gra,gdec,gpra,gpdec,gx,gy,gm,gc,gobj1,nbg,
			 nmagmax);

	/* Sort reference stars by image Y coordinate */
	if (catsort == SORT_Y)
	    YSortStars (gnum,gra,gdec,gpra,gpdec,gx,gy,gm,gc,gobj1,nbg,
			 nmagmax);

	/* Sort star-like objects in image by ID number */
	else if (catsort == SORT_ID)
	    IDSortStars (gnum,gra,gdec,gpra,gpdec,gx,gy,gm,gc,gobj1,nbg,
			 nmagmax);

	/* Sort star-like objects in image by right ascenbgion */
	else if (catsort == SORT_RA)
	    RASortStars (gnum,gra,gdec,gpra,gpdec,gx,gy,gm,gc,gobj1,nbg,
			 nmagmax);

	/* Sort star-like objects in image by declination */
	else if (catsort == SORT_DEC)
	    DecSortStars(gnum,gra,gdec,gpra,gpdec,gx,gy,gm,gc,gobj1,nbg,
			 nmagmax);

	/* Sort reference stars from brightest to faintest */
	else if (catsort == SORT_MAG) {
	    MagSortStars(gnum,gra,gdec,gpra,gpdec,gx,gy,gm,gc,gobj1,nbg,
			 nmagmax,sortmag);
	    }
	}

    sprintf (headline, "image	%s", filename);

    /* Open plate catalog file */
    if (wfile && icat == 0) {
	fname = strrchr (filename, '/');
	if (fname != NULL)
	    strcpy (outfile, fname+1);
	else
	    strcpy (outfile, filename);
	for (i = 0; i < ncat; i++) {
	    strcat (outfile,".");
	    strcat (outfile,refcatname[i]);
	    }
	if (region_radius[0])
	    strcat (outfile, ".reg");
	fd = fopen (outfile, "w");
	if (fd == NULL) {
	    fprintf (stderr, "IMCAT:  cannot write file %s\n", outfile);
	    FreeBuffers();
	    wcsfree (wcs);
            return;
	    }
        }

    /* Write region file for SAOimage overplotting */
    if (region_radius[0]) {
	double x, y, ddec, min_mag, max_mag, min_rad, max_rad, rscale;
	int radius, ix, iy;
	char snum[32], rstr[16];
	if (region_radius[icat] == 0) {
	    if (icat > 0)
		region_radius[icat] = region_radius[icat - 1];
	    else
		region_radius[icat] = 20.0 * wcs->xinc / 3600.0;
	    }
	else if (region_radius[icat] < -1)
	    region_radius[icat] = -region_radius[icat] * wcs->xinc / 3600.0;
	if (region_char[icat] == WCS_VAR)
	    strcat (title, " (+ stars, x nonstars)");
	else if (region_radius[icat] > 0 && region_char[icat] > 10) {
	    sprintf (temp, " (%d pixel radius)", region_radius[icat]);
	    strcat (title, temp);
	    }
	else if (region_radius[icat] > 0) {
	    sprintf (temp, " (%d\" radius)", region_radius[icat]);
	    strcat (title, temp);
	    }
	fprintf (fd, "# %s\n", title);
	ddec = (double)region_radius[icat] / 3600.0;
	if (region_radius1[icat] != region_radius[icat]) {
	    max_mag = gm[magsort][0];
	    min_mag = gm[magsort][0];
	    for (i = 0; i < nbg; i++) {
		if (gm[magsort][i] > max_mag) max_mag = gm[magsort][i];
		if (gm[magsort][i] < min_mag) min_mag = gm[magsort][i];
		}
	    if (max_mag == min_mag) {
		min_rad = region_radius[icat];
		rscale = 0;
		}
	    else {
		min_rad = region_radius[icat];
		max_rad = region_radius1[icat];
		rscale = (max_rad - min_rad) / (max_mag - min_mag);
		if (rscale < 0)
		    rscale = -rscale;
		}
	    }
	else {
	    max_mag = gm[magsort][0];
	    min_mag = gm[magsort][0];
	    rscale = 0;
	    }
	if (region_char[icat] == 0) {
	    if (icat > 0)
		region_char[icat] = region_char[icat - 1] + 1;
	    else
		region_char[icat] = WCS_CIRCLE;
	    }
	switch (region_char[icat]) {
	    case WCS_SQUARE:
	    case WCS_PSQUARE:
		strcpy (rstr, "SQUARE");
		break;
	    case WCS_DIAMOND:
	    case WCS_PDIAMOND:
		strcpy (rstr, "DIAMOND");
		break;
	    case WCS_CROSS:
	    case WCS_PCROSS:
		strcpy (rstr, "CROSS");
		break;
	    case WCS_EX:
	    case WCS_PEX:
		strcpy (rstr, "EX");
		break;
	    case WCS_CIRCLE:
	    case WCS_PCIRCLE:
	    default:
		strcpy (rstr, "CIRCLE");
	    }

	for (i = 0; i < nbg; i++) {
	    if (gx[i] > 0.0 && gy[i] > 0.0) {
		if (rscale > 0)
		    radius = (int) ((max_mag - gm[magsort][i]) * rscale);
		else if (region_radius[icat] > 0) {
		    if (region_char[icat] > 10)
			radius = region_radius[icat];
		    else {
			wcs2pix (wcs, gra[i], gdec[i]+ddec, &x, &y, &offscale);
			radius = (int) (sqrt ((x-gx[i])*(x-gx[i]) +
					      (y-gy[i])*(y-gy[i])) + 0.5);
			}
		    }
		else
		    radius = 20;
		ix = (int)(gx[i] + 0.5);
		iy = (int)(gy[i] + 0.5);
		printobj = 0;
		if (obname[icat] && gobj1 != NULL) {
		    if (gobj1[i] != NULL) {
			if (strlen (gobj1[i]) < 32)
			    strcpy (snum, gobj1[i]);
			else
			    strncpy (snum, gobj1[i], 31);
			printobj = 1;
			}
		    else
			CatNum (refcat, 0, -1, gnum[i], snum);
		    }
		else
		    CatNum (refcat, 0, -1, gnum[i], snum);
		if (region_char[icat] == WCS_VAR) {
		    if (gc[i] == 0)
			strcpy (rstr, "CROSS");
		    else
			strcpy (rstr, "EX");
		    }
		if (printobj)
		    fprintf (fd, "%s(%d,%d,%d) # %s\n",
			     rstr, ix, iy, radius, snum);
		else
		    fprintf (fd, "%s(%d,%d,%d) # %s %s\n",
			     rstr, ix, iy, radius, refcatname[icat], snum);
		}
	    }
	if (icat == ncat-1)
	    printf ("%s\n", outfile);
	continue;
	}

    /* Write heading */
    if (wfile)
	fprintf (fd,"%s\n", headline);
    if (tabout)
	printf ("%s\n", headline);

    /* Set degree flag for output */
    if (sysout == WCS_ECLIPTIC || sysout == WCS_GALACTIC)
	degout = 1;
    else
	degout = degout0;
    setlimdeg (degout);

    catalog = CatName (refcat, refcatname[icat]);
    if (wfile)
	fprintf (fd, "catalog	%s\n", catalog);
    if (tabout)
	printf ("catalog	%s\n", catalog);

    if (uplate > 0) {
	sprintf (headline, "plate	%d", uplate);
	if (wfile)
            fprintf (fd, "%s\n", headline);
        if (tabout)
            printf ("%s\n", headline);
        }

    /* Minimum number of plate IDs for USNO-B1.0 catalog */
    if (minid != 0) {
	sprintf (headline, "minid	%d", minid);
	if (wfile)
	    fprintf (fd, "%s\n", headline);
	if (tabout)
	    printf ("%s\n", headline);
	}

    /* Minimum proper motion quality for USNO-B1.0 catalog */
    if (minpmqual > 0) {
	sprintf (headline, "minpmq	%d", minpmqual);
	if (wfile)
	    fprintf (fd, "%s\n", headline);
	if (tabout)
	    printf ("%s\n", headline);
	}

    if (catsort == SORT_RA) {
	sprintf (headline, "rasort	T");
	if (wfile)
	    fprintf (fd, "%s\n", headline);
	if (tabout)
	    printf ("%s\n", headline);
	}

    /* Equinox of output coordinates */
    if (sysout == WCS_J2000)
	sprintf (headline, "radecsys	FK5");
    else if (sysout == WCS_B1950)
	sprintf (headline, "radecsys	FK4");
    else if (sysout == WCS_ECLIPTIC)
	sprintf (headline, "radecsys	ecliptic");
    else if (sysout == WCS_GALACTIC)
	sprintf (headline, "radecsys	galactic");
    else
	sprintf (headline, "radecsys	unknown");
    if (wfile)
	fprintf (fd, "%s\n", headline);
    if (tabout)
	printf ("%s\n", headline);

    /* Equinox of output coordinates */
    sprintf (headline, "equinox	%.4f", eqout);
    if (wfile)
	fprintf (fd, "%s\n", headline);
    if (tabout)
	printf ("%s\n", headline);

    /* Epoch of output coordinates from image header time of observation */
    sprintf (headline, "epoch	%.4f", epout);
    if (wfile)
	fprintf (fd, "%s\n", headline);
    if (tabout)
	printf ("%s\n", headline);

    /* Proper Motion units, if proper motion is in this catalog */
    if (mprop == 1) {
	if (wfile) {
	    fprintf (fd, "rpmunit	mas/year\n");
	    fprintf (fd, "dpmunit	mas/year\n");
	    }
	else if (tabout) {
	    printf ("rpmunit	mas/year\n");
	    printf ("dpmunit	mas/year\n");
	    }
	}

    sprintf (headline, "program	%s %s", progname, RevMsg);
    if (wfile)
	fprintf (fd, "%s\n", headline);
    if (tabout)
	printf ("%s\n", headline);

    if (refcat == TABCAT && starcat[icat]->keyid[0] >0) {
	strcpy (headline, starcat[icat]->keyid);
	strcat (headline, "                ");
	}
    else if (refcat == SKYBOT) {
	strcpy (headline, "object");
	for (i = 6; i < nnfld; i++)
	    strcat (headline, " ");
	}
    else
	CatID (headline, refcat);
    headline[nnfld] = (char) 0;
    strcat (headline, "	");
    if (sysout == WCS_B1950)
	strcat (headline,"ra1950   	dec1950      	");
    else if (sysout == WCS_ECLIPTIC)
	strcat (headline,"long_ecl 	lat_ecl       	");
    else if (sysout == WCS_GALACTIC)
	strcat (headline,"long_gal  	lat_gal       	");
    else
	strcat (headline,"ra      	dec           	");
    /* if (refcat == UCAC2 || refcat == UCAC3)
	strcat (headline,"raerr 	decerr	"); */
    if (refcat == UAC  || refcat == UA1  || refcat == UA2 ||
	refcat == USAC || refcat == USA1 || refcat == USA2)
	strcat (headline,"magb  	magr  	");
    else if (refcat==TYCHO || refcat==TYCHO2 || refcat==HIP || refcat==ACT)
	strcat (headline,"magb  	magv  	");
    else if (refcat==TYCHO2E)
	strcat (headline,"magb  	magv  	magbe	magve	");
    else if (refcat ==IRAS)
	strcat (headline,"f10m  	f25m  	f60m  	f100m 	");
    else if (refcat == GSC2)
	strcat (headline,"magf  	magj 	magv	magn	");
    else if (refcat == UCAC2)
	strcat (headline,"magj 	magh 	magk 	magc 	");
    else if (refcat == UCAC3)
	strcat (headline,"magb 	magr 	magi 	magj 	magh 	magk 	magm 	maga 	");
    else if (refcat == UB1)
	strcat (headline,"magb1 	magr1	magb2	magr2	magn 	");
    else if (refcat == YB6)
	strcat (headline,"magb  	magr 	magj 	magh 	magk 	");
    else if (refcat == TMPSC || refcat == TMIDR2)
	strcat (headline,"magj   	magh   	magk   	");
    else if (refcat == TMPSCE)
	strcat (headline,"magj   	magh   	magk   	magje	maghe	magke	");
    else if (refcat == TMXSC)
	strcat (headline,"magj   	magh   	magk  	");
    else if (refcat == TMXSC)
	strcat (headline,"magj   	magh   	magk  	");
    else if (refcat == SKYBOT)
	strcat (headline, "magv  	gdist 	hdist 	");
    else if (refcat == SDSS)
	strcat (headline, "magu   	magg   	magr   	magi   	magz    ");
    else {
	for (imag = 0; imag < nmag; imag++) {
	    if (printepoch && imag == nmag-1)
		sprintf (temp, "epoch     	");
	    else if (starcat[icat] != NULL &&
		strlen (starcat[icat]->keymag[imag]) > 0)
		sprintf (temp, "%s 	", starcat[icat]->keymag[imag]);
	    else if ((!printepoch && nmag > 1) || (printepoch && nmag > 2))
		sprintf (temp, "mag%d 	", imag);
	    else
		sprintf (temp, "mag  	");
	    strcat (headline, temp);
	    }
	}

    if (refcat == HIP)
	strcat (headline,"parlx	parer	");
    else if (refcat == IRAS)
	strcat (headline,"f60m 	f100m	");
    else if (refcat == UAC  || refcat == UA1  || refcat == UA2 ||
	refcat == USAC || refcat == USA1 || refcat == USA2 || refcat == UJC)
	strcat (headline,"plate	");
    else if (refcat == GSC || refcat == GSCACT)
	strcat (headline,"class 	band	N	");
    else if (refcat == UB1)
	strcat (headline,"pm 	ni	sg	");
    else if (refcat == TMXSC)
	strcat (headline,"size  	");
    else if (sptype == 1)
	strcat (headline,"type	");
    else if (gcset && refcat != UCAC2 && refcat != UCAC3)
	strcat (headline,"peak	");
    if (mprop)
	strcat (headline, "pmra 	pmdec	");
    /* if (refcat == UCAC2 || refcat == UCAC3)
	strcat (headline, "epmra 	epmdec"); */
    if (refcat == UCAC2 || refcat == UCAC3)
	strcat (headline, "ni	nc	");
    if (refcat == GSC2)
	strcat (headline,"class	");
    strcat (headline,"x    	y    ");
    if (refcat == TABCAT && keyword != NULL) {
	strcat (headline,"	");
	strcat (headline, keyword);
	}
    if (wfile)
	fprintf (fd, "%s\n", headline);
    if (tabout)
	printf ("%s\n", headline);

    strcpy (headline,"--------------------------------");	/* ID number */
    headline[nnfld] = (char) 0;
    strcat (headline, "	-----------	------------");/* RA Dec */
    /* if (refcat == UCAC2 || refcat == UCAC3)
	strcat (headline,"	------	------"); */	/* RA, Dec error */
    strcat (headline, "	-----");	/* First Mag */
    if (refcat == UAC  || refcat == UA1  || refcat == UA2 || 
	refcat == USAC || refcat == USA1 || refcat == USA2 || refcat == TYCHO ||
	refcat == TYCHO2 || refcat == ACT)
	strcat (headline,"	-----");		/* Second magnitude */
    else if (refcat == TYCHO2E)		/* Second magnitude + errors */
	strcat (headline,"	-----	-----	-----");
    else if (refcat == TMPSC || refcat == TMIDR2) /* JHK Magnitudes */
	strcat (headline,"--	-------	-------");
    else if (refcat == TMPSCE) /* JHK Magnitudes + errors */
	strcat (headline,"--	-------	-------	-----	-----	-----");
    else if (refcat == TMXSC)
	strcat (headline,"--	-------	-------"); /* JHK Magnitudes + size */
    else if (refcat == IRAS)
	strcat (headline,"-	------	------	------"); /* 4 fluxes */
    else if (refcat == GSC2)
	strcat (headline,"	-----	-----	-----	-----"); /* 4 magnitudes */
    else if (refcat == SKYBOT)
	strcat (headline,"	-----	-----	-----"); /* 1 magnitude, 2 distances */
    else if (refcat == HIP)
	strcat (headline,"	-----	-----	-----"); /* 3 magnitudes */
    else if (refcat == UB1 || refcat == YB6)
	strcat (headline,"	-----	-----	-----	-----");
    else {
	for (imag = 1; imag < nmag; imag++) {
	    if (printepoch && imag == nmag-1)
		sprintf (temp, "	----------");
	    else
		sprintf (temp, "	-----");
	    strcat (headline, temp);
	    }
	}

    if (refcat == GSC || refcat == GSCACT)
	strcat (headline,"	-----	----	-");	/* class, band, n */
    else if (refcat == UB1)
	strcat (headline,"	--	--	--");
    else if (refcat == TMXSC)
	strcat (headline,"	------");
    else if (gcset && refcat != UCAC2 && refcat != UCAC3)
	strcat (headline, "	-----");		/* plate or peak */
    if (mprop )
	strcat (headline, "	------	------");	/* Proper motion */
    if (refcat == UCAC2 || refcat == UCAC3)
	strcat (headline, "	--	--");
    if (refcat == GSC2)
	strcat (headline,"	-----");		/* GSC2 object class */
    strcat (headline, "	------	------");		/* X and Y */
    if (refcat == TABCAT && keyword != NULL)
	strcat (headline,"	------");		/* Additional keyword */
    if (wfile)
	fprintf (fd, "%s\n", headline);
    if (tabout)
	printf ("%s\n", headline);
    if (printhead) {
	if (nbg == 0)
	    printf ("No %s Found\n", title);
	else {
	    if (refcat == TABCAT && strlen(starcat[icat]->keyid) > 0)
		printf ("%s          ", starcat[icat]->keyid);
	    else if (refcat == SKYBOT) {
		strcpy (headline, "Object");
		for (i = 6; i < nnfld-1; i++)
		    strcat (headline, " ");
		}
	    else
		CatID (headline, refcat);
	    headline[nnfld] = (char) 0;
	    printf ("%s", headline);

	    if (sysout == WCS_B1950) {
		if (degout) {
		    if (eqout == 1950.0)
			printf ("  RA1950   Dec1950  ");
		    else
			printf (" RAB%7.2f DecB%7.2f  ", eqout, eqout);
		    }
		else {
		    if (eqout == 1950.0)
			printf ("  RAB1950      DecB1950    ");
		    else
			printf (" RAB%7.2f   DecB%7.2f  ", eqout, eqout);
		    }
		}
	    else if (sysout == WCS_ECLIPTIC)
		printf ("Ecl Lon    Ecl Lat  ");
	    else if (sysout == WCS_GALACTIC)
		printf ("Gal Lon    Gal Lat  ");
	    else {
		if (degout) {
		    if (eqout == 2000.0)
			printf ("  RA2000   Dec2000  ");
		    else
			printf (" RAJ%7.2f  DecJ%7.2f ", eqout, eqout);
		    }
		else {
		    if (eqout == 2000.0)
			printf ("  RA2000        Dec2000    ");
		    else
			printf (" RAJ%7.2f   DecJ%7.2f  ", eqout, eqout);
		    }
		}
	    if (refcat == UAC  || refcat == UA1  || refcat == UA2 ||
		refcat == USAC || refcat == USA1 || refcat == USA2)
		printf ("MagB  MagR Plate   X      Y   \n");
	    else if (refcat == UJC)
		printf ("  Mag Plate   X      Y   \n");
	    else if (refcat == GSC || refcat == GSCACT)
		printf ("  Mag Class Band N    X       Y   \n");
	    else if (refcat == GSC2)
		printf ("MagF  MagJ  MagV  MagN   Class   X       Y   \n");
	    else if (refcat == UCAC2)
		printf (" MagJ   MagH   MagK   MagC   NIm NCt   X      Y   \n");
	    else if (refcat == UCAC3)
		printf (" MagB   MagR   MagI   MagJ   MagH   MagK   MagM   MagA   NIm NCt   X      Y   \n");
	    else if (refcat == UB1)
		printf ("MagB1 MagR1 MagB2 MagR2 MagN  PM NI SG    X       Y   \n");
	    else if (refcat == YB6)
		printf ("MagB  MagR  MagJ  MagH  MagK    X       Y   \n");
	    else if (refcat == SDSS)
		printf ("Magu  Magg  Magr  Magi  Magz    X       Y   \n");
	    else if (refcat == IRAS)
		printf ("f10m  f25m  f60m  f100m   X       Y   \n");
	    else if (refcat == HIP)
		printf ("MagB  MagV  parlx parer   X       Y   \n");
	    else if (refcat == TMPSC || refcat == TMIDR2)
		printf ("MagJ    MagH    MagK      X       Y   \n");
	    else if (refcat == SKYBOT)
		printf (" MagV  GDist HDist   X      Y   \n");
	    else if (refcat == TMPSCE)
		printf ("MagJ    MagH    MagK   MagJe MagHe MagKe   X       Y   \n");
	    else if (refcat == TMXSC)
		printf ("MagJ    MagH    MagK     Size     X       Y   \n");
	    else if (refcat == SAO || refcat == PPM || refcat == BSC)
		printf ("  Mag  Type   X       Y     \n");
	    else if (refcat==TYCHO || refcat==TYCHO2 || refcat==ACT)
		printf ("MagB   MagV    X       Y     \n");
	    else if (refcat==TYCHO2E)
		printf ("MagB   MagV   MagBe MagVe  X       Y     \n");
	    else if (refcat == TABCAT) {
		for (imag = 0; imag < nmag; imag++) {
		    if (printepoch && imag == nmag-1)
			sprintf (temp, "    epoch   ");
		    else if (starcat[icat] != NULL &&
			strlen (starcat[icat]->keymag[imag]) > 0)
			sprintf (temp, "    %s ", starcat[icat]->keymag[imag]);
		    else if ((!printepoch && nmag > 1) || (printepoch && nmag > 2))
			sprintf (temp, "    mag%d ", imag);
		    else
			sprintf (temp, "    mag  ");
		    strcat (headline, temp);
		    }
		if (refcat == UCAC2 || refcat == UCAC3) {
		    nim = gc[i] / 1000;
		    nct = gc[i] % 1000;
		    sprintf (temp, "  ni  nc", nim, nct);
		    strcat (headline, temp);
		    }
		else if (gcset)
		    printf (" Peak ");
		printf (" X      Y  ");
		if (keyword != NULL)
		    printf ("   %s\n", keyword);
		}
	    else if (refcat == BINCAT)
		printf (" Mag   Type   X      Y     Object\n");
	    else if (refcat == TXTCAT)
		printf (" Mag     X      Y     Object\n");
	    else if (gcset)
		printf (" Mag  Peak     X       Y   \n");
	    else
		printf (" Mag     X       Y   \n");
	    }
	}

    /* Print positions from reference catalog */
    for (i = 0; i < nbg; i++) {
	if (gx[i] > 0.0 && gy[i] > 0.0 && gx[i] < gxmax && gy[i] < gymax) {
	    if (sptype == 1) {
	    	isp[0] = gc[i] / 1000;
		isp[1] = gc[i] % 1000;
		}
	    if (refcat == GSC || refcat == GSCACT) {
		ngsc = gc[i] / 10000;
		gc[i] = gc[i] - (ngsc * 10000);
		band = gc[i] / 100;
		gc[i] = gc[i] - (band * 100);
		}

	    /* Set up object name or number to print */
	    if (starcat[icat] != NULL) {
		if (starcat[icat]->stnum < 0 && gobj1 != NULL) {
		    strncpy (numstr, gobj1[i], 32);
		    if (lofld > 0) {
			for (j = 0; j < lofld; j++) {
			    if (!numstr[j])
				numstr[j] = ' ';
			    }
			}
		    }
		else
		    CatNum (refcat,-nnfld,starcat[icat]->nndec,gnum[i],numstr);
		}
	    else if (refcat == SDSS || refcat == GSC2 || refcat == SKYBOT) {
		strcpy (numstr, gobj[i]);
		lobj = strlen (gobj[i]);
		if (lobj < nnfld) {
		    for (j = lobj; j < nnfld; j++)
			strcat (numstr, " ");
		    }
		}
	    else
		CatNum (refcat, -nnfld, nndec, gnum[i], numstr);

	    if (degout) {
		deg2str (rastr, 32, gra[i], 5);
		deg2str (decstr, 32, gdec[i], 5);
		}
	    else {
		ra2str (rastr, 32, gra[i], 3);
		dec2str (decstr, 32, gdec[i], 2);
		}
	    if (tabout || wfile) {
		if (refcat == GSC || refcat == GSCACT)
		    sprintf (headline, "%s	%s	%s	%5.2f	%d	%d	%d",
		     numstr, rastr, decstr, gm[0][i], gc[i], band, ngsc);
		else if (refcat == GSC2)
		    sprintf (headline, "%s	%s	%s	%5.2f	%5.2f	%5.2f	%5.2f	%d",
		     numstr,rastr,decstr,gm[0][i],gm[1][i],gm[2][i],gm[3][i], gc[i]);
		else if (refcat == HIP)
		    sprintf (headline, "%s	%s	%s	%5.2f	%5.2f	%5.2f	%5.2f",
		     numstr,rastr,decstr,gm[0][i],gm[1][i],gm[2][i],gm[3][i]);
		else if (refcat == UB1)
		    sprintf (headline, "%s	%s	%s	%5.2f	%5.2f	%5.2f	%5.2f	%5.2f	%2d	%2d	%2d",
		     numstr,rastr,decstr,gm[0][i],gm[1][i],gm[2][i],gm[3][i],
		     gm[4][i],gc[i]%10000/100,gc[i]%100,gc[i]/10000);
		else if (refcat == YB6 || refcat == SDSS)
		    sprintf (headline, "%s	%s	%s	%5.2f	%5.2f	%5.2f	%5.2f	%5.2f",
		     numstr,rastr,decstr,gm[0][i],gm[1][i],gm[2][i],gm[3][i],
		     gm[4][i]);
		else if (refcat == TMPSC || refcat == TMIDR2 ||
			 refcat == TMPSCE || refcat == TMXSC) {
		    sprintf (headline, "%s	%s	%s", numstr, rastr, decstr);
		    for (imag = 0; imag < 3; imag++) {
			if (gm[imag][i] > 100.0)
			    sprintf (temp, "	%6.3fL", gm[imag][i]-100.0);
			else
			    sprintf (temp, "	%6.3f ", gm[imag][i]);
			strcat (headline, temp);
			}
		    if (refcat == TMPSCE) {
			for (imag = 3; imag < 6; imag++) {
			    sprintf (temp, "	%6.3f ", gm[imag][i]);
			    strcat (headline, temp);
			    }
			}
		    if (refcat == TMXSC) {
			sprintf (temp,"	%6.1f", ((double)gc[i])* 0.1);
			strcat (headline, temp);
			}
		    }
		else if (refcat == IRAS) {
		    sprintf (headline, "%s	%s	%s", numstr, rastr, decstr);
		    for (imag = 0; imag < 4; imag++) {
			if (gm[imag][i] > 100.0) {
			    flux = 1000.0 * pow (10.0,-(gm[imag][i]-100.0)/2.5);
			    sprintf (temp, "	%.2fL", flux);
			    }
			else {
			    flux = 1000.0 * pow (10.0, -gm[imag][i] / 2.5);
			    sprintf (temp, "	%.2f ", flux);
			    }
			strcat (headline, temp);
			}
		    }
		else if (refcat == UAC  || refcat == UA1  || refcat == UA2 ||
			 refcat == USAC || refcat == USA1 || refcat == USA2)
		    sprintf (headline, "%s	%s	%s	%5.1f	%5.1f	%d",
		     numstr,rastr,decstr,gm[0][i],gm[1][i],gc[i]);
		else if (refcat == SKYBOT)
		    sprintf (headline, "%s	%s	%s	%5.2f	%5.2f	%5.2f	%5.2f",
		     numstr,rastr,decstr,gm[0][i],gm[1][i],gm[2][i],gm[3][i]);
		else if (refcat == UJC)
		    sprintf (headline, "%s	%s	%s	%5.2f	%d",
		     numstr, rastr, decstr, gm[0][i], gc[i]);
		else if (refcat==SAO || refcat==PPM || refcat== BSC )
		    sprintf (headline, "%s	%s	%s	%5.2f	%2s",
		     numstr,rastr,decstr,gm[0][i],isp);
		else if (refcat==TYCHO || refcat==TYCHO2 || refcat==ACT)
		    sprintf (headline, "%s	%s	%s	%5.2f	%5.2f",
		     numstr,rastr,decstr,gm[0][i],gm[1][i]);
		else if (refcat==TYCHO2E)
		    sprintf (headline, "%s	%s	%s	%5.2f	%5.2f	%5.2f	%5.2f",
		     numstr,rastr,decstr,gm[0][i],gm[1][i],gm[2][i],gm[3][i]);
		else {
		    sprintf (headline, "%s	%s	%s",
			     numstr, rastr, decstr);
		    if (tabout && (refcat == UCAC2 || refcat == UCAC3)) {
			era = gm[nmag][i] * cosdeg (gdec[i]) * 3600.0;
			edec = gm[nmag+1][i] * 3600.0;
			sprintf (temp, "	%5.3f	%5.3f");
			strcat (headline, temp);
			}
		    for (imag = 0; imag < nmag; imag++) {
			if (printepoch && imag == nmag-1) {
			    temp1 = ep2fd (gm[nmag-1][i]);
			    if (strlen (temp1) > 10)
				temp1[10] = (char) 0;
			    sprintf (temp, "	%10s", temp1);
			    free (temp1);
			    }
			else if (gm[imag][i] > 100.0)
			    sprintf (temp, "	%5.2fL",gm[imag][i]-100.0);
			else
			    sprintf (temp, "	%5.2f",gm[imag][i]);
			strcat (headline, temp);
			}
		    if (gcset && refcat != UCAC2 && refcat != UCAC3) {
			sprintf (temp, "	%d", gc[i]);
			strcat (headline, temp);
			}
		    if (sptype == 1) {
			sprintf (temp, "	%s", isp);
			strcat (headline, temp);
			}
		    }
		if (mprop) {
		    pra = gpra[i] * 3600000.0 * cosdeg (gpdec[i]);
		    pdec = gpdec[i] * 3600000.0;
		    sprintf (temp, "	%5.1f	%5.1f", pra,pdec);
		    strcat (headline, temp);
		    }
		if (refcat == UCAC2 || refcat == UCAC3) {
		    nim = gc[i] / 1000;
		    nct = gc[i] % 1000;
		    if (tabout) {
			epmr = gm[nmag+2][i] * cosdeg (gdec[i]) * 3600000.0;
			epmd = gm[nmag+3][i] * 3600000.0;
			sprintf (temp, "	%5.1f	%5.1f	%2d	%2d",
				 epmr, epmd, nim, nct);
			}
		    else
			sprintf (temp, "	%2d	%2d", nim, nct);
		    strcat (headline, temp);
		    }

		sprintf (temp, "	%.2f	%.2f",
			 gx[i],gy[i]);
		strcat (headline, temp);
		if (wfile)
		    fprintf (fd, "%s\n", headline);
		if (tabout)
		    printf ("%s\n", headline);
		}
	    else if (!tabout) {
		if (refcat == USAC || refcat == USA1 || refcat == USA2 ||
		    refcat == UAC  || refcat == UA1  || refcat == UA2)
		    sprintf (headline,"%s %s %s %5.1f %5.1f %4d",
			numstr,rastr,decstr,gm[0][i],gm[1][i],gc[i]);
		else if (refcat == UJC)
		    sprintf (headline,"%s %s %s %6.2f %4d",
			numstr, rastr, decstr, gm[0][i], gc[i]);
		else if (refcat == GSC || refcat == GSCACT)
		    sprintf (headline,"%s %s %s %6.2f %4d %4d %2d",
			numstr, rastr, decstr, gm[0][i], gc[i], band, ngsc);
		else if (refcat == TMPSC || refcat == TMIDR2 ||
			 refcat == TMPSCE || refcat == TMXSC) {
		    sprintf (headline, "%s %s %s", numstr, rastr, decstr);
		    for (imag = 0; imag < 3; imag++) {
			if (gm[imag][i] > 100.0)
			    sprintf (temp, " %6.3fL", gm[imag][i]-100.0);
			else
			    sprintf (temp, " %6.3f ", gm[imag][i]);
			strcat (headline, temp);
			}
		    if (refcat == TMPSCE) {
			for (imag = 3; imag < 6; imag++) {
			    sprintf (temp, " %6.3f ", gm[imag][i]);
			    strcat (headline, temp);
			    }
			}
		    if (refcat == TMXSC) {
			sprintf (temp," %6.1f", ((double)gc[i])* 0.1);
			strcat (headline, temp);
			}
		    }
		else if (refcat == GSC2)
		    sprintf (headline,"%s %s %s %5.2f %5.2f %5.2f %5.2f   %2d  ",
			     numstr,rastr,decstr,gm[0][i],gm[1][i],gm[2][i],gm[3][i],gc[i]);
		else if (refcat == SKYBOT)
		    sprintf (headline,"%s %s %s %5.2f %5.2f %5.2f",
			     numstr,rastr,decstr,gm[0][i],gm[1][i],gm[2][i]);
		else if (refcat == HIP)
		    sprintf (headline,"%s %s %s %5.2f %5.2f %5.2f %5.2f",
			     numstr,rastr,decstr,gm[0][i],gm[1][i],gm[2][i],gm[3][i]);
		else if (refcat == UB1)
		    sprintf (headline,"%s %s %s %5.2f %5.2f %5.2f %5.2f %5.2f %2d %2d %2d",
			     numstr,rastr,decstr,gm[0][i],gm[1][i],gm[2][i],
			     gm[3][i],gm[4][i],gc[i]%10000/100,gc[i]%100,gc[i]/10000);
		else if (refcat == YB6)
		    sprintf (headline,"%s %s %s %5.2f %5.2f %5.2f %5.2f %5.2f",
			     numstr,rastr,decstr,gm[0][i],gm[1][i],gm[2][i],
			     gm[3][i],gm[4][i]);
		else if (refcat == SDSS)
		    sprintf (headline,"%s %s %s %5.2f %5.2f %5.2f %5.2f %5.2f %2d",
			     numstr,rastr,decstr,gm[0][i],gm[1][i],gm[2][i],
			     gm[3][i],gm[4][i],gc[i]);
		else if (refcat==IRAS) {
		    sprintf (headline, "%s %s %s", numstr, rastr, decstr);
		    for (imag = 0; imag < 3; imag++) {
			if (gm[imag][i] > 100.0) {
			    flux = 1000.0 * pow (10.0, -(gm[imag][i]-100.0) / 2.5);
			    sprintf (temp, " %5.2fL", flux);
			    }
			else {
			    flux = 1000.0 * pow (10.0, -gm[imag][i] / 2.5);
			    sprintf (temp, " %5.2f ", flux);
			    }
			strcat (headline, temp);
			}
		    }
		else if (refcat==SAO || refcat==PPM || refcat== BSC)
		    sprintf (headline,"%s  %s %s %6.2f  %2s",
			numstr,rastr,decstr,gm[0][i],isp);
		else if (refcat==TYCHO || refcat==TYCHO2 || refcat==ACT)
		    sprintf (headline,"%s %s %s %6.2f %6.2f",
			     numstr,rastr,decstr,gm[0][i],gm[1][i]);
		else if (refcat==TYCHO2E)
		    sprintf (headline,"%s %s %s %6.2f %6.2f %5.2f %5.2f",
			     numstr,rastr,decstr,gm[0][i],gm[1][i],gm[2][i],gm[3][i]);
		else {
		    sprintf (headline,"%s %s %s",
			     numstr,rastr,decstr);
		    for (imag = 0; imag < nmag; imag++) {
			if (printepoch && imag == nmag-1) {
			    temp1 = ep2fd (gm[nmag-1][i]);
			    if (strlen (temp1) > 10)
				temp1[10] = (char) 0;
			    sprintf (temp, "	%10s", temp1);
			    free (temp1);
			    }
			else if (gm[imag][i] > 100.0)
			    sprintf (temp, " %5.2fL",gm[imag][i]-100.0);
			else
			    sprintf (temp, " %5.2f ",gm[imag][i]);
			strcat (headline, temp);
			}
		    if (sptype == 1) {
			sprintf (temp, " %2s", isp);
			strcat (headline, temp);
			}
		    if (refcat == TABCAT && gcset) {
			sprintf(temp,"	%d", gc[i]);
			strcat (headline, temp);
			}
		    }
		if (refcat == UCAC2 || refcat == UCAC3) {
		    nim = gc[i] / 1000;
		    nct = gc[i] % 1000;
		    sprintf (temp, "  %2d  %2d", nim, nct);
		    strcat (headline, temp);
		    }

		/* Add image pixel coordinates to output line */
		if (wcs->nxpix < 1000.0 && wcs->nypix < 1000.0)
		    sprintf (temp, " %6.2f %6.2f", gx[i], gy[i]);
		else if (wcs->nxpix < 10000.0 && wcs->nypix < 10000.0)
		    sprintf (temp, " %6.1f %6.1f", gx[i], gy[i]);
		else
		    sprintf (temp, " %.1f %.1f", gx[i], gy[i]);
		strcat (headline, temp);

		/* Add object name to output line */
		if (refcat == TABCAT && keyword != NULL) {
		    if (gobj[i] != NULL)
			sprintf (temp, " %s", gobj[i]);
		    else
			sprintf (temp, " ___");
		    strcat (headline, temp);
		    }
		else if ((refcat == BINCAT || refcat == TXTCAT) &&
			 gobj1 != NULL) {
		    if (gobj[i] != NULL)
			sprintf (temp, " %s", gobj[i]);
		    else
			sprintf (temp, " ___");
		    strcat (headline, temp);
		    }
		printf ("%s\n", headline);
		}
	    }
	}

	/* If searching more than one catalog, separate them with blank line */
	if (ncat > 0 && icat < ncat-1)
	    printf ("\n");

	/* Free memory used for object names in current catalog */
	if (gobj1 != NULL) {
	    for (i = 0; i < nbg; i++) {
		if (gobj[i] != NULL) {
		    free (gobj[i]);
		    gobj[i] = NULL;
		    }
		}
	    }
	}

    if (wfile)
	fclose (fd);
    wcsfree (wcs);

    return;
}

/* Set up limits for search */
static void
ImageLim (wcs, cra, cdec, dra, ddec, ramin, ramax, decmin, decmax)

struct WorldCoor *wcs;		/* WCS parameter structure */
double	*cra, *cdec;		/* Center of search area  in degrees (returned) */
double	*dra, *ddec;		/* Horizontal and vertical half-widths in degrees (returned) */
double	*ramin, *ramax;		/* Right ascension limits in degrees (returned) */
double	*decmin, *decmax;	/* Declination limits in degrees (returned) */

{
    double xmin = 0.5;
    double xmax = wcs->nxpix + 0.5;
    double xcen = 0.5 + (wcs->nxpix * 0.5);
    double ymin = 0.5;
    double ymax = wcs->nypix + 0.5;
    double ycen = 0.5 + (wcs->nypix * 0.5);
    double ra[8], dec[8];
    int i;

    /* Find sky coordinates of corners and middles of sides */
    pix2wcs (wcs, xmin, ymin, &ra[0], &dec[0]);
    pix2wcs (wcs, xmin, ycen, &ra[1], &dec[1]);
    pix2wcs (wcs, xmin, ymax, &ra[2], &dec[2]);
    pix2wcs (wcs, xcen, ymin, &ra[3], &dec[3]);
    pix2wcs (wcs, xcen, ymax, &ra[4], &dec[4]);
    pix2wcs (wcs, xmax, ymin, &ra[5], &dec[5]);
    pix2wcs (wcs, xmax, ycen, &ra[6], &dec[6]);
    pix2wcs (wcs, xmax, ymax, &ra[7], &dec[7]);

    /* Find minimum and maximum right ascensions watch for wrap-around */
    if (wcs->rot > 315.0 || wcs->rot <= 45.0) {
	*ramin = ra[5];
	if (ra[6] < *ramin)
	    *ramin = ra[6];
	if (ra[7] < *ramin)
	    *ramin = ra[7];
	*ramax = ra[0];
	if (ra[1] > *ramax)
	   *ramax = ra[1];
	if (ra[2] > *ramax)
	   *ramax = ra[2];
	}
    else if (wcs->rot > 45.0 && wcs->rot <= 135.0) {
	*ramin = ra[0];
	if (ra[3] < *ramin)
	    *ramin = ra[3];
	if (ra[5] < *ramin)
	    *ramin = ra[5];
	*ramax = ra[1];
	if (ra[2] > *ramax)
	   *ramax = ra[2];
	if (ra[4] > *ramax)
	   *ramax = ra[4];
	if (ra[7] > *ramax)
	   *ramax = ra[7];
	}
    else if (wcs->rot > 225.0 && wcs->rot <= 315.0) {
	*ramin = ra[0];
	if (ra[3] < *ramin)
	    *ramin = ra[3];
	if (ra[5] < *ramin)
	    *ramin = ra[5];
	*ramax = ra[2];
	if (ra[4] > *ramax)
	   *ramax = ra[4];
	if (ra[7] > *ramax)
	   *ramax = ra[7];
	}
    else {
	*ramin = ra[2];
	if (ra[4] < *ramin)
	   *ramin = ra[4];
	if (ra[7] < *ramin)
	   *ramin = ra[7];
	*ramax = ra[0];
	if (ra[3] > *ramax)
	    *ramax = ra[3];
	if (ra[5] > *ramax)
	    *ramax = ra[5];
	}

    /* Find minimum and maximum declinations */
    *decmin = dec[0];
    *decmax = dec[0];
    for (i = 0; i < 8; i++) {
	if (dec[i] < *decmin)
	   *decmin = dec[i];
	if (dec[i] > *decmax)
	   *decmax = dec[i];
	}

    /* Set center and extent */
    if (*ramin > *ramax) {
	*cra = 0.5 * (*ramin - 360.0 + *ramax);
	*dra = 0.5 * (*ramax - (*ramin - 360.0));
	}
    else {
	*cra = 0.5 * (*ramin + *ramax);
	*dra = 0.5 * (*ramax - *ramin);
	}
    *cdec = 0.5 * (*decmin + *decmax);
    *ddec = 0.5 * (*decmax - *decmin);

    return;
}

/* May 21 1996	New program
 * Jul 11 1996	Update file reading
 * Jul 16 1996	Remove unused image pointer; do not free unallocated header
 * Aug 15 1996	Clean up file reading code
 * Sep  4 1996	Free header immediately after use
 * Oct  1 1996	Set file extension according to the catalog which is used
 * Oct  1 1996	Write output file only if flag is set
 * Oct 15 1996	Use GetFITSWCS instead of local code
 * Oct 16 1996	Write list of stars to standard output by default
 * Nov 13 1996	Add UA catalog reading capabilities
 * Nov 15 1996	Change catalog reading subroutine arguments
 * Dec 10 1996	Change equinox in getfitswcs call to double
 * Dec 12 1996	Version 1.2
 * Dec 12 1996	Add option for bright as well as faint magnitude limits
 * Dec 12 1996	Fix header for UAC magnitudes
 * Dec 13 1996	Write plate into header if selected
 *
 * Jan 10 1997	Fix bug in RASort Stars which did not sort magnitudes
 * Feb 21 1997	Get image header from GetFITSWCS()
 * Mar 14 1997	Add support for USNO SA-1.0 catalog
 * Apr 25 1997	Fix bug in uacread
 * May 28 1997	Add option to read a list of filenames from a file
 * Nov 17 1997	Initialize both magnitude limits
 * Dec  8 1997	Set up program to be called by various names
 * Dec 15 1997	Add capability of reading and writing IRAF 2.11 images
 *
 * Jan 27 1998	Implement Mark Calabretta's WCSLIB
 * Jan 27 1998	Add -z for AIPS classic WCS projections
 * Feb 18 1998	Version 2.0: Full Calabretta WCS implementation
 * Mar 27 1998	Version 2.1: Add IRAF TNX projection
 * Apr 14 1998	Version 2.2: Add polynomial plate fit
 * Apr 24 1998	change coordinate setting to setsys() from setfk4()
 * Apr 28 1998	Change coordinate system flags to WCS_*
 * May 13 1998	If nstars is set use it as a limit no matter how small
 * May 27 1998	Do not include fitshead.h
 * Jun  2 1998	Fix bug in tabread()
 * Jun 24 1998	Add string lengths to ra2str() and dec2str() calls
 * Jun 25 1998	Set WCS subroutine choice with SETDEFWCS()
 * Jul  8 1998	Add other coordinate types
 * Jul  9 1998	Adjust report headings
 * Sep 10 1998	Add SAOTDC binary format catalogs
 * Sep 16 1998	Add coordinate system and equinox to binread()
 * Sep 17 1998	Add coordinate system to GetFITSWCS()
 * Sep 21 1998	Add epoch to heading
 * Sep 25 1998	Add system, equinox, and epoch to catalog search calls
 * Oct  9 1998	Add option to write SAOimage region file
 * Oct  9 1998	Add option to read arbitrary SAO binary catalog file
 * Oct  9 1998	Add source ID in comment after SAOimage region output
 * Oct 13 1998	Make sure refcatname is always set
 * Oct 14 1998	Use isiraf() to determine file type
 * Oct 15 1998	Add TDC ASCII catalog access
 * Oct 19 1998	Add variable SAOimage region shapes
 * Oct 19 1998	Add magnitude-scaled SAOimage region size
 * Oct 21 1998	Add object name to binary catalogs
 * Oct 22 1998	Use RefCat() to set type of catalog name
 * Oct 23 1998	Allow searches of multiple catalogs into one output file
 * Oct 26 1998	Return object name in same operation as object position
 * Oct 27 1998	Move region shape codes to wcscat.h
 * Oct 29 1998	Add GSC class to output header
 * Oct 29 1998	Add tab table keyword to output
 * Nov 20 1998	Add support for USNO A-2.0 and SA-2.0 catalogs
 * Nov 30 1998	Add x command for new reference pixel
 * Nov 30 1998	Add version and help commands for consistency
 * Dec  8 1998	Add support for Hipparcos and ACT catalogs
 * Dec 21 1998	Fix formats for text catalogs
 * Dec 21 1998	Write output file to current working directory
 *
 * Jan 25 1999	Add -i for IRAF formatted output (X Y RA Dec)
 * Jan 26 1999	Drop -i; add similar feature to immatch
 * Feb 12 1999	Finish adding support for ACT catalog
 * Feb 18 1998	Add variable number of decimal places to TDC catalog output
 * Mar  2 1999	Add x and y to non-tab output (bug fix)
 * Apr  7 1999	Add filename argument to GetFITSWCS
 * Apr 13 1999	Fix progname to drop / when full pathname
 * Apr 20 1999	Fix minor bug in character assignment code
 * May 12 1999	Adjust command listing
 * Jun 17 1999	Use SearchLim() to compute search limits
 * Jul  8 1999	Fix bug when noll object name list
 * Aug 24 1999	If radius not set for region mode, use 20 pixels
 * Aug 25 1999	Add Bright Star Catalog, BSC
 * Aug 25 1999	Allocate using calloc(ngmax, ) instead of malloc(nbytes)
 * Aug 25 1999	Add option to set circle radius in pixels if < -1
 * Sep 10 1999	Do all searches through catread() and catrnum()
 * Sep 16 1999	Add zero distsort argument to catread() call
 * Oct 15 1999	Free wcs using wcsfree()
 * Oct 22 1999	Drop unused variables after lint
 * Oct 22 1999	Change catread() to ctgread() to avoid system conflict
 *
 * Jan 11 2000	Get nndec for Starbase catalogs
 * Jan 28 2000	Call setdefwcs() with WCS_ALT instead of 1
 * Feb 15 2000	Use MAXCAT from lwcs.h instead of MAXREF
 * Feb 28 2000	Drop Peak column if not set in TAB catalogs
 * Mar 10 2000	Move catalog selection from executable name to subroutine
 * Mar 15 2000	Add proper motions to catalog calls and Starbase output
 * Mar 28 2000	Clean up output for catalog IDs and GSC classes
 * May 26 2000	Add Tycho 2 catalog
 * May 26 2000	Always use CatNumLen() to get ID number field size
 * Jul 12 2000	Add star catalog data structure to ctgread() argument list
 * Jul 25 2000	Add coordinate system to SearchLim() call
 * Jul 25 2000	Fix star catalog structure initialization bug
 * Jul 25 2000	Pass address of star catalog data structure address
 * Sep 21 2000	Print spectral type instead of plate number of USNO-A catalogs
 * Dec  1 2000	Print plate, not type for USNO catalogs
 * Dec 15 2000	Deal with missing catalog names
 * Dec 18 2000	Always allocate proper motion arrays
 *
 * Jan 22 2001	Drop declaration of wcsinit()
 * Feb 23 2001	Drop distance from search center output everywhere
 * Mar 27 2001	Add option to set size of overplotted stars in pixels
 * May 23 2001	Add support for GSC-ACT catalog
 * May 24 2001	Add support for 2MASS Point Source Catalog
 * Jun  8 2001	Add proper motion flag and number of magnitudes to RefCat()
 * Jun 29 2001	Add support for GSC II catalog
 * Sep 13 2001	Allow sort by RA, Dec, X, Y, any magnitude
 * Sep 13 2001	Use 2-D array of magnitudes, rather than multiple vectors
 * Sep 14 2001	Add option to print catalog as returned over the web
 * Sep 18 2001	Add flags to IRAS and 2MASS point sources
 * Sep 20 2001	Get magnitude name for limits from CatMagName()
 * Oct 19 2001	Add -y to set epoch of observation
 * Oct 25 2001	Allow arbitrary argument order on command line
 * Oct 31 2001	Print complete help message if no arguments
 * Nov  6 2001	Declare undeclared subroutine setparm()
 *
 * Feb  1 2002	Print spectral type for TDC format catalogs, if present
 * Apr  3 2002	Add magnitude number to sort options
 * Apr  8 2002	Add magnitude number to magnitude limit setting
 * Apr  8 2002	Fix bug so that characters other than circles can be plotted
 * Apr 10 2002	Fix magnitude number bug and add magnitude letter
 * May  1 2002	Add -a command to set initial rotation angle
 * Jun 19 2002	Add verbose argument to GetFITShead()
 * Aug  6 2002	Print all magnitudes for BINARY, TABTABLE, or ASCII catalogs
 *
 * Jan 26 2003	Add support for USNO-B1.0 catalog
 * Jan 28 2003	Fix bug printing proper motion
 * Jan 29 2003	Add header lines if USNO-B1.0 ID or PM quality limits
 * Mar  4 2003	If star is offscale, set x and y to 0.0
 * Mar 25 2003	Deal correctly with rotated images
 * Apr  2 2003	Try rotated images again
 * Apr 13 2003	Set revision message for subroutines using setrevmsg()
 * Apr 14 2003	Pass through nstarmax=-1 option to ctgread()
 * Apr 24 2003	Add UCAC1 catalog; turn off header if tab on
 * May 28 2003	Add TMIDR2 with TMPSC formats
 * May 30 2003	Add UCAC2 catalog
 * Jun  2 2003	Print both RA and Dec proper motion as mas/year
 * Aug  7 2003	Change NOSORT to SORT_NONE
 * Aug 19 2003	Fix help listing to note that magnitude limits are comma-separated
 * Aug 20 2003	Print 4 decimal places for epoch and equinox
 * Aug 22 2003	Add inner radius = 0.0 argument to ctgread call
 * Oct  7 2003	Add -f to print only number of catalog stars in image
 * Nov 22 2003	Add class to GSC II output
 * Dec  4 2003	Add support for USNO YB6 catalog and GSC 2.3
 * Dec 10 2003	Add L for limiting magnitude (>100.0) in default catalog
 *
 * Jan 14 2004	Add support for Sloan Digital Sky Survey Photometric Catalog
 * Jan 22 2004	Add support for 2MASS Extended Source Catalog
 * Jan 22 2004	Call setlimdeg() to optionally print limits in degrees
 * Aug 30 2004	Fix declarations
 * Sep 16 2004	Fix RA limits in ImageLim() to deal with wrap through 0:00
 * Nov  5 2004	Print epoch if in ASCII catalog
 * Nov 19 2004	Print star/galaxy type for USNO-B1.0
 *
 * Apr  4 2005	Exit with error message if no catalog is specified
 * Aug  5 2005	Add magnitude error option to 2MASS PSC and Tycho2
 *
 * Feb 23 2006	Add second radius for magnitude-scaled star plots
 * Apr 12 2006	Add sort by ID number
 * Jun  8 2006	Print object name if no number present
 * Jun 21 2006	Clean up code
 * Jun 27 2006	Deal with specified keyword in Starbase files correctly
 * Sep 11 2006	Add .reg suffix when writing PROS region files
 * Sep 26 2006	Increase length of rastr and destr from 16 to 32
 * Nov  6 2006	Print SDSS number as character string; it is now 18 digits long
 *
 * Jan 10 2007	Declare RevMsg static, not const
 * Jul  5 2007	Add -l command to set image size
 * Jul  5 2007	Modify code to use WCS info from command line without image
 * Jul 24 2007	Add SkyBot format for output
 * Jul 26 2007	Clean up code for running without an image file
 *
 * Jul  9 2008	Free catalog arrays at end of program not of image
 * Nov 17 2008	Drop computed spectral type from Tycho and Tycho-2 catalogs
 *
 * Sep 25 2009	Add FreeBuffers() and AllocBuffers() after Douglas Burke
 * Nov 10 2009	Fix image limits for 90 degree rotation
 * Nov 10 2009	Allocat MAXNMAG magnitude vectors
 * Dec 14 2009	Add UCAC3 catalog
 */
