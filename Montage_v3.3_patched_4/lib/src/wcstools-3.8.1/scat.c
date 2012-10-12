/* File scat.c
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
#include "libwcs/lwcs.h"
#include "libwcs/fitsfile.h"
#include "libwcs/wcscat.h"

static char *RevMsg = "WCSTools 3.8.1, 14 December 2009, Doug Mink SAO";

static void PrintUsage();
	static int scatparm();
static void scatcgi();

static int ListCat ();
extern void setcenter();
static void SearchHead();
static int GetArea();
static void PrintGSClass();
static void PrintGSCBand();
static void PrintWebHelp();

static int verbose = 0;		/* Verbose/debugging flag */
static int afile = 0;		/* True to append output file */
static int wfile = 0;		/* True to print output file */
static int classd = -1;		/* Guide Star Catalog object classes */
static double maglim1 = MAGLIM1; /* Catalog bright magnitude limit */
static double maglim2 = MAGLIM2; /* Catalog faint magnitude limit */
static int sysout0 = 0;		/* Output coordinate system */
static double eqcoor = 0.0;	/* Equinox of search center */
static int degout0 = 0;		/* 1 if degrees output instead of hms */
static double ra0 = -99.0;	/* Initial center RA in degrees */
static double dec0 = -99.0;	/* Initial center Dec in degrees */
static double rad0 = 0.0;	/* Search box radius */
static double rad1 = 0.0;	/* Inner search annulus radius */
static double dra0 = 0.0;	/* Search box width */
static double ddec0 = 0.0;	/* Search box height */
static double epoch0 = 0.0;	/* Epoch for coordinates */
static double epoch1 = 0.0;	/* Earliest epoch for catalog search */
static double epoch2 = 0.0;	/* Latest epoch for catalog search */
static int syscoor = 0;		/* Input search coordinate system */
static int nstars = 0;		/* Number of brightest stars to list */
static int ndra = 3;		/* Number of decimal places in RA seconds */
static int nddec = 2;		/* Number of decimal places in Dec seconds */
static int nddeg = 7;		/* Number of decimal places in degree output */
static int printhead = 0;	/* 1 to print table heading */
static int printabhead = 1;	/* 1 to print tab table heading if no sources */
static int printprog = 0;	/* 1 to print program name and version */
static int printepoch = 0;	/* 1 to print epoch of entry */
static int nohead = 1;		/* 1 to print table heading */
static int searchcenter = 0;	/* 1 to print simpler format */
static int tabout = 0;		/* 1 for tab table to standard output */
static int catsort = SORT_UNSET; /* Default to sort stars by magnitude */
static int debug = 0;		/* True for extra information */
static char *objname;		/* Object name for output */
static char *keyword;		/* Column to add to tab table output */
static char *progname;		/* Name of program as executed */
static char *coorsys;		/* Coordinate system of search center */
static int match = 0;		/* If 1, match num exactly in BIN or ASC cats*/
static int printobj = 0;	/* If 1, print object name instead of number */
static char cpname[16];		/* Name of program for error messages */
static int oneline = 0;		/* If 1, print center and closest on 1 line */
static struct Star *srch;	/* Search center structure for catalog search */
static struct StarCat *srchcat; /* Search catalog structure */
static int readlist = 0;	/* If 1, search centers are from a list */
static int notprinted = 1;	/* If 1, print header */
static char *listfile;		/* Name of catalog file with search centers */
static int printxy = 0;		/* If 1, print X Y instead of object number */
static char *xstr, *ystr;	/* X and Y strings if printxy */
static int closest;		/* 1 if printing only closest star */
static double *gnum;		/* Catalog star numbers */
static double *gra;		/* Catalog star right ascensions */
static double *gdec;		/* Catalog star declinations */
static double *gpra;		/* Catalog star RA proper motions */
static double *gpdec;		/* Catalog star declination proper motions */
static double **gm;		/* Catalog magnitudes */
static double *gx;		/* Catalog star X positions on image */
static double *gy;		/* Catalog star Y positions on image */
static int *gc;			/* Catalog star object classes */
static char **gobj;		/* Catalog star object names */
static char **gobj1;		/* Catalog star object names */
static int nalloc = 0;
static struct StarCat *starcat[5]; /* Star catalog data structure */
static double eqout = 0.0;	/* Equinox for output coordinates */
static int ncat = 0;		/* Number of reference catalogs to search */
static char *refcatname[5];	/* reference catalog names */
static char *ranges;		/* Catalog numbers to print */
static int http=0;		/* Set to one if http header needed on output */
static int padspt = 0;		/* Set to one to pad out long spectral type */
static int nmagmax = MAXNMAG;
static int sortmag = 0;
static int lofld = 0;		/* Length of object name field in output */
static int webdump = 0;
static int votab = 0;		/* If 1, print output as VOTable XML */
static int minid = 0;		/* Minimum number of plate IDs for USNO-B1.0 */
static int minpmqual = 0;	/* Minimum USNO-B1.0 proper motion quality */
static int rdra = 0;		/* If 1, dra is in ra units, not sky units */
static int idrun = 0;		/* If 1, 2MASS ID run from inside loop */
static char voerror[80];	/* Error for Virtual Observatory */
static int refcat = 0;		/* reference catalog switch */
static char title[80];	/* Title of reference Catalog */
static int nmag = 0;	/* Number of magnitudes in reference catalog */
static int mprop = 0;	/* 1 if proper motion in reference catalog */
static int sysref = 0;	/* Coordinate system of reference catalog */
static double eqref;	/* Equinox of catalog to be searched */
static double epref;	/* Epoch of catalog to be searched */
extern void setminpmqual();
extern void setminid();
extern void setrevmsg();

int
main (ac, av)
int ac;
char **av;
{
    FILE *fd;
    char *str, *str1;
    char rastr[32];
    char decstr[32];
    char line[200];
    char errmsg[256];
    int i, lcat;
    char *refcatn;
    char cs, cs1;
    int ncat1;
    int srchtype;
    char *newranges;
    int systemp = 0;		/* Input search coordinate system */
    int istar;
    char *blank;
    int imag;
    int nmag1;
    int lprop, lnmag;
    char listtitle[80];
    double epoch;
    char coorout[32];
    char *query;
    int ndcat;
    int lrange;
    char *rstr, *dstr, *astr, *cstr, *ccom, *cep2;

    ranges = NULL;
    keyword = NULL;
    objname = NULL;
    voerror[0] = (char) 0;
    for (i = 0; i < 5; i++)
	starcat[i] = NULL;

    /* Null out buffers before starting */
    gnum = NULL;
    gra = NULL;
    gdec = NULL;
    gpra = NULL;
    gpdec = NULL;
    gm = NULL;
    gx = NULL;
    gy = NULL;
    gc = NULL;
    gobj = NULL;
    gobj1 = NULL;
    setrevmsg (RevMsg);
    coorout[0] = (char) 0;

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
	refcatname[ncat] = refcatn;
	ncat++;
	refcat = RefCat (refcatn,title,&sysref,&eqref,&epref,&mprop,&nmag);
        if (refcat == UCAC2 || refcat == UCAC3)
            nmag1 = nmag + 4;
        else if (refcat)
            nmag1 = nmag;
        else
            PrintUsage ("Cannot find catalog");
        if (nmag1 > nmagmax)
            nmagmax = nmag1;
	ndcat = CatNdec (refcat);
	}
    else
	ndcat = -1;

    /* Set parameters from keyword=value arguments */
    if ((query = getenv ("QUERY_STRING")) != NULL && ac < 3) {
	http++;
        scatcgi (query);
	tabout++;
	}

    /* If not http and no arguments, print command list */
    else if (ac == 1)
        PrintUsage (NULL);

    /* Check for help or version command first */
    if (!http) {
	str = *(av+1);
	if (!str || !strcmp (str, "help") || !strcmp (str, "-help"))
	    PrintUsage (NULL);
	if (!strcmp (str, "version") || !strcmp (str, "-version"))
	    PrintUsage ("version");
	if (!strcasecmp (str, "webhelp"))
	    PrintWebHelp ();
	if (!strncasecmp (str, "band", 4) || !strncasecmp (str, "filt", 4))
	    PrintGSCBand ();
	if (!strncasecmp (str, "clas", 4) || !strncasecmp (str, "obje", 4))
	    PrintGSClass ();
	}

    /* crack arguments */
    for (av++; --ac > 0; av++) {

	/* Set parameters from keyword=value& arguments */
	if (strchr (*av, '&')) {
	    tabout++;
            scatcgi (*av);
	    }

	/* Set parameters from keyword=value arguments */
	else if (strchr (*av, '=')) {
            if (scatparm (*av))
		fprintf (stderr, "SCAT: %s is not a parameter.\n", *av);
	    refcat = CatCode (refcatname[ncat-1]);
	    if (nmag > nmagmax)
		nmagmax = nmag;
	    ndcat = CatNdec (refcat);
	    }

	/* Set search RA, Dec, and equinox if 2MASS ID */
	else if (tmcid (*av, &ra0, &dec0)) {
	    syscoor = WCS_J2000;
	    eqcoor = 2000.0;
	    if (epoch0 == 0.0)
		epoch0 = 2000.0;
	    if (eqout == 0.0)
		eqout = 2000.0;
	    idrun = 0;
	    ListCat (ranges, eqout);
	    idrun = 1;
	    ra0 = 0.0;
	    dec0 = 0.0;
	    }

	/* Set search RA, Dec, and equinox if colon in argument */
	else if (strsrch (*av,":") != NULL) {
	    if (ac < 2)
		PrintUsage (*av);
	    else {
		if (strlen (*av) < 32)
		    strcpy (rastr, *av);
		else {
		    strncpy (rastr, *av, 31);
		    rastr[31] = (char) 0;
		    }
		ac--;
		av++;
		if (strlen (*av) < 32)
		    strcpy (decstr, *av);
		else {
		    strncpy (decstr, *av, 31);
		    decstr[31] = (char) 0;
		    }
		ac--;
		ra0 = str2ra (rastr);
		dec0 = str2dec (decstr);
		if (ac < 1) {
		    syscoor = WCS_J2000;
		    eqcoor = 2000.0;
		    }
		else if ((syscoor = wcscsys (*(av+1))) >= 0) {
		    coorsys = *++av;
		    eqcoor = wcsceq (coorsys);
		    }
		else {
		    syscoor = WCS_J2000;
		    eqcoor = 2000.0;
		    }
		}
	    }

	/* Set range and make a list of star numbers from it */
	else if (isrange (*av)) {
	    if (ranges) {
		lrange = strlen(ranges) + strlen(*av) + 16;
		newranges = (char *) calloc (lrange, 1);
		strcpy (newranges, ranges);
		strcat (newranges, ",");
		strcat (newranges, *av);
		free (ranges);
		ranges = newranges;
		newranges = NULL;
		}
	    else {
		lrange = strlen(*av) + 16;
		ranges = (char *) calloc (lrange, 1);
		if (strchr (*av,'.'))
		    match = 1;
		strcpy (ranges, *av);
		}
	    if (strchr (*av, 'x') == NULL && ndcat > 0) {
		int n = ndcat;
		strcat (ranges, "x0.");
		while (n-- > 0)
		    strcat (ranges, "0");
		strcat (ranges, "1");
		}
	    }

	/* Set decimal degree center or star number */
	else if (isnum (*av)) {

	    if (ac > 1 && isnum (*(av+1))) {
		int ndec1, ndec2;

		/* Check for second number and coordinate system */
		if (ac > 2 && (systemp = wcscsys (*(av + 2))) > 0) {
		    rstr = *av++;
		    ac--;
		    dstr = *av++;
		    ac--;
		    cstr = *av;
		    eqcoor = wcsceq (cstr);
		    }

		/* Check for two numbers which aren't catalog numbers */
		else {
		    ndec1 = StrNdec (*av);
		    ndec2 = StrNdec (*(av+1));
		    if (ndcat > -1 && ndec1 == ndcat && ndec2 == ndcat)
			cstr = NULL;
		    else {
			rstr = *av++;
			ac--;
			dstr = *av;
			cstr = (char *) malloc (8);
			strcpy (cstr, "J2000");
			systemp = WCS_J2000;
			}
		    }
		}
	    else
		cstr = NULL;

	    /* Set decimal degree center */
	    if (cstr != NULL) {
		ra0 = atof (rstr);
		dec0 = atof (dstr);
		syscoor = systemp;
		eqcoor = wcsceq (cstr);
		}

	    /* Assume number to be star number if no coordinate system */
	    else {
		if (strchr (*av,'.'))
		    match = 1;
		if (ranges) {
		    lrange = strlen(ranges) + strlen(*av) + 2;
		    newranges = (char *)calloc (lrange, 1);
		    strcpy (newranges, ranges);
		    strcat (newranges, ",");
		    strcat (newranges, *av);
		    free (ranges);
		    ranges = newranges;
		    newranges = NULL;
		    }
		else {
		    lrange = strlen(*av) + 2;
		    ranges = (char *) calloc (lrange, 1);
		    strcpy (ranges, *av);
		    }
		}
	    }

	else if (*(str = *av) == '@') {
	    readlist = 1;
	    listfile = *av + 1;
	    }

	/* Otherwise, read command */
	else if ((*(str = *av)) == '-') {
	    char c;
	    while ((c = *++str))
	    switch (c) {

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
		printprog++;
		break;

	    case 'a':	/* Get closest source */
		catsort = SORT_DIST;
		closest++;
		break;

    	    case 'b':	/* output coordinates in B1950 */
		sysout0 = WCS_B1950;
		eqout = 1950.0;
    		break;

	    case 'c':       /* Set reference catalog */
		if (ac < 2)
		    PrintUsage (str);
		lcat = strlen (*++av);
		refcatn = (char *) calloc (1, lcat + 2);
		strcpy (refcatn, *av);
		refcatname[ncat] = refcatn;
		refcat = RefCat (refcatn,title,&sysref,&eqref,&epref,&mprop,&nmag);
		if (refcat == UCAC2 || refcat == UCAC3)
		    nmag1 = nmag + 4;
		else if (refcat)
		    nmag1 = nmag;
		else
		    PrintUsage ("Cannot find catalog");
		if (nmag1 > nmagmax)
		    nmagmax = nmag1;
		ndcat = CatNdec (refcat);
		ncat = ncat + 1;
		ac--;
		break;

	    case 'd':	/* output in degrees instead of sexagesimal */
		degout0++;
		break;

	    case 'e':	/* Set ecliptic coordinate output */
		sysout0 = WCS_ECLIPTIC;
		break;

	    case 'f':	/* output in centering coordinates */
		searchcenter++;
		break;

	    case 'g':	/* Set galactic coordinate output and optional center */
		sysout0 = WCS_GALACTIC;
		break;

	    case 'h':	/* output descriptive header */
		if (tabout)
		    printabhead = 0;
		else {
		    printhead++;
		    printprog++;
		    }
		break;

	    case 'i':	/* ouput catalog object name instead of number */
		printobj++;
		if (!(*(str+1)) && ac > 1 && isnum (*(av+1))) {
		    lofld = atoi (*++av);
		    ac--;
		    }
		break;

    	    case 'j':	/* center coordinates on command line in J2000 */
		sysout0 = WCS_J2000;
		eqout = 2000.0;
    		break;

	    case 'k':	/* Keyword (column) to add to output from tab table */
		if (ac < 2)
		    PrintUsage (str);
		keyword = *++av;
		settabkey (keyword);
		ac--;
		break;

	    case 'l':	/* Print center and closest star on one line */
		oneline++;
		catsort = SORT_DIST;
		closest++;
		nstars = 1;
		break;

	    case 'm':	/* Magnitude limit */
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

	    case 'n':	/* Number of brightest stars to read */
		if (ac < 2)
		    PrintUsage (str);
		nstars = atoi (*++av);
		ac--;
		break;

	    case 'o':	/* Object name */
		if (ac < 2)
		    PrintUsage (str);
		objname = *++av;
		ac--;
		break;

	    case 'p':	/* Sort by distance from center */
		catsort = SORT_DIST;
		printprog++;
		break;

    	    case 'q':	/* Output equinox in years */
    		if (ac < 2)
    		    PrintUsage (str);
		strcpy (coorout, *++av);
		if (coorout[0] == 'J' || coorout[0] == 'j')
		    sysout0 = WCS_J2000;
		else if (coorout[0] == 'B' || coorout[0] == 'b')
		    sysout0 = WCS_B1950;
		eqout = wcsceq (coorout);
    		ac--;
    		break;

    	    case 'r':	/* Search box or circle half-size in arcseconds */
    		if (ac < 2)
    		    PrintUsage (str);
		cs1 = *(str+1);
		if (cs1 != (char) 0)
		    ++str;
		av++;

		/* Separate 2 arguments for rectangles */
		if ((dstr = strchr (*av, ',')) != NULL) {
		    *dstr = (char) 0;
		    dstr++;
		    }

		/* Separate 2 arguments for annulus */
		if ((astr = strchr ((*av)+1, '-')) != NULL) {
		    *astr = (char) 0;
		    astr++;
		    }

		/* Convert radius or first argument to arcseconds */
		if (strchr (*av,':'))
		    rad0 = 3600.0 * str2dec (*av);
		else
		    rad0 = atof (*av);

		/* Convert outer radius of annulus to arcseconds */
		if (astr != NULL) {
		    rad1 = rad0;
		    if (strchr (astr,':'))
			rad0 = 3600.0 * str2dec (astr);
		    else
			rad0 = atof (astr);
		    }

		/* Convert second argument (=dec radius) to arcseconds */
		if (dstr != NULL) {
		    if (cs1 == 'r')
			rdra = 1;
		    else
			rdra = 0;
		    dra0 = rad0;
		    rad0 = 0.0;
		    if (strchr (dstr, ':'))
			ddec0 = 3600.0 * str2dec (dstr);
		    else
			ddec0 = atof (dstr);
		    if (ddec0 <= 0.0)
			ddec0 = dra0;
		    }
    		ac--;
    		break;

	    case 's':	/* sort by RA, Dec, magnitude or nothing */
		catsort = SORT_RA;
		if (ac > 1) {
		    str1 = *(av + 1);
		    cs = str1[0];
		    if (strchr ("ademinprs",(int)cs)) {
			cs1 = str1[1];
			av++;
			ac--;
			}
		    else
			cs = 'r';
		    }
		else
		    cs = 'r';
		switch (cs) {

		    /* Merge */
		    case 'e':
			catsort = SORT_MERGE;
			break;

		    /* Declination */
		    case 'd':
			catsort = SORT_DEC;
			break;

		    /* ID Number */
		    case 'i':
			catsort = SORT_ID;
			break;

		    /* Magnitude (brightest first) */
		    case 'm':
			catsort = SORT_MAG;
			if (cs1 != (char) 0) {
			    if (cs1 > '9')
				sortmag = (int) cs1;
			    else
				sortmag = (int) cs1 - 48;
			    }
			break;

		    /* No sorting */
		    case 'n':
			catsort = SORT_NONE;
			break;

		    /* Distance from search center (closest first) */
		    case 'a':
		    case 'p':
		    case 's':
			catsort = SORT_DIST;
			break;

		    /* Right ascension */
		    case 'r':
			catsort = SORT_RA;
			break;
		    default:
			catsort = SORT_RA;
		    }
		break;

	    case 't':	/* tab table to stdout */
		if (tabout) {
		    tabout = 0;
		    votab = 1;
		    degout0 = 1;
		    }
		else
		    tabout = 1;
		if (printhead) {
		    printabhead = 0;
		    printhead = 0;
		    printprog = 0;
		    }
		break;

	    case 'u':       /* Print following 2 numbers at start of line */
		if (ac > 2) {
		    printxy = 1;
		    xstr = *++av;
		    ac--;
		    ystr = *++av;
		    ac--;
		    }
		break;

    	    case 'w':	/* write output file */
    		wfile++;
    		break;

	    case 'x':       /* Guide Star object class */
		if (ac < 2)
		    PrintUsage (str);
		classd = (int) atof (*++av);
		setgsclass (classd);
		ac--;
		break;

	    case 'y':	/* Set output coordinate epoch */
		if (str[1] == 'j')
		    setdateform (EP_JD);
		else if (str[1] == 'm')
		    setdateform (EP_MJD);
		else if (str[1] == 'f')
		    setdateform (EP_FD);
		else if (str[1] == 'i')
		    setdateform (EP_ISO);
		else
		    setdateform (EP_EP);
		if (ac < 2)
		    PrintUsage (str);
		if ((cep2 = strchr (*(av+1), ','))) {
		    av++;
		    *cep2 = (char) 0;
		    cep2 = cep2 + 1;
		    if (strchr (*av, '.'))
			epoch1 = atof (*av);
		    else
			epoch1 = fd2ep (*av);
		    if (strchr (*av, '.'))
			epoch2 = atof (cep2);
		    else
			epoch2 = fd2ep (cep2);
		    ac--;
		    }
		else if (strchr (*(av+1), '.')) {
		    av++;
		    epoch0 = atof (*av);
		    ac--;
		    }
		else {
		    av++;
		    epoch0 = fd2ep (*av);
		    ac--;
		    }
		break;

	    case 'z':	/* Set append flag */
		afile++;
		wfile++;
		break;

	    default:
		sprintf (errmsg, "* Illegal command -%s-", *av);
		PrintUsage (errmsg);
		break;
	    }
	    }
	else {
	    lcat = strlen (*av);
	    refcatn = (char *) calloc (1, lcat + 2);
	    strcpy (refcatn, *av);
	    refcatname[ncat] = refcatn;
	    ncat = ncat + 1;
	    }
	}

    /* If 2MASS ID run from inside loop, quit now */
    if (idrun)
	exit (0);

    /* If no catalog has been specified, quit now */
    if (ncat < 1) {
	sprintf (errmsg, "* No catalog name given");
	PrintUsage (errmsg);
	}

    /* Set output equinox appropriately if output system is specified */
    if (eqout == 0.0) {
	if (eqcoor != 0.0)
	    eqout = eqcoor;
	if (sysout0 != 0) {
	    if (sysout0 == WCS_J2000)
		eqout = 2000.0;
	    if (sysout0 == WCS_B1950)
		eqout = 1950.0;
	    }
	}

    /* Set output epoch appropriately if output system is specified */
    if (sysout0 == 0) {
	if (strlen (coorout) > 0) {
	    sysout0 = wcscsys (coorout);
	    eqout = wcsceq (coorout);
	    }
	else if (syscoor != 0)
	    sysout0 = syscoor;
	}

    /* Set output equinox appropriately if output system is specified */
    if (eqout == 0.0) {
	if (eqcoor != 0.0)
	    eqout = eqcoor;
	if (sysout0 != 0) {
	    if (sysout0 == WCS_J2000)
		eqout = 2000.0;
	    if (sysout0 == WCS_B1950)
		eqout = 1950.0;
	    }
	}

    /* Set output epoch from output equinox if not otherwise set */
    if (epoch0 == 0.0)
	epoch0 = eqout;

    /* If http output, send header */
    if (votab) {
	printf ("Content-type: text/xml\n\n");
	printprog = 0;
	}
    else if (http) {
	printf ("Content-type: text/plain\n\n");
	}

    if (readlist) {

	/* Read search center list from starbase tab table catalog */
	if (istab (listfile)) {
	    ranges = NULL;
	    srchcat = tabcatopen (listfile, NULL, 10000);
	    if (srchcat != NULL) {
		srch = (struct Star *) calloc (1, sizeof (struct Star));
		for (istar = 1; istar <= srchcat->nstars; istar ++) {
		    if (tabstar (istar, srchcat, srch, verbose)) {
			if (verbose)
			    fprintf (stderr,"%s: Cannot read star %d\n",
				     cpname, istar);
                	break;
                	}
		    ra0 = srch->ra;
		    dec0 = srch->dec;
		    if (eqout > 0.0)
			eqcoor = eqout;
		    else
			eqcoor = srch->equinox;
		    if (epoch0 != 0.0)
			epoch = epoch0;
		    else
			epoch = srch->epoch;
		    if (sysout0)
			syscoor = sysout0;
		    else
			syscoor = srch->coorsys;
		    wcsconp (srch->coorsys, syscoor, srch->equinox, eqcoor,
			     srch->epoch,epoch,
			     &ra0,&dec0,&srch->rapm,&srch->decpm);
		    ListCat (ranges, eqout);
		    }
		tabcatclose (srchcat);
		}
	    }

	/* Read search center list from SAOTDC ASCII table catalog */
	else if (isacat (listfile)) {
	    ranges = NULL;
	    if (!(srchtype = RefCat (listfile,listtitle,&syscoor,&eqcoor,
				     &epoch,&lprop,&lnmag))) {
		if (lnmag > nmagmax)
		    nmagmax = lnmag;
		fprintf (stderr,"List catalog '%s' is missing\n", listfile);
		return (0);
		}
	    srchcat = ctgopen (listfile, srchtype);
	    if (srchcat != NULL) {
		srch = (struct Star *) calloc (1, sizeof (struct Star));
		for (istar = 1; istar <= srchcat->nstars; istar ++) {
		    if (ctgstar (istar, srchcat, srch)) {
			if (verbose)
			    fprintf (stderr,"%s: Cannot read star %d\n",
				 cpname, istar);
                	}
		    else {
			ra0 = srch->ra;
			dec0 = srch->dec;
			if (eqout > 0.0)
			    eqcoor = eqout;
			else
			    eqcoor = srch->equinox;
			if (epoch0 != 0.0)
			    epoch = epoch0;
			else
			    epoch = srch->epoch;
			if (sysout0)
			    syscoor = sysout0;
			else
			    syscoor = srch->coorsys;
			wcsconp (srch->coorsys, syscoor, srch->equinox, eqcoor,
				 srch->epoch,epoch,
				 &ra0,&dec0,&srch->rapm,&srch->decpm);
			ListCat (ranges, eqout);
			}
		    }
		ctgclose (srchcat);
		}
	    }
	else {
	    if (strcmp (listfile,"STDIN")==0 || strcmp (listfile,"stdin")==0)
                fd = stdin;
            else
                fd = fopen (listfile, "r");
            if (fd != NULL) {
                while (fgets (line, 200, fd)) {
		    blank = strchr (line, ' ');
		    if (blank)
			*blank = (char) 0;
		    blank = strchr (line, '\n');
		    if (blank)
			*blank = (char) 0;
		    ranges = (char *) calloc (strlen(line) + 1, 1);
		    match = 1;
		    strcpy (ranges, line);
		    ListCat (ranges, eqout);
		    }
		fclose (fd);
		}
	    }
	}
    else {
	if (sysout0 && !syscoor)
	    syscoor = sysout0;
	if (syscoor) {
	    if (eqout == 0.0) {
		if (eqcoor != 0.0)
		    eqout = eqcoor;
		else if (syscoor == WCS_B1950)
		    eqout = 1950.0;
		else
		    eqout = 2000.0;
		}
	    if (epoch0 == 0.0)
		epoch0 = eqout;
	    }
	ListCat (ranges, eqout);
	}

    for (i = 0; i < ncat; i++) {
	if (refcatname[i]) free (refcatname[i]);
	ctgclose (starcat[i]);
	}

    /* Free memory used for search results and return */
    if (gx) {
	free ((char *)gx);
	gx = NULL;
	}
    if (gy) {
	free ((char *)gy);
	gy = NULL;
	}
    if (gm) {
	for (imag = 0; i < nmagmax; i++) {
	    if (gm[imag]) {
		free ((char *)gm[imag]);
		gm[imag] = NULL;
		}
	    }
	free ((char *)gm);
	gm = NULL;
	}
    if (gra) {
	free ((char *)gra);
	gra = NULL;
	}
    if (gdec) {
	free ((char *)gdec);
	gdec = NULL;
	}
    if (gnum) {
	free ((char *)gnum);
	gnum = NULL;
	}
    if (gc) {
	free ((char *)gc);
	gc = NULL;
	}
    if (gobj) {
	for (i = 0; i < nalloc; i++) {
	    if (gobj[i] != NULL)
		free ((char *)gobj[i]);
	    }
	free ((char *)gobj);
	gobj = NULL;
	}

    return (0);
}

static void
PrintUsage (command)

char	*command;	/* Command where error occurred or NULL */
{
    FILE *dev;	/* Output, stderr for command line, stdout for web */
    int catcode;
    char *srcname;
    char *catname;

    if (http) {
	dev = stdout;
	fprintf (dev, "Content-type: text/plain\n\n");
	}
    else
	dev = stderr;

    /* Print program name and version */
    fprintf (dev,"%s %s\n", progname, RevMsg);
    if (command != NULL && !strncasecmp (command, "ver", 3))
	exit (0);

    if (command != NULL) {
	if (command[0] == '*')
	    fprintf (dev, "%s\n", command);
	else
	    fprintf (dev, "* Missing argument for command: %c\n", command[0]);
	exit (1);
	}

    catname = ProgCat (progname);
    catcode = CatCode (catname);
    srcname = CatSource (catcode, catname);
    fprintf (stderr,"List %s in a region on the sky\n", srcname);

    if (catname == NULL)
	fprintf (dev,"Usage: %s -c catalog [arguments] ra dec system (J2000, B1950, etc.)\n", progname);
    else
	fprintf (dev,"Usage: %s [arguments] ra dec system (J2000, B1950, etc.)\n", progname);
    fprintf (dev,"  or : %s [arguments] list of catalog number ranges\n",
	progname);
    fprintf (dev,"  or : %s [arguments] @file of either positions or numbers)\n",
	progname);
    fprintf(dev,"  -a: List single closest catalog source\n");
    fprintf(dev,"  -b: Output B1950 (FK4) coordinates\n");
    if (!strcmp (progname, "scat"))
    fprintf(dev,"  -c name: Reference catalog (act, gsc, ua2, usa2, or local file\n");
    fprintf(dev,"  -d: Output RA and Dec in degrees instead of hms dms\n");
    fprintf(dev,"  -e: Output ecliptic coordinates\n");
    fprintf(dev,"  -f: Output search center for other programs\n");
    fprintf(dev,"  -g: Output galactic coordinates\n");
    fprintf(dev,"  -h: Print heading, else do not \n");
    fprintf(dev,"  -i [length]: Print catalog object name, not catalog number (length optional)\n");
    fprintf(dev,"  -j: Output J2000 (FK5) coordinates\n");
    fprintf(dev,"  -k kwd: Add this keyword to output from tab table search\n");
    fprintf(dev,"  -l: Print center and closest star on one line\n");
    fprintf(dev,"  -mx mag1[,mag2]: Magnitude #x limit(s) (only one set allowed, default none) \n");
    fprintf(dev,"  -n num: Number of brightest stars to print (-1=all as found)\n");
    fprintf(dev,"  -o name: Object name \n");
    fprintf(dev,"  -q year: Equinox of output positions in FITS date format or years\n");
    fprintf(dev,"  -r rad: Search radius (<0=-half-width) in arcsec\n");
    fprintf(dev,"  -r radi-rado: Inner and outer edges of search annulus in arcsec\n");
    fprintf(dev,"  -r dx,dy: Search halfwidths in ra,dec in great circle arcseconds\n");
    fprintf(dev,"  -rr dra,ddec: Search halfwidths in ra,dec in arcsec of RA and Dec\n");
    fprintf(dev,"  -s d|e|mx|n|p|r: Sort by r=RA d=Dec mx=Mag#x n=none p=distance e=merge\n");
    fprintf(dev,"  -t: Tab table to standard output as well as file\n");
    fprintf(dev,"  -u x y: Print x y instead of number in front of non-tab entry\n");
    fprintf(dev,"  -v: Verbose\n");
    fprintf(dev,"  -w: Write output file search[objname].[catalog]\n");
    if (!strcmp (progname, "scat") || !strcmp (progname, "sgsc"))
	fprintf(dev,"  -x type: GSC object type (0=stars 3=galaxies -1=all -2=bands)\n");
    fprintf(dev,"  -y year: Epoch of output positions in FITS date format or years\n");
    fprintf(dev,"     year,year: First and last acceptable catalog entry epochs\n");
    fprintf(dev,"  -z: Append to output file search[objname].[catalog]\n");
    fprintf(dev,"   x: Number of magnitude must be same for sort and limits\n");
    fprintf(dev,"      and x may be omitted from either or both -m and -s m\n");
    if (command != NULL)
	exit (1);
    else
	exit (0);
}

#define TABMAX 64

static int
ListCat (ranges, eqout)

char	*ranges;	/* String with range of catalog numbers to list */
double	eqout;		/* Equinox for output coordinates */

{
    double cra = 0.0;	/* Search center long or RA in degrees */
    double cdec = 0.0;	/* Search center lat or Dec in degrees */
    double crao, cdeco;	/* Output center long/lat or RA/Dec in degrees */
    double epout = 0.0;
    int ng;		/* Number of catalog stars */
    int ns;		/* Number of brightest catalog stars actually used */
    struct Range *range = NULL; /* Range of catalog numbers to list */
    int nfind;		/* Number of stars to find */
    int i, is, j, ngmax, nc;
    double das, dds, drs;
    double gnmax;
    int degout;
    double maxnum;
    FILE *fd = NULL;
    char rastr[32], decstr[32];	/* coordinate strings */
    char numstr[80];	/* Catalog number */
    char cstr[32];	/* Coordinate system */
    char *catalog;
    double drad = 0.0;
    double dradi = 0.0;
    double dra, ddec, mag1, mag2;
    double gdist, da, dd, dec, gdmax;
    double epoch = 0;
    double date, time;
    double era, edec, epmr, epmd;
    int nim, nct;
    int nlog;
    int magsort;
    int typecol;
    int band;
    int imag, nmagr;
    int sysout = 0;
    char sortletter = (char) 0;
    char headline[160];
    char filename[80];
    char string[TABMAX];
    char temp[80];
    char *dtemp;
    char isp[4];
    int ngsc = 0;
    int smag;
    int nns;
    int nid;
    int icat, nndec, nnfld, nsfld;
    int gcset;
    int ndist;
    int distsort;
    int lrv;
    int lobj;
    int nf;
    char tstr[32];
    double flux;
    double pra = 0.0;
    double pdec = 0.0;
    char magname[16];
    char *date1, *date2;
    void ep2dt();
    void PrintNum();
    int LenNum();

    /* Drop out if no catalog is specified */
    if (ncat < 1) {
	fprintf (stderr, "No catalog specified\n");
	exit (-1);
	}

    /* Allocate space for returned catalog information */
    if (ranges != NULL) {
	int nfdef = 9;

	/* Allocate and fill list of numbers to read */
	range = RangeInit (ranges, nfdef);
	ngmax = rgetn (range) * 4;
	}
    else if (nstars != 0)
	ngmax = nstars;
    else
	ngmax = MAXCAT;

    if (ngmax > nalloc) {

	/* Free currently allocated buffers if more entries are needed */
	if (nalloc > 0) {
	    if (gm) {
		for (imag = 0; imag < nmagmax; imag++)
		    if (gm[imag]) free ((char *) gm[imag]);
		free ((char *)gm);
		}
	    if (gra) free ((char *)gra);
	    if (gdec) free ((char *)gdec);
	    if (gpra) free ((char *)gpra);
	    if (gpdec) free ((char *)gpdec);
	    if (gnum) free ((char *)gnum);
	    if (gc) free ((char *)gc);
	    if (gx) free ((char *)gx);
	    if (gy) free ((char *)gy);
	    if (gobj) {
		for (is = 1; is < nalloc; is++) {
		    if (gobj[is] != NULL)
			free ((char *)gobj[is]);
		    }
		free ((char *)gobj);
		}
	    }
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

	if (!(gnum = (double *) calloc (ngmax, sizeof(double))))
	    fprintf (stderr, "Could not calloc %d bytes for gnum\n",
		    ngmax*sizeof(double));
	if (!(gra = (double *) calloc (ngmax, sizeof(double))))
	    fprintf (stderr, "Could not calloc %d bytes for gra\n",
		     ngmax*sizeof(double));
	if (!(gdec = (double *) calloc (ngmax, sizeof(double))))
	    fprintf (stderr, "Could not calloc %d bytes for gdec\n",
		     ngmax*sizeof(double));
	if (!(gm = (double **) calloc (nmagmax, sizeof(double *))))
	    fprintf (stderr, "Could not calloc %d bytes for gm\n",
		     nmagmax*sizeof(double *));
	else {
	    for (imag = 0; imag < nmagmax; imag++) {
		if (!(gm[imag] = (double *) calloc (ngmax, sizeof(double))))
		    fprintf (stderr, "Could not calloc %d bytes for gm\n",
			     ngmax*sizeof(double));
		}
	    }
	if (!(gc = (int *) calloc (ngmax, sizeof(int))))
	    fprintf (stderr, "Could not calloc %d bytes for gc\n",
		     ngmax*sizeof(int));
	if (!(gx = (double *) calloc (ngmax, sizeof(double))))
	    fprintf (stderr, "Could not calloc %d bytes for gx\n",
		     ngmax*sizeof(double));
	if (!(gy = (double *) calloc (ngmax, sizeof(double))))
	    fprintf (stderr, "Could not calloc %d bytes for gy\n",
		     ngmax*sizeof(double));
	if (!(gobj = (char **) calloc (ngmax, sizeof(char *))))
	    fprintf (stderr, "Could not calloc %d bytes for obj\n",
		     ngmax*sizeof(char *));
	else {
	    for (i = 0; i < ngmax; i++)
		gobj[i] = NULL;
	    }
	if (!(gpra = (double *) calloc (ngmax, sizeof(double))))
	    fprintf (stderr, "Could not calloc %d bytes for gpra\n",
		     ngmax*sizeof(double));
	if (!(gpdec = (double *) calloc (ngmax, sizeof(double))))
	    fprintf (stderr, "Could not calloc %d bytes for gpdec\n",
		     ngmax*sizeof(double));
	if (gnum==NULL || gra==NULL || gdec==NULL || gm==NULL || gc==NULL ||
	   gx==NULL || gy==NULL || gobj==NULL || gpra == NULL || gpdec == NULL){
	    if (gm) {
		for (imag = 0; imag < nmagmax; imag++)
		    if (gm[imag]) free ((char *) gm[imag]);
		free ((char *)gm);
		gm = NULL;
		}
	    if (gra) free ((char *)gra);
	    gra = NULL;
	    if (gdec) free ((char *)gdec);
	    gdec = NULL;
	    if (gpra) free ((char *)gpra);
	    gpra = NULL;
	    if (gpdec) free ((char *)gpdec);
	    gpdec = NULL;
	    if (gnum) free ((char *)gnum);
	    gnum = NULL;
	    if (gc) free ((char *)gc);
	    gc = NULL;
	    if (gx) free ((char *)gx);
	    gx = NULL;
	    if (gy) free ((char *)gy);
	    gy = NULL;
	    if (gobj) {
		for (is = 1; is < nalloc; is++) {
		    if (gobj[is] != NULL)
			free ((char *)gobj[is]);
		    }
		free ((char *)gobj);
		}
	    gobj = NULL;
	    nalloc = 0;
	    return (0);
	    }

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
	nalloc = ngmax;
	}

    /* Start of per catalog loop */
    for (icat = 0; icat < ncat; icat++) {
	if (ncat > 1)
	   nohead = 1;
	nndec = 0;
	isp[2] = (char) 0;
	isp[3] = (char) 0;

	/* Skip this catalog if no name is given */
	if (refcatname[icat] == NULL || strlen (refcatname[icat]) == 0) {
	    fprintf (stderr, "Catalog %d not specified\n", icat);
	    continue;
	    }

        if (printprog && notprinted) {
	    if (closest)
	        printf ("\n%s %s  Find closest star\n", progname, RevMsg);
	    else
	        printf ("\n%s %s\n", progname, RevMsg);
	    }

	/* Figure out which catalog we are searching */
	if (ncat > 1 || refcat == 0) {
	    if (!(refcat = RefCat (refcatname[icat],title,&sysref,&eqref,
				   &epref,&mprop,&nmag))) {
		fprintf (stderr,"ListCat: Catalog '%s' is missing\n", refcatname[icat]);
		return (0);
		}
	    }

	if (webdump)
	    nlog = -1;
	else if (debug)
	    nlog = 1;
	else if (verbose) {
	    if (refcat==UAC || refcat==UA1 || refcat==UA2 || refcat==UB1 ||
		refcat==USAC || refcat==USA1 || refcat==USA2 || refcat==GSC ||
		refcat==GSCACT || refcat==TMPSC || refcat==TMPSCE ||
		refcat==TMIDR2 || refcat==TMXSC || refcat == YB6)
		nlog = 1000;
	    else
		nlog = 100;
	    }
	else
	    nlog = 0;

	/* If more magnitudes are needed, allocate space for them */
	if (nmag > nmagmax) {
	    if (gm) {
		for (imag = 0; imag < nmagmax; imag++)
		    free ((char *) gm[imag]);
		free ((char *)gm);
		gm = NULL;
		}
	    nmagmax = nmag;
	    if (!(gm = (double **) calloc (nmagmax, sizeof(double *))))
		fprintf (stderr, "Could not calloc %d bytes for gm\n",
			 nmagmax*sizeof(double *));
	    else {
		for (imag = 0; imag < nmagmax; imag++) {
		    if (!(gm[imag] = (double *) calloc (ngmax, sizeof(double))))
			fprintf (stderr, "Could not calloc %d bytes for gm\n",
				 ngmax*sizeof(double));
		    }
		}
	    }

	/* Set output coordinate system from command line or catalog */
	if (sysout0)
	    sysout = sysout0;
	else if (srch!= NULL && srch->epoch != 0.0)
	    sysout = srch->coorsys;
	if (!sysout)
	    sysout = sysref;
	if (!sysout)
	    sysout = WCS_J2000;

	/* Set equinox from command line, search catalog, or searched catalog */
	if (eqout == 0.0) {
	    if (srch!= NULL && srch->equinox != 0.0)
		eqout = srch->equinox;
	    else if (sysout0 == WCS_J2000)
		eqout = 2000.0;
	    else if (sysout0 == WCS_B1950)
		eqout = 1950.0;
	    else if (eqcoor != 0.0)
		eqout = eqcoor;
	    else
		eqout = epref;
	    if (eqout == 0.0)
		eqout = 2000.0;
	    }

	/* Set epoch from command line, search catalog, or searched catalog */
	epout = epoch0;
	if (epout == 0.0) {
	    if (srch!= NULL && srch->epoch != 0.0)
		epout = srch->epoch;
	    else if (sysout0 == WCS_J2000)
		epout = 2000.0;
	    else if (sysout0 == WCS_B1950)
		epout = 1950.0;
	    else if (mprop != 1)
		epout = epref;
	    else if (eqout != 0.0)
		epout = eqout;
	    else {
		if (sysout == WCS_B1950)
		    epout = 1950.0;
		else
		    epout = 2000.0;
		}
	    }

	/* Set degree flag for output */
	if (sysout == WCS_ECLIPTIC || sysout == WCS_GALACTIC)
	    degout = 1;
	else
	    degout = degout0;
	setlimdeg (degout);

	/* Find stars specified by number */
	if (ranges != NULL) {

	    /* Default to keep stars by order read */
	    if (catsort == SORT_UNSET)
		catsort = SORT_NONE;

	    nfind = rgetn (range);
	    for (i = 0; i < nfind; i++)
		gnum[i] = rgetr8 (range);
	    wfile = 0;

	    /* Find the specified catalog stars */
	    if (nstars < 0)
		nlog = -1;
	    ng = ctgrnum (refcatname[icat], refcat,
		      nfind, sysout, eqout, epout, match, &starcat[icat],
		      gnum, gra, gdec, gpra, gpdec, gm, gc, gobj, nlog);
	    if (nlog < 0)
		return (ng);

	    if (gobj == NULL)
		gobj1 = NULL;
	    else if (gobj[0] == NULL)
		gobj1 = NULL;
	    else
		gobj1 = gobj;
	    /* if (ng > nfind)
		ns = nfind;
	    else */
		ns = ng;

	    /* Set flag if any proper motions are non-zero */
	    if (mprop == 1 && !oneline) {
		mprop = 0;
		for (i = 0; i < ng; i++) {
		    if (gpra[i] != 0.0 || gpdec[i] != 0.0) {
			mprop = 1;
			break;
			}
		    }
		}
	    if (mprop == 0 && starcat[icat] != NULL && starcat[icat]->entrv > 0)
		mprop = 2;

	    for (i = 0; i < ns; i++ ) {
		gx[i] = 0.0;
		gy[i] = 1.0;
		}

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
		for (i = 0; i < ns; i++ ) {
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

	    /* Check to see whether gc is set at all */
	    gcset = 0;
	    for (i = 0; i < ns; i++ ) {
		if (gc[i] != 0) {
		    gcset = 1;
		    break;
		    }
		}

	    /* Set flag for plate, class, or type column */
	    if (refcat == BINCAT || refcat == SAO  || refcat == PPM ||
		refcat == BSC || refcat == SDSS || refcat == SKY2K ||
		refcat == IRAS || refcat == HIP)
		typecol = 1;
	    else if ((refcat == GSC || refcat == GSCACT) && classd < -1)
		typecol = 3;
	    else if (refcat == GSC || refcat == GSCACT ||
		refcat == UJC || refcat == UB1 ||
		refcat == USAC || refcat == USA1   || refcat == USA2 ||
		refcat == UAC  || refcat == UA1    || refcat == UA2 ||
		refcat == BSC  || (refcat == TABCAT&&gcset))
		typecol = 2;
	    else if (starcat[icat] != NULL && starcat[icat]->sptype > 0)
		typecol = 1;
	    else
		typecol = 0;
	    if (mprop == 2)
		nmagr = nmag - 1;
	    else
		nmagr = nmag;
	    if (printepoch)
		nmagr = nmagr - 1;

	    /* Write out entries for use as image centers */
	    if (searchcenter) {
		gnmax = gnum[0];
		for (i = 1; i < ns; i++ ) {
		    if (gnum[i] > gnmax) gnmax = gnum[i];
		    }
		for (i = 0; i < ns; i++ ) {
		    if (sysref == WCS_XY) {
			num2str (rastr, gra[i], 10, 5);
			num2str (decstr, gdec[i], 10, 5);
			}
		    else if (degout) {
			deg2str (rastr, 32, gra[i], nddeg);
			deg2str (decstr, 32, gdec[i], nddeg);
			}
		    else {
			ra2str (rastr, 32, gra[i], ndra);
			dec2str (decstr, 32, gdec[i], nddec);
			}
		    wcscstr (cstr, sysout, eqout, epout);
		    if (printobj && gobj1 != NULL)
			printf ("%s %s %s %s\n",
				gobj[i], rastr, decstr, cstr);
		    else {
			CatNum (refcat, -nnfld, nndec, gnum[i], numstr);
			printf ("%s_%s %s %s %s\n",
				refcatname[icat],numstr,rastr,decstr,cstr);
			}
		    }
		return (ns);
		}

	    /* Sort catalogued objects, if requested */
	    if (ns > 1) {

		switch (catsort) {

		    /* Merge found catalog objects within rad of each other */
		    case SORT_MERGE:
			if (verbose)
			    fprintf (stderr, "SCAT: Merging %d stars\n", ns);
			if (rad0 == 0.0)
			    rad0 = 1.5;
			nns = MergeStars (gnum,gra,gdec,gpra,gpdec,gx,gy,gm,gc,gobj1,
				     ns,nmagmax,rad0,nlog);
			ns = nns;
			break;

		    /* Sort found catalog objects from closest to furthest */
		    case SORT_DIST:
			XSortStars (gnum,gra,gdec,gpra,gpdec,gx,gy,gm,gc,gobj1,
				    ns,nmagmax);
			break;

		    /* Sort found catalog objects by right ascension */
		    case SORT_RA:
			RASortStars (gnum,gra,gdec,gpra,gpdec,gx,gy,gm,gc,gobj1,
				     ns,nmagmax);
			break;

		    /* Sort found catalog objects by declination */
		    case SORT_DEC:
			DecSortStars(gnum,gra,gdec,gpra,gpdec,gx,gy,gm,gc,gobj1,
				     ns,nmagmax);
			break;

		    /* Sort found catalog objects from brightest to faintest */
		    case SORT_MAG:
			MagSortStars(gnum,gra,gdec,gpra,gpdec,gx,gy,gm,gc,gobj1,
				     ns,nmagmax,sortmag);
			break;

		    /* Sort found catalog objects by id number */
		    case SORT_ID:
			IDSortStars(gnum,gra,gdec,gpra,gpdec,gx,gy,gm,gc,gobj1,
				     ns,nmagmax);
			break;
		    }
		}
	    }

	/* Find catalog entries specified by date */
	else if (dec0 < -90.0 && epoch1 > 0.0 && epoch2 > 0.0) {

	    /* Default to keep stars by order read */
	    if (catsort == SORT_UNSET)
		catsort = SORT_NONE;

	    wfile = 0;

	    /* Find the specified catalog stars */
	    ng = ctgrdate (refcatname[icat], refcat,
		      sysout,eqout,epout,&starcat[icat],epoch1,epoch2,
		      ngmax,gnum,gra,gdec,gpra,gpdec,gm,gc,gobj,nlog);

	    if (ng < 1) {
		date1 = ep2fd (epoch1);
		date2 = ep2fd (epoch2);
		fprintf (stderr, "SCAT: No entries between %s and %s\n",
			 date1, date2);
		continue;
		}

	    if (gobj == NULL)
		gobj1 = NULL;
	    else if (gobj[0] == NULL)
		gobj1 = NULL;
	    else
		gobj1 = gobj;
	    /* if (ng > nfind)
		ns = nfind;
	    else */
		ns = ng;

	    /* Set flag if any proper motions are non-zero */
	    if (mprop == 1 && !oneline) {
		mprop = 0;
		for (i = 0; i < ng; i++) {
		    if (gpra[i] != 0.0 || gpdec[i] != 0.0) {
			mprop = 1;
			break;
			}
		    }
		}
	    if (mprop == 0 && starcat[icat] != NULL && starcat[icat]->entrv > 0)
		mprop = 2;

	    for (i = 0; i < ns; i++ ) {
		gx[i] = 0.0;
		gy[i] = 1.0;
		}

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
		for (i = 0; i < ns; i++ ) {
		    if (gnum[i] > maxnum)
			maxnum = gnum[i];
		    }
		nnfld = CatNumLen (refcat, maxnum, nndec);
		}

	    printepoch = 1;

	    /* Check to see whether gc is set at all */
	    gcset = 0;
	    for (i = 0; i < ns; i++ ) {
		if (gc[i] != 0) {
		    gcset = 1;
		    break;
		    }
		}

	    /* Set flag for plate, class, or type column */
	    if (refcat == BINCAT || refcat == SAO  || refcat == PPM ||
		refcat == BSC || refcat == SDSS || refcat == SKY2K)
		typecol = 1;
	    else if ((refcat == GSC || refcat == GSCACT) && classd < -1)
		typecol = 3;
	    else if (refcat == GSC || refcat == GSCACT ||
		refcat == UJC || refcat == UB1 ||
		refcat == USAC || refcat == USA1   || refcat == USA2 ||
		refcat == UAC  || refcat == UA1    || refcat == UA2 ||
		refcat == BSC  || (refcat == TABCAT&&gcset))
		typecol = 2;
	    else if (starcat[icat] != NULL && starcat[icat]->sptype > 0)
		typecol = 1;
	    else
		typecol = 0;
	    if (mprop == 2)
		nmagr = nmag - 1;
	    else
		nmagr = nmag;
	    if (printepoch)
		nmagr = nmagr - 1;


	    /* Sort catalogued objects, if requested */
	    if (ns > 1) {

		switch (catsort) {

		    /* Sort found catalog objects in image by right ascension */
		    case SORT_RA:
			RASortStars (gnum,gra,gdec,gpra,gpdec,gx,gy,gm,gc,gobj1,
				     ns,nmagmax);
			break;

		    /* Sort found catalog objects by declination */
		    case SORT_DEC:
			DecSortStars(gnum,gra,gdec,gpra,gpdec,gx,gy,gm,gc,gobj1,
				     ns,nmagmax);
			break;

		    /* Sort found catalog objects from brightest to faintest */
		    case SORT_MAG:
			MagSortStars(gnum,gra,gdec,gpra,gpdec,gx,gy,gm,gc,gobj1,
				     ns,nmagmax,sortmag);
			break;

		    /* Sort found catalog objects by ID number */
		    case SORT_ID:
			IDSortStars(gnum,gra,gdec,gpra,gpdec,gx,gy,gm,gc,gobj1,
				    ns,nmagmax);
			break;
		    }
		}
	    }

	/* Find stars specified by location */
	else {

	    /* Default to sort stars by magnitude */
	    if (catsort == SORT_UNSET)
		catsort = SORT_MAG;

	    /* Set search radius if finding closest star */
	    if (rad0 == 0.0 && dra0 == 0.0) {
		if (closest)
		    rad0 = CatRad (refcat);
		else
		    rad0 = 10.0;
		}

	    /* Set limits from defaults and command line information */
	    if (GetArea (verbose,syscoor,eqcoor,sysout,eqout,epout,
		     &cra,&cdec,&dra,&ddec,&drad,&dradi,&crao,&cdeco))
		return (0);

	    if (srch != NULL) {
		if (srchcat->stnum <= 0 && strlen (srch->objname) > 0) {
		    if (objname == NULL)
			objname = (char *) malloc (32);
		    strcpy (objname, srch->objname);
		    }
		else {
		    if (objname == NULL)
			objname = (char *) calloc (1,32);
		    nsfld = CatNumLen (TXTCAT, srch->num, srchcat->nndec);
		    CatNum (TXTCAT, nsfld, srchcat->nndec, srch->num, objname);
		    }
		}
	    nnfld = CatNumLen (refcat, 0.0, nndec);

	    /* Print search center and size in input and output coordinates */
	    if (verbose || (printhead && !oneline)) {
		if (sysout != syscoor || eqcoor != eqout)
		    SearchHead (icat,syscoor,eqcoor,epout,
				cra,cdec,dra,ddec,drad,dradi,nnfld,degout);
		SearchHead (icat,sysout,eqout,epout,
			crao,cdeco,dra,ddec,drad,dradi,nnfld,degout);
		if (!closest) {
		    if (sysref != syscoor && sysref != sysout) {
			double cra2 = cra;
			double cdec2 = cdec;
			wcscon (syscoor, sysref, 0.0, 0.0, &cra2, &cdec2, epout);
			SearchHead (icat,sysref,eqref, epref,cra2,cdec2,
				    dra,ddec,drad,dradi,nnfld,degout);
			}
		    }
		}

	    /* Set the magnitude limits for the catalog search */
	    if (maglim2 == 0.0) {
		mag1 = 0.0;
		mag2 = 0.0;
		}
	    else {
		mag1 = maglim1;
		mag2 = maglim2;
		}

	    /* Find the nearby reference stars, in ra/dec */
	    if (catsort == SORT_DIST)
		distsort = 1;
	    else
		distsort = 0;
	    if (sortmag > 9) {
		sortletter = (char) sortmag;
		sortmag = CatMagNum (sortmag, refcat);
		}
	    else if (sortmag > 0)
		sortletter = (char) (48 + sortmag);
	    else
		sortletter = ' ';
	    ng = ctgread (refcatname[icat], refcat, distsort, crao, cdeco,
		      dra,ddec,drad,dradi,sysout,eqout,epout,mag1,mag2,
		      sortmag,ngmax,&starcat[icat],
		      gnum,gra,gdec,gpra,gpdec,gm,gc,gobj,nlog);
	    if (ngmax < 1)
		return (ng);

	    if ((verbose || printhead) && ncat == 1 && ng < 1) {
		fprintf (stderr, "No stars found in %s\n",refcatname[icat]);
		return (0);
		}

	    if (gobj[0] == NULL)
		gobj1 = NULL;
	    else
		gobj1 = gobj;
	    if (ng > ngmax)
		ns = ngmax;
	    else
		ns = ng;

	    /* Set flag if any proper motions are non-zero */
	    if (mprop == 1 && !oneline) {
		mprop = 0;
		for (i = 0; i < ns; i++) {
		    if (gpra[i] != 0.0 || gpdec[i] != 0.0) {
			mprop = 1;
			break;
			}
		    }
		}

	    /* Convert coordinates to output coordinate system */
	    if (syscoor != sysout) {
		if (mprop == 1) {
		    for (i = 0; i < ns; i++) {
    			wcsconp (syscoor,sysout,eqout,eqout,epout,epout,
				 &gra[i],&gdec[i],&gpra[i],&gpdec[i]);
			}
		    }
		else {
		    for (i = 0; i < ns; i++) {
    			wcscon (syscoor,sysout,eqout,eqout,
				&gra[i],&gdec[i],epout);
			}
		    }
		}

	    /* Set flag if radial velocity is included in catalog entry */
	    if (mprop == 0 && starcat[icat] != NULL && starcat[icat]->entrv > 0)
		mprop = 2;

	    /* Check to see if epoch is contained in entries */
	    if (starcat[icat] != NULL && starcat[icat]->nepoch != 0)
		printepoch = 1;
	    else
		printepoch = 0;

	    /* Find largest catalog number to be printed */
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
		for (i = 0; i < ns; i++ ) {
		    if (gnum[i] > maxnum)
			maxnum = gnum[i];
		    }
		nnfld = CatNumLen (refcat, maxnum, nndec);
		}

	    /* Check to see whether gc is set at all */
	    gcset = 0;
	    for (i = 0; i < ns; i++ ) {
		if (gc[i] != 0) {
		    gcset = 1;
		    break;
		    }
		}

	    /* Set flag for plate, class, or type column */
	    if (refcat == BINCAT || refcat == SAO  || refcat == PPM ||
		refcat == BSC || refcat == SDSS || refcat == SKY2K)
		typecol = 1;
	    else if ((refcat == GSC || refcat == GSCACT) && classd < -1)
		typecol = 3;
	    else if (refcat == GSC || refcat == GSCACT ||
		refcat == UJC ||  refcat == UB1 ||
		refcat == USAC || refcat == USA1   || refcat == USA2 ||
		refcat == UAC  || refcat == UA1    || refcat == UA2 ||
		refcat == BSC  || (refcat == TABCAT&&gcset))
		typecol = 2;
	    else if (starcat[icat] != NULL && starcat[icat]->sptype > 0)
		typecol = 1;
	    else
		typecol = 0;

	    /* Set number of magnitudes (n-1 if radial velocity present) */
	    if (mprop == 2)
		nmagr = nmag - 1;
	    else
		nmagr = nmag;
	    if (printepoch)
		nmagr = nmagr - 1;

	    /* Compute distance from star to search center */
	    for (i = 0; i < ns; i++ ) {
		gx[i] = wcsdist (crao, cdeco, gra[i], gdec[i]);
		gy[i] = 1.0;
		}

	    /* Sort catalogued objects, if requested */
	    if (ns > 1) {

		/* Sort found catalog objects from closest to furthest */
		if (catsort == SORT_DIST)
		    XSortStars (gnum,gra,gdec,gpra,gpdec,gx,gy,gm,gc,gobj1,ns,
				nmagmax);

		/* Sort found catalog objects by right ascension */
		else if (catsort == SORT_RA)
		    RASortStars (gnum,gra,gdec,gpra,gpdec,gx,gy,gm,gc,gobj1,ns,
				 nmagmax);

		/* Sort found catalog objects by declination */
		else if (catsort == SORT_DEC)
		    DecSortStars(gnum,gra,gdec,gpra,gpdec,gx,gy,gm,gc,gobj1,ns,
				 nmagmax);

		/* Sort found catalog objects from brightest to faintest */
		else if (catsort == SORT_MAG)
		    MagSortStars(gnum,gra,gdec,gpra,gpdec,gx,gy,gm,gc,gobj1,ns,
				 nmagmax,sortmag);

		/* Sort found catalog objects by ID number */
		else if (catsort == SORT_ID)
		    IDSortStars(gnum,gra,gdec,gpra,gpdec,gx,gy,gm,gc,gobj1,
				ns,nmagmax);
		}

	    /* Print one line with search center and found star */
	    if (oneline) {
		if (ns > 0) {
		    das = dra0 * 3600.0;
		    dds = ddec0 * 3600.0;
		    drs = rad0;
		    if (drs < 0.0)
			drs = -drs;
		    else if (drs == 0.0)
			drs = sqrt (das*das + dds*dds);
		    if (das <= 0.0) {
			das = drs;
			dds = drs;
			}
		    if (srchcat != NULL) {
			smag = srchcat->nmag;
			if (srchcat->entrv > 0)
			    smag = smag - 1;
			if (srchcat->nepoch)
			    smag = smag - 1;
			}
		    else
			smag = 1;

		    if (nohead && tabout) {

			/* Write tab table heading */
			catalog = CatName (refcat, refcatname[icat]);
			printf ("catalog	%s\n", catalog);
			if (listfile != NULL)
			    printf ("search	%s\n", listfile);
			if (sysout == WCS_GALACTIC)
			    printf ("radecsys	galactic\n");
			else if (sysout == WCS_ECLIPTIC)
			    printf ("radecsys	ecliptic\n");
			else if (sysout == WCS_B1950)
			    printf ("radecsys	fk4\n");
			else
			    printf ("radecsys	fk5\n");
			printf ("equinox	%.4f\n", eqout);
			if (!printepoch)
			    printf ("epoch	%.4f\n", epout);
			if (minid != 0)		/* Min number of plate IDs for USNO-B1.0 */
			    printf ("minid	%d\n", minid);
			if (minpmqual > 0)	/* Min proper motion quality for USNO-B1.0 */
			    printf ("minpmq	%d\n", minpmqual);
			if (dra0 > 0.0 || rad0 < 0) {
			    if (syscoor == WCS_GALACTIC) {
				printf ("glonsec	%.2f\n", das);
				printf ("glatsec	%.2f\n", dds);
				}
			    else if (syscoor == WCS_ECLIPTIC) {
				printf ("elonsec	%.2f\n", das);
				printf ("elatsec	%.2f\n", dds);
				}
			    else {
				printf ("drasec	%.2f\n", das);
				printf ("ddecsec	%.2f\n", dds);
				}
			    }
			else if (rad0 > 0)
			    printf ("radsec	%.2f\n", drs);
			if (mprop==1 || (srchcat != NULL && srchcat->mprop==1)) {
			    if (degout)
				printf ("pmunit	mas/yr\n");
			    else {
				printf ("rpmunit	mas/yr\n");
				printf ("dpmunit	mas/yr\n");
				}
			    }
		        printf ("program	%s %s\n", progname, RevMsg);

		        /* Write column headings */
		        if (srch != NULL) {
			    if (srchcat->keyid[0] > 0) {
				printf ("%s", srchcat->keyid);
				nc = strlen (srchcat->keyid);
				}
			    else {
				printf ("srch_id");
				nc = 7;
				}
			    if (srchcat->nnfld > nc) {
				for (i = nc; i < srchcat->nnfld; i++)
				    printf (" ");
				}
			    printf ("	");
			    }
		        printf ("srch_ra     	srch_dec    	");
		        if (srchcat != NULL) {
			    if (smag > 0) {
				for (imag = 0; imag < smag; imag++) {
				    if (strlen (srchcat->keymag[imag]) >0)
					printf ("s_%s	", srchcat->keymag[imag]);
				    else if (smag > 1)
					printf ("s_mag%d	", imag+1);
				    else
					printf ("s_mag	");
				    }
				}
			    if (srchcat->nepoch)
				printf ("epoch    	");
			    if (srchcat->sptype) {
				if (strlen (srch->isp) > 2) {
				    padspt = 1;
				    if (strlen (srchcat->keytype) >0)
					printf ("s_%s 	",srchcat->keytype);
				    else
					printf ("s_type  	");
				    }
				else {
				    padspt = 0;
				    if (strlen (srchcat->keytype) >0)
					printf ("s_%s	",srchcat->keytype);
				    else
					printf ("s_type	");
				    }
				}
			    if (srchcat->entrv > 0) {
				if ((lrv = strlen (srchcat->keyrv)) >0) {
				    sprintf (tstr, "%s	            ",
					     srchcat->keyrv);
				    tstr[9] = (char)0;
				    printf ("%s	", tstr);
				    }
				else
				    printf ("s_rv    	");
				}
			    if (srchcat->mprop > 0)
				    printf ("s_pra 	s_pdec	");
				
			    }
			if (refcat == TABCAT && starcat[icat]->keyid[0] >0) {
			    strcpy (headline, starcat[icat]->keyid);
			    strcat (headline, "                ");
			    }
			else
			    CatID (headline, refcat);
			headline[nnfld] = (char) 0;
			printf ("%s", headline);
			printf ("	ra          	dec        	");
			if (refcat == GSC2)
			    printf ("magf 	magj 	magn 	magv	");
			else if (refcat == HIP)
			    printf ("magb 	magv 	parlx	parer	");
			else if (refcat == SKYBOT)
			    printf ("magv 	gdist 	hdist	");
			else if (refcat == IRAS)
			    printf ("f10m 	f25m 	f60m 	f100m	");
			else if (refcat == TMPSC || refcat == TMIDR2)
			    printf ("magj   	magh    	magk   		");
			else if (refcat == TMPSCE)
			    printf ("magje 	maghe  	magke  	");
			else if (refcat == TMXSC)
			    printf ("magj  	magh   	magk   	size  	");
			else if (refcat == UB1)
			    printf ("magb1	magr1	magb2	magr2	magn 	");
			else if (refcat == YB6)
			    printf ("magb 	magr 	magj 	magh 	magk 	");
			else if (refcat == SDSS)
			    printf ("magu 	magg 	magr 	magi 	magz 	");
			else if (refcat == UCAC2)
			    printf ("raerr	decerr	magj 	magh 	magk 	magc 	");
			else if (refcat == UCAC3)
			    printf ("raerr	decerr	magb 	magr 	magi 	magj 	magh 	magk 	magm 	maga 	");
			else if (nmagr > 0) {
			    for (imag = 0; imag < nmagr; imag++) {
				if (starcat[icat] != NULL &&
				    strlen (starcat[icat]->keymag[imag]) >0)
				    printf ("%s	", starcat[icat]->keymag[imag]);
				else if (nmagr > 1)
				    printf ("mag%d  ", imag+1);
				else
				    printf ("mag    ");
				}
			    }
			if (typecol == 1)
			    printf ("spt   	");
			if (mprop == 1)
			    printf ("pmra  	pmdec 	");
			if (refcat == UCAC2 || refcat == UCAC3)
			    printf ("epmra	epmdec	ni	nc	");
			if (refcat == UB1)
			    printf ("pm	ni	sg	");
			if (refcat == GSC2)
			    printf ("class	");
			if ((starcat[icat]!=NULL && starcat[icat]->entrv>0) &&
			    mprop == 2)
			    printf ("velocity	");
			printf ("n 	dra");
			for (i = 3; i < LenNum(das,2); i++)
			    printf (" ");
			printf ("	ddec");
			for (i = 4; i < LenNum(dds,2); i++)
			    printf (" ");
			printf ("	drad");
			for (i = 4; i < LenNum(drs,2); i++)
			    printf (" ");
			printf ("\n");
			if (srch != NULL) {
			    printf ("-------");
			    if (srchcat->nnfld > 7) {
				for (i = 8; i < srchcat->nnfld; i++)
				    printf ("-");
				}
			    printf ("	");
			    }
			printf ("------------	------------	");
			if (srchcat != NULL) {
			    for (imag = 0; imag < smag; imag++)
				printf ("-----	");
			    if (srchcat->nepoch)
				printf ("---------	");
			    if (srchcat->sptype != 0) {
				if (padspt)
				    printf ("-------	");
				else
				    printf ("----	");
				}
			    if (srchcat->entrv > 0)
				printf ("---------	");
			    if (srchcat->mprop == 1)
				printf ("------	------	");
			    }
			strcpy (headline,"----------------------");
			headline[nnfld] = (char) 0;
			printf ("%s", headline);
			printf ("	------------	------------	");
			if (refcat == UCAC2 || refcat == UCAC3)
			    printf ("-----	-----	");
			if (refcat == GSC2)
			    printf ("-----	-----	-----	-----	");
			else if (refcat == HIP || refcat == IRAS)
			    printf ("-----	-----	-----	-----	");
			else if (refcat == TMPSC || refcat == TMIDR2)
			    printf ("-------	-------	-------	");
			else if (refcat == TMPSCE)
			    printf ("-------	-------	-------	-------	-------	-------	");
			else if (refcat == TMXSC)
			    printf ("-------	-------	-------	------	");
			else if (nmagr > 0) {
			    for (imag = 0; imag < nmagr; imag++)
				printf ("-----	");
			    }
			if (typecol == 1)
			    printf ("---	");
			if ((starcat[icat]!=NULL && starcat[icat]->entrv>0) &&
			    mprop == 2)
			    printf ("---------	");
			if (mprop == 1)
			    printf ("------	------	");
			if (refcat == UCAC2 || refcat == UCAC3)
			    printf ("-----	-----	--	--	");
			if (refcat == UB1)
			    printf ("--	--	--	");
			printf ("--	");
			for (i = 0; i < LenNum(das,2); i++)
			    printf ("-");
			printf ("	");
			for (i = 0; i < LenNum(dds,2); i++)
			    printf ("-");
			printf ("	");
			for (i = 0; i < LenNum(drs,2); i++)
			    printf ("-");
			printf ("\n");
			nohead = 0;
			}
		    if (srch != NULL) {
			if (srchcat->keyid[0] > 0 && strlen (srch->objname))
			    strcpy (numstr, srch->objname);
			else if (srchcat->stnum <= 0 &&
			    strlen (srch->objname) > 0)
			    strcpy (numstr, srch->objname);
			else
			    CatNum (TXTCAT,-srchcat->nnfld,srchcat->nndec,
				    srch->num,numstr);
		        if (tabout)
			    printf ("%s	", numstr);
		        else
			    printf ("%s ", numstr);
			}
		    if (degout) {
			num2str (rastr, crao, 12, nddeg);
			num2str (decstr, cdeco, 12, nddeg);
			}
		    else {
			ra2str (rastr, 32, crao, ndra);
			dec2str (decstr, 32, cdeco, nddec);
			}
		    if (tabout)
			printf ("%s	%s", rastr, decstr);
		    else
			printf ("%s %s", rastr, decstr);
		    if (srchcat != NULL && srch != NULL) {
			if (smag > 0) {
			    for (imag = 0; imag < smag; imag++) {
				if (tabout) {
				    if (srch->xmag[imag] > 100.0)
					printf ("	%5.2fL", srch->xmag[imag]-100.0);
				    else
					printf ("	%5.2f", srch->xmag[imag]);
				    }
				else {
				    if (srch->xmag[imag] > 100.0)
					printf (" %5.2fL", srch->xmag[imag]-100.0);
				    else
					printf (" %5.2f", srch->xmag[imag]);
				    }
				}
			    }
			if (srchcat->nepoch) {
			    ep2dt (srch->epoch, &date, &time);
			    if (tabout)
				printf ("	%9.4f", date);
			    else
				printf (" %9.4f", date);
			    }
			if (srchcat->sptype != 0) {
			    if (padspt) {
				for (i = 0; i < 7; i++) {
				    if (srch->isp[i] == (char) 0)
					srch->isp[i] = ' ';
				    }
				srch->isp[7] = (char) 0;
				if (tabout)
				    printf ("	%7s", srch->isp);
				else
				    printf ("  %7s ", srch->isp);
				}
			    else if (tabout)
				printf ("	%s", srch->isp);
			    else
				printf ("  %s ", srch->isp);
			    }
			if (srchcat->entrv > 0) {
			    if (tabout)
				printf ("	%9.2f", srch->radvel);
			    else
				printf (" %9.2f", srch->radvel);
			    }
			if (srchcat->mprop == 1) {
			    pra = srch->rapm * 3600000.0 * cosdeg (srch->dec);
			    pdec = srch->decpm * 3600000.0;
			    if (tabout)
				printf ("	%6.1f	%6.1f", pra, pdec);
			    else
				printf (" %6.1f %6.1f", pra, pdec);
			    }
			}
		    if (mprop == 1) {
			pra = gpra[0] * 3600000.0 * cosdeg (gdec[0]);
			pdec = gpdec[0] * 3600000.0;
			}

		    /* Set up object name or number to print */
		    if (refcat == SDSS || refcat == GSC2 || refcat == SKYBOT)
			strcpy (numstr, gobj[0]);
		    else if (starcat[icat] != NULL) {
			if (starcat[icat]->stnum < 0 && gobj1 != NULL) {
			    strncpy (numstr, gobj1[0], 79);
			    if (lofld > 0) {
				for (j = 0; j < lofld; j++) {
				    if (!numstr[j])
					numstr[j] = ' ';
				    }
				}
			    }
			else
			    CatNum (refcat,-nnfld,starcat[icat]->nndec,gnum[0],numstr);
			}
		    else
			CatNum (refcat, -nnfld, nndec, gnum[0], numstr);
		    /* if (gobj1 != NULL) {
			if (strlen (gobj1[0]) > 0)
			    strcpy (numstr, gobj1[0]);
			}
		    if (starcat[icat] != NULL)
			CatNum (refcat,-nnfld,starcat[icat]->nndec,gnum[0],numstr);
		    else
			CatNum (refcat,-nnfld,nndec,gnum[0],numstr);  */
		    if (degout) {
			num2str (rastr, gra[0], 12, nddeg);
			num2str (decstr, gdec[0], 12, nddeg);
			}
		    else {
			ra2str (rastr, 32, gra[0], ndra);
			dec2str (decstr, 32, gdec[0], nddec);
			}
		    if (tabout)
			printf ("	%s	%s	%s",
			        numstr, rastr, decstr);
		    else
			printf (" %s %s %s",
			        numstr, rastr, decstr);
		    if (refcat == UCAC2 || refcat == UCAC3) {
			era = gm[nmagr][0] * cosdeg (gdec[i]) * 3600.0;
			edec = gm[nmagr+1][0] * 3600.0;
			printf ("	%5.3f	%5.3f", era, edec);
			}
		    if (refcat == GSC2) {
			if (tabout)
			    printf ("	%5.2f	%5.2f	%5.2f	%5.2f %5.2f",
				    gm[0][0],gm[1][0],gm[2][0],gm[4][0],gm[5][0]);
			else
			    printf (" %5.2f %5.2f %5.2f %5.2f %5.2f",
				    gm[0][0],gm[1][0],gm[2][0],gm[4][0],gm[5][0]);
			}
		    else if (refcat == HIP) {
			if (tabout)
			    printf ("	%5.2f	%5.2f	%5.2f	%5.2f",
				    gm[0][0], gm[1][0], gm[2][0], gm[3][0]);
			else
			    printf (" %5.2f %5.2f %5.2f %5.2f",
				    gm[0][0], gm[1][0], gm[2][0], gm[3][0]);
			}
		    else if (refcat == IRAS) {
			for (imag = 0; imag < 4; imag++) {
			    if (gm[imag][0] > 100.0) {
				flux = 1000.0 * pow (10.0,-(gm[imag][0]-100.0)/2.5);
				if (tabout)
				    printf ("	%.2fL", flux);
				else
				    printf (" %5.2fL", flux);
				}
			    else {
				flux = 1000.0 * pow (10.0,-gm[imag][0]/2.5);
				if (tabout)
				    printf ("	%.2f ", flux);
				else
				    printf (" %5.2f ", flux);
				}
			    }
			}
		    else if (refcat == TMPSC || refcat == TMIDR2 ||
			     refcat == TMPSCE || refcat == TMXSC) {
			for (imag = 0; imag < 3; imag++) {
			    if (gm[imag][0] > 100.0) {
				if (tabout)
				    printf ("	%6.3fL", gm[imag][0]-100.0);
				else
				    printf (" %6.3fL", gm[imag][0]-100.0);
				}
			    else {
				if (tabout)
				    printf ("	%6.3f ", gm[imag][0]);
				else
				    printf (" %6.3f ", gm[imag][0]);
				}
			    }
			if (refcat == TMPSCE) {
			    for (imag = 3; imag < 6; imag++) {
				if (tabout)
				    printf ("	%6.3f ", gm[imag][0]);
				else
				    printf (" %6.3f ", gm[imag][0]);
				}
			    }
			if (refcat == TMXSC) {
			    if (tabout)
				printf("	%6.1f",((double)gc[0])* 0.1);
			    else
				    printf (" %6.1f", ((double)gc[0])*0.1);
			    }
			}
		    else if (nmagr > 0) {
			for (imag = 0; imag < nmagr; imag++) {
			    if (tabout)
				printf ("	%5.2f", gm[imag][0]);
			    else
				printf (" %5.2f", gm[imag][0]);
			    }
			}
		    if (typecol == 1) {
			isp[0] = gc[0] / 1000;
			isp[1] = gc[0] % 1000;
			if (isp[0] == ' ' && isp[1] == ' ') {
			    isp[0] = '_';
			    isp[1] = '_';
			    }
			if (tabout)
			    printf ("	%2s ", isp);
			else
			    printf (" %2s ", isp);
			}
		    if (starcat[icat] != NULL && starcat[icat]->entrv>0) {
			if (tabout)
			    printf ("	%9.2f", gm[nmagr][0]);
			else
			    printf (" %9.2f", gm[nmagr][0]);
			}
		    if (starcat[icat] != NULL && starcat[icat]->nepoch>0) {
			if (starcat[icat]->entrv>0)
			    dtemp = DateString (gm[nmagr+1][0], tabout);
			else
			    dtemp = DateString (gm[nmagr][0], tabout);
			printf ("%s", dtemp);
			free (dtemp);
			}
		    if (mprop == 1) {
			if (tabout)
			    printf ("	%6.1f	%6.1f", pra, pdec);
			else
			    printf (" %6.1f %6.1f", pra, pdec);
			}
		    if (refcat == UCAC2 || refcat == UCAC3) {
			epmr = gm[nmagr+2][0] * cosdeg (gdec[i]) * 3600000.0;
			epmd = gm[nmagr+3][0] * 3600000.0;
			nim = gc[0] / 1000;
			nct = gc[0] % 1000;
			if (tabout)
			    printf ("	%5.1f	%5.1f	%2d	%2d",
				    epmr, epmd, nim, nct);
			else
			    printf (" %5.1f %5.1f $3d $3d",
				    epmr, epmd, nim, nct);
			}
		    if (refcat == UB1) {
			if (tabout)
			    printf ("	%2d	%2d	%2d",
				    gc[0]%10000/100, gc[0]%100, gc[0]/10000);
			else
			    printf (" %2d %2d %2d",
				    gc[0]%10000/100, gc[0]%100, gc[0]/10000);
			}
		    if (refcat == GSC2) {
			if (tabout)
			    printf ("	%d", gc[0]);
			else
			    printf ("  %2d  ", gc[0]);
			}

		    /* Number of stars in search radius */
		    if (tabout)
			printf ("	%d", ng);
		    else
			printf (" %d", ng);
		    dec = (gdec[0] + cdeco) * 0.5;
		    if (degout) {
			if ((gra[0] - crao) > 180.0)
			    da = gra[0] - crao - 360.0;
			else if ((gra[0] - crao) < -180.0)
			    da = gra[0] - crao + 360.0;
			else
			    da = gra[0] - crao;
			dd = gdec[0] - cdeco;
			gdist = sqrt (da*da + dd*dd);
			ndist = 5;
			}
		    else {
			if ((gra[0] - crao) > 180.0)
			    da = 3600.0*(gra[0]-crao-360.0)*cos(degrad(dec));
			else if ((gra[0] - crao) < -180.0)
			    da = 3600.0*(gra[0]+360.0-crao)*cos(degrad(dec));
			else
			    da = 3600.0 * (gra[0] - crao) * cos (degrad (dec));
			dd = 3600.0 * (gdec[0] - cdeco);
			gdist = 3600.0 * gx[0];
			ndist = 2;
			}
		    if (tabout)
			printf ("	");
		    else
			printf (" ");
		    PrintNum (das, da, ndist);
		    if (tabout)
			printf ("	");
		    else
			printf (" ");
		    PrintNum (dds, dd, ndist);
		    if (tabout)
			printf ("	");
		    else
			printf (" ");
		    PrintNum (drs, gdist, ndist);
		    printf ("\n");
		    }
		notprinted = 0;
		continue;
		}

	    /* List the brightest or closest MAXSTARS reference stars */
	    if (sortmag > 0 && sortmag <= nmag)
		magsort = sortmag - 1;
	    else
		magsort = 0;
	    CatMagName (sortmag, refcat, magname);
	    if (ng > ngmax) {
		if ((verbose || printhead) && !closest) {
		    if (distsort) {
			if (ng > 1)
			    printf ("Closest %d / %d %s (closer than %.2f arcsec)",
				ns, ng, title, 3600.0*gx[ns-1]);
		        else
			    printf ("Closest of %d %s",ng, title);
			}
		    else if (maglim1 > 0.0) {
			double magmin, magmax;
			magmin = gm[magsort][0];
			magmax = gm[magsort][0];
			for (is = 0; is < ns; is++) {
			    if (gm[magsort][is] > magmax)
				magmax = gm[magsort][is];
			    if (gm[magsort][is] < magmin)
				magmax = gm[magsort][is];
			    }
			printf ("%d / %d %s (%s between %.2f and %.2f)",
			    ns, ng, title, magname, magmin, magmax);
			}
		    else {
			double magmax;
			magmax = gm[magsort][0];
			for (is = 0; is < ns; is++) {
			    if (gm[magsort][is] > magmax)
				magmax = gm[magsort][is];
			    }
			printf ("%d / %d %s (%s brighter than %.2f)",
		 	    ns, ng, title, magname, magmax);
			}
		    printf ("\n");
		    }
		}
	    else {
	        if (verbose || printhead) {
		    if (maglim1 > 0.0)
			printf ("%d %s, %s between %.2f and %.2f\n",
			    ng, title, magname, maglim1, maglim2);
		    else if (maglim2 > 0.0)
			printf ("%d %s, %s brighter than %.2f\n",
			    ng, title, magname, maglim2);
		    else if (verbose)
			printf ("%d %s\n", ng, title);
		    }
		}

	    /* Open result catalog file */
	    if (wfile && icat == 0) {
		if (objname)
		    strcpy (filename,objname);
		else
		    strcpy (filename,"search");
		for (i = 0; i < ncat; i++) {
		    strcat (filename,".");
		    strcat (filename,refcatname[icat]);
		    }
		if (printxy)
		    strcat (filename,".match");

		if (afile)
		    fd = fopen (filename, "a");
		else
		    fd = fopen (filename, "w");

		/* Free result arrays and return if cannot write file */
		if (fd == NULL) {
		    if (afile)
			fprintf (stderr, "%s:  cannot append to file %s\n",
				 cpname, filename);
		    else
			fprintf (stderr, "%s:  cannot write file %s\n",
				 cpname, filename);
        	    return (0);
		    }
		}
            }

	/* Write heading */
	if (votab) {
	    nf = vothead(refcat,refcatname[icat],mprop,typecol,ns,cra,cdec,drad);
	    if (strlen (voerror) > 0) {
		printf ("<TR>\n<TD>ERROR: %s</TD>", voerror);
		for (i = 1; i < nf; i++)
		    printf ("<TD/>");
		printf ("\n</TR>/n");
		vottail ();
		return (ns);
		}
	    }

	else if (ng == 0 && (!tabout || (tabout && !printabhead))) {
	    if (verbose)
		printf ("No %s Stars Found\n", title);
	    }

	else if (tabout && nohead) {
	    catalog = CatName (refcat, refcatname[icat]);
	    sprintf (headline, "catalog	%s", catalog);
	    if (wfile)
		fprintf (fd, "%s\n", headline);
	    else
		printf ("%s\n", headline);

	    if (!ranges) {
		if (sysout == WCS_GALACTIC) {
		    num2str (rastr, crao, 12, nddeg);
		    if (wfile)
			fprintf (fd, "glon	%s\n", rastr);
		    else
			printf ("glon	%s\n", rastr);
		    num2str (decstr, cdeco, 12, nddeg);
		    if (wfile)
			fprintf (fd, "glat	%s\n", decstr);
		    else
			printf ("glat	%s\n", decstr);
		    }
		else if (sysout == WCS_ECLIPTIC) {
		    num2str (rastr, crao, 12, nddeg);
		    if (wfile)
			fprintf (fd, "elon	%s\n", rastr);
		    else
			printf ("elon	%s\n", rastr);
		    num2str (decstr, cdeco, 12, nddeg);
		    if (wfile)
			fprintf (fd, "elat	%s\n", decstr);
		    else
			printf ("elat	%s\n", decstr);
		    }
		else if (degout) {
		    num2str (rastr, crao, 12, nddeg);
		    if (wfile)
			fprintf (fd, "ra	%s\n", rastr);
		    else
			printf ("ra	%s\n", rastr);
		    num2str (decstr, cdeco, 12, nddeg);
		    if (wfile)
			fprintf (fd, "dec	%s\n", decstr);
		    else
			printf ("dec	%s\n", decstr);
		    }
		else {
		    ra2str (rastr, 32, crao, ndra);
		    if (wfile)
			fprintf (fd, "ra	%s\n", rastr);
		    else
			printf ("ra	%s\n", rastr);
		    dec2str (decstr, 32, cdeco, nddec);
		    if (wfile)
			fprintf (fd, "dec	%s\n", decstr);
		    else
			printf ("dec	%s\n", decstr);
		    }
		if (syscoor == WCS_GALACTIC && syscoor != sysout) {
		    num2str (rastr, cra, 12, nddeg);
		    if (wfile)
			fprintf (fd, "glon	%s\n", rastr);
		    else
			printf ("glon	%s\n", rastr);
		    num2str (decstr, cdec, 12, nddeg);
		    if (wfile)
			fprintf (fd, "glat	%s\n", decstr);
		    else
			printf ("glat	%s\n", decstr);
		    }
		if (syscoor == WCS_ECLIPTIC && syscoor != sysout) {
		    num2str (rastr, cra, 12, nddeg);
		    if (wfile)
			fprintf (fd, "elon	%s\n", rastr);
		    else
			printf ("elon	%s\n", rastr);
		    num2str (decstr, cdec, 12, nddeg);
		    if (wfile)
			fprintf (fd, "elat	%s\n", decstr);
		    else
			printf ("elat	%s\n", decstr);
		    }
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
	
	    if (wfile) {
		if (sysout == WCS_GALACTIC)
		    fprintf (fd,"radecsys	galactic\n");
		else if (sysout == WCS_ECLIPTIC)
		    fprintf (fd,"radecsys	ecliptic\n");
		else if (sysout == WCS_B1950)
		    fprintf (fd,"radecsys	fk4\n");
		else
		    fprintf (fd,"radecsys	fk5\n");
		fprintf (fd, "equinox	%.4f\n", eqout);
		if (!printepoch)
		    fprintf (fd, "epoch	%.4f\n", epout);
		}
	    else {
		if (sysout == WCS_GALACTIC)
		    printf ("radecsys	galactic\n");
		else if (sysout == WCS_ECLIPTIC)
		    printf ("radecsys	ecliptic\n");
		else if (sysout == WCS_B1950)
		    printf ("radecsys	fk4\n");
		else
		    printf ("radecsys	fk5\n");
		printf ("equinox	%.4f\n", eqout);
		if (!printepoch)
		    printf ("epoch	%.4f\n", epout);
		}

	    if (mprop == 1) {
		if (wfile) {
		    fprintf (fd, "rpmunit	mas/year\n");
		    fprintf (fd, "dpmunit	mas/year\n");
		    }
		else {
		    printf ("rpmunit	mas/year\n");
		    printf ("dpmunit	mas/year\n");
		    }
		}

	    das = dra0  / 3600.0;
	    dds = ddec0 / 3600.0;
	    drs = rad0;
	    if (drs < 0.0)
		drs = -drs;
	    else if (drs == 0.0)
		drs = sqrt (das*das + dds*dds);
	    if (das <= 0.0) {
		das = drs;
		dds = drs;
		}
 	    if (dra0 > 0.0 || rad0 < 0.0) {
		if (syscoor == WCS_GALACTIC || syscoor == WCS_ECLIPTIC) {
		    if (wfile) {
			fprintf (fd, "dlonsec	%.2f\n", das);
			fprintf (fd, "dlatsec	%.2f\n", dds);
			}
		    else {
			printf ("dlonsec	%.2f\n", das);
			printf ("dlatsec	%.2f\n", dds);
			}
		    }
		else {
		    if (wfile) {
			fprintf (fd, "drasec	%.2f\n", das);
			fprintf (fd, "ddecsec	%.2f\n", dds);
			}
		    else {
			printf ("drasec	%.2f\n", das);
			printf ("ddecsec	%.2f\n", dds);
			}
		    }
		}
	    else if (rad0 > 0) {
		if (wfile)
		    fprintf (fd, "radsec	%.2f\n", drs);
		else
		    printf ("radsec	%.2f\n", drs);
		}

	    if (catsort > 0) {
		switch (catsort) {
		    case SORT_DEC:
			if (wfile)
			    fprintf (fd, "catsort	dec\n");
			else
			    printf ("catsort	dec\n");
			break;
		    case SORT_DIST:
			if (wfile)
			    fprintf (fd, "catsort	dist\n");
			else
			    printf ("catsort	dist\n");
			break;
		    case SORT_MAG:
			if (wfile)
			    fprintf (fd, "catsort	mag%c\n", sortletter);
			else
			    printf ("catsort	mag%c\n", sortletter);
			break;
		    case SORT_RA:
			if (wfile)
			    fprintf (fd, "catsort	ra\n");
			else
			    printf ("catsort	ra\n");
			break;
		    case SORT_ID:
			if (wfile)
			    fprintf (fd, "catsort	id\n");
			else
			    printf ("catsort	id\n");
			break;
		    default:
			break;
		    }
		}

	    if (wfile)
		fprintf (fd, "program	%s %s\n", progname, RevMsg);
	    else
		printf ("program	%s %s\n", progname, RevMsg);

	    /* Print column headings */
	    if (refcat == TABCAT && strlen(starcat[icat]->keyid) > 0)
		sprintf (headline,"%s          ", starcat[icat]->keyid);
	    else if (refcat == SKYBOT) {
		strcpy (headline, "object");
		for (i = 6; i < nnfld; i++)
		    strcat (headline, " ");
		}
	    else
		CatID (headline, refcat);
	    headline[nnfld] = (char) 0;

	    if (sysout == WCS_GALACTIC)
		strcat (headline,"	long_gal   	lat_gal  ");
	    else if (sysout == WCS_ECLIPTIC)
		strcat (headline,"	long_ecl   	lat_ecl  ");
	    else if (sysout == WCS_B1950)
		strcat (headline,"	ra1950      	dec1950  ");
	    else
		strcat (headline,"	ra      	dec      ");
	    if (refcat == UCAC2 || refcat == UCAC3)
		strcat (headline,"	raerr	decerr");
	    if (refcat == USAC || refcat == USA1 || refcat == USA2 ||
		refcat == UAC  || refcat == UA1  || refcat == UA2)
		strcat (headline,"	magb	magr	plate");
	    else if (refcat == TMPSC || refcat == TMIDR2)
		strcat (headline,"	magj  	magh  	magk  ");
	    else if (refcat == TMPSCE)
		strcat (headline,"	magj  	magh  	magk 	magje 	maghe 	magke ");
	    else if (refcat == TMXSC)
		strcat (headline,"	magj  	magh  	magk  	size  ");
	    else if (refcat == UB1)
		strcat (headline, "	magb1	magr1	magb2	magr2	magn 	pm	ni	sg");
	    else if (refcat == YB6)
		strcat (headline, "	magb 	magr 	magj 	magh 	magk ");
	    else if (refcat == SDSS)
		strcat (headline, "	magu 	magg 	magr 	magi 	magz ");
	    else if (refcat == GSC2)
		strcat (headline,"	magf	magj	magn  	magv ");
	    else if (refcat == UCAC2)
		strcat (headline,"	magj	magh	magk  	magc ");
	    else if (refcat == UCAC3)
		strcat (headline,"	magb 	magr 	magi 	magj 	magh 	magk 	magm 	maga ");
	    else if (refcat == SKYBOT)
		strcat (headline,"	magv	gdist	hdist ");
	    else if (refcat == IRAS)
		strcat (headline,"	f10m  	f25m  	f60m   	f100m ");
	    else if (refcat == HIP)
		strcat (headline,"	magb	magv	parlx 	parer");
	    else if (refcat==TYCHO || refcat==TYCHO2 || refcat==ACT)
		strcat (headline,"	magb	magv");
	    else if (refcat==TYCHO2E)
		strcat (headline,"	magb 	magv 	magbe	magve");
	    else if (refcat==SKY2K)
		strcat (headline,"	magb 	magv 	magph	magpv");
	    else if (refcat==HIP)
		strcat (headline,"	magb 	magv 	prllx	parer");
	    else if (refcat==SKYBOT)
		strcat (headline,"	magv 	gdist  	hdist");
	    else if (refcat == GSC || refcat == GSCACT)
		strcat (headline,"	mag	class	band	N");
	    else if (refcat == UJC)
		strcat (headline,"	mag	plate");
	    else if (nmagr > 0) {
		for (imag = 0; imag < nmagr; imag++) {
		    if (starcat[icat] != NULL &&
			strlen (starcat[icat]->keymag[imag]) > 0)
			sprintf (temp, "	%s ", starcat[icat]->keymag[imag]);
		    else if (nmagr > 1)
			sprintf (temp, "	mag%d ", imag);
		    else
			sprintf (temp, "	mag  ");
		    strcat (headline, temp);
		    }
		}
	    if (typecol == 1)
		strcat (headline,"	type");
	    if (printepoch)
		strcat (headline, "	epoch     ");
	    if (mprop == 2)
		strcat (headline," 	velocity");
	    if (mprop == 1)
		strcat (headline,"	pmra  	pmdec ");
	    if (refcat == UCAC2 || refcat == UCAC3)
		strcat (headline,"	epmra	epmdec	ni	nc");
	    if (refcat == GSC2)
		strcat (headline,"	class");
	    if (ranges == NULL)
		strcat (headline,"	arcsec");
	    if (refcat == TABCAT && keyword != NULL) {
		strcat (headline,"	");
		strcat (headline, keyword);
		}
	    if (catsort == SORT_MERGE)
		strcat (headline,"	nmatch");
	    if (gobj1 != NULL && refcat != GSC2 && refcat != SDSS && refcat != SKYBOT) {
		if (starcat[icat] == NULL ||
		    (starcat[icat] != NULL && starcat[icat]->stnum > 0))
		    strcat (headline,"	object");
		}
	    if (printxy)
		strcat (headline, "	X      	Y      ");
	    if (wfile)
		fprintf (fd, "%s\n", headline);
	    else
		printf ("%s\n", headline);

	    strcpy (headline, "-------------------------");
	    headline[nnfld] = (char) 0;
	    strcat (headline,"	------------	------------");
	    if (refcat == UCAC2 || refcat == UCAC3)
		strcat (headline,"	-----	-----");
	    if (refcat == TMPSC || refcat == TMIDR2)
		strcat (headline,"	-------	-------	-------");
	    else if (refcat == TMPSCE)
		strcat (headline,"	-------	-------	-------	------	------	------");
	    else if (refcat == TMXSC)
		strcat (headline,"	-------	-------	-------	------");
	    else if (refcat == IRAS)
		strcat (headline,"	-----	-----	-----	-----");
	    else if (refcat == HIP)
		strcat (headline,"	-----	-----	-----	-----");
	    else if (refcat == GSC2)
		strcat (headline,"	-----	-----	-----	-----");
	    else if (nmagr > 0) {
		for (imag = 0; imag < nmagr; imag++)
		    strcat (headline,"	-----");
		}
	    if (refcat == GSC || refcat == GSCACT)
		strcat (headline,"	-----	----	-");
	    else if (refcat == UB1)
		strcat (headline,"	--	--	--");
	    else if (typecol == 1)
		strcat (headline,"	----");
	    else if (typecol == 2)
		strcat (headline,"	-----");
	    if (printepoch)
		strcat (headline, "	----------");
	    if (mprop == 2)
		strcat (headline,"	--------");
	    if (mprop == 1)
		strcat (headline,"	------	------");
	    if (refcat == UCAC2 || refcat == UCAC3)
		strcat (headline,"	-----	-----	--	--");
	    if (refcat == GSC2)
		strcat (headline,"	-----");
	    if (ranges == NULL)
		strcat (headline, "	------");
	    if (refcat == TABCAT && keyword != NULL)
		strcat (headline,"	------");
	    if (catsort == SORT_MERGE)
		strcat (headline,"	------");
	    if (gobj1 != NULL && refcat != GSC2 && refcat != SDSS && refcat != SKYBOT) {
		if (starcat[icat] == NULL ||
		    starcat[icat]->stnum > 0)
		     strcat (headline,"	------");
		}
	    if (printxy)
		strcat (headline, "	-------	-------");
	    if (wfile)
		fprintf (fd, "%s\n", headline);
	    else
		printf ("%s\n", headline);
	    nohead = 0;
	    }

	else if (printhead && nohead) {
	    if (printxy)
		strcpy (headline, "  X     Y   ");
	    else {
		if (refcat == GSC || refcat == GSCACT)
		    strcpy (headline, "GSC_number ");
		else if (refcat == GSC2)
		    strcpy (headline, "GSC2_id     ");
		else if (refcat == USAC)
		    strcpy (headline, "USNO_SA_number ");
		else if (refcat == USA1)
		    strcpy (headline, "USNO_SA1_number");
		else if (refcat == USA2)
		    strcpy (headline, "USNO_SA2_number");
		else if (refcat == UAC)
		    strcpy (headline, "USNO_A_number  ");
		else if (refcat == UA1)
		    strcpy (headline, "USNO_A1_number ");
		else if (refcat == UA2)
		    strcpy (headline, "USNO_A2_number ");
		else if (refcat == UB1)
		    strcpy (headline, "USNO_B1_number ");
		else if (refcat == YB6)
		    strcpy (headline, "USNO_YB6_number ");
		else if (refcat == SDSS)
		    strcpy (headline, "SDSS_number          ");
		else if (refcat == SKYBOT)
		    strcpy (headline, "Object          ");
		else if (refcat == UCAC1)
		    strcpy (headline, "UCAC1_num    ");
		else if (refcat == UCAC2)
		    strcpy (headline, "UCAC2_num    ");
		else if (refcat == TMPSC || refcat == TMPSCE)
		    strcpy (headline, "2MASS_num.  ");
		else if (refcat == TMXSC)
		    strcpy (headline, "2MASS_XSC   ");
		else if (refcat == TMIDR2)
		    strcpy (headline, "2MIDR2_num.");
		else if (refcat == UJC)
		    strcpy (headline, " UJ_number    ");
		else if (refcat == SAO)
		    strcpy (headline, "SAO_number ");
		else if (refcat == PPM)
		    strcpy (headline, "PPM_number ");
		else if (refcat == SKY2K)
		    strcpy (headline, "SKY_id    ");
		else if (refcat == BSC)
		    strcpy (headline, "BSC_number ");
		else if (refcat == IRAS)
		    strcpy (headline, "IRASnum  ");
		else if (refcat == TYCHO)
		    strcpy (headline, "Tycho_number ");
		else if (refcat == TYCHO2 || refcat == TYCHO2E)
		    strcpy (headline, "Tycho2_num  ");
		else if (refcat == HIP)
		    strcpy (headline, "Hip_num ");
		else if (refcat == ACT)
		    strcpy (headline, "ACT_number  ");
		else if (nnfld > 5) {
		    strcpy (headline, "Number      ");
		    headline[nnfld+3] = (char) 0;
		    }
		else {
		    strcpy (headline, "ID       ");
		    headline[nnfld+3] = (char) 0;
		    }
		}
	    if (sysout == WCS_B1950) {
		if (degout) {
		    if (eqout == 1950.0)
			strcat (headline, "   RA1950     Dec1950   ");
		    else {
			sprintf (temp, " RAB%7.2f   DecB%7.2f  ", eqout, eqout);
			strcat (headline, temp);
			}
		    }
		else {
		    if (eqout == 1950.0)
			strcat (headline, "RA1950      Dec1950    ");
		    else {
			sprintf (temp, "RAB%7.2f  DecB%7.2f ", eqout, eqout);
			strcat (headline, temp);
			}
		    }
		}
	    else if (sysout == WCS_ECLIPTIC)
		strcat (headline, "Ecl Lon    Ecl Lat  ");
	    else if (sysout == WCS_GALACTIC)
		strcat (headline, "Gal Lon    Gal Lat  ");
	    else {
		if (degout) {
		    if (eqout == 2000.0)
			strcat (headline, "   RA2000     Dec2000 ");
		    else {
			sprintf (temp," RAJ%7.2f    DecJ%7.2f ", eqout, eqout);
			strcat (headline, temp);
			}
		    }
		else {
		    if (eqout == 2000.0)
			strcat (headline, " RA2000       Dec2000   ");
		    else {
			sprintf (temp,"RAJ%7.2f   DecJ%7.2f  ", eqout, eqout);
			strcat (headline, temp);
			}
		    }
		}
	    if (refcat == USAC || refcat == USA1 || refcat == USA2 ||
		refcat == UAC  || refcat == UA1  || refcat == UA2)
		strcat (headline, "  MagB  MagR");
	    else if (refcat == UB1)
		strcat (headline, "  MagB1  MagR1  MagB2  MagR2  MagN ");
	    else if (refcat == YB6)
		strcat (headline, "  MagB   MagR   MagJ   MagH   MagK ");
	    else if (refcat == SDSS)
		strcat (headline, "  Magu   Magg   Magr   Magi   Magz ");
	    else if (refcat == UJC)
		strcat (headline, "  Mag ");
	    else if (refcat == GSC || refcat == GSCACT)
		strcat (headline, "   Mag ");
	    else if (refcat==SAO || refcat==PPM || refcat==BSC || refcat==UCAC1)
		strcat (headline, "    Mag");
	    else if (refcat==SKY2K)
		strcat (headline, "   MagB   MagV  MagPh  MagPv");
	    else if (refcat==UCAC2)
		strcat (headline, "   MagJ   MagH   MagK   MagC");
	    else if (refcat == UCAC3)
		strcat (headline,"    MagB   MagR   MagI   MagJ   MagH   MagK   MagM   MagA");
	    else if (refcat==SKYBOT)
		strcat (headline, "     MagV   GDist  HDist");
	    else if (refcat==TMPSC || refcat == TMIDR2)
		strcat (headline, "   MagJ    MagH    MagK  ");
	    else if (refcat==TMPSCE)
		strcat (headline, "   MagJ    MagH    MagK   MagJe  MagHe  MagKe");
	    else if (refcat==TMXSC)
		strcat (headline, "   MagJ    MagH    MagK  ");
	    else if (refcat==IRAS)
		strcat (headline, "  f10m   f25m   f60m   f100m");
	    else if (refcat==GSC2)
		strcat (headline, "  MagF  MagJ  MagN  MagV");
	    else if (refcat==HIP)
		strcat (headline, "  MagB  MagV  Parlx Parer");
	    else if (refcat==TYCHO || refcat==TYCHO2 || refcat==ACT)
		strcat (headline, "   MagB   MagV  ");
	    else if (refcat==TYCHO2E) 
		strcat (headline, "   MagB   MagV MagBe MagVe");
	    else if (nmagr > 0) {
		for (imag = 0; imag < nmagr; imag++) {
		    if (starcat != NULL &&
			strlen(starcat[icat]->keymag[imag]) > 0) {
			strcat (headline," ");
			sprintf (temp, " %s ", starcat[icat]->keymag[imag]);
			strcat (headline, temp);
			}
		    else if (nmagr > 1) {
			sprintf (temp, "   Mag%d", imag+1);
			strcat (headline, temp);
			}
		    else
			strcat (headline, "  Mag");
		    }
		}
	    if (refcat == USAC || refcat == USA1 || refcat == USA2 ||
		refcat == UAC  || refcat == UA1  || refcat == UA2 ||
		refcat == UJC)
		strcat (headline, "  Plate");
	    else if (refcat == UB1)
		strcat (headline, " PM NI SG");
	    else if (refcat==GSC2)
		strcat (headline, " Class");
	    else if (refcat == GSC || refcat == GSCACT)
		strcat (headline, " Class Band N");
	    else if (refcat==TMXSC)
		strcat (headline, "   Size");
	    else if (typecol == 1)
		strcat (headline, " Type");
	    else if (gcset && refcat != UCAC2 && refcat != UCAC3)
		strcat (headline, "     Peak");
	    if (printepoch)
		strcat (headline, "   Epoch   ");
	    /* if (mprop == 1)
		strcat (headline, "   pmRA  pmDec"); */
	    if (mprop == 2)
		strcat (headline, " Velocity");
	    if (refcat == UCAC2 || refcat == UCAC3)
		strcat (headline, " nim ncat");
	    if (ranges == NULL)
		strcat (headline, "  Arcsec");
	    if (gobj1 != NULL && refcat != GSC2 && refcat != SDSS && refcat != SKYBOT) {
		if (starcat[icat] == NULL || starcat[icat]->stnum > 0)
		    strcat (headline,"  Object");
		}
	    if (catsort == SORT_MERGE)
	        strcat (headline, "  Nmatch");
	    if (wfile)
		fprintf (fd, "%s\n", headline);
	    else
		printf ("%s\n", headline);
	    }
	nohead = 0;

    /* Find maximum separation for formatting */
    gdmax = 0.0;
    for (i = 0; i < ns; i++) {
	if (gx[i] > 0.0) {
	    gdist = 3600.0 * gx[i];
	    if (gdist > gdmax)
		gdmax = gdist;
	    }
	}

    string[0] = (char) 0;
    if (closest) ns = 1;
    for (i = 0; i < ns; i++) {

	/* Set source position epoch passed as a magnitude */
	if (printepoch) {
	    if (mprop == 2)
		epoch = gm[nmagr+1][i];
	    else
		epoch = gm[nmagr][i];
	    if (epoch == 99.0)
		epoch = 0.0;
	    }

	/* Check entry epoch to see if it is within specified range */
	if ((epoch1 > 0.0 && epoch2 > 0.0) &&
	    (epoch < epoch1 || epoch > epoch2))
	    continue;
	else if (epoch1 > 0.0 && epoch < epoch1)
	    continue;
	else if (epoch2 > 0.0 && epoch > epoch2)
	    continue;

	/* Set spectra type (or other 2-character code) */
	if (typecol == 1) {
	    isp[0] = gc[i] / 1000;
	    isp[1] = gc[i] % 1000;
	    if (isp[0] == ' ' && isp[1] == ' ') {
		isp[0] = '_';
		isp[1] = '_';
		}
	    }

	/* For HST Guide Star Catalog, set number of entries and band */
	if (refcat == GSC || refcat == GSCACT) {
	    ngsc = gc[i] / 10000;
	    gc[i] = gc[i] - (ngsc * 10000);
	    band = gc[i] / 100;
	    gc[i] = gc[i] - (band * 100);
	    }

	/* For USNO-B1.0 catalog, drop sources on too few plates */
	if (refcat == UB1 && minid > 0) {
	    nid = gc[i]%100;
	    if (nid < minid)
		continue;
	    }

	/* For USNO UCAC2 and UCAC3 catalogs, set errors and number of epochs */
	if (refcat == UCAC2 || refcat == UCAC3) {
	    era = gm[nmagr][i] * cosdeg (gdec[i]) * 3600.0;
	    edec = gm[nmagr+1][i] * 3600.0;
	    epmr = gm[nmagr+2][i] * cosdeg (gdec[i]) * 3600000.0;
	    epmd = gm[nmagr+3][i] * 3600000.0;
	    nim = gc[i] / 1000;
	    nct = gc[i] % 1000;
	    }

	if (gy[i] > 0.0) {
	    if (degout) {
		deg2str (rastr, 32, gra[i], nddeg);
		deg2str (decstr, 32, gdec[i], nddeg);
		}
	    else {
		ra2str (rastr, 32, gra[i], ndra);
		dec2str (decstr, 32, gdec[i], nddec);
		}
	    if (gx[i] > 0.0)
		gdist = 3600.0 * gx[i];
	    else
		gdist = 0.0;

	    /* Convert proper motion to milliarcsec/year from deg/year */
	    if (mprop == 1) {
		pra = gpra[i] * 3600000.0 * cosdeg (gdec[i]);
		pdec = gpdec[i] * 3600000.0;
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

	    if (votab) {
		sprintf (headline, "<tr>\n<td>%s</td><td>%s</td><td>%s</td>",
			numstr,rastr,decstr);
		if (refcat == UCAC2 || refcat == UCAC3) {
		    sprintf (temp,"<td>%5.1f</td><td>%5.1f</td>", era, edec);
	            strcat (headline, temp);
		    }
		for (imag = 0; imag < nmagr; imag++) {
		    sprintf (temp, "<td>%.2f</td>", gm[imag][i]);
		    strcat (headline, temp);
		    }
		if (refcat == USAC || refcat == USA1 || refcat == USA2 ||
		    refcat == UAC  || refcat == UA1  || refcat == UA2) {
		    sprintf (temp, "<td>%d</td>", gc[i]);
		    strcat (headline, temp);
		    }
		else if (refcat == GSC || refcat == GSCACT) {
		    sprintf (temp, "<td>%d</td><td>%d</td><td>%d</td>",
			    gc[i], band, ngsc);
		    strcat (headline, temp);
		    }
		if (typecol == 1) {
		    sprintf (temp, "<td>%2s</td>", isp);
		    strcat (headline, temp);
		    }
		if (mprop == 2) {
		    sprintf (temp, "<td>%8.2f</td>", gm[nmagr][i]);
	            strcat (headline, temp);
		    }
		if (mprop == 1) {
	            sprintf (temp, "<td>%6.1f</td><td>%6.1f</td>", pra, pdec);
	            strcat (headline, temp);
		    }
		if (refcat == UCAC2 || refcat == UCAC3) {
		    sprintf (temp,"<td>%5.1f</td><td>%5.1f</td><td>%2d</td><td>%2d</td>",
				epmr, epmd, nim, nct);
	            strcat (headline, temp);
		    }
		sprintf (temp, "<td>%.5f</td>\n</tr>", gdist / 3600.0);
	        strcat (headline, temp);

		printf ("%s\n", headline);
		}

	    /* Print or write tab-delimited output line for one star */
	    else if (tabout) {
		if (refcat == USAC || refcat == USA1 || refcat == USA2 ||
		    refcat == UAC  || refcat == UA1  || refcat == UA2)
		    sprintf (headline, "%s	%s	%s	%.1f	%.1f	%d",
		     numstr, rastr, decstr, gm[0][i], gm[1][i], gc[i]);
		else if (refcat == GSC || refcat == GSCACT)
	            sprintf (headline,
			 "%s	%s	%s	%.2f	%d	%d	%d",
			 numstr, rastr, decstr, gm[0][i], gc[i], band, ngsc);
		else if (refcat==TMPSC || refcat==TMIDR2 ||
			 refcat == TMPSCE || refcat==TMXSC) {
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
			    sprintf (temp, "	%5.3f ", gm[imag][i]);
			    strcat (headline, temp);
			    }
			}
		    }
		else if (refcat == GSC2)
		    sprintf (headline, "%s	%s	%s	%.2f	%.2f	%.2f	%.2f",
		     numstr,rastr,decstr,gm[0][i],gm[1][i],gm[2][i],gm[3][i]);
		else if (refcat == IRAS) {
		    sprintf (headline, "%s	%s	%s", numstr, rastr, decstr);
		    for (imag = 0; imag < 4; imag++) {
			if (gm[imag][i] > 100.0) {
			    flux = 1000.0 * pow (10.0, -(gm[imag][i]-100.0) / 2.5);
			    sprintf (temp, "	%.2fL", flux);
			    }
			else {
			    flux = 1000.0 * pow (10.0, -gm[imag][i] / 2.5);
			    sprintf (temp, "	%.2f ", flux);
			    }
			strcat (headline, temp);
			}
		    }
		else if (refcat == UB1)
		    sprintf (headline, "%s	%s	%s	%.2f	%.2f	%.2f	%.2f	%.2f	%2d	%2d	%2d",
		     numstr,rastr,decstr,gm[0][i],gm[1][i],gm[2][i],gm[3][i],gm[4][i],
		     gc[i]%10000/100, gc[i]%100, gc[i]/10000);
		else if (refcat == YB6)
		    sprintf (headline, "%s	%s	%s	%.2f	%.2f	%.2f	%.2f	%.2f",
		     numstr,rastr,decstr,gm[0][i],gm[1][i],gm[2][i],gm[3][i],gm[4][i]);
		else if (refcat == SDSS)
		    sprintf (headline, "%s	%s	%s	%.2f	%.2f	%.2f	%.2f	%.2f",
		     numstr,rastr,decstr,gm[0][i],gm[1][i],gm[2][i],gm[3][i],gm[4][i]);
		else if (refcat == HIP)
		    sprintf (headline, "%s	%s	%s	%.2f	%.2f	%.2f	%.2f",
		     numstr,rastr,decstr,gm[0][i],gm[1][i],gm[2][i],gm[3][i]);
		else if (refcat == UJC)
	            sprintf (headline, "%s	%s	%s	%.2f	%d",
		     numstr, rastr, decstr, gm[0][i], gc[i]);
		else if (refcat==SAO || refcat==PPM || refcat == BSC)
	            sprintf (headline, "%s	%s	%s	%.2f",
		     numstr, rastr, decstr, gm[0][i]);
		else if (refcat==TYCHO || refcat==TYCHO2 || refcat==ACT)
	            sprintf (headline, "%s	%s	%s	%.2f	%.2f",
		     numstr, rastr, decstr, gm[0][i], gm[1][i]);
		else if (refcat==TYCHO2E)
	            sprintf (headline, "%s	%s	%s	%.2f	%.2f	%.2f	%.2f",
		     numstr, rastr, decstr, gm[0][i], gm[1][i], gm[2][i], gm[3][i]);
		else {
	            sprintf (headline, "%s	%s	%s", numstr, rastr, decstr);
		    if (refcat == UCAC2 || refcat == UCAC3) {
			sprintf (temp,"	%5.3f	%5.3f", era, edec);
	        	strcat (headline, temp);
			}

		    for (imag = 0; imag < nmagr; imag++) {
			sprintf (temp, "	%.2f", gm[imag][i]);
			strcat (headline, temp);
			}
		    if (typecol == 2) {
			sprintf (temp, "	%d", gc[i]);
			strcat (headline, temp);
			}
		    }
		if (typecol == 1) {
		    sprintf (temp, "	%2s", isp);
		    strcat (headline, temp);
		    }
		if (mprop == 2) {
		    if (printepoch) {
			dtemp = DateString (epoch, tabout);
        		strcat (headline, dtemp);
			free (dtemp);
			}
		    sprintf (temp, "	%8.2f", gm[nmagr][i]);
	            strcat (headline, temp);
		    }
		else if (printepoch) {
		    dtemp = DateString (epoch, tabout);
		    strcat (headline, dtemp);
		    free (dtemp);
		    }
		if (mprop == 1) {
	            sprintf (temp, "	%6.1f	%6.1f", pra, pdec);
	            strcat (headline, temp);
		    }
		if (refcat == TABCAT && gcset) {
	            sprintf (temp, "	%d", gc[i]);
	            strcat (headline, temp);
		    }
		else if (refcat == TMXSC) {
		    sprintf (temp,"	%6.1f", ((double)gc[i])* 0.1);
		    strcat (headline, temp);
		    }
		else if (refcat == GSC2) {
	            sprintf (temp, "	%d  ", gc[i]);
		    strcat (headline, temp);
		    }
		else if (refcat == UCAC2 || refcat == UCAC3) {
		    sprintf (temp, "	%5.1f	%5.1f	%2d	%2d",
			    epmr, epmd, nim, nct);
		    strcat (headline, temp);
		    }

		if (ranges == NULL) {
	            sprintf (temp, "	%.2f", gdist);
	            strcat (headline, temp);
		    }
		if (refcat == TABCAT && keyword != NULL) {
		    strcat (headline, "	");
		    if (gobj[i] != NULL)
			strcat (headline, gobj[i]);
		    else
			strcat (headline, "___");
		    }
		if (catsort == SORT_MERGE) {
	            sprintf (temp, "	%d", (int)gx[i]);
	            strcat (headline, temp);
		    }
		if ((refcat == BINCAT || refcat == TXTCAT) &&
		     gobj1 != NULL && gobj[i] != NULL) {
		    if (starcat[icat] == NULL || starcat[icat]->stnum > 0) {
			strcat (headline, "	");
			strcat (headline, gobj[i]);
			}
		    }
		if (printxy) {
		    strcat (headline, "	");
		    strcat (numstr, xstr);
		    strcat (headline, "	");
		    strcat (numstr, ystr);
		    }
		if (wfile)
		    fprintf (fd, "%s\n", headline);
		else
		    printf ("%s\n", headline);
		}

	    /* Print or write space-delimited output line for one star */
	    else {
		if (printxy) {
		    strcpy (numstr, xstr);
		    strcat (numstr, " ");
		    strcat (numstr, ystr);
		    }
		if (refcat == USAC || refcat == USA1 || refcat == USA2 ||
		    refcat == UAC  || refcat == UA1  || refcat == UA2)
		    sprintf (headline,"%s %s %s %5.1f %5.1f ",
			     numstr,rastr,decstr,gm[0][i],gm[1][i]);
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
			    sprintf (temp, " %5.3f ", gm[imag][i]);
			    strcat (headline, temp);
			    }
			}
		    }
		else if (refcat == IRAS) {
		    sprintf (headline, "%s %s %s", numstr, rastr, decstr);
		    for (imag = 0; imag < 4; imag++) {
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
		else if (refcat == GSC2)
		    sprintf (headline, "%s %s %s %5.2f %5.2f %5.2f %5.2f",
			     numstr, rastr, decstr, gm[0][i], gm[1][i],
			     gm[2][i], gm[3][i]);
		else if (refcat == HIP)
		    sprintf (headline, "%s %s %s %5.2f %5.2f %5.2f %5.2f",
			     numstr, rastr, decstr, gm[0][i], gm[1][i],
			     gm[2][i], gm[3][i]);
		else if (refcat == GSC || refcat == GSCACT)
	            sprintf (headline, "%s %s %s %6.2f",
			     numstr, rastr, decstr, gm[0][i]);
		else if (refcat == UJC)
		    sprintf (headline,"%s %s %s %6.2f",
			     numstr, rastr, decstr, gm[0][i]);
		else if (refcat == UCAC1)
		    sprintf (headline,"%s  %s %s %6.2f",
			     numstr,rastr,decstr,gm[0][i]);
		else if (refcat == UCAC2)
		    sprintf (headline,"%s  %s %s %6.2f %6.2f %6.2f %6.2f",
			     numstr,rastr,decstr,gm[0][i],
			     gm[1][i], gm[2][i], gm[3][i]);
		else if (refcat==SAO || refcat==PPM || refcat == BSC)
		    sprintf (headline,"  %s  %s %s %6.2f",
			     numstr,rastr,decstr,gm[0][i]);
		else if (refcat==TYCHO || refcat==TYCHO2 || refcat==ACT)
		    sprintf (headline,"%s %s %s %6.2f %6.2f ",
			     numstr,rastr,decstr,gm[0][i],gm[1][i]);
		else if (refcat==TYCHO2E)
		    sprintf (headline,"%s %s %s %6.2f %6.2f %5.2f %5.2f",
			     numstr,rastr,decstr,gm[0][i],gm[1][i],gm[2][i],gm[3][i]);
		else {
		    sprintf (headline,"%s %s %s ", numstr, rastr, decstr);
		    for (imag = 0; imag < nmagr; imag++) {
			sprintf (temp, " %6.2f", gm[imag][i]);
			strcat (headline, temp);
			}
		    }
		if (refcat == USAC || refcat == USA1 || refcat == USA2 ||
		    refcat == UAC  || refcat == UA1  || refcat == UA2 ||
		    refcat == UJC) {
		    sprintf (temp," %4d", gc[i]);
		    strcat (headline, temp);
		    }
		else if (refcat == UB1) {
		    sprintf (temp," %2d %2d %2d",
			    gc[i]%10000/100, gc[i]%100, gc[i]/10000);
		    strcat (headline, temp);
		    }
		else if (refcat == GSC || refcat == GSCACT) {
	            sprintf (temp, " %4d %4d %2d", gc[i], band, ngsc);
		    strcat (headline, temp);
		    }
		else if (refcat == GSC2) {
	            sprintf (temp, " %2d  ", gc[i]);
		    strcat (headline, temp);
		    }
		else if (typecol == 1) {
		    sprintf (temp, "  %2s", isp);
		    strcat (headline, temp);
		    }
		else if (refcat == TMXSC) {
		    sprintf (temp," %6.1f", ((double)gc[i])* 0.1);
		    strcat (headline, temp);
		    }
		else if (gcset && refcat != UCAC2 && refcat != UCAC3) {
		    sprintf (temp," %7d",gc[i]);
		    strcat (headline, temp);
		    }
		if (mprop == 2) {
		    if (printepoch) {
			dtemp = DateString (epoch, tabout);
			strcat (headline, dtemp);
			free (dtemp);
			}
		    sprintf (temp, " %9.2f", gm[nmagr][i]);
		    strcat (headline, temp);
		    }
		else if (printepoch) {
		    dtemp = DateString (epoch, tabout);
		    strcat (headline, dtemp);
		    free (dtemp);
		    }
	    	/* if (mprop == 1) {
	    	    sprintf (temp, " %6.1f %6.1f", pra, pdec);
	    	    strcat (headline, temp);
		    } */

		if (refcat == UCAC2 || refcat == UCAC3) {
		    sprintf (temp, "  %2d   %2d", nim, nct);
		    strcat (headline, temp);
		    }

		/* Add distance from search center */
		if (ranges == NULL) {
		    if (gdmax < 100.0)
			sprintf (temp, "  %5.2f", gdist);
		    else if (gdmax < 1000.0)
			sprintf (temp, "  %6.2f", gdist);
		    else if (gdmax < 10000.0)
			sprintf (temp, "  %7.2f", gdist);
		    else if (gdmax < 100000.0)
			sprintf (temp, "  %8.2f", gdist);
		    else
			sprintf (temp, "  %.2f", gdist);
		    strcat (headline, temp);
		    }

		/* Add specified keyword or object name */
		if (refcat == TABCAT && keyword != NULL) {
		    if (gobj[i] != NULL)
			sprintf (temp, "  %s", gobj[i]);
		    else
			sprintf (temp, "  ___");
		    strcat (headline, temp);
		    }
		else if ((refcat == BINCAT || refcat == TXTCAT) &&
			 gobj1 != NULL && gobj[i] != NULL) {
		    if (starcat[icat] == NULL || starcat[icat]->stnum > 0) {
			sprintf (temp, "  %s", gobj[i]);
			strcat (headline, temp);
			}
		    }

		/* Add number of original entries in merged output */
		if (catsort == SORT_MERGE) {
	            sprintf (temp, "  %2d", (int)gx[i]);
	            strcat (headline, temp);
		    }

		/* Write to file or standard output */
		if (wfile)
		    fprintf (fd, "%s\n", headline);
		else
		    printf ("%s\n", headline);
		}
	    }
	}

	/* If searching more than one catalog, separate them with blank line */
	if (ncat > 0 && icat < ncat-1)
	    printf ("\n");

	/* Free memory used for object names in current catalog */
	if (gobj1 != NULL) {
	    for (i = 0; i < ns; i++) {
		if (gobj[i] != NULL) free (gobj[i]);
		gobj[i] = NULL;
		}
	    }
	if (votab) vottail ();
	}

    /* Close output file */
    if (wfile)
	fclose (fd);

    return (ns);
    }


/* Get a center and radius for a search area.  If the image center is not
 * given in the system of the reference catalog, convert it.
 * Return 0 if OK, else -1
 */

static int
GetArea (verbose,syscoor,eqcoor,sysout,eqout,epout,cra,cdec,dra,ddec,drad,dradi,
	 crao,cdeco)

int	verbose;	/* Extra printing if =1 */
int	syscoor;	/* Coordinate system of input search coordinates */
double	eqcoor;		/* Equinox in years of input coordinates */
int	sysout;		/* Coordinate system of output coordinates */
double	eqout;		/* Equinox in years of output coordinates */
double	epout;		/* Epoch in years of output coordinates (0=eqcoor */
double	*cra;		/* Search center longitude/right ascension (degrees returned)*/
double	*cdec;		/* Search center latitude/declination (degrees returned) */
double	*dra;		/* Longitude/RA half-width (degrees returned) */
double	*ddec;		/* Latitude/Declination half-width (degrees returned) */
double	*drad;		/* Radius to search in degrees (0=box) (returned) */
double	*dradi;		/* Radius of inner edge of search annulus (returned) */
double	*crao;		/* Output search center longitude/right ascension */
double	*cdeco;		/* Output search center latitude/declination */
{
    char rstr[32], dstr[32], cstr[32], dstri[32], dstro[32];

    *cra = ra0;
    *cdec = dec0;
    if (verbose) {
	if (syscoor == WCS_XY) {
	    num2str (rstr, *cra, 10, 5);
            num2str (dstr, *cdec, 10, 5);
	    }
	else if (syscoor==WCS_ECLIPTIC || syscoor==WCS_GALACTIC || degout0) {
	    deg2str (rstr, 32, *cra, nddeg);
            deg2str (dstr, 32, *cdec, nddeg);
	    }
	else {
	    ra2str (rstr, 32, *cra, ndra);
            dec2str (dstr, 32, *cdec, nddec);
	    }
	wcscstr (cstr, syscoor, 0.0, 0.0);
	fprintf (stderr,"Center:  %s   %s %s\n", rstr, dstr, cstr);
	}

    *crao = *cra;
    *cdeco = *cdec;
    if (syscoor != sysout || eqcoor != eqout) {
	wcscon (syscoor, sysout, eqcoor, eqout, crao, cdeco, epout);
	if (verbose) {
	    if (syscoor == WCS_ECLIPTIC || syscoor == WCS_GALACTIC || degout0) {
		deg2str (rstr, 32, *crao, nddeg);
        	deg2str (dstr, 32, *cdeco, nddeg);
		}
	    else {
		ra2str (rstr, 32, *crao, ndra);
        	dec2str (dstr, 32, *cdeco, nddec);
		}
	    wcscstr (cstr, sysout, 0.0, 0.0);
	    fprintf (stderr,"Center:  %s   %s %s\n", rstr, dstr, cstr);
	    }
	}

    /* Set search box radius from command line, if it is there */
    if (dra0 > 0.0) {
	*drad = 0.0;
	if (syscoor == WCS_XY) {
	    *dra = dra0;
	    *ddec = ddec0;
	    }
	else {
	    *ddec = ddec0 / 3600.0;
	    if (*cdec < 90.0 && *cdec > -90.0) {
		if (rdra)
		    *dra = dra0 / 3600.0;
		else
		    *dra = (dra0 / 3600.0) / cos (degrad (*cdec));
		}
	    else
		*dra = 180.0;
	    }
	}

    /* Search box */
    else if (rad0 < 0.0) {
	*drad = 0.0;
	if (syscoor == WCS_XY) {
	    *dra = -rad0;
	    *ddec = -rad0;
	    }
	else {
	    *ddec = -rad0 / 3600.0;
	    *dra = *ddec / cos (degrad (*cdec));
	    }
	}

    /* Search circle */
    else if (rad0 > 0.0) {
	if (syscoor == WCS_XY)
	    *drad = rad0;
	else
	    *drad = rad0 / 3600.0;
	*dra = *drad / cos (degrad (*cdec));
	*ddec = *drad;
	if (rad1 > 0.0)
	    *dradi = rad1 / 3600.0;
	else
	    *dradi = 0.0;
	}
    else {
	if (verbose)
	    fprintf (stderr, "GetArea: Illegal radius, rad= %.5f\n",rad0);
	return (-1);
	}

    if (verbose) {
	if (syscoor == WCS_XY) {
	    num2str (rstr, *dra, 10, 5); 
	    num2str (dstr, *ddec, 10, 5); 
	    num2str (dstro, *drad, 10, 5); 
	    num2str (dstri, *dradi, 10, 5); 
	    }
	else if (degout0) {
	    deg2str (rstr, 32, *dra, 6); 
	    deg2str (dstr, 32, *ddec, 6); 
	    deg2str (dstro, 32, *drad, 6); 
	    deg2str (dstri, 32, *dradi, 6); 
	    }
	else {
	    dec2str (rstr, 32, *dra, 2); 
	    dec2str (dstr, 32, *ddec, 2); 
	    dec2str (dstro, 32, *drad, 2); 
	    dec2str (dstri, 32, *dradi, 2); 
	    }
	if (*drad == 0.0)
	    fprintf (stderr,"Area:    %s x %s\n", rstr, dstr);
	else if (*dradi > 0.0)
	    fprintf (stderr, "Radius: %s-%s\n", dstri, dstro);
	else
	    fprintf (stderr, "Radius: %s\n", dstro);
	}

    return (0);
}


static void
SearchHead (icat,sys,eq,ep,cra,cdec,dra,ddec,drad,dradi,nnfld,degout)

int	icat;		/* Number of catalog in list */
int	sys;		/* Coordinate system */
double	eq;		/* Equinox */
double	ep;		/* Epoch */
double	cra, cdec;	/* Center coordinates of search box in degrees */
double	dra, ddec;	/* Width and height of search box in degrees */
double	drad;		/* Radius of search region in degrees */
double	dradi;		/* Inner edge of annulus in degrees (ignore if 0) */
int	nnfld;		/* Number of characters in ID field */
int	degout;		/* Ouput in degrees if 1 */
{
    char rastr[32];
    char decstr[32];
    char cstr[16];
    char oform[16];
    int refcat;
    char *catname;

    if (sys == WCS_XY) {
	num2str (rastr, cra, 10, 5);
	num2str (decstr, cdec, 10, 5);
	}
    else if (sys == WCS_ECLIPTIC || sys == WCS_GALACTIC || degout) {
	deg2str (rastr, 32, cra, nddeg);
	deg2str (decstr, 32, cdec, nddeg);
	}
    else {
	ra2str (rastr, 32, cra, ndra);
	dec2str (decstr, 32, cdec, nddec);
	}

    /* Set type of catalog being searched */
    if (!(refcat = CatCode (refcatname[icat]))) {
	fprintf (stderr,"ListCat: Catalog '%s' is missing\n",refcatname[icat]);
	return;
	}

    /* Label search center */
    sprintf (oform, "%%%ds", nnfld);
    if (objname)
	printf (oform, objname);
    else {
	catname = CatName (refcat, refcatname[icat]);
	printf (oform, catname);
	}
    wcscstr (cstr, sys, eq, ep);
    printf (" %s %s %s", rastr, decstr, cstr);
    if (degout) {
	if (drad != 0.0) {
	    if (dradi > 0.0)
		printf (" r= %.2f - %.2f", dradi,drad);
	    else
		printf (" r= %.2f", drad);
	    }
	else
	    printf (" +- %.2f %.2f", dra, ddec);
	}
    else {
	if (drad != 0.0) {
	    dec2str (decstr, 32, drad, 1);
	    if (dradi > 0.0) {
		dec2str (rastr, 32, dradi, 1);
		printf (" r= %s - %s", rastr, decstr);
		}
	    else
		printf (" r= %s", decstr);
	    }
	else {
	    dec2str (rastr, 32, dra, 1);
	    dec2str (decstr, 32, ddec, 1);
	    printf (" +- %s %s", rastr, decstr);
	    }
	}
    if (classd == 0)
	printf (" stars");
    else if (classd == 3)
	printf (" nonstars");
    if (ep != 0.0)
	printf (" at epoch %9.4f\n", ep);
    else
	printf ("\n");
    return;
}


void
PrintNum (maxnum, num, ndec)

double	maxnum;	/* Maximum value of number to print */
double	num;	/* Number to print */
int	ndec;	/* Number of decimal places in output */
{
    char nform[8];
    int LenNum();

    if (ndec > 0)
	sprintf (nform, "%%%d.%df", LenNum (maxnum,ndec), ndec);
    else
	sprintf (nform, "%%%dd", LenNum (maxnum,ndec));
    printf (nform, num);
}


int
LenNum (maxnum, ndec)

double	maxnum;	/* Maximum value of number to print */
int	ndec;	/* Number of decimal places in output */
{
    if (ndec <= 0)
	ndec = -1;
    if (maxnum < 9.999)
	return (ndec + 3);
    else if (maxnum < 99.999)
	return (ndec + 4);
    else if (maxnum < 999.999)
	return (ndec + 5);
    else if (maxnum < 9999.999)
	return (ndec + 6);
    else
	return (ndec + 7);
}



/* Set parameter values from cgi query line as kw=val&kw=val... */

static void
scatcgi (qstring)

char *qstring;
{
    char *pstring, *parend;

    pstring = qstring;
    while ((parend = strchr (pstring, '&')) != NULL) {
	*parend = (char) 0;
	if (scatparm (pstring))
	    fprintf (stderr, "SCATCGI: %s is not a parameter.\n", pstring);
	pstring = parend + 1;
	}
    if (scatparm (pstring)) {
	if (!votab)
	    fprintf (stderr, "SCATCGI: %s is not a parameter.\n", pstring);
	}
    return;
}


/* Set parameter values from the command line as keyword=value
 * Return 1 if successful, else 0 */

static int
scatparm (parstring)

char *parstring;
{
    char *parname;
    char *parvalue;
    char *parequal;
    char *temp;
    char *refcatn;
    int lcat, lrange;

    /* Check for request for help */
    if (!strncasecmp (parstring,"help", 4)) {
	PrintWebHelp ();
	}

    /* Check for request for command line help */
    if (!strncasecmp (parstring,"comhelp", 7)) {
	PrintUsage (NULL);
	}

    /* Check for request for GSC bandpasses */
    if (!strncasecmp (parstring, "band", 4) ||
	!strncasecmp (parstring, "filt", 4)) {
	PrintGSCBand ();
	}

    /* Check for request for GSC object classes */
    if (!strncasecmp (parstring, "clas", 4) ||
	!strncasecmp (parstring, "obje", 4)) {
	PrintGSClass ();
	}

    /* Check for scat version request */
    if (!strcasecmp (parstring, "version")) {
	PrintUsage ("version");
	}

    /* Separate parameter name and value */
    parname = parstring;
    if ((parequal = strchr (parname,'=')) == NULL)
	return (0);
    *parequal = (char) 0;
    parvalue = parequal + 1;

    /* Get closest source */
    if (!strcasecmp (parname, "closest")) {
	if (!strncasecmp (parvalue, "y", 1)) {
	    catsort = SORT_DIST;
	    nstars = 1;
	    closest++;
	    }
	}

    /* Set range of source numbers to print */
    else if (!strncasecmp (parname,"num",3)) {
	if (ranges) {
	    temp = ranges;
	    lrange = strlen(ranges) + strlen(parvalue) + 2;
	    ranges = (char *) malloc (lrange);
	    strcpy (ranges, temp);
	    strcat (ranges, ",");
	    strcat (ranges, parvalue);
	    free (temp);
	    }
	else {
	    lrange = strlen (parvalue) + 2;
	    ranges = (char *) malloc (lrange);
	    if (strchr (parvalue,'.'))
		match = 1;
	    strcpy (ranges, parvalue);
	    }
	}

    /* Radius in arcseconds */
    else if (!strncasecmp (parname,"degree",6)) {
	if (strchr (parvalue, 'y'))
	     degout0 = 1;
	else
	     degout0 = 0;
	}

    /* Radius in arcseconds */
    else if (!strncasecmp (parname,"rad",3)) {
	if (strchr (parvalue,':'))
	    rad0 = 3600.0 * str2dec (parvalue);
	else if (isnum (parvalue))
	    rad0 = atof (parvalue);
	else {
	    sprintf (voerror, "Search radius %s (seconds) is not a number", parvalue);
	    return (-1);
	    }
	}

    /* Radius in arcminutes */
    else if (!strncasecmp (parname,"mrad",4)) {
	if (strchr (parvalue,':'))	/* hours:minutes:seconds */
	    rad0 = 3600.0 * str2dec (parvalue);
	else if (isnum (parvalue))
	    rad0 = 60.0 * atof (parvalue);
	else {
	    sprintf (voerror, "Search radius %s (minutes) is not a number", parvalue);
	    return (-1);
	    }
	}

    /* Inner Annulus radius in arcseconds */
    else if (!strncasecmp (parname,"inrad",5)) {
	if (strchr (parvalue,':'))	/* hours:minutes:seconds */
	    rad1 = 3600.0 * str2dec (parvalue);
	else if (isnum (parvalue))
	    rad1 = atof (parvalue);
	else {
	    sprintf (voerror, "Search radius %s is not a number", parvalue);
	    return (-1);
	    }
	}

    /* Radius in degrees */
    else if (!strncasecmp (parname,"sr",2) ||
	     !strncasecmp (parname,"drad",4)) {
	if (strchr (parvalue,':'))
	    rad0 = 3600.0 * str2dec (parvalue);
	else if (isnum (parvalue))
	    rad0 = 3600.0 * atof (parvalue);
	else {
	    sprintf (voerror, "Search radius %s (degrees) is not a number", parvalue);
	    return (-1);
	    }
	votab = 1;
	degout0 = 1;
	}

    /* Search center right ascension */
    else if (!strcasecmp (parname,"ra")) {
	if (!isnum (parvalue) && !strchr (parvalue,':')) {
	    sprintf (voerror, "Right ascension %s is not a number", parvalue);
	    return (-1);
	    }
	ra0 = str2ra (parvalue);
	}

    /* Search center declination */
    else if (!strcasecmp (parname,"dec")) {
	if (!isnum (parvalue) && !strchr (parvalue,':')) {
	    sprintf (voerror, "Declination %s is not a number", parvalue);
	    return (-1);
	    }
	dec0 = str2dec (parvalue);
	}

    /* Search center coordinate system */
    else if (!strncasecmp (parname,"sys",3)) {
	syscoor = wcscsys (parvalue);
	eqcoor = wcsceq (parvalue);
	}

    /* Output coordinate system */
    else if (!strcasecmp (parname, "outsys")) {

	/* B1950 (FK4) coordinates */
	if (!strcasecmp (parvalue, "B1950") ||
	    !strcasecmp (parvalue, "FK4")) {
	    sysout0 = WCS_B1950;
	    eqout = 1950.0;
    	    }

	/* J2000 (FK5) coordinates */
	else if (!strcasecmp (parvalue, "J2000") ||
	    !strcasecmp (parvalue, "FK5")) {
	    sysout0 = WCS_J2000;
	    eqout = 2000.0;
    	    }

	/* Galactic coordinates */
	else if (!strncasecmp (parvalue, "GAL", 3))
	    sysout0 = WCS_GALACTIC;

	/* Ecliptic coordinates */
	else if (!strncasecmp (parvalue, "ECL", 3))
	    sysout0 = WCS_ECLIPTIC;
	}

    /* Set reference catalog */
    else if (!strcasecmp (parname, "catalog")) {
	lcat = strlen (parvalue) + 2;
	refcatn = (char *) malloc (lcat);
	strcpy (refcatn, parvalue);
	refcatname[ncat] = refcatn;
	ncat = ncat + 1;
	}

    /* Set output coordinate epoch */
    else if (!strcasecmp (parname, "epoch"))
	epoch0 = fd2ep (parvalue);

    /* Output equinox in years */
    else if (!strcasecmp (parname, "equinox"))
    	eqout = fd2ep (parvalue);

    /* Output in degrees instead of sexagesimal */
    else if (!strcasecmp (parname, "cformat")) {
	if (!strncasecmp (parvalue, "deg", 3))
	    degout0 = 1;
	else if (!strncasecmp (parvalue, "rad", 3))
	    degout0 = 2;
	else
	    degout0 = 0;
	}

    /* Number of decimal places in output positions */
    else if (!strcasecmp (parname, "ndec")) {
	if (isnum (parvalue)) {
	    if (degout0) {
		nddeg = atoi (parvalue);
		if (nddeg < 0 || nddeg > 10)
		    nddeg = 7;
		}
	    else {
		ndra = atoi (parvalue);
		if (ndra < 0 || ndra > 10)
		    ndra = 3;
		nddec = ndra - 1;
		}
	    }
	}

    /* Minimum proper motion quality for USNO-B1.0 catalog */
    else if (!strcasecmp (parname, "minpmq")) {
	if (isnum (parvalue)) {
	    minid = atoi (parvalue);
	    setminpmqual (minid);
	    }
	}

    /* Format for epoch of catalog source position */
    else if (!strcasecmp (parname, "dateform")) {
	if (parvalue[0] == 'j')
	    setdateform (EP_JD);
	else if (parvalue[0] == 'm')
	    setdateform (EP_MJD);
	else if (parvalue[0] == 'f')
	    setdateform (EP_FD);
	else if (parvalue[0] == 'i')
	    setdateform (EP_ISO);
	else
	    setdateform (EP_EP);
	}

    /* Minimum number of plate ID's for USNO-B1.0 catalog */
    else if (!strcasecmp (parname, "minid")) {
	if (isnum (parvalue)) {
	    minid = atoi (parvalue);
	    setminid (minid);
	    }
	}

    /* Output in VOTable XML instead of tab-separated table */
    else if (!strcasecmp (parname, "format")) {
	if (!strncasecmp (parvalue, "vot", 3)) {
	    votab = 1;
	    degout0 = 1;
	    }
	else
	    votab = 0;
	}

    /* Print center and closest star on one line */
    else if (!strcasecmp (parname, "oneline")) {
	if (parvalue[0] == 'y' || parvalue[0] == 'Y') {
	    oneline++;
	    catsort = SORT_DIST;
	    closest++;
	    nstars = 1;
	    }
	}

    /* Magnitude limit */
    else if (!strncasecmp (parname,"mag",3)) {
	maglim2 = atof (parvalue);
	if (MAGLIM1 == MAGLIM2)
	    maglim1 = -2.0;
	}
    else if (!strncasecmp (parname,"max",3))
	maglim2 = atof (parvalue);
    else if (!strncasecmp (parname,"min",3))
	maglim1 = atof (parvalue);

    /* Number of brightest stars to read */
    else if (!strncasecmp (parname,"nstar",5))
	nstars = atoi (parvalue);

    /* Object name */
    else if (!strcmp (parname, "object") || !strcmp (parname, "OBJECT")) {
	lcat = strlen (parvalue) + 2;
	objname = (char *) malloc (lcat);
	strcpy (objname, parvalue);
	}

    /* Magnitude by which to sort */
    else if (!strcasecmp (parname, "sortmag")) {
	if (isnum (parvalue+1))
	    sortmag = atoi (parvalue);
	}

    /* Output sorting */
    else if (!strcasecmp (parname, "sort")) {

	/* Sort by distance from center */
	if (!strncasecmp (parvalue,"di",2))
	    catsort = SORT_DIST;

	/* Sort by RA */
	else if (!strncasecmp (parvalue,"r",1))
	    catsort = SORT_RA;

	/* Sort by Dec */
	else if (!strncasecmp (parvalue,"de",2))
		catsort = SORT_DEC;

	/* Sort by ID */
	else if (!strncasecmp (parvalue,"id",2))
		catsort = SORT_ID;

	/* Sort by magnitude */
	else if (!strncasecmp (parvalue,"m",1)) {
	    catsort = SORT_MAG;
	    if (strlen (parvalue) > 1) {
		if (isnum (parvalue+1))
		    sortmag = atoi (parvalue+1);
		}
	    }

	/* No sort */
	else if (!strncasecmp (parvalue,"n",1))
	    catsort = SORT_NONE;
	else
	    catsort = SORT_NONE;
	}

    /* Search box half-width in RA */
    else if (!strcasecmp (parname,"dra")) {
	if (strchr (parvalue,':'))
	    dra0 = 3600.0 * str2ra (parvalue);
	else
	    dra0 = atof (parvalue);
	if (ddec0 <= 0.0)
	    ddec0 = dra0;
	}

    /* Search box half-height in Dec */
    else if (!strcasecmp (parname,"ddec")) {
	if (strchr (parvalue,':'))
	    ddec0 = 3600.0 * str2dec (parvalue);
	else
	    ddec0 = atof (parvalue);
	if (dra0 <= 0.0)
	    dra0 = ddec0;
	}

    /* Guide Star object class */
    else if (!strncasecmp (parname,"cla",3)) {
	classd = (int) atof (parvalue);
	setgsclass (classd);
	}

    /* Catalog to be searched */
    else if (!strncasecmp (parname,"cat",3)) {
	lcat = strlen (parvalue) + 2;
	refcatn = (char *) malloc (lcat);
	strcpy (refcatn, parvalue);
	refcatname[ncat] = refcatn;
	ncat = ncat + 1;
	}
    else {
	*parequal = '=';
	return (1);
	}
    *parequal = '=';
    return (0);
}


static void
PrintWebHelp ()

{
    FILE *out;	/* Output, stderr for command line, stdout for web */
    if (http) {
	out = stdout;
	fprintf (out, "Content-type: text/plain\n\n");
	}
    else
	out = stderr;
    fprintf (out, "Web catalog searching using WCSTools %s %s\n", progname, RevMsg);
    fprintf (out, "Results will be returned as a tab-separated Starbase table\n");
    fprintf (out, "Enter a sequence of keyword=value, separated by &, in one line\n");
    fprintf (out, "Help at http://tdc-www.harvard.edu/software/wcstools/scat/\n");
    fprintf (out, "\n");
    fprintf (out, "keyword  value description\n");
    fprintf (out, "-------  ------------------------------------------------\n");
    fprintf (out, "catalog  gsc (HST GSC),  ua2 (USNO-A2.0),  gsc2 (GSC II),\n");
    fprintf (out, "         gsca (GSC-ACT), ty2 (Tycho-2),    tmc (2MASS PSC),\n");
    fprintf (out, "         ub1 (USNO-B1.0), ucac2 (UCAC2), ppm, sao, etc.\n");
    fprintf (out, "ra       right ascension in degrees or hh:mm:ss.sss\n");
    fprintf (out, "dec      declination in degrees or [+/-]dd:mm:ss.sss\n");
    fprintf (out, "sys      coordinate system (B1950, J2000, Ecliptic, Galactic\n");
    fprintf (out, "outsys   output coordinate system if not same as above\n");
    fprintf (out, "nstar    maximum number of stars to be returned\n");
    fprintf (out, "rad      search radius in arcseconds or dd:mm:ss.sss\n");
    fprintf (out, "         (negate for square box)\n");
    fprintf (out, "inrad    inner annulus radius in arcseconds or dd:mm:ss.sss\n");
    fprintf (out, "sr       search radius in degrees or dd:mm:ss.sss\n");
    fprintf (out, "         (negate for square box)\n");
    fprintf (out, "dra      search halfwidth in RA arcseconds or dd:mm:ss.sss\n");
    fprintf (out, "ddec     search halfheight in Dec arcseconds or dd:mm:ss.sss\n");
    fprintf (out, "sort     dist   distance from search center\n");
    fprintf (out, "         ra     right ascension\n");
    fprintf (out, "         dec    declination\n");
    fprintf (out, "         mag    magnitude (brightest first)\n");
    fprintf (out, "         none   no sort\n");
    fprintf (out, "epoch    output epoch in fractional years\n");
    fprintf (out, "equinox  output equinox in fractional years\n");
    fprintf (out, "cformat  col    output coordinates hh:mm:ss dd:mm:ss\n");
    fprintf (out, "         deg    output coordinates in degrees\n");
    fprintf (out, "         rad    output coordinates in radians\n");
    fprintf (out, "format   tab    tab-separated table returned\n");
    fprintf (out, "         vot    VOTable XML returned\n");
    fprintf (out, "min      minimum magnitude\n");
    fprintf (out, "max      maximum magnitude\n");
    fprintf (out, "num      range of catalog numbers (n-nxn or n,n,n)\n");
    fprintf (out, "\n");
    fprintf (out, "help     List scat web parameters (this list)\n");
    fprintf (out, "version  Return scat version\n");
    fprintf (out, "band     Return list of HST GSC bandpass codes\n");
    fprintf (out, "class    Return list of HST GSC object class codes\n");
    fprintf (out, "comhelp  Return list of scat command line arguments\n");
    exit (0);
}

static void
PrintGSCBand ()

{
    FILE *out;	/* Output, stderr for command line, stdout for web */
    if (http) {
	out = stdout;
	fprintf (out, "Content-type: text/plain\n\n");
	}
    else
	out = stderr;
    fprintf (out, "HST Guide Star Catalog Bandpass Codes\n");
    fprintf (out, "Code Bandpass Emulsion/Filter Notes\n");
    fprintf (out, " 0      J     IIIaJ+GG395     SERC-J/EJ\n");
    fprintf (out, " 1      V     IIaD+W12        Pal Quick-V\n");
    fprintf (out, " 3      B     -               Johnson\n");
    fprintf (out, " 4      V     -               Johnson\n");
    fprintf (out, " 5      R     IIIaF+RG630     -\n");
    fprintf (out, " 6      V495  IIaD+GG495      Pal QV/AAO XV\n");
    fprintf (out, " 7      O     103aO+no filt   POSS-I Blue\n");
    fprintf (out, " 8      E     103aE+redplex   POSS-I Red\n");
    fprintf (out, " 9      R     IIIaF+RG630     -\n");
    fprintf (out, "10      -     IIaD+GG495+yel  GPO Astrograph\n");
    fprintf (out, "11      -     103aO+blue      Black Birch Astrograph\n");
    fprintf (out, "12      -     103aO+blue      Black Birch Astrograph (GSC cal)\n");
    fprintf (out, "13      -     103aG+GG495+yel Black Birch Astrograph\n");
    fprintf (out, "14      -     103aG+GG495+yel Black Birch Astrograph (GSC cal)\n");
    fprintf (out, "16      J     IIIaJ+GG495     -\n");
    fprintf (out, "18      V     IIIaJ+GG385     POSS-II Blue\n");
    fprintf (out, "19      U     -               Johnson\n");
    fprintf (out, "20      R     -               Johnson\n");
    fprintf (out, "21      I     -               Johnson\n");
    fprintf (out, "22      U     -               Cape\n");
    fprintf (out, "23      R     -               Kron\n");
    fprintf (out, "24      I     -               Kron\n");
    exit (0);
}

static void
PrintGSClass ()

{
    FILE *out;	/* Output, stderr for command line, stdout for web */
    if (http) {
	out = stdout;
	fprintf (out, "Content-type: text/plain\n\n");
	}
    else
	out = stderr;
    fprintf (out, "HST Guide Star Catalog Object Classes\n");
    fprintf (out, "0: Stellar\n");
    fprintf (out, "3: Non-Stellar\n");
    fprintf (out, "5: Not really an object\n");
    exit (0);
}

/* Oct 18 1996	New program based on imtab
 * Nov 13 1996	Set maximum nstar from command line if greater than default
 * Nov 14 1996	Set limits from subroutine
 * Nov 19 1996	Fix usage
 * Dec 12 1996	Allow bright as well as faint magnitude limit
 * Dec 12 1996	Fix header for UAC
 * Dec 12 1996	Fix header for UAC magnitudes
 * Dec 13 1996	Write plate into header if selected
 * Dec 18 1996	Allow WCS sky coordinate format as input argument for center
 * Dec 18 1996	Add option to print entries for specified catalog numbers
 * Dec 30 1996	Clean up closest star message
 * Dec 30 1996	Print message instead of heading if no stars are found
 *
 * Jan 10 1997	Fix bug in RASort Stars which did not sort magnitudes
 * Mar 12 1997	Add USNO SA-1.0 catalog as USAC
 * Apr 25 1997	Fix bug in uacread
 * May 29 1997	Add option to add keyword to tab table output
 * Nov 12 1997	Fix DEC in header to print Dec string instead of RA string
 * Nov 17 1997	Initialize both magnitude limits
 * Dec  8 1997	Set up program to be called by various names
 * Dec 12 1997	Fix center coordinate printing in heading
 *
 * Apr 10 1998	Fix bug search USNO A-1.0 catalog created by last revision
 * Apr 10 1998	Set search radius only if argument negative
 * Apr 14 1998	Version 2.2: to match other software
 * Apr 23 1998	Add output in galactic or ecliptic coordinates; use wcscon()
 * Apr 28 1998	Change coordinate system flags to WCS_*
 * Jun  2 1998	Fix bug in tabread()
 * Jun 24 1998	Add string lengths to ra2str() and dec2str() calls
 * Jun 30 1998	Fix declaration of GetArea()
 * Jul  1 1998	Allow specification of center different from output system
 * Jul  9 1998	Adjust all report headings
 * Jul 30 1998	Realign heading and fix help
 * Aug  6 1998	Do not include fitshead.h; it is in wcs.h
 * Sep 10 1998	Add SAOTDC binary format catalogs
 * Sep 15 1998	Adjust output format for binary format catalogs
 * Sep 16 1998	Fix bug creating output filename
 * Sep 21 1998	Do not print distance to search center if not searching
 * Sep 21 1998	Print epoch if not that of equinox
 * Sep 24 1998	Increase search radius for closest star
 * Sep 24 1998	Add second magnitude for Tycho Catalogue
 * Oct 15 1998	Add ability to read TDC ASCII catalog files
 * Oct 16 1998	Add ability to read any TDC binary catalog file
 * Oct 21 1998	Add object name to TDC binary catalogs
 * Oct 21 1998	Use wcscat.h common
 * Oct 23 1998	Allow up to 10 catalogs to be searched at once
 * Oct 26 1998	Return object name in same operation as object position
 * Oct 27 1998	Fix RefCat() calls
 * Oct 29 1998	Add GSC class selection argument x; it should have been there
 * Oct 30 1998	Read object name if accessing catalogs by number, too
 * Nov 20 1998	Implement USNO A-2.0 and SA-2.0 catalogs; differentiate from A1
 * Nov 30 1998	Add version and help commands for consistency
 * Dec  8 1998	Add Hipparcos and ACT catalogs
 * Dec 14 1998	Fix format for UJC
 * Dec 21 1998	Fix format for BINCAT and TXTCAT format catalogs

 * Jan 20 1999	Add option to print search center as output
 * Jan 21 1999	Drop option of adding coordinates to -b -e -g -j
 * Jan 21 1999	Improve command parser to accept fractional degree centers
 * Jan 21 1999	Set output coordinate system for TDC ASCII and Binary catalogs
 * Jan 26 1999	Add option of range of star numbers
 * Feb  1 1999	Add switch to get sequence number unless . in number
 * Feb  2 1999	Vary number of decimal places according to input ASCII catalog
 * Feb  2 1999	Set output equinox, epoch, coorsys from catalog if not set
 * Feb 10 1999	Increase search radius for closest star in smaller catalogs
 * Feb 12 1999	Add ACT catalog from CDROM
 * Feb 18 1999	Make printing name in search centers optional
 * Apr 13 1999	Fix progname to drop / when full pathname
 * May 12 1999	Add option to search from a TDC ASCII catalog
 * May 19 1999	Add option to print search center and closest star on 1 line
 * May 19 1999	Format catalog number using CatNum()
 * May 21 1999	Allow option of setting epoch from search catalog
 * May 28 1999	Allow search from starbase table catalog as well as ASCII
 * May 28 1999	Write epoch of coordinates into tab header if not = equinox
 * Jun  4 1999	Improve labelling for search from file
 * Jun  4 1999	Use calloc() instead of malloc() for proper initialization
 * Jun  4 1999	Allow rectangular in addition to square and circular searches
 * Jun  7 1999	Allow radius input in sexagesimal
 * Jun 30 1999	Use isrange() to check for a range of source numbers
 * Jul  1 1999	Allow any legal FITS date format for epoch
 * Aug 16 1999	Be more careful checking for 2nd -r argument
 * Aug 20 1999	Change u from USNO plate to print X Y 
 * Aug 23 1999	Fix closest star search by setting search radius not box
 * Aug 25 1999	Add the Bright Star Catalog
 * Sep  8 1999	Clean up code
 * Sep 10 1999	Do all searches through catread() and catrnum()
 * Sep 16 1999	Fix galactic coordinate header
 * Sep 16 1999	Add distsort argument to catread() call
 * Sep 21 1999	Change description of search area from square to region
 * Oct 15 1999	Fix calls to catopen() and catclose()
 * Oct 22 1999	Drop unused variables after lint
 * Oct 22 1999	Change catread() to ctgread() to avoid system conflict
 * Oct 22 1999	Increase default r for closest search to 1800 arcsec
 * Nov 19 1999	Use CatNum when concocting names from numbers
 * Nov 29 1999	Include fitsfile.h for date conversion
 *
 * Jan 11 2000	Get nndec for Starbase catalogs
 * Feb 10 2000	Use reference catalog coordinate system by default
 * Feb 15 2000	Use MAXCAT from lwcs.h instead of MAXREF
 * Feb 28 2000	Drop Peak column if not set in TAB catalogs
 * Mar 10 2000	Move catalog selection from executable name to subroutine
 * Mar 14 2000	Lowercase all header keywords
 * Mar 14 2000	Add proper motion, if present, to Starbase output
 * Mar 27 2000	Drop unused subroutine setradius() declaration
 * Mar 28 2000	Clean up output for catalog IDs and GSC classes
 * Apr  3 2000	Allocate search return buffers only once per execution
 * May 26 2000	Add Tycho 2 catalog
 * May 26 2000	Always use CatNumLen() to get ID number field size
 * May 31 2000	Do not sort if only one star
 * Jun  2 2000	Set to NULL when freeing
 * Jun 23 2000	Add degree output for one-line matches (-l)
 * Jun 26 2000	Add XY output
 * Jul 13 2000	Add star catalog data structure to ctgread() argument list
 * Jul 13 2000	Precess search catalog sources to specified system and epoch
 * Jul 25 2000	Pass address of star catalog data structure address
 * Sep  1 2000	Call CatNum with -nnfld to print leading zeroes on search ctrs
 * Sep 21 2000	Print spectral type instead of plate number of USNO A catalogs
 * Sep 25 2000	Print spectral type and second magnitude if present in one-line
 * Oct 26 2000	Print proper motion in msec/year and masec/year unless degout
 * Nov  3 2000	Print proper motion in sec/century and arcsec/century
 * Nov  9 2000	Set output equinox and epoch from -j and -b flags
 * Nov 17 2000	Add keyword=value command line parameter decoding
 * Nov 22 2000	Pass starcat to ctgrnum()
 * Nov 28 2000	Add CGI query decoding
 * Dec 15 2000	Add code to deal with coordinates without systems
 * Dec 18 2000	Change two-argument box size separator from space to comma
 * Dec 18 2000	Always allocate proper motion arrays
 * Dec 29 2000	Set debug flag if two v's encountered as command line arguments
 *
 * Jan  2 2001	Fix proper motion test; fix box size in heading
 * Mar  1 2001	If printing x and y, add .match extension to output file
 * Mar  1 2001	Print output file as tab/Starbase only if -t
 * Mar  1 2001	Add -z option to append to output file
 * Mar 23 2001	If catalog system, equinox, and epoch not set, set to J2000
 * Mar 23 2001	Set epoch and equinox to match search coords if not set
 * Apr 24 2001	Add HST GSC band output
 * May 22 2001	Rewrite sort options to include declination and no sort
 * May 23 2001	Add GSC-ACT catalog (updated HST GSC)
 * May 30 2001	Add 2MASS Point Source Catalog
 * Jun  5 2001	Read tab table search catalog one line at a time
 * Jun  6 2001	Set output equinox like output epoch; fix one-line tab bugs
 * Jun  7 2001	Add arguments to RefCat()
 * Jun 13 2001	Make -r argument radius if positive box half-width if negative
 * Jun 13 2001	Print id and magnitude column headings for tab table searches
 * Jun 13 2001	Print headings only of first of list of star numbers
 * Jun 14 2001	Initialize gobj elements to NULL
 * Jun 18 2001	Fix printing of long spectral type from search catalog
 * Jun 22 2001	Add GSC2 and modify Hipparcos output
 * Jun 25 2001	Add fifth digit to IRAS 3rd and 4th magnitudes
 * Jun 26 2001	If nstars < 0, print from catalog subroutine
 * Jul  3 2001	Add help to CGI response
 * Jul  5 2001	Add hundredths digit to magnitude limit
 * Jul 18 2001	Fix CGI help response; add web help to command line
 * Aug  8 2001	Add support for radial velocities
 * Aug 24 2001	Add support for radial velocities in one-line reports
 * Sep 12 2001	Use 2-D array of magnitudes, rather than multiple vectors
 * Sep 13 2001	Add sort magnitude option as -m* (no space)
 * Sep 14 2001	Add sort magnitude options to web interface
 * Sep 17 2001	Add limits for 2MASS Point Source Catalog
 * Sep 19 2001	Fix bug dealing with IRAS limited fluxes in one-line output
 * Sep 20 2001	Print appropriate limiting magnitude and name
 * Oct 19 2001	Allow FITS or fractional year for epoch (-y) or equinox (-q)
 * Oct 24 2001	Convert RA half-width to sky arcseconds by dividing by cos(dec)
 * Oct 24 2001	Improve error reporting
 * Nov 13 2001	Add option to set object name field length using -i
 * Nov 14 2001	Print 7 decimal places if degrees printed to avoid losing precision
 * Nov 20 2001	Check all allocated memory before freeing it
 * Nov 20 2001	Change temp to newranges when reallocating ranges
 * Dec 11 2001	Ignore CGI query string if there are command line arguments without error
 *
 * Feb  1 2002	Print spectral type for TDC format catalogs, if present
 * Mar  2 2002	Add web query sr= option for search radius in degrees
 * Apr  3 2002	Add magnitude number to sort options
 * Apr  8 2002	Add magnitude number to magnitude limit setting
 * Apr 10 2002	Fix magnitude number bug and add magnitude letter
 * Aug  2 2002	Fix bug printing UJC magnitudes
 * Aug  5 2002	Add proper motion of searched catalog in one-line output
 * Aug  5 2002	Handle magnitudes and proper motions for search catalogs
 * Aug  6 2002	Rewrite to handle vectors of magnitudes
 * Aug  6 2002	Fix proper motion units to sec|arcsec/year
 * Sep 18 2002	Add VOTable output option
 * Sep 27 2002	Fix bug which printed only one line of results for spaced output
 * Sep 27 2002	Fix bug which mischecked proper motions
 * Oct 18 2002	Never print RevMsg when outputting VOTable format
 * Oct 26 2002	Clean up output
 * Oct 30 2002	Print out coordinate epoch
 * Nov  4 2002	Add entry-by-entry epoch filter
 * Dec  6 2002	Fix epoch output format bug
 *
 * Jan 26 2003	Add USNO-B1.0 Catalog
 * Jan 28 2003	Assume dra on command line in RA arcseconds, not sky
 * Jan 28 2003	Add option to set number of decimal places over web
 * Jan 29 2003	Add options to set min ID and min PM qual for USNO-B1.0
 * Jan 29 2003	Add header lines if USNO-B1.0 ID or PM quality limits
 * Jan 29 2003	Print magnitude headers from Starbase files
 * Jan 30 2003	Put proper motion back into one line output
 * Feb  4 2003	Fix bug freeing magnitude vectors prior to reallocating them
 * Feb 20 2003	Fix typos and drop unused variables after lint
 * Mar  3 2003	Fix proper motion spacing bug in one line tab table output
 * Mar 10 2003	Always make search box in angle on sky
 * Mar 10 2003	Add letter or number of magnitude if sorted
 * Mar 25 2003	Output coordinate system can be different from search system
 * Mar 25 2003	Fix headings print search, catalog, and output coordinates
 * Apr 13 2003	Set revision message for subroutines using setrevmsg()
 * Apr 24 2003	Add UCAC1 catalog
 * May 27 2003	Add TMIDR2 to TMPSC formats
 * May 28 2003	Print radecsys in tab table header
 * May 30 2003	Add UCAC2 catalog
 * Jun  2 2003	Print proper motions as mas/year to make them more useful
 * June 4 2003	Fix bug so filed input numbers are matched to catalog numbers
 * June 4 2003	Fix bug so -rr command works
 * Jun 11 2003	Always print proper motion as f6.1
 * Jun 13 2003	Fix comparison of character to pointer
 * Aug  7 2003	Allow sorting of stars read by number; NOSORT -> SORT_NONE
 * Aug 11 2003	Fix bug so one-line search output gets mag heads correct
 * Aug 19 2003	Fix help listing to note that magnitude limits are comma-separated
 * Aug 20 2003	Print 4 decimal places in equinox and epoch in heading
 * Aug 22 2003	Add -r second radius giving search annulus
 * Sep 23 2003	Add -s e to merge all catalog objects less than rad0 apart
 * Nov 22 2003	Add class to GSC II output
 * Dec  3 2003	Fix bugs in single line tab-separated output headers
 * Dec  3 2003	Add support for USNO YB6 Catalog
 * Dec 10 2003	Fix limiting magnitudes for search catalogs
 *
 * Jan  5 2004	Add support for SDSS Photmetric Catalog
 * Jan 12 2004	Add support for 2MASS Extended Source Catalog
 * Jan 22 2004	Call setlimdeg() to optionally print limits in degrees
 * Jan 23 2004	Print radius in degrees in verbose mode if degout is set
 * Feb 23 2004	Fix bug which printed too many ---'s for 2MASS PSC in tab mode
 * Mar  4 2004	Fix bug in setting merge flag
 * Mar 16 2004	Use star count returned from catalog merging subroutine
 * Mar 17 2004	RA-sort in MergeStars(); add logging
 * May 12 2004	Exit with error message if no catalog name is given
 * Aug 30 2004	Fix two subroutine declarations
 * Aug 30 2004	Drop out if RefCat cannot find catalog
 * Sep 10 2004	Do not print anything if no stars found and not in verbose
 *		or tab table output mode
 * Sep 17 2004	Use -h to turn off header if no stars found in tab table mode
 * Oct 20 2004	Fix -rr and -r two argument definitions to match reality
 * Nov 17 2004	Fix main output loop bug and print type name in one line output
 * Nov 19 2004	Add star/galaxy code to USNO-B1.0 output
 *
 * Apr 19 2005	Fix minor format bug when printing tabbed epoch
 * May 12 2005	Add 2MASS ID decoding
 * Jul 27 2005	Add dateform=[i,f,m,e] to format output dates using DateString()
 * Aug  1 2005	Always set epoch; test only if a limit is present
 * Aug  3 2005	If nstars<0, print catalog information as found, unsorted
 * Aug  5 2005	Add 2MASS and Tycho-2 catalogs with errors
 *
 * Jan  5 2006	Set output equinox correctly; match epoch to set equinox
 * Jan  6 2006	Print search center in input system as well as output system
 * Jan  6 2006	Precess search center if input and output equinox different
 * Mar 15 2006	Clean up VOTable code
 * Mar 17 2006	Add VOTable error reporting
 * Apr 13 2006	Add sort by ID number
 * Jun  6 2006	Add 2 spaces per magnitude header
 * Jun  8 2006	Print object name if no number present in one-line output
 * Jun 21 2006	Initialize uninitialized variables
 * Jun 27 2006	Print ___ if requested keyword in Starbase file not present
 * Sep 26 2006	Increase length of rastr and destr from 16 to 32
 * Sep 26 2006	Allow coordinates on command line to be any length
 * Nov  6 2006	Print SDSS number as character string; it is now 18 digits long
 *
 * Jan 10 2007	Declare RevMsg static, not const
 * Jan 10 2007	Drop extra argument to IDSortStars (last one)
 * Mar 13 2007	Print GSC2 ID from object name, not number
 * May 21 2007	Raise maximum object name length from 32 to 79
 * Jul  6 2007	Drop a magnitude if epoch present in search catalog
 * Jul  9 2007	Print 5 magnitudes for GSC 2.3, not 4; add B
 * Jul  9 2007	Fix bug so GSC2 or SDSS numbers print correctly in 1-line output
 * Jul 20 2007	Add SkyBot report format
 * Aug 24 2007	Add mrad for search radius in minutes; add number checking, too
 *
 * Apr  9 2009	Add "degrees" as CGI parameter
 * Aug 17 2009	Print only four magnitudes for GSC2: F,J,N,V
 * Sep 25 2009	Add one more format to non-tab GSC2 output magnitudes
 * Oct 22 2009	Add UCAC3 catalog headings
 * Nov  5 2009	Add UCAC3 errors and number of measurements
 * Dec 14 2009	Fix but so enough space is allocated for UCAC2+3 errors
 */
