/* File xy2sky.c
 * July 18, 2007
 * By Doug Mink, Harvard-Smithsonian Center for Astrophysics
 * Send bug reports to dmink@cfa.harvard.edu

   Copyright (C) 1996-2007 
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

static void PrintUsage();
extern struct WorldCoor *GetWCSFITS();	/* Read WCS from FITS or IRAF header */
static void PrintHead();

static int verbose = 0;		/* verbose/debugging flag */
static int append = 0;		/* append input line flag */
static int tabtable = 0;	/* tab table output flag */
static int identifier = 0;	/* 1st column=id flag */
static char coorsys[16];
static int linmode = -1;
static int face = 1;
static int ncm = 0;
static int printhead = 0;
static char printonly = 'n';
static int centerset = 0;
static int sizeset = 0;
static int scaleset = 0;
static int version = 0;		/* If 1, print only program name and version */

static char *RevMsg = "XY2SKY WCSTools 3.8.1, 14 December 2009, Doug Mink (dmink@cfa.harvard.edu)";

int
main (ac, av)
int ac;
char **av;
{
    char *str, *str1;
    char wcstring[64];
    char lstr = 64;
    int ndecset = 0;
    int degout = 0;
    int ndec = 3;
    int nlines, iline;
    int entx = 0;
    int enty = 0;
    int entmag = 0;
    int i, ic;
    double x, y, mag;
    double cra, cdec, dra, ddec, secpix, drot;
    double eqout = 0.0;
    double eqin = 0.0;
    int sysout = 0;
    int wp, hp, nx, ny, lhead;
    FILE *fd = NULL;
    char *ln, *listcoord;
    char *listname;
    char **fnx, *newfn;
    int *ft, *newft;
    char linebuff[1024];
    char *line;
    char *header;
    char *fn;
    int bitpix = 0;
    struct WorldCoor *wcs;
    char xstr[32], ystr[32], mstr[32];
    char rastr[32], decstr[32];
    char keyword[16];
    char temp[64];
    int ncx = 0;
    char *cofile;
    char *cobuff;
    char *space, *cstr, *dstr;
    char *fni;
    int nterms = 0;
    char *ilistfile;
    int modwcs = 0;
    double mag0, magx;
    double coeff[5];
    struct Tokens tokens;
    struct TabTable *tabxy;
    struct WorldCoor *GetFITSWCS();

    nfile = 0;
    fn = (char **)calloc (maxnfile, sizeof(char *));
    ft = (int *)calloc (maxnfile, sizeof(int));
    listcoord = NULL;
    listfile = NULL;

    *coorsys = 0;
    cofile = NULL;

    /* Check for help or version command first */
    str = *(av+1);
    if (!str || !strcmp (str, "help") || !strcmp (str, "-help"))
	PrintUsage (str);
    if (!strcmp (str, "version") || !strcmp (str, "-version")) {
	version = 1;
	PrintUsage (str);
	}

    /* crack arguments */
    for (av++; --ac > 0; av++) {
	fn = *av;
	if (*(str = *av)=='-') {
	    char c;
	    while ((c = *++str))
    	    switch (c) {

    	    case 'v':	/* more verbosity */
    		verbose++;
    		break;

	    case 'a':	/* Append input line to sky position */
		append++;
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
		    setsys (WCS_B1950);
		    sysout = WCS_B1950;
		    eqout = 1950.0;
		    strcpy (rastr, *++av);
		    ac--;
		    strcpy (decstr, *++av);
		    ac--;
		    setcenter (rastr, decstr);
		    modwcs = 1;
		    }
		strcpy (coorsys,"B1950");
		break;

	    case 'c':	/* Magnitude conversion coefficients */
		if (ac < 2)
		    PrintUsage (str);
		cofile = *++av;
		ac--;
    		break;

	    case 'd':	/* Output degrees instead of hh:mm:ss dd:mm:ss */
        	degout++;
        	break;

	    case 'e':	/* Output galactic coordinates */
		str1 = *(av+1);
		ic = (int)str1[0];
		if (*(str+1) || ic < 48 || ic > 58) {
		    setsys (WCS_ECLIPTIC);
		    sysout = WCS_ECLIPTIC;
		    eqout = 2000.0;
		    }
		else if (ac < 3)
		    PrintUsage (str);
		else {
		    setsys (WCS_ECLIPTIC);
		    sysout = WCS_ECLIPTIC;
		    eqout = 2000.0;
		    strcpy (rastr, *++av);
		    ac--;
		    strcpy (decstr, *++av);
		    ac--;
		    setcenter (rastr, decstr);
		    modwcs = 1;
		    }
		strcpy (coorsys,"ecliptic");
        	degout++;
    		break;

	    case 'f':	/* Face to use for projections with 3rd dimension */
		if (ac < 2)
		    PrintUsage (str);
		face = atoi (*++av);
		(void) wcszin (face);
		ac--;
		break;

	    case 'g':	/* Output galactic coordinates */
		str1 = *(av+1);
		ic = (int)str1[0];
		if (*(str+1) || ic < 48 || ic > 58) {
		    setsys (WCS_GALACTIC);
		    sysout = WCS_GALACTIC;
		    eqout = 2000.0;
		    }
		else if (ac < 3)
		    PrintUsage (str);
		else {
		    setsys (WCS_GALACTIC);
		    sysout = WCS_GALACTIC;
		    eqout = 2000.0;
		    strcpy (rastr, *++av);
		    ac--;
		    strcpy (decstr, *++av);
		    ac--;
		    setcenter (rastr, decstr);
		    modwcs = 1;
		    }
		strcpy (coorsys,"galactic");
        	degout++;
    		break;

	    case 'h':	/* Print column heading */
		printhead++;
		break;

	    case 'i':	/* 1st column = star id */
		identifier++;
		break;

	    case 'j':       /* center coordinates on command line in J2000 */
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
		    modwcs = 1;
		    }
		strcpy (coorsys,"J2000");
		break;

	    case 'k':	/* column for X; Y is next column */
		ncx = atoi (*++av);
		ac--;
    		break;

	    case 'l':	/* Mode for output of linear coordinates */
		if (ac < 2)
		    PrintUsage (str);
		linmode = atoi (*++av);
		ac--;
		break;

	    case 'm':	/* column for magnitude */
		ncm = atoi (*++av);
		ac--;
    		break;

	    case 'n':	/* Number of decimal places in output sec or deg */
		if (ac < 2)
		    PrintUsage (str);
		ndec = atoi (*++av);
		ndecset++;
		ac--;
		break;

	    case 'o':   /* Output only the following part of the coordinates */
		if (ac < 2)
		    PrintUsage (str);
		av++;
		printonly = **av;
		ac--;
		break;

	    case 'p':       /* Initial plate scale in arcseconds per pixel */
		if (ac < 2)
		    PrintUsage (str);
		setsecpix (atof (*++av));
		ac--;
		break;

	    case 'q':	/* Equinox for output */
		if (ac < 2)
		    PrintUsage (str);
		strcpy (coorsys, *++av);
		ac--;
		break;

	    case 's':   /* Size of image in X and Y pixels */
		if (ac < 3)
		    PrintUsage(str);
		nx = atoi (*++av);
		ac--;
		ny = atoi (*++av);
		ac--;
		modwcs = 1;
		setnpix (nx, ny);
		break;

	    case 't':	/* Output tab table */
		tabtable++;
    		break;

	    case 'x':       /* X and Y coordinates of reference pixel */
		if (ac < 3)
		    PrintUsage (str);
		x = atof (*++av);
		ac--;
		y = atof (*++av);
		ac--;
		setrefpix (x, y);
		modwcs = 1;
		break;

	    case 'y':       /* Epoch of image in FITS date format */
		if (ac < 2)
		    PrintUsage (str);
		setdateobs (*++av);
		ac--;
		modwcs = 1;
		break;

    	    case 'z':	/* Use AIPS classic WCS */
    		setdefwcs (WCS_ALT);
    		break;

    	    default:
    		PrintUsage (str);
    		break;
	    }

	/* Read name of list file and classify it */
	else if (fn[0] == '@') {
	    listname = *av + 1;

	/* List of image files */
	    if (isimlist (listname)) {
		listfile = listname;
		nfile = getfilelines (listfile);
		}

	/* List of files which may be image files (checked when extracted) */
	    else if (isfilelist (listname, rootdir)) {
		listfile = listname;
		nfile = getfilelines (listfile);
		}

	    /* List of x y coordinates */
	    else {
		listcoord = listname;
		if (strcmp (listcoord,"STDIN")==0 || strcmp (listcoord,"stdin")==0) {
		    fd = stdin;
		    nlines = 10000;
		    tabxy = NULL;
		    if (printhead || verbose || tabtable)
			PrintHead (fn, wcs, NULL, listcoord);
		    }
		else if (istab (listcoord)) {
		    tabxy = tabopen (listcoord, 0);
		    nlines = tabxy->nlines;
		    tabtable = 1;
		    wcs->tabsys = 1;
		    if (append) {
			tabtable++;
			PrintHead (fn, wcs, tabxy, listcoord);
			}
		    else
			PrintHead (fn, wcs, NULL, listcoord);

		    /* Find columns for X and Y */
		    entx = tabccol (tabxy, "x");
		    enty = tabccol (tabxy, "y");

		    /* Find column for magnitude */
		    if (!(entmag = tabccol (tabxy, "mag"))) {
			if (!(entmag = tabccol (tabxy, "magv"))) {
			    if (!(entmag = tabccol (tabxy, "magj")))
				entmag = tabccol (tabxy, "magr");
			    }
			}
		    }
		else {
		    if (printhead || verbose || tabtable)
			PrintHead (fn, wcs, NULL, listcoord);
		    tabxy = NULL;
		    nlines = getfilelines (listcoord);
		    fd = fopen (listcoord, "r");
		    if (fd == NULL) {
			fprintf (stderr, "Cannot read file %s\n", listcoord);
			nlines = 0;
			}
		    }
		}
	    }

	/* Add file name to list of possible image files */
	else if (isfits(fn) || isiraf(fn) || istiff(fn) || isjpeg(fn) || isgif(fn)) {
	    if (nfile >= maxnfile) {
		maxnfile = maxnfile * 2;
		nbytes = maxnfile * sizeof (char *);
		newfn = (char **) calloc (maxnfile, sizeof (char *));
		for (i = 0; i < nfile; i++)
		    newfn[i] = fn[i];
		free (fn);
		fn = newfn;
		newft = (int *) calloc (maxnfile*2, sizeof (int));
		for (i = 0; i < nfile; i++)
		    newft[i] = ft[i];
		free (ft);
		ft = newft;
		maxnfile = maxnfile * 2;
		}
	    fnx[nfile] = *av;
	    if (isfits (*av))
		ftx[nfile] = FILE_FITS;
	    else
		ftx[nfile] = FILE_IRAF;

	    lfn = strlen (fn);
	    if (lfn > maxlfn)
		maxlfn = lfn;
	    nfile++;
	    }
	}

    /* If no files and no coordinates print online help */
    if (nfile == 0 && nlines == 0)
	PrintUsage ();

    /* If no file is given make WCS from arguments if coordinates entered */
    wcs = NULL;
    if (nfile == 0 && nlines > 0) {
	fni = NULL;
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
	wcs = GetFITSWCS (fni,header,verbose,&cra,&cdec,&dra,&ddec,&secpix,
			  &wp, &hp, &sysout, &eqout);
	if (nowcs (wcs)) {
	    fprintf (stderr, "Incomplete WCS on command line\n");
	    wcsfree (wcs);
	    free (header);
	    exit (1);
	    }
	else
	    nfile = 1;
	}

    /* Loop through files */
    for (ifile = 0; ifile < nfiles; ifile++) {
	if (!wcs || ifile > 0) {
	    fni = fn[i];
	    if (isfits(fni) || isiraf(fni) || istiff(fni) ||
		isjpeg(fni) || isgif(fni)) {
		wcs = GetWCSFITS (fni, verbose);
		if (nowcs (wcs)) {
		    printf ("%s: No WCS for file, cannot compute image size\n", fni);
		    wcsfree (wcs);
		    exit(1);
		    }
		}
	    }
	if (linmode > -1)
	    setwcslin (wcs, linmode);
	if (wcs->sysout == WCS_B1950)
	    wcsoutinit (wcs, "B1950");
	if (wcs->sysout == WCS_J2000)
	    wcsoutinit (wcs, "J2000");
	if (*coorsys)
	    wcsoutinit (wcs, coorsys);

	/* Get magnitude conversion polynomial coefficients */
	if (cofile != NULL) {
	    cobuff = getfilebuff (cofile);
	    mag0 = 0.0;
	    (void) agetr8 (cobuff, "mag0", &mag0);
	    for (i = 0; i < 5; i++) {
		coeff[i] = 0.0;
		sprintf (keyword, "mcoeff%d", i);
		(void) agetr8 (cobuff, keyword, &coeff[i]);
		if (coeff[i] != 0.0) nterms = i + 1;
		}
	    if (ncm == 0)
		ncm = 3;
	    }
	if (degout) {
	    wcs->degout = degout;
            if (!ndecset) {
        	ndec = 5;
		ndecset++;
		}
	    }
	if (tabtable)
	    wcs->tabsys = 1;
	if (ndecset)
	    wcs->ndec = ndec;
	}


	/* Loop through image coordinates */
	while (ac-- > 1) {

	/* Process file of image coordinates */
	listcoord = *av;
	if (listcoord[0] == '@') {
	    ln = listcoord;
	    while (*ln++)
		*(ln-1) = *ln;
	    if (strcmp (listcoord,"STDIN")==0 || strcmp (listcoord,"stdin")==0) {
		fd = stdin;
		nlines = 10000;
		tabxy = NULL;
		if (printhead || verbose || tabtable)
		    PrintHead (fn, wcs, NULL, listcoord);
		}
	    else if (istab (listcoord)) {
		tabxy = tabopen (listcoord, 0);
		nlines = tabxy->nlines;
		tabtable = 1;
		wcs->tabsys = 1;
		if (append) {
		    tabtable++;
		    PrintHead (fn, wcs, tabxy, listcoord);
		    }
		else
		    PrintHead (fn, wcs, NULL, listcoord);

		/* Find columns for X and Y */
		entx = tabccol (tabxy, "x");
		enty = tabccol (tabxy, "y");

		/* Find column for magnitude */
		if (!(entmag = tabccol (tabxy, "mag"))) {
		    if (!(entmag = tabccol (tabxy, "magv"))) {
			if (!(entmag = tabccol (tabxy, "magj")))
			    entmag = tabccol (tabxy, "magr");
			}
		    }
		}
	    else {
		if (printhead || verbose || tabtable)
		    PrintHead (fn, wcs, NULL, listcoord);
		tabxy = NULL;
		nlines = getfilelines (listcoord);
		fd = fopen (listcoord, "r");
		if (fd == NULL) {
		    fprintf (stderr, "Cannot read file %s\n", listcoord);
		    nlines = 0;
		    }
		}
	    for (iline = 0; iline < nlines; iline++) {
		if (tabxy != NULL) {

		    /* Read line for next position */
		    if ((line = gettabline (tabxy, iline+1)) == NULL) {
			fprintf (stderr,"Cannot read star %d\n", iline);
			break;
			}

		    /* Extract x, y, and magnitude */
		    (void) setoken (&tokens, line, "tab");
		    x = tabgetr8 (&tokens, entx);
		    y = tabgetr8 (&tokens, enty);
		    if (ncm)
			mag = tabgetr8 (&tokens, entmag);
		    }
		else {
		    if (!fgets (linebuff, 1023, fd))
			break;
		    line = linebuff;
		    if (line[0] == '#')
			continue;
		    if (ncm || ncx)
			setoken (&tokens, line, "");
		    if (ncx) {
			getoken (&tokens, ncx, xstr, 31);
			getoken (&tokens, ncx+1, ystr, 31);
			}
		    else if (identifier)
			sscanf (line,"%s %s %s", temp, xstr, ystr);
		    else
			sscanf (line,"%s %s", xstr, ystr);
		    x = atof (xstr);
		    y = atof (ystr);
		    if (ncm) {
			wcs->printsys = 0;
			getoken (&tokens, ncm, mstr, 31);
			mag = atof (mstr);
			}
		    }
		if (pix2wcst (wcs, x, y, wcstring, lstr)) {
		    /* Remove coordinate system if tab table output
		    if (tabtable) {
			ctab = strrchr (wcstring, (char) 9);
			*ctab = (char) 0;
			} */
		    if (wcs->sysout == WCS_ECLIPTIC) {
			sprintf(temp,"%.5f",wcs->epoch);
			strcat (wcstring, " ");
			strcat (wcstring, temp);
			}
		    if (ncm) {
			magx = polcomp (mag, mag0, nterms, coeff);
			if (tabtable)
			    sprintf(temp,"	%6.2f", magx);
			else
			    sprintf(temp," %6.2f", magx);
			strcat (wcstring, temp);
			}
		    if (append) {
			if (tabtable)
			    printf ("%s	%s", wcstring, line);
			else
			    printf ("%s %s", wcstring, line);
			}
		    else {
			if (tabtable)
			    printf ("%s	", wcstring);
			else
			    printf ("%s ", wcstring);

			if (verbose && !tabtable)
			    printf (" <- ");
			if (wcs->nxpix > 9999 || wcs->nypix > 9999) {
			    if (tabtable)
				printf ("%9.3f	%9.3f	",x, y);
			    else
				printf ("%9.3f %9.3f ",x, y);
			    }
			else if (wcs->nxpix > 999 || wcs->nypix > 999) {
			    if (tabtable)
				printf ("%8.3f	%8.3f	",x, y);
			    else
				printf ("%8.3f %8.3f ",x, y);
			    }
			else {
			    if (tabtable)
				printf ("%7.3f	%7.3f	",x, y);
			    else
				printf ("%7.3f %9.3f ",x, y);
			    }
			if (wcs->naxis > 2) {
			    if (tabtable)
				printf ("%2d	", face);
			    else
				printf ("%2d  ", face);
			    }
			}
		    printf ("\n");
		    }
		}
	    av++;
	    }

	/* Process image coordinates from the command line */
	else if (ac > 1) {
	    if (printhead || verbose) {
		PrintHead (fn, wcs, NULL, NULL);
		printhead = 0;
		}
	    x = atof (*av);
	    ac--;
	    y = atof (*++av);
	    if (pix2wcst (wcs, x, y, wcstring, lstr)) {
		if (wcs->sysout == WCS_ECLIPTIC) {
		    sprintf(temp,"%.5f",wcs->epoch);
		    if (tabtable)
			strcat (wcstring, "	");
		    else
			strcat (wcstring, " ");
		    strcat (wcstring, temp);
		    }
		if (ncm) {
		    magx = polcomp (mag, mag0, nterms, coeff);
		    if (tabtable)
			sprintf(temp,"	%6.2f", magx);
		    else
			sprintf(temp," %6.2f", magx);
		    strcat (wcstring, temp);
		    }
		if (printonly == 'r') {
		    space = strchr (wcstring, ' ');
		    *space = (char) 0;
		    printf ("%s ", wcstring);
		    if (tabtable)
			printf ("%s	", wcstring);
		    else
			printf ("%s ", wcstring);
		    }
		else if (printonly == 'd') {
		    dstr = strchr (wcstring, ' ') + 1;
		    space = strchr (dstr, ' ');
		    *space = (char) 0;
		    if (tabtable)
			printf ("%s	", dstr);
		    else
			printf ("%s ", dstr);
		    }
		else if (printonly == 's') {
		    dstr = strchr (wcstring, ' ') + 1;
		    cstr = strchr (dstr, ' ') + 1;
		    if (tabtable)
			printf ("%s	", cstr);
		    else
			printf ("%s ", cstr);
		    }
		else {
		    if (tabtable)
			printf ("%s	", wcstring);
		    else
			printf ("%s ", wcstring);
		    if (verbose && !tabtable)
			printf (" <- ");
		    if (wcs->nxpix > 9999 || wcs->nypix > 9999) {
			if (tabtable)
			    printf ("%9.3f	%9.3f	",x, y);
			else
			    printf ("%9.3f %9.3f ",x, y);
			}
		    else if (wcs->nxpix > 999 || wcs->nypix > 999) {
			if (tabtable)
			    printf ("%8.3f	%8.3f	",x, y);
			else
			    printf ("%8.3f %8.3f ",x, y);
			}
		    else {
			if (tabtable)
			    printf ("%7.3f	%7.3f	",x, y);
			else
			    printf ("%7.3f %9.3f ",x, y);
			}
		    if (wcs->naxis > 2) {
			if (tabtable)
			    printf ("%2d	", face);
			else
			    printf ("%2d  ", face);
			}
		    }
		printf ("\n");
		}
	    av++;
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
    fprintf (stderr,"Compute RA Dec from X Y using WCS in FITS and IRAF image files\n");
    fprintf (stderr,"Usage: [-abdjgv] [-n ndec] file.fits x1 y1 ... xn yn\n");
    fprintf (stderr,"  or : [-abdjgv] [-n ndec] file.fits @listname\n");
    fprintf (stderr,"  -a: append input line after output position\n");
    fprintf (stderr,"  -b [RA Dec]: Output (center) B1950 (FK4)\n");
    fprintf (stderr,"  -c: file with coefficients for magnitude conversion\n");
    fprintf (stderr,"  -d: RA and Dec output in degrees\n");
    fprintf (stderr,"  -e [long lat]: Output (center) in ecliptic coordinates\n");
    fprintf (stderr,"  -f: Number of face to use for 3-d projection\n");
    fprintf (stderr,"  -g [long lat]: Output (center) in galactic coordinates\n");
    fprintf (stderr,"  -i: first column is star id; 2nd, 3rd are x,y position\n");
    fprintf (stderr,"  -j [RA Dec]: Output (center) J2000 (FK5)\n");
    fprintf (stderr,"  -k num: Column for image X coordinate; Y follows\n");
    fprintf (stderr,"  -l: mode for output of LINEAR WCS coordinates\n");
    fprintf (stderr,"  -m col: column for magnitude (defaults to 3 if -c used)\n");
    fprintf (stderr,"  -n num: number of decimal places in output RA seconds\n");
    fprintf (stderr,"  -o r|d|s: print only ra, dec, or coordinate system\n");
    fprintf (stderr,"  -p num: Initial plate scale in arcsec per pixel\n");
    fprintf (stderr,"  -q: output equinox if not 2000 or 1950\n");
    fprintf (stderr,"  -s x y: horizontal and vertical dimensions of image \n");
    fprintf (stderr,"  -t: tab table output\n");
    fprintf (stderr,"  -v: verbose\n");
    fprintf (stderr,"  -x x y: X and Y coordinates of reference pixel (default is center)\n");
    fprintf (stderr,"  -y date: Epoch of image in FITS date format or year\n");
    fprintf (stderr,"  -y date: Epoch of image in FITS date format or year\n");
    fprintf (stderr,"  -z: use AIPS classic projections instead of WCSLIB\n");
    exit (1);
}

static void
PrintHead (fn, wcs, tabxy, listfile)

char	*fn;		/* Name of file containing list of x,y coordinates */
struct WorldCoor *wcs;	/* World coordinate system structure */
struct TabTable *tabxy;	/* tab table structure */
char *listfile;		/* Name of file with list of input coordinates */

{
    char newline = (char) 10;
    char *eol;
    char *ctab, *tabhead;
    int lhead, i;

    if (tabtable) {
	printf ("image	%s\n", fn);
	if (listfile != NULL)
	    printf ("listfile	%s\n", listfile);
	printf ("radecsys	%s\n",wcs->radecout);
	printf ("epoch	%.4f\n",wcs->epoch);
	printf ("program	%s\n",RevMsg);
	if (wcs->sysout == WCS_B1950 || wcs->sysout == WCS_J2000)
	    printf ("ra         	dec         	");
	else if (wcs->sysout == WCS_GALACTIC)
	    printf ("glon     	glat     	");
	else if (wcs->sysout == WCS_ECLIPTIC)
	    printf ("elon     	elat     	");
	if (wcs->sysout == WCS_ECLIPTIC || wcs->sysout == WCS_GALACTIC)
	    printf ("sys     	");
	else
	    printf ("sys 	");
	if (wcs->sysout == WCS_ECLIPTIC)
	    printf ("epoch     	");
	
	if (ncm)
	    printf ("mag   	");
	if (tabxy != NULL) {
	    eol = strchr (tabxy->tabhead, newline);
	    *eol = (char) 0;
	    if (wcs->sysout == WCS_B1950 || wcs->sysout == WCS_J2000) {
		lhead = strlen (tabxy->tabhead);
		tabhead = (char *) calloc (lhead+4, sizeof (char));
		ctab = tabhead;
		for (i = 0; i < lhead; i++) {
		    *ctab++ = tabxy->tabhead[i];
		    if (*(ctab-2) == 'r' && *(ctab-1) == 'a')
			*ctab++ = '0';
		    if (*(ctab-3) == 'd' && *(ctab-2) == 'e' && *(ctab-1) == 'c')
			*ctab++ = '0';
		    }
		printf ("%s", tabhead);
		}
	    else
		printf ("%s", tabxy->tabhead);
	    *eol = newline;
	    }
	else {
	    printf ("x       	y       	");
	    if (wcs->naxis > 2)
		printf ("z    	");
	    }
	printf ("\n");
	if (wcs->degout)
	    printf ("---------	---------	");
	else
	    printf ("------------	------------	");
	if (wcs->sysout == WCS_ECLIPTIC || wcs->sysout == WCS_GALACTIC)
	    printf ("--------	");
	else
	    printf ("-----	");
	if (wcs->sysout == WCS_ECLIPTIC)
	    printf ("----------	");
	if (ncm)
	    printf ("------	");
	if (tabxy != NULL) {
	    eol = strchr (tabxy->tabdash, newline);
	    *eol = (char) 0;
	    printf ("%s", tabxy->tabdash);
	    *eol = newline;
	    }
	else {
	    printf ("--------	--------");
	    if (wcs->naxis > 2)
		printf ("	-----");
	    }
	printf ("\n");
	}
    else {
        fprintf (stderr,"%s\n",RevMsg);
	if (listfile == NULL)
	    fprintf (stderr,
		"Print sky coordinates from %s image coordinates\n", fn);
	else
	    fprintf (stderr,
		"Print sky coordinates from %s image coordinates in %s\n",
		fn, listfile);
	if (wcs->sysout == WCS_ECLIPTIC || wcs->sysout == WCS_GALACTIC)
	    printf ("Longitude  Latitude   Sys    ");
	else
	    printf ("    RA           Dec       Sys  ");
	if (wcs->sysout == WCS_ECLIPTIC)
	    printf("  Epoch    ");
	if (verbose) printf ("    ");
	printf ("    X        Y\n");
	}
    return;
}

/*
 * Feb 23 1996	New program
 * Apr 24 1996	Version 1.1: Add B1950, J2000, or galactic coordinate output options
 * Jun 10 1996	Change name of subroutine which reads WCS
 * Aug 28 1996	Remove unused variables after lint
 * Nov  1 1996	Add options to set number of decimal places and output degrees
 *
 * Dec 15 1997	Print message if no WCS; read IRAF 2.11 header format
 * Dec 15 1997	Drop -> if output sky coordinates are in degrees
 * Dec 31 1997	Allow entire input line to be appended to sky position
 *
 * Jan  7 1998	Apply WFPC and WFPC2 pixel corrections if requested
 * Jan  7 1998	Add tab table output using -t
 * Jan 26 1998	Implement Mark Calabretta's WCSLIB
 * Jan 29 1998	Add -z for AIPS classic WCS projections
 * Feb 18 1998	Version 2.0: Full Calabretta implementation
 * Mar 12 1998	Version 2.1: IRAF TNX projection added
 * Mar 27 1998	Version 2.2: Polynomial plate fit added
 * Apr 24 1998	Increase size of WCS string from 40 to 64
 * Apr 28 1998	Change coordinate system flag to WCS_*
 * Apr 28 1998	Add output mode for linear coordinates
 * Apr 28 1998	Add ecliptic coordinate system output
 * May 13 1998	Allow arbitrary equinox for output coordinates
 * Jun 25 1998	Set WCS subroutine choice with SETDEFWCS()
 * Jul  7 1998	Change setlinmode() to setwcslin()
 * Jul  7 1998	Add -f for face to use in 3-d projection
 * Jul  7 1998	Add 3rd dimension in output
 * Nov 30 1998	Add version and help commands for consistency
 *
 * Mar 29 1999	Add -i option for X,Y after ID in input file (J.-B. Marquette)
 * Apr 29 1999	Drop pix2wcst declaration; it is in wcs.h
 * Jun  2 1999	Make tab output completely-defined catalog
 * Oct 15 1999	Free wcs using wcsfree()
 * Oct 22 1999	Link includes to libwcs
 *
 * Jan 28 2000	Call setdefwcs() with WCS_ALT instead of 1
 *
 * Jul 19 2001	Add -x to specify column for X coordinate; Y follows immediately
 * Jul 19 2001	Add -m to specify column for magnitude
 * Jul 19 2001	Add -c to specify column for magnitude coefficients
 * Jul 23 2001	Add code to calibrate magnitudes using polynomial from immatch
 * Jul 25 2001	Ignore lines with # in first column
 * Sep 12 2001	Fix output to match column headings
 * Oct 16 2001	Increase maximum input line length from 200 to 1024
 * Oct 16 2001	Add option to prepend coordinates to tab-separated table
 * Oct 19 2001	Change names of old ra and dec columns if prepending ra and dec
 * Dec 13 2001	Add -h for headings and add to verbose output
 *
 * Apr  8 2002	Free wcs structure if no WCS is found in file header
 *
 * Jun 19 2002	Add verbose argument to GetWCSFITS()
 *
 * Jan  7 2003	Fix bug which dropped declination for tab output from file
 * Jan  7 2003	Fix bug which failed to ignore #-commented-out input file lines
 * Apr  7 2003	Add -o to output only RA, Dec, or system
 * Oct 14 2003	Change naxes to naxes in wcs structure
 *
 * Feb 23 2006	Allow appended headers in TIFF, JPEG, and GIF files
 * Jun 21 2006	Initialize uninitialized variables
 *
 * Jan 10 2007	Declare RevMsg static, not const
 * Jan 10 2007	Add buffer size=0 to tabopen() call
 * May  2 2007	Add heading for radecsys column in tab table output
 * Jul  5 2007	Parse command line arguments to initialize a WCS without a file
 * Jul 18 2007	Call tabccol() instead of tabcol()
 */
