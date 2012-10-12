/* File imwcs.c
 * April 6, 2007
 * By Doug Mink, after Elwood Downey
 * (Harvard-Smithsonian Center for Astrophysics)
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
#include "libwcs/fitsfile.h"
#include "libwcs/wcs.h"
#include "libwcs/wcscat.h"
#include "libwcs/lwcs.h"

#define MAXFILES 1000
static int maxnfile = MAXFILES;

static void PrintUsage();
static void FitWCS();

static char *RevMsg = "WCSTools 3.8.1, 14 December 2009, Doug Mink (dmink@cfa.harvard.edu)";

static int verbose = 0;		/* verbose/debugging flag */
static int writeheader = 0;	/* write header fields; else read-only */
static int overwrite = 0;	/* allow overwriting of input image file */
static int rot = 0;		/* Angle to rotate image (multiple of 90 deg) */
static int mirror = 0;		/* If 1, flip image right-left before rotating*/
static int bitpix = 0;
static int fitsout = 0;		/* Output FITS file from IRAF input if 1 */
static int imsearch = 1;	/* set to 0 if image catalog provided */
static int erasewcs = 0;	/* Set to 1 to erase initial image WCS */
static int rotatewcs = 1;	/* If 1, rotate FITS WCS keywords in image */
char outname[128];		/* Name for output image */
static char *refcatname;	/* Name of reference catalog to match */
static int version = 0;		/* If 1, print only program name and version */
static char *matchfile;		/* File of X Y RA Dec matches for initial fit */
static char *progname;		/* Name of program as executed */

extern char *RotFITS();
extern int SetWCSFITS();
extern int DelWCSFITS();
extern int PrintWCS();
extern void settolerance();
extern void setreflim();
extern void setrot();
extern void setnfit();
extern void setbin();
extern void setnfiterate();
extern void setsecpix();
extern void setsecpix2();
extern void setcenter();
extern void setresid_refine();
extern void setsys();
extern void setminb();
extern void setmaxcat();
extern void setstarsig();
extern void setgsclass();
extern void setimcat();
extern void setbmin();
extern void setfrac();
extern void setrefpix();
extern void setwcsproj();
extern void setfitplate();
extern void setproj();
extern void setiterate();
extern void setiteratet();
extern void setrecenter();
extern void setmatch();
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
    char *str, *str1, c, c1, c2;
    double bmin, maglim1, maglim2, drot, arot;
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
    int i, imag;
    int ifile, nfile;

    outname[0] = 0;
    refcatname = NULL;
    matchfile = NULL;
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
	if (str == NULL || !strcmp (str, "help") || !strcmp (str, "-help"))
	    PrintUsage (NULL);

	/* Check for version command */
	else if (!strcmp (str, "version") || !strcmp (str, "-version")) {
	    version = 1;
	    PrintUsage ("version");
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

		case 'a':       /* Initial rotation angle in degrees */
		    if (ac < 2)
			PrintUsage (str);
		    drot = atof (*++av);
		    arot = fabs (drot);
		    if (arot != 90.0 && arot != 180.0 && arot != 270.0) {
			setrot (drot);
			rot = 0;
			}
		    else
			rot = atoi (*av);
		    ac--;
		    break;

    		case 'b':   /* initial coordinates on command line in B1950 */
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

		case 'd':  /* Read image star positions from DAOFIND file */
		    if (ac < 2)
			PrintUsage (str);
		    setimcat (*++av);
		    imsearch = 0;
		    ac--;
		    break;

		case 'e':	/* Erase WCS projection in image header */
		    erasewcs++;
		    break;

    		case 'f':	/* Write FITS file */
		    fitsout = 1;
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

    		case 'j':  /* center coordinates on command line in J2000 */
    		    if (ac < 3)
    			PrintUsage ("* Missing RA Dec or coordinate system");
		    setsys (WCS_J2000);
		    strcpy (rastr, *++av);
		    ac--;
		    strcpy (decstr, *++av);
		    ac--;
		    setcenter (rastr, decstr);
    		    break;

    		case 'k':  /* select magnitude to use from reference catalog */
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
		    maglim1 = -99.0;
    		    maglim2 = atof (*++av);
    		    ac--;
		    if (ac > 1 && isnum (*(av+1))) {
			maglim1 = maglim2;
			maglim2 = atof (*++av);
			ac--;
			}
    		    setreflim (maglim1, maglim2);
    		    break;

		case 'n':	/* Number of parameters to fit */
    		    if (ac < 2)
    			PrintUsage (str);
    		    setnfit ((int) atof (*++av));
    		    ac--;
    		    break;

    		case 'o':	/* Specifiy output image filename */
    		    if (ac < 2)
    			PrintUsage (str);
		    if (*(av+1)[0] == '-' || *(str+1) != (char)0)
			overwrite++;
		    else {
			strcpy (outname, *(av+1));
			overwrite = 0;
			av++;
			ac--;
			}
    		    writeheader++;
    		    break;

    		case 'p':  /* Initial plate scale in arcseconds per pixel */
    		    if (ac < 2)
    			PrintUsage ("* Missing arcseconds per pixel");
    		    setsecpix (atof (*++av));
    		    ac--;
		    if (ac > 1 && isnum (*(av+1))) {
			setsecpix2 (atof (*++av));
			ac--;
			}
    		    break;

    		case 'q':	/* Fit again */
    		    if (ac < 2)
    			PrintUsage ("* Missing -q option");
		    str1 = *++av;
		    ac--;
		    while ((c1 = *str1) != 0) {
    		    switch (c1) {
	
			case 'b':	/* Bin star matches for speed */
			    setbin (1);
			    break;

			case 'i':	/* Iterate fit: new area */
			    c2 = *(str1+1);
			    if ((int)c2 > 47 && (int)c2 < 58) {
				i = (int) c2 - 48;
				str1++;
				}
			    else
				i = 1;
    			    setiterate (i);
			    break;
	
			case 'n':	/* Increase number of parameters fit */
			    c2 = *(str1+1);
			    if ((int)c2 > 47 && (int)c2 < 58) {
				i = (int) c2 - 48;
				str1++;
				}
			    else
				i = 1;
    			    setnfiterate (i);
			    break;
	
			case 'r':	/* Recenter fit and rerun */
    			    setrecenter (1);
			    break;
	
			case 's':	/* Use only matches within 2 sigma */
    			    setresid_refine(1);
			    break;
	
			case 't':	/* Iterate fit: tighten up */
			    c2 = *(str1+1);
			    if ((int)c2 > 47 && (int)c2 < 58) {
				i = (int) c2 - 48;
				str1++;
				}
			    else
				i = 1;
    			    setiteratet (i);
			    break;
	
			case 'p':	/* Use polynomial WCS */
			    c2 = *(str1+1);
			    if ((int)c2 > 47 && (int)c2 < 58) {
				i = (int) c2;
				str1++;
				}
			    else
				i = 6;
    			    setfitplate (i);
			    break;
	
			case '8':	/* Fit 8 polynomial parameters */
			    setfitplate (8);
			    break;

			case 'w':	/* Do not rotate image WCS */
			    rotatewcs = 0;
			    break;
	
			default:
			    sprintf (errmsg, "* Illegal q option -%s-", str1);
			    PrintUsage (errmsg);
			    break;
			}
			str1++;
			}
    		    break;

    		case 'r':	/* Angle in degrees to rotate before fitting */
    		    if (ac < 2)
    			PrintUsage (str);
    		    rot = (int) atof (*++av);
		    setrotate (rot);
    		    ac--;
    		    break;

		case 's':   /* Fraction image stars over reference stars */
	    	    if (ac < 2)
    			PrintUsage (str);
    		    setfrac (atof (*++av));
    		    ac--;
    		    break;

    		case 't':	/* Tolerance in pixels for star match */
    		    if (ac < 2)
    			PrintUsage (str);
    		    settolerance (atof (*++av));
    		    ac--;
    		    break;

		case 'u':	/* File of prematched (x,y)/(ra,dec) */
    		    if (ac < 2)
    			PrintUsage (str);
		    matchfile = *++av;
    		    setmatch (matchfile);
    		    ac--;
    		    break;

		case 'v':	/* More verbosity */
    		    verbose++;
    		    break;

    		case 'w':	/* Update the fields in a new FITS file */
    		    writeheader++;
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

    /* If reference catalog is not set, exit with an error message */
    if (refcatname == NULL && matchfile == NULL) {
	PrintUsage ("* Must specifiy a reference catalog using -c or alias.");
	}

    if (!writeheader && !verbose) {
	PrintUsage ("* Must have either w or v argument");
	}

    /* Process image files from list file */
    if (readlist) {
	if ((flist = fopen (listfile, "r")) == NULL) {
	    sprintf (errmsg,"* List file %s cannot be read", listfile);
	    PrintUsage (errmsg);
	    }
	while (fgets (filename, 128, flist) != NULL) {
	    lastchar = filename + strlen (filename) - 1;
	    if (*lastchar < 32) *lastchar = 0;
	    FitWCS (progname, filename);
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
	    FitWCS (progname, fn[ifile]);
	    if (verbose)
		printf ("\n");
	    }
	}

    /* Print error message if no image files to process */
    else
	PrintUsage ("* No files to process.");

    return (0);
}

static void
PrintUsage (command)

char    *command;

{
    fprintf (stderr,"%s %s\n", progname, RevMsg);
    if (version)
	exit (-1);

    if (command != NULL) {
	if (command[0] == '*')
	    fprintf (stderr, "%s\n", command);
	else
	    fprintf (stderr, "* Missing argument for command %c\n", command[0]);
	exit (1);
	}

    if (strsrch (progname,"gsc") != NULL)
	fprintf (stderr,"Set WCS in FITS and IRAF image files using HST Guide Stars)\n");
    else if (strsrch (progname,"gsca") != NULL)
	fprintf (stderr,"Set WCS in FITS and IRAF image files using GSC-ACT Catalog Stars)\n");
    else if (strsrch (progname,"tmc") != NULL ||
	strsrch (progname,"2mp") != NULL)
	fprintf (stderr,"Set WCS in FITS and IRAF image files using 2MASS Point Sources)\n");
    else if (strsrch (progname,"ujc") != NULL)
	fprintf (stderr,"Set WCS in FITS and IRAF image files using USNO J Catalog stars)\n");
    else if (strsrch (progname,"uac") != NULL)
	fprintf (stderr,"Set WCS in FITS and IRAF image files using USNO A Catalog stars)\n");
    else if (strsrch (progname,"ua1") != NULL)
	fprintf (stderr,"Set WCS in FITS and IRAF image files using USNO A1.0 Catalog stars)\n");
    else if (strsrch (progname,"ua2") != NULL)
	fprintf (stderr,"Set WCS in FITS and IRAF image files using USNO A2.0 Catalog stars)\n");
    else if (strsrch (progname,"usac") != NULL)
	fprintf (stderr,"Set WCS in FITS and IRAF image files using USNO SA Catalog stars)\n");
    else if (strsrch (progname,"usa1") != NULL)
	fprintf (stderr,"Set WCS in FITS and IRAF image files using USNO SA1.0 Catalog stars)\n");
    else if (strsrch (progname,"usa2") != NULL)
	fprintf (stderr,"Set WCS in FITS and IRAF image files using USNO SA2.0 Catalog stars)\n");
    else if (strsrch (progname,"ub1") != NULL)
	fprintf (stderr,"Set WCS in FITS and IRAF image files using USNO B1.0 Catalog stars)\n");
    else if (strsrch (progname,"act") != NULL)
	fprintf (stderr,"Set WCS in FITS and IRAF image files using USNO ACT Catalog stars)\n");
    else if (strsrch (progname,"bsc") != NULL)
	fprintf (stderr,"Set WCS in FITS and IRAF image files using Bright Star Catalog stars)\n");
    else if (strsrch (progname,"iras") != NULL)
	fprintf (stderr,"Set WCS in FITS and IRAF image files using IRAS Point Sources)\n");
    else if (strsrch (progname,"sao") != NULL)
	fprintf (stderr,"Set WCS in FITS and IRAF image files using SAO Catalog stars)\n");
    else if (strsrch (progname,"ppm") != NULL)
	fprintf (stderr,"Set WCS in FITS and IRAF image files using PPM Catalog stars)\n");
    else if (strsrch (progname,"tycho") != NULL)
	fprintf (stderr,"Set WCS in FITS and IRAF image files using Tycho Catalog stars)\n");
    else
	fprintf (stderr,"Set WCS in FITS and IRAF image files (after UIowa SETWCS)\n");

    fprintf(stderr,"Usage: [-vwdfl][-o filename][-m mag][-n frac][-s mode][-g class]\n");
    fprintf(stderr,"       [-h maxref][-i peak][-c catalog][-p scale][-b ra dec][-j ra dec]\n");
    fprintf(stderr,"       [-r deg][-t tol][-u matchfile][-x x y][-y frac] FITS or IRAF file(s)\n");
    fprintf(stderr,"  -a: initial rotation angle in degrees (default 0)\n");
    fprintf(stderr,"  -b: initial center in B1950 (FK4) RA and Dec\n");
    fprintf(stderr,"  -c: reference catalog (gsc, uac, usac, ujc, tab table file\n");
    fprintf(stderr,"  -d: Use following DAOFIND output catalog instead of search\n");
    fprintf(stderr,"  -e: Erase image WCS keywords\n");
    fprintf(stderr,"  -f: write FITS output no matter what input\n");
    fprintf(stderr,"  -g: Guide Star Catalog class (-1=all,0,3 (default -1)\n");
    fprintf(stderr,"  -h: maximum number of reference stars to use (10-200, default %d\n", MAXSTARS);
    fprintf(stderr,"  -i: minimum peak value for star in image (<0=-sigma)\n");
    fprintf(stderr,"  -j: initial center in J2000 (FK5) RA and Dec\n");
    fprintf(stderr,"  -k: magnitude to use (1 to nmag)\n");
    fprintf(stderr,"  -l: reflect left<->right before rotating and fitting\n");
    fprintf(stderr,"  -m: reference catalog magnitude limit(s) (default none)\n");
    fprintf(stderr,"  -n: list of parameters to fit (12345678; negate for refinement)\n");
    fprintf(stderr,"  -o: name for output image, no argument to overwrite\n");
    fprintf(stderr,"  -p: initial plate scale in arcsec per pixel (default 0)\n");
    fprintf(stderr,"  -q: <i>terate, <r>ecenter, <s>igma clip, <p>olynomial, <t>olerance reduce, <w>do not rotate WCS, <n>more params\n");
    fprintf(stderr,"  -r: rotation angle in degrees before fitting (default 0)\n");
    fprintf(stderr,"  -s: use this fraction extra stars (default 1.0)\n");
    fprintf(stderr,"  -t: offset tolerance in pixels (default %d)\n", PIXDIFF);
    fprintf(stderr,"  -u: File of X Y RA Dec assignments for initial WCS\n");
    fprintf(stderr,"  -v: verbose\n");
    fprintf(stderr,"  -w: write header (default is read-only)\n");
    fprintf(stderr,"  -x: X and Y coordinates of reference pixel (default is center)\n");
    fprintf(stderr,"  -y: add this fraction to image dimensions for search (default is 0)\n");
    fprintf(stderr,"  -z: use AIPS classic projections instead of WCSLIB\n");
    exit (1);
}


static void
FitWCS (progname, name)

char	*progname;	/* Name of program being executed */
char	*name;		/* FITS or IRAF image filename */

{
    int lhead;			/* Maximum number of bytes in FITS header */
    int nbhead;			/* Actual number of bytes in FITS header */
    int iraffile;		/* 1 if IRAF image */
    int bpix = 0;
    char *image;		/* Image */
    char *header;		/* FITS header */
    char *irafheader = NULL;	/* IRAF image header */
    char newname[256];		/* Name for revised image */
    char pixname[256];		/* Pixel file name for revised image */
    char temp[16];
    char *ext;
    char *fname;
    int lext, lname;
    int rename = 0;
    char *imext, *imext1;
    char *newimage;

    image = NULL;

    /* Open IRAF image if .imh extension is present */
    if (isiraf (name)) {
	iraffile = 1;
	if ((irafheader = irafrhead (name, &lhead)) != NULL) {
	    header = iraf2fits (name, irafheader, lhead, &nbhead);
	    if (header == NULL) {
		fprintf (stderr, "Cannot translate IRAF header %s/n",name);
		free (irafheader);
		return;
		}
	    if (imsearch || writeheader || rot || mirror) {
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

    /* Open FITS file if .imh extension is not present */
    else {
	iraffile = 0;
	fitsout = 1;
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
	fprintf (stderr,"Set World Coordinate System in ");
	if (iraffile)
	    fprintf (stderr,"IRAF image file %s\n", name);
	else
	    fprintf (stderr,"FITS image file %s\n", name);
	}

    /* Print existing WCS keywords and optionally erase them */
    if (verbose)
	(void) PrintWCS (header, verbose);
    if (erasewcs) {
	if (strchr (name, ',') || strchr (name,'['))
	    setheadshrink (0);
	(void) DelWCSFITS (header, verbose);
	}

    /* Rotate and/or reflect image */
    if ((imsearch || writeheader) && (rot != 0 || mirror)) {
	if ((newimage = RotFITS (name,header,image,0,0,rot,mirror,bitpix,
				 rotatewcs,verbose))
	    == NULL) {
	    fprintf (stderr,"Image %s could not be rotated\n", name);
	    if (iraffile)
		free (irafheader);
	    if (image != NULL)
		free (image);
	    free (header);
	    return;
	    }
	else {
	    if (image != NULL)
		free (image);
	    image = newimage;
	    }

	if (!overwrite)
	    rename = 1;
	}

    /* Check for permission to overwrite */
    else if (overwrite)
	rename = 0;
    else
	rename = 1;

    /* Use output filename if it is set on the command line */
    if (outname[0] > 0)
	strcpy (newname, outname);

    /* Make up name for new FITS or IRAF output file */
    else if (rename) {

    /* Remove directory path and extension from file name */
	ext = strrchr (name, '.');
	fname = strrchr (name, '/');
	if (fname)
	    fname = fname + 1;
	else
	    fname = name;
	lname = strlen (fname);
	if (ext) {
	    lext = strlen (ext);
	    strncpy (newname, fname, lname - lext);
	    *(newname + lname - lext) = 0;
	    }
	else
	    strcpy (newname, fname);

    /* Add image extension number or name to output file name */
	imext = strchr (fname, ',');
	imext1 = NULL;
	if (imext == NULL) {
	    imext = strchr (fname, '[');
	    if (imext != NULL) {
		imext1 = strchr (fname, ']');
		*imext1 = (char) 0;
		}
	    }
	if (imext != NULL) {
	    strcat (newname, "_");
	    strcat (newname, imext+1);
	    }

    /* Add rotation and reflection to image name */
	if (mirror)
	    strcat (newname, "m");
	else if (rot != 0)
	    strcat (newname, "r");
	if (rot < 10 && rot > -1)
	    sprintf (temp,"%1d",rot);
	else if (rot < 100 && rot > -10)
	    sprintf (temp,"%2d",rot);
	else if (rot < 1000 && rot > -100)
	    sprintf (temp,"%3d",rot);
	else
	    sprintf (temp,"%4d",rot);
	if (rot != 0)
	    strcat (newname, temp);

    /* Add file extension preceded by a w */
	if (fitsout)
	    strcat (newname, "w.fits");
	else {
	    strcpy (pixname, "HDR$");
	    strcat (pixname, newname);
	    strcat (pixname, "w.pix");
	    hputm (header, "PIXFIL", pixname);
	    strcat (newname, "w.imh");
	    }
	}
    else
	strcpy (newname, name);

    if (SetWCSFITS (name, header, image, refcatname, verbose)) {
	if (writeheader) {
	    if (verbose)
		(void) PrintWCS (header, verbose);	/* print new WCS */

	/* Log WCS program version in the image header */
	    hputs (header,"IMWCS",RevMsg);
	    hgeti4 (header, "BITPIX", &bpix);

	    if (fitsout) {
		if (bpix == 0) {
		    if (fitswhead (newname, header) > 0 && verbose) {
			if (overwrite)
			    printf ("%s: rewritten successfully.\n", newname);
			else
			    printf ("%s: written successfully.\n", newname);
			}
		    else if (verbose)
			printf ("%s could not be written.\n", newname);
		    }
		else if (image == NULL) {
		    if (fitscimage (newname, header, name) > 0 && verbose) {
			if (overwrite)
			    printf ("%s: rewritten successfully.\n", newname);
			else
			    printf ("%s: written successfully.\n", newname);
			}
		    else if (verbose)
			printf ("%s could not be written.\n", newname);
		    }
		else {
		    if (fitswimage (newname, header, image) > 0 && verbose) {
			if (overwrite)
			    printf ("%s: rewritten successfully.\n", newname);
			else
			    printf ("%s: written successfully.\n", newname);
			}
		    else if (verbose)
			printf ("%s could not be written.\n", newname);
		    }
		}
	    else if (rename) {
		if (irafwimage (newname,lhead,irafheader,header,image) > 0 && verbose) {
		    if (overwrite)
			printf ("%s: rewritten successfully.\n", newname);
		    else
			printf ("%s: written successfully.\n", newname);
		    }
		else if (verbose)
		    printf ("%s could not be written.\n", newname);
		}
	    else {
		if (irafwhead (newname,lhead,irafheader,header) > 0 && verbose) {
		    if (overwrite)
			printf ("%s: rewritten successfully.\n", newname);
		    else
			printf ("%s: written successfully.\n", newname);
		    }
		else if (verbose)
		    printf ("%s could not be written.\n", newname);
		}
	    }
	else if (verbose)
	    printf ("%s: file unchanged.\n", name);
	}
    else if (verbose)
	printf ("%s: file unchanged.\n", name);

    free (header);
    if (iraffile)
	free (irafheader);
    if (image != NULL)
	free (image);
    return;
}

/* Feb 16 1996	New program
 * Apr 15 1996	Move delWCSFITS to libwcs
 * Apr 24 1996	Add optional initial plate center on command line
 * May  2 1996	Add option to rotate and/or reflect before fitting
 * May 14 1996	Change GSCLIM to REFLIM
 * May 22 1996	Rearrange commands; set initial plate center explicitly
 * May 31 1996	Rename subroutines; drop WCS deletion as an option
 * Jun  4 1996	Allow writing of FITS file even if input is IRAF
 * Jun 14 1996	Write IMWCS record in header
 * Jun 28 1996	Add option to set number of parameters to fit
 * Jul  3 1996	Always write to new file unless -o
 * Jul 22 1996	Add option to change WCS projection
 * Aug  6 1996	Force number of decimal places in PrintWCS
 * Aug 26 1996	Change HGETC call to HGETS; fix irafrhead call
 * Aug 28 1996	Declare undeclared variables after lint
 * Aug 29 1996	Allow writing of new IRAF files
 * Sep  1 1996	Move parameter defaults to lwcs.h
 * Sep  3 1996	Fix star finding
 * Sep 17 1996	Fix bug in GSC reading
 * Oct 11 1996	Fix DelWCS declaration and do not free strings in PrintWCS
 * Oct 17 1996	Fix bugs which Sun C ignored
 * Nov 19 1996	Revised search subroutines, USNO A catalog added
 * Dec 10 1996	Revised WCS initialization
 * Dec 10 1996	Add option to get image stars from DAOFIND output list
 *
 * Feb 21 1997  Check pointers against NULL explicitly for Linux
 * Mar 20 1997	Fix bug in GetFITSWCS which affected odd equinoxes
 * Apr 25 1997	Fix bug in uacread
 * May 28 1997  Add option to read a list of filenames from a file
 * Jul 12 1997	Add option to center reference pixel ccords on the command line
 * Aug 20 1997	Add option to set maximum number of reference stars to try to match
 * Sep  3 1996	Add option to change dimensions of search by fraction
 * Sep  9 1997	Add option to turn on residual refinement by negating nfit
 * Sep  9 1997	Do not read image unless it is needed
 * Oct 31 1997	Specify parameters to fit with numeric string
 * Nov  6 1997	Move PrintWCS to library
 * Nov  7 1997	Specify output image filename
 * Nov 14 1997	Change image increase from multiple to fraction added
 * Nov 17 1997	Add optional second magnitude limit
 * Dec  8 1997	Fixed bug in setting nominal WCS
 * Dec 15 1997	Add capability of reading and writing IRAF 2.11 images
 *
 * Jan 27 1998  Implement Mark Calabretta's WCSLIB
 * Jan 29 1998  Add -z for AIPS classic WCS projections
 * Feb 18 1998	Version 2.0: Calabretta WCS
 * Feb 20 1998	Add -q to fit residuals
 * Mar  1 1998	Add optional second axis plate scale argument to -p
 * Mar  3 1998	Add option to use first WCS result to try again
 * Mar  6 1998	Add option to recenter on second pass
 * Mar  6 1998	Change default FITS extension from .fit to .fits
 * Mar  6 1998	Add option to set projection type
 * Mar 27 1998	Version 2.2: Drop residual fitting; add polynomial fit
 * Apr 14 1998	New coordinate conversion software; polynomial debugged
 * Apr 24 1998	change coordinate setting to setsys() from setfk4()
 * Apr 27 1998	Add image extension name/number to output file name
 * Apr 28 1998	Change coordinate system flags to WCS_*
 * May  4 1998	Make erasure of original image WCS optional
 * May 28 1998	Include fitsio.h instead of fitshead.h
 * Jun  2 1998	Fix bugs in hput() and tabread()
 * Jun 11 1998	Change setwcstype() to setwcsproj() to avoid conflict
 * Jun 25 1998	Set WCS subroutine choice with SETDEFWCS()
 * Jul 24 1998	Make irafheader char instead of int
 * Aug  6 1998	Change fitsio.h to fitsfile.h
 * Aug 20 1998	Delete WCS if -e set no matter what PrintWCS returns
 * Oct 13 1998	Use isiraf() to figure out what kind of file is being read
 * Oct 27 1998	Add reference catalog name as SetWCSFITS argument
 * Nov 30 1998	Add version and help commands for consistency
 *
 * Feb 11 1999	Add option to use linked program name to select catalog
 * Apr 13 1999	Fix progname to drop / when full pathname
 * Apr 21 1999	Make HST GSC the reference catalog if none on command line
 * Jun  8 1999	Return image pointer from RotFITS, not flag
 * Jun 10 1999	If -a argument is multiple of 90, rotate image
 * Jun 11 1999	Fix -o argument so overwrite doesn't need a standalone -
 * Jul  7 1999	Drop out if null filename
 * Jul 15 1999	Add Bright Star Catalog for really wide fields
 * Jul 21 1999	Update online help
 * Jul 26 1999	Update online help
 * Sep 14 1999	Add option to start with file of matched stars
 * Oct 22 1999	Drop unused variables after lint
 * Nov 23 1999	If not using image pixels, copy them using fitscimage()
 *
 * Jan 28 2000	Call setdefwcs() with WCS_ALT instead of 1
 * Feb 15 2000	Allow number following i or p in -q option
 * Mar  8 2000	Move catalog selection from executable name to subroutine
 * Mar  8 2000	Rewrite q option handling
 * Mar 23 2000	Use hgetm() to get the IRAF pixel file name, not hgets()
 * Mar 28 2000	Fix bug setting number of iterations
 * Dec  6 2000	Set refcatname to default gsc only if matchfile is not set
 * Dec 11 2000	Fix last argument test
 *
 * Jan 30 2001	Fix -o option to overwrite an image
 * May 21 2001	Add GSC-ACT and 2MASS Point Source Catalogs
 * Oct 16 2001	Add command line parameters setting using keyword=value
 * Oct 24 2001	Improve error handling
 * Oct 25 2001	Allow arbitrary argument order on command line
 * Oct 31 2001	Print complete help message if no arguments
 * Dec 17 2001	Set mirror and rotation in FindStars()
 *
 * Apr 10 2002	Accept letter as well as number for magnitude
 * Jul 31 2002	Add iteration with more parameters fit
 *
 * Jan 23 2003	Add USNO-B1.0 Catalog
 * Apr 13 2003	Set revision message for subroutines using setrevmsg()
 *
 * Jul  1 2004	If working on FITS extension, keep blank lines in header
 * Sep 15 2004	Add missing 0 shift arguments to RotFITS() call (Rob Creager)
 *
 * Apr  4 2005	If not catalog is specified, print an error message and quit
 * Jun 21 2006	Clean up code
 * Sep 26 2006	Increase length of rastr and destr from 16 to 32
 * Oct 12 2006	Add b option to q command to bin star matches for speed
 *
 * Jan 10 2007	Call setgsclass() instead of setclass()
 * Apr  6 2007	Add -q w to not rotate initial image WCS
 */
