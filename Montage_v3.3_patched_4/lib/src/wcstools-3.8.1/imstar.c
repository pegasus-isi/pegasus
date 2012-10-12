/* File imstar.c
 * October 15, 2007
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
#include <math.h>
#include <unistd.h>
#include <math.h>
#include "libwcs/wcs.h"
#include "libwcs/fitsfile.h"
#include "libwcs/wcscat.h"

#define CAT_DEFAULT	0
#define CAT_ASCII	1
#define CAT_STARBASE	2
#define CAT_DAOFIND	3

#define MAXFILES 1000
static int maxnfile = MAXFILES;

static char *RevMsg = "IMSTAR WCSTools 3.8.1, 14 December 2009, Doug Mink (dmink@cfa.harvard.edu)";

static int verbose = 0;		/* verbose flag */
static int debug = 0;		/* debugging flag */
static int version = 0;		/* If 1, print only program name and version */
static int rot = 0;		/* Angle to rotate image (multiple of 90 deg) */
static int mirror = 0;		/* If 1, flip image right-left before rotating*/
static int printcounts = 0;	/* If 1, print counts instead of magnitudes */

static void PrintUsage();
static void ListStars();
extern char *RotFITS();
extern void setstarsig();
extern void setbmin();
extern void setmaxrad();
extern void setborder();
extern void setimcat();
extern void setparm();
extern void setrot();
extern void setcenter();
extern void setsys();
extern void setsecpix();
extern void setsecpix2();
extern void setrefpix();
extern void setmirror();
extern void setrotate();

extern struct WorldCoor *GetFITSWCS();

static int wfile = 0;		/* True to print output file */
static double magoff = 90.0;
static int rasort = 0;
static int printhead = 0;
static int nstar = 0;
static double eqout = 0.0;
static int sysout = -1;
static int outform = 0;		/* Output catalog format */
static int setuns = 0;		/* Change to unsigned integer flag */
static int imsearch = 1;	/* If 1, search for stars in image */
static int region_char;
static int region_radius;
static int rotatewcs = 1;	/* If 1, rotate FITS WCS keywords in image */

int
main (ac, av)
int ac;
char **av;
{
    char *str;
    double bmin, arot, drot, xr, yr;
    char rastr[32], decstr[32];
    int readlist = 0;
    char *lastchar;
    char *cstr;
    char filename[128];
    char errmsg[256];
    FILE *flist;
    char *listfile = NULL;
    int maxrad;
    char **fn;
    int ifile, nfile;
    char c;

    nfile = 0;
    fn = (char **)calloc (maxnfile, sizeof(char *));

    /* crack arguments */
    for (av++; --ac > 0; av++) {
   	str = *av; 

	/* Check for help command first */
	if (!str || !strcmp (str, "help") || !strcmp (str, "-help"))
	    PrintUsage (NULL);

	/* Check for version command */
	if (!strcmp (str, "version") || !strcmp (str, "-version")) {
	    version = 1;
	    PrintUsage (NULL);
	    }

	else if (*str == '@') {
	    readlist++;
	    listfile = ++str;
	    str = str + strlen (str) - 1;
	    av++;
	    ac--;
	    }

	else if (strchr (str, '='))
	    setparm (str);

        /* Set RA, Dec, and equinox if WCS-generated argument */
	else if (strsrch (str,":") != NULL) {
	    if (ac < 3)
		PrintUsage (str);
	    else {
		strcpy (rastr, *av);
		ac--;
		strcpy (decstr, *++av);
		ac--;
		setcenter (rastr, decstr);
		setsys (*++av);
		if (sysout < 0) {
		    sysout = wcscsys (*av);
		    eqout = wcsceq (*av);
		    }
		}
	    }

	else if (*str == '-') {
	    while ((c = *++str)) {
		switch (c) {
		case 'v':	/* more verbosity */
		    verbose++;
		    break;
	
		case 'a':       /* Initial rotation angle in degrees */
		    if (ac < 2)
			PrintUsage (str);
		    drot = atof (*++av);
		    arot = fabs (drot);
		    if (arot != 90.0 && arot != 180.0 && arot != 270.0) {
			setrot (drot);
			setrotate (rot);
			rot = 0;
			}
		    else
			rot = atoi (*av);
		    ac--;
		    break;
	
		case 'b':	/* Output FK4 (B1950) coordinates */
		    eqout = 1950.0;
		    sysout = WCS_B1950;
		    break;
	
		case 'c':	/* Output coounts instead of magnitudes */
		    printcounts++;;
		    break;
	
		case 'd':	/* Read image star positions from DAOFIND file */
		    if (ac < 2)
			PrintUsage (str);
		    setimcat (*++av);
		    imsearch = 0;
		    ac--;
		    break;
	
		case 'e':	/* Number of pixels to ignore around image edge */
		    if (ac < 2)
			PrintUsage (str);
		    setborder (atoi (*++av));
		    ac--;
		    break;
	
		case 'f':	/* Write ASCII catalog format for SKYMAP */
		    outform = CAT_ASCII;
		    break;
	
		case 'g':	/* Do not rotate image WCS with image */
		    rotatewcs = 0;
		    break;
	
		case 'h':	/* Output descriptive header */
		    printhead++;
		    break;
	
		case 'i':	/* Image star minimum peak value (or minimum sigma */
		    if (ac < 2)
			PrintUsage (str);
		    bmin = atof (*++av);
		    if (bmin < 0)
			setstarsig (-bmin);
		    else
			setbmin (bmin);
		    ac--;
		    break;
	
		case 'j':	/* Output FK5 (J2000) coordinates */
		    eqout = 2000.0;
		    sysout = WCS_J2000;
		    break;
	
		case 'k':	/* Print each star as it is found */
		    debug++;
		    break;
	
    		case 'l':	/* Left-right reflection before rotating */
		    mirror = 1;
		    setmirror (mirror);
    		    break;
	
		case 'm':	/* Magnitude offset */
		    if (ac < 2)
			PrintUsage (str);
		    magoff = atof (*++av);
		    ac--;
		    break;
	
		case 'n':	/* Number of brightest stars to read */
		    if (ac < 2)
			PrintUsage (str);
		    nstar = atoi (*++av);
		    ac--;
		    break;

		case 'o':	/* Output DAOFIND format */
		    outform = CAT_DAOFIND;
		    break;
	
    		case 'p':	/* Plate scale in arcseconds per pixel */
    		    if (ac < 2)
			PrintUsage (str);
    		    setsecpix (atof (*++av));
    		    ac--;
		    if (ac > 1 && isnum (*(av+1))) {
			setsecpix2 (atof (*++av));
			ac--;
			}
    		    break;
	
		case 'q':	/* Output region file shape for SAOimage */
    		    if (ac < 2)
			PrintUsage (str);
		    cstr = *++av;
		    switch (cstr[0]){
			case 'c':
			    if (cstr[1] == 'i')
				region_char = WCS_CIRCLE;
			    else
				region_char = WCS_CROSS;
			    break;
			case 'd':
			    region_char = WCS_DIAMOND;
			    break;
			case 's':
			    region_char = WCS_SQUARE;
			    break;
			case 'x':
			    region_char = WCS_EX;
			    break;
			case 'v':
			    region_char = WCS_VAR;
			    break;
			case '+':
			    region_char = WCS_CROSS;
			    break;
			case 'o':
			default:
			    region_char = WCS_CIRCLE;
			}
		    if (region_radius == 0)
			region_radius = 10;
    		    ac--;
		    wfile = 1;
		    break;
	
		case 'r':	/* Maximum acceptable radius for a star */
		    if (ac < 2)
			PrintUsage (str);
		    maxrad = (int) atof (*++av);
		    region_radius = maxrad;
		    setmaxrad (maxrad);
		    ac--;
		    break;
	
		case 's':	/* sort by RA */
		    rasort = 1;
		    break;
	
		case 't':	/* Output Starbase tab table */
		    outform = CAT_STARBASE;
		    break;
	
		case 'u':	/* Set 16-bit int image file to unsigned */
		    setuns = 1;
		    break;
	
		case 'w':	/* Write output to file */
		    wfile = 1;
		    break;

		case 'x':	/* X and Y coordinates of reference pixel */
		    if (ac < 3)
			PrintUsage (str);
    		    xr = atof (*++av);
		    ac--;
    		    yr = atof (*++av);
		    ac--;
    		    setrefpix (xr, yr);
    		    break;

		case 'z':       /* Use AIPS classic WCS */
		    setdefwcs (1);
		    break;

		default:
		    sprintf (errmsg, "* Illegal command -%s-", str);
		    PrintUsage (str);
		}
		}
	    }

	/* Save a FITS or IRAF file */
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

    /* Find number of images to search and leave listfile open for reading */
    if (readlist) {
	if ((flist = fopen (listfile, "r")) == NULL) {
	    sprintf (errmsg,"IMSTAR: List file %s cannot be read\n",
		     listfile);
	    PrintUsage (errmsg);
	    }
	while (fgets (filename, 128, flist) != NULL) {
	    lastchar = filename + strlen (filename) - 1;
	    if (*lastchar < 32) *lastchar = 0;
	    ListStars (filename);
	    }
	fclose (flist);
	}

    /* Process image files */
    else if (nfile > 0) {
        for (ifile = 0; ifile < nfile; ifile++) {
            if (verbose)
                printf ("%s:\n", fn[ifile]);
            ListStars (fn[ifile]);
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

char	*command;	/* Name of program being executed */

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

    fprintf (stderr,"Find stars in FITS and IRAF image files\n");
    fprintf(stderr,"usage: imstar [-vbsjt] [-m mag_off] [-n num] [-d file][ra dec sys] file.fits ...\n");
    fprintf(stderr,"  -a: Rotation angle in degrees (default 0)\n");
    fprintf(stderr,"  -b: Output B1950 (FK4) coordinates \n");
    fprintf(stderr,"  -c: Output total flux in counts instead of magnitudes\n");
    fprintf(stderr,"  -d: Read following DAOFIND output catalog instead of search \n");
    fprintf(stderr,"  -e: Number of pixels to ignore around image edge \n");
    fprintf(stderr,"  -f: Output simple ASCII catalog format\n");
    fprintf(stderr,"  -g: Do not rotate image WCS with image\n");
    fprintf(stderr,"  -h: Print heading, else do not \n");
    fprintf(stderr,"  -i: Minimum peak value for star in image (<0=-sigma)\n");
    fprintf(stderr,"  -j: Output J2000 (FK5) coordinates \n");
    fprintf(stderr,"  -k: Print each star as it is found for debugging \n");
    fprintf(stderr,"  -l: reflect left<->right before rotating and searching\n");
    fprintf(stderr,"  -m: Magnitude offset (set brightest to abs(offset) if < 0)\n");
    fprintf(stderr,"  -n: Number of brightest stars to print \n");
    fprintf(stderr,"  -o: Output DAOFIND format star list\n");
    fprintf(stderr,"  -p: Plate scale in arcsec per pixel (default 0)\n");
    fprintf(stderr,"  -q: Output region file shape for SAOimage (default o)\n");
    fprintf(stderr,"  -r: Maximum radius for star in pixels \n");
    fprintf(stderr,"  -s: Sort by RA instead of flux \n");
    fprintf(stderr,"  -t: Output Starbase tab table format star list\n");
    fprintf(stderr,"  -u: Set BITPIX to -16 for unsigned integer\n");
    fprintf(stderr,"  -v: Verbose; print star list to stdout\n");
    fprintf(stderr,"  -w: write star list to output file\n");
    fprintf(stderr,"  -x: X and Y coordinates of reference pixel (if not in header or center)\n");
    fprintf (stderr,"  -z: use AIPS classic projections instead of WCSLIB\n");
    fprintf(stderr,"  @listfile: file containing a list of filenames to search\n");
    exit (1);
}


extern int FindStars ();
extern int pix2wcst();

static void
ListStars (filename)

char	*filename;	/* FITS or IRAF file filename */

{
    char *image = NULL;		/* FITS image */
    char *newimage;		/* Rotated FITS image */
    char *header;		/* FITS header */
    int lhead;			/* Maximum number of bytes in FITS header */
    int nbhead;			/* Actual number of bytes in FITS header */
    char *irafheader = NULL;	/* IRAF image header */
    double *sx=0, *sy=0;	/* image stars, pixels */
    double *sra=0, *sdec=0;	/* image star RA and Dec */
    int ns;			/* n image stars */
    double *smag;		/* image star magnitudes */
    int *sp;			/* peak flux in counts */
    double cra,cdec,dra,ddec,secpix;
    int wp, hp;
    char rastr[32], decstr[32];
    int i, bitpix;
    char headline[160];
    char pixname[256];
    char outfile[256];
    char *ext;
    char temp[32];
    FILE *fd;
    struct WorldCoor *wcs;	/* World coordinate system structure */
    int iraffile = 0;

    /* Open IRAF header */
    if (isiraf (filename)) {
	if ((irafheader = irafrhead (filename, &lhead)) != NULL) {
	    if ((header = iraf2fits (filename,irafheader,lhead,&nbhead)) == NULL) {
		fprintf (stderr, "Cannot translate IRAF header %s/n",filename);
		free (irafheader);
		return;
		}
	    if (imsearch) {
		if ((image = irafrimage (header)) == NULL) {
		    hgetm (header,"PIXFIL", 255, pixname);
		    fprintf (stderr, "Cannot read IRAF pixel file %s\n", pixname);
		    free (irafheader);
		    free (header);
		    return;
		    }
		}
	    iraffile = 1;
	    }
	else {
	    fprintf (stderr, "Cannot read IRAF header file %s\n", filename);
	    return;
	    }
	}

    /* Read FITS image header */
    else {
	if ((header = fitsrhead (filename, &lhead, &nbhead)) != NULL) {
	    if (imsearch) {
		if ((image = fitsrimage (filename, nbhead, header)) == NULL) {
		    fprintf (stderr, "Cannot read FITS image %s\n", filename);
		    free (header);
		    return;
		    }
		}
	    }
	else {
	    fprintf (stderr, "Cannot read FITS file %s\n", filename);
	    return;
	    }
	}
    if (verbose && printhead)
	fprintf (stderr,"%s\n",RevMsg);

    /* Set image to unsigned integer if 16-bit and flag set */
    if (setuns) {
	hgeti4 (header, "BITPIX", &bitpix);
	if (bitpix == 16)
	    hputi4 (header, "BITPIX", -16);
	}

    /* Rotate and/or reflect image */
    if (imsearch && (rot != 0 || mirror)) {
	if ((newimage = RotFITS (filename,header,image,0,0,rot,mirror,bitpix,
				 rotatewcs,verbose)) == NULL) {
	    fprintf (stderr,"Image %s could not be rotated\n", filename);
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
	}

/* Find the stars in an image and use the world coordinate system
 * information in the header to produce a plate catalog with right
 * ascension, declination, and a plate magnitude
 */

    wcs = GetFITSWCS (filename, header,verbose, &cra, &cdec, &dra, &ddec,
		      &secpix, &wp, &hp, &sysout, &eqout);
    if (wcs == NULL)
	outform = CAT_DAOFIND;

    /* Discover star-like things in the image, in pixels */
    ns = FindStars (header, image, &sx, &sy, &smag, &sp, debug, 1);
    if (ns < 1) {
	fprintf (stderr,"ListStars: no stars found in image %s\n", filename);
	wcsfree (wcs);
	free (header);
	if (imsearch)
	    free (image);
	return;
	}

    if (debug) printf ("\n");

    /* Save star positions */
    if (nstar > 0 && ns > nstar)
	ns = nstar;

    /* Sort stars */
    MagSortStars (NULL,NULL,NULL,NULL,NULL,sx, sy, &smag, sp, NULL, ns, 1, 1);

    /* Reset magnitude offset, if negative, so it is value for brightest star */
    /* Default is instrument magnitude */
    if (magoff <= 0.0)
	magoff = -magoff - smag[0];
    else if (magoff > 89.0)
	magoff = 0.0;

    /* Compute right ascension and declination for all stars to be listed */
    sra = (double *) malloc (ns * sizeof (double));
    sdec = (double *) malloc (ns * sizeof (double));
    for (i = 0; i < ns; i++) {
	if (iswcs (wcs))
	    pix2wcs (wcs, sx[i], sy[i], &sra[i], &sdec[i]);
	else {
	    sra[i] = 0.0;
	    sdec[i] = 0.0;
	    }
	smag[i] = smag[i] + magoff;
	}

    /* Sort star-like objects in image by right ascension */
    if (rasort && iswcs (wcs))
	RASortStars (0, sra, sdec, NULL, NULL, sx, sy, &smag, sp, NULL, ns, 1);
    sprintf (headline, "IMAGE	%s", filename);

    /* Open plate catalog file */
    if (strcmp (filename,"stdin")) {
	if (strrchr (filename, '/'))
	    strcpy (outfile, strrchr (filename, '/')+1);
	else
	    strcpy (outfile,filename);
	if ((ext = strsrch (outfile, ".fit")) != NULL ||
	    (ext = strsrch (outfile, ".imh")) != NULL)
	    *ext = (char) 0;
	}
    else {
	strcpy (outfile,filename);
	(void) hgets (header,"OBJECT",64,outfile);
	}

    /* Add rotation and reflection to output file name */
    if (mirror)
	strcat (outfile, "m");
    else if (rot != 0)
	strcat (outfile, "r");
    if (rot != 0) {
	if (rot < 10 && rot > -1) {
	    sprintf (temp,"%1d",rot);
	    strcat (outfile, temp);
	    }
	else if (rot < 100 && rot > -10) {
	    sprintf (temp,"%2d",rot);
	    strcat (outfile, temp);
	    }
	else if (rot < 1000 && rot > -100) {
	    sprintf (temp,"%3d",rot);
	    strcat (outfile, temp);
	    }
	else {
	    sprintf (temp,"%4d",rot);
	    strcat (outfile, temp);
	    }
	}

    if (wfile) {
	if (region_char)
	    strcat (outfile, ".reg");
	else if (outform == CAT_STARBASE)
	    strcat (outfile,".tab");
	else if (outform == CAT_DAOFIND)
	    strcat (outfile,".dao");
	else
	    strcat (outfile,".imstar");
	if (verbose)
	    printf ("%s\n", outfile);
		
	fd = fopen (outfile, "w");
	if (fd == NULL) {
	    fprintf (stderr, "IMSTAR:  cannot write file %s\n", outfile);
            wfile = 0;
            }
	}
    else
	fd = NULL;

    /* Write file of positions for SAOimage regions */
    if (wfile && region_char) {
	int radius, ix, iy;
	char rstr[16];
	fprintf (fd, "# %d stars in %s\n", ns, filename);
	if (verbose)
	    printf ("# %d stars in %s\n", ns, filename);
	switch (region_char) {
	    case WCS_SQUARE:
		strcpy (rstr, "SQUARE");
		break;
	    case WCS_DIAMOND:
		strcpy (rstr, "DIAMOND");
		break;
	    case WCS_CROSS:
		strcpy (rstr, "CROSS");
		break;
	    case WCS_EX:
		strcpy (rstr, "EX");
		break;
	    case WCS_CIRCLE:
	    default:
		strcpy (rstr, "CIRCLE");
	    }
	radius = region_radius;
	for (i = 0; i < ns; i++) {
	    ix = (int)(sx[i] + 0.5);
	    iy = (int)(sy[i] + 0.5);
	    fprintf (fd, "%s(%d,%d,%d) # %s %d\n",
		     rstr, ix, iy, radius, filename, i);
	    if (verbose)
		printf ("%s(%d,%d,%d) # %s %d\n",
			rstr, ix, iy, radius, filename, i);
	    }
	printf ("%s\n", outfile);
	}
    else {

    /* Write header */
    if (outform == CAT_STARBASE) {
	if (wfile)
	    fprintf (fd,"%s\n", headline);
	else
	    printf ("%s\n", headline);
	}
    else if (outform == CAT_DAOFIND) {
	if (wfile)
	    fprintf (fd,"#%s\n", headline);
	else
	    printf ("#%s\n", headline);
	}

    if (iswcs (wcs)) {
	if (rasort && outform == CAT_STARBASE) {
	    if (wfile)
		fprintf (fd, "rasort	T\n");
	    else
		printf ("rasort	T\n");
	    }

	if (outform == CAT_STARBASE) {
	    if (wcs->sysout == WCS_B1950)
		sprintf (headline, "equinox	1950.0");
	    else
		sprintf (headline, "equinox	2000.0");
	    if (wfile)
		fprintf (fd, "%s\n", headline);
	    else
		printf ("%s\n", headline);
	    }
	else if (outform == CAT_ASCII) {
	    if (wcs->sysout == WCS_B1950)
		sprintf (headline, "%s.cat\n", filename);
	    else
		sprintf (headline, "%s.cat/j\n", filename);
	    if (wfile)
		fprintf (fd, "%s\n", headline);
	    else
		printf ("%s\n", headline);
	    }
	else if (outform == CAT_DAOFIND) {
	    if (wcs->sysout == WCS_B1950)
		sprintf (headline, "#equinox 1950.0");
	    else
		sprintf (headline, "#equinox 2000.0");
	    if (printcounts)
		strcat (headline, "(counts)");
	    if (wfile)
		fprintf (fd, "%s\n", headline);
	    else
		printf ("%s\n", headline);
	    }

	if (outform == CAT_STARBASE) {
	    sprintf (headline, "epoch	%9.4f", wcs->epoch);
	    if (wfile)
		fprintf (fd, "%s\n", headline);
	    else
		printf ("%s\n", headline);
	    sprintf (headline, "program	%s", RevMsg);
	    if (wfile)
		fprintf (fd, "%s\n", headline);
	    else
		printf ("%s\n", headline);
	    if (printcounts)
		sprintf (headline,"id 	ra      	dec     	counts	x    	y    	peak");
	    else
		sprintf (headline,"id 	ra      	dec     	mag   	x    	y    	peak");
	    if (wfile)
		fprintf (fd, "%s\n", headline);
	    else
		printf ("%s\n", headline);
	    sprintf (headline,"---	------------	------------	------	-----	-----	------");
	    if (wfile)
		fprintf (fd, "%s\n", headline);
	    else
		printf ("%s\n", headline);
	    }
	else if (outform == CAT_ASCII) {
	    sprintf (headline, "Stars extracted by %s", RevMsg);
	    if (wfile)
		fprintf (fd, "%s\n", headline);
	    else
		printf ("%s\n", headline);
	    }
	else if (outform == CAT_DAOFIND) {
	    sprintf (headline, "#Stars extracted by %s", RevMsg);
	    if (wfile)
		fprintf (fd, "%s\n", headline);
	    else
		printf ("%s\n", headline);
	    }
	}


    for (i = 0; i < ns; i++) {
	ra2str (rastr, 32, sra[i], 3);
	dec2str (decstr, 32, sdec[i], 2);
	if (printcounts)
	    smag[i] = pow (10.0, smag[i] / -2.5);
	if (outform == CAT_STARBASE) {
	    sprintf (headline, "%d	%s	%s	%.2f	%.2f	%.2f	%d",
		     i+1, rastr,decstr, smag[i], sx[i], sy[i], sp[i]);
	    if (wfile)
		fprintf (fd, "%s\n", headline);
	    else
		printf ("%s\n", headline);
	    }
	else if (outform == CAT_DAOFIND) {
	    sprintf (headline, "%7.2f %7.2f %6.2f  %d",
		    sx[i],sy[i],smag[i],sp[i]);
	    if (iswcs (wcs))
		sprintf (headline, "%s %s %s", headline, rastr, decstr);
	    if (wfile)
		fprintf (fd, "%s\n", headline);
	    else
		printf ("%s\n", headline);
	    }
	else {
	    sprintf (headline, "%3d %s %s %6.2f", i+1,rastr,decstr,smag[i]);
	    if (wcs->nxpix < 100.0 && wcs->nypix > 100.0)
		sprintf (headline, "%s  %5.2f %5.2f %d",
		headline, sx[i],sy[i], sp[i]);
	    else if (wcs->nxpix < 1000.0 && wcs->nypix < 1000.0)
		sprintf (headline, "%s  %6.2f %6.2f %d",
		headline, sx[i],sy[i], sp[i]);
	    else
		sprintf (headline, "%s  %7.2f %7.2f %d",
		headline, sx[i],sy[i], sp[i]);
	    if (wfile)
		fprintf (fd, "%s\n", headline);
	    else
		printf ("%s\n", headline);
	    }
	}
	}

    if (fd) fclose (fd);
    if (sx) free ((char *)sx);
    if (sy) free ((char *)sy);
    if (sp) free ((char *)sp);
    if (sra) free ((char *)sra);
    if (sdec) free ((char *)sdec);
    if (smag) free ((char *)smag);
    wcsfree (wcs);
    free (header);
    if (imsearch)
	free (image);
    return;
}

/* Feb 29 1996	New program
 * Apr 30 1996	Add FOCAS-style catalog matching
 * May  1 1996	Add initial image center from command line
 * May  2 1996	Set up four star matching modes
 * May 14 1996	Pass verbose flag; allow other reference catalogs
 * May 21 1996	Sort by right ascension; allow output in FK4 or FK5
 * May 29 1996	Add optional new image center coordinates
 * Jun 10 1996	Drop 3 arguments flux sorting subroutine
 * Jul 16 1996	Update input code
 * Aug 26 1996	Change HGETC call to HGETS
 * Aug 27 1996	Remove unused variables after lint
 * Aug 30 1996	Allow border to be set
 * Sep  1 1996	Move parameter defaults to lwcs.h
 * Oct 17 1996	Drop unused variables
 * Dec 10 1996	Improve hot pixel rejection
 * Dec 11 1996	Allow reading from DAOFIND file instead of searching image
 * Dec 11 1996	Add WCS default rotation and use getfitswcs
 *
 * Feb 21 1997  Check pointers against NULL explicitly for Linux
 * Mar 18 1997	Skip WCS calls if no WCS
 * May 28 1997	Add option to read a list of filenames from a file
 * Jul 12 1997  Add option to center reference pixel coords on the command line
 * Nov  7 1997	Print file in tab, DAO, or ASCII format, just like STDOUT
 * Dec 16 1997	Support IRAF 2.11 image headers
 * Dec 16 1997	Fix spacing in non-tab-table output
 *
 * Jan 27 1998  Implement Mark Calabretta's WCSLIB
 * Jan 29 1998  Add -z for AIPS classic WCS projections
 * Feb 18 1998	Version 2.0: Full Calabretta WCS
 * Mar  2 1998	Fix RA sorting bug
 * Mar 27 1998	Version 2.1: Add IRAF TNX projection
 * Mar 27 1998	Version 2.2: Add polynomial plate fit
 * Apr 27 1998	Drop directory from output file name
 * Apr 28 1998	Change coordinate system flags to WCS_*
 * May 28 1998	Include fitsio.h instead of fitshead.h
 * Jun  2 1998  Fix bug in hput()
 * Jun 15 1998	Default to tab table file; ASCII table verbosee
 * Jun 15 1998	Write DAO-format file if -w flag set; ASCII table verbose
 * Jun 17 1998	Add option to set 16-bit files to unsigned int BITPIX=-16
 * Jul 24 1998	Make irafheader char instead of int
 * Jul 27 1998	Fix bug in ra2str() and dec2str() arguments
 * Aug  6 1998	Change fitsio.h to fitsfile.h
 * Sep 17 1998	Add coordinate system to GetFITSWCS() argument list
 * Sep 29 1998	Changesystem and equinox arguments to GetFITSWCS()
 * Oct 14 1998	Use isiraf() to determine file type
 * Oct 27 1998	Add option to write region file to plot results over image
 * Nov 30 1998	Add version and help commands for consistency
 *
 * Apr  7 1999	Add filename argument to GetFITSWCS
 * May 25 1999	Add epoch to output tab table header
 * Jun  9 1999	Set brightest magnitude for any magnitude offset (J-B Marquette)
 * Jun 10 1999	Add option to rotation and reflect image before searching
 * Jun 10 1999	Drop .fits or .imh file extension from output file name
 * Jun 11 1999	Add parameter setting on command line
 * Jul  1 1999	Only free image if it was allocated
 * Jul  7 1999	Fix bug setting rotation
 * Jul  7 1999	Do not add 0 to file name if no rotation
 * Jul  7 1999	If -n argument more than found stars, list only number found
 * Sep 20 1999	Drop second call to pix2wcs
 * Oct 15 1999	Free wcs using wcsfree(); free sp
 * Oct 22 1999	Drop unused variables after lint
 * Oct 22 1999	Add optional second plate scale arg
 * Oct 26 1999	Read reference pixel coordinate from command line without -c
 * Nov 19 1999	Make display and file output formats identical
 *
 * Jan 28 2000	Call setdefwcs() with WCS_ALT instead of 1
 * Mar 15 2000	Add NULL proper motion arguments to RASortStars()
 * Mar 23 2000	Use hgetm() to get the IRAF pixel file name, not hgets()
 *
 * Jan 22 2001	Drop declaration of wcsinit()
 * Jul 25 2001	FindStar() now returns magnitude instead of flux
 * Jul 25 2001	Print to stdout unless writing to a file
 * Oct 25 2001	Allow arbitrary argument order on command line
 * Dec 17 2001	Set mirror and rotation in FindStars()
 *
 * Jan 22 2002	Set edge pixels as integer, not float
 * Jan 23 2002	Add zap=1 to FindStars() call
 *
 * Sep 15 2004	Add missing 0 shift arguments to RotFITS() call (Rob Creager)
 *
 * Jun 21 2006	Clean up code
 *
 * Jan 10 2007	Fix arguments to MagSortStars() and RASortStars()
 * Jan 10 2007	Declare RevMsg static, not const
 * Apr  6 2007	Add -g command to not rotate image WCS with image
 * Apr 27 2007	Set magoff to zero only if greater than 89, not 20
 * Oct 15 2007	Add -c option to print total flux in counts
 */
