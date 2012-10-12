/* File newfits.c
 * May 10, 2006
 * By Doug Mink, Harvard-Smithsonian Center for Astrophysics
 * Send bug reports to dmink@cfa.harvard.edu

   Copyright (C) 2006 
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

/* Write a FITS header without any data or a blank FITS image.
 * Add information using edhead or sethead */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>
#include <math.h>

#include "libwcs/fitsfile.h"
#include "libwcs/wcs.h"

static void usage();
static void MakeFITS ();
extern void setcenter();
extern void setsys();
extern void setrot();
extern void setsecpix();
extern void setsecpix2();
extern void setrefpix();
extern void setcdelt();
extern void setproj();
extern struct WorldCoor *GetFITSWCS();

static char *RevMsg = "NEWFITS WCSTools 3.8.1, 14 December 2009, Doug Mink (dmink@cfa.harvard.edu)";
static int verbose = 0;	/* verbose/debugging flag */
static int bitpix = 0;	/* number of bits per pixel (FITS code, 0=no image) */
static int version = 0;	/* If 1, print only program name and version */
static int wcshead = 0;	/* If 1, add WCS information from command line */
static int nx = 0;	/* width of image in pixels */
static int ny = 0;	/* height of image in pixels */
static int extend = 0;	/* If 1, write primary header, add other files as ext */
static char *pixfile;	/* Pixel file name */
static char *wcsfile;	/* FITS WCS file name */
static char *newname;	/* FITS extension file name */

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
    char *listfile = NULL;
    char rastr[32], decstr[32];
    double x, y;

    pixfile = NULL;
    newname = NULL;

    /* Check for help or version command first */
    str = *(av+1);
    if (!str || !strcmp (str, "help") || !strcmp (str, "-help"))
	usage();
    if (!strcmp (str, "version") || !strcmp (str, "-version")) {
	version = 1;
	usage();
	}

    /* crack arguments */
    for (av++; --ac > 0; av++) {

	/* Set RA, Dec, and equinox if WCS-generated argument */
	if (strsrch (*av,":") != NULL) {
	    if (ac < 3)
		usage();
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
		usage();
	    else {
		strcpy (rastr, *++av);
		ac--;
		strcpy (decstr, *++av);
		setcenter (rastr, decstr);
		ac--;
		setsys (wcscsys (*++av));
		}
	    }

	/* Read list of files to create from file */
	else if (*(str = *av) == '@') {
	    readlist = 1;
	    listfile = *av + 1;
	    }

	/* Otherwise, read command */
	else if (*(str = *av) == '-') {
	    char c;
	    while ((c = *++str))
	    switch (c) {

    		case 'a':	/* Initial rotation angle in degrees */
    		    if (ac < 2)
    			usage();
    		    setrot (atof (*++av));
    		    ac--;
		    wcshead++;
    		    break;
	
    		case 'b':	/* Reference pixel coordinates in B1950 */
    		    if (ac < 3)
    			usage();
		    setsys (WCS_B1950);
		    strcpy (rastr, *++av);
		    ac--;
		    strcpy (decstr, *++av);
		    ac--;
		    setcenter (rastr, decstr);
		    wcshead++;
    		    break;

    		case 'd':	/* Set CDELTn, CROTAn instead of CD matrix */
		    setcdelt();
    		    break;

    		case 'e':	/* Make an extended FITS file */
		    extend = 1;
		    break;
	
    		case 'g':	/* Reference pixel coordinates in Galactic */
    		    if (ac < 3)
    			usage();
		    setsys (WCS_GALACTIC);
		    strcpy (rastr, *++av);
		    ac--;
		    strcpy (decstr, *++av);
		    ac--;
		    setcenter (rastr, decstr);
		    wcshead++;
    		    break;

    		case 'i':	/* Input pixel file */
    		    if (ac < 2)
    			usage();
		    pixfile = *++av;
		    ac--;
		    break;
	
    		case 'j':	/* Reference pixel coordinates in J2000 */
    		    if (ac < 3)
    			usage();
		    setsys (WCS_J2000);
		    strcpy (rastr, *++av);
		    ac--;
		    strcpy (decstr, *++av);
		    ac--;
		    setcenter (rastr, decstr);
		    wcshead++;
    		    break;
	
    		case 'o':	/* Number of bits per pixel */
    		    if (ac < 2)
    			usage ();
    		    bitpix = (int) atof (*++av);
    		    ac--;
    		    break;
	
    		case 'p':	/* Initial plate scale in arcseconds / pixel */
    		    if (ac < 2)
    			usage();
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
			usage();
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
    		    verbose++;
    		    break;

    		case 'w':	/* Input FITS WCS file */
    		    if (ac < 2)
    			usage();
		    wcsfile = *++av;
		    ac--;
		    wcshead++;
		    break;
	
		case 'x':	/* Reference pixel X and Y coordinates */
		    if (ac < 3)
			usage();
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
    		    usage();
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
usage ()
{
    fprintf (stderr,"%s\n",RevMsg);
    if (version)
	exit (-1);
    fprintf (stderr,"Make dataless FITS image header files\n");
    fprintf(stderr,"Usage: [-v][-a degrees][-p scale][-b ra dec][-j ra dec][-s nx ny][-i file]\n");
    fprintf(stderr,"       [-x x y] file.fits ...\n");
    fprintf(stderr,"  -a num: initial rotation angle in degrees (default 0)\n");
    fprintf(stderr,"  -b ra dec: initial center in B1950 (FK4) RA and Dec\n");
    fprintf(stderr,"  -d: set CDELTn, CROTAn instead of CD matrix\n");
    fprintf(stderr,"  -g lon lat: initial center in Galactic longitude and latitude\n");
    fprintf(stderr,"  -i file : read image from a binary file\n");
    fprintf(stderr,"  -j ra dec: initial center in J2000 (FK5) RA and Dec\n");
    fprintf(stderr,"  -o num: output pixel size in bits (FITS code, default=0)\n");
    fprintf(stderr,"  -p arcsec: initial plate scale in arcsec per pixel (default 0)\n");
    fprintf(stderr,"  -s num num: size of image in x and y pixels (default 100x100)\n");
    fprintf(stderr,"  -t proj: set FITS CTYPE projection (default TAN)\n");
    fprintf(stderr,"  -v: verbose\n");
    fprintf(stderr,"  -w file: Read FITS WCS from this file\n");
    fprintf(stderr,"  -x num num: X and Y coordinates of reference pixel (default is center)\n");
    exit (1);
}

static void
MakeFITS (name)
char *name;
{
    char *image = NULL;	/* FITS image */
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
    int nbskip;		/* Number of bytes to skip in file before image */
    int nbfile;		/* Number of bytes in file */
    int nbread;		/* Number of bytes actually read from file */
    int nbhead;
    struct WorldCoor *wcs;
    FILE *diskfile;
    char *cplus;
    char history[256];
    char extname[16];

    if (verbose) {
	fprintf (stderr,"%s\n",RevMsg);
	if (bitpix != 0)
	    fprintf (stderr,"Create ");
	else
	    fprintf (stderr,"Create header as ");
	fprintf (stderr,"FITS file %s\n", name);
	}

    /* Make sure that no existing file is overwritten */
    if ((diskfile = fopen (name, "r")) != NULL) {
	fprintf (stderr,"NEWFITS: FITS file %s exists, no new file written\n",
		     name);
	fclose (diskfile);
	return;
	}

    /* Write primary header for FITS extension file */
    if (extend == 1) {
	lhead = 2880;
	header = (char *) calloc (1, lhead);
	strcpy (header, "END ");
	hlength (header, 2880);
	hputl (header, "SIMPLE", 1);
	hputi4 (header, "BITPIX", 16);
	hputi4 (header, "NAXIS", 0);
	hputl (header, "EXTEND", 1);
	image = NULL;
	hputc (header, "COMMENT", "FITS (Flexible Image Transport System) format is defined in 'Astronomy");
	hputc (header, "COMMENT", "and Astrophysics' vol 376, page 359; bibcode: 2001A&A...376..359H.");
	if (fitswimage (name, header, image) == 0)
	    printf ("%s: dataless FITS image header not written.\n", name);
	else {
	    if (verbose)
		printf ("%s: dataless FITS image header written.\n", name);
	    newname = name;
	    }
	extend++;
	return;
	}
    else if (extend > 1) {
	if (newname == NULL)
	    return;
	diskfile = fopen (newname, "a");
	if ((header = fitsrhead (name, &lhead, &nbhead)) != NULL) {
	    if ((image = fitsrimage (name, nbhead, header)) == NULL) {
		fprintf (stderr, "Cannot read FITS image %s\n", name);
		free (header);
		return;
		}
	    }
	else {
	    fprintf (stderr, "Cannot read FITS file %s\n", name);
	    return;
	    }
	sprintf (extname, "EXT%d", extend-1);
	hputs (header, "EXTNAME", extname);
	hputi4 (header, "EXTVER", extend-1);
	hputc (header, "COMMENT", "FITS (Flexible Image Transport System) format is defined in 'Astronomy");
	hputc (header, "COMMENT", "and Astrophysics' vol 376, page 359; bibcode 2001A&A...376..359H.");
	if (fitswimage (name, header, image) == 0)
	    printf ("%s: FITS image extension %s not written.\n",name,extname);
	else {
	    if (verbose)
		printf ("%s: FITS image extension %s written.\n",name,extname);
	    }
	extend++;
	return;
	}

    if (wcsfile) {
	header = fitsrhead (wcsfile, &lhead, &nbhead);
	hputi4 (header, "BITPIX", bitpix);
	}
    else {
	lhead = 14400;
	header = (char *) calloc (1, lhead);
	strcpy (header, "END ");
	for (i = 4; i < lhead; i++)
	    header[i] = ' ';
	hlength (header, 14400);
	hputl (header, "SIMPLE", 1);
	hputi4 (header, "BITPIX", bitpix);
	hputi4 (header, "NAXIS", 2);
	}
    if (nx > 0)
	hputi4 (header, "NAXIS1", nx);
    else if (wcsfile)
	hgeti4 (header, "NAXIS1", &nx);
    else {
	nx = 100;
	hputi4 (header, "NAXIS1", nx);
	}
    if (ny > 0)
	hputi4 (header, "NAXIS2", ny);
    else if (wcsfile)
	hgeti4 (header, "NAXIS2", &ny);
    else {
	ny = 100;
	hputi4 (header, "NAXIS2", ny);
	}
    if (bitpix < 0)
	nbimage = (-bitpix / 8) * nx * ny;
    else
	nbimage =  (bitpix / 8) * nx * ny;

    /* Read image file, if there is one */
    nbskip = 0;
    if (pixfile != NULL) {
	if ((cplus = strchr (pixfile,'+')) != NULL) {
	    *cplus = (char) 0;
	    nbskip = atoi (cplus+1);
	    }
	nbfile = getfilesize (pixfile);
	if (nbfile > 0) {
	    nbfile = nbfile - nbskip;
	    if ((image = calloc (nbimage, 1)) != NULL) {
		if ((diskfile = fopen (pixfile, "r")) != NULL) {
		    fseek (diskfile, nbskip, 0);
		    nbread = fread (image, 1, nbimage, diskfile);
		    if (nbread < nbimage)
			printf ("*** NEWFITS: %d / %d bytes read from %s\n",
				nbread, nbimage, pixfile);
		    else if (verbose)
			printf ("NEWFITS: %d bytes read from %s\n",
				nbread, pixfile);
		    fclose (diskfile);
		    sprintf (history, "Pixels from file %s", pixfile);
		    hputc (header, "HISTORY", history);
		    }
		else
		    printf ("*** NEWFITS: Could not open file %s\n", pixfile);
		}
	    else
		printf ("*** NEWFITS: Could not allocate %d bytes for image\n",
			nbimage);
	    }
	}

    /* Set up blank image, if bitpix is non-zero */
    else if (bitpix != 0) {
	if ((image = calloc (nbimage, 1)) == NULL)
	    printf ("*** NEWFITS: Could not allocate %d bytes for image\n",
		    nbimage);
	if (verbose)
	    fprintf (stderr, "NEWFITS: %d bits/pixel\n", bitpix);
	}
    else
	image = NULL;

    /* Initialize header */
    if (wcshead) {
	if (wcsfile)
	    wcs = GetFITSWCS (wcsfile,header,verbose,&cra,&cdec,&dra,&ddec,&secpix,
			  &wp,&hp,&sysout,&eqout);
	else
	    wcs = GetFITSWCS (name,header,verbose,&cra,&cdec,&dra,&ddec,&secpix,
			  &wp,&hp,&sysout,&eqout);
	wcsfree (wcs);
	}

    if (!wcsfile) {
	hputc (header, "COMMENT", "FITS (Flexible Image Transport System) format is defined in 'Astronomy");
	hputc (header, "COMMENT", "and Astrophysics' vol 376, page 359; bibcode 2001A&A...376..359H.");
	}

    if (fitswimage (name, header, image) > 0 && verbose) {
	if (image == NULL)
	    printf ("%s: dataless FITS image header written successfully.\n",
		name);
	else
	    printf ("%s: %d-byte FITS image written successfully.\n",
		name, nbimage);
	}

    free (header);
    if (image != NULL)
	free (image);
    return;
}
/* Jan  4 1999	New program
 * Apr  7 1999	Add filename argument to GetFITSWCS
 * May 13 1999	Change name to NEWFITS and add option to write blank image
 * Jun  3 1999	Allow center to be set as standard WCSTools coordinate string
 * Jun  3 1999	Move command line filenames into processing loop
 * Oct 15 1999	Free wcs using wcsfree()
 * Oct 22 1999	Drop unused variables after lint
 * Nov  1 1999	Set header length after creating it; add option for CDELTn
 *
 * Aug  1 2000	Add -i option to add image from binary file
 *
 * Jan 18 2001	Add -e option to build FITS extension file
 * Oct 11 2001	Add COMMENT with FITS reference to each header that is written
 *
 * Apr  9 2002	Fix bug in final print statement
 *
 * May 28 2003	Add -g for image with galactic coordinate WCS
 * Sep 25 2003	Add -t to set projection
 *
 * Aug 30 2004	Declare undeclared setproj() subroutine
 * Sep 29 2004	Add option to read WCS from another file
 *
 * May 10 2006	Always set BITPIX before NAXIS
 */
