/* File getfits.c
 * April 4, 2008
 * By Doug Mink, Harvard-Smithsonian Center for Astrophysics
 * Send bug reports to dmink@cfa.harvard.edu

   Copyright (C) 2002-2008
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

#define MAXRANGE 20
#define MAXFILES 2000
static int maxnfile = MAXFILES;

#define MAXKWD 500
static int maxnkwd = MAXKWD;

static void usage();
static void nextname();	/* Find next available name (namea, nameb, ...) */
static int ExtractFITS();

static int verbose = 0;		/* verbose/debugging flag */
static char *RevMsg = "GETFITS WCSTools 3.8.1, 14 December 2009, Doug Mink (dmink@cfa.harvard.edu)";
static int version = 0;		/* If 1, print only program name and version */
static char outname[128];	/* Name for output image */
static char outdir[256];	/* Output directory pathname */
static int first = 1;
static int nlog = 100;
static int xcpix = 0;
static int ycpix = 0;
static int xdpix = 0;
static int ydpix = 0;
static char *rrange;	/* Row range string */
static char *crange;	/* Column range string */
static double ra0 = -99.0;	/* Initial center RA in degrees */
static double dec0 = -99.0;	/* Initial center Dec in degrees */
static int syscoor = 0;		/* Input search coordinate system */
static double eqcoor = 0.0;	/* Input search coordinate system */

int
main (ac, av)
int ac;
char **av;
{			
    char *str;
    char *listfile = NULL;
    char **fn;
    char filename[256];
    char temp[80];
    int ifile, nfile, nbytes;
    FILE *flist;
    char *nextarg;
    char rastr[32], decstr[32];
    int nkwd = 0;		/* Number of keywords to delete */
    int nkwd1 = 0;		/* Number of keywords in delete file */
    char **kwd = NULL;		/* List of keywords to be deleted */
    char **kwdnew;
    int ikwd;
    FILE *fdk;
    char *klistfile;

    crange = NULL;
    rrange = NULL;
    outname[0] = 0;
    outdir[0] = 0;
    nfile = 0;
    fn = (char **)calloc (maxnfile, sizeof(char *));
    klistfile = NULL;

    /* Check for help or version command first */
    str = *(av+1);
    if (!str || !strcmp (str, "help") || !strcmp (str, "-help"))
	usage("");
    if (!strcmp (str, "version") || !strcmp (str, "-version")) {
	version = 1;
	usage("");
	}

    /* crack arguments */
    for (av++; --ac > 0; av++) {

	/* Set center RA, Dec, and equinox if colon in argument */
	if (strsrch (*av,":") != NULL) {
	    if (ac < 2)
		usage ("Right ascension given but no declination");
	    else {
		strcpy (rastr, *av);
		strcpy (decstr, *++av);
		ra0 = str2ra (rastr);
		dec0 = str2dec (decstr);
		ac--;
		if (ac < 1) {
		    syscoor = WCS_J2000;
		    eqcoor = 2000.0;
		    }
		else if ((syscoor = wcscsys (*(av+1))) >= 0) {
		    eqcoor = wcsceq (*++av);
		    ac--;
		    }
		else {
		    syscoor = WCS_J2000;
		    eqcoor = 2000.0;
		    }
		xcpix = -1;
		ycpix = -1;
		}
	    }

	/* Read command */
	else if (*(str = *av) == '-') {
	    char c;
	    while ((c = *++str)) {
		switch (c) {

		case 'v':	/* more verbosity */
		    verbose++;
		    break;

		case 'c':	/* center coordinates for extraction */
		    if (ac < 3)
			usage ("-c needs two arguments");
		    xcpix = atoi (*++av);
		    ac--;
		    ycpix = atoi (*++av);
		    ac--;
		    break;

		case 'd':	/* output directory */
		    if (ac < 2)
			usage ("-d needs a directory");
		    strcpy (outdir, *++av);
		    ac--;
		    break;

		case 'k':	/* Read keywords to delete from this file */
		    if (ac < 2)
			usage ("-k needs a file of keywords");
		    klistfile = *av++;
		    nkwd1 = getfilelines (klistfile);
		    if (nkwd1 + nkwd > maxnkwd) {
			maxnkwd = maxnkwd + nkwd1;
			kwdnew = (char **)calloc (maxnkwd, sizeof(char *));
			if (nkwd > 0) {
			    for (ikwd = 0; ikwd < nkwd; ikwd++)
				kwdnew[ikwd] = kwd[ikwd];
			    free (kwd);
			    }
			kwd = kwdnew;
			}
		    if ((fdk = fopen (klistfile, "r")) == NULL) {
			fprintf (stderr,"GETFITS: File %s cannot be read\n",
				 klistfile);
			}
		    else {
			for (ikwd = nkwd; ikwd < nkwd+nkwd1; ikwd++) {
			    kwd[ikwd] = (char *) calloc (32, 1);
			    first_token (fdk, 31, kwd[ikwd]);
			    }
			fclose (fdk);
			}
		    ac--;
		    break;

		case 'i':	/* logging interval */
		    if (ac < 2)
			usage ("-i needs an argument");
		    nlog = atoi (*++av);
		    ac--;
		    break;

		case 'o':	/* output file name */
		    if (ac < 2)
			usage ("-o needs an argument");
		    strcpy (outname, *++av);
		    ac--;
		    break;

		case 's':	/* output to stdout */
		    strcpy (outname, "stdout");
		    break;

		case 'x':	/* width and height for extraction */
		    if (ac < 2)
			usage ("-x needs at least one argument");
		    xdpix = atoi (*++av);
		    ac--;
		    if (ac > 1) {
			nextarg = *(av+1);
			if (isnum (nextarg)) {
			    ydpix = atoi (nextarg);
			    av++;
			    ac--;
			    }
			else
			    ydpix = xdpix;
			}
		    else
			ydpix = xdpix;
		    break;

	        default:
		    sprintf (temp, "Illegal argument '%c'", c);
		    usage(temp);
		    break;
		}
		}
    	    }

        /* center or center and size of section to extract */
        else if (isnum (*av)) {
	    if (ac > 2 && isnum (*(av+1)) && (syscoor = wcscsys (*(av+2))) > 0) {
		ra0 = str2ra (*av++);
		ac--;
		dec0 = str2dec (*av++);
		ac--;
		eqcoor = wcsceq (*av);
		xcpix = -1;
		ycpix = -1;
		}
	    else {
		if (!xcpix)
		    xcpix = atoi (str);
		else if (!ycpix)
		    ycpix = atoi (str);
		else if (!xdpix)
		    xdpix = atoi (str);
		else if (!ydpix)
		    ydpix = atoi (str);
		}
	    }

        /* range of pixels to extract */
        else if (isrange (*av)) {
	    if (crange == NULL)
		crange = str;
	    else
		rrange = str;
	    }

 	else if (*av[0] == '@') {
	    listfile = *av + 1;
	    }

        /* Image file */
        else if (isfits (*av)) {
            if (nfile >= maxnfile) {
                maxnfile = maxnfile * 2;
                nbytes = maxnfile * sizeof (char *);
                fn = (char **) realloc ((void *)fn, nbytes);
                }
            fn[nfile] = *av;
            nfile++;
            }
	}

    /* If center is set, but not size, extract 500x500 image */
    if (!crange) {
	if (xcpix && ycpix && xdpix == 0)
	    xdpix = 500;
	if (xcpix && ycpix && ydpix == 0)
	    ydpix = xdpix;
	}

    /* If one side is set, set the other */
    if (xdpix && !ydpix)
	ydpix = xdpix;

    /* now there are ac remaining file names starting at av[0] */
    if (listfile && isimlist (listfile)) {
	nfile = getfilelines (listfile);
	if ((flist = fopen (listfile, "r")) == NULL) {
	    fprintf (stderr,"I2F: Image list file %s cannot be read\n",
		     listfile);
	    usage ();
	    }
	for (ifile = 0; ifile < nfile; ifile++) {
	    first_token (flist, 254, filename);
	    ExtractFITS (filename,kwd,nkwd);
	    }
	fclose (flist);
	}

    /* Process files from command line */
    else if (fn) {
	for (ifile = 0; ifile < nfile; ifile++) {
	    (void) ExtractFITS (fn[ifile],kwd,nkwd);
	    }
	}

    return (0);
}

static void
usage (errmsg)

char *errmsg;	/* Error message */
{
    fprintf (stderr,"%s\n",RevMsg);
    if (version)
	exit (-1);
    if (*errmsg)
	fprintf (stderr, "*** %s ***\n", errmsg);
    fprintf (stderr,"Extract FITS files from FITS image files\n");
    fprintf(stderr,"Usage: getfits -sv [-i num] [-o name] [-d dir] file.fits [xrange yrange] [x y dx [dy]] ...\n");
    fprintf(stderr,"  or : getfits [-sv1][-i num][-o name] [-d path] @fitslist [xrange yrange] [x y dx [dy]]\n");
    fprintf(stderr,"  xrange: Columns to extract in format x1-x2\n");
    fprintf(stderr,"  yrange: Rows to extract in format y1-y2\n");
    fprintf(stderr,"  x y: Center pixel (column row) of region to extract\n");
    fprintf(stderr,"  hh:mm:ss dd:mm:ss sys: Center pixel in sky coordintes\n");
    fprintf(stderr,"  dx dy: Width and height in pixels of region to extract\n");
    fprintf(stderr,"         (Height is same as width if omitted)\n");
    fprintf(stderr,"  -d dir: write FITS file(s) to this directory\n");
    fprintf(stderr,"  -i num: log rows as they are copied at this interval\n");
    fprintf(stderr,"  -k file: file of keyword names to delete from output header\n");
    fprintf(stderr,"  -o name: output name for one file\n");
    fprintf(stderr,"  -s: write output to standard output\n");
    fprintf(stderr,"  -x dx dy: dimensions of image section to be extracted\n");
    fprintf(stderr,"  -v: verbose\n");
    exit (1);
}

static int
ExtractFITS (name, kwd, nkwd)

char	*name;
char	**kwd;
int	nkwd;
{
    char *header;	/* FITS header */
    int lhead;		/* Maximum number of bytes in FITS header */
    int nbhead;		/* Actual number of bytes in FITS header */
    char history[128];	/* for HISTORY line */
    char fitsname[256];	/* Name of FITS file */
    char fitspath[256];	/* Pathname of FITS file  */
    char *fitsfile;
    double crpix1, crpix2;
    double cxpix, cypix;
    double ra, dec;
    double bzero, bscale, dmin, dmax, drange;
    int offscl;
    int wp;             /* Image width in pixels (returned) */
    int hp;             /* Image height in pixels (returned) */
    double eqout=0.0;   /* Equinox to return (0=image, returned) */
    int ifrow1, ifrow2;	/* First and last rows to extract */
    int ifcol1, ifcol2;	/* First and last columns to extracT */
    int npimage;        /* Number of pixels in image */
    int nbimage;        /* Number of bytes in image */
    int ikwd;
    char *endchar;
    char *ltime;
    char *newimage;
    char *kw, *kwl;
    int bitpix;
    int bytepix;
    int nblock, nbleft;
    int nrows, ncols;
    char rastr[32], decstr[32], cstr[32];
    char temp[80];
    int xdim = 1;
    int ydim = 1;
    struct Range *xrange;    /* Column range structure */
    struct Range *yrange;    /* Row range structure */
    struct WorldCoor *wcs, *GetWCSFITS();

    /* Check to see if this is a FITS file */
    if (!isfits (name)) {
	fprintf (stderr, "File %s is not a FITS file\n", name);
	return (-1);
	}

    /* Read FITS header */
    if ((header = fitsrhead (name, &lhead, &nbhead)) == NULL) {
	fprintf (stderr, "Cannot read FITS file %s\n", name);
	return (-1);
	}

    /* If requested, delete keywords one at a time */
    for (ikwd = 0; ikwd < nkwd; ikwd++) {

	/* Make keyword all upper case */
	kwl = kwd[ikwd] + strlen (kwd[ikwd]);
	for (kw = kwd[ikwd]; kw < kwl; kw++) {
	    if (*kw > 96 && *kw < 123)
		*kw = *kw - 32;
	    }

	/* Delete keyword */
	if (hdel (header, kwd[ikwd]) && verbose)
	    printf ("%s: %s deleted\n", name, kwd[ikwd]);
	}


    if (verbose && first) {
	fprintf (stderr,"%s\n",RevMsg);
	fprintf (stderr, "Extract from FITS image file %s\n", name);
	}

    if (ra0 > -99.0 && dec0 > -99.0) {
	wcs = GetWCSFITS (name, header, verbose);
	if (iswcs (wcs)) {
	    ra = ra0;
	    dec = dec0;
	    wcscon (syscoor, wcs->syswcs, eqcoor, eqout, &ra, &dec, wcs->epoch);
	    wcs2pix (wcs, ra, dec, &cxpix, &cypix, &offscl);
	    xcpix = (int) (cxpix + 0.5);
	    ycpix = (int) (cypix + 0.5);
	    wcsfree (wcs);
	    }
	else {
	    fprintf (stderr, "No WCS in FITS image file %s\n", name);
	    wcsfree (wcs);
	    return (-1);
	    }
	}

    ncols = 0;
    (void) hgeti4 (header, "NAXIS1", &ncols);
    nrows = 1;
    (void) hgeti4 (header, "NAXIS2", &nrows);
    bitpix = 16;
    (void) hgeti4 (header, "BITPIX", &bitpix);
    if (bitpix < 0)
	bytepix = -bitpix / 8;
    else
	bytepix = bitpix / 8;

    /* Set up limiting rows to read */
    if (rrange) {
	yrange = RangeInit (rrange, ydim);
	ifrow1 = yrange->ranges[0];
	ifrow2 = yrange->ranges[1];
	}
    else if (xcpix && ycpix) {
	int ydpix2 = ydpix / 2;
	ifrow1 = ycpix - ydpix2 + 1;
	ifrow2 = ifrow1 + ydpix - 1;
	}
    else{
	ifrow1 = 1;
	ifrow2 = nrows;
	}
    if (ifrow1 < 1)
	ifrow1 = 1;
    if (ifrow2 < 1)
	ifrow2 = 1;
    if (ifrow1 > nrows)
	ifrow1 = nrows;
    if (ifrow2 > nrows)
	ifrow2 = nrows;
    hp = ifrow2 - ifrow1 + 1;

    /* Set up first and number of columns to write */
    if (crange) {
	xrange = RangeInit (crange, xdim);
	ifcol1 = xrange->ranges[0];
	ifcol2 = xrange->ranges[1];
	}
    else if (xdpix) {
	int xdpix2 = xdpix / 2;
	ifcol1 = xcpix - xdpix2 + 1;
	ifcol2 = ifcol1 + xdpix - 1;
	}
    else {
	ifcol1 = 1;
	ifcol2 = ncols;
	}
    if (ifcol1 < 1)
	ifcol1 = 1;
    if (ifcol2 < 1)
	ifcol2 = 1;
    if (ifcol1 > ncols)
	ifcol1 = ncols;
    if (ifcol2 > ncols)
	ifcol2 = ncols;
    wp = ifcol2 - ifcol1 + 1;

    /* Extract image */
    newimage = fitsrsect (name, header, nbhead, ifcol1, ifrow1, wp, hp, nlog);

    hputi4 (header, "NAXIS", 2);
    hputi4 (header, "NAXIS1", wp);
    hputi4 (header, "NAXIS2", hp);
    hdel (header, "NAXIS3");
    hdel (header, "NAXIS4");
    hdel (header, "NAXIS5");
    npimage = wp * hp;
    nbimage = npimage * bytepix;
    nblock = nbimage / 2880;
    nbleft = (nblock + 1) * 2880 - nbimage;
    if (nbleft > 2880)
	nbleft = 0;

    /* Set data limits for scaling when converting image formats */
    bscale = 1.0;
    hgetr8 (header, "BSCALE", &bscale);
    bzero = 0.0;
    hgetr8 (header, "BZERO", &bzero);
    dmin = minvec (newimage, bitpix, bzero, bscale, 0, npimage);
    dmax = maxvec (newimage, bitpix, bzero, bscale, 0, npimage);
    drange = dmax - dmin;
    dmin = dmin - (0.2 * drange);
    dmax = dmax + (0.2 * drange);
    hputnr8 (header, "DATAMIN", 0, dmin);
    hputnr8 (header, "DATAMAX", 0, dmax);

    /* Reset image WCS if present */
    if (ifcol1 > 1 || ifrow1 > 1) {

	/* If IRAF TNX image, keep all WCS keywords, but add dependency on PLATE WCS */
	if (wcs->lngcor != NULL) {
	   hputs (header, "WCSDEP", "PLATE");
	   }

	/* Otherwise, reset reference pixel coordinate */
	else {
	    if (hgetr8 (header, "CRPIX1", &crpix1)) {
		crpix1 = crpix1 - (double) (ifcol1 - 1);
		hputr8 (header, "CRPIX1", crpix1);
		}
	    else {
		hputs (header, "CTYPE1", "PIXEL");
		hputi4 (header, "CRPIX1", 1);
		hputi4 (header, "CRVAL1", ifcol1);
		hputi4 (header, "CDELT1", 1);
		}
	    if (hgetr8 (header, "CRPIX2", &crpix2)) {
		crpix2 = crpix2 - (double) (ifrow1 - 1);
		hputr8 (header, "CRPIX2", crpix2);
		}
	    else {
		hputs (header, "CTYPE2", "PIXEL");
		hputi4 (header, "CRPIX2", 1);
		hputi4 (header, "CRVAL2", ifrow1);
		hputi4 (header, "CDELT2", 1);
		}
	    }

	/* Set up output pixel to original pixel transformation */
	if (hgetr8 (header, "CRPIX1P", &crpix1)) {
	    crpix1 = crpix1 - (double) (ifcol1 - 1);
	    hputr8 (header, "CRPIX1P", crpix1);
	    }
	else {
	    hputs (header, "WCSNAMEP", "PLATE");
	    hputs (header, "CTYPE1P", "PIXEL");
	    hputi4 (header, "CRPIX1P", 1);
	    hputi4 (header, "CRVAL1P", ifcol1);
	    hputi4 (header, "CDELT1P", 1);
	    }
	if (hgetr8 (header, "CRPIX2P", &crpix2)) {
	    crpix2 = crpix2 - (double) (ifrow1 - 1);
	    hputr8 (header, "CRPIX2P", crpix2);
	    }
	else {
	    hputs (header, "CTYPE2P", "PIXEL");
	    hputi4 (header, "CRPIX2P", 1);
	    hputi4 (header, "CRVAL2P", ifrow1);
	    hputi4 (header, "CDELT2P", 1);
	    }
	}

    /* Add HISTORY notice of this conversion */
    if (ra0 > -99.0 && dec0 > -99.0) {
	strcpy (history, RevMsg);
	endchar = strchr (history, ',');
	*endchar = (char) 0;
	strcat (history, " ");
	ltime = lt2fd ();
	strcat (history, ltime);
	endchar = strrchr (history,':');
	*endchar = (char) 0;
	ra2str (rastr, 31, ra0, 3);
	dec2str (decstr, 31, dec0, 2);
	wcscstr (cstr, syscoor, eqcoor, 0.0);
	sprintf (temp, " %d x %d centered on %s %s %s",
		 xdpix, ydpix, rastr, decstr, cstr);
	strcat (history, temp);
	hputc (header, "HISTORY", history);
	}
    if (xcpix && ycpix) {
	strcpy (history, RevMsg);
	endchar = strchr (history, ',');
	*endchar = (char) 0;
	strcat (history, " ");
	ltime = lt2fd ();
	strcat (history, ltime);
	endchar = strrchr (history,':');
	*endchar = (char) 0;
	sprintf (temp, " %d x %d centered on [%d,%d]",
		 xdpix, ydpix, xcpix, ycpix);
	strcat (history, temp);
	hputc (header, "HISTORY", history);
	}
    else if (crange && rrange) {
	strcpy (history, RevMsg);
	endchar = strchr (history, ',');
	*endchar = (char) 0;
	strcat (history, " ");
	ltime = lt2fd ();
	strcat (history, ltime);
	endchar = strrchr (history,':');
	*endchar = (char) 0;
	sprintf (temp, " Rows %d - %d, Columns %d - %d",
		 ifrow1, ifrow2, ifcol1, ifcol2);
	strcat (history, temp);
	hputc (header, "HISTORY", history);
	}

    /* If assigned output name, use it */
    if (outname[0] != (char) 0)
	strcpy (fitsname, outname);

    /* Otherwise, create a new name */
    else
	nextname (name, fitsname);

    /* Write FITS file to a specified directory */
    if (outdir[0] > 0) {
	strcpy (fitspath, outdir);
	strcat (fitspath, "/");
	fitsfile = strrchr (fitsname,'/');
	if (fitsfile == NULL)
	    strcat (fitspath, fitsname);
	else
	    strcat (fitspath, fitsfile+1);
	}
    else
	strcpy (fitspath, fitsname);

    if (fitswimage (fitspath, header, newimage) > 0) {
	if (verbose)
	    fprintf (stderr, "%s: written successfully.\n", fitspath);
	else
	    printf ("%s\n", fitspath);
	}
    else if (verbose)
	fprintf (stderr, "NEWFITS: File %s not written.\n", fitspath);

    if (verbose)
	fprintf (stderr, "\n");
    free (newimage);
    free (header);
    return (0);
}

static void
nextname (name, newname)

char *name;
char *newname;
{
    char *ext, *sufchar;
    int lname;

    ext = strrchr (name, '.');
    if (ext)
	lname = ext - name;
    else
	lname = strlen (name);
    strncpy (newname, name, lname);
    sufchar = newname + lname;
    *sufchar = 'a';
    *(sufchar+1) = (char) 0;
    strcat (newname, ext);
    while (!access (newname, F_OK)) {
	if (*sufchar < 'z')
	    (*sufchar)++;
	else {
	    *sufchar = 'a';
	    sufchar++;
	    *sufchar = 'a';
	    *(sufchar+1) = (char) 0;
	    strcat (newname, ext);
	    }
	}
    return;
}

/* Oct 22 2002	New program based on t2f
 * Dec  6 2002	Initialize bytepix, which wasn't
 * Dec 16 2002	Add -k option to delete FITS keywords when copying
 *
 * Jan 30 2003	Fix typo in variable name 
 * May  2 2003	Fix bug if no keywords are deleted
 *
 * Apr 16 2004	Delete NAXISn for n > 2 in output image
 * Sep 15 2004	Fix bug dealing with center specified as sky coordinates
 * Sep 17 2004	Add option to set extraction center as decimal degrees
 * Dec  6 2004	Don't print gratuitous newline at end of process
 *
 * Sep 30 2005	Convert input center coordinates to image system
 *
 * Jun 21 2006	Clean up code
 *
 * Jan 10 2007	Use range subroutines in library
 * Jun 11 2007	Compute minimum and maximum data values in output image
 *
 * Apr  4 2008	Make extracted TNX WCS dependent on original PLATE WCS
 */
