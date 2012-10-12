/* File remap.c
 * January 10, 2007
 * By Doug Mink, Harvard-Smithsonian Center for Astrophysics
 * Send bug reports to dmink@cfa.harvard.edu

   Copyright (C) 1999-2007
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

#define REMAP_CLOSEST	1
#define REMAP_FLUX	2

extern void setcenter();
extern void setsys();
extern void setrot();
extern void setsecpix();
extern void setsecpix2();
extern void setrefpix();
extern void setnpix();
extern void setwcsproj();
extern void setscale();

static void usage();
static int verbose = 0;		/* verbose flag */
static char *outname0 = "remap.fits";
static char *outname;
static char *wcsproj;           /* WCS projection name */

static char *RevMsg = "REMAP WCSTools 3.8.1, 14 December 2009, Doug Mink (dmink@cfa.harvard.edu)";
static int nfiles = 0;
static int fitsout = 1;
static double secpix = 0;
static double secpix2 = 0;
static int bitpix0 = 0; /* Output BITPIX, =input if 0 */
static int RemapImage();
static void getsection();
extern struct WorldCoor *GetFITSWCS();
static struct WorldCoor *wcsout = NULL;
static char *irafheader;	/* IRAF image header */
static char *headout;		/* FITS output header */
static char *imout;		/* FITS output image */
static int lhead;		/* Maximum number of bytes in FITS header */
static int iraffile;
static int eqsys = 0;
static double equinox = 0.0;
static int outsys = 0;
static int version = 0;		/* If 1, print only program name and version */
static int remappix=0;		/* Number of samples of input pixels */
static char *wcsfile;		/* Read WCS from this FITS or IRAF file */
static double xrpix = 0.0;
static double yrpix = 0.0;
static double blankpix = 0.0;	/* image value for blank pixel */
static int nx = 0;
static int ny = 0;
static int nlog = 0;
static int undistort = 0;

int
main (ac, av)
int ac;
char **av;
{
    char *str;
    char rastr[32];
    char decstr[32];
    char filename[128];
    char *filelist[100];
    char errmsg[100];
    char *listfile = NULL;
    int readlist = 0;
    FILE *flist = NULL;
    int ifile, i;
    int lwcs;
    char c;

    for (i = 0; i < 100; i++)
	filelist[i] = NULL;
    wcsproj = NULL;
    wcsfile = NULL;

    outname = NULL;

    /* Check for help or version command first */
    str = *(av+1);
    if (!str || !strcmp (str, "help") || !strcmp (str, "-help"))
	usage((char) 0, NULL);
    if (!strcmp (str, "version") || !strcmp (str, "-version")) {
	version = 1;
	usage((char) 0, NULL);
	}

    setrot (0.0);

    /* Crack arguments */
    for (av++; --ac > 0 && (*(str = *av)=='-' || *str == '@'); av++) {
	if (*str == '@')
	    str = str - 1;
	while ((c = *++str))
	switch (c) {
    	case 'a':	/* Output rotation angle in degrees */
    	    if (ac < 2)
    		usage(c, "needs rotation angle in degrees");
    	    setrot (atof (*++av));
    	    ac--;
    	    break;

    	case 'b':	/* Output image center on command line in B1950 */
    	    if (ac < 3)
    		usage(c, "needs B1950 coordinates of output reference pixel");
	    setsys (WCS_B1950);
	    outsys = WCS_B1950;
	    strcpy (rastr, *++av);
	    ac--;
	    strcpy (decstr, *++av);
	    ac--;
	    setcenter (rastr, decstr);
    	    break;

    	case 'e':	/* Output image center on command line in ecliptic */
    	    if (ac < 3)
    		usage(c,"needs ecliptic coordinates of output reference pixel");
	    setsys (WCS_ECLIPTIC);
	    outsys = WCS_ECLIPTIC;
	    strcpy (rastr, *++av);
	    ac--;
	    strcpy (decstr, *++av);
	    ac--;
	    setcenter (rastr, decstr);
    	    break;

	case 'f':	/* Read WCS from a FITS or IRAF file */
	    if (ac < 2)
		usage(c, "needs FITS or IRAF file name for header WCS");
	    wcsfile = *++av;
	    ac--;
	    break;

    	case 'g':	/* Output image center on command line in galactic */
    	    if (ac < 3)
    		usage(c,"needs galactic coordinates of output reference pixel");
	    setsys (WCS_GALACTIC);
	    outsys = WCS_GALACTIC;
	    strcpy (rastr, *++av);
	    ac--;
	    strcpy (decstr, *++av);
	    ac--;
	    setcenter (rastr, decstr);
    	    break;

	case 'i':	/* Bits per output pixel in FITS code */
    	    if (ac < 2)
    		usage(c, "needs bits per output pixel");
    	    bitpix0 = atoi (*++av);
    	    ac--;
    	    break;

    	case 'j':	/* Output image center on command line in J2000 */
    	    if (ac < 3)
    		usage(c, "needs J2000 coordinates of output reference pixel");
	    setsys (WCS_J2000);
	    outsys = WCS_J2000;
	    strcpy (rastr, *++av);
	    ac--;
	    strcpy (decstr, *++av);
	    ac--;
	    setcenter (rastr, decstr);
    	    break;

    	case 'l':	/* Logging interval for processing */
    	    if (ac < 2)
    		usage(c, "needs logging interval");
    	    nlog = atoi (*++av);
    	    ac--;
    	    break;

	case 'n':	 /* value for blank (null) pixel */
    	    if (ac < 2)
    		usage(c, "needs a blank pixel value");
    	    blankpix = atof (*++av);
    	    ac--;
    	    break;

    	case 'o':	/* Specifiy output image filename */
    	    if (ac < 2)
    		usage(c, "needs a filename");
	    outname = *++av;
	    ac--;
    	    break;

    	case 'p':	/* Output image plate scale in arcseconds per pixel */
    	    if (ac < 2)
    		usage(c, "needs a plate scale");
	    secpix = atof (*++av);
    	    setsecpix (secpix);
    	    ac--;
	    if (ac > 1 && isnum (*(av+1))) {
		secpix2 = atof (*++av);
		setsecpix2 (secpix2);
		ac--;
		}
    	    break;

	case 's':	/* Use BSCALE and BZERO to scale output image pixels */
	    setscale (1);
    	    break;

	case 't':	/* Number of samples per linear input pixel */
    	    if (ac < 2)
    		usage(c, "needs a number of samples per input pixel");
    	    remappix = atoi (*++av);
    	    ac--;
    	    break;

	case 'u':	/* Delete distortion keywords */
	    undistort++;
	    break;

	case 'v':	/* more verbosity */
	    verbose++;
	    break;

	case 'w':	/* Set WCS projection */
	    if (ac < 2)
		usage(c, "needs a WCS projection");
	    wcsproj = *++av;
	    lwcs = strlen (wcsproj);
	    for (i =0; i < lwcs; i++) {
		if (wcsproj[i] > 96)
		    wcsproj[i] = wcsproj[i] - 64;
		}
	    ac--;
	    break;

	case 'x':	/* X and Y coordinates of output image reference pixel */
	    if (ac < 3)
		usage(c, "needs X and Y coordinates of reference pixel");
	    xrpix = atof (*++av);
	    ac--;
	    yrpix = atof (*++av);
	    ac--;
    	    setrefpix (xrpix, yrpix);
    	    break;

	case 'y':	/* Dimensions of output image in pixels */
	    if (ac < 3)
		usage(c, "needs x and y dimensions of output image");
	    nx = atoi (*++av);
	    ac--;
	    ny = atoi (*++av);
	    ac--;
    	    setnpix (nx, ny);
    	    break;

	case 'z':       /* Use AIPS classic WCS */
	    setdefwcs (WCS_ALT);
	    break;

	case '@':	/* List of files to be read */
	    readlist++;
	    listfile = ++str;
	    str = str + strlen (str) - 1;
	    av++;
	    ac--;
	    break;

	default:
	    usage(c,"Argument not recognized");
	    break;
	}
    }

    /* Find number of images to stack  and leave listfile open for reading */
    if (readlist) {
	if ((flist = fopen (listfile, "r")) == NULL) {
	    sprintf (errmsg,"List file %s cannot be read", listfile);
	    usage ((char) 0, errmsg);
	    }
	while (fgets (filename, 128, flist) != NULL)
	    nfiles++;
	fclose (flist);
	flist = NULL;
	if (nfiles > 0)
	    flist = fopen (listfile, "r");
	}

    /* If no arguments left, print usage */
    else if (ac == 0)
	usage((char)0, "No arguments left");

    /* Read ac remaining file names starting at av[0] */
    else {
	while (ac-- > 0) {
	    filelist[nfiles] = *av++;
	    if (verbose)
		printf ("Reading %s\n",filelist[nfiles]);
	    nfiles++;
	    }
	}

    /* Set up FITS header and WCS for output file */

    /* Remap images */
    for (ifile = 0;  ifile < nfiles; ifile++) {
	if (readlist) {
	    if (fgets (filename, 128, flist) != NULL) {
		filename[strlen (filename) - 1] = 0;
		if (RemapImage (ifile, filename))
		    break;
		}
	    }
	else {
	    if (RemapImage (ifile, filelist[ifile]))
		break;
	    }
	}

    if (readlist)
	fclose (flist);

    /* Write output image */
    if (iraffile && !fitsout) {
        if (irafwimage (outname, lhead, irafheader, headout, imout) > 0 && verbose)
            printf ("%s: written successfully.\n", outname);
        }
    else {
        if (fitswimage (outname, headout, imout) > 0 && verbose)
            printf ("%s: written successfully.\n", outname);
        }

    free (headout);
    wcsfree (wcsout);
    free (imout);
    return (0);
}

static void
usage (arg, message)
char	arg;	/* single character command line argument */
char	*message;	/* Error message */

{
    fprintf (stderr,"%s\n",RevMsg);
    if (version)
	exit (-1);
    if (message != NULL)
	fprintf (stderr, "ERROR: %c %s\n", arg, message);
    fprintf (stderr,"Remap FITS or IRAF images into single FITS image using WCS\n");
    fprintf(stderr,"Usage: remap [-vf][-a rot][[-b][-j] ra dec][-i bits][-l num] file1.fit file2.fit ... filen.fit\n");
    fprintf(stderr,"  or : remap [-vf][-a rot][[-b][-j] ra dec][-i bits][-l num] @filelist\n");
    fprintf(stderr,"  -a: Output rotation angle in degrees (default 0)\n");
    fprintf(stderr,"  -b ra dec: Output center in B1950 (FK4) RA and Dec\n");
    fprintf(stderr,"  -e long lat: Output center in ecliptic longitude and latitude\n");
    fprintf(stderr,"  -f file: Use WCS from this file as output WCS\n");
    fprintf(stderr,"  -g long lat: Output center in galactic longitude and latitude\n");
    fprintf(stderr,"  -i num: Number of bits per output pixel (default is input)\n");
    fprintf(stderr,"  -j ra dec: center in J2000 (FK5) RA and Dec\n");
    fprintf(stderr,"  -l num: Log every num rows of output image\n");
    fprintf(stderr,"  -m mode: c closest pixel (more to come)\n");
    fprintf(stderr,"  -n num: integer pixel value for blank pixel\n");
    fprintf(stderr,"  -o name: Name for output image\n");
    fprintf(stderr,"  -p secpix: Output plate scale in arcsec/pixel (default =input)\n");
    fprintf(stderr,"  -s: Set BZERO and BSCALE in output file from input file\n");
    fprintf(stderr,"  -t: Number of samples per linear output pixel\n");
    fprintf(stderr,"  -u: Delete distortion keywords from output file\n");
    fprintf(stderr,"  -v: Verbose\n");
    fprintf(stderr,"  -w type: Output WCS type (input is default)\n");
    fprintf(stderr,"  -x x y: Output image reference X and Y coordinates (default is center)\n");
    fprintf(stderr,"  -y nx ny: Output image dimensions (default is first input image)\n");
    exit (1);
}


static int
RemapImage (ifile, filename)

int	ifile;		/* Sequence number of input file */
char	*filename;	/* FITS or IRAF file filename */

{
    char *image;		/* FITS input image */
    char *header;		/* FITS header */
    int nbhead;			/* Actual number of bytes in FITS header */
    int bitpix;
    int bitpixout = -32;
    double cra, cdec, dra, ddec;
    int hpin, wpin, hpout, wpout, nbout, npout;
    int iin = 0;
    int iout, jin, jout;
    int iout1, iout2, jout1, jout2;
    int idiff, jdiff;
    int offscl, lblock;
    char pixname[256];
    struct WorldCoor *wcsin;
    double bzin, bsin, bzout, bsout;
    double dx, secpixin1, secpixin2, secpix1, dpix, dnpix;
    double xout, yout, xin, yin, xpos, ypos, dpixi, dpixo, xout0, yout0;
    double xmin, xmax, ymin, ymax, xin1, xin2, yin1, yin2;
    double pixratio;
    char secstring[32];
    char history[80];
    char wcstemp[16];
    struct WorldCoor *GetWCSFITS();
    double *imvec;
    int npix;
    int addscale = 0;
    double *dxout, *dyout;

    /* Read input IRAF header and image */
    if (isiraf (filename)) {
	iraffile = 1;
	if ((irafheader = irafrhead (filename, &lhead)) != NULL) {
	    nbhead = 0;
	    if ((header = iraf2fits (filename,irafheader,lhead, &nbhead)) == NULL) {
		fprintf (stderr, "Cannot translate IRAF header %s/n",filename);
		free (irafheader);
		return (1);
		}
	    if ((image = irafrimage (header)) == NULL) {
		hgetm (header,"PIXFIL", 255, pixname);
		fprintf (stderr, "Cannot read IRAF pixel file %s\n", pixname);
		free (irafheader);
		free (header);
		return (1);
		}
	    }
	else {
	    fprintf (stderr, "Cannot read IRAF header file %s\n", filename);
	    return (1);
	    }
	}

    /* Or read input FITS image */
    else {
	iraffile = 0;
	if ((header = fitsrhead (filename, &lhead, &nbhead)) != NULL) {
	    if ((image = fitsrimage (filename, nbhead, header)) == NULL) {
		fprintf (stderr, "Cannot read FITS image %s\n", filename);
		free (header);
		return (1);
		}
	    }
	else {
	    fprintf (stderr, "Cannot read FITS file %s\n", filename);
	    return (1);
	    }
	}
    if (ifile < 1 && verbose)
	fprintf (stderr,"%s\n",RevMsg);

    /* Set input world coordinate system from first image header */
    wcsin = wcsinit (header);

    if (!outsys && !wcsfile)
	outsys = wcsin->syswcs;

    if (ifile < 1) {

	/* Read output image if one is specified */
	imout = NULL;
	if (outname != NULL) {
	    if (access (outname, F_OK)) {
		if (verbose)
		    fprintf (stderr, "REMAP: Writing file %s\n", outname);
		}
	    else if ((headout = fitsrhead (outname, &lhead, &nbhead)) != NULL) {
		if ((imout = fitsrimage (outname, nbhead, headout)) == NULL)
		    fprintf (stderr, "REMAP: Overwriting file %s\n", outname);
		}
	    }
	else
	    outname = outname0;

	/* Set output world coordinate system from existing output image header */
	if (imout != NULL)
	    wcsout = wcsinit (headout);

	/* Set output world coordinate system from another image header */
	else if (wcsfile) {
	    wcsout = GetWCSFITS (wcsfile, verbose);
	    outsys = wcsout->syswcs;
	    headout = fitsrhead (wcsfile, &lhead, &nbhead);
	    wpout = wcsout->nxpix;
	    hpout = wcsout->nypix;
	    }

	/* Otherwise set it from command line and first image */
	else {

	    /* Set output plate scale */
	    secpixin1 = wcsin->cdelt[1] * 3600.0;
	    secpixin2 = wcsin->cdelt[2] * 3600.0;
	    if (secpixin1 < 0)
		secpixin1 = -secpixin1;
	    if (secpix == 0)
		secpix = secpixin1;
	    if (secpix2 == 0)
		secpix2 = secpix;

	    /* Change output dimensions to match output plate scale */
	    if (secpix != 0.0)
		pixratio = fabs (secpixin1) / fabs (secpix);
	    else
		pixratio = 1.0;
	    if (nx == 0 && ny == 0) {
		nx = wcsin->nxpix * pixratio;
		ny = wcsin->nypix * pixratio;
    		setnpix (nx, ny);
		}

	    /* Set reference pixel to default of new image center */
	    if (nx != 0 && xrpix == 0.0 && yrpix == 0.0) {
		xrpix = 0.5 * (double) nx;
		yrpix = 0.5 * (double) ny;
		setrefpix (xrpix, yrpix);
		}

	    /* Set output header from command line and first image header */
	    lhead = strlen (header);
	    lblock = lhead / 2880;
	    if (lblock * 2880  < lhead)
		lhead = (lblock+2) * 2880;
	    else
		lhead = (lblock+1) * 2880;
	    if (!(headout = (char *) calloc (lhead, sizeof (char)))) {
		fprintf (stderr, "REMAP: cannot allocate output image header\n");
        	return (1);
		}
	    strcpy (headout, header);

	    hputi4 (headout, "NAXIS1", nx);
	    hputi4 (headout, "NAXIS2", ny);

	    if (wcsproj != NULL || outsys != wcsin->syswcs) {
		if (wcsproj == NULL)
		    wcsproj = wcsin->ctype[0]+4;
		if (outsys == WCS_GALACTIC)
		    strcpy (wcstemp, "GLON-");
		else if (outsys == WCS_ECLIPTIC)
		    strcpy (wcstemp, "ELON-");
		else
		    strcpy (wcstemp, "RA---");
		strcat (wcstemp, wcsproj);
	        hputs  (headout, "CTYPE1", wcstemp);

		if (outsys == WCS_GALACTIC)
		    strcpy (wcstemp, "GLAT-");
		else if (outsys == WCS_ECLIPTIC)
		    strcpy (wcstemp, "ELAT-");
		else
		    strcpy (wcstemp, "DEC--");
		strcat (wcstemp, wcsproj);
		hputs  (headout, "CTYPE2", wcstemp);
		}
	    hputr8 (headout, "CRPIX1", xrpix);
	    hputr8 (headout, "CRPIX2", yrpix);
	    hputr8 (headout, "CDELT1", -secpix/3600.0);
	    hputr8 (headout, "CDELT2", secpix2/3600.0);
	    if (hgetr8 (headout, "SECPIX1", &secpix1)) {
		hputr8 (headout, "SECPIX1", secpix);
		hputr8 (headout, "SECPIX2", secpix2);
		}
	    else if (hgetr8 (headout, "SECPIX", &secpix1)) {
		if (secpix == secpix2)
		    hputr8 (headout, "SECPIX", secpix);
		else {
		    hputr8 (headout, "SECPIX1", secpix);
		    hputr8 (headout, "SECPIX2", secpix2);
		    }
		}

	    /* Delete distortion keywords from header if requested */
	    if (undistort)
		DelDistort (headout, verbose);

	    /* Set output WCS from command line and first image header */
	    wcsout = GetFITSWCS (filename, headout, verbose, &cra, &cdec, &dra,
			 &ddec, &secpix, &wpout, &hpout, &eqsys, &equinox);
	    }

	hgeti4 (header, "BITPIX", &bitpix);
	if (bitpix0 != 0) {
	    hputi4 (headout, "BITPIX", bitpix0);
	    bitpixout = bitpix0;
	    }
	else
	    bitpixout = bitpix;

	/* Warn if remapping is not acceptable */
	pixratio = wcsin->xinc / wcsout->xinc;
	if (remappix == 0)
	    remappix = 1;

	/* Allocate space for output image */
	if (imout == NULL) {
	    npout = hpout * wpout;
	    nbout = npout * bitpixout / 8;
	    if (nbout < 0)
		nbout = -nbout;
	    if (!(imout = (char * ) calloc (nbout, 1))) {
		fprintf (stderr, "REMAP: cannot allocate output image\n");
        	return (1);
		}

	    /* Fill output image */
	    bsin = 1.0;
	    hgetr8 (header, "BSCALE", &bsin);
	    bzin = 0.0;
	    hgetr8 (header, "BZERO", &bzin);
	    bsout = 1.0;
	    hgetr8 (header, "BSCALE", &bsout);
	    bzout = 0.0;
	    hgetr8 (header, "BZERO", &bzout);

	    /* Delete data section keywords which do not apply to output image */
	    hdel (headout, "DATASEC");
	    hdel (headout, "CCDSEC");
	    hdel (headout, "TRIMSEC");
	    hdel (headout, "BIASSEC");
	    hdel (headout, "ORIGSEC");

	    /* Add source of WCS if not from command line */
	    if (wcsfile) {
		sprintf (history, "REMAP WCS from file %s", wcsfile);
		hputc (headout, "HISTORY", history);
		}

	    /* Fill output image with value of blank pixels if not zero */
	    if (blankpix != 0.0 || bzout != 0.0) {
		if (!(imvec = (double *) calloc (wpout, sizeof (double)))) {
		    fprintf (stderr, "REMAP: cannot allocate blank pixel vector\n");
        	    return (1);
		    }
		if (verbose)
		    fprintf (stderr,"REMAP: Filling output image with blank pixel %f\n",
			     blankpix);

		npix = hpout * wpout;
		fillvec1 (imout, bitpixout, bzout, bsout, 1, npix, blankpix);
		}

	    /* Save blank pixel value in the image header */
	    if (bitpixout > 0) {
		int iblank;
		if (blankpix > 0.0)
		    iblank = (int) (blankpix + 0.5);
		else if (blankpix < 0.0)
		    iblank = (int) (blankpix - 0.5);
		else
		    iblank = 0;
		hputi4 (headout, "BLANK", iblank);
		}
	    else {
		hputnr8 (headout, "BLANK", 1, blankpix);
		}

	    /* Set output WCS output coordinate system to input coordinate system*/
	    wcsout->sysout = wcsin->syswcs;
	    }

	/* Set local used parameters if filling existing image */
	else {
	    }
	}

    /* Set input WCS output coordinate system to output coordinate system */
    wcsin->sysout = wcsout->syswcs;
    strcpy (wcsin->radecout, wcsout->radecsys);
    wpin = wcsin->nxpix;
    hpin = wcsin->nypix;

    sprintf (history, "REMAP input file %s", filename);
    hputc (headout, "HISTORY", history);

    /* Find limiting edges of input image in output image */
    if (hgets (header, "DATASEC", 32, secstring)) {
	getsection (secstring, wpin, hpin, &xin1, &xin2, &yin1, &yin2);
	if (verbose)
	    printf ("REMAP: Input file %d x: %d-%d, y: %d-%d\n", ifile,
		(int)(xin1+0.5), (int)(xin2+0.5), (int)(yin1+0.5),
		(int)(yin2+0.5));
	}
    else {
	xin1 = 1.0;
	xin2 = (double) wpin;
	yin1 = 1.0;
	yin2 = (double) hpin;
	}
    pix2wcs (wcsin, xin1, yin1, &xpos, &ypos);
    wcscon (wcsin->syswcs,wcsout->syswcs,wcsin->equinox,wcsout->equinox,
	    &xpos,&ypos,wcsin->epoch);
    wcs2pix (wcsout, xpos, ypos, &xout, &yout, &offscl);
    xmin = xout;
    xmax = xout;
    ymin = yout;
    ymax = yout;
    pix2wcs (wcsin, xin1, yin2, &xpos, &ypos);
    wcscon (wcsin->syswcs,wcsout->syswcs,wcsin->equinox,wcsout->equinox,
	    &xpos,&ypos,wcsin->epoch);
    wcs2pix (wcsout, xpos, ypos, &xout, &yout, &offscl);
    if (xout < xmin) xmin = xout;
    if (xout > xmax) xmax = xout;
    if (yout < ymin) ymin = yout;
    if (yout > ymax) ymax = yout;
    pix2wcs (wcsin, xin2, yin1, &xpos, &ypos);
    wcscon (wcsin->syswcs,wcsout->syswcs,wcsin->equinox,wcsout->equinox,
	    &xpos,&ypos,wcsin->epoch);
    wcs2pix (wcsout, xpos, ypos, &xout, &yout, &offscl);
    if (xout < xmin) xmin = xout;
    if (xout > xmax) xmax = xout;
    if (yout < ymin) ymin = yout;
    if (yout > ymax) ymax = yout;
    pix2wcs (wcsin, xin2, yin2, &xpos, &ypos);
    wcscon (wcsin->syswcs,wcsout->syswcs,wcsin->equinox,wcsout->equinox,
	    &xpos,&ypos,wcsin->epoch);
    wcs2pix (wcsout, xpos, ypos, &xout, &yout, &offscl);
    if (xout < xmin) xmin = xout;
    if (xout > xmax) xmax = xout;
    if (yout < ymin) ymin = yout;
    if (yout > ymax) ymax = yout;
    iout1 = (int) (ymin + 0.5);
    if (iout1 < 1) iout1 = 1;
    iout2 = (int) (ymax + 0.5);
    if (iout2 > hpout) iout2 = hpout;
    jout1 = (int) (xmin + 0.5);
    if (jout1 < 1) jout1 = 1;
    jout2 = (int) (xmax + 0.5);
    if (jout2 > wpout) jout2 = wpout;

    if (verbose)
	printf ("REMAP: Output x: %d-%d, y: %d-%d\n",
		jout1,jout2,iout1,iout2);

    /* Eliminate rescaling if input and output scaling is the same */
    if (bzin == bzout && bsin == bsout) {
	if (bzin != 0.0 && bsin != 1.0)
	    addscale = 1;
	else
	    addscale = 0;
	blankpix = (blankpix + bzin) / bsin;
	setscale (0);
	}

    dxout = (double *) calloc (remappix, sizeof (double));
    dyout = (double *) calloc (remappix, sizeof (double));
    if (remappix > 1) {
	dpix = 1.0 / (double) remappix;
	dx = -0.5 + (0.5 * dpix);
	for (idiff = 0; idiff < remappix; idiff++) {
	    dxout[idiff] = dx;
	    dyout[idiff] = dx;
	    dx = dx + dpix;
	    }
	}
    else {
	dxout[0] = 0.0;
	dyout[0] = 0.0;
	}

    /* Loop through vertical pixels (output image lines) */
    for (iout = iout1; iout <= iout2; iout++) {
	yout0 = (double) iout;

	/* Loop through horizontal pixels (output image columns) */
	for (jout = jout1; jout <= jout2; jout++) {
	    xout0 = (double) jout;

	    /* Read pixel from output file */
	    dpixo = getpix1 (imout,bitpixout,wpout,hpout,bzout,bsout,jout,iout);
	    dpix = 0.0;
	    dnpix = 0.0;

	    for (idiff = 0; idiff < remappix; idiff++) {
		xout = xout0 + dxout[idiff];

		for (jdiff = 0; jdiff < remappix; jdiff++) {
		    yout = yout0 + dyout[idiff];

		    /* Get WCS coordinates of this pixel in output image */
		    pix2wcs (wcsout, xout, yout, &xpos, &ypos);
		    if (!wcsout->offscl) {

			/* Convert to output coordinate system */
			wcscon (wcsin->syswcs,wcsout->syswcs,wcsin->equinox,wcsout->equinox,
				&xpos,&ypos,wcsin->epoch);

			/* Get image coordinates of this subpixel in input image */
			wcs2pix (wcsin, xpos, ypos, &xin, &yin, &offscl);
			if (!offscl) {
			    iin = (int) (yin + 0.5);
			    jin = (int) (xin + 0.5);

			    /* Read pixel from input file */
			    dpixi = getpix1 (image,bitpix,wpin,hpin,bzin,bsin,jin,iin);
			    if (dpixi != blankpix) {
				dpix = dpix + dpixi;
				dnpix = dnpix + 1.0;
				}
			    }
			}
		    }
		if (dnpix > 0.0)
		    dpix = dpix / dnpix;
		else
		    dpix = blankpix;

		/* If output pixel is blank, set rather than add */
		if (dpixo == blankpix) {
		    putpix1 (imout,bitpixout,wpout,hpout,bzout,bsout,jout,iout,dpix);
		    }

		/* Otherwise add to current pixel value and write to output image */
		else {
		    if (addscale)
			dpix = (dpix - bzin);
		    dpixo = dpixo + dpix;
		    putpix1 (imout,bitpixout,wpout,hpout,bzout,bsout,jout,iout,dpixo);
		    }
		}
	    }

	if (nlog > 0 && iout%nlog == 0)
	    fprintf (stderr,"REMAP: Output image line %04d / %04d filled from %d / %d.\r",
		     iout, iout2, iin, hpin);
	}
    if (nlog > 0)
	printf ("\n");

    free (header);
    free (image);
    wcsfree (wcsin);
    return (0);
}


static void
getsection (section, nx, ny, x1, x2, y1, y2)

char	*section;	/* Header value string for *SEC keyword */
int	nx;		/* Dimension in X */
int	ny;		/* Dimension in Y */
double	*x1;		/* Lower left x coordinate (returned) */
double	*x2;		/* Lower right x coordinate (returned) */
double	*y1;		/* Lower left y coordinate (returned) */
double	*y2;		/* Upper right y coordinate (returned) */

{
    char *next;
    *x1 = 1.0;
    next = section;
    if (*next == '[') next++;
    *x1 = atof (next);
    *x2 = (double) nx;
    if ((next = strchr (next, ':')) != NULL)
	*x2 = atof (next + 1);
    *y1 = 1.0;
    if ((next = strchr (next, ',')) != NULL)
	*y1 = atof (next + 1);
    *y2 = (double) ny;
    if ((next = strchr (next, ':')) != NULL)
	*y2 = atof (next + 1);
    return;
}


/* Sep 28 1999	New program
 * Oct 22 1999	Drop unused variables after lint
 * Oct 22 1999	Add optional second plate scale argument
 * Nov 19 1999	Add galactic coordinate output option
 * Dec  3 1999	Add option to set output BITPIX
 *
 * Jan 28 2000	Call setdefwcs() with WCS_ALT instead of 1
 * Mar 23 2000	Use hgetm() to get the IRAF pixel file name, not hgets()
 *
 * Apr 13 2001	Set default center to center of output image, not input
 * Jul  2 2001	Fix recentering bug
 * Jul  5 2001	Simplify output by just grabbing the closest input pixel
 * Jul  9 2001	Write projection type into CRVALn of output file
 * Jul 10 2001	Fix -w help message so it correctly notes input proj is default
 * Jul 10 2001	Set CTYPEn correctly for ecliptic or galactic output
 *
 * Apr  9 2002	Do not free unallocated header
 *
 * May 23 2003	Add -e for ecliptic coordinates
 * Aug 14 2003	Add option to read WCS from FITS or IRAF image file using -f
 * Aug 18 2003	Add -n option to set BLANK pixel value
 * Nov 18 2003	Fix error returns in RemapImage() to always return 1
 *
 * Jan 15 2004	Add -u to delete distortion keywords from output file
 * Jan 23 2004	Finish implementing blank pixel setting
 * Feb 27 2004	Add -s option to use BZERO and BSCALE in output image
 * Feb 27 2004	Fix bugs in BLANK initialization; allow multiple input files
 * Feb 27 2004	Use DATASEC to limit input data coordinates if it is present
 * Mar  1 2004	Do not rescale pixels if unnecessary
 * Apr 28 2004	Return error on failure of any memory allocation
 * Aug 30 2004	Add multiple samples from output to input images
 * Oct 12 2004	Fix message if writing to named file and print only if verbose
 *
 * Apr  6 2006	Convert between coordinate systems if requested
 * Apr 19 2006	Check to see if output file exists before reading its header
 * Jun 21 2006	Clean up code
 * Jun 22 2006	Fix bug checking for pre-existing output file
 * Aug 16 2006	Check for output image off-scale as well as input image
 * Sep 11 2006	Cleanup scaling
 * Sep 26 2006	Increase length of rastr and destr from 16 to 32
 *
 * Jan 10 2007	Drop unused variable dy
 */
