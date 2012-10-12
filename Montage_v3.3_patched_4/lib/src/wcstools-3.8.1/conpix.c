/* File conpix.c
 * July 5, 2006
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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>
#include <math.h>
#include "libwcs/fitsfile.h"
#include "libwcs/wcs.h"

#define PIX_ADD	1
#define PIX_SUB	2
#define PIX_MUL	3
#define PIX_DIV	4
#define PIX_SET 5
#define PIX_SQRT 6
#define PIX_NOISE 7
#define PIX_ADDNOISE 8
#define PIX_LOG10 9

static void usage();
static void OpPix();
static double gnoise();

static int nlog = 0;		/* Logging frequency */
static int newimage = 0;
static int verbose = 0;		/* verbose flag */
static int version = 0;		/* If 1, print only program name and version */
static int setgnoise = 0;	/* If 1, pixels have been set to random noise */
static int addgnoise = 0;	/* If 1, pixels have random noise added */

static char *RevMsg = "CONPIX WCSTools 3.8.1, 14 December 2009, Doug Mink (dmink@cfa.harvard.edu)";

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
    int nop, op[10];
    double opcon[10];

    /* Check for help or version command first */
    str = *(av+1);
    if (!str || !strcmp (str, "help") || !strcmp (str, "-help"))
	usage();
    if (!strcmp (str, "version") || !strcmp (str, "-version")) {
	version = 1;
	usage();
	}

    nop = 0;

    /* crack arguments */
    for (av++; --ac > 0 && *(str = *av) == '-'; av++) {
	char c;
	while ((c = *++str))
	switch (c) {

	case 'a':	/* add constant to all pixels */
	    op[nop] = PIX_ADD;
            if (ac < 2)
                usage ();
	    if (*(av+1)[0] == 'g') {
		addgnoise++;
		op[nop++] = PIX_ADDNOISE;
		newimage++;
		av++;
		}
	    else
		opcon[nop++] = atof (*++av);
	    ac--;
	    break;

	case 'c':	/* Set all pixels to a constant value */
	    op[nop] = PIX_SET;
            if (ac < 2)
                usage ();
            opcon[nop++] = atof (*++av);
	    ac--;
	    break;

	case 'd':	/* divide all pixels by constant */
	    op[nop] = PIX_DIV;
            if (ac < 2)
                usage ();
            opcon[nop++] = atof (*++av);
	    ac--;
	    break;

	case 'g':	/* Gaussian noise for square root of pixel value */
	    op[nop] = PIX_NOISE;
	    nop++;
	    newimage++;
	    setgnoise++;
	    break;

	case 'i':	/* Logging frequency */
            if (ac < 2)
                usage ();
            nlog = atoi (*++av);
	    ac--;
	    break;

	case 'l':	/* Log base 10 of pixel values */
	    op[nop] = PIX_LOG10;
	    nop++;
	    newimage++;
	    break;

	case 'm':	/* multiply all pixels by constant */
	    op[nop] = PIX_MUL;
            if (ac < 2)
                usage ();
            opcon[nop++] = atof (*++av);
	    ac--;
	    break;

	case 'n':	/* ouput new file */
	    newimage++;
	    break;

	case 'r':	/* take square root of all pixels */
	    op[nop] = PIX_SQRT;
	    nop++;
	    break;

	case 's':	/* subtract constant from all pixels */
	    op[nop] = PIX_SUB;
            if (ac < 2)
                usage ();
            opcon[nop++] = atof (*++av);
	    ac--;
	    break;

	case '@':       /* List of files to be read */
	    readlist++;
	    listfile = ++str;
	    str = str + strlen (str) - 1;
	    av++;
	    ac--;

	case 'v':	/* more verbosity */
	    verbose++;
	    break;

	default:
	    usage ();
	    break;
	}
    }

    /* Process files in file of filenames */
    if (readlist) {
	if ((flist = fopen (listfile, "r")) == NULL) {
	    fprintf (stderr,"CONPIX: List file %s cannot be read\n",
		     listfile);
	    usage ();
	    }
	while (fgets (filename, 128, flist) != NULL) {
	    lastchar = filename + strlen (filename) - 1;
	    if (*lastchar < 32) *lastchar = 0;
	    OpPix (filename, nop, op, opcon);
	    if (verbose)
		printf ("\n");
	    }
	fclose (flist);
	}

    /* If no arguments left, print usage */
    if (ac == 0)
	usage ();

    /* Process files on command line */
    else {
	while (ac-- > 0) {
    	    char *fn = *av++;
    	    if (verbose)
    		printf ("%s:\n", fn);
	    OpPix (fn, nop, op, opcon);
    	    if (verbose)
    		printf ("\n");
	    }
	}

    return (0);
}

static void
usage ()
{
    fprintf (stderr,"%s\n",RevMsg);
    if (version)
	exit (-1);
    fprintf (stderr,"Operate on all pixels of a FITS or IRAF image file\n");
    fprintf(stderr,"Usage: conpix [-vnpr][-asmd constant][-l num] file.fits ...\n");
    fprintf(stderr,"  or : conpix [-vnpr][-asmd constant][-l num] @filelist\n");
    fprintf(stderr,"  -a: add constant to all pixels (g=noise)\n");
    fprintf(stderr,"  -d: divide all pixels by constant\n");
    fprintf(stderr,"  -g: Gaussian noise from each pixel\n");
    fprintf(stderr,"  -i: logging interval (default = 10)\n");
    fprintf(stderr,"  -l: log10 of each pixel\n");
    fprintf(stderr,"  -m: multiply all pixels by constant\n");
    fprintf(stderr,"  -n: write new file, else overwrite\n");
    fprintf(stderr,"  -r: square root of each pixel\n");
    fprintf(stderr,"  -s: subtract constant from all pixels\n");
    fprintf(stderr,"  -v: verbose\n");
    exit (1);
}


static void
OpPix (filename, nop, op, opcon)

char	*filename;	/* FITS or IRAF file filename */
int	nop;		/* Number of pixels to change */
int	*op;		/* List of operations to perform */
double	*opcon;		/* Constants for operations */

{
    char *image;		/* FITS image */
    char *header;		/* FITS header */
    int lhead;			/* Maximum number of bytes in FITS header */
    int nbhead;			/* Actual number of bytes in FITS header */
    int iraffile;		/* 1 if IRAF image */
    char *irafheader = NULL;	/* IRAF image header */
    int lext, lroot;
    char *imext, *imext1;
    char newname[256];
    char pixname[256];
    char tempname[256];
    char history[64];
    char *ext, *fname;
    char echar;
    double *imvec, *dvec, *endvec;
    int bitpix, xdim, ydim, y, pixoff, iop;
    double bzero;		/* Zero point for pixel scaling */
    double bscale;		/* Scale factor for pixel scaling */

    strcpy (tempname, "fitshead.temp");

    /* Open IRAF image and header */
    if (isiraf (filename)) {
	iraffile = 1;
	if ((irafheader = irafrhead (filename, &lhead)) != NULL) {
	    if ((header = iraf2fits (filename, irafheader, lhead, &nbhead)) == NULL) {
		free (irafheader);
                fprintf (stderr, "Cannot translate IRAF header %s/n", filename);
                return;
                }
	    if ((image = irafrimage (header)) == NULL) {
		hgetm (header,"PIXFIL", 255, pixname);
		fprintf (stderr, "Cannot read IRAF pixel file %s\n", pixname);
		free (irafheader);
		free (header);
		return;
		}
	    }
	else {
	    fprintf (stderr, "Cannot read IRAF header file %s\n", filename);
	    return;
	    }
	}

    /* Read FITS image and header */
    else {
	iraffile = 0;
	if ((header = fitsrhead (filename, &lhead, &nbhead)) != NULL) {
	    if ((image = fitsrimage (filename, nbhead, header)) == NULL) {
		fprintf (stderr, "Cannot read FITS image %s\n", filename);
		free (header);
		return;
		}
	    }
	else {
	    fprintf (stderr, "Cannot read FITS file %s\n", filename);
	    return;
	    }
	}
    if (verbose)
	fprintf (stderr,"%s\n",RevMsg);

    /* Add specified value to specified pixel */
    hgeti4 (header,"BITPIX",&bitpix);
    xdim = 1;
    hgeti4 (header,"NAXIS1",&xdim);
    ydim = 1;
    hgeti4 (header,"NAXIS2",&ydim);
    bzero = 0.0;
    hgetr8 (header,"BZERO",&bzero);
    bscale = 1.0;
    hgetr8 (header,"BSCALE",&bscale);

    if (!(imvec = (double *) calloc (xdim, sizeof (double))))
	return;
    endvec = imvec + xdim;

    pixoff = 0;
    for (y = 0; y < ydim; y++) {
	getvec (image, bitpix, bzero, bscale, pixoff, xdim, imvec);
	for (iop = 0; iop < nop; iop++) {
	    double dpix = opcon[iop];
	    switch (op[iop]) {
		case PIX_ADD:
		    for (dvec = imvec; dvec < endvec; dvec++)
			*dvec = *dvec + dpix;
		    break;
		case PIX_SUB:
		    for (dvec = imvec; dvec < endvec; dvec++)
			*dvec = *dvec - dpix;
		    break;
		case PIX_MUL:
		    for (dvec = imvec; dvec < endvec; dvec++)
			*dvec = *dvec * dpix;
		    break;
		case PIX_DIV:
		    for (dvec = imvec; dvec < endvec; dvec++)
			*dvec = *dvec / dpix;
		    break;
		case PIX_SET:
		    for (dvec = imvec; dvec < endvec; dvec++)
			*dvec = dpix;
		    break;
		case PIX_SQRT:
		    for (dvec = imvec; dvec < endvec; dvec++)
			*dvec = sqrt (*dvec);
		    break;
		case PIX_NOISE:
		    for (dvec = imvec; dvec < endvec; dvec++)
			*dvec = gnoise (*dvec);
		    break;
		case PIX_ADDNOISE:
		    for (dvec = imvec; dvec < endvec; dvec++)
			*dvec = *dvec + gnoise (*dvec);
		    break;
		case PIX_LOG10:
		    for (dvec = imvec; dvec < endvec; dvec++) {
			if (*dvec > 0.0)
			    *dvec = log10 (*dvec);
			}
		    break;
		default:
		    break;
		}
	    }
	putvec (image, bitpix, bzero, bscale, pixoff, xdim, imvec);
	pixoff = pixoff + xdim;
	if (nlog > 0 && y % nlog == 0) {
	    fprintf (stderr, "Row %4d operations complete\r", y);
	    }
	}
    if (verbose)
	fprintf (stderr,"\n");

    /* Note operation as history line in header */
    for (iop = 0; iop < nop; iop++) {
	double dpix = opcon[iop];
	if (bitpix > 0) {
	    int ipix = (int)dpix;
	    switch (op[iop]) {
		case PIX_ADD:
		    sprintf (history, "CONPIX: %d added to all pixels", ipix);
		    break;
		case PIX_SUB:
		    sprintf (history,
			    "CONPIX: %d subtracted from all pixels", ipix);
		    break;
		case PIX_MUL:
		    sprintf (history,
			     "CONPIX: all pixels multiplied by %d", ipix);
		    break;
		case PIX_DIV:
		    sprintf (history,
			     "CONPIX: all pixels divided by %d", ipix);
		    break;
		case PIX_SET:
		    sprintf (history,
			     "CONPIX: all pixels set to %d", ipix);
		    break;
		case PIX_SQRT:
		    sprintf (history,
			     "CONPIX: all pixels replaced by their square root");
		    break;
		case PIX_NOISE:
		    sprintf (history,
			     "CONPIX: all pixels replaced by Gaussian noise");
		    break;
		case PIX_ADDNOISE:
		    sprintf (history,
			     "CONPIX: Gaussian noise added to all pixels");
		    break;
		case PIX_LOG10:
		    sprintf (history,
			     "CONPIX: all pixels replaced by log10 of their value");
		    break;
		}
	    }
	else if (dpix < 1.0 && dpix > -1.0) {
	    switch (op[iop]) {
		case PIX_ADD:
		    sprintf (history, "CONPIX: %f added to all pixels", dpix);
		    break;
		case PIX_SUB:
		    sprintf (history,
			    "CONPIX: %f subtracted from all pixels", dpix);
		    break;
		case PIX_MUL:
		    sprintf (history,
			     "CONPIX: all pixels multiplied by %f", dpix);
		    break;
		case PIX_DIV:
		    sprintf (history,
			     "CONPIX: all pixels divided by %f", dpix);
		    break;
		case PIX_SET:
		    sprintf (history,
			     "CONPIX: all pixels set to %f", dpix);
		    break;
		case PIX_SQRT:
		    sprintf (history,
			     "CONPIX: all pixels replaced by their square root");
		    break;
		case PIX_NOISE:
		    sprintf (history,
			     "CONPIX: all pixels replaced by Gaussian noise");
		    break;
		case PIX_ADDNOISE:
		    sprintf (history,
			     "CONPIX: Gaussian noise added to all pixels");
		    break;
		case PIX_LOG10:
		    sprintf (history,
			     "CONPIX: all pixels replaced by log10 of their value");
		    break;
		}
	    }
	else {
	    switch (op[iop]) {
		case PIX_ADD:
		    sprintf (history, "CONPIX: %.2f added to all pixels", dpix);
		    break;
		case PIX_SUB:
		    sprintf (history,
			    "CONPIX: %.2f subtracted from all pixels", dpix);
		    break;
		case PIX_MUL:
		    sprintf (history,
			     "CONPIX: all pixels multiplied by %.2f", dpix);
		    break;
		case PIX_DIV:
		    sprintf (history,
			     "CONPIX: all pixels divided by %.2f", dpix);
		    break;
		case PIX_SET:
		    sprintf (history,
			     "CONPIX: all pixels set to %.2f", dpix);
		    break;
		case PIX_SQRT:
		    sprintf (history,
			     "CONPIX: all pixels replaced by their square root");
		    break;
		case PIX_NOISE:
		    sprintf (history,
			     "CONPIX: all pixels replaced by Gaussian noise");
		    break;
		case PIX_ADDNOISE:
		    sprintf (history,
			     "CONPIX: Gaussian noise added to all pixels");
		    break;
		case PIX_LOG10:
		    sprintf (history,
			     "CONPIX: all pixels replaced by log10 of their value");
		    break;
		}
	    }
	hputc (header,"HISTORY",history);
	if (verbose)
	    printf ("%s\n", history);
	}

    /* Make up name for new FITS or IRAF output file */
    if (newimage) {

    /* Remove directory path and extension from file name */
	fname = strrchr (filename, '/');
	if (fname)
	    fname = fname + 1;
	else
	    fname = filename;
	ext = strrchr (fname, '.');
	if (ext != NULL) {
	    if (!strncmp (ext, ".fit", 4)) {
		if (!strncmp (ext-3, ".ms", 3))
		    ext = ext - 3;
		}
	    lext = (fname + strlen (fname)) - ext;
	    lroot = ext - fname;
	    strncpy (newname, fname, lroot);
	    *(newname + lroot) = 0;
	    }
	else {
	    lext = 0;
	    lroot = strlen (fname);
	    strcpy (newname, fname);
	    }
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
	if (fname)
	    fname = fname + 1;
	else
	    fname = filename;
	if (setgnoise)
	    strcat (newname, "g");
	else if (addgnoise)
	    strcat (newname, "ag");
	else
	    strcat (newname, "e");
	if (lext > 0) {
	    if (imext != NULL) {
		echar = *imext;
		*imext = (char) 0;
		strcat (newname, ext);
		*imext = echar;
		if (imext1 != NULL)
		    *imext1 = ']';
		}
	    else
		strcat (newname, ext);
	    }
	}
    else
	strcpy (newname, filename);

    /* Write fixed header to output file */
    if (iraffile) {
	if (irafwimage (newname,lhead,irafheader,header,image) > 0 && verbose) {
	    if (verbose)
		printf ("%s rewritten successfully.\n", newname);
	    else
		printf ("%s\n", newname);
	    }
	else if (verbose)
	    printf ("%s could not be written.\n", newname);
	free (irafheader);
	}
    else {
	if (fitswimage (newname, header, image) > 0) {
	    if (verbose)
		printf ("%s: rewritten successfully.\n", newname);
	    else
		printf ("%s\n", newname);
	    }
	else if (verbose)
	    printf ("%s could not be written.\n", newname);
	}

    free (image);
    free (header);
    return;
}

static int iset = 0;
static double gset;

/* Random noise drawn from a Gaussian distribution centered on zero */

static double
gnoise (flux)

double	flux;	/* Square root of this is 1/2.35 of Gaussian FWHM */

{
    double fac, rsq, v1, v2, sig;

    sig = sqrt (flux);

    if (iset == 0) {
	do {
	    v1 = 2.0 * ((double) random() / (pow (2.0, 31.0) - 1.0)) - 1.0;
	    v2 = 2.0 * ((double) random() / (pow (2.0, 31.0) - 1.0)) - 1.0;
/*	    v1 = 2.0 * drand48() - 1.0;
	    v2 = 2.0 * drand48() - 1.0; */
	    rsq = v1 * v1 + v2 * v2;
	    } while (rsq >= 1.0 || rsq == 0.0);

	fac = sqrt (-2.0 * log (rsq) / rsq);

	gset = sig * v1 * fac;
	iset = 1;
	return (sig * v2 * fac);
	}

    else {
	iset = 0;
	return (gset);
	}
}

/* Dec  2 1998	New program
 *
 * Feb 12 1999	Initialize dimensions to one so it works with 1-D images
 * Apr 29 1999	Add BZERO and BSCALE
 * Jun 17 1999	Finish adding BZERO and BSCALE
 * Jun 29 1999	Fix typo in BSCALE setting
 * Oct 21 1999	Drop unused variables after lint
 * Dec 14 1999	Add constant, square root, and Gaussian noise
 *
 * Feb  1 2000	Always write new file if Gaussian noise; add 'g' to filename
 * Feb  1 2000	Add option to add Gaussian noise; add 'ag' to filename
 * Mar 23 2000	Use hgetm() to get the IRAF pixel file name, not hgets()
 *
 * Nov 20 2001	Use random() instead of drand48() for portability
 *
 * Apr  9 2002	Do not free unallocated header
 *
 * Jun 21 2006	Clean up code
 * Jul  5 2006	Add option to take base 10 log of entire image
 */
