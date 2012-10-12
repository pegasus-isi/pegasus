/* File imresize.c
 * June 12, 2007
 * By Doug Mink, Harvard-Smithsonian Center for Astrophysics
 * Send bug reports to dmink@cfa.harvard.edu

   Copyright (C) 2006-2007 
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

#define MEDIAN 1
#define MEAN 2
#define GAUSSIAN 3

static void usage();
static void imResize();
extern char *FiltFITS();
extern char *ShrinkFITSHeader();
extern char *ShrinkFITSImage();
extern void setghwidth();

#define MAXFILES 1000
static int maxnfile = MAXFILES;

static int verbose = 0;		/* verbose/debugging flag */
static char outname[128];	/* Name for output image */
static int fitsout = 0;		/* Output FITS file from IRAF input if 1 */
static int overwrite = 0;	/* allow overwriting of input image file */
static int version = 0;		/* If 1, print only program name and version */
static int filter = 0;		/* Filter code */
static int xsize = 3;		/* Filter box width */
static int ysize = 3;		/* Filter box height */
static int resize = 0;		/* Resize flag */
static int nlog = 100;		/* Number of lines between log messages */
static int xfactor = 0;		/* Horizontal axis reduction factor */
static int yfactor = 0;		/* Vertical axis reduction factor */
static double ghwidth = 1.0;	/* Gaussian half width for smoothing */
static int bitpix = 0;		/* Bits per output pixel */
static int mean = 0;		/* 1 if mean for regrouped pixels */
static int northup = 0;		/* 1 to rotate to north up, east left */

static char *RevMsg = "IMRESIZE WCSTools 3.8.1, 14 December 2009, Doug Mink (dmink@cfa.harvard.edu)";

int
main (ac, av)
int ac;
char **av;
{
    char *str;
    char c;
    int readlist = 0;
    char *lastchar;
    char filename[128];
    FILE *flist;
    char **fn, *fname;
    char *listfile = NULL;
    int lfn, ifile, nfile;

    nfile = 0;
    fn = (char **)calloc (maxnfile, sizeof(char *));
    outname[0] = 0;

    /* Check for help or version command first */
    str = *(av+1);
    if (!str || !strcmp (str, "help") || !strcmp (str, "-help"))
	usage();
    if (!strcmp (str, "version") || !strcmp (str, "-version")) {
	version = 1;
	usage();
	}

    /* Loop through the arguments */
    for (av++; --ac > 0; av++) {
	str = *av;

	/* List of files to be read */
	if (*str == '@') {
	    readlist++;
	    listfile = ++str;
	    str = str + strlen (str) - 1;
	    av++;
	    ac--;
	    str = str - 1;
	    }

	/* Parameters */
	else if (str[0] == '-') {
	    while ((c = *++str) != 0) {
		switch (c) {

		case 'a':	/* Mean filter (average) */
		    if (ac < 3)
			usage ();
		    filter = MEAN;
		    xsize = (int) atof (*++av);
		    ac--;
		    ysize = (int) atof (*++av);
		    ac--;
		    break;

		case 'b': /* Number of output bits per pixel */
		    if (ac < 2)
			usage ();
		    bitpix = (int) atof (*++av);
		    ac--;
		    break;

		case 'f':	/* Horizontal and vertical reduction factor */
		    if (ac < 2)
			usage ();
		    resize = 1;
		    xfactor = (int) atof (*++av);
		    yfactor = xfactor;
		    ac--;
		    break;

		case 'g':	/* Gaussian filter */
		    if (ac < 2)
			usage ();
		    filter = GAUSSIAN;
		    xsize = (int) atof (*++av);
		    ysize = xsize;
		    ac--;
		    break;

		case 'h':	/* Gaussian filter half-width at half-height */
		    if (ac < 2)
			usage ();
		    ghwidth = atof (*++av);
		    setghwidth (ghwidth);
		    ac--;
		    break;

		case 'l': /* Number of lines to log */
		    if (ac < 2)
			usage ();
		    nlog = (int) atof (*++av);
		    ac--;
		    break;

		case 'm': /* Median filter */
		    if (ac < 3)
			usage ();
		    filter = MEDIAN;
		    xsize = (int) atof (*++av);
		    ac--;
		    ysize = (int) atof (*++av);
		    ac--;
		    break;

		case 'n': /* Rotate to north up, east left */
		    northup++;
		    break;

		case 'o':	/* Specifiy output image filename */
		    if (ac < 2)
			usage ();
		    if (*(av+1)[0] == '-' || *(str+1) != (char)0)
			overwrite++;
		    else {
			strcpy (outname, *(av+1));
			overwrite = 0;
			av++;
			ac--;
			}
		    break;

		case 'v':	/* more verbosity */
		    verbose++;
		    break;

		case 'x':	/* Horizontal reduction factor */
		    if (ac < 2)
			usage ();
		    resize = 1;
		    xfactor = (int) atof (*++av);
		    ac--;
		    break;

		case 'y':	/* Reduction factor */
		    if (ac < 2)
			usage ();
		    resize = 1;
		    yfactor = (int) atof (*++av);
		    ac--;
		    break;

		default:
		    usage();
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
	    fprintf (stderr,"IMRESIZE: %s is not a FITS or IRAF file \n",str);
            usage();
            }

    }

    if (xfactor == 0 && yfactor != 0)
	xfactor = yfactor;
    if (xfactor != 0 && yfactor == 0)
	yfactor = xfactor;

    /* Process files in file of filenames */
    if (readlist) {
	if ((flist = fopen (listfile, "r")) == NULL) {
	    fprintf (stderr,"IMRESIZE: List file %s cannot be read\n",
		     listfile);
	    usage ();
	    }
	while (fgets (filename, 128, flist) != NULL) {
	    lastchar = filename + strlen (filename) - 1;
	    if (*lastchar < 32) *lastchar = 0;
	    imResize (filename);
	    if (verbose)
		printf ("\n");
	    }
	fclose (flist);
	}

    /* Process files on command line */
    else if (nfile > 0) {
	for (ifile = 0; ifile < nfile; ifile++) {
	    fname = fn[ifile];
	    lfn = strlen (fname);
	    if (lfn < 8)
		lfn = 8;
	    else {
		if (verbose)
		    printf ("%s:\n", fname);
		imResize (fname);
		if (verbose)
  		    printf ("\n");
		}
	    }
	}

    /* If no files processed, print usage */
    else
	usage ();

    return (0);
}

static void
usage ()
{
    fprintf (stderr,"%s\n",RevMsg);
    if (version)
	exit (-1);
    fprintf (stderr,"Resize FITS and IRAF image files\n");
    fprintf(stderr,"Usage: [-v][-a dx[,dy]][-g dx[,dy]][-m dx[,dy]] file.fits ...\n");
    fprintf(stderr,"  -a dx dy: Mean filter dx x dy pixels\n");
    fprintf(stderr,"  -b bitpix: FITS bits per pixel in output image\n");
    fprintf(stderr,"  -f factor: Reduce both image dimensions by factor\n");
    fprintf(stderr,"  -g dx: Gaussian filter dx pixels square\n");
    fprintf(stderr,"  -h halfwidth: Gaussian half-width at half-height\n");
    fprintf(stderr,"  -l num: Logging interval in lines\n");
    fprintf(stderr,"  -m dx dy: Median filter dx x dy pixels\n");
    fprintf(stderr,"  -n: Rotate to North Up East Left\n");
    fprintf(stderr,"  -o: Allow overwriting of input image, else write new one\n");
    fprintf(stderr,"  -v: Verbose\n");
    fprintf(stderr,"  -x factor: Reduce image horizontal dimension by factor\n");
    fprintf(stderr,"  -y factor: Reduce image vertical dimension by factor\n");
    exit (1);
}

static void
imResize (name)
char *name;
{
    char *image;		/* FITS image */
    char *header;		/* FITS header */
    int lhead;			/* Maximum number of bytes in FITS header */
    int nbhead;			/* Actual number of bytes in FITS header */
    int iraffile;		/* 1 if IRAF image */
    char *irafheader = NULL;		/* IRAF image header */
    char newname[256];		/* Name for revised image */
    char *ext = NULL;
    char *newimage = NULL;
    char *imext = NULL;
    char *imext1 = NULL;
    char *fname;
    char *newhead;
    char extname[16];
    int lext = 0;
    int lroot;
    double rotang;
    char echar;
    char temp[16];
    char history[64];
    char pixname[256];
    char *RotFITS();
    struct WorldCoor *wcs;

    /* If not overwriting input file, make up a name for the output file */
    if (!overwrite) {
	fname = strrchr (name, '/');
	if (fname)
	    fname = fname + 1;
	else
	    fname = name;
	ext = strrchr (fname, '.');
	if (ext != NULL) {
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
	}
    else
	strcpy (newname, name);

    /* Open IRAF image */
    if (isiraf (name)) {
	iraffile = 1;
	if ((irafheader = irafrhead (name, &lhead)) != NULL) {
	    if ((header = iraf2fits (name, irafheader, lhead, &nbhead)) == NULL) {
		fprintf (stderr, "Cannot translate IRAF header %s/n",name);
		free (irafheader);
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
	    fprintf (stderr, "Cannot read IRAF header file %s\n", name);
	    return;
	    }
	}

    /* Open FITS file */
    else {
	iraffile = 0;
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
	}

    /* Use output filename if it is set on the command line */
    if (outname[0] > 0)
	strcpy (newname, outname);

    /* Create new file name */
    else if (!overwrite) {
	if (imext != NULL) {
	    if (hgets (header, "EXTNAME",8,extname)) {
		strcat (newname, ".");
		strcat (newname, extname);
		}
	    else {
		strcat (newname, "_");
		strcat (newname, imext+1);
		}
	    }
	if (filter == MEDIAN)
	    strcat (newname, "m");
	else if (filter == GAUSSIAN)
	    strcat (newname, "g");
	else if (filter == MEAN)
	    strcat (newname, "a");

	if (resize) {
	    strcat (newname, "s");
	    if (xfactor < 10)
		sprintf (temp,"%1d",xfactor);
	    else if (xfactor < 100)
		sprintf (temp,"%2d",xfactor);
	    else
		sprintf (temp,"%d",xfactor);
	    strcat (newname, temp);
	    strcat (newname, "x");
	    if (yfactor < 10)
		sprintf (temp,"%1d",yfactor);
	    else if (yfactor < 100)
		sprintf (temp,"%2d",yfactor);
	    else
		sprintf (temp,"%d",yfactor);
	    strcat (newname, temp);
	    }
	if (filter) {
	    if (xsize < 10 && xsize > -1)
		sprintf (temp,"%1d",xsize);
	    else if (xsize < 100 && xsize > -10)
		sprintf (temp,"%2d",xsize);
	    else if (xsize < 1000 && xsize > -100)
		sprintf (temp,"%3d",xsize);
	    else
		sprintf (temp,"%4d",xsize);
	    strcat (newname, temp);
	    strcat (newname, "x");
	    if (ysize < 10 && ysize > -1)
		sprintf (temp,"%1d",ysize);
	    else if (ysize < 100 && ysize > -10)
		sprintf (temp,"%2d",ysize);
	    else if (ysize < 1000 && ysize > -100)
		sprintf (temp,"%3d",ysize);
	    else
		sprintf (temp,"%4d",ysize);
	    strcat (newname, temp);
	    }
	if (fitsout)
	    strcat (newname, ".fits");
	else if (lext > 0) {
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
	strcpy (newname, name);

    if (verbose) {
	fprintf (stderr,"%s\n",RevMsg);
	if (resize)
	    fprintf (stderr,"%d x %d dimension reduction ", xfactor, yfactor);
	if (filter == GAUSSIAN)
	    fprintf (stderr,"%d x %d Gaussian Smooth ", xsize, ysize);
	else if (filter == MEDIAN)
	    fprintf (stderr,"%d x %d Median Smooth ", xsize, ysize);
	else if (filter == MEAN)
	    fprintf (stderr,"%d x %d Mean Smooth ", xsize, ysize);
	if (iraffile)
	    fprintf (stderr,"IRAF image file %s", name);
	else
	    fprintf (stderr,"FITS image file %s", name);
	fprintf (stderr, " -> %s\n", newname);
	}

    /* Resize image, if requested */
    if (resize) {
	if ((newhead = ShrinkFITSHeader (name, header, xfactor, yfactor, mean, bitpix)) == NULL)
	    fprintf (stderr,"Cannot make new image header for %s; file is unchanged.\n",newname);
	else if ((newimage = ShrinkFITSImage (header, image, xfactor, yfactor,
					      mean, bitpix, nlog)) == NULL) {
	    fprintf (stderr,"Cannot shrink image %s.\n",name);
	    free (newhead);
	    }
	else {
	    free (image);
	    image = newimage;
	    free (header);
	    header = newhead;
	    if (verbose)
		printf ("IMRESIZE: File %s has been resized %d x %d\n",
			 newname, xfactor, yfactor);
	    /* Add IMRESIZE keyword to say how image was changed */
	    if (hgets (header, "IMRESIZE", 63, history))
		hputs (header, "HISTORY", history);
	    sprintf (history, "Image size reduced by %d in x, %d in y",
		     xfactor, yfactor);
	    hputs (header, "IMRESIZE", history);
	    }
	}

    /* Rotate image to north up, east left, if requested */
    if (northup) {
	wcs = wcsinit (header);
	if (wcs != NULL) {
	    if (wcs->rot >= 45.0 && wcs->rot < 135.0)
		rotang = 270.0;
	    else if (wcs->rot >= 135.0 && wcs->rot < 225.0)
		rotang = 180.0;
	    else if (wcs->rot >= 225.0 && wcs->rot < 315.0)
		rotang = 90.0;
	    else
		rotang = 0.0;
	    newimage = RotFITS (name,header,image,0,0,rotang,0,bitpix,1,verbose);
	    }
	}

    if (filter) {


	if ((newimage = FiltFITS (header,image,filter,xsize,ysize,nlog)) == NULL)
	    fprintf (stderr,"Cannot filter image %s; file is unchanged.\n",name);
	else {
	    free (image);
	    image = newimage;

	    /* Add IMSMOOTH keyword to say how image was changed */
	    if (hgets (header, "IMSMOOTH", 63, history))
		hputs (header, "HISTORY", history);
	    if (filter == MEDIAN)
		sprintf (history, "Median filtered over %d x %d pixels",xsize,ysize);
	    else if (filter == GAUSSIAN)
		sprintf (history, "Gaussian halfwidth %.2f pixels filtered over %d x %d pixels",ghwidth,xsize,ysize);
	    else
		sprintf (history, "Mean filtered over %d x %d pixels",xsize,ysize);
	    hputs (header, "IMSMOOTH", history);
	    }
	}
 
    if (iraffile && !fitsout) {
	if (irafwimage (newname,lhead,irafheader,header,image) > 0) {
	    if (verbose)
		printf ("%s: written successfully.\n", newname);
	    else
		printf ("%s\n", newname);
	    }
	else if (verbose)
	    printf ("IMRESIZE: File %s not written.\n", newname);
	}
    else if (fitswimage (newname, header, image) > -1) {
	if (verbose)
	    printf ("%s: written successfully.\n", newname);
	else
	    printf ("%s\n", newname);
	}
    else if (verbose) {
	printf ("IMRESIZE: File %s not written.\n", newname);
	free (newimage);
	}

    free (header);
    if (iraffile)
	free (irafheader);
    free (image);
    return;
}
/* Apr 19 2006	New program from imsmooth.c
 * Jun 21 2006	Write keywords IMRESIZE and IMSMOOTH to header describing action
 * Jun 21 2006	Clean up code
 * Sep 25 2006	Add -f to reduce both dimensions by the same factor
 *
 * Jan  5 2007	Add string length argument to hgets() call
 * Jun 12 2007	Add -n option to rotate WCS-ed image to north up, east left
 */
