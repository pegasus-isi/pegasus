/* File imfill.c
 * January 10, 2007
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
#include "libwcs/fitsfile.h"

#define FILL_NONE 0
#define FILL_MEDIAN 1
#define FILL_MEAN 2
#define FILL_GAUSSIAN 3

static void usage();
static void imFill();
extern char *FillFITS();
extern char *SetBadFITS();
extern char *SetBadVal();
extern void setghwidth();
extern int getnfilled();
extern void setbadpix();

#define MAXFILES 1000
static int maxnfile = MAXFILES;

static int verbose = 0;		/* verbose/debugging flag */
static char outname[128];	/* Name for output image */
static int fitsout = 0;		/* Output FITS file from IRAF input if 1 */
static int overwrite = 0;	/* allow overwriting of input image file */
static int version = 0;		/* If 1, print only program name and version */
static int xsize = 3;
static int ysize = 3;
static int filter = 0;		/* Filter code */
static int nlog = 100;		/* Number of lines between log messages */
static double ghwidth;		/* Gaussian half-width */
static char badpixfile[128];	/* Bad pixel file (zeroes except bad pixels) */
static char *badimage;		/* FITS bad pixel image */
static char *badheader;		/* FITS bad pixel header */
static double minpixval = -9999.0;	/* Minimum good pixel value */
static double maxpixval = -9999.0;	/* Maximum good pixel value */

static char *RevMsg = "IMFILL WCSTools 3.8.1, 14 December 2009, Doug Mink (dmink@cfa.harvard.edu)";

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
    outname[0] = (char) 0;
    badpixfile[0] = (char) 0;
    badimage = NULL;
    badheader = NULL;
    filter = FILL_NONE;

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
			usage();
		    filter = FILL_MEAN;
		    xsize = (int) atof (*++av);
		    ac--;
		    ysize = (int) atof (*++av);
		    ac--;
		    break;

		case 'b': /* Bad pixel value */
		    if (ac < 2)
			usage();
		    setbadpix (atof (*++av));
		    ac--;
		    break;

		case 'f':	/* Bad pixel FITS file */
		    if (ac < 2)
			usage();
		    strcpy (badpixfile, *(av+1));
		    if (!isfits (badpixfile)) {
			fprintf (stderr,"%s is not a FITS file\n",badpixfile);
			usage();
			}
		    av++;
		    ac--;
		    break;

		case 'g':	/* Gaussian filter */
		    if (ac < 3)
			usage();
		    filter = FILL_GAUSSIAN;
		    xsize = (int) atof (*++av);
		    ac--;
		    ysize = (int) atof (*++av);
		    ac--;
		    break;

		case 'h':	/* Gaussian filter half-width at half-height */
		    if (ac < 2)
			usage();
		    ghwidth = atof (*++av);
		    setghwidth (ghwidth);
		    ac--;
		    break;

		case 'l': /* Number of lines to log */
		    if (ac < 2)
			usage();
		    nlog = (int) atof (*++av);
		    ac--;
		    break;

		case 'm': /* Median filter */
		    if (ac < 3)
			usage();
		    filter = FILL_MEDIAN;
		    xsize = (int) atof (*++av);
		    ac--;
		    ysize = (int) atof (*++av);
		    ac--;
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

		case 'p': /* Minimum and maximum good pixel values */
		    if (ac < 2)
			usage();
		    minpixval = atof (*++av);
		    ac--;
		    if (isnum (*(av+1))) {
			maxpixval = atof (*++av);
			ac--;
			}
		    break;

		case 'v':	/* more verbosity */
		    verbose++;
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
	    fprintf (stderr,"IMFILL: %s is not a FITS or IRAF file \n",str);
            usage();
            }

    }

    if (filter == FILL_NONE) {
	fprintf (stderr, "IMFILL: No filling method has been set\n");
	exit (-1);
	}

    /* Process files in file of filenames */
    if (readlist) {
	if ((flist = fopen (listfile, "r")) == NULL) {
	    fprintf (stderr,"IMFILL: List file %s cannot be read\n",
		     listfile);
	    usage ();
	    }
	while (fgets (filename, 128, flist) != NULL) {
	    lastchar = filename + strlen (filename) - 1;
	    if (*lastchar < 32) *lastchar = 0;
	    imFill (filename);
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
		imFill (fname);
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
    fprintf (stderr,"Fill bad pixels in FITS and IRAF image files\n");
    fprintf(stderr,"Usage: [-v][-a dx[,dy]][-g dx[,dy]][-m dx[,dy]] file.fits ...\n");
    fprintf(stderr,"  -a dx dy: Mean filter dx x dy pixels\n");
    fprintf(stderr,"  -b num: Bad pixel value (default is BLANK or -9999)\n");
    fprintf(stderr,"  -f file: FITS file with zeroes except at bad pixels\n");
    fprintf(stderr,"  -g dx x dy: Gaussian filter dx x dy pixels\n");
    fprintf(stderr,"  -h halfwidth: Gaussian half-width at half-height\n");
    fprintf(stderr,"  -l num: Logging interval in lines\n");
    fprintf(stderr,"  -m dx dy: Median filter dx x dy pixels\n");
    fprintf(stderr,"  -o: Allow overwriting of input image, else write new one\n");
    fprintf(stderr,"  -p num [num]: Minimum [maximum] good pixel value\n");
    fprintf(stderr,"  -v: Verbose\n");
    exit (1);
}

static void
imFill (name)
char *name;
{
    char *image;		/* FITS image */
    char *header;		/* FITS header */
    int lhead;			/* Maximum number of bytes in FITS header */
    int nbhead;			/* Actual number of bytes in FITS header */
    int lheadb;			/* Maximum number of bytes in bad pixel FITS header */
    int nbheadb;		/* Actual number of bytes in bad pixel FITS header */
    int iraffile;		/* 1 if IRAF image */
    int nfilled;
    int nbadpix;
    int i;
    char *irafheader;		/* IRAF image header */
    char newname[256];		/* Name for revised image */
    char *ltime;
    char *endchar;
    char *ext = NULL;
    char *newimage = NULL;
    char *imext = NULL;
    char *imext1 = NULL;
    char *fname;
    char extname[16];
    int lext = 0;
    int lroot;
    char echar;
    char temp[8];
    char history[128];
    char pixname[256];

    /* If not overwriting input file, make up a name for the output file */
    if (!overwrite) {
	fname = strrchr (name, '/');
	if (fname)
	    fname = fname + 1;
	else
	    fname = name;
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
	if (filter == FILL_MEDIAN)
	    strcat (newname, "m");
	else if (filter == FILL_GAUSSIAN)
	    strcat (newname, "g");
	else if (filter == FILL_MEAN)
	    strcat (newname, "a");
	else
	    strcat (newname, "b");

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
	strcat (newname, "f");
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
	if (iraffile)
	    fprintf (stderr,"IRAF image file %s", name);
	else
	    fprintf (stderr,"FITS image file %s", name);
	fprintf (stderr, " -> %s\n", newname);
	if (filter == FILL_GAUSSIAN)
	    fprintf (stderr, "%d x %d x %.2f Gaussian filled\n",
		 xsize, ysize, ghwidth);
	else if (filter == FILL_MEDIAN)
	    fprintf (stderr, "%d x %d Median filled\n", xsize, ysize);
	else
	    fprintf (stderr, "%d x %d Mean filled\n", xsize, ysize);
	}

    /* Set bad pixels in input image from FITS format bad pixel file */
    if (strlen (badpixfile) > 0) {
	if (badheader == NULL) {
	    if ((badheader = fitsrhead (badpixfile, &lheadb, &nbheadb)) != NULL) {
		if (badimage == NULL) {
		    if ((badimage = fitsrimage (badpixfile, nbheadb, badheader)) == NULL) {
			fprintf (stderr, "Cannot read bad pixel image %s\n", badpixfile);
			free (badheader);
			free (header);
			free (image);
			return;
			}
		    }
		}
	    else {
		fprintf (stderr, "Cannot read bad pixel file %s\n", badpixfile);
		free (header);
		free (image);
		return;
		}
	    }
	if ((newimage = SetBadFITS (header,image,badheader,badimage,nlog))==NULL) {
	    fprintf (stderr,"Bad pixels in %s not set from %s; file unchanged.\n",
		     name, badpixfile);
	    return;
	    }
	else {
	    nbadpix = getnfilled();
	    if (verbose)
		printf ("%s: %d bad pixels flagged from %s\n",
			name, nbadpix, badpixfile);
	    free (image);
	    image = newimage;
	    sprintf (history,"%d bad pixels set from %s", nbadpix, badpixfile);
	    hputs (header, "BADPIX", history);
	    }
	}

    /* Set pixels below minimum good pixel value to bad pixel value */
    if (minpixval > -9999.0 && maxpixval > -9999.0) {
	if ((newimage = SetBadVal (header,image,minpixval,maxpixval,nlog))==NULL) {
	    fprintf (stderr,"Bad pixels in %s < %f, > %f not set; file unchanged.\n",
		     name, minpixval, maxpixval);
	    return;
	    }
	else {
	    nbadpix = getnfilled();
	    if (verbose)
		printf ("%s: %d bad pixels flagged  unless %f < x < %f\n",
			name, nbadpix, minpixval, maxpixval);
	    free (image);
	    image = newimage;
	    sprintf (history,"%d bad pixels < %f > %f",
		     nbadpix, minpixval, maxpixval);
	    hputs (header, "BADPIX", history);
	    }
	}
    else if (minpixval > -9999.0) {
	if ((newimage = SetBadVal (header,image,minpixval,maxpixval,nlog))==NULL) {
	    fprintf (stderr,"Bad pixels in %s < %f not set; file unchanged.\n",
		     name, minpixval);
	    return;
	    }
	else {
	    nbadpix = getnfilled();
	    if (verbose)
		printf ("%s: %d bad pixels flagged if < %f\n",
			name, nbadpix, minpixval);
	    free (image);
	    image = newimage;
	    sprintf (history,"%d bad pixels < %f", nbadpix, minpixval);
	    hputs (header, "BADPIX", history);
	    }
	}

    /* Fill image bad pixels with interpolations */
    if (filter != FILL_NONE) {
        if ((newimage = FillFITS (header,image,filter,xsize,ysize,nlog)) == NULL) {
	    fprintf (stderr,"Cannot flag image %s from %s; file unchanged.\n",
		     name, badpixfile);
	    free (image);
	    free (header);
	    return;
	    }
	else {
	    free (image);
	    image = newimage;
	    }
	}

    /* Check header size */

    nfilled = getnfilled ();
    if (image && nfilled == 0)
	fprintf (stderr,"IMFILL: No pixel values replaced\n");

    /* Get current date and time to minute */
    ltime = lt2fd ();
    endchar = strrchr (ltime,':');
    *endchar = (char) 0;

    /* Set IMFILL keyword */
    for (i = 0; i < 128; i++)
	history[i] = (char) 0;
    if (hgets (header, "IMFILL", 72, history))
	hputc (header, "HISTORY", history);
    for (i = 0; i < 128; i++)
	history[i] = (char) 0;
    if (filter == FILL_GAUSSIAN)
	sprintf (history, "%s: %d pixels %d x %d x %.1f Gaussian-filled",
		 ltime, nfilled, xsize, ysize, ghwidth);
    else if (filter == FILL_MEDIAN)
	sprintf (history, "%s: %d pixels %d x %d median-filled ",
		 ltime, nfilled, xsize, ysize);
    else
	sprintf (history, "%s: %d pixels %d x %d mean-filled ",
		 ltime, nfilled, xsize, ysize);
    hputs (header, "IMFILL", history);

    /* Write output image file */
    if (iraffile && !fitsout) {
	if (irafwimage (newname,lhead,irafheader,header,image) > 0) {
	    if (verbose)
		printf ("%s: written successfully.\n", newname);
	    else
		printf ("%s\n", newname);
	    }
	else if (verbose)
	    printf ("IMFILL: File %s not written.\n", newname);
	}
    else if (fitswimage (newname, header, image) > 0) {
	if (verbose)
	    printf ("%s: written successfully.\n", newname);
	else
	    printf ("%s\n", newname);
	}
    else if (verbose) {
	printf ("IMFILL: File %s not written.\n", newname);
	if (newimage != NULL)
	    free (newimage);
	}

    free (header);
    if (iraffile)
	free (irafheader);
    free (image);
    return;
}
/* Apr 11 2006	New program
 * Apr 19 2006	Fix calls to usage()
 * Jun 21 2006	Clean up code
 * Jul  5 2006	Add option to set bad pixels if less than minimum value
 * Jul  5 2006	Change argument letters around
 * Jul  6 2006	Make both dimensions of Gaussian variable
 * Jul  7 2006	Add option to set bad pixels if greater than maximum value
 *
 * Jan 10 2007	Declare setbadpix()
 */
