/* File imsmooth.c
 * January 5, 2007
 * By Doug Mink, Harvard-Smithsonian Center for Astrophysics
 * Send bug reports to dmink@cfa.harvard.edu

   Copyright (C) 2005-2007
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

#define MEDIAN 1
#define MEAN 2
#define GAUSSIAN 3

static void usage();
static void imSmooth();
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
static int xsize = 3;		/* Width of sliding box in pixels */
static int ysize = 3;		/* Height of sliding box in pixels */
static double ghwidth = 1.0;	/* Gaussian half-width */
static int filter = 0;		/* Filter code */
static int nlog = 100;		/* Number of lines between log messages */

static char *RevMsg = "IMSMOOTH WCSTools 3.8.1, 14 December 2009, Doug Mink (dmink@cfa.harvard.edu)";

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
			usage();
		    filter = MEAN;
		    xsize = (int) atof (*++av);
		    ac--;
		    ysize = (int) atof (*++av);
		    ac--;
		    break;

		case 'g':	/* Gaussian filter */
		    if (ac < 3)
			usage();
		    filter = GAUSSIAN;
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
		    filter = MEDIAN;
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
	    fprintf (stderr,"IMSMOOTH: %s is not a FITS or IRAF file \n",str);
            usage();
            }

    }

    /* Process files in file of filenames */
    if (readlist) {
	if ((flist = fopen (listfile, "r")) == NULL) {
	    fprintf (stderr,"IMSMOOTH: List file %s cannot be read\n",
		     listfile);
	    usage ();
	    }
	while (fgets (filename, 128, flist) != NULL) {
	    lastchar = filename + strlen (filename) - 1;
	    if (*lastchar < 32) *lastchar = 0;
	    imSmooth (filename);
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
		imSmooth (fname);
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
    fprintf (stderr,"Filter FITS and IRAF image files\n");
    fprintf(stderr,"Usage: [-v][-a dx dy]][-g dx dy]][-m dx dy]] file.fits ...\n");
    fprintf(stderr,"  -a dx dy: Mean filter dx x dy pixels\n");
    fprintf(stderr,"  -g dx dy: Gaussian filter dx x dy pixels\n");
    fprintf(stderr,"  -h halfwidth: Gaussian half-width at half-height\n");
    fprintf(stderr,"  -l num: Logging interval in lines\n");
    fprintf(stderr,"  -m dx dy: Median filter dx x dy pixels\n");
    fprintf(stderr,"  -o: Allow overwriting of input image, else write new one\n");
    fprintf(stderr,"  -v: Verbose\n");
    exit (1);
}

static void
imSmooth (name)
char *name;
{
    char *image;		/* FITS image */
    char *header;		/* FITS header */
    int lhead;			/* Maximum number of bytes in FITS header */
    int nbhead;			/* Actual number of bytes in FITS header */
    int iraffile;		/* 1 if IRAF image */
    char *irafheader = NULL;	/* IRAF image header */
    char newname[256];		/* Name for revised image */
    char *ext = NULL;
    char *newimage = NULL;
    char *imext = NULL;
    char *imext1;
    char *fname;
    char extname[16];
    int lext = 0;
    int lroot;
    char echar;
    char temp[8];
    char history[64];
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
	if (filter == MEDIAN)
	    strcat (newname, "m");
	else if (filter == GAUSSIAN)
	    strcat (newname, "g");
	else
	    strcat (newname, "a");

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
	if (filter == GAUSSIAN)
	    fprintf (stderr,"%d x %d Gaussian Filter ", xsize, ysize);
	else if (filter == MEDIAN)
	    fprintf (stderr,"%d x %d Median Filter ", xsize, ysize);
	else
	    fprintf (stderr,"%d x %d Mean Filter ", xsize, ysize);
	if (iraffile)
	    fprintf (stderr,"IRAF image file %s", name);
	else
	    fprintf (stderr,"FITS image file %s", name);
	fprintf (stderr, " -> %s\n", newname);
	}

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

    if ((newimage = FiltFITS (header,image,filter,xsize,ysize,nlog)) == NULL)
	fprintf (stderr,"Cannot filter image %s; file is unchanged.\n",name);
    else if (iraffile && !fitsout) {
	if (irafwimage (newname,lhead,irafheader,header,newimage) > 0) {
	    if (verbose)
		printf ("%s: written successfully.\n", newname);
	    else
		printf ("%s\n", newname);
	    }
	else if (verbose)
	    printf ("IMSMOOTH: File %s not written.\n", newname);
	}
    else if (fitswimage (newname, header, newimage) > 0) {
	if (verbose)
	    printf ("%s: written successfully.\n", newname);
	else
	    printf ("%s\n", newname);
	}
    else if (verbose) {
	printf ("IMSMOOTH: File %s not written.\n", newname);
	free (newimage);
	}

    free (header);
    if (iraffile)
	free (irafheader);
    free (image);
    return;
}
/* Oct 25 2005	New program
 *
 * Jan 25 2006	Add dimension size reduction factor
 * Feb 28 2006	Add -h for Gaussian half-width
 * Apr 11 2006	Add -o to overwrite or specify output filename
 * Apr 19 2006	Rename program imsmooth from imfilt
 * Apr 19 2006	Move image size change to imresize program
 * Jun 21 2006	Drop image reduction code; it is in imresize.c
 * Jun 21 2006	Add IMSMOOTH keyword to output image file
 * Jun 22 2006	Check for two-token extension .ms.fit(s)
 * Jul  6 2006	Make both dimensions of Gaussian variable
 *
 * Jan  5 2007	Add string length to call to hgets()
 */
