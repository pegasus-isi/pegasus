/* File i2f.c
 * May 11 2009
 * By Doug Mink, Harvard-Smithsonian Center for Astrophysics
 * Send bug reports to dmink@cfa.harvard.edu

   Copyright (C) 1996-2009
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

static void usage();
static void IRAFtoFITS ();

static int verbose = 0;		/* verbose/debugging flag */
static char *RevMsg = "I2F WCSTools 3.8.1, 14 December 2009, Doug Mink (dmink@cfa.harvard.edu)";
static int version = 0;		/* If 1, print only program name and version */
static int delirafkey = 0;	/* If 1, delete IRAF .imh keywords */
static int deliraffile = 0;	/* If 1, delete IRAF .imh files */
static char outname[128];	/* Name for output image */
static char outdir[256];	/* Output directory pathname */
static int first = 1;

int
main (ac, av)
int ac;
char **av;
{
    char *str;
    char *listfile;
    char filename[256];
    int ifile, nfile;
    FILE *flist;

    outname[0] = 0;
    outdir[0] = 0;

    /* Check for help or version command first */
    str = *(av+1);
    if (!str || !strcmp (str, "help") || !strcmp (str, "-help"))
	usage();
    if (!strcmp (str, "version") || !strcmp (str, "-version")) {
	version = 1;
	usage();
	}

    /* crack arguments */
    for (av++; --ac > 0 && *(str = *av) == '-'; av++) {
	char c;
	while ((c = *++str)) {
	    switch (c) {
		case 'v':	/* more verbosity */
		    verbose++;
		    break;
		case 'd':	/* output directory */
		    if (ac < 2)
			usage ();
		    strcpy (outdir, *++av);
		    ac--;
		    break;
		case 'x':	/* delete IRAF imh files */
		    deliraffile = 1;
		    break;
		case 'i':	/* delete IRAF imh keywords */
		    delirafkey = 1;
		    break;
		case 'o':	/* output file name */
		    if (ac < 2)
			usage ();
		    strcpy (outname, *++av);
		    ac--;
		    break;
		case 's':	/* output to stdout */
		    strcpy (outname, "stdout");
		    break;
	        default:
		    usage();
		    break;
		}
    	    }
	}

    /* now there are ac remaining file names starting at av[0] */
    if (ac == 0)
	usage ();

     else if (*av[0] == '@') {
	listfile = *av + 1;
	if (isimlist (listfile)) {
	    nfile = getfilelines (listfile);
	    if ((flist = fopen (listfile, "r")) == NULL) {
		fprintf (stderr,"I2F: Image list file %s cannot be read\n",
			 listfile);
		usage ();
		}
	    for (ifile = 0; ifile < nfile; ifile++) {
		first_token (flist, 254, filename);
		IRAFtoFITS (filename);
		}
	    fclose (flist);
	    }
	}

    else {
	while (ac-- > 0) {
	    char *fn = *av++;
	    if (verbose)
		fprintf (stderr,"%s", fn);
	    IRAFtoFITS (fn);
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
    fprintf (stderr,"Write FITS files from IRAF image files\n");
    fprintf(stderr,"Usage: i2f [-isvx] [-o name] [-d path] file.imh ...\n");
    fprintf(stderr,"  or : i2f [-isvx] [-o name] [-d path] @imhlist\n");
    fprintf(stderr,"  -d: write FITS file(s) to this directory\n");
    fprintf(stderr,"  -i: delete unnecessary IRAF keywords\n");
    fprintf(stderr,"  -o: output name for one file\n");
    fprintf(stderr,"  -s: write output to standard output\n");
    fprintf(stderr,"  -v: verbose\n");
    fprintf(stderr,"  -x: delete IRAF image file\n");
    exit (1);
}

static void
IRAFtoFITS (name)
char *name;
{
    char *image;	/* FITS image */
    char *header;	/* FITS header */
    int lhead;		/* Maximum number of bytes in FITS header */
    int nbhead;		/* Actual number of bytes in FITS header */
    char *irafheader;	/* IRAF image header */
    char pixname[256];	/* Pixel file name */
    char history[128];	/* for HISTORY line */
    char *filename;	/* Pointer to start of file name */
    char irafname[256];	/* Name of IRAF file */
    char hdrfile[256];	/* Name of IRAF header file */
    char hdrback[256];	/* Name of backup IRAF header file */
    char pixfile[256];	/* Name of IRAF pixel file */
    char fitsname[256];	/* Name of FITS file */
    char fitspath[256];	/* Pathname of FITS file  */
    char *hdrfile1;
    char *fitsfile;
    char command[256];
    char *ext;		/* Pointer to start of extension */
    char *endchar;
    char *ltime;
    int nc, ier;

    /* Open IRAF image if .imh extension is present */
    if (strsrch (name,".imh") != NULL) {
	if ((irafheader = irafrhead (name, &lhead)) != NULL) {
	    header = iraf2fits (name, irafheader, lhead, &nbhead);
	    free (irafheader);
	    if (header == NULL) {
		fprintf (stderr, "Cannot translate IRAF header %s/n",name);
		return;
		}
	    if ((image = irafrimage (header)) == NULL) {
		hgetm (header,"PIXFIL", 255, pixname);
		fprintf (stderr, "Cannot read IRAF pixel file %s\n", pixname);
		free (header);
		return;
		}
	    }
	else {
	    fprintf (stderr,"Cannot read IRAF header file %s\n", name);
	    return;
	    }
	strcpy (fitsname, name);
	ext = strsrch (fitsname,".imh");
	strcpy (ext,".fits");
	if (verbose && first) {
	    fprintf (stderr,"%s\n",RevMsg);
	    if (outname[0] == (char) 0)
		fprintf (stderr, "Write FITS file from IRAF image file %s\n",
			 name);
	    else
		fprintf (stderr,"Write FITS file %s from IRAF image file %s\n",
			 outname, name);
	    }
	}

    /* Add .imh extension to make IRAF header file name if not present */
    else {
	strcpy (irafname, name);
	strcat (irafname,".imh");
	if ((irafheader = irafrhead (irafname, &lhead)) != NULL) {
	    header = iraf2fits (irafname, irafheader, lhead, &nbhead);
	    free (irafheader);
	    if (header == NULL) {
		fprintf (stderr, "Cannot translate IRAF header %s/n",irafname);
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
	    fprintf (stderr,"Cannot read IRAF header file %s\n", irafname);
	    return;
	    }
	strcpy (fitsname, name);
	strcat (fitsname,".fits");
	if (verbose && first) {
	    fprintf (stderr,"%s\n",RevMsg);
	    fprintf (stderr,"Write FITS files from IRAF image file %s\n", irafname);
	    }
	}

    /* Add HISTORY notice of this conversion */
    filename = strrchr (name,'/');
    if (filename)
	filename = filename + 1;
    else
	filename = name;
    strcpy (history, RevMsg);
    endchar = strchr (history, ',');
    *endchar = (char) 0;
    strcat (history, " ");
    ltime = lt2fd ();
    strcat (history, ltime);
    endchar = strrchr (history,':');
    *endchar = (char) 0;
    strcat (history, " FITS from ");
    strcat (history, filename);
    if (strlen (history) > 72)
	history[72] = 0;
    hputc (header, "HISTORY", history);

    /* If assigned output name, use it */
    if (outname[0] != (char) 0)
	strcpy (fitsname, outname);

    /* Save name of header file and pixel file */
    hgetm (header, "IMHFIL", 256, hdrfile);
    hgetm (header, "PIXFIL", 256, pixfile);

    /* Delete IRAF header information */
    if (delirafkey) {
	hdel (header, "IMHFIL_1");
	hdel (header, "PIXFIL_1");
	hdel (header, "IMHVER");
	hdel (header, "PIXOFF");
	hdel (header, "PIXSWAP");
	hdel (header, "HEADSWAP");
	hdel (header, "DATE-MOD");
	hdel (header, "PIXSWAP");
	hdel (header, "DATE-MOD");
	hdel (header, "IRAFMIN");
	hdel (header, "IRAFMAX");
	hdel (header, "IRAFTYPE");
	hdel (header, "IRAF-BPX");
	}

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
    first = 0;

    /* Write FITS image */
    if (fitswimage (fitspath, header, image) > 0) {
	if (verbose)
	    fprintf (stderr, " to %s\n", fitspath);

	/* Delete IRAF .imh, .pix files */
	if (deliraffile) {
	    hdrfile1 = strrchr (hdrfile, '/');
	    if (hdrfile1 == NULL) {
		strcpy (hdrback, "..");
		strcat (hdrback, hdrfile);
		}
	    else {
		nc = hdrfile1 - hdrfile;
		strncpy (hdrback, hdrfile, nc+1);
		strcat (hdrback, "..");
		strcat (hdrback, hdrfile1+1);
		}
	    if (verbose)
		fprintf (stderr, "deleting %s, %s, and %s\n",
			 hdrfile, pixfile, hdrback);
	    strcpy (command, "rm ");
	    strcat (command, hdrfile);
	    ier = system (command);
	    if (ier)
		(void)fprintf(stderr,"Command %s failed %d\n",command,ier);
	    strcpy (command, "rm ");
	    strcat (command, pixfile);
	    ier = system (command);
	    if (ier)
		(void)fprintf(stderr,"Command %s failed %d\n",command,ier);
	    strcpy (command, "rm ");
	    strcat (command, hdrback);
	    ier = system (command);
	    if (ier)
		(void)fprintf(stderr,"Command %s failed %d\n",command,ier);
	    }
	}

    else if (verbose)
	fprintf (stderr, "%s: not written.\n", fitspath);

    free (header);
    free (image);
    return;
}
/* Jun  6 1996	New program
 * Jul 16 1996	Update header input
 * Aug 16 1996	Clean up code
 * Aug 26 1996	Change HGETC call to HGETS
 * Aug 27 1996	Drop unused variables after lint
 * Oct 17 1996	Clean up after lint
 *
 * Feb 21 1997  Check pointers against NULL explicitly for Linux
 *
 * Jan 14 1998	Version 1.3 to handle IRAF 2.11 .imh files
 * May 27 1998	Include fitsio.h instead of fitshead.h
 * Jun  2 1998  Fix bug in hput()
 * Jul 24 1998	Make irafheader char instead of int
 * Aug  6 1998	Change fitsio.h to fitsfile.h
 * Aug 14 1998	Write file.fits instead of file.fit
 * Aug 17 1998	Add HISTORY to header
 * Nov 30 1998	Add version and help commands for consistency

 * Sep 28 1999	Add standard output option
 * Oct 22 1999	Drop unused variables after lint
 *
 * Mar 22 2000	Use lt2fd() instead of getltime()
 * Mar 23 2000	Use hgetm() to get the IRAF pixel file name, not hgets()
 * May 30 2000	Add option to delete IRAF keywords
 * Jun  6 2000	Add options to delete IRAF files and to write FITS elsewhere
 * Jul  6 2000	Implement conversion of file list
 *
 * Jun 21 2006	Clean up code
 *
 * May 11 2006	Drop extra free of irafheader when pixel file is missing
 */
