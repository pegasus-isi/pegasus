/* File delhead.c
 * September 25, 2009
 * By Doug Mink Harvard-Smithsonian Center for Astrophysics)
 * Send bug reports to dmink@cfa.harvard.edu

   Copyright (C) 1998-2009
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

#define MAXKWD 500
#define MAXFILES 1000
static int maxnkwd = MAXKWD;
static int maxnfile = MAXFILES;
#define MAXNEW 1024

static void usage();
static void DelKeywords();
static void DelCOMMENT();
extern int DelWCSFITS();
extern char *fitserrmsg;

static int verbose = 0;		/* verbose/debugging flag */
static int delwcs = 0;		/* WCS deletion flag */
static int newimage = 0;
static int delcom = 0;		/* If 1, delete blank COMMENT lines */
static int readimage = 0;	/* Read and write image as well as header */
static int version = 0;		/* If 1, print only program name and version */
static int logfile = 0;
static int nproc = 0;
static int overwrite = 0;	/* If 1, overwrite input image */
static int first_file = 1;

static char *RevMsg = "DELHEAD WCSTools 3.8.1, 14 December 2009, Doug Mink (dmink@cfa.harvard.edu)";

int
main (ac, av)
int ac;
char **av;
{
    char *str;
    char **kwd;
    int nkwd = 0;
    int nkwd1 = 0;
    int ikwd;
    int i;
    char **fn, **newfn;
    int nfile = 0;
    int ifile;
    int nbytes;
    char filename[128];
    FILE *flist = NULL;
    FILE *fdk;
    char *listfile;
    char *ilistfile;
    char *klistfile;
    char **kwdnew;

    ilistfile = NULL;
    klistfile = NULL;
    nfile = 0;
    fn = (char **)calloc (maxnfile, sizeof(char *));
    kwd = (char **)calloc (maxnkwd, sizeof(char *));

    /* Check for help or version command first */
    str = *(av+1);
    if (!str || !strcmp (str, "help") || !strcmp (str, "-help"))
	usage ();
    if (!strcmp (str, "version") || !strcmp (str, "-version")) {
	version = 1;
	usage ();
	}

    /* crack arguments */
    for (av++; --ac > 0; av++) {
	if ((*(str = *av)) == '-') {
	    char c;
	    while ((c = *++str))
	    switch (c) {
	
		case 'b': /* Leave blank lines at end after removing keywords */
		    setheadshrink (0);
		    readimage = 0;
		    break;

		case 'c': /* Delete blank COMMENTs */
		    delcom++;
		    nkwd++;
		    break;
	
		case 'l':	/* Log files changed */
		    logfile++;
		    break;

		case 'n':	/* write new file */
		    newimage++;
		    break;

		case 'o':	/* overwrite file */
		    newimage++;
		    overwrite++;
		    break;
	
		case 'v':	/* more verbosity */
		    verbose++;
		    break;
	
		case 'w':	/* delete all WCS keywords */
		    delwcs++;
		    nkwd++;
		    break;
	
		default:
		    usage();
		    break;
		}
	    }

	/* File containing a list of keywords or files */
	else if (*av[0] == '@') {
	    listfile = *av + 1;
	    if (isimlist (listfile)) {
		ilistfile = listfile;
		nfile = getfilelines (ilistfile);
		}
	    else {
		klistfile = listfile;
		nkwd1 = getfilelines (klistfile);
		if (nkwd1 > 0) {
		    if (nkwd1 + nkwd > maxnkwd) {
			maxnkwd = maxnkwd + nkwd1 + 32;
			kwdnew = (char **)calloc (maxnkwd, sizeof(char *));
			for (ikwd = 0; ikwd < nkwd; ikwd++)
			    kwdnew[ikwd] = kwd[ikwd];
			free (kwd);
			kwd = kwdnew;
			}
		    if ((fdk = fopen (klistfile, "r")) == NULL) {
			fprintf (stderr,"DELHEAD: File %s cannot be read\n",
				 klistfile);
			}
		    else {
			for (ikwd = 0; ikwd < nkwd1; ikwd++) {
			    kwd[nkwd] = (char *) calloc (32, 1);
			    first_token (fdk, 31, kwd[nkwd++]);
			    }
			fclose (fdk);
			}
		    }
		}
	    }

	/* Image file */
	else if (isfits (*av) || isiraf (*av)) {
	    if (nfile >= maxnfile) {
		maxnfile = maxnfile * 2;
		nbytes = maxnfile * sizeof (char *);
		newfn = (char **) calloc (maxnfile, sizeof (char *));
		for (i = 0; i < nfile; i++)
		    newfn[i] = fn[i];
		free (fn);
		fn = newfn;
		}
	    fn[nfile] = *av;
	    nfile++;
	    }

	/* Keyword */
	else {
	    if (nkwd >= maxnkwd) {
		maxnkwd = maxnkwd * 2;
		kwdnew = (char **) realloc ((void *)kwd, maxnkwd);
		for (ikwd = 0; ikwd < nkwd; ikwd++)
		    kwdnew[ikwd] = kwd[ikwd];
		free (kwd);
		kwd = kwdnew;
		}
	    kwd[nkwd] = *av;
	    nkwd++;
	    }
	}

    if (nkwd <= 0 && nfile <= 0 )
	usage ();
    else if (nkwd <= 0) {
	fprintf (stderr, "DELHEAD: no keywords specified\n");
	exit (1);
	}
    else if (nfile <= 0 ) {
	fprintf (stderr, "DELHEAD: no files specified\n");
	exit (1);
	}

    /* Delete keyword values one file at a time */

    /* Open file containing a list of images, if there is one */
    if (ilistfile != NULL) {
	if ((flist = fopen (ilistfile, "r")) == NULL) {
	    fprintf (stderr,"DELHEAD: Image list file %s cannot be read\n",
		     ilistfile);
	    usage ();
	    }
	}

    /* Read through headers of images */
    for (ifile = 0; ifile < nfile; ifile++) {
	if (ilistfile != NULL) {
	    first_token (flist, 254, filename);
	    DelKeywords (filename, nkwd, kwd);
	    }
	else
	    DelKeywords (fn[ifile], nkwd, kwd);
	}
    if (ilistfile != NULL)
	fclose (flist);

    return (0);
}

static void
usage ()
{
    fprintf (stderr,"%s\n",RevMsg);
    if (version)
	exit (-1);
    fprintf (stderr,"Delete FITS or IRAF header keyword entries\n");
    fprintf(stderr,"Usage: [-nv] file1.fits [ ... filen.fits] kw1 [... kwn]\n");
    fprintf(stderr,"  or : [-nv] @listfile kw1 [... kwn]\n");
    fprintf(stderr,"  or : [-nv] file1.fits [ ... filen.fits] @keylistfile\n");
    fprintf(stderr,"  or : [-nv] @listfile @keylistfile\n");
    fprintf(stderr,"  -b: leave blank line in header for each deleted line\n");
    fprintf(stderr,"  -c: delete blank COMMENT lines\n");
    fprintf(stderr,"  -n: write new file\n");
    fprintf(stderr,"  -o: overwrite file\n");
    fprintf(stderr,"  -v: verbose\n");
    fprintf(stderr,"  -w: delete all WCS keywords in header\n");
    exit (1);
}


static void
DelKeywords (filename, nkwd, kwd)

char	*filename;	/* Name of FITS or IRAF image file */
int	nkwd;		/* Number of keywords to delete */
char	*kwd[];		/* Names of those keywords */

{
    char *header;	/* FITS image header */
    char *image;	/* FITS image */
    int lhead;		/* Maximum number of bytes in FITS header */
    int nbhead;		/* Actual number of bytes in FITS header */
    int nblold, nblnew;	/* Number of FITS blocks (=2880 bytes) in header */
    char *irafheader = NULL;	/* IRAF image header */
    int iraffile;	/* 1 if IRAF image, 0 if FITS image */
    int lext, lroot, naxis;
    char newname[MAXNEW];
    char *ext, *fname, *imext, *imext1;
    char *kw, *kwl;
    char kwi[10];
    char echar;
    int ikwd;
    int fdr, fdw, ipos, nbr, nbw, bitpix, i;
    int imageread = 0;
    int nbold, nbnew;

    image = NULL;
    header = NULL;

    /* Open IRAF image if .imh extension is present */
    if (isiraf (filename)) {
	iraffile = 1;
	if ((irafheader = irafrhead (filename, &lhead)) != NULL) {
	    if ((header = iraf2fits (filename, irafheader, lhead, &nbhead)) == NULL) {
		fprintf (stderr, "Cannot translate IRAF header %s/n",filename);
		free (irafheader);
		return;
		}
	    }
	else {
	    fprintf (stderr, "Cannot read IRAF file %s\n", filename);
	    return;
	    }
	}

    /* Open FITS file if .imh extension is not present */
    else {
	iraffile = 0;
	setfitsinherit (0);
	if ((header = fitsrhead (filename, &lhead, &nbhead)) != NULL) {
	    }
	else {
	    fprintf (stderr, "Cannot read FITS file %s\n", filename);
	    return;
	    }
	}
    if (verbose && first_file) {
	fprintf (stderr,"%s\n",RevMsg);
	if (delwcs)
	    fprintf (stderr,"Delete Header WCS Parameter Entries from ");
	else if (delcom)
	    fprintf (stderr,"Delete Blank COMMENT lines from ");
	else
	    fprintf (stderr,"Delete Header Parameter Entries from ");
	if (iraffile)
	    fprintf (stderr,"IRAF image file %s\n", filename);
	else
	    fprintf (stderr,"FITS image file %s\n", filename);
	first_file = 0;
	}

    if (nkwd < 1)
	return;

    nbold = fitsheadsize (header);

    /* Remove directory path and extension from file name */
    fname = strrchr (filename, '/');
    if (fname)
	fname = fname + 1;
    else
	fname = filename;

    if (strchr (fname, ',') || strchr (fname,'['))
	setheadshrink (0);
    if (isiraf (filename))
	readimage = 0;
    if (newimage)
	readimage = 1;

    /* First, delete WCS keywords if requested */
    if (delwcs) {
	(void) DelWCSFITS (header, verbose);
	nkwd--;
	}

    /* Then delete blank COMMENT lines if requested */
    if (delcom) {
	DelCOMMENT (header, verbose);
	nkwd--;
	}

    /* Delete keywords one at a time */
    for (ikwd = 0; ikwd < nkwd; ikwd++) {

	/* Make keyword all upper case */
	kwl = kwd[ikwd] + strlen (kwd[ikwd]);
	for (kw = kwd[ikwd]; kw < kwl; kw++) {
	    if (*kw > 96 && *kw < 123)
		*kw = *kw - 32;
	    }

	/* Delete keyword */
	if (hdel (header, kwd[ikwd])) {
	    if (verbose)
		 printf ("%s: %s deleted\n", filename, kwd[ikwd]);
	    }

	/* If single-line keyword not found, try IRAF multiple-line format */
	else {
	    i = 1;
	    sprintf (kwi, "%s_%03d", kwd[ikwd], i++);
	    while (hdel (header, kwi)) {
		if (verbose)
		    printf ("%s: %s deleted\n", filename, kwi);
		sprintf (kwi, "%s_%03d", kwd[ikwd], i++);  
		}
	    }
	}

    /* Compare size of output header to size of input header */
    nbnew = fitsheadsize (header);
    nblnew = (int) (0.98 + (double) nbnew / 2880.0);
    nblold = (int) (0.98 + (double) nbold / 2880.0);
    if (nbnew > nbold && naxis == 0 && bitpix != 0) {
	if (verbose)
	    fprintf (stderr, "Rewriting primary header, copying rest of file\n");
	newimage = 1;
	}

    /* Make up name for new FITS or IRAF output file */
    if (newimage) {
	ext = strrchr (fname, '.');
	if (ext != NULL) {
	    lext = (fname + strlen (fname)) - ext;
	    lroot = ext - fname;
	    if (lroot > MAXNEW)
		lroot = MAXNEW - 1;
	    strncpy (newname, fname, lroot);
	    newname[lroot] = (char) 0;
	    }
	else {
	    lext = 0;
	    lroot = strlen (fname);
	    if (lroot > MAXNEW)
		lroot = MAXNEW - 1;
	    strncpy (newname, fname, lroot);
	    newname[lroot] = (char) 0;
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
	if (imext != NULL && *(imext+1) != '0') {
	    strcat (newname, "_");
	    strcat (newname, imext+1);
	    }
	if (fname)
	    fname = fname + 1;
	else
	    fname = filename;
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

    else {

	/* Keep name */
	strcpy (newname, filename);

	/* Set image extension if there is one */
	imext = strchr (filename, ',');
	if (imext == NULL)
	    imext = strchr (filename, '[');

	/* Add extension if modifying extension header */
	if (imext == NULL && ksearch (header,"XTENSION")) {
	    strcat (newname, ",1");
	    imext = strchr (newname,',');
	    }
	}

    if (nblnew == nblold && !newimage) 
	readimage = 0;

	hgeti4 (header,"NAXIS",&naxis);

    /* Write fixed header to output file */
    if (iraffile) {
	if (irafwhead (newname, lhead, irafheader, header) > 0 && verbose)
	    printf ("%s rewritten successfully.\n", newname);
	else if (verbose)
	    printf ("%s could not be written.\n", newname);
	free (irafheader);
	}

    /* If there is no data, write header by itself */
    else if (bitpix == 0) {
	if ((fdw = fitswhead (newname, header)) > 0) {
	    if (verbose)
		printf ("%s: rewritten successfully.\n", newname);
	    close (fdw);
	    }
	}

    /* Rewrite only header if it fits into the space from which it was read */
    else if (!readimage) {
	if (!fitswexhead (newname, header)) {
	    if (verbose)
		printf ("%s: rewritten successfully.\n", newname);
	    }
	}

    /* Rewrite header and data to a new image file */
    else if (naxis > 0 && readimage) {
	hgeti4 (header,"BITPIX",&bitpix);
	if (naxis > 0 && bitpix != 0) {
	    if ((image = fitsrfull (filename, nbhead, header)) == NULL) {
		if (verbose)
		    fprintf (stderr, "No FITS image in %s\n", filename);
		imageread = 0;
		}
	    else
		imageread = 1;
	    }
	if (fitswimage (newname, header, image) > 0 && verbose)
	    printf ("%s: rewritten successfully.\n", newname);
	else if (verbose)
	    printf ("%s could not be written.\n", newname);
	}

    else {
	if ((fdw = fitswhead (newname, header)) > 0) {
	    fdr = fitsropen (filename);
	    ipos = lseek (fdr, nbhead, SEEK_SET);
	    image = (char *) calloc (2880, 1);
	    while ((nbr = read (fdr, image, 2880)) > 0) {
		nbw = write (fdw, image, nbr);
		if (nbw < nbr)
		    fprintf (stderr,"SETHEAD: %d / %d bytes written\n",nbw,nbr);
		}
	    close (fdr);
	    close (fdw);
	    if (verbose)
		printf ("%s: rewritten successfully.\n", newname);
	    }
	}

    if (overwrite) {
	rename (newname, filename);
	if (verbose)
	    printf ("%s: overwritten successfully.\n", filename);
	}

    /* Log the processing of this file, if requested */
    if (logfile) {
	nproc++;
	fprintf (stderr, "%d: %s processed.\r", nproc, newname);
	}

    if (header != NULL)
	free (header);
    if (image != NULL)
	free (image);
    return;
}

static void
DelCOMMENT (header, verbose)

char *header;	/* FITS header */
int verbose;	/* If true, print deletion confirmations */
{
    char *hplace, *hcom, *v, *v2, *ve;
    int i, killcom, nline;
    int nkill = 0;

    hplace = header;
    while ((hcom = ksearch (hplace, "COMMENT"))) {
	killcom = 1;
	for (i = 7; i < 80; i++) {
	    if (hcom[i] != '=' && hcom[i] != ':' && hcom[i] != ' ' && hcom[i] != '\'')
		killcom = 0;
	    }
	if (killcom) {
	    if (verbose) {
		nkill++;
		nline = (hplace - header) / 80 + 1;
		/* fprintf (stderr, "%3d %3d %70.70s\n", nkill, nline, hcom); */
		}

	    /* Shift rest of header up one line */
	    if (newimage) {
		ve = ksearch (header, "END");
		for (v =hcom; v < ve; v = v + 80) {
		    v2 = v + 80;
		    strncpy (v, v2, 80);
		    }

		/* Cover former last line with spaces */
		v2 = ve + 80;
		for (v = ve; v < v2; v++)
		    *v = ' ';
		}

	    /* Fill line with blanks */
	    else {
		for (i = 0; i < 80; i++)
		    hcom[i] = ' ';
		hplace = hplace + 80;
		}
	    }

	/* Move on to next line if COMMENT line is OK */
	else
	    hplace = hplace + 80;
	}
	if (verbose)
	    fprintf (stderr, "%d blank COMMENT lines deleted\n", nkill);
}

/* Jul 27 1998	New program
 * Aug  6 1998	Change fitsio.h to fitsfile.h
 * Aug 14 1998	If changing primary header, write out entire input file
 * Oct  5 1998	Allow header changes even if no data is present
 * Oct  5 1998	Use isiraf() and isfits() to check for data file
 * Nov 30 1998	Add version and help commands for consistency
 *
 * Mar  2 1999	Add option to delete list of keyword names from file
 * Apr  2 1999	Add warning if too many files or keywords on command line
 * Apr  2 1999	Add -f and -m to change maximum number of files or keywords
 * Jul 14 1999	Read lists of BOTH keywords and files simultaneously
 * Jul 14 1999	Reallocate keyword array if too many in file
 * Jul 15 1999	Reallocate keyword and file lists if default limits exceeded
 * Sep 29 1999	Change maximum number of keywords from 100 to 500
 * Oct 21 1999	Drop unused variables after lint
 * Nov 29 1999	Fix usage command list
 * Nov 30 1999	Cast realloc's
 *
 * Jun  8 2000	If no files or keywords specified, say so
 *
 * Dec 16 2002	Fix bug so arbitrary number of keywords can be deleted
 *
 * Aug 21 2003	Use fitsrfull() to deal with n dimensional FITS images
 * Oct 29 2003	Keep count of keywords correctly when reading them from file
 *
 * May  6 2004	Allow keywords to be deleted from extension headers
 * Jul  1 2004	Do not drop lines from multi-extension headers
 * Jul  1 2004	Change first extension if no extension specified
 *
 * Jan 12 2005	Write over unread image only if number of header blocks same
 * Mar  1 2005	Print program version only on first file if looping
 * Jun 10 2005	Fix bug dealing with large numbers of keywords
 *
 * Apr 26 2006	Avoid freeing alread-freed image buffers
 * May 23 2006	Add -b option to leave blank lines in header
 * Jun 20 2006	Clean up code
 *
 * Jan 10 2007	Add int to readimage declaration
 * Apr 18 2007	Add -w to delete all WCS keywords
 * May  1 2007	Add -c to delete blank COMMENTs
 * Jun 12 2007	Delete IRAF multiple line keywords
 *
 * Aug 19 2009	Fix bug to remove limit to the number of files on command line
 * Sep 25 2009	Declare DelWCSFITS() and drop unused variable v1
 */
