/* File edhead.c
 * October 31, 2006
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
#include <sys/types.h>
#include <sys/stat.h>
#include <math.h>
#include "libwcs/fitsfile.h"
#include "libwcs/wcs.h"

static void usage();
static void EditHead();

static int newimage = 0;
static int verbose = 0;		/* verbose flag */
static char *editcom0;		/* Editor command from command line */
static char *RevMsg = "EDHEAD WCSTools 3.8.1, 14 December 2009, Doug Mink (dmink@cfa.harvard.edu)";
static int version = 0;		/* If 1, print only program name and version */


int
main (ac, av)
int ac;
char **av;
{
    char *str;

    editcom0 = NULL;

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
	while ((c = *++str))
	switch (c) {
	case 'v':	/* more verbosity */
	    verbose++;
	    break;

	case 'n':	/* ouput new file */
	    newimage++;
	    break;

	case 'e':	/* Specify editor */
	    if (ac < 2)
		usage ();
	    editcom0 = *++av;
	    ac--;
	    break;

	default:
	    usage();
	    break;
	}
    }

    /* now there are ac remaining file names starting at av[0] */
    if (ac == 0)
	usage();

    while (ac-- > 0) {
	char *fn = *av++;
	EditHead (fn);
	if (verbose)
	    printf ("\n");
	}

    return (0);
}

static void
usage ()
{
    fprintf (stderr,"%s\n",RevMsg);
    if (version)
	exit (-1);
    fprintf (stderr,"Edit header of FITS or IRAF image file\n");
    fprintf(stderr,"usage: edhead [-nv] [-e editor] file.fits file.imh...\n");
    fprintf(stderr,"  -e: Set editor, overiding environment EDITOR \n");
    fprintf(stderr,"  -n: write new file, else overwrite \n");
    fprintf(stderr,"  -v: verbose\n");
    exit (1);
}


static void
EditHead (filename)

char	*filename;	/* FITS or IRAF file filename */

{
    char *image;		/* FITS image */
    char *header;		/* FITS header */
    int lhead;			/* Maximum number of bytes in FITS header */
    int nbhead;			/* Actual number of bytes in FITS header */
    int nbold, nbnew;
    int iraffile;		/* 1 if IRAF image */
    char *irafheader = NULL;		/* IRAF image header */
    int i, nbytes, nhb, nhblk, lext, lroot;
    int fdr, ipos, nbr, nbw;
    int fdw;
    int naxis = 0;
    int nblock, nlines;
    int bitpix = 0;
    int imageread = 0;
    char *head, *headend, *hlast;
    char headline[160];
    char newname[128];
    char space;
    char temphead[] = "/tmp/edheadXXXXXX";
    FILE *fd;
    char *ext, *fname, *imext, *imext1;
    char *editcom;
    char newline[1];
    char echar;

    newline[0] = 10;
    space = (char) 32;
    image = NULL;

    /* Open IRAF image and header if .imh extension is present */
    if (isiraf (filename)) {
	iraffile = 1;
	if ((irafheader = irafrhead (filename, &lhead)) != NULL) {
	    if ((header = iraf2fits (filename, irafheader, lhead, &nbhead)) == NULL) {;
		free (irafheader);
                fprintf (stderr, "Cannot translate IRAF header %s/n", filename);
                return;
                }
	    }

	else {
	    fprintf (stderr, "Cannot read IRAF header file %s\n", filename);
	    return;
	    }
	}

    /* Read FITS image and header if .imh extension is not present */
    else {
	iraffile = 0;
	if ((header = fitsrhead (filename, &lhead, &nbhead)) != NULL) {
	    hgeti4 (header,"NAXIS",&naxis);
	    hgeti4 (header,"BITPIX",&bitpix);
	    if ((image = fitsrfull (filename, nbhead, header)) == NULL) {
		fprintf (stderr, "Cannot read FITS image %s\n", filename);
		imageread = 0;
		}
	    else
		imageread = 1;
	    }
	else {
	    fprintf (stderr, "Cannot read FITS file %s\n", filename);
	    return;
	    }
	}
    if (verbose)
	fprintf (stderr,"%s\n",RevMsg);

    nbold = fitsheadsize (header);

    /* Write current header to temporary file */
    fd = fdopen (mkstemp (temphead), "w");
    if (fd != NULL) {
	headend = ksearch (header, "END") + 80;
	for (head = header; head < headend; head = head + 80) {
	    for (i = 0; i< 80; i++)
		headline[i] = 0;
	    strncpy (headline,head,80);
	    for (i = 0; i < 80; i++) {
		if (headline[i] < space)
		    headline[i] = space;
		}
	    for (i = 79; i > 0; i--) {
		if (headline[i] == ' ')
		    headline[i] = 0;
		else
		    break;
		}
	    nbytes = i + 1;
	    (void) fwrite (headline, 1, nbytes, fd);
	    (void) fwrite (newline, 1, 1, fd);
	    }
	fclose (fd);
	free (header);
	}
    else {
	fprintf (stderr, "Cannot write temporary header file %s\n", temphead);
	free (header);
	if (iraffile)
	    free (irafheader);
	free (image);
	return;
	}

    /* Run an editor on the temporary header file */
    editcom = (char *)calloc (1, 256);
    if (editcom0 != NULL)
	strcpy (editcom, editcom0);
    else if ((editcom0 = getenv ("EDITOR")))
	strcpy (editcom, editcom0);
    else if (access ("vi", X_OK))
	strcpy (editcom, "vi");
    else if (access ("vim", X_OK))
	strcpy (editcom, "vim");
    else {
	fprintf (stderr, "Cannot find EDITOR or vi or vim\n");
	free (header);
	if (iraffile)
	    free (irafheader);
	free (image);
	return;
	}
    strcat (editcom," ");
    strcat (editcom,temphead);
    if (verbose)
	printf ("Edit command is '%s'\n",editcom);
    if (strncmp (editcom, "none", 4) &&
	strncmp (editcom, "NONE", 4)) {
	if (system (editcom)) {
	    free (header);
	    if (iraffile)
		free (irafheader);
	    free (image);
	    unlink (temphead);
	    free (editcom);
	    return;
	    }
	}

    /* Read the new header from the temporary file */
    if ((nlines = getfilelines (temphead)) > 0) {
        nbytes = nlines * 80;
	nblock = nbytes / 2880;
	if (nblock * 2880 < nbytes)
	    nblock = nblock + 1;
        nbytes = nblock * 2880;
	header = (char *) calloc (nbytes, 1);
	head = header;
	hlast = header + nbytes - 1;
	for (i = 0; i< 81; i++)
	    headline[i] = 0;
	if ((fd = fopen (temphead, "r")) == NULL) {
	    fprintf (stderr, "Cannot read edited header file %s\n", temphead);
	    free (header);
	    if (iraffile)
		free (irafheader);
	    free (image);
	    free (editcom);
	    return;
	    }
	while (fgets (headline,82,fd)) {
	    int i = 79;
	    while (headline[i] == (char) 0 || headline[i] == (char) 10)
		headline[i--] = ' ';
	    strncpy (head,headline,80);
	    head = head + 80;
	    if (head > hlast) {
		nhblk = (head - header) / 2880;
		nhb = (nhblk + 10) * 2880;
		header = (char *) realloc (header,nhb);
		head = header + nhb;
		hlast = hlast + 28800;
		}
	    for (i = 0; i< 80; i++)
		headline[i] = (char) 0;
	    }
	fclose (fd);
	}
    else {
	fprintf (stderr, "Cannot read temporary header file %s\n", temphead);
	free (header);
	if (iraffile)
	    free (irafheader);
	free (image);
	free (editcom);
	return;
	}

    /* Compare size of output header to size of input header */
    nbnew = fitsheadsize (header);
    if (nbnew > nbold  && naxis == 0 && bitpix != 0) {
	if (verbose)
	    fprintf (stderr, "Rewriting primary header, copying rest of file\n");
	newimage = 1;
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
	strcpy (newname, filename);
	if (ksearch (header,"XTENSION")) {
	    strcat (newname, ",1");
	    imext = strchr (newname,',');
	    }
	}

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
    else if (nbnew <= nbold && !newimage) {
	if (!fitswexhead (newname, header)) {
	    if (verbose)
		printf ("%s: rewritten successfully.\n", newname);
	    }
	}

    /* Rewrite header and data to a new image file */
    else if (naxis > 0 && imageread) {
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

    free (header);
    if (image)
	free (image);
    unlink (temphead);
    free (editcom);
    return;
}

/* Aug 15 1996	New program
 * Aug 26 1996	Change HGETC call to HGETS
 * Aug 27 1996	Read up to 82 characters per line to get newline
 * Aug 29 1996	Allow new file to be written
 * Oct 17 1996	Drop unused variables
 * Dec 11 1996	Allocate editcom if environment variable is not set
 *
 * Feb 21 1997  Check pointers against NULL explicitly for Linux
 *
 * May 20 1998	Set reread buffer size based on temporary file size
 * May 26 1998	Version 2.2.1: Fix long-standing .imh writing bug
 * May 27 1998	Include fitsio.h instead of fitshead.h
 * Jun  2 1998  Fix bug in hput()
 * Jun 24 1998	Preserve file extension
 * Jul 24 1998	Make irafheader char instead of int
 * Oct 14 1998	Use isiraf() to determine file type
 * Nov 30 1998	Add version and help commands for consistency
 * Dec  2 1998	Create and delete temporary file for header being edited
 * Dec 30 1998	Write header without image if no image is present
 *
 * Oct 21 1999	Drop unused variables after lint
 * Nov 24 1999	Do not invoke editor if it is none or NONE
 * Nov 24 1999	Add -e to set editor on command line
 * Nov 24 1999	Set characters less than 32 in header string to space
 * Nov 29 1999	Fix bug so environment editor is used correctly
 *
 * Apr  9 2002	Do not free unallocated header
 *
 * Aug 21 2003	Use fitsrfull() to handle any simple FITS image
 * Jul  1 2004	Change first extension if no extension specified
 * Jul  1 2004	Overwrite edited header, if new header fits
 *
 * Apr 14 2005	Set new header size by number of lines in temporary file
 *
 * Feb  1 2006	Drop redundant free(image) found by Sergey Koposov
 * Jun 12 2006	Use mkstemp() instead of tempnam() suggested by Sergio Pascual
 * Jun 20 2006	Clean up code
 * Sep  1 2006	Change temphead declaration to [] from *
 * Oct 31 2006	Check for vim as well as vi if EDITOR not in environment
 */
