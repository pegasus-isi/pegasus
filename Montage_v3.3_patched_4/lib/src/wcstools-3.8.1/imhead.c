/* File imhead.c
 * December 1, 2008
 * By Doug Mink Harvard-Smithsonian Center for Astrophysics)
 * Send bug reports to dmink@cfa.harvard.edu

   Copyright (C) 1996-2008 
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
static int PrintFITSHead();
static void PrintHead();
extern char *GetFITShead();

static int nfiles = 0;		/* Nuber of files for headers */
static int verbose = 0;		/* verbose/debugging flag */
static int fitsout = 0;		/* If 1, write exact FITS header */
static int zbitpix = 0;		/* If 1, set BITPIX to 0 for dataless header */
static char *RevMsg = "IMHEAD WCSTools 3.8.1, 14 December 2009, Doug Mink SAO";
static int version = 0;		/* If 1, print only program name and version */

int
main (ac, av)
int ac;
char **av;
{
    char *str;
    int readlist = 0;
    int add = 0;
    char *lastchar;
    char filename[128];
    FILE *flist;
    char *listfile = NULL;

    /* Check for help or version command first */
    str = *(av+1);
    if (!str || !strcmp (str, "help") || !strcmp (str, "-help"))
	usage();
    if (!strcmp (str, "version") || !strcmp (str, "-version")) {
	version = 1;
	usage();
	}

    /* crack arguments */
    for (av++; --ac > 0 && (*(str = *av)=='-' || *str == '@' || *str == '+'); av++) {
	char c;
	if (*str == '@')
	    str = str - 1;
	if (*str == '+')
	    add = 1;
	else
	    add = 0;
	while ((c = *++str))
	switch (c) {

	case 'f':	/* Write FITS header only */
	    fitsout++;
	    break;

	case 'i':	/* Turn off inheritance from Primary header */
	    if (add)
		setfitsinherit (1);
	    else
		setfitsinherit (0);
	    break;

	case 'v':	/* more verbosity */
	    verbose++;
	    break;

	case 'z':	/* Write header with BITPIX = 0 */
	    zbitpix++;
	    break;

	case '@':	/* List of files to be read */
	    readlist++;
	    listfile = ++str;
	    str = str + strlen (str) - 1;
	    av++;
	    ac--;

	default:
	    usage();
	    break;
	}
    }

    /* Find number of images to search and leave listfile open for reading */
    if (readlist) {
	if ((flist = fopen (listfile, "r")) == NULL) {
	    fprintf (stderr,"IMHEAD: List file %s cannot be read\n",
		     listfile);
	    usage();
	    }
	while (fgets (filename, 128, flist) != NULL) {
	    lastchar = filename + strlen (filename) - 1;
	    if (*lastchar < 32) *lastchar = 0;
	    PrintHead (filename);
	    if (verbose)
		printf ("\n");
	    }
	fclose (flist);
	}

    /* If no arguments left, print usage */
    if (ac == 0)
	usage();

    nfiles = ac;
    while (ac-- > 0) {
	char *fn = *av++;
	PrintHead (fn);
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
    fprintf (stderr,"Print FITS or IRAF image header\n");
    fprintf(stderr,"usage: imhead [-fvz] file.fit ...\n");
    fprintf(stderr,"  -f: Write exact FITS header\n");
    fprintf(stderr,"  -i: Drop primary header even if inherited\n");
    fprintf(stderr,"  +i: Append primary header even if not inherited\n");
    fprintf(stderr,"  -v: verbose\n");
    fprintf(stderr,"  -z: Set BITPIX to 0 for dataless header\n");
    exit (1);
}


static void
PrintHead (name)

char *name;

{
    char *header;	/* FITS image header */
    int i, nw, nbytes, lhead, nblk;
    char *endhead;

    if ((header = GetFITShead (name, verbose)) == NULL)
	return;

    if (verbose)
	fprintf (stderr,"%s\n",RevMsg);

    if (verbose || nfiles > 1) {
	if (isiraf (name))
	    printf ("%s IRAF file header:\n", name);
	else if (isfits (name))
	    printf ("%s FITS file header:\n", name);
	else if (istiff (name))
	    printf ("%s FITS file header from TIFF file:\n", name);
	else if (isjpeg (name))
	    printf ("%s FITS file header from JPEG file:\n", name);
	else if (isgif (name))
	    printf ("%s FITS file header from GIF file:\n", name);
	else {
	    printf ("*** %s is not a FITS or IRAF or known image file\n", name);
	    return;
	    }
	}
    else if (!isiraf (name) && !isfits (name) && !istiff (name) &&
	     !isjpeg (name) && !isgif(name)) {
	printf ("*** %s is not a FITS or IRAF or known image file\n", name);
	return;
	}

    if (fitsout) {
	if (zbitpix)
	    hputi4 (header, "BITPIX", 0);
	endhead = ksearch (header,"END");
	lhead = endhead + 80 - header;
	nblk = lhead / 2880;
	if (lhead <= nblk * 2880)
	   nbytes = nblk * 2880;
	else
	    nbytes = (nblk + 1) * 2880;
	for (i = lhead; i < nbytes; i++)
	    header[i] = ' ';
	nw = write (1, header, nbytes);
	}
    else if (PrintFITSHead (header) && verbose)
	printf ("%s: no END of header found\n", name);

    free (header);
    return;
}


static int
PrintFITSHead (header)

char	*header;	/* Image FITS header */
{
    char line[81], *iline, *endhead;
    int i, nblank;

    line[80] = (char) 0;
    endhead = ksearch (header, "END") + 80;
    if (endhead == NULL)
	return (1);

    nblank = 0;
    for (iline = header; iline < endhead; iline = iline + 80) {
	strncpy (line, iline, 80);
	i = 79;
	while (line[i] <= 32 && i > 0)
	    line[i--] = 0;
	if (i > 0) {
	    if (nblank > 1) {
		printf ("COMMENT   %d blank lines\n",nblank);
		nblank = 0;
		}
	    else if (nblank > 0) {
		printf ("COMMENT   %d blank line\n",nblank);
		nblank = 0;
		}
	    printf ("%s\n",line);
	    }
	else
	    nblank++;
	}

    return (0);
}
/* Jul 10 1996	New program
 * Jul 16 1996	Update header I/O
 * Aug 15 1996	Drop unnecessary reading of FITS image; clean up code
 * Aug 27 1996	Drop unused variables after lint
 * Nov 19 1996	Add linefeeds after filename in verbose mode
 * Dec  4 1996	Print "header" instead of "WCS" in verbose mode
 * Dec 17 1996	Add byte skipping before header
 *
 * Feb 21 1997  Get header from subroutine
 * May 28 1997  Add option to read a list of filenames from a file
 * Dec 12 1997	Read IRAF version 2 .imh files
 * Dec 15 1997	Note number of blank lines in header as comment
 *
 * Jan  5 1998	Print file name if multiple headers printed
 * Jan  5 1998	Print error message if no END is found in header
 * Jan 14 1998	Really get IRAF 2.11 files right on any architecture
 * Mar 16 1998	Print line instead of lines if there is only one blank line
 * May 27 1998	Include fitsio.h instead of fitshead.h
 * Jun  2 1998	Fix bug in hput()
 * Aug  6 1998	Change fitsio.h to fitsfile.h
 * Oct 14 1998	Use isiraf() to determine file type
 * Nov 30 1998	Add version and help commands for consistency
 *
 * Oct 22 1999	Drop unused variables after lint
 * Nov 24 1999	Add options to output entire FITS header and set BITPIX to 0
 *
 * Jun 19 2002	Add verbose argument to GetFITShead()
 *
 * May  6 2004	Add -i argument to read extension header without primary
 *
 * Mar 17 2005	Check to make sure that input images are FITS or IRAF format
 * Oct 28 2005	Set 81st char of output buffer NULL, suggested by Sergey Koposov
 *
 * Jan 17 2006	Add +i to append primary header even if INHERIT is not set
 * Feb 23 2006	Read headers appended to TIFF, JPEG, or GIF image files
 * Jun 20 2006	Clean up code
 *
 * Dec  1 2008	Pad output FITS header with blanks
 */
