/* File imstack.c
 * August 30, 2007
 * By Doug Mink, Harvard-Smithsonian Center for Astrophysics
 * Send bug reports to dmink@cfa.harvard.edu

   Copyright (C) 1997-2007
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

static void usage();
static int StackImage();

static int verbose = 0;		/* verbose flag */
static int wfits = 1;		/* if 1, write FITS header before data */
static char *newname = NULL;
static char *RevMsg = "IMSTACK WCSTools 3.8.1, 14 December 2009, Doug Mink (dmink@cfa.harvard.edu)";
static int nfiles = 0;
static int nbstack = 0;
static int extend = 0;		/* If 1, output multi-extension FITS file */
static FILE *fstack = NULL;
static int version = 0;		/* If 1, print only program name and version */
static int multispec = 0;	/* Add header keywords for IRAF multispec format */
static char *extroot;

int
main (ac, av)
int ac;
char **av;
{
    char filename[128];
    char *filelist[100];
    char *listfile = NULL;
    char *str;
    int readlist = 0;
    int ntimes = 1;
    FILE *flist;
    int ifile, nblocks, nbytes, i;
    char *blanks;

    /* Check for help or version command first */
    str = *(av+1);
    if (!str || !strcmp (str, "help") || !strcmp (str, "-help"))
	usage();
    if (!strcmp (str, "version") || !strcmp (str, "-version")) {
	version = 1;
	usage();
	}

    /* Crack arguments */
    for (av++; --ac > 0 && (*(str = *av)=='-' || *str == '@'); av++) {
	char c;
	if (*str == '@')
	    str = str - 1;
	while ((c = *++str))
	switch (c) {
	case 'v':	/* more verbosity */
	    verbose++;
	    break;

	case 'i':	/* Write only image data to output file */
	    wfits = 0;
	    if (newname == NULL) {
		newname = calloc (16, 1);
		strcpy (newname,"imstack.out");
		}
	    break;

	case 'm':	/* Add header keywords for IRAF multispec format */
	    multispec++;
	    break;

	case 'n':	/* Use each input file this many times */
	    if (ac < 2)
                usage ();
	    ntimes = (int) atof (*++av);
	    ac--;
	    break;

	case 'o':	/* Set output file name */
	    if (ac < 2)
                usage ();
	    if (newname != NULL)
		free (newname);
	    newname = *++av;
	    ac--;
	    break;

	case 'x':	/* Set FITS extension EXTNAME root */
	    if (ac < 2)
                usage ();
	    if (extroot != NULL)
		free (extroot);
	    extroot = *++av;
	    extend = 1;
	    ac--;
	    break;

	case '@':	/* List of files to be read */
	    readlist++;
	    listfile = ++str;
	    str = str + strlen (str) - 1;
	    av++;
	    ac--;
	    break;

	default:
	    usage();
	    break;
	}
    }

    /* If output file name has not yet been set, set it */
    if (newname == NULL) {
	newname = calloc (16, 1);
	strcpy (newname,"imstack.fits");
	}

    /* Find number of images to stack  and leave listfile open for reading */
    if (readlist) {
	if ((flist = fopen (listfile, "r")) == NULL) {
	    fprintf (stderr,"IMSTACK: List file %s cannot be read\n",
		     listfile);
	    usage();
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
	usage();

    /* Read ac remaining file names starting at av[0] */
    else {
	while (ac-- > 0) {
	    filelist[nfiles] = *av++;
	    nfiles++;
	    if (verbose)
		printf ("Reading %s\n",filelist[nfiles-1]);
	    }
	}

    /* Stack images from list in file */
    if (readlist) {
	for (ifile = 0;  ifile < nfiles; ifile++) {
	    if (fgets (filename, 128, flist) != NULL) {
		filename[strlen (filename) - 1] = 0;
		if (StackImage (ifile, ntimes, filename))
		    break;
		}
	    }
	fclose (flist);
	}

    /* Stack images from list on command line */
    else {
	for (ifile = 0;  ifile < nfiles; ifile++) {
	    if (StackImage (ifile, ntimes, filelist[ifile]))
		break;
	    }
	}


    /* Pad out FITS file to 2880 blocks */
    if (wfits && fstack != NULL) {
	nblocks = nbstack / FITSBLOCK;
	if (nblocks * FITSBLOCK < nbstack)
	    nblocks = nblocks + 1;
	nbytes = (nblocks * FITSBLOCK) - nbstack;
	if (nbytes > 0) {
	    blanks = (char *) malloc ((size_t) nbytes);
	    for (i = 0;  i < nbytes; i++)
		blanks[i] = 0;
	    (void) fwrite (blanks, (size_t) 1, (size_t)nbytes, fstack);
	    free (blanks);
	    }
	}

    if (fstack != NULL)
	fclose (fstack);
    return (0);
}

static void
usage ()
{
    fprintf (stderr,"%s\n",RevMsg);
    if (version)
	exit (-1);
    fprintf (stderr,"Stack FITS or IRAF images into single FITS image\n");
    fprintf(stderr,"Usage: imstack [-vi][-o filename][-n num] file1.fits file2.fits ... filen.fits\n");
    fprintf(stderr,"  or : imstack [-vi][-n num] @filelist\n");
    fprintf(stderr,"  -i: Do not put FITS header in output file\n");
    fprintf(stderr,"  -n num: Use each file this many times\n");
    fprintf(stderr,"  -m: Write IRAF multispec file\n");
    fprintf(stderr,"  -o name: Output filename\n");
    fprintf(stderr,"  -v: Verbose\n");
    fprintf(stderr,"  -x root: Make first file root, others extensions\n");
    exit (1);
}


static int
StackImage (ifile, ntimes, filename)

int	ifile;		/* Sequence number of input file */
int	ntimes;		/* Stack each image this many times */
char	*filename;	/* FITS or IRAF file filename */

{
    char *image = NULL;		/* FITS image */
    char *header;		/* FITS header */
    char *hplace;
    int lhead;			/* Maximum number of bytes in FITS header */
    int nbhead;			/* Actual number of bytes in FITS header */
    char *irafheader;		/* IRAF image header */
    int nbimage, naxis, naxis1, naxis2, naxis3, bytepix;
    int bitpix, nblocks, nbytes;
    int iraffile;
    int i, itime, nout;
    char spaces[80];
    char pixname[256];
    char extname[16];
    char *blanks;
    char *roothead, *rootend, *headend, *iline;

    /* Open IRAF header */
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

    /* Read FITS image header */
    else {
	iraffile = 0;
	if ((header = fitsrhead (filename, &lhead, &nbhead)) != NULL) {
	    naxis = 0;
	    hgeti4 (header, "NAXIS", &naxis);
	    bitpix = 0;
	    hgeti4 (header, "BITPIX", &bitpix);
	    if (naxis < 1 || bitpix == 0) {
		if (verbose)
		    fprintf (stderr, "Dataless FITS file %s\n", filename);
		}
	    else if ((image = fitsrimage (filename, nbhead, header)) == NULL) {
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

    /* Compute size of input image in bytes from header parameters */
    naxis = 0;
    naxis1 = 0;
    nbimage = 0;
    hgeti4 (header,"NAXIS1",&naxis1);
    if (naxis1 > 1) {
	naxis = naxis + 1;
	nbimage = naxis1;
	}
    naxis2 = 0;
    hgeti4 (header,"NAXIS2",&naxis2);
    if (naxis2 > 1) {
	naxis = naxis + 1;
	nbimage = nbimage * naxis2;
	}
    naxis3 = 0;
    hgeti4 (header,"NAXIS3",&naxis3);
    if (naxis3 > 1) {
	naxis = naxis + 1;
	nbimage = nbimage * naxis3;
	}
    hgeti4 (header,"BITPIX",&bitpix);
    bytepix = bitpix / 8;
    if (bytepix < 0) bytepix = -bytepix;
    nbimage = nbimage * bytepix;
    nbstack = nbstack + nbimage;

    /* Set NAXIS2 to # of images stacked; pad out FITS header to 2880 blocks */
    if (ifile < 1 && wfits) {
	if (naxis == 1) {
	    hputi4 (header,"NAXIS", 2);
	    hputi4 (header,"NAXIS2", nfiles*ntimes);
	    }
	else if (naxis == 2) {
	    hputi4 (header,"NAXIS", 3);
	    hputi4 (header,"NAXIS3", nfiles*ntimes);
	    }
	else if (naxis == 3) {
	    hputi4 (header,"NAXIS", 4);
	    hputi4 (header,"NAXIS4", nfiles*ntimes);
	    }

	/* Add IRAF multispec header keywords, if requested */
	if (multispec) {
	    hputi4 (header,"NAXIS",3);
	    hputi4 (header,"NAXIS2",1);
	    hputi4 (header,"NAXIS3",nfiles);
	    hputi4 (header,"WCSDIM",3);
	    hputs (header,"BANDID2","raw spectrum");
	    hputs (header,"BANDID3","averaged sky");
	    hputs (header,"WAT2_001","wtype=linear");
	    hputs (header,"WAT3_001","wtype=linear");
	    hputnr8 (header,"CD2_2",1.0,1);
	    hputnr8 (header,"CD3_3",1.0,1);
	    hputnr8 (header,"LTM2_2",1.0,1);
	    hputnr8 (header,"LTM3_3",1.0,1);
	    }

	nbhead = strlen (header);
	nblocks = nbhead / FITSBLOCK;
	if (nblocks * FITSBLOCK < nbhead)
	    nblocks = nblocks + 1;
	nbytes = nblocks * FITSBLOCK;
	for (i = nbhead+1; i < nbytes; i++)
	    header[i] = ' ';
	nbhead = nbytes;
	}

    /* If first file, open to write and, optionally, write FITS header */
    if (ifile == 0) {
	if (extend)
	    hputl (header, "EXTEND", 1);
	fstack = fopen (newname, "w");
	if (fstack == NULL) {
	    fprintf (stderr, "Cannot write image %s\n", newname);
	    return (1);
	    }
	}
    else if (fstack == NULL) {
	fstack = fopen (newname, "a");
	if (fstack == NULL) {
	    fprintf (stderr, "Cannot write image %s\n", newname);
	    return (1);
	    }
	}
    if (extend && ifile > 0) {
	hchange (header, "SIMPLE", "XTENSION");
	hputs (header, "XTENSION", "IMAGE");
	roothead = ksearch (header, "ROOTHEAD");
	rootend = ksearch (header, "ROOTEND");
	if (roothead && rootend) {
	    headend = ksearch (header, "END");
	    for (i = 0; i < 80; i++)
		spaces[i] = ' ';
	    for (iline = rootend+80; iline <= headend; iline = iline + 80){
		strncpy (iline, roothead, 80);
		strncpy (spaces, iline, 80);
		roothead = roothead + 80;
		}
	    }
	}
    for (itime = 0; itime < ntimes; itime++) {
	nout = (ifile * ntimes) + itime + 1;

	/* Write header before each data unit if writing extensions */
	if (extend) {
	    if (ifile > 0) {
		snprintf (extname, 15, "%s%d", extroot, nout-1);
		hplace = ksearch (header, "NAXIS4");
		if (!hplace)
		    hplace = ksearch (header, "NAXIS3");
		if (!hplace)
		    hplace = ksearch (header, "NAXIS2");
		if (!hplace)
		    hplace = ksearch (header, "NAXIS1");
		if (hplace) {
		    hplace = hplace + 80;
		    hadd (hplace, "EXTNAME");
		    hputs (header, "EXTNAME", extname);
		    }
		else
		    hputs (header, "EXTNAME", extname);
		}
	    if (wfits) {
		if (fwrite (header, (size_t) nbhead, (size_t) 1, fstack)) {
		    if (verbose) {
			printf ("%d-byte FITS header from %s written to %s[%d]\n",
			    nbhead, filename, newname, nout);
			}
		    }
		else
		    printf ("FITS file %s cannot be written.\n", newname);
		}
	    }

	/* Otherwise write header once before first data unit */
	else if (itime ==0 && ifile == 0) {
	    if (fwrite (header, (size_t) nbhead, (size_t) 1, fstack)) {
		if (verbose) {
		    printf ("%d-byte FITS header from %s written to %s[%d]\n",
			    nbhead, filename, newname, nout);
		    }
		}
	    else
		printf ("FITS file %s cannot be written.\n", newname);
	    }

	/* Write data */
        if (nbimage > 0 && image != NULL) {

	    /* Swap data if it has been swapped for this architecture */
	    if (imswapped())
		imswap (bitpix,image, nbimage);

	    if (fwrite (image, (size_t) 1, (size_t) nbimage, fstack)) {
		if (verbose) {
		    if (iraffile)
			printf ("IRAF %d bytes of file %s added to %s[%d]",
			    nbimage, filename, newname, nout);
		    else
			printf ("FITS %d bytes of file %s added to %s[%d]",
			    nbimage, filename, newname, nout);
		    }

		/* if extension, pad it out to 2880 blocks */
		if (extend) {
		    if (wfits && fstack != NULL) {
			nblocks = nbimage / FITSBLOCK;
			if (nblocks * FITSBLOCK < nbimage)
			    nblocks = nblocks + 1;
			nbytes = (nblocks * FITSBLOCK) - nbimage;
			if (nbytes > 0) {
			    blanks = (char *) malloc ((size_t) nbytes);
			    for (i = 0;  i < nbytes; i++)
				blanks[i] = 0;
			    (void) fwrite (blanks, (size_t) 1, (size_t)nbytes, fstack);
			    free (blanks);
			    }
			}
		    }
		if (verbose) {
		   if (itime == 0 || itime == ntimes-1)
			printf ("\n");
		    else
			printf ("\r");
		    }
		}
	    else {
		if (iraffile)
		    printf ("IRAF file %s NOT added to %s[%d]\n",
		        filename, newname, nout);
		else
		    printf ("FITS file %s NOT added to %s[%d]\n",
		        filename, newname, nout);
		}
	    }
	}

    if (ifile < 1) {
	fclose (fstack);
	fstack = NULL;
	}

    free (header);
    free (image);
    return (0);
}

/* May 15 1997	New program
 * May 30 1997	Fix FITS data padding to integral multiple of 2880 bytes
 *
 * May 28 1998	Include fitsio.h instead of fitshead.h
 * Jul 24 1998	Make irafheader char instead of int
 * Aug  6 1998	Change fitsio.h to fitsfile.h
 * Oct 14 1998	Use isiraf() to determine file type
 * Nov 30 1998	Add version and help commands for consistency
 *
 * Oct 22 1999	Drop unused variables after lint
 *
 * Feb  7 2000	Add option to repeat files in stack
 * Mar 23 2000	Use hgetm() to get the IRAF pixel file name, not hgets()
 * Sep  6 2000	Add -o option to set output filename
 * Sep  8 2000	Default ntimes to 1 so program works
 *
 * Apr  9 2002	Do not free unallocated header
 *
 * Aug  1 2003	Add option to build multi-extension FITS files
 * Aug  8 2003	Drop header from ROOTHEAD to ROOTEND when make multi-ext. FITS
 * Aug 21 2003	Fix bug when stacking 2-D files
 * Sep 17 2003	Change variable inline to iline for Redhat Linux
 *
 * Jan  5 2006	Print CR or LF after stacking only in verbose mode
 * Jun 21 2006	Clean up code
 *
 * Jan  5 2007	Drop extra argument in call to hadd()
 * Aug 30 2007	Add -m to stack IRAF multispec files
 */
