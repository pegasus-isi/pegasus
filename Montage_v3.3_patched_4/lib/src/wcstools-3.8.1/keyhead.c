/* File keyhead.c
 * August 19, 2009
 * By Doug Mink Harvard-Smithsonian Center for Astrophysics)
 * Send bug reports to dmink@cfa.harvard.edu

   Copyright (C) 1997-2009
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

#define MAXKWD 50
#define MAXFILES 1000
static int maxnkwd = MAXKWD;
static int maxnfile = MAXFILES;
extern char fitserrmsg;

static void usage();
static void ChangeKeyNames ();

static int verbose = 0;		/* verbose/debugging flag */
static int newimage = 0;	/* write new image with modified header */
static int replace = 0;		/* replace value of first keyword with second */
static int keyset = 0;
static int histset = 0;
static int version = 0;		/* If 1, print only program name and version */
static int logfile = 0;
static int nproc = 0;
static int first_file = 1;

static char *RevMsg = "KEYHEAD WCSTools 3.8.1, 14 December 2009, Doug Mink (dmink@cfa.harvard.edu)";

int
main (ac, av)
int ac;
char **av;
{
    char *str;
    char **kwd, **kwdnew;
    int nkwd = 0;
    int nkwd1 = 0;
    char **fn, **newfn;
    int nfile = 0;
    int ifile;
    int i;
    int nbytes;
    int readlist = 0;
    char filename[128];
    FILE *flist = NULL;
    FILE *fdk;
    char *listfile;
    char *ilistfile;
    char *klistfile;
    int ikwd;

    ilistfile = NULL;
    klistfile = NULL;

    fn = (char **)calloc (maxnfile, sizeof(char *));
    kwd = (char **)calloc (maxnkwd, sizeof(char *));

    /* Check for help or version command first */
    str = *(av+1);
    if (!str || !strcmp (str, "help") || !strcmp (str, "-help"))
	usage();
    if (!strcmp (str, "version") || !strcmp (str, "-version")) {
	version = 1;
	usage();
	}

    /* crack arguments */
    for (av++; --ac > 0; av++) {
	if (*(str = *av) == '-') {
	    char c;
	    while ((c = *++str))
	    switch (c) {

		case 'h':	/* set HISTORY */
		    histset++;
		    break;
	
		case 'k':	/* set KEYHEAD keyword */
		    keyset++;
		    break;
	
		case 'l':	/* Log files changed */
		    logfile++;
		    break;
	
		case 'n':	/* write to new file */
		    newimage = 1;
		    break;

		case 'r':	/* write to new file */
		    replace = 1;
		    break;
	
		case 'v':	/* more verbosity */
		    verbose++;
		    break;
	
		default:
		    usage();
		    break;
		}
	    }

	/* File containing a list of keywords or files */
	else if (*av[0] == '@') {
	    readlist++;
	    listfile = *av + 1;
	    if (isimlist (listfile)) {
		ilistfile = listfile;
		nfile = getfilelines (ilistfile);
		}
	    else {
		klistfile = listfile;
		nkwd1 = getfilelines (klistfile);
		if (nkwd1 > 0) {
		    if (nkwd+nkwd1 >= maxnkwd) {
			maxnkwd = nkwd + nkwd1 + 32;
			kwdnew = (char **) calloc (maxnkwd, sizeof (void *));
			for (ikwd = 0; ikwd < nkwd; ikwd++)
			    kwdnew[ikwd] = kwd[ikwd];
			free (kwd);
			kwd = kwdnew;
			}
		    if ((fdk = fopen (klistfile, "r")) == NULL) {
			fprintf (stderr,"KEYHEAD: File %s cannot be read\n",
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
		kwdnew = (char **) calloc (maxnkwd, sizeof (void *));
		for (ikwd = 0; ikwd < nkwd; ikwd++)
		    kwdnew[ikwd] = kwd[ikwd];
		free (kwd);
		kwd = kwdnew;
		}
	    kwd[nkwd++] = *av;
	    }
	}

    if (nkwd <= 0 && nfile <= 0 )
	usage ();
    else if (nkwd <= 0) {
	fprintf (stderr, "KEYHEAD: no keywords specified\n");
	exit (1);
	}
    else if (nfile <= 0 ) {
	fprintf (stderr, "KEYHEAD: no files specified\n");
	exit (1);
	}

    /* Open file containing a list of images, if there is one */
    if (ilistfile != NULL) {
	if ((flist = fopen (ilistfile, "r")) == NULL) {
	    fprintf (stderr,"KEYHEAD: Image list file %s cannot be read\n",
		     ilistfile);
	    usage ();
	    }
	}

    /* Read through headers of images */
    for (ifile = 0; ifile < nfile; ifile++) {
	if (ilistfile != NULL) {
	    first_token (flist, 254, filename);
	    ChangeKeyNames (filename, nkwd, kwd);
	    }
	else
	    ChangeKeyNames (fn[ifile], nkwd, kwd);

	if (verbose)
	    printf ("\n");
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
    fprintf (stderr,"Change FITS or IRAF header keyword names\n");
    fprintf(stderr,"Usage: [-hknv][-f num][-m num] file.fit [file.fits...] kw1=kw1a ... kwn=kwna\n");
    fprintf(stderr,"  or : [-nhkrv][-f num][-m num] @filelist kw1=kw1a kw2=kw2a ... kwn=kwna\n");
    fprintf(stderr,"  or : [-hknv][-f num][-m num] file.fit [file.fits...] @keywordlist\n");
    fprintf(stderr,"  or : [-nhkrv][-f num][-m num] @filelist @keywordlist\n");
    fprintf(stderr,"  -h: save procesing in HISTORY keyword in files\n");
    fprintf(stderr,"  -k: save procesing in KEYHEAD keyword in files\n");
    fprintf(stderr,"  -n: write new file\n");
    fprintf(stderr,"  -r: replace value of 1st keyword with value of 2nd keyword\n");
    fprintf(stderr,"  -v: verbose\n");
    exit (1);
}


static void
ChangeKeyNames (filename, nkwd, kwd)

char	*filename;	/* Name of FITS or IRAF image file */
int	nkwd;		/* Number of keywords for which to set values */
char	*kwd[];		/* Names and values of those keywords */

{
    char *image = NULL;	/* FITS image */
    char *header;	/* FITS image header */
    float rnum;
    double dnum;
    int lhead;		/* Maximum number of bytes in FITS header */
    int nbhead;		/* Actual number of bytes in FITS header */
    char *irafheader = NULL; /* IRAF image header */
    int iraffile;	/* 1 if IRAF image, 0 if FITS image */
    int lext, lroot, lhist;
    char *imext, *imext1;
    char newname[128];
    char oldvalue[64];
    char newvalue[32];
    char *ext, *fname;
    char *kw, *kwv, *kwl, *kwn;
    char *value, *q, *line, *ccol;
    char echar;
    int ikwd, lkwd, lkwn;
    int nbold, nbnew;
    int squote = 39;
    int dquote = 34;
    int bitpix = 0;
    int naxis = 0;
    int imageread = 0;
    char cval[24];
    int fdr, fdw, ipos, nbr, nbw, nchange;
    char history[72];
    char comment[72];
    char *endchar;
    char *ltime;

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
	    hgeti4 (header,"NAXIS",&naxis);
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
	    }
	else {
	    fprintf (stderr, "Cannot read FITS file %s\n", filename);
	    return;
	    }
	}
    if (verbose && first_file) {
	fprintf (stderr,"%s\n",RevMsg);
	fprintf (stderr,"Change Header Keyword Names from ");
	if (iraffile)
	    fprintf (stderr,"IRAF image file %s\n", filename);
	else
	    fprintf (stderr,"FITS image file %s\n", filename);
	first_file = 0;
	}

    if (nkwd < 1)
	return;

    nbold = fitsheadsize (header);

    /* Change keywords one at a time */
    nchange = 0;
    for (ikwd = 0; ikwd < nkwd; ikwd++) {
	if (strchr (kwd[ikwd],'=') != NULL) {
        strcpy (cval,"                    ");
	kwv = strchr (kwd[ikwd],'=');
	*kwv = 0;
	lkwd = kwv - kwd[ikwd];
	kwn = kwv + 1;
	lkwn = strlen (kwn);

	/* Make keywords all upper case */
	kwl = kwd[ikwd] + lkwd;
	for (kw = kwd[ikwd]; kw < kwl; kw++) {
	    if (*kw > 96 && *kw < 123)
		*kw = *kw - 32;
	    }
	kwl = kwn + lkwn;
	for (kw = kwn; kw < kwl; kw++) {
	    if (*kw > 96 && *kw < 123)
		*kw = *kw - 32;
	    }

	/* Change keyword value */
	if (replace) {
	    if ((line = ksearch (header, kwn)) == NULL) {
		if (verbose) {
		    if (hgets (header, kwd[ikwd], 64, oldvalue))
			printf ("%s = %s already\n", kwd[ikwd], oldvalue);
		    else
			printf ("%s not found\n", kwn);
		    }
		}
	    else {
		q = strchr (line, squote);
		if (q == NULL)
		    q = strchr (line, dquote);
		value = hgetc (header, kwn);

		/* Some special replacements to standardize 2dF data */
		if (!strcmp (kwn, "UTDATE") && !strcmp (kwd[ikwd], "DATE-OBS")) {
		    if ((ccol = strchr (value, ':')) != NULL)
			*ccol = '-';
		    if ((ccol = strchr (value, ':')) != NULL)
			*ccol = '-';
		    hputs (header, kwd[ikwd], value);
		    if (verbose)
			printf ("%s = %s = '%s'\n", kwd[ikwd], kwn, value);
		    }
		else if (!strcmp (kwn, "OBSRA") && !strcmp (kwd[ikwd], "RA")) {
		    rnum = atof (value);
		    dnum = raddeg (rnum);
		    ra2str (newvalue, 32, dnum, 3);
		    hputs (header, kwd[ikwd], newvalue);
		    if (verbose)
			printf ("%s = %s = '%s'\n", kwd[ikwd], kwn, newvalue);
		    }
		else if (!strcmp (kwn, "OBSDEC") && !strcmp (kwd[ikwd], "DEC")) {
		    rnum = atof (value);
		    dnum = raddeg (rnum);
		    dec2str (newvalue, 32, dnum, 3);
		    hputs (header, kwd[ikwd], newvalue);
		    if (verbose)
			printf ("%s = %s = '%s'\n", kwd[ikwd], kwn, newvalue);
		    }

		else if (q != NULL && q < line+80) {
		    hputs (header, kwd[ikwd], value);
		    if (verbose)
			printf ("%s = %s = '%s'\n", kwd[ikwd], kwn, value);
		    }
		else {
		    hputc (header, kwd[ikwd], value);
		    if (verbose)
			printf ("%s = %s = %s\n", kwd[ikwd], kwn, value);
		    }
		}
	    }

	/* Change keyword name */
	else {
	    if ((line = ksearch (header, kwd[ikwd])) == NULL)
		continue;
	    if (!strcmp (kwn, "SIMPLE")) {
		hgets (header, kwd[ikwd], 32, oldvalue);
		sprintf (comment, " %s was %s", kwd[ikwd], oldvalue);
		hchange (header, kwd[ikwd], kwn);
		hputl (header, kwn, 1);
		hputcom (header, kwn, comment);
		}
	    else
		hchange (header, kwd[ikwd], kwn);
	    if (verbose)
		printf ("%s => %s\n", kwd[ikwd], kwn);
	    }
	*kwv = '=';
	nchange++;
	}
	}

    if (!nchange)
	return;

    /* Remove directory path and extension from file name */
    fname = strrchr (filename, '/');
    if (fname)
	fname = fname + 1;
    else
	fname = filename;

    /* Set image extension if there is one */
    imext = strchr (fname, ',');
    imext1 = NULL;
    if (imext == NULL) {
	imext = strchr (fname, '[');
	if (imext != NULL) {
	    imext1 = strchr (fname, ']');
	    *imext1 = (char) 0;
	    }
	}

    /* Add history to header */
    if (keyset || histset) {
	if (hgets (header, "KEYHEAD", 72, history))
	    hputc (header, "HISTORY", history);
	strcpy (history, RevMsg);
	endchar = strchr (history, ',');
	*endchar = (char) 0;
	strcat (history, " ");
	ltime = lt2fd ();
	strcat (history, ltime);
	endchar = strrchr (history,':');
	*endchar = (char) 0;
	strcat (history, " ");
	for (ikwd = 0; ikwd < nkwd; ikwd++) {
	    lhist = strlen (history);
	    lkwd = strlen (kwd[ikwd]);

	    /* If too may keywords, start a second history line */
	    if (lhist + lkwd > 71) {
		if (histset) {
		    strcat (history, " updated");
		    hputc (header, "HISTORY", history);
		    strcpy (history, RevMsg);
		    endchar = strchr (history, ',');
		    *endchar = (char) 0;
		    strcat (history, " ");
		    ltime = lt2fd ();
		    strcat (history, ltime);
		    endchar = strrchr (history,':');
		    *endchar = (char) 0;
		    strcat (history, " ");
		    }
		else
		    break;
		}
	    strcat (history, kwd[ikwd]);
	    if (nkwd == 2 && ikwd < nkwd-1)
		strcat (history, " and ");
	    else if (ikwd < nkwd-1)
		strcat (history, ", ");
	    }
	if (keyset)
	    hputs (header, "KEYHEAD", history);
	if (histset)
	    hputc (header, "HISTORY", history);
	}

    /* Compare size of output header to size of input header */
    nbnew = fitsheadsize (header);
    if (nbnew > nbold  && naxis == 0 && bitpix != 0) {
	if (verbose)
	    fprintf (stderr, "Rewriting primary header, copying rest of file\n")
;
	newimage = 1;
	}

    /* Make up name for new FITS or IRAF output file */
    if (newimage) {
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
	strcpy (newname, filename);
	if (!imext && ksearch (header,"XTENSION")) {
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
	free (image);
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

    /* Log the processing of this file, if requested */
    if (logfile) {
	nproc++;
	fprintf (stderr, "%d: %s processed.\r", nproc, newname);
	}

    free (header);
    return;
}

/* Dec 17 1997	New program
 *
 * May 28 1998	Include fitsio.h instead of fitshead.h
 * Jul 24 1998	Make irafheader char instead of int
 * Aug  6 1998	Change fitsio.h to fitsfile.h
 * Aug 14 1998	Preserve extension when creating new file name
 * Aug 14 1998	If changing primary header, write out entire input file
 * Oct  5 1998	Allow header changes even if no data is present
 * Oct  5 1998	Determine assignment arguments by presence of equal sign
 * Oct  5 1998	Use isiraf() to check for file type
 * Oct 28 1998	Add option to replace keyword value with that of other keyword
 * Nov 25 1998	Fix bug in keyword name change code
 * Nov 30 1998	Add version and help commands for consistency
 *
 * Mar 18 1999	Fix bug so that reading filenames from a file works
 * Apr  2 1999	Add warning if too many files or keywords on command line
 * Apr  2 1999	Add -f and -m to change maximum number of files or keywords
 * Jul 14 1999  Read lists of BOTH keywords and files simultaneously
 * Jul 14 1999  Reallocate keyword array if too many in file
 * Jul 15 1999	Reallocate keyword and file lists if default limits exceeded
 * Oct 22 1999	Drop unused variables after lint
 * Nov 30 1999	Cast realloc's
 *
 * Mar 22 2000	Use lt2fd() instead of getltime()
 * Jun  8 2000	If no files or keywords specified, say so
 *
 * Jan 30 2002	If changing keyword name to SIMPLE, set to T
 * Feb  4 2002	Add time and angle conversions for 2dF keyword changes
 * Feb  5 2002	Add -l command to log files as they are processed
 *
 * Aug 21 2003	Read image with fitsrfull() to deal with n dimensions
 * Oct 29 2003	Allow combination of keyword designation methods
 *
 * May  6 2004	Add code to write into FITS extension headers in situ
 * Jul  1 2004	Change first extension if no extension specified
 *
 * Mar  1 2005	Print program information only on first file if looping
 *
 * Jun 21 2006	Clean up code
 *
 * Nov 09 2007	Add more verbosity replacing value from another keyword
 *
 * Aug 19 2009	Fix bug to remove limit to the number of files on command line
 */
