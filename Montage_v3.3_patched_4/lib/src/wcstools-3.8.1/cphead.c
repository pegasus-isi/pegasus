/* File cphead.c
 * ugust 19, 2009
 * By Doug Mink Harvard-Smithsonian Center for Astrophysics)
 * Send bug reports to dmink@cfa.harvard.edu

   Copyright (C) 2000-2009
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

#define MAXKWD 100
#define MAXFILES 2000
static int maxnkwd = MAXKWD;
static int maxnfile = MAXFILES;

static void usage();
static void CopyValues();
extern char *GetFITShead();

static int verbose = 0;		/* verbose/debugging flag */
static char *RevMsg = "CPHEAD WCSTools 3.8.1, 14 December 2009, Doug Mink (dmink@cfa.harvard.edu)";
static int copyall = 0;		/* Copy entire header, overwriting old one */
static int nfile = 0;
static int ndec0 = -9;
static int maxlfn = 0;
static int listpath = 0;
static int newimage0 = 0;
static int keyset = 0;
static int histset = 0;
static int version = 0;		/* If 1, print only program name and version */
static char *rootdir=NULL;	/* Root directory for input files */

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
    int ifile;
    int lfn;
    int nbytes;
    char filename[256];
    char *infile;
    char *name;
    FILE *flist = NULL;
    FILE *fdk;
    char *listfile;
    char *ilistfile;
    char *klistfile;
    int ikwd, i;
    char keyword[16];

    ilistfile = NULL;
    klistfile = NULL;
    infile = NULL;
    nfile = 0;
    fn = (char **)calloc (maxnfile, sizeof(char *));
    kwd = (char **)calloc (maxnkwd, sizeof(char *));
    for (i = 0; i < maxnkwd; i++)
	kwd[i] = (char *) calloc (9, sizeof(char));

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
	if ((*(str = *av))=='-') {
	    char c;
	    while ((c = *++str))
	    switch (c) {

		case 'a':	/* Copy entire header */
		    copyall++;
		    break;

		case 'd': /* Root directory for input */
		    if (ac < 2)
			usage();
		    rootdir = *++av;
		    ac--;
		    break;

		case 'h':	/* Set HISTORY */
		    histset++;
		    break;
	
		case 'k':	/* Set CPHEAD keyword */
		    keyset++;
		    break;

		case 'n':	/* Write new file */
		    newimage0++;
		    break;

		case 'p': /* Number of decimal places in output */
		    if (ac < 2)
			usage();
		    ndec0 = (int) (atof (*++av));
		    ac--;
		    break;
	
		case 'v': /* More verbosity */
		    verbose++;
		    break;
	
		case 'w': /* Copy entire WCS */
		    nkwd1 = 87;
		    if (nkwd + nkwd1 > maxnkwd) {
			maxnkwd = nkwd + nkwd1 + 32;
			kwdnew = (char **) calloc (maxnkwd, sizeof (void *));
			for (i = 0; i < maxnkwd; i++)
			    kwd[i] = (char *) calloc (9, sizeof(char));
			for (ikwd = 0; ikwd < nkwd; ikwd++)
			    kwdnew[ikwd] = kwd[ikwd];
			free (kwd);
			kwd = kwdnew;
			}
		    for (ikwd = nkwd; i < nkwd+nkwd1; i++) {
			kwd[ikwd] = (char *) calloc (32, 1);
			}
		    strcpy (kwd[nkwd], "RA");
		    strcpy (kwd[++nkwd], "DEC");
		    strcpy (kwd[++nkwd], "EPOCH");
		    strcpy (kwd[++nkwd], "EQUINOX");
		    strcpy (kwd[++nkwd], "RADECSYS");
		    strcpy (kwd[++nkwd], "SECPIX");
		    strcpy (kwd[++nkwd], "SECPIX1");
		    strcpy (kwd[++nkwd], "SECPIX2");
		    strcpy (kwd[++nkwd], "CTYPE1");
		    strcpy (kwd[++nkwd], "CTYPE2");
		    strcpy (kwd[++nkwd], "CRVAL1");
		    strcpy (kwd[++nkwd], "CRVAL2");
		    strcpy (kwd[++nkwd], "CDELT1");
		    strcpy (kwd[++nkwd], "CDELT2");
		    strcpy (kwd[++nkwd], "CRPIX1");
		    strcpy (kwd[++nkwd], "CRPIX2");
		    strcpy (kwd[++nkwd], "CROTA1");
		    strcpy (kwd[++nkwd], "CROTA2");
		    strcpy (kwd[++nkwd], "IMWCS");
		    strcpy (kwd[++nkwd], "CD1_1");
		    strcpy (kwd[++nkwd], "CD1_2");
		    strcpy (kwd[++nkwd], "CD2_1");
		    strcpy (kwd[++nkwd], "CD2_2");
		    strcpy (kwd[++nkwd], "PC1_1");
		    strcpy (kwd[++nkwd], "PC1_2");
		    strcpy (kwd[++nkwd], "PC2_1");
		    strcpy (kwd[++nkwd], "PC2_2");
		    strcpy (kwd[++nkwd], "PC001001");
		    strcpy (kwd[++nkwd], "PC001002");
		    strcpy (kwd[++nkwd], "PC002001");
		    strcpy (kwd[++nkwd], "PC002002");
		    strcpy (kwd[++nkwd], "LATPOLE");
		    strcpy (kwd[++nkwd], "LONPOLE");
		    for (i = 1; i < 13; i++) {
			sprintf (keyword,"CO1_%d", i);
			strcpy (kwd[++nkwd], keyword);
			}
		    for (i = 1; i < 13; i++) {
			sprintf (keyword,"CO2_%d", i);
			strcpy (kwd[++nkwd], keyword);
			}
		    for (i = 0; i < 10; i++) {
			sprintf (keyword,"PROJP%d", i);
			strcpy (kwd[++nkwd], keyword);
			}
		    for (i = 0; i < 10; i++) {
			sprintf (keyword,"PV1_%d", i);
			strcpy (kwd[++nkwd], keyword);
			}
		    for (i = 0; i < 10; i++) {
			sprintf (keyword,"PV2_%d", i);
			strcpy (kwd[++nkwd], keyword);
			}
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
			maxnkwd = nkwd + nkwd1 + 32;
			kwdnew = (char **) calloc (maxnkwd, sizeof (void *));
			for (ikwd = 0; ikwd < nkwd; ikwd++)
			    kwdnew[ikwd] = kwd[ikwd];
			free (kwd);
			kwd = kwdnew;
			}
		    if ((fdk = fopen (klistfile, "r")) == NULL) {
			fprintf (stderr,"GETHEAD: File %s cannot be read\n",
				 klistfile);
			nkwd = 0;
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
	    if (infile == NULL)
		infile = *av;
	    else {
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

		if (listpath || (name = strrchr (fn[nfile],'/')) == NULL)
		    name = fn[nfile];
		else
		    name = name + 1;
		lfn = strlen (name);
		if (lfn > maxlfn)
		    maxlfn = lfn;
		nfile++;
		}
	    }

	/* Keyword */
	else {
	    if (nkwd >= maxnkwd) {
		maxnkwd = maxnkwd * 2;
		kwd = (char **) realloc ((void *)kwd, maxnkwd);
		}
	    kwd[nkwd++] = *av;
	    }
	}

    if (nkwd <= 0 && nfile <= 0 )
	usage ();
    else if (!copyall && nkwd <= 0) {
	fprintf (stderr, "CPHEAD: no keywords specified\n");
	exit (1);
	}
    else if (nfile <= 0 ) {
	fprintf (stderr, "CPHEAD: no files specified\n");
	exit (1);
	}

    /* Open file containing a list of images, if there is one */
    if (ilistfile != NULL) {
	if ((flist = fopen (ilistfile, "r")) == NULL) {
	    fprintf (stderr,"GETHEAD: Image list file %s cannot be read\n",
		     ilistfile);
	    usage ();
	    }
	}
    if (nfile < 1)
	usage();

    /* Read through headers of images */
    for (ifile = 0; ifile < nfile; ifile++) {
	if (ilistfile != NULL) {
	    first_token (flist, 254, filename);
	    CopyValues (infile, filename, nkwd, kwd);
	    }
	else
	    CopyValues (infile, fn[ifile], nkwd, kwd);

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
    fprintf (stderr,"Copy FITS or IRAF header keyword values from first file to others\n");
    fprintf(stderr,"Usage: cphead [-v][-d dir][-p num] file1.fit ... filen.fits kw1 kw2 ... kwn\n");
    fprintf(stderr,"  or : cphead [-v][-d dir][-p num] file1.fit @filelist kw1 kw2 ... kwn\n");
    fprintf(stderr,"  or : cphead [-v][-d dir][-p num] file1.fit @filelist @kwlist\n");
    fprintf(stderr,"  -a: Replace header of 2nd file with that from first\n");
    fprintf(stderr,"  -d: Root directory for input files (default is cwd)\n");
    fprintf(stderr,"  -h: Write HISTORY line\n");
    fprintf(stderr,"  -k: Write CPHEAD keyword\n");
    fprintf(stderr,"  -n: Write a new file (add e before the extension)\n");
    fprintf(stderr,"  -p: Number of decimal places in numeric output\n");
    fprintf(stderr,"  -v: Verbose\n");
    fprintf(stderr,"  -w: Copy all WCS keywords\n");
    exit (1);
}


static void
CopyValues (infile, filename, nkwd, kwd)

char	*infile;	/* FITS or IRAF image file from which to read */
char	*filename;	/* FITS or IRAF image file to which to write */
int	nkwd;		/* Number of keywords for which to copy values */
char	*kwd[];		/* Names of keywords for which to copy values */

{
    char *header;	/* FITS image header from which to copy */
    char *headin;	/* FITS image header to which to add */
    char *headout;	/* FITS image header to output */
    int newimage;
    char *irafheader = NULL;	/* IRAF image header */
    char *image = NULL;	/* Input and output image buffer */
    double dval;
    int ival, nch, inum;
    int iraffile;
    int ndec, nbheadout, nbheadin, nbheader;
    char newname[128];
    char string[80];
    char *kw, *kwe, *filepath, *fname, *ext, *imext, *imext1;
    char *kwv0;
    int ikwd, lkwd, nfound, notfound, lext, lroot, lhist, lhead;
    char *ltime;
    int naxis, ipos, nbhead, nbr, nbw;
    int fdr, fdw;
    char history[72];
    char echar;
    char *endchar;
    int imageread = 0;

    newimage = newimage0;

    if (rootdir) {
	nch = strlen (rootdir) + strlen (infile) + 2;
	filepath = (char *) calloc (1, nch);
	strcat (filepath, rootdir);
	strcat (filepath, "/");
	strcat (filepath, infile);
	}
    else
	filepath = infile;

    /* Retrieve FITS header from FITS or IRAF .imh file */
    if ((header = GetFITShead (filepath, verbose)) == NULL)
	return;

    /* Open IRAF image if .imh extension is present */
    if (isiraf (filename)) {
	iraffile = 1;
	if ((irafheader = irafrhead (filename, &lhead)) != NULL) {
	    if ((headin = iraf2fits (filename, irafheader, lhead, &nbhead)) == NULL) {
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
	if ((headin = fitsrhead (filename, &lhead, &nbhead)) != NULL) {
	    hgeti4 (headin,"NAXIS",&naxis);
	    if (naxis > 0) {
		if ((image = fitsrfull (filename, nbhead, headin)) == NULL) {
		    if (verbose)
			fprintf (stderr, "No FITS image in %s\n", filename);
		    imageread = 0;
		    }
		else
		    imageread = 1;
		}
	    else {
		if (verbose)
		    fprintf (stderr, "Rewriting primary header, copying rest\n");
		newimage = 1;
		}
	    }
	else {
	    fprintf (stderr, "Cannot read FITS file %s\n", filename);
	    return;
	    }
	}

    /* Allocate output FITS header and copy original header into it */
    nbheader = strlen (header);
    nbheadin = nbhead;
    if (copyall) {
	nbhead = strlen (header);
	headout = header;
	}
    else {
	nbheadout = nbheadin + (nkwd * 80);
	headout = (char *) calloc (nbheadout, 1);
	strncpy (headout, headin, nbheadin);
	}

    if (verbose) {
	fprintf (stderr,"%s\n",RevMsg);
	if (copyall)
	    fprintf (stderr,"Copy Header from ");
	else
	    fprintf (stderr,"Read %d Header Parameter Values from ", nkwd);
	hgeti4 (header, "IMHVER", &iraffile );
	if (iraffile)
	    fprintf (stderr,"IRAF image file %s\n", infile);
	else
	    fprintf (stderr,"FITS image file %s\n", infile);
	hgeti4 (headout, "IMHVER", &iraffile );
	if (iraffile)
	    fprintf (stderr,"and write to IRAF image file %s\n", filename);
	else
	    fprintf (stderr,"and write to FITS image file %s\n", filename);
	}

    nfound = 0;
    notfound = 0;
    for (ikwd = 0; ikwd < nkwd; ikwd++) {
	lkwd = strlen (kwd[ikwd]);
	kwe = kwd[ikwd] + lkwd;
	for (kw = kwd[ikwd]; kw < kwe; kw++) {
	    if (*kw > 96 && *kw < 123)
		*kw = *kw - 32;
	    }
	(void) hlength (header, nbheader);
	if (hgets (header, kwd[ikwd], 80, string)) {
	    strfix (string,0,0);
	    inum = isnum (string);
	    if (inum == 2) {
		if (ndec0 > -9)
		    ndec = ndec0;
		else
		    ndec = numdec (string);
		hgetr8 (header, kwd[ikwd], &dval);
		(void) hlength (headout, nbheadout);
		hputnr8 (headout, kwd[ikwd], ndec, dval);
		}
	    else if (inum == 1) {
		hgeti4 (header, kwd[ikwd], &ival);
		(void) hlength (headout, nbheadout);
		hputi4 (headout, kwd[ikwd], ival);
		}
	    else {
		(void) hlength (headout, nbheadout);
		hputs (headout, kwd[ikwd], string);
		}

	    if (verbose)
		printf ("%03d: %s = %s\n", ikwd, kwd[ikwd], string);
	    nfound++;
	    }
	else if (verbose)
	    printf ("%s not found\r", kwd[ikwd]);
	else
	    notfound++;
	}

    if (nfound < 1 && !copyall) {
	if (verbose)
	    fprintf (stderr, "No keywords on list in input file %s\n", infile);
	return;
	}
    if (notfound > 0 && verbose)
	fprintf (stderr, "%d keywords missing from input file %s\n",
		 notfound, infile);

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
    else
	strcpy (newname, filename);

    /* Add history to header */
    if (keyset || histset) {
	if (hgets (headout, "CPHEAD", 72, history))
	    hputc (headout, "HISTORY", history);
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
	    kwv0 = strchr (kwd[ikwd],'=');
	    if (kwv0)
		*kwv0 = (char) 0;
	    lhist = strlen (history);
	    lkwd = strlen (kwd[ikwd]);

	    /* If too may keywords, start a second history line */
	    if (lhist + lkwd + 10 > 71) {
		if (histset) {
		    if (history[lhist-2] == ',')
			history[lhist-2] = (char) 0;
		    strcat (history, " updated");
		    hputc (headout, "HISTORY", history);
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
	    if (kwv0)
		*kwv0 = '=';
	    if (nkwd == 2 && ikwd < nkwd-1)
		strcat (history, " and ");
	    else if (ikwd < nkwd-1)
		strcat (history, ", ");
	    }
	strcat (history, " updated");
	if (keyset)
	    hputs (headout, "CPHEAD", history);
	if (histset)
	    hputc (headout, "HISTORY", history);
	}

    /* Write fixed header to output file */
    if (iraffile) {
	if (irafwhead (newname, lhead, irafheader, headout) > 0 && verbose)
	    printf ("%s rewritten successfully.\n", newname);
	else if (verbose)
	    printf ("%s could not be written.\n", newname);
	free (irafheader);
	}
    else if (naxis > 0 && imageread) {
	if (fitswimage (newname, headout, image) > 0 && verbose)
	    printf ("%s: rewritten successfully.\n", newname);
	else if (verbose)
	    printf ("%s could not be written.\n", newname);
	free (image);
	}
    else {
	if ((fdw = fitswhead (newname, headout)) > 0) {
	    fdr = fitsropen (filename);
	    ipos = lseek (fdr, nbhead, SEEK_SET);
	    image = (char *) calloc (2880, 1);
	    while ((nbr = read (fdr, image, 2880)) > 0) {
		nbw = write (fdw, image, nbr);
		if (nbw < nbr)
		    fprintf (stderr,"CPHEAD: %d / %d bytes written\n",nbw,nbr);
		}
	    close (fdr);
	    close (fdw);
	    if (verbose)
		printf ("%s: rewritten successfully.\n", newname);
	    free (image);
	    }
	}

    free (header);
    free (headout);
    return;
}

/* Feb 24 2000	New program based on sethead and gethead
 * Mar 22 2000	Use lt2fd() instead of getltime()
 * Jun  8 2000	If no files or keywords specified, say so
 *
 * Jun 19 2002	Add verbose argument to GetFITShead()
 *
 * Aug 21 2003	Use fitsrfull() to deal with n-dimensional FITS images
 * Oct 20 2003	Keep same number of decimal places unless -p option used
 * Oct 23 2003	Add -w option to copy all WCS keywords
 * Oct 29 2003	Allow combination of keyword designation methods
 * Dec  4 2003	Initialize infile to null (bug found by Jean-Francois Le Borgne)
 *
 * Apr 15 2004	Avoid removing trailing zeroes from exponents
 *
 * Jan 13 2005	Print linefeed after verbose confirmation, not after missing kw
 * Aug 26 2005	Improve one-line description
 * Aug 30 2005	Write output header into new, adequately long array
 *
 * Feb 22 2006	Allocate keyword name buffer correctly
 * May 31 2006	Add diagnostic messages
 * Jun 20 2006	Drop unused variables
 * Jun 29 2006	Rename strclean() strfix() and move to hget.c
 *
 * Jan 10 2007	Add second set of parentheses in line 100 if clause
 * May 15 2007	Add -a option to copy an entire header from one image to another
 *
 * May 28 2008	Clean up value string copying code
 *
 * Aug 19 2009	Fix bug to remove limit to the number of files on command line
 */
