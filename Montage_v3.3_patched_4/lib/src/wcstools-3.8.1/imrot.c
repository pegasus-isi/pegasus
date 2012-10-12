/* File imrot.c
 * September 25, 2009
 * By Doug Mink, Harvard-Smithsonian Center for Astrophysics
 * Send bug reports to dmink@cfa.harvard.edu
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
static void imRot ();
extern char *RotFITS();
extern int DelWCSFITS();

#define MAXFILES 1000
static int maxnfile = MAXFILES;

static int verbose = 0;	/* verbose/debugging flag */
static int mirror = 0;	/* reflect image across vertical axis */
static int automirror = 0;	/* reflect image if IRAF header says to */
static int rotate = 0;	/* rotation in degrees, degrees counter-clockwise */
char outname[128];		/* Name for output image */
static int bitpix = 0;	/* number of bits per pixel (FITS code) */
static int fitsout = 0;	/* Output FITS file from IRAF input if 1 */
static int nsplit = 0;	/* Output multiple FITS files from n-extension file */
static int overwrite = 0;	/* allow overwriting of input image file */
static int version = 0;		/* If 1, print only program name and version */
static int xshift = 0;
static int yshift = 0;
static int shifted = 0;
static int inverted = 0;	/* If 1, invert intensity (-1 * (z-zmax)) */
static int deletewcs = 0;	/* If 1, delete FITS WCS keywords in image */
static int rotatewcs = 1;	/* If 1, rotate FITS WCS keywords in image */
static int extnum = 0;		/* Use extension number instead of EXTNAME for output */
static char *RevMsg = "IMROT WCSTools 3.8.1, 14 December 2009, Doug Mink (dmink@cfa.harvard.edu)";

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
    char *fni;
    int lfn, i, ifile, nfile;
    double dx, dy;

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

	/* Set image shifts */
	else if (isnum (str)) {
	    if (shifted) {
		dy = atof (str);
		if (dy < 0)
		    yshift = (int) (dy - 0.5);
		else
		    yshift = (int) (dy + 0.5);
		}
	    else {
		shifted = 1;
		dx = atof (str);
		if (dx < 0)
		    xshift = (int) (dx - 0.5);
		else
		    xshift = (int) (dx + 0.5);
		}
	    }

	/* Parameters */
	else if (str[0] == '+') {
	    if (str[1] == 'i')
		setfitsinherit (1);
	    }
	else if (str[0] == '-') {
	    while ((c = *++str) != 0) {
		switch (c) {
 	   	case 'f':	/* FITS file output */
		    fitsout++;
		    break;

		case 'a': /* Flip image around N-S axis if IRAF header says */
		    automirror = 1;
		    break;

		case 'e': /* Delete FITS WCS keywords in image header */
		    deletewcs = 1;
		    break;

		case 'l':	/* image flipped around horizontal axis */
		    mirror = 2;
		    break;

		case 'm':	/* image flipped around vertical axis */
		    mirror = 1;
		    break;

		case 'i':	/* Turn off inheritance from Primary header */
		    setfitsinherit (0);
		    break;

		case 'n':	/* Use extension number instead of EXTNAME */
		    extnum++;
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

		case 'p':	/* Make positive image from negative */
		    inverted++;
		    break;

		case 'r':	/* Rotation angle in degrees */
		    if (ac < 2)
			usage ();
		    rotate = (int) atof (*++av);
		    if (rotate < 0)
			rotate = rotate + 360.0;
		    ac--;
		    break;

		case 's':	/* split input FITS multiextension image file */
		    if (ac < 2)
			usage ();
		    nsplit = atoi (*++av);
		    ac--;
		    break;

		case 'v':	/* more verbosity */
		    verbose++;
		    break;

		case 'w': /* Do not rotate FITS WCS keywords in image header */
		    rotatewcs = 0;
		    break;

		case 'x':	/* Number of bits per pixel */
		    if (ac < 2)
			usage ();
		    bitpix = (int) atof (*++av);
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
	    fprintf (stderr,"IMROT: %s is not a FITS or IRAF file \n",str);
            usage();
            }

    }

    /* Process files in file of filenames */
    if (readlist) {
	if ((flist = fopen (listfile, "r")) == NULL) {
	    fprintf (stderr,"IMROT: List file %s cannot be read\n",
		     listfile);
	    usage ();
	    }
	while (fgets (filename, 128, flist) != NULL) {
	    lastchar = filename + strlen (filename) - 1;
	    if (*lastchar < 32) *lastchar = 0;
	    imRot (filename);
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
	    if (nsplit > 0) {
		fni = (char *)calloc (lfn+4, 1);
		for (i = 1; i <= nsplit; i++) {
		    sprintf (fni, "%s,%d", fname, i);
		    if (verbose)
			printf ("%s:\n", fni);
		    imRot (fni);
		    if (verbose)
  			printf ("\n");
		    }
		free (fni);
		}
	    else {
		if (verbose)
		    printf ("%s:\n", fname);
		imRot (fname);
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
    fprintf (stderr,"Shift, Rotate, and/or Reflect FITS and IRAF image files\n");
    fprintf(stderr,"Usage: [-vm][-r rot][-s num] [xshift yshift] file.fits ...\n");
    fprintf(stderr,"  xshift: integer horizontal pixel shift, applied first\n");
    fprintf(stderr,"  yshift: integer vertical pixel shift, applied first\n");
    fprintf(stderr,"  -a: Mirror if IRAF image WCS says to\n");
    fprintf(stderr,"  -e: Delete FITS WCS keywords in image\n");
    fprintf(stderr,"  -f: Write FITS image from IRAF input\n");
    fprintf(stderr,"  -i: Do not append primary header to extension header\n");
    fprintf(stderr,"  -l: Reflect image across horizontal axis\n");
    fprintf(stderr,"  -m: Reflect image across vertical axis\n");
    fprintf(stderr,"  -n: Use extension number instead of EXTNAME for output file name\n");
    fprintf(stderr,"  -o: Allow overwriting of input image, else write new one\n");
    fprintf(stderr,"  -p: Make positive from negative image\n");
    fprintf(stderr,"  -r: Image rotation angle in degrees (default 0)\n");
    fprintf(stderr,"  -s: Split n-extension FITS file\n");
    fprintf(stderr,"  -v: Verbose\n");
    fprintf(stderr,"  -w: Rotate FITS WCS keywords in image\n");
    fprintf(stderr,"  -x: Output pixel size in bits (FITS code, default=input)\n");
    exit (1);
}

static void
imRot (name)
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
    char *newimage;
    char *imext = NULL;
    char *imext1 = NULL;
    char *fname;
    char extname[16];
    int lext = 0;
    int lroot;
    int bitpix0;
    int nx, ny, npix;
    char echar;
    char temp[8];
    char history[64];
    char pixname[256];
    double ctemp;
    double dmax, dpix;
    double bs, bz;

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

    /* Set mirror flag if IRAF WCS is mirrored and automirror is set */
    if (hgetr8 (header, "LTM1_1", &ctemp)) {
	if (ctemp < 0 && automirror)
	    mirror = 1;
	}

    /* Use output filename if it is set on the command line */
    if (outname[0] > 0)
	strcpy (newname, outname);

    /* Create new file name */
    else if (!overwrite) {
	if (imext != NULL) {
	    if (!extnum && hgets (header, "EXTNAME",8,extname)) {
		strcat (newname, "_");
		strcat (newname, extname);
		}
	    else {
		strcat (newname, "_");
		strcat (newname, imext+1);
		}
	    }
	if (inverted)
	    strcat (newname, "p");
	if (shifted)
	    strcat (newname, "s");
	if (mirror == 1)
	    strcat (newname, "m");
	else if (mirror == 2)
	    strcat (newname, "f");
	if (rotate != 0) {
	    strcat (newname, "r");
	    if (rotate < 10 && rotate > -1)
		sprintf (temp,"%1d",rotate);
	    else if (rotate < 100 && rotate > -10)
		sprintf (temp,"%2d",rotate);
	    else if (rotate < 1000 && rotate > -100)
		sprintf (temp,"%3d",rotate);
	    else
		sprintf (temp,"%4d",rotate);
	    strcat (newname, temp);
	    }
	if (bitpix == -64)
	    strcat (newname, "bn64");
	else if (bitpix == -32)
	    strcat (newname, "bn32");
	else if (bitpix == -16)
	    strcat (newname, "bn16");
	else if (bitpix == 32)
	    strcat (newname, "b32");
	else if (bitpix == 16)
	    strcat (newname, "b16");
	else if (bitpix == 8)
	    strcat (newname, "b8");
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
	fprintf (stderr,"Shift, rotate, and/or reflect ");
	if (iraffile)
	    fprintf (stderr,"IRAF image file %s", name);
	else
	    fprintf (stderr,"FITS image file %s", name);
	fprintf (stderr, " -> %s\n", newname);
	}

    if (bitpix != 0) {
	hgeti4 (header, "BITPIX", &bitpix0);
	if (verbose)
	    fprintf (stderr, "IMROT: %d bits/pixel -> %d bits/pixel\n",
		     bitpix0, bitpix);
	sprintf (history, "New copy of %s BITPIX %d -> %d",
		 name, bitpix0, bitpix);
	hputc (header,"HISTORY",history);
	}

    /* Make positive image from negative if requested */
    if (inverted) {
	bitpix = 16;
	hgeti4 (header, "BITPIX", &bitpix0);
	bs = 1.0;
	hgetr8 (header, "BSCALE", &bs);
	bz = 0.0;
	hgetr8 (header, "BZERO", &bz);
	nx = 1;
	hgeti4 (header, "NAXIS1", &nx);
	ny = 1;
	hgeti4 (header, "NAXIS2", &ny);
	npix = nx * ny;
	dmax = -maxvec (image, bitpix0, bz, bs, 0, npix);
	addvec (image, bitpix0, bz, bs, 0, npix, dmax);
	dpix = -1.0;
	multvec (image, bitpix0, bz, bs, 0, npix, dpix);
	}

    /* Delete FITS WCS keywords in the image if requested */
    if (deletewcs) {
	DelWCSFITS (header, verbose);
	}

    if ((newimage = RotFITS (name,header,image,xshift,yshift,rotate,mirror,
			     bitpix,rotatewcs,verbose)) == NULL) {
	fprintf (stderr,"Cannot rotate image %s; file is unchanged.\n",name);
	}
    else {
	if (bitpix != 0)
	    hputi4 (header, "BITPIX", bitpix);
	if (iraffile && !fitsout) {
	    if (irafwimage (newname,lhead,irafheader,header,newimage) > 0) {
		if (verbose)
		    printf ("%s: written successfully.\n", newname);
		else
		    printf ("%s\n", newname);
		}
	    else if (verbose)
		printf ("IMROT: File %s not written.\n", newname);
	    }
	else {
	    if (fitswimage (newname, header, newimage) > 0) {
		if (verbose)
		    printf ("%s: written successfully.\n", newname);
		else
		    printf ("%s\n", newname);
		}
	    else if (verbose)
		printf ("IMROT: File %s not written.\n", newname);
	    }
	free (newimage);
	}

    free (header);
    if (iraffile)
	free (irafheader);
    free (image);
    return;
}
/* Apr 15 1996	New program
 * Apr 18 1996	Add option to write to current working directory
 * May  2 1996	Pass filename to rotFITS
 * May 28 1996	Change rotFITS to RotFITS
 * Jun  6 1996	Always write to current working directory
 * Jun 14 1996	Use single image buffer
 * Jul  3 1996	Allow optional overwriting of input image
 * Jul 16 1996	Update header reading and allocation
 * Aug 26 1996	Change HGETC call to HGETS; pass LHEAD in IRAFWIMAGE
 * Aug 27 1996	Remove unused variables after lint
 *
 * Feb 21 1997  Check pointers against NULL explicitly for Linux
 * Aug 13 1997	Fix bug when overwriting an image
 * Dec 15 1997	Add capability of reading and writing IRAF 2.11 images
 *
 * Feb 24 1998	Add ext. to filename if writing part of multi-ext. file
 * May 26 1998	Fix bug when writing .imh images
 * May 27 1998	Include fitsio.h instead of fitshead.h
 * Jul 24 1998	Make irafheader char instead of int
 * Aug  6 1998	Change fitsio.h to fitsfile.h
 * Oct 14 1998	Use isiraf() to determine file type
 * Nov 30 1998	Add version and help commands for consistency
 * Jun  8 1999  Return image pointer from RotFITS, not flag
 * Oct 22 1999	Drop unused variables after lint
 *
 * Jan 24 2000	Add to name if BITPIX is changed
 * Mar 23 2000	Use hgetm() to get the IRAF pixel file name, not hgets()
 * Jul 20 2000	Use .fits, not .fit, extension on output FITS file names
 * Jul 20 2000	Add -s option to split multi-extension FITS files
 * Sep 12 2000	Echo new file name to standard output, if not verbose
 * Sep 12 2000	Use .extname if multi-extension extraction, not _number
 *
 * Jan 18 2001	Add automirror -a option
 * Feb 13 2001	Add -o name option to -o argument (from imwcs)
 *
 * Jan 28 2004	Add option to shift file data within file (before rotating)
 * May  6 2004	Add -i to avoid appending primary header to extension header
 * Jun 14 2004	Write HISTORY only if BITPIX is changed
 *
 * Aug 17 2005	If rotation angle < 360, add 360
 * Aug 17 2005	-m replaces -l for mirror reflection, -l flips
 * Aug 18 2005	Add -p to make positive image from negative
 *
 * Jan 19 2006	Add +i to force inheritance of keywords from primary header
 * Jun 21 2006	Clean up code
 * Jun 22 2006	Check for two-token extension .ms.fit(s)
 *
 * Jan  5 2007	Fix BSCALE and BZERO hget calls
 *
 * Apr  4 2007	Add -w option to not rotate and -e option to erase WCS keywords
 *
 * Mar 27 2009	Use _ instead of . to separate extension name or number in output filename
 * Mar 27 2009	Add -n option to force use of extension number instead of EXTNAME
 * Sep 25 2009	Declare DelWCSFITS()
 */
