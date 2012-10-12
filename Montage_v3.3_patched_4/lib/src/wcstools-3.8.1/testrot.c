/* File testrot.c
 * June 19, 2002
 * By Doug Mink Harvard-Smithsonian Center for Astrophysics)
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
#include "libwcs/wcs.h"

static void usage();
static void PrintHead();

static int verbose = 0;		/* verbose/debugging flag */
static int tabout = 0;		/* tab table output flag */
static int nchar = 16;		/* maximum number of characters for filename */
static int hms = 0;		/* 1 for output in hh:mm:ss dd:mm:ss */
static int nf = 0;
static int version = 0;		/* If 1, print only program name and version */

static char *RevMsg = "TESTROT WCSTools 3.8.1, 14 December 2009, Doug Mink SAO";

main (ac, av)
int ac;
char **av;
{
    char *str;
    int readlist = 0;
    char *lastchar;
    char filename[128];
    FILE *flist;
    char *listfile;

    /* Check for help or version command first */
    str = *(av+1);
    if (!strcmp (str, "help") || !strcmp (str, "-help"))
	usage();
    if (!strcmp (str, "version") || !strcmp (str, "-version")) {
	version = 1;
	usage();
	}

    /* crack arguments */
    for (av++; --ac > 0 && (*(str = *av)=='-' || *str == '@'); av++) {
	char c;
	if (*str == '@')
	    str = str - 1;
	while (c = *++str)
	switch (c) {

	case 'h':	/* hh:mm:ss output for crval, cdelt in arcsec/pix */
	    hms++;
	    break;

	case 'n':	/* hh:mm:ss output */
	    tabout++;
	    break;

	case 't':	/* tab table output */
	    tabout++;
	    break;

	case 'v':	/* more verbosity */
	    verbose++;
	    break;

    	case 'z':	/* Use AIPS classic WCS */
    	    setdefwcs (WCS_ALT);
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
	    fprintf (stderr,"TESTROT: List file %s cannot be read\n",
		     listfile);
	    usage ();
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
	usage ();

    if (verbose)
	fprintf (stderr,"%s\n",RevMsg);

    nf = 0;
    while (ac-- > 0) {
	char *fn = *av++;
	nf++;
	PrintHead (fn);
	}

    return (0);
}

static void
usage ()
{
    fprintf (stderr,"%s\n",RevMsg);
    if (version)
	exit (-1);
    fprintf (stderr,"Print WCS part of FITS or IRAF image header\n");
    fprintf (stderr,"usage: testrot [-htv] file.fit ...\n");
    fprintf (stderr,"  -h: print CRVALs as hh:mm:ss dd:mm:ss\n");
    fprintf (stderr,"  -t: print tab table output\n");
    fprintf (stderr,"  -v: verbose\n");
    fprintf (stderr,"  -z: Use AIPS classic WCS subroutines\n");
    exit (1);
}


static void
PrintHead (filename)

char	*filename;	/* FITS or IRAF image file name */

{
    struct WorldCoor *wcs, *GetWCSFITS();
    static void TabRot();

    wcs = GetWCSFITS (filename, verbose);
    if (nowcs (wcs))
	return;

    TabRot (filename, wcs);

    free (wcs);
    return;
}


static void
TabRot (filename, wcs)

char	*filename;	/* FITS or IRAF image file name */

struct WorldCoor *wcs;

{
    int i, off;
    char fform[8];
    double xc, yc, cra, cdec, xe, ye, xn, yn, pa_north, pa_east;

    if (wcs->ctype[0][0] == (char) 0)
	return;
    if (tabout && nf == 1) {
	printf ("filename   ");
	for (i = 1; i < nchar - 5; i++) printf (" ");
	printf ("	flip	cd  ");
	printf ("	cd1_1	cd1_2	cd2_1	cd2_2");
	printf ("	cdelt1	cdelt2");
	printf ("	crota2  	north   	east     ");
	printf ("	imrot   	imflip\n");
	printf ("------------");
	for (i = 1; i < nchar - 8; i++) printf ("-");
	printf ("	------	------");
	printf ("	------	------	------	------");
	printf ("	------	------");
	printf ("	---------	---------	---------");
	printf ("	---------	-------\n");
	}

    sprintf (fform,"%%%d.%ds",nchar,nchar);
    if (tabout)
	printf (fform, filename);
    else
	printf (fform, filename);

    if (tabout)
	printf ("	%d	%d", wcs->coorflip, wcs->rotmat);
    else
	printf (" %d %d", wcs->coorflip, wcs->rotmat);

    if (tabout)
	printf ("	%7.4f	%7.4f	%7.4f	%7.4f",
		3600.0*wcs->cd[0], 3600.0*wcs->cd[1],
		3600.0*wcs->cd[2], 3600.0*wcs->cd[3]);
    else
	printf (" %7.4f %7.4f %7.4f %7.4f",
		3600.0*wcs->cd[0], 3600.0*wcs->cd[1],
		3600.0*wcs->cd[2], 3600.0*wcs->cd[3]);

    if (tabout)
	printf ("	%7.4f	%7.4f	%9.4f",
		3600.0*wcs->xinc, 3600.0*wcs->yinc, wcs->rot);
    else
	printf (" %7.4f %7.4f %9.4f",
		3600.0*wcs->xinc, 3600.0*wcs->yinc, wcs->rot);

    xc = wcs->crpix[0];
    yc = wcs->crpix[1];
    pix2wcs (wcs, xc, yc, &cra, &cdec);
    if (wcs->coorflip) {
	wcs2pix (wcs, cra+fabs(wcs->cdelt[1]), cdec, &xe, &ye, &off);
	wcs2pix (wcs, cra, cdec+fabs(wcs->cdelt[0]), &xn, &yn, &off);
	}
    else {
	wcs2pix (wcs, cra+fabs(wcs->cdelt[0]), cdec, &xe, &ye, &off);
	wcs2pix (wcs, cra, cdec+fabs(wcs->cdelt[1]), &xn, &yn, &off);
	}
    pa_north = raddeg (atan2 (yn-yc, xn-xc));
    pa_east = raddeg (atan2 (ye-yc, xe-xc));
    if (tabout)
	printf ("	%9.4f	%9.4f	%9.4f	%d\n",
		pa_north, pa_east, wcs->imrot, wcs->imflip);
    else
	printf (" %9.4f %9.4f %9.4f %d\n",
		pa_north, pa_east, wcs->imrot, wcs->imflip);

    return;
}
/* Sep  2 1998	New program
 * Nov 30 1998	Add version and help commands for consistency
 *
 * Oct 22 1999	Drop unused variables after lint
 *
 * Jan 28 2000	Call setdefwcs() with WCS_ALT instead of 1
 *
 * Jun 19 2002	Add verbose argument to GetWCSFITS()
 */
