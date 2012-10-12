/*** File libwcs/imsetwcs.c
 *** December 14, 2009
 *** By Doug Mink, dmink@cfa.harvard.edu (based on UIowa code)
 *** Harvard-Smithsonian Center for Astrophysics
 *** Copyright (C) 1996-2009
 *** Smithsonian Astrophysical Observatory, Cambridge, MA, USA

    This library is free software; you can redistribute it and/or
    modify it under the terms of the GNU Lesser General Public
    License as published by the Free Software Foundation; either
    version 2 of the License, or (at your option) any later version.

    This library is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
    Lesser General Public License for more details.
    
    You should have received a copy of the GNU Lesser General Public
    License along with this library; if not, write to the Free Software
    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

    Correspondence concerning WCSTools should be addressed as follows:
           Internet email: dmink@cfa.harvard.edu
           Postal address: Doug Mink
                           Smithsonian Astrophysical Observatory
                           60 Garden St.
                           Cambridge, MA 02138 USA
 */

#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <string.h>

#include "wcs.h"
#include "lwcs.h"
#include "fitsfile.h"
#include "wcscat.h"

extern int FindStars();
extern int TriMatch();
extern int FocasMatch();
extern int StarMatch();
extern int ReadMatch();
extern int FitMatch();
extern int WCSMatch();
extern int FitPlate();
extern struct WorldCoor *GetFITSWCS ();
extern char *getimcat();
extern void SetFITSWCS();
extern int iscdfit();
extern void setminbin();
extern void setnfit();
extern int getnfit();

/* Set the C* WCS fields in a FITS header based on a reference catalog
 * by finding stars in the image and in the reference catalog and
 * fitting the scaling, rotation, and offsets.
 * verbose generates extra info on stderr.
 * Try using deeper reference star catalog searches if there is trouble.
 * Return 1 if all ok, else 0
 */

/* These parameters can be set on the command line */
static double tolerance = PIXDIFF;	/* +/- this many pixels is a hit */
static double refmag1 = MAGLIM1;	/* reference catalog magnitude limit */
static double refmag2 = MAGLIM2;	/* reference catalog magnitude limit */
static double frac = 1.0;	/* Additional catalog/image stars */
static int nofit = 0;		/* if =1, do not fit WCS */
static int maxcat = MAXSTARS;	/* Maximum number of catalog stars to use */
static int fitwcs = 1;		/* If 1, fit WCS, else use current WCS */
static int fitplate = 0;	/* If 1, fit polynomial, else do not */
static double imfrac0 = 0.0;	/* If > 0.0, multiply image dimensions
					   by this for search */
static int iterate0 = 0;	/* If 1, search field again */
static int toliterate0 = 0;	/* if 1, halve tolerances when iter */
static int nfiterate0 = 0;	/* if 1, add two parameters to fit */
static int recenter0 = 0;	/* If 1, search again with new center*/
static char matchcat[32]="";	/* Match catalog name */
static int irafout = 0;		/* if 1, write X Y RA Dec out */
static int magfit = 0;		/* If 1, write magnitude polynomial(s) */
static int sortmag = 1;		/* Magnitude by which to sort stars */
static int minstars0 = MINSTARS;	/* Number of star matches for fit */
static int nxydec = NXYDEC;	/* Number of decimal places in image coordinates */
static void PrintRes();
static void CompRes();
extern void SetFITSPlate();

static char *kwt = NULL;        /* Keyword returned by ctgread() */
void settabkrw (keyword0)
char *keyword0;
{ kwt = keyword0; return; }


/* Set the C* WCS fields in the input image header based on the given limiting
 * reference mag.
 * Finding stars in the input image and in the reference catalog between
 * refmag1 and refmag2 and compute the angle and offsets which result in the best fit.
 * verbose generates extra info on stdout.
 * return 0 if all ok, else -1
 */

int
SetWCSFITS (filename, header, image, refcatname, verbose)

char	*filename;	/* image file name */
char	*header;	/* FITS header */
char	*image;		/* Image pixels */
char	*refcatname;	/* Name of reference catalog */
int	verbose;

{
    double *gnum;	/* Reference star numbers */
    double *gra;	/* Reference star right ascensions in degrees */
    double *gdec;	/* Reference star declinations in degrees */
    double *gpra;	/* Reference star right ascension proper motions (deg)*/
    double *gpdec;	/* Reference star declination proper motions (deg) */
    double **gm;	/* Reference star magnitudes */
    double *gx;		/* Reference star image X-coordinates in pixels */
    double *gy;		/* Reference star image Y-coordinates in pixels */
    int *gc;		/* Reference object types */
    int *goff;		/* Reference star offscale flags */
    int ng;		/* Number of reference stars in image */
    int nbg;		/* Number of brightest reference stars from search */
    int nrg;		/* Number of brightest reference stars actually used */
    double *sx;		/* Image star image X-coordinates in pixels */
    double *sy;		/* Image star image X-coordinates in pixels */
    double *sm;		/* Image star instrumental magnitude */
    int *sp;		/* Image star peak fluxes in counts */
    int ns;		/* Number of image stars */
    int nbs;		/* Number of brightest image stars actually used */
    double cra, cdec;	/* Nominal center in degrees from RA/DEC FITS fields */
    double dra, ddec;	/* Image half-widths in degrees */
    double secpix;	/* Pixel size in arcseconds */
    int imw, imh;	/* Image size, pixels */
    int imsearch = 1;	/* Flag set if image should be searched for sources */
    int nmax;		/* Maximum number of matches possible (nrg or nbs) */
    int lofld = 0;	/* Length of object name field in output */
    double mag1,mag2;
    int refcat;		/* reference catalog switch */
    int nmag, nmag1, mprop;
    double dxys;
    char numstr[32];
    int minstars;
    int ngmax;
    int nbin, nbytes;
    int iterate, toliterate, nfiterate;
    int imag, magsort;
    int niter = 0;
    int recenter = recenter0;
    int ret = 0;
    int is, ig, igs, i, j;
    char rstr[32], dstr[32];
    double refeq, refep;
    double maxnum;
    int nnfld;
    int refsys;
    char refcoor[8];
    char title[80];
    char *imcatname;	/* file name for image star catalog, if used */
    struct WorldCoor *wcs=0;	/* WCS structure */
    double *sx1, *sy1, *sm1, *gra1, *gdec1, *gnum1, *gm1;
    char **gobj, **gobj1;	/* Catalog star object names */
    double imfrac = imfrac0;
    int nmatch;
    double ra,dec;
    double dx, dy, dx2, dy2, dxy;
    struct StarCat *starcat;
    int npfit;
    int nndec;
    extern int NParamFit();
    extern void setdcenter(),setsys(),setrefpix(),setsecpix();
    extern void setsecpix2(),setrot();

    iterate = iterate0;
    toliterate = toliterate0;
    nfiterate = nfiterate0;
    gnum = NULL;
    gra = NULL;
    gdec = NULL;
    gpra = NULL;
    gpdec = NULL;
    gm = NULL;
    gx = NULL;
    gy = NULL;
    gc = NULL;
    gobj = NULL;
    gobj1 = NULL;
    goff = NULL;
    sm = NULL;
    sx = NULL;
    sy = NULL;
    sp = NULL;
    starcat = NULL;
    imcatname = NULL;
    ns = 0;

    if (refmag1 == refmag2) {
	mag1 = 0.0;
	mag2 = 0.0;
	}
    else {
	mag1 = refmag1;
	mag2 = refmag2;
	}

    /* Use already-matched stars first, if they are present */
    if (strlen (matchcat) > 0) {
	refsys = WCS_J2000;
	refeq = 2000.0;
	if ((nbin = ReadMatch (matchcat, &sx, &sy, &gra, &gdec, verbose)) < 1) {
	    ret = 0;
	    goto out;
	    }

	if (nbin > 1)
	    WCSMatch (nbin, sx, sy, gra, gdec, verbose);

	/* Set WCS from image header and command line */
	wcs = GetFITSWCS (filename,header,verbose,&cra,&cdec,&dra,&ddec,
			  &secpix, &imw,&imh,&refsys, &refeq);
	if (nowcs (wcs)) {
	    ret = 0;
	    goto out;
	    }
	if (getnfit() == 0) {
	    SetFITSWCS (header, wcs);
	    return (1);
	    }
	nbin = FitMatch (nbin, sx, sy, gra, gdec, wcs, verbose);
	sm = (double *) calloc (nbin, sizeof (double));
	hputs (header, "WCSRFCAT", matchcat);
	hputs (header, "WCSIMCAT", matchcat);
	hputi4 (header, "WCSMATCH", nbin);
	hputi4 (header, "WCSNREF", nbin);
	if (!(gnum = (double *) calloc (nbin, sizeof(double))))
	    fprintf (stderr, "Could not calloc %d bytes for gnum\n",
		     nbin*sizeof(double));
	for (is = 0; is < nbin; is++)
	    gnum[is] = (double)(is + 1);
	if (!(gx = (double *) calloc (nbin, sizeof(double))))
	    fprintf (stderr, "Could not calloc %d bytes for gx\n",
		     nbin*sizeof(double));
	if (!(gy = (double *) calloc (nbin, sizeof(double))))
	    fprintf (stderr, "Could not calloc %d bytes for gy\n",
		     nbin*sizeof(double));
	if (!(goff = (int *) calloc (nbin, sizeof(int))))
	    fprintf (stderr, "Could not calloc %d bytes for goff\n",
		     nbin*sizeof(double));

	SetFITSWCS (header, wcs);
	nrg = nbin;
	ns = nbin;
	if (refcatname == NULL)
	    goto match;
	}

    /* Set reference catalog coordinate system and epoch */
    if (nofit) {
	refsys = 0;
	refeq = 0.0;
	refcat = 0;
	}
    else {
	refcat = RefCat (refcatname,title,&refsys,&refeq,&refep,&mprop,&nmag);
	if (refcat == UCAC2 | refcat == UCAC3)
	    nmag1 = nmag + 4;
	else
	    nmag1 = nmag;
	wcscstr (refcoor, refsys, refeq, refep);
	}

    /* get nominal position and scale */
getfield:
    wcs = GetFITSWCS (filename,header,verbose,&cra,&cdec,&dra,&ddec,&secpix,
		      &imw,&imh,&refsys, &refeq);
    if (nowcs (wcs)) {
	ret = 0;
	goto out;
	}

    refep = wcs->epoch;
    if (nofit) {
	SetFITSWCS (header, wcs);
	ret = 1;
	goto out;
	}

    if (fitwcs) {
	wcs->prjcode = WCS_TAN;
	wcseqset (wcs, refeq);
	}

    if (refcatname == NULL) {
	refcatname = CatName (refcat, refcatname);
	if (refcatname == NULL) {
	    ret = 0;
	    goto out;
	    }
	}

    if (imfrac > 0.0) {
	dra = dra * imfrac;
	ddec = ddec * imfrac;
	}
    if (sortmag > 9)
	sortmag = CatMagNum (sortmag, refcat);

    /* Allocate arrays for results of reference star search */
    ngmax = maxcat;
    if (imfrac > 1.0)
	ngmax = (int) ((double) ngmax * imfrac * imfrac);
    nbytes = ngmax * sizeof (double);
    if (!(gnum = (double *) calloc (ngmax, sizeof(double))))
	fprintf (stderr, "Could not calloc %d bytes for gnum\n",
		 ngmax*sizeof(double));
    if (!(gra = (double *) calloc (ngmax, sizeof(double))))
	fprintf (stderr, "Could not calloc %d bytes for gra\n",
		 ngmax*sizeof(double));
    if (!(gdec = (double *) calloc (ngmax, sizeof(double))))
	fprintf (stderr, "Could not calloc %d bytes for gdec\n",
		 ngmax*sizeof(double));
    if (!(gpra = (double *) calloc (ngmax, sizeof(double))))
	fprintf (stderr, "Could not calloc %d bytes for gpra\n",
		 ngmax*sizeof(double));
    if (!(gpdec = (double *) calloc (ngmax, sizeof(double))))
	fprintf (stderr, "Could not calloc %d bytes for gpdec\n",
		 ngmax*sizeof(double));
    if (!(gm = (double **) calloc (nmag1, sizeof(double *))))
	fprintf (stderr, "Could not calloc %d bytes for gm\n",
		 nmag1*sizeof(double *));
    else {
	for (imag = 0; imag < nmag1; imag++) {
	    if (!(gm[imag] = (double *) calloc (ngmax, sizeof(double))))
		fprintf (stderr, "Could not calloc %d bytes for gm\n",
		    ngmax*sizeof(double));
	    }
	}
    if (!(gc = (int *) calloc (ngmax, sizeof(int))))
	fprintf (stderr, "Could not calloc %d bytes for gc\n",
		 ngmax*sizeof(int));
    if (!(gobj = (char **) calloc (ngmax, sizeof(char *))))
	fprintf (stderr, "Could not calloc %d bytes for obj\n",
		 ngmax*sizeof(char *));
    else {
	for (i = 0; i < ngmax; i++)
	    gobj[i] = NULL;
	}

    /* Find the nearby reference stars, in ra/dec */
getstars:
    ng = ctgread (refcatname,refcat,0,cra,cdec,dra,ddec,0.0,0.0,refsys,refeq,
		  refep,mag1,mag2,sortmag,ngmax,&starcat,
		  gnum,gra,gdec,gpra,gpdec,gm,gc,gobj,verbose*100);
    if (ng > ngmax)
	nrg = ngmax;
    else
	nrg = ng;
    if (gobj[0] == NULL)
	gobj1 = NULL;
    else
	gobj1 = gobj;

    minstars = minstars0;
    npfit = NParamFit (100);
    if (npfit < minstars0)
	minstars = 1;

    if (sortmag > 0 && sortmag <= nmag)
	magsort = sortmag - 1;
    else
	magsort = 0;

    /* Sort reference stars by brightness (magnitude) */
    MagSortStars (gnum, gra, gdec, gpra, gpdec, NULL, NULL, gm, gc, gobj1, nrg, 
		  nmag1, sortmag);

    /* Project the reference stars into pixels on a plane at ra0/dec0 */
    if (!(gx = (double *) calloc (ngmax, sizeof(double))))
	fprintf (stderr, "Could not calloc %d bytes for gx\n",
		 ngmax*sizeof(double));
    if (!(gy = (double *) calloc (ngmax, sizeof(double))))
	fprintf (stderr, "Could not calloc %d bytes for gy\n",
		 ngmax*sizeof(double));
    if (!(goff = (int *) calloc (ngmax, sizeof(int))))
	fprintf (stderr, "Could not calloc %d bytes for gy\n",
		 ngmax*sizeof(double));
    if (!gx || !gy || !goff) {
	ret = 0;
	goto out;
	}

    /* use the nominal WCS info to find x/y on image */
    for (ig = 0; ig < nrg; ig++) {
	gx[ig] = 0.0;
	gy[ig] = 0.0;
	wcs2pix (wcs, gra[ig], gdec[ig], &gx[ig], &gy[ig], &goff[ig]);
	}

    /* Note how reference stars were selected */
    if (ng > ngmax) {
	if (verbose)
	    fprintf (stderr,"Using %d / %d reference stars brighter than %.1f\n",
		     nrg, ng, gm[magsort][nrg-1]);
	}
    else {
	if (verbose) {
	    if (refmag1 > 0.0 && refmag2 > 0.0)
		fprintf (stderr,"Using all %d reference stars from %.1f to %.1f mag.\n",
			ng, refmag1, refmag2);
	    else if (refmag2 > 0.0)
		fprintf (stderr,"Using all %d reference stars brighter than %.1f\n",
			ng,refmag2);
	    else
		fprintf (stderr,"using all %d reference stars\n", ng);
	    }
	}

    if (verbose) {
	fprintf (stderr,"%s:\n",refcatname);
	for (ig = 0; ig < nrg; ig++) {
	    if (ig == 0)
		maxnum = gnum[ig];
	    else if (gnum[ig] > maxnum)
		maxnum = gnum[ig];
	    }
	nnfld = CatNumLen (refcat, maxnum, 0);
	nndec = 0;
	for (ig = 0; ig < nrg; ig++) {
	    ra2str (rstr, 32, gra[ig], 3);
	    dec2str (dstr, 32, gdec[ig], 2);

	    /* Set up object name or number to print */
            if (starcat != NULL) {
		if (starcat->stnum < 0 && gobj1 != NULL) {
		    strncpy (numstr, gobj1[ig], 32);
		    if (lofld > 0) {
			for (j = 0; j < lofld; j++) {
			    if (!numstr[j])
				numstr[j] = ' ';
			    }
			}
		    }
		else
		    CatNum (refcat,-nnfld,starcat->nndec,gnum[ig],numstr);
		}
	    else
		CatNum (refcat, -nnfld, nndec, gnum[ig], numstr);

	    if (nmag > 0)
		fprintf (stderr,"%s %s %s %5.2f %6.1f %6.1f\r",
		    numstr,rstr,dstr,gm[magsort][ig],gx[ig],gy[ig]);
	    else
		fprintf (stderr,"%s %s %s %6.1f %6.1f\r",
		    numstr,rstr,dstr,gx[ig],gy[ig]);
	    }
	fprintf (stderr,"\n");
	}

    if (nrg < minstars) {
	if (ng < 0)
	    fprintf (stderr, "Error getting reference stars: %d\n", ng);
	else if (ng == 0)
	    fprintf (stderr,"No reference stars found in image area\n");
	else if (fitwcs)
	    fprintf (stderr, "Found only %d out of %d reference stars needed\n",
			     nrg, minstars);
	if (ng <= 0 || fitwcs) {
	    ret = 0;
	    goto out;
	    }
	}

    /* Discover star-like things in the image, in pixels */
    if (imsearch) {
	ns = FindStars (header, image, &sx, &sy, &sm, &sp, verbose, 0);
	if (ns < minstars) {
	    if (ns < 0)
		fprintf (stderr, "Error getting image stars: %d\n", ns);
	    else if (ns == 0)
		fprintf (stderr,"No stars found in image\n");
	    else if (fitwcs)
		fprintf (stderr, "Need at least %d image stars but only found %d\n",
			 minstars, ns);
	    if (ns <= 0 || fitwcs) {
		ret = 0;
		iterate = 0;
		recenter = 0;
		goto out;
		}
	    }
	imsearch = 0;
	}

    /* Fit a world coordinate system if requested */
    if (fitwcs) {
	niter++;

	/* Sort star-like objects in image by brightness (magnitude) */
	MagSortStars (NULL,NULL,NULL,NULL,NULL,sx,sy,&sm,sp,NULL,ns,1,1);

	/* If matching a catalog field the same size as the image field,
	   use only as many star-like objects as reference stars.  If using
	   a larger catalog field (imfrac > 0), increase the number of stars
	   proportionately (imfrac^2 * the number of image stars).  To adjust
	   for different bandpasses between the image and the reference catalog
	   use frac * the number of reference stars if more than image stars
	   or frac * the number of image stars if more than reference stars.
	*/
	if (imfrac > 0.0) {
	    double imfrac2 = imfrac * imfrac;
	    if (ns > (nrg / imfrac2)) {
		nbs = nrg * frac / imfrac2;
		if (nbs > ns)
		    nbs = ns;
		nbg = nrg;
		}
	    else {
		nbs = ns;
		nbg = (int) ((double) nbs * imfrac2);
		if (nbg > nrg)
		    nbg = nrg;
		}
	    }
	else {
	    if (ns > nrg) {
		nbs = nrg * frac;
		if (nbs > ns)
		    nbs = ns;
		nbg = nrg;
		}
	    else {
		nbs = ns;
		nbg = nbs * frac;
		if (nbg > nrg)
		    nbg = nrg;
		}
	    }
	if (verbose) {
	    if (nbg == ng)
		fprintf (stderr,"Using all %d reference stars\n", ng);
	    else
		fprintf (stderr,"Using brightest %d / %d reference stars\n", nbg, ng);
	    if (nbs == ns)
		fprintf (stderr,"Using all %d image stars\n", ns);
	    else
		fprintf (stderr,"Using brightest %d / %d image stars\n", nbs,ns);
	    }

	/* Print nominal positions of image stars */
	if (verbose) {
	    char rastr[32], decstr[32];
	    double xmag, mdiff, ra, dec;
	    for (is = 0; is < nbs; is++) {
		pix2wcs (wcs, sx[is], sy[is], &ra, &dec);
		ra2str (rastr, 32, ra, 3);
		dec2str (decstr, 32, dec, 2);
		xmag = sm[is];
		if (nmag > 0 && !is) {
		    mdiff = gm[magsort][0] - xmag;
		    }
		else {
		    mdiff = 0.0;
		    }
		xmag = xmag + mdiff;
		fprintf (stderr,"%4d %s %s %6.2f %6.1f %6.1f %d\r",
			is+1, rastr, decstr, xmag, sx[is], sy[is], sp[is]);
		}
	    fprintf (stderr,"\n");
	    }

	/* Match offsets between all pairs of image stars and reference stars
	   and fit WCS to matches */
	nbin = StarMatch (nbs,sx,sy,refcat,nbg,gnum,gra,gdec,goff,gx,gy,
			  tolerance,wcs,verbose);

	if (nbin < 0) {
	    fprintf (stderr, "Star registration failed.\n");
	    ret = 0;
	    goto done;
	    }
	else if (nbin < minstars0) {
	    fprintf (stderr, "Only %d matches, registration failed.\n", nbin);
	    ret = 0;
	    goto done;
	    }
	else if (verbose)
	    fprintf (stderr,"%d / %d bin hits\n", nbin, nbg);

	hputs (header, "WCSRFCAT", refcatname);
	imcatname = getimcat ();
	if (strlen (imcatname) == 0)
	    hputs (header, "WCSIMCAT", filename);
	else
	    hputs (header, "WCSIMCAT", imcatname);
	hputi4 (header, "WCSMATCH", nbin);
	if (ns < nbg)
	    hputi4 (header, "WCSNREF", ns);
	else
	    hputi4 (header, "WCSNREF", nbg);
	hputnr8 (header, "WCSTOL", 4, tolerance);

	SetFITSWCS (header, wcs);
	}

    /* Match reference and image stars */
match:
    nmatch = 0;

    if (verbose || !fitwcs) {
	imcatname = getimcat ();
	if (wcs->ncoeff1 > 0)
	    printf ("# %d-term x, %d-term y polynomial fit\n",
		    wcs->ncoeff1, wcs->ncoeff2);
	else
	    printf ("# Arcsec/Pixel= %.6f %.6f  Rotation= %.6f degrees\n",
		    3600.0*wcs->xinc, 3600.0*wcs->yinc, wcs->rot);
	ra2str (rstr, 32, wcs->xref, 3);
	dec2str (dstr, 32, wcs->yref, 2);
	printf ("# Optical axis= %s  %s %s x= %.2f y= %.2f\n",
		rstr,dstr, refcoor, wcs->xrefpix, wcs->yrefpix);
	ra = wcs->xref;
	dec = wcs->yref;
	if (refsys == WCS_J2000) {
	    fk524e (&ra, &dec, refep);
	    ra2str (rstr, 32, ra, 3);
	    dec2str (dstr, 32, dec, 2);
	    printf ("# Optical axis= %s  %s B1950  x= %.2f y= %.2f\n",
		    rstr,dstr, wcs->xrefpix, wcs->yrefpix);
	    }
	else {
	    fk425e (&ra, &dec, refep);
	    ra2str (rstr, 32, ra, 3);
	    dec2str (dstr, 32, dec, 2);
	    printf ("# Optical axis= %s  %s J2000  x= %.2f y= %.2f\n",
		    rstr,dstr, wcs->xrefpix, wcs->yrefpix);
	    }
	}

    /* Find star matches for this offset and print them */

    /* Use the fit WCS info to find catalog star x/y on image */
    for (ig = 0; ig < nrg; ig++) {
	gx[ig] = 0.0;
	gy[ig] = 0.0;
	wcs2pix (wcs, gra[ig], gdec[ig], &gx[ig], &gy[ig], &goff[ig]);
	}

    /* Set maximum number of matches which are possible */
    if (nrg < ns)
	nmax = nrg;
    else
	nmax = ns;

    /* Find best catalog matches to stars in image */
    nmatch = 0;
    nbytes = ns * sizeof (double);
    if (!(gra1 = (double *) calloc (ns, sizeof(double))))
            fprintf (stderr, "Could not calloc %d bytes for gra1\n",nbytes);
    if (!(gdec1 = (double *) calloc (ns, sizeof(double))))
            fprintf (stderr, "Could not calloc %d bytes for gdec1\n",nbytes);
    if (!(gm1 = (double *) calloc (ns, sizeof(double))))
            fprintf (stderr, "Could not calloc %d bytes for gm1\n",nbytes);
    if (!(gnum1 = (double *) calloc (ns, sizeof(double))))
            fprintf (stderr, "Could not calloc %d bytes for gnum1\n",nbytes);
    if (!(sx1 = (double *) calloc (ns, sizeof(double))))
            fprintf (stderr, "Could not calloc %d bytes for sx1\n",nbytes);
    if (!(sy1 = (double *) calloc (ns, sizeof(double))))
            fprintf (stderr, "Could not calloc %d bytes for sy1\n",nbytes);
    if (!(sm1 = (double *) calloc (ns, sizeof(double))))
            fprintf (stderr, "Could not calloc %d bytes for sm1\n",nbytes);
    for (is = 0; is < ns; is++) {
	dxys = tolerance * tolerance;
	igs = -1;
	for (ig = 0; ig < nrg; ig++) {
	    if (!goff[ig]) {
		dx = gx[ig] - sx[is];
		dy = gy[ig] - sy[is];
		dx2 = dx * dx;
		dy2 = dy * dy;
		dxy = dx2 + dy2;
		if (dxy < dxys) {
		    dxys = dxy;
		    igs = ig;
		    }
		}
	    }
	if (igs > -1) {
	    gnum1[nmatch] = gnum[igs];
	    if (gm != NULL && nmag > 0)
		gm1[nmatch] = gm[magsort][igs];
	    else
		gm1[nmatch] = 0.0;
	    gra1[nmatch] = gra[igs];
	    gdec1[nmatch] = gdec[igs];
	    sx1[nmatch] = sx[is];
	    sy1[nmatch] = sy[is];
	    sm1[nmatch] = sm[is];
	    nmatch++;
	    }
	}

    /* If there were any matches found, print them */
    if (nmatch > 0) {
	int rprint = verbose || !fitwcs;
	hputi4 (header, "WCSMATCH", nmatch);
	hputi4 (header, "WCSNREF", nmax);
	hputnr8 (header, "WCSTOL", 4, tolerance);
	if (rprint) {

	    PrintRes (header,wcs,nmatch,sx1,sy1,sm1,gra1,gdec1,gm1,gnum1,
		      refcat,rprint);
	    if (refcatname == NULL)
		printf ("# nmatch= %d nstars= %d in and %s  niter= %d\n",
			nmatch, nmax, matchcat, niter);
	    else if (strlen (imcatname) == 0)
		printf ("# nmatch= %d nstars= %d between %s and %s  niter= %d\n",
			nmatch, nmax, refcatname, filename, niter);
	    else
		printf ("# nmatch= %d nstars= %d between %s and %s  niter= %d\n",
			nmatch, nmax, refcatname, imcatname, niter);
	    }
	else
	    CompRes (header,wcs,nmatch,sx1,sy1,sm1,gra1,gdec1,gm1,gnum1);

	/* Fit the matched catalog and image stars with a polynomial */
	if (!iterate && !recenter && fitplate && refcatname != NULL) {

	    if (verbose)
		fprintf (stderr,"Fitting matched stars with a polynomial\n");

	    /* Fit residuals */
	    if (FitPlate (wcs, sx1, sy1, gra1, gdec1, nmatch, fitplate,
			  verbose))
		fprintf (stderr,"FitPlate cannot fit matches\n");

	    /* Print the new residuals */
	    else if (rprint) {
		PrintRes (header,wcs,nmatch,sx1,sy1,sm1,gra1,gdec1,gm1,gnum1,
			  refcat,verbose);
		if (refcatname == NULL)
		    printf ("# nmatch= %d nstars= %d in %s niter= %d\n",
			    nmatch, nmax, matchcat, niter);
		else if (strlen (imcatname) == 0)
		printf ("# nmatch= %d nstars= %d between %s and %s niter= %d\n",
			nmatch, nmax, refcatname, filename, niter);
		else
		printf ("# nmatch= %d nstars= %d between %s and %s niter= %d\n",
			nmatch, nmax, refcatname, imcatname, niter);
		SetFITSPlate (header, wcs);
		}
	    else {
		CompRes (header,wcs,nmatch,sx1,sy1,sm1,gra1,gdec1,gm1,gnum1);
		SetFITSPlate (header, wcs);
		}
	    }
	}

    else {
	if (refcatname == NULL)
	    fprintf (stderr, "SetWCSFITS: No matches in %s:\n",
		     matchcat);
	else if (strlen (imcatname) == 0)
	    fprintf (stderr, "SetWCSFITS: No matches between %s and %s:\n",
		     refcatname, filename);
	else
	    fprintf (stderr, "SetWCSFITS: No matches between %s and %s:\n",
		     refcatname, imcatname);
	hputi4 (header, "WCSMATCH", 0);
	}
    if (gra1) free ((char *)gra1);
    if (gdec1) free ((char *)gdec1);
    if (gm1) free ((char *)gm1);
    if (gnum1) free ((char *)gnum1);
    if (sx1) free ((char *)sx1);
    if (sy1) free ((char *)sy1);
    if (sm1) free ((char *)sm1);

    ret = 1;

    out:

    if (iterate) {
/*	setdcenter (wcs->xref, wcs->yref);
	setsys (wcs->syswcs);
	setrefpix (wcs->xrefpix, wcs->yrefpix);
	if (iscdfit())
	    setcd (wcs->cd);
	else {
	    setsecpix (-3600.0 * wcs->xinc);
	    setsecpix2 (3600.0 * wcs->yinc);
	    setrot (wcs->rot);
	    }
	wcsfree (wcs); */

	wcssize (wcs, &cra, &cdec, &dra, &ddec);
	if (cra < 0.0) cra = cra + 360.0;
	iterate--;
	imfrac = 0.0;
	goto getstars;
	}
    if (toliterate) {
/*	setdcenter (wcs->xref, wcs->yref);
	setsys (wcs->syswcs);
	setrefpix (wcs->xrefpix, wcs->yrefpix);
	if (iscdfit())
	    setcd (wcs->cd);
	else {
	    setsecpix (-3600.0 * wcs->xinc);
	    setsecpix2 (3600.0 * wcs->yinc);
	    setrot (wcs->rot);
	    }
	wcsfree (wcs); */

	wcssize (wcs, &cra, &cdec, &dra, &ddec);
	tolerance = tolerance * 0.5;
	toliterate--;
	imfrac = 0.0;
	goto getstars;
	}
    if (recenter) {
	double ra, dec, x, y;
	x = 0.5 * wcs->nxpix;
	y = 0.5 * wcs->nypix;
	pix2wcs (wcs, x, y, &ra, &dec);
	setdcenter (ra, dec);
	setsys (wcs->syswcs);
	setrefpix (x, y);
	setsecpix (-3600.0 * wcs->xinc);
	setsecpix2 (3600.0 * wcs->yinc);
	setrot (wcs->rot);
	recenter = 0;
	imfrac = 0.0;
	if (wcs) {
	    wcsfree (wcs);
	    wcs = NULL;
	    }
	goto getfield;
	}
    if (nfiterate) {
	int nfit = getnfit ();
	wcssize (wcs, &cra, &cdec, &dra, &ddec);
	if (verbose)
	    printf ("\n fitting %d instead of %d parameters\n", nfit+2, nfit);
	if (nfit < 7)
	    nfit = nfit + 2;
	setnfit (nfit);
	nfiterate--;
	goto getstars;
	}

done:
    if (wcs) {
	wcsfree (wcs);
	wcs = NULL;
	}

    /* Free catalog source arrays */
    if (gra) free ((char *)gra);
    if (gdec) free ((char *)gdec);
    if (gpra) free ((char *)gpra);
    if (gpdec) free ((char *)gpdec);
    if (gm) {
	for (imag = 0; imag < nmag1; imag++) {
	    if (gm[imag])
		free ((char *)gm[imag]);
	    }
	free ((char *)gm);
	}
    if (gnum) free ((char *)gnum);
    if (gx) free ((char *)gx);
    if (gy) free ((char *)gy);
    if (goff) free ((char *)goff);
    if (gc) free ((char *)gc);

    /* Free memory used for object names in reference catalog */
    if (gobj1 != NULL) {
	for (i = 0; i < ngmax; i++) {
	    if (gobj[i] != NULL) {
		free (gobj[i]);
		gobj[i] = NULL;
		}
	    }
	}
    if (gobj) {
	free ((char *) gobj);
	gobj = NULL;
	}

    /* Free image source arrays */
    if (sx) free ((char *)sx);
    if (sy) free ((char *)sy);
    if (sm) free ((char *)sm);
    if (sp) free ((char *)sp);

    return (ret);
}


static void
PrintRes (header,wcs,nmatch,sx1,sy1,sm1,gra1,gdec1,gm1,gnum1,refcat,verbose)

char	*header;	/* Image FITS header */
struct WorldCoor *wcs;	/* Image World Coordinate System */
int	nmatch;		/* Number of image/catalog matches */
double	*sx1, *sy1;	/* Image star pixel coordinates */
double	*sm1;		/* Plate magnitudes */
double	*gra1, *gdec1;	/* Reference catalog sky coordinates */
double	*gm1;		/* Reference catalog magnitudes */
double	*gnum1;		/* Reference catalog numbers */
int	refcat;		/* Reference catalog code */
int	verbose;	/* True for more information */

{
    int i, goff;
    double gx, gy, dx, dy, dx2, dy2, dxy, mag0;
    double sep, sep2, rsep, rsep2, dsep, dsep2;
    double dmatch, dmatch1, sra, sdec;
    double sepsum = 0.0;
    double rsepsum = 0.0;
    double rsep2sum = 0.0;
    double dsepsum = 0.0;
    double dsep2sum = 0.0;
    double sep2sum = 0.0;
    double dxsum = 0.0;
    double dysum = 0.0;
    double dx2sum = 0.0;
    double dy2sum = 0.0;
    double dxysum = 0.0;
    double coeff[5];
    double msig;
    double maxnum;
    double cmax;
    int nnfld;
    int nxyfld;
    char rstr[32], dstr[32], numstr[32], xstr[32], ystr[32], mstr[8];

    maxnum = 0.0;
    for (i = 0; i < nmatch; i++) {
	if (i == 0)
	    maxnum = gnum1[i];
	else if (gnum1[i] > maxnum)
	    maxnum = gnum1[i];
	}
    nnfld = CatNumLen (refcat, maxnum, 0);
    CatMagName (sortmag, refcat, mstr);

    CatID (numstr, refcat);
    if (irafout)
	printf ("#   x      y        ra2000   dec2000  %5s %s", mstr, numstr); 
    else
	printf ("# %s ra2000       dec2000    %5s    X      Y     magi",
		mstr, numstr);
    printf ("    dra   ddec   sep\n");

    /* Find maximum image coordinates and set field size accordingly */
    cmax = 0.0;
    for (i = 0; i < nmatch; i++) {
	if (sx1[i] > cmax) cmax = sx1[i];
	if (sy1[i] > cmax) cmax = sy1[i];
	}
    if (cmax > 9999.0)
	nxyfld = 6 + nxydec;
    else if (cmax > 999.0)
	nxyfld = 5 + nxydec;
    else
	nxyfld = 4 + nxydec;

    for (i = 0; i < nmatch; i++) {
	wcs2pix (wcs, gra1[i], gdec1[i], &gx, &gy, &goff);
	dx = gx - sx1[i];
	dy = gy - sy1[i];
	dx2 = dx * dx;
	dy2 = dy * dy;
	dxy = dx2 + dy2;
	dxsum = dxsum + dx;
	dysum = dysum + dy;
	dx2sum = dx2sum + dx2;
	dy2sum = dy2sum + dy2;
	dxysum = dxysum + sqrt (dxy);
	pix2wcs (wcs, sx1[i], sy1[i], &sra, &sdec);
	sep = 3600.0 * wcsdist(gra1[i],gdec1[i],sra,sdec);
	rsep = 3600.0 * ((gra1[i]-sra) * cos(degrad(sdec)));
	if (rsep > sep)
	    rsep = 3600.0 * ((gra1[i] - sra - 360.0) * cos(degrad(sdec)));
	rsep2 = rsep * rsep;
	dsep = 3600.0 * (gdec1[i] - sdec);
	dsep2 = dsep * dsep;
	sepsum = sepsum + sep;
	rsepsum = rsepsum + rsep;
	dsepsum = dsepsum + dsep;
	rsep2sum = rsep2sum + rsep2;
	dsep2sum = dsep2sum + dsep2;
	sep2sum = sep2sum + (sep*sep);
	ra2str (rstr, 32, gra1[i], 3);
	dec2str (dstr, 32, gdec1[i], 2);
	num2str (xstr, sx1[i], nxyfld, nxydec);
	num2str (ystr, sy1[i], nxyfld, nxydec);
	CatNum (refcat, -nnfld, 0, gnum1[i], numstr);
	if (irafout)
	    printf (" %s %s %s %s %5.2f %s",
		    xstr, ystr, rstr, dstr, gm1[i], numstr);
	else
	    printf ("%s %s %s %5.2f %s %s %6.2f ",
		    numstr, rstr, dstr, gm1[i], xstr, ystr, sm1[i]);
	printf ("%6.2f %6.2f %6.2f\n", rsep, dsep, sep);
	}
    dmatch = (double) nmatch;
    dmatch1 = (double) (nmatch - 1);
    dx = dxsum / dmatch;
    dy = dysum / dmatch;
    dx2 = sqrt (dx2sum / dmatch1);
    dy2 = sqrt (dy2sum / dmatch1);
    dxy = dxysum / dmatch;
    rsep = rsepsum / dmatch;
    dsep = dsepsum / dmatch;
    rsep2 = sqrt (rsep2sum / dmatch1);
    dsep2 = sqrt (dsep2sum / dmatch1);
    sep = sepsum / dmatch;
    sep2 = sqrt (sep2sum / dmatch1);
    printf ("# Mean  dx= %.4f/%.4f  dy= %.4f/%.4f  dxy= %.4f\n",
	    dx, dx2, dy, dy2, dxy);
    printf ("# Mean dra= %.4f/%.4f  ddec= %.4f/%.4f sep= %.4f/%.4f\n",
	    rsep, rsep2, dsep, dsep2, sep, sep2);

    /* Fit and save image to catalog magnitude calibration polynomial */
    if (magfit) {
	mag0 = sm1[0];
	coeff[0] = 0.0;
	coeff[1] = 0.0;
	coeff[2] = 0.0;
	coeff[3] = 0.0;
	coeff[4] = 0.0;
	polfit (sm1, gm1, mag0, nmatch, 4, coeff, &msig);
	printf ("# Plate to catalog mag: mag0=%.6f mcoeff0=%.6f mcoeff1=%.6f\n",
		mag0, coeff[0], coeff[1]);
	printf ("# Plate to catalog mag: mcoeff2=%.6f mcoeff3=%.6f sigma=%.3f\n",
		coeff[2], coeff[3], msig);
	}
    hputi4 (header, "WCSMATCH", nmatch);
    hputnr8 (header, "WCSSEP", 3, sep);

    return;
}


static void
CompRes (header,wcs,nmatch,sx1,sy1,sm1,gra1,gdec1,gm1,gnum1)

char	*header;	/* Image FITS header */
struct WorldCoor *wcs;	/* Image World Coordinate System */
int	nmatch;		/* Number of image/catalog matches */
double	*sx1, *sy1;	/* Image star pixel coordinates */
double	*sm1;		/* Plate magnitudes */
double	*gra1, *gdec1;	/* Reference catalog sky coordinates */
double	*gm1;		/* Reference catalog magnitudes */
double	*gnum1;		/* Reference catalog numbers */

{
    int i, goff;
    double gx, gy, dx, dy, dx2, dy2, dxy, mag0;
    double sep, sep2, rsep, rsep2, dsep, dsep2;
    double dmatch, dmatch1, sra, sdec;
    double sepsum = 0.0;
    double rsepsum = 0.0;
    double rsep2sum = 0.0;
    double dsepsum = 0.0;
    double dsep2sum = 0.0;
    double sep2sum = 0.0;
    double dxsum = 0.0;
    double dysum = 0.0;
    double dx2sum = 0.0;
    double dy2sum = 0.0;
    double dxysum = 0.0;

    for (i = 0; i < nmatch; i++) {
	wcs2pix (wcs, gra1[i], gdec1[i], &gx, &gy, &goff);
	dx = gx - sx1[i];
	dy = gy - sy1[i];
	dx2 = dx * dx;
	dy2 = dy * dy;
	dxy = dx2 + dy2;
	dxsum = dxsum + dx;
	dysum = dysum + dy;
	dx2sum = dx2sum + dx2;
	dy2sum = dy2sum + dy2;
	dxysum = dxysum + sqrt (dxy);
	pix2wcs (wcs, sx1[i], sy1[i], &sra, &sdec);
	sep = 3600.0 * wcsdist(gra1[i],gdec1[i],sra,sdec);
	rsep = 3600.0 * ((gra1[i]-sra) * cos(degrad(sdec)));
	if (rsep > sep)
	    rsep = 3600.0 * ((gra1[i] - sra - 360.0) * cos(degrad(sdec)));
	rsep2 = rsep * rsep;
	dsep = 3600.0 * (gdec1[i] - sdec);
	dsep2 = dsep * dsep;
	sepsum = sepsum + sep;
	rsepsum = rsepsum + rsep;
	dsepsum = dsepsum + dsep;
	rsep2sum = rsep2sum + rsep2;
	dsep2sum = dsep2sum + dsep2;
	sep2sum = sep2sum + (sep*sep);
	}
    dmatch = (double) nmatch;
    dmatch1 = (double) (nmatch - 1);
    dx = dxsum / dmatch;
    dy = dysum / dmatch;
    dx2 = sqrt (dx2sum / dmatch1);
    dy2 = sqrt (dy2sum / dmatch1);
    dxy = dxysum / dmatch;
    rsep = rsepsum / dmatch;
    dsep = dsepsum / dmatch;
    rsep2 = sqrt (rsep2sum / dmatch1);
    dsep2 = sqrt (dsep2sum / dmatch1);
    sep = sepsum / dmatch;
    sep2 = sqrt (sep2sum / dmatch1);

    hputi4 (header, "WCSMATCH", nmatch);
    hputnr8 (header, "WCSSEP", 3, sep);

    return;
}

/* Subroutines to initialize various parameters */

void
settolerance (tol)
double tol;
{ tolerance = tol; return; }


/* Number of decimal places in X and Y image coordinates of sources */
void
setnxydec (ndec)
int ndec;
{ nxydec = ndec; return; }

void
setirafout ()
{ irafout = 1; return; }

void
setmatch (cat)
char *cat;
{ strcpy (matchcat, cat); return; }

void
setreflim (lim1, lim2)
double lim1, lim2;
{ refmag2 = lim2;
  if (lim1 > -2.0) refmag1 = lim1;
  return; }

void
setfitwcs (wfit)
int wfit;
{ fitwcs = wfit; return; }

void
setfitplate (nc)
int nc;
{ fitplate = nc; return; }

void
setminstars (minstars)
int minstars;
{ minstars0 = minstars;
  setminbin (minstars);
  return; }

void
setnofit ()
{ nofit = 1; return; }

void
setfrac (frac0)
double frac0;
{ if (frac0 < 1.0) frac = 1.0 + frac0;
    else frac = frac0;
  return; }

/* Fraction by which to increase dimensions of area to be searched */
void
setimfrac (frac0)
double frac0;
{ if (frac0 > 0.0) imfrac0 = frac0;
  else imfrac0 = 1.0;
  return; }

void
setmaxcat (ncat)
int ncat;
{ if (ncat < 1) maxcat = 25;
  else maxcat = ncat;
  return; }

void
setiterate (iter)
int iter;
{ iterate0 = iterate0 + iter;
  return; }

void
setnfiterate (iter)
int iter;
{ nfiterate0 = nfiterate0 + iter;
  return; }

void
setiteratet (iter)
int iter;
{ toliterate0 = toliterate0 + iter;
  return; }

void
setrecenter (recenter)
int recenter;
{ recenter0 = recenter;
  return; }

void
setsortmag (imag)
int imag;
{ sortmag = imag;
  return; }

void
setmagfit ()
{magfit++; return;}

/* Feb 29 1996	New program
 * Apr 30 1996	Add FOCAS-style catalog matching
 * May  1 1996	Add initial image center from command line
 * May  2 1996	Set up four star matching modes
 * May 15 1996	Pass verbose flag; allow other reference catalogs
 * May 16 1996	Remove sorting to separate file sortstar.c
 * May 17 1996	Add class and verbose arguments
 * May 22 1996  Allow for named reference catalog
 * May 23 1996	Use pre-existing WCS for center, if it is present
 * May 29 1996	Simplify program by always using WCS structure
 * May 30 1996	Make reference/image pair matching the default method
 * Jun 11 1996  Number and zero positions of image stars
 * Jun 12 1996	Be more careful with nominal WCS setting
 * Jun 14 1996	Add residual table
 * Jun 28 1996	Set FITS header from WCS
 * Jul  3 1996	Set epoch from old equinox if not already set
 * Jul 19 1996	Declare tabread
 * Jul 19 1996	Set image center in WCS if DSS WCS
 * Jul 22 1996	Debug tab table reading
 * Aug  5 1996	Add option to change WCS projection
 * Aug  5 1996	Check for SECPIX1 as well as SECPIX
 * Aug  5 1996	Set number of parameters to fit here
 * Aug  7 1996	Save specified number of decimal places in header parameters
 * Aug  7 1996	Rename old center parameters
 * Aug 26 1996	Decrease default pixel tolerance from 20 to 10
 * Aug 26 1996	Drop unused variable EQ in setfitswcs
 * Aug 28 1996	Improve output format for matched stars
 * Sep  1 1996	Set some defaults in lwcs.h
 * Sep  3 1996	Fix bug to set plate scale from command line
 * Sep  4 1996	Print reference catalog name on separate line from entries
 * Sep 17 1996	Clean up code
 * Oct 15 1996	Break off getfitswcs into separate file
 * Nov 18 1996	Add USNO A catalog searching
 * Nov 18 1996	Write same number into CROAT2 as CROTA1
 * Nov 19 1996	If EPOCH was equinox in original image or not set, set it
 * Dec 10 1996	Make equinox double in getfitswcs call
 *
 * Mar 17 1997	Print found reference stars even when there are not enough
 * Jul 14 1997	If nfit is negative return with header set for nominal WCS
 * Aug  4 1997	Reset nfit limit to 7 for reference pixel fit and fix nfit0 bug
 * Aug 20 1997	Make maximum number of reference stars settable on the command line
 * Aug 28 1997	Print star ID numbers in appropriate format for each catalog
 * Aug 28 1997	Add option to match image to reference stars without fitting WCS
 * Sep  3 1997	Add option to change image dimensions by a fraction
 * Sep  9 1997	Return with default WCS in header only if nfit < -7
 * Sep  9 1997	Print separate right ascension and declination residuals
 * Sep 11 1997	Print average magnitude as well as value of residuals
 * Oct 16 1997	Print same information for image stars as for reference stars
 * Oct 22 1997	Print result of chip rotation as well as optical axis rotation
 * Nov  6 1997	Move nfit entirely to matchstar
 * Nov  6 1997	Rearrange output for IMMATCH use, adding filename argument
 * Nov 14 1997	Increase, not multiply, dimensions by IMFRAC
 * Nov 17 1997	Initialize both magnitude limits
 * Dec  1 1997	Fix bug computing RA separation
 * Dec 16 1997	Fix bug printing no match error message
 *
 * Jan 26 1998	Remove chip rotation code
 * Jan 27 1998	Add switch to use either Calabretta or classic AIPS WCS
 * Jan 29 1998	Fix summary to keep only one reference star per image star
 * Feb  3 1998	Add option to improve WCS by fitting residuals
 * Feb 12 1998	Add USNO SA catalog to reference catalog options
 * Feb 12 1998	Match stars even if less than the minimum if no WCS fit
 * Feb 19 1998	Increase number of reference stars if IMFRAC > 1
 * Feb 22 1998	Fix residual fitting
 * Mar  3 1998	Repeat field search and match after first try
 * Mar  4 1998	Use imfrac on first pass, but not second
 * Mar  5 1998	Correct number of stars used if IMFRAC > 1
 * Mar  6 1998	Add option to use image center for second pass
 * Mar 25 1998	Change residual polynomial fit to full plate-style polynomial
 * Mar 25 1998	Move residual printing to a subroutine
 * Mar 26 1998	Do not fit polynomial until both recenter and iterate done
 * Mar 27 1998	Save plate fit coefficients to FITS header
 * Apr  8 1998	Reset equinox to that of reference catalog
 * Apr 30 1998	Handle prematched star/pixel file
 * Jun 24 1998	Add string lengths to ra2str() and dec2str() calls
 * Sep 17 1998	Allow use of catalogs with other than J2000 coordinates
 * Sep 17 1998	Add coordinate system argument to GetFITSWCS()
 * Sep 28 1998	Add SAO binary format catalogs (SAO, PPM, IRAS, Tycho)
 * Sep 28 1998	Pass system, equinox, and epoch to all catalog search programs
 * Oct  2 1998	Fix arguments in call to GetFITSWCS
 * Oct  7 1998	Set projection to TAN before fitting
 * Oct 16 1998	Add option to read from TDC ASCII format catalog
 * Oct 26 1998	Use passed refcatname and new RefCat subroutine and wcscat.h
 * Oct 26 1998	Add TDC binary catalog option
 * Oct 28 1998	Only search for sources in image once
 * Nov 19 1998	Add catalog name to uacread() call
 * Dec  1 1998	Add version 2.0 of USNO A and SA catalogs
 * Dec  8 1998	Add support for ACT and Hipparcos catalogs

 * Jan  9 1999	Fix bug so that no fit option works
 * Jan 26 1999	Add option to output matched image/catalog stars
 * Feb 10 1999	Finish support for ACT reference catalog
 * Apr  7 1999	Add file name to GetFITSWCS call
 * Apr 21 1999	Fix RA residual bug: *cos(dec), not /cos(dec)
 * Jul  7 1999	Fix bug setting secpix when iterating
 * Jul  7 1999	List catalog and image stars without linefeeds
 * Jul  9 1999	Log tabread() every 100 if verbose
 * Jul 23 1999	Add BSC for very wide fields
 * Jul 26 1999	Add WCSIMCAT, WCSFRCAT, WCSMATCH, and WCSSEP to header
 * Jul 26 1999	Always compute residuals so WCSMATCH and WCSSEP match WCS
 * Jul 27 1999	Add WCSNREF, maximum possible matches
 * Aug 26 1999	Handle true number return from search subroutines
 * Aug 31 1999	Set image catalog name when only matching stars
 * Sep 13 1999	Do all catalog searches through catread()
 * Sep 15 1999	Fix improper uses of ng instead of nrg
 * Sep 16 1999	Add zero distsort argument to catread() call
 * Sep 29 1999	Add option to start with pre-matched stars
 * Oct 22 1999	Change catread() to ctgread() to avoid system conflict
 * Nov 23 1999	Free wcs only after it is used to set up iterate or recenter
 *
 * Feb  8 2000	If iterating, halve pixel tolerance on second pass
 * Feb 11 2000	Print maximum number of matches with number matched
 * Feb 15 2000	Drop maximum number of image stars
 * Feb 15 2000	When iterating, reinitialize CD matrix if it's being fit
 * Mar  1 2000	Modify residual output so = used instead of :
 * Mar  1 2000	Add seperate option to tighten tolerances when iterating
 * Mar 10 2000	Add proper motion arguments to ctgread()
 * Mar 10 2000	Do not change WCS unless fitting
 * Mar 13 2000	Use PropCat() to dind out whether catalog has proper motion
 * Mar 15 2000	Add proper motion arguments to RASortStars() and ctgread()
 * Mar 28 2000	Separate tolerance reducing iterations and other iterations
 * May 26 2000	Set catalog number field size using CatNumLen()
 * Jun 22 2000	Fix bug created in last update (found by J.-B. Marquette)
 * Jul 12 2000	Add catalog data structre to ctgread() call
 * Jul 25 2000	Pass address of star catalog data structure address
 * Dec  6 2000	If no reference catalog is set, skip catalog fit
 * Dec  6 2000	Drop static refcatname and setrefcat()
 * Dec 18 2000	Always allocate proper motion arrays; clean up code after lint
 * Dec 18 2000	Call ReadMatch() to read file of X/Y/RA/Dec matches
 *
 * Jan  8 2001	Add verbose flag to ReadMatch() call
 * Jan  9 2001	Fix bug in FitMatch() call
 * Jan 11 2001	All output except residuals to stderr
 * Mar  1 2001	Fill in catalog name using CatName() if not set
 * Jun  7 2001	Add proper motion flag and number of magnitudes to RefCat()
 * Jun 11 2001	Set refep from wcs->epoch after WCS is set
 * Jul 20 2001	FindStars() now returns magnitude instead of flux
 * Jul 24 2001	Add code to fit plate to catalog magnitude polynomial
 * Jul 25 2001	Add headings to residual table
 * Aug  2 2001	If fitting fewer than MINSTARS parameters, allow 1 match
 * Sep 11 2001	Add magnitude selection
 * Sep 11 2001	Use single, 2-D magnitude argument to ctgread()
 * Sep 13 2001	Add reference catalog magnitude selection
 * Sep 19 2001	Drop fitshead.h; it is in wcs.h
 * Oct 16 2001	Add command line parameter setting using keyword=value
 * Oct 29 2001	Print number of matches after residuals to avoid scroll-off
 * Nov  1 2001	Add goff to StarMatch() arguments
 * Nov  5 2001	Add refcat and gnum to StarMatch() arguments
 * Nov  6 2001	If iterating keep exact fit WCS after first pass
 * Nov  6 2001	Change sb to sm because it is now always a magnitude
 * Nov  6 2001	Fix image star magnitude sorting error
 * Nov  7 2001	Allow minimum number of stars for match to be set externally
 * Nov  7 2001	Drop out if match is unsuccessful
 *
 * Jan 18 2002	Allocate sm when using prematched stars
 * Jan 23 2002	Add zap=0 to FindStar() argument list
 * Jan 24 2002	Handle catalog nmag = 0 safely
 * Apr 10 2002	Allow sort/limit magnitude to be specified by letter as well as number
 * Jul 31 2002	Add iteration with increasing number of parameters to be fit
 * Aug  2 2002	Use WCSMatch() to set initial values for pre-matched stars
 * Sep  4 2002	Don't iterate if there is no catalog
 *
 * Aug 22 2003	Add inner radius =0.0 argument to ctgread call
 * Dec 12 2003	Add second argument to CatName()
 *
 * Aug 30 2004	Declare void undeclared set*() subroutines
 *
 * Mar 30 2006	Allow number of decimal places in image coordinates to be set
 * Jun  8 2006	Print object name instead of number if necessary
 * Jun 19 2006	Initialize uninitialized variables
 *
 * Jan  9 2006	Drop declarations of fk425e() and fk524e(); moved to wcs.h
 * Jan 10 2007	Drop setclass() and setplate(); it did not do anything
 * Jan 11 2007	Include fitsfile.h
 * Mar 14 2007	Return if -n 0 after computing WCS from match
 *
 * Aug  3 2009	If not printing residuals, still compute WCSSEP using CompRes()
 * Sep 24 2009	Free pointers more carefully
 * Nov 13 2009	Print catalog magnitude name  in residual output header
 * Dec 14 2009	Allow more than nmag magnitudes to save other things
 */
