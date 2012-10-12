/*** File libwcs/wcsinit.c
 *** March 24, 2009
 *** By Doug Mink, dmink@cfa.harvard.edu
 *** Harvard-Smithsonian Center for Astrophysics
 *** Copyright (C) 1998-2009
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

 * Module:	wcsinit.c (World Coordinate Systems)
 * Purpose:	Convert FITS WCS to pixels and vice versa:
 * Subroutine:	wcsinit (hstring) sets a WCS structure from an image header
 * Subroutine:	wcsninit (hstring,lh) sets a WCS structure from fixed-length header
 * Subroutine:	wcsinitn (hstring, name) sets a WCS structure for specified WCS
 * Subroutine:	wcsninitn (hstring,lh, name) sets a WCS structure for specified WCS
 * Subroutine:	wcsinitc (hstring, mchar) sets a WCS structure if multiple
 * Subroutine:	wcsninitc (hstring,lh,mchar) sets a WCS structure if multiple
 * Subroutine:	wcschar (hstring, name) returns suffix for specifed WCS
 * Subroutine:	wcseq (hstring, wcs) set radecsys and equinox from image header
 * Subroutine:	wcseqm (hstring, wcs, mchar) set radecsys and equinox if multiple
 */

#include <string.h>		/* strstr, NULL */
#include <stdio.h>		/* stderr */
#include <math.h>
#include "wcs.h"
#ifndef VMS
#include <stdlib.h>
#endif

static void wcseq();
static void wcseqm();
static void wcsioset();
void wcsrotset();
char wcschar();

/* set up a WCS structure from a FITS image header lhstring bytes long 
 * for a specified WCS name */

struct WorldCoor *
wcsninitn (hstring, lhstring, name)

const char *hstring;	/* character string containing FITS header information
		   	in the format <keyword>= <value> [/ <comment>] */
int	lhstring;	/* Length of FITS header in bytes */
const char *name;		/* character string with identifying name of WCS */
{
    hlength (hstring, lhstring);
    return (wcsinitn (hstring, name));
}


/* set up a WCS structure from a FITS image header for specified WCSNAME */

struct WorldCoor *
wcsinitn (hstring, name)

const char *hstring;	/* character string containing FITS header information
			   in the format <keyword>= <value> [/ <comment>] */
const char *name;		/* character string with identifying name of WCS */
{
    char mchar;		/* Suffix character for one of multiple WCS */

    mchar = wcschar (hstring, name);
    if (mchar == '_') {
	fprintf (stderr, "WCSINITN: WCS name %s not matched in FITS header\n",
		 name);
	return (NULL);
	}
    return (wcsinitc (hstring, &mchar));
}


/* WCSCHAR -- Find the letter for a specific WCS conversion */

char
wcschar (hstring, name)

const char *hstring;	/* character string containing FITS header information
		   	in the format <keyword>= <value> [/ <comment>] */
const char *name;		/* Name of WCS conversion to be matched
			   (case-independent) */
{
    char *upname, *uppercase();
    char cwcs, charwcs;
    int iwcs;
    char keyword[12];
    char *upval, value[72];

    /* If no WCS character, return 0 */
    if (name == NULL)
	return ((char) 0);

    /* Convert input name to upper case */
    upname = uppercase (name);

    /* If single character name, return that character */
    if (strlen (upname) == 1)
	return (upname[0]);

    /* Try to match input name to available WCSNAME names in header */
    strcpy (keyword, "WCSNAME");
    keyword[8] = (char) 0;
    charwcs = '_';
    for (iwcs = 0; iwcs < 27; iwcs++) {
	if (iwcs > 0)
	    cwcs = (char) (64 + iwcs);
	else
	    cwcs = (char) 0;
	keyword[7] = cwcs;
	if (hgets (hstring, keyword, 72, value)) {
	    upval = uppercase (value);
	    if (!strcmp (upval, upname))
		charwcs = cwcs;
	    free (upval);
	    }
	}
    free (upname);
    return (charwcs);
}


/* Make string of arbitrary case all uppercase */

char *
uppercase (string)
char *string;
{
    int lstring, i;
    char *upstring;

    lstring = strlen (string);
    upstring = (char *) calloc (1,lstring+1);
    for (i = 0; i < lstring; i++) {
	if (string[i] > 96 && string[i] < 123)
	    upstring[i] = string[i] - 32;
	else
	    upstring[i] = string[i];
	}
    upstring[lstring] = (char) 0;
    return (upstring);
}


/* set up a WCS structure from a FITS image header lhstring bytes long */

struct WorldCoor *
wcsninit (hstring, lhstring)

const char *hstring;	/* character string containing FITS header information
		   	in the format <keyword>= <value> [/ <comment>] */
int	lhstring;	/* Length of FITS header in bytes */
{
    char mchar;		/* Suffix character for one of multiple WCS */
    mchar = (char) 0;
    hlength (hstring, lhstring);
    return (wcsinitc (hstring, &mchar));
}


/* set up a WCS structure from a FITS image header lhstring bytes long */

struct WorldCoor *
wcsninitc (hstring, lhstring, mchar)

const char *hstring;	/* character string containing FITS header information
		   	in the format <keyword>= <value> [/ <comment>] */
int	lhstring;	/* Length of FITS header in bytes */
char	*mchar;		/* Suffix character for one of multiple WCS */
{
    hlength (hstring, lhstring);
    if (mchar[0] == ' ')
	mchar[0] = (char) 0;
    return (wcsinitc (hstring, mchar));
}


/* set up a WCS structure from a FITS image header */

struct WorldCoor *
wcsinit (hstring)

const char *hstring;	/* character string containing FITS header information
			   in the format <keyword>= <value> [/ <comment>] */
{
    char mchar;		/* Suffix character for one of multiple WCS */
    mchar = (char) 0;
    return (wcsinitc (hstring, &mchar));
}


/* set up a WCS structure from a FITS image header for specified suffix */

struct WorldCoor *
wcsinitc (hstring, wchar)

const char *hstring;	/* character string containing FITS header information
			   in the format <keyword>= <value> [/ <comment>] */
char *wchar;		/* Suffix character for one of multiple WCS */
{
    struct WorldCoor *wcs, *depwcs;
    char ctype1[32], ctype2[32], tstring[32];
    char pvkey1[8],pvkey2[8],pvkey3[8];
    char *hcoeff;		/* pointer to first coeff's in header */
    char decsign;
    double rah,ram,ras, dsign,decd,decm,decs;
    double dec_deg,ra_hours, secpix, ra0, ra1, dec0, dec1, cvel;
    double cdelt1, cdelt2, cd[4], pc[81];
    char keyword[16];
    int ieq, i, j, k, naxes, cd11p, cd12p, cd21p, cd22p;
    int ilat;	/* coordinate for latitude or declination */
    /*
    int ix1, ix2, iy1, iy2, idx1, idx2, idy1, idy2;
    double dxrefpix, dyrefpix;
    */
    char temp[80];
    char wcsname[64];	/* Name of WCS depended on by current WCS */
    char mchar;
    char cspace = (char) ' ';
    char cnull = (char) 0;
    double mjd;
    double rot;
    double ut;
    int nax;
    int twod;
    int iszpx = 0;
    extern int tnxinit();
    extern int platepos();
    extern int dsspos();

    wcs = (struct WorldCoor *) calloc (1, sizeof(struct WorldCoor));

    /* Set WCS character and name in structure */
    mchar = wchar[0];
    if (mchar == ' ')
	mchar = cnull;
    wcs->wcschar = mchar;
    if (hgetsc (hstring, "WCSNAME", &mchar, 63, wcsname)) {
	wcs->wcsname = (char *) calloc (strlen (wcsname)+2, 1);
	strcpy (wcs->wcsname, wcsname);
	}
    

    /* Set WCSLIB flags so that structures will be reinitialized */
    wcs->cel.flag = 0;
    wcs->lin.flag = 0;
    wcs->wcsl.flag = 0;
    wcs->wcsl.cubeface = -1;

    /* Initialize to no plate fit */
    wcs->ncoeff1 = 0;
    wcs->ncoeff2 = 0;

    /* Initialize to no CD matrix */
    cdelt1 = 0.0;
    cdelt2 = 0.0;
    cd[0] = 0.0;
    cd[1] = 0.0;
    cd[2] = 0.0;
    cd[3] = 0.0;
    pc[0] = 0.0;
    wcs->rotmat = 0;
    wcs->rot = 0.0;

    /* Header parameters independent of projection */
    naxes = 0;
    hgeti4c (hstring, "WCSAXES", &mchar, &naxes);
    if (naxes == 0)
	hgeti4 (hstring, "WCSAXES", &naxes);
    if (naxes == 0)
	hgeti4 (hstring, "NAXIS", &naxes);
    if (naxes == 0)
	hgeti4 (hstring, "WCSDIM", &naxes);
    if (naxes < 1) {
	setwcserr ("WCSINIT: No WCSAXES, NAXIS, or WCSDIM keyword");
	wcsfree (wcs);
	return (NULL);
	}
    if (naxes > 2)
	naxes = 2;
    wcs->naxis = naxes;
    wcs->naxes = naxes;
    wcs->lin.naxis = naxes;
    wcs->nxpix = 0;
    hgetr8 (hstring, "NAXIS1", &wcs->nxpix);
    if (wcs->nxpix < 1)
	hgetr8 (hstring, "IMAGEW", &wcs->nxpix);
    if (wcs->nxpix < 1) {
	setwcserr ("WCSINIT: No NAXIS1 or IMAGEW keyword");
	wcsfree (wcs);
	return (NULL);
	}
    wcs->nypix = 0;
    hgetr8 (hstring, "NAXIS2", &wcs->nypix);
    if (wcs->nypix < 1)
	hgetr8 (hstring, "IMAGEH", &wcs->nypix);
    if (naxes > 1 && wcs->nypix < 1) {
	setwcserr ("WCSINIT: No NAXIS2 or IMAGEH keyword");
	wcsfree (wcs);
	return (NULL);
	}

    /* Reset number of axes to only those with dimension greater than one */
    nax = 0;
    for (i = 0; i < naxes; i++) {

	/* Check for number of pixels in axis more than one */
	strcpy (keyword, "NAXIS");
	sprintf (temp, "%d", i+1);
	strcat (keyword, temp);
	if (!hgeti4 (hstring, keyword, &j)) {
	    if (i == 0 && wcs->nxpix > 1) {
		/* fprintf (stderr,"WCSINIT: Missing keyword %s set to %.0f from IMAGEW\n",
			 keyword, wcs->nxpix); */
		j = wcs->nxpix;
		}
	    else if (i == 1 && wcs->nypix > 1) {
		/* fprintf (stderr,"WCSINIT: Missing keyword %s set to %.0f from IMAGEH\n",
			 keyword, wcs->nypix); */
		j = wcs->nypix;
		}
	    else
		fprintf (stderr,"WCSINIT: Missing keyword %s assumed 1\n",keyword);
	    }

	/* Check for TAB WCS in axis */
	strcpy (keyword, "CTYPE");
	strcat (keyword, temp);
	if (hgets (hstring, keyword, 16, temp)) {
	    if (strsrch (temp, "-TAB"))
		j = 0;
	    }
	if (j > 1) nax = nax + 1;
	}
    naxes = nax;
    wcs->naxes = nax;
    wcs->naxis = nax;

    hgets (hstring, "INSTRUME", 16, wcs->instrument);
    hgeti4 (hstring, "DETECTOR", &wcs->detector);
    wcs->wcsproj = getdefwcs();
    wcs->logwcs = 0;
    hgeti4 (hstring, "DC-FLAG", &wcs->logwcs);

    /* Initialize rotation matrices */
    for (i = 0; i < 81; i++) wcs->pc[i] = 0.0;
    for (i = 0; i < 81; i++) pc[i] = 0.0;
    for (i = 0; i < naxes; i++) wcs->pc[(i*naxes)+i] = 1.0;
    for (i = 0; i < naxes; i++) pc[(i*naxes)+i] = 1.0;
    for (i = 0; i < 9; i++) wcs->cdelt[i] = 0.0;
    for (i = 0; i < naxes; i++) wcs->cdelt[i] = 1.0;

    /* If the current world coordinate system depends on another, set it now */
    if (hgetsc (hstring, "WCSDEP",&mchar, 63, wcsname)) {
	if ((wcs->wcs = wcsinitn (hstring, wcsname)) == NULL) {
	    setwcserr ("WCSINIT: depended on WCS could not be set");
	    wcsfree (wcs);
	    return (NULL);
	    }
	depwcs = wcs->wcs;
	depwcs->wcsdep = wcs;
	}
    else
	wcs->wcs = NULL;

    /* Read radial velocity from image header */
    wcs->radvel = 0.0;
    wcs->zvel = 0.0;
    cvel = 299792.5;
    if (hgetr8c (hstring, "VSOURCE", &mchar, &wcs->radvel))
	wcs->zvel = wcs->radvel / cvel;
    else if (hgetr8c (hstring, "ZSOURCE", &mchar, &wcs->zvel))
	wcs->radvel = wcs->zvel * cvel;
    else if (hgetr8 (hstring, "VELOCITY", &wcs->radvel))
	wcs->zvel = wcs->radvel / cvel;

    for (i = 0; i < 10; i++) {
	wcs->prj.p[i] = 0.0;
	}

    /* World coordinate system reference coordinate information */
    if (hgetsc (hstring, "CTYPE1", &mchar, 16, ctype1)) {
	if (!strncmp (ctype1+5,"ZPX", 3)) {
	    iszpx = 1;
	    ctype1[7] = 'N';

	    /* IRAF ZPX parameters for ZPN projection */
	    for (i = 0; i < 10; i++) {
		sprintf (keyword,"projp%d",i);
		mgetr8 (hstring, "WAT1",keyword, &wcs->prj.p[i]);
		}
	    }
	else
	    iszpx = 0;

	/* Read second coordinate type */
	strcpy (ctype2, ctype1);
	if (!hgetsc (hstring, "CTYPE2", &mchar, 16, ctype2))
	    twod = 0;
	else {
	    twod = 1;
	    if (!strncmp (ctype2+5,"ZPX", 3)) {
		iszpx = 1;
		ctype2[7] = 'N';
		}
	    }
	strcpy (wcs->ctype[0], ctype1);
	strcpy (wcs->ctype[1], ctype2);
	if (strsrch (ctype2, "LAT") || strsrch (ctype2, "DEC"))
	    ilat = 2;
	else
	    ilat = 1;

	/* Read third and fourth coordinate types, if present */
	strcpy (wcs->ctype[2], "");
	hgetsc (hstring, "CTYPE3", &mchar, 9, wcs->ctype[2]);
	strcpy (wcs->ctype[3], "");
	hgetsc (hstring, "CTYPE4", &mchar, 9, wcs->ctype[3]);

	/* Set projection type in WCS data structure */
	if (wcstype (wcs, ctype1, ctype2)) {
	    wcsfree (wcs);
	    return (NULL);
	    }

	/* Get units, if present, for linear coordinates */
	if (wcs->prjcode == WCS_LIN) {
	    if (!hgetsc (hstring, "CUNIT1", &mchar, 16, wcs->units[0])) {
		if (!mgetstr (hstring, "WAT1", "units", 16, wcs->units[0])) {
		    wcs->units[0][0] = 0;
		    }
		}
	    if (!strcmp (wcs->units[0], "pixel"))
		wcs->prjcode = WCS_PIX;
	    if (twod) {
		if (!hgetsc (hstring, "CUNIT2", &mchar, 16, wcs->units[1])) {
		    if (!mgetstr (hstring, "WAT2", "units", 16, wcs->units[1])) {
			wcs->units[1][0] = 0;
			}
		    }
		if (!strcmp (wcs->units[0], "pixel"))
		    wcs->prjcode = WCS_PIX;
		}
	    }

	/* Reference pixel coordinates and WCS value */
	wcs->crpix[0] = 1.0;
	hgetr8c (hstring, "CRPIX1", &mchar, &wcs->crpix[0]);
	wcs->crpix[1] = 1.0;
	hgetr8c (hstring, "CRPIX2", &mchar, &wcs->crpix[1]);
	wcs->xrefpix = wcs->crpix[0];
	wcs->yrefpix = wcs->crpix[1];
	wcs->crval[0] = 0.0;
	hgetr8c (hstring, "CRVAL1", &mchar, &wcs->crval[0]);
	wcs->crval[1] = 0.0;
	hgetr8c (hstring, "CRVAL2", &mchar, &wcs->crval[1]);
	if (wcs->syswcs == WCS_NPOLE)
	    wcs->crval[1] = 90.0 - wcs->crval[1];
	if (wcs->syswcs == WCS_SPA)
	    wcs->crval[1] = wcs->crval[1] - 90.0;
	wcs->xref = wcs->crval[0];
	wcs->yref = wcs->crval[1];
	if (wcs->coorflip) {
	    wcs->cel.ref[0] = wcs->crval[1];
	    wcs->cel.ref[1] = wcs->crval[0];
	    }
	else {
	    wcs->cel.ref[0] = wcs->crval[0];
	    wcs->cel.ref[1] = wcs->crval[1];
	    }
	wcs->longpole = 999.0;
	hgetr8c (hstring, "LONPOLE", &mchar, &wcs->longpole);
	wcs->cel.ref[2] = wcs->longpole;
	wcs->latpole = 999.0;
	hgetr8c (hstring, "LATPOLE", &mchar, &wcs->latpole);
	wcs->cel.ref[3] = wcs->latpole;
	wcs->lin.crpix = wcs->crpix;
	wcs->lin.cdelt = wcs->cdelt;
	wcs->lin.pc = wcs->pc;

	/* Projection constants (this should be projection-dependent */
	wcs->prj.r0 = 0.0;
	hgetr8c (hstring, "PROJR0", &mchar, &wcs->prj.r0);

	/* FITS WCS interim proposal projection constants */
	for (i = 0; i < 10; i++) {
	    sprintf (keyword,"PROJP%d",i);
	    hgetr8c (hstring, keyword, &mchar, &wcs->prj.p[i]);
	    }

	sprintf (pvkey1, "PV%d_1", ilat);
	sprintf (pvkey2, "PV%d_2", ilat);
	sprintf (pvkey3, "PV%d_3", ilat);

	/* FITS WCS standard projection constants (projection-dependent) */
	if (wcs->prjcode == WCS_AZP || wcs->prjcode == WCS_SIN ||
	    wcs->prjcode == WCS_COP || wcs->prjcode == WCS_COE ||
	    wcs->prjcode == WCS_COD || wcs->prjcode == WCS_COO) {
	    hgetr8c (hstring, pvkey1, &mchar, &wcs->prj.p[1]);
	    hgetr8c (hstring, pvkey2, &mchar, &wcs->prj.p[2]);
	    }
	else if (wcs->prjcode == WCS_SZP) {
	    hgetr8c (hstring, pvkey1, &mchar, &wcs->prj.p[1]);
	    hgetr8c (hstring, pvkey2, &mchar, &wcs->prj.p[2]);
	    if (wcs->prj.p[3] == 0.0)
		wcs->prj.p[3] = 90.0;
	    hgetr8c (hstring, pvkey3, &mchar, &wcs->prj.p[3]);
	    }
	else if (wcs->prjcode == WCS_CEA) {
	    if (wcs->prj.p[1] == 0.0)
		wcs->prj.p[1] = 1.0;
	    hgetr8c (hstring, pvkey1, &mchar, &wcs->prj.p[1]);
	    }
	else if (wcs->prjcode == WCS_CYP) {
	    if (wcs->prj.p[1] == 0.0)
		wcs->prj.p[1] = 1.0;
	    hgetr8c (hstring, pvkey1, &mchar, &wcs->prj.p[1]);
	    if (wcs->prj.p[2] == 0.0)
		wcs->prj.p[2] = 1.0;
	    hgetr8c (hstring, pvkey2, &mchar, &wcs->prj.p[2]);
	    }
	else if (wcs->prjcode == WCS_AIR) {
	    if (wcs->prj.p[1] == 0.0)
		wcs->prj.p[1] = 90.0;
	    hgetr8c (hstring, pvkey1, &mchar, &wcs->prj.p[1]);
	    }
	else if (wcs->prjcode == WCS_BON) {
	    hgetr8c (hstring, pvkey1, &mchar, &wcs->prj.p[1]);
	    }
	else if (wcs->prjcode == WCS_ZPN) {
	    for (i = 0; i < 10; i++) {
		sprintf (keyword,"PV%d_%d", ilat, i);
		hgetr8c (hstring, keyword, &mchar, &wcs->prj.p[i]);
		}
	    }

	/* Initialize TNX, defaulting to TAN if there is a problem */
	if (wcs->prjcode == WCS_TNX) {
	    if (tnxinit (hstring, wcs)) {
		wcs->ctype[0][6] = 'A';
		wcs->ctype[0][7] = 'N';
		wcs->ctype[1][6] = 'A';
		wcs->ctype[1][7] = 'N';
		wcs->prjcode = WCS_TAN;
		}
	    }

	/* If ZPX, read coefficients from WATi keyword */
	if (iszpx) {
	    char mkey[8];
	    sprintf (mkey,"WAT%d", ilat);
	    for (i = 0; i < 10; i++) {
		wcs->prj.p[i] = 0.0;
		sprintf (keyword,"projp%d",i);
		mgetr8 (hstring, mkey, keyword, &wcs->prj.p[i]);
		}
	    }


	/* Coordinate reference frame, equinox, and epoch */
	if (wcs->wcsproj > 0)
	    wcseqm (hstring, wcs, &mchar);
	wcsioset (wcs);

	/* Read distortion coefficients, if present */
	distortinit (wcs, hstring);

	/* Use polynomial fit instead of projection, if present */
	wcs->ncoeff1 = 0;
	wcs->ncoeff2 = 0;
	cd11p = hgetr8c (hstring, "CD1_1", &mchar, &cd[0]);
	cd12p = hgetr8c (hstring, "CD1_2", &mchar, &cd[1]);
	cd21p = hgetr8c (hstring, "CD2_1", &mchar, &cd[2]);
	cd22p = hgetr8c (hstring, "CD2_2", &mchar, &cd[3]);
	if (wcs->wcsproj != WCS_OLD &&
	    (hcoeff = ksearch (hstring,"CO1_1")) != NULL) {
	    wcs->prjcode = WCS_PLT;
	    (void)strcpy (wcs->ptype, "PLATE");
	    for (i = 0; i < 20; i++) {
		sprintf (keyword,"CO1_%d", i+1);
		wcs->x_coeff[i] = 0.0;
		if (hgetr8 (hcoeff, keyword, &wcs->x_coeff[i]))
		    wcs->ncoeff1 = i + 1;
		}
	    hcoeff = ksearch (hstring,"CO2_1");
	    for (i = 0; i < 20; i++) {
		sprintf (keyword,"CO2_%d",i+1);
		wcs->y_coeff[i] = 0.0;
		if (hgetr8 (hcoeff, keyword, &wcs->y_coeff[i]))
		    wcs->ncoeff2 = i + 1;
		}

	    /* Compute a nominal scale factor */
	    platepos (wcs->crpix[0], wcs->crpix[1], wcs, &ra0, &dec0);
	    platepos (wcs->crpix[0], wcs->crpix[1]+1.0, wcs, &ra1, &dec1);
	    wcs->yinc = dec1 - dec0;
	    wcs->xinc = -wcs->yinc;

	    /* Compute image rotation angle */
	    wcs->wcson = 1;
	    wcsrotset (wcs);
	    rot = degrad (wcs->rot);

	    /* Compute scale at reference pixel */
	    platepos (wcs->crpix[0], wcs->crpix[1], wcs, &ra0, &dec0);
	    platepos (wcs->crpix[0]+cos(rot),
		      wcs->crpix[1]+sin(rot), wcs, &ra1, &dec1);
	    wcs->cdelt[0] = -wcsdist (ra0, dec0, ra1, dec1);
	    wcs->xinc = wcs->cdelt[0];
	    platepos (wcs->crpix[0]+sin(rot),
		      wcs->crpix[1]+cos(rot), wcs, &ra1, &dec1);
	    wcs->cdelt[1] = wcsdist (ra0, dec0, ra1, dec1);
	    wcs->yinc = wcs->cdelt[1];

	    /* Set CD matrix from header */
	    wcs->cd[0] = cd[0];
	    wcs->cd[1] = cd[1];
	    wcs->cd[2] = cd[2];
	    wcs->cd[3] = cd[3];
	    (void) matinv (2, wcs->cd, wcs->dc);
	    }

	/* Else use CD matrix, if present */
	else if (cd11p || cd12p || cd21p || cd22p) {
	    wcs->rotmat = 1;
	    wcscdset (wcs, cd);
	    }

	/* Else get scaling from CDELT1 and CDELT2 */
	else if (hgetr8c (hstring, "CDELT1", &mchar, &cdelt1) != 0) {
	    hgetr8c (hstring, "CDELT2", &mchar, &cdelt2);

	    /* If CDELT1 or CDELT2 is 0 or missing */
	    if (cdelt1 == 0.0 || (wcs->nypix > 1 && cdelt2 == 0.0)) {
		if (ksearch (hstring,"SECPIX") != NULL ||
		    ksearch (hstring,"PIXSCALE") != NULL ||
		    ksearch (hstring,"PIXSCAL1") != NULL ||
		    ksearch (hstring,"XPIXSIZE") != NULL ||
		    ksearch (hstring,"SECPIX1") != NULL) {
		    secpix = 0.0;
		    hgetr8 (hstring,"SECPIX",&secpix);
		    if (secpix == 0.0)
			hgetr8 (hstring,"PIXSCALE",&secpix);
		    if (secpix == 0.0) {
			hgetr8 (hstring,"SECPIX1",&secpix);
			if (secpix != 0.0) {
			    if (cdelt1 == 0.0)
				cdelt1 = -secpix / 3600.0;
			    if (cdelt2 == 0.0) {
				hgetr8 (hstring,"SECPIX2",&secpix);
				cdelt2 = secpix / 3600.0;
				}
			    }
			else {
			    hgetr8 (hstring,"XPIXSIZE",&secpix);
			    if (secpix != 0.0) {
				if (cdelt1 == 0.0)
				    cdelt1 = -secpix / 3600.0;
				if (cdelt2 == 0.0) {
				    hgetr8 (hstring,"YPIXSIZE",&secpix);
				    cdelt2 = secpix / 3600.0;
				    }
				}
			    else {
				hgetr8 (hstring,"PIXSCAL1",&secpix);
				if (secpix != 0.0 && cdelt1 == 0.0)
				    cdelt1 = -secpix / 3600.0;
				if (cdelt2 == 0.0) {
				    hgetr8 (hstring,"PIXSCAL2",&secpix);
				    cdelt2 = secpix / 3600.0;
				    }
				}
			    }
			}
		    else {
			if (cdelt1 == 0.0)
			    cdelt1 = -secpix / 3600.0;
			if (cdelt2 == 0.0)
			    cdelt2 = secpix / 3600.0;
			}
		    }
		}
	    if (cdelt2 == 0.0 && wcs->nypix > 1)
		cdelt2 = -cdelt1;
	    wcs->cdelt[2] = 1.0;
	    wcs->cdelt[3] = 1.0;

	    /* Initialize rotation matrix */
	    for (i = 0; i < 81; i++) {
		pc[i] = 0.0;
		wcs->pc[i] = 0.0;
		}
	    for (i = 0; i < naxes; i++)
		pc[(i*naxes)+i] = 1.0;

	    /* Read FITS WCS interim rotation matrix */
	    if (!mchar && hgetr8 (hstring,"PC001001",&pc[0]) != 0) {
		k = 0;
		for (i = 0; i < naxes; i++) {
		    for (j = 0; j < naxes; j++) {
			if (i == j)
			    pc[k] = 1.0;
			else
			    pc[k] = 0.0;
			sprintf (keyword, "PC00%1d00%1d", i+1, j+1);
			hgetr8 (hstring, keyword, &pc[k++]);
			}
		    }
		wcspcset (wcs, cdelt1, cdelt2, pc);
		}

	    /* Read FITS WCS standard rotation matrix */
	    else if (hgetr8c (hstring, "PC1_1", &mchar, &pc[0]) != 0) {
		k = 0;
		for (i = 0; i < naxes; i++) {
		    for (j = 0; j < naxes; j++) {
			if (i == j)
			    pc[k] = 1.0;
			else
			    pc[k] = 0.0;
			sprintf (keyword, "PC%1d_%1d", i+1, j+1);
			hgetr8c (hstring, keyword, &mchar, &pc[k++]);
			}
		    }
		wcspcset (wcs, cdelt1, cdelt2, pc);
		}

	    /* Otherwise, use CROTAn */
	    else {
		rot = 0.0;
		if (ilat == 2)
		    hgetr8c (hstring, "CROTA2", &mchar, &rot);
		else
		    hgetr8c (hstring,"CROTA1", &mchar, &rot);
		wcsdeltset (wcs, cdelt1, cdelt2, rot);
		}
	    }

	/* If no scaling is present, set to 1 per pixel, no rotation */
	else {
	    wcs->xinc = 1.0;
	    wcs->yinc = 1.0;
	    wcs->cdelt[0] = 1.0;
	    wcs->cdelt[1] = 1.0;
	    wcs->rot = 0.0;
	    wcs->rotmat = 0;
	    setwcserr ("WCSINIT: setting CDELT to 1");
	    }

	/* If linear or pixel WCS, print "degrees" */
	if (!strncmp (wcs->ptype,"LINEAR",6) ||
	    !strncmp (wcs->ptype,"PIXEL",5)) {
	    wcs->degout = -1;
	    wcs->ndec = 5;
	    }

	/* Epoch of image (from observation date, if possible) */
	if (hgetr8 (hstring, "MJD-OBS", &mjd))
	    wcs->epoch = 1900.0 + (mjd - 15019.81352) / 365.242198781;
	else if (!hgetdate (hstring,"DATE-OBS",&wcs->epoch)) {
	    if (!hgetdate (hstring,"DATE",&wcs->epoch)) {
		if (!hgetr8 (hstring,"EPOCH",&wcs->epoch))
		    wcs->epoch = wcs->equinox;
		}
	    }

	/* Add time of day if not part of DATE-OBS string */
	else {
	    hgets (hstring,"DATE-OBS",32,tstring);
	    if (!strchr (tstring,'T')) {
		if (hgetr8 (hstring, "UT",&ut))
		    wcs->epoch = wcs->epoch + (ut / (24.0 * 365.242198781));
		else if (hgetr8 (hstring, "UTMID",&ut))
		    wcs->epoch = wcs->epoch + (ut / (24.0 * 365.242198781));
		}
	    }

	wcs->wcson = 1;
	}

    else if (mchar != cnull && mchar != cspace) {
	(void) sprintf (temp, "WCSINITC: No image scale for WCS %c", mchar);
	setwcserr (temp);
	wcsfree (wcs);
	return (NULL);
	}

    /* Plate solution coefficients */
    else if (ksearch (hstring,"PLTRAH") != NULL) {
	wcs->prjcode = WCS_DSS;
	hcoeff = ksearch (hstring,"PLTRAH");
	hgetr8 (hcoeff,"PLTRAH",&rah);
	hgetr8 (hcoeff,"PLTRAM",&ram);
	hgetr8 (hcoeff,"PLTRAS",&ras);
	ra_hours = rah + (ram / (double)60.0) + (ras / (double)3600.0);
	wcs->plate_ra = hrrad (ra_hours);
	decsign = '+';
	hgets (hcoeff,"PLTDECSN", 1, &decsign);
	if (decsign == '-')
	    dsign = -1.;
	else
	    dsign = 1.;
	hgetr8 (hcoeff,"PLTDECD",&decd);
	hgetr8 (hcoeff,"PLTDECM",&decm);
	hgetr8 (hcoeff,"PLTDECS",&decs);
	dec_deg = dsign * (decd+(decm/(double)60.0)+(decs/(double)3600.0));
	wcs->plate_dec = degrad (dec_deg);
	hgetr8 (hstring,"EQUINOX",&wcs->equinox);
	hgeti4 (hstring,"EQUINOX",&ieq);
	if (ieq == 1950)
	    strcpy (wcs->radecsys,"FK4");
	else
	    strcpy (wcs->radecsys,"FK5");
	wcs->epoch = wcs->equinox;
	hgetr8 (hstring,"EPOCH",&wcs->epoch);
	(void)sprintf (wcs->center,"%2.0f:%2.0f:%5.3f %c%2.0f:%2.0f:%5.3f %s",
		       rah,ram,ras,decsign,decd,decm,decs,wcs->radecsys);
	hgetr8 (hstring,"PLTSCALE",&wcs->plate_scale);
	hgetr8 (hstring,"XPIXELSZ",&wcs->x_pixel_size);
	hgetr8 (hstring,"YPIXELSZ",&wcs->y_pixel_size);
	hgetr8 (hstring,"CNPIX1",&wcs->x_pixel_offset);
	hgetr8 (hstring,"CNPIX2",&wcs->y_pixel_offset);
	hcoeff = ksearch (hstring,"PPO1");
	for (i = 0; i < 6; i++) {
	    sprintf (keyword,"PPO%d", i+1);
	    wcs->ppo_coeff[i] = 0.0;
	    hgetr8 (hcoeff,keyword,&wcs->ppo_coeff[i]);
	    }
	hcoeff = ksearch (hstring,"AMDX1");
	for (i = 0; i < 20; i++) {
	    sprintf (keyword,"AMDX%d", i+1);
	    wcs->x_coeff[i] = 0.0;
	    hgetr8 (hcoeff, keyword, &wcs->x_coeff[i]);
	    }
	hcoeff = ksearch (hstring,"AMDY1");
	for (i = 0; i < 20; i++) {
	    sprintf (keyword,"AMDY%d",i+1);
	    wcs->y_coeff[i] = 0.0;
	    hgetr8 (hcoeff, keyword, &wcs->y_coeff[i]);
	    }
	wcs->wcson = 1;
	(void)strcpy (wcs->c1type, "RA");
	(void)strcpy (wcs->c2type, "DEC");
	(void)strcpy (wcs->ptype, "DSS");
	wcs->degout = 0;
	wcs->ndec = 3;

	/* Compute a nominal reference pixel at the image center */
	strcpy (wcs->ctype[0], "RA---DSS");
	strcpy (wcs->ctype[1], "DEC--DSS");
	wcs->crpix[0] = 0.5 * wcs->nxpix;
	wcs->crpix[1] = 0.5 * wcs->nypix;
	wcs->xrefpix = wcs->crpix[0];
	wcs->yrefpix = wcs->crpix[1];
	dsspos (wcs->crpix[0], wcs->crpix[1], wcs, &ra0, &dec0);
	wcs->crval[0] = ra0;
	wcs->crval[1] = dec0;
	wcs->xref = wcs->crval[0];
	wcs->yref = wcs->crval[1];

	/* Compute a nominal scale factor */
	dsspos (wcs->crpix[0], wcs->crpix[1]+1.0, wcs, &ra1, &dec1);
	wcs->yinc = dec1 - dec0;
	wcs->xinc = -wcs->yinc;
	wcsioset (wcs);

	/* Compute image rotation angle */
	wcs->wcson = 1;
	wcsrotset (wcs);
	rot = degrad (wcs->rot);

	/* Compute image scale at center */
	dsspos (wcs->crpix[0]+cos(rot),
		wcs->crpix[1]+sin(rot), wcs, &ra1, &dec1);
	wcs->cdelt[0] = -wcsdist (ra0, dec0, ra1, dec1);
	dsspos (wcs->crpix[0]+sin(rot),
		wcs->crpix[1]+cos(rot), wcs, &ra1, &dec1);
	wcs->cdelt[1] = wcsdist (ra0, dec0, ra1, dec1);

	/* Set all other image scale parameters */
	wcsdeltset (wcs, wcs->cdelt[0], wcs->cdelt[1], wcs->rot);
	}

    /* Approximate world coordinate system if plate scale is known */
    else if ((ksearch (hstring,"SECPIX") != NULL ||
	     ksearch (hstring,"PIXSCALE") != NULL ||
	     ksearch (hstring,"PIXSCAL1") != NULL ||
	     ksearch (hstring,"XPIXSIZE") != NULL ||
	     ksearch (hstring,"SECPIX1") != NULL)) {
	secpix = 0.0;
	hgetr8 (hstring,"SECPIX",&secpix);
	if (secpix == 0.0)
	    hgetr8 (hstring,"PIXSCALE",&secpix);
	if (secpix == 0.0) {
	    hgetr8 (hstring,"SECPIX1",&secpix);
	    if (secpix != 0.0) {
		cdelt1 = -secpix / 3600.0;
		hgetr8 (hstring,"SECPIX2",&secpix);
		cdelt2 = secpix / 3600.0;
		}
	    else {
		hgetr8 (hstring,"XPIXSIZE",&secpix);
		if (secpix != 0.0) {
		    cdelt1 = -secpix / 3600.0;
		    hgetr8 (hstring,"YPIXSIZE",&secpix);
		    cdelt2 = secpix / 3600.0;
		    }
		else {
		    hgetr8 (hstring,"PIXSCAL1",&secpix);
		    cdelt1 = -secpix / 3600.0;
		    hgetr8 (hstring,"PIXSCAL2",&secpix);
		    cdelt2 = secpix / 3600.0;
		    }
		}
	    }
	else {
	    cdelt2 = secpix / 3600.0;
	    cdelt1 = -cdelt2;
	    }

	/* Get rotation angle from the header, if it's there */
	rot = 0.0;
	hgetr8 (hstring,"CROTA1", &rot);
	if (wcs->rot == 0.)
	    hgetr8 (hstring,"CROTA2", &rot);

	/* Set CD and PC matrices */
	wcsdeltset (wcs, cdelt1, cdelt2, rot);

	/* By default, set reference pixel to center of image */
	wcs->crpix[0] = 0.5 + (wcs->nxpix * 0.5);
	wcs->crpix[1] = 0.5 + (wcs->nypix * 0.5);

	/* Get reference pixel from the header, if it's there */
	if (ksearch (hstring,"CRPIX1") != NULL) {
	    hgetr8 (hstring,"CRPIX1",&wcs->crpix[0]);
	    hgetr8 (hstring,"CRPIX2",&wcs->crpix[1]);
	    }

	/* Use center of detector array as reference pixel
	else if (ksearch (hstring,"DETSIZE") != NULL ||
		 ksearch (hstring,"DETSEC") != NULL) {
	    char *ic;
	    hgets (hstring, "DETSIZE", 32, temp);
	    ic = strchr (temp, ':');
	    if (ic != NULL)
		*ic = ' ';
	    ic = strchr (temp, ',');
	    if (ic != NULL)
		*ic = ' ';
	    ic = strchr (temp, ':');
	    if (ic != NULL)
		*ic = ' ';
	    ic = strchr (temp, ']');
	    if (ic != NULL)
		*ic = cnull;
	    sscanf (temp, "%d %d %d %d", &idx1, &idx2, &idy1, &idy2);
	    dxrefpix = 0.5 * (double) (idx1 + idx2 - 1);
	    dyrefpix = 0.5 * (double) (idy1 + idy2 - 1);
	    hgets (hstring, "DETSEC", 32, temp);
	    ic = strchr (temp, ':');
	    if (ic != NULL)
		*ic = ' ';
	    ic = strchr (temp, ',');
	    if (ic != NULL)
		*ic = ' ';
	    ic = strchr (temp, ':');
	    if (ic != NULL)
		*ic = ' ';
	    ic = strchr (temp, ']');
	    if (ic != NULL)
		*ic = cnull;
	    sscanf (temp, "%d %d %d %d", &ix1, &ix2, &iy1, &iy2);
	    wcs->crpix[0] = dxrefpix - (double) (ix1 - 1);
	    wcs->crpix[1] = dyrefpix - (double) (iy1 - 1);
	    } */
	wcs->xrefpix = wcs->crpix[0];
	wcs->yrefpix = wcs->crpix[1];

	wcs->crval[0] = -999.0;
	if (!hgetra (hstring,"RA",&wcs->crval[0])) {
	    setwcserr ("WCSINIT: No RA with SECPIX, no WCS");
	    wcsfree (wcs);
	    return (NULL);
	    }
	wcs->crval[1] = -999.0;
	if (!hgetdec (hstring,"DEC",&wcs->crval[1])) {
	    setwcserr ("WCSINIT No DEC with SECPIX, no WCS");
	    wcsfree (wcs);
	    return (NULL);
	    }
	wcs->xref = wcs->crval[0];
	wcs->yref = wcs->crval[1];
	wcs->coorflip = 0;

	wcs->cel.ref[0] = wcs->crval[0];
	wcs->cel.ref[1] = wcs->crval[1];
	wcs->cel.ref[2] = 999.0;
	if (!hgetr8 (hstring,"LONPOLE",&wcs->cel.ref[2]))
	    hgetr8 (hstring,"LONGPOLE",&wcs->cel.ref[2]);
	wcs->cel.ref[3] = 999.0;
	hgetr8 (hstring,"LATPOLE",&wcs->cel.ref[3]);

	/* Epoch of image (from observation date, if possible) */
	if (hgetr8 (hstring, "MJD-OBS", &mjd))
	    wcs->epoch = 1900.0 + (mjd - 15019.81352) / 365.242198781;
	else if (!hgetdate (hstring,"DATE-OBS",&wcs->epoch)) {
	    if (!hgetdate (hstring,"DATE",&wcs->epoch)) {
		if (!hgetr8 (hstring,"EPOCH",&wcs->epoch))
		    wcs->epoch = wcs->equinox;
		}
	    }

	/* Add time of day if not part of DATE-OBS string */
	else {
	    hgets (hstring,"DATE-OBS",32,tstring);
	    if (!strchr (tstring,'T')) {
		if (hgetr8 (hstring, "UT",&ut))
		    wcs->epoch = wcs->epoch + (ut / (24.0 * 365.242198781));
		else if (hgetr8 (hstring, "UTMID",&ut))
		    wcs->epoch = wcs->epoch + (ut / (24.0 * 365.242198781));
		}
	    }

	/* Coordinate reference frame and equinox */
	(void) wcstype (wcs, "RA---TAN", "DEC--TAN");
	wcs->coorflip = 0;
	wcseq (hstring,wcs);
	wcsioset (wcs);
	wcs->degout = 0;
	wcs->ndec = 3;
	wcs->wcson = 1;
	}

    else {
	setwcserr ("WCSINIT: No image scale");
	wcsfree (wcs);
	return (NULL);
	}

    wcs->lin.crpix = wcs->crpix;
    wcs->lin.cdelt = wcs->cdelt;
    wcs->lin.pc = wcs->pc;

    wcs->printsys = 1;
    wcs->tabsys = 0;
    wcs->linmode = 0;

    /* Initialize special WCS commands */
    setwcscom (wcs);

    return (wcs);
}


/* Set coordinate system of image, input, and output */

static void
wcsioset (wcs)

struct WorldCoor *wcs;
{
    if (strlen (wcs->radecsys) == 0 || wcs->prjcode == WCS_LIN)
	strcpy (wcs->radecsys, "LINEAR");
    if (wcs->prjcode == WCS_PIX)
	strcpy (wcs->radecsys, "PIXEL");
    wcs->syswcs = wcscsys (wcs->radecsys);

    if (wcs->syswcs == WCS_B1950)
	strcpy (wcs->radecout, "FK4");
    else if (wcs->syswcs == WCS_J2000)
	strcpy (wcs->radecout, "FK5");
    else
	strcpy (wcs->radecout, wcs->radecsys);
    wcs->sysout = wcscsys (wcs->radecout);
    wcs->eqout = wcs->equinox;
    strcpy (wcs->radecin, wcs->radecsys);
    wcs->sysin = wcscsys (wcs->radecin);
    wcs->eqin = wcs->equinox;
    return;
}


static void
wcseq (hstring, wcs)

char	*hstring;	/* character string containing FITS header information
		   	in the format <keyword>= <value> [/ <comment>] */
struct WorldCoor *wcs;	/* World coordinate system data structure */
{
    char mchar;		/* Suffix character for one of multiple WCS */
    mchar = (char) 0;
    wcseqm (hstring, wcs, &mchar);
    return;
}


static void
wcseqm (hstring, wcs, mchar)

char	*hstring;	/* character string containing FITS header information
		   	in the format <keyword>= <value> [/ <comment>] */
struct WorldCoor *wcs;	/* World coordinate system data structure */
char	*mchar;		/* Suffix character for one of multiple WCS */
{
    int ieq = 0;
    int eqhead = 0;
    char systring[32], eqstring[32];
    char radeckey[16], eqkey[16];
    char tstring[32];
    double ut;

    /* Set equinox from EQUINOX, EPOCH, or RADECSYS; default to 2000 */
    systring[0] = 0;
    eqstring[0] = 0;
    if (mchar[0]) {
	sprintf (eqkey, "EQUINOX%c", mchar[0]);
	sprintf (radeckey, "RADECSYS%c", mchar[0]);
	}
    else {
	strcpy (eqkey, "EQUINOX");
	sprintf (radeckey, "RADECSYS");
	}
    if (!hgets (hstring, eqkey, 31, eqstring)) {
	if (hgets (hstring, "EQUINOX", 31, eqstring))
	    strcpy (eqkey, "EQUINOX");
	}
    if (!hgets (hstring, radeckey, 31, systring)) {
	if (hgets (hstring, "RADECSYS", 31, systring))
	    sprintf (radeckey, "RADECSYS");
	}

    if (eqstring[0] == 'J') {
	wcs->equinox = atof (eqstring+1);
	ieq = atoi (eqstring+1);
	strcpy (systring, "FK5");
	}
    else if (eqstring[0] == 'B') {
	wcs->equinox = atof (eqstring+1);
	ieq = (int) atof (eqstring+1);
	strcpy (systring, "FK4");
	}
    else if (hgeti4 (hstring, eqkey, &ieq)) {
	hgetr8 (hstring, eqkey, &wcs->equinox);
	eqhead = 1;
	}

    else if (hgeti4 (hstring,"EPOCH",&ieq)) {
	if (ieq == 0) {
	    ieq = 1950;
	    wcs->equinox = 1950.0;
	    }
	else {
            hgetr8 (hstring,"EPOCH",&wcs->equinox);
	    eqhead = 1;
	    }
	}

    else if (systring[0] != (char)0) {
	if (!strncmp (systring,"FK4",3)) {
	    wcs->equinox = 1950.0;
	    ieq = 1950;
	    }
	else if (!strncmp (systring,"ICRS",4)) {
	    wcs->equinox = 2000.0;
	    ieq = 2000;
	    }
	else if (!strncmp (systring,"FK5",3)) {
	    wcs->equinox = 2000.0;
	    ieq = 2000;
	    }
	else if (!strncmp (systring,"GAL",3)) {
	    wcs->equinox = 2000.0;
	    ieq = 2000;
	    }
	else if (!strncmp (systring,"ECL",3)) {
	    wcs->equinox = 2000.0;
	    ieq = 2000;
	    }
	}

    if (ieq == 0) {
	wcs->equinox = 2000.0;
	ieq = 2000;
	if (!strncmp (wcs->c1type, "RA",2) || !strncmp (wcs->c1type,"DEC",3))
	    strcpy (systring,"FK5");
	}

    /* Epoch of image (from observation date, if possible) */
    if (!hgetdate (hstring,"DATE-OBS",&wcs->epoch)) {
	if (!hgetdate (hstring,"DATE",&wcs->epoch)) {
	    if (!hgetr8 (hstring,"EPOCH",&wcs->epoch))
		wcs->epoch = wcs->equinox;
	    }
	}

	/* Add time of day if not part of DATE-OBS string */
    else {
	hgets (hstring,"DATE-OBS",32,tstring);
	if (!strchr (tstring,'T')) {
	    if (hgetr8 (hstring, "UT",&ut))
		wcs->epoch = wcs->epoch + (ut / (24.0 * 365.242198781));
	    else if (hgetr8 (hstring, "UTMID",&ut))
		wcs->epoch = wcs->epoch + (ut / (24.0 * 365.242198781));
	    }
	}
    if (wcs->epoch == 0.0)
	wcs->epoch = wcs->equinox;

    /* Set coordinate system from keyword, if it is present */
    if (systring[0] == (char) 0)
	 hgets (hstring, radeckey, 31, systring);
    if (systring[0] != (char) 0) {
	strcpy (wcs->radecsys,systring);
	if (!eqhead) {
	    if (!strncmp (wcs->radecsys,"FK4",3))
		wcs->equinox = 1950.0;
	    else if (!strncmp (wcs->radecsys,"FK5",3))
		wcs->equinox = 2000.0;
	    else if (!strncmp (wcs->radecsys,"ICRS",4))
		wcs->equinox = 2000.0;
	    else if (!strncmp (wcs->radecsys,"GAL",3) && ieq == 0)
		wcs->equinox = 2000.0;
	    }
	}

    /* Otherwise set coordinate system from equinox */
    /* Systemless coordinates cannot be translated using b, j, or g commands */
    else if (wcs->syswcs != WCS_NPOLE) {
	if (ieq > 1980)
	    strcpy (wcs->radecsys,"FK5");
	else
	    strcpy (wcs->radecsys,"FK4");
	}

    /* Set galactic coordinates if GLON or GLAT are in C1TYPE */
    if (wcs->c1type[0] == 'G')
	strcpy (wcs->radecsys,"GALACTIC");
    else if (wcs->c1type[0] == 'E')
	strcpy (wcs->radecsys,"ECLIPTIC");
    else if (wcs->c1type[0] == 'S')
	strcpy (wcs->radecsys,"SGALACTC");
    else if (wcs->c1type[0] == 'H')
	strcpy (wcs->radecsys,"HELIOECL");
    else if (wcs->c1type[0] == 'A')
	strcpy (wcs->radecsys,"ALTAZ");
    else if (wcs->c1type[0] == 'L')
	strcpy (wcs->radecsys,"LINEAR");

    wcs->syswcs = wcscsys (wcs->radecsys);

    return;
}

/* Jun 11 1998	Split off header-dependent WCS initialization from other subs
 * Jun 15 1998	Fix major bug in wcsinit() when synthesizing WCS from header
 * Jun 18 1998	Fix bug in CD initialization; split PC initialization off
 * Jun 18 1998	Split PC initialization off into subroutine wcspcset()
 * Jun 24 1998	Set equinox from RADECSYS only if EQUINOX and EPOCH not present
 * Jul  6 1998  Read third and fourth axis CTYPEs
 * Jul  7 1998  Initialize eqin and eqout to equinox,
 * Jul  9 1998	Initialize rotation matrices correctly
 * Jul 13 1998	Initialize rotation, scale for polynomial and DSS projections
 * Aug  6 1998	Fix CROTA computation for DSS projection
 * Sep  4 1998	Fix CROTA, CDELT computation for DSS and polynomial projections
 * Sep 14 1998	If DATE-OBS not found, check for DATE
 * Sep 14 1998	If B or J present in EQUINOX, use that info to set system
 * Sep 29 1998  Initialize additional WCS commands from the environment
 * Sep 29 1998	Fix bug which read DATE as number rather than formatted date
 * Dec  2 1998	Read projection constants from header (bug fix)
 *
 * Feb  9 1999	Set rotation angle correctly when using DSS projection
 * Feb 19 1999	Fill in CDELTs from scale keyword if absent or zero
 * Feb 19 1999	Add PIXSCALE as possible default arcseconds per pixel
 * Apr  7 1999	Add error checking for NAXIS and NAXIS1 keywords
 * Apr  7 1999	Do not set systring if epoch is 0 and not RA/Dec
 * Jul  8 1999	In RADECSYS, use FK5 and FK4 instead of J2000 and B1950
 * Oct 15 1999	Free wcs using wcsfree()
 * Oct 20 1999	Add multiple WCS support using new subroutine names
 * Oct 21 1999	Delete unused variables after lint; declare dsspos()
 * Nov  9 1999	Add wcschar() to check WCSNAME keywords for desired WCS
 * Nov  9 1999	Check WCSPREx keyword to find out if chained WCS's
 *
 * Jan  6 1999	Add wcsinitn() to initialize from specific WCSNAME
 * Jan 24 2000  Set CD matrix from header even if using polynomial
 * Jan 27 2000  Fix MJD to epoch conversion for when MJD-OBS is the only date
 * Jan 28 2000  Set CD matrix for DSS projection, too
 * Jan 28 2000	Use wcsproj instead of oldwcs
 * Dec 18 2000	Fix error in hgets() call in wcschar()
 * Dec 29 2000  Compute inverse CD matrix even if polynomial solution
 * Dec 29 2000  Add PROJR0 keyword for WCSLIB projections
 * Dec 29 2000  Use CDi_j matrix if any elements are present
 *
 * Jan 31 2001	Fix to allow 1D WCS
 * Jan 31 2001	Treat single character WCS name as WCS character
 * Feb 20 2001	Implement WCSDEPx nested WCS's
 * Feb 23 2001	Initialize all 4 terms of CD matrix
 * Feb 28 2001	Fix bug which read CRPIX1 into CRPIX2
 * Mar 20 2001	Compare mchar to (char)0, not null
 * Mar 21 2001	Move ic declaration into commented out code
 * Jul 12 2001	Read PROJPn constants into proj.p array instead of PVn
 * Sep  7 2001	Set system to galactic or ecliptic based on CTYPE, not RADECSYS
 * Oct 11 2001	Set ctype[0] as well as ctype[1] to TAN for TNX projections
 * Oct 19 2001	WCSDIM keyword overrides zero value of NAXIS
 *
 * Feb 19 2002	Add XPIXSIZE/YPIXSIZE (KPNO) as default image scale keywords
 * Mar 12 2002	Add LONPOLE as well as LONGPOLE for WCSLIB 2.8
 * Apr  3 2002	Implement hget8c() and hgetsc() to simplify code
 * Apr  3 2002	Add PVj_n projection constants in addition to PROJPn
 * Apr 19 2002	Increase numeric keyword value length from 16 to 31
 * Apr 19 2002	Fix bug which didn't set radecsys keyword name
 * Apr 24 2002	If no WCS present for specified letter, return null
 * Apr 26 2002	Implement WCSAXESa keyword as first choice for number of axes
 * Apr 26 2002	Add wcschar and wcsname to WCS structure
 * May  9 2002	Add radvel and zvel to WCS structure
 * May 13 2002	Free everything which is allocated
 * May 28 2002	Read 10 prj.p instead of maximum of 100
 * May 31 2002	Fix bugs with PV reading
 * May 31 2002	Initialize syswcs, sysin, sysout in wcsioset()
 * Sep 25 2002	Fix subroutine calls for radvel and latpole
 * Dec  6 2002	Correctly compute pixel at center of image for default CRPIX
 *
 * Jan  2 2002	Do not reinitialize projection vector for PV input
 * Jan  3 2002	For ZPN, read PVi_0 to PVi_9, not PVi_1 to PVi_10
 * Mar 27 2003	Clean up default center computation
 * Apr  3 2003	Add input for SIRTF distortion coefficients
 * May  8 2003	Change PROJP reading to start with 0 instead of 1
 * May 22 2003	Add ZPX approximation, reading projpn from WATi
 * May 28 2003	Avoid reinitializing coefficients set by PROJP
 * Jun 26 2003	Initialize xref and yref to -999.0
 * Sep 23 2003	Change mgets() to mgetstr() to avoid name collision at UCO Lick
 * Oct  1 2003	Rename wcs->naxes to wcs->naxis to match WCSLIB 3.2
 * Nov  3 2003	Initialize distortion coefficients in distortinit() in distort.c
 * Dec  1 2003	Change p[0,1,2] initializations to p[1,2,3]
 * Dec  3 2003	Add back wcs->naxes for backward compatibility
 * Dec  3 2003	Remove unused variables j,m in wcsinitc()
 * Dec 12 2003	Fix call to setwcserr() with format in it
 *
 * Feb 26 2004	Add parameters for ZPX projection
 *
 * Jun 22 2005	Drop declaration of variable wcserrmsg which is not used
 * Nov  9 2005	Use CROTA1 if CTYPE1 is LAT/DEC, CROTA2 if CTYPE2 is LAT/DEC
 *
 * Mar  9 2006	Get Epoch of observation from MJD-OBS or DATE-OBS/UT unless DSS
 * Apr 24 2006	Initialize rotation matrices
 * Apr 25 2006	Ignore axes with dimension of one
 * May 19 2006	Initialize all of 9x9 PC matrix; read in loops
 * Aug 21 2006	Limit naxes to 2 everywhere; RA and DEC should always be 1st
 * Oct  6 2006	If units are pixels, projection type is PIXEL
 * Oct 30 2006	Initialize cube face to -1, not a cube projection
 *
 * Jan  4 2007	Drop declarations of wcsinitc() and wcsinitn() already in wcs.h
 * Jan  8 2007	Change WCS letter from char to char*
 * Feb  1 2007	Read IRAF log wavelength flag DC-FLAG to wcs.logwcs
 * Feb 15 2007	Check for wcs->wcsproj > 0 instead of CTYPEi != LINEAR or PIXEL
 * Mar 13 2007	Try for RA, DEC, SECPIX if WCS character is space or null
 * Apr 27 2007	Ignore axes with TAB WCS for now
 * Oct 17 2007	Fix bug testing &mchar instead of mchar in if statement
 *
 * May  9 2008	Initialize TNX projection when projection types first set
 * Jun 27 2008	If NAXIS1 and NAXIS2 not present, check for IMAGEW and IMAGEH
 *
 * Mar 24 2009	Fix dimension bug if NAXISi not present (fix from John Burns)
 */
