/*** File libwcs/wcscat.h
 *** January 10, 2007
 *** By Doug Mink, dmink@cfa.harvard.edu
 *** Copyright (C) 1998-2007
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

#ifndef _wcscat_h_
#define _wcscat_h_

/* Data structure for SAO TDC ASCII and binary star catalog entries */
struct Star {
    float rdum;
    float xno;		/* Catalog number */
    double ra;		/* Right Ascension (degrees) */
    double dec;		/* Declination (degrees) */
    char isp[24];	/* Spectral type or other 2-char identifier */
    short mag[11];	/* Up to 10 Magnitudes * 100 */
    double rapm;	/* RA proper motion (degrees per year) */
    double decpm;	/* Dec proper motion (degrees per year) */
    double xmag[11];	/* Up to 10 Magnitudes */
    double num;		/* Actual star number */
    int coorsys;	/* Coordinate system (WCS_J2000, WCS_B1950,...) */
    double equinox;	/* Equinox of coordinate system as fractional year */
    double epoch;	/* Epoch of position as fractional year */
    double parallax;	/* Parallax in arcseconds */
    double pxerror;	/* Parallax error in arcseconds */
    double radvel;	/* Radial velocity in km/sec, positive away */
    double dist;	/* Distance from search center in arcseconds */
    double size;	/* Semi-major axis in arcseconds */
    char *entry;	/* Line copied from input catalog */
    char objname[80];	/* Object name */
    int peak;		/* Peak flux per pixel in star image */
};

/* Catalog proper motion units */
#define PM_MASYR		1	/* milliarcseconds per year */
#define PM_ARCSECYR		2	/* arcseconds per year */
#define PM_DEGYR		3	/* degrees per year */
#define PM_RADYR		4	/* radians per year */
#define PM_TSECYR		5	/* seconds of time (RA) per century */
#define PM_ARCSECCEN		6	/* arcseconds per year */
#define PM_TSECCEN		7	/* seconds of time (RA) per century */
#define PM_MTSYR		8	/* milliseconds of time (RA) per year */

/* Data structure for SAO TDC ASCII and binary star catalogs */
struct StarCat {
    int star0;		/* Subtract from star number for file sequence number */
    int star1;		/* First star number in file */
    int nstars;		/* Number of stars in file */
    int stnum;		/* Star number format in catalog file:
			  <0: -stnum-character name at end instead of number
			   0:  no star i.d. numbers
			   1: Real*4 star i.d. numbers
			   2: Integer*4 <region><nnnn>
			   3: Integer*4 <region><nnnnn>
			   4: Integer*4 <nnnnnnnnn>
			   5: Character ID instead of number in ASCII files */
    int mprop;		/* 1 if proper motion is included */
			/* 2 if radial velocity is included */
    int nmag;		/* Number of magnitudes present
			   Negative for J2000 catalog */
    int nbent;		/* Number of bytes per star entry */
    int	rasorted;	/* 1 if RA-sorted, else 0 */
    int	ignore;		/* 1 if ignoring info after position and magnitude */
    FILE *ifcat;	/* File descriptor for catalog file */
    char isfil[24];	/* Star catalog file name */
    char isname[64];	/* Star catalog description */
    int  byteswapped;	/* 1 if catalog is byte-reversed from CPU */
    int  refcat;	/* Code for type of catalog (TXTCAT, BINCAT, etc.) */
    int  coorsys;	/* Coordinate system
			   B1950 J2000 Galactic Ecliptic */
    double epoch;	/* Epoch of catalog coordinates in years */
    double equinox;	/* Equinox of catalog coordinates in years */
    char inform;	/* Coordinate format
			   (B>inary D>egrees H>MS T>able U>SNO) */
    char incdir[128];	/* Catalog directory pathname */
    char incfile[32];	/* Catalog file name */
    int ncobj;		/* Length of object name in binary star entry */
    int nnfld;		/* Length of star number  */
    int nndec;		/* Number of decimal places in star number */
    int nepoch;		/* 1 if epoch of coordinates is present */
    int sptype;		/* 1 if spectral type is present in catalog */
    int plate;		/* 1 if plate or field number is present in catalog */
    char *catbuff;	/* Pointer to start of catalog */
    char *catdata;	/* Pointer to first entry in catalog */
    char *catline;	/* Pointer to current entry in catalog */
    char *catlast;	/* Pointer to one past end of last entry in catalog */
    int  istar;		/* Number of current catalog entry */
    struct TabTable *startab;	/* Structure for tab table catalog */
    int entid;		/* Entry number for ID */
    int entra;		/* Entry number for right ascension */
    int entdec;		/* Entry number for declination */
    int entmag[10];	/* Entry numbers for up to 10 magnitudes */
    int entpeak;	/* Entry number for peak counts */
    int entepoch;	/* Entry number for epoch of observation */
    int entdate;	/* Entry number for FITS-format date of observation */
    int entname;	/* Entry number for object name */
    int entadd;		/* Entry number for additional keyword */
    int entrpm;		/* Entry number for proper motion in right ascension */
    int entdpm;		/* Entry number for proper motion in declination */
    int entpx;		/* Entry number for parallax */
    int entpxe;		/* Entry number for parallax error */
    int entrv;		/* Entry number for radial velocity */
    int enttype;	/* Entry number for spectral type */
    int entsize;	/* Entry number for size of object */
    int rpmunit;	/* Units for RA proper motion (PM_x) */
    int dpmunit;	/* Units for DEC proper motion (PM_x) */
    char *caturl;	/* set if web search, else NULL */
    char keyid[16];	/* Entry name for ID */
    char keyra[16];	/* Entry name for right ascension */
    char keydec[16];	/* Entry name for declination */
    char keymag[10][16]; /* Entry name for up to 10 magnitudes */
    char keyrpm[16];	/* Entry name for right ascension proper motion */
    char keydpm[16];	/* Entry name for declination proper motion */
    char keypeak[16];	/* Entry name for integer code */
    char keytype[16];	/* Entry name for spectral type */
    char keyrv[16];	/* Entry name for radial velocity */
    char keyadd[16];	/* Entry name for additional keyword */
    char keyepoch[16];	/* Entry name for epoch */
};

/* Data structure for tab table files */
struct TabTable {
    char *filename;	/* Name of tab table file */
    int nlines;		/* Number of entries in table */
    char *tabname;	/* Name of this table or NULL */
    char *tabbuff;	/* Pointer to start of saved tab table in memory */
    char *tabheader;	/* Pointer to start of line containing table header */
    char *tabhead;	/* Pointer to start of line containing column heading */
    char *tabdash;	/* Pointer to start of line with dashes after column headings */
    char *tabdata;	/* Pointer to start of first line of table data */
    int lhead;		/* Number of bytes before first data line */
    int iline;		/* Number of current line (1=first) */
    int lline;		/* Length in bytes of line buffer */
    char *tabline;	/* Pointer to start of current line */
    FILE *tcat;		/* File descriptor for tab table file */
    int ncols;		/* Number of columns per table entry */
    char **colname;	/* Column names */
    int *lcol;		/* Lengths of column header names */
    int *lcfld;		/* Number of columns in field (hyphens) */
    int lbuff;		/* Number of bytes in entire tab table */
};

/* Source catalog flags and subroutines */

/* Source catalog flags returned from CatCode */
#define GSC		1	/* HST Guide Star Catalog */
#define UJC		2	/* USNO UJ Star Catalog */
#define UAC		3	/* USNO A Star Catalog */
#define USAC		4	/* USNO SA Star Catalog */
#define SAO		5	/* SAO Star Catalog */
#define IRAS		6	/* IRAS Point Source Catalog */
#define PPM		7	/* PPM Star Catalog */
#define TYCHO		8	/* Tycho Star Catalog */
#define UA1		9	/* USNO A-1.0 Star Catalog */
#define UA2		10	/* USNO A-2.0 Star Catalog */
#define USA1		11	/* USNO SA-1.0 Star Catalog */
#define USA2		12	/* USNO SA-2.0 Star Catalog */
#define HIP		13	/* Hipparcos Star Catalog */
#define ACT		14	/* USNO ACT Star Catalog */
#define BSC		15	/* Yale Bright Star Catalog */
#define TYCHO2		16	/* Tycho-2 Star Catalog */
#define USNO		17	/* USNO-format plate catalog */
#define TMPSC		18	/* 2MASS All-Sky Point Source Catalog */
#define GSCACT		19	/* GSC-ACT revised Guide Star Catalog */
#define GSC2		20	/* GSC II version 2.2 */
#define UB1		21	/* USNO B-1.0 Star Catalog */
#define UCAC1		22	/* USNO CCD Astrograph Catalog 1.0 */
#define UCAC2		23	/* USNO CCD Astrograph Catalog 2.0 */
#define TMIDR2		24	/* 2MASS IDR2 Point Source Catalog */
#define YB6		25	/* USNO YB6 Catalog */
#define SDSS		26	/* Sloan Digital Sky Survey Catalog */
#define TMXSC		27	/* 2MASS Extended Source Catalog */
#define TMPSCE		28	/* 2MASS Point Source Catalog with mag errors */
#define TYCHO2E		29	/* Tycho-2 Star Catalog with magnitude errors */
#define SKY2K		30	/* SKY2000 Master Catalog */
#define TABCAT		-1	/* StarBase tab table catalog */
#define BINCAT		-2	/* TDC binary catalog */
#define TXTCAT		-3	/* TDC ASCII catalog */
#define WEBCAT		-4	/* Tab catalog via the web */
#define NUMCAT		30	/* Number of predefined catalogs */

/* Structure for access to tokens within a string */
#define MAXTOKENS 1000    /* Maximum number of tokens to parse */
#define MAXWHITE 20     /* Maximum number of different whitespace characters */
struct Tokens {
    char *line;		/* Line which has been parsed */
    int lline;		/* Number of characters in line */
    int ntok;		/* Number of tokens on line */
    int nwhite;		/* Number of whitespace characters */
    char white[MAXWHITE]; /* Whitespace (separator) characters */
    char *tok1[MAXTOKENS]; /* Pointers to start of tokens */
    int ltok[MAXTOKENS]; /* Lengths of tokens */
    int itok;		/* Current token number */
};
#define EP_EP   1	/* Output epoch as fractional year */
#define EP_JD   2	/* Output epoch as Julian Date */
#define EP_MJD  3	/* Ouput epoch as Modified Julian Date */
#define EP_FD   4	/* Output epoch in FITS format (yyyy-mm-dd) */
#define EP_ISO  5	/* Output epoch in ISO format (yyyy-mm-ddThh:mm:ss) */

/* Structure for dealing with ranges */
#define MAXRANGE 20
struct Range {
    double first;	/* Current minimum value */
    double last;	/* Current maximum value */
    double step;	/* Current step in value */
    double value;	/* Current value */
    double ranges[MAXRANGE*3];	/* nranges sets of first, last, step */
    int nvalues;	/* Total number of values in all ranges */
    int nranges;	/* Number of ranges */
    int irange;		/* Index of current range */
};

/* Flags for sorting catalog search results */
#define SORT_UNSET	-1	/* Catalog sort flag not set yet */
#define SORT_NONE	0	/* Do not sort catalog output */
#define SORT_MAG	1	/* Sort output by magnitude */
#define SORT_DIST	2	/* Sort output by distance from center */
#define SORT_RA		3	/* Sort output by right ascension */
#define SORT_DEC	4	/* Sort output by declination */
#define SORT_X		5	/* Sort output by image X coordinate */
#define SORT_Y		6	/* Sort output by image Y coordinate */
#define SORT_ID		7	/* Merge close catalog objects */
#define SORT_MERGE	8	/* Merge close catalog objects */

/* Shapes for SAOimage region file output */
#define WCS_CIRCLE 1	/* circle shape for SAOimage plotting */
#define WCS_SQUARE 2	/* square shape for SAOimage plotting */
#define WCS_DIAMOND 3	/* diamond shape for SAOimage plotting */
#define WCS_CROSS 4	/* cross shape for SAOimage plotting */
#define WCS_EX 5	/* x shape for SAOimage plotting */
#define WCS_VAR 6	/* variable (+ and x) shape for HSTGSC plotting */
#define WCS_PCIRCLE 11	/* pixel circle shape for SAOimage plotting */
#define WCS_PSQUARE 12	/* pixel square shape for SAOimage plotting */
#define WCS_PDIAMOND 13	/* pixel diamond shape for SAOimage plotting */
#define WCS_PCROSS 14	/* pixel cross shape for SAOimage plotting */
#define WCS_PEX 15	/* pixel ex shape for SAOimage plotting */
#define WCS_PVAR 16	/* pixel variable (+ and x) shape for HSTGSC plotting */

/* Subroutines for extracting sources from catalogs */

#ifdef __cplusplus
extern "C" {
#endif

#ifdef __STDC__   /* Full ANSI prototypes */

/* Subroutines for reading any catalogs, including TDC ASCII catalogs */

    int ctgread(	/* Read sources by sky region from any catalog */
	char *catfile,	/* Name of reference star catalog file */
	int refcat,	/* Catalog code from wcscat.h */
	int distsort,	/* 1 to sort stars by distance from center */
	double cra,	/* Search center J2000 right ascension in degrees */
	double cdec,	/* Search center J2000 declination in degrees */
	double dra,	/* Search half width in right ascension in degrees */
	double ddec,	/* Search half-width in declination in degrees */
	double drad,	/* Limiting separation in degrees (ignore if 0) */
	double dradi,	/* Inner edge of annulus in degrees (ignore if 0) */
	int sysout,	/* Search coordinate system */
	double eqout,	/* Search coordinate equinox */
	double epout,	/* Proper motion epoch (0.0 for no proper motion) */
	double mag1,	/* Limiting magnitudes (none if equal) */
	double mag2,	/* Limiting magnitudes (none if equal) */
	int sortmag,	/* Number of magnitude by which to limit and sort */
	int nsmax,	/* Maximum number of stars to be returned */
	struct StarCat **starcat, /* Catalog data structure */
	double *tnum,	/* Array of ID numbers (returned) */
	double *tra,	/* Array of right ascensions (returned) */
	double *tdec,	/* Array of declinations (returned) */
	double *tpra,	/* Array of right ascension proper motions (returned) */
	double *tpdec,	/* Array of declination proper motions (returned) */
	double **tmag,	/* 2-D array of magnitudes (returned) */
	int *tc,	/* Array of fluxes (returned) */
	char **tobj,	/* Array of object names (returned) */
	int nlog);	/* Verbose mode if > 1, number of sources per log line */
    int ctgrnum(	/* Read sources by number from any catalog */
	char *catfile,	/* Name of reference star catalog file */
	int refcat,	/* Catalog code from wcscat.h */
	int nnum,	/* Number of stars to look for */
	int sysout,	/* Search coordinate system */
	double eqout,	/* Search coordinate equinox */
	double epout,	/* Proper motion epoch (0.0 for no proper motion) */
	struct StarCat **starcat, /* Star catalog data structure */
	int match,	/* 1 to match star number exactly, else sequence num */
	double *tnum,	/* Array of source numbers to look for */
	double *tra,	/* Array of right ascensions (returned) */
	double *tdec,	/* Array of declinations (returned) */
	double *tpra,	/* Array of right ascension proper motions (returned) */
	double *tpdec,	/* Array of declination proper motions (returned) */
	double **tmag,	/* 2-D Array of magnitudes (returned) */
	int *tpeak,	/* Array of peak counts (returned) */
	char **tkey,	/* Array of values of additional keyword */
	int nlog);	/* Verbose mode if > 1, number of sources per log line */
    int ctgrdate(	/* Read sources by date from SAO TDC ASCII format catalog */
	char *catfile,	/* Name of reference star catalog file */
	int refcat,	/* Catalog code from wcscat.h */
	int sysout,	/* Search coordinate system */
	double eqout,	/* Search coordinate equinox */
	double epout,	/* Proper motion epoch (0.0 for no proper motion) */
	struct StarCat **starcat, /* Star catalog data structure */
	double date1,	/* Start time as Modified Julian Date or Julian Date */
	double date2,	/* End time as Modified Julian Date or Julian Date */
	int nmax,	/* Maximum number of stars to look for */
	double *tnum,	/* Array of source numbers (returned) */
	double *tra,	/* Array of right ascensions (returned) */
	double *tdec,	/* Array of declinations (returned) */
	double *tpra,	/* Array of right ascension proper motions (returned) */
	double *tpdec,	/* Array of declination proper motions (returned) */
	double **tmag,	/* 2-D Array of magnitudes (returned) */
	int *tc,	/* Array of fluxes (returned) */
	char **tobj,	/* Array of object names (returned) */
	int nlog);	/* Verbose mode if > 1, number of sources per log line */
    int ctgbin(		/* Bin sources from SAO TDC ASCII format catalog */
	char *catfile,	/* Name of reference star catalog file */
	int refcat,	/* Catalog code from wcscat.h */
	struct WorldCoor *wcs, /* World coordinate system for image */
	char *header,	/* FITS header for output image */
	char *image,	/* Output FITS image */
	double mag1,	/* Minimum (brightest) magnitude (no limits if equal) */
	double mag2,	/* Maximum (faintest) magnitude (no limits if equal) */
	int sortmag,	/* Magnitude by which to sort (1 to nmag) */
	double magscale, /* Scaling factor for magnitude to pixel flux
			 * (image of number of catalog objects per bin if 0) */
	int nlog);	/* Verbose mode if > 1, number of sources per log line */
    int ctgstar(		/* Read one star entry from ASCII catalog, 0 if OK */
	int istar,	/* Star sequence number in ASCII catalog */
	struct StarCat *sc, /* Star catalog data structure */
	struct Star *st); /* Star data structure, updated on return */
    int isacat(		/* Return 1 if string is name of ASCII catalog file */
	char *catpath);	/* Path to file to check */
    struct StarCat *ctgopen( /* Open a Starbase, TDC ASCII, or TDC binary catalog */
	char *catfile,	/* Name of reference star catalog file */
	int refcat);	/* Catalog code from wcscat.h */
    void ctgclose(	/* Close Starbase, TDC ASCII, or TDC binary catalog
			 * and free data structures */
	struct StarCat *sc); /* Star catalog data structure */

/* Subroutines for extracting sources from HST Guide Star Catalog */
    int gscread(	/* Read sources by sky region from HST Guide Star Catalog */
	int refcat,	/* Catalog code from wcscat.h (GSC or GSCACT) */
	double cra,	/* Search center J2000 right ascension in degrees */
	double cdec,	/* Search center J2000 declination in degrees */
	double dra,	/* Search half width in right ascension in degrees */
	double ddec,	/* Search half-width in declination in degrees */
	double drad,	/* Limiting separation in degrees (ignore if 0) */
	double dradi,	/* Inner edge of annulus in degrees (ignore if 0) */
	int distsort,	/* 1 to sort stars by distance from center */
	int sysout,	/* Search coordinate system */
	double eqout,	/* Search coordinate equinox */
	double epout,	/* Proper motion epoch (0.0 for no proper motion) */
	double mag1,	/* Limiting magnitudes (none if equal) */
	double mag2,	/* Limiting magnitudes (none if equal) */
	int nstarmax,	/* Maximum number of stars to be returned */
	double *gnum,	/* Array of ID numbers (returned) */
	double *gra,	/* Array of right ascensions (returned) */
	double *gdec,	/* Array of declinations (returned) */
	double **gmag,	/* 2-D array of magnitudes (returned) */
	int *gtype,	/* Array of object types (returned) */
	int nlog);	/* Verbose mode if > 1, number of sources per log line */
    int gscrnum(	/* Read sources by ID number from HST Guide Star Catalog */
	int refcat,	/* Catalog code from wcscat.h (GSC or GSCACT) */
	int nnum,	/* Number of stars to look for */
	int sysout,	/* Search coordinate system */
	double eqout,	/* Search coordinate equinox */
	double epout,	/* Proper motion epoch (0.0 for no proper motion) */
	int match,	/* 1 to match star number exactly, else sequence num */
	double *gnum,	/* Array of source numbers to look for */
	double *gra,	/* Array of right ascensions (returned) */
	double *gdec,	/* Array of declinations (returned) */
	double **gmag,	/* 2-D array of magnitudes (returned) */
	int *gtype,	/* Array of object types (returned) */
	int nlog);	/* Verbose mode if > 1, number of sources per log line */
    int gscbin(		/* Bin sources from HST Guide Star Catalog */
	int refcat,	/* Catalog code from wcscat.h (GSC or GSCACT) */
	struct WorldCoor *wcs, /* World coordinate system for image */
	char *header,	/* FITS header for output image */
	char *image,	/* Output FITS image */
	double mag1,	/* Minimum (brightest) magnitude (no limits if equal) */
	double mag2,	/* Maximum (faintest) magnitude (no limits if equal) */
	double magscale, /* Scaling factor for magnitude to pixel flux
			 * (image of number of catalog objects per bin if 0) */
	int nlog);	/* Verbose mode if > 1, number of sources per log line */
    void setgsclass(	/* Set GSC object class to return (<0=all) */
	int class);	/* Class of objects to return */

/* Subroutine to read GSC II catalog over the web */
    int gsc2read(	/* Read sources by sky region from GSC II Catalog */
	char *refcatname, /* Name of catalog (GSC2 for 2.2; GSC2.3 for 2.3) */
	double cra,	/* Search center J2000 right ascension in degrees */
	double cdec,	/* Search center J2000 declination in degrees */
	double dra,	/* Search half width in right ascension in degrees */
	double ddec,	/* Search half-width in declination in degrees */
	double drad,	/* Limiting separation in degrees (ignore if 0) */
	double dradi,	/* Inner edge of annulus in degrees (ignore if 0) */
	int distsort,	/* 1 to sort stars by distance from center */
	int sysout,	/* Search coordinate system */
	double eqout,	/* Search coordinate equinox */
	double epout,	/* Proper motion epoch (0.0 for no proper motion) */
	double mag1,	/* Limiting magnitudes (none if equal) */
	double mag2,	/* Limiting magnitudes (none if equal) */
	int sortmag,	/* Number of magnitude by which to limit and sort */
	int nstarmax,	/* Maximum number of stars to be returned */
	double *gnum,	/* Array of ID numbers (returned) */
	double *gra,	/* Array of right ascensions (returned) */
	double *gdec,	/* Array of declinations (returned) */
	double *gpra,	/* Array of right ascension proper motions (returned) */
	double *gpdec,	/* Array of declination proper motions (returned) */
	double **gmag,	/* 2-D array of magnitudes (returned) */
	int *gtype,	/* Array of object types (returned) */
	int nlog);	/* Verbose mode if > 1, number of sources per log line */

/* Subroutine to read SDSS catalog over the web */
    int sdssread(	/* Read sources by sky region from SDSS Catalog */
	double cra,	/* Search center J2000 right ascension in degrees */
	double cdec,	/* Search center J2000 declination in degrees */
	double dra,	/* Search half width in right ascension in degrees */
	double ddec,	/* Search half-width in declination in degrees */
	double drad,	/* Limiting separation in degrees (ignore if 0) */
	double dradi,	/* Inner edge of annulus in degrees (ignore if 0) */
	int distsort,	/* 1 to sort stars by distance from center */
	int sysout,	/* Search coordinate system */
	double eqout,	/* Search coordinate equinox */
	double epout,	/* Proper motion epoch (0.0 for no proper motion) */
	double mag1,	/* Limiting magnitudes (none if equal) */
	double mag2,	/* Limiting magnitudes (none if equal) */
	int sortmag,	/* Number of magnitude by which to limit and sort */
	int nstarmax,	/* Maximum number of stars to be returned */
	char **gobj,	/* Array of object IDs (too long for integer*4) */
	double *gra,	/* Array of right ascensions (returned) */
	double *gdec,	/* Array of declinations (returned) */
	double **gmag,	/* 2-D array of magnitudes (returned) */
	int *gtype,	/* Array of object types (returned) */
	int nlog);	/* Verbose mode if > 1, number of sources per log line */
    char *sdssc2t(	/* Convert SDSS buffer from comma- to tab-separated */
	char *csvbuff);	/* Input comma-separated table */

/* Subroutines to read local copy of 2MASS Point Source Catalog */
    int tmcread(	/* Read sources by sky region from 2MASS Point Source Catalog */
	int refcat,	/* Catalog code from wcscat.h (TMPSC or TMXSC or TMPSCE) */
	double cra,	/* Search center J2000 right ascension in degrees */
	double cdec,	/* Search center J2000 declination in degrees */
	double dra,	/* Search half width in right ascension in degrees */
	double ddec,	/* Search half-width in declination in degrees */
	double drad,	/* Limiting separation in degrees (ignore if 0) */
	double dradi,	/* Inner edge of annulus in degrees (ignore if 0) */
	int distsort,	/* 1 to sort stars by distance from center */
	int sysout,	/* Search coordinate system */
	double eqout,	/* Search coordinate equinox */
	double epout,	/* Proper motion epoch (0.0 for no proper motion) */
	double mag1,	/* Limiting magnitudes (none if equal) */
	double mag2,	/* Limiting magnitudes (none if equal) */
	int sortmag,	/* Number of magnitude by which to limit and sort */
	int nstarmax,	/* Maximum number of stars to be returned */
	double *gnum,	/* Array of catalog numbers (returned) */
	double *gra,	/* Array of right ascensions (returned) */
	double *gdec,	/* Array of declinations (returned) */
	double **gmag,	/* 2-D array of magnitudes (returned) */
	int *gtype,	/* Array of object types (returned) */
	int nlog);	/* Verbose mode if > 1, number of sources per log line */
    int tmcrnum(	/* Read sources by ID number from 2MASS Point Source Catalog */
	int refcat,	/* Catalog code from wcscat.h (TMPSC or TMXSC or TMPSCE) */
	int nstars,	/* Number of stars to look for */
	int sysout,	/* Search coordinate system */
	double eqout,	/* Search coordinate equinox */
	double epout,	/* Proper motion epoch (0.0 for no proper motion) */
	double *gnum,	/* Array of source numbers to look for */
	double *gra,	/* Array of right ascensions (returned) */
	double *gdec,	/* Array of declinations (returned) */
	double **gmag,	/* 2-D array of magnitudes (returned) */
	int *gtype,	/* Array of object types (returned) */
	int nlog);	/* Verbose mode if > 1, number of sources per log line */
    int tmcbin(		/* Bin sources from 2MASS Point Source Catalog */
	int refcat,	/* Catalog code from wcscat.h (TMPSC or TMXSC or TMPSCE) */
	struct WorldCoor *wcs, /* World coordinate system for image */
	char *header,	/* FITS header for output image */
	char *image,	/* Output FITS image */
	double mag1,	/* Minimum (brightest) magnitude (no limits if equal) */
	double mag2,	/* Maximum (faintest) magnitude (no limits if equal) */
	int sortmag,	/* Magnitude to use (1 to nmag) */
	double magscale, /* Scaling factor for magnitude to pixel flux
			 * (image of number of catalog objects per bin if 0) */
	int nlog);	/* Verbose mode if > 1, number of sources per log line */

/* Subroutines to read local copies of USNO catalogs */
    int uacread(	/* Read sources by sky region from USNO A or SA Catalog */
	char *refcatname, /* Name of catalog (UAC, USAC, UAC2, USAC2) */
	int distsort,	/* 1 to sort stars by distance from center */
	double cra,	/* Search center J2000 right ascension in degrees */
	double cdec,	/* Search center J2000 declination in degrees */
	double dra,	/* Search half width in right ascension in degrees */
	double ddec,	/* Search half-width in declination in degrees */
	double drad,	/* Limiting separation in degrees (ignore if 0) */
	double dradi,	/* Inner edge of annulus in degrees (ignore if 0) */
	int sysout,	/* Search coordinate system */
	double eqout,	/* Search coordinate equinox */
	double epout,	/* Proper motion epoch (0.0 for no proper motion) */
	double mag1,	/* Limiting magnitudes (none if equal) */
	double mag2,	/* Limiting magnitudes (none if equal) */
	int sortmag,	/* Number of magnitude by which to limit and sort */
	int nstarmax,	/* Maximum number of stars to be returned */
	double *unum,	/* Array of catalog numbers (returned) */
	double *ura,	/* Array of right ascensions (returned) */
	double *udec,	/* Array of declinations (returned) */
	double **umag,	/* 2-D array of magnitudes (returned) */
	int *uplate,	/* Array of plate numbers (returned) */
	int nlog);	/* Verbose mode if > 1, number of sources per log line */
    int uacrnum(	/* Read sources by ID number from USNO A or SA Catalog */
	char *refcatname, /* Name of catalog (UAC, USAC, UAC2, USAC2) */
	int nnum,	/* Number of stars to look for */
	int sysout,	/* Search coordinate system */
	double eqout,	/* Search coordinate equinox */
	double epout,	/* Proper motion epoch (0.0 for no proper motion) */
	double *unum,	/* Array of source numbers to look for */
	double *ura,	/* Array of right ascensions (returned) */
	double *udec,	/* Array of declinations (returned) */
	double **umag,	/* 2-D array of magnitudes (returned) */
	int *uplate,	/* Array of plate numbers (returned) */
	int nlog);	/* Verbose mode if > 1, number of sources per log line */
    int uacbin(		/* Bin sources from USNO A or SA Catalog */
	char *refcatname, /* Name of catalog (UAC, USAC, UAC2, USAC2) */
	struct WorldCoor *wcs, /* World coordinate system for image */
	char *header,	/* FITS header for output image */
	char *image,	/* Output FITS image */
	double mag1,	/* Minimum (brightest) magnitude (no limits if equal) */
	double mag2,	/* Maximum (faintest) magnitude (no limits if equal) */
	int sortmag,	/* Magnitude to use (1 to nmag) */
	double magscale, /* Scaling factor for magnitude to pixel flux
			 * (image of number of catalog objects per bin if 0) */
	int nlog);	/* Verbose mode if > 1, number of sources per log line */
    void setuplate(	/* Set USNO catalog plate number to search */
	int xplate);	/* If nonzero, use objects only from this plate */
    int getuplate(void); /* Get USNO catalog plate number to search */

    int ubcread(	/* Read sources by sky region from USNO B Catalog */
	char *refcatname, /* Name of catalog (UB1 only for now) */
	int distsort,	/* 1 to sort stars by distance from center */
	double cra,	/* Search center J2000 right ascension in degrees */
	double cdec,	/* Search center J2000 declination in degrees */
	double dra,	/* Search half width in right ascension in degrees */
	double ddec,	/* Search half-width in declination in degrees */
	double drad,	/* Limiting separation in degrees (ignore if 0) */
	double dradi,	/* Inner edge of annulus in degrees (ignore if 0) */
	int sysout,	/* Search coordinate system */
	double eqout,	/* Search coordinate equinox */
	double epout,	/* Proper motion epoch (0.0 for no proper motion) */
	double mag1,	/* Limiting magnitudes (none if equal) */
	double mag2,	/* Limiting magnitudes (none if equal) */
	int sortmag,	/* Number of magnitude by which to limit and sort */
	int nstarmax,	/* Maximum number of stars to be returned */
	double *unum,	/* Array of ID numbers (returned) */
	double *ura,	/* Array of right ascensions (returned) */
	double *udec,	/* Array of declinations (returned) */
	double *upra,	/* Array of right ascension proper motions (returned) */
	double *updec,	/* Array of declination proper motions (returned) */
	double **umag,	/* 2-D array of magnitudes (returned) */
	int *upmni,	/* Array of number of ids and pm quality (returned) */
	int nlog);	/* Verbose mode if > 1, number of sources per log line */
    int ubcrnum(	/* Read sources by ID number from USNO B Catalog */
	char *refcatname, /* Name of catalog (UB1 only for now) */
	int nnum,	/* Number of stars to look for */
	int sysout,	/* Search coordinate system */
	double eqout,	/* Search coordinate equinox */
	double epout,	/* Proper motion epoch (0.0 for no proper motion) */
	double *unum,	/* Array of source numbers to look for */
	double *ura,	/* Array of right ascensions (returned) */
	double *udec,	/* Array of declinations (returned) */
	double *upra,	/* Array of right ascension proper motions (returned) */
	double *updec,	/* Array of declination proper motions (returned) */
	double **umag,	/* 2-D array of magnitudes (returned) */
	int *upmni,	/* Array of number of ids and pm quality (returned) */
	int nlog);	/* Verbose mode if > 1, number of sources per log line */
    int ubcbin(		/* Bin sources from USNO B Catalog */
	char *refcatname, /* Name of catalog (UB1 only for now) */
	struct WorldCoor *wcs, /* World coordinate system for image */
	char *header,	/* FITS header for output image */
	char *image,	/* Output FITS image */
	double mag1,	/* Minimum (brightest) magnitude (no limits if equal) */
	double mag2,	/* Maximum (faintest) magnitude (no limits if equal) */
	int sortmag,	/* Magnitude to use (1 to nmag) */
	double magscale, /* Scaling factor for magnitude to pixel flux
			 * (image of number of catalog objects per bin if 0) */
	int nlog);	/* Verbose mode if > 1, number of sources per log line */

    int ucacread(	/* Read sources by sky region from USNO UCAC 1 Catalog */
	char *refcatname, /* Name of catalog (UCAC1 or UCAC2) */
	double cra,	/* Search center J2000 right ascension in degrees */
	double cdec,	/* Search center J2000 declination in degrees */
	double dra,	/* Search half width in right ascension in degrees */
	double ddec,	/* Search half-width in declination in degrees */
	double drad,	/* Limiting separation in degrees (ignore if 0) */
	double dradi,	/* Inner edge of annulus in degrees (ignore if 0) */
	int distsort,	/* 1 to sort stars by distance from center */
	int sysout,	/* Search coordinate system */
	double eqout,	/* Search coordinate equinox */
	double epout,	/* Proper motion epoch (0.0 for no proper motion) */
	double mag1,	/* Limiting magnitudes (none if equal) */
	double mag2,	/* Limiting magnitudes (none if equal) */
	int sortmag,	/* Number of magnitude by which to limit and sort */
	int nstarmax,	/* Maximum number of stars to be returned */
	double *gnum,	/* Array of ID numbers (returned) */
	double *gra,	/* Array of right ascensions (returned) */
	double *gdec,	/* Array of declinations (returned) */
	double *gpra,	/* Array of right ascension proper motions (returned) */
	double *gpdec,	/* Array of declination proper motions (returned) */
	double **gmag,	/* 2-D array of magnitudes (returned) */
	int *gtype,	/* Array of object types (returned) */
	int nlog);	/* Verbose mode if > 1, number of sources per log line */
    int ucacrnum(	/* Read sources by ID number from USNO UCAC 1 Catalog */
	char *refcatname, /* Name of catalog (UCAC1 or UCAC2) */
	int nstars,	/* Number of stars to look for */
	int sysout,	/* Search coordinate system */
	double eqout,	/* Search coordinate equinox */
	double epout,	/* Proper motion epoch (0.0 for no proper motion) */
	double *gnum,	/* Array of source numbers to look for */
	double *gra,	/* Array of right ascensions (returned) */
	double *gdec,	/* Array of declinations (returned) */
	double *gpra,	/* Array of right ascension proper motions (returned) */
	double *gpdec,	/* Array of declination proper motions (returned) */
	double **gmag,	/* 2-D array of magnitudes (returned) */
	int *gtype,	/* Array of object types (returned) */
	int nlog);	/* Verbose mode if > 1, number of sources per log line */
    int ucacbin(	/* Bin sources from USNO UCAC 1 Catalog */
	char *refcatname, /* Name of catalog (UCAC1 or UCAC2) */
	struct WorldCoor *wcs, /* World coordinate system for image */
	char *header,	/* FITS header for output image */
	char *image,	/* Output FITS image */
	double mag1,	/* Minimum (brightest) magnitude (no limits if equal) */
	double mag2,	/* Maximum (faintest) magnitude (no limits if equal) */
	int sortmag,	/* Magnitude to use (1 to nmag) */
	double magscale, /* Scaling factor for magnitude to pixel flux
			 * (image of number of catalog objects per bin if 0) */
	int nlog);	/* Verbose mode if > 1, number of sources per log line */

    int ujcread(	/* Read sources by sky region from USNO J Catalog */
	char *refcatname, /* Name of catalog (UJC, xxxxx.usno) */
	double cra,	/* Search center J2000 right ascension in degrees */
	double cdec,	/* Search center J2000 declination in degrees */
	double dra,	/* Search half width in right ascension in degrees */
	double ddec,	/* Search half-width in declination in degrees */
	double drad,	/* Limiting separation in degrees (ignore if 0) */
	double dradi,	/* Inner edge of annulus in degrees (ignore if 0) */
	int distsort,	/* 1 to sort stars by distance from center */
	int sysout,	/* Search coordinate system */
	double eqout,	/* Search coordinate equinox */
	double epout,	/* Proper motion epoch (0.0 for no proper motion) */
	double mag1,	/* Limiting magnitudes (none if equal) */
	double mag2,	/* Limiting magnitudes (none if equal) */
	int nstarmax,	/* Maximum number of stars to be returned */
	double *unum,	/* Array of catalog numbers (returned) */
	double *ura,	/* Array of right ascensions (returned) */
	double *udec,	/* Array of declinations (returned) */
	double **umag,	/* 2-D array of magnitudes (returned) */
	int *uplate,	/* Array of plate numbers (returned) */
	int nlog);	/* Verbose mode if > 1, number of sources per log line */
    int ujcrnum(	/* Read sources by ID number from USNO J Catalog */
	char *refcatname, /* Name of catalog (UJC, xxxxx.usno) */
	int nnum,	/* Number of stars to look for */
	int sysout,	/* Search coordinate system */
	double eqout,	/* Search coordinate equinox */
	double epout,	/* Proper motion epoch (0.0 for no proper motion) */
	double *unum,	/* Array of source numbers to look for */
	double *ura,	/* Array of right ascensions (returned) */
	double *udec,	/* Array of declinations (returned) */
	double **umag,	/* 2-D array of magnitudes (returned) */
	int *uplate,	/* Array of plate numbers (returned) */
	int nlog);	/* Verbose mode if > 1, number of sources per log line */
    int ujcbin(		/* Bin sources from USNO J Catalog */
	char *refcatname, /* Name of catalog (UJC, xxxxx.usno) */
	struct WorldCoor *wcs, /* World coordinate system for image */
	char *header,	/* FITS header for output image */
	char *image,	/* Output FITS image */
	double mag1,	/* Minimum (brightest) magnitude (no limits if equal) */
	double mag2,	/* Maximum (faintest) magnitude (no limits if equal) */
	int sortmag,	/* Magnitude to use (1 to nmag) */
	double magscale, /* Scaling factor for magnitude to pixel flux
			 * (image of number of catalog objects per bin if 0) */
	int nlog);	/* Verbose mode if > 1, number of sources per log line */

/* Subroutines to read a local copy of the Tycho-2 catalog */
    int ty2read(	/* Read sources by sky region from Tycho 2 Catalog */
	int refcat,	/* Catalog code from wcscat.h (TYCHO2 or TYCHO2E */
	double cra,	/* Search center J2000 right ascension in degrees */
	double cdec,	/* Search center J2000 declination in degrees */
	double dra,	/* Search half width in right ascension in degrees */
	double ddec,	/* Search half-width in declination in degrees */
	double drad,	/* Limiting separation in degrees (ignore if 0) */
	double dradi,	/* Inner edge of annulus in degrees (ignore if 0) */
	int distsort,	/* 1 to sort stars by distance from center */
	int sysout,	/* Search coordinate system */
	double eqout,	/* Search coordinate equinox */
	double epout,	/* Proper motion epoch (0.0 for no proper motion) */
	double mag1,	/* Limiting magnitudes (none if equal) */
	double mag2,	/* Limiting magnitudes (none if equal) */
	int sortmag,	/* Number of magnitude by which to limit and sort */
	int nstarmax,	/* Maximum number of stars to be returned */
	double *gnum,	/* Array of ID numbers (returned) */
	double *gra,	/* Array of right ascensions (returned) */
	double *gdec,	/* Array of declinations (returned) */
	double *gpra,	/* Array of right ascension proper motions (returned) */
	double *gpdec,	/* Array of declination proper motions (returned) */
	double **gmag,	/* 2-D array of magnitudes (returned) */
	int *gtype,	/* Array of object types (returned) */
	int nlog);	/* Verbose mode if > 1, number of sources per log line */
    int ty2rnum(	/* Read sources by ID number from Tycho 2 Catalog */
	int refcat,	/* Catalog code from wcscat.h (TYCHO2 or TYCHO2E */
	int nstars,	/* Number of stars to look for */
	int sysout,	/* Search coordinate system */
	double eqout,	/* Search coordinate equinox */
	double epout,	/* Proper motion epoch (0.0 for no proper motion) */
	double *gnum,	/* Array of source numbers to look for */
	double *gra,	/* Array of right ascensions (returned) */
	double *gdec,	/* Array of declinations (returned) */
	double *gpra,	/* Array of right ascension proper motions (returned) */
	double *gpdec,	/* Array of declination proper motions (returned) */
	double **gmag,	/* 2-D array of magnitudes (returned) */
	int *gtype,	/* Array of object types (returned) */
	int nlog);	/* Verbose mode if > 1, number of sources per log line */
    int ty2bin(		/* Bin sources from Tycho 2 Catalog */
	int refcat,	/* Catalog code from wcscat.h (TYCHO2 or TYCHO2E */
	struct WorldCoor *wcs, /* World coordinate system for image */
	char *header,	/* FITS header for output image */
	char *image,	/* Output FITS image */
	double mag1,	/* Minimum (brightest) magnitude (no limits if equal) */
	double mag2,	/* Maximum (faintest) magnitude (no limits if equal) */
	int sortmag,	/* Magnitude to use (1 to nmag) */
	double magscale, /* Scaling factor for magnitude to pixel flux
			 * (image of number of catalog objects per bin if 0) */
	int nlog);	/* Verbose mode if > 1, number of sources per log line */

/* Subroutines to read a local copy of the ACT catalog */
    int actread(	/* Read sources by sky region from USNO ACT Catalog */
	double cra,	/* Search center J2000 right ascension in degrees */
	double cdec,	/* Search center J2000 declination in degrees */
	double dra,	/* Search half width in right ascension in degrees */
	double ddec,	/* Search half-width in declination in degrees */
	double drad,	/* Limiting separation in degrees (ignore if 0) */
	double dradi,	/* Inner edge of annulus in degrees (ignore if 0) */
	int distsort,	/* 1 to sort stars by distance from center */
	int sysout,	/* Search coordinate system */
	double eqout,	/* Search coordinate equinox */
	double epout,	/* Proper motion epoch (0.0 for no proper motion) */
	double mag1,	/* Limiting magnitudes (none if equal) */
	double mag2,	/* Limiting magnitudes (none if equal) */
	int sortmag,	/* Number of magnitude by which to limit and sort */
	int nstarmax,	/* Maximum number of stars to be returned */
	double *gnum,	/* Array of ID numbers (returned) */
	double *gra,	/* Array of right ascensions (returned) */
	double *gdec,	/* Array of declinations (returned) */
	double *gpra,	/* Array of right ascension proper motions (returned) */
	double *gpdec,	/* Array of declination proper motions (returned) */
	double **gmag,	/* 2-D array of magnitudes (returned) */
	int *gtype,	/* Array of object types (returned) */
	int nlog);	/* Verbose mode if > 1, number of sources per log line */
    int actrnum(	/* Read sources by ID number from USNO ACT Catalog */
	int nstars,	/* Number of stars to look for */
	int sysout,	/* Search coordinate system */
	double eqout,	/* Search coordinate equinox */
	double epout,	/* Proper motion epoch (0.0 for no proper motion) */
	double *gnum,	/* Array of source numbers to look for */
	double *gra,	/* Array of right ascensions (returned) */
	double *gdec,	/* Array of declinations (returned) */
	double *gpra,	/* Array of right ascension proper motions (returned) */
	double *gpdec,	/* Array of declination proper motions (returned) */
	double **gmag,	/* 2-D array of magnitudes (returned) */
	int *gtype,	/* Array of object types (returned) */
	int nlog);	/* Verbose mode if > 1, number of sources per log line */
    int actbin(		/* Bin sources from USNO ACT Catalog */
	struct WorldCoor *wcs, /* World coordinate system for image */
	char *header,	/* FITS header for output image */
	char *image,	/* Output FITS image */
	double mag1,	/* Minimum (brightest) magnitude (no limits if equal) */
	double mag2,	/* Maximum (faintest) magnitude (no limits if equal) */
	int sortmag,	/* Magnitude to use (1 to nmag) */
	double magscale, /* Scaling factor for magnitude to pixel flux
			 * (image of number of catalog objects per bin if 0) */
	int nlog);	/* Verbose mode if > 1, number of sources per log line */

/* Subroutines to read SAO-TDC binary format catalogs */
    int binread(	/* Read from sky region from SAO TDC binary format catalog */
	char *bincat,	/* Name of reference star catalog file */
	int distsort,	/* 1 to sort stars by distance from center */
	double cra,	/* Search center J2000 right ascension in degrees */
	double cdec,	/* Search center J2000 declination in degrees */
	double dra,	/* Search half width in right ascension in degrees */
	double ddec,	/* Search half-width in declination in degrees */
	double drad,	/* Limiting separation in degrees (ignore if 0) */
	double dradi,	/* Inner edge of annulus in degrees (ignore if 0) */
	int sysout,	/* Search coordinate system */
	double eqout,	/* Search coordinate equinox */
	double epout,	/* Proper motion epoch (0.0 for no proper motion) */
	double mag1,	/* Limiting magnitudes (none if equal) */
	double mag2,	/* Limiting magnitudes (none if equal) */
	int sortmag,	/* Number of magnitude by which to limit and sort */
	int nstarmax,	/* Maximum number of stars to be returned */
	struct StarCat **starcat, /* Star catalog data structure */
	double *tnum,	/* Array of ID numbers (returned) */
	double *tra,	/* Array of right ascensions (returned) */
	double *tdec,	/* Array of declinations (returned) */
	double *tpra,	/* Array of right ascension proper motions (returned) */
	double *tpdec,	/* Array of declination proper motions (returned) */
	double **tmag,	/* 2-D array of magnitudes (returned) */
	int *tpeak,	/* Array of encoded spectral types (returned) */
	char **tobj,	/* Array of object names (returned) */
	int nlog);	/* Verbose mode if > 1, number of sources per log line */
    int binrnum(	/* Read sources by ID number from SAO TDC binary format catalog */
	char *bincat,	/* Name of reference star catalog file */
	int nstars,	/* Number of stars to look for */
	int sysout,	/* Search coordinate system */
	double eqout,	/* Search coordinate equinox */
	double epout,	/* Proper motion epoch (0.0 for no proper motion) */
	int match,	/* If 1, match number exactly, else number is sequence*/
	double *tnum,	/* Array of source numbers to look for */
	double *tra,	/* Array of right ascensions (returned) */
	double *tdec,	/* Array of declinations (returned) */
	double *tpra,	/* Array of right ascension proper motions (returned) */
	double *tpdec,	/* Array of declination proper motions (returned) */
	double **tmag,	/* 2-D array of magnitudes (returned) */
	int *tpeak,	/* Array of encoded spectral types (returned) */
	char **tobj,	/* Array of object names (returned) */
	int nlog);	/* Verbose mode if > 1, number of sources per log line */
    int binbin(		/* Bin sources from SAO TDC binary format catalog */
	char *bincat,	/* Name of reference star catalog file */
	struct WorldCoor *wcs, /* World coordinate system for image */
	char *header,	/* FITS header for output image */
	char *image,	/* Output FITS image */
	double mag1,	/* Minimum (brightest) magnitude (no limits if equal) */
	double mag2,	/* Maximum (faintest) magnitude (no limits if equal) */
	int sortmag,	/* Magnitude to use (1 to nmag) */
	double magscale, /* Scaling factor for magnitude to pixel flux
			 * (image of number of catalog objects per bin if 0) */
	int nlog);	/* Verbose mode if > 1, number of sources per log line */

    int binstar(	/* Read one star entry from binary catalog, 0 if OK */
	struct StarCat *sc, /* Star catalog descriptor */
	struct Star *st, /* Current star entry (returned) */
	int istar);	/* Star sequence number in binary catalog */
    struct StarCat *binopen( /* Open binary catalog, returning number of entries */
	char *bincat);	/* Name of reference star catalog file */
    void binclose(	/* Close binary catalog */
	struct StarCat *sc); /* Star catalog descriptor */
    int isbin(		/* Return 1 if TDC binary catalog file, else 0 */
	char *filename); /* Name of file to check */

/* Subroutines for extracting tab table information (in tabread.c) */
    int tabread(	/* Read sources from tab table catalog */
	char *tabcatname, /* Name of reference star catalog file */
	int distsort,	/* 1 to sort stars by distance from center */
	double cra,	/* Search center J2000 right ascension in degrees */
	double cdec,	/* Search center J2000 declination in degrees */
	double dra,	/* Search half width in right ascension in degrees */
	double ddec,	/* Search half-width in declination in degrees */
	double drad,	/* Limiting separation in degrees (ignore if 0) */
	double dradi,	/* Inner edge of annulus in degrees (ignore if 0) */
	int sysout,	/* Search coordinate system */
	double eqout,	/* Search coordinate equinox */
	double epout,	/* Proper motion epoch (0.0 for no proper motion) */
	double mag1,	/* Minimum (brightest) magnitude (no limits if equal) */
	double mag2,	/* Maximum (faintest) magnitude (no limits if equal) */
	int sortmag,	/* Magnitude by which to sort (1 to nmag) */
	int nstarmax,	/* Maximum number of stars to be returned */
	struct StarCat **starcat, /* Star catalog data structure */
	double *tnum,	/* Array of source numbers (returned) */
	double *tra,	/* Array of right ascensions (returned) */
	double *tdec,	/* Array of declinations (returned) */
	double *tpra,	/* Array of right ascension proper motions (returned) */
	double *tpdec,	/* Array of declination proper motions (returned) */
	double **tmag,	/* 2-D Array of magnitudes (returned) */
	int *tpeak,	/* Array of peak counts (returned) */
	char **tkey,	/* Array of values of additional keyword */
	int nlog);	/* Verbose mode if > 1, number of sources per log line */
    int tabrnum(	/* Read sources from tab table catalog */
	char *tabcatname, /* Name of reference star catalog file */
	int nnum,	/* Number of stars to look for */
	int sysout,	/* Search coordinate system */
	double eqout,	/* Search coordinate equinox */
	double epout,	/* Proper motion epoch (0.0 for no proper motion) */
	struct StarCat **starcat, /* Star catalog data structure */
	int match,	/* 1 to match star number exactly, else sequence num */
	double *tnum,	/* Array of source numbers to look for */
	double *tra,	/* Array of right ascensions (returned) */
	double *tdec,	/* Array of declinations (returned) */
	double *tpra,	/* Array of right ascension proper motions (returned) */
	double *tpdec,	/* Array of declination proper motions (returned) */
	double **tmag,	/* 2-D Array of magnitudes (returned) */
	int *tpeak,	/* Array of peak counts (returned) */
	char **tkey,	/* Array of values of additional keyword */
	int nlog);	/* Verbose mode if > 1, number of sources per log line */
    int tabbin(		/* Read sources from tab table catalog */
	char *tabcatname, /* Name of reference star catalog file */
	struct WorldCoor *wcs, /* World coordinate system for image */
	char *header,	/* FITS header for output image */
	char *image,	/* Output FITS image */
	double mag1,	/* Minimum (brightest) magnitude (no limits if equal) */
	double mag2,	/* Maximum (faintest) magnitude (no limits if equal) */
	int sortmag,	/* Magnitude by which to sort (1 to nmag) */
	double magscale, /* Scaling factor for magnitude to pixel flux
			 * (image of number of catalog objects per bin if 0) */
	int nlog);	/* Verbose mode if > 1, number of sources per log line */
    int tabxyread(	/* Read x, y, and magnitude from tab table star list */
	char *tabcatname, /* Name of reference star catalog file */
	double **xa,	/* Array of x coordinates (returned) */
	double **ya,	/* Array of y coordinates (returned) */
	double **ba,	/* Array of magnitudes (returned) */
	int **pa,	/* Array of fluxes (returned) */
	int nlog);	/* Verbose mode if > 1, number of sources per log line */
    int tabrkey(		/* Keyword values from tab table catalogs */
	char *tabcatname, /* Name of reference star catalog file */
	struct StarCat **starcat, /* Star catalog data structure */
	int nnum,	/* Number of stars to look for */
	double *tnum,	/* Array of source numbers to look for */
	char *keyword,	/* Keyword for which to return values */
	char **tval);	/* Returned values for specified keyword */
    struct StarCat *tabcatopen(	/* Open tab table catalog */
	char *tabpath,	/* Tab table catalog file pathname */
	struct TabTable *tabtable, /* Tab table data structure */
	int nbbuff);	/* Number of bytes in buffer; 0=read whole file */
    void tabcatclose(	/* Close tab table catalog */
	struct StarCat *sc);	/* Source catalog data structure */
    int tabstar(	/* Read one star entry from tab table catalog, 0 if OK */
	int istar,		/* Source sequence number in tab table catalog */
	struct StarCat *sc,	/* Source catalog data structure */
	struct Star *st,	/* Star data structure, updated on return */
	int verbose);		/* 1 to print error messages */
    struct TabTable *tabopen(	/* Open tab table file */
	char *tabfile,	/* Tab table catalog file name */
	int nbbuff);	/* Number of bytes in buffer; 0=read whole file */
    void tabclose(	/* Free all arrays left open by tab table structure */
	struct TabTable *tabtable); /* Tab table data structure */
    char *gettabline(	/* Find a specified line in a tab table */
	struct TabTable *tabtable, /* Tab table data structure */
	int iline);	/* Line sequence number in tab table */
    double tabgetra(	/* Return right ascension in degrees from tab table*/
	struct Tokens *tabtok,  /* Line token structure */
	int ientry);	/* sequence of entry on line */
    double tabgetdec(	/* Return declination in degrees from tab table*/
	struct Tokens *tabtok,  /* Line token structure */
	int ientry);	/* sequence of entry on line */
    double tabgetr8(	/* Return double number from tab table line */
	struct Tokens *tabtok,  /* Line token structure */
	int ientry);	/* sequence of entry on line */
    int tabgeti4(	/* Return 4-byte integer from tab table line */
	struct Tokens *tabtok,  /* Line token structure */
	int ientry);	/* sequence of entry on line */
    void settabkey(	/* Set tab table keyword to read for object */
	char *keyword);	/* column header of desired value */
    int tabgetk(	/* Get tab table entries for named column */
	struct TabTable *tabtable, /* Tab table data structure */
	struct Tokens *tabtok,  /* Line token structure */
	char *keyword,	/* column header of desired value */
	char *string,	/* character string (returned) */
	int maxchar);	/* Maximum number of characters in returned string */
    int tabgetc(	/* Get tab table entry for named column */
	struct Tokens *tabtok,  /* Line token structure */
	int ientry,	/* sequence of entry on line */
	char *string,	/* character string (returned) */
	int maxchar);	/* Maximum number of characters in returned string */
    int tabparse(		/* Aeturn column names and positions in tabtable */
	struct TabTable *tabtable); /* Tab table data structure */
    int tabcol(		/* Find column for name */
	struct TabTable *tabtable, /* Tab table data structure */
	char *keyword);	/* column header of desired value */
    int istab(		/* Return 1 if tab table file, else 0 */
	char *filename); /* Name of file to check */
    char *gettaberr();	/* Return most recent tab table error message */
    int gettabndec();	/* Return number of decimal places in tab catalog ids */

/* Subroutines to read catalogs over the web, from SCAT, HST, ESO, or SDSS servers */
    int webread(	/* Read sources by sky region from WWW catalog */
	char *caturl,	/* URL of search engine */
	char *refcatname, /* Name of catalog */
	int distsort,	/* 1 to sort stars by distance from center */
	double cra,	/* Search center J2000 right ascension in degrees */
	double cdec,	/* Search center J2000 declination in degrees */
	double dra,	/* Search half width in right ascension in degrees */
	double ddec,	/* Search half-width in declination in degrees */
	double drad,	/* Limiting separation in degrees (ignore if 0) */
	double dradi,	/* Inner edge of annulus in degrees (ignore if 0) */
	int sysout,	/* Search coordinate system */
	double eqout,	/* Search coordinate equinox */
	double epout,	/* Proper motion epoch (0.0 for no proper motion) */
	double mag1,	/* Limiting magnitudes (none if equal) */
	double mag2,	/* Limiting magnitudes (none if equal) */
	int sortmag,	/* Number of magnitude by which to limit and sort */
	int nstarmax,	/* Maximum number of stars to be returned */
	double *unum,	/* Array of ID numbers (returned) */
	double *ura,	/* Array of right ascensions (returned) */
	double *udec,	/* Array of declinations (returned) */
	double *upra,	/* Array of right ascension proper motions (returned) */
	double *updec,	/* Array of declination proper motions (returned) */
	double **umag,	/* 2-D array of magnitudes (returned) */
	int *utype,	/* Array of integer catalog values (returned) */
	int nlog);	/* Verbose mode if > 1, number of sources per log line */
    int webrnum(	/* Read sources by ID number from WWW catalog */
	char *caturl,	/* URL of search engine */
	char *refcatname, /* Name of catalog */
	int nnum,	/* Number of stars to look for */
	int sysout,	/* Search coordinate system */
	double eqout,	/* Search coordinate equinox */
	double epout,	/* Proper motion epoch (0.0 for no proper motion) */
	double *unum,	/* Array of source numbers to look for */
	double *ura,	/* Array of right ascensions (returned) */
	double *udec,	/* Array of declinations (returned) */
	double *upra,	/* Array of right ascension proper motions (returned) */
	double *updec,	/* Array of declination proper motions (returned) */
	double **umag,	/* 2-D array of magnitudes (returned) */
	int *utype,	/* Array of object types (returned) */
	int nlog);	/* Verbose mode if > 1, number of sources per log line */
    char *webbuff(	/* Read URL into buffer across the web */
	char *url,	/* URL to read */
	int diag,	/* 1 to print diagnostic messages */
	int *lbuff);	/* Length of buffer (returned) */
    struct TabTable *webopen(	/* Open tab table across the web */
	char *caturl,	/* URL of search engine */
	char *srchpar,	/* Search engine parameters to append */
	int nlog);	/* 1 to print diagnostic messages */

/* Subroutines to read DAOPHOT-style catalogs of sources found in an image */
    int daoread(	/* Read image source positions from x y mag file */
	char *daocat,	/* Name of DAOFIND catalog file */
	double **xa,	/* X and Y coordinates of stars, array returned */
	double **ya,	/* X and Y coordinates of stars, array returned */
	double **ba,	/* Instrumental magnitudes of stars, array returned */
	int **pa,	/* Peak counts of stars in counts, array returned */
	int nlog);	/* 1 to print each star's position */
    int daoopen(	/* Open image source position x y mag file */
	char *daofile);	/* DAOFIND catalog file name */
    char *daoline(	/* Read line from image source position x y mag file */
	int iline,	/* Star sequence number in DAOFIND catalog */
	char *line);	/* Pointer to iline'th entry (returned updated) */

/* Subroutines for sorting tables of star positions and magnitudes from sortstar.c */
    void FluxSortStars(	/* Sort image stars by decreasing flux */
	double *sx,	/* Image X coordinate */
	double *sy,	/* Image Y coordinate */
	double *sb,	/* Brighness in counts */
	int *sc,	/* Other 4-byte information */
	int ns);	/* Number of stars to sort */
    void MagSortStars(	/* Sort image stars by increasing magnitude */
	double *sn,	/* Identifying number */
	double *sra,	/* Right Ascension */
	double *sdec,	/* Declination */
	double *spra,	/* Right Ascension proper motion */
	double *spdec,	/* Declination proper motion */
	double *sx,	/* Image X coordinate */
	double *sy,	/* Image Y coordinate */
	double **sm,	/* Magnitudes */
	int *sc,	/* Other 4-byte information */
	char **sobj,	/* Object name */
	int ns,		/* Number of stars to sort */
	int nm,		/* Number of magnitudes per star */
	int ms);	/* Magnitude by which to sort (1 to nmag) */
    void IDSortStars(	/* Sort image stars by increasing ID Number value */
	double *sn,	/* Identifying number */
	double *sra,	/* Right Ascension */
	double *sdec,	/* Declination */
	double *spra,	/* Right Ascension proper motion */
	double *spdec,	/* Declination proper motion */
	double *sx,	/* Image X coordinate */
	double *sy,	/* Image Y coordinate */
	double **sm,	/* Magnitudes */
	int *sc,	/* Other 4-byte information */
	char **sobj,	/* Object name */
	int ns,		/* Number of stars to sort */
	int nm);	/* Number of magnitudes per star */
    void RASortStars(	/* Sort image stars by increasing right ascension */
	double *sn,	/* Identifying number */
	double *sra,	/* Right Ascension */
	double *sdec,	/* Declination */
	double *spra,	/* Right Ascension proper motion */
	double *spdec,	/* Declination proper motion */
	double *sx,	/* Image X coordinate */
	double *sy,	/* Image Y coordinate */
	double **sm,	/* Magnitudes */
	int *sc,	/* Other 4-byte information */
	char **sobj,	/* Object name */
	int ns,		/* Number of stars to sort */
	int nm);	/* Number of magnitudes per star */
    void DecSortStars(	/* Sort image stars by increasing declination */
	double *sn,	/* Identifying number */
	double *sra,	/* Right Ascension */
	double *sdec,	/* Declination */
	double *spra,	/* Right Ascension proper motion */
	double *spdec,	/* Declination proper motion */
	double *sx,	/* Image X coordinate */
	double *sy,	/* Image Y coordinate */
	double **sm,	/* Magnitudes */
	int *sc,	/* Other 4-byte information */
	char **sobj,	/* Object name */
	int ns,		/* Number of stars to sort */
	int nm);	/* Number of magnitudes per star */
    void XSortStars(	/* Sort image stars by increasing image X value */
	double *sn,	/* Identifying number */
	double *sra,	/* Right Ascension */
	double *sdec,	/* Declination */
	double *spra,	/* Right Ascension proper motion */
	double *spdec,	/* Declination proper motion */
	double *sx,	/* Image X coordinate */
	double *sy,	/* Image Y coordinate */
	double **sm,	/* Magnitudes */
	int *sc,	/* Other 4-byte information */
	char **sobj,	/* Object name */
	int ns,		/* Number of stars to sort */
	int nm);	/* Number of magnitudes per star */
    void YSortStars(	/* Sort image stars by increasing image Y value */
	double *sn,	/* Identifying number */
	double *sra,	/* Right Ascension */
	double *sdec,	/* Declination */
	double *spra,	/* Right Ascension proper motion */
	double *spdec,	/* Declination proper motion */
	double *sx,	/* Image X coordinate */
	double *sy,	/* Image Y coordinate */
	double **sm,	/* Magnitudes */
	int *sc,	/* Other 4-byte information */
	char **sobj,	/* Object name */
	int ns,		/* Number of stars to sort */
	int nm);	/* Number of magnitudes per star */
    int MergeStars(	/* Merge multiple entries within given radius
			 * return mean ra, dec, proper motion, and magnitude(s) */
	double *sn,	/* Identifying number */
	double *sra,	/* Right Ascension */
	double *sdec,	/* Declination */
	double *spra,	/* Right Ascension proper motion */
	double *spdec,	/* Declination proper motion */
	double *sx,	/* Image X coordinate */
	double *sy,	/* Image Y coordinate */
	double **sm,	/* Magnitudes */
	int *sc,	/* Other 4-byte information */
	char **sobj,	/* Object name */
	int ns,		/* Number of stars to sort */
	int nm,		/* Number of magnitudes per star */
	double rad,	/* Maximum separation in arcseconds to merge */
	int log);	/* If >0, log progress every time mod number written */

/* Catalog utility subroutines from catutil.c */

/* Subroutines for dealing with catalogs */
    int RefCat(		/* Return catalog type code, title, coord. system */
	char *refcatname, /* Name of reference catalog */
	char *title,	/* Description of catalog (returned) */
	int *syscat,	/* Catalog coordinate system (returned) */
	double *eqcat,	/* Equinox of catalog (returned) */
	double *epcat,	/* Epoch of catalog (returned) */
	int *catprop,	/* 1 if proper motion in catalog (returned) */
	int *nmag);	/* Number of magnitudes in catalog (returned) */
    int CatCode(	/* Return catalog type code */
	char *refcatname); /* Name of reference catalog */
    char *CatName(	/* Return catalog name given catalog type code */
	int refcat,	/* Catalog code */
	char *refcatname); /* Name of reference catalog */
    char *CatSource(	/* Return catalog source description given catalog type code */
	int refcat,	/* Catalog code */
	char *refcatname); /* Name of reference catalog */
    void CatID(		/* Return catalog ID keyword given catalog type code */
	char *catid,	/* Catalog ID (returned) */
	int refcat);	/* Catalog code */
    double CatRad(	/* Return default search radius for given catalog */
	int refcat);	/* Catalog code */
    char *ProgName(	/* Return program name given program path used */
	char *progpath0); /* Pathname by which program is invoked */
    char *ProgCat(	/* Return catalog name given program name used */
	char *progname); /* Program name which might contain catalog code */
    void CatNum(	/* Return formatted source number */
	int refcat,	/* Catalog code */
	int nnfld,	/* Number of characters in number (from CatNumLen)
			 * Print leading zeroes if negative */
	int nndec,	/* Number of decimal places ( >= 0)
			 * Omit leading spaces if negative */
	double dnum,	/* Catalog number of source */
	char *numstr);	/* Formatted number (returned) */
    int CatNumLen(	/* Return length of source numbers */
	int refcat,	/* Catalog code */
	double maxnum,	/* Maximum ID number
			 * (Ignored for standard catalogs) */
	int nndec);	/* Number of decimal places ( >= 0) */
    int CatNdec(	/* Return number of decimal places in source numbers */
	int refcat);	/* Catalog code */
    void CatMagName(	/* Return name of specified magnitude */
	int imag,	/* Sequence number of magnitude */
	int refcat,	/* Catalog code */
	char *magname); /* Name of magnitude, returned */
    int CatMagNum(	/* Returns number of magnitude specified by letter as int */
	int imag,	/* int of magnitude letter */
	int refcat);	/* Catalog code */
    void CatTabHead (	/* Print heading for catalog search result table */
	int refcat,	/* Catalog being searched */
	int sysout,	/* Output coordinate system */
	int nnfld,	/* Number of characters in ID column */
	int mprop,	/* 1 if proper motion in catalog */
	int nmag,	/* Number of magnitudes */
	char *ranges,	/* Catalog numbers to print */
	char *keyword,	/* Column to add to tab table output */
	int gcset,	/* 1 if there are any values in gc[] */
	int tabout,	/* 1 if output is tab-delimited */
	int classd,	/* GSC object class to accept (-1=all) */
	int printxy,	/* 1 if X and Y included in output */
	char **gobj1,	/* Pointer to array of object names; NULL if none */
	FILE *fd);	/* Output file descriptor; none if NULL */
    int StrNdec(	/* Return number of decimal places in numeric string */
	char *string);	/* Numeric string */
    int NumNdec(	/* Return number of decimal places in a number */
	double number); /* Floating point number */
    void setdateform (	/* Set date format code */
	int dateform0);	/* Date format code */
    char *DateString(	/* Return string with epoch of position in desired format */
	double epoch,	/* Date as fraction of a year */
	int tabout);	/* 1 for tab-preceded output string, else space-preceded */
    void setlimdeg(	/* Limit output in degrees (1) or hh:mm:ss dd:mm:ss (0) */
	int degout);	/* 1 for fractional degrees, else sexagesimal hours, degrees */

    void SearchLim(	/* Compute limiting RA and Dec */
	double cra,	/* Longitude/Right Ascension of Center of search area in degrees */
	double cdec,	/* Latitude/Declination of search area in degrees */
	double dra,	/* Horizontal half-width in degrees */
	double ddec,	/* Vertical half-width in degrees */
	int syscoor,	/* Coordinate system */
	double *ra1,	/* Lower right ascension limit in degrees (returned) */
	double *ra2,	/* Upper right ascension limit in degrees (returned) */
	double *dec1,	/* Lower declination limit in degrees (returned) */
	double *dec2,	/* Upper declination limit in degrees (returned) */
	int verbose);	/* 1 to print limits, else 0 */
    void RefLim(	/* Compute limiting RA and Dec in new system */
	double cra,	/* Longitude/Right Ascension of Center of search area in degrees */
	double cdec,	/* Latitude/Declination of search area in degrees */
	double dra,	/* Horizontal half-width in degrees */
	double ddec,	/* Vertical half-width in degrees */
	int sysc,	/* System of search coordinates */
	int sysr,	/* System of reference catalog coordinates */
	double eqc,	/* Equinox of search coordinates in years */
	double epr,	/* Epoch of reference catalog coordinates in years */
	double secmarg,	/* Margin in arcsec/century to catch moving stars */
	double *ramin,	/* Lower right ascension limit in degrees (returned) */
	double *ramax,	/* Upper right ascension limit in degrees (returned) */
	double *decmin,	/* Lower declination limit in degrees (returned) */
	double *decmax,	/* Upper declination limit in degrees (returned) */
	int verbose);	/* 1 to print limits, else 0 */

/* Subroutines for dealing with ranges */
    struct Range *RangeInit(	/* Initialize range structure from string */
	char *string,	/* String containing numbers separated by , and - */
	int ndef);	/* Maximum allowable range value */
    int isrange(	/* Return 1 if string is a range of numbers, else 0 */
	char *string);	/* String which might be a range of numbers */
    void rstart(	/* Restart range */
	struct Range *range); /* Range structure */
    int rgetn(		/* Return number of values in all ranges */
	struct Range *range); /* Range structure */
    int rgeti4(		/* Return next number in range as integer */
	struct Range *range); /* Range structure */
    double rgetr8(	/* Return next number in range as double */
	struct Range *range); /* Range structure */

/* Subroutines for access to tokens within a string */
    int setoken(	/* Tokenize a string for easy decoding */
	struct Tokens *tokens, /* Token structure returned */
	char    *string, /* character string to tokenize */
	char *cwhite);	/* additional whitespace characters
			 * if = tab, disallow spaces and commas */
    int nextoken(	/* Get next token from tokenized string */
	struct Tokens *tokens, /* Token structure returned */
	char *token,	/* token (returned) */
	int maxchars);	/* Maximum length of token */
    int getoken(	/* Get specified token from tokenized string */
	struct Tokens *tokens, /* Token structure returned */
	int itok,	/* token sequence number of token
			 * if <0, get whole string after token -itok
			 * if =0, get whole string */
	char *token,	/* token (returned) */
	int maxchars);	/* Maximum length of token */

    int ageti4(		/* Extract int value from keyword= value in string */
	char *string,	/* character string containing <keyword>= <value> */
	char *keyword,	/* character string containing the name of the keyword
			 * the value of which is returned.  hget searches for a
			 * line beginning with this string.  if "[n]" or ",n" is
			 * present, the n'th token in the value is returned. */
	int *ival);	/* Integer value, returned */
    int agetr8(		/* Extract double value from keyword= value in string */
	char *string,	/* character string containing <keyword>= <value> */
	char *keyword,	/* character string containing the name of the keyword */
	double *dval);	/* Double value, returned */
    int agets(		/* Extract value from keyword= value in string */
	char *string,	/* character string containing <keyword>= <value> */
	char *keyword,	/* character string containing the name of the keyword */
	int lval,	/* Size of value in characters
			 * If negative, value ends at end of line */
	char *value);	/* String (returned) */

    int tmcid(		/* Return 1 if string is 2MASS ID, else 0 */
	char *string,	/* Character string to check */
	double *ra,	/* Right ascension (returned) */
	double *dec);	/* Declination (returned) */

/* Subroutines for VOTable output */
    int vothead(	/* Print heading for VOTable SCAT output */
	int refcat,	/* Catalog code */
	char *refcatname, /* Name of catalog */
	int mprop,	/* Proper motion flag */
	int typecol,	/* Flag for spectral type */
	int ns,		/* Number of sources found in catalog */
	double cra,	/* Search center right ascension */
	double cdec,	/* Search center declination */
	double drad);	/* Radius to search in degrees */
    void vottail();	/* Terminate VOTable SCAT output */

/* Subroutines for version/date string */
    void setrevmsg(	/* Set version/date string */
    char *getrevmsg(	/* Return version/date string */

/* Subroutines for fitting and evaluating polynomials */
    void polfit(	/* Fit polynomial coefficients */
    double polcomp(	/* Evaluate polynomial from polfit coefficients */

#else /* K&R prototypes */

/* Subroutines for reading TDC ASCII catalogs (ctgread.c) */
int ctgread();		/* Read sources by sky region from SAO TDC ASCII format catalog */
int ctgrnum();		/* Read sources by number from SAO TDC ASCII format catalog */
int ctgrdate();		/* Read sources by date range from SAO TDC ASCII format catalog */
int ctgbin();		/* Bin sources from SAO TDC ASCII format catalog */
int ctgstar();		/* Read one star entry from ASCII catalog, 0 if OK */
int isacat();		/* Return 1 if string is name of ASCII catalog file */
struct StarCat *ctgopen();
void ctgclose();

/* Subroutines for extracting sources from HST Guide Star Catalog */
int gscread();		/* Read sources by sky region from HST Guide Star Catalog */
int gscrnum();		/* Read sources by ID number from HST Guide Star Catalog */
int gscbin();		/* Bin sources from HST Guide Star Catalog */
void setgsclass();	/* Set GSC object class */

/* Subroutine to read GSC II catalog over the web (gsc2read.c) */
int gsc2read();		/* Read sources by sky region from GSC II Catalog */

/* Subroutine to read SDSS catalog over the web (sdssread.c) */
int sdssread();		/* Read sources by sky region from SDSS Catalog */
char *sdssc2t();	/* Convert SDSS buffer from comma- to tab-separated */

/* Subroutines to read local copy of 2MASS Point Source Catalog (tmcread.c) */
int tmcread();		/* Read sources by sky region from 2MASS Point Source Catalog */
int tmcrnum();		/* Read sources by ID number from 2MASS Point Source Catalog */
int tmcbin();		/* Bin sources from 2MASS Point Source Catalog */

/* Subroutines to read local copies of USNO A and SA catalogs (uacread.c) */
int uacread();		/* Read sources by sky region from USNO A or SA Catalog */
int uacrnum();		/* Read sources by ID number from USNO A or SA Catalog */
int uacbin();		/* Bin sources from USNO A or SA Catalog */
void setuplate();	/* Set USNO catalog plate number to search */
int getuplate();	/* Get USNO catalog plate number to search */

/* Subroutines to read local copies of USNO B catalogs (ubcread.c) */
int ubcread();		/* Read sources by sky region from USNO B Catalog */
int ubcrnum();		/* Read sources by ID number from USNO B Catalog */
int ubcbin();		/* Bin sources from USNO B Catalog */

/* Subroutines to read local copies of USNO UCAC catalogs (ucacread.c) */
int ucacread();		/* Read sources by sky region from USNO UCAC 1 Catalog */
int ucacrnum();		/* Read sources by ID number from USNO UCAC 1 Catalog */
int ucacbin();		/* Bin sources from USNO UCAC 1 Catalog */

/* Subroutines to read local copies of USNO UJ catalog (ucacread.c) */
int ujcread();		/* Read sources by sky region from USNO J Catalog */
int ujcrnum();		/* Read sources by ID number from USNO J Catalog */
int ujcbin();		/* Bin sources from USNO J Catalog */

/* Subroutines to read a local copy of the Tycho-2 catalog (ty2read.c) */
int ty2read();		/* Read sources by sky region from Tycho 2 Catalog */
int ty2rnum();		/* Read sources by ID number from Tycho 2 Catalog */
int ty2bin();		/* Bin sources from Tycho 2 Catalog */

/* Subroutines to read a local copy of the ACT catalog (actread.c) */
int actread();		/* Read sources by sky region from USNO ACT Catalog */
int actrnum();		/* Read sources by ID number from USNO ACT Catalog */
int actbin();		/* Bin sources from USNO ACT Catalog */

/* Subroutines to read SAO-TDC binary format catalogs (binread.c) */
int binread();		/* Read sources by sky region from SAO TDC binary format catalog */
int binrnum();		/* Read sources by ID number from SAO TDC binary format catalog */
int binbin();		/* Bin sources from SAO TDC binary format catalog */
int binstar();		/* Read one star entry from binary catalog, 0 if OK */
int isbin();
struct StarCat *binopen();
void binclose();

/* Subroutines for extracting tab table information (tabread.c) */
int tabread();		/* Read sources from tab table catalog */
int tabrnum();		/* Read sources from tab table catalog */
int tabbin();		/* Read sources from tab table catalog */
struct TabTable *tabopen();	/* Open tab table file */
struct StarCat *tabcatopen();	/* Open tab table catalog */
void tabcatclose();	/* Close tab table catalog */
int tabxyread();	/* Read x, y, and magnitude from tab table star list */
void settabkey();	/* Set tab table keyword to read for object */
char *gettabline();	/* Find a specified line in a tab table */
int tabrkey();		/* Keyword values from tab table catalogs */
int tabcol();		/* Find column for name */
int tabgetk();		/* Get tab table entries for named column */
int tabgetc();		/* Get tab table entry for named column */
int tabgeti4();		/* Return 4-byte integer from tab table line */
int tabparse();		/* Aeturn column names and positions in tabtable */
double tabgetra();	/* Return right ascension in degrees from tab table*/
double tabgetdec();	/* Return declination in degrees from tab table*/
double tabgetr8();	/* Return double number from tab table line */
void tabclose();	/* Free all arrays left open by tab table structure */
char *gettaberr();	/* Return most recent tab table error message */
int istab();		/* Return 1 if tab table file, else 0 */
int gettabndec();	/* Return number of decimal places in tab catalog ids */

/* Subroutines to read catalogs over the web, from SCAT, HST, ESO, or SDSS servers */
int webread();		/* Read sources by sky region from catalog on the World Wide Web */
int webrnum();		/* Read sources by ID number from catalog on the World Wide Web */
char *webbuff();	/* Read URL into buffer across the web */
struct TabTable *webopen();	/* Open tab table across the web */

/* Subroutines to read DAOPHOT-style catalogs of sources found in an image */
int daoread();		/* Read image source positions from x y mag file */
int daoopen();		/* Open image source position x y mag file */
char *daoline();	/* Read line from image source position x y mag file */

/* Subroutines for sorting tables of star positions and magnitudes from sortstar.c */
void FluxSortStars();	/* Sort image stars by decreasing flux */
void MagSortStars();	/* Sort image stars by increasing magnitude */
void IDSortStars();	/* Sort image stars by increasing ID Number value */
void RASortStars();	/* Sort image stars by increasing right ascension */
void DecSortStars();	/* Sort image stars by increasing declination */
void XSortStars();	/* Sort image stars by increasing image X value */
void YSortStars();	/* Sort image stars by increasing image Y value */
int MergeStars();	/* Merge multiple entries within given radius */

/* Catalog utility subroutines from catutil.c */

/* Subroutines for dealing with catalogs */
int CatCode();		/* Return catalog type code */
int RefCat();		/* Return catalog type code, title, coord. system */
char *CatName();	/* Return catalog name given catalog type code */
char *CatSource();	/* Return catalog source description given catalog type code */
char *ProgCat();	/* Return catalog name given program name used */
char *ProgName();	/* Return program name given program path used */
char *CatName();	/* Return catalog name given catalog type code */
void CatID();		/* Return catalog ID keyword given catalog type code */
void CatNum();		/* Return formatted source number */
int CatNumLen();	/* Return length of source numbers */
int CatNdec();		/* Return number of decimal places in source numbers */
void CatMagName();	/* Return name of specified magnitude */
int CatMagNum();	/* Returns number of magnitude specified by letter as int */
double CatRad();	/* Return default search radius for given catalog */
int tmcid();		/* Return 1 if string is 2MASS ID, else 0 */

int NumNdec();		/* Return number of decimal places in a number */
int StrNdec();		/* Return number of decimal places in numeric string */
void setdateform();	/* Set date format code */
void setlimdeg();	/* Limit output in degrees (1) or hh:mm:ss dd:mm:ss (0) */
char *DateString();		/* Convert epoch to output format */
void SearchLim();	/* Compute limiting RA and Dec */
void RefLim();		/* Compute limiting RA and Dec in new system */
int ageti4();		/* Extract int value from keyword= value in string */
int agetr8();		/* Extract double value from keyword= value in string */
int agets();		/* Extract value from keyword= value in string */
void bv2sp();		/* Approximate main sequence spectral type from B - V */

/* Subroutines for dealing with ranges */
struct Range *RangeInit();	/* Initialize range structure from string */
int isrange();		/* Return 1 if string is a range of numbers, else 0 */
int rgetn();		/* Return number of values in all ranges */
int rgeti4();		/* Return next number in range as integer */
double rgetr8();	/* Return next number in range as double */
void rstart();		/* Restart range */

/* Subroutines for access to tokens within a string */
int setoken();		/* Tokenize a string for easy decoding */
int nextoken();		/* Get next token from tokenized string */
int getoken();		/* Get specified token from tokenized string */

/* Subroutines for VOTable output */
int vothead();		/* Print heading for VOTable SCAT output */
void vottail();		/* Terminate VOTable SCAT output */

/* Subroutines for version/date string */
void setrevmsg();	/* Set version/date string */
char *getrevmsg();	/* Return version/date string */

/* Subroutines for fitting and evaluating polynomials */
void polfit();		/* Fit polynomial coefficients */
double polcomp();	/* Evaluate polynomial from polfit coefficients */

#endif  /* __STDC__ */

#ifdef __cplusplus
}
#endif __cplusplus

#endif  /* _wcscat_h_ */

/* Sep 22 1998  New header file (star.h)
 * Oct 16 1998  Add more options for ASCII catalogs
 * Oct 20 1998  Add object name to binary files
 * Oct 21 1998	New file (wcscat.h)
 * Oct 26 1998	Combined wcscat.h and star.h
 * Oct 27 1998	Add SAOimage region shapes
 * Nov  9 1998	Add rasorted flag to catalog structure
 * Nov 20 1998	Add support for USNO A-2.0 and SA-2.0 catalogs
 * Dec  8 1998	Add support for the Hipparcos and ACT catalogs
 *
 * Jan 25 1999	Add declarations for tab table access
 * Jan 25 1999	Add declarations for dealing with ranges of numbers
 * Feb  2 1999	Add number of decimal places in star number to StarCat
 * Feb 11 1999	Add coordinate system info to star structure
 * Feb 11 1999	Change starcat.insys to starcat.coorsys for consistency
 * May 14 1999	Update Star and StarCat structure to cover tab tables
 * May 19 1999	Update StarCat structure to include epoch from catalog
 * June 4 1999	Add CatNumLen()
 * Jun 14 1999	Add SearchLim()
 * Jun 30 1999	Add isrange()
 * Jul  1 1999	Add declarations for date/time conversions in dateutil.c
 * Jul  2 1999	Add rstart()
 * Jul 26 1999	Add Yale Bright Star Catalog
 * Aug 16 1999	Add RefLim() to get converted search coordinates right
 * Aug 25 1999	Add ACT catalog
 * Sep 10 1999	Move special case setting from argument list to subroutines
 * Sep 13 1999	Add subroutines to access data structure for single stars
 * Oct  1 1999	Add structure and subroutines for tokenized strings
 * Oct 22 1999	Change cat*() to ctg*() to avoid system conflict
 * Oct 29 1999	Add tabget() subroutines
 * Nov  1 1999	Increase maximum number of tokens on a line from 20 to 100
 * Nov  2 1999	Move date utilities to fitsfile.h
 *
 * Jan 10 2000	Add column names to catalog data structure
 * Jan 11 2000	Add gettabndec()
 * Feb  9 2000	Add proper motion entry information to star data structure
 * Feb 16 2000	Add gettaberr() to return tab table error message
 * Mar  1 2000	Add isfile() and agets() to help with ASCII files
 * Mar  8 2000	Add ProgCat() to return catalog name from program name used
 * Mar  8 2000	Add ProgName() to extract program name from path used
 * Mar 10 2000	Add PropCat() to tell whether a catalog has proper motions
 * Mar 27 2000	Add tabxyread()
 * Apr  3 2000	Add option in catalog structure to ignore extra info
 * May 22 2000	Add Tycho 2 support, bv2sp()
 * May 26 2000	Add separate pointer to header in tab table structure
 * May 26 2000	Add separate pointer to table name in tab table structure
 * Jul 12 2000	Add catalog type code to ctalog data structure
 * Sep 20 2000	Add isacat() to detect ASCII catalog files
 * Sep 25 2000	Add starcat.sptype to flag spectral type in catalog
 * Oct 23 2000	Add USNO plate catalog to catalog type table
 * Oct 26 2000	Add proper motion flags for seconds and arcseconds per century
 * Oct 31 2000	Add proper motion flags for milliseconds per year
 * Nov  2 2000	Add parallax and radial velocity to star structure
 * Nov 21 2000	Add WEBCAT catalog type for tab ctalogs returned from the Web
 * Nov 22 2000	Add webread() and webrnum()
 * Nov 28 2000	Add tabparse()
 * Nov 30 2000	Add spectral type to catalog header; make star->isp 4 char.
 * Dec 13 2000	Add StrNdec() to get number of decimal places in number strings
 * Dec 15 2000	Add CatNdec() to get number of decimal places in source numbers
 * Dec 18 2000	Drop PropCat(), a cludgy proper motion flag
 *
 * Mar 22 2001	Add web search flag in catalog data structure
 * Mar 27 2001	Add shapes in pixels to SAOimage region options
 * May 14 2001	Add 2MASS Point Source Catalog flags
 * May 22 2001	Add declination sorting
 * May 24 2001	Add 2MASS Point Source Catalog subroutines
 * May 29 2001	Add length of star number to catalog structure
 * May 30 2001	Add third magnitude for tab tables to catalog structure
 * Jun 15 2001	Add CatName() and CatID()
 * Jun 19 2001	Add parallax error to catalog and star structures
 * Jun 20 2001	Add webopen(), GSC2, fourth magnitude to star and starcat
 * Jul 12 2001	Add separate web access subroutine, webbuff()
 * Jul 23 2001	Add ageti4() and agetr8()
 * Jul 24 2001	Add polfit() and polcomp()
 * Aug  8 2001	Add keyrv and option to set mprop to 2 to include rv/cz
 * Sep 10 2001	Add entry line and distance from search center to Star
 * Sep 13 2001	Add YSortStars() and SORT_Y
 * Sep 14 2001	Add lbuff to TabTable structure
 * Sep 20 2001	Add CatMagName()
 * Sep 25 2001	Move isfile() to fitsfile.h
 * Oct 16 2001	Add tabdash pointer to tabtable data structure
 *
 * Apr  9 2002	Fix typo in gettaberr() declaration
 * Apr 10 2002	Add CatMagNum()
 * May  6 2002	Increase object name length from 31 to 79 characters
 * May 13 2002	Add NumNdec(), gsc2read(), and gsc2rnum()
 * Aug  6 2002	Make magnitude entries and positions vectors of 10
 * Oct 30 2002	Add epoch keyword and FITS date to StarCat data structure
 *
 * Jan 16 2003	Add USNO-B1.0 catalog
 * Mar 24 2003	Add CatCde() to get only catalog code
 * Apr  3 2003	Add ubcread(), ubcrnum(), and FluxSortStars()
 * Apr  3 2003	Drop gsc2rnum()
 * Apr 14 2003	Add setrevmsg() and getrevmsg()
 * Apr 24 2003	Add UCAC1 and UCAC2, ucacread() and ucacrnum()
 * May 20 2003	Add TMIDR2 for 2MASS PSC Interim Data Release 2
 * Sep 16 2003	Add SORT_MERGE for scat
 * Sep 25 2003	Add *bin() subroutines for catalog binning
 * Dec  3 2003	Add USNO YB6 catalog
 *
 * Jan  5 2004	Add SDSS catalog
 * Jan 12 2004	Add 2MASS Extended Source catalog and size to star structure
 * Jan 14 2004	Add CatSource() subroutine to simplify help message creation
 * Jan 22 2004	Add setlimdeg() to print limit coordinates in degrees
 * Mar 16 2004	Add MergeStars()
 * Apr 23 2004	Add ctgrdate()
 * Aug 31 2004	Increase MAXTOKENS from 100 to 200
 * Sep  2 2004	Increase MAXTOKENS from 200 to 1000
 *
 * Jul 27 2005	Add date format codes and DateString()
 * Aug  5 2005	Add Tycho-2 and 2MASS PSC with magnitude errors
 *
 * Jan  6 2006	Add CatRad() subroutine
 * Mar 17 2006	Make vothead() int as it now returns the number of fields
 * Apr  3 2006	Add tmcid() to check for 2MASS identifiers
 * Apr  3 2006	Add setdateform() to set output date format
 * Apr 12 2006	Add SORT_ID for scat to sort catalog entries by ID number
 * Jun 20 2006	Add IDSortStars()
 *
 * Jan 10 2006	Add ANSI C function prototypes
 */
