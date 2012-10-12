/*** File libwcs/wcs.h
 *** July 25, 2007
 *** By Doug Mink, dmink@cfa.harvard.edu
 *** Harvard-Smithsonian Center for Astrophysics
 *** Copyright (C) 1994-2007
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

#ifndef _wcs_h_
#define _wcs_h_

#include "wcslib.h"
#include "fitshead.h"

/* SIRTF distortion matrix coefficients */
#define DISTMAX 10
struct Distort {
  int    a_order;                /* max power for the 1st dimension */
  double a[DISTMAX][DISTMAX];  /* coefficient array of 1st dimension */
  int    b_order;                /* max power for 1st dimension */
  double b[DISTMAX][DISTMAX];  /* coefficient array of 2nd dimension */
  int    ap_order;               /* max power for the 1st dimension */
  double ap[DISTMAX][DISTMAX]; /* coefficient array of 1st dimension */
  int    bp_order;               /* max power for 1st dimension */
  double bp[DISTMAX][DISTMAX]; /* coefficient array of 2nd dimension */
};

struct WorldCoor {
  double	xref;		/* X reference coordinate value (deg) */
  double	yref;		/* Y reference coordinate value (deg) */
  double	xrefpix;	/* X reference pixel */
  double	yrefpix;	/* Y reference pixel */
  double	xinc;		/* X coordinate increment (deg) */
  double	yinc;		/* Y coordinate increment (deg) */
  double	rot;		/* rotation around axis (deg) (N through E) */
  double	cd[4];		/* rotation matrix */
  double	dc[4];		/* inverse rotation matrix */
  double	equinox;	/* Equinox of coordinates default to 1950.0 */
  double	epoch;		/* Epoch of coordinates default to equinox */
  double	nxpix;		/* Number of pixels in X-dimension of image */
  double	nypix;		/* Number of pixels in Y-dimension of image */
  double	plate_ra;	/* Right ascension of plate center */
  double	plate_dec;	/* Declination of plate center */
  double	plate_scale;	/* Plate scale in arcsec/mm */
  double	x_pixel_offset;	/* X pixel offset of image lower right */
  double	y_pixel_offset;	/* Y pixel offset of image lower right */
  double	x_pixel_size;	/* X pixel_size */
  double	y_pixel_size;	/* Y pixel_size */
  double	ppo_coeff[6];	/* pixel to plate coefficients for DSS */
  double	x_coeff[20];	/* X coefficients for plate model */
  double	y_coeff[20];	/* Y coefficients for plate model */
  double	xpix;		/* X (RA) coordinate (pixels) */
  double	ypix;		/* Y (dec) coordinate (pixels) */
  double	zpix;		/* Z (face) coordinate (pixels) */
  double	xpos;		/* X (RA) coordinate (deg) */
  double	ypos;		/* Y (dec) coordinate (deg) */
  double	crpix[9];	/* Values of CRPIXn keywords */
  double	crval[9];	/* Values of CRVALn keywords */
  double	cdelt[9];	/* Values of CDELTn keywords */
  double	pc[81];		/* Values of PCiiijjj keywords */
  double	projp[10];	/* Constants for various projections */
  double	longpole;	/* Longitude of North Pole in degrees */
  double	latpole;	/* Latitude of North Pole in degrees */
  double	rodeg;		/* Radius of the projection generating sphere */
  double	imrot;		/* Rotation angle of north pole */
  double	pa_north;	/* Position angle of north (0=horizontal) */
  double	pa_east;	/* Position angle of east (0=horizontal) */
  double	radvel;		/* Radial velocity (km/sec away from observer)*/
  double	zvel;		/* Radial velocity (v/c away from observer)*/
  int		imflip;		/* If not 0, image is reflected around axis */
  int		prjcode;	/* projection code (-1-32) */
  int		latbase;	/* Latitude base 90 (NPA), 0 (LAT), -90 (SPA) */
  int		ncoeff1;	/* Number of x-axis plate fit coefficients */
  int		ncoeff2;	/* Number of y-axis plate fit coefficients */
  int		changesys;	/* 1 for FK4->FK5, 2 for FK5->FK4 */
  				/* 3 for FK4->galactic, 4 for FK5->galactic */
  int		printsys;	/* 1 to print coordinate system, else 0 */
  int		ndec;		/* Number of decimal places in PIX2WCST */
  int		degout;		/* 1 to always print degrees in PIX2WCST */
  int		tabsys;		/* 1 to put tab between RA & Dec, else 0 */
  int		rotmat;		/* 0 if CDELT, CROTA; 1 if CD */
  int		coorflip;	/* 0 if x=RA, y=Dec; 1 if x=Dec, y=RA */
  int		offscl;		/* 0 if OK, 1 if offscale */
  int		wcson;		/* 1 if WCS is set, else 0 */
  int		naxis;		/* Number of axes in image (for WCSLIB 3.0) */
  int		naxes;		/* Number of axes in image */
  int		wcsproj;	/* WCS_OLD: AIPS worldpos() and worldpix()
				   WCS_NEW: Mark Calabretta's WCSLIB subroutines
				   WCS_BEST: WCSLIB for all but CAR,COE,NCP
				   WCS_ALT:  AIPS for all but CAR,COE,NCP */
  int		linmode;	/* 0=system only, 1=units, 2=system+units */
  int		detector;	/* Instrument detector number */
  char		instrument[32];	/* Instrument name */
  char		ctype[9][9];	/* Values of CTYPEn keywords */
  char		c1type[9];	/*  1st coordinate type code:
					RA--, GLON, ELON */
  char		c2type[9];	/*  2nd coordinate type code:
					DEC-, GLAT, ELAT */
  char		ptype[9];	/*  projection type code:
				    SIN, TAN, ARC, NCP, GLS, MER, AIT, etc */
  char		units[9][32];	/* Units if LINEAR */
  char		radecsys[32];	/* Reference frame: FK4, FK4-NO-E, FK5, GAPPT*/
  char		radecout[32];	/* Output reference frame: FK4,FK5,GAL,ECL */
  char		radecin[32];	/* Input reference frame: FK4,FK5,GAL,ECL */
  double	eqin;		/* Input equinox (match sysin if 0.0) */
  double	eqout;		/* Output equinox (match sysout if 0.0) */
  int		sysin;		/* Input coordinate system code */
  int		syswcs;		/* WCS coordinate system code */
  int		sysout;		/* Output coordinate system code */
				/* WCS_B1950, WCS_J2000, WCS_ICRS, WCS_GALACTIC,
				 * WCS_ECLIPTIC, WCS_LINEAR, WCS_ALTAZ  */
  char		center[32];	/* Center coordinates (with frame) */
  struct wcsprm wcsl;		/* WCSLIB main projection parameters */
  struct linprm lin;		/* WCSLIB image/pixel conversion parameters */
  struct celprm cel;		/* WCSLIB projection type */
  struct prjprm prj;		/* WCSLIB projection parameters */
  struct IRAFsurface *lngcor;	/* RA/longitude correction structure */
  struct IRAFsurface *latcor;	/* Dec/latitude correction structure */
  int		distcode;	/* Distortion code 0=none 1=SIRTF */
  struct Distort distort;	/* SIRTF distortion coefficients */
  char *command_format[10];	/* WCS command formats */
				/* where %s is replaced by WCS coordinates */
				/* where %f is replaced by the image filename */
				/* where %x is replaced by image coordinates */
  double	ltm[4];		/* Image rotation matrix */
  double	ltv[2];		/* Image offset */
  int		idpix[2];	/* First pixel to use in image (x, y) */
  int		ndpix[2];	/* Number of pixels to use in image (x, y) */
  struct WorldCoor *wcs;	/* WCS upon which this WCS depends */
  struct WorldCoor *wcsdep;	/* WCS depending on this WCS */
  char		*wcsname;	/* WCS name (defaults to NULL pointer) */
  char		wcschar;	/* WCS character (A-Z, null, space) */
  int		logwcs;		/* 1 if DC-FLAG is set for log wavelength */
};

/* Projections (1-26 are WCSLIB) (values for wcs->prjcode) */
#define WCS_PIX -1	/* Pixel WCS */
#define WCS_LIN  0	/* Linear projection */
#define WCS_AZP  1	/* Zenithal/Azimuthal Perspective */
#define WCS_SZP  2	/* Zenithal/Azimuthal Perspective */
#define WCS_TAN  3	/* Gnomonic = Tangent Plane */
#define WCS_SIN  4	/* Orthographic/synthesis */
#define WCS_STG  5	/* Stereographic */
#define WCS_ARC  6	/* Zenithal/azimuthal equidistant */
#define WCS_ZPN  7	/* Zenithal/azimuthal PolyNomial */
#define WCS_ZEA  8	/* Zenithal/azimuthal Equal Area */
#define WCS_AIR  9	/* Airy */
#define WCS_CYP 10	/* CYlindrical Perspective */
#define WCS_CAR 11	/* Cartesian */
#define WCS_MER 12	/* Mercator */
#define WCS_CEA 13	/* Cylindrical Equal Area */
#define WCS_COP 14	/* Conic PerSpective (COP) */
#define WCS_COD 15	/* COnic equiDistant */
#define WCS_COE 16	/* COnic Equal area */
#define WCS_COO 17	/* COnic Orthomorphic */
#define WCS_BON 18	/* Bonne */
#define WCS_PCO 19	/* Polyconic */
#define WCS_SFL 20	/* Sanson-Flamsteed (GLobal Sinusoidal) */
#define WCS_PAR 21	/* Parabolic */
#define WCS_AIT 22	/* Hammer-Aitoff */
#define WCS_MOL 23	/* Mollweide */
#define WCS_CSC 24	/* COBE quadrilateralized Spherical Cube */
#define WCS_QSC 25	/* Quadrilateralized Spherical Cube */
#define WCS_TSC 26	/* Tangential Spherical Cube */
#define WCS_NCP 27	/* Special case of SIN */
#define WCS_GLS 28	/* Same as SFL */
#define WCS_DSS 29	/* Digitized Sky Survey plate solution */
#define WCS_PLT 30	/* Plate fit polynomials (SAO) */
#define WCS_TNX 31	/* Gnomonic = Tangent Plane (NOAO with corrections) */

/* Coordinate systems */
#define WCS_J2000	1	/* J2000(FK5) right ascension and declination */
#define WCS_B1950	2	/* B1950(FK4) right ascension and declination */
#define WCS_GALACTIC	3	/* Galactic longitude and latitude */
#define WCS_ECLIPTIC	4	/* Ecliptic longitude and latitude */
#define WCS_ALTAZ	5	/* Azimuth and altitude/elevation */
#define WCS_LINEAR	6	/* Linear with optional units */
#define WCS_NPOLE	7	/* Longitude and north polar angle */
#define WCS_SPA		8	/* Longitude and south polar angle */
#define WCS_PLANET	9	/* Longitude and latitude on planet */
#define WCS_XY		10	/* X-Y Cartesian coordinates */
#define WCS_ICRS	11	/* ICRS right ascension and declination */

/* Method to use */
#define WCS_BEST	0	/* Use best WCS projections */
#define WCS_ALT		1	/* Use not best WCS projections */
#define WCS_OLD		2	/* Use AIPS WCS projections */
#define WCS_NEW		3	/* Use WCSLIB 2.5 WCS projections */

/* Distortion codes (values for wcs->distcode) */
#define DISTORT_NONE	0	/* No distortion coefficients */
#define DISTORT_SIRTF	1	/* SIRTF distortion matrix */

#ifndef PI
#define PI	3.141592653589793238462643
#endif

/* pi/(180*3600):  arcseconds to radians */
#define AS2R		4.8481368110953e-6

/* Conversions among hours of RA, degrees and radians. */
#define degrad(x)	((x)*PI/180.)
#define raddeg(x)	((x)*180./PI)
#define hrdeg(x)	((x)*15.)
#define deghr(x)	((x)/15.)
#define hrrad(x)	degrad(hrdeg(x))
#define radhr(x)	deghr(raddeg(x))
#define secrad(x)	((x)*AS2R)

/* TNX surface fitting structure and flags */
struct IRAFsurface {
  double xrange;	/* 2. / (xmax - xmin), polynomials */
  double xmaxmin;	/* - (xmax + xmin) / 2., polynomials */
  double yrange;	/* 2. / (ymax - ymin), polynomials */
  double ymaxmin;	/* - (ymax + ymin) / 2., polynomials */
  int	 type;		/* type of curve to be fitted */
  int    xorder;	/* order of the fit in x */
  int    yorder;	/* order of the fit in y */
  int    xterms;	/* cross terms for polynomials */
  int    ncoeff;	/* total number of coefficients */
  double *coeff;	/* pointer to coefficient vector */
  double *xbasis;	/* pointer to basis functions (all x) */
  double *ybasis;	/* pointer to basis functions (all y) */
};

/* TNX permitted types of surfaces */
#define  TNX_CHEBYSHEV    1
#define  TNX_LEGENDRE     2
#define  TNX_POLYNOMIAL   3

/* TNX cross-terms flags */
#define	TNX_XNONE	0	/* no x-terms (old no) */
#define	TNX_XFULL	1	/* full x-terms (new yes) */
#define	TNX_XHALF	2	/* half x-terms (new) */

#ifdef __cplusplus /* C++ prototypes */
extern "C" {
#endif

#ifdef __STDC__   /* Full ANSI prototypes */

    /* WCS data structure initialization subroutines in wcsinit.c */
    struct WorldCoor *wcsinit ( /* set up WCS structure from a FITS image header */
	const char* hstring);

    struct WorldCoor *wcsninit ( /* set up WCS structure from a FITS image header */
	const char* hstring,	/* FITS header */
	int len);		/* Length of FITS header */

    struct WorldCoor *wcsinitn ( /* set up WCS structure from a FITS image header */
	const char* hstring,	/* FITS header */
	const char* wcsname);	/* WCS name */

    struct WorldCoor *wcsninitn ( /* set up WCS structure from a FITS image header */
	const char* hstring,	/* FITS header */
	int len,		/* Length of FITS header */
	const char* wcsname);	/* WCS name */

    struct WorldCoor *wcsinitc ( /* set up WCS structure from a FITS image header */
	const char* hstring,	/* FITS header */
	char *wcschar);		/* WCS character (A-Z) */

    struct WorldCoor *wcsninitc ( /* set up WCS structure from a FITS image header */
	const char* hstring,	/* FITS header */
	int len,		/* Length of FITS header */
	char *wcschar);		/* WCS character (A-Z) */

    /* WCS subroutines in wcs.c */
    void wcsfree (		/* Free a WCS structure and its contents */
	struct WorldCoor *wcs);	/* World coordinate system structure */

    int wcstype(		/* Set projection type from header CTYPEs */
	struct WorldCoor *wcs,	/* World coordinate system structure */
	char *ctype1,		/* FITS WCS projection for axis 1 */
	char *ctype2);		/* FITS WCS projection for axis 2 */

    int iswcs(		/* Returns 1 if wcs structure set, else 0 */
	struct WorldCoor *wcs);	/* World coordinate system structure */
    int nowcs(		/* Returns 0 if wcs structure set, else 1 */
	struct WorldCoor *wcs);	/* World coordinate system structure */

    int pix2wcst (	/* Convert pixel coordinates to World Coordinate string */
        struct WorldCoor *wcs,  /* World coordinate system structure */
        double xpix, 	/* Image horizontal coordinate in pixels */
        double ypix,	/* Image vertical coordinate in pixels */
        char *wcstring,	/* World coordinate string (returned) */
        int lstr);	/* Length of world coordinate string (returned) */

    void pix2wcs (	/* Convert pixel coordinates to World Coordinates */
        struct WorldCoor *wcs,  /* World coordinate system structure */
        double xpix,	/* Image horizontal coordinate in pixels */	
        double ypix,	/* Image vertical coordinate in pixels */
        double *xpos,	/* Longitude/Right Ascension in degrees (returned) */
        double *ypos);	/* Latitude/Declination in degrees (returned) */

    void wcsc2pix (	/* Convert World Coordinates to pixel coordinates */
        struct WorldCoor *wcs,  /* World coordinate system structure */
        double xpos,	/* Longitude/Right Ascension in degrees */
        double ypos,	/* Latitude/Declination in degrees */
	char *coorsys,	/* Coordinate system (B1950, J2000, etc) */
        double *xpix,	/* Image horizontal coordinate in pixels (returned) */
        double *ypix,	/* Image vertical coordinate in pixels (returned) */
        int *offscl);

    void wcs2pix (	/* Convert World Coordinates to pixel coordinates */
        struct WorldCoor *wcs,  /* World coordinate system structure */
        double xpos,	/* Longitude/Right Ascension in degrees */
        double ypos,	/* Latitude/Declination in degrees */
        double *xpix,	/* Image horizontal coordinate in pixels (returned) */
        double *ypix,	/* Image vertical coordinate in pixels (returned) */
        int *offscl);

    double wcsdist(	/* Compute angular distance between 2 sky positions */
	double ra1,	/* First longitude/right ascension in degrees */
	double dec1,	/* First latitude/declination in degrees */
	double ra2,	/* Second longitude/right ascension in degrees */
	double dec2);	/* Second latitude/declination in degrees */

    double wcsdiff(	/* Compute angular distance between 2 sky positions */
	double ra1,	/* First longitude/right ascension in degrees */
	double dec1,	/* First latitude/declination in degrees */
	double ra2,	/* Second longitude/right ascension in degrees */
	double dec2);	/* Second latitude/declination in degrees */

    struct WorldCoor* wcsxinit( /* set up a WCS structure from arguments */
        double cra,	/* Center right ascension in degrees */
        double cdec,	/* Center declination in degrees */
        double secpix,	/* Number of arcseconds per pixel */
        double xrpix,	/* Reference pixel X coordinate */
        double yrpix,	/* Reference pixel X coordinate */
        int nxpix,	/* Number of pixels along x-axis */
        int nypix,	/* Number of pixels along y-axis */
        double rotate,	/* Rotation angle (clockwise positive) in degrees */
        int equinox,	/* Equinox of coordinates, 1950 and 2000 supported */
        double epoch,	/* Epoch of coordinates, used for FK4/FK5 conversion
                         * no effect if 0 */
        char *proj);	/* Projection */

    struct WorldCoor* wcskinit( /* set up WCS structure from keyword values */
	int naxis1,	/* Number of pixels along x-axis */
	int naxis2,	/* Number of pixels along y-axis */
	char *ctype1,	/* FITS WCS projection for axis 1 */
	char *ctype2,	/* FITS WCS projection for axis 2 */
	double crpix1,	/* Reference pixel coordinates */
	double crpix2,	/* Reference pixel coordinates */
	double crval1,	/* Coordinate at reference pixel in degrees */
	double crval2,	/* Coordinate at reference pixel in degrees */
	double *cd,	/* Rotation matrix, used if not NULL */
	double cdelt1,	/* scale in degrees/pixel, if cd is NULL */
	double cdelt2,	/* scale in degrees/pixel, if cd is NULL */
	double crota,	/* Rotation angle in degrees, if cd is NULL */
	int equinox,	/* Equinox of coordinates, 1950 and 2000 supported */
	double epoch);	/* Epoch of coordinates, for FK4/FK5 conversion */

    void wcsshift(	/* Change center of WCS */
        struct WorldCoor *wcs,  /* World coordinate system structure */
        double cra,	/* New center right ascension in degrees */
        double cdec,	/* New center declination in degrees */
        char *coorsys); /* FK4 or FK5 coordinates (1950 or 2000) */

    void wcsfull(	/* Return RA and Dec of image center, size in degrees */
        struct WorldCoor *wcs,  /* World coordinate system structure */
        double  *cra,	/* Right ascension of image center (deg) (returned) */
        double  *cdec,	/* Declination of image center (deg) (returned) */
        double  *width,	/* Width in degrees (returned) */
        double  *height); /* Height in degrees (returned) */

    void wcscent(	/* Print the image center and size in WCS units */
        struct WorldCoor *wcs); /* World coordinate system structure */

    void wcssize(	/* Return image center and size in RA and Dec */
        struct WorldCoor *wcs,  /* World coordinate system structure */
        double *cra,	/* Right ascension of image center (deg) (returned) */
        double *cdec,	/* Declination of image center (deg) (returned) */
        double *dra,	/* Half-width in right ascension (deg) (returned) */
        double *ddec);	/* Half-width in declination (deg) (returned) */

    void wcsrange(	/* Return min and max RA and Dec of image in degrees */
        struct WorldCoor *wcs,  /* World coordinate system structure */
        double  *ra1,	/* Min. right ascension of image (deg) (returned) */
        double  *ra2,	/* Max. right ascension of image (deg) (returned) */
        double  *dec1,	/* Min. declination of image (deg) (returned) */
        double  *dec2);	/* Max. declination of image (deg) (returned) */

    void wcscdset(	/* Set scaling and rotation from CD matrix */
	struct WorldCoor *wcs,	/* World coordinate system structure */
	double *cd);	/* CD matrix, ignored if NULL */

    void wcsdeltset(	/* set scaling, rotation from CDELTi, CROTA2 */
	struct WorldCoor *wcs,	/* World coordinate system structure */
	double cdelt1,	/* degrees/pixel in first axis (or both axes) */
	double cdelt2,	/* degrees/pixel in second axis if nonzero */
	double crota);	/* Rotation counterclockwise in degrees */

    void wcspcset(	/* set scaling, rotation from CDELTs and PC matrix */
	struct WorldCoor *wcs,	/* World coordinate system structure */
	double cdelt1,	/* degrees/pixel in first axis (or both axes) */
	double cdelt2,	/* degrees/pixel in second axis if nonzero */
	double *pc);	/* Rotation matrix, ignored if NULL */

    void setwcserr(	/* Set WCS error message for later printing */
	char *errmsg);	/* Error mesage < 80 char */
    void wcserr(void);	/* Print WCS error message to stderr */

    void setdefwcs(	/* Set flag to use AIPS WCS instead of WCSLIB */
	int oldwcs);	/* 1 for AIPS WCS subroutines, else WCSLIB */
    int getdefwcs(void);	/* Return flag for AIPS WCS set by setdefwcs */

    char *getradecsys(	/* Return name of image coordinate system */
        struct WorldCoor *wcs);	/* World coordinate system structure */
	
    void wcsoutinit(	/* Set output coordinate system for pix2wcs */
        struct WorldCoor *wcs,	/* World coordinate system structure */
	char *coorsys);	/* Coordinate system (B1950, J2000, etc) */

    char *getwcsout(	/* Return current output coordinate system */
        struct WorldCoor *wcs);	/* World coordinate system structure */

    void wcsininit(	/* Set input coordinate system for wcs2pix */
        struct WorldCoor *wcs,	/* World coordinate system structure */
	char *coorsys);	/* Coordinate system (B1950, J2000, etc) */

    char *getwcsin(	/* Return current input coordinate system */
        struct WorldCoor *wcs);	/* World coordinate system structure */

    int setwcsdeg(	/* Set WCS coordinate output format */
        struct WorldCoor *wcs,	/* World coordinate system structure */
	int degout);	/* 1= degrees, 0= hh:mm:ss dd:mm:ss */

    int wcsndec(	/* Set or get number of output decimal places */
        struct WorldCoor *wcs,	/* World coordinate system structure */
	int ndec);	/* Number of decimal places in output string
			   if < 0, return current ndec unchanged */

    int wcsreset(	/* Change WCS using arguments */
	struct WorldCoor *wcs,	/* World coordinate system data structure */
	double crpix1,	/* Horizontal reference pixel */
	double crpix2,	/* Vertical reference pixel */
	double crval1,	/* Reference pixel horizontal coordinate in degrees */
	double crval2,	/* Reference pixel vertical coordinate in degrees */
	double cdelt1,	/* Horizontal scale in degrees/pixel, ignored if cd is not NULL */
	double cdelt2,	/* Vertical scale in degrees/pixel, ignored if cd is not NULL */
	double crota,	/* Rotation angle in degrees, ignored if cd is not NULL */
	double *cd);	/* Rotation matrix, used if not NULL */

    void wcseqset(	/* Change equinox of reference pixel coordinates in WCS */
	struct WorldCoor *wcs,	/* World coordinate system data structure */
	double equinox);	/* Desired equinox as fractional year */

    void setwcslin(	/* Set pix2wcst() mode for LINEAR coordinates */
        struct WorldCoor *wcs,	/* World coordinate system structure */
	int mode);	/* 0: x y linear, 1: x units x units
			   2: x y linear units */

    int wcszin(		/* Set third dimension for cube projections */
	int izpix);	/* Set coordinate in third dimension (face) */

    int wcszout (	/* Return coordinate in third dimension */
        struct WorldCoor *wcs);	/* World coordinate system structure */

    void wcscominit(	/* Initialize catalog search command set by -wcscom */
	struct WorldCoor *wcs,	/* World coordinate system structure */
	int i,		/* Number of command (0-9) to initialize */
	char *command);	/* command with %s where coordinates will go */

    void wcscom(	/* Execute catalog search command set by -wcscom */
	struct WorldCoor *wcs,	/* World coordinate system structure */
	int i,		/* Number of command (0-9) to execute */
	char *filename,	/* Image file name */
	double xfile,	/* Horizontal image pixel coordinates for WCS command */
	double yfile,	/* Vertical image pixel coordinates for WCS command */
	char *wcstring); /* WCS String from pix2wcst() */

    void savewcscom(	/* Save WCS shell command */
	int i,		/* i of 10 possible shell commands */
	char *wcscom);	/* Shell command using output WCS string */
    char *getwcscom(	/* Return WCS shell command */
	int i);		/* i of 10 possible shell commands */
    void setwcscom(	/* Set WCS shell commands from stored values */
        struct WorldCoor *wcs);	/* World coordinate system structure */
    void freewcscom(	/* Free memory storing WCS shell commands */
        struct WorldCoor *wcs);	/* World coordinate system structure */

    void setwcsfile(	/* Set filename for WCS error message */
	char *filename); /* FITS or IRAF file name */
    int cpwcs (		/* Copy WCS keywords with no suffix to ones with suffix */
	char **header,	/* Pointer to start of FITS header */
	char *cwcs);	/* Keyword suffix character for output WCS */

    void savewcscoor(	/* Save output coordinate system */
	char *wcscoor);	/* coordinate system (J2000, B1950, galactic) */
    char *getwcscoor(void); /* Return output coordinate system */

    /* Coordinate conversion subroutines in wcscon.c */
    void wcsconv(	/* Convert between coordinate systems and equinoxes */
	int sys1,	/* Input coordinate system (J2000, B1950, ECLIPTIC, GALACTIC */
	int sys2,	/* Output coordinate system (J2000, B1950, ECLIPTIC, G ALACTIC */
	double eq1,	/* Input equinox (default of sys1 if 0.0) */
	double eq2,	/* Output equinox (default of sys2 if 0.0) */
	double ep1,	/* Input Besselian epoch in years */
	double ep2,	/* Output Besselian epoch in years */
	double *dtheta,	/* Longitude or right ascension in degrees
			   Input in sys1, returned in sys2 */
	double *dphi,	/* Latitude or declination in degrees
			   Input in sys1, returned in sys2 */
	double *ptheta,	/* Longitude or right ascension proper motion in deg/year
			   Input in sys1, returned in sys2 */
	double *pphi,	/* Latitude or declination proper motion in deg/year */
	double *px,	/* Parallax in arcseconds */
	double *rv);	/* Radial velocity in km/sec */
    void wcsconp(	/* Convert between coordinate systems and equinoxes */
	int sys1,	/* Input coordinate system (J2000, B1950, ECLIPTIC, GALACTIC */
	int sys2,	/* Output coordinate system (J2000, B1950, ECLIPTIC, G ALACTIC */
	double eq1,	/* Input equinox (default of sys1 if 0.0) */
	double eq2,	/* Output equinox (default of sys2 if 0.0) */
	double ep1,	/* Input Besselian epoch in years */
	double ep2,	/* Output Besselian epoch in years */
	double *dtheta,	/* Longitude or right ascension in degrees
			   Input in sys1, returned in sys2 */
	double *dphi,	/* Latitude or declination in degrees
			   Input in sys1, returned in sys2 */
	double *ptheta,	/* Longitude or right ascension proper motion in degrees/year
			   Input in sys1, returned in sys2 */
	double *pphi);	/* Latitude or declination proper motion in degrees/year
			   Input in sys1, returned in sys2 */
    void wcscon(	/* Convert between coordinate systems and equinoxes */
	int sys1,	/* Input coordinate system (J2000, B1950, ECLIPTIC, GALACTIC */
	int sys2,	/* Output coordinate system (J2000, B1950, ECLIPTIC, G ALACTIC */
	double eq1,	/* Input equinox (default of sys1 if 0.0) */
	double eq2,	/* Output equinox (default of sys2 if 0.0) */
	double *dtheta,	/* Longitude or right ascension in degrees
			   Input in sys1, returned in sys2 */
	double *dphi,	/* Latitude or declination in degrees
			   Input in sys1, returned in sys2 */
	double epoch);	/* Besselian epoch in years */
    void fk425e (	/* Convert B1950(FK4) to J2000(FK5) coordinates */
	double *ra,	/* Right ascension in degrees (B1950 in, J2000 out) */
	double *dec,	/* Declination in degrees (B1950 in, J2000 out) */
	double epoch);	/* Besselian epoch in years */
    void fk524e (	/* Convert J2000(FK5) to B1950(FK4) coordinates */
	double *ra,	/* Right ascension in degrees (J2000 in, B1950 out) */
	double *dec,	/* Declination in degrees (J2000 in, B1950 out) */
	double epoch);	/* Besselian epoch in years */
    int wcscsys(	/* Return code for coordinate system in string */
	char *coorsys);	 /* Coordinate system (B1950, J2000, etc) */
    double wcsceq (	/* Set equinox from string (return 0.0 if not obvious) */
	char *wcstring);  /* Coordinate system (B1950, J2000, etc) */
    void wcscstr (	/* Set coordinate system type string from system and equinox */
	char   *cstr,	 /* Coordinate system string (returned) */
	int    syswcs,	/* Coordinate system code */
	double equinox,	/* Equinox of coordinate system */
	double epoch);	/* Epoch of coordinate system */
    void d2v3 (		/* Convert RA and Dec in degrees and distance to vector */
	double	rra,	/* Right ascension in degrees */
	double	rdec,	/* Declination in degrees */
	double	r,	/* Distance to object in same units as pos */
	double pos[3]);	/* x,y,z geocentric equatorial position of object (returned) */
    void s2v3 (		/* Convert RA and Dec in radians and distance to vector */
	double	rra,	/* Right ascension in radians */
	double	rdec,	/* Declination in radians */
	double	r,	/* Distance to object in same units as pos */
	double pos[3]);	/* x,y,z geocentric equatorial position of object (returned) */
    void v2d3 (		/* Convert vector to RA and Dec in degrees and distance */
	double	pos[3],	/* x,y,z geocentric equatorial position of object */
	double	*rra,	/* Right ascension in degrees (returned) */
	double	*rdec,	/* Declination in degrees (returned) */
	double	*r);	/* Distance to object in same units as pos (returned) */
    void v2s3 (		/* Convert vector to RA and Dec in radians and distance */
	double	pos[3],	/* x,y,z geocentric equatorial position of object */
	double	*rra,	/* Right ascension in radians (returned) */
	double	*rdec,	/* Declination in radians (returned) */
	double	*r);	/* Distance to object in same units as pos (returned) */

/* Distortion model subroutines in distort.c */
    void distortinit (	/* Set distortion coefficients from FITS header */
	struct WorldCoor *wcs,	/* World coordinate system structure */
	const char* hstring);	/* FITS header */
    void setdistcode (	/* Set WCS distortion code string from CTYPEi value */
	struct WorldCoor *wcs,	/* World coordinate system structure */
	char	*ctype);	/* CTYPE value from FITS header */
    char *getdistcode (	/* Return distortion code string for CTYPEi */
	struct WorldCoor *wcs);	/* World coordinate system structure */
    int DelDistort (	/* Delete all distortion-related fields */
	char *header,	/* FITS header */
	int verbose);	/* If !=0, print keywords as deleted */
    void pix2foc (	/* Convert pixel to focal plane coordinates */
	struct WorldCoor *wcs,	/* World coordinate system structure */
	double x,	/* Image pixel horizontal coordinate */
	double y,	/* Image pixel vertical coordinate */
	double *u,	/* Focal plane horizontal coordinate(returned) */
	double *v);	/* Focal plane vertical coordinate (returned) */
    void foc2pix (	/* Convert focal plane to pixel coordinates */
	struct WorldCoor *wcs,	/* World coordinate system structure */
	double u,	/* Focal plane horizontal coordinate */
	double v,	/* Focal plane vertical coordinate */
	double *x,	/* Image pixel horizontal coordinate(returned) */
	double *y);	/* Image pixel vertical coordinate (returned) */

/* Other projection subroutines */

/* 8 projections using AIPS algorithms (worldpos.c) */
    int worldpos (	/* Convert from pixel location to RA,Dec */
	double xpix,	/* x pixel number  (RA or long without rotation) */
	double ypix,	/* y pixel number  (Dec or lat without rotation) */
	struct WorldCoor *wcs, /* WCS parameter structure */
	double *xpos,	/* x (RA) coordinate (deg) (returned) */
	double *ypos);	/* y (dec) coordinate (deg) (returned) */
    int worldpix (	/* Convert from RA,Dec to pixel location */
	double xpos,	/* x (RA) coordinate (deg) */
	double ypos,	/* y (dec) coordinate (deg) */
	struct WorldCoor *wcs, /* WCS parameter structure */
	double *xpix,	/* x pixel number (RA or long without rotation) */
	double *ypix);	/* y pixel number (dec or lat without rotation) */

/* Digital Sky Survey projection (dsspos.c) */
    int dsspos (	/* Convert from pixel location to RA,Dec */
	double xpix,	/* x pixel number  (RA or long without rotation) */
	double ypix,	/* y pixel number  (Dec or lat without rotation) */
	struct WorldCoor *wcs, /* WCS parameter structure */
	double *xpos,	/* x (RA) coordinate (deg) (returned) */
	double *ypos);	/* y (dec) coordinate (deg) (returned) */
    int dsspix (	/* Convert from RA,Dec to pixel location */
	double xpos,	/* x (RA) coordinate (deg) */
	double ypos,	/* y (dec) coordinate (deg) */
	struct WorldCoor *wcs, /* WCS parameter structure */
	double *xpix,	/* x pixel number (RA or long without rotation) */
	double *ypix);	/* y pixel number (dec or lat without rotation) */

/* SAO TDC TAN projection with higher order terms (platepos.c) */
    int platepos (	/* Convert from pixel location to RA,Dec */
	double xpix,	/* x pixel number  (RA or long without rotation) */
	double ypix,	/* y pixel number  (Dec or lat without rotation) */
	struct WorldCoor *wcs, /* WCS parameter structure */
	double *xpos,	/* x (RA) coordinate (deg) (returned) */
	double *ypos);	/* y (dec) coordinate (deg) (returned) */
    int platepix (	/* Convert from RA,Dec to pixel location */
	double xpos,	/* x (RA) coordinate (deg) */
	double ypos,	/* y (dec) coordinate (deg) */
	struct WorldCoor *wcs, /* WCS parameter structure */
	double *xpix,	/* x pixel number (RA or long without rotation) */
	double *ypix);	/* y pixel number (dec or lat without rotation) */
    void SetFITSPlate (	/* Set FITS header plate fit coefficients from structure */
	char *header,	/* Image FITS header */
	struct WorldCoor *wcs); /* WCS structure */
    int SetPlate (	/* Set plate fit coefficients in structure from arguments */
	struct WorldCoor *wcs, /* World coordinate system structure */
	int ncoeff1,	/* Number of coefficients for x */
	int ncoeff2,	/* Number of coefficients for y */
	double *coeff);	/* Plate fit coefficients */
    int GetPlate (	/* Return plate fit coefficients from structure in arguments */
	struct WorldCoor *wcs, /* World coordinate system structure */
	int *ncoeff1,	/* Number of coefficients for x */
	int *ncoeff2,	/* Number of coefficients for y) */
	double *coeff);	/* Plate fit coefficients */

/* IRAF TAN projection with higher order terms (tnxpos.c) */
    int tnxinit (	/* initialize the gnomonic forward or inverse transform */
	const char *header, /* FITS header */
	struct WorldCoor *wcs); /* pointer to WCS structure */
    int tnxpos (	/* forward transform (physical to world) gnomonic projection. */
	double xpix,	/* Image X coordinate */
	double ypix,	/* Image Y coordinate */
	struct WorldCoor *wcs, /* pointer to WCS descriptor */
	double *xpos,	/* Right ascension (returned) */
	double *ypos);	/* Declination (returned) */
    int tnxpix (	/* Inverse transform (world to physical) gnomonic projection */
	double xpos,     /* Right ascension */
	double ypos,     /* Declination */
	struct WorldCoor *wcs, /* Pointer to WCS descriptor */
	double *xpix,	/* Image X coordinate (returned) */
	double *ypix);	/* Image Y coordinate (returned) */


#else /* K&R prototypes */

/* WCS subroutines in wcs.c */
struct WorldCoor *wcsinit(); /* set up a WCS structure from a FITS image header */
struct WorldCoor *wcsninit(); /* set up a WCS structure from a FITS image header */
struct WorldCoor *wcsinitn(); /* set up a WCS structure from a FITS image header */
struct WorldCoor *wcsninitn(); /* set up a WCS structure from a FITS image header */
struct WorldCoor *wcsinitc(); /* set up a WCS structure from a FITS image header */
struct WorldCoor *wcsninitc(); /* set up a WCS structure from a FITS image header */
struct WorldCoor *wcsxinit(); /* set up a WCS structure from arguments */
struct WorldCoor *wcskinit(); /* set up a WCS structure from keyword values */
void wcsfree(void);		/* Free a WCS structure and its contents */
int wcstype();		/* Set projection type from header CTYPEs */
void wcscdset();	/* Set scaling and rotation from CD matrix */
void wcsdeltset();	/* set scaling and rotation from CDELTs and CROTA2 */
void wcspcset();	/* set scaling and rotation from CDELTs and PC matrix */
int iswcs();		/* Return 1 if WCS structure is filled, else 0 */
int nowcs();		/* Return 0 if WCS structure is filled, else 1 */
void wcsshift();	/* Reset the center of a WCS structure */
void wcscent();		/* Print the image center and size in WCS units */
void wcssize();		/* Return RA and Dec of image center, size in RA and Dec */
void wcsfull();		/* Return RA and Dec of image center, size in degrees */
void wcsrange();	/* Return min and max RA and Dec of image in degrees */
double wcsdist();	/* Distance in degrees between two sky coordinates */
double wcsdiff();	/* Distance in degrees between two sky coordinates */
void wcscominit();	/* Initialize catalog search command set by -wcscom */
void wcscom();		/* Execute catalog search command set by -wcscom */
char *getradecsys();	/* Return current value of coordinate system */
void wcsoutinit();	/* Initialize WCS output coordinate system for use by pix2wcs */
char *getwcsout();	/* Return current value of WCS output coordinate system */
void wcsininit();	/* Initialize WCS input coordinate system for use by wcs2pix */
char *getwcsin();	/* Return current value of WCS input coordinate system */
int setwcsdeg();	/* Set WCS output in degrees (1) or hh:mm:ss dd:mm:ss (0) */
int wcsndec();		/* Set or get number of output decimal places */
int wcsreset();		/* Change WCS using arguments */
void wcseqset();	/* Change equinox of reference pixel coordinates in WCS */
void wcscstr();		/* Return system string from system code, equinox, epoch */
void setwcslin();	/* Set output string mode for LINEAR coordinates */
int pix2wcst();		/* Convert pixel coordinates to World Coordinate string */
void pix2wcs();		/* Convert pixel coordinates to World Coordinates */
void wcsc2pix();	/* Convert World Coordinates to pixel coordinates */
void wcs2pix();		/* Convert World Coordinates to pixel coordinates */
void setdefwcs();	/* Call to use AIPS classic WCS (also not PLT or TNX */
int getdefwcs();	/* Call to get flag for AIPS classic WCS */
int wcszin();		/* Set coordinate in third dimension (face) */
int wcszout();		/* Return coordinate in third dimension */
void wcserr();		/* Print WCS error message to stderr */
void setwcserr();	/* Set WCS error message for later printing */
void savewcscoor();	/* Save output coordinate system */
char *getwcscoor();	/* Return output coordinate system */
void savewcscom();	/* Save WCS shell command */
char *getwcscom();	/* Return WCS shell command */
void setwcscom();	/* Set WCS shell commands from stored values */
void freewcscom();	/* Free memory used to store WCS shell commands */
void setwcsfile();	/* Set filename for WCS error message */
int cpwcs();		/* Copy WCS keywords with no suffix to ones with suffix */

/* Coordinate conversion subroutines in wcscon.c */
void wcscon();		/* Convert between coordinate systems and equinoxes */
void wcsconp();		/* Convert between coordinate systems and equinoxes */
void wcsconv();		/* Convert between coordinate systems and equinoxes */
void fk425e();		/* Convert B1950(FK4) to J2000(FK5) coordinates */
void fk524e();		/* Convert J2000(FK5) to B1950(FK4) coordinates */
int wcscsys();		/* Set coordinate system from string */
double wcsceq();	/* Set equinox from string (return 0.0 if not obvious) */
void d2v3();		/* Convert RA and Dec in degrees and distance to vector */
void s2v3();		/* Convert RA and Dec in radians and distance to vector */
void v2d3();		/* Convert vector to RA and Dec in degrees and distance */
void v2s3();		/* Convert vector to RA and Dec in radians and distance */

/* Distortion model subroutines in distort.c */
void distortinit();	/* Set distortion coefficients from FITS header */
void setdistcode();	/* Set WCS distortion code string from CTYPEi value */
char *getdistcode();	/* Return distortion code string for CTYPEi */
int DelDistort();	/* Delete all distortion-related fields */
void pix2foc();		/*  pixel coordinates -> focal plane coordinates */
void foc2pix();		/*  focal plane coordinates -> pixel coordinates */

/* Other projection subroutines */

/* 8 projections using AIPS algorithms (worldpos.c) */
extern int worldpos();	/* Convert from pixel location to RA,Dec */
extern int worldpix();	/* Convert from RA,Dec to pixel location */

/* Digital Sky Survey projection (dsspos.c) */
extern int dsspos();	/* Convert from pixel location to RA,Dec */
extern int dsspix();	/* Convert from RA,Dec to pixel location */

/* SAO TDC TAN projection with higher order terms (platepos.c) */
extern int platepos();	/* Convert from pixel location to RA,Dec */
extern int platepix();	/* Convert from RA,Dec to pixel location */
extern void SetFITSPlate(); /* Set FITS header plate fit coefficients from structure */
extern int SetPlate();	/* Set plate fit coefficients in structure from arguments */
extern int GetPlate();	/* Return plate fit coefficients from structure in arguments */

/* IRAF TAN projection with higher order terms (tnxpos.c) */
extern int tnxinit();	/* initialize the gnomonic forward or inverse transform */
extern int tnxpos();	/* forward transform (physical to world) gnomonic projection. */
extern int tnxpix();	/* Inverse transform (world to physical) gnomonic projection */

#endif	/* __STDC__ */

#ifdef __cplusplus
}
#endif

#endif	/* _wcs_h_ */

/* Oct 26 1994	New file
 * Dec 21 1994	Add rotation matrix
 * Dec 22 1994	Add flag for coordinate reversal

 * Mar  6 1995	Add parameters for Digital Sky Survey plate fit
 * Jun  8 1995	Add parameters for coordinate system change
 * Jun 21 1995	Add parameter for plate scale
 * Jul  6 1995	Add parameter to note whether WCS is set
 * Aug  8 1995	Add parameter to note whether to print coordinate system
 * Oct 16 1995	Add parameters to save image dimensions and center coordinates

 * Feb 15 1996	Add coordinate conversion functions
 * Feb 20 1996	Add flag for tab tables
 * Apr 26 1996	Add epoch of positions (actual date of image)
 * Jul  5 1996	Add subroutine declarations
 * Jul 19 1996	Add WCSFULL declaration
 * Aug  5 1996	Add WCSNINIT to initialize WCS for non-terminated header
 * Oct 31 1996	Add DCnn inverse rotation matrix
 * Nov  1 1996	Add NDEC number of decimal places in output
 *
 * May 22 1997	Change range of pcode from 1-8 to -1-8 for linear transform
 * Sep 12 1997	Add chip rotation MROT, XMPIX, YMPIX
 *
 * Jan  7 1998	Add INSTRUME and DETECTOR for HST metric correction
 * Jan 16 1998	Add Mark Calabretta's WCSLIB data structures
 * Jan 16 1998	Add LONGPOLE, LATPOLE, and PROJP constants for Calabretta
 * Jan 22 1998	Add ctype[], crpix[], crval[], and cdelt[] for Calabretta
 * Jan 23 1998	Change wcsset() to wcsxinit() and pcode to prjcode
 * Jan 23 1998	Define projection type flags
 * Jan 26 1998	Remove chip rotation
 * Jan 26 1998	Add chip correction polynomial
 * Feb  3 1998	Add number of coefficients for residual fit
 * Feb  5 1998	Make cd and dc matrices vectors, not individual elements
 * Feb 19 1998	Add projection names
 * Feb 23 1998	Add TNX projection from NOAO
 * Mar  3 1998	Add NOAO plate fit and residual fit
 * Mar 12 1998	Add variables for TNX correction surface
 * Mar 23 1998	Add PLT plate fit polynomial projection; reassign DSS
 * Mar 23 1998	Drop plate_fit flag from structure
 * Mar 25 1998	Add npcoeff to wcs structure for new plate fit WCS
 * Apr  7 1998	Change amd_i_coeff to i_coeff
 * Apr  8 1998	Add wcseqset() and wcsreset() subroutine declarations
 * Apr 10 1998	Rearrange order of nonstandard WCS types
 * Apr 13 1998	Add setdefwcs() subroutine declaration
 * Apr 14 1998	Add coordinate systems and wcscoor()
 * Apr 24 1998	Add units
 * Apr 28 1998	Change coordinate system flags to WCS_*
 * Apr 28 1998	Change projection flags to WCS_*
 * Apr 28 1998	Add wcsc2pix()
 * May  7 1998	Add C++ declarations
 * May 13 1998	Add eqin and eqout for conversions to and from equinoxes
 * May 14 1998	Add declarations for coordinate conversion subroutines
 * May 27 1998	Add blsearch()
 * May 27 1998	Change linear projection back to WCS_LIN from WCS_LPR
 * May 27 1998	Move hget.c and hput.c C++ declarations to fitshead.h
 * May 27 1998	Include fitshead.h
 * May 29 1998	Add wcskinit()
 * Jun  1 1998	Add wcserr()
 * Jun 11 1998	Add initialization support subroutines
 * Jun 18 1998	Add wcspcset()
 * Jun 25 1998	Add wcsndec()
 * Jul  6 1998	Add wcszin() and wcszout() to use third dimension of images
 * Jul  7 1998	Change setdegout() to setwcsdeg(); setlinmode() to setwcslin()
 * Jul 17 1998	Add savewcscoor(), getwcscoor(), savewcscom(), and getwcscom()
 * Aug 14 1998	Add freewcscom(), setwcscom(), and multiple WCS commands
 * Sep  3 1998	Add pa_north, pa_east, imrot and imflip to wcs structure
 * Sep 14 1998	Add latbase for AXAF North Polar angle (NPOL not LAT-)
 * Sep 16 1998	Make WCS_system start at 1; add NPOLE
 * Sep 17 1998	Add wcscstr()
 * Sep 21 1998	Add wcsconp() to convert proper motions, too.
 * Dec  2 1998	Add WCS type for planet surface

 * Jan 20 1999	Add declaration of wcsfree()
 * Jun 16 1999	Add declaration of wcsrange()
 * Oct 21 1999	Add declaration of setwcsfile()
 *
 * Jan 28 2000	Add flags for choice of WCS projection subroutines
 * Jun 26 2000	Add XY coordinate system
 * Nov  2 2000	Add wcsconv() to convert coordinates when parallax or rv known
 *
 * Jan 17 2001	Add idpix and ndpix for trim section, ltm for readout rotation
 * Jan 31 2001	Add wcsinitn(), wcsninitn(), wcsinitc(), and wcsninitc()
 * Feb 20 2001	Add wcs->wcs to main data structure
 * Mar 20 2001	Close unclosed comment in wcsconv() argument list
 *
 * Apr  3 2002	Add SZP and second GLS/SFL projection
 * Apr  9 2002	Add wcs->wcsdep for pointer to WCS depending on this WCS
 * Apr 26 2002	Add wcs->wcsname and wcs->wcschar to identify WCS structure
 * May  9 2002	Add wcs->radvel and wcs->zvel for radial velocity in km/sec
 *
 * Apr  1 2003	Add wcs->distort Distort structure for distortion correction
 * Apr  1 2003	Add foc2pix() and pix2foc() subroutines for distortion correction
 * May  1 2003	Add missing semicolons after C++ declarations of previous two functions
 * Oct  1 2003	Rename wcs->naxes to wcs->naxis to match WCSLIB 3.2
 * Nov  3 2003	Add distinit(), setdistcode(), and getdistcode() to distort.c
 * Dec  3 2003	Add back wcs->naxes for backward compatibility
 *
 * Aug 30 2004	Add DelDistort()
 *
 * Nov  1 2005	Add WCS_ICRS
 *
 * Jan  5 2006	Add secrad()
 * Apr 21 2006	Increase maximum number of axes from 4 to 8
 * Apr 24 2006	Increase maximum number of axes to 9
 * Nov 29 2006	Drop semicolon at end of C++ ifdef
 * Dec 21 2006	Add cpwcs()
 *
 * Jan  4 2007	Drop extra declaration of wcscstr()
 * Jan  4 2007	Fix declarations so ANSI prototypes are not just for C++
 * Jan  9 2007	Add fk425e() and fk524e() subroutines
 * Jan  9 2007	Add worldpos.c, dsspos.c, platepos.c, and tnxpos.c subroutines
 * Jan 10 2007	Add ANSI prototypes for all subroutines
 * Feb  1 2007	Add wcs.wcslog for log wavelength
 * Jul 25 2007	Add v2s3(), s2v3(), d2v3(), v2d3() for coordinate-vector conversion
 */
