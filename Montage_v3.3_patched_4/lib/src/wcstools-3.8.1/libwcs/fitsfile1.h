/*** File fitsfile.h  FITS and IRAF file access subroutines
 *** January 5, 2007
 *** By Doug Mink, dmink@cfa.harvard.edu
 *** Harvard-Smithsonian Center for Astrophysics
 *** Copyright (C) 1996-2007
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

#ifndef fitsfile_h_
#define fitsfile_h_
#include "fitshead.h"

/* FITS table keyword structure */
struct Keyword {
    char kname[10];	/* Keyword for table entry */
    int lname;		/* Length of keyword name */
    int kn;		/* Index of entry on line */
    int kf;		/* Index in line of first character of entry */
    int kl;		/* Length of entry value */
};

#define FITSBLOCK 2880

/* Declarations for subroutines in fitsfile.c, imhfile.c, imio.c,
 * fileutil.c, and dateutil.c */

/* FITS file access subroutines in fitsfile.c */
    int fitsropen(	/* Open a FITS file for reading, returning a FILE pointer */
	char *inpath);	/* Pathname for FITS tables file to read */
    char *fitsrhead(	/* Read a FITS header */
	char *filename,	/* Name of FITS image file */
	int *lhead,	/* Allocated length of FITS header in bytes (returned) */
	int *nbhead);	/* Number of bytes before start of data (returned) */
    char *fitsrtail(	/* Read FITS header appended to graphics file */
	char *filename,	/* Name of FITS image file */
	int *lhead,	/* Allocated length of FITS header in bytes (returned) */
	int *nbhead);	/* Number of bytes before start of data (returned) */
    char *fitsrimage(	/* Read a FITS image */
	char *filename,	/* Name of FITS image file */
	int *nbhead),	/* Actual length of image header(s) in bytes */
	char *header,	/* FITS header for image (previously read) */
    char *fitsrfull(	/* Read a FITS image of any dimension */
	char *filename,	/* Name of FITS image file */
	int *nbhead),	/* Actual length of image header(s) in bytes */
	char *header,	/* FITS header for image (previously read) */
    char *fitsrsect(	/* Read a piece of a FITS image, header */
	char *filename,	/* Name of FITS image file */
	char *header,	/* FITS header for image (previously read) */
	int *nbhead,	/* Actual length of image header(s) in bytes */
	int x0, y0, 	/* FITS image coordinate of first pixel */
	int nx,		/* Number of columns to read (less than NAXIS1) */
	int ny,		/* Number of rows to read (less than NAXIS2) */
	int nlog);	/* Note progress mod this rows */
    int fitswhead(	/* Write FITS header; keep file open for further writing */
	char *filename,	/* Name of FITS image file */
	char *header);	/* FITS header for image (previously read) */
    int fitswexhead(	/* Write FITS header in place */
	char *filename,	/* Name of FITS image file */
	char *header);	/* FITS header for image */
    int fitswext(	/* Write FITS header and image as extension to a file */
	char *filename,	/* Name of FITS image file */
	char *header,	/* FITS image header */
	char *image);	/* FITS image pixels */
    int fitswhdu(	/* Write FITS head and image as extension */
	int fd,		/* File descriptor */
	char *filename,	/* Name of FITS image file */
	char *header,	/* FITS image header */
	char *image);	/* FITS image pixels */
    int fitswimage(	/* Write FITS header and image */
	char *filename,	/* Name of FITS image file */
	char *header,	/* FITS image header */
	char *image);	/* FITS image pixels */
    int fitscimage(	/* Write FITS header and copy FITS image */
	char *filename,	/* Name of output FITS image file */
	char *header,	/* FITS image header */
	char *filename0); /* Name of input FITS image file */
    int isfits(		/* Return 1 if file is a FITS file */
	char *filename); /* Name of file to check */
    void fitserr();	/* Print FITS error message to stderr */
    void setfitsinherit( /* Set flag to append primary data header */
	int inh);	/* 1 to inherit primary data header, else 0 */
    int fitsheadsize(	/* Return size of fitsheader in bytes */
	char *header);	/* FITS image header */
	
/* FITS table file access subroutines in fitsfile.c */

    int fitsrtopen(	/* Open FITS table file and fill structure with
			 * pointers to selected keywords
			 * Return file descriptor (-1 if unsuccessful) */
	char *inpath,	/* Pathname for FITS tables file to read */
	int *nk,	/* Number of keywords to use */
	struct Keyword **kw, /* Structure for desired entries */
	int *nrows,	/* Number of rows in table (returned) */
	int *nchar,	/* Number of characters in one table row (returned) */
	int *nbhead);	/* Number of characters before table starts */
    int fitsrthead(	/* Read pointers to selected keywords
			 * from FITS table header */
	char *header,	/* Header for FITS tables file */
	int *nk,	/* Number of keywords to use */
	struct Keyword **kw, /* Structure for desired entries */
	int *nrows,	/* Number of rows in table (returned) */
	int *nchar);	/* Number of characters in one table row (returned) */
    void fitsrtlset();	/* Reset FITS Table buffer limits from start of data */
    int fitsrtline(	/* Return specified line of FITS table */
	int fd,		/* File descriptor for FITS file */
	int nbhead,	/* Number of bytes in FITS header */
	int lbuff,	/* Number of bytes in table buffer */
	char *tbuff,	/* FITS table buffer */
	int irow,	/* Number of table row to read */
	int nbline,	/* Number of bytes to read for this line */
	char *line);	/* One line of FITS table (returned) */
short ftgeti2(		/* Extract column for keyword from FITS table line
			 * as short */
	char *entry,	/* Row or entry from table */
	struct Keyword *kw); /* Table column information from FITS header */
    int ftgeti4(	/* Extract column for keyword from FITS table line
			 * as int */
	char *entry,	/* Row or entry from table */
	struct Keyword *kw); /* Table column information from FITS header */
float ftgetr4(		/* Extract column for keyword from FITS table line
			 * as float */
	char *entry,	/* Row or entry from table */
	struct Keyword *kw); /* Table column information from FITS header */
    double ftgetr8(	/* Extract column for keyword from FITS table line
			 * as double */
	char *entry,	/* Row or entry from table */
	struct Keyword *kw); /* Table column information from FITS header */
    int ftgetc(		/* Extract column for keyword from FITS table line
			 * as char string */
	char *entry,	/* Row or entry from table */
	struct Keyword *kw, /* Table column information from FITS header */
	char *string,	/* Returned string */
	int maxchar);	/* Maximum number of characters in returned string */

/* IRAF file access subroutines in imhfile.c */

    char *irafrhead(	/* Read IRAF .imh header file and translate to FITS header */
	char *filename,	/* Name of IRAF header file */
	int *lihead);	/* Length of IRAF image header in bytes (returned) */
    char *irafrimage(	/* Read IRAF image pixels (call after irafrhead) */
	char *fitsheader); /* FITS image header (filled) */
    int irafwhead(	/* Write IRAF .imh header file */
	char *hdrname,	/* Name of IRAF header file */
	int lhead,	/* Length of IRAF header */
	char *irafheader, /* IRAF header */
	char *fitsheader); /* FITS image header */
    int irafwimage(	/* Write IRAF .imh header file and .pix image file */
	char *hdrname,	/* Name of IRAF header file */
	int lhead,	/* Length of IRAF header */
	char *irafheader, /* IRAF header */
	char *fitsheader, /* FITS image header */
	char *image);	/* IRAF image */
    int isiraf(		/* return 1 if IRAF imh file, else 0 */
	char *filename); /* Name of file to check */
    char *iraf2fits(	/* Convert IRAF image header to FITS image header,
			 * returning FITS header */
	char *hdrname,	/* IRAF header file name (may be path) */
	char *irafheader, /* IRAF image header */
	int nbiraf,	/* Number of bytes in IRAF header */
	int *nbfits);	/* Number of bytes in FITS header (returned) */

    char *fits2iraf(	/* Convert FITS image header to IRAF image header,
			 * returning IRAF header */
	char *fitsheader, /* FITS image header */
	char *irafheader, /* IRAF image header (returned updated) */
	int nbhead,	/* Length of IRAF header */
	int *nbiraf);	/* Length of returned IRAF header */

/* Image pixel access subroutines in imio.c */

    double getpix(	/* Read one pixel from any data type 2-D array (0,0)*/
	char *image,	/* Image array as 1-D vector */
	int bitpix,	/* FITS bits per pixel
			 *  16 = short, -16 = unsigned short, 32 = int
			 * -32 = float, -64 = double */
	int w;		/* Image width in pixels */
	int h;		/* Image height in pixels */
	double bzero,	/* Zero point for pixel scaling */
	double bscale,	/* Scale factor for pixel scaling */
	int x,		/* Zero-based horizontal pixel number */
	int y);		/* Zero-based vertical pixel number */
    double getpix1(	/* Read one pixel from any data type 2-D array (1,1)*/
	char *image,	/* Image array as 1-D vector */
	int bitpix,	/* FITS bits per pixel */
	int w;		/* Image width in pixels */
	int h;		/* Image height in pixels */
	double bzero,	/* Zero point for pixel scaling */
	double bscale,	/* Scale factor for pixel scaling */
	int x,		/* One-based horizontal pixel number */
	int y);		/* One-based vertical pixel number */
    double maxvec(	/* Get maximum value in vector from a image */
	char *image,	/* Image array from which to extract vector */
	int bitpix,	/* Number of bits per pixel in image */
	double bzero,	/* Zero point for pixel scaling */
	double bscale,	/* Scale factor for pixel scaling */
	int pix1,	/* Offset of first pixel to extract */
	int npix);	/* Number of pixels to extract */
    void putpix(	/* Write one pixel to any data type 2-D array (0,0)*/
	char *image,	/* Image array as 1-D vector */
	int bitpix,	/* FITS bits per pixel */
	int w;		/* Image width in pixels */
	int h;		/* Image height in pixels */
	double bzero,	/* Zero point for pixel scaling */
	double bscale,	/* Scale factor for pixel scaling */
	int x,		/* Zero-based horizontal pixel number */
	int y,		/* Zero-based vertical pixel number */
	double dpix);	/* Value to put into image pixel */
    void putpix1(	/* Write one pixel to any data type 2-D array (1,1) */
	char *image,	/* Image array as 1-D vector */
	int bitpix,	/* FITS bits per pixel */
	int w;		/* Image width in pixels */
	int h;		/* Image height in pixels */
	double bzero,	/* Zero point for pixel scaling */
	double bscale,	/* Scale factor for pixel scaling */
	int x,		/* One-based horizontal pixel number */
	int y,		/* One-based vertical pixel number */
	double dpix);	/* Value to put into image pixel */
    void addpix(	/* Add to one pixel in any data type 2-D array (0,0)*/
	char *image,	/* Image array as 1-D vector */
	int bitpix,	/* FITS bits per pixel */
	int w;		/* Image width in pixels */
	int h;		/* Image height in pixels */
	double bzero,	/* Zero point for pixel scaling */
	double bscale,	/* Scale factor for pixel scaling */
	int x,		/* Zero-based horizontal pixel number */
	int y,		/* Zero-based vertical pixel number */
	double dpix);	/* Value to add to image pixel */
    void addpix1(	/* Add to one pixel in any data type 2-D array (1,1)*/
	char *image,	/* Image array as 1-D vector */
	int bitpix,	/* FITS bits per pixel */
	int w;		/* Image width in pixels */
	int h;		/* Image height in pixels */
	double bzero,	/* Zero point for pixel scaling */
	double bscale,	/* Scale factor for pixel scaling */
	int x,		/* One-based horizontal pixel number */
	int y,		/* One-based vertical pixel number */
	double dpix);	/* Value to add to image pixel */
    void movepix(	/* Move one pixel value between two 2-D arrays (0,0) */
	char *image1,	/* Pointer to first pixel in input image */
	int bitpix1,	/* Bits per input pixel (FITS codes) */
	int w1,		/* Number of horizontal pixels in input image */
	int x1, y1);	/* Zero-based row and column for input pixel */
	char *image2,	/* Pointer to first pixel in output image */
	int bitpix2,	/* Bits per output pixel (FITS codes) */
	int w2,		/* Number of horizontal pixels in output image */
	int x2, y2);	/* Zero-based row and column for output pixel */
    void movepix1(	/* Move one pixel value between two 2-D arrays (1,1) */
	char *image1,	/* Pointer to first pixel in input image */
	int bitpix1,	/* Bits per input pixel (FITS codes) */
	int w1,		/* Number of horizontal pixels in input image */
	int x1, y1);	/* One-based row and column for input pixel */
	char *image2,	/* Pointer to first pixel in output image */
	int bitpix2,	/* Bits per output pixel (FITS codes) */
	int w2,		/* Number of horizontal pixels in output image */
	int x2, y2);	/* One-based row and column for output pixel */

/* Image vector processing subroutines in imio.c */

    void addvec(	/* Add constant to vector from 2-D array */
	char *image,	/* Image array as 1-D vector */
	int bitpix,	/* FITS bits per pixel */
	double bzero,	/* Zero point for pixel scaling */
	double bscale,	/* Scale factor for pixel scaling */
	int pix1,	/* Offset of first pixel to which to add */
	int npix,	/* Number of pixels to which to add */
	double dpix);	/* Value to add to pixels */
    void multvec(	/* Multiply vector from 2-D array by a constant */
	char *image,	/* Image array as 1-D vector */
	int bitpix,	/* FITS bits per pixel */
	double bzero,	/* Zero point for pixel scaling */
	double bscale,	/* Scale factor for pixel scaling */
	int pix1,	/* Offset of first pixel to multiply */
	int npix,	/* Number of pixels to multiply */
	double dpix);	/* Value to add to pixels */
    void getvec(	/* Read vector from 2-D array */
	char *image,	/* Image array as 1-D vector */
	int bitpix,	/* FITS bits per pixel */
	double bzero,	/* Zero point for pixel scaling */
	double bscale,	/* Scale factor for pixel scaling */
	int pix1,	/* Offset of first pixel to extract */
	int npix,	/* Number of pixels to extract */
	double *dvec0);	/* Vector of pixels (returned) */
    void putvec(	/* Write vector into 2-D array */
	char *image,	/* Image array as 1-D vector */
	int bitpix,	/* FITS bits per pixel */
	double bzero,	/* Zero point for pixel scaling */
	double bscale,	/* Scale factor for pixel scaling */
	int pix1,	/* Offset of first pixel to insert */
	int npix,	/* Number of pixels to insert */
	double *dvec0);	/* Vector of pixels to insert */
    void fillvec(	/* Write constant into a vector */
	char *image,	/* Image array as 1-D vector */
	int bitpix,	/* FITS bits per pixel */
	double bzero,	/* Zero point for pixel scaling */
	double bscale,	/* Scale factor for pixel scaling */
	int pix1,	/* Zero-based offset of first pixel to multiply */
	int npix,	/* Number of pixels to multiply */
	double dpix);	/* Value to which to set pixels */
    void fillvec1(	/* Write constant into a vector */
	char *image,	/* Image array as 1-D vector */
	int bitpix,	/* FITS bits per pixel */
	double bzero,	/* Zero point for pixel scaling */
	double bscale,	/* Scale factor for pixel scaling */
	int pix1,	/* One-based offset of first pixel to multiply */
	int npix,	/* Number of pixels to multiply */
	double dpix);	/* Value to which to set pixels */

/* Image pixel byte-swapping subroutines in imio.c */

    void imswap(	/* Swap alternating bytes in a vector */
	int bitpix,	/* Number of bits per pixel */
	char *string,	/* Address of starting point of bytes to swap */
	int nbytes);	/* Number of bytes to swap */
    void imswap2(	/* Swap bytes in a vector of 2-byte (short) integers */
	char *string,	/* Address of starting point of bytes to swap */
	int nbytes);	/* Number of bytes to swap */
    void imswap4(	/* Reverse bytes in a vector of 4-byte numbers */
	char *string,	/* Address of starting point of bytes to swap */
	int nbytes);	/* Number of bytes to swap */
    void imswap8(	/* Reverse bytes in a vector of 8-byte numbers */
	char *string,	/* Address of starting point of bytes to swap */
	int nbytes);	/* Number of bytes to swap */
    int imswapped();	/* Return 1 if machine byte order is not FITS order */

/* File utilities from fileutil.c */

    int getfilelines(	/*  Return number of lines in an ASCII file */
	char *filename); /* Name of file to check */
    char *getfilebuff(	/* Return entire file contents in a character string */
	char *filename); /* Name of file to read */
    int getfilesize(	/* Return size of a binary or ASCII file */
	char *filename); /* Name of file to check */
    int isimlist(	/* Return 1 if file is list of FITS or IRAF image files, else 0 */
	char *filename); /* Name of file to check */
    int isimlistd(	/* Return 1 if file is list of FITS or IRAF image files, else 0 */
	char *filename,	/* Name of file to check */
	char *rootdir);	/* Name of root directory for files in list */
    int isfilelist(	/* Return 1 if list of readable files, else 0 */
	char *filename); /* Name of file to check */
	char *rootdir);	/* Name of root directory for files in list */
    int isfile(		/* Return 1 if file is a readable file, else 0 */
	char *filename); /* Name of file to check */
    int istiff(		/* Return 1 if TIFF image file, else 0 */
	char *filename); /* Name of file to check */
    int isjpeg(		/* Return 1 if JPEG image file, else 0 */
	char *filename); /* Name of file to check */
    int isgif(		/* Return 1 if GIF image file, else 0 */
	char *filename); /* Name of file to check */
    int first_token(	/* Return first token from the next line of an ASCII file */
	FILE *diskfile,	/* File descriptor for ASCII file */
	int ncmax,	/* Maximum number of characters returned */
	char *token);	/* First token on next line (returned) */
    int stc2s (		/* Replace character in string with space */
	char spchar,	/* Character to replace with spaces */
	char *string);	/* Character string to process */
    int sts2c (		/* Replace spaces in string with character */
	char spchar,	/* Character with which to replace spaces */
	char *string);	/* Character string to process */

/* Subroutines for translating dates and times in dateutil.c */

    /* Subroutines to convert from year and day of year */

    void doy2dt(	/* Year and day of year to yyyy.mmdd hh.mmss */
	int year,	/* Year */
	double doy,	/* Day of year with fraction */
	double *date,	/* Date as yyyy.mmdd (returned) */
	double *time);	/* Time as hh.mmssxxxx (returned) */
    double doy2ep(	/* Year and day of year to fractional year (epoch) */
	int year,	/* Year */
	double doy);	/* Day of year with fraction */
    double doy2epb( /* year and day of year to Besselian epoch */
	int year,	/* Year */
	double doy);	/* Day of year with fraction */
    double doy2epj( /* year and day of year to Julian epoch */
	int year,	/* Year */
	double doy);	/* Day of year with fraction */
    char *doy2fd(	/* year and day of year to FITS date */
	int year,	/* Year */
	double doy);	/* Day of year with fraction */
    double doy2jd( /* year and day of year to Julian Day */
	int year,	/* Year */
	double doy);	/* Day of year with fraction */
    double doy2mjd( /* year and day of year to Modified Julian Day */
	int year,	/* Year */
	double doy);	/* Day of year with fraction */
    double doy2ts( /* year and day of year to seconds since 1950.0 */ 
	int year,	/* Year */
	double doy);	/* Day of year with fraction */
    int doy2tsi(	/* year and day of year to IRAF seconds since 1980-01-01 */
	int year,	/* Year */
	double doy);	/* Day of year with fraction */
    time_t doy2tsu(	/* year and day of year to Unix seconds since 1970-01-01 */
	int year,	/* Year */
	double doy);	/* Day of year with fraction */

    /* Subroutines to convert from date and time */

    void dt2doy(	/* yyyy.mmdd hh.mmss to year and day of year */
	double date,	/* Date as yyyy.mmdd
			 * yyyy = calendar year (e.g. 1973)
			 * mm = calendar month (e.g. 04 = april)
			 * dd = calendar day (e.g. 15) */
	double time;	/* Time as hh.mmssxxxx
			 * if time<0, it is time as -(fraction of a day)
			 * hh = hour of day (0 .le. hh .le. 23)
			 * nn = minutes (0 .le. nn .le. 59)
			 * ss = seconds (0 .le. ss .le. 59)
			 * xxxx = tenths of milliseconds (0 .le. xxxx .le. 9999) */
	int *year,	/* Year (returned) */
	double *doy);	/* Day of year with fraction (returned) */
    double dt2ep(	/* yyyy.ddmm and hh.mmsss to fractional year (epoch) */
	double date,	/* Date as yyyy.mmdd */
	double time);	/* Time as hh.mmssxxxx */
    double dt2epb(	/* yyyy.ddmm and hh.mmsss to Besselian epoch */
	double date,	/* Date as yyyy.mmdd */
	double time);	/* Time as hh.mmssxxxx */
    double dt2epj(	/* yyyy.ddmm and hh.mmsss to Julian epoch */
	double date,	/* Date as yyyy.mmdd */
	double time);	/* Time as hh.mmssxxxx */
    char *dt2fd(	/* yyyy.ddmm and hh.mmsss to FITS date string */
	double date,	/* Date as yyyy.mmdd */
	double time);	/* Time as hh.mmssxxxx */
    void dt2i(		/* yyyy.ddmm and hh.mmsss to year, month, day, hrs, min, sec */
	double date,	/* Date as yyyy.mmdd */
	double time,	/* Time as hh.mmssxxxx */
	int *iyr,	/* year (returned) */
	int *imon,	/* month (returned) */
	int *iday,	/* day (returned) */
	int *ihr,	/* hours (returned) */
	int *imn,	/* minutes (returned) */
	double *sec,	/* seconds (returned) */
	int ndsec);	/* Number of decimal places in seconds (0=int) */
    double dt2jd(	/* yyyy.ddmm and hh.mmsss to Julian Day */
	double date,	/* Date as yyyy.mmdd */
	double time);	/* Time as hh.mmssxxxx */
    double dt2mjd(	/* yyyy.ddmm and hh.mmsss to Modified Julian Day */
	double date,	/* Date as yyyy.mmdd */
	double time);	/* Time as hh.mmssxxxx */
    double dt2ts(	/* yyyy.ddmm and hh.mmsss to seconds since 1950.0 */ 
	double date,	/* Date as yyyy.mmdd */
	double time);	/* Time as hh.mmssxxxx */
    int dt2tsi(		/* yyyy.ddmm and hh.mmsss to IRAF seconds since 1980-01-01 */
	double date,	/* Date as yyyy.mmdd */
	double time);	/* Time as hh.mmssxxxx */
    time_t dt2tsu(	/* yyyy.ddmm and hh.mmsss to Unix seconds since 1970-01-01 */
	double date,	/* Date as yyyy.mmdd */
	double time);	/* Time as hh.mmssxxxx */

    /* Subroutines to convert from epoch (various types of fractional year) */

    void ep2dt(		/* Fractional year to yyyy.mmdd hh.mmssss */
	double epoch,	/* Date as fractional year */
	double *date,	/* Date as yyyy.mmdd (returned) */
	double *time);	/* Time as hh.mmssxxxx (returned) */
    void epb2dt(	/* Besselian epoch to yyyy.mmdd hh.mmssss */
	double  epoch,  /* Besselian epoch (fractional 365.242198781-day years) */
	double *date,	/* Date as yyyy.mmdd (returned) */
	double *time);	/* Time as hh.mmssxxxx (returned) */
    void epj2dt(	/* Julian epoch to yyyy.mmdd hh.mmssss */
	double  epoch,  /* Julian epoch (fractional 365.25-day years) */
	double *date,	/* Date as yyyy.mmdd (returned)*/
	double *time);	/* Time as hh.mmssxxxx (returned) */
    char *ep2fd(	/* Fractional year to FITS date string yyyy-mm-ddThh:mm:ss.ss */
	double epoch);	/* Date as fractional year */
    char *epb2fd(	/* Besselian epoch to FITS date string yyyy-mm-ddThh:mm:ss.ss */
	double  epoch);	/* Besselian epoch (fractional 365.242198781-day years) */
    char *epj2fd(	/* Julian epoch to FITS date string yyyy-mm-ddThh:mm:ss.ss */
	double  epoch);  /* Julian epoch (fractional 365.25-day years) */
    void ep2i(		/* Fractional year to year, month, day, hours, min., sec. */
	double epoch,	/* Date as fractional year */
	int *iyr,	/* year (returned) */
	int *imon,	/* month (returned) */
	int *iday,	/* day (returned) */
	int *ihr,	/* hours (returned) */
	int *imn,	/* minutes (returned) */
	double *sec,	/* seconds (returned) */
	int ndsec);	/* Number of decimal places in seconds (0=int) */
    void epb2i(		/* Besselian epoch to year, month, day, hours, min., sec. */
	double  epoch,	/* Besselian epoch (fractional 365.242198781-day years) */
	int *iyr,	/* year (returned) */
	int *imon,	/* month (returned) */
	int *iday,	/* day (returned) */
	int *ihr,	/* hours (returned) */
	int *imn,	/* minutes (returned) */
	double *sec,	/* seconds (returned) */
	int ndsec);	/* Number of decimal places in seconds (0=int) */
    void epj2i(		/* Julian epoch to year, month, day, hours, min., sec. */
	double  epoch,  /* Julian epoch (fractional 365.25-day years) */
	int *iyr,	/* year (returned) */
	int *imon,	/* month (returned) */
	int *iday,	/* day (returned) */
	int *ihr,	/* hours (returned) */
	int *imn,	/* minutes (returned) */
	double *sec,	/* seconds (returned) */
	int ndsec);	/* Number of decimal places in seconds (0=int) */
    double ep2jd(	/* Fractional year to Julian Date */
	double epoch);	/* Date as fractional year */
    double epb2jd(	/* Besselian epoch to Julian Date */
	double  epoch);	/* Besselian epoch (fractional 365.242198781-day years) */
    double epj2jd(	/* Julian epoch to Julian Date */
	double  epoch);  /* Julian epoch (fractional 365.25-day years) */
    double ep2mjd(	/* Fractional year to Modified Julian Date */
	double epoch);	/* Date as fractional year */
    double epb2mjd(	/* Besselian epoch to Modified Julian Date */
	double  epoch);	/* Besselian epoch (fractional 365.242198781-day years) */
    double epj2mjd(	/* Julian epoch to Modified Julian Date */
	double  epoch);  /* Julian epoch (fractional 365.25-day years) */
    double ep2epb(	/* Fractional year to Besselian epoch */
	double epoch);	/* Date as fractional year */
    double ep2epj(	/* Fractional year to Julian epoch */
	double epoch);	/* Date as fractional year */
    double epb2epj(	/* Besselian epoch to Julian epoch */
	double  epoch);	/* Besselian epoch (fractional 365.242198781-day years) */
    double epj2epb(	/* Julian epoch to Besselian epoch */
	double  epoch);  /* Julian epoch (fractional 365.25-day years) */
    double epb2ep(	/* Besselian epoch to fractional year */
	double  epoch);	/* Besselian epoch (fractional 365.242198781-day years) */
    double epj2ep(	/* Julian epoch to fractional year */
	double  epoch);  /* Julian epoch (fractional 365.25-day years) */
    double ep2ts(	/* Fractional year to seconds since 1950.0 */
	double epoch);	/* Date as fractional year */
    double epb2ts(	/* Besselian epoch to seconds since 1950.0 */
	double  epoch);  /* Besselian epoch (fractional 365.242198781-day years) */
    double epj2ts( /* Julian epoch to seconds since 1950.0 */
	double  epoch);  /* Julian epoch (fractional 365.25-day years) */

    /* Convert from FITS standard date string */

    void fd2dt(		/* FITS standard date string to date and time */
	char *string;	/* FITS date string, which may be:
			 * fractional year
			 * dd/mm/yy (FITS standard before 2000)
			 * dd-mm-yy (nonstandard use before 2000)
			 * yyyy-mm-dd (FITS standard after 1999)
			 * yyyy-mm-ddThh:mm:ss.ss (FITS standard after 1999) */
	double *date,	/* Date as yyyy.mmdd (returned)*/
	double *time);	/* Time as hh.mmssxxxx (returned) */
    void fd2doy(	/* FITS standard date string to year, day of year */
	char *string,	/* FITS date string */
	int *year,	/* Year (returned) */
	double *doy);	/* Day of year with fraction (returned) */
    double fd2ep(	/* FITS standard date string to fractional year (epoch) */
	char *string);	/* FITS date string */
    double fd2epb(	/* FITS standard date string to Besselian epoch */
	char *string);	/* FITS date string */
    double fd2epj(	/* FITS standard date string to Julian epoch */
	char *string);	/* FITS date string */
    char *fd2fd(	/* Any FITS standard date string to ISO FITS date string */
	char *string);	/* FITS date string */
    char *fd2of(	/* Any FITS standard date string to old FITS date and time */
	char *string);	/* FITS date string */
    char *fd2ofd(	/* Any FITS standard date string to old FITS date string */
	char *string);	/* FITS date string */
    char *fd2oft(	/* Any FITS standard date string to old FITS time string */
	char *string);	/* FITS date string */
    void fd2i(		/* FITS standard date string to year, mon, day, hrs, min, sec */
	char *string,	/* FITS date string */
	int *iyr,	/* year (returned) */
	int *imon,	/* month (returned) */
	int *iday,	/* day (returned) */
	int *ihr,	/* hours (returned) */
	int *imn,	/* minutes (returned) */
	double *sec,	/* seconds (returned) */
	int ndsec);	/* Number of decimal places in seconds (0=int) */
    double fd2jd(	/* FITS standard date string to Julian Day */
	char *string);	/* FITS date string */
    double fd2mjd(	/* FITS standard date string to Modified Julian Day */
	char *string);	/* FITS date string */
    double fd2ts(	/* FITS standard date to seconds since 1950-01-01 */
	char *string);	/* FITS date string */
    int fd2tsi(		/* FITS standard date to IRAF seconds since 1980-01-01 */
	char *string);	/* FITS date string */
    time_t fd2tsu(	/* FITS standard date to Unix seconds since 1970-01-01 */
	char *string);	/* FITS date string */

    /* Convert from Julian Day */

    void jd2doy(	/* Julian Day to year and day of year */
	double dj,	/* Julian Day */
	int *year,	/* Year (returned) */
	double *doy);	/* Day of year with fraction (returned) */
    void jd2dt(		/* Julian Day to yyyy.mmdd hh.mmssss */
	double dj,	/* Julian Day */
	double *date,	/* Date as yyyy.mmdd (returned)*/
	double *time);	/* Time as hh.mmssxxxx (returned) */
    double jd2ep(	/* Julian Day to fractional year */
	double dj);	/* Julian Day */
    double jd2epb(	/* Julian Day to Besselian epoch */
	double dj);	/* Julian Day */
    double jd2epj(	/* Julian Day to Julian epoch */
	double dj);	/* Julian Day */
    char *jd2fd(	/* Julian Day to FITS date string yyyy-mm-ddThh:mm:ss.ss */
	double dj);	/* Julian Day */
    void jd2i(		/* Julian Day to year, month, day, hours, min., sec. */
	double dj,	/* Julian Day */
	int *iyr,	/* year (returned) */
	int *imon,	/* month (returned) */
	int *iday,	/* day (returned) */
	int *ihr,	/* hours (returned) */
	int *imn,	/* minutes (returned) */
	double *sec,	/* seconds (returned) */
	int ndsec);	/* Number of decimal places in seconds (0=int) */
    double jd2mjd( /* Julian Day to Modified Julian day */
	double dj);	/* Julian Day */
    double jd2ts(	/* Julian Day to seconds since 1950.0 */
	double dj);	/* Julian Day */
    time_t jd2tsu( /* Julian Day to Unix seconds since 1970-01-01T00:00 */
	double dj);	/* Julian Day */
    int jd2tsi( /* Julian Day to IRAF seconds since 1980-01-01T00:00 */
	double dj);	/* Julian Day */

    /* Convert current local time to various formats */

    void lt2dt(		/* Current local time to date (yyyy.mmdd), time (hh.mmsss) */
	double *date,	/* Date as yyyy.mmdd (returned) */
	double *time);	/* Time as hh.mmssxxxx (returned) */
    char *lt2fd();	/* Current local time to FITS ISO date string */
    int lt2tsi();	/* Current local time to IRAF seconds since 1980-01-01T00:00 */
    time_t lt2tsu();	/* Current local time to Unix seconds since 1970-01-01T00:00 */
    double lt2ts();	/* Current local time to IRAF seconds since 1950-01-01T00:00 */

    /* Convert from Modified Julian Day (JD - 2400000.5) */

    void mjd2doy(	/* Modified Julian Day to year and day of year */
	double dj,	/* Modified Julian Day */
	int *year,	/* Year (returned) */
	double *doy);	/* Day of year with fraction (returned) */
    void mjd2dt(	/* Modified Julian Day to yyyy.mmdd hh.mmssss */
	double dj,	/* Modified Julian Date */
	double *date,	/* Date as yyyy.mmdd (returned)*/
	double *time);	/* Time as hh.mmssxxxx (returned) */
    double mjd2ep( /* Modified Julian Day to fractional year */
	double dj);	/* Modified Julian Date */
    double mjd2epb( /* Modified Julian Day to Besselian epoch */
	double dj);	/* Modified Julian Date */
    double mjd2epj( /* Modified Julian Day to Julian epoch */
	double dj);	/* Modified Julian Date */
    char *mjd2fd(	/* Modified Julian Day to FITS date yyyy-mm-ddThh:mm:ss.ss */
	double dj);	/* Modified Julian Date */
    void mjd2i(	/* Modified Julian Day to year, month, day, hours, min, sec */
	double dj,	/* Modified Julian Date */
	int *iyr,	/* year (returned) */
	int *imon,	/* month (returned) */
	int *iday,	/* day (returned) */
	int *ihr,	/* hours (returned) */
	int *imn,	/* minutes (returned) */
	double *sec,	/* seconds (returned) */
	int ndsec);	/* Number of decimal places in seconds (0=int) */
    double mjd2jd( /* Modified Julian Day to Julian day */
	double dj);	/* Modified Julian Date */
    double mjd2ts( /* Modified Julian Day to seconds since 1950.0 */
	double dj);	/* Modified Julian Date */

    /* Convert from seconds since 1950-01-01 0:00 (JPL Ephemeris time) */

    void ts2dt(		/* Seconds since 1950.0 to yyyy.mmdd hh.mmssss */
	double tsec,	/* seconds since 1950.0 */
	double *date,	/* Date as yyyy.mmdd (returned)*/
	double *time);	/* Time as hh.mmssxxxx (returned) */
    double ts2ep(	/* Seconds since 1950.0 to fractional year */
	double tsec);	/* seconds since 1950.0 */
    double ts2epb( /* Seconds since 1950.0 to Besselian epoch */
	double tsec);	/* seconds since 1950.0 */
    double ts2epj( /* Seconds since 1950.0 to Julian epoch */
	double tsec);	/* seconds since 1950.0 */
    char *ts2fd(	/* Seconds since 1950.0 to FITS date, yyyy-mm-ddT00:00:00.000 */
	double tsec);	/* seconds since 1950.0 */
    void ts2i(	/* Seconds since 1950.0 to year, month, day, hours, min, sec */
	double tsec,	/* seconds since 1950.0 */
	int *iyr,	/* year (returned) */
	int *imon,	/* month (returned) */
	int *iday,	/* day (returned) */
	int *ihr,	/* hours (returned) */
	int *imn,	/* minutes (returned) */
	double *sec,	/* seconds (returned) */
	int ndsec);	/* Number of decimal places in seconds (0=int) */
    double ts2jd(	/* Seconds since 1950.0 to Julian Day */
	double tsec);	/* seconds since 1950.0 */
    double ts2mjd( /* Seconds since 1950.0 to Modified Julian Day */
	double tsec);	/* seconds since 1950.0 */

    /* Convert from IRAF time (seconds since 1980-01-01 0:00 UT) */

    char *tsi2fd(	/* Seconds since 1980-01-01 to FITS standard date string */
    double tsi2ts( /* Seconds since 1980-01-01 to seconds since 1950-01-01 */
    double tsi2ts( /* Seconds since 1980-01-01 to seconds since 1950-01-01 */
    void tsi2dt( /* Seconds since 1980-01-01 to date yyyy.mmdd, time hh.mmssss */

    /* Convert from Unix time (seconds since 1970-01-01 0:00 UT) */

    void tsu2dt(	/* Seconds since 1970-01-01 to date yyyy.ddmm, time hh.mmsss */
    char *tsu2fd(	/* Seconds since 1970-01-01 to FITS standard date string */
    char *tsd2fd(	/* Seconds since start of day to FITS standard time string */
    double tsu2ts( /* Seconds since 1970-01-01 to seconds since 1950-01-01 */
    int tsu2tsi(	/* Seconds since 1970-01-01 to local seconds since 1980-01-01 */

    /* Convert from current Universal Time */

    void ut2dt(	/* Current Universal Time to date (yyyy.mmdd), time (hh.mmsss) */
    void ut2doy( /* Current Universal Time to year, day of year */
    double ut2ep( /* Current Universal Time to fractional year */
    double ut2epb( /* Current Universal Time to Besselian Epoch */
    double ut2epj( /* Current Universal Time to Julian Epoch */
    char *ut2fd(	/* Current Universal Time to FITS ISO date string */
    double ut2jd(	/* Current Universal Time to Julian Date */
    double ut2mjd( /* Current Universal Time to Modified Julian Date */
    int ut2tsi(	/* Current UT to IRAF seconds since 1980-01-01T00:00 */
    time_t ut2tsu(	/* Current UT to Unix seconds since 1970-01-01T00:00 */
    double ut2ts( /* Current UT to IRAF seconds since 1950-01-01T00:00 */

    double tsd2dt( /* Seconds since start of day to hh.mmsssss */
    int isdate(	/* Return 1 if string is FITS old or ISO date */
    int sts2c(	/* Replaces spaces in a string with a specified character */
    int stc2s(	/* Replaces a specified character in a string with spaces */

    /* Ephemeris time conversions (ET, TT, and TDT) */

    char *et2fd(	/* ET (or TDT or TT) in FITS format to UT in FITS format */
	char *string);	/* Ephemeris Time as FITS date string (E not T) */
    char *fd2et(	/* UT in FITS format to ET (or TDT or TT) in FITS format */
	char *string);	/* FITS date string */
    void dt2et(		/* yyyy.ddmm and hh.mmsss to Ephemeris Time */ 
	double *date,	/* Date as yyyy.mmdd */
	double *time);	/* Time as hh.mmssxxxx
			 *if time<0, it is time as -(fraction of a day) */
    double jd2jed(	/* Convert from Julian Date to Julian Ephemeris Date */
	double dj);	/* Julian Date */
    double jed2jd(	/* Convert from Julian Ephemeris Date to Julian Date */
	double dj);	/* Julian Ephemeris Date */
    double ets2ts(	/* ET in seconds since 1950-01-01 to UT in same format */
	double tsec);	/* ET in seconds since 1950-01-01 */
    double ts2ets(	/* UT in seconds since 1950-01-01 to ET in same format */
	double tsec);	/* UT in seconds since 1950-01-01 */
    void edt2dt(	/* yyyy.ddmm and hh.mmsss Ephemeris Time to UT */ 
	double *date;	/* Date as yyyy.mmdd */
	double *time;	/* Time as hh.mmssxxxx
			 * If time<0, it is time as -(fraction of a day) */
    double utdt(	/* Compute difference between UT and dynamical time (ET-UT) */
	double dj);	/* Julian Date (UT) */

    /* Sidereal Time conversions */

    char *fd2gst(	/* Convert from FITS UT date to Greenwich Sidereal Time */
	char *string);	/* FITS date string */
    void dt2gst(	/* Convert from UT as yyyy.mmdd hh.mmssss to Greenwich Sidereal Time */
	double *date,	/* Date as yyyy.mmdd */
	double *time);	/* Time as hh.mmssxxxx
			 * If time<0, it is time as -(fraction of a day) */
    double jd2gst(	/* Calculate Greenwich Sidereal Time given Julian Date */
	double dj);	/* Julian Date (UT) */
    double ts2gst(	/* Calculate Greenwich Sidereal Time given Universal Time */
	double tsec);	/* Time since 1950.0 in UT seconds */
    char *fd2lst(	/* Convert from FITS UT date to Local Sidereal Time */
	char *string);	/* FITS date string */
    void dt2lst(	/* Convert from UT as yyyy.mmdd hh.mmssss to Local Sidereal Time */
	double *date,	/* Date as yyyy.mmdd */
	double *time);	/* Time as hh.mmssxxxx
			 * If time<0, it is time as -(fraction of a day) */
    double ts2lst(	/* Calculate Local Sidereal Time given Universal Time */
	double tsec);	/* Time since 1950.0 in UT seconds */
    double jd2lst(	/* Calculate Local Sidereal Time given Julian Date */
	double dj);	/* Julian Date (UT) */
    double eqeqnx(	/* Compute equation of eqinoxes from Julian Date */
	double dj);	/* Julian Date (UT) */
    char *fd2mst(	/* Convert from FITS UT date to Mean Sidereal Time */
	char *string);	/* FITS date string */
    double jd2mst(	/* Convert from Julian Date to Mean Sidereal Time */
	double dj);	/* Julian Date (UT) */
    double jd2mst2(	/* Convert from Julian Date to Mean Sidereal Time */
	double dj);	/* Julian Date (UT) */
    void dt2mst(	/* Convert from UT as yyyy.mmdd hh.mmssss to Mean Sidereal Time */
	double *date,	/* Date as yyyy.mmdd */
	double *time);	/* Time as hh.mmssxxxx
			 * If time<0, it is time as -(fraction of a day) */
    double lst2dt(	/* Calculate UT as hh.mmsss given UT date and
			 * Local Sidereal Time */
	double date0;   /* UT date as yyyy.mmdd */
	double time0;   /* LST as hh.mmssss */
    double lst2jd(	/* Calculate UT as Julian Date given UT date and
			 * Local Sidereal Time */
	double sdj);     /* Julian Date of desired day at 0:00 UT + sidereal time */
    char *lst2fd(	/* Calculate FITS UT date and time given UT date and
			 * Local Sidereal Time */
	char *string);	/* UT Date, LST as yyyy-mm-ddShh:mm:ss.ss */
    char *gst2fd(	/* Calculate FITS UT date and time given Greenwich Sidereal Time */
	char *string);	/* UT Date, GST as yyyy-mm-ddShh:mm:ss.ss */
    double gst2jd(	/* Calculate FITS UT Julian Date given Greenwich Sidereal Time */
	double sdj);	/* UT Date, GST as Julian Date */
    char *mst2fd(	/* Calculate FITS UT date and time given Mean Sidereal Time */
	char *string);	/* UT Date, MST as yyyy-mm-ddShh:mm:ss.ss */
    double mst2jd(	/* Calculate FITS UT Julian Date given Mean Sidereal Time */
	double sdj);	/* UT Date, MST as Julian Date */
    double ts2mst(	/* Calculate Mean Sidereal Time given Universal Time */
	double tsec);	/* time since 1950.0 in UT seconds */
    void setlongitude( /* Longitude for sidereal time in or out */
	double longitude); /* longitude of observatory in degrees (+=west) */
    void compnut(	/* Compute nutation in longitude and obliquity and mean obliquity*/
	double dj,	/* TDB (loosely ET or TT) as Julian Date */
	double *dpsi,	/* Nutation in longitude in radians (returned) */
	double *deps,	/* Nutation in obliquity in radians (returned) */
	double *eps0);	/* Mean obliquity in radians (returned) */

    /* Heliocentric Julian Date conversions */

    double mjd2mhjd(	/* Convert from Modified Julian Date to Heliocentric MJD */
	double mjd,	/* Julian date (geocentric) */
	double ra,	/* Right ascension (degrees) */
	double dec,	/* Declination (degrees) */
	int sys);	/* J2000, B1950, GALACTIC, ECLIPTIC */
    double mjd2hjd( /* Convert from Modified Julian Date to Heliocentric JD */
	double mjd,	/* Julian date (geocentric) */
	double ra,	/* Right ascension (degrees) */
	double dec,	/* Declination (degrees) */
	int sys);	/* J2000, B1950, GALACTIC, ECLIPTIC */
    double mhjd2mjd(	/* Convert from Heliocentric Modified Julian Date to MJD */
	double mhjd,	/* Modified Heliocentric Julian date */
	double ra,	/* Right ascension (degrees) */
	double dec,	/* Declination (degrees) */
	int sys);	/* J2000, B1950, GALACTIC, ECLIPTIC */
    double jd2hjd( /* Convert from Julian Date to Heliocentric Julian Date */
	double  dj,     /* Julian date (geocentric) */
	double ra,	/* Right ascension (degrees) */
	double dec,	/* Declination (degrees) */
	int sys);	/* J2000, B1950, GALACTIC, ECLIPTIC */
    double hjd2jd( /* Convert from Heliocentric Julian Date to Julian Date */
	double  dj,	/* Heliocentric Julian date */
	double ra,	/* Right ascension (degrees) */
	double dec,	/* Declination (degrees) */
	int sys);	/* J2000, B1950, GALACTIC, ECLIPTIC */

    void setdatedec(	/* Set number of decimal places in FITS dates */
	int nd);	/* Number of decimal places in FITS dates */
