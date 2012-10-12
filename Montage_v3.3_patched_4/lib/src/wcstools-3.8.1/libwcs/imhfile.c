/*** File imhfile.c
 *** January 8, 2007
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

 * Module:      imhfile.c (IRAF .imh image file reading and writing)
 * Purpose:     Read and write IRAF image files (and translate headers)
 * Subroutine:  check_immagic (irafheader, teststring )
 *		Verify that file is valid IRAF imhdr or impix
 * Subroutine:  irafrhead (filename, lfhead, fitsheader, lihead)
 *              Read IRAF image header
 * Subroutine:  irafrimage (fitsheader)
 *              Read IRAF image pixels (call after irafrhead)
 * Subroutine:	same_path (pixname, hdrname)
 *		Put filename and header path together
 * Subroutine:	iraf2fits (hdrname, irafheader, nbiraf, nbfits)
 *		Convert IRAF image header to FITS image header
 * Subroutine:	irafwhead (hdrname, irafheader, fitsheader)
 *		Write IRAF header file
 * Subroutine:	irafwimage (hdrname, irafheader, fitsheader, image )
 *		Write IRAF image and header files
 * Subroutine:	fits2iraf (fitsheader, irafheader)
 *		Convert FITS image header to IRAF image header
 * Subroutine:  irafgeti4 (irafheader, offset)
 *		Get 4-byte integer from arbitrary part of IRAF header
 * Subroutine:  irafgetc2 (irafheader, offset)
 *		Get character string from arbitrary part of IRAF v.1 header
 * Subroutine:  irafgetc (irafheader, offset)
 *		Get character string from arbitrary part of IRAF header
 * Subroutine:  iraf2str (irafstring, nchar)
 * 		Convert 2-byte/char IRAF string to 1-byte/char string
 * Subroutine:  str2iraf (string, irafstring, nchar)
 * 		Convert 1-byte/char string to IRAF 2-byte/char string
 * Subroutine:	irafswap (bitpix,string,nbytes)
 *		Swap bytes in string in place, with FITS bits/pixel code
 * Subroutine:	irafswap2 (string,nbytes)
 *		Swap bytes in string in place
 * Subroutine	irafswap4 (string,nbytes)
 *		Reverse bytes of Integer*4 or Real*4 vector in place
 * Subroutine	irafswap8 (string,nbytes)
 *		Reverse bytes of Real*8 vector in place
 * Subroutine	irafsize (filename)
 *		Return length of file in bytes
 * Subroutine	isiraf (filename)
 *		Return 1 if IRAF .imh file, else 0


 * Copyright:   2000 Smithsonian Astrophysical Observatory
 *              You may do anything you like with this file except remove
 *              this copyright.  The Smithsonian Astrophysical Observatory
 *              makes no representations about the suitability of this
 *              software for any purpose.  It is provided "as is" without
 *              express or implied warranty.
 */

#include <stdio.h>		/* define stderr, FD, and NULL */
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <time.h>
#include <sys/types.h>
#include "fitsfile.h"

/* Parameters from iraf/lib/imhdr.h for IRAF version 1 images */
#define SZ_IMPIXFILE	 79		/* name of pixel storage file */
#define SZ_IMHDRFILE	 79   		/* length of header storage file */
#define SZ_IMTITLE	 79		/* image title string */
#define LEN_IMHDR	2052		/* length of std header */

/* Parameters from iraf/lib/imhdr.h for IRAF version 2 images */
#define	SZ_IM2PIXFILE	255		/* name of pixel storage file */
#define	SZ_IM2HDRFILE	255		/* name of header storage file */
#define	SZ_IM2TITLE	383		/* image title string */
#define LEN_IM2HDR	2046		/* length of std header */

/* Offsets into header in bytes for parameters in IRAF version 1 images */
#define IM_HDRLEN	 12		/* Length of header in 4-byte ints */
#define IM_PIXTYPE       16             /* Datatype of the pixels */
#define IM_NDIM          20             /* Number of dimensions */
#define IM_LEN           24             /* Length (as stored) */
#define IM_PHYSLEN       52             /* Physical length (as stored) */
#define IM_PIXOFF        88             /* Offset of the pixels */
#define IM_CTIME        108             /* Time of image creation */
#define IM_MTIME        112             /* Time of last modification */
#define IM_LIMTIME      116             /* Time of min,max computation */
#define IM_MAX          120             /* Maximum pixel value */
#define IM_MIN          124             /* Maximum pixel value */
#define IM_PIXFILE      412             /* Name of pixel storage file */
#define IM_HDRFILE      572             /* Name of header storage file */
#define IM_TITLE        732             /* Image name string */

/* Offsets into header in bytes for parameters in IRAF version 2 images */
#define IM2_HDRLEN	  6		/* Length of header in 4-byte ints */
#define IM2_PIXTYPE      10             /* Datatype of the pixels */
#define IM2_SWAPPED      14             /* Pixels are byte swapped */
#define IM2_NDIM         18             /* Number of dimensions */
#define IM2_LEN          22             /* Length (as stored) */
#define IM2_PHYSLEN      50             /* Physical length (as stored) */
#define IM2_PIXOFF       86             /* Offset of the pixels */
#define IM2_CTIME       106             /* Time of image creation */
#define IM2_MTIME       110             /* Time of last modification */
#define IM2_LIMTIME     114             /* Time of min,max computation */
#define IM2_MAX         118             /* Maximum pixel value */
#define IM2_MIN         122             /* Maximum pixel value */
#define IM2_PIXFILE     126             /* Name of pixel storage file */
#define IM2_HDRFILE     382             /* Name of header storage file */
#define IM2_TITLE       638             /* Image name string */

/* Codes from iraf/unix/hlib/iraf.h */
#define	TY_CHAR		2
#define	TY_SHORT	3
#define	TY_INT		4
#define	TY_LONG		5
#define	TY_REAL		6
#define	TY_DOUBLE	7
#define	TY_COMPLEX	8
#define TY_POINTER      9
#define TY_STRUCT       10
#define TY_USHORT       11
#define TY_UBYTE        12

#define LEN_IRAFHDR	25000
#define LEN_PIXHDR	1024
#define LEN_FITSHDR	11520

int check_immagic();
int irafgeti4();
float irafgetr4();
char *irafgetc2();
char *irafgetc();
char *iraf2str();
static char *same_path();
static void irafputr4();
static void irafputi4();
static void irafputc2();
static void irafputc();
static void str2iraf();
static int headswap=-1;	/* =1 to swap data bytes of foreign IRAF file */
static void irafswap();
static void irafswap2();
static void irafswap4();
static void irafswap8();
int head_version ();
int pix_version ();
int irafncmp ();
static int machswap();
static int irafsize();

#define SECONDS_1970_TO_1980    315532800L

/* Subroutine:	irafrhead
 * Purpose:	Open and read the iraf .imh file, translating it to FITS, too.
 * Returns:	NULL if failure, else pointer to IRAF .imh image header
 * Notes:	The imhdr format is defined in iraf/lib/imhdr.h, some of
 *		which defines or mimicked, above.
 */

char *
irafrhead (filename, lihead)

char	*filename;	/* Name of IRAF header file */
int	*lihead;	/* Length of IRAF image header in bytes (returned) */
{
    FILE *fd;
    int nbr;
    char *irafheader;
    int nbhead, nbytes;
    int imhver;

    headswap = -1;
    *lihead = 0;

    /* open the image header file */
    fd = fopen (filename, "rb");
    if (fd == NULL) {
	fprintf (stderr, "IRAFRHEAD:  cannot open file %s to read\n", filename);
	return (NULL);
	}

    /* Find size of image header file */
    if ((nbhead = irafsize (fd)) <= 0) {
	fprintf (stderr, "IRAFRHEAD:  cannot read file %s, size = %d\n",
		 filename, nbhead);
	return (NULL);
	}

    /* allocate initial sized buffer */
    nbytes = nbhead + 5000;
    irafheader = (char *) calloc (nbytes/4, 4);
    if (irafheader == NULL) {
	(void)fprintf(stderr, "IRAFRHEAD Cannot allocate %d-byte header\n",
		      nbytes);
	return (NULL);
	}
    *lihead = nbytes;

    /* Read IRAF header */
    nbr = fread (irafheader, 1, nbhead, fd);
    fclose (fd);

    /* Reject if header less than minimum length */
    if (nbr < LEN_PIXHDR) {
	(void)fprintf(stderr, "IRAFRHEAD header file %s: %d / %d bytes read.\n",
		      filename,nbr,LEN_PIXHDR);
	free (irafheader);
	return (NULL);
	}

    /* Check header magic word */
    imhver = head_version (irafheader);
    if (imhver < 1) {
	free (irafheader);
	(void)fprintf(stderr, "IRAFRHEAD: %s is not a valid IRAF image header\n",
		      filename);
	return(NULL);
	}

    /* check number of image dimensions
    if (imhver == 2)
	ndim = irafgeti4 (irafheader, IM2_NDIM])
    else
	ndim = irafgeti4 (irafheader, IM_NDIM])
    if (ndim < 2) {
	free (irafheader);
	(void)fprintf(stderr, "File %s does not contain 2d image\n", filename);
	return (NULL);
	} */

    return (irafheader);
}


char *
irafrimage (fitsheader)

char	*fitsheader;	/* FITS image header (filled) */
{
    FILE *fd;
    char *bang;
    int naxis, naxis1, naxis2, naxis3, npaxis1, npaxis2,bitpix, bytepix, pixswap, i;
    char *image;
    int nbr, nbimage, nbaxis, nbl, nbdiff, lpname;
    char *pixheader;
    char *linebuff, *pixchar;
    int imhver, lpixhead, len;
    char pixname[SZ_IM2PIXFILE+1];
    char newpixname[SZ_IM2HDRFILE+1];

    /* Convert pixel file name to character string */
    hgetm (fitsheader, "PIXFIL", SZ_IM2PIXFILE, pixname);

    /* Drop trailing spaces */
    lpname = strlen (pixname);
    pixchar = pixname + lpname - 1;
    while (*pixchar == ' ')
	*pixchar = (char) 0;

    hgeti4 (fitsheader, "PIXOFF", &lpixhead);

    /* Open pixel file, ignoring machine name if present */
    if ((bang = strchr (pixname, '!')) != NULL )
	fd = fopen (bang + 1, "rb");
    else
	fd = fopen (pixname, "rb");

    /* If not at pathname in header, try same directory as header file */
    if (!fd) {
	hgetm (fitsheader, "IMHFIL", SZ_IM2HDRFILE, newpixname);
	len = strlen (newpixname);
	newpixname[len-3] = 'p';
	newpixname[len-2] = 'i';
	newpixname[len-1] = 'x';
	fd = fopen (newpixname, "rb");
	}

    /* Print error message and exit if pixel file is not found */
    if (!fd) {
	(void)fprintf(stderr,
	     "IRAFRIMAGE: Cannot open IRAF pixel file %s\n", pixname);
	return (NULL);
	}

    /* Read pixel header */
    pixheader = (char *) calloc (lpixhead/4, 4);
    if (pixheader == NULL) {
	(void)fprintf(stderr, "IRAFRIMAGE Cannot allocate %d-byte pixel header\n",
		lpixhead);
	return (NULL);
	}
    nbr = fread (pixheader, 1, lpixhead, fd);

    /* Check size of pixel header */
    if (nbr < lpixhead) {
	(void)fprintf(stderr, "IRAF pixel file %s: %d / %d bytes read.\n",
		      pixname,nbr,LEN_PIXHDR);
	free (pixheader);
	fclose (fd);
	return (NULL);
	}

    /* check pixel header magic word */
    imhver = pix_version (pixheader);
    if (imhver < 1) {
	(void)fprintf(stderr, "File %s not valid IRAF pixel file.\n", pixname);
	free (pixheader);
	fclose (fd);
	return(NULL);
	}
    free (pixheader);

    /* Find number of bytes to read */
    hgeti4 (fitsheader,"NAXIS",&naxis);
    hgeti4 (fitsheader,"NAXIS1",&naxis1);
    hgeti4 (fitsheader,"NAXIS2",&naxis2);
    hgeti4 (fitsheader,"NPAXIS1",&npaxis1);
    hgeti4 (fitsheader,"NPAXIS2",&npaxis2);
    hgeti4 (fitsheader,"BITPIX",&bitpix);
    if (bitpix < 0)
	bytepix = -bitpix / 8;
    else
	bytepix = bitpix / 8;

    /* If either dimension is one and image is 3-D, read all three dimensions */
    if (naxis == 3 && ((naxis1 == 1) | (naxis2 == 1))) {
	hgeti4 (fitsheader,"NAXIS3",&naxis3);
	nbimage = naxis1 * naxis2 * naxis3 * bytepix;
	}
    else {
	nbimage = naxis1 * naxis2 * bytepix;
	naxis3 = 1;
	}

    if (bytepix > 4)
	image =  (char *) calloc (nbimage/8, 8);
    else if (bytepix > 2)
	image =  (char *) calloc (nbimage/4, 4);
    else if (bytepix > 1)
	image =  (char *) calloc (nbimage/2, 2);
    else
	image =  (char *) calloc (nbimage, 1);
    if (image == NULL) {
	(void)fprintf(stderr, "IRAFRIMAGE Cannot allocate %d-byte image buffer\n",
		nbimage);
	return (NULL);
	}

    /* Read IRAF image all at once if physical and image dimensions are the same */
    if (npaxis1 == naxis1)
	nbr = fread (image, 1, nbimage, fd);

    /* Read IRAF image one line at a time if physical and image dimensions differ */
    else {
	nbdiff = (npaxis1 - naxis1) * bytepix;
	nbaxis = naxis1 * bytepix;
	linebuff = image;
	nbr = 0;
	if (naxis2 == 1 && naxis3 > 1)
	    naxis2 = naxis3;
	for (i = 0; i < naxis2; i++) {
	    nbl = fread (linebuff, 1, nbaxis, fd);
	    nbr = nbr + nbl;
	    (void) fseek (fd, nbdiff, SEEK_CUR);
	    linebuff = linebuff + nbaxis;
	    }
	}
    fclose (fd);

    /* Check size of image */
    if (nbr < nbimage) {
	(void)fprintf(stderr, "IRAF pixel file %s: %d / %d bytes read.\n",
		      pixname,nbr,nbimage);
	free (image);
	return (NULL);
	}

    /* Byte-reverse image, if necessary */
    pixswap = 0;
    hgetl (fitsheader, "PIXSWAP", &pixswap);
    if (pixswap)
	irafswap (bitpix, image, nbimage);

    return (image);
}


/* Return IRAF image format version number from magic word in IRAF header*/

int
head_version (irafheader)

char	*irafheader;	/* IRAF image header from file */

{

    /* Check header file magic word */
    if (irafncmp (irafheader, "imhdr", 5) != 0 ) {
	if (strncmp (irafheader, "imhv2", 5) != 0)
	    return (0);
	else
	    return (2);
	}
    else
	return (1);
}


/* Return IRAF image format version number from magic word in IRAF pixel file */

int
pix_version (irafheader)

char	*irafheader;	/* IRAF image header from file */

{

    /* Check pixel file header magic word */
    if (irafncmp (irafheader, "impix", 5) != 0) {
	if (strncmp (irafheader, "impv2", 5) != 0)
	    return (0);
	else
	    return (2);
	}
    else
	return (1);
}


/* Verify that file is valid IRAF imhdr or impix by checking first 5 chars
 * Returns:	0 on success, 1 on failure */

int
irafncmp (irafheader, teststring, nc)

char	*irafheader;	/* IRAF image header from file */
char	*teststring;	/* C character string to compare */
int	nc;		/* Number of characters to compate */

{
    char *line;

    headswap = -1;
    if ((line = iraf2str (irafheader, nc)) == NULL)
	return (1);
    if (strncmp (line, teststring, nc) == 0) {
	free (line);
	return (0);
	}
    else {
	free (line);
	return (1);
	}
}

/* Convert IRAF image header to FITS image header, returning FITS header */

char *
iraf2fits (hdrname, irafheader, nbiraf, nbfits)

char	*hdrname;	/* IRAF header file name (may be path) */
char	*irafheader;	/* IRAF image header */
int	nbiraf;		/* Number of bytes in IRAF header */
int	*nbfits;	/* Number of bytes in FITS header (returned) */

{
    char *objname;	/* object name from FITS file */
    int lstr, i, j, k, ib, nax, nbits, nl;
    int lname = 0;
    char *pixname, *newpixname, *bang, *chead;
    char *fitsheader;
    int nblock, nlines;
    char *fhead, *fhead1, *fp, endline[81];
    char irafchar;
    char fitsline[81];
    char *dstring;
    int pixtype;
    int imhver, n, imu, pixoff, impixoff, immax, immin, imtime;
    int imndim, imlen, imphyslen, impixtype, pixswap, hpixswap, mtime;
    float rmax, rmin;

    headswap = -1;

    /* Set up last line of FITS header */
    (void)strncpy (endline,"END", 3);
    for (i = 3; i < 80; i++)
	endline[i] = ' ';
    endline[80] = 0;

    /* Check header magic word */
    imhver = head_version (irafheader);
    if (imhver < 1) {
	(void)fprintf(stderr, "File %s not valid IRAF image header\n",
		      hdrname);
	return(NULL);
	}
    if (imhver == 2) {
	nlines = 24 + ((nbiraf - LEN_IM2HDR) / 81);
	imndim = IM2_NDIM;
	imlen = IM2_LEN;
	imphyslen = IM2_PHYSLEN;
	impixtype = IM2_PIXTYPE;
	impixoff = IM2_PIXOFF;
	imtime = IM2_MTIME;
	immax = IM2_MAX;
	immin = IM2_MIN;
	}
    else {
	nlines = 24 + ((nbiraf - LEN_IMHDR) / 162);
	imndim = IM_NDIM;
	imlen = IM_LEN;
	imphyslen = IM_PHYSLEN;
	impixtype = IM_PIXTYPE;
	impixoff = IM_PIXOFF;
	imtime = IM_MTIME;
	immax = IM_MAX;
	immin = IM_MIN;
	}

    /*  Initialize FITS header */
    nblock = (nlines * 80) / 2880;
    *nbfits = (nblock + 5) * 2880 + 4;
    fitsheader = (char *) calloc (*nbfits, 1);
    if (fitsheader == NULL) {
	(void)fprintf(stderr, "IRAF2FITS Cannot allocate %d-byte FITS header\n",
		*nbfits);
	return (NULL);
	}
    hlength (fitsheader, *nbfits);
    fhead = fitsheader;
    (void)strncpy (fitsheader, endline, 80);
    hputl (fitsheader, "SIMPLE", 1);
    fhead = fhead + 80;

    /*  Set pixel size in FITS header */
    pixtype = irafgeti4 (irafheader, impixtype);
    switch (pixtype) {
	case TY_CHAR:
	    nbits = 8;
	    break;
	case TY_UBYTE:
	    nbits = 8;
	    break;
	case TY_SHORT:
	    nbits = 16;
	    break;
	case TY_USHORT:
	    nbits = -16;
	    break;
	case TY_INT:
	case TY_LONG:
	    nbits = 32;
	    break;
	case TY_REAL:
	    nbits = -32;
	    break;
	case TY_DOUBLE:
	    nbits = -64;
	    break;
	default:
	    (void)fprintf(stderr,"Unsupported data type: %d\n", pixtype);
	    return (NULL);
	}
    hputi4 (fitsheader,"BITPIX",nbits);
    hputcom (fitsheader,"BITPIX", "IRAF .imh pixel type");
    fhead = fhead + 80;

    /*  Set image dimensions in FITS header */
    nax = irafgeti4 (irafheader, imndim);
    hputi4 (fitsheader,"NAXIS",nax);
    hputcom (fitsheader,"NAXIS", "IRAF .imh naxis");
    fhead = fhead + 80;

    n = irafgeti4 (irafheader, imlen);
    hputi4 (fitsheader, "NAXIS1", n);
    hputcom (fitsheader,"NAXIS1", "IRAF .imh image naxis[1]");
    fhead = fhead + 80;

    if (nax > 1) {
	n = irafgeti4 (irafheader, imlen+4);
	hputi4 (fitsheader, "NAXIS2", n);
	hputcom (fitsheader,"NAXIS2", "IRAF .imh image naxis[2]");
	}
    else
	hputi4 (fitsheader, "NAXIS2", 1);
	hputcom (fitsheader,"NAXIS2", "IRAF .imh naxis[2]");
    fhead = fhead + 80;

    if (nax > 2) {
	n = irafgeti4 (irafheader, imlen+8);
	hputi4 (fitsheader, "NAXIS3", n);
	hputcom (fitsheader,"NAXIS3", "IRAF .imh image naxis[3]");
	fhead = fhead + 80;
	}
    if (nax > 3) {
	n = irafgeti4 (irafheader, imlen+12);
	hputi4 (fitsheader, "NAXIS4", n);
	hputcom (fitsheader,"NAXIS4", "IRAF .imh image naxis[4]");
	fhead = fhead + 80;
	}

    /* Set object name in FITS header */
    if (imhver == 2)
	objname = irafgetc (irafheader, IM2_TITLE, SZ_IM2TITLE);
    else
	objname = irafgetc2 (irafheader, IM_TITLE, SZ_IMTITLE);
    if ((lstr = strlen (objname)) < 8) {
	for (i = lstr; i < 8; i++)
	    objname[i] = ' ';
	objname[8] = 0;
	}
    hputs (fitsheader,"OBJECT",objname);
    hputcom (fitsheader,"OBJECT", "IRAF .imh title");
    free (objname);
    fhead = fhead + 80;

    /* Save physical axis lengths so image file can be read */
    n = irafgeti4 (irafheader, imphyslen);
    hputi4 (fitsheader, "NPAXIS1", n);
    hputcom (fitsheader,"NPAXIS1", "IRAF .imh physical naxis[1]");
    fhead = fhead + 80;
    if (nax > 1) {
	n = irafgeti4 (irafheader, imphyslen+4);
	hputi4 (fitsheader, "NPAXIS2", n);
	hputcom (fitsheader,"NPAXIS2", "IRAF .imh physical naxis[2]");
	fhead = fhead + 80;
	}
    if (nax > 2) {
	n = irafgeti4 (irafheader, imphyslen+8);
	hputi4 (fitsheader, "NPAXIS3", n);
	hputcom (fitsheader,"NPAXIS3", "IRAF .imh physical naxis[3]");
	fhead = fhead + 80;
	}
    if (nax > 3) {
	n = irafgeti4 (irafheader, imphyslen+12);
	hputi4 (fitsheader, "NPAXIS4", n);
	hputcom (fitsheader,"NPAXIS4", "IRAF .imh physical naxis[4]");
	fhead = fhead + 80;
	}

    /* Save image minimum and maximum in header */
    rmax = irafgetr4 (irafheader, immax);
    rmin = irafgetr4 (irafheader, immin);
    if (rmin != rmax) {
	hputr4 (fitsheader, "IRAFMIN", &rmin);
	fhead = fhead + 80;
	hputcom (fitsheader,"IRAFMIN", "IRAF .imh minimum");
	hputr4 (fitsheader, "IRAFMAX", &rmax);
	hputcom (fitsheader,"IRAFMAX", "IRAF .imh maximum");
	fhead = fhead + 80;
	}

    /* Save image header filename in header */
    nl = hputm (fitsheader,"IMHFIL",hdrname);
    if (nl > 0) {
	lname = strlen (hdrname);
	strcpy (fitsline, "IRAF header file name");
	if (lname < 43)
	    hputcom (fitsheader,"IMHFIL_1", fitsline);
	else if (lname > 67 && lname < 110)
	    hputcom (fitsheader,"IMHFIL_2", fitsline);
	else if (lname > 134 && lname < 177)
	    hputcom (fitsheader,"IMHFIL_3", fitsline);
	}
    if (nl > 0) fhead = fhead + (nl * 80);

    /* Save image pixel file pathname in header */
    if (imhver == 2)
	pixname = irafgetc (irafheader, IM2_PIXFILE, SZ_IM2PIXFILE);
    else
	pixname = irafgetc2 (irafheader, IM_PIXFILE, SZ_IMPIXFILE);
    if (strncmp(pixname, "HDR", 3) == 0 ) {
	newpixname = same_path (pixname, hdrname);
	free (pixname);
	pixname = newpixname;
	}
    if (strchr (pixname, '/') == NULL && strchr (pixname, '$') == NULL) {
	newpixname = same_path (pixname, hdrname);
	free (pixname);
	pixname = newpixname;
	}
	
    if ((bang = strchr (pixname, '!')) != NULL )
	nl = hputm (fitsheader,"PIXFIL",bang+1);
    else
	nl = hputm (fitsheader,"PIXFIL",pixname);
    free (pixname);
    if (nl > 0) {
	strcpy (fitsline, "IRAF .pix pixel file");
	if (lname < 43)
	    hputcom (fitsheader,"PIXFIL_1", fitsline);
	else if (lname > 67 && lname < 110)
	    hputcom (fitsheader,"PIXFIL_2", fitsline);
	else if (lname > 134 && lname < 177)
	    hputcom (fitsheader,"PIXFIL_3", fitsline);
	}
    if (nl > 0) fhead = fhead + (nl * 80);

    /* Save image offset from star of pixel file */
    pixoff = irafgeti4 (irafheader, impixoff);
    pixoff = (pixoff - 1) * 2;
    hputi4 (fitsheader, "PIXOFF", pixoff);
    hputcom (fitsheader,"PIXOFF", "IRAF .pix pixel offset (Do not change!)");
    fhead = fhead + 80;

    /* Save IRAF file format version in header */
    hputi4 (fitsheader,"IMHVER",imhver);
    hputcom (fitsheader,"IMHVER", "IRAF .imh format version (1 or 2)");
    fhead = fhead + 80;

    /* Set flag if header numbers are byte-reversed on this machine */
    if (machswap() != headswap)
	hputl (fitsheader, "HEADSWAP", 1);
    else
	hputl (fitsheader, "HEADSWAP", 0);
    hputcom (fitsheader,"HEADSWAP", "IRAF header, FITS byte orders differ if T");
    fhead = fhead + 80;

    /* Set flag if image pixels are byte-reversed on this machine */
    if (imhver == 2) {
	hpixswap = irafgeti4 (irafheader, IM2_SWAPPED);
	if (headswap && !hpixswap)
	    pixswap = 1;
	else if (!headswap && hpixswap)
	    pixswap = 1;
	else
	    pixswap = 0;
	}
    else
	pixswap = headswap;
    if (machswap() != pixswap)
	hputl (fitsheader, "PIXSWAP", 1);
    else
	hputl (fitsheader, "PIXSWAP", 0);
    hputcom (fitsheader,"PIXSWAP", "IRAF pixels, FITS byte orders differ if T");
    fhead = fhead + 80;

    /* Read modification time */
    mtime = irafgeti4 (irafheader, imtime);
    if (mtime == 0)
	dstring = lt2fd ();
    else
	dstring = tsi2fd (mtime);
    hputs (fitsheader, "DATE-MOD", dstring);
    hputcom (fitsheader,"DATE-MOD", "Date of latest file modification");
    free (dstring);
    fhead = fhead + 80;

    /* Add user portion of IRAF header to FITS header */
    fitsline[80] = 0;
    if (imhver == 2) {
	imu = LEN_IM2HDR;
	chead = irafheader;
	j = 0;
	for (k = 0; k < 80; k++)
	    fitsline[k] = ' ';
	for (i = imu; i < nbiraf; i++) {
	    irafchar = chead[i];
	    if (irafchar == 0)
		break;
	    else if (irafchar == 10) {
		(void)strncpy (fhead, fitsline, 80);
		/* fprintf (stderr,"%80s\n",fitsline); */
		if (strncmp (fitsline, "OBJECT ", 7) != 0) {
		    fhead = fhead + 80;
		    }
		for (k = 0; k < 80; k++)
		    fitsline[k] = ' ';
		j = 0;
		}
	    else {
		if (j > 80) {
		    if (strncmp (fitsline, "OBJECT ", 7) != 0) {
			(void)strncpy (fhead, fitsline, 80);
			/* fprintf (stderr,"%80s\n",fitsline); */
			j = 9;
			fhead = fhead + 80;
			}
		    for (k = 0; k < 80; k++)
			fitsline[k] = ' ';
		    }
		if (irafchar > 32 && irafchar < 127)
		    fitsline[j] = irafchar;
		j++;
		}
	    }
	}
    else {
	imu = LEN_IMHDR;
	chead = irafheader;
	if (headswap == 1)
	    ib = 0;
	else
	    ib = 1;
	for (k = 0; k < 80; k++)
	    fitsline[k] = ' ';
	j = 0;
	for (i = imu; i < nbiraf; i=i+2) {
	    irafchar = chead[i+ib];
	    if (irafchar == 0)
		break;
	    else if (irafchar == 10) {
		if (strncmp (fitsline, "OBJECT ", 7) != 0) {
		    (void)strncpy (fhead, fitsline, 80);
		    fhead = fhead + 80;
		    }
		/* fprintf (stderr,"%80s\n",fitsline); */
		j = 0;
		for (k = 0; k < 80; k++)
		    fitsline[k] = ' ';
		}
	    else {
		if (j > 80) {
		    if (strncmp (fitsline, "OBJECT ", 7) != 0) {
			(void)strncpy (fhead, fitsline, 80);
			j = 9;
			fhead = fhead + 80;
			}
		    /* fprintf (stderr,"%80s\n",fitsline); */
		    for (k = 0; k < 80; k++)
			fitsline[k] = ' ';
		    }
		if (irafchar > 32 && irafchar < 127)
		    fitsline[j] = irafchar;
		j++;
		}
	    }
	}

    /* Add END to last line */
    (void)strncpy (fhead, endline, 80);

    /* Find end of last 2880-byte block of header */
    fhead = ksearch (fitsheader, "END") + 80;
    nblock = *nbfits / 2880;
    fhead1 = fitsheader + (nblock * 2880);

    /* Pad rest of header with spaces */
    strncpy (endline,"   ",3);
    for (fp = fhead; fp < fhead1; fp = fp + 80) {
	(void)strncpy (fp, endline,80);
	}

    return (fitsheader);
}


int
irafwhead (hdrname, lhead, irafheader, fitsheader)

char	*hdrname;	/* Name of IRAF header file */
int	lhead;		/* Length of IRAF header */
char	*irafheader;	/* IRAF header */
char	*fitsheader;	/* FITS image header */

{
    int fd;
    int nbw, nbhead, lphead, pixswap;

   /* Get rid of redundant header information */
    hgeti4 (fitsheader, "PIXOFF", &lphead);
    hgeti4 (fitsheader, "PIXSWAP", &pixswap);

    /* Write IRAF header file */

    /* Convert FITS header to IRAF header */
    irafheader = fits2iraf (fitsheader, irafheader, lhead, &nbhead);
    if (irafheader == NULL) {
	fprintf (stderr, "IRAFWIMAGE:  file %s header error\n", hdrname);
	return (-1);
	}

    /* Open the output file */
    if (!access (hdrname, 0)) {
	fd = open (hdrname, O_WRONLY);
	if (fd < 3) {
	    fprintf (stderr, "IRAFWIMAGE:  file %s not writeable\n", hdrname);
	    return (0);
	    }
	}
    else {
	fd = open (hdrname, O_RDWR+O_CREAT, 0666);
	if (fd < 3) {
	    fprintf (stderr, "IRAFWIMAGE:  cannot create file %s\n", hdrname);
	    return (0);
	    }
	}

    /* Write IRAF header to disk file */
    nbw = write (fd, irafheader, nbhead);
    (void) ftruncate (fd, nbhead);
    close (fd);
    if (nbw < nbhead) {
	(void)fprintf(stderr, "IRAF header file %s: %d / %d bytes written.\n",
		      hdrname, nbw, nbhead);
	return (-1);
	}

    return (nbw);
}

/* IRAFWIMAGE -- write IRAF .imh header file and .pix image file
 * No matter what the input, this always writes in the local byte order */

int
irafwimage (hdrname, lhead, irafheader, fitsheader, image )

char	*hdrname;	/* Name of IRAF header file */
int	lhead;		/* Length of IRAF header */
char	*irafheader;	/* IRAF header */
char	*fitsheader;	/* FITS image header */
char	*image;		/* IRAF image */

{
    int fd;
    char *bang;
    int nbw, bytepix, bitpix, naxis, naxis1, naxis2, nbimage, lphead;
    char *pixn, *newpixname;
    char pixname[SZ_IM2PIXFILE+1];
    int imhver, pixswap;

    hgeti4 (fitsheader, "IMHVER", &imhver);

    if (!hgetm (fitsheader, "PIXFIL", SZ_IM2PIXFILE, pixname)) {
	if (imhver == 2)
	    pixn = irafgetc (irafheader, IM2_PIXFILE, SZ_IM2PIXFILE);
	else
	    pixn = irafgetc2 (irafheader, IM_PIXFILE, SZ_IMPIXFILE);
	if (strncmp(pixn, "HDR", 3) == 0 ) {
	    newpixname = same_path (pixn, hdrname);
	    strcpy (pixname, newpixname);
	    }
	else {
	    if ((bang = strchr (pixn, '!')) != NULL )
		strcpy (pixname, bang+1);
	    else
		strcpy (pixname, pixn);
	    }
	free (pixn);
        }

    /* Find number of bytes to write */
    hgeti4 (fitsheader,"NAXIS",&naxis);
    hgeti4 (fitsheader,"NAXIS1",&naxis1);
    hgeti4 (fitsheader,"NAXIS2",&naxis2);
    hgeti4 (fitsheader,"BITPIX",&bitpix);
    if (bitpix < 0)
	bytepix = -bitpix / 8;
    else
	bytepix = bitpix / 8;

    /* If either dimension is one and image is 3-D, read all three dimensions */
    if (naxis == 3 && ((naxis1 == 1) | (naxis2 == 1))) {
	int naxis3;
	hgeti4 (fitsheader,"NAXIS3",&naxis3);
	nbimage = naxis1 * naxis2 * naxis3 * bytepix;
	}
    else
	nbimage = naxis1 * naxis2 * bytepix;

   /* Read information about pixel file from header */
    hgeti4 (fitsheader, "PIXOFF", &lphead);
    hgeti4 (fitsheader, "PIXSWAP", &pixswap);

    /* Write IRAF header file */
    if (irafwhead (hdrname, lhead, irafheader, fitsheader))
        return (0);

    /* Open the output file */
    if (!access (pixname, 0)) {
	fd = open (pixname, O_WRONLY);
	if (fd < 3) {
	    fprintf (stderr, "IRAFWIMAGE:  file %s not writeable\n", pixname);
	    return (0);
	    }
	}
    else {
	fd = open (pixname, O_RDWR+O_CREAT, 0666);
	if (fd < 3) {
	    fprintf (stderr, "IRAFWIMAGE:  cannot create file %s\n", pixname);
	    return (0);
	    }
	}

    /* Write header to IRAF pixel file */
    if (imhver == 2)
	irafputc ("impv2", irafheader, 0, 5);
    else
	irafputc2 ("impix", irafheader, 0, 5);
    nbw = write (fd, irafheader, lphead);

    /* Byte-reverse image, if necessary */
    if (pixswap)
	irafswap (bitpix, image, nbimage);

    /* Write data to IRAF pixel file */
    nbw = write (fd, image, nbimage);
    close (fd);

    free (pixname);
    return (nbw);
}


/* Put filename and header path together */

static char *
same_path (pixname, hdrname)

char	*pixname;	/* IRAF pixel file pathname */
char	*hdrname;	/* IRAF image header file pathname */

{
    int len;
    char *newpixname;

    newpixname = (char *) calloc (SZ_IM2PIXFILE, 1);

    /* Pixel file is in same directory as header */
    if (strncmp(pixname, "HDR$", 4) == 0 ) {
	(void)strncpy (newpixname, hdrname, SZ_IM2PIXFILE);

	/* find the end of the pathname */
	len = strlen (newpixname);
#ifndef VMS
	while( (len > 0) && (newpixname[len-1] != '/') )
#else
	while( (len > 0) && (newpixname[len-1] != ']') && (newpixname[len-1] != ':') )
#endif
	    len--;

	/* add name */
	newpixname[len] = '\0';
	(void)strncat (newpixname, &pixname[4], SZ_IM2PIXFILE);
	}

    /* Bare pixel file with no path is assumed to be same as HDR$filename */
    else if (strchr (pixname, '/') == NULL && strchr (pixname, '$') == NULL) {
	(void)strncpy (newpixname, hdrname, SZ_IM2PIXFILE);

	/* find the end of the pathname */
	len = strlen (newpixname);
#ifndef VMS
	while( (len > 0) && (newpixname[len-1] != '/') )
#else
	while( (len > 0) && (newpixname[len-1] != ']') && (newpixname[len-1] != ':') )
#endif
	    len--;

	/* add name */
	newpixname[len] = '\0';
	(void)strncat (newpixname, pixname, SZ_IM2PIXFILE);
	}

    /* Pixel file has same name as header file, but with .pix extension */
    else if (strncmp (pixname, "HDR", 3) == 0) {

	/* load entire header name string into name buffer */
	(void)strncpy (newpixname, hdrname, SZ_IM2PIXFILE);
	len = strlen (newpixname);
	newpixname[len-3] = 'p';
	newpixname[len-2] = 'i';
	newpixname[len-1] = 'x';
	}

    return (newpixname);
}

/* Convert FITS image header to IRAF image header, returning IRAF header */
/* No matter what the input, this always writes in the local byte order */

char *
fits2iraf (fitsheader, irafheader, nbhead, nbiraf)

char	*fitsheader;	/* FITS image header */
char	*irafheader;	/* IRAF image header (returned updated) */
int	nbhead;		/* Length of IRAF header */
int	*nbiraf;	/* Length of returned IRAF header */

{
    int i, n, pixoff, lhdrdir;
    short *irafp, *irafs, *irafu;
    char *iraf2u, *iraf2p, *filename, *hdrdir;
    char *fitsend, *fitsp, pixfile[SZ_IM2PIXFILE], hdrfile[SZ_IM2HDRFILE];
    char title[SZ_IM2TITLE], temp[80];
    int	nax, nlfits, imhver, nbits, pixtype, hdrlength, mtime;
    int imndim, imlen, imphyslen, impixtype, imhlen, imtime, immax, immin;
    float rmax, rmin;

    hgeti4 (fitsheader, "IMHVER", &imhver);
    hdel (fitsheader, "IMHVER");
    hdel (fitsheader, "IMHVER");
    hgetl (fitsheader, "HEADSWAP", &headswap);
    hdel (fitsheader, "HEADSWAP");
    hdel (fitsheader, "HEADSWAP");
    if (imhver == 2) {
	imhlen = IM2_HDRLEN;
	imndim = IM2_NDIM;
	imlen = IM2_LEN;
	imtime = IM2_MTIME;
	imphyslen = IM2_PHYSLEN;
	impixtype = IM2_PIXTYPE;
	immax = IM2_MAX;
	immin = IM2_MIN;
	}
    else {
	imhlen = IM_HDRLEN;
	imndim = IM_NDIM;
	imlen = IM_LEN;
	imtime = IM_MTIME;
	imphyslen = IM_PHYSLEN;
	impixtype = IM_PIXTYPE;
	immax = IM_MAX;
	immin = IM_MIN;
	}

    /* Delete FITS header keyword not needed by IRAF */
    hdel (fitsheader,"SIMPLE");

    /* Set IRAF image data type */
    hgeti4 (fitsheader,"BITPIX", &nbits);
    switch (nbits) {
	case 8:
	    pixtype = TY_CHAR;
	    break;
	case -8:
	    pixtype = TY_UBYTE;
	    break;
	case 16:
	    pixtype = TY_SHORT;
	    break;
	case -16:
	    pixtype = TY_USHORT;
	    break;
	case 32:
	    pixtype = TY_INT;
	    break;
	case -32:
	    pixtype = TY_REAL;
	    break;
	case -64:
	    pixtype = TY_DOUBLE;
	    break;
	default:
	    (void)fprintf(stderr,"Unsupported data type: %d\n", nbits);
	    return (NULL);
	}
    irafputi4 (irafheader, impixtype, pixtype);
    hdel (fitsheader,"BITPIX");

    /* Set IRAF image dimensions */
    hgeti4 (fitsheader,"NAXIS",&nax);
    irafputi4 (irafheader, imndim, nax);
    hdel (fitsheader,"NAXIS");

    hgeti4 (fitsheader, "NAXIS1", &n);
    irafputi4 (irafheader, imlen, n);
    irafputi4 (irafheader, imphyslen, n);
    hdel (fitsheader,"NAXIS1");

    hgeti4 (fitsheader,"NAXIS2",&n);
    irafputi4 (irafheader, imlen+4, n);
    irafputi4 (irafheader, imphyslen+4, n);
    hdel (fitsheader,"NAXIS2");

    if (nax > 2) {
	hgeti4 (fitsheader,"NAXIS3",&n);
	irafputi4 (irafheader, imlen+8, n);
	irafputi4 (irafheader, imphyslen+8, n);
	hdel (fitsheader,"NAXIS3");
	}

    if (nax > 3) {
	hgeti4 (fitsheader,"NAXIS4",&n);
	irafputi4 (irafheader, imlen+12, n);
	irafputi4 (irafheader, imphyslen+12, n);
	hdel (fitsheader,"NAXIS4");
	}

    /* Set image pixel value limits */
    rmin = 0.0;
    hgetr4 (fitsheader, "IRAFMIN", &rmin);
    rmax = 0.0;
    hgetr4 (fitsheader, "IRAFMAX", &rmax);
    if (rmin != rmax) {
	irafputr4 (irafheader, immax, rmax);
	irafputr4 (irafheader, immin, rmin);
	}
    hdel (fitsheader, "IRAFMIN");
    hdel (fitsheader, "IRAFMAX");

    /* Replace pixel file name, if it is in the FITS header */
    if (hgetm (fitsheader, "PIXFIL", SZ_IM2PIXFILE, pixfile)) {
	if (strchr (pixfile, '/')) {
	    if (hgetm (fitsheader, "IMHFIL", SZ_IM2HDRFILE, hdrfile)) {
		hdrdir = strrchr (hdrfile, '/');
		if (hdrdir != NULL) {
		    lhdrdir = hdrdir - hdrfile + 1;
		    if (!strncmp (pixfile, hdrfile, lhdrdir)) {
			filename = pixfile + lhdrdir;
			strcpy (temp, "HDR$");
			strcat (temp,filename);
			strcpy (pixfile, temp);
			}
		    }
		if (pixfile[0] != '/' && pixfile[0] != 'H') {
		    strcpy (temp, "HDR$");
		    strcat (temp,pixfile);
		    strcpy (pixfile, temp);
		    }
		}
	    }

	if (imhver == 2)
            irafputc (pixfile, irafheader, IM2_PIXFILE, SZ_IM2PIXFILE);
	else
            irafputc2 (pixfile, irafheader, IM_PIXFILE, SZ_IMPIXFILE);
	hdel (fitsheader,"PIXFIL_1");
	hdel (fitsheader,"PIXFIL_2");
	hdel (fitsheader,"PIXFIL_3");
	hdel (fitsheader,"PIXFIL_4");
	}

    /* Replace header file name, if it is in the FITS header */
    if (hgetm (fitsheader, "IMHFIL", SZ_IM2HDRFILE, pixfile)) {
	if (!strchr (pixfile,'/') && !strchr (pixfile,'$')) {
	    strcpy (temp, "HDR$");
	    strcat (temp,pixfile);
	    strcpy (pixfile, temp);
	    }
	if (imhver == 2)
            irafputc (pixfile, irafheader, IM2_HDRFILE, SZ_IM2HDRFILE);
	else
            irafputc2 (pixfile, irafheader, IM_HDRFILE, SZ_IMHDRFILE);
	hdel (fitsheader, "IMHFIL_1");
	hdel (fitsheader, "IMHFIL_2");
	hdel (fitsheader, "IMHFIL_3");
	hdel (fitsheader, "IMHFIL_4");
	}

    /* Replace image title, if it is in the FITS header */
    if (hgets (fitsheader, "OBJECT", SZ_IM2TITLE, title)) {
	if (imhver == 2)
            irafputc (title, irafheader, IM2_TITLE, SZ_IM2TITLE);
	else
            irafputc2 (title, irafheader, IM_TITLE, SZ_IMTITLE);
	hdel (fitsheader, "OBJECT");
	}
    hgeti4 (fitsheader, "PIXOFF", &pixoff);
    hdel (fitsheader, "PIXOFF");
    hdel (fitsheader, "PIXOFF");
    hdel (fitsheader, "PIXSWAP");
    hdel (fitsheader, "PIXSWAP");
    hdel (fitsheader, "DATE-MOD");
    hdel (fitsheader, "DATE-MOD");
    fitsend = ksearch (fitsheader,"END");

    /* Find length of FITS header */
    fitsend = ksearch (fitsheader,"END");
    nlfits = ((fitsend - fitsheader) / 80);

    /* Find new length of IRAF header */
    if (imhver == 2)
	*nbiraf = LEN_IM2HDR + (81 * nlfits);
    else
	*nbiraf = LEN_IMHDR + (162 * nlfits);
    if (*nbiraf > nbhead)
	irafheader = realloc (irafheader, *nbiraf);

    /* Reset modification time */
    mtime = lt2tsi ();
    irafputi4 (irafheader, imtime, mtime);

    /*  Replace user portion of IRAF header with remaining FITS header */
    if (imhver == 2) {
	iraf2u = irafheader + LEN_IM2HDR;
	iraf2p = iraf2u;
	for (fitsp = fitsheader; fitsp < fitsend; fitsp = fitsp + 80) {
	    for (i = 0; i < 80; i++)
		*iraf2p++ = fitsp[i];
	    *iraf2p++ = 10;
	    }
	*iraf2p++ = 0;
	*nbiraf = iraf2p - irafheader;
	hdrlength = 1 + *nbiraf / 2;
	}
    else {
	irafs = (short *)irafheader;
	irafu = irafs + (LEN_IMHDR / 2);
	irafp = irafu;
	for (fitsp = fitsheader; fitsp < fitsend; fitsp = fitsp + 80) {
	    for (i = 0; i < 80; i++)
		*irafp++ = (short) fitsp[i];
	    *irafp++ = 10;
	    }
	*irafp++ = 0;
	*irafp++ = 32;
	*nbiraf = 2 * (irafp - irafs);
	hdrlength = *nbiraf / 4;
	}

    /* Length of header file */
    irafputi4 (irafheader, imhlen, hdrlength);

    /* Offset in .pix file to first pixel data
    hputi4 (fitsheader, "PIXOFF", pixoff); */

    /* Return number of bytes in new IRAF header */
    return (irafheader);
}


int
irafgeti4 (irafheader, offset)

char	*irafheader;	/* IRAF image header */
int	offset;		/* Number of bytes to skip before number */

{
    char *ctemp, *cheader;
    int  temp;

    cheader = irafheader;
    ctemp = (char *) &temp;

    /* If header swap flag not set, set it now */
    if (headswap < 0) {
	if (cheader[offset] > 0)
	    headswap = 1;
	else
	    headswap = 0;
	}

    if (machswap() != headswap) {
	ctemp[3] = cheader[offset];
	ctemp[2] = cheader[offset+1];
	ctemp[1] = cheader[offset+2];
	ctemp[0] = cheader[offset+3];
	}
    else {
	ctemp[0] = cheader[offset];
	ctemp[1] = cheader[offset+1];
	ctemp[2] = cheader[offset+2];
	ctemp[3] = cheader[offset+3];
	}
    return (temp);
}


float
irafgetr4 (irafheader, offset)

char	*irafheader;	/* IRAF image header */
int	offset;		/* Number of bytes to skip before number */

{
    char *ctemp, *cheader;
    float  temp;

    cheader = irafheader;
    ctemp = (char *) &temp;

    /* If header swap flag not set, set it now */
    if (headswap < 0) {
	if (cheader[offset] > 0)
	    headswap = 1;
	else
	    headswap = 0;
	}

    if (machswap() != headswap) {
	ctemp[3] = cheader[offset];
	ctemp[2] = cheader[offset+1];
	ctemp[1] = cheader[offset+2];
	ctemp[0] = cheader[offset+3];
	}
    else {
	ctemp[0] = cheader[offset];
	ctemp[1] = cheader[offset+1];
	ctemp[2] = cheader[offset+2];
	ctemp[3] = cheader[offset+3];
	}
    return (temp);
}


/* IRAFGETC2 -- Get character string from arbitrary part of v.1 IRAF header */

char *
irafgetc2 (irafheader, offset, nc)

char	*irafheader;	/* IRAF image header */
int	offset;		/* Number of bytes to skip before string */
int	nc;		/* Maximum number of characters in string */

{
    char *irafstring, *string;

    irafstring = irafgetc (irafheader, offset, 2*(nc+1));
    string = iraf2str (irafstring, nc);
    free (irafstring);

    return (string);
}


/* IRAFGETC -- Get character string from arbitrary part of IRAF header */

char *
irafgetc (irafheader, offset, nc)

char	*irafheader;	/* IRAF image header */
int	offset;		/* Number of bytes to skip before string */
int	nc;		/* Maximum number of characters in string */

{
    char *ctemp, *cheader;
    int i;

    cheader = irafheader;
    ctemp = (char *) calloc (nc+1, 1);
    if (ctemp == NULL) {
	(void)fprintf(stderr, "IRAFGETC Cannot allocate %d-byte variable\n",
		nc+1);
	return (NULL);
	}
    for (i = 0; i < nc; i++) {
	ctemp[i] = cheader[offset+i];
	if (ctemp[i] > 0 && ctemp[i] < 32)
	    ctemp[i] = ' ';
	}

    return (ctemp);
}


/* Convert IRAF 2-byte/char string to 1-byte/char string */

char *
iraf2str (irafstring, nchar)

char	*irafstring;	/* IRAF 2-byte/character string */
int	nchar;		/* Number of characters in string */
{
    char *string;
    int i, j;

    /* Set swap flag according to position of nulls in 2-byte characters */
    if (headswap < 0) {
	if (irafstring[0] != 0 && irafstring[1] == 0)
	    headswap = 1;
	else if (irafstring[0] == 0 && irafstring[1] != 0)
	    headswap = 0;
	else
	    return (NULL);
	}

    string = (char *) calloc (nchar+1, 1);
    if (string == NULL) {
	(void)fprintf(stderr, "IRAF2STR Cannot allocate %d-byte variable\n",
		nchar+1);
	return (NULL);
	}

    /* Swap bytes, if requested */
    if (headswap)
	j = 0;
    else
	j = 1;

    /* Convert appropriate byte of input to output character */
    for (i = 0; i < nchar; i++) {
	string[i] = irafstring[j];
	j = j + 2;
	}

    return (string);
}


/* IRAFPUTI4 -- Insert 4-byte integer into arbitrary part of IRAF header */

static void
irafputi4 (irafheader, offset, inum)

char	*irafheader;	/* IRAF image header */
int	offset;		/* Number of bytes to skip before number */
int	inum;		/* Number to put into header */

{
    char *cn, *chead;

    chead = irafheader;
    cn = (char *) &inum;
    if (headswap < 0)
	headswap = 0;
    if (headswap != machswap()) {
	chead[offset+3] = cn[0];
	chead[offset+2] = cn[1];
	chead[offset+1] = cn[2];
	chead[offset] = cn[3];
	}
    else {
	chead[offset] = cn[0];
	chead[offset+1] = cn[1];
	chead[offset+2] = cn[2];
	chead[offset+3] = cn[3];
	}
    return;
}


/* IRAFPUTR4 -- Insert 4-byte real number into arbitrary part of IRAF header */

static void
irafputr4 (irafheader, offset, rnum)

char	*irafheader;	/* IRAF image header */
int	offset;		/* Number of bytes to skip before number */
float	rnum;		/* Number to put into header */

{
    char *cn, *chead;

    chead = irafheader;
    cn = (char *) &rnum;
    if (headswap < 0)
	headswap = 0;
    if (headswap != machswap()) {
	chead[offset+3] = cn[0];
	chead[offset+2] = cn[1];
	chead[offset+1] = cn[2];
	chead[offset] = cn[3];
	}
    else {
	chead[offset] = cn[0];
	chead[offset+1] = cn[1];
	chead[offset+2] = cn[2];
	chead[offset+3] = cn[3];
	}
    return;
}


/* IRAFPUTC2 -- Insert character string into arbitrary part of v.1 IRAF header */

static void
irafputc2 (string, irafheader, offset, nc)

char	*string;	/* String to insert into header */
char	*irafheader;	/* IRAF image header */
int	offset;		/* Number of bytes to skip before string */
int	nc;		/* Maximum number of characters in string */

{
    char *irafstring;

    irafstring = (char *) calloc (2 * nc, 1);
    if (irafstring == NULL) {
	(void)fprintf(stderr, "IRAFPUTC2 Cannot allocate %d-byte variable\n",
		2 * nc);
	}
    str2iraf (string, irafstring, nc);
    irafputc (irafstring, irafheader, offset, 2*nc);

    return;
}


/* IRAFPUTC -- Insert character string into arbitrary part of IRAF header */

static void
irafputc (string, irafheader, offset, nc)

char	*string;	/* String to insert into header */
char	*irafheader;	/* IRAF image header */
int	offset;		/* Number of bytes to skip before string */
int	nc;		/* Maximum number of characters in string */

{
    char *chead;
    int i;

    chead = irafheader;
    for (i = 0; i < nc; i++)
	chead[offset+i] = string[i];

    return;
}


/* STR2IRAF -- Convert 1-byte/char string to IRAF 2-byte/char string */

static void
str2iraf (string, irafstring, nchar)

char	*string;	/* 1-byte/character string */
char	*irafstring;	/* IRAF 2-byte/character string */
int	nchar;		/* Maximum number of characters in IRAF string */
{
    int i, j, nc, nbytes;

    nc = strlen (string);

    /* Fill output string with zeroes */
    nbytes = nchar * 2;
    for (i = 0; i < nbytes; i++)
	irafstring[i] = 0;

    /* If swapped, start with first byte of 2-byte characters */
    if (headswap)
	j = 0;
    else
	j = 1;

    /* Move input characters to appropriate bytes of output */
    for (i = 0; i < nchar; i++) {
	if (i > nc)
	    irafstring[j] = 0;
	else
	    irafstring[j] = string[i];
	j = j + 2;
	}

    return;
}


/* IRAFSWAP -- Reverse bytes of any type of vector in place */

static void
irafswap (bitpix, string, nbytes)

int	bitpix;		/* Number of bits per pixel */
			/*  16 = short, -16 = unsigned short, 32 = int */
			/* -32 = float, -64 = double */
char	*string;	/* Address of starting point of bytes to swap */
int	nbytes;		/* Number of bytes to swap */

{
    switch (bitpix) {

	case 16:
	    if (nbytes < 2) return;
	    irafswap2 (string,nbytes);
	    break;

	case 32:
	    if (nbytes < 4) return;
	    irafswap4 (string,nbytes);
	    break;

	case -16:
	    if (nbytes < 2) return;
	    irafswap2 (string,nbytes);
	    break;

	case -32:
	    if (nbytes < 4) return;
	    irafswap4 (string,nbytes);
	    break;

	case -64:
	    if (nbytes < 8) return;
	    irafswap8 (string,nbytes);
	    break;

	}
    return;
}


/* IRAFSWAP2 -- Swap bytes in string in place */

static void
irafswap2 (string,nbytes)


char *string;	/* Address of starting point of bytes to swap */
int nbytes;	/* Number of bytes to swap */

{
    char *sbyte, temp, *slast;

    slast = string + nbytes;
    sbyte = string;
    while (sbyte < slast) {
	temp = sbyte[0];
	sbyte[0] = sbyte[1];
	sbyte[1] = temp;
	sbyte= sbyte + 2;
	}
    return;
}


/* IRAFSWAP4 -- Reverse bytes of Integer*4 or Real*4 vector in place */

static void
irafswap4 (string,nbytes)

char *string;	/* Address of Integer*4 or Real*4 vector */
int nbytes;	/* Number of bytes to reverse */

{
    char *sbyte, *slast;
    char temp0, temp1, temp2, temp3;

    slast = string + nbytes;
    sbyte = string;
    while (sbyte < slast) {
	temp3 = sbyte[0];
	temp2 = sbyte[1];
	temp1 = sbyte[2];
	temp0 = sbyte[3];
	sbyte[0] = temp0;
	sbyte[1] = temp1;
	sbyte[2] = temp2;
	sbyte[3] = temp3;
	sbyte = sbyte + 4;
	}

    return;
}


/* IRAFSWAP8 -- Reverse bytes of Real*8 vector in place */

static void
irafswap8 (string,nbytes)

char *string;	/* Address of Real*8 vector */
int nbytes;	/* Number of bytes to reverse */

{
    char *sbyte, *slast;
    char temp[8];

    slast = string + nbytes;
    sbyte = string;
    while (sbyte < slast) {
	temp[7] = sbyte[0];
	temp[6] = sbyte[1];
	temp[5] = sbyte[2];
	temp[4] = sbyte[3];
	temp[3] = sbyte[4];
	temp[2] = sbyte[5];
	temp[1] = sbyte[6];
	temp[0] = sbyte[7];
	sbyte[0] = temp[0];
	sbyte[1] = temp[1];
	sbyte[2] = temp[2];
	sbyte[3] = temp[3];
	sbyte[4] = temp[4];
	sbyte[5] = temp[5];
	sbyte[6] = temp[6];
	sbyte[7] = temp[7];
	sbyte = sbyte + 8;
	}
    return;
}


/* Set flag if machine on which program is executing is not FITS byte order
 * ( i.e., if it is an Alpha or PC instead of a Sun ) */

static int
machswap ()

{
    char *ctest;
    int itest;

    itest = 1;
    ctest = (char *)&itest;
    if (*ctest)
	return (1);
    else
	return (0);
}


/* ISIRAF -- return 1 if IRAF imh file, else 0 */

int
isiraf (filename)

char	*filename;	/* Name of file for which to find size */
{
    if (strchr (filename, '='))
	return (0);
    else if (strsrch (filename, ".imh"))
	return (1);
    else
	return (0);
}


/* IRAFSIZE -- return size of file in bytes */

static int
irafsize (diskfile)

FILE *diskfile;		/* Descriptor of file for which to find size */
{
    long filesize;
    long offset;

    offset = (long) 0;

    /* Move to end of the file */
    if (fseek (diskfile, offset, SEEK_END) == 0) {

 	/* Position is the size of the file */
	filesize = ftell (diskfile);

	/* Move file pointer back tot he start of the file */
	fseek (diskfile, offset, SEEK_SET);
	}

    else
	filesize = -1;

    return (filesize);
}

/* Feb 15 1996	New file
 * Apr 10 1996	Add more documentation
 * Apr 17 1996	Print error message on open failure
 * Jun  5 1996	Add byte swapping (reversal); use streams
 * Jun 10 1996	Make fixes after running lint
 * Jun 12 1996	Use IMSWAP subroutines instead of local ones
 * Jul  3 1996	Go back to using local IRAFSWAP subroutines
 * Jul  3 1996	Write to pixel file from FITS header
 * Jul 10 1996	Allocate all headers
 * Aug 13 1996	Add unistd.h to include list
 * Aug 26 1996	Allow 1-d images; fix comments; fix arguments after lint
 * Aug 26 1996	Add IRAF header lingth argument to IRAFWIMAGE and IRAFWHEAD
 * Aug 28 1996	Clean up code in IRAF2FITS
 * Aug 30 1996	Use write instead of fwrite
 * Sep  4 1996	Fix write mode bug
 * Oct 15 1996	Drop unused variables
 * Oct 17 1996	Minor fix after lint; cast arguments to STR2IRAF
 *
 * May 15 1997	Fix returned header length in IRAF2FITS
 * Dec 19 1997	Add IRAF version 2 .imh files
 *
 * Jan  2 1998	Allow uneven length of user parameter lines in IRAF headers
 * Jan  6 1998	Fix output of imh2 headers; allow newlines in imh1 headers
 * Jan 14 1998	Handle byte reversing correctly
 * Apr 17 1998	Add new IRAF data types unsigned char and unsigned short
 * Apr 30 1998  Fix error return if illegal data type after Allan Brighton
 * May 15 1998	Delete header keywords used for IRAF binary values
 * May 15 1998	Fix bug so FITS OBJECT is put into IRAF title
 * May 26 1998	Fix bug in fits2iraf keeping track of end of header
 * May 27 1998	Include fitsio.h instead of fitshead.h
 * Jun  4 1998	Write comments into header for converted IRAF binary values
 * Jun  4 1998	Pad FITS strings to 8 character minimum
 * Jul 24 1998	Write header file length to IRAF header file
 * Jul 27 1998	Print error messages to stderr for all failed malloc's
 * Jul 27 1998	Fix bug padding FITS header with spaces in iraf2fits
 * Jul 27 1998	Write modification time to IRAF header file
 * Aug  6 1998	Change fitsio.h to fitsfile.h; imhio.c to imhfile.c
 * Oct  1 1998	Set irafswap flag only once per file
 * Oct  5 1998	Add subroutines irafsize() and isiraf()
 * Nov 16 1998	Fix byte-swap checking
 *
 * Jan 27 1999	Read and write all of 3D image if one dimension is =1
 * Jul 13 1999	Improve error messages; change irafsize() argument to fd
 * Sep 22 1999	Don't copy OBJECT keyword from .imh file; use binary title
 * Oct 14 1999	Set FITS header length
 * Oct 20 1999	Allocate 5000 extra bytes for IRAF header
 * Nov  2 1999	Fix getclocktime() to use only time.h subroutines
 * Nov  2 1999	Add modification date and time to FITS header in iraf2fits()
 * Nov 24 1999	Delete HEADSWAP, IMHVER, DATE-MOD from header before writing
 * Nov 29 1999	Delete PIXSWAP, IRAF-MIN, IRAF-MAX from header before writing
 *
 * Jan 13 2000	Fix bug which dropped characters in iraf2fits()
 * Feb  3 2000	Declare timezone long, not time_t; drop unused variable
 * Mar  7 2000	Add more code to keep pixel file path short
 * Mar 10 2000	Fix bugs when writing .imh file headers
 * Mar 21 2000	Change computation of IRAF time tags to use only data structure
 * Mar 22 2000	Move IRAF time tag computation to lt2tsi() in dateutil.c
 * Mar 24 2000	Use Unix file update time if none in header
 * Mar 27 2000	Use hputm() to save file paths up to 256 characters
 * Mar 27 2000	Write filename comments after 1st keyword with short value
 * Mar 27 2000	Allocate pixel file name in same_path to imh2 length
 * Mar 29 2000	Add space after last linefeed of header in fits2iraf()
 * Apr 28 2000	Dimension pixname in irafwimage()
 * May  1 2000	Fix code for updating pixel file name with HDR$ in fits2iraf()
 * Jun  2 2000	Drop unused variables in fits2iraf() after lint
 * Jun 12 2000	If pixel filename has no / or $, use same path as header file
 * Sep  6 2000	Use header directory if pixel file not found at its pathname
 *
 * Jan 11 2001	Print all messages to stderr
 * Aug 24 2001	In isiraf(), return 0 if argument contains an equal sign
 *
 * Apr  8 2002	Fix bug in error message for unidentified nbits in fits2iraf()
 *
 * Feb  4 2003	Open catalog file rb instead of r (Martin Ploner, Bern)
 * Oct 31 2003	Read image only in irafrimage() if physical dimension > image dim.
 * Nov  3 2003	Set NAXISi to image, not physical dimensions in iraf2fits()
 *
 * Jun 13 2005	Drop trailing spaces on pixel file name
 *
 * Jun 20 2006	Initialize uninitialized variables
 *
 * Jan  4 2007	Change hputr4() calls to send pointer to value
 * Jan  8 2007	Drop unused variable nbx in irafrimage()
 * Jan  8 2006	Align header and image buffers properly by 4 and by BITPIX
 */
