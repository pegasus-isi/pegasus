/*** File libwcs/fitsfile.c
 *** September 25, 2009
 *** By Doug Mink, dmink@cfa.harvard.edu
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

 * Module:      fitsfile.c (FITS file reading and writing)
 * Purpose:     Read and write FITS image and table files
 * fitsropen (inpath)
 *		Open a FITS file for reading, returning a FILE pointer
 * fitsrhead (filename, lhead, nbhead)
 *		Read FITS header and return it
 * fitsrtail (filename, lhead, nbhead)
 *		Read appended FITS header and return it
 * fitsrsect (filename, nbhead, header, fd, x0, y0, nx, ny)
 *		Read section of a FITS image, having already read the header
 * fitsrimage (filename, nbhead, header)
 *		Read FITS image, having already ready the header
 * fitsrfull (filename, nbhead, header)
 *		Read a FITS image of any dimension
 * fitsrtopen (inpath, nk, kw, nrows, nchar, nbhead)
 *		Open a FITS table file for reading; return header information
 * fitsrthead (header, nk, kw, nrows, nchar, nbhead)
 *		Extract FITS table information from a FITS header
 * fitsrtline (fd, nbhead, lbuff, tbuff, irow, nbline, line)
 *		Read next line of FITS table file
 * ftgetr8 (entry, kw)
 *		Extract column from FITS table line as double
 * ftgetr4 (entry, kw)
 *		Extract column from FITS table line as float
 * ftgeti4 (entry, kw)
 *		Extract column from FITS table line as int
 * ftgeti2 (entry, kw)
 *		Extract column from FITS table line as short
 * ftgetc (entry, kw, string, maxchar)
 *		Extract column from FITS table line as a character string
 * fitswimage (filename, header, image)
 *		Write FITS header and image
 * fitswext (filename, header, image)
 *		Write FITS header and image as extension to existing FITS file
 * fitswhdu (fd, filename, header, image)
 *		Write FITS header and image as extension to file descriptor
 * fitscimage (filename, header, filename0)
 *		Write FITS header and copy FITS image
 * fitswhead (filename, header)
 *		Write FITS header and keep file open for further writing 
 * fitswexhead (filename, header)
 *		Write FITS header only to FITS extension without writing data
 * isfits (filename)
 *		Return 1 if file is a FITS file, else 0
 * fitsheadsize (header)
 *  		Return size of FITS header in bytes
 */

#include <stdlib.h>
#ifndef VMS
#include <unistd.h>
#endif
#include <stdio.h>
#include <fcntl.h>
#include <sys/file.h>
#include <errno.h>
#include <string.h>
#include "fitsfile.h"

static int verbose=0;		/* Print diagnostics */
static char fitserrmsg[80];
static int fitsinherit = 1;		/* Append primary header to extension header */
void
setfitsinherit (inh)
int inh;
{fitsinherit = inh; return;}

static int ibhead = 0;		/* Number of bytes read before header starts */

int
getfitsskip()
{return (ibhead);}

/* FITSRHEAD -- Read a FITS header */

char *
fitsrhead (filename, lhead, nbhead)

char	*filename;	/* Name of FITS image file */
int	*lhead;		/* Allocated length of FITS header in bytes (returned) */
int	*nbhead;	/* Number of bytes before start of data (returned) */
			/* This includes all skipped image extensions */

{
    int fd;
    char *header;	/* FITS image header (filled) */
    int extend;
    int nbytes,naxis, i;
    int ntry,nbr,irec,nrec, nbh, ipos, npos, nbprim, lprim, lext;
    int nax1, nax2, nax3, nax4, nbpix, ibpix, nblock, nbskip;
    char fitsbuf[2884];
    char *headend;	/* Pointer to last line of header */
    char *headnext;	/* Pointer to next line of header to be added */
    int hdu;		/* header/data unit counter */
    int extnum;		/* desired header data number
			   (0=primary -1=first with data -2=use EXTNAME) */
    char extname[32];	/* FITS extension name */
    char extnam[32];	/* Desired FITS extension name */
    char *ext;		/* FITS extension name or number in header, if any */
    char *pheader;	/* Primary header (naxis is 0) */
    char cext = 0;
    char *rbrac;	/* Pointer to right bracket if present in file name */
    char *mwcs;		/* Pointer to WCS name separated by % */
    char *newhead;	/* New larger header */
    int nbh0;		/* Length of old too small header */
    char *pheadend;
    int inherit = 1;	/* Value of INHERIT keyword in FITS extension header */
    int extfound = 0;	/* Set to one if desired FITS extension is found */
    int npcount;

    pheader = NULL;
    lprim = 0;
    header = NULL;

    /* Check for FITS WCS specification and ignore for file opening */
    mwcs = strchr (filename, '%');
    if (mwcs != NULL)
	*mwcs = (char) 0;

    /* Check for FITS extension and ignore for file opening */
    rbrac = NULL;
    ext = strchr (filename, ',');
    if (ext == NULL) {
	ext = strchr (filename, '[');
	if (ext != NULL) {
	    rbrac = strchr (filename, ']');
	    if (rbrac != NULL)
		*rbrac = (char) 0;
	    }
	}
    if (ext != NULL) {
	cext = *ext;
	*ext = (char) 0;
	}

    /* Open the image file and read the header */
    if (strncasecmp (filename,"stdin",5)) {
	fd = -1;
	fd = fitsropen (filename);
	}
#ifndef VMS
    else {
	fd = STDIN_FILENO;
	extnum = -1;
	}
#endif

    if (ext != NULL) {
	if (isnum (ext+1))
	    extnum = atoi (ext+1);
	else {
	    extnum = -2;
	    strcpy (extnam, ext+1);
	    }
	}
    else
	extnum = -1;

    /* Repair the damage done to the file-name string during parsing */
    if (ext != NULL)
	*ext = cext;
    if (rbrac != NULL)
	*rbrac = ']';
    if (mwcs != NULL)
	*mwcs = '%';

    if (fd < 0) {
	fprintf (stderr,"FITSRHEAD:  cannot read file %s\n", filename);
	return (NULL);
	}

    nbytes = FITSBLOCK;
    *nbhead = 0;
    headend = NULL;
    nbh = FITSBLOCK * 20 + 4;
    header = (char *) calloc ((unsigned int) nbh, 1);
    (void) hlength (header, nbh);
    headnext = header;
    nrec = 1;
    hdu = 0;
    ibhead = 0;

    /* Read FITS header from input file one FITS block at a time */
    irec = 0;
    ibhead = 0;
    while (irec < 100) {
	nbytes = FITSBLOCK;
	for (ntry = 0; ntry < 10; ntry++) {
	    for (i = 0; i < 2884; i++) fitsbuf[i] = 0;
	    nbr = read (fd, fitsbuf, nbytes);

	    /* Short records allowed only if they have the last header line */
	    if (nbr < nbytes) {
		headend = ksearch (fitsbuf,"END");
		if (headend == NULL) {
		    if (ntry < 9) {
			if (verbose)
			    fprintf (stderr,"FITSRHEAD: %d / %d bytes read %d\n",
				     nbr,nbytes,ntry);
			}
		    else {
			snprintf(fitserrmsg,79,"FITSRHEAD: '%d / %d bytes of header read from %s\n"
				,nbr,nbytes,filename);
#ifndef VMS
			if (fd != STDIN_FILENO)
#endif
			    (void)close (fd);
			free (header);
			/* if (pheader != NULL)
			    return (pheader); */
    			if (extnum != -1 && !extfound) {
			    *ext = (char) 0;
			    if (extnum < 0) {
	    			snprintf (fitserrmsg,79,
				    "FITSRHEAD: Extension %s not found in file %s",
				    extnam, filename);
				}
			    else {
	    			snprintf (fitserrmsg,79,
				    "FITSRHEAD: Extension %d not found in file %s",
				    extnum, filename);
				}
			    *ext = cext;
			    }
			else if (hdu > 0) {
	    		    snprintf (fitserrmsg,79,
				"FITSRHEAD: No extensions found in file %s", filename);
			    hdu = 0;
			    if (pheader != NULL) {
				*lhead = nbprim;
				*nbhead = nbprim;
				return (pheader);
				}
			    break;
			    }
			else {
	    		    snprintf (fitserrmsg,79,
				"FITSRHEAD: No header found in file %s", filename);
			    }
			return (NULL);
			}
		    }
		else
		    break;
		}
	    else
		break;
	    }

	/* Move current FITS record into header string */
	for (i = 0; i < 2880; i++)
	    if (fitsbuf[i] < 32) fitsbuf[i] = 32;
	strncpy (headnext, fitsbuf, nbr);
	*nbhead = *nbhead + nbr;
	nrec = nrec + 1;
	*(headnext+nbr) = 0;
	ibhead = ibhead + 2880;

	/* Check to see if this is the final record in this header */
	headend = ksearch (fitsbuf,"END");
	if (headend == NULL) {

	    /* Increase size of header buffer by 4 blocks if too small */
	    if (nrec * FITSBLOCK > nbh) {
		nbh0 = nbh;
		nbh = (nrec + 4) * FITSBLOCK + 4;
		newhead = (char *) calloc (1,(unsigned int) nbh);
		for (i = 0; i < nbh0; i++)
		    newhead[i] = header[i];
		free (header);
		header = newhead;
		(void) hlength (header, nbh);
		headnext = header + *nbhead - FITSBLOCK;
		}
	    headnext = headnext + FITSBLOCK;
	    }

	else {
	    naxis = 0;
	    hgeti4 (header,"NAXIS",&naxis);

	    /* If header has no data, save it for appending to desired header */
	    if (naxis < 1) {
		nbprim = nrec * FITSBLOCK;
		headend = ksearch (header,"END");
		lprim = headend + 80 - header;
		pheader = (char *) calloc ((unsigned int) nbprim, 1);
		for (i = 0; i < lprim; i++)
		    pheader[i] = header[i];
		strncpy (pheader, header, lprim);
		}

	    /* If header has no data, start with the next record */
	    if (naxis < 1 && extnum == -1) {
		extend = 0;
		hgetl (header,"EXTEND",&extend);
		if (naxis == 0 && extend) {
		    headnext = header;
		    *headend = ' ';
		    headend = NULL;
		    /* nrec = 1; */
		    hdu = hdu + 1;
		    }
		else
		    break;
		}

	    /* If this is the desired header data unit, keep it */
	    else if (extnum != -1) {
		if (extnum > -1 && hdu == extnum) {
		    extfound = 1;
		    break;
		    }
		else if (extnum < 0) {
		    extname[0] = 0;
		    hgets (header, "EXTNAME", 32, extname);
		    if (!strcmp (extnam,extname)) {
			extfound = 1;
			break;
			}
		    }

		/* If this is not desired header data unit, skip over data */
		hdu = hdu + 1;
		nblock = 0;
		ibhead = 0;
		if (naxis > 0) {
		    ibpix = 0;
		    hgeti4 (header,"BITPIX",&ibpix);
		    if (ibpix < 0)
			nbpix = -ibpix / 8;
		    else
			nbpix = ibpix / 8;
		    nax1 = 1;
		    hgeti4 (header,"NAXIS1",&nax1);
		    nax2 = 1;
		    if (naxis > 1)
			hgeti4 (header,"NAXIS2",&nax2);
		    nax3 = 1;
		    if (naxis > 2)
			hgeti4 (header,"NAXIS3",&nax3);
		    nax4 = 1;
		    if (naxis > 3)
			hgeti4 (header,"NAXIS4",&nax4);
		    nbskip = nax1 * nax2 * nax3 * nax4 * nbpix;
		    nblock = nbskip / 2880;
		    if (nblock*2880 < nbskip)
			nblock = nblock + 1;
		    npcount = 0;
		    hgeti4 (header,"PCOUNT", &npcount);
		    if (npcount > 0) {
			nbskip = nbskip + npcount;
			nblock = nbskip / 2880;
			if (nblock*2880 < nbskip)
			    nblock = nblock + 1;
			}
		    }
		else
		    nblock = 0;
		*nbhead = *nbhead + (nblock * 2880);

		/* Set file pointer to beginning of next header/data unit */
		if (nblock > 0) {
#ifndef VMS
		    if (fd != STDIN_FILENO) {
			ipos = lseek (fd, *nbhead, SEEK_SET);
			npos = *nbhead;
			}
		    else {
#else
			{
#endif
			ipos = 0;
			for (i = 0; i < nblock; i++) {
			    nbytes = FITSBLOCK;
			    nbr = read (fd, fitsbuf, nbytes);
			    if (nbr < nbytes) {
				ipos = ipos + nbr;
				break;
				}
			    else
				ipos = ipos + nbytes;
			    }
			npos = nblock * 2880;
			}
		    if (ipos < npos) {
			snprintf (fitserrmsg,79,"FITSRHEAD: %d / %d bytes skipped\n",
				 ipos,npos);
			extfound = 0;
			break;
			}
		    }
		headnext = header;
		headend = NULL;
		nrec = 1;
		}
	    else
		break;
	    }
	}

#ifndef VMS
    if (fd != STDIN_FILENO)
	(void)close (fd);
#endif

/* Print error message and return null if extension not found */
    if (extnum != -1 && !extfound) {
	if (extnum < 0)
	    fprintf (stderr, "FITSRHEAD: Extension %s not found in file %s\n",extnam, filename);
	else
	    fprintf (stderr, "FITSRHEAD: Extension %d not found in file %s\n",extnum, filename);
	if (pheader != NULL) {
	    free (pheader);
	    pheader = NULL;
	    }
	return (NULL);
	}

    /* Allocate an extra block for good measure */
    *lhead = (nrec + 1) * FITSBLOCK;
    if (*lhead > nbh) {
	newhead = (char *) calloc (1,(unsigned int) *lhead);
	for (i = 0; i < nbh; i++)
	    newhead[i] = header[i];
	free (header);
	header = newhead;
	(void) hlength (header, *lhead);
	}
    else
	*lhead = nbh;

    /* If INHERIT keyword is FALSE, never append primary header */
    if (hgetl (header, "INHERIT", &inherit)) {
	if (!inherit && fitsinherit)
	    fitsinherit = 0;
	}

    /* Append primary data header to extension header */
    if (pheader != NULL && extnum != 0 && fitsinherit && hdu > 0) {
	extname[0] = 0;
	hgets (header, "XTENSION", 32, extname);
	if (!strcmp (extname,"IMAGE")) {
	    strncpy (header, "SIMPLE  ", 8);
	    hputl (header, "SIMPLE", 1);
	    }
	hputs (header,"COMMENT","-------------------------------------------");
	hputs (header,"COMMENT","Information from Primary Header");
	hputs (header,"COMMENT","-------------------------------------------");
	headend = blsearch (header,"END");
	if (headend == NULL)
	    headend = ksearch (header, "END");
	lext = headend - header;

	/* Update primary header for inclusion at end of extension header */
	hchange (pheader, "SIMPLE", "ROOTHEAD");
	hchange (pheader, "NEXTEND", "NUMEXT");
	hdel (pheader, "BITPIX");
	hdel (pheader, "NAXIS");
	hdel (pheader, "EXTEND");
	hputl (pheader, "ROOTEND",1);
	pheadend = ksearch (pheader,"END");
	lprim = pheadend + 80 - pheader;
	if (lext + lprim > nbh) {
	    nrec = (lext + lprim) / FITSBLOCK;
	    if (FITSBLOCK*nrec < lext+lprim)
		nrec = nrec + 1;
	    *lhead = (nrec+1) * FITSBLOCK;
	    newhead = (char *) calloc (1,(unsigned int) *lhead);
	    for (i = 0; i < nbh; i++)
		newhead[i] = header[i];
	    free (header);
	    header = newhead;
	    headend = header + lext;
	    (void) hlength (header, *lhead);
	    }
	pheader[lprim] = 0;
	strncpy (headend, pheader, lprim);
	if (pheader != NULL) {
	    free (pheader);
	    pheader = NULL;
	    }
	}

    ibhead = *nbhead - ibhead;

    return (header);
}


/* FITSRTAIL -- Read FITS header appended to graphics file */

char *
fitsrtail (filename, lhead, nbhead)

char	*filename;	/* Name of image file */
int	*lhead;		/* Allocated length of FITS header in bytes (returned) */
int	*nbhead;	/* Number of bytes before start of data (returned) */
			/* This includes all skipped image extensions */

{
    int fd;
    char *header;	/* FITS image header (filled) */
    int nbytes, i, ndiff;
    int nbr, irec, offset;
    char *mwcs;		/* Pointer to WCS name separated by % */
    char *headstart;
    char *newhead;

    header = NULL;

    /* Check for FITS WCS specification and ignore for file opening */
    mwcs = strchr (filename, '%');
    if (mwcs != NULL)
	*mwcs = (char) 0;

    /* Open the image file and read the header */
    if (strncasecmp (filename,"stdin",5)) {
	fd = -1;
	fd = fitsropen (filename);
	}
#ifndef VMS
    else {
	fd = STDIN_FILENO;
	}
#endif

    /* Repair the damage done to the file-name string during parsing */
    if (mwcs != NULL)
	*mwcs = '%';

    if (fd < 0) {
	fprintf (stderr,"FITSRTAIL:  cannot read file %s\n", filename);
	return (NULL);
	}

    nbytes = FITSBLOCK;
    *nbhead = 0;
    *lhead = 0;

    /* Read FITS header from end of input file one FITS block at a time */
    irec = 0;
    while (irec < 100) {
	nbytes = FITSBLOCK * (irec + 2);
	header = (char *) calloc ((unsigned int) nbytes, 1);
	offset = lseek (fd, -nbytes, SEEK_END);
	if (offset < 0) {
	    free (header);
	    header = NULL;
	    nbytes = 0;
	    break;
	    }
	for (i = 0; i < nbytes; i++) header[i] = 0;
	nbr = read (fd, header, nbytes);

	/* Check for SIMPLE at start of header */
	for (i = 0; i < nbr; i++)
	    if (header[i] < 32) header[i] = 32;
	if ((headstart = ksearch (header,"SIMPLE"))) {
	    if (headstart != header) {
		ndiff = headstart - header;
		newhead = (char *) calloc ((unsigned int) nbytes, 1);
		for (i = 0; i < nbytes-ndiff; i++)
		    newhead[i] = headstart[i];
		free (header);
		header = newhead;
		}
	    *lhead = nbytes;
	    *nbhead = nbytes;
	    break;
	    }
	free (header);
	}
    (void) hlength (header, nbytes);

#ifndef VMS
    if (fd != STDIN_FILENO)
	(void)close (fd);
#endif

    return (header);
}


/* FITSRSECT -- Read a piece of a FITS image, having already read the header */

char *
fitsrsect (filename, header, nbhead, x0, y0, nx, ny, nlog)

char	*filename;	/* Name of FITS image file */
char	*header;	/* FITS header for image (previously read) */
int	nbhead;		/* Actual length of image header(s) in bytes */
int	x0, y0;		/* FITS image coordinate of first pixel */
int	nx;		/* Number of columns to read (less than NAXIS1) */
int	ny;		/* Number of rows to read (less than NAXIS2) */
int	nlog;		/* Note progress mod this rows */
{
    int fd;		/* File descriptor */
    int nbimage, naxis1, naxis2, bytepix, nbread;
    int bitpix, naxis, nblocks, nbytes, nbr;
    int x1, y1, nbline, impos, nblin, nyleft;
    char *image, *imline, *imlast;
    int ilog = 0;
    int row;

    /* Open the image file and read the header */
    if (strncasecmp (filename,"stdin", 5)) {
	fd = -1;

	fd = fitsropen (filename);
	if (fd < 0) {
	    snprintf (fitserrmsg,79, "FITSRSECT:  cannot read file %s\n", filename);
	    return (NULL);
	    }

	/* Skip over FITS header and whatever else needs to be skipped */
	if (lseek (fd, nbhead, SEEK_SET) < 0) {
	    (void)close (fd);
	    snprintf (fitserrmsg,79, "FITSRSECT:  cannot skip header of file %s\n",
		     filename);
	    return (NULL);
	    }
	}
#ifndef VMS
    else
	fd = STDIN_FILENO;
#endif

    /* Compute size of image in bytes using relevant header parameters */
    naxis = 1;
    hgeti4 (header,"NAXIS",&naxis);
    naxis1 = 1;
    hgeti4 (header,"NAXIS1",&naxis1);
    naxis2 = 1;
    hgeti4 (header,"NAXIS2",&naxis2);
    bitpix = 0;
    hgeti4 (header,"BITPIX",&bitpix);
    if (bitpix == 0) {
	/* snprintf (fitserrmsg,79, "FITSRSECT:  BITPIX is 0; image not read\n"); */
	(void)close (fd);
	return (NULL);
	}
    bytepix = bitpix / 8;
    if (bytepix < 0) bytepix = -bytepix;

    /* Keep X coordinates within image limits */
    if (x0 < 1)
	x0 = 1;
    else if (x0 > naxis1)
	x0 = naxis1;
    x1 = x0 + nx - 1;
    if (x1 < 1)
	x1 = 1;
    else if (x1 > naxis1)
	x1 = naxis1;
    nx = x1 - x0 + 1;

    /* Keep Y coordinates within image limits */
    if (y0 < 1)
	y0 = 1;
    else if (y0 > naxis2)
	y0 = naxis2;
    y1 = y0 + ny - 1;
    if (y1 < 1)
	y1 = 1;
    else if (y1 > naxis2)
	y1 = naxis2;
    ny = y1 - y0 + 1;

    /* Number of bytes in output image */
    nbline = nx * bytepix;
    nbimage = nbline * ny;

    /* Set number of bytes to integral number of 2880-byte blocks */
    nblocks = nbimage / FITSBLOCK;
    if (nblocks * FITSBLOCK < nbimage)
	nblocks = nblocks + 1;
    nbytes = nblocks * FITSBLOCK;

    /* Allocate image section to be read */
    image = (char *) malloc (nbytes);
    nyleft = ny;
    imline = image;
    nbr = 0;

    /* Computer pointer to first byte of input image to read */
    nblin = naxis1 * bytepix;
    impos = ((y0 - 1) * nblin) + ((x0 - 1) * bytepix);
    row = y0 - 1;

    /* Read image section one line at a time */
    while (nyleft-- > 0) {
	if (lseek (fd, impos, SEEK_CUR) >= 0) {
	    nbread = read (fd, imline, nbline);
	    nbr = nbr + nbread;
	    impos = nblin - nbread;
	    imline = imline + nbline;
	    row++;
	    if (++ilog == nlog) {
		ilog = 0;
		fprintf (stderr, "Row %5d extracted   ", row);
                (void) putc (13,stderr);
		}
	    }
	}
    if (nlog)
	fprintf (stderr, "\n");

    /* Fill rest of image with zeroes */
    imline = image + nbimage;
    imlast = image + nbytes;
    while (imline++ < imlast)
	*imline = (char) 0;

    /* Byte-reverse image, if necessary */
    if (imswapped ())
	imswap (bitpix, image, nbytes);

    return (image);
}


/* FITSRIMAGE -- Read a FITS image */

char *
fitsrimage (filename, nbhead, header)

char	*filename;	/* Name of FITS image file */
int	nbhead;		/* Actual length of image header(s) in bytes */
char	*header;	/* FITS header for image (previously read) */
{
    int fd;
    int nbimage, naxis1, naxis2, bytepix, nbread;
    int bitpix, naxis, nblocks, nbytes, nbleft, nbr;
    int simple;
    char *image, *imleft;

    /* Open the image file and read the header */
    if (strncasecmp (filename,"stdin", 5)) {
	fd = -1;

	fd = fitsropen (filename);
	if (fd < 0) {
	    snprintf (fitserrmsg,79, "FITSRIMAGE:  cannot read file %s\n", filename);
	    return (NULL);
	    }

	/* Skip over FITS header and whatever else needs to be skipped */
	if (lseek (fd, nbhead, SEEK_SET) < 0) {
	    (void)close (fd);
	    snprintf (fitserrmsg,79, "FITSRIMAGE:  cannot skip header of file %s\n",
		     filename);
	    return (NULL);
	    }
	}
#ifndef VMS
    else
	fd = STDIN_FILENO;
#endif

    /* If SIMPLE=F in header, simply put post-header part of file in buffer */
    hgetl (header, "SIMPLE", &simple);
    if (!simple) {
	nbytes = getfilesize (filename) - nbhead;
	if ((image = (char *) malloc (nbytes + 1)) == NULL) {
	    /* snprintf (fitserrmsg,79, "FITSRIMAGE:  %d-byte image buffer cannot be allocated\n"); */
	    (void)close (fd);
	    return (NULL);
	    }
	hputi4 (header, "NBDATA", nbytes);
	nbread = read (fd, image, nbytes);
	return (image);
	}

    /* Compute size of image in bytes using relevant header parameters */
    naxis = 1;
    hgeti4 (header,"NAXIS",&naxis);
    naxis1 = 1;
    hgeti4 (header,"NAXIS1",&naxis1);
    naxis2 = 1;
    hgeti4 (header,"NAXIS2",&naxis2);
    bitpix = 0;
    hgeti4 (header,"BITPIX",&bitpix);
    if (bitpix == 0) {
	/* snprintf (fitserrmsg,79, "FITSRIMAGE:  BITPIX is 0; image not read\n"); */
	(void)close (fd);
	return (NULL);
	}
    bytepix = bitpix / 8;
    if (bytepix < 0) bytepix = -bytepix;

    /* If either dimension is one and image is 3-D, read all three dimensions */
    if (naxis == 3 && (naxis1 ==1 || naxis2 == 1)) {
	int naxis3;
	hgeti4 (header,"NAXIS3",&naxis3);
	nbimage = naxis1 * naxis2 * naxis3 * bytepix;
	}
    else
	nbimage = naxis1 * naxis2 * bytepix;

    /* Set number of bytes to integral number of 2880-byte blocks */
    nblocks = nbimage / FITSBLOCK;
    if (nblocks * FITSBLOCK < nbimage)
	nblocks = nblocks + 1;
    nbytes = nblocks * FITSBLOCK;

    /* Allocate and read image */
    image = (char *) malloc (nbytes);
    nbleft = nbytes;
    imleft = image;
    nbr = 0;
    while (nbleft > 0) {
	nbread = read (fd, imleft, nbleft);
	nbr = nbr + nbread;
#ifndef VMS
	if (fd == STDIN_FILENO && nbread < nbleft && nbread > 0) {
	    nbleft = nbleft - nbread;
	    imleft = imleft + nbread;
	    }
	else
#endif
	    nbleft = 0;
	}
#ifndef VMS
    if (fd != STDIN_FILENO)
	(void)close (fd);
#endif
    if (nbr < nbimage) {
	snprintf (fitserrmsg,79, "FITSRIMAGE:  %d of %d bytes read from file %s\n",
		 nbr, nbimage, filename);
	return (NULL);
	}

    /* Byte-reverse image, if necessary */
    if (imswapped ())
	imswap (bitpix, image, nbytes);

    return (image);
}


/* FITSRFULL -- Read a FITS image of any dimension */

char *
fitsrfull (filename, nbhead, header)

char	*filename;	/* Name of FITS image file */
int	nbhead;		/* Actual length of image header(s) in bytes */
char	*header;	/* FITS header for image (previously read) */
{
    int fd;
    int nbimage, naxisi, iaxis, bytepix, nbread;
    int bitpix, naxis, nblocks, nbytes, nbleft, nbr, simple;
    char keyword[16];
    char *image, *imleft;

    /* Open the image file and read the header */
    if (strncasecmp (filename,"stdin", 5)) {
	fd = -1;

	fd = fitsropen (filename);
	if (fd < 0) {
	    snprintf (fitserrmsg,79, "FITSRFULL:  cannot read file %s\n", filename);
	    return (NULL);
	    }

	/* Skip over FITS header and whatever else needs to be skipped */
	if (lseek (fd, nbhead, SEEK_SET) < 0) {
	    (void)close (fd);
	    snprintf (fitserrmsg,79, "FITSRFULL:  cannot skip header of file %s\n",
		     filename);
	    return (NULL);
	    }
	}
#ifndef VMS
    else
	fd = STDIN_FILENO;
#endif

    /* If SIMPLE=F in header, simply put post-header part of file in buffer */
    hgetl (header, "SIMPLE", &simple);
    if (!simple) {
	nbytes = getfilesize (filename) - nbhead;
	if ((image = (char *) malloc (nbytes + 1)) == NULL) {
	    snprintf (fitserrmsg,79, "FITSRFULL:  %d-byte image buffer cannot be allocated\n",nbytes+1);
	    (void)close (fd);
	    return (NULL);
	    }
	hputi4 (header, "NBDATA", nbytes);
	nbread = read (fd, image, nbytes);
	return (image);
	}

    /* Find number of bytes per pixel */
    bitpix = 0;
    hgeti4 (header,"BITPIX",&bitpix);
    if (bitpix == 0) {
	snprintf (fitserrmsg,79, "FITSRFULL:  BITPIX is 0; image not read\n");
	(void)close (fd);
	return (NULL);
	}
    bytepix = bitpix / 8;
    if (bytepix < 0) bytepix = -bytepix;
    nbimage = bytepix;

    /* Compute size of image in bytes using relevant header parameters */
    naxis = 1;
    hgeti4 (header,"NAXIS",&naxis);
    for (iaxis = 1; iaxis <= naxis; iaxis++) {
	sprintf (keyword, "NAXIS%d", iaxis);
	naxisi = 1;
	hgeti4 (header,keyword,&naxisi);
	nbimage = nbimage * naxisi;
	}

    /* Set number of bytes to integral number of 2880-byte blocks */
    nblocks = nbimage / FITSBLOCK;
    if (nblocks * FITSBLOCK < nbimage)
	nblocks = nblocks + 1;
    nbytes = nblocks * FITSBLOCK;

    /* Allocate and read image */
    image = (char *) malloc (nbytes);
    nbleft = nbytes;
    imleft = image;
    nbr = 0;
    while (nbleft > 0) {
	nbread = read (fd, imleft, nbleft);
	nbr = nbr + nbread;
#ifndef VMS
	if (fd == STDIN_FILENO && nbread < nbleft && nbread > 0) {
	    nbleft = nbleft - nbread;
	    imleft = imleft + nbread;
	    }
	else
#endif
	    nbleft = 0;
	}
#ifndef VMS
    if (fd != STDIN_FILENO)
	(void)close (fd);
#endif
    if (nbr < nbimage) {
	snprintf (fitserrmsg,79, "FITSRFULL:  %d of %d image bytes read from file %s\n",
		 nbr, nbimage, filename);
	return (NULL);
	}

    /* Byte-reverse image, if necessary */
    if (imswapped ())
	imswap (bitpix, image, nbytes);

    return (image);
}


/* FITSROPEN -- Open a FITS file, returning the file descriptor */

int
fitsropen (inpath)

char	*inpath;	/* Pathname for FITS tables file to read */

{
    int ntry;
    int fd;		/* file descriptor for FITS tables file (returned) */
    char *ext;		/* extension name or number */
    char cext = 0;
    char *rbrac;
    char *mwcs;		/* Pointer to WCS name separated by % */

/* Check for FITS WCS specification and ignore for file opening */
    mwcs = strchr (inpath, '%');

/* Check for FITS extension and ignore for file opening */
    ext = strchr (inpath, ',');
    rbrac = NULL;
    if (ext == NULL) {
	ext = strchr (inpath, '[');
	if (ext != NULL) {
	    rbrac = strchr (inpath, ']');
	    }
	}

/* Open input file */
    for (ntry = 0; ntry < 3; ntry++) {
	if (ext != NULL) {
	    cext = *ext;
	    *ext = 0;
	    }
	if (rbrac != NULL)
	    *rbrac = (char) 0;
	if (mwcs != NULL)
	    *mwcs = (char) 0;
	fd = open (inpath, O_RDONLY);
	if (ext != NULL)
	    *ext = cext;
	if (rbrac != NULL)
	    *rbrac = ']';
	if (mwcs != NULL)
	    *mwcs = '%';
	if (fd >= 0)
	    break;
	else if (ntry == 2) {
	    snprintf (fitserrmsg,79, "FITSROPEN:  cannot read file %s\n", inpath);
	    return (-1);
	    }
	}

    if (verbose)
	fprintf (stderr,"FITSROPEN:  input file %s opened\n",inpath);

    return (fd);
}


static int offset1=0;
static int offset2=0;

/* FITSRTOPEN -- Open FITS table file and fill structure with
 *		 pointers to selected keywords
 *		 Return file descriptor (-1 if unsuccessful)
 */

int
fitsrtopen (inpath, nk, kw, nrows, nchar, nbhead)

char	*inpath;	/* Pathname for FITS tables file to read */
int	*nk;		/* Number of keywords to use */
struct Keyword	**kw;	/* Structure for desired entries */
int	*nrows;		/* Number of rows in table (returned) */
int	*nchar;		/* Number of characters in one table row (returned) */
int	*nbhead;	/* Number of characters before table starts */

{
    char temp[16];
    int fd;
    int	lhead;		/* Maximum length in bytes of FITS header */
    char *header;	/* Header for FITS tables file to read */

/* Read FITS header from input file */
    header = fitsrhead (inpath, &lhead, nbhead);
    if (!header) {
	snprintf (fitserrmsg,79,"FITSRTOPEN:  %s is not a FITS file\n",inpath);
	return (0);
	}

/* Make sure this file is really a FITS table file */
    temp[0] = 0;
    (void) hgets (header,"XTENSION",16,temp);
    if (strlen (temp) == 0) {
	snprintf (fitserrmsg,79,
		  "FITSRTOPEN:  %s is not a FITS table file\n",inpath);
	free ((void *) header);
	return (0);
	}

/* If it is a FITS file, get table information from the header */
    else if (!strcmp (temp, "TABLE") || !strcmp (temp, "BINTABLE")) {
	if (fitsrthead (header, nk, kw, nrows, nchar)) {
	    snprintf (fitserrmsg,79,
		      "FITSRTOPEN: Cannot read FITS table from %s\n",inpath);
	    free ((void *) header);
	    return (-1);
	    }
	else {
	    fd = fitsropen (inpath);
	    offset1 = 0;
	    offset2 = 0;
	    free ((void *) header);
	    return (fd);
	    }
	}

/* If it is another FITS extension note it and return */
    else {
	snprintf (fitserrmsg,79,
		  "FITSRTOPEN:  %s is a %s extension, not table\n",
		  inpath, temp);
	free ((void *) header);
	return (0);
	}
}

static struct Keyword *pw;	/* Structure for all entries */
static int *lpnam;		/* length of name for each field */
static int bfields = 0;

/* FITSRTHEAD -- From FITS table header, read pointers to selected keywords */

int
fitsrthead (header, nk, kw, nrows, nchar)

char	*header;	/* Header for FITS tables file to read */
int	*nk;		/* Number of keywords to use */
struct Keyword	**kw;	/* Structure for desired entries */
int	*nrows;		/* Number of rows in table (returned) */
int	*nchar;		/* Number of characters in one table row (returned) */

{
    struct Keyword *rw;	/* Structure for desired entries */
    int nfields;
    int ifield, ik, i, ikf, ltform, kl;
    char *h0, *h1, *tf1, *tf2;
    char tname[12];
    char temp[16];
    char tform[16];
    int tverb;
    int bintable = 0;

    h0 = header;

/* Make sure this is really a FITS table file header */
    temp[0] = 0;
    hgets (header,"XTENSION",16,temp);
    if (strlen (temp) == 0) {
	snprintf (fitserrmsg,79, "FITSRTHEAD:  Not a FITS table header\n");
	return (-1);
	}
    else if (!strcmp (temp, "BINTABLE")) {
	bintable = 1;
	}
    else if (strcmp (temp, "TABLE")) {
	snprintf (fitserrmsg,79, "FITSRTHEAD:  %s extension, not TABLE\n",temp);
	return (-1);
	}

/* Get table size from FITS header */
    *nchar = 0;
    hgeti4 (header,"NAXIS1",nchar);
    *nrows = 0;
    hgeti4 (header,"NAXIS2", nrows);
    if (*nrows <= 0 || *nchar <= 0) {
	snprintf (fitserrmsg,79, "FITSRTHEAD: cannot read %d x %d table\n",
		 *nrows,*nchar);
	return (-1);
	}

/* Set up table for access to individual fields */
    nfields = 0;
    hgeti4 (header,"TFIELDS",&nfields);
    if (verbose)
	fprintf (stderr, "FITSRTHEAD: %d fields per table entry\n", nfields);
    if (nfields > bfields) {
	if (bfields > 0)
	    free ((void *)pw);
	pw = (struct Keyword *) calloc (nfields, sizeof(struct Keyword));
	if (pw == NULL) {
	    snprintf (fitserrmsg,79,"FITSRTHEAD: cannot allocate table structure\n");
	    return (-1);
	    }
	if (bfields > 0)
	    free ((void *)lpnam);
	lpnam = (int *) calloc (nfields, sizeof(int));
	if (lpnam == NULL) {
	    snprintf (fitserrmsg,79,"FITSRTHEAD: cannot allocate length structure\n");
	    return (-1);
	    }
	bfields = nfields;
	}

    tverb = verbose;
    verbose = 0;
    ikf = 0;

    for (ifield = 0; ifield < nfields; ifield++) {

    /* Name of field */
	for (i = 0; i < 12; i++) tname[i] = 0;
	sprintf (tname, "TTYPE%d", ifield+1);;
	temp[0] = 0;
	h1 = ksearch (h0,tname);
	h0 = h1;
	hgets (h0,tname,16,temp);
	strcpy (pw[ifield].kname,temp);
	pw[ifield].lname = strlen (pw[ifield].kname);

    /* Sequence of field on line */
	pw[ifield].kn = ifield + 1;

    /* First column of field */
	if (bintable)
	    pw[ifield].kf = ikf;
	else {
	    for (i = 0; i < 12; i++) tname[i] = 0;
	    sprintf (tname, "TBCOL%d", ifield+1);
	    pw[ifield].kf = 0;
	    hgeti4 (h0,tname, &pw[ifield].kf);
	    }

    /* Length of field */
	for (i = 0; i < 12; i++) tname[i] = 0;
	sprintf (tname, "TFORM%d", ifield+1);;
	tform[0] = 0;
	hgets (h0,tname,16,tform);
	strcpy (pw[ifield].kform, tform);
	ltform = strlen (tform);
	if (tform[ltform-1] == 'A') {
	    pw[ifield].kform[0] = 'A';
	    for (i = 0; i < ltform-1; i++)
		pw[ifield].kform[i+1] = tform[i];
	    pw[ifield].kform[ltform] = (char) 0;
	    tf1 = pw[ifield].kform + 1;
	    kl = atof (tf1);
	    }
	else if (!strcmp (tform,"I"))
	    kl = 2;
	else if (!strcmp (tform, "J"))
	    kl = 4;
	else if (!strcmp (tform, "E"))
	    kl = 4;
	else if (!strcmp (tform, "D"))
	    kl = 8;
	else {
	    tf1 = tform + 1;
	    tf2 = strchr (tform,'.');
	    if (tf2 != NULL)
		*tf2 = ' ';
	    kl = atoi (tf1);
	    }
	pw[ifield].kl = kl;
	ikf = ikf + kl;
	}

/* Set up table for access to desired fields */
    verbose = tverb;
    if (verbose)
	fprintf (stderr, "FITSRTHEAD: %d keywords read\n", *nk);

/* If nk = 0, allocate and return structures for all table fields */
    if (*nk <= 0) {
	*kw = pw;
	*nk = nfields;
	return (0);
	}
    else
	rw = *kw;

/* Find each desired keyword in the header */
    for (ik = 0; ik < *nk; ik++) {
	if (rw[ik].kn <= 0) {
	    for (ifield = 0; ifield < nfields; ifield++) {
		if (rw[ik].lname != pw[ifield].lname)
		    continue;
		if (strcmp (pw[ifield].kname, rw[ik].kname) == 0) {
		    break;
		    }
		}
	    }
	else
	    ifield = rw[ik].kn - 1;

/* Set pointer, lentth, and name in returned array of structures */
	rw[ik].kn = ifield + 1;
	rw[ik].kf = pw[ifield].kf - 1;
	rw[ik].kl = pw[ifield].kl;
	strcpy (rw[ik].kform, pw[ifield].kform);
	strcpy (rw[ik].kname, pw[ifield].kname);
	}

    return (0);
}


int
fitsrtline (fd, nbhead, lbuff, tbuff, irow, nbline, line)

int	fd;		/* File descriptor for FITS file */
int	nbhead;		/* Number of bytes in FITS header */
int	lbuff;		/* Number of bytes in table buffer */
char	*tbuff;		/* FITS table buffer */
int	irow;		/* Number of table row to read */
int	nbline;		/* Number of bytes to read for this line */
char	*line;		/* One line of FITS table (returned) */

{
    int nbuff, nlbuff;
    int nbr = 0;
    int offset, offend, ntry, ioff;
    char *tbuff1;

    offset = nbhead + (nbline * irow);
    offend = offset + nbline - 1;

/* Read a new buffer of the FITS table into memory if needed */
    if (offset < offset1 || offend > offset2) {
	nlbuff = lbuff / nbline;
	nbuff = nlbuff * nbline;
	for (ntry = 0; ntry < 3; ntry++) {
	    ioff = lseek (fd, offset, SEEK_SET);
	    if (ioff < offset) {
		if (ntry == 2)
		    return (0);
		else
		    continue;
		}
	    nbr = read (fd, tbuff, nbuff);
	    if (nbr < nbline) {
		if (verbose)
		    fprintf (stderr, "FITSRTLINE: %d / %d bytes read %d\n",
				nbr,nbuff,ntry);
		if (ntry == 2)
		    return (nbr);
		}
	    else
		break;
	    }
	offset1 = offset;
	offset2 = offset + nbr - 1;
	strncpy (line, tbuff, nbline);
	return (nbline);
	}
    else {
	tbuff1 = tbuff + (offset - offset1);
	strncpy (line, tbuff1, nbline);
	return (nbline);
	}
}


void
fitsrtlset ()
{
    offset1 = 0;
    offset2 = 0;
    return;
}


/* FTGETI2 -- Extract n'th column from FITS table line as short */

short
ftgeti2 (entry, kw)

char	*entry;		/* Row or entry from table */
struct Keyword *kw;	/* Table column information from FITS header */
{
    char temp[30];
    short i;
    int j;
    float r;
    double d;

    if (ftgetc (entry, kw, temp, 30)) {
	if (!strcmp (kw->kform, "I"))
	    moveb (temp, (char *) &i, 2, 0, 0);
	else if (!strcmp (kw->kform, "J")) {
	    moveb (temp, (char *) &j, 4, 0, 0);
	    i = (short) j;
	    }
	else if (!strcmp (kw->kform, "E")) {
	    moveb (temp, (char *) &r, 4, 0, 0);
	    i = (short) r;
	    }
	else if (!strcmp (kw->kform, "D")) {
	    moveb (temp, (char *) &d, 8, 0, 0);
	    i = (short) d;
	    }
	else
	    i = (short) atof (temp);
	return (i);
	}
    else
	return ((short) 0);
}


/* FTGETI4 -- Extract n'th column from FITS table line as int */

int
ftgeti4 (entry, kw)

char	*entry;		/* Row or entry from table */
struct Keyword *kw;	/* Table column information from FITS header */
{
    char temp[30];
    short i;
    int j;
    float r;
    double d;

    if (ftgetc (entry, kw, temp, 30)) {
	if (!strcmp (kw->kform, "I")) {
	    moveb (temp, (char *) &i, 2, 0, 0);
	    j = (int) i;
	    }
	else if (!strcmp (kw->kform, "J"))
	    moveb (temp, (char *) &j, 4, 0, 0);
	else if (!strcmp (kw->kform, "E")) {
	    moveb (temp, (char *) &r, 4, 0, 0);
	    j = (int) r;
	    }
	else if (!strcmp (kw->kform, "D")) {
	    moveb (temp, (char *) &d, 8, 0, 0);
	    j = (int) d;
	    }
	else
	    j = (int) atof (temp);
	return (j);
	}
    else
	return (0);
}


/* FTGETR4 -- Extract n'th column from FITS table line as float */

float
ftgetr4 (entry, kw)

char	*entry;		/* Row or entry from table */
struct Keyword *kw;	/* Table column information from FITS header */
{
    char temp[30];
    short i;
    int j;
    float r;
    double d;

    if (ftgetc (entry, kw, temp, 30)) {
	if (!strcmp (kw->kform, "I")) {
	    moveb (temp, (char *) &i, 2, 0, 0);
	    r = (float) i;
	    }
	else if (!strcmp (kw->kform, "J")) {
	    moveb (temp, (char *) &j, 4, 0, 0);
	    r = (float) j;
	    }
	else if (!strcmp (kw->kform, "E"))
	    moveb (temp, (char *) &r, 4, 0, 0);
	else if (!strcmp (kw->kform, "D")) {
	    moveb (temp, (char *) &d, 8, 0, 0);
	    r = (float) d;
	    }
	else
	    r = (float) atof (temp);
	return (r);
	}
    else
	return ((float) 0.0);
}


/* FTGETR8 -- Extract n'th column from FITS table line as double */

double
ftgetr8 (entry, kw)

char	*entry;		/* Row or entry from table */
struct Keyword *kw;	/* Table column information from FITS header */
{
    char temp[30];
    short i;
    int j;
    float r;
    double d;

    if (ftgetc (entry, kw, temp, 30)) {
	if (!strcmp (kw->kform, "I")) {
	    moveb (temp, (char *) &i, 2, 0, 0);
	    d = (double) i;
	    }
	else if (!strcmp (kw->kform, "J")) {
	    moveb (temp, (char *) &j, 4, 0, 0);
	    d = (double) j;
	    }
	else if (!strcmp (kw->kform, "E")) {
	    moveb (temp, (char *) &r, 4, 0, 0);
	    d = (double) r;
	    }
	else if (!strcmp (kw->kform, "D"))
	    moveb (temp, (char *) &d, 8, 0, 0);
	else
	    d = atof (temp);
	return (d);
	}
    else
	return ((double) 0.0);
}


/* FTGETC -- Extract n'th column from FITS table line as character string */

int
ftgetc (entry, kw, string, maxchar)

char	*entry;		/* Row or entry from table */
struct Keyword *kw;	/* Table column information from FITS header */
char	*string;	/* Returned string */
int	maxchar;	/* Maximum number of characters in returned string */
{
    int length = maxchar;

    if (kw->kl < length)
	length = kw->kl;
    if (length > 0) {
	strncpy (string, entry+kw->kf, length);
	string[length] = 0;
	return ( 1 );
	}
    else
	return ( 0 );
}

extern int errno;


/*FITSWIMAGE -- Write FITS header and image */

int
fitswimage (filename, header, image)

char	*filename;	/* Name of FITS image file */
char	*header;	/* FITS image header */
char	*image;		/* FITS image pixels */

{
    int fd;

    /* Open the output file */
    if (strcasecmp (filename,"stdout") ) {

	if (!access (filename, 0)) {
	    fd = open (filename, O_WRONLY);
	    if (fd < 3) {
		snprintf (fitserrmsg,79, "FITSWIMAGE:  file %s not writeable\n", filename);
		return (0);
		}
	    }
	else {
	    fd = open (filename, O_RDWR+O_CREAT, 0666);
	    if (fd < 3) {
		snprintf (fitserrmsg,79, "FITSWIMAGE:  cannot create file %s\n", filename);
		return (0);
		}
	    }
	}
#ifndef VMS
    else
	fd = STDOUT_FILENO;
#endif

    return (fitswhdu (fd, filename, header, image));
}


/*FITSWEXT -- Write FITS header and image as extension to a file */

int
fitswext (filename, header, image)

char	*filename;	/* Name of IFTS image file */
char	*header;	/* FITS image header */
char	*image;		/* FITS image pixels */

{
    int fd;

    /* Open the output file */
    if (strcasecmp (filename,"stdout") ) {

	if (!access (filename, 0)) {
	    fd = open (filename, O_WRONLY);
	    if (fd < 3) {
		snprintf (fitserrmsg,79, "FITSWEXT:  file %s not writeable\n",
			 filename);
		return (0);
		}
	    }
	else {
	    fd = open (filename, O_APPEND, 0666);
	    if (fd < 3) {
		snprintf (fitserrmsg,79, "FITSWEXT:  cannot append to file %s\n",
			 filename);
		return (0);
		}
	    }
	}
#ifndef VMS
    else
	fd = STDOUT_FILENO;
#endif

    return (fitswhdu (fd, filename, header, image));
}


/* FITSWHDU -- Write FITS head and image as extension */

int
fitswhdu (fd, filename, header, image)

int	fd;		/* File descriptor */
char	*filename;	/* Name of IFTS image file */
char	*header;	/* FITS image header */
char	*image;		/* FITS image pixels */
{
    int nbhead, nbimage, nblocks, bytepix, i, nbhw;
    int bitpix, naxis, iaxis, naxisi, nbytes, nbw, nbpad, nbwp, simple;
    char *endhead, *padding;
    double bzero, bscale;
    char keyword[32];

    /* Change BITPIX=-16 files to BITPIX=16 with BZERO and BSCALE */
    bitpix = 0;
    hgeti4 (header,"BITPIX",&bitpix);
    if (bitpix == -16) {
	if (!hgetr8 (header, "BZERO", &bzero) &&
	    !hgetr8 (header, "BSCALE", &bscale)) {
	    bitpix = 16;
	    hputi4 (header, "BITPIX", bitpix);
	    hputr8 (header, "BZERO", 32768.0);
	    hputr8 (header, "BSCALE", 1.0);
	    }
	}

    /* Write header to file */
    endhead = ksearch (header,"END") + 80;
    nbhead = endhead - header;
    nbhw = write (fd, header, nbhead);
    if (nbhw < nbhead) {
	snprintf (fitserrmsg,79, "FITSWHDU:  wrote %d / %d bytes of header to file %s\n",
		 nbhw, nbhead, filename);
	(void)close (fd);
	return (0);
	}

    /* Write extra spaces to make an integral number of 2880-byte blocks */
    nblocks = nbhead / FITSBLOCK;
    if (nblocks * FITSBLOCK < nbhead)
	nblocks = nblocks + 1;
    nbytes = nblocks * FITSBLOCK;
    nbpad = nbytes - nbhead;
    padding = (char *)calloc (1, nbpad);
    for (i = 0; i < nbpad; i++)
	padding[i] = ' ';
    nbwp = write (fd, padding, nbpad);
    if (nbwp < nbpad) {
	snprintf (fitserrmsg,79, "FITSWHDU:  wrote %d / %d bytes of header padding to file %s\n",
		 nbwp, nbpad, filename);
	(void)close (fd);
	return (0);
	}
    nbhw = nbhw + nbwp;
    free (padding);

    /* Return if file has no data */
    if (bitpix == 0 || image == NULL) {
	/* snprintf (fitserrmsg,79, "FITSWHDU:  BITPIX is 0; image not written\n"); */
	(void)close (fd);
	return (0);
	}

    /* If SIMPLE=F in header, just write whatever is in the buffer */
    hgetl (header, "SIMPLE", &simple);
    if (!simple) {
	hgeti4 (header, "NBDATA", &nbytes);
	nbimage = nbytes;
	}

    else {

	/* Compute size of pixel in bytes */
	bytepix = bitpix / 8;
	if (bytepix < 0) bytepix = -bytepix;
	nbimage = bytepix;

	/* Compute size of image in bytes using relevant header parameters */
	naxis = 1;
	hgeti4 (header,"NAXIS",&naxis);
	for (iaxis = 1; iaxis <= naxis; iaxis++) {
	    sprintf (keyword, "NAXIS%d", iaxis);
	    naxisi = 1;
	    hgeti4 (header,keyword,&naxisi);
	    nbimage = nbimage * naxisi;
	    }

	/* Number of bytes to write is an integral number of FITS blocks */
	nblocks = nbimage / FITSBLOCK;
	if (nblocks * FITSBLOCK < nbimage)
	    nblocks = nblocks + 1;
	nbytes = nblocks * FITSBLOCK;

	/* Byte-reverse image before writing, if necessary */
	if (imswapped ())
	    imswap (bitpix, image, nbimage);
	}

    /* Write image to file */
    nbw = write (fd, image, nbimage);
    if (nbw < nbimage) {
	snprintf (fitserrmsg,79, "FITSWHDU:  wrote %d / %d bytes of image to file %s\n",
		 nbw, nbimage, filename);
	return (0);
	}

    /* Write extra zeroes to make an integral number of 2880-byte blocks */
    nbpad = nbytes - nbimage;
    if (nbpad > 0) {
	padding = (char *)calloc (1, nbpad);
	nbwp = write (fd, padding, nbpad);
	if (nbwp < nbpad) {
	    snprintf (fitserrmsg,79, "FITSWHDU:  wrote %d / %d bytes of image padding to file %s\n",
		 nbwp, nbpad, filename);
	    (void)close (fd);
	    return (0);
	    }
	free (padding);
	}
    else
	nbwp = 0;

    (void)close (fd);

    /* Byte-reverse image after writing, if necessary */
    if (imswapped ())
	imswap (bitpix, image, nbimage);

    nbw = nbw + nbwp + nbhw;
    return (nbw);
}


/*FITSCIMAGE -- Write FITS header and copy FITS image
		Return number of bytes in output image, 0 if failure */

int
fitscimage (filename, header, filename0)

char	*filename;	/* Name of output FITS image file */
char	*header;	/* FITS image header */
char	*filename0;	/* Name of input FITS image file */

{
    int fdout, fdin;
    int nbhead, nbimage, nblocks, bytepix;
    int bitpix, naxis, naxis1, naxis2, nbytes, nbw, nbpad, nbwp;
    char *endhead, *lasthead, *padding;
    char *image;	/* FITS image pixels */
    char *oldhead;	/* Input file image header */
    int nbhead0;	/* Length of input file image header */
    int lhead0;
    int nbbuff, nbuff, ibuff, nbr, nbdata;

    /* Compute size of image in bytes using relevant header parameters */
    naxis = 1;
    hgeti4 (header, "NAXIS", &naxis);
    naxis1 = 1;
    hgeti4 (header, "NAXIS1", &naxis1);
    naxis2 = 1;
    hgeti4 (header, "NAXIS2", &naxis2);
    hgeti4 (header, "BITPIX", &bitpix);
    bytepix = bitpix / 8;
    if (bytepix < 0) bytepix = -bytepix;

    /* If either dimension is one and image is 3-D, read all three dimensions */
    if (naxis == 3 && (naxis1 ==1 || naxis2 == 1)) {
	int naxis3;
	hgeti4 (header,"NAXIS3",&naxis3);
	nbimage = naxis1 * naxis2 * naxis3 * bytepix;
	}
    else
	nbimage = naxis1 * naxis2 * bytepix;

    nblocks = nbimage / FITSBLOCK;
    if (nblocks * FITSBLOCK < nbimage)
	nblocks = nblocks + 1;
    nbytes = nblocks * FITSBLOCK;

    /* Allocate image buffer */
    nbbuff = FITSBLOCK * 100;
    if (nbytes < nbbuff)
	nbbuff = nbytes;
    image = (char *) calloc (1, nbbuff);
    nbuff = nbytes / nbbuff;
    if (nbytes > nbuff * nbbuff)
	nbuff = nbuff + 1;

    /* Read input file header */
    if ((oldhead = fitsrhead (filename0, &lhead0, &nbhead0)) == NULL) {
	snprintf (fitserrmsg, 79,"FITSCIMAGE: header of input file %s cannot be read\n",
		 filename0);
	return (0);
	}

    /* Find size of output header */
    nbhead = fitsheadsize (header);

    /* If overwriting, be more careful if new header is longer than old */
    if (!strcmp (filename, filename0) && nbhead > nbhead0) {
	if ((image = fitsrimage (filename0, nbhead0, oldhead)) == NULL) {
	    snprintf (fitserrmsg,79, "FITSCIMAGE:  cannot read image from file %s\n",
		     filename0);
	    free (oldhead);
	    return (0);
	    }
	return (fitswimage (filename, header, image));
	}
    free (oldhead);

    /* Open the input file and skip over the header */
    if (strcasecmp (filename0,"stdin")) {
	fdin = -1;
	fdin = fitsropen (filename0);
	if (fdin < 0) {
	    snprintf (fitserrmsg, 79,"FITSCIMAGE:  cannot read file %s\n", filename0);
	    return (0);
	    }

	/* Skip over FITS header */
	if (lseek (fdin, nbhead0, SEEK_SET) < 0) {
	    (void)close (fdin);
	    snprintf (fitserrmsg,79, "FITSCIMAGE:  cannot skip header of file %s\n",
		     filename0);
	    return (0);
	    }
	}
#ifndef VMS
    else
	fdin = STDIN_FILENO;
#endif

    /* Open the output file */
    if (!access (filename, 0)) {
	fdout = open (filename, O_WRONLY);
	if (fdout < 3) {
	    snprintf (fitserrmsg,79, "FITSCIMAGE:  file %s not writeable\n", filename);
	    return (0);
	    }
	}
    else {
	fdout = open (filename, O_RDWR+O_CREAT, 0666);
	if (fdout < 3) {
	    snprintf (fitserrmsg,79, "FITSCHEAD:  cannot create file %s\n", filename);
	    return (0);
	    }
	}

    /* Pad header with spaces */
    endhead = ksearch (header,"END") + 80;
    lasthead = header + nbhead;
    while (endhead < lasthead)
	*(endhead++) = ' ';

    /* Write header to file */
    nbw = write (fdout, header, nbhead);
    if (nbw < nbhead) {
	snprintf (fitserrmsg, 79,"FITSCIMAGE:  wrote %d / %d bytes of header to file %s\n",
		 nbw, nbytes, filename);
	(void)close (fdout);
	(void)close (fdin);
	return (0);
	}

    /* Return if no data */
    if (bitpix == 0) {
	(void)close (fdout);
	(void)close (fdin);
	return (nbhead);
	}

    nbdata = 0;
    for (ibuff = 0; ibuff < nbuff; ibuff++) {
	nbr = read (fdin, image, nbbuff);
	if (nbr > 0) {
	    nbw = write (fdout, image, nbr);
	    nbdata = nbdata + nbw;
	    }
	}

    /* Write extra to make integral number of 2880-byte blocks */
    nblocks = nbdata / FITSBLOCK;
    if (nblocks * FITSBLOCK < nbdata)
	nblocks = nblocks + 1;
    nbytes = nblocks * FITSBLOCK;
    nbpad = nbytes - nbdata;
    padding = (char *)calloc (1,nbpad);
    nbwp = write (fdout, padding, nbpad);
    nbw = nbdata + nbwp;
    free (padding);

    (void)close (fdout);
    (void)close (fdin);

    if (nbw < nbimage) {
	snprintf (fitserrmsg, 79, "FITSWIMAGE:  wrote %d / %d bytes of image to file %s\n",
		 nbw, nbimage, filename);
	return (0);
	}
    else
	return (nbw);
}


/* FITSWHEAD -- Write FITS header and keep file open for further writing */

int
fitswhead (filename, header)

char	*filename;	/* Name of IFTS image file */
char	*header;	/* FITS image header */

{
    int fd;
    int nbhead, nblocks;
    int nbytes, nbw;
    char *endhead, *lasthead;

    /* Open the output file */
    if (!access (filename, 0)) {
	fd = open (filename, O_WRONLY);
	if (fd < 3) {
	    snprintf (fitserrmsg, 79, "FITSWHEAD:  file %s not writeable\n", filename);
	    return (0);
	    }
	}
    else {
	fd = open (filename, O_RDWR+O_CREAT, 0666);
	if (fd < 3) {
	    snprintf (fitserrmsg, 79, "FITSWHEAD:  cannot create file %s\n", filename);
	    return (0);
	    }
	}

    /* Write header to file */
    endhead = ksearch (header,"END") + 80;
    nbhead = endhead - header;
    nblocks = nbhead / FITSBLOCK;
    if (nblocks * FITSBLOCK < nbhead)
	nblocks = nblocks + 1;
    nbytes = nblocks * FITSBLOCK;

    /* Pad header with spaces */
    lasthead = header + nbytes;
    while (endhead < lasthead)
	*(endhead++) = ' ';
    
    nbw = write (fd, header, nbytes);
    if (nbw < nbytes) {
	fprintf (stderr, "FITSWHEAD:  wrote %d / %d bytes of header to file %s\n",
		 nbw, nbytes, filename);
	(void)close (fd);
	return (0);
	}
    return (fd);
}


/* FITSWEXHEAD -- Write FITS header in place */

int
fitswexhead (filename, header)

char	*filename;	/* Name of FITS image file with ,extension */
char	*header;	/* FITS image header */

{
    int fd;
    int nbhead, lhead;
    int nbw, nbnew, nbold;
    char *endhead, *lasthead, *oldheader;
    char *ext, cext;

    /* Compare size of existing header to size of new header */
    fitsinherit = 0;
    oldheader = fitsrhead (filename, &lhead, &nbhead);
    if (oldheader == NULL) {
	snprintf (fitserrmsg, 79, "FITSWEXHEAD:  file %s cannot be read\n", filename);
	return (-1);
	}
    nbold = fitsheadsize (oldheader);
    nbnew = fitsheadsize (header);

    /* Return if the new header is bigger than the old header */
    if (nbnew > nbold) {
	snprintf (fitserrmsg, 79, "FITSWEXHEAD:  old header %d bytes, new header %d bytes\n", nbold,nbnew);
	free (oldheader);
	oldheader = NULL;
	return (-1);
	}

    /* Add blank lines if new header is smaller than the old header */
    else if (nbnew < nbold) {
	strcpy (oldheader, header);
	endhead = ksearch (oldheader,"END");
	lasthead = oldheader + nbold;
	while (endhead < lasthead)
	    *(endhead++) = ' ';
	strncpy (lasthead-80, "END", 3);
	}

    /* Pad header with spaces */
    else {
	endhead = ksearch (header,"END") + 80;
	lasthead = header + nbnew;
	while (endhead < lasthead)
	    *(endhead++) = ' ';
	strncpy (oldheader, header, nbnew);
	}

    /* Check for FITS extension and ignore for file opening */
    ext = strchr (filename, ',');
    if (ext == NULL)
	ext = strchr (filename, '[');
    if (ext != NULL) {
	cext = *ext;
	*ext = (char) 0;
	}

    /* Open the output file */
    fd = open (filename, O_WRONLY);
    if (ext != NULL)
	*ext = cext;
    if (fd < 3) {
	snprintf (fitserrmsg, 79, "FITSWEXHEAD:  file %s not writeable\n", filename);
	return (-1);
	}

    /* Skip to appropriate place in file */
    (void) lseek (fd, ibhead, SEEK_SET);

    /* Write header to file */
    nbw = write (fd, oldheader, nbold);
    (void)close (fd);
    free (oldheader);
    oldheader = NULL;
    if (nbw < nbold) {
	fprintf (stderr, "FITSWHEAD:  wrote %d / %d bytes of header to file %s\n",
		 nbw, nbold, filename);
	return (-1);
	}
    return (0);
}


/* ISFITS -- Return 1 if FITS file, else 0 */
int
isfits (filename)

char    *filename;      /* Name of file for which to find size */
{
    int diskfile;
    char keyword[16];
    char *comma;
    int nbr;

    /* First check to see if this is an assignment */
    if (strchr (filename, '='))
	return (0);

    /* Then check file extension */
    else if (strsrch (filename, ".fit") ||
	strsrch (filename, ".fits") ||
	strsrch (filename, ".fts"))
	return (1);

    /* Check for stdin (input from pipe) */
    else if (!strcasecmp (filename,"stdin"))
	return (1);

    /* If no FITS file extension, try opening the file */
    else {
	if ((comma = strchr (filename,',')))
	    *comma = (char) 0;
	if ((diskfile = open (filename, O_RDONLY)) < 0) {
	    if (comma)
		*comma = ',';
	    return (0);
	    }
	else {
	    nbr = read (diskfile, keyword, 8);
	    if (comma)
		*comma = ',';
	    close (diskfile);
	    if (nbr < 8)
		return (0);
	    else if (!strncmp (keyword, "SIMPLE", 6))
		return (1);
	    else
		return (0);
	    }
	}
}


/* FITSHEADSIZE -- Find size of FITS header */

int
fitsheadsize (header)

char	*header;	/* FITS header */
{
    char *endhead;
    int nbhead, nblocks;

    endhead = ksearch (header,"END") + 80;
    nbhead = endhead - header;
    nblocks = nbhead / FITSBLOCK;
    if (nblocks * FITSBLOCK < nbhead)
        nblocks = nblocks + 1;
    return (nblocks * FITSBLOCK);
}


/* Print error message */
void
fitserr ()
{   fprintf (stderr, "%s\n",fitserrmsg);
    return; }


/* MOVEB -- Copy nbytes bytes from source+offs to dest+offd (any data type) */

void
moveb (source, dest, nbytes, offs, offd)

char *source;	/* Pointer to source */
char *dest;	/* Pointer to destination */
int nbytes;	/* Number of bytes to move */
int offs;	/* Offset in bytes in source from which to start copying */
int offd;	/* Offset in bytes in destination to which to start copying */
{
char *from, *last, *to;
        from = source + offs;
        to = dest + offd;
        last = from + nbytes;
        while (from < last) *(to++) = *(from++);
        return;
}

/*
 * Feb  8 1996	New subroutines
 * Apr 10 1996	Add subroutine list at start of file
 * Apr 17 1996	Print error message to stderr
 * May  2 1996	Write using stream IO
 * May 14 1996	If FITSRTOPEN NK is zero, return all keywords in header
 * May 17 1996	Make header internal to FITSRTOPEN
 * Jun  3 1996	Use stream I/O for input as well as output
 * Jun 10 1996	Remove unused variables after running lint
 * Jun 12 1996	Deal with byte-swapped images
 * Jul 11 1996	Rewrite code to separate header and data reading
 * Aug  6 1996  Fixed small defects after lint
 * Aug  6 1996  Drop unused NBHEAD argument from FITSRTHEAD
 * Aug 13 1996	If filename is stdin, read from standard input instead of file
 * Aug 30 1996	Use write for output, not fwrite
 * Sep  4 1996	Fix mode when file is created
 * Oct 15 1996	Drop column argument from FGET* subroutines
 * Oct 15 1996	Drop unused variable 
 * Dec 17 1996	Add option to skip bytes in file before reading the header
 * Dec 27 1996	Turn nonprinting header characters into spaces
 *
 * Oct  9 1997	Add FITS extension support as filename,extension
 * Dec 15 1997	Fix minor bugs after lint
 *
 * Feb 23 1998	Do not append primary header if getting header for ext. 0
 * Feb 23 1998	Accept either bracketed or comma extension
 * Feb 24 1998	Add SIMPLE keyword to start of extracted extension
 * Apr 30 1998	Fix error return if not table file after Allan Brighton
 * May  4 1998	Fix error in argument sequence in HGETS call
 * May 27 1998	Include fitsio.h and imio.h
 * Jun  1 1998	Add VMS fixes from Harry Payne at STScI
 * Jun  3 1998	Fix bug reading EXTNAME
 * Jun 11 1998	Initialize all header parameters before reading them
 * Jul 13 1998	Clarify argument definitions
 * Aug  6 1998	Rename fitsio.c to fitsfile.c to avoid conflict with CFITSIO
 * Aug 13 1998	Add FITSWHEAD to write only header
 * Sep 25 1998	Allow STDIN or stdin for standard input reading
 * Oct  5 1998	Add isfits() to decide whether a file is FITS
 * Oct  9 1998	Assume stdin and STDIN to be FITS files in isfits()
 * Nov 30 1998	Fix bug found by Andreas Wicenec when reading large headers
 * Dec  8 1998	Fix bug introduced by previous bug fix
 *
 * Jan  4 1999	Do not print error message if BITPIX is 0
 * Jan 27 1999	Read and write all of 3D images if one dimension is 1
 * Jan 27 1999	Pad out data to integral number of 2880-byte blocks
 * Apr 29 1999	Write BITPIX=-16 files as BITPIX=16 with BSCALE and BZERO
 * Apr 30 1999	Add % as alternative to , to denote sub-images
 * May 25 1999	Set buffer offsets to 0 when FITS table file is opened
 * Jul 14 1999	Do not try to write image data if BITPIX is 0
 * Sep 27 1999	Add STDOUT as output filename option in fitswimage()
 * Oct  6 1999	Set header length global variable hget.lhead0 in fitsrhead()
 * Oct 14 1999	Update header length as it is changed in fitsrhead()
 * Oct 20 1999	Change | in if statements to ||
 * Oct 25 1999	Change most malloc() calls to calloc()
 * Nov 24 1999	Add fitscimage()
 *
 * Feb 23 2000	Fix problem with some error returns in fitscimage()
 * Mar 17 2000	Drop unused variables after lint
 * Jul 20 2000	Drop BITPIX and NAXIS from primary header if extension printerd
 * Jul 20 2000	Start primary part of header with ROOTHEAD keyword
 * Jul 28 2000	Add loop to deal with buffered stdin
 *
 * Jan 11 2001	Print all messages to stderr
 * Jan 12 2001	Add extension back onto filename after fitsropen() (Guy Rixon)
 * Jan 18 2001	Drop EXTEND keyword when extracting an extension
 * Jan 18 2001	Add fitswext() to append HDU and fitswhdu() to do actual writing
 * Jan 22 2001	Ignore WCS name or letter following a : in file name in fitsrhead()
 * Jan 30 2001	Fix FITSCIMAGE so it doesn't overwrite data when overwriting a file
 * Feb 20 2001	Ignore WCS name or letter following a : in file name in fitsropen()
 * Feb 23 2001	Initialize rbrac in fitsropen()
 * Mar  8 2001	Use % instead of : for WCS specification in file name
 * Mar  9 2001	Fix bug so primary header is always appended to secondary header
 * Mar  9 2001	Change NEXTEND to NUMEXT in appended primary header
 * Mar 20 2001	Declare fitsheadsize() in fitschead()
 * Apr 24 2001	When matching column names, use longest length
 * Jun 27 2001	In fitsrthead(), allocate pw and lpnam only if more space needed
 * Aug 24 2001	In isfits(), return 0 if argument contains an equal sign
 *
 * Jan 28 2002	In fitsrhead(), allow stdin to include extension and/or WCS selection
 * Jun 18 2002	Save error messages as fitserrmsg and use fitserr() to print them
 * Oct 21 2002	Add fitsrsect() to read a section of an image
 *
 * Feb  4 2003	Open catalog file rb instead of r (Martin Ploner, Bern)
 * Apr  2 2003	Drop unused variable in fitsrsect()
 * Jul 11 2003	Use strcasecmp() to check for stdout and stdin
 * Aug  1 2003	If no other header, return root header from fitsrhead()
 * Aug 20 2003	Add fitsrfull() to read n-dimensional FITS images
 * Aug 21 2003	Modify fitswimage() to always write n-dimensional FITS images
 * Nov 18 2003	Fix minor bug in fitswhdu()
 * Dec  3 2003	Remove unused variable lasthead in fitswhdu()
 *
 * May  3 2004	Do not always append primary header to extension header
 * May  3 2004	Add ibhead as position of header read in file
 * May 19 2004	Do not reset ext if NULL in fitswexhead()
 * Jul  1 2004	Initialize INHERIT to 1
 * Aug 30 2004	Move fitsheadsize() declaration to fitsfile.h
 * Aug 31 2004	If SIMPLE=F, put whatever is in file after header in image
 *
 * Mar 17 2005	Use unbuffered I/O in isfits() for robustness
 * Jun 27 2005	Drop unused variable nblocks in fitswexhead()
 * Aug  8 2005	Fix space-padding bug in fitswexhead() found by Armin Rest
 * Sep 30 2005	Fix fitsrsect() to position relatively, not absolutely
 * Oct 28 2005	Add error message if desired FITS extension is not found
 * Oct 28 2005	Fix initialization problem found by Sergey Koposov
 *
 * Feb 23 2006	Add fitsrtail() to read appended FITS headers
 * Feb 27 2006	Add file name to header-reading error messages
 * May  3 2006	Remove declarations of unused variables
 * Jun 20 2006	Initialize uninitialized variables
 * Nov  2 2006	Change all realloc() calls to calloc()
 *
 * Jan  5 2007	In fitsrtail(), change control characters in header to spaces
 * Apr 30 2007	Improve error reporting in FITSRFULL
 * Nov 28 2007	Add support to BINTABLE in ftget*() and fitsrthead()
 * Dec 20 2007	Add data heap numerated by PCOUNT when skipping HDU in fitsrhead()
 * Dec 20 2007	Return NULL pointer if fitsrhead() cannot find requested HDU
 *
 * Apr  7 2008	Drop comma from name when reading file in isfits()
 * Jun 27 2008	Do not append primary data header if it is the only header
 * Nov 21 2008	In fitswhead(), print message if too few bytes written
 *
 * Sep 18 2009	In fitswexhead() write to error string instead of stderr
 * Sep 22 2009	In fitsrthead(), fix lengths for ASCII numeric table entries
 * Sep 25 2009	Add subroutine moveb() and fix calls to it
 * Sep 25 2009	Fix several small errors found by Douglas Burke
 */
