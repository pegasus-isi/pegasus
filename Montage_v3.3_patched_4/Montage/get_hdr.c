/* Module: get_hdr.c

Version  Developer        Date     Change
-------  ---------------  -------  -----------------------
1.7      John Good        14Jan04  Added "bad image" output option.
1.6.1    Anastasia Laity  03Sep03  Fixed read_fits_keyword problem
				   with status value by resetting
				   status to 0 after each attempt
				   to read keywords
1.6      John Good        21Aug03  Flipped and rotated cdelts under
				   certain conditions (see code).
1.5.1    A. C. Laity      30Jun03  Added explanatory comments at top
1.5      John Good        22Mar03  Fixed processing of "bad headers"
1.4      John Good        22Mar03  Renamed wcsCheck to checkWCS for
				   consistency.
1.3      John Good        18Mar03  Added processing for "bad FITS"
				   count.
1.2      John Good        14Mar03  Minor modification associated
				   with removing leading "./" 
				   from file names
1.1      John Good        13Mar03  Added WCS header check
1.0      John Good        29Jan03  Baseline code

*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/param.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <math.h>
#include <coord.h>
#include <fitsio.h>
#include <wcs.h>

#include "hdr_rec.h"

extern int   debug;
extern int   showbad;
extern int   cntr;
extern FILE *tblf;

typedef struct
{
   char name  [128];
   char type  [128];
   char value [128];
   char defval[128];
   int  width;
}
FIELDS;

extern FIELDS *fields;
extern int     nfields;

extern int     badwcs;

int  checkWCS (struct WorldCoor *wcs, int action);
void print_rec(struct Hdr_rec*);



/* get_hdr reads the FITS headers from a file and parses */
/* the values into a structure more easily handled by    */
/* Montage modules (Hdr_rec)                             */

int get_hdr (char *fname, struct Hdr_rec *hdr_rec, char *msg)
{
   char     *header;
   char      value[1024], comment[1024], *ptr;
   fitsfile *fptr;
   int       i, status, csys, nfailed, first_failed, clockwise;
   double    lon, lat, equinox;
   double    ra2000, dec2000;
   double    ra, dec;
   double    x1, y1, z1;
   double    x2, y2, z2;

   double    dtr = 1.745329252e-2;

   struct WorldCoor *wcs;

   struct stat buf;

   nfailed      = 0;
   first_failed = 0;
   status       = 0;

   if(fits_open_file(&fptr, fname, READONLY, &status)) 
   {
      sprintf (msg, "Cannot open FITS file %s", fname);

      if(showbad)
      {
	 printf("[struct stat=\"INFO\", msg=\"Cannot open file\", file=\"%s\"]\n",
	    fname);
	 
	 fflush(stdout);
      }
      return (1);
   }

   stat(fname, &buf);

   hdr_rec->size = buf.st_size;

   hdr_rec->hdu = 1;

   while(1)
   {
      status = 0;
      if(fits_movabs_hdu(fptr, hdr_rec->hdu, NULL, &status))
	 break;


      /* Now try to get the values of the */
      /* extra required keywords. We look */
      /* in HDU 1 in case there are some  */
      /* that are there and meant to be   */
      /* global (i.e. not in the others)  */

      if(hdr_rec->hdu == 1)
      {
	 for(i=0; i<nfields; ++i)
	 {
	    status = 0;
	    if(fits_read_keyword(fptr, fields[i].name, value, comment, &status))
	       strcpy(fields[i].defval, "");

	    else
            {
	       ptr = value;

	       if(*ptr == '\'' && value[strlen(value)-1] == '\'')
	       {
		  value[strlen(value)-1] = '\0';
		  ++ptr;
	       }

	       strcpy(fields[i].defval, ptr);
            }
	 }
      }

      if(hdr_rec->hdu == 2 && first_failed)
	 --nfailed;

      status = 0;
      if(fits_get_image_wcs_keys(fptr, &header, &status)) 
      {
	 ++hdr_rec->hdu;
         continue;
      }

      wcs = wcsinit(header);

      if(wcs == (struct WorldCoor *)NULL) 
      {
	 if(hdr_rec->hdu == 1)
	    first_failed = 1;

	 ++nfailed;
	 ++badwcs;

	 if(showbad)
	 {
	    printf("[struct stat=\"INFO\", msg=\"WCS lib init failure\", file=\"%s\", hdu=%d]\n",
	       fname, hdr_rec->hdu);
	    
	    fflush(stdout);
	 }

	 ++hdr_rec->hdu;
         continue;
      } 

      if(checkWCS(wcs, 1) == 1)
      {
	 if(debug)
	 {
	    printf("Bad WCS for file %s\n", fname);
	    fflush(stdout);
	 }

	 wcs = (struct WorldCoor *)NULL;

	 if(hdr_rec->hdu == 1)
	    first_failed = 1;

	 ++nfailed;
	 ++badwcs;

	 if(showbad)
	 {
	    printf("[struct stat=\"INFO\", msg=\"Bad WCS\", file=\"%s\", hdu=%d]\n",
	       fname, hdr_rec->hdu);
	    
	    fflush(stdout);
	 }

	 ++hdr_rec->hdu;

	 continue;
      }

      hdr_rec->ns = (int) wcs->nxpix;
      hdr_rec->nl = (int) wcs->nypix;

      strcpy(hdr_rec->ctype1, wcs->ctype[0]);
      strcpy(hdr_rec->ctype2, wcs->ctype[1]);

      hdr_rec->crpix1  = wcs->xrefpix;
      hdr_rec->crpix2  = wcs->yrefpix;
      hdr_rec->equinox = wcs->equinox;
      hdr_rec->crval1  = wcs->xref;
      hdr_rec->crval2  = wcs->yref;
      hdr_rec->cdelt1  = wcs->xinc;
      hdr_rec->cdelt2  = wcs->yinc;
      hdr_rec->crota2  = wcs->rot;

      if(hdr_rec->cdelt1 > 0.
      && hdr_rec->cdelt2 > 0.
      && (hdr_rec->crota2 < -90. || hdr_rec->crota2 > 90.))
      {
	 hdr_rec->cdelt1 = -hdr_rec->cdelt1;
	 hdr_rec->cdelt2 = -hdr_rec->cdelt2;

	 hdr_rec->crota2 += 180.;

	 while(hdr_rec->crota2 >= 360.)
	    hdr_rec->crota2 -= 360.;

	 while(hdr_rec->crota2 <= -360.)
	    hdr_rec->crota2 += 360.;
      }


      /* Convert center of image to sky coordinates */

      csys = EQUJ;

      if(strncmp(hdr_rec->ctype1, "RA",   2) == 0)
	 csys = EQUJ;
      if(strncmp(hdr_rec->ctype1, "GLON", 4) == 0)
	 csys = GAL;
      if(strncmp(hdr_rec->ctype1, "ELON", 4) == 0)
	 csys = ECLJ;

      equinox = hdr_rec->equinox;

      pix2wcs (wcs, hdr_rec->ns/2., hdr_rec->nl/2., &lon, &lat);


      /* Convert lon, lat to EQU J2000 */

      convertCoordinates (csys, equinox, lon, lat,
			  EQUJ, 2000., &ra2000, &dec2000, 0.);

      hdr_rec->ra2000  = ra2000;
      hdr_rec->dec2000 = dec2000;

      clockwise = 0;

      if((hdr_rec->cdelt1 < 0 && hdr_rec->cdelt2 < 0)
      || (hdr_rec->cdelt1 > 0 && hdr_rec->cdelt2 > 0)) clockwise = 1;


      if(clockwise)
      {
	 pix2wcs(wcs, -0.5, -0.5, &lon, &lat);
	 convertCoordinates (csys, equinox, lon, lat,
			     EQUJ, 2000., &ra, &dec, 0.);

	 hdr_rec->ra1 = ra;
	 hdr_rec->dec1 = dec;


	 pix2wcs(wcs, wcs->nxpix+0.5, -0.5, &lon, &lat);
	 convertCoordinates (csys, equinox, lon, lat,
			     EQUJ, 2000., &ra, &dec, 0.);

	 hdr_rec->ra2 = ra;
	 hdr_rec->dec2 = dec;


	 pix2wcs(wcs, wcs->nxpix+0.5, wcs->nypix+0.5, &lon, &lat);
	 convertCoordinates (csys, equinox, lon, lat,
			     EQUJ, 2000., &ra, &dec, 0.);

	 hdr_rec->ra3 = ra;
	 hdr_rec->dec3 = dec;


	 pix2wcs(wcs, -0.5, wcs->nypix+0.5, &lon, &lat);
	 convertCoordinates (csys, equinox, lon, lat,
			     EQUJ, 2000., &ra, &dec, 0.);

	 hdr_rec->ra4 = ra;
	 hdr_rec->dec4 = dec;
      }
      else
      {
	 pix2wcs(wcs, -0.5, -0.5, &lon, &lat);
	 convertCoordinates (csys, equinox, lon, lat,
			     EQUJ, 2000., &ra, &dec, 0.);

	 hdr_rec->ra1 = ra;
	 hdr_rec->dec1 = dec;


	 pix2wcs(wcs, wcs->nxpix+0.5, -0.5, &lon, &lat);
	 convertCoordinates (csys, equinox, lon, lat,
			     EQUJ, 2000., &ra, &dec, 0.);

	 hdr_rec->ra2 = ra;
	 hdr_rec->dec2 = dec;


	 pix2wcs(wcs, wcs->nxpix+0.5, wcs->nypix+0.5, &lon, &lat);
	 convertCoordinates (csys, equinox, lon, lat,
			     EQUJ, 2000., &ra, &dec, 0.);

	 hdr_rec->ra3 = ra;
	 hdr_rec->dec3 = dec;


	 pix2wcs(wcs, -0.5, wcs->nypix+0.5, &lon, &lat);
	 convertCoordinates (csys, equinox, lon, lat,
			     EQUJ, 2000., &ra, &dec, 0.);

	 hdr_rec->ra4 = ra;
	 hdr_rec->dec4 = dec;
      }


      x1 = cos(hdr_rec->ra2000*dtr) * cos(hdr_rec->dec2000*dtr);
      y1 = sin(hdr_rec->ra2000*dtr) * cos(hdr_rec->dec2000*dtr);
      z1 = sin(hdr_rec->dec2000*dtr);

      x2 = cos(ra*dtr) * cos(dec*dtr);
      y2 = sin(ra*dtr) * cos(dec*dtr);
      z2 = sin(dec*dtr);

      hdr_rec->radius = acos(x1*x2 + y1*y2 + z1*z2) / dtr;

      free (header);    


      /* Now try to get the values of the */
      /* extra required keywords          */

      for(i=0; i<nfields; ++i)
      {
	 status=0;
	 if(fits_read_keyword(fptr, fields[i].name, value, comment, &status))
	    strcpy(fields[i].value,  fields[i].defval);

         else
         {
	    ptr = value;

	    if(*ptr == '\'' && value[strlen(value)-1] == '\'')
	    {
	       value[strlen(value)-1] = '\0';
	       ++ptr;
	    }

	    strcpy(fields[i].value, ptr);

	    if(strlen(fields[i].value) == 0)
	       strcpy(fields[i].value, fields[i].defval);
         }
      }

      hdr_rec->cntr = cntr;

      print_rec (hdr_rec);

      ++hdr_rec->hdu;
   }

   status = 0;

   fits_close_file(fptr, &status);

   return(nfailed);
}
