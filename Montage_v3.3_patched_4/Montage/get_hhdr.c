/* Module: get_hhdr.c

Version  Developer        Date     Change
-------  ---------------  -------  -----------------------
1.0      John Good        31Jan05  Baseline code

*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/param.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <math.h>
#include <coord.h>
#include <wcs.h>

#include "hdr_rec.h"

extern int   debug;
extern int   showbad;
extern int   cntr;
extern FILE *tblf;

int  checkWCS (struct WorldCoor *wcs, int action);
void print_rec(struct Hdr_rec*);



/* get_hhdr reads the FITS headers from a file and parses */
/* the values into a structure more easily handled by     */
/* Montage modules (Hdr_rec)                              */

int get_hhdr (char *fname, struct Hdr_rec *hdr_rec, char *msg)
{
   char      header[80000];
   char      str[132];
   FILE     *fptr;
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

   fptr = fopen(fname, "r");
 
   if(fptr == (FILE *)NULL)
   {
      sprintf (msg, "Cannot open header file %s", fname);

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

   strcpy(header, "");

   while(1)
   {
      if(fgets(str, 80, fptr) == (char *)NULL)
         break;
      
      while(str[strlen(str)-1] == '\n'
         || str[strlen(str)-1] == '\r')
            str[strlen(str)-1] =  '\0';

      for(i=strlen(str); i<80; ++i)
	 str[i] = ' ';

      str[80] = '\0';

      strcat(header, str);
   }

   wcs = wcsinit(header);

   if(wcs == (struct WorldCoor *)NULL) 
   {
      if(hdr_rec->hdu == 1)
	 first_failed = 1;

      ++nfailed;

      if(showbad)
      {
	 printf("[struct stat=\"INFO\", msg=\"WCS lib init failure\", file=\"%s\", hdu=%d]\n",
	    fname, hdr_rec->hdu);
	 
	 fflush(stdout);
      }

      ++hdr_rec->hdu;
      
      return(1);
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

      if(showbad)
      {
	 printf("[struct stat=\"INFO\", msg=\"Bad WCS\", file=\"%s\", hdu=%d]\n",
	    fname, hdr_rec->hdu);
	 
	 fflush(stdout);
      }

      ++hdr_rec->hdu;

      return(1);
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

   hdr_rec->cntr = cntr;
   print_rec (hdr_rec);

   status = 0;
   fclose(fptr);

   return(nfailed);
}
