/* Module: mPix2Coord

Version  Developer        Date     Change
-------  ---------------  -------  -----------------------
1.0      John Good        15Apr04  Baseline code

*/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <math.h>

#include "fitsio.h"
#include "coord.h"
#include "wcs.h"

#define MAXSTR   256
#define MAXHDR 80000

int stradd      (char *header, char *card);
int readTemplate(char *template);
int printHeader (char *header);

struct WorldCoor *getWCS();

static struct WorldCoor *wcs;

FILE    *fstatus;

extern char *optarg;
extern int optind, opterr;

extern int getopt(int argc, char *const *argv, const char *options);

int checkHdr(char *infile, int hdrflag, int hdu);

int wcsSys; 

int debug;


/*************************************************************************/
/*                                                                       */
/*  mPix2Coord                                                           */
/*                                                                       */
/*  Montage is a set of general reprojection / coordinate-transform /    */
/*  mosaicking programs.  Any number of input images can be merged into  */
/*  an output FITS file.  The attributes of the input are read from the  */
/*  input files; the attributes of the output are read a combination of  */
/*  the command line and a FITS header template file.                    */
/*                                                                       */
/*  This module takes an image FITS header template and a pixel          */
/*  coordinate and outputs the corresponding sky location.               */
/*                                                                       */
/*************************************************************************/

int main(int argc, char **argv)
{
   int      c, hdu, csys, jpeg, invert, naxis2; 

   char     tmpl[MAXSTR];
   char     native_csys[MAXSTR];

   double   ns, nl;
   double   cdelt1, cdelt2;

   double   ix, iy;
   double   lon, lat;
   double   ra,  dec;


   /************************************/
   /* Read the command-line parameters */
   /************************************/

   hdu       =  0;
   opterr    =  0;
   debug     =  0;
   jpeg      =  0;
   fstatus   = stdout;

   while ((c = getopt(argc, argv, "dh:j")) != EOF) 
   {
      switch (c) 
      {
         case 'd':
            debug = 1;
            break;

         case 'h':
            hdu = atoi(optarg);
            break;

         case 'j':
            jpeg = 1;
            break;

         default:
	    fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Usage: %s [-d][-h hdu] image.fits|template.hdr ixpix jypix\"]\n", 
	       argv[0]);
            exit(1);
            break;
      }
   }

   if (argc - optind < 3) 
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Usage: %s [-d][-h hdu] image.fits|template.hdr ixpix jypix\"]\n", 
	 argv[0]);
      exit(1);
   }

   strcpy(tmpl, argv[optind]);

   ix = atof(argv[optind+1]);
   iy = atof(argv[optind+2]);

   if(debug)
   {
      printf("DEBUG> Command-line read.\n");
      fflush(stdout);
   }


   /*****************************************/
   /* Read the image or template file and   */
   /* set up the WCS for the original image */
   /*****************************************/

   checkHdr(tmpl, 2, hdu);

   wcs = getWCS();

   if(jpeg)
   {
      ns = wcs->nxpix;
      nl = wcs->nypix;

      cdelt1 = wcs->xinc;
      cdelt2 = wcs->yinc;

      if (cdelt1 < 0)
	  ix = ix + 0.5;

      if (cdelt1 > 0)
	  ix = ns - ix + 0.5;

      if (cdelt2 < 0)
	  iy = iy + 0.5;

      if (cdelt2 > 0)
	  iy = nl - iy + 0.5;
   }

   pix2wcs(wcs, ix, iy, &lon, &lat);

   csys = EQUJ;

   strcpy(native_csys, "eq J2000");

   if(strncmp(wcs->ctype[0], "RA--", 4) == 0)
   {
      if(wcs->equinox > 1975)
      {
         csys = EQUJ;

         strcpy(native_csys, "eq J2000");

         if(debug)
            printf("csys -> EQUJ\n");
      }
      else
      {
         csys = EQUB;

         strcpy(native_csys, "eq B 1950");

         if(debug)
            printf("csys -> EQUB\n");
      }
   }

   if(strncmp(wcs->ctype[0], "LON-", 4) == 0)
   {
      csys = GAL;

      strcpy(native_csys, "gal");

      if(debug)
         printf("csys -> GAL\n");
   }

   if(strncmp(wcs->ctype[0], "GLON", 4) == 0)
   {
      csys = GAL;

      strcpy(native_csys, "gal");

      if(debug)
         printf("csys -> GAL\n");
   }

   if(strncmp(wcs->ctype[0], "ELON", 4) == 0)
   {
      if(wcs->equinox > 1975)
      {
         csys = ECLJ;

         strcpy(native_csys, "ec J2000");

         if(debug)
            printf("csys -> ECLJ\n");
      }
      else
      {
         csys = ECLB;

         strcpy(native_csys, "ec B1950");

         if(debug)
            printf("csys -> ECLB\n");
      }
   }

   convertCoordinates (csys, wcs->equinox, lon, lat,
                       EQUJ, 2000., &ra, &dec, 0.); 

   printf("[struct stat=\"OK\", native_lon=%-g, native_lat=%-g, native_csys=\"%s\", lon=%.6f, lat=%.6f, locstr=\"%.6f %.6f eq J2000\"]\n",
       lon, lat, native_csys, ra, dec, ra, dec);


   exit(0);
}
