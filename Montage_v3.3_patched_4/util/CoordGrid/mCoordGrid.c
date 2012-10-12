/* Module: mCoordGrid

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
/*  mCoordGrid                                                           */
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
   int      c, hdu, csys, csysg;

   char     tmpl       [MAXSTR];
   char     native_csys[MAXSTR];
   char     grid_csys  [MAXSTR];

   double   ns, nl;
   double   ix, iy;
   double   cdelt1, cdelt2;
   double   equinox;

   double   loni, lati;
   double   lon,  lat;


   /************************************/
   /* Read the command-line parameters */
   /************************************/

   hdu       =  0;
   opterr    =  0;
   debug     =  0;
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

         default:
	    fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Usage: %s [-d][-h hdu] image.fits|template.hdr [csys [equinox]]\"]\n", 
	       argv[0]);
            exit(1);
            break;
      }
   }

   if (argc - optind < 2) 
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Usage: %s [-d][-h hdu] image.fits|template.hdr [csys [equinox]]\"]\n", 
	 argv[0]);
      exit(1);
   }

   strcpy(tmpl, argv[optind]);

   strcpy(grid_csys, "EQ");

   if(argc - optind >= 2)
      strcpy(grid_csys, argv[optind+1]);

   equinox = 2000.;

   if(argc - optind >= 3)
      equinox = atof(argv[optind+2]);

        if(strncasecmp(grid_csys, "equb", 4) == 0) csysg = EQUB;
   else if(strncasecmp(grid_csys, "eq",   2) == 0) csysg = EQUJ;
   else if(strncasecmp(grid_csys, "eclb", 4) == 0) csysg = ECLB;
   else if(strncasecmp(grid_csys, "ec",   2) == 0) csysg = ECLJ;
   else if(strncasecmp(grid_csys, "ga",   2) == 0) csysg = GAL;
   else if(strncasecmp(grid_csys, "su",   2) == 0) csysg = SGAL;
   else if(strncasecmp(grid_csys, "sg",   2) == 0) csysg = SGAL;

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

   ns = wcs->nxpix;
   nl = wcs->nypix;

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

   for(ix=0.; ix<ns; ix+=1.0)
   {
      iy = ix;

      pix2wcs(wcs, ix, iy, &loni, &lati);

      convertCoordinates (csys, wcs->equinox, loni, lati,
			  csysg, equinox, &lon, &lat, 0.); 

      printf("XXX> %-g,%-g -> %.6f,%.6f -> %.6f,%.6f\n", ix, iy, loni, lati, lon, lat);
   }

   printf("[struct stat=\"OK\", native_csys=\"%s\", grid_csys=\"%s\"]\n",
       native_csys, grid_csys);

   exit(0);
}
