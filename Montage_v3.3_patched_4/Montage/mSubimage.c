/* Module: mSubimage.c

Version  Developer        Date     Change
-------  ---------------  -------  -----------------------
5.0      John Good        24Jun07  Add -c (shrinkwrap) mode where the
				   subimage is chosen based on the range
			           of non-blank pixels
4.6      John Good        24Jun07  Added correction for CAR projection error
4.5      John Good        15Sep06  Corrected usage message
4.4      John Good        08Sep05  Adjusted rounding so that subimages in
				   different parts of the image will more
				   often have the same number of pixels.
4.3      John Good        25Aug05  Added one more pixel in both X and Y so
				   we can get the edges of the input and
				   added -a flag to force getting the whole
				   image (mostly for extracting whole HDUs)
4.2      John Good        04Jun05  Added output keyword ("content") to signal
                                   when the cutout region is blank or flat
4.1      John Good        16May05  Added checks for correct number of args
				   in pixel range mode and for pixel ranges
				   completely outside input image
4.0      John Good        13Feb05  Modified code to preserve original BITPIX
				   (originally, we were converting to 64-bit)
				   actually in subImage.c but we note it here
3.3      Loring Craymer   20Jan05  Factored out image extraction code
                                   (subImage.h, subImage.c) so that it could
				   also be used in  a new version of mTile
				   that properly handles DSS images.
3.2      John Good        02Dec04  Added special processing for DSS CNPIX 
3.1      John Good        03Aug04  Changed precision on CRPIX; was getting
				   low-level round-off treated as precision
3.0      John Good        28Jun04  Changed to do our own subsetting.
				   FITS library failing for large files.
2.1      John Good        07Jun04  Modified FITS key updating precision
2.0      John Good        25May04  Added "pixel mode" subsetting
1.5      John Good        25Aug03  Added status file processing
1.4      John Good        30Apr03  Removed the getopt() library use; it
				   screws up on negative dec values.
1.3      John Good        30Apr03  Forgot to exit when no overlap. 
                                   Also, wrong message text for bad output
				   file path.
1.2      John Good        22Mar03  Replaced wcsCheck with checkHdr
1.1      John Good        13Mar03  Added WCS header check and
				   modified command-line processing
				   to use getopt() library.  Check for
				   valid image sizes. Added more informative
				   FITS open error messages.  Removed
				   extra printFitsError() call.
1.0      John Good        29Jan03  Baseline code

*/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <math.h>
#include "fitsio.h"
#include "wcs.h"
#include "coord.h"

#include "montage.h"
#include "subImage.h"

int  checkHdr (char *infile, int hdrflag, int hdu);
void fixxy    (double *x, double *y, int *offscl);

struct WorldCoor *wcs;

double xcorrection;
double ycorrection;

extern int debug;

extern char content[];


/*************************************************************************/
/*                                                                       */
/*  mSubimage                                                            */
/*                                                                       */
/*  This program subsets an input image around a location of interest    */
/*  and creates a new output image consisting of just those pixels.      */
/*  The location is defined by the RA,Dec (J2000) of the new center and  */
/*  the XY size in degrees of the area (X and Y) in the direction of     */
/*  the image axes, not Equatorial coordinates.                          */
/*                                                                       */
/*************************************************************************/

int main(int argc, char **argv)
{
   char      infile [1024];
   char      outfile[1024];
   char      appname[1024];

   fitsfile *infptr, *outfptr;

   int       i, offscl, pixmode;
   double    cdelt[10];
   int       hdu, allPixels, haveHDU, shrinkWrap;
   int       imin, imax, jmin, jmax;

   int       sys;
   double    epoch;
   double    ra, dec;
   double    lon, lat;
   double    xpix, ypix;
   double    xsize, ysize;
   double    xoff, yoff;
   double    rotang, dtr;

   double    x, y;
   double    ix, iy;
   double    xpos, ypos;

   char     *end;

   char     *header[2];

   int       status = 0;

   struct imageParams params;

   dtr = atan(1.)/45.;

   debug      = 0;
   pixmode    = 0;
   allPixels  = 0;
   shrinkWrap = 0;
   hdu        = 0;
   haveHDU    = 0;

   fstatus = stdout;

   strcpy(appname, argv[0]);
      
   if(argc < 2)
   {
      printf("[struct stat=\"ERROR\", msg=\"Usage: %s [-d][-a(ll pixels)][-h hdu][-s statusfile] in.fit out.fit ra dec xsize [ysize] | %s -p [-d][-h hdu][-s statusfile] in.fit out.fit xstartpix ystartpix xpixsize [ypixsize] | %s -c [-d][-h hdu][-s statusfile] in.fit out.fit\"]\n", appname, appname, appname);
      exit(1);
   }

   for(i=0; i<argc; ++i)
   {
      if(strcmp(argv[i], "-d") == 0)
	 debug = 1;
      
      if(strcmp(argv[i], "-a") == 0)
	 allPixels = 1;
      
      if(strcmp(argv[i], "-p") == 0)
	 pixmode = 1;
      
      if(strcmp(argv[i], "-c") == 0)
	 shrinkWrap = 1;
      
      if(i<argc-1 && strncmp(argv[i], "-h", 2) == 0)
      {
         hdu = strtol(argv[i+1], &end, 10);

         if(end < argv[i+1] + strlen(argv[i+1]) || hdu < 0)
         {
            printf("[struct stat=\"ERROR\", msg=\"HDU value (%s) must be a non-negative integer\"]\n",
	       argv[i+1]);
            exit(1);
         }

         haveHDU = 1;
         ++i;
      }
      
      if(i<argc-1 && strncmp(argv[i], "-s", 2) == 0)
      {
	 if((fstatus = fopen(argv[i+1], "w+")) == (FILE *)NULL)
	 {
	    printf ("[struct stat=\"ERROR\", msg=\"Cannot open status file: %s\"]\n",
	       argv[i+1]);
	    exit(1);
	 }

         ++i;
      }
   }

   if(debug)
   {
      ++argv;
      --argc;
   }

   if(allPixels)
   {
      ++argv;
      --argc;
   }

   if(shrinkWrap)
   {
      ++argv;
      --argc;
   }

   if(pixmode)
   {
      ++argv;
      --argc;
   }

   if(fstatus != stdout)
   {
      argv += 2;
      argc -= 2;
   }

   if(haveHDU)
   {
      argv += 2;
      argc -= 2;
   }

   if((shrinkWrap || allPixels) && argc < 3)
   {
      printf("[struct stat=\"ERROR\", msg=\"Usage: %s [-d][-a(ll pixels)][-h hdu][-s statusfile] in.fit out.fit ra dec xsize [ysize] | %s -p [-d][-h hdu][-s statusfile] in.fit out.fit xstartpix ystartpix xpixsize [ypixsize] | %s -c [-d][-h hdu][-s statusfile] in.fit out.fit\"]\n", appname, appname, appname);
      exit(1);
   }

   if (!shrinkWrap && !allPixels && (argc < 6 || (pixmode && argc < 6))) 
   {
      printf("[struct stat=\"ERROR\", msg=\"Usage: %s [-d][-a(ll pixels)][-h hdu][-s statusfile] in.fit out.fit ra dec xsize [ysize] | %s -p [-d][-h hdu][-s statusfile] in.fit out.fit xstartpix ystartpix xpixsize [ypixsize] | %s -c [-d][-h hdu][-s statusfile] in.fit out.fit\"]\n", appname, appname, appname);
      exit(1);
   }

   strcpy(infile,  argv[1]);
   strcpy(outfile, argv[2]);

   if(allPixels)
   {
      pixmode = 1;

      ra  = 0.;
      dec = 0.;

      xsize = 1.e25;
      ysize = 1.e25;
   }
   else if(!shrinkWrap)
   {
      ra  = strtod(argv[3], &end);

      if(end < argv[3] + (int)strlen(argv[3]))
      {
	 fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Center RA string (%s) cannot be interpreted as a real number\"]\n", 
	    argv[3]);
	 exit(1);
      }

      dec = strtod(argv[4], &end);

      if(end < argv[4] + (int)strlen(argv[4]))
      {
	 fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Center Dec string (%s) cannot be interpreted as a real number\"]\n", 
	    argv[4]);
	 exit(1);
      }

      xsize = strtod(argv[5], &end);
      ysize = xsize;

      if(end < argv[5] + (int)strlen(argv[5]))
      {
	 fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"X size string (%s) cannot be interpreted as a real number\"]\n", 
	    argv[5]);
	 exit(1);
      }

      if (argc > 6) 
      {
	 ysize = strtod(argv[6], &end);

	 if(end < argv[6] + (int)strlen(argv[6]))
	 {
	    fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Y size string (%s) cannot be interpreted as a real number\"]\n", 
	       argv[6]);
	    exit(1);
	 }
      }
   }
   
   if(!shrinkWrap)
   {
      if(xsize <= 0.)
      {
	 fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Invalid 'x' size\"]\n");
	 exit(1);
      }

      if(ysize <= 0.)
      {
	 fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Invalid 'y' size\"]\n");
	 exit(1);
      }
   }
   

   /****************************************/
   /* Open the (unsubsetted) original file */
   /* to get the WCS info                  */
   /****************************************/

   checkHdr(infile, 0, hdu);

   header[0] = malloc(32768);
   header[1] = (char *)NULL;

   if(fits_open_file(&infptr, infile, READONLY, &status))
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Image file %s missing or invalid FITS\"]\n",
	 infile);
      exit(1);
   }

   if(shrinkWrap)
   {
      montage_dataRange(infptr, &imin, &imax, &jmin, &jmax);

      if(debug)
      {
         printf("imin  = %d\n", imin);
         printf("imax  = %d\n", imax);
         printf("jmin  = %d\n", jmin);
         printf("jmax  = %d\n", jmax);
         fflush(stdout);
      }
   }

   if(hdu > 0)
   {
      if(fits_movabs_hdu(infptr, hdu+1, NULL, &status))
      {
	 fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Can't find HDU %d\"]\n", hdu);
	 exit(1);
      }
   }

   wcs = montage_getFileInfo(infptr, header, &params);

   rotang = atan2(wcs->cd[2], wcs->cd[0])/dtr;

   while(rotang <   0.) rotang += 360.;
   while(rotang > 360.) rotang -= 360.;

   if((rotang >  45. && rotang < 135.)
   || (rotang > 225. && rotang < 315.))
   {
      cdelt[0] = wcs->cd[2]/sin(rotang*dtr);
      cdelt[1] = wcs->cd[1]/sin(rotang*dtr);
   }
   else
   {
      cdelt[0] = wcs->cd[0]/cos(rotang*dtr);
      cdelt[1] = wcs->cd[3]/cos(rotang*dtr);
   }

   if(debug)
   {
      for(i=0; i<params.naxis; ++i)
      {
	 printf("crpix%d = %-g\n", i+1, params.crpix[i]);
	 printf("cdelt%d = %-g\n", i+1, cdelt[i]);
      }

      fflush(stdout);
   }


   /* Kludge to get around bug in WCS library:   */
   /* 360 degrees sometimes added to pixel coord */

   ix = 0.5;
   iy = 0.5;

   offscl = 0;

   pix2wcs(wcs, ix, iy, &xpos, &ypos);
   wcs2pix(wcs, xpos, ypos, &x, &y, &offscl);

   xcorrection = x-ix;
   ycorrection = y-iy;


   /* Extract the coordinate system and epoch info */

   if(wcs->syswcs == WCS_J2000)
   {
      sys   = EQUJ;
      epoch = 2000.;

      if(wcs->equinox == 1950.)
	 epoch = 1950;
   }
   else if(wcs->syswcs == WCS_B1950)
   {
      sys   = EQUB;
      epoch = 1950.;

      if(wcs->equinox == 2000.)
	 epoch = 2000;
   }
   else if(wcs->syswcs == WCS_GALACTIC)
   {
      sys   = GAL;
      epoch = 2000.;
   }
   else if(wcs->syswcs == WCS_ECLIPTIC)
   {
      sys   = ECLJ;
      epoch = 2000.;

      if(wcs->equinox == 1950.)
      {
	 sys   = ECLB;
	 epoch = 1950.;
      }
   }
   else       
   {
      sys   = EQUJ;
      epoch = 2000.;
   }
   if(debug)
   {
      printf("input coordinate system = %d\n", EQUJ);
      printf("input epoch             = %-g\n", 2000.);
      printf("image coordinate system = %d\n", sys);
      printf("image epoch             = %-g\n", epoch);
      fflush(stdout);
   }


   /******************************************/
   /* If we are working in shrinkwrap mode,  */
   /* we use the ranges determined above.    */
   /*                                        */
   /* If we are working in pixel mode, we    */
   /* already have the info needed to subset */
   /* the image.                             */
   /*                                        */
   /* Otherwise, we need to convert the      */
   /* coordinates to pixel space.            */
   /******************************************/

   if(shrinkWrap)
   {
      params.ibegin = (int) imin;
      params.iend   = (int) imax;

      params.jbegin = (int) jmin;
      params.jend   = (int) jmax;
   }

   if(pixmode)
   {
      params.ibegin = (int)ra;
      params.iend   = (int)(ra + xsize + 0.5);

      params.jbegin = (int)dec;
      params.jend   = (int)(dec + ysize + 0.5);

      if(params.ibegin < 1              ) params.ibegin = 1;
      if(params.ibegin > params.naxes[0]) params.ibegin = params.naxes[0];
      if(params.iend   > params.naxes[0]) params.iend   = params.naxes[0];
      if(params.iend   < 0              ) params.iend   = params.naxes[0];

      if(params.jbegin < 1              ) params.jbegin = 1;
      if(params.jbegin > params.naxes[1]) params.jbegin = params.naxes[1];
      if(params.jend   > params.naxes[1]) params.jend   = params.naxes[1];
      if(params.jend   < 0              ) params.jend   = params.naxes[1];
   }
   
   else
   {
      /**********************************/
      /* Find the pixel location of the */
      /* sky coordinate specified       */
      /**********************************/

      convertCoordinates(EQUJ, 2000., ra, dec, sys, epoch, &lon, &lat, 0.);

      offscl = 0;

      wcs2pix(wcs, lon, lat, &xpix, &ypix, &offscl);

      fixxy(&xpix, &ypix, &offscl);

      /****** Skip this check: the location may be of the image but part of the region on ********
      if(offscl == 1)
      {
	 fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Location is off image\"]\n");
	 fflush(stdout);
	 exit(1);
      }
      ********************************************************************************************/

      if(debug)
      {
	 printf("   ra   = %-g\n", ra);
	 printf("   dec  = %-g\n", dec);
	 printf("-> lon  = %-g\n", lon);
	 printf("   lat  = %-g\n", lat);
	 printf("-> xpix = %-g\n", xpix);
	 printf("   ypix = %-g\n", ypix);
	 fflush(stdout);
      }


      /************************************/
      /* Find the range of pixels to keep */
      /************************************/

      xoff = fabs(xsize/2./cdelt[0]);
      yoff = fabs(ysize/2./cdelt[1]);

      params.ibegin = xpix - xoff;
      params.iend   = params.ibegin + floor(2.*xoff + 1.0);

      params.jbegin = ypix - yoff;
      params.jend   = params.jbegin + floor(2.*yoff + 1.0);

      if((   params.ibegin <              1
          && params.iend   <              1 )
      || (   params.ibegin > params.naxes[0]
          && params.iend   > params.naxes[0])
      || (   params.jbegin <              1
          && params.jend   <              1 )
      || (   params.jbegin > params.naxes[1]
          && params.jend   > params.naxes[1]))
      {
	 fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Region outside image.\"]\n");
	 fflush(stdout);
	 exit(1);
      }

      if(params.ibegin < 1              ) params.ibegin = 1;
      if(params.ibegin > params.naxes[0]) params.ibegin = params.naxes[0];
      if(params.iend   > params.naxes[0]) params.iend   = params.naxes[0];
      if(params.iend   < 0              ) params.iend   = params.naxes[0];

      if(params.jbegin < 1              ) params.jbegin = 1;
      if(params.jbegin > params.naxes[1]) params.jbegin = params.naxes[1];
      if(params.jend   > params.naxes[1]) params.jend   = params.naxes[1];
      if(params.jend   < 0              ) params.jend   = params.naxes[1];

      if(debug)
      {
	 printf("cdelt1  = %-g\n", cdelt[0]);
	 printf("cdelt2  = %-g\n", cdelt[1]);
	 printf("xsize   = %-g\n", xsize);
	 printf("ysize   = %-g\n", ysize);
	 printf("xoff    = %-g\n", xoff);
	 printf("yoff    = %-g\n", yoff);
	 printf("ibegin  = %d\n",  params.ibegin);
	 printf("iend    = %d\n",  params.iend);
	 printf("jbegin  = %d\n",  params.jbegin);
	 printf("jend    = %d\n",  params.jend);
	 fflush(stdout);
      }

      if(params.ibegin >= params.iend
      || params.jbegin >= params.jend)
      {
	 fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"No pixels match area.\"]\n");
	 fflush(stdout);
	 exit(1);
      }
   }

   params.nelements = params.iend - params.ibegin + 1;

   if(debug)
   {
      printf("ibegin    = %d\n",  params.ibegin);
      printf("iend      = %d\n",  params.iend);
      printf("nelements = %ld\n", params.nelements);
      printf("jbegin    = %d\n",  params.jbegin);
      printf("jend      = %d\n",  params.jend);
      fflush(stdout);
   }



   /**************************/
   /* Create the output file */
   /**************************/

   unlink(outfile);

   if(fits_create_file(&outfptr, outfile, &status))
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Can't create output file: %s\"]\n",
	 outfile);
      exit(1);
   }
   

   /********************************/
   /* Copy all the header keywords */
   /* from the input to the output */
   /********************************/

   montage_copyHeaderInfo(infptr, outfptr, &params);


   /************************/
   /* Copy the data subset */
   /************************/

   montage_copyData(infptr, outfptr, &params);


   /*******************/
   /* Close the files */
   /*******************/

   if(fits_close_file(outfptr, &status))
      montage_printFitsError(status);

   if(fits_close_file(infptr, &status))
      montage_printFitsError(status);

   fprintf(fstatus, "[struct stat=\"OK\", content=\"%s\"]\n", content);
   fflush(stdout);
   exit(0);
}


/**************************************************/
/*  Projections like CAR sometimes add an extra   */
/*  360 degrees worth of pixels to the return     */
/*  and call it off-scale.                        */
/**************************************************/

void fixxy(double *x, double *y, int *offscl)
{
   *x = *x - xcorrection;
   *y = *y - ycorrection;

   *offscl = 0;

   if(*x < 0.
   || *x > wcs->nxpix+1.
   || *y < 0.
   || *y > wcs->nypix+1.)
      *offscl = 1;

   return;
}
