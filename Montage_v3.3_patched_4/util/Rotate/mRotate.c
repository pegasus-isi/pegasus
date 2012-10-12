/* Module: mRotate.c

Version  Developer        Date     Change
-------  ---------------  -------  -----------------------
1.1      John Good        24Jun07  Added correction for CAR projection error
1.0      John Good        11May05  Baseline code

*/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <time.h>
#include <math.h>
#include <fitsio.h>
#include <wcs.h>
#include <coord.h>

#include "montage.h"
#include "mNaN.h"

#define MAXSTR  1024
#define MAXFILE 1024

extern char *optarg;
extern int optind, opterr;

char input_file [MAXSTR];
char output_file[MAXSTR];

char  *input_header;

struct WorldCoor *wcs;

int  readFits      (char *fluxfile);
void printFitsError(int);
void printError    (char *);

int  rotInit       ();
int  rotFwd        (double iin, double jin, double *iout, double *jout);
int  rotRev        (double iout, double jout, double *iin, double *jin);

int  checkHdr      (char *infile, int hdrflag, int hdu);
int  checkWCS      (struct WorldCoor *wcs, int action);
void fixxy         (double *x, double *y, int *offscl);

double xcorrection;
double ycorrection;

int  debug;

struct
{
   fitsfile *fptr;
   long      naxes[2];
   double    crpix1, crpix2;
   double    cd11, cd12, cd21, cd22;
   double    cdelt1, cdelt2;
   double    crota2;
}
   input, output;

double naxis1in;
double naxis2in;
double naxis1out;
double naxis2out;

double sint, cost;
double dtr;

double rotation;

int haveCDELT1;
int haveCDELT2;
int haveCROTA2;

int haveCD1_1;
int haveCD1_2;
int haveCD2_1;
int haveCD2_2;


/*****************************************************/
/*                                                   */
/*  mRotate                                          */
/*                                                   */
/*  This module rotates a FITS image by an arbitrary */
/*  angle.  This module is meant for quick-look      */
/*  only; it is not flux conserving.                 */
/*                                                   */
/*****************************************************/

int main(int argc, char **argv)
{
   int       i, j, nullcnt, status, inlines;
   int       iin, jin, haveRegion, offscl, sys;
   int       outibegin, outiend, outjbegin, outjend;
   int       inibegin, iniend, injbegin, injend;
   long      fpixel[4], innelements, outnelements;
   double    xscale, yscale, epoch;
   double    xin, yin;
   double    ra, dec;
   double    lon, lat;
   double    inxpix, inypix;
   double    outxpix, outypix;
   double    xsize, ysize, maxsize;
   double    xoff, yoff, maxoff;

   double  **indata;
   double   *outdata;

   char     *end;


   /************************************************/
   /* Make a NaN value to use setting blank pixels */
   /************************************************/

   union
   {
      double d;
      char   c[8];
   }
   value;

   double nan;

   for(i=0; i<8; ++i)
      value.c[i] = 255;

   nan = value.d;


   /***************************************/
   /* Process the command-line parameters */
   /***************************************/

   dtr     = atan(1.)/45.;
   debug   = 0;
   fstatus = stdout;

   for(i=0; i<argc; ++i)
   {
      if(strcmp(argv[i], "-s") == 0)
      {
	 if(i+1 >= argc)
	 {
	    printf("[struct stat=\"ERROR\", msg=\"No status file name given\"]\n");
	    exit(1);
	 }

	 if((fstatus = fopen(argv[i+1], "w+")) == (FILE *)NULL)
	 {
	    printf ("[struct stat=\"ERROR\", msg=\"Cannot open status file: %s\"]\n",
	       argv[i+1]);
	    exit(1);
	 }

	 argv += 2;
	 argc -= 2;
      }

      if(strcmp(argv[i], "-r") == 0)
      {
	 if(i+1 >= argc)
	 {
	    printf("[struct stat=\"ERROR\", msg=\"No rotation angle given\"]\n");
	    exit(1);
	 }

	 rotation = strtod(argv[i+1], &end);

	 if(end - argv[i+1] < strlen(argv[i+1]))
	 {
	    printf("[struct stat=\"ERROR\", msg=\"Rotation angle string is invalid: '%s'\"]\n", argv[i+1]);
	    exit(1);
	 }

	 argv += 2;
	 argc -= 2;
      }

      if(strcmp(argv[i], "-d") == 0)
      {
	 if(i+1 >= argc)
	 {
	    printf("[struct stat=\"ERROR\", msg=\"No debug level given\"]\n");
	    exit(1);
	 }

	 debug = strtol(argv[i+1], &end, 0);

	 if(end - argv[i+1] < strlen(argv[i+1]))
	 {
	    printf("[struct stat=\"ERROR\", msg=\"Debug level string is invalid: '%s'\"]\n", argv[i+1]);
	    exit(1);
	 }

	 if(debug < 0)
	 {
	    printf("[struct stat=\"ERROR\", msg=\"Debug level value cannot be negative\"]\n");
	    exit(1);
	 }

	 argv += 2;
	 argc -= 2;
      }
   }
   
   if (argc < 3) 
   {
      printf ("[struct stat=\"ERROR\", msg=\"Usage: mRotate [-d level] [-s statusfile] [-r rotang] in.fits out.fits [ra dec xsize [ysize]]\"]\n");
      exit(1);
   }

   strcpy(input_file, argv[1]);

   if(input_file[0] == '-')
   {
      printf ("[struct stat=\"ERROR\", msg=\"Invalid input file '%s'\"]\n", input_file);
      exit(1);
   }

   strcpy(output_file, argv[2]);

   if(output_file[0] == '-')
   {
      printf ("[struct stat=\"ERROR\", msg=\"Invalid output file '%s'\"]\n", output_file);
      exit(1);
   }


   haveRegion = 0;

   if(argc > 5)
   {
      haveRegion = 1;

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

      maxsize = xsize;

      if(ysize > xsize)
	 maxsize = xsize;
   }

   if(debug >= 1)
   {
      printf("debug            = %d\n",   debug);
      printf("input_file       = [%s]\n", input_file);
      printf("output_file      = [%s]\n", output_file);

      if(haveRegion)
      {
	 printf("ra               = %-g\n",  ra);
	 printf("dec              = %-g\n",  dec);
	 printf("xsize            = %-g\n",  xsize);
	 printf("ysize            = %-g\n",  ysize);
      }

      fflush(stdout);
   }


   /************************/
   /* Read the input image */
   /************************/

   readFits(input_file);

   if(debug >= 1)
   {
      printf("input.naxes[0]       =  %ld\n",   input.naxes[0]);
      printf("input.naxes[1]       =  %ld\n",   input.naxes[1]);
      printf("input.crpix1         =  %-g\n",   input.crpix1);
      printf("input.crpix2         =  %-g\n",   input.crpix2);
      printf("input.cd11           =  %-g\n",   input.cd11);
      printf("input.cd12           =  %-g\n",   input.cd12);
      printf("input.cd21           =  %-g\n",   input.cd21);
      printf("input.cd22           =  %-g\n",   input.cd22);
      printf("input.cdelt1         =  %-g\n",   input.cdelt1);
      printf("input.cdelt2         =  %-g\n",   input.cdelt2);
      printf("input.crota2         =  %-g\n",   input.crota2);
      printf("input.coorflip       =  %d\n",    wcs->coorflip);
      printf("\n");
      fflush(stdout);
   }

   naxis1in = input.naxes[0];
   naxis2in = input.naxes[1];


   /**********************************/
   /* Find the pixel location of the */
   /* sky coordinate specified       */
   /**********************************/

   if(haveRegion)
   {
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

      /* Find the location in the image coordinate system */

      convertCoordinates(EQUJ, 2000., ra, dec, sys, epoch, &lon, &lat, 0.);

      offscl = 0;

      wcs2pix(wcs, lon, lat, &inxpix, &inypix, &offscl);

      fixxy(&inxpix, &inypix, &offscl);

      if(offscl)
      {
	 fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Location is off image\"]\n");
	 fflush(stdout);
	 exit(1);
      }

      if(debug)
      {
	 printf("   ra     = %-g\n", ra);
	 printf("   dec    = %-g\n", dec);
	 printf("-> lon    = %-g\n", lon);
	 printf("   lat    = %-g\n", lat);
	 printf("-> inxpix = %-g\n", inxpix);
	 printf("   inypix = %-g\n", inypix);
	 fflush(stdout);
      }
   }


   /***********************************/
   /* Determine output scale/rotation */
   /***********************************/

   if(haveCD1_1 && haveCD1_2 && haveCD2_1 && haveCD2_2)
   {
      input.crota2 = atan2(input.cd21, input.cd11)/dtr;

      rotInit();

      if(fabs(sin(input.crota2*dtr)) 
       > fabs(cos(input.crota2*dtr)))
      {
	 input.cdelt1 =  input.cd21 / sin(input.crota2*dtr);
	 input.cdelt2 = -input.cd12 / sin(input.crota2*dtr);
      }
      else
      {
	 input.cdelt1 =  input.cd11 / cos(input.crota2*dtr);
	 input.cdelt2 =  input.cd22 / cos(input.crota2*dtr);
      }
   }

   output.cdelt1 = input.cdelt1;
   output.cdelt2 = input.cdelt2;
   output.crota2 = rotation;

   if(wcs->coorflip)
   {
      output.cd11 =  output.cdelt1 * cos(rotation*dtr-90.*dtr);
      output.cd12 = -output.cdelt1 * sin(rotation*dtr-90.*dtr);
      output.cd21 =  output.cdelt2 * sin(rotation*dtr-90.*dtr);
      output.cd22 =  output.cdelt2 * cos(rotation*dtr-90.*dtr);
   }
   else
   {
      output.cd11 =  output.cdelt1 * cos(rotation*dtr);
      output.cd12 = -output.cdelt1 * sin(rotation*dtr);
      output.cd21 =  output.cdelt2 * sin(rotation*dtr);
      output.cd22 =  output.cdelt2 * cos(rotation*dtr);
   }

   rotInit();

   xscale = fabs(input.naxes[0] * cost) 
          + fabs(input.naxes[1] * sint);

   yscale = fabs(input.naxes[0] * sint) 
	  + fabs(input.naxes[1] * cost);

   if(debug >= 1)
   {
      printf("xscale               =  %-g\n",   xscale);
      printf("yscale               =  %-g\n",   yscale);
      printf("\n");
      fflush(stdout);
   }

   output.naxes[0] =  xscale + 1.0;
   output.naxes[1] =  yscale + 1.0;

   naxis1out = output.naxes[0];
   naxis2out = output.naxes[1];


   rotFwd(input.crpix1, input.crpix2, &output.crpix1, &output.crpix2);

   rotFwd(inxpix, inypix, &outxpix, &outypix);

   if(debug >= 1)
   {
      printf("\n");
      printf("haveCDELT             =  %d\n",    haveCDELT1 && haveCDELT2);
      printf("output.naxes[0]       =  %ld\n",   output.naxes[0]);
      printf("output.naxes[1]       =  %ld\n",   output.naxes[1]);
      printf("output.crpix1         =  %-g\n",   output.crpix1);
      printf("output.crpix2         =  %-g\n",   output.crpix2);
      printf("output.cd11           =  %-g\n",   output.cd11);
      printf("output.cd12           =  %-g\n",   output.cd12);
      printf("output.cd21           =  %-g\n",   output.cd21);
      printf("output.cd22           =  %-g\n",   output.cd22);
      printf("output.cdelt1         =  %-g\n",   output.cdelt1);
      printf("output.cdelt2         =  %-g\n",   output.cdelt2);
      printf("output.crota2         =  %-g\n",   output.crota2);
      printf("outxpix               =  %-g\n",   outxpix);
      printf("outypix               =  %-g\n",   outypix);
      printf("\n");
      fflush(stdout);
   }


   /************************************/
   /* Find the range of pixels to keep */
   /************************************/

   if(haveRegion)
   {
      /* Output image range */

      xoff = fabs(xsize/2./input.cdelt1);
      yoff = fabs(ysize/2./input.cdelt2);

      outibegin = outxpix - xoff;
      outiend   = outxpix + xoff + 0.5;

      outjbegin = outypix - yoff;
      outjend   = outypix + yoff + 0.5;

      if(outibegin < 1              ) outibegin = 1;
      if(outibegin > output.naxes[0]) outibegin = output.naxes[0];
      if(outiend   > output.naxes[0]) outiend   = output.naxes[0];
      if(outiend   < 0              ) outiend   = output.naxes[0];

      if(outjbegin < 1              ) outjbegin = 1;
      if(outjbegin > output.naxes[1]) outjbegin = output.naxes[1];
      if(outjend   > output.naxes[1]) outjend   = output.naxes[1];
      if(outjend   < 0              ) outjend   = output.naxes[1];

      if(debug)
      {
	 printf("xoff       = %-g\n", xoff);
	 printf("yoff       = %-g\n", yoff);
	 printf("outibegin  = %d\n",  outibegin);
	 printf("outiend    = %d\n",  outiend);
	 printf("outjbegin  = %d\n",  outjbegin);
	 printf("outjend    = %d\n",  outjend);
	 fflush(stdout);
      }

      if(outibegin >= outiend
      || outjbegin >= outjend)
      {
	 fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"No pixels match output area.\"]\n");
	 fflush(stdout);
	 exit(1);
      }


      /* Input image range */

      maxoff = 1.5 * fabs(ysize/2./input.cdelt2);

      inibegin = inxpix - maxoff;
      iniend   = inxpix + maxoff + 0.5;

      injbegin = inypix - maxoff;
      injend   = inypix + maxoff + 0.5;

      if(inibegin < 1             ) inibegin = 1;
      if(inibegin > input.naxes[0]) inibegin = input.naxes[0];
      if(iniend   > input.naxes[0]) iniend   = input.naxes[0];
      if(iniend   < 0             ) iniend   = input.naxes[0];

      if(injbegin < 1             ) injbegin = 1;
      if(injbegin > input.naxes[1]) injbegin = input.naxes[1];
      if(injend   > input.naxes[1]) injend   = input.naxes[1];
      if(injend   < 0             ) injend   = input.naxes[1];

      if(debug)
      {
	 printf("maxoff    = %-g\n", maxoff);
	 printf("inibegin  = %d\n",  inibegin);
	 printf("iniend    = %d\n",  iniend);
	 printf("injbegin  = %d\n",  injbegin);
	 printf("injend    = %d\n",  injend);
	 fflush(stdout);
      }

      if(inibegin >= iniend
      || injbegin >= injend)
      {
	 fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"No pixels match input area.\"]\n");
	 fflush(stdout);
	 exit(1);
      }
   }
   else
   {
      inibegin = 1;
      iniend   = input.naxes[0];

      injbegin = 1;
      injend   = input.naxes[1];
      
      outibegin = 1;
      outiend   = output.naxes[0];

      outjbegin = 1;
      outjend   = output.naxes[1];
   }


   outnelements = outiend - outibegin + 1;
    innelements =  iniend -  inibegin + 1;
    inlines     =  injend -  injbegin + 1;



   /**********************************************/ 
   /* Allocate memory for the input image pixels */ 
   /**********************************************/ 

   indata = (double **)malloc(inlines * sizeof(double *));

   for(j=0; j<inlines; ++j)
      indata[j] = (double *)malloc(innelements * sizeof(double));

   if(debug >= 1)
   {
      printf("%ld bytes allocated for input image pixels\n", 
	 innelements * inlines * sizeof(double));
      fflush(stdout);
   }


   /***********************************************/ 
   /* Allocate memory for the output image pixels */ 
   /***********************************************/ 

   outdata = (double *)malloc(outnelements * sizeof(double));

   if(debug >= 1)
   {
      printf("%ld bytes allocated for output image pixel row\n", 
	 outnelements * sizeof(double));
      printf("\n");
      fflush(stdout);
   }


   /************************/
   /* Read the input lines */
   /************************/

   fpixel[0] = inibegin;
   fpixel[1] = injbegin;
   fpixel[2] = 1;
   fpixel[3] = 1;

   status = 0;

   if(debug >= 3)
      printf("\n");

   for (j=injbegin; j<injend; ++j)
   {
      if(debug >= 2)
      {
	 if(debug >= 3)
	    printf("Reading input row %5d\n", j);
	 else
	    printf("\rReading input row %5d     ", j);

	 fflush(stdout);
      }


      /***********************************/
      /* Read a line from the input file */
      /***********************************/

      if(fits_read_pix(input.fptr, TDOUBLE, fpixel, innelements, NULL,
		       (void *)indata[j-injbegin], &nullcnt, &status))
	 printFitsError(status);
      
      ++fpixel[1];
   }

   if(debug >= 1)
   {
      printf("Input image read complete.\n\n");
      fflush(stdout);
   }


   /********************************/
   /* Create the output FITS files */
   /********************************/

   remove(output_file);               

   if(fits_create_file(&output.fptr, output_file, &status)) 
      printFitsError(status);           

   if(debug >= 1)
   {
      printf("\nFITS output files created (not yet populated)\n"); 
      fflush(stdout);
   }



   /********************************/
   /* Copy all the header keywords */
   /* from the input to the output */
   /********************************/

   if(fits_copy_header(input.fptr, output.fptr, &status))
      printFitsError(status);           

   if(debug >= 1)
   {
      printf("Header keywords copied to FITS output files\n"); 
      fflush(stdout);
   }

   if(fits_close_file(input.fptr, &status))
      printFitsError(status);


   /************************************/
   /* Reset all the WCS header kewords */
   /************************************/

   if(fits_update_key_lng(output.fptr, "BITPIX", -64, 
				  (char *)NULL, &status))
      printFitsError(status);

   if(fits_update_key_lng(output.fptr, "NAXIS", 2,
                                  (char *)NULL, &status))
      printFitsError(status);

   if(fits_update_key_lng(output.fptr, "NAXIS1", (long)(outiend-outibegin+1),
                                  (char *)NULL, &status))
      printFitsError(status);

   if(fits_update_key_lng(output.fptr, "NAXIS2", (long)(outjend-outjbegin+1),
                                  (char *)NULL, &status))
      printFitsError(status);

   if(fits_update_key_dbl(output.fptr, "CRPIX1", output.crpix1-outibegin, -14,
                                  (char *)NULL, &status))
      printFitsError(status);

   if(fits_update_key_dbl(output.fptr, "CRPIX2", output.crpix2-outjbegin, -14,
                                  (char *)NULL, &status))
      printFitsError(status);

   if(haveCDELT1 && fits_update_key_dbl(output.fptr, "CDELT1", output.cdelt1, -14,
                                  (char *)NULL, &status))
      printFitsError(status);

   if(haveCDELT2 && fits_update_key_dbl(output.fptr, "CDELT2", output.cdelt2, -14,
                                  (char *)NULL, &status))
      printFitsError(status);

   if(haveCROTA2 && fits_update_key_dbl(output.fptr, "CROTA2", output.crota2, -14,
                                  (char *)NULL, &status))
      printFitsError(status);

   if(haveCD1_1 && fits_update_key_dbl(output.fptr, "CD1_1", output.cd11, -14,
                                  (char *)NULL, &status))
      printFitsError(status);

   if(haveCD1_2 && fits_update_key_dbl(output.fptr, "CD1_2", output.cd12, -14,
                                  (char *)NULL, &status))
      printFitsError(status);

   if(haveCD2_1 && fits_update_key_dbl(output.fptr, "CD2_1", output.cd21, -14,
                                  (char *)NULL, &status))
      printFitsError(status);

   if(haveCD2_2 && fits_update_key_dbl(output.fptr, "CD2_2", output.cd22, -14,
                                  (char *)NULL, &status))
      printFitsError(status);

   if(debug >= 1)
   {
      printf("WCS keywords reset in output\n\n"); 
      fflush(stdout);
   }


   /************************/
   /* Write the image data */
   /************************/

   fpixel[0] = 1;
   fpixel[1] = 1;

   for(j=outjbegin; j<=outjend; ++j)
   {
      if(debug >= 2)
      {
	 if(debug >= 3)
	    printf("\n");

	 printf("\rWriting output row %5d  ", j);

	 if(debug >= 3)
	    printf("\n");

	 fflush(stdout);
      }

      for(i=outibegin; i<= outiend; ++i)
      {
         rotRev((double)i, (double)j, &xin, &yin);

	 iin = (int)(xin + 0.5);
	 jin = (int)(yin + 0.5);

	 if(debug >= 3)
	 {
	    printf("iin = %d (0 to %ld)   jin = %d (0 to %ld) -> indata[%d][%d]\n",
	       iin, input.naxes[0], jin, input.naxes[1], 
	       jin-injbegin, iin-inibegin);
	    fflush(stdout);
	 }

	 if(iin < 0 || iin >= input.naxes[0]
	 || jin < 0 || jin >= input.naxes[1])
	    outdata[i-outibegin] = nan;
	 else
	    outdata[i-outibegin] = indata[jin-injbegin+1][iin-inibegin+1];

	 if(debug >= 3)
	 {
	    printf("outdata[%d] = %-g\n", i-outibegin, outdata[i-outibegin]);
	    fflush(stdout);
	 }
      }

      if(debug >= 2)
      {
	 for(i=0; i<outnelements; ++i)
	    printf("line %d: outdata[%d] = %-g\n",
	       j, i, outdata[i]);
	 fflush(stdout);
      }

      if (fits_write_pix(output.fptr, TDOUBLE, fpixel, outnelements, 
			 (void *)outdata, &status))
	 printFitsError(status);

      ++fpixel[1];
   }

   if(debug >= 1)
   {
      printf("Data written to FITS data image\n"); 
      fflush(stdout);
   }


   /******************************/
   /* Close the output FITS file */
   /******************************/

   if(fits_close_file(output.fptr, &status))
      printFitsError(status);           

   if(debug >= 1)
   {
      printf("FITS data image finalized\n"); 
      fflush(stdout);
   }

   fprintf(fstatus, "[struct stat=\"OK\"]\n");
   fflush(fstatus);

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


/*******************************************/
/*                                         */
/*  Open a FITS file pair and extract the  */
/*  pertinent header information.          */
/*                                         */
/*******************************************/

int readFits(char *fluxfile)
{
   int    status, nfound;
   long   naxes[2];
   double crpix[2];
   double cd11, cd12, cd21, cd22;
   double cdelt1, cdelt2, crota2;
   char   errstr[MAXSTR];
   double x, y;
   double ix, iy;
   double xpos, ypos;
   int    offscl;


   status = 0;

   checkHdr(fluxfile, 0, 0);

   if(fits_open_file(&input.fptr, fluxfile, READONLY, &status))
   {
      sprintf(errstr, "Image file %s missing or invalid FITS", fluxfile);
      printError(errstr);
   }

   if(fits_get_image_wcs_keys(input.fptr, &input_header, &status))
      printFitsError(status);

   wcs = wcsinit(input_header);

   if(wcs == (struct WorldCoor *)NULL)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"wcsinit() failed.\"]\n");
      exit(1);
   }

   checkWCS(wcs, 0);


   /* Kludge to get around bug in WCS library:   */
   /* 360 degrees sometimes added to pixel coord */

   ix = 0.5;
   iy = 0.5;

   offscl = 0;

   pix2wcs(wcs, ix, iy, &xpos, &ypos);
   wcs2pix(wcs, xpos, ypos, &x, &y, &offscl);

   xcorrection = x-ix;
   ycorrection = y-iy;


   haveCDELT1 = 1;
   haveCDELT2 = 1;
   haveCROTA2 = 1;

   haveCD1_1  = 1;
   haveCD1_2  = 1;
   haveCD2_1  = 1;
   haveCD2_2  = 1;

   if(fits_read_keys_lng(input.fptr, "NAXIS", 1, 2, naxes, &nfound, &status))
      printFitsError(status);
   
   input.naxes[0] = naxes[0];
   input.naxes[1] = naxes[1];

   if(fits_read_keys_dbl(input.fptr, "CRPIX", 1, 2, crpix, &nfound, &status))
      printFitsError(status);

   input.crpix1 = crpix[0];
   input.crpix2 = crpix[1];

   status = 0;
   fits_read_key(input.fptr, TDOUBLE, "CDELT1", &cdelt1, (char *)NULL, &status);
   if(status == KEY_NO_EXIST)
      haveCDELT1 = 0;
   else
      input.cdelt1 = cdelt1;

   status = 0;
   fits_read_key(input.fptr, TDOUBLE, "CDELT2", &cdelt2, (char *)NULL, &status);
   if(status == KEY_NO_EXIST)
      haveCDELT2 = 0;
   else
      input.cdelt2 = cdelt2;

   status = 0;
   fits_read_key(input.fptr, TDOUBLE, "CROTA2", &crota2, (char *)NULL, &status);
   if(status == KEY_NO_EXIST)
      haveCROTA2 = 0;
   else
      input.crota2 = crota2;

   status = 0;
   fits_read_key(input.fptr, TDOUBLE, "CD1_1", &cd11, (char *)NULL, &status);
   if(status == KEY_NO_EXIST)
      haveCD1_1 = 0;
   else
      input.cd11 = cd11;

   status = 0;
   fits_read_key(input.fptr, TDOUBLE, "CD1_2", &cd12, (char *)NULL, &status);
   if(status == KEY_NO_EXIST)
      haveCD1_2 = 0;
   else
      input.cd12 = cd12;

   status = 0;
   fits_read_key(input.fptr, TDOUBLE, "CD2_1", &cd21, (char *)NULL, &status);
   if(status == KEY_NO_EXIST)
      haveCD2_1 = 0;
   else
      input.cd21 = cd21;

   status = 0;
   fits_read_key(input.fptr, TDOUBLE, "CD2_2", &cd22, (char *)NULL, &status);
   if(status == KEY_NO_EXIST)
      haveCD2_2 = 0;
   else
      input.cd22 = cd22;

   return 0;
}



/******************************/
/*                            */
/*  Print out general errors  */
/*                            */
/******************************/

void printError(char *msg)
{
   fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"%s\"]\n", msg);
   exit(1);
}




/***********************************/
/*                                 */
/*  Print out FITS library errors  */
/*                                 */
/***********************************/

void printFitsError(int status)
{
   char status_str[FLEN_STATUS];

   fits_get_errstatus(status, status_str);

   fprintf(fstatus, "[struct stat=\"ERROR\", status=%d, msg=\"%s\"]\n", status, status_str);

   exit(1);
}


int rotInit()
{
   if(wcs->coorflip)
   {
      rotation = -rotation;

      sint = sin(rotation*dtr + input.crota2*dtr + 90.*dtr);
      cost = cos(rotation*dtr + input.crota2*dtr + 90.*dtr);
   }
   else
   {
      sint = sin(rotation*dtr - input.crota2*dtr);
      cost = cos(rotation*dtr - input.crota2*dtr);
   }

   return 0;
}


int rotFwd(double iin, double jin, double *iout, double *jout)
{
   double xin,  yin;
   double xout, yout;

   xin = iin - naxis1in/2.;
   yin = jin - naxis2in/2.;

   xout =  xin*cost - yin*sint;
   yout =  xin*sint + yin*cost;

   *iout = xout + naxis1out/2.;
   *jout = yout + naxis2out/2.;

   if(debug >= 3)
   {
      printf("rotFwd: %-g,%-g -> %-g,%-g -> %-g,%-g -> %-g, %-g\n",
	 iin, jin, xin, yin, xout, yout, *iout, *jout);
      fflush(stdout);
   }

   return 0;
}



int rotRev(double iout, double jout, double *iin, double *jin)
{
   double xin,  yin;
   double xout, yout;

   xout = iout - naxis1out/2.;
   yout = jout - naxis2out/2.;

   xin =  xout*cost + yout*sint;
   yin = -xout*sint + yout*cost;

   *iin = xin + naxis1in/2.;
   *jin = yin + naxis2in/2.;

   if(debug >= 3)
   {
      printf("rotRev: %-g,%-g -> %-g,%-g -> %-g,%-g -> %-g, %-g\n",
	 iout, jout, xout, yout, xin, yin, *iin, *jin);
      fflush(stdout);
   }

   return 0;
}
