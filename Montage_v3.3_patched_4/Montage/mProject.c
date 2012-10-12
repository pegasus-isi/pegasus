/* Module: mProject.c

Version  Developer        Date     Change
-------  ---------------  -------  -----------------------
2.4      John Good        15May08  Add -f flag to ensure full region is used
2.3      John Good        24Jun07  Added correction for CAR projection error
2.2      John Good        15Jun05  Added -X option to force reprojection
                                   of whole image, even if part of it is
                                   outside region of interest.
2.1      John Good        31May05  Added option flux rescaling 
				   (e.g. magnitude zero point correction)
2.0      John Good        25Mar05  Added in weight image and HDU functionality
1.23     John Good        13Oct04  Changed format for printing time
1.22     John Good        08Oct04  Put in check for "flipped" lon, lat axes
1.21     John Good        03Aug04  Changed precision on updated keywords
1.20     John Good        29Jul04  Changed from using cd to using xinc, yinc
1.19     John Good        28Jul04  Fixed bug in cd -> cdelt computation
1.18     John Good        27Jul04  Added error message for malloc() failure
1.17     John Good        07Jun04  Modified FITS key updating precision
1.16     John Good        13Apr04  Use cd matrix for determining 
				   clockwise/counterclockwise
1.15     John Good        25Nov03  Added extern optarg references
1.14     John Good        29Sep03  Corrected DSS setting for "clockwise" 
1.13     John Good        17Sep03  Change to using wcs->cd values for 
				   pixel scale
1.12     John Good        15Sep03  Updated fits_read_pix() call
1.11     John Good        25Aug03  Added status file processing
1.10     John Good        27May03  Fixed handling of BLANK values in integer images
				   and error in handling of ".fit" extension
1.9      John Good        27Jun03  Added a few comments for clarity
1.8      John Good        09May03  Added special processing for DSS CNPIX 
				   header fields
1.7      John Good        08Apr03  Also remove <CR> from template lines
1.6      John Good        23Mar03  Replaced atof() with strtod() for 
				   command-line parsing
1.5      John Good        22Mar03  Replaced wcsCheck with checkHdr
1.4      John Good        19Mar03  Added check to see if -i or -o coordinates
				   out of range.
1.3      John Good        18Mar03  Made FITS open error message more
				   understandable.
1.2      John Good        13Mar03  Added WCS header check and
				   modified command-line processing
				   to use getopt() library
1.1      Joe Jacob        05Mar03  Added output perimeter check to bbox calc
1.0      John Good        29Jan03  Baseline code

*/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <math.h>
#include <sys/types.h>
#include <time.h>

#include "fitsio.h"
#include "wcs.h"
#include "coord.h"
#include "mNaN.h"

#include "montage.h"

#define MAXSTR  256
#define MAXFILE 256

#ifndef NAN
#define NAN (0.0 / 0.0)
#endif

extern char *optarg;
extern int optind, opterr;

extern int getopt(int argc, char *const *argv, const char *options);

int    debugCheck (char *debugStr);
int    checkHdr   (char *infile, int hdrflag, int hdu);
int    readFits   (char *filename, char *weightfile);
void   fixxy      (double *x, double *y, int *offscl);

static int    hdu;
int    haveWeights;

double offset;

char   input_file   [MAXSTR];
char   output_file  [MAXSTR];
char   area_file    [MAXSTR];
char   weight_file  [MAXSTR];

double computeOverlap(double *, double *, double *, double *, 
		      int, double, double *);
void   printFitsError(int);
void   printError    (char *);
void   UpdateBounds (double oxpix, double oypix,
                     double *oxpixMin, double *oxpixMax,
                     double *oypixMin, double *oypixMax);

int   parseLine     (char *linein);
int   stradd        (char *header, char *card);
int   readTemplate  (char *filename);

int    inRow,  inColumn;
int    outRow, outColumn;

double dtr;

int  debug;


/* Structure used to store relevant */
/* information about a FITS file    */

struct
{
   fitsfile         *fptr;
   long              naxes[2];
   struct WorldCoor *wcs;
   int               sys;
   double            epoch;
   int               clockwise;
}
   input, weight, output, output_area;


double cnpix1, cnpix2;
double crpix1, crpix2;

int isDSS      = 0;
int energyMode = 0;
int fullRegion = 0;

double refArea;


/* Structure contains the geometric       */
/* information for an input pixel         */
/* coordinate in (lat,lon), three-vector, */
/* and output pixel terms                 */

struct Ipos
{
   double lon;
   double lat;

   double x;
   double y;
   double z;

   double oxpix;
   double oypix;

   int    offscl;
};

struct Ipos *topl, *bottoml;
struct Ipos *topr, *bottomr;
struct Ipos *postmp;

double xcorrection;
double ycorrection;

double xcorrectionIn;
double ycorrectionIn;

static time_t currtime, start;


/*************************************************************************/
/*                                                                       */
/*  mProject                                                             */
/*                                                                       */
/*  Montage is a set of general reprojection / coordinate-transform /    */
/*  mosaicking programs.  Any number of input images can be merged into  */
/*  an output FITS file.  The attributes of the input are read from the  */
/*  input files; the attributes of the output are read a combination of  */
/*  the command line and a FITS header template file.                    */
/*                                                                       */
/*  This module, mProject, processes a single input image and            */
/*  projects it onto the output space.  It's output is actually a pair   */
/*  of FITS files, one for the sky flux the other for the fractional     */
/*  pixel coverage. Once this has been done for all input images,        */
/*  mAdd can be used to coadd them into a composite output.              */
/*                                                                       */
/*  Each input pixel is projected onto the output pixel space and the    */
/*  exact area of overlap is computed.  Both the total "flux" and the    */
/*  total sky area of input pixels added to each output pixel is         */
/*  tracked, and the flux is appropriately normalized before writing to  */
/*  the final output file.  This automatically corrects for any multiple */
/*  coverages that may occur.                                            */
/*                                                                       */
/*  The input can come from from arbitrarily disparate sources.  It is   */
/*  assumed that the flux scales in the input images match, but this is  */
/*  not required (leading to some interesting combinations).             */
/*                                                                       */
/*************************************************************************/

int main(int argc, char **argv)
{
   int       i, j, k, l, m, c;
   int       nullcnt, expand;
   long      fpixel[4], nelements;
   double    lon, lat;
   double    oxpix, oypix;
   double    oxpixMin, oypixMin;
   double    oxpixMax, oypixMax;
   int       haveIn, haveOut, haveMinMax, haveTop;
   int       xrefin, yrefin;
   int       xrefout, yrefout;
   int       xpixIndMin, xpixIndMax;
   int       ypixIndMin, ypixIndMax;
   int       imin, imax, jmin, jmax;
   int       istart, ilength;
   int       jstart, jlength;
   double    xpos, ypos;
   int       offscl, use;
   double    *buffer;
   double    *weights;
   double    datamin, datamax;
   double    areamin, areamax;
   double    threshold, fluxScale;
   double    areaRatio;

   double    xcw[]  = {0.5, 1.5, 1.5, 0.5};
   double    ycw[]  = {0.5, 0.5, 1.5, 1.5};

   double    xccw[] = {1.5, 0.5, 0.5, 1.5};
   double    yccw[] = {0.5, 0.5, 1.5, 1.5};

   double    xcorner[4];
   double    ycorner[4];

   double    ilon[4];
   double    ilat[4];

   double    olon[4];
   double    olat[4];

   double    pixel_value  = 0;
   double    weight_value = 1;

   double  **data;
   double  **area;

   double    overlapArea;
   double    drizzle;

   int       status = 0;

   char      template_file[MAXSTR];
   char     *end;



   /*************************************************/
   /* Initialize output FITS basic image parameters */
   /*************************************************/

   int  bitpix = DOUBLE_IMG; 
   long naxis  = 2;  



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

   dtr = atan(1.)/45.;

   time(&currtime);
   start = currtime;

   haveIn    = 0;
   haveOut   = 0;

   xrefin    = 0;
   yrefin    = 0;

   xrefout   = 0;
   yrefout   = 0;

   drizzle   = 1.0;
   threshold = 0.0;
   fluxScale = 1.0;

   debug     = 0;
   opterr    = 0;
   hdu       = 0;
   expand    = 0;

   haveWeights = 0;
   energyMode  = 0;

   fstatus = stdout;

   while ((c = getopt(argc, argv, "z:d:i:o:s:h:w:t:x:Xf")) != EOF) 
   {
      switch (c) 
      {
         case 'z':
            drizzle = strtod(optarg, &end);

	    if(end < optarg + strlen(optarg))
	    {
	       printf("[struct stat=\"ERROR\", msg=\"Drizzle factor string (%s) cannot be interpreted as a real number\"]\n", 
		  optarg);
	       exit(1);
	    }

            break;

         case 'd':
            debug = debugCheck(optarg);
            break;

         case 'i':
	    haveIn = 1;
            sscanf(optarg, "%d %d", &xrefin, &yrefin);
            break;

         case 'o':
	    haveOut = 1;
            sscanf(optarg, "%d %d", &xrefout, &yrefout);
            break;

	 case 'w':
	    haveWeights = 1;
	    strcpy(weight_file, optarg);
	    break;

         case 't':
            threshold = strtod(optarg, &end);

	    if(end < optarg + strlen(optarg))
	    {
	       printf("[struct stat=\"ERROR\", msg=\"Weight threshold string (%s) cannot be interpreted as a real number\"]\n", 
		  optarg);
	       exit(1);
	    }

            break;

         case 'x':
            fluxScale = strtod(optarg, &end);

	    if(end < optarg + strlen(optarg))
	    {
	       printf("[struct stat=\"ERROR\", msg=\"Flux scale string (%s) cannot be interpreted as a real number\"]\n", 
		  optarg);
	       exit(1);
	    }

            break;

	 case 'X':
	    expand = 1;
	    break;

         case 's':
            if((fstatus = fopen(optarg, "w+")) == (FILE *)NULL)
            {
               printf("[struct stat=\"ERROR\", msg=\"Cannot open status file: %s\"]\n",
                  optarg);
               exit(1);
            }
            break;

         case 'h':
            hdu = strtol(optarg, &end, 10);

            if(end < optarg + strlen(optarg) || hdu < 0)
            {
               printf("[struct stat=\"ERROR\", msg=\"HDU value (%s) must be a non-negative integer\"]\n",
                  optarg);
               exit(1);
            }
	    break;

	 case 'e':
	    energyMode = 1;
	    break;

	 case 'f':
	    fullRegion = 1;
	    break;

         default:
	    printf("[struct stat=\"ERROR\", msg=\"Usage: %s [-z factor][-d level][-s statusfile][-h hdu][-x scale][-w weightfile][-t threshold][-X(expand)][-e(nergy-mode)][-f(ull-region)] in.fits out.fits hdr.template\"]\n", argv[0]);
            exit(1);
            break;
      }
   }

   if (argc - optind < 3) 
   {
      printf("[struct stat=\"ERROR\", msg=\"Usage: %s [-z factor][-d level][-s statusfile][-h hdu][-x scale][-w weightfile][-t threshold][-X(expand)][-e(nergy-mode)][-f(ull-region)] in.fits out.fits hdr.template\"]\n", argv[0]);
      exit(1);
   }

   strcpy(input_file,    argv[optind]);
   strcpy(output_file,   argv[optind + 1]);
   strcpy(template_file, argv[optind + 2]);

   checkHdr(input_file, 0, hdu);
   checkHdr(template_file, 1, 0);

   if(strlen(output_file) > 5 &&
      strncmp(output_file+strlen(output_file)-5, ".FITS", 5) == 0)
         output_file[strlen(output_file)-5] = '\0';
      
   else if(strlen(output_file) > 5 &&
      strncmp(output_file+strlen(output_file)-5, ".fits", 5) == 0)
         output_file[strlen(output_file)-5] = '\0';
      
   else if(strlen(output_file) > 4 &&
      strncmp(output_file+strlen(output_file)-4, ".FIT", 4) == 0)
         output_file[strlen(output_file)-4] = '\0';
      
   else if(strlen(output_file) > 4 &&
      strncmp(output_file+strlen(output_file)-4, ".fit", 4) == 0)
         output_file[strlen(output_file)-4] = '\0';
      
   strcpy(area_file,     output_file);
   strcat(output_file,  ".fits");
   strcat(area_file,    "_area.fits");

   if(haveIn && debug == 0)
      debug = 3;

   if(haveOut && debug == 0)
      debug = 3;

   if(debug >= 1)
   {
      printf("\ninput_file    = [%s]\n", input_file);
      printf("output_file   = [%s]\n", output_file);
      printf("area_file     = [%s]\n", area_file);
      printf("template_file = [%s]\n\n", template_file);
      fflush(stdout);
   }


   /************************/
   /* Read the input image */
   /************************/

   if(debug >= 1)
   {
      time(&currtime);
      printf("\nStarting to process pixels (time %.0f)\n\n", 
         (double)(currtime - start));
      fflush(stdout);
   }

   readFits(input_file, weight_file);

   if(debug >= 1)
   {
      printf("input.naxes[0]   =  %ld\n",  input.naxes[0]);
      printf("input.naxes[1]   =  %ld\n",  input.naxes[1]);
      printf("input.sys        =  %d\n",   input.sys);
      printf("input.epoch      =  %-g\n",  input.epoch);
      printf("input.clockwise  =  %d\n",   input.clockwise);
      printf("input proj       =  %s\n\n", input.wcs->ptype);

      fflush(stdout);
   }

   if(haveIn)
   {
      if(debug >= 1)
      {
	 printf("xrefin           =  %d\n",  xrefin);
	 printf("yrefin           =  %d\n\n",  yrefin);

	 fflush(stdout);
      }

      if(xrefin < 0 || xrefin >= input.naxes[0]
      || yrefin < 0 || yrefin >= input.naxes[1])
      {
	 fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Debug input pixel coordinates out of range\"]\n");
	 exit(1);
      }
   }

   offset = 0.;
   if(expand)
   {
      offset = (int)(sqrt(input.naxes[0]*input.naxes[0]
                        + input.naxes[1]*input.naxes[1]));
   }

   if(debug >= 1)
   {
      printf("\nexpand output template by %-g on all sides\n\n", offset);
      fflush(stdout);
   }


   /*************************************************/ 
   /* Process the output header template to get the */ 
   /* image size, coordinate system and projection  */ 
   /*************************************************/ 

   readTemplate(template_file);

   if(output.clockwise)
   {
      for(k=0; k<4; ++k)
      {
         xcorner[k] = xcw[k];
         ycorner[k] = ycw[k];
      }
   }
   else
   {
      for(k=0; k<4; ++k)
      {
         xcorner[k] = xccw[k];
         ycorner[k] = yccw[k];
      }
   }

   if(debug >= 1)
   {
      printf("\noutput.naxes[0]  =  %ld\n", output.naxes[0]);
      printf("output.naxes[1]  =  %ld\n",   output.naxes[1]);
      printf("output.sys       =  %d\n",    output.sys);
      printf("output.epoch     =  %-g\n",   output.epoch);
      printf("output.clockwise =  %d\n",    output.clockwise);
      printf("output proj      =  %s\n",    output.wcs->ptype);

      fflush(stdout);
   }

   if(haveOut)
   {
      if(xrefout < 0 || xrefout >= output.naxes[0]
      || yrefout < 0 || yrefout >= output.naxes[1])
      {
	 fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Debug output pixel coordinates out of range\"]\n");
	 exit(1);
      }
   }



   /********************************************************/
   /* Create the position information structures for the   */
   /* top and bottom corners of the current row of pixels. */
   /* As we step down the image, we move the current       */
   /* "bottom" row to the "top" and compute a new bottom.  */
   /********************************************************/

   topl    = (struct Ipos *)malloc((input.naxes[0]+1) * sizeof(struct Ipos));
   topr    = (struct Ipos *)malloc((input.naxes[0]+1) * sizeof(struct Ipos));
   bottoml = (struct Ipos *)malloc((input.naxes[0]+1) * sizeof(struct Ipos));
   bottomr = (struct Ipos *)malloc((input.naxes[0]+1) * sizeof(struct Ipos));


   /**************************************************/
   /* Create the buffer for one line of input pixels */
   /**************************************************/

   buffer = (double *)malloc(input.naxes[0] * sizeof(double));


   if(haveWeights)
   {
      /*****************************************************/
      /* Create the weight buffer for line of input pixels */
      /*****************************************************/

      weights = (double *)malloc(input.naxes[0] * sizeof(double));
   }


   /************************************************/
   /* Go around the outside of the INPUT image,    */
   /* finding the range of output pixel locations  */
   /************************************************/

   oxpixMin =  100000000;
   oxpixMax = -100000000;
   oypixMin =  100000000;
   oypixMax = -100000000;


   /* Check input left and right */

   for (j=0; j<input.naxes[1]+1; ++j)
   {
      pix2wcs(input.wcs, 0.5, j+0.5, &xpos, &ypos);

      convertCoordinates(input.sys, input.epoch, xpos, ypos,
                         output.sys, output.epoch, &lon, &lat, 0.0);
      
      offscl = 0;

      wcs2pix(output.wcs, lon, lat, &oxpix, &oypix, &offscl);

      fixxy(&oxpix, &oypix, &offscl);

      if(!offscl)
      {
         if(oxpix < oxpixMin) oxpixMin = oxpix;
         if(oxpix > oxpixMax) oxpixMax = oxpix;
         if(oypix < oypixMin) oypixMin = oypix;
         if(oypix > oypixMax) oypixMax = oypix;
      }

      pix2wcs(input.wcs, input.naxes[0]+0.5, j+0.5, &xpos, &ypos);

      convertCoordinates(input.sys, input.epoch, xpos, ypos,
                         output.sys, output.epoch, &lon, &lat, 0.0);
      
      offscl = 0;

      wcs2pix(output.wcs, lon, lat, &oxpix, &oypix, &offscl);

      fixxy(&oxpix, &oypix, &offscl);

      if(!offscl)
      {
         if(oxpix < oxpixMin) oxpixMin = oxpix;
         if(oxpix > oxpixMax) oxpixMax = oxpix;
         if(oypix < oypixMin) oypixMin = oypix;
         if(oypix > oypixMax) oypixMax = oypix;
      }
   }


   /* Check input top and bottom */

   for (i=0; i<input.naxes[0]+1; ++i)
   {
      pix2wcs(input.wcs, i+0.5, 0.5, &xpos, &ypos);

      convertCoordinates(input.sys, input.epoch, xpos, ypos,
                         output.sys, output.epoch, &lon, &lat, 0.0);
      
      offscl = 0;

      wcs2pix(output.wcs, lon, lat, &oxpix, &oypix, &offscl);

      fixxy(&oxpix, &oypix, &offscl);

      if(!offscl)
      {
         if(oxpix < oxpixMin) oxpixMin = oxpix;
         if(oxpix > oxpixMax) oxpixMax = oxpix;
         if(oypix < oypixMin) oypixMin = oypix;
         if(oypix > oypixMax) oypixMax = oypix;
      }

      pix2wcs(input.wcs, i+0.5, input.naxes[1]+0.5, &xpos, &ypos);

      convertCoordinates(input.sys, input.epoch, xpos, ypos,
                         output.sys, output.epoch, &lon, &lat, 0.0);
      
      offscl = 0;

      wcs2pix(output.wcs, lon, lat, &oxpix, &oypix, &offscl);

      fixxy(&oxpix, &oypix, &offscl);

      if(!offscl)
      {
         if(oxpix < oxpixMin) oxpixMin = oxpix;
         if(oxpix > oxpixMax) oxpixMax = oxpix;
         if(oypix < oypixMin) oypixMin = oypix;
         if(oypix > oypixMax) oypixMax = oypix;
      }
   }

   /************************************************/
   /* Go around the outside of the OUTPUT image,   */
   /* finding the range of output pixel locations  */
   /************************************************/

   /* 
    * Check output left and right 
    */

   for (j=0; j<output.naxes[1]+1; j++) {
     oxpix = 0.5;
     oypix = (double)j+0.5;
     UpdateBounds (oxpix, oypix, &oxpixMin, &oxpixMax, &oypixMin, &oypixMax);
     oxpix = (double)output.naxes[0]+0.5;
     UpdateBounds (oxpix, oypix, &oxpixMin, &oxpixMax, &oypixMin, &oypixMax);
   }

   /* 
    * Check output top and bottom 
    */

   for (i=0; i<output.naxes[0]+1; i++) {
     oxpix = (double)i+0.5;
     oypix = 0.5;
     UpdateBounds (oxpix, oypix, &oxpixMin, &oxpixMax, &oypixMin, &oypixMax);
     oypix = (double)output.naxes[1]+0.5;
     UpdateBounds (oxpix, oypix, &oxpixMin, &oxpixMax, &oypixMin, &oypixMax);
   }

   /*
    * ASSERT: Output bounding box now specified by
    *   (oxpixMin, oxpixMax, oypixMin, oypixMax)
    */

   if(fullRegion)
   {
      oxpixMin = 0.5;
      oxpixMax = output.naxes[0]+0.5+1;

      oypixMin = 0.5;
      oypixMax = output.naxes[1]+0.5+1;
   }

   istart = oxpixMin - 1;

   if(istart < 0) 
      istart = 0;
   
   ilength = oxpixMax - oxpixMin + 2;

   if(ilength > output.naxes[0])
      ilength = output.naxes[0];


   jstart = oypixMin - 1;

   if(jstart < 0) 
      jstart = 0;
   
   jlength = oypixMax - oypixMin + 2;

   if(jlength > output.naxes[1])
      jlength = output.naxes[1];

   if(debug >= 2)
   {
      printf("\nOutput range:\n");
      printf(" oxpixMin = %-g\n", oxpixMin);
      printf(" oxpixMax = %-g\n", oxpixMax);
      printf(" oypixMin = %-g\n", oypixMin);
      printf(" oypixMax = %-g\n", oypixMax);
      printf(" istart   = %-d\n", istart);
      printf(" ilength  = %-d\n", ilength);
      printf(" jstart   = %-d\n", jstart);
      printf(" jlength  = %-d\n", jlength);
      fflush(stdout);
   }

   if(oxpixMin > oxpixMax || oypixMin > oypixMax)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"No overlap\"]\n");
      exit(1);
   }
    

   /***********************************************/ 
   /* Allocate memory for the output image pixels */ 
   /***********************************************/ 

   data = (double **)malloc(jlength * sizeof(double *));

   if(data == (void *)NULL)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Not enough memory for output data image array\"]\n");
      exit(1);
   }

   for(j=0; j<jlength; j++)
   {
      data[j] = (double *)malloc(ilength * sizeof(double));

      if(data[j] == (void *)NULL)
      {
         fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Not enough memory for output data image array\"]\n");
         exit(1);
      }
   }

   if(debug >= 1)
   {
      printf("\n%d bytes allocated for image pixels\n", 
         ilength * jlength * sizeof(double));
      fflush(stdout);
   }


   /*********************/
   /* Initialize pixels */
   /*********************/

   for (j=0; j<jlength; ++j)
   {
      for (i=0; i<ilength; ++i)
      {
         data[j][i] = NAN;
      }
   }


   /**********************************************/ 
   /* Allocate memory for the output pixel areas */ 
   /**********************************************/ 

   area = (double **)malloc(jlength * sizeof(double *));

   if(area == (void *)NULL)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Not enough memory for output area image array\"]\n");
      exit(1);
   }

   for(j=0; j<jlength; j++)
   {
      area[j] = (double *)malloc(ilength * sizeof(double));                               

      if(area[j] == (void *)NULL)
      {
         fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Not enough memory for output area image array\"]\n");
         exit(1);
      }
   }

   if(debug >= 1)
   {
      printf("%d bytes allocated for pixel areas\n", 
         ilength * jlength * sizeof(double));
      fflush(stdout);
   }


   /********************/
   /* Initialize areas */
   /********************/

   for (j=0; j<jlength; ++j)
   {
      for (i=0; i<ilength; ++i)
      {
         area[j][i] = 0.;
      }
   }


   /*****************************/
   /* Loop over the input lines */
   /*****************************/

   haveTop   = 0;

   fpixel[0] = 1;
   fpixel[1] = 1;
   fpixel[2] = 1;
   fpixel[3] = 1;

   nelements = input.naxes[0];

   for (j=0; j<input.naxes[1]; ++j)
   {
      if(haveIn && j > yrefin)
         break;

      if(haveIn && j < yrefin)
         continue;

      inRow = j;

      if(debug == 2 && !haveOut)
      {
         printf("\rProcessing input row %5d  ", j);
         fflush(stdout);
      }


      /***********************************/
      /* Read a line from the input file */
      /***********************************/

      if(fits_read_pix(input.fptr, TDOUBLE, fpixel, nelements, &nan,
                       buffer, &nullcnt, &status))
         printFitsError(status);

      if(haveWeights)
      {
         if(fits_read_pix(weight.fptr, TDOUBLE, fpixel, nelements, &nan,
                          weights, &nullcnt, &status))
            printFitsError(status);
      }

      ++fpixel[1];


      /*************************************************************/
      /*                                                           */
      /* Calculate the locations of the bottoms of the pixels      */
      /* (first time the tops, too) otherwise, switch top and      */
      /* bottom before recomputing bottoms.                        */
      /*                                                           */
      /* We use "bottom" and "top" (and "left" and "right")        */
      /* advisedly here.  If CDELT1 and CDELT2 are both negative,  */
      /* these descriptions are accurate.  If either is positive,  */
      /* the corresponding left/right or top/bottom roles are      */
      /* reversed.  What we really mean is increasing j (top to    */
      /* bottom) and increasing i (left to right).  The only place */
      /* it makes any difference is in making sure that we go      */
      /* around the pixel vertices counterclockwise, so that is    */
      /* where we check the CDELTs explicitly.                     */
      /*                                                           */
      /*************************************************************/


      /* 'TOPS' of the pixels */

      if(!haveTop || drizzle != 1.0)
      {
         for (i=0; i<input.naxes[0]+1; ++i)
         {
            /* For the general 'drizzle' algorithm we must       */
            /* project all four of the pixel corners separately. */
            /* However, in the default case where the input      */
            /* pixels are assumed to fill their area, we can     */
            /* save compute time by reusing the values for the   */
            /* shared corners of one pixel for the next.         */


            /* Project the top corners (if corners are shared) */

            if(drizzle == 1.)
            {
               pix2wcs(input.wcs, i+0.5, j+0.5, &xpos, &ypos);

               convertCoordinates(input.sys, input.epoch, xpos, ypos,
                                  output.sys, output.epoch, 
                                  &((topl+i)->lon), &((topl+i)->lat), 0.0);
               
               offscl = 0;

               wcs2pix(output.wcs, (topl+i)->lon,  (topl+i)->lat,
                       &((topl+i)->oxpix), &((topl+i)->oypix), &offscl);

               fixxy(&((topl+i)->oxpix), &((topl+i)->oypix), &offscl);

               (topl+i)->offscl = offscl;

               if(i>0)
               {
                  (topr+i-1)->lon    = (topl+i)->lon;
                  (topr+i-1)->lat    = (topl+i)->lat;
                  (topr+i-1)->oxpix  = (topl+i)->oxpix;
                  (topr+i-1)->oypix  = (topl+i)->oypix;
                  (topr+i-1)->offscl = (topl+i)->offscl;
               }

               if(debug >= 5)
               {
                  printf("    pixel (top)  = (%10.6f,%10.6f) [%d,%d]\n",
                     i+1.5, j+0.5, i, j);

                  printf(" -> input coord  = (%10.6f,%10.6f)\n", xpos, ypos);
                  printf(" -> output coord = (%10.6f,%10.6f)\n", 
                         (topl+i)->lon, (topl+i)->lat);

                  if((topl+i)->offscl)
                  {
                     printf(" -> opix         = (%10.6f,%10.6f) OFF SCALE\n\n",
                         (topl+i)->oxpix, (topl+i)->oypix);
                     fflush(stdout);
                  }
                  else
                  {
                     printf(" -> opix         = (%10.6f,%10.6f)\n\n",
                         (topl+i)->oxpix, (topl+i)->oypix);
                     fflush(stdout);
                  }
               }

               haveTop = 1;
            }


            /* Project the top corners (if corners aren't shared) */

            else
            {
               /* TOP LEFT */

               pix2wcs(input.wcs, i+1-0.5*drizzle, j+1-0.5*drizzle,
                       &xpos, &ypos);

               convertCoordinates(input.sys, input.epoch, xpos, ypos,
                                  output.sys, output.epoch, 
                                  &((topl+i)->lon), &((topl+i)->lat), 0.0);
               
               offscl = 0;

               wcs2pix(output.wcs, (topl+i)->lon,  (topl+i)->lat,
                       &((topl+i)->oxpix), &((topl+i)->oypix), &offscl);

               fixxy(&((topl+i)->oxpix), &((topl+i)->oypix), &offscl);

               (topl+i)->offscl = offscl;

               if(debug >= 5)
               {
                  printf("    pixel TL     = (%10.6f,%10.6f) [%d,%d]\n",  
                          i+1-0.5*drizzle, j+1-0.5*drizzle, i, j);

                  printf(" -> input coord  = (%10.6f,%10.6f)\n", xpos, ypos);
                  printf(" -> output coord = (%10.6f,%10.6f)\n", 
                         (topl+i)->lon, (topl+i)->lat);

                  if((topl+i)->offscl)
                  {
                     printf(" -> opix         = (%10.6f,%10.6f) OFF SCALE\n\n",
                         (topl+i)->oxpix, (topl+i)->oypix);
                     fflush(stdout);
                  }
                  else
                  {
                     printf(" -> opix         = (%10.6f,%10.6f)\n\n",
                         (topl+i)->oxpix, (topl+i)->oypix);
                     fflush(stdout);
                  }
               }


               /* TOP RIGHT */

               pix2wcs(input.wcs, i+1+0.5*drizzle, j+1-0.5*drizzle,
                       &xpos, &ypos);

               convertCoordinates(input.sys, input.epoch, xpos, ypos,
                                  output.sys, output.epoch, 
                                  &((topr+i)->lon), &((topr+i)->lat), 0.0);
               
               offscl = 0;

               wcs2pix(output.wcs, (topr+i)->lon,  (topr+i)->lat,
                       &((topr+i)->oxpix), &((topr+i)->oypix), &offscl);

               fixxy(&((topr+i)->oxpix), &((topr+i)->oypix), &offscl);

               (topr+i)->offscl = offscl;

               if(debug >= 5)
               {
                  printf("    pixel TR     = (%10.6f,%10.6f) [%d,%d]\n", 
                          i+1+0.5*drizzle, j+1-0.5*drizzle, i, j);

                  printf(" -> input coord  = (%10.6f,%10.6f)\n", xpos, ypos);
                  printf(" -> output coord = (%10.6f,%10.6f)\n", 
                         (topr+i)->lon, (topr+i)->lat);

                  if((topr+i)->offscl)
                  {
                     printf(" -> opix         = (%10.6f,%10.6f) OFF SCALE\n\n",
                         (topr+i)->oxpix, (topr+i)->oypix);
                     fflush(stdout);
                  }
                  else
                  {
                     printf(" -> opix         = (%10.6f,%10.6f)\n\n",
                         (topr+i)->oxpix, (topr+i)->oypix);
                     fflush(stdout);
                  }
               }
            }
         }
      }


      /* If the corners are shared, we don't need     */
      /* to recompute when we move down a row, rather */
      /* we move the 'bottom' to the 'top' and        */
      /* recompute the 'bottom'                       */

      else
      {
         postmp  = topl;
         topl    = bottoml;
         bottoml = postmp;

         postmp  = topr;
         topr    = bottomr;
         bottomr = postmp;
      }


      /* 'BOTTOMS' of the pixels */

      for (i=0; i<input.naxes[0]+1; ++i)
      {

         /* Project the bottom corners (if corners are shared) */

         if(drizzle == 1.)
         {
            pix2wcs(input.wcs, i+0.5, j+1.5, &xpos, &ypos);

            convertCoordinates(input.sys, input.epoch, xpos, ypos,
                               output.sys, output.epoch, 
                               &((bottoml+i)->lon), &((bottoml+i)->lat), 0.0);

            offscl = 0;

            wcs2pix(output.wcs, (bottoml+i)->lon,  (bottoml+i)->lat,
                    &((bottoml+i)->oxpix), &((bottoml+i)->oypix), &offscl);

            fixxy(&((bottoml+i)->oxpix), &((bottoml+i)->oypix), &offscl);

            (bottoml+i)->offscl = offscl;

            if(i>0)
            {
               (bottomr+i-1)->lon    = (bottoml+i)->lon;
               (bottomr+i-1)->lat    = (bottoml+i)->lat;
               (bottomr+i-1)->oxpix  = (bottoml+i)->oxpix;
               (bottomr+i-1)->oypix  = (bottoml+i)->oypix;
               (bottomr+i-1)->offscl = (bottoml+i)->offscl;
            }

            if(debug >= 5)
            {
               printf("    pixel (bot)  = (%10.6f,%10.6f) [%d,%d]\n", 
                  i+1.5, j+0.5, i, j-1);

               printf(" -> input coord  = (%10.6f,%10.6f)\n", xpos, ypos);
               printf(" -> output coord = (%10.6f,%10.6f)\n", 
                      (bottoml+i)->lon, (bottoml+i)->lat);

               if((bottoml+i)->offscl)
               {
                  printf(" -> opix         = (%10.6f,%10.6f) OFF SCALE\n\n",
                      (bottoml+i)->oxpix, (bottoml+i)->oypix);
                  fflush(stdout);
               }
               else
               {
                  printf(" -> opix         = (%10.6f,%10.6f)\n\n",
                      (bottoml+i)->oxpix, (bottoml+i)->oypix);
                  fflush(stdout);
               }
            }
         }


         /* Project the bottom corners (if corners aren't shared) */

         else
         {
            /* BOTTOM LEFT */

            pix2wcs(input.wcs, i+1-0.5*drizzle, j+1+0.5*drizzle,
                    &xpos, &ypos);

            convertCoordinates(input.sys, input.epoch, xpos, ypos,
                               output.sys, output.epoch, 
                               &((bottoml+i)->lon), &((bottoml+i)->lat), 0.0);

            offscl = 0;

            wcs2pix(output.wcs, (bottoml+i)->lon,  (bottoml+i)->lat,
                    &((bottoml+i)->oxpix), &((bottoml+i)->oypix), &offscl);

            fixxy(&((bottoml+i)->oxpix), &((bottoml+i)->oypix), &offscl);

            (bottoml+i)->offscl = offscl;

            if(debug >= 5)
            {
               printf("    pixel BL     = (%10.6f,%10.6f) [%d,%d]\n",
                       i+1-0.5*drizzle, j+1+0.5*drizzle, i, j);

               printf(" -> input coord  = (%10.6f,%10.6f)\n", xpos, ypos);
               printf(" -> output coord = (%10.6f,%10.6f)\n", 
                      (bottoml+i)->lon, (bottoml+i)->lat);

               if((bottoml+i)->offscl)
               {
                  printf(" -> opix         = (%10.6f,%10.6f) OFF SCALE\n\n",
                      (bottoml+i)->oxpix, (bottoml+i)->oypix);
                  fflush(stdout);
               }
               else
               {
                  printf(" -> opix         = (%10.6f,%10.6f)\n\n",
                      (bottoml+i)->oxpix, (bottoml+i)->oypix);
                  fflush(stdout);
               }
            }


            /* BOTTOM RIGHT */

            pix2wcs(input.wcs, i+1+0.5*drizzle, j+1+0.5*drizzle,
                    &xpos, &ypos);

            convertCoordinates(input.sys, input.epoch, xpos, ypos,
                               output.sys, output.epoch, 
                               &((bottomr+i)->lon), &((bottomr+i)->lat), 0.0);

            offscl = 0;

            wcs2pix(output.wcs, (bottomr+i)->lon,  (bottomr+i)->lat,
                    &((bottomr+i)->oxpix), &((bottomr+i)->oypix), &offscl);

            fixxy(&((bottomr+i)->oxpix), &((bottomr+i)->oypix), &offscl);

            (bottomr+i)->offscl = offscl;

            if(debug >= 5)
            {
               printf("    pixel BR     = (%10.6f,%10.6f) [%d,%d]\n", 
                       i+1+0.5*drizzle, j+1+0.5*drizzle, i, j);

               printf(" -> input coord  = (%10.6f,%10.6f)\n", xpos, ypos);
               printf(" -> output coord = (%10.6f,%10.6f)\n", 
                      (bottomr+i)->lon, (bottomr+i)->lat);

               if((bottomr+i)->offscl)
               {
                  printf(" -> opix         = (%10.6f,%10.6f) OFF SCALE\n\n",
                      (bottomr+i)->oxpix, (bottomr+i)->oypix);
                  fflush(stdout);
               }
               else
               {
                  printf(" -> opix         = (%10.6f,%10.6f)\n\n",
                      (bottomr+i)->oxpix, (bottomr+i)->oypix);
                  fflush(stdout);
               }
            }
         }
      }

      
      /************************/
      /* For each input pixel */
      /************************/

      for (i=0; i<input.naxes[0]; ++i)
      {
         if(haveIn && (j != yrefin || i != xrefin))
            continue;

         inColumn = i;

         pixel_value = buffer[i];

         if(haveWeights)
	 {
            weight_value = weights[i];

	    if(weight_value < threshold)
	       weight_value = 0.;
	 }

         if(mNaN(pixel_value))
            continue;

	 pixel_value *= fluxScale;

         if(debug >= 3 && !haveOut)
         {
            if(haveWeights)
               printf("\nInput: line %d / pixel %d, value = %-g (weight: %-g)\n\n",
                  j, i, pixel_value, weight_value);
            else
               printf("\nInput: line %d / pixel %d, value = %-g\n\n",
                  j, i, pixel_value);
            fflush(stdout);
         }



         /************************************/
         /* Find the four corners' locations */
         /* in output pixel coordinates      */
         /************************************/

         oxpixMin =  100000000;
         oxpixMax = -100000000;
         oypixMin =  100000000;
         oypixMax = -100000000;

         use = 1;

         if(input.clockwise)
         {
            ilon[0] = (bottomr+i)->lon;
            ilat[0] = (bottomr+i)->lat;

            ilon[1] = (bottoml+i)->lon;
            ilat[1] = (bottoml+i)->lat;

            ilon[2] = (topl+i)->lon;
            ilat[2] = (topl+i)->lat;

            ilon[3] = (topr+i)->lon;
            ilat[3] = (topr+i)->lat;
         }
         else
         {
            ilon[0] = (topr+i)->lon;
            ilat[0] = (topr+i)->lat;

            ilon[1] = (topl+i)->lon;
            ilat[1] = (topl+i)->lat;

            ilon[2] = (bottoml+i)->lon;
            ilat[2] = (bottoml+i)->lat;

            ilon[3] = (bottomr+i)->lon;
            ilat[3] = (bottomr+i)->lat;
         }

         if((topl+i)->oxpix < oxpixMin) oxpixMin = (topl+i)->oxpix;
         if((topl+i)->oxpix > oxpixMax) oxpixMax = (topl+i)->oxpix;
         if((topl+i)->oypix < oypixMin) oypixMin = (topl+i)->oypix;
         if((topl+i)->oypix > oypixMax) oypixMax = (topl+i)->oypix;

         if((topr+i)->oxpix < oxpixMin) oxpixMin = (topr+i)->oxpix;
         if((topr+i)->oxpix > oxpixMax) oxpixMax = (topr+i)->oxpix;
         if((topr+i)->oypix < oypixMin) oypixMin = (topr+i)->oypix;
         if((topr+i)->oypix > oypixMax) oypixMax = (topr+i)->oypix;

         if((bottoml+i)->oxpix < oxpixMin) oxpixMin = (bottoml+i)->oxpix;
         if((bottoml+i)->oxpix > oxpixMax) oxpixMax = (bottoml+i)->oxpix;
         if((bottoml+i)->oypix < oypixMin) oypixMin = (bottoml+i)->oypix;
         if((bottoml+i)->oypix > oypixMax) oypixMax = (bottoml+i)->oypix;

         if((bottomr+i)->oxpix < oxpixMin) oxpixMin = (bottomr+i)->oxpix;
         if((bottomr+i)->oxpix > oxpixMax) oxpixMax = (bottomr+i)->oxpix;
         if((bottomr+i)->oypix < oypixMin) oypixMin = (bottomr+i)->oypix;
         if((bottomr+i)->oypix > oypixMax) oypixMax = (bottomr+i)->oypix;

         if((topl+i)->offscl)    use = 0;
         if((topr+i)->offscl)    use = 0;
         if((bottoml+i)->offscl) use = 0;
         if((bottomr+i)->offscl) use = 0;


         if(use)
         {
            /************************************************/
            /* Determine the range of output pixels we need */
            /* to check against this input pixel            */
            /************************************************/

            xpixIndMin = floor(oxpixMin - 0.5);
            xpixIndMax = floor(oxpixMax - 0.5) + 1;
            ypixIndMin = floor(oypixMin - 0.5);
            ypixIndMax = floor(oypixMax - 0.5) + 1;

            if(debug >= 3 && !haveOut)
            {
               printf("\n");
               printf(" oxpixMin = %20.13e\n", oxpixMin);
               printf(" oxpixMax = %20.13e\n", oxpixMax);
               printf(" oypixMin = %20.13e\n", oypixMin);
               printf(" oypixMax = %20.13e\n", oypixMax);
               printf("\n");
               printf("Output X range: %5d to %5d\n", xpixIndMin, xpixIndMax);
               printf("Output Y range: %5d to %5d\n", ypixIndMin, ypixIndMax);
               printf("\n");
            }


            /***************************************************/
            /* Loop over these, computing the fractional area  */
            /* of overlap (which we use to update the data and */
            /* area arrays)                                    */
            /***************************************************/

            for(m=ypixIndMin; m<ypixIndMax; ++m)
            {
               if(m-jstart < 0 || m-jstart >= jlength)
                  continue;

               outRow = m;

               for(l=xpixIndMin; l<xpixIndMax; ++l)
               {
                  if(l-istart < 0 || l-istart >= ilength)
                     continue;

                  if(haveOut && m == yrefout && l > xrefout)
                     break;

                  if(haveOut && (m != yrefout && l != xrefout))
                     continue;

                  outColumn = l;

                  for(k=0; k<4; ++k)
                  {
                     oxpix = l + xcorner[k];
                     oypix = m + ycorner[k];

                     pix2wcs(output.wcs, oxpix, oypix, &olon[k], &olat[k]);
                  }


                  /* If we've given a reference input/output pixel, print */
                  /* out all the info on it's overlap with corresponding  */
                  /* output/input pixels                                  */

                  if((haveIn  && j == yrefin  && i == xrefin )
                  || (haveOut && m == yrefout && l == xrefout))
                  {  
                     printf("\n\n\n===================================================\n\n");
                     printf("Input pixel:  (%d,%d) [pixel value = %12.5e, weight = %-g]\n", 
                        j, i, pixel_value, weight_value);

                     for(k=0; k<4; ++k)
		     {
                        offscl = 0;

			wcs2pix(output.wcs, ilon[k], ilat[k],
				&oxpix, &oypix, &offscl);

                        fixxy(&oxpix, &oypix, &offscl);

                        printf("   corner %d: (%10.6f,%10.6f) -> [%10.6f,%10.6f]\n", 
                           k+1, ilon[k], ilat[k], oxpix, oypix);
		     }

                     printf("\nOutput pixel: (%d,%d)\n", m, l);

                     for(k=0; k<4; ++k)
		     {
                        offscl = 0;

			wcs2pix(output.wcs, olon[k], olat[k],
				&oxpix, &oypix, &offscl);

                        fixxy(&oxpix, &oypix, &offscl);

                        printf("   corner %d: (%10.6f,%10.6f) -> [%10.6f,%10.6f]\n", 
                           k+1, olon[k], olat[k], oxpix, oypix);
		     }


                     fflush(stdout);
                  }
                    

                  /* Now compute the overlap area */

                  if(weight_value > 0)
                  {
		     if(!haveIn && !haveOut)
			overlapArea = computeOverlap(ilon, ilat, olon, olat, energyMode, refArea, &areaRatio);

		     if((haveIn  && j == yrefin  && i == xrefin )
		     || (haveOut && m == yrefout && l == xrefout))
		     {  
			overlapArea = computeOverlap(ilon, ilat, olon, olat, energyMode, refArea, &areaRatio);

			printf("\n   => overlap area: %12.5e\n\n", overlapArea);
			fflush(stdout);
		     }
		  }


                  /* Update the output data and area arrays */

                  if (mNaN(data[m-jstart][l-istart]))
                     data[m-jstart][l-istart] = pixel_value * overlapArea * areaRatio * weight_value;
                  else
                     data[m-jstart][l-istart] += pixel_value * overlapArea * areaRatio * weight_value;

                  area[m-jstart][l-istart] += overlapArea * weight_value;

                  if(debug >= 3)
                  {
                     if((!haveIn && !haveOut)
                     || (haveIn  && j == yrefin  && i == xrefin )
                     || (haveOut && m == yrefout && l == xrefout))
                     {
                        printf("Compare out(%d,%d) to in(%d,%d) => ", m, l, j, i);
                        printf("overlapArea = %12.5e (%12.5e / %12.5e)\n", overlapArea, 
                        data[m-jstart][l-istart], area[m-jstart][l-istart]);
                        fflush(stdout);
                     }
                  }
               }
            }
         }
      }
   }

   if(debug >= 1)
   {
      time(&currtime);
      printf("\n\nDone processing pixels (%.0f seconds)\n\n",
         (double)(currtime - start));
      fflush(stdout);
   }

   if(fits_close_file(input.fptr, &status))
      printFitsError(status);

   if(haveIn)
      exit(0);


   /*********************************/
   /* Normalize image data based on */
   /* total area added to pixel     */
   /*********************************/

   haveMinMax = 0;

   datamax = 0.,
   datamin = 0.;
   areamin = 0.;
   areamax = 0.;

   imin = 99999;
   imax = 0;

   jmin = 99999;
   jmax = 0;

   for (j=0; j<jlength; ++j)
   {
      for (i=0; i<ilength; ++i)
      {
         if(area[j][i] > 0.)
         {
            data[j][i] 
               = data[j][i] / area[j][i];

            if(!haveMinMax)
            {
               datamin = data[j][i];
               datamax = data[j][i];
               areamin = area[j][i];
               areamax = area[j][i];

               haveMinMax = 1;
            }

            if(data[j][i] < datamin) 
               datamin = data[j][i];

            if(data[j][i] > datamax) 
               datamax = data[j][i];

            if(area[j][i] < areamin) 
               areamin = area[j][i];

            if(area[j][i] > areamax) 
               areamax = area[j][i];

            if(i < imin) imin = i;
            if(i > imax) imax = i;
            if(j < jmin) jmin = j;
            if(j > jmax) jmax = j;
         }
         else
         {
            data[j][i] = nan;
            area[j][i] = 0.;
         }
      }
   }
   
   imin = imin + istart;
   imax = imax + istart;
   jmin = jmin + jstart;
   jmax = jmax + jstart;

   if(debug >= 1)
   {
      printf("Data min = %-g\n", datamin);
      printf("Data max = %-g\n", datamax);
      printf("Area min = %-g\n", areamin);
      printf("Area max = %-g\n\n", areamax);
      printf("i min    = %d\n", imin);
      printf("i max    = %d\n", imax);
      printf("j min    = %d\n", jmin);
      printf("j max    = %d\n", jmax);
   }

   if(jmin > jmax || imin > imax)
      printError("All pixels are blank.");


   /********************************/
   /* Create the output FITS files */
   /********************************/

   remove(output_file);               
   remove(area_file);               

   if(fits_create_file(&output.fptr, output_file, &status)) 
      printFitsError(status);           

   if(fits_create_file(&output_area.fptr, area_file, &status)) 
      printFitsError(status);           


   /*********************************************************/
   /* Create the FITS image.  All the required keywords are */
   /* handled automatically.                                */
   /*********************************************************/

   if (fits_create_img(output.fptr, bitpix, naxis, output.naxes, &status))
      printFitsError(status);          

   if(debug >= 1)
   {
      printf("\nFITS data image created (not yet populated)\n"); 
      fflush(stdout);
   }

   if (fits_create_img(output_area.fptr, bitpix, naxis, output_area.naxes, &status))
      printFitsError(status);          

   if(debug >= 1)
   {
      printf("FITS area image created (not yet populated)\n"); 
      fflush(stdout);
   }


   /****************************************/
   /* Set FITS header from a template file */
   /****************************************/

   if(fits_write_key_template(output.fptr, template_file, &status))
      printFitsError(status);           

   if(debug >= 1)
   {
      printf("Template keywords written to FITS data image\n"); 
      fflush(stdout);
   }

   if(fits_write_key_template(output_area.fptr, template_file, &status))
      printFitsError(status);           

   if(debug >= 1)
   {
      printf("Template keywords written to FITS area image\n\n"); 
      fflush(stdout);
   }


   /***************************/
   /* Modify BITPIX to be -64 */
   /***************************/

   if(fits_update_key_lng(output.fptr, "BITPIX", -64,
                                  (char *)NULL, &status))
      printFitsError(status);           

   if(fits_update_key_lng(output_area.fptr, "BITPIX", -64,
                                  (char *)NULL, &status))
      printFitsError(status);           


   /***************************************************/
   /* Update NAXIS, NAXIS1, NAXIS2, CRPIX1 and CRPIX2 */
   /***************************************************/


   if(fits_update_key_lng(output.fptr, "NAXIS", 2,
                                  (char *)NULL, &status))
      printFitsError(status);           

   if(fits_update_key_lng(output.fptr, "NAXIS1", imax-imin+1,
                                  (char *)NULL, &status))
      printFitsError(status);           

   if(fits_update_key_lng(output.fptr, "NAXIS2", jmax-jmin+1,
                                  (char *)NULL, &status))
      printFitsError(status);           

   if(isDSS)
   {
      if(fits_update_key_dbl(output.fptr, "CNPIX1", cnpix1+imin, -14,
				     (char *)NULL, &status))
	 printFitsError(status);           

      if(fits_update_key_dbl(output.fptr, "CNPIX2", cnpix2+jmin, -14,
				     (char *)NULL, &status))
	 printFitsError(status);           
   }
   else
   {
      if(fits_update_key_dbl(output.fptr, "CRPIX1", crpix1-imin, -14,
				     (char *)NULL, &status))
	 printFitsError(status);           

      if(fits_update_key_dbl(output.fptr, "CRPIX2", crpix2-jmin, -14,
				     (char *)NULL, &status))
	 printFitsError(status);           
   }



   if(fits_update_key_lng(output_area.fptr, "NAXIS", 2,
                                  (char *)NULL, &status))
      printFitsError(status);           

   if(fits_update_key_lng(output_area.fptr, "NAXIS1", imax-imin+1,
                                  (char *)NULL, &status))
      printFitsError(status);           

   if(fits_update_key_lng(output_area.fptr, "NAXIS2", jmax-jmin+1,
                                  (char *)NULL, &status))
      printFitsError(status);           

   if(isDSS)
   {
      if(fits_update_key_dbl(output_area.fptr, "CNPIX1", cnpix1+imin, -14,
				     (char *)NULL, &status))
	 printFitsError(status);           

      if(fits_update_key_dbl(output_area.fptr, "CNPIX2", cnpix2+jmin, -14,
				     (char *)NULL, &status))
	 printFitsError(status);           
   }
   else
   {
      if(fits_update_key_dbl(output_area.fptr, "CRPIX1", crpix1-imin, -14,
				     (char *)NULL, &status))
	 printFitsError(status);           

      if(fits_update_key_dbl(output_area.fptr, "CRPIX2", crpix2-jmin, -14,
				     (char *)NULL, &status))
	 printFitsError(status);           
   }


   if(debug)
   {
      printf("Template keywords BITPIX, CRPIX, and NAXIS updated\n");
      fflush(stdout);
   }


   /************************/
   /* Write the image data */
   /************************/

   fpixel[0] = 1;
   fpixel[1] = 1;
   nelements = imax - imin + 1;

   for(j=jmin; j<=jmax; ++j)
   {
      if (fits_write_pix(output.fptr, TDOUBLE, fpixel, nelements, 
                         (void *)(&data[j-jstart][imin-istart]), &status))
         printFitsError(status);

      ++fpixel[1];
   }

   free(data[0]);

   if(debug >= 1)
   {
      printf("Data written to FITS data image\n"); 
      fflush(stdout);
   }


   /***********************/
   /* Write the area data */
   /***********************/

   fpixel[0] = 1;
   fpixel[1] = 1;
   nelements = imax - imin + 1;

   for(j=jmin; j<=jmax; ++j)
   {
      if (fits_write_pix(output_area.fptr, TDOUBLE, fpixel, nelements,
                         (void *)(&area[j-jstart][imin-istart]), &status))
         printFitsError(status);

      ++fpixel[1];
   }

   free(area[0]);

   if(debug >= 1)
   {
      printf("Data written to FITS area image\n\n"); 
      fflush(stdout);
   }


   /***********************/
   /* Close the FITS file */
   /***********************/

   if(fits_close_file(output.fptr, &status))
      printFitsError(status);           

   if(debug >= 1)
   {
      printf("FITS data image finalized\n"); 
      fflush(stdout);
   }

   if(fits_close_file(output_area.fptr, &status))
      printFitsError(status);           

   if(debug >= 1)
   {
      printf("FITS area image finalized\n\n"); 
      fflush(stdout);
   }

   time(&currtime);
   fprintf(fstatus, "[struct stat=\"OK\", time=%.0f]\n", 
      (double)(currtime - start));
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
   || *x > output.wcs->nxpix+1.
   || *y < 0.
   || *y > output.wcs->nypix+1.)
      *offscl = 1;

   return;
}


/**************************************************/
/*                                                */
/*  Read the output header template file.         */
/*  Specifically extract the image size info.     */
/*  Also, create a single-string version of the   */
/*  header data and use it to initialize the      */
/*  output WCS transform.                         */
/*                                                */
/**************************************************/

int readTemplate(char *filename)
{
   int       j;

   FILE     *fp;

   char      line[MAXSTR];

   char      header[80000];

   int       sys;
   double    epoch;

   double    x, y;
   double    ix, iy;
   double    xpos, ypos;
   int       offscl;


   /********************************************************/
   /* Open the template file, read and parse all the lines */
   /********************************************************/

   fp = fopen(filename, "r");

   if(fp == (FILE *)NULL)
      printError("Template file not found.");

   strcpy(header, "");

   for(j=0; j<1000; ++j)
   {
      if(fgets(line, MAXSTR, fp) == (char *)NULL)
         break;

      if(line[strlen(line)-1] == '\n')
         line[strlen(line)-1]  = '\0';
      
      if(line[strlen(line)-1] == '\r')
	 line[strlen(line)-1]  = '\0';

      if(debug >= 3)
      {
         printf("Template line: [%s]\n", line);
         fflush(stdout);
      }

      parseLine(line);

      stradd(header, line);
   }


   /****************************************/
   /* Initialize the WCS transform library */
   /****************************************/

   if(debug >= 3)
   {
      printf("Output Header to wcsinit():\n%s\n", header);
      fflush(stdout);
   }

   output.wcs = wcsinit(header);

   if(output.wcs == (struct WorldCoor *)NULL)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Output wcsinit() failed.\"]\n");
      exit(1);
   }

   output_area.wcs = output.wcs;


   /* Kludge to get around bug in WCS library:   */
   /* 360 degrees sometimes added to pixel coord */

   ix = (output.wcs->nxpix)/2.;
   iy = (output.wcs->nypix)/2.;

   offscl = 0;

   pix2wcs(output.wcs, ix, iy, &xpos, &ypos);
   wcs2pix(output.wcs, xpos, ypos, &x, &y, &offscl);

   xcorrection = 0;
   ycorrection = 0;

   if(!offscl)
   {
      xcorrection = x-ix;
      ycorrection = y-iy;
   }

   if(debug)
   {
      printf("xcorrection = %.2f\n", xcorrection);
      printf(" ycorrection = %.2f\n\n", ycorrection);
      fflush(stdout);
   }


   /*************************************/
   /*  Set up the coordinate transform  */
   /*************************************/

   if(output.wcs->syswcs == WCS_J2000)
   {
      sys   = EQUJ;
      epoch = 2000.;

      if(output.wcs->equinox == 1950.)
         epoch = 1950;
   }
   else if(output.wcs->syswcs == WCS_B1950)
   {
      sys   = EQUB;
      epoch = 1950.;

      if(output.wcs->equinox == 2000.)
         epoch = 2000;
   }
   else if(output.wcs->syswcs == WCS_GALACTIC)
   {
      sys   = GAL;
      epoch = 2000.;
   }
   else if(output.wcs->syswcs == WCS_ECLIPTIC)
   {
      sys   = ECLJ;
      epoch = 2000.;

      if(output.wcs->equinox == 1950.)
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

   output.sys   = sys;
   output.epoch = epoch;

   output_area.sys   = sys;
   output_area.epoch = epoch;


   /***************************************************/
   /*  Determine whether these pixels are 'clockwise' */
   /* or 'counterclockwise'                           */
   /***************************************************/

   output.clockwise = 0;

   if((output.wcs->xinc < 0 && output.wcs->yinc < 0)
   || (output.wcs->xinc > 0 && output.wcs->yinc > 0)) output.clockwise = 1;

   if(strcmp(output.wcs->c1type, "DEC") == 0
   || output.wcs->c1type[strlen(output.wcs->c1type)-1] == 'T')
      output.clockwise = !output.clockwise;

   if(debug >= 3)
   {
      if(output.clockwise)
         printf("Output pixels are clockwise.\n");
      else
         printf("Output pixels are counterclockwise.\n");
   }

   return 0;
}



/**********************************************/
/*                                            */
/*  Parse header lines from the template,     */
/*  looking for NAXIS1, NAXIS2, CRPIX1 CRPIX2 */
/*                                            */
/**********************************************/

int parseLine(char *linein)
{
   char *keyword;
   char *value;
   char *end;

   int   len;

   char  line[MAXSTR];

   strcpy(line, linein);

   len = strlen(line);

   keyword = line;

   while(*keyword == ' ' && keyword < line+len)
      ++keyword;
   
   end = keyword;

   while(*end != ' ' && *end != '=' && end < line+len)
      ++end;

   value = end;

   while((*value == '=' || *value == ' ' || *value == '\'')
         && value < line+len)
      ++value;
   
   *end = '\0';
   end = value;

   if(*end == '\'')
      ++end;

   while(*end != ' ' && *end != '\'' && end < line+len)
      ++end;
   
   *end = '\0';

   if(debug >= 2)
   {
      printf("keyword [%s] = value [%s]\n", keyword, value);
      fflush(stdout);
   }


   if(strcmp(keyword, "NAXIS1") == 0)
   {
      output.naxes[0]      = atoi(value) + 2 * offset;
      output_area.naxes[0] = atoi(value) + 2 * offset;

      sprintf(linein, "NAXIS1  = %ld", output.naxes[0]);
   }

   if(strcmp(keyword, "NAXIS2") == 0)
   {
      output.naxes[1]      = atoi(value) + 2 * offset;
      output_area.naxes[1] = atoi(value) + 2 * offset;

      sprintf(linein, "NAXIS2  = %ld", output.naxes[1]);
   }

   if(strcmp(keyword, "CRPIX1") == 0)
   {
      crpix1 = atof(value) + offset;

      sprintf(linein, "CRPIX1  = %11.6f", crpix1);
   }

   if(strcmp(keyword, "CRPIX2") == 0)
   {
      crpix2 = atof(value) + offset;

      sprintf(linein, "CRPIX2  = %11.6f", crpix2);
   }

   return 0;
}


/**************************************************/
/*                                                */
/*  Read a FITS file and extract some of the      */
/*  header information.                           */
/*                                                */
/**************************************************/

int readFits(char *filename, char *weightfile)
{
   int       status;

   char     *header;

   char      errstr[MAXSTR];

   int       sys;
   double    epoch;
   
   double    x, y;
   double    ix, iy;
   double    xpos, ypos;
   int       offscl;

   status = 0;

   /*****************************************/
   /* Open the FITS file and get the header */
   /* for WCS setup                         */
   /*****************************************/

   if(fits_open_file(&input.fptr, filename, READONLY, &status))
   {
      sprintf(errstr, "Image file %s missing or invalid FITS", filename);
      printError(errstr);
   }

   if(hdu > 0)
   {
      if(fits_movabs_hdu(input.fptr, hdu+1, NULL, &status))
         printFitsError(status);
   }

   if(fits_get_image_wcs_keys(input.fptr, &header, &status))
      printFitsError(status);


   /************************/
   /* Open the weight file */
   /************************/

   if(haveWeights)
   {
      if(fits_open_file(&weight.fptr, weightfile, READONLY, &status))
      {
         sprintf(errstr, "Weight file %s missing or invalid FITS", weightfile);
         printError(errstr);
      }

      if(hdu > 0)
      {
         if(fits_movabs_hdu(weight.fptr, hdu+1, NULL, &status))
            printFitsError(status);
      }
   }


   /****************************************/
   /* Initialize the WCS transform library */
   /****************************************/

   input.wcs = wcsinit(header);

   if(input.wcs == (struct WorldCoor *)NULL)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Input wcsinit() failed.\"]\n");
      exit(1);
   }

   input.naxes[0] = input.wcs->nxpix;
   input.naxes[1] = input.wcs->nypix;

   refArea = fabs(input.wcs->xinc * input.wcs->yinc) * dtr * dtr;


   /* Kludge to get around bug in WCS library:   */
   /* 360 degrees sometimes added to pixel coord */

   ix = (input.wcs->nxpix)/2.;
   iy = (input.wcs->nypix)/2.;

   offscl = 0;

   pix2wcs(input.wcs, ix, iy, &xpos, &ypos);
   wcs2pix(input.wcs, xpos, ypos, &x, &y, &offscl);

   xcorrectionIn = 0;
   ycorrectionIn = 0;

   if(!offscl)
   {
      xcorrectionIn = x-ix;
      ycorrectionIn = y-iy;
   }

   if(debug)
   {
      printf("xcorrectionIn = %.2f\n", xcorrectionIn);
      printf(" ycorrectionIn = %.2f\n\n", ycorrectionIn);
      fflush(stdout);
   }
   

   /***************************************************/
   /*  Determine whether these pixels are 'clockwise' */
   /* or 'counterclockwise'                           */
   /***************************************************/

   input.clockwise = 0;

   if((input.wcs->xinc < 0 && input.wcs->yinc < 0)
   || (input.wcs->xinc > 0 && input.wcs->yinc > 0)) input.clockwise = 1;

   if(strcmp(input.wcs->c1type, "DEC") == 0
   || input.wcs->c1type[strlen(input.wcs->c1type)-1] == 'T')
      input.clockwise = !input.clockwise;

   if(isDSS)
      input.clockwise = 0;

   if(debug >= 3)
   {
      if(input.clockwise)
         printf("Input pixels are clockwise.\n");
      else
         printf("Input pixels are counterclockwise.\n");
   }


   /*************************************/
   /*  Set up the coordinate transform  */
   /*************************************/

   if(input.wcs->syswcs == WCS_J2000)
   {
      sys   = EQUJ;
      epoch = 2000.;

      if(input.wcs->equinox == 1950.)
         epoch = 1950;
   }
   else if(input.wcs->syswcs == WCS_B1950)
   {
      sys   = EQUB;
      epoch = 1950.;

      if(input.wcs->equinox == 2000.)
         epoch = 2000;
   }
   else if(input.wcs->syswcs == WCS_GALACTIC)
   {
      sys   = GAL;
      epoch = 2000.;
   }
   else if(input.wcs->syswcs == WCS_ECLIPTIC)
   {
      sys   = ECLJ;
      epoch = 2000.;

      if(input.wcs->equinox == 1950.)
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

   input.sys   = sys;
   input.epoch = epoch;

   free(header);

   return 0;
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



/* stradd adds the string "card" to a header line, and */
/* pads the header out to 80 characters.               */

int stradd(char *header, char *card)
{
   int i;

   int hlen = strlen(header);
   int clen = strlen(card);

   for(i=0; i<clen; ++i)
      header[hlen+i] = card[i];

   if(clen < 80)
      for(i=clen; i<80; ++i)
         header[hlen+i] = ' ';
   
   header[hlen+80] = '\0';

   return(strlen(header));
}


/*
 * Given an image coordinate in the output image, (oxpix,oypix), update
 * output bounding box (oxpixMin,oypixMax,oypixMin,oypixMax) to include 
 * (oxpix,oypix) only if that coordinate maps in the bounds of the input 
 * image.
 */

void UpdateBounds (double oxpix, double oypix,
                   double *oxpixMin, double *oxpixMax,
                   double *oypixMin, double *oypixMax)
{
  double xpos, ypos;   /* sky coordinate in output space */
  double lon, lat;     /* sky coordinates in input epoch */
  double ixpix, iypix; /* image coordinates in input space */
  int offscl;          /* out of input image bounds flag */
  /*
   * Convert output image coordinates to sky coordinates
   */
  pix2wcs (output.wcs, oxpix, oypix, &xpos, &ypos);
  convertCoordinates (output.sys, output.epoch, xpos, ypos,
                      input.sys, input.epoch, &lon, &lat, 0.0);
  /* 
   * Convert sky coordinates to input image coordinates
   */
  offscl = 0;

  wcs2pix (input.wcs, lon, lat, &ixpix, &iypix, &offscl);

  ixpix = ixpix - xcorrectionIn;
  iypix = iypix - ycorrectionIn;

  offscl = 0;

  if(ixpix < 0.
  || ixpix > input.wcs->nxpix+1.
  || iypix < 0.
  || iypix > input.wcs->nypix+1.)
     offscl = 1;


  /*
   * Update output bounding box if in bounds
   */
  if (!offscl) {
    if (oxpix < *oxpixMin) *oxpixMin = oxpix;
    if (oxpix > *oxpixMax) *oxpixMax = oxpix;
    if (oypix < *oypixMin) *oypixMin = oypix;
    if (oypix > *oypixMax) *oypixMax = oypix;
  }
}
