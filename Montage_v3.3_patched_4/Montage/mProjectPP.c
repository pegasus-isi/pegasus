/* Module: mProjectPP.c

Version  Developer        Date     Change
-------  ---------------  -------  -----------------------
3.2      John Good        15Jun05  Added -X option to force reprojection
                                   of whole image, even if part of it is
                                   outside region of interest.
3.1      John Good        31May05  Added option flux rescaling 
				   (e.g. magnitude zero point correction)
3.0      John Good        25Mar05  Added in weight image and HDU functionality
2.1      John Good        13Oct04  Changed format for printing time
2.0      John Good        12Sep04  Added polygon border handling
1.8      John Good        10Sep04  Put in error checks for correct projections
                                   and for input, output in same coordinate system
1.7      John Good        03Aug04  Changed precision on updated keywords
1.6      John Good        29Jul04  Switched from using cd to xinc, yinc
1.5      John Good        28Jul04  Fixed bug in cd -> cdelt computation
1.4      John Good        27Jul04  Added error message for malloc() failure
1.3      John Good        07Jun04  Added -i (alternate input header) option.
                                   Modified FITS key updating precision
1.2      John Good        25Mar04  Bug fix: border was not being accounted for
				   in FITS file reading
1.1      John Good        18Mar04  Bug fix: need special code for TwoPlane
				   projection library initialization.
1.0      John Good        27Jan04  Baseline code (derived from mProject v1.15)

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
#include "two_plane.h"
#include "distort.h"
#include "mNaN.h"

#include "montage.h"

#define MAXSTR  256
#define MAXFILE 256
#define HDRLEN  80000

#define NORMAL_TEMPLATE  0
#define ALTERNATE_INPUT  1
#define ALTERNATE_OUTPUT 2

#define FIXEDBORDER 0
#define POLYBORDER  1

#define MIN(x,y) (x < y ? x : y)
#define MAX(x,y) (x > y ? x : y)

#ifndef NAN
#define NAN (0.0 / 0.0)
#endif

extern char *optarg;
extern int optind, opterr;

extern int getopt(int argc, char *const *argv, const char *options);

int debugCheck (char *debugStr);
int checkHdr   (char *infile, int hdrflag, int hdu);
int readFits   (char *filename, char *weightfile);

int    hdu;
int    haveWeights;

double offset;

char   input_file   [MAXSTR];
char   output_file  [MAXSTR];
char   area_file    [MAXSTR];
char   weight_file  [MAXSTR];
char   altout       [MAXSTR];
char   altin        [MAXSTR];

char  *input_header;
char   template_header  [HDRLEN];
char   alt_input_header [HDRLEN];
char   alt_output_header[HDRLEN];

double computeOverlapPP(double *, double *, 
			double, double, double, double, double);

void   printFitsError(int);
void   printError    (char *);

void UpdateBounds (double oxpix, double oypix,
                   double *oxpixMin, double *oxpixMax,
                   double *oypixMin, double *oypixMax);

int   parseLine     (char *linein, int headerType);
int   stradd        (char *header, char *card);
int   readTemplate  (char *filename, int headerType);
int   BorderSetup   (char *strin);
int   BorderRange   (int jrow, int maxpix, 
                     int *imin, int *imax);

struct TwoPlane two_plane;

int nborder;

typedef struct 
{
   int x, y;
}
   BorderPoint;

BorderPoint polygon[256];



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


double crpix1, crpix2;

double pixelArea;


/* Structure contains the geometric  */
/* information for an input pixel in */
/* output pixel coordinates          */

struct Ipos
{
   double oxpix;
   double oypix;

   int    offscl;
};

struct Ipos *topl, *bottoml;
struct Ipos *topr, *bottomr;
struct Ipos *postmp;


static time_t currtime, start;


/*************************************************************************/
/*                                                                       */
/*  mProjectPP                                                           */
/*                                                                       */
/*  Montage is a set of general reprojection / coordinate-transform /    */
/*  mosaicking programs.  Any number of input images can be merged into  */
/*  an output FITS file.  The attributes of the input are read from the  */
/*  input files; the attributes of the output are read a combination of  */
/*  the command line and a FITS header template file.                    */
/*                                                                       */
/*  This module, mProjectPP, processes a single input image and          */
/*  projects it onto the output space.  It's output is actually a pair   */
/*  of FITS files, one for the sky flux the other for the fractional     */
/*  pixel coverage. Once this has been done for all input images,        */
/*  mAdd can be used to coadd them into a composite output.              */
/*                                                                       */
/*  mProjectPP is a "special case" version of mProject and can be used   */
/*  only where the input and output images have tangent-plane            */
/*  projections (e.g. TAN, SIN) or where they can be approximated by     */
/*  a "pseudo-TAN" header as determined by mTANHdr.                      */
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
   int       i, j, l, m, c;
   int       nullcnt, expand;
   long      fpixel[4], nelements;
   double    ixpix[4], iypix[4];
   double    oxpixe, oypixe;
   double    oxpixt, oypixt;
   double    lon, lat;
   double    oxpixMin, oypixMin;
   double    oxpixMax, oypixMax;
   int       haveMinMax, haveTop;
   int       xpixIndMin, xpixIndMax;
   int       ypixIndMin, ypixIndMax;
   int       imin, imax, jmin, jmax;
   int       ibmin, ibmax, ibfound;
   int       istart, ilength;
   int       jstart, jlength;
   int       offscl, offscl1, use, border, bordertype;
   double    *buffer;
   double    *weights;
   double    datamin, datamax;
   double    areamin, areamax;
   double    threshold, fluxScale;

   double    minX, maxX, minY, maxY;

   double    pixel_value  = 0.;
   double    weight_value = 1.;

   double  **data;
   double  **area;

   double    overlapArea;
   double    drizzle;

   int       status = 0;

   char      template_file[MAXSTR];
   char      msg          [MAXSTR];
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

   time(&currtime);
   start = currtime;

   drizzle    = 1.0;
   threshold  = 0.0;
   fluxScale  = 1.0;
   border     = 0;
   bordertype = FIXEDBORDER;

   debug   = 0;
   opterr  = 0;
   hdu     = 0;
   expand  = 0;

   haveWeights = 0;

   strcpy(altout, "");
   strcpy(altin,  "");

   fstatus = stdout;

   while ((c = getopt(argc, argv, "z:d:s:b:o:i:h:w:t:x:X")) != EOF) 
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

         case 's':
            if((fstatus = fopen(optarg, "w+")) == (FILE *)NULL)
            {
               printf("[struct stat=\"ERROR\", msg=\"Cannot open status file: %s\"]\n",
                  optarg);
               exit(1);
            }
            break;

         case 'i':
            strcpy(altin, optarg);
	    break;

         case 'o':
            strcpy(altout, optarg);
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

         case 'b':
            border = strtol(optarg, &end, 10);

	    if(end < optarg + strlen(optarg))
	    {
	       if(BorderSetup(optarg) <= 3)
	       {
		  printf("[struct stat=\"ERROR\", msg=\"Border value string (%s) cannot be interpreted as an integer or a set of polygon vertices\"]\n", 
		     optarg);
		  exit(1);
	       }
	       else
	       {
		  border = 0;
		  bordertype = POLYBORDER;
	       }
	    }

	    if(border < 0)
	    {
	       printf("[struct stat=\"ERROR\", msg=\"Border value (%d) must be greater than or equal to zero\"]\n", 
		  border);
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

         default:
	    printf("[struct stat=\"ERROR\", msg=\"Usage: %s [-z factor][-d level][-b border][-s statusfile][-o altout.hdr][-i altin.hdr][-h hdu][-x scale][-w weightfile][-t threshold][-X(expand)] in.fits out.fits template.hdr\"]\n", argv[0]);
            exit(1);
            break;
      }
   }

   if (argc - optind < 3) 
   {
      printf("[struct stat=\"ERROR\", msg=\"Usage: %s [-z factor][-d level][-b border][-s statusfile][-o altout.hdr][-i altin.hdr][-h hdu][-x scale][-w weightfile][-t threshold][-X(expand)] in.fits out.fits template.hdr\"]\n", argv[0]);
      exit(1);
   }

   strcpy(input_file,    argv[optind]);
   strcpy(output_file,   argv[optind + 1]);
   strcpy(template_file, argv[optind + 2]);

   checkHdr(input_file, 0, hdu);
   checkHdr(template_file, 1, 0);

   if(altin[0] != '\0')
      checkHdr(altin, 1, 0);

   if(altout[0] != '\0')
      checkHdr(altout, 1, 0);

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

   if(debug >= 1)
   {
      printf("\ninput_file    = [%s]\n", input_file);
      printf("output_file   = [%s]\n", output_file);
      printf("area_file     = [%s]\n", area_file);
      printf("template_file = [%s]\n", template_file);

      if(altin[0] != '\0')
	 printf("altin         = [%s]\n\n", altin);

      if(altout[0] != '\0')
	 printf("altout        = [%s]\n\n", altout);

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

   if(altin[0] != '\0')
      readTemplate(altin, ALTERNATE_INPUT);

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

   if(strcmp(input.wcs->ptype, "TAN") != 0
   && strcmp(input.wcs->ptype, "SIN") != 0
   && strcmp(input.wcs->ptype, "ZEA") != 0
   && strcmp(input.wcs->ptype, "STG") != 0
   && strcmp(input.wcs->ptype, "ARC") != 0)
   {
      sprintf(msg, "Input image projection (%s) must be TAN, SIN, ZEA, STG or ARC for fast reprojection", input.wcs->ptype);
      printError(msg);
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

   readTemplate(template_file, NORMAL_TEMPLATE);

   if(altout[0] != '\0')
      readTemplate(altout, ALTERNATE_OUTPUT);

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

   if(strcmp(output.wcs->ptype, "TAN") != 0
   && strcmp(output.wcs->ptype, "SIN") != 0
   && strcmp(output.wcs->ptype, "ZEA") != 0
   && strcmp(output.wcs->ptype, "STG") != 0
   && strcmp(output.wcs->ptype, "ARC") != 0)
   {
      sprintf(msg, "Output image projection (%s) must be TAN, SIN, ZEA, STG or ARC for fast reprojection", output.wcs->ptype);
      printError(msg);
   }

   if(input.sys != output.sys)
      printError("Input and output must be in the same coordinate system for fast reprojection");



   /***************************************/
   /* Set up the plane-to-plane transform */
   /***************************************/

   if(altin[0] != '\0')
   {
      if(altout[0] != '\0')
	 status = Initialize_TwoPlane_BothDistort(&two_plane, alt_input_header, alt_output_header);
      else 
	 status = Initialize_TwoPlane_BothDistort(&two_plane, alt_input_header, template_header);
   }
   else
   {
      if(altout[0] != '\0')
	 status = Initialize_TwoPlane_BothDistort(&two_plane, input_header, alt_output_header);
      else 
	 status = Initialize_TwoPlane_BothDistort(&two_plane, input_header, template_header);
   }

   if(status)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Could not set up plane-to-plane transform.  Check for compliant headers.\"]\n");
      exit(1);
   }

   if(debug >= 2)
   {
      printf("Initialize_TwoPlane_BothDistort() successful\n");
      fflush(stdout);
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
      plane1_to_plane2_transform(0.5, j+0.5, &oxpixe, &oypixe, &two_plane);
      offscl = (oxpixe < -0.5 || oxpixe > two_plane.naxis1_2 + 1.5 ||
                oypixe < -0.5 || oypixe > two_plane.naxis2_2 + 1.5);
      if(debug >= 3)
      {
         printf("Range: %-g,%-g -> %-g,%-g (%d)\n", 
	    0.5, j+0.5, oxpixe, oypixe, offscl);
	 fflush(stdout);

	 pix2wcs(input.wcs, 0.5, j+0.5, &lon, &lat);
	 wcs2pix(output.wcs, lon, lat, &oxpixt, &oypixt, &offscl1);
	 printf("     -> %-g,%-g ->%-g,%-g (%d)\n", lon, lat, oxpixt, oypixt, offscl1);
      }

      if(!offscl)
      {
         if(oxpixe < oxpixMin) oxpixMin = oxpixe;
         if(oxpixe > oxpixMax) oxpixMax = oxpixe;
         if(oypixe < oypixMin) oypixMin = oypixe;
         if(oypixe > oypixMax) oypixMax = oypixe;
      }

      plane1_to_plane2_transform(input.naxes[0]+0.5, j+0.5,
	                         &oxpixe, &oypixe, &two_plane);
      offscl = (oxpixe < -0.5 || oxpixe > two_plane.naxis1_2 + 1.5 ||
                oypixe < -0.5 || oypixe > two_plane.naxis2_2 + 1.5);

      if(debug >= 3)
      {
         printf("Range: %-g,%-g -> %-g,%-g (%d)\n", 
	    input.naxes[0]+0.5, j+0.5, oxpixe, oypixe, offscl);
	 fflush(stdout);

	 pix2wcs(input.wcs, input.naxes[0]+0.5, j+0.5, &lon, &lat);
	 wcs2pix(output.wcs, lon, lat, &oxpixt, &oypixt, &offscl1);
	 printf("     -> %-g,%-g ->%-g,%-g (%d)\n", lon, lat, oxpixt, oypixt, offscl1);
      }

      if(!offscl)
      {
         if(oxpixe < oxpixMin) oxpixMin = oxpixe;
         if(oxpixe > oxpixMax) oxpixMax = oxpixe;
         if(oypixe < oypixMin) oypixMin = oypixe;
         if(oypixe > oypixMax) oypixMax = oypixe;
      }
   }


   /* Check input top and bottom */

   for (i=0; i<input.naxes[0]+1; ++i)
   {
      plane1_to_plane2_transform(i+0.5, 0.5, &oxpixe, &oypixe, &two_plane);
      offscl = (oxpixe < -0.5 || oxpixe > two_plane.naxis1_2 + 1.5 ||
                oypixe < -0.5 || oypixe > two_plane.naxis2_2 + 1.5);

      if(debug >= 3)
      {
         printf("Range: %-g,%-g -> %-g,%-g (%d)\n", 
	    i+0.5, 0.5, oxpixe, oypixe, offscl);
	 fflush(stdout);

	 pix2wcs(input.wcs, i+0.5, 0.5, &lon, &lat);
	 wcs2pix(output.wcs, lon, lat, &oxpixt, &oypixt, &offscl1);
	 printf("     -> %-g,%-g ->%-g,%-g (%d)\n", lon, lat, oxpixt, oypixt, offscl1);
      }

      if(!offscl)
      {
         if(oxpixe < oxpixMin) oxpixMin = oxpixe;
         if(oxpixe > oxpixMax) oxpixMax = oxpixe;
         if(oypixe < oypixMin) oypixMin = oypixe;
         if(oypixe > oypixMax) oypixMax = oypixe;
      }

      plane1_to_plane2_transform(i+0.5, input.naxes[1]+0.5,
	                         &oxpixe, &oypixe, &two_plane);
      offscl = (oxpixe < -0.5 || oxpixe > two_plane.naxis1_2 + 1.5 ||
                oypixe < -0.5 || oypixe > two_plane.naxis2_2 + 1.5);

      if(debug >= 3)
      {
         printf("Range: %-g,%-g -> %-g,%-g (%d)\n",
	    i+0.5, input.naxes[1]+0.5, oxpixe, oypixe, offscl);
	 fflush(stdout);

	 pix2wcs(input.wcs, i+0.5, input.naxes[1]+0.5, &lon, &lat);
	 wcs2pix(output.wcs, lon, lat, &oxpixt, &oypixt, &offscl1);
	 printf("     -> %-g,%-g ->%-g,%-g (%d)\n", lon, lat, oxpixt, oypixt, offscl1);
      }

      if(!offscl)
      {
         if(oxpixe < oxpixMin) oxpixMin = oxpixe;
         if(oxpixe > oxpixMax) oxpixMax = oxpixe;
         if(oypixe < oypixMin) oypixMin = oypixe;
         if(oypixe > oypixMax) oypixMax = oypixe;
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
     oxpixe = 0.5;
     oypixe = (double)j+0.5;
     UpdateBounds (oxpixe, oypixe, &oxpixMin, &oxpixMax, &oypixMin, &oypixMax);
     oxpixe = (double)output.naxes[0]+0.5;
     UpdateBounds (oxpixe, oypixe, &oxpixMin, &oxpixMax, &oypixMin, &oypixMax);
   }

   /* 
    * Check output top and bottom 
    */

   for (i=0; i<output.naxes[0]+1; i++) {
     oxpixe = (double)i+0.5;
     oypixe = 0.5;
     UpdateBounds (oxpixe, oypixe, &oxpixMin, &oxpixMax, &oypixMin, &oypixMax);
     oypixe = (double)output.naxes[1]+0.5;
     UpdateBounds (oxpixe, oypixe, &oxpixMin, &oxpixMax, &oypixMin, &oypixMax);
   }

   /*
    * ASSERT: Output bounding box now specified by
    *   (oxpixMin, oxpixMax, oypixMin, oypixMax)
    */

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
      for(i=0; i<ilength; ++i)
      {
         area[j][i] = 0.0;
      }
   }

   if(debug >= 1)
   {
      printf("%d bytes allocated for pixel areas\n", 
         ilength * jlength * sizeof(double));
      fflush(stdout);
   }


   /*****************************/
   /* Loop over the input lines */
   /*****************************/

   haveTop   = 0;

   fpixel[0] = 1;
   fpixel[1] = border+1;
   fpixel[2] = 1;
   fpixel[3] = 1;

   nelements = input.naxes[0];

   for (j=border; j<input.naxes[1]-border; ++j)
   {
      ibmin = border;
      ibmax = input.naxes[0]-border;

      if(bordertype == POLYBORDER)
      {
         ibfound = BorderRange(j, input.naxes[0]-1, &ibmin, &ibmax);

	 if(debug >= 2)
	 {
	    printf("\rProcessing input row %5d: border range %d to %d (%d)", 
	       j, ibmin, ibmax, ibfound);
	    fflush(stdout);
	 }

	 if(!ibfound)
         {
            ++fpixel[1];
	    continue;
         }
      }
      else if(debug == 2)
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
               offscl = plane1_to_plane2_transform(i+0.5, j+0.5, 
		                             &((topl+i)->oxpix), 
                                             &((topl+i)->oypix),
                                             &two_plane);

               (topl+i)->offscl = offscl;

               if(i>0)
               {
                  (topr+i-1)->oxpix  = (topl+i)->oxpix;
                  (topr+i-1)->oypix  = (topl+i)->oypix;
                  (topr+i-1)->offscl = (topl+i)->offscl;
               }

               haveTop = 1;
            }


            /* Project the top corners (if corners aren't shared) */

            else
            {
               /* TOP LEFT */

               offscl = plane1_to_plane2_transform(
                                    i+1-0.5*drizzle, j+1-0.5*drizzle,
		                    &((topl+i)->oxpix), 
                                    &((topl+i)->oypix), 
                                    &two_plane);

               (topl+i)->offscl = offscl;



               /* TOP RIGHT */

               offscl = plane1_to_plane2_transform(
                                    i+1+0.5*drizzle, j+1-0.5*drizzle,
		                    &((topr+i)->oxpix), 
                                    &((topr+i)->oypix), 
                                    &two_plane);

               (topr+i)->offscl = offscl;
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
            offscl = plane1_to_plane2_transform(i+0.5, j+1.5,
	                               &((bottoml+i)->oxpix), 
                                       &((bottoml+i)->oypix), 
                                       &two_plane);

            (bottoml+i)->offscl = offscl;

            if(i>0)
            {
               (bottomr+i-1)->oxpix  = (bottoml+i)->oxpix;
               (bottomr+i-1)->oypix  = (bottoml+i)->oypix;
               (bottomr+i-1)->offscl = (bottoml+i)->offscl;
            }
         }


         /* Project the bottom corners (if corners aren't shared) */

         else
         {
            /* BOTTOM LEFT */

            offscl = plane1_to_plane2_transform(
                                i+1-0.5*drizzle, j+1+0.5*drizzle,
	                        &((bottoml+i)->oxpix), 
                                &((bottoml+i)->oypix), 
                                &two_plane);

            (bottoml+i)->offscl = offscl;



            /* BOTTOM RIGHT */

            offscl = plane1_to_plane2_transform(
                                   i+1+0.5*drizzle, j+1+0.5*drizzle,
	                           &((bottomr+i)->oxpix), 
                                   &((bottomr+i)->oypix), 
                                   &two_plane);

            (bottomr+i)->offscl = offscl;
         }
      }

      
      /************************/
      /* For each input pixel */
      /************************/

      for (i=ibmin; i<ibmax; ++i)
      {
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

         if(debug >= 3)
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
            ixpix[0] = (bottomr+i)->oxpix;
            iypix[0] = (bottomr+i)->oypix;

            ixpix[1] = (bottoml+i)->oxpix;
            iypix[1] = (bottoml+i)->oypix;

            ixpix[2] = (topl+i)->oxpix;
            iypix[2] = (topl+i)->oypix;

            ixpix[3] = (topr+i)->oxpix;
            iypix[3] = (topr+i)->oypix;
         }
         else
         {
            ixpix[0] = (topr+i)->oxpix;
            iypix[0] = (topr+i)->oypix;

            ixpix[1] = (topl+i)->oxpix;
            iypix[1] = (topl+i)->oypix;

            ixpix[2] = (bottoml+i)->oxpix;
            iypix[2] = (bottoml+i)->oypix;

            ixpix[3] = (bottomr+i)->oxpix;
            iypix[3] = (bottomr+i)->oypix;
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

            if(debug >= 3)
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

               for(l=xpixIndMin; l<xpixIndMax; ++l)
               {
                  if(l-istart < 0 || l-istart >= ilength)
                     continue;

		  minX = l + 0.5;
		  maxX = l + 1.5;
		  minY = m + 0.5;
		  maxY = m + 1.5;


                  /* Now compute the overlap area */

                  if(weight_value > 0.)
                  {
		     overlapArea = computeOverlapPP(ixpix, iypix, 
						     minX,  maxX,
						     minY,  maxY,
						     pixelArea);
                  }


                  /* Update the output data and area arrays */
                  if (mNaN(data[m-jstart][l-istart]))
                     data[m-jstart][l-istart] = pixel_value * overlapArea * weight_value;
                  else
                     data[m-jstart][l-istart] += pixel_value * overlapArea * weight_value;

                  area[m-jstart][l-istart] += overlapArea * weight_value;

                  if(debug >= 3)
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

   if(debug >= 1)
   {
      time(&currtime);
      printf("\n\nDone processing pixels (%.0f seconds)\n\n",
         (double)(currtime - start));
      fflush(stdout);
   }

   if(fits_close_file(input.fptr, &status))
      printFitsError(status);


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

   if(fits_update_key_dbl(output.fptr, "CRPIX1", crpix1-imin, -14,
				  (char *)NULL, &status))
      printFitsError(status);           

   if(fits_update_key_dbl(output.fptr, "CRPIX2", crpix2-jmin, -14,
				  (char *)NULL, &status))
      printFitsError(status);           



   if(fits_update_key_lng(output_area.fptr, "NAXIS", 2,
                                  (char *)NULL, &status))
      printFitsError(status);           

   if(fits_update_key_lng(output_area.fptr, "NAXIS1", imax-imin+1,
                                  (char *)NULL, &status))
      printFitsError(status);           

   if(fits_update_key_lng(output_area.fptr, "NAXIS2", jmax-jmin+1,
                                  (char *)NULL, &status))
      printFitsError(status);           

   if(fits_update_key_dbl(output_area.fptr, "CRPIX1", crpix1-imin, -14,
				  (char *)NULL, &status))
      printFitsError(status);           

   if(fits_update_key_dbl(output_area.fptr, "CRPIX2", crpix2-jmin, -14,
				  (char *)NULL, &status))
      printFitsError(status);           


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
/*                                                */
/*  Read the output header template file.         */
/*  Specifically extract the image size info.     */
/*  Also, create a single-string version of the   */
/*  header data and use it to initialize the      */
/*  output WCS transform.                         */
/*                                                */
/**************************************************/

int readTemplate(char *filename, int headerType)
{
   int       j;
   FILE     *fp;
   char      line[MAXSTR];
   int       sys;
   double    epoch;
   char      headerStr[HDRLEN];

   double    dtr = atan(1.0) / 45.;

   if(debug >= 3)
   {
      printf("readTemplate() file = [%s]\n", filename);
      fflush(stdout);
   }



   /********************************************************/
   /* Open the template file, read and parse all the lines */
   /********************************************************/

   fp = fopen(filename, "r");

   if(fp == (FILE *)NULL)
   {
      sprintf(line, "Template file [%s] not found.", filename);
      printError(line);
   }

   strcpy(headerStr, "");

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

      parseLine(line, headerType);

      stradd(headerStr, line);
   }


   /****************************************/
   /* Initialize the WCS transform library */
   /****************************************/

   if(headerType == ALTERNATE_INPUT)
   {
      if(debug >= 3)
      {
	 printf("Alternate input header to wcsinit() [input.wcs]:\n%s\n", headerStr);
	 fflush(stdout);
      }

      strcpy(alt_input_header, headerStr);

      input.wcs = wcsinit(headerStr);

      if(input.wcs == (struct WorldCoor *)NULL)
      {
	 fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Output wcsinit() failed.\"]\n");
	 exit(1);
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


      /***************************************************/
      /*  Determine whether these pixels are 'clockwise' */
      /* or 'counterclockwise'                           */
      /***************************************************/

      input.clockwise = 0;

      if((input.wcs->xinc < 0 && input.wcs->yinc < 0)
      || (input.wcs->xinc > 0 && input.wcs->yinc > 0))
	 input.clockwise = 1;

      input.clockwise = !input.clockwise;

      if(debug >= 3)
      {
	 if(input.clockwise)
	    printf("Input pixels are clockwise.\n");
	 else
	    printf("Input pixels are counterclockwise.\n");
      }
   }
   else
   {
      if(debug >= 3)
      {
	 if(headerType == ALTERNATE_OUTPUT)
	 {
	    printf("Alternate output header to wcsinit() [output.wcs]:\n%s\n", headerStr);
	    fflush(stdout);
	 }
         else
	 {
	    printf("Template output header to wcsinit() [output.wcs]:\n%s\n", headerStr);
	    fflush(stdout);
	 }
      }

      if(headerType == ALTERNATE_OUTPUT)
	 strcpy(alt_output_header, headerStr);
      else
	 strcpy(template_header, headerStr);

      output.wcs = wcsinit(headerStr);

      if(output.wcs == (struct WorldCoor *)NULL)
      {
	 fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Output wcsinit() failed.\"]\n");
	 exit(1);
      }

      output_area.wcs = output.wcs;

      pixelArea = fabs(output.wcs->xinc * output.wcs->yinc) * dtr * dtr;


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

      if(debug >= 3)
      {
	 if(output.clockwise)
	    printf("Output pixels are clockwise.\n");
	 else
	    printf("Output pixels are counterclockwise.\n");
      }
   }

   return 0;
}



/**********************************************/
/*                                            */
/*  Parse header lines from the template,     */
/*  looking for NAXIS1, NAXIS2, CRPIX1 CRPIX2 */
/*                                            */
/**********************************************/

int parseLine(char *linein, int headerType)
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


   if(headerType == NORMAL_TEMPLATE 
   || headerType == ALTERNATE_OUTPUT)
   {
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

   char      errstr[MAXSTR];

   int       sys;
   double    epoch;

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

   if(fits_get_image_wcs_keys(input.fptr, &input_header, &status))
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

   if(debug >= 3)
   {
      printf("Input header to wcsinit() [input.wcs]:\n%s\n", input_header);
      fflush(stdout);
   }

   input.wcs = wcsinit(input_header);

   if(input.wcs == (struct WorldCoor *)NULL)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Input wcsinit() failed.\"]\n");
      exit(1);
   }

   input.wcs->nxpix += 2 * offset;
   input.wcs->nypix += 2 * offset;

   input.wcs->xrefpix += offset;
   input.wcs->yrefpix += offset;

   input.naxes[0] = input.wcs->nxpix;
   input.naxes[1] = input.wcs->nypix;


   /***************************************************/
   /*  Determine whether these pixels are 'clockwise' */
   /* or 'counterclockwise'                           */
   /***************************************************/

   input.clockwise = 0;

   if((input.wcs->xinc < 0 && input.wcs->yinc < 0)
   || (input.wcs->xinc > 0 && input.wcs->yinc > 0)) input.clockwise = 1;

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
  double ixpix, iypix; /* image coordinates in input space */
  int offscl;          /* out of input image bounds flag */

  /*
   * Convert output image coordinates to input image coordinates
   */

  plane2_to_plane1_transform(oxpix, oypix, &ixpix, &iypix, &two_plane);
  offscl = (ixpix < -0.5 || ixpix > two_plane.naxis1_1 + 1.5 ||
            iypix < -0.5 || iypix > two_plane.naxis2_1 + 1.5);

  if(debug >= 3)
  {
     printf("Bounds: %-g,%-g -> %-g,%-g (%d)\n", 
       oxpix, oypix, ixpix, iypix, offscl);
     fflush(stdout);
  }

  /*
   * Update output bounding box if in bounds
   */

  if (!offscl) 
  {
    if (oxpix < *oxpixMin) *oxpixMin = oxpix;
    if (oxpix > *oxpixMax) *oxpixMax = oxpix;
    if (oypix < *oypixMin) *oypixMin = oypix;
    if (oypix > *oypixMax) *oypixMax = oypix;
  }
}


/* Boundary polygon handling */

int BorderSetup(char *strin)
{
   int   len;
   char  str[8192];
   char *ptr, *end;

   nborder = 0;

   strcpy(str, strin);

   if(debug >= 3)
   {
      printf("Polygon string: [%s]\n", str);

      fflush(stdout);
   }

   ptr = str;

   len = strlen(str);

   while(*ptr == ' ' && ptr < str+len) 
      ++ptr;

   end = ptr;

   while(1)
   {
      if(ptr >= str+len)
	 break;


      /* find the next x coordinate */

      while(*end != ' ' &&  *end != ',' && end < str+len)
	 ++end;

      *end = '\0';


      polygon[nborder].x = atoi(ptr);


      /* find the corresponding y coordinate */

      ptr = end+1;

      while(*ptr == ' ' && ptr < str+len) 
	 ++ptr;

      if(ptr >= str+len)
	 break;

      end = ptr;

      while(*end != ' ' &&  *end != ',' && end < str+len)
	 ++end;

      *end = '\0';

      polygon[nborder].y = atoi(ptr);

      if(debug)
      {
	 printf("Polygon border  %3d: %6d %6d\n", 
	    nborder,
	    polygon[nborder].x,
	    polygon[nborder].y);

	 fflush(stdout);
      }

      ++nborder;

      ptr = end+1;
   }

   return nborder;
}


int BorderRange(int jrow, int maxpix, 
                int *imin, int *imax)
{
   int          i, found;
   double       xinters, y;
   BorderPoint  p1, p2;
   double       xmin, xmax;

   y = (double)jrow;

   found = 0;
   p1    = polygon[0];
   xmin  = (double)maxpix + 1.;
   xmax  = 0.;

   for (i=1; i<=nborder; ++i) 
   {
      p2 = polygon[i % nborder];

      if(y > MIN((double)p1.y, (double)p2.y))
      {
	 if(y < MAX((double)p1.y, (double)p2.y))
	 {
	    found = 1;

	    xinters =  (double)(y-p1.y)*(double)(p2.x-p1.x)
		      /(double)(p2.y-p1.y)+(double)p1.x;

	    xmin = MIN(xmin, xinters);
	    xmax = MAX(xmax, xinters);
	 }
      }

      p1 = p2;
   }

   if(found)
   {
      *imin = (int)(xmin);

      if(*imin < 0) *imin = 0;

      *imax = (int)(xmax + 0.5);

      if(*imax > maxpix) *imax = maxpix;
   }

   else
   {
      *imin = 0;
      *imax = maxpix;
   }

   return found;
}
