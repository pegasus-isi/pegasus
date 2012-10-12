/* Module: mDAGTbls.c

Version  Developer        Date     Change
-------  ---------------  -------  -----------------------
1.5      John Good        01Apr06  Need "raw" image list also
1.4      John Good        25Mar05  Added check for ".gz", etc. extension
1.3      John Good        23Mar05  Added CD matrix processing
1.2      John Good        25Nov03  Added extern optarg references
1.1      John Good        09Oct03  Changed type of variable c
                                   and output formatting
1.0      John Good        02Oct03  Baseline code

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
#include "mtbl.h"

#include "montage.h"

#define MAXSTR  256
#define MAXFILE 256

#define CDELT   1
#define CD      2

extern char *optarg;
extern int optind, opterr;

extern int getopt(int argc, char *const *argv, const char *options);

char   origimg_file  [MAXSTR];
char   template_file [MAXSTR];
char   rawimg_file   [MAXSTR];
char   projimg_file  [MAXSTR];
char   corrimg_file  [MAXSTR];

int    readTemplate  (char *filename);
int    stradd        (char *header, char *card);
void   printError    (char *);
char  *fileName      (char *);

int checkHdr   (char *infile, int hdrflag, int hdu);
int checkWCS   (struct WorldCoor *wcs, int action);


/* Structure used to store relevant */
/* information about a FITS file    */

struct
{
   struct WorldCoor *wcs;
   int               sys;
   int               equinox;
   double            epoch;
   char              ctype1[16];
   char              ctype2[16];
   int               naxis1;
   int               naxis2;
   double            crpix1;
   double            crpix2;
   double            crval1;
   double            crval2;
   double            cdelt1;
   double            cdelt2;
   double            crota2;
   int               cntr;
   char              fname[MAXSTR];
   double            cd11;
   double            cd12;
   double            cd21;
   double            cd22;
}
   input;


struct
{
   struct WorldCoor *wcs;
   int               sys;
   double            epoch;
}
   output;

int    icntr;
int    ictype1;
int    ictype2;
int    iequinox;
int    iepoch;
int    inl;
int    ins;
int    icrval1;
int    icrval2;
int    icrpix1;
int    icrpix2;
int    icdelt1;
int    icdelt2;
int    icrota2;
int    iscale;
int    ifname;
int    icd11;
int    icd12;
int    icd21;
int    icd22;

int  debug;


/*************************************************************************/
/*                                                                       */
/*  mDAGTbls                                                             */
/*                                                                       */
/*  Montage is a set of general reprojection / coordinate-transform /    */
/*  mosaicking programs.  Any number of input images can be merged into  */
/*  an output FITS file.  The attributes of the input are read from the  */
/*  input files; the attributes of the output are read a combination of  */
/*  the command line and a FITS header template file.                    */
/*                                                                       */
/*  This module, mDAGTbls, starts with a metadata table for a set        */
/*  of unprojected images and the template for an output image.  It      */
/*  then generates approximate metadata for the reprojected images       */
/*  (to be used by mBgModel) and the corrected images (to be used by     */
/*  mAdd).                                                               */
/*                                                                       */
/*************************************************************************/

int main(int argc, char **argv)
{
   int    c;
   char   header[32768];
   char   temp  [MAXSTR];
   char   fmt   [MAXSTR];
   char   rfmt  [MAXSTR];
   char   pfmt  [MAXSTR];
   char   cfmt  [MAXSTR];
   char   ofile [MAXSTR];
   char   scale [MAXSTR];
   int    i, j;
   int    namelen, nimages, ntotal, stat;
   double xpos, ypos;
   double lon, lat;
   double oxpix, oypix;
   int    oxpixMin, oypixMin;
   int    oxpixMax, oypixMax;
   int    offscl, mode;
   int    ncols;
   FILE  *fraw;
   FILE  *fproj;
   FILE  *fcorr;


   /***************************************/
   /* Process the command-line parameters */
   /***************************************/

   debug   = 0;
   opterr  = 0;

   fstatus = stdout;

   while ((c = getopt(argc, argv, "ds:")) != EOF) 
   {
      switch (c) 
      {
         case 'd':
            debug = 1;
            break;

         case 's':
            if((fstatus = fopen(optarg, "w+")) == (FILE *)NULL)
            {
               printf("[struct stat=\"ERROR\", msg=\"Cannot open status file: %s\"]\n",
                  optarg);
               exit(1);
            }
            break;

         default:
	    printf("[struct stat=\"ERROR\", msg=\"Usage: %s [-d][-s statusfile] images.tbl hdr.template raw.tbl projected.tbl corrected.tbl\"]\n", argv[0]);
            exit(1);
            break;
      }
   }

   if (argc - optind < 5) 
   {
      printf("[struct stat=\"ERROR\", msg=\"Usage: %s [-d][-s statusfile] images.tbl hdr.template raw.tbl projected.tbl corrected.tbl\"]\n", argv[0]);
      exit(1);
   }

   strcpy(origimg_file,  argv[optind]);
   strcpy(template_file, argv[optind + 1]);
   strcpy(rawimg_file,   argv[optind + 2]);
   strcpy(projimg_file,  argv[optind + 3]);
   strcpy(corrimg_file,  argv[optind + 4]);

   checkHdr(template_file, 1, 0);

   if(debug)
   {
      printf("\norigimg_file   = [%s]\n", origimg_file);
      printf("template_file  = [%s]\n\n", template_file);
      printf("rawimg_file    = [%s]\n", rawimg_file);
      printf("projimg_file   = [%s]\n", projimg_file);
      printf("corrimg_file   = [%s]\n", corrimg_file);
      fflush(stdout);
   }


   /*************************************************/ 
   /* Process the output header template to get the */ 
   /* image size, coordinate system and projection  */ 
   /*************************************************/ 

   readTemplate(template_file);

   if(debug)
   {
      printf("\noutput.sys       =  %d\n",  output.sys);
      printf("output.epoch     =  %-g\n", output.epoch);
      printf("output proj      =  %s\n",  output.wcs->ptype);

      fflush(stdout);
   }


   /*********************************************/ 
   /* Open the image header metadata table file */
   /*********************************************/ 

   ncols = topen(origimg_file);

   if(ncols <= 0)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Invalid image metadata file: %s\"]\n",
         origimg_file);
      exit(1);
   }


   icntr    = tcol("cntr");
   ictype1  = tcol("ctype1");
   ictype2  = tcol("ctype2");
   iequinox = tcol("equinox");
   inl      = tcol("nl");
   ins      = tcol("ns");
   icrval1  = tcol("crval1");
   icrval2  = tcol("crval2");
   icrpix1  = tcol("crpix1");
   icrpix2  = tcol("crpix2");
   icdelt1  = tcol("cdelt1");
   icdelt2  = tcol("cdelt2");
   icrota2  = tcol("crota2");
   iepoch   = tcol("epoch");
   ifname   = tcol("fname");
   iscale   = tcol("scale");

   icd11    = tcol("cd1_1");
   icd12    = tcol("cd1_2");
   icd21    = tcol("cd2_1");
   icd22    = tcol("cd2_2");

   if(ins < 0)
      ins = tcol("naxis1");

   if(inl < 0)
      inl = tcol("naxis2");

   if(ifname < 0)
      ifname = tcol("file");

   if(icd11 >= 0 && icd12 >= 0  && icd21 >= 0  && icd12 >= 0)
      mode = CD;

   else if(icdelt1 >= 0 && icdelt2 >= 0  && icrota2 >= 0)
      mode = CDELT;

   else
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Not enough information to determine coverages (CDELTs or CD matrix)\"]\n");
      exit(1);
   }


   if(icntr   < 0
   || ictype1 < 0
   || ictype2 < 0
   || inl     < 0
   || ins     < 0
   || icrval1 < 0
   || icrval2 < 0
   || icrpix1 < 0
   || icrpix2 < 0
   || ifname  < 0)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Need columns: cntr ctype1 ctype2 nl ns crval1 crval2 crpix1 crpix2 cdelt1 cdelt2 crota2 fname (equinox optional)\"]\n");
      exit(1);
   }


   /******************************************************/
   /* Scan the table to get the true 'file' column width */
   /******************************************************/

   namelen = 0;

   while(1)
   {
      stat = tread();

      if(stat < 0)
	 break;

      strcpy(input.fname, fileName(tval(ifname)));

      if(strlen(input.fname) > namelen)
	 namelen = strlen(input.fname);
   }

   tseek(0);


   /*************************************/
   /* Write headers to the output files */
   /*************************************/

   if((fraw = (FILE *)fopen(rawimg_file, "w+")) == (FILE *)NULL)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Invalid output metadata file: %s\"]\n",
         rawimg_file);
      exit(1);
   }

   fprintf(fraw, "\\datatype=fitshdr\n");

   if(iscale >= 0)
   {
      sprintf(fmt, "|%%5s|%%8s|%%8s|%%6s|%%6s|%%10s|%%10s|%%10s|%%10s|%%11s|%%11s|%%8s|%%7s|%%10s|%%%ds|\n", namelen+2);

      fprintf(fraw, fmt,
	 "cntr",
	 "ctype1",
	 "ctype2",
	 "naxis1",
	 "naxis2",
	 "crval1",
	 "crval2",
	 "crpix1",
	 "crpix2",
	 "cdelt1",
	 "cdelt2",
	 "crota2",
	 "equinox",
	 "scale",
	 "file");

      fprintf(fraw, fmt,
	 "int",
	 "char",
	 "char",
	 "int",
	 "int",
	 "double",
	 "double",
	 "double",
	 "double",
	 "double",
	 "double",
	 "double",
	 "int",
	 "double",
	 "char");
   }
   else
   {
      sprintf(fmt, "|%%5s|%%8s|%%8s|%%6s|%%6s|%%10s|%%10s|%%10s|%%10s|%%11s|%%11s|%%8s|%%7s|%%%ds|\n", namelen+2);


      fprintf(fraw, fmt,
	 "cntr",
	 "ctype1",
	 "ctype2",
	 "naxis1",
	 "naxis2",
	 "crval1",
	 "crval2",
	 "crpix1",
	 "crpix2",
	 "cdelt1",
	 "cdelt2",
	 "crota2",
	 "equinox",
	 "file");

      fprintf(fraw, fmt,
	 "int",
	 "char",
	 "char",
	 "int",
	 "int",
	 "double",
	 "double",
	 "double",
	 "double",
	 "double",
	 "double",
	 "double",
	 "int",
	 "char");
   }

   if((fproj = (FILE *)fopen(projimg_file, "w+")) == (FILE *)NULL)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Invalid output metadata file: %s\"]\n",
         projimg_file);
      exit(1);
   }

   fprintf(fproj, "\\datatype=fitshdr\n");

   sprintf(fmt, "|%%5s|%%8s|%%8s|%%6s|%%6s|%%10s|%%10s|%%10s|%%10s|%%11s|%%11s|%%8s|%%7s|%%%ds|\n", namelen+2);

   fprintf(fproj, fmt,
      "cntr",
      "ctype1",
      "ctype2",
      "naxis1",
      "naxis2",
      "crval1",
      "crval2",
      "crpix1",
      "crpix2",
      "cdelt1",
      "cdelt2",
      "crota2",
      "equinox",
      "file");

   fprintf(fproj, fmt,
      "int",
      "char",
      "char",
      "int",
      "int",
      "double",
      "double",
      "double",
      "double",
      "double",
      "double",
      "double",
      "int",
      "char");


   if((fcorr = (FILE *)fopen(corrimg_file, "w+")) == (FILE *)NULL)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Invalid output metadata file: %s\"]\n",
         corrimg_file);
      exit(1);
   }

   fprintf(fcorr, "\\datatype=fitshdr\n");

   fprintf(fcorr, fmt,
      "cntr",
      "ctype1",
      "ctype2",
      "naxis1",
      "naxis2",
      "crval1",
      "crval2",
      "crpix1",
      "crpix2",
      "cdelt1",
      "cdelt2",
      "crota2",
      "equinox",
      "file");

   fprintf(fcorr, fmt,
      "int",
      "char",
      "char",
      "int",
      "int",
      "double",
      "double",
      "double",
      "double",
      "double",
      "double",
      "double",
      "int",
      "char");


   /************************************************/
   /* Read the metadata and process each image WCS */
   /************************************************/

   namelen = 0;
   nimages = 0;
   ntotal  = 0;

   if(iscale >= 0)
      sprintf(rfmt, " %%5d %%8s %%8s %%6d %%6d %%10.6f %%10.6f %%10.2f %%10.2f %%11.8f %%11.8f %%8.5f %%7.0f %%10s %%%ds\n", namelen+2);
   else
      sprintf(rfmt, " %%5d %%8s %%8s %%6d %%6d %%10.6f %%10.6f %%10.2f %%10.2f %%11.8f %%11.8f %%8.5f %%7.0f %%%ds\n", namelen+2);

   sprintf(pfmt, " %%5d %%8s %%8s %%6d %%6d %%10.6f %%10.6f %%10.2f %%10.2f %%11.8f %%11.8f %%8.5f %%7.0f p%%%ds\n", namelen+2);

   sprintf(cfmt, " %%5d %%8s %%8s %%6d %%6d %%10.6f %%10.6f %%10.2f %%10.2f %%11.8f %%11.8f %%8.5f %%7.0f c%%%ds\n", namelen+2);

   while(1)
   {
      stat = tread();

      if(stat < 0)
	 break;
      
      ++ntotal;

      strcpy(input.ctype1, tval(ictype1));
      strcpy(input.ctype2, tval(ictype2));

      input.cntr      = atoi(tval(icntr));
      input.naxis1    = atoi(tval(ins));
      input.naxis2    = atoi(tval(inl));
      input.crpix1    = atof(tval(icrpix1));
      input.crpix2    = atof(tval(icrpix2));
      input.crval1    = atof(tval(icrval1));
      input.crval2    = atof(tval(icrval2));

      if(mode == CDELT)
      {
	 input.cdelt1    = atof(tval(icdelt1));
	 input.cdelt2    = atof(tval(icdelt2));
	 input.crota2    = atof(tval(icrota2));
      }
      else
      {
	 input.cd11      = atof(tval(icd11));
	 input.cd12      = atof(tval(icd12));
	 input.cd21      = atof(tval(icd21));
	 input.cd22      = atof(tval(icd22));
      }

      input.epoch     = 2000;

      strcpy(header, "");
      sprintf(temp, "SIMPLE  = T"                    ); stradd(header, temp);
      sprintf(temp, "BITPIX  = -64"                  ); stradd(header, temp);
      sprintf(temp, "NAXIS   = 2"                    ); stradd(header, temp);
      sprintf(temp, "NAXIS1  = %d",     input.naxis1 ); stradd(header, temp);
      sprintf(temp, "NAXIS2  = %d",     input.naxis2 ); stradd(header, temp);
      sprintf(temp, "CTYPE1  = '%s'",   input.ctype1 ); stradd(header, temp);
      sprintf(temp, "CTYPE2  = '%s'",   input.ctype2 ); stradd(header, temp);
      sprintf(temp, "CRVAL1  = %11.6f", input.crval1 ); stradd(header, temp);
      sprintf(temp, "CRVAL2  = %11.6f", input.crval2 ); stradd(header, temp);
      sprintf(temp, "CRPIX1  = %11.6f", input.crpix1 ); stradd(header, temp);
      sprintf(temp, "CRPIX2  = %11.6f", input.crpix2 ); stradd(header, temp);

      if(mode == CDELT)
      {
	 sprintf(temp, "CDELT1  = %11.6f", input.cdelt1 ); stradd(header, temp);
	 sprintf(temp, "CDELT2  = %11.6f", input.cdelt2 ); stradd(header, temp);
	 sprintf(temp, "CROTA2  = %11.6f", input.crota2 ); stradd(header, temp);
      }
      else
      {
	 sprintf(temp, "CD1_1   = %11.6f", input.cd11   ); stradd(header, temp);
	 sprintf(temp, "CD1_2   = %11.6f", input.cd12   ); stradd(header, temp);
	 sprintf(temp, "CD2_1   = %11.6f", input.cd21   ); stradd(header, temp);
	 sprintf(temp, "CD2_2   = %11.6f", input.cd22   ); stradd(header, temp);
      }

      sprintf(temp, "EQUINOX = %d",     input.equinox); stradd(header, temp);
      sprintf(temp, "END"                            ); stradd(header, temp);
      
      if(iequinox >= 0)
	 input.equinox = atoi(tval(iequinox));

      strcpy(input.fname, fileName(tval(ifname)));

      if(iscale >= 0)
	 strcpy(scale, tval(iscale));

      if(strlen(input.fname) > namelen)
	 namelen = strlen(input.fname);

      input.wcs = wcsinit(header);

      checkWCS(input.wcs, 0);
			     
      if(input.wcs == (struct WorldCoor *)NULL)
      {
	 fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Bad WCS for image %d\"]\n", 
	    nimages);
	 exit(1);
      }

      if(input.wcs->syswcs == WCS_J2000)
      {
	 input.sys   = EQUJ;
	 input.epoch = 2000.;

	 if(input.wcs->equinox == 1950.)
	    input.epoch = 1950;
      }
      else if(input.wcs->syswcs == WCS_B1950)
      {
	 input.sys   = EQUB;
	 input.epoch = 1950.;

	 if(input.wcs->equinox == 2000.)
	    input.epoch = 2000;
      }
      else if(input.wcs->syswcs == WCS_GALACTIC)
      {
	 input.sys   = GAL;
	 input.epoch = 2000.;
      }
      else if(input.wcs->syswcs == WCS_ECLIPTIC)
      {
	 input.sys   = ECLJ;
	 input.epoch = 2000.;

	 if(input.wcs->equinox == 1950.)
	 {
	    input.sys   = ECLB;
	    input.epoch = 1950.;
	 }
      }
      else       
      {
	 input.sys   = EQUJ;
	 input.epoch = 2000.;
      }


      if(debug)
      {
         printf("DEBUG> input sys: %d -> output sys: %d\n", input.sys, output.sys);
	 fflush(stdout);
      }


      /***************************************************/
      /* Check the boundaries of the input image against */
      /* the output region of interest                   */
      /***************************************************/

      oxpixMin =  100000000;
      oxpixMax = -100000000;
      oypixMin =  100000000;
      oypixMax = -100000000;


      /* Check input left and right */

      for (j=0; j<input.naxis2+1; ++j)
      {
	 pix2wcs(input.wcs, 0.5, j+0.5, &xpos, &ypos);

	 convertCoordinates(input.sys, input.epoch, xpos, ypos,
			    output.sys, output.epoch, &lon, &lat, 0.0);
	 
	 wcs2pix(output.wcs, lon, lat, &oxpix, &oypix, &offscl);

	 if(!offscl)
	 {
	    if(oxpix < oxpixMin) oxpixMin = oxpix;
	    if(oxpix > oxpixMax) oxpixMax = oxpix;
	    if(oypix < oypixMin) oypixMin = oypix;
	    if(oypix > oypixMax) oypixMax = oypix;
	 }

	 pix2wcs(input.wcs, input.naxis1+0.5, j+0.5, &xpos, &ypos);

	 convertCoordinates(input.sys, input.epoch, xpos, ypos,
			    output.sys, output.epoch, &lon, &lat, 0.0);
	 
	 wcs2pix(output.wcs, lon, lat, &oxpix, &oypix, &offscl);

	 if(!offscl)
	 {
	    if(oxpix < oxpixMin) oxpixMin = oxpix;
	    if(oxpix > oxpixMax) oxpixMax = oxpix;
	    if(oypix < oypixMin) oypixMin = oypix;
	    if(oypix > oypixMax) oypixMax = oypix;
	 }
      }


      /* Check input top and bottom */

      for (i=0; i<input.naxis1+1; ++i)
      {
	 pix2wcs(input.wcs, i+0.5, 0.5, &xpos, &ypos);

	 convertCoordinates(input.sys, input.epoch, xpos, ypos,
			    output.sys, output.epoch, &lon, &lat, 0.0);
	 
	 wcs2pix(output.wcs, lon, lat, &oxpix, &oypix, &offscl);

	 if(!offscl)
	 {
	    if(oxpix < oxpixMin) oxpixMin = oxpix;
	    if(oxpix > oxpixMax) oxpixMax = oxpix;
	    if(oypix < oypixMin) oypixMin = oypix;
	    if(oypix > oypixMax) oypixMax = oypix;
	 }

	 pix2wcs(input.wcs, i+0.5, input.naxis2+0.5, &xpos, &ypos);

	 convertCoordinates(input.sys, input.epoch, xpos, ypos,
			    output.sys, output.epoch, &lon, &lat, 0.0);
	 
	 wcs2pix(output.wcs, lon, lat, &oxpix, &oypix, &offscl);

	 if(!offscl)
	 {
	    if(oxpix < oxpixMin) oxpixMin = oxpix;
	    if(oxpix > oxpixMax) oxpixMax = oxpix;
	    if(oypix < oypixMin) oypixMin = oypix;
	    if(oypix > oypixMax) oypixMax = oypix;
	 }
      }

      if(debug)
      {
         printf("DEBUG> Image %d ranges> X: %10d to %10d, Y: %10d to %10d\n", 
	    ntotal, oxpixMin, oxpixMax, oypixMin, oypixMax);
	 fflush(stdout);
      }


      /***************************************************/
      /* Check the boundaries of the region of interest  */
      /* against the input image                         */
      /***************************************************/

      /* Check output left and right */

      for (j=0; j<output.wcs->nypix+1; ++j)
      {
	 pix2wcs(output.wcs, 0.5, j+0.5, &xpos, &ypos);

	 convertCoordinates(output.sys, output.epoch, xpos, ypos,
			     input.sys,  input.epoch, &lon, &lat, 0.0);
	 
	 wcs2pix(input.wcs, lon, lat, &oxpix, &oypix, &offscl);

	 if(!offscl)
	 {
	    if(0.5   < oxpixMin) oxpixMin = 0.5;
	    if(0.5   > oxpixMax) oxpixMax = 0.5;

	    if(j+0.5 < oypixMin) oypixMin = j+0.5;
	    if(j+0.5 > oypixMax) oypixMax = j+0.5;
	 }

	 pix2wcs(output.wcs, output.wcs->nxpix+0.5, j+0.5, &xpos, &ypos);

	 convertCoordinates(output.sys, output.epoch, xpos, ypos,
			     input.sys,  input.epoch, &lon, &lat, 0.0);
	 
	 wcs2pix(input.wcs, lon, lat, &oxpix, &oypix, &offscl);

	 if(!offscl)
	 {
	    if(output.wcs->nxpix+0.5 < oxpixMin) oxpixMin = output.wcs->nxpix+0.5;
	    if(output.wcs->nxpix+0.5 > oxpixMax) oxpixMax = output.wcs->nxpix+0.5;

	    if(j+0.5 < oypixMin) oypixMin = j+0.5;
	    if(j+0.5 > oypixMax) oypixMax = j+0.5;
	 }
      }


      /* Check input top and bottom */

      for (i=0; i<output.wcs->nxpix+1; ++i)
      {
	 pix2wcs(output.wcs, i+0.5, 0.5, &xpos, &ypos);

	 convertCoordinates(output.sys, output.epoch, xpos, ypos,
			     input.sys,  input.epoch, &lon, &lat, 0.0);
	 
	 wcs2pix(input.wcs, lon, lat, &oxpix, &oypix, &offscl);

	 if(!offscl)
	 {
	    if(i+0.5 < oxpixMin) oxpixMin = i+0.5;
	    if(i+0.5 > oxpixMax) oxpixMax = i+0.5;

	    if(0.5   < oypixMin) oypixMin = 0.5  ;
	    if(0.5   > oypixMax) oypixMax = 0.5  ;
	 }

	 pix2wcs(output.wcs, i+0.5, output.wcs->nypix+0.5, &xpos, &ypos);

	 convertCoordinates(output.sys, output.epoch, xpos, ypos,
			     input.sys,  input.epoch, &lon, &lat, 0.0);
	 
	 wcs2pix(input.wcs, lon, lat, &oxpix, &oypix, &offscl);

	 if(!offscl)
	 {
	    if(i+0.5 < oxpixMin) oxpixMin = i+0.5;
	    if(i+0.5 > oxpixMax) oxpixMax = i+0.5;

	    if(output.wcs->nypix+0.5 < oypixMin) oypixMin = output.wcs->nypix+0.5;
	    if(output.wcs->nypix+0.5 > oypixMax) oypixMax = output.wcs->nypix+0.5;
	 }
      }

      if(debug)
      {
         printf("DEBUG> Image %d ranges> X: %10d to %10d, Y: %10d to %10d (after reverse check)\n", 
	    ntotal, oxpixMin, oxpixMax, oypixMin, oypixMax);
	 fflush(stdout);
      }

      if(oxpixMax < oxpixMin) continue;
      if(oypixMax < oypixMin) continue;


      /* Remove any possible compression extension */

      strcpy(ofile, input.fname);

      if(strlen(ofile) > 3 && strcmp(ofile+strlen(ofile)-3, ".gz") == 0)
	 ofile[strlen(ofile)-3] = '\0';

      else if(strlen(ofile) > 2 && strcmp(ofile+strlen(ofile)-2, ".Z") == 0)
	 ofile[strlen(ofile)-2] = '\0';

      else if(strlen(ofile) > 2 && strcmp(ofile+strlen(ofile)-2, ".z") == 0)
	 ofile[strlen(ofile)-2] = '\0';

      else if(strlen(ofile) > 4 && strcmp(ofile+strlen(ofile)-4, ".zip") == 0)
	 ofile[strlen(ofile)-4] = '\0';

      else if(strlen(ofile) > 2 && strcmp(ofile+strlen(ofile)-2, "-z") == 0)
	 ofile[strlen(ofile)-2] = '\0';

      else if(strlen(ofile) > 3 && strcmp(ofile+strlen(ofile)-3, "-gz") == 0)
	 ofile[strlen(ofile)-3] = '\0';


      /* Make sure the extension is ".fits" */

      if(strlen(ofile) > 5 && strcmp(ofile+strlen(ofile)-5, ".fits") == 0)
	 ofile[strlen(ofile)-5] = '\0';

      else if(strlen(ofile) > 5 && strcmp(ofile+strlen(ofile)-5, ".FITS") == 0)
	 ofile[strlen(ofile)-5] = '\0';

      else if(strlen(ofile) > 4 && strcmp(ofile+strlen(ofile)-4, ".fit") == 0)
	 ofile[strlen(ofile)-4] = '\0';

      else if(strlen(ofile) > 4 && strcmp(ofile+strlen(ofile)-4, ".FIT") == 0)
	 ofile[strlen(ofile)-4] = '\0';

      else if(strlen(ofile) > 4 && strcmp(ofile+strlen(ofile)-4, ".fts") == 0)
	 ofile[strlen(ofile)-4] = '\0';

      else if(strlen(ofile) > 4 && strcmp(ofile+strlen(ofile)-4, ".FTS") == 0)
	 ofile[strlen(ofile)-4] = '\0';

      strcat(ofile, ".fits");

      if(iscale >= 0)
      {
	 fprintf(fraw, rfmt,
	   nimages+1,
	   output.wcs->ctype[0],
	   output.wcs->ctype[1],
	   oxpixMax - oxpixMin + 1,
	   oypixMax - oypixMin + 1,
	   output.wcs->crval[0],
	   output.wcs->crval[1],
	   output.wcs->crpix[0] - oxpixMin,
	   output.wcs->crpix[1] - oypixMin,
	   output.wcs->cdelt[0],
	   output.wcs->cdelt[1],
	   output.wcs->rot,
	   output.epoch,
	   scale,
	   ofile);
      }
      else
      {
	 fprintf(fraw, rfmt,
	   nimages+1,
	   output.wcs->ctype[0],
	   output.wcs->ctype[1],
	   oxpixMax - oxpixMin + 1,
	   oypixMax - oypixMin + 1,
	   output.wcs->crval[0],
	   output.wcs->crval[1],
	   output.wcs->crpix[0] - oxpixMin,
	   output.wcs->crpix[1] - oypixMin,
	   output.wcs->cdelt[0],
	   output.wcs->cdelt[1],
	   output.wcs->rot,
	   output.epoch,
	   ofile);
      }

      fprintf(fproj, pfmt,
	nimages+1,
	output.wcs->ctype[0],
	output.wcs->ctype[1],
	oxpixMax - oxpixMin + 1,
	oypixMax - oypixMin + 1,
	output.wcs->crval[0],
	output.wcs->crval[1],
	output.wcs->crpix[0] - oxpixMin,
	output.wcs->crpix[1] - oypixMin,
	output.wcs->cdelt[0],
	output.wcs->cdelt[1],
	output.wcs->rot,
	output.epoch,
	ofile);

      fprintf(fcorr, cfmt,
	nimages+1,
	output.wcs->ctype[0],
	output.wcs->ctype[1],
	oxpixMax - oxpixMin + 1,
	oypixMax - oypixMin + 1,
	output.wcs->crval[0],
	output.wcs->crval[1],
	output.wcs->crpix[0] - oxpixMin,
	output.wcs->crpix[1] - oypixMin,
	output.wcs->cdelt[0],
	output.wcs->cdelt[1],
	output.wcs->rot,
	output.epoch,
	ofile);

      ++nimages;
   }


   fclose(fraw);
   fclose(fproj);
   fclose(fcorr);

   fprintf(fstatus, "[struct stat=\"OK\", count=\"%d\", total=\"%d\"]\n", 
      nimages, ntotal);
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

int readTemplate(char *filename)
{
   int       j;

   FILE     *fp;

   char      line[MAXSTR];

   char      header[80000];

   int       sys;
   double    epoch;



   /**************************************************/
   /* Open the template file and collect information */
   /**************************************************/

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

      if(debug)
      {
         printf("Template line: [%s]\n", line);
         fflush(stdout);
      }

      stradd(header, line);
   }


   /****************************************/
   /* Initialize the WCS transform library */
   /****************************************/

   output.wcs = wcsinit(header);

   if(output.wcs == (struct WorldCoor *)NULL)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Output wcsinit() failed.\"]\n");
      exit(1);
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
