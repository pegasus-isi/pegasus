/* Module: mDiff.c

Version  Developer        Date     Change
-------  ---------------  -------  -----------------------
2.7      John Good        19Sep06  The area check for including a pixel was
                                   too stringent. Changed from 0.05 to 0.333
2.6      John Good        09Jul06  Make the WARNINGs into ERRORs
2.5      John Good        28Dec04  Deleted print lines that referred to
                                   uninitialized ilength, jlength (Ticket 252)
2.4      John Good        13Oct04  Changed format for printing time
2.3      John Good        24Sep04  Change a couple of messages to WARNINGs
2.2      John Good        03Aug04  Changed precision on updated keywords
2.1      John Good        07Jun04  Modified FITS key updating precision
2.0      John Good        16May04  Added "no areas" option
1.7      John Good        25Nov03  Added extern optarg references
1.6      John Good        21Nov03  Corrected bug in differencing;
				   pixels outside the region of 
				   overlap were sometimes getting
				   kept.
1.5      John Good        15Sep03  Updated fits_read_pix() call
1.4      John Good        25Aug03  Added status file processing
1.3      John Good        08Apr03  Also remove <CR> from template lines
1.2      John Good        19Mar03  Check for no overlap
1.1      John Good        14Mar03  Modified command-line processing
				   to use getopt() library.  Added
                                   specific messages for missing flux
                                   and area files.
1.0      John Good        29Jan03  Baseline code

*/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <time.h>
#include <math.h>

#include "montage.h"
#include "fitsio.h"
#include "wcs.h"
#include "mNaN.h"

#define MAXSTR  256
#define MAXFILE 256

char input_file1      [MAXSTR];
char input_file2      [MAXSTR];
char output_file      [MAXSTR];
char output_area_file [MAXSTR];

void printFitsError(int);
void printError    (char *);
void printWarning  (char *);

extern char *optarg;
extern int optind, opterr;

extern int getopt(int argc, char *const *argv, const char *options);

int debugCheck(char *debugStr);
int checkHdr(char *infile, int hdrflag, int hdu);
int readTemplate(char *filename);
int parseLine(char *line);
int readFits(char *fluxfile, char *areafile);

int  debug;
int  noAreas;

struct
{
   fitsfile *fptr;
   long      naxes[2];
   double    crpix1, crpix2;
}
   input, input_area, output, output_area;

static time_t currtime, start;


/*************************************************************************/
/*                                                                       */
/*  mDiff                                                                */
/*                                                                       */
/*  Montage is a set of general reprojection / coordinate-transform /    */
/*  mosaicking programs.  Any number of input images can be merged into  */
/*  an output FITS file.  The attributes of the input are read from the  */
/*  input files; the attributes of the output are read a combination of  */
/*  the command line and a FITS header template file.                    */
/*                                                                       */
/*  This module, mDiff, is used as part of a background                  */
/*  correction mechanism.  Pairwise, images that overlap are differenced */
/*  and the difference images fit with a surface (usually a plane).      */
/*  These planes are analyzed, and a correction determined for each      */
/*  input image (by mBgModel).                                           */
/*                                                                       */
/*************************************************************************/

int main(int argc, char **argv)
{
   int       i, j, c, ifile, status;
   long      fpixel[4], nelements;
   int       nullcnt;
   int       imin, jmin, haveMinMax;
   int       imax, jmax;
   int       istart, iend, ilength;
   int       jstart, jend, jlength;
   double   *buffer, *abuffer;
   double    datamin, datamax;
   double    areamin, areamax;

   int       narea1, narea2;
   double    avearea1, avearea2;

   double    pixel_value;

   double  **data;
   double  **area;

   char      template_file[MAXSTR];
   char      line         [MAXSTR];

   char      infile[2][MAXSTR];
   char      inarea[2][MAXSTR];


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

   debug   = 0;
   noAreas = 0;

   opterr = 0;

   fstatus = stdout;

   while ((c = getopt(argc, argv, "nd:s:")) != EOF) 
   {
      switch (c) 
      {
         case 'n':
            noAreas = 1;
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

         default:
	    printf("[struct stat=\"ERROR\", msg=\"Usage: %s [-d level] [-n(o-areas)] [-s statusfile] in1.fits in2.fits out.fits hdr.template\"]\n", argv[0]);
            exit(1);
            break;
      }
   }

   if (argc - optind < 4) 
   {
      printf("[struct stat=\"ERROR\", msg=\"Usage: %s [-d level] [-n(o-areas)] [-s statusfile] in1.fits in2.fits out.fits hdr.template\"]\n", argv[0]);
      exit(1);
   }

   strcpy(input_file1,   argv[optind]);
   strcpy(input_file2,   argv[optind + 1]);
   strcpy(output_file,   argv[optind + 2]);
   strcpy(template_file, argv[optind + 3]);

   checkHdr(template_file, 1, 0);

   if(strlen(output_file) > 5 &&
      strncmp(output_file+strlen(output_file)-5, ".fits", 5) == 0)
         output_file[strlen(output_file)-5] = '\0';

   strcpy(output_area_file, output_file);
   strcat(output_file,  ".fits");
   strcat(output_area_file, "_area.fits");

   if(debug >= 1)
   {
      printf("input_file1      = [%s]\n", input_file1);
      printf("input_file2      = [%s]\n", input_file2);
      printf("output_file      = [%s]\n", output_file);
      printf("output_area_file = [%s]\n", output_area_file);
      printf("template_file    = [%s]\n", template_file);
      fflush(stdout);
   }


   /****************************/
   /* Get the input file names */
   /****************************/

   if(strlen(input_file1) > 5 
   && strcmp(input_file1+strlen(input_file1)-5, ".fits") == 0)
   {
      strcpy(line, input_file1);

      line[strlen(line)-5] = '\0';

      strcpy(infile[0], line);
      strcat(infile[0],  ".fits");
      strcpy(inarea[0], line);
      strcat(inarea[0], "_area.fits");
   }
   else
   {
      strcpy(infile[0], input_file1);
      strcat(infile[0],  ".fits");
      strcpy(inarea[0], input_file1);
      strcat(inarea[0], "_area.fits");
   }
      

   if(strlen(input_file2) > 5 
   && strcmp(input_file2+strlen(input_file2)-5, ".fits") == 0)
   {
      strcpy(line, input_file2);

      line[strlen(line)-5] = '\0';

      strcpy(infile[1], line);
      strcat(infile[1],  ".fits");
      strcpy(inarea[1], line);
      strcat(inarea[1], "_area.fits");
   }
   else
   {
      strcpy(infile[1], input_file2);
      strcat(infile[1],  ".fits");
      strcpy(inarea[1], input_file2);
      strcat(inarea[1], "_area.fits");
   }
      
   if(debug >= 1)
   {
      printf("\ninput files:\n\n");

      printf("   [%s][%s]\n", infile[0], inarea[0]);
      printf("   [%s][%s]\n", infile[1], inarea[1]);
      
      printf("\n");
   }


   /*************************************************/ 
   /* Process the output header template to get the */ 
   /* image size, coordinate system and projection  */ 
   /*************************************************/ 

   readTemplate(template_file);

   if(debug >= 1)
   {
      printf("output.naxes[0] =  %ld\n", output.naxes[0]);
      printf("output.naxes[1] =  %ld\n", output.naxes[1]);
      printf("output.crpix1   =  %-g\n", output.crpix1);
      printf("output.crpix2   =  %-g\n", output.crpix2);
      fflush(stdout);
   }


   /*****************************************************/
   /* We open the two input files briefly to get enough */
   /* information to determine the region of overlap    */
   /* (for memory allocation purposes)                  */
   /*****************************************************/

   readFits(infile[0], inarea[0]);

   imin = output.crpix1 - input.crpix1;
   jmin = output.crpix2 - input.crpix2;

   imax = imin + input.naxes[0];
   jmax = jmin + input.naxes[1];

   istart = imin;
   iend   = imax;

   jstart = jmin;
   jend   = jmax;

   if(debug >= 1)
   {
      printf("\nFile 1:\n");
      printf("input.naxes[0]       =  %ld\n",   input.naxes[0]);
      printf("input.naxes[1]       =  %ld\n",   input.naxes[1]);
      printf("input.crpix1         =  %-g\n",   input.crpix1);
      printf("input.crpix2         =  %-g\n",   input.crpix2);
      printf("imin                 =  %d\n",    imin);
      printf("imax                 =  %d\n",    imax);
      printf("jmin                 =  %d\n",    jmin);
      printf("jmax                 =  %d\n\n",  jmax);
      printf("istart               =  %d\n",    istart);
      printf("iend                 =  %d\n",    iend);
      printf("jstart               =  %d\n",    jstart);
      printf("jend                 =  %d\n",    jend);

      fflush(stdout);
   }

   status = 0;

   if(fits_close_file(input.fptr, &status))
      printFitsError(status);

   if(!noAreas)
   {
      if(fits_close_file(input_area.fptr, &status))
         printFitsError(status);
   }

   readFits(infile[1], inarea[1]);

   imin = output.crpix1 - input.crpix1;
   jmin = output.crpix2 - input.crpix2;

   imax = imin + input.naxes[0];
   jmax = jmin + input.naxes[1];

   if(debug >= 1)
   {
      printf("\nFile 2:\n");
      printf("input.naxes[0]       =  %ld\n",   input.naxes[0]);
      printf("input.naxes[1]       =  %ld\n",   input.naxes[1]);
      printf("input.crpix1         =  %-g\n",   input.crpix1);
      printf("input.crpix2         =  %-g\n",   input.crpix2);
      printf("imin                 =  %d\n",    imin);
      printf("imax                 =  %d\n",    imax);
      printf("jmin                 =  %d\n",    jmin);
      printf("jmax                 =  %d\n",    jmax);
      printf("istart               =  %d\n\n",  istart);
      printf("iend                 =  %d\n",    iend);
      printf("jstart               =  %d\n",    jstart);
      printf("jend                 =  %d\n",    jend);
      printf("\n");

      fflush(stdout);
   }

   if(imin > istart) istart = imin;
   if(imax < iend  ) iend   = imax;

   if(jmin > jstart) jstart = jmin;
   if(jmax < jend  ) jend   = jmax;

   if(istart < 0) istart = 0;
   if(jstart < 0) jstart = 0;

   if(iend > output.naxes[0]-1) iend = output.naxes[0]-1;
   if(jend > output.naxes[1]-1) jend = output.naxes[1]-1;

   ilength = iend - istart + 1;
   jlength = jend - jstart + 1;

   if(debug >= 1)
   {
      printf("\nComposite:\n");
      printf("input.naxes[0]       =  %ld\n",   input.naxes[0]);
      printf("input.naxes[1]       =  %ld\n",   input.naxes[1]);
      printf("input.crpix1         =  %-g\n",   input.crpix1);
      printf("input.crpix2         =  %-g\n",   input.crpix2);
      printf("istart               =  %d\n",    istart);
      printf("iend                 =  %d\n",    iend);
      printf("jstart               =  %d\n",    jstart);
      printf("jend                 =  %d\n",    jend);
      printf("ilength              =  %d\n",    ilength);
      printf("jlength              =  %d\n",    jlength);
      printf("\n");

      fflush(stdout);
   }

   if(fits_close_file(input.fptr, &status))
      printFitsError(status);

   if(!noAreas)
   {
      if(fits_close_file(input_area.fptr, &status))
         printFitsError(status);
   }

   /*********************/
   /* Check for overlap */
   /*********************/

   if(ilength <= 0 || jlength <= 0)
      printWarning("Images don't overlap");


   /***********************************************/ 
   /* Allocate memory for the output image pixels */ 
   /***********************************************/ 

   data = (double **)malloc(jlength * sizeof(double *));

   for(j=0; j<jlength; ++j)
      data[j] = (double *)malloc(ilength * sizeof(double));

   if(debug >= 1)
   {
      printf("%d bytes allocated for image pixels\n", 
	 ilength * jlength * sizeof(double));
      fflush(stdout);
   }


   /****************************/
   /* Initialize data to zeros */
   /****************************/

   for (j=0; j<jlength; ++j)
   {
      for (i=0; i<ilength; ++i)
      {
	 data[j][i] = 0.;
      }
   }


   /**********************************************/ 
   /* Allocate memory for the output pixel areas */ 
   /**********************************************/ 

   area = (double **)malloc(jlength * sizeof(double *));

   for(j=0; j<jlength; ++j)
      area[j] = (double *)malloc(ilength * sizeof(double));

   if(debug >= 1)
   {
      printf("%d bytes allocated for pixel areas\n", 
	 ilength * jlength * sizeof(double));
      fflush(stdout);
   }


   /****************************/
   /* Initialize area to zeros */
   /****************************/

   for (j=0; j<jlength; ++j)
   {
      for (i=0; i<ilength; ++i)
      {
	 area[j][i] = 0.;
      }
   }


   /***************************/
   /* For the two input files */
   /***************************/

   time(&currtime);
   start = currtime;

   avearea1 = 0.;
   avearea2 = 0.;
   narea1   = 0.;
   narea2   = 0.;

   for(ifile=0; ifile<2; ++ifile)
   {
      /************************/
      /* Read the input image */
      /************************/

      readFits(infile[ifile], inarea[ifile]);

      imin = output.crpix1 - input.crpix1;
      jmin = output.crpix2 - input.crpix2;

      if(debug >= 1)
      {
	 printf("\nflux file            =  %s\n",  infile[ifile]);
	 printf("input.naxes[0]       =  %ld\n",   input.naxes[0]);
	 printf("input.naxes[1]       =  %ld\n",   input.naxes[1]);
	 printf("input.crpix1         =  %-g\n",   input.crpix1);
	 printf("input.crpix2         =  %-g\n",   input.crpix2);
	 printf("\narea file            =  %s\n",  inarea[ifile]);
	 printf("input_area.naxes[0]  =  %ld\n",   input.naxes[0]);
	 printf("input_area.naxes[1]  =  %ld\n",   input.naxes[1]);
	 printf("input_area.crpix1    =  %-g\n",   input.crpix1);
	 printf("input_area.crpix2    =  %-g\n",   input.crpix2);
	 printf("\nimin                 =  %d\n",  imin);
	 printf("jmin                 =  %d\n\n",  jmin);

	 fflush(stdout);
      }


      /**********************************************************/
      /* Create the output array by processing the input pixels */
      /**********************************************************/

      buffer  = (double *)malloc(input.naxes[0] * sizeof(double));
      abuffer = (double *)malloc(input.naxes[0] * sizeof(double));

      fpixel[0] = 1;
      fpixel[1] = 1;
      fpixel[2] = 1;
      fpixel[3] = 1;

      nelements = input.naxes[0];

      status = 0;


      /*****************************/
      /* Loop over the input lines */
      /*****************************/

      for (j=0; j<input.naxes[1]; ++j)
      {
	 if(debug >= 2)
	 {
	    printf("\rProcessing input row %5d  ", j);
	    fflush(stdout);
	 }


	 /***********************************/
	 /* Read a line from the input file */
	 /***********************************/

	 if(fits_read_pix(input.fptr, TDOUBLE, fpixel, nelements, NULL,
			  buffer, &nullcnt, &status))
	    printFitsError(status);
	 
         
	 if(noAreas)
	 {
	    for(i=0; i<input.naxes[0]; ++i)
	       abuffer[i] = 1.;
	 }
	 else
	 {
	 if(fits_read_pix(input_area.fptr, TDOUBLE, fpixel, nelements, NULL,
			  abuffer, &nullcnt, &status))
	    printFitsError(status);
	 }
	 
	 ++fpixel[1];


	 /************************/
	 /* For each input pixel */
	 /************************/

	 for (i=0; i<input.naxes[0]; ++i)
	 {
	    pixel_value = buffer[i] * abuffer[i];

	    if(debug >= 4)
	    {
	       printf("input: line %5d / pixel %5d, value = %10.2e (%10.2e) [array: %5d %5d]\n",
		  j, i, buffer[i], abuffer[i], j+jmin-jstart, i+imin-istart);
	       fflush(stdout);
	    }

	    if(i+imin < istart) continue;
	    if(j+jmin < jstart) continue;

	    if(i+imin >= iend) continue;
	    if(j+jmin >= jend) continue;

	    if(debug >= 3)
	    {
	       printf("keep: line %5d / pixel %5d, value = %10.2e (%10.2e) [array: %5d %5d]\n",
		  j, i, buffer[i], abuffer[i], j+jmin-jstart, i+imin-istart);
	       fflush(stdout);
	    }

	    if(ifile == 0)
	    {
	       if(mNaN(buffer[i])
	       || abuffer[i] <= 0.)
	       {
		  if(debug >= 5)
		  {
		     printf("First file. Setting data to NaN and area to zero.\n");
		     fflush(stdout);
		  }

		  data[j+jmin-jstart][i+imin-istart] = nan;
		  area[j+jmin-jstart][i+imin-istart] = 0.;

		  if(debug >= 5)
		  {
		     printf("done.\n");
		     fflush(stdout);
		  }

		  continue;
	       }
               else
	       {
		  if(debug >= 5)
		  {
		     printf("First file. Setting data to pixel value.\n");
		     fflush(stdout);
		  }

		  data[j+jmin-jstart][i+imin-istart] = pixel_value;
		  area[j+jmin-jstart][i+imin-istart] = abuffer[i];

		  ++narea1;
		  avearea1 += abuffer[i];

		  if(debug >= 5)
		  {
		     printf("done.\n");
		     fflush(stdout);
		  }

	       }
	    }
	    else
	    {
	      if(mNaN(buffer[i])
	       || abuffer[i] <= 0.
               || data[j+jmin-jstart][i+imin-istart] == nan
               || area[j+jmin-jstart][i+imin-istart] == 0.)
	       {
		  if(debug >= 5)
		  {
		     printf("Second file. One or the other value is NaN (or zero area).\n");
		     fflush(stdout);
		  }

		  data[j+jmin-jstart][i+imin-istart] = nan;
		  area[j+jmin-jstart][i+imin-istart] = 0.;

		  continue;
	       }
	       else
	       {
		  if(debug >= 5)
		  {
		     printf("Second file. Subtracting pixel value.\n");
		     fflush(stdout);
		  }

		  data[j+jmin-jstart][i+imin-istart] -= pixel_value;
		  area[j+jmin-jstart][i+imin-istart] += abuffer[i];

		  ++narea2;
		  avearea2 += abuffer[i];

		  if(debug >= 5)
		  {
		     printf("done.\n");
		     fflush(stdout);
		  }
	       }
	    }
	 }
      }

      free(buffer);
      free(abuffer);

      if(fits_close_file(input.fptr, &status))
	 printFitsError(status);

      if(!noAreas)
      {
	 if(fits_close_file(input_area.fptr, &status))
	    printFitsError(status);
      }
   }

   if(debug >= 1)
   {
      time(&currtime);
      printf("\nDone reading data (%.0f seconds)\n", 
	 (double)(currtime - start));
      fflush(stdout);
   }


   /************************************/
   /* Anly keep those pixels that were */
   /* covered twice reasonably fully   */
   /************************************/

   avearea1 = avearea1/narea1;
   avearea2 = avearea2/narea2;

   areamax = avearea1 + avearea2;

   if(debug >= 2)
   {
      printf("\npixel areas: %-g + %-g = %-g\n\n", avearea1, avearea2, areamax);
      fflush(stdout);
   }

   for (j=0; j<jlength; ++j)
   {
      for (i=0; i<ilength; ++i)
      {
         if(mNaN(area[j][i]) || area[j][i] == 0.)
	 {
            data[j][i] = 0.;
            area[j][i] = 0.;
	 }
         else
	 {
	    if(fabs(area[j][i] - areamax)/areamax > 0.333)
	    {
	       data[j][i] = 0.;
	       area[j][i] = 0.;
	    }
	 }
      }
   }


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
            data[j][i] = 2. * data[j][i] / area[j][i];

	    if(!haveMinMax)
	    {
	       datamin = data[j][i];
	       datamax = data[j][i];
	       areamin = area[j][i];
	       areamax = area[j][i];

	       haveMinMax = 1;
	    }

	    if(data[j][i] < datamin) datamin = data[j][i];
	    if(data[j][i] > datamax) datamax = data[j][i];
	    if(area[j][i] < areamin) areamin = area[j][i];
	    if(area[j][i] > areamax) areamax = area[j][i];

	    if(j < jmin) jmin = j;
	    if(j > jmax) jmax = j;
	    if(i < imin) imin = i;
	    if(i > imax) imax = i;
	 }
         else
	 {
            data[j][i] = nan;
            area[j][i] = 0.;
	 }
      }
   }

   imin += istart;
   jmin += jstart;

   imax += istart;
   jmax += jstart;

   if(debug >= 1)
   {
      printf("Data min = %-g\n", datamin);
      printf("Data max = %-g\n", datamax);
      printf("Area min = %-g\n", areamin);
      printf("Area max = %-g\n\n", areamax);
      printf("j min    = %d\n", jmin);
      printf("j max    = %d\n", jmax);
      printf("i min    = %d\n", imin);
      printf("i max    = %d\n", imax);
   }

   if(jmin > jmax || imin > imax)
      printWarning("All pixels are blank.");


   /********************************/
   /* Create the output FITS files */
   /********************************/

   remove(output_file);               
   remove(output_area_file);               

   if(fits_create_file(&output.fptr, output_file, &status)) 
      printFitsError(status);           

   if(fits_create_file(&output_area.fptr, output_area_file, &status)) 
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

   if(fits_update_key_dbl(output.fptr, "CRPIX1", output.crpix1-imin, -14,
				  (char *)NULL, &status))
      printFitsError(status);           

   if(fits_update_key_dbl(output.fptr, "CRPIX2", output.crpix2-jmin, -14,
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

   if(fits_update_key_dbl(output_area.fptr, "CRPIX1", output.crpix1-imin, -14,
				  (char *)NULL, &status))
      printFitsError(status);           

   if(fits_update_key_dbl(output_area.fptr, "CRPIX2", output.crpix2-jmin, -14,
				  (char *)NULL, &status))
      printFitsError(status);           

   if(debug >= 1)
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

   if(debug >= 1)
   {
      time(&currtime);
      printf("Done (%.0f seconds total)\n", (double)(currtime - start));
      fflush(stdout);
   }

   fprintf(fstatus, "[struct stat=\"OK\"]\n");
   fflush(stdout);

   exit(0);
}


/**************************************************/
/*                                                */
/*  Read the output header template file.         */
/*  Specifically extract the image size info.     */
/*                                                */
/**************************************************/

int readTemplate(char *filename)
{
   int       i;
   FILE     *fp;
   char      line[MAXSTR];


   /********************************************************/
   /* Open the template file, read and parse all the lines */
   /********************************************************/

   fp = fopen(filename, "r");

   if(fp == (FILE *)NULL)
      printError("Template file not found.");

   while(1)
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

      for(i=strlen(line); i<80; ++i)
	 line[i] = ' ';
      
      line[80] = '\0';

      parseLine(line);
   }

   return 0;
}



/**************************************************/
/*                                                */
/*  Parse header lines from the template,         */
/*  looking for NAXIS1, NAXIS2, CRPIX1 and CRPIX2 */
/*                                                */
/**************************************************/

int parseLine(char *line)
{
   char *keyword;
   char *value;
   char *end;

   int   len;

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
      output.naxes[0] = atoi(value);
      output_area.naxes[0] = atoi(value);
   }

   if(strcmp(keyword, "NAXIS2") == 0)
   {
      output.naxes[1] = atoi(value);
      output_area.naxes[1] = atoi(value);
   }

   if(strcmp(keyword, "CRPIX1") == 0)
   {
      output.crpix1 = atof(value);
      output_area.crpix1 = atof(value);
   }

   if(strcmp(keyword, "CRPIX2") == 0)
   {
      output.crpix2 = atof(value);
      output_area.crpix2 = atof(value);
   }

   return 0;
}


/*******************************************/
/*                                         */
/*  Open a FITS file pair and extract the  */
/*  pertinent header information.          */
/*                                         */
/*******************************************/

int readFits(char *fluxfile, char *areafile)
{
   int    status, nfound;
   long   naxes[2];
   double crpix[2];
   char   errstr[MAXSTR];

   status = 0;

   if(!noAreas)
   {
      if(fits_open_file(&input_area.fptr, areafile, READONLY, &status))
      {
	 sprintf(errstr, "Area file %s missing or invalid FITS", areafile);
	 printError(errstr);
      }
   }

   if(fits_open_file(&input.fptr, fluxfile, READONLY, &status))
   {
      sprintf(errstr, "Image file %s missing or invalid FITS", fluxfile);
      printError(errstr);
   }

   if(fits_read_keys_lng(input.fptr, "NAXIS", 1, 2, naxes, &nfound, &status))
      printFitsError(status);
   
   input.naxes[0] = naxes[0];
   input.naxes[1] = naxes[1];

   input_area.naxes[0] = naxes[0];
   input_area.naxes[1] = naxes[1];

   if(fits_read_keys_dbl(input.fptr, "CRPIX", 1, 2, crpix, &nfound, &status))
      printFitsError(status);

   input.crpix1 = crpix[0];
   input.crpix2 = crpix[1];

   input_area.crpix1 = crpix[0];
   input_area.crpix2 = crpix[1];
   
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


/******************************/
/*                            */
/*  Print out general warning */
/*                            */
/******************************/

void printWarning(char *msg)
{
   fprintf(fstatus, "[struct stat=\"WARNING\", msg=\"%s\"]\n", msg);
   exit(0);
}
