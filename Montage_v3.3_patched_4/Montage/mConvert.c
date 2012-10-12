/* Module: mConvert.c

Version  Developer        Date     Change
-------  ---------------  -------  -----------------------
1.0      John Good        02Feb06  Baseline code

*/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <time.h>
#include <math.h>
#include <fitsio.h>

#include "montage.h"
#include "mNaN.h"

#define MAXSTR  256

char input_file  [MAXSTR];
char output_file [MAXSTR];

void printFitsError(int);
void printError    (char *);
int  readFits      (char *fluxfile);
int  checkHdr      (char *infile, int hdrflag, int hdu);

long naxes[2];

int  debug;

fitsfile *infptr;
fitsfile *outfptr;

double    bscalei, bzeroi;
int       bitpixi;

static time_t currtime, start;


/*************************************************************************/
/*                                                                       */
/*  mConvert                                                             */
/*                                                                       */
/*  Montage is a set of general reprojection / coordinate-transform /    */
/*  mosaicking programs.  Any number of input images can be merged into  */
/*  an output FITS file.  The attributes of the input are read from the  */
/*  input files; the attributes of the output are read a combination of  */
/*  the command line and a FITS header template file.                    */
/*                                                                       */
/*  This module, mConvert, changes the datatype of the image.  When      */
/*  converting to floatin point, no additional information is needed.    */
/*  However, when converting from higher precision (e.g. 64-bit          */
/*  floating point) to lower (e.g. 16-bit integer) scaling information   */
/*  is necessary.  This can be given explicitly by the user or           */
/*  guessed by the program.                                              */
/*                                                                       */
/*************************************************************************/

int main(int argc, char **argv)
{
   int       i, j, status;
   int       haveBitpix, haveRmin, haveRmax, haveBlank;
   long      fpixeli[4], fpixelo[4], nelementsi, nelementso;
   double   *inbuffer;
   double   *outbuffer;
   double    bscaleo, bzeroo;
   int       nullcnt, haveStatus;

   double    rmin, rmax, Rmin, Rmax, blankVal, val;

   char     *end;

   int       bitpixo; 


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

   debug       = 0;
   haveStatus  = 0;
   bitpixo     = -64;
   haveBitpix  = 0;
   Rmin        = 0;
   Rmax        = 1;
   haveRmin    = 0;
   haveRmax    = 0;
   blankVal    = 0.;
   haveBlank   = 0.;


   fstatus = stdout;

   for(i=0; i<argc; ++i)
   {
      if(strcmp(argv[i], "-s") == 0)
      {
         haveStatus = 1;

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

         ++i;
      }

      else if(strcmp(argv[i], "-d") == 0)
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

	 ++i;
      }

      else if(strcmp(argv[i], "-b") == 0)
      {
	 haveBitpix = 1;

	 if(i+1 >= argc)
	 {
	    printf("[struct stat=\"ERROR\", msg=\"No bitpix value given\"]\n");
	    exit(1);
	 }

	 bitpixo = strtol(argv[i+1], &end, 0);

	 if(end - argv[i+1] < strlen(argv[i+1]))
	 {
	    printf("[struct stat=\"ERROR\", msg=\"Bitpix string is invalid: '%s'\"]\n", argv[i+1]);
	    exit(1);
	 }

	 if(bitpixo !=   8
	 && bitpixo !=  16
	 && bitpixo !=  32
	 && bitpixo != -32
	 && bitpixo != -64)
	 {
	    printf("[struct stat=\"ERROR\", msg=\"Bitpix must be one of (8, 16, 32, -32, -64)\"]\n");
	    exit(1);
	 }

	 ++i;
      }

      else if(strcmp(argv[i], "-min") == 0)
      {
	 haveRmin = 1;

	 if(i+1 >= argc)
	 {
	    printf("[struct stat=\"ERROR\", msg=\"No range min value given\"]\n");
	    exit(1);
	 }

	 Rmin = strtod(argv[i+1], &end);

	 if(end - argv[i+1] < strlen(argv[i+1]))
	 {
	    printf("[struct stat=\"ERROR\", msg=\"Range min string is invalid: '%s'\"]\n", argv[i+1]);
	    exit(1);
	 }

	 ++i;
      }

      else if(strcmp(argv[i], "-max") == 0)
      {
	 haveRmax = 1;

	 if(i+1 >= argc)
	 {
	    printf("[struct stat=\"ERROR\", msg=\"No range max value given\"]\n");
	    exit(1);
	 }

	 Rmax = strtod(argv[i+1], &end);

	 if(end - argv[i+1] < strlen(argv[i+1]))
	 {
	    printf("[struct stat=\"ERROR\", msg=\"Range max string is invalid: '%s'\"]\n", argv[i+1]);
	    exit(1);
	 }

	 ++i;
      }

      else if(strcmp(argv[i], "-blank") == 0)
      {
	 haveBlank = 1;

	 if(i+1 >= argc)
	 {
	    printf("[struct stat=\"ERROR\", msg=\"No blank value given\"]\n");
	    exit(1);
	 }

	 blankVal = strtod(argv[i+1], &end);

	 if(end - argv[i+1] < strlen(argv[i+1]))
	 {
	    printf("[struct stat=\"ERROR\", msg=\"Blank string is invalid: '%s'\"]\n", argv[i+1]);
	    exit(1);
	 }

	 ++i;
      }
   }

   if(debug)
   {
      argv += 2;
      argc -= 2;;
   }
   
   if(haveBitpix)
   {
      argv += 2;
      argc -= 2;;
   }
   
   if(haveRmin)
   {
      argv += 2;
      argc -= 2;;
   }
   
   if(haveRmax)
   {
      argv += 2;
      argc -= 2;;
   }
   
   if(haveBlank)
   {
      argv += 2;
      argc -= 2;;
   }
   
   if (argc < 3) 
   {
      printf ("[struct stat=\"ERROR\", msg=\"Usage: mConvert [-d level][-s statusfile][-b bitpix][-min minval][-max maxval][-blank blankval] in.fits out.fits\"]\n");
      exit(1);
   }

   strcpy(input_file,  argv[1]);

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

   if(debug >= 1)
   {
      printf("debug            = %d\n",   debug);
      printf("input_file       = [%s]\n", input_file);
      printf("output_file      = [%s]\n", output_file);
      fflush(stdout);
   }


   /************************/
   /* Read the input image */
   /************************/

   time(&currtime);
   start = currtime;

   readFits(input_file);

   if(debug >= 1)
   {
      printf("\nflux file            =  %s\n",  input_file);
      printf("naxes[0]       =  %ld\n",    naxes[0]);
      printf("naxes[1]       =  %ld\n",    naxes[1]);

      fflush(stdout);
   }


   /********************************/
   /* Create the output FITS files */
   /********************************/

   remove(output_file);               

   status = 0;

   if(fits_create_file(&outfptr, output_file, &status)) 
      printFitsError(status);           

   if(debug >= 1)
   {
      printf("\nFITS output file created (not yet populated)\n"); 
      fflush(stdout);
   }


   /********************************/
   /* Copy all the header keywords */
   /* from the input to the output */
   /********************************/

   if(fits_copy_header(infptr, outfptr, &status))
      printFitsError(status);           

   if(debug >= 1)
   {
      printf("Header keywords copied to FITS output files\n\n"); 
      fflush(stdout);
   }


   /**********************************/
   /* Modify BITPIX to desired value */
   /**********************************/

   if(fits_update_key_lng(outfptr, "BITPIX", bitpixo,
                                  (char *)NULL, &status))
      printFitsError(status);

   
   /******************************************************************/
   /* If we are scaling this to integer, add/update BSCALE and BZERO */
   /******************************************************************/

   if(!haveRmin || !haveRmax)
   {
      if(bitpixi == 8)
      {
	 Rmin =   0.;
	 Rmax = 255.;
      }
      else if(bitpixi == 16)
      {
	 Rmin = -32767.;
	 Rmax =  32768.;
      }
      else if(bitpixi == 32)
      {
	 Rmin = -2147483647.;
	 Rmax =  2147483648.;
      }
   }

   rmin = 0.;
   rmax = 1.;

   if(bitpixo == 8)
   {
      rmin =   0.;
      rmax = 255.;
   }
   else if(bitpixo == 16)
   {
      rmin = -32767.;
      rmax =  32767.;
   }
   else if(bitpixo == 32)
   {
      rmin = -2147483647.;
      rmax =  2147483647.;
   }

   bscaleo = (Rmax - Rmin) / (rmax - rmin) * bscalei;

   bzeroo  = bzeroi + (Rmin*rmax - Rmax*rmin) / (rmax - rmin) * bscalei;

   if(!haveBlank)
      blankVal = rmin;

   if(bitpixo > 0)
   {
      if(fits_update_key_dbl(outfptr, "BSCALE", bscaleo, -14,
				     (char *)NULL, &status))
	 printFitsError(status);

      if(fits_update_key_dbl(outfptr, "BZERO", bzeroo, -14,
				     (char *)NULL, &status))
	 printFitsError(status);
   }
   else
   {
      fits_delete_key(outfptr, "BSCALE", &status);
      fits_delete_key(outfptr, "BZERO", &status);
   }

   if(debug >= 1)
   {
      printf("bitpixi          = %d\n",  bitpixi);
      printf("Rmin             = %-g\n", Rmin);
      printf("Rmax             = %-g\n", Rmax);
      printf("bscalei          = %-g\n", bscalei);
      printf("bzeroi           = %-g\n", bzeroi);
      printf("\n");

      printf("bitpixo          = %d\n",  bitpixo);
      printf("rmin             = %-g\n", rmin);
      printf("rmax             = %-g\n", rmax);
      printf("bscaleo          = %-g\n", bscaleo);
      printf("bzeroo           = %-g\n", bzeroo);
      printf("\n");
      fflush(stdout);
   }



   /*****************************/
   /* Loop over the input lines */
   /*****************************/

   inbuffer  = (double *)malloc(naxes[0] * sizeof(double));

   fpixeli[0] = 1;
   fpixeli[1] = 1;
   fpixeli[2] = 1;
   fpixeli[3] = 1;

   nelementsi = naxes[0];


   outbuffer  = (double *)malloc(naxes[0] * sizeof(double));

   fpixelo[0] = 1;
   fpixelo[1] = 1;
   fpixelo[2] = 1;
   fpixelo[3] = 1;
   nelementso = naxes[0];

   status = 0;

   for (j=0; j<naxes[1]; ++j)
   {
      if(debug >= 2)
      {
	 if(debug >= 3)
	    printf("\n");

	 printf("\rProcessing input row %5d  ", j);

	 if(debug >= 3)
	    printf("\n");

	 fflush(stdout);
      }


      /***********************************/
      /* Read a line from the input file */
      /***********************************/

      if(fits_read_pix(infptr, TDOUBLE, fpixeli, nelementsi, NULL,
		       inbuffer, &nullcnt, &status))
	 printFitsError(status);
      
      ++fpixeli[1];


      /**********************************/
      /* Convert input values to output */
      /**********************************/

      for (i=0; i<naxes[0]; ++i)
      {
	 outbuffer[i] = inbuffer[i];

	 if(bitpixo > 0)
	 {
	    if(mNaN(outbuffer[i]))
	    {
	       outbuffer[i] = blankVal * bscaleo + bzeroo;
	    }
	    else
	    {
	       val = (outbuffer[i] - bzeroo) / bscaleo;

	       if(val < rmin)
		  outbuffer[i] = rmin * bscaleo + bzeroo;

	       else if(val > rmax)
		  outbuffer[i] = rmax * bscaleo + bzeroo;
	    }
	 }
      }


      /***********************************/
      /* Write a line to the output file */
      /***********************************/

      if (fits_write_pix(outfptr, TDOUBLE, fpixelo, nelementso, 
			 (void *)(outbuffer), &status))
	 printFitsError(status);

      ++fpixelo[1];
   }

   if(debug >= 1)
   {
      printf("Data written to FITS data image\n"); 
      fflush(stdout);
   }


   /************************/
   /* Close the FITS files */
   /************************/

   if(fits_close_file(infptr, &status))
      printFitsError(status);

   if(fits_close_file(outfptr, &status))
      printFitsError(status);           

   if(debug >= 1)
   {
      printf("FITS data copied\n"); 
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


/*******************************************/
/*                                         */
/*  Open a FITS file pair and extract the  */
/*  pertinent header information.          */
/*                                         */
/*******************************************/

int readFits(char *fluxfile)
{
   int    status, nfound;
   char   errstr[MAXSTR];

   status = 0;

   checkHdr(fluxfile, 0, 0);

   if(fits_open_file(&infptr, fluxfile, READONLY, &status))
   {
      sprintf(errstr, "Image file %s missing or invalid FITS", fluxfile);
      printError(errstr);
   }

   if(fits_read_key(infptr, TINT, "BITPIX", &bitpixi, (char *)NULL, &status))
      printFitsError(status);
   
   if(fits_read_keys_lng(infptr, "NAXIS", 1, 2, naxes, &nfound, &status))
      printFitsError(status);
   
   if(fits_read_key(infptr, TDOUBLE, "BSCALE", &bscalei, (char *)NULL, &status))
      bscalei = 1.;

   if(fits_read_key(infptr, TDOUBLE, "BZERO", &bzeroi, (char *)NULL, &status))
      bzeroi = 0.;

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

