/* Module: mDump.c

Version  Developer        Date     Change
-------  ---------------  -------  -----------------------
1.0      John Good        31Aug10  Baseline code

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

char input_file1[MAXSTR];
char input_file2[MAXSTR];
char output_file[MAXSTR];

char  *input_header;

int  readFits      (char *fluxfile1, char *fluxfile2);
void printFitsError(int);
void printError    (char *);

int  debug;

struct
{
   fitsfile *fptr;
   long      naxes[2];
}
   input1, input2;

FILE *fout;


/*****************************************************/
/*                                                   */
/*  mDump                                            */
/*                                                   */
/*  Determine pixel statistics for a region on the   */
/*  sky in the given image.                          */
/*                                                   */
/*****************************************************/

int main(int argc, char **argv)
{
   char     *end;
   int       i, j, status, nullcnt;
   long      fpixel[4], lines, nelements;
   int       ibegin, iend;
   int       jbegin, jend;
   int       sum;

   double  **data1;
   double  **data2;

   double    flux, flux1, flux2;

   int *buf;



   /***************************************/
   /* Process the command-line parameters */
   /***************************************/

   debug   = 0;
   fstatus = stdout;

   for(i=0; i<argc; ++i)
   {
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
      printf ("[struct stat=\"ERROR\", msg=\"Usage: mExamine [-d level] in1.fits in2.fits\"]\n");
      exit(1);
   }

   strcpy(input_file1, argv[1]);
   strcpy(input_file2, argv[2]);
   strcpy(output_file, argv[3]);

   if(debug >= 1)
   {
      printf("debug       = %d\n",   debug);
      printf("input_file1 = [%s]\n", input_file1);
      printf("input_file2 = [%s]\n", input_file2);
      printf("output_file = [%s]\n", output_file);

      fflush(stdout);
   }

   fout = fopen(output_file, "w+");


   /************************/
   /* Read the input image */
   /************************/

   readFits(input_file1, input_file2);

   if(debug >= 1)
   {
      printf("input1.naxes[0]       =  %ld\n",   input1.naxes[0]);
      printf("input1.naxes[1]       =  %ld\n",   input1.naxes[1]);
      printf("input2.naxes[0]       =  %ld\n",   input2.naxes[0]);
      printf("input2.naxes[1]       =  %ld\n",   input2.naxes[1]);
      printf("\n");
      fflush(stdout);
   }


   /**********************************************/ 
   /* Allocate memory for the input image pixels */ 
   /**********************************************/ 

   nelements = input1.naxes[0];
   lines     = input2.naxes[1];

   data1 = (double **)malloc(lines * sizeof(double *));
   data2 = (double **)malloc(lines * sizeof(double *));

   for(j=0; j<lines; ++j)
   {
      data1[j] = (double *)malloc(nelements * sizeof(double));
      data2[j] = (double *)malloc(nelements * sizeof(double));
   }

   buf  = (int *)malloc(nelements * sizeof(int));

   if(debug >= 1)
   {
      printf("%ld bytes allocated for input image pixels\n", 
	 nelements * lines * sizeof(double));
      fflush(stdout);
   }

   // printf("XXX> sizeof(int) = %d\n", sizeof(int));


   /************************/
   /* Read the input lines */
   /************************/

   ibegin = 1;
   iend   = nelements;

   jbegin = 1;
   jend   = lines;

   fpixel[0] = ibegin;
   fpixel[1] = jbegin;
   fpixel[2] = 1;
   fpixel[3] = 1;

   status = 0;

   if(debug >= 3)
      printf("\n");

   for (j=jbegin; j<jend; ++j)
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

      if(fits_read_pix(input1.fptr, TDOUBLE, fpixel, nelements, NULL,
		       (void *)data1[j-jbegin], &nullcnt, &status))
	 printFitsError(status);
      
      if(fits_read_pix(input2.fptr, TDOUBLE, fpixel, nelements, NULL,
		       (void *)data2[j-jbegin], &nullcnt, &status))
	 printFitsError(status);
      
      ++fpixel[1];
   }


   /************************/
   /* Loop over the pixels */
   /************************/

   sum = 0;

   for (j=jbegin; j<=jend; ++j)
   {
      for(i=ibegin; i<=iend; ++i)
      {
	 flux1 = data1[j-jbegin][i-ibegin];
	 flux2 = data2[j-jbegin][i-ibegin];

	 flux = flux2 - flux1;

	 buf[i-ibegin] = (int)flux;

	 // printf("XXX> buf[%d] = %d\n", i-ibegin, buf[i-ibegin]);
	 fflush(stdout);
      }

      fwrite(buf, 4, nelements, fout);

      sum += 4*nelements;
   }

   // printf("XXX> %d\n", sum);

   if(debug >= 1)
   {
      printf("Input image read complete.\n\n");
      fflush(stdout);
   }

   fclose(fout);

   // fprintf(fstatus, "[struct stat=\"OK\"]\n");
   // fflush(fstatus);

   exit(0);
}


/*******************************************/
/*                                         */
/*  Open a FITS file pair and extract the  */
/*  pertinent header information.          */
/*                                         */
/*******************************************/

int readFits(char *fluxfile1, char * fluxfile2)
{
   int    status, nfound;
   long   naxes[2];
   char   errstr[MAXSTR];

   status = 0;

   if(fits_open_file(&input1.fptr, fluxfile1, READONLY, &status))
   {
      sprintf(errstr, "Image file %s missing or invalid FITS", fluxfile1);
      printError(errstr);
   }

   if(fits_read_keys_lng(input1.fptr, "NAXIS", 1, 2, naxes, &nfound, &status))
      printFitsError(status);
   
   input1.naxes[0] = naxes[0];
   input1.naxes[1] = naxes[1];

   
   status = 0;

   if(fits_open_file(&input2.fptr, fluxfile2, READONLY, &status))
   {
      sprintf(errstr, "Image file %s missing or invalid FITS", fluxfile2);
      printError(errstr);
   }

   if(fits_read_keys_lng(input2.fptr, "NAXIS", 1, 2, naxes, &nfound, &status))
      printFitsError(status);
   
   input2.naxes[0] = naxes[0];
   input2.naxes[1] = naxes[1];

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
