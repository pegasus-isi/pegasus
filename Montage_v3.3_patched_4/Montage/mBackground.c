/* Module: mBackground.c

Version  Developer        Date     Change
-------  ---------------  -------  -----------------------
2.1      John Good        24Apr06  Don't want to fail in table mode when
                                   the image is not in the list.
2.0      John Good        25Aug05  Updated flag handling (it was buggy)
                                   and "no area" to mean both input and 
                                   output areas
1.9      John Good        13Oct04  Changed format for printing time
1.8      John Good        18Sep04  Check argument count again after mode is known
1.7      John Good        27Aug04  Added "[-s statusfile]" to Usage statement
                                   and fixed status file usage (wasn't incrementing
				   argv properly)
1.6      John Good        17Apr04  Added "no areas" mode
1.5      John Good        14Nov03  Forgot to initialize 'tableDriven' flag
1.4      John Good        10Oct03  Added variant file column name handling
1.3      John Good        15Sep03  Updated fits_read_pix() call
1.2      John Good        07Apr03  Processing of output_file parameter
				   had minor order problem
1.1      John Good        14Mar03  Modified command-line processing
				   to use getopt() library.  Also
				   put in code to check if A,B,C 
				   are valid numbers and added specific
				   messages for missing flux and area files.
1.0      John Good        29Jan03  Baseline code

*/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <time.h>
#include <math.h>
#include <mtbl.h>
#include <fitsio.h>
#include <wcs.h>


#include "montage.h"
#include "mNaN.h"

#define MAXSTR  256
#define MAXFILE 256

char input_file       [MAXSTR];
char output_file      [MAXSTR];
char output_area_file [MAXSTR];

void printFitsError(int);
void printError    (char *);
int  readFits      (char *fluxfile, char *areafile);
int  checkHdr      (char *infile, int hdrflag, int hdu);


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
/*  mBackground                                                          */
/*                                                                       */
/*  Montage is a set of general reprojection / coordinate-transform /    */
/*  mosaicking programs.  Any number of input images can be merged into  */
/*  an output FITS file.  The attributes of the input are read from the  */
/*  input files; the attributes of the output are read a combination of  */
/*  the command line and a FITS header template file.                    */
/*                                                                       */
/*  This module, mBackground, removes a background plane from a single   */
/*  projected image.                                                     */
/*                                                                       */
/*************************************************************************/

int main(int argc, char **argv)
{
   int       i, j, status;
   long      fpixel[4], nelements;
   double   *buffer, *abuffer;
   int       nullcnt, tableDriven, haveStatus;
   int       icntr, ifname, cntr;
   int       ncols, index, istat;
   int       ia, ib, ic, id;

   double    pixel_value, background, x, y;

   double    A, B, C;
   double  **data;
   double  **area;

   char      tblfile [MAXSTR];
   char      corrfile[MAXSTR];
   char      file    [MAXSTR];
   char      infile  [MAXSTR];
   char      inarea  [MAXSTR];
   char      line    [MAXSTR];
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

   debug       = 0;
   tableDriven = 0;
   noAreas     = 0;
   haveStatus  = 0;

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

      else if(strcmp(argv[i], "-n") == 0)
	 noAreas = 1;

      else if(strcmp(argv[i], "-t") == 0)
	 tableDriven = 1;

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
   }

   if(haveStatus)
   {
      argv += 2;
      argc -= 2;;
   }
   
   if(debug)
   {
      argv += 2;
      argc -= 2;;
   }
   
   if(noAreas)
   {
      ++argv;
      --argc;
   }
   
   if(tableDriven)
   {
      ++argv;
      --argc;
   }
   
   if (argc < 5) 
   {
      printf ("[struct stat=\"ERROR\", msg=\"Usage: mBackground [-d level] [-n(o-areas)] [-s statusfile] in.fits out.fits A B C | mBackground [-t(able-mode)] [-d level] [-n(o-areas)] [-s statusfile] in.fits out.fits images.tbl corrfile.tbl\"]\n");
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

   A = 0.;
   B = 0.;
   C = 0.;

   if(!tableDriven)
   {
      if (argc != 6) 
      {
	 printf ("[struct stat=\"ERROR\", msg=\"Usage: mBackground [-d level] [-n(o-areas)] [-s statusfile] in.fits out.fits A B C | mBackground [-t](able-mode) [-d level] [-n(o-areas)] [-s statusfile] in.fits out.fits images.tbl corrfile.tbl\"]\n");
	 exit(1);
      }

      A = strtod(argv[3], &end);

      if(end < argv[3] + strlen(argv[3]))
      {
	 printf ("[struct stat=\"ERROR\", msg=\"A coefficient string is not a number\"]\n");
	 exit(1);
      }

      B = strtod(argv[4], &end);

      if(end < argv[4] + strlen(argv[4]))
      {
	 printf ("[struct stat=\"ERROR\", msg=\"B coefficient string is not a number\"]\n");
	 exit(1);
      }

      C = strtod(argv[5], &end);

      if(end < argv[5] + strlen(argv[5]))
      {
	 printf ("[struct stat=\"ERROR\", msg=\"C coefficient string is not a number\"]\n");
	 exit(1);
      }
   }
   else
   {
      /* Look up the file cntr in the images.tbl file */
      /* and then the correction coefficients in the  */
      /* corrections table generated by mBgModel      */

      if (argc != 5) 
      {
	 printf ("[struct stat=\"ERROR\", msg=\"Usage: mBackground [-d level] [-n(o-areas)] [-s statusfile] in.fits out.fits A B C | mBackground [-t](able-mode) [-d level] [-n(o-areas)] [-s statusfile] in.fits out.fits images.tbl corrfile.tbl\"]\n");
	 exit(1);
      }

      strcpy(tblfile,  argv[3]);
      strcpy(corrfile, argv[4]);


      /* Open the image list table file */

      ncols = topen(tblfile);

      if(ncols <= 0)
      {
	 fprintf (fstatus, "[struct stat=\"ERROR\", msg=\"Invalid image metadata file: %s\"]\n",
	    tblfile);
	 exit(1);
      }

      icntr  = tcol( "cntr");
      ifname = tcol( "fname");

      if(ifname < 0)
	 ifname = tcol( "file");

      if(icntr  < 0
      || ifname < 0)
      {
	 fprintf (fstatus, "[struct stat=\"ERROR\", msg=\"Image table needs columns cntr and fname\"]\n");
	 exit(1);
      }


      /* Read the records and find the cntr for our file name */

      index = 0;

      while(1)
      {
	 istat = tread();

	 if(istat < 0)
	 {
	    fprintf (fstatus, "[struct stat=\"ERROR\", msg=\"Hit end of image table without finding file name\"]\n");
	    exit(1);
	 }

	 cntr = atoi(tval(icntr));

	 strcpy(file, tval(ifname));

         if(strcmp(file, input_file) == 0)
            break;
      }

      tclose();


      ncols = topen(corrfile);

      icntr    = tcol( "id");
      ia       = tcol( "a");
      ib       = tcol( "b");
      ic       = tcol( "c");

      if(icntr    < 0
      || ia       < 0
      || ib       < 0
      || ic       < 0)
      {
	 fprintf (fstatus, "[struct stat=\"ERROR\", msg=\"Need columns: id,a,b,c in corrections file\"]\n");
	 exit(1);
      }


      /* Read the records and find the correction coefficients */

      while(1)
      {
	 istat = tread();

	 if(istat < 0)
	 {
	    A = 0.;
	    B = 0.;
	    C = 0.;

	    break;
	 }

	 id = atoi(tval(icntr));

	 if(id != cntr)
	    continue;

	 A = atof(tval(ia));
	 B = atof(tval(ib));
	 C = atof(tval(ic));

         break;
      }

      tclose();
   }

   if(strlen(input_file) > 5 
   && strcmp(input_file+strlen(input_file)-5, ".fits") == 0)
   {
      strcpy(line, input_file);

      line[strlen(line)-5] = '\0';

      strcpy(infile, line);
      strcat(infile,  ".fits");
      strcpy(inarea, line);
      strcat(inarea, "_area.fits");
   }
   else
   {
      strcpy(infile, input_file);
      strcat(infile,  ".fits");
      strcpy(inarea, input_file);
      strcat(inarea, "_area.fits");
   }

   if(strlen(output_file) > 5 &&
      strncmp(output_file+strlen(output_file)-5, ".fits", 5) == 0)
         output_file[strlen(output_file)-5] = '\0';

   strcpy(output_area_file, output_file);
   strcat(output_file,  ".fits");
   strcat(output_area_file, "_area.fits");

   if(debug >= 1)
   {
      printf("debug            = %d\n",   debug);
      printf("input_file       = [%s]\n", input_file);
      printf("output_file      = [%s]\n", output_file);
      printf("output_area_file = [%s]\n", output_area_file);
      printf("A                = %-g\n",  A);
      printf("B                = %-g\n",  B);
      printf("C                = %-g\n",  C);
      printf("tableDriven      = %d\n",   tableDriven);
      printf("noAreas          = %d\n",   noAreas);
      fflush(stdout);
   }


   /************************/
   /* Read the input image */
   /************************/

   time(&currtime);
   start = currtime;

   readFits(infile, inarea);

   if(debug >= 1)
   {
      printf("\nflux file            =  %s\n",  infile);
      printf("input.naxes[0]       =  %ld\n",    input.naxes[0]);
      printf("input.naxes[1]       =  %ld\n",    input.naxes[1]);
      printf("input.crpix1         =  %-g\n",   input.crpix1);
      printf("input.crpix2         =  %-g\n",   input.crpix2);

      printf("\narea file            =  %s\n",  inarea);
      printf("input_area.naxes[0]  =  %ld\n",    input.naxes[0]);
      printf("input_area.naxes[1]  =  %ld\n",    input.naxes[1]);
      printf("input_area.crpix1    =  %-g\n",   input.crpix1);
      printf("input_area.crpix2    =  %-g\n",   input.crpix2);

      fflush(stdout);
   }

   output.naxes[0] = input.naxes[0];
   output.naxes[1] = input.naxes[1];
   output.crpix1   = input.crpix1;
   output.crpix2   = input.crpix2;

   output_area.naxes[0] = output.naxes[0];
   output_area.naxes[1] = output.naxes[1];
   output_area.crpix1   = output.crpix1;
   output_area.crpix2   = output.crpix2;



   /***********************************************/ 
   /* Allocate memory for the output image pixels */ 
   /* (same size as the input image)              */ 
   /***********************************************/ 

   data = (double **)malloc(output.naxes[1] * sizeof(double *));

   for(j=0; j<output.naxes[1]; ++j)
      data[j] = (double *)malloc(output.naxes[0] * sizeof(double));

   if(debug >= 1)
   {
      printf("\n%lu bytes allocated for image pixels\n", 
	 output.naxes[0] * output.naxes[1] * sizeof(double));
      fflush(stdout);
   }


   /****************************/
   /* Initialize data to zeros */
   /****************************/

   for (j=0; j<output.naxes[1]; ++j)
   {
      for (i=0; i<output.naxes[0]; ++i)
      {
	 data[j][i] = 0.;
      }
   }


   /**********************************************/ 
   /* Allocate memory for the output pixel areas */ 
   /**********************************************/ 

   area = (double **)malloc(output.naxes[1] * sizeof(double *));

   for(j=0; j<output.naxes[1]; ++j)
      area[j] = (double *)malloc(output.naxes[0] * sizeof(double));

   if(debug >= 1)
   {
      printf("%lu bytes allocated for pixel areas\n", 
	 output.naxes[0] * output.naxes[1] * sizeof(double));
      fflush(stdout);
   }


   /****************************/
   /* Initialize area to zeros */
   /****************************/

   for (j=0; j<output.naxes[1]; ++j)
   {
      for (i=0; i<output.naxes[0]; ++i)
      {
	 area[j][i] = 0.;
      }
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

   if(debug >= 1)
   {
      x = input.naxes[0]/2. - input.crpix1;
      y = input.naxes[1]/2. - input.crpix2;

      background = A*x + B*y + C;

      printf("\nBackground offset for %s at center (%-g,%-g) = %-g\n\n", 
	 infile, x, y, background);
      
      fflush(stdout);
   }

   for (j=0; j<input.naxes[1]; ++j)
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

      if(fits_read_pix(input.fptr, TDOUBLE, fpixel, nelements, NULL,
		       buffer, &nullcnt, &status))
	 printFitsError(status);
      
      if(noAreas)
      {
	 for (i=0; i<input.naxes[0]; ++i)
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
	 x = i - input.crpix1;
	 y = j - input.crpix2;

	 pixel_value = buffer[i];

	 background = A*x + B*y + C;

	 if(mNaN(buffer[i])
	 || abuffer[i] <= 0.)
	 {
	    data[j][i] = nan;
	    area[j][i] = 0.;
	 }
	 else
	 {
	    data[j][i] = pixel_value - background;
	    area[j][i] = abuffer[i];
	 }

	 if(debug >= 3)
	 {
	    printf("(%4d,%4d): %10.3e (bg: %10.3e) at (%8.1f,%8.1f) -> %10.3e (%10.3e)\n",
	       j, i, pixel_value, background, x, y, data[j][i], area[j][i]);
	    fflush(stdout);
	 }
      }
   }

   free(buffer);
   free(abuffer);

   if(debug >= 1)
   {
      time(&currtime);
      printf("\nDone reading data (%.0f seconds)\n", 
	 (double)(currtime - start));
      fflush(stdout);
   }


   /********************************/
   /* Create the output FITS files */
   /********************************/

   remove(output_file);               

   if(fits_create_file(&output.fptr, output_file, &status)) 
      printFitsError(status);           

   if(!noAreas)
   {
      remove(output_area_file);               

      if(fits_create_file(&output_area.fptr, output_area_file, &status)) 
	 printFitsError(status);           
   }

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

   if(!noAreas)
   {
      if(fits_copy_header(input.fptr, output_area.fptr, &status))
	 printFitsError(status);           
   }

   if(debug >= 1)
   {
      printf("Header keywords copied to FITS output files\n\n"); 
      fflush(stdout);
   }

   if(fits_close_file(input.fptr, &status))
      printFitsError(status);

   if(!noAreas)
      if(fits_close_file(input_area.fptr, &status))
	 printFitsError(status);


   /***************************/
   /* Modify BITPIX to be -64 */
   /***************************/

   if(fits_update_key_lng(output.fptr, "BITPIX", -64,
                                  (char *)NULL, &status))
      printFitsError(status);

   if(!noAreas)
   {
      if(fits_update_key_lng(output_area.fptr, "BITPIX", -64,
				     (char *)NULL, &status))
	 printFitsError(status);
   }


   /************************/
   /* Write the image data */
   /************************/

   fpixel[0] = 1;
   fpixel[1] = 1;
   nelements = output.naxes[0];

   for(j=0; j<output.naxes[1]; ++j)
   {
      if (fits_write_pix(output.fptr, TDOUBLE, fpixel, nelements, 
			 (void *)(&data[j][0]), &status))
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

   if(!noAreas)
   {
      fpixel[0] = 1;
      fpixel[1] = 1;
      nelements = output.naxes[0];

      for(j=0; j<output.naxes[1]; ++j)
      {
	 if (fits_write_pix(output_area.fptr, TDOUBLE, fpixel, nelements,
			    (void *)(&area[j][0]), &status))
	    printFitsError(status);

	 ++fpixel[1];
      }

      free(area[0]);

      if(debug >= 1)
      {
	 printf("Data written to FITS area image\n\n"); 
	 fflush(stdout);
      }
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

   if(!noAreas)
   {
      if(fits_close_file(output_area.fptr, &status))
	 printFitsError(status);           

      if(debug >= 1)
      {
	 printf("FITS area image finalized\n\n"); 
	 fflush(stdout);
      }
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

int readFits(char *fluxfile, char *areafile)
{
   int    status, nfound;
   long   naxes[2];
   double crpix[2];
   char   errstr[MAXSTR];

   status = 0;

   checkHdr(fluxfile, 0, 0);

   if(!noAreas)
   {
      checkHdr(areafile, 0, 0);

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

