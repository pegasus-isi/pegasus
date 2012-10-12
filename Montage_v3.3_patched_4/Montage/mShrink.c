/* Module: mShrink.c

Version  Developer        Date     Change
-------  ---------------  -------  -----------------------
3.1      John Good        27Feb09  Add ability to extract HDU
3.0      John Good        29Dec08  Added separate code for scale factors
				   less than one (expanding).  The code 
				   developed orginally was aimed at shrinking
				   and didn't handle expanding properly.
2.2      John Good        12Jan06  Added check so we don't write more
				   lines to file than header says are there
2.1      John Good        03Mar05  Added a "fixed size" mode so user
				   can get shrunken image to fit a specified
				   space
2.0      John Good        22Feb05  Now copy the whole header and check
				   for DSS parameters
1.6      John Good        13Oct04  Changed format for printing time
1.5      John Good        03Aug04  Changed precision on updated keywords
1.4      A. Alexov        15Jul04  CD matrix needed multiplication by
                                   the xfactor to match CDELT calculations.
1.3      John Good        06Jul04  Switch FITS header reading routines;
				   old one did not give "not found" status
1.2      John Good        07Jun04  Modified FITS key updating precision
1.1      John Good        27May04  Made sure messages were written
				   to fstatus
1.0      John Good        12Feb04  Baseline code

*/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <time.h>
#include <math.h>

#include "fitsio.h"
#include "wcs.h"
#include "mNaN.h"

#include "montage.h"

#define MAXSTR  256

extern char *optarg;
extern int optind, opterr;

extern int getopt(int argc, char *const *argv, const char *options);

int debugCheck(char *debugStr);

int  haveCtype;
int  haveCrval;
int  haveCrpix;
int  haveCnpix;
int  havePixelsz;
int  havePP;
int  haveCdelt;
int  haveCrota2;
int  haveCD11;
int  haveCD12;
int  haveCD21;
int  haveCD22;
int  havePC11;
int  havePC12;
int  havePC21;
int  havePC22;
int  haveEpoch;
int  haveEquinox;
int  haveBunit;
int  haveBlank;

char input_file [MAXSTR];
char output_file[MAXSTR];

int  readFits      (char *fluxfile);
void printFitsError(int);
void printError    (char *);

int  debug;

struct
{
   fitsfile *fptr;
   long      bitpix;
   long      naxes[2];
   char      ctype1[16];
   char      ctype2[16];
   double    crval1, crval2;
   double    crpix1, crpix2;
   double    cnpix1, cnpix2;
   double    xpixelsz, ypixelsz;
   double    ppo3, ppo6;
   double    cdelt1, cdelt2;
   double    crota2;
   double    cd11;
   double    cd12;
   double    cd21;
   double    cd22;
   double    pc11;
   double    pc12;
   double    pc21;
   double    pc22;
   double    epoch;
   double    equinox;
   char      bunit[80];
   long      blank;
}
   input, output;

static time_t currtime, start;

static int hdu;


/*************************************************************************/
/*                                                                       */
/*  mShrink                                                              */
/*                                                                       */
/*  Montage is a set of general reprojection / coordinate-transform /    */
/*  mosaicking programs.  Any number of input images can be merged into  */
/*  an output FITS file.                                                 */
/*                                                                       */
/*  This module, mShrink, is a utility program for making smaller        */
/*  versions of a FITS file by averaging NxN blocks of pixels.           */
/*                                                                       */
/*************************************************************************/

int main(int argc, char **argv)
{
   int       i, j, ii, jj, c, status, bufrow, fixedSize, split;
   int       ibuffer, jbuffer, ifactor, nbuf, nullcnt, k, l, imin, imax, jmin, jmax;
   long      fpixel[4], fpixelo[4], nelements, nelementso;
   double    obegin, oend;
   double   *colfact, *rowfact;
   double   *buffer;
   double    xfactor, flux, area;

   double   *outdata;
   double  **indata;

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

   time(&currtime);
   start = currtime;

   debug     = 0;
   opterr    = 0;
   fixedSize = 0;
   hdu       = 0;

   fstatus = stdout;

   while ((c = getopt(argc, argv, "d:h:s:f")) != EOF) 
   {
      switch (c) 
      {
         case 'd':
            debug = debugCheck(optarg);
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
	    
         case 's':
            if((fstatus = fopen(optarg, "w+")) == (FILE *)NULL)
            {
               printf("[struct stat=\"ERROR\", msg=\"Cannot open status file: %s\"]\n",
                  optarg);
               exit(1);
            }
            break;

         case 'f':
            fixedSize = 1;
            break;

         default:
            printf("[struct stat=\"ERROR\", msg=\"Usage: %s [-f(ixed-size)] [-d level] [-h hdu] [-s statusfile] in.fits out.fits factor\"]\n", argv[0]);
            exit(1);
            break;
      }
   }

   if (argc - optind < 3) 
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Usage: %s [-f(ixed-size)] [-d level] [-h hdu] [-s statusfile] in.fits out.fits factor\"]\n", argv[0]);
      exit(1);
   }
  
   strcpy(input_file,    argv[optind]);
   strcpy(output_file,   argv[optind + 1]);

   xfactor = strtod(argv[optind + 2], &end);

   if(end < argv[optind + 2] + strlen(argv[optind + 2]))
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Shrink factor (%s) cannot be interpreted as an real number\"]\n",
         argv[optind + 2]);
      exit(1);
   }

   if(!fixedSize)
   {
      ifactor = ceil(xfactor);

      if((double)ifactor < xfactor)
	 xfactor += 2;
   }

   if(xfactor <= 0)
   {
      if(fixedSize)
	 fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Image size (%s) must be positive\"]\n",
	    argv[optind + 2]);
      else
	 fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Shrink factor (%s) must be positive\"]\n",
	    argv[optind + 2]);
      exit(1);
   }

   if(debug >= 1)
   {
      printf("input_file       = [%s]\n", input_file);
      printf("output_file      = [%s]\n", output_file);
      printf("xfactor          = %-g\n",  xfactor);
      printf("ifactor          = %d\n",   ifactor);
      fflush(stdout);
   }


   /************************/
   /* Read the input image */
   /************************/

   readFits(input_file);

   if(input.bitpix > 0)
   {
     if(haveBlank) 
        nan = input.blank;
     else
        nan = 0;
   }

   if(debug >= 1)
   {
      printf("\nflux file            =  %s\n",  input_file);
      printf("input.bitpix         =  %ld\n",   input.bitpix);
      printf("input.naxes[0]       =  %ld\n",   input.naxes[0]);
      printf("input.naxes[1]       =  %ld\n",   input.naxes[1]);

      if(haveCtype)   printf("input.ctype1         =  %s\n",    input.ctype1);
      if(haveCtype)   printf("input.typel2         =  %s\n",    input.ctype2);
      if(haveCrval)   printf("input.crval1         =  %-g\n",   input.crval1);
      if(haveCrval)   printf("input.crval2         =  %-g\n",   input.crval2);
      if(haveCrpix)   printf("input.crpix1         =  %-g\n",   input.crpix1);
      if(haveCrpix)   printf("input.crpix2         =  %-g\n",   input.crpix2);
      if(haveCnpix)   printf("input.cnpix1         =  %-g\n",   input.cnpix1);
      if(haveCnpix)   printf("input.cnpix2         =  %-g\n",   input.cnpix2);
      if(havePixelsz) printf("input.xpixelsz       =  %-g\n",   input.xpixelsz);
      if(havePixelsz) printf("input.ypixelsz       =  %-g\n",   input.ypixelsz);
      if(havePP)      printf("input.ppo3           =  %-g\n",   input.ppo3);
      if(havePP)      printf("input.ppo6           =  %-g\n",   input.ppo6);
      if(haveCdelt)   printf("input.cdelt1         =  %-g\n",   input.cdelt1);
      if(haveCdelt)   printf("input.cdelt2         =  %-g\n",   input.cdelt2);
      if(haveCrota2)  printf("input.crota2         =  %-g\n",   input.crota2);
      if(haveCD11)    printf("input.cd11           =  %-g\n",   input.cd11);
      if(haveCD12)    printf("input.cd12           =  %-g\n",   input.cd12);
      if(haveCD21)    printf("input.cd21           =  %-g\n",   input.cd21);
      if(haveCD22)    printf("input.cd22           =  %-g\n",   input.cd22);
      if(havePC11)    printf("input.pc11           =  %-g\n",   input.pc11);
      if(havePC12)    printf("input.pc12           =  %-g\n",   input.pc12);
      if(havePC21)    printf("input.pc21           =  %-g\n",   input.pc21);
      if(havePC22)    printf("input.pc22           =  %-g\n",   input.pc22);
      if(haveEpoch)   printf("input.epoch          =  %-g\n",   input.epoch);
      if(haveEquinox) printf("input.equinox        =  %-g\n",   input.equinox);
      if(haveBunit)   printf("input.bunit          =  %s\n",    input.bunit);
      if(haveBlank)   printf("input.blank          =  %d\n",    input.blank);
      printf("\n");

      fflush(stdout);
   }


   /***********************************************/
   /* If we are going for a fixed size, the scale */
   /* factor needs to be computed.                */
   /***********************************************/

   if(fixedSize)
   {
      if(input.naxes[0] > input.naxes[1])
	 xfactor = (double)input.naxes[0]/(int)xfactor;
      else
	 xfactor = (double)input.naxes[1]/(int)xfactor;

      ifactor = ceil(xfactor);

      if((double)ifactor < xfactor)
	 xfactor += 2;

      if(debug >= 1)
      {
	 printf("xfactor         -> %-g\n",  xfactor);
	 printf("ifactor         -> %d\n",   ifactor);
	 fflush(stdout);
      }
   }

   /***********************************************/
   /* Compute all the parameters for the shrunken */
   /* output file.                                */
   /***********************************************/

   output.naxes[0] = floor((double)input.naxes[0]/xfactor);
   output.naxes[1] = floor((double)input.naxes[1]/xfactor);
   
   if(debug >= 1)
   {
      printf("output.naxes[0] = %d\n",  output.naxes[0]);
      printf("output.naxes[1] = %d\n",  output.naxes[1]);
      fflush(stdout);
   }

   strcpy(output.ctype1, input.ctype1);
   strcpy(output.ctype2, input.ctype2);

   output.crval1   = input.crval1;
   output.crval2   = input.crval2;
   output.crpix1   = input.crpix1/xfactor+1;
   output.crpix2   = input.crpix2/xfactor;
   output.cdelt1   = input.cdelt1*xfactor;
   output.cdelt2   = input.cdelt2*xfactor;
   output.crota2   = input.crota2;
   output.cd11     = input.cd11*xfactor;
   output.cd12     = input.cd12*xfactor;
   output.cd21     = input.cd21*xfactor;
   output.cd22     = input.cd22*xfactor;
   output.pc11     = input.pc11;
   output.pc12     = input.pc12;
   output.pc21     = input.pc21;
   output.pc22     = input.pc22;
   output.epoch    = input.epoch;
   output.equinox  = input.equinox;

   strcpy(output.bunit, input.bunit);

   if(haveCnpix)
   {
      input.crpix1    = input.ppo3 / input.xpixelsz - input.cnpix1 + 0.5; 
      input.crpix2    = input.ppo6 / input.ypixelsz - input.cnpix2 + 0.5; 

      output.crpix1   = input.crpix1/xfactor;
      output.crpix2   = input.crpix2/xfactor;

      output.xpixelsz = input.xpixelsz * xfactor;
      output.ypixelsz = input.ypixelsz * xfactor;

      output.cnpix1   = input.ppo3 / output.xpixelsz - output.crpix1 + 0.5;
      output.cnpix2   = input.ppo6 / output.ypixelsz - output.crpix2 + 0.5;
   }


   /********************************/
   /* Create the output FITS files */
   /********************************/

   status = 0;

   remove(output_file);               

   if(fits_create_file(&output.fptr, output_file, &status)) 
      printFitsError(status);           


   /******************************************************/
   /* Create the FITS image.  Copy over the whole header */
   /******************************************************/

   if(fits_copy_header(input.fptr, output.fptr, &status))
      printFitsError(status);

   if(debug >= 1)
   {
      printf("\nFITS header copied to output\n"); 
      fflush(stdout);
   }


   /************************************/
   /* Reset all the WCS header kewords */
   /************************************/

   if(fits_update_key_lng(output.fptr, "NAXIS", 2,
                                  (char *)NULL, &status))
      printFitsError(status);           

   if(fits_update_key_lng(output.fptr, "NAXIS1", output.naxes[0],
                                  (char *)NULL, &status))
      printFitsError(status);           

   if(fits_update_key_lng(output.fptr, "NAXIS2", output.naxes[1],
                                  (char *)NULL, &status))
      printFitsError(status);           

   if(haveBlank && fits_update_key_str(output.fptr, "BUNIT", output.bunit,
                                  (char *)NULL, &status))
      printFitsError(status);           

   if(haveBunit && fits_update_key_lng(output.fptr, "BLANK", output.blank,
                                  (char *)NULL, &status))
      printFitsError(status);           

   if(haveCtype && fits_update_key_str(output.fptr, "CTYPE1", output.ctype1,
                                  (char *)NULL, &status))
      printFitsError(status);           

   if(haveCtype && fits_update_key_str(output.fptr, "CTYPE2", output.ctype2,
                                  (char *)NULL, &status))
      printFitsError(status);           

   if(haveCrval && fits_update_key_dbl(output.fptr, "CRVAL1", output.crval1, -14,
                                  (char *)NULL, &status))
      printFitsError(status);           

   if(haveCrval && fits_update_key_dbl(output.fptr, "CRVAL2", output.crval2, -14,
                                  (char *)NULL, &status))
      printFitsError(status);           

   if(haveCrpix && fits_update_key_dbl(output.fptr, "CRPIX1", output.crpix1, -14,
                                  (char *)NULL, &status))
      printFitsError(status);           

   if(haveCrpix && fits_update_key_dbl(output.fptr, "CRPIX2", output.crpix2, -14,
                                  (char *)NULL, &status))
      printFitsError(status);           

   if(haveCnpix && fits_update_key_dbl(output.fptr, "CNPIX1", output.cnpix1, -14,
                                  (char *)NULL, &status))
      printFitsError(status);           

   if(haveCnpix && fits_update_key_dbl(output.fptr, "CNPIX2", output.cnpix2, -14,
                                  (char *)NULL, &status))
      printFitsError(status);           

   if(havePixelsz && fits_update_key_dbl(output.fptr, "XPIXELSZ", output.xpixelsz, -14,
                                  (char *)NULL, &status))
      printFitsError(status);           

   if(havePixelsz && fits_update_key_dbl(output.fptr, "YPIXELSZ", output.ypixelsz, -14,
                                  (char *)NULL, &status))
      printFitsError(status);           

   if(haveCdelt && fits_update_key_dbl(output.fptr, "CDELT1", output.cdelt1, -14,
				     (char *)NULL, &status))
	 printFitsError(status);           

   if(haveCdelt && fits_update_key_dbl(output.fptr, "CDELT2", output.cdelt2, -14,
                                  (char *)NULL, &status))
      printFitsError(status);           

   if(haveCrota2 && fits_update_key_dbl(output.fptr, "CROTA2", output.crota2, -14,
                                  (char *)NULL, &status))
      printFitsError(status);           

   if(haveCD11 && fits_update_key_dbl(output.fptr, "CD1_1", output.cd11, -14,
                                  (char *)NULL, &status))
      printFitsError(status);           

   if(haveCD12 && fits_update_key_dbl(output.fptr, "CD1_2", output.cd12, -14,
                                  (char *)NULL, &status))
      printFitsError(status);           

   if(haveCD21 && fits_update_key_dbl(output.fptr, "CD2_1", output.cd21, -14,
                                  (char *)NULL, &status))
      printFitsError(status);           

   if(haveCD22 && fits_update_key_dbl(output.fptr, "CD2_2", output.cd22, -14,
                                  (char *)NULL, &status))
      printFitsError(status);           

   if(havePC11 && fits_update_key_dbl(output.fptr, "PC1_1", output.pc11, -14,
                                  (char *)NULL, &status))
      printFitsError(status);           

   if(havePC12 && fits_update_key_dbl(output.fptr, "PC1_2", output.pc12, -14,
                                  (char *)NULL, &status))
      printFitsError(status);           

   if(havePC21 && fits_update_key_dbl(output.fptr, "PC2_1", output.pc21, -14,
                                  (char *)NULL, &status))
      printFitsError(status);           

   if(havePC22 && fits_update_key_dbl(output.fptr, "PC2_2", output.pc22, -14,
                                  (char *)NULL, &status))
      printFitsError(status);           

   if(haveEpoch && fits_update_key_dbl(output.fptr, "EPOCH", output.epoch, -14,
                                  (char *)NULL, &status))
      printFitsError(status);           

   if(haveEquinox && fits_update_key_dbl(output.fptr, "EQUINOX", output.equinox, -14,
                                  (char *)NULL, &status))
      printFitsError(status);           


   if(debug >= 1)
   {
      printf("Output header keywords set\n\n");
      fflush(stdout);
   }



   /***********************************************/ 
   /* Allocate memory for a line of output pixels */ 
   /***********************************************/ 

   outdata = (double *)malloc(output.naxes[0] * sizeof(double));


   /*************************************************************************/ 
   /* We could probably come up with logic that would work for both scale   */
   /* factors of less than one and greater than one but the it would be too */
   /* hard to follow.  Instead, we put in a big switch here to deal with    */
   /* the two cases separately.                                             */
   /*************************************************************************/ 

   if(xfactor < 1.)
   {
      /************************************************/ 
      /* Allocate memory for "ifactor" lines of input */ 
      /************************************************/ 

      nbuf = 2;

      indata = (double **)malloc(nbuf * sizeof(double *));

      for(j=0; j<nbuf; ++j)
	 indata[j] = (double *)malloc((input.naxes[0]+1) * sizeof(double));


      /**********************************************************/
      /* Create the output array by processing the input pixels */
      /**********************************************************/

      ibuffer = 0;

      buffer  = (double *)malloc(input.naxes[0] * sizeof(double));
      colfact = (double *)malloc(nbuf * sizeof(double));
      rowfact = (double *)malloc(nbuf * sizeof(double));

      fpixel[0] = 1;
      fpixel[1] = 1;
      fpixel[2] = 1;
      fpixel[3] = 1;

      fpixelo[0] = 1;
      fpixelo[1] = 1;

      nelements = input.naxes[0];

      status = 0;


      /******************************/
      /* Loop over the output lines */
      /******************************/

      split = 0;

      for(l=0; l<output.naxes[1]; ++l)
      {
	 obegin = (fpixelo[1] - 1.) * xfactor;
	 oend   =  fpixelo[1] * xfactor;

	 if(floor(oend) == oend)
	    oend = obegin;

	 if(debug >= 2)
	 {
	    printf("OUTPUT row %d: obegin = %.2f -> oend = %.3f\n\n", l, obegin, oend);
	    fflush(stdout);
	 }

	 rowfact[0] = 1.;
	 rowfact[1] = 0.;


	 /******************************************/
	 /* If we have gone over into the next row */
	 /******************************************/

	 if(l == 0 || (int)oend > (int)obegin)
	 {
	    rowfact[0] = 1.;
	    rowfact[1] = 0.;

	    if(l > 0)
	    {
	       split = 1;

	       jbuffer = (ibuffer + 1) % nbuf;

	       rowfact[1] = (oend - (int)(fpixelo[1] * xfactor))/xfactor;
	       rowfact[0] = 1. - rowfact[1];
	    }
	    else
	    {
	       jbuffer = 0;
	    }

	    if(debug >= 2)
	    {
	       printf("Reading input image row %5ld  (ibuffer %d)\n", fpixel[1], jbuffer);
	       fflush(stdout);
	    }

	    if(debug >= 2)
	    {
	       printf("Rowfact:  %-g %-g\n", rowfact[0], rowfact[1]);
	       fflush(stdout);
	    }


	    /***********************************/
	    /* Read a line from the input file */
	    /***********************************/

	    if(fpixel[1] <= input.naxes[1])
	    {
	       if(fits_read_pix(input.fptr, TDOUBLE, fpixel, nelements, NULL,
				buffer, &nullcnt, &status))
		  printFitsError(status);
	    }
	    
	    ++fpixel[1];


	    /************************/
	    /* For each input pixel */
	    /************************/

	    indata[jbuffer][input.naxes[0]] = nan;

	    for (i=0; i<input.naxes[0]; ++i)
	    {
	       indata[jbuffer][i] = buffer[i];

	       if(debug >= 4)
	       {
		  printf("input: line %5d / pixel %5d: indata[%d][%d] = %10.3e\n",
		     fpixel[1]-2, i, jbuffer, i, indata[jbuffer][i]);
		  fflush(stdout);
	       }
	    }

	    if(debug >= 4)
	    {
	       printf("---\n");
	       fflush(stdout);
	    }
	 }


	 /*************************************/
	 /* Write out the next line of output */
	 /*************************************/

	 nelementso = output.naxes[0];

	 for(k=0; k<nelementso; ++k)
	 {
	    /* When "expanding" we never need to use more than two   */
	    /* pixels in more than two rows.  The row factors were   */
	    /* computed above and the column factors will be compute */
	    /* here as we go.                                        */

	    outdata[k] = nan;

	    colfact[0] = 1.;
	    colfact[1] = 0.;

	    obegin =  (double)k     * xfactor;
	    oend   = ((double)k+1.) * xfactor;

	    if(floor(oend) == oend)
	       oend = obegin;

	    imin = (int)obegin;

	    if((int)oend > (int)obegin)
	    {
	       colfact[1] = (oend - (int)(((double)k+1.) * xfactor))/xfactor;
	       colfact[0] = 1. - colfact[1];
	    }

	    flux = 0;
	    area = 0;

	    for(jj=0; jj<2; ++jj)
	    {
	       if(rowfact[jj] == 0.)
		  continue;

	       for(ii=0; ii<2; ++ii)
	       {
		  bufrow = (ibuffer + jj) % nbuf;

		  if(!mNaN(indata[bufrow][imin+ii]) && colfact[ii] > 0.)
		  {
		     flux += indata[bufrow][imin+ii] * colfact[ii] * rowfact[jj];
		     area += colfact[ii] * rowfact[jj];

		     if(debug >= 3)
		     {
			printf("output[%d][%d] -> %10.2e (area: %10.2e) (using indata[%d][%d] = %10.2e, colfact[%d] = %5.3f, rowfact[%d] = %5.3f)\n", 
			   l, k, flux, area,
			   bufrow, imin+ii, indata[bufrow][imin+ii], 
			   imin+ii, colfact[ii],
			   jj, rowfact[jj]);

			fflush(stdout);
		     }
		  }
	       }
	    }

	    if(area > 0.)
	       outdata[k] = flux/area;
	    else
	       outdata[k] = nan;

	    if(debug >= 3)
	    {
	       printf("\nflux[%d] = %-g / area = %-g --> outdata[%d] = %-g\n",
		  k, flux, area, k, outdata[k]);
	       
	       printf("---\n");
	       fflush(stdout);
	    }
	 }

	 if(fpixelo[1] <= output.naxes[1])
	 {
	    if(debug >= 2)
	    {
	       printf("\nWRITE output image row %5ld\n===========================================\n", fpixelo[1]);
	       fflush(stdout);
	    }

	    if (fits_write_pix(output.fptr, TDOUBLE, fpixelo, nelementso, 
			       (void *)(outdata), &status))
	       printFitsError(status);
	    }

	 ++fpixelo[1];

	 if(split)
	 {
	    ibuffer = jbuffer;
	    split = 0;
	 }


	 /***************************************************************/
	 /* Special case:  The expansion factor is integral and we have */
	 /* gotten to the point where we need the next line.            */
	 /***************************************************************/

	 oend   =  fpixelo[1] * xfactor;

	 if(fpixel[1] <= input.naxes[1] && floor(oend) == oend)
	 {
	    if(debug >= 2)
	    {
	       printf("Reading input image row %5ld  (ibuffer %d)\n", fpixel[1], jbuffer);
	       fflush(stdout);
	    }

	    if(fits_read_pix(input.fptr, TDOUBLE, fpixel, nelements, NULL,
			     buffer, &nullcnt, &status))
	       printFitsError(status);
	    
	    ++fpixel[1];

	    indata[jbuffer][input.naxes[0]] = nan;

	    for (i=0; i<input.naxes[0]; ++i)
	    {
	       indata[jbuffer][i] = buffer[i];

	       if(debug >= 4)
	       {
		  printf("input: line %5d / pixel %5d: indata[%d][%d] = %10.3e\n",
		     fpixel[1]-2, i, jbuffer, i, indata[jbuffer][i]);
		  fflush(stdout);
	       }
	    }

	    if(debug >= 4)
	    {
	       printf("---\n");
	       fflush(stdout);
	    }
	 }
      }
   }
   else
   {
      /************************************************/ 
      /* Allocate memory for "ifactor" lines of input */ 
      /************************************************/ 

      nbuf = ifactor + 1;

      indata = (double **)malloc(nbuf * sizeof(double *));

      for(j=0; j<nbuf; ++j)
	 indata[j] = (double *)malloc(input.naxes[0] * sizeof(double));



      /**********************************************************/
      /* Create the output array by processing the input pixels */
      /**********************************************************/

      ibuffer = 0;

      buffer  = (double *)malloc(input.naxes[0] * sizeof(double));
      colfact = (double *)malloc(input.naxes[0] * sizeof(double));
      rowfact = (double *)malloc(input.naxes[1] * sizeof(double));

      fpixel[0] = 1;
      fpixel[1] = 1;
      fpixel[2] = 1;
      fpixel[3] = 1;

      fpixelo[0] = 1;
      fpixelo[1] = 1;

      nelements = input.naxes[0];

      status = 0;


      /*****************************/
      /* Loop over the input lines */
      /*****************************/

      l = 0;

      obegin =  (double)l     * xfactor;
      oend   = ((double)l+1.) * xfactor;

      jmin = floor(obegin);
      jmax = ceil (oend);

      for(jj=jmin; jj<=jmax; ++jj)
      {
	 rowfact[jj-jmin] = 1.;

	      if(jj <= obegin && jj+1 <= oend) rowfact[jj-jmin] = jj+1. - obegin;
	 else if(jj <= obegin && jj+1 >= oend) rowfact[jj-jmin] = oend - obegin;
	 else if(jj >= obegin && jj+1 >= oend) rowfact[jj-jmin] = oend - jj;

	 if(rowfact[jj-jmin] < 0.)
	    rowfact[jj-jmin] = 0.;

	 if(debug >= 4)
	 {
	    printf("rowfact[%d]  %-g\n", jj, rowfact[jj]);
	    fflush(stdout);
	 }
      }

      for (j=0; j<input.naxes[1]; ++j)
      {
	 if(debug >= 2)
	 {
	    printf("Reading input image row %5ld  (ibuffer %d)\n", fpixel[1], ibuffer);
	    fflush(stdout);
	 }


	 /***********************************/
	 /* Read a line from the input file */
	 /***********************************/

	 if(fits_read_pix(input.fptr, TDOUBLE, fpixel, nelements, NULL,
			  buffer, &nullcnt, &status))
	    printFitsError(status);
	 
	 ++fpixel[1];

	 /************************/
	 /* For each input pixel */
	 /************************/

	 for (i=0; i<input.naxes[0]; ++i)
	 {
	    indata[ibuffer][i] = buffer[i];

	    if(debug >= 4)
	    {
	       printf("input: line %5d / pixel %5d: indata[%d][%d] = %10.2e\n",
		  j, i, ibuffer, i, indata[ibuffer][i]);
	       fflush(stdout);
	    }
	 }

	 if(debug >= 4)
	 {
	    printf("---\n");
	    fflush(stdout);
	 }


	 /**************************************************/
	 /* If we have enough for the next line of output, */
	 /* compute and write it                           */
	 /**************************************************/

	 if(j == jmax || fpixel[1] == input.naxes[1])
	 {
	    nelementso = output.naxes[0];

	    for(k=0; k<nelementso; ++k)
	    {
	       /* OK, we are trying to determine the correct flux   */
	       /* for output pixel k in output line l.  We have all */
	       /* the input lines we need (modulo looping back from */
	       /* indata[ibuffer])                                  */

	       outdata[k] = nan;

	       obegin =  (double)k     * xfactor;
	       oend   = ((double)k+1.) * xfactor;

	       imin = floor(obegin);
	       imax = ceil (oend);

	       if(debug >= 3)
	       {
		  printf("\nimin = %4d, imax = %4d, jmin = %4d, jmax = %4d\n", imin, imax, jmin, jmax);
		  fflush(stdout);
	       }

	       flux = 0;
	       area = 0;

	       for(ii=imin; ii<=imax; ++ii)
	       {
		  colfact[ii-imin] = 1.;

		       if(ii <= obegin && ii+1 <= oend) colfact[ii-imin] = ii+1. - obegin;
		  else if(ii <= obegin && ii+1 >= oend) colfact[ii-imin] = oend - obegin;
		  else if(ii >= obegin && ii+1 >= oend) colfact[ii-imin] = oend - ii;

		  if(colfact[ii-imin] < 0.)
		     colfact[ii-imin] = 0.;
	       }

	       for(jj=jmin; jj<=jmax; ++jj)
	       {
		  if(rowfact[jj-jmin] == 0.)
		     continue;

		  for(ii=imin; ii<=imax; ++ii)
		  {
		     bufrow = (ibuffer - jmax + jj + nbuf) % nbuf;

		     if(!mNaN(indata[bufrow][ii]) && colfact[ii-imin] > 0.)
		     {
			flux += indata[bufrow][ii] * colfact[ii-imin] * rowfact[jj-jmin];
			area += colfact[ii-imin] * rowfact[jj-jmin];

			if(debug >= 3)
			{
			   printf("output[%d][%d] -> %10.2e (area: %10.2e) (using indata[%d][%d] = %10.2e, colfact[%d-%d] = %5.3f, rowfact[%d-%d] = %5.3f)\n", 
			      l, k, flux, area,
			      bufrow, ii, indata[bufrow][ii], 
			      ii, imin, colfact[ii-imin],
			      jj, jmin, rowfact[jj-jmin]);

			   fflush(stdout);
			}
		     }
		  }

		  if(debug >= 3)
		  {
		     printf("---\n");
		     fflush(stdout);
		  }
	       }

	       if(area > 0.)
		  outdata[k] = flux/area;
	       else
		  outdata[k] = nan;

	       if(debug >= 3)
	       {
		  printf("\nflux = %-g / area = %-g --> outdata[%d] = %-g\n",
		     flux, area, k, outdata[k]);
		  
		  fflush(stdout);
	       }
	    }

	    if(fpixelo[1] <= output.naxes[1])
	    {
	       if(debug >= 2)
	       {
		  printf("\nWRITE output image row %5ld\n===========================================\n", fpixelo[1]);
		  fflush(stdout);
	       }

	       if (fits_write_pix(output.fptr, TDOUBLE, fpixelo, nelementso, 
				  (void *)(outdata), &status))
		  printFitsError(status);
	       }

	    ++fpixelo[1];

	    ++l;

	    obegin =  (double)l     * xfactor;
	    oend   = ((double)l+1.) * xfactor;

	    jmin = floor(obegin);
	    jmax = ceil (oend);

	    for(jj=jmin; jj<=jmax; ++jj)
	    {
	       rowfact[jj-jmin] = 1.;

		    if(jj <= obegin && jj+1 <= oend) rowfact[jj-jmin] = jj+1. - obegin;
	       else if(jj <= obegin && jj+1 >= oend) rowfact[jj-jmin] = oend - obegin;
	       else if(jj >= obegin && jj+1 >= oend) rowfact[jj-jmin] = oend - jj;

	       if(rowfact[jj-jmin] < 0.)
		  rowfact[jj-jmin] = 0.;

	       if(debug >= 4)
	       {
		  printf("rowfact[%d-%d] -> %-g\n", jj, jmin, rowfact[jj-jmin]);
		  fflush(stdout);
	       }
	    }
	 }

	 ibuffer = (ibuffer + 1) % nbuf;
      }
   }


   /*******************/
   /* Close the files */
   /*******************/

   if(fits_close_file(input.fptr, &status))
      printFitsError(status);

   if(fits_close_file(output.fptr, &status))
      printFitsError(status);           

   if(debug >= 1)
   {
      printf("FITS data image finalized\n"); 
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



/**************************************/
/*                                    */
/*  Open a FITS file and extract the  */
/*  pertinent header information.     */
/*                                    */
/**************************************/

int readFits(char *fluxfile)
{
   int      status;
   long     naxis1, naxis2, bitpix;
   char     ctype1[32], ctype2[32];
   double   crval1, crval2;
   double   crpix1, crpix2;
   double   cnpix1, cnpix2;
   double   xpixelsz, ypixelsz;
   double   ppo3, ppo6;
   double   cdelt1, cdelt2;
   double   crota2;
   double   cd11;
   double   cd12;
   double   cd21;
   double   cd22;
   double   pc11;
   double   pc12;
   double   pc21;
   double   pc22;
   double   epoch;
   double   equinox;

   char     bunit[80];

   char    msg [1024];

   status = 0;

   haveCtype   = 1;
   haveCrval   = 1;
   haveCrpix   = 1;
   haveCnpix   = 1;
   havePixelsz = 1;
   havePP      = 1;
   haveCdelt   = 1;
   haveCrota2  = 1;
   haveCD11    = 1;
   haveCD12    = 1;
   haveCD21    = 1;
   haveCD22    = 1;
   havePC11    = 1;
   havePC12    = 1;
   havePC21    = 1;
   havePC22    = 1;
   haveEpoch   = 1;
   haveEquinox = 1;
   haveBunit   = 1;

   input.cdelt1  = 0;
   input.cdelt2  = 0;
   input.crota2  = 0;
   input.cd11    = 0;
   input.cd12    = 0;
   input.cd21    = 0;
   input.cd22    = 0;
   input.pc11    = 0;
   input.pc12    = 0;
   input.pc21    = 0;
   input.pc22    = 0;
   input.epoch   = 0;
   input.equinox = 0;

   strcpy(input.bunit, "");

   if(fits_open_file(&input.fptr, fluxfile, READONLY, &status))
   {
      sprintf(msg, "Image file %s missing or invalid FITS", fluxfile);
      printError(msg);
   }

   if(hdu > 0)
   {
      if(fits_movabs_hdu(input.fptr, hdu+1, NULL, &status))
         printFitsError(status);
   }

   status = 0;
   if(fits_read_key(input.fptr, TLONG, "BITPIX", &bitpix, (char *)NULL, &status))
      printFitsError(status);

   input.bitpix = bitpix;

   status = 0;
   if(fits_read_key(input.fptr, TLONG, "NAXIS1", &naxis1, (char *)NULL, &status))
      printFitsError(status);

   status = 0;
   if(fits_read_key(input.fptr, TLONG, "NAXIS2", &naxis2, (char *)NULL, &status))
      printFitsError(status);
   
   input.naxes[0] = naxis1;
   input.naxes[1] = naxis2;

   status = 0;
   fits_read_key(input.fptr, TSTRING, "CTYPE1", ctype1, (char *)NULL, &status);
   if(status == KEY_NO_EXIST)
      haveCtype = 0;
   else strcpy(input.ctype1, ctype1);

   status = 0;
   fits_read_key(input.fptr, TSTRING, "CTYPE2", ctype2, (char *)NULL, &status);
   if(status == KEY_NO_EXIST)
      haveCtype = 0;
   else strcpy(input.ctype2, ctype2);

   status = 0;
   fits_read_key(input.fptr, TDOUBLE, "CRVAL1", &crval1, (char *)NULL, &status);
   if(status == KEY_NO_EXIST)
      haveCrval = 0;
   else input.crval1 = crval1;

   status = 0;
   fits_read_key(input.fptr, TDOUBLE, "CRVAL2", &crval2, (char *)NULL, &status);
   if(status == KEY_NO_EXIST)
      haveCrval = 0;
   else input.crval2 = crval2;

   status = 0;
   fits_read_key(input.fptr, TDOUBLE, "CRPIX1", &crpix1, (char *)NULL, &status);
   if(status == KEY_NO_EXIST)
      haveCrpix = 0;
   else input.crpix1 = crpix1;

   status = 0;
   fits_read_key(input.fptr, TDOUBLE, "CRPIX2", &crpix2, (char *)NULL, &status);
   if(status == KEY_NO_EXIST)
      haveCrpix = 0;
   else input.crpix2 = crpix2;

   status = 0;
   fits_read_key(input.fptr, TDOUBLE, "CNPIX1", &cnpix1, (char *)NULL, &status);
   if(status == KEY_NO_EXIST)
      haveCnpix = 0;
   else input.cnpix1 = cnpix1;

   status = 0;
   fits_read_key(input.fptr, TDOUBLE, "CNPIX2", &cnpix2, (char *)NULL, &status);
   if(status == KEY_NO_EXIST)
      haveCnpix = 0;
   else input.cnpix2 = cnpix2;

   status = 0;
   fits_read_key(input.fptr, TDOUBLE, "XPIXELSZ", &xpixelsz, (char *)NULL, &status);
   if(status == KEY_NO_EXIST)
      havePixelsz = 0;
   else input.xpixelsz = xpixelsz;

   status = 0;
   fits_read_key(input.fptr, TDOUBLE, "YPIXELSZ", &ypixelsz, (char *)NULL, &status);
   if(status == KEY_NO_EXIST)
      havePixelsz = 0;
   else input.ypixelsz = ypixelsz;

   status = 0;
   fits_read_key(input.fptr, TDOUBLE, "PPO3", &ppo3, (char *)NULL, &status);
   if(status == KEY_NO_EXIST)
      havePP = 0;
   else input.ppo3 = ppo3;

   status = 0;
   fits_read_key(input.fptr, TDOUBLE, "PPO6", &ppo6, (char *)NULL, &status);
   if(status == KEY_NO_EXIST)
      havePP = 0;
   else input.ppo6 = ppo6;

   status = 0;
   fits_read_key(input.fptr, TDOUBLE, "CDELT1", &cdelt1, (char *)NULL, &status);
   if(status == KEY_NO_EXIST)
      haveCdelt = 0;
   else input.cdelt1 = cdelt1;

   status = 0;
   fits_read_key(input.fptr, TDOUBLE, "CDELT2", &cdelt2, (char *)NULL, &status);
   if(status == KEY_NO_EXIST)
      haveCdelt = 0;
   else input.cdelt2 = cdelt2;

   status = 0;
   fits_read_key(input.fptr, TDOUBLE, "CD1_1", &cd11, (char *)NULL, &status);
   if(status == KEY_NO_EXIST)
      haveCD11 = 0;
   else input.cd11 = cd11;

   status = 0;
   fits_read_key(input.fptr, TDOUBLE, "CD1_2", &cd12, (char *)NULL, &status);
   if(status == KEY_NO_EXIST)
      haveCD12 = 0;
   else input.cd12 = cd12;

   status = 0;
   fits_read_key(input.fptr, TDOUBLE, "CD2_1", &cd21, (char *)NULL, &status);
   if(status == KEY_NO_EXIST)
      haveCD21 = 0;
   else input.cd21 = cd21;

   status = 0;
   fits_read_key(input.fptr, TDOUBLE, "CD2_2", &cd22, (char *)NULL, &status);
   if(status == KEY_NO_EXIST)
      haveCD22 = 0;
   else input.cd22 = cd22;

   status = 0;
   fits_read_key(input.fptr, TDOUBLE, "PC1_1", &pc11, (char *)NULL, &status);
   if(status == KEY_NO_EXIST)
      havePC11 = 0;
   else input.pc11 = pc11;

   status = 0;
   fits_read_key(input.fptr, TDOUBLE, "PC1_2", &pc12, (char *)NULL, &status);
   if(status == KEY_NO_EXIST)
      havePC12 = 0;
   else input.pc12 = pc12;

   status = 0;
   fits_read_key(input.fptr, TDOUBLE, "PC2_1", &pc21, (char *)NULL, &status);
   if(status == KEY_NO_EXIST)
      havePC21 = 0;
   else input.pc21 = pc21;

   status = 0;
   fits_read_key(input.fptr, TDOUBLE, "PC2_2", &pc22, (char *)NULL, &status);
   if(status == KEY_NO_EXIST)
      havePC22 = 0;
   else input.pc22 = pc22;

   status = 0;
   fits_read_key(input.fptr, TDOUBLE, "CROTA2", &crota2, (char *)NULL, &status);
   if(status == KEY_NO_EXIST)
      haveCrota2 = 0;
   else input.crota2 = crota2;

   status = 0;
   fits_read_key(input.fptr, TDOUBLE, "EPOCH", &epoch, (char *)NULL, &status);
   if(status == KEY_NO_EXIST)
      haveEpoch = 0;
   else input.epoch = epoch;

   status = 0;
   fits_read_key(input.fptr, TDOUBLE, "EQUINOX", &equinox, (char *)NULL, &status);
   if(status == KEY_NO_EXIST)
      haveEquinox = 0;
   else input.equinox = equinox;

   status = 0;
   fits_read_key(input.fptr, TSTRING, "BUNIT", bunit, (char *)NULL, &status);
   if(status == KEY_NO_EXIST)
      haveBunit = 0;
   else strcpy(input.bunit, bunit);

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

   fprintf(fstatus, "[struct stat=\"ERROR\", flag=%d, msg=\"%s\"]\n", status, status_str);

   exit(1);
}



/******************************/
/*                            */
/*  Print out general errors  */
/*                            */
/******************************/

void printError(char *msg)
{
   fprintf(stderr, "ERROR: %s\n", msg);
   fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"%s\"]\n", msg);
   exit(1);
}
