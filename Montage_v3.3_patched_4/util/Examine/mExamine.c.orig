/* Module: mExamine.c

Version  Developer        Date     Change
-------  ---------------  -------  -----------------------
1.0      John Good        13Feb08  Baseline code

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

char  *input_header;

struct WorldCoor *wcs;

int  readFits      (char *fluxfile);
void printFitsError(int);
void printError    (char *);

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
   input;

int haveCDELT1;
int haveCDELT2;
int haveCROTA2;

int haveCD1_1;
int haveCD1_2;
int haveCD2_1;
int haveCD2_2;


/*****************************************************/
/*                                                   */
/*  mExamine                                         */
/*                                                   */
/*  Determine pixel statistics for a region on the   */
/*  sky in the given image.                          */
/*                                                   */
/*****************************************************/

int main(int argc, char **argv)
{
   int       i, j, nullcnt, status, lines;
   int       use, nnull, first, offscl, sys;
   int       ibegin, iend, jbegin, jend;
   char     *end;
   long      fpixel[4], nelements;
   double    epoch;
   double    ra, dec;
   double    lon, lat;
   double    xpix, ypix;
   double    radius;
   double    xoff, yoff;

   double  **data;

   double    flux;

   double    ramax, decmax;
   double    fluxmax;

   double    ramin, decmin;
   double    fluxmin;

   double    raref, decref;
   double    fluxref;

   double    fluxave;
   double    fluxsq;
   int       npixel;

   double    dist, distmax, distmin;
   double    xc, yc, zc;
   double    x, y, z;

   double    dtr;

   dtr = atan(1.)/45.;


   /***************************************/
   /* Process the command-line parameters */
   /***************************************/

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
   
   if (argc < 5) 
   {
      printf ("[struct stat=\"ERROR\", msg=\"Usage: mExamine [-d level] [-s statusfile] in.fits ra dec radius\"]\n");
      exit(1);
   }

   strcpy(input_file, argv[1]);

   if(input_file[0] == '-')
   {
      printf ("[struct stat=\"ERROR\", msg=\"Invalid input file '%s'\"]\n", input_file);
      exit(1);
   }

   ra  = strtod(argv[2], &end);

   if(end < argv[2] + (int)strlen(argv[2]))
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Center RA string (%s) cannot be interpreted as a real number\"]\n",
	 argv[3]);
      exit(1);
   }

   dec = strtod(argv[3], &end);

   if(end < argv[3] + (int)strlen(argv[3]))
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Center Dec string (%s) cannot be interpreted as a real number\"]\n",
	 argv[4]);
      exit(1);
   }

   radius = strtod(argv[4], &end);

   if(end < argv[4] + (int)strlen(argv[4]))
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Radius string (%s) cannot be interpreted as a real number\"]\n",
	 argv[5]);
      exit(1);
   }

   if(radius <= 0.)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Invalid region radius\"]\n");
      exit(1);
   }

   if(debug >= 1)
   {
      printf("debug      = %d\n",   debug);
      printf("input_file = [%s]\n", input_file);
      printf("ra         = %-g\n",  ra);
      printf("dec        = %-g\n",  dec);
      printf("radius     = %-g\n",  radius);

      fflush(stdout);
   }

   xc = cos(ra*dtr) * cos(dec*dtr);
   yc = sin(ra*dtr) * cos(dec*dtr);
   zc = sin(dec*dtr);

   if(debug >= 1)
   {
      printf("xc         = %-g\n",  xc);
      printf("yc         = %-g\n",  yc);
      printf("zc         = %-g\n",  zc);

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


   /**********************************/
   /* Find the pixel location of the */
   /* sky coordinate specified       */
   /**********************************/

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

   wcs2pix(wcs, lon, lat, &xpix, &ypix, &offscl);

   fixxy(&xpix, &ypix, &offscl);

   if(offscl)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Location is off image\"]\n");
      fflush(stdout);
      exit(1);
   }

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


   /***************************************/
   /* Find the range of pixels to analyze */
   /***************************************/

   xoff = fabs(radius/input.cdelt1);
   yoff = fabs(radius/input.cdelt2);

   distmax = radius;

   ibegin = xpix - 2.* xoff - 1.0;
   iend   = xpix + 2.* xoff + 1.0;

   jbegin = ypix - 2.* yoff - 1.0;
   jend   = ypix + 2.* yoff + 1.0;

   if(ibegin < 1             ) ibegin = 1;
   if(ibegin > input.naxes[0]) ibegin = input.naxes[0];
   if(iend   > input.naxes[0]) iend   = input.naxes[0];
   if(iend   < 0             ) iend   = input.naxes[0];

   if(jbegin < 1             ) jbegin = 1;
   if(jbegin > input.naxes[1]) jbegin = input.naxes[1];
   if(jend   > input.naxes[1]) jend   = input.naxes[1];
   if(jend   < 0             ) jend   = input.naxes[1];

   if(debug)
   {
      printf("xoff    = %-g\n", xoff);
      printf("yoff    = %-g\n", yoff);
      printf("ibegin  = %d\n",  ibegin);
      printf("iend    = %d\n",  iend);
      printf("jbegin  = %d\n",  jbegin);
      printf("jend    = %d\n",  jend);
      printf("distmax = %-g\n", distmax);
      fflush(stdout);
   }

   if(ibegin >= iend
   || jbegin >= jend)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"No pixels match output area.\"]\n");
      fflush(stdout);
      exit(1);
   }

   nelements =  iend -  ibegin + 1;
   lines     =  jend -  jbegin + 1;


   /**********************************************/ 
   /* Allocate memory for the input image pixels */ 
   /**********************************************/ 

   data = (double **)malloc(lines * sizeof(double *));

   for(j=0; j<lines; ++j)
      data[j] = (double *)malloc(nelements * sizeof(double));

   if(debug >= 1)
   {
      printf("%ld bytes allocated for input image pixels\n", 
	 nelements * lines * sizeof(double));
      fflush(stdout);
   }


   /************************/
   /* Read the input lines */
   /************************/

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

      if(fits_read_pix(input.fptr, TDOUBLE, fpixel, nelements, NULL,
		       (void *)data[j-jbegin], &nullcnt, &status))
	 printFitsError(status);
      
      ++fpixel[1];
   }


   /*****************************************************/
   /* Loop over the pixels, computing region parameters */
   /*****************************************************/

   first = 1;

   fluxave = 0.;
   fluxsq  = 0.;
   npixel  = 0;
   nnull   = 0;

   raref   = 0.;
   decref  = 0;
   fluxref = 0;
   distmin = 1.e50;

   for (j=jbegin; j<jend; ++j)
   {
      if(debug >= 2)
	 printf("\n");

      for(i=ibegin; i<iend; ++i)
      {
	 offscl = 0;

	 xpix = i;
	 ypix = j;

	 pix2wcs(wcs, xpix, ypix, &lon, &lat);

	 convertCoordinates(sys, epoch, lon, lat, EQUJ, 2000., &ra, &dec, 0.);
	 
	 x = cos(ra*dtr) * cos(dec*dtr);
	 y = sin(ra*dtr) * cos(dec*dtr);
	 z = sin(dec*dtr);

	 dist = acos(x*xc + y*yc + z*zc)/dtr;

	 use = 0;
	 if(dist < distmax)
	    use = 1;

	 flux = data[j-jbegin][i-ibegin];

	 if(debug >= 2)
	 {
	    printf("i=%d j=%d data=%.6f (%.6f,%.6f) %.2f %d\n",
	       i, j, flux, ra, dec, dist*3600., use);

	    fflush(stdout);
	 }

	 if(use && mNaN(flux))
	    ++nnull;

	 if(use && !mNaN(flux))
	 {
	    if(first)
	    {
	       ramax   = ra;
	       ramin   = ra;
	       decmax  = dec;
	       decmin  = dec;
	       fluxmax = flux;
	       fluxmin = flux;

	       first = 0;
	    }

	    if(flux > fluxmax)
	    {
	       ramax   = ra;
	       decmax  = dec;
	       fluxmax = flux;
	    }

	    if(flux < fluxmin)
	    {
	       ramin   = ra;
	       decmin  = dec;
	       fluxmin = flux;
	    }

	    fluxave += flux;

	    fluxsq  += flux * flux;

	    ++npixel;

	    if(dist < distmin)
	    {
	       raref   = ra;
	       decref  = dec;
	       fluxref = flux;

	       distmin = dist;
	    }
	 }
      }
   }

   if(npixel > 0)
   {
      fluxave = fluxave/npixel;

      fluxsq = sqrt(fluxsq/(double)npixel - fluxave*fluxave);
   }

   if(debug >= 1)
   {
      printf("Input image read complete.\n\n");
      fflush(stdout);
   }

   fprintf(fstatus, "[struct stat=\"OK\", fluxmin=%-g, ramin=%.6f, decmin=%.6f, fluxmax=%-g, ramax=%.6f, decmax=%.6f, fluxref=%-g, raref=%.6f, decref=%.6f, aveflux=%-g, rmsflux=%-g, npixel=%d, nnull=%d]\n",
      fluxmin, ramin, decmin,
      fluxmax, ramax, decmax,
      fluxref, raref, decref,
      fluxave, fluxsq, npixel, nnull);
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
