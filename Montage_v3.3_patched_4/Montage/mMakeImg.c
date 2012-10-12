/* Module: mMakeImg.c

Version  Developer        Date     Change
-------  ---------------  -------  -----------------------
1.4      John Good        20Jul07  Add checks for 'short' image sides
1.3      John Good        24Jun07  Need fix for CAR offset problem 
1.2      John Good        13Oct06  Add 'region' and 'replace' modes
1.1      John Good        13Oct04  Changed format for printing time
1.0      John Good        11Sep03  Baseline code

*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>

#include "fitsio.h"
#include "coord.h"
#include "wcs.h"
#include "mtbl.h"

#define MAXSTR  256
#define MAXFILE 256


void   printFitsError(int);
void   printError    (char *);
int    readTemplate  (char *filename);
int    parseLine     (char *line);
double ltqnorm       (double);
void   fixxy         (double *x, double *y, int *offscl);


char  *tval(int);

int  debug;

struct
{
   fitsfile         *fptr;
   long              naxes[2];
   struct WorldCoor *wcs;
   int               sys;
   double            epoch;
}
   output;

double pixscale;

double xcorrection;
double ycorrection;

static time_t currtime, start;

typedef struct vec
{
   double x;
   double y;
   double z;
}
Vec;

int     Cross    (Vec *a, Vec *b, Vec *c);
double  Dot      (Vec *a, Vec *b);
double  Normalize(Vec *a);
void    Reverse  (Vec *a);


/******************************************************************/
/*                                                                */
/*  mMakeImg -- A point source image generation program           */
/*                                                                */
/*  A general output FITS image is defined and its pixels are     */
/*  then populated from a table of point sources.  The source     */
/*  fluxes from the table are distributed based on a              */
/*  source-specific point-spread function.                        */
/*                                                                */
/******************************************************************/

int main(int argc, char **argv)
{
   int       i, j, jnext, k, l, m, count, ncol;
   int       dl, dm, wraparound, inext;
   int       loncol, latcol, fluxcol;
   int       haveTemplate, haveOut, region, replace;
   long      fpixel[4], nelements;
   double    oxpix, oypix;
   int       index, ifile, offscl;
   int       tblSys;
   double    dx, dy, dist2, width2, weight;
   double    noise_level, randnum;
   double    x, y, background;

   double    tolerance  = 0.0000277778;
   int       nShortSide;
   int       shortSide[4];

   double    ilon, ilat;
   double    olon, olat;
   double    ref_lon[5], ref_lat[5];
   double    xref[5], yref[5], zref[5];
   double    xarc, yarc, zarc;

   double    pixel_value;
   double    bg1, bg2, bg3, bg4;
   double    noise;

   double    theta0, theta, alpha, a, A;

   double  **data;

   int       status = 0;

   char      template_file[MAXSTR];
   char      output_file  [MAXSTR];
   char      table_file   [MAXSTR][MAXFILE];
   char      image_file   [MAXSTR][MAXFILE];
   char      colname      [MAXSTR][MAXFILE];
   double    width        [MAXFILE];
   double    tblEpoch     [MAXFILE];
   double    refmag       [MAXFILE];

   int       nfile  = 0;
   int       nimage = 0;

   int       bitpix = DOUBLE_IMG; 
   long      naxis  = 2;  

   double    imin, imax;
   double    jmin, jmax, jdiff;
   double    xpos, ypos;
   double    ix, iy;
   double    ixtest, iytest;

   int       ira [5];
   int       idec[5];
   double    ra  [5];
   double    dec [5];
   double    ipix;
   double    jpix;

   double    d2, maxd2, radius;

   int       npix;

   Vec       image_corner[4];
   Vec       image_normal[4];

   Vec       normal1, normal2, direction, pixel_loc;

   int       clockwise, interior;

   double    dtr;


   dtr = atan(1.)/45.;

   time(&start);


   /***************************************/
   /* Process the command-line parameters */
   /***************************************/

   if(argc < 3)
   {
      printf("[struct stat=\"ERROR\", msg=\"Usage: %s [-d level] [-r(eplace)] [-n noise_level] [-b bg1 bg2 bg3 bg4] [-t tbl col width epoch mag] template.hdr out.fits (-t args can be repeated)\"]\n", argv[0]);
      exit(1);
   }

   debug = 0;

   haveTemplate = 0;
   haveOut      = 0;

   bg1     = 0.;
   bg2     = 0.;
   bg3     = 0.;
   bg4     = 0.;

   noise   = 0.;

   index   = 1;
   region  = 0;
   replace = 0;

   while(1)
   {
      if(index >= argc)
	 break;

      if(argv[index][0] == '-')
      {
	 if(strlen(argv[index]) < 2)
	 {
	    printf("[struct stat=\"ERROR\", msg=\"Usage: %s [-d level] [-r(eplace)] [-n noise_level] [-b bg1 bg2 bg3 bg4] [-t tbl col width epoch mag] template.hdr out.fits (-t args can be repeated)\"]\n", argv[0]);
	    exit(1);
	 }

	 switch(argv[index][1])
	 {
	    case 'd':
	       if(argc < index+2)
	       {
		  printf("[struct stat=\"ERROR\", msg=\"Usage: %s [-d level] [-r(eplace)] [-n noise_level] [-b bg1 bg2 bg3 bg4] [-t tbl col width epoch mag] template.hdr out.fits (-t args can be repeated)\"]\n", argv[0]);
		  exit(1);
	       }

	       debug = atoi(argv[index+1]);

	       index += 2;

	       break;


	    case 'r':
	       replace = 1;

	       ++index;

	       break;


	    case 'f':
	       region = 1;

	       ++index;

	       break;


	    case 'n':
	       if(argc < index+2)
	       {
		  printf("[struct stat=\"ERROR\", msg=\"Usage: %s [-d level] [-r(eplace)] [-n noise_level] [-b bg1 bg2 bg3 bg4] [-t tbl col width epoch mag] template.hdr out.fits (-t args can be repeated)\"]\n", argv[0]);
		  exit(1);
	       }

	       noise = atof(argv[index+1]);

	       index += 2;

	       break;


	    case 'b':
	       if(argc < index+5)
	       {
		  printf("[struct stat=\"ERROR\", msg=\"Usage: %s [-d level] [-r(eplace)] [-n noise_level] [-b bg1 bg2 bg3 bg4] [-t tbl col width epoch mag] template.hdr out.fits (-t args can be repeated)\"]\n", argv[0]);
		  exit(1);
	       }

	       bg1 = atof(argv[index+1]);
	       bg2 = atof(argv[index+2]);
	       bg3 = atof(argv[index+3]);
	       bg4 = atof(argv[index+4]);

	       index += 5;

	       break;


	    case 't':
	       if(argc < index+6)
	       {
		  printf("[struct stat=\"ERROR\", msg=\"Usage: %s [-d level] [-r(eplace)] [-n noise_level] [-b bg1 bg2 bg3 bg4] [-t tbl col width epoch mag] template.hdr out.fits (-t args can be repeated)\"]\n", argv[0]);
		  exit(1);
	       }

	       strcpy(table_file[nfile], argv[index+1]);
	       strcpy(colname   [nfile], argv[index+2]);

	       width   [nfile] = atof(argv[index+3]);

               if(region)
	          width[nfile] = width[nfile]/3.;
          
	       tblEpoch[nfile] = atof(argv[index+4]);
	       refmag  [nfile] = atof(argv[index+5]);

	       index += 6;

	       ++nfile;

	       break;
	       


	    case 'i':
	       if(argc < index+2)
	       {
		  printf("[struct stat=\"ERROR\", msg=\"Usage: %s [-d level] [-r(eplace)] [-n noise_level] [-b bg1 bg2 bg3 bg4] [-t tbl col width epoch mag] template.hdr out.fits (-t args can be repeated)\"]\n", argv[0]);
		  exit(1);
	       }

	       strcpy(image_file[nfile], argv[index+1]);

	       index += 2;

	       ++nimage;

	       break;
	       
	    default:
	       break;
	 }
      }

      else if(!haveTemplate)
      {
	 strcpy(template_file, argv[index]);
	 ++index;
	 haveTemplate = 1;
      }

      else if(!haveOut)
      {
	 strcpy(output_file, argv[index]);
	 ++index;
	 haveOut = 1;
      }

      else
      {
	 printf("[struct stat=\"ERROR\", msg=\"Usage: %s [-d level] [-r(eplace)] [-n noise_level] [-b bg1 bg2 bg3 bg4] [-t tbl col width epoch mag] template.hdr out.fits (-t args can be repeated)\"]\n", argv[0]);
	 exit(1);
      }
   }

   if(debug >= 1)
   {
      time(&currtime);
      printf("Done gathering arguments (%.0f seconds)\n", 
	 (double)(currtime - start));
      fflush(stdout);

      printf("debug         = %d\n",  debug);
      printf("noise         = %-g\n", noise);
      printf("bg1           = %-g\n", bg1);
      printf("bg2           = %-g\n", bg2);
      printf("bg3           = %-g\n", bg3);
      printf("bg4           = %-g\n", bg4);
      printf("template_file = %s\n",  template_file);
      printf("output_file   = %s\n",  output_file);
      fflush(stdout);

      for(ifile=0; ifile<nfile; ++ifile)
      {
	 printf("table_file[%d] = %s\n",   ifile, table_file[ifile]);
	 printf("colname   [%d] = %s\n",   ifile, colname   [ifile]);
	 printf("width     [%d] = %-g\n",  ifile, width     [ifile]);
	 printf("tblEpoch  [%d] = %-g\n",  ifile, tblEpoch  [ifile]);
	 printf("refmag    [%d] = %-g\n",  ifile, refmag    [ifile]);
	 fflush(stdout);
      }

      for(ifile=0; ifile<nimage; ++ifile)
      {
	 printf("image_file[%d] = %s\n",   ifile, image_file[ifile]);
	 fflush(stdout);
      }
   }

   if(!haveOut)
   {
      printf("[struct stat=\"ERROR\", msg=\"Usage: %s [-d level] [-r(eplace)] [-n noise_level] [-b bg1 bg2 bg3 bg4] [-t tbl col width epoch mag] template.hdr out.fits (-t args can be repeated)\"]\n", argv[0]);
      exit(1);
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
      printf("output.sys      =  %d\n",  output.sys);
      printf("output.epoch    =  %-g\n", output.epoch);
      printf("output proj     =  %s\n",  output.wcs->ptype);
      printf("output crval[0] =  %-g\n", output.wcs->crval[0]);
      printf("output crval[1] =  %-g\n", output.wcs->crval[1]);
      printf("output crpix[0] =  %-g\n", output.wcs->crpix[0]);
      printf("output crpix[1] =  %-g\n", output.wcs->crpix[1]);
      printf("output cdelt[0] =  %-g\n", output.wcs->cdelt[0]);
      printf("output cdelt[1] =  %-g\n", output.wcs->cdelt[1]);

      fflush(stdout);
   }


   /***********************************************/ 
   /* Allocate memory for the output image pixels */ 
   /***********************************************/ 

   data = (double **)malloc(output.naxes[1] * sizeof(double *));

   data[0] = (double *)malloc(output.naxes[0] * output.naxes[1] * sizeof(double));

   if(debug >= 1)
   {
      printf("%ld bytes allocated for image pixels\n", 
	 output.naxes[0] * output.naxes[1] * sizeof(double));
      fflush(stdout);
   }


   /**********************************************************/
   /* Initialize pointers to the start of each row of pixels */
   /**********************************************************/

   for(i=1; i<output.naxes[1]; i++)
      data[i] = data[i-1] + output.naxes[0];

   if(debug >= 1)
   {
      printf("pixel line pointers populated\n"); 
      fflush(stdout);
   }


   /**************************************************/
   /* Initialize the data with background plus noise */
   /**************************************************/

   for (j=0; j<output.naxes[1]; ++j)
   {
      for (i=0; i<output.naxes[0]; ++i)
      {
	 x = i / (output.naxes[0] - 1.);
	 y = j / (output.naxes[1] - 1.);

	 background = bg1 * (x * y)
	            + bg2 * y * (1 - x)
	            + bg3 * x * (1 - y)
	            + bg4 * (1 - x) * (1 - y);

	 if(noise > 0.)
	 {
	    randnum = ((double)rand())/RAND_MAX; 

	    noise_level = ltqnorm(randnum) * noise;
	 }
	 else
	    noise_level = 0.;

	 data[j][i] = background + noise_level;

      }
   }


   /************************/
   /* Create the FITS file */
   /************************/

   remove(output_file);               

   if(fits_create_file(&output.fptr, output_file, &status)) 
      printFitsError(status);           


   /*********************************************************/
   /* Create the FITS image.  All the required keywords are */
   /* handled automatically.                                */
   /*********************************************************/

   if (fits_create_img(output.fptr, bitpix, naxis, output.naxes, &status))
      printFitsError(status);          

   if(debug >= 1)
   {
      printf("FITS image created (not yet populated)\n"); 
      fflush(stdout);
   }


   /*****************************/
   /* Loop over the table files */
   /*****************************/

   count = 0;

   for(ifile=0; ifile<nfile; ++ifile)
   {
      /************************************************/ 
      /* Open the table file and find the data column */ 
      /************************************************/ 

      width2 = width[ifile] * width[ifile] / 4.;

      ncol = topen(table_file[ifile]);

      if(ncol <= 0)
      {
	 printf("[struct stat=\"ERROR\", msg=\"Can't open table table %s\"]\n",
	    table_file[ifile]);
	 exit(1);
      }

      tblSys = EQUJ;

      loncol  = tcol("ra");
      latcol  = tcol("dec");

      if(loncol <  0 || latcol <  0)
      {
	 tblSys = GAL;

	 loncol  = tcol("glon");
	 latcol  = tcol("glat");

	 if(loncol <  0 || latcol <  0)
	 {
	    tblSys = ECLJ;

	    loncol  = tcol("elon");
	    latcol  = tcol("elat");

	    if(loncol <  0 || latcol <  0)
	    {
	       printf("[struct stat=\"ERROR\", msg=\"Can't find lon, lat columns\"]\n");

	       exit(1);
	    }
	 }
      }

      fluxcol = tcol(colname[ifile]);


      /*******************/
      /* For each source */
      /*******************/

      while(tread() >= 0)
      {
	 ++count;

	 ilon = atof(tval(loncol));
	 ilat = atof(tval(latcol));

	 convertCoordinates(    tblSys, tblEpoch[ifile],  ilon,  ilat,
			    output.sys,    output.epoch, &olon, &olat, 0.);

	 if(fluxcol < 0)
	    pixel_value = 1.;
	 else
	 {
	    pixel_value = atof(tval(fluxcol));

	    if(refmag[ifile] > 0.)
	       pixel_value = pow(10., 0.4 * (refmag[ifile] - pixel_value));
	 }

         offscl = 0;

	 wcs2pix(output.wcs, olon, olat, &oxpix, &oypix, &offscl);

         fixxy(&oxpix, &oypix, &offscl);

	 if(debug >= 2)
	 {
	    printf(" value = %11.3e at coord = (%12.8f,%12.8f) -> (%12.8f,%12.8f)",
	       pixel_value, ilon, ilat, olon, olat);

	    if(offscl)
	    {
	       printf(" -> opix = (%7.1f,%7.1f) OFF SCALE",
		  oxpix, oypix);
	       fflush(stdout);
	    }
	    else
	    {
	       printf(" -> opix = (%7.1f,%7.1f)",
		  oxpix, oypix);
	    }

	    if(debug == 2)
	       printf("\r");
	    else
	       printf("\n");

	    fflush(stdout);
	 }

	 if(!offscl)
	 {
	    l = (int)(oxpix + 0.5) - 1;
	    m = (int)(oypix + 0.5) - 1;

	    if(l < 0 || m < 0 || l >= output.naxes[0] || m >= output.naxes[1])
	    {
	       printf("[struct stat=\"INFO\", msg=\"Bad Values: l=%d, m=%d\"]\n", l, m);
	       fflush(stdout);
	    }

	    else
	    {
	       for(dl=-3*width[ifile]; dl<=3*width[ifile]; ++dl)
	       {
		  if(l+dl < 0 || l+dl>= output.naxes[0])
		     continue;

		  dx = oxpix - (l+dl);

		  for(dm=-3*width[ifile]; dm<=3*width[ifile]; ++dm)
		  {
		     if(m+dm < 0 || m+dm>= output.naxes[1])
			continue;

		     dy = oypix - (m+dm);

		     dist2 = dx*dx + dy*dy;

                     if(region)
                        weight = 1;
                     else
		        weight = exp(-dist2/width2);

                     
		     if(replace)
                        data[m+dm][l+dl] = weight * pixel_value;
                     else
		        data[m+dm][l+dl] += weight * pixel_value;
		  }
	       }
	    }
	 }
      }

      tclose();
   }

   if(debug >= 1)
   {
      time(&currtime);
      printf("Done processing tables (%.0f seconds)\n", 
	 (double)(currtime - start));
      fflush(stdout);
   }


   /*****************************/
   /* Loop over the image files */
   /*****************************/

   count = 0;

   for(ifile=0; ifile<nimage; ++ifile)
   {
      /******************************************************/ 
      /* Open the image table file and find the data column */ 
      /******************************************************/ 

      if(debug > 2)
      {
         printf("Image file[%d] =\"%s\"\n", ifile, image_file[ifile]);
	 fflush(stdout);
      }

      ncol = topen(image_file[ifile]);

      if(ncol <= 0)
      {
	 printf("[struct stat=\"ERROR\", msg=\"Can't open table table %s\"]\n",
	    table_file[ifile]);
	 exit(1);
      }

      ira [0] = tcol("ra");
      idec[0] = tcol("dec");

      ira [1] = tcol("ra1");
      idec[1] = tcol("dec1");

      ira [2] = tcol("ra2");
      idec[2] = tcol("dec2");

      ira [3] = tcol("ra3");
      idec[3] = tcol("dec3");

      ira [4] = tcol("ra4");
      idec[4] = tcol("dec4");

      if(ira[0] <  0 || idec[0] <  0
      || ira[1] <  0 || idec[1] <  0
      || ira[2] <  0 || idec[2] <  0
      || ira[3] <  0 || idec[3] <  0
      || ira[4] <  0 || idec[4] <  0)
      {
	 printf("[struct stat=\"ERROR\", msg=\"Can't find image center or four corners\"]\n");
	 fflush(stdout);

	 exit(1);
      }


      /******************/
      /* For each image */
      /******************/

      while(tread() >= 0)
      {
	 ++count;

	 if(debug >= 3)
	 {
	    printf("\nImage %d:\n", count);
	    fflush(stdout);
	 }

	 if(tnull(ira [0])) continue;
	 if(tnull(idec[0])) continue;
	 if(tnull(ira [1])) continue;
	 if(tnull(idec[1])) continue;
	 if(tnull(ira [2])) continue;
	 if(tnull(idec[2])) continue;
	 if(tnull(ira [3])) continue;
	 if(tnull(idec[3])) continue;
	 if(tnull(ira [4])) continue;
	 if(tnull(idec[4])) continue;

	 /****************************************/
	 /* Process the center and  four corners */
	 /* to find the min/max pixel coverage   */                     
	 /****************************************/

	 imin =  100000000;
	 imax = -100000000;
	 jmin =  100000000;
	 jmax = -100000000;

	 for(i=0; i<5; ++i)
	 {
	    ra [i] = atof(tval(ira [i]));
	    dec[i] = atof(tval(idec[i]));

	    convertCoordinates(EQUJ, 2000., ra[i], dec[i],
			       output.sys, output.epoch, &ref_lon[i], &ref_lat[i], 0.);

	    xref[i] = cos(ref_lon[i]*dtr) * cos(ref_lat[i]*dtr);
	    yref[i] = sin(ref_lon[i]*dtr) * cos(ref_lat[i]*dtr);
	    zref[i] = sin(ref_lat[i]*dtr);
	 }

	 nShortSide = 0;

	 for(i=1; i<5; ++i)
	 {
	    inext = i+1;

	    if(inext == 5)
	       inext = 1;

	    theta0 = acos(xref[i]*xref[inext] + yref[i]*yref[inext] + zref[i]*zref[inext])/dtr;

	    shortSide[i-1] = 0;

	    if(theta0 < tolerance)
	    {
	       if(debug >= 3)
	       {
		  printf("   Side %d: (%10.6f,%10.6f) -> (%10.6f,%10.6f) [theta0 = %10.6f, pixscale = %12.9f SHORT SIDE]\n", 
		     i, ra[i], dec[i], ra[inext], dec[inext], theta0, pixscale);
		  fflush(stdout);
	       }

	       ++nShortSide;
	       shortSide[i-1] = 1;
	       continue;
	    }

	    if(debug >= 3)
	    {
	       printf("   Side %d: (%10.6f,%10.6f) -> (%10.6f,%10.6f) [theta0 = %10.6f, pixscale = %12.9f]\n", 
		  i, ra[i], dec[i], ra[inext], dec[inext], theta0, pixscale);
	       fflush(stdout);
	    }

	    for(alpha=0; alpha<=theta0; alpha+=pixscale/2.)
	    {
	       theta = theta0/2. - alpha;

	       A = tan(theta*dtr) * cos(theta0/2.*dtr);

	       a = (sin(theta0/2.*dtr) - A)/(2.*sin(theta0/2.*dtr));

	       xarc = (1.-a) * xref[i] + a * xref[inext];
	       yarc = (1.-a) * yref[i] + a * yref[inext];
	       zarc = (1.-a) * zref[i] + a * zref[inext];

	       olon = atan2(yarc, xarc)/dtr;
	       olat = asin(zarc)/dtr;

	       offscl = 0;

	       wcs2pix(output.wcs, olon, olat, &oxpix, &oypix, &offscl);

	       fixxy(&oxpix, &oypix, &offscl);

	       if(debug >= 4)
	       {
		  printf("theta = %.6f -> A = %.6f -> a = %.6f -> (%.6f,%.6f,%.6f) -> (%12.8f,%12.8f)",
		     theta, A, a, xarc, yarc, zarc, olon, olat);

		  if(offscl)
		  {
		     printf(" -> opix = (%7.1f,%7.1f) OFF SCALE\n",
			oxpix, oypix);
		     fflush(stdout);
		  }
		  else
		  {
		     printf(" -> opix = (%7.1f,%7.1f)\n",
			oxpix, oypix);
		     fflush(stdout);
		  }
	       }
	       
	       if(!offscl)
	       {
		  ipix = (int)(oxpix + 0.5);
		  jpix = (int)(oypix + 0.5);

		  if(ipix < 0 || jpix < 0 || ipix >= output.naxes[0] || jpix >= output.naxes[1])
		     offscl = 1;
		  else
		  {
		     if(ipix < imin) imin = ipix;
		     if(ipix > imax) imax = ipix;
		     if(jpix < jmin) jmin = jpix;
		     if(jpix > jmax) jmax = jpix;
		  }
	       }
	    }
	 }

	 if(debug >= 3)
	 {
	    printf("\n   Range:  i = %.2f -> %.2f   j= %.2f -> %.2f\n", 
	       imin, imax, jmin, jmax);
	    fflush(stdout);
	 }


	 /*****************************************/
	 /* Compute the image corners and normals */
	 /*****************************************/

	 for(i=0; i<4; ++i)
	 {
	    image_corner[i].x = cos(ra [i+1]*dtr) * cos(dec[i+1]*dtr);
	    image_corner[i].y = sin(ra [i+1]*dtr) * cos(dec[i+1]*dtr);
	    image_corner[i].z = sin(dec[i+1]*dtr);
	 }


	 /* Reverse if counterclockwise on the sky */

	 Cross(&image_corner[0], &image_corner[1], &normal1);
	 Cross(&image_corner[1], &image_corner[2], &normal2);
	 Cross(&normal1, &normal2, &direction);

         Normalize(&direction);

	 clockwise = 0;
	 if(Dot(&direction, &image_corner[1]) > 0.)
	    clockwise = 1;

	 if(!clockwise)
	 {
	    swap(&image_corner[0].x, &image_corner[3].x);
	    swap(&image_corner[0].y, &image_corner[3].y);
	    swap(&image_corner[0].z, &image_corner[3].z);

	    swap(&image_corner[1].x, &image_corner[2].x);
	    swap(&image_corner[1].y, &image_corner[2].y);
	    swap(&image_corner[1].z, &image_corner[2].z);
	 }

         for(j=0; j<4; ++j)
         {
            jnext = (j+1)%4;

            Cross(&image_corner[j], &image_corner[jnext], &image_normal[j]);

            Normalize(&image_normal[j]);
         }



	 /***************************************************************************/
	 /*                                                                         */
	 /* Normally, we can check the pixels between imin->imax and jmin->jmax     */
	 /* to see if they are inside the image.  There are two situations where    */
	 /* this fails, however.                                                    */
	 /*                                                                         */
	 /* First, in an all-sky projection, the image may include the pole, in     */
	 /* which case the latitude range should be extended to include it, even    */
	 /* though the pixel range from checking the region edge doesn't include    */
	 /* it.                                                                     */
	 /*                                                                         */
	 /* Second, the image may be so small compared to the pixel that checking   */
	 /* pixel centers may find none that are inside the image.  If this happens */
	 /* (no pixels are turned on for this image), we can force the pixel        */
	 /* associated with the image center on.                                    */
	 /*                                                                         */
	 /***************************************************************************/

	 /*********************************************/
	 /* Special check for inclusion of North pole */
	 /*********************************************/

	 pixel_loc.x = 0.;
	 pixel_loc.y = 0.;
	 pixel_loc.z = 1.;

	 interior = 1;

	 if(nShortSide == 4)
	    interior = 0;

	 for(k=0; k<4; ++k)
	 {
	    if(shortSide[k])
	       continue;

	    if(Dot(&image_normal[k], &pixel_loc) < 0)
	    {
	       interior = 0;
	       break;
	    }
	 }

	 if(interior)
	 {
	    offscl = 0;

	    wcs2pix(output.wcs, 0., 90., &oxpix, &oypix, &offscl);

	    fixxy(&oxpix, &oypix, &offscl);

	    if(offscl)
	    {
	       if(output.wcs->cdelt[1] > 0.)
	       {
		  jmax = output.naxes[0];

		  if(debug >= 3)
		  {
		     printf("\n   North pole in image:  jmax -> %.2f\n", jmax); 
		     fflush(stdout);
		  }
	       }
	       else
	       {
		  jmin = output.naxes[0];

		  if(debug >= 3)
		  {
		     printf("\n   North pole in image:  jmin -> %.2f\n", jmin); 
		     fflush(stdout);
		  }
	       }
	    }
	    else
	    {
	       if(oypix < jmin)
	       {
		  jmin = oypix;

		  if(debug >= 3)
		  {
		     printf("\n   North pole in image:  jmin -> %.2f\n", jmin); 
		     fflush(stdout);
		  }
	       }
	       else if(oypix > jmax)
	       {
		  jmax = oypix;

		  if(debug >= 3)
		  {
		     printf("\n   North pole in image:  jmax -> %.2f\n", jmax); 
		     fflush(stdout);
		  }
	       }
	       else
	       {
		  if(debug >= 3)
		  {
		     printf("\n   North pole in image:  no range change\n"); 
		     fflush(stdout);
		  }
	       }
	    }
	 }


	 /*********************************************/
	 /* Special check for inclusion of South pole */
	 /*********************************************/

	 pixel_loc.x =  0.;
	 pixel_loc.y =  0.;
	 pixel_loc.z = -1.;

	 interior = 1;

	 if(nShortSide == 4)
	    interior = 0;

	 for(k=0; k<4; ++k)
	 {
	    if(shortSide[k])
	       continue;

	    if(Dot(&image_normal[k], &pixel_loc) < 0)
	    {
	       interior = 0;
	       break;
	    }
	 }

	 if(interior)
	 {
	    offscl = 0;

	    wcs2pix(output.wcs, 0., -90., &oxpix, &oypix, &offscl);

	    fixxy(&oxpix, &oypix, &offscl);

	    if(offscl)
	    {
	       if(output.wcs->cdelt[1] > 0.)
	       {
		  jmax = 0.;

		  if(debug >= 3)
		  {
		     printf("\n   South pole in image:  jmax -> %.2f\n", jmax); 
		     fflush(stdout);
		  }
	       }
	       else
	       {
		  jmin = 0.;

		  if(debug >= 3)
		  {
		     printf("\n   South pole in image:  jmin -> %.2f\n", jmin); 
		     fflush(stdout);
		  }
	       }
	    }
	    else
	    {
	       if(oypix < jmin)
	       {
		  jmin = oypix;

		  if(debug >= 3)
		  {
		     printf("\n   South pole in image:  jmin -> %.2f\n", jmin); 
		     fflush(stdout);
		  }
	       }
	       else if(oypix > jmax)
	       {
		  jmax = oypix;

		  if(debug >= 3)
		  {
		     printf("\n   South pole in image:  jmax -> %.2f\n", jmax); 
		     fflush(stdout);
		  }
	       }
	       else
	       {
		  if(debug >= 3)
		  {
		     printf("\n   South pole in image:  no range change\n"); 
		     fflush(stdout);
		  }
	       }
	    }
	 }


	 /*******************************************/
	 /* Loop over the possible pixel range,     */
	 /* checking to see if that pixel is inside */                     
	 /* the image (turn it on if so)            */                     
	 /*******************************************/

	 npix = 0;

	 for(i=imin; i<=imax; ++i)
	 {
	    for(j=jmin; j<=jmax; ++j)
	    {
	       ix = i;
	       iy = j;

	       pix2wcs(output.wcs, ix, iy, &xpos, &ypos);
	 
	       convertCoordinates(output.sys, output.epoch,  xpos,  ypos,
				  EQUJ,       2000.,        &olon, &olat, 0.);

	       offscl = 0;

	       wcs2pix(output.wcs, xpos, ypos, &ixtest, &iytest, &offscl);

	       if(offscl
	       || fabs(ixtest - ix) > 0.01
	       || fabs(iytest - iy) > 0.01)
		  continue;

	       pixel_loc.x = cos(olon*dtr) * cos(olat*dtr);
	       pixel_loc.y = sin(olon*dtr) * cos(olat*dtr);
	       pixel_loc.z = sin(olat*dtr);

	       interior = 1;

	       if(nShortSide == 4)
		  interior = 0;

	       for(k=0; k<4; ++k)
	       {
		  if(shortSide[k])
		     continue;

		  if(Dot(&image_normal[k], &pixel_loc) < 0)
		  {
		     interior = 0;
		     break;
		  }
	       }

               if(debug >= 4)
               {
	          printf("%6d %6d -> %11.6f %11.6f -> %11.6f %11.6f (%d)\n",
		     i, j, xpos, ypos, olon, olat, interior);
		  fflush(stdout);
	       }

	       if(interior)
	       {
	          ++npix;

		  if(replace)
		     data[j][i]  = 1.;
		  else
		     data[j][i] += 1.;
	       }
	    }
	 }

	
	 /* Make sure small images have at least one pixel on */

	 if(npix == 0)
	 {
	    wcs2pix(output.wcs, ref_lon[0], ref_lat[0], &ipix, &jpix, &offscl);

	    fixxy(&ipix, &jpix, &offscl);

	    if(!offscl)
	    {
	       i = (int)(ipix);
	       j = (int)(jpix);

	       if(debug >= 4)
	       {
		  printf("Single pixel turn-on: %6d %6d\n", i, j);
		  fflush(stdout);
	       }

	       if(replace)
		  data[j][i]  = 1.;
	       else
		  data[j][i] += 1.;
	    }
         }
      }

      tclose();
   }

   if(debug >= 1)
   {
      time(&currtime);
      printf("Done processing tables (%.0f seconds)\n", 
	 (double)(currtime - start));
      fflush(stdout);
   }


   /************************/
   /* Write the image data */
   /************************/

   fpixel[0] = 1;
   fpixel[1] = 1;
   fpixel[2] = 1;
   fpixel[3] = 1;

   nelements = output.naxes[0];

   for(j=0; j<output.naxes[1]; ++j)
   {
      if (fits_write_pix(output.fptr, TDOUBLE, fpixel, nelements,
                         (void *)(data[j]), &status))
         printFitsError(status);

      ++fpixel[1];
   }

   if(debug >= 1)
   {
      printf("Data written to FITS data image\n");
      fflush(stdout);
   }


   /*************************************/
   /* Add keywords from a template file */
   /*************************************/

   if(fits_write_key_template(output.fptr, template_file, &status))
      printFitsError(status);           

   if(debug >= 1)
   {
      printf("Template keywords written to FITS image\n"); 
      fflush(stdout);
   }


   /***********************/
   /* Close the FITS file */
   /***********************/

   if(fits_close_file(output.fptr, &status))
      printFitsError(status);           

   if(debug >= 1)
   {
      printf("FITS image finalized\n"); 
      fflush(stdout);
   }

   printf("[struct stat=\"OK\"]\n");
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
   int       i;

   FILE     *fp;

   char      line[MAXSTR];

   char     *header[2];

   int       sys;
   double    epoch;

   double    x, y;
   double    ix, iy;
   double    xpos, ypos;
   int       offscl;

   header[0] = malloc(32768);
   header[1] = (char *)NULL;


   /********************************************************/
   /* Open the template file, read and parse all the lines */
   /********************************************************/

   fp = fopen(filename, "r");

   if(fp == (FILE *)NULL)
   {
      printf("[struct stat=\"ERROR\", msg=\"Template file [%s] not found.\"]\n",
         filename);
      exit(1);
   }

   while(1)
   {
      if(fgets(line, MAXSTR, fp) == (char *)NULL)
	 break;

      if(line[strlen(line)-1] == '\n')
         line[strlen(line)-1]  = '\0';

      if(debug >= 2)
      {
	 printf("Template line: [%s]\n", line);
	 fflush(stdout);
      }

      for(i=strlen(line); i<80; ++i)
	 line[i] = ' ';
      
      line[80] = '\0';

      strcat(header[0], line);

      parseLine(line);
   }

   if(debug >= 2)
   {
      printf("\nheader ----------------------------------------\n");
      printf("%s\n", header[0]);
      printf("-----------------------------------------------\n\n");
   }


   /****************************************/
   /* Initialize the WCS transform library */
   /****************************************/

   output.wcs = wcsinit(header[0]);

   if(output.wcs == (struct WorldCoor *)NULL)
   {
      printf("[struct stat=\"ERROR\", msg=\"Output wcsinit() failed. Exiting.\"]\n");
      exit(1);
   }

   pixscale = fabs(output.wcs->xinc);
   if(fabs(output.wcs->yinc) < pixscale)
      pixscale = fabs(output.wcs->xinc);


   /* Kludge to get around bug in WCS library:   */
   /* 360 degrees sometimes added to pixel coord */

   ix = (output.naxes[0] + 1.)/2.;
   iy = (output.naxes[1] + 1.)/2.;

   offscl = 0;

   pix2wcs(output.wcs, ix, iy, &xpos, &ypos);
   wcs2pix(output.wcs, xpos, ypos, &x, &y, &offscl);

   xcorrection = x-ix;
   ycorrection = y-iy;

   if(debug)
   {
      printf("DEBUG> xcorrection = %.2f\n", xcorrection);
      printf("DEBUG> ycorrection = %.2f\n\n", ycorrection);
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

   free(header[0]);

   return 0;
}



/**************************************************/
/*                                                */
/*  Parse header lines from the template,         */
/*  looking for NAXIS1 and NAXIS2                 */
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
      output.naxes[0] = atoi(value);

   if(strcmp(keyword, "NAXIS2") == 0)
      output.naxes[1] = atoi(value);

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

   printf("[struct stat=\"ERROR\", flag=%d, msg=\"%s\"]\n", status, status_str);

   exit(1);
}



/***************************************************/
/*                                                 */
/* swap()                                          */
/*                                                 */
/* Switches the values of two memory locations     */
/*                                                 */
/***************************************************/

int swap(double *x, double *y)
{
   double tmp;

   tmp = *x;
   *x  = *y;
   *y  = tmp;

   return(0);
}



/***************************************************/
/*                                                 */
/* Cross()                                         */
/*                                                 */
/* Vector cross product.                           */
/*                                                 */
/***************************************************/

int Cross(Vec *v1, Vec *v2, Vec *v3)
{
   v3->x =  v1->y*v2->z - v2->y*v1->z;
   v3->y = -v1->x*v2->z + v2->x*v1->z;
   v3->z =  v1->x*v2->y - v2->x*v1->y;

   if(v3->x == 0.
   && v3->y == 0.
   && v3->z == 0.)
      return 0;

   return 1;
}


/***************************************************/
/*                                                 */
/* Dot()                                           */
/*                                                 */
/* Vector dot product.                             */
/*                                                 */
/***************************************************/

double Dot(Vec *a, Vec *b)
{
   double sum = 0.0;

   sum = a->x * b->x
       + a->y * b->y
       + a->z * b->z;

   return sum;
}


/***************************************************/
/*                                                 */
/* Normalize()                                     */
/*                                                 */
/* Normalize the vector                            */
/*                                                 */
/***************************************************/

double Normalize(Vec *v)
{
   double len;

   len = 0.;

   len = sqrt(v->x * v->x + v->y * v->y + v->z * v->z);

   v->x = v->x / len;
   v->y = v->y / len;
   v->z = v->z / len;

   return len;
}
