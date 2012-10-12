/* Module: mCatMap.c

Version  Developer        Date     Change
-------  ---------------  -------  -----------------------
1.2      John Good        24Jun07  Added correction for CAR projection error
1.1      John Good        05Aug06  Add image coverage capability
1.0      John Good        05Oct05  Baseline code

*/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <math.h>
#include <time.h>

#include <mtbl.h>
#include <fitsio.h>
#include <coord.h>
#include <wcs.h>

#define MAXSTR  256
#define MAXFILE 256

int    parseLine     (char *line);
int    readTemplate  (char *filename);
void   printFitsError(int);
void   printError    (char *);

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

static time_t currtime, start;

extern char *optarg;
extern int optind, opterr;

extern int getopt(int argc, char *const *argv, const char *options);

int  debugCheck (char *debugStr);
void fixxy      (double *x, double *y, int *offscl);

double xcorrection;
double ycorrection;


/******************************************************************/
/*                                                                */
/*  mCatMap -- A point source imaging program                     */
/*                                                                */
/*  A general output FITS image is defined and its pixels are     */
/*  then populated from a table of point sources.  The source     */
/*  fluxes (or just source counts) from the table are added       */
/*  in to the appropriate pixel.                                  */
/*                                                                */
/******************************************************************/

int main(int argc, char **argv)
{
   int       i, j, l, m, c, count, ismag, ncol;
   int       dl, dm, width, haveFlux, isImg, csys;
   int       side, ibegin, iend, nstep, useCenter;
   int       racol, deccol, fluxcol;
   int       ra1col, dec1col;
   int       ra2col, dec2col;
   int       ra3col, dec3col;
   int       ra4col, dec4col;
   long      fpixel, nelements;
   double    rac, decc;
   double    ra[4], dec[4];
   double    x,  y,  z;
   double    x0, y0, z0;
   double    x1, y1, z1;
   double    oxpix, oypix, equinox;
   int       offscl;
   double    ilon;
   double    ilat;
   double    pixscale, len, sideLength, dtr;
   double    offset;

   double    xn, yn, zn;
   double    ran, decn;
   double    sina, cosa;
   double    sind, cosd;

   double    a11, a12, a13;
   double    a21, a22, a23;
   double    a31, a32, a33;

   double    x0p, y0p, z0p;
   double    x1p, y1p, z1p;

   double    lon0, lon1;
   double    xp, yp;
   double    lon;

   double    pixel_value;

   double    weights[5][5];

   double    weights3[5][5] = {{0.0, 0.0, 0.0, 0.0, 0.0},
                               {0.0, 0.1, 0.2, 0.1, 0.0},
                               {0.0, 0.2, 1.0, 0.2, 0.0},
                               {0.0, 0.1, 0.2, 0.1, 0.0},
                               {0.0, 0.0, 0.0, 0.0, 0.0}};

   double    weights5[5][5] = {{0.0, 0.1, 0.2, 0.1, 0.0},
                               {0.1, 0.3, 0.5, 0.3, 0.1},
                               {0.2, 0.5, 1.0, 0.5, 0.2},
                               {0.1, 0.3, 0.5, 0.3, 0.1},
                               {0.0, 0.1, 0.2, 0.1, 0.0}};

   double  **data;

   int       status = 0;

   char      input_file   [MAXSTR];
   char      colname      [MAXSTR];
   char      output_file  [MAXSTR];
   char      template_file[MAXSTR];

   int       bitpix = DOUBLE_IMG; 
   long      naxis  = 2;  

   double    sumweights;
   double    refmag;

   dtr = atan(1.0)/45.;


   /***************************************/
   /* Process the command-line parameters */
   /***************************************/

   ismag  = 0;
   opterr = 0;

   useCenter = 0;

   strcpy(colname, "");

   while ((c = getopt(argc, argv, "pm:d:w:c:")) != EOF) 
   {
      switch (c) 
      {
         case 'm':
	    ismag = 1;
	    refmag = atof(optarg);
            break;

         case 'd':
            debug = debugCheck(optarg);
            break;

         case 'w':
	    width  = atoi(optarg);
            break;

         case 'c':
            strcpy(colname, optarg);
            break;

         case 'p':
            useCenter = 1;
            break;

         default:
	    printf("[struct stat=\"ERROR\", msg=\"Usage: %s [-c column][-m refmag][-d level][-w size] in.tbl out.fits hdr.template\"]\n", argv[0]);
            exit(1);
            break;
      }
   }

   if (argc - optind < 3) 
   {
      printf("[struct stat=\"ERROR\", msg=\"Usage: %s [-c column][-m refmag][-d level][-w size] in.tbl out.fits hdr.template\"]\n", argv[0]);
      exit(1);
   }

   strcpy(input_file,    argv[optind]);
   strcpy(output_file,   argv[optind+1]);
   strcpy(template_file, argv[optind+2]);

   if(debug >= 1)
   {
      printf("input_file    = [%s]\n", input_file);
      printf("colname       = [%s]\n", colname);
      printf("output_file   = [%s]\n", output_file);
      printf("template_file = [%s]\n", template_file);
      printf("width         = %d\n",   width);
      printf("ismag         = %d\n",   ismag);
      fflush(stdout);
   }


   /********************************************/
   /* Set the weights for spreading the points */
   /********************************************/

   sumweights = 0.;

   for(i=0; i<5; ++i)
   {
      for(j=0; j<5; ++j)
      {
	 if(width == 3)
	    sumweights += weights3[i][j];

	 else if(width == 5)
	    sumweights += weights5[i][j];
      }
   }

   for(i=0; i<5; ++i)
   {
      for(j=0; j<5; ++j)
      {
	 if(width == 3)
	    weights[i][j] = weights3[i][j]/sumweights;

	 else if(width == 5)
	    weights[i][j] = weights5[i][j]/sumweights;

	 else 
	    weights[i][j] = 0.;
      }
   }

   if(width != 3 && width != 5)
      weights[2][2] = 1.;




   /************************************************/ 
   /* Open the table file and find the data column */ 
   /************************************************/ 

   ncol = topen(input_file);

   if(ncol <= 0)
   {
      printf("[struct stat=\"ERROR\", msg=\"Can't open input table %s\"]\n", input_file);
      exit(0);
   }

   racol   = tcol( "ra");
   deccol  = tcol("dec");

    ra1col = tcol( "ra1");
   dec1col = tcol("dec1");
    ra2col = tcol( "ra2");
   dec2col = tcol("dec2");
    ra3col = tcol( "ra3");
   dec3col = tcol("dec3");
    ra4col = tcol( "ra4");
   dec4col = tcol("dec4");

   haveFlux = 1;
   if(strlen(colname) == 0)
      haveFlux = 0;
   else
      fluxcol = tcol(colname);

   isImg = 1;

   if(ra1col < 0 || dec1col < 0
   || ra2col < 0 || dec2col < 0
   || ra3col < 0 || dec3col < 0
   || ra4col < 0 || dec4col < 0)
      isImg = 0;

   if(useCenter)
      isImg = 0;

   if(!isImg)
   {
      if(racol <  0)
      {
	 printf("[struct stat=\"ERROR\", msg=\"Can't find column 'ra'\"]\n");
	 exit(0);
      }

      if(deccol <  0)
      {
	 printf("[struct stat=\"ERROR\", msg=\"Can't find column 'dec'\"]\n");
	 exit(0);
      }
   }

   if(haveFlux && fluxcol <  0)
   {
      printf("[struct stat=\"ERROR\", msg=\"Can't find column '%s'\"]\n", colname);
      exit(0);
   }


   /*************************************************/ 
   /* Process the output header template to get the */ 
   /* image size, coordinate system and projection  */ 
   /*************************************************/ 

   readTemplate(template_file);

   pixscale = fabs(output.wcs->xinc);

   if(fabs(output.wcs->yinc) > pixscale)
      pixscale = fabs(output.wcs->yinc);

   csys = EQUJ;

   if(strncmp(output.wcs->c1type, "RA",   2) == 0)
      csys = EQUJ;
   if(strncmp(output.wcs->c1type, "GLON", 4) == 0)
      csys = GAL;
   if(strncmp(output.wcs->c1type, "ELON", 4) == 0)
      csys = ECLJ;

   equinox = output.wcs->equinox;

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

   for (j=0; j<output.naxes[1]; ++j)
   {
      for (i=0; i<output.naxes[0]; ++i)
      {
	 data[j][i] = 0.;
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
   /* Loop over the input files */
   /*****************************/

   time(&currtime);
   start = currtime;


   /*******************/
   /* For each source */
   /*******************/

   count = 0;

   while(tread() >= 0)
   {
      ++count;

      if(isImg)
      {
	 if(debug && count/1000*1000 == count)
	 {
	    printf("%9d image outlines processed\n", count);
	    fflush(stdout);
	 }

	 ra [0] = atof(tval( ra1col));
	 dec[0] = atof(tval(dec1col));
	 ra [1] = atof(tval( ra2col));
	 dec[1] = atof(tval(dec2col));
	 ra [2] = atof(tval( ra3col));
	 dec[2] = atof(tval(dec3col));
	 ra [3] = atof(tval( ra4col));
	 dec[3] = atof(tval(dec4col));

	 for(side=0; side<4; ++side)
	 {
	    ibegin = side;
	    iend   = (side+1)%4;

	    x0 = cos(ra[ibegin]*dtr) * cos(dec[ibegin]*dtr);
	    y0 = sin(ra[ibegin]*dtr) * cos(dec[ibegin]*dtr);
	    z0 = sin(dec[ibegin]*dtr);

	    x1 = cos(ra[iend]*dtr) * cos(dec[iend]*dtr);
	    y1 = sin(ra[iend]*dtr) * cos(dec[iend]*dtr);
	    z1 = sin(dec[iend]*dtr);

	    xn = y0*z1 - z0*y1;
	    yn = z0*x1 - x0*z1;
	    zn = x0*y1 - y0*x1;

	    len = sqrt(xn*xn + yn*yn + zn*zn);

	    xn = xn / len;
	    yn = yn / len;
	    zn = zn / len;

	    ran  = atan2(yn, xn);
	    decn = asin(zn);

	    sina = sin(ran);
	    cosa = cos(ran);

	    sind = sin(decn);
	    cosd = cos(decn);

	    a11 =  cosa*sind;
	    a12 =  sina*sind;
	    a13 = -cosd;
	    a21 = -sina;
	    a22 =  cosa;
	    a23 =  0.;
	    a31 =  cosa*cosd;
	    a32 =  sina*cosd;
	    a33 =  sind;

	    x0p =  a11*x0 + a12*y0 + a13*z0;
	    y0p =  a21*x0 + a22*y0 + a23*z0;
	    z0p =  a31*x0 + a32*y0 + a33*z0;

	    x1p =  a11*x1 + a12*y1 + a13*z1;
	    y1p =  a21*x1 + a22*y1 + a23*z1;
	    z1p =  a31*x1 + a32*y1 + a33*z1;

	    lon0 = atan2(y0p, x0p);
	    lon1 = atan2(y1p, x1p);

	    if(fabs(lon1-lon0)/dtr > 180.)
	    {
	       if(lon0 < 0.) lon0 += 360.*dtr;
	       if(lon1 < 0.) lon1 += 360.*dtr;
	    }

	    sideLength = acos(x0*x1 + y0*y1 + z0*z1) / dtr;

            offset = pixscale/2.*dtr;
	    if(lon0 > lon1)
	       offset = -offset;

	    nstep = (lon1 - lon0)/offset;

	    lon = lon0;
	    for(i=0; i<nstep; ++i)
	    {
	       lon += offset;

	       xp = cos(lon);
	       yp = sin(lon);

	       x = a11*xp + a21*yp;
	       y = a12*xp + a22*yp;
	       z = a13*xp + a23*yp;

	       rac  = atan2(y,x)/dtr;
	       decc = asin(z)/dtr;

	       convertCoordinates (EQUJ,   2000.,    rac,   decc,
				   csys, equinox, &ilon, &ilat, 0.);

               offscl = 0;

	       wcs2pix(output.wcs, ilon, ilat, &oxpix, &oypix, &offscl);

               fixxy(&oxpix, &oypix, &offscl);

	       if(haveFlux)
		  pixel_value = atof(tval(fluxcol));
	       else
		  pixel_value = 1;

	       l = (int)(oxpix + 0.5) - 1;
	       m = (int)(oypix + 0.5) - 1;

	       if(!offscl)
		  data[m][l] = pixel_value;
	    }
	 }
      }
      else
      {
	 if(debug && count/1000*1000 == count)
	 {
	    printf("%9d sources processed\n", count);
	    fflush(stdout);
	 }

	 rac  = atof(tval(racol));
	 decc = atof(tval(deccol));

	 convertCoordinates (EQUJ,   2000.,    rac,   decc,
			     csys, equinox, &ilon, &ilat, 0.);

	 if(haveFlux)
	    pixel_value = atof(tval(fluxcol));
	 else
	    pixel_value = 1;

	 if(ismag)
	    pixel_value = pow(10., 0.4 * (refmag - pixel_value));

	 wcs2pix(output.wcs, ilon, ilat, &oxpix, &oypix, &offscl);

	 if(pixel_value < 1.e10)
	 {
	    if(debug >= 3)
	    {
	       printf(" value = %11.3e at coord = (%12.8f,%12.8f)", pixel_value, ilon, ilat);

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
	    else if(pixel_value > 1000000.)
	    {
	       printf(" value = %11.3e at coord = (%12.8f,%12.8f)", pixel_value, ilon, ilat);

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
	       l = (int)(oxpix + 0.5) - 1;
	       m = (int)(oypix + 0.5) - 1;

	       if(l < 0 || m < 0 || l >= output.naxes[0] || m >= output.naxes[1])
	       {
		  /*
		  printf("ERROR: l=%d, m=%d\n", l, m);
		  fflush(stdout);
		  */
	       }

	       else
	       {
		  for(dl=-2; dl<=2; ++dl)
		  {
		     if(l+dl < 0 || l+dl>= output.naxes[0])
			continue;

		     for(dm=-2; dm<=2; ++dm)
		     {
			if(m+dm < 0 || m+dm>= output.naxes[1])
			   continue;

			data[m+dm][l+dl] += weights[dm+2][dl+2] * pixel_value;
		     }
		  }
	       }
	    }
	 }
      }
   }


   /************************/
   /* Write the image data */
   /************************/

   fpixel    = 1;
   nelements = output.naxes[0] * output.naxes[1];

   if (fits_write_img(output.fptr, TDOUBLE, fpixel, nelements, data[0], &status))
      printFitsError(status);

   if(debug >= 1)
   {
      printf("Data array written to FITS image\n"); 
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


   time(&currtime);

   printf("[struct stat=\"OK\", time=%d]\n", 
      (int)(currtime - start));
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
   int      i;

   FILE    *fp;

   char     line[MAXSTR];

   char    *header[2];

   int      sys;
   double   epoch;

   double   x, y;
   double   ix, iy;
   double   xpos, ypos;

   int      offscl;

   header[0] = malloc(32768);
   header[1] = (char *)NULL;


   /********************************************************/
   /* Open the template file, read and parse all the lines */
   /********************************************************/

   fp = fopen(filename, "r");

   if(fp == (FILE *)NULL)
      printError("Template file not found");

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
      printf("[struct stat=\"ERROR\", msg=\"Output wcsinit() failed.\"]\n");
      exit(0);
   }


   /* Kludge to get around bug in WCS library:   */
   /* 360 degrees sometimes added to pixel coord */

   ix = 0.5;
   iy = 0.5;

   offscl = 0;

   pix2wcs(output.wcs, ix, iy, &xpos, &ypos);
   wcs2pix(output.wcs, xpos, ypos, &x, &y, &offscl);

   xcorrection = x-ix;
   ycorrection = y-iy;


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
   char status_str[FLEN_STATUS], errmsg[FLEN_ERRMSG];

   if(status)
      fprintf(stderr, "\n*** Error occurred during program execution ***\n");

   fits_get_errstatus(status, status_str);

   fprintf(stderr, "\nstatus = %d: %s\n", status, status_str);

   if(fits_read_errmsg(errmsg))
   {
      fprintf(stderr, "\nError message stack:\n");
      fprintf(stderr, " %s\n", errmsg);

      while(fits_read_errmsg(errmsg))
         fprintf(stderr, " %s\n", errmsg);
   }

   exit(status);
}



/******************************/
/*                            */
/*  Print out general errors  */
/*                            */
/******************************/

void printError(char *msg)
{
   printf("[struct stat=\"ERROR\", msg=\"%s\"]\n", msg);
   exit(0);
}
