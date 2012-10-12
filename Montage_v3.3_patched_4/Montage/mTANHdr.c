/* Module: mTANHdr.c

Version  Developer        Date     Change
-------  ---------------  -------  -----------------------
4.0      John Good        21Jun07  Trying to match CAR projections away
                                   from the equator led to the following
                                   changes:  correction for 360 degree
				   offset that sometimes happens with 
				   WCS library handling of CAR; initialization
				   of CDELT in distorted header based on
				   local spacing (for CAR this effectively
				   adds a cos(lat) correction; and a correction
				   to the reporting of maximum pixel error
				   (it was being reported in reverse sense
				   to the definitions in the code).
3.0      John Good        26Jun06  Made several changes to the code:  
				   Scaling/rotation now inherited verbatim
				   from the input header; stepsize was
				   used inconsistently (and slowed things
				   down); increased the number of points
				   used in fit; and checked fit for "all
				   off-scale" condition.
2.0      John Good        24Mar06  Added correlation matrix checking (to
				   remove coefficients if they are too
				   correlated and we are getting a singular
				   matrix).
1.3      John Good        21Sep04  A_ORDER, etc. should have been the maximum
				   index number, not the count (i.e. we were
				   off by 1).
1.2      John Good        27Aug04  Added "[-e equinox]" to Usage statement
1.1      John Good        11Aug04  Checking -c, -i, -o, -e, and -t arguments
                                   for valid values
1.0      John Good        15Apr04  Baseline code

*/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <math.h>

#include "coord.h"
#include "wcs.h"

#define MAXSTR  256

static struct WorldCoor *wcs;
static struct WorldCoor *WCS;

#define SWAP(a,b) {temp=(a);(a)=(b);(b)=temp;}

void     gaussj(double **, int, double **, int);
void     nrerror(char *);
int     *ivector(int);
void     free_ivector(int *);

int      order;

double **a ;
double **b ;
double **ap;
double **bp;

FILE    *fout;
FILE    *fstatus;

extern char *optarg;
extern int optind, opterr;

extern int getopt(int argc, char *const *argv, const char *options);

int    checkHdr    (char *infile, int hdrflag, int hdu);
int    checkWCS    (struct WorldCoor *wcs, int action);

int    readTemplate(char *template);
int    extractCD   (char *template);
int    makeWCS     ();
int    stradd      (char *header, char *card);
int    printHeader (char *header);
double distance    (double ra, double dec, double rao, double deco);
void   fixxy       (double *x, double *y, int *offscl);

int  haveCdelt1;
int  haveCdelt2;
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

char  cdelt1 [80];
char  cdelt2 [80];
char  crota2 [80];
char  cd11   [80];
char  cd12   [80];
char  cd21   [80];
char  cd22   [80];
char  pc11   [80];
char  pc12   [80];
char  pc21   [80];
char  pc22   [80];
char  epoch  [80];
char  equinox[80];

double xcorrection;
double ycorrection;

double pcdelt1, pcdelt2;
double dtr;

int debug;


/*************************************************************************/
/*                                                                       */
/*  mTANHdr                                                              */
/*                                                                       */
/*  Montage is a set of general reprojection / coordinate-transform /    */
/*  mosaicking programs.  Any number of input images can be merged into  */
/*  an output FITS file.  The attributes of the input are read from the  */
/*  input files; the attributes of the output are read a combination of  */
/*  the command line and a FITS header template file.                    */
/*                                                                       */
/*  There are two reprojection modules, mProject and mProjectPP.         */
/*  The first can handle any projection transformation but is slow.  The */
/*  second can only handle transforms between tangent plane projections  */
/*  but is very fast.  In many cases, however, a non-tangent-plane       */
/*  projection can be approximated by a TAN projection with pixel-space  */
/*  distortions (in particular when the region covered is small, which   */
/*  is often the case in practice).                                      */
/*                                                                       */
/*  This module analyzes a template file and determines if there is      */
/*  an adequate equivalent distorted TAN projection that would be        */
/*  equivelent (i.e. location shifts less than, say, 0.01 pixels).       */
/*  mProjectPP can then be used to produce this distorted TAN image      */
/*  with the original non-TAN FITS header swapped in before writing      */
/*  to disk.                                                             */
/*                                                                       */
/*                                                                       */
/*  NOTE:  The "reverse" error is the important one for deciding whether */
/*  the new distorted-TAN header can be used in place of the original    */
/*  when reprojecting in mProjectPP since it is a measure of the         */
/*  process of going from distorted TAN to sky to original projection.   */
/*  Since the second part of this is exact, this error is all about how  */
/*  accurately the distorted-TAN maps to the right point of the sky.     */
/*                                                                       */
/*                                                                       */
/*************************************************************************/

int main(int argc, char **argv)
{
   char     origtmpl[MAXSTR];
   char     newtmpl [MAXSTR];

   int      iter, useOffscl;
   int      i, j, n, m, c;
   int      ii, ij, ji, jj;
   int      ipix, jpix;
   int      iorder, jorder;
   int      stepsize, smallstep, offscl;

   double   x, y;
   double   ix, iy;
   double   xpos, ypos;
   double   xpow, ypow;
   double   z, w;

   double **matrix;
   double **vector;
   double   change;
   double   fwdmaxx, fwdmaxy;
   double   revmaxx, revmaxy;

   int      maxiter;
   int      fwditer = 0;
   int      reviter = 0;
   double   tolerance;

   double   fullcirclex;
   double   fullcircley;

   double   xcen, ycen;
   double   xoff, yoff;
   double   ocdelt1, ocdelt2;

   char    *end;

   dtr = atan(1.)/45.;


   /************************************/
   /* Read the command-line parameters */
   /************************************/

   opterr    =  0;
   debug     =  0;
   order     =  5;
   maxiter   = 50;
   tolerance =  0.01;
   fstatus   = stdout;
   useOffscl =  0;

   while ((c = getopt(argc, argv, "dui:o:t:s:")) != EOF) 
   {
      switch (c) 
      {
         case 'd':
            debug = 2;
            break;

         case 'u':
            useOffscl = 1;
            break;

         case 'i':
	    maxiter = strtol(optarg, &end, 0);

	    if(end < optarg + strlen(optarg))
	    {
	       printf("[struct stat=\"ERROR\", msg=\"-i (iterations) argument \"%s\" not an integer\"]\n", optarg);
	       fflush(stdout);
	       exit(1);
	    }

	    if(maxiter < 1)
	       maxiter = 1;
            break;

         case 'o':
	    order = strtol(optarg, &end, 0);

	    if(end < optarg + strlen(optarg))
	    {
	       printf("[struct stat=\"ERROR\", msg=\"-o (order) argument \"%s\" not an integer\"]\n", optarg);
	       fflush(stdout);
	       exit(1);
	    }

	    if(order < 0)
	       order = 0;
            break;

         case 't':
	    tolerance = strtod(optarg, &end);

	    if(end < optarg + strlen(optarg))
	    {
	       printf("[struct stat=\"ERROR\", msg=\"-t (tolerance) argument \"%s\" not a real number\"]\n", optarg);
	       fflush(stdout);
	       exit(1);
	    }

	    if(tolerance < 0)
	       tolerance = 0;
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
	    fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Usage: %s [-d][-o order][-i maxiter][-t tolerance][-s statusfile] orig.hdr new.hdr (default: order = 5, maxiter = 50, tolerance = 0.01)\"]\n", 
	       argv[0]);
            exit(1);
            break;
      }
   }

   if (argc - optind < 2) 
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Usage: %s [-d][-o order][-i maxiter][-t tolerance][-s statusfile] orig.hdr new.hdr (default: order = 5, maxiter = 50, tolerance = 0.01)\"]\n", 
	 argv[0]);
      exit(1);
   }

   strcpy(origtmpl, argv[optind]);
   strcpy(newtmpl,  argv[optind + 1]);

   checkHdr(origtmpl, 1, 0);

   extractCD(origtmpl);

   fout = fopen(newtmpl, "w+");

   if(fout == (FILE *)NULL)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Cannot open output template file %s\"]\n", 
	 newtmpl);
      fflush(stdout);

      exit(1);
   }

   if(debug)
   {
      printf("DEBUG> Command-line read.\n");
      printf("DEBUG> FWD:  native projection to distorted TAN (wcs->WCS)\n");
      printf("DEBUG> REV:  distorted TAN to native projection (WCS->wcs)\n");
      fflush(stdout);
   }


   /********************************************/
   /* Set up the distortion parameter matrices */
   /********************************************/

   a  = (double **)malloc((order+1) * sizeof(double *));
   ap = (double **)malloc((order+1) * sizeof(double *));

   for(iorder=0; iorder<order; ++iorder)
   {
      a [iorder] = (double *)malloc((order+1) * sizeof(double));
      ap[iorder] = (double *)malloc((order+1) * sizeof(double));

      for(jorder=0; jorder<order; ++jorder)
      {
	 a [iorder][jorder] = 0.;
	 ap[iorder][jorder] = 0.;
      }
   }

   b  = (double **)malloc((order+1) * sizeof(double *));
   bp = (double **)malloc((order+1) * sizeof(double *));

   for(iorder=0; iorder<order; ++iorder)
   {
      b [iorder] = (double *)malloc((order+1) * sizeof(double));
      bp[iorder] = (double *)malloc((order+1) * sizeof(double));

      for(jorder=0; jorder<order; ++jorder)
      {
	 b [iorder][jorder] = 0.;
	 bp[iorder][jorder] = 0.;
      }
   }

   if(debug)
   {
      printf("DEBUG> Distortion parameters initialized.\n");
      fflush(stdout);
   }


   /****************************************/
   /* Allocate space for the least-squares */
   /* matrices.                            */
   /****************************************/

   m = order;

   n = m * m;

   vector = (double **)malloc((2*n+1)*sizeof(double *));
   matrix = (double **)malloc((2*n+1)*sizeof(double *));

   for(i=0; i<2*n; ++i)
   {
      vector[i] = (double *)malloc(     2 *sizeof(double));
      matrix[i] = (double *)malloc((2*n+1)*sizeof(double));
   }


   /*************************************/
   /* Read the template file and set up */
   /* the WCS for the original image    */
   /*************************************/

   readTemplate(origtmpl);

   fullcirclex = fabs(360. / wcs->xinc);
   fullcircley = fabs(360. / wcs->yinc);

   if(wcs->prjcode == WCS_DSS)
   {
      haveCdelt1 = 1;
      sprintf(cdelt1, "%15.10f", wcs->cdelt[0]);

      haveCdelt2 = 1;
      sprintf(cdelt2, "%15.10f", wcs->cdelt[1]);

      haveCdelt1 = 1;
      sprintf(crota2, "%15.10f", wcs->rot);
   }


   /*****************************************/
   /* Build an initial TAN FITS header with */
   /* no distortions                        */
   /*****************************************/

   x = (wcs->nxpix+1)/2.;
   y = (wcs->nypix+1)/2.;

   pix2wcs(wcs, x, y, &xcen, &ycen);


   ocdelt1 = atof(cdelt1);

   pix2wcs(wcs, x+1., y, &xoff, &yoff);

   pcdelt1 = distance(xcen, ycen, xoff, yoff);

   if(ocdelt1 < 0.)
      pcdelt1 = -pcdelt1;


   ocdelt2 = atof(cdelt2);

   pix2wcs(wcs, x, y+1., &xoff, &yoff);

   pcdelt2 = distance(xcen, ycen, xoff, yoff);

   if(ocdelt2 < 0.)
      pcdelt2 = -pcdelt2;


   /******************************************************/
   /* Since the x and y directions are solved separately */
   /* (and affect each other), we have to iterate        */
   /******************************************************/

   stepsize = wcs->nxpix;

   if(wcs->nypix < stepsize)
      stepsize = wcs->nypix;

   stepsize = stepsize  / 8.;

   smallstep = stepsize / 20.;

   if(stepsize < 1)
      stepsize = 1;

   if(smallstep < 1)
      smallstep = 1;


   /***********************************************/
   /* FWD: First we fit for the AP, BP parameters */
   /***********************************************/

   makeWCS();

   for(iter=0; iter<maxiter; ++iter)
   {
      /*****************************************************************/
      /* Populate the matrix and vector used to least-squares solve    */
      /* for the distortion parameter values.  The "z" value is the    */
      /* error in pixel position between the pixel location of a       */
      /* point in the original pixel space and its location in the     */
      /* distorted TAN pixel space.  If we can make these close enough */
      /* to zero for all pixels, we can use the "distorted" TAN header */
      /* in place of the original                                      */
      /*****************************************************************/

      /**********************************************/
      /* FWD: First the A (x-direction) distortions */
      /**********************************************/

      for(i=0; i<2*n; ++i)
      {
	 vector[i][0] = 0.;
	 
	 for(j=0; j<2*n; ++j)
	    matrix[i][j] = 0.;
      }

      if(debug > 1)
      {
	 printf("\nDEBUG> FWD: A (x-direction) distortions [%d]\n", iter); 
	 fflush(stdout);
      }

      for (ipix=0; ipix<wcs->nxpix+1; ipix+=stepsize)
      {
	 for (jpix=0; jpix<wcs->nypix+1; jpix+=stepsize)
	 {
	    ix = ipix - 0.5;
	    iy = jpix - 0.5;

	    pix2wcs(wcs, ix, iy, &xpos, &ypos);

            offscl = 0;

	    wcs2pix(WCS, xpos, ypos, &x, &y, &offscl);

            z = ix - x;

	    if(debug > 1)
	    {
	       printf("DEBUG> FWD %10.2f %10.2f -> %.5f %.5f -> %10.2f %10.2f -> %10.2f (%d)\n", 
		  ix, iy, xpos, ypos, x, y, z, offscl);
	       fflush(stdout);
	    }
            
	    if(useOffscl || !offscl)
	    {
	       for(ii=0; ii<order; ++ii)
	       {
		  for(ij=0; ij<order; ++ij)
		  {
		     i = ii*m + ij;

		     xpow = ii;
		     ypow = ij;

		     vector[i][0] += pow(ix, xpow) * pow(iy, ypow) * z;

		     for(jj=0; jj<order; ++jj)
		     {
			for(ji=0; ji<order; ++ji)
			{
			   j = jj*m + ji;

			   xpow = ii + jj;
			   ypow = ij + ji;

			   matrix[i][j] += pow(ix, xpow) * pow(iy, ypow);
			}
		     }
		  }
	       }
	    }
	 }
      }


      /********************************************/
      /* FWD: Now the B (y-direction) distortions */
      /********************************************/

      if(debug > 1)
      {
	 printf("\nDEBUG> FWD: B (y-direction) distortions [%d]\n", iter); 
	 fflush(stdout);
      }

      for (ipix=0; ipix<wcs->nxpix+1; ipix+=stepsize)
      {
	 for (jpix=0; jpix<wcs->nypix+1; jpix+=stepsize)
	 {
	    ix = ipix - 0.5;
	    iy = jpix - 0.5;

	    pix2wcs(wcs, ix, iy, &xpos, &ypos);

            offscl = 0;

	    wcs2pix(WCS, xpos, ypos, &x, &y, &offscl);

	    z = iy - y;

	    if(debug > 1)
	    {
	       printf("DEBUG> FWD %10.2f %10.2f -> %.5f %.5f -> %10.2f %10.2f -> %10.2f (%d)\n", 
		  ix, iy, xpos, ypos, x, y, z, offscl);
	       fflush(stdout);
	    }

	    if(useOffscl || !offscl)
	    {
	       for(ii=0; ii<order; ++ii)
	       {
		  for(ij=0; ij<order; ++ij)
		  {
		     i = ii*m + ij;

		     xpow = ii;
		     ypow = ij;

		     vector[i+n][0] += pow(x, xpow) * pow(y, ypow) * z;

		     for(jj=0; jj<order; ++jj)
		     {
			for(ji=0; ji<order; ++ji)
			{
			   j = jj*m + ji;

			   xpow = ii + jj;
			   ypow = ij + ji;

			   matrix[i+n][j+n] += pow(x, xpow) * pow(y, ypow);
			}
		     }
		  }
	       }
	    }
	 }
      }


      /************************************************************/
      /* FWD: Solve this system for the B distortion coefficients */
      /************************************************************/

      if(matrix[n-1][n-1] == 0.0)
      {
	 fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"All points offscale in forward transform\"]\n");
	 exit(1);
      }

      if(debug)
      {
	 printf("\n\nFWD: Before gaussj():\n");

	 for(i=0; i<n; ++i)
	 {
	    printf("matrix:\n");
	    for(j=0; j<n; ++j)
	       printf("%12.5e ", matrix[i][j]);

	    printf("\nvector:\n");
	    printf("  %12.5e\n", vector[i][0]);
	 }

	 printf("\n");
      }

      gaussj(matrix, 2*n, vector, 1);

      if(debug)
      {
	 printf("\n\nAfter gaussj():\n");

	 for(i=0; i<n; ++i)
	 {
	    printf("matrix:\n");
	    for(j=0; j<n; ++j)
	       printf("%12.5e ", matrix[i][j]);

	    printf("\nvector:\n");
	    printf("  %12.5e\n", vector[i][0]);
	 }

	 printf("\n");
      }

      for(ii=0; ii<order; ++ii)
      {
	 for(ij=0; ij<order; ++ij)
	 {
	    i = ii*m + ij;

	    ap[ii][ij] += vector[i][0]/2.;

	    if(ap[ii][ij] != 0)
	       change = vector[i][0]/ap[ii][ij] * 100.;
	    else
	       change = -999.;
	    
	    if(debug)
	    {
	       printf("ap[%d][%d] = %12.5e (%5.1f%%)\n", 
		  ii, ij, ap[ii][ij], change);
	       fflush(stdout);
	    }
	 }
      }

      if(debug)
	 printf("\n");

      for(ii=0; ii<order; ++ii)
      {
	 for(ij=0; ij<order; ++ij)
	 {
	    i = ii*m + ij;

	    bp[ii][ij] += vector[i+n][0]/2.;

	    if(bp[ii][ij] != 0)
	       change = vector[i+n][0]/bp[ii][ij] * 100.;
	    else
	       change = -999.;
	    
	    if(debug)
	    {
	       printf("bp[%d][%d] = %12.5e (%5.1f%%)\n", 
		  ii, ij, bp[ii][ij], change);
	       fflush(stdout);
	    }
	 }
      }


      makeWCS();


      /******************************************/
      /* FWD: Find the maximum positional error */
      /******************************************/

      fwdmaxx = 0.;
      fwdmaxy = 0.;

      if(debug > 1)
      {
	 printf("\nDEBUG> FWD: maximum positional error [%d]\n", iter); 
	 fflush(stdout);
      }

      for (ipix=0; ipix<wcs->nxpix+1; ipix+=stepsize)
      {
	 for (jpix=0; jpix<wcs->nypix+1; jpix+=stepsize)
	 {
	    ix = ipix - 0.5;
	    iy = jpix - 0.5;

	    pix2wcs(wcs, ix, iy, &xpos, &ypos);

            offscl = 0;

	    wcs2pix(WCS, xpos, ypos, &x, &y, &offscl);

	    if(debug > 1)
	    {
	       printf("DEBUG> FWD %10.2f %10.2f -> %.5f %.5f -> %10.2f %10.2f (%d)\n", 
		  ix, iy, xpos, ypos, x, y, offscl);
	       fflush(stdout);
	    }

	    if(useOffscl || !offscl)
	    {
	       if(fabs(ix-x) > fwdmaxx)
		  fwdmaxx = fabs(ix-x);

	       if(fabs(iy-y) > fwdmaxy)
		  fwdmaxy = fabs(iy-y);
	    }
	 }
      }

      if(debug)
      {
	 printf("fwdmaxx = %-g [%d]\n", fwdmaxx, iter);
	 printf("fwdmaxy = %-g [%d]\n", fwdmaxy, iter);
	 fflush(stdout);
      }

      if(fwdmaxx < tolerance
      && fwdmaxy < tolerance)
	 break;
   }

   fwditer = iter;


   /********************************************/
   /* REV: Then we fit for the A, B parameters */
   /********************************************/

   makeWCS();

   for(iter=0; iter<maxiter; ++iter)
   {
      /**********************************************/
      /* REV: First the A (x-direction) distortions */
      /**********************************************/

      for(i=0; i<2*n; ++i)
      {
	 vector[i][0] = 0.;
	 
	 for(j=0; j<2*n; ++j)
	    matrix[i][j] = 0.;
      }

      if(debug > 1)
      {
	 printf("\nDEBUG> REV: A (x-direction) distortions [%d]\n", iter); 
	 fflush(stdout);
      }

      for (ipix=0; ipix<wcs->nxpix+1; ipix+=stepsize)
      {
	 for (jpix=0; jpix<wcs->nypix+1; jpix+=stepsize)
	 {
	    ix = ipix - 0.5;
	    iy = jpix - 0.5;

	    pix2wcs(WCS, ix, iy, &xpos, &ypos);

            offscl = 0;

	    wcs2pix(wcs, xpos, ypos, &x, &y, &offscl);

            fixxy(&x, &y, &offscl);

	    z = ix - x;

	    if(debug > 1)
	    {
	       printf("DEBUG> REV %10.2f %10.2f -> %.5f %.5f -> %10.2f %10.2f -> %10.2f (%d)\n", 
		  ix, iy, xpos, ypos, x, y, z, offscl);
	       fflush(stdout);
	    }

	    if(useOffscl || !offscl)
	    {
	       for(ii=0; ii<order; ++ii)
	       {
		  for(ij=0; ij<order; ++ij)
		  {
		     i = ii*m + ij;

		     xpow = ii;
		     ypow = ij;

		     vector[i][0] += pow(ix, xpow) * pow(iy, ypow) * z;

		     for(jj=0; jj<order; ++jj)
		     {
			for(ji=0; ji<order; ++ji)
			{
			   j = jj*m + ji;

			   xpow = ii + jj;
			   ypow = ij + ji;

			   matrix[i][j] += pow(ix, xpow) * pow(iy, ypow);
			}
		     }
		  }
	       }
	    }
	 }
      }


      /********************************************/
      /* REV: Now the B (y-direction) distortions */
      /********************************************/

      if(debug > 1)
      {
	 printf("\nDEBUG> REV: B (y-direction) distortions [%d]\n", iter); 
	 fflush(stdout);
      }

      for (ipix=0; ipix<wcs->nxpix+1; ipix+=stepsize)
      {
	 for (jpix=0; jpix<wcs->nypix+1; jpix+=stepsize)
	 {
	    ix = ipix - 0.5;
	    iy = jpix - 0.5;

	    pix2wcs(WCS, ix, iy, &xpos, &ypos);

            offscl = 0;

	    wcs2pix(wcs, xpos, ypos, &x, &y, &offscl);

            fixxy(&x, &y, &offscl);

	    z = iy - y;

	    if(debug > 1)
	    {
	       printf("DEBUG> REV %10.2f %10.2f -> %.5f %.5f -> %10.2f %10.2f -> %10.2f (%d)\n", 
		  ix, iy, xpos, ypos, x, y, z, offscl);
	       fflush(stdout);
	    }


	    if(useOffscl || !offscl)
	    {
	       for(ii=0; ii<order; ++ii)
	       {
		  for(ij=0; ij<order; ++ij)
		  {
		     i = ii*m + ij;

		     xpow = ii;
		     ypow = ij;

		     vector[i+n][0] += pow(x, xpow) * pow(y, ypow) * z;

		     for(jj=0; jj<order; ++jj)
		     {
			for(ji=0; ji<order; ++ji)
			{
			   j = jj*m + ji;

			   xpow = ii + jj;
			   ypow = ij + ji;

			   matrix[i+n][j+n] += pow(x, xpow) * pow(y, ypow);
			}
		     }
		  }
	       }
	    }
	 }
      }


      /************************************************************/
      /* REV: Solve this system for the B distortion coefficients */
      /************************************************************/

      if(matrix[n-1][n-1] == 0.0)
      {
	 fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"All points offscale in reverse transform\"]\n");
	 exit(1);
      }

      if(debug)
      {
	 printf("\n\nREV: Before gaussj():\n");

	 for(i=0; i<n; ++i)
	 {
	    printf("matrix:\n");
	    for(j=0; j<n; ++j)
	       printf("%12.5e ", matrix[i][j]);

	    printf("\nvector:\n");
	    printf("  %12.5e\n", vector[i][0]);
	 }

	 printf("\n");
      }

      gaussj(matrix, 2*n, vector, 1);

      if(debug)
      {
	 printf("\n\nAfter gaussj():\n");

	 for(i=0; i<n; ++i)
	 {
	    printf("matrix:\n");
	    for(j=0; j<n; ++j)
	       printf("%12.5e ", matrix[i][j]);

	    printf("\nvector:\n");
	    printf("  %12.5e\n", vector[i][0]);
	 }

	 printf("\n");
      }

      for(ii=0; ii<order; ++ii)
      {
	 for(ij=0; ij<order; ++ij)
	 {
	    i = ii*m + ij;

	    a[ii][ij] += vector[i][0]/2.;

	    if(a[ii][ij] != 0)
	       change = vector[i][0]/a[ii][ij] * 100.;
	    else
	       change = -999.;
	    
	    if(debug)
	    {
	       printf("a[%d][%d] = %12.5e (%5.1f%%)\n", 
		  ii, ij, a[ii][ij], change);
	       fflush(stdout);
	    }
	 }
      }

      if(debug)
	 printf("\n");

      for(ii=0; ii<order; ++ii)
      {
	 for(ij=0; ij<order; ++ij)
	 {
	    i = ii*m + ij;

	    b[ii][ij] += vector[i+n][0]/2.;

	    if(b[ii][ij] != 0)
	       change = vector[i+n][0]/b[ii][ij] * 100.;
	    else
	       change = -999.;
	    
	    if(debug)
	    {
	       printf("b[%d][%d] = %12.5e (%5.1f%%)\n", 
		  ii, ij, b[ii][ij], change);
	       fflush(stdout);
	    }
	 }
      }


      makeWCS();


      /******************************************/
      /* REV: Find the maximum positional error */
      /******************************************/

      revmaxx = 0.;
      revmaxy = 0.;

      if(debug > 1)
      {
	 printf("\nDEBUG> REV: maximum positional error [%d]\n", iter); 
	 fflush(stdout);
      }

      for (ipix=0; ipix<wcs->nxpix+1; ipix+=stepsize)
      {
	 for (jpix=0; jpix<wcs->nypix+1; jpix+=stepsize)
	 {
	    ix = ipix - 0.5;
	    iy = jpix - 0.5;

	    pix2wcs(WCS, ix, iy, &xpos, &ypos);

            offscl = 0;

	    wcs2pix(wcs, xpos, ypos, &x, &y, &offscl);

            fixxy(&x, &y, &offscl);

	    if(debug > 1)
	    {
	       printf("DEBUG> REV %10.2f %10.2f -> %.5f %.5f -> %10.2f %10.2f (%d)\n", 
		  ix, iy, xpos, ypos, x, y, offscl);
	       fflush(stdout);
	    }

	    if(useOffscl || !offscl)
	    {
	       if(fabs(ix-x) > revmaxx)
		  revmaxx = fabs(ix-x);

	       if(fabs(iy-y) > revmaxy)
		  revmaxy = fabs(iy-y);
	    }
	 }
      }

      if(debug)
      {
	 printf("revmaxx = %-g [%d]\n", revmaxx, iter);
	 printf("revmaxy = %-g [%d]\n", revmaxy, iter);
	 fflush(stdout);
      }

      if(revmaxx < tolerance
      && revmaxy < tolerance)
	 break;
   }

   reviter = iter;


   /****************************************/
   /* Find the worst case pixel errors     */
   /* associated with the "forward"        */
   /* transform (AP,BP parameters).        */
   /****************************************/

   fwdmaxx = 0.;
   fwdmaxy = 0.;

   if(debug)
      printf("\n");

   if(debug > 1)
   {
      printf("\nDEBUG> FWD: Worst case pixel error\n"); 
      fflush(stdout);
   }

   for (ipix=0; ipix<wcs->nxpix+1; ipix+=smallstep)
   {
      for (jpix=0; jpix<wcs->nypix+1; jpix+=smallstep)
      {
	 ix = ipix - 0.5;
	 iy = jpix - 0.5;

	 pix2wcs(wcs, ix, iy, &xpos, &ypos);

         offscl = 0;

	 wcs2pix(WCS, xpos, ypos, &x, &y, &offscl);

	 if(debug > 1)
	 {
	    printf("DEBUG> FWD %10.2f %10.2f -> %.5f %.5f -> %10.2f %10.2f (%d)\n", 
	       ix, iy, xpos, ypos, x, y, offscl);
	    fflush(stdout);
	 }

	 if(useOffscl || !offscl)
	 {
	    if(fabs(ix-x) > fwdmaxx)
	       fwdmaxx = fabs(ix-x);

	    if(fabs(iy-y) > fwdmaxy)
	       fwdmaxy = fabs(iy-y);
	 }
      }
   }

   if(debug)
   {
      printf("\n");
      printf("final fwdmaxx = %-g\n", fwdmaxx);
      printf("final fwdmaxy = %-g\n", fwdmaxy);
      fflush(stdout);
   }


   /****************************************/
   /* Find the worst case pixel errors     */
   /* associated with the "reverse"        */
   /* transform (A,B parameters).          */
   /****************************************/

   revmaxx = 0.;
   revmaxy = 0.;

   if(debug > 1)
   {
      printf("\nDEBUG> REV: Worst case pixel error\n"); 
      fflush(stdout);
   }

   for (ipix=0; ipix<wcs->nxpix+1; ipix+=smallstep)
   {
      for (jpix=0; jpix<wcs->nypix+1; jpix+=smallstep)
      {
	 ix = ipix - 0.5;
	 iy = jpix - 0.5;

	 pix2wcs(WCS, ix, iy, &xpos, &ypos);

         offscl = 0;

	 wcs2pix(wcs, xpos, ypos, &x, &y, &offscl);

         fixxy(&x, &y, &offscl);

	 if(debug > 1)
	 {
	    printf("DEBUG> REV %10.2f %10.2f -> %.5f %.5f -> %10.2f %10.2f (%d)\n", 
	       ix, iy, xpos, ypos, x, y, offscl);
	    fflush(stdout);
	 }

	 if(useOffscl || !offscl)
	 {
	    if(fabs(ix-x) > revmaxx)
	       revmaxx = fabs(ix-x);

	    if(fabs(iy-y) > revmaxy)
	       revmaxy = fabs(iy-y);
	 }
      }
   }

   if(debug)
   {
      printf("final revmaxx = %-g\n", revmaxx);
      printf("final revmaxy = %-g\n\n", revmaxy);
      fflush(stdout);
   }


   /****************/
   /* Final output */
   /****************/

   fclose(fout);

   if(fwditer >= maxiter) fwditer = maxiter-1;
   if(reviter >= maxiter) reviter = maxiter-1;

   fprintf(fstatus, "[struct stat=\"OK\", fwdxerr=%-g, fwdyerr=%-g, fwditer=%d, revxerr=%-g, revyerr=%-g, reviter=%d]\n",
      fwdmaxx, fwdmaxy, fwditer+1,
      revmaxx, revmaxy, reviter+1);
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

/*
   if(*x < 0.
   || *x > wcs->nxpix+1.
   || *y < 0.
   || *y > wcs->nypix+1.)
      *offscl = 1;
*/

   return;
}


/**************************************************/
/*                                                */
/*  Extract the CDELT/CD information from the     */
/*  input header.  Originally we used the data    */
/*  from the WCS library structure but it is      */
/*  sometimes hard to be sure what info to use.   */
/*                                                */
/**************************************************/

int extractCD(char *template)
{
   int       len;
   FILE     *fp;
   char     *end, *keyword, *value;
   char      line[MAXSTR];

   fp = fopen(template, "r");

   if(fp == (FILE *)NULL)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Bad template: %s\"]\n", 
	 template);
      exit(1);
   }

   haveCdelt1  = 0;
   haveCdelt2  = 0;
   haveCrota2  = 0;
   haveCD11    = 0;
   haveCD12    = 0;
   haveCD21    = 0;
   haveCD22    = 0;
   havePC11    = 0;
   havePC12    = 0;
   havePC21    = 0;
   havePC22    = 0;
   haveEpoch   = 0;
   haveEquinox = 0;

   strcpy(cdelt1,  "");
   strcpy(cdelt2,  "");
   strcpy(crota2,  "");
   strcpy(cd11,    "");
   strcpy(cd12,    "");
   strcpy(cd21,    "");
   strcpy(cd22,    "");
   strcpy(pc11,    "");
   strcpy(pc12,    "");
   strcpy(pc21,    "");
   strcpy(pc22,    "");
   strcpy(epoch,   "");
   strcpy(equinox, "");

   while(1)
   {
      if(fgets(line, MAXSTR, fp) == (char *)NULL)
         break;

      len = (int)strlen(line);

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

      while(   *end != ' '  && *end != '\'' 
	    && *end != '\r' && *end != '\n'
            &&  end < line+len)
	 ++end;

      *end = '\0';

      if(strcmp(keyword, "CDELT1") == 0)
      {
	 haveCdelt1 = 1;
	 strcpy(cdelt1, value);
      }
      else if(strcmp(keyword, "CDELT2") == 0)
      {
	 haveCdelt2 = 1;
	 strcpy(cdelt2, value);
      }
      else if(strcmp(keyword, "CROTA2") == 0)
      {
	 haveCrota2 = 1;
	 strcpy(crota2, value);
      }
      else if(strcmp(keyword, "CD1_1") == 0)
      {
	 haveCD11 = 1;
	 strcpy(cd11, value);
      }
      else if(strcmp(keyword, "CD1_2") == 0)
      {
	 haveCD12 = 1;
	 strcpy(cd12, value);
      }
      else if(strcmp(keyword, "CD2_1") == 0)
      {
	 haveCD21 = 1;
	 strcpy(cd21, value);
      }
      else if(strcmp(keyword, "CD2_2") == 0)
      {
	 haveCD22 = 1;
	 strcpy(cd22, value);
      }
      else if(strcmp(keyword, "PC1_1") == 0)
      {
	 havePC11 = 1;
	 strcpy(pc11, value);
      }
      else if(strcmp(keyword, "PC1_2") == 0)
      {
	 havePC12 = 1;
	 strcpy(pc12, value);
      }
      else if(strcmp(keyword, "PC2_1") == 0)
      {
	 havePC21 = 1;
	 strcpy(pc21, value);
      }
      else if(strcmp(keyword, "PC2_2") == 0)
      {
	 havePC22 = 1;
	 strcpy(pc22, value);
      }
      else if(strcmp(keyword, "EPOCH") == 0)
      {
	 haveEpoch = 1;
	 strcpy(epoch, value);
      }
      else if(strcmp(keyword, "EQUINOX") == 0)
      {
	 haveEquinox = 1;
	 strcpy(equinox, value);
      }
   }

   if(debug)
   {
      printf("\nextractCD():\n");
      if(haveCdelt1)  printf("cdelt1  = [%s]\n", cdelt1);
      if(haveCdelt2)  printf("cdelt2  = [%s]\n", cdelt2);
      if(haveCrota2)  printf("crota2  = [%s]\n", crota2);
      if(haveCD11)    printf("cd11    = [%s]\n", cd11);
      if(haveCD12)    printf("cd12    = [%s]\n", cd12);
      if(haveCD21)    printf("cd21    = [%s]\n", cd21);
      if(haveCD22)    printf("cd22    = [%s]\n", cd22);
      if(havePC11)    printf("pc11    = [%s]\n", pc11);
      if(havePC12)    printf("pc12    = [%s]\n", pc12);
      if(havePC21)    printf("pc21    = [%s]\n", pc21);
      if(havePC22)    printf("pc22    = [%s]\n", pc22);
      if(haveEpoch)   printf("epoch   = [%s]\n", epoch);
      if(haveEquinox) printf("equinox = [%s]\n", equinox);
      printf("\n");
   }

   return 0;
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

int readTemplate(char *template)
{
   int      j;
   FILE    *fp;
   char     line[MAXSTR];
   char     header[80000];
   double   x, y;
   double   ix, iy;
   double   xpos, ypos;
   int      offscl;

   fp = fopen(template, "r");

   if(fp == (FILE *)NULL)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Bad template: %s\"]\n", 
	 template);
      exit(1);
   }

   strcpy(header, "");

   for(j=0; j<1000; ++j)
   {
      if(fgets(line, MAXSTR, fp) == (char *)NULL)
         break;

      if(line[strlen(line)-1] == '\n')
         line[strlen(line)-1]  = '\0';
      
      if(line[strlen(line)-1] == '\r')
	 line[strlen(line)-1]  = '\0';

      stradd(header, line);
   }

   if(debug)
   {
      printf("\nDEBUG> Original Header:\n\n");
      fflush(stdout);

      printHeader(header);
      fflush(stdout);
   }

   wcs = wcsinit(header);

   if(wcs == (struct WorldCoor *)NULL)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Output wcsinit() failed.\"]\n");
      exit(1);
   }

   checkWCS(wcs, 0);

   if(debug)
   {
      printf("DEBUG> Original image WCS initialized\n\n");
      fflush(stdout);
   }


   /* Kludge to get around bug in WCS library:   */
   /* 360 degrees sometimes added to pixel coord */

   ix = 0.5;
   iy = 0.5;

   offscl = 0;

   pix2wcs(wcs, ix, iy, &xpos, &ypos);
   wcs2pix(wcs, xpos, ypos, &x, &y, &offscl);

   xcorrection = x-ix;
   ycorrection = y-iy;

   if(debug)
   {
      printf("DEBUG> xcorrection = %.2f\n", xcorrection);
      printf("DEBUG> ycorrection = %.2f\n\n", ycorrection);
      fflush(stdout);
   }

   fclose(fp);

   return 0;
}



int stradd(char *header, char *card)
{
   int i, hlen, clen;

   hlen = strlen(header);
   clen = strlen(card);

   for(i=0; i<clen; ++i)
      header[hlen+i] = card[i];

   if(clen < 80)
      for(i=clen; i<80; ++i)
         header[hlen+i] = ' ';
   
   header[hlen+80] = '\0';

   return(strlen(header));
}



/***********************************/
/*                                 */
/*  Create a distorted TAN header  */
/*  and initialize WCS using it    */
/*                                 */
/***********************************/

int makeWCS()
{
   char     header  [32768];
   char     temp    [MAXSTR];

   int      naxis1, naxis2;
   int      iorder, jorder;

   double   x, y;
   double   ix, iy;
   double   xpos, ypos;
   int      offscl;


   /********************************/
   /* Find the location on the sky */
   /* of the center of the region  */
   /********************************/

   rewind(fout);

   naxis1 = wcs->nxpix;
   naxis2 = wcs->nypix;

   x = (wcs->nxpix+1)/2.;
   y = (wcs->nypix+1)/2.;

   pix2wcs(wcs, x, y, &xpos, &ypos);

   strcpy(header, "");
   sprintf(temp, "SIMPLE  = T"                      ); 
   stradd(header, temp); 
   fprintf(fout, "%s\n", temp);

   sprintf(temp, "BITPIX  = -64"                    ); 
   stradd(header, temp); 
   fprintf(fout, "%s\n", temp);

   sprintf(temp, "NAXIS   = 2"                      ); 
   stradd(header, temp); 
   fprintf(fout, "%s\n", temp);

   sprintf(temp, "NAXIS1  = %d",          naxis1    ); 
   stradd(header, temp); 
   fprintf(fout, "%s\n", temp);

   sprintf(temp, "NAXIS2  = %d",          naxis2    ); 
   stradd(header, temp); 
   fprintf(fout, "%s\n", temp);

   if(strncmp(wcs->c1type, "RA", 2) == 0)
   {
      sprintf(temp, "CTYPE1  = 'RA---TAN-SIP'");
      stradd(header, temp); 
      fprintf(fout, "%s\n", temp);

      sprintf(temp, "CTYPE2  = 'DEC--TAN-SIP'");
      stradd(header, temp); 
      fprintf(fout, "%s\n", temp);
   }
   else
   {
      sprintf(temp, "CTYPE1  = '%s-TAN-SIP'", wcs->c1type);
      stradd(header, temp); 
      fprintf(fout, "%s\n", temp);

      sprintf(temp, "CTYPE2  = '%s-TAN-SIP'", wcs->c2type); 
      stradd(header, temp); 
      fprintf(fout, "%s\n", temp);
   }

   sprintf(temp, "CRVAL1  = %15.10f",  xpos         ); 
   stradd(header, temp); 
   fprintf(fout, "%s\n", temp);

   sprintf(temp, "CRVAL2  = %15.10f",  ypos         ); 
   stradd(header, temp); 
   fprintf(fout, "%s\n", temp);

   sprintf(temp, "CRPIX1  = %15.10f",  x            ); 
   stradd(header, temp); 
   fprintf(fout, "%s\n", temp);

   sprintf(temp, "CRPIX2  = %15.10f",  y            ); 
   stradd(header, temp); 
   fprintf(fout, "%s\n", temp);

   if(haveCdelt1)
   {
      sprintf(temp, "CDELT1  = %.10f",  pcdelt1     ); 
      stradd(header, temp); 
      fprintf(fout, "%s\n", temp);
   }

   if(haveCdelt2)
   {
      sprintf(temp, "CDELT2  = %.10f",  pcdelt2     ); 
      stradd(header, temp); 
      fprintf(fout, "%s\n", temp);
   }

   if(haveCrota2)
   {
      sprintf(temp, "CROTA2  = %s",  crota2       ); 
      stradd(header, temp); 
      fprintf(fout, "%s\n", temp);
   }

   if(haveCD11)
   {
      sprintf(temp, "CD1_1   = %s",  cd11         ); 
      stradd(header, temp); 
      fprintf(fout, "%s\n", temp);
   }

   if(haveCD12)
   {
      sprintf(temp, "CD1_2   = %s",  cd12         ); 
      stradd(header, temp); 
      fprintf(fout, "%s\n", temp);
   }

   if(haveCD21)
   {
      sprintf(temp, "CD2_1   = %s",  cd21         ); 
      stradd(header, temp); 
      fprintf(fout, "%s\n", temp);
   }

   if(haveCD22)
   {
      sprintf(temp, "CD2_2   = %s",  cd22         ); 
      stradd(header, temp); 
      fprintf(fout, "%s\n", temp);
   }

   if(havePC11)
   {
      sprintf(temp, "PC1_1   = %s",  pc11         ); 
      stradd(header, temp); 
      fprintf(fout, "%s\n", temp);
   }

   if(havePC12)
   {
      sprintf(temp, "PC1_2   = %s",  pc12         ); 
      stradd(header, temp); 
      fprintf(fout, "%s\n", temp);
   }

   if(havePC21)
   {
      sprintf(temp, "PC2_1   = %s",  pc21         ); 
      stradd(header, temp); 
      fprintf(fout, "%s\n", temp);
   }

   if(havePC22)
   {
      sprintf(temp, "PC2_2   = %s",  pc22         ); 
      stradd(header, temp); 
      fprintf(fout, "%s\n", temp);
   }

   if(haveEpoch)
   {
      sprintf(temp, "EPOCH   = %s",  epoch        ); 
      stradd(header, temp); 
      fprintf(fout, "%s\n", temp);
   }

   if(haveEquinox)
   {
      sprintf(temp, "EQUINOX = %s",  equinox      ); 
      stradd(header, temp); 
      fprintf(fout, "%s\n", temp);
   }


   sprintf(temp, "A_ORDER = %d", order-1);
   stradd(header, temp); 

   fprintf(fout, "%s\n", temp);

   for(iorder=0; iorder<order; ++iorder)
   {
      for(jorder=0; jorder<order; ++jorder)
      {
	 /* if(a[iorder][jorder] != 0.0) */
	 {
	    sprintf(temp, "A_%d_%d   = %10.3e",
	       iorder, jorder, a[iorder][jorder]);
	    stradd(header, temp); 

	    fprintf(fout, "%s\n", temp);
	 }
      }
   }

   sprintf(temp, "B_ORDER = %d", order-1);
   stradd(header, temp); 

   fprintf(fout, "%s\n", temp);

   for(iorder=0; iorder<order; ++iorder)
   {
      for(jorder=0; jorder<order; ++jorder)
      {
	 /* if(b[iorder][jorder] != 0.0) */
	 {
	    sprintf(temp, "B_%d_%d   = %10.3e",
	       iorder, jorder, b[iorder][jorder]);
	    stradd(header, temp); 

	    fprintf(fout, "%s\n", temp);
	 }
      }
   }


   sprintf(temp, "AP_ORDER= %d", order-1);
   stradd(header, temp); 

   fprintf(fout, "%s\n", temp);

   for(iorder=0; iorder<order; ++iorder)
   {
      for(jorder=0; jorder<order; ++jorder)
      {
	 /* if(ap[iorder][jorder] != 0.0) */
	 {
	    sprintf(temp, "AP_%d_%d  = %10.3e",
	       iorder, jorder, ap[iorder][jorder]);
	    stradd(header, temp); 

	    fprintf(fout, "%s\n", temp);
	 }
      }
   }

   sprintf(temp, "BP_ORDER= %d", order-1);
   stradd(header, temp); 

   fprintf(fout, "%s\n", temp);

   for(iorder=0; iorder<order; ++iorder)
   {
      for(jorder=0; jorder<order; ++jorder)
      {
	 /* if(bp[iorder][jorder] != 0.0) */
	 {
	    sprintf(temp, "BP_%d_%d  = %10.3e",
	       iorder, jorder, bp[iorder][jorder]);
	    stradd(header, temp); 

	    fprintf(fout, "%s\n", temp);
	 }
      }
   }

   sprintf(temp, "END"); stradd(header, temp); 

   fprintf(fout, "%s\n", temp);

   if(debug)
   {
      printf("\nDEBUG> Distorted TAN Header:\n\n");
      printHeader(header);
      fflush(stdout);
   }


   /****************************************************/
   /* Initialize the WCS library with this FITS header */
   /****************************************************/

   WCS = wcsinit(header);

   if(WCS == (struct WorldCoor *)NULL)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Output wcsinit() failed.\"]\n");
      exit(1);
   }

   if(debug)
   {
      printf("DEBUG> Distorted TAN WCS initialized\n\n");
      fflush(stdout);
   }

   fflush(fout);
   return 0;
}


/****************************************************/
/*                                                  */
/* Find the distance between two points on the sky. */
/*                                                  */
/****************************************************/

double distance(double ra, double dec, double rao, double deco)
{
   double d;
   double x, y, z;
   double xo, yo, zo;

   x = cos(ra*dtr) * cos(dec*dtr);
   y = sin(ra*dtr) * cos(dec*dtr);
   z = sin(dec*dtr);

   xo = cos(rao*dtr) * cos(deco*dtr);
   yo = sin(rao*dtr) * cos(deco*dtr);
   zo = sin(deco*dtr);

   d = acos(x*xo + y*yo + z*zo)/dtr;

   return(d);
}


/*********************************************************/
/*                                                       */
/* Format the FITS header string and print out the lines */
/*                                                       */
/*********************************************************/

int printHeader(char *header)
{
   int  i, j, len, linecnt;
   char line[81];

   len = strlen(header);

   linecnt = 0;

   while(1)
   {
      for(i=0; i<80; ++i)
         line[i] = '\0';

      for(i=0; i<80; ++i)
      {
         j = linecnt * 80 + i;
         
         if(j > len)
            break;

         line[i] = header[j];
      }
      
      line[80] = '\0';

      for(i=80; i>=0; --i)
      {
         if(line[i] != ' ' && line[i] != '\0')
            break;
         else
            line[i] = '\0';
      }
      
      if(strlen(line) > 0)
         printf("%4d: %s\n", linecnt+1, line);

      if(j > len)
         break;

      ++linecnt;
   }

   printf("\n");
   return 0;
}

      
/***********************************/
/*                                 */
/*  Performs least-squares         */
/*  simultaneous equation solution */
/*                                 */
/***********************************/

void gaussj(double **a, int n, double **b, int m)
{
   int   *indxc, *indxr, *ipiv;
   int    i, icol, irow, j, k, l, ll;
   double big, dum, pivinv, temp;

   int    maxi, maxj;
   double corr, maxcor;

   indxc = ivector(n);
   indxr = ivector(n);
   ipiv  = ivector(n);

   for (j=0; j<n; j++) 
      ipiv[j] = 0;

   for (i=0; i<n; i++) 
   {
      big=0.0;

      for (j=0; j<n; j++)
      {
         if (ipiv[j] != 1)
	 {
            for (k=0; k<n; k++) 
	    {
               if (ipiv[k] == 0) 
	       {
                  if (fabs(a[j][k]) >= big) 
		  {
                     big = fabs(a[j][k]);

                     irow = j;
                     icol = k;
                  }
               }

	       else if (ipiv[k] > 1) 
		  nrerror("Singular Matrix-1");
            }
	 }
      }

      ++(ipiv[icol]);

      if (irow != icol) 
      {
         for (l=0; l<n; l++) SWAP(a[irow][l], a[icol][l])
         for (l=0; l<m; l++) SWAP(b[irow][l], b[icol][l])
      }

      indxr[i] = irow;
      indxc[i] = icol;

      if (a[icol][icol] == 0.0)
	 nrerror("Singular Matrix-2");

      pivinv=1.0/a[icol][icol];

      a[icol][icol]=1.0;

      for (l=0; l<n; l++) a[icol][l] *= pivinv;
      for (l=0; l<m; l++) b[icol][l] *= pivinv;

      for (ll=0; ll<n; ll++)
      {
         if (ll != icol) 
	 {
            dum=a[ll][icol];

            a[ll][icol]=0.0;

            for (l=0; l<n; l++) a[ll][l] -= a[icol][l]*dum;
            for (l=0; l<m; l++) b[ll][l] -= b[icol][l]*dum;
         }
      }
   }

   for (l=n-1;l>=0; l--) 
   {
      if (indxr[l] != indxc[l])
      {
         for (k=0; k<n; k++)
            SWAP(a[k][indxr[l]], a[k][indxc[l]]);
      }
   }

   if(debug)
   {
      printf("\n\nCorrelation Matrix:\n");

      maxcor =  0.;
      maxi   = -1;
      maxj   = -1;

      for(j=0; j<n; ++j)
      {
	 for(i=0; i<n; ++i)
	 {
	    corr = a[i][j] /sqrt(fabs(a[i][i] * a[j][j]));

	    printf("%5.2f ", corr);

	    if(i != j)
	    {
	       if(fabs(corr) > maxcor)
	       {
		  maxcor = fabs(corr);

		  maxi = i;
		  maxj = j;
	       }
	    }
	 }
      
	 printf("\n");
      }

      printf("\nMaximum correlation: %.5f at (%d,%d)\n\n",
	 maxcor, maxi, maxj);
   }

   free_ivector(ipiv);
   free_ivector(indxr);
   free_ivector(indxc);
}


/* Prints out an error message */

void nrerror(char *error_text)
{
	fprintf(fstatus, "[struct stat=\"ERROR\" msg=\"%s\"]\n", error_text);
	exit(1);
}


/* Allocates memory for an array of integers */

int *ivector(int nh)
{
	int *v;

	v=(int *)malloc((size_t) (nh*sizeof(int)));

	if (!v) 
	   nrerror("Allocation failure in ivector()");

	return v;
}


/* Frees memory allocated by ivector */

void free_ivector(int *v)
{
	free((char *) v);
}
