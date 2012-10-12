/* Module: mTANHdr.c

Version  Developer        Date     Change
-------  ---------------  -------  -----------------------
1.4      John Good        09Feb05  When using DSS projection, the WCS library
                                   can incorrectly mistake distortion for
                                   rotation (in what it returns for wcs->rot).
                                   In this case, we fix that rotation at zero.
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

double   rotationAngle();

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

int      switchSys, wcsSys, WCSsys;
double   EQUINOX;
char     ctype1[5], ctype2[5];
double   rotation;

extern char *optarg;
extern int optind, opterr;

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
/*************************************************************************/

int main(int argc, char **argv)
{
   char     origtmpl[MAXSTR];
   char     newtmpl [MAXSTR];
   char     csys    [MAXSTR];

   int      iter;
   int      i, j, n, m, c;
   int      ii, ij, ji, jj;
   int      ipix, jpix;
   int      iorder, jorder;
   int      stepsize, smallstep, offscl;

   double   x, y;
   double   ix, iy;
   double   xpos, ypos;
   double   xout, yout;
   double   xpow, ypow;
   double   z;

   double **matrix;
   double **vector;
   double   change;
   double   fwdmaxx, fwdmaxy;
   double   revmaxx, revmaxy;

   int      maxiter;
   int      fwditer = 0;
   int      reviter = 0;
   double   tolerance;

   char    *end;


   /************************************/
   /* Read the command-line parameters */
   /************************************/

   switchSys =  0;
   opterr    =  0;
   debug     =  0;
   order     =  4;
   maxiter   = 50;
   tolerance =  0.01;
   fstatus   = stdout;
   EQUINOX   = -1.;

   while ((c = getopt(argc, argv, "e:c:di:o:t:s:")) != EOF) 
   {
      switch (c) 
      {
	 case 'e':
	    EQUINOX = strtod(optarg, &end);

	    if(end < optarg + strlen(optarg))
	    {
	       printf("[struct stat=\"ERROR\", msg=\"-e (equinox) argument \"%s\" not a real number\"]\n", optarg);
	       fflush(stdout);
	       exit(1);
	    }

            break;

	 case 'c':
	    strcpy(csys, optarg);

	    for(i=0; i<strlen(csys); ++i)
	       csys[i] = tolower(csys[i]);

	    if(strncmp(csys, "eq", 2) == 0)
	    {
	       switchSys = 1;
	       WCSsys    = EQUJ;

	       strcpy(ctype1, "RA--");
	       strcpy(ctype2, "DEC-");
	    }

	    else if(strncmp(csys, "ec", 2) == 0)
	    {
	       switchSys = 1;
	       WCSsys    = ECLJ;

	       strcpy(ctype1, "ELON");
	       strcpy(ctype2, "ELAT");
	    }

	    else if(strncmp(csys, "ga", 2) == 0)
	    {
	       switchSys = 1;
	       WCSsys    = GAL;

	       strcpy(ctype1, "GLON");
	       strcpy(ctype2, "GLAT");
	    }

	    else
	    {
	       printf("[struct stat=\"ERROR\", msg=\"-c (coordinate system) argument \"%s\" is not valid (eq, ec, or ga)\"]\n", optarg);
	       fflush(stdout);
	       exit(1);
	    }

	    break;

         case 'd':
            debug = 1;
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
	    fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Usage: %s [-d][-c csys][-e equinox][-o order][-i maxiter][-t tolerance][-s statusfile] orig.hdr new.hdr (default: order = 4, maxiter = 50, tolerance = 0.01)\"]\n", 
	       argv[0]);
            exit(1);
            break;
      }
   }

   if (argc - optind < 2) 
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Usage: %s [-d][-c csys][-e equinox][-o order][-i maxiter][-t tolerance][-s statusfile] orig.hdr new.hdr (default: order = 4, maxiter = 50, tolerance = 0.01)\"]\n", 
	 argv[0]);
      exit(1);
   }

   strcpy(origtmpl, argv[optind]);
   strcpy(newtmpl,  argv[optind + 1]);

   checkHdr(origtmpl, 1, 0);

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

   if(EQUINOX < 0.)
      EQUINOX = wcs->equinox;


   /*****************************************/
   /* Find the rotation angle between       */
   /* the output coordinate system and the  */
   /* template                              */
   /*****************************************/

   rotation = rotationAngle();


   /*****************************************/
   /* Build an initial TAN FITS header with */
   /* no distortions                        */
   /*****************************************/

   makeWCS();


   /******************************************************/
   /* Since the x and y directions are solved separately */
   /* (and affect each other), we have to iterate        */
   /******************************************************/

   stepsize = wcs->nxpix;

   if(wcs->nypix < stepsize)
      stepsize = wcs->nypix;

   stepsize = stepsize  /  5.;

   smallstep = stepsize / 10;

   if(stepsize < 1)
      stepsize = 1;

   if(smallstep < 1)
      smallstep = 1;


   for (ipix=0; ipix<wcs->nxpix+1; ipix+=smallstep)
   {
      for (jpix=0; jpix<wcs->nypix+1; jpix+=smallstep)
      {
	 ix = ipix - 0.5;
	 iy = jpix - 0.5;

         printf("XXX> ix, iy = %-g, %-g ", ix, iy);

	 pix2wcs(WCS, ix, iy, &xpos, &ypos);

         printf("-> xpos, ypos = %-g, %-g ", xpos, ypos);

	 wcs2pix(WCS, xpos, xpos, &x, &y, &offscl);

         printf("-> ix, iy = %-g, %-g (%d)\n", x, y, offscl);
      }
   }

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

readTemplate(char *template)
{
   int       i, j;
   FILE     *fp;
   char      line[MAXSTR];
   char      header[80000];

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

   if(strncmp(wcs->c1type, "RA", 2) == 0) wcsSys = EQUJ;
   if(strncmp(wcs->c1type, "EL", 2) == 0) wcsSys = ECLJ;
   if(strncmp(wcs->c1type, "GL", 2) == 0) wcsSys = GAL;

   if(debug)
   {
      printf("DEBUG> Original image WCS initialized\n\n");
      fflush(stdout);
   }

   fclose(fp);

   return;
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

int makeWCS(int set)
{
   char     header  [32768];
   char     temp    [MAXSTR];

   int      naxis1, naxis2;
   int      iorder, jorder;

   double   x, y;
   double   xpos, ypos;


   /********************************/
   /* Find the location on the sky */
   /* of the center of the region  */
   /********************************/

   naxis1 = wcs->nxpix;
   naxis2 = wcs->nypix;

   x = (wcs->nxpix+1)/2.;
   y = (wcs->nypix+1)/2.;

   pix2wcs(wcs, x, y, &xpos, &ypos);


   strcpy(header, "");
   sprintf(temp, "SIMPLE  = T"                      ); stradd(header, temp);
   sprintf(temp, "BITPIX  = -64"                    ); stradd(header, temp);
   sprintf(temp, "NAXIS   = 2"                      ); stradd(header, temp);
   sprintf(temp, "NAXIS1  = %d",          naxis1    ); stradd(header, temp);
   sprintf(temp, "NAXIS2  = %d",          naxis2    ); stradd(header, temp);

   if(strncmp(wcs->c1type, "RA", 2) == 0)
   {
      sprintf(temp, "CTYPE1  = 'RA---TAN-SIP'");
      stradd(header, temp);
      sprintf(temp, "CTYPE2  = 'DEC--TAN-SIP'");
      stradd(header, temp);
   }
   else
   {
      sprintf(temp, "CTYPE1  = '%s-TAN-SIP'", wcs->c1type);
      stradd(header, temp);
      sprintf(temp, "CTYPE2  = '%s-TAN-SIP'", wcs->c2type); 
      stradd(header, temp);
   }

   sprintf(temp, "CRVAL1  = %15.10f",  xpos         ); stradd(header, temp);
   sprintf(temp, "CRVAL2  = %15.10f",  ypos         ); stradd(header, temp);
   sprintf(temp, "CRPIX1  = %15.10f",  x            ); stradd(header, temp);
   sprintf(temp, "CRPIX2  = %15.10f",  y            ); stradd(header, temp);
   sprintf(temp, "CDELT1  = %15.10f",  wcs->cdelt[0]); straddheader, temp);
   sprintf(temp, "CDELT2  = %15.10f",  wcs->cdelt[1]); stradd(header, temp);

   if(strcmp(wcs->ptype, "DSS") == 0)
     {sprintf(temp, "CROTA2  = %15.10f",  0.        ); stradd(header, temp);}
   else
     {sprintf(temp, "CROTA2  = %15.10f",  wcs->rot  ); stradd(header, temp);}

   sprintf(temp, "EQUINOX = %7.2f",    wcs->equinox ); stradd(header, temp);

   sprintf(temp, "A_ORDER = %d", order-1);
   stradd(header, temp);

   for(iorder=0; iorder<order; ++iorder)
   {
      for(jorder=0; jorder<order; ++jorder)
      {
	 if(a[iorder][jorder] != 0.0)
	 {
	    sprintf(temp, "A_%d_%d   = %10.3e",
	       iorder, jorder, a[iorder][jorder]);
	    stradd(header, temp);
	 }
      }
   }

   sprintf(temp, "B_ORDER = %d", order-1);
   stradd(header, temp);

   for(iorder=0; iorder<order; ++iorder)
   {
      for(jorder=0; jorder<order; ++jorder)
      {
	 if(b[iorder][jorder] != 0.0)
	 {
	    sprintf(temp, "B_%d_%d   = %10.3e",
	       iorder, jorder, b[iorder][jorder]);
	    stradd(header, temp);
	 }
      }
   }


   sprintf(temp, "AP_ORDER= %d", order-1);
   stradd(header, temp);

   for(iorder=0; iorder<order; ++iorder)
   {
      for(jorder=0; jorder<order; ++jorder)
      {
	 if(ap[iorder][jorder] != 0.0)
	 {
	    sprintf(temp, "AP_%d_%d  = %10.3e",
	       iorder, jorder, ap[iorder][jorder]);
	    stradd(header, temp);
	 }
      }
   }

   sprintf(temp, "BP_ORDER= %d", order-1);
   stradd(header, temp);

   for(iorder=0; iorder<order; ++iorder)
   {
      for(jorder=0; jorder<order; ++jorder)
      {
	 if(bp[iorder][jorder] != 0.0)
	 {
	    sprintf(temp, "BP_%d_%d  = %10.3e",
	       iorder, jorder, bp[iorder][jorder]);
	    stradd(header, temp);
	 }
      }
   }

   sprintf(temp, "END"); stradd(header, temp);

   if(debug)
   {
      printf("\nDEBUG> Distorted TAN Header:\n\n");
      printHeader(header);
      fflush(stdout);
   }


   /*************************************************/
   /* Initial the WCS library with this FITS header */
   /*************************************************/

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

   return 0;
}



/***********************************/
/*                                 */
/*  Write out distorted TAN header */
/*                                 */
/***********************************/

int writeHdr()
{
   int      naxis1, naxis2;
   int      iorder, jorder;

   double   x, y;
   double   xpos, ypos;
   double   xout, yout;
   double   A;

   char     header  [32768];
   char     temp    [MAXSTR];

   strcpy(header, "");


   /********************************/
   /* Find the location on the sky */
   /* of the center of the region  */
   /********************************/

   naxis1 = wcs->nxpix;
   naxis2 = wcs->nypix;

   x = (wcs->nxpix+1)/2.;
   y = (wcs->nypix+1)/2.;

   pix2wcs(wcs, x, y, &xpos, &ypos);

   if(debug)
   {
      printf("\n\nDEBUG> writeHeader()\n");
      printf("DEBUG> image center: %-g,%-g (%s)\n", xpos, ypos, wcs->c1type);
      fflush(stdout);
   }

   fprintf(fout, "SIMPLE  = T\n"                      ); 
   fprintf(fout, "BITPIX  = -64\n"                    ); 
   fprintf(fout, "NAXIS   = 2\n"                      ); 
   fprintf(fout, "NAXIS1  = %d\n",          naxis1    ); 
   fprintf(fout, "NAXIS2  = %d\n",          naxis2    ); 
   fflush(fout);

   sprintf(temp, "SIMPLE  = T"                        ); stradd(header, temp);
   sprintf(temp, "BITPIX  = -64"                      ); stradd(header, temp);
   sprintf(temp, "NAXIS   = 2"                        ); stradd(header, temp);
   sprintf(temp, "NAXIS1  = %d",            naxis1    ); stradd(header, temp);
   sprintf(temp, "NAXIS2  = %d",            naxis2    ); stradd(header, temp);

   if(switchSys && strncmp(ctype1, wcs->c1type,2) != 0)
   {
      fprintf(fout, "CTYPE1  = '%s-TAN-SIP'\n", ctype1);
      fprintf(fout, "CTYPE2  = '%s-TAN-SIP'\n", ctype2);
      fflush(fout);

      sprintf(temp, "CTYPE1  = '%s-TAN-SIP'",   ctype1); stradd(header, temp);
      sprintf(temp, "CTYPE2  = '%s-TAN-SIP'",   ctype2); stradd(header, temp);
   }
   else
   {
      if(strncmp(wcs->c1type, "RA", 2) == 0)
      {
	 fprintf(fout, "CTYPE1  = 'RA---TAN-SIP'\n");
	 fprintf(fout, "CTYPE2  = 'DEC--TAN-SIP'\n");
         fflush(fout);

	 sprintf(temp, "CTYPE1  = 'RA---TAN-SIP'"  ); stradd(header, temp);
	 sprintf(temp, "CTYPE2  = 'DEC--TAN-SIP'"  ); stradd(header, temp);
      }
      else
      {
	 fprintf(fout, "CTYPE1  = '%s-TAN-SIP'\n", wcs->c1type);
	 fprintf(fout, "CTYPE2  = '%s-TAN-SIP'\n", wcs->c2type); 
         fflush(fout);

	 sprintf(temp, "CTYPE1  = '%s-TAN-SIP'",   wcs->c1type); stradd(header, temp);
	 sprintf(temp, "CTYPE2  = '%s-TAN-SIP'",   wcs->c2type); stradd(header, temp);
      }
   }

   if(switchSys && strncmp(ctype1, wcs->c1type,2) != 0)
   {
      convertCoordinates(wcsSys, wcs->equinox,  xpos,  ypos,
                         WCSsys, wcs->equinox, &xout, &yout, 0.);

      if(debug)
      {
	 printf("DEBUG> Reference coordinates:\n");
	 printf("       wcsSys  = %d\n",      wcsSys);
	 printf("       equinox = %7.2f\n",   wcs->equinox);
	 printf("       xpos    = %15.10f\n", xpos);
	 printf("       ypos    = %15.10f\n", ypos);
	 printf("    -> WCSsys  = %d\n",      WCSsys);
	 printf("       equinox = %7.2f\n",   wcs->equinox);
	 printf("       xout    = %15.10f\n", xout);
	 printf("       yout    = %15.10f\n", yout);
	 fflush(stdout);
      }

      fprintf(fout, "CRVAL1  = %15.10f\n", xout );
      fprintf(fout, "CRVAL2  = %15.10f\n", yout );
      fflush(fout);

      sprintf(temp, "CRVAL1  = %15.10f",   xout );  stradd(header, temp);
      sprintf(temp, "CRVAL2  = %15.10f",   yout );  stradd(header, temp);
   }
   else
   {
      fprintf(fout, "CRVAL1  = %15.10f\n", xpos); 
      fprintf(fout, "CRVAL2  = %15.10f\n", ypos); 
      fflush(fout);

      sprintf(temp, "CRVAL1  = %15.10f",   xpos);  stradd(header, temp);
      sprintf(temp, "CRVAL2  = %15.10f",   ypos);  stradd(header, temp);
   }

   fprintf(fout, "CRPIX1  = %15.10f\n",   x                ); 
   fprintf(fout, "CRPIX2  = %15.10f\n",   y                ); 
   fprintf(fout, "CDELT1  = %15.10f\n",   wcs->cdelt[0]    ); 
   fprintf(fout, "CDELT2  = %15.10f\n",   wcs->cdelt[1]    ); 
   fflush(fout);

   sprintf(temp, "CRPIX1  = %15.10f",     x                );  stradd(header, temp);
   sprintf(temp, "CRPIX2  = %15.10f",     y                );  stradd(header, temp);
   sprintf(temp, "CDELT1  = %15.10f",     wcs->cdelt[0]    );  stradd(header, temp);
   sprintf(temp, "CDELT2  = %15.10f",     wcs->cdelt[1]    );  stradd(header, temp);

   if(strcmp(wcs->ptype, "DSS") == 0)
     fprintf(fout, "CROTA2  = %15.10f\n",  rotation         );
   else
     fprintf(fout, "CROTA2  = %15.10f\n",  wcs->rot+rotation);

   fprintf(fout, "EQUINOX = %7.2f\n",     wcs->equinox     ); 
   fflush(fout);

   if(strcmp(wcs->ptype, "DSS") == 0)
     {sprintf(temp, "CROTA2  = %15.10f",  rotation         );  stradd(header, temp);}
   else
     {sprintf(temp, "CROTA2  = %15.10f",  wcs->rot+rotation);  stradd(header, temp);}

   sprintf(temp, "EQUINOX = %7.2f",       wcs->equinox     );  stradd(header, temp);

   fprintf(fout, "A_ORDER = %d\n", order-1);
   fflush(fout);
   
   sprintf(temp, "A_ORDER = %d",   order-1); stradd(header, temp);

   for(iorder=0; iorder<order; ++iorder)
   {
      for(jorder=0; jorder<order; ++jorder)
      {
	 if(a[iorder][jorder] != 0.0)
	 {
	    fprintf(fout, "A_%d_%d   = %10.3e\n",
	       iorder, jorder, a[iorder][jorder]);
	    fflush(fout);

	    sprintf(temp, "A_%d_%d   = %10.3e",
	       iorder, jorder, a[iorder][jorder]);
	    stradd(header, temp);
	 }
      }
   }

   fprintf(fout, "B_ORDER = %d\n", order-1);
   fflush(fout);
   
   sprintf(temp, "B_ORDER = %d",   order-1); stradd(header, temp);

   for(iorder=0; iorder<order; ++iorder)
   {
      for(jorder=0; jorder<order; ++jorder)
      {
	 if(b[iorder][jorder] != 0.0)
	 {
	    fprintf(fout, "B_%d_%d   = %10.3e\n",
	       iorder, jorder, b[iorder][jorder]);
	    fflush(fout);

	    sprintf(temp, "B_%d_%d   = %10.3e",
	       iorder, jorder, b[iorder][jorder]);
	    stradd(header, temp);
	 }
      }
   }

   fprintf(fout, "AP_ORDER= %d\n", order-1);
   fflush(fout);
  
   sprintf(temp, "AP_ORDER= %d",   order-1); stradd(header, temp);

   for(iorder=0; iorder<order; ++iorder)
   {
      for(jorder=0; jorder<order; ++jorder)
      {
	 if(ap[iorder][jorder] != 0.0)
	 {
	    fprintf(fout, "AP_%d_%d  = %10.3e\n",
	       iorder, jorder, ap[iorder][jorder]);
	    fflush(fout);

	    sprintf(temp, "AP_%d_%d  = %10.3e",
	       iorder, jorder, ap[iorder][jorder]);
	    stradd(header, temp);
	 }
      }
   }

   fprintf(fout, "BP_ORDER= %d\n", order-1);
   fflush(fout);

   sprintf(temp, "BP_ORDER= %d",   order-1);  stradd(header, temp);

   for(iorder=0; iorder<order; ++iorder)
   {
      for(jorder=0; jorder<order; ++jorder)
      {
	 if(bp[iorder][jorder] != 0.0)
	 {
	    fprintf(fout, "BP_%d_%d  = %10.3e\n",
	       iorder, jorder, bp[iorder][jorder]);
	    fflush(fout);

	    sprintf(temp, "BP_%d_%d  = %10.3e",
	       iorder, jorder, bp[iorder][jorder]);
	    stradd(header, temp);
	 }
      }
   }

   fprintf(fout, "END\n"); 
   fflush(fout);

   sprintf(temp, "END"  ); stradd(header, temp);
   fclose(fout);

   if(debug)
   {
      printf("\nDEBUG> Final distorted TAN Header:\n\n");
      printHeader(header);
      fflush(stdout);
   }


   /*************************************************/
   /* Initial the WCS library with this FITS header */
   /*************************************************/

   WCS = wcsinit(header);

   if(WCS == (struct WorldCoor *)NULL)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Output wcsinit() failed.\"]\n");
      exit(1);
   }

   if(debug)
   {
      printf("DEBUG> Distorted TAN WCS initialized and header written to output\n\n");
      fflush(stdout);
   }

   return 0;
}



/***************************************/
/*                                     */
/*  Find the rotation angle equivalent */
/*  to the coordinate system change    */
/*                                     */
/***************************************/

double rotationAngle()
{
   int      naxis1, naxis2;
   int      offscl;

   double   x,    y;
   double   xcen, ycen;
   double   Xcen, Ycen;
   double   xoff, yoff;
   double   Xoff, Yoff;
   double   xc,   yc;
   double   xo,   yo;
   double   A;

   char     header  [32768];
   char     temp    [MAXSTR];

   double   dtr;


   dtr = atan(1.0)/45.;

   strcpy(header, "");



   /********************************/
   /* If no system change, get out */
   /********************************/

   if(!switchSys 
   || strncmp(ctype1, wcs->c1type,2) == 0)
   {
      if(debug)
      {
         printf("DEBUG> No system change, rotation angle is zero\n");
	 fflush(stdout);
      }

      return 0.;
   }


   /*********************************/
   /* Find the locations on the sky */
   /* of the center of the region   */
   /* and one pixel up              */
   /*********************************/

   naxis1 = wcs->nxpix;
   naxis2 = wcs->nypix;

   x = (wcs->nxpix+1)/2.;
   y = (wcs->nypix+1)/2.;

   pix2wcs(wcs, x, y, &xcen, &ycen);

   if(debug)
   {
      printf("DEBUG>    image center: %-g,%-g -> %-g,%-g (%s)\n",
              x, y, xcen, ycen, wcs->c1type);
      fflush(stdout);
   }

   
   if(wcs->cdelt[1] > 0.)
      y = y + 1.0;
   else
      y = y - 1.0;

   pix2wcs(wcs, x, y, &xoff, &yoff);

   if(debug)
   {
      printf("DEBUG> offset location: %-g,%-g -> %-g,%-g (%s)\n", 
             x, y, xoff, yoff, wcs->c1type);
      fflush(stdout);
   }


   /*********************************/
   /* Convert these location to the */
   /* output coordinate system      */
   /*********************************/

   convertCoordinates(wcsSys, wcs->equinox,  xcen,  ycen,
		      WCSsys, wcs->equinox, &Xcen, &Ycen, 0.);

   if(debug)
   {
      printf("DEBUG> Reference coordinates:\n");
      printf("       wcsSys  = %d\n",      wcsSys);
      printf("       equinox = %7.2f\n",   wcs->equinox);
      printf("       xcen    = %15.10f\n", xcen);
      printf("       ycen    = %15.10f\n", ycen);
      printf("    -> WCSsys  = %d\n",      WCSsys);
      printf("       equinox = %7.2f\n",   wcs->equinox);
      printf("       Xcen    = %15.10f\n", Xcen);
      printf("       Ycen    = %15.10f\n", Ycen);
      fflush(stdout);
   }

   convertCoordinates(wcsSys, wcs->equinox,  xoff,  yoff,
		      WCSsys, wcs->equinox, &Xoff, &Yoff, 0.);

   if(debug)
   {
      printf("DEBUG> Reference coordinates:\n");
      printf("       wcsSys  = %d\n",      wcsSys);
      printf("       equinox = %7.2f\n",   wcs->equinox);
      printf("       xoff    = %15.10f\n", xoff);
      printf("       yoff    = %15.10f\n", yoff);
      printf("    -> WCSsys  = %d\n",      WCSsys);
      printf("       equinox = %7.2f\n",   wcs->equinox);
      printf("       Xoff    = %15.10f\n", Xoff);
      printf("       Yoff    = %15.10f\n", Yoff);
      fflush(stdout);
   }


   /************************/
   /* Build the WCS header */
   /************************/


   sprintf(temp, "SIMPLE  = T"                      ); stradd(header, temp);
   sprintf(temp, "BITPIX  = -64"                    ); stradd(header, temp);
   sprintf(temp, "NAXIS   = 2"                      ); stradd(header, temp);
   sprintf(temp, "NAXIS1  = %d",       naxis1       ); stradd(header, temp);
   sprintf(temp, "NAXIS2  = %d",       naxis2       ); stradd(header, temp);
   sprintf(temp, "CTYPE1  = '%s-TAN'", ctype1       ); stradd(header, temp);
   sprintf(temp, "CTYPE2  = '%s-TAN'", ctype2       ); stradd(header, temp);
   sprintf(temp, "CRVAL1  = %15.10f",  Xcen         ); stradd(header, temp);
   sprintf(temp, "CRVAL2  = %15.10f",  Ycen         ); stradd(header, temp);
   sprintf(temp, "CRPIX1  = %15.10f",  x            ); stradd(header, temp);
   sprintf(temp, "CRPIX2  = %15.10f",  y            ); stradd(header, temp);
   sprintf(temp, "CDELT1  = %15.10f",  wcs->cdelt[0]); stradd(header, temp);
   sprintf(temp, "CDELT2  = %15.10f",  wcs->cdelt[1]); stradd(header, temp);
   sprintf(temp, "CROTA2  = 0.00000"                ); stradd(header, temp);
   sprintf(temp, "EQUINOX = %7.2f",    wcs->equinox ); stradd(header, temp);
   sprintf(temp, "END"                              ); stradd(header, temp);

   if(debug)
   {
      printf("\nDEBUG> Unrotated TAN Header:\n\n");
      printHeader(header);
      fflush(stdout);
   }


   /*************************************************/
   /* Initial the WCS library with this FITS header */
   /*************************************************/

   WCS = wcsinit(header);

   if(WCS == (struct WorldCoor *)NULL)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Output wcsinit() failed.\"]\n");
      exit(1);
   }

   if(debug)
   {
      printf("DEBUG> Unrotated TAN WCS initialized \n\n");
      fflush(stdout);
   }

   
   /*************************************************/
   /* Transform center and offset locations on the  */
   /* sky into output pixel coordinates and find    */
   /* the rotation angle                            */
   /*************************************************/

   wcs2pix(WCS, Xcen, Ycen, &xc, &yc, &offscl);
   wcs2pix(WCS, Xoff, Yoff, &xo, &yo, &offscl);

   if(debug)
   {
      printf("DEBUG> Reference pixel location in new system  (%.4f,%.4f)\n", xc, yc);
      printf("DEBUG> Offset    pixel location in new system  (%.4f,%.4f)\n", xo, yo);
      fflush(stdout);
   }

   A = atan2(xo-xc, yo-yc) / dtr;
   
   if(debug)
   {
      printf("DEBUG> Rotation using unrotated WCS: %12.8f\n\n", A);
      fflush(stdout);
   }

   return A;
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
