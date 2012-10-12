/* Module: mBgModel.c

Version  Developer        Date     Change
-------  ---------------  -------  -----------------------
2.6      John Good        28Apr08  Add flag to turn off rejection 
                                   (e.g. for all-sky CAR)
2.5      John Good        31Mar08  Make the rejection limits parameters
2.4      John Good        05Sep06  Increase default iterations to 10000
2.3      John Good        15Jun04  Don't stop if we hit a bad matrix 
                                   inversion, just don't generate any
                                   corrections for that image.
2.2      John Good        27Aug04  Added "[-s statusfile]" to Usage statement
2.1      John Good        28Jul04  Shouldn't have had a lower RMS cutoff;
				   this removes overlaps that are actually
				   just part of a larger tiled image.
2.0      John Good        20Apr04  Changed pixel "sums" to use integral form
				   and allow for rotated overlap regions
1.9      John Good        09Mar04  Added "level-only" flag
1.8      John Good        07Mar04  Added checks for whether to use background 
                                   fit. Now must be at least 2% of average image
                                   area, have RMS no more than 2.0 times the
                                   average, and have linear dimensions of at
                                   least 25% that of one of the corresponding
                                   images.
1.7      John Good        25Nov03  Added extern optarg references
1.6      John Good        06Oct03  Added NAXIS1,2 as alternatives to ns,nl
1.5      John Good        25Aug03  Added status file processing
1.4      John Good        28May03  Changed fittype handling to allow arbitrarily
				   large number of iterations
1.3      John Good        24Mar03  Fixed max count in error in message 
				   statement for -i 
1.2      John Good        22Mar03  Fixed error in message statement for -i 
				   option, and corrected error where -i value 
				   was being overwritten, and turned on debug
				   if refimage given
1.1      John Good        14Mar03  Modified command-line processing
				   to use getopt() library.  Checks validity of
				   niteration.  Checks for missing/invalid
				   images.tbl or fits.tbl.
1.0      John Good        29Jan03  Baseline code

*/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <math.h>

#include "montage.h"
#include <mtbl.h>

#define MAXSTR  256
#define MAXCNT 1024

#define LEVEL 0
#define SLOPE 1
#define BOTH  2

#define SWAP(a,b) {temp=(a);(a)=(b);(b)=temp;}

extern char *optarg;
extern int optind, opterr;

int    gaussj(float **, int, float **, int);
int   *ivector(int);
void   free_ivector(int *);

extern char *optarg;
extern int optind, opterr;
extern int getopt(int argc, char *const *argv, const char *options);

int debugCheck(char *debugStr);


/*************************************************************************/
/*                                                                       */
/*  mBModel                                                              */
/*                                                                       */
/*  Given a set of image overlap difference fits (parameters on the      */
/*  planes fit to pairwise difference images between adjacent images)    */
/*  interatively determine the "best" background adjustment for each     */
/*  image (assuming each image is changed to best match its neighbors    */
/*  with them remaining unchanged) uses these adjustments to modify      */
/*  each set of difference parameters, and iterate until the changes     */
/*  modifications become small enough.                                   */
/*                                                                       */
/*************************************************************************/


/* This structure contains the basic geometry alignment          */
/* information for images.  Since all images are aligned to the  */
/* same location on the sky (and the same projection, etc.,      */
/* aligning them in cartesian pixel space only requires knowing  */
/* their size and reference pixel coordinates                    */

struct ImgInfo
{
   int               cntr;
   int               naxis1;
   int               naxis2;
   double            crpix1;
   double            crpix2;
}
   *imgs;

int nimages, maximages;


/* This structure contains the information describing the */
/* plan to be subtracted from each image to "correct" it  */
/* to its neighbors.                                      */

struct FitInfo
{
   int    plus;
   int    minus;
   double a;
   double b;
   double c;
   double crpix1;
   double crpix2;
   int    xmin;
   int    xmax;
   int    ymin;
   int    ymax;
   double xcenter;
   double ycenter;
   int    npix;
   double rms;
   double Xmin;
   double Xmax;
   double Ymin;
   double Ymax;
   double boxangle;
   int    compl;
   int    use;

   struct CorrInfo *plusimg;
   struct CorrInfo *minusimg;
}
  *fits;

int nfits, maxfits;


/* This structure contains the incremental           */
/* correction values to be applied to each image.    */
/* It is used to update the FitInfo structure above. */


struct CorrInfo
{
   int    id;

   double a;
   double b;
   double c;

   double acorrection;
   double bcorrection;
   double ccorrection;

   struct FitInfo **neighbors;

   int nneighbors;
   int maxneighbors;
}
   *corrs;

int ncorrs, maxcorrs;


int debug;


int main(int argc, char **argv)
{
   int     i, j, k, c, index, stat;
   int     noslope, ncols, iteration, istatus;
   int     niteration, maxlevel, useall, refimage;
   double  averms, sigrms, avearea;
   double  imin, imax, jmin, jmax;
   double  A, B, C;
   char    imgfile[MAXSTR];
   char    fitfile[MAXSTR];
   char    corrtbl[MAXSTR];
   FILE   *fout;

   int     iplus;
   int     iminus;
   int     ia;
   int     ib;
   int     ic;
   int     icrpix1;
   int     icrpix2;
   int     ixmin;
   int     ixmax;
   int     iymin;
   int     iymax;
   int     ixcenter;
   int     iycenter;
   int     inpix;
   int     irms;
   int     iboxx;
   int     iboxy;
   int     iboxwidth;
   int     iboxheight;
   int     iboxangle;
   int     icntr;
   int     inl;
   int     ins;

   int     fittype;

   double  boxx, boxy;
   double  width, height;
   double  angle;
   double  X0, Y0;

   double  sumxx, sumyy, sumxy, sumx, sumy, sumn;
   double  dsumxx, dsumyy, dsumxy, dsumx, dsumy, dsumn;
   double  sumxz, sumyz, sumz;

   double  dtr;
   double  Xmin, Xmax, Ymin, Ymax;
   double  theta, sinTheta, cosTheta;

   double  xfsize,  yfsize;
   double  xisize1, yisize1;
   double  xisize2, yisize2;

   double  linearLimit = 0.25;
   double  sigmaLimit  = 2.00;
   double  areaLimit   = 0.002;

   char   *end;


   /* Simultaneous equation stuff */

   float **a;
   int     n;
   float **b;
   int     m;

   dtr = atan(1) / 45.;


   /***************************************************************/
   /* Allocate matrix space (for solving least-squares equations) */
   /***************************************************************/

   n = 3;

   a = (float **)malloc(n*sizeof(float *));

   for(i=0; i<n; ++i)
      a[i] = (float *)malloc(n*sizeof(float));


   /*************************/
   /* Allocate vector space */
   /*************************/

   m = 1;

   b = (float **)malloc(n*sizeof(float *));

   for(i=0; i<n; ++i)
      b[i] = (float *)malloc(m*sizeof(float));



   /***************************************/
   /* Process the command-line parameters */
   /***************************************/

   debug  = 0;
   opterr = 0;

   noslope  = 0;
   useall   = 0;
   refimage = 0;

   niteration = 10000;
   maxlevel   =  2500;

   fstatus = stdout;

   while ((c = getopt(argc, argv, "ai:r:s:ld:")) != EOF) 
   {
      switch (c) 
      {
	 case 'a':
	    useall = 1;
	    break;

         case 'i':
	    niteration = strtol(optarg, &end, 0);

	    if(end < optarg + strlen(optarg))
	    {
	       printf("[struct stat=\"ERROR\", msg=\"Argument for -i (%s) cannot be interpreted as an integer\"]\n", 
		  optarg);
	       exit(1);
	    }

	    if(niteration < 1)
	    {
	       printf ("[struct stat=\"ERROR\", msg=\"Number of iterations too small (%d). This parameter is normally around 5000.\"]\n", i);
	       exit(1);
	    }

	    if(niteration < 5000)
	       maxlevel = niteration / 2.;

            break;

         case 'r':
	    refimage = strtol(optarg, &end, 0);

	    if(end < optarg + strlen(optarg))
	    {
	       printf("[struct stat=\"ERROR\", msg=\"Argument for -r (%s) cannot be interpreted as an integer\"]\n", 
		  optarg);
	       exit(1);
	    }

            break;

         case 's':
            if((fstatus = fopen(optarg, "w+")) == (FILE *)NULL)
            {
               printf ("[struct stat=\"ERROR\", msg=\"Cannot open status file: %s\"]\n",
                  optarg);
               exit(1);
            }
            break;

         case 'l':
	    noslope = 1;
            break;

         case 'd':
	    debug = debugCheck(optarg);
            break;

         default:
	    printf ("[struct stat=\"ERROR\", msg=\"Usage: %s [-i niter] [-l(evel-only)] [-d level] [-a(ll-overlaps)] [-r refimg] [-s statusfile] images.tbl fits.tbl corrections.tbl\"]\n", argv[0]);
            exit(1);
            break;
      }
   }

   if(refimage != 0 && debug == 0)
      debug = 1;

   if (argc - optind < 3) 
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Usage: %s [-i niter] [-l(evel-only)] [-d level] [-a(ll-overlaps)] [-r refimg] [-s statusfile] images.tbl fits.tbl corrections.tbl\"]\n", argv[0]);
      exit(1);
   }

   strcpy(imgfile, argv[optind]);
   strcpy(fitfile, argv[optind + 1]);
   strcpy(corrtbl, argv[optind + 2]);

   if(debug)
   {
      printf("niteration = %d\n", niteration);
      printf("noslope    = %d\n", noslope);
      printf("imgfile    = %s\n", imgfile);
      printf("fitfile    = %s\n", fitfile);
      printf("corrtbl    = %s\n", corrtbl);
      fflush(stdout);
   }

   fout = fopen(corrtbl, "w+");

   if(fout == (FILE *)NULL)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Failed to open output %s\"]\n",
         corrtbl);
      exit(1);
   }


   /*********************************************/ 
   /* Open the image header metadata table file */
   /*********************************************/ 

   ncols = topen(imgfile);

   if(ncols <= 0)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Invalid image metadata file: %s\"]\n",
	 imgfile);
      exit(1);
   }

   icntr    = tcol("cntr");
   inl      = tcol("nl");
   ins      = tcol("ns");
   icrpix1  = tcol("crpix1");
   icrpix2  = tcol("crpix2");

   if(ins < 0)
      ins = tcol("naxis1");

   if(inl < 0)
      inl = tcol("naxis2");

   if(icntr   < 0
   || inl     < 0
   || ins     < 0
   || icrpix1 < 0
   || icrpix2 < 0)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Need columns: cntr nl ns crpix1 crpix2 in image info file\"]\n");
      exit(1);
   }



   /******************************/ 
   /* Read the image information */ 
   /******************************/ 

   nimages   =      0;
   maximages = MAXCNT;

   imgs = (struct ImgInfo *)malloc(maximages * sizeof(struct ImgInfo));

   if(imgs == (struct ImgInfo *)NULL)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"malloc() failed (ImgInfo)\"]\n");
      exit(1);
   }


   avearea = 0.0;

   while(1)
   {
      stat = tread();

      if(stat < 0)
	 break;

      imgs[nimages].cntr      = atoi(tval(icntr));
      imgs[nimages].naxis1    = atoi(tval(ins));
      imgs[nimages].naxis2    = atoi(tval(inl));
      imgs[nimages].crpix1    = atof(tval(icrpix1));
      imgs[nimages].crpix2    = atof(tval(icrpix2));

      avearea += imgs[nimages].naxis1*imgs[nimages].naxis2;

      ++nimages;

      if(nimages >= maximages)
      {
	 maximages += MAXCNT;
	 imgs = (struct ImgInfo *)realloc(imgs, 
				      maximages * sizeof(struct ImgInfo));

	 if(imgs == (struct ImgInfo *)NULL)
	 {
	    fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"realloc() failed (ImgInfo)\"]\n");
	    exit(1);
	 }
      }
   }

   avearea = avearea / nimages;



   /**************************************/ 
   /* Open the difference fit table file */
   /**************************************/ 

   ncols = topen(fitfile);

   if(ncols <= 0)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Invalid background fit parameters file: %s\"]\n",
	 fitfile);
      exit(1);
   }

   iplus      = tcol("plus");
   iminus     = tcol("minus");
   ia         = tcol("a");
   ib         = tcol("b");
   ic         = tcol("c");
   icrpix1    = tcol("crpix1");
   icrpix2    = tcol("crpix2");
   ixmin      = tcol("xmin");
   ixmax      = tcol("xmax");
   iymin      = tcol("ymin");
   iymax      = tcol("ymax");
   ixcenter   = tcol("xcenter");
   iycenter   = tcol("ycenter");
   inpix      = tcol("npixel");
   irms       = tcol("rms");
   iboxx      = tcol("boxx");
   iboxy      = tcol("boxy");
   iboxwidth  = tcol("boxwidth");
   iboxheight = tcol("boxheight");
   iboxangle  = tcol("boxang");

   if(iplus      < 0
   || iminus     < 0
   || ia         < 0
   || ib         < 0
   || ic         < 0
   || icrpix1    < 0
   || icrpix2    < 0
   || ixmin      < 0
   || ixmax      < 0
   || iymin      < 0
   || iymax      < 0
   || ixcenter   < 0
   || iycenter   < 0
   || inpix      < 0
   || irms       < 0
   || iboxx      < 0
   || iboxy      < 0
   || iboxwidth  < 0
   || iboxheight < 0
   || iboxangle  < 0)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Need columns: plus minus a b c crpix1 crpix2 xmin xmax ymin ymax xcenter ycenter npixel rms boxx boxy boxwidth boxheight boxang\"]\n");
      exit(1);
   }



   /*****************/ 
   /* Read the fits */ 
   /*****************/ 

   nfits   =      0;
   maxfits = MAXCNT;

   fits = (struct FitInfo *)malloc(maxfits * sizeof(struct FitInfo));

   if(fits == (struct FitInfo *)NULL)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"malloc() failed (FitInfo)\"]\n");
      exit(1);
   }

   averms = 0.0;

   while(1)
   {
      stat = tread();

      if(stat < 0)
	 break;

      fits[nfits].plus      = atoi(tval(iplus));
      fits[nfits].minus     = atoi(tval(iminus));
      fits[nfits].a         = atof(tval(ia));
      fits[nfits].b         = atof(tval(ib));
      fits[nfits].c         = atof(tval(ic));
      fits[nfits].crpix1    = atof(tval(icrpix1));
      fits[nfits].crpix2    = atof(tval(icrpix2));
      fits[nfits].xmin      = atoi(tval(ixmin));
      fits[nfits].xmax      = atoi(tval(ixmax));
      fits[nfits].ymin      = atoi(tval(iymin));
      fits[nfits].ymax      = atoi(tval(iymax));
      fits[nfits].xcenter   = atof(tval(ixcenter));
      fits[nfits].ycenter   = atof(tval(iycenter));
      fits[nfits].npix      = atof(tval(inpix));
      fits[nfits].rms       = atof(tval(irms));

      boxx   = atof(tval(iboxx));
      boxy   = atof(tval(iboxy));
      width  = atof(tval(iboxwidth ));
      height = atof(tval(iboxheight));
      angle  = atof(tval(iboxangle)) * dtr;

      X0 =  boxx * cos(angle) + boxy * sin(angle);
      Y0 = -boxx * sin(angle) + boxy * cos(angle);

      fits[nfits].Xmin = X0 - width /2.;
      fits[nfits].Xmax = X0 + width /2.;
      fits[nfits].Ymin = Y0 - height/2.;
      fits[nfits].Ymax = Y0 + height/2.;

      fits[nfits].boxangle  = angle/dtr;
      fits[nfits].compl     = nfits+1;

      averms += fits[nfits].rms;

      ++nfits;

      if(nfits >= maxfits-2)
      {
	 maxfits += MAXCNT;
	 fits = (struct FitInfo *)realloc(fits, 
				     maxfits * sizeof(struct FitInfo));

	 if(fits == (struct FitInfo *)NULL)
	 {
	    fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"realloc() failed (FitInfo) [%d]\"]\n", 
	       maxfits * sizeof(struct FitInfo));
	    exit(1);

	 }
      }


      /* Use the same info for the complementary */
      /* comparison, with the fit reversed       */

      fits[nfits].plus     =  atoi(tval(iminus));
      fits[nfits].minus    =  atoi(tval(iplus));
      fits[nfits].a        = -atof(tval(ia));
      fits[nfits].b        = -atof(tval(ib));
      fits[nfits].c        = -atof(tval(ic));
      fits[nfits].xmin     =  atoi(tval(ixmin));
      fits[nfits].xmax     =  atoi(tval(ixmax));
      fits[nfits].ymin     =  atoi(tval(iymin));
      fits[nfits].ymax     =  atoi(tval(iymax));
      fits[nfits].xcenter  =  atof(tval(ixcenter));
      fits[nfits].ycenter  =  atof(tval(iycenter));
      fits[nfits].npix     =  atof(tval(inpix));
      fits[nfits].rms      =  atof(tval(irms));
      fits[nfits].Xmin     =  fits[nfits-1].Xmin;
      fits[nfits].Xmax     =  fits[nfits-1].Xmax;
      fits[nfits].Ymin     =  fits[nfits-1].Ymin;
      fits[nfits].Ymax     =  fits[nfits-1].Ymax;
      fits[nfits].boxangle =  fits[nfits-1].boxangle;
      fits[nfits].compl    =  nfits-1;

      ++nfits;

      if(nfits >= maxfits-2)
      {
	 maxfits += MAXCNT;
	 fits = (struct FitInfo *)realloc(fits, 
				     maxfits * sizeof(struct FitInfo));

	 if(fits == (struct FitInfo *)NULL)
	 {
	    fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"realloc() failed (FitInfo) [%d]\"]\n", 
	       maxfits * sizeof(struct FitInfo));
	    exit(1);
	 }
      }
   }

   averms = averms / nfits;


   /********************************************/
   /* From the fit information, initialize the */
   /* image structures                         */
   /********************************************/

   ncorrs   =      0;
   maxcorrs = MAXCNT;

   corrs = (struct CorrInfo *)malloc(maxcorrs * sizeof(struct CorrInfo));

   if(corrs == (struct CorrInfo *)NULL)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"malloc() failed (CorrInfo)\"]\n");
      exit(1);
   }

   for(i=0; i<maxcorrs; ++i)
   {
      corrs[i].id = -1;

      corrs[i].a = 0.;
      corrs[i].b = 0.;
      corrs[i].c = 0.;

      corrs[i].nneighbors = 0;
      corrs[i].maxneighbors = MAXCNT;

      corrs[i].neighbors 
	 = (struct FitInfo **)malloc(corrs[i].maxneighbors 
				     * sizeof(struct FitInfo *));

      if(corrs[i].neighbors == (struct FitInfo **)NULL)
      {
	 fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"malloc() failed (FitInfo *)\"]\n");
	 exit(1);
      }
   }

   for(k=0; k<nfits; ++k)
   {
      /* See if we already have a structure for this image */

      index = -1;

      for(j=0; j<ncorrs; ++j)
      {
	 if(corrs[j].id < 0)
	    break;
	 
	 if(corrs[j].id == fits[k].plus)
	 {
	    index = j;
	    break;
	 }
      }


      /* If not, get the next free one */

      if(index < 0)
      {
	 if(ncorrs >= maxcorrs)
	 {
	    maxcorrs += MAXCNT;

	    corrs = (struct CorrInfo *)realloc(corrs, maxcorrs * sizeof(struct CorrInfo));

	    if(corrs == (struct CorrInfo *)NULL)
	    {
	       fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"realloc() failed (CorrInfo)\"]\n");
	       exit(1);
	    }

	    for(i=maxcorrs-MAXCNT; i<maxcorrs; ++i)
	    {
	       corrs[i].id = -1;

	       corrs[i].a = 0.;
	       corrs[i].b = 0.;
	       corrs[i].c = 0.;

	       corrs[i].nneighbors = 0;
	       corrs[i].maxneighbors = MAXCNT;

	       corrs[i].neighbors 
		  = (struct FitInfo **)malloc(corrs[i].maxneighbors 
					      * sizeof(struct FitInfo *));

	       if(corrs[i].neighbors == (struct FitInfo **)NULL)
	       {
		  fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"malloc() failed (FitInfo *)\"]\n");
		  exit(1);
	       }
	    }
	 }

	 index = ncorrs;

	 corrs[index].id = fits[k].plus;

         if(debug >= 3)
         {
	    fprintf(fstatus, "corrs[%d].id = %d\n", index, corrs[index].id);
	    fflush(stdout);
	 }

	 ++ncorrs;
      }


      /* Add this reference */

      corrs[index].a = 0.;
      corrs[index].b = 0.;
      corrs[index].c = 0.;

      corrs[index].neighbors[corrs[index].nneighbors] = &fits[k];

      ++corrs[index].nneighbors;

      if(corrs[index].nneighbors >= corrs[index].maxneighbors)
      {
	 corrs[index].maxneighbors = MAXCNT;

	 corrs[index].neighbors 
	    = (struct FitInfo **)realloc(corrs[index].neighbors,
				         corrs[index].maxneighbors 
				         * sizeof(struct FitInfo *));

	 if(corrs[i].neighbors == (struct FitInfo **)NULL)
	 {
	    fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"realloc() failed (FitInfo *)\"]\n");
	    exit(1);
	 }
      }
   }

   if(refimage != 0 && (refimage < 0 || refimage >= ncorrs))
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Debug reference image out of range (0 - %d)\"]\n", 
	 ncorrs-1);
      exit(1);
   }


   /*******************************************************/
   /* Find the back reference from the fits to the images */
   /*******************************************************/

   for(j=0; j<nfits; ++j)
   {
      for(i=0; i<ncorrs; ++i)
      {
	 if(fits[j].plus == corrs[i].id)
	 {
	    fits[j].plusimg = &corrs[i];
	    break;
	 }
      }
      
      for(i=0; i<ncorrs; ++i)
      {
	 if(fits[j].minus == corrs[i].id)
	 {
	    fits[j].minusimg = &corrs[i];
	    break;
	 }
      }
      
      if(debug >= 3)
      {
	 if(j == 0)
            printf("\n");

         printf("fits[%3d]: (plusimg=%3d  minusimg=%3d) ", 
	    j, fits[j].plusimg->id, fits[j].minusimg->id);

         printf(" %12.5e ",  fits[j].a);
         printf(" %12.5e ",  fits[j].b);
         printf(" %12.5e\n", fits[j].c);

	 fflush(stdout);
      }
   }



   /***********************************************/
   /* Turn off the fits which represent those     */
   /* overlaps which are smaller than 2% of the   */
   /* average image area and those whose linear   */
   /* extent in at least one direction isn't      */
   /* at least half the size of the corresponding */
   /* images                                      */
   /***********************************************/

   for(k=0; k<nfits; ++k)
      fits[k].use = 1;

   if(!useall)
   {
      for(k=0; k<nfits; ++k)
      {
	 if(fits[k].npix < areaLimit * avearea)
	 {
	    if(debug >= 2)
	       printf("not using fit %d [%d|%d] (area to small: %d/%-g\n",
		  k, fits[k].plus, fits[k].minus, fits[k].npix, avearea);

	    fits[k].use = 0;

	    continue;
	 }

	 xfsize = fits[k].xmax - fits[k].xmin;
	 yfsize = fits[k].ymax - fits[k].ymin;

	 xisize1 = imgs[fits[k].plus].naxis1;
	 yisize1 = imgs[fits[k].plus].naxis2;

	 xisize2 = imgs[fits[k].minus].naxis1;
	 yisize2 = imgs[fits[k].minus].naxis2;

	 if(xfsize < xisize1 * linearLimit
	 && yfsize < yisize1 * linearLimit
	 && xfsize < xisize2 * linearLimit
	 && yfsize < yisize2 * linearLimit)
	 {
	    if(debug >= 2)
	       printf("not using fit %d [%d|%d] (linear size too small: %-g %-g %-g %-g)\n",
		  k, fits[k].plus, fits[k].minus, xfsize/xisize1, yfsize/yisize1, xfsize/xisize2, yfsize/yisize2);

	    fits[k].use = 0;
	 }
      }
   }


   /***********************************************/
   /* We don't want to use noisy fits, so turn    */
   /* off those with an rms more than two sigma   */
   /* above the average                           */
   /***********************************************/

   if(!useall)
   {
      sumn   = 0.;
      sumx   = 0.;
      sumxx  = 0.;

      for(k=0; k<nfits; ++k)
      {
	 if(fits[k].use)
	 {
	    sumn  += 1.;
	    sumx  += fits[k].rms;
	    sumxx += fits[k].rms * fits[k].rms;
	 }
      }

      averms = sumx / sumn;
      sigrms = sqrt(sumxx/sumn - averms*averms);

      for(k=0; k<nfits; ++k)
      {
	 if(fits[k].use)
	 {
	    if(fits[k].rms > averms + sigmaLimit * sigrms)
	    {
	       if(debug >= 2)
		  printf("not using fit %d [%d|%d] rms too large: %-g/%-g+%-g)\n",
		     k, fits[k].plus, fits[k].minus, fits[k].rms, averms, sigrms);

	       fits[k].use = 0;

	       continue;
	    }
	 }
      }
   }


   /***************************************/
   /* Dump out the correction information */
   /***************************************/

   if(debug >= 3)
   {
      for(i=0; i<ncorrs; ++i)
      {
	 printf("\n-----\n\nCorrection %d (Image %d)\n\n", i, corrs[i].id);

	 for(j=0; j<corrs[i].nneighbors; ++j)
	 {
	    printf("\n  neighbor %3d:\n", j+1);

	    printf("            id: %d\n", corrs[i].neighbors[j]->minus);

	    printf("       (A,B,C): (%-g,%-g,%-g)\n", 
	       corrs[i].neighbors[j]->a, 
	       corrs[i].neighbors[j]->b, 
	       corrs[i].neighbors[j]->c);   

	    printf("             x: %5d to %5d\n", 
	       corrs[i].neighbors[j]->xmin, corrs[i].neighbors[j]->xmax);

	    printf("             y: %5d to %5d\n",
	       corrs[i].neighbors[j]->ymin, corrs[i].neighbors[j]->ymax);

	    printf("        center: (%-g,%-g)\n", 
	       corrs[i].neighbors[j]->xcenter, corrs[i].neighbors[j]->ycenter);
	 }
      }
   }


   iteration = 0;

   while(1)
   {
      if(noslope)
         fittype = LEVEL;
      else
      {
         if(iteration < maxlevel)
	    fittype = LEVEL;
         else
	    fittype = BOTH;
      }

      if(debug >= 2)
      {
	 printf("Iteration %d", iteration+1);
	      if(fittype == LEVEL) printf(" (LEVEL):\n");
	 else if(fittype == SLOPE) printf(" (SLOPE):\n");
	 else if(fittype == BOTH)  printf(" (BOTH ):\n");
	 else                      printf(" (ERROR):\n");
	 fflush(stdout);
      }

      /*********************************************/
      /* For each image, calculate the "best fit"  */
      /* correction plane, based of the difference */
      /* data between an image and its neighbors   */
      /*********************************************/

      for(i=0; i<ncorrs; ++i)
      {
	 sumn  = 0.;
	 sumx  = 0.;
	 sumy  = 0.;
	 sumxx = 0.;
	 sumxy = 0.;
	 sumyy = 0.;
	 sumxz = 0.;
	 sumyz = 0.;
	 sumz  = 0.;

	 corrs[i].acorrection = 0.;
	 corrs[i].bcorrection = 0.;
	 corrs[i].ccorrection = 0.;

	 for(j=0; j<corrs[i].nneighbors; ++j)
	 {
            /* We have earlier "turned off" some of these because */
            /* the fit was bad (too few points or too noisy).     */
            /* If so, don't include them in the sums.             */

            if(corrs[i].neighbors[j]->use == 0)
               continue;


	    /* What we do here is essentially a "least squares",   */
	    /* though rather than go back to the difference files  */
	    /* we instead use the parameterized value of the plane */
	    /* fit to that data.                                   */

	    imin = corrs[i].neighbors[j]->xmin;
	    imax = corrs[i].neighbors[j]->xmax;
	    jmin = corrs[i].neighbors[j]->ymin;
	    jmax = corrs[i].neighbors[j]->ymax;

	    theta = corrs[i].neighbors[j]->boxangle;

	    Xmin = corrs[i].neighbors[j]->Xmin;
	    Xmax = corrs[i].neighbors[j]->Xmax;
	    Ymin = corrs[i].neighbors[j]->Ymin;
	    Ymax = corrs[i].neighbors[j]->Ymax;

	    if(debug >= 3)
	    {
	       printf("\n--------------------------------------------------\n");
	       printf("\nCorrection %d (%d) / Neighbor %d (%d)\n\nPixel Range:\n",
                  i, corrs[i].id, j, corrs[i].neighbors[j]->minus);
	       printf("i:     %12.5e->%12.5e (%12.5e)\n", imin, imax, imax-imin+1);
	       printf("j:     %12.5e->%12.5e (%12.5e)\n", jmin, jmax, jmax-jmin+1);
	       printf("X:     %12.5e->%12.5e (%12.5e)\n", Xmin, Xmax, Xmax-Xmin+1);
	       printf("Y:     %12.5e->%12.5e (%12.5e)\n", Ymin, Ymax, Ymax-Ymin+1);
	       printf("angle: %-g\n", theta);
	       printf("\n");

	       fflush(stdout);
	    }

	    sinTheta = sin(theta*dtr);
	    cosTheta = cos(theta*dtr);

	    dsumn = (Xmax - Xmin) * (Ymax - Ymin);

	    dsumx  = (Ymax - Ymin) * (Xmax*Xmax - Xmin*Xmin)/2. * cosTheta 
	           - (Xmax - Xmin) * (Ymax*Ymax - Ymin*Ymin)/2. * sinTheta;

	    dsumy  = (Ymax - Ymin) * (Xmax*Xmax - Xmin*Xmin)/2. * sinTheta 
	           + (Xmax - Xmin) * (Ymax*Ymax - Ymin*Ymin)/2. * cosTheta;
	    
	    dsumxx = (Ymax - Ymin) * (Xmax*Xmax*Xmax - Xmin*Xmin*Xmin)/3. * cosTheta*cosTheta
		   - 2. * (Xmax*Xmax - Xmin*Xmin)/2. * (Ymax*Ymax - Ymin*Ymin)/2. * cosTheta*sinTheta
	           + (Xmax - Xmin) * (Ymax*Ymax*Ymax - Ymin*Ymin*Ymin)/3. * sinTheta*sinTheta;

	    dsumyy = (Ymax - Ymin) * (Xmax*Xmax*Xmax - Xmin*Xmin*Xmin)/3. * sinTheta*sinTheta
		   + 2. * (Xmax*Xmax - Xmin*Xmin)/2. * (Ymax*Ymax - Ymin*Ymin)/2. * sinTheta*cosTheta
	           + (Xmax - Xmin) * (Ymax*Ymax*Ymax - Ymin*Ymin*Ymin)/3. * cosTheta*cosTheta;

	    dsumxy = (Ymax - Ymin) * (Xmax*Xmax*Xmax - Xmin*Xmin*Xmin)/3. * cosTheta*sinTheta
		   + (Xmax*Xmax - Xmin*Xmin)/2. * (Ymax*Ymax - Ymin*Ymin)/2. * (cosTheta*cosTheta - sinTheta*sinTheta)
	           - (Xmax - Xmin) * (Ymax*Ymax*Ymax - Ymin*Ymin*Ymin)/3. * sinTheta*cosTheta;


	    if(debug >= 3)
	    {
	       printf("\nSums:\n");
	       printf("dsumn   = %12.5e\n", dsumn);
	       printf("dsumx   = %12.5e\n", dsumx);
	       printf("dsumy   = %12.5e\n", dsumy);
	       printf("dsumxx  = %12.5e\n", dsumxx);
	       printf("dsumxy  = %12.5e\n", dsumxy);
	       printf("dsumyy  = %12.5e\n", dsumyy);
	       printf("\n");

	       fflush(stdout);
	    }

	    sumn  += dsumn;
	    sumx  += dsumx;
	    sumy  += dsumy;
	    sumxx += dsumxx;
	    sumxy += dsumxy;
	    sumyy += dsumyy;

	    index = corrs[i].neighbors[j]->plusimg->id;

	    A = corrs[i].neighbors[j]->a;
	    B = corrs[i].neighbors[j]->b;
	    C = corrs[i].neighbors[j]->c;

	    sumz  += A * dsumx  + B * dsumy  + C * dsumn;
	    sumxz += A * dsumxx + B * dsumxy + C * dsumx;
            sumyz += A * dsumxy + B * dsumyy + C * dsumy;
	    
	    if(debug >= 3)
	    {
	       printf("\n");
	       printf("sumn    = %12.5e\n", sumn);
	       printf("sumx    = %12.5e\n", sumx);
	       printf("sumy    = %12.5e\n", sumy);
	       printf("sumxx   = %12.5e\n", sumxx);
	       printf("sumxy   = %12.5e\n", sumxy);
	       printf("sumyy   = %12.5e\n", sumyy);
	       printf("A       = %12.5e\n", A);
	       printf("B       = %12.5e\n", B);
	       printf("C       = %12.5e\n", C);
	       printf("sumz    = %12.5e\n", sumz);
	       printf("sumxz   = %12.5e\n", sumxz);
	       printf("sumyz   = %12.5e\n", sumyz);
	       printf("\n");

	       fflush(stdout);
	    }
	 }


         /* If we found no overlaps, don't */
         /* try to compute correction      */

         if(sumn == 0.)
            continue;



	 /***********************************/
	 /* Least-squares plane calculation */

	 /*** Fill the matrix and vector  ****

	      |a00  a01 a02| |A|   |b00|
	      |a10  a11 a12|x|B| = |b01|
	      |a20  a21 a22| |C|   |b02|

	 *************************************/

	 a[0][0] = sumxx;
	 a[1][0] = sumxy;
	 a[2][0] = sumx;

	 a[0][1] = sumxy;
	 a[1][1] = sumyy;
	 a[2][1] = sumy;

	 a[0][2] = sumx;
	 a[1][2] = sumy;
	 a[2][2] = sumn;

	 b[0][0] = sumxz;
	 b[1][0] = sumyz;
	 b[2][0] = sumz;

	 if(debug >= 3)
	 {
	    printf("\nMatrix:\n");
	    printf("| %12.5e %12.5e %12.5e | |A|   |%12.5e|\n", a[0][0], a[0][1], a[0][2], b[0][0]);
	    printf("| %12.5e %12.5e %12.5e |x|B| = |%12.5e|\n", a[1][0], a[1][1], a[1][2], b[1][0]);
	    printf("| %12.5e %12.5e %12.5e | |C|   |%12.5e|\n", a[2][0], a[2][1], a[2][2], b[2][0]);
	    printf("\n");

	    fflush(stdout);
	 }


	 /* Solve */

	 if(fittype == LEVEL)
	 {
	    b[0][0] = 0.;
	    b[1][0] = 0.;
	    b[2][0] = sumz/sumn;

	    istatus = 0;
	 }
	 else if(fittype == SLOPE)
	 {
	    n = 2;
	    istatus = gaussj(a, n, b, m);

	    b[2][0] = 0.;
	 }
	 else if(fittype == BOTH)
	 {
	    n = 3;
	    istatus = gaussj(a, n, b, m);
	 }
	 else
	 {
	    fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Invalid fit type\"]\n");
	    exit(1);
	 }


	 /* Singular matrix, don't use corrections */

	 if(istatus)
	 {
	    b[0][0] = 0.;
	    b[1][0] = 0.;
	    b[2][0] = 0.;
	 }

	 /* Apply the corrections */

	 if(debug >= 3)
	 {
	    printf("\nMatrix Solution:\n");

	    printf(" |%12.5e|\n", b[0][0]);
	    printf(" |%12.5e|\n", b[1][0]);
	    printf(" |%12.5e|\n", b[2][0]);
	    printf("\n");

	    fflush(stdout);
	 }

	 corrs[i].acorrection = b[0][0] / 2.;
	 corrs[i].bcorrection = b[1][0] / 2.;
	 corrs[i].ccorrection = b[2][0] / 2.;

	 if(debug >= 2)
	 {
	    printf("Background corrections (Correction %d (%4d) / Iteration %d) ", 
	       i, corrs[i].id, iteration+1);

		 if(fittype == LEVEL) printf(" (LEVEL):\n");
	    else if(fittype == SLOPE) printf(" (SLOPE):\n");
	    else if(fittype == BOTH)  printf(" (BOTH ):\n");
	    else                      printf(" (ERROR):\n");

	    if(istatus)
	       printf("\n***** Singular Matrix ***** \n\n");

	    printf("  A = %12.5e\n",   corrs[i].acorrection);
	    printf("  B = %12.5e\n",   corrs[i].bcorrection);
	    printf("  C = %12.5e\n\n", corrs[i].ccorrection);

	    fflush(stdout);
	 }
      }


      /***************************************/
      /* Apply the corrections to each image */
      /***************************************/

      for(i=0; i<ncorrs; ++i)
      {
	 corrs[i].a += corrs[i].acorrection;
	 corrs[i].b += corrs[i].bcorrection;
	 corrs[i].c += corrs[i].ccorrection;

	 if(debug >= 1)
	 {
            if(i == 0)
               printf("\n");

            if(refimage == 0 || i == refimage)
            {
	       printf("Corrected backgrounds (Correction %4d (%4d) / Iteration %4d) ", 
		  i, corrs[i].id, iteration+1);

		    if(fittype == LEVEL) printf(" (LEVEL): ");
	       else if(fittype == SLOPE) printf(" (SLOPE): ");
	       else if(fittype == BOTH)  printf(" (BOTH ): ");
	       else                      printf(" (ERROR): ");

	       printf(" %12.5e ",  corrs[i].a);
	       printf(" %12.5e ",  corrs[i].b);
	       printf(" %12.5e\n", corrs[i].c);

	       fflush(stdout);
            }
	 }
      }


      /*************************************/
      /* Apply the corrections to each fit */
      /*************************************/

      for(i=0; i<nfits; ++i)
      {
	 fits[i].a -= fits[i].plusimg->acorrection;
	 fits[i].b -= fits[i].plusimg->bcorrection;
	 fits[i].c -= fits[i].plusimg->ccorrection;

	 fits[i].a += fits[i].minusimg->acorrection;
	 fits[i].b += fits[i].minusimg->bcorrection;
	 fits[i].c += fits[i].minusimg->ccorrection;

	 if(debug >= 2)
	 {
            if(i == 0)
               printf("\n");

	    printf("Corrected fit (fit %4d / Iteration %5d) ", 
	       i, iteration+1);

		 if(fittype == LEVEL) printf(" (LEVEL): ");
	    else if(fittype == SLOPE) printf(" (SLOPE): ");
	    else if(fittype == BOTH)  printf(" (BOTH ): ");
	    else                      printf(" (ERROR): ");

	    printf(" %12.5e ",  fits[i].a);
	    printf(" %12.5e ",  fits[i].b);
	    printf(" %12.5e\n", fits[i].c);

	    fflush(stdout);
	 }
      }


      ++iteration;

      if(iteration >= niteration)
	 break;
   }


   /*********************************************/
   /* For each image, print out the final plane */
   /*********************************************/

   fprintf(fout,"|  id |      a       |      b       |      c       |\n");

   for(i=0; i<ncorrs; ++i)
      fprintf(fout, " %5d  %13.5e  %13.5e  %13.5e\n", 
	 corrs[i].id, corrs[i].a, corrs[i].b, corrs[i].c);
   
   fflush(fout);
   fclose(fout);


   fprintf(fstatus, "[struct stat=\"OK\"]\n");
   fflush(stdout);
   exit(0);
}


/***********************************/
/*                                 */
/*  Performs gaussian fitting on   */
/*  backgrounds and returns the    */
/*  parameters A and B in a        */
/*  function of the form           */
/*  y = Ax + B                     */
/*                                 */
/***********************************/

int gaussj(float **a, int n, float **b, int m)
{
   int  *indxc, *indxr, *ipiv;
   int   i, icol, irow, j, k, l, ll;
   float big, dum, pivinv, temp;

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
	       {
		  free_ivector(ipiv);
		  free_ivector(indxr);
		  free_ivector(indxc);

		  return 1;
	       }
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
      {
	 free_ivector(ipiv);
	 free_ivector(indxr);
	 free_ivector(indxc);

	 return 1;
      }

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

   return 0;
}


/* Allocates memory for an array of integers */

int *ivector(int nh)
{
   int *v;

   v=(int *)malloc((size_t) (nh*sizeof(int)));

   if (!v) 
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Allocation failure in ivector()\"]\n"); 
      exit(0);
   }

   return v;
}


/* Frees memory allocated by ivector */

void free_ivector(int *v)
{
   free((char *) v);
}
