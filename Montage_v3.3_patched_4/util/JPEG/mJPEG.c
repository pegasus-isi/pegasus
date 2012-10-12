/* Module: mJPEG.c

Version  Developer        Date     Change
-------  ---------------  -------  -----------------------
3.6      John Good        24Jun07  Added correction for CAR projection error
3.5      John Good        17May07  Added compass 'rose' and 'no WCS' options
3.4      John Good        07Feb06  Fixed output keyword printing (some
				   lines didn't leave 8 char for the keyword)
3.3      John Good        14Sep05  Added HDU handling                       
3.2      John Good        15May05  Added logic to deal with positive CDELT1 
3.1      John Good        28Apr05  Dealt with large FITS headers and 
				   pure shift offsets between RGB images
3.0      John Good        29Mar05  Fixed gaussian stretch in color mode
                                   and added "true color" flag
2.1      John Good        01Mar05  Added simple "marker" capability
2.0      John Good        28Feb05  Added gaussian histogram equalization
1.1      John Good        08Oct04  Added check for valid file names
1.0      John Good        25Sep04  Baseline code

*/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <ctype.h>
#include <math.h>
#include <string.h>
#include <stdio.h>
#include <sys/types.h>
#include <errno.h>

#include <jpeglib.h>
#include <fitsio.h>
#include <wcs.h>
#include <coord.h>

#include <montage.h>
#include <mNaN.h>

#define  MAXDIM 65500

#define  VALUE       0
#define  PERCENTILE  1
#define  SIGMA       2

#define  POWER       0
#define  GAUSSIAN    1
#define  GAUSSIANLOG 2
#define  ASINH       3

double asinh(double x);

int    getHDU(char *);
double percentileLevel(double percentile);
double valuePercentile(double value);
double snpinv(double p);
double erfinv (double p);
void   printFitsError(int);

void    createColorTable(int itable);
int     make_comment(char *header, char *comment);
void    parseRange(char const *str, char const *kind,
                   double *val, double *extra, int *type);
void    getRange(fitsfile *fptr, char *minstr, char *maxstr,
                 double *rangemin, double *rangemax, 
	         int type, char *betastr, double *rangebeta, double *dataval,
	         int imnaxis1, int imnaxis2);

int     checkHdr(char *infile, int hdrflag, int hdu);
int     checkWCS(struct WorldCoor *wcs, int action);

int     covered(int i, int j, 
    	        double x1, double y1, double x2, double y2, 
    	        double len2, int lwidth, int *color);

void    fixxy   (double *x, double *y, int *offscl);
int     stradd  (char *header, char *card);

struct WorldCoor *wcsfake(int naxis1, int naxis2);

int color_table[256][3];

int    naxis1, naxis2;
double crpix1, crpix2;
double crval1, crval2;
double cdelt1, cdelt2;
double crota2;
double cd11,   cd12;
double cd21,   cd22;
double xinc,   yinc;

double xcorrection;
double ycorrection;

struct WorldCoor *wcs;

double dtr;

int debug;


/*************************************************************************/
/*                                                                       */
/*  mJPEG                                                                */
/*                                                                       */
/*  This program generates a JPEG image file from a FITS file (or a set  */
/*  of three FITS files in color).  A data range for each image can      */
/*  be defined and the data can be stretch by any power of the log()     */
/*  function (including zero: linear).  Pseudo-color color tables can    */
/*  be applied in single-image mode.                                     */
/*                                                                       */
/*************************************************************************/

int main(int argc, char **argv)
{
   int       i, j, k, jj, ipix, index, itemp, nowcs, showCompass;
   int       istart, iend, nx;
   int       jstart, jend, ny, jinc;
   int       imin, imax, jmin, jmax;
   int       nullcnt, lwidth, color;
   int       keysexist, keynum;

   double    x, y;
   double    ix, iy;
   double    xpos, ypos;

   int       grayType  = 0;
   int       redType   = 0;
   int       greenType = 0;
   int       blueType  = 0;

   int       quality, offscl;
   int       marksize[1024], imark[1024], jmark[1024], usemark[1024];
   int       markcolor[1024][3];
   int       sys, marksys[1024];
   int       hdu[3];

   int       nmark = 0;

   double    rosescale, len2n, len2e;
   double    x1n, y1n, x2n, y2n;
   double    x1e, y1e, x2e, y2e;

   double    graydiff, reddiff, greendiff, bluediff;
   double    grayval, redval, greenval, blueval;
   double    grayjpeg, redjpeg, greenjpeg, bluejpeg, maxjpeg;

   double    redxoff, greenxoff, bluexoff;
   double    redyoff, greenyoff, blueyoff;
   double    redxmin, greenxmin, bluexmin;
   double    redxmax, greenxmax, bluexmax;
   double    redymin, greenymin, blueymin;
   double    redymax, greenymax, blueymax;

   int       rednaxis1,   rednaxis2;
   int       greennaxis1, greennaxis2;
   int       bluenaxis1,  bluenaxis2;

   double    xmin, xmax;
   double    ymin, ymax;

   double    marklon[1024], marklat[1024];
   double    xmark, ymark;
   double    lon, lat;
   double    epoch, markepoch[1024];
   double    truecolor;

   int       status = 0;

   int       isRGB  = 0;
   int       flipX  = 0;
   int       flipY  = 0;
   int       noflip = 0;

   int       colortable = 0;

   char      statusfile[1024];
   char      grayfile  [1024];
   char      redfile   [1024];
   char      greenfile [1024];
   char      bluefile  [1024];
   char      jpegfile  [1024];

   char      grayminstr  [256];
   char      graymaxstr  [256];
   char      graybetastr [256];
   char      redminstr   [256];
   char      redmaxstr   [256];
   char      redbetastr  [256];
   char      greenminstr [256];
   char      greenmaxstr [256];
   char      greenbetastr[256];
   char      blueminstr  [256];
   char      bluemaxstr  [256];
   char      bluebetastr [256];

   char      sysstring  [256];
   char      epochstring[256];
   char      colorstring[256];
   char      colorelem    [3];

   double    grayminval      = 0.;
   double    graymaxval      = 0.;
   double    grayminpercent  = 0.;
   double    graymaxpercent  = 0.;
   double    graybetaval     = 0.;
   int       graylogpower    = 0;
   double    graydataval[256];

   double    redminval       = 0.;
   double    redmaxval       = 0.;
   double    redminpercent   = 0.;
   double    redmaxpercent   = 0.;
   double    redbetaval      = 0.;
   int       redlogpower     = 0;
   double    reddataval[256];

   double    greenminval     = 0.;
   double    greenmaxval     = 0.;
   double    greenminpercent = 0.;
   double    greenmaxpercent = 0.;
   double    greenbetaval    = 0.;
   int       greenlogpower   = 0;
   double    greendataval[256];

   double    blueminval      = 0.;
   double    bluemaxval      = 0.;
   double    blueminpercent  = 0.;
   double    bluemaxpercent  = 0.;
   double    bluebetaval     = 0.;
   int       bluelogpower    = 0;
   double    bluedataval[256];

   fitsfile *grayfptr;
   fitsfile *redfptr;
   fitsfile *greenfptr;
   fitsfile *bluefptr;

   long      fpixel[4];
   long      nelements;

   double   *fitsbuf;
   double   *rfitsbuf;
   double   *gfitsbuf;
   double   *bfitsbuf;

   char     *end;

   char     *header  = (char *)NULL;
   char     *comment = (char *)NULL;

   FILE     *jpegfp;

   JSAMPROW  jpegdata;
   JSAMPROW  jpegptr[1];

   struct jpeg_compress_struct cinfo;

   struct jpeg_error_mgr jerr;


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

   dtr = atan(1.)/45.;



   /* Command-line arguments */

   debug   = 0;
   fstatus = stdout;

   truecolor   = 0.;
   showCompass = 0;
   nowcs       = 0;

   strcpy(statusfile, "");
   strcpy(grayfile,   "");
   strcpy(redfile,    "");
   strcpy(greenfile,  "");
   strcpy(bluefile,   "");
   strcpy(jpegfile,   "");

   if(argc < 2)
   {
      printf("[struct stat=\"ERROR\", msg=\"Usage: %s [-d] [-nowcs] [-compass] [-noflip] [-t(rue-color) power] [-s statusfile] [-ct color-table] -gray in.fits minrange maxrange [logpower/gaussian] -red red.fits rminrange rmaxrange [rlogpower/gaussian] -green green.fits gminrange gmaxrange [glogpower/gaussian] -blue blue.fits bminrange bmaxrange [blogpower/gaussian] -out out.jpg\"]\n", argv[0]);
      exit(1);
   }

   for(i=0; i<argc; ++i)
   {
      /* DEBUG */

      if(strcmp(argv[i], "-d") == 0)
         debug = 1;
      

      /* Ignore WCS */

      else if(strcmp(argv[i], "-nowcs") == 0)
         nowcs = 1;
      

      /* COMPASS 'ROSE' */

      else if(strcmp(argv[i], "-compass") == 0)
         showCompass = 1;


      /* Don't flip the image (to get North up) */

      else if(strcmp(argv[i], "-noflip") == 0)
         noflip = 1;


      /* TRUE COLOR */

      else if(strcmp(argv[i], "-t") == 0)
      {
         truecolor = atof(argv[i+1]);

	 if(truecolor < 1.) 
	    truecolor = 1;
      }
      

      /* MARKER SYMBOL */

      else if(strcmp(argv[i], "-mark") == 0)
      {
         if(i+6 >= argc)
         {
            printf ("[struct stat=\"ERROR\", msg=\"Too few arguments following -mark flag\"]\n");
            fflush(stdout);
            exit(1);
         }

	 marklon [nmark] = atof(argv[i+1]);
	 marklat [nmark] = atof(argv[i+2]);

	 if(marksize[nmark]%2 == 0)
	    ++marksize[nmark];

	 marksize[nmark] = (marksize[nmark]-1)/2;

	 strcpy(sysstring,   argv[i+3]);
	 strcpy(epochstring, argv[i+4]);

	 marksize[nmark] = atoi(argv[i+5]);

	 strcpy(colorstring, argv[i+6]);

         for(j=0; j<strlen(sysstring); ++j)
            sysstring[j] = toupper(sysstring[j]);

         for(j=0; j<strlen(epochstring); ++j)
            epochstring[j] = toupper(epochstring[j]);

         if(strncmp(sysstring, "GA", 2) == 0)
            marksys[nmark] = GAL;

         else if(strncmp(sysstring, "EQ", 2) == 0
              && epochstring[0] == 'J') 
            marksys[nmark] = EQUJ;

         else if(strncmp(sysstring, "EQ", 2) == 0
              && epochstring[0] == 'B') 
            marksys[nmark] = EQUB;

         else if(strncmp(sysstring, "EC", 2) == 0
              && epochstring[0] == 'J') 
            marksys[nmark] = ECLJ;

         else if(strncmp(sysstring, "EC", 2) == 0
              && epochstring[0] == 'B') 
            marksys[nmark] = ECLB;
         
         markepoch[nmark] = atof(epochstring+1);

	 markcolor[nmark][0] = 0;
	 markcolor[nmark][1] = 0;
	 markcolor[nmark][2] = 0;

	 if(strcmp(colorstring, "red") == 0)
	    markcolor[nmark][0] = 255;

	 else if(strcmp(colorstring, "green") == 0)
	    markcolor[nmark][1] = 255;

	 else if(strcmp(colorstring, "blue") == 0)
	    markcolor[nmark][2] = 255;

	 else if(strcmp(colorstring, "magenta") == 0)
	 {
	    markcolor[nmark][0] = 255;
	    markcolor[nmark][2] = 255;
	 }

	 else if(strcmp(colorstring, "cyan") == 0)
	 {
	    markcolor[nmark][1] = 255;
	    markcolor[nmark][2] = 255;
	 }

	 else if(strcmp(colorstring, "yellow") == 0)
	 {
	    markcolor[nmark][0] = 255;
	    markcolor[nmark][1] = 255;
	 }

	 else if(strcmp(colorstring, "white") == 0)
	 {
	    markcolor[nmark][0] = 255;
	    markcolor[nmark][1] = 255;
	    markcolor[nmark][2] = 255;
	 }

	 else
	 {
	    colorelem[2] = '\0';

	    colorelem[0] = colorstring[0];
	    colorelem[1] = colorstring[1];

	    sscanf(colorelem, "%x", &markcolor[nmark][0]);

	    colorelem[0] = colorstring[2];
	    colorelem[1] = colorstring[3];

	    sscanf(colorelem, "%x", &markcolor[nmark][1]);

	    colorelem[0] = colorstring[4];
	    colorelem[1] = colorstring[5];

	    sscanf(colorelem, "%x", &markcolor[nmark][2]);
	 }

	 ++nmark;

	 i+=6;
      }
      

      /* COLOR TABLE */

      else if(strcmp(argv[i], "-ct") == 0)
      {
         if(i+1 >= argc)
         {
            printf ("[struct stat=\"ERROR\", msg=\"Too few arguments following -ct flag\"]\n");
            fflush(stdout);
            exit(1);
         }

         colortable = strtol(argv[i+1], &end, 10);

         if(colortable < 0  || colortable > 11)
         {
            printf ("[struct stat=\"ERROR\", msg=\"Color table index must be between 0 and 11\"]\n");
            fflush(stdout);
            exit(1);
         }

         ++i;
      }
      

      /* STATUS FILE */

      else if(strcmp(argv[i], "-s") == 0)
      {
         if(i+1 >= argc)
         {
            printf ("[struct stat=\"ERROR\", msg=\"Too few arguments following -s flag\"]\n");
            fflush(stdout);
            exit(1);
         }

         strcpy(statusfile, argv[i+1]);

         if((fstatus = fopen(statusfile, "w+")) == (FILE *)NULL)
         {
            printf ("[struct stat=\"ERROR\", msg=\"Cannot open status file: %s\"]\n",
               argv[i+1]);
            fflush(stdout);
            exit(1);
         }

         ++i;
      }


      /* GRAY */

      else if(strcmp(argv[i], "-gray") == 0
           || strcmp(argv[i], "-grey") == 0)
      {
         if(i+3 >= argc)
         {
            printf ("[struct stat=\"ERROR\", msg=\"Too few arguments following -gray flag\"]\n");
            fflush(stdout);
            exit(1);
         }

         strcpy(grayfile, argv[i+1]);

	 hdu[0] = getHDU(grayfile);

	 if(!nowcs)
	    checkHdr(grayfile, 0, hdu[0]);

         strcpy(grayminstr, argv[i+2]);
         strcpy(graymaxstr, argv[i+3]);

	 grayType = POWER;

         if(i+4 < argc)
         {
	    if(argv[i+4][0] == 'g')
	    {
	       grayType = GAUSSIAN;

	       if(strlen(argv[i+4]) > 1 
	       && (   argv[i+4][strlen(argv[i+4])-1] == 'g'
	           || argv[i+4][strlen(argv[i+4])-1] == 'l'))
	          grayType = GAUSSIANLOG;

               i+=1;
	    }
	    
	    else if(argv[i+4][0] == 'a')
	    {
	       grayType = ASINH;

	       strcpy(graybetastr, "2s");

	       if(i+5 < argc)
		  strcpy(graybetastr, argv[i+5]);
               
               i += 2;
	    }
	    
	    else if(strcmp(argv[i+4], "lin") == 0)
	       graylogpower = 0;
	    
	    else if(strcmp(argv[i+4], "log") == 0)
	       graylogpower = 1;

	    else if(strcmp(argv[i+4], "loglog") == 0)
	       graylogpower = 2;

	    else
	    {
	       graylogpower = strtol(argv[i+4], &end, 10);
 
	       if(graylogpower < 0  || end < argv[i+4] + strlen(argv[i+4]))
                  graylogpower = 0;
               else
		  i += 1;
	    }
         }
         
         i += 2;

         if(fits_open_file(&grayfptr, grayfile, READONLY, &status))
         {
            printf("[struct stat=\"ERROR\", msg=\"Image file %s invalid FITS\"]\n",
               grayfile);
            exit(1);
         }

	 if(hdu[0] > 0)
	 {
	    if(fits_movabs_hdu(grayfptr, hdu[0]+1, NULL, &status))
	    {
	       printf("[struct stat=\"ERROR\", msg=\"Can't find HDU %d\"]\n", hdu[0]);
	       exit(1);
	    }
	 }
      }


      else if(strcmp(argv[i], "-red") == 0)
      {
         if(i+3 >= argc)
         {
            printf ("[struct stat=\"ERROR\", msg=\"Too few arguments following -red flag\"]\n");
            fflush(stdout);
            exit(1);
         }

         strcpy(redfile, argv[i+1]);

	 hdu[0] = getHDU(redfile);

         if(!nowcs)
	    checkHdr(redfile, 0, hdu[0]);

         strcpy(redminstr, argv[i+2]);
         strcpy(redmaxstr, argv[i+3]);

	 redType = POWER;

         if(i+4 < argc)
         {
	    if(argv[i+4][0] == 'g')
	    {
	       redType = GAUSSIAN;

	       if(strlen(argv[i+4]) > 1 
	       && (   argv[i+4][strlen(argv[i+4])-1] == 'g'
	           || argv[i+4][strlen(argv[i+4])-1] == 'l'))
	          redType = GAUSSIANLOG;
               
               i += 1;
	    }
	    
            else if(argv[i+4][0] == 'a')
            {
               redType = ASINH;

               strcpy(redbetastr, "2s");

               if(i+5 < argc)
                  strcpy(redbetastr, argv[i+5]);

               i += 2;
            }

	    else if(strcmp(argv[i+4], "lin") == 0)
	       redlogpower = 0;
	    
	    else if(strcmp(argv[i+4], "log") == 0)
	       redlogpower = 1;

	    else if(strcmp(argv[i+4], "loglog") == 0)
	       redlogpower = 2;

	    else
	    {
	       redlogpower = strtol(argv[i+4], &end, 10);

	       if(redlogpower < 0  || end < argv[i+4] + strlen(argv[i+4]))
		  redlogpower = 0.;
	       else
		  i += 1;
	    }
         }
         
         i += 2;

         if(fits_open_file(&redfptr, redfile, READONLY, &status))
         {
            printf("[struct stat=\"ERROR\", msg=\"Image file %s invalid FITS\"]\n",
               redfile);
            exit(1);
         }

	 if(hdu[0] > 0)
	 {
	    if(fits_movabs_hdu(redfptr, hdu[0]+1, NULL, &status))
	    {
	       printf("[struct stat=\"ERROR\", msg=\"Can't find HDU %d\"]\n", hdu[0]);
	       exit(1);
	    }
	 }
      }


      else if(strcmp(argv[i], "-green") == 0)
      {
         if(i+3 >= argc)
         {
            printf ("[struct stat=\"ERROR\", msg=\"Too few arguments following -green flag\"]\n");
            fflush(stdout);
            exit(1);
         }

         strcpy(greenfile, argv[i+1]);

	 hdu[1] = getHDU(greenfile);

         if(!nowcs)
	    checkHdr(greenfile, 0, hdu[1]);

         strcpy(greenminstr, argv[i+2]);
         strcpy(greenmaxstr, argv[i+3]);

	 greenType = POWER;

         if(i+4 < argc)
         {
	    if(argv[i+4][0] == 'g')
	    {
	       greenType = GAUSSIAN;

	       if(strlen(argv[i+4]) > 1 
	       && (   argv[i+4][strlen(argv[i+4])-1] == 'g'
	           || argv[i+4][strlen(argv[i+4])-1] == 'l'))
	          greenType = GAUSSIANLOG;

               i += 1;
	    }
	    

            else if(argv[i+4][0] == 'a')
            {
               greenType = ASINH;

               strcpy(greenbetastr, "2s");

               if(i+5 < argc)
                  strcpy(greenbetastr, argv[i+5]);

               i += 2;
            }

	    else if(strcmp(argv[i+4], "lin") == 0)
	       greenlogpower = 0;
	    
	    else if(strcmp(argv[i+4], "log") == 0)
	       greenlogpower = 1;

	    else if(strcmp(argv[i+4], "loglog") == 0)
	       greenlogpower = 2;

	    else
	    {
	       greenlogpower = strtol(argv[i+4], &end, 10);

	       if(greenlogpower < 0  || end < argv[i+4] + strlen(argv[i+4]))
		  greenlogpower = 0;
	       else
		  i += 1;
	    }
         }
         
	 i += 2;

         if(fits_open_file(&greenfptr, greenfile, READONLY, &status))
         {
            printf("[struct stat=\"ERROR\", msg=\"Image file %s invalid FITS\"]\n",
               greenfile);
            exit(1);
         }

	 if(hdu[1] > 0)
	 {
	    if(fits_movabs_hdu(greenfptr, hdu[1]+1, NULL, &status))
	    {
	       printf("[struct stat=\"ERROR\", msg=\"Can't find HDU %d\"]\n", hdu[1]);
	       exit(1);
	    }
	 }
      }


      /* BLUE */

      else if(strcmp(argv[i], "-blue") == 0)
      {
         if(i+3 >= argc)
         {
            printf ("[struct stat=\"ERROR\", msg=\"Too few arguments following -blue flag\"]\n");
            fflush(stdout);
            exit(1);
         }

         strcpy(bluefile, argv[i+1]);

	 hdu[2] = getHDU(bluefile);

         if(!nowcs)
	    checkHdr(bluefile, 0, hdu[2]);

         strcpy(blueminstr, argv[i+2]);
         strcpy(bluemaxstr, argv[i+3]);

	 blueType = POWER;

         if(i+4 < argc)
         {
	    if(argv[i+4][0] == 'g')
	    {
	       blueType = GAUSSIAN;

	       if(strlen(argv[i+4]) > 1 
	       && (   argv[i+4][strlen(argv[i+4])-1] == 'g'
	           || argv[i+4][strlen(argv[i+4])-1] == 'l'))
	          blueType = GAUSSIANLOG;
	    }

            else if(argv[i+4][0] == 'a')
            {
               blueType = ASINH;

               strcpy(bluebetastr, "2s");

               if(i+5 < argc)
                  strcpy(bluebetastr, argv[i+5]);

               i += 2;
            }
	    
	    else if(strcmp(argv[i+4], "lin") == 0)
	       bluelogpower = 0;
	    
	    else if(strcmp(argv[i+4], "log") == 0)
	       bluelogpower = 1;

	    else if(strcmp(argv[i+4], "loglog") == 0)
	       bluelogpower = 2;

	    else
	    {
	       bluelogpower = strtol(argv[i+4], &end, 10);

	       if(bluelogpower < 0. || end < argv[i+4] + strlen(argv[i+4]))
		  bluelogpower = 0.;
	       else
		  i += 1;
	    }
         }
         
	 i += 2;

         if(fits_open_file(&bluefptr, bluefile, READONLY, &status))
         {
            printf("[struct stat=\"ERROR\", msg=\"Image file %s invalid FITS\"]\n",
               bluefile);
            exit(1);
         }

	 if(hdu[2] > 0)
	 {
	    if(fits_movabs_hdu(bluefptr, hdu[2]+1, NULL, &status))
	    {
	       printf("[struct stat=\"ERROR\", msg=\"Can't find HDU %d\"]\n", hdu[2]);
	       exit(1);
	    }
	 }
      }


      /* OUTPUT FILE */

      else if(strcmp(argv[i], "-out") == 0)
      {
         if(i+1 >= argc)
         {
            printf ("[struct stat=\"ERROR\", msg=\"Too few arguments following -out flag\"]\n");
            fflush(stdout);
            exit(1);
         }

         strcpy(jpegfile, argv[i+1]);

         jpegfp = fopen(jpegfile, "w+");

         if(jpegfp == (FILE *)NULL)
         {
            printf ("[struct stat=\"ERROR\", msg=\"Error opening output file '%s'\"]\n",
               jpegfile);
            fflush(stdout);
            exit(1);
         }

         ++i;
      }
   }

   if(debug)
   {
      printf("DEBUG> statusfile      = [%s]\n", statusfile);
      printf("DEBUG> colortable      = [%d]\n", colortable);
      printf("\n");
      printf("DEBUG> grayfile        = [%s]\n", grayfile);
      printf("DEBUG> grayminstr      = [%s]\n", grayminstr);
      printf("DEBUG> graymaxstr      = [%s]\n", graymaxstr);
      printf("DEBUG> graylogpower    = [%d]\n", graylogpower);
      printf("DEBUG> grayType        = [%d]\n", grayType);
      printf("DEBUG> graybetastr     = [%s]\n", graybetastr);
      printf("\n");
      printf("DEBUG> redfile         = [%s]\n", redfile);
      printf("DEBUG> redminstr       = [%s]\n", redminstr);
      printf("DEBUG> redmaxstr       = [%s]\n", redmaxstr);
      printf("DEBUG> redlogpower     = [%d]\n", redlogpower);
      printf("DEBUG> redType         = [%d]\n", redType);
      printf("DEBUG> redbetastr      = [%s]\n", redbetastr);
      printf("\n");
      printf("DEBUG> greenfile       = [%s]\n", greenfile);
      printf("DEBUG> greenminstr     = [%s]\n", greenminstr);
      printf("DEBUG> greenmaxstr     = [%s]\n", greenmaxstr);
      printf("DEBUG> greenlogpower   = [%d]\n", greenlogpower);
      printf("DEBUG> greenType       = [%d]\n", greenType);
      printf("DEBUG> greenbetastr    = [%s]\n", greenbetastr);
      printf("\n");
      printf("DEBUG> bluefile        = [%s]\n", bluefile);
      printf("DEBUG> blueminstr      = [%s]\n", blueminstr);
      printf("DEBUG> bluemaxstr      = [%s]\n", bluemaxstr);
      printf("DEBUG> bluelogpower    = [%d]\n", bluelogpower);
      printf("DEBUG> blueType        = [%d]\n", blueType);
      printf("DEBUG> bluebetastr     = [%s]\n", bluebetastr);
      printf("\n");
      printf("DEBUG> jpegfile        = [%s]\n", jpegfile);
      fflush(stdout);
   }

   if(nowcs && nmark)
   {
      printf ("[struct stat=\"ERROR\", msg=\"Can't place markers in non-WCS mode\"]\n");
      fflush(stdout);
      exit(1);
   }

   if(nowcs && showCompass)
   {
      printf ("[struct stat=\"ERROR\", msg=\"Can't show compass in non-WCS mode\"]\n");
      fflush(stdout);
      exit(1);
   }

   isRGB = 0;

   if(strlen(redfile)   > 0
   || strlen(greenfile) > 0
   || strlen(bluefile)  > 0)
      isRGB = 1;
   

   if(isRGB)
   {
      if(strlen(redfile) == 0)
      {
	 printf ("[struct stat=\"ERROR\", msg=\"No input 'red' FITS file name given\"]\n");
	 fflush(stdout);
	 exit(1);
      }

      if(strlen(greenfile) == 0)
      {
	 printf ("[struct stat=\"ERROR\", msg=\"No input 'green' FITS file name given\"]\n");
	 fflush(stdout);
	 exit(1);
      }

      if(strlen(bluefile) == 0)
      {
	 printf ("[struct stat=\"ERROR\", msg=\"No input 'blue' FITS file name given\"]\n");
	 fflush(stdout);
	 exit(1);
      }
   }
   else
   {
      if(strlen(grayfile) == 0)
      {
	 printf ("[struct stat=\"ERROR\", msg=\"No input FITS file name given\"]\n");
	 fflush(stdout);
	 exit(1);
      }
   }

      
   if(strlen(jpegfile) == 0)
   {
      printf ("[struct stat=\"ERROR\", msg=\"No output JPEG file name given\"]\n");
      fflush(stdout);
      exit(1);
   }


   /***************************/
   /* Set up pseudocolor info */
   /***************************/

   createColorTable(colortable);


   /*****************************************************/
   /* We are either making a grayscale/pseudocolor JPEG */
   /* from a single FITS file or a true color JPEG from */
   /* three FITS files.                                 */
   /*****************************************************/

   /* Full color mode */

   if(isRGB)
   {
      /* First make sure that the WCS info is OK */
      /* and is the same for all three images    */

      if(strlen(redfile) == 0)
      {
         printf ("[struct stat=\"ERROR\", msg=\"Color mode but no red image given\"]\n");
         fflush(stdout);
         exit(1);
      }

      if(strlen(greenfile) == 0)
      {
         printf ("[struct stat=\"ERROR\", msg=\"Color mode but no green image given\"]\n");
         fflush(stdout);
         exit(1);
      }

      if(strlen(bluefile) == 0)
      {
         printf ("[struct stat=\"ERROR\", msg=\"Color mode but no blue image given\"]\n");
         fflush(stdout);
         exit(1);
      }


      /* Get the red file's header */

      if(fits_get_hdrpos(redfptr, &keysexist, &keynum, &status))
         printFitsError(status);

      header  = malloc(keysexist * 80 + 1024);
      comment = malloc(keysexist * 80 + 1024);

      if(fits_get_image_wcs_keys(redfptr, &header, &status))
         printFitsError(status);

      wcs = wcsinit(header);

      if(wcs == (struct WorldCoor *)NULL)
      {
         if(nowcs)
         {
	    status = 0;
	    if(fits_read_key(redfptr, TLONG, "NAXIS1", &naxis1, (char *)NULL, &status))
	       printFitsError(status);

	    status = 0;
	    if(fits_read_key(redfptr, TLONG, "NAXIS2", &naxis2, (char *)NULL, &status))
	       printFitsError(status);

            wcs = wcsfake(naxis1, naxis2);
	 }
	 else
	 {
	    fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"WCS init failed for [%s].\" ]\n", redfile);
	    exit(1);
	 }
      }

      if(!nowcs)
	 checkWCS(wcs, 0);

      naxis1 = wcs->nxpix;
      naxis2 = wcs->nypix;

      crval1 = wcs->xpos;
      crval2 = wcs->ypos;

      crpix1 = wcs->xrefpix;
      crpix2 = wcs->yrefpix;

      xinc   = wcs->xinc;
      yinc   = wcs->yinc;
      cdelt1 = wcs->cdelt[0];
      cdelt2 = wcs->cdelt[1];
      cd11   = wcs->cd[0];
      cd12   = wcs->cd[1];
      cd21   = wcs->cd[2];
      cd22   = wcs->cd[3];
      crota2 = wcs->rot;

      if(debug)
      {
	 printf("\nRed image:\n");
	 printf("naxis1 = %d\n", (int)wcs->nxpix);
	 printf("naxis2 = %d\n", (int)wcs->nypix);
	 printf("crval1 = %.6f\n", wcs->xpos);
	 printf("crval2 = %.6f\n", wcs->ypos);
	 printf("crpix1 = %.6f\n", wcs->xrefpix);
	 printf("crpix2 = %.6f\n", wcs->yrefpix);
	 printf("xinc   = %.6f\n", wcs->xinc);
	 printf("yinc   = %.6f\n", wcs->yinc);
	 printf("crota2 = %.6f\n", wcs->rot);
	 fflush(stdout);
      }

      redxmin = -wcs->xrefpix;
      redxmax = redxmin + wcs->nxpix;

      redymin = -wcs->yrefpix;
      redymax = redymin + wcs->nypix;

      if(debug)
      {
	 printf("redxmin   = %-g\n", redxmin);
	 printf("redxmax   = %-g\n", redxmax);
	 printf("redymin   = %-g\n", redymin);
	 printf("redymax   = %-g\n", redymax);
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



      /* Check to see if the green image */
      /* has the same size / scale       */

      free(header);

      if(fits_get_hdrpos(greenfptr, &keysexist, &keynum, &status))
         printFitsError(status);

      header = malloc(keysexist * 80 + 1024);

      if(fits_get_image_wcs_keys(greenfptr, &header, &status))
         printFitsError(status);

      wcsfree(wcs);

      wcs = wcsinit(header);

      if(wcs == (struct WorldCoor *)NULL)
      {
         if(nowcs)
         {
	    status = 0;
	    if(fits_read_key(greenfptr, TLONG, "NAXIS1", &naxis1, (char *)NULL, &status))
	       printFitsError(status);

	    status = 0;
	    if(fits_read_key(greenfptr, TLONG, "NAXIS2", &naxis2, (char *)NULL, &status))
	       printFitsError(status);

            wcs = wcsfake(naxis1, naxis2);
	 }
	 else
	 {
	    fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"WCS init failed for [%s].\" ]\n", greenfile);
	    exit(1);
	 }
      }

      if(!nowcs)
	 checkWCS(wcs, 0);

      if(debug)
      {
	 printf("\nGreen image:\n");
	 printf("naxis1 = %d\n", (int)wcs->nxpix);
	 printf("naxis2 = %d\n", (int)wcs->nypix);
	 printf("crval1 = %.6f\n", wcs->xpos);
	 printf("crval2 = %.6f\n", wcs->ypos);
	 printf("crpix1 = %.6f\n", wcs->xrefpix);
	 printf("crpix2 = %.6f\n", wcs->yrefpix);
	 printf("xinc   = %.6f\n", wcs->xinc);
	 printf("yinc   = %.6f\n", wcs->yinc);
	 printf("crota2 = %.6f\n", wcs->rot);
	 fflush(stdout);
      }

      if(crval1 != wcs->xpos
      || crval2 != wcs->ypos
      || xinc   != wcs->xinc
      || yinc   != wcs->yinc
      || crota2 != wcs->rot)
      {
         printf ("[struct stat=\"ERROR\", msg=\"Red and green FITS images don't match\"]\n");
         fflush(stdout);
         exit(1);
      }

      greenxmin = -wcs->xrefpix;
      greenxmax = greenxmin + wcs->nxpix;

      greenymin = -wcs->yrefpix;
      greenymax = greenymin + wcs->nypix;

      if(debug)
      {
	 printf("greenxmin = %-g\n", greenxmin);
	 printf("greenxmax = %-g\n", greenxmax);
	 printf("greenymin = %-g\n", greenymin);
	 printf("greenymax = %-g\n", greenymax);
	 fflush(stdout);
      }



      /* Check to see if the blue image */
      /* has the same size / scale      */

      free(header);

      if(fits_get_hdrpos(greenfptr, &keysexist, &keynum, &status))
         printFitsError(status);

      header = malloc(keysexist * 80 + 1024);

      if(fits_get_image_wcs_keys(bluefptr, &header, &status))
         printFitsError(status);

      wcsfree(wcs);

      wcs = wcsinit(header);

      if(wcs == (struct WorldCoor *)NULL)
      {
         if(nowcs)
         {
	    status = 0;
	    if(fits_read_key(bluefptr, TLONG, "NAXIS1", &naxis1, (char *)NULL, &status))
	       printFitsError(status);

	    status = 0;
	    if(fits_read_key(bluefptr, TLONG, "NAXIS2", &naxis2, (char *)NULL, &status))
	       printFitsError(status);

            wcs = wcsfake(naxis1, naxis2);
	 }
	 else
	 {
	    fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"WCS init failed for [%s].\" ]\n", bluefile);
	    exit(1);
	 }
      }

      if(!nowcs)
	 checkWCS(wcs, 0);

      if(debug)
      {
	 printf("\nBlue image:\n");
	 printf("naxis1 = %d\n", (int)wcs->nxpix);
	 printf("naxis2 = %d\n", (int)wcs->nypix);
	 printf("crval1 = %.6f\n", wcs->xpos);
	 printf("crval2 = %.6f\n", wcs->ypos);
	 printf("crpix1 = %.6f\n", wcs->xrefpix);
	 printf("crpix2 = %.6f\n", wcs->yrefpix);
	 printf("xinc   = %.6f\n", wcs->xinc);
	 printf("yinc   = %.6f\n", wcs->yinc);
	 printf("crota2 = %.6f\n", wcs->rot);
	 fflush(stdout);
      }

      if(crval1 != wcs->xpos
      || crval2 != wcs->ypos
      || xinc   != wcs->xinc
      || yinc   != wcs->yinc
      || crota2 != wcs->rot)
      {
         printf ("[struct stat=\"ERROR\", msg=\"Red and blue FITS images don't match\"]\n");
         fflush(stdout);
         exit(1);
      }

      bluexmin = -wcs->xrefpix;
      bluexmax = bluexmin + wcs->nxpix;

      blueymin = -wcs->yrefpix;
      blueymax = blueymin + wcs->nypix;

      if(debug)
      {
	 printf("bluexmin  = %-g\n", bluexmin);
	 printf("bluexmax  = %-g\n", bluexmax);
	 printf("blueymin  = %-g\n", blueymin);
	 printf("blueymax  = %-g\n", blueymax);
	 fflush(stdout);
      }


      /* Find the X offsets and total size */

      xmin = redxmin;

      if(greenxmin < xmin) xmin = greenxmin;
      if(bluexmin  < xmin) xmin = bluexmin;

      xmax = redxmax;

      if(greenxmax > xmax) xmax = greenxmax;
      if(bluexmax  > xmax) xmax = bluexmax;

      redxoff   = redxmin   - xmin;
      greenxoff = greenxmin - xmin;
      bluexoff  = bluexmin  - xmin;

      greennaxis1 = greenxmax - greenxmin;
      rednaxis1   = redxmax   - redxmin;
      bluenaxis1  = bluexmax  - bluexmin;

      naxis1 = xmax - xmin;

      crpix1 = -xmin;



      /* Find the Y offsets and total size */

      ymin = redymin;

      if(greenymin < ymin) ymin = greenymin;
      if(blueymin  < ymin) ymin = blueymin;

      ymax = redymax;

      if(greenymax > ymax) ymax = greenymax;
      if(blueymax  > ymax) ymax = blueymax;

      redyoff   = redymin   - ymin;
      greenyoff = greenymin - ymin;
      blueyoff  = blueymin  - ymin;

      greennaxis2 = greenymax - greenymin;
      rednaxis2   = redymax   - redymin;
      bluenaxis2  = blueymax  - blueymin;

      naxis2 = ymax - ymin;

      crpix2 = -ymin;


      if(debug)
      {
         printf("\n");
         printf("DEBUG> COLOR: crval1    = %.6f\n", crval1);
         printf("DEBUG> COLOR: crval2    = %.6f\n", crval2);
         printf("DEBUG> COLOR: cdelt1    = %.6f\n", cdelt1);
         printf("DEBUG> COLOR: cdelt2    = %.6f\n", cdelt2);
         printf("DEBUG> COLOR: crota2    = %.6f\n", crota2);
         printf("DEBUG> COLOR: naxis1    = %d\n",  naxis1);
         printf("DEBUG> COLOR: naxis2    = %d\n",  naxis2);
         printf("DEBUG> COLOR: crpix1    = %-g\n", crpix1);
         printf("DEBUG> COLOR: crpix2    = %-g\n", crpix2);
         printf("DEBUG> COLOR: xmin      = %-g\n", xmin);
         printf("DEBUG> COLOR: xmax      = %-g\n", xmax);
         printf("DEBUG> COLOR: ymin      = %-g\n", ymin);
         printf("DEBUG> COLOR: ymax      = %-g\n", ymax);
         printf("DEBUG> COLOR: redxoff   = %-g\n", redxoff);
         printf("DEBUG> COLOR: greenxoff = %-g\n", greenxoff);
         printf("DEBUG> COLOR: bluexoff  = %-g\n", bluexoff);
         printf("DEBUG> COLOR: redyoff   = %-g\n", redyoff);
         printf("DEBUG> COLOR: greenyoff = %-g\n", greenyoff);
         printf("DEBUG> COLOR: blueyoff  = %-g\n", blueyoff);
         fflush(stdout);
      }

      if(!noflip)
      {
	 if(xinc > 0)
	   flipX = 1;

	 if(yinc > 0)
	   flipY = 1;

	 if(fabs(crota2) >  90.
	 && fabs(crota2) < 270.)
	 {
	    flipX = 1 - flipX;
	    flipY = 1 - flipY;

	    crota2 = crota2 + 180;

	    while(crota2 > 360) crota2 -= 360.;
	 }

	 if(wcs->coorflip)
	    flipX = 1 - flipX;

	 if(flipX)
	 {
	   cdelt1 = -cdelt1;
	   cd11   = -cd11;
	   cd21   = -cd21;
	 }

	 if(flipY)
	 {
	   cdelt2 = -cdelt2;
	   cd12   = -cd12;
	   cd22   = -cd22;
	 }
      }

      make_comment(header, comment);


      /* Find the pixel locations of any markers */

      if(wcs->syswcs == WCS_J2000)
      {
	 sys   = EQUJ;
	 epoch = 2000.;

	 if(wcs->equinox == 1950)
	    epoch = 1950.;
      }
      else if(wcs->syswcs == WCS_B1950)
      {
	 sys   = EQUB;
	 epoch = 1950.;

	 if(wcs->equinox == 2000)
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

	 if(wcs->equinox == 1950)
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

      for(i=0; i<nmark; ++i)
      {
	 offscl = 0;

	 usemark[i] = 1;

         convertCoordinates(marksys[i], markepoch[i],
                            marklon[i], marklat[i],
                            sys, epoch, &lon, &lat, 0.0);

	 wcs2pix(wcs, lon, lat, 
		 &xmark, &ymark, &offscl);

         fixxy(&xmark, &ymark, &offscl);

	 imark[i] = (int)(xmark+0.5);
	 jmark[i] = (int)(ymark+0.5);

	 if(   imark[i] < -marksize[i] - 2
	    || imark[i] > naxis1 + marksize[i] + 2
	    || jmark[i] < -marksize[i] - 2
	    || jmark[i] < naxis2 + marksize[i] + 2)
	    usemark[i] = 0;
      }


      /* Now adjust the data range if the limits */
      /* were percentiles.  We had to wait until */
      /* we had naxis1, naxis2 values which is   */
      /* why this is here                        */

      if(debug)
         printf("\n RED RANGE:\n");

      getRange(redfptr,       redminstr,    redmaxstr,   
	       &redminval,   &redmaxval,    redType,   
	       redbetastr,   &redbetaval,   reddataval,
	       rednaxis1,     rednaxis2);

      redminpercent = valuePercentile(redminval);
      redmaxpercent = valuePercentile(redmaxval);

      reddiff   = redmaxval   - redminval;

      if(debug)
      {
	 printf("DEBUG> redminval   = %-g (%-g%%)\n", redminval, redminpercent);
	 printf("DEBUG> redmaxval   = %-g (%-g%%)\n", redmaxval, redmaxpercent);
	 printf("DEBUG> reddiff     = %-g\n\n", reddiff);
	 fflush(stdout);
      }

      if(debug)
         printf("\n GREEN RANGE:\n");

      getRange(greenfptr,     greenminstr,  greenmaxstr, 
               &greenminval, &greenmaxval,  greenType, 
	       greenbetastr, &greenbetaval, greendataval,
	       greennaxis1,   greennaxis2);

      greenminpercent = valuePercentile(greenminval);
      greenmaxpercent = valuePercentile(greenmaxval);

      greendiff = greenmaxval - greenminval;
      
      if(debug)
      {
	 printf("DEBUG> greenminval = %-g (%-g%%)\n", greenminval, greenminpercent);
	 printf("DEBUG> greenmaxval = %-g (%-g%%)\n", greenmaxval, greenmaxpercent);
	 printf("DEBUG> greendiff   = %-g\n\n", greendiff);
	 fflush(stdout);
      }

      if(debug)
         printf("\n BLUE RANGE:\n");

      getRange(bluefptr,      blueminstr,   bluemaxstr,  
               &blueminval,  &bluemaxval,   blueType,  
	       bluebetastr,  &bluebetaval,  bluedataval,
	       bluenaxis1,    bluenaxis2);

      blueminpercent = valuePercentile(blueminval);
      bluemaxpercent = valuePercentile(bluemaxval);

      bluediff  = bluemaxval  - blueminval;

      if(debug)
      {
	 printf("DEBUG> blueminval  = %-g (%-g%%)\n", blueminval, blueminpercent);
	 printf("DEBUG> bluemaxval  = %-g (%-g%%)\n", bluemaxval, bluemaxpercent);
	 printf("DEBUG> bluediff    = %-g\n\n", bluediff);
	 fflush(stdout);
      }
	 

      /* Figure out which set of pixels to use */
      /* and what order to read them in        */

      istart = 0;
      iend   = naxis1 - 1;
      nx     = naxis1;

      if(naxis1 > MAXDIM)
      {
         istart = (naxis1 - MAXDIM)/2;
         iend   = istart + MAXDIM - 1;
         nx     = MAXDIM;
      }

      jstart = 0;
      jend   = naxis2 - 1;
      ny     = naxis2;
      jinc   = 1;

      if(naxis2 > MAXDIM)
      {
         jstart = (naxis2 - MAXDIM)/2;
         jend   = istart + MAXDIM - 2;
         ny     = MAXDIM;
      }

      if(flipY)
      {
         itemp  = jstart;
	 jstart = jend;
	 jend   = itemp;
	 jinc   = -1;
      }

      if(debug)
      {
         printf("\n");
         printf("DEBUG> nx               = %d\n", nx);
         printf("DEBUG> istart           = %d\n", istart);
         printf("DEBUG> iend             = %d\n", iend);
         printf("DEBUG> flipX            = %d\n", flipX);
         printf("\n");
         printf("DEBUG> ny               = %d\n", ny);
         printf("DEBUG> jstart           = %d\n", jstart);
         printf("DEBUG> jend             = %d\n", jend);
         printf("DEBUG> jinc             = %d\n", jinc);
         printf("DEBUG> flipY            = %d\n", flipY);
         fflush(stdout);
      }


      /* Set up the JPEG library info  */
      /* and start the JPEG compressor */

      cinfo.err = jpeg_std_error(&jerr);

      jpeg_create_compress(&cinfo);

      jpeg_stdio_dest(&cinfo, jpegfp);

      cinfo.image_width      = nx;
      cinfo.image_height     = ny;
      cinfo.input_components = 3;
      cinfo.in_color_space   = JCS_RGB;

      quality = 85;

      jpeg_set_defaults(&cinfo);

      jpeg_set_quality (&cinfo, quality, TRUE);

      jpeg_start_compress(&cinfo, TRUE);

      jpeg_write_marker(&cinfo, JPEG_COM, 
	 (const JOCTET *)comment, strlen(comment));


      /* Compute info for optional compass 'rose' */

      rosescale = nx / 10.;

      if(ny/10. < rosescale)
	 rosescale = ny / 10.;

      jmin = 0;
      jmax = 2 * rosescale;

      imin = 0;
      imax = 2 * rosescale;

      if(jmax > ny)
	 jmax = ny;

      if(imax > nx)
	 imax = nx;

      x1n = rosescale;
      y1n = rosescale;

      x2n = rosescale + rosescale * sin((crota2+180.)*dtr);
      y2n = rosescale + rosescale * cos((crota2+180.)*dtr);

      len2n = rosescale * rosescale;

      x1e = rosescale;
      y1e = rosescale;

      x2e = rosescale + rosescale/2. * sin((crota2+270.)*dtr);
      y2e = rosescale + rosescale/2. * cos((crota2+270.)*dtr);

      len2e = rosescale/2. * rosescale/2.;

      lwidth = 2;

      
      /* Now, loop over the parts of the images we want */

      fpixel[0] = 1;
      fpixel[2] = 1;
      fpixel[3] = 1;

      nelements = naxis1;

      rfitsbuf = (double *)malloc(nelements * sizeof(double));
      gfitsbuf = (double *)malloc(nelements * sizeof(double));
      bfitsbuf = (double *)malloc(nelements * sizeof(double));

      jpegdata = (JSAMPROW)malloc(3*nelements);

      jpegptr[0] = (JSAMPROW) jpegdata;

      for(jj=0; jj<ny; ++jj)
      {
	 for(i=0; i<nelements; ++i)
	 {
	    rfitsbuf[i] = nan;
	    gfitsbuf[i] = nan;
	    bfitsbuf[i] = nan;
	 }

	 j = jstart + jj*jinc;


	 /* Read red */

	 if(j - redyoff >= 0 && j - redyoff < rednaxis2)
	 {
	    fpixel[1] = j - redyoff + 1;

	    if(fits_read_pix(redfptr, TDOUBLE, fpixel, rednaxis1, NULL,
			     rfitsbuf, &nullcnt, &status))
	       printFitsError(status);
	 }


	 /* Read green */

	 if(j - greenyoff >= 0 && j - greenyoff < greennaxis2)
	 {
	    fpixel[1] = j - greenyoff + 1;

	    if(fits_read_pix(greenfptr, TDOUBLE, fpixel, greennaxis1, NULL,
			     gfitsbuf, &nullcnt, &status))
	       printFitsError(status);
	 }


	 /* Read blue */

	 if(j - blueyoff >= 0 && j - blueyoff < bluenaxis2)
	 {
	    fpixel[1] = j - blueyoff + 1;

	    if(fits_read_pix(bluefptr, TDOUBLE, fpixel, bluenaxis1, NULL,
			     bfitsbuf, &nullcnt, &status))
	       printFitsError(status);
	 }


	 /* Look up pixel color values */

         for(i=0; i<nx; ++i)
         {

            /* RED */

	    ipix = i + istart - redxoff;

	    if(ipix < 0 || ipix >= rednaxis1)
	       redval = nan;
	    else
	       redval = rfitsbuf[ipix];


	    /* Special case: blank pixel */

            if(mNaN(redval))
               redjpeg = 0.;


	    /* Gaussian histogram equalization */

            else if(redType == GAUSSIAN
                 || redType == GAUSSIANLOG)
            {
               for(index=0; index<256; ++index)
               {
                  if(reddataval[index] >= redval)
                     break;
               }

               if(index <   0) index =   0;
               if(index > 255) index = 255;

               redjpeg = index;
            }


	    /* ASIHN stretch */

            else if(redType == ASINH)
            {
               redjpeg  = (redval - redminval)/(redmaxval - redminval);

               if(redjpeg < 0.0)
                  redjpeg = 0.;
               else
	          redjpeg = 255. * asinh(redbetaval * redjpeg)/redbetaval;

               if(redjpeg < 0.)
                  redjpeg = 0.;

               if(redjpeg > 255.)
                  redjpeg = 255.;

	       index = (int)(redjpeg+0.5);
	    }


	    /* Finally, log power (including linear) */

            else
            {
               if(redval < redminval)
                  redval = redminval;

               if(redval > redmaxval)
                  redval = redmaxval;

               redjpeg = (redval - redminval)/reddiff;

               for(k=1; k<=redlogpower; ++k)
                  redjpeg = log(9.*redjpeg+1.);
               
               redjpeg = 255. * redjpeg;
            }

            if(redjpeg < 0.)
               redjpeg = 0.;



            /* GREEN */

	    ipix = i + istart - greenxoff;

	    if(ipix < 0 || ipix >= greennaxis1)
	       greenval = nan;
	    else
	       greenval = gfitsbuf[ipix];


	    /* Special case: blank pixel */

            if(mNaN(greenval))
               greenjpeg = 0.;


	    /* Gaussian histogram equalization */

            else if(greenType == GAUSSIAN
                 || greenType == GAUSSIANLOG)
            {
               for(index=0; index<256; ++index)
               {
                  if(greendataval[index] >= greenval)
                     break;
               }

               if(index <   0) index =   0;
               if(index > 255) index = 255;

               greenjpeg = index;
            }

	    /* ASIHN stretch */

            else if(greenType == ASINH)
            {
               greenjpeg  = (greenval - greenminval)/(greenmaxval - greenminval);

               if(greenjpeg < 0.0)
                  greenjpeg = 0.;
               else
	          greenjpeg = 255. * asinh(greenbetaval * greenjpeg)/greenbetaval;

               if(greenjpeg < 0.)
                  greenjpeg = 0.;

               if(greenjpeg > 255.)
                  greenjpeg = 255.;

	       index = (int)(greenjpeg+0.5);
	    }


	    /* Finally, log power (including linear) */

            else
            {
               if(greenval < greenminval)
                  greenval = greenminval;

               if(greenval > greenmaxval)
                  greenval = greenmaxval;

               greenjpeg = (greenval - greenminval)/greendiff;

               for(k=1; k<=greenlogpower; ++k)
                  greenjpeg = log(9.*greenjpeg+1.);
               
               greenjpeg = 255. * greenjpeg;
            }

            if(greenjpeg < 0.)
               greenjpeg = 0.;



            /* BLUE */

	    ipix = i + istart - bluexoff;

	    if(ipix < 0 || ipix >= bluenaxis1)
	       blueval = nan;
	    else
	       blueval = bfitsbuf[ipix];


	    /* Special case: blank pixel */

            if(mNaN(blueval))
               bluejpeg = 0.;


	    /* Gaussian histogram equalization */

            else if(blueType == GAUSSIAN
                 || blueType == GAUSSIANLOG)
            {
               for(index=0; index<256; ++index)
               {
                  if(bluedataval[index] >= blueval)
                     break;
               }

               if(index <   0) index =   0;
               if(index > 255) index = 255;

               bluejpeg = index;
            }


	    /* ASIHN stretch */

            else if(blueType == ASINH)
            {
               bluejpeg  = (blueval - blueminval)/(bluemaxval - blueminval);

               if(bluejpeg < 0.0)
                  bluejpeg = 0.;
               else
	          bluejpeg = 255. * asinh(bluebetaval * bluejpeg)/bluebetaval;

               if(bluejpeg < 0.)
                  bluejpeg = 0.;

               if(bluejpeg > 255.)
                  bluejpeg = 255.;

	       index = (int)(bluejpeg+0.5);
	    }


	    /* Finally, log power (including linear) */

            else
            {
               if(blueval < blueminval)
                  blueval = blueminval;

               if(blueval > bluemaxval)
                  blueval = bluemaxval;

               bluejpeg = (blueval - blueminval)/bluediff;

               for(k=1; k<=bluelogpower; ++k)
                  bluejpeg = log(9.*bluejpeg+1.);
               
               bluejpeg = 255. * bluejpeg;
            }

            if(bluejpeg < 0.)
               bluejpeg = 0.;


	    /* If we wish to preserve "color" */

            if(truecolor > 0.)
            {
	       if(bluejpeg  >= 255.
	       || greenjpeg >= 255.
	       || redjpeg   >= 255.)
	       {
		  maxjpeg = redjpeg;

		  if(greenjpeg > maxjpeg) maxjpeg = greenjpeg;
		  if(bluejpeg  > maxjpeg) maxjpeg = bluejpeg;

		  redjpeg   = pow(redjpeg   / maxjpeg, truecolor) * 255.;
		  greenjpeg = pow(greenjpeg / maxjpeg, truecolor) * 255.;
		  bluejpeg  = pow(bluejpeg  / maxjpeg, truecolor) * 255.;
	       }
	       else
	       {
		  maxjpeg = redjpeg;

		  if(greenjpeg > maxjpeg) maxjpeg = greenjpeg;
		  if(bluejpeg  > maxjpeg) maxjpeg = bluejpeg;

		  if(maxjpeg > 0.)
		  {
		     redjpeg   = pow(redjpeg   / maxjpeg, truecolor) * maxjpeg;
		     greenjpeg = pow(greenjpeg / maxjpeg, truecolor) * maxjpeg;
		     bluejpeg  = pow(bluejpeg  / maxjpeg, truecolor) * maxjpeg;
		  }
	       }
	    }
            else
	    {
	       if(bluejpeg  > 255.) bluejpeg  = 255.;
	       if(greenjpeg > 255.) greenjpeg = 255.;
	       if(redjpeg   > 255.) redjpeg   = 255.;
	    }

            
	    /* Populate the output JPEG array */

	    if(flipX)
	    {
	       jpegdata[3*(nx-1-i)  ] = (int)redjpeg;
	       jpegdata[3*(nx-1-i)+1] = (int)greenjpeg;
	       jpegdata[3*(nx-1-i)+2] = (int)bluejpeg;
	    }
	    else
	    {
	       jpegdata[3*i  ] = (int)redjpeg;
	       jpegdata[3*i+1] = (int)greenjpeg;
	       jpegdata[3*i+2] = (int)bluejpeg;
	    }

	    for(k=0; k<nmark; ++k)
	    {
	       if((   abs(imark[k] - i) == marksize[k]
	           && abs(jmark[k] - j) <  marksize[k])
	       || (   abs(jmark[k] - j) == marksize[k]
	           && abs(imark[k] - i) <  marksize[k]))
	       {
		  if(flipX)
		  {
		     jpegdata[3*(nx-1-i)  ] = markcolor[k][0];
		     jpegdata[3*(nx-1-i)+1] = markcolor[k][1];
		     jpegdata[3*(nx-1-i)+2] = markcolor[k][2];
		  }
		  else
		  {
		     jpegdata[3*i  ] = markcolor[k][0];
		     jpegdata[3*i+1] = markcolor[k][1];
		     jpegdata[3*i+2] = markcolor[k][2];
		  }
	       }
	    }
         }


	 /* Add compass "rose" */

	 if(showCompass)
	 {
	    if(jj >= jmin && jj <= jmax)
	    {
	       for(i=imin; i<imax; ++i)
	       {
		  if(covered(i, jj, x1n, y1n, x2n, y2n, len2n, lwidth, &color))
		  {
		     jpegdata[3*i  ] = color;
		     jpegdata[3*i+1] = color;
		     jpegdata[3*i+2] = color;
		  }

		  if(covered(i, jj, x1e, y1e, x2e, y2e, len2e, lwidth, &color))
		  {
		     jpegdata[3*i  ] = color;
		     jpegdata[3*i+1] = color;
		     jpegdata[3*i+2] = color;
		  }
	       }
	    }
	 }
	 
         
         jpeg_write_scanlines(&cinfo, jpegptr, 1);
      }
   }


   /* Grayscale/pseudocolor mode */

   else
   {
      /* First make sure that the WCS info is OK */

      if(strlen(grayfile) == 0)
      {
         printf ("[struct stat=\"ERROR\", msg=\"Grayscale/pseudocolor mode but no gray image given\"]\n");
         fflush(stdout);
         exit(1);
      }

      if(fits_get_hdrpos(grayfptr, &keysexist, &keynum, &status))
         printFitsError(status);

      header  = malloc(keysexist * 80 + 1024);
      comment = malloc(keysexist * 80 + 1024);

      if(fits_get_image_wcs_keys(grayfptr, &header, &status))
         printFitsError(status);

      wcs = wcsinit(header);

      if(wcs == (struct WorldCoor *)NULL)
      {
         if(nowcs)
         {
	    status = 0;
	    if(fits_read_key(grayfptr, TLONG, "NAXIS1", &naxis1, (char *)NULL, &status))
	       printFitsError(status);

	    status = 0;
	    if(fits_read_key(grayfptr, TLONG, "NAXIS2", &naxis2, (char *)NULL, &status))
	       printFitsError(status);

            wcs = wcsfake(naxis1, naxis2);
	 }
	 else
	 {
	    fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"WCS init failed for [%s].\" ]\n", grayfile);
	    exit(1);
	 }
      }

      if(!nowcs)
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


      naxis1 = wcs->nxpix;
      naxis2 = wcs->nypix;

      crpix1 = wcs->xrefpix;
      crpix2 = wcs->yrefpix;

      xinc   = wcs->xinc;
      yinc   = wcs->yinc;
      cdelt1 = wcs->cdelt[0];
      cdelt2 = wcs->cdelt[1];
      cd11   = wcs->cd[0];
      cd12   = wcs->cd[1];
      cd21   = wcs->cd[2];
      cd22   = wcs->cd[3];
      crota2 = wcs->rot;

      if(debug)
      {
         printf("\n");
         printf("DEBUG> GRAY: naxis1 = %d\n" , naxis1);
         printf("DEBUG> GRAY: naxis2 = %d\n",  naxis2);
         printf("DEBUG> GRAY: xinc   = %-g\n", xinc);
         printf("DEBUG> GRAY: yinc   = %-g\n", yinc);
         printf("DEBUG> GRAY: crota2 = %-g\n", crota2);
         fflush(stdout);
      }

      if(!noflip)
      {
	 if(xinc > 0)
	   flipX = 1;

	 if(yinc > 0)
	   flipY = 1;

	 if(fabs(crota2) >  90.
	 && fabs(crota2) < 270.)
	 {
	    flipX = 1 - flipX;
	    flipY = 1 - flipY;

	    crota2 = crota2 + 180;

	    while(crota2 > 360) crota2 -= 360.;
	 }

	 if(wcs->coorflip)
	    flipX = 1 - flipX;

	 if(flipX)
	 {
	   cdelt1 = -cdelt1;
	   cd11   = -cd11;
	   cd21   = -cd21;
	 }

	 if(flipY)
	 {
	   cdelt2 = -cdelt2;
	   cd12   = -cd12;
	   cd22   = -cd22;
	 }
      }

      make_comment(header, comment);


      /* Find the pixel locations of any markers */

      if(wcs->syswcs == WCS_J2000)
      {
	 sys   = EQUJ;
	 epoch = 2000.;

	 if(wcs->equinox == 1950)
	    epoch = 1950.;
      }
      else if(wcs->syswcs == WCS_B1950)
      {
	 sys   = EQUB;
	 epoch = 1950.;

	 if(wcs->equinox == 2000)
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

	 if(wcs->equinox == 1950)
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

      for(i=0; i<nmark; ++i)
      {
	 offscl = 0;

	 usemark[i] = 1;

         convertCoordinates(marksys[i], markepoch[i],
                            marklon[i], marklat[i],
                            sys, epoch, &lon, &lat, 0.0);

	 wcs2pix(wcs, lon, lat, 
		 &xmark, &ymark, &offscl);

	 imark[i] = (int)(xmark+0.5);
	 jmark[i] = (int)(ymark+0.5);

	 if(offscl)
	    usemark[i] = 0;
      }


      /* Now adjust the data range if the limits */
      /* were percentiles.  We had to wait until */
      /* we had naxis1, naxis2 which is why this */
      /* is here                                 */

      if(debug)
         printf("\n GRAY RANGE:\n");

      getRange(grayfptr,     grayminstr,  graymaxstr, 
	       &grayminval, &graymaxval,  grayType, 
	       graybetastr, &graybetaval, graydataval,
	       naxis1,       naxis2);

      grayminpercent = valuePercentile(grayminval);
      graymaxpercent = valuePercentile(graymaxval);

      graydiff = graymaxval - grayminval;

      if(debug)
      {
	 printf("DEBUG> grayminval = %-g (%-g%%)\n", grayminval, grayminpercent);
	 printf("DEBUG> graymaxval = %-g (%-g%%)\n", graymaxval, graymaxpercent);
	 printf("DEBUG> graydiff   = %-g\n", graydiff);
	 fflush(stdout);
      }


      /* Figure out which set of pixels to use */
      /* and what order to read them in        */

      istart = 0;
      iend   = naxis1 - 1;
      nx     = naxis1;

      if(naxis1 > MAXDIM)
      {
         istart = (naxis1 - MAXDIM)/2;
         iend   = istart + MAXDIM - 1;
         nx     = MAXDIM;
      }

      jstart = 0;
      jend   = naxis2 - 1;
      ny     = naxis2;
      jinc   = 1;

      if(naxis2 > MAXDIM)
      {
         jstart = (naxis2 - MAXDIM)/2;
         jend   = istart + MAXDIM - 2;
         ny     = MAXDIM;
      }

      if(flipY)
      {
         itemp  = jstart;
	 jstart = jend;
	 jend   = itemp;
	 jinc   = -1;
      }

      if(debug)
      {
         printf("\n");
         printf("DEBUG> nx               = %d\n", nx);
         printf("DEBUG> istart           = %d\n", istart);
         printf("DEBUG> iend             = %d\n", iend);
         printf("DEBUG> flipX            = %d\n", flipX);
         printf("\n");
         printf("DEBUG> ny               = %d\n", ny);
         printf("DEBUG> jstart           = %d\n", jstart);
         printf("DEBUG> jend             = %d\n", jend);
         printf("DEBUG> jinc             = %d\n", jinc);
         printf("DEBUG> flipY            = %d\n", flipY);
         fflush(stdout);
      }


      /* Set up the JPEG library info  */
      /* and start the JPEG compressor */

      cinfo.err = jpeg_std_error(&jerr);

      jpeg_create_compress(&cinfo);

      jpeg_stdio_dest(&cinfo, jpegfp);

      cinfo.image_width      = nx;
      cinfo.image_height     = ny;
      cinfo.input_components = 3;
      cinfo.in_color_space = JCS_RGB;

      jpeg_set_defaults(&cinfo);

      quality = 85;

      jpeg_set_quality (&cinfo, quality, TRUE);

      jpeg_start_compress(&cinfo, TRUE);

      jpeg_write_marker(&cinfo, JPEG_COM, 
	 (const JOCTET *)comment, strlen(comment));


      /* Compute info for optional compass 'rose' */

      rosescale = nx / 10.;

      if(ny/10. < rosescale)
	 rosescale = ny / 10.;

      jmin = 0;
      jmax = 2 * rosescale;

      imin = 0;
      imax = 2 * rosescale;

      if(jmax > ny)
	 jmax = ny;

      if(imax > nx)
	 imax = nx;

      x1n = rosescale;
      y1n = rosescale;

      x2n = rosescale + rosescale * sin((crota2+180.)*dtr);
      y2n = rosescale + rosescale * cos((crota2+180.)*dtr);

      len2n = rosescale * rosescale;

      x1e = rosescale;
      y1e = rosescale;

      x2e = rosescale + rosescale/2. * sin((crota2+270.)*dtr);
      y2e = rosescale + rosescale/2. * cos((crota2+270.)*dtr);

      len2e = rosescale/2. * rosescale/2.;

      lwidth = 2;


      /* Now, loop over the part of the image we want */

      fpixel[0] = istart+1;
      fpixel[2] = 1;
      fpixel[3] = 1;

      nelements = nx;

      fitsbuf = (double *)malloc(nelements * sizeof(double));

      jpegdata = (JSAMPROW)malloc(3*nelements);

      jpegptr[0] = (JSAMPROW) jpegdata;


      for(jj=0; jj<ny; ++jj)
      {
	 j = jstart + jj*jinc;

	 fpixel[1] = j+1;

         if(fits_read_pix(grayfptr, TDOUBLE, fpixel, nelements, NULL,
                          fitsbuf, &nullcnt, &status))
            printFitsError(status);

         for(i=0; i<nx; ++i)
         {
	    /* Special case: blank pixel */

            if(mNaN(fitsbuf[i-istart]))
               index = 0;


	    /* Gaussian histogram equalization */

            else if(grayType == GAUSSIAN
                 || grayType == GAUSSIANLOG)
	    {
               grayval = fitsbuf[i-istart];

	       for(index=0; index<256; ++index)
	       {
		  if(graydataval[index] >= grayval)
		     break;
	       }

	       if(index <   0) index =   0;
	       if(index > 255) index = 255;
	    }


	    /* ASIHN stretch */

            else if(grayType == ASINH)
            {
               grayval = fitsbuf[i-istart];

               grayjpeg  = (grayval - grayminval)/(graymaxval - grayminval);

               if(grayjpeg < 0.0)
                  grayjpeg = 0.;
               else
	          grayjpeg = 255. * asinh(graybetaval * grayjpeg)/graybetaval;

               if(grayjpeg < 0.)
                  grayjpeg = 0.;

               if(grayjpeg > 255.)
                  grayjpeg = 255.;

	       index = (int)(grayjpeg+0.5);
	    }


	    /* Finally, log power (including linear) */

            else
            {
               grayval = fitsbuf[i-istart];

               if(grayval < grayminval)
                  grayval = grayminval;

               if(grayval > graymaxval)
                  grayval = graymaxval;

               grayjpeg = (grayval - grayminval)/graydiff;

               for(k=1; k<=graylogpower; ++k)
                  grayjpeg = log(9.*grayjpeg+1.);
               
               grayjpeg = 255. * grayjpeg;

	       if(grayjpeg <   0.)
		  grayjpeg =   0.;

	       if(grayjpeg > 255.)
		  grayjpeg = 255.;

	       index = (int)(grayjpeg+0.5);
	    }

	    if(flipX)
	    {
	       jpegdata[3*(nx-1-i)  ] = color_table[index][0];
	       jpegdata[3*(nx-1-i)+1] = color_table[index][1];
	       jpegdata[3*(nx-1-i)+2] = color_table[index][2];
	    }
	    else
	    {
	       jpegdata[3*i  ] = color_table[index][0];
	       jpegdata[3*i+1] = color_table[index][1];
	       jpegdata[3*i+2] = color_table[index][2];
	    }


	    /* Add any markers */

	    for(k=0; k<nmark; ++k)
	    {
	       if((   abs(imark[k] - i) == marksize[k]
	           && abs(jmark[k] - j) <  marksize[k])
	       || (   abs(jmark[k] - j) == marksize[k]
	           && abs(imark[k] - i) <  marksize[k]))
	       {
		  if(flipX)
		  {
		     jpegdata[3*(nx-1-i)  ] = markcolor[k][0];
		     jpegdata[3*(nx-1-i)+1] = markcolor[k][1];
		     jpegdata[3*(nx-1-i)+2] = markcolor[k][2];
		  }
		  else
		  {
		     jpegdata[3*i  ] = markcolor[k][0];
		     jpegdata[3*i+1] = markcolor[k][1];
		     jpegdata[3*i+2] = markcolor[k][2];
		  }
	       }
	    }
         }


	 /* Add compass "rose" */

	 if(showCompass)
	 {
	    if(jj >= jmin && jj <= jmax)
	    {
	       for(i=imin; i<imax; ++i)
	       {
		  if(covered(i, jj, x1n, y1n, x2n, y2n, len2n, lwidth, &color))
		  {
		     jpegdata[3*i  ] = color;
		     jpegdata[3*i+1] = color;
		     jpegdata[3*i+2] = color;
		  }

		  if(covered(i, jj, x1e, y1e, x2e, y2e, len2e, lwidth, &color))
		  {
		     jpegdata[3*i  ] = color;
		     jpegdata[3*i+1] = color;
		     jpegdata[3*i+2] = color;
		  }
	       }
	    }
	 }
	 
         jpeg_write_scanlines(&cinfo, jpegptr, 1);
      }
   }

   /* Close up the JPEG file and get out */

   jpeg_finish_compress(&cinfo);

   fclose(jpegfp);

   jpeg_destroy_compress(&cinfo);

   if(isRGB)
      printf("[struct stat=\"OK\", bmin=%-g, bminpercent=%.2f, bmax=%-g, bmaxpercent=%.2f, gmin=%-g, gminpercent=%.2f, gmax=%-g, gmaxpercent=%.2f, rmin=%-g, rminpercent=%.2f, rmax=%-g, rmaxpercent=%.2f]\n",
         blueminval,  blueminpercent, 
	 bluemaxval,  bluemaxpercent,
         greenminval, greenminpercent,
	 greenmaxval, greenmaxpercent,
         redminval,   redminpercent,
	 redmaxval,   redmaxpercent);
   else
      printf("[struct stat=\"OK\", min=%-g, minpercent=%.2f, max=%-g, maxpercent=%.2f]\n",
         grayminval,  grayminpercent,
	 graymaxval,  graymaxpercent);
   fflush(stdout);
   exit(0);
}


/******************************************/
/*                                        */
/*  Build a fake WCS for the pixel array  */
/*                                        */
/******************************************/

struct WorldCoor *wcsfake(int naxis1, int naxis2)
{
   static struct WorldCoor *wcs;

   char header[4096];
   char hline  [256];

   header[0] = '\0';

   sprintf(hline, "SIMPLE = T");                       stradd(header, hline);
   sprintf(hline, "NAXIS  = 2");                       stradd(header, hline);
   sprintf(hline, "NAXIS1 = %d", naxis1);              stradd(header, hline);
   sprintf(hline, "NAXIS2 = %d", naxis2);              stradd(header, hline);
   sprintf(hline, "CTYPE1 = 'RA---TAN'");              stradd(header, hline);
   sprintf(hline, "CTYPE2 = 'DEC--TAN'");              stradd(header, hline);
   sprintf(hline, "CDELT1 = 0.000001");                stradd(header, hline);
   sprintf(hline, "CDELT2 = 0.000001");                stradd(header, hline);
   sprintf(hline, "CRVAL1 = 0.");                      stradd(header, hline);
   sprintf(hline, "CRVAL2 = 0.");                      stradd(header, hline);
   sprintf(hline, "CRPIX1 = %.2f", (naxis1 + 1.)/2.);  stradd(header, hline);
   sprintf(hline, "CRPIX2 = %.2f", (naxis2 + 1.)/2.);  stradd(header, hline);
   sprintf(hline, "CROTA2 = 0.");                      stradd(header, hline);
   sprintf(hline, "END");                              stradd(header, hline);

   wcs = wcsinit(header);

   if(wcs == (struct WorldCoor *)NULL)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"wcsinit() failed for fake header.\"]\n");
      exit(1);
   }

   return wcs;
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



/***********************************************/
/*                                             */
/*  Determine whether a pixel is part of the   */
/*  compass 'rose' lines                       */
/*                                             */
/***********************************************/

int covered(int i, int j, 
	    double x1, double y1, double x2, double y2, 
	    double len2, int lwidth, int *color)
{
   double u, x, y, dist2;

   static double umin = 99999999.;

   *color = 0;

   u = (((double)i-x1)*(x2-x1) + ((double)j-y1)*(y2-y1)) / len2;

   if(u < 0. || u > 1.)
      return 0;

   x = x1 + u * (x2 - x1);
   y = y1 + u * (y2 - y1);

   dist2 = (i-x)*(i-x) + (j-y)*(j-y);

   if(dist2 > lwidth)
      *color = 255;

   if(dist2 > 3*lwidth)
      return 0;

   return 1;
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


/***********************************************/
/*                                             */
/*  Parse the HDU number out of the file name  */
/*                                             */
/***********************************************/

int getHDU(char *file)
{
   int   hdu;
   char *ptr;

   hdu = 0;

   ptr = file + strlen(file) - 1;

   if(*ptr != ']')
      return hdu;

   *ptr = '\0';

   while(1)
   {
      if(ptr <= file)
	 break;
      
      if(*ptr == '[')
      {
	 *ptr = '\0';

	 ++ptr;

	 hdu = atoi(ptr);

	 break;
      }

      --ptr;
   }

   return hdu;
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



/***********************************/
/*                                 */
/*  Color tables                   */
/*                                 */
/***********************************/


void createColorTable(int itable)
{
    int    i, j, nseg;
    int   *dn, *red_tbl, *grn_tbl, *blue_tbl;
    double rscale, gscale, bscale, tmp; 

    int dn0[] = {  0, 255};
    
    int dn2[] = {  0,  17,  34,  51,  68,  85, 102, 119, 
                 136, 153, 170, 187, 204, 221, 238, 255};
    
    int dn3[] = {  0,  25,  40,  55,  70,  90, 110, 130,
                 150, 175, 200, 225, 255};
     
    int dn4[] = {  0,  23,  46,  70,  93, 116, 
		 139, 162, 185, 209, 232, 255};
    
    int red_tbl0[]   = {  0, 255};
    int grn_tbl0[]   = {  0, 255};
    int blue_tbl0[]  = {  0, 255};
        
    int red_tbl1[]   = {255,   0};
    int grn_tbl1[]   = {255,   0};
    int blue_tbl1[]  = {255,   0};
    
    int red_tbl2[]   = {  0,  98, 180, 255, 246, 220, 181,  88,
                          0,   0,   0,   0,   0,  85, 170, 255};

    int grn_tbl2[]   = {  0,   0,   0,   0,  88, 181, 220, 245, 
                        255, 235, 180,  98,   0, 128, 192, 255};

    int blue_tbl2[]  = {  0, 180,  98,   0,   0,   0,   0,   0,
                          0,  98, 180, 235, 255, 255, 255, 255};
        

    int red_tbl3[]   = {  0,   0,   0,   0,   0,   0,   0, 150,
                        225, 255, 255, 196, 255}; 

    int grn_tbl3[]   = {  0,   0,   0, 125, 196, 226, 255, 255,
                        225, 150,   0,   0, 255}; 

    int blue_tbl3[]  = {  0, 150, 255, 225, 196, 125,   0,   0,
                          0,   0,   0, 196, 255}; 


    int red_tbl4[]   = {  0,   0,   0,   0,   0,   0,   0,  20,
                        135, 210, 240, 255, 240, 255, 255, 255}; 

    int grn_tbl4[]   = {  0,   0,  20,  63, 112, 162, 210, 255,
                        240, 205, 135,  20,   0,  85, 170, 255}; 

    int blue_tbl4[]  = {  0,  95, 175, 250, 255, 175,  90,  30, 
                          0,   0,   0,   0,   0,  85, 170, 255}; 


    int red_tbl5[]   = {255, 255, 255, 240, 255, 240, 210, 135,
                         20,   0,   0,   0,   0,   0,   0,   0};

    int grn_tbl5[]   = {255, 170,  85,   0,  20, 135, 205, 240,
                        255, 210, 162, 112,  63,  20,   0,   0};

    int blue_tbl5[]  = {255, 170,  85,   0,   0,   0,   0,   0,
                         30,  90, 175, 255, 250, 175,  95,   0};


    int red_tbl6[]   = {  0,   0,   0,   0,   0,   0,   0,   0,
                          0,   0,   0,  20, 210, 240, 240, 255};

    int grn_tbl6[]   = {  0,   0,   0,  10,  20,  41,  63,  83,
                        112, 137, 162, 255, 205, 135,   0, 255};

    int blue_tbl6[]  = {  0,  47,  95, 140, 175, 212, 250, 252,
                        255, 212, 175,  30,   0,   0,   0, 255};


    int red_tbl7[]   = {255, 255, 255, 255, 255, 255, 255, 255,
                        224, 192, 160, 128,  96,  64,  32,   0};

    int grn_tbl7[]   = {  0,  32,  64,  96, 128, 160, 192, 224,
                        224, 192, 160, 128,  96,  64,  32,   0};

    int blue_tbl7[]  = {  0,  32,  64,  96, 128, 160, 192, 224,
                        255, 255, 255, 255, 255, 255, 255, 255};


    int red_tbl8[]   = {  0, 255};
    int grn_tbl8[]   = {  0,   0};
    int blue_tbl8[]  = {  0,   0};
        
    int red_tbl9[]   = {  0,   0};
    int grn_tbl9[]   = {  0, 255};
    int blue_tbl9[]  = {  0,   0};
        
    int red_tbl10[]  = {  0,   0};
    int grn_tbl10[]  = {  0,   0};
    int blue_tbl10[] = {  0, 255};
        

    int red_tbl11[]  = {    0,   0,   0,  20, 135, 210,
			  240, 255, 240, 255, 255, 255}; 

    int grn_tbl11[]  = {    0,  85, 170, 255, 240, 205, 
			  135,  20,   0,  85, 170, 255}; 

    int blue_tbl11[] = {  255, 175,  90,  30,   0,   0,   
			    0,   0,   0,  85, 170, 255}; 


   switch(itable)
   {
      case 0:
	 dn       = dn0;
	 nseg     = 2;
	 red_tbl  = red_tbl0;
	 grn_tbl  = grn_tbl0;
	 blue_tbl = blue_tbl0;
	 break;
	  
      case 1:
	 dn       = dn0;
	 nseg     = 2;
	 red_tbl  = red_tbl1;
	 grn_tbl  = grn_tbl1;
	 blue_tbl = blue_tbl1;
	 break;
     
      case 2:
	 dn       = dn2;
	 nseg     = 16;
	 red_tbl  = red_tbl2;
	 grn_tbl  = grn_tbl2;
	 blue_tbl = blue_tbl2;
	 break;
      
      case 3:
	 dn       = dn3;
	 nseg     = 13;
	 red_tbl  = red_tbl3;
	 grn_tbl  = grn_tbl3;
	 blue_tbl = blue_tbl3;
	 break;

      case 4:
	 dn       = dn2;
	 nseg     = 16;
	 red_tbl  = red_tbl4;
	 grn_tbl  = grn_tbl4;
	 blue_tbl = blue_tbl4;
	 break;
      
      case 5:
	 dn       = dn2;
	 nseg     = 16;
	 red_tbl  = red_tbl5;
	 grn_tbl  = grn_tbl5;
	 blue_tbl = blue_tbl5;
	 break;
      
      case 6:
	 dn       = dn2;
	 nseg     = 16;
	 red_tbl  = red_tbl6;
	 grn_tbl  = grn_tbl6;
	 blue_tbl = blue_tbl6;
	 break;
      
      case 7:
	 dn       = dn2;
	 nseg     = 16;
	 red_tbl  = red_tbl7;
	 grn_tbl  = grn_tbl7;
	 blue_tbl = blue_tbl7;
	 break;
      
      case 8:
	 dn       = dn0;
	 nseg     = 2;
	 red_tbl  = red_tbl8;
	 grn_tbl  = grn_tbl8;
	 blue_tbl = blue_tbl8;
	 break;

      case 9:
	 dn       = dn0;
	 nseg     = 2;
	 red_tbl  = red_tbl9;
	 grn_tbl  = grn_tbl9;
	 blue_tbl = blue_tbl9;
	 break;

      case 10:
	 dn       = dn0;
	 nseg     = 2;
	 red_tbl  = red_tbl10;
	 grn_tbl  = grn_tbl10;
	 blue_tbl = blue_tbl10;
	 break;

      case 11:
	 dn       = dn4;
	 nseg     = 12;
	 red_tbl  = red_tbl11;
	 grn_tbl  = grn_tbl11;
	 blue_tbl = blue_tbl11;
	 break;
      
      default:
	 dn       = dn0;
	 nseg     = 2;
	 red_tbl  = red_tbl0;
	 grn_tbl  = grn_tbl0;
	 blue_tbl = blue_tbl0;
      break;
   }

   for (j=1; j<nseg; ++j)
   {
      rscale = (double)(( red_tbl[j] -  red_tbl[j-1])/ (dn[j]-dn[j-1]));
      gscale = (double)(( grn_tbl[j] -  grn_tbl[j-1])/ (dn[j]-dn[j-1]));
      bscale = (double)((blue_tbl[j] - blue_tbl[j-1])/ (dn[j]-dn[j-1]));

      for (i=dn[j-1]; i<=dn[j]; i++) 
      {
	 tmp = rscale * (i-dn[j-1]) + red_tbl[j-1];

	 if (tmp > 255.) tmp = 255.;
	 if (tmp <   0.) tmp =   0.;

	 color_table[i][0] = (int)(tmp);

	 tmp = gscale*(i-dn[j-1]) + grn_tbl[j-1];

	 if (tmp > 255.) tmp = 255.;
	 if (tmp <   0.) tmp =   0.;

	 color_table[i][1] = (int)(tmp);

	 tmp = bscale*(i-dn[j-1]) + blue_tbl[j-1];

	 if (tmp > 255.) tmp = 255.;
	 if (tmp <   0.) tmp =   0.;

	 color_table[i][2] = (int)(tmp);
      }
   }

   if(itable == 11)
   {
      color_table[0][0] = 0;
      color_table[0][1] = 0;
      color_table[0][2] = 0;
   }
}


/***********************************/
/*                                 */
/*  Parse a range string           */
/*                                 */
/***********************************/

void parseRange(char const *str, char const *kind, double *val, double *extra, int *type) {
   char const *ptr;
   char *end;
   double sign = 1.0;

   ptr = str;
   while (isspace(*ptr)) ++ptr;
   if (*ptr == '-' || *ptr == '+') {
      sign = (*ptr == '-') ? -1.0 : 1.0;
      ++ptr;
   }
   while (isspace(*ptr)) ++ptr;
   errno = 0;
   *val = strtod(ptr, &end) * sign;
   if (errno != 0) {
      printf("[struct stat=\"ERROR\", msg=\"leading numeric term in %s '%s' "
             "cannot be converted to a finite floating point number\"]\n",
             kind, str);
      fflush(stdout);
      exit(1);
   }
   if (ptr == end) {
      /* range string didn't start with a number, check for keywords */
      if (strncmp(ptr, "min", 3) == 0) {
         *val = 0.0;
      } else if (strncmp(ptr, "max", 3) == 0) {
         *val = 100.0;
      } else if (strncmp(ptr, "med", 3) == 0) {
         *val = 50.0;
      } else {
         printf("[struct stat=\"ERROR\", msg=\"'%s' is not a valid %s\"]\n",
                str, kind);
         fflush(stdout);
         exit(1);
      }
      *type = PERCENTILE;
      ptr += 3;
   } else {
      /* range string began with a number, look for a type spec */
      ptr = end;
      while (isspace(*ptr)) ++ptr;
      switch (*ptr) {
         case '%':
            if (*val < 0.0) {
               printf("[struct stat=\"ERROR\", msg=\"'%s': negative "
                      "percentile %s\"]\n", str, kind);
               fflush(stdout);
               exit(1);
            }
            if (*val > 100.0) {
               printf("[struct stat=\"ERROR\", msg=\"'%s': percentile %s "
                      "larger than 100\"]\n", str, kind);
               fflush(stdout);
               exit(1);
            }
            *type = PERCENTILE;
            ++ptr;
            break;
         case 's':
         case 'S':
            *type = SIGMA;
            ++ptr;
            break;
         case '+':
         case '-':
         case '\0':
            *type = VALUE;
            break;
         default:
            printf("[struct stat=\"ERROR\", msg=\"'%s' is not a valid %s\"]\n",
                   str, kind);
            fflush(stdout);
            exit(1);
      }
   }

   /* look for a trailing numeric term */
   *extra = 0.0;
   while (isspace(*ptr)) ++ptr;
   if (*ptr == '-' || *ptr == '+') {
      sign = (*ptr == '-') ? -1.0 : 1.0;
      ++ptr;
      while (isspace(*ptr)) ++ptr;
      *extra = strtod(ptr, &end) * sign;
      if (errno != 0) {
         printf("[struct stat=\"ERROR\", msg=\"extra numeric term in %s '%s' "
                "cannot be converted to a finite floating point number\"]\n",
                kind, str);
         fflush(stdout);
         exit(1);
      }
      if (ptr == end) {
         printf("[struct stat=\"ERROR\", msg=\"%s '%s' contains trailing "
                "junk\"]\n", kind, str);
         fflush(stdout);
         exit(1);
      }
      ptr = end;
   }
   while (isspace(*ptr)) ++ptr;
   if (*ptr != '\0') {
      printf("[struct stat=\"ERROR\", msg=\"%s '%s' contains trailing "
             "junk\"]\n", kind, str);
      fflush(stdout);
      exit(1);
   }
}


/***********************************/
/*                                 */
/*  Histogram percentile ranges    */
/*                                 */
/***********************************/

#define NBIN 20000

int     nbin;

int     hist    [NBIN];
double  chist   [NBIN];
double  datalev [NBIN];
double  gausslev[NBIN];

int     npix;
double  delta, rmin, rmax;
 
void getRange(fitsfile *fptr, char *minstr, char *maxstr,
              double *rangemin, double *rangemax, 
	      int type, char *betastr, double *rangebeta, double *dataval,
	      int imnaxis1, int imnaxis2)
{
   int     i, j, k, mintype, maxtype, betatype, nullcnt, status;
   long    fpixel[4], nelements;
   double  d, diff, minval, maxval, betaval;
   double  lev16, lev50, lev84, sigma;
   double  minextra, maxextra, betaextra;
   double *data;
   char    valstr[1024];
   char   *end;
   char   *ptr;

   double  glow, ghigh, gaussval, gaussstep;
   double  dlow, dhigh;
   double  gaussmin, gaussmax;


   nbin = NBIN - 1;

   /* MIN/MAX: Determine what type of  */
   /* range string we are dealing with */

   parseRange(minstr, "display min", &minval, &minextra, &mintype);
   parseRange(maxstr, "display max", &maxval, &maxextra, &maxtype);
   if (type == ASINH) {
      parseRange(betastr, "beta value", &betaval, &betaextra, &betatype);
   }

   /* If we don't have to generate the image */
   /* histogram, get out now                 */

   *rangemin  = minval + minextra;
   *rangemax  = maxval + maxextra;
   *rangebeta = betaval + betaextra;

   if(mintype  == VALUE
   && maxtype  == VALUE
   && betatype == VALUE
   && (type == POWER || type == ASINH))
      return;


   /* Find the min, max values in the image */

   npix = 0;

   rmin =  1.0e10;
   rmax = -1.0e10;

   fpixel[0] = 1;
   fpixel[1] = 1;
   fpixel[2] = 1;
   fpixel[3] = 1;

   nelements = imnaxis1;

   data = (double *)malloc(nelements * sizeof(double));

   status = 0;

   for(j=0; j<imnaxis2; ++j) 
   {
      if(fits_read_pix(fptr, TDOUBLE, fpixel, nelements, NULL,
		       data, &nullcnt, &status))
	 printFitsError(status);
      
      for(i=0; i<imnaxis1; ++i) 
      {
	 if(!mNaN(data[i])) 
	 {
	    if (data[i] > rmax) rmax = data[i];
	    if (data[i] < rmin) rmin = data[i];

	    ++npix;
	 }
      }

      ++fpixel[1];
   }

   diff = rmax - rmin;

   if(debug)
   {
      printf("DEBUG> getRange(): rmin = %-g, rmax = %-g (diff = %-g)\n",
	 rmin, rmax, diff);
      fflush(stdout);
   }


   /* Populate the histogram */

   for(i=0; i<nbin+1; i++) 
      hist[i] = 0;
   
   fpixel[1] = 1;

   for(j=0; j<imnaxis2; ++j) 
   {
      if(fits_read_pix(fptr, TDOUBLE, fpixel, nelements, NULL,
		       data, &nullcnt, &status))
	 printFitsError(status);

      for(i=0; i<imnaxis1; ++i) 
      {
	 if(!mNaN(data[i])) 
	 {
	    d = floor(nbin*(data[i]-rmin)/diff);
	    k = (int)d;

	    if(k > nbin-1)
               k = nbin-1;

	    if(k < 0)
               k = 0;

	    ++hist[k];
	 }
      }

      ++fpixel[1];
   }


   /* Compute the cumulative histogram      */
   /* and the histogram bin edge boundaries */

   delta = diff/nbin;

   chist[0] = 0;

   for(i=1; i<=nbin; ++i)
      chist[i] = chist[i-1] + hist[i-1];


   /* Find the data value associated    */
   /* with the minimum percentile/sigma */

   if(mintype == PERCENTILE)
      *rangemin = percentileLevel(minval) + minextra;

   else if(mintype == SIGMA)
   {
      lev16 = percentileLevel(16.);
      lev50 = percentileLevel(50.);
      lev84 = percentileLevel(84.);

      sigma = (lev84 - lev16) / 2.;

      *rangemin = lev50 + minval * sigma + minextra;
   }


   /* Find the data value associated    */
   /* with the max percentile/sigma     */

   if(maxtype == PERCENTILE)
      *rangemax = percentileLevel(maxval) + maxextra;
   
   else if(maxtype == SIGMA)
   {
      lev16 = percentileLevel(16.);
      lev50 = percentileLevel(50.);
      lev84 = percentileLevel(84.);

      sigma = (lev84 - lev16) / 2.;

      *rangemax = lev50 + maxval * sigma + maxextra;
   }


   /* Find the data value associated    */
   /* with the beta percentile/sigma    */

   if(type == ASINH)
   {
      if(betatype == PERCENTILE)
	 *rangebeta = percentileLevel(betaval) + betaextra;

      else if(betatype == SIGMA)
      {
	 lev16 = percentileLevel(16.);
	 lev50 = percentileLevel(50.);
	 lev84 = percentileLevel(84.);

	 sigma = (lev84 - lev16) / 2.;

	 *rangebeta = lev50 + betaval * sigma + betaextra;
      }
   }

   if(*rangemin == *rangemax)
      *rangemax = *rangemin + 1.;
   
   if(debug)
   {
      if(type == ASINH)
	 printf("DEBUG> getRange(): range = %-g to %-g (beta = %-g)\n", 
	    *rangemin, *rangemax, *rangebeta);
      else
	 printf("DEBUG> getRange(): range = %-g to %-g\n", 
	    *rangemin, *rangemax);
   }


   /* Find the data levels associated with */
   /* a Guassian histogram stretch         */

   if(type == GAUSSIAN
   || type == GAUSSIANLOG)
   {
      for(i=0; i<nbin; ++i)
      {
	 datalev [i] = rmin+delta*i;
	 gausslev[i] = snpinv(chist[i+1]/npix);
      }


      /* Find the guassian levels associated */
      /* with the range min, max             */

      for(i=0; i<nbin-1; ++i)
      {
	 if(datalev[i] > *rangemin)
	 {
	    gaussmin = gausslev[i];
	    break;
	 }
      }

      gaussmax = gausslev[nbin-2];

      for(i=0; i<nbin-1; ++i)
      {
	 if(datalev[i] > *rangemax)
	 {
	    gaussmax = gausslev[i];
	    break;
	 }
      }

      if(type == GAUSSIAN)
      {
	 gaussstep = (gaussmax - gaussmin)/255.;

	 for(j=0; j<256; ++j)
	 {
	    gaussval = gaussmin + gaussstep * j;

	    for(i=1; i<nbin-1; ++i)
	       if(gausslev[i] >= gaussval)
		  break;

	    glow  = gausslev[i-1];
	    ghigh = gausslev[i];

	    dlow  = datalev [i-1];
	    dhigh = datalev [i];

	    if(glow == ghigh)
	       dataval[j] = dlow;
	    else
	       dataval[j] = dlow
			  + (gaussval - glow) * (dhigh - dlow) / (ghigh - glow);
	 }
      }
      else
      {
	 gaussstep = log10(gaussmax - gaussmin)/255.;

	 for(j=0; j<256; ++j)
	 {
	    gaussval = gaussmax - pow(10., gaussstep * (256. - j));

	    for(i=1; i<nbin-1; ++i)
	       if(gausslev[i] >= gaussval)
		  break;

	    glow  = gausslev[i-1];
	    ghigh = gausslev[i];

	    dlow  = datalev [i-1];
	    dhigh = datalev [i];

	    if(glow == ghigh)
	       dataval[j] = dlow;
	    else
	       dataval[j] = dlow
			  + (gaussval - glow) * (dhigh - dlow) / (ghigh - glow);
	 }
      }
   }

   return;
}



/***********************************/
/* Find the data values associated */
/* with the desired percentile     */
/***********************************/

double percentileLevel(double percentile)
{
   int    i, count;
   double percent, maxpercent, minpercent;
   double fraction, value;

   if(percentile <=   0) return rmin;
   if(percentile >= 100) return rmax;

   percent = 0.01 * percentile;

   count = (int)(npix*percent);

   i = 1;
   while(i < nbin+1 && chist[i] < count)
      ++i;
   
   minpercent = (double)chist[i-1] / npix;
   maxpercent = (double)chist[i]   / npix;

   fraction = (percent - minpercent) / (maxpercent - minpercent);

   value = rmin + (i-1+fraction) * delta;

   if(debug)
   {
      printf("DEBUG> percentileLevel(%-g):\n", percentile);
      printf("DEBUG> percent    = %-g -> count = %d -> bin %d\n",
	 percent, count, i);
      printf("DEBUG> minpercent = %-g\n", minpercent);
      printf("DEBUG> maxpercent = %-g\n", maxpercent);
      printf("DEBUG> fraction   = %-g\n", fraction);
      printf("DEBUG> rmin       = %-g\n", rmin);
      printf("DEBUG> delta      = %-g\n", delta);
      printf("DEBUG> value      = %-g\n\n", value);
      fflush(stdout);
   }

   return(value);
}



/***********************************/
/* Find the percentile level       */
/* associated with a data value    */
/***********************************/

double valuePercentile(double value)
{
   int    i;
   double maxpercent, minpercent;
   double ival, fraction, percentile;

   if(value <= rmin) return   0.0;
   if(value >= rmax) return 100.0;

   ival = (value - rmin) / delta;

   i = ival;

   fraction = ival - i;

   minpercent = (double)chist[i]     / npix;
   maxpercent = (double)chist[i+1]   / npix;

   percentile = 100. *(minpercent * (1. - fraction) + maxpercent * fraction);

   if(debug)
   {
      printf("DEBUG> valuePercentile(%-g):\n", value);
      printf("DEBUG> rmin       = %-g\n", rmin);
      printf("DEBUG> delta      = %-g\n", delta);
      printf("DEBUG> value      = %-g -> bin %d (fraction %-g)\n",
	 value, i, fraction);
      printf("DEBUG> minpercent = %-g\n", minpercent);
      printf("DEBUG> maxpercent = %-g\n", maxpercent);
      printf("DEBUG> percentile = %-g\n\n", percentile);
      fflush(stdout);
   }

   return(percentile);
}




/*************************************/
/* Turn a FITS header block into a   */
/* comment block (newline delimited) */
/*************************************/

int make_comment(char *header, char *comment)
{
   int   i, j, count;
   char *ptr, *end;
   char  line[81];

   ptr = header;
   end = ptr + strlen(header);

   strcpy(comment, "");

   count = 0;

   while(ptr < end)
   {
      for(i=0; i<80; ++i)
      {
	 line[i] = *(ptr+i);

	 if(ptr+i >= end)
	    break;
      }

      line[80] = '\0';

      if(strncmp(line, "NAXIS1", 6) == 0)
	 sprintf(line, "NAXIS1  = %d", naxis1);

      if(strncmp(line, "NAXIS2", 6) == 0)
	 sprintf(line, "NAXIS2  = %d", naxis2);

      if(strncmp(line, "CRPIX1", 6) == 0)
	 sprintf(line, "CRPIX1  = %15.10f", crpix1);

      if(strncmp(line, "CRPIX2", 6) == 0)
	 sprintf(line, "CRPIX2  = %15.10f", crpix2);

      /*
      if(strncmp(line, "CDELT1", 6) == 0)
	 sprintf(line, "CDELT1  = %15.10f", cdelt1);

      if(strncmp(line, "CDELT2", 6) == 0)
	 sprintf(line, "CDELT2  = %15.10f", cdelt2);

      if(strncmp(line, "CROTA2", 6) == 0)
	 sprintf(line, "CROTA2  = %15.10f", crota2);

      if(strncmp(line, "CD1_1",  5) == 0)
	 sprintf(line, "CD1_1   = %15.10f", cd11);

      if(strncmp(line, "CD1_2",  5) == 0)
	 sprintf(line, "CD1_2   = %15.10f", cd12);

      if(strncmp(line, "CD2_1",  5) == 0)
	 sprintf(line, "CD2_1   = %15.10f", cd21);

      if(strncmp(line, "CD2_2",  5) == 0)
	 sprintf(line, "CD2_2   = %15.10f", cd22);
      */

      for(j=i; j>=0; --j)
      {
	 if(line[j] == ' '
	 || line[j] == '\0')
	    line[j] = '\0';
	 else
	    break;
      }
      
      strcat(comment, line);
      strcat(comment, "\n");

      count += strlen(line) + 1;

      if(count >= 65000)
      {
	 strcat(comment, "END\n");
	 break;
      }

      ptr += 80;
   }

   return 0;
}



/*********************************************************************/
/*                                                                   */
/* Wrapper around ERFINV to turn it into the inverse of the          */
/* standard normal probability function.                             */
/*                                                                   */
/*********************************************************************/

double snpinv(double p)
{
   if(p > 0.5)
      return( sqrt(2) * erfinv(2.*p-1.0));
   else
      return(-sqrt(2) * erfinv(1.0-2.*p));
}



/*********************************************************************/
/*                                                                   */
/* ERFINV: Evaluation of the inverse error function                  */
/*                                                                   */
/*        For 0 <= p <= 1,  w = erfinv(p) where erf(w) = p.          */
/*        If either inequality on p is violated then erfinv(p)       */
/*        is set to a negative value.                                */
/*                                                                   */
/*    reference. mathematics of computation,oct.1976,pp.827-830.     */
/*                 j.m.blair,c.a.edwards,j.h.johnson                 */
/*                                                                   */
/*********************************************************************/

double erfinv (double p)
{
   double q, t, v, v1, s, retval;

   /*  c2 = ln(1.e-100)  */

   double c  =  0.5625;
   double c1 =  0.87890625;
   double c2 = -0.2302585092994046e+03;

   double a[6]  = { 0.1400216916161353e+03, -0.7204275515686407e+03,
                    0.1296708621660511e+04, -0.9697932901514031e+03,
                    0.2762427049269425e+03, -0.2012940180552054e+02 };

   double b[6]  = { 0.1291046303114685e+03, -0.7312308064260973e+03,
                    0.1494970492915789e+04, -0.1337793793683419e+04,
                    0.5033747142783567e+03, -0.6220205554529216e+02 };

   double a1[7] = {-0.1690478046781745e+00,  0.3524374318100228e+01,
                   -0.2698143370550352e+02,  0.9340783041018743e+02,
                   -0.1455364428646732e+03,  0.8805852004723659e+02,
                   -0.1349018591231947e+02 };
     
   double b1[7] = {-0.1203221171313429e+00,  0.2684812231556632e+01,
                   -0.2242485268704865e+02,  0.8723495028643494e+02,
                   -0.1604352408444319e+03,  0.1259117982101525e+03,
                   -0.3184861786248824e+02 };

   double a2[9] = { 0.3100808562552958e-04,  0.4097487603011940e-02,
                    0.1214902662897276e+00,  0.1109167694639028e+01,
                    0.3228379855663924e+01,  0.2881691815651599e+01,
                    0.2047972087262996e+01,  0.8545922081972148e+00,
                    0.3551095884622383e-02 };

   double b2[8] = { 0.3100809298564522e-04,  0.4097528678663915e-02,
                    0.1215907800748757e+00,  0.1118627167631696e+01,
                    0.3432363984305290e+01,  0.4140284677116202e+01,
                    0.4119797271272204e+01,  0.2162961962641435e+01 };

   double a3[9] = { 0.3205405422062050e-08,  0.1899479322632128e-05,
                    0.2814223189858532e-03,  0.1370504879067817e-01,
                    0.2268143542005976e+00,  0.1098421959892340e+01,
                    0.6791143397056208e+00, -0.8343341891677210e+00,
                    0.3421951267240343e+00 };

   double b3[6] = { 0.3205405053282398e-08,  0.1899480592260143e-05,
                    0.2814349691098940e-03,  0.1371092249602266e-01,
                    0.2275172815174473e+00,  0.1125348514036959e+01 };


   /* Error conditions */

   q = 1.0 - p;

   if(p < 0.0 || q < 0.0)
     return(-1.e99);


   /* Extremely large value */

   if (q == 0.0)
      return(1.e99);


   /* p between 0 and 0.75 */

   if(p <= 0.75)
   {
      v = p * p - c;

      t = p *  (((((a[5] * v + a[4]) * v + a[3]) * v + a[2]) * v
			     + a[1]) * v + a[0]);

      s = (((((v + b[5]) * v + b[4]) * v + b[3]) * v + b[2]) * v
			     + b[1]) * v + b[0];
    
      retval = t/s;
    
      return(retval);
   }


   /* p between 0.75 and 0.9375 */

   if (p <= 0.9375)
   {
      v = p*p - c1;

      t = p *  ((((((a1[6] * v + a1[5]) * v + a1[4]) * v + a1[3]) * v
                               + a1[2]) * v + a1[1]) * v + a1[0]);

      s = ((((((v + b1[6]) * v + b1[5]) * v + b1[4]) * v + b1[3]) * v
                  + b1[2]) * v + b1[1]) * v + b1[0];
    
      retval = t/s;
    
      return(retval);
   }


   /* q between 1.e-100 and 0.0625 */

   v1 = log(q);

   v  = 1.0/sqrt(-v1);

   if (v1 < c2)
   {
      t = (((((((a2[8] * v + a2[7]) * v + a2[6]) * v + a2[5]) * v + a2[4]) * v
			   + a2[3]) * v + a2[2]) * v + a2[1]) * v + a2[0];

      s = v *  ((((((((  v + b2[7]) * v + b2[6]) * v + b2[5]) * v + b2[4]) * v
			   + b2[3]) * v + b2[2]) * v + b2[1]) * v + b2[0]);
  
      retval = t/s;
    
      return(retval);
   }


   /* q between 1.e-10000 and 1.e-100 */

   t = (((((((a3[8] * v + a3[7]) * v + a3[6]) * v + a3[5]) * v + a3[4]) * v
                        + a3[3]) * v + a3[2]) * v + a3[1]) * v + a3[0];

   s = v *    ((((((  v + b3[5]) * v + b3[4]) * v + b3[3]) * v + b3[2]) * v
                        + b3[1]) * v + b3[0]);
   retval = t/s;

   return retval;
}
