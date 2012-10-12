/* Module: mMakeHdr.c

Version  Developer        Date     Change
-------  ---------------  -------  -----------------------
2.6      John Good        20Aug07  Add 'table of tables' capability
2.5      John Good        07Aug07  Added check for "allsky" (large area) data
2.4      John Good        09Jul06  Exit properly when a bad WCS encountered
2.3      John Good        22Feb06  Removed extra (repeated) header lines
2.2      John Good        17Nov05  Added flag (-p) to externally set pixel scale
2.1      John Good        30Nov04  Forgot to check for pixel scale when
				   using four corners (or lat,lon).
2.0      John Good        16Aug04  Added code to alternately check:
				   four corners (equatorial) or four corners
				   (arbitrary with system in header) or
				   just a set of ra, dec or just a set of
				   lon, lat (with system in header)
1.9      John Good        10Aug04  Added four corners of region to output
1.8      John Good        09Jan04  Fixed realloc() bug for 'lats'
1.7      John Good        25Nov03  Added extern optarg references
1.6      John Good        01Oct03  Add check for naxis1, naxis2 columns
				   in addition to ns, nl
1.5      John Good        25Aug03  Added status file processing
1.4      John Good        27Jun03  Added a few comments for clarity
1.3      John Good        09Apr03  Removed unused variable offscl
1.2      John Good        22Mar03  Renamed wcsCheck to checkWCS
				   for consistency.  Checking system
				   and equinox strings on command line
				   for validity
1.1      John Good        13Mar03  Added WCS header check and
                                   modified command-line processing
				   to use getopt() library.  Check for 
                                   missing/invalid images.tbl.  Check
				   for valid equinox and propogate to
				   output header.
1.0      John Good        29Jan03  Baseline code

*/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <ctype.h>
#include <math.h>
#include <mtbl.h>
#include <fitsio.h>
#include <coord.h>
#include <wcs.h>
#include <boundaries.h>

#include "montage.h"

#define MAXSTR   4096
#define MAXFILES   16
#define MAXCOORD 4096

#define UNKNOWN     0
#define FOURCORNERS 1
#define WCS         2
#define LONLAT      3


struct WorldCoor *outwcs;

extern char *optarg;
extern int optind, opterr;

extern int getopt(int argc, char *const *argv, const char *options);

int debugCheck  (char *debugStr);
int checkWCS    (struct WorldCoor *wcs, int action);
int stradd      (char *header, char *card);
int readTemplate(char *filename);

int    debugLevel;


/* Basic image WCS information    */
/* (from the FITS header and as   */
/* returned from the WCS library) */

struct ImgInfo
{
   struct WorldCoor *wcs;
   int               sys;
   int               equinox;
   double            epoch;
   char              ctype1[16];
   char              ctype2[16];
   int               naxis1;
   int               naxis2;
   double            crpix1;
   double            crpix2;
   double            crval1;
   double            crval2;
   double            cdelt1;
   double            cdelt2;
   double            crota2;
}
   input;



/*******************************************************************/
/*                                                                 */
/*  mMakeHdr                                                       */
/*                                                                 */
/*  Read through a table of image metadata and find the 'best'     */
/*  header for a mosaic of the set of images.                      */
/*                                                                 */
/*******************************************************************/

int main(int argc, char **argv)
{
   int     i, c, stat, ncols, nimages, ntables, maxfiles;
   int     naxis1, naxis2, northAligned, datatype;

   int     sys, ifiles, pad, isPercentage;
   double  equinox, val, pixelScale;

   int     itable;

   int     ictype1;
   int     ictype2;
   int     iequinox;
   int     iepoch;
   int     inl;
   int     ins;
   int     icrval1;
   int     icrval2;
   int     icrpix1;
   int     icrpix2;
   int     icdelt1;
   int     icdelt2;
   int     icrota2;

   int     ilon1, ilat1;
   int     ilon2, ilat2;
   int     ilon3, ilat3;
   int     ilon4, ilat4;

   char    tblfile  [MAXSTR];
   char    template [MAXSTR];
   char    epochStr [MAXSTR];
   char    csysStr  [MAXSTR];

   double  xpos, ypos;
   double  lon, lat;

   double  x, y, z;
   double  xmin, ymin, zmin;
   double  xmax, ymax, zmax;

   double  minCdelt = 360.;

   char    header[1600];
   char    temp[80];

   char   *end;

   char   *keyval;
   char    refsys;

   double *lons, *lats;
   int     maxcoords, ncoords;

   struct bndInfo *box = (struct bndInfo *)NULL;

   FILE   *fout;

   char  **fnames;

   double  dtr;

   dtr = atan(1.)/45.;

   maxfiles = MAXFILES;

   fnames = (char **)malloc(maxfiles * sizeof(char *));
   
   for(i=0; i<maxfiles; ++i)
      fnames[i] = (char *)malloc(MAXSTR * sizeof(char));

   lons = (double *)malloc(MAXCOORD * sizeof(double));
   lats = (double *)malloc(MAXCOORD * sizeof(double));

   if(lats == (double *)NULL)
   {
      printf("[struct stat=\"ERROR\", msg=\"Memory allocation failure.\"]\n");
      fflush(stdout);

      exit(0);
   }

   maxcoords = MAXCOORD;
   ncoords   = 0;


   /***************************************/
   /* Process the command-line parameters */
   /***************************************/

   debugLevel   = 0;
   northAligned = 0;
   opterr       = 0;
   pad          = 0;
   isPercentage = 0;

   pixelScale   = 0;

   fstatus = stdout;

   while ((c = getopt(argc, argv, "nd:e:s:p:")) != EOF) 
   {
      switch (c) 
      {
         case 'n':
	    northAligned = 1;
            break;

         case 'd':
            debugLevel = debugCheck(optarg);
            break;

         case 'e':
            pad = atoi(optarg);

	    if(pad < 0)
	    {
               printf("[struct stat=\"ERROR\", msg=\"Invalid pad string: %s\"]\n",
                  optarg);
               exit(1);
	    }

            if(strstr(optarg, "%"))
	       isPercentage = 1;

            break;

         case 'p':
            pixelScale = atof(optarg);

	    if(pixelScale <=0)
	    {
               printf("[struct stat=\"ERROR\", msg=\"Invalid pixel scale string: %s\"]\n",
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

         default:
	    printf("[struct stat=\"ERROR\", msg=\"Usage: %s [-d level] [-s statusfile] [-p(ixel-scale) cdelt] [-e edgepixels] [-n] images.tbl template.hdr [system [equinox]] (where system = EQUJ|EQUB|ECLJ|ECLB|GAL|SGAL)\"]\n", argv[0]);
	    fflush(stdout);
            exit(1);
            break;
      }
   }

   if (argc - optind < 2) 
   {
      printf("[struct stat=\"ERROR\", msg=\"Usage: %s [-d level] [-s statusfile] [-p(ixel-scale) cdelt] [-e edgepixels] [-n] images.tbl template.hdr [system [equinox]] (where system = EQUJ|EQUB|ECLJ|ECLB|GAL|SGAL)\"]\n", argv[0]);
      fflush(stdout);
      exit(1);
   }

   strcpy(tblfile,  argv[optind]);
   strcpy(template, argv[optind + 1]);

   sys     = EQUJ;
   equinox = 2000.;

   if (argc - optind > 2) 
   {
      if(strcmp(argv[optind + 2], "EQUJ") == 0) sys = EQUJ;
      else if(strcmp(argv[optind + 2], "EQUB") == 0) sys = EQUB;
      else if(strcmp(argv[optind + 2], "ECLJ") == 0) sys = ECLJ;
      else if(strcmp(argv[optind + 2], "ECLB") == 0) sys = ECLB;
      else if(strcmp(argv[optind + 2], "GAL" ) == 0) sys = GAL ;
      else if(strcmp(argv[optind + 2], "SGAL") == 0) sys = SGAL;
      else
      {
	 fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Invalid system string.  Must be EQUJ|EQUB|ECLJ|ECLB|GAL|SGAL\"]\n");
	 fflush(stdout);
	 exit(1);
      }
   }

   if (argc - optind > 3) 
   {
      equinox = strtod(argv[optind + 3], &end);

      if(end < argv[optind + 3] + strlen(argv[optind + 3]))
      {
         fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Equinox string is not a number\"]\n");
         exit(1);
      }
   }
   else if(sys == EQUB || sys == ECLB)
      equinox = 1950.;

   bndSetDebug(debugLevel);

   fout = fopen(template, "w+");

   if(fout == (FILE *)NULL)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Can't open output header file.\"]\n");
      fflush(stdout);
      exit(0);
   }


   /****************************/ 
   /* Open the list table file */
   /* This may be a list of    */
   /* tables, in which case    */
   /* read them in.            */
   /****************************/ 

   ntables = 0;
   nimages = 0;

   ncols = topen(tblfile);

   if(ncols <= 0)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Invalid table file: %s\"]\n",
         tblfile);
      exit(1);
   }

   itable  = tcol("table");

   if(itable < 0)
   {
      strcpy(fnames[0], tblfile);

      ntables = 1;
   }

   else
   {
      while(1)
      {
	 stat = tread();

	 if(stat < 0)
	    break;

	 strcpy(fnames[ntables], tval(itable));

	 ++ntables;

	 if(ntables >= maxfiles)
	 {
	    maxfiles += MAXFILES;

	    fnames = (char **)malloc(maxfiles * sizeof(char *));
	    
	    for(i=maxfiles-MAXFILES; i<maxfiles; ++i)
	       fnames[i] = (char *)malloc(MAXSTR * sizeof(char));
	 }
      }
   }

   tclose();


   /*********************************************/ 
   /* Loop over the set of image metadata files */
   /*********************************************/ 

   for(ifiles=0; ifiles<ntables; ++ifiles)
   {
      strcpy(tblfile, fnames[ifiles]);

      if(debugLevel >= 1)
      {
         printf("Table file %d: [%s]\n", ifiles, tblfile);
         fflush(stdout);
      }

      /**********************************/ 
      /* Open the image list table file */
      /**********************************/ 
      ncols = topen(tblfile);

      if(ncols <= 0)
      {
	 fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Invalid image metadata file: %s\"]\n",
	    tblfile);
	 exit(1);
      }
      

      /* First check to see if we have four equatorial corners */

      icdelt1 = tcol("cdelt1");
      icdelt2 = tcol("cdelt2");
      
      datatype = UNKNOWN;

      ilon1 = tcol("ra1");
      ilon2 = tcol("ra2");
      ilon3 = tcol("ra3");
      ilon4 = tcol("ra4");

      ilat1 = tcol("dec1");
      ilat2 = tcol("dec2");
      ilat3 = tcol("dec3");
      ilat4 = tcol("dec4");

      if(ilon1 >= 0
      && ilon2 >= 0
      && ilon3 >= 0
      && ilon4 >= 0
      && ilat1 >= 0
      && ilat2 >= 0
      && ilat3 >= 0
      && ilat4 >= 0)
	 datatype = FOURCORNERS;


      /* Then check for generic lon, lat corners */

      if(datatype == UNKNOWN)
      {
	 ilon1 = tcol("lon1");
	 ilon2 = tcol("lon2");
	 ilon3 = tcol("lon3");
	 ilon4 = tcol("lon4");

	 ilat1 = tcol("lat1");
	 ilat2 = tcol("lat2");
	 ilat3 = tcol("lat3");
	 ilat4 = tcol("lat4");

	 if(ilon1 >= 0
	 && ilon2 >= 0
	 && ilon3 >= 0
	 && ilon4 >= 0
	 && ilat1 >= 0
	 && ilat2 >= 0
	 && ilat3 >= 0
	 && ilat4 >= 0)
	    datatype = FOURCORNERS;
      }


      /* Then check for full WCS */

      if(datatype == UNKNOWN)
      {
	 ictype1  = tcol("ctype1");
	 ictype2  = tcol("ctype2");
	 iequinox = tcol("equinox");
	 inl      = tcol("nl");
	 ins      = tcol("ns");
	 icrval1  = tcol("crval1");
	 icrval2  = tcol("crval2");
	 icrpix1  = tcol("crpix1");
	 icrpix2  = tcol("crpix2");
	 icdelt1  = tcol("cdelt1");
	 icdelt2  = tcol("cdelt2");
	 icrota2  = tcol("crota2");
	 iepoch   = tcol("epoch");

	 if(ins < 0)
	    ins = tcol("naxis1");

	 if(inl < 0)
	    inl = tcol("naxis2");

	 if(ictype1 >= 0
	 && ictype2 >= 0
	 && inl     >= 0
	 && ins     >= 0
	 && icrval1 >= 0
	 && icrval2 >= 0
	 && icrpix1 >= 0
	 && icrpix2 >= 0
	 && icdelt1 >= 0
	 && icdelt2 >= 0
	 && icrota2 >= 0)
	    datatype = WCS;
      }


      /* And finally settle for just (ra,dec) or (lon,lat) columns */

      if(datatype == UNKNOWN)
      {
	 ilon1 = tcol("ra");
	 ilat1 = tcol("dec");

	 if(ilon1 >= 0
	 && ilat1 >= 0)
	    datatype = LONLAT;
      }

      if(datatype == UNKNOWN)
      {
	 ilon1 = tcol("lon");
	 ilat1 = tcol("lat");

	 if(ilon1 >= 0
	 && ilat1 >= 0)
	    datatype = LONLAT;
      }

      if(datatype == UNKNOWN)
      {
	 ilon1 = tcol("crval1");
	 ilat1 = tcol("crval2");

	 if(ilon1 >= 0
	 && ilat1 >= 0)
	    datatype = LONLAT;
      }


      /* If we found none of the above, give up */

      if(datatype == UNKNOWN)
      {
	 fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Need columns: ctype1 ctype2 nl ns crval1 crval2 crpix1 crpix2 cdelt1 cdelt2 crota2 (equinox optional).  Four corners (equatorial) will be used if they exist or even just a single set of coordinates\"]\n");
	 exit(0);
      }



      /**************************************************/ 
      /* Try to determine coordinate system and equinox */
      /**************************************************/ 

      input.sys   = EQUJ;
      input.epoch = 2000.;


      /* Equinox */

      keyval = tfindkey("EQUINOX");

      if(keyval != (char *)NULL)
	strcpy(epochStr, keyval);  

      keyval = tfindkey("EPOCH");

      if(keyval != (char *)NULL)
	strcpy(epochStr, keyval);  

      keyval = tfindkey("equinox");

      if(keyval != (char *)NULL)
	strcpy(epochStr, keyval);  

      keyval = tfindkey("epoch");

      if(keyval != (char *)NULL)
	strcpy(epochStr, keyval);  


      /* Coordinate system */

      keyval = tfindkey("CSYS");

      if(keyval != (char *)NULL)
	strcpy(csysStr, keyval);

      keyval = tfindkey("SYSTEM");

      if(keyval != (char *)NULL)
	strcpy(csysStr, keyval);

      keyval = tfindkey("SYS");

      if(keyval != (char *)NULL)
	strcpy(csysStr, keyval);

      keyval = tfindkey("COORD");

      if(keyval != (char *)NULL)
	strcpy(csysStr, keyval);

      keyval = tfindkey("COORDSYS");

      if(keyval != (char *)NULL)
	strcpy(csysStr, keyval);

      keyval = tfindkey("csys");

      if(keyval != (char *)NULL)
	strcpy(csysStr, keyval);

      keyval = tfindkey("system");

      if(keyval != (char *)NULL)
	strcpy(csysStr, keyval);

      keyval = tfindkey("sys");

      if(keyval != (char *)NULL)
	strcpy(csysStr, keyval);

      keyval = tfindkey("coord");

      if(keyval != (char *)NULL)
	strcpy(csysStr, keyval);

      keyval = tfindkey("coordsys");

      if(keyval != (char *)NULL)
	strcpy(csysStr, keyval);

      for(i=0; i<strlen(csysStr); ++i)
	 csysStr[i] = tolower(csysStr[i]);
      
      if(epochStr[0] == 'j'
      || epochStr[0] == 'J')
      {
	 refsys = 'j';
	 input.epoch = atof(epochStr+1);
      }
      else if(epochStr[0] == 'b'
	   || epochStr[0] == 'B')
      {
	 refsys = 'b';
	 input.epoch = atof(epochStr+1);
      }
      else
      {
	 refsys = 'j';
	 input.epoch = atof(epochStr);

	 if(input.epoch == 0.)
	    input.epoch = 2000.;
      }

      if(csysStr[strlen(csysStr)-1] == 'j')
	 refsys = 'j';
      else if(csysStr[strlen(csysStr)-1] == 'j')
	 refsys = 'b';

      if(strncmp(csysStr, "eq", 2) == 0 && refsys == 'j') input.sys = EQUJ;
      if(strncmp(csysStr, "ec", 2) == 0 && refsys == 'j') input.sys = ECLJ;
      if(strncmp(csysStr, "eq", 2) == 0 && refsys == 'b') input.sys = EQUB;
      if(strncmp(csysStr, "ec", 2) == 0 && refsys == 'b') input.sys = ECLB;
      
      if(strncmp(csysStr, "ga", 2) == 0) input.sys = GAL;
      if(strncmp(csysStr, "sg", 2) == 0) input.sys = SGAL;
      if(strncmp(csysStr, "su", 2) == 0) input.sys = SGAL;



      /**************************************************/ 
      /* Read the records and collect the image corners */
      /**************************************************/ 

      while(1)
      {
	 stat = tread();

	 if(stat < 0)
	    break;

	 ++nimages;

	 if(datatype == LONLAT)
	 {
	    xpos = atof(tval(ilon1));
	    ypos = atof(tval(ilat1));

	    convertCoordinates(input.sys, input.epoch, xpos, ypos,
			       sys, equinox, &lon, &lat, 0.0);

	    lons[ncoords] = lon;
	    lats[ncoords] = lat;

	    ++ncoords;

	    if(icdelt1 >= 0)
	    {
	       val = fabs(atof(tval(icdelt1)));

	       if(val < minCdelt)
		  minCdelt = val;
	    }

	    if(icdelt2 >= 0)
	    {
	       val = fabs(atof(tval(icdelt2)));

	       if(val < minCdelt)
		  minCdelt = val;
	    }
	 }	 
	 if(datatype == FOURCORNERS)
	 {
	    xpos = atof(tval(ilon1));
	    ypos = atof(tval(ilat1));

	    convertCoordinates(input.sys, input.epoch, xpos, ypos,
			       sys, equinox, &lon, &lat, 0.0);

	    lons[ncoords] = lon;
	    lats[ncoords] = lat;

	    ++ncoords;
	    
	    xpos = atof(tval(ilon2));
	    ypos = atof(tval(ilat2));

	    convertCoordinates(input.sys, input.epoch, xpos, ypos,
			       sys, equinox, &lon, &lat, 0.0);

	    lons[ncoords] = lon;
	    lats[ncoords] = lat;

	    ++ncoords;
	    
	    xpos = atof(tval(ilon3));
	    ypos = atof(tval(ilat3));

	    convertCoordinates(input.sys, input.epoch, xpos, ypos,
			       sys, equinox, &lon, &lat, 0.0);

	    lons[ncoords] = lon;
	    lats[ncoords] = lat;

	    ++ncoords;
	    
	    xpos = atof(tval(ilon4));
	    ypos = atof(tval(ilat4));

	    convertCoordinates(input.sys, input.epoch, xpos, ypos,
			       sys, equinox, &lon, &lat, 0.0);

	    lons[ncoords] = lon;
	    lats[ncoords] = lat;

	    ++ncoords;

	    if(icdelt1 >= 0)
	    {
	       val = fabs(atof(tval(icdelt1)));

	       if(val < minCdelt)
		  minCdelt = val;
	    }

	    if(icdelt2 >= 0)
	    {
	       val = fabs(atof(tval(icdelt2)));

	       if(val < minCdelt)
		  minCdelt = val;
	    }
	 }
	 else if(datatype == WCS)
	 {
	    strcpy(input.ctype1, tval(ictype1));
	    strcpy(input.ctype2, tval(ictype2));

	    input.naxis1    = atoi(tval(ins));
	    input.naxis2    = atoi(tval(inl));
	    input.crpix1    = atof(tval(icrpix1));
	    input.crpix2    = atof(tval(icrpix2));
	    input.crval1    = atof(tval(icrval1));
	    input.crval2    = atof(tval(icrval2));
	    input.cdelt1    = atof(tval(icdelt1));
	    input.cdelt2    = atof(tval(icdelt2));
	    input.crota2    = atof(tval(icrota2));
	    input.equinox   = 2000;

	    if(iequinox >= 0)
	       input.equinox = atoi(tval(iequinox));
	    
	    if(fabs(input.cdelt1) < minCdelt) minCdelt = fabs(input.cdelt1);
	    if(fabs(input.cdelt2) < minCdelt) minCdelt = fabs(input.cdelt2);

	    strcpy(header, "");
	    sprintf(temp, "SIMPLE  = T"                    ); stradd(header, temp);
	    sprintf(temp, "BITPIX  = -64"                  ); stradd(header, temp);
	    sprintf(temp, "NAXIS   = 2"                    ); stradd(header, temp);
	    sprintf(temp, "NAXIS1  = %d",     input.naxis1 ); stradd(header, temp);
	    sprintf(temp, "NAXIS2  = %d",     input.naxis2 ); stradd(header, temp);
	    sprintf(temp, "CTYPE1  = '%s'",   input.ctype1 ); stradd(header, temp);
	    sprintf(temp, "CTYPE2  = '%s'",   input.ctype2 ); stradd(header, temp);
	    sprintf(temp, "CRVAL1  = %11.6f", input.crval1 ); stradd(header, temp);
	    sprintf(temp, "CRVAL2  = %11.6f", input.crval2 ); stradd(header, temp);
	    sprintf(temp, "CRPIX1  = %11.6f", input.crpix1 ); stradd(header, temp);
	    sprintf(temp, "CRPIX2  = %11.6f", input.crpix2 ); stradd(header, temp);
	    sprintf(temp, "CDELT1  = %14.9f", input.cdelt1 ); stradd(header, temp);
	    sprintf(temp, "CDELT2  = %14.9f", input.cdelt2 ); stradd(header, temp);
	    sprintf(temp, "CROTA2  = %11.6f", input.crota2 ); stradd(header, temp);
	    sprintf(temp, "EQUINOX = %d",     input.equinox); stradd(header, temp);
	    sprintf(temp, "END"                            ); stradd(header, temp);
	    
	    input.wcs = wcsinit(header);
				   
	    if(input.wcs == (struct WorldCoor *)NULL)
	    {
	       fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Bad WCS for image %d\"]\n", 
		  nimages);
	       exit(0);
	    }

	    checkWCS(input.wcs, 0);


	    /* Get the coordinate system and epoch in a     */
	    /* form compatible with the conversion library  */

	    if(input.wcs->syswcs == WCS_J2000)
	    {
	       input.sys   = EQUJ;
	       input.epoch = 2000.;

	       if(input.wcs->equinox == 1950)
		  input.epoch = 1950.;
	    }
	    else if(input.wcs->syswcs == WCS_B1950)
	    {
	       input.sys   = EQUB;
	       input.epoch = 1950.;

	       if(input.wcs->equinox == 2000)
		  input.epoch = 2000;
	    }
	    else if(input.wcs->syswcs == WCS_GALACTIC)
	    {
	       input.sys   = GAL;
	       input.epoch = 2000.;
	    }
	    else if(input.wcs->syswcs == WCS_ECLIPTIC)
	    {
	       input.sys   = ECLJ;
	       input.epoch = 2000.;

	       if(input.wcs->equinox == 1950)
	       {
		  input.sys   = ECLB;
		  input.epoch = 1950.;
	       }
	    }
	    else       
	    {
	       input.sys   = EQUJ;
	       input.epoch = 2000.;
	    }



	    /* Collect the locations of the corners of the images */

	    pix2wcs(input.wcs, 0.5, 0.5, &xpos, &ypos);

	    convertCoordinates(input.sys, input.epoch, xpos, ypos,
			       sys, equinox, &lon, &lat, 0.0);

	    lons[ncoords] = lon;
	    lats[ncoords] = lat;

	    ++ncoords;


	    pix2wcs(input.wcs, input.naxis1+0.5, 0.5, &xpos, &ypos);

	    convertCoordinates(input.sys, input.epoch, xpos, ypos,
			       sys, equinox, &lon, &lat, 0.0);

	    lons[ncoords] = lon;
	    lats[ncoords] = lat;

	    ++ncoords;


	    pix2wcs(input.wcs, input.naxis1+0.5, input.naxis2+0.5,
		    &xpos, &ypos);

	    convertCoordinates(input.sys, input.epoch, xpos, ypos,
			       sys, equinox, &lon, &lat, 0.0);

	    lons[ncoords] = lon;
	    lats[ncoords] = lat;

	    ++ncoords;


	    pix2wcs(input.wcs, 0.5, input.naxis2+0.5, &xpos, &ypos);

	    convertCoordinates(input.sys, input.epoch, xpos, ypos,
			       sys, equinox, &lon, &lat, 0.0);

	    lons[ncoords] = lon;
	    lats[ncoords] = lat;

	    ++ncoords;
	 }

	 if(pixelScale > 0.)
	    minCdelt = pixelScale;

	 if(ncoords >= maxcoords)
	 {
	    maxcoords += MAXCOORD;

	    lons = (double *)realloc(lons, maxcoords * sizeof(double));
	    lats = (double *)realloc(lats, maxcoords * sizeof(double));

	    if(lats == (double *)NULL)
	    {
	       fclose(fout);

	       fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Memory allocation failure.\"]\n");
	       fflush(stdout);

	       exit(0);
	    }
	 }
      }

      tclose();
   }


   /**********************************************/ 
   /* Check for very large region (e.g. all-sky) */
   /**********************************************/ 

   xmin =  1.;
   xmax = -1.;
   ymin =  1.;
   ymax = -1.;
   zmin =  1.;
   zmax = -1.;

   for(i=0; i<ncoords; ++i)
   {
      x = cos(lons[i] * dtr) * cos(lats[i] * dtr);
      y = sin(lons[i] * dtr) * cos(lats[i] * dtr);
      z = sin(lats[i] * dtr);

      if(x < xmin) xmin = x;
      if(x > xmax) xmax = x;
      if(y < ymin) ymin = y;
      if(y > ymax) ymax = y;
      if(z < zmin) zmin = z;
      if(z > zmax) zmax = z;
   }

   if(xmax - xmin > 1
   || ymax - ymin > 1
   || zmax - zmin > 1)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"All-sky data\"]\n");
      fflush(stdout);
      exit(0);
   }


   /************************************/ 
   /* Get the bounding box information */
   /************************************/ 

   if(northAligned)
      box = bndVerticalBoundingBox(ncoords, lons, lats);
   else
      box = bndBoundingBox(ncoords, lons, lats);

   if(box == (struct bndInfo *)NULL)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Error computing boundaries.\"]\n");
      fflush(stdout);
      exit(0);
   }

   if(minCdelt <= 0. || minCdelt >= 360.)
      minCdelt = 1./3600.;

   if(isPercentage)
   {
      naxis1 = box->lonSize / minCdelt;
      naxis2 = box->latSize / minCdelt;

      if(naxis1 > naxis2)
	 pad = pad / 100. * naxis1;
      else
	 pad = pad / 100. * naxis2;
   }

   if(debugLevel >= 1)
   {
      printf("pad = %d (isPercentage = %d)\n", pad, isPercentage);
      fflush(stdout);
   }

   naxis1 = box->lonSize / minCdelt + 2 * pad;
   if(naxis1 * minCdelt < box->lonSize) naxis1 += 2;

   naxis2 = box->latSize / minCdelt + 2 * pad;
   if(naxis2 * minCdelt < box->lonSize) naxis2 += 2;

   fprintf(fout, "SIMPLE  = T\n");
   fprintf(fout, "BITPIX  = -64\n");
   fprintf(fout, "NAXIS   = 2\n");
   fprintf(fout, "NAXIS1  = %d\n",     naxis1);
   fprintf(fout, "NAXIS2  = %d\n",     naxis2);

   if(sys == EQUJ)
   {
      fprintf(fout, "CTYPE1  = 'RA---TAN'\n");
      fprintf(fout, "CTYPE2  = 'DEC--TAN'\n");
      fprintf(fout, "EQUINOX = %-g\n", equinox);
   }
   if(sys == EQUB)
   {
      fprintf(fout, "CTYPE1  = 'RA---TAN'\n");
      fprintf(fout, "CTYPE2  = 'DEC--TAN'\n");
      fprintf(fout, "EQUINOX = %-g\n", equinox);
   }
   if(sys == ECLJ)
   {
      fprintf(fout, "CTYPE1  = 'ELON-TAN'\n");
      fprintf(fout, "CTYPE2  = 'ELAT-TAN'\n");
      fprintf(fout, "EQUINOX = %-g\n", equinox);
   }
   if(sys == ECLB)
   {
      fprintf(fout, "CTYPE1  = 'ELON-TAN'\n");
      fprintf(fout, "CTYPE2  = 'ELAT-TAN'\n");
      fprintf(fout, "EQUINOX = %-g\n", equinox);
   }
   if(sys == GAL)
   {
      fprintf(fout, "CTYPE1  = 'GLON-TAN'\n");
      fprintf(fout, "CTYPE2  = 'GLAT-TAN'\n");
   }
   if(sys == SGAL)
   {
      fprintf(fout, "CTYPE1  = 'SLON-TAN'\n");
      fprintf(fout, "CTYPE2  = 'SLAT-TAN'\n");
   }
   
   fprintf(fout, "CRVAL1  = %14.9f\n", box->centerLon);
   fprintf(fout, "CRVAL2  = %14.9f\n", box->centerLat);
   fprintf(fout, "CDELT1  = %14.9f\n", -minCdelt);
   fprintf(fout, "CDELT2  = %14.9f\n", minCdelt);
   fprintf(fout, "CRPIX1  = %14.4f\n", ((double)naxis1 + 1.)/2.);
   fprintf(fout, "CRPIX2  = %14.4f\n", ((double)naxis2 + 1.)/2.);
   fprintf(fout, "CROTA2  = %14.9f\n", box->posAngle);
   fprintf(fout, "END\n");
   fflush(fout);
   fclose(fout);
   

   /* Collect the locations of the corners of the images */

   readTemplate(template);

   ncoords = 0;

   pix2wcs(outwcs, 0.5, 0.5, &lon, &lat);

   lons[ncoords] = lon;
   lats[ncoords] = lat;

   ++ncoords;


   pix2wcs(outwcs, naxis1+0.5, 0.5, &lon, &lat);

   lons[ncoords] = lon;
   lats[ncoords] = lat;

   ++ncoords;


   pix2wcs(outwcs, naxis1+0.5, naxis2+0.5, &lon, &lat);

   lons[ncoords] = lon;
   lats[ncoords] = lat;

   ++ncoords;


   pix2wcs(outwcs, 0.5, naxis2+0.5, &lon, &lat);

   lons[ncoords] = lon;
   lats[ncoords] = lat;

   ++ncoords;


   if(debugLevel != 1)
   {
      fprintf(fstatus, "[struct stat=\"OK\", count=%d, clon=%.6f, clat=%.6f, lonsize=%.6f, latsize=%.6f, posang=%.6f, lon1=%.6f, lat1=%.6f, lon2=%.6f, lat2=%.6f, lon3=%.6f, lat3=%.6f, lon4=%.6f, lat4=%.6f]\n",
	 nimages, 
	 box->centerLon, box->centerLat,
	 minCdelt*naxis1, minCdelt*naxis2,
	 box->posAngle,
	 lons[0], lats[0],
	 lons[1], lats[1],
	 lons[2], lats[2],
	 lons[3], lats[3]);
      fflush(stdout);
   }

   exit(0);
}


int stradd(char *header, char *card)
{
   int i;

   int hlen = strlen(header);
   int clen = strlen(card);

   for(i=0; i<clen; ++i)
      header[hlen+i] = card[i];

   if(clen < 80)
      for(i=clen; i<80; ++i)
	 header[hlen+i] = ' ';
   
   header[hlen+80] = '\0';

   return(strlen(header));
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
   int       j;

   FILE     *fp;

   char      line[MAXSTR];

   char      header[80000];


   /********************************************************/
   /* Open the template file, read and parse all the lines */
   /********************************************************/

   fp = fopen(filename, "r");

   if(fp == (FILE *)NULL)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Template file not found.\"]\n");
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

      if(debugLevel >= 3)
      {
         printf("Template line: [%s]\n", line);
         fflush(stdout);
      }

      stradd(header, line);
   }


   /****************************************/
   /* Initialize the WCS transform library */
   /****************************************/

   outwcs = wcsinit(header);

   if(outwcs == (struct WorldCoor *)NULL)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Output wcsinit() failed.\"]\n");
      exit(1);
   }

   return 0;
}
