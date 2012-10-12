/*

Version  Developer        Date     Change
-------  ---------------  -------  -----------------------
2.6      John Good        24Jun07  CAR fix should not adjust CRVAL2 
2.5      John Good        29Mar06  Need spaces in "datatype=fitshdr"
2.4      John Good        16Feb06  For some reason, the WCS library screws
				   up CAR sometimes unless CRVAL is centered
2.3      John Good        22Mar05  Previous fix was not quite good enough
				   when dealing with rotated images
2.2      John Good        15Mar05  Fixed "-cutout" mode when coord conversion
				   is needed
2.1      Anastasia Alexov 20Jun04  Added "-cutout" mode
1.9      John Good        06Oct03  Added NAXIS1,2 as alternatives to ns,nl
1.8      John Good        25Aug03  Added status file processing
1.7      John Good        24May03  Added "-header" mode
1.6      John Good        04May03  Fixed polygon check
1.5      John Good        02Apr03  Updated overlap checking; circle
				   checks had been left out.
1.4      John Good        22Mar03  Check for existence of input file
				   and for open failure of output
1.3      John Good        22Mar03  Replace atof() with strtod()
				   for command-line parsing
1.2      John Good        22Mar03  Renamed wcsCheck to checkWCS
				   for consistency
1.1      John Good        13Mar03  Added WCS header check
1.0      John Good        29Jan03  Baseline code

*/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <math.h>

#include <montage.h>
#include <fitsio.h>
#include <wcs.h>
#include <coord.h>
#include <mtbl.h>
#include <boundaries.h>

#define POINTS  0
#define BOX     1
#define CIRCLE  2
#define POINT   3
#define HEADER  4
#define CUTOUT  5

#define NULLMODE   0
#define WCSMODE    1
#define CORNERMODE 2

#define COLINEAR_SEGMENTS 0
#define ENDPOINT_ONLY     1
#define NORMAL_INTERSECT  2
#define NO_INTERSECTION   3

typedef struct vec
{
   double x;
   double y;
   double z;
}
Vec;

int     Cross               (Vec *a, Vec *b, Vec *c);
double  Dot                 (Vec *a, Vec *b);
double  Normalize           (Vec *a);
void    Reverse             (Vec *a);
int     SegSegIntersect     (Vec *a, Vec *b, Vec *c, Vec *d, 
                             Vec *e, Vec *f, Vec *p, Vec *q);
int     Between             (Vec *a, Vec *b, Vec *c);

int     swap                (double *x, double *y);

void    pix2wcs (struct WorldCoor*, double, double, double*, double*);

char   *getHdr();

int     checkFile          (char *filename);
int     checkWCS           (struct WorldCoor *wcs, int action);
int     checkHdr           (char *infile, int hdrflag, int hdu);



double dtr;



/*************************************************************************/
/*                                                                       */
/*  mCoverageCheck                                                       */
/*                                                                       */
/*  Montage is a set of general reprojection / coordinate-transform /    */
/*  mosaicking programs.  Any number of input images can be merged into  */
/*  an output FITS file.  The attributes of the input are read from the  */
/*  input files; the attributes of the output are read a combination of  */
/*  the command line and a FITS header template file.                    */
/*                                                                       */
/*  This module, mCoverageCheck, can be used to subset an image          */
/*  metadata table (containing FITS/WCS information or image corners)    */
/*  by determining which records in the table represent images that      */
/*  overlap with a region definition (box or circle on the sky) given    */
/*  on the command line.                                                 */
/*                                                                       */
/*  There are several ways to define the region of interest              */
/*  (polygon, box, circle, point) and defaults for that can be used      */
/*  to simplify some of them.  The following examples should illustrate  */
/*  this:                                                                */
/*                                                                       */
/*  mCoverageCheck swire.tbl region.tbl -points 2. 4. 4. 4. 4. 6. 2. 6.  */
/*  mCoverageCheck swire.tbl region.tbl -box 33. 25. 2. 2. 45.           */
/*  mCoverageCheck swire.tbl region.tbl -box 33. 25. 2. 2.               */
/*  mCoverageCheck swire.tbl region.tbl -box 33. 25. 2.                  */
/*  mCoverageCheck swire.tbl region.tbl -circle 33. 25. 2.8              */
/*  mCoverageCheck swire.tbl region.tbl -point 33. 25.                   */
/*  mCoverageCheck swire.tbl region.tbl -header region.hdr               */
/*  mCoverageCheck swire.tbl region.tbl -cutout 33. 25. 2.               */
/*                                                                       */
/*************************************************************************/

int main(int argc, char **argv) 
{
   int    debug = 0;

   int    i, j, inext, jnext,  offscl, ii, blankRec;
   int    imode, tblmode, stat, ncol, nrow, is_covered; 
   int    status, clockwise, interior, intersectionCode;
   int    ibegin, jbegin, iend, jend, nelements;

   char   infile  [1024];
   char   outfile [1024];
   char   mode    [1024];
   char   fname   [1024];
   char   fullname[1024];
   char   path    [1024];

   FILE  *fout;

   fitsfile *fptr;

   int    nimages;

   double lon, lat;
   double loni, lati;
   double xmin, xmax, ymin, ymax;
   double center_ra, center_dec, box_xsize, box_ysize, xsize, ysize;
   double new_center_ra, new_center_dec;
   double box_rotation, box_radius, circle_radius, dist;
   double center_dist, scale_factor;
   double point_ra[256], point_dec[256], xpix, ypix;

   Vec    point[256], center, edge;
   Vec    point_normal[256];
   Vec    normal1, normal2, direction;

   int    npoints;

   char   proj[16];
   int    csys;

   int    ictype1;
   int    ictype2;
   int    iequinox;
   int    iepoch;
   int    inl;
   int    ins;
   int    icrval1;
   int    icrval2;
   int    icrpix1;
   int    icrpix2;
   int    icdelt1;
   int    icdelt2;
   int    icrota2;

   int    ira, idec;
   int    ira1, idec1;
   int    ira2, idec2;
   int    ira3, idec3;
   int    ira4, idec4;

   int    ifname;

   char   ctype1[256];
   char   ctype2[256];
   char  *end;

   int    nl, naxis1;
   int    ns, naxis2;

   int    equinox;
   double epoch;

   double crpix1;
   double crpix2;

   double crval1;
   double crval2;

   double cdelt1;
   double cdelt2;

   double crota2;

   double image_center_ra, image_center_dec;
   double image_corner_ra[4], image_corner_dec[4];
   double image_box_radius;

   Vec image_corner[4], image_center;
   Vec image_normal[4];

   Vec firstIntersection, secondIntersection;

   struct WorldCoor *wcsbox, *wcsimg;

   char *header;
   
   char   field     [512][MTBL_MAXSTR];
   int    ifield    [512];
   char   fmt       [64];
   char   value     [512][MTBL_MAXSTR];
   char   tmpstr    [MTBL_MAXSTR];
   char   status_str[FLEN_STATUS];

   int naxes[2];
   double crpix[2];
   double cdelt[2];


   struct COORD in, out;

   box_xsize    = 0.;
   box_ysize    = 0.;
   box_rotation = 0.;

   strcpy(in.sys,   "EQ");
   strcpy(in.fmt,   "DDR");
   strcpy(in.epoch, "J2000");

   strcpy(out.sys,   "EQ");
   strcpy(out.fmt,   "SEXC");
   strcpy(out.epoch, "J2000");

   dtr = atan(1.) / 45.;

   fstatus = stdout;


   /* Process basic command-line arguments */

   if(argc < 5)
   {
      printf("[struct stat=\"ERROR\", msg=\"Usage: %s [-s statusfile] in.tbl out.tbl -<mode> <parameters> [where mode can be 'points', 'box', 'circle', 'header', 'point' or 'cutout'\"]\n", argv[0]);

      exit(0);
   }

   for(i=1; i<argc; ++i)
   {
      if(argv[i][0] != '-')
	 break;

      if(strncmp(argv[i], "-s", 2) == 0)
      {
	 if(argc > i+1 && (fstatus = fopen(argv[i+1], "w+")) == (FILE *)NULL)
	 {
	    printf ("[struct stat=\"ERROR\", msg=\"Cannot open status file: %s\"]\n",
	       argv[2]);
	    exit(1);
	 }
      }

      if(strncmp(argv[i], "-p", 2) == 0)
      {
	 if(argc > i+1)
	 strcpy(path, argv[i+1]);
      }
   }

   if(fstatus != stdout)
   {
      argv += 2;
      argc -= 2;
   }

   if(strlen(path) > 0)
   {
      argv += 2;
      argc -= 2;
   }

   if(argc < 5)
   {
      printf("[struct stat=\"ERROR\", msg=\"Usage: %s [-s statusfile] in.tbl out.tbl -<mode> <parameters> [where mode can be 'points', 'box', 'circle', 'header', 'point' or 'cutout'\"]\n", argv[0]);

      exit(0);
   }

   strcpy(infile,  argv[1]);
   strcpy(outfile, argv[2]);
   strcpy(mode,    argv[3]);

   if(debug)
   {
      printf("\n");
      printf("infile   = %s\n", infile);
      printf("outfile  = %s\n", outfile);
      printf("mode     = %s\n", mode);
      fflush(stdout);
   }

   if(checkFile(infile) != 0)
   {
      printf("[struct stat=\"ERROR\", msg=\"Usage: Input table file (%s) does not exist\"]\n", infile);
      exit(1);
   }


        if(strcmp(mode, "-points" ) == 0)  imode = POINTS;
   else if(strcmp(mode, "-box"    ) == 0)  imode = BOX;
   else if(strcmp(mode, "-circle" ) == 0)  imode = CIRCLE;
   else if(strcmp(mode, "-point"  ) == 0)  imode = POINT;
   else if(strcmp(mode, "-header" ) == 0)  imode = HEADER;
   else if(strcmp(mode, "-cutout" ) == 0)  imode = CUTOUT;
   else
   {
      printf("[struct stat=\"ERROR\", msg=\"Invalid region definition mode: \"%s\"]\"]\n", 
         mode);
      fflush(stdout);
      exit(0);
   }



   /* Based on the mode, we look at the rest of the command-line arguments */

   /* If a set of polygon points, interpret as RA,Dec pairs */

   if(imode == POINTS)
   {
      if(argc < 10)
      {
	 fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Too few vertices for region (must be at least three)\"]\n");

	 exit(0);
      }

      npoints = 0;

      center.x = 0.;
      center.y = 0.;
      center.z = 0.;

      for(i=4; i<argc-1; i+=2)
      {
         point_ra [npoints] = strtod(argv[i], &end);

	 if(end < argv[i] + strlen(argv[i]))
	 {
	    fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Longitude %d (%s) cannot be interpreted as a real number\"]\n", 
	       npoints+1, argv[i]);
	    exit(1);
	 }

         point_dec[npoints] = strtod(argv[i+1], &end);

	 if(end < argv[i+1] + strlen(argv[i+1]))
	 {
	    fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Latitude %d (%s) cannot be interpreted as a real number\"]\n", 
	       npoints+1, argv[i+1]);
	    exit(1);
	 }

         point[npoints].x = cos(point_ra[npoints]*dtr) * cos(point_dec[npoints]*dtr);
         point[npoints].y = sin(point_ra[npoints]*dtr) * cos(point_dec[npoints]*dtr);
         point[npoints].z = sin(point_dec[npoints]*dtr);

	 center.x += point[npoints].x;
	 center.y += point[npoints].y;
	 center.z += point[npoints].z;

	 ++npoints;
      }

      Normalize(&center);

      center_ra  = atan2(center.y, center.x) / dtr;
      center_dec = asin (center.z) / dtr;


      /* Find an ordered bounding polygon for these points */

      status = bndBoundaries(npoints, point_ra, point_dec, 3);

      if(status < 0)
      {
	 fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Failed to find bounding polygon for points\"]\n");

	 exit(0);
      }

      if(debug)
      {
	 printf("\nBounding Polygon:\n");

	 for(i=0; i<bndNpoints; ++i)
	 {
	    printf("%25.20f %25.20f\n", bndPoints[i].lon, bndPoints[i].lat);
	    fflush(stdout);
	 }
      }

      for(i=0; i<bndNpoints; ++i)
      {
	 point_ra [i] = bndPoints[i].lon;
	 point_dec[i] = bndPoints[i].lat;

	 point[i].x = bndPoints[i].x;
	 point[i].y = bndPoints[i].y;
	 point[i].z = bndPoints[i].z;
      }

      npoints = bndNpoints;


      box_radius = 0.;

      for(i=0; i<npoints; ++i)
      {
         dist = acos(Dot(&point[i], &center)) / dtr;

	 if(dist > box_radius)
            box_radius = dist;
      }
	 
      if(debug)
      {
	 if (imode == POINTS)
	    printf("\nPOINTS:\n");

	 printf("Center:    %11.6f %11.6f (%10.6f,%10.6f,%10.6f)\n",
		 center_ra, center_dec,
		 center.x, center.y, center.z);

	 for(i=0; i<npoints; ++i)
	    printf("Corner %d:  %11.6f %11.6f (%10.6f,%10.6f,%10.6f)\n",
		   i, point_ra[i], point_dec[i],
	           point[i].x, point[i].y, point[i].z);

	 printf("\nBounding radius: %11.6f\n", box_radius);
      }
   }


   /* If the mode is BOX, get the center, sizes and rotation and use the */
   /* WCS library to determine the points and a bounding circle.         */

   else if((imode == BOX) || (imode == CUTOUT))
   {
      if(argc < 7)
      {
	 fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Too few arguments for box or cutout (must at least have center and size)\"]\n");

	 exit(0);
      }

      center_ra  = strtod(argv[4], &end);

      if(end < argv[4] + strlen(argv[4]))
      {
	 fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Center RA string (%s) cannot be interpreted as a real number\"]\n", 
	    argv[4]);
	 exit(1);
      }

      center_dec = strtod(argv[5], &end);

      if(end < argv[5] + strlen(argv[5]))
      {
	 fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Center Dec string (%s) cannot be interpreted as a real number\"]\n", 
	    argv[5]);
	 exit(1);
      }

      center.x = cos(center_ra*dtr) * cos(center_dec*dtr);
      center.y = sin(center_ra*dtr) * cos(center_dec*dtr);
      center.z = sin(center_dec*dtr);

      box_xsize = strtod(argv[6], &end);

      if(end < argv[6] + strlen(argv[6]))
      {
	 fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"X box size string (%s) cannot be interpreted as a real number\"]\n", 
	    argv[6]);
	 exit(1);
      }

      if(box_xsize <= 0)
      {
	 fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"X box size (%s) must be a positive number\"]\n", 
	    argv[6]);
	 exit(1);
      }

      box_ysize = box_xsize;

      box_rotation = 0.;

      if(argc > 7)
      {
	 box_ysize = strtod(argv[7], &end);

	 if(end < argv[7] + strlen(argv[7]))
	 {
	    fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Y box size string (%s) cannot be interpreted as a real number\"]\n", 
	       argv[7]);
	    exit(1);
	 }
      }

      if(box_ysize <= 0)
      {
	 fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Y box size (%s) must be a positive number\"]\n", 
	    argv[7]);
	 exit(1);
      }

      if(argc > 8)
      {
	 box_rotation = strtod(argv[8], &end);

	 if(end < argv[8] + strlen(argv[8]))
	 {
	    fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Box rotation string (%s) cannot be interpreted as a real number\"]\n", 
	       argv[8]);
	    exit(1);
	 }
      }

      wcsbox = wcskinit (1000, 1000,
                         "RA---TAN", "DEC--TAN",
                         500.5, 500.5,
                         center_ra, center_dec, (double *)NULL,
                         box_xsize/1000., box_ysize/1000.,
                         box_rotation, 2000, 0.);
      
      checkWCS(wcsbox, 0);
		      
      pix2wcs(wcsbox,              -0.5,              -0.5, &point_ra[0], &point_dec[0]);
      pix2wcs(wcsbox, wcsbox->nxpix+0.5,              -0.5, &point_ra[1], &point_dec[1]);
      pix2wcs(wcsbox, wcsbox->nxpix+0.5, wcsbox->nypix+0.5, &point_ra[2], &point_dec[2]);
      pix2wcs(wcsbox,              -0.5, wcsbox->nypix+0.5, &point_ra[3], &point_dec[3]);

      npoints = 4;

      box_radius = 0;

      for(i=0; i<npoints; ++i)
      {
         point[i].x = cos(point_ra[i]*dtr) * cos(point_dec[i]*dtr);
         point[i].y = sin(point_ra[i]*dtr) * cos(point_dec[i]*dtr);
         point[i].z = sin(point_dec[i]*dtr);

         dist = acos(Dot(&point[i], &center)) / dtr;

	 if(dist > box_radius)
            box_radius = dist;
      }
	 
      if(debug)
      {
	 if (imode == BOX)
	    printf("\nBOX:\n");
	 else if (imode == CUTOUT)
	    printf("\nCUTOUT:\n");

	 printf("Center:    %11.6f %11.6f (%10.6f,%10.6f,%10.6f)\n",
		 center_ra, center_dec,
		 center.x, center.y, center.z);
	 printf("Size:      %11.6f %11.6f\n", box_xsize, box_ysize);
	 printf("Angle:     %11.6f\n\n", box_rotation);

	 for(i=0; i<npoints; ++i)
	    printf("Corner %d:  %11.6f %11.6f (%10.6f,%10.6f,%10.6f)\n",
		   i, point_ra[i], point_dec[i],
	           point[i].x, point[i].y, point[i].z);

	 printf("\nBounding radius: %11.6f\n", box_radius);
      }

      if (imode == CUTOUT)
      {
         xsize=box_xsize;
         ysize=box_xsize;

         if (debug)
         {
	    printf("\nXsize= %11.6f, Ysize=%11.6f\n", xsize, ysize);
         }
      }
   }


   /* If the mode is HEADER, use the WCS library     */
   /* to determine the points and a bounding circle. */

   else if(imode == HEADER)
   {
      if(argc < 5)
      {
	 fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Must give header file name\"]\n");
	 exit(1);
      }

      checkHdr(argv[4], 1, 0);

      header = getHdr();

      wcsbox = wcsinit(header);

      if(wcsbox->syswcs == WCS_J2000)
      {
	 csys  = EQUJ;
	 epoch = 2000.;

	 if(wcsbox->equinox == 1950.)
	    epoch = 1950;
      }
      else if(wcsbox->syswcs == WCS_B1950)
      {
	 csys  = EQUB;
	 epoch = 1950.;

	 if(wcsbox->equinox == 2000.)
	    epoch = 2000.;
      }
      else if(wcsbox->syswcs == WCS_GALACTIC)
      {
	 csys  = GAL;
	 epoch = 2000.;
      }
      else if(wcsbox->syswcs == WCS_ECLIPTIC)
      {
	 csys  = ECLJ;
	 epoch = 2000.;

	 if(wcsbox->equinox == 1950.)
	 {
	    csys  = ECLB;
	    epoch = 1950.;
	 }
      }
      else
      {
	 csys  = EQUJ;
	 epoch = 2000.;
      }
      
      pix2wcs(wcsbox, (wcsbox->nxpix+1.)/2., (wcsbox->nypix+1.)/2., &lon, &lat);

      convertCoordinates (csys, epoch, lon, lat, 
			  EQUJ, 2000., &center_ra, &center_dec, 0.);

      center.x = cos(center_ra*dtr) * cos(center_dec*dtr);
      center.y = sin(center_ra*dtr) * cos(center_dec*dtr);
      center.z = sin(center_dec*dtr);

      npoints = 4;

      pix2wcs(wcsbox,              -0.5,              -0.5, &lon, &lat);

      convertCoordinates (csys, epoch, lon, lat, 
			  EQUJ, 2000., &point_ra[0], &point_dec[0], 0.);

      pix2wcs(wcsbox, wcsbox->nxpix+0.5,              -0.5, &lon, &lat);

      convertCoordinates (csys, epoch, lon, lat, 
			  EQUJ, 2000., &point_ra[1], &point_dec[1], 0.);

      pix2wcs(wcsbox, wcsbox->nxpix+0.5, wcsbox->nypix+0.5, &lon, &lat);

      convertCoordinates (csys, epoch, lon, lat, 
			  EQUJ, 2000., &point_ra[2], &point_dec[2], 0.);

      pix2wcs(wcsbox,              -0.5, wcsbox->nypix+0.5, &lon, &lat);

      convertCoordinates (csys, epoch, lon, lat, 
			  EQUJ, 2000., &point_ra[3], &point_dec[3], 0.);


      /* Find an ordered bounding polygon for these points */

      status = bndBoundaries(npoints, point_ra, point_dec, 3);

      if(status < 0)
      {
         fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Failed to find bounding polygon for points\"]\n");
         exit(0);
      }

      if(debug)
      {
         printf("\nBounding Polygon:\n");

         for(i=0; i<bndNpoints; ++i)
         {
            printf("%25.20f %25.20f\n", bndPoints[i].lon, bndPoints[i].lat);
            fflush(stdout);
         }
      }

      for(i=0; i<bndNpoints; ++i)
      {
         point_ra [i] = bndPoints[i].lon;
         point_dec[i] = bndPoints[i].lat;

         point[i].x = bndPoints[i].x;
         point[i].y = bndPoints[i].y;
         point[i].z = bndPoints[i].z;
      }

      npoints = bndNpoints;


      box_radius = 0;

      for(i=0; i<npoints; ++i)
      {
         dist = acos(Dot(&point[i], &center)) / dtr;

	 if(dist > box_radius)
            box_radius = dist;
      }
	 
      if(debug)
      {
	 printf("\nHEADER:\n");
	 printf("Center:    %11.6f %11.6f (%10.6f,%10.6f,%10.6f)\n",
		 center_ra, center_dec,
		 center.x, center.y, center.z);
	 printf("Size:      %11.6f %11.6f\n", box_xsize, box_ysize);
	 printf("Angle:     %11.6f\n\n", box_rotation);

	 for(i=0; i<npoints; ++i)
	    printf("Corner %d:  %11.6f %11.6f (%10.6f,%10.6f,%10.6f)\n",
		   i, point_ra[i], point_dec[i],
	           point[i].x, point[i].y, point[i].z);

	 printf("\nBounding radius: %11.6f\n", box_radius);
      }
   }


   /* If the mode is CIRCLE, get the center and radius */

   else if(imode == CIRCLE)
   {
      if(argc < 6)
      {
	 fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Too few arguments for circle (must at least have center)\"]\n");

	 exit(0);
      }

      center_ra  = strtod(argv[4], &end);

      if(end < argv[4] + strlen(argv[4]))
      {
	 fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Center RA string (%s) cannot be interpreted as a real number\"]\n", 
	    argv[4]);
	 exit(1);
      }

      center_dec = strtod(argv[5], &end);

      if(end < argv[5] + strlen(argv[5]))
      {
	 fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Center Dec string (%s) cannot be interpreted as a real number\"]\n", 
	    argv[5]);
	 exit(1);
      }


      center.x = cos(center_ra*dtr) * cos(center_dec*dtr);
      center.y = sin(center_ra*dtr) * cos(center_dec*dtr);
      center.z = sin(center_dec*dtr);

      circle_radius = 0.;

      if(argc > 6)
      {
	 circle_radius = strtod(argv[6], &end);

	 if(end < argv[6] + strlen(argv[6]))
	 {
	    fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Circle radius string (%s) cannot be interpreted as a real number\"]\n", 
	       argv[6]);
	    exit(1);
	 }
      }
      else
	 imode = POINT;
      
      if(circle_radius <= 0.)
	 imode = POINT;
	 
      if(debug)
      {
	 printf("\nCIRCLE:\n");
	 printf("Center:    %11.6f %11.6f (%10.6f,%10.6f,%10.6f)\n",
		 center_ra, center_dec,
		 center.x, center.y, center.z);
	 printf("Radius:    %11.6f\n", circle_radius);
      }
   }


   /* Finally, if the mode is POINT, get the center */

   else if(imode == POINT)
   {
      if(argc < 6)
      {
	 fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Too few arguments for point (must have coordinates)\"]\n");

	 exit(0);
      }

      center_ra  = strtod(argv[4], &end);

      if(end < argv[4] + strlen(argv[4]))
      {
	 fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Point RA string (%s) cannot be interpreted as a real number\"]\n", 
	    argv[4]);
	 exit(1);
      }

      center_dec = strtod(argv[5], &end);

      if(end < argv[5] + strlen(argv[5]))
      {
	 fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Point RA string (%s) cannot be interpreted as a real number\"]\n", 
	    argv[5]);
	 exit(1);
      }
	 
      center.x = cos(center_ra*dtr) * cos(center_dec*dtr);
      center.y = sin(center_ra*dtr) * cos(center_dec*dtr);
      center.z = sin(center_dec*dtr);

      if(debug)
      {
	 printf("\nPOINT:\n");
	 printf("Location:  %11.6f %11.6f (%10.6f,%10.6f,%10.6f)\n",
		 center_ra, center_dec,
		 center.x, center.y, center.z);
      }

      circle_radius = 0.;
   }


   /* Open the table and find the sky geometry columns */

   ncol = topen(infile);
 
   if(ncol < 0)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Error opening table %s\"]\n", 
         infile);
      fflush(stdout);
      exit(0);
   }

   fout = fopen(outfile, "w+");

   if(fout == (FILE *)NULL)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Cannot create output file (%s)\"]\n",
	 outfile);
      exit(1);
   }

   fprintf(fout, "\\datatype = fitshdr\n");
   fprintf(fout, "%s\n", tbl_hdr_string);

   if(haveType)
      fprintf(fout, "%s\n", tbl_typ_string);

   fflush(fout);

   if(debug)
   {
      printf("ncol     = %d\n", ncol);
      printf("\n");
      fflush(stdout);
   }

   ictype1  = tcol( "ctype1");
   ictype2  = tcol( "ctype2");
   iequinox = tcol( "equinox");
   iepoch   = tcol( "epoch");
   inl      = tcol( "nl");
   ins      = tcol( "ns");
   icrval1  = tcol( "crval1");
   icrval2  = tcol( "crval2");
   icrpix1  = tcol( "crpix1");
   icrpix2  = tcol( "crpix2");
   icdelt1  = tcol( "cdelt1");
   icdelt2  = tcol( "cdelt2");
   icrota2  = tcol( "crota2");
   ira      = tcol( "ra");
   idec     = tcol( "dec");

   if(ins < 0)
      ins = tcol("naxis1");

   if(inl < 0)
      inl = tcol("naxis2");

   ira1     = tcol( "ra1");
   idec1    = tcol( "dec1");
   ira2     = tcol( "ra2");
   idec2    = tcol( "dec2");
   ira3     = tcol( "ra3");
   idec3    = tcol( "dec3");
   ira4     = tcol( "ra4");
   idec4    = tcol( "dec4");

   ifname   = tcol("fname");

   if(ifname < 0)
      ifname = tcol( "file");


   if(debug)
   {
      printf("ira      = %d\n", ira);
      printf("idec     = %d\n", idec);
      printf("ictype1  = %d\n", ictype1);
      printf("ictype2  = %d\n", ictype2);
      printf("iequinox = %d\n", iequinox);
      printf("iepoch   = %d\n", iepoch);
      printf("inl      = %d\n", inl);
      printf("ins      = %d\n", ins);
      printf("icrval1  = %d\n", icrval1);
      printf("icrval2  = %d\n", icrval2);
      printf("icrpix1  = %d\n", icrpix1);
      printf("icrpix2  = %d\n", icrpix2);
      printf("icdelt1  = %d\n", icdelt1);
      printf("icdelt2  = %d\n", icdelt2);
      printf("icrota2  = %d\n", icrota2);
      printf("ira1     = %d\n", ira1);
      printf("idec1    = %d\n", idec1);
      printf("ira2     = %d\n", ira2);
      printf("idec2    = %d\n", idec2);
      printf("ira3     = %d\n", ira3);
      printf("idec3    = %d\n", idec3);
      printf("ira4     = %d\n", ira4);
      printf("idec4    = %d\n", idec4);
      printf("ifname   = %d\n", ifname);
      printf("\n");
      fflush(stdout);
   }

   if (imode == CUTOUT)
   {
      /* get all the names of the columns */

      for (ii=0; ii < ncol; ii++)
      {
        strcpy(field[ii], tbl_rec[ii].name);
        ifield[ii] = tcol(tbl_rec[ii].name);
      }

      if(ifname < 0)
      {
	 fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"CUTOUT mode needs a valid 'fname' or 'file' column\"]\n",
	    outfile);
	 exit(1);
      }
   }

   /* Corners, if they exist, take precedence */
   /* except for CUTOUT, where we need the    */
   /* full transform.                         */

   tblmode = NULLMODE;

   if(ictype1  >= 0
   && ictype2  >= 0
   && inl      >= 0
   && ins      >= 0
   && icrval1  >= 0
   && icrval2  >= 0
   && icrpix1  >= 0
   && icrpix2  >= 0
   && icdelt1  >= 0
   && icdelt2  >= 0
   && icrota2  >= 0)
      tblmode = WCSMODE;

   if(ira1     >= 0
   && idec1    >= 0
   && ira2     >= 0
   && idec2    >= 0
   && ira3     >= 0
   && idec3    >= 0
   && ira4     >= 0
   && idec4    >= 0)
      tblmode = CORNERMODE;

   if(tblmode == NULLMODE)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Need either WCS or corner columns.\"]\n");
      exit(0);
   }



   /* Read the table file and process each record */

   nrow    = 0;
   nimages = 0;

   while(1)
   {
      blankRec = 0;

      stat = tread();

      if(stat < 0)
         break;

      ++nrow;


      /* If we don't have the corners, compute them */
      /* (and the center) using the WCS library     */
      /* Use the center and corners to determine a  */
      /* bounding radius                            */

      if(tblmode == WCSMODE)
      {
	 strcpy(ctype1, tval(ictype1));
	 strcpy(ctype2, tval(ictype2));

	 equinox = 0;
	 epoch   = 0;

	 if(iequinox >= 0)
	    equinox = atoi(tval(iequinox));

	 if(iepoch >= 0)
	    epoch   = atof(tval(iepoch));

	 nl      = atoi(tval(inl));
	 ns      = atoi(tval(ins));

	 if(strlen(tval(icrval1)) == 0
	 || strlen(tval(icrval2)) == 0)
	    blankRec = 1;
	    
	 crval1  = atof(tval(icrval1));
	 crval2  = atof(tval(icrval2));

	 crpix1  = atof(tval(icrpix1));
	 crpix2  = atof(tval(icrpix2));

	 cdelt1  = atof(tval(icdelt1));
	 cdelt2  = atof(tval(icdelt2));

	 crota2  = atof(tval(icrota2));

	 clockwise = 0;

	 if((cdelt1 < 0 && cdelt2 < 0)
	 || (cdelt1 > 0 && cdelt2 > 0)) clockwise = 1;


	 strcpy(proj, "");
	 csys = EQUJ;

	 if(strlen(ctype1) > 4)
	 strcpy (proj, ctype1+4);  

	 if(strncmp(ctype1, "RA",   2) == 0)
	    csys = EQUJ;
	 if(strncmp(ctype1, "GLON", 4) == 0)
	    csys = GAL;
	 if(strncmp(ctype1, "ELON", 4) == 0)
	    csys = ECLJ;

	 if(debug)
	 {
	    printf("proj      = [%s]\n", proj);
	    printf("csys      = %d\n",   csys);
	    printf("clockwise = %d\n",   clockwise);
	    printf("\n");
	    fflush(stdout);
	 }


	 /* Correct if no epoch / equinox */

	 if (epoch == 0) 
	    epoch = 2000.;

	 if (equinox == 0) 
	 {
	    equinox = 2000;

	    if ((epoch >= 1950.0) && (epoch <= 2000.)) 
	       equinox = (int)epoch;
	 }

	 if(debug)
	 {
	    printf("nrow     = %d\n",    nrow);
	    printf("-------------\n");
	    printf("ctype1   = [%s]\n",  ctype1);
	    printf("ctype2   = [%s]\n",  ctype2);

	    printf("equinox  = %d\n",    equinox);
	    printf("epoch    = %-g\n",   epoch);

	    printf("ns       = %d\n",    ns);
	    printf("nl       = %d\n",    nl);

	    printf("crval1   = %-g\n",   crval1);
	    printf("crval2   = %-g\n",   crval2);

	    printf("crpix1   = %-g\n",   crpix1);
	    printf("crpix2   = %-g\n",   crpix2);

	    printf("cdelt1   = %-g\n",   cdelt1);
	    printf("cdelt2   = %-g\n",   cdelt2);

	    printf("crota2   = %-g\n",   crota2);
	    printf("\n");
	    fflush(stdout);
	 }

	 wcsimg = wcskinit (ns,     nl,
	   		    ctype1, ctype2,
			    crpix1, crpix2,
			    crval1, crval2, (double *)NULL,
			    cdelt1, cdelt2,
			    crota2, equinox, 0.);
	 
	 checkWCS(wcsimg, 0);

	 if(debug)
	 {
	    printf("WCS set within WCSMODE\n");
	    fflush(stdout);
	 }

	 if (nowcs (wcsimg)) 
	 {
	    fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Failed to create wcs structure for record %d.\"]\n", nrow);
	    exit(0);
	 }

	 pix2wcs(wcsimg, wcsimg->nxpix/2.+0.5, wcsimg->nypix/2.+0.5, &lon, &lat);

	 convertCoordinates (csys, (double)equinox, lon, lat, 
			     EQUJ, 2000., &image_center_ra, &image_center_dec, 0.);

	 image_center.x = cos(image_center_ra*dtr) * cos(image_center_dec*dtr);
	 image_center.y = sin(image_center_ra*dtr) * cos(image_center_dec*dtr);
	 image_center.z = sin(image_center_dec*dtr);

	 pix2wcs(wcsimg, -0.5, -0.5, &lon, &lat);
	 convertCoordinates (csys, (double)equinox, lon, lat, 
			     EQUJ, 2000., &image_corner_ra[0], &image_corner_dec[0], 0.);


	 pix2wcs(wcsimg, wcsimg->nxpix+0.5, -0.5, &lon, &lat);
	 convertCoordinates (csys, (double)equinox, lon, lat, 
			     EQUJ, 2000., &image_corner_ra[1], &image_corner_dec[1], 0.);


	 pix2wcs(wcsimg, wcsimg->nxpix+0.5, wcsimg->nypix+0.5, &lon, &lat);
	 convertCoordinates (csys, (double)equinox, lon, lat, 
			     EQUJ, 2000., &image_corner_ra[2], &image_corner_dec[2], 0.);


	 pix2wcs(wcsimg, -0.5, wcsimg->nypix+0.5, &lon, &lat);
	 convertCoordinates (csys, (double)equinox, lon, lat, 
			     EQUJ, 2000., &image_corner_ra[3], &image_corner_dec[3], 0.);
      }


      /* If we have corners, determine the centroid and */
      /* bounding radius in the same way                */

      else
      {
	 if(strlen(tval(ira1) ) == 0
	 || strlen(tval(idec1)) == 0
	 || strlen(tval(ira2) ) == 0
	 || strlen(tval(idec2)) == 0
	 || strlen(tval(ira3) ) == 0
	 || strlen(tval(idec3)) == 0
	 || strlen(tval(ira4) ) == 0
	 || strlen(tval(idec4)) == 0)
	    blankRec = 1;
	    
	 image_corner_ra [0] = atof(tval(ira1));
	 image_corner_dec[0] = atof(tval(idec1));
	 image_corner_ra [1] = atof(tval(ira2));
	 image_corner_dec[1] = atof(tval(idec2));
	 image_corner_ra [2] = atof(tval(ira3));
	 image_corner_dec[2] = atof(tval(idec3));
	 image_corner_ra [3] = atof(tval(ira4));
	 image_corner_dec[3] = atof(tval(idec4));
      }

      for(i=0; i<4; ++i)
      {
	 image_corner[i].x = cos(image_corner_ra[i]*dtr) * cos(image_corner_dec[i]*dtr);
	 image_corner[i].y = sin(image_corner_ra[i]*dtr) * cos(image_corner_dec[i]*dtr);
	 image_corner[i].z = sin(image_corner_dec[i]*dtr);
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
	 swap(&image_corner_ra [0], &image_corner_ra [3]);
	 swap(&image_corner_dec[0], &image_corner_dec[3]);

	 swap(&image_corner[0].x, &image_corner[3].x);
	 swap(&image_corner[0].y, &image_corner[3].y);
	 swap(&image_corner[0].z, &image_corner[3].z);

	 swap(&image_corner_ra [1], &image_corner_ra [2]);
	 swap(&image_corner_dec[1], &image_corner_dec[2]);

	 swap(&image_corner[1].x, &image_corner[2].x);
	 swap(&image_corner[1].y, &image_corner[2].y);
	 swap(&image_corner[1].z, &image_corner[2].z);
      }

      if(tblmode == CORNERMODE)
      {
	 image_center.x = 0.;
	 image_center.y = 0.;
	 image_center.z = 0.;

	 for(i=0; i<4; ++i)
	 {
	    image_center.x += image_corner[i].x;
	    image_center.y += image_corner[i].y;
	    image_center.z += image_corner[i].z;
	 }

	 Normalize(&image_center);

	 image_center_ra  = atan2(center.y, center.x) / dtr;
	 image_center_dec = asin (center.z) / dtr;
      }

      image_box_radius = 0.;

      for(i=0; i<4; ++i)
      {
	 dist = acos(Dot(&image_corner[i], &image_center)) / dtr;

	 if(dist > image_box_radius)
	    image_box_radius = dist;
      }

      if(debug)
      {
	 printf("\nImage %d:\n", nrow);
	 printf("   Center:    %11.6f %11.6f (%10.6f,%10.6f,%10.6f)\n",
		 image_center_ra, image_center_dec,
		 image_center.x, image_center.y, image_center.z);

	 for(i=0; i<4; ++i)
	    printf("   Corner %d:  %11.6f %11.6f (%10.6f,%10.6f,%10.6f) [%10.6f]\n",
		   i, image_corner_ra[i], image_corner_dec[i],
	           image_corner[i].x, image_corner[i].y, image_corner[i].z,
		   acos(Dot(&image_corner[i], &image_center)) / dtr);

	 printf("\n   Bounding radius: %11.6f\n", image_box_radius);
      }


      
      /***********************************************/
      /* The checks are specific to the region shape */
      /* (point or circle or box/polygon)            */
      /*                                             */
      /* In each case, we first check to see if the  */
      /* bounding radii overlap (if not, we discard  */
      /* the record).                                */
      /***********************************************/

      /**************/
      /* POINT Mode */
      /**************/

      if(imode == POINT)
      {
	 /* Bounding circle check */

         if(blankRec)
	    continue;

	 center_dist = acos(Dot(&center, &image_center)) / dtr;

	 if(center_dist > image_box_radius)
	    continue;

	 if(debug)
	 {
	    printf("POINT passed bounding circle check)\n");
	    fflush(stdout);
	 }


	 /* Point inside image check */

	 interior = 1;

	 for(j=0; j<4; ++j)
	 {
	    jnext = (j+1)%4;

	    Cross(&image_corner[j], &image_corner[jnext], &image_normal[j]);

            Normalize(&image_normal[j]);

	    if(Dot(&image_normal[j], &center) < 0)
	    {
	       interior = 0;
	       break;
	    }
	 }
        
	 if(interior)
	 {
	    if(debug)
	    {
	       printf("\n******** POINT Overlap *********\n");
	       fflush(stdout);
	    }

	    fprintf(fout, "%s\n", tbl_rec_string);
	    fflush(fout);
	    ++nimages;
	 }

	 continue;
      }


      /**************************/
      /* BOX/POINTS/HEADER Mode */
      /**************************/

      else if(imode == BOX || imode == POINTS || imode == HEADER || imode == CUTOUT)
      {
	 /* Bounding circle check */

	 center_dist = acos(Dot(&center, &image_center)) / dtr;

	 if(center_dist > box_radius + image_box_radius)
	    continue;

	 if(debug)
	 {
	    printf("BOX/POINTS/HEADER/CUTOUT passed bounding circle check)\n");
	    fflush(stdout);
	 }


	 /* Get image side normals */

	 for(j=0; j<4; ++j)
	 {
	    jnext = (j+1)%4;

	    Cross(&image_corner[j], &image_corner[jnext], &image_normal[j]);

            Normalize(&image_normal[j]);
	 }


	 /* Get region side normals */

	 for(j=0; j<npoints; ++j)
	 {
	    jnext = (j+1)%npoints;

	    Cross(&point[j], &point[jnext], &point_normal[j]);

            Normalize(&point_normal[j]);
	 }


	 /* Region inside image check */

	 is_covered = 0;

	 for(i=0; i<npoints; ++i)
	 {
	    interior = 1;

	    for(j=0; j<4; ++j)
	    {
	       if(Dot(&image_normal[j], &point[i]) < 0)
	       {
		  interior = 0;
		  break;
	       }
	    }

	    if(interior)
	    {
	       is_covered = 1;

	       if(debug)
	       {
		  printf("\n******** BOX/POINTS/HEADER/CUTOUT Overlap (region inside image) *********\n");
		  fflush(stdout);
	       }

	       break;
	    }
	 }


	 /* Image inside region check */

	 if(!is_covered)
	 {
	    for(i=0; i<4; ++i)
	    {
	       interior = 1;

	       for(j=0; j<npoints; ++j)
	       {
		  if(Dot(&point_normal[j], &image_corner[i]) < 0)
		  {
		     interior = 0;
		     break;
		  }
	       }

	       if(interior)
	       {
		  is_covered = 1;

		  if(debug)
		  {
		     printf("\n******** BOX/POINTS/HEADER/CUTOUT Overlap (image inside region) *********\n");
		     fflush(stdout);
		  }

		  break;
	       }
	    }
	 }


	 /* Overlapping segments check */

	 if(!is_covered)
	 {
	    /* Find point(s) of intersection between edges */

	    for(j=0; j<npoints; ++j)
	    {
	       jnext = (j+1)%npoints;

	       for(i=0; i<4; ++i)
	       {
		  inext = (i+1)%4;

		  intersectionCode = SegSegIntersect(&point_normal[j],   &image_normal[i], 
						     &point[j],          &point[jnext],
						     &image_corner[i],   &image_corner[inext], 
						     &firstIntersection, &secondIntersection);

		  if(intersectionCode != NO_INTERSECTION) 
		  {
		     is_covered = 1;

		     if(debug)
		     {
			printf("\n******** BOX/POINTS/HEADER/CUTOUT Overlap (overlapping segments) *********\n");
			fflush(stdout);
		     }

		     break;
		  }
	       }

	       if(is_covered)
		  break;
	    }
	 }


	 /* For CUTOUT, we may need the exact WCS set up */
	 /* To be safe, if we do we go back to the file  */

         if ((is_covered) && (imode == CUTOUT))
	 {
            strcpy(ctype1, tval(ictype1));
            strcpy(ctype2, tval(ictype2));

            equinox = 0;
            epoch   = 0;

            if(iequinox >= 0)
	       equinox = atoi(tval(iequinox));

            if(iepoch >= 0)
	       epoch   = atof(tval(iepoch));

            nl	    = atoi(tval(inl));
            ns	    = atoi(tval(ins));

            if(strlen(tval(icrval1)) == 0
            || strlen(tval(icrval2)) == 0)
	       blankRec = 1;
	   
            crval1  = atof(tval(icrval1));
            crval2  = atof(tval(icrval2));

            crpix1  = atof(tval(icrpix1));
            crpix2  = atof(tval(icrpix2));

            cdelt1  = atof(tval(icdelt1));
            cdelt2  = atof(tval(icdelt2));

            crota2  = atof(tval(icrota2));

            clockwise = 0;

            if((cdelt1 < 0 && cdelt2 < 0)
	       || (cdelt1 > 0 && cdelt2 > 0)) clockwise = 1;

            strcpy(proj, "");
            csys = EQUJ;

            if(strlen(ctype1) > 4)
	      strcpy (proj, ctype1+4);

            if(strncmp(ctype1, "RA",   2) == 0)
	      csys = EQUJ;
            if(strncmp(ctype1, "GLON", 4) == 0)
	      csys = GAL;
            if(strncmp(ctype1, "ELON", 4) == 0)
	      csys = ECLJ;

            if(debug)
            {
              printf("proj	= [%s]\n", proj);
              printf("csys	= %d\n",   csys);
              printf("clockwise = %d\n",   clockwise);
              printf("\n");
              fflush(stdout);
            }


            /* KLUDGE for CAR projection	   */
            /* WCS library gives wrong answer for  */
            /* very large CRPIX offsets (e.g. from */
            /* large longitudes back to 0,0)	   */

            if(strcmp(proj, "-CAR") == 0)
            {
               crval1 = crval1 - cdelt1 * crpix1 + cdelt1 * ns/2.;
               crpix1 = ns/2.;
            }


            /* Correct if no epoch / equinox */

            if (epoch == 0)
              epoch = 2000.;

            if (equinox == 0)
            {
              equinox = 2000;

              if ((epoch >= 1950.0) && (epoch <= 2000.))
		equinox = (int)epoch;
            }

            if(debug)
            {
              printf("nrow     = %d\n",	   nrow);
              printf("-------------\n");
              printf("ctype1   = [%s]\n",  ctype1);
              printf("ctype2   = [%s]\n",  ctype2);

              printf("equinox  = %d\n",	   equinox);
              printf("epoch    = %-g\n",   epoch);

              printf("ns       = %d\n",	   ns);
              printf("nl       = %d\n",	   nl);

              printf("crval1   = %-g\n",   crval1);
              printf("crval2   = %-g\n",   crval2);

              printf("crpix1   = %-g\n",   crpix1);
              printf("crpix2   = %-g\n",   crpix2);

              printf("cdelt1   = %-g\n",   cdelt1);
              printf("cdelt2   = %-g\n",   cdelt2);

              printf("crota2   = %-g\n",   crota2);
              printf("\n");
              fflush(stdout);
            }

            wcsimg = wcskinit (ns,     nl,
                               ctype1, ctype2,
                               crpix1, crpix2,
                               crval1, crval2, (double *)NULL,
                               cdelt1, cdelt2,
                               crota2, equinox, 0.);
	

	    /* If we aren't sure the WCS from the metadata table   */
	    /* is adequately defined, go back to the original file */

	    if(strcmp(proj, "-ZPN") == 0 
	    || strcmp(proj, "-COP") == 0
	    || strcmp(proj, "-COE") == 0
	    || strcmp(proj, "-COD") == 0
	    || strcmp(proj, "-COO") == 0
	    || strcmp(proj, "-BON") == 0
	    || strcmp(proj, "-DSS") == 0)
	    {
	       strcpy(fname, tval(ifname));

	       if(fname[0] != '/')
	       {
		  strcpy(fullname, path);

		  if(fullname[strlen(fullname)-1] != '/')
		     strcat(fullname, "/");

		  strcat(fullname, fname);

		  strcpy(fname, fullname);
	       }

	       if(fits_open_file(&fptr, fname, READONLY, &status))
	       {
		  fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Image file %s missing or invalid FITS\"]\n", fname);
		  exit(1);
	       }

	       if(fits_get_image_wcs_keys(fptr, &header, &status))
	       {
		  fits_get_errstatus(status, status_str);

		  fprintf(fstatus, "[struct stat=\"ERROR\", status=%d, msg=\"%s\"]\n", status, status_str);

		  exit(1);
	       }

	       wcsimg = wcsinit(header);

	       if(wcsimg == (struct WorldCoor *)NULL)
	       {
		  fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Input wcsinit() failed.\"]\n");
		  exit(1);
	       }

	       csys = EQUJ;

	       if(strncmp(wcsimg->ctype[0], "RA",   2) == 0)
		  csys = EQUJ;
	       if(strncmp(wcsimg->ctype[0], "GLON", 4) == 0)
		  csys = GAL;
	       if(strncmp(wcsimg->ctype[0], "ELON", 4) == 0)
		  csys = ECLJ;

	       equinox = wcsimg->equinox;

	       if (equinox == 0) 
		 equinox = 2000;

	       strcpy(ctype1, wcsimg->ctype[0]);
	       strcpy(ctype2, wcsimg->ctype[1]);

	       ns     = wcsimg->nxpix;
	       nl     = wcsimg->nypix;
	       crval1 = wcsimg->crval[0];
	       crval2 = wcsimg->crval[1];
	       crpix1 = wcsimg->xrefpix;
	       crpix2 = wcsimg->yrefpix;
	       cdelt1 = wcsimg->xinc;
	       cdelt2 = wcsimg->yinc;
	       crota2 = wcsimg->rot;

	       if(debug)
	       {
		 printf("csys      = %d\n",   csys);
		 printf("equinox   = %d\n",   equinox);
		 printf("ctype1    = \"%s\"\n",   ctype1);
		 printf("ctype2    = \"%s\"\n",   ctype2);
		 printf("ns        = %d\n",   ns);
		 printf("nl        = %d\n",   nl);
		 printf("crval1    = %-g\n",  crval1);
		 printf("crval2    = %-g\n",  crval2);
		 printf("crpix1    = %-g\n",  crpix1);
		 printf("crpix2    = %-g\n",  crpix2);
		 printf("cdelt1    = %-g\n",  cdelt1);
		 printf("cdelt2    = %-g\n",  cdelt2);
		 printf("crota2    = %-g\n",  crota2);
		 printf("\n");
		 fflush(stdout);
	       }
	    }

	    checkWCS(wcsimg, 0);

	    if(debug)
	    {
	      printf("WCS set for CUTOUTs\n");
	      fflush(stdout);
	    }

	    if (nowcs (wcsimg)) 
	    {
	      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Failed to create wcs structure for record %d.\"]\n", nrow);
	      exit(0);
	    }


	    /* Now we need to find the range of pixel X,Y  */
	    /* that cover the part of the image inside the */
	    /* region. We check the image vertices and the */
	    /* points of intersection between image and    */
	    /* region sides.                               */

	    xmin =  1.e20;
	    xmax = -1.e20;
	    ymin =  1.e20;
	    ymax = -1.e20;


	    /* First find region vertices inside image */

	    for(i=0; i<npoints; ++i)
	    {
	       interior = 1;

	       for(j=0; j<4; ++j)
	       {
		  if(Dot(&image_normal[j], &point[i]) < 0)
		  {
		     interior = 0;
		     break;
		  }
	       }

	       if(interior)
	       {
		  loni = atan2(point[i].y, point[i].x) / dtr;
		  lati = asin (point[i].z) / dtr;

		  convertCoordinates (EQUJ, 2000., loni, lati,
				      csys, epoch, &lon, &lat, 0.);

		  wcs2pix(wcsimg, lon, lat, &xpix, &ypix, &offscl);

		  if(xpix < xmin) xmin = xpix;
		  if(xpix > xmax) xmax = xpix;
		  if(ypix < ymin) ymin = ypix;
		  if(ypix > ymax) ymax = ypix;

		  if(debug)
		  {
		     printf("Include: %d %d: %-g %-g -> %-g %-g -> %-g %-g (region corner)\n",
			i, j, loni, lati, lon, lat, xpix, ypix);
		  }
	       }
	    }


	    /* Then find image vertices inside region */

	    for(i=0; i<4; ++i)
	    {
	       interior = 1;

	       for(j=0; j<npoints; ++j)
	       {
		  if(Dot(&point_normal[j], &image_corner[i]) < 0)
		  {
		     interior = 0;
		     break;
		  }
	       }

	       if(interior)
	       {
		  loni = atan2(image_corner[i].y, image_corner[i].x) / dtr;
		  lati = asin (image_corner[i].z) / dtr;

		  convertCoordinates (EQUJ, 2000., loni, lati,
				      csys, epoch, &lon, &lat, 0.);

		  wcs2pix(wcsimg, lon, lat, &xpix, &ypix, &offscl);

		  if(xpix < xmin) xmin = xpix;
		  if(xpix > xmax) xmax = xpix;
		  if(ypix < ymin) ymin = ypix;
		  if(ypix > ymax) ymax = ypix;

		  if(debug)
		  {
		     printf("Include: %d %d: %-g %-g -> %-g %-g -> %-g %-g (image corner)\n",
			i, j, loni, lati, lon, lat, xpix, ypix);
		  }
	       }
	    }


	    /* Find point(s) of intersection between edges */

	    for(j=0; j<npoints; ++j)
	    {
	       jnext = (j+1)%npoints;

	       for(i=0; i<4; ++i)
	       {
		  inext = (i+1)%4;

		  intersectionCode = SegSegIntersect(&point_normal[j],   &image_normal[i], 
						     &point[j],          &point[jnext],
						     &image_corner[i],   &image_corner[inext], 
						     &firstIntersection, &secondIntersection);

		  if(intersectionCode == NORMAL_INTERSECT 
		  || intersectionCode == ENDPOINT_ONLY 
		  || intersectionCode == COLINEAR_SEGMENTS) 
		  {
		     loni = atan2(firstIntersection.y, firstIntersection.x) / dtr;
		     lati = asin (firstIntersection.z) / dtr;

		     convertCoordinates (EQUJ, 2000., loni, lati,
					 csys, epoch, &lon, &lat, 0.);

		     wcs2pix(wcsimg, lon, lat, &xpix, &ypix, &offscl);

		     if(xpix < xmin) xmin = xpix;
		     if(xpix > xmax) xmax = xpix;
		     if(ypix < ymin) ymin = ypix;
		     if(ypix > ymax) ymax = ypix;

		     if(debug)
		     {
			printf("Include: %d %d: %-g %-g -> %-g %-g -> %-g %-g (intersection)\n",
			   i, j, loni, lati, lon, lat, xpix, ypix);
		     }
		  }

		  if(intersectionCode == COLINEAR_SEGMENTS) 
		  {
		     loni = atan2(secondIntersection.y, secondIntersection.x) / dtr;
		     lati = asin (secondIntersection.z) / dtr;

		     convertCoordinates (EQUJ, 2000., loni, lati,
					 csys, epoch, &lon, &lat, 0.);

		     wcs2pix(wcsimg, lon, lat, &xpix, &ypix, &offscl);

		     if(xpix < xmin) xmin = xpix;
		     if(xpix > xmax) xmax = xpix;
		     if(ypix < ymin) ymin = ypix;
		     if(ypix > ymax) ymax = ypix;

		     if(debug)
		     {
			printf("Include: %d %d: %-g %-g -> %-g %-g -> %-g %-g (intersection 2)\n",
			   i, j, loni, lati, lon, lat, xpix, ypix);
		     }
		  }
	       }
	    }

            naxes[0]=ns;
            naxes[1]=nl;
            crpix[0]=crpix1;
	    crpix[1]=crpix2;
	    cdelt[0]=cdelt1;
	    cdelt[1]=cdelt2;

	    ibegin = xmin;
	    iend   = xmax + 1.0;

	    jbegin = ymin;
	    jend   = ymax + 1.0;


	    if(debug)
	    {
	       printf("Pixel ranges:  %d to %d and %d to %d\n", 
		  ibegin, iend, jbegin, jend);
	       fflush(stdout); 
	    }

	    if(debug)
	    {
	       printf("naxes[0] = %d\n",   naxes[0]);
	       printf("naxes[1] = %d\n",   naxes[1]);
	       fflush(stdout);
	    }

	    if(ibegin < 1       ) ibegin = 1;
	    if(ibegin > naxes[0]) ibegin = naxes[0];
	    if(iend   > naxes[0]) iend   = naxes[0];
	    if(iend   < 0       ) iend   = naxes[0];

	    if(jbegin < 1       ) jbegin = 1;
	    if(jbegin > naxes[1]) jbegin = naxes[1];
	    if(jend   > naxes[1]) jend   = naxes[1];
	    if(jend   < 0       ) jend   = naxes[1];

	    nelements = iend - ibegin;
	    naxis1 = nelements;
	    naxis2 = jend-jbegin;
	    crpix1 = crpix1-ibegin+1;
	    crpix2 = crpix2-jbegin+1;

	    if(debug)
	    {
	       printf("xsize   = %-g\n",  xsize);
	       printf("ysize   = %-g\n",  ysize);
	       printf("ibegin  = %d\n",   ibegin);
	       printf("iend    = %d\n",   iend);
	       printf("jbegin  = %d\n",   jbegin);
	       printf("jend    = %d\n\n", jend);

	       printf("naxis1 -> %d\n",     naxis1);
	       printf("naxis2 -> %d\n",     naxis2);
	       printf("ctype1  = \"%s\"\n", ctype1);
	       printf("ctype2  = \"%s\"\n", ctype2);
	       printf("crpix1 -> %-g\n",    crpix1);
	       printf("crpix2 -> %-g\n",    crpix2);
	       printf("crval1  = %-g\n",    crval1);
	       printf("crval2  = %-g\n",    crval2);
	       printf("cdelt1  = %-g\n",    cdelt1);
	       printf("cdelt2  = %-g\n",    cdelt2);
	       printf("crota2  = %-g\n",    crota2);
	       printf("equinox = %d\n",     equinox);
	       fflush(stdout);
	    }

	    if(naxis1 <= 0 || naxis2 <= 0)
	    {
	       if(debug)
	       {
		  printf("\nBad naxis value: skipping\n\n");
		  fflush(stdout);
	       }

	       is_covered = 0;
	    }

	    else
	    {
	       /* get the new 4-corners information based on the cutout stats */

	       pix2wcs(wcsimg, xmin, ymin, &lon, &lat);

	       convertCoordinates (csys, epoch, lon, lat, 
				   EQUJ, 2000., &point_ra[0], &point_dec[0], 0.);

	       pix2wcs(wcsimg, xmax, ymin, &lon, &lat);

	       convertCoordinates (csys, epoch, lon, lat, 
				   EQUJ, 2000., &point_ra[1], &point_dec[1], 0.);

	       pix2wcs(wcsimg, xmax, ymax, &lon, &lat);

	       convertCoordinates (csys, epoch, lon, lat, 
				   EQUJ, 2000., &point_ra[2], &point_dec[2], 0.);

	       pix2wcs(wcsimg, xmin, ymax, &lon, &lat);

	       convertCoordinates (csys, epoch, lon, lat, 
				   EQUJ, 2000., &point_ra[3], &point_dec[3], 0.);

	       pix2wcs(wcsimg,(xmin+xmax)/2., (ymin+ymax)/2., &lon, &lat);

	       convertCoordinates (csys, epoch, lon, lat, 
				   EQUJ, 2000., &new_center_ra, &new_center_dec, 0.);


	       if(debug)
	       {
		  printf("CUTOUT Image stats:\n");
		  printf("Center:    %11.6f %11.6f \n",
		     new_center_ra, new_center_dec);
		  printf("Size:      %11.6f %11.6f\n", xsize, ysize);
		  printf("Angle:     %11.6f\n\n", crota2);

		  for(i=0; i<npoints; ++i)
		    printf("Corner %d:  %11.6f %11.6f \n",
			   i, point_ra[i], point_dec[i]);

		  printf("\n");
		  fflush(stdout);
	       }

	       in.lon = new_center_ra;
	       in.lat = new_center_dec;

	       ccalc(&in, &out, "t", "t");

	    }
	 } /* end if ((is_covered) && (imode == CUTOUT)) */


	 /* If it passed any of the checks, copy the record to output */

	 if(is_covered)
	 {
	    if (imode == CUTOUT)
	    {
               /* now update the table structure with new axes and crpix values */
	      for (ii=0; ii<ncol; ii++)
	      {
		 if (strcmp(field[ii], "naxis1") == 0 )
		 {
		    sprintf(tmpstr, "%d", naxis1);
		 }
		 else if (strcmp(field[ii], "naxis2") == 0 )
		 {
		    sprintf(tmpstr, "%d", naxis2);
		 }
		 else if (strcmp(field[ii], "crpix1") == 0 )
		 {
		    sprintf(tmpstr, "%.2f", crpix1);
		 }
		 else if (strcmp(field[ii], "crpix2") == 0 )
		 {
		    sprintf(tmpstr, "%.2f", crpix2);
		 }
		 else if (strcmp(field[ii], "ra") == 0 )
		 {
		    sprintf(tmpstr, "%f", new_center_ra);
		 }
		 else if (strcmp(field[ii], "dec") == 0 )
		 {
		    sprintf(tmpstr, "%f", new_center_dec);
		 }
		 else if (strcmp(field[ii], "cra") == 0 )
		 {
		    sprintf(tmpstr, "%s", out.clon);
		 }
		 else if (strcmp(field[ii], "cdec") == 0 )
		 {
		    sprintf(tmpstr, "%s", out.clat);
		 }
		 else if (strcmp(field[ii], "crval1") == 0 )
		 {
		    sprintf(tmpstr, "%f", crval1);
		 }
		 else if (strcmp(field[ii], "crval2") == 0 )
		 {
		    sprintf(tmpstr, "%f", crval2);
		 }
		 else if (strcmp(field[ii], "ra1") == 0 )
		 {
		    sprintf(tmpstr, "%f", point_ra[0]);
		 }
		 else if (strcmp(field[ii], "dec1") == 0 )
		 {
		    sprintf(tmpstr, "%f", point_dec[0]);
		 }
		 else if (strcmp(field[ii], "ra2") == 0 )
		 {
		    sprintf(tmpstr, "%f", point_ra[1]);
		 }
		 else if (strcmp(field[ii], "dec2") == 0 )
		 {
		    sprintf(tmpstr, "%f", point_dec[1]);
		 }
		 else if (strcmp(field[ii], "ra3") == 0 )
		 {
		    sprintf(tmpstr, "%f", point_ra[2]);
		 }
		 else if (strcmp(field[ii], "dec3") == 0 )
		 {
		    sprintf(tmpstr, "%f", point_dec[2]);
		 }
		 else if (strcmp(field[ii], "ra4") == 0 )
		 {
		    sprintf(tmpstr, "%f", point_ra[3]);
		 }
		 else if (strcmp(field[ii], "dec4") == 0 )
		 {
		    sprintf(tmpstr, "%f", point_dec[3]);
		 }
                 else
		 {
                    strcpy(tmpstr, tval(ifield[ii]));
		 }
		 strcpy(value[ii], tmpstr);

		 if (ii == 0)
	            sprintf(fmt, "%%%ds", tbl_rec[ifield[ii]].colwd-1);
		 else
	            sprintf(fmt, " %%%ds", tbl_rec[ifield[ii]].colwd-1);

	         fprintf(fout, fmt, value[ii]);

		 if (debug)
		 {
		   printf("Column %s, has value[%d] = %s\n", field[ii], ii, value[ii]);
		   fflush(stdout);
		 }
	      }

              fprintf(fout, " \n");

	      if(debug)
	      {
		printf("Record %d written to output\n", nrow);
		fflush(stdout);
	      }
	   }

           else
	      fprintf(fout, "%s\n", tbl_rec_string);

	    fflush(fout);
	    ++nimages;
	 } 
	 
	 continue;
      }

      else if(imode == CIRCLE)
      {
	 /* Bounding circle check */

	 center_dist = acos(Dot(&center, &image_center)) / dtr;

	 if(center_dist > circle_radius + image_box_radius)
	    continue;


	 /* Circle center inside image check */

	 interior = 1;

	 for(j=0; j<4; ++j)
	 {
	    jnext = (j+1)%4;

	    Cross(&image_corner[j], &image_corner[jnext], &image_normal[j]);

            Normalize(&image_normal[j]);

	    if(Dot(&image_normal[j], &center) < 0)
	    {
	       interior = 0;
	       break;
	    }
	 }

	 if(interior)
	 {
	    if(debug)
	    {
	       printf("\n******** CIRCLE Overlap (circle center in image) *********\n");
	       fflush(stdout);
	    }

	    fprintf(fout, "%s\n", tbl_rec_string);
	    fflush(fout);
	    ++nimages;

	    continue;
	 }


	 /* Image center inside circle check */

	 if(center_dist < circle_radius)
	 {
	    if(debug)
	    {
	       printf("\n******** CIRCLE Overlap (image center in circle) *********\n");
	       fflush(stdout);
	    }

	    fprintf(fout, "%s\n", tbl_rec_string);
	    fflush(fout);
	    ++nimages;

	    continue;
	 }


	 /* Point on circle closest to image center inside image */

	 scale_factor = sin(circle_radius * dtr)/sin((center_dist-circle_radius) * dtr);

	 edge.x = center.x + scale_factor * image_center.x;
	 edge.y = center.y + scale_factor * image_center.y;
	 edge.z = center.z + scale_factor * image_center.z;

	 Normalize(&edge);
	 

	 interior = 1;

	 for(j=0; j<4; ++j)
	 {
	    jnext = (j+1)%4;

	    Cross(&image_corner[j], &image_corner[jnext], &image_normal[j]);

            Normalize(&image_normal[j]);

	    if(Dot(&image_normal[j], &edge) < 0)
	    {
	       interior = 0;
	       break;
	    }
	 }

	 if(interior)
	 {
	    if(debug)
	    {
	       printf("\n******** CIRCLE Overlap (closest circle point in image) *********\n");
	       fflush(stdout);
	    }

	    fprintf(fout, "%s\n", tbl_rec_string);
	    fflush(fout);
	    ++nimages;

	    continue;
	 }
      }
   }

   fflush(fout);
   fclose(fout);

   fprintf(fstatus, "[struct stat=\"OK\", count=\"%d\"]\n", nimages);
   fflush(stdout);

   exit(0);
}


/****************************************************************************/
/*                                                                          */
/* SegSegIntersect()                                                        */
/*                                                                          */
/* Finds the point of intersection p between two closed                     */
/* segments ab and cd.  Returns p and a char with the following meaning:    */
/*                                                                          */
/*   COLINEAR_SEGMENTS: The segments colinearly overlap, sharing a point.   */
/*                                                                          */
/*   ENDPOINT_ONLY:     An endpoint (vertex) of one segment is on the other */
/*                      segment, but COLINEAR_SEGMENTS doesn't hold.        */
/*                                                                          */
/*   NORMAL_INTERSECT:  The segments intersect properly (i.e., they share   */
/*                      a point and neither ENDPOINT_ONLY nor               */
/*                      COLINEAR_SEGMENTS holds).                           */
/*                                                                          */
/*   NO_INTERSECTION:   The segments do not intersect (i.e., they share     */
/*                      no points).                                         */
/*                                                                          */
/* Note that two colinear segments that share just one point, an endpoint   */
/* of each, returns COLINEAR_SEGMENTS rather than ENDPOINT_ONLY as one      */
/* might expect.                                                            */
/*                                                                          */
/****************************************************************************/

int SegSegIntersect(Vec *pEdge, Vec *qEdge, 
                    Vec *p0, Vec *p1, Vec *q0, Vec *q1, 
                    Vec *intersect1, Vec *intersect2)
{
   double pDot,  qDot;  /* Dot product [cos(length)] of the edge vertices */
   double p0Dot, p1Dot; /* Dot product from vertices to intersection      */
   double q0Dot, q1Dot; /* Dot product from vertices to intersection      */
   int    len;


   /* Get the edge lengths (actually cos(length)) */

   pDot = Dot(p0, p1);
   qDot = Dot(q0, q1);


   /* Find the point of intersection */

   len = Cross(pEdge, qEdge, intersect1);

   Normalize(intersect1);


   /* If the two edges are colinear, */ 
   /* check to see if they overlap   */

   if(len == 0)
   {
      if(Between(q0, p0, p1)
      && Between(q1, p0, p1))
      {
         *intersect1 = *q0;
         *intersect2 = *q1;
         return COLINEAR_SEGMENTS;
      }

      if(Between(p0, q0, q1)
      && Between(p1, q0, q1))
      {
         *intersect1 = *p0;
         *intersect2 = *p1;
         return COLINEAR_SEGMENTS;
      }

      if(Between(q0, p0, p1)
      && Between(p1, q0, q1))
      {
         *intersect1 = *q0;
         *intersect2 = *p1;
         return COLINEAR_SEGMENTS;
      }

      if(Between(p0, q0, q1)
      && Between(q1, p0, p1))
      {
         *intersect1 = *p0;
         *intersect2 = *q1;
         return COLINEAR_SEGMENTS;
      }

      if(Between(q1, p0, p1)
      && Between(p1, q0, q1))
      {
         *intersect1 = *p0;
         *intersect2 = *p1;
         return COLINEAR_SEGMENTS;
      }

      if(Between(q0, p0, p1)
      && Between(p0, q0, q1))
      {
         *intersect1 = *p0;
         *intersect2 = *q0;
         return COLINEAR_SEGMENTS;
      }

      return NO_INTERSECTION;
   }


   /* If this is the wrong one of the two */
   /* (other side of the sky) reverse it  */

   if(Dot(intersect1, p0) < 0.)
      Reverse(intersect1);


   /* Point has to be inside both sides to be an intersection */

   if((p0Dot = Dot(intersect1, p0)) <  pDot) return NO_INTERSECTION;
   if((p1Dot = Dot(intersect1, p1)) <  pDot) return NO_INTERSECTION;
   if((q0Dot = Dot(intersect1, q0)) <  qDot) return NO_INTERSECTION;
   if((q1Dot = Dot(intersect1, q1)) <  qDot) return NO_INTERSECTION;


   /* Otherwise, if the intersection is at an endpoint */

   if(p0Dot == pDot) return ENDPOINT_ONLY;
   if(p1Dot == pDot) return ENDPOINT_ONLY;
   if(q0Dot == qDot) return ENDPOINT_ONLY;
   if(q1Dot == qDot) return ENDPOINT_ONLY;


   /* Otherwise, it is a normal intersection */

   return NORMAL_INTERSECT;
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
/* Between()                                       */
/*                                                 */
/* Tests whether whether a point on an arc is      */
/* between two other points.                       */
/*                                                 */
/***************************************************/

int Between(Vec *v, Vec *a, Vec *b)
{
   double abDot, avDot, bvDot;

   abDot = Dot(a, b);
   avDot = Dot(a, v);
   bvDot = Dot(b, v);

   if(avDot > abDot
   && bvDot > abDot)
      return 1;
   else
      return 0;
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


/***************************************************/
/*                                                 */
/* Reverse()                                       */
/*                                                 */
/* Reverse the vector                              */
/*                                                 */
/***************************************************/

void Reverse(Vec *v)
{
   v->x = -v->x;
   v->y = -v->y;
   v->z = -v->z;
}
