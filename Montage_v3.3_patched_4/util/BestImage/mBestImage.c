/* Module: mBestImage.c

Version  Developer        Date     Change
-------  ---------------  -------  -----------------------
1.2      John Good        05Oct07  Add check for lower case "url"
1.1      John Good        04Oct07  Corrected handling of WCS (no corner) data
1.0      John Good        14Feb05  Baseline code

*/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <math.h>

#include <mtbl.h>
#include <wcs.h>
#include <coord.h>

#define MAXSTR 256

int debugCheck(char *debugStr);
int checkWCS  (struct WorldCoor *wcs, int action);


/* Vector stuff */

typedef struct vec
{
   double x;
   double y;
   double z;
}
Vec;

int    Cross    (Vec *a, Vec *b, Vec *c);
double Dot      (Vec *a, Vec *b);
double Normalize(Vec *a);

int    stradd(char *header, char *card);

int debug = 0;


/*************************************************************************/
/*                                                                       */
/*  mBestImage                                                           */
/*                                                                       */
/*  Given a list of images and a position, determine which image covers  */
/*  the location "best" (i.e. the one where the position is farthest     */
/*  from the nearest edge).                                              */
/*                                                                       */
/*************************************************************************/

int main(int argc, char **argv)
{
   int    i, c, stat, ncols, inside;
   int    inext, nimages, corners;
   double xpos, ypos;
   double x0, y0, z0;
   double x, y, z, dist, mindist, bestdist, dtr;
   double ra, dec;
   double ra1, dec1;
   double ra2, dec2;
   double ra3, dec3;
   double ra4, dec4;
   int    index[4];

   char   tblfile [MAXSTR];
   char   bestURL [MAXSTR];
   char   bestName[MAXSTR];

   char   header[80000];
   char   temp[80];

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
   int    icd1_1;
   int    icd1_2;
   int    icd2_1;
   int    icd2_2;
   int    ira1, idec1;
   int    ira2, idec2;
   int    ira3, idec3;
   int    ira4, idec4;
   int    iurl;
   int    ifname;

   /* Basic image WCS information    */
   /* (from the FITS header and as   */
   /* returned from the WCS library) */

   struct WorldCoor *wcs;
   int               equinox;
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
   double            cd1_1;
   double            cd1_2;
   double            cd2_1;
   double            cd2_2;
   Vec               center;
   Vec               corner[4];
   Vec               normal[4];
   double            maxRadius;
   char              url  [MAXSTR];
   char              fname[MAXSTR];

   Vec   point;
   char *fileName();

   FILE *fstatus;

   dtr = atan(1.)/45.;


   /***************************************/
   /* Process the command-line parameters */
   /***************************************/

   debug  = 0;

   fstatus = stdout;

   for(i=0; i<argc; ++i)
   {
      if(strcmp(argv[i], "-d") == 0)
      {
         debug = 1;
	 ++argv;
	 --argc;
      }
   }

   if (argc < 3) 
   {
      printf("[struct stat=\"ERROR\", msg=\"Usage: mBestImages [-d] images.tbl ra dec\"]\n");
      exit(1);
   }

   strcpy(tblfile, argv[1]);

   ra  = atof(argv[2]);
   dec = atof(argv[3]);

   x0 = cos(dec*dtr) * cos(ra*dtr);
   y0 = cos(dec*dtr) * sin(ra*dtr);
   z0 = sin(dec*dtr);

   point.x = x0;
   point.y = y0;
   point.z = z0;


   /*********************************************/ 
   /* Open the image header metadata table file */
   /*********************************************/ 

   ncols = topen(tblfile);

   if(ncols <= 0)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Invalid image metadata file: %s\"]\n",
         tblfile);
      exit(1);
   }

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
   icd1_1   = tcol("cd1_1");
   icd1_2   = tcol("cd1_2");
   icd2_1   = tcol("cd2_1");
   icd2_2   = tcol("cd2_2");
   iepoch   = tcol("epoch");
   ira1     = tcol("ra1");
   idec1    = tcol("dec1");
   ira2     = tcol("ra2");
   idec2    = tcol("dec2");
   ira3     = tcol("ra3");
   idec3    = tcol("dec3");
   ira4     = tcol("ra4");
   idec4    = tcol("dec4");
   ifname   = tcol("fname");
   iurl     = tcol("URL");

   if(ins < 0)
      ins = tcol("naxis1");

   if(inl < 0)
      inl = tcol("naxis2");

   if(ifname < 0)
      ifname = tcol("file");

   if(iurl < 0)
      iurl = tcol("url");

   corners = 0;

   if(ira1 >= 0 && idec1 >= 0
   && ira2 >= 0 && idec2 >= 0
   && ira3 >= 0 && idec3 >= 0
   && ira4 >= 0 && idec4 >= 0)
   {
      corners = 1;

      if(debug)
      {
	 printf("\nUsing corners columns from table\n\n");
	 fflush(stdout);
      }
   }
   else
   {
      if(debug)
      {
	 printf("\nUsing WCS keyword columns from table\n\n");
	 fflush(stdout);
      }
   }

   if(ifname  < 0)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Need columns: fname ctype1 ctype2 nl ns crval1 crval2 crpix1 crpix2 cdelt1 cdelt2 and crota2 or cd matrix / ra dec ra1 ... dec4\"]\n");
      exit(1);
   }

   if(ictype1 < 0 || ictype2 < 0 || inl < 0 || ins < 0 
   || icrval1 < 0 || icrval2 < 0 || icrpix1 < 0 || icrpix2 < 0)
   {
      if(!corners)
      {
	 fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Need columns: fname ctype1 ctype2 nl ns crval1 crval2 crpix1 crpix2 cdelt1 cdelt2 and crota2 or cd matrix / ra dec ra1 ... dec4\"]\n");
	 exit(1);
      }
   }

   if(!corners
   && !(   (  icdelt1 >= 0 && icdelt2 >= 0 && icrota2 >= 0)
        || (   icd1_1 >= 0 && icd1_2  >= 0 && icd1_1  >= 0 && icd1_2 >= 0)))
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Need columns: fname ctype1 ctype2 nl ns crval1 crval2 crpix1 crpix2 cdelt1 cdelt2 and crota2 or cd matrix / ra dec ra1 ... dec4\"]\n");
      exit(1);
   }



   /***********************************/ 
   /* Read the projection information */ 
   /***********************************/ 

   nimages = 0;

   strcpy(bestName, "No name");
   strcpy(bestURL,  "No URL");

   while(1)
   {
      stat = tread();

      if(stat < 0)
	 break;

      ++nimages;

      if(debug)
      {
	 printf("\n-----------------\nReading image table record %d\n", nimages);
	 fflush(stdout);
      }

      if(ictype1 >= 0) strcpy(ctype1, tval(ictype1));
      if(ictype2 >= 0) strcpy(ctype2, tval(ictype2));

      if(ins     >= 0) naxis1    = atoi(tval(ins));
      if(inl     >= 0) naxis2    = atoi(tval(inl));

      if(icrpix1 >= 0) crpix1    = atof(tval(icrpix1));
      if(icrpix2 >= 0) crpix2    = atof(tval(icrpix2));

      if(icrval1 >= 0) crval1    = atof(tval(icrval1));
      if(icrval2 >= 0) crval2    = atof(tval(icrval2));

      if(icdelt1 >= 0) cdelt1 = atof(tval(icdelt1));
      if(icdelt2 >= 0) cdelt2 = atof(tval(icdelt2));
      if(icrota2 >= 0) crota2 = atof(tval(icrota2));

      if(icd1_1  >= 0) cd1_1  = atof(tval(icd1_1));
      if(icd1_2  >= 0) cd1_2  = atof(tval(icd1_2));
      if(icd2_1  >= 0) cd2_1  = atof(tval(icd2_1));
      if(icd2_2  >= 0) cd2_2  = atof(tval(icd2_2));

      if(ira1    >= 0) ra1    = atof(tval(ira1));
      if(idec1   >= 0) dec1   = atof(tval(idec1));
      if(ira2    >= 0) ra2    = atof(tval(ira2));
      if(idec2   >= 0) dec2   = atof(tval(idec2));
      if(ira3    >= 0) ra3    = atof(tval(ira3));
      if(idec3   >= 0) dec3   = atof(tval(idec3));
      if(ira4    >= 0) ra4    = atof(tval(ira4));
      if(idec4   >= 0) dec4   = atof(tval(idec4));

      equinox   = 2000;
      maxRadius = 0.;

      if(iequinox >= 0)
	 equinox = atoi(tval(iequinox));

      strcpy(fname, tval(ifname));

      if(iurl >= 0) 
	 strcpy(url, tval(iurl));

      if(corners)
      {
	 x = cos(dec1*dtr) * cos(ra1*dtr);
	 y = cos(dec1*dtr) * sin(ra1*dtr);
	 z = sin(dec1*dtr);

	 corner[3].x = x;
	 corner[3].y = y;
	 corner[3].z = z;

	 x = cos(dec2*dtr) * cos(ra2*dtr);
	 y = cos(dec2*dtr) * sin(ra2*dtr);
	 z = sin(dec2*dtr);

	 corner[2].x = x;
	 corner[2].y = y;
	 corner[2].z = z;

	 x = cos(dec3*dtr) * cos(ra3*dtr);
	 y = cos(dec3*dtr) * sin(ra3*dtr);
	 z = sin(dec3*dtr);

	 corner[1].x = x;
	 corner[1].y = y;
	 corner[1].z = z;

	 x = cos(dec4*dtr) * cos(ra4*dtr);
	 y = cos(dec4*dtr) * sin(ra4*dtr);
	 z = sin(dec4*dtr);

	 corner[0].x = x;
	 corner[0].y = y;
	 corner[0].z = z;


	 x = corner[0].x + corner[1].x + corner[2].x + corner[3].x;
	 y = corner[0].y + corner[1].y + corner[2].y + corner[3].y;
	 z = corner[0].z + corner[1].z + corner[2].z + corner[3].z;

	 center.x = x;
	 center.y = y;
	 center.z = z;

	 Normalize(&center);

	 for(i=0; i<4; ++i)
	 {
	    dist = acos(corner[i].x*center.x 
		      + corner[i].y*center.y 
		      + corner[i].z*center.z) / dtr;

	    if(dist > maxRadius) 
	       maxRadius = dist;
	 }
      }
      else
      {
	 strcpy(header, "");
	 sprintf(temp, "SIMPLE  = T"              ); stradd(header, temp);
	 sprintf(temp, "BITPIX  = -64"            ); stradd(header, temp);
	 sprintf(temp, "NAXIS   = 2"              ); stradd(header, temp);
	 sprintf(temp, "NAXIS1  = %d",     naxis1 ); stradd(header, temp);
	 sprintf(temp, "NAXIS2  = %d",     naxis2 ); stradd(header, temp);
	 sprintf(temp, "CTYPE1  = '%s'",   ctype1 ); stradd(header, temp);
	 sprintf(temp, "CTYPE2  = '%s'",   ctype2 ); stradd(header, temp);
	 sprintf(temp, "CRVAL1  = %11.6f", crval1 ); stradd(header, temp);
	 sprintf(temp, "CRVAL2  = %11.6f", crval2 ); stradd(header, temp);
	 sprintf(temp, "CRPIX1  = %11.6f", crpix1 ); stradd(header, temp);
	 sprintf(temp, "CRPIX2  = %11.6f", crpix2 ); stradd(header, temp);

	 if(icdelt1 >= 0)
	    {sprintf(temp, "CDELT1  = %11.6f", cdelt1 ); stradd(header, temp);}

	 if(icdelt2 >= 0)
	    {sprintf(temp, "CDELT2  = %11.6f", cdelt2 ); stradd(header, temp);}

	 if(icrota2 >= 0)
	    {sprintf(temp, "CROTA2  = %11.6f", crota2 ); stradd(header, temp);}

	 if(icd1_1  >= 0)
	    {sprintf(temp, "CD1_1   = %11.6f", cd1_1  ); stradd(header, temp);}

	 if(icd1_2  >= 0)
	    {sprintf(temp, "CD1_2   = %11.6f", cd1_2  ); stradd(header, temp);}

	 if(icd2_1  >= 0)
	    {sprintf(temp, "CD2_1   = %11.6f", cd2_1  ); stradd(header, temp);}

	 if(icd2_2  >= 0)
	    {sprintf(temp, "CD2_2   = %11.6f", cd2_2  ); stradd(header, temp);}

	 sprintf(temp, "EQUINOX = %d",     equinox); stradd(header, temp);
	 sprintf(temp, "END"                      ); stradd(header, temp);
	 
	 if(debug)
	 {
	    printf("header:\n%s\n\n", header);
	    fflush(stdout);
	 }

	 wcs = wcsinit(header);

	 checkWCS(wcs, 0);
				
	 if(wcs == (struct WorldCoor *)NULL)
	 {
	    fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Bad WCS for image %d\"]\n", 
	       nimages);
	    exit(1);
	 }


	 /* We need to get the corners in "clockwise" order */

	 if((wcs->xinc < 0 && wcs->yinc < 0)
	 || (wcs->xinc > 0 && wcs->yinc > 0))
	 {
	    index[0] = 0;
	    index[1] = 1;
	    index[2] = 2;
	    index[3] = 3;
	 }
	 else
	 {
	    index[0] = 3;
	    index[1] = 2;
	    index[2] = 1;
	    index[3] = 0;
	 }


	 /* Coordinates of center */

	 pix2wcs(wcs, naxis1/2., naxis2/2., 
	    &xpos, &ypos);
	 
	 x0 = cos(ypos*dtr) * cos(xpos*dtr);
	 y0 = cos(ypos*dtr) * sin(xpos*dtr);
	 z0 = sin(ypos*dtr);

	 center.x = x0;
	 center.y = y0;
	 center.z = z0;

	 
	 /* Lower left */

	 pix2wcs(wcs, 0.5, 0.5, &xpos, &ypos);

	 x = cos(ypos*dtr) * cos(xpos*dtr);
	 y = cos(ypos*dtr) * sin(xpos*dtr);
	 z = sin(ypos*dtr);

	 corner[index[0]].x = x;
	 corner[index[0]].y = y;
	 corner[index[0]].z = z;

	 dist = acos(x*x0 + y*y0 + z*z0) / dtr;

	 if(dist > maxRadius) 
	    maxRadius = dist;


	 /* Lower right */

	 pix2wcs(wcs, naxis1+0.5, 0.5, &xpos, &ypos);

	 x = cos(ypos*dtr) * cos(xpos*dtr);
	 y = cos(ypos*dtr) * sin(xpos*dtr);
	 z = sin(ypos*dtr);

	 corner[index[1]].x = x;
	 corner[index[1]].y = y;
	 corner[index[1]].z = z;

	 dist = acos(x*x0 + y*y0 + z*z0) / dtr;

	 if(dist > maxRadius) 
	    maxRadius = dist;

	 
	 /* Upper right */
	 pix2wcs(wcs, naxis1+0.5, 
		 naxis2+0.5, &xpos, &ypos);

	 x = cos(ypos*dtr) * cos(xpos*dtr);
	 y = cos(ypos*dtr) * sin(xpos*dtr);
	 z = sin(ypos*dtr);

	 corner[index[2]].x = x;
	 corner[index[2]].y = y;
	 corner[index[2]].z = z;

	 dist = acos(x*x0 + y*y0 + z*z0) / dtr;

	 if(dist > maxRadius) 
	    maxRadius = dist;

	 
	 /* Upper left */

	 pix2wcs(wcs, 0.5, naxis2+0.5, &xpos, &ypos);

	 x = cos(ypos*dtr) * cos(xpos*dtr);
	 y = cos(ypos*dtr) * sin(xpos*dtr);
	 z = sin(ypos*dtr);

	 corner[index[3]].x = x;
	 corner[index[3]].y = y;
	 corner[index[3]].z = z;

	 dist = acos(x*x0 + y*y0 + z*z0) / dtr;

	 if(dist > maxRadius) 
	    maxRadius = dist;
      }


      /* Normals to the image "sides" */

      for(i=0; i<4; ++i)
      {
	 inext = (i+1)%4;

	 Cross(&corner[i], &corner[inext], &normal[i]);

	 Normalize(&normal[i]);
      }


      /**************************************************/
      /* Check this image against the point of interest */
      /**************************************************/

      /* Check to see if the point of interest is inside */
      /* the bounding radius circle (abandon if not)     */

      dist = acos(Dot(&point, &center)) / dtr;

      if(debug)
      {
	 printf("\nChecking image %d (%s) center: (%-g,%-g,%-g) against point: (%-g,%-g,%-g)\n",
	    nimages,
	    fname,
	    center.x,
	    center.y,
	    center.z,
	    point.x,
	    point.y,
	    point.z);

	 printf("  dist = %-g < %-g ?\n", 
	    dist, maxRadius);

	 fflush(stdout);
      }

      if(dist > maxRadius)
	 continue;


      /* Location inside image check             */
      /* Keep track of the minimum edge distance */

      inside = 1;

      for(i=0; i<4; ++i)
      {
	 if(debug)
	 {
	    printf("\nChecking image side %d: (%-g,%-g,%-g) against point: (%-g,%-g,%-g)\n",
	       i,
	       normal[i].x,
	       normal[i].y,
	       normal[i].z,
	       point.x,
	       point.y,
	       point.z);

	    fflush(stdout);
	 }

	 if(Dot(&normal[i], &point) < 0)
	 {
	    if(debug)
	    {
	       printf("Outside side %d\n", i);
	       fflush(stdout);
	    }
	    inside = 0;
	    break;
	 }

	 dist = 90. - acos(Dot(&normal[i], &point)) / dtr;

	 if(debug)
	 {
	    printf("Side %d distance = %-g\n", i, dist);
	    fflush(stdout);
	 }

	 if(i == 0 || dist < mindist)
	    mindist = dist;
      }

      if(!inside)
	 continue;

      else if(debug)
      {
	 printf("Min dist = %-g\n", mindist);
	 fflush(stdout);
      }


      /* If it passed, see whether this image is a new "best" */

      if(mindist > bestdist)
      {
	 bestdist = mindist;

	 strcpy(bestName, fname);

	 if(iurl >= 0)
	    strcpy(bestURL, url);

	 if(debug)
	 {
	    printf("\nNew best file: %s\n", bestName);

	    if(iurl >= 0)
	       printf("New best url:  %s\n", bestURL);
	       
	    printf("\n");
	    fflush(stdout);
	 }
      }
   }

   if(strcmp(bestName, "No name") == 0)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"No image covers this point\"]\n");
      fflush(fstatus);
      exit(0);
   }

   if(iurl >= 0)
      fprintf(fstatus, "[struct stat=\"OK\", file=\"%s\", url=\"%s\", edgedist=%.6f]\n",
	 bestName, bestURL, bestdist);
   else
      fprintf(fstatus, "[struct stat=\"OK\", file=\"%s\", edgedist=%.6f]\n",
	 bestName, bestdist);

   fflush(fstatus);
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
