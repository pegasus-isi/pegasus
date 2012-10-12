#include <stdio.h>
#include <stdlib.h>
#include <math.h>

#include <boundaries.h>

int debugLevel = 0;

double tolerance = 4.848137e-10;  /* sin(x) where x = 0.001 arcsec */

double bndProjMatrix  [3][3];
double bndDeprojMatrix[3][3];

double bndXpix, bndYpix;
double bndLon,  bndLat;

struct bndSkyLocation *bndPoints;

double bndDTR, bndPI;

struct bndSkyLocation Centroid;
struct bndSkyLocation bndCorner1, bndCorner2, bndCorner3, bndCorner4, bndCenter;
double bndSize, bndSize1, bndSize2, bndRadius;
double bndAngle;

int bndNpoints;
int bndNdelete;
int bndDebug;

static void bndTANsetup(double, double, double);
static void bndTANproj(double, double);
static void bndTANdeproj(double, double);





void bndSetDebug(int debug)
{
   debugLevel = debug;
}


struct bndInfo *bndVerticalBoundingBox(int npts, double *lon, double *lat)
{
   int status;
   struct bndInfo *retval;

   if(npts < 3)
      return (struct bndInfo *)NULL;

   status = bndBoundaries(npts, lon, lat, 0);

   if(status < 0)
      return (struct bndInfo *)NULL;
   
   if(bndDebug >= 1)
      bndDrawBox();

   retval = (struct bndInfo *)malloc(sizeof(struct bndInfo));

   retval->cornerLon[0] = bndCorner1.lon;
   retval->cornerLat[0] = bndCorner1.lat;
   retval->cornerLon[1] = bndCorner2.lon;
   retval->cornerLat[1] = bndCorner2.lat;
   retval->cornerLon[2] = bndCorner3.lon;
   retval->cornerLat[2] = bndCorner3.lat;
   retval->cornerLon[3] = bndCorner4.lon;
   retval->cornerLat[3] = bndCorner4.lat;
   retval->centerLon    = bndCenter.lon;
   retval->centerLat    = bndCenter.lat;
   retval->lonSize      = bndSize1;
   retval->latSize      = bndSize2;
   retval->posAngle     = bndAngle;

   return retval;
}


struct bndInfo *bndBoundingBox(int npts, double *lon, double *lat)
{
   int status;
   struct bndInfo *retval;

   if(npts < 3)
      return (struct bndInfo *)NULL;

   status = bndBoundaries(npts, lon, lat, 1);

   if(status < 0)
      return (struct bndInfo *)NULL;

   if(bndDebug >= 1)
      bndDrawBox();

   retval = (struct bndInfo *)malloc(sizeof(struct bndInfo));

   retval->cornerLon[0] = bndCorner1.lon;
   retval->cornerLat[0] = bndCorner1.lat;
   retval->cornerLon[1] = bndCorner2.lon;
   retval->cornerLat[1] = bndCorner2.lat;
   retval->cornerLon[2] = bndCorner3.lon;
   retval->cornerLat[2] = bndCorner3.lat;
   retval->cornerLon[3] = bndCorner4.lon;
   retval->cornerLat[3] = bndCorner4.lat;
   retval->centerLon    = bndCenter.lon;
   retval->centerLat    = bndCenter.lat;
   retval->lonSize      = bndSize1;
   retval->latSize      = bndSize2;
   retval->posAngle     = bndAngle;

   return retval;
}


struct bndInfo *bndBoundingCircle(int npts, double *lon, double *lat)
{
   int status;
   struct bndInfo *retval;

   if(npts < 3)
      return (struct bndInfo *)NULL;

   status = bndBoundaries(npts, lon, lat, 2);

   if(status < 0)
      return (struct bndInfo *)NULL;

   if(bndDebug >= 1)
      bndDrawCircle();

   retval = (struct bndInfo *)malloc(sizeof(struct bndInfo));

   retval->centerLon    = bndCenter.lon;
   retval->centerLat    = bndCenter.lat;
   retval->radius       = bndRadius;

   return retval;
}


int bndBoundaries(int npts, double *lon, double *lat, int flag)
{
   int i;

   static struct bndStackCell *top;

   bndNpoints = 0;
   bndDebug   = debugLevel;

   bndDTR = atan(1.)/45.;
   bndPI  = atan(1.)*4.;

   bndPoints = (struct bndSkyLocation *)
		malloc(npts * sizeof(struct bndSkyLocation));

   if(!bndPoints)
      return(-1);

   bndNpoints = npts;

   if(bndDebug >= 2)
   {
      printf("\nInput points:\n");
      fflush(stdout);
   }

   for(i=0; i<bndNpoints; ++i) 
   {
      if(bndDebug >= 2)
      {
	 printf("%25.20f %25.20f\n", lon[i], lat[i]);
	 fflush(stdout);
      }

      bndPoints[i].lon = lon[i];
      bndPoints[i].lat = lat[i];

      bndPoints[i].x = cos(bndPoints[i].lon*bndDTR) 
		     * cos(bndPoints[i].lat*bndDTR);

      bndPoints[i].y = sin(bndPoints[i].lon*bndDTR) 
		     * cos(bndPoints[i].lat*bndDTR);

      bndPoints[i].z = sin(bndPoints[i].lat*bndDTR);

      bndPoints[i].vnum = i;
   }

   bndInitialize();

   if(bndDebug >= 2)
      PrintSkyPoints();

   if(bndDebug >= 1)
      bndDrawSkyPoints();

   qsort(
      &bndPoints[1],                 /* pointer to 1st elem      */
      bndNpoints-1,                  /* number of elems          */
      sizeof(struct bndSkyLocation), /* Size of each elem        */
      bndCompare                     /* -1,0,+1 compare function */
   );

   if(bndDebug >= 2)
   {
      printf("\nAfter sorting:\n");
      PrintSkyPoints();
   }

   if(bndNdelete > 0)
   {
      bndRemoveDeleted();

      if(bndDebug >= 2)
      {
	 printf("\nAfter deleting 'duplicates':\n");
	 PrintSkyPoints();
      }
   }

   top = bndGraham();

   if(top == (struct bndStackCell *)NULL)
      return(-1);
   
   if(bndDebug >= 2)
   {
      printf("\n-----------------------------\nFinal hull polygon:\n");

      bndPrintStack(top);
   }

   if(bndDebug >= 1)
      bndDrawOutline(top);

   if(flag == 0)
      bndComputeVerticalBoundingBox(top);

   else if(flag == 1)
      bndComputeBoundingBox(top);

   else if(flag == 2)
      bndComputeBoundingCircle(top);

   else if(flag == 3)
   {
      /* Do nothing (we just wanted the bounding polygon */
   }

   else 
   {
      bndFree(top);
      return(-1);
   }
   
   bndFree(top);
   return(0);
}


void bndFree(struct bndStackCell *t)
{
   struct bndStackCell *next;

   if(bndNpoints > 0)
      free((char *)bndPoints);

   while(t) 
   { 
      next = t->next;
      free(t);
      t = next;
   }

   t = NULL;
}


void bndInitialize(void)
{
   struct bndSkyLocation refDir, dir, angVec;

   int    i, imin, itmp;
   double dot, dotmin, len, xtmp;

   double xsum, ysum, zsum;

   bndNdelete = 0;

   xsum = 0;
   ysum = 0;
   zsum = 0;


   /* Find the centroid location */

   for (i = 0; i < bndNpoints; ++i)
   {
      xsum += bndPoints[i].x;
      ysum += bndPoints[i].y;
      zsum += bndPoints[i].z;
   }

   len = sqrt(xsum*xsum + ysum*ysum + zsum*zsum);

   xsum = xsum/len;
   ysum = ysum/len;
   zsum = zsum/len;

   Centroid.x = xsum;
   Centroid.y = ysum;
   Centroid.z = zsum;

   Centroid.lon = atan2(Centroid.y, Centroid.x) / bndDTR; 
   Centroid.lat = asin(Centroid.z) / bndDTR; 

   while(Centroid.lon >= 360.) Centroid.lon -= 360.;
   while(Centroid.lon <    0.) Centroid.lon += 360.;

   if(bndDebug >= 2)
   {
      printf("\nCentroid:\n"); 
      printf("x = %13.5e\n", Centroid.x); 
      printf("y = %13.5e\n", Centroid.y); 
      printf("z = %13.5e\n", Centroid.z); 

      printf("lon = %11.6f\n",   Centroid.lon);
      printf("lat = %11.6f\n\n", Centroid.lat);
   }


   /* Then find the point farthest from the     */
   /* centroid (our reference point ("point 0") */

   dotmin = 1.;
   imin   = 0;

   for (i = 0; i < bndNpoints; ++i)
   {
      dot = bndPoints[i].x * xsum
          + bndPoints[i].y * ysum
          + bndPoints[i].z * zsum;

      if(dot < dotmin)
      {
	 dotmin = dot;
	 imin   = i;
      }
   }

   bndSize = acos(dotmin) / bndDTR;


   /* Swap the reference point and the original point 0 */

   xtmp                = bndPoints[0].lon;
   bndPoints[0].lon       = bndPoints[imin].lon;
   bndPoints[imin].lon    = xtmp;

   xtmp                = bndPoints[0].lat;
   bndPoints[0].lat       = bndPoints[imin].lat;
   bndPoints[imin].lat    = xtmp;

   xtmp                = bndPoints[0].x;
   bndPoints[0].x         = bndPoints[imin].x;
   bndPoints[imin].x      = xtmp;

   xtmp                = bndPoints[0].y;
   bndPoints[0].y         = bndPoints[imin].y;
   bndPoints[imin].y      = xtmp;

   xtmp                = bndPoints[0].z;
   bndPoints[0].z         = bndPoints[imin].z;
   bndPoints[imin].z      = xtmp;

   itmp                = bndPoints[0].vnum;
   bndPoints[0].vnum      = bndPoints[imin].vnum;
   bndPoints[imin].vnum   = itmp;

   bndPoints[0].ang       = -1.;
   bndPoints[0].delete    =  0;

   /* Use the centroid and point 0 to define */
   /* reference direction (for sorting)      */

   bndCross(&bndPoints[0], &Centroid, &refDir);
   bndNormalize(&refDir);


   /* Now determine an angle measurement for */
   /* all the bndPoints (the point i - point 0) */
   /* direction vector relative to the       */
   /* reference direction                    */

   for (i = 1; i < bndNpoints; ++i)
   {
      bndPoints[i].delete = 0;

      if(bndEqual(&bndPoints[0], &bndPoints[i]))
      {
	 bndPoints[i].ang    = 0.;
	 bndPoints[i].delete = 1;
	 ++bndNdelete;
	 continue;
      }

      bndCross(&bndPoints[0], &bndPoints[i], &dir);
      bndNormalize(&dir);

      bndCross(&refDir, &dir, &angVec);

      bndPoints[i].ang = bndNormalize(&angVec);

      if(bndDot(&bndPoints[0], &angVec) < 0.)
	 bndPoints[i].ang = -bndPoints[i].ang;
   }
}


int bndCompare(const void *tpi, const void *tpj)
{
   double measure;

   struct bndSkyLocation *pi, *pj;

   pi = (struct bndSkyLocation *)tpi;
   pj = (struct bndSkyLocation *)tpj;

   measure = bndDot(pi, &bndPoints[0]) - bndDot(pj, &bndPoints[0]);

   if(bndDebug >= 3)
   {
      printf("\n");
      printf("pi->vnum = %d\n", pi->vnum);
      printf("pj->vnum = %d\n", pj->vnum);
      printf("pi->ang  = %20.15f\n", pi->ang);
      printf("pj->ang  = %20.15f\n", pj->ang);
      printf("measure  = %20.15f\n", measure);
      fflush(stdout);
   }


   /* If the two angles are unambiguously different,  */
   /* return 1 if the first is larger than the second */
   /* or -1 if the second is larger than the first    */

   if(bndDebug >= 3)
   {
      if(pi->ang > pj->ang) printf("Greater\n");
      if(pi->ang < pj->ang) printf("Less\n");
      fflush(stdout);
   }

   if(pi->ang > pj->ang) return(1);
   if(pi->ang < pj->ang) return(-1);


   /* If the two angles are the same (i.e. the points are */
   /* colinear as seen from the 'first' point), see which */
   /* is farther from the first point and return a value  */
   /* indicating that it is 'larger' ('delete' the other) */

   if(measure > tolerance)
   {
      pj->delete = 1;

      if(bndDebug >= 3)
      {
	 printf("Delete pj (%d)\n", pj->vnum);
	 printf("Less\n");
	 fflush(stdout);
      }

      ++bndNdelete;

      return(-1);
   }
   else if(measure < -tolerance)
   {
      pi->delete = 1;

      if(bndDebug >= 3)
      {
	 printf("Delete pi (%d)\n", pi->vnum);
	 printf("Greater\n");
	 fflush(stdout);
      }

      ++bndNdelete;

      return( 1);
   }
   else
   {
      if(pi->vnum > pj->vnum)
      {
	 pj->delete = 1;

	 if(bndDebug >= 3)
	 {
	    printf("Delete pj (%d)\n", pj->vnum);
	    printf("Greater\n");
	    fflush(stdout);
	 }

	 ++bndNdelete;
	 return 1;
      }

      else if(!pi->delete)
      {
	 pi->delete = 1;

	 if(bndDebug >= 3)
	 {
	    printf("Delete pi (%d)\n", pi->vnum);
	    fflush(stdout);
	 }

	 ++bndNdelete;
      }

      if(bndDebug >= 3)
      {
	 printf("Equal\n");
	 fflush(stdout);
      }

      return 0;
   }
}



void bndRemoveDeleted()
{
   int i, j;

   i = 0; j = 0;

   while(i < bndNpoints) 
   {
      if(!bndPoints[i].delete)
      {
         bndCopy(&bndPoints[i], &bndPoints[j]); 
         j++;
      }
      
      i++;
   }
   bndNpoints = j;
}



struct bndStackCell *bndPop(struct bndStackCell *s)
{
   struct bndStackCell *top;

   top = s->next;

   if(s)
   { 
      free((char *)s);
      s = NULL; 
   }

   return top;
}



struct bndStackCell *bndPush(struct bndSkyLocation *p, 
			     struct bndStackCell *top)
{
   struct bndStackCell *s;

   if((s=(struct bndStackCell *) malloc (sizeof(struct bndStackCell))) == NULL)
   {
      printf ("Out of Memory!\n");
      exit(1);
   }

   s->p    = p;
   s->next = top;

   return s;
}



void bndPrintStack(struct bndStackCell *t)
{
   if (!t) printf("Empty stack\n");

   while(t) 
   { 
      printf("vnum=%4d    lon=%11.6f  lat=%11.6f    x=%9.7f  y=%9.7f  z=%9.7f\n", 
             t->p->vnum, t->p->lon, t->p->lat, t->p->x, t->p->y, t->p->z); 

      t = t->next;
   }
}



void bndComputeBoundingBox(struct bndStackCell *t)
{
   struct bndStackCell *first    = (struct bndStackCell *)NULL;
   struct bndStackCell *current  = (struct bndStackCell *)NULL;
   struct bndStackCell *next     = (struct bndStackCell *)NULL;
   struct bndStackCell *loop     = (struct bndStackCell *)NULL;
   struct bndStackCell *mainloop = (struct bndStackCell *)NULL;

   struct bndSkyLocation *Pi;
   struct bndSkyLocation *P1, *P2;
   struct bndSkyLocation *E1, *E2;
   struct bndSkyLocation R1, R2;
   struct bndSkyLocation N1, N2;
   struct bndSkyLocation I1, I2;
   struct bndSkyLocation M1, M2;
   struct bndSkyLocation N, R;
   struct bndSkyLocation C, T;
   struct bndSkyLocation Z;
   struct bndSkyLocation tmp;

   double sina, cosa;
   double a, b, c;
   double amin, amax;
   double len, lenmin;
   double cosalpha;
   double Xmin, Xmax;
   double Ymin, Ymax;

   double area, minArea;
   double size1, size2;
   double angle;
   double stmp;

   int    counter;



   /* Check each border segment to see it it would   */
   /* give us the smallest area for our bounding box */

   Z.x = 0.;
   Z.y = 0.;
   Z.z = 1.;

   first   = t;
   counter = 0;

   minArea = 1.e99;

   mainloop = t;

   while(mainloop) 
   { 
      if(bndDebug >= 2)
      {
	 printf("-----------------------------------------\n");
	 printf("Counter %d:\n", counter);
	 fflush(stdout);
      }

      ++counter;

      current  = mainloop;
      mainloop = mainloop->next;
      next     = mainloop;

      if(!next)
	 next = first;


      /* Find plane defined by pair of points */

      if(bndDebug >= 2)
      {
	 printf("Segment %d to %d\n", current->p->vnum, next->p->vnum);
	 fflush(stdout);
      }

      P1 = current->p;
      P2 = next->p;

      bndCross(P1, P2, &N1);
      bndNormalize(&N1);


      /* Find the pair of points on the polygon      */
      /* which are the farthest away from each other */
      /* "along" the arc defined by P1-P2            */

      loop = first;

      amin =  999.;
      amax = -999.;

      while(loop)
      {
	 Pi = loop->p;

	 bndCross(Pi, &N1, &R);

	 bndNormalize(&R);

	 cosa = bndDot(&R, P1);

	 bndCross(&R, P1, &tmp);

	 sina = bndNormalize(&tmp);

	 a = atan2(sina, cosa);

	 if(a<amin)
	 {
	    amin = a;
	    E1 = Pi;
	    bndCopy(&R, &R1);
	 }

	 if(a>amax)
	 {
	    amax = a;
	    E2 = Pi;
	    bndCopy(&R, &R2);
	 }

	 loop = loop->next;
      }

      if(bndDebug >= 2)
      {
	 printf("amin = %13.5f\n", amin);
	 printf("amax = %13.5f\n", amax);
	 fflush(stdout);
      }


      /* Find the points on the P1-P2 arc corresponding */
      /* to these maxima and the point half way between */

      bndCross(&N1, &R1, &I1);
      bndNormalize(&I1); 

      bndCross(&N1, &R2, &I2);
      bndNormalize(&I2); 

      bndAdd(&I1, &I2, &M1);
      bndNormalize(&M1); 

      M1.lon = atan2(M1.y, M1.x) / bndDTR;
      M1.lat = asin(M1.z) / bndDTR;

      while(M1.lon >= 360.) M1.lon -= 360.;
      while(M1.lon <    0.) M1.lon += 360.;

      if(bndDebug >= 2)
      {
	 printf("\nM1:\n\n");
	 printf("%13.6f %13.6f\n", M1.lon, M1.lat);
	 fflush(stdout);
      }


      /* Check all points to see which represents */
      /* the plane farthest from N1 along a line  */
      /* through M1                               */

      loop = first;

      lenmin = 999.;

      while(loop)
      {
	 Pi = loop->p;

	 bndCross(&M1, &N1, &tmp);
	 bndNormalize(&tmp);

	 bndCross(&tmp, Pi, &N);
	 bndNormalize(&N);

	 len = bndDot(&N1, &N);

	 if(len < lenmin)
	 {
	    lenmin = len;

	    bndCopy(&N, &N2);
	 }

	 loop = loop->next;
      }


      /* Find the point on this plane along the */
      /* along the line N1 - M1                 */

      bndCross(&M1, &N1, &tmp);
      bndNormalize(&tmp);

      bndCross(&N2, &tmp, &M2);
      bndNormalize(&M2);


      /* The region center is half way between */
      /* M1 and M2                             */

      M2.lon = atan2(M2.y, M2.x) / bndDTR;
      M2.lat = asin(M2.z) / bndDTR;

      while(M2.lon >= 360.) M2.lon -= 360.;
      while(M2.lon <    0.) M2.lon += 360.;

      if(bndDebug >= 2)
      {
	 printf("\nM2:\n\n");
	 printf("%13.6f %13.6f\n", M2.lon, M2.lat);
	 fflush(stdout);
      }

      bndAdd(&M1, &M2, &C);
      bndNormalize(&C);

      C.lon = atan2(C.y, C.x) / bndDTR;
      C.lat = asin(C.z) / bndDTR;

      while(C.lon >= 360.) C.lon -= 360.;
      while(C.lon <    0.) C.lon += 360.;

      if(bndDebug >= 2)
      {
	 printf("\nCenter:\n\n");
	 printf("%13.6f %13.6f\n", C.lon, C.lat);
	 fflush(stdout);
      }


      /* Find the "pole" for the region */

      bndCross(&M2, &M1, &T);
      bndNormalize(&T);

      T.lon = atan2(T.y, T.x) / bndDTR;
      T.lat = asin(T.z) / bndDTR;

      while(T.lon >= 360.) T.lon -= 360.;
      while(T.lon <    0.) T.lon += 360.;

      if(bndDebug >= 2)
      {
	 printf("\nT:\n\n");
	 printf("%13.6f %13.6f\n", T.lon, T.lat);
	 fflush(stdout);
      }


      /* Find the image rotation angle */

      a = acos(bndDot(&T, &Z));
      b = acos(bndDot(&C, &Z));
      c = acos(bndDot(&C, &T));

      cosalpha = (cos(a) - cos(b)*cos(c)) / (sin(b)*sin(c));

      angle = acos(cosalpha) / bndDTR;

      if(bndDebug >= 2)
      {
	 printf("\nAngle:\n\n");
	 printf("%-g\n", angle);
	 fflush(stdout);
      }


      /* Find the X,Y extent range for the polygon points */

      bndTANsetup(C.lon, C.lat, angle);

      loop = first;

      Xmin =  1.e20;
      Xmax = -1.e20;
      Ymin =  1.e20;
      Ymax = -1.e20;

      while(loop)
      {
	 Pi = loop->p;

	 bndTANproj(Pi->lon, Pi->lat);

	 if(bndXpix < Xmin) Xmin = bndXpix;
	 if(bndXpix > Xmax) Xmax = bndXpix;
	 if(bndYpix < Ymin) Ymin = bndYpix;
	 if(bndYpix > Ymax) Ymax = bndYpix;

	 loop = loop->next;
      }

      if(bndDebug >= 2)
      {
	 printf("Xmin = %13.6f\n", Xmin);
	 printf("Xmax = %13.6f\n", Xmax);
	 printf("Ymin = %13.6f\n", Ymin);
	 printf("Ymax = %13.6f\n", Ymax);
	 fflush(stdout);
      }

      size1 = fabs(Xmax);
      if(fabs(Xmin) > size1)
	 size1 = fabs(Xmin);

      size2 = fabs(Ymax);
      if(fabs(Ymin) > size2)
	 size2 = fabs(Ymin);

      size1 *= 2;
      size2 *= 2;

      area = size1 * size2;

      if(bndDebug >= 2)
      {
	 printf("area = %13.6f\n", area);
	 fflush(stdout);
      }

      if(area < minArea)
      {
	 while(angle <  -180) angle += 360.;
	 while(angle >=  180) angle -= 360.;

	 if(angle > 90.)
	    angle = angle - 180.;
	 
         if(angle < -90.)
	    angle = angle + 180.;

	 if(angle > 45.)
	 {
	    angle = angle - 90;
	    stmp  = size1;
	    size1 = size2;
	    size2 = stmp;
	 }

	 if(angle < -45.)
	 {
	    angle = angle + 90;
	    stmp  = size1;
	    size1 = size2;
	    size2 = stmp;
	 }

	 while(angle <= 0) angle += 360.;

	 minArea = area;

	 bndSize1 = size1;
	 bndSize2 = size2;

	 bndCopy(&C, &bndCenter);

	 bndAngle = angle;
      }
   }

   bndTANsetup(bndCenter.lon, bndCenter.lat, bndAngle);

   bndTANdeproj(-bndSize1/2., -bndSize2/2.);
   bndCorner1.lon = bndLon;
   bndCorner1.lat = bndLat;

   bndTANdeproj( bndSize1/2., -bndSize2/2.);
   bndCorner2.lon = bndLon;
   bndCorner2.lat = bndLat;

   bndTANdeproj( bndSize1/2.,  bndSize2/2.);
   bndCorner3.lon = bndLon;
   bndCorner3.lat = bndLat;

   bndTANdeproj(-bndSize1/2.,  bndSize2/2.);
   bndCorner4.lon = bndLon;
   bndCorner4.lat = bndLat;

   if(bndDebug >= 2)
   {
      printf("bndCenter = %11.6f %11.6f\n", bndCenter.lon, bndCenter.lat);
      printf("bndSize1  = %11.6f\n",        bndSize1);
      printf("bndSize2  = %11.6f\n",        bndSize2);
      printf("bndAngle  = %11.6f\n",        bndAngle);
      printf("Corner1   = %11.6f %11.6f\n", bndCorner1.lon, bndCorner1.lat);
      printf("Corner2   = %11.6f %11.6f\n", bndCorner2.lon, bndCorner2.lat);
      printf("Corner3   = %11.6f %11.6f\n", bndCorner3.lon, bndCorner3.lat);
      printf("Corner4   = %11.6f %11.6f\n", bndCorner4.lon, bndCorner4.lat);
      fflush(stdout);
   }
}



void bndComputeVerticalBoundingBox(struct bndStackCell *t)
{
   struct bndStackCell *current  = (struct bndStackCell *)NULL;
   struct bndStackCell *loop     = (struct bndStackCell *)NULL;
   struct bndStackCell *mainloop = (struct bndStackCell *)NULL;

   struct bndSkyLocation *Pi;
   struct bndSkyLocation *Pj, *Pk;
   struct bndSkyLocation *Pjmax, *Pjmin;

   struct bndSkyLocation *P;
   struct bndSkyLocation *Pmin = (struct bndSkyLocation *)NULL;
   struct bndSkyLocation *Pmax = (struct bndSkyLocation *)NULL;

   struct bndSkyLocation Ni;
   struct bndSkyLocation R, Z;

   struct bndSkyLocation N1, N2, N3, N4;
   struct bndSkyLocation C1, C2, C3, C4, C0;
   struct bndSkyLocation temp, temp1, temp2;

   double Xmin, Xmax;
   double Ymin, Ymax;

   double len, lenmin, lenmax;

   double zmin, zmax, dotmin;

   int    counter, allsouth;

   Z.x = 0.;
   Z.y = 0.;
   Z.z = 1.;


   /* Check the beginning point of each segment to  */
   /* see if it is the point closest to the equator */

   counter  = 0;
   zmin     =  999.;
   zmax     = -999.;
   mainloop = t;
   allsouth = 1;

   while(mainloop) 
   { 
      ++counter;

      current  = mainloop;
      mainloop = mainloop->next;

      P = current->p;

      if(P->z > zmax)
      {
	 zmax = P->z;
	 Pmax = P;
      }

      if(P->z < zmin)
      {
	 zmin = P->z;
	 Pmin = P;
      }

      if(P->z > 0)
	 allsouth = 0;
   }

   if(allsouth == 1)
      Pmin = Pmax;

   if(bndDebug >= 2)
   {
      printf("minimum point  = %d\n", Pmin->vnum);
      fflush(stdout);
   }

   bndCross(&Z, Pmin, &R);
   bndNormalize(&R);

   bndCross(Pmin, &R, &N1);
   bndNormalize(&N1);


   /* Check the beginning point of each segment to */
   /* see if it is the point farthest "north" of   */
   /* the first point (really greatest NS opening) */

   counter  = 0;
   mainloop = t;
   dotmin   = 999.;

   while(mainloop) 
   { 
      ++counter;

      current  = mainloop;
      mainloop = mainloop->next;

      P = current->p;

      bndCross(P, &R, &Ni);
      bndNormalize(&Ni);

      if(bndDot(&Ni, &N1) < dotmin)
      {
	 dotmin = bndDot(&Ni, &N1);
	 Pmax = P;
      }
   }

   bndCross(Pmax, &R, &N2);
   bndNormalize(&N2);

   if(bndDebug >= 2)
   {
      printf("maximum point  = %d\n", Pmax->vnum);
      fflush(stdout);
   }


   /* Check all points to see which are the      */
   /* extremes in terms of their distance away   */
   /* from the line running from the point found */
   /* above normal to the first line segment     */
   /* (measured by distance from R)              */

   loop = t;

   lenmin =  999.;
   lenmax = -999.;

   Pjmax = loop->p;
   Pjmin = loop->p;

   while(loop)
   {
      Pj = loop->p;

      len = bndDot(Pj, &R);

      if(len > lenmax)
      {
	 lenmax = len;
	 Pjmax  = Pj;
      }

      if(len < lenmin)
      {
	 lenmin = len;
	 Pjmin  = Pj;
      }

      loop = loop->next;
   }

   Pj = Pjmax;
   Pk = Pjmin;


   /* Compute N3 and N4 */

   bndCross(Pj, &R, &temp);
   bndCross(&temp, Pj, &N3);
   bndNormalize(&N3);

   bndCross(Pk, &R, &temp);
   bndCross(&temp, Pk, &N4);
   bndNormalize(&N4);

   if(bndDebug >= 2)
   {
      printf("\nPlanes:\n\n");
      printf("%13.5e %13.5e %13.5e\n", N1.x, N1.y, N1.z);
      printf("%13.5e %13.5e %13.5e\n", N2.x, N2.y, N2.z);
      printf("%13.5e %13.5e %13.5e\n", N3.x, N3.y, N3.z);
      printf("%13.5e %13.5e %13.5e\n", N4.x, N4.y, N4.z);
      fflush(stdout);
   }



   /* Find the points of intersection     */
   /* between the normal planes (i.e. the */
   /* corners of our box)                 */

   bndCross(&N1, &N3, &C1);
   bndNormalize(&C1);
   if(bndDot(&C1, Pj) < 0.)
      bndReverse(&C1);

   C1.lon = atan2(C1.y, C1.x) / bndDTR;
   C1.lat = asin(C1.z) / bndDTR;

   while(C1.lon >= 360.) C1.lon -= 360.;
   while(C1.lon <    0.) C1.lon += 360.;

   bndCross(&N2, &N3, &C2);
   bndNormalize(&C2);
   if(bndDot(&C2, Pj) < 0.)
      bndReverse(&C2);

   C2.lon = atan2(C2.y, C2.x) / bndDTR;
   C2.lat = asin(C2.z) / bndDTR;

   while(C2.lon >= 360.) C2.lon -= 360.;
   while(C2.lon <    0.) C2.lon += 360.;

   bndCross(&N2, &N4, &C3);
   bndNormalize(&C3);
   if(bndDot(&C3, Pk) < 0.)
      bndReverse(&C3);

   C3.lon = atan2(C3.y, C3.x) / bndDTR;
   C3.lat = asin(C3.z) / bndDTR;

   while(C3.lon >= 360.) C3.lon -= 360.;
   while(C3.lon <    0.) C3.lon += 360.;

   bndCross(&N1, &N4, &C4);
   bndNormalize(&C4);
   if(bndDot(&C4, Pk) < 0.)
      bndReverse(&C4);

   C4.lon = atan2(C4.y, C4.x) / bndDTR;
   C4.lat = asin(C4.z) / bndDTR;

   while(C4.lon >= 360.) C4.lon -= 360.;
   while(C4.lon <    0.) C4.lon += 360.;

   if(bndDebug >= 2)
   {
      printf("\nbndCorners:\n\n");
      printf("%13.6f %13.6f\n", C1.lon, C1.lat);
      printf("%13.6f %13.6f\n", C2.lon, C2.lat);
      printf("%13.6f %13.6f\n", C3.lon, C3.lat);
      printf("%13.6f %13.6f\n", C4.lon, C4.lat);
      fflush(stdout);
   }

   
   /* Find the "image" center */

   bndCross(&C1, &C3, &temp1);
   bndCross(&C2, &C4, &temp2);
   bndCross(&temp1, &temp2, &C0);
   bndNormalize(&C0);

   if(bndDot(&C0, &C1) < 0.)
      bndReverse(&C0);

   C0.lon = atan2(C0.y, C0.x) / bndDTR;
   C0.lat = asin(C0.z) / bndDTR;

   while(C0.lon >= 360.) C0.lon -= 360.;
   while(C0.lon <    0.) C0.lon += 360.;


   /* Find the X,Y extent range for the polygon points */

   bndTANsetup(C0.lon, C0.lat, 0.0);

   loop = t;

   Xmin =  1.e20;
   Xmax = -1.e20;
   Ymin =  1.e20;
   Ymax = -1.e20;

   while(loop)
   {
      Pi = loop->p;

      bndTANproj(Pi->lon, Pi->lat);

      if(bndXpix < Xmin) Xmin = bndXpix;
      if(bndXpix > Xmax) Xmax = bndXpix;
      if(bndYpix < Ymin) Ymin = bndYpix;
      if(bndYpix > Ymax) Ymax = bndYpix;

      loop = loop->next;
   }

   if(bndDebug >= 2)
   {
      printf("Xmin = %13.6f\n", Xmin);
      printf("Xmax = %13.6f\n", Xmax);
      printf("Ymin = %13.6f\n", Ymin);
      printf("Ymax = %13.6f\n", Ymax);
      fflush(stdout);
   }

   bndSize1 = fabs(Xmax);
   if(fabs(Xmin) > bndSize1)
      bndSize1 = fabs(Xmin);

   bndSize2 = fabs(Ymax);
   if(fabs(Ymin) > bndSize2)
      bndSize2 = fabs(Ymin);

   bndSize1 *= 2;
   bndSize2 *= 2;

   bndCopy(&C0, &bndCenter);

   bndAngle = 0.0;

   bndTANsetup(bndCenter.lon, bndCenter.lat, bndAngle);

   bndTANdeproj(-bndSize1/2., -bndSize2/2.);
   bndCorner1.lon = bndLon;
   bndCorner1.lat = bndLat;

   bndTANdeproj( bndSize1/2., -bndSize2/2.);
   bndCorner2.lon = bndLon;
   bndCorner2.lat = bndLat;

   bndTANdeproj( bndSize1/2.,  bndSize2/2.);
   bndCorner3.lon = bndLon;
   bndCorner3.lat = bndLat;

   bndTANdeproj(-bndSize1/2.,  bndSize2/2.);
   bndCorner4.lon = bndLon;
   bndCorner4.lat = bndLat;

   if(bndDebug >= 2)
   {
      printf("bndCenter = %11.6f %11.6f\n", bndCenter.lon, bndCenter.lat);
      printf("bndSize1  = %11.6f\n",        bndSize1);
      printf("bndSize2  = %11.6f\n",        bndSize2);
      printf("bndAngle  = %11.6f\n",        bndAngle);
      printf("Corner1   = %11.6f %11.6f\n", bndCorner1.lon, bndCorner1.lat);
      printf("Corner2   = %11.6f %11.6f\n", bndCorner2.lon, bndCorner2.lat);
      printf("Corner3   = %11.6f %11.6f\n", bndCorner3.lon, bndCorner3.lat);
      printf("Corner4   = %11.6f %11.6f\n", bndCorner4.lon, bndCorner4.lat);
      fflush(stdout);
   }
}



void bndComputeBoundingCircle(struct bndStackCell *t)
{
   struct bndStackCell *first = (struct bndStackCell *)NULL;
   struct bndStackCell *loop  = (struct bndStackCell *)NULL;

   struct bndSkyLocation *P;

   double radius;

   first = t;


   /* First, find the best bounding box.  We   */
   /* will use its center as the center of the */
   /* circle                                   */

   bndComputeBoundingBox(t);


   /* Check all points to see which is */
   /* farthest from the region center  */

   loop = first;

   bndRadius = 0;

   while(loop)
   {
      P = loop->p;

      radius = acos(bndDot(P, &bndCenter)) / bndDTR;

      if(radius > bndRadius)
	 bndRadius = radius;

      loop = loop->next;
   }
}



void bndDrawBox()
{
   printf("color white\n");
   printf("ptype o\n");
   printf("move %13.6f %13.6f\n", bndCorner1.lon, bndCorner1.lat);
   printf("dot\n");
   printf("draw %13.6f %13.6f\n", bndCorner2.lon, bndCorner2.lat);
   printf("draw %13.6f %13.6f\n", bndCorner3.lon, bndCorner3.lat);
   printf("draw %13.6f %13.6f\n", bndCorner4.lon, bndCorner4.lat);
   printf("draw %13.6f %13.6f\n", bndCorner1.lon, bndCorner1.lat);

   printf("move %13.6f %13.6f\n", bndCenter.lon, bndCenter.lat);
   printf("ptype +\n");
   printf("expand 3\n");
   printf("dot\n");
}



void bndDrawCircle()
{
   int    i;
   double x, y, angle;

   printf("color white\n");
   printf("ptype o\n");

   bndTANsetup(bndCenter.lon, bndCenter.lat, 0.0);

   for(i=0; i<=360; ++i)
   {
      angle = i * bndDTR;

      x = bndRadius * cos(angle);
      y = bndRadius * sin(angle);

      bndTANdeproj(x, y);

      if(i == 0)
         printf("move %13.6f %13.6f\n", bndLon, bndLat);

      printf("draw %13.6f %13.6f\n", bndLon, bndLat);
   }

   printf("move %13.6f %13.6f\n", bndCenter.lon, bndCenter.lat);
   printf("ptype +\n");
   printf("expand 3\n");
   printf("dot\n");
}



void bndDrawOutline(struct bndStackCell *t)
{
   int first = 1;
   struct bndStackCell *f;

   f = t;

   while(t) 
   { 
      if(first)
      {
	 printf("color yellow\n");
	 printf("move %12.6f %12.6f\n", 
		t->p->lon, t->p->lat); 
      }
      else
	 printf("draw %12.6f %12.6f\n", 
		t->p->lon, t->p->lat); 

      t = t->next;

      first = 0;
   }

   printf("draw %12.6f %12.6f\n", 
	  f->p->lon, f->p->lat); 

   printf("dot\n");
   fflush(stdout);
}



struct bndStackCell *bndGraham()
{
   int i;

   struct bndStackCell   *top;
   struct bndSkyLocation *p1, *p2;  /* Top two points on stack. */


   /* bndInitialize stack. */

   top = NULL;
   top = bndPush(&bndPoints[0], top);
   top = bndPush(&bndPoints[1], top);


   /* Bottom two elements will never be removed. */
   i = 2;

   while (i < bndNpoints) 
   {
      if(bndDebug >= 2)
      {
	 printf("\n-----------------------------\n");
	 printf("Stack at top of while loop, i=%d, vnum=%d:\n", i,
		 bndPoints[i].vnum);

	 bndPrintStack(top);
      }

      if(!top->next)
      {
         top = bndPush(&bndPoints[i], top);
         ++i;
      }

      p1 = top->next->p;
      p2 = top->p;

      if(bndLeft(p1 , p2, &bndPoints[i])) 
      {
	 if(bndDebug >= 2)
	 {
	    printf("%d -> %d -> %d : Left turn (push %d)\n", p1->vnum, p2->vnum, 
		   bndPoints[i].vnum, bndPoints[i].vnum);
	    fflush(stdout);
	 }
	 
         top = bndPush(&bndPoints[i], top);
         ++i;
      } 
      else    
      {
	 if(bndDebug >= 3)
	 {
	    printf("%d -> %d -> %d : Right turn (pop %d)\n", p1->vnum, p2->vnum, 
		   bndPoints[i].vnum, top->p->vnum);
	    fflush(stdout);
	 }
	 
         top = bndPop(top);
      }

      if(bndDebug >= 2)
      {
	 printf("\nStack at bottom of while loop, i=%d, vnum=%d:\n", 
		 i, bndPoints[i].vnum);

	 bndPrintStack(top);
      }
   }

   if(i<3)
      return (struct bndStackCell *)NULL;

   return top;
}


int bndLeft(struct bndSkyLocation *p1, 
	    struct bndSkyLocation *p2, 
	    struct bndSkyLocation *p3)
{
   struct bndSkyLocation dir1, dir2;
   struct bndSkyLocation angVec;

   bndCross(p1, p2, &dir1);
   bndNormalize(&dir1);

   bndCross(p2, p3, &dir2);
   bndNormalize(&dir2);

   bndCross(&dir2, &dir1, &angVec);
   bndNormalize(&angVec);

   if(bndDot(p2, &angVec) > 0.)
      return(0);

   return(1);
}


void PrintSkyPoints(void)
{
   int i;

   printf("Points:\n");

   printf("%13s %13s %13s %13s %13s %13s %6s %6s\n", 
          "lon", "lat", "x", "y", "z", "ang", "vnum", "delete"); 

   for(i = 0; i < bndNpoints; ++i)
   {
      printf("%13.6f %13.6f %13.5e %13.5e %13.5e %13.10f %6d %6d\n", 
	     bndPoints[i].lon, bndPoints[i].lat, 
	     bndPoints[i].x, bndPoints[i].y, bndPoints[i].z,
             bndPoints[i].ang, 
             bndPoints[i].vnum,
             bndPoints[i].delete);
   }
}



void bndDrawSkyPoints(void)
{
   int i;

   printf("proj gnomonic\n");
   printf("pcent %13.6f %13.6f\n", Centroid.lon, Centroid.lat);
   printf("mcent %13.6f %13.6f\n", Centroid.lon, Centroid.lat);
   printf("size  %13.6f %13.6f\n", 2.2*bndSize, 2.2*bndSize);
   printf("color blue\n");
   printf("border\n");
   printf("grid\n");
   printf("color red\n");

   for(i = 0; i < bndNpoints; ++i)
   {
      printf("move %13.6f %13.6f\ndot\n", 
	     bndPoints[i].lon, bndPoints[i].lat);
   }
}




/***************************************************/
/*                                                 */
/* bndAdd()                                        */
/*                                                 */
/* Vector addition.                                */
/*                                                 */
/***************************************************/

void bndAdd(struct bndSkyLocation *v1, 
	    struct bndSkyLocation *v2, 
	    struct bndSkyLocation *v3)

{
   v3->x =  v1->x + v2->x;
   v3->y =  v1->y + v2->y;
   v3->z =  v1->z + v2->z;
}


/***************************************************/
/*                                                 */
/* bndCross()                                      */
/*                                                 */
/* Vector cross product.                           */
/*                                                 */
/***************************************************/

void bndCross(struct bndSkyLocation *v1, 
	      struct bndSkyLocation *v2, 
	      struct bndSkyLocation *v3)

{
   v3->x =  v1->y*v2->z - v2->y*v1->z;
   v3->y = -v1->x*v2->z + v2->x*v1->z;
   v3->z =  v1->x*v2->y - v2->x*v1->y;
}


/***************************************************/
/*                                                 */
/* bndDot()                                        */
/*                                                 */
/* Vector dot product.                             */
/*                                                 */
/***************************************************/

double bndDot(struct bndSkyLocation *a, 
	      struct bndSkyLocation *b)
{
   double sum = 0.0;

   sum = a->x * b->x
       + a->y * b->y
       + a->z * b->z;

   return sum;
}


/***************************************************/
/*                                                 */
/* bndNormalize()                                  */
/*                                                 */
/* Normalize the vector                            */
/*                                                 */
/***************************************************/

double bndNormalize(struct bndSkyLocation *v)
{
   double len;

   len = sqrt(v->x * v->x + v->y * v->y + v->z * v->z);

   if(len < tolerance && bndDebug >= 3)
   {
      printf("\nWARNING:  vector length = %13.6e\n", len);
      fflush(stdout);
   }

   if(len <= 0.)
      return 0.;

   v->x = v->x / len;
   v->y = v->y / len;
   v->z = v->z / len;

   return len;
}


/***************************************************/
/*                                                 */
/* bndReverse()                                    */
/*                                                 */
/* Reverse the vector                              */
/*                                                 */
/***************************************************/

void bndReverse(struct bndSkyLocation *v)
{
   v->x = -(v->x);
   v->y = -(v->y);
   v->z = -(v->z);
}


/***************************************************/
/*                                                 */
/* bndCopy()                                       */
/*                                                 */
/* Copy the contents of one vector to another      */
/*                                                 */
/***************************************************/

void bndCopy(struct bndSkyLocation *v1, 
	     struct bndSkyLocation *v2)
{
   v2->lon    = v1->lon;
   v2->lat    = v1->lat;
   v2->x      = v1->x;
   v2->y      = v1->y;
   v2->z      = v1->z;
   v2->ang    = v1->ang;
   v2->vnum   = v1->vnum;
   v2->delete = v1->delete;
}


/***************************************************/
/*                                                 */
/* bndEqual()                                      */
/*                                                 */
/* Returns 1 if the two vectors are equal          */
/* (within tolerance)                              */
/*                                                 */
/***************************************************/

int bndEqual(struct bndSkyLocation *v1, 
             struct bndSkyLocation *v2)
{
   if(fabs(v1->x - v2->x) < tolerance
   && fabs(v1->y - v2->y) < tolerance
   && fabs(v1->z - v2->z) < tolerance)
      return(1);

   return 0;
}


/***************************************************/
/*                                                 */
/* TAN projection transform.  Used to determine    */
/* the extent of the image in the projection plane */
/*                                                 */
/***************************************************/

static void bndTANsetup(double alpha, double delta, double rot)
{
   double sinr, cosr;
   double sina, cosa;
   double sind, cosd;


   sinr = sin(rot   * bndDTR);
   cosr = cos(rot   * bndDTR);

   sina = sin(alpha * bndDTR);
   cosa = cos(alpha * bndDTR);

   sind = sin(delta * bndDTR);
   cosd = cos(delta * bndDTR);


   bndProjMatrix[0][0]   =  cosd * cosa;
   bndProjMatrix[0][1]   =  cosd * sina;
   bndProjMatrix[0][2]   =  sind;

   bndProjMatrix[1][0]   = -cosr * sina - sinr * sind * cosa;
   bndProjMatrix[1][1]   =  cosr * cosa - sinr * sind * sina;
   bndProjMatrix[1][2]   =  sinr * cosd;

   bndProjMatrix[2][0]   =  sinr * sina - cosr * sind * cosa;
   bndProjMatrix[2][1]   = -sinr * cosa - cosr * sind * sina;
   bndProjMatrix[2][2]   =  cosr * cosd;


   bndDeprojMatrix[0][0] =  cosa * cosd;
   bndDeprojMatrix[0][1] = -cosa * sind * sinr - sina * cosr;
   bndDeprojMatrix[0][2] = -cosa * sind * cosr + sina * sinr;

   bndDeprojMatrix[1][0] =  sina * cosd;
   bndDeprojMatrix[1][1] = -sina * sind * sinr + cosa * cosr;
   bndDeprojMatrix[1][2] = -sina * sind * cosr - cosa * sinr;

   bndDeprojMatrix[2][0] =  sind;
   bndDeprojMatrix[2][1] =  cosd * sinr;
   bndDeprojMatrix[2][2] =  cosd * cosr;
}

static void bndTANproj(double lon, double lat)
{
   struct bndSkyLocation P, Pim, X, Y, N, tmp;

   double theta, R;
   double phi, sinphi, cosphi;

   X.x = 1.;
   X.y = 0.;
   X.z = 0.;

   Y.x = 0.;
   Y.y = 1.;
   Y.z = 0.;

   P.x = cos(lon * bndDTR) * cos(lat * bndDTR);
   P.y = sin(lon * bndDTR) * cos(lat * bndDTR);
   P.z = sin(lat * bndDTR);

   Pim.x = bndProjMatrix[0][0] * P.x 
	 + bndProjMatrix[0][1] * P.y 
	 + bndProjMatrix[0][2] * P.z;

   Pim.y = bndProjMatrix[1][0] * P.x 
	 + bndProjMatrix[1][1] * P.y 
	 + bndProjMatrix[1][2] * P.z;

   Pim.z = bndProjMatrix[2][0] * P.x 
	 + bndProjMatrix[2][1] * P.y 
	 + bndProjMatrix[2][2] * P.z;

   bndNormalize(&Pim);

   theta = acos(Pim.x);

   R = tan(theta);

   bndCross(&X, &Pim, &N);
   bndNormalize(&N);

   bndCross(&Y, &N, &tmp);

   sinphi = bndNormalize(&tmp);
   cosphi = bndDot(&Y, &N);

   if(bndDot(&X, &tmp) < 0)
      sinphi = -sinphi;

   phi = atan2(sinphi, cosphi);

   bndXpix =  R * sinphi / bndDTR;
   bndYpix = -R * cosphi / bndDTR;
}


static void bndTANdeproj(double X, double Y)
{
   struct bndSkyLocation Pim, P;

   Pim.y = X * bndDTR;
   Pim.z = Y * bndDTR;
   Pim.x = 1.;

   bndNormalize(&Pim);

   P.x = bndDeprojMatrix[0][0] * Pim.x 
       + bndDeprojMatrix[0][1] * Pim.y 
       + bndDeprojMatrix[0][2] * Pim.z;

   P.y = bndDeprojMatrix[1][0] * Pim.x 
       + bndDeprojMatrix[1][1] * Pim.y 
       + bndDeprojMatrix[1][2] * Pim.z;

   P.z = bndDeprojMatrix[2][0] * Pim.x 
       + bndDeprojMatrix[2][1] * Pim.y 
       + bndDeprojMatrix[2][2] * Pim.z;

   bndNormalize(&P);

   bndLon = atan2(P.y, P.x)/bndDTR;
   bndLat = asin(P.z)/bndDTR;
}
