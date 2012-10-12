#include <stdio.h>
#include <stdlib.h>
#include <math.h>

#include "boundaries.h"

double tolerance = 4.848137e-10;  /* sin(x) where x = 0.001 arcsec */

double bndProjMatrix  [3][3];
double bndDeprojMatrix[3][3];

double bndXpix, bndYpix;
double bndLon,  bndLat;

main(int argc, char **argv)
{
   double rot;
   double alpha, delta;
   double lon, lat;

   bndDTR = atan(1.)/45.;

   printf("\nInput: alpha delta rot\n");
   scanf("%lf %lf %lf", &alpha, &delta, &rot);

   bndTANsetup(alpha, delta, rot);

   while(1)
   {
      printf("\nInput: lon lat\n");
      scanf("%lf %lf", &lon, &lat);

      bndTANproj  (lon, lat);
      bndTANdeproj(bndXpix, bndYpix);

      printf("X,Y     = %11.6f %11.6f\n", bndXpix, bndYpix);
      printf("lon,lat = %11.6f %11.6f\n", bndLon,  bndLat);
   }
}


bndTANsetup(double alpha, double delta, double rot)
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

   printf("%13.6f %13.6f %13.6f\n", bndDeprojMatrix[0][0], bndDeprojMatrix[0][1], bndDeprojMatrix[0][2]);
   printf("%13.6f %13.6f %13.6f\n", bndDeprojMatrix[1][0], bndDeprojMatrix[1][1], bndDeprojMatrix[1][2]);
   printf("%13.6f %13.6f %13.6f\n", bndDeprojMatrix[2][0], bndDeprojMatrix[2][1], bndDeprojMatrix[2][2]);
}

bndTANproj(double lon, double lat)
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

   P.lon = atan2(P.y, P.x)/bndDTR;
   P.lat = asin(P.z)/bndDTR;

   printf("bndTANproj:   P   = %13.6f %13.6f %13.6f -> %13.6f %13.6f\n",
      P.x, P.y, P.z, P.lon, P.lat);

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

   Pim.lon = atan2(Pim.y, Pim.x)/bndDTR;
   Pim.lat = asin(Pim.z)/bndDTR;

   printf("bndTANproj:   Pim = %13.6f %13.6f %13.6f -> %13.6f %13.6f\n",
      Pim.x, Pim.y, Pim.z, Pim.lon, Pim.lat);

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


bndTANdeproj(double X, double Y)
{
   struct bndSkyLocation Pim, P;

   Pim.y = X * bndDTR;
   Pim.z = Y * bndDTR;
   Pim.x = 1.;

   bndNormalize(&Pim);

   Pim.lon = atan2(Pim.y, Pim.x)/bndDTR;
   Pim.lat = asin(Pim.z)/bndDTR;

   printf("bndTANdeproj: Pim = %13.6f %13.6f %13.6f -> %13.6f %13.6f\n",
      Pim.x, Pim.y, Pim.z, Pim.lon, Pim.lat);

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
   
   P.lon = atan2(P.y, P.x)/bndDTR;
   P.lat = asin(P.z)/bndDTR;

   printf("bndTANdeproj: P   = %13.6f %13.6f %13.6f -> %13.6f %13.6f\n",
      P.x, P.y, P.z, P.lon, P.lat);

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
   int i;
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
