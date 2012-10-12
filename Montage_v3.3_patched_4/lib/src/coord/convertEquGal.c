#include <stdio.h>
#include <math.h>
#include <coord.h>

/***************************************************************************/
/*                                                                         */
/* convertGalToEqu computes equatorial coords from galactic coords.        */
/* ---------------                                                         */
/*                                                                         */
/* Inputs:  glon (l)  in degrees, glat (b)   in degrees.                   */
/* Outputs: ra   (ra) in degrees, dec  (dec) in degrees.                   */
/*                                                                         */
/*                                                                         */
/* Right hand rule coordinates:                                            */
/*                                                                         */
/*    x = cos(lat)*cos(lon)                                                */
/*    y = sin(lon)*cos(lat)                                                */
/*    z = sin(lat)                                                         */
/*                                                                         */
/*                                                                         */
/* Gal <==> Equ equations                                                  */
/*                                                                         */
/* assume: Gal north pole at 12h 49.0m, +27d 24' [192.25, +27.40]          */
/*         Gal (0,0)      at 17h 42.4m, -28d 55' [265.36, -28.91]          */
/*                                                                         */
/* equators cross at:   Gal (33.00, 0.00)        [282.25,   0.00]          */
/*                                                                         */
/***************************************************************************/


void convertGalToEqu(double glon, double glat, double *ra, double *dec)
{     
   static int nthru = 0;

   static double dtor, rtod;
   static double trans[3][3];

   double glonr, glatr;
   double cosl, cosL, sinL;

   double x, y, z;
   double xeq, yeq, zeq;

   double cosph, sinph, cosps, sinps, costh, sinth;

   if(coord_debug)
   {
      fprintf(stderr, "DEBUG: convertGalToEqu()\n");
      fflush(stderr);
   }




   /* First time using this function, compute the        */
   /* degrees <-> radians conversion factors dynamically */
   /* (ensuring computational consistency).  Also        */
   /* calculate xyz transformation matrix from galactic  */
   /* to equatorial.                                     */

   if(nthru == 0) 
   {
      /* Angles defining the relationship */
      /* between galactic and equatorial  */

      double psi = -33.00, theta = 62.60, phi = 282.25;


      dtor = atan(1.0) / 45.0;
      rtod = 1.0 / dtor;

      cosps = cos(  psi*dtor);
      sinps = sin(  psi*dtor);

      cosph = cos(  phi*dtor);
      sinph = sin(  phi*dtor);

      costh = cos(theta*dtor);
      sinth = sin(theta*dtor);

      trans[0][0] =  cosps*cosph - costh*sinph*sinps;
      trans[0][1] = -sinps*cosph - costh*sinph*cosps;
      trans[0][2] =  sinth*sinph;

      trans[1][0] =  cosps*sinph + costh*cosph*sinps;
      trans[1][1] = -sinps*sinph + costh*cosph*cosps;
      trans[1][2] = -sinth*cosph;

      trans[2][0] =  sinth*sinps;
      trans[2][1] =  sinth*cosps;
      trans[2][2] =  costh;

      nthru = 1;
   }


   /* Compute the xyz (galactic) coordinates of the point */
   
   glonr = glon * dtor;
   glatr = glat * dtor;

   cosl = cos(glatr);
   cosL = cos(glonr);
   sinL = sin(glonr);

   x = cosL*cosl;
   y = sinL*cosl;

   z = sin(glatr);



   /* Convert to xyz equatorial (with various range checks */
   /* and renormalizations to prevent invalid values)      */

   zeq = trans[2][0]*x + trans[2][1]*y + trans[2][2]*z;


   /* Special case for the poles (set ra to 0) */

   if(fabs(zeq) >= 1.0)
   {
      zeq = zeq/fabs(zeq);

      *dec = asin(zeq);
      *ra  = 0.0;
   }

   /* General case */
   else
   {
      xeq  = trans[0][0]*x + trans[0][1]*y + trans[0][2]*z;
      yeq  = trans[1][0]*x + trans[1][1]*y + trans[1][2]*z;

      *dec = asin (zeq);
      *ra  = atan2(yeq, xeq);
   }



   /* Convert to degrees and adjust range */

   *ra  = *ra  * rtod;

   while(*ra <   0.0) *ra += 360.0;
   while(*ra > 360.0) *ra -= 360.0;

   *dec = *dec * rtod;



   /* Double check for the poles */

   if(fabs(*dec) >= 90.0) 
   {
      *ra = 0.0;
      if(*dec >  90.0) *dec =  90.0;
      if(*dec < -90.0) *dec = -90.0;
   }

   return;
}




/***************************************************************************/
/*                                                                         */
/* convertEquToGal computes galactic coords from equatorial coords.        */
/* ---------------                                                         */
/*                                                                         */
/* Inputs:  ra   (ra) in degrees, dec  (dec) in degrees.                   */
/* Outputs: glon (l)  in degrees, glat (b)   in degrees.                   */
/*                                                                         */
/*                                                                         */
/* see information on convertGalToEqu().                                   */
/*                                                                         */
/***************************************************************************/


void convertEquToGal(double ra, double dec, double *glon, double *glat)
{
   static int nthru = 0;

   static double dtor, rtod;
   static double trans[3][3];

   double rar, decr;
   double cosl, cosL, sinL;

   double x, y, z;
   double xgal, ygal, zgal;

   double cosph, sinph, cosps, sinps, costh, sinth;

   if(coord_debug)
   {
      fprintf(stderr, "DEBUG: convertEquToGal()\n");
      fflush(stderr);
   }




   /* First time using this function, compute the         */
   /* degrees <-> radians conversion factors dynamically  */
   /* (ensuring computational consistency).  Also         */
   /* calculate xyz transformation matrix from equatorial */
   /* to galactic.                                        */

   if(nthru == 0)
   {
      /* Angles defining the relationship */
      /* between galactic and equatorial  */

      double psi = -33.00, theta = 62.60, phi = 282.25;

      dtor = atan(1.0) / 45.0;
      rtod = 1.0 / dtor;

      cosps = cos(  psi*dtor);
      sinps = sin(  psi*dtor);

      cosph = cos(  phi*dtor);
      sinph = sin(  phi*dtor);

      costh = cos(theta*dtor);
      sinth = sin(theta*dtor);

      trans[0][0] =  cosps*cosph - costh*sinph*sinps;
      trans[0][1] =  cosps*sinph + costh*cosph*sinps;
      trans[0][2] =  sinps*sinth;

      trans[1][0] = -sinps*cosph - costh*sinph*cosps;
      trans[1][1] = -sinps*sinph + costh*cosph*cosps;
      trans[1][2] =  cosps*sinth;

      trans[2][0] =  sinth*sinph;
      trans[2][1] = -sinth*cosph;
      trans[2][2] =  costh;
       
      nthru = 1;
   }


   /* Compute the xyz (equatorial) coordinates of the point */

   rar  = ra  * dtor;
   decr = dec * dtor;

   cosl = cos(decr);
   cosL = cos(rar);
   sinL = sin(rar);

   x = cosL*cosl;
   y = sinL*cosl;

   z = sin(decr);



   /* Convert to xyz galactic (with various range checks */
   /* and renormalizations to prevent invalid values)    */

   zgal = trans[2][0]*x+ trans[2][1]*y + trans[2][2]*z;


   /* Special case for the poles (set ra to 0) */

   if(fabs(zgal)>=1.0) 
   {
      zgal = zgal/fabs(zgal);

      *glat = asin(zgal);
      *glon = 0.0;
   }

   /* Normal case */

   else
   {
      xgal = trans[0][0]*x + trans[0][1]*y + trans[0][2]*z;
      ygal = trans[1][0]*x + trans[1][1]*y + trans[1][2]*z;

      *glat =  asin(zgal);
      *glon = atan2(ygal, xgal);
   }



   /* Convert to degrees and adjust range */

   *glon = *glon * rtod;

   while(*glon <   0.0) *glon += 360.0;
   while(*glon > 360.0) *glon -= 360.0;

   *glat = *glat * rtod;



   /* Double check for the poles */

   if(fabs(*glat) >= 90.0) 
   {
      *glon = 0.0;
      if(*glat >  90.0) *glat =  90.0;
      if(*glat < -90.0) *glat = -90.0;
   }

   return;
}
