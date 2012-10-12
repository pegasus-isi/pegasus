#include <stdio.h>
#include <math.h>
#include <coord.h>

double computeEquPole();



/***************************************************************************/
/*                                                                         */
/* convertEclToEqu computes equatorial coords from ecliptic coords.        */
/* ---------------                                                         */
/*                                                                         */
/* Inputs:  elon    in degrees, elat      in degrees.                      */
/* Outputs: ra (ra) in degrees, dec (dec) in degrees.                      */
/*                                                                         */
/*                                                                         */
/* Right hand rule coordinates:                                            */
/*                                                                         */
/*    x = cos(lat)*cos(lon)                                                */
/*    y = sin(lon)*cos(lat)                                                */
/*    z = sin(lat)                                                         */
/*                                                                         */
/*                                                                         */
/* Ecl <==> Equ equations                                                  */
/*                                                                         */
/* assume: Ecliptic and Equatorial (0,0) points coincide.                  */
/*         Ecliptic location of Equatorial pole is given by jgtobq().      */
/*                                                                         */
/***************************************************************************/


void convertEclToEqu(double elon, double elat, double *ra, double *dec, 
		     double date, int besselian)
{
   static int    nthru          =    0;
   static int    savebesselian  =  -99;
   static double savedate       = -1.0;

   static double dtor, rtod;
   static double cosp, sinp;
   
   double cosl, sinl, cosL, sinL;
   double x, y, z;
   double xeq, yeq, zeq;
   double elonr, elatr;



   if(coord_debug)
   {
      fprintf(stderr, "DEBUG: convertEclToEqu()\n");
      fflush(stderr);
   }

   

   /* First time using this function, compute the        */
   /* degrees <-> radians conversion factors dynamically */
   /* (ensuring computational consistency).              */

   if(nthru == 0)
   {
      dtor = atan(1.0) / 45.0;
      rtod = 1.0 / dtor;
      nthru = 1;
   }



   /* Compute the ecliptic/equatorial pole offset angle   */
   /* Remember the time setting so we don't have to       */
   /* repeat the calculation next time if nothing changed */
   
   if(date != savedate || besselian != savebesselian) 
   {
      double pole;

      pole = computeEquPole(date, besselian) * dtor;

      cosp = cos(pole);
      sinp = sin(pole);

      savedate      = date;
      savebesselian = besselian;
   }



   /* Compute the xyz (ecliptic) coordinates of the point */
   
   elonr = elon*dtor;
   elatr = elat*dtor;

   sinl = sin(elatr);
   cosl = cos(elatr);

   sinL = sin(elonr);
   cosL = cos(elonr);

   x =  cosL*cosl;
   y = -sinL*cosl;
   z =  sinl;



   /* Convert to xyz equatorial (with various range checks */
   /* and renormalizations to prevent invalid values)      */

   xeq = x;
   yeq = sinp*z + cosp*y;
   zeq = cosp*z - sinp*y;

   *ra = atan2(-yeq, xeq) * rtod;

   while(*ra <   0.0) *ra += 360.0;
   while(*ra > 360.0) *ra -= 360.0;


   /* Check for the pole */

   if(fabs(zeq) > 1.0)
   {
      *dec = 90.0*zeq/fabs(zeq);
      *ra  = 0.0;
   }


   /* Normal case */

   else
   {
      *dec = asin(zeq) * rtod;

      if(fabs(*dec) >= 90.0)
      {
	 *ra = 0.0;

	 if(*dec >  90.0) *dec =  90.0;
	 if(*dec < -90.0) *dec = -90.0;
      }
   }


   return;
}






/***************************************************************************/
/*                                                                         */
/* convertEquToEcl computes ecliptic coords from equatorial coords.        */
/* ---------------                                                         */
/*                                                                         */
/* Inputs:  ra (ra) in degrees, dec (dec) in degrees.                      */
/* Outputs  elon    in degrees, elat      in degrees.                      */
/*                                                                         */
/*                                                                         */
/* Right hand rule coordinates:                                            */
/*                                                                         */
/*    x = cos(lat)*cos(lon)                                                */
/*    y = sin(lon)*cos(lat)                                                */
/*    z = sin(lat)                                                         */
/*                                                                         */
/*                                                                         */
/* Ecl <==> Equ equations                                                  */
/*                                                                         */
/* assume: Ecliptic and Equatorial (0,0) points coincide.                  */
/*         Ecliptic location of Equatorial pole is given by jgtobq().      */
/*                                                                         */
/***************************************************************************/


void convertEquToEcl(double ra, double dec, double *elon, double *elat, 
		     double date, int besselian)
{
   static int    nthru          =    0;
   static int    savebesselian  =  -99;
   static double savedate       = -1.0;
   static double cosp, sinp;

   static double dtor, rtod;
   
   double cosl, sinl, cosL, sinL;
   double x, y, z;
   double xec, yec, zec;
   double rar, decr;   

   

   if(coord_debug)
   {
      fprintf(stderr, "DEBUG: convertEquToEcl()\n");
      fflush(stderr);
   }




   /* First time using this function, compute the        */
   /* degrees <-> radians conversion factors dynamically */
   /* (ensuring computational consistency).              */

   if(nthru == 0)
   {
      dtor = atan(1.0) / 45.0;
      rtod = 1.0 / dtor;
      nthru = 1;
   }



   /* Compute the ecliptic/equatorial pole offset angle   */
   /* Remember the time setting so we don't have to       */
   /* repeat the calculation next time if nothing changed */
   
   /*if(date != savedate || besselian != savebesselian) */
   {
      double pole;

      pole = computeEquPole(date, besselian) * dtor;

      cosp = cos(pole);
      sinp = sin(pole);

      savedate      = date;
      savebesselian = besselian;
   }



   /* Compute the xyz (equatorial) coordinates of the point */
   
   rar   = ra *dtor;
   decr  = dec*dtor;

   sinl = sin(decr);
   cosl = cos(decr);

   sinL = sin(rar);
   cosL = cos(rar);

   x =  cosL*cosl;
   y = -sinL*cosl;
   z =  sinl;



   /* Convert to xyz ecliptic (with various range checks */
   /* and renormalizations to prevent invalid values)    */

   xec =  x;
   yec = -sinp*z + cosp*y;
   zec =  cosp*z + sinp*y;

   *elon = atan2(-yec, xec) * rtod;

   while(*elon <   0.0) *elon += 360.0;
   while(*elon > 360.0) *elon -= 360.0;


   /* Check for the pole */

   if(fabs(zec) > 1.0)
   {
      *elat = 90.0*zec/fabs(zec);
      *elon = 0.0;
   }


   /* Normal case */

   else
   {
      *elat = asin(zec) * rtod;

      if(fabs(*elat) >= 90.0)
      {
	 *elon = 0.0;

	 if(*elat >  90.0) *elat =  90.0;
	 if(*elat < -90.0) *elat = -90.0;
      }
   }


   return;
}





/***************************************************************************/
/*                                                                         */
/* computeEquPole   computes location (actually co-latitude) of the        */
/* --------------   equatorial pole in the ecliptic system of date         */
/*                                                                         */
/* Inputs:         date in decimal years, julian/besselian flag            */
/* Return value:   pole co-latitude                                        */
/*                                                                         */
/***************************************************************************/


double computeEquPole(double date, int besselian)
{   

   /* 't' is the time offset from the reference date in centuries */

   double pole;
   double t, t2, t3;


   if(coord_debug)
   {
      fprintf(stderr, "DEBUG: computeEquPole()\n");
      fflush(stderr);
   }



   if(besselian)
   {
      t  = (date - 1950.0) * 0.01;
      t2 = t*t;
      t3 = t*t2;

      pole =  (84404.84  - 46.850*t  - 0.0033*t2  + 0.00182*t3) / 3600.0;
   }

   else
   {
      t  = (date - 2000.0) * 0.01;
      t2 = t*t;
      t3 = t*t2;

      pole = (84381.448 - 46.8150*t - 0.00059*t2 + 0.001813*t3) / 3600.0;
   }

   return(pole);
}
