#include <stdio.h>
#include <math.h>
#include <coord.h>



/***************************************************************************/
/*                                                                         */
/* precessJulian   Julian precession of Equatorial  coordinates.           */
/* -------------                                                           */
/*                                                                         */
/* ( see precessJulianWithProperMotion() )                                 */
/*                                                                         */
/***************************************************************************/


void precessJulian(double epochin,  double  rain,  double  decin, 
                   double epochout, double *raout, double *decout)
{
   double rapm, decpm;

   if(coord_debug)
   {
      fprintf(stderr, "DEBUG: precessJulian()\n");
      fflush(stderr);
   }


   precessJulianWithProperMotion
      (epochin,  rain,  decin,  
       epochout, raout, decout, 
       0., 0., 0., 0., &rapm, &decpm);

   return ; 
}



/***************************************************************************/
/*                                                                         */
/* precessJulianWithProperMotion   Besselian precession of Equatorial      */
/* -----------------------------   coordinates.                            */
/*                                                                         */
/*  Also allows precession of proper motion values (making use of          */
/*  parallax and radial velocity if not zero).                             */
/*                                                                         */
/*  Arguments (all double precision):                                      */
/*                                                                         */
/*    epochin         epoch of input position (in year - e.g. 1950.0d0)    */
/*    rain,  decin    input position in decimal degrees at epochin         */
/*    pmain, pmdin    proper motion in ra,dec in seconds of time           */
/*                    per Tropical century.                                */
/*                                                                         */
/*    pin             parallax in arc seconds (0.0d0 if none)              */
/*    vin             radial velocity in km/sec (0.0d0 if none)            */
/*    epochout        epoch of output position (in year - e.g. 1992.0d0)   */
/*    raout, decout   position in decimal degrees at epochout              */
/*    rapm, decpm     proper motion in ra,dec in seconds of time           */
/*                    per Tropical century for epochout                    */
/*                                                                         */
/***************************************************************************/


void precessJulianWithProperMotion
   (double epochin,  double  rain,  double  decin, 
    double epochout, double *raout, double *decout, 

    double pmain, double pmdin, double pin, double vin, 
    double *rapm, double *decpm)
{
   static double rtod, dtor, delt, f, p[3][3];
   
   static double saveepochin  = -1.0;
   static double saveepochout = -1.0;

   int     i;
   double  zetar, zr, thetar, zeta, z, theta;
   double  tau,  t;
   double  r0[3], rdot0[3], r[3], rdot[3], pivelf;
   double  czet, cthet, cz, szet, sthet, sz;
   double  cosa, sina, cosd, sind, rar, decr;
   double  cosao, sinao, cosdo, sindo;
   double  pmas, pmds, rdiv, raoutr, decoutr, rapms, decpms;
   double  duda[3], dudd[3];

   if(coord_debug)
   {
      fprintf(stderr, "DEBUG: precessJulianWithProperMotion()\n");
      fflush(stderr);
   }



   /* Requested input is the same as input: No-op */

   if(epochin == epochout)
   {
      *raout  = rain;
      *decout = decin;
      *rapm   = pmain;
      *decpm  = pmdin;
      return;
   }


   /* First time through for a conversion, set up constants and   */
   /* the transformation parameters for this specific time change */

   if(epochin  != saveepochin 
   || epochout != saveepochout) 
   {
      /* Radians <-> Degrees */

      dtor = atan(1.0) / 45.0;
      rtod = 1.0 / dtor;


      /* 1 arcsec, in radians */

      f = dtor / 3600.;


      /* TAU, T, DELT in Tropical centuries */

      tau  = (epochin  - 2000.0   ) * 0.01;
      t    = (epochout - epochin) * 0.01;

      delt = t;

   
      /* ZETA, THETA, Z from FK5 Catalogue in seconds of arc */
   
      zeta  = (2306.2181 + 1.39656*tau - 0.000139*tau*tau)*t +
              (0.30188 - 0.000344*tau)*t*t + 0.017998*t*t*t;

      z     = (2306.2181 + 1.39656*tau - 0.000139*tau*tau)*t +
              (1.09468 + 0.000066*tau)*t*t + 0.018203*t*t*t;

      theta = (2004.3109 - 0.85330*tau - 0.000217*tau*tau)*t -
              (0.42665 + 0.000217*tau)*t*t - 0.041833*t*t*t;

      zetar  = ( zeta/3600.0)*dtor;
      zr     = (    z/3600.0)*dtor;
      thetar = (theta/3600.0)*dtor;

      czet  = cos(zetar);
      szet  = sin(zetar);

      cz    = cos(zr);
      sz    = sin(zr);

      cthet = cos(thetar);
      sthet = sin(thetar);


      /* P matrix */
   
      p[0][0] =  czet*cthet*cz - szet*sz;
      p[1][0] =  czet*cthet*sz + szet*cz;
      p[2][0] =  czet*sthet;

      p[0][1] = -szet*cthet*cz - czet*sz;
      p[1][1] = -szet*cthet*sz + czet*cz;
      p[2][1] = -szet*sthet;

      p[0][2] = -sthet*cz;
      p[1][2] = -sthet*sz;
      p[2][2] =  cthet;


      /* Remember the current tranform */

      saveepochin  = epochin;
      saveepochout = epochout;
   }


   /* Apply the precession formulae */

   rar  = dtor*rain;
   decr = dtor*decin;

   cosa = cos(rar);
   sina = sin(rar);

   cosd = cos(decr);
   sind = sin(decr);

   r0[0] = cosd*cosa;
   r0[1] = cosd*sina;
   r0[2] = sind;

   pmas = pmain * 15.0;
   pmds = pmdin;

   if(vin == 0.0 || pin == 0.0)
   {
      rdot0[0] = f*(pmas*(-cosd)*sina + pmds*(-sind)*cosa);
      rdot0[1] = f*(pmas*cosd*cosa + pmds*(-sind)*sina);
      rdot0[2] = f*(pmds*cosd);
   }

   else
   {
      pivelf = 21.094953 * pin * vin;

      rdot0[0] = f*(pmas*(-cosd)*sina + pmds*(-sind)*cosa+pivelf*r0[0]);
      rdot0[1] = f*(pmas*cosd*cosa + pmds*(-sind)*sina + pivelf*r0[1]);
      rdot0[2] = f*(pmds*cosd + pivelf*r0[2]);
   }

   for(i=0; i<3; i++)
   {
      rdot[i] = p[i][0]*rdot0[0] + p[i][1]*rdot0[1] + p[i][2]*rdot0[2];

      r[i]    = p[i][0]*(r0[0]+rdot0[0]*delt) +
                p[i][1]*(r0[1]+rdot0[1]*delt) +
                p[i][2]*(r0[2]+rdot0[2]*delt);
   }

   raoutr  = atan2(r[1], r[0]);
   decoutr = atan2(r[2], sqrt(r[0]*r[0] + r[1]*r[1]));

   rdiv  = sqrt(r[0]*r[0] + r[1]*r[1] + r[2]*r[2]);

   cosdo = cos(decoutr);
   sindo = sin(decoutr);

   cosao = cos(raoutr);
   sinao = sin(raoutr);

   duda[0] = -cosdo*sinao;
   duda[1] =  cosdo*cosao;
   duda[2] =  0.0;

   dudd[0] = -sindo*cosao;
   dudd[1] = -sindo*sinao;
   dudd[2] =  cosdo;

   rdot[0] = rdot[0] / rdiv;
   rdot[1] = rdot[1] / rdiv;
   rdot[2] = rdot[2] / rdiv;

   rapms  = (rdot[0]*duda[0] + rdot[1]*duda[1] + rdot[2]*duda[2])/
            (f*cosdo*cosdo);

   decpms = (rdot[0]*dudd[0] + rdot[1]*dudd[1] + rdot[2]*dudd[2])/f;

   *raout = raoutr * rtod;

   while(*raout <   0.0) *raout += 360.0;
   while(*raout > 360.0) *raout -= 360.0;

   *decout = decoutr * rtod;

   if(*decout >  90.0) *decout =  90.0;
   if(*decout < -90.0) *decout = -90.0;

   *rapm  = rapms / 15.0;
   *decpm = decpms;

   return;
}
