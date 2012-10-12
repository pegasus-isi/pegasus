#include <math.h>
#include <coord.h>

int iway = 1, japply = 1;

static long    idad[181],    idpmad[181],    idd[181],     idpmdd[181];
static long    idaa[19][25], idpmaa[19][25], idda[19][25], idpmda[19][25];
static long    idamm[5][7],  idamam[5][7];

static double  dad[181],     dpmad[181],     dd[181],      dpmdd[181],
               daa[19][25],  dpmaa[19][25],  dda[19][25],  dpmda[19][25],
               dam[5][7],    dpmam[5][7];


void initializeFK5CorrectionData();
void loadFK5Constants();


/****************************************************************************/
/*                                                                          */
/*  besselianToJulianFKCorrection   Systematic FK5-FK4 corrections for      */
/*  -----------------------------   B1950 to J2000                          */
/*                                                                          */
/*  ain,d            B1950 RA,Dec                                           */
/*                                                                          */
/*  dmag             Photometric magnitude                                  */
/*                   (used only if between 1.0 and 7.0)                     */
/*                                                                          */
/*  epoch            Normally 1950.0                                        */
/*                   (provides for an adjustment if not)                    */
/*                                                                          */
/*  corra,corrd      Corrections to RA,Dec                                  */
/*                                                                          */
/*  corrpa,corrpd    Correction for proper motion in RA,Dec                 */
/*                   (seconds of time per century)                          */
/*                                                                          */
/*  All coordinates and deltas are in decimal degrees.                      */
/*                                                                          */
/****************************************************************************/


void
besselianToJulianFKCorrection(double ain, double d, double dmag, double epoch, 
                              double *corra, double *corrd, double *corrpa, 
                              double *corrpd)
{
      static int nthru = 0;
      static double dtor;

      int loc, loc1, loc2, locx1, locx2;
      int n1, n3;

      double interpolateLinear(), interpolateBilinear();

      double dec1, dec2, dtest, fkpdec= 89.999;
      double xmag1, xmag2, a;
      double delepk, delras, deldas, delpma, delpmd, dcosd;
      double cdadec, cdpmad, cdd, cdpmdd, cdaa, cdpmaa;
      double cdda, cdpmda, cdam, cdpmam;


      static double decs[19]={ +85.0, +80.0, +70.0, +60.0, +50.0, +40.0, 
                               +30.0, +20.0, +10.0,   0.0, -10.0, -20.0, 
                               -30.0, -40.0, -50.0, -60.0, -70.0, -80.0, 
                               -85.0};

      static double rads[25]={  0.0,  15.0,  30.0,  45.0,  60.0,  75.0, 
                               90.0, 105.0, 120.0, 135.0, 150.0, 165.0, 
                              180.0, 195.0, 210.0, 225.0, 240.0, 255.0, 
                              270.0, 285.0, 300.0, 315.0, 330.0, 345.0, 
                              360.0};

      if(nthru == 0) 
      {
         dtor = atan(1.0) / 45.0;
         initializeFK5CorrectionData();
         nthru = 1;
      }

      *corra  = 0.0;
      *corrd  = 0.0;
      *corrpa = 0.0;
      *corrpd = 0.0;

      if(fabs(d) > fkpdec) return;

      a = ain;
      while(a <   0.0) a += 360.0;
      while(a > 360.0) a -= 360.0;

      loc1 = (int) (91.0 - d);

      if(loc1 > 180) loc1 = 180;
      if(loc1 <   1) loc1 =   1;

      loc2 = loc1 + 1;

      dec1 = (double) (91 - loc1);
      dec2 = (double) (91 - loc2);

      cdadec = interpolateLinear(  dad[loc1-1],   dad[loc2-1], dec1, dec2, d);
      cdpmad = interpolateLinear(dpmad[loc1-1], dpmad[loc2-1], dec1, dec2, d);
      cdd    = interpolateLinear(   dd[loc1-1],    dd[loc2-1], dec1, dec2, d);
      cdpmdd = interpolateLinear(dpmdd[loc1-1], dpmdd[loc2-1], dec1, dec2, d);

      for (n1=1; n1<19; n1++)
      {
         if(d >= decs[n1])
            break;
      }

      if (n1 > 18) n1 = 18;

      loc2 = n1;
      loc1 = loc2 - 1;

      for (n3=1; n3<25; n3++)
      {
         if(a <= rads[n3])
            break;
      }

      if (n3 > 24) n3 = 24;

      locx2 = n3;
      locx1 = locx2 - 1;

      cdaa   = interpolateBilinear(
                      daa[loc1][locx1], daa[loc1][locx2],
                      daa[loc2][locx1], daa[loc2][locx2],
                      rads[locx1], rads[locx2], decs[loc1], decs[loc2], a, d);

      cdpmaa = interpolateBilinear(
                      dpmaa[loc1][locx1], dpmaa[loc1][locx2], 
                      dpmaa[loc2][locx1], dpmaa[loc2][locx2], 
                      rads[locx1], rads[locx2], decs[loc1], decs[loc2], a, d);

      cdda   = interpolateBilinear(
                      dda[loc1][locx1],   dda[loc1][locx2], 
                      dda[loc2][locx1],   dda[loc2][locx2], 
                      rads[locx1], rads[locx2], decs[loc1], decs[loc2], a, d);

      cdpmda = interpolateBilinear(
                      dpmda[loc1][locx1], dpmda[loc1][locx2], 
                      dpmda[loc2][locx1], dpmda[loc2][locx2], 
                      rads[locx1], rads[locx2], decs[loc1], decs[loc2], a, d);

      cdam   = 0.0;
      cdpmam = 0.0;
      if(dmag >= 1.0 && dmag <= 7.0)
      {
         if( d >= +60.0) 
            loc = 1;

         else if ( d >= 0.0) 
            loc = 2;

         else if ( d >= -30.0)
            loc = 3;

         else if ( d >= -60.0) 
            loc = 4;

         else
            loc = 5;

         loc1 = dmag;
         loc2 = loc1 + 1;

         if(loc2 > 7) loc2 = 7;

         xmag1 = loc1;
         xmag2 = loc2;

         cdam   = interpolateLinear(  dam[loc-1][loc1-1],   dam[loc-1][loc2-1],
                                    xmag1, xmag2, dmag);

         cdpmam = interpolateLinear(dpmam[loc-1][loc1-1], dpmam[loc-1][loc2-1],
                                    xmag1, xmag2, dmag);
      }

      dcosd = cos(d * dtor);

      delras = (cdadec  + cdaa   + cdam)   / dcosd;
      delpma = (cdpmad + cdpmaa + cdpmam) / dcosd;
      deldas =  cdd    + cdda;
      delpmd =  cdpmdd + cdpmda;

      if(epoch > 0.0 && epoch != 1950.0)
      {
          delepk = (epoch - 1950.0) * 0.01;
          delras = delras + delpma*delepk;
          deldas = deldas + delpmd*delepk;
      }

      dtest = deldas / 3600.0;

      if(iway >= 0) 
      {
         if(fabs(d+dtest) > fkpdec) return;
      }

      else
      {
         if(fabs(d-dtest) > fkpdec) return;
      }

      *corrd = dtest;

      *corra = (delras * 15.0) / 3600.0;
      *corrpa = delpma;
      *corrpd = delpmd;

      return;
}




/****************************************************************************/
/*                                                                          */
/*  julianToBesselianFKCorrection  Systematic FK5-FK4 corrections for       */
/*  -----------------------------  J2000 to B1950                           */
/*                                                                          */
/*  Find, by iteration, FK5-FK4 systematic corrections to subtract from     */
/*  RA,Dec when returning position back to B1950/FK4 system.                */
/*                                                                          */
/****************************************************************************/


void
julianToBesselianFKCorrection(double ra, double dec, double xmag, double tobs, 
                              double *corra, double *corrd, double *corrpa, 
                              double *corrpd)
{
      int lway, n10e, n10;
      double rat, dect, fkpdec = 89.999;

      if(fabs(dec) > fkpdec) 
      {
         *corra  = 0.0;
         *corrd  = 0.0;
         *corrpa = 0.0;
         *corrpd = 0.0;
         return;
      }

      lway = iway;
      iway = -1;

      rat  = ra;
      dect = dec;

      n10e = 3;
      for (n10=1; n10<=n10e; n10++)
      {
         besselianToJulianFKCorrection(rat, dect, xmag, tobs, 
                                       corra, corrd, corrpa, corrpd);
         if(n10 == n10e) 
         {
            iway = lway;
            return;
         }

         rat = ra - *corra;
         dect = dec - *corrd;

         while(rat <   0.0) rat += 360.0;
         while(rat > 360.0) rat -= 360.0;
      }

      iway = lway;

      return;
}




/****************************************************************************/
/*                                                                          */
/*  initializeFK5CorrectionData   initializes values for use by             */
/*  ---------------------------   besselianToJulianFKCorrection()           */
/*                                                                          */
/*                                                                          */
/*  Values from Fricke,W., Schwan,H., Lederle,T, Fifth Fundamental          */
/*  Catalogue (FK5), 1988.                                                  */
/*                                                                          */
/****************************************************************************/


void initializeFK5CorrectionData()
{
   int    i, j, k, m, n;



/* idad     delta(ra(dec))* cos(dec):                          */
/*          idad is in units of 0.001s; make dad seconds       */
/*                                                             */
/* idpmad   delta(propermotion_in_ra(dec)) * cos(dec):         */
/*          idpmad is in units of 0.001s/century;              */
/*          make dpmad seconds/century                         */
/*                                                             */
/* idd      delta(dec(dec)):                                   */
/*          idd is in units of 0.01"; make dd "                */
/*                                                             */
/* idpmdd   delta(propermotion_in_dec(dec)):                   */
/*          idpmdd is in units of 0.01"/century;               */
/*          make dpmdd "/century                               */
/*                                                             */
/* For dad, dpmad, dd, and dpmdd values are given              */
/* for each degree of declination:                             */
/*                                                             */
/*        location   1 is associated with dec = +90deg;        */
/*        location  91 is associated with dec =   0deg;        */
/*        location 181 is associated with dec = -90deg.        */

   loadFK5Constants();

   for (i=0; i<181; i++)
   {
      dad[i]   = 0.001 * idad[i];
      dpmad[i] = 0.001 * idpmad[i];
      dd[i]    = 0.01  * idd[i];
      dpmdd[i] = 0.01  * idpmdd[i];
   }



/* idaa      delta(ra(ra)) * cos(dec)                          */
/*           idaa is in units of 0.001s: make daa seconds.     */
/*                                                             */
/* idpmaa    delta(propermotion_in_ra(ra) * cos(dec):          */
/*           idpmaa is in units of 0.001s/century;             */
/*           make dpmaa s/century.                             */
/*                                                             */
/* idda      delta(dec(ra)):                                   */
/*           idda is in units of 0.01"; make dda ".            */
/*                                                             */
/* idpmda    delta(propermotion_in_dec(ra)):                   */
/*           idpmda is in units of 0.01"/century;              */
/*           make dpmda "/century.                             */
/*                                                             */
/* For idaa, idpma, idda and idpmd values given for each       */
/* hour of ra (m=1 for 0 hrs, m=25 for 24 hrs) and             */
/* for dec +/- 85 and for +80 thru -80 in steps of 10 deg      */
/* (n=1  for +85 deg, n=2  for +80 deg, n=10 for 0 deg,        */
/*  n=18 for -80 deg; n=19 for -85 deg)                        */

   for (n=0; n<19; n++)
   {
      for (m=0; m<25; m++)
      {
            daa[n][m] =   idaa[n][m] * 0.001;
          dpmaa[n][m] = idpmaa[n][m] * 0.001;
            dda[n][m] =   idda[n][m] * 0.01;
          dpmda[n][m] = idpmda[n][m] * 0.01;
      }
   }



/* idamm     delta(ra(m)) * cos(dec):                          */
/*           idamm is in units of 0.001s; make dam seconds.    */
/*                                                             */
/* idamam    delta(propermotion_in_ra(m))*cos(dec):            */
/*           idamam is in units of 0.001s/century;             */
/*           make dpmam s/century.                             */
/*                                                             */
/* m         magnitude in this context.                        */
/*           values are given for the following                */
/*           Dec zones:                                        */
/*                                                             */
/*            for k = 1: +90 >= dec >= +60                     */
/*                k = 2: +60 >  dec >=   0                     */
/*                k = 3    0 >  dec >= -30                     */
/*                k = 4: -30 >  dec >= -60                     */
/*                k = 5: -60 >  dec >= -90                     */
/*                                                             */
/* and the following magnitudes : j = magnitude = 1 thru 7     */

   for (k=0; k<5; k++)
   {
      for (j=0; j<7; j++)
      {
         dam[k][j]   =  idamm[k][j] * 0.001;
         dpmam[k][j] = idamam[k][j] * 0.001;
      }
   }


      return;
}




/****************************************************************************/
/*                                                                          */
/*  interpolateLinear  Linear interpolation function                        */
/*  -----------------                                                       */
/*                                                                          */
/****************************************************************************/


double interpolateLinear(double y1, double y2, 
                         double x1, double x2, double x0)
{
   return ((y2-y1)/(x2-x1)*(x0-x1) + y1);
}




/****************************************************************************/
/*                                                                          */
/*  interpolateBilinear  Bilinear interpolation function                    */
/*  -------------------                                                     */
/*                                                                          */
/*    Given a table,                                                        */
/*                                                                          */
/*                               a1      a2                                 */
/*                             -----   ------                               */
/*                     d1    | za1d1 | za2d1 |                              */
/*                     d2    | za1d2 | za2d2 |                              */
/*                                                                          */
/*    find z value where a is value between a1 and a2; d is value           */
/*    between d1 and d2; za2d1 is zvalue at a1,d1, etc.                     */
/*                                                                          */
/****************************************************************************/


double interpolateBilinear(double za1d1, double za2d1, 
                           double za1d2, double za2d2, 
                           double a1,    double a2, 
                           double d1,    double d2, 
                           double a,     double d)
{
      double za1d,za2d;

      za1d = interpolateLinear(za1d1, za1d2, d1, d2, d);
      za2d = interpolateLinear(za2d1, za2d2, d1, d2, d);

      return (interpolateLinear(za1d, za2d, a1, a2, a));
}




/****************************************************************************/
/*                                                                          */
/*  loadFK5Constants  Used in calculating FK4-FK5 corrections               */
/*  ----------------                                                        */
/*                                                                          */
/****************************************************************************/


void loadFK5Constants()
{
   int i, j;

   static long idad1[181] = { 
      -3, -3, -2, -2, -1, -1,  0,  0,  1,  1,  1,  1,  1,  1,  1,
       1,  1,  1,  1,  1,  1,  1,  2,  2,  2,  2,  1,  1,  1,  1,
       0,  0,  0,  0, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
      -1, -1,  0,  0,  0,  1,  1,  1,  1,  2,  2,  1,  1,  1,  1,
       0,  0,  0, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
      -1, -1, -1, -1, -1, -1, -1, -1,  0,  0,  0,  1,  1,  1,  1,
       1,  1,  1,  2,  2,  2,  2,  3,  3,  3,  4,  4,  4,  4,  4,
       4,  4,  3,  2,  2,  1,  0, -1, -1, -2, -3, -3, -3, -3, -3,
      -3, -3, -2, -1, -1,  0,  1,  2,  2,  3,  3,  3,  2,  2,  1,
       0,  0, -1, -2, -2, -3, -3, -3, -3, -3, -3, -3, -3, -4, -5,
      -5, -7, -8, -9,-11,-12,-13,-14,-14,-14,-14,-13,-13,-12,-11,
      -10, -9, -8, -7, -7, -6, -6, -6, -6, -6, -6, -6, -6, -6, -7,
      -7};

   static long idpmad1[181] = {
      -12,-12,-12,-12,-11,-10, -9, -8, -7, -6, -5, -4, -3, -2, -1,
       0,  0,  1,  1,  1,  2,  2,  2,  2,  3,  3,  3,  4,  4,  5,
       5,  5,  5,  6,  6,  6,  6,  6,  6,  5,  5,  5,  5,  4,  4,
       3,  3,  2,  2,  1,  1,  0,  0, -1, -2, -3, -4, -5, -6, -7,
      -9,-10,-11,-12,-14,-14,-15,-16,-16,-16,-16,-15,-15,-14,-13,
      -12,-10, -9, -7, -6, -5, -4, -3, -2, -1,  0,  1,  2,  3,  3,
       4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 13, 14, 14, 13, 13,
      12, 11,  9,  7,  5,  3,  1, -1, -3, -4, -5, -6, -7, -7, -6,
      -5, -4, -2, -1,  1,  4,  6,  7,  9, 10, 11, 12, 12, 11, 10,
       9,  7,  5,  3,  0, -3, -5, -8,-11,-13,-16,-19,-21,-23,-26,
      -28,-30,-33,-35,-38,-41,-43,-46,-48,-51,-53,-54,-56,-56,-57,
      -56,-55,-54,-52,-49,-46,-43,-39,-35,-32,-29,-26,-23,-22,-20,
      -20};

   static long idd1[181] = {
       0,  0,  1,  1,  1,  1,  1,  1,  0,  0, -1, -2, -3, -4, -5,
      -5, -5, -5, -5, -5, -4, -4, -4, -3, -3, -3, -3, -2, -2, -2,
      -1, -1, -1, -1,  0,  0,  0,  0,  0, -1, -1, -1, -1, -1, -1,
       0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0, -1,
      -1, -2, -3, -3, -3, -3, -3, -2, -2, -1,  0,  0,  1,  1,  0,
       0, -1, -1, -2, -2, -3, -3, -3, -3, -3, -3, -3, -3, -3, -3,
      -3, -3, -2, -2, -2, -1, -1,  0,  0,  1,  1,  1,  1,  1,  1,
       1,  1,  0,  0, -1, -1, -1, -2, -2, -1, -1,  0,  0,  1,  2,
       2,  3,  3,  3,  3,  2,  2,  2,  2,  2,  2,  3,  3,  3,  3,
       3,  2,  2,  2,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,
       1,  1,  1,  2,  3,  4,  5,  7,  8,  8,  8,  8,  6,  4,  2,
      -1, -3, -6, -8, -9,-10,-10, -9, -7, -5, -3,  0,  2,  3,  5,
       5};

   static long idpmdd1[181] = {
       1,  1,  2,  3,  3,  4,  4,  3,  2,  0, -2, -5, -8,-11,-13,
      -15,-16,-16,-16,-15,-13,-12,-11,-10, -9, -9, -9, -9, -8, -7,
      -6, -4, -2,  0,  2,  4,  5,  5,  5,  4,  3,  2,  1,  0,  0,
       0,  0,  1,  2,  3,  4,  5,  6,  7,  7,  7,  7,  6,  4,  3,
       1, -1, -2, -3, -4, -3, -2,  0,  2,  5,  7,  8,  9,  9,  8,
       6,  4,  1, -1, -3, -4, -5, -5, -5, -5, -4, -4, -4, -3, -3,
      -3, -2, -1,  1,  3,  5,  7,  9, 11, 13, 15, 16, 16, 16, 16,
      15, 14, 12, 11, 10,  9,  8,  7,  6,  6,  5,  5,  4,  4,  3,
       2,  2,  1,  1,  1,  1,  1,  1,  0,  0, -1, -2, -4, -5, -7,
      -9,-11,-12,-13,-14,-15,-15,-15,-14,-14,-13,-13,-11,-10, -8,
      -5, -3,  0,  3,  6,  9, 11, 13, 15, 16, 16, 16, 14, 12,  8,
       3, -2, -8,-14,-20,-24,-26,-25,-22,-17,-11, -3,  4, 10, 14,
      16};

   static long idaa1[5][25]= {
      {  1,  0, -2, -3, -4, -3, -2,  0,  1,  1,  0, -1, -1,  0,  2,
         4,  4,  4,  2,  0, -1, -1,  0,  1,  1},
      {  1,  0, -2, -3, -4, -3, -1,  0,  1,  1,  0, -1, -1,  0,  2,
         4,  4,  4,  2,  0, -1, -1,  0,  0,  1},
      {  0, -1, -2, -3, -3, -2, -1,  0,  1,  1,  0, -1, -1,  0,  2,
         3,  4,  4,  2,  0, -1, -1, -1,  0,  0},
      {  0, -1, -2, -2, -2, -2, -1,  0,  1,  0,  0, -1, -1,  0,  1,
         3,  4,  3,  2,  1,  0, -1, -1,  0,  0},
      { -1, -1, -2, -2, -2, -1,  0,  0,  0,  0,  0, -1, -1,  0,  1,
         2,  3,  3,  2,  1,  0, -1, -1, -1, -1}};

   static long idaa2[5][25]= {
      { -1, -1, -1, -1, -1, -1,  0,  0,  0,  0,  0,  0,  0,  0,  1,
         2,  2,  2,  2,  1,  0,  0, -1, -1, -1},
      { -1, -1, -1, -1,  0,  0,  0,  0,  0,  0,  0,  0,  0,  1,  1,
         1,  1,  1,  1,  1,  0,  0, -1, -1, -1},
      { -1, -1,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  1,  1,
         1,  0,  0,  0,  0,  0, -1, -1, -2, -1},
      { -1, -1,  0,  1,  1,  1,  1,  1,  1,  0,  0,  0,  0,  1,  0,
         0,  0,  0,  0,  0,  0, -1, -1, -2, -1},
      { -1, -1,  0,  1,  1,  2,  2,  1,  1,  0,  0,  0,  0,  0,  0,
         0, -1, -1,  0,  0,  0, -1, -2, -2, -1}};

   static long idaa3[5][25] = {
      { -1,  0,  0,  1,  1,  2,  2,  2,  1,  1,  0,  0,  0,  0,  0,
       - 1, -1, -1,  0,  0,  0, -1, -2, -2, -1},
      { -1,  0,  0,  1,  1,  1,  1,  1,  1,  0,  0, -1,  0,  0,  0,
         0, -1,  0,  0,  0,  0, -1, -1, -2, -1},
      { -1,  0,  0,  0,  0,  0,  1,  1,  1,  0, -1, -1,  0,  0,  0,
         0,  0,  0,  1,  1,  1,  0, -1, -1, -1},
      { -1,  0,  0,  0, -1, -1,  0,  0,  0, -1, -1, -1,  0,  0,  1,
         0,  0,  1,  1,  2,  1,  1,  0, -1, -1},
      {  0,  0,  0, -1, -1, -1, -1,  0,  0, -1, -1, -1,  0,  0,  1,
         1,  0,  1,  1,  2,  2,  1,  0, -1,  0}};


   static long idaa4[4][25] = {
      { -1,  0,  0,  0, -1, -1,  0,  0,  0, -1, -1, -1,  0,  0,  1,
         0,  0,  1,  1,  2,  2,  1,  0, -1, -1},
      { -1,  0,  0,  0,  0,  0,  0,  1,  0, -1, -1, -1, -1,  0,  0,
         0,  0,  0,  1,  2,  2,  1,  0, -1, -1},
      { -1,  0,  0,  0,  0,  1,  1,  1,  1,  0, -1, -1, -1,  0,  0,
        -1, -1,  0,  1,  2,  2,  1, -1, -1, -1},
      { -1,  0,  0,  1,  1,  1,  1,  1,  1,  0, -1, -1, -1,  0,  0,
        -1, -1,  0,  1,  2,  2,  0, -1, -1, -1}};

   static long idpma1[5][25] = {
      { -8, -9,-10,-11,-10, -8, -4,  2,  7, 10, 10,  6,  2,  2,  6,
        12, 17, 16,  9,  0, -8,-12,-11, -9, -8},
      { -8, -8,-10,-10,-10, -8, -3,  2,  7, 10, 10,  6,  2,  2,  5,
        12, 16, 15,  9,  0, -8,-11,-11, -9, -8},
      { -7, -7, -7, -8, -8, -6, -2,  2,  7,  9,  9,  6,  2,  1,  4,
         9, 13, 13,  7, -1, -8,-11,-10, -8, -7},
      { -5, -4, -4, -5, -4, -3, -1,  2,  6,  8,  7,  5,  2,  1,  3,
         6,  9,  9,  4, -2, -7, -9, -9, -7, -5},
      { -4, -2, -1, -1, -1, -1,  0,  2,  4,  6,  6,  4,  1,  0,  1,
         3,  5,  5,  2, -2, -6, -8, -8, -6, -4}};

   static long idpma2[5][25] = {
      { -2,  0,  2,  2,  2,  2,  2,  2,  3,  4,  4,  3,  1,  0, -1,
         0,  1,  1,  0, -2, -5, -6, -6, -5, -2},
      { -2,  1,  4,  5,  5,  4,  3,  2,  2,  2,  2,  1,  0, -1, -2,
        -2, -2, -2, -2, -2, -3, -5, -5, -4, -2},
      { -1,  2,  5,  6,  6,  5,  4,  2,  1,  1,  0,  0,  0, -1, -2,
        -3, -4, -3, -2, -2, -2, -3, -4, -4, -1},
      { -2,  1,  5,  7,  7,  7,  5,  3,  2,  0,  0,  0,  0, -1, -2,
        -4, -5, -4, -3, -2, -2, -3, -5, -4, -2},
      { -3,  0,  4,  7,  8,  8,  7,  5,  3,  1,  0,  0,  0, -1, -3,
        -4, -5, -5, -3, -2, -2, -4, -5, -6, -3}};

   static long idpma3[5][25] = {
      { -5, -1,  3,  6,  8,  9,  9,  7,  5,  3,  1,  0,  0, -1, -3,
        -4, -5, -5, -3, -3, -3, -5, -7, -7, -5},
      { -6, -3,  2,  6,  8, 10, 10,  9,  7,  5,  3,  1,  0, -1, -3,
        -4, -5, -4, -4, -3, -4, -6, -8, -8, -6},
      { -8, -4,  0,  4,  8, 10, 10, 10,  8,  6,  4,  2,  1, -1, -3,
        -4, -4, -4, -3, -4, -5, -7, -9, -9, -8},
      { -8, -6, -2,  3,  6,  9,  9,  9,  8,  7,  5,  3,  1, -1, -2,
        -3, -2, -2, -2, -3, -5, -7, -9, -9, -8},
      { -9, -7, -4,  0,  4,  6,  7,  7,  6,  6,  6,  4,  2,  0, -1,
        -1,  0,  0,  0, -2, -4, -6, -7, -9, -9}};

   static long idpma4[4][25] = {
      { -9, -8, -6, -2,  1,  3,  3,  3,  4,  5,  6,  5,  3,  1,  0,
         1,  2,  3,  2,  1, -1, -4, -6, -7, -9},
      { -8, -9, -7, -4, -2,  0,  0,  0,  1,  3,  5,  5,  3,  2,  1,
         3,  5,  6,  5,  3,  1, -1, -4, -6, -8},
      { -8, -9, -9, -6, -4, -3, -3, -3, -1,  2,  5,  5,  4,  2,  2,
         4,  6,  8,  7,  5,  3,  0, -2, -5, -8},
      { -8, -9, -9, -7, -4, -3, -4, -4, -2,  1,  4,  5,  4,  2,  2,
         4,  7,  8,  8,  6,  3,  1, -2, -5, -8}};

   static long idda1[5][25] = {
      { -4, -3, -2, -2, -2, -2, -1,  0,  2,  4,  5,  5,  4,  2,  2,
         2,  2,  2,  1,  0, -2, -4, -5, -5, -4},
      { -4, -3, -2, -2, -1, -1, -1,  1,  2,  4,  4,  4,  3,  2,  2,
         2,  2,  2,  1,  0, -2, -4, -4, -5, -4},
      { -3, -3, -2, -1, -1, -1,  0,  1,  2,  3,  3,  3,  3,  2,  2,
         1,  1,  1,  0,  0, -2, -3, -3, -3, -3},
      { -3, -2, -2, -1, -1,  0,  1,  1,  2,  2,  2,  2,  2,  2,  2,
         1,  1,  0,  0, -1, -1, -2, -2, -2, -3},
      { -2, -2, -2, -1,  0,  0,  1,  1,  2,  2,  2,  2,  2,  2,  1,
         1,  1,  0,  0, -1, -1, -2, -2, -2, -2}};

   static long idda2[5][25] = {
      { -2, -2, -2, -1,  0,  0,  1,  1,  2,  2,  2,  1,  1,  1,  1,
         1,  1,  0,  0, -1, -1, -2, -2, -2, -2},
      { -2, -2, -2, -1,  0,  0,  1,  1,  2,  2,  2,  1,  1,  1,  1,
         1,  1,  0,  0, -1, -1, -2, -2, -2, -2},
      { -2, -2, -1, -1,  0,  1,  1,  1,  2,  2,  1,  1,  1,  1,  1,
         1,  1,  0,  0, -1, -1, -2, -2, -2, -2},
      { -2, -2, -1,  0,  0,  1,  1,  1,  2,  1,  1,  1,  1,  1,  1,
         0,  0,  0,  0,  0, -1, -1, -2, -2, -2},
      { -2, -1, -1,  0,  1,  1,  1,  2,  2,  2,  1,  1,  1,  0,  0,
         0,  0,  0,  0, -1, -1, -2, -2, -2, -2}};

   static long idda3[5][25] = {
      { -2, -1, -1,  0,  1,  1,  2,  2,  2,  2,  1,  1,  0,  0,  0,
         0,  0,  0,  0, -1, -1, -2, -2, -2, -2},
      { -2, -1, -1,  0,  1,  2,  2,  3,  2,  2,  1,  1,  0,  0,  0,
         0,  0, -1, -1, -1, -1, -2, -2, -2, -2},
      { -2, -2, -1,  0,  1,  2,  3,  3,  2,  1,  1,  0,  0,  0,  0,
         0, -1, -1, -1, -1, -1, -1, -2, -2, -2},
      { -1, -1, -1,  0,  2,  3,  3,  3,  2,  1,  0, -1,  0,  0,  0,
         0, -1, -1, -1, -1, -1, -1, -1, -1, -1},
      { -1, -1, -1,  0,  2,  3,  3,  3,  2,  0, -1, -1, -1,  0,  0,
         0, -1, -1, -1, -1, -1,  0,  0,  0, -1}};

   static long idda4[4][25] = {
      { -1, -2, -2, -1,  1,  3,  4,  3,  1, -1, -2, -2, -1,  0,  1,
         1,  0, -1, -2, -1,  0,  1,  1,  0, -1},
      { -1, -3, -3, -2,  1,  3,  4,  3,  1, -2, -3, -3, -1,  1,  2,
         2,  0, -1, -2, -1,  0,  2,  2,  1, -1},
      { -2, -4, -5, -3,  0,  3,  5,  3,  0, -3, -4, -3, -1,  2,  4,
         3,  1, -1, -2, -2,  1,  3,  3,  1, -2},
      { -2, -5, -5, -4,  0,  3,  5,  4,  0, -3, -4, -3, -1,  3,  4,
         4,  1, -1, -2, -2,  1,  3,  3,  1, -2}};

   static long idpmd1[5][25] = {
      {-18,-14,-10, -9, -9, -7, -3,  3,  7,  9,  9,  7,  5,  4,  4,
         6, 10, 14, 16, 12,  4, -7,-15,-19,-18},
      {-16,-13,-11, -9, -9, -7, -3,  2,  6,  8,  8,  6,  5,  4,  5,
         7, 10, 13, 14, 11,  4, -5,-13,-17,-16},
      {-13,-12,-11,-10, -8, -6, -2,  2,  4,  4,  4,  4,  4,  5,  6,
         7,  9, 10, 10,  9,  4, -2, -8,-11,-13},
      { -9,-10,-10, -9, -7, -4, -1,  1,  2,  2,  1,  2,  3,  5,  6,
         7,  7,  7,  7,  6,  4,  1, -3, -6, -9},
      { -7, -8, -8, -7, -5, -2,  1,  2,  2,  2,  1,  1,  2,  3,  4,
         5,  5,  5,  4,  4,  3,  1, -2, -5, -7}};

   static long idpmd2[5][25] = {
      { -7, -8, -7, -5, -2,  0,  3,  4,  4,  4,  3,  2,  1,  2,  2,
         3,  3,  4,  3,  3,  1, -2, -4, -6, -7},
      { -9, -8, -6, -4, -1,  2,  4,  6,  6,  5,  4,  3,  2,  1,  2,
         2,  3,  3,  3,  2, -1, -3, -6, -8, -9},
      {-10, -9, -7, -4, -2,  2,  4,  6,  7,  6,  4,  3,  2,  2,  3,
         3,  3,  3,  3,  2, -1, -4, -7, -9,-10},
      { -9, -8, -7, -5, -2,  0,  3,  5,  6,  5,  3,  2,  2,  3,  3,
         4,  4,  4,  4,  3,  0, -3, -7, -9, -9},
      { -8, -7, -6, -4, -3, -1,  2,  4,  4,  3,  2,  1,  1,  2,  3,
         3,  4,  5,  5,  4,  2, -2, -6, -8, -8}};

   static long idpmd3[5][25] = {
      { -7, -6, -5, -3, -2,  0,  2,  3,  3,  1,  0, -1, -1,  0,  2,
         3,  4,  6,  6,  6,  3, -1, -5, -7, -7},
      { -6, -6, -5, -3, -1,  1,  3,  3,  2,  0, -2, -3, -2,  0,  2,
         3,  4,  5,  6,  5,  3,  1, -3, -5, -6},
      { -5, -7, -7, -5, -1,  2,  4,  4,  1, -2, -4, -4, -2,  1,  4,
         5,  4,  4,  3,  3,  3,  2,  0, -3, -5},
      { -4, -7, -8, -6, -2,  1,  3,  3,  0, -4, -5, -3,  0,  5,  7,
         7,  4,  1,  0,  0,  2,  3,  3,  0, -4},
      { -1, -6, -9, -8, -4,  0,  2,  1, -2, -5, -5, -2,  3,  9, 11,
         8,  3, -2, -4, -3,  1,  5,  6,  3, -1}};

   static long idpmd4[4][25] = {
      {  1, -5, -9, -9, -5,  0,  3,  2, -2, -6, -7, -2,  5, 12, 14,
         9,  1, -6, -9, -7,  0,  6,  9,  7,  1},
      {  3, -5,-10, -9, -4,  4,  8,  5, -1, -9,-11, -5,  5, 14, 17,
        10, -1,-12,-16,-11, -2,  8, 12, 10,  3},
      {  4, -6,-12,-10, -2,  8, 13, 10,  0,-11,-15, -9,  4, 16, 19,
        11, -4,-17,-21,-15, -2, 10, 16, 13,  4},
      {  4, -7,-13,-10, -1, 10, 15, 12,  0,-11,-16,-10,  4, 16, 20,
        11, -4,-18,-23,-16, -3, 10, 17, 14,  4}};

   static long idamm1[5][7] = { 
      {  -2, -2, -1, -1,  0,  1,  2},
      {  -2, -1, -1,  0,  0,  0,  1},
      {  -4, -2, -1,  0,  0,  0,  0},
      {  -6, -3, -1,  0,  1,  0,  0},
      {  -8, -5, -2,  0,  1,  1,  1}};

   static long idamam1[5][7] = {
      { -11, -8, -5, -2,  1,  3,  6},
      {  -7, -3, -1,  0,  1,  0, -1},
      { -14, -8, -3,  0,  1,  1, -1},
      { -28,-17, -8, -1,  3,  5,  4},
      { -39,-24,-12, -3,  4,  8,  9}};


   for (i=0; i<181; i++)
     idad[i] = idad1[i];

   for (i=0; i<181; i++)
     idpmad[i] = idpmad1[i];

   for (i=0; i<181; i++)
     idd[i] = idd1[i];

   for (i=0; i<181; i++)
     idpmdd[i] = idpmdd1[i];

   for(i=0; i<5; i++)
     for (j=0; j<25; j++)
        idaa[i][j] = idaa1[i][j]; 

   for(i=0; i<5; i++)
     for (j=0; j<25; j++)
        idaa[5+i][j] = idaa2[i][j];

   for(i=0; i<5; i++)
     for (j=0; j<25; j++)
        idaa[10+i][j] = idaa3[i][j];

   for(i=0; i<4; i++)
     for (j=0; j<25; j++)
        idaa[15+i][j] = idaa4[i][j];

   for(i=0; i<5; i++)
     for (j=0; j<25; j++)
        idpmaa[i][j] = idpma1[i][j];

   for(i=0; i<5; i++)
     for (j=0; j<25; j++)
        idpmaa[5+i][j] = idpma2[i][j];

   for(i=0; i<5; i++)
     for (j=0; j<25; j++)
        idpmaa[10+i][j] = idpma3[i][j];

   for(i=0; i<4; i++)
     for (j=0; j<25; j++)
        idpmaa[15+i][j] = idpma4[i][j];

   for(i=0; i<5; i++)
     for (j=0; j<25; j++)
        idda[i][j] = idda1[i][j];

   for(i=0; i<5; i++)
     for (j=0; j<25; j++)
        idda[5+i][j] = idda2[i][j];

   for(i=0; i<5; i++)
     for (j=0; j<25; j++)
        idda[10+i][j] = idda3[i][j];

   for(i=0; i<4; i++)
     for (j=0; j<25; j++)
        idda[15+i][j] = idda4[i][j];

   for(i=0; i<5; i++)
     for (j=0; j<25; j++)
        idpmda[i][j] = idpmd1[i][j];

   for(i=0; i<5; i++)
     for (j=0; j<25; j++)
        idpmda[5+i][j] = idpmd2[i][j];

   for(i=0; i<5; i++)
     for (j=0; j<25; j++)
        idpmda[10+i][j] = idpmd3[i][j];

   for(i=0; i<4; i++)
     for (j=0; j<25; j++)
        idpmda[15+i][j] = idpmd4[i][j];

   for (i=0; i<5; i++)
     for (j=0; j<7; j++)
        idamm[i][j] = idamm1[i][j];

   for (i=0; i<5; i++)
     for (j=0; j<7; j++)
        idamam[i][j] = idamam1[i][j];
}
