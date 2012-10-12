#include <stdio.h>
#include <math.h>
#include <coord.h>

extern int japply;

void correctCoordinateRange();
void getEquETermCorrection();
void getEclETermCorrection();
void refinedEquETermCorrection();
void refinedEclETermCorrection();
void correctForEquatorialETerms();
void correctForEclipticETerms();



/***************************************************************************/
/*                                                                         */
/* convertBesselianToJulian   Convert coordinates from B1950 to J2000      */
/* ------------------------                                                */
/*                                                                         */
/*                                                                         */
/*  All arguments are double precision, except ieflg which is integer.     */
/*  Input values:                                                          */
/*                                                                         */
/*  equinoxin  equinox of the input position                               */
/*                                                                         */
/*  ra, dec    Coordinates (decimal degrees) at equinoxin                  */
/*                                                                         */
/*  obsdatein  Year of observation (i.e. when the object was observed at   */
/*             this position (e.g. 1983.5).  Defaults to equinoxin.        */
/*                                                                         */
/*  ieflg      Flag allowing removal of E-terms of aberration if any       */
/*             (usually they are present):                                 */
/*                                                                         */
/*             ieflg = -1 do not remove E-terms (there are none).          */
/*             ieflg = +1 any value except -1 indicates E-terms            */
/*                        are present and are to be removed.               */
/*                                                                         */
/*                                                                         */
/*  Returned values:                                                       */
/*                                                                         */
/*  raout     Right ascension at equator and equinox of J2000.             */
/*  decout    Declination at equator and equinox of J2000.                 */
/*                                                                         */
/***************************************************************************/


void convertBesselianToJulian(double equinoxin, double ra, double dec, 
                              double obsdatein, int ieflg, 
                              double *raout, double *decout)
{
   double obsdateb, obsdatej, jde, equinox, rat50, dect50;
   double rat, dect, delt, dela;
   double corra, corrd, corrpa, corrpd;


   if(coord_debug)
   {
      fprintf(stderr, "DEBUG: convertBesselianToJulian()\n");
      fflush(stderr);
   }



   /* Set default obsdate */

   equinox  = fabs(equinoxin);
   obsdateb = fabs(obsdatein);

   if(obsdateb == 0.)
      obsdateb = equinox;



   /* Determine FK5-FK4 systematic correction */
   /* using equinox B1950 postion             */

   if(japply) 
   {
      if(equinox == 1950.)
      {
          rat50 =  ra;
         dect50 = dec;
      }
      else
          precessBesselian(equinox, ra, dec, 1950., &rat50, &dect50);


      besselianToJulianFKCorrection(rat50, dect50, 0., obsdateb, 
                                    &corra, &corrd, &corrpa, &corrpd);

       rat50 =  rat50 + corra;
      dect50 = dect50 + corrd;

      correctCoordinateRange(&rat50, &dect50);

      equinox = 1950.;
   }
   else
   {
       rat50 =  ra;
      dect50 = dec;
   }



   /* Use old Newcomb formula to precess from equinox to obsdate */

   if (obsdateb != equinox)
       precessBesselian( equinox, rat50, dect50, obsdateb, &rat, &dect);
   else
   {
        rat =  rat50;
       dect = dect50;
   }


   /* Correct for right ascension at the mean epoch of observation    */
   /*                                                                 */
   /*  B1950 = JDE 2433282.4235  365.2421988 tropical days per year   */
   /*  J2000 = JDE 2451545.0000  365.2500000 Julian   days per year   */

   jde      = ((obsdateb - 1950.) * 365.2421988) + 2433282.4235;     

   obsdatej = 2000. + ((jde - 2451545.) / 365.25);



   /* Remove E-terms, if necessary.                                */
   /*                                                              */
   /* The mean positions in all star catalogues published prior    */
   /* to 1984 contain the effects of elliptic aberration (E-terms) */

   if(ieflg != -1)
      correctForEquatorialETerms(obsdatej, &rat, &dect);



   /* Apply the equinox correction (use obsdateb not obsdatej) */

   delt = (obsdateb - 1950.) * 0.01;
   dela = ((0.035 + 0.085*delt) * 15.0) / 3600.;

   rat  = rat + dela;
   while(rat > 360.) rat -= 360.;
   while(rat <   0.) rat += 360.;

   correctCoordinateRange(&rat, &dect);



   /* Use new formula to precess from obsdatej to 2000 Jan. 1.5 */

   precessJulian(obsdatej, rat, dect, 2000., raout, decout);
}






/***************************************************************************/
/*                                                                         */
/* convertJulianToBesselian   Convert coordinates from J2000 to B1950      */
/* ------------------------                                                */
/*                                                                         */
/*  This routine is just the reverse logic of convertBesselian to Julian.  */
/*                                                                         */
/*                                                                         */
/*  All arguments are double precision, except ieflg which is integer.     */
/*  Input values:                                                          */
/*                                                                         */
/*  ra, dec    Coordinates (decimal degrees) at equinoxin                  */
/*                                                                         */
/*  obsdatein  Year of observation (i.e. when the object was observed at   */
/*             this position (e.g. 1983.5).  Defaults to equinoxin.        */
/*                                                                         */
/*  ieflg      Flag allowing removal of E-terms of aberration if any       */
/*             (usually they are present):                                 */
/*                                                                         */
/*             ieflg = -1 do not remove E-terms (there are none).          */
/*             ieflg = +1 any value except -1 indicates E-terms            */
/*                        are present and are to be removed.               */
/*                                                                         */
/*  equinoxout equinox of the output position                              */
/*                                                                         */
/*                                                                         */
/*  Returned values:                                                       */
/*                                                                         */
/*  raout     Right ascension at equator and equinox of J2000.             */
/*  decout    Declination at equator and equinox of J2000.                 */
/*                                                                         */
/***************************************************************************/


void convertJulianToBesselian(double ra, double dec, 
                              double obsdatein, int ieflg, double equinoxout, 
                              double *raout, double *decout)
{
   double obsdatej, jde, obsdateb;
   double rat, dect, delt, dela;
   double rat50, dect50;
   double corra, corrd, corrpa, corrpd;

   double equinox1 = 2000., equinox2;


   if(coord_debug)
   {
      fprintf(stderr, "DEBUG: convertJulianToBesselian()\n");
      fflush(stderr);
   }



   /* Set default equinox */

   if(equinoxout != 0.) 
      equinox2 = fabs(equinoxout);
   else
      equinox2 = 1950.;



   /* Set default obsdate */

   if(obsdatein != 0.) 
      obsdateb = fabs(obsdatein);
   else
      obsdateb = equinox2;



   /* Correct for right ascension at the mean epoch of observation    */
   /*                                                                 */
   /*  B1950 = JDE 2433282.4235  365.2421988 tropical days per year   */
   /*  J2000 = JDE 2451545.0000  365.2500000 Julian   days per year   */

   jde      = ((obsdateb - 1950.) * 365.2421988) + 2433282.4235;     

   obsdatej = 2000. + ((jde - 2451545.)/365.25);



   /* Use new formula to precess from equinox to obsdatej */

   precessJulian( equinox1, ra, dec, obsdatej, &rat, &dect);



   /* Remove the equinox correction (use obsdateb not obsdatej) */

   delt = (obsdateb - 1950.) * 0.01;
   dela = ((0.035 + 0.085*delt) * 15.0) / 3600.;

   rat = rat - dela;
   while(rat > 360.) rat -= 360.;
   while(rat <   0.) rat += 360.;



   /* Add E-terms, if necessary.                                   */
   /* The mean positions in all star catalogues published prior    */
   /* to 1984 contain the effects of elliptic aberration (E-terms) */

   if(ieflg != -1)  
      correctForEclipticETerms(obsdatej, &rat, &dect);




   /* Determine (and remove) FK5-FK4 systematic correction */
   /* using equinox B1950 postion                          */

   if(japply) 
   {
       if(obsdateb == 1950.) 
       {
           rat50 =  rat;
          dect50 = dect;
       }

       else
          precessBesselian(obsdateb, rat, dect, 1950., &rat50, &dect50);

       julianToBesselianFKCorrection(rat50, dect50, 0., obsdateb, 
                                     &corra, &corrd, &corrpa, &corrpd);

        rat50 =  rat50 - corra;
       dect50 = dect50 - corrd;

       correctCoordinateRange(&rat50, &dect50);

       if(equinox2 != 1950.) 
           precessBesselian(1950., rat50, dect50, equinox2, raout, decout);

       else
       {
           *raout =  rat50;
          *decout = dect50;
       }
   }

   else
   {
      if(obsdateb != equinox2) 
         precessBesselian(obsdateb, rat, dect, equinox2, raout, decout);

      else
      {
         *raout = rat;
         *decout = dect;
      }     
   }

   return;
}




/***************************************************************************/
/*                                                                         */
/* correctCoordinateRange   Make sure RA and Dec are in the correct        */
/* ----------------------   value ranges (wrap around the sky if we        */
/*                          go over the pole.                              */
/*                                                                         */
/***************************************************************************/


void correctCoordinateRange(double *ra, double *dec)
{
   if(coord_debug)
   {
      fprintf(stderr, "DEBUG: correctCoordinateRange()\n");
      fflush(stderr);
   }


    while(*ra > 360.) *ra -= 360.;
    while(*ra <   0.) *ra += 360.;

    if(fabs(*dec) > 90.)
    {
       *ra = *ra + 180.;

       if(*ra >= 360.) 
          *ra = *ra - 360.;

       if(*dec >   0.)
          *dec =   180. - *dec;
       else
          *dec = -(180. + *dec);
   }

   return;
}





/***************************************************************************/
/*                                                                         */
/* E-TERM UTILITY ROUTINES:  All the functions below are for use in        */
/* -----------------------   when correcting for "E-terms".                */
/*                                                                         */
/* The mean positions in all star catalogues published prior to 1984       */
/* contain the effects of elliptic aberration (E-terms)                    */
/*                                                                         */
/***************************************************************************/





/***************************************************************************/
/*                                                                         */
/* getEquETermCorrection:   Compute E-terms to be removed at               */
/* ----------------------    a specific RA, Dec                            */
/*                                                                         */
/*  From Suppl. to Astron. Alman. 1984 (also 1961 supp. page 144)          */
/*  see also Standish, A&A 115, 20-22 (1982)                               */
/*                                                                         */
/*  Since the E-terms (terms of elliptic aberration) change so slowly      */
/*    (Smart,Textbook on Spherical Astronomy, Sixth Ed. Section 108,p186)  */
/*     these values do not require t as input and will be valid in the     */
/*     1950 to 2000 time span we are dealing with.                         */
/*                                                                         */
/*  The 1961 supp called these equations an approximation and stated that  */
/*  small errors in this procedure are usually negligible. However, they   */
/*  did not explain what lead up to the procedure:  "The form of the       */
/*  equations of condition and their solution are not discussed here."     */
/*                                                                         */
/*  All arguments are decimal degrees.                                     */
/*                                                                         */
/***************************************************************************/


void getEquETermCorrection(double ra, double dec, double *dra, double *ddec)
{
   static int    nthru = 0;
   static double e1, e2, e3, e4, dtor;

   double dcosd, alplus; 


   if(coord_debug)
   {
      fprintf(stderr, "DEBUG: getEquETermCorrection()\n");
      fflush(stderr);
   }


   /* First time through, calculate a few constants */

   if(nthru == 0)
   {
      /* NOTE: e1 = (0.0227 * 15.0) / 3600.0 = 0.341/3600 = e3 */

      dtor = atan(1.0) / 45.0;

      e2 = 11.25 * 15.0;
      e3 = 0.341 / 3600.;
      e4 = 0.029 / 3600.;

      e1 = e3;

      nthru = 1;
   }


   /* RA correction term */

   alplus = ra + e2;

   if(alplus >= 360.) 
      alplus = alplus - 360.;

   alplus = alplus * dtor;


   /* Dec correction term */

   dcosd = cos(dtor * dec);


   /* Calculate deltas to RA and Dec */

   if(fabs(dec) >= 90. || fabs(dcosd) < 1.0e0-27)
   {
      *dra  = 0.;
      *ddec = 0.;
   }

   else
      *dra = (e1 * sin(alplus)) / dcosd;

   *ddec = (e3 * cos(alplus) * sin(dec*dtor)) + (e4 * dcosd);

   return;
}




/***************************************************************************/
/*                                                                         */
/* getEclETermCorrection:   Compute E-terms to be removed at a specific    */
/* ----------------------   Elon,Elat,Date (returned as deltas to RA, Dec) */
/*                                                                         */
/*                                                                         */
/* E-term formulas from ASTRONOMICAL ALGORITHMS by Jean Meeus (1991) ch.22 */
/*                                                                         */
/*   Equations as presented are for computing E-terms from position        */
/*   that does not contain E-terms.  To get better answer (when            */
/*   splitting hairs), iterate to get best E-term (for position that       */
/*   has E-terms) to be removed. Subroutine refinedEclETermCorrection()    */
/*   may be called to do the iteration.                                    */
/*                                                                         */
/*   These formulas for E-terms, as function of ecliptic lon,lat           */
/*   also appear in  Spherical Astronomy by R.Green (1985),page 192;       */
/*   and in Textbook on Spherical Astronomy, Sixth Ed.,by Smart            */
/*   (1977), page 186.                                                     */
/*                                                                         */
/*   To remove E-terms, subtract dra & ddec from elon & elat,              */
/*   respectively, in the calling program.                                 */
/*                                                                         */
/*   To add back E-terms, add dra & ddec to elon & elat, respectively      */
/*   in the calling program.                                               */
/*                                                                         */
/*  All arguments are decimal degrees.  Epoch in years (e.g. 1950)         */
/*                                                                         */
/***************************************************************************/

void getEclETermCorrection(double epoch, double elon, double elat, 
                           double *dra, double *ddec)
{
   static int nthru = 0;
   static double dtor, kappa, lepoch=-1.0, ecc, perihelion;

   double t, t2, lon, lat;


   if(coord_debug)
   {
      fprintf(stderr, "DEBUG: getEclETermCorrection()\n");
      fflush(stderr);
   }



   /* First time through, calculate some constants */

   if(nthru == 0) 
   {
      /* Constant of aberration, kappa = 20.49552" */
      /* (new; old was 20.496)                     */

      dtor   = atan(1.0) / 45.0;
      kappa  = 0.0056932;
      nthru  = 1;
   }

   *dra  = 0.;
   *ddec = 0.;



   /* Epoch-dependent terms:                                 */
   /* Don't recompute if epoch doesn't change                */

   /* ecc        = eccentricity of the Earth's orbit         */
   /* perihelion = longitude of the perihelion of this orbit */

   if(epoch != lepoch) 
   {
      t          = (epoch - 2000.) * 0.01;
      t2         = t*t;
      lepoch     = epoch;
     
      ecc        =  0.016708617 - 0.000042037*t - 0.0000001236*t2;
      perihelion = (102.93735 + 0.71953*t + 0.00046*t2) * dtor;
   }



   /* Calculate deltas */

   if(fabs(elat) > 89.999) return;

   lon = dtor * elon;
   lat = dtor * elat;

   *dra  = ecc * kappa * cos(perihelion-lon) / cos(lat);
   *ddec = ecc * kappa * sin(perihelion-lon) * sin(lat);

   return;
}




/****************************************************************************/
/*                                                                          */
/* refinedEquETermCorrection  Progressively refine Equatorial E-term deltas */
/* -------------------------                                                */
/*                                                                          */
/****************************************************************************/

void refinedEquETermCorrection(double ra, double dec, double *dra, double *ddec)
{
   int i, imax;
   double tmpra, tmpdec;

   if(coord_debug)
   {
      fprintf(stderr, "DEBUG: refinedEquETermCorrection()\n");
      fflush(stderr);
   }

   tmpra  = ra;
   tmpdec = dec;

   imax  = 3;

   for (i=0; i<imax; i++)
   {
      getEquETermCorrection(tmpra, tmpdec, dra, ddec);

      if (i == 2)
         return;

      tmpra  = ra  - *dra;
      tmpdec = dec - *ddec;

      correctCoordinateRange(&tmpra, &tmpdec);
   }

   return;
}



 
/***************************************************************************/
/*                                                                         */
/* refinedEclETermCorrection  Progressively refine Ecliptic E-term deltas  */
/* -------------------------                                               */
/*                                                                         */
/***************************************************************************/


void refinedEclETermCorrection(double obsdatej, double elon, double elat, 
                               double *delon, double *delat)
{
   int i, imax = 3;
   double tmpelon, tmpelat;

   if(coord_debug)
   {
      fprintf(stderr, "DEBUG: refinedEclETermCorrection()\n");
      fflush(stderr);
   }

   tmpelon = elon;
   tmpelat = elat;

   for (i=0; i<imax; i++)
   {
      getEclETermCorrection(obsdatej, tmpelon, tmpelat, delon, delat);

      tmpelon = elon - *delon;
      tmpelat = elat - *delat;

      correctCoordinateRange(&tmpelon, &tmpelat);
   }

   return;
}




/***************************************************************************/
/*                                                                         */
/* correctForEquatorialETerms  Remove Equatorial E-term deltas             */
/* --------------------------                                              */
/*                                                                         */
/***************************************************************************/


void correctForEquatorialETerms(double date, double *ra, double *dec)
{
   /* Remove E-terms */

   double elon,  elat;
   double delon, delat;
   double dra,   ddec;  
   
   double pole = 89.999;


   if(coord_debug)
   {
      fprintf(stderr, "DEBUG: correctForEquatorialETerms()\n");
      fflush(stderr);
   }


   if(fabs(*dec) < pole)
   {
      getEquETermCorrection( *ra, *dec, &dra, &ddec);

      *ra  = *ra  + dra;
      *dec = *dec + ddec;

      correctCoordinateRange(ra, dec);
   }

   else
   {
      /* NOTE:  Use JULIAN system here.                */
      /* Makes no difference in resulting E-terms and  */
      /* simplifies argument list for this function    */
      
      convertEquToEcl(*ra, *dec, &elon, &elat, date, JULIAN);

      refinedEclETermCorrection(date, elon, elat, &delon, &delat);

      elon = elon - delon;
      elat = elat - delat;

      correctCoordinateRange(&elon, &elat);

      convertEclToEqu(elon, elat, ra, dec, date, JULIAN);
   } 

   return;
}




/***************************************************************************/
/*                                                                         */
/* correctForEclipticETerms  Replace Equatorial E-term deltas (correct     */
/* ------------------------  Ecliptic but caste into Equatorial)           */
/*                                                                         */
/***************************************************************************/


void correctForEclipticETerms(double date, double *ra, double *dec)
{
   /* Add back  E-terms */

   double dra,   ddec;
   double elon,  elat;
   double delon, delat;

   double pole = 89.999;


   if(coord_debug)
   {
      fprintf(stderr, "DEBUG: correctForEclipticETerms()\n");
      fflush(stderr);
   }


   if(fabs(*dec) < pole) 
   {
      refinedEquETermCorrection( *ra, *dec, &dra, &ddec);

      *ra  = *ra  - dra;
      *dec = *dec - ddec;

      correctCoordinateRange(ra, dec);
   }

   else
   {
      convertEquToEcl(*ra, *dec, &elon, &elat, date, JULIAN);

      getEclETermCorrection(date, elon, elat, &delon, &delat);

      elon = elon + delon;
      elat = elat + delat;

      correctCoordinateRange(&elon, &elat);

      convertEclToEqu(elon, elat, ra, dec, date, JULIAN);
   }

   return;
}
