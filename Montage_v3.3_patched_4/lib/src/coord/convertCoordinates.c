#include <stdio.h>
#include <math.h>
#include <coord.h>

/***************************************************************************/
/*                                                                         */
/* convertCoordinates  general coordinate conversion wrapper for other     */
/* ------------------  routines in the library.                            */
/*                                                                         */
/* Inputs:  coord sys, epoch (unit: years), lon, lat (degrees)             */
/*          for point on the sky and target coords sys, epoch              */
/*                                                                         */
/* Outputs: lon, lat of target (in degrees)                                */
/*                                                                         */
/***************************************************************************/


void
convertCoordinates(int  insys, double  inepoch, double   inlon, double   inlat,
                   int outsys, double outepoch, double *outlon, double *outlat,
		   double obstime)
{
   int    systemin, systemout, equinoxin, equinoxout;
   double lonout, latout, tobs;
   double lonin, latin, epochin, epochout;


   /*****************************************************/
   /* First a little processing on the input parameters */
   /*****************************************************/

   if(coord_debug)
   {
      fprintf(stderr, "DEBUG: convertCoordinates()\n");
      fflush(stderr);
   }


   /* Set date of observation */

   tobs = obstime;
   if(tobs == 0.0) tobs = 1950.;


   /* equinoxin, equinoxout = BESSELIAN or JULIAN */
   /* based on the systemin, systemout values     */

   systemin   = insys;
   systemout  = outsys;

   equinoxin  = BESSELIAN;
   equinoxout = BESSELIAN;

   if( systemin == EQUJ ||  systemin == ECLJ)
      equinoxin  = JULIAN;

   if(systemout == EQUJ || systemout == ECLJ)
      equinoxout = JULIAN;


   /* For GAL and SGAL, we arbitrarily set epoch to 1950, and if epoch  */
   /* is not given we take the default year associated with the equinox */

   epochin  = inepoch;
   epochout = outepoch;

   if(systemin == GAL || systemin == SGAL)
   {
      epochin  = 1950.;
   }

   else if(epochin == 0.0) 
   {
      if(equinoxin  == JULIAN) epochin  = 2000.;
      else                     epochin  = 1950.;
   }

   if(systemout == GAL || systemout == SGAL) 
   {
      epochout = 1950.;
   }

   else if(epochout == 0.0) 
   {
      if(equinoxout == JULIAN) epochout = 2000.;
      else                     epochout = 1950.;
   }




   /*****************************************************/
   /* TRANSLATION CASES:  First translate to EQUATORIAL */
   /* (with shortcut if we find we are done)            */
   /*****************************************************/

   lonin = inlon;
   latin = inlat;


   /* In and out are the same: No-op         */

   if(systemin == systemout && epochin == epochout) 
   {
      *outlon = inlon;
      *outlat = inlat;
      return;
   }


   /* If we are starting with ECLIPTIC,      */
   /* convert to EQUATORIAL                  */

   if(systemin == ECLB || systemin == ECLJ)
   {
      convertEclToEqu(inlon, inlat, &lonin, &latin, epochin, equinoxin);

      if(equinoxin == BESSELIAN) 
	 systemin = EQUB;
      else
	 systemin = EQUJ;

      if(systemout == systemin && epochin == epochout)
      {
	 *outlon = lonin;
	 *outlat = latin;
	 return;
      }
   }


   /* If we are starting with SUPERGALACTIC, */
   /* convert to GALACTIC and then to        */
   /* EQUATORIAL (Besselian)                 */

   else if(systemin == SGAL) 
   {
      /*  see if supergalactic or galactic on input: */

       convertSgalToGal(lonin, latin, &lonout, &latout);

       if(systemout == GAL)
       {
	  *outlon = lonout;
	  *outlat = latout;
	  return;
       }

       convertGalToEqu(lonout, latout, &lonin, &latin);
       systemin = EQUB;
   }


   /* If we are starting with GALACTIC,      */
   /* convert to EQUATORIAL (Besselian)      */

   else if(systemin == GAL) 
   {
      if(systemout == SGAL) 
      {
	 convertGalToSgal(lonin, latin, outlon, outlat);
	 return;
      }

      convertGalToEqu(lonin, latin, &lonout, &latout);

      lonin = lonout;
      latin = latout;
      systemin = EQUB;
   }




   /*****************************************************/
   /* Then convert to the output coordinate system and  */
   /* epoch (again with shortcuts where appropriate)    */
   /*****************************************************/


   /* The conversions above were enough; we're done */

   if(systemout == systemin 
   &&  epochout ==  epochin)
   {
      *outlon = lonin;
      *outlat = latin;
      return;
   }


   /* Next, if ECLIPTIC is really what is desired, change  */
   /* systemout to be EQUATORIAL as an intermediate step   */

   if(outsys == ECLB) 
      systemout = EQUB;

   if(outsys == ECLJ) 
      systemout = EQUJ;



   /* EQUATORIAL Besselian to EQUATORIAL Julian */

   if(systemin == EQUB) 
   {

      /* Precess BESSELIAN to the epoch desired */

      if(equinoxout == BESSELIAN) 
      {
	 if(epochin != epochout) 
	 {
	    precessBesselian(epochin, lonin, latin, epochout, &lonout, &latout);

	    lonin   = lonout;
	    latin   = latout;
	    epochin = epochout;
	 }

	 if(outsys == EQUB) 
	 {
	    *outlon = lonin;
	    *outlat = latin;
	    return;
	 }
      }

      else   /* equinoxout = JULIAN */
      {

	 /* Convert BESSELIAN to JULIAN */

	 convertBesselianToJulian(epochin, lonin, latin, tobs, 1, 
				  &lonout, &latout);


	 /* Precess the JULIAN from 2000 to epoch desired */

	 if(epochout != 2000.) 
	 {
	    lonin = lonout;
	    latin = latout;

	    precessJulian(2000., lonin, latin, epochout, &lonout, &latout);
	 }

	 if(outsys == EQUJ) 
	 {
	    *outlon = lonout;
	    *outlat = latout;
	    return;
	 }

	 else /* Setting up for the sections below */
	 {
	    lonin     = lonout;
	    latin     = latout;
	    epochin   = epochout;
	    equinoxin = JULIAN;
	    systemin  = EQUJ;
	 } 
      }
   }



   /* Processing Equatorial Julian */

   else if(systemin == EQUJ) 
   {

      /* Precess JULIAN from epoch to 2000 */

      if(equinoxout == BESSELIAN) 
      {
	 if(epochin != 2000.) 
	 {
	    precessJulian(epochin, lonin, latin, 2000., &lonout, &latout);

	    lonin   = lonout;
	    latin   = latout;

	    epochin = 2000.;
	 }


	 /* Convert JULIAN to BESSELIAN */

	 convertJulianToBesselian(lonin, latin, tobs, 1,
				  epochout, &lonout, &latout);

	 if(outsys == EQUB)
	 {	
	    *outlon = lonout;
	    *outlat = latout;
	    return;
	 }

	 else
	 {
	    lonin     = lonout;
	    latin     = latout;
	    epochin   = epochout;
	    equinoxin = BESSELIAN;
	    systemin  = EQUB;
	 }
      }


      /* precess JULIAN to desired epoch */

      else
      {
	 if(epochin != epochout) 
	 {
	    precessJulian(epochin, lonin, latin, epochout, &lonout, &latout);

	    lonin   = lonout;
	    latin   = latout;
	    epochin = epochout;
	 }

	 if(outsys == EQUJ)
	 {
	    *outlon = lonin;
	    *outlat = latin;
	    return;
	 }
      }
   }



   /* EQUATORIAL to ECLIPTIC */

   if(outsys == ECLB || outsys == ECLJ) 
   {
      convertEquToEcl(lonin, latin, outlon, outlat, epochout, equinoxout);
      return;
   }



   /* EQUATORIAL to GALACTIC or SUPERGALACTIC */

   if(outsys == SGAL || outsys == GAL) 
   {
      convertEquToGal(lonin, latin, &lonout, &latout);

      if(outsys == SGAL) 
	  convertGalToSgal(lonout, latout, outlon, outlat);

      else
      {
	 *outlon = lonout;
	 *outlat = latout;
      }

      return;
   }

   return;
}
