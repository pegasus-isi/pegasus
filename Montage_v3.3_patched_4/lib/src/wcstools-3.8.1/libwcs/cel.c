/*=============================================================================
*
*   WCSLIB - an implementation of the FITS WCS proposal.
*   Copyright (C) 1995-2002, Mark Calabretta
*
*   This library is free software; you can redistribute it and/or
*   modify it under the terms of the GNU Lesser General Public
*   License as published by the Free Software Foundation; either
*   version 2 of the License, or (at your option) any later version.
*
*   This library is distributed in the hope that it will be useful,
*   but WITHOUT ANY WARRANTY; without even the implied warranty of
*   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
*   Lesser General Public License for more details.
*   
*   You should have received a copy of the GNU Lesser General Public
*   License along with this library; if not, write to the Free Software
*   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*
*   Correspondence concerning WCSLIB may be directed to:
*      Internet email: mcalabre@atnf.csiro.au
*      Postal address: Dr. Mark Calabretta,
*                      Australia Telescope National Facility,
*                      P.O. Box 76,
*                      Epping, NSW, 2121,
*                      AUSTRALIA
*
*=============================================================================
*
*   C routines which implement the FITS World Coordinate System (WCS)
*   convention.
*
*   Summary of routines
*   -------------------
*   These routines are provided as drivers for the lower level spherical
*   coordinate transformation and projection routines.  There are separate
*   driver routines for the forward, celfwd(), and reverse, celrev(),
*   transformations.
*
*   An initialization routine, celset(), computes intermediate values from
*   the transformation parameters but need not be called explicitly - see the
*   explanation of cel.flag below.
*
*
*   Initialization routine; celset()
*   --------------------------------
*   Initializes members of a celprm data structure which hold intermediate
*   values.  Note that this routine need not be called directly; it will be
*   invoked by celfwd() and celrev() if the "flag" structure member is
*   anything other than a predefined magic value.
*
*   Given:
*      pcode[4] const char
*                        WCS projection code (see below).
*
*   Given and returned:
*      cel      celprm*  Spherical coordinate transformation parameters
*                        (see below).
*      prj      prjprm*  Projection parameters (usage is described in the
*                        prologue to "proj.c").
*
*   Function return value:
*               int      Error status
*                           0: Success.
*                           1: Invalid coordinate transformation parameters.
*                           2: Ill-conditioned coordinate transformation
*                              parameters.
*
*   Forward transformation; celfwd()
*   --------------------------------
*   Compute (x,y) coordinates in the plane of projection from celestial
*   coordinates (lng,lat).
*
*   Given:
*      pcode[4] const char
*                        WCS projection code (see below).
*      lng,lat  const double
*                        Celestial longitude and latitude of the projected
*                        point, in degrees.
*
*   Given and returned:
*      cel      celprm*  Spherical coordinate transformation parameters
*                        (see below).
*
*   Returned:
*      phi,     double*  Longitude and latitude in the native coordinate
*      theta             system of the projection, in degrees.
*
*   Given and returned:
*      prj      prjprm*  Projection parameters (usage is described in the
*                        prologue to "proj.c").
*
*   Returned:
*      x,y      double*  Projected coordinates, "degrees".
*
*   Function return value:
*               int      Error status
*                           0: Success.
*                           1: Invalid coordinate transformation parameters.
*                           2: Invalid projection parameters.
*                           3: Invalid value of (lng,lat).
*
*   Reverse transformation; celrev()
*   --------------------------------
*   Compute the celestial coordinates (lng,lat) of the point with projected
*   coordinates (x,y).
*
*   Given:
*      pcode[4] const char
*                        WCS projection code (see below).
*      x,y      const double
*                        Projected coordinates, "degrees".
*
*   Given and returned:
*      prj      prjprm*  Projection parameters (usage is described in the
*                        prologue to "proj.c").
*
*   Returned:
*      phi,     double*  Longitude and latitude in the native coordinate
*      theta             system of the projection, in degrees.
*
*   Given and returned:
*      cel      celprm*  Spherical coordinate transformation parameters
*                        (see below).
*
*   Returned:
*      lng,lat  double*  Celestial longitude and latitude of the projected
*                        point, in degrees.
*
*   Function return value:
*               int      Error status
*                           0: Success.
*                           1: Invalid coordinate transformation parameters.
*                           2: Invalid projection parameters.
*                           3: Invalid value of (x,y).
*
*   Coordinate transformation parameters
*   ------------------------------------
*   The celprm struct consists of the following:
*
*      int flag
*         The celprm struct contains pointers to the forward and reverse
*         projection routines as well as intermediaries computed from the
*         reference coordinates (see below).  Whenever the projection code
*         (pcode) or any of ref[4] are set or changed then this flag must be
*         set to zero to signal the initialization routine, celset(), to
*         redetermine the function pointers and recompute intermediaries.
*         Once this has been done pcode itself is ignored.
*
*      double ref[4]
*         The first pair of values should be set to the celestial longitude
*         and latitude (usually right ascension and declination) of the
*         reference point of the projection.  These are given by the CRVALn
*         keywords in FITS.
*
*         The second pair of values are the native longitude of the celestial
*         pole and the celestial latitude of the native pole and correspond to
*         FITS keywords LONPOLE and LATPOLE.
*
*         LONPOLE defaults to 0 degrees if the celestial latitude of the
*         reference point of the projection is greater than the native
*         latitude, otherwise 180 degrees.  (This is the condition for the
*         celestial latitude to increase in the same direction as the native
*         latitude at the reference point.)  ref[2] may be set to 999.0 to
*         indicate that the correct default should be substituted.
*
*         In some circumstances the celestial latitude of the native pole may
*         be determined by the first three values only to within a sign and
*         LATPOLE is used to choose between the two solutions.  LATPOLE is
*         set in ref[3] and the solution closest to this value is used to
*         reset ref[3].  It is therefore legitimate, for example, to set
*         ref[3] to 999.0 to choose the more northerly solution - the default
*         if the LATPOLE card is omitted from the FITS header.  For the
*         special case where the reference point of the projection is at
*         native latitude zero, its celestial latitude is zero, and
*         LONPOLE = +/- 90 then the celestial latitude of the pole is not
*         determined by the first three reference values and LATPOLE
*         specifies it completely.
*
*   The remaining members of the celprm struct are maintained by the
*   initialization routines and should not be modified.  This is done for the
*   sake of efficiency and to allow an arbitrary number of contexts to be
*   maintained simultaneously.
*
*      double euler[5]
*         Euler angles and associated intermediaries derived from the
*         coordinate reference values.
*
*
*   WCS projection codes
*   --------------------
*   Zenithals/azimuthals:
*      AZP: zenithal/azimuthal perspective
*      TAN: gnomonic
*      STG: stereographic
*      SIN: synthesis (generalized orthographic)
*      ARC: zenithal/azimuthal equidistant
*      ZPN: zenithal/azimuthal polynomial
*      ZEA: zenithal/azimuthal equal area
*      AIR: Airy
*
*   Cylindricals:
*      CYP: cylindrical perspective
*      CEA: cylindrical equal area
*      CAR: Cartesian
*      MER: Mercator
*
*   Pseudo-cylindricals:
*      SFL: Sanson-Flamsteed
*      PAR: parabolic
*      MOL: Mollweide
*
*   Conventional:
*      AIT: Hammer-Aitoff
*
*   Conics:
*      COP: conic perspective
*      COD: conic equidistant
*      COE: conic equal area
*      COO: conic orthomorphic
*
*   Polyconics:
*      BON: Bonne
*      PCO: polyconic
*
*   Quad-cubes:
*      TSC: tangential spherical cube
*      CSC: COBE quadrilateralized spherical cube
*      QSC: quadrilateralized spherical cube
*
*   Author: Mark Calabretta, Australia Telescope National Facility
*   $Id: cel.c,v 2.14 2002/04/03 01:25:29 mcalabre Exp $
*===========================================================================*/

#include <math.h>
#include <string.h>
#include "wcslib.h"

/* Map error number to error message for each function. */
const char *celset_errmsg[] = {
   0,
   "Invalid coordinate transformation parameters",
   "Ill-conditioned coordinate transformation parameters"};

const char *celfwd_errmsg[] = {
   0,
   "Invalid coordinate transformation parameters",
   "Invalid projection parameters",
   "Invalid value of (lng,lat)"};

const char *celrev_errmsg[] = {
   0,
   "Invalid coordinate transformation parameters",
   "Invalid projection parameters",
   "Invalid value of (x,y)"};
 

int
celset(pcode, cel, prj)

const char pcode[4];
struct celprm *cel;
struct prjprm *prj;

{
   int dophip;
   const double tol = 1.0e-10;
   double clat0, cphip, cthe0, slat0, sphip, sthe0;
   double latp, latp1, latp2;
   double u, v, x, y, z;

   /* Initialize the projection driver routines. */
   if (prjset(pcode, prj)) {
      return 1;
   }

   /* Set default for native longitude of the celestial pole? */
   dophip = (cel->ref[2] == 999.0);

   /* Compute celestial coordinates of the native pole. */
   if (prj->theta0 == 90.0) {
      /* Reference point is at the native pole. */

      if (dophip) {
         /* Set default for longitude of the celestial pole. */
         cel->ref[2] = 180.0;
      }

      latp = cel->ref[1];
      cel->ref[3] = latp;

      cel->euler[0] = cel->ref[0];
      cel->euler[1] = 90.0 - latp;
   } else {
      /* Reference point away from the native pole. */

      /* Set default for longitude of the celestial pole. */
      if (dophip) {
         cel->ref[2] = (cel->ref[1] < prj->theta0) ? 180.0 : 0.0;
      }

      clat0 = cosdeg (cel->ref[1]);
      slat0 = sindeg (cel->ref[1]);
      cphip = cosdeg (cel->ref[2]);
      sphip = sindeg (cel->ref[2]);
      cthe0 = cosdeg (prj->theta0);
      sthe0 = sindeg (prj->theta0);

      x = cthe0*cphip;
      y = sthe0;
      z = sqrt(x*x + y*y);
      if (z == 0.0) {
         if (slat0 != 0.0) {
            return 1;
         }

         /* latp determined by LATPOLE in this case. */
         latp = cel->ref[3];
      } else {
         if (fabs(slat0/z) > 1.0) {
            return 1;
         }

         u = atan2deg (y,x);
         v = acosdeg (slat0/z);

         latp1 = u + v;
         if (latp1 > 180.0) {
            latp1 -= 360.0;
         } else if (latp1 < -180.0) {
            latp1 += 360.0;
         }

         latp2 = u - v;
         if (latp2 > 180.0) {
            latp2 -= 360.0;
         } else if (latp2 < -180.0) {
            latp2 += 360.0;
         }

         if (fabs(cel->ref[3]-latp1) < fabs(cel->ref[3]-latp2)) {
            if (fabs(latp1) < 90.0+tol) {
               latp = latp1;
            } else {
               latp = latp2;
            }
         } else {
            if (fabs(latp2) < 90.0+tol) {
               latp = latp2;
            } else {
               latp = latp1;
            }
         }

         cel->ref[3] = latp;
      }

      cel->euler[1] = 90.0 - latp;

      z = cosdeg (latp)*clat0;
      if (fabs(z) < tol) {
         if (fabs(clat0) < tol) {
            /* Celestial pole at the reference point. */
            cel->euler[0] = cel->ref[0];
            cel->euler[1] = 90.0 - prj->theta0;
         } else if (latp > 0.0) {
            /* Celestial pole at the native north pole.*/
            cel->euler[0] = cel->ref[0] + cel->ref[2] - 180.0;
            cel->euler[1] = 0.0;
         } else if (latp < 0.0) {
            /* Celestial pole at the native south pole. */
            cel->euler[0] = cel->ref[0] - cel->ref[2];
            cel->euler[1] = 180.0;
         }
      } else {
         x = (sthe0 - sindeg (latp)*slat0)/z;
         y =  sphip*cthe0/clat0;
         if (x == 0.0 && y == 0.0) {
            return 1;
         }
         cel->euler[0] = cel->ref[0] - atan2deg (y,x);
      }

      /* Make euler[0] the same sign as ref[0]. */
      if (cel->ref[0] >= 0.0) {
         if (cel->euler[0] < 0.0) cel->euler[0] += 360.0;
      } else {
         if (cel->euler[0] > 0.0) cel->euler[0] -= 360.0;
      }
   }

   cel->euler[2] = cel->ref[2];
   cel->euler[3] = cosdeg (cel->euler[1]);
   cel->euler[4] = sindeg (cel->euler[1]);
   cel->flag = CELSET;

   /* Check for ill-conditioned parameters. */
   if (fabs(latp) > 90.0+tol) {
      return 2;
   }

   return 0;
}

/*--------------------------------------------------------------------------*/

int
celfwd(pcode, lng, lat, cel, phi, theta, prj, x, y)

const char pcode[4];
const double lng, lat;
struct celprm *cel;
double *phi, *theta;
struct prjprm *prj;
double *x, *y;

{
   int    err;

   if (cel->flag != CELSET) {
      if (celset(pcode, cel, prj)) return 1;
   }

   /* Compute native coordinates. */
   sphfwd(lng, lat, cel->euler, phi, theta);

   /* Apply forward projection. */
   if ((err = prj->prjfwd(*phi, *theta, prj, x, y))) {
      return err == 1 ? 2 : 3;
   }

   return 0;
}

/*--------------------------------------------------------------------------*/

int
celrev(pcode, x, y, prj, phi, theta, cel, lng, lat)

const char pcode[4];
const double x, y;
struct prjprm *prj;
double *phi, *theta;
struct celprm *cel;
double *lng, *lat;

{
   int    err;

   if (cel->flag != CELSET) {
      if(celset(pcode, cel, prj)) return 1;
   }

   /* Apply reverse projection. */
   if ((err = prj->prjrev(x, y, prj, phi, theta))) {
      return err == 1 ? 2 : 3;
   }

   /* Compute native coordinates. */
   sphrev(*phi, *theta, cel->euler, lng, lat);

   return 0;
}

/* Dec 20 1999	Doug Mink - Change cosd() and sind() to cosdeg() and sindeg()
 * Dec 20 1999	Doug Mink - Include wcslib.h, which includes wcsmath.h and cel.h
 *
 * Dec 18 2000	Doug Mink - Include string.h for strcmp()
 *
 * Mar 20 2001	Doug Mink - Add () around err assignments in if statements
 * Sep 19 2001	Doug Mink - Add above changes to WCSLIB-2.7 cel.c
 *
 * Mar 12 2002	Doug Mink - Add changes to WCSLIB-2.8.2 cel.c
 */
