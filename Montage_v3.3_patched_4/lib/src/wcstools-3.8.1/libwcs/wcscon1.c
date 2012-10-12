/*** File wcscon.c
 *** November 29, 2006
 *** Doug Mink, Harvard-Smithsonian Center for Astrophysics
 *** Some subroutines are based on Starlink subroutines by Patrick Wallace
 *** Copyright (C) 1995-2006
 *** Smithsonian Astrophysical Observatory, Cambridge, MA, USA

    This library is free software; you can redistribute it and/or
    modify it under the terms of the GNU Lesser General Public
    License as published by the Free Software Foundation; either
    version 2 of the License, or (at your option) any later version.

    This library is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
    Lesser General Public License for more details.
    
    You should have received a copy of the GNU Lesser General Public
    License along with this library; if not, write to the Free Software
    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

    Correspondence concerning WCSTools should be addressed as follows:
           Internet email: dmink@cfa.harvard.edu
           Postal address: Doug Mink
                           Smithsonian Astrophysical Observatory
                           60 Garden St.
                           Cambridge, MA 02138 USA

 * Module:	wcscon.c (World Coordinate System conversion)
 * Purpose:	Convert between various sky coordinate systems
 * Subroutine:	wcscon (sys1,sys2,eq1,eq2,theta,phi,epoch)
 *		convert between coordinate systems
 * Subroutine:  wcsconp (sys1,sys2,eq1,eq2,ep1,ep2,dtheta,dphi,ptheta,pphi)
 *              convert coordinates and proper motion between coordinate systems
 * Subroutine:  wcsconv (sys1,sys2,eq1,eq2,ep1,ep2,dtheta,dphi,ptheta,pphi,px,rv)
 *              convert coordinates and proper motion between coordinate systems
 * Subroutine:	wcscsys (cstring) returns code for coordinate system in string
 * Subroutine:	wcsceq (wcstring) returns equinox in years from system string
 * Subroutine:	wcscstr (sys,equinox,epoch) returns system string from equinox
 * Subroutine:	fk524 (ra,dec) Convert J2000(FK5) to B1950(FK4) coordinates
 * Subroutine:	fk524e (ra, dec, epoch) (more accurate for known position epoch)
 * Subroutine:	fk524m (ra,dec,rapm,decpm) exact
 * Subroutine:	fk524pv (ra,dec,rapm,decpm,parallax,rv) more exact
 * Subroutine:	fk425 (ra,dec) Convert B1950(FK4) to J2000(FK5) coordinates
 * Subroutine:	fk425e (ra, dec, epoch) (more accurate for known position epoch)
 * Subroutine:	fk425m (ra, dec, rapm, decpm) exact
 * Subroutine:	fk425pv (ra,dec,rapm,decpm,parallax,rv) more exact
 * Subroutine:	fk42gal (dtheta,dphi) Convert B1950(FK4) to galactic coordinates
 * Subroutine:	fk52gal (dtheta,dphi) Convert J2000(FK5) to galactic coordinates
 * Subroutine:	gal2fk4 (dtheta,dphi) Convert galactic coordinates to B1950(FK4)
 * Subroutine:	gal2fk5 (dtheta,dphi) Convert galactic coordinates to J2000<FK5)
 * Subroutine:	fk42ecl (dtheta,dphi,epoch) Convert B1950(FK4) to ecliptic coordinates
 * Subroutine:	fk52ecl (dtheta,dphi,epoch) Convert J2000(FK5) to ecliptic coordinates
 * Subroutine:	ecl2fk4 (dtheta,dphi,epoch) Convert ecliptic coordinates to B1950(FK4)
 * Subroutine:	ecl2fk5 (dtheta,dphi,epoch) Convert ecliptic coordinates to J2000<FK5)
 * Subroutine:  fk5prec (ep0, ep1, ra, dec) Precession ep0 to ep1, FK5 system
 * Subroutine:  fk4prec (ep0, ep1, ra, dec) Precession ep0 to ep1, FK4 system
 * Subroutine:  s2v3 (rra, rdec, r, pos) RA, Dec, Distance to Cartesian
 * Subroutine:  v2s3 (pos, rra, rdec, r) Cartesion to RA, Dec, Distance
 * Subroutine:  rotmat (axes, rot1, rot2, rot3, matrix) Rotation angles to matrix
 */

#include <math.h>
#ifndef VMS
#include <stdlib.h>
#endif
#include <stdio.h>	/* for fprintf() and sprintf() */
#include <ctype.h>
#include <string.h>
#include "wcs.h"

void fk524(), fk524e(), fk524m(), fk524pv();
void fk425(), fk425e(), fk425m(), fk425pv();
void fk42gal(), fk52gal(), gal2fk4(), gal2fk5();
void fk42ecl(), fk52ecl(), ecl2fk4(), ecl2fk5();

/* Convert from coordinate system sys1 to coordinate system sys2, converting
   proper motions, too, and adding them if an epoch is specified */

void
wcsconp (sys1, sys2, eq1, eq2, ep1, ep2, dtheta, dphi, ptheta, pphi)

int	sys1;	/* Input coordinate system (J2000, B1950, ECLIPTIC, GALACTIC */
int	sys2;	/* Output coordinate system (J2000, B1950, ECLIPTIC, GALACTIC */
double	eq1;	/* Input equinox (default of sys1 if 0.0) */
double	eq2;	/* Output equinox (default of sys2 if 0.0) */
double	ep1;	/* Input Besselian epoch in years (for proper motion) */
double	ep2;	/* Output Besselian epoch in years (for proper motion) */
double	*dtheta; /* Longitude or right ascension in degrees
		   Input in sys1, returned in sys2 */
double	*dphi;	/* Latitude or declination in degrees
		   Input in sys1, returned in sys2 */
double	*ptheta; /* Longitude or right ascension proper motion in degrees/year
		   Input in sys1, returned in sys2 */
double	*pphi;	/* Latitude or declination proper motion in degrees/year
		   Input in sys1, returned in sys2 */

{
    void fk5prec(), fk4prec();

    /* Set equinoxes if 0.0 */
    if (eq1 == 0.0) {
	if (sys1 == WCS_B1950)
	    eq1 = 1950.0;
	else
	    eq1 = 2000.0;
	}
    if (eq2 == 0.0) {
	if (sys2 == WCS_B1950)
	    eq2 = 1950.0;
	else
	    eq2 = 2000.0;
	}

    /* Set epochs if 0.0 */
    if (ep1 == 0.0) {
	if (sys1 == WCS_B1950)
	    ep1 = 1950.0;
	else
	    ep1 = 2000.0;
	}
    if (ep2 == 0.0) {
	if (sys2 == WCS_B1950)
	    ep2 = 1950.0;
	else
	    ep2 = 2000.0;
	}

    if (sys1 == WCS_ICRS && sys2 == WCS_ICRS)
	eq2 = eq1;

    if (sys1 == WCS_J2000 && sys2 == WCS_ICRS && eq1 == 2000.0) {
	eq2 = eq1;
	sys1 = sys2;
	}

    /* Set systems and equinoxes so that ICRS coordinates are not precessed */
    if (sys1 == WCS_ICRS && sys2 == WCS_J2000 && eq2 == 2000.0) {
	eq1 = eq2;
	sys1 = sys2;
	}

    /* If systems and equinoxes are the same, add proper motion and return */
    if (sys2 == sys1 && eq1 == eq2) {
	if (ep1 != ep2) {
	    if (sys1 == WCS_J2000) {
		*dtheta = *dtheta + ((ep2 - ep1) * *ptheta);
		*dphi = *dphi + ((ep2 - ep1) * *pphi);
		}
	    else if (sys1 == WCS_B1950) {
		*dtheta = *dtheta + ((ep2 - ep1) * *ptheta);
		*dphi = *dphi + ((ep2 - ep1) * *pphi);
		}
	    }
	if (eq1 != eq2) {
	    if (sys1 == WCS_B1950)
		fk4prec (eq1, eq2, dtheta, dphi);
	    if (sys1 == WCS_J2000)
		fk5prec (eq1, 2000.0, dtheta, dphi);
	    }
	return;
	}

    /* Precess from input equinox to input system equinox, if necessary */
    if (sys1 == WCS_B1950 && eq1 != 1950.0)
	fk4prec (eq1, 1950.0, dtheta, dphi);
    if (sys1 == WCS_J2000 && eq1 != 2000.0)
	fk5prec (eq1, 2000.0, dtheta, dphi);

    /* Convert to B1950 FK4 */
    if (sys2 == WCS_B1950) {
	if (sys1 == WCS_J2000) {
	    if (*ptheta != 0.0 || *pphi != 0.0) {
		fk524m (dtheta, dphi, ptheta, pphi);
		if (ep1 == 2000.0)
		    ep1 = 1950.0;
		if (ep2 != 1950.0) {
		    *dtheta = *dtheta + ((ep2 - 1950.0) * *ptheta);
		    *dphi = *dphi + ((ep2 - 1950.0) * *pphi);
		    }
		}
	    else if (ep2 != 1950.0)
		fk524e (dtheta, dphi, ep2);
	    else
		fk524 (dtheta, dphi);
	    }
	else if (sys1 == WCS_GALACTIC) 
	    gal2fk4 (dtheta, dphi);
	else if (sys1 == WCS_ECLIPTIC)
	    ecl2fk4 (dtheta, dphi, ep2);
	}

    else if (sys2 == WCS_J2000) {
        if (sys1 == WCS_B1950) {
	    if (*ptheta != 0.0 || *pphi != 0.0) {
		fk425m (dtheta, dphi, ptheta, pphi);
		if (ep2 != 2000.0) {
		    *dtheta = *dtheta + ((ep2 - 2000.0) * *ptheta);
		    *dphi = *dphi + ((ep2 - 2000.0) * *pphi);
		    }
		}
            else if (ep2 > 0.0)
                fk425e (dtheta, dphi, ep2);
            else
                fk425 (dtheta, dphi);
            }
        else if (sys1 == WCS_GALACTIC)
            gal2fk5 (dtheta, dphi);
	else if (sys1 == WCS_ECLIPTIC)
	    ecl2fk5 (dtheta, dphi, ep2);
	}

    else if (sys2 == WCS_GALACTIC) {
        if (sys1 == WCS_B1950) {
	    if (ep2 != 0.0 && (*ptheta != 0.0 || *pphi != 0.0)) {
		*dtheta = *dtheta + (*ptheta * (ep2 - ep1));
		*dphi = *dphi + (*pphi * (ep2 - ep1));
		}
	    fk42gal (dtheta, dphi);
	    }
        else if (sys1 == WCS_J2000) {
	    if (ep2 != 0.0 && (*ptheta != 0.0 || *pphi != 0.0)) {
		*dtheta = *dtheta + (*ptheta * (ep2 - ep1));
		*dphi = *dphi + (*pphi * (ep2 - ep1));
		}
	    fk52gal (dtheta, dphi);
	    }
        else if (sys1 == WCS_ECLIPTIC) {
	    ecl2fk5 (dtheta, dphi, ep2);
	    fk52gal (dtheta, dphi);
	    }
	}

    else if (sys2 == WCS_ECLIPTIC) {
        if (sys1 == WCS_B1950) {
	    if (ep2 != 0.0 && (*ptheta != 0.0 || *pphi != 0.0)) {
		*dtheta = *dtheta + (*ptheta * (ep2 - ep1));
		*dphi = *dphi + (*pphi * (ep2 - ep1));
		}
	    if (ep2 > 0.0)
		fk42ecl (dtheta, dphi, ep2);
	    else
		fk42ecl (dtheta, dphi, 1950.0);
	    }
        else if (sys1 == WCS_J2000) {
	    if (ep2 != 0.0 && (*ptheta != 0.0 || *pphi != 0.0)) {
		*dtheta = *dtheta + (*ptheta * (ep2 - ep1));
		*dphi = *dphi + (*pphi * (ep2 - ep1));
		}
	    fk52ecl (dtheta, dphi, ep2);
	    }
        else if (sys1 == WCS_GALACTIC) {
	    gal2fk5 (dtheta, dphi);
	    fk52ecl (dtheta, dphi, ep2);
	    }
	}

    /* Precess to desired equinox, if necessary */
    if (sys2 == WCS_B1950 && eq2 != 1950.0)
	fk4prec (1950.0, eq2, dtheta, dphi);
    if (sys2 == WCS_J2000 && eq2 != 2000.0)
	fk5prec (2000.0, eq2, dtheta, dphi);

    /* Keep latitude/declination between +90 and -90 degrees */
    if (*dphi > 90.0) {
	*dphi = 180.0 - *dphi;
	*dtheta = *dtheta + 180.0;
	}
    else if (*dphi < -90.0) {
	*dphi = -180.0 - *dphi;
	*dtheta = *dtheta + 180.0;
	}

    /* Keep longitude/right ascension between 0 and 360 degrees */
    if (*dtheta > 360.0)
	*dtheta = *dtheta - 360.0;
    else if (*dtheta < 0.0)
	*dtheta = *dtheta + 360.0;
    return;
}


/* Convert from coordinate system sys1 to coordinate system sys2, converting
   proper motions, too, and adding them if an epoch is specified */

void
wcsconv (sys1, sys2, eq1, eq2, ep1, ep2, dtheta, dphi, ptheta, pphi, px, rv)

int	sys1;	/* Input coordinate system (J2000, B1950, ECLIPTIC, GALACTIC */
int	sys2;	/* Output coordinate system (J2000, B1950, ECLIPTIC, GALACTIC */
double	eq1;	/* Input equinox (default of sys1 if 0.0) */
double	eq2;	/* Output equinox (default of sys2 if 0.0) */
double	ep1;	/* Input Besselian epoch in years (for proper motion) */
double	ep2;	/* Output Besselian epoch in years (for proper motion) */
double	*dtheta; /* Longitude or right ascension in degrees
		   Input in sys1, returned in sys2 */
double	*dphi;	/* Latitude or declination in degrees
		   Input in sys1, returned in sys2 */
double	*ptheta; /* Longitude or right ascension proper motion in degrees/year
		   Input in sys1, returned in sys2 */
double	*pphi;	/* Latitude or declination proper motion in degrees/year
		   Input in sys1, returned in sys2 */
double	*px;	/* Parallax in arcseconds */
double	*rv;	/* Radial velocity in km/sec */

{
    void fk5prec(), fk4prec();

    /* Set equinoxes if 0.0 */
    if (eq1 == 0.0) {
	if (sys1 == WCS_B1950)
	    eq1 = 1950.0;
	else
	    eq1 = 2000.0;
	}
    if (eq2 == 0.0) {
	if (sys2 == WCS_B1950)
	    eq2 = 1950.0;
	else
	    eq2 = 2000.0;
	}

    /* Set epochs if 0.0 */
    if (ep1 == 0.0) {
	if (sys1 == WCS_B1950)
	    ep1 = 1950.0;
	else
	    ep1 = 2000.0;
	}
    if (ep2 == 0.0) {
	if (sys2 == WCS_B1950)
	    ep2 = 1950.0;
	else
	    ep2 = 2000.0;
	}

    /* Set systems and equinoxes so that ICRS coordinates are not precessed */
    if (sys1 == WCS_ICRS && sys2 == WCS_ICRS)
	eq2 = eq1;

    if (sys1 == WCS_J2000 && sys2 == WCS_ICRS && eq1 == 2000.0) {
	eq2 = eq1;
	sys1 = sys2;
	}

    if (sys1 == WCS_ICRS && sys2 == WCS_J2000 && eq2 == 2000.0) {
	eq1 = eq2;
	sys1 = sys2;
	}

    /* If systems and equinoxes are the same, add proper motion and return */
    if (sys2 == sys1 && eq1 == eq2) {
	if (ep1 != ep2) {
	    if (sys1 == WCS_J2000) {
		*dtheta = *dtheta + ((ep2 - ep1) * *ptheta);
		*dphi = *dphi + ((ep2 - ep1) * *pphi);
		}
	    else if (sys1 == WCS_B1950) {
		*dtheta = *dtheta + ((ep2 - ep1) * *ptheta);
		*dphi = *dphi + ((ep2 - ep1) * *pphi);
		}
	    }
	return;
	}

    /* Precess from input equinox to input system equinox, if necessary */
    if (eq1 != eq2) {
	if (sys1 == WCS_B1950 && eq1 != 1950.0)
	   fk4prec (eq1, 1950.0, dtheta, dphi);
	if (sys1 == WCS_J2000 && eq1 != 2000.0)
	   fk5prec (eq1, 2000.0, dtheta, dphi);
	}

    /* Convert to B1950 FK4 */
    if (sys2 == WCS_B1950) {
	if (sys1 == WCS_J2000) {
	    if (*ptheta != 0.0 || *pphi != 0.0) {
		if (*px != 0.0 || *rv != 0.0)
		    fk524pv (dtheta, dphi, ptheta, pphi, px, rv);
		else
		    fk524m (dtheta, dphi, ptheta, pphi);
		if (ep1 == 2000.0)
		    ep1 = 1950.0;
		if (ep2 != 1950.0) {
		    *dtheta = *dtheta + ((ep2 - 1950.0) * *ptheta);
		    *dphi = *dphi + ((ep2 - 1950.0) * *pphi);
		    }
		}
	    else if (ep2 != 1950.0)
		fk524e (dtheta, dphi, ep2);
	    else
		fk524 (dtheta, dphi);
	    }
	else if (sys1 == WCS_GALACTIC) 
	    gal2fk4 (dtheta, dphi);
	else if (sys1 == WCS_ECLIPTIC)
	    ecl2fk4 (dtheta, dphi, ep2);
	}

    else if (sys2 == WCS_J2000) {
        if (sys1 == WCS_B1950) {
	    if (*ptheta != 0.0 || *pphi != 0.0) {
		if (*px != 0.0 || *rv != 0.0)
		    fk425pv (dtheta, dphi, ptheta, pphi, px, rv);
		else
		    fk425m (dtheta, dphi, ptheta, pphi);
		if (ep2 != 2000.0) {
		    *dtheta = *dtheta + ((ep2 - 2000.0) * *ptheta);
		    *dphi = *dphi + ((ep2 - 2000.0) * *pphi);
		    }
		}
            else if (ep2 > 0.0)
                fk425e (dtheta, dphi, ep2);
            else
                fk425 (dtheta, dphi);
            }
        else if (sys1 == WCS_GALACTIC)
            gal2fk5 (dtheta, dphi);
	else if (sys1 == WCS_ECLIPTIC)
	    ecl2fk5 (dtheta, dphi, ep2);
	}

    else if (sys2 == WCS_GALACTIC) {
        if (sys1 == WCS_B1950) {
	    if (ep2 != 0.0 && (*ptheta != 0.0 || *pphi != 0.0)) {
		*dtheta = *dtheta + (*ptheta * (ep2 - ep1));
		*dphi = *dphi + (*pphi * (ep2 - ep1));
		}
	    fk42gal (dtheta, dphi);
	    }
        else if (sys1 == WCS_J2000) {
	    if (ep2 != 0.0 && (*ptheta != 0.0 || *pphi != 0.0)) {
		*dtheta = *dtheta + (*ptheta * (ep2 - ep1));
		*dphi = *dphi + (*pphi * (ep2 - ep1));
		}
	    fk52gal (dtheta, dphi);
	    }
        else if (sys1 == WCS_ECLIPTIC) {
	    ecl2fk5 (dtheta, dphi, ep2);
	    fk52gal (dtheta, dphi);
	    }
	}

    else if (sys2 == WCS_ECLIPTIC) {
        if (sys1 == WCS_B1950) {
	    if (ep2 != 0.0 && (*ptheta != 0.0 || *pphi != 0.0)) {
		*dtheta = *dtheta + (*ptheta * (ep2 - ep1));
		*dphi = *dphi + (*pphi * (ep2 - ep1));
		}
	    if (ep2 > 0.0)
		fk42ecl (dtheta, dphi, ep2);
	    else
		fk42ecl (dtheta, dphi, 1950.0);
	    }
        else if (sys1 == WCS_J2000) {
	    if (ep2 != 0.0 && (*ptheta != 0.0 || *pphi != 0.0)) {
		*dtheta = *dtheta + (*ptheta * (ep2 - ep1));
		*dphi = *dphi + (*pphi * (ep2 - ep1));
		}
	    fk52ecl (dtheta, dphi, ep2);
	    }
        else if (sys1 == WCS_GALACTIC) {
	    gal2fk5 (dtheta, dphi);
	    fk52ecl (dtheta, dphi, ep2);
	    }
	}

    /* Precess to desired equinox, if necessary */
    if (eq1 != eq2) {
	if (sys2 == WCS_B1950 && eq2 != 1950.0)
	    fk4prec (1950.0, eq2, dtheta, dphi);
	if (sys2 == WCS_J2000 && eq2 != 2000.0)
	    fk5prec (2000.0, eq2, dtheta, dphi);
	}

    /* Keep latitude/declination between +90 and -90 degrees */
    if (*dphi > 90.0) {
	*dphi = 180.0 - *dphi;
	*dtheta = *dtheta + 180.0;
	}
    else if (*dphi < -90.0) {
	*dphi = -180.0 - *dphi;
	*dtheta = *dtheta + 180.0;
	}

    /* Keep longitude/right ascension between 0 and 360 degrees */
    if (*dtheta > 360.0)
	*dtheta = *dtheta - 360.0;
    else if (*dtheta < 0.0)
	*dtheta = *dtheta + 360.0;
    return;
}


/* Convert from coordinate system sys1 to coordinate system sys2 */

void
wcscon (sys1, sys2, eq1, eq2, dtheta, dphi, epoch)

int	sys1;	/* Input coordinate system (J2000, B1950, ECLIPTIC, GALACTIC */
int	sys2;	/* Output coordinate system (J2000, B1950, ECLIPTIC, GALACTIC */
double	eq1;	/* Input equinox (default of sys1 if 0.0) */
double	eq2;	/* Output equinox (default of sys2 if 0.0) */
double	*dtheta; /* Longitude or right ascension in degrees
		   Input in sys1, returned in sys2 */
double	*dphi;	/* Latitude or declination in degrees
		   Input in sys1, returned in sys2 */
double	epoch;	/* Besselian epoch in years */

{
    void fk5prec(), fk4prec();

    /* Set equinoxes if 0.0 */
    if (eq1 == 0.0) {
	if (sys1 == WCS_B1950)
	    eq1 = 1950.0;
	else
	    eq1 = 2000.0;
	}
    if (eq2 == 0.0) {
	if (sys2 == WCS_B1950)
	    eq2 = 1950.0;
	else
	    eq2 = 2000.0;
	}

    /* Set systems and equinoxes so that ICRS coordinates are not precessed */
    if (sys1 == WCS_ICRS && sys2 == WCS_ICRS)
	eq2 = eq1;

    if (sys1 == WCS_J2000 && sys2 == WCS_ICRS && eq1 == 2000.0) {
	eq2 = eq1;
	sys1 = sys2;
	}

    if (sys1 == WCS_ICRS && sys2 == WCS_J2000 && eq2 == 2000.0) {
	eq1 = eq2;
	sys1 = sys2;
	}

    /* If systems and equinoxes are the same, return */
    if (sys2 == sys1 && eq1 == eq2)
	return;

    /* Precess from input equinox, if necessary */
    if (eq1 != eq2) {
	if (sys1 == WCS_B1950 && eq1 != 1950.0)
	   fk4prec (eq1, 1950.0, dtheta, dphi);
	if (sys1 == WCS_J2000 && eq1 != 2000.0)
	   fk5prec (eq1, 2000.0, dtheta, dphi);
	}

    /* Convert to B1950 FK4 */
    if (sys2 == WCS_B1950) {
	if (sys1 == WCS_J2000) {
	    if (epoch > 0)
		fk524e (dtheta, dphi, epoch);
	    else
		fk524 (dtheta, dphi);
	    }
	else if (sys1 == WCS_GALACTIC) 
	    gal2fk4 (dtheta, dphi);
	else if (sys1 == WCS_ECLIPTIC) {
	    if (epoch > 0)
		ecl2fk4 (dtheta, dphi, epoch);
	    else
		ecl2fk4 (dtheta, dphi, 1950.0);
	    }
	}

    else if (sys2 == WCS_J2000) {
        if (sys1 == WCS_B1950) {
            if (epoch > 0)
                fk425e (dtheta, dphi, epoch);
            else
                fk425 (dtheta, dphi);
            }
        else if (sys1 == WCS_GALACTIC)
            gal2fk5 (dtheta, dphi);
	else if (sys1 == WCS_ECLIPTIC) {
	    if (epoch > 0)
		ecl2fk5 (dtheta, dphi, epoch);
	    else
		ecl2fk5 (dtheta, dphi, 2000.0);
	    }
	}

    else if (sys2 == WCS_GALACTIC) {
        if (sys1 == WCS_B1950)
	    fk42gal (dtheta, dphi);
        else if (sys1 == WCS_J2000)
	    fk52gal (dtheta, dphi);
        else if (sys1 == WCS_ECLIPTIC) {
	    if (epoch > 0)
		ecl2fk5 (dtheta, dphi, epoch);
	    else
		ecl2fk5 (dtheta, dphi, 2000.0);
	    fk52gal (dtheta, dphi);
	    }
	}

    else if (sys2 == WCS_ECLIPTIC) {
        if (sys1 == WCS_B1950) {
	    if (epoch > 0)
		fk42ecl (dtheta, dphi, epoch);
	    else
		fk42ecl (dtheta, dphi, 1950.0);
	    }
        else if (sys1 == WCS_J2000) {
	    if (epoch > 0)
		fk52ecl (dtheta, dphi, epoch);
	    else
		fk52ecl (dtheta, dphi, 2000.0);
	    }
        else if (sys1 == WCS_GALACTIC) {
	    gal2fk5 (dtheta, dphi);
	    if (epoch > 0)
		fk52ecl (dtheta, dphi, epoch);
	    else
		fk52ecl (dtheta, dphi, 2000.0);
	    }
	}

    /* Precess to desired equinox, if necessary */
    if (eq1 != eq2) {
	if (sys2 == WCS_B1950 && eq2 != 1950.0)
	    fk4prec (1950.0, eq2, dtheta, dphi);
	if (sys2 == WCS_J2000 && eq2 != 2000.0)
	    fk5prec (2000.0, eq2, dtheta, dphi);
	}

    /* Keep latitude/declination between +90 and -90 degrees */
    if (*dphi > 90.0) {
	*dphi = 180.0 - *dphi;
	*dtheta = *dtheta + 180.0;
	}
    else if (*dphi < -90.0) {
	*dphi = -180.0 - *dphi;
	*dtheta = *dtheta + 180.0;
	}

    /* Keep longitude/right ascension between 0 and 360 degrees */
    if (*dtheta > 360.0)
	*dtheta = *dtheta - 360.0;
    else if (*dtheta < 0.0)
	*dtheta = *dtheta + 360.0;

    return;
}


/* Set coordinate system from string */
int
wcscsys (wcstring)

char *wcstring;		/* Name of coordinate system */
{
    double equinox;

    if (wcstring[0] == 'J' || wcstring[0] == 'j' ||
	!strcmp (wcstring,"2000") || !strcmp (wcstring, "2000.0") ||
	!strcmp (wcstring,"ICRS") || !strcmp (wcstring, "icrs") ||
	!strncmp (wcstring,"FK5",3) || !strncmp (wcstring, "fk5",3))
	return WCS_J2000;

    if (wcstring[0] == 'B' || wcstring[0] == 'b' ||
	!strcmp (wcstring,"1950") || !strcmp (wcstring, "1950.0") ||
	!strncmp (wcstring,"FK4",3) || !strncmp (wcstring, "fk4",3))
	return WCS_B1950;

    else if (wcstring[0] == 'I' || wcstring[0] == 'i' )
	return WCS_ICRS;

    else if (wcstring[0] == 'G' || wcstring[0] == 'g' )
	return WCS_GALACTIC;

    else if (wcstring[0] == 'E' || wcstring[0] == 'e' )
	return WCS_ECLIPTIC;

    else if (wcstring[0] == 'A' || wcstring[0] == 'a' )
	return WCS_ALTAZ;

    else if (wcstring[0] == 'N' || wcstring[0] == 'n' )
	return WCS_NPOLE;

    else if (wcstring[0] == 'L' || wcstring[0] == 'l' )
	return WCS_LINEAR;

    else if (!strncasecmp (wcstring, "pixel", 5))
	return WCS_XY;

    else if (wcstring[0] == 'P' || wcstring[0] == 'p' )
	return WCS_PLANET;

    else if (isnum (wcstring)) {
	equinox = atof (wcstring);
	if (equinox > 1980.0)
	    return WCS_J2000;
	else if (equinox > 1900.0)
	    return WCS_B1950;
	else
	    return -1;
	}
    else
	return -1;
}


/* Set equinox from string (return 0.0 if not obvious) */

double
wcsceq (wcstring)

char *wcstring;		/* Name of coordinate system */
{
    if (wcstring[0] == 'J' || wcstring[0] == 'j' ||
	wcstring[0] == 'B' || wcstring[0] == 'b')
	return (atof (wcstring+1));
    else if (!strncmp (wcstring, "FK4",3) ||
	     !strncmp (wcstring, "fk4",3))
	return (1950.0);
    else if (!strncmp (wcstring, "FK5",3) ||
	     !strncmp (wcstring, "fk5",3))
	return (2000.0);
    else if (!strncmp (wcstring, "ICRS",4) ||
	     !strncmp (wcstring, "icrs",4))
	return (2000.0);
    else if (wcstring[0] == '1' || wcstring[0] == '2')
	return (atof (wcstring));
    else
	return (0.0);
}


/* Set coordinate system type string from system and equinox */

void
wcscstr (cstr, syswcs, equinox, epoch)

char	*cstr;		/* Coordinate system string (returned) */
int	syswcs;		/* Coordinate system code */
double	equinox;	/* Equinox of coordinate system */
double	epoch;		/* Epoch of coordinate system */
{

    char *estr;

    if (syswcs == WCS_XY) {
	strcpy (cstr, "XY");
	return;
	}

    /* Try to figure out coordinate system if it is not set */
    if (epoch == 0.0)
	epoch = equinox;
    if (syswcs < 0) {
	if (equinox > 0.0) {
	    if (equinox == 2000.0)
		syswcs = WCS_J2000;
	    else if (equinox == 1950.0)
		syswcs = WCS_B1950;
	    }
	else if (epoch > 0.0) {
	    if (epoch > 1980.0) {
		syswcs = WCS_J2000;
		equinox = 2000.0;
		}
	    else {
		syswcs = WCS_B1950;
		equinox = 1950.0;
		}
	    }
	else
	    syswcs = WCS_J2000;
	}

    /* Set coordinate system string from system flag and epoch */
    if (syswcs == WCS_B1950) {
	if (epoch == 1950.0 || epoch == 0.0)
	    strcpy (cstr, "B1950");
	else
	    sprintf (cstr, "B%7.2f", equinox);
	if ((estr = strsrch (cstr,".00")) != NULL) {
	    estr[0] = (char) 0;
	    estr[1] = (char) 0;
	    estr[2] = (char) 0;
	    }
	}
    else if (syswcs == WCS_GALACTIC)
	strcpy (cstr, "galactic");
    else if (syswcs == WCS_ECLIPTIC)
	strcpy (cstr, "ecliptic");
    else if (syswcs == WCS_J2000) {
	if (epoch == 2000.0 || epoch == 0.0)
	    strcpy (cstr, "J2000");
	else
	    sprintf (cstr, "J%7.2f", equinox);
	if ((estr = strsrch (cstr,".00")) != NULL) {
	    estr[0] = (char) 0;
	    estr[1] = (char) 0;
	    estr[2] = (char) 0;
	    }
	}
    else if (syswcs == WCS_ICRS) {
	strcpy (cstr, "ICRS");
	}
    else if (syswcs == WCS_PLANET) {
	strcpy (cstr, "PLANET");
	}
    else if (syswcs == WCS_LINEAR || syswcs == WCS_XY) {
	strcpy (cstr, "LINEAR");
	}
    return;
}


/*  Constant vector and matrix (by columns)
    These values were obtained by inverting C.Hohenkerk's forward matrix
    (private communication), which agrees with the one given in reference
    2 but which has one additional decimal place.  */

static double a[3] = {-1.62557e-6, -0.31919e-6, -0.13843e-6};
static double ad[3] = {1.245e-3,  -1.580e-3,  -0.659e-3};
static double d2pi = 6.283185307179586476925287;	/* two PI */
static double tiny = 1.e-30; /* small number to avoid arithmetic problems */

/* FK524  convert J2000 FK5 star data to B1950 FK4
   based on Starlink sla_fk524 by P.T.Wallace 27 October 1987 */

static double emi[6][6] = {
    {	 0.9999256795,		/* emi[0][0] */
	 0.0111814828,		/* emi[0][1] */
	 0.0048590039,		/* emi[0][2] */
	-0.00000242389840,	/* emi[0][3] */
	-0.00000002710544,	/* emi[0][4] */
	-0.00000001177742 },	/* emi[0][5] */
 
    {	-0.0111814828,		/* emi[1][0] */
	 0.9999374849,		/* emi[1][1] */
	-0.0000271771,		/* emi[1][2] */
	 0.00000002710544,	/* emi[1][3] */
	-0.00000242392702,	/* emi[1][4] */
	 0.00000000006585 },	/* emi[1][5] */
 
    {	-0.0048590040,		/* emi[2][0] */
	-0.0000271557,		/* emi[2][1] */
	 0.9999881946,		/* emi[2][2] */
	 0.00000001177742,	/* emi[2][3] */
	 0.00000000006585,	/* emi[2][4] */
	-0.00000242404995 },	/* emi[2][5] */
 
    {	-0.000551,		/* emi[3][0] */
	 0.238509,		/* emi[3][1] */
	-0.435614,		/* emi[3][2] */
	 0.99990432,		/* emi[3][3] */
	 0.01118145,		/* emi[3][4] */
	 0.00485852 },		/* emi[3][5] */
 
    {	-0.238560,		/* emi[4][0] */
	-0.002667,		/* emi[4][1] */
	 0.012254,		/* emi[4][2] */
	-0.01118145,		/* emi[4][3] */
	 0.99991613,		/* emi[4][4] */
	-0.00002717 },		/* emi[4][5] */
 
    {	 0.435730,		/* emi[5][0] */
	-0.008541,		/* emi[5][1] */
	 0.002117,		/* emi[5][2] */
	-0.00485852,		/* emi[5][3] */
	-0.00002716,		/* emi[5][4] */
	 0.99996684 }		/* emi[5][5] */
    };

void
fk524 (ra,dec)

double	*ra;		/* Right ascension in degrees (J2000 in, B1950 out) */
double	*dec;		/* Declination in degrees (J2000 in, B1950 out) */

{
    double	rapm;	/* Proper motion in right ascension */
    double	decpm;	/* Proper motion in declination  */
			/* In:  deg/jul.yr.  Out: deg/trop.yr.  */

    rapm = (double) 0.0;
    decpm = (double) 0.0;
    fk524m (ra, dec, &rapm, &decpm);
    return;
}

void
fk524e (ra, dec, epoch)

double	*ra;		/* Right ascension in degrees (J2000 in, B1950 out) */
double	*dec;		/* Declination in degrees (J2000 in, B1950 out) */
double	epoch;		/* Besselian epoch in years */

{
    double	rapm;	/* Proper motion in right ascension */
    double	decpm;	/* Proper motion in declination  */
			/* In:  deg/jul.yr.  Out: deg/trop.yr.  */

    rapm = (double) 0.0;
    decpm = (double) 0.0;
    fk524m (ra, dec, &rapm, &decpm);
    *ra = *ra + (rapm * (epoch - 1950.0));
    *dec = *dec + (decpm * (epoch - 1950.0));
    return;
}

void
fk524m (ra,dec,rapm,decpm)

double	*ra;		/* Right ascension in degrees (J2000 in, B1950 out) */
double	*dec;		/* Declination in degrees (J2000 in, B1950 out) */
double	*rapm;		/* Proper motion in right ascension */
double	*decpm;		/* Proper motion in declination */
			/* In:  ra/dec deg/jul.yr.  Out: ra/dec deg/trop.yr.  */

{
    double parallax = 0.0;
    double rv = 0.0;

    fk524pv (ra, dec, rapm, decpm, &parallax, &rv);
    return;
}


void
fk524pv (ra,dec,rapm,decpm, parallax, rv)

double	*ra;		/* Right ascension in degrees (J2000 in, B1950 out) */
double	*dec;		/* Declination in degrees (J2000 in, B1950 out) */
double	*rapm;		/* Proper motion in right ascension */
double	*decpm;		/* Proper motion in declination
			 * In:  ra/dec degrees/Julian year (not ra*cos(dec))
			 * Out: ra/dec degrees/tropical year */
double *parallax;	/* Parallax (arcsec) */
double *rv;		/* Rradial velocity (km/s, +ve = moving away) */

/*  This routine converts stars from the IAU 1976 FK5 Fricke
    system, to the old Bessel-Newcomb FK4 system, using Yallop's
    implementation (see ref 2) of a matrix method due to Standish
    (see ref 3).  The numerical values of ref 2 are used canonically.

 *  Conversion from other than Julian epoch 2000.0 to other than Besselian
    epoch 1950.0 will require use of the appropriate precession, proper
    motion, and e-terms routines before and/or after fk524 is called.
 
 *  In the FK4 catalogue the proper motions of stars within 10 degrees
    of the poles do not embody the differential e-term effect and should,
    strictly speaking, be handled in a different manner from stars outside
    these regions.  however, given the general lack of homogeneity of the
    star data available for routine astrometry, the difficulties of handling
    positions that may have been determined from astrometric fields spanning
    the polar and non-polar regions, the likelihood that the differential
    e-terms effect was not taken into account when allowing for proper motion
    in past astrometry, and the undesirability of a discontinuity in the
    algorithm, the decision has been made in this routine to include the
    effect of differential e-terms on the proper motions for all stars,
    whether polar or not, at epoch 2000, and measuring on the sky rather
    than in terms of dra, the errors resulting from this simplification are
    less than 1 milliarcsecond in position and 1 milliarcsecond per century
    in proper motion.

    References:

      1  "Mean and apparent place computations in the new IAU System.
          I. The transformation of astrometric catalog systems to the
 	  equinox J2000.0." Smith, C.A.; Kaplan, G.H.; Hughes, J.A.;
	  Seidelmann, P.K.; Yallop, B.D.; Hohenkerk, C.Y.
 	  Astronomical Journal vol. 97, Jan. 1989, p. 265-273.

      2  "Mean and apparent place computations in the new IAU System.
	  II. Transformation of mean star places from FK4 B1950.0 to
 	  FK5 J2000.0 using matrices in 6-space."  Yallop, B.D.;
	  Hohenkerk, C.Y.; Smith, C.A.; Kaplan, G.H.; Hughes, J.A.;
	  Seidelmann, P.K.; Astronomical Journal vol. 97, Jan. 1989,
	  p. 274-279.
 
      3  Seidelmann, P.K. (ed), 1992.  "Explanatory Supplement to
         the Astronomical Almanac", ISBN 0-935702-68-7.

      4  "Conversion of positions and proper motions from B1950.0 to the
	  IAU system at J2000.0", Standish, E.M.  Astronomy and
	  Astrophysics, vol. 115, no. 1, Nov. 1982, p. 20-22.

   P.T.Wallace   Starlink   19 December 1993
   Doug Mink     Smithsonian Astrophysical Observatory 1 November 2000 */

{
    double r2000,d2000;		/* J2000.0 ra,dec (radians) */
    double r1950,d1950;		/* B1950.0 ra,dec (rad) */

    /* Miscellaneous */
    double ur,ud;
    double sr, cr, sd, cd, x, y, z, w, wd;
    double v1[6],v2[6];
    double xd,yd,zd;
    double rxyz, rxysq, rxy;
    double dra,ddec;
    int	i,j;
    int	diag = 0;

    /* Constants */
    double zero = (double) 0.0;
    double vf = 21.095;	/* Km per sec to AU per tropical century */
			/* = 86400 * 36524.2198782 / 149597870 */

    /* Convert J2000 RA and Dec from degrees to radians */
    r2000 = degrad (*ra);
    d2000 = degrad (*dec);

    /* Convert J2000 RA and Dec proper motion from degrees/year to arcsec/tc */
    ur = *rapm  * 360000.0;
    ud = *decpm * 360000.0;

    /* Spherical to Cartesian */
    sr = sin (r2000);
    cr = cos (r2000);
    sd = sin (d2000);
    cd = cos (d2000);

    x = cr * cd;
    y = sr * cd;
    z = sd;

    v1[0] = x;
    v1[1] = y;
    v1[2] = z;
 
    if (ur != zero || ud != zero) {
	v1[3] = -(ur*y) - (cr*sd*ud);
	v1[4] =  (ur*x) - (sr*sd*ud);
	v1[5] =          (cd*ud);
	}
    else {
	v1[3] = zero;
	v1[4] = zero;
	v1[5] = zero;
	}
 
    /* Convert position + velocity vector to bn system */
    for (i = 0; i < 6; i++) {
	w = zero;
	for (j = 0; j < 6; j++) {
	    w = w + emi[i][j] * v1[j];
	    }
	v2[i] = w;
	}
 
    /* Vector components */
    x = v2[0];
    y = v2[1];
    z = v2[2];
    rxyz = sqrt (x*x + y*y + z*z);

    /* Magnitude of position vector */
    rxyz = sqrt (x*x + y*y + z*z);
 
    /* Apply e-terms to position */
    w = (x * a[0]) + (y * a[1]) + (z * a[2]);
    x = x + (a[0] * rxyz) - (w * x);
    y = y + (a[1] * rxyz) - (w * z);
    z = z + (a[2] * rxyz) - (w * z);
 
    /* Recompute magnitude of position vector */
    rxyz = sqrt (x*x + y*y + z*z);

    /* Apply e-terms to position and velocity */
    x = v2[0];
    y = v2[1];
    z = v2[2];
    w = (x * a[0]) + (y * a[1]) + (z * a[2]);
    wd = (x * ad[0]) + (y * ad[1]) + (z * ad[2]);
    x = x + (a[0] * rxyz) - (w * x);
    y = y + (a[1] * rxyz) - (w * y);
    z = z + (a[2] * rxyz) - (w * z);
    xd = v2[3] + (ad[0] * rxyz) - (wd * x);
    yd = v2[4] + (ad[1] * rxyz) - (wd * y);
    zd = v2[5] + (ad[2] * rxyz) - (wd * z);

    /*  Convert to spherical  */
    rxysq = (x * x) + (y * y);
    rxy = sqrt (rxysq);

    /* Convert back to spherical coordinates */
    if (x == zero && y == zero)
	r1950 = zero;
    else {
	r1950 = atan2 (y,x);
	if (r1950 < zero)
	    r1950 = r1950 + d2pi;
	}
    d1950 = atan2 (z,rxy);

    if (rxy > tiny) {
	ur = (x*yd - y*xd) / rxysq;
	ud = (zd*rxysq - z * (x*xd + y*yd)) / ((rxysq + z*z) * rxy);
	}

    if (*parallax > tiny) {
	*rv = ((x * xd) + (y * yd) + (z * zd)) / (*parallax * vf * rxyz);
	*parallax = *parallax / rxyz;
	}

    /* Return results */
    *ra = raddeg (r1950);
    *dec = raddeg (d1950);
    *rapm  = ur / 360000.0;
    *decpm = ud / 360000.0;

    if (diag) {
	dra = 240.0 * raddeg (r1950 - r2000);
	ddec = 3600.0 * raddeg (d1950 - d2000);
	fprintf(stderr,"B1950-J2000: dra= %11.5f sec  ddec= %f11.5f arcsec\n",
		dra, ddec);
	}

    return;
}


/* Convert B1950.0 FK4 star data to J2000.0 FK5 */
static double em[6][6] = {
    {	 0.9999256782,		/* em[0][0] */
	-0.0111820611,		/* em[0][1] */
	-0.0048579477,		/* em[0][2] */
	 0.00000242395018,	/* em[0][3] */
	-0.00000002710663,	/* em[0][4] */
	-0.00000001177656 },	/* em[0][5] */
 
    {	 0.0111820610,		/* em[1][0] */
	 0.9999374784,		/* em[1][1] */
	-0.0000271765,		/* em[1][2] */
	 0.00000002710663,	/* em[1][3] */
	 0.00000242397878,	/* em[1][4] */
	-0.00000000006587 },	/* em[1][5] */
 
    {	 0.0048579479,		/* em[2][0] */
	-0.0000271474,		/* em[2][1] */
	 0.9999881997,		/* em[2][2] */
	 0.00000001177656,	/* em[2][3] */
	-0.00000000006582,	/* em[2][4] */
	 0.00000242410173 },	/* em[2][5] */
 
    {	-0.000551,		/* em[3][0] */
	-0.238565,		/* em[3][1] */
	 0.435739,		/* em[3][2] */
	 0.99994704,		/* em[3][3] */
	-0.01118251,		/* em[3][4] */
	-0.00485767 },		/* em[3][5] */
 
    {	 0.238514,		/* em[4][0] */
	-0.002667,		/* em[4][1] */
	-0.008541,		/* em[4][2] */
	 0.01118251,		/* em[4][3] */
	 0.99995883,		/* em[4][4] */
	-0.00002718 },		/* em[4][5] */
 
    {	-0.435623,		/* em[5][0] */
	 0.012254,		/* em[5][1] */
	 0.002117,		/* em[5][2] */
	 0.00485767,		/* em[5][3] */
	-0.00002714,		/* em[5][4] */
	 1.00000956 }		/* em[5][5] */
    };

void
fk425 (ra, dec)

double	*ra;		/* Right ascension in degrees (B1950 in, J2000 out) */
double	*dec;		/* Declination in degrees (B1950 in, J2000 out) */

{
double	rapm;		/* Proper motion in right ascension */
double	decpm;		/* Proper motion in declination  */
			/* In: rad/trop.yr.  Out:  rad/jul.yr. */

    rapm = (double) 0.0;
    decpm = (double) 0.0;
    fk425m (ra, dec, &rapm, &decpm);
    return;
}


void
fk425e (ra, dec, epoch)

double	*ra;		/* Right ascension in degrees (B1950 in, J2000 out) */
double	*dec;		/* Declination in degrees (B1950 in, J2000 out) */
double	epoch;		/* Besselian epoch in years */
{
double	rapm;		/* Proper motion in right ascension */
double	decpm;		/* Proper motion in declination  */
			/* In: rad/trop.yr.  Out:  rad/jul.yr. */

    rapm = (double) 0.0;
    decpm = (double) 0.0;
    fk425m (ra, dec, &rapm, &decpm);
    *ra = *ra + (rapm * (epoch - 2000.0));
    *dec = *dec + (decpm * (epoch - 2000.0));
    return;
}

void
fk425m (ra, dec, rapm, decpm)

double	*ra, *dec;	/* Right ascension and declination in degrees
			   input:  B1950.0,FK4	returned:  J2000.0,FK5 */
double	*rapm, *decpm;	/* Proper motion in right ascension and declination
			   input:  B1950.0,FK4	returned:  J2000.0,FK5
			           ra/dec deg/trop.yr.     ra/dec deg/jul.yr.  */
{
    double parallax = 0.0;
    double rv = 0.0;

    fk425pv (ra, dec, rapm, decpm, &parallax, &rv);
    return;
}


void
fk425pv (ra,dec,rapm,decpm, parallax, rv)

double	*ra;		/* Right ascension in degrees (J2000 in, B1950 out) */
double	*dec;		/* Declination in degrees (J2000 in, B1950 out) */
double	*rapm;		/* Proper motion in right ascension */
double	*decpm;		/* Proper motion in declination
			 * In:  ra/dec degrees/Julian year (not ra*cos(dec))
			 * Out: ra/dec degrees/tropical year */
double *parallax;	/* Parallax (arcsec) */
double *rv;		/* Rradial velocity (km/s, +ve = moving away) */

/*  This routine converts stars from the old Bessel-Newcomb FK4 system
    to the IAU 1976 FK5 Fricke system, using Yallop's implementation
    (see ref 2) of a matrix method due to Standish (see ref 3).  The
    numerical values of ref 2 are used canonically.

 *  Conversion from other than Besselian epoch 1950.0 to other than Julian
    epoch 2000.0 will require use of the appropriate precession, proper
    motion, and e-terms routines before and/or after fk425 is called.
 
 *  In the FK4 catalogue the proper motions of stars within 10 degrees
    of the poles do not embody the differential e-term effect and should,
    strictly speaking, be handled in a different manner from stars outside
    these regions.  however, given the general lack of homogeneity of the
    star data available for routine astrometry, the difficulties of handling
    positions that may have been determined from astrometric fields spanning
    the polar and non-polar regions, the likelihood that the differential
    e-terms effect was not taken into account when allowing for proper motion
    in past astrometry, and the undesirability of a discontinuity in the
    algorithm, the decision has been made in this routine to include the
    effect of differential e-terms on the proper motions for all stars,
    whether polar or not, at epoch 2000, and measuring on the sky rather
    than in terms of dra, the errors resulting from this simplification are
    less than 1 milliarcsecond in position and 1 milliarcsecond per century
    in proper motion.

    References:

      1  "Mean and apparent place computations in the new IAU System.
          I. The transformation of astrometric catalog systems to the
 	  equinox J2000.0." Smith, C.A.; Kaplan, G.H.; Hughes, J.A.;
	  Seidelmann, P.K.; Yallop, B.D.; Hohenkerk, C.Y.
 	  Astronomical Journal vol. 97, Jan. 1989, p. 265-273.

      2  "Mean and apparent place computations in the new IAU System.
	  II. Transformation of mean star places from FK4 B1950.0 to
 	  FK5 J2000.0 using matrices in 6-space."  Yallop, B.D.;
	  Hohenkerk, C.Y.; Smith, C.A.; Kaplan, G.H.; Hughes, J.A.;
	  Seidelmann, P.K.; Astronomical Journal vol. 97, Jan. 1989,
	  p. 274-279.

      3  "Conversion of positions and proper motions from B1950.0 to the
	  IAU system at J2000.0", Standish, E.M.  Astronomy and
	  Astrophysics, vol. 115, no. 1, Nov. 1982, p. 20-22.

   P.T.Wallace   Starlink   20 December 1993
   Doug Mink     Smithsonian Astrophysical Observatory  7 June 1995 */

{
    double r1950,d1950;		/* B1950.0 ra,dec (rad) */
    double r2000,d2000;		/* J2000.0 ra,dec (rad) */

    /* Miscellaneous */
    double ur,ud,sr,cr,sd,cd,w,wd;
    double x,y,z,xd,yd,zd, dra,ddec;
    double rxyz, rxysq, rxy, rxyzsq, spxy, spxyz;
    int	i,j;
    int	diag = 0;

    double r0[3],rd0[3];	/* star position and velocity vectors */
    double v1[6],v2[6];		/* combined position and velocity vectors */

    /* Constants */
    double zero = (double) 0.0;
    double vf = 21.095;	/* Km per sec to AU per tropical century */
			/* = 86400 * 36524.2198782 / 149597870 */

    /* Convert B1950 RA and Dec from degrees to radians */
    r1950 = degrad (*ra);
    d1950 = degrad (*dec);

    /* Convert B1950 RA and Dec proper motion from degrees/year to arcsec/tc */
    ur = *rapm  * 360000.0;
    ud = *decpm * 360000.0;

    /* Convert direction to Cartesian */
    sr = sin (r1950);
    cr = cos (r1950);
    sd = sin (d1950);
    cd = cos (d1950);
    r0[0] = cr * cd;
    r0[1] = sr * cd;
    r0[2] = sd;

    /* Convert motion to Cartesian */
    w = vf * *rv * *parallax;
    if (ur != zero || ud != zero || (*rv != zero && *parallax != zero)) {
	rd0[0] = (-sr * cd * ur) - (cr * sd * ud) + (w * r0[0]);
	rd0[1] =  (cr * cd * ur) - (sr * sd * ud) + (w * r0[1]);
	rd0[2] = 	                (cd * ud) + (w * r0[2]);
	}
    else {
	rd0[0] = zero;
	rd0[1] = zero;
	rd0[2] = zero;
	}

    /* Remove e-terms from position and express as position+velocity 6-vector */
    w = (r0[0] * a[0]) + (r0[1] * a[1]) + (r0[2] * a[2]);
    for (i = 0; i < 3; i++)
	v1[i] = r0[i] - a[i] + (w * r0[i]);

    /* Remove e-terms from proper motion and express as 6-vector */
    wd = (r0[0] * ad[0]) + (r0[1] * ad[1]) + (r0[2] * ad[2]);
    for (i = 0; i < 3; i++)
	v1[i+3] = rd0[i] - ad[i] + (wd * r0[i]);

    /* Alternately: Put proper motion in 6-vector without adding e-terms
    for (i = 0; i < 3; i++)
	v1[i+3] = rd0[i]; */

    /* Convert position + velocity vector to FK5 system */
    for (i = 0; i < 6; i++) {
	w = zero;
	for (j = 0; j < 6; j++) {
	    w += em[i][j] * v1[j];
	    }
	v2[i] = w;
	}

    /* Vector components */
    x = v2[0];
    y = v2[1];
    z = v2[2];
    xd = v2[3];
    yd = v2[4];
    zd = v2[5];

    /* Magnitude of position vector */
    rxysq = x*x + y*y;
    rxy = sqrt (rxysq);
    rxyzsq = rxysq + z*z;
    rxyz = sqrt (rxyzsq);

    spxy = (x * xd) + (y * yd);
    spxyz = spxy + (z * zd);

    /* Convert back to spherical coordinates */
    if (x == zero && y == zero)
	r2000 = zero;
    else {
	r2000 = atan2 (y,x);
	if (r2000 < zero)
	    r2000 = r2000 + d2pi;
	}
    d2000 = atan2 (z,rxy);

    if (rxy > tiny) {
	ur = ((x * yd) - (y * xd)) / rxysq;
	ud = ((zd * rxysq) - (z * spxy)) / (rxyzsq * rxy);
	}

    if (*parallax > tiny) {
	*rv = spxyz / (*parallax * rxyz * vf);
	*parallax = *parallax / rxyz;
	}

    /* Return results */
    *ra = raddeg (r2000);
    *dec = raddeg (d2000);
    *rapm  = ur / 360000.0;
    *decpm = ud / 360000.0;

    if (diag) {
	dra = 240.0 * raddeg (r2000 - r1950);
	ddec = 3600.0 * raddeg (d2000 - d1950);
	fprintf(stderr,"J2000-B1950: dra= %11.5f sec  ddec= %f11.5f arcsec\n",
		dra, ddec);
	}
    return;
}

int	idg=0;

/*  l2,b2 system of galactic coordinates
 *  p = 192.25       ra of galactic north pole (mean b1950.0)
 *  q =  62.6        inclination of galactic to mean b1950.0 equator
 *  r =  33          longitude of ascending node
 *  p,q,r are degrees

 *  Equatorial to galactic rotation matrix
    (The Eulerian angles are p, q, 90-r)
	+cp.cq.sr-sp.cr	+sp.cq.sr+cp.cr	-sq.sr
	-cp.cq.cr-sp.sr	-sp.cq.cr+cp.sr	+sq.cr
	cp.sq		+sp.sq		+cq
 */

static
double bgal[3][3] =
	{{-0.066988739415,-0.872755765852,-0.483538914632},
	{0.492728466075,-0.450346958020, 0.744584633283},
	{-0.867600811151,-0.188374601723, 0.460199784784}};

/*---  Transform B1950.0 FK4 equatorial coordinates to
 *     IAU 1958 galactic coordinates */

void
fk42gal (dtheta,dphi)

double *dtheta;	/* B1950.0 FK4 right ascension in degrees
		   Galactic longitude (l2) in degrees (returned) */
double *dphi;	/* B1950.0 FK4 declination in degrees
		   Galactic latitude (b2) in degrees (returned) */

/*  Input equatorial coordinates are B1950 FK4.
    Use fk52gal() to convert from j2000.0 coordinates.
    Reference: Blaauw et al, MNRAS,121,123 (1960) */
{
    double pos[3],pos1[3],r,dl,db,rl,rb,rra,rdec,dra,ddec;
    void v2s3(),s2v3();
    int i;
    char *eqcoor, *eqstrn();

    dra = *dtheta;
    ddec = *dphi;
    rra = degrad (dra);
    rdec = degrad (ddec);

    /*  remove e-terms */
    /*	call jpabe (rra,rdec,-1,idg) */

    /*  Spherical to Cartesian */
    r = 1.;
    s2v3 (rra,rdec,r,pos);

    /*  rotate to galactic */
    for (i = 0; i<3; i++) {
	pos1[i] = pos[0]*bgal[i][0] + pos[1]*bgal[i][1] + pos[2]*bgal[i][2];
	}

    /*  Cartesian to spherical */
    v2s3 (pos1,&rl,&rb,&r);

    dl = raddeg (rl);
    db = raddeg (rb);
    *dtheta = dl;
    *dphi = db;

    /*  Print result if in diagnostic mode */
    if (idg) {
	eqcoor = eqstrn (dra,ddec);
	fprintf (stderr,"FK42GAL: B1950 RA,Dec= %s\n",eqcoor);
	fprintf (stderr,"FK42GAL: long = %.5f lat = %.5f\n",dl,db);
	free (eqcoor);
	}

    return;
}


/*--- Transform IAU 1958 galactic coordinates to B1950.0 'FK4'
 *    equatorial coordinates */

void
gal2fk4 (dtheta,dphi)

double *dtheta;	/* Galactic longitude (l2) in degrees
		   B1950 FK4 RA in degrees (returned) */
double *dphi;	/* Galactic latitude (b2) in degrees
		   B1950 FK4 Dec in degrees (returned) */

/*  Output equatorial coordinates are B1950.0 FK4.
    Use gal2fk5() to convert to J2000 coordinates.
    Reference:  Blaauw et al, MNRAS,121,123 (1960) */

{
    double pos[3],pos1[3],r,dl,db,rl,rb,rra,rdec,dra,ddec;
    void v2s3(),s2v3();
    char *eqcoor, *eqstrn();
    int i;

    /*  spherical to cartesian */
    dl = *dtheta;
    db = *dphi;
    rl = degrad (dl);
    rb = degrad (db);
    r = 1.0;
    s2v3 (rl,rb,r,pos);

    /*  rotate to equatorial coordinates */
    for (i = 0; i < 3; i++) {
	pos1[i] = pos[0]*bgal[0][i] + pos[1]*bgal[1][i] + pos[2]*bgal[2][i];
	}

    /*  cartesian to spherical */
    v2s3 (pos1,&rra,&rdec,&r);

/*  introduce e-terms */
/*	jpabe (rra,rdec,-1,idg); */

    dra = raddeg (rra);
    ddec = raddeg (rdec);
    *dtheta = dra;
    *dphi = ddec;

    /*  print result if in diagnostic mode */
    if (idg) {
	fprintf (stderr,"GAL2FK4: long = %.5f lat = %.5f\n",dl,db);
	eqcoor = eqstrn (dra,ddec);
	fprintf (stderr,"GAL2FK4: B1950 RA,Dec= %s\n",eqcoor);
	free (eqcoor);
	}

    return;
}


/*  l2,b2 system of galactic coordinates
    p = 192.25       ra of galactic north pole (mean b1950.0)
    q =  62.6        inclination of galactic to mean b1950.0 equator
    r =  33          longitude of ascending node
    p,q,r are degrees */

/*  Equatorial to galactic rotation matrix
    The eulerian angles are p, q, 90-r
	+cp.cq.sr-sp.cr     +sp.cq.sr+cp.cr     -sq.sr
	-cp.cq.cr-sp.sr     -sp.cq.cr+cp.sr     +sq.cr
	+cp.sq              +sp.sq              +cq		*/

static
double jgal[3][3] =
	{{-0.054875539726,-0.873437108010,-0.483834985808},
	{0.494109453312,-0.444829589425, 0.746982251810},
	{-0.867666135858,-0.198076386122, 0.455983795705}};

/* Transform J2000 equatorial coordinates to IAU 1958 galactic coordinates */

void
fk52gal (dtheta,dphi)

double *dtheta;	/* J2000 right ascension in degrees
		   Galactic longitude (l2) in degrees (returned) */
double *dphi;	/* J2000 declination in degrees
		   Galactic latitude (b2) in degrees (returned) */

/* Rotation matrices by P.T.Wallace, Starlink eqgal and galeq, March 1986 */

/*  Input equatorial coordinates are J2000 FK5.
    Use gal2fk4() if converting from B1950 FK4 coordinates.
    Reference: Blaauw et al, MNRAS,121,123 (1960) */
{
    double pos[3],pos1[3],r,dl,db,rl,rb,rra,rdec,dra,ddec;
    void v2s3(),s2v3();
    char *eqcoor, *eqstrn();
    int i;

    /*  Spherical to cartesian */
    dra = *dtheta;
    ddec = *dphi;
    rra = degrad (dra);
    rdec = degrad (ddec);
    r = 1.0;
    (void)s2v3 (rra,rdec,r,pos);

    /*  Rotate to galactic */
    for (i = 0; i < 3; i++) {
	pos1[i] = pos[0]*jgal[i][0] + pos[1]*jgal[i][1] + pos[2]*jgal[i][2];
	}

    /*  Cartesian to spherical */
    v2s3 (pos1,&rl,&rb,&r);

    dl = raddeg (rl);
    db = raddeg (rb);
    *dtheta = dl;
    *dphi = db;

    /*  Print result if in diagnostic mode */
    if (idg) {
	eqcoor = eqstrn (dra,ddec);
	fprintf (stderr,"FK52GAL: J2000 RA,Dec= %s\n",eqcoor);
	fprintf (stderr,"FK52GAL: long = %.5f lat = %.5f\n",dl,db);
	free (eqcoor);
	}

    return;
}


/*--- Transform IAU 1958 galactic coordinates to J2000 equatorial coordinates */

void
gal2fk5 (dtheta,dphi)

double *dtheta;	/* Galactic longitude (l2) in degrees
		   J2000.0 ra in degrees (returned) */
double *dphi;	/* Galactic latitude (b2) in degrees
		   J2000.0 dec in degrees (returned) */

/*  Output equatorial coordinates are J2000.
   Use gal2fk4() to convert to B1950 coordinates.
    Reference: Blaauw et al, MNRAS,121,123 (1960) */

{
    double pos[3],pos1[3],r,dl,db,rl,rb,rra,rdec,dra,ddec;
    void v2s3(),s2v3();
    int i;
    char *eqcoor, *eqstrn();

    /*  Spherical to Cartesian */
    dl = *dtheta;
    db = *dphi;
    rl = degrad (dl);
    rb = degrad (db);
    r = 1.0;
    s2v3 (rl,rb,r,pos);

    /*  Rotate to equatorial coordinates */
    for (i = 0; i < 3; i++) {
	    pos1[i] = pos[0]*jgal[0][i] + pos[1]*jgal[1][i] + pos[2]*jgal[2][i];
	    }

    /*  Cartesian to Spherical */
    v2s3 (pos1,&rra,&rdec,&r);
    dra = raddeg (rra);
    ddec = raddeg (rdec);
    *dtheta = dra;
    *dphi = ddec;

    /*  Print result if in diagnostic mode */
    if (idg) {
	fprintf (stderr,"GAL2FK5: long = %.5f lat = %.5f\n",dl,db);
	eqcoor = eqstrn (dra,ddec);
	fprintf (stderr,"GAL2FK5: J2000 RA,Dec= %s\n",eqcoor);
	free (eqcoor);
	}

    return;
}


/* Return string with right ascension in hours and declination in degrees */

char *eqstrn (dra, ddec)

double	dra;		/* Right ascension in degrees */
double	ddec;		/* Declination in degrees */

{
char	*eqcoor;	/* ASCII character string of position (returned) */
char	decp;
int	rah,irm,decd,decm;
double	xpos,ypos,xp,yp,ras,decs;

    /*  Right ascension to hours, minutes, and seconds */
    xpos = dra / 15.0;
    rah = (int) xpos;
    xp = (double) 60.0 * (xpos - (double) rah);
    irm = (int) xp;
    ras = (double) 60.0 * (xp - (double) irm);

    /* Declination to degrees, minutes, seconds */
    if (ddec < 0) {
	ypos = -ddec;
	decp = '-';
	}
    else {
	decp = '+';
	ypos = ddec;
	}
    decd = (int) ypos;
    yp = (double) 60.0 * (ypos - (double) decd);
    decm = (int) yp;
    decs = (double) 60.0 * (yp - (double) decm);

    eqcoor = malloc (32);
    (void)sprintf (eqcoor,"%02d:%02d:%06.3f %c%02d:%02d:%05.2f",
		   rah,irm,ras,decp,decd,decm,decs);
    if (eqcoor[6] == ' ')
	eqcoor[6] = '0';
    if (eqcoor[20] == ' ')
	eqcoor[20] = '0';

    return (eqcoor);
}


/* Convert geocentric equatorial rectangular coordinates to
   right ascension and declination, and distance */


/* These routines are based on similar ones in Pat Wallace's slalib package */

/* Convert B1950 right ascension and declination to ecliptic coordinates */

void
fk42ecl (dtheta, dphi, epoch)

double *dtheta;	/* B1950 right ascension in degrees
		   Galactic longitude (l2) in degrees (returned) */
double *dphi;	/* B1950 declination in degrees
		   Galactic latitude (b2) in degrees (returned) */
double	epoch;	/* Besselian epoch in years */

{
    void fk425e(), fk52ecl();

    /* Convert from B1950 to J2000 coordinates */
    fk425e (dtheta, dphi, epoch);

    /* Convert from J2000 to ecliptic coordinates */
    fk52ecl (dtheta, dphi, epoch);

    return;
}

/* Convert J2000 right ascension and declination to ecliptic coordinates */

void
fk52ecl (dtheta, dphi, epoch)

double *dtheta;	/* J2000 right ascension in degrees
		   Galactic longitude (l2) in degrees (returned) */
double *dphi;	/* J2000 declination in degrees
		   Galactic latitude (b2) in degrees (returned) */
double	epoch;	/* Besselian epoch in years */

{
    int i, j;
    double t, eps0, rphi, rtheta;
    double v1[3], v2[3], r;
    double rmat[9], *rmati;	/* Rotation matrix  */

    void rotmat(), v2s3(), s2v3(), fk5prec();

    /* Precess coordinates from J2000 to epoch */
    if (epoch != 2000.0)
	fk5prec (2000.0, epoch, dtheta, dphi);

    /* Convert from degrees to radians */
    rtheta = degrad (*dtheta);
    rphi = degrad (*dphi);

    /* Convert RA,Dec to x,y,z */
    r = 1.0;
    s2v3 (rtheta, rphi, r, v1);

    /* Interval between basic epoch J2000.0 and current epoch (JC) in centuries*/
    t = (epoch - 2000.0) * 0.01;
 
    /* Mean obliquity */
    eps0 = secrad ((84381.448 + (-46.8150 + (-0.00059 + 0.001813*t) * t) * t));
 
    /* Form the equatorial to ecliptic rotation matrix (IAU 1980 theory).
     *  References: Murray, C.A., Vectorial Astrometry, section 4.3.
     *    The matrix is in the sense   v[ecl]  =  rmat * v[equ];  the
     *    equator, equinox and ecliptic are mean of date. */
    rotmat (1, eps0, 0.0, 0.0, rmat);

    /* Multiply position vector by equatoria to eccliptic rotation matrix */
    rmati = rmat;
    for (i = 0; i < 3; i++) {
	v2[i] = 0;
	for (j = 0; j < 3; j++)
	    v2[i] = v2[i] + (*rmati++ * v1[j]);
	}

    /* Convert x,y,z to latitude, longitude */
    v2s3 (v2, &rtheta, &rphi, &r);

    /* Convert from radians to degrees */
    *dtheta = raddeg (rtheta);
    *dphi = raddeg (rphi);
}


/* Convert ecliptic coordinates to B1950 right ascension and declination */

void
ecl2fk4 (dtheta, dphi, epoch)

double *dtheta;	/* Galactic longitude (l2) in degrees
		   B1950 right ascension in degrees (returned) */
double *dphi;	/* Galactic latitude (b2) in degrees
		   B1950 declination in degrees (returned) */
double	epoch;	/* Besselian epoch in years */

{
    void ecl2fk5(), fk524e();

    /* Convert from ecliptic to J2000 coordinates */
    ecl2fk5 (dtheta, dphi, epoch);

    /* Convert from J2000 to B1950 coordinates */
    fk524e (dtheta, dphi, epoch);

    return;
}



/* Convert ecliptic coordinates to J2000 right ascension and declination */

void
ecl2fk5 (dtheta, dphi, epoch)

double *dtheta;	/* Galactic longitude (l2) in degrees
		   J2000 right ascension in degrees  (returned) */
double *dphi;	/* Galactic latitude (b2) in degrees
		   J2000 declination in degrees (returned) */
double	epoch;	/* Besselian epoch in years */

{
    int i, j;
    double rtheta, rphi, v1[3], v2[3];
    double t, eps0, r;
    double rmat[9];	/* Rotation matrix */
    void v2s3(),s2v3(), fk5prec(), rotmat();

    rtheta = degrad (*dtheta);
    rphi = degrad (*dphi);

    /* Convert RA,Dec to x,y,z */
    r = 1.0;
    s2v3 (rtheta, rphi, r, v1);

    /* Interval between basic epoch J2000.0 and current epoch (JC) in centuries*/
    t = (epoch - 2000.0) * 0.01;
 
    /* Mean obliquity */
    eps0 = secrad ((84381.448 + (-46.8150 + (-0.00059 + 0.001813*t) * t) * t));
 
    /* Form the equatorial to ecliptic rotation matrix (IAU 1980 theory).
     *  References: Murray, C.A., Vectorial Astrometry, section 4.3.
     *    The matrix is in the sense   v[ecl]  =  rmat * v[equ];  the
     *    equator, equinox and ecliptic are mean of date. */
    rotmat (1, eps0, 0.0, 0.0, rmat);
 
    /* Multiply position vector by ecliptic to equatorial rotation matrix */
    for (i = 0; i < 3; i++) {
	v2[i] = 0;
	for (j = 0; j < 3; j++)
	    v2[i] = v2[i] + (rmat[3*j + i] * v1[j]);
	}

    /* Cartesian to spherical */
    v2s3 (v2, &rtheta, &rphi, &r);

    /* Convert from radians to degrees */
    *dtheta = raddeg (rtheta);
    *dphi = raddeg (rphi);

    if (epoch != 2000.0)
	fk5prec (epoch, 2000.0, dtheta, dphi);
}


/* The following routines are modified from Patrick Wallace's SLALIB */

/* Precess coordinates between epochs in FK4 */
void
fk4prec (ep0, ep1, ra, dec)

double ep0;	/* Starting Besselian epoch */
double ep1;	/* Ending Besselian epoch */
double *ra;	/* RA in degrees mean equator & equinox of epoch ep0
		      mean equator & equinox of epoch ep1 (returned) */
double *dec;	/* Dec in degrees mean equator & equinox of epoch ep0
		       mean equator & equinox of epoch ep1 (returned) */
/*
**  Precession - FK4 (Bessel-Newcomb, pre-IAU1976)
**
**  This routine will not correctly convert between FK4 and FK5
**  For output in FK5, precess to 1950.0 and use fk425() on result.
**
**  Based on slaPreces(), P.T.Wallace   Starlink   22 December 1993
*/
{
    int i, j;
    double pm[9], *pmi, v1[3], v2[3], rra, rdec, r;
    void v2s3(),s2v3(), mprecfk4();

    rra = degrad (*ra);
    rdec = degrad (*dec);
    r = 1.0;
 
    /* Generate appropriate precession matrix */
    mprecfk4 ( ep0, ep1, pm );
 
    /* Convert RA,Dec to x,y,z */
    s2v3 (rra, rdec, r, v1);
 
    /* Multiply position vector by precession matrix */
    pmi = pm;
    for (i = 0; i < 3; i++) {
	v2[i] = 0;
	for (j = 0; j < 3; j++)
	    v2[i] = v2[i] + (*pmi++ * v1[j]);
	}
 
    /* Back to RA,Dec */
    v2s3 (v2, &rra, &rdec, &r);

    /* Convert from radians to degrees */
    *ra = raddeg (rra);
    *dec = raddeg (rdec);
}

void
fk5prec (ep0, ep1, ra, dec)

double ep0;	/* Starting epoch */
double ep1;	/* Ending epoch */
double *ra;	/* RA in degrees mean equator & equinox of epoch ep0
		      mean equator & equinox of epoch ep1 (returned) */
double *dec;	/* Dec in degrees mean equator & equinox of epoch ep0
		       mean equator & equinox of epoch ep1 (returned) */
/*
**  Precession -  FK5 (Fricke, post-IAU2000)
**
**  This routine will not correctly convert between FK5 and FK4.
**  For output in FK4, precess to 2000.0 and use fk524() on result.
*/
{
    void fk5ep2j(), fk5j2ep();

    fk5ep2j (ep0, ra, dec);
    fk5j2ep (ep1, ra, dec);

    return;
}


void
mprecfk4 (bep0, bep1, rmatp)

double bep0;		/* Beginning Besselian epoch */
double bep1;		/* Ending Besselian epoch */
double rmatp[9];	/* 3x3 Precession matrix (returned) */

/*
**  Generate the matrix of precession between two epochs,
**  using the old, pre-IAU1976, Bessel-Newcomb model, using
**  Kinoshita's formulation (double precision)
**
**  The matrix is in the sense   v(bep1)  =  rmatp * v(bep0)
**
**  Reference:
**     Kinoshita, H. (1975) 'Formulas for precession', SAO Special
**     Report No. 364, Smithsonian Institution Astrophysical
**     Observatory, Cambridge, Massachusetts.
**
**  Based on slaPrebn() by P.T.Wallace   Starlink   30 October 1993
*/
{
    double bigt, t, tas2r, w, zeta, z, theta;
    void rotmat();
 
    /* Interval between basic epoch B1850.0 and beginning epoch in TC */
    bigt  = ( bep0 - 1850.0 ) / 100.0;
 
    /* Interval over which precession required, in tropical centuries */
    t = ( bep1 - bep0 ) / 100.0;
 
    /* Euler angles */
    tas2r = secrad (t);
    w = 2303.5548 + ( 1.39720 + 0.000059 * bigt ) * bigt;
    zeta = (w + ( 0.30242 - 0.000269 * bigt + 0.017996 * t ) * t ) * tas2r;
    z = (w + ( 1.09478 + 0.000387 * bigt + 0.018324 * t ) * t ) * tas2r;
    theta = ( 2005.1125 + ( - 0.85294 - 0.000365* bigt ) * bigt +
	    ( - 0.42647 - 0.000365 * bigt - 0.041802 * t ) * t ) * tas2r;
 
    /* Rotation matrix */
    rotmat (323, -zeta, theta, -z, rmatp);
    return;
}


void
mprecfk5 (ep0, ep1, rmatp)

double ep0;		/* Beginning epoch */
double ep1;		/* Ending epoch */
double rmatp[9];	/* 3x3 Precession matrix (returned) */

/*
**  Form the matrix of precession between two epochs (IAU 1976, FK5).
**  Notes:
**  1)  The epochs are TDB (loosely ET) Julian epochs.
**  2)  The matrix is in the sense   v(ep1)  =  rmatp * v(ep0) .
**
**  References:
**     Lieske,J.H., 1979. Astron. Astrophys.,73,282.
**          equations (6) & (7), p283.
**     Kaplan,G.H., 1981. USNO circular no. 163, pa2.
**
**  Based on slaPrec(), P.T.Wallace   Starlink   31 October 1993
*/
{
    double t0, t, tas2r, w, zeta, z, theta;
    void rotmat();

    /* Interval in Julian centuries  between J2000.0 and beginning epoch */
    t0 = ( ep0 - 2000.0 ) / 100.0;
 
    /* Interval over which precession required (JC) */
    t =  ( ep1 - ep0 ) / 100.0;
 
    /* Euler angles */
    tas2r = secrad (t);
    w = 2306.2181 + ( ( 1.39656 - ( 0.000139 * t0 ) ) * t0 );
    zeta = (w + ( ( 0.30188 - 0.000344 * t0 ) + 0.017998 * t ) * t ) * tas2r;
    z = (w + ( ( 1.09468 + 0.000066 * t0 ) + 0.018203 * t ) * t ) * tas2r;
    theta = ( ( 2004.3109 + ( - 0.85330 - 0.000217 * t0 ) * t0 )
	  + ( ( -0.42665 - 0.000217 * t0 ) - 0.041833 * t ) * t ) * tas2r;
 
    /* Rotation matrix */
    rotmat (323, -zeta, theta, -z, rmatp);
    return;
}


/* Precess coordinates to J2000 in FK5 */

void
fk5ep2j (ep, ra, dec)

double ep;	/* Starting Julian epoch */
double *ra;	/* RA in degrees mean equator & equinox of epoch ep0
		      mean equator & equinox of epoch ep1 (returned) */
double *dec;	/* Dec in degrees mean equator & equinox of epoch ep0
		       mean equator & equinox of epoch ep1 (returned) */
/*
**  Precession -  FK5 (Fricke, post-IAU2000)
**
**  This routine will not correctly convert between FK5 and FK4.
**  For output in FK4, precess to 2000.0 and use fk524() on result.
**
**  Based on slaPreces(), P.T.Wallace   Starlink   22 December 1993
*/
{
    int i, j;
    double pm[9], pmt[9], *pmi, v1[3], v2[3], rra, rdec, r;
    void v2s3(),s2v3(), mfk5j2ep();

    rra = degrad (*ra);
    rdec = degrad (*dec);
    r = 1.0;
 
    /* Convert RA,Dec to x,y,z */
    s2v3 (rra, rdec, r, v1);
 
    /* Generate and transpose precession matrix */
    mfk5j2ep ( ep, pm );
    for (i = 0; i < 3; i++) {
	for (j = 0; j < 3; j++) {
	    pmt[i+(j*3)] = pm[j+(i*3)];
	    }
	}
 
    /* Multiply position vector by precession matrix */
    pmi = pmt;
    for (i = 0; i < 3; i++) {
	v2[i] = 0;
	for (j = 0; j < 3; j++)
	    v2[i] = v2[i] + (*pmi++ * v1[j]);
	}
 
    /* Back to RA,Dec */
    v2s3 (v2, &rra, &rdec, &r);

    /* Convert from radians to degrees */
    *ra = raddeg (rra);
    *dec = raddeg (rdec);
}


/* Precess coordinates from J2000 in FK5 */

void
fk5j2ep (ep, ra, dec)

double ep;	/* Starting Julian epoch */
double *ra;	/* RA in degrees mean equator & equinox of epoch ep0
		      mean equator & equinox of epoch ep1 (returned) */
double *dec;	/* Dec in degrees mean equator & equinox of epoch ep0
		       mean equator & equinox of epoch ep1 (returned) */
/*
**  Precession -  FK5 (Fricke, post-IAU2000)
**
**  This routine will not correctly convert between FK5 and FK4.
**  For output in FK4, precess to 2000.0 and use fk524() on result.
**
**  Based on slaPreces(), P.T.Wallace   Starlink   22 December 1993
*/
{
    int i, j;
    double pm[9], *pmi, v1[3], v2[3], rra, rdec, r;
    void v2s3(), s2v3(), mfk5j2ep();

    rra = degrad (*ra);
    rdec = degrad (*dec);
    r = 1.0;
 
    /* Convert RA,Dec to x,y,z */
    s2v3 (rra, rdec, r, v1);
 
    /* Generate precession matrix */
    mfk5j2ep ( ep, pm );
 
    /* Multiply position vector by precession matrix */
    pmi = pm;
    for (i = 0; i < 3; i++) {
	v2[i] = 0;
	for (j = 0; j < 3; j++)
	    v2[i] = v2[i] + (*pmi++ * v1[j]);
	}
 
    /* Back to RA,Dec */
    v2s3 (v2, &rra, &rdec, &r);

    /* Convert from radians to degrees */
    *ra = raddeg (rra);
    *dec = raddeg (rdec);
}


void
mfk5j2ep (ep, rmatp)

double ep;		/* Beginning epoch */
double rmatp[9];	/* 3x3 Precession matrix (returned) */

/*
**  Form the matrix of precession between two epochs (IAU 2000, FK5).
**  Notes:
**  1)  The epochs are TDB (loosely ET) Julian epochs.
**  2)  The matrix is in the sense   v(ep)  =  rmatp * v(J2000) .
**
**  References:
**     Lieske,J.H., 1979. Astron. Astrophys.,73,282.
**          equations (6) & (7), p283.
**     Kaplan,G.H., 2005. USNO circular no. 179, p. 44.
**
**  Based on slaPrec(), P.T.Wallace   Starlink   31 October 1993
*/
{
    double t, t2, t3, t4, tas2r, zeta, z, theta;
    void rotmat();
 
    /* Interval in Julian centuries  between J2000.0 and beginning epoch */
    t = ( ep - 2000.0 ) / 100.0;
    t2 = t * t;
    t3 = t2 * t;
    t4 = t3 * t;
 
    /* Euler angles */
    tas2r = secrad (t);
    zeta = 2.65045 + (2306.083227 + (0.2988499 * t) + (0.01801828 * t2)
	  - (0.000005971 * t3) - (0.0000003173 * t4)) * tas2r;
    z = -2.650545 + (2306.077181 + (1.0927348 * t) + (0.01801828 * t2)
	   - (0.000005971 * t3) - (0.0000003173 * t4)) * tas2r;
    theta = (2004.191903 - (0.4294934 * t) - (0.04182264 * t2)
	    - (0.000007089 * t3) - (0.0000001274 * t4)) * tas2r;
 
    /* Rotation matrix */
    rotmat (323, -z, theta, -zeta, rmatp);
    return;
}


/* Make 3-D rotation matrix from up to three rotations */

void
rotmat (axes, rot1, rot2, rot3, matrix)

int axes;	/* Axes about which coordinates are rotated (1=x, 2=y, 3=z) */
double rot1;	/* First rotation in degrees */
double rot2;	/* Second rotation in degrees */
double rot3;	/* Third rotation in degrees */
double *matrix;	/* 3x3 rotation matrix (returned) */

{
    int i, j, k, naxis, iaxes, iaxis;
    double rot, srot, crot, *mati, w, wm[9], *wmi, matn[9];
    int axis[3];

    /* Initial final rotation matrix */
    mati = matrix;
    for (i = 0; i < 3; i++) {
	for (j=0; j < 3; j++) {
	    if (i == j)
		*mati++ = 1.0;
	    else
		*mati++ = 0.0;
	    }
	}

    /* Separate digits of rotation axis string and count rotations */
    naxis = 0;
    iaxes = axes;
    axis[0] = iaxes / 100;
    if (axis[0] > 0) {
	naxis++;
	iaxes = iaxes - (100 * axis[0]);
	}
    axis[naxis] = iaxes / 10;
    if (axis[naxis] > 0) {
	iaxes = iaxes - (10 * axis[naxis]);
	naxis++;
	}
    axis[naxis] = iaxes;
    if (axis[naxis] > 0)
	naxis++;

    /* For each digit of axis string, set up matrix */
    for (iaxis = 0; iaxis < naxis; iaxis++) {

	/* Initialize current rotation matrix */
	mati = matn;
	for (i = 0; i < 3; i++) {
	    for (j=0; j < 3; j++) {
		if (i == j)
		    *mati++ = 1.0;
		else
		    *mati++ = 0.0;
		}
	    }

	/* Select rotation angle from argument list */
	if (axis[iaxis] == 1)
	    rot = rot1;
	else if (axis[iaxis] == 2)
	    rot = rot2;
	else
	    rot = rot3;
	srot = sin (rot);
	crot = cos (rot);
	
	/* Matrix for rotation in X */
	if (axis[iaxis] == 1) {
	    matn[4] = crot;
	    matn[5] = srot;
	    matn[7] = -srot;
	    matn[8] = crot;
	    }
	
	/* Matrix for rotation in Y */
	else if (axis[iaxis] == 2) {
	    matn[0] = crot;
	    matn[2] = -srot;
	    matn[6] = srot;
	    matn[8] = crot;
	    }
	
	/* Matrix for rotation in Z */
	else {
	    matn[0] = crot;
	    matn[1] = srot;
	    matn[3] = -srot;
	    matn[4] = crot;
	    }

	/* Multiply existing rotation matrix by new rotation matrix */
	for (i = 0; i < 3; i++) {
	    for (j = 0; j < 3; j++) {
		w = 0.0;
		for (k = 0; k < 3; k++)
		    w+= matn[3*i + k] * matrix[3*k + j];
		wm[3*i + j] = w;
		}
	    }

	/* Update output matrix */
	mati = matrix;
	wmi = wm;
	for (i = 0; i < 9; i++) {
	    *mati++ = *wmi++;
	    }
	}
    return;
}


/* The following routines are from Doug Mink's Fortran ephemeris library */

/* Convert right ascension, declination, and distance to
   geocentric equatorial rectangular coordinates */

void
s2v3 (rra,rdec,r,pos)

double rra;	/* Right ascension in radians */
double rdec;	/* Declination in radians */
double r;	/* Distance to object in same units as pos */
double pos[3];	/* x,y,z geocentric equatorial position of object (returned) */
{
    pos[0] = r * cos (rra) * cos (rdec);
    pos[1] = r * sin (rra) * cos (rdec);
    pos[2] = r * sin (rdec);

    return;
}

/* Convert geocentric equatorial rectangular coordinates to
   right ascension, declination, and distance */

void
v2s3 (pos,rra,rdec,r)

double pos[3];	/* x,y,z geocentric equatorial position of object */
double *rra;	/* Right ascension in radians (returned) */
double *rdec;	/* Declination in radians (returned) */
double *r;	/* Distance to object in same units as pos (returned) */

{
    double x,y,z,rxy,rxy2,z2;

    x = pos[0];
    y = pos[1];
    z = pos[2];

    *rra = atan2 (y, x);

    /* Keep RA within 0 to 2pi range */
    if (*rra < 0.0)
	*rra = *rra + (2.0 * PI);
    if (*rra > 2.0 * PI)
	*rra = *rra - (2.0 * PI);

    rxy2 = x*x + y*y;
    rxy = sqrt (rxy2);
    *rdec = atan2 (z, rxy);

    z2 = z * z;
    *r = sqrt (rxy2 + z2);

    return;
}

/*
 * Nov  6 1995	Include stdlib.h instead of malloc.h
 * Apr  1 1996	Add arbitrary epoch precession
 * Apr 26 1996	Add FK4 <-> FK5 subroutines for use when epoch is known
 * Aug  6 1996	Clean up after lint
 * Nov  4 1996	Break SLA subroutines into separate file slasubs.c
 * Dec  9 1996	Change arguments to degrees in FK4 and FK5 precession programs
 * Dec 10 1996	All subroutine arguments are degrees except vector conversions
 *
 * Mar 20 1997	Drop unused variables after lint
 *
 * Apr 14 1998	Add ecliptic coordinate conversions and general conversion routines
 * Apr 23 1998	Add LINEAR coordinate system
 * Apr 28 1998	Change coordinate system flags to WCS_*
 * Apr 28 1998	Return -1 from wcscsys if not a legal coordinate system
 * May  7 1998	Keep theta within 0 to 2pi in ecl2fk5()
 * May 13 1998	Add wcsceq()
 * May 13 1998	Add equinox arguments to wcscon()
 * Jun 24 1998	Set J2000 from ICRS in wcscsys()
 * Jul  9 1998	Include stdio.h for fprintf() and sprintf() declarations
 * Sep 17 1998	Add wcscstr() to get coordinate string
 * Sep 21 1998	Fix bug in wcscstr() which returned B2000 instead of J2000
 * Sep 21 1998	Add subroutine to convert proper motions, too.
 * Oct 21 1998	In wcscstr(), drop .00 from returned string
 * Nov 18 1998	Rename jpcop() v2s3() and jpcon() s2v3() (spherical to vector)
 * Dec  2 1998	Add PLANET coordinate system to wcscsys() and wcscstr()
 *
 * Mar 10 2000	Precess coordinates correctly from other than 1950.0 and 2000.0
 * Mar 10 2000	Set coordinate system to J2000 or B1950 if string is numeric
 * Mar 14 2000	Clean up code in fk524m() and fk425m()
 * May 31 2000	Add proper motion correctly if proper motion precessed
 * Jun 26 2000	Add some support for WCS_XY image coordinates
 * Sep 14 2000	Return -1 from wcscsys if equinox is less than 1900.0
 * Oct 31 2000	Add proper motion after fk425 or fk524 from system epoch
 * Oct 31 2000	Fix proper motion units in fk524p() and fk425p()
 * Nov  6 2000	Update fk425 and fk524 algorithms to include parallax and rv
 *
 * Jan 11 2001	Print all messages to stderr
 * Mar 21 2001	Move braces around bgal[] and jgal[] matrix initialization
 *
 * Feb 13 2002	Fix precession units problem in ecl2fk5() and fk52ecl()
 *
 * Apr 13 2005	Replace all sla_lib calls with local code
 * Nov  1 2005	Add WCS_ICRS, and unprecessable system
 *
 * Jan  5 2006	Fix bugs in precession subroutines mprecxxx()
 * May  3 2006	Drop declarations of unused variables suggested by Robert Lupton
 * Oct  6 2006	If pixel coordinates, set system to WCS_XY in wcscsys()
 * Oct 30 2006	Add LINEAR and ICRS to wcscstr() returns
 * Nov 29 2006	Rewrite FK5 precession subroutines to IAU 2000 standard
 */
