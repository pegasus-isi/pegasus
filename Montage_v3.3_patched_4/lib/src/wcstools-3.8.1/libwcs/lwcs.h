/*** File lwcs.h
 *** April 25, 2006
 *** By Doug Mink, dmink@cfa.harvard.edu
 *** Harvard-Smithsonian Center for Astrophysics
 *** Copyright (C) 1999-2006
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
 */

/* The following are used in star finding (findstar.c) */
#define	NSTATPIX	25	/* Stats are computed for +- this many pixels */
#define	ISTATPIX	10	/* Stats are computed every this many pixels */
#define	MAXWALK		20	/* Farthest distance to walk from seed */
#define	BURNEDOUT	0	/* Clamp pixels brighter than this, if > 0 */
#define NITERATE	5	/* Number of iterations for sigma clipping */
#define RNOISE	 	50	/* Mean noise is from center +- this many pixels */

#define STARSIGMA	5.0	/* Stars must be this many sigmas above mean */
#define BORDER		10	/* Ignore this much of the edge */
#define MAXRAD		20	/* Maximum radius for a star */
#define MINRAD		1	/* Minimum radius for a star */
#define MINPEAK		10	/* Minimum peak for a star */
#define MINSEP		10	/* Minimum separations for stars */

/* The following are used in star matching (matchstar.c) */
#define	FTOL	0.0000001	/* Fractional change of chisqr() to be done */
#define NMAX		3000	/* Maximum number of minimization iterations */
#define	NPEAKS		20	/* Binning peak history */
#define MINMATCH	50	/* Stars to match to drop out of loop */

/* The following are used in world coordinate system fitting (imsetwcs.c) */
#define MINSTARS	3	/* Minimum stars from reference and image */
#define MAXSTARS	50	/* Default max star pairs to try matching */
#define MAGLIM1		0.0	/* Faintest reference catalog magnitude to use*/
#define MAGLIM2		0.0	/* Faintest reference catalog magnitude to use*/
#define PIXDIFF		10	/* +- this many pixels is a match */
#define PSCALE		0	/* Plate scale in arcsec/pixel */
				/* (if nonzero, this overrides image header) */
#define NXYDEC		2	/* Number of decimal places in image coords */

#define MAXCAT		100	/* Max reference stars to keep in scat or imcat */

/* Jun 11 1999	Set BURNEDOUT to 0 so it is ignored
 *
 * Feb 15 2000	Drop MAXREF; add MAXCAT for imcat and scat; MAXSTARS from 25 to 50
 *
 * Oct 31 2001	Add MINMATCH and set default value to 50
 *
 * Mar 30 2006	Add NXYDEC and set default to 2 (constant value was 1)
 * Apr 25 2006	Add RNOISE and set default to previous constant value of 50
 */
