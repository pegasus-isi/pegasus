/*** File libwcs/catutil.c
 *** November 13, 2009
 *** By Doug Mink, dmink@cfa.harvard.edu
 *** Harvard-Smithsonian Center for Astrophysics
 *** Copyright (C) 1998-2009
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

/* int RefCat (refcatname,title,syscat,eqcat,epcat, catprop, nmag)
 *	Return catalog type code, title, coord. system, proper motion, num mags
 * char *CatName (refcat, refcatname)
 *	Return catalog name given catalog type code
 * char *CatSource (refcat, refcatname)
 *	Return name for catalog sources given catalog type code
 * void CatID (catid, refcat)
 *	Return ID column heading for the given catalog
 * double CatRad (refcat)
 *	Return default search radius for the given catalog
 * char *ProgCat (progname)
 *	Return catalog name from program name, NULL if none there
 * char *ProgName (progpath0)
 *	Return program name from pathname by which program is invoked
 * void CatNum (refcat, nndec, dnum, numstr)
 *	Return formatted source number
 * void SearchLim (cra, cdec, dra, ddec, sys, ra1, ra2, dec1, dec2, verbose)
 *	Compute limiting RA and Dec from center and half-widths
 * int CatNumLen (refcat, nndec)
 *	Return length of source number
 * int CatNdec (refcat)
 *	Return number of decimal places in source number, if known
 * void CatMagName (imag, refcat, magname)
 *	Returns name of specified magnitude
 * int CatMagNum (imag, refcat)
 *	Returns number of magnitude specified by letter as int
 * int StrNdec (string)
 *	Returns number of decimal places in a numeric string (-1=not number)
 * int NumNdec (number)
 *	Returns number of decimal places in a number
 * char *DateString (dateform,epoch)
 *	Return string with epoch of position in desired format
 * void RefLim (cra,cdec,dra,ddec,sysc,sysr,eqc,eqr,epc,ramin,ramax,decmin,decmax,verbose)
 *	Compute limiting RA and Dec in new system from center and half-widths
 * struct Range *RangeInit (string, ndef)
 *	Return structure containing ranges of numbers
 * int isrange (string)
 *	Return 1 if string is a range, else 0
 * int rstart (range)
 *	Restart at beginning of range
 * int rgetn (range)
 *	Return number of values from range structure
 * int rgeti4 (range)
 *	Return next number from range structure as 4-byte integer
 * int rgetr8 (range)
 *	Return next number from range structure as 8-byte floating point number
 * int ageti4 (string, keyword, ival)
 *	Read int value from a file where keyword=value, anywhere on a line
 * int agetr8 (string, keyword, dval)
 *	Read double value from a file where keyword=value, anywhere on a line
 * int agets (string, keyword, lval, value)
 *	Read value from a file where keyword=value, anywhere on a line
 * void bv2sp (bv, b, v, isp)
 *	approximate spectral type given B - V or B and V magnitudes
 * void br2sp (br, b, r, isp)
 *	approximate spectral type given B - R or B and R magnitudes
 * void vothead (refcat, refcatname, mprop, typecol)
 *	Print heading for VOTable format catalog search return
 * void vottail ()
 *	Print end of VOTable format catalog search return
 * void	setrevmsg (revmessage)
 *	Set version/date message for nstarmax=-1 returns from *read subroutines
 * char	*getrevmsg ()
 *	Return version/date message for nstarmax=-1 returns from *read subroutines
 * int	*is2massid (string)
 *	Return 1 if string is 2MASS ID, else 0
 * void polfit (x, y, x0, npts, nterms, a, stdev)
 *	Polynomial least squares fitting program
 *double polcomp (xi, x0, norder, a)
 *	Polynomial evaluation Y = A(1) + A(2)*X + A(3)*X^2 + A(3)*X^3 + ...
 * void moveb (source, dest, nbytes, offs, offd)
 *	Copy nbytes bytes from source+offs to dest+offd (any data type)
 */

#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <math.h>
#include "wcs.h"
#include "fitsfile.h"
#include "wcscat.h"

static char *revmessage = NULL;	/* Version and date for calling program */
static char *revmsg0 = "";
void
setrevmsg (revmsg)		/* Set version and date string*/
char *revmsg;
{ revmessage = revmsg; return; }
char *
getrevmsg ()			/* Return version and date string */
{ if (revmessage == NULL) return (revmsg0);
  else return (revmessage); }

static int degout = 0;	/* Set to 1 to print coordinates in degrees */
void
setlimdeg (degoutx)		/* Set degree output flag */
int degoutx;
{ degout = degoutx; return; }


/* Return code for reference catalog or its type */

int
RefCat (refcatname, title, syscat, eqcat, epcat, catprop, nmag)

char	*refcatname;	/* Name of reference catalog */
char	*title;		/* Description of catalog (returned) */
int	*syscat;	/* Catalog coordinate system (returned) */
double	*eqcat;		/* Equinox of catalog (returned) */
double	*epcat;		/* Epoch of catalog (returned) */
int	*catprop;	/* 1 if proper motion in catalog (returned) */
int	*nmag;		/* Number of magnitudes in catalog (returned) */
{
    struct StarCat *starcat;
    int refcat, nbuff;

    *catprop = 0;

    refcat = CatCode (refcatname);
    if (refcat == GSCACT) {
	strcpy (title, "HST Guide Stars/ACT");
	*syscat = WCS_J2000;
	*eqcat = 2000.0;
	*epcat = 2000.0;
	*nmag = 1;
	*catprop = 0;
	}
    else if (refcat == GSC2) {
	if (strsrch (refcatname, "22")) {
	    strcpy (title, "GSC 2.2 Sources");
	    *catprop = 0;
	    }
	else if (strsrch (refcatname, "23")) {
	    strcpy (title, "GSC 2.3 Sources");
	    *catprop = 1;
	    }
	else {
	    strcpy (title, "GSC 2.3 Sources");
	    *catprop = 0;
	    }
	*syscat = WCS_J2000;
	*eqcat = 2000.0;
	*epcat = 2000.0;
	*nmag = 5;
	refcat = GSC2;
	}
    else if (refcat == GSC) {
	strcpy (title, "HST Guide Stars");
	*syscat = WCS_J2000;
	*eqcat = 2000.0;
	*epcat = 2000.0;
	*catprop = 0;
	*nmag = 1;
	}
    else if (refcat == SDSS) {
	strcpy (title, "SDSS Sources");
	*syscat = WCS_J2000;
	*eqcat = 2000.0;
	*epcat = 2000.0;
	*catprop = 0;
	*nmag = 5;
	}
    else if (refcat == SKYBOT) {
	strcpy (title, "SkyBot Sources");
	*syscat = WCS_J2000;
	*eqcat = 2000.0;
	*epcat = 2000.0;
	*catprop = 1;
	*nmag = 3;
	}
    else if (refcat == UB1) {
	strcpy (title, "USNO-B1.0 Sources");
	*syscat = WCS_J2000;
	*eqcat = 2000.0;
	*epcat = 2000.0;
	*catprop = 1;
	*nmag = 5;
	refcat = UB1;
	}
    else if (refcat == YB6) {
	strcpy (title, "USNO-YB6 Sources");
	*syscat = WCS_J2000;
	*eqcat = 2000.0;
	*epcat = 2000.0;
	*catprop = 1;
	*nmag = 5;
	refcat = YB6;
	}
    else if (refcat == USA1 || refcat == USA2 || refcat == USAC) {
	*syscat = WCS_J2000;
	*eqcat = 2000.0;
	*epcat = 2000.0;
	*nmag = 2;
	*catprop = 0;
	if (strchr (refcatname, '1') != NULL)
	    strcpy (title, "USNO SA-1.0 Catalog Stars");
	else if (strchr (refcatname, '2') != NULL)
	    strcpy (title, "USNO SA-2.0 Catalog Stars");
	else
	    strcpy (title, "USNO SA Catalog Stars");
	}
    else if (refcat == USNO) {
	*syscat = WCS_J2000;
	*eqcat = 2000.0;
	*epcat = 2000.0;
	*catprop = 0;
	*nmag = 1;
	sprintf (title, "USNO %s Stars", refcatname);
	}
    else if (refcat == UA1 || refcat == UA2 || refcat == UAC) {
	*syscat = WCS_J2000;
	*eqcat = 2000.0;
	*epcat = 2000.0;
	*catprop = 0;
	*nmag = 2;
	if (strchr (refcatname, '1') != NULL)
	    strcpy (title, "USNO A-1.0 Sources");
	else if (strchr (refcatname, '2') != NULL)
	    strcpy (title, "USNO A-2.0 Sources");
	else
	    strcpy (title, "USNO A Sources");
	}
    else if (refcat == UCAC1) {
	strcpy (title, "USNO UCAC1 Catalog Stars");
	*syscat = WCS_J2000;
	*eqcat = 2000.0;
	*epcat = 2000.0;
	*catprop = 1;
	*nmag = 1;
	}
    else if (refcat == UCAC2) {
	strcpy (title, "USNO UCAC2 Catalog Stars");
	*syscat = WCS_J2000;
	*eqcat = 2000.0;
	*epcat = 2000.0;
	*catprop = 1;
	*nmag = 4;
	}
    else if (refcat == UCAC3) {
	strcpy (title, "USNO UCAC3 Catalog Stars");
	*syscat = WCS_J2000;
	*eqcat = 2000.0;
	*epcat = 2000.0;
	*catprop = 1;
	*nmag = 8;
	}
    else if (refcat == UJC) {
	strcpy (title, "USNO J Catalog Stars");
	*syscat = WCS_J2000;
	*eqcat = 2000.0;
	*epcat = 2000.0;
	*catprop = 0;
	*nmag = 1;
	}
    else if (refcat == SAO) {
	strcpy (title, "SAO Catalog Stars");
	starcat = binopen ("SAO");
	if (starcat == NULL)
	    starcat = binopen ("SAOra");
	if (starcat) {
	    *syscat = starcat->coorsys;
	    *eqcat = starcat->equinox;
	    *epcat = starcat->epoch;
	    *catprop = starcat->mprop;
	    *nmag = 1;
	    binclose (starcat);
	    }
	}
    else if (refcat == PPM) {
	strcpy (title, "PPM Catalog Stars");
	starcat = binopen ("PPM");
	if (starcat == NULL)
	    starcat = binopen ("PPMra");
	if (starcat) {
	    *syscat = starcat->coorsys;
	    *eqcat = starcat->equinox;
	    *epcat = starcat->epoch;
	    *catprop = starcat->mprop;
	    *nmag = 1;
	    binclose (starcat);
	    }
	}
    else if (refcat == IRAS) {
	strcpy (title, "IRAS Point Sources");
	if ((starcat = binopen ("IRAS"))) {
	    *syscat = starcat->coorsys;
	    *eqcat = starcat->equinox;
	    *epcat = starcat->epoch;
	    *nmag = 1;
	    *catprop = starcat->mprop;
	    binclose (starcat);
	    }
	}
    else if (refcat == SKY2K) {
	strcpy (title, "SKY2000 Master Catalog Stars");
	starcat = binopen ("sky2k");
	if (starcat == NULL)
	    starcat = binopen ("sky2kra");
	if (starcat) {
	    *syscat = starcat->coorsys;
	    *eqcat = starcat->equinox;
	    *epcat = starcat->epoch;
	    *catprop = starcat->mprop;
	    *nmag = 4;
	    binclose (starcat);
	    }
	}
    else if (refcat == TYCHO2) {
	strcpy (title, "Tycho 2 Catalog Stars");
	*syscat = WCS_J2000;
	*eqcat = 2000.0;
	*epcat = 2000.0;
	*catprop = 1;
	*nmag = 2;
	}
    else if (refcat == TYCHO2E) {
	strcpy (title, "Tycho 2 Catalog Stars");
	*syscat = WCS_J2000;
	*eqcat = 2000.0;
	*epcat = 2000.0;
	*catprop = 1;
	*nmag = 4;
	}
    else if (refcat == TYCHO) {
	strcpy (title, "Tycho Catalog Stars");
	if ((starcat = binopen ("tycho"))) {
	    *syscat = starcat->coorsys;
	    *eqcat = starcat->equinox;
	    *epcat = starcat->epoch;
	    *catprop = 1;
	    *nmag = 2;
	    binclose (starcat);
	    }
	}
    else if (refcat == HIP) {
	strcpy (title, "Hipparcos Catalog Stars");
	if ((starcat = binopen ("hipparcos"))) {
	    *syscat = starcat->coorsys;
	    *eqcat = starcat->equinox;
	    *epcat = starcat->epoch;
	    *catprop = starcat->mprop;
	    *nmag = 1;
	    binclose (starcat);
	    }
	}
    else if (refcat == ACT) {
	strcpy (title, "ACT Catalog Stars");
	*syscat = WCS_J2000;
	*eqcat = 2000.0;
	*epcat = 2000.0;
	*catprop = 1;
	*nmag = 2;
	}
    else if (refcat == BSC) {
	strcpy (title, "Bright Star Catalog Stars");
	if ((starcat = binopen ("BSC5"))) {
	    *syscat = starcat->coorsys;
	    *eqcat = starcat->equinox;
	    *epcat = starcat->epoch;
	    *catprop = starcat->mprop;
	    *nmag = 1;
	    binclose (starcat);
	    }
	}
    else if (refcat == TMPSC || refcat == TMIDR2) {
	strcpy (title, "2MASS Point Sources");
	*syscat = WCS_J2000;
	*eqcat = 2000.0;
	*epcat = 2000.0;
	*catprop = 0;
	*nmag = 3;
	}
    else if (refcat == TMPSCE) {
	strcpy (title, "2MASS Point Sources");
	*syscat = WCS_J2000;
	*eqcat = 2000.0;
	*epcat = 2000.0;
	*catprop = 0;
	*nmag = 6;
	}
    else if (refcat == TMXSC) {
	strcpy (title, "2MASS Extended Sources");
	*syscat = WCS_J2000;
	*eqcat = 2000.0;
	*epcat = 2000.0;
	*catprop = 0;
	*nmag = 3;
	}
    else if (refcat == USNO) {
	*syscat = WCS_J2000;
	*eqcat = 2000.0;
	*epcat = 2000.0;
	*catprop = 0;
	*nmag = 1;
	sprintf (title, "USNO %s Stars", refcatname);
	}
    else if (refcat == BINCAT) {
	strcpy (title, refcatname);
	strcat (title, " Catalog Sources");
	if ((starcat = binopen (refcatname))) {
	    *syscat = starcat->coorsys;
	    *eqcat = starcat->equinox;
	    *epcat = starcat->epoch;
	    *catprop = starcat->mprop;
	    *nmag = starcat->nmag;
	    binclose (starcat);
	    }
	}
    else if (refcat == TABCAT) {
	strcpy (title, refcatname);
	strcat (title, " Catalog Sources");
	if (strchr (refcatname, ','))
	    nbuff = 0;
	else
	    nbuff = 1000;
	if ((starcat = tabcatopen (refcatname, NULL, nbuff))) {
	    *syscat = starcat->coorsys;
	    *eqcat = starcat->equinox;
	    *epcat = starcat->epoch;
	    *catprop = starcat->mprop;
	    *nmag = starcat->nmag;
	    ctgclose (starcat);
	    }
	}
    else if (refcat != 0) {
	strcpy (title, refcatname);
	strcat (title, " Catalog Sources");
	if ((starcat = ctgopen (refcatname, TXTCAT))) {
	    *syscat = starcat->coorsys;
	    *eqcat = starcat->equinox;
	    *epcat = starcat->epoch;
	    *catprop = starcat->mprop;
	    *nmag = starcat->nmag;
	    ctgclose (starcat);
	    }
	}
    return refcat;
}


/* Return code for reference catalog or its type */

int
CatCode (refcatname)

char	*refcatname;	/* Name of reference catalog */
{
    struct StarCat *starcat;
    int refcat, nbuff;

    refcat = 0;
    if (refcatname == NULL)
	refcat = 0;
    else if (strlen (refcatname) < 1)
	refcat = 0;
    else if (strncasecmp(refcatname,"gsca",4)==0 &&
	strcsrch(refcatname, ".tab") == NULL)
	refcat = GSCACT;
    else if (strncasecmp(refcatname,"gsc2",4)==0 &&
	     strcsrch(refcatname, ".tab") == NULL)
	refcat = GSC2;
    else if (strncasecmp(refcatname,"sdss",4)==0 &&
	     strcsrch(refcatname, ".tab") == NULL)
	refcat = SDSS;
    else if (strncasecmp(refcatname,"skyb",4)==0 &&
	     strcsrch(refcatname, ".tab") == NULL)
	refcat = SKYBOT;
    else if (strncasecmp(refcatname,"gs",2)==0 &&
	     strcsrch(refcatname, ".tab") == NULL)
	refcat = GSC;
    else if (strncasecmp(refcatname,"ub",2)==0 &&
	     strcsrch(refcatname, ".tab") == NULL)
	refcat = UB1;
    else if (strncasecmp(refcatname,"ucac1",5)==0 &&
	     strcsrch(refcatname, ".tab") == NULL)
	refcat = UCAC1;
    else if (strncasecmp(refcatname,"ucac2",5)==0 &&
	     strcsrch(refcatname, ".tab") == NULL)
	refcat = UCAC2;
    else if (strncasecmp(refcatname,"ucac3",5)==0 &&
	     strcsrch(refcatname, ".tab") == NULL)
	refcat = UCAC3;
    else if (strncasecmp(refcatname,"usa",3)==0 &&
	     strcsrch(refcatname, ".tab") == NULL) {
	if (strchr (refcatname, '1') != NULL)
	    refcat = USA1;
	else if (strchr (refcatname, '2') != NULL)
	    refcat = USA2;
	else
	    refcat = USAC;
	}
    else if (strncmp (refcatname, ".usnop", 6) == 0)
	refcat = USNO;
    else if (strncasecmp(refcatname,"ua",2)==0 &&
	     strcsrch(refcatname, ".tab") == NULL) {
	if (strchr (refcatname, '1') != NULL)
	    refcat = UA1;
	else if (strchr (refcatname, '2') != NULL)
	    refcat = UA2;
	else
	    refcat = UAC;
	}
    else if (strncasecmp(refcatname,"uj",2)==0 &&
	     strcsrch(refcatname, ".tab") == NULL)
	refcat = UJC;
    else if (strncasecmp(refcatname,"yb6",3)==0 &&
	     strcsrch(refcatname, ".tab") == NULL)
	refcat = YB6;
    else if (strncasecmp(refcatname,"sky2k",5)==0 &&
	     strcsrch(refcatname, ".tab") == NULL)
	refcat = SKY2K;
    else if (strncasecmp(refcatname,"sao",3)==0 &&
	     strcsrch(refcatname, ".tab") == NULL) {
	starcat = binopen ("SAO");
	if (starcat == NULL)
	    starcat = binopen ("SAOra");
	if (starcat) {
	    binclose (starcat);
	    refcat = SAO;
	    }
	}
    else if (strncasecmp(refcatname,"ppm",3)==0 &&
	     strcsrch(refcatname, ".tab") == NULL) {
	starcat = binopen ("PPM");
	if (starcat == NULL)
	    starcat = binopen ("PPMra");
	if (starcat) {
	    binclose (starcat);
	    refcat = PPM;
	    }
	}
    else if (strncasecmp(refcatname,"iras",4)==0 &&
	     strcsrch(refcatname, ".tab") == NULL) {
	if ((starcat = binopen ("IRAS"))) {
	    binclose (starcat);
	    refcat = IRAS;
	    }
	}
    else if (strncasecmp(refcatname,"ty",2)==0 &&
	     strcsrch(refcatname, ".tab") == NULL) {
	if (strcsrch (refcatname, "2e") != NULL) {
	    refcat = TYCHO2E;
	    }
	else if (strsrch (refcatname, "2") != NULL) {
	    refcat = TYCHO2;
	    }
	else {
	    if ((starcat = binopen ("tycho"))) {
		binclose (starcat);
		refcat = TYCHO;
		}
	    }
	}
    else if (strncasecmp(refcatname,"hip",3)==0 &&
	      strcsrch(refcatname, ".tab") == NULL) {
	if ((starcat = binopen ("hipparcos"))) {
	    binclose (starcat);
	    refcat = HIP;
	    }
	}
    else if (strncasecmp(refcatname,"act",3)==0 &&
	     strcsrch(refcatname, ".tab") == NULL)
	refcat = ACT;
    else if (strncasecmp(refcatname,"bsc",3)==0 &&
	     strcsrch(refcatname, ".tab") == NULL) {
	if ((starcat = binopen ("BSC5"))) {
	    binclose (starcat);
	    refcat = BSC;
	    }
	}
    else if (strcsrch (refcatname,"idr2") &&
	     strcsrch (refcatname, ".tab") == NULL) {
	refcat = TMIDR2;
	}
    else if ((strncasecmp(refcatname,"2mp",3)==0 ||
	     strncasecmp(refcatname,"2mc",3)==0 ||
	     strncasecmp(refcatname,"tmp",3)==0 ||
	     strncasecmp(refcatname,"tmc",3)==0) &&
	     strcsrch(refcatname, ".tab") == NULL) {
	if (strcsrch (refcatname, "ce"))
	    refcat = TMPSCE;
	else
	    refcat = TMPSC;
	}
    else if ((strncasecmp(refcatname,"2mx",3)==0 ||
	     strncasecmp(refcatname,"tmx",3)==0) &&
	     strcsrch(refcatname, ".tab") == NULL) {
	refcat = TMXSC;
	}
    else if (strcsrch (refcatname, ".usno")) {
	refcat = USNO;
	}
    else if (isbin (refcatname)) {
	if ((starcat = binopen (refcatname))) {
	    binclose (starcat);
	    refcat = BINCAT;
	    }
	else
	    refcat = 0;
	}
    else if (istab (refcatname)) {
	if (strchr (refcatname, ','))
	    nbuff = 0;
	else
	    nbuff = 1000;
	if ((starcat = tabcatopen (refcatname, NULL, nbuff))) {
	    ctgclose (starcat);
	    refcat = TABCAT;
	    }
	else
	    refcat = 0;
	}
    else if (refcatname != NULL) {
	if ((starcat = ctgopen (refcatname, TXTCAT))) {
	    ctgclose (starcat);
	    refcat = TXTCAT;
	    }
	else
	    refcat = 0;
	}
    else
	refcat = 0;
    return refcat;
}


char *
CatName (refcat, refcatname)

int	refcat;		/* Catalog code */
char	*refcatname;	/* Catalog file name */
{
    char *catname;

    if (refcat < 1 || refcat > NUMCAT)
	return (refcatname);

    /* Allocate string in which to return a catalog name */
    catname = (char *)calloc (16, 1);

    if (refcat ==  GSC)		/* HST Guide Star Catalog */
	strcpy (catname, "GSC");
    else if (refcat ==  GSCACT)	/* HST GSC revised with ACT */
	strcpy (catname, "GSC-ACT");
    else if (refcat ==  GSC2) {	/* GSC II */
	if (strsrch (refcatname, "22")) {
	    strcpy (catname, "GSC 2.2");
	    }
	else {
	    strcpy (catname, "GSC 2.3");
	    }
	}
    else if (refcat == YB6)	/* USNO YB6 Star Catalog */
	strcpy (catname, "USNO-YB6");
    else if (refcat ==  UJC)	/* USNO UJ Star Catalog */
	strcpy (catname, "UJC");
    else if (refcat ==  UAC)	/* USNO A Star Catalog */
	strcpy (catname, "USNO-A2.0");
    else if (refcat ==  USAC)	/* USNO SA Star Catalog */
	strcpy (catname, "USNO-SA2.0");
    else if (refcat ==  SAO)	/* SAO Star Catalog */
	strcpy (catname, "SAO");
    else if (refcat ==  IRAS)	/* IRAS Point Source Catalog */
	strcpy (catname, "IRAS PSC");
    else if (refcat ==  SDSS)	/* Sloan Digital Sky Survey */
	strcpy (catname, "SDSS");
    else if (refcat ==  PPM)	/* PPM Star Catalog */
	strcpy (catname, "PPM");
    else if (refcat ==  TYCHO)	/* Tycho Star Catalog */
	strcpy (catname, "TYCHO");
    else if (refcat ==  UA1)	/* USNO A-1.0 Star Catalog */
	strcpy (catname, "USNO-A1.0");
    else if (refcat ==  UB1)	/* USNO B-1.0 Star Catalog */
	strcpy (catname, "USNO-B1.0");
    else if (refcat ==  UCAC1)	/* USNO UCAC1 Star Catalog */
	strcpy (catname, "USNO-UCAC1");
    else if (refcat ==  UCAC2)	/* USNO UCAC2 Star Catalog */
	strcpy (catname, "USNO-UCAC2");
    else if (refcat ==  UCAC3)	/* USNO UCAC3 Star Catalog */
	strcpy (catname, "USNO-UCAC3");
    else if (refcat ==  UA2)	/* USNO A-2.0 Star Catalog */
	strcpy (catname, "USNO-A2.0");
    else if (refcat ==  USA1)	/* USNO SA-1.0 Star Catalog */
	strcpy (catname, "USNO-SA1.0");
    else if (refcat ==  USA2)	/* USNO SA-2.0 Star Catalog */
	strcpy (catname, "USNO-SA2.0");
    else if (refcat ==  HIP)	/* Hipparcos Star Catalog */
	strcpy (catname, "Hipparcos");
    else if (refcat ==  ACT)	/* USNO ACT Star Catalog */
	strcpy (catname, "ACT");
    else if (refcat ==  BSC)	/* Yale Bright Star Catalog */
	strcpy (catname, "BSC");
    else if (refcat ==  TYCHO2 ||
	     refcat == TYCHO2E)	/* Tycho-2 Star Catalog */
	strcpy (catname, "TYCHO-2");
    else if (refcat ==  TMPSC ||
	     refcat == TMPSCE)	/* 2MASS Point Source Catalog */
	strcpy (catname, "2MASS PSC");
    else if (refcat ==  TMXSC)	/* 2MASS Extended Source Catalog */
	strcpy (catname, "2MASS XSC");
    else if (refcat ==  TMIDR2)	/* 2MASS Point Source Catalog */
	strcpy (catname, "2MASS PSC IDR2");
    else if (refcat ==  SKY2K)	/* SKY2000 Master Catalog */
	strcpy (catname, "SKY2000");
    else if (refcat ==  SKYBOT)	/* SkyBot Solar System Objects */
	strcpy (catname, "SkyBot");
    return (catname);
}


char *
CatSource (refcat, refcatname)

int	refcat;		/* Catalog code */
char	*refcatname;	/* Catalog file name */
{
    char *catname;
    int lname;

    if (refcat < 1 || refcat > NUMCAT) {
	if (refcatname == NULL)
	    lname = 0;
	else
	    lname = strlen (refcatname);
	catname = (char *)calloc (lname + 16, 1);
	if (lname > 0)
	    sprintf (catname, "%s sources", refcatname);
	else
	    sprintf (catname, "catalog sources");
	return (catname);
	}

    /* Allocate string in which to return a catalog name */
    catname = (char *)calloc (64, 1);

    if (refcat ==  GSC)		/* HST Guide Star Catalog */
	strcpy (catname, "HST Guide Stars");
    else if (refcat ==  GSCACT)	/* HST GSC revised with ACT */
	strcpy (catname, "GSC-ACT Stars");
    else if (refcat ==  GSC2) {	/* GSC II */
	if (strsrch (refcatname, "22")) {
	    strcpy (catname, "GSC 2.2 Stars");
	    }
	else {
	    strcpy (catname, "GSC 2.3 Stars");
	    }
	}
    else if (refcat == YB6)	/* USNO YB6 Star Catalog */
	strcpy (catname, "USNO-YB6 Stars");
    else if (refcat ==  UJC)	/* USNO UJ Star Catalog */
	strcpy (catname, "USNO J Catalog Stars");
    else if (refcat ==  UAC)	/* USNO A Star Catalog */
	strcpy (catname, "USNO-A2.0 Stars");
    else if (refcat ==  USAC)	/* USNO SA Star Catalog */
	strcpy (catname, "USNO-SA2.0 Stars");
    else if (refcat ==  SAO)	/* SAO Star Catalog */
	strcpy (catname, "SAO Catalog Stars");
    else if (refcat ==  IRAS)	/* IRAS Point Source Catalog */
	strcpy (catname, "IRAS Point Sources");
    else if (refcat ==  SDSS)	/* Sloan Digital Sky Survey */
	strcpy (catname, "SDSS Photmetric Catalog Sources");
    else if (refcat ==  PPM)	/* PPM Star Catalog */
	strcpy (catname, "PPM Catalog Stars");
    else if (refcat ==  TYCHO)	/* Tycho Star Catalog */
	strcpy (catname, "Tycho Catalog Stars");
    else if (refcat ==  TYCHO2)	/* Tycho-2 Star Catalog */
	strcpy (catname, "Tycho-2 Catalog Stars");
    else if (refcat == TYCHO2E)	/* Tycho-2 Star Catalog */
	strcpy (catname, "Tycho-2 Catalog Stars with mag error");
    else if (refcat ==  UA1)	/* USNO A-1.0 Star Catalog */
	strcpy (catname, "USNO-A1.0 Stars");
    else if (refcat ==  UB1)	/* USNO B-1.0 Star Catalog */
	strcpy (catname, "USNO-B1.0 Stars");
    else if (refcat ==  UCAC1)	/* USNO UCAC1 Star Catalog */
	strcpy (catname, "USNO-UCAC1 Stars");
    else if (refcat ==  UCAC2)	/* USNO UCAC2 Star Catalog */
	strcpy (catname, "USNO-UCAC2 Stars");
    else if (refcat ==  UCAC3)	/* USNO UCAC3 Star Catalog */
	strcpy (catname, "USNO-UCAC3 Stars");
    else if (refcat ==  UA2)	/* USNO A-2.0 Star Catalog */
	strcpy (catname, "USNO-A2.0 Stars");
    else if (refcat ==  USA1)	/* USNO SA-1.0 Star Catalog */
	strcpy (catname, "USNO-SA1.0 Stars");
    else if (refcat ==  USA2)	/* USNO SA-2.0 Star Catalog */
	strcpy (catname, "USNO-SA2.0 Stars");
    else if (refcat ==  HIP)	/* Hipparcos Star Catalog */
	strcpy (catname, "Hipparcos Catalog Stars");
    else if (refcat ==  ACT)	/* USNO ACT Star Catalog */
	strcpy (catname, "ACT Catalog Stars");
    else if (refcat ==  BSC)	/* Yale Bright Star Catalog */
	strcpy (catname, "Bright Star Catalog Stars");
    else if (refcat ==  TMPSC)	/* 2MASS Point Source Catalog */
	strcpy (catname, "2MASS Point Sources");
    else if (refcat == TMPSCE)	/* 2MASS Point Source Catalog */
	strcpy (catname, "2MASS Point Sources with mag error");
    else if (refcat ==  TMXSC)	/* 2MASS Extended Source Catalog */
	strcpy (catname, "2MASS Extended Sources");
    else if (refcat ==  TMIDR2)	/* 2MASS Point Source Catalog */
	strcpy (catname, "2MASS-IDR2 Point Sources");
    else if (refcat ==  SKY2K)	/* SKY2000 Master Catalog */
	strcpy (catname, "SKY2000 Catalog Stars");
    else if (refcat ==  SKYBOT)	/* SkyBot Solar System Objects */
	strcpy (catname, "SkyBot Objects");
    return (catname);
}


void
CatID (catid, refcat)

char	*catid;		/* Catalog ID (returned) */
int	refcat;		/* Catalog code */
{
    if (refcat == ACT)
	strcpy (catid, "act_id     ");
    else if (refcat == BSC)
	strcpy (catid, "bsc_id    ");
    else if (refcat == GSC || refcat == GSCACT)
	strcpy (catid, "gsc_id    ");
    else if (refcat == GSC2)
	strcpy (catid, "gsc2_id        ");
    else if (refcat == SDSS)
	strcpy (catid, "sdss_id            ");
    else if (refcat == USAC)
	strcpy (catid,"usac_id       ");
    else if (refcat == USA1)
	strcpy (catid,"usa1_id       ");
    else if (refcat == USA2)
	strcpy (catid,"usa2_id       ");
    else if (refcat == UAC)
	strcpy (catid,"usnoa_id      ");
    else if (refcat == UA1)
	strcpy (catid,"usnoa1_id     ");
    else if (refcat == UB1)
	strcpy (catid,"usnob1_id    ");
    else if (refcat == YB6)
	strcpy (catid,"usnoyb6_id   ");
    else if (refcat == UA2)
	strcpy (catid,"usnoa2_id     ");
    else if (refcat == UCAC1)
	strcpy (catid,"ucac1_id  ");
    else if (refcat == UCAC2)
	strcpy (catid,"ucac2_id  ");
    else if (refcat == UCAC3)
	strcpy (catid,"ucac3_id  ");
    else if (refcat == UJC)
	strcpy (catid,"usnoj_id     ");
    else if (refcat == TMPSC || refcat == TMPSCE)
	strcpy (catid,"2mass_id      ");
    else if (refcat == TMXSC)
	strcpy (catid,"2mx_id        ");
    else if (refcat == SAO)
	strcpy (catid,"sao_id ");
    else if (refcat == PPM)
	strcpy (catid,"ppm_id ");
    else if (refcat == IRAS)
	strcpy (catid,"iras_id");
    else if (refcat == TYCHO)
	strcpy (catid,"tycho_id  ");
    else if (refcat == TYCHO2 || refcat == TYCHO2E)
	strcpy (catid,"tycho2_id ");
    else if (refcat == HIP)
	strcpy (catid,"hip_id ");
    else if (refcat == SKY2K)
	strcpy (catid,"sky_id ");
    else if (refcat == SKYBOT)
	strcpy (catid,"skybot_id ");
    else
	strcpy (catid,"id              ");

    return;
}

double
CatRad (refcat)

int	refcat;		/* Catalog code */
{
    if (refcat==GSC || refcat==GSCACT || refcat==UJC || refcat==USAC ||
	refcat==USA1 || refcat==USA2 ||
	refcat == UCAC1 || refcat == UCAC2 || refcat == UCAC3)
	return (900.0);
    else if (refcat==UAC  || refcat==UA1  || refcat==UA2)
	return (120.0);
    else if (refcat == UB1 || refcat==SDSS)
	return (120.0);
    else if (refcat==GSC2)
	return (120.0);
    else if (refcat==TMPSC || refcat==TMPSCE || refcat==TMIDR2)
	return (120.0);
    else if (refcat==TMXSC)
	return (900.0);
    else if (refcat==GSC2)
	return (120.0);
    else if (refcat==SAO || refcat==PPM || refcat==IRAS || refcat == SKY2K ||
	    refcat==SKYBOT)
	return (5000.0);
    else
	return (1800.0);
}


char *
ProgName (progpath0)

char *progpath0;	/* Pathname by which program is invoked */
{
    char *progpath, *progname;
    int i, lpath;

    lpath = (strlen (progpath0) + 2) / 8;
    lpath = (lpath + 1) * 8;;
    progpath = (char *) calloc (lpath, 1);
    strcpy (progpath, progpath0);
    progname = progpath;
    for (i = strlen (progpath); i > -1; i--) {
        if (progpath[i] > 63 && progpath[i] < 90)
            progpath[i] = progpath[i] + 32;
        if (progpath[i] == '/') {
            progname = progpath + i + 1;
            break;
            }
	}
    return (progname);
}


char *
ProgCat (progname)

char *progname;	/* Program name which might contain catalog code */
{
    char *refcatname;
    refcatname = NULL;

    if (strcsrch (progname,"gsca") != NULL) {
	refcatname = (char *) calloc (1,8);
	strcpy (refcatname, "gscact");
	}
    else if (strcsrch (progname,"gsc2") != NULL) {
	refcatname = (char *) calloc (1,8);
	strcpy (refcatname, "gsc2");
	}
    else if (strcsrch (progname,"gsc") != NULL) {
	refcatname = (char *) calloc (1,8);
	strcpy (refcatname, "gsc");
	}
    else if (strcsrch (progname,"sdss") != NULL) {
	refcatname = (char *) calloc (1,8);
	strcpy (refcatname, "sdss");
	}
    else if (strcsrch (progname,"uac") != NULL) {
	refcatname = (char *) calloc (1,8);
	strcpy (refcatname, "uac");
	}
    else if (strcsrch (progname,"ua1") != NULL) {
	refcatname = (char *) calloc (1,8);
	strcpy (refcatname, "ua1");
	}
    else if (strcsrch (progname,"ub") != NULL) {
	refcatname = (char *) calloc (1,8);
	strcpy (refcatname, "ub1");
	}
    else if (strcsrch (progname,"yb6") != NULL) {
	refcatname = (char *) calloc (1,8);
	strcpy (refcatname, "yb6");
	}
    else if (strcsrch (progname,"ua2") != NULL) {
	refcatname = (char *) calloc (1,8);
	strcpy (refcatname, "ua2");
	}
    else if (strcsrch (progname,"usac") != NULL) {
	refcatname = (char *) calloc (1,8);
	strcpy (refcatname, "usac");
	}
    else if (strcsrch (progname,"usa1") != NULL) {
	refcatname = (char *) calloc (1,8);
	strcpy (refcatname, "usa1");
	}
    else if (strcsrch (progname,"usa2") != NULL) {
	refcatname = (char *) calloc (1,8);
	strcpy (refcatname, "usa2");
	}
    else if (strcsrch (progname,"ucac1") != NULL) {
	refcatname = (char *) calloc (1,8);
	strcpy (refcatname, "ucac1");
	}
    else if (strcsrch (progname,"ucac2") != NULL) {
	refcatname = (char *) calloc (1,8);
	strcpy (refcatname, "ucac2");
	}
    else if (strcsrch (progname,"ucac3") != NULL) {
	refcatname = (char *) calloc (1,8);
	strcpy (refcatname, "ucac3");
	}
    else if (strcsrch (progname,"ujc") != NULL) {
	refcatname = (char *) calloc (1,8);
	strcpy (refcatname, "ujc");
	}
    else if (strcsrch (progname,"sao") != NULL) {
	refcatname = (char *) calloc (1,8);
	strcpy (refcatname, "sao");
	}
    else if (strcsrch (progname,"ppm") != NULL) {
	refcatname = (char *) calloc (1,8);
	strcpy (refcatname, "ppm");
	}
    else if (strcsrch (progname,"ira") != NULL) {
	refcatname = (char *) calloc (1,8);
	strcpy (refcatname, "iras");
	}
    else if (strcsrch (progname,"ty") != NULL) {
	refcatname = (char *) calloc (1,8);
	if (strcsrch (progname, "2e") != NULL)
	    strcpy (refcatname, "tycho2e");
	else if (strcsrch (progname, "2") != NULL)
	    strcpy (refcatname, "tycho2");
	else
	    strcpy (refcatname, "tycho");
	}
    else if (strcsrch (progname,"hip") != NULL) {
	refcatname = (char *) calloc (1,16);
	strcpy (refcatname, "hipparcos");
	}
    else if (strcsrch (progname,"act") != NULL) {
	refcatname = (char *) calloc (1,8);
	strcpy (refcatname, "act");
	}
    else if (strcsrch (progname,"bsc") != NULL) {
	refcatname = (char *) calloc (1,8);
	strcpy (refcatname, "bsc");
	}
    else if (strcsrch (progname,"sky2k") != NULL) {
	refcatname = (char *) calloc (1,8);
	strcpy (refcatname, "sky2k");
	}
    else if (strcsrch (progname,"skybot") != NULL) {
	refcatname = (char *) calloc (1,8);
	strcpy (refcatname, "skybot");
	}
    else if (strcsrch (progname,"2mp") != NULL ||
	strcsrch (progname,"tmc") != NULL) {
	refcatname = (char *) calloc (1,8);
	if (strcsrch (progname,"ce"))
	    strcpy (refcatname, "tmce");
	else
	    strcpy (refcatname, "tmc");
	}
    else if (strcsrch (progname,"2mx") != NULL ||
	strcsrch (progname,"tmx") != NULL) {
	refcatname = (char *) calloc (1,8);
	strcpy (refcatname, "tmx");
	}
    else
	refcatname = NULL;

    return (refcatname);
}


void
CatNum (refcat, nnfld, nndec, dnum, numstr)

int	refcat;		/* Catalog code */
int	nnfld;		/* Number of characters in number (from CatNumLen) */
			/* Print leading zeroes if negative */
int	nndec;		/* Number of decimal places ( >= 0) */
			/* Omit leading spaces if negative */
double	dnum;		/* Catalog number of source */
char	*numstr;	/* Formatted number (returned) */

{
    char nform[16];	/* Format for star number */
    int lnum, i;

    /* USNO A1.0, A2.0, SA1.0, or SA2.0 Catalogs */
    if (refcat == USAC || refcat == USA1 || refcat == USA2 ||
	refcat == UAC  || refcat == UA1  || refcat == UA2) {
	if (nnfld < 0)
	    sprintf (numstr, "%013.8f", dnum);
	else
	    sprintf (numstr, "%13.8f", dnum);
	}

    /* USNO-B1.0  and USNO-YB6 */
    else if (refcat == UB1 || refcat == YB6) {
	if (nnfld < 0)
	    sprintf (numstr, "%012.7f", dnum);
	else
	    sprintf (numstr, "%12.7f", dnum);
	}

    /* USNO-UCAC1 */
    else if (refcat == UCAC1) {
	if (nnfld < 0)
	    sprintf (numstr, "%010.6f", dnum);
	else
	    sprintf (numstr, "%10.6f", dnum);
	}

    /* USNO-UCAC2 */
    else if (refcat == UCAC2) {
	if (nnfld < 0)
	    sprintf (numstr, "%010.6f", dnum);
	else
	    sprintf (numstr, "%10.6f", dnum);
	}

    /* USNO-UCAC3 */
    else if (refcat == UCAC3) {
	if (nnfld < 0)
	    sprintf (numstr, "%010.6f", dnum);
	else
	    sprintf (numstr, "%10.6f", dnum);
	}

    /* SDSS */
    else if (refcat == SDSS) {
	sprintf (numstr, "582%015.0f", dnum);
	}

    /* GSC II */
    else if (refcat == GSC2) {
	if (nnfld < 0) {
	    if (dnum > 0)
		sprintf (numstr, "N%.0f", (dnum+0.01));
	    else
		sprintf (numstr, "S%.0f", (-dnum + 0.01));
	    lnum = strlen (numstr);
	    if (lnum < -nnfld) {
		for ( i = lnum; i < -nnfld; i++)
		    strcat (numstr, " ");
		}
	    }
	else {
	    if (dnum > 0)
		sprintf (numstr, "N%.0f", (dnum+0.5));
	    else
		sprintf (numstr, "S%.0f", (-dnum + 0.5));
	    }
	}

    /* 2MASS Point Source Catalogs */
    else if (refcat == TMPSC || refcat == TMPSCE) {
	if (nnfld < 0)
	    sprintf (numstr, "%011.6f", dnum);
	else
	    sprintf (numstr, "%11.6f", dnum);
	}

    /* 2MASS Extended Source Catalog */
    else if (refcat == TMXSC) {
	if (nnfld < 0)
	    sprintf (numstr, "%011.6f", dnum);
	else
	    sprintf (numstr, "%11.6f", dnum);
	}

    /* 2MASS Point Source Catalogs */
    else if (refcat == TMIDR2) {
	if (nnfld < 0)
	    sprintf (numstr, "%010.7f", dnum);
	else
	    sprintf (numstr, "%10.7f", dnum);
	}

    /* USNO Plate Catalog */
    else if (refcat == USNO) {
	if (nnfld < 0)
	    sprintf (numstr, "%07d", (int)(dnum+0.5));
	else
	    sprintf (numstr, "%7d", (int)(dnum+0.5));
	}

    /* USNO UJ 1.0 Catalog */
    else if (refcat == UJC) {
	if (nnfld < 0)
	    sprintf (numstr, "%012.7f", dnum);
	else
	    sprintf (numstr, "%12.7f", dnum);
	}

    /* HST Guide Star Catalog */
    else if (refcat == GSC || refcat == GSCACT) {
	if (nnfld < 0)
	    sprintf (numstr, "%09.4f", dnum);
	else
	    sprintf (numstr, "%9.4f", dnum);
	}

    /* SAO, PPM, or IRAS Point Source Catalogs (TDC binary format) */
    else if (refcat==SAO || refcat==PPM || refcat==IRAS || refcat==BSC ||
	     refcat==HIP) {
	if (nnfld < 0)
	    sprintf (numstr, "%06d", (int)(dnum+0.5));
	else
	    sprintf (numstr, "%6d", (int)(dnum+0.5));
	}

    /* SKY2000 Catalog (TDC binary format) */
    else if (refcat==SKY2K) {
	if (nnfld < 0)
	    sprintf (numstr, "%07d", (int)(dnum+0.5));
	else
	    sprintf (numstr, "%7d", (int)(dnum+0.5));
	}


    /* Tycho or ACT catalogs */
    else if (refcat==TYCHO || refcat==TYCHO2 ||
	     refcat == TYCHO2E || refcat==ACT) {
	if (nnfld < 0)
	    sprintf (numstr, "%010.5f", dnum);
	else
	    sprintf (numstr, "%10.5f", dnum);
	}

    /* Starbase tab-separated, TDC binary, or TDC ASCII catalogs */
    else if (nndec > 0) {
	if (nnfld > 0)
	    sprintf (nform,"%%%d.%df", nnfld, nndec);
	else if (nnfld < 0)
	    sprintf (nform,"%%0%d.%df", -nnfld, nndec);
	else
	    sprintf (nform,"%%%d.%df", nndec+5, nndec);
	sprintf (numstr, nform, dnum);
	}
    else if (nnfld > 10) {
	sprintf (nform,"%%%d.0f", nnfld);
	sprintf (numstr, nform, dnum+0.49);
	}
    else if (nnfld > 0) {
	sprintf (nform,"%%%dd", nnfld);
	sprintf (numstr, nform, (int)(dnum+0.49));
	}
    else if (nnfld < 0) {
	sprintf (nform,"%%0%dd", -nnfld);
	sprintf (numstr, nform, (int)(dnum+0.49));
	}
    else if (nndec < 0)
	sprintf (numstr, "%d", (int)(dnum+0.49));
    else
	sprintf (numstr, "%6d", (int)(dnum+0.49));

    return;
}


int
CatNumLen (refcat, maxnum, nndec)

int	refcat;		/* Catalog code */
double	maxnum;		/* Maximum ID number */
			/* (Ignored for standard catalogs) */
int	nndec;		/* Number of decimal places ( >= 0) */

{
    int ndp;		/* Number of characters for decimal point */

    /* USNO A1.0, A2.0, SA1.0, or SA2.0 Catalogs */
    if (refcat == USAC || refcat == USA1 || refcat == USA2 ||
	refcat == UAC  || refcat == UA1  || refcat == UA2)
	return (13);

    /* USNO-B1.0 and YB6 */
    else if (refcat == UB1 || refcat == YB6)
	return (12);

    /* GSC II */
    else if (refcat == GSC2)
	return (13);

    /* 2MASS Point Source Catalog */
    else if (refcat == TMPSC || refcat == TMPSCE)
	return (11);
    else if (refcat == TMIDR2)
	return (10);

    /* 2MASS Extended Source Catalog */
    else if (refcat == TMXSC)
	return (11);

    /* UCAC1 Catalog */
    else if (refcat == UCAC1)
	return (10);

    /* UCAC2 Catalog */
    else if (refcat == UCAC2)
	return (10);

    /* UCAC3 Catalog */
    else if (refcat == UCAC3)
	return (10);

    /* USNO Plate Catalogs */
    else if (refcat == USNO)
	return (7);

    /* USNO UJ 1.0 Catalog */
    else if (refcat == UJC)
	return (12);

    /* SDSS Catalog */
    else if (refcat == SDSS)
	return (18);

    /* SkyBot Objects */
    else if (refcat == SKYBOT)
	return (6);

    /* HST Guide Star Catalog */
    else if (refcat == GSC || refcat == GSCACT)
	return (9);

    /* SAO, PPM, Hipparcos, or IRAS Point Source Catalogs (TDC binary format) */
    else if (refcat==SAO || refcat==PPM || refcat==IRAS || refcat==BSC ||
	     refcat==HIP)
	return (6);

    /* SKY2000 Catalog (TDC binary format) */
    else if (refcat==SKY2K)
	return (7);

    /* Tycho, Tycho2, or ACT catalogs */
    else if (refcat == TYCHO || refcat == TYCHO2 ||
	     refcat == TYCHO2E || refcat == ACT)
	return (10);

    /* Starbase tab-separated, TDC binary, or TDC ASCII catalogs */
    else {
	if (nndec > 0)
	    ndp = 1;
	else {
	    if ((nndec = NumNdec (maxnum)) > 0)
		ndp = 1;
	    else
		ndp = 0;
	    }
	if (maxnum < 10.0)
	    return (1 + nndec + ndp);
	else if (maxnum < 100.0)
	    return (2 + nndec + ndp);
	else if (maxnum < 1000.0)
	    return (3 + nndec + ndp);
	else if (maxnum < 10000.0)
	    return (4 + nndec + ndp);
	else if (maxnum < 100000.0)
	    return (5 + nndec + ndp);
	else if (maxnum < 1000000.0)
	    return (6 + nndec + ndp);
	else if (maxnum < 10000000.0)
	    return (7 + nndec + ndp);
	else if (maxnum < 100000000.0)
	    return (8 + nndec + ndp);
	else if (maxnum < 1000000000.0)
	    return (9 + nndec + ndp);
	else if (maxnum < 10000000000.0)
	    return (10 + nndec + ndp);
	else if (maxnum < 100000000000.0)
	    return (11 + nndec + ndp);
	else if (maxnum < 1000000000000.0)
	    return (12 + nndec + ndp);
	else if (maxnum < 10000000000000.0)
	    return (13 + nndec + ndp);
	else
	    return (14 + nndec + ndp);
	}
}


/* Return number of decimal places in catalogued numbers, if known */
int
CatNdec (refcat)

int	refcat;		/* Catalog code */

{
    /* USNO A1.0, A2.0, SA1.0, or SA2.0 Catalogs */
    if (refcat == USAC || refcat == USA1 || refcat == USA2 ||
	refcat == UAC  || refcat == UA1  || refcat == UA2)
	return (8);

    /* USNO B1.0 and USNO YB6 */
    else if (refcat == UB1 || refcat == YB6)
	return (7);

    /* GSC II */
    else if (refcat == GSC2)
	return (0);

    /* SDSS */
    else if (refcat == SDSS)
	return (0);

    /* SkyBot */
    else if (refcat == SKYBOT)
	return (0);

    /* 2MASS Point Source Catalog */
    else if (refcat == TMPSC || refcat == TMPSCE)
	return (6);

    /* 2MASS Extended Source Catalog */
    else if (refcat == TMXSC)
	return (6);

    /* 2MASS Point Source Catalog */
    else if (refcat == TMIDR2)
	return (7);

    /* USNO Plate Catalogs */
    else if (refcat == USNO)
	return (0);

    /* UCAC1 Catalog */
    else if (refcat == UCAC1)
	return (6);

    /* UCAC2 Catalog */
    else if (refcat == UCAC2)
	return (6);

    /* UCAC3 Catalog */
    else if (refcat == UCAC3)
	return (6);

    /* USNO UJ 1.0 Catalog */
    else if (refcat == UJC)
	return (7);

    /* HST Guide Star Catalog */
    else if (refcat == GSC || refcat == GSCACT)
	return (4);

    /* SAO, PPM, Hipparcos, or IRAS Point Source Catalogs (TDC binary format) */
    else if (refcat==SAO || refcat==PPM || refcat==IRAS || refcat==BSC ||
	     refcat==HIP || refcat == SKY2K)
	return (0);

    /* Tycho, Tycho2, or ACT catalogs */
    else if (refcat == TYCHO || refcat == TYCHO2 ||
	     refcat == TYCHO2E || refcat == ACT)
	return (5);

    /* Starbase tab-separated, TDC binary, or TDC ASCII catalogs */
    else
	return (-1);
}

/* Return name of specified magnitude */

void
CatMagName (imag, refcat, magname)

int	imag;		/* Sequence number of magnitude */
int	refcat;		/* Catalog code */
char	*magname;	/* Name of magnitude, returned */
{
    if (refcat == UAC  || refcat == UA1  || refcat == UA2 ||
	refcat == USAC || refcat == USA1 || refcat == USA2) {
	if (imag == 2)
	    strcpy (magname, "MagR");
	else
	    strcpy (magname, "MagB");
	}
    else if (refcat == UB1) {
	if (imag == 5)
	    strcpy (magname, "MagN");
	else if (imag == 4)
	    strcpy (magname, "MagR2");
	else if (imag == 3)
	    strcpy (magname, "MagB2");
	else if (imag == 2)
	    strcpy (magname, "MagR1");
	else
	    strcpy (magname, "MagB1");
	}
    else if (refcat == YB6) {
	if (imag == 5)
	    strcpy (magname, "MagK");
	else if (imag == 4)
	    strcpy (magname, "MagH");
	else if (imag == 3)
	    strcpy (magname, "MagJ");
	else if (imag == 2)
	    strcpy (magname, "MagR");
	else
	    strcpy (magname, "MagB");
	}
    else if (refcat == SDSS) {
	if (imag == 5)
	    strcpy (magname, "Magz");
	else if (imag == 4)
	    strcpy (magname, "Magi");
	else if (imag == 3)
	    strcpy (magname, "Magr");
	else if (imag == 2)
	    strcpy (magname, "Magg");
	else
	    strcpy (magname, "Magu");
	}
    else if (refcat==TYCHO || refcat==TYCHO2 || refcat==HIP || refcat==ACT) {
	if (imag == 2)
	    strcpy (magname, "MagV");
	else
	    strcpy (magname, "MagB");
	}
    else if (refcat==TYCHO2E) {
	if (imag == 1)
	    strcpy (magname, "MagB");
	else if (imag == 3)
	    strcpy (magname, "MagBe");
	else if (imag == 4)
	    strcpy (magname, "MagVe");
	else
	    strcpy (magname, "MagV");
	}
    else if (refcat==GSC2) {
	if (imag == 2)
	    strcpy (magname, "MagJ");
	else if (imag == 3)
	    strcpy (magname, "MagN");
	else if (imag == 4)
	    strcpy (magname, "MagU");
	else if (imag == 5)
	    strcpy (magname, "MagB");
	else if (imag == 6)
	    strcpy (magname, "MagV");
	else if (imag == 7)
	    strcpy (magname, "MagR");
	else if (imag == 8)
	    strcpy (magname, "MagI");
	else
	    strcpy (magname, "MagF");
	}
    else if (refcat==SKY2K) {
	if (imag == 1)
	    strcpy (magname, "MagB");
	else if (imag == 2)
	    strcpy (magname, "MagV");
	else if (imag == 3)
	    strcpy (magname, "MagP");
	else
	    strcpy (magname, "MagPv");
	}
    else if (refcat==TMPSC || refcat == TMXSC) {
	if (imag == 1)
	    strcpy (magname, "MagJ");
	else if (imag == 2)
	    strcpy (magname, "MagH");
	else
	    strcpy (magname, "MagK");
	}
    else if (refcat==TMPSCE) {
	if (imag == 1)
	    strcpy (magname, "MagJ");
	else if (imag == 2)
	    strcpy (magname, "MagH");
	else if (imag == 3)
	    strcpy (magname, "MagK");
	else if (imag == 4)
	    strcpy (magname, "MagJe");
	else if (imag == 5)
	    strcpy (magname, "MagHe");
	else if (imag == 6)
	    strcpy (magname, "MagKe");
	}
    else if (refcat==UCAC2) {
	if (imag == 1)
	    strcpy (magname, "MagJ");
	else if (imag == 2)
	    strcpy (magname, "MagH");
	else if (imag == 3)
	    strcpy (magname, "MagK");
	else if (imag == 4)
	    strcpy (magname, "MagC");
	}
    else if (refcat==UCAC3) {
	if (imag == 1)
	    strcpy (magname, "MagB");
	else if (imag == 2)
	    strcpy (magname, "MagR");
	else if (imag == 3)
	    strcpy (magname, "MagI");
	else if (imag == 4)
	    strcpy (magname, "MagJ");
	else if (imag == 5)
	    strcpy (magname, "MagH");
	else if (imag == 6)
	    strcpy (magname, "MagK");
	else if (imag == 7)
	    strcpy (magname, "MagM");
	else if (imag == 8)
	    strcpy (magname, "MagA");
	}
    else if (refcat==SKYBOT)
	strcpy (magname, "MagV");
    else
	strcpy (magname, "Mag");
    return;
}

/* Return number of magnitude specified by int of letter */

int
CatMagNum (imag, refcat)

int	imag;		/* int of magnitude letter */
int	refcat;		/* Catalog code */
{
    char cmag = (char) imag;	/* Letter name of magnitude */

    /* Make letter upper case */
    if (cmag > 96)
	cmag = cmag - 32;
 
    if (refcat == UAC  || refcat == UA1  || refcat == UA2 ||
	refcat == USAC || refcat == USA1 || refcat == USA2) {
	if (cmag == 'R')
	    return (2);
	else
	    return (1);	/* B */
	}
    if (refcat == UB1) {
	if (cmag == 'N')
	    return (5);
	else if (cmag == 'R')
	    return (4);
	else
	    return (3);	/* B */
	}
    else if (refcat == YB6) {
	if (cmag == 'K')
	    return (5);
	else if (cmag == 'H')
	    return (4);
	else if (cmag == 'J')
	    return (3);
	else if (cmag == 'R')
	    return (2);
	else if (cmag == 'B')
	    return (1);
	else
	    return (3);	/* J */
	}
    else if (refcat == SKYBOT) {
	return (1);
	}
    else if (refcat == SDSS) {
	if (cmag == 'Z')
	    return (5);
	else if (cmag == 'I')
	    return (4);
	else if (cmag == 'R')
	    return (3);
	else if (cmag == 'G')
	    return (2);
	else if (cmag == 'B')
	    return (1);
	else
	    return (2);	/* G */
	}
    else if (refcat==TYCHO || refcat==TYCHO2 || refcat==HIP || refcat==ACT) {
	if (cmag == 'B')
	    return (1);
	else
	    return (2);	/* V */
	}
    else if (refcat==GSC2) {
	if (cmag == 'J')
	    return (2);
	else if (cmag == 'N')
	    return (3);
	else if (cmag == 'U')
	    return (4);
	else if (cmag == 'B')
	    return (5);
	else if (cmag == 'V')
	    return (6);
	else if (cmag == 'R')
	    return (7);
	else if (cmag == 'I')
	    return (8);
	else
	    return (1);	/* F */
	}
    else if (refcat==TMPSC || refcat == TMXSC) {
	if (cmag == 'J')
	    return (1);
	else if (cmag == 'H')
	    return (2);
	else
	    return (3);	/* K */
	}
    else if (refcat==UCAC2) {
	if (cmag == 'J')
	    return (1);
	else if (cmag == 'H')
	    return (2);
	else if (cmag == 'K')
	    return (3);
	else if (cmag == 'C')
	    return (4);
	else
	    return (3);	/* K */
	}
    else if (refcat==UCAC3) {
	if (cmag == 'R')
	    return (2);
	else if (cmag == 'I')
	    return (3);
	else if (cmag == 'J')
	    return (4);
	else if (cmag == 'H')
	    return (5);
	else if (cmag == 'K')
	    return (6);
	else if (cmag == 'M')
	    return (7);
	else if (cmag == 'A')
	    return (8);
	else
	    return (1);	/* B */
	}
    else
	return (1);
}


/* Return number of decimal places in numeric string (-1 if not number) */

int
StrNdec (string)

char *string;	/* Numeric string */
{
    char *cdot;
    int lstr;

    if (notnum (string))
	return (-1);
    else {
	lstr = strlen (string);
	if ((cdot = strchr (string, '.')) == NULL)
	    return (0);
	else
	    return (lstr - (cdot - string));
	}
}


/* Return number of decimal places in a number */

int
NumNdec (number)

double number;	/* Floating point number */
{
    char nstring[16];
    char format[16];
    int fracpart;
    int ndec, ndmax;
    double shift;

    if (number < 10.0) {
	ndmax = 12;
	shift = 1000000000000.0;
	}
    else if (number < 100.0) {
	ndmax = 11;
	shift = 100000000000.0;
	}
    else if (number < 1000.0) {
	ndmax = 10;
	shift = 10000000000.0;
	}
    else if (number < 10000.0) {
	ndmax = 9;
	shift = 1000000000.0;
	}
    else if (number < 100000.0) {
	ndmax = 8;
	shift = 100000000.0;
	}
    else if (number < 1000000.0) {
	ndmax = 7;
	shift = 10000000.0;
	}
    else if (number < 10000000.0) {
	ndmax = 6;
	shift = 1000000.0;
	}
    else if (number < 100000000.0) {
	ndmax = 5;
	shift = 100000.0;
	}
    else if (number < 1000000000.0) {
	ndmax = 4;
	shift = 10000.0;
	}
    else if (number < 10000000000.0) {
	ndmax = 3;
	shift = 1000.0;
	}
    else if (number < 100000000000.0) {
	ndmax = 2;
	shift = 100.0;
	}
    else if (number < 1000000000000.0) {
	ndmax = 1;
	shift = 10.0;
	}
    else
	return (0);
    fracpart = (int) (((number - floor (number)) * shift) + 0.5);
    sprintf (format, "%%0%dd", ndmax);
    sprintf (nstring, format, fracpart);
    for (ndec = ndmax; ndec > 0; ndec--) {
	if (nstring[ndec-1] != '0')
	    break;
	}
    return (ndec);
}


static int dateform = 0 ;

void
setdateform (dateform0)
int dateform0;
{ dateform = dateform0; return; }


/* DateString-- Return string with epoch of position in desired format */
char *
DateString (epoch, tabout)

double	epoch;
int	tabout;
{
    double year;
    char *temp, *temp1;
    temp = calloc (16, 1);

    if (dateform < 1)
	dateform = EP_MJD;

    if (dateform == EP_EP) {
	if (tabout)
	    sprintf (temp, "	%9.4f", epoch);
	else
	    sprintf (temp, " %9.4f", epoch);
	}
    else if (dateform == EP_JD) {
	if (epoch == 0.0)
	    year = 0.0;
	else
	    year = ep2jd (epoch);
	if (tabout)
	    sprintf (temp, "	%13.5f", year);
	else
	    sprintf (temp, " %13.5f", year);
	}
    else if (dateform == EP_MJD) {
	if (epoch == 0.0)
	    year = 0.0;
	else
	    year = ep2mjd (epoch);
	if (tabout)
	    sprintf (temp, "	%11.5f", year);
	else
	    sprintf (temp, " %11.5f", year);
	}
    else {
	if (epoch == 0.0) {
	    if (tabout)
		sprintf (temp,"	0000-00-00");
	    else
		sprintf (temp," 0000-00-00");
	    if (dateform == EP_ISO)
		sprintf (temp,"T00:00");
	    }
	else {
	    temp1 = ep2fd (epoch);
	    if (dateform == EP_FD && strlen (temp1) > 10)
		temp1[10] = (char) 0;
	    if (dateform == EP_ISO && strlen (temp1) > 16)
		temp1[16] = (char) 0;
	    if (tabout)
		sprintf (temp, "	%s", temp1);
	    else
		sprintf (temp, " %s", temp1);
	    free (temp1);
	    }
	}
    return (temp);
}


/* SEARCHLIM-- Set RA and Dec limits for search given center and dimensions */
void
SearchLim (cra, cdec, dra, ddec, syscoor, ra1, ra2, dec1, dec2, verbose)

double	cra, cdec;	/* Center of search area  in degrees */
double	dra, ddec;	/* Horizontal and vertical half-widths in degrees */
int	syscoor;	/* Coordinate system */
double	*ra1, *ra2;	/* Right ascension limits in degrees */
double	*dec1, *dec2;	/* Declination limits in degrees */
int	verbose;	/* 1 to print limits, else 0 */

{
    double dec;

    /* Set right ascension limits for search */
    *ra1 = cra - dra;
    *ra2 = cra + dra;

    /* Keep right ascension between 0 and 360 degrees */
    if (syscoor != WCS_XY) {
	if (*ra1 < 0.0)
	    *ra1 = *ra1 + 360.0;
	if (*ra2 > 360.0)
	    *ra2 = *ra2 - 360.0;
	}

    /* Set declination limits for search */
    *dec1 = cdec - ddec;
    *dec2 = cdec + ddec;

    /* dec1 is always the smallest declination */
    if (*dec1 > *dec2) {
	dec = *dec1;
	*dec1 = *dec2;
	*dec2 = dec;
	}

    /* Search zones which include the poles cover 360 degrees in RA */
    if (syscoor != WCS_XY) {
	if (*dec1 < -90.0) {
	    *dec1 = -90.0;
	    *ra1 = 0.0;
	    *ra2 = 359.99999;
	    }
	if (*dec2 > 90.0) {
	    *dec2 = 90.0;
	    *ra1 = 0.0;
	    *ra2 = 359.99999;
	    }
	}

    if (verbose) {
	char rstr1[16],rstr2[16],dstr1[16],dstr2[16];
	if (syscoor == WCS_XY) {
	    num2str (rstr1, *ra1, 10, 5);
            num2str (dstr1, *dec1, 10, 5);
	    num2str (rstr2, *ra2, 10, 5);
            num2str (dstr2, *dec2, 10, 5);
	    }
	else if (degout) {
	    deg2str (rstr1, 16, *ra1, 6);
            deg2str (dstr1, 16, *dec1, 6);
	    deg2str (rstr2, 16, *ra2, 6);
            deg2str (dstr2, 16, *dec2, 6);
	    }
	else {
	    ra2str (rstr1, 16, *ra1, 3);
            dec2str (dstr1, 16, *dec1, 2);
	    ra2str (rstr2, 16, *ra2, 3);
            dec2str (dstr2, 16, *dec2, 2);
	    }
	fprintf (stderr,"SearchLim: RA: %s - %s  Dec: %s - %s\n",
		 rstr1,rstr2,dstr1,dstr2);
	}
    return;
}


/* REFLIM-- Set limits in reference catalog coordinates given search coords */
void
RefLim (cra, cdec, dra, ddec, sysc, sysr, eqc, eqr, epc, epr, secmarg,
	ramin, ramax, decmin, decmax, wrap, verbose)

double	cra, cdec;	/* Center of search area  in degrees */
double	dra, ddec;	/* Horizontal and vertical half-widths of area */
int	sysc, sysr;	/* System of search, catalog coordinates */
double	eqc, eqr;	/* Equinox of search, catalog coordinates in years */
double	epc, epr;	/* Epoch of search, catalog coordinates in years */
double	secmarg;	/* Margin in arcsec/century to catch moving stars */
double	*ramin,*ramax;	/* Right ascension search limits in degrees (returned)*/
double	*decmin,*decmax; /* Declination search limits in degrees (returned) */
int	*wrap;		/* 1 if search passes through 0:00:00 RA */
int	verbose;	/* 1 to print limits, else 0 */

{
    double ra, ra1, ra2, ra3, ra4, dec1, dec2, dec3, dec4;
    double dec, acdec, adec, adec1, adec2, dmarg, dist, dra1;
    int nrot;

    /* Deal with all or nearly all of the sky */
    if (ddec > 80.0 && dra > 150.0) {
	*ramin = 0.0;
	*ramax = 360.0;
	*decmin = -90.0;
	*decmax = 90.0;
	*wrap = 0;
	if (verbose)
	    fprintf (stderr,"RefLim: RA: 0.0 - 360.0  Dec: -90.0 - 90.0\n");
	return;
	}

    /* Set declination limits for search */
    dec1 = cdec - ddec;
    dec2 = cdec + ddec;

    /* dec1 is always the smallest declination */
    if (dec1 > dec2) {
	dec = dec1;
	dec1 = dec2;
	dec2 = dec;
	}
    dec3 = dec2;
    dec4 = dec1;

    /* Deal with south pole */
    if (dec1 < -90.0) {
	dec1 = 90.0 - (dec1 + 90.0);
	if (dec1 > dec2)
	    dec2 = dec1;
	dec1 = -90.0;
	dra1 = 180.0;
	}

    /* Deal with north pole */
    if (dec2 > 90.0) {
	dec2 = 90.0 - (dec2 - 90.0);
	if (dec2 < dec1)
	    dec1 = dec2;
	dec2 = 90.0;
	dra1 = 180.0;
	}

    /* Adjust width in right ascension to that at max absolute declination */
    adec1 = fabs (dec1);
    adec2 = fabs (dec2);
    if (adec1 > adec2)
	adec = adec1;
    else
	adec = adec2;
    acdec = fabs (cdec);
    if (adec < 90.0 && adec > acdec)
	dra1 = dra * (cos (degrad(acdec)) / cos (degrad(adec)));
    else if (adec == 90.0)
	dra1 = 180.0;

    /* Set right ascension limits for search */
    ra1 = cra - dra1;
    ra2 = cra + dra1;

    /* Keep right ascension limits between 0 and 360 degrees */
    if (ra1 < 0.0) {
	nrot = 1 - (int) (ra1 / 360.0);
	ra1 = ra1 + (360.0 * (double) nrot);
	}
    if (ra1 > 360.0) {
	nrot = (int) (ra1 / 360.0);
	ra1 = ra1 - (360.0 * (double) nrot);
	}
    if (ra2 < 0.0) {
	nrot = 1 - (int) (ra2 / 360.0);
	ra2 = ra2 + (360.0 * (double) nrot);
	}
    if (ra2 > 360.0) {
	nrot = (int) (ra2 / 360.0);
	ra2 = ra2 - (360.0 * (double) nrot);
	}

    if (ra1 > ra2)
	*wrap = 1;
    else
	*wrap = 0;

    ra3 = ra1;
    ra4 = ra2;

    /* Convert search corners to catalog coordinate system and equinox */
    ra = cra;
    dec = cdec;
    wcscon (sysc, sysr, eqc, eqr, &ra, &dec, epc);
    wcscon (sysc, sysr, eqc, eqr, &ra1, &dec1, epc);
    wcscon (sysc, sysr, eqc, eqr, &ra2, &dec2, epc);
    wcscon (sysc, sysr, eqc, eqr, &ra3, &dec3, epc);
    wcscon (sysc, sysr, eqc, eqr, &ra4, &dec4, epc);

    /* Find minimum and maximum right ascensions to search */
    *ramin = ra1;
    if (ra3 < *ramin)
	*ramin = ra3;
    *ramax = ra2;
    if (ra4 > *ramax)
	*ramax = ra4;

    /* Add margins to RA limits to get most stars which move */
    if (secmarg > 0.0 && epc != 0.0) {
	dmarg = (secmarg / 3600.0) * fabs (epc - epr);
	*ramin = *ramin - (dmarg * cos (degrad (cdec)));
	*ramax = *ramax + (dmarg * cos (degrad (cdec)));
	}
   else
	dmarg = 0.0;

    if (*wrap) {
	ra = *ramax;
	*ramax = *ramin;
	*ramin = ra;
	}

    /* Find minimum and maximum declinatons to search */
    *decmin = dec1;
    if (dec2 < *decmin)
	*decmin = dec2;
    if (dec3 < *decmin)
	*decmin = dec3;
    if (dec4 < *decmin)
	*decmin = dec4;
    *decmax = dec1;
    if (dec2 > *decmax)
	*decmax = dec2;
    if (dec3 > *decmax)
	*decmax = dec3;
    if (dec4 > *decmax)
	*decmax = dec4;

    /* Add margins to Dec limits to get most stars which move */
    if (dmarg > 0.0) {
	*decmin = *decmin - dmarg;
	*decmax = *decmax + dmarg;
	}

    /* Check for pole */
    dist = wcsdist (ra, dec, *ramax, *decmax);
    if (dec + dist > 90.0) {
	*ramin = 0.0;
	*ramax = 359.99999;
	*decmax = 90.0;
	*wrap = 0;
	}
    else if (dec - dist < -90.0) {
	*ramin = 0.0;
	*ramax = 359.99999;
	*decmin = -90.0;
	*wrap = 0;
	}
	

    /* Search zones which include the poles cover 360 degrees in RA */
    else if (*decmin < -90.0) {
	*decmin = -90.0;
	*ramin = 0.0;
	*ramax = 359.99999;
	*wrap = 0;
	}
    else if (*decmax > 90.0) {
	*decmax = 90.0;
	*ramin = 0.0;
	*ramax = 359.99999;
	*wrap = 0;
	}
    if (verbose) {
	char rstr1[16],rstr2[16],dstr1[16],dstr2[16];
	if (degout) {
	    deg2str (rstr1, 16, *ramin, 6);
            deg2str (dstr1, 16, *decmin, 6);
	    deg2str (rstr2, 16, *ramax, 6);
            deg2str (dstr2, 16, *decmax, 6);
	    }
	else {
	    ra2str (rstr1, 16, *ramin, 3);
            dec2str (dstr1, 16, *decmin, 2);
	    ra2str (rstr2, 16, *ramax, 3);
            dec2str (dstr2, 16, *decmax, 2);
	    }
	fprintf (stderr,"RefLim: RA: %s - %s  Dec: %s - %s",
		 rstr1,rstr2,dstr1,dstr2);
	if (*wrap)
	    fprintf (stderr," wrap\n");
	else
	    fprintf (stderr,"\n");
	}
    return;
}


/* RANGEINIT -- Initialize range structure from string */

struct Range *
RangeInit (string, ndef)

char	*string;	/* String containing numbers separated by , and - */
int	ndef;		/* Maximum allowable range value */

{
    struct Range *range;
    int ip, irange;
    char *slast;
    double first, last, step;

    if (!isrange (string) && !isnum (string))
	return (NULL);
    ip = 0;
    range = (struct Range *)calloc (1, sizeof (struct Range));
    range->irange = -1;
    range->nvalues = 0;
    range->nranges = 0;

    for (irange = 0; irange < MAXRANGE; irange++) {

	/* Default to entire list */
	first = 1.0;
	last = ndef;
	step = 1.0;

	/* Skip delimiters to start of range */
	while (string[ip] == ' ' || string[ip] == '	' ||
	       string[ip] == ',')
	    ip++;

	/* Get first limit
	 * Must be a number, '-', 'x', or EOS.  If not return ERR */
	if (string[ip] == (char)0) {	/* end of list */
	    if (irange == 0) {

		/* Null string defaults */
		range->ranges[0] = first;
		if (first < 1)
		    range->ranges[1] = first;
		else
		    range->ranges[1] = last;
		range->ranges[2] = step;
		range->nvalues = range->nvalues + 1 +
			  ((range->ranges[1]-range->ranges[0])/step);
		range->nranges++;
		return (range);
		}
	    else
		return (range);
	    }
	else if (string[ip] > (char)47 && string[ip] < 58) {
	    first = strtod (string+ip, &slast);
	    ip = slast - string;
	    }
	else if (strchr ("-:x", string[ip]) == NULL) {
	    free (range);
	    return (NULL);
	    }

	/* Skip delimiters */
	while (string[ip] == ' ' || string[ip] == '	' ||
	       string[ip] == ',')
	    ip++;

	/* Get last limit
	* Must be '-', or 'x' otherwise last = first */
	if (string[ip] == '-' || string[ip] == ':') {
	    ip++;
	    while (string[ip] == ' ' || string[ip] == '	' ||
	   	   string[ip] == ',')
		ip++;
	    if (string[ip] == (char)0)
		last = first + ndef;
	    else if (string[ip] > (char)47 && string[ip] < 58) {
		last = strtod (string+ip, &slast);
		ip = slast - string;
		}
	    else if (string[ip] != 'x')
		last = first + ndef;
	    }
	else if (string[ip] != 'x')
	    last = first;

	/* Skip delimiters */
	while (string[ip] == ' ' || string[ip] == '	' ||
	       string[ip] == ',')
	    ip++;

	/* Get step
	 * Must be 'x' or assume default step. */
	if (string[ip] == 'x') {
	    ip++;
	    while (string[ip] == ' ' || string[ip] == '	' ||
	   	   string[ip] == ',')
		ip++;
	    if (string[ip] == (char)0)
		step = 1.0;
	    else if (string[ip] > (char)47 && string[ip] < 58) {
		step = strtod (string+ip, &slast);
		ip = slast - string;
		}
	    else if (string[ip] != '-' && string[ip] != ':')
		step = 1.0;
            }

	/* Output the range triple */
	range->ranges[irange*3] = first;
	range->ranges[irange*3 + 1] = last;
	range->ranges[irange*3 + 2] = step;
	range->nvalues = range->nvalues + ((last-first+(0.1*step)) / step + 1);
	range->nranges++;
	}

    return (range);
}


/* ISRANGE -- Return 1 if string is a range, else 0 */

int
isrange (string)

char *string;		/* String which might be a range of numbers */

{
    int i, lstr;

    /* If string is NULL or empty, return 0 */
    if (string == NULL || strlen (string) == 0)
	return (0);

    /* If range separators present, check to make sure string is range */
    else if (strchr (string+1, '-') || strchr (string+1, ',')) {
	lstr = strlen (string);
	for (i = 0; i < lstr; i++) {
	    if (strchr ("0123456789-,.x", (int)string[i]) == NULL)
		return (0);
	    }
	return (1);
	}
    else
	return (0);
}


/* RSTART -- Restart at beginning of range */

void
rstart (range)

struct Range *range;	/* Range structure */

{
    range->irange = -1;
    return;
}


/* RGETN -- Return number of values from range structure */

int
rgetn (range)

struct Range *range;	/* Range structure */

{
    return (range->nvalues);
}


/*  RGETR8 -- Return next number from range structure as 8-byte f.p. number */

double
rgetr8 (range)

struct Range *range;	/* Range structure */

{
    int i;

    if (range == NULL)
	return (0.0);
    else if (range->irange < 0) {
	range->irange = 0;
	range->first = range->ranges[0];
	range->last = range->ranges[1];
	range->step = range->ranges[2];
	range->value = range->first;
	}
    else {
	range->value = range->value + range->step;
	if (range->value > (range->last + (range->step * 0.5))) {
	    range->irange++;
	    if (range->irange < range->nranges) {
		i = range->irange * 3;
		range->first = range->ranges[i];
		range->last = range->ranges[i+1];
		range->step = range->ranges[i+2];
		range->value = range->first;
		}
	    else
		range->value = 0.0;
	    }
	}
    return (range->value);
}


/*  RGETI4 -- Return next number from range structure as 4-byte integer */

int
rgeti4 (range)

struct Range *range;	/* Range structure */

{
    double value;

    value = rgetr8 (range);
    return ((int) (value + 0.000000001));
}


/* AGETI4 -- Read int value from a file where keyword=value, anywhere */

int
ageti4 (string, keyword, ival)

char	*string;	/* character string containing <keyword>= <value> */
char	*keyword;	/* character string containing the name of the keyword
			   the value of which is returned.  hget searches for a
                 	   line beginning with this string.  if "[n]" or ",n" is
			   present, the n'th token in the value is returned. */
int	*ival;		/* Integer value, returned */
{
    char value[32];

    if (agets (string, keyword, 31, value)) {
	*ival = atoi (value);
	return (1);
	}
    else
	return (0);
}
	

/* AGETR8 -- Read double value from a file where keyword=value, anywhere */
int
agetr8 (string, keyword, dval)

char	*string;	/* character string containing <keyword>= <value> */
char	*keyword;	/* character string containing the name of the keyword
			   the value of which is returned.  hget searches for a
                 	   line beginning with this string.  if "[n]" or ",n" is
			   present, the n'th token in the value is returned. */
double	*dval;		/* Double value, returned */
{
    char value[32];

    if (agets (string, keyword, 31, value)) {
	*dval = atof (value);
	return (1);
	}
    else
	return (0);
}


/* AGETS -- Get keyword value from ASCII string with keyword=value anywhere */

int
agets (string, keyword0, lval, value)

char *string;  /* character string containing <keyword>= <value> info */
char *keyword0;  /* character string containing the name of the keyword
                   the value of which is returned.  hget searches for a
                   line beginning with this string.  if "[n]" or ",n" is
		   present, the n'th token in the value is returned. */
int lval;       /* Size of value in characters
		   If negative, value ends at end of line */
char *value;      /* String (returned) */
{
    char keyword[81];
    char *pval, *str, *pkey, *pv;
    char squot[2], dquot[2], lbracket[2], rbracket[2], comma[2];
    char *lastval, *rval, *brack1, *brack2, *lastring, *iquot, *ival;
    int ipar, i, lkey;

    squot[0] = (char) 39;
    squot[1] = (char) 0;
    dquot[0] = (char) 34;
    dquot[1] = (char) 0;
    lbracket[0] = (char) 91;
    lbracket[1] = (char) 0;
    comma[0] = (char) 44;
    comma[1] = (char) 0;
    rbracket[0] = (char) 93;
    rbracket[1] = (char) 0;
    lastring = string + strlen (string);

    /* Find length of variable name */
    strncpy (keyword,keyword0, sizeof(keyword)-1);
    brack1 = strsrch (keyword,lbracket);
    if (brack1 == NULL)
	brack1 = strsrch (keyword,comma);
    if (brack1 != NULL) {
	*brack1 = '\0';
	brack1++;
	}
    lkey = strlen (keyword);

    /* First check for the existence of the keyword in the string */
    pkey = strcsrch (string, keyword);

    /* If keyword has not been found, return 0 */
    if (pkey == NULL)
	return (0);

    /* If it has been found, check for = or : and preceding characters */
    pval = NULL;
    while (pval == NULL) {

	/* Must be at start of file or after control character or space */
	if (pkey != string && *(pkey-1) > 32) {
	    str = pkey;
	    pval = NULL;
	    }

	/* Must have "=" or ":" as next nonspace character */
	else {
	    pv = pkey + lkey;
	    while (*pv == ' ')
		pv++;
	    if (*pv != '=' && *pv != ':') {
		str = pkey;
		pval = NULL;
		}

	    /* If found, bump pointer past keyword, operator, and spaces */
	    else {
		pval = pv + 1;
		while (*pval == '=' || *pval == ' ')
		    pval++;
		break;
		}
	    }
	str = str + lkey;
	if (str > lastring)
	    break;
	pkey = strcsrch (str, keyword);
	if (pkey == NULL)
	    break;
	}
    if (pval == NULL)
	return (0);

    /* Drop leading spaces */
    while (*pval == ' ') pval++;

    /* Pad quoted material with _; drop leading and trailing quotes */
    iquot = NULL;
    if (*pval == squot[0]) {
	pval++;
	iquot = strsrch (pval, squot);
	}
    if (*pval == dquot[0]) {
	pval++;
	iquot = strsrch (pval, dquot);
	}
    if (iquot != NULL) {
	*iquot = (char) 0;
	for (ival = pval; ival < iquot; ival++) {
	    if (*ival == ' ')
		*ival = '_';
	    }
	}

    /* If keyword has brackets, figure out which token to extract */
    if (brack1 != NULL) {
        brack2 = strsrch (brack1,rbracket);
        if (brack2 != NULL)
            *brack2 = '\0';
        ipar = atoi (brack1);
	}
    else
	ipar = 1;

    /* Move to appropriate token */
    for (i = 1; i < ipar; i++) {
	while (*pval != ' ' && *pval != '/' && pval < lastring)
	    pval++;

	/* Drop leading spaces  or / */
	while (*pval == ' ' || *pval == '/')
	    pval++;
	}

    /* Transfer token value to returned string */
    rval = value;
    if (lval < 0) {
	lastval = value - lval - 1;
	while (*pval != '\n' && pval < lastring && rval < lastval) {
	    if (lval > 0 && *pval == ' ')
		break;
	    *rval++ = *pval++;
	    }
	}
    else {
	lastval = value + lval - 1;
	while (*pval != '\n' && *pval != '/' &&
	    pval < lastring && rval < lastval) {
	    if (lval > 0 && *pval == ' ')
		break;
	    *rval++ = *pval++;
	    }
	}
    if (rval < lastval)
	*rval = (char) 0;
    else
	*lastval = 0;

    return (1);
}

char sptbv[468]={"O5O8B0B0B0B1B1B1B2B2B2B3B3B3B4B5B5B6B6B6B7B7B8B8B8B9B9B9B9A0A0A0A0A0A0A0A0A0A2A2A2A2A2A2A2A2A5A5A5A5A6A7A7A7A7A7A7A7A7A7A7F0F0F0F0F0F0F0F2F2F2F2F2F2F2F5F5F5F5F5F5F5F5F5F8F8F8F8F8F8G0G5G5G2G2G2G3G3G4G4G5G5G5G6G6G6G6G6K6K6K6K6K7K7K7K7K7K7K7K7K7K7K7K7K7K7K8K8K8K8K8K8K8K8K8K8K8K8K8K8K8K8K8K8K8K5K5K5K5K5K6K6K6K6K6K6K6K7K7K7K7K7K7K7K8K8K8K8K9K9K9M0M0M0M0M0M0M1M1M1M1M1M2M2M2M2M3M3M4M4M5M5M5M2M2M2M3M3M4M4M5M5M5M6M6M6M6M6M6M6M6M6M7M7M7M7M7M7M7M7M7M7M7M7M7M7M8M8M8M8M8M8M8"};

void
bv2sp (bv, b, v, isp)

double	*bv;	/* B-V Magnitude */
double	b;	/* B Magnitude used if bv is NULL */
double	v;	/* V Magnitude used if bv is NULL */
char	*isp;	/* Spectral type */
{
    double bmv;	/* B - V magnitude */
    int im;

    if (bv == NULL)
	bmv = b - v;
    else
	bmv = *bv;

    if (bmv < -0.32) {
	isp[0] = '_';
	isp[1] = '_';
	}
    else if (bmv > 2.00) {
	isp[0] = '_';
	isp[1] = '_';
	}
    else if (bmv < 0) {
	im = 2 * (32 + (int)(bmv * 100.0 - 0.5));
	isp[0] = sptbv[im];
	isp[1] = sptbv[im+1];
	}
    else {
	im = 2 * (32 + (int)(bmv * 100.0 + 0.5));
	isp[0] = sptbv[im];
	isp[1] = sptbv[im+1];
	}
    return;
}

char sptbr1[96]={"O5O8O9O9B0B0B0B0B0B1B1B1B2B2B2B2B2B3B3B3B3B3B3B5B5B5B5B6B6B6B7B7B7B7B8B8B8B8B8B9B9B9B9B9A0A0A0"};

char sptbr2[904]={"A0A0A0A0A0A0A0A0A2A2A2A2A2A2A2A2A2A2A2A2A2A2A2A5A5A5A5A5A5A5A5A5A5A5A7A7A7A7A7A7A7A7A7A7A7A7A7A7A7A7F0F0F0F0F0F0F0F0F0F0F0F0F0F0F0F0F2F2F2F2F2F2F2F2F2F2F2F5F5F5F5F5F5F5F5F5F5F5F5F5F5F8F8F8F8F8F8F8F8F8F8F8F8F8F8G0G0G0G0G0G0G0G0G2G2G2G2G2G5G5G5G5G5G5G5G5G8G8G8G8G8G8G8G8G8G8G8G8G8G8K0K0K0K0K0K0K0K0K0K0K0K0K0K0K0K2K2K2K2K2K2K2K2K2K2K2K2K2K2K2K2K2K2K2K2K2K2K2K2K2K2K2K2K2K2K2K2K5K5K5K5K5K5K5K5K5K5K5K5K5K5K5K5K5K5K5K5K5K5K5K5K5K5K5K5K5K5K5K5K5K5K5K5K5K5K5K5K5K5K5K5K7K7K7K7K7K7K7K7K7K7K7K7K7K7K7K7K7K7K7K7K7K7K7K7K7M0M0M0M0M0M0M0M0M0M0M0M0M0M0M0M0M0M0M0M0M0M0M0M0M1M1M1M1M1M1M1M1M1M1M1M1M1M1M1M2M2M2M2M2M2M2M2M2M2M2M2M2M2M2M3M3M3M3M3M3M3M3M3M3M3M4M4M4M4M4M4M4M4M4M4M4M4M4M4M5M5M5M5M5M5M5M5M5M5M5M5M5M5M5M5M5M5M5M5M6M6M6M6M6M6M6M6M6M6M6M6M6M6M6M6M6M6M6M6M6M6M6M6M6M6M6M6M6M7M7M7M7M7M7M7M7M7M7M7M7M7M7M7M7M7M7M7M7M7M7M7M7M7M7M7M7M7M7M7M7M7M7M7M7M7M7M7M7M7M7M7M8M8M8M8M8M8M8M8M8M8M8M8M8M8M8M8M8M8M8M8M8M8M8M8"};

void
br2sp (br, b, r, isp)

double	*br;	/* B-R Magnitude */
double	b;	/* B Magnitude used if br is NULL */
double	r;	/* R Magnitude used if br is NULL */
char	*isp;	/* Spectral type */
{
    double bmr;	/* B - R magnitude */
    int im;

    if (br == NULL)
	bmr = b - r;
    else
	bmr = *br;

    if (b == 0.0 && r > 2.0) {
	isp[0] = '_';
	isp[1] = '_';
	}
    else if (bmr < -0.47) {
	isp[0] = '_';
	isp[1] = '_';
	}
    else if (bmr > 4.50) {
	isp[0] = '_';
	isp[1] = '_';
	}
    else if (bmr < 0) {
	im = 2 * (47 + (int)(bmr * 100.0 - 0.5));
	isp[0] = sptbr1[im];
	isp[1] = sptbr1[im+1];
	}
    else {
	im = 2 * ((int)(bmr * 100.0 + 0.49));
	isp[0] = sptbr2[im];
	isp[1] = sptbr2[im+1];
	}
    return;
}


void
CatTabHead (refcat,sysout,nnfld,mprop,nmag,ranges,keyword,gcset,tabout,
	    classd,printxy,gobj1,fd)

int	refcat;		/* Catalog being searched */
int	sysout;		/* Output coordinate system */
int	nnfld;		/* Number of characters in ID column */
int	mprop;		/* 1 if proper motion in catalog */
int	nmag;		/* Number of magnitudes */
char	*ranges;	/* Catalog numbers to print */
char	*keyword;	/* Column to add to tab table output */
int	gcset;		/* 1 if there are any values in gc[] */
int	tabout;		/* 1 if output is tab-delimited */
int	classd; 	/* GSC object class to accept (-1=all) */
int	printxy;	/* 1 if X and Y included in output */
char	**gobj1;	/* Pointer to array of object names; NULL if none */
FILE	*fd;		/* Output file descriptor; none if NULL */

{
    int typecol;
    char headline[160];

    /* Set flag for plate, class, type, or 3rd magnitude column */
    if (refcat == BINCAT || refcat == SAO  || refcat == PPM ||
	refcat == ACT  || refcat == TYCHO2 || refcat == BSC)
	typecol = 1;
    else if ((refcat == GSC || refcat == GSCACT) && classd < -1)
	typecol = 3;
    else if (refcat == TMPSC)
	typecol = 4;
    else if (refcat == GSC || refcat == GSCACT ||
	refcat == UJC || refcat == IRAS ||
	refcat == USAC || refcat == USA1   || refcat == USA2 ||
	refcat == UAC  || refcat == UA1    || refcat == UA2 ||
	refcat == BSC  || (refcat == TABCAT&&gcset))
	typecol = 2;
    else
	typecol = 0;


    /* Print column headings */
    if (refcat == ACT)
	strcpy (headline, "act_id       ");
    else if (refcat == BSC)
	strcpy (headline, "bsc_id       ");
    else if (refcat == GSC || refcat == GSCACT)
	strcpy (headline, "gsc_id       ");
    else if (refcat == USAC)
	strcpy (headline,"usac_id       ");
    else if (refcat == USA1)
	strcpy (headline,"usa1_id       ");
    else if (refcat == USA2)
	strcpy (headline,"usa2_id       ");
    else if (refcat == UAC)
	strcpy (headline,"usnoa_id      ");
    else if (refcat == UA1)
	strcpy (headline,"usnoa1_id     ");
    else if (refcat == UA2)
	strcpy (headline,"usnoa2_id     ");
    else if (refcat == UJC)
	strcpy (headline,"usnoj_id      ");
    else if (refcat == TMPSC)
	strcpy (headline,"2mass_id      ");
    else if (refcat == TMXSC)
	strcpy (headline,"2mx_id        ");
    else if (refcat == SAO)
	strcpy (headline,"sao_id        ");
    else if (refcat == PPM)
	strcpy (headline,"ppm_id        ");
    else if (refcat == IRAS)
	strcpy (headline,"iras_id       ");
    else if (refcat == TYCHO)
	strcpy (headline,"tycho_id      ");
    else if (refcat == TYCHO2)
	strcpy (headline,"tycho2_id     ");
    else if (refcat == HIP)
	strcpy (headline,"hip_id        ");
    else
	strcpy (headline,"id            ");
    headline[nnfld] = (char) 0;

    if (sysout == WCS_GALACTIC)
	strcat (headline,"	long_gal   	lat_gal  ");
    else if (sysout == WCS_ECLIPTIC)
	strcat (headline,"	long_ecl   	lat_ecl  ");
    else if (sysout == WCS_B1950)
	strcat (headline,"	ra1950      	dec1950  ");
    else
	strcat (headline,"	ra      	dec      ");
    if (refcat == USAC || refcat == USA1 || refcat == USA2 ||
	refcat == UAC  || refcat == UA1  || refcat == UA2)
	strcat (headline,"	magb	magr	plate");
    if (refcat == TMPSC)
	strcat (headline,"	magj	magh	magk");
    else if (refcat==TYCHO || refcat==TYCHO2 || refcat==HIP || refcat==ACT)
	strcat (headline,"	magb	magv");
    else if (refcat == GSC || refcat == GSCACT)
	strcat (headline,"	mag	class	band	N");
    else if (refcat == UJC)
	strcat (headline,"	mag	plate");
    else
	strcat (headline,"	mag");
    if (typecol == 1)
	strcat (headline,"	type");
    if (mprop)
	strcat (headline,"	Ura    	Udec  ");
    if (ranges == NULL)
	strcat (headline,"	arcsec");
    if (refcat == TABCAT && keyword != NULL) {
	strcat (headline,"	");
	strcat (headline, keyword);
	}
    if (gobj1 != NULL)
	strcat (headline,"	object");
    if (printxy)
	strcat (headline, "	x      	y      ");
    if (tabout) {
	printf ("%s\n", headline);
	if (fd != NULL)
	    fprintf (fd, "%s\n", headline);
	}

    strcpy (headline, "---------------------");
    headline[nnfld] = (char) 0;
    strcat (headline,"	------------	------------");
    if (nmag == 2)
	strcat (headline,"	-----	-----");
    else
	strcat (headline,"	-----");
    if (refcat == GSC || refcat == GSCACT)
	strcat (headline,"	-----	----	-");
    else if (typecol == 1)
	strcat (headline,"	----");
    else if (typecol == 2)
	strcat (headline,"	-----");
    else if (typecol == 4)
	strcat (headline,"	-----");
    if (mprop)
	strcat (headline,"	-------	------");
    if (ranges == NULL)
	strcat (headline, "	------");
    if (refcat == TABCAT && keyword != NULL)
	strcat (headline,"	------");
    if (printxy)
	strcat (headline, "	-------	-------");
    if (tabout) {
	printf ("%s\n", headline);
	if (fd != NULL)
	    fprintf (fd, "%s\n", headline);
	}
}


/* TMCID -- Return 1 if string is 2MASS ID, else 0 */

int
tmcid (string, ra, dec)

char	*string;	/* Character string to check */
double	*ra;		/* Right ascension (returned) */
double	*dec;		/* Declination (returned) */
{
    char *sdec;
    char csign;
    int idec, idm, ids, ira, irm, irs;

    /* Check first character */
    if (string[0] != 'J' && string[0] != 'j')
	return (0);

    /* Find declination sign */
    sdec = strsrch (string, "-");
    if (sdec == NULL)
	sdec = strsrch (string,"+");
    if (sdec == NULL)
	return (0);

    /* Parse right ascension */
    csign = *sdec;
    *sdec = (char) 0;
    ira = atoi (string+1);
    irs = ira % 10000;
    ira = ira / 10000;
    irm = ira % 100;
    ira = ira / 100;
    *ra = (double) ira + ((double) irm) / 60.0 + ((double) irs) / 360000.0;
    *ra = *ra * 15.0;

    /* Parse declination */
    idec = atoi (sdec+1);
    ids = idec % 1000;
    idec = idec / 1000;
    idm = idec % 100;
    idec = idec / 100;
    *dec = (double) idec + ((double) idm) / 60.0 + ((double) ids) / 36000.0;
    return (1);
}

int
vothead (refcat, refcatname, mprop, typecol, ns, cra, cdec, drad)

int	refcat;		/* Catalog code */
char	*refcatname;	/* Name of catalog */
int	mprop;		/* Proper motion flag */
int	typecol;	/* Flag for spectral type */
int	ns;		/* Number of sources found in catalog */
double	cra;		/* Search center right ascension */
double	cdec;		/* Search center declination */
double	drad;		/* Radius to search in degrees */

{
    char *catalog = CatName (refcat, refcatname);
    int nf = 0;

    printf ("<!DOCTYPE VOTABLE SYSTEM \"http://us-vo.org/xml/VOTable.dtd\">\n");
    printf ("<VOTABLE version=\"v1.1\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n");
    printf ("xsi:noNamespaceSchemaLocation=\"http://www.ivoa.net/xml/VOTable/VOTable/v1.1\">\n");
    printf (" <DESCRIPTION>SAO/TDC %s Cone Search Response</DESCRIPTION>\n", catalog);
    printf ("  <DEFINITIONS>\n");
    printf ("   <COOSYS  ID=\"J2000\" equinox=\"2000.0\" epoch=\"2000.0\" system=\"ICRS\" >\n");
    printf ("  </COOSYS>\n");
    printf ("  </DEFINITIONS>\n");
    printf ("  <RESOURCE>\n");
    printf ("   <TABLE name=\"results\">\n");
    printf ("    <DESCRIPTION>\n");
    printf ("     %d objects within %.6f degrees of ra=%010.6f dec=%09.6f\n",
	    ns, drad, cra, cdec);
    printf ("    </DESCRIPTION>\n");
    printf ("<FIELD ucd=\"ID_MAIN\" datatype=\"char\" name=\"Catalog Name\">\n");
    if (refcat == USAC || refcat == USA1 || refcat == USA2 ||
	refcat == UAC  || refcat == UA1  || refcat == UA2 || refcat == UB1)
	printf ("  <DESCRIPTION>USNO Object Identifier</DESCRIPTION>\n");
    else if (refcat == TYCHO2)
	printf ("  <DESCRIPTION>Tycho-2 Object Identifier</DESCRIPTION>\n");
    else if (refcat == GSC2)
	printf ("  <DESCRIPTION>GSC II Object Identifier</DESCRIPTION>\n");
    else if (refcat == TMPSC)
	printf ("  <DESCRIPTION>2MASS Point Source Identifier</DESCRIPTION>\n");
    else if (refcat == GSC || refcat == GSCACT)
	printf ("  <DESCRIPTION>GSC Object Identifier</DESCRIPTION>\n");
    else if (refcat == SAO)
	printf ("  <DESCRIPTION>SAO Catalog Number</DESCRIPTION>\n");
    else if (refcat == PPM)
	printf ("  <DESCRIPTION>PPM Catalog Number</DESCRIPTION>\n");
    else
	printf ("  <DESCRIPTION>Object Identifier</DESCRIPTION>\n");
    printf ("</FIELD>\n");

    printf ("<FIELD ucd=\"POS_EQ_RA_MAIN\" datatype=\"float\" name=\"RA\" unit=\"degrees\" ref=\"J2000\">\n");
    printf ("  <DESCRIPTION>Right Ascension of Object (J2000)</DESCRIPTION>\n");
    printf ("</FIELD>\n");

    printf ("<FIELD ucd=\"POS_EQ_DEC_MAIN\" datatype=\"float\" name=\"DEC\" unit=\"degrees\" ref=\"J2000\">\n");
    printf ("   <DESCRIPTION>Declination of Object (J2000)</DESCRIPTION>\n");
    printf ("</FIELD>\n");

    if (refcat == USAC || refcat == USA1 || refcat == USA2 ||
	refcat == UAC  || refcat == UA1  || refcat == UA2) {
	printf ("<FIELD ucd=\"PHOT_PHG_B\" datatype=\"float\" name=\"B Magnitude\" unit=\"mag\">\n");
	printf ("  <DESCRIPTION>Photographic B Magnitude of Object</DESCRIPTION>\n");
	printf ("</FIELD>\n");
	printf ("<FIELD ucd=\"PHOT_PHG_R\" datatype=\"float\" name=\"R Magnitude\" unit=\"mag\">\n");
	printf ("  <DESCRIPTION>Photographic R Magnitude of Object</DESCRIPTION>\n");
	printf ("</FIELD>\n");
	printf ("<FIELD ucd=\"INST_PLATE_NUMBER\" datatype=\"int\" name=\"PlateID\">\n");
	printf ("  <DESCRIPTION>USNO Plate ID of star</DESCRIPTION>\n");
	printf ("</FIELD>\n");
 nf = 7;
 }
    else if (refcat == TYCHO2) {
	printf ("<FIELD name=\"BTmag\" ucd=\"PHOT_TYCHO_B\" datatype=\"float\" unit=\"mag\">\n");
	printf ("  <DESCRIPTION> Tycho-2 BT magnitude </DESCRIPTION>\n");
	printf ("</FIELD>\n");
	printf ("<FIELD name=\"VTmag\" ucd=\"PHOT_TYCHO_V\" datatype=\"float\" unit=\"mag\">\n");
	printf ("  <DESCRIPTION> Tycho-2 VT magnitude </DESCRIPTION>\n");
 nf = 8;
	}
    else if (refcat == GSC || refcat == GSCACT) {
	printf ("<FIELD name=\"Vmag\" ucd=\"PHOT_GSC_V\" datatype=\"float\" unit=\"mag\">\n");
	printf ("  <DESCRIPTION> GSC V magnitude </DESCRIPTION>\n");
	printf ("</FIELD>\n");
 nf = 8;
	}
    else if (refcat == GSC2) {
	}
    else if (refcat == TMPSC) {
	printf ("<FIELD name=\"Jmag\" ucd=\"PHOT_MAG_J\" datatype=\"float\" unit=\"mag\">\n");
	printf ("  <DESCRIPTION> Johnson J magnitude </DESCRIPTION>\n");
	printf ("</FIELD>\n");
	printf ("<FIELD name=\"Hmag\" ucd=\"PHOT_MAG_H\" datatype=\"float\" unit=\"mag\">\n");
	printf ("  <DESCRIPTION> Johnson H magnitude </DESCRIPTION>\n");
	printf ("</FIELD>\n");
	printf ("<FIELD name=\"Kmag\" ucd=\"PHOT_MAG_K\" datatype=\"float\" unit=\"mag\">\n");
	printf ("  <DESCRIPTION> Johnson K magnitude </DESCRIPTION>\n");
	printf ("</FIELD>\n");
 nf = 7;
	}
    else if (refcat == SAO) {
	printf ("<FIELD name=\"Vmag\" ucd=\"PHOT_MAG_V\" datatype=\"float\" unit=\"mag\">\n");
	printf ("  <DESCRIPTION> SAO Catalog V magnitude (7)</DESCRIPTION>\n");
	printf ("</FIELD>\n");
 nf = 8;
	} 
    else if (refcat == PPM) {
	printf ("<FIELD name=\"Vmag\" ucd=\"PHOT_MAG_V\" datatype=\"float\" unit=\"mag\">\n");
	printf ("  <DESCRIPTION> PPM Catalog V magnitude (7)</DESCRIPTION>\n");
	printf ("</FIELD>\n");
 nf = 8;
	} 
    if (typecol == 1) {
	printf ("<FIELD ucd=\"SPECT_TYPE_GENERAL\" name=\"Spectral Type\">\n");
	printf ("  <DESCRIPTION>Spectral Type from catalog</DESCRIPTION>\n");
	printf ("</FIELD>\n");
	}
    printf ("<FIELD ucd=\"POS_ANG_DIST_GENERAL\" datatype=\"float\" name=\"Offset\" unit=\"degrees\">\n");
    printf ("  <DESCRIPTION>Radial distance from requested position</DESCRIPTION>\n");
    printf ("</FIELD>\n");
    printf ("<DATA> <TABLEDATA>\n");

    return (nf);
}


void
vottail ()
{
    printf ("        </TABLEDATA> </DATA>\n");
    printf ("      </TABLE>\n");
    printf ("    </RESOURCE>\n");
    printf ("</VOTABLE>\n");
    return;
}


/*    Polynomial least squares fitting program, almost identical to the
 *    one in Bevington, "Data Reduction and Error Analysis for the
 *    Physical Sciences," page 141.  The argument list was changed and
 *    the weighting removed.
 *      y = a(1) + a(2)*(x-x0) + a(3)*(x-x0)**2 + a(3)*(x-x0)**3 + . . .
 */

static double determ();

void
polfit (x, y, x0, npts, nterms, a, stdev)

double	*x;		/* Array of independent variable points */
double	*y;		/* Array of dependent variable points */
double	x0;		/* Offset to independent variable */
int	npts;		/* Number of data points to fit */
int	nterms;		/* Number of parameters to fit */
double	*a;		/* Vector containing current fit values */
double	*stdev; 	/* Standard deviation of fit (returned) */
{
    double sigma2sum;
    double xterm,yterm,xi,yi;
    double *sumx, *sumy;
    double *array;
    int i,j,k,l,n,nmax;
    double delta;

    /* accumulate weighted sums */
    nmax = 2 * nterms - 1;
    sumx = (double *) calloc (nmax, sizeof(double));
    sumy = (double *) calloc (nterms, sizeof(double));
    for (n = 0; n < nmax; n++)
	sumx[n] = 0.0;
    for (j = 0; j < nterms; j++)
	sumy[j] = 0.0;
    for (i = 0; i < npts; i++) {
	xi = x[i] - x0;
	yi = y[i];
	xterm = 1.0;
	for (n = 0; n < nmax; n++) {
	    sumx[n] = sumx[n] + xterm;
	    xterm = xterm * xi;
	    }
	yterm = yi;
	for (n = 0; n < nterms; n++) {
	    sumy[n] = sumy[n] + yterm;
	    yterm = yterm * xi;
	    }
	}

    /* Construct matrices and calculate coeffients */
    array = (double *) calloc (nterms*nterms, sizeof(double));
    for (j = 0; j < nterms; j++) {
	for (k = 0; k < nterms; k++) {
	    n = j + k;
	    array[j+k*nterms] = sumx[n];
	    }
	}
    delta = determ (array, nterms);
    if (delta == 0.0) {
	*stdev = 0.;
	for (j = 0; j < nterms; j++)
	    a[j] = 0. ;
	free (array);
	free (sumx);
	free (sumy);
	return;
	}

    for (l = 0; l < nterms; l++) {
	for (j = 0; j < nterms; j++) {
	    for (k = 0; k < nterms; k++) {
		n = j + k;
		array[j+k*nterms] = sumx[n];
		}
	    array[j+l*nterms] = sumy[j];
	    }
	a[l] = determ (array, nterms) / delta;
	}

    /* Calculate sigma */
    sigma2sum = 0.0;
    for (i = 0; i < npts; i++) {
	yi = polcomp (x[i], x0, nterms, a);
	sigma2sum = sigma2sum + ((y[i] - yi) * (y[i] - yi));
	}
    *stdev = sqrt (sigma2sum / (double) (npts - 1));

    free (array);
    free (sumx);
    free (sumy);
    return;
}


/*--- Calculate the determinant of a square matrix
 *    This subprogram destroys the input matrix array
 *    From Bevington, page 294.
 */

static double
determ (array, norder)

double	*array;		/* Input matrix array */
int	norder;		/* Order of determinant (degree of matrix) */

{
    double save, det;
    int i,j,k,k1, zero;

    det = 1.0;
    for (k = 0; k < norder; k++) {

	/* Interchange columns if diagonal element is zero */
	if (array[k+k*norder] == 0) {
	    zero = 1;
	    for (j = k; j < norder; j++) {
		if (array[k+j*norder] != 0.0)
		    zero = 0;
		}
	    if (zero)
		return (0.0);

	    for (i = k; i < norder; i++) {
		save = array[i+j*norder]; 
		array[i+j*norder] = array[i+k*norder];
		array[i+k*norder] = save ;
		}
	    det = -det;
	    }

	/* Subtract row k from lower rows to get diagonal matrix */
	det = det * array[k+k*norder];
	if (k < norder - 1) {
	    k1 = k + 1;
	    for (i = k1; i < norder; i++) {
		for (j = k1; j < norder; j++) {
		    array[i+j*norder] = array[i+j*norder] -
				      (array[i+k*norder] * array[k+j*norder] /
				      array[k+k*norder]);
		    }
		}
	    }
	}
	return (det);
}

/* POLCOMP -- Polynomial evaluation
 *	Y = A(1) + A(2)*X + A(3)*X**2 + A(3)*X**3 + . . . */

double
polcomp (xi, x0, norder, a)

double	xi;	/* Independent variable */
double	x0;	/* Offset to independent variable */
int	norder;	/* Number of coefficients */
double	*a;	/* Vector containing coeffiecients */
{
    double xterm, x, y;
    int iterm;

    /* Accumulate polynomial value */
    x = xi - x0;
    y = 0.0;
    xterm = 1.0;
    for (iterm = 0; iterm < norder; iterm++) {
	y = y + a[iterm] * xterm;
	xterm = xterm + x;
	}
    return (y);
}

/* MOVEB -- Copy nbytes bytes from source+offs to dest+offd (any data type) */

void
movebuff (source, dest, nbytes, offs, offd)

char *source;	/* Pointer to source */
char *dest;	/* Pointer to destination */
int nbytes;	/* Number of bytes to move */
int offs;	/* Offset in bytes in source from which to start copying */
int offd;	/* Offset in bytes in destination to which to start copying */
{
char *from, *last, *to;
        from = source + offs;
        to = dest + offd;
        last = from + nbytes;
        while (from < last) *(to++) = *(from++);
        return;
}

/* Mar  2 1998	Make number and second magnitude optional
 * Oct 21 1998	Add RefCat() to set reference catalog code
 * Oct 26 1998	Include object names in star catalog entry structure
 * Oct 29 1998	Return coordinate system and title from RefCat
 * Nov 20 1998	Add USNO A-2.0 catalog and return different code
 * Dec  9 1998	Add Hipparcos and Tycho catalogs
 *
 * Jan 26 1999	Add subroutines to deal with ranges of numbers
 * Feb  8 1999	Fix bug initializing ACT catalog
 * Feb 11 1999	Change starcat.insys to starcat.coorsys
 * May 19 1999	Separate catalog subroutines into separate file
 * May 19 1999	Add CatNum() to return properly formatted catalog number
 * May 20 1999	Add date/time conversion subroutines translated from Fortran
 * May 28 1999	Fix bug in CatNum() which omitted GSC
 * Jun  3 1999	Add return to CatNum()
 * Jun  3 1999	Add CatNumLen()
 * Jun 16 1999	Add SearchLim(), used by all catalog search subroutines
 * Jun 30 1999	Add isrange() to check to see whether a string is a range
 * Jul  1 1999	Move date and time utilities to dateutil.c
 * Jul 15 1999	Add getfilebuff()
 * Jul 23 1999	Add Bright Star Catalog
 * Aug 16 1999	Add RefLim() to set catalog search limits
 * Sep 21 1999	In isrange(), check for x
 * Oct  5 1999	Add setoken(), nextoken(), and getoken()
 * Oct 15 1999	Fix format eror in error message
 * Oct 20 1999	Use strchr() in range decoding
 * Oct 21 1999	Fix declarations after lint
 * Oct 21 1999	Fix arguments to catopen() and catclose() after lint
 * Nov  3 1999	Fix bug which lost last character on a line in getoken
 * Dec  9 1999	Add next_token(); set pointer to next token in first_token
 *
 * Jan 11 2000	Use nndec for Starbase files, too
 * Feb 10 2000	Read coordinate system, epoch, and equinox from Starbase files
 * Mar  1 2000	Add isfile() to tell whether string is name of readable file
 * Mar  1 2000	Add agets() to return value from keyword = value in string
 * Mar  1 2000	Add isfile() to tell if a string is the name of a readable file
 * Mar  1 2000	Add agets() to read a parameter from a comment line of a file
 * Mar  8 2000	Add ProgCat() to return catalog flag from program name
 * Mar 13 2000	Add PropCat() to return whether catalog has proper motions
 * Mar 27 2000	Clean up code after lint
 * May 22 2000	Add bv2sp() to approximate main sequence spectral type from B-V
 * May 25 2000	Add Tycho 2 catalog
 * May 26 2000	Add field size argument to CatNum() and CatNumLen()
 * Jun  2 2000	Set proper motion for all catalog types in RefCat()
 * Jun 26 2000	Add XY image coordinate system
 * Jul 26 2000	Include math.h to get strtod() on SunOS machines
 * Aug  2 2000	Allow up to 14 digits in catalog IDs
 * Sep  1 2000	Add option in CatNum to print leading zeroes if nnfld > 0
 * Sep 22 2000	Add br2sp() to approximate main sequence spectral type from B-R
 * Oct 24 2000	Add USNO option to RefCat()
 * Nov 21 2000	Clean up logic in RefCat()
 * Nov 28 2000	Try PPMra and SAOra in RefCat() as well as PPM and SAO
 * Dec 13 2000	Add StrNdec() to get number of decimal places in star numbers
 *
 * Jan 17 2001	Add vertical bar (|) as column separator
 * Feb 28 2001	Separate .usno stars from usa stars
 * Mar  1 2001	Add CatName()
 * Mar 19 2001	Fix setting of ra-sorted PPM catalog in RefCat()
 * Mar 27 2001	Add option to omit leading spaces in CatNum()
 * May  8 2001	Fix bug in setokens() which failed to deal with quoted tokens
 * May 18 2001	Fix bug in setokens() which returned on ntok < maxtok
 * May 22 2001	Add GSC-ACT catalog
 * May 24 2001	Add 2MASS Point Source Catalog
 * Jun  7 2001	Return proper motion flag and number of magnitudes from RefCat()
 * Jun 13 2001	Fix rounding problem in rgetr8()
 * Jun 13 2001	Use strncasecmp() instead of two calls to strncmp() in RefCat()
 * Jun 15 2001	Add CatName() and CatID()
 * Jun 18 2001	Add maximum length of returned string to getoken(), nextoken()
 * Jun 18 2001	Pad returned string in getoken(), nextoken()
 * Jun 19 2001	Treat "bar" like "tab" as special single character terminator
 * Jun 19 2001	Allow tab table options for named catalogs in RefCat()
 * Jun 19 2001	Change number format to integer for Hipparcos catalog
 * Jun 19 2001	Add refcatname as argument to CatName()
 * Jun 20 2001	Add GSC II
 * Jun 25 2001	Fix GSC II number padding
 * Aug 20 2001	Add NumNdec() and guess number of decimal places if needed
 * Sep 20 2001	Add CatMagName()
 * Sep 25 2001	Move isfile() to fileutil.c
 *
 * Feb 26 2002	Fix agets() to work with keywords at start of line
 * Feb 26 2002	Add option in agets() to return value to end of line or /
 * Mar 25 2002	Fix bug in agets() to find second occurence of string 
 * Apr 10 2002	Add CatMagNum() to translate single letters to mag sequence number
 * May 13 2002	In agets(), allow arbitrary number of spaces around : or =
 * Jun 10 2002	In isrange(), return 0 if string is null or empty
 * Aug  1 2002	In agets(), read through / if reading to end of line
 * Sep 18 2002	Add vothead() and vottail() for VOTable output from scat
 * Oct 26 2002	Fix bugs in vothead()
 *
 * Jan 23 2003	Add USNO-B1.0 Catalog
 * Jan 27 2003	Adjust dra in RefLimit to max width in RA seconds in region
 * Mar 10 2003	Clean up RefLim() to better represent region to be searched
 * Mar 24 2003	Add CatCode() to separate catalog type from catalog parameters
 * Apr 14 2003	Add setrevmsg() and getrevmsg()
 * Apr 24 2003	Add UCAC1 Catalog
 * Apr 24 2003	Return 5 magnitudes for GSC II, including epoch
 * Apr 24 2003	Fix bug dealing with HST GSC
 * May 21 2003	Add TMIDR2=2MASS IDR2, and new 2MASS=TMPSC
 * May 28 2003	Fix bug checking for TMIDR2=2MASS IDR2; 11 digits for TMPSC
 * May 30 2003	Add UCAC2 catalog
 * Sep 19 2003	Fix bug which shrank search width in RefLim()
 * Sep 26 2003	In RefLim() do not use cos(90)
 * Sep 29 2003	Add proper motion margins and wrap arguments to RefLim()
 * Oct  1 2003	Add code in RefLim() for all-sky images
 * Oct  6 2003	Add code in RefLim() to cover near-polar searches
 * Dec  4 2003	Implement GSC 2.3 and USNO-YB6
 * Dec 15 2003	Set refcat to 0 if no catalog name and refcatname to NULL
 *
 * Jan  5 2004	Add SDSS catalog
 * Jan 12 2004	Add 2MASS Extended Source Catalog
 * Jan 14 2004	Add CatSource()
 * Jan 22 2004	Add global flag degout to print limits in degrees
 *
 * May 12 2005	Add tmcid() to decode 2MASS ID strings
 * May 18 2005	Change Tycho-2 magnitudes to include B and V errors
 * Jul 27 2005	Add DateString() to convert epoch to desired format
 * Aug  2 2005	Fix setoken() to deal with whitespace before end of line
 * Aug  2 2005	Use static maxtokens set to header MAXTOKENS
 * Aug  5 2005	Add code to support magnitude errors in Tycho2 and 2MASS PSC
 * Aug 11 2005	Add setdateform() so date can be formatted anywhere
 * Aug 11 2005	Add full FITS ISO date as EP_ISO
 * Aug 16 2005	Make all string matches case-independent
 *
 * Mar 15 2006	Clean up VOTable code
 * Mar 17 2006	Return number of fields from vothead()
 * Apr  7 2006	Keep quoted character strings together as a single token
 * Jun  6 2006	Add SKY2000 catalog for wide fields
 * Jun 20 2006	In CatSource() increase catalog descriptor from 32 to 64 chars
 *
 * Jan 10 2007	Add polynomial fitting subroutines from polfit.c
 * Jan 11 2007	Move token access subroutines to fileutil.c
 * Mar 13 2007	Set title accordingly for gsc22 and gsc23 and gsc2 options
 * Jul  8 2007	Set up 8 magnitudes for GSC 2.3 from GALEX
 * Jul 13 2007	Add SkyBot solar system object search
 * Nov 28 2006	Add moveb() from binread.c
 *
 * Aug 19 2009	If pole is included, set RA range to 360 degrees in RefLim()
 * Sep 25 2009	Change name of moveb() to movebuff()
 * Sep 28 2009	For 2MASS Extended Source catalog, use 2mx_id, not 2mass_id
 * Sep 30 2009	Add UCAC3 catalog
 * Oct 26 2009	Do not wrap in RefLim() if dra=360
 * Nov  6 2009	Add UCAC3 catalog to ProgCat()
 * Nov 13 2009	Add UCAC3 and UCAC2 to CatMagName() and CatMagNum()
 */
