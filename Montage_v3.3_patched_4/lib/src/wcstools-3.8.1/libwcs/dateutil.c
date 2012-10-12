/*** File libwcs/dateutil.c
 *** September 24, 2009
 *** By Doug Mink, dmink@cfa.harvard.edu
 *** Harvard-Smithsonian Center for Astrophysics
 *** Copyright (C) 1999-2009
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

/* Date and time conversion routines using the following conventions:
  ang = Angle in fractional degrees
  deg = Angle in degrees as dd:mm:ss.ss
  doy = 2 floating point numbers: year and day, including fraction, of year
	*** First day of year is 1, not zero.
   dt = 2 floating point numbers: yyyy.mmdd, hh.mmssssss
   ep = fractional year, often epoch of a position including proper motion
  epb = Besselian epoch = 365.242198781-day years based on 1900.0
  epj = Julian epoch = 365.25-day years based on 2000.0
   fd = FITS date string which may be any of the following:
	yyyy.ffff (fractional year)
	dd/mm/yy (FITS standard before 2000)
	dd-mm-yy (nonstandard FITS use before 2000)
	yyyy-mm-dd (FITS standard after 1999)
	yyyy-mm-ddThh:mm:ss.ss (FITS standard after 1999)
   hr = Sexigesimal hours as hh:mm:dd.ss
   jd = Julian Date
   lt = Local time
  mjd = modified Julian Date = JD - 2400000.5
  ofd = FITS date string (dd/mm/yy before 2000, else no return)
 time = use fd2* with no date to convert time as hh:mm:ss.ss to sec, day, year
   ts = UT seconds since 1950-01-01T00:00 (used for ephemeris computations)
  tsi = local seconds since 1980-01-01T00:00 (used by IRAF as a time tag)
  tsu = UT seconds since 1970-01-01T00:00 (used as Unix system time)
  tsd = UT seconds of current day
   ut = Universal Time (UTC)
   et = Ephemeris Time (or TDB or TT)
  mst = Mean Greenwich Sidereal Time
  gst = Greenwich Sidereal Time (includes nutation)
  lst = Local Sidereal Time (includes nutation) (longitude must be set)
  hjd = Heliocentric Julian Date
 mhjd = modified Heliocentric Julian Date = HJD - 2400000.5

 * ang2hr (angle)
 *	Convert angle in decimal floating point degrees to hours as hh:mm:ss.ss
 * ang2deg (angle)
 *	Convert angle in decimal floating point degrees to degrees as dd:mm:ss.ss
 * deg2ang (angle as dd:mm:ss.ss)
 *	Convert angle in degrees as dd:mm:ss.ss to decimal floating point degrees
 * ang2hr (angle)
 *	Convert angle in hours as hh:mm:ss.ss to decimal floating point degrees
 *
 * doy2dt (year, doy, date, time)
 *	Convert year and day of year to date as yyyy.ddmm and time as hh.mmsss
 * doy2ep, doy2epb, doy2epj (date, time)
 *	Convert year and day of year to fractional year
 * doy2fd (year, doy)
 *	Convert year and day of year to FITS date string
 * doy2mjd (year, doy)
 *	Convert year and day of year to modified Julian date
 *
 * dt2doy (date, time, year, doy)
 *	Convert date as yyyy.ddmm and time as hh.mmsss to year and day of year
 * dt2ep, dt2epb, dt2epj (date, time)
 *	Convert date as yyyy.ddmm and time as hh.mmsss to fractional year
 * dt2fd (date, time)
 *	Convert date as yyyy.ddmm and time as hh.mmsss to FITS date string
 * dt2i (date,time,iyr,imon,iday,ihr,imn,sec, ndsec)
 *	Convert yyyy.mmdd hh.mmssss to year month day hours minutes seconds
 * dt2jd (date,time)
 *	Convert date as yyyy.ddmm and time as hh.mmsss to Julian date
 * dt2mjd (date,time)
 *	Convert date as yyyy.ddmm and time as hh.mmsss to modified Julian date
 * dt2ts (date,time)
 *	Convert date (yyyy.ddmm) and time (hh.mmsss) to seconds since 1950-01-01
 * dt2tsi (date,time)
 *	Convert date (yyyy.ddmm) and time (hh.mmsss) to seconds since 1980-01-01
 * dt2tsu (date,time)
 *	Convert date (yyyy.ddmm) and time (hh.mmsss) to seconds since 1970-01-01
 *
 * ep2dt, epb2dt, epj2dt (epoch,date, time)
 *	Convert fractional year to date as yyyy.ddmm and time as hh.mmsss
 * ep2fd, epb2fd, epj2fd (epoch)
 *	Convert epoch to FITS ISO date string
 * ep2i, epb2i, epj2i (epoch,iyr,imon,iday,ihr,imn,sec, ndsec)
 *	Convert fractional year to year month day hours minutes seconds
 * ep2jd, epb2jd, epj2jd (epoch)
 *	Convert fractional year as used in epoch to Julian date
 * ep2mjd, epb2mjd, epj2mjd (epoch)
 *	Convert fractional year as used in epoch to modified Julian date
 * ep2ts, epb2ts, epj2ts (epoch)
 *	Convert fractional year to seconds since 1950.0
 *
 * et2fd (string)
 *	Convert from ET (or TDT or TT) in FITS format to UT in FITS format
 * fd2et (string)
 *	Convert from UT in FITS format to ET (or TDT or TT) in FITS format
 * jd2jed (dj)
 *	Convert from Julian Date to Julian Ephemeris Date
 * jed2jd (dj)
 *	Convert from Julian Ephemeris Date to Julian Date
 * dt2et (date, time)
 *	Convert date (yyyy.ddmm) and time (hh.mmsss) to ephemeris time
 * edt2dt (date, time)
 *	Convert ephemeris date (yyyy.ddmm) and time (hh.mmsss) to UT
 * ts2ets (tsec)
 *	Convert from UT in seconds since 1950-01-01 to ET in same format
 * ets2ts (tsec)
 *	Convert from ET in seconds since 1950-01-01 to UT in same format
 *
 * fd2ep, fd2epb, fd2epj (string)
 *	Convert FITS date string to fractional year
 *	Convert time alone to fraction of Besselian year
 * fd2doy (string, year, doy)
 *	Convert FITS standard date string to year and day of year
 * fd2dt (string, date, time)
 *	Convert FITS date string to date as yyyy.ddmm and time as hh.mmsss
 *	Convert time alone to hh.mmssss with date set to 0.0
 * fd2i (string,iyr,imon,iday,ihr,imn,sec, ndsec)
 *	Convert FITS standard date string to year month day hours min sec
 *	Convert time alone to hours min sec, year month day are zero
 * fd2jd (string)
 *	Convert FITS standard date string to Julian date
 *	Convert time alone to fraction of day
 * fd2mjd (string)
 *	Convert FITS standard date string to modified Julian date
 * fd2ts (string)
 *	Convert FITS standard date string to seconds since 1950.0
 *	Convert time alone to seconds of day
 * fd2fd (string)
 *	Convert FITS standard date string to ISO FITS date string
 * fd2of (string)
 *	Convert FITS standard date string to old-format FITS date and time
 * fd2ofd (string)
 *	Convert FITS standard date string to old-format FITS date string
 * fd2oft (string)
 *	Convert time part of FITS standard date string to FITS date string
 *
 * jd2doy (dj, year, doy)
 *	Convert Julian date to year and day of year
 * jd2dt (dj,date,time)
 *	Convert Julian date to date as yyyy.mmdd and time as hh.mmssss
 * jd2ep, jd2epb, jd2epj (dj)
 *	Convert Julian date to fractional year as used in epoch
 * jd2fd (dj)
 *	Convert Julian date to FITS ISO date string
 * jd2i (dj,iyr,imon,iday,ihr,imn,sec, ndsec)
 *	Convert Julian date to year month day hours min sec
 * jd2mjd (dj)
 *	Convert Julian date to modified Julian date
 * jd2ts (dj)
 *	Convert Julian day to seconds since 1950.0
 *
 * lt2dt()
 *	Return local time as yyyy.mmdd and time as hh.mmssss
 * lt2fd()
 *	Return local time as FITS ISO date string
 * lt2tsi()
 *	Return local time as IRAF seconds since 1980-01-01 00:00
 * lt2tsu()
 *	Return local time as Unix seconds since 1970-01-01 00:00
 * lt2ts()
 *	Return local time as Unix seconds since 1950-01-01 00:00
 *
 * mjd2doy (dj,year,doy)
 *	Convert modified Julian date to date as year and day of year
 * mjd2dt (dj,date,time)
 *	Convert modified Julian date to date as yyyy.mmdd and time as hh.mmssss
 * mjd2ep, mjd2epb, mjd2epj (dj)
 *	Convert modified Julian date to fractional year as used in epoch
 * mjd2fd (dj)
 *	Convert modified Julian date to FITS ISO date string
 * mjd2i (dj,iyr,imon,iday,ihr,imn,sec, ndsec)
 *	Convert modified Julian date to year month day hours min sec
 * mjd2jd (dj)
 *	Convert modified Julian date to Julian date
 * mjd2ts (dj)
 *	Convert modified Julian day to seconds since 1950.0
 *
 * ts2dt (tsec,date,time)
 *	Convert seconds since 1950.0 to date as yyyy.ddmm and time as hh.mmsss
 * ts2ep, ts2epb, ts2epj (tsec)
 *	Convert seconds since 1950.0 to fractional year
 * ts2fd (tsec)
 *	Convert seconds since 1950.0 to FITS standard date string
 * ts2i (tsec,iyr,imon,iday,ihr,imn,sec, ndsec)
 *	Convert sec since 1950.0 to year month day hours minutes seconds
 * ts2jd (tsec)
 *	Convert seconds since 1950.0 to Julian date
 * ts2mjd (tsec)
 *	Convert seconds since 1950.0 to modified Julian date
 * tsi2fd (tsec)
 *	Convert seconds since 1980-01-01 to FITS standard date string
 * tsi2dt (tsec,date,time)
 *	Convert seconds since 1980-01-01 to date as yyyy.ddmm, time as hh.mmsss
 * tsu2fd (tsec)
 *	Convert seconds since 1970-01-01 to FITS standard date string
 * tsu2tsi (tsec)
 *	Convert UT seconds since 1970-01-01 to local seconds since 1980-01-01
 * tsu2dt (tsec,date,time)
 *	Convert seconds since 1970-01-01 to date as yyyy.ddmm, time as hh.mmsss
 *
 * tsd2fd (tsec)
 *	Convert seconds since start of day to FITS time, hh:mm:ss.ss
 * tsd2dt (tsec)
 *	Convert seconds since start of day to hh.mmssss
 *
 * fd2gst (string)
 *      convert from FITS date Greenwich Sidereal Time
 * dt2gst (date, time)
 *      convert from UT as yyyy.mmdd hh.mmssss to Greenwich Sidereal Time
 * ts2gst (tsec)
 *      Calculate Greenwich Sidereal Time given Universal Time
 *          in seconds since 1951-01-01T0:00:00
 * fd2mst (string)
 *      convert from FITS UT date to Mean Sidereal Time
 * dt2gmt (date, time)
 *      convert from UT as yyyy.mmdd hh.mmssss to Mean Sidereal Time
 * ts2mst (tsec)
 *      Calculate Mean Sidereal Time given Universal Time
 *          in seconds since 1951-01-01T0:00:00
 * jd2mst (string)
 *      convert from Julian Date to Mean Sidereal Time
 * mst2fd (string)
 *	convert to current UT in FITS format given Greenwich Mean Sidereal Time
 * mst2jd (dj)
 *	convert to current UT as Julian Date given Greenwich Mean Sidereal Time
 * jd2lst (dj)
 *	Calculate Local Sidereal Time from Julian Date
 * ts2lst (tsec)
 *	Calculate Local Sidereal Time given UT in seconds since 1951-01-01T0:00
 * fd2lst (string)
 *	Calculate Local Sidereal Time given Universal Time as FITS ISO date
 * lst2jd (dj, lst)
 *	Calculate Julian Date given current Julian date and Local Sidereal Time
 * lst2fd (string, lst)
 *	Calculate Julian Date given current UT date and Local Sidereal Time
 * gst2fd (string)
 * 	Calculate current UT given UT date and Greenwich Sidereal Time
 * gst2jd (dj)
 * 	Calculate current UT given UT date and Greenwich Sidereal Time as JD
 *
 * compnut (dj, dpsi, deps, eps0)
 *      Compute the longitude and obliquity components of nutation and
 *      mean obliquity from the IAU 1980 theory
 *
 * utdt (dj)
 *	Compute difference between UT and dynamical time (ET-UT)
 * ut2dt (year, doy)
 *	Current Universal Time to year and day of year
 * ut2dt (date, time)
 *	Current Universal Time to date (yyyy.mmdd) and time (hh.mmsss)
 * ut2ep(), ut2epb(), ut2epj()
 *	Current Universal Time to fractional year, Besselian, Julian epoch
 * ut2fd()
 *	Current Universal Time to FITS ISO date string
 * ut2jd()
 *	Current Universal Time to Julian Date
 * ut2mjd()
 *	Current Universal Time to Modified Julian Date
 * ut2tsi()
 *	Current Universal Time to IRAF seconds since 1980-01-01T00:00
 * ut2tsu()
 *	Current Universal Time to Unix seconds since 1970-01-01T00:00
 * ut2ts()
 *	Current Universal Time to seconds since 1950-01-01T00:00
 * isdate (string)
 *	Return 1 if string is a FITS date (old or ISO)
 *
 * Internally-used subroutines
 *
 * fixdate (iyr, imon, iday, ihr, imn, sec, ndsec)
 *	Round seconds and make sure date and time numbers are within limits
 * caldays (year, month)
 *	Calculate days in month 1-12 given year (Gregorian calendar only
 * dint (dnum)
 *	Return integer part of floating point number
 * dmod (dnum)
 *	Return Mod of floating point number
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <sys/time.h>
#include "wcs.h"
#include "fitsfile.h"

static double suntl();
static void fixdate();
static int caldays();
static double dint();
static double dmod();

static double longitude = 0.0;	/* longitude of observatory in degrees (+=west) */
void
setlongitude (longitude0)
double longitude0;
{ longitude = longitude0; return; }

static int ndec = 3;
void
setdatedec (nd)
int nd;
{ ndec = nd; return; }

/* ANG2HR -- Convert angle in fraction degrees to hours as hh:mm:ss.ss */

void
ang2hr (angle, lstr, string)

double	angle;	/* Angle in fractional degrees */
int	lstr;	/* Maximum number of characters in string */
char	*string; /* Character string (hh:mm:ss.ss returned) */

{
    angle = angle / 15.0;
    dec2str (string, lstr, angle, ndec);
    return;
}


/* ANG2DEG -- Convert angle in fraction degrees to degrees as dd:mm:ss.ss */

void
ang2deg (angle, lstr, string)

double	angle;	/* Angle in fractional degrees */
int	lstr;	/* Maximum number of characters in string */
char	*string; /* Character string (dd:mm:ss.ss returned) */
{
    dec2str (string, lstr, angle, ndec);
    return;
}


/* DEG2ANG -- Convert angle in degrees as dd:mm:ss.ss to fractional degrees */

double
deg2ang (angle)

char *angle;	/* Angle as dd:mm:ss.ss */
{
    double deg;

    deg = str2dec (angle);
    return (deg);
}

/* HR2ANG -- Convert angle in hours as hh:mm:ss.ss to fractional degrees */

double
hr2ang (angle)

char *angle;	/* Angle in sexigesimal hours (hh:mm:ss.sss) */

{
    double deg;

    deg = str2dec (angle);
    deg = deg * 15.0;
    return (deg);
}


/* DT2FD-- convert vigesimal date and time to FITS date, yyyy-mm-ddThh:mm:ss.ss */

char *
dt2fd (date, time)

double	date;	/* Date as yyyy.mmdd
		    yyyy = calendar year (e.g. 1973)
		    mm = calendar month (e.g. 04 = april)
		    dd = calendar day (e.g. 15) */
double	time;	/* Time as hh.mmssxxxx
		    *if time<0, it is time as -(fraction of a day)
		    hh = hour of day (0 .le. hh .le. 23)
		    nn = minutes (0 .le. nn .le. 59)
		    ss = seconds (0 .le. ss .le. 59)
		  xxxx = tenths of milliseconds (0 .le. xxxx .le. 9999) */
{
    int iyr,imon,iday,ihr,imn;
    double sec;
    int nf;
    char *string;
    char tstring[32], dstring[32];
    char outform[64];

    dt2i (date, time, &iyr,&imon,&iday,&ihr,&imn,&sec, ndec);

    /* Convert to ISO date format */
    string = (char *) calloc (32, sizeof (char));

    /* Make time string */
    if (time != 0.0 || ndec > 0) {
	if (ndec == 0)
	    nf = 2;
	else
	    nf = 3 + ndec;
	if (ndec > 0) {
	    sprintf (outform, "%%02d:%%02d:%%0%d.%df", nf, ndec);
	    sprintf (tstring, outform, ihr, imn, sec);
	    }
	else {
	    sprintf (outform, "%%02d:%%02d:%%0%dd", nf);
	    sprintf (tstring, outform, ihr, imn, (int)(sec+0.5));
	    }
	}

    /* Make date string */
    if (date != 0.0)
	sprintf (dstring, "%4d-%02d-%02d", iyr, imon, iday);

    /* Make FITS (ISO) date string */
    if (date == 0.0)
	strcpy (string, tstring);
    else if (time == 0.0 && ndec < 1)
	strcpy (string, dstring);
    else
	sprintf (string, "%sT%s", dstring, tstring);

    return (string);
}


/* DT2JD-- convert from date as yyyy.mmdd and time as hh.mmsss to Julian Date
 *	   Return fractional days if date is zero */

double
dt2jd (date,time)

double	date;	/* Date as yyyy.mmdd
		    yyyy = calendar year (e.g. 1973)
		    mm = calendar month (e.g. 04 = april)
		    dd = calendar day (e.g. 15) */
double	time;	/* Time as hh.mmssxxxx
		    *if time<0, it is time as -(fraction of a day)
		    hh = hour of day (0 .le. hh .le. 23)
		    nn = minutes (0 .le. nn .le. 59)
		    ss = seconds (0 .le. ss .le. 59)
		  xxxx = tenths of milliseconds (0 .le. xxxx .le. 9999) */
{
    double dj;		/* Julian date (returned) */
    double tsec;	/* seconds since 1950.0 */

    tsec = dt2ts (date, time);
    if (date == 0.0)
	dj = tsec / 86400.0;
    else
	dj = ts2jd (tsec);

    return (dj);
}


/* DT2MJD-- convert from date yyyy.mmdd time hh.mmsss to modified Julian Date
 *	   Return fractional days if date is zero */

double
dt2mjd (date,time)

double	date;	/* Date as yyyy.mmdd
		    yyyy = calendar year (e.g. 1973)
		    mm = calendar month (e.g. 04 = april)
		    dd = calendar day (e.g. 15) */
double	time;	/* Time as hh.mmssxxxx
		    *if time<0, it is time as -(fraction of a day)
		    hh = hour of day (0 .le. hh .le. 23)
		    nn = minutes (0 .le. nn .le. 59)
		    ss = seconds (0 .le. ss .le. 59)
		  xxxx = tenths of milliseconds (0 .le. xxxx .le. 9999) */
{
    double dj;		/* Modified Julian date (returned) */
    double tsec;	/* seconds since 1950.0 */

    tsec = dt2ts (date, time);
    if (date == 0.0)
	dj = tsec / 86400.0;
    else
	dj = ts2jd (tsec);

    return (dj - 2400000.5);
}


/* HJD2JD-- convert  Heliocentric Julian Date to (geocentric) Julian date */

double
hjd2jd (dj, ra, dec, sys)

double	dj;	/* Heliocentric Julian date */
double	ra;	/* Right ascension (degrees) */
double	dec;	/* Declination (degrees) */
int	sys;	/* J2000, B1950, GALACTIC, ECLIPTIC */
{
    double lt;		/* Light travel difference to the Sun (days) */

    lt = suntl (dj, ra, dec, sys);

    /* Return Heliocentric Julian Date */
    return (dj - lt);
}


/* JD2HJD-- convert (geocentric) Julian date to Heliocentric Julian Date */

double
jd2hjd (dj, ra, dec, sys)

double	dj;	/* Julian date (geocentric) */
double	ra;	/* Right ascension (degrees) */
double	dec;	/* Declination (degrees) */
int	sys;	/* J2000, B1950, GALACTIC, ECLIPTIC */
{
    double lt;		/* Light travel difference to the Sun (days) */

    lt = suntl (dj, ra, dec, sys);

    /* Return Heliocentric Julian Date */
    return (dj + lt);
}


/* MHJD2MJD-- convert modified Heliocentric Julian Date to
	      modified geocentric Julian date */

double
mhjd2mjd (mhjd, ra, dec, sys)

double	mhjd;	/* Modified Heliocentric Julian date */
double	ra;	/* Right ascension (degrees) */
double	dec;	/* Declination (degrees) */
int	sys;	/* J2000, B1950, GALACTIC, ECLIPTIC */
{
    double lt;		/* Light travel difference to the Sun (days) */
    double hjd;		/* Heliocentric Julian date */

    hjd = mjd2jd (mhjd);

    lt = suntl (hjd, ra, dec, sys);

    /* Return Heliocentric Julian Date */
    return (jd2mjd (hjd - lt));
}


/* MJD2MHJD-- convert modified geocentric Julian date tp
	      modified Heliocentric Julian Date */

double
mjd2mhjd (mjd, ra, dec, sys)

double	mjd;	/* Julian date (geocentric) */
double	ra;	/* Right ascension (degrees) */
double	dec;	/* Declination (degrees) */
int	sys;	/* J2000, B1950, GALACTIC, ECLIPTIC */
{
    double lt;		/* Light travel difference to the Sun (days) */
    double	dj;	/* Julian date (geocentric) */

    dj = mjd2jd (mjd);

    lt = suntl (dj, ra, dec, sys);

    /* Return Heliocentric Julian Date */
    return (jd2mjd (dj + lt));
}


/* SUNTL-- compute light travel time to heliocentric correction in days */
/* Translated into C from IRAF SPP noao.astutils.asttools.asthjd.x */

static double
suntl (dj, ra, dec, sys)

double	dj;	/* Julian date (geocentric) */
double	ra;	/* Right ascension (degrees) */
double	dec;	/* Declination (degrees) */
int	sys;	/* J2000, B1950, GALACTIC, ECLIPTIC */
{
    double t;		/* Number of Julian centuries since J1900 */
    double manom;	/* Mean anomaly of the Earth's orbit (degrees) */
    double lperi;	/* Mean longitude of perihelion (degrees) */
    double oblq;	/* Mean obliquity of the ecliptic (degrees) */
    double eccen;	/* Eccentricity of the Earth's orbit (dimensionless) */
    double eccen2, eccen3;
    double tanom;	/* True anomaly (approximate formula) (radians) */
    double slong;	/* True longitude of the Sun from the Earth (radians) */
    double rs;		/* Distance to the sun (AU) */
    double lt;		/* Light travel difference to the Sun (days) */
    double l;		/* Longitude of star in orbital plane of Earth (radians) */
    double b;		/* Latitude of star in orbital plane of Earth (radians) */
    double epoch;	/* Epoch of obervation */
    double rs1,rs2;

    t = (dj - 2415020.0) / 36525.0;

    /* Compute earth orbital parameters */
    manom = 358.47583 + (t * (35999.04975 - t * (0.000150 + t * 0.000003)));
    lperi = 101.22083 + (t * (1.7191733 + t * (0.000453 + t * 0.000003)));
    oblq = 23.452294 - (t * (0.0130125 + t * (0.00000164 - t * 0.000000503)));
    eccen = 0.01675104 - (t * (0.00004180 + t * 0.000000126));
    eccen2 = eccen * eccen;
    eccen3 = eccen * eccen2;

    /* Convert to principle angles */
    manom = manom - (360.0 * (dint) (manom / 360.0));
    lperi = lperi - (360.0 * (dint) (lperi / 360.0));

    /* Convert to radians */
    manom = degrad (manom);
    lperi = degrad (lperi);
    oblq = degrad (oblq);

    /* True anomaly */
    tanom = manom + (2 * eccen - 0.25 * eccen3) * sin (manom) +
	    1.25 * eccen2 * sin (2 * manom) +
	    13./12. * eccen3 * sin (3 * manom);

    /* Distance to the Sun */
    rs1 = 1.0 - eccen2;
    rs2 = 1.0 + (eccen * cos (tanom));
    rs = rs1 / rs2;

    /* True longitude of the Sun seen from the Earth */
    slong = lperi + tanom + PI;

    /* Longitude and latitude of star in orbital plane of the Earth */
    epoch = jd2ep (dj);
    wcscon (sys, WCS_ECLIPTIC, 0.0, 0.0, &ra, &dec, epoch);
    l = degrad (ra);
    b = degrad (dec);

    /* Light travel difference to the Sun */
    lt = -0.005770 * rs * cos (b) * cos (l - slong);

    /* Return light travel difference */
    return (lt);
}


/* JD2DT-- convert Julian date to date as yyyy.mmdd and time as hh.mmssss */

void
jd2dt (dj,date,time)

double	dj;	/* Julian date */
double	*date;	/* Date as yyyy.mmdd (returned) */
double	*time;	/* Time as hh.mmssxxxx (returned) */
{
    int iyr,imon,iday,ihr,imn;
    double sec;

    /* Convert Julian Date to date and time */
    jd2i (dj, &iyr, &imon, &iday, &ihr, &imn, &sec, 4);

    /* Convert date to yyyy.mmdd */
    if (iyr < 0) {
	*date = (double) (-iyr) + 0.01 * (double) imon + 0.0001 * (double) iday;
	*date = -(*date);
	}
    else
	*date = (double) iyr + 0.01 * (double) imon + 0.0001 * (double) iday;

    /* Convert time to hh.mmssssss */
    *time = (double) ihr + 0.01 * (double) imn + 0.0001 * sec;

    return;
}


/* JD2I-- convert Julian date to date as year, month, and day, and time hours,
          minutes, and seconds */
/*        after Fliegel and Van Flander, CACM 11, 657 (1968) */


void
jd2i (dj, iyr, imon, iday, ihr, imn, sec, ndsec)

double	dj;	/* Julian date */
int	*iyr;	/* year (returned) */
int	*imon;	/* month (returned) */
int	*iday;	/* day (returned) */
int	*ihr;	/* hours (returned) */
int	*imn;	/* minutes (returned) */
double	*sec;	/* seconds (returned) */
int	ndsec;	/* Number of decimal places in seconds (0=int) */

{
    double tsec;
    double frac, dts, ts, sday;
    int jd, l, n, i, j;

    tsec = jd2ts (dj);
    /* ts2i (tsec, iyr, imon, iday, ihr, imn, sec, ndsec); */

    /* Round seconds to 0 - 4 decimal places */
    if (tsec < 0.0)
	dts = -0.5;
    else
	dts = 0.5;
    if (ndsec < 1)
	ts = dint (tsec + dts);
    else if (ndsec < 2)
	ts = dint (tsec * 10.0 + dts) / 10.0;
    else if (ndsec < 3)
	ts = dint (tsec * 100.0 + dts) / 100.0;
    else if (ndsec < 4)
	ts = dint (tsec * 1000.0 + dts) / 1000.0;
    else
	ts = dint (tsec * 10000.0 + dts) / 10000.0;

    /* Convert back to Julian Date */
    dj = ts2jd (ts);

    /* Compute time from fraction of a day */
    frac = dmod (dj, 1.0);
    if (frac < 0.5) {
	jd = (int) (dj - frac);
	sday = (frac + 0.5) * 86400.0;
	}
    else {
	jd = (int) (dj - frac) + 1;
	sday = (frac - 0.5) * 86400.0;
	}
    
    *ihr = (int) (sday / 3600.0);
    sday = sday - (double) (*ihr * 3600);
    *imn = (int) (sday / 60.0);
    *sec = sday - (double) (*imn * 60);

    /* Compute day, month, year */
    l = jd + 68569;
    n = (4 * l) / 146097;
    l = l - (146097 * n + 3) / 4;
    i = (4000 * (l + 1)) / 1461001;
    l = l - (1461 * i) / 4 + 31;
    j = (80 * l) / 2447;
    *iday = l - (2447 * j) / 80;
    l = j / 11;
    *imon = j + 2 - (12 * l);
    *iyr = 100 * (n - 49) + i + l;

    return;
}


/* JD2MJD-- convert Julian Date to Modified Julian Date */

double
jd2mjd (dj)

double	dj;	/* Julian Date */

{
    return (dj - 2400000.5);
}


/* JD2EP-- convert Julian date to fractional year as used in epoch */

double
jd2ep (dj)

double	dj;	/* Julian date */

{
    double date, time;
    jd2dt (dj, &date, &time);
    return (dt2ep (date, time));
}


/* JD2EPB-- convert Julian date to Besselian epoch */

double
jd2epb (dj)

double	dj;	/* Julian date */

{
    return (1900.0 + (dj - 2415020.31352) / 365.242198781);
}


/* JD2EPJ-- convert Julian date to Julian epoch */

double
jd2epj (dj)

double	dj;	/* Julian date */

{
    return (2000.0 + (dj - 2451545.0) / 365.25);
}


/* LT2DT-- Return local time as yyyy.mmdd and time as hh.mmssss */

void
lt2dt(date, time)

double	*date;	/* Date as yyyy.mmdd (returned) */
double	*time;	/* Time as hh.mmssxxxx (returned) */

{
    time_t tsec;
    struct timeval tp;
    struct timezone tzp;
    struct tm *ts;

    gettimeofday (&tp,&tzp);

    tsec = tp.tv_sec;
    ts = localtime (&tsec);

    if (ts->tm_year < 1000)
	*date = (double) (ts->tm_year + 1900);
    else
	*date = (double) ts->tm_year;
    *date = *date + (0.01 * (double) (ts->tm_mon + 1));
    *date = *date + (0.0001 * (double) ts->tm_mday);
    *time = (double) ts->tm_hour;
    *time = *time + (0.01 * (double) ts->tm_min);
    *time = *time + (0.0001 * (double) ts->tm_sec);

    return;
}


/* LT2FD-- Return current local time as FITS ISO date string */

char *
lt2fd()
{
    time_t tsec;
    struct tm *ts;
    struct timeval tp;
    struct timezone tzp;
    int month, day, year, hour, minute, second;
    char *isotime;

    gettimeofday (&tp,&tzp);
    tsec = tp.tv_sec;

    ts = localtime (&tsec);

    year = ts->tm_year;
    if (year < 1000)
	year = year + 1900;
    month = ts->tm_mon + 1;
    day = ts->tm_mday;
    hour = ts->tm_hour;
    minute = ts->tm_min;
    second = ts->tm_sec;

    isotime = (char *) calloc (32, sizeof (char));
    sprintf (isotime, "%04d-%02d-%02dT%02d:%02d:%02d",
                      year, month, day, hour, minute, second);
    return (isotime);
}


/* LT2TSI-- Return local time as IRAF seconds since 1980-01-01 00:00 */

int
lt2tsi()
{
    return ((int)(lt2ts() - 946684800.0));
}


/* LT2TSU-- Return local time as Unix seconds since 1970-01-01 00:00 */

time_t
lt2tsu()
{
    return ((time_t)(lt2ts() - 631152000.0));
}

/* LT2TS-- Return local time as Unix seconds since 1950-01-01 00:00 */

double
lt2ts()
{
    double tsec;
    char *datestring;
    datestring = lt2fd();
    tsec = fd2ts (datestring);
    free (datestring);
    return (tsec);
}


/* MJD2DT-- convert Modified Julian Date to date (yyyy.mmdd) time (hh.mmssss) */

void
mjd2dt (dj,date,time)

double	dj;	/* Modified Julian Date */
double	*date;	/* Date as yyyy.mmdd (returned)
		    yyyy = calendar year (e.g. 1973)
		    mm = calendar month (e.g. 04 = april)
		    dd = calendar day (e.g. 15) */
double	*time;	/* Time as hh.mmssxxxx (returned)
		    *if time<0, it is time as -(fraction of a day)
		    hh = hour of day (0 .le. hh .le. 23)
		    nn = minutes (0 .le. nn .le. 59)
		    ss = seconds (0 .le. ss .le. 59)
		  xxxx = tenths of milliseconds (0 .le. xxxx .le. 9999) */
{
    double tsec;

    tsec = jd2ts (dj + 2400000.5);
    ts2dt (tsec, date, time);

    return;
}


/* MJD2I-- convert Modified Julian Date to date as year, month, day and
           time as hours, minutes, seconds */

void
mjd2i (dj, iyr, imon, iday, ihr, imn, sec, ndsec)

double	dj;	/* Modified Julian Date */
int	*iyr;	/* year (returned) */
int	*imon;	/* month (returned) */
int	*iday;	/* day (returned) */
int	*ihr;	/* hours (returned) */
int	*imn;	/* minutes (returned) */
double	*sec;	/* seconds (returned) */
int	ndsec;	/* Number of decimal places in seconds (0=int) */

{
    double tsec;

    tsec = jd2ts (dj + 2400000.5);
    ts2i (tsec, iyr, imon, iday, ihr, imn, sec, ndsec);
    return;
}


/* MJD2DOY-- convert Modified Julian Date to Year,Day-of-Year */

void
mjd2doy (dj, year, doy)

double	dj;	/* Modified Julian Date */
int	*year;	/* Year (returned) */
double	*doy;	/* Day of year with fraction (returned) */

{
    jd2doy (dj + 2400000.5, year, doy);
    return;
}


/* MJD2JD-- convert Modified Julian Date to Julian Date */

double
mjd2jd (dj)

double	dj;	/* Modified Julian Date */

{
    return (dj + 2400000.5);
}


/* MJD2EP-- convert Modified Julian Date to fractional year */

double
mjd2ep (dj)

double	dj;	/* Modified Julian Date */

{
    double date, time;
    jd2dt (dj + 2400000.5, &date, &time);
    return (dt2ep (date, time));
}


/* MJD2EPB-- convert Modified Julian Date to Besselian epoch */

double
mjd2epb (dj)

double	dj;	/* Modified Julian Date */

{
    return (1900.0 + (dj - 15019.81352) / 365.242198781);
}


/* MJD2EPJ-- convert Modified Julian Date to Julian epoch */

double
mjd2epj (dj)

double	dj;	/* Modified Julian Date */

{
    return (2000.0 + (dj - 51544.5) / 365.25);
}


/* MJD2FD-- convert modified Julian date to FITS date, yyyy-mm-ddThh:mm:ss.ss */

char *
mjd2fd (dj)

double	dj;	/* Modified Julian date */
{
    return (jd2fd (dj + 2400000.5));
}


/* MJD2TS-- convert modified Julian date to seconds since 1950.0 */

double
mjd2ts (dj)

double	dj;	/* Modified Julian date */
{
    return ((dj - 33282.0) * 86400.0);
}


/* EP2FD-- convert fractional year to FITS date, yyyy-mm-ddThh:mm:ss.ss */

char *
ep2fd (epoch)

double	epoch;	/* Date as fractional year */
{
    double tsec; /* seconds since 1950.0 (returned) */
    tsec = ep2ts (epoch);
    return (ts2fd (tsec));
}


/* EPB2FD-- convert Besselian epoch to FITS date, yyyy-mm-ddThh:mm:ss.ss */

char *
epb2fd (epoch)

double	epoch;	/* Besselian epoch (fractional 365.242198781-day years) */
{
    double dj;		/* Julian Date */
    dj = epb2jd (epoch);
    return (jd2fd (dj));
}


/* EPJ2FD-- convert Julian epoch to FITS date, yyyy-mm-ddThh:mm:ss.ss */

char *
epj2fd (epoch)

double	epoch;	/* Julian epoch (fractional 365.25-day years) */
{
    double dj;		/* Julian Date */
    dj = epj2jd (epoch);
    return (jd2fd (dj));
}


/* EP2TS-- convert fractional year to seconds since 1950.0 */

double
ep2ts (epoch)

double	epoch;	/* Date as fractional year */
{
    double dj;
    dj = ep2jd (epoch);
    return ((dj - 2433282.5) * 86400.0);
}


/* EPB2TS-- convert Besselian epoch to seconds since 1950.0 */

double
epb2ts (epoch)

double	epoch;	/* Besselian epoch (fractional 365.242198781-day years) */
{
    double dj;
    dj = epb2jd (epoch);
    return ((dj - 2433282.5) * 86400.0);
}


/* EPJ2TS-- convert Julian epoch to seconds since 1950.0 */

double
epj2ts (epoch)

double	epoch;	/* Julian epoch (fractional 365.25-day years) */
{
    double dj;
    dj = epj2jd (epoch);
    return ((dj - 2433282.5) * 86400.0);
}


/* EPB2EP-- convert Besselian epoch to fractional years */

double
epb2ep (epoch)

double	epoch;	/* Besselian epoch (fractional 365.242198781-day years) */
{
    double dj;
    dj = epb2jd (epoch);
    return (jd2ep (dj));
}


/* EP2EPB-- convert fractional year to Besselian epoch */

double
ep2epb (epoch)

double	epoch;	/* Fractional year */
{
    double dj;
    dj = ep2jd (epoch);
    return (jd2epb (dj));
}


/* EPJ2EP-- convert Julian epoch to fractional year */

double
epj2ep (epoch)

double	epoch;	/* Julian epoch (fractional 365.25-day years) */
{
    double dj;
    dj = epj2jd (epoch);
    return (jd2ep (dj));
}


/* EP2EPJ-- convert fractional year to Julian epoch */

double
ep2epj (epoch)

double	epoch;	/* Fractional year */
{
    double dj;
    dj = ep2jd (epoch);
    return (jd2epj (dj));
}


/* EP2I-- convert fractional year to year month day hours min sec */

void
ep2i (epoch, iyr, imon, iday, ihr, imn, sec, ndsec)

double	epoch;	/* Date as fractional year */
int	*iyr;	/* year (returned) */
int	*imon;	/* month (returned) */
int	*iday;	/* day (returned) */
int	*ihr;	/* hours (returned) */
int	*imn;	/* minutes (returned) */
double	*sec;	/* seconds (returned) */
int	ndsec;	/* Number of decimal places in seconds (0=int) */
{
    double date, time;

    ep2dt (epoch, &date, &time);
    dt2i (date, time, iyr,imon,iday,ihr,imn,sec, ndsec);
    return;
}


/* EPB2I-- convert Besselian epoch to year month day hours min sec */

void
epb2i (epoch, iyr, imon, iday, ihr, imn, sec, ndsec)

double	epoch;	/* Besselian epoch (fractional 365.242198781-day years) */
int	*iyr;	/* year (returned) */
int	*imon;	/* month (returned) */
int	*iday;	/* day (returned) */
int	*ihr;	/* hours (returned) */
int	*imn;	/* minutes (returned) */
double	*sec;	/* seconds (returned) */
int	ndsec;	/* Number of decimal places in seconds (0=int) */
{
    double date, time;

    epb2dt (epoch, &date, &time);
    dt2i (date, time, iyr,imon,iday,ihr,imn,sec, ndsec);
    return;
}


/* EPJ2I-- convert Julian epoch to year month day hours min sec */

void
epj2i (epoch, iyr, imon, iday, ihr, imn, sec, ndsec)

double	epoch;	/* Julian epoch (fractional 365.25-day years) */
int	*iyr;	/* year (returned) */
int	*imon;	/* month (returned) */
int	*iday;	/* day (returned) */
int	*ihr;	/* hours (returned) */
int	*imn;	/* minutes (returned) */
double	*sec;	/* seconds (returned) */
int	ndsec;	/* Number of decimal places in seconds (0=int) */
{
    double date, time;

    epj2dt (epoch, &date, &time);
    dt2i (date, time, iyr,imon,iday,ihr,imn,sec, ndsec);
    return;
}


/* EP2JD-- convert fractional year as used in epoch to Julian date */

double
ep2jd (epoch)

double	epoch;	/* Date as fractional year */

{
    double dj;	/* Julian date (returned)*/
    double date, time;

    ep2dt (epoch, &date, &time);
    dj = dt2jd (date, time);
    return (dj);
}


/* EPB2JD-- convert Besselian epoch to Julian Date */

double
epb2jd (epoch)

double	epoch;	/* Besselian epoch (fractional 365.242198781-day years) */

{
    return (2415020.31352 + ((epoch - 1900.0) * 365.242198781));
}


/* EPJ2JD-- convert Julian epoch to Julian Date */

double
epj2jd (epoch)

double	epoch;	/* Julian epoch (fractional 365.25-day years) */

{
    return (2451545.0 + ((epoch - 2000.0) * 365.25));
}


/* EP2MJD-- convert fractional year as used in epoch to modified Julian date */

double
ep2mjd (epoch)

double	epoch;	/* Date as fractional year */

{
    double dj;	/* Julian date (returned)*/
    double date, time;

    ep2dt (epoch, &date, &time);
    dj = dt2jd (date, time);
    return (dj - 2400000.5);
}


/* EPB2MJD-- convert Besselian epoch to modified Julian Date */

double
epb2mjd (epoch)

double	epoch;	/* Besselian epoch (fractional 365.242198781-day years) */

{
    return (15019.81352 + ((epoch - 1900.0) * 365.242198781));
}


/* EPJ2MJD-- convert Julian epoch to modified Julian Date */

double
epj2mjd (epoch)

double	epoch;	/* Julian epoch (fractional 365.25-day years) */

{
    return (51544.5 + ((epoch - 2000.0) * 365.25));
}



/* EPB2EPJ-- convert Besselian epoch to Julian epoch */

double
epb2epj (epoch)

double	epoch;	/* Besselian epoch (fractional 365.242198781-day years) */
{
    double dj;		/* Julian date */
    dj = epb2jd (epoch);
    return (jd2epj (dj));
}


/* EPJ2EPB-- convert Julian epoch to Besselian epoch */

double
epj2epb (epoch)

double	epoch;	/* Julian epoch (fractional 365.25-day years) */
{
    double dj;		/* Julian date */
    dj = epj2jd (epoch);
    return (jd2epb (dj));
}


/* JD2FD-- convert Julian date to FITS date, yyyy-mm-ddThh:mm:ss.ss */

char *
jd2fd (dj)

double	dj;	/* Julian date */
{
    double tsec;		/* seconds since 1950.0 (returned) */
    tsec = (dj - 2433282.5) * 86400.0;
    return (ts2fd (tsec));
}


/* JD2TS-- convert Julian date to seconds since 1950.0 */

double
jd2ts (dj)

double	dj;	/* Julian date */
{
    return ((dj - 2433282.5) * 86400.0);
}


/* JD2TSI-- convert Julian date to IRAF seconds since 1980-01-01T0:00 */

int
jd2tsi (dj)

double	dj;	/* Julian date */
{
    double ts;
    ts = (dj - 2444239.5) * 86400.0;
    return ((int) ts);
}


/* JD2TSU-- convert Julian date to Unix seconds since 1970-01-01T0:00 */

time_t
jd2tsu (dj)

double	dj;	/* Julian date */
{
    return ((time_t)((dj - 2440587.5) * 86400.0));
}


/* DT2DOY-- convert yyyy.mmdd hh.mmss to year and day of year */

void
dt2doy (date, time, year, doy)

double	date;	/* Date as yyyy.mmdd */
double	time;	/* Time as hh.mmssxxxx */
int	*year;	/* Year (returned) */
double	*doy;	/* Day of year with fraction (returned) */
{
    double	dj;	/* Julian date */
    double	dj0;	/* Julian date on January 1 0:00 */
    double	date0;	/* January first of date's year */
    double	dyear;

    dyear = floor (date);
    date0 = dyear + 0.0101;
    dj0 = dt2jd (date0, 0.0);
    dj = dt2jd (date, time);
    *year = (int) (dyear + 0.00000001);
    *doy = dj - dj0 + 1.0;
    return;
}


/* DOY2DT-- convert year and day of year to yyyy.mmdd hh.mmss */

void
doy2dt (year, doy, date, time)

int	year;	/* Year */
double	doy;	/* Day of year with fraction */
double	*date;	/* Date as yyyy.mmdd (returned) */
double	*time;	/* Time as hh.mmssxxxx (returned) */
{
    double	dj;	/* Julian date */
    double	dj0;	/* Julian date on January 1 0:00 */
    double	date0;	/* January first of date's year */

    date0 = year + 0.0101;
    dj0 = dt2jd (date0, 0.0);
    dj = dj0 + doy - 1.0;
    jd2dt (dj, date, time);
    return;
}


/* DOY2EP-- convert year and day of year to fractional year as used in epoch */

double
doy2ep (year, doy)

int	year;	/* Year */
double	doy;	/* Day of year with fraction */
{
    double date, time;
    doy2dt (year, doy, &date, &time);
    return (dt2ep (date, time));
}



/* DOY2EPB-- convert year and day of year to Besellian epoch */

double
doy2epb (year, doy)

int	year;	/* Year */
double	doy;	/* Day of year with fraction */
{
    double dj;
    dj = doy2jd (year, doy);
    return (jd2epb (dj));
}


/* DOY2EPJ-- convert year and day of year to Julian epoch */

double
doy2epj (year, doy)

int	year;	/* Year */
double	doy;	/* Day of year with fraction */
{
    double dj;
    dj = doy2jd (year, doy);
    return (jd2epj (dj));
}


/* DOY2FD-- convert year and day of year to FITS date */

char *
doy2fd (year, doy)

int	year;	/* Year */
double	doy;	/* Day of year with fraction */
{
    double dj;	/* Julian date  */

    dj = doy2jd (year, doy);
    return (jd2fd (dj));
}


/* DOY2JD-- convert year and day of year to Julian date */

double
doy2jd (year, doy)

int	year;	/* Year */
double	doy;	/* Day of year with fraction */
{
    double	dj0;	/* Julian date */
    double	date;	/* Date as yyyy.mmdd (returned) */
    double	time;	/* Time as hh.mmssxxxx (returned) */

    date = (double) year + 0.0101;
    time = 0.0;
    dj0 = dt2jd (date, time);
    return (dj0 + doy - 1.0);
}


/* DOY2MJD-- convert year and day of year to Julian date */

double
doy2mjd (year, doy)

int	year;	/* Year */
double	doy;	/* Day of year with fraction */
{
    double	dj0;	/* Julian date */
    double	date;	/* Date as yyyy.mmdd (returned) */
    double	time;	/* Time as hh.mmssxxxx (returned) */

    date = (double) year + 0.0101;
    time = 0.0;
    dj0 = dt2jd (date, time);
    return (dj0 + doy - 1.0 - 2400000.5);
}


/* DOY2TSU-- convert from FITS date to Unix seconds since 1970-01-01T0:00 */

time_t
doy2tsu (year, doy)

int	year;	/* Year */
double	doy;	/* Day of year with fraction */
{
    double dj;
    dj = doy2jd (year, doy);
    return ((time_t)jd2ts (dj));
}


/* DOY2TSI-- convert from FITS date to IRAF seconds since 1980-01-01T0:00 */

int
doy2tsi (year, doy)

int	year;	/* Year */
double	doy;	/* Day of year with fraction */
{
    double dj;
    dj = doy2jd (year, doy);
    return ((int)jd2tsi (dj));
}


/* DOY2TS-- convert year, day of year to seconds since 1950 */

double
doy2ts (year, doy)

int	year;	/* Year */
double	doy;	/* Day of year with fraction */
{
    double dj;
    dj = doy2jd (year, doy);
    return (jd2ts (dj));
}


/* FD2DOY-- convert FITS date to year and day of year */

void
fd2doy (string, year, doy)

char	*string;	/* FITS date string, which may be:
			fractional year
			dd/mm/yy (FITS standard before 2000)
			dd-mm-yy (nonstandard use before 2000)
			yyyy-mm-dd (FITS standard after 1999)
			yyyy-mm-ddThh:mm:ss.ss (FITS standard after 1999) */
int	*year;	/* Year (returned) */
double	*doy;	/* Day of year with fraction (returned) */
{
    double dj;	/* Julian date */

    dj = fd2jd (string);
    jd2doy (dj, year, doy);
    return;
}


/* JD2DOY-- convert Julian date to year and day of year */

void
jd2doy (dj, year, doy)

double	dj;	/* Julian date */
int	*year;	/* Year (returned) */
double	*doy;	/* Day of year with fraction (returned) */
{
    double date;	/* Date as yyyy.mmdd (returned) */
    double time;	/* Time as hh.mmssxxxx (returned) */
    double dj0;		/* Julian date at 0:00 on 1/1 */
    double dyear;

    jd2dt (dj, &date, &time);
    *year = (int) date;
    dyear = (double) *year;
    dj0 = dt2jd (dyear+0.0101, 0.0);
    *doy = dj - dj0 + 1.0;
    return;
}


/* TS2JD-- convert seconds since 1950.0 to Julian date */

double
ts2jd (tsec)

double	tsec;	/* seconds since 1950.0 */
{
    return (2433282.5 + (tsec / 86400.0));
}


/* TS2MJD-- convert seconds since 1950.0 to modified Julian date */

double
ts2mjd (tsec)

double	tsec;	/* seconds since 1950.0 */
{
    return (33282.0 + (tsec / 86400.0));
}


/* TS2EP-- convert seconds since 1950.0 to fractional year as used in epoch */

double
ts2ep (tsec)

double	tsec;	/* Seconds since 1950.0 */

{
    double date, time;
    ts2dt (tsec, &date, &time);
    return (dt2ep (date, time));
}


/* TS2EPB-- convert seconds since 1950.0 to Besselian epoch */

double
ts2epb (tsec)

double	tsec;	/* Seconds since 1950.0 */

{
    double dj;		/* Julian Date */
    dj = ts2jd (tsec);
    return (jd2epb (dj));
}


/* TS2EPB-- convert seconds since 1950.0 to Julian epoch */

double
ts2epj (tsec)

double	tsec;	/* Seconds since 1950.0 */

{
    double dj;		/* Julian Date */
    dj = ts2jd (tsec);
    return (jd2epj (dj));
}


/* DT2EP-- convert from date, time as yyyy.mmdd hh.mmsss to fractional year */

double
dt2ep (date, time)

double	date;	/* Date as yyyy.mmdd
		    yyyy = calendar year (e.g. 1973)
		    mm = calendar month (e.g. 04 = april)
		    dd = calendar day (e.g. 15) */
double	time;	/* Time as hh.mmssxxxx
		    *if time<0, it is time as -(fraction of a day)
		    hh = hour of day (0 .le. hh .le. 23)
		    nn = minutes (0 .le. nn .le. 59)
		    ss = seconds (0 .le. ss .le. 59)
		  xxxx = tenths of milliseconds (0 .le. xxxx .le. 9999) */
{
    double epoch; /* Date as fractional year (returned) */
    double dj, dj0, dj1, date0, time0, date1;

    dj = dt2jd (date, time);
    if (date == 0.0)
	epoch = dj / 365.2422;
    else {
	time0 = 0.0;
	date0 = dint (date) + 0.0101;
	date1 = dint (date) + 1.0101;
	dj0 = dt2jd (date0, time0);
	dj1 = dt2jd (date1, time0);
	epoch = dint (date) + ((dj - dj0) / (dj1 - dj0));
	}
    return (epoch);
}


/* DT2EPB-- convert from date, time as yyyy.mmdd hh.mmsss to Besselian epoch */

double
dt2epb (date, time)

double	date;	/* Date as yyyy.mmdd
		    yyyy = calendar year (e.g. 1973)
		    mm = calendar month (e.g. 04 = april)
		    dd = calendar day (e.g. 15) */
double	time;	/* Time as hh.mmssxxxx
		    *if time<0, it is time as -(fraction of a day)
		    hh = hour of day (0 .le. hh .le. 23)
		    nn = minutes (0 .le. nn .le. 59)
		    ss = seconds (0 .le. ss .le. 59)
		  xxxx = tenths of milliseconds (0 .le. xxxx .le. 9999) */
{
    double dj;		/* Julian date */
    double epoch;	/* Date as fractional year (returned) */
    dj = dt2jd (date, time);
    if (date == 0.0)
	epoch = dj / 365.242198781;
    else
	epoch = jd2epb (dj);
    return (epoch);
}


/* DT2EPJ-- convert from date, time as yyyy.mmdd hh.mmsss to Julian epoch */

double
dt2epj (date, time)

double	date;	/* Date as yyyy.mmdd
		    yyyy = calendar year (e.g. 1973)
		    mm = calendar month (e.g. 04 = april)
		    dd = calendar day (e.g. 15) */
double	time;	/* Time as hh.mmssxxxx
		    *if time<0, it is time as -(fraction of a day)
		    hh = hour of day (0 .le. hh .le. 23)
		    nn = minutes (0 .le. nn .le. 59)
		    ss = seconds (0 .le. ss .le. 59)
		  xxxx = tenths of milliseconds (0 .le. xxxx .le. 9999) */
{
    double dj;		/* Julian date */
    double epoch;	/* Date as fractional year (returned) */
    dj = dt2jd (date, time);
    if (date == 0.0)
	epoch = dj / 365.25;
    else
	epoch = jd2epj (dj);
    return (epoch);
}


/* EP2DT-- convert from fractional year to date, time as yyyy.mmdd hh.mmsss */

void
ep2dt (epoch, date, time)

double epoch;	/* Date as fractional year */
double	*date;	/* Date as yyyy.mmdd (returned)
		    yyyy = calendar year (e.g. 1973)
		    mm = calendar month (e.g. 04 = april)
		    dd = calendar day (e.g. 15) */
double	*time;	/* Time as hh.mmssxxxx (returned)
		    *if time<0, it is time as -(fraction of a day)
		    hh = hour of day (0 .le. hh .le. 23)
		    nn = minutes (0 .le. nn .le. 59)
		    ss = seconds (0 .le. ss .le. 59)
		  xxxx = tenths of milliseconds (0 .le. xxxx .le. 9999) */
{
    double dj, dj0, dj1, date0, time0, date1, epochi, epochf;

    time0 = 0.0;
    epochi = dint (epoch);
    epochf = epoch - epochi;
    date0 = epochi + 0.0101;
    date1 = epochi + 1.0101;
    dj0 = dt2jd (date0, time0);
    dj1 = dt2jd (date1, time0);
    dj = dj0 + epochf * (dj1 - dj0);
    jd2dt (dj, date, time);
    return;
}


/* EPB2DT-- convert from Besselian epoch to date, time as yyyy.mmdd hh.mmsss */

void
epb2dt (epoch, date, time)

double	epoch;	/* Besselian epoch (fractional 365.242198781-day years) */
double	*date;	/* Date as yyyy.mmdd (returned)
		    yyyy = calendar year (e.g. 1973)
		    mm = calendar month (e.g. 04 = april)
		    dd = calendar day (e.g. 15) */
double	*time;	/* Time as hh.mmssxxxx (returned)
		    *if time<0, it is time as -(fraction of a day)
		    hh = hour of day (0 .le. hh .le. 23)
		    nn = minutes (0 .le. nn .le. 59)
		    ss = seconds (0 .le. ss .le. 59)
		  xxxx = tenths of milliseconds (0 .le. xxxx .le. 9999) */
{
    double dj;		/* Julian date */
    dj = epb2jd (epoch);
    jd2dt (dj, date, time);
}


/* EPJ2DT-- convert from Julian epoch to date, time as yyyy.mmdd hh.mmsss */

void
epj2dt (epoch, date, time)

double	epoch;	/* Julian epoch (fractional 365.25-day years) */
double	*date;	/* Date as yyyy.mmdd (returned)
		    yyyy = calendar year (e.g. 1973)
		    mm = calendar month (e.g. 04 = april)
		    dd = calendar day (e.g. 15) */
double	*time;	/* Time as hh.mmssxxxx (returned)
		    *if time<0, it is time as -(fraction of a day)
		    hh = hour of day (0 .le. hh .le. 23)
		    nn = minutes (0 .le. nn .le. 59)
		    ss = seconds (0 .le. ss .le. 59)
		  xxxx = tenths of milliseconds (0 .le. xxxx .le. 9999) */
{
    double dj;		/* Julian date */
    dj = epj2jd (epoch);
    jd2dt (dj, date, time);
}


/* FD2JD-- convert FITS standard date to Julian date */

double
fd2jd (string)

char *string;	/* FITS date string, which may be:
			fractional year
			dd/mm/yy (FITS standard before 2000)
			dd-mm-yy (nonstandard use before 2000)
			yyyy-mm-dd (FITS standard after 1999)
			yyyy-mm-ddThh:mm:ss.ss (FITS standard after 1999) */
{
    double date, time;

    fd2dt (string, &date, &time);
    return (dt2jd (date, time));
}


/* FD2MJD-- convert FITS standard date to modified Julian date */

double
fd2mjd (string)

char *string;	/* FITS date string, which may be:
			fractional year
			dd/mm/yy (FITS standard before 2000)
			dd-mm-yy (nonstandard use before 2000)
			yyyy-mm-dd (FITS standard after 1999)
			yyyy-mm-ddThh:mm:ss.ss (FITS standard after 1999) */
{
    return (fd2jd (string) - 2400000.5);
}


/* FD2TSU-- convert from FITS date to Unix seconds since 1970-01-01T0:00 */

time_t
fd2tsu (string)

char *string;	/* FITS date string, which may be:
			fractional year
			dd/mm/yy (FITS standard before 2000)
			dd-mm-yy (nonstandard use before 2000)
			yyyy-mm-dd (FITS standard after 1999)
			yyyy-mm-ddThh:mm:ss.ss (FITS standard after 1999) */
{
    double date, time;
    fd2dt (string, &date, &time);
    return (dt2tsu (date, time));
}


/* FD2TSI-- convert from FITS date to IRAF seconds since 1980-01-01T0:00 */

int
fd2tsi (string)

char *string;	/* FITS date string, which may be:
			fractional year
			dd/mm/yy (FITS standard before 2000)
			dd-mm-yy (nonstandard use before 2000)
			yyyy-mm-dd (FITS standard after 1999)
			yyyy-mm-ddThh:mm:ss.ss (FITS standard after 1999) */
{
    double date, time;
    fd2dt (string, &date, &time);
    return (dt2tsi (date, time));
}


/* FD2TS-- convert FITS standard date to seconds since 1950 */

double
fd2ts (string)

char *string;	/* FITS date string, which may be:
			fractional year
			dd/mm/yy (FITS standard before 2000)
			dd-mm-yy (nonstandard use before 2000)
			yyyy-mm-dd (FITS standard after 1999)
			yyyy-mm-ddThh:mm:ss.ss (FITS standard after 1999) */
{
    double date, time;
    fd2dt (string, &date, &time);
    return (dt2ts (date, time));
}


/* FD2FD-- convert any FITS standard date to ISO FITS standard date */

char *
fd2fd (string)

char *string;	/* FITS date string, which may be:
			fractional year
			dd/mm/yy (FITS standard before 2000)
			dd-mm-yy (nonstandard use before 2000)
			yyyy-mm-dd (FITS standard after 1999)
			yyyy-mm-ddThh:mm:ss.ss (FITS standard after 1999) */
{
    double date, time;
    fd2dt (string, &date, &time);
    return (dt2fd (date, time));
}


/* FD2OF-- convert any FITS standard date to old FITS standard date time */

char *
fd2of (string)

char *string;	/* FITS date string, which may be:
			fractional year
			dd/mm/yy (FITS standard before 2000)
			dd-mm-yy (nonstandard use before 2000)
			yyyy-mm-dd (FITS standard after 1999)
			yyyy-mm-ddThh:mm:ss.ss (FITS standard after 1999) */
{
    int iyr,imon,iday,ihr,imn;
    double sec;

    fd2i (string,&iyr,&imon,&iday,&ihr,&imn,&sec, 3);

    /* Convert to old FITS date format */
    string = (char *) calloc (32, sizeof (char));
    if (iyr < 1900)
	sprintf (string, "*** date out of range ***");
    else if (iyr < 2000)
	sprintf (string, "%02d/%02d/%02d %02d:%02d:%06.3f",
		 iday, imon, iyr-1900, ihr, imn, sec);
    else if (iyr < 2900.0)
	sprintf (string, "%02d/%02d/%3d %02d:%02d:%6.3f",
		 iday, imon, iyr-1900, ihr, imn, sec);
    else
	sprintf (string, "*** date out of range ***");
    return (string);
}


/* TAI-UTC from the U.S. Naval Observatory */
/* ftp://maia.usno.navy.mil/ser7/tai-utc.dat */
static double taijd[23]={2441317.5, 2441499.5, 2441683.5, 2442048.5, 2442413.5,
	      2442778.5, 2443144.5, 2443509.5, 2443874.5, 2444239.5, 2444786.5,
	      2445151.5, 2445516.5, 2446247.5, 2447161.5, 2447892.5, 2448257.5,
	      2448804.5, 2449169.5, 2449534.5, 2450083.5, 2450630.5, 2451179.5};
static double taidt[23]={10.0,11.0,12.0,13.0,14.0,15.0,16.0,17.0,18.0,19.0,
	   20.0,21.0,22.0,23.0,24.0,25.0,26.0,27.0,28.0,29.0,30.0,31.0,32.0};
static double dttab[173]={13.7,13.4,13.1,12.9,12.7,12.6,12.5,12.5,12.5,12.5,
	   12.5,12.5,12.5,12.5,12.5,12.5,12.5,12.4,12.3,12.2,12.0,11.7,11.4,
	   11.1,10.6,10.2, 9.6, 9.1, 8.6, 8.0, 7.5, 7.0, 6.6, 6.3, 6.0, 5.8,
	    5.7, 5.6, 5.6, 5.6, 5.7, 5.8, 5.9, 6.1, 6.2, 6.3, 6.5, 6.6, 6.8,
            6.9, 7.1, 7.2, 7.3, 7.4, 7.5, 7.6, 7.7, 7.7, 7.8, 7.8,7.88,7.82,
	   7.54, 6.97, 6.40, 6.02, 5.41, 4.10, 2.92, 1.82, 1.61, 0.10,-1.02,
	  -1.28,-2.69,-3.24,-3.64,-4.54,-4.71,-5.11,-5.40,-5.42,-5.20,-5.46,
	  -5.46,-5.79,-5.63,-5.64,-5.80,-5.66,-5.87,-6.01,-6.19,-6.64,-6.44,
	  -6.47,-6.09,-5.76,-4.66,-3.74,-2.72,-1.54,-0.02, 1.24, 2.64, 3.86,
	   5.37, 6.14, 7.75, 9.13,10.46,11.53,13.36,14.65,16.01,17.20,18.24,
	  19.06,20.25,20.95,21.16,22.25,22.41,23.03,23.49,23.62,23.86,24.49,
	  24.34,24.08,24.02,24.00,23.87,23.95,23.86,23.93,23.73,23.92,23.96,
	  24.02,24.33,24.83,25.30,25.70,26.24,26.77,27.28,27.78,28.25,28.71,
	  29.15,29.57,29.97,30.36,30.72,31.07,31.35,31.68,32.18,32.68,33.15,
	  33.59,34.00,34.47,35.03,35.73,36.54,37.43,38.29,39.20,40.18,41.17,
	  42.23};


/* ET2FD-- convert from ET (or TDT or TT) in FITS format to UT in FITS format */

char *
et2fd (string)

char *string;	/* FITS date string, which may be:
			fractional year
			dd/mm/yy (FITS standard before 2000)
			dd-mm-yy (nonstandard use before 2000)
			yyyy-mm-dd (FITS standard after 1999)
			yyyy-mm-ddThh:mm:ss.ss (FITS standard after 1999) */
{
    double dj0, dj, tsec, dt;

    dj0 = fd2jd (string);
    dt = utdt (dj0);
    dj = dj0 - (dt / 86400.0);
    dt = utdt (dj);
    tsec = fd2ts (string);
    tsec = tsec - dt;
    return (ts2fd (tsec));
}


/* FD2ET-- convert from UT in FITS format to ET (or TDT or TT) in FITS format */

char *
fd2et (string)

char *string;	/* FITS date string, which may be:
			fractional year
			dd/mm/yy (FITS standard before 2000)
			dd-mm-yy (nonstandard use before 2000)
			yyyy-mm-dd (FITS standard after 1999)
			yyyy-mm-ddThh:mm:ss.ss (FITS standard after 1999) */
{
    double dj, tsec, dt;

    dj = fd2jd (string);
    dt = utdt (dj);
    tsec = fd2ts (string);
    tsec = tsec + dt;
    return (ts2fd (tsec));
}


/* DT2ET-- convert from UT as yyyy.mmdd hh.mmssss to ET in same format */

void
dt2et (date, time)
double	*date;	/* Date as yyyy.mmdd */
double	*time;	/* Time as hh.mmssxxxx
		 *if time<0, it is time as -(fraction of a day) */
{
    double dj, dt, tsec;

    dj = dt2jd (*date, *time);
    dt = utdt (dj);
    tsec = dt2ts (*date, *time);
    tsec = tsec + dt;
    ts2dt (tsec, date, time);
    return;
}


/* EDT2DT-- convert from ET as yyyy.mmdd hh.mmssss to UT in same format */

void
edt2dt (date, time)
double	*date;	/* Date as yyyy.mmdd */
double	*time;	/* Time as hh.mmssxxxx
		 *if time<0, it is time as -(fraction of a day) */
{
    double dj, dt, tsec, tsec0;

    dj = dt2jd (*date, *time);
    dt = utdt (dj);
    tsec0 = dt2ts (*date, *time);
    tsec = tsec0 + dt;
    dj = ts2jd (tsec);
    dt = utdt (dj);
    tsec = tsec0 + dt;
    ts2dt (tsec, date, time);
    return;
}


/* JD2JED-- convert from Julian Date to Julian Ephemeris Date */

double
jd2jed (dj)

double dj;	/* Julian Date */
{
    double dt;

    dt = utdt (dj);
    return (dj + (dt / 86400.0));
}


/* JED2JD-- convert from Julian Ephemeris Date to Julian Date */

double
jed2jd (dj)

double dj;	/* Julian Ephemeris Date */
{
    double dj0, dt;

    dj0 = dj;
    dt = utdt (dj);
    dj = dj0 - (dt / 86400.0);
    dt = utdt (dj);
    return (dj - (dt / 86400.0));
}


/* TS2ETS-- convert from UT in seconds since 1950-01-01 to ET in same format */

double
ts2ets (tsec)

double tsec;
{
    double dj, dt;

    dj = ts2jd (tsec);
    dt = utdt (dj);
    return (tsec + dt);
}


/* ETS2TS-- convert from ET in seconds since 1950-01-01 to UT in same format */

double
ets2ts (tsec)

double tsec;
{
    double dj, dj0, dt;

    dj0 = ts2jd (tsec);
    dt = utdt (dj0);
    dj = dj0 - (dt / 86400.0);
    dt = utdt (dj);
    return (tsec - dt);
}


/* UTDT-- Compute difference between UT and dynamical time (ET-UT) */

double
utdt (dj)

double dj;	/* Julian Date (UT) */
{
    double dt, date, time, ts, ts1, ts0, date0, yfrac, diff, cj;
    int i, iyr, iyear;

    /* If after 1972-01-01, use tabulated TAI-UT */
    if (dj >= 2441317.5) {
	dt = 0.0;
	for (i = 22;  i > 0; i--) {
	    if (dj >= taijd[i])
		dt = taidt[i];
	    }
	dt = dt + 32.84;
	}

    /* For 1800-01-01 to 1972-01-01, use table of ET-UT from AE */
    else if (dj >= 2378496.5) {
	jd2dt (dj, &date, &time);
	ts = jd2ts (dj);
	iyear = (int) date;
	iyr = iyear - 1800;
	date0 = (double) iyear + 0.0101;
	ts0 = dt2ts (date0, 0.0);
	date0 = (double) (iyear + 1) + 0.0101;
	ts1 = dt2ts (date0, 0.0);
	yfrac = (ts - ts0) / (ts1 - ts0);
	diff = dttab[iyr+1] - dttab[iyr];
	dt = dttab[iyr] + (diff * yfrac);
	}

    /* Compute back to 1600 using formula from McCarthy and Babcock (1986) */
    else if (dj >= 2305447.5) {
	cj = (dj - 2378496.5) / 36525.0;
	dt = 5.156 + 13.3066 * (cj - 0.19) * (cj - 0.19);
	}

    /* Compute back to 948 using formula from Stephenson and Morrison (1984) */
    else if (dj >= 2067309.5) {
	cj = (dj - 2378496.5) / 36525.0;
	dt = 25.5 * cj * cj;
	}

    /*Compute back to 390 BC using formula from Stephenson and Morrison (1984)*/
    else if (dj >= 0.0) {
	cj = (dj = 2378496.5) / 36525.0;
	dt = 1360.0 + (320.0 * cj) + (44.3 * cj * cj);
	}

    else
	dt = 0.0;
    return (dt);
}


/* FD2OFD-- convert any FITS standard date to old FITS standard date */

char *
fd2ofd (string)

char *string;	/* FITS date string, which may be:
			fractional year
			dd/mm/yy (FITS standard before 2000)
			dd-mm-yy (nonstandard use before 2000)
			yyyy-mm-dd (FITS standard after 1999)
			yyyy-mm-ddThh:mm:ss.ss (FITS standard after 1999) */
{
    int iyr,imon,iday,ihr,imn;
    double sec;

    fd2i (string,&iyr,&imon,&iday,&ihr,&imn,&sec, 3);

    /* Convert to old FITS date format */
    string = (char *) calloc (32, sizeof (char));
    if (iyr < 1900)
	sprintf (string, "*** date out of range ***");
    else if (iyr < 2000)
	sprintf (string, "%02d/%02d/%02d", iday, imon, iyr-1900);
    else if (iyr < 2900.0)
	sprintf (string, "%02d/%02d/%3d", iday, imon, iyr-1900);
    else
	sprintf (string, "*** date out of range ***");
    return (string);
}


/* FD2OFT-- convert any FITS standard date to old FITS standard time */

char *
fd2oft (string)

char *string;	/* FITS date string, which may be:
			fractional year
			dd/mm/yy (FITS standard before 2000)
			dd-mm-yy (nonstandard use before 2000)
			yyyy-mm-dd (FITS standard after 1999)
			yyyy-mm-ddThh:mm:ss.ss (FITS standard after 1999) */
{
    int iyr,imon,iday,ihr,imn;
    double sec;

    fd2i (string,&iyr,&imon,&iday,&ihr,&imn,&sec, 3);

    /* Convert to old FITS date format */
    string = (char *) calloc (32, sizeof (char));
    sprintf (string, "%02d:%02d:%06.3f", ihr, imn, sec);
    return (string);
}


/* FD2DT-- convert FITS standard date to date, time as yyyy.mmdd hh.mmsss */

void
fd2dt (string, date, time)

char *string;	/* FITS date string, which may be:
		    fractional year
		    dd/mm/yy (FITS standard before 2000)
		    dd-mm-yy (nonstandard use before 2000)
		    yyyy-mm-dd (FITS standard after 1999)
		    yyyy-mm-ddThh:mm:ss.ss (FITS standard after 1999) */
double	*date;	/* Date as yyyy.mmdd (returned)
		    yyyy = calendar year (e.g. 1973)
		    mm = calendar month (e.g. 04 = april)
		    dd = calendar day (e.g. 15) */
double	*time;	/* Time as hh.mmssxxxx (returned)
		    *if time<0, it is time as -(fraction of a day)
		    hh = hour of day (0 .le. hh .le. 23)
		    nn = minutes (0 .le. nn .le. 59)
		    ss = seconds (0 .le. ss .le. 59)
		  xxxx = tenths of milliseconds (0 .le. xxxx .le. 9999) */
{
    int iyr,imon,iday,ihr,imn;
    double sec;

    fd2i (string,&iyr,&imon,&iday,&ihr,&imn,&sec, 4);

    /* Convert date to yyyy.mmdd */
    if (iyr < 0) {
	*date = (double) (-iyr) + 0.01 * (double) imon + 0.0001 * (double) iday;
	*date = -(*date);
	}
    else
	*date = (double) iyr + 0.01 * (double) imon + 0.0001 * (double) iday;

    /* Convert time to hh.mmssssss */
    *time = (double) ihr + 0.01 * (double) imn + 0.0001 * sec;

    return;
}


/* FD2EP-- convert from FITS standard date to fractional year */

double
fd2ep (string)

char *string;	/* FITS date string, which may be:
			yyyy.ffff (fractional year)
			dd/mm/yy (FITS standard before 2000)
			dd-mm-yy (nonstandard FITS use before 2000)
			yyyy-mm-dd (FITS standard after 1999)
			yyyy-mm-ddThh:mm:ss.ss (FITS standard after 1999) */

{
    double dj;		/* Julian date */
    dj = fd2jd (string);
    if (dj < 1.0)
	return (dj / 365.2422);
    else
	return (jd2ep (dj));
}


/* FD2EPB-- convert from FITS standard date to Besselian epoch */

double
fd2epb (string)

char *string;	/* FITS date string, which may be:
			yyyy.ffff (fractional year)
			dd/mm/yy (FITS standard before 2000)
			dd-mm-yy (nonstandard FITS use before 2000)
			yyyy-mm-dd (FITS standard after 1999)
			yyyy-mm-ddThh:mm:ss.ss (FITS standard after 1999) */

{
    double dj;		/* Julian date */
    dj = fd2jd (string);
    if (dj < 1.0)
	return (dj / 365.242198781);
    else
	return (jd2epb (dj));
}


/* FD2EPJ-- convert from FITS standard date to Julian epoch */

double
fd2epj (string)

char *string;	/* FITS date string, which may be:
			yyyy.ffff (fractional year)
			dd/mm/yy (FITS standard before 2000)
			dd-mm-yy (nonstandard FITS use before 2000)
			yyyy-mm-dd (FITS standard after 1999)
			yyyy-mm-ddThh:mm:ss.ss (FITS standard after 1999) */

{
    double dj;		/* Julian date */
    dj = fd2jd (string);
    if (dj < 1.0)
	return (dj / 365.25);
    else
	return (jd2epj (dj));
}


/* DT2TSU-- convert from date and time to Unix seconds since 1970-01-01T0:00 */

time_t
dt2tsu (date,time)

double	date;	/* Date as yyyy.mmdd */
double	time;	/* Time as hh.mmssxxxx
		 *if time<0, it is time as -(fraction of a day) */
{
    return ((time_t)(dt2ts (date, time) - 631152000.0));
}


/* DT2TSI-- convert from date and time to IRAF seconds since 1980-01-01T0:00 */

int
dt2tsi (date,time)

double	date;	/* Date as yyyy.mmdd */
double	time;	/* Time as hh.mmssxxxx
		 *if time<0, it is time as -(fraction of a day) */
{
    return ((int)(dt2ts (date, time) - 946684800.0));
}



/* DT2TS-- convert from date, time as yyyy.mmdd hh.mmsss to sec since 1950.0 */

double
dt2ts (date,time)

double	date;	/* Date as yyyy.mmdd
		    yyyy = calendar year (e.g. 1973)
		    mm = calendar month (e.g. 04 = april)
		    dd = calendar day (e.g. 15) */
double	time;	/* Time as hh.mmssxxxx
		    *if time<0, it is time as -(fraction of a day)
		    hh = hour of day (0 .le. hh .le. 23)
		    nn = minutes (0 .le. nn .le. 59)
		    ss = seconds (0 .le. ss .le. 59)
		  xxxx = tenths of milliseconds (0 .le. xxxx .le. 9999) */
{
    double tsec; /* Seconds past 1950.0 (returned) */

    double dh,dm,dd;
    int iy,im,id;

/* Calculate the number of full years, months, and days already
 * elapsed since 0h, March 1, -1 (up to most recent midnight). */

    /* convert time of day to elapsed seconds */

    /* If time is < 0, it is assumed to be a fractional day */
    if (time < 0.0)
	tsec = time * -86400.0;
    else {
	dh = (int) (time + 0.0000000001);
	dm = (int) (((time - dh) * 100.0) + 0.0000000001);
	tsec = (time * 10000.0) - (dh * 10000.0) - (dm * 100.0);
	tsec = (int) (tsec * 100000.0 + 0.0001) / 100000.0;
	tsec = tsec + (dm * 60.0) + (dh * 3600.0);
	}


    /* Calculate the number of full months elapsed since
     * the current or most recent March */
    if (date >= 0.0301) {
	iy = (int) (date + 0.0000000001);
	im = (int) (((date - (double) (iy)) * 10000.0) + 0.00000001);
	id = im % 100;
	im = (im / 100) + 9;
	if (im < 12) iy = iy - 1;
	im = im % 12;
	id = id - 1;

	/* starting with March as month 0 and ending with the following
	 * February as month 11, the calculation of the number of days
	 * per month reduces to a simple formula. the following statement
	 * determines the number of whole days elapsed since 3/1/-1 and then
	 * subtracts the 712163 days between then and 1/1/1950.  it converts
	 * the result to seconds and adds the accumulated seconds above. */
	id = id + ((im+1+im/6+im/11)/2 * 31) + ((im-im/6-im/11)/2 * 30) +
	     (iy / 4) - (iy / 100) + (iy / 400);
	dd = (double) id + (365.0 * (double) iy) - 712163.0;
	tsec = tsec + (dd * 86400.0);
	}

    return (tsec);
}


/* TS2DT-- convert seconds since 1950.0 to date, time as yyyy.mmdd hh.mmssss */

void
ts2dt (tsec,date,time)

double	tsec;	/* Seconds past 1950.0 */
double	*date;	/* Date as yyyy.mmdd (returned)
		    yyyy = calendar year (e.g. 1973)
		    mm = calendar month (e.g. 04 = april)
		    dd = calendar day (e.g. 15) */
double	*time;	/* Time as hh.mmssxxxx (returned)
		    *if time<0, it is time as -(fraction of a day)
		    hh = hour of day (0 .le. hh .le. 23)
		    nn = minutes (0 .le. nn .le. 59)
		    ss = seconds (0 .le. ss .le. 59)
		  xxxx = tenths of milliseconds (0 .le. xxxx .le. 9999) */
{
    int iyr,imon,iday,ihr,imn;
    double sec;

    ts2i (tsec,&iyr,&imon,&iday,&ihr,&imn,&sec, 4);

    /* Convert date to yyyy.mmdd */
    if (iyr < 0) {
	*date = (double) (-iyr) + 0.01 * (double) imon + 0.0001 * (double) iday;
	*date = -(*date);
	}
    else
	*date = (double) iyr + 0.01 * (double) imon + 0.0001 * (double) iday;

    /* Convert time to hh.mmssssss */
    *time = (double) ihr + 0.01 * (double) imn + 0.0001 * sec;

    return;
}


/* TSI2DT-- Convert seconds since 1980-01-01 to date yyyy.ddmm, time hh.mmsss */

void
tsi2dt (isec,date,time)

int	isec;	/* Seconds past 1980-01-01 */
double	*date;	/* Date as yyyy.mmdd (returned) */
double	*time;	/* Time as hh.mmssxxxx (returned) */
{
    ts2dt (tsi2ts (isec), date, time);
}


/* TSI2FD-- Convert seconds since 1980-01-01 to FITS standard date string */

char *
tsi2fd (isec)

int	isec;	/* Seconds past 1980-01-01 */
{
    return (ts2fd (tsi2ts (isec)));
}


/* TSI2TS-- Convert seconds since 1980-01-01 to seconds since 1950-01-01 */

double
tsi2ts (isec)
int	isec;	/* Seconds past 1980-01-01 */
{
    return ((double) isec + 946684800.0);
}


/* TSU2FD-- Convert seconds since 1970-01-01 to FITS standard date string */

char *
tsu2fd (isec)
time_t	isec;	/* Seconds past 1970-01-01 */
{
    return (ts2fd (tsu2ts (isec)));
}


/* TSU2DT-- Convert seconds since 1970-01-01 to date yyyy.ddmm, time hh.mmsss */

void
tsu2dt (isec,date,time)
time_t	isec;	/* Seconds past 1970-01-01 */
double	*date;	/* Date as yyyy.mmdd (returned) */
double	*time;	/* Time as hh.mmssxxxx (returned) */
{
    ts2dt (tsu2ts (isec), date, time);
}


/* TSU2TS-- Convert seconds since 1970-01-01 to seconds since 1950-01-01 */

double
tsu2ts (isec)
time_t	isec;	/* Seconds past 1970-01-01 */
{
    return ((double) isec + 631152000.0);
}

/* TSU2TSI-- UT seconds since 1970-01-01 to local seconds since 1980-01-01 */

int
tsu2tsi (isec)
time_t	isec;	/* Seconds past 1970-01-01 */
{
    double date, time;
    struct tm *ts;

    /* Get local time  from UT seconds */
    ts = localtime (&isec);
    if (ts->tm_year < 1000)
	date = (double) (ts->tm_year + 1900);
    else
	date = (double) ts->tm_year;
    date = date + (0.01 * (double) (ts->tm_mon + 1));
    date = date + (0.0001 * (double) ts->tm_mday);
    time = (double) ts->tm_hour;
    time = time + (0.01 * (double) ts->tm_min);
    time = time + (0.0001 * (double) ts->tm_sec);
    return ((int)(dt2ts (date, time) - 631152000.0));
}


/* TS2FD-- convert seconds since 1950.0 to FITS date, yyyy-mm-ddThh:mm:ss.ss */

char *
ts2fd (tsec)

double	tsec;	/* Seconds past 1950.0 */
{
    double date, time;

    ts2dt (tsec, &date, &time);
    return (dt2fd (date, time));
}


/* TSD2FD-- convert seconds since start of day to FITS time, hh:mm:ss.ss */

char *
tsd2fd (tsec)

double	tsec;	/* Seconds since start of day */
{
    double date, time;
    char *thms, *fdate;
    int lfd, nbc;

    ts2dt (tsec, &date, &time);
    fdate = dt2fd (date, time);
    thms = (char *) calloc (16, 1);
    lfd = strlen (fdate);
    nbc = lfd - 11;
    strncpy (thms, fdate+11, nbc);
    return (thms);
}


/* TSD2DT-- convert seconds since start of day to hh.mmssss */

double
tsd2dt (tsec)

double	tsec;	/* Seconds since start of day */
{
    double date, time;

    ts2dt (tsec, &date, &time);
    return (time);
}



/* DT2I-- convert vigesimal date and time to year month day hours min sec */

void
dt2i (date, time, iyr, imon, iday, ihr, imn, sec, ndsec)

double	date;	/* Date as yyyy.mmdd (returned)
		    yyyy = calendar year (e.g. 1973)
		    mm = calendar month (e.g. 04 = april)
		    dd = calendar day (e.g. 15) */
double	time;	/* Time as hh.mmssxxxx (returned)
		    *if time<0, it is time as -(fraction of a day)
		    hh = hour of day (0 .le. hh .le. 23)
		    nn = minutes (0 .le. nn .le. 59)
		    ss = seconds (0 .le. ss .le. 59)
		  xxxx = tenths of milliseconds (0 .le. xxxx .le. 9999) */
int	*iyr;	/* year (returned) */
int	*imon;	/* month (returned) */
int	*iday;	/* day (returned) */
int	*ihr;	/* hours (returned) */
int	*imn;	/* minutes (returned) */
double	*sec;	/* seconds (returned) */
int	ndsec;	/* Number of decimal places in seconds (0=int) */

{
    double t,d;

    t = time;
    if (date < 0.0)
	d = -date;
    else
	d = date;

    /* Extract components of time */
    *ihr = dint (t + 0.000000001);
    t = 100.0 * (t - (double) *ihr);
    *imn = dint (t + 0.0000001);
    *sec = 100.0 * (t - (double) *imn);

    /* Extract components of date */
    *iyr = dint (d + 0.00001);
    d = 100.0 * (d - (double) *iyr);
    if (date < 0.0)
	*iyr = - *iyr;
    *imon = dint (d + 0.001);
    d = 100.0 * (d - (double) *imon);
    *iday = dint (d + 0.1);

   /* Make sure date and time are legal */
    fixdate (iyr, imon, iday, ihr, imn, sec, ndsec);

    return;
}


/* FD2I-- convert from FITS standard date to year, mon, day, hours, min, sec */

void
fd2i (string, iyr, imon, iday, ihr, imn, sec, ndsec)

char	*string; /* FITS date string, which may be:
			yyyy.ffff (fractional year)
			dd/mm/yy (FITS standard before 2000)
			dd-mm-yy (nonstandard FITS use before 2000)
			yyyy-mm-dd (FITS standard after 1999)
			yyyy-mm-ddThh:mm:ss.ss (FITS standard after 1999) */
int	*iyr;	/* year (returned) */
int	*imon;	/* month (returned) */
int	*iday;	/* day (returned) */
int	*ihr;	/* hours (returned) */
int	*imn;	/* minutes (returned) */
double	*sec;	/* seconds (returned) */
int	ndsec;	/* Number of decimal places in seconds (0=int) */

{
    double tsec, fday, hr, mn;
    int i;
    char *sstr, *dstr, *tstr, *cstr, *nval, *fstr;

    /* Initialize all returned data to zero */
    *iyr = 0;
    *imon = 0;
    *iday = 0;
    *ihr = 0;
    *imn = 0;
    *sec = 0.0;

    /* Return if no input string */
    if (string == NULL)
	return;

    /* Check for various non-numeric characters */
    sstr = strchr (string,'/');
    dstr = strchr (string,'-');
    if (dstr == string)
	dstr = strchr (string+1, '-');
    fstr = strchr (string, '.');
    tstr = strchr (string,'T');
    if (tstr == NULL)
	tstr = strchr (string, 'Z');
    if (tstr == NULL)
	tstr = strchr (string, 'S');
    if (fstr != NULL && tstr != NULL && fstr > tstr)
	fstr = NULL;
    cstr = strchr (string,':');

    /* Original FITS date format: dd/mm/yy */
    if (sstr > string) {
	*sstr = '\0';
	*iday = (int) atof (string);
	if (*iday > 31) {
	    *iyr = *iday;
	    if (*iyr >= 0 && *iyr <= 49)
		*iyr = *iyr + 2000;
	    else if (*iyr < 1000)
		*iyr = *iyr + 1900;
	    *sstr = '/';
	    nval = sstr + 1;
	    sstr = strchr (nval,'/');
	    if (sstr > string) {
		*sstr = '\0';
		*imon = (int) atof (nval);
		*sstr = '/';
		nval = sstr + 1;
		*iday = (int) atof (nval);
		}
	    }
	else {
	    *sstr = '/';
	    nval = sstr + 1;
	    sstr = strchr (nval,'/');
	    if (sstr == NULL)
		sstr = strchr (nval,'-');
	    if (sstr > string) {
		*sstr = '\0';
		*imon = (int) atof (nval);
		*sstr = '/';
		nval = sstr + 1;
		*iyr = (int) atof (nval);
		if (*iyr >= 0 && *iyr <= 49)
		    *iyr = *iyr + 2000;
		else if (*iyr < 1000)
		    *iyr = *iyr + 1900;
		}
	    }
	tstr = strchr (string,'_');
	if (tstr == NULL)
	    return;
	}

    /* New FITS date format: yyyy-mm-ddThh:mm:ss[.sss] */
    else if (dstr > string) {
	*dstr = '\0';
	*iyr = (int) atof (string);
	*dstr = '-';
	nval = dstr + 1;
	dstr = strchr (nval,'-');
	*imon = 1;
	*iday = 1;

	/* Decode year, month, and day */
	if (dstr > string) {
	    *dstr = '\0';
	    *imon = (int) atof (nval);
	    *dstr = '-';
	    nval = dstr + 1;
	    if (tstr > string)
		*tstr = '\0';
	    *iday = (int) atof (nval);

	    /* If fraction of a day is present, turn it into a time */
	    if (fstr != NULL) {
		fday = atof (fstr);
		hr = fday * 24.0;
		*ihr = (int) hr;
		mn = 60.0 * (hr - (double) *ihr);
		*imn = (int) mn;
		*sec = 60.0 * (mn - (double) *imn);
		}

	    if (tstr > string)
		*tstr = 'T';
	    }

	/* If date is > 31, it is really year in old format */
	if (*iday > 31) {
	    i = *iyr;
	    if (*iday < 100)
		*iyr = *iday + 1900;
	    else
		*iyr = *iday;
	    *iday = i;
	    }
	}

    /* In rare cases, a FITS time is entered as an epoch */
    else if (tstr == NULL && cstr == NULL && isnum (string)) {
	tsec = ep2ts (atof (string));
	ts2i (tsec,iyr,imon,iday,ihr,imn,sec, ndsec);
	return;
	}

    /* Extract time, if it is present */
    if (tstr > string || cstr > string) {
	if (tstr > string)
	    nval = tstr + 1;
	else
	    nval = string;
	cstr = strchr (nval,':');
	if (cstr > string) {
	    *cstr = '\0';
	    *ihr = (int) atof (nval);
	    *cstr = ':';
	    nval = cstr + 1;
	    cstr = strchr (nval,':');
	    if (cstr > string) {
		*cstr = '\0';
		*imn = (int) atof (nval);
		*cstr = ':';
		nval = cstr + 1;
		*sec = atof (nval);
		}
	    else
		*imn = (int) atof (nval);
	    }
	else
	    *ihr = (int) atof (nval);
	}
    else
	ndsec = -1;

   /* Make sure date and time are legal */
    fixdate (iyr, imon, iday, ihr, imn, sec, ndsec);

    return;
}


/* TS2I-- convert sec since 1950.0 to year month day hours minutes seconds */

void
ts2i (tsec,iyr,imon,iday,ihr,imn,sec, ndsec)

double	tsec;	/* seconds since 1/1/1950 0:00 */
int	*iyr;	/* year (returned) */
int	*imon;	/* month (returned) */
int	*iday;	/* day (returned) */
int	*ihr;	/* hours (returned) */
int	*imn;	/* minutes (returned) */
double	*sec;	/* seconds (returned) */
int	ndsec;	/* Number of decimal places in seconds (0=int) */

{
    double t,days, ts, dts;
    int nc,nc4,nly,ny,m,im;

    /* Round seconds to 0 - 4 decimal places */
    ts = tsec + 61530883200.0;
    if (ts < 0.0)
	dts = -0.5;
    else
	dts = 0.5;
    if (ndsec < 1)
	t = dint (ts + dts) * 10000.0;
    else if (ndsec < 2)
	t = dint (ts * 10.0 + dts) * 1000.0;
    else if (ndsec < 3)
	t = dint (ts * 100.0 + dts) * 100.0;
    else if (ndsec < 4)
	t = dint (ts * 1000.0 + dts) * 10.0;
    else
	t = dint (ts * 10000.0 + dts);
    ts = t / 10000.0;

    /* Time of day (hours, minutes, seconds */
    *ihr = (int) (dmod (ts/3600.0, 24.0));
    *imn = (int) (dmod (ts/60.0, 60.0));
    *sec = dmod (ts, 60.0);

    /* Number of days since 0 hr 0/0/0000 */
    days = dint ((t / 864000000.0) + 0.000001);

    /* Number of leap centuries (400 years) */
    nc4 = (int) ((days / 146097.0) + 0.00001);

    /* Number of centuries since last /400 */
    days = days - (146097.0 * (double) (nc4));
    nc = (int) ((days / 36524.0) + 0.000001);
    if (nc > 3) nc = 3;

    /* Number of leap years since last century */
    days = days - (36524.0 * nc);
    nly = (int) ((days / 1461.0) + 0.0000000001);

    /* Number of years since last leap year */
    days = days - (1461.0 * (double) nly);
    ny = (int) ((days / 365.0) + 0.00000001);
    if (ny > 3) ny = 3;

    /* Day of month */
    days = days - (365.0 * (double) ny);
    if (days < 0) {
	m = 0;
	*iday = 29;
	}
    else {
	*iday = (int) (days + 0.00000001) + 1;
	for (m = 1; m <= 12; m++) {
	    im = (m + ((m - 1) / 5)) % 2;
	    /* fprintf (stderr,"%d %d %d %d\n", m, im, *iday, nc); */
	    if (*iday-1 < im+30) break;
	    *iday = *iday - im - 30;
	    }
	}

    /* Month */
    *imon = ((m+1) % 12) + 1;

    /* Year */
    *iyr = nc4*400 + nc*100 + nly*4 + ny + m/11;

   /* Make sure date and time are legal */
    fixdate (iyr, imon, iday, ihr, imn, sec, ndsec);

    return;
}


/* UT2DOY-- Current Universal Time as year, day of year */

void
ut2doy (year, doy)

int	*year;	/* Year (returned) */
double	*doy;	/* Day of year (returned) */
{
    double date, time;
    ut2dt (&date, &time);
    dt2doy (date, time, year, doy);
    return;
}


/* UT2DT-- Current Universal Time as date (yyyy.mmdd) and time (hh.mmsss) */

void
ut2dt(date, time)

double	*date;	/* Date as yyyy.mmdd (returned) */
double	*time;	/* Time as hh.mmssxxxx (returned) */
{
    time_t tsec;
    struct timeval tp;
    struct timezone tzp;
    struct tm *ts;

    gettimeofday (&tp,&tzp);

    tsec = tp.tv_sec;
    ts = gmtime (&tsec);

    if (ts->tm_year < 1000)
	*date = (double) (ts->tm_year + 1900);
    else
	*date = (double) ts->tm_year;
    *date = *date + (0.01 * (double) (ts->tm_mon + 1));
    *date = *date + (0.0001 * (double) ts->tm_mday);
    *time = (double) ts->tm_hour;
    *time = *time + (0.01 * (double) ts->tm_min);
    *time = *time + (0.0001 * (double) ts->tm_sec);

    return;
}


/* UT2EP-- Return current Universal Time as fractional year */

double
ut2ep()
{
    return (jd2ep (ut2jd()));
}


/* UT2EPB-- Return current Universal Time as Besselian epoch */

double
ut2epb()
{
    return (jd2epb (ut2jd()));
}


/* UT2EPJ-- Return current Universal Time as Julian epoch */

double
ut2epj()
{
    return (jd2epj (ut2jd()));
}


/* UT2FD-- Return current Universal Time as FITS ISO date string */

char *
ut2fd()
{
    int year, month, day, hour, minute, second;
    time_t tsec;
    struct timeval tp;
    struct timezone tzp;
    struct tm *ts;
    char *isotime;

    gettimeofday (&tp,&tzp);
    tsec = tp.tv_sec;
    ts = gmtime (&tsec);

    year = ts->tm_year;
    if (year < 1000)
	year = year + 1900;
    month = ts->tm_mon + 1;
    day = ts->tm_mday;
    hour = ts->tm_hour;
    minute = ts->tm_min;
    second = ts->tm_sec; 

    isotime = (char *) calloc (32, sizeof (char));
    sprintf (isotime, "%04d-%02d-%02dT%02d:%02d:%02d",
		      year, month, day, hour, minute, second);
    return (isotime);
}


/* UT2JD-- Return current Universal Time as Julian Date */

double
ut2jd()
{
    return (fd2jd (ut2fd()));
}


/* UT2MJD-- convert current UT to Modified Julian Date */

double
ut2mjd ()

{
    return (ut2jd() - 2400000.5);
}

/* UT2TS-- current Universal Time as IRAF seconds since 1950-01-01T00:00 */

double
ut2ts()
{
    double tsec;
    char *datestring;
    datestring = ut2fd();
    tsec = fd2ts (datestring);
    free (datestring);
    return (tsec);
}


/* UT2TSI-- current Universal Time as IRAF seconds since 1980-01-01T00:00 */

int
ut2tsi()
{
    return ((int)(ut2ts() - 946684800.0));
}


/* UT2TSU-- current Universal Time as IRAF seconds since 1970-01-01T00:00 */

time_t
ut2tsu()
{
    return ((time_t)(ut2ts () - 631152000.0));
}


/* FD2GST-- convert from FITS date to Greenwich Sidereal Time */

char *
fd2gst (string)

char	*string;	/* FITS date string, which may be:
			  fractional year
			  dd/mm/yy (FITS standard before 2000)
			  dd-mm-yy (nonstandard use before 2000)
			  yyyy-mm-dd (FITS standard after 1999)
			  yyyy-mm-ddThh:mm:ss.ss (FITS standard after 1999) */
{
    double dj, gsec, date, time;

    dj = fd2jd (string);
    gsec = jd2gst (dj);
    ts2dt (gsec, &date, &time);
    date = 0.0;
    return (dt2fd (date, time));
}


/* DT2GST-- convert from UT as yyyy.mmdd hh.mmssss to Greenwich Sidereal Time*/

void
dt2gst (date, time)
double  *date;  /* Date as yyyy.mmdd */
double  *time;  /* Time as hh.mmssxxxx
                 *if time<0, it is time as -(fraction of a day) */
{
    double dj, gsec;

    dj = dt2ts (*date, *time);
    gsec = jd2gst (dj);
    ts2dt (gsec, date, time);
    *date = 0.0;
    return;
}


/* JD2LST - Local Sidereal Time in seconds from Julian Date */

double
jd2lst (dj)

double dj;	/* Julian Date */
{
    double gst, lst, l0;

    /* Compute Greenwich Sidereal Time at this epoch */
    gst = jd2gst (dj);

    /* Subtract longitude (degrees to seconds of time) */
    lst = gst - (240.0 * longitude);
    if (lst < 0.0)
	lst = lst + 86400.0;
    else if (lst > 86400.0)
	lst = lst - 86400.0;
    return (lst);
}


/* FD2LST - Local Sidereal Time  as hh:mm:ss.ss
            from Universal Time as FITS ISO date */

char *
fd2lst (string)

char	*string;	/* FITS date string, which may be:
			  fractional year
			  dd/mm/yy (FITS standard before 2000)
			  dd-mm-yy (nonstandard use before 2000)
			  yyyy-mm-dd (FITS standard after 1999) */
{
    double dj, date, time, lst;

    dj = fd2jd (string);
    lst = jd2lst (dj);
    ts2dt (lst, &date, &time);
    date = 0.0;
    return (dt2fd (date, time));
}


/* DT2LST - Local Sidereal Time  as hh.mmssss
            from Universal Time as yyyy.mmdd hh.mmssss */

void
dt2lst (date, time)

double  *date;  /* Date as yyyy.mmdd */
double  *time;  /* Time as hh.mmssxxxx
                 *if time<0, it is time as -(fraction of a day) */
{
    double dj, lst, date0;

    dj = dt2jd (*date, *time);
    lst = jd2lst (dj);
    date0 = 0.0;
    ts2dt (lst, &date0, time);
    return;
}


/* TS2LST - Local Sidereal Time in seconds of day
 *          from Universal Time in seconds since 1951-01-01T0:00:00
 */

double
ts2lst (tsec)

double tsec;		/* time since 1950.0 in UT seconds */
{
    double gst;	/* Greenwich Sidereal Time in seconds since 0:00 */
    double lst;	/* Local Sidereal Time in seconds since 0:00 */
    double gsec, date;

    /* Greenwich Sidereal Time */
    gsec = ts2gst (tsec);
    date = 0.0;
    ts2dt (gsec, &date, &gst);

    lst = gst - (longitude / 15.0);
    if (lst < 0.0)
	lst = lst + 86400.0;
    else if (lst > 86400.0)
	lst = lst - 86400.0;
    return (lst);
}


/* LST2FD - calculate current UT given Local Sidereal Time
 *	    plus date in FITS ISO format (yyyy-mm-dd)
 *	    Return UT date and time in FITS ISO format
 */

char *
lst2fd (string)

char *string;		/* UT Date, LST as yyyy-mm-ddShh:mm:ss.ss */
{
    double sdj, dj;

    sdj = fd2jd (string);

    dj = lst2jd (sdj);

    return (jd2fd (dj));
}


/* LST2JD - calculate current Julian Date given Local Sidereal Time
 *	    plus current Julian Date (0.5 at 0:00 UT)
 *	    Return UT date and time as Julian Date
 */

double
lst2jd (sdj)

double sdj;	/* Julian Date of desired day at 0:00 UT + sidereal time */
{
    double gst;	/* Greenwich Sidereal Time in seconds since 0:00 */
    double lsd;	/* Local Sidereal Time in seconds since 0:00 */
    double gst0, tsd, dj1, dj0, eqnx;
    int idj;

    /* Julian date at 0:00 UT */
    idj = (int) sdj;
    dj0 = (double) idj + 0.5;
    if (dj0 > sdj) dj0 = dj0 - 1.0;

    /* Greenwich Sidereal Time at 0:00 UT in seconds */
    gst0 = jd2gst (dj0);

    /* Sidereal seconds since 0:00 */
    lsd = (sdj - dj0) * 86400.0;

    /* Remove longitude for current Greenwich Sidereal Time in seconds */
    /* (convert longitude from degrees to seconds of time) */
    gst = lsd + (longitude * 240.0);

    /* Time since 0:00 UT */
    tsd = (gst - gst0) / 1.0027379093;

    /* Julian Date (UT) */
    dj1 = dj0 + (tsd / 86400.0);

    /* Equation of the equinoxes converted to UT seconds */
    eqnx = eqeqnx (dj1) / 1.002739093;

    /* Remove equation of equinoxes */
    dj1 = dj1 - (eqnx / 86400.0);
    if (dj1 < dj0)
	dj1 = dj1 + 1.0;

    return (dj1);
}


/* MST2FD - calculate current UT given Greenwich Mean Sidereal Time
 *	    plus date in FITS ISO format (yyyy-mm-ddShh:mm:ss.ss)
 *	    Return UT date and time in FITS ISO format
 */

char *
mst2fd (string)

char *string;		/* UT Date, MST as yyyy-mm-ddShh:mm:ss.ss */
{
    double sdj, dj;

    sdj = fd2jd (string);

    dj = mst2jd (sdj);

    return (jd2fd (dj));
}


/* MST2JD - calculate current UT given Greenwich Mean Sidereal Time
 *	    plus date in Julian Date (0:00 UT + Mean Sidereal Time)
 *	    Return UT date and time as Julian Date
 */

double
mst2jd (sdj)

double sdj;		/* UT Date, MST as Julian Date */
{
    double tsd, djd, st0, dj0, dj;

    dj0 = (double) ((int) sdj) + 0.5;

    /* Greenwich Mean Sidereal Time at 0:00 UT in seconds */
    st0 = jd2mst (dj0);

    /* Mean Sidereal Time in seconds */
    tsd = (sdj - dj0) * 86400.0;
    if (tsd < 0.0)
	tsd = tsd + 86400.0;

    /* Convert to fraction of a day since 0:00 UT */
    djd = ((tsd - st0) / 1.0027379093) / 86400.0;

    /* Julian Date */
    dj = dj0 + djd;
    if (dj < dj0)
	dj = dj + (1.0 / 1.0027379093);

    return (dj);
}



/* GST2FD - calculate current UT given Greenwich Sidereal Time
 *	    plus date in FITS ISO format (yyyy-mm-ddShh:mm:ss.ss)
 *	    Return UT date and time in FITS ISO format
 */

char *
gst2fd (string)

char *string;		/* UT Date, GST as yyyy-mm-ddShh:mm:ss.ss */
{
    double sdj, dj;

    sdj = fd2jd (string);

    dj = gst2jd (sdj);

    return (jd2fd (dj));
}


/* GST2JD - calculate current UT given Greenwich Sidereal Time
 *	    plus date as Julian Date (JD at 0:00 UT + sidereal time)
 *	    Return UT date and time as Julian Date
 */

double
gst2jd (sdj)

double sdj;		/* UT Date, GST as Julian Date */
{
    double dj, tsd, djd, st0, dj0, eqnx;

    dj0 = (double) ((int) sdj) + 0.5;

    /* Greenwich Mean Sidereal Time at 0:00 UT in seconds */
    st0 = jd2mst (dj0);

    /* Mean Sidereal Time in seconds */
    tsd = (sdj - dj0) * 86400.0;
    if (tsd < 0.0)
	tsd = tsd + 86400.0;

    /* Convert to fraction of a day since 0:00 UT */
    djd = ((tsd - st0) / 1.0027379093) / 86400.0;

    /* Julian Date */
    dj = dj0 + djd;

    /* Equation of the equinoxes (converted to UT seconds) */
    eqnx = eqeqnx (dj) / 1.002737909;

    dj = dj - eqnx / 86400.0;
    if (dj < dj0)
	dj = dj + 1.0;

    return (dj);
}


/* LST2DT - calculate current UT given Local Sidereal Time as hh.mmsss
 *	    plus date as yyyy.mmdd
 *	    Return UT time as hh.mmssss
 */

double
lst2dt (date0, time0)

double date0;	/* UT date as yyyy.mmdd */
double time0;	/* LST as hh.mmssss */
{
    double gst;	/* Greenwich Sidereal Time in seconds since 0:00 */
    double lst;	/* Local Sidereal Time in seconds since 0:00 */
    double date1; /* UT date as yyyy.mmdd */
    double time1; /* UT as hh.mmssss */
    double tsec0, gst0, tsd, tsec;

    /* Greenwich Sidereal Time at 0:00 UT */
    tsec0 = dt2ts (date0, 0.0);
    gst0 = ts2gst (tsec0);

    /* Current Greenwich Sidereal Time in seconds */
    /* (convert longitude from degrees to seconds of time) */
    lst = dt2ts (0.0, time0);
    gst = lst + (longitude * 240.0);

    /* Time since 0:00 UT */
    tsd = (gst - gst0) / 1.0027379093;

    /* UT date and time */
    tsec = tsec0 + tsd;
    ts2dt (tsec, &date1, &time1);

    return (time1);
}


/* TS2GST - calculate Greenwich Sidereal Time given Universal Time
 *	    in seconds since 1951-01-01T0:00:00
 *	    Return sidereal time of day in seconds
 */

double
ts2gst (tsec)

double tsec;	/* time since 1950.0 in UT seconds */
{
    double gst;	/* Greenwich Sidereal Time in seconds since 0:00 */
    double tsd, eqnx, dj;
    int its;

    /* Elapsed time as of 0:00 UT */
    if (tsec >= 0.0) {
	its = (int) (tsec + 0.5);
	tsd = (double) (its % 86400);
	}
    else {
	its = (int) (-tsec + 0.5);
	tsd = (double) (86400 - (its % 86400));
	}

    /* Mean sidereal time */
    gst = ts2mst (tsec);

    /* Equation of the equinoxes */
    dj = ts2jd (tsec);
    eqnx = eqeqnx (dj);

    /* Apparent sidereal time at 0:00 ut */
    gst = gst + eqnx;

    /* Current sidereal time */
    gst = gst + (tsd * 1.0027379093);
    gst = dmod (gst,86400.0);

    return (gst);
}


/* FD2MST-- convert from FITS date Mean Sidereal Time */

char *
fd2mst (string)

char	*string;	/* FITS date string, which may be:
			  fractional year
			  dd/mm/yy (FITS standard before 2000)
			  dd-mm-yy (nonstandard use before 2000)
			  yyyy-mm-dd (FITS standard after 1999)
			  yyyy-mm-ddThh:mm:ss.ss (FITS standard after 1999) */
{
    double gsec, date, time, dj;

    dj = fd2jd (string);
    gsec = jd2mst (dj);
    ts2dt (gsec, &date, &time);
    date = 0.0;
    return (dt2fd (date, time));
}


/* DT2MST-- convert from UT as yyyy.mmdd hh.mmssss to Mean Sidereal Time
	    in the same format */

void
dt2mst (date, time)
double  *date;  /* Date as yyyy.mmdd */
double  *time;  /* Time as hh.mmssxxxx
                 *if time<0, it is time as -(fraction of a day) */
{
    double date0, gsec, dj;
    date0 = *date;
    dj = dt2jd (*date, *time);
    gsec = jd2mst (dj);
    ts2dt (gsec, date, time);
    *date = date0;
    return;
}


/* TS2MST - calculate Greenwich Mean Sidereal Time given Universal Time
 *	    in seconds since 1951-01-01T0:00:00
 */

double
ts2mst (tsec)

double tsec;	/* time since 1950.0 in UT seconds */
{
    double dj;

    dj = ts2jd (tsec);
    return (jd2mst (dj));
}


/* JD2MST - Julian Date to Greenwich Mean Sidereal Time using IAU 2000
 *	    Return sideral time in seconds of time
 *	    (from USNO NOVAS package
 *	     http://aa.usno.navy.mil/software/novas/novas_info.html
 */

double
jd2mst2 (dj)

double	dj;	/* Julian Date */
{
    double dt, t, t2, t3, mst, st;

    dt = dj - 2451545.0;
    t = dt / 36525.0;
    t2 = t * t;
    t3 = t2 * t;

    /* Compute Greenwich Mean Sidereal Time in seconds */
    st = (8640184.812866 * t) +  (3155760000.0 * t) - (0.0000062 * t3)
	 + (0.093104 * t2) + 67310.54841;

    mst = dmod (st, 86400.0);
    if (mst < 0.0)
	mst = mst + 86400.0;
    return (mst);
}


/* MJD2MST - Modified Julian Date to Greenwich Mean Sidereal Time using IAU 2000
 *	    Return sideral time in seconds of time
 *	    (from USNO NOVAS package
 *	     http://aa.usno.navy.mil/software/novas/novas_info.html
 */

double
mjd2mst (dj)

double	dj;	/* Modified Julian Date */
{
    double dt, t, t2, t3, mst, st;

    dt = dj - 51544.5;
    t = dt / 36525.0;
    t2 = t * t;
    t3 = t2 * t;

    /* Compute Greenwich Mean Sidereal Time in seconds */
    st = (8640184.812866 * t) +  (3155760000.0 * t) - (0.0000062 * t3)
	 + (0.093104 * t2) + 67310.54841;

    mst = dmod (st, 86400.0);
    if (mst < 0.0)
	mst = mst + 86400.0;
    return (mst);
}


/* JD2GST - Julian Date to Greenwich Sideral Time
 *          Return sideral time in seconds of time
 *	    (Jean Meeus, Astronomical Algorithms, Willmann-Bell, 1991, pp 83-84)
 */

double
jd2gst (dj)

double	dj;	/* Julian Date */
{
    double dj0, gmt, gst, tsd, eqnx, ssd, l0;
    double ts2ss = 1.00273790935;
    int ijd;

    /* Julian date at 0:00 UT */
    ijd = (int) dj;
    dj0 = (double) ijd + 0.5;
    if (dj0 > dj) dj0 = dj0 - 1.0;

    /* Greenwich mean sidereal time at 0:00 UT in seconds */
    l0 = longitude;
    longitude = 0.0;
    gmt = jd2mst (dj0);
    longitude = l0;

    /* Equation of the equinoxes */
    eqnx = eqeqnx (dj);

    /* Apparent sidereal time at 0:00 ut */
    gst = gmt + eqnx;

    /* UT seconds since 0:00 */
    tsd = (dj - dj0) * 86400.0;
    ssd = tsd * ts2ss;

    /* Current sidereal time */
    gst = gst + ssd;
    gst = dmod (gst, 86400.0);

    return (gst);
}


/* EQEQNX - Compute equation of the equinoxes for apparent sidereal time */

double
eqeqnx (dj)

double	dj;	/* Julian Date */

{
    double dt, edj, dpsi, deps, obl, eqnx;
    double rad2tsec = 13750.98708;

    /* Convert UT to Ephemeris Time (TDB or TT)*/
    dt = utdt (dj);
    edj = dj + dt / 86400.0;

    /* Nutation and obliquity */
    compnut (edj, &dpsi, &deps, &obl);

    /* Correct obliquity for nutation */
    obl = obl + deps;

    /* Equation of the equinoxes in seconds */
    eqnx = (dpsi * cos (obl)) * rad2tsec;

    return (eqnx);
}



/* JD2MST - Julian Date to Mean Sideral Time
 *          Return sideral time in seconds of time
 *	    (Jean Meeus, Astronomical Algorithms, Willmann-Bell, 1991, pp 83-84)
 */

double
jd2mst (dj)

double	dj;	/* Julian Date */
{
    double dt, t, mst;
    double ts2ss = 1.00273790935;

    dt = dj - 2451545.0;
    t = dt / 36525.0;

    /* Compute Greenwich mean sidereal time in degrees (Meeus, page 84) */
    mst = 280.46061837 + (360.98564736629 * dt) + (0.000387933 * t * t) -
	  (t * t * t / 38710000.0);

    /* Keep degrees between 0 and 360 */
    while (mst > 360.0)
	mst = mst - 360.0;
    while (mst < 0.0)
	mst = mst + 360.0;

    /* Convert to time in seconds  (3600 / 15) */
    mst = mst * 240.0;

    /* Subtract longitude (degrees to seconds of time) */
    mst = mst - (240.0 * longitude);
    if (mst < 0.0)
	mst = mst + 86400.0;
    else if (mst > 86400.0)
	mst = mst - 86400.0;

    return (mst);
}


/*  COMPNUT - Compute nutation using the IAU 2000b model */
/*  Translated from Pat Wallace's Fortran subroutine iau_nut00b (June 26 2007)
    into C by Doug Mink on September 5, 2008 */

#define NLS	77 /* number of terms in the luni-solar nutation model */

void
compnut (dj, dpsi, deps, eps0)

double	dj;	/* Julian Date */
double *dpsi;   /* Nutation in longitude in radians (returned) */
double *deps;   /* Nutation in obliquity in radians (returned) */
double *eps0;   /* Mean obliquity in radians (returned) */

/*  This routine is translated from the International Astronomical Union's
 *  Fortran SOFA (Standards Of Fundamental Astronomy) software collection.
 *
 *  notes:
 *
 *  1) the nutation components in longitude and obliquity are in radians
 *     and with respect to the equinox and ecliptic of date.  the
 *     obliquity at j2000 is assumed to be the lieske et al. (1977) value
 *     of 84381.448 arcsec.  (the errors that result from using this
 *     routine with the iau 2006 value of 84381.406 arcsec can be
 *     neglected.)
 *
 *     the nutation model consists only of luni-solar terms, but includes
 *     also a fixed offset which compensates for certain long-period
 *     planetary terms (note 7).
 *
 *  2) this routine is an implementation of the iau 2000b abridged
 *     nutation model formally adopted by the iau general assembly in
 *     2000.  the routine computes the mhb_2000_short luni-solar nutation
 *     series (luzum 2001), but without the associated corrections for
 *     the precession rate adjustments and the offset between the gcrs
 *     and j2000 mean poles.
 *
 *  3) the full IAU 2000a (mhb2000) nutation model contains nearly 1400
 *     terms.  the IAU 2000b model (mccarthy & luzum 2003) contains only
 *     77 terms, plus additional simplifications, yet still delivers
 *     results of 1 mas accuracy at present epochs.  this combination of
 *     accuracy and size makes the IAU 2000b abridged nutation model
 *     suitable for most practical applications.
 *
 *     the routine delivers a pole accurate to 1 mas from 1900 to 2100
 *     (usually better than 1 mas, very occasionally just outside 1 mas).
 *     the full IAU 2000a model, which is implemented in the routine
 *     iau_nut00a (q.v.), delivers considerably greater accuracy at
 *     current epochs;  however, to realize this improved accuracy,
 *     corrections for the essentially unpredictable free-core-nutation
 *     (fcn) must also be included.
 *
 *  4) the present routine provides classical nutation.  the
 *     mhb_2000_short algorithm, from which it is adapted, deals also
 *     with (i) the offsets between the gcrs and mean poles and (ii) the
 *     adjustments in longitude and obliquity due to the changed
 *     precession rates.  these additional functions, namely frame bias
 *     and precession adjustments, are supported by the sofa routines
 *     iau_bi00 and iau_pr00.
 *
 *  6) the mhb_2000_short algorithm also provides "total" nutations,
 *     comprising the arithmetic sum of the frame bias, precession
 *     adjustments, and nutation (luni-solar + planetary).  these total
 *     nutations can be used in combination with an existing IAU 1976
 *     precession implementation, such as iau_pmat76, to deliver gcrs-to-
 *     true predictions of mas accuracy at current epochs.  however, for
 *     symmetry with the iau_nut00a routine (q.v. for the reasons), the
 *     sofa routines do not generate the "total nutations" directly.
 *     should they be required, they could of course easily be generated
 *     by calling iau_bi00, iau_pr00 and the present routine and adding
 *     the results.
 *
 *  7) the IAU 2000b model includes "planetary bias" terms that are fixed
 *     in size but compensate for long-period nutations.  the amplitudes
 *     quoted in mccarthy & luzum (2003), namely dpsi = -1.5835 mas and
 *     depsilon = +1.6339 mas, are optimized for the "total nutations"
 *     method described in note 6.  the luzum (2001) values used in this
 *     sofa implementation, namely -0.135 mas and +0.388 mas, are
 *     optimized for the "rigorous" method, where frame bias, precession
 *     and nutation are applied separately and in that order.  during the
 *     interval 1995-2050, the sofa implementation delivers a maximum
 *     error of 1.001 mas (not including fcn).
 *
 *  References from original Fortran subroutines:
 *
 *     Hilton, J. et al., 2006, Celest.Mech.Dyn.Astron. 94, 351
 *
 *     Lieske, J.H., Lederle, T., Fricke, W., Morando, B., "Expressions
 *     for the precession quantities based upon the IAU 1976 system of
 *     astronomical constants", Astron.Astrophys. 58, 1-2, 1-16. (1977)
 *
 *     Luzum, B., private communication, 2001 (Fortran code
 *     mhb_2000_short)
 *
 *     McCarthy, D.D. & Luzum, B.J., "An abridged model of the
 *     precession-nutation of the celestial pole", Cel.Mech.Dyn.Astron.
 *     85, 37-49 (2003)
 *
 *     Simon, J.-L., Bretagnon, P., Chapront, J., Chapront-Touze, M.,
 *     Francou, G., Laskar, J., Astron.Astrophys. 282, 663-683 (1994)
 *
 */

{ 
    double as2r = 0.000004848136811095359935899141; /* arcseconds to radians */

    double dmas2r = as2r / 1000.0;	/* milliarcseconds to radians */

    double as2pi = 1296000.0;	/* arc seconds in a full circle */

    double d2pi = 6.283185307179586476925287; /* 2pi */

    double u2r = as2r / 10000000.0;  /* units of 0.1 microarcsecond to radians */

    double dj0 = 2451545.0;	/* reference epoch (j2000), jd */

    double djc = 36525.0;	/* Days per julian century */

    /*  Miscellaneous */
    double t, el, elp, f, d, om, arg, dp, de, sarg, carg;
    double dpsils, depsls, dpsipl, depspl;
    int i, j;

    int nls = NLS; /* number of terms in the luni-solar nutation model */

    /* Fixed offset in lieu of planetary terms (radians) */
    double dpplan = - 0.135 * dmas2r;
    double deplan = + 0.388 * dmas2r;

/* Tables of argument and term coefficients */

    /* Coefficients for fundamental arguments */
    /* Luni-solar argument multipliers: */
    /*       l     l'    f     d     om */
static int nals[5*NLS]=
	    {0,    0,    0,    0,    1,
             0,    0,    2,   -2,    2,
             0,    0,    2,    0,    2,
             0,    0,    0,    0,    2,
             0,    1,    0,    0,    0,
             0,    1,    2,   -2,    2,
             1,    0,    0,    0,    0,
             0,    0,    2,    0,    1,
             1,    0,    2,    0,    2,
             0,   -1,    2,   -2,    2,
             0,    0,    2,   -2,    1,
            -1,    0,    2,    0,    2,
            -1,    0,    0,    2,    0,
             1,    0,    0,    0,    1,
            -1,    0,    0,    0,    1,
            -1,    0,    2,    2,    2,
             1,    0,    2,    0,    1,
            -2,    0,    2,    0,    1,
             0,    0,    0,    2,    0,
             0,    0,    2,    2,    2,
             0,   -2,    2,   -2,    2,
            -2,    0,    0,    2,    0,
             2,    0,    2,    0,    2,
             1,    0,    2,   -2,    2,
            -1,    0,    2,    0,    1,
             2,    0,    0,    0,    0,
             0,    0,    2,    0,    0,
             0,    1,    0,    0,    1,
            -1,    0,    0,    2,    1,
             0,    2,    2,   -2,    2,
             0,    0,   -2,    2,    0,
             1,    0,    0,   -2,    1,
             0,   -1,    0,    0,    1,
            -1,    0,    2,    2,    1,
             0,    2,    0,    0,    0,
             1,    0,    2,    2,    2,
            -2,    0,    2,    0,    0,
             0,    1,    2,    0,    2,
             0,    0,    2,    2,    1,
             0,   -1,    2,    0,    2,
             0,    0,    0,    2,    1,
             1,    0,    2,   -2,    1,
             2,    0,    2,   -2,    2,
            -2,    0,    0,    2,    1,
             2,    0,    2,    0,    1,
             0,   -1,    2,   -2,    1,
             0,    0,    0,   -2,    1,
            -1,   -1,    0,    2,    0,
             2,    0,    0,   -2,    1,
             1,    0,    0,    2,    0,
             0,    1,    2,   -2,    1,
             1,   -1,    0,    0,    0,
            -2,    0,    2,    0,    2,
             3,    0,    2,    0,    2,
             0,   -1,    0,    2,    0,
             1,   -1,    2,    0,    2,
             0,    0,    0,    1,    0,
            -1,   -1,    2,    2,    2,
            -1,    0,    2,    0,    0,
             0,   -1,    2,    2,    2,
            -2,    0,    0,    0,    1,
             1,    1,    2,    0,    2,
             2,    0,    0,    0,    1,
            -1,    1,    0,    1,    0,
             1,    1,    0,    0,    0,
             1,    0,    2,    0,    0,
            -1,    0,    2,   -2,    1,
             1,    0,    0,    0,    2,
            -1,    0,    0,    1,    0,
             0,    0,    2,    1,    2,
            -1,    0,    2,    4,    2,
            -1,    1,    0,    1,    1,
             0,   -2,    2,   -2,    1,
             1,    0,    2,    2,    1,
            -2,    0,    2,    2,    2,
            -1,    0,    0,    0,    2,
             1,    1,    2,   -2,    2};

    /* Luni-solar nutation coefficients, in 1e-7 arcsec */
    /* longitude (sin, t*sin, cos), obliquity (cos, t*cos, sin) */
static double cls[6*NLS]=
   {-172064161.0, -174666.0,  33386.0, 92052331.0,  9086.0, 15377.0,
     -13170906.0,   -1675.0, -13696.0,  5730336.0, -3015.0, -4587.0,
      -2276413.0,    -234.0,   2796.0,   978459.0,  -485.0,  1374.0,
       2074554.0,     207.0,   -698.0,  -897492.0,   470.0,  -291.0,
       1475877.0,   -3633.0,  11817.0,    73871.0,  -184.0, -1924.0,
       -516821.0,    1226.0,   -524.0,   224386.0,  -677.0,  -174.0,
        711159.0,      73.0,   -872.0,    -6750.0,     0.0,   358.0,
       -387298.0,    -367.0,    380.0,   200728.0,    18.0,   318.0,
       -301461.0,     -36.0,    816.0,   129025.0,   -63.0,   367.0,
        215829.0,    -494.0,    111.0,   -95929.0,   299.0,   132.0,
        128227.0,     137.0,    181.0,   -68982.0,    -9.0,    39.0,
        123457.0,      11.0,     19.0,   -53311.0,    32.0,    -4.0,
        156994.0,      10.0,   -168.0,    -1235.0,     0.0,    82.0,
         63110.0,      63.0,     27.0,   -33228.0,     0.0,    -9.0,
        -57976.0,     -63.0,   -189.0,    31429.0,     0.0,   -75.0,
        -59641.0,     -11.0,    149.0,    25543.0,   -11.0,    66.0,
        -51613.0,     -42.0,    129.0,    26366.0,     0.0,    78.0,
         45893.0,      50.0,     31.0,   -24236.0,   -10.0,    20.0,
         63384.0,      11.0,   -150.0,    -1220.0,     0.0,    29.0,
        -38571.0,      -1.0,    158.0,    16452.0,   -11.0,    68.0,
         32481.0,       0.0,      0.0,   -13870.0,     0.0,     0.0,
        -47722.0,       0.0,    -18.0,      477.0,     0.0,   -25.0,
        -31046.0,      -1.0,    131.0,    13238.0,   -11.0,    59.0,
         28593.0,       0.0,     -1.0,   -12338.0,    10.0,    -3.0,
         20441.0,      21.0,     10.0,   -10758.0,     0.0,    -3.0,
         29243.0,       0.0,    -74.0,     -609.0,     0.0,    13.0,
         25887.0,       0.0,    -66.0,     -550.0,     0.0,    11.0,
        -14053.0,     -25.0,     79.0,     8551.0,    -2.0,   -45.0,
         15164.0,      10.0,     11.0,    -8001.0,     0.0,    -1.0,
        -15794.0,      72.0,    -16.0,     6850.0,   -42.0,    -5.0,
         21783.0,       0.0,     13.0,     -167.0,     0.0,    13.0,
        -12873.0,     -10.0,    -37.0,     6953.0,     0.0,   -14.0,
        -12654.0,      11.0,     63.0,     6415.0,     0.0,    26.0,
        -10204.0,       0.0,     25.0,     5222.0,     0.0,    15.0,
         16707.0,     -85.0,    -10.0,      168.0,    -1.0,    10.0,
         -7691.0,       0.0,     44.0,     3268.0,     0.0,    19.0,
        -11024.0,       0.0,    -14.0,      104.0,     0.0,     2.0,
          7566.0,     -21.0,    -11.0,    -3250.0,     0.0,    -5.0,
         -6637.0,     -11.0,     25.0,     3353.0,     0.0,    14.0,
         -7141.0,      21.0,      8.0,     3070.0,     0.0,     4.0,
         -6302.0,     -11.0,      2.0,     3272.0,     0.0,     4.0,
          5800.0,      10.0,      2.0,    -3045.0,     0.0,    -1.0,
          6443.0,       0.0,     -7.0,    -2768.0,     0.0,    -4.0,
         -5774.0,     -11.0,    -15.0,     3041.0,     0.0,    -5.0,
         -5350.0,       0.0,     21.0,     2695.0,     0.0,    12.0,
         -4752.0,     -11.0,     -3.0,     2719.0,     0.0,    -3.0,
         -4940.0,     -11.0,    -21.0,     2720.0,     0.0,    -9.0,
          7350.0,       0.0,     -8.0,      -51.0,     0.0,     4.0,
          4065.0,       0.0,      6.0,    -2206.0,     0.0,     1.0,
          6579.0,       0.0,    -24.0,     -199.0,     0.0,     2.0,
          3579.0,       0.0,      5.0,    -1900.0,     0.0,     1.0,
          4725.0,       0.0,     -6.0,      -41.0,     0.0,     3.0,
         -3075.0,       0.0,     -2.0,     1313.0,     0.0,    -1.0,
         -2904.0,       0.0,     15.0,     1233.0,     0.0,     7.0,
          4348.0,       0.0,    -10.0,      -81.0,     0.0,     2.0,
         -2878.0,       0.0,      8.0,     1232.0,     0.0,     4.0,
         -4230.0,       0.0,      5.0,      -20.0,     0.0,    -2.0,
         -2819.0,       0.0,      7.0,     1207.0,     0.0,     3.0,
         -4056.0,       0.0,      5.0,       40.0,     0.0,    -2.0,
         -2647.0,       0.0,     11.0,     1129.0,     0.0,     5.0,
         -2294.0,       0.0,    -10.0,     1266.0,     0.0,    -4.0,
          2481.0,       0.0,     -7.0,    -1062.0,     0.0,    -3.0,
          2179.0,       0.0,     -2.0,    -1129.0,     0.0,    -2.0,
          3276.0,       0.0,      1.0,       -9.0,     0.0,     0.0,
         -3389.0,       0.0,      5.0,       35.0,     0.0,    -2.0,
          3339.0,       0.0,    -13.0,     -107.0,     0.0,     1.0,
         -1987.0,       0.0,     -6.0,     1073.0,     0.0,    -2.0,
         -1981.0,       0.0,      0.0,      854.0,     0.0,     0.0,
          4026.0,       0.0,   -353.0,     -553.0,     0.0,  -139.0,
          1660.0,       0.0,     -5.0,     -710.0,     0.0,    -2.0,
         -1521.0,       0.0,      9.0,      647.0,     0.0,     4.0,
          1314.0,       0.0,      0.0,     -700.0,     0.0,     0.0,
         -1283.0,       0.0,      0.0,      672.0,     0.0,     0.0,
         -1331.0,       0.0,      8.0,      663.0,     0.0,     4.0,
          1383.0,       0.0,     -2.0,     -594.0,     0.0,    -2.0,
          1405.0,       0.0,      4.0,     -610.0,     0.0,     2.0,
          1290.0,       0.0,      0.0,     -556.0,     0.0,     0.0};

    /* Interval between fundamental epoch J2000.0 and given date (JC) */
    t = (dj - dj0) / djc;

/* Luni-solar nutation */

/* Fundamental (delaunay) arguments from Simon et al. (1994) */

    /* Mean anomaly of the moon */
    el  = fmod (485868.249036 + (1717915923.2178 * t), as2pi) * as2r;

    /* Mean anomaly of the sun */
    elp = fmod (1287104.79305 + (129596581.0481 * t), as2pi) * as2r;

    /* Mean argument of the latitude of the moon */
    f   = fmod (335779.526232 + (1739527262.8478 * t), as2pi) * as2r;

    /* Mean elongation of the moon from the sun */
    d   = fmod (1072260.70369 + (1602961601.2090 * t), as2pi ) * as2r;

    /* Mean longitude of the ascending node of the moon */
    om  = fmod (450160.398036 - (6962890.5431 * t), as2pi ) * as2r;

    /* Initialize the nutation values */
    dp = 0.0;
    de = 0.0;

    /* Summation of luni-solar nutation series (in reverse order) */
    for (i = nls; i > 0; i=i-1) {
	j = i - 1;

	/* Argument and functions */
	arg = fmod ( (double) (nals[5*j]) * el +
		     (double) (nals[1+5*j]) * elp +
		     (double) (nals[2+5*j]) * f +
		     (double) (nals[3+5*j]) * d +
		     (double) (nals[4+5*j]) * om, d2pi);
	sarg = sin (arg);
	carg = cos (arg);

	/* Terms */
	dp = dp + (cls[6*j] + cls[1+6*j] * t) * sarg + cls[2+6*j] * carg;
	de = de + (cls[3+6*j] + cls[4+6*j] * t) * carg + cls[5+6*j] * sarg;
	}

    /* Convert from 0.1 microarcsec units to radians */
    dpsils = dp * u2r;
    depsls = de * u2r;

/* In lieu of planetary nutation */

    /* Fixed offset to correct for missing terms in truncated series */
    dpsipl = dpplan;
    depspl = deplan;

/* Results */

    /* Add luni-solar and planetary components */
    *dpsi = dpsils + dpsipl;
    *deps = depsls + depspl;

    /* Mean Obliquity in radians (IAU 2006, Hilton, et al.) */
    *eps0 = ( 84381.406     +
	    ( -46.836769    +
	    (  -0.0001831   +
	    (   0.00200340  +
	    (  -0.000000576 +
	    (  -0.0000000434 ) * t ) * t ) * t ) * t ) * t ) * as2r;
}


/* ISDATE - Return 1 if string is an old or ISO FITS standard date */

int
isdate (string)

char	*string; /* Possible FITS date string, which may be:
			dd/mm/yy (FITS standard before 2000)
			dd-mm-yy (nonstandard FITS use before 2000)
			yyyy-mm-dd (FITS standard after 1999)
			yyyy-mm-ddThh:mm:ss.ss (FITS standard after 1999) */

{
    int iyr = 0;	/* year (returned) */
    int imon = 0;	/* month (returned) */
    int iday = 0;	/* day (returned) */
    int i;
    char *sstr, *dstr, *tstr, *nval;

    /* Translate string from ASCII to binary */
    if (string == NULL) 
	return (0);

    sstr = strchr (string,'/');
    dstr = strchr (string,'-');
    if (dstr == string)
	dstr = strchr (string+1,'-');
    tstr = strchr (string,'T');

    /* Original FITS date format: dd/mm/yy */
    if (sstr > string) {
	*sstr = '\0';
	iday = (int) atof (string);
	*sstr = '/';
	nval = sstr + 1;
	sstr = strchr (nval,'/');
	if (sstr == NULL)
	    sstr = strchr (nval,'-');
	if (sstr > string) {
	    *sstr = '\0';
	    imon = (int) atof (nval);
	    *sstr = '/';
	    nval = sstr + 1;
	    iyr = (int) atof (nval);
	    if (iyr < 1000)
		iyr = iyr + 1900;
	    }
	if (imon > 0 && iday > 0)
	    return (1);
	else
	    return (0);
	}

    /* New FITS date format: yyyy-mm-ddThh:mm:ss[.sss] */
    else if (dstr > string) {
	*dstr = '\0';
	iyr = (int) atof (string);
	nval = dstr + 1;
	*dstr = '-';
	dstr = strchr (nval,'-');
	imon = 0;
	iday = 0;

	/* Decode year, month, and day */
	if (dstr > string) {
	    *dstr = '\0';
	    imon = (int) atof (nval);
	    *dstr = '-';
	    nval = dstr + 1;
	    if (tstr > string)
		*tstr = '\0';
	    iday = (int) atof (nval);
	    if (tstr > string)
		*tstr = 'T';
	    }

	/* If day is > 31, it is really year in old format */
	if (iday > 31) {
	    i = iyr;
	    if (iday < 100)
		iyr = iday + 1900;
	    else
		iyr = iday;
	    iday = i;
	    }
	if (imon > 0 && iday > 0)
	    return (1);
	else
	    return (0);
	}

    /* If FITS date is entered as an epoch, return 0 anyway */
    else
	return (0);
}


/* Round seconds and make sure date and time numbers are within limits */

static void
fixdate (iyr, imon, iday, ihr, imn, sec, ndsec)

int	*iyr;	/* year (returned) */
int	*imon;	/* month (returned) */
int	*iday;	/* day (returned) */
int	*ihr;	/* hours (returned) */
int	*imn;	/* minutes (returned) */
double	*sec;	/* seconds (returned) */
int	ndsec;	/* Number of decimal places in seconds (0=int) */
{
    double days;

    /* Round seconds to 0 - 4 decimal places (no rounding if <0, >4) */
    if (ndsec == 0)
	*sec = dint (*sec + 0.5);
    else if (ndsec < 2)
	*sec = dint (*sec * 10.0 + 0.5) / 10.0;
    else if (ndsec < 3)
	*sec = dint (*sec * 100.0 + 0.5) / 100.0;
    else if (ndsec < 4)
	*sec = dint (*sec * 1000.0 + 0.5) / 1000.0;
    else if (ndsec < 5)
	*sec = dint (*sec * 10000.0 + 0.5) / 10000.0;

    /* Adjust minutes and hours */
    if (*sec > 60.0) {
	*sec = *sec - 60.0;
	*imn = *imn + 1;
	}
    if (*imn > 60) {
	*imn = *imn - 60;
	*ihr = *ihr + 1;
	}

    /* Return if no date */
    if (*iyr == 0 && *imon == 0 && *iday == 0)
	return;

   /* Adjust date */
    if (*ihr > 23) {
	*ihr = *ihr - 24;
	*iday = *iday + 1;
	}
    days = caldays (*iyr, *imon);
    if (*iday > days) {
	*iday = *iday - days;
	*imon = *imon + 1;
	}
    if (*iday < 1) {
	*imon = *imon - 1;
	if (*imon < 1) {
	    *imon = *imon + 12;
	    *iyr = *iyr - 1;
	    }
	days = caldays (*iyr, *imon);
	*iday = *iday + days;
	}
    if (*imon < 1) {
	*imon = *imon + 12;
	*iyr = *iyr - 1;
	days = caldays (*iyr, *imon);
	if (*iday > days) {
	    *iday = *iday - days;
	    *imon = *imon + 1;
	    }
	}
    if (*imon > 12) {
	*imon = *imon - 12;
	*iyr = *iyr + 1;
	}
    return;
}


/* Calculate days in month 1-12 given year (Gregorian calendar only) */

static int
caldays (year, month)

int	year;	/* 4-digit year */
int	month;	/* Month (1=January, 2=February, etc.) */
{
    if (month < 1) {
	month = month + 12;
	year = year + 1;
	}
    if (month > 12) {
	month = month - 12;
	year = year + 1;
	}
    switch (month) {
	case 1:
	    return (31);
	case 2:
	    if (year%400 == 0)
		return (29);
	    else if (year%100 == 0)
		return (28);
	    else if (year%4 == 0)
		return (29);
	    else
		return (28);
	case 3:
	    return (31);
	case 4:
	    return (30);
	case 5:
	    return (31);
	case 6:
	    return (30);
	case 7:
	    return (31);
	case 8:
	    return (31);
	case 9:
	    return (30);
	case 10:
	    return (31);
	case 11:
	    return (30);
	case 12:
	    return (31);
	default:
	    return (0);
	}
}


static double
dint (dnum)

double	dnum;
{
    double dn;

    if (dnum < 0.0)
	dn = -floor (-dnum);
    else
	dn = floor (dnum);
    return (dn);
}


static double
dmod (dnum, dm)

double	dnum, dm;
{
    double dnumx, dnumi, dnumf;
    if (dnum < 0.0)
	dnumx = -dnum;
    else
	dnumx = dnum;
    dnumi = dint (dnumx / dm);
    if (dnum < 0.0)
	dnumf = dnum + (dnumi * dm);
    else if (dnum > 0.0)
	dnumf = dnum - (dnumi * dm);
    else
	dnumf = 0.0;
    return (dnumf);
}

/* Jul  1 1999	New file, based on iolib/jcon.f and iolib/vcon.f and hgetdate()
 * Oct 21 1999	Fix declarations after lint
 * Oct 27 1999	Fix bug to return epoch if fractional year input
 * Dec  9 1999	Fix bug in ts2jd() found by Pete Ratzlaff (SAO)
 * Dec 17 1999	Add all unimplemented conversions
 * Dec 20 1999	Add isdate(); leave date, time strings unchanged in fd2i()
 * Dec 20 1999	Make all fd2*() subroutines deal with time alone
 *
 * Jan  3 2000	In old FITS format, year 100 is assumed to be 2000
 * Jan 11 2000	Fix epoch to date conversion so .0 is 0:00, not 12:00
 * Jan 21 2000	Add separate Besselian and Julian epoch computations
 * Jan 28 2000	Add Modified Julian Date conversions
 * Mar  2 2000	Implement decimal places for FITS date string
 * Mar 14 2000	Fix bug in dealing with 2000-02-29 in ts2i()
 * Mar 22 2000	Add lt2* and ut2* to get current time as local and UT
 * Mar 24 2000	Fix calloc() calls
 * Mar 24 2000	Add tsi2* and tsu2* to convert IRAF and Unix seconds
 * May  1 2000	In old FITS format, all years < 1000 get 1900 added to them
 * Aug  1 2000	Make ep2jd and jd2ep consistently starting at 1/1 0:00
 *
 * Jan 11 2001	Print all messages to stderr
 * May 21 2001	Add day of year conversions
 * May 25 2001	Allow fraction of day in FITS date instead of time
 *
 * Apr  8 2002	Change all long declaration to time_t
 * May 13 2002	Fix bugs found by lint
 * Jul  5 2002	Fix bug in fixdate() so fractional seconds come out
 * Jul  8 2002	Fix rounding bug in t2i()
 * Jul  8 2002	Try Fliegel and Van Flandern's algorithm for JD to UT date
 * Jul  8 2002	If first character of string is -, check for other -'s in isdate
 * Sep 10 2002	Add ET/TDT/TT conversion from UT subroutines
 * Sep 10 2002	Add sidereal time conversions
 *
 * Jan 30 2003	Fix typo in ts2gst()
 * Mar  7 2003	Add conversions for heliocentric julian dates
 * May 20 2003	Declare nd in setdatedec()
 * Jul 18 2003	Add code to parse Las Campanas dates
 *
 * Mar 24 2004	If ndec > 0, add UT to FITS date even if it is 0:00:00
 *
 * Oct 14 2005	Add tsd2fd() and tsd2dt()
 *
 * May  3 2006	Drop declaration of unused variables
 * Jun 20 2006	Initialized uninitialized variables
 * Aug  2 2006	Add local sidereal time
 * Sep 13 2006	Add more local sidereal time subroutines
 * Oct  2 2006	Add UT to old FITS date conversions
 * Oct  6 2006	Add eqeqnx() to compute equation of the equinoxes
 *
 * Jan  8 2007	Remove unused variables
 *
 * Sep  5 2008	Replace nutation with IAU 2006 model translated from SOFA
 * Sep  9 2008	Add ang2hr(), ang2deg(), hr2ang(), deg2ang()
 * Sep 10 2008	Add longitude to mean standard time (default = Greenwich)
 * Oct  8 2008	Clean up sidereal time computations
 *
 * Sep 24 2009	Add end to comment "Coefficients for fundamental arguments"
 */
