/* File getdate.c
 * April 6, 2009
 * By Doug Mink, Harvard-Smithsonian Center for Astrophysics
 * Send bug reports to dmink@cfa.harvard.edu

   Copyright (C) 1999-2009
   Smithsonian Astrophysical Observatory, Cambridge, MA USA

   This program is free software; you can redistribute it and/or
   modify it under the terms of the GNU General Public License
   as published by the Free Software Foundation; either version 2
   of the License, or (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software Foundation,
   Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>
#include <math.h>
#include <time.h>
#include "libwcs/wcs.h"
#include "libwcs/fitsfile.h"

#define DTVIG	1	/* Vigesimal (yyyy.mmdd hh.mmssss) */
#define DTFITS	2	/* FITS date (new format) */
#define DTJD	3	/* Julian date */
#define DTMJD	4	/* Modified Julian date */
#define DT1950	5	/* Seconds since 1950.0 */
#define DTEP	6	/* Bessellian Epoch */
#define DTEPB	7	/* Bessellian Epoch */
#define DTEPJ	8	/* Julian Epoch */
#define DTOF	9	/* Old FITS date and time */
#define DTOFD	10	/* Old FITS date only */
#define DTOFT	11	/* Old FITS time only */
#define DTLT	12	/* Current local time */
#define DTUT	13	/* Current Universal Time */
#define DTIRAF	14	/* IRAF seconds since 1980-01-01 0:00 */
#define DTUNIX	15	/* Unix seconds since 1970-01-01 0:00 */
#define DTDOY	16	/* Year and day of year (including fraction) */
#define DTHJD	17	/* Heliocentric Julian Date (requires RA, Dec) */
#define DTMHJD	18	/* Modified Heliocentric Julian Date (requires RA, Dec) */
#define DTNOW	19	/* Current time (for input only) */
#define DTDSEC	20	/* Seconds of day (for input only) */
#define DTMST	21	/* Mean Sidereal Time */
#define DTGST	22	/* Greenwich Sidereal Time (with nutation) */
#define DTLST	23	/* Local Sidereal Time (requires longitude) */
#define DTET	24	/* Local Sidereal Time (requires longitude) */
#define DTANG	25	/* Angle in fractional degrees */
#define DTHR	26	/* Angle in sexigesimal hours (hh:mm:ss.ss) */
#define DTDEG	27	/* Angle in sexigesimal degrees (dd:mm:ss.ss) */

#define UT	0
#define ET	1
#define GST	2
#define MST	3
#define LST	4

static char *RevMsg = "GETDATE WCSTools 3.8.1, 14 December 2009, Doug Mink SAO";

static void usage();
static void ConvertDate();
extern void setdatedec();

static int verbose = 0;		/* Verbose/debugging flag */
static int version = 0;	/* If 1, print only program name and version */
static int ndec = 5;	/* Number of decimal places in output */
static int dateonly = 0; /* If 1, print date without time */
static int timeonly = 0; /* If 1, print time without date */
static char *outform0;
static int nftok = 2;	/* Number of tokens for FITS date format */
static int outtime = UT;	/* Type of time to be output */
static double ra = 0.0;
static double dec = 0.0;
static int coorsys = WCS_J2000;
extern void sdatedec();
static double longitude = -1.0;

int
main (ac, av)
int ac;
char **av;
{
    char *str;
    char *datestring;
    char *timestring;
    int intype = 0;	/* Input date type */
    int outtype = 0;	/* Output date type */
    int appdate = 0;	/* Append date to input file */
    int typeset = 0;
    int lline;
    char line[82];
    FILE *fd;

    datestring = NULL;
    timestring = NULL;
    outform0 = NULL;

    if (ac == 1)
        usage ();

    /* Check for help or version command first */
    str = *(av+1);
    if (!str || !strcmp (str, "help") || !strcmp (str, "-help"))
	usage ();
    if (!strcmp (str, "version") || !strcmp (str, "-version")) {
	version = 1;
	usage ();
	}

    /* crack arguments */
    for (av++; --ac > 0; av++) {

	if (typeset == 0) {

	    /* Set input date format */
	    if (!strncmp (*av, "dt2", 3))
		intype = DTVIG;
	    else if (!strncmp (*av, "ang2", 4))
		intype = DTANG;
	    else if (!strncmp (*av, "deg2", 4))
		intype = DTDEG;
	    else if (!strncmp (*av, "hr2", 3))
		intype = DTHR;
	    else if (!strncmp (*av, "ep2", 3))
		intype = DTEP;
	    else if (!strncmp (*av, "epb2", 3))
		intype = DTEPB;
	    else if (!strncmp (*av, "epj2", 3))
		intype = DTEPJ;
	    else if (!strncmp (*av, "nfd2", 3)) {
		intype = DTFITS;
		nftok = 1;
		}
	    else if (!strncmp (*av, "fd2", 3))
		intype = DTFITS;
	    else if (!strncmp (*av, "jd2", 3)) {
		intype = DTJD;
		nftok = 1;
		}
	    else if (!strncmp (*av, "hjd2", 4)) {
		intype = DTHJD;
		nftok = 1;
		}
	    else if (!strncmp (*av, "mjd2", 4)) {
		intype = DTMJD;
		nftok = 1;
		}
	    else if (!strncmp (*av, "mhjd2", 5)) {
		intype = DTMHJD;
		nftok = 1;
		}
	    else if (!strncmp (*av, "now2", 4))
		intype = DTNOW;
	    else if (!strncmp (*av, "ts2", 3)) {
		intype = DT1950;
		nftok = 1;
		}
	    else if (!strncmp (*av, "tsi2", 4)) {
		intype = DTIRAF;
		nftok = 1;
		}
	    else if (!strncmp (*av, "tsd2", 4)) {
		intype = DTDSEC;
		nftok = 1;
		}
	    else if (!strncmp (*av, "tsu2", 4)) {
		intype = DTUNIX;
		nftok = 1;
		}
	    else if (!strncmp (*av, "lt2", 3))
		intype = DTLT;
	    else if (!strncmp (*av, "ut2", 3))
		intype = DTUT;
	    else if (!strncmp (*av, "doy2", 4))
		intype = DTDOY;
	    else if (!strncmp (*av, "gst2", 4))
		intype = DTGST;
	    else if (!strncmp (*av, "mst2", 4))
		intype = DTMST;
	    else if (!strncmp (*av, "lst2", 4))
		intype = DTLST;

	    /* Set output date format */
	    if (strsrch (*av, "2dt"))
		outtype = DTVIG;
	    else if (strsrch (*av, "2ang"))
		outtype = DTANG;
	    else if (strsrch (*av, "2deg"))
		outtype = DTDEG;
	    else if (strsrch (*av, "2hr"))
		outtype = DTHR;
	    else if (strsrch (*av, "2epb"))
		outtype = DTEPB;
	    else if (strsrch (*av, "2epj"))
		outtype = DTEPJ;
	    else if (strsrch (*av, "2ep"))
		outtype = DTEP;
	    else if (strsrch (*av, "2fd"))
		outtype = DTFITS;
	    else if (strsrch (*av, "2jd"))
		outtype = DTJD;
	    else if (strsrch (*av, "2hjd"))
		outtype = DTHJD;
	    else if (strsrch (*av, "2mhjd"))
		outtype = DTMHJD;
	    else if (strsrch (*av, "2mjd"))
		outtype = DTMJD;
	    else if (strsrch (*av, "2of"))
		outtype = DTOF;
	    else if (strsrch (*av, "2ofd"))
		outtype = DTOFD;
	    else if (strsrch (*av, "2oft"))
		outtype = DTOFT;
	    else if (strsrch (*av, "2tsi"))
		outtype = DTIRAF;
	    else if (strsrch (*av, "2tsu"))
		outtype = DTUNIX;
	    else if (strsrch (*av, "2ts"))
		outtype = DT1950;
	    else if (strsrch (*av, "2doy"))
		outtype = DTDOY;
	    else if (strsrch (*av, "2mst"))
		outtype = DTMST;
	    else if (strsrch (*av, "2gst"))
		outtype = DTGST;
	    else if (strsrch (*av, "2lst"))
		outtype = DTLST;
	    else if (strsrch (*av, "2et"))
		outtype = DTET;

	    if (intype > 0 || outtype > 0) {
		typeset = 1;
		continue;
		}
	    }

	/* Set RA, Dec, and equinox if WCS-generated argument */
	if (ac > 3 &&
		 strsrch (*av,":") != NULL && strsrch (*(av+1),":") != NULL &&
		 (strcsrch(*(av+2),"j")!=NULL || strcsrch(*(av+2),"b")!=NULL)) {
	    if (ac < 3)
		usage();
	    else {
		ra = str2ra (*av);
		ac--;
		dec = str2dec (*++av);
		ac--;
		coorsys = wcscsys (*++av);
		}
	    }

	/* Read command line parameters */
	else if (*(str = *av) == '-' && !isdate(*av) && !isnum(*av)) {
	    char c;
	    while ((c = *++str))
	    switch (c) {

		case 'a':	/* Append date to input file */
		    appdate++;
		    break;

		case 'd':	/* Print date without time */
		    dateonly++;
		    break;

		case 'e':	/* Print Ephemeris Time (ET, TDT, TT) */
		    outtime = ET;
		    break;

		case 'f':	/* Output format */
		    if (ac < 2)
			usage();
		    outform0 = *++av;
		    ac--;
		    break;

		case 'h':	/* Longitude in hours */
		    if (ac < 2)
			usage();
		    longitude = str2ra (*++av);
		    setlongitude (longitude);
		    ac--;
		    break;

		case 'l':	/* Longitude in degrees */
		    if (ac < 2)
			usage();
		    longitude = str2dec (*++av);
		    setlongitude (longitude);
		    ac--;
		    break;

		case 'n':	/* Number of decimal places in output */
		    if (ac < 2)
			usage();
		    ndec = (int) (atof (*++av));
		    setdatedec (ndec);
		    ac--;
		    break;

		case 't':	/* Print time without date */
		    timeonly++;
		    break;

		case 'v':	/* More verbosity */
		    verbose++;
		    break;

		default:
		    usage ();
		    break;
		}
	    }
	else if (*(str = *av) == '@') {
	    if (strlen (str) > 1) {
		if (!strcmp (str+1, "stdin"))
		    fd = stdin;
		else if (!(fd = fopen (str+1, "r"))) {
		    if (verbose)
			fprintf (stderr, "*** Cannot read file %s\n", str+1);
		    return (-1);
		    }
		datestring = (char *) calloc (80,1);
		timestring = (char *) calloc (80,1);
		while (fgets (line, 80, fd)) {
		    lline = strlen (line);
		    if ((int)line[lline-1] < 32)
			line[lline-1] = (char) 0;
		    if (appdate)
			printf ("%s ", line);
		    if (nftok > 1)
			sscanf (line, "%s %s", datestring, timestring);
		    else
			sscanf (line, "%s", datestring);
		    ConvertDate (intype, outtype, datestring, timestring);
		    }
		free (datestring);
		free (timestring);
		}
	    }
		
	else if (datestring == NULL) {
	    datestring = *av;
	    if (ac > 1 && isdate (datestring) == 1) {
		timestring = *(av+1);
		if (strchr (timestring, ':')) {
		    av++;
		    ac--;
		    /* timestring = NULL; */
		    }
		}
	    if (intype == DTDSEC) {
		ConvertDate (intype, outtype, datestring, datestring);
		datestring = NULL;
		timestring = NULL;
		}
	    else if ((intype != DTVIG && intype != DTDOY) || ac == 1) {
		ConvertDate (intype, outtype, datestring, timestring);
		datestring = NULL;
		timestring = NULL;
		}
	    else if (intype == DTVIG && isnum (timestring)) {
		ConvertDate (intype, outtype, datestring, timestring);
		datestring = NULL;
		timestring = NULL;
		av++;
		ac--;
		}
	    }
	else if (timestring == NULL) {
	    timestring = *av;
	    ConvertDate (intype, outtype, datestring, timestring);
	    datestring = NULL;
	    timestring = NULL;
	    }
	}
    if (intype == DTLT || intype == DTUT || intype == DTNOW)
	ConvertDate (intype, outtype, datestring, timestring);

    return (0);
}

static void
usage ()

{
    fprintf (stderr,"%s\n", RevMsg);
    if (version)
	exit (-1);
    fprintf (stderr,"Convert date and time between various formats\n");
    fprintf (stderr,"Usage: [-dv][-n dec][-f format] itype2otype [date and/or time] [ra dec sys]\n");
    fprintf (stderr,"       [-dv][-n dec][-f format] itype2otype @file\n");
    fprintf(stderr,"  itype: nfd=ISOFITS fd=FITS, dt=yyyy.mmdd, hr=hh:mm:ss, deg=dd:mm:ss\n");
    fprintf(stderr,"         jd=Julian Date, mjd=Modified Julian Date\n");
    fprintf(stderr,"         jhd=Heliocentric Julian Date, mhjd=Modified HJD\n");
    fprintf(stderr,"         ep=epoch, epj=Julian epoch, epb=Besselian epoch\n");
    fprintf(stderr,"         lt=local time, ut=UT, ts=seconds since 1950-01-01\n");
    fprintf(stderr,"         now=current time, ang=fractional degrees\n");
    fprintf(stderr,"  otype: fd=FITS, dt=yyyy.mmdd, jd=Julian Date, mjd=Modified Julian Date\n");
    fprintf(stderr,"         jhd=Heliocentric Julian Date, mhjd=Modified HJD\n");
    fprintf(stderr,"         hr=hh:mm:ss, deg=dd:mm:ss, ang=fractional degrees\n");
    fprintf(stderr,"         ep=epoch, epj=Julian epoch, epb=Besselian epoch\n");
    fprintf(stderr,"         ts=seconds since 1950-01-01, tsu=Unix sec, tsi=IRAF sec\n");
    fprintf(stderr,"         gst=Greenwich Sidereal Time, lst=Local Sidereal Time\n");
    fprintf(stderr,"  @file: First one or two columns are in itype format\n");
    fprintf(stderr,"  ra dec sys:  Need for Heliocentric conversions\n");
    fprintf(stderr,"     -a: Append date to input file, if there is one\n");
    fprintf(stderr,"     -d: Print date without time\n");
    fprintf(stderr,"     -e: Print output as ET/TDT/TT converting from UT\n");
    fprintf(stderr,"     -f: Format for output number (C printf)\n");
    fprintf(stderr,"     -h hours: Longitude in hours, west positive\n");
    fprintf(stderr,"     -l degrees: Longitude in degrees, west positive\n");
    fprintf(stderr,"     -n: Number of decimal places in sec, epoch, JD\n");
    fprintf(stderr,"     -t: Print time without date\n");
    fprintf(stderr,"     -v: Verbose\n");
    exit (1);
}

static void
ConvertDate (intype, outtype, datestring, timestring)

int	intype;		/* Type of input date */
int	outtype;	/* Type of output date */
char	*datestring;	/* Input date string */
char	*timestring;	/* Input time string */

{
    double vdate, vtime, epoch, jd, ts, jd1, ts1, epoch1;
    double doy, vdoy;
    int year, vyear;
    int lfd, oldfits;
    char outform[16];
    char *fitsdate, *newfdate, *stdate;
    char temp[64];
    char fyear[16];
    char ts0[8];
    char *tchar;
    int its, its1;
    time_t lts;

    strcpy (ts0, "00:00:00");
    if (outform0 == NULL) {
	if (outtype == DTUNIX || outtype == DTIRAF)
	    strcpy (outform,"%d\n");
	else
	    sprintf (outform,"%%.%df\n",ndec);
	}
    else {
	strncpy (outform,outform0, 15);
	strcat (outform, "\n");
	}
    if (outtype < 1)
	outtype = DTFITS;

    if (datestring != NULL) {
	if (!strcmp (datestring, "now")) {
	    intype = DTFITS;

	    /* Get current UTC */
	    newfdate = ut2fd ();
	    datestring = newfdate;
	    }
	}
    if (outtype == DTLST) {
/* 	if (longitude < 0.0) {
	    setlongitude (0.0);
	    fprintf (stderr, "*** Greenwich longitude used for Local Sidereal Time ***\n");
	    } */
	outtime = LST;
	if (intype == DTFITS || intype == DTVIG)
	    outtype = intype;
	else
	    outtype = DTFITS;
	}
    else if (outtype == DTGST) {
	outtime = GST;
	if (intype == DTFITS || intype == DTVIG)
	    outtype = intype;
	else
	    outtype = DTFITS;
	}
    else if (outtype == DTMST) {
	outtime = MST;
	if (intype == DTFITS || intype == DTVIG)
	    outtype = intype;
	else
	    outtype = DTFITS;
	}
    else if (outtype == DTET) {
	outtime = ET;
	outtype = intype;
	}

    switch (intype) {

	/* Angle in degrees */
	case DTANG:
	    if (verbose)
		printf ("%s -> ", datestring);
	    switch (outtype) {
		case DTHR:
		    vtime = atof (datestring);
		    ang2hr (vtime, 64, temp);
		    printf ("%s\n", temp);
		    break;
		case DTDEG:
		    vtime = atof (datestring);
		    ang2deg (vtime, 64, temp);
		    printf ("%s\n", temp);
		    break;
		default:
		    printf ("*** Unknown output type %d\n", outtype);
		}
	    break;

	/* Angle as dd:mm:ss */
	case DTDEG:
	    if (verbose)
		printf ("%s -> ", datestring);
	    switch (outtype) {
		case DTANG:
		    vtime = deg2ang (datestring);
		    printf (outform, vtime);
		    break;
		case DTHR:
		    vtime = deg2ang (datestring);
		    ang2hr (vtime, 64, temp);
		    printf ("%s\n", temp);
		    break;
		default:
		    printf ("*** Unknown output type %d\n", outtype);
		}
	    break;

	/* Angle as hh:mm:ss */
	case DTHR:
	    if (verbose)
		printf ("%s -> ", datestring);
	    switch (outtype) {
		case DTANG:
		    vtime = hr2ang (datestring);
		    printf (outform, vtime);
		    break;
		case DTDEG:
		    vtime = hr2ang (datestring);
		    ang2deg (vtime, 64, temp);
		    printf ("%s\n", temp);
		    break;
		default:
		    printf ("*** Unknown output type %d\n", outtype);
		}
	    break;


	/* Vigesimal date and time (yyyy.mmdd hh.mmssss) */
	case DTVIG:
	    if (datestring != NULL) {
		if (strcmp (datestring, "now")) {
		    vdate = atof (datestring);
		    if (timestring != NULL) 
			vtime = atof (timestring);
		    else {
			timestring = ts0;
			vtime = 0.0;
			}
		    }
		if (verbose)
		    printf ("%s %s -> ", datestring, timestring);

		switch (outtype) {
		    case DTEP:
			epoch = dt2ep (vdate, vtime);
			printf (outform, epoch);
			break;
		    case DTEPB:
			epoch = dt2epb (vdate, vtime);
			printf (outform, epoch);
			break;
		    case DTEPJ:
			epoch = dt2epj (vdate, vtime);
			printf (outform, epoch);
			break;
		    case DTFITS:
			fitsdate = dt2fd (vdate, vtime);
			if (outtime == ET) newfdate = fd2et (fitsdate);
			else if (outtime == GST) newfdate = fd2gst (fitsdate);
			else if (outtime == MST) newfdate = fd2mst (fitsdate);
			else if (outtime == LST) newfdate = fd2lst (fitsdate);
			else newfdate = fitsdate;
			tchar = strchr (newfdate, 'T');
			if (tchar == NULL)
			    tchar = strchr (newfdate, 'S');
			if (dateonly) {
			    strncpy (fyear, newfdate, 10);
			    fyear[10] = (char) 0;
			    printf ("%s\n", fyear);
			    }
			else if (timeonly) {
			    if (tchar != NULL)
				printf ("%s\n", tchar+1);
			    else
				printf ("%s\n", newfdate);
			    }
			else if (outtime==GST || outtime==MST || outtime==LST) {
			    printf ("%10sS%s\n", fyear, newfdate);
			    }
			else {
			    if (outtime == ET)
				*tchar = 'E';
			    printf ("%s\n", newfdate);
			    }
			break;
		    case DTJD:
			jd = dt2jd (vdate, vtime);
			if (outtime == ET) jd = jd2jed (jd);
			printf (outform, jd);
			break;
		    case DTMJD:
			jd = dt2mjd (vdate, vtime);
			printf (outform, jd);
			break;
		    case DTHJD:
			jd = dt2jd (vdate, vtime);
			jd = jd2hjd (jd, ra, dec, coorsys);
			printf (outform, jd);
			break;
		    case DT1950:
			ts = dt2ts (vdate, vtime);
			if (outtime == ET) ts = ts2ets (ts);
			printf (outform, ts);
			break;
		    case DTIRAF:
			its = dt2tsi (vdate, vtime);
			printf (outform, its);
			break;
		    case DTUNIX:
			lts = dt2tsu (vdate, vtime);
			printf (outform, lts);
			break;
		    case DTDOY:
			dt2doy (vdate, vtime, &year, &doy);
			sprintf (temp, outform, doy);
			printf ("%04d %s\n", year, temp);
			break;
		    case DTVIG:
			if (outtime == ET) dt2et (&vdate, &vtime);
			else if (outtime == GST) dt2gst (&vdate, &vtime);
			else if (outtime == MST) dt2mst (&vdate, &vtime);
			else if (outtime == LST) dt2lst (&vdate, &vtime);
			if (dateonly)
			    printf ("%9.4f\n", vdate);
			else if (vdate == 0.0 || timeonly)
			    printf ("%10.7f\n", vtime);
			else
			    printf ("%9.4f %10.7f\n", vdate, vtime);
			break;
		    default:
			printf ("*** Unknown output type %d\n", outtype);
		    }
		}
	    break;

	/* FITS format date and time, either old or post-1999 ISO */
	case DTFITS:
	case DTNOW:
	case DTLT:
	    if (intype == DTNOW) {
		newfdate = ut2fd ();
		datestring = newfdate;
		}
	    else if (intype == DTLT) {
		newfdate = lt2fd ();
		datestring = newfdate;
		}
	    if (datestring != NULL) {
		if (strchr (datestring,'/'))
		    oldfits = 1;
		else
		    oldfits = 0;
		if (timestring != NULL) {
		    if (oldfits) {
			lfd = strlen (datestring) + strlen (timestring) + 2;
			fitsdate = (char *) calloc (1, lfd);
			strcpy (fitsdate, datestring);
			strcat (fitsdate, "_");
			strcat (fitsdate, timestring);
			}
		    else {
			lfd = strlen (datestring) + strlen (timestring) + 2;
			fitsdate = (char *) calloc (1, lfd);
			strcpy (fitsdate, datestring);
			strcat (fitsdate, "T");
			strcat (fitsdate, timestring);
			}
		    }
		else
		    fitsdate = datestring;
		if (verbose)
		    printf ("%s -> ", fitsdate);
		switch (outtype) {
		    case DTEP:
			epoch = fd2ep (fitsdate);
			if (oldfits && timestring) {
			    epoch1 = fd2ep (timestring);
			    epoch = epoch + epoch1;
			    }
			printf (outform, epoch);
			break;
		    case DTEPB:
			epoch = fd2epb (fitsdate);
			if (oldfits && timestring) {
			    epoch1 = fd2epb (timestring);
			    epoch = epoch + epoch1;
			    }
			printf (outform, epoch);
			break;
		    case DTEPJ:
			epoch = fd2epj (fitsdate);
			if (oldfits && timestring) {
			    epoch1 = fd2epj (timestring);
			    epoch = epoch + epoch1;
			    }
			printf (outform, epoch);
			break;
		    case DTVIG:
			fd2dt (fitsdate, &vdate, &vtime);
			if (outtime == ET) dt2et (&vdate, &vtime);
			else if (outtime == GST) dt2gst (&vdate, &vtime);
			else if (outtime == MST) dt2mst (&vdate, &vtime);
			else if (outtime == LST) dt2lst (&vdate, &vtime);
			if (oldfits) {
			    if (timestring == NULL)
				vtime = 0.0;
			    else
				vtime = str2dec (timestring);
			    }
			if (dateonly)
			    printf ("%9.4f\n", vdate);
			else if (vdate == 0.0 || timeonly)
			    printf ("%10.7f\n", vtime);
			else
			    printf ("%9.4f %10.7f\n", vdate, vtime);
			break;
		    case DTFITS:
			newfdate = fd2fd (fitsdate);
			if (oldfits && timestring) {
			    strcat (newfdate, "T");
			    strcat (newfdate, timestring);
			    }
			strncpy (fyear, newfdate, 10);
			fyear[10] = (char) 0;
			if (outtime == ET) newfdate = fd2et (newfdate);
			else if (outtime == GST) newfdate = fd2gst (newfdate);
			else if (outtime == MST) newfdate = fd2mst (newfdate);
			else if (outtime == LST) newfdate = fd2lst (newfdate);
			tchar = strchr (newfdate, 'T');
			if (tchar == NULL)
			    tchar = strchr (newfdate, 'S');
			if (dateonly) {
			    strncpy (fyear, newfdate, 10);
			    fyear[10] = (char) 0;
			    printf ("%s\n", fyear);
			    }
			else if (timeonly) {
			    if (tchar != NULL)
				printf ("%s\n", tchar+1);
			    else
				printf ("%s\n", newfdate);
			    }
			else if (outtime==GST || outtime==MST || outtime==LST) {
			    printf ("%10sS%s\n", fyear, newfdate);
			    }
			else {
			    if (outtime == ET)
				*tchar = 'E';
			    printf ("%s\n", newfdate);
			    }
			break;
		    case DTJD:
			jd = fd2jd (fitsdate);
			if (oldfits && timestring) {
			    jd1 = fd2jd (timestring);
			    jd = jd + jd1;
			    }
			if (outtime == ET) jd = jd2jed (jd);
			printf (outform, jd);
			break;
		    case DTMJD:
			jd = fd2mjd (fitsdate);
			if (oldfits && timestring) {
			    jd1 = fd2jd (timestring);
			    jd = jd + jd1;
			    }
			printf (outform, jd);
			break;
		    case DTHJD:
			jd = fd2jd (fitsdate);
			if (oldfits && timestring) {
			    jd1 = fd2jd (timestring);
			    jd = jd + jd1;
			    }
			jd = jd2hjd (jd, ra, dec, coorsys);
			printf (outform, jd);
			break;
		    case DTOF:
			newfdate = fd2of (fitsdate);
			if (oldfits && timestring) {
			    free (newfdate);
			    newfdate = fd2ofd (fitsdate);
			    strcat (newfdate, " ");
			    strcat (newfdate, timestring);
			    }
			printf ("%s\n", newfdate);
			break;
		    case DTOFD:
			newfdate = fd2ofd (fitsdate);
			printf ("%s\n", newfdate);
			break;
		    case DTOFT:
			newfdate = fd2oft (fitsdate);
			if (oldfits && timestring) {
			    strcpy (newfdate, timestring);
			    }
			printf ("%s\n", newfdate);
			break;
		    case DT1950:
			ts = fd2ts (fitsdate);
			if (oldfits && timestring) {
			    ts1 = fd2ts (timestring);
			    ts = ts + ts1;
			    }
			if (outtime == ET) ts = ts2ets (ts);
			printf (outform, ts);
			break;
		    case DTIRAF:
			its = fd2tsi (fitsdate);
			if (oldfits && timestring) {
			    its1 = (int) fd2ts (timestring);
			    its = its + its1;
			    }
			printf (outform, its);
			break;
		    case DTUNIX:
			lts = fd2tsu (fitsdate);
			if (oldfits && timestring) {
			    its1 = (int) fd2ts (timestring);
			    lts = lts + (time_t) its1;
			    }
			printf (outform, lts);
			break;
		    case DTDOY:
			fd2doy (fitsdate, &year, &doy);
			if (oldfits && timestring) {
			    ts1 = (int) fd2ts (timestring);
			    doy = doy + (ts1 / 86400.0);
			    }
			sprintf (temp, outform, doy);
			printf ("%04d %s\n", year, temp);
			break;
		    default:
			printf ("*** Unknown output type %d\n", outtype);
		    }
		}
	    break;

	/* Day of year (with time as fraction or separate string) */
	case DTDOY:
	    if (datestring != NULL) {
		if (strcmp (datestring, "now")) {
		    vyear = atoi (datestring);
		    if (timestring != NULL) 
			vdoy = atof (timestring);
		    else
			vdoy = 1.0;
		    }
		if (verbose) {
		    sprintf (temp, outform, vdoy);
		    printf ("%04d %s -> ", vyear, temp);
		    }

		switch (outtype) {
		    case DTEP:
			epoch = doy2ep (vyear, vdoy);
			printf (outform, epoch);
			break;
		    case DTEPB:
			epoch = doy2epb (vyear, vdoy);
			printf (outform, epoch);
			break;
		    case DTEPJ:
			epoch = doy2epj (vyear, vdoy);
			printf (outform, epoch);
			break;
		    case DTFITS:
			fitsdate = doy2fd (vyear, vdoy);
			if (outtime == ET) fitsdate = fd2et (fitsdate);
			else if (outtime == GST) fitsdate = fd2gst (fitsdate);
			else if (outtime == MST) fitsdate = fd2mst (fitsdate);
			else if (outtime == LST) fitsdate = fd2lst (fitsdate);
			tchar = strchr (fitsdate, 'T');
			if (tchar == NULL)
			    tchar = strchr (fitsdate, 'S');
			if (dateonly) {
			    strncpy (fyear, fitsdate, 10);
			    fyear[10] = (char) 0;
			    printf ("%s\n", fyear);
			    }
			else if (timeonly) {
			    if (tchar != NULL)
				printf ("%s\n", tchar+1);
			    else
				printf ("%s\n", fitsdate);
			    }
			else if (outtime==GST || outtime==MST || outtime==LST) {
			    printf ("%10sS%s\n", fyear, fitsdate);
			    }
			else {
			    if (outtime == ET)
				*tchar = 'E';
			    printf ("%s\n", fitsdate);
			    }
			break;
		    case DTJD:
			jd = doy2jd (vyear, vdoy);
			if (outtime == ET) jd = jd2jed (jd);
			printf (outform, jd);
			break;
		    case DTMJD:
			jd = doy2mjd (vyear, vdoy);
			printf (outform, jd);
			break;
		    case DTHJD:
			jd = doy2jd (vyear, vdoy);
			jd = jd2hjd (jd, ra, dec, coorsys);
			printf (outform, jd);
			break;
		    case DT1950:
			ts = doy2ts (vyear, vdoy);
			if (outtime == ET) ts = ts2ets (ts);
			printf (outform, ts);
			break;
		    case DTIRAF:
			its = doy2tsi (vyear, vdoy);
			printf (outform, its);
			break;
		    case DTUNIX:
			lts = doy2tsu (vyear, vdoy);
			printf (outform, lts);
			break;
		    case DTVIG:
			doy2dt (vyear, vdoy, &vdate, &vtime);
			if (outtime == ET) dt2et (&vdate, &vtime);
			else if (outtime == GST) dt2gst (&vdate, &vtime);
			else if (outtime == MST) dt2mst (&vdate, &vtime);
			else if (outtime == LST) dt2lst (&vdate, &vtime);
			if (dateonly)
			    printf ("%9.4f\n", vdate);
			else if (vdate == 0.0)
			    printf ("%10.7f\n", vtime);
			else
			    printf ("%9.4f %10.7f\n", vdate, vtime);
			break;
		    default:
			printf ("*** Unknown output type %d\n", outtype);
		    }
		}
	    break;

	/* Geocentric Julian Date */
	case DTJD:
	    if (datestring != NULL) {
		jd = atof (datestring);
		if (timestring != NULL) {
		    vtime = str2dec (timestring);
		    jd = jd + vtime / 24.0;
		    }
		if (verbose)
		    printf ("%.5f -> ", jd);
		switch (outtype) {
		    case DTEP:
			epoch = jd2ep (jd);
			printf (outform, epoch);
			break;
		    case DTEPB:
			epoch = jd2epb (jd);
			printf (outform, epoch);
			break;
		    case DTEPJ:
			epoch = jd2epj (jd);
			printf (outform, epoch);
			break;
		    case DTHJD:
			jd = jd2hjd (jd, ra, dec, coorsys);
			printf (outform, jd);
			break;
		    case DTMJD:
			jd = jd2mjd (jd);
			printf (outform, jd);
			break;
		    case DTMHJD:
			jd = jd2mjd (jd);
			jd = mjd2mhjd (jd, ra, dec, coorsys);
			printf (outform, jd);
			break;
		    case DTVIG:
			jd2dt (jd, &vdate, &vtime);
			if (outtime == ET) dt2et (&vdate, &vtime);
			else if (outtime == GST) dt2gst (&vdate, &vtime);
			else if (outtime == MST) dt2mst (&vdate, &vtime);
			else if (outtime == LST) dt2lst (&vdate, &vtime);
			if (dateonly)
			    printf ("%9.4f\n", vdate);
			else if (vdate == 0.0)
			    printf ("%10.7f\n", vtime);
			else
			    printf ("%9.4f %10.7f\n", vdate, vtime);
			break;
		    case DTFITS:
			fitsdate = jd2fd (jd);
			if (outtime == ET) fitsdate = fd2et (fitsdate);
			else if (outtime == GST) fitsdate = fd2gst (fitsdate);
			else if (outtime == MST) fitsdate = fd2mst (fitsdate);
			else if (outtime == LST) fitsdate = fd2lst (fitsdate);
			tchar = strchr (fitsdate, 'T');
			if (tchar == NULL)
			    tchar = strchr (fitsdate, 'S');
			if (dateonly) {
			    strncpy (fyear, fitsdate, 10);
			    fyear[10] = (char) 0;
			    }
			else if (timeonly) {
			    if (tchar != NULL)
				printf ("%s\n", tchar+1);
			    else
				printf ("%s\n", fitsdate);
			    }
			else if (outtime==GST || outtime==MST || outtime==LST) {
			    printf ("%10sS%s\n", fyear, fitsdate);
			    }
			else {
			    if (outtime == ET)
				*tchar = 'E';
			    printf ("%s\n", fitsdate);
			    }
			break;
		    case DT1950:
			ts = jd2ts (jd);
			if (outtime == ET) ts = ts2ets (ts);
			printf (outform, ts);
			break;
		    case DTIRAF:
			its = jd2tsi (jd);
			printf (outform, its);
			break;
		    case DTUNIX:
			lts = jd2tsu (jd);
			printf (outform, lts);
			break;
		    case DTDOY:
			jd2doy (jd, &year, &doy);
			sprintf (temp, outform, doy);
			printf ("%04d %s\n", year, temp);
			break;
		    default:
			printf ("*** Unknown output type %d\n", outtype);
		    }
		}
	    break;

	/* Modified Geocentric Julian Date */
	case DTMJD:
	    if (datestring != NULL) {
		jd = atof (datestring);
		if (timestring != NULL) {
		    vtime = str2dec (timestring);
		    jd = jd + vtime / 24.0;
		    }
		if (verbose)
		    printf ("%.5f -> ", jd);
		switch (outtype) {
		    case DTEP:
			epoch = mjd2ep (jd);
			printf (outform, epoch);
			break;
		    case DTEPB:
			epoch = mjd2epb (jd);
			printf (outform, epoch);
			break;
		    case DTEPJ:
			epoch = mjd2epj (jd);
			printf (outform, epoch);
			break;
		    case DTHJD:
			jd = mjd2jd (jd);
			jd = jd2hjd (jd, ra, dec, coorsys);
			printf (outform, jd);
			break;
		    case DTJD:
			jd = mjd2jd (jd);
			if (outtime == ET) jd = jd2jed (jd);
			printf (outform, jd);
			break;
		    case DTMHJD:
			jd = mjd2mhjd (jd, ra, dec, coorsys);
			printf (outform, jd);
			break;
		    case DTVIG:
			mjd2dt (jd, &vdate, &vtime);
			if (outtime == ET) dt2et (&vdate, &vtime);
			else if (outtime == GST) dt2gst (&vdate, &vtime);
			else if (outtime == MST) dt2mst (&vdate, &vtime);
			else if (outtime == LST) dt2lst (&vdate, &vtime);
			if (dateonly)
			    printf ("%9.4f\n", vdate);
			else if (vdate == 0.0)
			    printf ("%10.7f\n", vtime);
			else
			    printf ("%9.4f %10.7f\n", vdate, vtime);
			break;
		    case DTFITS:
			fitsdate = mjd2fd (jd);
			if (outtime == ET) fitsdate = fd2et (fitsdate);
			else if (outtime == GST) fitsdate = fd2gst (fitsdate);
			else if (outtime == MST) fitsdate = fd2mst (fitsdate);
			else if (outtime == LST) fitsdate = fd2lst (fitsdate);
			tchar = strchr (fitsdate, 'T');
			if (tchar == NULL)
			    tchar = strchr (fitsdate, 'S');
			if (dateonly) {
			    strncpy (fyear, fitsdate, 10);
			    fyear[10] = (char) 0;
			    printf ("%s\n", fyear);
			    }
			else if (timeonly) {
			    if (tchar != NULL)
				printf ("%s\n", tchar+1);
			    else
				printf ("%s\n", fitsdate);
			    }
			else if (outtime==GST || outtime==MST || outtime==LST) {
			    printf ("%10sS%s\n", fyear, fitsdate);
			    }
			else {
			    if (outtime == ET)
				*tchar = 'E';
			    printf ("%s\n", fitsdate);
			    }
			break;
		    case DT1950:
			ts = mjd2ts (jd);
			if (outtime == ET) ts = ts2ets (ts);
			printf (outform, ts);
			break;
		    case DTDOY:
			mjd2doy (jd, &year, &doy);
			sprintf (temp, outform, doy);
			printf ("%04d %s\n", year, temp);
			break;
		    default:
			printf ("*** Unknown output type %d\n", outtype);
		    }
		}
	    break;

	/* Heliocentric Julian Date */
	case DTHJD:
	    if (datestring != NULL) {
		jd = atof (datestring);
		if (timestring != NULL) {
		    vtime = str2dec (timestring);
		    jd = jd + vtime / 24.0;
		    }
		if (verbose)
		    printf ("%.5f -> ", jd);
		switch (outtype) {
		    case DTEP:
			jd = hjd2jd (jd, ra, dec, coorsys);
			epoch = jd2ep (jd);
			printf (outform, epoch);
			break;
		    case DTEPB:
			jd = hjd2jd (jd, ra, dec, coorsys);
			epoch = jd2epb (jd);
			printf (outform, epoch);
			break;
		    case DTEPJ:
			jd = hjd2jd (jd, ra, dec, coorsys);
			epoch = jd2epj (jd);
			printf (outform, epoch);
			break;
		    case DTJD:
			jd = hjd2jd (jd, ra, dec, coorsys);
			printf (outform, jd);
			break;
		    case DTMJD:
			jd = hjd2jd (jd, ra, dec, coorsys);
			jd = jd2mjd (jd);
			printf (outform, jd);
			break;
		    case DTMHJD:
			jd = jd2mjd (jd);
			printf (outform, jd);
			break;
		    case DTVIG:
			jd = hjd2jd (jd, ra, dec, coorsys);
			jd2dt (jd, &vdate, &vtime);
			if (outtime == ET) dt2et (&vdate, &vtime);
			if (dateonly)
			    printf ("%9.4f\n", vdate);
			else if (vdate == 0.0)
			    printf ("%10.7f\n", vtime);
			else
			    printf ("%9.4f %10.7f\n", vdate, vtime);
			break;
		    case DTFITS:
			jd = hjd2jd (jd, ra, dec, coorsys);
			fitsdate = jd2fd (jd);
			if (outtime == ET) fitsdate = fd2et (fitsdate);
			else if (outtime == GST) fitsdate = fd2gst (fitsdate);
			else if (outtime == MST) fitsdate = fd2mst (fitsdate);
			else if (outtime == LST) fitsdate = fd2lst (fitsdate);
			tchar = strchr (fitsdate, 'T');
			if (tchar == NULL)
			    tchar = strchr (fitsdate, 'S');
			if (dateonly) {
			    strncpy (fyear, fitsdate, 10);
			    fyear[10] = (char) 0;
			    printf ("%s\n", fyear);
			    }
			else if (timeonly) {
			    if (tchar != NULL)
				printf ("%s\n", tchar+1);
			    else
				printf ("%s\n", fitsdate);
			    }
			else if (outtime==GST || outtime==MST || outtime==LST) {
			    printf ("%10sS%s\n", fyear, fitsdate);
			    }
			else {
			    if (outtime == ET)
				*tchar = 'E';
			    printf ("%s\n", fitsdate);
			    }
			break;
		    case DT1950:
			jd = hjd2jd (jd, ra, dec, coorsys);
			ts = jd2ts (jd);
			if (outtime == ET) ts = ts2ets (ts);
			printf (outform, ts);
			break;
		    case DTIRAF:
			jd = hjd2jd (jd, ra, dec, coorsys);
			its = jd2tsi (jd);
			printf (outform, its);
			break;
		    case DTUNIX:
			jd = hjd2jd (jd, ra, dec, coorsys);
			lts = jd2tsu (jd);
			printf (outform, lts);
			break;
		    case DTDOY:
			jd = hjd2jd (jd, ra, dec, coorsys);
			jd2doy (jd, &year, &doy);
			sprintf (temp, outform, doy);
			printf ("%04d %s\n", year, temp);
			break;
		    default:
			printf ("*** Unknown output type %d\n", outtype);
		    }
		}
	    break;

	/* Modified Heliocentric Julian Date */
	case DTMHJD:
	    if (datestring != NULL) {
		jd = atof (datestring);
		if (timestring != NULL) {
		    vtime = str2dec (timestring);
		    jd = jd + vtime / 24.0;
		    }
		if (verbose)
		    printf ("%.5f -> ", jd);
		switch (outtype) {
		    case DTEP:
			jd = mhjd2mjd (jd, ra, dec, coorsys);
			epoch = mjd2ep (jd);
			printf (outform, epoch);
			break;
		    case DTEPB:
			jd = mhjd2mjd (jd, ra, dec, coorsys);
			epoch = mjd2epb (jd);
			printf (outform, epoch);
			break;
		    case DTEPJ:
			jd = mhjd2mjd (jd, ra, dec, coorsys);
			epoch = mjd2epj (jd);
			printf (outform, epoch);
			break;
		    case DTHJD:
			jd = mjd2jd (jd);
			printf (outform, jd);
			break;
		    case DTJD:
			jd = mhjd2mjd (jd, ra, dec, coorsys);
			jd = mjd2jd (jd);
			if (outtime == ET) jd = jd2jed (jd);
			printf (outform, jd);
			break;
		    case DTMJD:
			jd = mhjd2mjd (jd, ra, dec, coorsys);
			printf (outform, jd);
			break;
		    case DTVIG:
			jd = mhjd2mjd (jd, ra, dec, coorsys);
			mjd2dt (jd, &vdate, &vtime);
			if (outtime == ET) dt2et (&vdate, &vtime);
			if (outtime == GST) dt2gst (&vdate, &vtime);
			if (outtime == MST) dt2mst (&vdate, &vtime);
			if (dateonly)
			    printf ("%9.4f\n", vdate);
			else if (vdate == 0.0)
			    printf ("%10.7f\n", vtime);
			else
			    printf ("%9.4f %10.7f\n", vdate, vtime);
			break;
		    case DTFITS:
			jd = mhjd2mjd (jd, ra, dec, coorsys);
			fitsdate = mjd2fd (jd);
			if (outtime == ET) fitsdate = fd2et (fitsdate);
			if (outtime == GST) fitsdate = fd2gst (fitsdate);
			if (outtime == MST) fitsdate = fd2mst (fitsdate);
			tchar = strchr (fitsdate, 'T');
			if (tchar == NULL)
			    tchar = strchr (fitsdate, 'S');
			if (dateonly) {
			    strncpy (fyear, fitsdate, 10);
			    fyear[10] = (char) 0;
			    printf ("%s\n", fyear);
			    }
			else if (timeonly) {
			    if (tchar != NULL)
				printf ("%s\n", tchar+1);
			    else
				printf ("%s\n", fitsdate);
			    }
			else if (outtime==GST || outtime==MST || outtime==LST) {
			    printf ("%10sS%s\n", fyear, fitsdate);
			    }
			else {
			    if (outtime == ET)
				*tchar = 'E';
			    printf ("%s\n", fitsdate);
			    }
			break;
		    case DT1950:
			jd = mhjd2mjd (jd, ra, dec, coorsys);
			ts = mjd2ts (jd);
			if (outtime == ET) ts = ts2ets (ts);
			printf (outform, ts);
			break;
		    case DTDOY:
			jd = mhjd2mjd (jd, ra, dec, coorsys);
			mjd2doy (jd, &year, &doy);
			sprintf (temp, outform, doy);
			printf ("%04d %s\n", year, temp);
			break;
		    default:
			printf ("*** Unknown output type %d\n", outtype);
		    }
		}
	    break;

	/* Sidereal time */
	case DTLST:
	case DTGST:
	case DTMST:
	    if (datestring != NULL) {
		if (strchr (datestring,'/'))
		    oldfits = 1;
		else
		    oldfits = 0;
		if (timestring != NULL) {
		    if (oldfits) {
			lfd = strlen (datestring) + strlen (timestring) + 2;
			stdate = (char *) calloc (1, lfd);
			strcpy (stdate, datestring);
			strcat (stdate, "_");
			strcat (stdate, timestring);
			}
		    else {
			lfd = strlen (datestring) + strlen (timestring) + 2;
			stdate = (char *) calloc (1, lfd);
			strcpy (stdate, datestring);
			strcat (stdate, "S");
			strcat (stdate, timestring);
			}
		    }
		else
		    stdate = datestring;

		if (verbose)
		    printf ("%s -> ", stdate);

		/* Convert Sidereal Time to UT */
		if (intype == DTLST)
		    fitsdate = lst2fd (stdate);
		else if (intype == DTMST)
		    fitsdate = mst2fd (stdate);
		else if (intype == DTGST)
		    fitsdate = gst2fd (stdate);
		if (verbose)
		    printf ("%s -> ", fitsdate);

		/* Convert to desired output format */
		switch (outtype) {
		    case DTEP:
			epoch = fd2ep (fitsdate);
			if (oldfits && timestring) {
			    epoch1 = fd2ep (timestring);
			    epoch = epoch + epoch1;
			    }
			printf (outform, epoch);
			break;
		    case DTEPB:
			epoch = fd2epb (fitsdate);
			if (oldfits && timestring) {
			    epoch1 = fd2epb (timestring);
			    epoch = epoch + epoch1;
			    }
			printf (outform, epoch);
			break;
		    case DTEPJ:
			epoch = fd2epj (fitsdate);
			if (oldfits && timestring) {
			    epoch1 = fd2epj (timestring);
			    epoch = epoch + epoch1;
			    }
			printf (outform, epoch);
			break;
		    case DTVIG:
			fd2dt (fitsdate, &vdate, &vtime);
			if (outtime == ET) dt2et (&vdate, &vtime);
			else if (outtime == GST) dt2gst (&vdate, &vtime);
			else if (outtime == MST) dt2mst (&vdate, &vtime);
			else if (outtime == LST) dt2lst (&vdate, &vtime);
			if (oldfits) {
			    if (timestring == NULL)
				vtime = 0.0;
			    else
				vtime = str2dec (timestring);
			    }
			if (dateonly)
			    printf ("%9.4f\n", vdate);
			else if (vdate == 0.0 || timeonly)
			    printf ("%10.7f\n", vtime);
			else
			    printf ("%9.4f %10.7f\n", vdate, vtime);
			break;
		    case DTFITS:
			newfdate = fd2fd (fitsdate);
			if (oldfits && timestring) {
			    strcat (newfdate, "T");
			    strcat (newfdate, timestring);
			    }
			strncpy (fyear, newfdate, 10);
			fyear[10] = (char) 0;
			if (outtime == ET) newfdate = fd2et (newfdate);
			else if (outtime == GST) newfdate = fd2gst (newfdate);
			else if (outtime == MST) newfdate = fd2mst (newfdate);
			else if (outtime == LST) newfdate = fd2lst (newfdate);
			tchar = strchr (newfdate, 'T');
			if (tchar == NULL)
			    tchar = strchr (newfdate, 'S');
			if (dateonly) {
			    strncpy (fyear, fitsdate, 10);
			    fyear[10] = (char) 0;
			    printf ("%s\n", fyear);
			    }
			else if (timeonly) {
			    if (tchar != NULL)
				printf ("%s\n", tchar+1);
			    else
				printf ("%s\n", newfdate);
			    }
			else if (outtime==GST || outtime==MST || outtime==LST) {
			    printf ("%10sS%s\n", fyear, newfdate);
			    }
			else {
			    if (outtime == ET)
				*tchar = 'E';
			    printf ("%s\n", newfdate);
			    }
			break;
		    case DTJD:
			jd = fd2jd (fitsdate);
			if (oldfits && timestring) {
			    jd1 = fd2jd (timestring);
			    jd = jd + jd1;
			    }
			if (outtime == ET) jd = jd2jed (jd);
			printf (outform, jd);
			break;
		    case DTMJD:
			jd = fd2mjd (fitsdate);
			if (oldfits && timestring) {
			    jd1 = fd2jd (timestring);
			    jd = jd + jd1;
			    }
			printf (outform, jd);
			break;
		    case DTHJD:
			jd = fd2jd (fitsdate);
			if (oldfits && timestring) {
			    jd1 = fd2jd (timestring);
			    jd = jd + jd1;
			    }
			jd = jd2hjd (jd, ra, dec, coorsys);
			printf (outform, jd);
			break;
		    case DTOF:
			newfdate = fd2of (fitsdate);
			if (oldfits && timestring) {
			    free (newfdate);
			    newfdate = fd2ofd (fitsdate);
			    strcat (newfdate, " ");
			    strcat (newfdate, timestring);
			    }
			printf ("%s\n", newfdate);
			break;
		    case DTOFD:
			newfdate = fd2ofd (fitsdate);
			printf ("%s\n", newfdate);
			break;
		    case DTOFT:
			newfdate = fd2oft (fitsdate);
			if (oldfits && timestring) {
			    strcpy (newfdate, timestring);
			    }
			printf ("%s\n", newfdate);
			break;
		    case DT1950:
			ts = fd2ts (fitsdate);
			if (oldfits && timestring) {
			    ts1 = fd2ts (timestring);
			    ts = ts + ts1;
			    }
			if (outtime == ET) ts = ts2ets (ts);
			printf (outform, ts);
			break;
		    case DTIRAF:
			its = fd2tsi (fitsdate);
			if (oldfits && timestring) {
			    its1 = (int) fd2ts (timestring);
			    its = its + its1;
			    }
			printf (outform, its);
			break;
		    case DTUNIX:
			lts = fd2tsu (fitsdate);
			if (oldfits && timestring) {
			    its1 = (int) fd2ts (timestring);
			    lts = lts + (time_t) its1;
			    }
			printf (outform, lts);
			break;
		    case DTDOY:
			fd2doy (fitsdate, &year, &doy);
			if (oldfits && timestring) {
			    ts1 = (int) fd2ts (timestring);
			    doy = doy + (ts1 / 86400.0);
			    }
			sprintf (temp, outform, doy);
			printf ("%04d %s\n", year, temp);
			break;
		    default:
			printf ("*** Unknown output type %d\n", outtype);
		    }
		}
	    break;

	/* Seconds since 0:00 on 1950-01-01 */
	case DT1950:
	    if (datestring != NULL) {
    		if (strcmp (datestring, "now"))
		    ts = atof (datestring);
		if (verbose)
		    printf ("%.3f -> ", ts);
		switch (outtype) {
		    case DTEP:
			epoch = ts2ep (ts);
			printf (outform, epoch);
			break;
		    case DTEPB:
			epoch = ts2epb (ts);
			printf (outform, epoch);
			break;
		    case DTEPJ:
			epoch = ts2epj (ts);
			printf (outform, epoch);
			break;
		    case DTVIG:
			ts2dt (ts, &vdate, &vtime);
			if (outtime == ET) dt2et (&vdate, &vtime);
			if (outtime == GST) dt2gst (&vdate, &vtime);
			if (outtime == MST) dt2mst (&vdate, &vtime);
			if (dateonly)
			    printf ("%9.4f\n", vdate);
			else if (vdate == 0.0)
			    printf ("%10.7f\n", vtime);
			else
			    printf ("%9.4f %10.7f\n", vdate, vtime);
			break;
		    case DTFITS:
			fitsdate = ts2fd (ts);
			if (outtime == ET) fitsdate = fd2et (fitsdate);
			if (outtime == GST) fitsdate = fd2gst (fitsdate);
			if (outtime == MST) fitsdate = fd2mst (fitsdate);
			tchar = strchr (fitsdate, 'T');
			if (tchar == NULL)
			    tchar = strchr (fitsdate, 'S');
			if (dateonly) {
			    strncpy (fyear, fitsdate, 10);
			    fyear[10] = (char) 0;
			    printf ("%s\n", fyear);
			    }
			else if (timeonly) {
			    if (tchar != NULL)
				printf ("%s\n", tchar+1);
			    else
				printf ("%s\n", fitsdate);
			    }
			else if (outtime==GST || outtime==MST || outtime==LST) {
			    printf ("%10sS%s\n", fyear, fitsdate);
			    }
			else {
			    if (outtime == ET)
				*tchar = 'E';
			    printf ("%s\n", fitsdate);
			    }
			break;
		    case DTJD:
			jd = ts2jd (ts);
			if (outtime == ET) jd = jd2jed (jd);
			printf (outform, jd);
			break;
		    case DTMJD:
			jd = ts2mjd (ts);
			printf (outform, jd);
			break;
		    case DTHJD:
			jd = ts2jd (ts);
			jd = jd2hjd (jd, ra, dec, coorsys);
			printf (outform, jd);
			break;
		    case DT1950:
			printf (outform, ts);
			if (outtime == ET) ts = ts2ets (ts);
			break;
		    default:
			printf ("*** Unknown output type %d\n", outtype);
		    }
		}
	    break;
	case DTDSEC:
	    ts = atof (datestring);
	    switch (outtype) {
		case DTFITS:
		    fitsdate = tsd2fd (ts);
		    printf ("%s\n", fitsdate);
		    break;
		case DTVIG:
		    vtime = tsd2dt (ts);
		    printf ("%10.7f\n", vtime);
		    break;
		}
	    break;
	case DTIRAF:
	    if (datestring != NULL) {
    		if (strcmp (datestring, "now"))
		    its = atoi (datestring);
		if (verbose)
		    printf ("%d -> ", its);
		switch (outtype) {
		    case DTEP:
			ts = tsi2ts (ts);
			epoch = ts2ep (ts);
			printf (outform, epoch);
			break;
		    case DTEPB:
			ts = tsi2ts (ts);
			epoch = ts2epb (ts);
			printf (outform, epoch);
			break;
		    case DTEPJ:
			ts = tsi2ts (ts);
			epoch = ts2epj (ts);
			printf (outform, epoch);
			break;
		    case DTVIG:
			tsi2dt (its, &vdate, &vtime);
			if (outtime == ET) dt2et (&vdate, &vtime);
			if (outtime == GST) dt2gst (&vdate, &vtime);
			if (outtime == MST) dt2mst (&vdate, &vtime);
			if (dateonly)
			    printf ("%9.4f\n", vdate);
			else if (vdate == 0.0)
			    printf ("%10.7f\n", vtime);
			else
			    printf ("%9.4f %10.7f\n", vdate, vtime);
			break;
		    case DTFITS:
			fitsdate = tsi2fd (its);
			if (outtime == ET) fitsdate = fd2et (fitsdate);
			if (outtime == GST) fitsdate = fd2gst (fitsdate);
			if (outtime == MST) fitsdate = fd2mst (fitsdate);
			tchar = strchr (fitsdate, 'T');
			if (tchar == NULL)
			    tchar = strchr (fitsdate, 'S');
			if (dateonly) {
			    strncpy (fyear, fitsdate, 10);
			    fyear[10] = (char) 0;
			    printf ("%s\n", fyear);
			    }
			else if (timeonly) {
			    if (tchar != NULL)
				printf ("%s\n", tchar+1);
			    else
				printf ("%s\n", fitsdate);
			    }
			else if (outtime==GST || outtime==MST || outtime==LST) {
			    printf ("%10sS%s\n", fyear, fitsdate);
			    }
			else {
			    if (outtime == ET)
				*tchar = 'E';
			    printf ("%s\n", fitsdate);
			    }
			break;
		    case DTJD:
			ts = tsi2ts (ts);
			jd = ts2jd (ts);
			if (outtime == ET) jd = jd2jed (jd);
			printf (outform, jd);
			break;
		    case DTMJD:
			ts = tsi2ts (ts);
			jd = ts2mjd (ts);
			printf (outform, jd);
			break;
		    case DTHJD:
			ts = tsi2ts (ts);
			jd = ts2jd (ts);
			jd = jd2hjd (jd, ra, dec, coorsys);
			printf (outform, jd);
			break;
		    case DT1950:
			ts = tsi2ts (its);
			if (outtime == ET) ts = ts2ets (ts);
			printf (outform, ts);
			break;
		    default:
			printf ("*** Unknown output type %d\n", outtype);
		    }
		}
	    break;
	case DTUNIX:
	    if (datestring != NULL) {
    		if (strcmp (datestring, "now")) {
		    its = (time_t) (atof (datestring) + 0.5);
		    ts = (double) its;
		    }
		else
		    its = (int) ts;
		if (verbose)
		    printf ("%d -> ", its);
		switch (outtype) {
		    case DTEP:
			ts = tsu2ts (ts);
			epoch = ts2ep (ts);
			printf (outform, epoch);
			break;
		    case DTEPB:
			ts = tsu2ts (ts);
			epoch = ts2epb (ts);
			printf (outform, epoch);
			break;
		    case DTEPJ:
			ts = tsu2ts (ts);
			epoch = ts2epj (ts);
			printf (outform, epoch);
			break;
		    case DTVIG:
			tsu2dt (ts, &vdate, &vtime);
			if (outtime == ET) dt2et (&vdate, &vtime);
			if (outtime == GST) dt2gst (&vdate, &vtime);
			if (outtime == MST) dt2mst (&vdate, &vtime);
			if (dateonly)
			    printf ("%9.4f\n", vdate);
			else if (vdate == 0.0)
			    printf ("%10.7f\n", vtime);
			else
			    printf ("%9.4f %10.7f\n", vdate, vtime);
			break;
		    case DTFITS:
			fitsdate = tsu2fd (ts);
			if (outtime == ET) fitsdate = fd2et (fitsdate);
			if (outtime == GST) fitsdate = fd2gst (fitsdate);
			if (outtime == MST) fitsdate = fd2mst (fitsdate);
			tchar = strchr (fitsdate, 'T');
			if (tchar == NULL)
			    tchar = strchr (fitsdate, 'S');
			if (dateonly) {
			    strncpy (fyear, fitsdate, 10);
			    fyear[10] = (char) 0;
			    printf ("%s\n", fyear);
			    }
			else if (timeonly) {
			    if (tchar != NULL)
				printf ("%s\n", tchar+1);
			    else
				printf ("%s\n", fitsdate);
			    }
			else if (outtime==GST || outtime==MST || outtime==LST) {
			    printf ("%10sS%s\n", fyear, fitsdate);
			    }
			else {
			    if (outtime == ET)
				*tchar = 'E';
			    printf ("%s\n", fitsdate);
			    }
			break;
		    case DTJD:
			ts = tsu2ts (ts);
			jd = ts2jd (ts);
			if (outtime == ET) jd = jd2jed (jd);
			printf (outform, jd);
			break;
		    case DTMJD:
			ts = tsu2ts (ts);
			jd = ts2mjd (ts);
			printf (outform, jd);
			break;
		    case DTHJD:
			ts = tsu2ts (ts);
			jd = ts2jd (ts);
			jd = jd2hjd (jd, ra, dec, coorsys);
			printf (outform, jd);
			break;
		    case DTIRAF:
			its = tsu2tsi (ts);
			printf (outform, its);
			break;
		    case DT1950:
			ts = tsu2ts (ts);
			if (outtime == ET) ts = ts2ets (ts);
			printf (outform, ts);
			break;
		    default:
			printf ("*** Unknown output type %d\n", outtype);
		    }
		}
	    break;
	case DTEP:
	    if (datestring != NULL) {
		epoch = atof (datestring);
		if (verbose)
		    printf ("%.5f -> ", epoch);
		switch (outtype) {
		    case DTEPB:
			epoch = ep2epb (epoch);
			printf (outform, epoch);
			break;
		    case DTEPJ:
			epoch = ep2epj (epoch);
			printf (outform, epoch);
			break;
		    case DTVIG:
			ep2dt (epoch, &vdate, &vtime);
			if (outtime == ET) dt2et (&vdate, &vtime);
			if (outtime == GST) dt2gst (&vdate, &vtime);
			if (outtime == MST) dt2mst (&vdate, &vtime);
			if (dateonly)
			    printf ("%9.4f\n", vdate);
			else if (vdate == 0.0)
			    printf ("%10.7f\n", vtime);
			else
			    printf ("%9.4f %10.7f\n", vdate, vtime);
			break;
		    case DTFITS:
			fitsdate = ep2fd (epoch);
			if (outtime == ET) fitsdate = fd2et (fitsdate);
			if (outtime == GST) fitsdate = fd2gst (fitsdate);
			if (outtime == MST) fitsdate = fd2mst (fitsdate);
			tchar = strchr (fitsdate, 'T');
			if (tchar == NULL)
			    tchar = strchr (fitsdate, 'S');
			if (dateonly) {
			    strncpy (fyear, fitsdate, 10);
			    fyear[10] = (char) 0;
			    printf ("%s\n", fyear);
			    }
			else if (timeonly) {
			    if (tchar != NULL)
				printf ("%s\n", tchar+1);
			    else
				printf ("%s\n", fitsdate);
			    }
			else if (outtime==GST || outtime==MST || outtime==LST) {
			    printf ("%10sS%s\n", fyear, fitsdate);
			    }
			else {
			    if (outtime == ET)
				*tchar = 'E';
			    printf ("%s\n", fitsdate);
			    }
			break;
		    case DTJD:
			jd = ep2jd (epoch);
			if (outtime == ET) jd = jd2jed (jd);
			printf (outform, jd);
			break;
		    case DTMJD:
			jd = ep2mjd (epoch);
			printf (outform, jd);
			break;
		    case DTHJD:
			jd = ep2jd (epoch);
			jd = jd2hjd (jd, ra, dec, coorsys);
			printf (outform, jd);
			break;
		    case DT1950:
			ts = ep2ts (epoch);
			if (outtime == ET) ts = ts2ets (ts);
			printf (outform, ts);
			break;
		    default:
			printf ("*** Unknown output type %d\n", outtype);
		    }
		}
	    break;
	case DTEPB:
	    if (datestring != NULL) {
		epoch = atof (datestring);
		if (verbose)
		    printf ("%.5f -> ", epoch);
		switch (outtype) {
		    case DTEP:
			epoch = epb2ep (epoch);
			printf (outform, epoch);
			break;
		    case DTEPJ:
			epoch = epb2epj (epoch);
			printf (outform, epoch);
			break;
		    case DTVIG:
			epb2dt (epoch, &vdate, &vtime);
			if (outtime == ET) dt2et (&vdate, &vtime);
			if (outtime == GST) dt2gst (&vdate, &vtime);
			if (outtime == MST) dt2mst (&vdate, &vtime);
			if (dateonly)
			    printf ("%9.4f\n", vdate);
			else if (vdate == 0.0)
			    printf ("%10.7f\n", vtime);
			else
			    printf ("%9.4f %10.7f\n", vdate, vtime);
			break;
		    case DTFITS:
			fitsdate = epb2fd (epoch);
			if (outtime == ET) fitsdate = fd2et (fitsdate);
			if (outtime == GST) fitsdate = fd2gst (fitsdate);
			if (outtime == MST) fitsdate = fd2mst (fitsdate);
			tchar = strchr (fitsdate, 'T');
			if (tchar == NULL)
			    tchar = strchr (fitsdate, 'S');
			if (dateonly) {
			    strncpy (fyear, fitsdate, 10);
			    fyear[10] = (char) 0;
			    printf ("%s\n", fyear);
			    }
			else if (timeonly) {
			    if (tchar != NULL)
				printf ("%s\n", tchar+1);
			    else
				printf ("%s\n", fitsdate);
			    }
			else if (outtime==GST || outtime==MST || outtime==LST) {
			    printf ("%10sS%s\n", fyear, fitsdate);
			    }
			else {
			    if (outtime == ET)
				*tchar = 'E';
			    printf ("%s\n", fitsdate);
			    }
			break;
		    case DTJD:
			jd = epb2jd (epoch);
			if (outtime == ET) jd = jd2jed (jd);
			printf (outform, jd);
			break;
		    case DTMJD:
			jd = epb2mjd (epoch);
			printf (outform, jd);
			break;
		    case DTHJD:
			jd = epb2jd (epoch);
			jd = jd2hjd (jd, ra, dec, coorsys);
			printf (outform, jd);
			break;
		    case DT1950:
			ts = epb2ts (epoch);
			if (outtime == ET) ts = ts2ets (ts);
			printf (outform, ts);
			break;
		    default:
			printf ("*** Unknown output type %d\n", outtype);
		    }
		}
	    break;
	case DTEPJ:
	    if (datestring != NULL) {
		epoch = atof (datestring);
		if (verbose)
		    printf ("%.5f -> ", epoch);
		switch (outtype) {
		    case DTEP:
			epoch = epj2ep (epoch);
			printf (outform, epoch);
			break;
		    case DTEPB:
			epoch = epj2epb (epoch);
			printf (outform, epoch);
			break;
		    case DTVIG:
			epj2dt (epoch, &vdate, &vtime);
			if (outtime == ET) dt2et (&vdate, &vtime);
			if (outtime == GST) dt2gst (&vdate, &vtime);
			if (outtime == MST) dt2mst (&vdate, &vtime);
			if (dateonly)
			    printf ("%9.4f\n", vdate);
			else if (vdate == 0.0)
			    printf ("%10.7f\n", vtime);
			else
			    printf ("%9.4f %10.7f\n", vdate, vtime);
			break;
		    case DTFITS:
			fitsdate = epj2fd (epoch);
			if (outtime == ET) fitsdate = fd2et (fitsdate);
			if (outtime == GST) fitsdate = fd2gst (fitsdate);
			if (outtime == MST) fitsdate = fd2mst (fitsdate);
			tchar = strchr (fitsdate, 'T');
			if (tchar == NULL)
			    tchar = strchr (fitsdate, 'S');
			if (dateonly) {
			    strncpy (fyear, fitsdate, 10);
			    fyear[10] = (char) 0;
			    printf ("%s\n", fyear);
			    }
			else if (timeonly) {
			    if (tchar != NULL)
				printf ("%s\n", tchar+1);
			    else
				printf ("%s\n", fitsdate);
			    }
			else if (outtime==GST || outtime==MST || outtime==LST) {
			    printf ("%10sS%s\n", fyear, fitsdate);
			    }
			else {
			    if (outtime == ET)
				*tchar = 'E';
			    printf ("%s\n", fitsdate);
			    }
			break;
		    case DTJD:
			jd = epj2jd (epoch);
			if (outtime == ET) jd = jd2jed (jd);
			printf (outform, jd);
			break;
		    case DTMJD:
			jd = epj2mjd (epoch);
			printf (outform, jd);
			break;
		    case DTHJD:
			jd = epj2jd (epoch);
			jd = jd2hjd (jd, ra, dec, coorsys);
			printf (outform, jd);
			break;
		    case DT1950:
			ts = epj2ts (epoch);
			if (outtime == ET) ts = ts2ets (ts);
			printf (outform, ts);
			break;
		    default:
			printf ("*** Unknown output type %d\n", outtype);
		    }
		}
	    break;
	case DTUT:
	    switch (outtype) {
		case DTEP:
		    epoch = ut2ep ();
		    printf (outform, epoch);
		    break;
		case DTEPB:
		    epoch = ut2epb ();
		    printf (outform, epoch);
		    break;
		case DTEPJ:
		    epoch = ut2epj ();
		    printf (outform, epoch);
		    break;
		case DTVIG:
		    ut2dt (&vdate, &vtime);
		    if (outtime == ET) dt2et (&vdate, &vtime);
		    if (outtime == GST) dt2gst (&vdate, &vtime);
		    if (outtime == MST) dt2mst (&vdate, &vtime);
		    if (dateonly)
			printf ("%9.4f\n", vdate);
		    else if (vdate == 0.0)
			printf ("%10.7f\n", vtime);
		    else
			printf ("%9.4f %10.7f\n", vdate, vtime);
		    break;
		case DTFITS:
		    newfdate = ut2fd ();
		    if (outtime == ET) newfdate = fd2et (newfdate);
		    if (outtime == GST) newfdate = fd2gst (newfdate);
		    if (outtime == MST) newfdate = fd2mst (newfdate);
		    tchar = strchr (newfdate, 'T');
		    if (tchar == NULL)
			tchar = strchr (newfdate, 'S');
		    if (dateonly) {
			strncpy (fyear, fitsdate, 10);
			fyear[10] = (char) 0;
			printf ("%s\n", fyear);
			}
		    else if (timeonly) {
			if (tchar != NULL)
			    printf ("%s\n", tchar+1);
			else
			    printf ("%s\n", newfdate);
			}
		    else if (outtime==GST || outtime==MST || outtime==LST) {
			printf ("%10sS%s\n", fyear, newfdate);
			}
		    else {
			if (outtime == ET)
			    *tchar = 'E';
			printf ("%s\n", newfdate);
			}
		    break;
		case DTJD:
		    jd = ut2jd ();
		    if (outtime == ET) jd = jd2jed (jd);
		    printf (outform, jd);
		    break;
		case DTMJD:
		    jd = ut2mjd ();
		    printf (outform, jd);
		    break;
		case DTHJD:
		    jd = ut2jd ();
		    jd = jd2hjd (jd, ra, dec, coorsys);
		    printf (outform, jd);
		    break;
		case DT1950:
		    ts = ut2ts ();
		    if (outtime == ET) ts = ts2ets (ts);
		    if (outtime == GST) ts = ts2gst (ts);
		    if (outtime == MST) ts = ts2mst (ts);
		    printf (outform, ts);
		    break;
		case DTIRAF:
		    its = ut2tsi ();
		    printf (outform, its);
		    break;
		case DTUNIX:
		    lts = ut2tsu ();
		    printf (outform, lts);
		    break;
		case DTDOY:
		    ut2doy (&year, &doy);
		    sprintf (temp, outform, doy);
		    printf ("%04d %s\n", year, temp);
		    break;
		default:
		    printf ("*** Unknown output type %d\n", outtype);
		}
	    break;
	default:
	    printf ("*** Unknown input type %d\n", intype);
	}

    return;
}

/* Dec 20 1999	New program
 *
 * Jan 20 2000	Add functions to convert Julian and Besselian epochs
 * Jan 21 2000	Add functions to convert to old FITS date and time
 * Jan 26 2000	Add modified Julian date conversions
 * Mar  2 2000	Add "today" to use current date and time
 * Mar  2 2000	Change date and time variables to vdate and vtime
 * Mar  3 2000	Add -d option to print only date
 * Mar 14 2000	Allow multiple conversions on one command line
 * Mar 24 2000	Add current local time, current UT, and IRAF and Unix seconds
 * May 31 2000	Fix bug which failed to convert to Julian or Bessellian epochs
 *
 * May 25 2001	Add year,day-of-year conversions
 *
 * Apr  8 2002	Change all long declarations to time_t
 * Apr  9 2002	Fix declaration of ConvertDate(); fix other bugs
 * Jul  8 2002	Check for negative dates if argument starts with -
 * Aug 27 2002	Add option to input file of dates to convert; fix output format
 * Aug 29 2002	Add option to append output to input line
 * Aug 30 2002	Add option to convert output to Ephemeris Time (ET/TDT/TT)
 * Sep  6 2002	Fix now option to return UT; fix 2FD bug introduced by -e
 * Sep  6 2002	Add conversion of DT format to ET
 * Sep 10 2002	Add conversion of UT to sidereal time
 *
 * Mar  6 2003	Add conversions to and from HJD and MHJD
 * Jul 28 2003	Correctly convert FITS date AND time correctly
 * Nov 21 2003	Fix bug so times are not assumed to be coordinates
 *
 * Apr 28 2004	Add more Heliocentric Julian Date conversions
 *
 * Mar  7 2005	Add "now" as input type
 * Oct 14 2005	Add tsd2fd and tsd2dt
 *
 * Jun 21 2006	Clean up code
 * Sep 15 2006	Add local sidereal time
 * Oct  2 2006	Add conversions from sidereal time to UT
 * Oct  2 2006	Add time to old FITS date conversions
 *
 * Jan 10 2007	Declare RevMsg static, not const
 * Jan 10 2007	Drop ignored argument from ut2jd() call
 * Jan 10 2007	Fix unix time output format
 * Dec 12 2007	Implement time only output for all 2FD options
 *
 * Jul 28 2008	Fix file input problems for vigesimal date/time strings
 * Jul 28 2008	Fix time system conversion but for vigesimal date/time strings
 * Sep  8 2008	Add vigesimal hour and degree to fractional degree conversions
 * Apr  6 2009	Fix bug so positions after FITS ISO dates are read correctly
 */
