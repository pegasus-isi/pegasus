/*** File libwcs/daoread.c
 *** January 11, 2007
 *** By Doug Mink, dmink@cfa.harvard.edu
 *** Harvard-Smithsonian Center for Astrophysics
 *** Copyright (C) 1996-2007
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

#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <math.h>
#include "wcs.h"
#include "fitsfile.h"
#include "wcscat.h"

static int nlines;	/* Number of lines in catalog */
#define ABS(a) ((a) < 0 ? (-(a)) : (a))

static char newline = 10;
static char *daobuff;

/* DAOREAD -- Read DAOFIND file of star positions in an image */

int
daoread (daocat, xa, ya, ba, pa, nlog)

char	*daocat;	/* Name of DAOFIND catalog file */
double	**xa, **ya;	/* X and Y coordinates of stars, array returned */
double	**ba;		/* Instrumental magnitudes of stars, array returned */
int	**pa;		/* Peak counts of stars in counts, array returned */
int	nlog;		/* 1 to print each star's position */
{
    int nstars;
    double xi, yi, magi;
    double flux;
    int iline;
    char *line;

    line = 0;
    iline = 0;
    nstars = 0;

    if (daoopen (daocat) > 0) {
	line = daobuff;

    /* Loop through catalog */
	for (iline = 1; iline <= nlines; iline++) {
	    line = daoline (iline, line);
	    if (line == NULL) {
		fprintf (stderr,"DAOREAD: Cannot read line %d\n", iline);
		break;
		}
	    else if (line[0] != '#') {

		/* Extract X, Y, magnitude  */
		sscanf (line,"%lg %lg %lg", &xi, &yi, &magi);

		/* Save star position, scaled flux, and magnitude in table */
		nstars++;
		*xa= (double *) realloc(*xa, nstars*sizeof(double));
		*ya= (double *) realloc(*ya, nstars*sizeof(double));
		*ba= (double *) realloc(*ba, nstars*sizeof(double));
		*pa= (int *) realloc(*pa, nstars*sizeof(int));
		(*xa)[nstars-1] = xi;
		(*ya)[nstars-1] = yi;
		(*ba)[nstars-1] = magi;
		flux = pow (10.0, (-magi / 2.5));
		(*pa)[nstars-1] = (int) flux;

		if (nlog == 1)
		    fprintf (stderr,"DAOREAD: %6d: %9.5f %9.5f %15.4f %6.2f\n",
			   nstars,xi,yi,flux,magi);
		}

	    /* Log operation */
	    if (nlog > 0 && iline%nlog == 0)
		fprintf (stderr,"DAOREAD: %5d / %5d / %5d stars from catalog %s\r",
			nstars, iline, nlines, daocat);

	    /* End of star loop */
	    }

	/* End of open catalog file */
	}

/* Summarize search */
    if (nlog > 0)
	fprintf (stderr,"DAOREAD: Catalog %s : %d / %d / %d found\n",
		 daocat, nstars, iline, nlines);

    free (daobuff);

    return (nstars);
}


/* DAOOPEN -- Open DAOFIND catalog, returning number of entries */

int
daoopen (daofile)

char *daofile;	/* DAOFIND catalog file name */
{
    FILE *fcat;
    int nr, lfile;
    char *daonew;
    
/* Find length of DAOFIND catalog */
    lfile = getfilesize (daofile);
    if (lfile < 2) {
	fprintf (stderr,"DAOOPEN: DAOFIND catalog %s has no entries\n",daofile);
	return (0);
	}

/* Open DAOFIND catalog */
    if (!(fcat = fopen (daofile, "r"))) {
	fprintf (stderr,"DAOOPEN: DAOFIND catalog %s cannot be read\n",daofile);
	return (0);
	}

/* Allocate buffer to hold entire catalog and read it */
    if ((daobuff = malloc (lfile)) != NULL) {
	nr = fread (daobuff, 1, lfile, fcat);
	if (nr < lfile) {
	    fprintf (stderr,"DAOOPEN: read only %d / %d bytes of file %s\n",
		     nr, lfile, daofile);
	    (void) fclose (fcat);
	    return (0);
	    }

    /* Enumerate entries in DAOFIND catalog by counting newlines */
	daonew = daobuff;
	nlines = 0;
	while ((daonew = strchr (daonew, newline)) != NULL) {
	    daonew = daonew + 1;
	    nlines = nlines + 1;
	    }
	}

    (void) fclose (fcat);
    return (nlines);
}


/* DAOLINE -- Get DAOFIND catalog entry for one star; return 0 if successful */

char *
daoline (iline, line)

int iline;	/* Star sequence number in DAOFIND catalog */
char *line;	/* Pointer to iline'th entry (returned updated) */
{
    char *nextline;
    int i;

    if (iline > nlines) {
	fprintf (stderr, "DAOSTAR:  %d is not in catalog\n",iline);
	return (NULL);
	}
    else if (iline < 1 && line) {
	nextline = strchr (line, newline) + 1;
	}
    else {
	nextline = daobuff;
	for (i = 1; i < iline; i++) {
	    nextline = strchr (nextline, newline) + 1;
	    }
	}

    return (nextline);
}

/* Dec 11 1996	New subroutines
 *
 * Mar 20 1997	Removed unused variables, fixed logging after lint
 *
 * Jul 20 2001	Return magnitude as well as flux
 *
 * May 27 2003	Use getfilesize() to get length of file
 *
 * Aug  3 2004	Move daoopen() and daoline() declarations to wcscat.h
 * Aug 30 2004	Include fitsfile.h
 *
 * Jun 19 2006	Initialized uninitialized  variable iline
 *
 * Jan 10 2007	Include wcs.h
 * Jan 11 2007	Include fitsfile.h
 */
