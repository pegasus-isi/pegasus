/*** nedname.c - Find name of object from its position as command line arguments
 *** August 23, 2004
 *** By Doug Mink, after IPAC nearposn.c
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "ned_client.h"
#include "../wcstools/libwcs/fitshead.h"
#include "../wcstools/libwcs/wcs.h"

extern int ned_errno;
static void usage();

int
main (ac, av)

int  ac;
char *av[];
{
   
    int    st;
    int    no_obj, lobj;
    int    i, j, n, ipos, nid;
    int syscoor[50];	/* Input search coordinate system */
    double eqcoor[50];		/* Equinox of search center */
    double ra[50], dec[50], ra0, dec0, ra1, dec1, dist;
    double v;
    char magnitude[32];
    char velocity[32];
    char zstring[32];
    char *str, *objname, *objin, *objout;
    char *objchar, *objlast;
    char rastr[32], decstr[32], cstr[32];
    int iobj;
    int npos = 0;
    CrossID *cp, *tmpcp;
    ObjInfo *op, *tmpop;
    MoreData     *mdatap;

    int verbose = 0;
    int printid = 0;
    int printdeg = 0;
    int printall = 0;
    int printpos = 0;
    int printvel = 0;
    int printtype = 0;
    int printmag = 0;
    int dropspace = 0;
    int outsys = WCS_J2000;
    int sysj = WCS_J2000;
    double c = 299792.58;
    double radius = 1.0;

    st = ned_connect();
    if (st < 0) {
	fprintf(stderr, "connection failed \n");
	exit(1);
	}

    /* print command list if no arguments */
    if (ac == 1)
	usage();

    /* crack arguments */
    for (av++; --ac > 0; av++) {

        /* Set search RA, Dec, and equinox if colon in argument */
	if (strsrch (*av,":") != NULL) {
            if (ac < 2)
                usage (*av);
            else {
                strcpy (rastr, *av);
                ac--;
                strcpy (decstr, *++av);
                ra[npos] = str2ra (rastr);
                dec[npos] = str2dec (decstr);
                ac--;
                if (ac < 1) {
                    syscoor[npos] = WCS_J2000;
                    eqcoor[npos] = 2000.0;
                    }
                else if ((syscoor[npos] = wcscsys (*(av+1))) >= 0)
                    eqcoor[npos] = wcsceq (*++av);
                else {
                    syscoor[npos] = WCS_J2000;
                    eqcoor[npos] = 2000.0;
                    }
		npos++;
                }
            }

	else if (ac > 1 && isnum (*av)) {
	    if (!isnum (*(av+1)))
		usage(*av);
	    else {
		strcpy (rastr, *av);
		ac--;
		strcpy (decstr, *++av);
                ra[npos] = str2ra (rastr);
                dec[npos] = str2dec (decstr);
                ac--;
                if (ac < 1) {
                    syscoor[npos] = WCS_J2000;
                    eqcoor[npos] = 2000.0;
                    }
                else if ((syscoor[npos] = wcscsys (*(av+1))) >= 0)
                    eqcoor[npos] = wcsceq (*++av);
                else {
                    syscoor[npos] = WCS_J2000;
                    eqcoor[npos] = 2000.0;
                    }
		npos++;
                }
	    }

	else if (*(str = *av) == '-') {
	    char c;
	    while (c = *++str)
	    switch (c) {

		case 'a':       /* Print all objects found by NED */
		    printall++;
		    break;

		case 'd':       /* Print object position in degrees */
		    printpos++;
		    printdeg++;
		    break;

		case 'i':       /* Print all IDs found by NED */
		    printid++;
		    break;

		case 'm':       /* Return object magnitude */
		    printmag++;
		    break;

		case 'o':       /* Return object type code */
		    printtype++;
		    break;

		case 'p':       /* Print position(s) of objects found by NED */
		    printpos++;
		    break;

		case 'r':       /* Search box or circle half-size in arcsec */
		   if (ac < 2)
			usage();

		    /* Convert radius or first argument to arcminutes */
		    av++;
		    if (strchr (*av,':'))
			radius = 60.0 * str2dec (*av);
		    else
			radius = atof (*av) / 60.0;
		    ac--;
		    break;

		case 's':       /* Drop spaces from object names */
		    dropspace++;
		    break;

		case 'v':       /* print all information returned by NED */
		    verbose++;
		    break;

		case 'z':       /* Return object velocity */
		    printvel++;
		    break;

		default:
		    usage();
		    break;
		}
	    }
	}

    /* There are ac remaining arguments starting at av[0] */
    if (syscoor == 0)
        usage ();

   for (ipos = 0; ipos < npos; ipos++) {

	ra0 = ra[ipos];
	dec0 = dec[ipos];
	wcscon (syscoor[ipos],WCS_J2000,eqcoor[ipos],2000.0,&ra0,&dec0,0.0);
	if (verbose) {
	    ra2str (rastr, 32, ra0, 3);
	    dec2str (decstr, 32, dec0, 2);
	    wcscstr (cstr, syscoor[ipos], eqcoor[ipos], 0.0);
	    printf ("%s %s %s:\n", rastr, decstr, cstr);
	    printid = 1;
	    printpos = 1;
	    printall = 1;
	    }

	st = ned_obj_nearposn (ra0, dec0, radius, &no_obj, &op);

	/* for simple error message */
	if (st < 0) {
	    /* fprintf(stderr, "%s\n", ned_get_errmsg()); */

	    switch (ned_errno) {
		case NE_RA:
		    fprintf(stderr, "ra is out of range\n");
		    break;
		case NE_DEC:
		    fprintf(stderr, "dec is out of range\n");
		    break;
		case NE_RADIUS:
		    fprintf(stderr, "radius is out of range\n");
		    break;
		case NE_NOBJ:
		    fprintf(stderr, "no object found in NED database\n");
		    break;
		case NE_NOSPACE:
		    fprintf(stderr, "memory allocation error happened \n");
		    break;
		case NE_QUERY:
		    fprintf(stderr, "Can't send query to the NED server\n");
		    break;
		case NE_BROKENC:
		    fprintf(stderr, "The connection to server is broken\n");
		    break;
		}
	    } /* -1 return code */
	else {
	    if (verbose && no_obj > 1)
		fprintf (stdout, "%d object(s) found in NED: \n", no_obj);
	    if (printall)
		n = no_obj;
	    else
		n = 1;
	    for (i=0, tmpop = op; i < n; i++, tmpop++) {
		if (verbose && no_obj > 1) {
		    fprintf (stdout, "%d crossid for object No. %d: \n", 
			    tmpop->no_crossid, i+1);
		    }
		if (printid || verbose)
		    nid = tmpop->no_crossid;
		else
		    nid = 1;

	    /* Loop through names for this found object */
		for (j=0, tmpcp = tmpop->cp; j<nid; j++, tmpcp++) {
		    lobj = strlen (tmpcp->objname);
		    while (tmpcp->objname[lobj-1] == ' ') {
			tmpcp->objname[--lobj] = (char) 0;
			}
		    lobj = strlen (tmpcp->objname);
		    objname = (char *) calloc (lobj, 1);
		    objin = tmpcp->objname;
		    objout = objname;
		    while (*objin != (char) 0) {
			if (*objin != ' ')
			    *objout++ = *objin;
			objin++;
			}

		    printf ("%s", objname);
		    if (printid && j < nid-1)
			printf (",");
		    }
		if (verbose)
		    printf (": ra= ");
		else
		    printf (" ");

		if (printpos || verbose) {
		    ra1 = tmpop->ra;
		    dec1 = tmpop->dec;
		    if (outsys != WCS_J2000)
			wcscon (sysj, outsys, 0.0, 0.0, &ra1, &dec1, 0.0);
		    if (printdeg)
			printf ("%f ", ra1);
		    else {
			ra2str (rastr, 31, ra1, 3);
			printf ("%s ", rastr);
			}
		    if (verbose)
			printf ("dec=");
		    if (printdeg)
			printf ("%f", dec1);
		    else {
			dec2str (decstr, 31, dec1, 2);
			printf ("%s", decstr);
			}
		    if (outsys == WCS_B1950)
			printf (" B1950");
		    else
			printf (" J2000");
		    dist = wcsdist (ra0,dec0,ra1,dec1);
		    if (verbose)
			printf (" dist= ");
		    if (dist < 1.0) {
			printf (" %.3f", dist * 3600.0);
			if (verbose)
			    printf (" arcsec");
			}
		    else {
			dec2str (decstr, 31, dist, 2);
			printf (" %s", decstr);
			}
		    if (no_obj > 1 && printpos & !printall)
			printf (" (%d)", no_obj);
		    }
		if (printtype || verbose)
		    printf (" %s", tmpop->objtype);

	    /* extract interesting information for this object */
		if (verbose)
		    printf ("\n");
		mdatap = tmpop->mdp;
		strcpy (velocity, "none");
		strcpy (magnitude, "none");
		while (mdatap) {
		    if (verbose)
			printf ("%-10s: %s\n", mdatap->data_typec,mdatap->data);
		    if (!strcmp (mdatap->data_typec, "HRV"))
			strcpy (velocity, mdatap->data);
		    if (!strcmp (mdatap->data_typec, "MAG"))
			strcpy (magnitude, mdatap->data);
		    if (!strcmp (mdatap->data_typec, "Z")) {
			strcpy (zstring, mdatap->data);
			v = c * atof (zstring);
			sprintf (velocity, "%.5f", v);
			}
		    mdatap = mdatap->next;
		    }
		if (printmag)
		    printf (" %s", magnitude);
		if (printvel)
		    printf (" %s", velocity);
		printf ("\n");
		}
	    }

	if (op)
	    ned_free_objp (op, no_obj);
	}
    ned_disconnect();
}

static void
usage ()
{
    fprintf (stderr,"nedname: Return object name for RA and Dec using NED, the NASA/IPAC\n");
    fprintf (stderr,"        Extragalactic Database from JPL at Caltech\n");
    fprintf (stderr,"Usage:  nedname [-idmopvz] [-r rad] ra dec sys\n");
    fprintf (stderr,"-a: Print all objects returned from NED\n");
    fprintf (stderr,"-d: Print coordinates in degrees instead of sexigesimal\n");
    fprintf (stderr,"-i: Print all IDs for each object returned from NED\n");
    fprintf (stderr,"-m: Print magnitude for each object returned from NED\n");
    fprintf (stderr,"-o: Print object type for each object returned from NED\n");
    fprintf (stderr,"-p: Print position for each object returned from NED\n");
    fprintf (stderr,"-r: Radius in arcseconds to search for objects\n");
    fprintf (stderr,"-v: Print extra descriptive info\n");
    fprintf (stderr,"-z: Print velocity for each object returned from NED\n");
    exit (1);
}

/* Jun 13 2002	New program based on IPAC byname.c (Xiuqin Wu, 1996)
 *
 * Jul 28 2004	Fixed lots of bugs to make program work for the first time
 * Jul 29 2004	Add option to input coordinates as fractional degrees
 * Jul 29 2004	Add options to print additional information about objects
 * Aug  2 2004	Debug name truncation and add degree position output
 * Aug 23 2004	Read velocity from Z as well as HRV
 */
