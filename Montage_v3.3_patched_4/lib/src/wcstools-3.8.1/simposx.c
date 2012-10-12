/*** simpos.c - search object by its name from command line arguments
 *** October 3, 2007
 *** By Doug Mink, sort of after IPAC byname.c
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "libwcs/wcs.h"
#include "libwcs/fitsfile.h"
#include "libwcs/wcscat.h"

extern int   ned_errno;
static void PrintUsage();
static char *RevMsg = "SIMPOS 3.8.1, 14 December 2009, Doug Mink SAO";

int
main (ac, av)
int  ac;
char *av[];
{
   
    int lobj;
    int i;
    int verbose = 0;
    int printid = 0;
    int printdeg = 0;
    int nid = 0;
    int nobj = 0;
    int nfobj = 0;
    int outsys = WCS_J2000;
    int sysj = WCS_J2000;
    int tabout = 0;
    int nameout = 1;
    double ra, dec;
    char *str, *objname, *posdec, *posra;
    char *listfile;
    char rastr[32], decstr[32];
    char newobj[32];
    char *buff, *idline, *posline, *errline, *id, *errend;
    char url[256];
    int lbuff;
    FILE *flist;

    listfile = NULL;

    str = *(av+1);
    if (!str || !strcmp (str, "help") || !strcmp (str, "-help"))
	PrintUsage (NULL);
    if (!strcmp (str, "version") || !strcmp (str, "-version"))
	PrintUsage ("version");


    /* crack arguments */
    for (av++; --ac > 0 && *(str = *av) == '-'; av++) {

	if (*(str = *av) = '-') {
            char c;
            while (c = *++str)
            switch (c) {

        	case 'b':       /* Print coordinates in B1950 */
	            outsys = WCS_B1950;
		    break;

		case 'd':       /* Print coordinates in degrees */
		    printdeg++;
		    break;

		case 'e':       /* Print coordinates in ecliptic coordinates */
		    outsys = WCS_ECLIPTIC;
		    break;

		case 'g':       /* Print coordinates in galactic coordinates */
		    outsys = WCS_GALACTIC;
		    break;

		case 'i':       /* Print all IDs found by SIMBAD */
		    printid++;
		    break;

		case 'n':       /* Set flag to print position only */
		    nameout = 0;
		    break;

		case 't':       /* Print output in tab-separated table */
		    tabout++;
		    break;

		case 'v':       /* more verbosity, including first ID */
		    verbose++;
		    break;

		default:
		    PrintUsage(NULL);
		    break;
		}
	    }

	/* File containing a list of object names */
	else if (*av[0] == '@') {
	    listfile = *av + 1;
	    if (isfile (listfile)) {
		nfobj = getfilelines (listfile);
		if (verbose)
		    fprintf (stderr,"NEDPOS: %d objects from file %s\n",
			     nfobj, listfile);
		if ((flist = fopen (listfile, "r")) == NULL) {
		    fprintf (stderr,"NEDPOS: List file %s cannot be read\n",
			     listfile);
		    }
		}
	    else {
		printf ("NEDPOS: List file %s does not exist\n", listfile);
		listfile = NULL;
		}
	    
	    }
	}

    /* There are ac remaining file names starting at av[0] */
    if (ac == 0 && !listfile)
        PrintUsage (NULL);

    if (listfile) {
	ac = nfobj;
	objname = newobj;
	}
    while (ac > 0) {

	if (listfile)
	    fgets (newobj, 32, flist);
	else
	    objname = *av++;
	ac--;

	/* Replace underscores and spaces with plusses */
	lobj = strlen (objname);
	for (i = 0; i < lobj; i++) {
	    if (objname[i] == '_')
		objname[i] = '+';
	    if (objname[i] == ' ')
		objname[i] = '+';
	    }
	if (verbose)
	    printf ("%s -> ", objname);

	strcpy (url, "http://vizier.u-strasbg.fr/cgi-bin/nph-sesame?");
	strcat (url, objname);
	buff = webbuff (url, verbose, &lbuff);

	if (buff == NULL) {
	    if (verbose)
		printf ("no return from SIMBAD\n");
	    else
		fprintf (stderr,"*** No return from SIMBAD for %s\n",objname);
	    continue;
	    }

	/* Read number of objects identified */
	if ((idline = strsrch (buff, "#=")) != NULL) {
	    id = strchr (idline, ':');
	    if (id != NULL)
		nid = atoi (id+2);
	    else
		nid = 0;

	/* Get position */
	    if ((posline = strsrch (buff, "%J ")) != NULL) {
		posra = posline + 3;
		while (*posra == ' ')
		    posra++;
		ra = atof (posra);
		posdec = strchr (posra, ' ');
		while (*posdec == ' ')
		    posdec++;
		posline = strchr (posdec, ' ');
		dec = atof (posdec);
		}
	    else {
		if (verbose)
		    printf ("no position from SIMBAD\n");
		else
		    fprintf (stderr,"*** No SIMBAD position for %s\n",objname);
		continue;
		}
	    }

	else {
	    nid = 0;
	    if ((errline = strsrch (buff, "#!SIMBAD: ")) != NULL) {
		if ((errend = strchr (errline, '\n')) != NULL)
		   *errend = (char) 0;
		fprintf (stderr, "*** %s\n", errline+10);
		}
	    else {
		fprintf (stderr, "*** No SIMBAD position for %s\n", objname);
		}
	    continue;
	    }

	if (nid > 0) {
	    if (verbose) {
		if (nid == 1)
		    fprintf (stdout, "%d object found by SIMBAD: \n", nid);
		else
		    fprintf (stdout, "%d objects found by SIMBAD: \n", nid);
		}

	/* Print Starbase header if requested */
	    nobj++;
	    if (tabout && nobj == 1) {
		printf ("catalog	SIMBAD\n");
		if (outsys == WCS_GALACTIC)
		    printf ("radecsys	galactic\n");
		else if (outsys == WCS_ECLIPTIC)
		    printf ("radecsys	ecliptic\n");
		else if (outsys == WCS_B1950)
		    printf ("radecsys	B1950\n");
		else
		    printf ("radecsys	J2000\n");
		if (outsys == WCS_B1950) {
		    printf ("equinox	1950.0\n");
		    printf ("epoch	1950.0\n");
		    }
		else {
		    printf ("equinox	2000.0\n");
		    printf ("epoch	2000.0\n");
		    }
		printf ("program	%s\n",RevMsg);
		if (nameout) {
		    printf ("id    	ra      	dec       ");
		    printf ("\n");
		    printf ("--	------------	------------");
		    printf ("\n");
		    }
		else {
		    printf ("ra             	dec         ");
		    printf ("\n");
		    printf ("------------	------------");
		    printf ("\n");
		    }
		}
	    if (nameout) {
		printf ("%s", objname);
		if (tabout)
		    printf ("	");
		else
		    printf (" ");
		}
	    if (outsys != WCS_J2000)
		wcscon (sysj, outsys, 0.0, 0.0, &ra, &dec, 0.0);
	    if (outsys == WCS_ECLIPTIC || outsys == WCS_GALACTIC) {
		if (verbose)
		    fprintf (stdout, "l= ");
		fprintf (stdout, "%.6f", ra);
		if (tabout)
		    printf ("	");
		else
		    printf (" ");
		if (verbose)
		    fprintf (stdout, "b= ");
		if (dec >= 0.0)
		    fprintf (stdout, "+");
		fprintf (stdout, "%.6f", dec);
		if (!tabout) {
		    if (outsys == WCS_GALACTIC)
			fprintf (stdout, " Galactic");
		    else if (outsys == WCS_ECLIPTIC)
			fprintf (stdout, " Ecliptic");
		    }
		fprintf (stdout, "\n");
		}
	    else {
		if (verbose)
		    fprintf (stdout, "ra= ");
		if (printdeg)
		    fprintf (stdout, "%.6f", ra);
		else {
		    ra2str (rastr, 31, ra, 3);
		    fprintf (stdout, "%s", rastr);
		    }
		if (tabout)
		    printf ("	");
		else
		    printf (" ");
		if (verbose)
		    fprintf (stdout, "dec=");
		if (printdeg)
		    fprintf (stdout, "%.6f", dec);
		else {
		    dec2str (decstr, 31, dec, 2);
		    fprintf (stdout, "%s", decstr);
		    }
		if (!tabout) {
		    if (outsys == WCS_B1950)
			fprintf (stdout, " B1950");
		    else
			fprintf (stdout, " J2000");
		    }
		fprintf (stdout, "\n");
		}
	    }
	free (buff);
	}
    exit (0);
}

static void
PrintUsage (command)

char	*command;	/* Command where error occurred or NULL */

{
    fprintf (stderr,"%s\n", RevMsg);
    fprintf (stderr,"Return RA and Dec for object name using SIMBAD\n");
    fprintf (stderr,"Usage:  simpos [-idtv][b|e|g] name1 name2 ...\n");
    fprintf (stderr,"        simpos [-idtv][b|e|g] @namelist ...\n");
    fprintf (stderr,"name(n): Objects for which to search (space -> _)\n");
    fprintf (stderr,"namelist: File with one object name per line\n");
    fprintf (stderr,"-b: Print coordinates in B1950 instead of J2000\n");
    fprintf (stderr,"-d: Print coordinates in degrees instead of sexigesimal\n");
    fprintf (stderr,"-e: Print coordinates in ecliptic instead of J2000\n");
    fprintf (stderr,"-g: Print coordinates in galactic instead of J2000\n");
    fprintf (stderr,"-n: Print position without object name\n");
    fprintf (stderr,"-i: Print ID returned from SIMBAD\n");
    fprintf (stderr,"-t: Print output as tab-separated table\n");
    fprintf (stderr,"-v: Print extra descriptive info\n");
    exit (1);
}

/* Oct 25 2002	New program based on nedpos.c
 *
 * Jun 20 2006	Clean up code
 *
 * Jan 10 2007	Declare RevMsg static, not const
 * Jan 10 2007	exit(0) if successful
 * Jan 11 2007	Include fitsfile.h instead of fitshead.h
 * Jan 16 2007	Fix leading space bug
 * Sep 19 2007	Add -t option for Starbase output and @ for lists of names
 * Oct  3 2007	Add -n option to print position without object name
 */
