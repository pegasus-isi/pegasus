/* File getftab.c
 * January 10, 2007
 * By Doug Mink Harvard-Smithsonian Center for Astrophysics)
 * Send bug reports to dmink@cfa.harvard.edu

   Copyright (C) 1999-2007
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
#include "libwcs/wcs.h"
#include "libwcs/wcscat.h"

#define MAXCOL 200
#define MAXCOND 10
#define MAXFILES 1000
#define MAXLINES 1000

static void usage();
static void PrintValues();
static int maxncond = 100;

static char *RevMsg = "GETFTAB WCSTools 3.8.1, 14 December 2009, Doug Mink (dmink@cfa.harvard.edu)";

static int verbose = 0;		/* verbose/debugging flag */
static int nfile = 0;
static int ndec = -9;
static int maxlfn = 0;
static int listall = 0;
static int listpath = 0;
static int tabout = 0;
static int printhead = 0;
static int assign = 0;
static int version = 0;		/* If 1, print only program name and version */
static int nlines = 0;
static int nkeep = 0;
static int *keeplines;		/* List of lines to keep if nkeep > 0 */
static int ncond=0;		/* Number of keyword conditions to check */
static int condand=1;		/* If 1, AND comparisons, else OR */
static char **cond;		/* Conditions to check */
static char **ccond;		/* Comparison characters */
static char *tcond;		/* Condition character */
struct TabTable *tabtable;

main (ac, av)
int ac;
char **av;
{
    char *str;
    char *kwd[MAXCOL];
    char *alias[MAXCOL];
    int ifile;
    int nkwd = 0;
    char *fn[MAXFILES];
    int readlist = 0;
    int lfn;
    char *lastchar;
    char filename[128];
    char *name;
    char ctemp;
    FILE *flist;
    char *listfile;
    int ikwd, lkwd, i;
    int lfield;
    char *kw1;
    char *cstr, *nstr;
    char string[80];
    int icond;
    char *vali, *calias, *valeq, *valgt, *vallt;
    char *ranges = NULL;
    char *temp;
    int nldef = 1;
    int lstr;
    double dnum;
    struct Range *range; /* Range of sequence numbers to list */

    /* Check for help or version command first */
    str = *(av+1);
    if (!str || !strcmp (str, "help") || !strcmp (str, "-help"))
	usage();
    if (!strcmp (str, "version") || !strcmp (str, "-version")) {
	version = 1;
	usage();
	}

    cond = (char **)calloc (maxncond, sizeof(char *));
    ccond = (char **)calloc (maxncond, sizeof(char *));
    tcond = (char *)calloc (maxncond, sizeof(char));

    nkwd = 0;
    ncond = 0;
    nfile = 0;

    /* crack arguments */
    for (av++; --ac > 0; av++) {
	if (*(str = *av)=='-') {
	    char c;
	    while (c = *++str)
	    switch (c) {

		case 'a':	/* list file even if no keywords are found */
		    listall++;
		    break;

		case 'e':	/* output assignments */
		    assign++;
		    break;

		case 'v':	/* more verbosity */
		    verbose++;
		    break;

		case 'h':	/* output column headings */
		    printhead++;
		    break;

		case 'n':	/* number of decimal places in output */
		    if (ac < 2)
			usage();
		    ndec = (int) (atof (*++av));
		    ac--;
		    break;

		case 'o': /* OR conditions insted of ANDing them */
		    condand = 0;
		    break;

		case 'p':	/* output column headings */
		    listpath++;
		    break;

		case 't':	/* output tab table */
		    tabout++;
		    break;

		default:
		    usage();
		    break;
		}
	    continue;
	    }

	/* Set range and make a list of line numbers from it */
	else if (isrange (*av)) {
	    if (ranges) {
		temp = ranges;
		ranges = (char *) calloc (strlen(ranges) + strlen(*av) + 2, 1);
		strcpy (ranges, temp);
		strcat (ranges, ",");
		strcat (ranges, *av);
		free (temp);
		}
	    else {
		ranges = (char *) calloc (strlen(*av) + 1, 1);
		strcpy (ranges, *av);
		}
	    continue;
	    }

	/* If numeric argument, set line to be read */
	else if (isnum (str)) {
	    if (ranges) {
		temp = ranges;
		ranges = (char *)calloc (strlen(ranges)+strlen(*av)+2, 1);
		strcpy (ranges, temp);
		strcat (ranges, ",");
		strcat (ranges, *av);
		free (temp);
		}
	    else {
		ranges = (char *) calloc (strlen(*av) + 1, 1);
		strcpy (ranges, *av);
		}
	    continue;
	    }

	else if (*av[0] == '@') {
	    readlist++;
	    listfile = *av + 1;
	    nfile = 2;
	    continue;
	    }
	else if (!strcmp (*av, "stdin")) {
	    fn[nfile] = *av;
	    name = fn[nfile];
	    lfn = strlen (name);
	    if (lfn > maxlfn)
		maxlfn = lfn;
	    nfile++;
	    continue;
	    }

	/* Record condition
	else if (strchr (*av, '=') || strchr (*av, '<') || strchr (*av, '>')) {
	    cond[ncond] = *av;
	    if (ncond < MAXCOND)
		ncond++;
	    continue;
	    } */

	/* Record column name with output alias */
	else if (strchr (*av, '@')) {
	    kwd[nkwd] = *av;
	    calias = strchr (*av, '@');
	    alias[nkwd] = calias + 1;
	    *calias = (char) 0;
	    if (nkwd < MAXCOL)
		nkwd++;
	    continue;
	    }

	/* Record tab table file name */
	else if (istab (*av)) {
	    fn[nfile] = *av;

	    if (listpath || (name = strrchr (fn[nfile],'/')) == NULL)
		name = fn[nfile];
	    else
		name = name + 1;
	    lfn = strlen (name);
	    if (lfn > maxlfn)
		maxlfn = lfn;
	    if (nfile < MAXFILES)
		nfile++;
	    continue;
	    }

	/* Condition */
	else if (strchr (*av, '=') != NULL || strchr (*av, '#') != NULL ||
		 strchr (*av, '>') != NULL || strchr (*av, '<') != NULL ) {
	    if (ncond >= maxncond) {
		maxncond = maxncond * 2;
		cond = (char **)realloc((void *)cond,maxncond*sizeof(void *));
		ccond = (char **)realloc((void *)ccond,maxncond*sizeof(void *));
		tcond = (char *)realloc((void *)tcond,maxncond*sizeof(char));
		}
	    cond[ncond] = *av;
	    cstr = strchr (*av, '=');
	    if (cstr != NULL)
		tcond[ncond] = '=';
	    else {
		cstr = strchr (*av, '#');
		if (cstr != NULL)
		    tcond[ncond] = '#';
		else {
		    cstr = strchr (*av, '>');
		    if (cstr != NULL)
			tcond[ncond] = '>';
		    else {
			cstr = strchr (*av, '<');
			if (cstr != NULL)
			    tcond[ncond] = '<';
			else
			    tcond[ncond] = (char) 0;
			}
		    }
		}
	    if (tcond[ncond]) {
		cstr[0] = (char) 0;
		cstr++;
		strfix (cstr, 0, 1);
		if (strchr (cstr, ':')) {
		    dnum = str2dec (cstr);
		    nstr = (char *) calloc (32, sizeof(char));
		    num2str (nstr, dnum, 0, 7);
		    ccond[ncond] = nstr;
		    }
		else {
		    lstr = strlen (cstr);
		    ccond[ncond] = (char *) calloc (lstr+2, sizeof(char));
		    strcpy (ccond[ncond], cstr);
		    }
		ncond++;
		}
	    }

	/* Record column name */
	else {
	    kwd[nkwd] = *av;
	    alias[nkwd] = NULL;
	    if (nkwd < MAXCOL)
		nkwd++;
	    continue;
	    }
	}

    /* Read from standard input if no file is specified */
    if (nfile == 0 && (nkwd > 0 || ncond > 0)) {
	name = malloc (8);
	strcpy (name, "stdin");
	fn[nfile] = name;
	lfn = strlen (name);
	if (lfn > maxlfn)
	    maxlfn = lfn;
	nfile++;
	}

    /* Decode ranges */
    if (ranges != NULL) {
	range = RangeInit (ranges, nldef);
	nkeep = rgetn (range);
	keeplines = (int *) calloc (1, nkeep);
	for (i = 0; i < nkeep; i++)
	    keeplines[i] = rgeti4 (range);
	}

    if (nkwd > 0) {

	/* Print column headings if tab table or headings requested */
	if (printhead || tabout) {

	    /* Open the input tab table */
	    if ((tabtable = tabopen (fn[0], 0)) == NULL) {
		fprintf (stderr,"%s\n", gettaberr());
		return (1);
		}

	    /* Print the name of the table, if it has one */
	    if (tabtable->tabname != NULL)
		printf ("%s\n", tabtable->tabname);

	    /* For tab table output, keep input header information */
	    if (tabout) {
		if (tabtable->tabheader != tabtable->tabhead) {
		    ctemp = *(tabtable->tabhead-1);
		    *(tabtable->tabhead-1) = (char) 0;
		    printf ("%s\n", tabtable->tabheader);
		    *(tabtable->tabhead-1) = ctemp;
		    }
		}

	    /* Print conditions in header */
	    for (icond = 0; icond < ncond; icond++) {
		if (verbose) {
		    if (condand || icond == 0)
			printf ("%s\n",cond[icond]);
		    else
			printf (" or %s\n",cond[icond]);
		    }
		else if (tabout) {
		    if (condand || icond == 0)
			printf ("condition	%s %c %s\n",
				cond[icond], tcond[icond], ccond[icond]);
		    else
			printf ("condition	or %s %c %s\n",
				cond[icond], tcond[icond], ccond[icond]);
		    }
		}

	    /* If multiple files, add filename at start of output line */
	    if (nfile > 1) {
		printf ("filename");
		if (maxlfn > 8) {
		    for (i = 8; i < maxlfn; i++)
			printf (" ");
		    }
		if (tabout)
	    	    printf ("	");
		else
		    printf (" ");
		}

	    /* Print column names */
	    for (ikwd = 0; ikwd < nkwd; ikwd++) {
		if (alias[ikwd]) {
		    kw1 = alias[ikwd];
		    lkwd = strlen (alias[ikwd]);
		    }
		else {
		    kw1 = kwd[ikwd];
		    lkwd = strlen (kwd[ikwd]);
		    }
		printf ("%s",kw1);
		if ((i = tabcol (tabtable, kwd[ikwd])) > 0) {
		    lfield = tabtable->lcfld[i-1];
		    if (lfield > 32)
			lfield = 32;
		    }
		if (tabout && lfield > lkwd) {
		    for (i = lkwd; i < lfield; i++)
			printf (" ");
		    }
		if (verbose || ikwd == nkwd - 1)
	    	    printf ("\n");
		else if (tabout)
	    	    printf ("	");
		else
		    printf (" ");
		}

	    /* Print field-defining hyphens if tab table output requested */
	    if (tabout) {
		for (ikwd = 0; ikwd < nkwd; ikwd++) {
		    if ((i = tabcol (tabtable, kwd[ikwd])) > 0) {
			lfield = tabtable->lcfld[i-1];
			for (i = 0; i < lfield; i++)
			    printf ("-");
			if (ikwd == nkwd - 1)
			    printf ("\n");
			else
			    printf ("	");
			}
		    }
		}
	    }

    /* Get table values one at a time */

	/* Read through tables in listfile */
	if (readlist) {
	    if ((flist = fopen (listfile, "r")) == NULL) {
		fprintf (stderr,"GETTAB: List file %s cannot be read\n",
		     listfile);
		usage ();
		}
	    while (fgets (filename, 128, flist) != NULL) {
		lastchar = filename + strlen (filename) - 1;
		if (*lastchar < 32) *lastchar = 0;
		PrintValues (filename,nkwd,kwd,alias);
		if (verbose)
		    printf ("\n");
		}
	    fclose (flist);
	    }

	/* Read tables from command line list */
	else {
	    for (ifile = 0; ifile < nfile; ifile++)
	    	PrintValues (fn[ifile],nkwd,kwd,alias);
	    }
	}

    else {
	if (printhead || tabout) {

	    /* Open the input tab table */
	    if (strcasecmp (name, "stdin")) {
		if ((tabtable = tabopen (fn[0], 0)) == NULL) {
		    fprintf (stderr,"%s\n", gettaberr());
		    return (1);
		    }
		}

	    /* Print the name of the table, if it has one */
	    if (tabtable->tabname != NULL)
		printf ("%s\n", tabtable->tabname);

	    /* For tab table output, keep input header information */
	    if (tabout) {
		if (tabtable->tabheader != tabtable->tabhead) {
		    ctemp = *(tabtable->tabhead-1);
		    *(tabtable->tabhead-1) = (char) 0;
		    printf ("%s\n", tabtable->tabheader);
		    *(tabtable->tabhead-1) = ctemp;
		    }
		}

	    /* Print conditions in header */
	    for (icond = 0; icond < ncond; icond++) {
		if (verbose) {
		    if (condand || icond == 0)
			printf ("%s\n",cond[icond]);
		    else
			printf (" or %s\n",cond[icond]);
		    }
		else if (tabout) {
		    if (condand || icond == 0)
			printf ("condition	%s %c %s\n",
				cond[icond], tcond[icond], ccond[icond]);
		    else
			printf ("condition	or %s %c %s\n",
				cond[icond], tcond[icond], ccond[icond]);
		    }
		}

	    /* If multiple files, add filename at start of output line */
	    if (nfile > 1) {
		printf ("filename");
		if (maxlfn > 8) {
		    for (i = 8; i < maxlfn; i++)
			printf (" ");
		    }
		if (tabout)
	    	    printf ("	");
		else
		    printf (" ");
		}

	    /* Print column headers */
	    ctemp = *(tabtable->tabdata-1);
	    *(tabtable->tabdata-1) = (char) 0;
	    printf ("%s\n", tabtable->tabhead);
	    *(tabtable->tabdata-1) = ctemp;
	    }
	for (ifile = 0; ifile < nfile; ifile++)
	    PrintValues (fn[ifile],nkwd,kwd,alias);
	}

    if (ccond != NULL) {
	for (i = 0; i < ncond; i++)
	    free (ccond[i]);
	free (ccond);
	}
    if (cond != NULL)
	free (cond);
    if (tcond != NULL)
	free (tcond);
    return (0);
}

static void
usage ()
{
    fprintf (stderr,"%s\n",RevMsg);
    if (version)
	exit (-1);
    fprintf (stderr,"Print FITS or IRAF header keyword values\n");
    fprintf(stderr,"usage: gettab [-ahoptv][-n num] file1.tab ... filen.tab kw1 kw2 ... kwn\n");
    fprintf(stderr,"       gettab [-ahoptv][-n num] @filelist kw1 kw2 ... kwn\n");
    fprintf(stderr,"       gettab [-ahoptv][-n num] <file1.tab kw1 kw2 ... kwn\n");
    fprintf(stderr,"  -a: List file even if keywords are not found\n");
    fprintf(stderr,"  -e: Print keyword=value list\n");
    fprintf(stderr,"  -h: Print column headings\n");
    fprintf(stderr,"  -n: Number of decimal places in numeric output\n");
    fprintf(stderr,"  -o: OR conditions instead of ANDing them\n");
    fprintf(stderr,"  -p: Print full pathnames of files\n");
    fprintf(stderr,"  -t: Output in tab-separated table format\n");
    fprintf(stderr,"  -v: Verbose\n");
    exit (1);
}


static void
PrintValues (name, nkwd, kwd, alias)

char	*name;	  /* Name of FITS or IRAF image file */
int	nkwd;	  /* Number of keywords for which to print values */
char	*kwd[];	  /* Names of keywords for which to print values */
char	*alias[]; /* Output names of keywords if different from input */

{
    char *str;
    char *cstr, *cval, cvalue[64];
    char numstr[32], numstr1[32];
    int pass;
    int drop;
    int jval, jcond, icond, i, lstr;
    double dval, dcond, dnum;
    char fnform[8];
    char string[80];
    char *filename;
    char outline[1000];
    char *line, *last;
    char *endline;
    char newline = 10;
    int ikwd, nfound;
    int iline, keep;
    double xnum;
    struct Tokens tokens;
    int *col;
    int *ccol;
    int ntok;
    int *nextkeep;
    int lastkeep;

    if (nkeep > 0) {
	nextkeep = keeplines;
	lastkeep = keeplines[nkeep - 1];
	}

    /* Figure out conditions first, separating out keywords to check */

    /* Read tab table and set up data structure */
    if (tabtable == NULL) {
	if ((tabtable = tabopen (name, 0)) == NULL)
	    return;
	}

    if (verbose) {
	fprintf (stderr,"%s\n",RevMsg);
	fprintf (stderr,"Print table Values from tab table file %s\n", name);
	}

    /* Find file name */
    if (listpath || (filename = strrchr (name,'/')) == NULL)
	filename = name;
    else
	filename = filename + 1;

    if (nfile > 1) {
	if (tabout)
	    sprintf (fnform, "%%-%ds	", maxlfn);
	else
	    sprintf (fnform, "%%-%ds ", maxlfn);
	sprintf (outline, fnform, filename);
	}

    nfound = 0;
    line = tabtable->tabdata;
    last = line + strlen (tabtable->tabdata);

    /* Find column numbers for column names to speed up comparisons */
    if (ncond > 0) {
	ccol = (int *) calloc (ncond, sizeof (int));
	ccol[0] = 0;
	for (icond = 0; icond < ncond; icond++)
            ccol[icond] = tabcol (tabtable, cond[icond]);
	}

    /* Find column numbers for column names to speed up extraction */
    col = (int *) calloc (nkwd, sizeof (int));
    col[0] = 0;
    for (ikwd = 0; ikwd < nkwd; ikwd++)
	col[ikwd] = tabcol (tabtable, kwd[ikwd]);

    iline = 0;
    while (line != NULL && line < last) {
	if (*line == (char)12)
	    break;
	if (*line != newline) {
	    outline[0] = (char) 0;

	    /* Check line number if extracting specific lines */
	    iline++;
	    drop = 0;
	    if (nkeep > 0) {
		if (iline != *nextkeep)
		    pass = 0;
		else if (*nextkeep < lastkeep) {
		    pass = 1;
		    nextkeep++;
		    }
		}
	    else
		pass = 0;

	    /* Check conditions */
	    ntok = setoken (&tokens, line, "tab");
	    if (ncond > 0) {
		for (icond = 0; icond < ncond; icond++) {
		    if (condand)
			pass = 0;

		/* Extract test value from comparison string */
		cstr = ccond[icond];

		/* Read comparison value from tab table */
		if (tabgetc (&tokens, ccol[icond], cvalue, 64))
		    continue;
		cval = cvalue;
		if (strchr (cval, ':')) {
		    dnum = str2dec (cval);
		    num2str (numstr1, dnum, 0, 7);
		    cval = numstr1;
		    }
		strfix (cval, 0, 1);

		/* Compare floating point numbers */
		if (isnum (cstr) == 2 && isnum (cval)) {
		    dcond = atof (cstr);
		    dval = atof (cval);
		    if (tcond[icond] == '=' && dval == dcond)
			pass = 1;
		    else if (tcond[icond] == '#' && dval != dcond)
			pass = 1;
		    else if (tcond[icond] == '>' && dval > dcond)
			pass = 1;
		    else if (tcond[icond] == '<' && dval < dcond)
			pass = 1;
		    }

		/* Compare integers */
		else if (isnum (cstr) == 1 && isnum (cval)) {
		    jcond = atoi (cstr);
		    jval = atoi (cval);
		    if (tcond[icond] == '=' && jval == jcond)
			pass = 1;
		    else if (tcond[icond] == '#' && jval != jcond)
			pass = 1;
		    else if (tcond[icond] == '>' && jval > jcond)
			pass = 1;
		    else if (tcond[icond] == '<' && jval < jcond)
			pass = 1;
		    }

		/* Compare strings (only equal or not equal */
		else {
		    if (tcond[icond] == '=' && !strcmp (cstr, cval))
			pass = 1;
		    else if (tcond[icond] == '#' && strcmp (cstr, cval))
			pass = 1;
		    }
		if (condand && !pass)
		    break;
		}
	    if (!pass) {
		line = strchr (line+1, newline);
		if (line == NULL)
		    break;
		if (strlen (line) < 1)
		    break;
		if (*++line == (char) 0)
		    break;
		continue;
		}
	    }

	/* Extract desired columns */
	if (nkwd == 0) {
	    endline = strchr (line+1, newline);
	    if (endline == NULL)
		break;
	    else {
		*endline = 0;
		printf ("%s\n", line);
		*endline = newline;
		}
	    }
	else {
	    ntok = setoken (&tokens, line, "tab");
	    for (ikwd = 0; ikwd < nkwd; ikwd++) {
		if (!tabgetc (&tokens, col[ikwd], string, 80)) {
		    str = strfix (string, 0, 0);
		    if (ndec > -9 && isnum (str) && strchr (str, '.'))
			num2str (str, atof(str), 0, ndec);
		    if (verbose) {
			if (alias[ikwd])
			    printf ("%s/%s = %s",kwd[ikwd],alias[ikwd],str);
			else
			    printf ("%s = %s", kwd[ikwd], str);
			}
		    else if (assign) {
			if (alias[ikwd])
			    strcat (outline, alias[ikwd]);
			else
			    strcat (outline, kwd[ikwd]);
			strcat (outline, "=");
			strcat (outline, str);
			 }
		    else
			strcat (outline, str);
		    nfound++;
		    }
		else if (verbose)
		    printf ("%s not found", kwd[ikwd]);
		else
		    strcat (outline, "___");

		if (verbose)
		    printf ("\n");
		else if (ikwd < nkwd-1) {
		    if (tabout)
			strcat (outline, "	");
		    else
			strcat (outline, " ");
		    }
		}

	    if (!verbose && (nfile < 2 || nfound > 0 || listall))
		printf ("%s\n", outline);
	    }
	    }

	line = strchr (line+1, newline);
	if (line == NULL)
	    break;
	if (strlen (line) < 1)
	    break;
	if (*++line == (char) 0)
	    break;
	if (nkeep > 0 && *nextkeep == lastkeep)
	    break;
	}

    tabclose (tabtable);
    tabtable = NULL;
    return;
}

/* Jan 22 1999	New program
 * Jan 25 1999	Keep header information
 * Mar  9 1999	Add range of lines; rework command line decoding logic
 * Oct 22 1999	Drop unused variables after lint
 *
 * Jan  3 2000	Use isrange() to check for ranges
 * Jan  4 2000	If no keywords are specified, print entire line if tests met
 * Feb 16 2000	Always open tab table if printing headers OR putting out tab
 * May 26 2000	Handle multiple tables in a single file
 * May 26 2000	Print full header if all columns asked for
 *
 * Jun 11 2001	Add buffer size argument to tabopen()
 * Jun 18 2001	Use token parsing to speed column extraction
 * Jun 29 2001	Open stdin only once
 * Oct 10 2001	Fix bugs in condition handling
 *
 * Feb 21 2002	Improve line range implementation
 * Apr 10 2002	Fix bug dealing with ranges
 *
 * Jan 22 2004	Increase maximum number of columns from 100 to 200
 * Apr 15 2004	Avoid removing trailing zeroes from exponents
 *
 * Jun 29 2006	Rename strclean() strfix() and move to hget.c
 *
 * Jan 10 2007	Declare RevMsg static, not const
 */
