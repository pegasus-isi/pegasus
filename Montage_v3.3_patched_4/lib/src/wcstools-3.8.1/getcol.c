/* File getcol.c
 * September 25, 2009
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
#include "libwcs/wcs.h"
#include "libwcs/fitsfile.h"
#include "libwcs/wcscat.h"

#define	MAX_LTOK	256
#define	MAX_NTOK	1024
#define MAXFILES	2000
#define MAXLINES	100000

static char *RevMsg = "GETCOL WCSTools 3.8.1, 14 December 2009, Doug Mink SAO";

static void usage();
static int ListFile();
static char *iscolop();
static int iscol();
static double median();

static int maxnfile = MAXFILES;

static double badval = 0.0;	/* Value to ignore */
static int isbadval = 0;	/* 1 if badval is set */
static int maxlfn = 0;
static int maxncond = 100;
static int maxnop = 100;
static int listpath = 0;	/* 1 to list pathnames */
static int verbose = 0;		/* Verbose/debugging flag */
static int sumcol = 0;		/* True to sum column values */
static int ameancol = 0;	/* True for absolute mean of column values */
static int meancol = 0;		/* True to compute mean of column values */
static int amedcol = 0;		/* True for absolute median of column values */
static int medcol = 0;		/* True to compute median of column values */
static int countcol = 0;	/* True to count entries in columns */
static int rangecol = 0;	/* True to print range of column values */
static int version = 0;		/* If 1, print only program name and version */
static int nread = 0;		/* Number of lines to read (0=all) */
static int nskip = 0;		/* Number of lines to skip */
static int tabout = 0;		/* If 1, separate output fields with tabs */
static int counttok = 0;	/* If 1, print number of columns on line */
static int printhead = 0;	/* If 1, print Starbase tab table header */
static int intcompare();
static int ncond=0;		/* Number of keyword conditions to check */
static int condand=1;		/* If 1, AND comparisons, else OR */
static char **cond;		/* Conditions to check */
static char **ccond;		/* Condition characters */
static int nop=0;		/* Number of keyword values to operate on */
static char **cop;		/* First column to operate on */
static int *lc;			/* Length of column 1 */
static char **op;		/* Operator and second column to operate on */
static int napp=0;		/* Number of lines to append */
static int ndec=-1;		/* Number of decimal places in f.p. output */
static int printcol = 1;	/* Flag to print extracted data */
static char *cwhite;
static int *frstchar;		/* First character of column to use (1-n) */
static int *lastchar;		/* Last character of column to use (1-n) */
static int qmeancol = 0;	/* If 1, print mean of columns added in quadrature */
				/* 0 = no mean of quadruature */
static char **format;

int
main (ac, av)
int ac;
char **av;
{
    char *str;
    char *temp;
    char **fn;
    int nfile = 0;
    int ifile;
    int nbytes, lfn;
    char *filename;
    char *ranges = NULL;
    char *lfile = NULL;
    char *lranges = NULL;
    char *cdot, *ccol;
    char *opi;
    int icol;
    int match, newbytes;
    int nrbytes = 0;
    int maxnop0;
    int i;
    char **op1;		/* Operator */
    char **cop1;	/* Operation character */
    int *lc1;		/* Length of column 1 */
    char sop1[16], sop2[16];

    cwhite = NULL;
    fn = (char **)calloc (maxnfile, sizeof(char *));

    if (ac == 1)
        usage ();

    cond = (char **)calloc (maxncond, sizeof(char *));
    ccond = (char **)calloc (maxncond, sizeof(char *));
    op = (char **)calloc (maxnop, sizeof(char *));
    lc = (int *)calloc (maxnop, sizeof(int *));
    cop = (char **)calloc (maxnop, sizeof(char));
    frstchar = (int *) calloc (MAX_NTOK, sizeof (int));
    lastchar = (int *) calloc (MAX_NTOK, sizeof (int));
    format = (char **)calloc (MAX_NTOK, sizeof (char *));

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

	/* Set column number */
	if (iscol (*av)) {
	    cdot = strchr (*av,'.');
	    if (cdot) {
		ccol = strchr (*av, ':');
		if (ccol) {
		    *cdot = (char) 0;
		    icol = atoi (*av);
		    *ccol = (char) 0;
		    if (icol > 0 && icol < MAX_NTOK) {
			if (strlen (cdot+1) > 0)
			    frstchar[icol] = atoi (cdot+1);
			else
			    frstchar[icol] = 1;
			if (strlen (ccol+1) > 0)
			    lastchar[icol] = atoi (ccol+1);
			else
			    lastchar[icol] = 0;
			}
		    }
		else
		    match = 1;
		}
		
	    if (ranges) {
		newbytes = strlen(ranges) + strlen(*av) + 2;
		newbytes = ((newbytes / 16) + 1) * 16;
		if (newbytes > nrbytes) {
		    temp = ranges;
		    ranges = (char *)calloc (newbytes, 1);
		    strcpy (ranges, temp);
		    nrbytes = newbytes;
		    free (temp);
		    }
		strcat (ranges, ",");
		strcat (ranges, *av);
		}
	    else {
		nrbytes = strlen(*av) + 2;
		nrbytes = ((nrbytes / 16) + 1) * 16;
		ranges = (char *) calloc (nrbytes, 1);
		strcpy (ranges, *av);
		}
	    icol = atoi (*av);
	    }

	/* Set range and make a list of column numbers from it */
	else if (isrange (*av)) {
	    if (ranges) {
		newbytes = strlen(ranges) + strlen(*av) + 2;
		newbytes = ((newbytes / 16) + 1) * 16;
		if (newbytes > nrbytes) {
		    temp = ranges;
		    ranges = (char *) calloc (newbytes, 1);
		    strcpy (ranges, temp);
		    nrbytes = newbytes;
		    free (temp);
		    }
		strcat (ranges, ",");
		strcat (ranges, *av);
		}
	    else {
		ranges = (char *) calloc (strlen(*av) + 2, 1);
		if (strchr (*av,'.'))
		    match = 1;
		strcpy (ranges, *av);
		}
	    }

	/* Negative numbers aren't ranges, but positive ones are */
	else if (isnum (*av)) {
	    if (ranges) {
		newbytes = strlen(ranges) + strlen(*av) + 2;
		newbytes = ((newbytes / 16) + 1) * 16;
		if (newbytes > nrbytes) {
		    temp = ranges;
		    ranges = (char *) calloc (newbytes, 1);
		    strcpy (ranges, temp);
		    nrbytes = newbytes;
		    free (temp);
		    }
		strcat (ranges, ",");
		strcat (ranges, *av);
		}
	    else {
		ranges = (char *) calloc (strlen(*av) + 2, 1);
		if (strchr (*av,'.'))
		    match = 1;
		strcpy (ranges, *av);
		}
	    }

	/* Read format string */
	else if (*(str = *av) == '%') {
	    if (icol > 0) {
		format[icol] = (char *) calloc (1+strlen(*av), 1);
		strcpy (format[icol], *av);
		}
	    }

	/* Read and save file name */
	else if (isfile (*av)) {
	    if (nfile >= maxnfile) {
		maxnfile = maxnfile * 2;
		nbytes = maxnfile * sizeof (char *);
		fn = (char **) realloc ((void *)fn, nbytes);
		}
	    fn[nfile] = *av;
	    if (listpath || (filename = strrchr (fn[nfile],'/')) == NULL)
		filename = fn[nfile];
	    else
		filename = filename + 1;
	    lfn = strlen (filename);
	    if (lfn > maxlfn)
		maxlfn = lfn;
	    nfile++;
	    }

	/* Condition */
	else if (strchr (*av, '=') != NULL || strchr (*av, '#') != NULL ||
		 strchr (*av, '>') != NULL || strchr (*av, '<') != NULL ) {
	    if (ncond >= maxncond) {
		maxncond = maxncond * 2;
		cond = (char **)realloc((void *)cond, maxncond*sizeof(void *));
		ccond = (char **)realloc((void *)ccond, maxncond*sizeof(void *));
		}
	    cond[ncond] = *av;
	    ccond[ncond] = strchr (*av, '=');
	    if (ccond[ncond] == NULL)
		ccond[ncond] = strchr (*av, '#');
	    if (ccond[ncond] == NULL)
		ccond[ncond] = strchr (*av, '>');
	    if (ccond[ncond] == NULL)
		ccond[ncond] = strchr (*av, '<');
	    ncond++;
	    }

	/* Otherwise, read command */
	else if (*(str = *av) == '-') {
	    char c;
	    while ((c = *++str))
	    switch (c) {

	    case 'a':	/* Sum values in each numeric column */
		sumcol++;
		break;

	    case 'b':	/* bar-separated input */
		cwhite = (char *) calloc (4,1);
		strcpy (cwhite, "bar");
		break;

	    case 'c':	/* Count entries in each column */
		countcol++;
		break;

	    case 'd':	/* Number of decimal places in f.p. output */
		if (ac < 2)
		    usage ();
		ndec = atoi (*++av);
		ac--;
		break;

	    case 'e':	/* Compute median of each numeric column */
		medcol++;
		break;

	    case 'f':	/* Range of values in column */
		rangecol++;
		break;

	    case 'g':	/* Compute absolute median of each numeric column */
		amedcol++;
		break;

	    case 'h':	/* Print Starbase tab table header */
		printhead++;
		break;

	    case 'i':	/* tab-separated input */
		cwhite = (char *) calloc (4,1);
		strcpy (cwhite, "tab");
		break;

	    case 'j':	/* Compute absolute mean of each numeric column */
		ameancol++;
		break;

	    case 'k':	/* Count columns on first line */
		counttok++;
		break;

	    case 'l':	/* Number of lines to append */
		if (ac < 2)
		    usage ();
		napp = atoi (*++av);
		ac--;
		break;

	    case 'm':	/* Compute mean of each numeric column */
		meancol++;
		break;

	    case 'n':	/* Number of lines to read */
		if (ac < 2)
		    usage ();
		nread = atoi (*++av);
		ac--;
		break;

	    case 'o': /* OR conditions insted of ANDing them */
		condand = 0;
		break;

	    case 'p': /* Print sum or mean only */
		printcol = 0;
		break;

	    case 'q':	/* Compute mean in quadrature of selected numeric columns */
		qmeancol = 1;
		break;

	    case 'r':	/* Range of lines to read */
		if (ac < 2)
		    usage ();
		if (*(av+1)[0] == '@') {
		    lfile = *++av + 1;
		    ac--;
		    }
		else if (isrange (*(av+1)) || isnum (*(av+1))) {
		    lranges = (char *) calloc (strlen(*av) + 1, 1);
		    strcpy (lranges, *++av);
		    ac--;
		    }
		break;

	    case 's':	/* Number of lines to skip */
		if (ac < 2)
		    usage ();
		nskip = atoi (*++av);
		ac--;
		break;

	    case 't':	/* Tab-separated output */
		tabout++;
		break;

	    case 'v':	/* More verbosity */
		verbose++;
		break;

	    case 'w':	/* Print file pathnames */
		listpath++;
		break;

	    case 'x':	/* Value to ignore */
		if (ac < 2)
		    usage ();
		badval = atof (*++av);
		isbadval++;
		ac--;
		break;

	    default:
		usage ();
		break;
	    }
	    }

	/* Operation */
	else if ((opi = iscolop (*av)) != NULL) {
	    if (nop >= maxnop) {
		maxnop0 = maxnop;
		maxnop = maxnop0 * 2;
		op1 = (char **)calloc (maxnop, sizeof (char *));
		cop1 = (char **)calloc (maxnop, sizeof (char));
		lc1 = (int *) calloc (maxnop, sizeof (int));
		for (i = 0; i < maxnop0; i++) {
		    op1[i] = op[i];
		    cop1[i] = cop[i];
		    lc1[i] = lc[i];
		    }
		free (op);
		free (cop);
		free (lc);
		op = op1;
		cop = cop1;
		lc = lc1;
		op1 = NULL;
		cop1 = NULL;
		lc1 = NULL;
		}
	    op[nop] = opi;
	    cop[nop] = *av;
	    lc[nop] = (int) (op[nop] - cop[nop]);
	    for (i = 0; i < 16; i++)
		sop1[i] = (char) 0;
	    for (i = 0; i < 16; i++)
		sop2[i] = (char) 0;
	    strncpy (sop1, cop[nop], lc[nop]);
	    strcpy (sop2, op[nop]+1);
	    if (isnum (sop1) && isnum (sop2)) {
		if (verbose)
		    printf (" Column %s %c %s\n", sop1, opi[0], sop2);
		nop++;
		}
	    else
		printf (" Constants %s, %s, or operation %c illegal\n",sop1,sop2,opi[0]);
	    }

	/* File to read */
	else
	    usage();
	}

    if (nfile <= 0) {
	fprintf (stderr, "GETCOL: no files specified\n");
	exit (1);
	}
    for (ifile = 0; ifile < nfile; ifile++)
	(void)ListFile (fn[ifile], ranges, lranges, lfile);

    free (lranges);
    free (ranges);
    return (0);
}

static void
usage ()

{
    fprintf (stderr,"%s\n", RevMsg);
    if (version)
	exit (-1);
    fprintf (stderr,"Extract specified columns from an ASCII table file\n");
    fprintf (stderr,"Usage: [-abcefghijkmopqtv][-d num][-l num][-n num][-r lines][-s num] filename [col] [cond] ...\n");
    fprintf(stderr," col: Number range (n1-n2,n3-n4...) or col.c1:c2\n");
    fprintf(stderr," col: Combination of columns n1[+_*/asmd][n2 or constant with .]\n");
    fprintf(stderr," cond: Condition for which to list [n1][=#><][n2 or constant with .]\n");
    fprintf(stderr," %%f.dx: C output format for last column specified\n");
    fprintf(stderr,"  -a: Sum selected numeric column(s)\n");
    fprintf(stderr,"  -b: Input columns are delimited by vertical bars\n");
    fprintf(stderr,"  -c: Add count of number of lines in each column at end\n");
    fprintf(stderr,"  -d num: Number of decimal places in f.p. output\n");
    fprintf(stderr,"  -e: Median values of selected numeric column(s)\n");
    fprintf(stderr,"  -f: Print range of values in selected column(s)\n");
    fprintf(stderr,"  -g: Median absolute values of selected column(s)\n");
    fprintf(stderr,"  -h: Print Starbase tab table header and # comments\n");
    fprintf(stderr,"  -i: Input is tab-separate table file\n");
    fprintf(stderr,"  -j: Print means of absolute values of selected column(s)\n");
    fprintf(stderr,"  -k: Print number of columns on first line\n");
    fprintf(stderr,"  -l num: Number of lines to add to each line\n");
    fprintf(stderr,"  -m: Print means of selected numeric column(s)\n");
    fprintf(stderr,"  -n num: Number of lines to read (default %d)\n", MAXLINES);
    fprintf(stderr,"  -o: OR conditions insted of ANDing them\n");
    fprintf(stderr,"  -p: Print only sum or mean, not individual values\n");
    fprintf(stderr,"  -q: Compute mean of selected columns added in quadrature\n");
    fprintf(stderr,"  -r: Range or @file of lines to read, if not all\n");
    fprintf(stderr,"  -s: Number of lines to skip\n");
    fprintf(stderr,"  -t: Starbase tab table output\n");
    fprintf(stderr,"  -v: Verbose\n");
    fprintf(stderr,"  -w: Print file pathnames\n");
    fprintf(stderr,"  -x num: Set value to ignore\n");
    exit (1);
}

static int
ListFile (filename, ranges, lranges, lfile)

char	*filename;	/* File name */
char	*ranges;	/* String with range of column numbers to list */
char	*lranges;	/* String with range of lines to list */
char	*lfile;		/* Name of file with lines to list */

{
    int i, j, il, ir, nbytes, ncol;
    char line[4096];
    char *nextline;
    char *lastchar;
    FILE *fd = NULL;
    FILE *lfd = NULL;
    struct Tokens tokens;  /* Token structure */
    struct Range *range = NULL;
    struct Range *lrange;
    int *iline;
    int nline = 0;
    int idnum;
    int iln;
    int nfdef = 9;
    double *sum;
    double *asum = NULL;
    double *colmin, *colmax, **med;
    double **amed = NULL;
    double *qmed = NULL;
    double qsum = 0.0;
    double qsum1;
    int *nsum;
    int *nent;
    int *hms;		/* Flag for hh:mm:ss or dd:mm:ss format */
    int *limset;	/* Flag for range initialization */
    int nlmax;
    double dtok, dnum;
    int nfind = 0;
    int ntok, nt, ltok,iop, ndtok, ndnum, nd;
    int *inum;
    int icond, itok;
    char tcond, *cstr, *cval, top;
    char colstr[16];
    char numstr[32], numstr1[32];
    double dcond, dval;
    int pass;
    int nchar, k;
    int iapp;
    int jcond, jval;
    char token[MAX_LTOK];
    char token1[MAX_LTOK];
    int lform;
    char *iform;
    char cform;
    int nq, nqsum = 0;

    nent = NULL;
    sum = NULL;
    inum = NULL;
    nsum = NULL;
    colmin = NULL;
    colmax = NULL;
    lrange = NULL;
    iline = NULL;
    hms = NULL;
    med = NULL;
    limset = NULL;

    if (verbose)
	printf ("\n%s\n", RevMsg);

    if (listpath)
	printf ("%s ", filename);

    if (nread < 1)
	nread = MAXLINES;

    /* Make list of line numbers to read from list or range on command line */
    if (lranges != NULL) {
	lrange = RangeInit (lranges, nfdef);
	nline = rgetn (lrange);
	nbytes = nline * sizeof (int);
	if (!(iline = (int *) calloc (nline+1, sizeof(int))) ) {
	    fprintf (stderr, "Could not calloc %d bytes for iline\n", nbytes);
	    return (-1);
	    }
	for (i = 0; i < nline; i++)
	    iline[i] = rgeti4 (lrange);
	qsort (iline, nline, sizeof(int), intcompare);
	}

    /* Make list of line numbers to read from file specified on command line */
    if (lfile != NULL) {
	if (!(lfd = fopen (lfile, "r")))
            return (-1);
	nlmax = 99;
	nline = 0;
	nbytes = nlmax * sizeof(int);
	if (!(iline = (int *) calloc (nlmax, sizeof(int))) ) {
	    fprintf (stderr, "Could not calloc %d bytes for iline\n", nbytes);
	    fclose (lfd);
	    return (-1);
	    }
	il = 0;
	nextline = line;
	for (ir = 0; ir < nread; ir++) {
	    if (fgets (nextline, 1023, lfd) == NULL)
		break;

	    /* Skip lines with comments */
	    if (nextline[0] == '#') {
		if (!printhead) {
		    while (line[0] == '#') {
			if (fgets (nextline, 1023, lfd) == NULL)
			    break;
			}
		    }
		else
		    continue;
		}

	    /* Drop linefeeds */
	    lastchar = nextline + strlen(nextline) - 1;
	    if (*lastchar < 32)
		*lastchar = (char) 0;

	    /* Add lines with escaped linefeeds */
	    lastchar = nextline + strlen(nextline) - 1;
	    if (*lastchar == (char) 92) {
		nextline = lastchar;
		continue;
		}
	    else
		nextline = line;

	    ntok = setoken (&tokens, line, cwhite);
	    nt = 0;
	    il++;
	    if (il > nlmax) {
		nlmax = nlmax + 100;
		nbytes = nlmax * sizeof(int);
		if (!(iline = (int *) realloc ((void *) iline, nbytes))) {
		    fprintf (stderr, "Could not realloc %d bytes for iline\n",
			     nbytes);
		    fclose (lfd);
		    return (-1);
		    }
		}
	    for (i = 0; i < ntok; i++) {
		if (getoken (&tokens, i+1, token, MAX_LTOK)) {
		    iline[il] = atoi (token);
		    if (iline[il] > 0) {
			nline++;
			break;
			}
		    }
		}
	    }
	fclose (lfd);
	qsort (iline, nline, sizeof(int), intcompare);
	}

    /* Open input file */
    if (!strcmp (filename, "stdin"))
	fd = stdin;
    else if (!(fd = fopen (filename, "r"))) {
	if (verbose)
	    fprintf (stderr, "*** Cannot read file %s\n", filename);
        return (-1);
	}

    /* Skip lines into input file */
    if (nskip > 0) {
	for (i = 0; i < nskip; i++) {
	    if (fgets (line, 1023, fd) == NULL)
		break;
	    if (!printhead) {
		while (line[0] == '#') {
		    if (fgets (line, 1023, fd) == NULL)
			break;
		    }
		}
	    }
	}

    /* Print entire selected lines */
    if (ranges == NULL && nop == 0) {
	iln = 0;
	il = 0;
	iapp = 0;
	nextline = line;
	for (ir = 0; ir < nread; ir++) {
	    if (fgets (nextline, 1023, fd) == NULL)
		break;


	    /* Skip lines with comments */
	    if (nextline[0] == '#') {
		if (!printhead) {
		    while (nextline[0] == '#') {
			if (fgets (nextline, 1023, lfd) == NULL)
			    break;
			}
		    }
		else
		    continue;
		}

	    /* Add lines with escaped linefeeds */
	    lastchar = nextline + strlen(nextline) - 1;
	    if (*lastchar == (char) 92) {
		nextline = lastchar;
		continue;
		}
	    else if (iapp++ < napp) {
		*lastchar = ' ';
		nextline = lastchar + 1;
		continue;
		}
	    else {
		iapp = 0;
		nextline = line;
		}

	    il++;

	    /* Skip if line is not on list, if there is one */
	    if (iline != NULL) {
		if (il+1 < iline[iln])
		    continue;
		else if (il+1 > iline[nline-1])
		    break;
		else
		    iln++;
		}

	    /* Drop control character at end of string */
	    lastchar = line + strlen(line) - 1;
	    if (*lastchar < 32)
		*lastchar = (char) 0;

	    /* Drop second control character at end of string */
	    lastchar = line + strlen(line) - 1;
	    if (*lastchar < 32)
		*lastchar = (char) 0;

	    /* Echo line if it is a comment */
	    if (line[0] == '#' && printhead) {
		printf ("%s\n", line);
		continue;
		}

	    /* Check conditions */
	    ntok = setoken (&tokens, line, cwhite);
	    if (counttok) {
		printf ("%d", ntok);
		if (verbose)
		    printf (" columns in %s", filename);
		else
		    printf ("\n");
		return (0);
		}
	    pass = 0;
	    if (ncond > 0) {
		for (icond = 0; icond < ncond; icond++) {
		    if (condand)
			pass = 0;
	
		    /* Extract test value from comparison string */
		    cstr = ccond[icond]+1;
		    if (strchr (cstr, ':')) {
			dnum = str2dec (cstr);
			num2str (numstr, dnum, 0, 7);
			cstr = numstr;
			}
		    else if (isnum (cstr) == 1) {
			itok = atoi (cstr);
			getoken (&tokens, itok, token1, MAX_LTOK);
			cstr = token1;
			}
		    strfix (cstr, 0, 1);
	
		    /* Read comparison value from header */
		    tcond = *ccond[icond];
		    *ccond[icond] = (char) 0;
		    itok = atoi (cond[icond]);
		    *ccond[icond] = tcond;
		    getoken (&tokens, itok, token, MAX_LTOK);
		    cval = token;
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
			if (tcond == '=' && dval == dcond)
			    pass = 1;
			if (tcond == '#' && dval != dcond)
			    pass = 1;
			if (tcond == '>' && dval > dcond)
			    pass = 1;
			if (tcond == '<' && dval < dcond)
			    pass = 1;
			}
	
		    /* Compare integers */
		    else if (isnum (cstr) == 1 && isnum (cval)) {
			jcond = (int) atof (cstr);
			jval = (int) atof (cval);
			if (tcond == '=' && jval == jcond)
			    pass = 1;
			if (tcond == '#' && jval != jcond)
			    pass = 1;
			if (tcond == '>' && jval > jcond)
			    pass = 1;
			if (tcond == '<' && jval < jcond)
			    pass = 1;
			}
	
		    /* Compare strings (only equal or not equal */
		    else {
			if (tcond == '=' && !strcmp (cstr, cval))
			    pass = 1;
			if (tcond == '#' && strcmp (cstr, cval))
			    pass = 1;
			}
		    if (condand && !pass)
			break;
		    }
		if (pass)
		    printf ("%s\n", line);
		}
	    else
		printf ("%s\n", line);
	    }
	}

    /* Find columns specified by number */
    else {
	if (ranges || nop > 0) {
	    if (ranges) {
		range = RangeInit (ranges, nfdef);
		nfind = rgetn (range);
		}
	    else
		nfind = 0;
	ncol = nfind + nop;
	nbytes = ncol * sizeof (double);
	if (!(sum = (double *) calloc (ncol, sizeof (double))) ) {
	    fprintf (stderr, "Could not calloc %d bytes for sum\n", nbytes);
	    if (fd != stdin) fclose (fd);
	    return (-1);
	    }
	if (!(asum = (double *) calloc (ncol, sizeof (double))) ) {
	    fprintf (stderr, "Could not calloc %d bytes for asum\n", nbytes);
	    if (fd != stdin) fclose (fd);
	    return (-1);
	    }
	if (!(colmin = (double *) calloc (ncol, sizeof (double))) ) {
	    fprintf (stderr, "Could not calloc %d bytes for colmin\n", nbytes);
	    if (fd != stdin) fclose (fd);
	    return (-1);
	    }
	if (!(colmax = (double *) calloc (ncol, sizeof (double))) ) {
	    fprintf (stderr, "Could not calloc %d bytes for colmax\n", nbytes);
	    if (fd != stdin) fclose (fd);
	    return (-1);
	    }
	if (!(med = (double **) calloc (ncol, sizeof (double *))) ) {
	    fprintf (stderr, "Could not calloc %d bytes for med\n", nbytes);
	    if (fd != stdin) fclose (fd);
	    return (-1);
	    }
	if (!(amed = (double **) calloc (ncol, sizeof (double *))) ) {
	    fprintf (stderr, "Could not calloc %d bytes for amed\n", nbytes);
	    if (fd != stdin) fclose (fd);
	    return (-1);
	    }
	else {
	    nbytes = nread * sizeof (double);
	    if (!(qmed = calloc (nread, sizeof(double)))) {
		fprintf (stderr, "Could not calloc %d bytes for qmed\n", nbytes);
		if (fd != stdin) fclose (fd);
		return (-1);
		}
	    for (i = 0; i < ncol; i++) {
		if (!(med[i] = calloc (nread, sizeof(double)))) {
		    fprintf (stderr, "Could not calloc %d bytes for med%d\n",
			     nbytes, i);
		    if (fd != stdin) fclose (fd);
		    return (-1);
		    }
		if (!(amed[i] = calloc (nread, sizeof(double)))) {
		    fprintf (stderr, "Could not calloc %d bytes for amed%d\n",
			     nbytes, i);
		    if (fd != stdin) fclose (fd);
		    return (-1);
		    }
		}
	    }
	nbytes = ncol * sizeof (int);
	if (!(nsum = (int *) calloc (ncol, sizeof(int))) ) {
	    fprintf (stderr, "Could not calloc %d bytes for nsum\n", nbytes);
	    if (fd != stdin) fclose (fd);
	    return (-1);
	    }
	if (!(hms = (int *) calloc (ncol, sizeof(int))) ) {
	    fprintf (stderr, "Could not calloc %d bytes for hms\n", nbytes);
	    if (fd != stdin) fclose (fd);
	    return (-1);
	    }
	if (!(limset = (int *) calloc (ncol, sizeof(int))) ) {
	    fprintf (stderr, "Could not calloc %d bytes for limset\n", nbytes);
	    if (fd != stdin) fclose (fd);
	    return (-1);
	    }
	if (!(nent = (int *) calloc (ncol, sizeof(int))) ) {
	    fprintf (stderr, "Could not calloc %d bytes for nent\n", nbytes);
	    if (fd != stdin) fclose (fd);
	    return (-1);
	    }
	if (!(inum = (int *) calloc (ncol, sizeof(int))) ) {
	    fprintf (stderr, "Could not calloc %d bytes for inum\n", nbytes);
	    if (fd != stdin) fclose (fd);
	    return (-1);
	    }
	else {
	    for (i = 0; i < nfind; i++)
		inum[i] = rgeti4 (range);
	    }
	    }
	iln = 0;
	iapp = 0;
	nextline = line;
	for (il = 0; il < nread; il++) {
	    if (fgets (nextline, 1023, fd) == NULL)
		break;

	    /* Ignore line if it is a comment and printhead flag is not set */
	    if (line[0] == '#' && !printhead) {
		while (line[0] == '#') {
		    if (fgets (nextline, 1023, fd) == NULL)
			break;
		    }
		}

	    /* Clear control character at end of string */
	    lastchar = line + strlen(line) - 1;
	    if (*lastchar < 32)
		*lastchar = (char) 0;

	    /* Clear second control character at end of string */
	    lastchar = line + strlen(line) - 1;
	    if (*lastchar < 32)
		*lastchar = (char) 0;

	    /* Add lines with escaped linefeeds */
	    lastchar = nextline + strlen(nextline) - 1;
	    if (*lastchar == (char) 92) {
		nextline = lastchar;
		continue;
		}
	    else if (iapp++ < napp) {
		*++lastchar = ' ';
		nextline = lastchar + 1;
		continue;
		}
	    else
		nextline = line;
		iapp = 0;

	    /* Skip if line is not on list, if there is one */
	    if (iline != NULL) {
		if (il+1 < iline[iln])
		    continue;
		else if (il > iline[nline-1])
		    break;
		else {
		    iln++;
		    if (iln > nline)
			break;
		    }
		}

	    /* Echo line if it is a comment */
	    if (line[0] == '#') {
		if (printhead)
		    printf ("%s\n", line);
		continue;
		}

	    ntok = setoken (&tokens, line, cwhite);
	    if (counttok) {
		printf ("%d", ntok);
		if (verbose)
		    printf (" columns in %s", filename);
		else
		    printf ("\n");
		return (0);
		}

	    /* Check conditions */
	    pass = 0;
	    if (ncond > 0) {
		for (icond = 0; icond < ncond; icond++) {
		    if (condand)
			pass = 0;
	
		    /* Extract test value from comparison string */
		    cstr = ccond[icond]+1;
		    if (strchr (cstr, ':')) {
			dnum = str2dec (cstr);
			num2str (numstr, dnum, 0, 7);
			cstr = numstr;
			}
		    else if (isnum (cstr) == 1) {
			itok = atoi (cstr);
			getoken (&tokens, itok, token1, MAX_LTOK);
			cstr = token1;
			}
		    strfix (cstr, 0, 1);
	
		    /* Read comparison value from header */
		    tcond = *ccond[icond];
		    *ccond[icond] = (char) 0;
		    itok = atoi (cond[icond]);
		    *ccond[icond] = tcond;
		    getoken (&tokens, itok, token, MAX_LTOK);
		    cval = token;
		    if (strchr (cval, ':')) {
			dnum = str2dec (cval);
			num2str (numstr, dnum, 0, 7);
			cval = numstr;
			}
		    strfix (cval, 0, 1);
	
		    /* Compare floating point numbers */
		    if (isnum (cstr) == 2 && isnum (cval)) {
			dcond = atof (cstr);
			dval = atof (cval);
			if (tcond == '=' && dval == dcond)
			    pass = 1;
			if (tcond == '#' && dval != dcond)
			    pass = 1;
			if (tcond == '>' && dval > dcond)
			    pass = 1;
			if (tcond == '<' && dval < dcond)
			    pass = 1;
			}
	
		    /* Compare integers */
		    else if (isnum (cstr) == 1 && isnum (cval)) {
			jcond = atoi (cstr);
			jval = atoi (cval);
			if (tcond == '=' && jval == jcond)
			    pass = 1;
			if (tcond == '#' && jval != jcond)
			    pass = 1;
			if (tcond == '>' && jval > jcond)
			    pass = 1;
			if (tcond == '<' && jval < jcond)
			    pass = 1;
			}
	
		    /* Compare strings (only equal or not equal */
		    else {
			if (tcond == '=' && !strcmp (cstr, cval))
			    pass = 1;
			if (tcond == '#' && strcmp (cstr, cval))
			    pass = 1;
			}
		    if (condand && !pass)
			break;
		    }
		if (!pass)
		    continue;
		}

	    nt = 0;
	    if (il == 0 && printhead && tabout) {

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
			    printf ("condition  %s\n", cond[icond]);
			else
			    printf ("condition  or %s\n", cond[icond]);
			}
		    }

		for (i = 0; i < nfind; i++) {
		    if (getoken (&tokens, inum[i], token, MAX_LTOK)) {
			ltok = strlen (token);
			printf ("%03d", inum[i]);
			for (j = 3; j < ltok; j++)
			    printf (" ");
			}
		    else
			printf ("%03d", inum[i]);
		    printf ("	");
		    }
		printf ("\n");
		for (i = 0; i < nfind; i++) {
		    if (getoken (&tokens, inum[i], token, MAX_LTOK)) {
			ltok = strlen (token);
			for (j = 0; j < ltok; j++)
			    printf ("-");
			}
		    else
			printf ("---");
		    printf ("	");
		    }
		printf ("\n");
		}

	    /* Print requested columns */
	    qsum1 = 0;
	    nq = 0;
	    for (i = 0; i < nfind; i++) {
		if (getoken (&tokens, inum[i], token, MAX_LTOK)) {

		    /* Get substring of column, if requested */
		    if (frstchar[i] > 0) {
			ltok = strlen (token);
			if (lastchar[i] > 0)
			    nchar = lastchar[i] - frstchar[i] + 1;
			else
			    nchar = ltok - frstchar[i] + 1;
			k = frstchar[i];
			for (j = 0; j < nchar; j++)
			    token[j] = token[k++];
			for (j = nchar; j < ltok; j++);
			    token[j] = (char) 0;
			}
		    strfix (token, 0, 1);
		    if (isnum (token)) {
			dval = atof (token);
			hms[i] = 0;
			idnum = 1;
			}
		    else if (strchr (token, ':')) {
			dval = str2dec (token);
			hms[i] = 1;
			idnum = 1;
			}
		    else {
			dval = 0.0;
			idnum = 0;
			}
		    if (idnum) {
			if (!isbadval || (isbadval && dval != badval)) {
			    qsum1 = qsum1 + dval * dval;
			    nq++;
			    sum[i] = sum[i] + dval;
			    asum[i] = asum[i] + fabs (dval);
			    if (!limset[i]) {
				colmin[i] = dval;
				colmax[i] = dval;
				limset[i]++;
				}
			    else if (dval < colmin[i])
				colmin[i] = dval;
			    else if (dval > colmax[i])
				colmax[i] = dval;
			    med[i][nsum[i]] = dval;
			    amed[i][il] = fabs (dval);
			    nsum[i]++;
			    }
			}
		    if (printcol) {
			if (i > 0) {
			    if (tabout)
				printf ("	");
			    else
				printf (" ");
			    }
			iform = format[inum[i]];
			if (inum[i] > tokens.ntok || inum[i] < 1)
			    printf ("___");
			else if (iform) {
			    lform = strlen (iform);
			    cform = iform[lform-1];
			    if (cform == 'f' || cform == 'g')
				printf (iform, atof(token));
			    else if (cform == 'd')
				printf (iform, atoi(token));
			    else
				printf (iform, token);
			    }
			else if (ndec > -1 && isnum (token) == 2) {
			    num2str (numstr, atof (token), 0, ndec);
			    printf ("%s", numstr);
			    }
			else
			    printf ("%s", token);
			}
		    nt++;
		    nent[i]++;
		    }
		if (nq > 1) {
		    qmed[nqsum] = sqrt (qsum1);
		    nqsum++;
		    qsum = qsum + sqrt (qsum1);
		    }
		}

	    /* Print columns being operated on */
	    for (iop = 0; iop < nop; iop++) {
		if (i > 0 || nfind > 0) {
		    if (printcol) {
			if (tabout)
			    printf ("	");
			else
			    printf (" ");
			}
		    }

		/* Extract first value from input line */
		for (j = 0; j< 16; j++)
		    colstr[j] = (char) 0;
		strncpy (colstr, cop[iop], lc[iop]);
		if (isnum (colstr) > 1) {
		    dnum = atof (colstr);
		    ndnum = numdec (colstr);
		    }
		else {
		    itok = atoi (colstr);
		    if (getoken (&tokens, itok, token, MAX_LTOK)) {
			dnum = atof (token);
			ndnum = numdec (token);
			}
		    else {
			printf ("___");
			continue;
			}
		    }

		/* Extract operator from input line */
		top = op[iop][0];

		/* Extract second value from input line and operate on it */
		itok = atoi (op[iop]+1);
		if (getoken (&tokens, itok, token, MAX_LTOK)) {
		    dtok = atof (token);
		    ndtok = numdec (token);
		    nd = ndtok;
		    if (ndec > -1)
			nd = ndec;
		    if (ndnum > nd)
			nd = ndnum;
		    if (top == '+' || top == 'a') {
			num2str (numstr, dnum+dtok, 0, nd);
			dval = dnum + dtok;
			}
		    else if (top == '_' || top == 's') {
			num2str (numstr, dnum-dtok, 0, nd);
			dval = dnum - dtok;
			}
		    else if (top == '*' || top == 'm') {
			num2str (numstr, dnum*dtok, 0, nd);
			dval = dnum * dtok;
			}
		    else if (top == '/' || top == 'd') {
			num2str (numstr, dnum/dtok, 0, nd);
			dval = dnum / dtok;
			}
		    else
			strcpy (numstr,"___");
		    if (printcol)
			printf ("%s", numstr);
		    if ((!isbadval || (isbadval && dval != badval)) &&
			strcmp (numstr,"___")) {
			qsum1 = qsum1 + dval * dval;
			nq++;
			sum[i] = sum[i] + dval;
			asum[i] = asum[i] + fabs (dval);
			if (!limset[i]) {
			    colmin[i] = dval;
			    colmax[i] = dval;
			    limset[i]++;
			    }
			else if (dval < colmin[i])
			    colmin[i] = dval;
			else if (dval > colmax[i])
			    colmax[i] = dval;
			med[i][nsum[i]] = dval;
			amed[i][il] = fabs (dval);
			nsum[i]++;
			}
		    }
		nt++;
		if (nq > 1) {
		    qmed[nqsum] = sqrt (qsum1);
		    nqsum++;
		    qsum = qsum + sqrt (qsum1);
		    }
		}
	    if (nt > 0 && printcol)
		printf ("\n");
	    }
        }

    /* Print sums of values in numeric columns */
    ncol = nfind + nop;
    if (sumcol) {
	for (i = 0; i < ncol; i++) {
	    if (nsum[i] > 0) {
		if (i < ncol-1)
		    printf ("%f ", sum[i]);
		else
		    printf ("%f", sum[i]);
		}
	    else if (i < ncol-1)
		printf ("___ ");
	    else
		printf ("___");
	    }
	if (ncol > 0)
	    printf ("\n");
	}

    /* Print means of absolute values in numeric columns */
    if (ameancol) {
	for (i = 0; i < ncol; i++) {
	    if (nsum[i] > 0) {
		dval = asum[i] / (double)nsum[i];
		if (hms[i]) {
		    if (ndec > -1)
			dec2str (numstr, 32, dval, ndec);
		    else
			dec2str (numstr, 32, dval, 3);
		    }
		else if (ndec > -1)
		    num2str (numstr, dval, 0, ndec);
		else {
		    sprintf (numstr, "%f", dval);
		    strfix (numstr, 0, 1);
		    }
		if (i < ncol-1)
		    printf ("%s ", numstr);
		else
		    printf ("%s", numstr);
		}
	    else if (i < ncol-1)
		printf ("___ ");
	    else
		printf ("___");
	    }
	if (countcol) {
	    for (i = 0; i < ncol; i++) {
		if (nent[i] > 0)
		    printf (" %d", nent[i]);
		}
	    }
	if (lranges != NULL)
	    printf (" %s", lranges);
	if (ncol > 0)
	    printf ("\n");
	}

    /* Print means of values in numeric columns */
    if (meancol) {
	for (i = 0; i < ncol; i++) {
	    if (nsum[i] > 0) {
		dval = sum[i] / (double)nsum[i];
		if (hms[i]) {
		    if (ndec > -1)
			dec2str (numstr, 32, dval, ndec);
		    else
			dec2str (numstr, 32, dval, 3);
		    }
		else if (ndec > -1)
		    num2str (numstr, dval, 0, ndec);
		else {
		    sprintf (numstr, "%f", dval);
		    strfix (numstr, 0, 1);
		    }
		if (i < ncol-1)
		    printf ("%s ", numstr);
		else
		    printf ("%s ", numstr);
		}
	    else if (i < ncol-1)
		printf ("___ ");
	    else
		printf ("___");
	    }
	if (countcol) {
	    for (i = 0; i < ncol; i++) {
		if (nent[i] > 0)
		    printf (" %d", nent[i]);
		}
	    }
	if (lranges != NULL)
	    printf (" %s", lranges);
	if (ncol > 0)
	    printf ("\n");
	}

    /* Print medians of absolute values in numeric columns */
    if (amedcol) {
	for (i = 0; i < ncol; i++) {
	    if (nsum[i] > 0) {
		dval = median (amed[i], nsum[i]);
		if (hms[i]) {
		    if (ndec > -1)
			dec2str (numstr, 32, dval, ndec);
		    else
			dec2str (numstr, 32, dval, 3);
		    }
		else if (ndec > -1)
		    num2str (numstr, dval, 0, ndec);
		else {
		    sprintf (numstr, "%f", dval);
		    strfix (numstr, 0, 1);
		    }
		if (i < ncol-1)
		    printf ("%s ", numstr);
		else
		    printf ("%s", numstr);
		}
	    else if (i < ncol-1)
		printf ("___ ");
	    else
		printf ("___");
	    }
	if (countcol) {
	    for (i = 0; i < ncol; i++) {
		if (nent[i] > 0)
		    printf (" %d", nent[i]);
		}
	    }
	if (lranges != NULL)
	    printf (" %s", lranges);
	if (ncol > 0)
	    printf ("\n");
	}

    /* Print medians of values in numeric columns */
    if (medcol) {
	for (i = 0; i < ncol; i++) {
	    if (nsum[i] > 0) {
		dval = median (med[i], nsum[i]);
		if (hms[i]) {
		    if (ndec > -1)
			dec2str (numstr, 32, dval, ndec);
		    else
			dec2str (numstr, 32, dval, 3);
		    }
		else if (ndec > -1)
		    num2str (numstr, dval, 0, ndec);
		else {
		    sprintf (numstr, "%f", dval);
		    strfix (numstr, 0, 1);
		    }
		if (i < ncol-1)
		    printf ("%s ", numstr);
		else
		    printf ("%s", numstr);
		}
	    else if (i < ncol-1)
		printf ("___ ");
	    else
		printf ("___");
	    }
	if (countcol) {
	    for (i = 0; i < ncol; i++) {
		if (nent[i] > 0)
		    printf (" %d", nent[i]);
		}
	    }
	if (lranges != NULL)
	    printf (" %s", lranges);
	if (ncol > 0)
	    printf ("\n");
	}

    /* Print ranges of values in numeric columns */
    if (rangecol) {
	for (i = 0; i < ncol; i++) {
	    if (hms[i]) {
		if (ndec > -1)
		    dec2str (numstr, 32, colmin[i], ndec);
		else
		    dec2str (numstr, 32, colmin[i], 3);
		}
	    else if (ndec > -1)
		num2str (numstr, colmin[i], 0, ndec);
	    else
		sprintf (numstr, "%f", colmin[i]);
	    strfix (numstr, 0, 1);
	    printf ("%s-", numstr);
	    if (hms[i]) {
		if (ndec > -1)
		    dec2str (numstr, 32, colmax[i], ndec);
		else
		    dec2str (numstr, 32, colmax[i], 3);
		}
	    else if (ndec > -1)
		num2str (numstr, colmax[i], 0, ndec);
	    else
		sprintf (numstr, "%f", colmax[i]);
	    strfix (numstr, 0, 1);
	    if (i < ncol-1)
		printf ("%s ", numstr);
	    else
		printf ("%s", numstr);
	    }
	if (lranges != NULL)
	    printf (" %s", lranges);
	if (ncol > 0)
	    printf ("\n");
	}

    /* Print count for each column */
    if (countcol && !meancol && !qmeancol && !ameancol && !medcol) {
	for (i = 0; i < ncol; i++) {
	    if (nent[i] > 0) {
		if (i < ncol-1)
		    printf ("%d ", nent[i]);
		else
		    printf ("%d", nent[i]);
		}
	    }
	if (ncol > 0)
	    printf ("\n");
	}

    /* Print mean of all numeric columns added in quadrature */
    if (qmeancol) {
	if (nqsum > 0) {
	    dval = qsum / (double)nqsum;
	    sprintf (numstr, "%f", dval);
	    strfix (numstr, 0, 1);
	    printf ("%s", numstr);
	    dval = median (qmed, nqsum);
	    sprintf (numstr, "%f", dval);
	    strfix (numstr, 0, 1);
	    printf (" %s\n", numstr);
	    }
	if (countcol) {
	    for (i = 0; i < ncol; i++) {
		if (nent[i] > 0)
		    printf (" %d", nent[i]);
		}
	    }
	}

    /* Free memory used for search results */
    if (inum) free ((char *)inum);
    if (nsum) free ((char *)nsum);
    if (nent) free ((char *)nent);
    if (sum) free ((char *)sum);

    if (fd != stdin) fclose (fd);
    return (ncol);
}

static int
intcompare (i, j)

int *i, *j;
{
    if (*i > *j)
	return (1);
    if (*i < *j)
	return (-1);
    return (0);
}


static char *
iscolop (string)

char *string;
{
    char *c;

    /* Check for presence of operation */
    if ((c = strchr (string, '+')) != NULL ||
	(c = strchr (string, '_')) != NULL ||
	(c = strchr (string, '*')) != NULL ||
	(c = strchr (string, '/')) != NULL ||
	(c = strchr (string, 'a')) != NULL ||
	(c = strchr (string, 's')) != NULL ||
	(c = strchr (string, 'm')) != NULL ||
	(c = strchr (string, 'd')) != NULL) {

	/* Check to see if string is a file */
	if (access(string,0) && strcmp (string, "stdin"))
	    return (c);
	else
	    return (NULL);
	}
    else
	return (NULL);
}

static double
median (x, n)

int	n;
double	*x;
{
    int rhs, lhs;
    int NComp();

    if (n <= 0)
	return (0.0);
   else if (n == 1)
	return (x[0]);

    qsort (x, n, sizeof (double), NComp);

    lhs = (n - 1) / 2;
    rhs = n / 2;

    if (lhs == rhs)
	return (x[lhs]);
    else
	return ((x[lhs] + x[rhs]) / 2.0);
}

static int
iscol (string)

char *string;   /* Character string */
{
    int lstr, i, nd;
    char cstr;

    /* Return 0 if string is NULL */
    if (string == NULL)
        return (0);

    lstr = strlen (string);
    nd = 0;

    /* Remove trailing spaces */
    while (string[lstr-1] == ' ')
        lstr--;

    /* Column strings contain 0123456789 and . or : for subranges */
    for (i = 0; i < lstr; i++) {
        cstr = string[i];
        if (cstr == '\n')
            break;

        /* Ignore leading spaces */
        if (cstr == ' ' && nd == 0)
            continue;

        if ((cstr < 48 || cstr > 57) &&
            cstr != ':' && cstr != '.' )
            return (0);
	else if (cstr >= 47 && cstr <= 57)
	    nd++;
        }
    if (nd > 0)
        return (1);
    else
        return (0);
}


int
NComp (pd1, pd2)

void *pd1, *pd2;
{
    double d1 = *((double *)pd1);
    double d2 = *((double *)pd2);

    if (d1 > d2)
	return (1);
    else if (d1 < d2)
	return (-1);
    else
	return (0);
}

/* Nov  2 1999	New program
 * Nov  3 1999	Add option to read from stdin as input filename
 * Dec  1 1999	Add options to print counts, means, and sums of columns
 * Dec 14 1999	Add option for tab-separated output
 *
 * Jan  7 2000	Add option to list range of lines or filed list of lines
 * Jan 26 2000	Add documentation of entry count and tab output
 * Jan 26 2000	Add option to print tab table header
 * Feb 11 2000	Fix reallocation of range variables
 * Mar 20 2000	Add conditional line printing
 * Apr  4 2000	Add option to operate on keyword values
 * Apr  5 2000	Simply echo lines starting with #
 * Jul 21 2000	Link lines with escaped linefeeds at end to next line
 * Aug  8 2000	Add -l option to append lines without backslashes
 * Oct 23 2000	Declare ListFile(); fix column arithmetic command line options
 * Nov 20 2000	Clean up code using lint
 * Dec 11 2000	Include fitshead.h for string search
 *
 * Jan 17 2001	Add -d option to set number of output decimal places
 * Jan 17 2001	Add a, s, m, d for add, subtract, multiply, divide const or col
 * Mar 19 2001	Drop type declarations from intcompare argument list
 * Jun 18 2001	Add maximum length of returned string to getoken()
 * Jul 17 2001	Check operations for stdin as well as file
 * Oct 10 2001	Add sum, mean, sigma for hh:mm:ss and dd:mm:ss entries
 * Oct 10 2001	Add -p option to print only sum, mean, sigma, not entries
 * Oct 11 2001	Add -f option to print range of values in selected columns
 * Oct 16 2001	Add -e option to compute medians
 * Oct 16 2001	Ignore non-numeric values for sums, means, and medians
 * Nov 13 2001	Add option to specifiy characters of column to use
 * Dec 28 2001	Clear second control character at the end of a line (CR/LF)
 *
 * Apr  9 2002	NComp() cannot be static as it is passed to qsort()
 * Apr 10 2002	Add option to print means and medians of absolute values
 * Apr 11 2002	Add option to print mean of selected columns added in quadrature
 * Apr 12 2002	Add option to print median of selected columns added in quadrature
 * Apr 12 2002	Fix bug in computing median of filtered file
 * Apr 12 2002	Add -x option to set ignorable value
 * Jun 19 2002	Fix bug that could read files as letter operations
 * Jul 18 2002	Read multiple files; add option to print pathname
 * Jul 19 2002	Ignore commented lines completely if -z
 *
 * Jun 10 2003	Print default number of lines in command list
 * Dec  9 2003	Allow operation on column if only argument
 * Dec 10 2003	Fix number of decimal places in output
 * Dec 15 2003	Fix column operation option
 * Dec 17 2003	Drop undocumented -z option; print # comments if -h is set
 *
 * Apr 15 2004	Add % output format option
 * Apr 15 2004	Avoid removing trailing zeroes from exponents
 * Jul 14 2004	Fix bug in online help message
 * Aug 30 2004
 * Nov 10 2004	Add -column to include rest of line to newline
 *
 * Jul 15 2005	Drop unused variables
 * Jul 15 2005	Use pointer to tokens in calls to getoken()
 * Dec  9 2005	Add line count to same line as mean or median
 * Dec  9 2005	Use decimal place setting for means, medians, and ranges, too
 * Dec 13 2005	If only scanning a range of lines, print the range
 * Dec 15 2005	Clean up line range code
 *
 * Jan 18 2006	Count tokens if no columns specified, too
 * Jun 21 2006	Increase maximum number of tokens from 256 to 1024
 * Jun 21 2006	Increase maximum line length from 1024 to 4096
 * Jun 21 2006	Clean up code
 * Jun 29 2006	Rename strclean() strfix() and move to hget.c
 *
 * Jan 10 2007	Declare revmsg static, not const
 * Jan 10 2007	Include wcs.h
 * Feb  6 2006	Fix bug setting conditions and improve usage()
 * Feb  7 2007	Fix bug setting up column arithmetic
 *
 * Jul  1 2008	Fix bug so columns can be compared if both are integers
 * Sep 25 2009	Fix error message about illegal operation
 */
