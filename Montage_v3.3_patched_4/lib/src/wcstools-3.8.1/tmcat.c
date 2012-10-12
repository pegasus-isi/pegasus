/*** File tmcat.c
 *** July 1, 2003
 *** By Doug Mink, SAO

   Copyright (C) 2006 
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

#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>

#define MAXTOKENS 100	/* Maximum number of tokens to parse */
#define MAXWHITE 20	/* Maximum number of whitespace characters */

struct Tokens {
    char *line;         /* Line which has been parsed */
    int lline;          /* Number of characters in line */
    int ntok;           /* Number of tokens on line */
    int nwhite;         /* Number of whitespace characters */
    char white[MAXWHITE];       /* Whitespace (separator) characters */
    char *tok1[MAXTOKENS];      /* Pointers to start of tokens */
    int ltok[MAXTOKENS];        /* Lengths of tokens */
    int itok;           /* Current token number */
};
int setoken();          /* Tokenize a string for easy decoding */
int nextoken();         /* Get next token from tokenized string */
int getoken();          /* Get specified token from tokenized string */

main (ac, av)
int ac;
char **av;
{
    double ra, dec, spd, mj, mh, mk;
    int i, j;
    char root[] = "/data/astrocat/tmc1";
    char dir[256];
    char file[256], file0[256];
    char line[512];
    char token[64];
    char f1[8], f2[8];
    char id[32];
    int use_src;
    int dup_src;
    int n = 0;
    int ni = 0;
    int nt = 0;
    int nz = 0;
    int z;
    FILE *outfile = NULL;
    FILE *sortfile = NULL;
    struct Tokens tokens;	/* Token structure */

    for (j = 0; j < 180; j = j + 1) {
	sprintf (dir,"%s/%03d", root, j);
	if (access (dir, F_OK&W_OK)) {
	    if (mkdir (dir, 00777))
		fprintf (stderr, "TMCAT: Cannot make directory %s\n", dir);
	    }
	}
    /* Initialize file name to all zeroes */
    for (i = 0; i < 256; i++)
	file[i] = (char) 0;
    strcpy (file0, file);

    /* Read through standard input until no more data can be read */
    while (fgets (line, 512, stdin)) {
	setoken (&tokens, line, "bar");

	/*  Compute South Polar Distance from declination */
	getoken (&tokens, 2, token, 16);
	dec = atof (token);
	spd = dec + 90.0;
	ni = ni + 1;

	/* Compute zone number and skip if out of range */
	z = (int) spd;
	if (z > 180)
	    continue;

	/* Set output directory name */
	sprintf (dir,"%s/%03d", root, z);

	/* Set output file name */
	i = (int) (((spd + 0.0000001) * 10.0) - (z * 10));
	sprintf (file, "%s/t%04d.cat", dir, i);

	/* If not the same file as last, close old file; open new */
	if (strcmp (file, file0)) {
	    /* if (nz > 1)
		printf (" TMCAT: %7d stars written to %s\n", nz, file0); */
	    if (nz == 1) {
	/* 	printf (" TMCAT: %7d stars written to %s", nz, file0);
		printf (" %10.6f %10.6f %17s\n", ra, dec, id); */
		sortfile = fopen ("resort", "a");
		if (sortfile == NULL)
		    sortfile = fopen ("resort", "w");
		if (sortfile != NULL) {
		    fprintf (sortfile,"%s\n", file0);
		    fclose (sortfile);
		    }
		}

	    if (outfile != NULL)
		fclose (outfile);
	    outfile = fopen (file, "a");
	    if (outfile == NULL) {
		outfile = fopen (file, "w");
		if (outfile == NULL) {
		    printf ("TMCAT: Cannot write to file %s\n", file);
		    continue;
		    }
		}
	    strcpy (file0, file);
	    nz = 0;
	    }

	/* Right ascension */
	getoken (&tokens, 1, token, 16);
	ra = atof (token);

	/* ID */
	getoken (&tokens, 6, id, 32);

	/* J, H, K magnitudes */
	getoken (&tokens, 7, token, 16);
	mj = atof (token);
	getoken (&tokens, 11, token, 16);
	mh = atof (token);
	getoken (&tokens, 15, token, 16);
	mk = atof (token);

	/* Photometric quality flag */
	getoken (&tokens, 19, token, 4);
	strncpy (f1, token, 3);
	f1[3] = (char) 0;

	/* Read flag */
	getoken (&tokens, 20, token, 4);
	strncpy (f2, token, 3);
	f2[3] = (char) 0;

	/* Duplicate source flag */
	getoken (&tokens, 49, token, 4);
	dup_src = atoi (token);

	/* Use source flag */
	getoken (&tokens, 50, token, 4);
	use_src = atoi (token);

	if (use_src) {
	    nz = nz + 1;

	    /* Append position and magnitudes from this line to current file */
	    fprintf (outfile,"%10.6f %10.6f %17s %6.3f %6.3f %6.3f %3s %3s\n",
		 ra, dec, id, mj, mh, mk, f1, f2);
	    nt = nt + 1;
	    if (nt % 100000 == 0)
		fprintf (stderr, "%10d stars read to %10.6f %10.6f (%s)\n",
		     nt, ra, dec, file);
	    }
	}

    /* Close output file */
    fclose (outfile);
}


/* -- SETOKEN -- tokenize a string for easy decoding */

int
setoken (tokens, string, cwhite)

struct Tokens *tokens;	/* Token structure returned */
char	*string;	/* character string to tokenize */
char	*cwhite;	/* additional whitespace characters
			 * if = tab, disallow spaces and commas */
{
    char squote, dquote, jch, newline;
    char *iq, *stri, *wtype, *str0, *inew;
    int i,j,naddw;

    newline = (char) 10;
    squote = (char) 39;
    dquote = (char) 34;
    if (string == NULL)
	return (0);

    /* Line is terminated by newline or NULL */
    inew = strchr (string, newline);
    if (inew != NULL)
	tokens->lline = inew - string - 1;
    else
	tokens->lline = strlen (string);

    /* Save current line in structure */
    tokens->line = string;

    /* Add extra whitespace characters */
    if (cwhite == NULL)
	naddw = 0;
    else
	naddw = strlen (cwhite);

    /* if character is tab, allow only tabs and nulls as separators */
    if (naddw > 0 && !strncmp (cwhite, "tab", 3)) {
	tokens->white[0] = (char) 9;	/* Tab */
	tokens->white[1] = (char) 0;	/* NULL (end of string) */
	tokens->nwhite = 2;
	}

    /* if character is bar, allow only bars and nulls as separators */
    else if (naddw > 0 && !strncmp (cwhite, "bar", 3)) {
	tokens->white[0] = '|';		/* Bar */
	tokens->white[1] = (char) 0;	/* NULL (end of string) */
	tokens->nwhite = 2;
	}

    /* otherwise, allow spaces, tabs, commas, nulls, and cwhite */
    else {
	tokens->nwhite = 4 + naddw;;
	tokens->white[0] = ' ';		/* Space */
	tokens->white[1] = (char) 9;	/* Tab */
	tokens->white[2] = ',';		/* Comma */
	tokens->white[3] = (char) 124;	/* Vertical bar */
	tokens->white[4] = (char) 0;	/* Null (end of string) */
	if (tokens->nwhite > 20)
	    tokens->nwhite = 20;
	if (naddw > 0) {
	    i = 0;
	    for (j = 4; j < tokens->nwhite; j++) {
		tokens->white[j] = cwhite[i];
		i++;
		}
	    }
	}
    tokens->white[tokens->nwhite] = (char) 0;

    tokens->ntok = 0;
    tokens->itok = 0;
    iq = string - 1;
    for (i = 0; i < MAXTOKENS; i++) {
	tokens->tok1[i] = NULL;
	tokens->ltok[i] = 0;
	}

    /* Process string one character at a time */
    stri = string;
    str0 = string;
    while (stri < string+tokens->lline) {

	/* Keep stuff between quotes in one token */
	if (stri <= iq)
	    continue;
	jch = *stri;

	/* Handle quoted strings */
	if (jch == squote)
	    iq = strchr (stri+1, squote);
	else if (jch == dquote)
	    iq = strchr (stri+1, dquote);
	else
	    iq = stri;
	if (iq > stri) {
	    tokens->ntok = tokens->ntok + 1;
	    if (tokens->ntok > MAXTOKENS) return (MAXTOKENS);
	    tokens->tok1[tokens->ntok] = stri + 1;
	    tokens->ltok[tokens->ntok] = (iq - stri) - 1;
	    stri = iq + 1;
	    str0 = iq + 1;
	    continue;
	    }

	/* Search for unquoted tokens */
	wtype = strchr (tokens->white, jch);

	/* If this is one of the additional whitespace characters,
	 * pass as a separate token */
	if (wtype > tokens->white + 3) {

	    /* Terminate token before whitespace */
	    if (stri > str0) {
		tokens->ntok = tokens->ntok + 1;
		if (tokens->ntok > MAXTOKENS) return (MAXTOKENS);
		tokens->tok1[tokens->ntok] = str0;
		tokens->ltok[tokens->ntok] = stri - str0;
		}

	    /* Make whitespace character next token; start new one */
	    tokens->ntok = tokens->ntok + 1;
	    if (tokens->ntok > MAXTOKENS) return (MAXTOKENS);
	    tokens->tok1[tokens->ntok] = stri;
	    tokens->ltok[tokens->ntok] = 1;
	    stri++;
	    str0 = stri;
	    }

	/* Pass previous token if regular whitespace or NULL */
	else if (wtype != NULL || jch == (char) 0) {

	    /* Ignore leading whitespace */
	    if (stri == str0) {
		stri++;
		str0 = stri;
		}

	    /* terminate token before whitespace; start new one */
	    else {
		tokens->ntok = tokens->ntok + 1;
		if (tokens->ntok > MAXTOKENS) return (MAXTOKENS);
		tokens->tok1[tokens->ntok] = str0;
		tokens->ltok[tokens->ntok] = stri - str0;
		stri++;
		str0 = stri;
		}
	    }

	/* Keep going if not whitespace */
	else
	    stri++;
	}

    /* Add token terminated by end of line */
    if (str0 < stri) {
	tokens->ntok = tokens->ntok + 1;
	if (tokens->ntok > MAXTOKENS)
	    return (MAXTOKENS);
	tokens->tok1[tokens->ntok] = str0;
	tokens->ltok[tokens->ntok] = stri - str0 + 1;
	}

    tokens->itok = 0;

    return (tokens->ntok);
}


/* NEXTOKEN -- get next token from tokenized string */

int
nextoken (tokens, token, maxchars)
 
struct Tokens *tokens;	/* Token structure returned */
char	*token;		/* token (returned) */
int	maxchars;	/* Maximum length of token */
{
    int ltok;		/* length of token string (returned) */
    int it, i;
    int maxc = maxchars - 1;

    tokens->itok = tokens->itok + 1;
    it = tokens->itok;
    if (it > tokens->ntok)
	it = tokens->ntok;
    else if (it < 1)
	it = 1;
    ltok = tokens->ltok[it];
    if (ltok > maxc)
	ltok = maxc;
    strncpy (token, tokens->tok1[it], ltok);
    for (i = ltok; i < maxc; i++)
	token[i] = (char) 0;
    return (ltok);
}


/* GETOKEN -- get specified token from tokenized string */

int
getoken (tokens, itok, token, maxchars)

struct Tokens *tokens;	/* Token structure returned */
int	itok;		/* token sequence number of token
			 * if <0, get whole string after token -itok
			 * if =0, get whole string */
char	*token;		/* token (returned) */
int	maxchars;	/* Maximum length of token */
{
    int ltok;		/* length of token string (returned) */
    int it, i;
    int maxc = maxchars - 1;

    it = itok;
    if (it > 0 ) {
	if (it > tokens->ntok)
	    it = tokens->ntok;
	ltok = tokens->ltok[it];
	if (ltok > maxc)
	    ltok = maxc;
	strncpy (token, tokens->tok1[it], ltok);
	}
    else if (it < 0) {
	if (it < -tokens->ntok)
	    it  = -tokens->ntok;
	ltok = tokens->line + tokens->lline - tokens->tok1[-it];
	if (ltok > maxc)
	    ltok = maxc;
	strncpy (token, tokens->tok1[-it], ltok);
	}
    else {
	ltok = tokens->lline;
	if (ltok > maxc)
	    ltok = maxc;
	strncpy (token, tokens->tok1[1], ltok);
	}
    for (i = ltok; i < maxc; i++)
	token[i] = (char) 0;

    return (ltok);
}
/* May 16 2003	New program
 * Jun 25 2003	Add filter to keep stars only if use_src is 1
 * Jun 25 2003	Log every 100,000 sources instead of every 10,000
 * Jul  1 2003	Fix bug writing flags
 */
