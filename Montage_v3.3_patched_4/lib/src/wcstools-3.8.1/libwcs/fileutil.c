/*** File libwcs/fileutil.c
 *** January 11, 2007
 *** By Doug Mink, dmink@cfa.harvard.edu
 *** Harvard-Smithsonian Center for Astrophysics
 *** Copyright (C) 1999-2007
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

 * Module:      fileutil.c (ASCII file utilities)
 * Purpose:     Find out things about ASCII files
 * Subroutine:	getfilelines (filename)
 *		Return number of lines in an ASCII file
 * Subroutine:	getfilebuff (filename)
 *		Return entire file contents in a character string
 * Subroutine:	getfilesize (filename)
 *		Return size of a binary or ASCII file
 * Subroutine:	isimlist (filename)
 *		Return 1 if file is list of FITS or IRAF image files, else 0
 * Subroutine:	isimlistd (filename, rootdir)
 *		Return 1 if file is list of FITS or IRAF image files, else 0
 * Subroutine:	isfilelist (filename, rootdir)
 *		Return 1 if file is list of readable files, else 0
 * Subroutine:	isfile (filename)
 *		Return 1 if file is a readable file, else 0
 * Subroutine:	first_token (diskfile, ncmax, token)
 *		Return first token from the next line of an ASCII file
 * Subroutine:  stc2s (spchar, string)
 *		Replace character in string with space
 * Subroutine:  sts2c (spchar, string)
 *		Replace spaces in string with character
 * Subroutine:	istiff (filename)
 *		Return 1 if file is a readable TIFF graphics file, else 0
 * Subroutine:	isjpeg (filename)
 *		Return 1 if file is a readable JPEG graphics file, else 0
 * int setoken (tokens, string, cwhite)
 *	Tokenize a string for easy decoding
 * int nextoken (tokens, token, maxchars)
 *	Get next token from tokenized string
 * int getoken (tokens, itok, token, maxchars)
 *	Get specified token from tokenized string
 */

#include <stdlib.h>
#ifndef VMS
#include <unistd.h>
#endif
#include <stdio.h>
#include <fcntl.h>
#include <sys/file.h>
#include <errno.h>
#include <string.h>
#include "fitsfile.h"
#include <sys/types.h>
#include <sys/stat.h>


/* GETFILELINES -- return number of lines in one file */

int
getfilelines (filename)

char    *filename;      /* Name of file for which to find number of lines */
{

    char *buffer, *bufline;
    int nlines = 0;
    char newline = 10;

    /* Read file */
    buffer = getfilebuff (filename);

    /* Count lines in file */
    if (buffer != NULL) {
	bufline = buffer;
	nlines = 0;
	while ((bufline = strchr (bufline, newline)) != NULL) {
            bufline = bufline + 1;
            nlines++;
	    }
	free (buffer);
	return (nlines);
	}
    else {
	return (0);
	}
}


/* GETFILEBUFF -- return entire file contents in one character string */

char *
getfilebuff (filename)

char    *filename;      /* Name of file for which to find number of lines */
{

    FILE *diskfile;
    int lfile, nr, lbuff, ipt, ibuff;
    char *buffer, *newbuff, *nextbuff;

    /* Treat stdin differently */
    if (!strcmp (filename, "stdin")) {
	lbuff = 5000;
	lfile = lbuff;
	buffer = NULL;
	ipt = 0;
	for (ibuff = 0; ibuff < 10; ibuff++) {
	    if ((newbuff = realloc (buffer, lfile+1)) != NULL) {
		buffer = newbuff;
		nextbuff = buffer + ipt;
        	nr = fread (nextbuff, 1, lbuff, stdin);
		if (nr == lbuff)
		    break;
		else {
		    ipt = ipt + lbuff;
		    lfile = lfile + lbuff;
		    }
		}
	    else {
		fprintf (stderr,"GETFILEBUFF: No room for %d-byte buffer\n",
			 lfile);
		break;
		}
	    }
	return (buffer);
	}

    /* Open file */
    if ((diskfile = fopen (filename, "rb")) == NULL)
        return (NULL);

   /* Find length of file */
    if (fseek (diskfile, 0, 2) == 0)
        lfile = ftell (diskfile);
    else
        lfile = 0;
    if (lfile < 1) {
	fprintf (stderr,"GETFILEBUFF: File %s is empty\n", filename);
	fclose (diskfile);
	return (NULL);
	}

    /* Allocate buffer to hold entire file and read it */
    if ((buffer = calloc (1, lfile+1)) != NULL) {
 	fseek (diskfile, 0, 0);
        nr = fread (buffer, 1, lfile, diskfile);
	if (nr < lfile) {
	    fprintf (stderr,"GETFILEBUFF: File %s: read %d / %d bytes\n",
		     filename, nr, lfile);
	    free (buffer);
	    fclose (diskfile);
	    return (NULL);
	    }
	buffer[lfile] = (char) 0;
	fclose (diskfile);
	return (buffer);
	}
    else {
	fprintf (stderr,"GETFILEBUFF: File %s: no room for %d-byte buffer\n",
		 filename, lfile);
	fclose (diskfile);
	return (NULL);
	}
}


/* GETFILESIZE -- return size of one file in bytes */

int
getfilesize (filename)

char    *filename;      /* Name of file for which to find size */
{
    struct stat statbuff;

    if (stat (filename, &statbuff))
	return (0);
    else
	return ((int) statbuff.st_size);
}

int
getfilesize0 (filename)

char    *filename;      /* Name of file for which to find size */
{
    FILE *diskfile;
    long filesize;

    /* Open file */
    if ((diskfile = fopen (filename, "rb")) == NULL)
        return (-1);

    /* Move to end of the file */
    if (fseek (diskfile, 0, 2) == 0)

        /* Position is the size of the file */
        filesize = ftell (diskfile);

    else
        filesize = -1;

    fclose (diskfile);

    return ((int) filesize);
}


/* ISIMLIST -- Return 1 if list of FITS or IRAF files, else 0 */
int
isimlist (filename)

char    *filename;      /* Name of possible list file */
{
    FILE *diskfile;
    char token[256];
    int ncmax = 254;

    if ((diskfile = fopen (filename, "r")) == NULL)
	return (0);
    else {
	first_token (diskfile, ncmax, token);
	fclose (diskfile);
	if (isfits (token) | isiraf (token))
	    return (1);
	else
	    return (0);
	}
}


/* ISIMLISTD -- Return 1 if list of FITS or IRAF files, else 0 */
int
isimlistd (filename, rootdir)

char    *filename;	/* Name of possible list file */
char    *rootdir;	/* Name of root directory for files in list */
{
    FILE *diskfile;
    char token[256];
    char filepath[256];
    int ncmax = 254;

    if ((diskfile = fopen (filename, "r")) == NULL)
	return (0);
    else {
	first_token (diskfile, ncmax, token);
	fclose (diskfile);
	if (rootdir != NULL) {
	    strcpy (filepath, rootdir);
	    strcat (filepath, "/");
	    strcat (filepath, token);
	    }
	else
	    strcpy (filepath, token);
	if (isfits (filepath) | isiraf (filepath))
	    return (1);
	else
	    return (0);
	}
}


/* ISFILELIST -- Return 1 if list of readable files, else 0 */
int
isfilelist (filename, rootdir)

char    *filename;      /* Name of possible list file */
char    *rootdir;	/* Name of root directory for files in list */
{
    FILE *diskfile;
    char token[256];
    char filepath[256];
    int ncmax = 254;

    if ((diskfile = fopen (filename, "r")) == NULL)
	return (0);
    else {
	first_token (diskfile, ncmax, token);
	fclose (diskfile);
	if (rootdir != NULL) {
	    strcpy (filepath, rootdir);
	    strcat (filepath, "/");
	    strcat (filepath, token);
	    }
	else
	    strcpy (filepath, token);
	if (isfile (filepath))
	    return (1);
	else
	    return (0);
	}
}


/* ISFILE -- Return 1 if file is a readable file, else 0 */

int
isfile (filename)

char    *filename;      /* Name of file to check */
{
    if (!strcasecmp (filename, "stdin"))
	return (1);
    else if (access (filename, R_OK))
	return (0);
    else
	return (1);
}


/* FIRST_TOKEN -- Return first token from the next line of an ASCII file */

int
first_token (diskfile, ncmax, token)

FILE	*diskfile;		/* File descriptor for ASCII file */
int	ncmax;			/* Maximum number of characters returned */
char	*token;			/* First token on next line (returned) */
{
    char *lastchar, *lspace;

    /* If line can be read, add null at the end of the first token */
    if (fgets (token, ncmax, diskfile) != NULL) {
	if (token[0] == '#') {
	    (void) fgets (token, ncmax, diskfile);
	    }

	/* If only character is a control character, return a NULL */
	if ((strlen(token)==1) && (token[0]<32)){
	    token[0]=0;
	    return (1);
	    }
	lastchar = token + strlen (token) - 1;

	/* Remove trailing spaces or control characters */
	while (*lastchar <= 32)
	    *lastchar-- = 0;

	if ((lspace = strchr (token, ' ')) != NULL) {
	    *lspace = (char) 0;
	    }
	return (1);
	}
    else
	return (0);
}


/* Replace character in string with space */

int
stc2s (spchar, string)

char	*spchar;	/* Character to replace with spaces */
char	*string;
{
    int i, lstr, n;
    lstr = strlen (string);
    n = 0;
    for (i = 0; i < lstr; i++) {
	if (string[i] == spchar[0]) {
	    n++;
	    string[i] = ' ';
	    }
	}
    return (n);
}


/* Replace spaces in string with character */

int
sts2c (spchar, string)

char	*spchar;	/* Character with which to replace spaces */
char	*string;
{
    int i, lstr, n;
    lstr = strlen (string);
    n = 0;
    for (i = 0; i < lstr; i++) {
	if (string[i] == ' ') {
	    n++;
	    string[i] = spchar[0];
	    }
	}
    return (n);
}


/* ISTIFF -- Return 1 if TIFF file, else 0 */
int
istiff (filename)

char    *filename;      /* Name of file to check */
{
    int diskfile;
    char keyword[16];
    int nbr;

    /* First check to see if this is an assignment */
    if (strchr (filename, '='))
        return (0);

    /* Check file extension */
    if (strsrch (filename, ".tif") ||
        strsrch (filename, ".tiff") ||
        strsrch (filename, ".TIFF") ||
        strsrch (filename, ".TIF"))
        return (1);

 /* If no TIFF file suffix, try opening the file */
    else {
        if ((diskfile = open (filename, O_RDONLY)) < 0)
            return (0);
        else {
            nbr = read (diskfile, keyword, 4);
            close (diskfile);
            if (nbr < 4)
                return (0);
            else if (!strncmp (keyword, "II", 2))
                return (1);
            else if (!strncmp (keyword, "MM", 2))
                return (1);
            else
                return (0);
            }
        }
}


/* ISJPEG -- Return 1 if JPEG file, else 0 */
int
isjpeg (filename)

char    *filename;      /* Name of file to check */
{
    int diskfile;
    char keyword[16];
    int nbr;

    /* First check to see if this is an assignment */
    if (strchr (filename, '='))
        return (0);

    /* Check file extension */
    if (strsrch (filename, ".jpg") ||
        strsrch (filename, ".jpeg") ||
        strsrch (filename, ".JPEG") ||
        strsrch (filename, ".jfif") ||
        strsrch (filename, ".jfi") ||
        strsrch (filename, ".JFIF") ||
        strsrch (filename, ".JFI") ||
        strsrch (filename, ".JPG"))
        return (1);

 /* If no JPEG file suffix, try opening the file */
    else {
        if ((diskfile = open (filename, O_RDONLY)) < 0)
            return (0);
        else {
            nbr = read (diskfile, keyword, 2);
            close (diskfile);
            if (nbr < 4)
                return (0);
            else if (keyword[0] == (char) 0xFF &&
		     keyword[1] == (char) 0xD8)
                return (1);
            else
                return (0);
            }
        }
}


/* ISGIF -- Return 1 if GIF file, else 0 */
int
isgif (filename)

char    *filename;      /* Name of file to check */
{
    int diskfile;
    char keyword[16];
    int nbr;

    /* First check to see if this is an assignment */
    if (strchr (filename, '='))
        return (0);

    /* Check file extension */
    if (strsrch (filename, ".gif") ||
        strsrch (filename, ".GIF"))
        return (1);

 /* If no GIF file suffix, try opening the file */
    else {
        if ((diskfile = open (filename, O_RDONLY)) < 0)
            return (0);
        else {
            nbr = read (diskfile, keyword, 6);
            close (diskfile);
            if (nbr < 4)
                return (0);
            else if (!strncmp (keyword, "GIF", 3))
                return (1);
            else
                return (0);
            }
        }
}


static int maxtokens = MAXTOKENS; /* Set maximum number of tokens from wcscat.h*/

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
    int i,j,naddw, ltok;

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
    for (i = 0; i < maxtokens; i++) {
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
	    if (tokens->ntok > maxtokens) return (maxtokens);
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
		if (tokens->ntok > maxtokens) return (maxtokens);
		tokens->tok1[tokens->ntok] = str0;
		tokens->ltok[tokens->ntok] = stri - str0;
		}

	    /* Make whitespace character next token; start new one */
	    tokens->ntok = tokens->ntok + 1;
	    if (tokens->ntok > maxtokens) return (maxtokens);
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
		if (tokens->ntok > maxtokens) return (maxtokens);
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
	if (tokens->ntok > maxtokens)
	    return (maxtokens);
	tokens->tok1[tokens->ntok] = str0;
	ltok = stri - str0 + 1;
	tokens->ltok[tokens->ntok] = ltok;

	/* Deal with white space just before end of line */
	jch = str0[ltok-1];
	if (strchr (tokens->white, jch)) {
	    ltok = ltok - 1;
	    tokens->ltok[tokens->ntok] = ltok;
	    tokens->ntok = tokens->ntok + 1;
	    tokens->tok1[tokens->ntok] = str0 + ltok;
	    tokens->ltok[tokens->ntok] = 0;
	    }
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

/*
 * Jul 14 1999	New subroutines
 * Jul 15 1999	Add getfilebuff()
 * Oct 15 1999	Fix format eror in error message
 * Oct 21 1999	Fix declarations after lint
 * Dec  9 1999	Add next_token(); set pointer to next token in first_token
 *
 * Sep 25 2001	Add isfilelist(); move isfile() from catutil.c
 *
 * Jan  4 2002	Allow getfilebuffer() to read from stdin
 * Jan  8 2002	Add sts2c() and stc2s() for space-replaced strings
 * Mar 22 2002	Clean up isfilelist()
 * Aug  1 2002	Return 1 if file is stdin in isfile()
 *
 * Feb  4 2003	Open catalog file rb instead of r (Martin Ploner, Bern)
 * Mar  5 2003	Add isimlistd() to check image lists with root directory
 * May 27 2003	Use file stat call in getfilesize() instead of opening file
 * Jul 17 2003	Add root directory argument to isfilelist()
 *
 * Sep 29 2004	Drop next_token() to avoid conflict with subroutine in catutil.c
 *
 * Sep 26 2005	In first_token, return NULL if token is only control character
 *
 * Feb 23 2006	Add istiff(), isjpeg(), isgif() to check TIFF, JPEG, GIF files
 * Jun 20 2006	Cast call to fgets() void
 *
 * Jan  5 2007	Change stc2s() and sts2c() to pass single character as pointer
 * Jan 11 2007	Move token access subroutines from catutil.c
 */
