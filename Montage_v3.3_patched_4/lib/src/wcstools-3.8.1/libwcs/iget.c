/*** File libwcs/iget.c
 *** January 4, 2007
 *** By Doug Mink, dmink@cfa.harvard.edu
 *** Harvard-Smithsonian Center for Astrophysics
 *** Copyright (C) 1998-2007
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

 * Module:	iget.c (Get IRAF FITS Header parameter values)
 * Purpose:	Extract values for variables from IRAF keyword value string
 * Subroutine:	mgeti4 (hstring,mkey,keyword,ival) returns long integer
 * Subroutine:	mgetr8 (hstring,mkey,keyword,dval) returns double
 * Subroutine:	mgetstr (hstring,mkey,keyword,lstr,str) returns character string
 * Subroutine:	igeti4 (hstring,keyword,ival) returns long integer
 * Subroutine:	igetr4 (hstring,keyword,rval) returns real
 * Subroutine:	igetr8 (hstring,keyword,dval) returns double
 * Subroutine:	igets  (hstring,keyword,lstr,str) returns character string
 * Subroutine:	igetc  (hstring,keyword) returns character string
 * Subroutine:	isearch (hstring,keyword) returns pointer to header string entry
 */

#include <string.h>		/* NULL, strlen, strstr, strcpy */
#include <stdio.h>
#include "fitshead.h"	/* FITS header extraction subroutines */
#include <stdlib.h>
#ifndef VMS
#include <limits.h>
#else
#define INT_MAX  2147483647 /* Biggest number that can fit in long */
#define SHRT_MAX 32767
#endif

#define MAX_LVAL 2000

static char *isearch();
static char val[30];

/* Extract long value for variable from IRAF multiline keyword value */

int
mgeti4 (hstring, mkey, keyword, ival)

const char *hstring;	/* Character string containing FITS or IRAF header information
		   in the format <keyword>= <value> ... */
const char *mkey;	/* Character string containing the name of the multi-line
		   keyword, the string value of which contains the desired
		   keyword, the value of which is returned. */
const char *keyword;	/* Character string containing the name of the keyword
		   within the multiline IRAF keyword */
int *ival;	/* Integer value returned */
{
    char *mstring;

    mstring = malloc (MAX_LVAL);

    if (hgetm (hstring, mkey, MAX_LVAL, mstring)) {
	if (igeti4 (mstring, keyword, ival)) {
	    free (mstring);
	    return (1);
	    }
	else {
	    free (mstring);
	    return (0);
	    }
	}
    else {
	free (mstring);
	return (0);
	}
}

/* Extract double value for variable from IRAF multiline keyword value */

int
mgetr8 (hstring, mkey, keyword, dval)

const char	*hstring; /* Character string containing FITS or IRAF header information
		   in the format <keyword>= <value> ... */
const char	*mkey;	  /* Character string containing the name of the multi-line
		   keyword, the string value of which contains the desired
		   keyword, the value of which is returned. */
const char	*keyword; /* Character string containing the name of the keyword
		   within the multiline IRAF keyword */
double	*dval;	  /* Integer value returned */
{
    char *mstring;
    mstring = malloc (MAX_LVAL);

    if (hgetm (hstring, mkey, MAX_LVAL, mstring)) {
	if (igetr8 (mstring, keyword, dval)) {
	    free (mstring);
	    return (1);
	    }
	else {
	    free (mstring);
	    return (0);
	    }
	}
    else {
	free (mstring);
	return (0);
	}
}


/* Extract string value for variable from IRAF keyword value string */

int
mgetstr (hstring, mkey, keyword, lstr, str)

const char *hstring;	/* character string containing FITS header information
		   in the format <keyword>= <value> {/ <comment>} */
const char *mkey;	/* Character string containing the name of the multi-line
		   keyword, the string value of which contains the desired
		   keyword, the value of which is returned. */
const char *keyword;	/* character string containing the name of the keyword
		   the value of which is returned.  hget searches for a
		   line beginning with this string.  if "[n]" is present,
		   the n'th token in the value is returned.
		   (the first 8 characters must be unique) */
const int lstr;	/* Size of str in characters */
char *str;	/* String (returned) */
{
    char *mstring;
    mstring = malloc (MAX_LVAL);

    if (hgetm (hstring, mkey, MAX_LVAL, mstring)) {
	if (igets (mstring, keyword, lstr, str)) {
	    free (mstring);
	    return (1);
	    }
	else {
	    free (mstring);
	    return (0);
	    }
	}
    else {
	free (mstring);
	return (0);
	}
}


/* Extract long value for variable from IRAF keyword value string */

int
igeti4 (hstring, keyword, ival)

const char *hstring;	/* character string containing IRAF header information
		   in the format <keyword>= <value> ... */
const char *keyword;	/* character string containing the name of the keyword
		   the value of which is returned.  hget searches for a
		   line beginning with this string.  if "[n]" is present,
		   the n'th token in the value is returned.
		   (the first 8 characters must be unique) */
int *ival;	/* Integer value returned */
{
char *value;
double dval;
int minint;

/* Get value from header string */
	value = igetc (hstring,keyword);

/* Translate value from ASCII to binary */
	if (value != NULL) {
	    minint = -INT_MAX - 1;
	    strcpy (val, value);
	    dval = atof (val);
	    if (dval+0.001 > INT_MAX)
		*ival = INT_MAX;
	    else if (dval >= 0)
		*ival = (int) (dval + 0.001);
	    else if (dval-0.001 < minint)
		*ival = minint;
	    else
		*ival = (int) (dval - 0.001);
	    return (1);
	    }
	else {
	    return (0);
	    }
}


/* Extract integer*2 value for variable from IRAF keyword value string */

int
igeti2 (hstring,keyword,ival)

const char *hstring;	/* character string containing FITS header information
		   in the format <keyword>= <value> {/ <comment>} */
const char *keyword;	/* character string containing the name of the keyword
		   the value of which is returned.  hget searches for a
		   line beginning with this string.  if "[n]" is present,
		   the n'th token in the value is returned.
		   (the first 8 characters must be unique) */
short *ival;
{
char *value;
double dval;
int minshort;

/* Get value from header string */
	value = igetc (hstring,keyword);

/* Translate value from ASCII to binary */
	if (value != NULL) {
	    strcpy (val, value);
	    dval = atof (val);
	    minshort = -SHRT_MAX - 1;
	    if (dval+0.001 > SHRT_MAX)
		*ival = SHRT_MAX;
	    else if (dval >= 0)
		*ival = (short) (dval + 0.001);
	    else if (dval-0.001 < minshort)
		*ival = minshort;
	    else
		*ival = (short) (dval - 0.001);
	    return (1);
	    }
	else {
	    return (0);
	    }
}

/* Extract real value for variable from IRAF keyword value string */

int
igetr4 (hstring,keyword,rval)

const char *hstring;	/* character string containing FITS header information
		   in the format <keyword>= <value> {/ <comment>} */
const char *keyword;	/* character string containing the name of the keyword
		   the value of which is returned.  hget searches for a
		   line beginning with this string.  if "[n]" is present,
		   the n'th token in the value is returned.
		   (the first 8 characters must be unique) */
float *rval;
{
	char *value;

/* Get value from header string */
	value = igetc (hstring,keyword);

/* Translate value from ASCII to binary */
	if (value != NULL) {
	    strcpy (val, value);
	    *rval = (float) atof (val);
	    return (1);
	    }
	else {
	    return (0);
	    }
}


/* Extract real*8 value for variable from IRAF keyword value string */

int
igetr8 (hstring,keyword,dval)

const char *hstring;	/* character string containing FITS header information
		   in the format <keyword>= <value> {/ <comment>} */
const char *keyword;	/* character string containing the name of the keyword
		   the value of which is returned.  hget searches for a
		   line beginning with this string.  if "[n]" is present,
		   the n'th token in the value is returned.
		   (the first 8 characters must be unique) */
double *dval;
{
	char *value,val[30];

/* Get value from header string */
	value = igetc (hstring,keyword);

/* Translate value from ASCII to binary */
	if (value != NULL) {
	    strcpy (val, value);
	    *dval = atof (val);
	    return (1);
	    }
	else {
	    return (0);
	    }
}


/* Extract string value for variable from IRAF keyword value string */

int
igets (hstring, keyword, lstr, str)

const char *hstring;	/* character string containing FITS header information
		   in the format <keyword>= <value> {/ <comment>} */
const char *keyword;	/* character string containing the name of the keyword
		   the value of which is returned.  hget searches for a
		   line beginning with this string.  if "[n]" is present,
		   the n'th token in the value is returned.
		   (the first 8 characters must be unique) */
const int lstr;	/* Size of str in characters */
char *str;	/* String (returned) */
{
	char *value;
	int lval;

/* Get value from header string */
	value = igetc (hstring,keyword);

	if (value != NULL) {
	    lval = strlen (value);
	    if (lval < lstr)
		strcpy (str, value);
	    else if (lstr > 1)
		strncpy (str, value, lstr-1);
	    else
		str[0] = value[0];
	    return (1);
	    }
	else
	    return (0);
}


/* Extract character value for variable from IRAF keyword value string */

char *
igetc (hstring,keyword0)

const char *hstring;	/* character string containing IRAF keyword value string
		   in the format <keyword>= <value> {/ <comment>} */
const char *keyword0;	/* character string containing the name of the keyword
		   the value of which is returned.  iget searches for a
		   line beginning with this string.  if "[n]" is present,
		   the n'th token in the value is returned.
		   (the first 8 characters must be unique) */
{
	static char cval[MAX_LVAL];
	char *value;
	char cwhite[8];
	char lbracket[2],rbracket[2];
	char keyword[16];
	char line[MAX_LVAL];
	char *vpos,*cpar;
	char *c1, *brack1, *brack2;
	int ipar, i;

	lbracket[0] = 91;
	lbracket[1] = 0;
	rbracket[0] = 93;
	rbracket[1] = 0;

/* Find length of variable name */
	strcpy (keyword,keyword0);
	brack1 = strsrch (keyword,lbracket);
	if (brack1 != NULL) *brack1 = '\0';

/* Search header string for variable name */
	vpos = isearch (hstring,keyword);

/* Exit if not found */
	if (vpos == NULL) {
	    return (NULL);
	    }

/* Initialize returned value to nulls */
	 for (i = 0; i < MAX_LVAL; i++)
	    line[i] = 0;

/* If quoted value, copy until second quote is reached */
	i = 0;
	if (*vpos == '"') {
	     vpos++;
	     while (*vpos && *vpos != '"' && i < MAX_LVAL)
		line[i++] = *vpos++;
	     }

/* Otherwise copy until next space or tab */
	else {
	     while (*vpos != ' ' && *vpos != (char)9 &&
		    *vpos > 0 && i < MAX_LVAL)
		line[i++] = *vpos++;
	     }

/* If keyword has brackets, extract appropriate token from value */
	if (brack1 != NULL) {
	    c1 = (char *) (brack1 + 1);
	    brack2 = strsrch (c1, rbracket);
	    if (brack2 != NULL) {
		*brack2 = '\0';
		ipar = atoi (c1);
		if (ipar > 0) {
		    cwhite[0] = ' ';
		    cwhite[1] = ',';
		    cwhite[2] = '\0';
		    cpar = strtok (line, cwhite);
		    for (i = 1; i < ipar; i++) {
			cpar = strtok (NULL, cwhite);
			}
		    if (cpar != NULL) {
			strcpy (cval,cpar);
			}
		    else
			value = NULL;
		    }
		}
	    }
	else
	    strcpy (cval, line);

	value = cval;

	return (value);
}


/* Find value for specified IRAF keyword */

static char *
isearch (hstring,keyword)

/* Find entry for keyword keyword in IRAF keyword value string hstring.
   NULL is returned if the keyword is not found */

const char *hstring;	/* character string containing fits-style header
		information in the format <keyword>= <value> {/ <comment>}
		the default is that each entry is 80 characters long;
		however, lines may be of arbitrary length terminated by
		nulls, carriage returns or linefeeds, if packed is true.  */
const char *keyword;	/* character string containing the name of the variable
		to be returned.  isearch searches for a line beginning
		with this string.  The string may be a character
		literal or a character variable terminated by a null
		or '$'.  it is truncated to 8 characters. */
{
    char *loc, *headnext, *headlast, *pval;
    int lastchar, nextchar, lkey, nleft, lhstr;

/* Search header string for variable name */
    lhstr = 0;
    while (lhstr < 57600 && hstring[lhstr] != 0)
	lhstr++;
    headlast = (char *) hstring + lhstr;
    headnext = (char *) hstring;
    pval = NULL;
    lkey = strlen (keyword);
    while (headnext < headlast) {
	nleft = headlast - headnext;
	loc = strnsrch (headnext, keyword, nleft);

	/* Exit if keyword is not found */
	if (loc == NULL) {
	    break;
	    }

	nextchar = (int) *(loc + lkey);
	lastchar = (int) *(loc - 1);

	/* If parameter name in header is longer, keep searching */
	if (nextchar != 61 && nextchar > 32 && nextchar < 127)
	    headnext = loc + 1;

	/* If start of string, keep it */
	else if (loc == hstring) {
	    pval = loc;
	    break;
	    }

	/* If preceeded by a blank or tab, keep it */
	else if (lastchar == 32 || lastchar == 9) {
	    pval = loc;
	    break;
	    }

	else
	    headnext = loc + 1;
	}

    /* Find start of value string for this keyword */
    if (pval != NULL) {
	pval = pval + lkey;
	while (*pval == ' ' || *pval == '=')
	    pval++;
	}

    /* Return pointer to calling program */
    return (pval);

}

/* Mar 12 1998	New subroutines
 * Apr 15 1998	Set IGET() and ISEARCH() static when defined
 * Apr 24 1998	Add MGETI4(), MGETR8(), and MGETS() for single step IRAF ext.
 * Jun  1 1998	Add VMS patch from Harry Payne at STScI
 * Jul  9 1998	Fix bracket token extraction after Paul Sydney

 * May  5 1999	values.h -> POSIX limits.h: MAXINT->INT_MAX, MAXSHORT->SHRT_MAX
 * Oct 21 1999	Fix declarations after lint
 *
 * Feb 11 2000	Stop search for end of quoted keyword if more than 500 chars
 * Jul 20 2000	Drop unused variables squot, dquot, and slash in igetc()
 *
 * Jun 26 2002	Change maximum string length from 600 to 2000; use MAX_LVAL
 * Jun 26 2002	Stop search for end of quoted keyword if > MAX_LVAL chars
 *
 * Sep 23 2003	Change mgets() to mgetstr() to avoid name collision at UCO Lick
 *
 * Feb 26 2004	Make igetc() accessible from outside this file
 *
 * Jan  4 2007	Declare header, keyword to be const
 */
