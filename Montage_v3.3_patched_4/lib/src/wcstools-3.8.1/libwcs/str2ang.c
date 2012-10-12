

/* Return the right ascension in degrees from sexagesimal hours or decimal degrees */

double
str2ra (in)

const char *in;	/* Character string of sexigesimal hours or decimal degrees */

{
    double ra;	/* Right ascension in degrees (returned) */

    ra = str2dec (in);
    if (strsrch (in,":"))
	ra = ra * 15.0;

    return (ra);
}


/* Return the declination in degrees from sexagesimal or decimal degrees */

double
str2dec (in)

const char *in;	/* Character string of sexigesimal or decimal degrees */

{
    double dec;		/* Declination in degrees (returned) */
    double deg, min, sec, sign;
    char *value, *c1, *c2;
    int lval;
    char *dchar;

    dec = 0.0;

    /* Return 0.0 if string is null */
    if (in == NULL)
	return (dec);

    /* Translate value from ASCII colon-delimited string to binary */
    if (in[0]) {
	value = (char *) in;

	/* Remove leading spaces */
	while (*value == ' ')
	    value++;

	/* Save sign */
	if (*value == '-') {
	    sign = -1.0;
	    value++;
	    }
	else if (*value == '+') {
	    sign = 1.0;
	    value++;
	    }
	else
	    sign = 1.0;

	/* Remove trailing spaces */
	lval = strlen (value);
	while (value[lval-1] == ' ')
	    lval--;
	
	if ((c1 = strsrch (value,":")) == NULL)
	    c1 = strnsrch (value," ",lval);
	if (c1 != NULL) {
	    *c1 = 0;
	    deg = (double) atoi (value);
	    *c1 = ':';
	    value = c1 + 1;
	    if ((c2 = strsrch (value,":")) == NULL)
		c2 = strsrch (value," ");
	    if (c2 != NULL) {
		*c2 = 0;
		min = (double) atoi (value);
		*c2 = ':';
		value = c2 + 1;
		sec = atof (value);
		}
	    else {
		sec = 0.0;
		if ((c1 = strsrch (value,".")) != NULL)
		    min = atof (value);
		if (strlen (value) > 0)
		    min = (double) atoi (value);
		}
	    dec = sign * (deg + (min / 60.0) + (sec / 3600.0));
	    }
	else if (isnum (value) == 2) {
	    if ((dchar = strchr (value, 'D')))
		*dchar = 'e';
	    if ((dchar = strchr (value, 'd')))
		*dchar = 'e';
	    if ((dchar = strchr (value, 'E')))
		*dchar = 'e';
	    dec = sign * atof (value);
	    }
	else 
	    dec = sign * (double) atoi (value);
	}
    return (dec);
}
