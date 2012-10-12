

/* Write the right ascension ra in sexagesimal format into string*/

void
ra2str (string, lstr, ra, ndec)

char	*string;	/* Character string (returned) */
int	lstr;		/* Maximum number of characters in string */
double	ra;		/* Right ascension in degrees */
int	ndec;		/* Number of decimal places in seconds */

{
    double a,b;
    double seconds;
    char tstring[64];
    int hours;
    int minutes;
    int isec, ltstr;
    double dsgn;

    /* Keep RA between 0 and 360 */
    if (ra < 0.0 ) {
	ra = -ra;
	dsgn = -1.0;
	}
    else
	dsgn = 1.0;
    ra = fmod(ra, 360.0);
    ra *= dsgn;
    if (ra < 0.0)
	ra = ra + 360.0;

    a = ra / 15.0;

    /* Convert to hours */
    hours = (int) a;

    /* Compute minutes */
    b =  (a - (double)hours) * 60.0;
    minutes = (int) b;

    /* Compute seconds */
    seconds = (b - (double)minutes) * 60.0;

    if (ndec > 5) {
	if (seconds > 59.999999) {
	    seconds = 0.0;
	    minutes = minutes + 1;
	    }
	if (minutes > 59) {
	    minutes = 0;
	    hours = hours + 1;
	    }
	hours = hours % 24;
	(void) sprintf (tstring,"%02d:%02d:%09.6f",hours,minutes,seconds);
	}
    else if (ndec > 4) {
	if (seconds > 59.99999) {
	    seconds = 0.0;
	    minutes = minutes + 1;
	    }
	if (minutes > 59) {
	    minutes = 0;
	    hours = hours + 1;
	    }
	hours = hours % 24;
	(void) sprintf (tstring,"%02d:%02d:%08.5f",hours,minutes,seconds);
	}
    else if (ndec > 3) {
	if (seconds > 59.9999) {
	    seconds = 0.0;
	    minutes = minutes + 1;
	    }
	if (minutes > 59) {
	    minutes = 0;
	    hours = hours + 1;
	    }
	hours = hours % 24;
	(void) sprintf (tstring,"%02d:%02d:%07.4f",hours,minutes,seconds);
	}
    else if (ndec > 2) {
	if (seconds > 59.999) {
	    seconds = 0.0;
	    minutes = minutes + 1;
	    }
	if (minutes > 59) {
	    minutes = 0;
	    hours = hours + 1;
	    }
	hours = hours % 24;
	(void) sprintf (tstring,"%02d:%02d:%06.3f",hours,minutes,seconds);
	}
    else if (ndec > 1) {
	if (seconds > 59.99) {
	    seconds = 0.0;
	    minutes = minutes + 1;
	    }
	if (minutes > 59) {
	    minutes = 0;
	    hours = hours + 1;
	    }
	hours = hours % 24;
	(void) sprintf (tstring,"%02d:%02d:%05.2f",hours,minutes,seconds);
	}
    else if (ndec > 0) {
	if (seconds > 59.9) {
	    seconds = 0.0;
	    minutes = minutes + 1;
	    }
	if (minutes > 59) {
	    minutes = 0;
	    hours = hours + 1;
	    }
	hours = hours % 24;
	(void) sprintf (tstring,"%02d:%02d:%04.1f",hours,minutes,seconds);
	}
    else {
	isec = (int)(seconds + 0.5);
	if (isec > 59) {
	    isec = 0;
	    minutes = minutes + 1;
	    }
	if (minutes > 59) {
	    minutes = 0;
	    hours = hours + 1;
	    }
	hours = hours % 24;
	(void) sprintf (tstring,"%02d:%02d:%02d",hours,minutes,isec);
	}

    /* Move formatted string to returned string */
    ltstr = (int) strlen (tstring);
    if (ltstr < lstr-1)
	strcpy (string, tstring);
    else {
	strncpy (string, tstring, lstr-1);
	string[lstr-1] = 0;
	}
    return;
}


/* Write the variable a in sexagesimal format into string */

void
dec2str (string, lstr, dec, ndec)

char	*string;	/* Character string (returned) */
int	lstr;		/* Maximum number of characters in string */
double	dec;		/* Declination in degrees */
int	ndec;		/* Number of decimal places in arcseconds */

{
    double a, b, dsgn, deg1;
    double seconds;
    char sign;
    int degrees;
    int minutes;
    int isec, ltstr;
    char tstring[64];

    /* Keep angle between -180 and 360 degrees */
    deg1 = dec;
    if (deg1 < 0.0 ) {
	deg1 = -deg1;
	dsgn = -1.0;
	}
    else
	dsgn = 1.0;
    deg1 = fmod(deg1, 360.0);
    deg1 *= dsgn;
    if (deg1 <= -180.0)
	deg1 = deg1 + 360.0;

    a = deg1;

    /* Set sign and do all the rest with a positive */
    if (a < 0) {
	sign = '-';
	a = -a;
	}
    else
	sign = '+';

    /* Convert to degrees */
    degrees = (int) a;

    /* Compute minutes */
    b =  (a - (double)degrees) * 60.0;
    minutes = (int) b;

    /* Compute seconds */
    seconds = (b - (double)minutes) * 60.0;

    if (ndec > 5) {
	if (seconds > 59.999999) {
	    seconds = 0.0;
	    minutes = minutes + 1;
	    }
	if (minutes > 59) {
	    minutes = 0;
	    degrees = degrees + 1;
	    }
	(void) sprintf (tstring,"%c%02d:%02d:%09.6f",sign,degrees,minutes,seconds);
	}
    else if (ndec > 4) {
	if (seconds > 59.99999) {
	    seconds = 0.0;
	    minutes = minutes + 1;
	    }
	if (minutes > 59) {
	    minutes = 0;
	    degrees = degrees + 1;
	    }
	(void) sprintf (tstring,"%c%02d:%02d:%08.5f",sign,degrees,minutes,seconds);
	}
    else if (ndec > 3) {
	if (seconds > 59.9999) {
	    seconds = 0.0;
	    minutes = minutes + 1;
	    }
	if (minutes > 59) {
	    minutes = 0;
	    degrees = degrees + 1;
	    }
	(void) sprintf (tstring,"%c%02d:%02d:%07.4f",sign,degrees,minutes,seconds);
	}
    else if (ndec > 2) {
	if (seconds > 59.999) {
	    seconds = 0.0;
	    minutes = minutes + 1;
	    }
	if (minutes > 59) {
	    minutes = 0;
	    degrees = degrees + 1;
	    }
	(void) sprintf (tstring,"%c%02d:%02d:%06.3f",sign,degrees,minutes,seconds);
	}
    else if (ndec > 1) {
	if (seconds > 59.99) {
	    seconds = 0.0;
	    minutes = minutes + 1;
	    }
	if (minutes > 59) {
	    minutes = 0;
	    degrees = degrees + 1;
	    }
	(void) sprintf (tstring,"%c%02d:%02d:%05.2f",sign,degrees,minutes,seconds);
	}
    else if (ndec > 0) {
	if (seconds > 59.9) {
	    seconds = 0.0;
	    minutes = minutes + 1;
	    }
	if (minutes > 59) {
	    minutes = 0;
	    degrees = degrees + 1;
	    }
	(void) sprintf (tstring,"%c%02d:%02d:%04.1f",sign,degrees,minutes,seconds);
	}
    else {
	isec = (int)(seconds + 0.5);
	if (isec > 59) {
	    isec = 0;
	    minutes = minutes + 1;
	    }
	if (minutes > 59) {
	    minutes = 0;
	    degrees = degrees + 1;
	    }
	(void) sprintf (tstring,"%c%02d:%02d:%02d",sign,degrees,minutes,isec);
	}

    /* Move formatted string to returned string */
    ltstr = (int) strlen (tstring);
    if (ltstr < lstr-1)
	strcpy (string, tstring);
    else {
	strncpy (string, tstring, lstr-1);
	string[lstr-1] = 0;
	}
   return;
}


/* Write the angle a in decimal format into string */

void
deg2str (string, lstr, deg, ndec)

char	*string;	/* Character string (returned) */
int	lstr;		/* Maximum number of characters in string */
double	deg;		/* Angle in degrees */
int	ndec;		/* Number of decimal places in degree string */

{
    char degform[8];
    int field, ltstr;
    char tstring[64];
    double deg1;
    double dsgn;

    /* Keep angle between -180 and 360 degrees */
    deg1 = deg;
    if (deg1 < 0.0 ) {
	deg1 = -deg1;
	dsgn = -1.0;
	}
    else
	dsgn = 1.0;
    deg1 = fmod(deg1, 360.0);
    deg1 *= dsgn;
    if (deg1 <= -180.0)
	deg1 = deg1 + 360.0;

    /* Write angle to string, adding 4 digits to number of decimal places */
    field = ndec + 4;
    if (ndec > 0) {
	sprintf (degform, "%%%d.%df", field, ndec);
	sprintf (tstring, degform, deg1);
	}
    else {
	sprintf (degform, "%%%4d", field);
	sprintf (tstring, degform, (int)deg1);
	}

    /* Move formatted string to returned string */
    ltstr = (int) strlen (tstring);
    if (ltstr < lstr-1)
	strcpy (string, tstring);
    else {
	strncpy (string, tstring, lstr-1);
	string[lstr-1] = 0;
	}
    return;
}


/* Write the variable a in decimal format into field-character string  */

void
num2str (string, num, field, ndec)

char	*string;	/* Character string (returned) */
double	num;		/* Number */
int	field;		/* Number of characters in output field (0=any) */
int	ndec;		/* Number of decimal places in degree string */

{
    char numform[8];

    if (field > 0) {
	if (ndec > 0) {
	    sprintf (numform, "%%%d.%df", field, ndec);
	    sprintf (string, numform, num);
	    }
	else {
	    sprintf (numform, "%%%dd", field);
	    sprintf (string, numform, (int)num);
	    }
	}
    else {
	if (ndec > 0) {
	    sprintf (numform, "%%.%df", ndec);
	    sprintf (string, numform, num);
	    }
	else {
	    sprintf (string, "%d", (int)num);
	    }
	}
    return;
}
