#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <math.h>
#include <string.h>

#include <coord.h>


/* define conversions between decimal degrees and radians */
#define DD2R(d) (1.74532925199432958e-02 * (d))
#define R2DD(r) (5.7295779513082321e+01 * (r))

/* define conversions between decimal degrees and arc seconds */
#define DD2AS(d) (d * 3600.)
#define AS2DD(a) (a / 3600.)

extern void convertCoordinates();

static int ExtractEpochInfo(char *from_sys, char *from_epoch, char *to_sys, 
			    char *to_epoch, int *jsysin, double *eqx1, 
			    int *jsysou, double *eqx2);
static int ConverttoDD(char *fmt, char *clon, char *clat, double *lon, 
		       double *lat);
static int ConvertfromDD(char *fmt, char *clongprec, char *clatprec, 
			 char *clon, char *clat, double *lon, double *lat);
static char *downcase(char *s);
char *strdup(const char *s1);
int ParseUnits(char *cunit, int *chflag, CoordUnit *unit);
int ParsePrec(CoordUnit units, char *cprec, int longflag);

/***************************************************************************
 * Name of Module:  ccalc() - Perform coordinate unit conversion, 
 *                            transformation and precession as specified
 *                            in ADS COORD structures.
 *                  
 * Authors:         Todd Karakashian, SAO  - top level routines, units
 *                                           conversion routines (e.g., those
 *                                           routines in this source file,
 *                                           units.c).
 *                  John Good, IPAC        - sexigesimal conversion routine
 *                                           (i.e., sexToDegree()).
 *                  Judy Bennett, IPAC     - coordinate transformation and
 *                                           precession routines (i.e., 
 *                                           convertCoordinates() and those it calls).
 *
 * Inputs:          from     - an ADS COORD structure that specifies the
 *                             input coordinate system, epoch and values.
 *                  longprec - number of decimal places of longitude to 
 *                             output for character output (ignored for
 *                             non-character output).  Is either an integer or
 *                             the string "A" (for arc second precision), 
 *                             "T" (tenth of arcsec precision), "H" (hundredth
 *                             of arc second precision) or "M" (thousandth of
 *                             arc second precision).
 *                  latprec  - number of decimal places of latitude to 
 *                             output for character output (ignored for
 *                             non-character output).  Is either an integer or
 *                             the string "A" (for arc second precision), 
 *                             "T" (tenth of arcsec precision), "H" (hundredth
 *                             of arc second precision) or "M" (thousandth of
 *                             arc second precision).
 * 
 * Input/Output:    to   - an ADS COORD structure that specifies the output
 *                         coordinate system and epoch and into which the
 *                         output values will be written.
 *                  
 * Output:          The routine returns 0 upon success, or a negative error
 *                  code in case of a failure.  For explanations of the error
 *                  codes, see ccalc.h.
 * 
 * Notes:           For documentation on the ADS COORD structure see the file
 *                  ccalc.h and the ADS Coordinate Server documentation.
 *
 *                  If the sys, epoch values of the from, to structures are
 *                  the same, only a units conversion is done.  This conversion
 *                  is two-step, done via decimal degrees.
 *
 * Overview:        The coordinate transformation and precession is
 *                  performedby the routine convertCoordinates() which operates 
 *                  on coordinates in units of decimal degrees.
 *              
 *                  The routine ExtractEpochInfo() converts the system, epoch
 *                  info into the appropriate variables expected by convertCoordinates().
 *                  The routines ConverttoDD(), ConvertfromDD() convert
 *                  to and from decimal degrees as required by convertCoordinates().
 ***************************************************************************/

int ccalc(struct COORD *from, struct COORD *to, char *longprec, char *latprec)
{
  int jsysin, jsysou;		/* convertCoordinates codes for input, output systems */
  double eqx1, eqx2;		/* equinoxes for input, output systems */
  int rv;			/* return value from calls */
  int unitsonly = 0;		/* flag indicating that only units conversion
				   is requested; i.e., input and output epoch,
				   epoch system are the same */
  double tobs = 0.0;		/* value of tobs parameter to convertCoordinates() */

  if (strcmp(from->sys,to->sys) == 0  &&  strcmp(from->epoch,to->epoch) == 0)
    unitsonly = 1;

#ifdef DEBUG
  printf("clogprec = %s, clatprec = %s\n", longprec, latprec);
#endif

  if (!unitsonly) {
    /* extract epoch information for convertCoordinates */

#ifdef DEBUG
    printf("from->sys = %s, from->epoch = %s\n", from->sys, from->epoch);
    printf("to->sys = %s, to->epoch = %s\n", to->sys, to->epoch);
#endif

    if ( (rv = ExtractEpochInfo(from->sys, from->epoch, to->sys, to->epoch, 
				&jsysin, &eqx1, &jsysou, &eqx2)) < 0)
      return rv;

#ifdef DEBUG
    printf("jsysin = %d, eqx1 = %-g\n", jsysin, eqx1);
    printf("jsysou = %d, eqx2 = %-g\n", jsysou, eqx2);
#endif
  }

  /* convert units of input longitude, latitude into decimal degrees */

#ifdef DEBUG
  printf("from->clon = %s, from->clat = %s\n", from->clon, from->clat);
  printf("from->fmt = %s\n", from->fmt);
#endif

  if ( (rv = ConverttoDD(from->fmt, from->clon, from->clat, &from->lon, 
			 &from->lat)) < 0)
    return rv;

#ifdef DEBUG
    printf("from->lon = %-g, from->lat = %-g\n", from->lon, from->lat);
#endif

  if (!unitsonly) {
    /* perform transformation and precession */

#ifdef DEBUG
    printf("from->lon = %-g, from->lat = %-g\n", from->lon, from->lat);
    printf("tobs = %-g\n", tobs);
#endif

    convertCoordinates(jsysin, eqx1, from->lon, from->lat, jsysou, eqx2, 
	   &to->lon, &to->lat, tobs);
  }
  else {			/* no transformation, precession */
    to->lon = from->lon;
    to->lat = from->lat;
  }

  /* convert units of output longitude, latitude into requested units */
  if ( (rv = ConvertfromDD(to->fmt, longprec, latprec, to->clon, to->clat, 
			   &to->lon, &to->lat)) < 0) 
    return rv;

  return 0;

}

/***************************************************************************
 * Name of Module:  ExtractEpochInfo() - extracts epoch information from 
 *                     the COORD structures and sets variables for use by 
 *                     convertCoordinates().
 *                  
 * Input:           from_sys -   A two-character string containing an 
 *                               abbreviation for the "from" coordinate 
 *                               system. 
 *                  from_epoch - A string containing the epoch of the "from"
 *                               coordinate system
 *                  to_sys -     A two-character string containing an 
 *                               abbreviation for the "to" coordinate 
 *                               system.
 *                  to_epoch   - A string containing the epoch of the "to"
 *                               coordinate system.
 *
 * Output:          jsysin     - convertCoordinates code for input coordinate system
 *                  eqx1       - epoch year of input
 *                  jsysou     - convertCoordinates code for output coordinate system
 *                  eqx2       - epoch year of output
 *
 *                  Returns 0 upon success, or a negative error code upon 
 *                  error (see ccalc.h for explanation of codes).
 ***************************************************************************/

static int ExtractEpochInfo(char *from_sys, char *from_epoch, char *to_sys, 
			    char *to_epoch, int *jsysin, double *eqx1, 
			    int *jsysou, double *eqx2)
{
  int fr_etyp = 0;		/* indicates type of "from" epoch */
  int to_etyp = 0;		/* indicates type of "to" epoch */
  char *sp;			/* pointer used in string to long conversion */

  /* convert strings to lower case */
  downcase(from_sys);
  downcase(from_epoch);
  downcase(to_sys);
  downcase(to_epoch);

  /* extract epoch information for equatorial and ecliptic coords */
  if (strcmp(from_sys,"eq") == 0 || strcmp(from_sys,"ec") == 0) {
    if (*(from_epoch) == 'j') 
      fr_etyp = JULIAN;
    else if (*(from_epoch) == 'b') 
      fr_etyp = BESSELIAN;
    else
      return ERR_CCALC_INVETYPE;
    *eqx1 = strtod(from_epoch+1,&sp);
    if (sp == (char *)NULL  ||  *sp != '\0')
      return ERR_CCALC_INVEYEAR;
  }
  if (strcmp(to_sys,"eq") == 0 || strcmp(to_sys,"ec") == 0) {
    if (*(to_epoch) == 'j') 
      to_etyp = JULIAN;
    else if (*(to_epoch) == 'b') 
      to_etyp = BESSELIAN;
    else
      return ERR_CCALC_INVETYPE;
    *eqx2 = strtod(to_epoch+1,&sp);
    if (sp == (char *)NULL  ||  *sp != '\0')
      return ERR_CCALC_INVEYEAR;
  }

  /* assign convertCoordinates system codes */
  if (strcmp(from_sys,"eq") == 0) {
    if (fr_etyp == JULIAN)
      *jsysin = EQUJ;
    else
      *jsysin = EQUB;
  }
  else if (strcmp(from_sys,"ec") == 0) {
    if (fr_etyp == JULIAN)
      *jsysin = ECLJ;
    else
      *jsysin = ECLB;
  }
  else if (strcmp(from_sys,"ga") == 0)
    *jsysin = GAL;
  else if (strcmp(from_sys,"sg") == 0)
    *jsysin = SGAL;
  else
    return ERR_CCALC_INVSYS;

  /* assign convertCoordinates system codes */
  if (strcmp(to_sys,"eq") == 0) {
    if (to_etyp == JULIAN)
      *jsysou = EQUJ;
    else
      *jsysou = EQUB;
  }
  else if (strcmp(to_sys,"ec") == 0) {
    if (to_etyp == JULIAN)
      *jsysou = ECLJ;
    else
      *jsysou = ECLB;
  }
  else if (strcmp(to_sys,"ga") == 0)
    *jsysou = GAL;
  else if (strcmp(to_sys,"sg") == 0)
    *jsysou = SGAL;
  else
    return ERR_CCALC_INVSYS;

  return 0;
}

/***************************************************************************
 * Name of Module:  ConverttoDD() - converts coordinates to decimal degrees
 *                  
 * Input:           fmt  - units of coordinates.  See Coordinate Library
 *                         Release Notes for documentation of valid units.
 *
 * Input/Output:
 *                  clon - longitude value stored as a char string
 *                  clat - latitude value stored as a char string
 *                  lon  - longitude value stored as a double
 *                  lat  - latitude value stored as a double
 *                  
 * Output:          Returns 0 upon success, or a negative error code upon 
 *                  error (see ccalc.h for explanation of codes).
 ***************************************************************************/

static int ConverttoDD(char *fmt, char *clon, char *clat, double *lon, 
		       double *lat)
{
  int chflag;
  CoordUnit unit;
  char *sp;

  if (ParseUnits(fmt, &chflag, &unit) < 0)
    return ERR_CCALC_INVFMT;

  /* convert character values of non-sexigesimal coordinates to double */
  if (chflag && unit != SEX) {
    *lon = strtod(clon,&sp);
    if (sp == (char *)NULL  ||  *sp != '\0')
      return ERR_CCALC_INVDOUBL;
    *lat = strtod(clat,&sp);
    if (sp == (char *)NULL  ||  *sp != '\0')
      return ERR_CCALC_INVDOUBL;
  }

  /* convert double values of sexigesimal coordinates to a character string */
  if (unit == SEX  &&  !chflag) {
    sprintf(clon,"%.9f",*lon);
    sprintf(clat,"%.9f",*lat);
  }

  /* perform conversions */
  switch (unit) {
    case DD:
      break;
    case MRAD:
      *lon /= 1000.;
      *lat /= 1000.;
    case RAD: 
      *lon = R2DD(*lon);
      *lat = R2DD(*lat);
      break;
    case MAS:
      *lon /= 1000.;
      *lat /= 1000.;
    case AS:
      *lon = AS2DD(*lon);
      *lat = AS2DD(*lat);
      break;
    case SEX:
      if (sexToDegree(clon, clat, lon, lat) != 0)
	return ERR_CCALC_SEXCONV;
      break;
  }

  while (*lon < 0)	/* convert negative RA to principal value region */
    *lon += 360.0;
  while (*lon > 360.0)		/* convert RA to principal value region */
    *lon -= 360.0;

  if (*lat < -90.0  ||  *lat > 90.0) /* disallow invalid latitude values */
    return ERR_CCALC_INVCOORD;

  return 0;
}

/***************************************************************************
 * Name of Module:  ConvertfromDD() - Convert decimal degrees to desired
 *                                    output units
 *                  
 * Inputs:          fmt - desired output units of coordinates.  See Coordinate
 *                         Library Release Notes for documentation of valid 
 *                         units.
 *                  longprec - number of decimal places of output for longitude
 *                  latprec - number of decimal places of output for latitude
 *            
 * Input/Output:
 *                  clon - longitude value stored as a char string
 *                  clat - latitude value stored as a char string
 *                  lon  - longitude value stored as a double
 *                  lat  - latitude value stored as a double
 *                  
 * Output:          Returns 0 upon success, or a negative error code upon 
 *                  error (see ccalc.h for explanation of codes).
 ***************************************************************************/

static int ConvertfromDD(char *fmt, char *clongprec, char *clatprec, 
			 char *clon, char *clat, double *lon, double *lat)
{
  CoordUnit unit;
  int deg, hr, hmin, dmin, sign; /* used in dd to sexigesimal conversion */
  double hsec, dsec;		/* used in dd to sexigesimal conversion */
  int longprec, latprec;

  if (ParseUnits(fmt, (int *)NULL, &unit) < 0)
    return ERR_CCALC_INVFMT;

  if ( (longprec = ParsePrec(unit,clongprec,1)) < 0)
      return ERR_CCALC_INVPREC;
  if ( (latprec = ParsePrec(unit,clatprec,0)) < 0)
      return ERR_CCALC_INVPREC;

#ifdef DEBUG
  printf("longprec = %d, latprec = %d\n", longprec, latprec);
#endif

  /* perform conversions to output units */
  switch (unit) {
    case DD:
      break;
    case RAD: 
      *lon = DD2R(*lon);
      *lat = DD2R(*lat);
      break;
    case MRAD:
      *lon = DD2R(*lon)*1000.;
      *lat = DD2R(*lat)*1000.;
      break;
    case AS:
      *lon = DD2AS(*lon);
      *lat = DD2AS(*lat);
      break;
    case MAS:
      *lon = DD2AS(*lon)*1000.;
      *lat = DD2AS(*lat)*1000.;
      break;
    case SEX:
      if (degreeToHMS(*lon,longprec,&sign,&hr,&hmin,&hsec) < 0)
	return ERR_CCALC_SEXCONV;
      if (longprec == 0)
	sprintf(clon,"%s%02dh %02dm %02.0fs", (sign? "-":""), hr, hmin, hsec); 
      else
	sprintf(clon,"%s%02dh %02dm %0*.*fs", (sign? "-":""), hr, hmin,
		longprec+3, longprec, hsec); 
      *lon = hr*10000.+hmin*100.+hsec;
      if (sign)
	*lon *= -1.0;

      if (degreeToDMS(*lat,latprec,&sign,&deg,&dmin,&dsec) < 0)
	return ERR_CCALC_SEXCONV;
      if (latprec == 0)
	sprintf(clat,"%s%02dd %02dm %02.0fs", (sign? "-":"+"), deg, dmin, 
		dsec); 
      else
	sprintf(clat,"%s%02dd %02dm %0*.*fs", (sign? "-":"+"), deg, dmin,
		latprec+3, latprec, dsec); 
      *lat = deg*10000.+dmin*100.+dsec;
      if (sign)
	*lat *= -1.0;
      break;
  }

  /* write output values in character strings as well */
  if (unit != SEX) {
    *lon = roundValue(*lon,longprec);
    sprintf(clon,"%.*f", longprec, *lon);
    *lat = roundValue(*lat,latprec);
    sprintf(clat,"%+.*f", latprec, *lat);
  }

  return 0;
}


static char *downcase(char *s)
/* convert a string to lower case */
{
  char *sp;

  for (sp = s; *sp != '\0'; ++sp)
    if (isupper((int) *sp))
      *sp = tolower((int) *sp);

  return s;
}


int ParseUnits(char *cunit, int *chflag, CoordUnit *unit)
{
  static char *cun = (char *)NULL;

  int cflag;

  if(cun)
     free(cun);

  cun = downcase(strdup(cunit));

  /* set of value of unit, chflag for switch statement */
  if (strcmp(cun,"dd") == 0  ||  strcmp(cun,"ddr") == 0) {
    *unit = DD;
    cflag = 0;
  }
  else if (strcmp(cun,"sexr") == 0) {
    *unit = SEX;
    cflag = 0;
  }
  else if (strcmp(cun,"rad") == 0  ||  strcmp(cun,"radr") == 0) {
    *unit = RAD;
    cflag = 0;
  }
  else if (strcmp(cun,"mrad") == 0  ||  strcmp(cun,"mradr") == 0) {
    *unit = MRAD;
    cflag = 0;
  }
  else if (strcmp(cun,"as") == 0  ||  strcmp(cun,"asr") == 0) {
    *unit = AS;
    cflag = 0;
  }
  else if (strcmp(cun,"mas") == 0  ||  strcmp(cun,"masr") == 0) {
    *unit = MAS;
    cflag = 0;
  }
  else if (strcmp(cun,"ddc") == 0) {
    *unit = DD;
    cflag = 1;
  }
  else if (strcmp(cun,"sex") == 0  ||  strcmp(cun,"sexc") == 0) {
    *unit = SEX;
    cflag = 1;
  }
  else if (strcmp(cun,"radc") == 0) {
    *unit = RAD;
    cflag = 1;
  }
  else if (strcmp(cun,"mradc") == 0) {
    *unit = MRAD;
    cflag = 1;
  }
  else if (strcmp(cun,"asc") == 0)  {
    *unit = AS;
    cflag = 1;
  }
  else if (strcmp(cun,"masc") == 0) {
    *unit = MAS;
    cflag = 1;
  }
  else
    return -1;

  if (chflag != (int *)NULL)
    *chflag = cflag;
  return 0;
}


int ParsePrec(CoordUnit units, char *cprec, int longflag)
{
  static int prectab[7][4] = {

/*             A      T      H      M     */
/*                   tenth  hundr   milli */
/*             as     as     as      as   */
/* DD     */   { 4,     5,      6,     7 },
/* SEX    */   { 0,     1,      2,     3 }, 
/* RAD    */   { 6,     7,      8,     9 }, 
/* MRAD   */   { 3,     4,      5,     6 },
/* AS     */   { 0,     1,      2,     3 },
/* MAS    */   { 0,     0,      0,     0 }
  };

  static char *cpre = (char *)NULL;

  int prec;
  ArcPrec aprec;
  char *sp;

  if(cpre)
     free(cpre);

  cpre = downcase(strdup(cprec));

  prec = strtol(cpre,&sp,10);
  if (sp != (char *)NULL  &&  *sp == '\0')
    return prec;

  if (strcmp(cpre,"a") == 0)
    aprec = A;
  else if (strcmp(cpre,"t") == 0)
    aprec = T;
  else if (strcmp(cpre,"h") == 0)
    aprec = H;
  else if (strcmp(cpre,"m") == 0)
    aprec = M;
  else
    return -1;

  prec = prectab[(int)units][(int)aprec];
  if (units == SEX  &&  longflag) /* sex longitude gets one higher precision */
    prec++;

  return prec;
}

/***************************************************************************
 ***************** FOR DEC machines only ***********************************
 ***************************************************************************/
#ifdef DEC

/***************************************************************************
 * Name of Module:  strdup - duplicate a string
 *                  
 * Description:  This mallocs space to hold another copy of a string and
 *               then copies the string.
 * 
 * Inputs:       string  - a string
 *
 * Output:       Returns a pointer to the new copy of the string, or NULL
 *               if the string was NULL or if the duplication failed.
 ***************************************************************************/

static char *strdup(char *string)
{
  char *tmp;
   
  if (string == (char *)NULL)
    return (char *)NULL;
  if ( (tmp = (char *)malloc(strlen(string)+1)) == (char *)NULL)
     return (char *)NULL;
  else {
    strcpy(tmp,string);
    return tmp;
  }
}
/***************************************************************************
 ***************************************************************************
 ***************************************************************************/
#endif
