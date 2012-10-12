#ifndef ISIS_COORD_LIB


int coord_debug;


struct COORD                /* Definition of coordinate structure            */
{                           /*                                               */ 
  char sys[3];              /* Coordinate system                             */
  char clon[25], clat[25];  /* Coordinates (when expressed as char string)   */
  double lon, lat;          /* Coordinates (when expressed as a real number) */
  char fmt[6];              /* Units                                         */
  char epoch[10];           /* Epoch type and year                           */
};


/* "sys" can be one of the following:                                        */
/*                                                                           */
/*    "EQ"   -   Equatorial                                                  */
/*    "GA"   -	 Galactic                                                    */
/*    "EC"   -	 Ecliptic                                                    */
/*    "SG"   -	 Supergalactic                                               */
/*                                                                           */
/* "fmt" can be any of the following:                                        */
/*                                                                           */
/*    "DD" or "DDR"     -  Decimal Degrees (expressed as a real number)      */
/*    "DDC"             -  Decimal Degrees (expressed as a char string)      */
/*    "SEXR"            -  Sexigesimal (expressed as a real number)          */
/*    "SEX" or "SEXC"   -  Sexigesimal (expressed as a char string)          */
/*    "RAD" or "RADR"   -  Radians (expressed as a real number)              */
/*    "RADC"            -  Radians (expressed as a char string)              */
/*    "MRAD" or "MRADR" -  Milliradians (expressed as a real number)         */
/*    "MRADC"           -  Milliradians	(expressed as a char string)         */
/*    "AS" or "ASR"     -  Arc-seconds (expressed as a real number)          */
/*    "ASC"             -  Arc-seconds (expressed as a char string)          */
/*    "MAS" or "MASR"   -  Milliarcseconds (expressed as a real number)      */
/*    "MASC"            -  Milliarcseconds (expressed as a char string)      */
/*                                                                           */
/* "epoch" must start with the characters "B" or "J" followed by a           */
/*    four-digit year (e.g. "J2000", "B1950").                               */



typedef enum {
	      DD = 0,      /* 0 */
	      SEX   ,      /* 1 */
	      RAD   ,      /* 2 */
	      MRAD  ,      /* 3 */
	      AS    ,      /* 4 */
	      MAS          /* 5 */
	     }
	      CoordUnit;


typedef enum {A = 0 ,      /* 0 */
	      T     ,      /* 1 */
	      H     ,      /* 2 */
	      M            /* 3 */
	     }
	      ArcPrec;





/* ERROR codes returned by ccalc()                                           */
/*                                                                           */
#define ERR_CCALC_INVETYPE  -1 /* Invalid epoch type was specified           */
#define ERR_CCALC_INVEYEAR  -2 /* Invalid epoch year was specified           */
#define ERR_CCALC_INVSYS    -3 /* Invalid coordinate system was specified    */
#define ERR_CCALC_INVDOUBL  -4 /* Couldn't convert a value to double         */
#define ERR_CCALC_SEXCONV   -5 /* Sexigesimal conversion failed              */
#define ERR_CCALC_SEXERR    -6 /* Internal error with sexigesimal conversion */
#define ERR_CCALC_INVFMT    -7 /* Invalid format was specified               */
#define ERR_CCALC_INVPREC   -8 /* Invalid precision was specified            */
#define ERR_CCALC_INVCOORD  -9 /* Invalid coordinate value was specified     */



/* Coordinate system codes           */
/* (used in transformation routines) */

#define EQUJ      0
#define EQUB      1
#define ECLJ      2
#define ECLB      3
#define GAL       4
#define SGAL      5

#define JULIAN    0
#define BESSELIAN 1



/* Prototypes of callable functions */

void convertCoordinates();
void convertEclToEqu();
void convertEquToEcl();
void convertEquToGal();
void convertGalToEqu();
void convertGalToSgal();
void convertSgalToGal();

void convertBesselianToJulian();
void convertJulianToBesselian();
void precessBesselian();
void precessBesselianWithProperMotion();
void precessJulian();
void precessJulianWithProperMotion();
void julianToBesselianFKCorrection();
void besselianToJulianFKCorrection();

int  ccalc();
int  degreeToDMS();
int  degreeToHMS();
int  degreeToSex();
int  sexToDegree();
int  parseCoordinateString();

double roundValue();


#define ISIS_COORD_LIB
#endif
