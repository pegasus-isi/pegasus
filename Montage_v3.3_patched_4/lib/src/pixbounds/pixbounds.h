#ifndef PIXEL_BOUNDARIES

typedef double tPointi[2];

struct tPointStructure
{
   int     vnum;
   tPointi v;
   int     delete;
};

typedef struct tPointStructure tsPoint;
typedef tsPoint               *tPoint;

typedef struct tStackCell tsStack;
typedef tsStack          *tStack;

struct tStackCell
{
   struct tPointStructure  *p;
   tStack                next;
};

int     cgeomInit            ( double *x, double *y, int n );
double  cgeomGetXcen         ( );
double  cgeomGetYcen         ( );
double  cgeomGetWidth        ( );
double  cgeomGetHeight       ( );
double  cgeomGetAngle        ( );
tStack  cgeomPop             ( tStack s );
void    cgeomPrintStack      ( tStack t );
tStack  cgeomPush            ( tPoint p, tStack top );
tStack  cgeomGraham          ( void );
void    cgeomBox             ( tStack start );
void    cgeomSquash          ( void );
void    cgeomCopy            ( int i, int j );
void    cgeomPrintPostscript ( tStack t );
int     cgeomCompare         ( const void *tp1, const void *tp2 );
void    cgeomFindLowest      ( void );
int     cgeomAreaSign        ( tPointi a, tPointi b, tPointi c );
int     cgeomLeft            ( tPointi a, tPointi b, tPointi c );
int     cgeomReadPoints      ( void );
void    cgeomPrintPoints     ( void );

#define PIXEL_BOUNDARIES
#endif
