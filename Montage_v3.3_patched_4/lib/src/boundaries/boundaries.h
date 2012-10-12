/**
    \file       boundaries.h
    \author     <a href="mailto:jcg@ipac.caltech.edu">John Good</a>
 */

/**
    \mainpage   libboundaries 
    \todo	Documentation
*/

#ifndef ISIS_BOUNDARY_LIB
#define ISIS_BOUNDARY_LIB

struct bndSkyLocation
{
   double lon;
   double lat;
   double x;
   double y;
   double z;

   double ang;

   int    vnum;
   int    delete;
};

struct bndStackCell
{
   struct bndSkyLocation *p;
   struct bndStackCell   *next;
};

extern double bndProjMatrix  [3][3];
extern double bndDeprojMatrix[3][3];

extern double bndXpix, bndYpix;
extern double bndLon,  bndLat;


void                 bndSetDebug            (int debug);

struct bndInfo      *bndVerticalBoundingBox (int n, double *lon, double *lat);
struct bndInfo      *bndBoundingBox         (int n, double *lon, double *lat);
struct bndInfo      *bndBoundingCircle      (int n, double *lon, double *lat);

int                  bndBoundaries          (int n, double *lon, 
					     double *lat, int flag);

void                 bndFree                (struct bndStackCell *t);

struct bndStackCell *bndPush                (struct bndSkyLocation *p, 
					     struct bndStackCell *top);

struct bndStackCell *bndPop                 (struct bndStackCell *s);

void                 bndPrintStack          (struct bndStackCell *t);

struct bndStackCell *bndGraham              (void);

int                  bndLeft                (struct bndSkyLocation *p1, 
                                             struct bndSkyLocation *p2, 
                                             struct bndSkyLocation *p3);

int                  bndCompare             (const void *tp1, 
					     const void *tp2);

void                 bndRemoveDeleted       ();
void                 bndInitialize          (void);
int                  ReadPoints             (void);
void                 PrintSkyPoints         (void);

void                 bndAdd                 (struct bndSkyLocation *v1, 
		   		             struct bndSkyLocation *v2, 
		   		             struct bndSkyLocation *v3);

void                 bndCross               (struct bndSkyLocation *v1, 
		   		             struct bndSkyLocation *v2, 
		   		             struct bndSkyLocation *v3);

double               bndDot                 (struct bndSkyLocation *v1,
				             struct bndSkyLocation *v2);

double               bndNormalize           (struct bndSkyLocation *v);
void                 bndReverse             (struct bndSkyLocation *v);

void                 bndCopy                (struct bndSkyLocation *v1, 
				             struct bndSkyLocation *v2);

int                  bndEqual               (struct bndSkyLocation *v1, 
				             struct bndSkyLocation *v2);

void                 bndDrawOutline         (struct bndStackCell *t);
void                 bndDrawSkyPoints       (void);
void                 bndDrawBox             ();
void                 bndDrawCircle          ();
void                 bndPrintFitsInfo       ();
void                 bndDrawImageBounds     ();

void                 bndComputeBoundingBox         (struct bndStackCell *t);
void                 bndComputeVerticalBoundingBox (struct bndStackCell *t);
void                 bndComputeBoundingCircle      (struct bndStackCell *t);


extern struct bndSkyLocation *bndPoints;

extern double bndDTR, bndPI;

extern struct bndSkyLocation Centroid;
extern struct bndSkyLocation bndCorner1, bndCorner2, bndCorner3, bndCorner4, bndCenter;
extern double bndSize, bndSize1, bndSize2, bndRadius;
extern double bndAngle;

extern int bndNpoints;
extern int bndNdelete;
extern int bndDebug;

struct bndInfo
{
   double cornerLon[4];
   double cornerLat[4];
   double centerLon;
   double centerLat;
   double lonSize;
   double latSize;
   double posAngle;
   double radius;
};

#endif /* ISIS_BOUNDARY_LIB */
