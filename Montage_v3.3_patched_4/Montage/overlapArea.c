/* Module: overlapArea.c

Version  Developer        Date     Change
-------  ---------------  -------  -----------------------
1.2      John Good        03Nov04  Make sure nv never exceeds max (16)
1.1      John Good        25Nov03  Don't exit if segments are disjoint
1.0      John Good        29Jan03  Baseline code

*/

#include <stdio.h>
#include <math.h>

#include "mNaN.h"

#define FALSE             0
#define TRUE              1
#define FOREVER           1

#define COLINEAR_SEGMENTS 0
#define ENDPOINT_ONLY     1
#define NORMAL_INTERSECT  2
#define NO_INTERSECTION   3

#define CLOCKWISE         1
#define PARALLEL          0
#define COUNTERCLOCKWISE -1

#define UNKNOWN           0
#define P_IN_Q            1
#define Q_IN_P            2


/* The two pixel polygons on the sky */
/* and the polygon of intersection   */

typedef struct vec
{
   double x;
   double y;
   double z;
}
Vec;

Vec P[8], Q[8], V[16];

int np = 4;
int nq = 4;
int nv;


double  computeOverlap      (double *ilon, double *ilat,
                             double *olon, double *olat,
			     int energyMode, double refArea, double *areaRatio);

int     DirectionCalculator (Vec *a, Vec *b, Vec *c);
int     SegSegIntersect     (Vec *a, Vec *b, Vec *c, Vec *d, 
                             Vec *e, Vec *f, Vec *p, Vec *q);
int     Between             (Vec *a, Vec *b, Vec *c);
int     Cross               (Vec *a, Vec *b, Vec *c);
double  Dot                 (Vec *a, Vec *b);
double  Normalize           (Vec *a);
void    Reverse             (Vec *a);
void    SaveVertex          (Vec *a);
void    SaveSharedSeg       (Vec *p, Vec *q);
void    PrintPolygon        ();

void    ComputeIntersection (Vec *P, Vec *Q);

int     UpdateInteriorFlag  (Vec *p, int interiorFlag, 
                             int pEndpointFromQdir,
			     int qEndpointFromPdir);

int     Advance             (int i, int *i_advances, 
			     int n, int inside, Vec *v);

double  Girard();
void    RemoveDups();

int     printDir           (char *point, char *vector, int dir);

extern int  debug;

extern int  inRow,  inColumn;
extern int  outRow, outColumn;


double  pi, dtr;

double tolerance = 4.424e-9;  /* sin(x) where x = 5e-4 arcsec */
                                  /* or cos(x) when x is within   */
                                  /* 1e-5 arcsec of 90 degrees    */


/***************************************************/
/*                                                 */
/* Simple main() program; reads the data and calls */
/* the intersection code.                          */
/*                                                 */
/***************************************************/

/***************** ONLY FOR DEBUGGING **********************

main(int argc, char **argv)
{
   double area;
   double ilon[4], ilat[4];
   double olon[4], olat[4];

   if(argc > 1)
      debug = 4;

   if(debug >= 4)
   {
      printf("\n");
      printf("-----\n");
      printf("Intersection Codes:\n");
      printf("------------------\n");
      printf("    0 (COLINEAR_SEGMENTS) :");
      printf("The segments colinearly overlap, sharing a point.\n");
      printf("\n");
      printf("    1 (ENDPOINT_ONLY)     : An endpoint (vertex) of one\n");
      printf("                            segment is on the other, but\n");
      printf("                            COLINEAR_SEGMENTS doesn't hold.\n");
      printf("\n");
      printf("    2 (NORMAL_INTERSECT)  : The segments intersect properly\n");
      printf("                            (i.e., they share a point and\n");
      printf("                            neither ENDPOINT_ONLY nor ");
      printf("                            COLINEAR_SEGMENTS holds).\n");
      printf("\n");
      printf("    3 (NO_INTERSECTION)   : The segments do not intersect \n");
      printf("                            (i.e., they share no points).\n");
      printf("\n");
      printf("interiorFlag Values:\n");
      printf("------------------- \n");
      printf("0 P_IN_Q\n");
      printf("1 Q_IN_P\n");
      printf("2 UNKNOWN\n");
      printf("\n");
      printf("Direction Codes:\n");
      printf("--------------- \n");
      printf(" 1 COUNTERCLOCKWISE\n");
      printf(" 0 PARALLEL\n");
      printf("-1 CLOCKWISE\n");
      printf("\n");
   }

   ReadData(ilon, ilat, olon, olat);

   area = computeOverlap(ilon, ilat, olon, olat);

   printf("\nArea = %-g\n\n", area);

   PrintPolygon();
}

****************** ONLY FOR DEBUGGING *********************/



/***************************************************/
/*                                                 */
/* computeOverlap()                                */
/*                                                 */
/* Sets up the polygons, runs the overlap          */
/* computation, and returns the area of overlap.   */
/*                                                 */
/***************************************************/

double computeOverlap(double *ilon, double *ilat,
                      double *olon, double *olat, 
		      int energyMode, double refArea, double *areaRatio)
{
   int    i;
   double thisPixelArea;

   pi  = atan(1.0) * 4.;
   dtr = pi / 180.;


   *areaRatio = 1.;

   if(energyMode)
   {
      nv = 0;

      for(i=0; i<4; ++i)
      SaveVertex(&P[i]);

      thisPixelArea = Girard();

      *areaRatio = thisPixelArea / refArea;
   }


   nv = 0;

   if(debug >= 4)
   {
      printf("\n-----------------------------------------------\n\nAdding pixel (%d,%d) to pixel (%d,%d)\n\n",
         inRow, inColumn, outRow, outColumn);

      printf("Input (P):\n");
      for(i=0; i<4; ++i)
         printf("%10.6f %10.6f\n", ilon[i], ilat[i]);

      printf("\nOutput (Q):\n");
      for(i=0; i<4; ++i)
         printf("%10.6f %10.6f\n", olon[i], olat[i]);

      printf("\n");
      fflush(stdout);
   }

   for(i=0; i<4; ++i)
   {
      P[i].x = cos(ilon[i]*dtr) * cos(ilat[i]*dtr);
      P[i].y = sin(ilon[i]*dtr) * cos(ilat[i]*dtr);
      P[i].z = sin(ilat[i]*dtr);
   }

   for(i=0; i<4; ++i)
   {
      Q[i].x = cos(olon[i]*dtr) * cos(olat[i]*dtr);
      Q[i].y = sin(olon[i]*dtr) * cos(olat[i]*dtr);
      Q[i].z = sin(olat[i]*dtr);
   }

   ComputeIntersection(P, Q);

   return(Girard());
}



/***************************************************/
/*                                                 */
/* ComputeIntersection()                           */
/*                                                 */
/* Find the polygon defining the area of overlap   */
/* between the two input polygons.                 */
/*                                                 */
/***************************************************/

void  ComputeIntersection(Vec *P, Vec *Q)
{
   Vec  Pdir, Qdir;             /* "Current" directed edges on P and Q   */
   Vec  other;                  /* Temporary "edge-like" variable        */
   int  ip, iq;                 /* Indices of ends of Pdir, Qdir         */
   int  ip_begin, iq_begin;     /* Indices of beginning of Pdir, Qdir    */
   int  PToQDir;                /* Qdir direction relative to Pdir       */
                                /* (e.g. CLOCKWISE)                      */
   int  qEndpointFromPdir;      /* End P vertex as viewed from beginning */
                                /* of Qdir relative to Qdir              */
   int  pEndpointFromQdir;      /* End Q vertex as viewed from beginning */
                                /* of Pdir relative to Pdir              */
   Vec  firstIntersection;      /* Point of intersection of Pdir, Qdir   */
   Vec  secondIntersection;     /* Second point of intersection          */
                                /* (if there is one)                     */
   int  interiorFlag;           /* Which polygon is inside the other     */
   int  contained;              /* Used for "completely contained" check */
   int  p_advances, q_advances; /* Number of times we've advanced        */
                                /* P and Q indices                       */
   int  isFirstPoint;           /* Is this the first point?              */
   int  intersectionCode;       /* SegSegIntersect() return code.        */ 


   /* Check for Q contained in P */

   contained = TRUE;

   for(ip=0; ip<np; ++ip)
   {
      ip_begin = (ip + np - 1) % np;

      Cross(&P[ip_begin], &P[ip], &Pdir);
      Normalize(&Pdir);

      for(iq=0; iq<nq; ++iq)
      {
	 if(debug >= 4)
	 {
	    printf("Q in P: Dot%d%d = %12.5e\n", ip, iq, Dot(&Pdir, &Q[iq]));
	    fflush(stdout);
	 }

	 if(Dot(&Pdir, &Q[iq]) < -tolerance)
	 {
	    contained = FALSE;
	    break;
	 }
      }

      if(!contained)
	 break;
   }

   if(contained)
   {
      if(debug >= 4)
      {
	 printf("Q is entirely contained in P (output pixel is in input pixel)\n");
	 fflush(stdout);
      }

      for(iq=0; iq<nq; ++iq)
	 SaveVertex(&Q[iq]);
      
      return;
   }


   /* Check for P contained in Q */

   contained = TRUE;

   for(iq=0; iq<nq; ++iq)
   {
      iq_begin = (iq + nq - 1) % nq;

      Cross(&Q[iq_begin], &Q[iq], &Qdir);
      Normalize(&Qdir);

      for(ip=0; ip<np; ++ip)
      {
	 if(debug >= 4)
	 {
	    printf("P in Q: Dot%d%d = %12.5e\n", iq, ip, Dot(&Qdir, &P[ip]));
	    fflush(stdout);
	 }

	 if(Dot(&Qdir, &P[ip]) < -tolerance)
	 {
	    contained = FALSE;
	    break;
	 }
      }

      if(!contained)
	 break;
   }

   if(contained)
   {
      if(debug >= 4)
      {
	 printf("P is entirely contained in Q (input pixel is in output pixel)\n");
	 fflush(stdout);
      }

      nv = 0;
      for(ip=0; ip<np; ++ip)
	 SaveVertex(&P[ip]);
      
      return;
   }


   /* Then check for polygon overlap */

   ip = 0;
   iq = 0;

   p_advances = 0;
   q_advances = 0;

   interiorFlag = UNKNOWN;
   isFirstPoint = TRUE;

   while(FOREVER)
   {
      if(p_advances >= 2*np) break;
      if(q_advances >= 2*nq) break;
      if(p_advances >= np && q_advances >= nq) break;

      if(debug >= 4)
      {
         printf("-----\n");

	 if(interiorFlag == UNKNOWN)
	 {
	    printf("Before advances (UNKNOWN interiorFlag): ip=%d, iq=%d ", ip, iq);
	    printf("(p_advances=%d, q_advances=%d)\n", 
	       p_advances, q_advances);
	 }

	 else if(interiorFlag == P_IN_Q)
	 {
	    printf("Before advances (P_IN_Q): ip=%d, iq=%d ", ip, iq);
	    printf("(p_advances=%d, q_advances=%d)\n", 
	       p_advances, q_advances);
	 }

	 else if(interiorFlag == Q_IN_P)
	 {
	    printf("Before advances (Q_IN_P): ip=%d, iq=%d ", ip, iq);
	    printf("(p_advances=%d, q_advances=%d)\n", 
	       p_advances, q_advances);
	 }
	 else
	    printf("\nBAD INTERIOR FLAG.  Shouldn't get here\n");
            
	 fflush(stdout);
      }


      /* Previous point in the polygon */

      ip_begin = (ip + np - 1) % np;
      iq_begin = (iq + nq - 1) % nq;


      /* The current polygon edges are given by  */
      /* the cross product of the vertex vectors */

      Cross(&P[ip_begin], &P[ip], &Pdir);
      Cross(&Q[iq_begin], &Q[iq], &Qdir);

      PToQDir = DirectionCalculator(&P[ip], &Pdir, &Qdir);

      Cross(&Q[iq_begin], &P[ip], &other);
      pEndpointFromQdir = DirectionCalculator(&Q[iq_begin], &Qdir, &other);

      Cross(&P[ip_begin], &Q[iq], &other);
      qEndpointFromPdir = DirectionCalculator(&P[ip_begin], &Pdir, &other);

      if(debug >= 4)
      {
         printf("   ");
	 printDir("P", "Q", PToQDir);
	 printDir("pEndpoint", "Q", pEndpointFromQdir);
	 printDir("qEndpoint", "P", qEndpointFromPdir);
         printf("\n");
	 fflush(stdout);
      }


      /* Find point(s) of intersection between edges */

      intersectionCode = SegSegIntersect(&Pdir,      &Qdir, 
                                         &P[ip_begin], &P[ip],
                                         &Q[iq_begin], &Q[iq], 
                                         &firstIntersection, 
                                         &secondIntersection);

      if(intersectionCode == NORMAL_INTERSECT 
      || intersectionCode == ENDPOINT_ONLY) 
      {
         if(interiorFlag == UNKNOWN && isFirstPoint) 
         {
            p_advances = 0;
            q_advances = 0;

            isFirstPoint = FALSE;
         }

         interiorFlag = UpdateInteriorFlag(&firstIntersection, interiorFlag, 
                                           pEndpointFromQdir, qEndpointFromPdir);

         if(debug >= 4)
	 {
	    if(interiorFlag == UNKNOWN)
	       printf("   interiorFlag -> UNKNOWN\n");

	    else if(interiorFlag == P_IN_Q)
	       printf("   interiorFlag -> P_IN_Q\n");

	    else if(interiorFlag == Q_IN_P)
	       printf("   interiorFlag -> Q_IN_P\n");

	    else 
	       printf("   BAD interiorFlag.  Shouldn't get here\n");

	    fflush(stdout);
	 }
      }


      /*-----Advance rules-----*/


      /* Special case: Pdir & Qdir overlap and oppositely oriented. */

      if((intersectionCode == COLINEAR_SEGMENTS)
      && (Dot(&Pdir, &Qdir) < 0))
      {
         if(debug >= 4)
	 {
            printf("   ADVANCE: Pdir and Qdir are colinear.\n");
	    fflush(stdout);
	 }

         SaveSharedSeg(&firstIntersection, &secondIntersection);

	 RemoveDups();
	 return;
      }


      /* Special case: Pdir & Qdir parallel and separated. */
      
      if((PToQDir          == PARALLEL) 
      && (pEndpointFromQdir == CLOCKWISE) 
      && (qEndpointFromPdir == CLOCKWISE))
      {
         if(debug >= 4)
	 {
            printf("   ADVANCE: Pdir and Qdir are disjoint.\n");
	    fflush(stdout);
	 }

	 RemoveDups();
	 return;
      }


      /* Special case: Pdir & Qdir colinear. */

      else if((PToQDir          == PARALLEL) 
           && (pEndpointFromQdir == PARALLEL) 
           && (qEndpointFromPdir == PARALLEL)) 
      {
         if(debug >= 4)
	 {
            printf("   ADVANCE: Pdir and Qdir are colinear.\n");
	    fflush(stdout);
	 }


         /* Advance but do not output point. */

         if(interiorFlag == P_IN_Q)
            iq = Advance(iq, &q_advances, nq, interiorFlag == Q_IN_P, &Q[iq]);
         else
            ip = Advance(ip, &p_advances, np, interiorFlag == P_IN_Q, &P[ip]);
      }


      /* Generic cases. */

      else if(PToQDir == COUNTERCLOCKWISE 
	   || PToQDir == PARALLEL)
      {
         if(qEndpointFromPdir == COUNTERCLOCKWISE)
         {
            if(debug >= 4)
	    {
               printf("   ADVANCE: Generic: PToQDir is COUNTERCLOCKWISE ");
	       printf("|| PToQDir is PARALLEL, ");
	       printf("qEndpointFromPdir is COUNTERCLOCKWISE\n");
	       fflush(stdout);
	    }

            ip = Advance(ip, &p_advances, np, interiorFlag == P_IN_Q, &P[ip]);
         }
         else
         {
            if(debug >= 4)
            {
               printf("   ADVANCE: Generic: PToQDir is COUNTERCLOCKWISE ");
               printf("|| PToQDir is PARALLEL, qEndpointFromPdir is CLOCKWISE\n");
	       fflush(stdout);
            }

            iq = Advance(iq, &q_advances, nq, interiorFlag == Q_IN_P, &Q[iq]);
         }
      }

      else 
      {
         if(pEndpointFromQdir == COUNTERCLOCKWISE)
         {
            if(debug >= 4)
            {
               printf("   ADVANCE: Generic: PToQDir is CLOCKWISE, ");
               printf("pEndpointFromQdir is COUNTERCLOCKWISE\n");
	       fflush(stdout);
            }

            iq = Advance(iq, &q_advances, nq, interiorFlag == Q_IN_P, &Q[iq]);
         }
         else
         {
            if(debug >= 4)
            {
               printf("   ADVANCE: Generic: PToQDir is CLOCKWISE, ");
               printf("pEndpointFromQdir is CLOCKWISE\n");
	       fflush(stdout);
            }

            ip = Advance(ip, &p_advances, np, interiorFlag == P_IN_Q, &P[ip]);
         }
      }

      if(debug >= 4)
      {
	 if(interiorFlag == UNKNOWN)
	 {
	    printf("After  advances: ip=%d, iq=%d ", ip, iq);
	    printf("(p_advances=%d, q_advances=%d) interiorFlag=UNKNOWN\n", 
	       p_advances, q_advances);
	 }

	 else if(interiorFlag == P_IN_Q)
	 {
	    printf("After  advances: ip=%d, iq=%d ", ip, iq);
	    printf("(p_advances=%d, q_advances=%d) interiorFlag=P_IN_Q\n", 
	       p_advances, q_advances);
	 }

	 else if(interiorFlag == Q_IN_P)
	 {
	    printf("After  advances: ip=%d, iq=%d ", ip, iq);
	    printf("(p_advances=%d, q_advances=%d) interiorFlag=Q_IN_P\n", 
	       p_advances, q_advances);
	 }
	 else
	    printf("BAD INTERIOR FLAG.  Shouldn't get here\n");

         printf("-----\n\n");
	 fflush(stdout);
      }
   }


   RemoveDups();
   return;
}




/***************************************************/
/*                                                 */
/* UpdateInteriorFlag()                            */
/*                                                 */
/* Print out the second point of intersection      */
/* and toggle in/out flag.                         */
/*                                                 */
/***************************************************/

int UpdateInteriorFlag(Vec *p, int interiorFlag, 
                       int pEndpointFromQdir, int qEndpointFromPdir)
{
   double lon, lat;

   if(debug >= 4)
   {
      lon = atan2(p->y, p->x)/dtr;
      lat = asin(p->z)/dtr;

      printf("   intersection [%13.6e,%13.6e,%13.6e]  -> (%10.6f,%10.6f) (UpdateInteriorFlag)\n",
         p->x, p->y, p->z, lon, lat);
      fflush(stdout);
   }

   SaveVertex(p);


   /* Update interiorFlag. */

   if(pEndpointFromQdir == COUNTERCLOCKWISE)
      return P_IN_Q;

   else if(qEndpointFromPdir == COUNTERCLOCKWISE)
      return Q_IN_P;

   else /* Keep status quo. */
      return interiorFlag;
}


/***************************************************/
/*                                                 */
/* SaveSharedSeg()                                 */
/*                                                 */
/* Save the endpoints of a shared segment.         */
/*                                                 */
/***************************************************/

void SaveSharedSeg(Vec *p, Vec *q)
{
   if(debug >= 4)
   {
      printf("\n   SaveSharedSeg():  from [%13.6e,%13.6e,%13.6e]\n",
         p->x, p->y, p->z);

      printf("   SaveSharedSeg():  to   [%13.6e,%13.6e,%13.6e]\n\n",
         q->x, q->y, q->z);

      fflush(stdout);
   }

   SaveVertex(p);
   SaveVertex(q);
}



/***************************************************/
/*                                                 */
/* Advance()                                       */
/*                                                 */
/* Advances and prints out an inside vertex if     */
/* appropriate.                                    */
/*                                                 */
/***************************************************/

int Advance(int ip, int *p_advances, int n, int inside, Vec *v)
{
   double lon, lat;

   lon = atan2(v->y, v->x)/dtr;
   lat = asin(v->z)/dtr;

   if(inside)
   {
      if(debug >= 4)
      {
         printf("   Advance(): inside vertex [%13.6e,%13.6e,%13.6e] -> (%10.6f,%10.6f)n",
            v->x, v->y, v->z, lon, lat);

	 fflush(stdout);
      }

      SaveVertex(v);
   }

   (*p_advances)++;

   return (ip+1) % n;
}



/***************************************************/
/*                                                 */
/* SaveVertex()                                    */
/*                                                 */
/* Save the intersection polygon vertices          */
/*                                                 */
/***************************************************/

void SaveVertex(Vec *v)
{
   int i, i_begin;
   Vec Dir;

   if(debug >= 4)
      printf("   SaveVertex ... ");

   /* What with tolerance and roundoff    */
   /* problems, we need to double check   */
   /* that the point to be save is really */
   /* in or on the edge of both pixels    */
   /* P and Q                             */

   for(i=0; i<np; ++i)
   {
      i_begin = (i + np - 1) % np;

      Cross(&P[i_begin], &P[i], &Dir);
      Normalize(&Dir);

      if(Dot(&Dir, v) < -1000.*tolerance)
      {
	 if(debug >= 4)
	 {
	    printf("rejected (not in P)\n");
	    fflush(stdout);
	 }

	 return;
      }
   }


   for(i=0; i<nq; ++i)
   {
      i_begin = (i + nq - 1) % nq;

      Cross(&Q[i_begin], &Q[i], &Dir);
      Normalize(&Dir);

      if(Dot(&Dir, v) < -1000.*tolerance)
      {
	 if(debug >= 4)
	 {
	    printf("rejected (not in Q)\n");
	    fflush(stdout);
	 }

	 return;
      }
   }


   if(nv < 15)
   {
      V[nv].x = v->x;
      V[nv].y = v->y;
      V[nv].z = v->z;

      ++nv;
   }

   if(debug >= 4)
   {
      printf("accepted (%d)\n", nv);
      fflush(stdout);
   }
}



/***************************************************/
/*                                                 */
/* PrintPolygon()                                  */
/*                                                 */
/* Print out the final intersection polygon        */
/*                                                 */
/***************************************************/

void PrintPolygon()
{
   int    i;
   double lon, lat;

   for(i=0; i<nv; ++i)
   {
      lon = atan2(V[i].y, V[i].x)/dtr;
      lat = asin(V[i].z)/dtr;

      printf("[%13.6e,%13.6e,%13.6e] -> (%10.6f,%10.6f)\n", 
         V[i].x, V[i].y, V[i].z, lon, lat);
   }
}


/***************************************************/
/*                                                 */
/* ReadData()                                      */
/*                                                 */
/* Reads in the coordinates of the vertices of     */
/* the polygons from stdin                         */
/*                                                 */
/***************************************************/

int ReadData(double *ilon, double *ilat, 
             double *olon, double *olat)
{
   int    n;

   n = 0;
   while((n < 4) && (scanf("%lf %lf",&ilon[n], &ilat[n]) != EOF)) 
      ++n;

   n = 0;
   while((n < 4) && (scanf("%lf %lf",&olon[n], &olat[n]) != EOF)) 
      ++n;

   return(0);
}



/***************************************************/
/*                                                 */
/* DirectionCalculator()                           */
/*                                                 */
/* Computes whether ac is CLOCKWISE, etc. of ab    */
/*                                                 */
/***************************************************/

int DirectionCalculator(Vec *a, Vec *b, Vec *c)
{
   Vec cross;
   int len;

   len = Cross(b, c, &cross);

   if(len == 0)                 return PARALLEL;
   else if(Dot(a, &cross) < 0.) return CLOCKWISE;
   else                         return COUNTERCLOCKWISE;
}


/****************************************************************************/
/*                                                                          */
/* SegSegIntersect()                                                        */
/*                                                                          */
/* Finds the point of intersection p between two closed                     */
/* segments ab and cd.  Returns p and a char with the following meaning:    */
/*                                                                          */
/*   COLINEAR_SEGMENTS: The segments colinearly overlap, sharing a point.   */
/*                                                                          */
/*   ENDPOINT_ONLY:     An endpoint (vertex) of one segment is on the other */
/*                      segment, but COLINEAR_SEGMENTS doesn't hold.        */
/*                                                                          */
/*   NORMAL_INTERSECT:  The segments intersect properly (i.e., they share   */
/*                      a point and neither ENDPOINT_ONLY nor               */
/*                      COLINEAR_SEGMENTS holds).                           */
/*                                                                          */
/*   NO_INTERSECTION:   The segments do not intersect (i.e., they share     */
/*                      no points).                                         */
/*                                                                          */
/* Note that two colinear segments that share just one point, an endpoint   */
/* of each, returns COLINEAR_SEGMENTS rather than ENDPOINT_ONLY as one      */
/* might expect.                                                            */
/*                                                                          */
/****************************************************************************/

int SegSegIntersect(Vec *pEdge, Vec *qEdge, 
                    Vec *p0, Vec *p1, Vec *q0, Vec *q1, 
                    Vec *intersect1, Vec *intersect2)
{
   double pDot,  qDot;  /* Dot product [cos(length)] of the edge vertices */
   double p0Dot, p1Dot; /* Dot product from vertices to intersection      */
   double q0Dot, q1Dot; /* Dot pro}duct from vertices to intersection     */
   int    len;


   /* Get the edge lengths (actually cos(length)) */

   pDot = Dot(p0, p1);
   qDot = Dot(q0, q1);


   /* Find the point of intersection */

   len = Cross(pEdge, qEdge, intersect1);


   /* If the two edges are colinear, */ 
   /* check to see if they overlap   */

   if(len == 0)
   {
      if(Between(q0, p0, p1)
      && Between(q1, p0, p1))
      {
         intersect1 = q0;
         intersect2 = q1;
         return COLINEAR_SEGMENTS;
      }

      if(Between(p0, q0, q1)
      && Between(p1, q0, q1))
      {
         intersect1 = p0;
         intersect2 = p1;
         return COLINEAR_SEGMENTS;
      }

      if(Between(q0, p0, p1)
      && Between(p1, q0, q1))
      {
         intersect1 = q0;
         intersect2 = p1;
         return COLINEAR_SEGMENTS;
      }

      if(Between(p0, q0, q1)
      && Between(q1, p0, p1))
      {
         intersect1 = p0;
         intersect2 = q1;
         return COLINEAR_SEGMENTS;
      }

      if(Between(q1, p0, p1)
      && Between(p1, q0, q1))
      {
         intersect1 = p0;
         intersect2 = p1;
         return COLINEAR_SEGMENTS;
      }

      if(Between(q0, p0, p1)
      && Between(p0, q0, q1))
      {
         intersect1 = p0;
         intersect2 = q0;
         return COLINEAR_SEGMENTS;
      }

      return NO_INTERSECTION;
   }


   /* If this is the wrong one of the two */
   /* (other side of the sky) reverse it  */

   Normalize(intersect1);

   if(Dot(intersect1, p0) < 0.)
      Reverse(intersect1);


   /* Point has to be inside both sides to be an intersection */

   if((p0Dot = Dot(intersect1, p0)) <  pDot) return NO_INTERSECTION;
   if((p1Dot = Dot(intersect1, p1)) <  pDot) return NO_INTERSECTION;
   if((q0Dot = Dot(intersect1, q0)) <  qDot) return NO_INTERSECTION;
   if((q1Dot = Dot(intersect1, q1)) <  qDot) return NO_INTERSECTION;


   /* Otherwise, if the intersection is at an endpoint */

   if(p0Dot == pDot) return ENDPOINT_ONLY;
   if(p1Dot == pDot) return ENDPOINT_ONLY;
   if(q0Dot == qDot) return ENDPOINT_ONLY;
   if(q1Dot == qDot) return ENDPOINT_ONLY;


   /* Otherwise, it is a normal intersection */

   return NORMAL_INTERSECT;
}



/***************************************************/
/*                                                 */
/* printDir()                                      */
/*                                                 */
/* Formats a message about relative directions.    */
/*                                                 */
/***************************************************/

int printDir(char *point, char *vector, int dir)

{
   if(dir == CLOCKWISE)
      printf("%s is CLOCKWISE of %s; ", point, vector);

   else if(dir == COUNTERCLOCKWISE)
      printf("%s is COUNTERCLOCKWISE of %s; ", point, vector);

   else if(dir == PARALLEL)
      printf("%s is PARALLEL to %s; ", point, vector);

   else 
      printf("Bad comparison (shouldn't get this; ");

   return 0;
}



/***************************************************/
/*                                                 */
/* Between()                                       */
/*                                                 */
/* Tests whether whether a point on an arc is      */
/* between two other points.                       */
/*                                                 */
/***************************************************/

int Between(Vec *v, Vec *a, Vec *b)
{
   double abDot, avDot, bvDot;

   abDot = Dot(a, b);
   avDot = Dot(a, v);
   bvDot = Dot(b, v);

   if(avDot > abDot
   && bvDot > abDot)
      return TRUE;
   else
      return FALSE;
}



/***************************************************/
/*                                                 */
/* Cross()                                         */
/*                                                 */
/* Vector cross product.                           */
/*                                                 */
/***************************************************/

int Cross(Vec *v1, Vec *v2, Vec *v3)
{
   v3->x =  v1->y*v2->z - v2->y*v1->z;
   v3->y = -v1->x*v2->z + v2->x*v1->z;
   v3->z =  v1->x*v2->y - v2->x*v1->y;

   if(v3->x == 0.
   && v3->y == 0.
   && v3->z == 0.)
      return 0;
   
   return 1;
}


/***************************************************/
/*                                                 */
/* Dot()                                           */
/*                                                 */
/* Vector dot product.                             */
/*                                                 */
/***************************************************/

double Dot(Vec *a, Vec *b)
{
   double sum = 0.0;

   sum = a->x * b->x
       + a->y * b->y
       + a->z * b->z;

   return sum;
}


/***************************************************/
/*                                                 */
/* Normalize()                                     */
/*                                                 */
/* Normalize the vector                            */
/*                                                 */
/***************************************************/

double Normalize(Vec *v)
{
   double len;

   len = 0.;

   len = sqrt(v->x * v->x + v->y * v->y + v->z * v->z);

   if(len == 0.)
      len = 1.;

   v->x = v->x / len;
   v->y = v->y / len;
   v->z = v->z / len;
   
   return len;
}


/***************************************************/
/*                                                 */
/* Reverse()                                       */
/*                                                 */
/* Reverse the vector                              */
/*                                                 */
/***************************************************/

void Reverse(Vec *v)
{
   v->x = -v->x;
   v->y = -v->y;
   v->z = -v->z;
}


/***************************************************/
/*                                                 */
/* Girard()                                        */
/*                                                 */
/* Use Girard's theorem to compute the area of a   */
/* sky polygon.                                    */
/*                                                 */
/***************************************************/

double Girard()
{
   int    i, j, ibad;

   double area;
   double lon, lat;

   Vec    side[16];
   double ang [16];

   Vec    tmp;

   double pi, dtr, sumang, cosAng, sinAng;

   pi  = atan(1.0) * 4.;
   dtr = pi / 180.;

   sumang = 0.;

   if(nv < 3)
      return(0.);

   if(debug >= 4)
   {
      for(i=0; i<nv; ++i)
      {
	 lon = atan2(V[i].y, V[i].x)/dtr;
	 lat = asin(V[i].z)/dtr;

	 printf("Girard(): %3d [%13.6e,%13.6e,%13.6e] -> (%10.6f,%10.6f)\n", 
	    i, V[i].x, V[i].y, V[i].z, lon, lat);
	 
	 fflush(stdout);
      }
   }

   for(i=0; i<nv; ++i)
   {
      Cross (&V[i], &V[(i+1)%nv], &side[i]);

      (void) Normalize(&side[i]);
   }

   for(i=0; i<nv; ++i)
   {
      Cross (&side[i], &side[(i+1)%nv], &tmp);

      sinAng =  Normalize(&tmp);
      cosAng = -Dot(&side[i], &side[(i+1)%nv]);


      /* Remove center point of colinear segments */

      ang[i] = atan2(sinAng, cosAng);

      if(debug >= 4)
      {
	 if(i==0)
	    printf("\n");

	 printf("Girard(): angle[%d] = %13.6e -> %13.6e (from %13.6e / %13.6e)\n", i, ang[i], ang[i] - pi/2., sinAng, cosAng);
	 fflush(stdout);
      }

      if(ang[i] > pi - 0.0175)  /* Direction changes of less than */
      {                         /* a degree can be tricky         */
	 ibad = (i+1)%nv;

	 if(debug >= 4)
	 {
	    printf("Girard(): ---------- Corner %d bad; Remove point %d -------------\n", 
	       i, ibad);
	    fflush(stdout);
	 }

	 --nv;

	 for(j=ibad; j<nv; ++j)
	 {
	    V[j].x = V[j+1].x;
	    V[j].y = V[j+1].y;
	    V[j].z = V[j+1].z;
	 }

	 return(Girard());
      }

      sumang += ang[i];
   }

   area = sumang - (nv-2.)*pi;

   if(mNaN(area) || area < 0.)
      area = 0.;

   if(debug >= 4)
   {
      printf("\nGirard(): area = %13.6e [%d]\n\n", area, nv);
      fflush(stdout);
   }

   return(area);
}




/***************************************************/
/*                                                 */
/* RemoveDups()                                    */
/*                                                 */
/* Check the vertex list for adjacent pairs of     */
/* points which are too close together for the     */
/* subsequent dot- and cross-product calculations  */
/* of Girard's theorem.                            */
/*                                                 */
/***************************************************/

void RemoveDups()
{
   int    i, nvnew;
   Vec    Vnew[16];
   Vec    tmp;
   double lon, lat;

   double separation;

   if(debug >= 4)
   {
      printf("RemoveDups() tolerance = %13.6e [%13.6e arcsec]\n\n", 
	 tolerance, tolerance/dtr*3600.);

      for(i=0; i<nv; ++i)
      {
	 lon = atan2(V[i].y, V[i].x)/dtr;
	 lat = asin(V[i].z)/dtr;

	 printf("RemoveDups() orig: %3d [%13.6e,%13.6e,%13.6e] -> (%10.6f,%10.6f)\n", 
	    i, V[i].x, V[i].y, V[i].z, lon, lat);
	 
	 fflush(stdout);
      }

      printf("\n");
   }

   Vnew[0].x = V[0].x;
   Vnew[0].y = V[0].y;
   Vnew[0].z = V[0].z;

   nvnew = 0;

   for(i=0; i<nv; ++i)
   {
      ++nvnew;

      Vnew[nvnew].x = V[(i+1)%nv].x;
      Vnew[nvnew].y = V[(i+1)%nv].y;
      Vnew[nvnew].z = V[(i+1)%nv].z;

      Cross (&V[i], &V[(i+1)%nv], &tmp);

      separation = Normalize(&tmp);

      if(debug >= 4)
      {
	 printf("RemoveDups(): %3d x %3d: distance = %13.6e [%13.6e arcsec] (would become %d)\n", 
	    (i+1)%nv, i, separation, separation/dtr*3600., nvnew);

	 fflush(stdout);
      }

      if(separation < tolerance)
      {
	 --nvnew;

	 if(debug >= 4)
	 {
	    printf("RemoveDups(): %3d is a duplicate (nvnew -> %d)\n",
	       i, nvnew);

	    fflush(stdout);
	 }
      }
   }

   if(debug >= 4)
   {
      printf("\n");
      fflush(stdout);
   }

   if(nvnew < nv)
   {
      for(i=0; i<nvnew; ++i)
      {
	 V[i].x = Vnew[i].x;
	 V[i].y = Vnew[i].y;
	 V[i].z = Vnew[i].z;
      }

      nv = nvnew;
   }
}
