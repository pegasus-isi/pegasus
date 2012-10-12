#include   <stdio.h>
#include   <stdlib.h>
#include   <math.h>
#include   "pixbounds.h"


static  struct tPointStructure *P;

static int n       = 0;   /* Actual # of points */
static int ndelete = 0;   /* Number deleted */

static double corner[4][2];
static double xcen, ycen;
static double theta, width, height;


static int debug = 0;


int cgeomSetDebug()
{
   debug = 1;

   return 0;
}


double cgeomGetXcen()
{
   return xcen;
}


double cgeomGetYcen()
{
   return ycen;
}


double cgeomGetWidth()
{
   return width;
}


double cgeomGetHeight()
{
   return height;
}


double cgeomGetAngle()
{
   return theta;
}


int cgeomInit(double *x, double *y, int nin)
{
   int i;
   tStack top;

   n = nin;

   P = (struct tPointStructure *)
          malloc(n * sizeof(struct tPointStructure));

   if(debug)
      printf("memory initialized for %d points\n", n);

   for(i=0; i<n; ++i)
   {
      P[i].v[0]   = x[i];
      P[i].v[1]   = y[i];
      P[i].vnum   = i;
      P[i].delete = 0;
   }

   cgeomFindLowest();
   
   if(debug)
   {
      printf("\nLowest point moved to start\n");

      cgeomPrintPoints();
   }

   qsort(
      &P[1],                            /* pointer to 1st elem      */
      n-1,                              /* number of elems          */
      sizeof( struct tPointStructure ), /* size of each elem        */
      cgeomCompare                      /* -1,0,+1 compare function */
   );

   if(debug)
   {
      printf("\nAfter sorting\n");

      cgeomPrintPoints();
   }

   if (ndelete > 0) 
      cgeomSquash();

   top = cgeomGraham();

   if(debug)
   {
      printf("\nHull:\n");
      cgeomPrintStack( top );
   }

   if(debug)
      printf("\nBox:\n");

   cgeomBox( top );
   
   if(debug)
      cgeomPrintPostscript( top );

   return 0;
}


/*---------------------------------------------------------------------
FindLowest finds the rightmost lowest point and swaps with 0-th.
The lowest point has the min y-coord, and amongst those, the
max x-coord: so it is rightmost among the lowest.
---------------------------------------------------------------------*/

void cgeomFindLowest( void )
{
   int    i;
   int    itemp;
   double temp;

   int m = 0;   /* Index of lowest so far. */

   for ( i = 1; i < n; i++ )
   {
      if ( (P[i].v[1] <  P[m].v[1]) ||
          ((P[i].v[1] == P[m].v[1]) && (P[i].v[0] > P[m].v[0])) ) 
         m = i;
   }

   if(m != 0)
   {
      itemp       = P[0].vnum;
      P[0].vnum   = P[m].vnum;
      P[m].vnum   = itemp;

      temp        = P[0].v[0];
      P[0].v[0]   = P[m].v[0];
      P[m].v[0]   = temp;

      temp        = P[0].v[1];
      P[0].v[1]   = P[m].v[1];
      P[m].v[1]   = temp;

      itemp       = P[0].delete;
      P[0].delete = P[m].delete;
      P[m].delete = itemp;
   }
}


/*---------------------------------------------------------------------
Compare: returns -1,0,+1 if p1 < p2, =, or > respectively;
here "<" means smaller angle.  Follows the conventions of qsort.
---------------------------------------------------------------------*/
int cgeomCompare( const void *tpi, const void *tpj )
{
   int a;             /* area */
   double x, y;       /* projections of ri & rj in 1st quadrant */
   tPoint pi, pj;
   pi = (tPoint)tpi;
   pj = (tPoint)tpj;

   a = cgeomAreaSign( P[0].v, pi->v, pj->v );

   if (a > 0)
      return -1;

   else if (a < 0)
      return 1;

   else  /* Collinear with P[0] */
   { 
      x = fabs( pi->v[0] -  P[0].v[0] ) - fabs( pj->v[0] -  P[0].v[0] );
      y = fabs( pi->v[1] -  P[0].v[1] ) - fabs( pj->v[1] -  P[0].v[1] );

      ndelete++;

      if ( (x < 0) || (y < 0) ) 
      {
         pi->delete = 1;
         return -1;
      }
      else if ( (x > 0) || (y > 0) ) 
      {
         pj->delete = 1;
         return 1;
      }
      else /* points are coincident */
      {
         if (pi->vnum > pj->vnum)
             pj->delete = 1;
         else 
             pi->delete = 1;
         return 0;
      }
   }
}


/*---------------------------------------------------------------------
Pops off top elment of stack s, frees up the cell, and returns new top.
---------------------------------------------------------------------*/

tStack cgeomPop( tStack s )
{
   tStack top;

   top = s->next;

   if (s) 
   {
      free ((char *) s);
      s = NULL;
   }
 
   return top;
}


/*---------------------------------------------------------------------
Get a new cell, fill it with p, and push it onto the stack.
Return pointer to new stack top.
---------------------------------------------------------------------*/
tStack cgeomPush( tPoint p, tStack top )
{
   tStack   s;

   /* Get new cell and fill it with point. */

   if ((s=(tsStack *) malloc (sizeof(tsStack))) == NULL) 
   {
     printf ("[struct stat=\"ERROR\", msg=\"Out of memory\"]\n");
     fflush(stdout);
     exit(1);
   }

   s->p = p;
   s->next = top;
   return s;
}


/*---------------------------------------------------------------------
---------------------------------------------------------------------*/
void cgeomPrintStack( tStack t )
{
   if (!t) printf("Empty stack\n");
   while (t) { 
      printf("vnum=%d\tx=%-g\ty=%-g\n", 
             t->p->vnum,t->p->v[0],t->p->v[1]); 
      t = t->next;
   }
}


/*---------------------------------------------------------------------
Performs the Graham scan on an array of angularly sorted points P.
---------------------------------------------------------------------*/
tStack cgeomGraham()
{
   tStack   top;
   int i;
   tPoint p1, p2;  /* Top two points on stack. */

   /* Initialize stack. */
   top = NULL;
   top = cgeomPush ( &P[0], top );
   top = cgeomPush ( &P[1], top );

   /* Bottom two elements will never be removed. */
   i = 2;

   while ( i < n ) 
   {
      if(debug)
      {
	 printf("Stack at top of while loop, i=%d, vnum=%d:\n", i, P[i].vnum);
	 cgeomPrintStack( top );
      }

      p1 = top->next->p;
      p2 = top->p;

      if ( cgeomLeft( p1->v , p2->v, P[i].v ) ) 
      {
         top = cgeomPush ( &P[i], top );
         i++;
      }
      else    
         top = cgeomPop( top );

      if(debug)
      {
	 printf("Stack at bot of while loop, i=%d, vnum=%d:\n", i, P[i].vnum);
	 cgeomPrintStack( top );
	 putchar('\n');
      }
   }

   return top;

}

/*---------------------------------------------------------------------
Finds the minimum area bounding box for the convex hull             
---------------------------------------------------------------------*/
void cgeomBox( tStack start )
{
   tStack current, next;
   tStack side2, side3, side2max;
   double A, B, C1, C2, C3, C4;
   double Amin, Bmin, C1min, C2min, C3min, C4min;
   double w, h;
   double area, areamin, norm;
   double dmax, d, C;

   int    firstpoint;

   firstpoint = -1;
   areamin    = -1.;
   current    = start;

   while (1) 
   { 
      ++firstpoint;

      if(debug)
         printf("\nfirstpoint = %d\n", firstpoint);

      next = current->next;

      if(!next)
         next = start;

      A  = current->p->v[1] - next->p->v[1];
      B  = next->p->v[0] - current->p->v[0];

      C1 = current->p->v[0] *    next->p->v[1] 
         -    next->p->v[0] * current->p->v[1];

      norm = sqrt(A*A + B*B);

      A  = A/norm;
      B  = B/norm;
      C1 = C1/norm;

      if(debug)
         printf("A = %-g, B = %-g, C1 = %-g (tests: %-g %-g)\n", 
            A, B, C1,
            A * current->p->v[0] + B * current->p->v[1] + C1,
            A *    next->p->v[0] + B *    next->p->v[1] + C1);

      dmax  = -1.;
      side2 = start;

      while(side2)
      {
         d = fabs(A*side2->p->v[0] + B*side2->p->v[1] + C1);

         if(debug)
            printf("d = %-g, dmax = %-g\n", d, dmax);

	 if(d > dmax)
	 {
	    dmax     = d;
            side2max = side2;
         }

	 side2 = side2->next;
      }

      C2 = -A*side2max->p->v[0] - B*side2max->p->v[1];

      if(debug)
      {
	 printf("C2 = %-g\n", C2);
	 printf("C1 = %-g (test: %-g)\n", 
	    C2, A*side2max->p->v[0]+B*side2max->p->v[1]+C2);
      }

      side3 = start;

      while(side3)
      {
	 C = B*side3->p->v[0] - A*side3->p->v[1];

	 if(debug)
	    printf("C = %-g (test: %-g)\n",
	       C, -B*side3->p->v[0] + A*side3->p->v[1] + C);

	 if(C < C3 || side3 == start)
	    C3 = C;

	 if(C > C4 || side3 == start)
	    C4 = C;

	 side3 = side3->next;
      }

      h = fabs(C1 - C2);
      w = fabs(C3 - C4);

      area = w * h;

      if(debug)
	 printf("w = %-g, h = %-g, area = %-g\n",
	    w, h, area);

      if(area > 0. && (area < areamin || areamin < 0.))
      {
	 Amin = A;
	 Bmin = B;

	 C1min = C1;
	 C2min = C2;
	 C3min = C3;
	 C4min = C4;

	 areamin = area;

	 width  = w;
	 height = h;

	 theta = atan2(A, B) * 45./atan(1);
        
         /*
         while(theta < -180.) theta += 360.;
         while(theta >  180.) theta -= 360.;

         if(theta > 90.)
            theta -= 180;

         if(theta < -90.)
            theta += 180;

         if(theta > 45.)
         {
            width  = h;
            height = w;

            theta -= 90.;
         }

         if(theta < -45.)
         {
            width  = h;
            height = w;

            theta += 90.;
         }
         */

	 if(debug)
	    printf("New min: theta = %-g, width = %-g, height = %-g\n",
	       theta, width, height);
      }

      current = next;

      if(current == start)
         break;
   }

   corner[0][0] = -Amin*C1min + Bmin*C3min;
   corner[0][1] = -Bmin*C1min - Amin*C3min;

   corner[1][0] = -Amin*C2min + Bmin*C3min;
   corner[1][1] = -Bmin*C2min - Amin*C3min;

   corner[2][0] = -Amin*C2min + Bmin*C4min;
   corner[2][1] = -Bmin*C2min - Amin*C4min;

   corner[3][0] = -Amin*C1min + Bmin*C4min;
   corner[3][1] = -Bmin*C1min - Amin*C4min;

   xcen = ( corner[0][0] + corner[1][0] + corner[2][0] + corner[3][0] ) / 4.;
   ycen = ( corner[0][1] + corner[1][1] + corner[2][1] + corner[3][1] ) / 4.;

   if(debug)
      printf("Center: (%-g, %-g)\n", xcen, ycen);
}


/*---------------------------------------------------------------------
Squash removes all elements from P marked delete.
---------------------------------------------------------------------*/
void cgeomSquash( void )
{
   int i, j;

   i = 0; j = 0;

   while ( i < n ) 
   {
      if ( !P[i].delete ) /* if not marked for deletion */
      {
         cgeomCopy( i, j ); /* Copy P[i] to P[j]. */
         j++;
      }

      /* else do nothing: delete by skipping. */
      i++;
   }
   n = j;

   if(debug)
      cgeomPrintPoints();
}


void cgeomCopy( int i, int j )
{
   P[j].v[0]   = P[i].v[0];
   P[j].v[1]   = P[i].v[1];
   P[j].vnum   = P[i].vnum;
   P[j].delete = P[i].delete;
}


/*---------------------------------------------------------------------
Returns true iff c is strictly to the left of the directed
line through a to b.
---------------------------------------------------------------------*/
int cgeomLeft( tPointi a, tPointi b, tPointi c )
{ 
   if( (b[0] - a[0]) * (c[1] - a[1]) - (c[0] - a[0]) * (b[1] - a[1]) > 0)
     return 1;
  else
     return 0;
}



void cgeomPrintPoints( void )
{
   int   i;

   printf("Points:\n");
   for( i = 0; i < n; i++ )
      printf("vnum=%3d, x=%-g, y=%-g, delete=%d\n", 
	     P[i].vnum, P[i].v[0], P[i].v[1], P[i].delete);
}


void cgeomPrintPostscript( tStack t)
{
   int    i;
   double xmin, ymin, xmax, ymax;

   xmin = xmax = P[0].v[0];
   ymin = ymax = P[0].v[1];
   for (i = 1; i < n; i++) 
   {
      if      ( P[i].v[0] > xmax ) xmax = P[i].v[0];
      else if ( P[i].v[0] < xmin ) xmin = P[i].v[0];
      if      ( P[i].v[1] > ymax ) ymax = P[i].v[1];
      else if ( P[i].v[1] < ymin ) ymin = P[i].v[1];
   }
   xmin -= 2.;
   xmax += 2.;
   ymin -= 2.;
   ymax += 2.;

   /* PostScript header */
   printf("%%!PS\n");
   printf("%%%%Creator: graham.c (Joseph O'Rourke)\n");
   printf("%%%%BoundingBox: %-g %-g %-g %-g\n", xmin, ymin, xmax, ymax);
   printf("%%%%EndComments\n");
   printf(".00 .00 setlinewidth\n");
   printf("%-g %-g translate\n", -xmin+72, -ymin+72 );
   /* The +72 shifts the figure one inch from the lower left corner */

   /* Draw the points as little circles. */
   printf("newpath\n");
   printf("\n%%Points:\n");
   for (i = 0; i < n; i++)
      printf("%-g\t%-g\t0.1 0  360\tarc\tstroke\n", P[i].v[0], P[i].v[1]);
   printf("closepath\n");

   /* Draw the polygon. */
   printf("\n%%Hull:\n");
   printf("newpath\n");
   printf("%-g\t%-g\tmoveto\n", t->p->v[0], t->p->v[1]);
   while (t) 
   {
      printf("%-g\t%-g\tlineto\n", t->p->v[0], t->p->v[1]);
      t = t->next;
   }
   printf("closepath stroke\n");

   /* Draw the box. */
   printf("\n%%Box:\n");
   printf("newpath\n");
   printf("%-g\t%-g\tmoveto\n", corner[0][0], corner[0][1]);
   for(i=1; i<4; ++i)
      printf("%-g\t%-g\tlineto\n", corner[i][0], corner[i][1]);

   printf("closepath stroke\n");

   printf("%-g\t%-g\t1.0 0  360\tarc\tstroke\n", xcen, ycen);
   
   printf("showpage\n%%%%EOF\n");
}


int cgeomAreaSign( tPointi a, tPointi b, tPointi c )
{
    double area2;

    area2 = ( b[0] - a[0] ) * (double)( c[1] - a[1] ) -
            ( c[0] - a[0] ) * (double)( b[1] - a[1] );

    if      ( area2 >  0.5 ) return  1;
    else if ( area2 < -0.5 ) return -1;
    else                     return  0;
}
