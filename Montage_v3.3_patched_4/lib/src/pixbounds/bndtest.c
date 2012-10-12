#include <stdio.h>
#include <math.h>
#include <pixbounds.h>

main()
{
   int i;
   double x[1000], y[1000];

   i = 0;
   while ( (i < 1000) && (scanf("%lf %lf",&x[i], &y[i]) != EOF) ) 
      ++i;
   
   /* geomSetDebug(); */

   cgeomInit(x, y, i);

   printf("\nCenter:    (%-g, %-g)\n",
      cgeomGetXcen(), 
      cgeomGetYcen());

   printf("Size:      %-g x %-g\n",
      cgeomGetWidth(), 
      cgeomGetHeight());

   printf("Rotation:  %-g\n\n",
      cgeomGetAngle()); 

   exit(0);
}
