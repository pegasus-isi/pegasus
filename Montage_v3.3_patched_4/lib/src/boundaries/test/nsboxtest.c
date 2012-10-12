#include <stdio.h>
#include <stdlib.h>
#include <math.h>

#include "boundaries.h"

#define PMAX 100000


main(int argc, char **argv)
{
   int   n, mode;
   FILE *fin;

   struct bndInfo *box = (struct bndInfo *)NULL;

   double ra[PMAX], dec[PMAX];

   bndSetDebug(1);

   if(argc < 2)
   {
      printf("Usage: %s tblfile [mode (0:box, 1:vertical box, 2: circle)]\n", argv[0]);
      exit(0);
   }

   mode = 0;
   if(argc > 2)
      mode = atoi(argv[2]);

   if(box)
      free((char *)box);

   fin = fopen(argv[1], "r");

   n = 0;
   while ( (n < PMAX) && (fscanf(fin, "%lf %lf",&ra[n], &dec[n]) != EOF) ) 
      ++n;

   if(mode == 1)
      box = bndVerticalBoundingBox(n, ra, dec);
   else if(mode == 2)
      box = bndBoundingCircle(n, ra, dec);
   else
      box = bndBoundingBox(n, ra, dec);

   if(box == (struct bndInfo *)NULL)
   {
      printf("Error computing boundaries\n");
      fflush(stdout);
      exit(0);
   }

   fclose(fin);

   if(mode == 2)
   {
      printf("Circle center: %13.6f %13.6f\n", box->centerLon, box->centerLat);
      printf("Circle radius: %13.6f\n",        box->radius);

      free((char *)box);
      fflush(stdout);
      exit(0);
   }

   printf("\n\nImage corners:\n\n");
   printf("%13.6f %13.6f\n", box->cornerLon[0],  box->cornerLat[0]);
   printf("%13.6f %13.6f\n", box->cornerLon[1],  box->cornerLat[1]);
   printf("%13.6f %13.6f\n", box->cornerLon[2],  box->cornerLat[2]);
   printf("%13.6f %13.6f\n", box->cornerLon[3],  box->cornerLat[3]);

   printf("\nFITS Info\n");
   printf("Center: %13.6f %13.6f\n", box->centerLon, box->centerLat);
   printf("Size:   %13.6f %13.6f \n", 
	   box->lonSize, box->latSize);
   printf("Posang: %13.6f\n", box->posAngle);

   free((char *)box);

   fflush(stdout);

   exit(0);
}
