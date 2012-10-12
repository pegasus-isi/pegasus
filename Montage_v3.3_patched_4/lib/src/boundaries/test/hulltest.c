#include <stdio.h>
#include <stdlib.h>
#include <math.h>

#include "boundaries.h"

#define PMAX 10000


main(int argc, char **argv)
{
   int   n;
   FILE *fin;

   struct bndInfo *box = (struct bndInfo *)NULL;

   double ra[PMAX], dec[PMAX];

   bndSetDebug(1);

   if(argc < 2)
   {
      printf("Usage: %s tblfile\n", argv[0]);
      exit(0);
   }

   if(box)
      free((char *)box);

   fin = fopen(argv[1], "r");

   n = 0;
   while ( (n < PMAX) && (fscanf(fin, "%lf %lf",&ra[n], &dec[n]) != EOF) ) 
      ++n;

   box = bndBoundingBox(n, ra, dec);

   if(box == (struct bndInfo *)NULL)
   {
      printf("Error computing boundaries\n");
      fflush(stdout);
      exit(0);
   }

   fclose(fin);

   printf("\n\nImage corners:\n\n");
   printf("%13.6f %13.6f\n", box->cornerLon[0],  box->cornerLat[0]);
   printf("%13.6f %13.6f\n", box->cornerLon[1],  box->cornerLat[1]);
   printf("%13.6f %13.6f\n", box->cornerLon[2],  box->cornerLat[2]);
   printf("%13.6f %13.6f\n", box->cornerLon[3],  box->cornerLat[3]);

   printf("\nFITS Info\n");
   printf("Center: %13.6f %13.6f\n", box->centerLon, box->centerLat);
   printf("Size:   %13.6f %13.6f (arcsec)\n", 
	   box->lonSize*3600., box->latSize*3600.);
   printf("Posang: %13.6f\n", box->posAngle);

   free((char *)box);

   fflush(stdout);

   exit(0);
}
