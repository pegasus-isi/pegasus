#include <stdio.h>
#include <stdlib.h>
#include <svc.h>


main(argc, argv)

int    argc;
char **argv;
{
   int   i;
   FILE *fout;

   fout = fopen("child.out", "w+");

   for(i=0; i<4; ++i)
   {
      sleep(5);
      
      fprintf(fout, "[struct stat=\"OK\", iter=%d]\n", i);
      fflush(fout);
      
      printf("[struct stat=\"OK\", iter=%d]\n", i);
      fflush(stdout);
   }

   fprintf(fout, "[struct stat=\"OK\", msg=\"done\"]\n");
   fflush(fout);

   printf("[struct stat=\"OK\", msg=\"done\"]\n");
   fflush(stdout);

   fclose(fout);

   exit(0);
}
