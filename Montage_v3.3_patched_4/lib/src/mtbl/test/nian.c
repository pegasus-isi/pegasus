#include <stdio.h>
#include <string.h>
#include <mtbl.h>

main(argc, argv)
int argc;
char **argv;
{
   int    i, icol, ncol;
   int    nrow, stat;
   char  *name, *value;

   if(argc == 1)
   {
      printf("No table file name given.\n");
      exit(0);
   }


   /* Open table */

   ncol = topen(argv[1]);
   if(ncol < 0)
   {
      printf("Error opening table %s\n", argv[1]);
      exit(0);
   }


   /* Read the rows */

   nrow = 0;
   while(1)
   {
      stat = tread();

      if(stat < 0)
	 break;

      ++nrow;

      for(i=0; i<ncol; ++i)
      {
	 value = tval(i);
	 printf("%s", value);

	 if(i<ncol-1)
	    printf("|");
      }

      printf("\n");
   }


   /* Close table */

   tclose();
}
