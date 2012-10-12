#include <stdio.h>
#include <string.h>
#include <mtbl.h>

main(argc, argv)
int argc;
char **argv;
{
   int    i, icol, ncol;
   int    nrow, stat;
   char  *name, value[1024];

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


   /* Echo the header */

   for(i=0; i<ncol; ++i)
   {
      strncpy(value, tbl_hdr_string + tbl_rec[i].endcol-tbl_rec[i].colwd+1, tbl_rec[i].colwd);
      value[tbl_rec[i].colwd] = '\0';
      printf("%s", value);
   }

   printf("\n");

   if(haveType)
   {
      for(i=0; i<ncol; ++i)
      {
	 strncpy(value, tbl_typ_string + tbl_rec[i].endcol-tbl_rec[i].colwd+1, tbl_rec[i].colwd);
	 value[tbl_rec[i].colwd] = '\0';
	 printf("%s", value);
      }

      printf("\n");
   }


   /* Read the rows */

   while(1)
   {
      stat = tread();

      if(stat < 0)
	 break;

      for(i=0; i<ncol; ++i)
      {
	 strncpy(value, tbl_rec_string + tbl_rec[i].endcol-tbl_rec[i].colwd+1, tbl_rec[i].colwd);
	 value[tbl_rec[i].colwd] = '\0';
	 printf("%s", value);
      }
      
      printf("\n");
   }

   /* Close table */

   tclose();
}
