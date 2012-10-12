#include <stdio.h>
#include <string.h>
#include <mtbl.h>

char *strip(char *str);

main(argc, argv)
int argc;
char **argv;
{
   int    i, ncol;
   char   tmp [1024];
   char   name[1024];
   char   type[1024];
   char  *ret;

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
      strncpy(tmp, tbl_hdr_string + tbl_rec[i].endcol-tbl_rec[i].colwd+1, tbl_rec[i].colwd);
      tmp[tbl_rec[i].colwd] = '\0';

      ret = strip(tmp);
      strcpy(name, ret);
      strcpy(type, "");

      if(haveType)
      {
	 strncpy(tmp, tbl_typ_string + tbl_rec[i].endcol-tbl_rec[i].colwd+1, tbl_rec[i].colwd);
	 tmp[tbl_rec[i].colwd] = '\0';

	 ret = strip(tmp);
	 strcpy(type, ret);
      }

      if(type[0] == 'c')
	 printf("%s char(%d)\n", name, tbl_rec[i].colwd);
      else
	 printf("%s %s\n", name, type);
   }


   /* Close table */

   tclose();
}


char *strip(char *str)
{
   int  i;

   static char outstr[1024];

   char *ptr;

   ptr = str;

   while((*ptr == '|' || *ptr == ' ') && ptr < str+strlen(str))
      ++ptr;

   strcpy(outstr, ptr);

   for(i=strlen(outstr)-1; i>= 0; --i)
   {
      if(outstr[i] != '|' && outstr[i] != ' ')
	 break;
      
      outstr[i] = '\0';
   }

   return(outstr);
}
