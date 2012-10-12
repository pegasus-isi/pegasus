#include <stdio.h>
#include <stdlib.h>
#include <svc.h>

#define MAXSTR 32768

char *svc_val();



main(int argc, char **argv)
{
   char *value;
   char  instr    [MAXSTR];
   char  structstr[MAXSTR];


   /* Construct the structure string */

   printf("\n---------------------------------------------\n\n");

   strcpy(structstr, "");
   while(gets(instr))
   {
      if(instr[0] == '\0')
	 break;

      printf("%s\n", instr);
      strcat(structstr, instr);
      strcat(structstr, " ");
   }


   /* Print it out (recursively) */

   printf("\n---------------------------------------------\n");
   printstruct(structstr, 0);
   printf("\n---------------------------------------------\n\n");


   /* Now see if the user wants to print out some specific values */

   while(gets(instr))
   {
      if(instr[0] == '\0')
	 continue;

      if(svc_val(structstr, instr, value) == (char *)NULL)
	 printf("[Invalid]\n");
      else
	 printf("<%s> = <%s>\n", instr, value);
   }

   printf("\n---------------------------------------------\n\n");
}


printstruct(char *instr, int level)
{
   int  i, j;
   char structstr[MAXSTR];
   char value    [MAXSTR];
   char blank    [MAXSTR];
   SVC *sv, *sub;


   /* Set up the indentation string */

   for(i=0; i<MAXSTR; ++i)
      blank[i] = ' ';
   
   blank[3*level] = '\0';


   /* Parse the string and (if it is a structure) print out the elements */

   strcpy(structstr, instr);

   if((sv = svc_struct(structstr)) != (SVC *)NULL)
   {
      printf("\n\n%s   sv->count = %d\n\n", blank, sv->count);

      for(j=0; j<sv->count; ++j)
      {
	 printf("%s%4d: <%s> = ", blank, j, sv->key[j]);

	 strcpy(value, sv->val[j]);
	 if((sub = svc_struct(value)) == (SVC *)NULL)
	    printf("<%s>\n", sv->val[j]);
	 else
	 {
	    strcpy(value, sv->val[j]);
	    printstruct(value, level+1);
	    printf("\n");
	    svc_free(sub);
	 }
      }

      svc_free(sv);
      return(1);
   }
   else
      return(0);
}
