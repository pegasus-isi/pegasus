#include <stdio.h>
#include <stdlib.h>
#include <svc.h>


main(argc, argv)

int    argc;
char **argv;
{
   int  i, j, k, l, index, index1, index2;
   char cmdstr[256], *retstr;
   SVC *sv, *ret, *val;

   svc_debug(stdout);
   svc_sigset();

   index1 = svc_init("agra -s -x");
   index2 = svc_init("agra -s");

   printf("Two agras: indices %d and %d\n", index1, index2);

   index = index1;
   if(argc > 1)
      index = index2;

   while(gets(cmdstr))
   {
      printf("-----------------------------------------------------------\n\n");
      printf("%s\n\n", cmdstr);

      svc_send(index, cmdstr);
      retstr = svc_receive(index);

      if((sv = svc_struct(retstr)) != (SVC *)NULL)
      {
	 printf("\n");
	 for(j=0; j<sv->count; ++j)
	 {
	    printf("   %d: <%s> = <%s>\n", j, sv->key[j], sv->val[j]);

	    if(strcmp(sv->key[j], "return") == 0)
	    {
	       if((ret = svc_struct(sv->val[j])) != (SVC *)NULL)
	       {
		  printf("\n");
		  for(k=0; k<ret->count; ++k)
		  {
		     printf("      %d: <%s> = <%s>\n", 
			     k, ret->key[k], ret->val[k]);

		     if(strcmp(ret->key[k], "value") == 0)
		     {
			if((val = svc_struct(ret->val[k])) != (SVC *)NULL)
			{
			   printf("\n");
			   for(l=0; l<val->count; ++l)
			   {
			      printf("         %d: <%s> = <%s>\n", 
				      l, val->key[l], val->val[l]);
			   }

			   svc_free(val);
			}
		     }
		  }

		  svc_free(ret);
	       }
	    }
	 }

	 svc_free(sv);
      }
      else
	 printf("Illegal structure.\n");

      ++i;
   }

   svc_close(index);
}
