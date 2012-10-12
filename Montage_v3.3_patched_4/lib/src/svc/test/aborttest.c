#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <svc.h>

void pgm_sigset();
void pgm_sighandler(int sig);

main(argc, argv)

int    argc;
char **argv;
{
   int  i, done, j, index;
   char *retstr;
   SVC *sv;

   svc_debug(stdout);
   svc_sigset();

   index = svc_init("abortchild");

   done = 0;

   i = 0;
   while(1)
   {
      retstr = svc_receive(index);

      if((sv = svc_struct(retstr)) != (SVC *)NULL)
      {
	 for(j=0; j<sv->count; ++j)
	 {
	    printf("   %d: <%s> = <%s>\n", j, sv->key[j], sv->val[j]);

	    if(strcmp(sv->key[j], "stat")  == 0
	    && strcmp(sv->val[j], "ABORT") == 0)
               done = 1;

	    if(strcmp(sv->key[j], "msg")  == 0
	    && strcmp(sv->val[j], "done") == 0)
               done = 1;
	 }

      }
      else
	 printf("Illegal return structure.\n");
      
      if(done)
         break;

      ++i;

      if(i == 2)
	 svc_kill(index);
   }

   svc_close(index);
   printf("End message detected.\n");
   svc_free(sv);
   exit(0);
}
