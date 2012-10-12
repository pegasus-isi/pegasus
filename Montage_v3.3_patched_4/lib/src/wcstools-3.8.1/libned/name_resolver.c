/***************************************************************************
* name_resolver.c - or a given name, get all the crossIDs in NED.  ^D to exit
*
* History 
*
* 23-Jul-2001:
*   Changed gets() to fgets() for compiler warnings about security. 
*   Joe Mazzarella
* 
* 1996:
*   Original. Xiuqin Wu
*
***************************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include "ned_client.h"

extern int   ned_errno;

void
main(argc, argv)

int  argc;
char *argv[];

{
   
   int    st;
   int    no_names;
   int    i, j;
   char   objname[101];
   CrossID *cp, *tmpcp;

   st = ned_connect();
   if (st < 0) {
      fprintf(stderr, "connection failed \n");
      exit(1);
      }
   fprintf(stdout, "input the objname:");
   while(fgets(objname, 101, stdin) != (char*)NULL) {
      st = ned_name_resolver(objname, &no_names, &cp);
      if (st < 0) {

	 /* for simple error message */
	 fprintf(stderr, "%s\n", ned_get_errmsg());

	 switch (ned_errno) {
	    case NE_NAME:
	       fprintf(stderr, 
	       "name %s can't be recognized by NED name interpreter\n",
		objname);
	       break;
	    case NE_AMBN:
	       fprintf(stderr, "%d ambiguous name: \n", no_names);
	       for (i=0, tmpcp = cp; i<no_names; i++, tmpcp++)
		  fprintf(stderr, "%s \n", tmpcp->objname);
	       break;
	    case NE_NOBJ:
	       fprintf(stderr, "object %s is not in NED database\n",
		  cp->objname);
	       break;
	    case NE_NOSPACE:
	       fprintf(stderr, "memory allocation error happened \n");
	       break;
	    case NE_QUERY:
	       fprintf(stderr, "Can't send query to the NED server\n");
	       break;
	    case NE_BROKENC:
	       fprintf(stderr, "The connection to server is broken\n");
	       break;
	    }
	 } /* -1 return code */
      else {
	 fprintf(stdout, "%d crossid(s) found in NED: \n", no_names);
	 for (i=0, tmpcp = cp; i<no_names; i++, tmpcp++)
	    fprintf(stderr, "%30s\n", tmpcp->objname);
	 }

      if (cp)
	 ned_free_cp(cp);
      fprintf(stdout, "input objname:");
      }

   ned_disconnect();
}
