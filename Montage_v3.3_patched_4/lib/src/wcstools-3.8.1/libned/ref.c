/***************************************************************************
* ref.c - search references by its name, year range,  ^D to exit
*
* History
*
* 23-Jul-2001:
*   -Changed gets() to fgets() for compiler warnings about security.
*    Joe Mazzarella
*
* 1996:
*   Original. Xiuqin Wu
*
***************************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include "ned_client.h"

extern int   ned_errno;

void
main(argc, argv)

int  argc;
char *argv[];
{
   
   int    st;
   int    no_ref;
   int    i, j;
   char   objname[101];
   int    begin_year, end_year;
   char   string[101];
   CrossID *cp, *tmpcp;
   NedRef  *refp, *tmprefp;

   st = ned_connect();
   if (st < 0) {
      fprintf(stderr, "connection failed \n");
      exit(1);
      }
   fprintf(stdout, "input the objname:");
   fgets(objname, 101, stdin);
   fprintf(stdout, "input the year range(1995, 1996):");
   while(fgets(string, 101, stdin) != (char*)NULL) {
      sscanf(string, "%d, %d", &begin_year, &end_year);
      st = ned_ref (objname, begin_year, end_year, &no_ref, &cp, &refp);
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
	       fprintf(stderr, "%d ambiguous name: \n", no_ref);
	       for (i=0, tmpcp = cp; i<no_ref; i++, tmpcp++)
		  fprintf(stderr, "%s \n", tmpcp->objname);
	       break;
	    case NE_NOREF:
	       fprintf(stderr, "No reference found for object %s \n",
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
	 fprintf(stdout, "%d reference(s) found in NED: \n", no_ref);
	 for (i=0, tmprefp = refp; i<no_ref; i++, tmprefp++) {
            fprintf(stdout, "\n");
            fprintf(stdout, "refcode:   %s\n", tmprefp->refcode);
	    fprintf(stdout, "publish:   %s\n", tmprefp->pub_name);
	    fprintf(stdout, "   year:   %d\n", tmprefp->year);
	    fprintf(stdout, " volume:   %s\n", tmprefp->vol);
	    fprintf(stdout, "   page:   %s\n", tmprefp->page);
	    fprintf(stdout, " title1:   %s\n", tmprefp->title1);
	    fprintf(stdout, " title2:   %s\n", tmprefp->title2);
	    fprintf(stdout, "author1:   %s\n", tmprefp->author1);
	    fprintf(stdout, "author2:   %s\n", tmprefp->author2);
	    }
	 }

      if (cp)
	 ned_free_cp(cp);
      if (refp)
	 ned_free_refp(refp);
      fprintf(stdout, "input objname:");
      fgets(objname, 101, stdin);
      fprintf(stdout, "input the year range(1995, 1996):");
      }
   ned_disconnect();
}
