/***************************************************************************
* ex_refcode.c - expand a refcode,  ^D to exit
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
#include "ned_client.h"

extern int   ned_errno;

void
main(argc, argv)
int  argc;
char *argv[];
{
   
   int    st;
   int    no_obj;
   int    i, j;
   char   refcode[101];
   NedRef full_ref;

   st = ned_connect();
   if (st < 0) {
      fprintf(stderr, "connection failed \n");
      exit(1);
      }
   fprintf(stdout, "input the refcode:");
   while(fgets(refcode, 101, stdin) != (char*)NULL) {
      st = ned_ex_refcode(refcode, &full_ref);
      if (st < 0) {
	 /* for simple error message */
	 fprintf(stderr, "%s\n", ned_get_errmsg());

	 switch (ned_errno) {
	    case NE_NOSPACE:
	       fprintf(stderr, "memory allocation error happened \n");
	       break;
	    case NE_QUERY:
	       fprintf(stderr, "Can't send query to the NED server\n");
	       break;
	    case NE_BROKENC:
	       fprintf(stderr, "The connection to server is broken\n");
	       break;
	    case NE_NOREFC:
	       fprintf(stderr, "No detailed infomation about this refcode\n");
	       break;
	    case NE_EREFC:
	       fprintf(stderr, "wrong refcode\n");
	       break;
	    }
	 } /* -1 return code */
      else {
	 fprintf(stdout, "Expanded refcode:\n");
	 fprintf(stdout, "refcode:   %s\n", full_ref.refcode);
	 fprintf(stdout, "publish:   %s\n", full_ref.pub_name);
	 fprintf(stdout, "   year:   %d\n", full_ref.year);
	 fprintf(stdout, " volume:   %s\n", full_ref.vol);
	 fprintf(stdout, "   page:   %s\n", full_ref.page);
	 fprintf(stdout, " title1:   %s\n", full_ref.title1);
	 fprintf(stdout, " title2:   %s\n", full_ref.title2);
	 fprintf(stdout, "author1:   %s\n", full_ref.author1);
	 fprintf(stdout, "author2:   %s\n", full_ref.author2);

	 }

      fprintf(stdout, "input the refcode:");
   }
   ned_disconnect();
}
