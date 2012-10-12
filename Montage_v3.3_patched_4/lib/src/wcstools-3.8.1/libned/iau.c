/***************************************************************************
* iau.c - search objects by an IAU name, specified style, equinox.  ^D to exit
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
   int    no_obj;
   int    i, j;
   char   iau_format[101], style[2], equinox[10];
   char   cra[101];
   char   cdec[101];
   char   sradius[101];
   double ra, dec, radius;
   CrossID *tmpcp;
   ObjInfo *op, *tmpop;
   MoreData     *mdatap;

   st = ned_connect();
   if (st < 0) {
      fprintf(stderr, "connection failed \n");
      exit(1);
      }
   fprintf(stdout, "input the IAU format:");
   fgets(iau_format, 101, stdin);
   fprintf(stdout, "input the style(S or L):");
   fgets(style, 2, stdin);
   fprintf(stdout, "input the equinox(Jnnnn.n or Bnnnn.n):");
   while(fgets(equinox, 10, stdin) != (char*)NULL) {
      st = ned_obj_iau(iau_format, style[0], equinox, 
	   cra, cdec, &radius, &no_obj, &op);
      if (st < 0) {
	 /* for simple error message */
	 fprintf(stderr, "%s\n", ned_get_errmsg());

	 switch (ned_errno) {
	    case NE_IAU:
	       fprintf(stderr, "IAU format %s is wrong \n", iau_format);
	       break;
	    case NE_JB:
	       fprintf(stderr, "Only J or B is valid in the equinox\n");
	       break;
	    case NE_EPOCH:
	       fprintf(stderr, "epoch is out of range [1500, 2500] %s\n",
	       equinox);
	       break;
	    case NE_NOBJ:
	       fprintf(stderr, "object %s is not in NED database\n",
	       iau_format);
	       fprintf(stderr, "%ra: %s, dec: %s, radius: %.1f\n", cra, cdec,
	       radius);
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
	 fprintf(stdout, "%d object(s) found in NED: \n", no_obj);
	 for (i=0, tmpop = op; i<no_obj; i++, tmpop++) {
	    fprintf(stdout, "\n\n%d crossid for object No. %d: \n\n", 
	       tmpop->no_crossid, i+1);
	    for (j=0, tmpcp = tmpop->cp; j<tmpop->no_crossid; j++, tmpcp++)
	       fprintf(stdout, "%s, %s \n", tmpcp->objname, tmpcp->objtype);
	    fprintf(stdout, "distance :    %f\n", tmpop->dist);
	    fprintf(stdout, "no_ref   :    %d\n", tmpop->no_ref);
	    fprintf(stdout, "no_note  :    %d\n", tmpop->no_note);
	    fprintf(stdout, "no_photom:    %d\n", tmpop->no_photom);
	    fprintf(stdout, "obj type :    %s\n", tmpop->objtype);
	    fprintf(stdout, "ra       :    %f\n", tmpop->ra);
	    fprintf(stdout, "dec      :    %f\n", tmpop->dec);
	    fprintf(stdout, "unc_maj  :    %f\n", tmpop->unc_maj);
	    fprintf(stdout, "unc_min  :    %f\n", tmpop->unc_min);
	    fprintf(stdout, "unc_ang  :    %f\n", tmpop->unc_ang);
	    fprintf(stdout, "refcode  :    %s\n", tmpop->refcode);
	    mdatap = tmpop->mdp;
	    while (mdatap) {
	       fprintf(stdout, "%-10s: %s\n", mdatap->data_typec,
				 mdatap->data);
	       mdatap = mdatap->next;
	       }
	    }
	 }

      if (op)
	 ned_free_objp(op, no_obj);
      fprintf(stdout, "\n\ninput the IAU format:");
      fgets(iau_format, 101, stdin);
      fprintf(stdout, "input the style(S or L):");
      fgets(style, 2, stdin);
      fprintf(stdout, "input the equinox(Jnnnn.n or Bnnnn.n):");
      }
   ned_disconnect();
}
