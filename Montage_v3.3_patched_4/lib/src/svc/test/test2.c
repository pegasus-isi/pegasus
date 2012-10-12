#include <stdio.h>
#include <stdlib.h>
#include <svc.h>


main(argc, argv)

int    argc;
char **argv;
{
   int    i, ncol, index;
   char   cmdstr[256], val[256], key[256], *retstr;
   double plon, plat, mlon, mlat, su, sv;

   svc_debug(stdout);
   svc_sigset();

   index = svc_init("agra -s");


   /* FILE */

   strcpy(cmdstr, "file bsc.tbl");
   svc_send(index, cmdstr);
   retstr = svc_receive(index);


   /* HEADER */

   printf("\nHeader:\n");
   strcpy(cmdstr, "header");
   svc_send(index, cmdstr);
   retstr = svc_receive(index);

   if(svc_val(retstr, "return.count", val))
   {
      ncol = atoi(val);
      printf("   %d columns:\n\n", ncol);

      for(i=0; i<ncol; ++i)
      {
	 sprintf(key, "return.value[%-d]", i);

	 if(svc_val(retstr, key, val))
	    printf("   %3d: \"%s\"\n", i, val);
	 else
	    printf("   %3d: \"%s\" not found.\n", i, key);
      }
   }


   /* READ (longitude) */

   strcpy(cmdstr, "read x ra");
   svc_send(index, cmdstr);
   retstr = svc_receive(index);


   /* READ (latitude) */

   strcpy(cmdstr, "read y dec");
   svc_send(index, cmdstr);
   retstr = svc_receive(index);


   /* SCALE */

   printf("\nScale:\n");
   strcpy(cmdstr, "scale x y");
   svc_send(index, cmdstr);
   retstr = svc_receive(index);

   if(svc_val(retstr, "return.plon", val))
      plon = atof(val);

   if(svc_val(retstr, "return.plat", val))
      plat = atof(val);

   if(svc_val(retstr, "return.mlon", val))
      mlon = atof(val);

   if(svc_val(retstr, "return.mlat", val))
      mlat = atof(val);

   if(svc_val(retstr, "return.su",   val))
      su = atof(val);

   if(svc_val(retstr, "return.sv",   val))
      sv = atof(val);

   if(svc_val(retstr, "return.proj", val))
      printf("   Projection:        \"%s\"\n", val);

   printf("   Projection center: (%-g, %-g)\n", plon, plat);
   printf("   Map center:        (%-g, %-g)\n", mlon, mlat);
   printf("   Size:              (%-g, %-g)\n",   su,   sv);


   /* APPLY */

   strcpy(cmdstr, "apply");
   svc_send(index, cmdstr);
   retstr = svc_receive(index);


   /* GRID */

   strcpy(cmdstr, "grid");
   svc_send(index, cmdstr);
   retstr = svc_receive(index);


   /* MAP */

   strcpy(cmdstr, "map x y");
   svc_send(index, cmdstr);
   retstr = svc_receive(index);


   /* QUIT */

   strcpy(cmdstr, "quit");
   svc_send(index, cmdstr);
   retstr = svc_receive(index);

   svc_close(index);
}
