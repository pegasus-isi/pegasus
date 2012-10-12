#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mtbl.h>

int main(int argc, char **argv)
{
   int    stat, count;

   int    nrow1, nrow2;
   int    ncol1, ncol2;
   int    icol1, icol2;
   int    id1,     id2;

   struct TBL_INFO *tbl1;
   struct TBL_INFO *tbl2;


   /* Open first table */

   ncol1 = topen("corrections.tbl");
   nrow1 = tlen();
   tbl1  = tsave();
   icol1 = tcol("id");


   /* Open second table */

   ncol2 = topen("pimages.tbl");
   nrow2 = tlen();
   tbl2  = tsave();
   icol2 = tcol("cntr");


   /* Read through the files, counting */
   /* the records that match           */

   count = 0;

   while(1)
   {
      trestore(tbl1);

      stat = tread();

      if(stat)
	 break;

      id1 = atoi(tval(icol1));

      trestore(tbl2);

      stat = tread();

      if(stat)
	 break;

      id2 = atoi(tval(icol2));

      if(id1 == id2)
	 ++count;
   }


   trestore(tbl1);
   tclose();
   tfree(tbl1);

   trestore(tbl2);
   tclose();
   tfree(tbl2);

   printf("%d match\n", count);
   fflush(stdout);

   exit(0);
}
