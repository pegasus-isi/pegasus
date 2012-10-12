#include <stdio.h>

main()
{
   int    i, ncol, icol, jcol;
   char  *value;

   ncol = topen("test1.tbl");
   icol = tcol("fnu_25");
   jcol = tcol("fnu_60");

   while(tread() >= 0)
      printf("%s %s\n",  tval(icol), tval(jcol));

   tclose();
}
