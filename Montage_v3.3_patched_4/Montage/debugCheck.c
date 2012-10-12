/* Module: debugCheck.c

Version  Developer        Date     Change
-------  ---------------  -------  -----------------------
1.1      John Good        25Aug03  Implemented status file processing
1.0      John Good        13Mar03  Baseline code

*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "montage.h"

/**********************************************************/
/*                                                        */
/*  debugCheck                                            */
/*                                                        */
/*  This routine checks a debug level string to see if it */
/*  represents a valid positive integer.                  */
/*                                                        */
/**********************************************************/

int debugCheck(char *debugStr)
{
   int   debug;
   char *end;

   debug = strtol(debugStr, &end, 0);

   if(end - debugStr < (int)strlen(debugStr))
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Debug level string is invalid: '%s'\"]\n", debugStr);
      exit(1);
   }

   if(debug < 0)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Debug level value cannot be negative\"]\n");
      exit(1);
   }

   return debug;
}
