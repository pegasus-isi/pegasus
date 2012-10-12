/* Module: checkFile.c

Version  Developer        Date     Change
-------  ---------------  -------  -----------------------
1.1      John Good        27Jun03  Added a few comments for clarity 
1.0      John Good        14Mar03  Baseline code

*/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <string.h>


/* Checks to see if file exists */
/* (and isn't directory)        */

int checkFile(char *filename)
{
   int istat;

   struct stat type;

   istat = stat(filename, &type);

   if(istat < 0)
      return 1;

   else if (S_ISDIR(type.st_mode) == 1)
      return 2;

   return 0;
}
