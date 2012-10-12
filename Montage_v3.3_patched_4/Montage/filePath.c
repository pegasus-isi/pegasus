/* Module: filePath.c

Version  Developer        Date     Change
-------  ---------------  -------  -----------------------
1.0      John Good        13Mar03  Baseline code

*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*************************************************************************/
/*                                                                       */
/*  filePath                                                             */
/*                                                                       */
/*  This routine updates file name strings by prepending a path          */
/*  If the string is already absolute, no change is made.                */
/*                                                                       */
/*************************************************************************/

char *filePath(char *path, char *fname)
{
   int   len;
   char *ptr;

   static char base[2048];


   /* Check to see if the file     */
   /* name is relative or absolute */

   if(fname[0] == '/')
      return(fname);


   /* Check to see if there is a "./"   */
   /* at the beginning of the file name */

   ptr = fname;

   if(strlen(fname) >= 2 && strncmp(fname, "./", 2) == 0)
      ptr += 2;
   

   /* Modify the path string to serve */
   /* as a base for the file path     */

   strcpy(base, path);

   len = strlen(base);

   if(len > 0)
   {
      if(base[len - 1] != '/')
         strcat(base, "/");
   }

   strcat(base, ptr);

   return(base);
}



/*************************************************************************/
/*                                                                       */
/*  fileName                                                             */
/*                                                                       */
/*  This routine pulls out the file name (no path info) from a string    */
/*                                                                       */
/*************************************************************************/

char *fileName(char *fname)
{
   int   i, len;


   /* Pull out the last part of the */
   /* string (the file name)        */

   len = strlen(fname);

   for(i=len-1; i>=0; --i)
   {
      if(fname[i] == '/')
	 return(fname + i + 1);
   }

   return(fname);
}
