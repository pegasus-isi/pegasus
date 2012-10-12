/* Module: get_hfiles.c

Version  Developer        Date     Change
-------  ---------------  -------  -----------------------
1.0      John Good        29Jan03  Baseline code

*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/param.h>
#include <math.h>
#include <fitshead.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>
#include "montage.h"
#include "hdr_rec.h"

#define MAXSTR 4096

extern int   debug;
extern int   recursiveMode;
extern int   processAreaFiles;
extern int   cntr;
extern int   failed;
extern int   hdrlen;

int  get_hhdr   (char*, struct Hdr_rec*, char*);
void print_hrec (struct Hdr_rec*);

struct Hdr_rec hdr_rec;


/* Recursively finds all FITS files     */
/* and passes them to the header reader */

void get_hfiles (char *pathname)
{
   char            dirname[MAXSTR], msg[MAXSTR];
   int             istatus, len;
   DIR            *dp;
   struct dirent  *entry;
   struct stat     type;

   dp = opendir (pathname);

   if(debug)
   {
      printf("DEBUG: Opening path    [%s]\n", pathname);
      fflush(stdout);
   }

   if (dp == NULL) 
   {
      fprintf (fstatus, "[struct stat=\"ERROR\", msg=\"Non-existent directory.\"]\n");
      exit(1);
   }

   while ((entry=(struct dirent *)readdir(dp)) != (struct dirent *)0) 
   {
      if(debug)
      {
	 printf("DEBUG:  entry [%s]\n", entry->d_name);
	 fflush(stdout);
      }

      sprintf (dirname, "%s/%s", pathname, entry->d_name);

      if(strncmp(dirname, "./", 2) == 0)
	 strcpy (hdr_rec.fname, dirname+2);
      else
	 strcpy (hdr_rec.fname, dirname+hdrlen);

      if(debug)
      {
	 printf("DEBUG: [%s] -> [%s]\n", dirname, hdr_rec.fname);
	 fflush(stdout);
      }

      if (stat(dirname, &type) == 0) 
      {
         if (S_ISDIR(type.st_mode) == 1)
         {
            if (recursiveMode
            && (strcmp(entry->d_name, "." ) != 0)
            && (strcmp(entry->d_name, "..") != 0))
            {
               if(debug)
               {
                  printf("DEBUG: Found directory [%s]\n", dirname);
                  fflush(stdout);
               }

               get_hfiles (dirname);
            }
         }
         else
	 {
	    len = strlen(dirname);

	    if(debug)
	    {
	       printf("DEBUG: Found file      [%s]\n", dirname);
	       fflush(stdout);
	    }

	    if ((strncmp(dirname+len-4, ".hdr", 4) == 0) ||
		(strncmp(dirname+len-8, ".HDR", 4) == 0)) 
	    { 
	       msg[0] = '\0';

	       istatus = get_hhdr (dirname, &hdr_rec, msg);

	       if (istatus != 0) 
		  failed += istatus;
	    }
	 }
      }
   }

   closedir(dp);
   return;
}
