/* Module: get_files.c

Version  Developer        Date     Change
-------  ---------------  -------  -----------------------
1.7      John Good        17Jul04  Use unique temporary file for unzipping
1.6      John Good        01Jul04  Add path to name if using "./"
				   construct.
1.5      John Good        25Aug03  Added status file processing
1.4      John Good        27Jun03  Added a few comments for clarity
1.3      John Good        18Mar03  Added processing for "bad FITS" count
1.2      John Good        14Mar03  Modified to use only full-path
				   or no-path file names
1.1      John Good        12Feb03  Changed error msg for non-existant dir
1.0      John Good        29Jan03  Baseline code

*/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
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

int  get_hdr   (char*, struct Hdr_rec*, char*);
void print_rec (struct Hdr_rec*);

char *mktemp(char *template);

struct Hdr_rec hdr_rec;


/* Recursively finds all FITS files     */
/* and passes them to the header reader */

void get_files (char *pathname)
{
   char            dirname[MAXSTR], msg[MAXSTR];
   char            template[MAXSTR], tmpname[MAXSTR], cmd[MAXSTR];
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
      return;

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

               get_files (dirname);
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

	    if(!processAreaFiles)
	    {
	       if ((strncmp(dirname+len-9,  "_area.fit",     9 ) == 0) ||
		   (strncmp(dirname+len-9,  "_area.FIT",     9 ) == 0) || 
		   (strncmp(dirname+len-10, "_area.fits",    10) == 0) || 
		   (strncmp(dirname+len-10, "_area.FITS",    10) == 0) ||
		   (strncmp(dirname+len-12, "_area.fit.gz",  12) == 0) ||
		   (strncmp(dirname+len-12, "_area.FIT.gz",  12) == 0) || 
		   (strncmp(dirname+len-13, "_area.fits.gz", 13) == 0) || 
		   (strncmp(dirname+len-13, "_area.FITS.gz", 13) == 0)) 
		  continue;
	    }

	    if ((strncmp(dirname+len-4, ".fit",     4) == 0) ||
		(strncmp(dirname+len-4, ".FIT",     4) == 0) || 
		(strncmp(dirname+len-5, ".fits",    5) == 0) || 
		(strncmp(dirname+len-5, ".FITS",    5) == 0) ||
	        (strncmp(dirname+len-7, ".fit.gz",  7) == 0) ||
		(strncmp(dirname+len-7, ".FIT.gz",  7) == 0) || 
		(strncmp(dirname+len-8, ".fits.gz", 8) == 0) || 
		(strncmp(dirname+len-8, ".FITS.gz", 8) == 0)) 
	    { 
	       msg[0] = '\0';

               if((strncmp(dirname+len-7, ".fit.gz",  7) == 0) ||
                  (strncmp(dirname+len-7, ".FIT.gz",  7) == 0) || 
                  (strncmp(dirname+len-8, ".fits.gz", 8) == 0) || 
                  (strncmp(dirname+len-8, ".FITS.gz", 8) == 0)) 
	       {
		  strcpy(template, "/tmp/IMTXXXXXX");
		  strcpy(tmpname, (char *)mktemp(template));

		  sprintf(cmd, "gunzip -c %s > %s", dirname, tmpname);
		  system(cmd);

	          istatus = get_hdr (tmpname, &hdr_rec, msg);

		  unlink(tmpname);

		  if (istatus != 0) 
		     failed += istatus;
	       }
	       else
	       {
	          istatus = get_hdr (dirname, &hdr_rec, msg);

		  if (istatus != 0) 
		     failed += istatus;
	       }
	    }
	 }
      }
   }

   closedir(dp);
   return;
}
