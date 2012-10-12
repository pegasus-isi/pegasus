/* Module: mHdrtbl.c

Version  Developer        Date     Change
-------  ---------------  -------  -----------------------
1.1      John Good        15Sep05  Minor fixes to argument documentation
1.0      John Good        31Jan05  Baseline code

*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <string.h>
#include <dirent.h>
#include <math.h>
#include <fitshead.h>
#include "mtbl.h"
#include "montage.h"
#include "hdr_rec.h"

#define MAXLEN 4096

extern char *optarg;
extern int optind, opterr;

extern int getopt(int argc, char *const *argv, const char *options);

int   debug;
int   showbad;
int   recursiveMode;
int   showCorners;
int   cntr;
int   failed;
int   hdrlen;
FILE *tblf;

void get_nfiles  (char*); 
void get_list    (char*, int); 

int  get_hhdr    (char*, struct Hdr_rec*, char*);
void print_hrec  (struct Hdr_rec*);
int  update_table(char *tblname);

void get_hfiles  (char *pathname);

char *mktemp(char *template);

struct Hdr_rec hdr_rec;


/*************************************************************************/
/*                                                                       */
/*  mHdrtbl                                                              */
/*                                                                       */
/*  Montage is a set of general reprojection / coordinate-transform /    */
/*  mosaicking programs.  Any number of input images can be merged into  */
/*  an output FITS file.  The attributes of the input are read from the  */
/*  input files; the attributes of the output are read a combination of  */
/*  the command line and a FITS header template file.                    */
/*                                                                       */
/*  This module, mHdrtbl, makes a list (with WCS information) of all     */
/*  header files in the named directory (and optionally recursively)     */
/*                                                                       */
/*************************************************************************/

int main(int argc, char **argv)
{
    int   c, istat, ncols, ifname, fromlist;
    char  pathname [256];
    char  tblname  [256];

    struct stat type;

    cntr   = 0;
    failed = 0;

    strcpy (pathname, "");
    strcpy (tblname,  "");

    debug            = 0;
    showbad          = 0;
    recursiveMode    = 0;
    showCorners      = 0;

    fstatus = stdout;

    fromlist = 0;

    while ((c = getopt(argc, argv, "rcdbs:t:")) != -1) 
    {
       switch (c) 
       {
	  case 'r':
	     recursiveMode = 1;
	     break;
 
	  case 'c':
	     showCorners = 1;
	     break;
 
	  case 'b':
	     showbad = 1;
	     break;

	  case 'd':
	     debug = 1;
	     break;

	  case 's':
	     if((fstatus = fopen(optarg, "w+")) == (FILE *)NULL)
	     {
		printf ("[struct stat=\"ERROR\", msg=\"Cannot open status file: %s\"]\n",
		   optarg);
		exit(1);
	     }
	     break;

	  case 't':

	     fromlist = 1;

	     ncols = topen(optarg);

	     if(ncols < 1)
	     {
		printf ("[struct stat=\"ERROR\", msg=\"Cannot open image list file: %s\"]\n",
		   optarg);
		exit(1);
	     }

	     ifname = tcol( "fname");

	     if(ifname < 0)
	        ifname = tcol( "file");

	     if(ifname < 0)
	     {
	        fprintf (fstatus, "[struct stat=\"ERROR\", msg=\"Image table needs column fname/file\"]\n");
	        exit(1);
	     }

	     break;


	  default:
	     fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Illegal argument: -%c\"]\n", c);
	     exit(1);
	     break;
       }
    }

    if (argc - optind < 2) 
    {
	fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Usage: %s [-rcdb][-s statusfile][-t imglist] directory images.tbl\"]\n", argv[0]);
	exit(1);
    }

    strcpy(pathname, argv[optind]);
    strcpy(tblname,  argv[optind+1]);

    if(strlen(pathname) > 1
    && pathname[strlen(pathname)-1] == '/')
       pathname[strlen(pathname)-1]  = '\0';


    /* Check to see if directory exists */

    istat = stat(pathname, &type);

    if(istat < 0)
    {
       fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Cannot access %s\"]\n", pathname);
       exit(1);
    }
 
    else if (S_ISDIR(type.st_mode) != 1)
    {
       fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"%s is not a directory\"]\n", pathname);
       exit(1);
    }


    hdrlen = 0;
    if(pathname[0] != '/')
       hdrlen = strlen(pathname);

    if(hdrlen && pathname[strlen(pathname) - 1] != '/')
       ++hdrlen;

    if(debug)
    {
       fprintf(fstatus, "DEBUG: header = [%s](%d)\n", pathname, hdrlen);
       fflush(stdout);
    }

    tblf = fopen(tblname, "w+");

    if(tblf == (FILE *)NULL)
    {
	fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Can't open output table.\"]\n");
	exit(1);
    }

    if(fromlist)
       get_list  (pathname, ifname);
    else
       get_hfiles (pathname);

    fclose(tblf);

    update_table(tblname);

    fprintf(fstatus, "[struct stat=\"OK\", count=%d, badhdr=%d]\n", cntr, failed);
    exit(0);
}


int update_table(char *tblname)
{
   char  str[MAXLEN], tmpname[128], template[128];
   int   i, len, maxlen;
   FILE *fdata, *ftmp;

   strcpy(template, "/tmp/IMTXXXXXX");
   strcpy(tmpname, (char *)mktemp(template));
   
   fdata = fopen(tblname, "r");

   if(fdata == (FILE *)NULL)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Can't open copy table.\"]\n");
      exit(1);
   }

   ftmp  = fopen(tmpname, "w+");

   if(ftmp == (FILE *)NULL)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Can't open tmp (in) table.\"]\n");
      exit(1);
   }

   maxlen = 0;
   while(1)
   {
      if(fgets(str, MAXLEN, fdata) == (char *)NULL)
	 break;

      str[MAXLEN-1] = '\0';

      len = strlen(str) - 1;

      if(len > maxlen)
	 maxlen = len;

      fputs(str, ftmp);
   }

   fclose(fdata);
   fclose(ftmp);

   ftmp  = fopen(tmpname, "r");

   if(ftmp == (FILE *)NULL)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Can't open tmp (out) table.\"]\n");
      exit(1);
   }

   fdata = fopen(tblname, "w+");

   if(fdata == (FILE *)NULL)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Can't open final table.\"]\n");
      exit(1);
   }


   while(1)
   {
      if(fgets(str, MAXLEN, ftmp) == (char *)NULL)
	 break;

      if(str[strlen(str) - 1] == '\n')
         str[strlen(str) - 1]  = '\0';

      if(str[0] == '\\')
      {
	 strcat(str, "\n");
	 fputs(str, fdata);
	 continue;
      }

      len = strlen(str);

      for(i=len; i<MAXLEN; ++i)
	 str[i] =  ' ';
      
      str[maxlen] = '\0';

      if(str[0] == '|')
	 strcat(str, "|\n");
      else
	 strcat(str, " \n");

      fputs(str, fdata);
   }

   fclose(fdata);
   fclose(ftmp);

   unlink(tmpname);

   return 0;
}



/* Recursively finds all header files   */
/* and passes them to the header reader */

void get_list (char *pathname, int ifname)
{
   char        dirname [MAXLEN];
   char        msg     [MAXLEN];
   char        fname   [MAXLEN];

   int         istatus, len;

   struct stat type;

   while (1)
   {
      istatus = tread();

      if(istatus < 0)
	 break;

      strcpy(fname, tval(ifname));

      if(debug)
      {
	 printf("DEBUG:  entry [%s]\n", fname);
	 fflush(stdout);
      }

      sprintf (dirname, "%s/%s", pathname, fname);

      strcpy (hdr_rec.fname, fname);

      if(debug)
      {
	 printf("DEBUG: [%s] -> [%s]\n", dirname, hdr_rec.fname);
	 fflush(stdout);
      }

      if (stat(dirname, &type) == 0) 
      {
	 len = strlen(dirname);

	 if(debug)
	 {
	    printf("DEBUG: Found file      [%s]\n", dirname);
	    fflush(stdout);
	 }

	 if ((strncmp(dirname+len-4, ".hdr", 4) == 0) ||
	     (strncmp(dirname+len-4, ".HDR", 4) == 0))
	 { 
	    msg[0] = '\0';

	    istatus = get_hhdr (dirname, &hdr_rec, msg);

	    if (istatus != 0) 
	       failed += istatus;
	 }
      }
   }

   return;
}
