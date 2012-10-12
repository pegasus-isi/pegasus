/* Module: mImgtbl.c

Version  Developer        Date     Change
-------  ---------------  -------  -----------------------
1.10     John Good        29Sep04  Added file size in MByte to table
1.9      John Good        12Aug04  Made tmp file for unzip unique
1.8      John Good        18Mar04  Added mode to read the candidate
				   image list from a table file
1.7      John Good        14Jan03  Added "bad image" output option.
1.6      John Good        25Nov03  Added extern optarg references
1.5      John Good        23Aug03  Added 'status file' output mode.
                                   Added check for trailing slash on
				   file path.  Added processing for
				   "-f" (additional keyword) flag
1.4      John Good        27Jun03  Added a few comments for clarity
1.3      John Good        04May03  Added check for ordering of corners
1.2      John Good        18Mar03  Added a count for bad FITS files
				   to the output.
1.1      John Good        14Mar03  Modified to use only full-path
				   or no-path file names and to 
				   use getopt() for command-line 
				   parsing. Check to see if directory
				   exists.
1.0      John Good        29Jan03  Baseline code

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

char *mktemp(char *template);

int   debug;
int   showbad;
int   recursiveMode;
int   processAreaFiles;
int   showCorners;
int   cntr;
int   failed;
int   hdrlen;
FILE *tblf;
FILE *ffields;

typedef struct
{
 char name  [128];
 char type  [128];
 char value [128];
 char defval[128];
 int  width;
}
FIELDS;

FIELDS *fields;
int     nfields;

int     badwcs = 0;

void get_files   (char*); 
void get_list    (char*, int); 

int  get_hdr     (char*, struct Hdr_rec*, char*);
void print_rec   (struct Hdr_rec*);

int  update_table(char *tblname);

struct Hdr_rec hdr_rec;


/*************************************************************************/
/*                                                                       */
/*  mImgtbl                                                              */
/*                                                                       */
/*  Montage is a set of general reprojection / coordinate-transform /    */
/*  mosaicking programs.  Any number of input images can be merged into  */
/*  an output FITS file.  The attributes of the input are read from the  */
/*  input files; the attributes of the output are read a combination of  */
/*  the command line and a FITS header template file.                    */
/*                                                                       */
/*  This module, mImgtbl, makes a list (with WCS information) of all     */
/*  FITS image files in the named directory (and optionally recursively) */
/*                                                                       */
/*************************************************************************/

int main(int argc, char **argv)
{
    int   c, istat, ncols, ifname, fromlist;
    char  pathname [256];
    char  tblname  [256];
    char  line     [1024];
    char *end;

    int   maxfields;

    char *ptr, *pname, *ptype, *pwidth;

    struct stat type;

    cntr   = 0;
    failed = 0;

    strcpy (pathname, "");
    strcpy (tblname,  "");

    debug            = 0;
    showbad          = 0;
    recursiveMode    = 0;
    processAreaFiles = 0;
    showCorners      = 0;

    fstatus = stdout;

    fromlist = 0;

    while ((c = getopt(argc, argv, "rcadbs:f:t:")) != -1) 
    {
       switch (c) 
       {
	  case 'r':
	     recursiveMode = 1;
	     break;
 
	  case 'c':
	     showCorners = 1;
	     break;
 
	  case 'a':
	     processAreaFiles = 1;
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

	  case 'f':
	     if((ffields = fopen(optarg, "r")) == (FILE *)NULL)
	     {
		printf ("[struct stat=\"ERROR\", msg=\"Cannot open field list file: %s\"]\n",
		   optarg);
		exit(1);
	     }

	     nfields   = 0;
	     maxfields = 32;

	     fields = (FIELDS *)
			  malloc(maxfields * sizeof(FIELDS));

	     while(fgets(line, 1024, ffields) != (char *)NULL)
	     {
		while(line[strlen(line)-1] == '\r'
		   || line[strlen(line)-1] == '\n')
		      line[strlen(line)-1]  = '\0';

		ptr = line;

		end = line + strlen(line);

		while(ptr < end && 
		     (*ptr == ' ' || *ptr == '\t'))
		   ++ptr;

		if(ptr == end)
		   break;

		pname = ptr;

		while(ptr < end && 
		     *ptr != ' ' && *ptr != '\t')
		   ++ptr;

		*ptr = '\0';
		++ptr;

		while(ptr < end && 
		     (*ptr == ' ' || *ptr == '\t'))
		   ++ptr;

		ptype = ptr;

		while(ptr < end && 
		     *ptr != ' ' && *ptr != '\t')
		   ++ptr;

		*ptr = '\0';
		++ptr;

		while(ptr < end && 
		     (*ptr == ' ' || *ptr == '\t'))
		   ++ptr;

		pwidth = ptr;

		while(ptr < end && 
		     *ptr != ' ' && *ptr != '\t')
		   ++ptr;

		*ptr = '\0';

		strcpy(fields[nfields].name, pname);
		strcpy(fields[nfields].type, ptype);

		fields[nfields].width = atoi(pwidth);

		if(strlen(fields[nfields].name) > fields[nfields].width)
		   fields[nfields].width = strlen(fields[nfields].name);

		if(strlen(fields[nfields].name) < 1)
		{
		   fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Illegal field name (line %d)\"]\n", nfields);
		   exit(1);
		}

		if(strlen(fields[nfields].type) < 1)
		{
		   fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Illegal field type (line %d)\"]\n", nfields);
		   exit(1);
		}

		strcpy(fields[nfields].value,  "");
		strcpy(fields[nfields].defval, ""); 

		if(debug)
		{
		   printf("DEBUG> fields[%d]: [%s][%s][%s]\n", 
		      nfields, pname, ptype, pwidth);
		   fflush(stdout);
		}

		++nfields;

		if(nfields >= maxfields)
		{
		   maxfields += 32;
		   
		   fields = (FIELDS *)
				realloc(fields, maxfields * sizeof(FIELDS));
		}
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
	fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Usage: %s [-rcadb][-s statusfile][-f fieldlistfile][-t imglist] directory images.tbl\"]\n", argv[0]);
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
       get_files (pathname);

    fclose(tblf);

    update_table(tblname);

    fprintf(fstatus, "[struct stat=\"OK\", count=%d, badfits=%d, badwcs=%d]\n", cntr, failed, badwcs);
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



/* Recursively finds all FITS files     */
/* and passes them to the header reader */

void get_list (char *pathname, int ifname)
{
   char            dirname [MAXLEN], msg  [MAXLEN];
   char            tmpname [MAXLEN], cmd  [MAXLEN];
   char            template[MAXLEN], fname[MAXLEN];

   int             istatus, len;

   struct stat     type;

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
	       strcpy(template, "/tmp/IMXXXXXX");
	       strcpy(tmpname, mktemp(template));
	       strcat(tmpname, ".fits");

	       sprintf(cmd, "gunzip -c %s > %s", dirname, tmpname);
	       system(cmd);

	       istatus = get_hdr (tmpname, &hdr_rec, msg);

	       if (istatus != 0) 
		  failed += istatus;

	       unlink(tmpname);
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

   return;
}
