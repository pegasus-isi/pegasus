/* Module: mFlattenExec.c

Version  Developer        Date     Change
-------  ---------------  -------  -----------------------
1.9      John Good        29Mar08  Add 'level only' capability
1.8      John Good        13Oct04  Changed format for printing time
1.7      John Good        03Aug04  Added count for mFitplane WARNINGs
1.6      John Good        17May04  Added "no areas" option
1.5      John Good        25Nov03  Added extern optarg references
1.4      John Good        25Aug03  Added status file processing
1.3      John Good        24Mar03  Updated handling of svc_run aborts
1.2      John Good        22Mar03  Fixed but in mBackground call
1.1      John Good        14Mar03  Added filePath() processing,
				   -p argument, and getopt()
				   argument processing.  Return error
				   if mFitplane or mBackground not in
				   path.  Check for existence of output
				   directory.
1.0      John Good        29Jan03  Baseline code

*/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <mtbl.h>

#include "montage.h"

#define MAXSTR 4096

char *svc_value();
char *svc_run  (char *cmd);

char *filePath (char *path, char *fname);
char *fileName (char *filename);


extern char *optarg;
extern int optind, opterr;

extern int getopt(int argc, char *const *argv, const char *options);

int debug;


/*******************************************************************/
/*                                                                 */
/*  mFlattenExec                                                   */
/*                                                                 */
/*  This executive "flattens" a set of images.  That is, it runs   */
/*  mFitplane on each one, then uses the fit to remove a           */
/*  background and create a new image.                             */
/*                                                                 */
/*******************************************************************/

int main(int argc, char **argv)
{
   int    ch, istat, ncols, count, failed;
   int    levelOnly, warning, noAreas;

   int    ifname;

   char   fname   [MAXSTR];

   char   path    [MAXSTR];
   char   tblfile [MAXSTR];
   char   flatdir [MAXSTR];

   char   cmd     [MAXSTR];
   char   msg     [MAXSTR];
   char   status  [32];

   double a;
   double b;
   double c;
   double crpix1;
   double crpix2;
   int    xmin;
   int    xmax;
   int    ymin;
   int    ymax;
   double xcenter;
   double ycenter;
   double rms;

   static time_t currtime, start;

   struct stat type;


   /***************************************/
   /* Process the command-line parameters */
   /***************************************/

   debug   = 0;
   noAreas = 0;

   levelOnly = 0;

   strcpy(path, "");

   opterr = 0;

   fstatus = stdout;

   while ((ch = getopt(argc, argv, "lnp:ds:")) != EOF) 
   {
      switch (ch) 
      {
         case 'l':
            levelOnly = 1;
            break;

         case 'p':
	    strcpy(path, optarg);
            break;

         case 'n':
            noAreas = 1;
            break;

         case 'd':
            debug = 1;
            break;

         case 's':
            if((fstatus = fopen(optarg, "w+")) == (FILE *)NULL)
            {
               printf("[struct stat=\"ERROR\", msg=\"Cannot open status file: %s\"]\n",
                  optarg);
               exit(1);
            }
            break;

         default:
	    printf("[struct stat=\"ERROR\", msg=\"Usage: %s [-p imgdir] [-d] [-n(o-areas)] [-s statusfile] images.tbl flatdir\"]\n", argv[0]);
            exit(1);
            break;
      }
   }

   if (argc - optind < 2) 
   {
      printf("[struct stat=\"ERROR\", msg=\"Usage: %s [-p imgdir] [-d] [-n(o-areas)] [-s statusfile] images.tbl flatdir\"]\n", argv[0]);
      exit(1);
   }

   strcpy(tblfile, argv[optind]);
   strcpy(flatdir, argv[optind + 1]);



   /**********************************/
   /* Check to see if diffdir exists */
   /**********************************/

   istat = stat(flatdir, &type);

   if(istat < 0)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Cannot access %s\"]\n", flatdir);
      exit(1);
   }

   else if (S_ISDIR(type.st_mode) != 1)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"%s is not a directory\"]\n", flatdir);
      exit(1);
   }


   /**********************************/ 
   /* Open the image list table file */
   /**********************************/ 

   ncols = topen(tblfile);

   if(ncols <= 0)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Invalid image metadata file: %s\"]\n",
         tblfile);
      exit(1);
   }

   ifname = tcol("fname");

   if(ifname < 0)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Need column 'file' in image metadata file %s\"]\n",
	 tblfile);
      exit(1);
   }


   /***************************************************/ 
   /* Read the records and call mFitplane/mBackground */
   /***************************************************/ 

   count   = 0;
   failed  = 0;
   warning = 0;

   time(&currtime);
   start = currtime;

   while(1)
   {
      istat = tread();

      if(istat < 0)
	 break;

      strcpy(fname, filePath(path, tval(ifname)));

      if(levelOnly)
	 sprintf(cmd, "mFitplane -l %s", fname);
      else
	 sprintf(cmd, "mFitplane %s", fname);

      if(debug)
      {
	 printf("[%s]\n", cmd);
	 fflush(stdout);
      }

      svc_run(cmd);

      strcpy( status, svc_value( "stat" ));

      if(strcmp( status, "ABORT") == 0)
      {
	 strcpy( msg, svc_value( "msg" ));

	 fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"%s\"]\n", msg);
	 fflush(stdout);

	 exit(1);
      }

      ++count;
      if(strcmp( status, "ERROR") == 0)
	 ++failed;
      else if(strcmp( status, "WARNING") == 0)
	 ++warning;
      else
      {
	 a       = atof(svc_value("a"));
	 b       = atof(svc_value("b"));
	 c       = atof(svc_value("c"));
	 crpix1  = atof(svc_value("crpix1"));
	 crpix2  = atof(svc_value("crpix2"));
	 xmin    = atoi(svc_value("xmin"));
	 xmax    = atoi(svc_value("xmax"));
	 ymin    = atoi(svc_value("ymin"));
	 ymax    = atoi(svc_value("ymax"));
	 xcenter = atof(svc_value("xcenter"));
	 ycenter = atof(svc_value("ycenter"));
	 rms     = atof(svc_value("rms"));

         if(noAreas)
	    sprintf(cmd, "mBackground -n %s %s %12.5e %12.5e %12.5e",
	       fname, filePath(flatdir, fileName(fname)), a, b, c);
         else
	    sprintf(cmd, "mBackground %s %s %12.5e %12.5e %12.5e",
	       fname, filePath(flatdir, fileName(fname)), a, b, c);

	 if(debug)
	 {
	    printf("[%s]\n", cmd);
	    fflush(stdout);
	 }

	 svc_run(cmd);

	 strcpy( status, svc_value( "stat" ));

	 if(strcmp( status, "ABORT") == 0)
	 {
	    strcpy( msg, svc_value( "msg" ));

	    fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"%s\"]\n", msg);
	    fflush(stdout);

	    exit(1);
	 }

	 if(strcmp( status, "ERROR") == 0)
	    ++failed;
      }

      time(&currtime);

      if(debug)
      {
	 printf("\nTime: %.0f seconds\n\n-------------------------------\n", 
	    (double)(currtime - start));
	 fflush(stdout);
	 
	 start = currtime;
      }
   }

   fprintf(fstatus, "[struct stat=\"OK\", count=%d, failed=%d, warning=%d]\n",
      count, failed, warning);
   fflush(stdout);

   exit(0);
}
