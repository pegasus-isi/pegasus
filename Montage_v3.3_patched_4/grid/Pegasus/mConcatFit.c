/* Module: mConcatFit.c

Version  Developer        Date     Change
-------  ---------------  -------  -----------------------
1.6      Mei-Hui Su       05May05  Bug fix: svc_free call need argument
1.5      Mei-Hui Su       11Oct04  Fixed handling of npixel, etc. return values
1.4      John Good        08Sep04  Added boxx,...boxangle handling
1.3      John Good        18Mar04  Bug fix: needed svc_free() call
1.2      John Good        25Nov03  Added extern optarg references
1.1      John Good        17Oct03  Added fclose(fstat) call
1.0      John Good        15Sep03  Baseline code

*/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <math.h>
#include <svc.h>
#include <mtbl.h>

#include "montage.h"

#define MAXSTR 4096

extern char *optarg;
extern int optind, opterr;

extern int getopt(int argc, char *const *argv, const char *options);

char *svc_val();

int checkFile(char *filename);

int   debug;


/*******************************************************************/
/*                                                                 */
/*  mConcatFit                                                     */
/*                                                                 */
/*  Working from a table listing a set of mFitplane "status" files */
/*  concatenates the contents of these into a fits.tbl file for    */
/*  mBgModel.                                                      */
/*                                                                 */
/*******************************************************************/

int main(int argc, char **argv)
{
   int    ch, stat, ncols, count, failed, missing, warning;

   int    icntr1;
   int    icntr2;
   int    istatfile;

   int    cntr1;
   int    cntr2;

   char   statfile[MAXSTR];

   char   tblfile [MAXSTR];
   char   fitfile [MAXSTR];
   char   statdir [MAXSTR];

   char   line    [MAXSTR];
   char   msg     [MAXSTR];
   char   val     [MAXSTR];
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
   double npixel;
   double rms;
   double boxx;
   double boxy;
   double boxwidth;
   double boxheight;
   double boxangle;

   FILE   *fstat;
   FILE   *fout;

   SVC *svc=NULL;



   /***************************************/
   /* Process the command-line parameters */
   /***************************************/

   debug = 0;

   opterr = 0;

   fstatus = stdout;

   while ((ch = getopt(argc, argv, "ds:")) != EOF) 
   {
      switch (ch) 
      {
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
            printf("[struct stat=\"ERROR\", msg=\"Usage: %s [-d] [-s statusfile] statfiles.tbl fits.tbl statdir\"]\n", argv[0]);
            exit(1);
            break;
      }
   }

   if (argc - optind < 3) 
   {
      printf("[struct stat=\"ERROR\", msg=\"Usage: %s [-d] [-s statusfile] statfiles.tbl fits.tbl statdir\"]\n", argv[0]);
      exit(1);
   }

   strcpy(tblfile, argv[optind]);
   strcpy(fitfile, argv[optind + 1]);
   strcpy(statdir, argv[optind + 2]);

   fout = fopen(fitfile, "w+");

   if(fout == (FILE *)NULL)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Can't open output file.\"]\n");
      fflush(stdout);
      exit(1);
   }


   /**************************************************/ 
   /* Open the difference fit status file list table */
   /**************************************************/ 

   ncols = topen(tblfile);

   if(ncols <= 0)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Invalid statfiles metadata file: %s\"]\n",
         tblfile);
      exit(1);
   }

   icntr1    = tcol( "cntr1");
   icntr2    = tcol( "cntr2");
   istatfile = tcol( "stat");

   if(icntr1    < 0
   || icntr2    < 0
   || istatfile < 0)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Need columns: cntr1 cntr2 stat\"]\n");
      exit(1);
   }


   /***************************************/ 
   /* Read the records and call mFitPlane */
   /***************************************/ 

   count   = 0;
   failed  = 0;
   missing = 0;
   warning = 0;

   fprintf(fout, "| plus|minus|       a    |      b     |      c     | crpix1  | crpix2  | xmin | xmax | ymin | ymax | xcenter | ycenter |  npixel |    rms     |    boxx    |    boxy    |  boxwidth  | boxheight  |   boxang   |\n");
   fflush(fout);

   while(1)
   {
      stat = tread();

      if(stat < 0)
         break;

      cntr1 = atoi(tval(icntr1));
      cntr2 = atoi(tval(icntr2));

      strcpy(statfile, statdir);
      strcat(statfile, "/");
      strcat(statfile, tval(istatfile));

      if(checkFile(statfile))
      {
         ++count;
         ++missing;
         continue;
      }

      fstat = fopen(statfile, "r");

      if(fstat == (FILE *)NULL)
      {
         ++count;
         ++missing;
         continue;
      }


      if(fgets(line, MAXSTR, fstat) == (char *)NULL)
      {
         ++count;
         ++missing;
         continue;
      }

      svc=svc_struct(line); 

      strcpy( status, svc_val(line, "stat", val));

      if(strcmp( status, "ABORT") == 0)
      {
         strcpy( msg, svc_val(line, "msg", val ));

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
         a         = atof(svc_val(line, "a", val));
         b         = atof(svc_val(line, "b", val));
         c         = atof(svc_val(line, "c", val));
         crpix1    = atof(svc_val(line, "crpix1", val));
         crpix2    = atof(svc_val(line, "crpix2", val));
         xmin      = atoi(svc_val(line, "xmin", val));
         xmax      = atoi(svc_val(line, "xmax", val));
         ymin      = atoi(svc_val(line, "ymin", val));
         ymax      = atoi(svc_val(line, "ymax", val));
         xcenter   = atof(svc_val(line, "xcenter", val));
         ycenter   = atof(svc_val(line, "ycenter", val));
         npixel    = atof(svc_val(line, "npixel", val));
         rms       = atof(svc_val(line, "rms", val));
         boxx      = atof(svc_val(line, "boxx", val));
         boxy      = atof(svc_val(line, "boxy", val));
         boxwidth  = atof(svc_val(line, "boxwidth", val));
         boxheight = atof(svc_val(line, "boxheight", val));
         boxangle  = atof(svc_val(line, "boxang", val));


         fprintf(fout, " %5d %5d %12.5e %12.5e %12.5e %9.2f %9.2f %6d %6d %6d %6d %9.2f %9.2f %9.0f %12.5e %12.1f %12.1f %12.1f %12.1f %12.1f\n",
            cntr1, cntr2, a, b, c, crpix1, crpix2, xmin, xmax, ymin, ymax,
            xcenter, ycenter, npixel, rms, boxx, boxy, boxwidth, boxheight, boxangle);
         fflush(fout);
      }

      svc_free(svc);

      fclose(fstat);
   }

   fprintf(fstatus, "[struct stat=\"OK\", count=%d, failed=%d, missing=%d, warning=%d]\n", count, failed, missing, warning);
   fflush(stdout);

   exit(0);
}
