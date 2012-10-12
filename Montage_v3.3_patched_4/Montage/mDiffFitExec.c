/* Module: mDiffFitExec.c

Version  Developer        Date     Change
-------  ---------------  -------  -----------------------
1.2      John Good        29Mar08  Add 'level only' capability
1.1      John Good        01Aug07  Leave more space in the output table columns
1.0      John Good        29Aug06  Baseline code

*/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <math.h>
#include <mtbl.h>

#include "montage.h"

#define MAXSTR 4096

char *svc_value();
char *svc_run  (char *cmd);

char *filePath (char *path, char *fname);
int   checkFile(char *filename);

extern char *optarg;
extern int optind, opterr;

extern int getopt(int argc, char *const *argv, const char *options);

int debug;


/*******************************************************************/
/*                                                                 */
/*  mDiffFitExec                                                   */
/*                                                                 */
/*  This routine combines the mDiff and mFit functionality and     */
/*  optionally discards the difference images as it goes (to       */
/*  minimize the file space needed).   It uses the table of        */
/*  oerlaps found by mOverlaps, running mDiff, then mFitplane      */
/*  on the difference images.  These fits are written to an        */
/*  output file which is then used by mBgModel.                    */
/*                                                                 */
/*******************************************************************/

int main(int argc, char **argv)
{
   int    ch, stat, ncols, count, ffailed, warning, dfailed;
   int    keepAll, noAreas, levelOnly;

   int    icntr1;
   int    icntr2;
   int    ifname1;
   int    ifname2;
   int    idiffname;

   int    cntr1;
   int    cntr2;

   char   fname1  [MAXSTR];
   char   fname2  [MAXSTR];
   char   diffname[MAXSTR];
   char   template[MAXSTR];
   char   rmname  [MAXSTR];

   char   tblfile [MAXSTR];
   char   fitfile [MAXSTR];
   char   diffdir [MAXSTR];
   char   path    [MAXSTR];

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
   double npixel;
   double rms;
   double boxx;
   double boxy;
   double boxwidth;
   double boxheight;
   double boxangle;

   FILE   *fout;


   /***************************************/
   /* Process the command-line parameters */
   /***************************************/

   debug   = 0;
   noAreas = 0;
   keepAll = 0;

   levelOnly = 0;

   strcpy(path, "");

   opterr = 0;

   fstatus = stdout;

   while ((ch = getopt(argc, argv, "klnp:ds:")) != EOF) 
   {
      switch (ch) 
      {
         case 'k':
            keepAll = 1;
            break;

         case 'l':
            levelOnly = 1;
            break;

         case 'p':
	    strcpy(path, optarg);
	    break;

         case 'd':
            debug = 1;
            break;

         case 'n':
            noAreas = 1;
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
	    printf("[struct stat=\"ERROR\", msg=\"Usage: %s [-d] [-l(evel-only)] [-n(o-areas)] [-p projdir] [-s statusfile] diffs.tbl template.hdr diffdir fits.tbl\"]\n", argv[0]);
            exit(1);
            break;
      }
   }

   if (argc - optind < 4) 
   {
      printf("[struct stat=\"ERROR\", msg=\"Usage: %s [-d] [-l(evel-only)] [-n(o-areas)] [-p projdir] [-s statusfile] diffs.tbl template.hdr diffdir fits.tbl\"]\n", argv[0]);
      exit(1);
   }

   strcpy(tblfile,  argv[optind]);
   strcpy(template, argv[optind + 1]);
   strcpy(diffdir,  argv[optind + 2]);
   strcpy(fitfile,  argv[optind + 3]);

   fout = fopen(fitfile, "w+");

   if(fout == (FILE *)NULL)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Can't open output file.\"]\n");
      fflush(stdout);
      exit(1);
   }


   /***************************************/ 
   /* Open the difference list table file */
   /***************************************/ 

   ncols = topen(tblfile);

   if(ncols <= 0)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Invalid diffs metadata file: %s\"]\n",
         tblfile);
      exit(1);
   }

   icntr1    = tcol( "cntr1");
   icntr2    = tcol( "cntr2");
   ifname1   = tcol( "plus");
   ifname2   = tcol( "minus");
   idiffname = tcol( "diff");

   if(icntr1    < 0
   || icntr2    < 0
   || ifname1   < 0
   || ifname2   < 0
   || idiffname < 0)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Need columns: cntr1 cntr2 plus minus diff\"]\n");
      exit(1);
   }


   /*********************************************/ 
   /* Read the records and call mDiff/mFitPlane */
   /*********************************************/ 

   count   = 0;
   ffailed = 0;
   warning = 0;
   dfailed = 0;

   fprintf(fout, "|   plus  |  minus  |         a      |        b       |        c       |    crpix1    |    crpix2    |   xmin   |   xmax   |   ymin   |   ymax   |   xcenter   |   ycenter   |    npixel   |      rms       |      boxx      |      boxy      |    boxwidth    |   boxheight    |     boxang     |\n");
   fflush(fout);

   while(1)
   {
      stat = tread();

      if(stat < 0)
	 break;

      cntr1 = atoi(tval(icntr1));
      cntr2 = atoi(tval(icntr2));

      strcpy(fname1,   filePath(path, tval(ifname1)));
      strcpy(fname2,   filePath(path, tval(ifname2)));
      strcpy(diffname, tval(idiffname));

      if(diffname[strlen(diffname)-1] != 's')
         strcat(diffname, "s");

      if(noAreas)
         sprintf(cmd, "mDiff -n %s %s %s %s", fname1, fname2,
            filePath(diffdir, diffname), template);
      else
         sprintf(cmd, "mDiff %s %s %s %s", fname1, fname2,
            filePath(diffdir, diffname), template);

      if(debug)
      {
         printf("[%s]\n", cmd);
         fflush(stdout);
      }

      svc_run(cmd);

      if(debug)
      {
         strcpy(cmd, svc_value((char *)NULL));

         printf("-> %s\n\n", cmd);
         fflush(stdout);
      }

      strcpy( status, svc_value( "stat" ));

      if(strcmp( status, "ABORT") == 0)
      {
         strcpy( msg, svc_value( "msg" ));

         fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"%s\"]\n", msg);
         fflush(stdout);

         exit(1);
      }

      ++count;

      if(strcmp( status, "ERROR"  ) == 0
      || strcmp( status, "WARNING") == 0)
         ++dfailed;

      else
      {
	 if(levelOnly)
	    sprintf(cmd, "mFitplane -l %s", filePath(diffdir, diffname));
	 else
	    sprintf(cmd, "mFitplane %s", filePath(diffdir, diffname));

	 if(debug)
	 {
	    printf("[%s]\n", cmd);
	    fflush(stdout);
	 }

	 svc_run(cmd);

         if(debug)
         {
            strcpy(cmd, svc_value((char *)NULL));

            printf("-> %s\n\n", cmd);
            fflush(stdout);
         }

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
	 {
	    ++ffailed;

	    if(debug)
	    {
	       printf("ERROR: %s\n", svc_value( "msg" ));
	       fflush(stdout);
	    }
	 }
	 else if(strcmp( status, "WARNING") == 0)
	 {
	    ++warning;

	    if(debug)
	    {
	       printf("WARNING: %s\n", svc_value( "msg" ));
	       fflush(stdout);
	    }
	 }
	 else
	 {
	    a         = atof(svc_value("a"));
	    b         = atof(svc_value("b"));
	    c         = atof(svc_value("c"));
	    crpix1    = atof(svc_value("crpix1"));
	    crpix2    = atof(svc_value("crpix2"));
	    xmin      = atoi(svc_value("xmin"));
	    xmax      = atoi(svc_value("xmax"));
	    ymin      = atoi(svc_value("ymin"));
	    ymax      = atoi(svc_value("ymax"));
	    xcenter   = atof(svc_value("xcenter"));
	    ycenter   = atof(svc_value("ycenter"));
	    npixel    = atof(svc_value("npixel"));
	    rms       = atof(svc_value("rms"));
	    boxx      = atof(svc_value("boxx"));
	    boxy      = atof(svc_value("boxy"));
	    boxwidth  = atof(svc_value("boxwidth"));
	    boxheight = atof(svc_value("boxheight"));
	    boxangle  = atof(svc_value("boxang"));

            fprintf(fout, " %9d %9d %16.5e %16.5e %16.5e %14.2f %14.2f %10d %10d %10d %10d %13.2f %13.2f %13.0f %16.5e %16.1f %16.1f %16.1f %16.1f %16.1f \n",
               cntr1, cntr2, a, b, c, crpix1, crpix2, xmin, xmax, ymin, ymax,
               xcenter, ycenter, npixel, rms, boxx, boxy, boxwidth, boxheight, boxangle);
	    fflush(fout);
	 }
      }


      /* Remove the diff files */

      if(!keepAll)
      {
         strcpy(rmname, filePath(diffdir, diffname));

	 if(debug)
	 {
	    printf("Remove [%s]\n", rmname);
	    fflush(stdout);
         }

	 unlink(rmname);

	 if(!noAreas)
	 {
	    rmname[strlen(rmname)-5] = '\0';
	    strcat(rmname, "_area.fits");

	    if(debug)
	    {
	       printf("Remove [%s]\n", rmname);
	       fflush(stdout);
	    }

	    unlink(rmname);
	 }
      }
   }

   fprintf(fstatus, "[struct stat=\"OK\", count=%d, diff_failed=%d, fit_failed=%d, warning=%d]\n", 
      count, dfailed, ffailed, warning);
   fflush(stdout);

   exit(0);
}
