/* Module: mFitExec.c

Version  Developer        Date     Change
-------  ---------------  -------  -----------------------
1.10     John Good        29Mar08  Add 'level only' capability
1.9      Daniel S. Katz   25Jan07  Fixing some small bugs in the MPI version
                                   that were inadvertently introduced after 1.8
1.8      Daniel S. Katz   15Dec04  Added optional parallel roundrobin
                                   computation
1.7      John Good        03Aug04  Added count for mFitplane WARNINGs
1.6      John Good        20Apr04  Added processing for box fit parameters
1.5      John Good        04Mar03  Added handling for pixel count
1.4      John Good        25Nov03  Added extern optarg references
1.3      John Good        25Aug03  Added status file processing
1.2      John Good        24Mar03  Checked for svc_run() abort (no executable)
1.1      John Good        14Mar03  Modified command-line processing
				   to use getopt() library. Return error
				   if mFitplane not in path.  Checks for
                                   invalid diffs.tbl file.
1.0      John Good        29Jan03  Baseline code

*/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <math.h>
#include <mtbl.h>

#ifdef MPI
#include <mpi.h>
#endif

#include "montage.h"

#define MAXSTR 4096

char *svc_value();
char *svc_run  (char *cmd);

#ifdef MPI
int   i;
#endif

int   checkFile(char *filename);

extern char *optarg;
extern int optind, opterr;

extern int getopt(int argc, char *const *argv, const char *options);


int debug;


/*******************************************************************/
/*                                                                 */
/*  mFitExec                                                       */
/*                                                                 */
/*  After mDiffExec has been run using the table of overlaps found */
/*  by mOverlaps, use this executive to run mFitplane on each of   */
/*  the differences.  Write the fits to a file to be used by       */
/*  mBModel.                                                       */
/*                                                                 */
/*******************************************************************/

int main(int argc, char **argv)
{
   int    ch, stat, ncols, count, failed;
   int    levelOnly, warning, missing;

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

   char   tblfile [MAXSTR];
   char   fitfile [MAXSTR];
   char   diffdir [MAXSTR];

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

#ifdef MPI
   FILE   *fin;
   int    row_number = 0;
   int    MPI_size, MPI_rank, MPI_err;
   int    exit_flag = 0;
   int    sum_tmp;
   char   orig_fitfile [MAXSTR];
   char   tmp          [MAXSTR];
#endif

#ifdef MPI
   /******************/
   /* Initialize MPI */
   /******************/

   MPI_err = MPI_Init(&argc,&argv);
   MPI_err |= MPI_Comm_size(MPI_COMM_WORLD,&MPI_size);
   MPI_err |= MPI_Comm_rank(MPI_COMM_WORLD,&MPI_rank);
   if (MPI_err != 0) {
     printf("[struct stat=\"ERROR\", msg=\"MPI initialization failed\"]\n");
     exit(1);
   }
#endif

   /***************************************/
   /* Process the command-line parameters */
   /***************************************/

   debug = 0;

   levelOnly = 0;

   opterr = 0;

   fstatus = stdout;

   while ((ch = getopt(argc, argv, "dls:")) != EOF) 
   {
      switch (ch) 
      {
         case 'd':
            debug = 1;
            break;

         case 'l':
            levelOnly = 1;
            break;

         case 's':
            if((fstatus = fopen(optarg, "w+")) == (FILE *)NULL)
            {
               printf("[struct stat=\"ERROR\", msg=\"Cannot open status file: %s\"]\n",
                  optarg);
#ifdef MPI
               exit_flag = 1;
#else
               exit(1);
#endif
            }
            break;

         default:
	    printf("[struct stat=\"ERROR\", msg=\"Usage: %s [-d] [-l(evel-only)] [-s statusfile] diffs.tbl fits.tbl diffdir\"]\n", argv[0]);
#ifdef MPI
            exit_flag = 1;
#else
            exit(1);
#endif
            break;
      }
   }

#ifdef MPI  
   MPI_err = MPI_Allreduce(&exit_flag, &sum_tmp, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
   if (sum_tmp != 0) exit(1);     
   exit_flag = 0;
#endif         

   if (argc - optind < 3) 
   {
      printf("[struct stat=\"ERROR\", msg=\"Usage: %s [-d] [-l(evel-only)] [-s statusfile] diffs.tbl fits.tbl diffdir\"]\n", argv[0]);
#ifdef MPI
      exit_flag = 1;
#else
      exit(1);
#endif
   }

#ifdef MPI  
   MPI_err = MPI_Allreduce(&exit_flag, &sum_tmp, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
   if (sum_tmp != 0) exit(1);     
   exit_flag = 0;
#endif         

   strcpy(tblfile, argv[optind]);
   strcpy(fitfile, argv[optind + 1]);
   strcpy(diffdir, argv[optind + 2]);

#ifdef MPI
   /* each process will write its own status file, to be combined later */
   strcpy(orig_fitfile, fitfile);
   (void) sprintf(fitfile, "%s_%d", orig_fitfile, MPI_rank);
#endif

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
#ifdef MPI
      exit_flag = 1;
#else
      exit(1);
#endif
   }

#ifdef MPI  
   MPI_err = MPI_Allreduce(&exit_flag, &sum_tmp, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
   if (sum_tmp != 0) exit(1);     
   exit_flag = 0;
#endif         

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
#ifdef MPI
      exit_flag = 1;
#else
      exit(1);
#endif
   }

#ifdef MPI  
   MPI_err = MPI_Allreduce(&exit_flag, &sum_tmp, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
   if (sum_tmp != 0) exit(1);     
   exit_flag = 0;
#endif         


   /***************************************/ 
   /* Read the records and call mFitplane */
   /***************************************/ 

   count   = 0;
   failed  = 0;
   warning = 0;
   missing = 0;

#ifndef MPI
   fprintf(fout, "| plus|minus|       a    |      b     |      c     | crpix1  | crpix2  | xmin | xmax | ymin | ymax | xcenter | ycenter |  npixel |    rms     |    boxx    |    boxy    |  boxwidth  | boxheight  |   boxang   |\n");
   fflush(fout);
#endif

   while(1)
   {
      stat = tread();

      if(stat < 0)
	 break;
#ifdef MPI
    if (row_number % MPI_size == MPI_rank) {
#endif

      cntr1 = atoi(tval(icntr1));
      cntr2 = atoi(tval(icntr2));

      strcpy(fname1,   tval(ifname1));
      strcpy(fname2,   tval(ifname2));

      strcpy(diffname, diffdir);
      strcat(diffname, "/");
      strcat(diffname, tval(idiffname));

      if(checkFile(diffname))
      {
         ++count;
	 ++missing;
	 continue;
      }

      if(levelOnly)
	 sprintf(cmd, "mFitplane -l %s", diffname);
      else
	 sprintf(cmd, "mFitplane %s", diffname);

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
      {
	 ++failed;

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

	 fprintf(fout, " %5d %5d %12.5e %12.5e %12.5e %9.2f %9.2f %6d %6d %6d %6d %9.2f %9.2f %9.0f %12.5e %12.1f %12.1f %12.1f %12.1f %12.1f\n",
	    cntr1, cntr2, a, b, c, crpix1, crpix2, xmin, xmax, ymin, ymax, 
	    xcenter, ycenter, npixel, rms, boxx, boxy, boxwidth, boxheight, boxangle);
	 fflush(fout);
      }
#ifdef MPI
    } // end if (row_number % MPI_size == MPI_rank)
    row_number++;
#endif
   }

#ifdef MPI
   MPI_err = MPI_Allreduce(&count, &sum_tmp, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
   count = sum_tmp;
   MPI_err = MPI_Allreduce(&failed, &sum_tmp, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
   failed = sum_tmp;
   MPI_err = MPI_Allreduce(&warning, &sum_tmp, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
   warning = sum_tmp;
   MPI_err = MPI_Allreduce(&missing, &sum_tmp, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
   missing = sum_tmp;

   if (MPI_rank == 0) {
#endif
   fprintf(fstatus, "[struct stat=\"OK\", count=%d, failed=%d, warning=%d, missing=%d]\n", 
      count, failed, warning, missing);
   fflush(stdout);
#ifdef MPI
   fout = fopen(orig_fitfile, "w+");

   if(fout == (FILE *)NULL)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Can't open output file.\"]\n");
      fflush(stdout);
   }
   fprintf(fout, "| plus|minus|       a    |      b     |      c     | crpix1  | crpix2  | xmin | xmax | ymin | ymax | xcenter | ycenter |  npixel |    rms     |    boxx    |    boxy    |  boxwidth  | boxheight  |   boxang   |\n");
   fprintf(fout, "|%-60s|%-30s|%10s|\n", "fname", "status", "time");

   for (i=0;i<MPI_size;i++) {
     (void) sprintf(fitfile, "%s_%d", orig_fitfile, i);
     // read all lines from fitfile and copy to orig_fitfile, then rm fitfile
     fin = fopen(fitfile, "r+");
     if(fin == (FILE *)NULL)
       {
         fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Can't open tmp status file.\"]\n");
         fflush(stdout);
        }
     while (fgets(tmp,MAXSTR,fin) != NULL) {
       fputs (tmp,fout);
     }
     fclose(fin);
     if (unlink(fitfile) < 0) {
         fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Can't delete tmp status file.\"]\n");
         fflush(stdout);
     }
   }
   fclose(fout);
   }  // end if (MPI_rank == 0)

   MPI_err = MPI_Finalize();

#endif

   exit(0);
}
