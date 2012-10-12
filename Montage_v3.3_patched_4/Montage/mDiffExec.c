/* Module: mDiffExec.c

Version  Developer        Date     Change
-------  ---------------  -------  -----------------------
1.5      Daniel S. Katz   04Aug04  Added optional parallel roundrobin
                                   computation
1.4      John Good        16May04  Added "noAreas" option
1.3      John Good        25Nov03  Added extern optarg references
1.2      John Good        25Aug03  Added status file processing
1.1      John Good        14Mar03  Added filePath() processing,
				   -p argument, and getopt()
				   argument processing.  Return error
				   if mDiff not in path.  Check for 
				   missing/invalid diffs table or diffs
                                   directory.  
1.0      John Good        29Jan03  Baseline code

*/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <mtbl.h>

#ifdef MPI
#include <mpi.h>
#endif

#include "montage.h"

#define MAXSTR 4096

char *svc_value();
char *svc_run  (char *cmd);

char *filePath (char *path, char *fname);
int   checkFile(char *filename);
int   checkHdr (char *infile, int hdrflag, int hdu);

extern char *optarg;
extern int optind, opterr;

extern int getopt(int argc, char *const *argv, const char *options);

int debug;


/*******************************************************************/
/*                                                                 */
/*  mDiffExec                                                      */
/*                                                                 */
/*  Read the table of overlaps found by mOverlap and rune mDiff to */
/*  generate the difference files.                                 */
/*                                                                 */
/*******************************************************************/

int main(int argc, char **argv)
{
   int    c, istat, ncols, count, failed, noAreas;

   int    icntr1;
   int    icntr2;
   int    ifname1;
   int    ifname2;
   int    idiffname;

   int    cntr1;
   int    cntr2;

   char   path    [MAXSTR];
   char   fname1  [MAXSTR];
   char   fname2  [MAXSTR];
   char   diffname[MAXSTR];

   char   tblfile [MAXSTR];
   char   diffdir [MAXSTR];
   char   template[MAXSTR];

   char   cmd     [MAXSTR];
   char   msg     [MAXSTR];
   char   status  [32];

   struct stat type;

#ifdef MPI
   int    row_number = 0;
   int    MPI_size, MPI_rank, MPI_err;
   int    exit_flag = 0;
   int    sum_tmp;
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

   debug   = 0;
   noAreas = 0;

   strcpy(path, "");

   opterr = 0;

   fstatus = stdout;

   while ((c = getopt(argc, argv, "np:ds:")) != EOF) 
   {
      switch (c) 
      {
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
#ifdef MPI
               exit_flag = 1;
#else
               exit(1);
#endif
            }
            break;

         default:
	    printf("[struct stat=\"ERROR\", msg=\"Usage: %s [-p projdir] [-d] [-n(o-areas)] [-s statusfile] diffs.tbl template.hdr diffdir\"]\n", argv[0]);
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
      printf("[struct stat=\"ERROR\", msg=\"Usage: %s [-p projdir] [-d] [-n(o-areas)] [-s statusfile] diffs.tbl template.hdr diffdir\"]\n", argv[0]);
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

   strcpy(tblfile,  argv[optind]);
   strcpy(template, argv[optind + 1]);
   strcpy(diffdir,  argv[optind + 2]);

   checkHdr(template, 1, 0);


   /**********************************/
   /* Check to see if diffdir exists */
   /**********************************/

   istat = stat(diffdir, &type);

   if(istat < 0)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Cannot access %s\"]\n", diffdir);
#ifdef MPI
      exit_flag = 1;
#else
      exit(1);
#endif
   }

   else if (S_ISDIR(type.st_mode) != 1)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"%s is not a directory\"]\n", diffdir);
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
   /* Open the difference list table file */
   /***************************************/ 

   ncols = topen(tblfile);

   if(ncols <= 0)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Invalid image metadata file: %s\"]\n",
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


   /***********************************/ 
   /* Read the records and call mDiff */
   /***********************************/ 

   count  = 0;
   failed = 0;

   while(1)
   {
      istat = tread();

      if(istat < 0)
	 break;

#ifdef MPI
    if (row_number % MPI_size == MPI_rank) {
#endif

      cntr1 = atoi(tval(icntr1));
      cntr2 = atoi(tval(icntr2));

      strcpy(fname1,   filePath(path, tval(ifname1)));
      strcpy(fname2,   filePath(path, tval(ifname2)));
      strcpy(diffname, tval(idiffname));

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
	 ++failed;
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

   if (MPI_rank == 0) {
#endif   
   fprintf(fstatus, "[struct stat=\"OK\", count=%d, failed=%d]\n", count, failed);
   fflush(stdout);

#ifdef MPI
   }  // end if (MPI_rank == 0)
   MPI_err = MPI_Finalize();
#endif

   exit(0);
}
