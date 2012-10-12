/*
 Module: mBgExec.c

Version         Developer          Date           Change
-------         ---------------  -------  -----------------------
2.2             John Good        15May08  Bug:  When mBgExec encountered a missing
					  image, the correction parameters for the
					  immediately subsequent image were applied
					  incorrectly.
2.1             Daniel S. Katz   27Jan07  Restored MPI functionality
2.0             John Good        06Sep06  Very large datasets caused memory
                                          problems.  Reworked logic to use
                                          sorted image/correction lists.  
                                          Removed MPI stuff for now.
1.7             John Good        18Sep04  Added code to copy images for which
                                          there was no correction information
1.6             Daniel S. Katz   06Aug04  Added optional parallel roundrobin
                                          computation
1.5             Daniel S. Katz   04Aug04  Added check for too many images in table
1.4             John Good        17May04  Added "no areas" option
1.3             John Good        25Nov03  Added extern optarg references
1.2             John Good        25Aug03  Added status file output.
1.1             John Good        14Mar03  Added filePath() processing,
                                          -p argument, and getopt()
                                          argument processing.         Also, check
                                          for missing/invalid images table.
                                          Return error if mBackground not in
                                          path.
1.0           John Good          29Jan03  Baseline code

*/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <math.h>
#include <mtbl.h>

#ifdef MPI
#include <mpi.h>
#endif

#include "montage.h"

#define MAXSTR  4096

char *svc_value();
int   svc_run  (char *cmd);
char *filePath (char *path, char *fname);
char *fileName (char *filename);

static struct TBL_INFO *imgs;
static struct TBL_INFO *corrs;

static int  iid;
static int  ifname;

static int  id;
static char file[MAXSTR];

static int  icntr;
static int  ia;
static int  ib;
static int  ic;

static int  cntr;
static char a[MAXSTR];
static char b[MAXSTR];
static char c[MAXSTR];

extern char *optarg;
extern int optind, opterr;

extern int getopt(int argc, char *const *argv, const char *options);

char *mktemp(char *template);

int nextImg();
int nextCorr();

int debug;


/*******************************************************************/
/*                                                                 */
/*  mBgExec                                                        */
/*                                                                 */
/*  Take the background correction determined for each image and   */
/*  subtract it using mBackground.                                 */
/*                                                                 */
/*******************************************************************/

int main(int argc, char **argv)
{
   int  ch, istat, ncols;
   int  count, nocorrection, failed, noAreas;

   char path      [MAXSTR];
   char tblfile   [MAXSTR];
   char fitfile   [MAXSTR];
   char corrdir   [MAXSTR];
   char imgsort   [MAXSTR];
   char corrsort  [MAXSTR];
   char template  [MAXSTR];

   char cmd       [MAXSTR];
   char msg       [MAXSTR];
   char status    [32];

   struct stat type;

#ifdef MPI
   int    counter = -1;
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
   fstatus = stdout;
   noAreas = 0;

   strcpy(path, "");

   opterr = 0;

   while ((ch = getopt(argc, argv, "np:s:d")) != EOF) 
   {
        switch (ch) 
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
                   printf ("[struct stat=\"ERROR\", msg=\"Cannot open status file: %s\"]\n",
                        optarg);
#ifdef MPI
                   exit_flag = 1;
#else
                   exit(1);
#endif
                }
                break;

           default:
            printf ("[struct stat=\"ERROR\", msg=\"Usage: %s [-p projdir] [-s statusfile] [-d] [-n(o-areas)] images.tbl corrections.tbl corrdir\"]\n", argv[0]);
                exit(1);
                break;
        }
   }

#ifdef MPI
   MPI_err = MPI_Allreduce(&exit_flag, &sum_tmp, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
   if (sum_tmp != 0) exit(1);
#endif

   if (argc - optind < 3) 
   {
        fprintf (fstatus, "[struct stat=\"ERROR\", msg=\"Usage: %s [-p projdir] [-s statusfile] [-d] [-n(o-areas)] images.tbl corrections.tbl corrdir\"]\n", argv[0]);
#ifdef MPI
        exit_flag = 1;
#else
        exit(1);
#endif
   }

#ifdef MPI
   MPI_err = MPI_Allreduce(&exit_flag, &sum_tmp, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
   if (sum_tmp != 0) exit(1);
#endif

   strcpy(tblfile,  argv[optind]);
   strcpy(fitfile,  argv[optind + 1]);
   strcpy(corrdir,  argv[optind + 2]);



   /**********************************/ 
   /* Check to see if corrdir exists */
   /**********************************/ 

   istat = stat(corrdir, &type);

   if(istat < 0)
   {
        fprintf (fstatus, "[struct stat=\"ERROR\", msg=\"Cannot access %s\"]\n", corrdir);
#ifdef MPI
        exit_flag = 1;
#else
        exit(1);
#endif
   }

#ifdef MPI
   MPI_err = MPI_Allreduce(&exit_flag, &sum_tmp, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
   if (sum_tmp != 0) exit(1);
#endif

   if (S_ISDIR(type.st_mode) != 1)
   {
        fprintf (fstatus, "[struct stat=\"ERROR\", msg=\"%s is not a directory\"]\n", corrdir);
#ifdef MPI
        exit_flag = 1;
#else
        exit(1);
#endif
   }

#ifdef MPI
   MPI_err = MPI_Allreduce(&exit_flag, &sum_tmp, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
   if (sum_tmp != 0) exit(1);
#endif

   /**************************************************************/
   /* Make sorted copies of the image list and corrections table */
   /**************************************************************/

#ifdef MPI
   if (MPI_rank == 0) {
#endif

   sprintf(template, "%s/IMGTBLXXXXXX", corrdir);
   strcpy(imgsort, (char *)mktemp(template));

   sprintf(cmd, "mTblSort %s cntr %s", tblfile, imgsort);

   if(debug)
   {
      printf("[%s]\n", cmd);
      fflush(stdout);
   }
 
   svc_run(cmd);

   strcpy(status, svc_value("stat"));
 
   if(strcmp( status, "ABORT") == 0
   || strcmp( status, "ERROR") == 0)
   {
      strcpy( msg, svc_value( "msg" ));
   
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"%s\"]\n", msg);
      fflush(fstatus);
   
#ifdef MPI
      exit_flag = 1;
#else
      exit(1);
#endif
   }

#ifdef MPI
   } //end of rank 0 work
   MPI_err = MPI_Allreduce(&exit_flag, &sum_tmp, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
   if (sum_tmp != 0) exit(1);
   MPI_err = MPI_Bcast(imgsort,MAXSTR,MPI_CHAR,0,MPI_COMM_WORLD);

   if (MPI_rank == 0) {
#endif

   sprintf(template, "%s/CORTBLXXXXXX", corrdir);
   strcpy(corrsort, (char *)mktemp(template));

   sprintf(cmd, "mTblSort %s id %s", fitfile, corrsort);

   if(debug)
   {
      printf("[%s]\n", cmd);
      fflush(stdout);
   }
 
   svc_run(cmd);
 
   strcpy(status, svc_value("stat"));
 
   if(strcmp( status, "ABORT") == 0
   || strcmp( status, "ERROR") == 0)
   {
      strcpy( msg, svc_value( "msg" ));
   
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"%s\"]\n", msg);
      fflush(fstatus);
   
#ifdef MPI
      exit_flag = 1;
#else
      exit(1);
#endif
   }

#ifdef MPI
   } //end of rank 0 work
   MPI_err = MPI_Allreduce(&exit_flag, &sum_tmp, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
   if (sum_tmp != 0) exit(1);
   MPI_err = MPI_Bcast(corrsort,MAXSTR,MPI_CHAR,0,MPI_COMM_WORLD);
#endif


   /********************************/ 
   /* Open the image metadata file */
   /********************************/ 

   ncols = topen(imgsort);

   if(ncols <= 0)
   {
        fprintf (fstatus, "[struct stat=\"ERROR\", msg=\"Invalid image metadata file: %s\"]\n",
         imgsort);
#ifdef MPI
        exit_flag = 1;
#else
        exit(1);
#endif
   }

#ifdef MPI
   MPI_err = MPI_Allreduce(&exit_flag, &sum_tmp, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
   if (sum_tmp != 0) exit(1);
#endif


   imgs = tsave();

   icntr  = tcol( "cntr");
   ifname = tcol( "fname");

   if(debug)
   {
      printf("\nImage metdata table\n");
      printf("icntr  = %d\n", icntr);
      printf("ifname = %d\n", ifname);
      fflush(stdout);
   }

   if(icntr  < 0
   || ifname < 0)
   {
      fprintf (fstatus, "[struct stat=\"ERROR\", msg=\"Need columns: cntr and fname in image list\"]\n");
#ifdef MPI
      exit_flag = 1;
#else
      exit(1);
#endif
   }

#ifdef MPI
   MPI_err = MPI_Allreduce(&exit_flag, &sum_tmp, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
   if (sum_tmp != 0) exit(1);
#endif

   /***********************************/ 
   /* Open the corrections table file */
   /***********************************/ 

   ncols = topen(corrsort);

   if(ncols <= 0)
   {
        fprintf (fstatus, "[struct stat=\"ERROR\", msg=\"Invalid corrections  file: %s\"]\n",
         corrsort);
#ifdef MPI
        exit_flag = 1;
#else
        exit(1);
#endif
   }

#ifdef MPI
   MPI_err = MPI_Allreduce(&exit_flag, &sum_tmp, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
   if (sum_tmp != 0) exit(1);
#endif

   corrs = tsave();

   iid = tcol( "id");
   ia  = tcol( "a");
   ib  = tcol( "b");
   ic  = tcol( "c");

   if(debug)
   {
      printf("\nCorrections table\n");
      printf("iid = %d\n", iid);
      printf("ia  = %d\n", ia);
      printf("ib  = %d\n", ib);
      printf("ic  = %d\n", ic);
      printf("\n");
      fflush(stdout);
   }

   if(iid < 0
   || ia  < 0
   || ib  < 0
   || ic  < 0)
   {
      fprintf (fstatus, "[struct stat=\"ERROR\", msg=\"Need columns: id,a,b,c in corrections file\"]\n");
#ifdef MPI
      exit_flag = 1;
#else
      exit(1);
#endif
   }

#ifdef MPI
   MPI_err = MPI_Allreduce(&exit_flag, &sum_tmp, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
   if (sum_tmp != 0) exit(1);
#endif

   /***************************************************/ 
   /* Read through the two sorted tables.             */
   /* If there is no correction for an image file,    */
   /* increment 'nocorrection' and copy it unchanged. */
   /* Then run mBackground to create the corrected    */
   /* image.  If there is an image in the list for    */
   /* which we don't actually have a projected file   */
   /* (can happen if the list was created from the    */
   /* 'raw' set), increment the 'failed' count.       */
   /***************************************************/ 

   count        = 0;
   nocorrection = 0;
   failed       = 0;

   if(nextImg())
   {
      fprintf (fstatus, "[struct stat=\"ERROR\", msg=\"No images in list\"]\n");
      fflush(fstatus);
#ifdef MPI
      exit_flag = 1;
#else
      exit(1);
#endif
   }

#ifdef MPI
   MPI_err = MPI_Allreduce(&exit_flag, &sum_tmp, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
   if (sum_tmp != 0) exit(1);
#endif

   if(nextCorr())
   {
      fprintf (fstatus, "[struct stat=\"ERROR\", msg=\"No corrections in list\"]\n");
      fflush(fstatus);
#ifdef MPI
      exit_flag = 1;
#else
      exit(1);
#endif
   }

#ifdef MPI
   MPI_err = MPI_Allreduce(&exit_flag, &sum_tmp, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
   if (sum_tmp != 0) exit(1);
#endif

   while(1)
   {
      if(debug)
      {
         printf("cntr = %d,  id = %d\n", cntr, id);
         fflush(stdout);
      }

      if(cntr == id)
      {
#ifdef MPI
         counter++;
         if (counter % MPI_size == MPI_rank) {
#endif
         if (noAreas)
            sprintf(cmd, "mBackground -n %s %s/%s %s %s %s", 
             filePath(path, file), corrdir, fileName(file), a, b, c);
         else
            sprintf(cmd, "mBackground %s %s/%s %s %s %s", 
             filePath(path, file), corrdir, fileName(file), a, b, c);

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
            fflush(fstatus);
         
#ifdef MPI
            exit_flag = 1;
#else
            exit(1);
#endif
         }

         ++count;
         if(strcmp( status, "ERROR") == 0)
            ++failed;

#ifdef MPI
         }
#endif

         if(nextImg())
            break;

         if(nextCorr())
            break;
      }

      else if(cntr < id)
      {
         if (noAreas)
            sprintf(cmd, "mBackground -n %s %s/%s 0. 0. 0.", 
             file, corrdir, fileName(file));
         else
            sprintf(cmd, "mBackground %s %s/%s 0. 0. 0.", 
             file, corrdir, fileName(file));

         if(debug)
         {
          printf("[%s] MISSING> No correction found\n", cmd);
          fflush(stdout);
         }

         ++count;
         ++nocorrection;

         if(nextImg())
            break;
      }

      else if(cntr > id)
      {
         if(debug)
         {
          printf("MISSING> No image found\n");
          fflush(stdout);
         }

         if(nextCorr())
            break;
      }
   }

   unlink(imgsort);
   unlink(corrsort);

#ifdef MPI
   MPI_err = MPI_Allreduce(&count, &sum_tmp, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
   count = sum_tmp;
   MPI_err = MPI_Allreduce(&failed, &sum_tmp, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
   failed = sum_tmp;
   MPI_err = MPI_Allreduce(&nocorrection, &sum_tmp, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
   nocorrection = sum_tmp;
   if (MPI_rank == 0) {
#endif

   fprintf(fstatus, "[struct stat=\"OK\", count=%d, nocorrection=%d, failed=%d]\n", count, nocorrection, failed);
   fflush(fstatus);

#ifdef MPI
   }
#endif

   exit(0);
}


int nextImg()
{
   int istat;

   trestore(imgs);

   istat = tread();

   if(istat < 0)
      return 1;

   cntr = atoi(tval(icntr));

   strcpy(file, tval(ifname));

   if(debug)
   {
      printf("nextImg(): %d: %s\n", cntr, file);
      fflush(stdout);
   }

   return 0;
}


int nextCorr()
{
   int istat;

   trestore(corrs);

   istat = tread();

   if(istat < 0)
   return 1;

   id = atoi(tval(iid));

   strcpy(a, tval(ia));
   strcpy(b, tval(ib));
   strcpy(c, tval(ic));

   if(debug)
   {
      printf("nextCorr(): %d: %s %s %s\n", id, a, b, c);
      fflush(stdout);
   }

   return 0;
}
