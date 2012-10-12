/* Module: mAddExec.c

Version  Developer        Date     Change
-------  ---------------  -------  -----------------------
1.3      Daniel S. Katz   25Jan07  Fixing some small bugs in the MPI version
                                   that were inadvertently introduced in 1.2
1.2      John Good        11Sep06  Require "tile" subdirectory as argument
1.1      Daniel S. Katz   09Dec04  Changed computation of number of
                                   tiles to match that done in mDAG.
                                   Added optional parallel roundrobin
                                   computation.
                                   Write output status of subprograms.
1.0      Daniel S. Katz   11Nov04  Baseline code.

*/


/*************************************************************************/
/*                                                                       */
/*  mAddExec                                                             */
/*                                                                       */
/*  Montage is a set of general reprojection / coordinate-transform /    */
/*  mosaicking programs.  Any number of input images can be merged into  */
/*  an output FITS file.  The attributes of the input are read from the  */
/*  input files; the attributes of the output are read a combination of  */
/*  the command line and a FITS header template file.                    */
/*                                                                       */
/*  This module, mAddExec, builds a series of outputs (which together    */
/*  make up a tiled output) through multiple executions of the           */
/*  mAdd module.                                                         */
/*                                                                       */
/*************************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <time.h>
#include <math.h>

#include <fitsio.h>
#include <wcs.h>
#include <svc.h>
#include <mtbl.h>

#ifdef MPI
#include <mpi.h>
#endif

#include "montage.h"

#define MAXSTR     256
#define HDRLEN   80000

#define MEAN   1
#define MEDIAN 2

extern char *optarg;
extern int optind, opterr;

extern int getopt(int argc, char *const *argv, const char *options);

char *svc_value();
int   svc_run(char *cmd);

/*********************/
/* Define prototypes */
/*********************/

void parseLine    (char *line);
void readTemplate (char *filename);
void printError   (char *msg);
int  intCheck     (char *Str);
int  debugCheck   (char *debugStr);
int  checkHdr     (char *infile, int hdrflag, int hdu);
int  stradd       (char *header, char *card);

int CALLmAdd (char *cmd, char *path, int haveAreas, int coadd, int shrink,
              int debug, char *status_file, char *tblfile,
              char *template_body, char *template_ext, char *output_dir,
	      char *output_file, int i, int j, int *count, 
	      int *failed, int *warning);


/***************************/
/* Define global variables */
/***************************/

char output_file[MAXSTR];
char output_dir [MAXSTR];

struct WorldCoor *imgWCS;
struct WorldCoor *hdrWCS;

int  debug;
int  haveAreas = 0;

static time_t currtime, start;


/***************************************************/
/* structure to hold file information and pointers */
/***************************************************/

struct outfile
{
  long      naxes[2];
}
output;


/***************************/
/*  mAddExec main routine  */
/***************************/

int main(int argc, char **argv)
{
   int       i, j, c;

   int       shrink = 1;  
   int       coadd = MEAN;

   int       numTile, numTileX, numTileY, tileX, tileY, overlapX, overlapY;

   int       countTH = 0;
   int       warningTH = 0;
   int       failedTH = 0;
   int       countSS = 0;
   int       warningSS = 0;
   int       failedSS = 0;
   int       countA = 0;
   int       warningA = 0;
   int       failedA = 0;

   char      argument     [MAXSTR];
   char      template_file[MAXSTR];
   char      template_body[MAXSTR];
   char      template_ext [MAXSTR];
   char      path         [MAXSTR];

   char      tblfile      [MAXSTR];

   char      cmd          [MAXSTR];
   char      msg          [MAXSTR];
   char      status_file  [MAXSTR];
   char      status       [32];

#ifdef MPI
   int      sum_tmp;
   int      exit_flag = 0;
   int      MPI_size, MPI_rank, MPI_err;
   int      cnt = 0;
#endif

/*
   printf("starting\n");
   fflush(stdout);
*/

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

   /****************/
   /* Start timing */
   /****************/

   time(&currtime);
   start = currtime;


   /***************************************/
   /* Process the command-line parameters */
   /***************************************/

   strcpy(path, "");
   strcpy(status_file, "");
   debug     = 0;
   opterr    = 0;
   haveAreas = 1;
   tileX     = 2000;
   tileY     = 2000;
   overlapX  = 0;
   overlapY  = 0;

   fstatus = stdout;

   while ((c = getopt(argc, argv, "enp:s:d:a:x:y:o:q:")) != EOF)
   {
      switch (c)
      {
         case 'x':

           /******************************/
           /* Find suggested tile x size */
           /******************************/

           tileX = intCheck(optarg);
           if (tileX <= 0) {
             printf("[struct stat=\"ERROR\", msg=\"Invalid argument for -x flag\"]\n");
             fflush(stdout);
#ifdef MPI
             exit_flag = 1;
#else
             exit(1);
#endif
           }
           tileY = tileX;
           break;

         case 'y':

           /******************************/
           /* Find suggested tile y size */
           /******************************/

           tileY = intCheck(optarg);
           if (tileY <= 0) {
             printf("[struct stat=\"ERROR\", msg=\"Invalid argument for -y flag\"]\n");
             fflush(stdout);
#ifdef MPI
             exit_flag = 1;
#else
             exit(1);
#endif
           }
           break;

         case 'o':

           /*********************************/
           /* Find suggested overlap x size */
           /*********************************/

           overlapX = intCheck(optarg);
           if (overlapX <= 0) {
             printf("[struct stat=\"ERROR\", msg=\"Invalid argument for -o flag\"]\n");
             fflush(stdout);
#ifdef MPI
             exit_flag = 1;
#else
             exit(1);
#endif
           }
           overlapY = overlapX;
           break;

         case 'q':

           /*********************************/
           /* Find suggested overlap y size */
           /*********************************/

           overlapY = intCheck(optarg);
           if (overlapY <= 0) {
             printf("[struct stat=\"ERROR\", msg=\"Invalid argument for -q flag\"]\n");
             fflush(stdout);
#ifdef MPI
             exit_flag = 1;
#else
             exit(1);
#endif
           }
           break;

         case 'a':

           /***********************/
           /* Find averaging type */
           /***********************/

           strcpy(argument, optarg);
           if (strcmp(argument, "mean") == 0)
             coadd = MEAN;
           else if (strcmp(argument, "median") == 0)
             coadd = MEDIAN;
           else
           {
             printf("[struct stat=\"ERROR\", msg=\"Invalid argument for -a flag\"]\n");
             fflush(stdout);
#ifdef MPI
             exit_flag = 1;
#else
             exit(1);
#endif
           }
           break;

         case 'e':

           /*****************************/
           /* Is 'exact-size" flag set? */
           /*****************************/

           shrink = 0;
           break;

         case 'p':

           /*****************************/
           /* Get path to image dir     */
           /*****************************/

            strcpy(path, optarg);
            break;

         case 'd':

           /************************/
           /* Look for debug level */
           /************************/

            debug = debugCheck(optarg);
            break;

         case 'n':

           /****************************/
           /* We don't have area files */
           /****************************/

            haveAreas = 0;
            break;

         case 's':

           /************************/
           /* Look for status file */
           /************************/

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
            strcpy(status_file, optarg);
            break;

         default:

           /************************/
           /* Print usage message  */
           /************************/

            printf("[struct stat=\"ERROR\", msg=\"Usage: %s [-p imgdir] [-n(o-areas)] [-a mean|median] [-e(xact-size)] [-d level] [-s statusfile] [-x TileSizeAxis0] [-y TileSizeAxis1] [-o OverlapAxis0] [-q OverlapAxis1] images.tbl template.hdr tiledir out.fits\"]\n", argv[0]);
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

   /*****************************/
   /* Get required arguments    */
   /*****************************/

   if (argc - optind < 4)
   {
      printf("[struct stat=\"ERROR\", msg=\"Usage: %s [-p imgdir] [-n(o-areas)] [-a mean|median] [-e(xact-size)] [-d level] [-s statusfile] [-x TileSizeAxis0] [-y TileSizeAxis1] [-o OverlapAxis0] [-q OverlapAxis1] images.tbl template.hdr tiledir out.fits\"]\n", argv[0]);
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

   strcpy(tblfile,       argv[optind]);
   strcpy(template_file, argv[optind + 1]);
   strcpy(output_dir,    argv[optind + 2]);
   strcpy(output_file,   argv[optind + 3]);
     if(debug >= 1)
   {
      time(&currtime);
      printf("Command line arguments processed [time: %.0f]\n",
         (double)(currtime - start));
      fflush(stdout);
   }


   /***********************************************/
   /* Check header and set up name of output file */
   /***********************************************/

   checkHdr(template_file, 1, 0);

   if(strlen(output_file) > 5 &&
      strncmp(output_file+strlen(output_file)-5, ".fits", 5) == 0)
         output_file[strlen(output_file)-5] = '\0';

   if(strlen(output_file) > 5 &&
      strncmp(output_file+strlen(output_file)-5, ".FITS", 5) == 0)
         output_file[strlen(output_file)-5] = '\0';

   if(strlen(output_file) > 4 &&
      strncmp(output_file+strlen(output_file)-4, ".fit", 4) == 0)
         output_file[strlen(output_file)-4] = '\0';

   if(strlen(output_file) > 4 &&
      strncmp(output_file+strlen(output_file)-4, ".FIT", 4) == 0)
         output_file[strlen(output_file)-4] = '\0';

   strcat(output_file,  ".fits");

   for (i=strlen(template_file)-1;i>=0;i--)
   {
      if (template_file[i] == '.') break;
   }
   if (i!=0)
   {
      strcpy(template_ext,(char *) &template_file[i]);
      strcpy(template_body,template_file);
      template_body[i] = '\0';
   }

   if(debug >= 1)
   {
      printf("image list       = [%s]\n", tblfile);
      printf("output_dir       = [%s]\n", output_dir);
      printf("output_file      = [%s]\n", output_file);
      printf("template_file    = [%s]\n", template_file);
      printf("template_body    = [%s]\n", template_body);
      printf("template_ext     = [%s]\n", template_ext);
      fflush(stdout);
   }


   /*************************************************/
   /* Process the output header template to get the */
   /* image size, coordinate system and projection  */
   /*************************************************/

   readTemplate(template_file);

   numTileX = (int) ((float) output.naxes[0]/(float) tileX + 0.5);
   numTileY = (int) ((float) output.naxes[1]/(float) tileY + 0.5);
   numTile = numTileX * numTileY;

   if(debug >= 1)
   {
      printf("#tiles (axis 0)  = [%d]\n", numTileX);
      printf("#tiles (axis 1)  = [%d]\n", numTileY);
      printf("#tiles (total)   = [%d]\n", numTileY);
      fflush(stdout);
   }

   if (numTile > 1)
   {
      for (i=0 ; i<numTileX ; i++)
      {
         for (j=0 ; j<numTileY ; j++)
         {
            /*****************/
            /* call mTileHdr */
            /*****************/
#ifdef MPI
            if (cnt % MPI_size == MPI_rank) {
#endif

            sprintf(cmd, "mTileHdr %s %s/%s_%d_%d%s %d %d %d %d %d %d",
                    template_file, output_dir,
                    template_body, i, j, template_ext, numTileX, numTileY, i, j,
                    overlapX, overlapY);

            if(debug)
            {
               printf("[%s]\n", cmd);
               fflush(stdout);
            }

            svc_run(cmd);

            if(strcmp( status, "ABORT") == 0)
            {
               strcpy( msg, svc_value( "msg" ));

               fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"%s\"]\n", msg);
               fflush(stdout);

               exit(1);
            }

            strcpy( status, svc_value( "stat" ));

            ++countTH;
            if(strcmp( status, "ERROR") == 0)
            {
               ++failedTH;

               if(debug)
               {
                  printf("ERROR: %s\n", svc_value( "msg" ));
                  fflush(stdout);
               }
            }
            else if(strcmp( status, "WARNING") == 0)
            {
               ++warningTH;

               if(debug)
               {
                  printf("WARNING: %s\n", svc_value( "msg" ));
                  fflush(stdout);
               }
            }

            /****************/
            /* call mSubset */
            /****************/

            sprintf(cmd, "mSubset -f %s %s/%s_%d_%d%s %s/%s_%d_%d.tbl",
                    tblfile, output_dir, template_body, i, j, template_ext,
		    output_dir, template_body, i, j);

            if(debug)
            {
               printf("[%s]\n", cmd);
               fflush(stdout);
            }

            svc_run(cmd);

            if(strcmp( status, "ABORT") == 0)
            {
               strcpy( msg, svc_value( "msg" ));

               fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"%s\"]\n", msg);
               fflush(stdout);

               exit(1);
            }

            strcpy( status, svc_value( "stat" ));

            ++countSS;
            if(strcmp( status, "ERROR") == 0)
            {
               ++failedSS;

               if(debug)
               {
                  printf("ERROR: %s\n", svc_value( "msg" ));
                  fflush(stdout);
               }
            }
            else if(strcmp( status, "WARNING") == 0)
            {
               ++warningSS;

               if(debug)
               {
                  printf("WARNING: %s\n", svc_value( "msg" ));
                  fflush(stdout);
               }
            }

            /*************/
            /* call mAdd */
            /*************/

            (void) CALLmAdd (cmd, path, haveAreas, coadd, shrink,
                             debug, status_file, tblfile,
                             template_body, template_ext, output_dir,
			     output_file, i, j,
                             &countA, &failedA, &warningA);

#ifdef MPI
            } // end if (cnt % MPI_size == MPI_rank)
            cnt++;
#endif
         }
      }
   }
   else
   {
      /****************************/
      /* just one tile, call mAdd */
      /****************************/
      (void) CALLmAdd (cmd, path, haveAreas, coadd, shrink,
                       debug, status_file, tblfile,
                       template_body, template_ext, output_dir,
		       output_file, -1, -1,
                       &countA, &failedA, &warningA);
   }

#ifdef MPI
   MPI_err = MPI_Allreduce(&countTH, &sum_tmp, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
   countTH = sum_tmp;
   MPI_err = MPI_Allreduce(&failedTH, &sum_tmp, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
   failedTH = sum_tmp;
   MPI_err = MPI_Allreduce(&warningTH, &sum_tmp, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
   warningTH = sum_tmp;
   MPI_err = MPI_Allreduce(&countTH, &sum_tmp, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
   countSS = sum_tmp;
   MPI_err = MPI_Allreduce(&failedSS, &sum_tmp, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
   failedSS = sum_tmp;
   MPI_err = MPI_Allreduce(&warningSS, &sum_tmp, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
   warningSS = sum_tmp;
   MPI_err = MPI_Allreduce(&countA, &sum_tmp, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
   countA = sum_tmp;
   MPI_err = MPI_Allreduce(&failedA, &sum_tmp, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
   failedA = sum_tmp;
   MPI_err = MPI_Allreduce(&warningA, &sum_tmp, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
   warningA = sum_tmp;

   if (MPI_rank == 0) {
#endif

   if(debug >= 1)
   {
      printf("mAddExec complete\n");
      fflush(stdout);
   }

   time(&currtime);
   fprintf(fstatus, "[struct stat=\"OK\", mTileHdr_count=%d, mTileHdr_failed=%d, mTileHdr_warning=%d, mSubset_count=%d, mSubset_failed=%d, mSubset_warning=%d, mAdd_count=%d, mAdd_failed=%d, mAdd_warning=%d, time=%.0f]\n",
      countTH, failedTH, warningTH, countSS, failedSS, warningSS, countA, failedA, warningA,
      (double)(currtime - start));
   fflush(stdout);

#ifdef MPI
   }  // end if (MPI_rank == 0)

   MPI_err = MPI_Finalize();

#endif
   exit(0);
}


/**************************************************/
/*                                                */
/*  Read the output header template file.         */
/*  Specifically extract the image size info.     */
/*                                                */
/**************************************************/

void readTemplate(char *filename)
{
   int       i, j;
   FILE     *fp;
   char      line     [MAXSTR];
   char      headerStr[HDRLEN];


   /********************************************************/
   /* Open the template file, read and parse all the lines */
   /********************************************************/

   fp = fopen(filename, "r");

   if(fp == (FILE *)NULL)
      printError("Template file not found.");

   strcpy(headerStr, "");

   for(j=0; j<1000; ++j)
   {
      if(fgets(line, MAXSTR, fp) == (char *)NULL)
         break;

      if(line[strlen(line)-1] == '\n')
         line[strlen(line)-1]  = '\0';
           if(line[strlen(line)-1] == '\r')
         line[strlen(line)-1]  = '\0';

      if(debug >= 3)
      {
         printf("Template line: [%s]\n", line);
         fflush(stdout);
      }

      for(i=strlen(line); i<80; ++i)
         line[i] = ' ';
           line[80] = '\0';

      stradd(headerStr, line);

      parseLine(line);
   }

   fclose(fp);

   hdrWCS = wcsinit(headerStr);

   if(hdrWCS == (struct WorldCoor *)NULL)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Bad WCS in header template.\"]\n");
      exit(1);
   }

   return;
}



/**************************************************/
/*                                                */
/*  Parse header lines from the template,         */
/*  looking for NAXIS1, NAXIS2, CRPIX1 and CRPIX2 */
/*                                                */
/**************************************************/

void parseLine(char *line)
{
   char *keyword;
   char *value;
   char *end;

   int   len;

   len = strlen(line);

   keyword = line;

   while(*keyword == ' ' && keyword < line+len)
      ++keyword;
     end = keyword;

   while(*end != ' ' && *end != '=' && end < line+len)
      ++end;

   value = end;

   while((*value == '=' || *value == ' ' || *value == '\'')
         && value < line+len)
      ++value;
     *end = '\0';
   end = value;

   if(*end == '\'')
      ++end;

   while(*end != ' ' && *end != '\'' && end < line+len)
      ++end;
     *end = '\0';

   if(debug >= 2)
   {
      printf("keyword [%s] = value [%s]\n", keyword, value);
      fflush(stdout);
   }

   if(strcmp(keyword, "NAXIS1") == 0) output.naxes[0] = atoi(value);

   if(strcmp(keyword, "NAXIS2") == 0) output.naxes[1] = atoi(value);

}


/******************************/
/*                            */
/*  Print out general errors  */
/*                            */
/******************************/

void printError(char *msg)
{
   fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"%s\"]\n", msg);

   remove(output_file);             
   exit(1);
}


/*******************************************************/
/* STRADD adds the string "card" to a header line, and */
/* pads the header out to 80 characters.               */
/*******************************************************/

int stradd(char *header, char *card)
{
   int i;

   int hlen = strlen(header);
   int clen = strlen(card);

   for(i=0; i<clen; ++i)
      header[hlen+i] = card[i];

   if(clen < 80)
      for(i=clen; i<80; ++i)
         header[hlen+i] = ' ';

   header[hlen+80] = '\0';

   return(strlen(header));
}

/********************************************************/
/* CALLmAdd build a command string for mAdd and calls is */
/********************************************************/

int CALLmAdd (char *cmd, char *path, int haveAreas, int coadd, int shrink,
              int debug, char *status_file, char *tblfile,
              char *template_body, char *template_ext, char *output_dir,
	      char *output_file, int i, int j, int *count, 
	      int *failed, int *warning)
{
   char msg[MAXSTR];
   char status[32];
   char fname[MAXSTR];

   sprintf(cmd, "mAdd");
   if (path[0] != NULL)
   {
      strcat(cmd," -p ");
      strcat(cmd,path);
   }
   if (!haveAreas) strcat(cmd," -n");
   strcat(cmd," -a ");
   switch(coadd)
   {
      case(MEAN):
         strcat(cmd,"mean");
         break;
      case(MEDIAN):
         strcat(cmd,"median");
         break;
   }
   if (!shrink) strcat(cmd," -e");
   /* do not pass -d N to mAdd, as it will make the svc library unhappy */
   if (status_file[0] != NULL)
   {
      strcat(cmd," -s ");
      strcat(cmd,status_file);
   }
   strcat(cmd," ");

   sprintf(fname, "%s/%s_%d_%d.tbl",
      output_dir, template_body, i, j);

   strcat(cmd, fname);

   strcat(cmd," ");

   sprintf(fname, "%s/%s_%d_%d%s",
      output_dir, template_body, i, j, template_ext);

   strcat(cmd, fname);

   strcat(cmd," ");

   strcat(cmd,output_dir);
   strcat(cmd,"/");
   strcat(cmd,output_file);

   if ((i != -1) && (j != -1))
   {
      /* add the tile info */

      cmd[strlen(cmd)-5] = '\0';
      sprintf(fname, "_%d_%d.fits", i, j);
      strcat(cmd,fname);
   }
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

   ++(*count);
   if(strcmp( status, "ERROR") == 0)
   {
      ++(*failed);

      if(debug)
      {
         printf("ERROR: %s\n", svc_value( "msg" ));
         fflush(stdout);
      }
   }
   else if(strcmp( status, "WARNING") == 0)
   {
      ++(*warning);

      if(debug)
      {
         printf("WARNING: %s\n", svc_value( "msg" ));
         fflush(stdout);
      }
   }
   
   return (1);
}

/*************************************************/
/*                                               */
/*  intCheck                                     */
/*                                               */
/*  This routine checks a string to see if it    */
/*  represents a valid positive integer.         */
/*                                               */
/*************************************************/

int intCheck(char *Str)
{
   int   intval;
   char *end;

   intval = strtol(Str, &end, 0);

   if(end - Str < strlen(Str))
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"command line option was not integer: '%s'\"]\n", Str);
      exit(1);
   }

   if(intval < 0)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"option cannot be negative\"]\n");
      exit(1);
   }

   return intval;
}

