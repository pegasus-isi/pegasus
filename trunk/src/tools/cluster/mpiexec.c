/*
 * This file or a portion of this file is licensed under the terms of
 * the Globus Toolkit Public License, found in file GTPL, or at
 * http://www.globus.org/toolkit/download/license.html. This notice must
 * appear in redistributions of this file, with or without modification.
 *
 * Redistributions of this Software, with or without modification, must
 * reproduce the GTPL in: (1) the Software, or (2) the Documentation or
 * some other similar material which is provided with the Software (if
 * any).
 *
 * Copyright 1999-2004 University of Chicago and The University of
 * Southern California. All rights reserved.
 *
 */

/*
 * Module: mpiexec.c
 * Author : Mei-hui Su
 * Revision : $REVISION$
 */





#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <math.h>
#include <mpi.h>
#include <time.h>

#include "mypopen.h"
#include "mympi.h"

#define MAXSTR 4096

#define CMD_TAG    1
#define RESULT_TAG 2
#define EXIT_TAG   3

void exit_panic(int);
FILE *parse_cmd_line(int,char**);
int run_it(char *, char*[], char *);

extern char *optarg;
extern int optind, opterr;


int debug;
FILE *fstatus;

/*******************************************************************/
/*                                                                 */
/*  mGenericExecMPI                                                */
/*                                                                 */
/*  Runs some commands from an input file/or stdin using MPI       */
/*                                                                 */
/*******************************************************************/

int main(int argc, char **argv, char *envp[])
{
   int    count, failed;

   char   cmd      [MAXSTR];

   FILE   *fin;

   MPI_Status mstatus;
   char       mpistatus[MAXSTR];
   int        myid;
   int        numprocs;

   MPI_Init(&argc, &argv);
   MPI_Comm_size(MPI_COMM_WORLD, &numprocs);
   MPI_Comm_rank(MPI_COMM_WORLD, &myid);

   debug  = 0;
   fstatus = stderr;

   /************************************************/ 
   /* Read the corresponding commands and run it   */
   /************************************************/ 

   count     = 0;
   failed    = 0;

   fin=parse_cmd_line(argc, argv);

   if (myid == 0) { /* I am the master */
      int  free_idx;
      int  pause_for_result=0;
      int  ret;

      int more_cmd=1;
      char *save_cmd=NULL;
      int pflag;

      time_t start_time;
      time_t end_time;
      double elapsed_time;

      time(&start_time);

      /* special case, I am the only one */
      if(numprocs==1) {
        while((save_cmd=fgets(cmd, MAXSTR, fin)) != (char *)NULL) {
          run_it(cmd, envp, mpistatus);
          count++;
          if(strstr( mpistatus, "[struct stat=\"ABORT\"") != NULL ||
                                 strstr( mpistatus, "[struct stat=\"ERROR\"") != NULL)
          {
            fprintf(fstatus, "%s\n", mpistatus);
            failed++;
          }
        }
      } else {

        init_my_list(numprocs-1); /* counting just the slaves */

        while(1) {
          while(more_cmd) {

            if((save_cmd!=NULL) || 
                     (save_cmd=fgets(cmd, MAXSTR, fin)) != (char *)NULL) {
               free_idx=next_idle_node();
               if(free_idx!= -1) { /* send it out */
  
  
                  MPI_Send(cmd,        /* buff  */
                    strlen(cmd)+1,       /* count */
                    MPI_CHAR,          /* type  */
                    free_idx,          /* dest  */
                    CMD_TAG,           /* tag   */
                    MPI_COMM_WORLD);   /* comm  */
                  save_cmd=NULL;
    
                  } else {
                    pause_for_result=1;
                    break;
                }
               } else { /* no more command */ 
                int i; 
                for(i=1; i< numprocs; i++) {
                  MPI_Send(NULL,       /* buff  */
                    0,                 /* count */
                    MPI_CHAR,          /* type  */
                    i,                 /* dest  */
                    EXIT_TAG,          /* tag   */
                    MPI_COMM_WORLD);   /* comm  */
                }
                more_cmd=0;
                break;
            }
          }

          if(more_cmd==0) {
            if(all_idling()) /* all is done */
              break;
              else /* just waiting for the result */
                pause_for_result=1;
          } 
  
          while(1) {
            pflag=0;
            if(pause_for_result) {
              ret=MPI_Probe( MPI_ANY_SOURCE, RESULT_TAG,
                             MPI_COMM_WORLD, &mstatus );
               pause_for_result=0;
               pflag=1;
               } else {
                  ret=MPI_Iprobe( MPI_ANY_SOURCE, RESULT_TAG,
                           MPI_COMM_WORLD, &pflag, 
                           &mstatus );
             }
    
  
           if(ret != MPI_SUCCESS || pflag == 0)
             break;
  
            MPI_Recv(mpistatus,      /* buff   */
                    MAXSTR,          /* count  */
                    MPI_CHAR,        /* type   */
                    MPI_ANY_SOURCE,  /* source */
                    RESULT_TAG,      /* tag    */
                    MPI_COMM_WORLD,  /* comm   */
                    &mstatus);       /* status */
  
            free_idx=mstatus.MPI_SOURCE;
            set_idle_node(free_idx);
            count++;
            if(strstr( mpistatus, "[struct stat=\"ABORT\"") != NULL ||
                                 strstr( mpistatus, "[struct stat=\"ERROR\"") != NULL)
            {
              fprintf(fstatus, "%s\n", mpistatus);
              failed++;
            }
          }
        }
      }

      time(&end_time);
      elapsed_time=difftime(start_time,end_time);


      fprintf(fstatus, "[struct stat=\"OK\", count=%d, failed=%d, time=%.0f]\n",
         count, failed, elapsed_time);
      fflush(fstatus);

   } else { /* I am the slave */
     int more_cmd=0;
     int done=0;

      while(1) {

        int ret;

        ret=MPI_Probe(0, MPI_ANY_TAG, MPI_COMM_WORLD, &mstatus);

        if(mstatus.MPI_TAG == CMD_TAG)
          more_cmd=1;
          else if(mstatus.MPI_TAG == EXIT_TAG) {
             done=1;
        }

        if(more_cmd) {
          MPI_Recv(cmd,         /* buff   */
               MAXSTR,          /* count  */
               MPI_CHAR,        /* type   */
               0,               /* source */
               CMD_TAG,         /* tag    */
               MPI_COMM_WORLD,  /* comm   */
               &mstatus);       /* status */


          run_it(cmd, envp, mpistatus);

          MPI_Send(mpistatus,     /* buff  */
              strlen(mpistatus)+1,  /* count */
              MPI_CHAR,           /* type  */
              0,                  /* dest  */
              RESULT_TAG,         /* tag   */
              MPI_COMM_WORLD);    /* comm  */
          more_cmd=0;
          continue;
        } 
        if(done) {
            MPI_Recv(cmd,       /* buff   */
               MAXSTR,          /* count  */
               MPI_CHAR,        /* type   */
               0,               /* source */
               EXIT_TAG,        /* tag    */
               MPI_COMM_WORLD,  /* comm   */
               &mstatus);       /* status */

            break;
        }
     }
   }

   MPI_Finalize();
   free_my_list();
   if(fin!=stdin) fclose(fin);
   exit(0);
}

void exit_panic(int e)
{
   MPI_Finalize();
   free_my_list();
   exit(e);
}

/* Process the command-line parameters */
FILE *parse_cmd_line(int argc, char **argv) 
{
   char   fname    [MAXSTR];
   int    c;
   FILE   *fin;

   while ((c = getopt(argc, argv, "ds:")) != -1) 
   {
      switch (c) 
      {
         case 'd':
            debug = 1;
            break;

         case 's':
            if((fstatus = fopen(optarg, "w+")) == (FILE *)NULL)
            {
               fprintf(fstatus,"[struct stat=\"ERROR\", msg=\"Cannot open status file: %s\"]\n",
                  optarg);
               exit_panic(1);
            }
            break;

         default:
	    fprintf(fstatus,"[struct stat=\"ERROR\", msg=\"Usage: %s [-d] [-s statusfile] [inputfile]\"]\n", argv[0]);
            exit_panic(1);
            break;
      }
   }

   if (!((optind==argc) || (argc - optind == 1)))
   {
      fprintf(fstatus,"[struct stat=\"ERROR\", msg=\"Usage: %s [-d] [-s statusfile] [inputfile]\"]\n", argv[0]);
      exit_panic(1);
   }

   if(optind != argc) {
   
     strcpy(fname,  argv[optind]);
     fin = fopen(fname, "r");
     if(fin == (FILE *)NULL)
     {
        fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Can't open command file.\"]\n");
        fflush(fstatus);
        exit_panic(1);
     }
   } else fin=stdin;

   return fin;
}

int run_it(char *cmd, char *envp[],  char *mpistatus) 
{
   char rline[125];

   int status=0;

   status=exec_cmd(cmd, envp, rline, sizeof(rline));

   if(status== -1) {
     sprintf(mpistatus, "[struct stat=\"ERROR\", msg=\"%s\"]\n", rline);
     return 1;
   }

   sprintf(mpistatus, "[struct stat=\"OK\"]\n" );
   return 0;
}

