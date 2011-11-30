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
 * Module: seqexec.c
 * Author : Mei-hui Su
 * Revision : $REVISION$
 */


	#include <stdio.h>
	#include <stdlib.h>
	#include <unistd.h>
	#include <string.h>
	#include <time.h>

	#include "mypopen.h"

	#define MAXSTR 4096

	char *fileName();

	extern char *optarg;
	extern int optind, opterr;

	int debug;
	FILE *fstatus;


	/*******************************************************************/
	/*                                                                 */
	/*  mGenericExec                                                   */
	/*                                                                 */
	/*  Runs some commands from some input file or stdin sequentially  */
	/*                                                                 */
	/*******************************************************************/

	int main(int argc, char **argv, char *envp[])
	{
	   int    c, count, failed;

	   char   fname    [MAXSTR];
	   char   cmd      [MAXSTR];
	   char   msg      [MAXSTR];
	   int    status;
	   char   rline    [1024];
	   long   result;

	   FILE   *fin;
	   FILE   *fout = stdout;

	   time_t start_time;
	   time_t end_time;
	   double elapsed_time;

	   time(&start_time);


	   /***************************************/
	   /* Process the command-line parameters */
	   /***************************************/

	   debug  = 0;

	   fstatus = stdout;

   while ((c = getopt(argc, argv, "ds:")) != EOF) 
   {
      switch (c) 
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
	    printf("[struct stat=\"ERROR\", msg=\"Usage: %s [-d] [-s statusfile] [inputfile] \"]\n", argv[0]);
            exit(1);
            break;
      }
   }

   if (!((argc - optind != 1) || (argc == optind)))
   {
      printf("[struct stat=\"ERROR\", msg=\"Usage: %s [-d] [-s statusfile] [inputfile]\"]\n", argv[0]);
      exit(1);
   }

   if(optind != argc) {
     strcpy(fname,  argv[optind]);

     fin = fopen(fname, "r");
     if(fin == (FILE *)NULL)
     {
        fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Can't open command file.\"]\n");
        exit(1);
     }
   } else fin=stdin;


   /************************************************/ 
   /* Read the commands and call each sequentially */
   /************************************************/ 

   count     = 0;
   failed    = 0;

   while(fgets(cmd, MAXSTR, fin) != (char *)NULL) {
      result = -1;

      status=exec_cmd(cmd, envp, rline, sizeof(rline));
      ++count;

      if ( status == -1 ) {
         fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"%s\"]\n", rline);
         failed++;
      }
   }

   if(fin != stdin)
     fclose(fin);

   time(&end_time);
   elapsed_time=difftime(start_time,end_time);

   fprintf(fstatus, "[struct stat=\"OK\", count=%d, failed=%d, time=%.0f, stime=(%s)]\n",
      count, failed, elapsed_time, ctime(&start_time));

   fflush(stdout);
   exit(0);
}
