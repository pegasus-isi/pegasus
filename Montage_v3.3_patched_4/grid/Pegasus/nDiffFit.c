/* Module: mDiffFit.c

Version  Developer        Date     Change
-------  ---------------  -------  -----------------------
1.2      Mei-Hui Su       16Mar05  Add the check for MONTAGE_HOME to
                                   access mFitplane and mDiff with 'path'
1.1      Mei-Hui Su       11Oct04  Changed one OK return to a WARNING
1.0      John Good        24Sep04  Baseline code

*/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "montage.h"

#define MAXSTR 4096

char input_file1  [MAXSTR];
char input_file2  [MAXSTR];
char output_file  [MAXSTR];
char template_file[MAXSTR];

char cmd          [MAXSTR];
char msg          [MAXSTR];
char status       [MAXSTR];

char *svc_value();
char *filePath ();

extern char *optarg;
extern int optind, opterr;

int debug;


/*******************************************************************/
/*                                                                 */
/*  mDiffFit                                                       */
/*                                                                 */
/*  Run mDiff immediatly followed by mFitplane and check the first */
/*  to decide whether to run the second.                           */
/*                                                                 */
/*******************************************************************/

int main(int argc, char **argv)
{
   int   ch;
   int   border;
   int   noAreas;
   char *end;

   char a        [MAXSTR];
   char b        [MAXSTR];
   char c        [MAXSTR];
   char crpix1   [MAXSTR];
   char crpix2   [MAXSTR];
   char xmin     [MAXSTR];
   char xmax     [MAXSTR];
   char ymin     [MAXSTR];
   char ymax     [MAXSTR];
   char xcenter  [MAXSTR];
   char ycenter  [MAXSTR];
   char npixel   [MAXSTR];
   char rms      [MAXSTR];
   char boxx     [MAXSTR];
   char boxy     [MAXSTR];
   char boxwidth [MAXSTR];
   char boxheight[MAXSTR];
   char boxang   [MAXSTR];

   char *path = getenv("MONTAGE_HOME");


   /***************************************/
   /* Process the command-line parameters */
   /***************************************/

   debug   = 0;
   noAreas = 0;
   border  = 0;

   opterr  = 0;

   fstatus = stdout;

   while ((ch = getopt(argc, argv, "ndb:s:")) != EOF) 
   {
      switch (ch) 
      {
         case 'd':
            debug = 1;
            break;

         case 'n':
            noAreas = 1;
            break;

         case 'b':
            border = strtol(optarg, &end, 0);

            if(end < optarg + strlen(optarg))
            {
               printf("[struct stat=\"ERROR\", msg=\"Argument to -b (%s) cannot be interpreted as an integer\"]\n",
                  optarg);
               exit(1);
            }

            if(border < 0)
            {
               printf("[struct stat=\"ERROR\", msg=\"Argument to -b (%s) must be a positive integer\"]\n",
                  optarg);
               exit(1);
            }

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
	    printf("[struct stat=\"ERROR\", msg=\"Usage: %s [-d] [-n(o-areas)] [-b border] [-s statusfile] in1.fits in2.fits out.fits hdr.template\"]\n", argv[0]);
            exit(1);
            break;
      }
   }

   if (argc - optind < 3) 
   {
      printf("[struct stat=\"ERROR\", msg=\"Usage: %s [-d] [-n(o-areas)] [-b border] [-s statusfile] in1.fits in2.fits out.fits hdr.template\"]\n", argv[0]);
      exit(1);
   }

   strcpy(input_file1,   argv[optind]);
   strcpy(input_file2,   argv[optind + 1]);
   strcpy(output_file,   argv[optind + 2]);
   strcpy(template_file, argv[optind + 3]);

   if(debug)
      svc_debug(stdout);


   /*******************/
   /* First run mDiff */
   /*******************/

   if(noAreas)
      if(path)
         sprintf(cmd, "%s/bin/mDiff -n %s %s %s %s", path, input_file1, input_file2,
            output_file, template_file);
      else
         sprintf(cmd, "mDiff -n %s %s %s %s", input_file1, input_file2,
            output_file, template_file);
   else
      if(path)
         sprintf(cmd, "%s/bin/mDiff %s %s %s %s", path, input_file1, input_file2,
            output_file, template_file);
      else 
         sprintf(cmd, "mDiff %s %s %s %s", input_file1, input_file2,
            output_file, template_file);


   if(debug)
   {
      printf("[%s]\n", cmd);
      fflush(stdout);
   }

   svc_run(cmd);

   strcpy( status, svc_value( "stat" ));

   if(strcmp( status, "ABORT") == 0
   || strcmp( status, "ERROR") == 0)
   {
      strcpy( msg, svc_value( "msg" ));

      fprintf(fstatus, "[struct stat=\"%s\", msg=\"%s\"]\n", status, msg);
      fflush(stdout);

      exit(1);
   }

   if(strcmp( status, "WARNING") == 0)
   {
      strcpy( msg, svc_value( "msg" ));

      fprintf(fstatus, "[struct stat=\"WARNING\", msg=\"%s\"]\n", msg);
      fflush(stdout);

      exit(0);
   }


   /**********************/
   /* Then run mFitplane */
   /**********************/

   if(path)
      sprintf(cmd, "%s/bin/mFitplane -b %d %s", path, border, output_file);
   else
      sprintf(cmd, "mFitplane -b %d %s", border, output_file);

   if(debug)
   {
      printf("[%s]\n", cmd);
      fflush(stdout);
   }

   svc_run(cmd);

   strcpy( status, svc_value( "stat" ));

   if(strcmp( status, "ABORT") == 0
   || strcmp( status, "ERROR") == 0)
   {
      strcpy( msg, svc_value( "msg" ));

      fprintf(fstatus, "[struct stat=\"%s\", msg=\"%s\"]\n", status, msg);
      fflush(stdout);

      exit(1);
   }

   else if(strcmp( status, "WARNING") == 0)
   {
      strcpy( msg, svc_value( "msg" ));

      fprintf(fstatus, "[struct stat=\"WARNING\", msg=\"%s\"]\n", msg);
      fflush(stdout);

      exit(0);
   }

   else
   {
      strcpy(a,         svc_value("a"));
      strcpy(b,         svc_value("b"));
      strcpy(c,         svc_value("c"));
      strcpy(crpix1,    svc_value("crpix1"));
      strcpy(crpix2,     svc_value("crpix2"));
      strcpy(xmin,      svc_value("xmin"));
      strcpy(xmax,      svc_value("xmax"));
      strcpy(ymin,      svc_value("ymin"));
      strcpy(ymax,      svc_value("ymax"));
      strcpy(xcenter,   svc_value("xcenter"));
      strcpy(ycenter,   svc_value("ycenter"));
      strcpy(npixel,    svc_value("npixel"));
      strcpy(rms,       svc_value("rms"));
      strcpy(boxx,      svc_value("boxx"));
      strcpy(boxy,      svc_value("boxy"));
      strcpy(boxwidth,  svc_value("boxwidth"));
      strcpy(boxheight, svc_value("boxheight"));
      strcpy(boxang,    svc_value("boxang"));


      fprintf(fstatus, "[struct stat=\"OK\", a=%s, b=%s, c=%s, crpix1=%s, crpix2=%s, xmin=%s, xmax=%s, ymin=%s, ymax=%s, xcenter=%s, ycenter=%s, npixel=%s, rms=%s, boxx=%s, boxy=%s, boxwidth=%s, boxheight=%s, boxang=%s]\n",
	 a, b, c, crpix1, crpix2, xmin, xmax, ymin, ymax, xcenter, ycenter, npixel, rms, boxx, boxy, boxwidth, boxheight, boxang);
      fflush(fstatus);

      exit(0);
   }
}
