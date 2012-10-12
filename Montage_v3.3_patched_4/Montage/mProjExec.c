/* Module: mProjExec.c

Version  Developer        Date     Change
-------  ---------------  -------  -----------------------
3.9      T. P. Robitaille 19Aug10  fixed gap issue in MPI version
3.8      Daniel S. Katz   16Jul10  fixes for MPI version
3.7      John Good        07Oct07  When using the -r flag, append to stats.tbl
3.6      John Good        06Dec06  Restructured the mTANHdr checks.  It wasn't
				   properly catching coordinate system 
				   differences.
3.5      John Good        01Jun06  Added support for "hdu" column in image
				   table
3.4      John Good        21Mar06  Behaved incorrectly if mTANHdr failed
				   (should go ahead and use mProject)
3.3      John Good        04Aug05  Added option (-X) to force reprojection
				   of whole images
3.2      John Good        31May05  Added option flux rescaling
				   (e.g. magnitude zero point correction)
3.1      John Good        22Feb05  Updates to output messages: double errors
				   in one case and counts were off if restart
3.0      John Good        07Feb05  Updated logic to allow automatic selection
                                   of mTANHdr/mProjectPP processing if it is
                                   possible to do so without large errors
				   (> 0.1 pixel).
2.1      Daniel S. Katz   16Dec04  Added optional parallel roundrobin
                                   computation
2.0      John Good        10Sep04  Changed border handling to allow polygon
				   outline
1.10     John Good        27Aug04  Fixed restart logic (and usage message)
1.9      John Good        05Aug04  Added "restart" to usage and fixed
				   restart error message
1.8      John Good        29Jul04  Fixed "Usage" statement text
1.7      John Good        28Jul04  Added a "restart" index flag '-s n' to
				   allow starting back up after an error
1.6      John Good        28Jan04  Added switch to allow use of mProjectPP
1.5      John Good        25Nov03  Added extern optarg references
1.4      John Good        25Aug03  Added status file processing
1.3      John Good        25Mar03  Checked -p argument (if given) to see
				   if it is a directory, the output directory
				   to see if it exists and the images.tbl
				   file to see if it exists
1.2      John Good        23Mar03  Modified output table to include mProject
				   message string for errors
1.1      John Good        14Mar03  Added filePath() processing,
                                   -p argument, and getopt()
				   argument processing.  Return error
				   if mProject not in path.
1.0      John Good        29Jan03  Baseline code

*/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <math.h>
#include <mtbl.h>
#include <fitsio.h>
#include <wcs.h>

#ifdef MPI
#include <mpi.h>
#endif

#include "montage.h"

#define MAXSTR 4096
#define MAXHDR 80000

#define INTRINSIC 0
#define COMPUTED  1
#define FAILED    2

char *svc_value();
char *svc_run  (char *cmd);

char *filePath (char *path, char *fname);
char *fileName (char *filename);
int   checkFile(char *filename);
int   checkHdr (char *infile, int hdrflag, int hdu);

int   readTemplate(char *filename);
int   stradd(char *header, char *card);

struct WorldCoor *wcsin, *wcsout;

extern char *optarg;
extern int optind, opterr;

extern int getopt(int argc, char *const *argv, const char *options);


int debug;


/*******************************************************************/
/*                                                                 */
/*  mProjExec                                                      */
/*                                                                 */
/*  Runs mProject on a set of images, given the final mosaic       */
/*  header file, a list of images, and a location to put the       */
/*  projected data.                                                */
/*                                                                 */
/*******************************************************************/

int main(int argc, char **argv)
{
   int    c, stat, ncols, count, hdu, failed, nooverlap, exact;
   int    wholeImages, ifname, ihdu, iscale, restart, inp2p, outp2p;
   int    tryAltOut, tryAltIn, wcsMatch;

   int    energyMode = 0;

   double error, maxerror, scale;

   char   path     [MAXSTR];
   char   tblfile  [MAXSTR];
   char   template [MAXSTR];
   char   projdir  [MAXSTR];
   char   stats    [MAXSTR];
   char   fname    [MAXSTR];
   char   infile   [MAXSTR];
   char   outfile  [MAXSTR];
   char   border   [MAXSTR];
   char   scaleCol [MAXSTR];
   char   scaleStr [MAXSTR];
   char   wholeStr [MAXSTR];
   char   hdustr   [MAXSTR];

   char origstr    [MAXSTR];
   char altinstr   [MAXSTR];
   char altoutstr  [MAXSTR];

   char   cmd      [MAXSTR];
   char   msg      [MAXSTR];
   char   status   [32];
   char  *end;
   char  *inheader;

   FILE   *fout;

   fitsfile *infptr;

   int    fitsstat = 0;

#ifdef MPI
   FILE   *fin;
   int    i;
   int    MPI_size, MPI_rank, MPI_err;
   int    row_number = 0;
   int    exit_flag = 0;
   int    sum_tmp;
   char   orig_stats [MAXSTR];
   char   tmp        [MAXSTR];
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

   inheader = malloc(MAXHDR);


   /***************************************/
   /* Process the command-line parameters */
   /***************************************/

   debug   = 0;
   exact   = 0;
   restart = 0;

   wholeImages = 0;

   strcpy(path,     "");
   strcpy(border,   "");
   strcpy(scaleCol, "");

   opterr = 0;

   fstatus = stdout;

   while ((c = getopt(argc, argv, "p:deb:s:r:x:Xf")) != EOF) 
   {
      switch (c) 
      {
         case 'p':
	    strcpy(path, optarg);

	    if(checkFile(path) != 2)
	    {
	       printf("[struct stat=\"ERROR\", msg=\"Path (%s) is not a directory\"]\n", path);
	       exit(1);
	    }

            break;

         case 'd':
            debug = 1;
            break;

         case 'e':
            exact = 1;
            break;

         case 'X':
            wholeImages = 1;
            break;

         case 'b':
            strcpy(border, optarg);
            break;

         case 'x':
            strcpy(scaleCol, optarg);
            break;

         case 'f':
            energyMode = 1;
            break;

         case 'r':
            restart = strtol(optarg, &end, 10);

            if(end < optarg + strlen(optarg))
            {
               printf("[struct stat=\"ERROR\", msg=\"Restart index value string (%s) cannot be interpreted as an integer\"]\n",
                  optarg);
#ifdef MPI
	       exit_flag = 1;
#else
	       exit(1);
#endif
            }

            if(restart < 0)
            {
               printf("[struct stat=\"ERROR\", msg=\"Restart index value (%d) must be greater than or equal to zero\"]\n",
                  restart);
#ifdef MPI
	       exit_flag = 1;
#else
	       exit(1);
#endif
            }

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
	    printf("[struct stat=\"ERROR\", msg=\"Usage: %s [-p rawdir] [-d] [-e(xact)] [-X(whole image)] [-b border] [-r restartrec] [-s statusfile] [-x scaleColumn] images.tbl template.hdr projdir stats.tbl\"]\n", argv[0]);
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
#endif

   if (argc - optind < 4) 
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Usage: %s [-p rawdir] [-d] [-e(xact)] [-X(whole image)] [-b border] [-r restartrec] [-s statusfile] [-x scaleColumn] images.tbl template.hdr projdir stats.tbl\"]\n", argv[0]);
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
   strcpy(template, argv[optind + 1]);
   strcpy(projdir,  argv[optind + 2]);
   strcpy(stats,    argv[optind + 3]);

   if(checkFile(tblfile) != 0)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Image metadata file (%s) does not exist\"]\n", tblfile);
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

   if(checkFile(projdir) != 2)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Output directory (%s) does not exist\"]\n", projdir);
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

   checkHdr(template, 1, 0);

#ifdef MPI
   /* each process will write its own status file, to be combined later */
   strcpy(orig_stats, stats);
   (void) sprintf(stats, "%s_%d", orig_stats, MPI_rank);
#endif

/* Added T.P.R 20100819: create unique filenames for orig, altin, altout to avoid MPI clashes*/
#ifdef MPI
   sprintf(origstr, "orig_%d.hdr", MPI_rank);
   sprintf(altinstr, "altin_%d.hdr", MPI_rank);
   sprintf(altoutstr, "altout_%d.hdr", MPI_rank);
#else
   strcpy(origstr, "orig.hdr");
   strcpy(altinstr, "altin.hdr");
   strcpy(altoutstr, "altout.hdr");
#endif

   if(restart > 0)
      fout = fopen(stats, "a+");
   else
      fout = fopen(stats, "w+");

   if(fout == (FILE *)NULL)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Can't open output file.\"]\n");
      fflush(fstatus);
      exit(0);
   }


   /*************************************************/
   /* Try to generate an alternate header so we can */
   /* use the fast projection                       */
   /*************************************************/

   outp2p = FAILED;

   readTemplate(template);

   tryAltOut = 1;

   if(exact)
      tryAltOut = 0;

   if(debug)
   {
      printf("Output wcs ptype: [%s]\n", wcsout->ptype);
      fflush(stdout);
   }

   if(   strcmp(wcsout->ptype, "TAN") == 0
      || strcmp(wcsout->ptype, "SIN") == 0
      || strcmp(wcsout->ptype, "ZEA") == 0
      || strcmp(wcsout->ptype, "STG") == 0
      || strcmp(wcsout->ptype, "ARC") == 0)
   {
      tryAltOut = 0;

      outp2p = INTRINSIC;
   }

   if(tryAltOut)
   {
      sprintf(cmd, "mTANHdr %s %s/%s", template, projdir, altoutstr);

      if(debug)
      {
	 printf("[%s]\n", cmd);
	 fflush(stdout);
      }

      svc_run(cmd);

      strcpy( status, svc_value( "stat" ));

      if(strcmp( status, "ERROR") == 0)
      {
         outp2p = FAILED;
      }
      else
      {
	 outp2p = COMPUTED;

	 maxerror = 0.;

	 error = atof(svc_value("fwdxerr"));

	 if(error > maxerror)
	    maxerror = error;

	 error = atof(svc_value("fwdyerr"));

	 if(error > maxerror)
	    maxerror = error;

	 error = atof(svc_value("revxerr"));

	 if(error > maxerror)
	    maxerror = error;

	 error = atof(svc_value("revyerr"));

	 if(error > maxerror)
	    maxerror = error;

	 if(debug)
	 {
	    printf("Using distorted TAN on output: max error = %-g\n", maxerror);
	    fflush(stdout);
	 }

	 if(maxerror > 0.1)
	    outp2p = FAILED;
      }
   }


   /**********************************/ 
   /* Open the image list table file */
   /**********************************/ 

   ncols = topen(tblfile);

   ihdu = tcol("hdu");

   ifname = tcol( "fname");

   if(ifname < 0)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Need column fname in input\"]\n");
#ifdef MPI
      exit_flag = 1;
#else
      exit(1);
#endif
   }

   iscale = -1;

   if(strlen(scaleCol) > 0)
   {
      iscale = tcol(scaleCol);

      if(iscale < 0)
      {
	 fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Need column %s in input\"]\n", 
	    scaleCol);
#ifdef MPI
	 exit_flag = 1;
#else
	 exit(1);
#endif
      }
   }

#ifdef MPI
   MPI_err = MPI_Allreduce(&exit_flag, &sum_tmp, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
   if (sum_tmp != 0) exit(1);
#endif


   /**************************************/ 
   /* Read the records and call mProject */
   /**************************************/ 

   count     = 0;
   failed    = 0;
   nooverlap = 0;

#ifndef MPI
   if(restart == 0)
      fprintf(fout, "|%-60s|%-30s|%10s|\n", "fname", "status", "time");
   fflush(fout);
#endif

   while(1)
   {
      stat = tread();

      if(stat < 0)
	 break;

      hdu = 0;
      if(ihdu >= 0)
	 hdu = atoi(tval(ihdu));

#ifdef MPI
    if (row_number % MPI_size == MPI_rank) {
#endif

      ++count;

      if(count <= restart)
      {
	 if(debug)
	 {
	    printf("Skipping [%s]\n", filePath(path, tval(ifname)));
	    fflush(stdout);
	 }

	 continue;
      }

      strcpy(infile,  filePath(path, tval(ifname)));

      strcpy(outfile, projdir);

      if(outfile[strlen(outfile) - 1] != '/')
	 strcat(outfile, "/");

      strcpy(hdustr, "");

      if(ihdu >= 0)
	 sprintf(hdustr, "hdu%d_", hdu);

      sprintf(fname, "%s%s", hdustr, fileName(tval(ifname)));

      strcat(outfile, fname);

      if(strcmp(infile, outfile) == 0)
      {
	 fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Output would overwrite input\"]\n");
#ifdef MPI
	 exit_flag = 1;
#else
	 exit(1);
#endif
      }


      /* Try to generate an alternate input header so we can */
      /* use the fast projection                             */

      fitsstat = 0;

      if(checkFile(infile) != 0)
      {
	 if(debug)
	 {
	    printf("Image file [%s] does not exist\n", infile);
	    fflush(stdout);
	 }

	 ++failed;
	 continue;
      }

      if(fits_open_file(&infptr, infile, READONLY, &fitsstat))
      {
	 if(debug)
	 {
	    printf("FITS open failed for [%s]\n", infile);
	    fflush(stdout);
	 }

	 ++failed;
	 continue;
      }

      if(hdu > 0)
      {
	 if(fits_movabs_hdu(infptr, hdu+1, NULL, &fitsstat))
	 {
	    if(debug)
	    {
	       printf("FITS move to HDU failed for [%s]\n", infile);
	       fflush(stdout);
	    }

	    ++failed;
	    continue;
	 }
      }

      if(fits_get_image_wcs_keys(infptr, &inheader, &fitsstat))
      {
	 if(debug)
	 {
	    printf("FITS get WCS keys failed for [%s]\n", infile);
	    fflush(stdout);
	 }

	 ++failed;
	 continue;
      }

      if(fits_close_file(infptr, &fitsstat))
      {
	 if(debug)
	 {
	    printf("FITS close failed for [%s]\n", infile);
	    fflush(stdout);
	 }

	 ++failed;
	 continue;
      }

      wcsin = wcsinit(inheader);

      if(wcsin == (struct WorldCoor *)NULL)
      {
	 if(debug)
	 {
	    printf("WCS init failed for [%s]\n", infile);
	    fflush(stdout);
	 }

	 ++failed;
	 continue;
      }

      inp2p = FAILED;
      
      tryAltIn = 1;

      if(exact)
	 tryAltIn = 0;
      
      wcsMatch = 1;

      if(wcsin->syswcs != wcsout->syswcs)
      {
	 tryAltIn = 0;
	 wcsMatch = 0;
      }

      if(debug)
      {
	 printf("Input wcs ptype: [%s]\n", wcsin->ptype);
	 fflush(stdout);
      }

      if(   strcmp(wcsin->ptype, "TAN") == 0
	 || strcmp(wcsin->ptype, "SIN") == 0
	 || strcmp(wcsin->ptype, "ZEA") == 0
	 || strcmp(wcsin->ptype, "STG") == 0
	 || strcmp(wcsin->ptype, "ARC") == 0)
      {
	 tryAltIn = 0;

	 inp2p = INTRINSIC;
      }

      if(tryAltIn)
      {
	 strcpy(hdustr, "");

	 if(ihdu >= 0)
	    sprintf(hdustr, "-h %d", hdu);

	 sprintf(cmd, "mGetHdr %s %s %s/%s", hdustr, infile, projdir, origstr);

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
	 else if(strcmp( status, "ERROR") == 0)
	 {
	    ++failed;
	    continue;
	 }

	 sprintf(cmd, "mTANHdr %s/%s %s/%s",
	    projdir, origstr, projdir, altinstr);

	 if(debug)
	 {
	    printf("[%s]\n", cmd);
	    fflush(stdout);
	 }

	 svc_run(cmd);

	 strcpy( status, svc_value( "stat" ));

	 if(strcmp( status, "ABORT") == 0)
	 {
	    inp2p = FAILED;

	    strcpy( msg, svc_value( "msg" ));

	    fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"%s\"]\n", msg);
	    fflush(stdout);

	    exit(1);
	 }
	 else if(strcmp( status, "ERROR") == 0)
	 {
	    inp2p = FAILED;

	    ++failed;
	    continue;
	 }
	 else
	 {
	    inp2p = COMPUTED;

	    maxerror = 0.;

	    error = atof(svc_value("fwdxerr"));

	    if(error > maxerror)
	       maxerror = error;

	    error = atof(svc_value("fwdyerr"));

	    if(error > maxerror)
	       maxerror = error;

	    error = atof(svc_value("revxerr"));

	    if(error > maxerror)
	       maxerror = error;

	    error = atof(svc_value("revyerr"));

	    if(error > maxerror)
	       maxerror = error;


	    if(debug)
	    {
	       printf("Using distorted TAN on input: max error = %-g\n", maxerror);
	       fflush(stdout);
	    }

	    if(maxerror > 0.1)
	       inp2p = FAILED;
	 }
      }


      /* Now run mProject or mProjectPP (depending */
      /* on what we have to work with)             */

      if(wholeImages)
	 strcpy(wholeStr, " -X");
      else
	 strcpy(wholeStr, "");

      if(energyMode)
	 strcat(wholeStr, " -f");
      else
	 strcat(wholeStr, "");

      strcpy(hdustr, "");

      if(ihdu >= 0)
	 sprintf(hdustr, "-h %d", hdu);

      if(iscale >= 0)
      {
	 scale = atof(tval(iscale));

	 if(scale == 0.)
	    scale = 1;

	 sprintf(scaleStr, "-x %-g%s", scale, wholeStr);
      }
      else
	 strcpy(scaleStr, wholeStr);

      if(exact && (inp2p != INTRINSIC || outp2p != INTRINSIC))
      {
	 inp2p  = FAILED;
	 outp2p = FAILED;
      }

      if(strlen(border) == 0)
      {
	 if(!wcsMatch)
	    sprintf(cmd, "mProject %s %s %s %s %s",
	       scaleStr, hdustr, infile, outfile, template);

	 else if(inp2p == COMPUTED  && outp2p == COMPUTED )
	    sprintf(cmd, "mProjectPP %s %s -i %s/%s -o %s/%s %s %s %s",
	       scaleStr, hdustr, projdir, altinstr, projdir, altoutstr, infile, outfile, template);

	 else if(inp2p == COMPUTED  && outp2p == INTRINSIC)
	    sprintf(cmd, "mProjectPP %s %s -i %s/%s %s %s %s",
	       scaleStr, hdustr, projdir, altinstr, infile, outfile, template);

	 else if(inp2p == INTRINSIC && outp2p == COMPUTED )
	    sprintf(cmd, "mProjectPP %s %s -o %s/%s %s %s %s",
	       scaleStr, hdustr, projdir, altoutstr, infile, outfile, template);

	 else if(inp2p == INTRINSIC && outp2p == INTRINSIC)
	    sprintf(cmd, "mProjectPP %s %s %s %s %s",
	       scaleStr, hdustr, infile, outfile, template);

	 else
	    sprintf(cmd, "mProject %s %s %s %s %s",
	       scaleStr, hdustr, infile, outfile, template);
      }
      else
      {
	 if(!wcsMatch)
	    sprintf(cmd, "mProject %s %s -b \"%s\" %s %s %s",
	       scaleStr, hdustr, border, infile, outfile, template);

	 else if(inp2p == COMPUTED  && outp2p == COMPUTED )
	    sprintf(cmd, "mProjectPP %s %s -b \"%s\" -i %s/%s -o %s/%s %s %s %s",
	       scaleStr, hdustr, border, projdir, altinstr, projdir, altoutstr, infile, outfile, template);

	 else if(inp2p == COMPUTED  && outp2p == INTRINSIC)
	    sprintf(cmd, "mProjectPP %s %s -b \"%s\" -i %s/%s %s %s %s",
	       scaleStr, hdustr, border, projdir, altinstr, infile, outfile, template);

	 else if(inp2p == INTRINSIC && outp2p == COMPUTED )
	    sprintf(cmd, "mProjectPP %s %s -b \"%s\" -o %s/%s %s %s %s",
	       scaleStr, hdustr, border, projdir, altoutstr, infile, outfile, template);

	 else if(inp2p == INTRINSIC && outp2p == INTRINSIC)
	    sprintf(cmd, "mProjectPP %s %s -b \"%s\" %s %s %s",
	       scaleStr, hdustr, border, infile, outfile, template);

	 else
	    sprintf(cmd, "mProject %s %s -b \"%s\" %s %s %s",
	       scaleStr, hdustr, border, infile, outfile, template);
      }

      if(debug)
      {
	 printf("wcsMatch = %d\n", wcsMatch);

	 if(wcsMatch)
	 {
	    if( inp2p == COMPUTED)  printf(" inp2p = COMPUTED\n");
	    if( inp2p == INTRINSIC) printf(" inp2p = INTRINSIC\n");
	    if( inp2p == FAILED)    printf(" inp2p = FAILED\n");

	    if(outp2p == COMPUTED)  printf("outp2p = COMPUTED\n");
	    if(outp2p == INTRINSIC) printf("outp2p = INTRINSIC\n");
	    if(outp2p == FAILED)    printf("outp2p = FAILED\n");
	 }

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

      else if(strcmp( status, "ERROR") == 0)
      {
	 strcpy( msg, svc_value( "msg" ));

	 if(strlen(msg) > 30)
	    msg[30] = '\0';

	 if(strcmp( msg, "No overlap")           == 0
	 || strcmp( msg, "All pixels are blank") == 0)
	 {
	    ++nooverlap;
	    fprintf(fout, " %-60s %-30s %10s\n", fileName(tval(ifname)), msg, "");
	 }
	 else
	 {
	    ++failed;
	    fprintf(fout, " %-60s %-30s %10s\n", fileName(tval(ifname)), msg, "");
	 }
      }
      else
	 fprintf(fout, " %-60s %-30s %10s\n", fileName(tval(ifname)), status, svc_value("time"));

      fflush(fout);
#ifdef MPI
     } // end if (row_number % MPI_size == MPI_rank)
     row_number++;
#endif
   }

   fclose(fout);

#ifdef MPI
   MPI_err = MPI_Allreduce(&count, &sum_tmp, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
   count = sum_tmp;
   MPI_err = MPI_Allreduce(&failed, &sum_tmp, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
   failed = sum_tmp;
   MPI_err = MPI_Allreduce(&nooverlap, &sum_tmp, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
   nooverlap = sum_tmp;

   if (MPI_rank == 0) {
#endif
   fprintf(fstatus, "[struct stat=\"OK\", count=%d, failed=%d, nooverlap=%d]\n",
      count-restart, failed, nooverlap);
   fflush(stdout);

#ifdef MPI
   fout = fopen(orig_stats, "w+");

   if(fout == (FILE *)NULL)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Can't open output file.\"]\n");
      fflush(stdout);
   }
   fprintf(fout, "|%-60s|%-30s|%10s|\n", "fname", "status", "time");

   for (i=0;i<MPI_size;i++) {
     (void) sprintf(stats, "%s_%d", orig_stats, i);
     // read all lines from stats and copy to orig_stats, then delete stats
     fin = fopen(stats, "r+");
     if(fin == (FILE *)NULL)
       {
         fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Can't open tmp status file.\"]\n");
         fflush(stdout);
        }
     while (fgets(tmp,MAXSTR,fin) != NULL) {
       fputs (tmp,fout);
     }
     fclose(fin);
     if (unlink(stats) < 0) {
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



/**************************************************/
/*                                                */
/*  Read the output header template file.         */
/*  Create a single-string version of the         */
/*  header data and use it to initialize the      */
/*  output WCS transform.                         */
/*                                                */
/**************************************************/

int readTemplate(char *filename)
{
   int       j;
   FILE     *fp;
   char      line[MAXSTR];
   char      header[80000];
#ifdef MPI
   int       exit_flag;
#endif


   /********************************************************/
   /* Open the template file, read and parse all the lines */
   /********************************************************/

   fp = fopen(filename, "r");

   if(fp == (FILE *)NULL)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Template file %s not found.\"]\n",
         filename);
#ifdef MPI
      exit_flag = 1;
#else
      exit(1);
#endif
   }

   strcpy(header, "");

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

      stradd(header, line);
   }


   /****************************************/
   /* Initialize the WCS transform library */
   /****************************************/

   wcsout = wcsinit(header);

   if(wcsout == (struct WorldCoor *)NULL)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Output wcsinit() failed.\"]\n");
      exit(1);
   }

   return 0;
}


/* stradd adds the string "card" to a header line, and */
/* pads the header out to 80 characters.               */

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
