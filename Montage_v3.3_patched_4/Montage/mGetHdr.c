/* Module: mGetHdr.c

Version  Developer        Date     Change
-------  ---------------  -------  -----------------------
1.5      John Good        25Nov03  Added extern optarg references
1.4      John Good        25Aug03  Added status file processing
1.3      John Good        30Apr03  Added checkFile() reference for infile  
1.2      John Good        14Mar03  Error: was checking for a FITS
				   header in the wrong file
1.1      John Good        14Mar03  Modified command-line processing
				   to use getopt() library
1.0      John Good        07Feb03  Baseline code

*/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <math.h>

#include "montage.h"
#include "fitsio.h"

void printFitsError(int);

extern char *optarg;
extern int optind, opterr;

extern int getopt(int argc, char *const *argv, const char *options);

int checkFile(char *filename);

int debug;


/*************************************************************************/
/*                                                                       */
/*  mGetHdr                                                              */
/*                                                                       */
/*  This program extracts the FITS header from an image into a text file */
/*                                                                       */
/*************************************************************************/

int main(int argc, char **argv)
{
   char      infile [1024];
   char      hdrfile[1024];
   char     *end;

   fitsfile *infptr;

   FILE     *fout;

   int       i, j, c, hdu;

   int       ncard;
   char      card[256];

   int       status = 0;

   int       morekeys;


   debug  = 0;
   opterr = 0;
   hdu    = 0;

   fstatus = stdout;

   while ((c = getopt(argc, argv, "ds:h:")) != EOF) 
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

         case 'h':
            hdu = strtol(optarg, &end, 10);

            if(end < optarg + strlen(optarg) || hdu < 0)
            {
               printf("[struct stat=\"ERROR\", msg=\"HDU value (%s) must be a non-negative integer\"]\n",
                  optarg);
               exit(1);
            }
            break;

         default:
	    printf("[struct stat=\"ERROR\", msg=\"Usage: %s [-d][-h hdu][-s statusfile] img.fits img.hdr\"]\n", argv[0]);
            exit(1);
            break;
      }
   }

   if (argc - optind < 2) 
   {
      printf("[struct stat=\"ERROR\", msg=\"Usage: %s [-d][-h hdu][-s statusfile] img.fits img.hdr\"]\n", argv[0]);
      exit(1);
   }

   strcpy(infile,  argv[optind]);
   strcpy(hdrfile, argv[optind + 1]);

   if(checkFile(infile) != 0)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Image file (%s) does not exist\"]\n",
         infile);
      exit(1);
   }

   /* checkHdr(infile, 0, hdu); */



   /**********************/
   /* Open the FITS file */
   /**********************/

   fout = fopen(hdrfile, "w+");

   if(fout == (FILE *)NULL)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Can't open output header file.\"]\n");
      fflush(stdout);
      exit(1);
   }


   /******************************************/
   /* Open the FITS file and read the header */
   /******************************************/

   if(fits_open_file(&infptr, infile, READONLY, &status))
      printFitsError(status);

   if(hdu > 0)
   {
      if(fits_movabs_hdu(infptr, hdu+1, NULL, &status))
         printFitsError(status);
   }

   if(fits_get_hdrspace(infptr, &ncard, &morekeys, &status))
      printFitsError(status);

   if(debug)
   {
      printf("%d cards\n", ncard);
      fflush(stdout);
   }

   for(i=1; i<=ncard; ++i)
   {
      fits_read_record(infptr, i, card, &status);

      for(j=(int)strlen(card)-1; j>=0; --j)
      {
	 if(card[j] != ' ')
	    break;
	 
	 card[j] = '\0';
      }

      fprintf(fout, "%s\n", card);
      fflush(fout);

      if(debug)
      {
	 printf("card %3d: [%s]\n", i, card);
	 fflush(stdout);
      }
   }

   fprintf(fout, "END\n");
   fflush(fout);

   fclose(fout);

   fits_close_file(infptr, &status);

   fprintf(fstatus, "[struct stat=\"OK\", ncard=%d]\n", ncard);
   fflush(stdout);
   exit(0);
}



/***********************************/
/*                                 */
/*  Print out FITS library errors  */
/*                                 */
/***********************************/

void printFitsError(int status)
{
   char status_str[FLEN_STATUS];

   fits_get_errstatus(status, status_str);

   fprintf(fstatus, "[struct stat=\"ERROR\", flag=%d, msg=\"%s\"]\n", status, status_str);

   exit(1);
}
