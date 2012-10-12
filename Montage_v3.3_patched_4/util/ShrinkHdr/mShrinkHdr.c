/* Module: mShrinkHdr

Version  Developer        Date     Change
-------  ---------------  -------  -----------------------
1.0      John Good        08Aug07  Baseline code

*/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <math.h>

#include "coord.h"
#include "wcs.h"

#define MAXSTR  256

int stradd      (char *header, char *card);
int readTemplate(char *template);
int printHeader (char *header);

static struct WorldCoor *wcs;

FILE    *fstatus;

extern char *optarg;
extern int optind, opterr;

extern int getopt(int argc, char *const *argv, const char *options);

int checkHdr(char *infile, int hdrflag, int hdu);

int wcsSys; 

int debug;


/*************************************************************************/
/*                                                                       */
/*  mShrinkHdr                                                           */
/*                                                                       */
/*  Montage is a set of general reprojection / coordinate-transform /    */
/*  mosaicking programs.  Any number of input images can be merged into  */
/*  an output FITS file.  The attributes of the input are read from the  */
/*  input files; the attributes of the output are read a combination of  */
/*  the command line and a FITS header template file.                    */
/*                                                                       */
/*  This module takes an image FITS header template and creates a new    */
/*  one with a scale suitable for thumbnails, etc. (i.e. NAXIS1,2 less   */
/*  than the given scale; default 200)                                   */
/*                                                                       */
/*************************************************************************/

int main(int argc, char **argv)
{
   int      c;

   char     itmpl[MAXSTR];
   char     otmpl[MAXSTR];

   int      scale;
   double   maxside, factor;

   FILE    *fout;

   scale = 200;


   /************************************/
   /* Read the command-line parameters */
   /************************************/

   opterr    =  0;
   debug     =  0;
   fstatus   = stdout;

   while ((c = getopt(argc, argv, "ds:")) != EOF) 
   {
      switch (c) 
      {
         case 'd':
            debug = 1;
            break;

         case 's':
            scale = atoi(optarg);
            break;

         default:
	    fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Usage: %s [-d][-s scale] template.hdr shrunken.hdr\"]\n", 
	       argv[0]);
            exit(1);
            break;
      }
   }

   if (argc - optind < 2) 
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Usage: %s [-d][-s scale] template.hdr shrunken.hdr\"]\n", 
	 argv[0]);
      exit(1);
   }

   strcpy(itmpl, argv[optind]);
   strcpy(otmpl, argv[optind+1]);

   fout = fopen(otmpl, "w+");

   if(fout == (FILE *)NULL)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Cannot open output template file\"]\n");
      exit(1);
   }

   checkHdr(itmpl, 1, 0);

   if(debug)
   {
      printf("DEBUG> Command-line read.\n");
      fflush(stdout);
   }


   /*************************************/
   /* Read the template file and set up */
   /* the WCS for the original image    */
   /*************************************/

   readTemplate(itmpl);


   /*************************************/
   /* Compute the new values for NAXIS  */
   /* CRPIX valuesthe original image    */
   /*************************************/

   maxside = wcs->nxpix;
   if(wcs->nypix > maxside)
      maxside = wcs->nypix;

   factor = scale / maxside;

   wcs->nxpix   = (int)(wcs->nxpix * factor) + 5.;
   wcs->nypix   = (int)(wcs->nypix * factor) + 5.;

   wcs->xrefpix =  wcs->xrefpix * factor + 2.5;
   wcs->yrefpix =  wcs->yrefpix * factor + 2.5;

   wcs->xinc    =  wcs->xinc    / factor;
   wcs->yinc    =  wcs->yinc    / factor;


   /*************************************/
   /* Read the template file and set up */
   /* the WCS for the original image    */
   /*************************************/

   fprintf(fout, "SIMPLE  = T\n"                        );
   fprintf(fout, "BITPIX  = -64\n"                      );
   fprintf(fout, "NAXIS   = 2\n"                        );
   fprintf(fout, "NAXIS1  = %d\n",     (int)(wcs->nxpix));
   fprintf(fout, "NAXIS2  = %d\n",     (int)(wcs->nypix));
   fprintf(fout, "CTYPE1  = '%s'\n",   wcs->ctype[0]    );
   fprintf(fout, "CTYPE2  = '%s'\n",   wcs->ctype[1]    );
   fprintf(fout, "CRVAL1  = %11.6f\n", wcs->crval[0]    );
   fprintf(fout, "CRVAL2  = %11.6f\n", wcs->crval[1]    );
   fprintf(fout, "CRPIX1  = %11.6f\n", wcs->xrefpix     );
   fprintf(fout, "CRPIX2  = %11.6f\n", wcs->yrefpix     );
   fprintf(fout, "CDELT1  = %14.9f\n", wcs->xinc        );
   fprintf(fout, "CDELT2  = %14.9f\n", wcs->yinc        );
   fprintf(fout, "CROTA2  = %11.6f\n", wcs->rot         );
   fprintf(fout, "EQUINOX = %9.2f\n",  wcs->equinox     );
   fprintf(fout, "END\n"                                );

   fclose(fout);

   printf("[struct stat=\"OK\"]\n");

   exit(0);
}



/**************************************************/
/*                                                */
/*  Read the output header template file.         */
/*  Specifically extract the image size info.     */
/*  Also, create a single-string version of the   */
/*  header data and use it to initialize the      */
/*  output WCS transform.                         */
/*                                                */
/**************************************************/

int readTemplate(char *template)
{
   int       j;
   FILE     *fp;
   char      line[MAXSTR];
   char      header[80000];

   fp = fopen(template, "r");

   if(fp == (FILE *)NULL)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Bad template: %s\"]\n", 
	 template);
      exit(1);
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

      stradd(header, line);
   }

   if(debug)
   {
      printf("\nDEBUG> Original Header:\n\n");
      fflush(stdout);

      printHeader(header);
      fflush(stdout);
   }

   wcs = wcsinit(header);

   if(wcs == (struct WorldCoor *)NULL)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Output wcsinit() failed.\"]\n");
      exit(1);
   }

   if(strncmp(wcs->c1type, "RA", 2) == 0) wcsSys = EQUJ;
   if(strncmp(wcs->c1type, "EL", 2) == 0) wcsSys = ECLJ;
   if(strncmp(wcs->c1type, "GL", 2) == 0) wcsSys = GAL;

   if(debug)
   {
      printf("DEBUG> Original image WCS initialized\n\n");
      fflush(stdout);
   }

   fclose(fp);

   return 0;
}



int stradd(char *header, char *card)
{
   int i, hlen, clen;

   hlen = strlen(header);
   clen = strlen(card);

   for(i=0; i<clen; ++i)
      header[hlen+i] = card[i];

   if(clen < 80)
      for(i=clen; i<80; ++i)
         header[hlen+i] = ' ';
   
   header[hlen+80] = '\0';

   return(strlen(header));
}


/*********************************************************/
/*                                                       */
/* Format the FITS header string and print out the lines */
/*                                                       */
/*********************************************************/

int printHeader(char *header)
{
   int  i, j, len, linecnt;
   char line[81];

   len = strlen(header);

   linecnt = 0;

   while(1)
   {
      for(i=0; i<80; ++i)
         line[i] = '\0';

      for(i=0; i<80; ++i)
      {
         j = linecnt * 80 + i;
         
         if(j > len)
            break;

         line[i] = header[j];
      }
      
      line[80] = '\0';

      for(i=80; i>=0; --i)
      {
         if(line[i] != ' ' && line[i] != '\0')
            break;
         else
            line[i] = '\0';
      }
      
      if(strlen(line) > 0)
         printf("%4d: %s\n", linecnt+1, line);

      if(j > len)
         break;

      ++linecnt;
   }

   printf("\n");
   return 0;
}
