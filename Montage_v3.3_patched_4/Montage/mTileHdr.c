/* Module: mTileHdr.c

Version  Developer        Date     Change
-------  ---------------  -------  -----------------------
1.4      Daniel S. Katz   06Oct07  Padding was being added with the wrong sign
1.3      Daniel S. Katz   28Dec04  Closes output file before exit (Ticket 241)
1.2      John Good        02Nov04  Fixed range of optind check
1.1      Joe Jacob        12Oct04  Fixed several type declarations and 
                                   print format strings
1.0      John Good        15Apr04  Baseline code

*/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

#include "wcs.h"

#define MAXSTR  256

static struct WorldCoor *wcs;

FILE    *fout;
FILE    *fstatus;

int      naxis1, naxis2;
double   crpix1, crpix2;

extern char *optarg;
extern int optind, opterr;

extern int getopt(int argc, char *const *argv, const char *options);

int checkHdr(char *infile, int hdrflag, int hdu);


int debug;

void readTemplate(char *template);
int  writeHdr    (char *template);
int  printHeader (char *header);
int  stradd      (char *header, char *card);


/*************************************************************************/
/*                                                                       */
/*  mTileHdr                                                             */
/*                                                                       */
/*  Montage is a set of general reprojection / coordinate-transform /    */
/*  mosaicking programs.  Any number of input images can be merged into  */
/*  an output FITS file.  The attributes of the input are read from the  */
/*  input files; the attributes of the output are read a combination of  */
/*  the command line and a FITS header template file.                    */
/*                                                                       */
/*  This module takes a header template file and creates another         */
/*  which represents one of a regular set of tiles covering the          */
/*  original.  The user specifies the tile gridding and which tile       */
/*  is desired.                                                          */
/*                                                                       */
/*************************************************************************/

int main(int argc, char **argv)
{
   int      c;
   char     origtmpl[MAXSTR];
   char     newtmpl [MAXSTR];

   int      nx, ny, ix, iy;

   int      xtilesize, ytilesize;
   int      xpad, ypad;

   /************************************/
   /* Read the command-line parameters */
   /************************************/

   opterr    =  0;
   debug     =  0;
   fstatus   = stdout;

   while ((c = getopt(argc, argv, "ds:")) != EOF) 
   {
      switch(c)
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
	    fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Usage: %s [-d][-s statusfile] orig.hdr new.hdr nx ny ix iy [xpad [ypad]]\"]\n", 
	       argv[0]);
            exit(1);
            break;
      }
   }

   if (argc - optind < 6) 
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Usage: %s [-d][-s statusfile] orig.hdr new.hdr nx ny ix iy [xpad [ypad]]\"]\n", 
	 argv[0]);
      exit(1);
   }

   strcpy(origtmpl, argv[optind]);
   strcpy(newtmpl,  argv[optind + 1]);

   nx = atoi(argv[optind+2]);
   ny = atoi(argv[optind+3]);
   ix = atoi(argv[optind+4]);
   iy = atoi(argv[optind+5]);

   if(nx <= 0 || ny <= 0
   || ix <  0 || ix >  nx-1
   || iy <  0 || iy >  ny-1)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"(nx,ny) have to be positive integers and (ix,iy) have to be in the range (0..nx-1, 0..ny-1)\"]\n");
      exit(1);
   }

   xpad = 0;
   if (argc - optind > 6)
      xpad = atoi(argv[optind+6]);

   ypad = xpad;
   if (argc - optind > 7)
      ypad = atoi(argv[optind+7]);

   checkHdr(origtmpl, 1, 0);

   fout = fopen(newtmpl, "w+");

   if(fout == (FILE *)NULL)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Cannot open output template file %s\"]\n", 
	 newtmpl);
      fflush(stdout);

      exit(1);
   }

   if(debug)
   {
      printf("DEBUG> Command-line read.\n");
      fflush(stdout);
   }


   /**************************/
   /* Read the template file */
   /**************************/

   readTemplate(origtmpl);


   /*******************************************/
   /* Determine the new pixel size and offset */
   /*******************************************/

   if(debug)
   {
      printf("DEBUG> Old naxis1 = %lf\n", wcs->nxpix);
      printf("DEBUG> Old naxis2 = %lf\n", wcs->nypix);
      printf("DEBUG> Old crpix1 = %lf\n", wcs->xrefpix);
      printf("DEBUG> Old crpix2 = %lf\n", wcs->yrefpix);
      printf("\n");
      printf("DEBUG> nx         = %d\n", nx);
      printf("DEBUG> ny         = %d\n", ny);
      printf("DEBUG> ix         = %d\n", ix);
      printf("DEBUG> iy         = %d\n", iy);

      printf("DEBUG> xpad       = %d\n", xpad);
      printf("DEBUG> ypad       = %d\n", ypad);
      printf("\n");
   }

   xtilesize = wcs->nxpix / nx;
   ytilesize = wcs->nypix / ny;

   if(debug)
   {
      printf("DEBUG> xtilesize  = %d\n", xtilesize);
      printf("DEBUG> ytilesize  = %d\n", ytilesize);
      printf("\n");
   }

   crpix1 = wcs->xrefpix - ix * xtilesize + xpad;
   crpix2 = wcs->yrefpix - iy * ytilesize + ypad;


   if(debug)
   {
      printf("DEBUG> New crpix1 = %lf\n", crpix1);
      printf("DEBUG> New crpix2 = %lf\n", crpix2);
      printf("\n");
   }

   naxis1 = xtilesize + 2*xpad;
   naxis2 = ytilesize + 2*ypad;

   if(debug)
   {
      printf("DEBUG> New naxis1 = %d\n", naxis1);
      printf("DEBUG> New naxis2 = %d\n", naxis2);
      printf("\n");
   }


   /****************************************/
   /* Generate the final header,           */
   /* including a coordinate system change */
   /* and rotation term to compensate.     */
   /* This updates WCS as well             */
   /****************************************/

   writeHdr(origtmpl);
   fclose(fout);



   /****************/
   /* Final output */
   /****************/

   fprintf(fstatus, "[struct stat=\"OK\", naxis1=%d, naxis2=%d, crpix1=%-g, crpix2=%-g]\n",
      naxis1, naxis2, crpix1, crpix2);
   fflush(stdout);

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

void readTemplate(char *template)
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

   if(debug)
   {
      printf("DEBUG> Original image WCS initialized\n\n");
      fflush(stdout);
   }

   fclose(fp);

   return;
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



/*****************************/
/*                           */
/*  Write out the new header */
/*                           */
/*****************************/

int writeHdr(char *template)
{
   int       j;
   FILE     *fp;
   char      line[MAXSTR];

   fp = fopen(template, "r");

   if(fp == (FILE *)NULL)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Bad template: %s\"]\n", 
	 template);
      exit(1);
   }

   for(j=0; j<1000; ++j)
   {
      if(fgets(line, MAXSTR, fp) == (char *)NULL)
         break;

      if(line[strlen(line)-1] == '\n')
         line[strlen(line)-1]  = '\0';
      
      if(line[strlen(line)-1] == '\r')
	 line[strlen(line)-1]  = '\0';

           if(strncmp(line, "NAXIS1", 6) == 0)
	 fprintf(fout, "NAXIS1  = %20d\n", naxis1);
      else if(strncmp(line, "NAXIS2", 6) == 0)
	 fprintf(fout, "NAXIS2  = %20d\n", naxis2);
      else if(strncmp(line, "CRPIX1", 6) == 0)
	 fprintf(fout, "CRPIX1  = %20.10f\n", crpix1);
      else if(strncmp(line, "CRPIX2", 6) == 0)
	 fprintf(fout, "CRPIX2  = %20.10f\n", crpix2);
      else
	 fprintf(fout, "%s\n", line);
   }


   return 0;
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
