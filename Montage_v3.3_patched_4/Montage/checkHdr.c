/* Module: checkHdr.c

Version  Developer        Date     Change
-------  ---------------  -------  -----------------------
2.3      John Good        07Oct07  Add explicit hdrflag=2 check 
				   (could be either FITS or HDR)
2.2      John Good        25Sep07  Added getWCS() call to return WCS structure pointer
2.1      John Good        28Apr05  Allowed realloc of mHeader string.
2.0      John Good        27Mar05  Added HDU support.
1.10     John Good        21Sep04  That last update was a mistake.  mHeader is
				   sometimes needed externally (e.g. mCoverageCheck)
1.9      John Good        21Sep04  Free mHeader space when no longer needed.
1.8      John Good        11Nov03  Added file existence check if fits
				   open fails.
1.7      John Good        25Aug03  Added status file processing
1.6      John Good        24May03  Added getHdr() call to return header
				   "string". 
1.5.1    A. C. Laity      30Jun03  Added code documentation for fitsCheck
				   and strAdd
1.5      John Good        29Apr03  Added check for failure when file is
				   FITS format but fails FITS open
1.4      John Good        15Apr03  Added special check for DSS headers
                                   (partial; just look for PLTRAH)
1.3      John Good        08Apr03  Also remove <CR> from template lines
1.2      John Good        22Mar03  Renamed wcsCheck to checkWCS for
				   consistency
1.1      John Good        19Mar03  Added limit on template file size
				   (wcsinit() was choking).
1.0      John Good        13Mar03  Baseline code

*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include "montage.h"
#include "fitsio.h"
#include "wcs.h"

#define MAXHDR 80000

#define FITS   0
#define HDR    1
#define EITHER 2

int havePLTRAH;

int haveSIMPLE;
int haveBITPIX;
int haveNAXIS;
int haveNAXIS1;
int haveNAXIS2;
int haveCTYPE1;
int haveCTYPE2;
int haveCRPIX1;
int haveCRPIX2;
int haveCRVAL1;
int haveCRVAL2;
int haveCDELT1;
int haveCDELT2;
int haveCD1_1;
int haveCD1_2;
int haveCD2_1;
int haveCD2_2;
int haveBSCALE;
int haveBZERO;
int haveBLANK;
int haveEPOCH;
int haveEQUINOX;

char ctype1[1024];
char ctype2[1024];

char *hdrCheck_outfile = (char *)NULL;
FILE *fout;

int CHdebug    = 0;
int errorCount = 0;

static char *mHeader = (char *)NULL;

static struct WorldCoor *hdrCheck_wcs = (struct WorldCoor *)NULL;

int fitsCheck  (char *keyword, char *value);
int strAdd     (char *header, char *card);
int FITSerror  (int status);
int checkWCS   (struct WorldCoor *wcs, int action);
int errorOutput(char *msg);

struct WorldCoor *getWCS();
char             *getHdr();


static int hdrStringent = 0;

int checkHdrExact(int stringent)
{
   hdrStringent = stringent;
}


/***********************************************/
/*                                             */
/*  checkHdr                                   */
/*                                             */
/*  Routine for checking header template files */
/*                                             */
/*  The hdrflag argument is used as follows:   */
/*                                             */
/*   0 - Assume we are checking a FITS file    */
/*   1 - Assume we are checking a hdr file     */
/*   2 - Could be either; check both ways      */
/*                                             */
/***********************************************/

int checkHdr(char *infile, int hdrflag, int hdu)
{
   int       i, len, ncard, morekeys;

   int       status = 0;

   char     *keyword;
   char     *value;

   char      fitskeyword[80];
   char      fitsvalue  [80];
   char      fitscomment[80];
   char      tmpstr     [80];

   char     *end;

   char      line  [1024];
   char      pline [1024];

   char     *ptr1;
   char     *ptr2;

   FILE     *fp;
   fitsfile *infptr;

   static int maxhdr;

   if(!mHeader)
   {
      mHeader = malloc(MAXHDR);
      maxhdr = MAXHDR;
   }

   havePLTRAH  = 0;

   haveSIMPLE  = 0;
   haveBITPIX  = 0;
   haveNAXIS   = 0;
   haveNAXIS1  = 0;
   haveNAXIS2  = 0;
   haveCTYPE1  = 0;
   haveCTYPE2  = 0;
   haveCRPIX1  = 0;
   haveCRPIX2  = 0;
   haveCDELT1  = 0;
   haveCDELT2  = 0;
   haveCD1_1   = 0;
   haveCD1_2   = 0;
   haveCD2_1   = 0;
   haveCD2_2   = 0;
   haveCRVAL1  = 0;
   haveCRVAL2  = 0;
   haveBSCALE  = 0;
   haveBZERO   = 0;
   haveBLANK   = 0;
   haveEPOCH   = 0;
   haveEQUINOX = 0;


   /****************************************/
   /* Initialize the WCS transform library */
   /* and find the pixel location of the   */
   /* sky coordinate specified             */
   /****************************************/

   errorCount = 0;

   if(hdrCheck_outfile)
   {
      fout = fopen(hdrCheck_outfile, "w+");

      if(fout == (FILE *)NULL)
      {
	 fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Cannot open output file %s.\"]\n", hdrCheck_outfile);
	 fflush(fstatus);
	 exit(1);
      }
   }

   strcpy(mHeader, "");

   if(fits_open_file(&infptr, infile, READONLY, &status) == 0)
   {
      if(CHdebug)
      {
	 printf("\nFITS file\n");
	 fflush(stdout);
      }

      if(hdrflag == HDR)
      {
       fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"FITS file (%s) cannot be used as a header template\"]\n",
		infile);
	 fflush(fstatus);
	 exit(1);
      }

      if(hdu > 0)
      {
         if(fits_movabs_hdu(infptr, hdu+1, NULL, &status))
            FITSerror(status);
      }

      if(fits_get_hdrspace (infptr, &ncard, &morekeys, &status))
	 FITSerror(status);
      
      if(ncard > 1000)
	 mHeader = realloc(mHeader, ncard * 80 + 1024);

      if(CHdebug)
      {
	 printf("ncard = %d\n", ncard);
	 fflush(stdout);
      }

      for (i=1; i<=ncard; i++)
      {
	 if(fits_read_keyn (infptr, i, fitskeyword, fitsvalue, fitscomment, &status))
	    FITSerror(status);

	 if(fitsvalue[0] == '\'')
	 {
	    strcpy(tmpstr, fitsvalue+1);

	    if(tmpstr[strlen(tmpstr)-1] == '\'')
	       tmpstr[strlen(tmpstr)-1] =  '\0';
	 }
	 else
	    strcpy(tmpstr, fitsvalue);

         fitsCheck(fitskeyword, tmpstr);

	 sprintf(line, "%-8s= %20s", fitskeyword, fitsvalue);

	 if(strncmp(line, "COMMENT", 7) != 0)
	    strAdd(mHeader, line);
      }

      strAdd(mHeader, "END");

      if(fits_close_file(infptr, &status))
         FITSerror(status);
   }
   else
   {
      if(CHdebug)
      {
	 printf("\nTemplate file\n");
	 fflush(stdout);
      }

      if(hdrflag == FITS)
      {
	 fp = fopen(infile, "r");

	 if(fp == (FILE *)NULL)
	 {
	    fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"File %s not found.\"]\n", infile);
	    fflush(fstatus);
	    exit(1);
	 }

	 fclose(fp);

	 fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"File (%s) is not a FITS image\"]\n",
		infile);
	 fflush(fstatus);
	 exit(1);
      }

      fp = fopen(infile, "r");

      if(fp == (FILE *)NULL)
      {
         fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"File %s not found.\"]\n", infile);
         fflush(fstatus);
         exit(1);
      }

      while(1)
      {
         if(fgets(line, 1024, fp) == (char *)NULL)
            break;

         if(line[(int)strlen(line)-1] == '\n')
            line[(int)strlen(line)-1]  = '\0';
	 
         if(line[(int)strlen(line)-1] == '\r')
            line[(int)strlen(line)-1]  = '\0';
	 
	 strcpy(pline, line);

	 if((int)strlen(line) > 80)
	 {
	    fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"FITS header lines cannot be greater than 80 characters.\"]\n");
	    fflush(fstatus);
	    exit(1);
	 }

	 len = (int)strlen(pline);

	 keyword = pline;

	 while(*keyword == ' ' && keyword < pline+len)
	    ++keyword;

	 end = keyword;

	 while(*end != ' ' && *end != '=' && end < pline+len)
	    ++end;

	 value = end;

	 while((*value == '=' || *value == ' ' || *value == '\'')
	       && value < pline+len)
	    ++value;

	 *end = '\0';
	 end = value;

	 if(*end == '\'')
	    ++end;

	 while(*end != ' ' && *end != '\'' && end < pline+len)
	    ++end;

	 *end = '\0';

	 fitsCheck(keyword, value);

	 strAdd(mHeader, line);
	 
	 if((int)strlen(mHeader) + 160 > maxhdr)
	 {
	    maxhdr += MAXHDR;
	    mHeader = realloc(mHeader, maxhdr);
	 }
      }

      fclose(fp);
   }


   /********************************************************/
   /*                                                      */
   /* Check to see if we have the minimum FITS header info */
   /*                                                      */
   /********************************************************/

   if(!haveBITPIX)
      errorOutput("No BITPIX keyword in FITS header\"]\n");

   if(!haveNAXIS)
      errorOutput("No NAXIS keyword in FITS header\"]\n");

   if(!haveNAXIS1)
      errorOutput("No NAXIS1 keyword in FITS header\"]\n");

   if(!haveNAXIS2)
      errorOutput("No NAXIS2 keyword in FITS header\"]\n");

   if(havePLTRAH)
   {
      /* If we have this parameter, we'll assume this is a DSS header  */
      /* the WCS checking routine should be able to verify if it isn't */

      free(mHeader);

      maxhdr = 0;
      
      mHeader = (char *)NULL;

      return(0);
   }

   if(!haveCTYPE1)
      errorOutput("No CTYPE1 keyword in FITS header\"]\n");

   if(!haveCTYPE2)
      errorOutput("No CTYPE2 keyword in FITS header\"]\n");

   if(!haveCRPIX1)
      errorOutput("No CRPIX1 keyword in FITS header\"]\n");

   if(!haveCRPIX2)
      errorOutput("No CRPIX2 keyword in FITS header\"]\n");

   if(!haveCRVAL1)
      errorOutput("No CRVAL1 keyword in FITS header\"]\n");

   if(!haveCRVAL2)
      errorOutput("No CRVAL2 keyword in FITS header\"]\n");

   if(!haveCD1_1 
   && !haveCD1_2 
   && !haveCD2_1 
   && !haveCD2_2)
   {
      if(!haveCDELT1)
	 errorOutput("No CDELT1 keyword (or incomplete CD matrix) in FITS header\"]\n");
      else if(!haveCDELT2)
	 errorOutput("No CDELT2 keyword (or incomplete CD matrix) in FITS header\"]\n");
   }

   if(strlen(ctype1) < 8)
      errorOutput("CTYPE1 must be at least 8 characters");

   if(strlen(ctype2) < 8)
      errorOutput("CTYPE2 must be at least 8 characters");

   ptr1 = ctype1;

   while(*ptr1 != '-' && *ptr1 != '\0') ++ptr1;
   while(*ptr1 == '-' && *ptr1 != '\0') ++ptr1;

   ptr2 = ctype2;

   while(*ptr2 != '-' && *ptr2 != '\0') ++ptr2;
   while(*ptr2 == '-' && *ptr2 != '\0') ++ptr2;

   if(strlen(ptr1) == 0
   || strlen(ptr2) == 0)
      errorOutput("Invalid CTYPE1 or CTYPE2 projection information");

   if(strcmp(ptr1, ptr2) != 0)
      errorOutput("CTYPE1, CTYPE2 projection information mismatch");

   if(hdrStringent)
   {
      if(strlen(ptr1) != 3)
	 errorOutput("Invalid CTYPE1 projection information");

      if(strlen(ptr2) != 3)
	 errorOutput("Invalid CTYPE2 projection information");
   }


   /****************************************/
   /* Initialize the WCS transform library */
   /* and find the pixel location of the   */
   /* sky coordinate specified             */
   /****************************************/

   /*
   if(CHdebug)
   {
      printf("header = \n%s\n", mHeader);
      fflush(stdout);
   }
   */

   hdrCheck_wcs = wcsinit(mHeader);

   checkWCS(hdrCheck_wcs, 0);

   if(errorCount > 0)
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"%d Errors\"]\n", 
	 errorCount);
      fflush(fstatus);
      exit(1);
   }

   return(0);
}



char *getHdr()
{
   return mHeader;
}



struct WorldCoor *getWCS()
{
   return hdrCheck_wcs;
}



/* fitsCheck checks the value of a given keyword to make */
/* sure it is a valid entry in the FITS header.          */

int fitsCheck(char *keyword, char *value)
{
   char  *end;
   int    ival;
   double dval;

   char   msg[1024];

   if(CHdebug)
   {
      printf("fitsCheck() [%s] = [%s]\n", keyword, value);
      fflush(stdout);
   }

   if(strcmp(keyword, "SIMPLE") == 0)
   {
      haveSIMPLE = 1;

      if(strcmp(value, "T") != 0
      && strcmp(value, "F") != 0)
	 errorOutput("SIMPLE keyword must be T or F");
   }

   else if(strcmp(keyword, "BITPIX") == 0)
   {
      haveBITPIX = 1;

      ival = strtol(value, &end, 0);

      if(end < value + (int)strlen(value))
	 errorOutput("BITPIX keyword in FITS header not an integer");

      if(ival != 8
      && ival != 16
      && ival != 32
      && ival != 64
      && ival != -32
      && ival != -64)
	 errorOutput("Invalid BITPIX in FITS header (must be 8,16,32,64,-32 or -64)");
   }

   else if(strcmp(keyword, "NAXIS") == 0)
   {
      haveNAXIS = 1;

      ival = strtol(value, &end, 0);

      if(end < value + (int)strlen(value))
	 errorOutput("NAXIS keyword in FITS header not an integer");

      if(ival < 2)
	 errorOutput("NAXIS keyword in FITS header must be >= 2");
   }

   else if(strcmp(keyword, "NAXIS1") == 0)
   {
      haveNAXIS1 = 1;

      ival = strtol(value, &end, 0);

      if(end < value + (int)strlen(value))
	 errorOutput("NAXIS1 keyword in FITS header not an integer");

      if(ival < 0)
	 errorOutput("NAXIS1 keyword in FITS header must be > 0");
   }

   else if(strcmp(keyword, "NAXIS2") == 0)
   {
      haveNAXIS2 = 1;

      ival = strtol(value, &end, 0);

      if(end < value + (int)strlen(value))
	 errorOutput("NAXIS2 keyword in FITS header not an integer");

      if(ival < 0)
	 errorOutput("NAXIS2 keyword in FITS header must be > 0");
   }

   else if(strcmp(keyword, "PLTRAH") == 0)
      havePLTRAH = 1;

   else if(strcmp(keyword, "CTYPE1") == 0)
   {
      haveCTYPE1 = 1;
      strcpy(ctype1, value);
   }

   else if(strcmp(keyword, "CTYPE2") == 0)
   {
      haveCTYPE2 = 1;
      strcpy(ctype2, value);
   }

   else if(strcmp(keyword, "CRPIX1") == 0)
   {
      haveCRPIX1 = 1;

      dval = strtod(value, &end);

      if(end < value + (int)strlen(value))
	 errorOutput("CRPIX1 keyword in FITS header not a real number");
   }

   else if(strcmp(keyword, "CRPIX2") == 0)
   {
      haveCRPIX2 = 1;

      dval = strtod(value, &end);

      if(end < value + (int)strlen(value))
	 errorOutput("CRPIX2 keyword in FITS header not a real number");
   }

   else if(strcmp(keyword, "CRVAL1") == 0)
   {
      haveCRVAL1 = 1;

      dval = strtod(value, &end);

      if(end < value + (int)strlen(value))
	 errorOutput("CRVAL1 keyword in FITS header not a real number");
   }

   else if(strcmp(keyword, "CRVAL2") == 0)
   {
      haveCRVAL2 = 1;

      dval = strtod(value, &end);

      if(end < value + (int)strlen(value))
	 errorOutput("CRVAL2 keyword in FITS header not a real number");
   }

   else if(strcmp(keyword, "CDELT1") == 0)
   {
      haveCDELT1 = 1;

      dval = strtod(value, &end);

      if(end < value + (int)strlen(value))
	 errorOutput("CDELT1 keyword in FITS header not a real number");
   }

   else if(strcmp(keyword, "CDELT2") == 0)
   {
      haveCDELT2 = 1;

      dval = strtod(value, &end);

      if(end < value + (int)strlen(value))
	 errorOutput("CDELT2 keyword in FITS header not a real number");
   }

   else if(strcmp(keyword, "CROTA2") == 0)
   {
      dval = strtod(value, &end);

      if(end < value + (int)strlen(value))
	 errorOutput("CROTA2 keyword in FITS header not a real number");
   }

   else if(strcmp(keyword, "CD1_1") == 0)
   {
      haveCD1_1 = 1;

      dval = strtod(value, &end);

      if(end < value + (int)strlen(value))
	 errorOutput("CD1_1 keyword in FITS header not a real number");
   }

   else if(strcmp(keyword, "CD1_2") == 0)
   {
      haveCD1_2 = 1;

      dval = strtod(value, &end);

      if(end < value + (int)strlen(value))
	 errorOutput("CD1_2 keyword in FITS header not a real number");
   }

   else if(strcmp(keyword, "CD2_1") == 0)
   {
      haveCD2_1 = 1;

      dval = strtod(value, &end);

      if(end < value + (int)strlen(value))
	 errorOutput("CD1_2 keyword in FITS header not a real number");
   }

   else if(strcmp(keyword, "CD2_2") == 0)
   {
      haveCD2_2 = 1;

      dval = strtod(value, &end);

      if(end < value + (int)strlen(value))
	 errorOutput("CD2_2 keyword in FITS header not a real number");
   }

   else if(strcmp(keyword, "BSCALE") == 0)
   {
      dval = strtod(value, &end);

      if(end < value + (int)strlen(value))
	 errorOutput("BSCALE keyword in FITS header not a real number");
   }

   else if(strcmp(keyword, "BZERO") == 0)
   {
      dval = strtod(value, &end);

      if(end < value + (int)strlen(value))
	 errorOutput("BZERO keyword in FITS header not a real number");
   }

   else if(strcmp(keyword, "BLANK") == 0)
   {
      dval = strtod(value, &end);

      if(end < value + (int)strlen(value))
	 errorOutput("BLANK keyword in FITS header not a real number");
   }

   else if(strcmp(keyword, "EPOCH") == 0)
   {
      dval = strtod(value, &end);

      if(end < value + (int)strlen(value))
	 errorOutput("EPOCH keyword in FITS header not a real number");
   }

   else if(strcmp(keyword, "EQUINOX") == 0)
   {
      dval = strtod(value, &end);

      if(end < value + (int)strlen(value))
      {
	 if(dval < 1900. || dval > 2050.)
	    errorOutput("EQUINOX keyword in FITS header not a real number");
      }
   }
   
   return 0;
}


/* Output error to report file */
/* or stop after first error   */
/* if there is no such file    */

int errorOutput(char *msg)
{
   if(hdrCheck_outfile)
   {
      fprintf(fout, "%s\n", msg);
      fflush(fout);

      ++errorCount;
   }
   else
   {
      fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"%s\"]\n", msg);
      fflush(fstatus);
      exit(1);
   }

   return(0);
}


/* Adds the string "card" to a header line, and */
/* pads the header out to 80 characters.        */

int strAdd(char *header, char *card)
{
   int i;

   int hlen = (int)strlen(header);
   int clen = (int)strlen(card);

   for(i=0; i<clen; ++i)
      header[hlen+i] = card[i];

   if(clen < 80)
      for(i=clen; i<80; ++i)
         header[hlen+i] = ' ';

   header[hlen+80] = '\0';

   return((int)strlen(header));
}



/***********************************/
/*                                 */
/*  Print out FITS library errors  */
/*                                 */
/***********************************/

int FITSerror(int status)
{
   char status_str[FLEN_STATUS];

   fits_get_errstatus(status, status_str);

   fprintf(fstatus, "[struct stat=\"ERROR\", status=%d, msg=\"%s\"]\n", status, status_str);

   exit(1);
}
