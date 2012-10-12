/* Module: print_rec.c

Version  Developer        Date     Change
-------  ---------------  -------  -----------------------
1.7      John Good        12Jan06  Changed printf format for file size
1.6      John Good        14Jun04  Gave crpix, cdelt values more digits
1.5      John Good        09Jan04  Changed HDU numbering to start at 0
				   for consistency with other FITS tools.
				   Also changed ns,nl to naxis1,naxis2
				   and added cra,cdec.
1.4      John Good        15Sep03  Added a few comments for clarity
1.3      Anastasia Laity  03Sep03  Fixed formatting for additional
				   fields (moved space to BEFORE value
				   instead of after to match all the
				   other columns)
1.2      John Good        23Aug03  Fixed incorrect "type" header line 
                                   and added processing for "additional"
				   keywords
1.1      John Good        22Mar03  Fixed alignment bug in "no corners"
				   table header
1.0      John Good        29Jan03  Baseline code

*/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <string.h>
#include <coord.h>
#include "hdr_rec.h"

extern int   showCorners;
extern int   debug;
extern int   cntr;
extern FILE *tblf;

typedef struct
{
   char name  [128];
   char type  [128];
   char value [128];
   char defval[128];
   int  width;
}
FIELDS;

extern FIELDS *fields;
extern int     nfields;


/* Given WCS information (and optionally corners) */
/* for an image, incrementally write a record to  */
/* an output image metadata (ASCII) table         */

void print_rec (struct Hdr_rec *hdr_rec) 
{
    int  i;
    char fmt[32];

    struct COORD in, out;

    strcpy(in.sys,   "EQ");
    strcpy(in.fmt,   "DDR");
    strcpy(in.epoch, "J2000");

    strcpy(out.sys,   "EQ");
    strcpy(out.fmt,   "SEXC");
    strcpy(out.epoch, "J2000");

    if(cntr == 0)
    {
       if(showCorners)
       {
	 fprintf(tblf, "\\datatype = fitshdr\n");

	 fprintf(tblf, "| cntr |      ra     |     dec     |      cra     |     cdec     |naxis1|naxis2| ctype1 | ctype2 |     crpix1    |     crpix2    |");
	 fprintf(tblf, "    crval1   |    crval2   |      cdelt1     |      cdelt2     |   crota2    |equinox |");

	 for(i=0; i<nfields; ++i)
	 {
	    sprintf(fmt, "%%%ds|", fields[i].width);
	    fprintf(tblf, fmt, fields[i].name);
	 }

	 fprintf(tblf, "      ra1    |     dec1    |      ra2    |     dec2    |      ra3    |     dec3    |      ra4    |     dec4    |");
	 fprintf(tblf, "    size    | hdu  | fname\n");

	 fprintf(tblf, "| int  |     double  |     double  |      char    |     char     | int  | int  |  char  |  char  |     double    |     double    |");
	 fprintf(tblf, "    double   |    double   |      double     |      double     |   double    | double |");

	 for(i=0; i<nfields; ++i)
	 {
	    sprintf(fmt, "%%%ds|", fields[i].width);
	    fprintf(tblf, fmt, fields[i].type);
	 }

	 fprintf(tblf, "     double  |     double  |     double  |     double  |     double  |     double  |     double  |     double  |");
	 fprintf(tblf, "    int     | int  | char\n");
      }
      else
      {
	 fprintf(tblf, "\\datatype = fitshdr\n");

	 fprintf(tblf, "| cntr |      ra     |     dec     |      cra     |     cdec     |naxis1|naxis2| ctype1 | ctype2 |     crpix1    |     crpix2    |");
	 fprintf(tblf, "    crval1   |    crval2   |      cdelt1     |      cdelt2     |   crota2    |equinox |");

	 for(i=0; i<nfields; ++i)
	 {
	    sprintf(fmt, "%%%ds|", fields[i].width);
	    fprintf(tblf, fmt, fields[i].name);
	 }

	 fprintf(tblf, "    size    | hdu  | fname\n");

	 fprintf(tblf, "| int  |    double   |    double   |      char    |    char      | int  | int  |  char  |  char  |     double    |     double    |");
	 fprintf(tblf, "    double   |    double   |      double     |      double     |   double    |  double|");

	 for(i=0; i<nfields; ++i)
	 {
	    sprintf(fmt, "%%%ds|", fields[i].width);
	    fprintf(tblf, fmt, fields[i].type);
	 }

	 fprintf(tblf, "     int    | int  | char\n");
      }
    }

    in.lon = hdr_rec->ra2000;
    in.lat = hdr_rec->dec2000;

    ccalc(&in, &out, "t", "t");

    fprintf(tblf, " %6d",     hdr_rec->cntr);
    fprintf(tblf, " %13.7f",  hdr_rec->ra2000);
    fprintf(tblf, " %13.7f",  hdr_rec->dec2000);
    fprintf(tblf, " %13s",    out.clon);
    fprintf(tblf, " %13s",    out.clat);
    fprintf(tblf, " %6d",     hdr_rec->ns);
    fprintf(tblf, " %6d",     hdr_rec->nl);
    fprintf(tblf, " %8s",     hdr_rec->ctype1);
    fprintf(tblf, " %8s",     hdr_rec->ctype2);
    fprintf(tblf, " %15.5f",  hdr_rec->crpix1);
    fprintf(tblf, " %15.5f",  hdr_rec->crpix2);
    fprintf(tblf, " %13.7f",  hdr_rec->crval1);
    fprintf(tblf, " %13.7f",  hdr_rec->crval2);
    fprintf(tblf, " %17.10e", hdr_rec->cdelt1);
    fprintf(tblf, " %17.10e", hdr_rec->cdelt2);
    fprintf(tblf, " %13.7f",  hdr_rec->crota2);
    fprintf(tblf, " %8.2f",   hdr_rec->equinox);

    for(i=0; i<nfields; ++i)
    {
       sprintf(fmt, " %%%ds", fields[i].width);
       fprintf(tblf, fmt, fields[i].value);
    }

    if(showCorners)
    {
       fprintf(tblf, " %13.7f", hdr_rec->ra1);
       fprintf(tblf, " %13.7f", hdr_rec->dec1);
       fprintf(tblf, " %13.7f", hdr_rec->ra2);
       fprintf(tblf, " %13.7f", hdr_rec->dec2);
       fprintf(tblf, " %13.7f", hdr_rec->ra3);
       fprintf(tblf, " %13.7f", hdr_rec->dec3);
       fprintf(tblf, " %13.7f", hdr_rec->ra4);
       fprintf(tblf, " %13.7f", hdr_rec->dec4);
    }

    fprintf(tblf, " %12lld", hdr_rec->size);
    fprintf(tblf, " %6d",    hdr_rec->hdu-1);
    fprintf(tblf, " %s\n",   hdr_rec->fname);
    fflush(tblf);

    ++cntr;
}
