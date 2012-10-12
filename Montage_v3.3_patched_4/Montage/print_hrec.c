/* Module: print_hrec.c

Version  Developer        Date     Change
-------  ---------------  -------  -----------------------
1.0      John Good        31Jan05  Baseline code

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


/* Given WCS information (and optionally corners) */
/* for an image, incrementally write a record to  */
/* an output image metadata (ASCII) table         */

void print_rec (struct Hdr_rec *hdr_rec) 
{
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

	 fprintf(tblf, "      ra1    |     dec1    |      ra2    |     dec2    |      ra3    |     dec3    |      ra4    |     dec4    |");
	 fprintf(tblf, "  fname\n");

	 fprintf(tblf, "| int  |     double  |     double  |      char    |     char     | int  | int  |  char  |  char  |     double    |     double    |");
	 fprintf(tblf, "    double   |    double   |      double     |      double     |   double    | double |");

	 fprintf(tblf, "     double  |     double  |     double  |     double  |     double  |     double  |     double  |     double  |");
	 fprintf(tblf, "  char\n");
      }
      else
      {
	 fprintf(tblf, "\\datatype = fitshdr\n");

	 fprintf(tblf, "| cntr |      ra     |     dec     |      cra     |     cdec     |naxis1|naxis2| ctype1 | ctype2 |     crpix1    |     crpix2    |");
	 fprintf(tblf, "    crval1   |    crval2   |      cdelt1     |      cdelt2     |   crota2    |equinox |");

	 fprintf(tblf, "  fname\n");

	 fprintf(tblf, "| int  |    double   |    double   |      char    |    char      | int  | int  |  char  |  char  |     double    |     double    |");
	 fprintf(tblf, "    double   |    double   |      double     |      double     |   double    |  double|");

	 fprintf(tblf, "  char\n");
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

    fprintf(tblf, " %s\n",   hdr_rec->fname);
    fflush(tblf);

    ++cntr;
}
