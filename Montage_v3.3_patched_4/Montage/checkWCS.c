/* Module: checkWCS.c

Version  Developer        Date     Change
-------  ---------------  -------  -----------------------
1.7      John Good        24Feb06  The CD matrix constraints were too strict
1.6      John Good        02Dec03  Change naxes to naxis to match
				   change ins WCS library.
1.5      John Good        25Aug03  Implement status file output
1.4      John Good        15Apr03  Allow for LON,LAT transpose
1.3      John Good        19Mar03  Renamed file / function from wcsCheck
				   to checkWCS for consistency
1.2      John Good        19Mar03  Modified bad WCS error message  
1.1      John Good        18Mar03  Corrected error with action flag
1.0      John Good        13Mar03  Baseline code

*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "montage.h"
#include "wcs.h"

#define  ERROR_OFF   0
#define  RETURN_CODE 1

int wcs_debug = 0;


/*************************************************************************/
/*                                                                       */
/*  checkWCS                                                             */
/*                                                                       */
/*  This routine check a WCS structure to determine if the information   */
/*  contained is internally consistent.                                  */
/*                                                                       */
/*************************************************************************/

int checkWCS(struct WorldCoor *wcs, int action)
{
   int i;

   if(wcs == (struct WorldCoor *)NULL)
   {
      if(action == ERROR_OFF)
      {
	 fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"No WCS information (or not FITS header)\"]\n");
	 exit(1);
      }
      else
      {
	 return(1);
      }
   }

   if(wcs_debug)
   {
      printf("prjcode    = %d\n", wcs->prjcode);
      printf("ptype      = [%s]\n", wcs->ptype);
      printf("nxpix      = %-g\n", wcs->nxpix);
      printf("nypix      = %-g\n", wcs->nypix);
      printf("c1type     = [%s]\n", wcs->c1type);
      printf("c2type     = [%s]\n", wcs->c2type);
      printf("naxis      = %d\n", wcs->naxis);

      for(i=0; i<wcs->naxis; ++i)
	printf("crval[%d]   = %-g\n", i, wcs->crval[i]);

      printf("xref       = %-g\n", wcs->xref);
      printf("yref       = %-g\n", wcs->yref);

      for(i=0; i<wcs->naxis; ++i)
	printf("crpix[%d]   = %-g\n", i, wcs->crpix[i]);

      printf("xrefpix    = %-g\n", wcs->xrefpix);
      printf("yrefpix    = %-g\n", wcs->yrefpix);

      if(wcs->rotmat)
      {
	 for(i=0; i<4; ++i)
	   printf("cd[%d]      = %-g\n", i, wcs->cd[i]);
      }
      else
      {
	 for(i=0; i<wcs->naxis; ++i)
	   printf("cdelt[%d]   = %-g\n", i, wcs->cdelt[i]);
      }

      printf("xinc       = %-g\n", wcs->xinc);
      printf("yinc       = %-g\n", wcs->yinc);
      printf("rot        = %-g\n", wcs->rot);

      printf("equinox    = %-g\n", wcs->equinox);
      printf("epoch      = %-g\n", wcs->epoch);

      for(i=0; i<16; ++i)
	printf("pc[%2d]     = %-g\n", i, wcs->pc[i]);

      for(i=0; i<10; ++i)
	printf("projp[%2d]  = %-g\n", i, wcs->projp[i]);

      printf("longpole   = %-g\n", wcs->longpole);
      printf("latpole    = %-g\n", wcs->latpole);
   }


   /* Check NAXIS */

   if(wcs->naxis < 2)
   {
      if(action == ERROR_OFF)
      {
	 fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Must have at least two dimensions\"]\n");
	 exit(1);
      }
      else
	 {
	    return(1);
	 }
   }


   /* Check the projection */

   if(wcs->prjcode <= 0)
   {
      if(action == ERROR_OFF)
      {
	 fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Invalid projection\"]\n");
	 exit(1);
      }
      else
	 {
	    return(1);
	 }
   }


   /* Check NAXIS1 and NAXIS2 */

   if(wcs->nxpix <= 0)
   {
      if(action == ERROR_OFF)
      {
	 fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Invalid NAXIS1\"]\n");
	 exit(1);
      }
      else
	 {
	    return(1);
	 }
   }

   if(wcs->nypix <= 0)
   {
      if(action == ERROR_OFF)
      {
	 fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Invalid NAXIS2\"]\n");
	 exit(1);
      }
      else
	 {
	    return(1);
	 }
   }


   /* Check the coordinate system (from CTYPE1 and CTYPE2) */

   if(strcmp(wcs->c1type, "RA") == 0)
   {
      if(strcmp(wcs->c2type, "DEC") != 0)
      {
	 if(action == ERROR_OFF)
	 {
	    fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"CTYPE1 and CTYPE2 don't match\"]\n");
	    exit(1);
	 }
	 else
	 {
	    return(1);
	 }
      }
   }
   else if(strcmp(wcs->c1type, "DEC") == 0)
   {
      if(strcmp(wcs->c2type, "RA") != 0)
      {
	 if(action == ERROR_OFF)
	 {
	    fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"CTYPE1 and CTYPE2 don't match\"]\n");
	    exit(1);
	 }
	 else
	 {
	    return(1);
	 }
      }
   }
   else if(strcmp(wcs->c1type, "GLON") == 0)
   {
      if(strcmp(wcs->c2type, "GLAT") != 0)
      {
	 if(action == ERROR_OFF)
	 {
	    fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"CTYPE1 and CTYPE2 don't match\"]\n");
	    exit(1);
	 }
	 else
	 {
	    return(1);
	 }
      }
   }
   else if(strcmp(wcs->c1type, "GLAT") == 0)
   {
      if(strcmp(wcs->c2type, "GLON") != 0)
      {
	 if(action == ERROR_OFF)
	 {
	    fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"CTYPE1 and CTYPE2 don't match\"]\n");
	    exit(1);
	 }
	 else
	 {
	    return(1);
	 }
      }
   }
   else if(strcmp(wcs->c1type, "ELON") == 0)
   {
      if(strcmp(wcs->c2type, "ELAT") != 0)
      {
	 if(action == ERROR_OFF)
	 {
	    fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"CTYPE1 and CTYPE2 don't match\"]\n");
	    exit(1);
	 }
	 else
	 {
	    return(1);
	 }
      }
   }
   else if(strcmp(wcs->c1type, "ELAT") == 0)
   {
      if(strcmp(wcs->c2type, "ELON") != 0)
      {
	 if(action == ERROR_OFF)
	 {
	    fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"CTYPE1 and CTYPE2 don't match\"]\n");
	    exit(1);
	 }
	 else
	 {
	    return(1);
	 }
      }
   }
   else
   {
      if(action == ERROR_OFF)
      {
	 fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Invalid CTYPE1\"]\n");
	 exit(1);
      }
      else
	 {
	    return(1);
	 }
   }


   /* Check the CD matrix or CDELT1, CDELT2 */

   if(wcs->rotmat)
   {
      if((wcs->cd[0] == 0. && wcs->cd[1] == 0.)
      || (wcs->cd[2] == 0. && wcs->cd[3] == 0.))
      {
	 if(action == ERROR_OFF)
	 {
	    fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Invalid CD matrix\"]\n");
	    exit(1);
	 }
	 else
	 {
	    return(1);
	 }
      }
   }
   else
   {
      if(wcs->xinc == 0.)
      {
	 if(action == ERROR_OFF)
	 {
	    fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Invalid CDELT1\"]\n");
	    exit(1);
	 }
	 else
	 {
	    return(1);
	 }
      }

      if(wcs->yinc == 0.)
      {
	 if(action == ERROR_OFF)
	 {
	    fprintf(fstatus, "[struct stat=\"ERROR\", msg=\"Invalid CDELT2\"]\n");
	    exit(1);
	 }
	 else
	 {
	    return(1);
	 }
      }
   }


   return 0;
}
