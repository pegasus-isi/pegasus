#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <math.h>
#include <mtbl.h>
#include <svc.h>

#define MAXSTR  4096
#define NPIX   23552

extern char *optarg;
extern int optind, opterr;

extern int getopt(int argc, char *const *argv, const char *options);

char *svc_value();

int   debug;
FILE *fdebug;


/*******************************************************************/
/*                                                                 */
/*  mArchiveExec                                                   */
/*                                                                 */
/*  Reads a listing archive images and calls mArchiveGet to get    */
/*  each one.                                                      */
/*                                                                 */
/*******************************************************************/

int main(int argc, char **argv)
{
   int    i, c, stat, ncols, count, failed;
   int    timeout, nread, nrestart, unzip, local2MASS;

   int    iurl;
   int    ifile;

   int    iimin, iimax, ijmin, ijmax;
   int    imin, imax, jmin, jmax;
   int    itmin, itmax, jtmin, jtmax;
   int    nx, ny, ix, jy;

   char  *ptr;
   char   url     [MAXSTR];
   char   urlbase [MAXSTR];
   char   file    [MAXSTR];
   char   filebase[MAXSTR];

   char   tblfile [MAXSTR];

   char   cmd     [MAXSTR];
   char   status  [32];


   /***************************************/
   /* Process the command-line parameters */
   /***************************************/

   debug      = 0;
   opterr     = 0;
   timeout    = 0;
   nrestart   = 0;
   unzip      = 0;
   local2MASS = 0;
   fdebug     = stdout;

   if(debug)
   {
      fprintf(fdebug, "DEBUGGING OUTPUT\n\n");
      fflush(fdebug);
   }

   while ((c = getopt(argc, argv, "d:r:t:uS")) != EOF)
   {
      switch (c)
      {
         case 'd':
            debug = atoi(optarg);
            break;

         case 't':
            timeout = atoi(optarg);
            break;

         case 'r':
            nrestart = atoi(optarg);
            break;

         case 'u':
            unzip = 1;
            break;

         case 'S':
            local2MASS = 1;
            break;

         default:
	    printf("[struct stat=\"ERROR\", msg=\"Usage: %s [-d level][-r startrec][-t timeout] region.tbl\"]\n", argv[0]);
            exit(0);
            break;
      }
   }

   if(argc - optind < 1)
   {
      printf("[struct stat=\"ERROR\", msg=\"Usage: %s [-d level][-r startrec][-t timeout] region.tbl\"]\n", argv[0]);
      exit(1);
   }

   strcpy(tblfile, argv[optind]);

   if(debug)
      svc_debug(fdebug);


   /***********************************/ 
   /* Open the region list table file */
   /***********************************/ 

   ncols = topen(tblfile);

   iurl = tcol( "URL");
   if(iurl < 0)
      iurl = tcol( "url");

   ifile = tcol( "fname");
   if(ifile < 0)
      ifile = tcol("file");

   iimin  = tcol( "imin");
   iimax  = tcol( "imax");
   ijmin  = tcol( "jmin");
   ijmax  = tcol( "jmax");

   if(iurl < 0)
   {
      printf("[struct stat=\"ERROR\", msg=\"Table %s needs column 'URL' or 'url' and can optionally have columns 'fname'/'file' and pixel ranges 'imin'..'jmax'\"]\n",
	 tblfile);
      exit(1);
   }


   /*****************************************/ 
   /* Read the records and call mArchiveGet */
   /*****************************************/ 

   count  = 0;
   failed = 0;
   nread  = 0;

   while(1)
   {
      stat = tread();

      ++nread;
      if(nread < nrestart)
	 continue;

      if(stat < 0)
	 break;

      strcpy(url, tval(iurl));

      if(ifile >= 0)
	 strcpy(file, tval(ifile));
      else
      {
	 if(debug > 1)
	 {
	    fprintf(fdebug, "DEBUG> url = [%s]\n", url);
	    fflush(fdebug);
	 }

	 ptr = url+strlen(url)-1;

	 while(1)
	 {
	    if(ptr == url || *ptr == '/')
	    {
	       strcpy(file, ptr+1);
	       break;
	    }

	    --ptr;
	 }
      }


      /* Special processing for DPOSS */
      /* (get the image in tiles)     */

      if(iimin >= 0
      && iimax >= 0
      && ijmin >= 0
      && ijmax >= 0)
      {
	 strcpy(filebase, file);

	 for(i=0; i<strlen(filebase); ++i)
	    if(filebase[i] == '.')
	       filebase[i] = '\0';

	 strcpy(urlbase, url);

	 for(i=0; i<strlen(urlbase); ++i)
	    if(urlbase[i] == '&')
	       urlbase[i] = '\0';

	 imin =    1;
	 imax = NPIX;
	 jmin =    1;
	 jmax = NPIX;

	 imin = atoi(tval(iimin));
	 imax = atoi(tval(iimax));
	 jmin = atoi(tval(ijmin));
	 jmax = atoi(tval(ijmax));

	 nx = NPIX / 500;
	 ny = NPIX / 500;

	 for(ix=3; ix<nx-3; ++ix)
	 {
	    for(jy=3; jy<nx-3; ++jy)
	    {
	       itmin = ix * 500 - 50;
	       jtmin = jy * 500 - 50;

	       itmax = (ix+1) * 500 + 50;
	       jtmax = (jy+1) * 500 + 50;

	       if(itmax < imin) continue;
	       if(itmin > imax) continue;
	       if(jtmax < jmin) continue;
	       if(jtmin > jmax) continue;

	       if(timeout > 0)
	       {
		  sprintf(cmd, "mArchiveGet -r -t %d %s&X1=%d&X2=%d&Y1=%d&Y2=%d %s_%d_%d.fits",
		     timeout,
		     urlbase,
		     itmin, itmax - itmin + 1,
		     jtmin, jtmax - jtmin + 1,
		     filebase, ix, jy);
	       }
	       else
	       {
		  sprintf(cmd, "mArchiveGet -r %s&X1=%d&X2=%d&Y1=%d&Y2=%d %s_%d_%d.fits",
		     urlbase,
		     itmin, itmax - itmin + 1,
		     jtmin, jtmax - jtmin + 1,
		     filebase, ix, jy);
	       }

	       if(debug)
	       {
		  fprintf(fdebug, "DEBUG> [%s]\n", cmd);
		  fflush(fdebug);
	       }

	       svc_run(cmd);

	       strcpy( status, svc_value( "stat" ));

	       ++count;

	       if(strcmp( status, "ERROR") == 0)
	       {
		  ++failed;
		  continue;
	       }
	    }
	 }
      }


      /* Special processing for IRSA access to 2MASS */
      /* where the files are locally cross-mounted   */

      else if(local2MASS)
      {
	 sprintf(cmd, "ln -s /stage%s %s", url+73, file);

	 if(debug)
	 {
	    fprintf(fdebug, "DEBUG> [%s]\n", cmd);
	    fflush(fdebug);
	 }

	 system(cmd);

	 ++count;
      }


      /* Normal URL-based retrieval */

      else
      {
	 if(timeout > 0)
	 {
	    sprintf(cmd, "mArchiveGet -t %d %s %s",
	       timeout, url, file);
	 }
	 else
	 {
	    sprintf(cmd, "mArchiveGet %s %s",
	       url, file);
	 }

	 if(debug)
	 {
	    fprintf(fdebug, "DEBUG> [%s]\n", cmd);
	    fflush(fdebug);
	 }

	 svc_run(cmd);

	 strcpy( status, svc_value( "stat" ));

	 ++count;

	 if(strcmp( status, "ERROR") == 0)
	 {
	    ++failed;
	    continue;
	 }

	 if(unzip && strlen(file) > 3 && strcmp(file+strlen(file)-3, ".gz") == 0)
	 {
	    sprintf(cmd, "gunzip %s", file);
	    system(cmd);
	 }
      }
   }

   printf("[struct stat=\"OK\", count=%d, failed=%d]\n", count, failed);
   fflush(stdout);

   exit(0);
}
