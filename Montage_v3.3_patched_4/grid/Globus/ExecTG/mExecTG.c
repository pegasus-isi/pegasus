#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>
#include <math.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <fcntl.h>

#include <fitsio.h>
#include <mtbl.h>
#include <svc.h>
#include <wcs.h>
#include <coord.h>

#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <sys/uio.h>

#include <math.h>
#include <time.h>

#define MAXLEN   1024
#define BUFSIZE 32769
#define MAXHDR  80000
#define MAXSOCK  4096

#define INTRINSIC 0
#define COMPUTED  1
#define FAILED    2

extern char *optarg;
extern int optind, opterr;

extern int getopt(int argc, char *const *argv, const char *options);

int   debugCheck (char *debugStr);
char *mktemp     (char *template);
int   strncasecmp(const char *s1, const char *s2, size_t n);
char *url_encode ();
char *svc_value  ();

static time_t currtime, start;

int debug;

char msg    [MAXLEN];
char logaddr[MAXLEN];

FILE  *fmsg;

int FITSerror(char *fname, int status);
int stradd   (char *header, char *card);
int printerr (char *str);


/*************************************************************************/
/*                                                                       */
/*                                                                       */
/*  MEXEC  --  Mosaicking executive for 2MASS, SDSS, DSS.                */
/*             Includes remote data and metadata access.                 */
/*                                                                       */
/*                                                                       */
/*                                                                       */
/*  Positional parameters:                                               */
/*                                                                       */
/*  ------------    ----------------------------------------------       */
/*   Parameter        Description                                        */
/*  ------------    ----------------------------------------------       */
/*                                                                       */
/*   survey          2MASS, SDSS, or DSS                                 */
/*   band            Depends on survey. e.g: J, H, K for 2MASS           */
/*   template.hdr    FITS header template for mosaic                     */
/*  [workspace]      Directory where we can create working stuff.        */
/*                   Best if it is empty.                                */
/*                                                                       */
/*                                                                       */
/*  If no workspace is given, a unique local subdirectory will be        */
/*  created (e.g.; ./MOSAIC_AAAaa17v).  If you are going to use this     */
/*  it is best to run the program where there is space for all the       */
/*  intermediate files and you should also consider using the            */
/*  -o and -s options.                                                   */
/*                                                                       */
/*                                                                       */
/*                                                                       */
/*  Additional controls:                                                 */
/*                                                                       */
/*  ------------    ------------------------   ------------------------  */
/*   Flag             Default                   Description              */
/*  ------------    ------------------------   ------------------------  */
/*                                                                       */
/*  -l               0 (false)                 Background matching       */
/*                                              adjusts levels only      */
/*  -k               0 (false)                 Keep all working files    */
/*  -c               0 (false)                 Delete everything         */
/*                                              (use with savefile)      */
/*  -o savefile      none                      Location to save          */
/*                                              mosaic                   */
/*  -d level         0 (none)                  Debugging output          */
/*                                                                       */
/*                                                                       */
/*                                                                       */
/*  So a minimal call would look like:                                   */
/*                                                                       */
/*                                                                       */
/*           mExec 2MASS J region.hdr                                    */
/*                                                                       */
/*                                                                       */
/*     This will produce a half-degree square mosaic of the              */
/*     2MASS J band data in a unique subdirectory                        */
/*     called mosaic.fits .  All other files will be                     */
/*     cleaned up when the processing is done.                           */
/*                                                                       */
/*                                                                       */
/*  To produce specifically named output mosaic                          */
/*  and clean up all the intermediate files:                             */
/*                                                                       */
/*                                                                       */
/*           mExec -o region.fits -c 2MASS J region.hdr                  */
/*                                                                       */
/*                                                                       */
/*     This will produce a half-degree square mosaic of the              */
/*     2MASS J band data in the subdirectory "workspace"                 */
/*     called mosaic.fits .  All other files will be                     */
/*     cleaned up when the processing is done.                           */
/*                                                                       */
/*                                                                       */
/*************************************************************************/

int main(int argc, char **argv, char **envp)
{
   int    i, j, k, ch, index, baseCount, count, sys, id, ival;
   int    ncols, iurl, ifname, failed, nooverlap, istat;
   int    errno, flag, noverlap, nraw, nimages;
   int    intan, outtan;
   int    keepAll, deleteAll, levelOnly;

   double val;

   struct WorldCoor *wcs, *wcsin;
   double epoch;

   fitsfile *infptr;

   int    fitsstat = 0;

   char   fheader[1600];
   char  *inheader;

   char  *ptr;

   char   temp   [MAXLEN];
   char   buf    [BUFSIZE];
   char   cwd    [MAXLEN];

   int    icntr1;
   int    icntr2;
   int    ifname1;
   int    ifname2;
   int    idiffname;

   int    cntr1;
   int    cntr2;

   char   url       [MAXLEN];
   char   gpfsname  [MAXLEN];
   char   fname     [MAXLEN];
   char   fname1    [MAXLEN];
   char   fname2    [MAXLEN];
   char   diffname  [MAXLEN];
   char   areafile  [MAXLEN];
   char   corrected [MAXLEN];
   char   survey    [MAXLEN];
   char   label     [MAXLEN];

   char   hdrfile   [MAXLEN];
   char   hdrtext   [MAXLEN];
   char   outstr    [MAXLEN];
   char   msgfile   [MAXLEN];
   char   savefile  [MAXLEN];
   char   fitsurl   [MAXLEN];
   char   urlbase   [MAXLEN];
   char   urlcoded  [MAXLEN];

   int    cntr      [MAXLEN];
   char   file      [MAXLEN][1024];

   int    ia;
   int    ib;
   int    ic;

   char   astr      [MAXLEN];
   char   bstr      [MAXLEN];
   char   cstr      [MAXLEN];

   double a;
   double b;
   double c;
   double crpix1;
   double crpix2;
   int    xmin;
   int    xmax;
   int    ymin;
   int    ymax;
   double xcenter;
   double ycenter;
   double npixel;
   double rms;
   double boxx;
   double boxy;
   double boxwidth;
   double boxheight;
   double boxangle;

   double scale;

   double error, maxerror;

   FILE  *fin;
   FILE  *fout;
   FILE  *fsave;
   FILE  *fhtml;
  
   char   band      [16];

   char   cmd       [MAXLEN];
   char   env0      [MAXLEN];
   char   env1      [MAXLEN];
   char   env2      [MAXLEN];
   char   env3      [MAXLEN];
   char   env4      [MAXLEN];
   char   env5      [MAXLEN];
   char   env6      [MAXLEN];
   char   env7      [MAXLEN];
   char   env8      [MAXLEN];
   char   status    [MAXLEN];
   char   infile    [MAXLEN];
   char   outfile   [MAXLEN];
   char   path      [MAXLEN];

   char   template [MAXLEN];
   char   workspace[MAXLEN];

   char  *subdir;

   FILE  *fhdr;
   FILE  *bhdr;

   double ra[4], dec[4];
   double rac, decc;
   double x1, y1, z1;
   double x2, y2, z2;
   double xpos, ypos;
   double dtr;

   dtr = atan(1.)/45.;

   inheader = malloc(MAXHDR);

   getcwd(cwd, MAXLEN);


   /************************/
   /* Initialization stuff */
   /************************/

   time(&currtime);
   start = currtime;

   svc_sigset();


   /*****************************/
   /* Read the input parameters */
   /*****************************/

   strcpy(workspace, "");
   strcpy(savefile,  "");
   strcpy(hdrfile,   "");
   strcpy(hdrtext,   "");
	
   keepAll   = 0;
   deleteAll = 0;
   levelOnly = 0;

   debug  = 0;
   opterr = 0;

   while ((ch = getopt(argc, argv, "lkch:f:o:d:L:n:")) != EOF)
   {
      switch (ch)
      {
         case 'l':
            levelOnly = 1;
            break;

         case 'k':
            keepAll = 1;
            break;

         case 'c':
            deleteAll = 1;
            break;

         case 'h':
            strcpy(hdrtext, optarg);
            break;

         case 'f':
            strcpy(hdrfile, optarg);
            break;

         case 'o':
            strcpy(savefile, optarg);
            break;

         case 'd':
            debug = debugCheck(optarg);
            break;

         case 'L':
            strcpy(label, optarg);
            break;

         case 'n':
            strcpy(logaddr, optarg);
            break;

         default:
            printf("[struct stat=\"ERROR\", msg=\"Usage: %s [-l(evel only)][-k(eep all)][-c(lean)][-o output.fits][-d(ebug) level][-f region.hdr | -h header] survey band [workspace-dir]\"]\n", argv[0]);
            exit(1);
            break;
      }
   }

   if (argc - optind < 2)
   {
      printf("[struct stat=\"ERROR\", msg=\"Usage: %s [-l(evel only)][-k(eep all)][-c(lean)][-o output.fits][-d(ebug) level][-f region.hdr | -h header] survey band [workspace-dir]\"]\n", argv[0]);
      exit(1);
   }

   strcpy(survey,  argv[optind]);
   strcpy(band,    argv[optind+1]);

   if(argc - optind > 2)
      strcpy(workspace, argv[optind+2]);

   if(strlen(workspace) == 0)
   {
      strcpy(template, "MOSAIC_XXXXXX");
      strcpy(workspace, (char *)mktemp(template));
   }

   subdir = workspace;

   if(workspace[0] != '/')
   {
      strcpy(temp, cwd);

      if(temp[strlen(temp)-1] != '/')
	 strcat(temp, "/");

      if(strlen(workspace) == 0)
	 temp[strlen(temp)-1] = '\0';
      else
	 strcat(temp, workspace);
      
      strcpy(workspace, temp);
   }
   else
      for(i=0; i<strlen(workspace); ++i)
         if(workspace[i] == '/')
            subdir = workspace+i+1;



   /************************************************************/
   /* Try Sinit/Sexit up front to see if SRB is available.     */
   /* Create it if it doesn't.                                 */
   /************************************************************/

   svc_run(cmd);

   strcpy( status, svc_value( "stat" ));

   if (strcmp( status, "ERROR") == 0)
   {
      sprintf( msg, "Sorry, the results storage system at SDSC is currently unavailable [Error %s]",
         svc_value("errorcode"));
      printerr(msg);
   }
      
   sprintf(cmd, "sexit.sh");

   svc_run(cmd);

   strcpy( status, svc_value( "stat" ));

   if (strcmp( status, "ERROR") == 0)
   {
      sprintf( msg, "Sorry, the results storage system at SDSC appears to be experiencing difficulties [Error %s]",
         svc_value("errorcode"));
      printerr(msg);
   }
      


   /******************************************************************/
   /* Make sure the workspace directory of the workspace exists.     */
   /* Create it if it doesn't.                                       */
   /******************************************************************/

   mkdir(workspace, 0775);

   sprintf(msgfile, "%s/msg.html", workspace);

   fmsg = fopen(msgfile, "w+");

   if(fmsg == (FILE *)NULL)
   {
      sprintf(msg, "Can't open workspace header template file: [%s]", 
	 msgfile);

      printerr(msg);
   }

   fprintf(fmsg, "<html>\n");
   fprintf(fmsg, "<body bgcolor=\"#ffffff\">\n");
   fprintf(fmsg, "<pre>\n");
   fflush(fmsg);

   if(debug)
   {
      fprintf(fmsg, "DEBUGGING OUTPUT\n\n");
      fflush(fmsg);

      svc_debug(fmsg);
   }


   
   /******************************************************************/
   /* Copy the header template from a file, if that is the way it    */
   /* was given.                                                     */
   /******************************************************************/

   if(strlen(hdrfile) > 0)
   {
      fin = fopen(hdrfile, "r" );

      if(fin == (FILE *)NULL)
      {
	 sprintf(msg, "Can't open original header template file: [%s]",
	    hdrfile);

	 printerr(msg);
      }

      sprintf(cmd, "%s/region.hdr", workspace);

      fout = fopen(cmd, "w+");

      if(fout == (FILE *)NULL)
      {
	 sprintf(msg, "Can't open workspace header template file: [%s]", 
	    cmd);

	 printerr(msg);
      }

      while(1)
      {
	 count = fread(buf, sizeof(char), BUFSIZE, fin);

	 if(count == 0)
	    break;

	 fwrite(buf, sizeof(char), count, fout);
      }

      fflush(fout);
      fclose(fout);
      fclose(fin);
   }

   
   /******************************************************************/
   /* Or if the header was given on the command-line, copy that.     */
   /******************************************************************/

   else
   {
      if(strlen(hdrtext) == 0)
      {
	 printf("[struct stat=\"ERROR\", msg=\"Must have either header file (-f) or header text (-h)\"]\n");
	 exit(1);
      }

      sprintf(cmd, "%s/region.hdr", workspace);

      fout = fopen(cmd, "w+");

      if(fout == (FILE *)NULL)
      {
	 sprintf(msg, "Can't open workspace header template file: [%s]", 
	    cmd);

	 printerr(msg);
      }
 
      while(hdrtext[strlen(hdrtext)-1] == '\n'
         || hdrtext[strlen(hdrtext)-1] == '\r')
            hdrtext[strlen(hdrtext)-1]  = '\0';

      k = 0;

      for(j=0; j<strlen(hdrtext); ++j)
      {
         if(hdrtext[j] == '\0')
	 {
	    outstr[k] = '\0';

	    if(strlen(outstr) > 0)
	       fprintf(fout, "%s\n", outstr);

	    break;
	 }

         else if(strncmp(hdrtext+j, "\\n", 2) == 0)
	 {
	    outstr[k] = '\0';

	    fprintf(fout, "%s\n", outstr);

            ++j;

	    k = 0;
	 }

	 else if(hdrtext[j] == '\r' || hdrtext[j] == '\n')
         {
            /* do nothing */
         }

	 else
	 {
	    outstr[k] = hdrtext[j];
	    ++k;
	 }
      }

      fclose(fout);
   }



   /******************************/
   /* Print out input parameters */
   /******************************/

   if(debug)
   {
     fprintf(fmsg, "\nDEBUG: SETUP PARAMETERS\n");
     fprintf(fmsg, "survey      = [%s]\n",  survey);
     fprintf(fmsg, "band        = [%s]\n",  band);
     fprintf(fmsg, "hdrfile     = [%s]\n",  hdrfile);
     fprintf(fmsg, "hdrtext     =  %d characters\n",  strlen(hdrtext));
     fprintf(fmsg, "workspace   = [%s]\n",  workspace);
     fprintf(fmsg, "levelOnly   =  %d\n",   levelOnly);
     fprintf(fmsg, "keepAll     =  %d\n",   keepAll);
     fprintf(fmsg, "deleteAll   =  %d\n\n", deleteAll);
     fprintf(fmsg, "cwd         = [%s]\n",  cwd);
     fflush(fmsg);
   }
   else 
   {
     fprintf(fmsg, "\nSETUP PARAMETERS:\n\n");
     fprintf(fmsg, "survey      = [%s]\n",  survey);
     fprintf(fmsg, "band        = [%s]\n",  band);
     fflush(fmsg);
   }


   /*************************/
   /* Create subdirectories */
   /*************************/

   if(debug)
   {
      fprintf(fmsg, "chdir to [%s]\n", workspace);
      fflush(fmsg);
   }

   chdir(workspace);

   if(mkdir("raw", 0775) < 0)
   {
      flag = 1;
      if(errno != EEXIST)
	 flag = 0;
   }
   
   if(mkdir("projected", 0775) < 0)
   {
      flag = 1;
      if(errno != EEXIST)
	 flag = 0;
   }

   if(mkdir("diffs", 0775) < 0)
   {
      flag = 1;
      if(errno != EEXIST)
	 flag = 0;
   }

   if(mkdir("corrected", 0775) < 0)
   {
      flag = 1;
      if(errno != EEXIST)
	 flag = 0;
   }

   if(flag)
      printerr("Can't create proper subdirectories in workspace");
   



   /***********************************************/
   /* Create the WCS using the header template    */
   /* and generate a new header covering more     */
   /* area (so we don't get partial input images) */
   /***********************************************/

   fhdr = fopen("region.hdr", "r");

   if(fhdr == (FILE *)NULL)
      printerr("Can't open header template file");


   bhdr = fopen("big_region.hdr", "w+");

   if(bhdr == (FILE *)NULL)
      printerr("Can't open expanded header file: [big_region.hdr]");


   fprintf(fmsg, "\nHEADER:\n");
   fprintf(fmsg, "-----------------------------------------------------------\n");
   fflush(fmsg);

   while(1)
   {
      if(fgets(temp, MAXLEN, fhdr) == (char *)NULL)
	 break;

      if(temp[strlen(temp)-1] == '\n')
         temp[strlen(temp)-1] =  '\0';

      fprintf(fmsg, "%s\n", temp);
      fflush(fmsg);

      if(strncmp(temp, "NAXIS1", 6) == 0)
      {
	 ival = atoi(temp+9);
	 fprintf(bhdr, "NAXIS1  = %d\n", ival+3000);
      }
      else if(strncmp(temp, "NAXIS2", 6) == 0)
      {
	 ival = atoi(temp+9);
	 fprintf(bhdr, "NAXIS2  = %d\n", ival+3000);
      }
      else if(strncmp(temp, "CRPIX1", 6) == 0)
      {
	 val = atof(temp+9);
	 fprintf(bhdr, "CRPIX1  = %15.10f\n", val+1500);
      }
      else if(strncmp(temp, "CRPIX2", 6) == 0)
      {
	 val = atof(temp+9);
	 fprintf(bhdr, "CRPIX2  = %15.10f\n", val+1500);
      }
      else
	 fprintf(bhdr, "%s\n", temp);

      stradd(fheader, temp);
   }

   fprintf(fmsg, "-----------------------------------------------------------\n\n");
   fflush(fmsg);

   fclose(fhdr);
   fclose(bhdr);



   /*********************************/
   /* Find the corners of this area */
   /* and the diagonal size         */
   /*********************************/

   wcs = wcsinit(fheader);


   /* Get the coordinate system and epoch in a form     */
   /* compatible with the coordinate conversion library */

   if(wcs->syswcs == WCS_J2000)
   {
      sys   = EQUJ;
      epoch = 2000.;

      if(wcs->equinox == 1950)
	 epoch = 1950.;
   }
   else if(wcs->syswcs == WCS_B1950)
   {
      sys   = EQUB;
      epoch = 1950.;

      if(wcs->equinox == 2000)
	 epoch = 2000;
   }
   else if(wcs->syswcs == WCS_GALACTIC)
   {
      sys   = GAL;
      epoch = 2000.;
   }
   else if(wcs->syswcs == WCS_ECLIPTIC)
   {
      sys   = ECLJ;
      epoch = 2000.;

      if(wcs->equinox == 1950)
      {
	 sys   = ECLB;
	 epoch = 1950.;
      }
   }
   else  
   {
      sys   = EQUJ;
      epoch = 2000.;
   }


   /* Get the corners and the center */

   pix2wcs(wcs, wcs->nxpix/2., wcs->nypix/2., &xpos, &ypos);

   convertCoordinates(sys, epoch, xpos, ypos,
		      EQUJ, 2000., &rac, &decc, 0.0);

   pix2wcs(wcs, 0.5, 0.5, &xpos, &ypos);

   convertCoordinates(sys, epoch, xpos, ypos,
		      EQUJ, 2000., &ra[0], &dec[0], 0.0);

   pix2wcs(wcs, wcs->nxpix+0.5, 0.5, &xpos, &ypos);

   convertCoordinates(sys, epoch, xpos, ypos,
		      EQUJ, 2000., &ra[1], &dec[1], 0.0);

   pix2wcs(wcs, wcs->nxpix+0.5, wcs->nypix+0.5, &xpos, &ypos);

   convertCoordinates(sys, epoch, xpos, ypos,
		      EQUJ, 2000., &ra[2], &dec[2], 0.0);

   pix2wcs(wcs, 0.5, wcs->nypix+0.5, &xpos, &ypos);

   convertCoordinates(sys, epoch, xpos, ypos,
		      EQUJ, 2000., &ra[3], &dec[3], 0.0);


   /* Compute the diagonal size */

   x1 = cos(ra[0]*dtr) * cos(dec[0]*dtr);
   y1 = sin(ra[0]*dtr) * cos(dec[0]*dtr);
   z1 = sin(dec[0]*dtr);

   x2 = cos(ra[2]*dtr) * cos(dec[2]*dtr);
   y2 = sin(ra[2]*dtr) * cos(dec[2]*dtr);
   z2 = sin(dec[2]*dtr);

   scale = acos(x1*x2 + y1*y2 + z1*z2) / dtr;

   if(debug)
   {
      fprintf(fmsg, "Scale = %-g\n", scale);
      fflush(fmsg);
   }



   /*************************************/
   /* Get the image list for the region */
   /* of interest                       */
   /*************************************/

   scale = scale * 1.42;
 
   sprintf(cmd, "mArchiveList -s gpfs %s %s \"%.4f %.4f eq j2000\" %.2f %.2f remote.tbl", 
      survey, band, rac, decc, scale, scale);

   if(debug)
   {
      fprintf(fmsg, "[%s]\n", cmd);
      fflush(fmsg);
   }

   svc_run(cmd);

   strcpy( status, svc_value( "stat" ));

   if (strcmp( status, "ERROR") == 0)
   {
      strcpy( msg, svc_value( "msg" ));

      printerr(msg);
   }
      
   if (strcmp(status, "ABORT") == 0) 
   {
      strcpy( msg, svc_value( "msg" ));

      printerr(msg);
   }
      
   nimages = atof(svc_value("count"));

   if (nimages == 0)
   {
      sprintf( msg, "%s has no data covering this area", survey);

      printerr(msg);
   }

   fprintf(fmsg, "\nACCESSING ARCHIVE:\n\n");
   fflush(fmsg);

   fprintf(fmsg, "%d images in overlap region\n", nimages);
   fflush(fmsg);

   sprintf(msg, "%d archive images in region", nimages);

   sprintf(cmd, "mNotifyTG %s \"%s\"",
      logaddr, msg);

   if(debug)
   {
      fprintf(fmsg, "[%s]\n", cmd);
      fflush(fmsg);
   }

   svc_run(cmd);



   /************************************/
   /* Get the images (using /gpfs-wan) */
   /************************************/

   if(debug)
   {
      fprintf(fmsg, "chdir to [%s]\n", "raw");
      fflush(fmsg);
   }

   chdir("raw");


   /* 2MASS can be accessed as "local" Teragrid */
   /* (i.e. GPFS) files                         */

   if(strncasecmp(survey, "2MASS", 5) == 0)
   {
      ncols = topen("../remote.tbl");

      ifname = tcol( "file");
      iurl   = tcol( "URL");

      if(ifname < 0 
      || iurl   < 0)
      {
	 strcpy(msg, "Need columns 'file' and 'url' in input");

	 printerr(msg);
      }

      nraw    = nimages;
      nimages = 0;

      while(1)
      {
	 istat = tread();

	 if(istat < 0)
	    break;

	 strcpy(url,   tval(iurl));
	 strcpy(fname, tval(ifname));

	 ptr = strstr(url, "ref=");

	 sprintf(gpfsname, "/gpfs-wan/2MASS-unzipped%s", ptr+9);

	 gpfsname[strlen(gpfsname)-3] = '\0';

	 if(debug)
	 {
	    fprintf(fmsg, "copy [%s] to [%s]\n", gpfsname, fname);
	    fflush(fmsg);
	 }

	 fin   = fopen(gpfsname, "r" );

	 if(fin == (FILE *)NULL)
	 {
	    sprintf(msg, "Can't open archive file: [%s]", gpfsname);

	    printerr(msg);
	 }

	 fsave = fopen( fname, "w+");

	 if(fsave == (FILE *)NULL)
	 {
	    sprintf(msg, "Can't open archive file copy file: [%s]", fname);

	    printerr(msg);
	 }

	 while(1)
	 {
	    count = fread(buf, sizeof(char), BUFSIZE, fin);

	    if(count == 0)
	       break;

	    fwrite(buf, sizeof(char), count, fsave);
	 }

	 fflush(fsave);
	 fclose(fsave);
	 fclose(fin);

	 ++nimages;

	 if(nimages/50 * 50 == nimages)
	 {
	    time(&currtime);

	    sprintf(msg, "Retrieved %d of %d archive images", 
	       nimages, nraw);

	    sprintf(cmd, "mNotifyTG %s \"%s\"",
	       logaddr, msg);

	    if(debug)
	    {
	       fprintf(fmsg, "[%s]\n", cmd);
	       fflush(fmsg);
	    }

	    svc_run(cmd);
	 }
      }

      tclose();
   }


   /* Other archives have to be downloaded from remote sites */

   else
   {
      strcpy(cmd, "mArchiveExec ../remote.tbl");

      if(debug)
      {
	 printf("[%s]\n", cmd);
	 fflush(stdout);
      }

      svc_run(cmd);

      strcpy( status, svc_value( "stat" ));

      if (strcmp( status, "ERROR") == 0)
      {
	 strcpy( msg, svc_value( "msg" ));
	 
	 printerr(msg);
      }

      if (strcmp(status, "ABORT") == 0)
      {
	 strcpy( msg, svc_value( "msg" ));

	 printerr(msg);
      }

      nimages = atof(svc_value("count"));
   }


   if (nimages == 0)
   {
      strcpy( msg, "No data was available for the region specified at this time");

      printerr(msg);
   }

   fprintf(fmsg, "%d images retrieved\n", nimages);
   fflush(fmsg);

   time(&currtime);

   fprintf(fmsg, "\n(Time: %d sec elapsed)\n\n", 
	  (int)(currtime - start));

   sprintf(msg, "Retrieved %d archive images", 
      nimages);

   sprintf(cmd, "mNotifyTG %s \"%s\"",
      logaddr, msg);

   if(debug)
   {
      fprintf(fmsg, "[%s]\n", cmd);
      fflush(fmsg);
   }

   svc_run(cmd);

   

   /********************************************/ 
   /* Create and open the raw image table file */
   /********************************************/ 

   chdir(workspace);

   sprintf(cmd, "mImgtbl raw rimages.tbl");

   if(debug)
   {
      fprintf(fmsg, "[%s]\n", cmd);
      fflush(fmsg);
   }

   svc_run(cmd);

   ncols = topen("rimages.tbl");

   ifname = tcol( "fname");

   if(ifname < 0)
      printerr("Need column 'fname' in input");




   /*************************************************/ 
   /* Try to generate an alternate header so we can */
   /* use the fast projection                       */
   /*************************************************/ 

   outtan = INTRINSIC;

   if((   wcs->syswcs  != WCS_J2000)
   || (   wcs->prjcode != WCS_TAN
       && wcs->prjcode != WCS_SIN
       && wcs->prjcode != WCS_ZEA
       && wcs->prjcode != WCS_STG
       && wcs->prjcode != WCS_ARC))
   {
      sprintf(cmd, "mTANHdr -c eq big_region.hdr altout.hdr");

      if(debug)
      {
	 fprintf(fmsg, "[%s]\n", cmd);
	 fflush(fmsg);
      }

      svc_run(cmd);

      strcpy( status, svc_value( "stat" ));

      if(strcmp( status, "ERROR") == 0)
      {
	 strcpy (msg, svc_value( "msg" ));
	 printerr(msg);
      }
      else
      {
	 outtan = COMPUTED;

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
	    fprintf(fmsg, "Using distorted TAN on output: max error = %-g\n", maxerror);
	    fflush(fmsg);
	 }

	 if(maxerror > 0.1)
	    outtan = FAILED;
      }
   }

   
   /*************************************************/ 
   /* Read the records and call mProject/mProjectPP */
   /*************************************************/ 

   index     = 0;
   failed    = 0;
   nooverlap = 0;

   fprintf(fmsg, "\nREPROJECTING:\n\n");
   fflush(fmsg);

   while(1)
   {
      istat = tread();

      if(istat < 0)
	 break;

      strcpy ( infile, tval(ifname));
      sprintf(outfile, "p%s", infile);

      if(strlen(outfile) > 3 && strcmp(outfile+strlen(outfile)-3, ".gz") == 0)
         *(outfile+strlen(outfile)-3) = '\0';

      if(strcmp(infile, outfile) == 0)
      {
	 strcpy(msg, "Output would overwrite input");

	 printerr(msg);
      }


      /* Try to generate an alternate input header so we can */
      /* use the fast projection                             */

      intan = 0;

      sprintf(path, "raw/%s", infile);

      if(fits_open_file(&infptr, path, READONLY, &fitsstat))
         FITSerror(infile, fitsstat);

      if(fits_get_image_wcs_keys(infptr, &inheader, &fitsstat))
         FITSerror(infile, fitsstat);

      if(fits_close_file(infptr, &fitsstat))
         FITSerror(infile, fitsstat);

      wcsin = wcsinit(inheader);

      if(wcsin == (struct WorldCoor *)NULL)
      {
	 strcpy(msg, "Bad WCS in input image");

	 printerr(msg);
      }

      if( wcs->syswcs != WCS_J2000

      || (   strcmp(wcsin->ptype, "TAN") != 0
	  && strcmp(wcsin->ptype, "SIN") != 0
	  && strcmp(wcsin->ptype, "ZEA") != 0
	  && strcmp(wcsin->ptype, "STG") != 0
	  && strcmp(wcsin->ptype, "ARC") != 0))
      {
	 sprintf(cmd, "mGetHdr %s orig.hdr", path);

	 if(debug)
	 {
	    fprintf(fmsg, "[%s]\n", cmd);
	    fflush(fmsg);
	 }

	 svc_run(cmd);

	 strcpy( status, svc_value( "stat" ));

	 if(strcmp( status, "ERROR") == 0)
	 {
	    strcpy(msg, svc_value( "msg" ));

	    printerr(msg);
	 }

	 sprintf(cmd, "mTANHdr -c eq orig.hdr altin.hdr");

	 if(debug)
	 {
	    fprintf(fmsg, "[%s]\n", cmd);
	    fflush(fmsg);
	 }

	 svc_run(cmd);

	 strcpy( status, svc_value( "stat" ));

	 if(strcmp( status, "ERROR") == 0)
	 {
	    strcpy(msg, svc_value( "msg" ));

	    printerr(msg);
	 }
	 else
	 {
            intan = COMPUTED;

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

	    fprintf(fmsg, "Using distorted TAN on input: max error = %-g\n", maxerror);
	    fflush(fmsg);

	    if(maxerror > 0.1)
               intan = FAILED;
	 }
      }
      else
         intan = INTRINSIC;



      /* Now run mProject or mProjectPP (depending */
      /* on what we have to work with)             */

      if(     intan == COMPUTED  && outtan == COMPUTED )
	 sprintf(cmd, "mProjectPP -b 1 -i altin.hdr -o altout.hdr raw/%s projected/%s big_region.hdr",
	    infile, outfile);

      else if(intan == COMPUTED  && outtan == INTRINSIC)
	 sprintf(cmd, "mProjectPP -b 1 -i altin.hdr raw/%s projected/%s big_region.hdr",
	    infile, outfile);

      else if(intan == INTRINSIC && outtan == COMPUTED )
	 sprintf(cmd, "mProjectPP -b 1 -o altout.hdr raw/%s projected/%s big_region.hdr",
	    infile, outfile);

      else if(intan == INTRINSIC && outtan == INTRINSIC)
	 sprintf(cmd, "mProjectPP -b 1 raw/%s projected/%s big_region.hdr",
	    infile, outfile);

      else
	 sprintf(cmd, "mProject -b 1 raw/%s projected/%s big_region.hdr",
            infile, outfile);

      if(debug)
      {
	 fprintf(fmsg, "[%s]\n", cmd);
	 fflush(fmsg);
      }

      svc_run(cmd);

      strcpy( status, svc_value( "stat" ));

      ++index;

      if(strcmp( status, "ABORT") == 0)
      {
	 strcpy( msg, svc_value( "msg" ));

	 printerr(msg);
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
	    strcat(msg, ": ");
	    strcat(msg, tval(ifname));
	 }
	 else
	 {
	    ++failed;
	    strcat(msg, ": ");
	    strcat(msg, tval(ifname));
	 }
      }
      else
      {
	 fprintf(fmsg, "%s took %s seconds (%3d of %3d)\n", 
            tval(ifname), svc_value("time"), index, nimages);
	 fflush(fmsg);
      }

      if(!keepAll)
      {
         sprintf(cmd, "raw/%s", infile);
	 unlink(cmd);
      }

      if(index/10 * 10 == index)
      {
	 time(&currtime);

	 sprintf(msg, "Reprojected %d of %d images", 
	    index, nimages);

	 sprintf(cmd, "mNotifyTG %s \"%s\"",
	    logaddr, msg);

	 if(debug)
	 {
	    fprintf(fmsg, "[%s]\n", cmd);
	    fflush(fmsg);
	 }

	 svc_run(cmd);
      }
   }

   baseCount = index - failed - nooverlap;

   fprintf(fmsg, "\n%d images reprojected successfully (%d failed, %d did not overlap region)\n",
      baseCount, failed, nooverlap);
   fflush(fmsg);

   time(&currtime);

   fprintf(fmsg, "\n(Time: %d sec elapsed)\n\n", 
	  (int)(currtime - start));

   sprintf(msg, "Reprojected %d images (%d failed, %d did not overlap region)", 
      baseCount, failed, nooverlap);

   sprintf(cmd, "mNotifyTG %s \"%s\"",
      logaddr, msg);

   if(debug)
   {
      fprintf(fmsg, "[%s]\n", cmd);
      fflush(fmsg);
   }

   svc_run(cmd);

   tclose();

   unlink("altin.hdr");
   unlink("altout.hdr");



   /*********************************/
   /* Generate the differences list */
   /*********************************/

   if(baseCount > 1)
   {
      sprintf(cmd, "mImgtbl projected pimages.tbl");

      if(debug)
      {
	 fprintf(fmsg, "[%s]\n", cmd);
	 fflush(fmsg);
      }

      svc_run(cmd);

      nimages = atof(svc_value("count"));

      strcpy(cmd, "mOverlaps pimages.tbl diffs.tbl");

      if(debug)
      {
	 fprintf(fmsg, "[%s]\n", cmd);
	 fflush(fmsg);
      }

      svc_run(cmd);

      strcpy( status, svc_value( "stat" ));

      if(strcmp( status, "ERROR") == 0)
      {
	 strcpy( msg, svc_value( "msg" ));

	 printerr(msg);
      }

      noverlap = atof(svc_value("count"));

      fprintf(fmsg, "\nFITTING DIFFERENCES:\n\n");
      fprintf(fmsg, "%d overlap regions\n\n", noverlap);
      fflush(fmsg);

      sprintf(msg, "%d overlap regions", noverlap);

      sprintf(cmd, "mNotifyTG %s \"%s\"",
	 logaddr, msg);

      if(debug)
      {
	 fprintf(fmsg, "[%s]\n", cmd);
	 fflush(fmsg);
      }

      svc_run(cmd);
   }



   /***************************************/ 
   /* Open the difference list table file */
   /***************************************/ 

   if(baseCount > 1)
   {
      ncols = topen("diffs.tbl");

      if(ncols <= 0)
      {
	 fprintf(fmsg, "[struct stat=\"ERROR\", msg=\"Invalid image metadata file: diffs.tbl\"]\n");
	 exit(1);
      }

      icntr1    = tcol( "cntr1");
      icntr2    = tcol( "cntr2");
      ifname1   = tcol( "plus");
      ifname2   = tcol( "minus");
      idiffname = tcol( "diff");


      /***************************************************/ 
      /* Read the records and call mDiff, then mFitplane */
      /***************************************************/ 

      count   = 0;
      failed  = 0;

      fout = fopen("fits.tbl", "w+");

      fprintf(fout, "| plus|minus|       a    |      b     |      c     | crpix1  | crpix2  | xmin | xmax | ymin | ymax | xcenter | ycenter |  npixel |    rms     |    boxx    |    boxy    |  boxwidth  | boxheight  |   boxang   |\n");
      fflush(fout);

      while(1)
      {
	 ++count;
	 istat = tread();

	 if(istat < 0)
	    break;

	 cntr1 = atoi(tval(icntr1));
	 cntr2 = atoi(tval(icntr2));

	 fprintf(fmsg, "Processing image difference %3d - %3d (%3d of %3d)\n", 
	    cntr1+1, cntr2+1, count, noverlap);
	 fflush(fmsg);

	 strcpy(fname1,   tval(ifname1));
	 strcpy(fname2,   tval(ifname2));
	 strcpy(diffname, tval(idiffname));

	 sprintf(cmd, "mDiff projected/%s projected/%s diffs/%s big_region.hdr", fname1, fname2, diffname);

	 if(debug)
	 {
	    fprintf(fmsg, "[%s]\n", cmd);
	    fflush(fmsg);
	 }

	 svc_run(cmd);

	 if(strcmp( status, "ABORT") == 0)
	 {
	    strcpy( msg, svc_value( "msg" ));

	    fprintf(fmsg, "[struct stat=\"ERROR\", msg=\"%s\"]\n", msg);
	    fflush(fmsg);

	    exit(1);
	 }

	 strcpy( status, svc_value( "stat" ));

	 if(strcmp( status, "ERROR") == 0)
	    ++failed;


	 sprintf(cmd, "mFitplane diffs/%s", diffname);

	 if(debug)
	 {
	    fprintf(fmsg, "[%s]\n", cmd);
	    fflush(fmsg);
	 }

	 svc_run(cmd);

	 strcpy( status, svc_value( "stat" ));

	 if(strcmp( status, "ABORT") == 0)
	 {
	    strcpy( msg, svc_value( "msg" ));

	    fprintf(fmsg, "[struct stat=\"ERROR\", msg=\"%s\"]\n", msg);
	    fflush(fmsg);

	    exit(1);
	 }

	 if(strcmp( status, "ERROR")   == 0
	 || strcmp( status, "WARNING") == 0)
	    ++failed;
	 else
	 {
	    a         = atof(svc_value("a"));
	    b         = atof(svc_value("b"));
	    c         = atof(svc_value("c"));
	    crpix1    = atof(svc_value("crpix1"));
	    crpix2    = atof(svc_value("crpix2"));
	    xmin      = atoi(svc_value("xmin"));
	    xmax      = atoi(svc_value("xmax"));
	    ymin      = atoi(svc_value("ymin"));
	    ymax      = atoi(svc_value("ymax"));
	    xcenter   = atof(svc_value("xcenter"));
	    ycenter   = atof(svc_value("ycenter"));
	    npixel    = atof(svc_value("npixel"));
	    rms       = atof(svc_value("rms"));
	    boxx      = atof(svc_value("boxx"));
	    boxy      = atof(svc_value("boxy"));
	    boxwidth  = atof(svc_value("boxwidth"));
	    boxheight = atof(svc_value("boxheight"));
	    boxangle  = atof(svc_value("boxang"));

	    fprintf(fout, " %5d %5d %12.5e %12.5e %12.5e %9.2f %9.2f %6d %6d %6d %6d %9.2f %9.2f %9.0f %12.5e %12.1f %12.1f %12.1f %12.1f %12.1f\n",
	       cntr1, cntr2, a, b, c, crpix1, crpix2, xmin, xmax, ymin, ymax, 
	       xcenter, ycenter, npixel, rms, boxx, boxy, boxwidth, boxheight, boxangle);
	    fflush(fout);
	 }

	 if(!keepAll)
	 {
	    sprintf(cmd, "diffs/%s", diffname);
	    unlink(cmd);

	    strcpy(areafile, cmd);
	    areafile[strlen(areafile) - 5] = '\0';
	    strcat(areafile, "_area.fits");

	    unlink(areafile);
	 }

	 if(count/10 * 10 == count)
	 {
	    time(&currtime);

	    sprintf(msg, "Processed %d of %d overlaps", 
	       count, noverlap);

	    sprintf(cmd, "mNotifyTG %s \"%s\"",
	       logaddr, msg);

	    if(debug)
	    {
	       fprintf(fmsg, "[%s]\n", cmd);
	       fflush(fmsg);
	    }

	    svc_run(cmd);
	 }
      }

      tclose();
   }


   /*********************************/
   /* Generate the correction table */
   /*********************************/

   if(baseCount > 1)
   {

      fprintf(fmsg, "\nGenerating background correction values\n");
      fflush(fmsg);

      if(levelOnly)
	 strcpy(cmd, "mBgModel -l pimages.tbl fits.tbl corrections.tbl");
      else
	 strcpy(cmd, "mBgModel pimages.tbl fits.tbl corrections.tbl");

      if(debug)
      {
	 fprintf(fmsg, "[%s]\n", cmd);
	 fflush(fmsg);
      }

      svc_run(cmd);

      strcpy( status, svc_value( "stat" ));

      if(strcmp( status, "ERROR") == 0)
      {
	 strcpy( msg, svc_value( "msg" ));

	 printerr(msg);
      }
   }

   time(&currtime);

   fprintf(fmsg, "\n(Time: %d sec elapsed)\n\n", 
	  (int)(currtime - start));

   sprintf(msg, "Overlap analysis complete");

   sprintf(cmd, "mNotifyTG %s \"%s\"",
      logaddr, msg);

   if(debug)
   {
      fprintf(fmsg, "[%s]\n", cmd);
      fflush(fmsg);
   }

   svc_run(cmd);


   /********************************************/ 
   /* Open the projected image list table file */
   /********************************************/ 

   if(baseCount > 1)
   {
      ncols = topen("pimages.tbl");

      icntr1 = tcol( "cntr");
      ifname = tcol( "fname");

      if(icntr1 < 0
      || ifname < 0)
      {
	 fprintf(fmsg,  "[struct stat=\"ERROR\", msg=\"Need columns: cntr and fname in image list\"]\n");
	 exit(1);
      }


      /********************************************/
      /* Read the records and save the file names */
      /********************************************/

      index = 0;

      while(1)
      {
	 istat = tread();

	 if(istat < 0)
	    break;

	 cntr[index] = atoi(tval(icntr1));

	 strcpy(file[index], tval(ifname));

	 ++index;
      }

      tclose();
   }



   /***********************************/ 
   /* Open the corrections table file */
   /***********************************/ 

   if(baseCount > 1)
   {
      ncols = topen("corrections.tbl");

      icntr1   = tcol( "id");
      ia       = tcol( "a");
      ib       = tcol( "b");
      ic       = tcol( "c");


      /*****************************************/ 
      /* Read the records and call mBackground */
      /*****************************************/ 

      count  = 0;
      failed = 0;

      fprintf(fmsg, "\nCORRECTING BACKGROUNDS:\n\n");
      fflush(fmsg);

      while(1)
      {
	 istat = tread();

	 if(istat < 0)
	    break;

	 id = atoi(tval(icntr1));

	 for(i=0; i<index; ++i)
	 {
	    if(id == cntr[i])
	       break;
	 }

	 if(i >= index)
	 {
	    if(debug)
	    {
	       fprintf(fmsg, "Can't find ID %d in list\n", id);
	       fflush(fmsg);
	    }

	    ++failed;
	    continue;
	 }

	 strcpy(astr, tval(ia));
	 strcpy(bstr, tval(ib));
	 strcpy(cstr, tval(ic));
	 
	 strcpy(corrected, file[i]);
	 corrected[0] = 'c';

	 fprintf(fmsg, "Background correcting file %s (%3d of %3d)\n",         
	    file[i], count+1, nimages);
	 fflush(fmsg);

	 sprintf(cmd, "mBackground projected/%s corrected/%s %s %s %s", 
	    file[i], corrected, astr, bstr, cstr);

	 if(debug)
	 {
	    fprintf(fmsg, "[%s]\n", cmd);
	    fflush(fmsg);
	 }

	 svc_run(cmd);

	 if(strcmp( status, "ABORT") == 0)
	 {
	    strcpy( msg, svc_value( "msg" ));
	 
	    fprintf(fmsg,  "[struct stat=\"ERROR\", msg=\"%s\"]\n", msg);
	    fflush(fmsg);
	 
	    exit(1);
	 }

	 strcpy( status, svc_value( "stat" ));

	 ++count;
	 if(strcmp( status, "ERROR") == 0)
	    ++failed;

	 if(!keepAll)
	 {
	    sprintf(cmd, "projected/%s", file[i]);
	    unlink(cmd);

	    strcpy(areafile, cmd);
	    areafile[strlen(areafile) - 5] = '\0';
	    strcat(areafile, "_area.fits");

	    unlink(areafile);
	 }

	 if(count/10 * 10 == count)
	 {
	    time(&currtime);

	    sprintf(msg, "Background corrected %d of %d images", 
	       count, nimages);

	    sprintf(cmd, "mNotifyTG %s \"%s\"",
	       logaddr, msg);

	    if(debug)
	    {
	       fprintf(fmsg, "[%s]\n", cmd);
	       fflush(fmsg);
	    }

	    svc_run(cmd);
	 }
      }
   }

   time(&currtime);

   fprintf(fmsg, "\n(Time: %d sec elapsed)\n\n", 
	  (int)(currtime - start));

   sprintf(msg, "Images background corrected");

   sprintf(cmd, "mNotifyTG %s \"%s\"",
      logaddr, msg);

   if(debug)
   {
      fprintf(fmsg, "[%s]\n", cmd);
      fflush(fmsg);
   }

   svc_run(cmd);


   /**************************/
   /* Coadd for final mosaic */
   /**************************/

   fprintf(fmsg, "\nFINAL IMAGE:\n\n");
   fflush(fmsg);

   if(baseCount > 1)
   {
      fprintf(fmsg, "Coadding for final mosaic\n");
      fflush(fmsg);

      sprintf(cmd, "mImgtbl corrected cimages.tbl");

      if(debug)
      {
	 fprintf(fmsg, "[%s]\n", cmd);
	 fflush(fmsg);
      }

      svc_run(cmd);

      sprintf(cmd, "mAdd -p corrected cimages.tbl region.hdr mosaic.fits");

      if(debug)
      {
	 fprintf(fmsg, "[%s]\n", cmd);
	 fflush(fmsg);
      }

      svc_run(cmd);

      strcpy( status, svc_value( "stat" ));

      if(strcmp( status, "ERROR") == 0)
      {
	 strcpy( msg, svc_value( "msg" ));

	 printerr(msg);
      }
   }
   else
   {
      fprintf(fmsg, "Cropping final image\n");
      fflush(fmsg);

      sprintf(cmd, "mImgtbl projected pimages.tbl");

      if(debug)
      {
	 fprintf(fmsg, "[%s]\n", cmd);
	 fflush(fmsg);
      }

      svc_run(cmd);

      sprintf(cmd, "mAdd -p projected pimages.tbl region.hdr mosaic.fits");

      if(debug)
      {
	 fprintf(fmsg, "[%s]\n", cmd);
	 fflush(fmsg);
      }

      svc_run(cmd);

      strcpy( status, svc_value( "stat" ));

      if(strcmp( status, "ERROR") == 0)
      {
	 strcpy( msg, svc_value( "msg" ));

	 printerr(msg);
      }
   }

   time(&currtime);

   fprintf(fmsg, "\n(Time: %d sec elapsed)\n\n", 
	  (int)(currtime - start));

   sprintf(msg, "Mosaic created");

   sprintf(cmd, "mNotifyTG %s \"%s\"",
      logaddr, msg);

   if(debug)
   {
      fprintf(fmsg, "[%s]\n", cmd);
      fflush(fmsg);
   }

   svc_run(cmd);


   /*******************************/ 
   /* Save file if so  instructed */
   /*******************************/ 

   if(strlen(savefile) > 0)
   {
      fin   = fopen("mosaic.fits", "r" );

      if(fin == (FILE *)NULL)
      {
	 sprintf(msg, "Can't open mosaic file: [mosaic.fits]");

	 printerr(msg);
      }

      fsave = fopen( savefile, "w+");

      if(fsave == (FILE *)NULL)
      {
	 sprintf(msg, "Can't open save file: [%s]", savefile);

	 printerr(msg);
      }

      while(1)
      {
	 count = fread(buf, sizeof(char), BUFSIZE, fin);

	 if(count == 0)
	    break;

	 fwrite(buf, sizeof(char), count, fsave);
      }

      fflush(fsave);
      fclose(fsave);
      fclose(fin);
   }


   /*******************************/ 
   /* Delete the corrected images */
   /* or the projected image if   */
   /* there was only the one      */
   /*******************************/ 

   if(baseCount > 1)
   {
      ncols = topen("cimages.tbl");

      ifname = tcol( "fname");

      if(ifname < 0)
      {
	 strcpy(msg, "Need column 'fname' in input");

	 printerr(msg);
      }

      while(1)
      {
	 istat = tread();

	 if(istat < 0)
	    break;

	 sprintf(infile, "corrected/%s", tval(ifname));

	 if(!keepAll)
	 {
	    unlink(infile);

	    strcpy(areafile, infile);
	    areafile[strlen(areafile) - 5] = '\0';
	    strcat(areafile, "_area.fits");

	    unlink(areafile);
	 }
      }

      tclose();

      if(!keepAll)
      {
	 unlink("big_region.hdr");
	 unlink("remote.tbl");
	 unlink("pimages.tbl");
	 unlink("cimages.tbl");
	 unlink("diffs.tbl");
	 unlink("fits.tbl");
	 unlink("corrections.tbl");
	 unlink("mosaic_area.fits");
	 rmdir ("raw");
	 rmdir ("projected");
	 rmdir ("diffs");
	 rmdir ("corrected");
      }
   }
   else
   {
      ncols = topen("pimages.tbl");

      ifname = tcol( "fname");

      if(ifname < 0)
      {
	 strcpy(msg, "Need column 'fname' in input");

	 printerr(msg);
      }

      while(1)
      {
	 istat = tread();

	 if(istat < 0)
	    break;

	 sprintf(infile, "projected/%s", tval(ifname));

	 if(!keepAll)
	 {
	    unlink(infile);

	    strcpy(areafile, infile);
	    areafile[strlen(areafile) - 5] = '\0';
	    strcat(areafile, "_area.fits");

	    unlink(areafile);
	 }
      }

      tclose();

      if(!keepAll)
      {
	 unlink("big_region.hdr");
	 unlink("remote.tbl");
	 unlink("pimages.tbl");
	 unlink("mosaic_area.fits");
	 rmdir ("raw");
	 rmdir ("projected");
      }
   }


   /***********************************/
   /* Create JPEG for results display */
   /***********************************/

   fprintf(fmsg, "Making JPEG\n");
   fflush(fmsg);

   strcpy(cmd, "mJPEG -ct 1 -gray mosaic.fits min max gaussianlog -out mosaic.jpg");


   if(debug)
   {
      fprintf(fmsg, "[%s]\n", cmd);
      fflush(fmsg);
   }

   svc_run(cmd);

   strcpy( status, svc_value( "stat" ));

   if(strcmp( status, "ERROR") == 0)
   {
      strcpy( msg, svc_value( "msg" ));

      printerr(msg);
   }

   time(&currtime);

   fprintf(fmsg, "\n(Time: %d sec elapsed)\n\n", 
	  (int)(currtime - start));

   sprintf(msg, "JPEG generated");

   sprintf(cmd, "mNotifyTG %s \"%s\"",
      logaddr, msg);

   if(debug)
   {
      fprintf(fmsg, "[%s]\n", cmd);
      fflush(fmsg);
   }

   svc_run(cmd);


   /**********************************************/ 
   /* Create the index.html file for the results */
   /* This is currently hardwired for SRB access */
   /* at SDSC.                                   */
   /**********************************************/ 

   time(&currtime);

   fhtml = fopen("index.html", "w+");

   strcpy(fitsurl,  "http://users.sdsc.edu/~leesa/cgi-bin/srb-get.cgi/mosaic.fits?/NVOzone/home/jcg.nvo/");
   strcpy(urlbase,  "http://users.sdsc.edu/~leesa/cgi-bin/srb-get.cgi?/NVOzone/home/jcg.nvo/");
   strcpy(urlcoded, "http%3A%2F%2Fusers.sdsc.edu%2F%7Eleesa%2Fcgi-bin%2Fsrb-get.cgi%3F%2FNVOzone%2Fhome%2Fjcg.nvo%2F");

   strcat(fitsurl,  subdir);
   strcat(urlbase,  subdir);
   strcat(urlcoded, subdir);

   fprintf(fhtml, "<html>\n");
   fprintf(fhtml, "<head>\n");
   fprintf(fhtml, "<title>MONTAGE Mosaic</title>\n");
   fprintf(fhtml, "<body bgcolor=\"#ffffff\">\n");
   fprintf(fhtml, "<center>\n");
   fprintf(fhtml, "<table>\n");
   fprintf(fhtml, "<tr>\n");
   fprintf(fhtml, "<td rowspan=2><img src=http://montage-lx.ipac.caltech.edu:8001/applications/Montage/NVO_100pixels.jpg></td>\n");
   fprintf(fhtml, "<td colspan=4 bgcolor=\"#6ba5d7\" align=center><font size=+2 color=\"#ffffff\">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Montage&nbsp;Mosaics&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</font></td>\n");
   fprintf(fhtml, "<td rowspan=2 align=right><img src=http://montage-lx.ipac.caltech.edu:8001/applications/Montage/logobig.jpg height=50></td>\n");
   fprintf(fhtml, "</tr>\n");
   fprintf(fhtml, "<tr>\n");
   fprintf(fhtml, "<td bgcolor=\"#6ba5d7\">&nbsp;</td>\n");
   fprintf(fhtml, "<td bgcolor=\"#6ba5d7\">&nbsp;</td>\n");
   fprintf(fhtml, "<td bgcolor=\"#6ba5d7\">&nbsp;</td>\n");
   fprintf(fhtml, "<td bgcolor=\"#6ba5d7\">&nbsp;</td>\n");
   fprintf(fhtml, "</tr>\n");
   fprintf(fhtml, "<tr>\n");
   fprintf(fhtml, "<td colspan=6>\n");
   fprintf(fhtml, "<table cellpadding=\"5\" border=\"1\">\n");
   fprintf(fhtml, "  <tr>\n");
   fprintf(fhtml, "    <td colspan=3 align=\"center\" bgcolor=\"#669999\">\n");
   fprintf(fhtml, "      <font size=\"+1\" color=\"ffffff\">\n");
   fprintf(fhtml, "        %s\n", label);
   fprintf(fhtml, "      </font>\n");
   fprintf(fhtml, "    </td>\n");
   fprintf(fhtml, "  </tr>\n");
   fprintf(fhtml, "  <tr>\n");
   fprintf(fhtml, "    <td colspan=\"3\" bgcolor=\"#d3d3d3\">\n");
   fprintf(fhtml, "      <center>\n");
   fprintf(fhtml, "        <img src=%s/mosaic.jpg width=600 alt=\"Region mosaic image\"><br>\n", urlbase);
   fprintf(fhtml, "      </center>\n");
   fprintf(fhtml, "    </td>\n");
   fprintf(fhtml, "   </tr>\n");
   fprintf(fhtml, "   <tr>\n");
   fprintf(fhtml, "     <td bgcolor=\"#d3d3d3\">\n");
   fprintf(fhtml, "      <center>\n");
   fprintf(fhtml, "      <a href=%s/mosaic.fits>FITS file</a>&nbsp;&nbsp;&nbsp;\n", fitsurl);
   fprintf(fhtml, "      <a href=\"http://irsa.ipac.caltech.edu/cgi-bin/OasisLink/nph-oasislink?ref=%s%%2Fmosaic.fits\">\n", urlcoded);
   fprintf(fhtml, "         <img align=middle src=http://irsa.ipac.caltech.edu/applications/Oasis/images/oasislink.gif alt=\"Send to Oasis\"></a>\n");
   fprintf(fhtml, "      </center>\n");
   fprintf(fhtml, "     </td>\n");
   fprintf(fhtml, "     </td>\n");
   fprintf(fhtml, "     <td bgcolor=\"#d3d3d3\">\n");
   fprintf(fhtml, "      <center>\n");
   fprintf(fhtml, "      <a href=\"%s/region.hdr\"  target=\"montageinfo\">FITS header</a>\n", urlbase);
   fprintf(fhtml, "      </center>\n");
   fprintf(fhtml, "     </td>\n");
   fprintf(fhtml, "     <td bgcolor=\"#d3d3d3\">\n");
   fprintf(fhtml, "      <center>\n");
   fprintf(fhtml, "      <a href=%s/rimages.tbl>Image metadata</a>&nbsp;&nbsp;&nbsp;&nbsp;\n", urlbase);
   fprintf(fhtml, "      <a href=\"http://irsa.ipac.caltech.edu/cgi-bin/OasisLink/nph-oasislink?ref=%s%%2Frimages.tbl\" target=\"montageinfo\">\n", urlcoded);
   fprintf(fhtml, "         <img align=middle src=http://irsa.ipac.caltech.edu/applications/Oasis/images/oasislink.gif alt=\"Send to Oasis\"></a>\n");
   fprintf(fhtml, "      </center>\n");
   fprintf(fhtml, "  </tr>\n");
   fprintf(fhtml, "</table><p>\n");
   fprintf(fhtml, "\n");
   fprintf(fhtml, "<center>\n");
   fprintf(fhtml, "<font size=-1><i>This mosaic took %d seconds.</i></font><p>\n", (int)(currtime - start));
   fprintf(fhtml, "\n");
   fprintf(fhtml, "For more information on using Oasis for data display and analysis, click <a href=\"http://montage.ipac.caltech.edu/docs/oasis.html\" target=\"oasisinfo\"> here </a><p>\n");
   fprintf(fhtml, "<a href=%s/msg.html>Processing history<a>\n", urlbase);
   fprintf(fhtml, "\n");
   fprintf(fhtml, "</td>\n");
   fprintf(fhtml, "</tr>\n");
   fprintf(fhtml, "</table>\n");
   fprintf(fhtml, "</center>\n");
   fprintf(fhtml, "</body>\n");
   fprintf(fhtml, "</html>\n");

   fclose(fhtml);


   /**********************************/ 
   /* Copy everything to SRB at SDSC */
   /**********************************/ 

   fprintf(fmsg, "\nData copied to permanent store\n");
   fflush(fmsg);

   sprintf(env0, "srbAuth=xxxxxx");                             putenv(env0);
   sprintf(env1, "mdasCollectionName=/NVOzone/home/jcg.nvo");   putenv(env1);
   sprintf(env2, "mdasCollectionHome=/NVOzone/home/jcg.nvo");   putenv(env2);
   sprintf(env3, "mdasDomainName=nvo");                         putenv(env3);
   sprintf(env4, "srbUser=jcg");                                putenv(env4);
   sprintf(env5, "srbHost=tgsrb.sdsc.edu");                     putenv(env5);
   sprintf(env6, "srbPort=8833");                               putenv(env6);
   sprintf(env7, "mcatZone=NVOzone");                           putenv(env7);
   sprintf(env8, "defaultResource=sf1-nvo");                    putenv(env8);

   if(debug)
   {
      fprintf(fmsg, "\n");
      fprintf(fmsg, "mdasCollectionName = \"%s\"\n", getenv("mdasCollectionName"));
      fprintf(fmsg, "mdasCollectionHome = \"%s\"\n", getenv("mdasCollectionHome"));
      fprintf(fmsg, "mdasDomainName     = \"%s\"\n", getenv("mdasDomainName"));
      fprintf(fmsg, "srbUser            = \"%s\"\n", getenv("srbUser"));
      fprintf(fmsg, "srbHost            = \"%s\"\n", getenv("srbHost"));
      fprintf(fmsg, "srbPort            = \"%s\"\n", getenv("srbPort"));
      fprintf(fmsg, "mcatZone           = \"%s\"\n", getenv("mcatZone"));
      fprintf(fmsg, "defaultResource    = \"%s\"\n", getenv("defaultResource"));
      fprintf(fmsg, "srbAuth            = \"%s\"\n", getenv("srbAuth"));
      fprintf(fmsg, "\n");
      fflush(fmsg);
   }

   chdir("../");

   sprintf(cmd, "sinit.sh");

   if(debug)
   {
      fprintf(fmsg, "[%s]\n", cmd);
      fflush(fmsg);
   }

   svc_run(cmd);

   strcpy( status, svc_value( "stat" ));

   if (strcmp( status, "ERROR") == 0)
   {
      sprintf( msg, "Sorry, the results storage system at SDSC is now unavailable [Error %s]",
         svc_value("errorcode"));
      printerr(msg);
   }
      
   sprintf(cmd, "sput.sh -r %s %s", subdir, subdir);

   if(debug)
   {
      fprintf(fmsg, "[%s]\n", cmd);
      fflush(fmsg);
   }

   svc_run(cmd);

   strcpy( status, svc_value( "stat" ));

   if (strcmp( status, "ERROR") == 0)
   {
      sprintf( msg, "Sorry, the results storage system at SDSC is not accepting downloads unavailable [Error %s]",
         svc_value("errorcode"));
      printerr(msg);
   }
      
   sprintf(cmd, "schmod.sh -r r public nvo /NVOzone/home/jcg.nvo/%s", subdir);

   if(debug)
   {
      fprintf(fmsg, "[%s]\n", cmd);
      fflush(fmsg);
   }

   svc_run(cmd);

   strcpy( status, svc_value( "stat" ));

   if (strcmp( status, "ERROR") == 0)
   {
      sprintf( msg, "Sorry, the results storage system at SDSC is not responding [Error %s]",
         svc_value("errorcode"));
      printerr(msg);
   }
      
   sprintf(cmd, "sexit.sh");

   if(debug)
   {
      fprintf(fmsg, "[%s]\n", cmd);
      fflush(fmsg);
   }

   svc_run(cmd);

   strcpy( status, svc_value( "stat" ));

   if (strcmp( status, "ERROR") == 0)
   {
      sprintf( msg, "There is some problem with the results storage system at SDSC. Some data may be compromised [Error %s]",
         svc_value("errorcode"));
      printerr(msg);
   }
      
   sprintf(msg, "Data copied to permanent store");

   sprintf(cmd, "mNotifyTG %s \"%s\"",
      logaddr, msg);

   if(debug)
   {
      fprintf(fmsg, "[%s]\n", cmd);
      fflush(fmsg);
   }

   svc_run(cmd);


   /**************************************/ 
   /* Delete everything if so instructed */
   /**************************************/ 

   if(deleteAll)
   {
      sprintf(cmd, "rm -rf %s", workspace);

      if(debug)
      {
	 fprintf(fmsg, "%s\n", cmd);
	 fflush(fmsg);
      }

      system(cmd);
   }


   /*************/
   /* Finish up */
   /*************/

   time(&currtime);

   fprintf(fmsg, "\nMosaic run complete. Time=%d\n\n", 
	  (int)(currtime - start));
   fprintf(fmsg, "</pre>\n");
   fprintf(fmsg, "</body>\n");
   fprintf(fmsg, "</html>\n");
   fflush(fmsg);

   printf("[struct stat=\"OK\", workspace=\"%s\"]\n", 
      workspace);
   fflush(stdout);

   sprintf(msg, "Processing took %d seconds", 
      (int)(currtime - start));

   sprintf(cmd, "mNotifyTG %s \"%s\"",
      logaddr, msg);

   if(debug)
   {
      fprintf(fmsg, "[%s]\n", cmd);
      fflush(fmsg);
   }

   svc_run(cmd);

   exit(0);
}


int printerr(char *str)
{
   char cmd   [MAXLEN];
   char msgstr[MAXLEN];

   if(debug)
   {
      fprintf(fmsg, "\nprinterr():\n");
      fflush(fmsg);
   }

   fprintf(fmsg, "ERROR: %s\n", str);
   fflush(fmsg);

   sprintf(msgstr, "ERROR: %s", str);

   sprintf(cmd, "mNotifyTG %s \"%s\"",
      logaddr, msgstr);

   if(debug)
   {
      fprintf(fmsg, "[%s]\n", cmd);
      fflush(fmsg);
   }

   svc_run(cmd);

   exit(1);
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



/***********************************/
/*                                 */
/*  Print out FITS library errors  */
/*                                 */
/***********************************/

int FITSerror(char *fname, int status)
{
   char status_str[FLEN_STATUS];

   fits_get_errstatus(status, status_str);

   sprintf(msg, "FITS error: %s (%d); File = %s",
      status_str, status, fname);

   printerr(msg);
   return(0);
}
