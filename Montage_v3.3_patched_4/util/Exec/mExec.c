#include <stdio.h>
#include <stdlib.h>
#include <string.h>
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

#define MAXLEN   4096
#define BUFSIZE 32769
#define MAXHDR  80000
#define MAXSOCK  4096

#define INTRINSIC 0
#define COMPUTED  1
#define FAILED    2

extern char *optarg;
extern int optind, opterr;

extern int getopt(int argc, char *const *argv, const char *options);

char *mktemp       (char *template);
int   debugCheck   (char *debugStr);

char *url_encode   ();
char *svc_value    ();
char *keyword_value();
char *filePath     ();
int   printerr     (char *str);
int   stradd       (char *header, char *card);
int   FITSerror    (char *fname, int status);

static time_t currtime, start, lasttime;

int debug;

FILE *fdebug;

char msg [MAXLEN];

static struct TBL_INFO *imgs;
static struct TBL_INFO *corrs;

static int  iidcorr;
static int  ifile;

static int  idcorr;
static char corrfile[MAXLEN];

static int  icntr;
static int  ia;
static int  ib;
static int  ic;

static int  cntr;
static char astr[MAXLEN];
static char bstr[MAXLEN];
static char cstr[MAXLEN];

int nextImg();
int nextCorr();


/*************************************************************************/
/*                                                                       */
/*                                                                       */
/*  MEXEC  --  Mosaicking executive for 2MASS, SDSS, DSS.                */
/*             Includes remote data and metadata access.                 */
/*             Alternatively, can mosaic preexisting user                */
/*             data.                                                     */
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
/*                   (If working with existing data, these two           */
/*                    parameters must be omitted)                        */
/*                                                                       */
/*   template.hdr    FITS header template for mosaic                     */
/*                                                                       */
/*  [workspace]      Directory where we can create working stuff.        */
/*                   Best if it is empty.                                */
/*                                                                       */
/*                                                                       */
/*  If no workspace is given, a unique local subdirectory will be        */
/*  created (e.g.; ./MOSAIC_AAAaa17v).  If you are going to use this     */
/*  it is best to run the program where there is space for all the       */
/*  intermediate files and you should also consider using the            */
/*  -o option.                                                           */
/*                                                                       */
/*                                                                       */
/*                                                                       */
/*  Additional controls:                                                 */
/*                                                                       */
/*  ------------    ------------  ------------------------               */
/*   Flag             Default      Description                           */
/*  ------------    ------------  ------------------------               */
/*                                                                       */
/*  -r rawdir        none         Location of user-supplied images       */
/*                                 images to mosaic                      */
/*  -f region.hdr    none         FITS header file                       */
/*  -h headertext    none         FITS header as a text string           */
/*                                 (either -f or -h must exist)          */
/*  -n tilecount     no tiling    Make an NxM set of tiles instead of    */
/*  -m tilecount                   one big mosaic (and no JPEGs)         */
/*  -s factor        1 (none)     "Shrink" the input image pixels prior  */
/*                                 to reprojection.  Saves time if the   */
/*                                 output is much lower resolution than  */
/*                                 the original data.                    */
/*  -e pixelerror    0.1          Set maximum mTANHdr pixel error to be  */
/*                                 allowed (abort if error is larger)    */
/*  -a               0 (false)    Don't run mSubset (get all images in   */
/*                                 expanded region)                      */
/*  -i               0 (false)    Emit ROME-friendly info messages       */
/*  -l               0 (false)    Background matching adjust levels only */
/*  -k               0 (false)    Keep all working files                 */
/*  -c               0 (false)    Delete everything. Pointless unless    */
/*                                 used with 'savefile'.  Ignored if     */
/*                                 tiling.  If the raw data is user-     */
/*                                 supplied, it is never deleted.        */
/*  -o savefile      none         Location to save mosaic.  This can't   */
/*                                 be used when tiling (do your own      */
/*                                 moving/cleanup)                       */
/*  -d level         0 (none)     Debugging output                       */
/*  -D filename      none         File for debug output                  */
/*                                 (otherwise it goes to stdout)         */
/*  -L labeltext     none         Label text used in HTML                */
/*  -O loctext       none         Location string text                   */
/*  -M contact       none         "Contact" string text                  */
/*  -x               0 (false)    Add a location marker to the JPEG      */
/*                                                                       */
/*                                                                       */
/*  So minimal calls would look like:                                    */
/*                                                                       */
/*                                                                       */
/*           mExec -f region.hdr 2MASS J region.hdr                      */
/*  or                                                                   */
/*           mExec -f region.hdr -r mydata region.hdr                    */
/*                                                                       */
/*                                                                       */
/*     The first would produce a mosaic (called mosaic.fits) of 2MASS    */
/*     J band data in a unique subdirectory.  The region.hdr file can be */
/*     generated with mHdr.  All other files will be cleaned up when the */
/*     processing is done.                                               */
/*                                                                       */
/*     The second will produce a mosaic of whatever is in subdirectory   */
/*     "mydata".                                                         */
/*                                                                       */
/*  To produce specifically named output mosaic                          */
/*  and clean up all the intermediate files:                             */
/*                                                                       */
/*                                                                       */
/*           mExec -f region.hdr -o region.fits -c 2MASS J workspace     */
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
   int    i, j, k, ch, index, baseCount, count, sys, ival;
   int    ncols, ifname, failed, nocorrection, nooverlap, istat;
   int    flag, noverlap, nimages, ntile, mtile;
   int    naxis1, naxis2, naxismax, nxtile, nytile;
   int    intan, outtan, iscale, ncell, local2MASS;
   int    keepAll, deleteAll, noSubset, infoMsg, levelOnly;
   int    userRaw, showMarker;

   double val, factor, shrink;

   struct WorldCoor *wcs, *wcsin;
   double epoch;

   fitsfile *infptr;

   int    fitsstat = 0;

   char   fheader[28800];
   char  *inheader;

   char   temp   [MAXLEN];
   char   buf    [BUFSIZE];
   char   cwd    [MAXLEN];

   int    icntr1;
   int    icntr2;
   int    ifname1;
   int    ifname2;
   int    idiffname;

   int    nmatches;

   int    cntr1;
   int    cntr2;

   char   fname1     [MAXLEN];
   char   fname2     [MAXLEN];
   char   diffname   [MAXLEN];
   char   areafile   [MAXLEN];
   char   survey     [MAXLEN];
   char   hostName   [MAXLEN];

   char   hdrfile    [MAXLEN];
   char   hdrtext    [MAXLEN];
   char   outstr     [MAXLEN];
   char   savefile   [MAXLEN];
   char   rawdir     [MAXLEN];
   char   datadir    [MAXLEN];
   char   scale_str  [MAXLEN];
   char   debugFile  [MAXLEN];
   char   labelText  [MAXLEN];
   char   locText    [MAXLEN];
   char   contactText[MAXLEN];

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
   char   status    [MAXLEN];
   char   infile    [MAXLEN];
   char   outfile   [MAXLEN];
   char   path      [MAXLEN];
   char   goodFile  [MAXLEN];

   char   locstr    [MAXLEN];
   char   radstr    [MAXLEN];

   char   imgsort  [MAXLEN];
   char   corrsort [MAXLEN];
   char   template [MAXLEN];
   char   tmpfile  [MAXLEN];
   char   workspace[MAXLEN];

   FILE  *fhdr;
   FILE  *bhdr;

   double allowedError;

   double ra[4], dec[4];
   double rac, decc;
   double x1, y1, z1;
   double x2, y2, z2;
   double xpos, ypos;
   double dtr;

   int    rflag, dflag;

   int    rh, rm, dd, dm;
   double rs, ds;

   allowedError = 0.1;

   dtr = atan(1.)/45.;

   inheader = malloc(MAXHDR);

   getcwd(cwd, MAXLEN);

   gethostname(hostName, MAXLEN);


   /************************/
   /* Initialization stuff */
   /************************/

   time(&currtime);

   start    = currtime;
   lasttime = currtime;

   svc_sigset();


   /*****************************/
   /* Read the input parameters */
   /*****************************/

   strcpy(workspace, "");
   strcpy(savefile,  "");
   strcpy(tmpfile,   "");
   strcpy(hdrfile,   "");
   strcpy(hdrtext,   "");
   strcpy(debugFile, "");
   strcpy(labelText, "");
   strcpy(locText,   "");
        
   noSubset   = 0;
   showMarker = 0;
   infoMsg    = 0;
   keepAll    = 0;
   deleteAll  = 0;
   levelOnly  = 0;
   userRaw    = 0;
   ntile      = 0;
   mtile      = 0;
   local2MASS = 0;

   shrink     = 1.0;

   strcpy(rawdir, "raw");

   debug  = 0;
   opterr = 0;

   while ((ch = getopt(argc, argv, "ilkcaxSh:f:o:d:D:e:r:s:n:m:L:O:M:")) != EOF)
   {
      switch (ch)
      {
         case 'a':
            noSubset = 1;
            break;

         case 'i':
            infoMsg = 1;
            break;

         case 'l':
            levelOnly = 1;
            break;

         case 'k':
            keepAll = 1;
            break;

         case 'c':
            deleteAll = 1;
            break;

         case 'x':
            showMarker = 1;
            break;

         case 'h':
            strcpy(hdrtext, optarg);
            break;

         case 'n':
            ntile = atoi(optarg);
            break;

         case 'm':
            mtile = atoi(optarg);
            break;

         case 'e':
	    allowedError = atof(optarg);
            break;

         case 'f':
            strcpy(hdrfile, optarg);
            break;

         case 'o':
            strcpy(tmpfile, optarg);
            break;

         case 'd':
            debug = debugCheck(optarg);
            break;

         case 'D':
            strcpy(debugFile, optarg);
            break;

         case 'L':
            strcpy(labelText, optarg);
            break;

         case 'O':
            strcpy(locText, optarg);
            break;

         case 'M':
            strcpy(contactText, optarg);
            break;

         case 'r':
            userRaw = 1;
            strcpy(rawdir, optarg);

            if(rawdir[0] != '/')
            {
               strcpy(temp, cwd);

               if(temp[strlen(temp)-1] != '/')
                  strcat(temp, "/");

               if(strlen(rawdir) == 0)
                  temp[strlen(temp)-1] = '\0';
               else
                  strcat(temp, rawdir);
               
               strcpy(rawdir, temp);
            }

            if(rawdir[strlen(rawdir) - 1] == '/')
               rawdir[strlen(rawdir) - 1]  = '\0';

            break;

         case 's':
	    shrink = atof(optarg);

	    if(shrink <= 0.0)
	       shrink = 1.;

            break;

	 case 'S':
	    local2MASS = 1;
	    break;

         default:
            printf("[struct stat=\"ERROR\", msg=\"Usage: %s [-r rawdir][-n ntilex][-m ntiley][-l(evel only)][-k(eep all)][-c(lean)][-s shrinkFactor][-o output.fits][-d(ebug) level][-f region.hdr | -h header] survey band [workspace-dir]\"]\n", argv[0]);
            exit(1);
            break;
      }
   }

   if(infoMsg)
   {
      printf("[struct stat=\"INFO\", msg=\"Compute node: %s\"]\n",  hostName);
      fflush(stdout);
   }

   if(ntile > 1 && mtile < 1) mtile = ntile;
   if(mtile > 1 && ntile < 1) ntile = mtile;

   if(ntile < 1 && mtile < 1) 
   {
      ntile = 1;
      mtile = 1;
   }

   if(strlen(tmpfile) > 0)
      strcpy(savefile, filePath(cwd, tmpfile));

   if(ntile*mtile > 1)
   {
      if(infoMsg)
      {
	 printf("[struct stat=\"INFO\", msg=\"Tiling %d x %d\"]\n",  ntile, mtile);
	 fflush(stdout);
      }

      strcpy(savefile, "");
      deleteAll = 0;
   }

   if (!userRaw && argc - optind < 2)
   {
      printf("[struct stat=\"ERROR\", msg=\"Usage: %s [-r rawdir][-n ntile][-m ntiley][-l(evel only)][-k(eep all)][-c(lean)][-s shrinkFactor][-o output.fits][-d(ebug) level][-f region.hdr | -h header] survey band [workspace-dir]\"]\n", argv[0]);
      exit(1);
   }

   if(userRaw)
   {
      if(argc > optind)
         strcpy(workspace, argv[optind]);
   }
   else
   {
      strcpy(survey,  argv[optind]);
      strcpy(band,    argv[optind+1]);

      if(argc - optind > 2)
         strcpy(workspace, argv[optind+2]);
   }

   if(strlen(workspace) == 0)
   {
      strcpy(template, "MOSAIC_XXXXXX");
      strcpy(workspace, mktemp(template));
   }

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

   if(debug)
   {
      fdebug = stdout;

      if(strlen(debugFile) > 0)
      {
         fdebug = fopen(debugFile, "w+");

         if(fdebug == (FILE *)NULL)
         {
            printf("[struct stat=\"ERROR\", msg=\"Invalid debug file [%s]", debugFile);
            exit(1);
         }
      }
   }


   /******************************************************************/
   /* Make sure the workspace directory of the workspace exists.     */
   /* Create it if it doesn't.                                       */
   /******************************************************************/

   mkdir(workspace, 0775);


   
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

      for(j=0; j<=strlen(hdrtext); ++j)
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

   if(debug >= 1)
   {
     fprintf(fdebug, "\n\nINPUT PARAMETERS:\n\n");
     fprintf(fdebug, "survey      = [%s]\n",  survey);
     fprintf(fdebug, "band        = [%s]\n",  band);
     fprintf(fdebug, "hdrfile     = [%s]\n",  hdrfile);
     fprintf(fdebug, "hdrtext     =  %d characters\n",  strlen(hdrtext));
     fprintf(fdebug, "workspace   = [%s]\n",  workspace);
     fprintf(fdebug, "levelOnly   =  %d\n",   levelOnly);
     fprintf(fdebug, "keepAll     =  %d\n",   keepAll);
     fprintf(fdebug, "deleteAll   =  %d\n\n", deleteAll);
     fprintf(fdebug, "cwd         = [%s]\n",  cwd);
     fflush(fdebug);
   }


   /*************************/
   /* Create subdirectories */
   /*************************/

   if(debug >= 4)
   {
      fprintf(fdebug, "chdir to [%s]\n", workspace);
      fflush(fdebug);
   }

   chdir(workspace);

   flag = 0;

   if(!userRaw)
   {
      if(mkdir(rawdir, 0775) < 0)
         flag = 1;
   }

   if(mkdir("projected", 0775) < 0)
      flag = 1;

   if(mkdir("diffs", 0775) < 0)
      flag = 1;

   if(mkdir("corrected", 0775) < 0)
      flag = 1;

   if(shrink != 1)
   {
      if(mkdir("shrunken", 0775) < 0)
         flag = 1;
   }

   if(ntile*mtile > 1)
   {
      if(mkdir("tiles", 0775) < 0)
         flag = 1;

      if(mkdir("tmp", 0775) < 0)
         flag = 1;
   }

   if(flag)
      printerr("Can't create proper subdirectories in workspace (may already exist)");
   



   /***********************************************/
   /* Create the WCS using the header template    */
   /***********************************************/

   fhdr = fopen("region.hdr", "r");

   if(fhdr == (FILE *)NULL)
      printerr("Can't open header template file");


   bhdr = fopen("big_region.hdr", "w+");

   if(bhdr == (FILE *)NULL)
      printerr("Can't open expanded header file: [big_region.hdr]");


   if(debug >= 1)
   {
      fprintf(fdebug, "\nHEADER:\n");
      fprintf(fdebug, "-----------------------------------------------------------\n");
      fflush(fdebug);
   }

   while(1)
   {
      if(fgets(temp, MAXLEN, fhdr) == (char *)NULL)
         break;

      if(temp[strlen(temp)-1] == '\n')
         temp[strlen(temp)-1] =  '\0';

      if(debug >= 1)
      {
         printf("%s\n", temp);
         fflush(stdout);
      }

      if(strncmp(temp, "NAXIS1", 6) == 0)
      {
         ival = atoi(temp+9);
         fprintf(bhdr, "NAXIS1  = %d\n", ival+3000);
         naxis1 = ival;
      }
      else if(strncmp(temp, "NAXIS2", 6) == 0)
      {
         ival = atoi(temp+9);
         fprintf(bhdr, "NAXIS2  = %d\n", ival+3000);
         naxis2 = ival;
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

   if(debug >= 1)
   {
      fprintf(fdebug, "-----------------------------------------------------------\n\n");
      fflush(fdebug);
   }

   naxismax = naxis1;

   if(naxis2 > naxismax)
      naxismax = naxis2;

   nxtile = (naxis1+ntile) / ntile;
   nytile = (naxis2+mtile) / mtile;

   if(debug >= 1 && ntile*mtile > 1)
   {
     fprintf(fdebug, "TILING: %dx%d tiles (%dx%d pixels each)\n\n", 
        ntile, mtile, nxtile, nytile);
     fflush(fdebug);
   }

   fclose(fhdr);
   fclose(bhdr);


   /*********************************/
   /* Find the corners of this area */
   /* and the diagonal size         */
   /*********************************/

   wcs = wcsinit(fheader);

   if(wcs == (struct WorldCoor *)NULL)
   {
      printf("[struct stat=\"ERROR\", msg=\"Output wcsinit() failed.\"]\n");
      exit(1);
   }


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

   if(debug >= 2)
   {
      fprintf(fdebug, "Scale = %-g\n", scale);
      fflush(fdebug);
   }


   /* Generate the location string */

   pix2wcs(wcs, wcs->nxpix/2., wcs->nypix/2., &xpos, &ypos);

   degreeToHMS(xpos, 2, &rflag, &rh, &rm, &rs);
   degreeToDMS(ypos, 1, &dflag, &dd, &dm, &ds);

   if(wcs->syswcs == WCS_J2000)
   {
      if(dflag)
         sprintf(locstr, "%dh%02dm%05.2fs&nbsp;-%dd%02dm%04.1fs&nbsp;J2000", rh, rm, rs, dd, dm, ds);
      else
         sprintf(locstr, "%dh%02dm%05.2fs&nbsp;+%dd%02dm%04.1fs&nbsp;J2000", rh, rm, rs, dd, dm, ds);

      if(wcs->equinox == 1950)
      {
	 if(dflag)
	    sprintf(locstr, "%dh%02dm%05.2fs&nbsp;-%dd%02dm%04.1fs&nbsp;J1950", rh, rm, rs, dd, dm, ds);
	 else
	    sprintf(locstr, "%dh%02dm%05.2fs&nbsp;+%dd%02dm%04.1fs&nbsp;J1950", rh, rm, rs, dd, dm, ds);
      }
   }
   else if(wcs->syswcs == WCS_B1950)
   {
      if(dflag)
         sprintf(locstr, "%dh%02dm%05.2fs&nbsp;-%dd%02dm%04.1fs&nbsp;B1950", rh, rm, rs, dd, dm, ds);
      else
         sprintf(locstr, "%dh%02dm%05.2fs&nbsp;+%dd%02dm%04.1fs&nbsp;B1950", rh, rm, rs, dd, dm, ds);


      if(wcs->equinox == 1950)
      {
	 if(dflag)
	    sprintf(locstr, "%dh%02dm%05.2fs&nbsp;-%dd%02dm%04.1fs&nbsp;B2000", rh, rm, rs, dd, dm, ds);
	 else
	    sprintf(locstr, "%dh%02dm%05.2fs&nbsp;+%dd%02dm%04.1fs&nbsp;B2000", rh, rm, rs, dd, dm, ds);
      }
   }
   else if(wcs->syswcs == WCS_GALACTIC)
   {
      sprintf(locstr, "%.4f %.4f Galactic", xpos, ypos);
   }
   else if(wcs->syswcs == WCS_ECLIPTIC)
   {
      sprintf(locstr, "%.4f %.4f Ecl J2000", xpos, ypos);

      if(wcs->equinox == 1950)
         sprintf(locstr, "%.4f %.4f Ecl J1950", xpos, ypos);
   }
   else  
   {
      if(dflag)
         sprintf(locstr, "%dh%02dm%05.2fs&nbsp;-%dd%02dm%04.1fs&nbsp;J2000", rh, rm, rs, dd, dm, ds);
      else
         sprintf(locstr, "%dh%02dm%05.2fs&nbsp;+%dd%02dm%04.1fs&nbsp;J2000", rh, rm, rs, dd, dm, ds);
   }


   /* Generate the size string */

   sprintf(radstr, "%.2f", fabs(wcs->nxpix * wcs->xinc));



   /*************************************/
   /* Get the image list for the region */
   /* of interest                       */
   /*************************************/

   if(!userRaw)
   {
      if(infoMsg)
      {
         printf("[struct stat=\"INFO\", msg=\"Computing image coverage list\"]\n");
         fflush(stdout);
      }

      time(&currtime);

      lasttime = currtime;

      scale = scale * 1.42;
    
      if(noSubset)
	 sprintf(cmd, "mArchiveList %s %s \"%.4f %.4f eq j2000\" %.2f %.2f remote.tbl", 
	    survey, band, rac, decc, scale, scale);
      else
	 sprintf(cmd, "mArchiveList %s %s \"%.4f %.4f eq j2000\" %.2f %.2f remote_big.tbl", 
	    survey, band, rac, decc, scale, scale);

      if(debug >= 4)
      {
         fprintf(fdebug, "[%s]\n", cmd);
         fflush(fdebug);
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
         sprintf( msg, "%s/%s has no data covering area", survey, band);

         printerr(msg);
      }

      time(&currtime);

      if(debug >= 1)
      {
         fprintf(fdebug, "TIME: mArchiveList     %6d (%d images)\n", (int)(currtime - lasttime), nimages);
         fflush(fdebug);
      }

      lasttime = currtime;


      /**************************************/
      /* Shrink it down to the exact region */
      /**************************************/

      if(!noSubset)
      {
	 sprintf(cmd, "mSubset remote_big.tbl region.hdr remote.tbl"); 

	 if(debug >= 4)
	 {
	    fprintf(fdebug, "[%s]\n", cmd);
	    fflush(fdebug);
	 }

	 svc_run(cmd);

	 strcpy( status, svc_value( "stat" ));

	 if (strcmp( status, "ERROR") == 0)
	 {
	    strcpy( msg, svc_value( "msg" ));

	    printerr(msg);
	 }
	    
	 nimages = atof(svc_value("nmatches"));

	 if (nimages == 0)
	 {
	    sprintf( msg, "%s has no data covering this area", survey);

	    printerr(msg);
	 }

	 time(&currtime);

	 if(debug >= 1)
	 {
	    fprintf(fdebug, "TIME: mSubset          %6d (%d images)\n",
	       (int)(currtime - lasttime), nimages);
	    fflush(fdebug);
	 }

	 lasttime = currtime;
      }


      /******************/
      /* Get the images */
      /******************/

      if(debug >= 4)
      {
         fprintf(fdebug, "chdir to [%s]\n", rawdir);
         fflush(fdebug);
      }

      chdir(rawdir);

      if(!userRaw)
      {
         if(infoMsg)
         {
            printf("[struct stat=\"INFO\", msg=\"Retrieving %d images\"]\n", nimages);
            fflush(stdout);
         }

 	 if(local2MASS)
 	    sprintf(cmd, "mArchiveExec -S ../remote.tbl");
 	 else
 	    sprintf(cmd, "mArchiveExec ../remote.tbl");

         if(debug >= 4)
         {
            fprintf(fdebug, "[%s]\n", cmd);
            fflush(fdebug);
         }

         svc_run(cmd);

         strcpy(status, svc_value( "stat" ));

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
            strcpy( msg, "No data was available for the region specified at this time");

            printerr(msg);
         }

         time(&currtime);

         if(debug >= 1)
         {
            fprintf(fdebug, "TIME: mArchiveExec     %6d (%d images)\n", (int)(currtime - lasttime), nimages);
            fflush(fdebug);
         }

         lasttime = currtime;

         if(debug >= 4)
         {
            fprintf(fdebug, "chdir to [%s]\n", workspace);
            fflush(fdebug);
         }

         chdir(workspace);
      }
   }


   

   /********************************************/ 
   /* Create and open the raw image table file */
   /********************************************/ 

   if(debug >= 4)
   {
      fprintf(fdebug, "chdir to [%s]\n", rawdir);
      fflush(fdebug);
   }

   chdir(rawdir);
    
   sprintf(cmd, "mImgtbl . rimages.tbl");

   if(debug >= 4)
   {
      fprintf(fdebug, "[%s]\n", cmd);
      fflush(fdebug);
   }

   svc_run(cmd);

   nimages = atof(svc_value("count"));

   if (nimages == 0)
   {
      sprintf( msg, "%s/%s has no data covering area", survey, band);

      printerr(msg);
   }

   if(infoMsg)
   {
      printf("[struct stat=\"INFO\", msg=\"Reprojecting %d images\"]\n", nimages);
      fflush(stdout);
   }

   if(debug >= 4)
   {
      fprintf(fdebug, "chdir to [%s]\n", workspace);
      fflush(fdebug);
   }

   chdir(workspace);

   sprintf(cmd, "mv %s/rimages.tbl .", rawdir);

   if(debug >= 4)
   {
      fprintf(fdebug, "[%s]\n", cmd);
      fflush(fdebug);
   }

   system(cmd);

   if(userRaw)
   {
      ncols = topen("rimages.tbl");

      ifname = tcol( "fname");

      if(ifname < 0)
         printerr("Need column 'fname' in input");
   }
   else
   {
      ncols = topen("remote.tbl");

      ifname = tcol( "file");

      if(ifname < 0)
         printerr("Need column 'file' in input");
   }

   iscale = tcol("scale");

   time(&currtime);

   if(debug >= 1)
   {
      fprintf(fdebug, "TIME: mImgtbl(raw)     %6d\n", (int)(currtime - lasttime));
      fflush(fdebug);
   }

   lasttime = currtime;


   /*************************************************/ 
   /* Try to generate an alternate header so we can */
   /* use the fast projection                       */
   /*************************************************/ 

   outtan = INTRINSIC;

   if(wcs->prjcode != WCS_TAN
   && wcs->prjcode != WCS_SIN
   && wcs->prjcode != WCS_ZEA
   && wcs->prjcode != WCS_STG
   && wcs->prjcode != WCS_ARC)
   {
      sprintf(cmd, "mTANHdr big_region.hdr altout.hdr");

      if(debug >= 4)
      {
         fprintf(fdebug, "[%s]\n", cmd);
         fflush(fdebug);
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

         error = atof(svc_value("revxerr"));

         if(error > maxerror)
            maxerror = error;

         error = atof(svc_value("revyerr"));

         if(error > maxerror)
            maxerror = error;

         if(debug >= 2)
         {
            fprintf(fdebug, "   Distorted TAN for output: max error = %-g, allowed error = %-g\n", 
	       maxerror, allowedError);
            fflush(fdebug);
         }

         if(maxerror > allowedError)
            outtan = FAILED;
      }

      time(&currtime);

      if(debug >= 1)
      {
         fprintf(fdebug, "TIME: mTANHdr          %6d\n", (int)(currtime - lasttime));
         fflush(fdebug);
      }

      lasttime = currtime;
   }

   
   /*************************************************/ 
   /* If we are shrinking the images beforehand, do */
   /* it now.                                       */
   /*************************************************/ 

   strcpy(datadir, rawdir);

   if(shrink != 1.)
   {
      while(1)
      {
	 istat = tread();

	 if(istat < 0)
	    break;

	 strcpy ( infile, tval(ifname));

	 sprintf(cmd, "mShrink %s/%s shrunken/%s %-g", 
	    rawdir, infile, infile, shrink);

	 if(debug >= 4)
	 {
	    fprintf(fdebug, "[%s]\n", cmd);
	    fflush(fdebug);
	 }

	 svc_run(cmd);

	 strcpy( status, svc_value( "stat" ));

	 if(strcmp( status, "ERROR") == 0)
	 {
	    strcpy( msg, svc_value( "msg" ));

	    printerr(msg);
	 }
      }

      tseek(0);

      strcpy(datadir, "shrunken");

      time(&currtime);

      if(debug >= 1)
      {
         fprintf(fdebug, "TIME: mShrink          %6d (%d images)\n", (int)(currtime - lasttime), nimages);
         fflush(fdebug);
      }

      lasttime = currtime;
   }


   /*************************************************/ 
   /* Read the records and call mProject/mProjectPP */
   /*************************************************/ 

   index     = 0;
   failed    = 0;
   nooverlap = 0;

   while(1)
   {
      istat = tread();

      if(istat < 0)
         break;

      strcpy ( infile, tval(ifname));
      strcpy (outfile, infile);

      if(strlen(outfile) > 3 && strcmp(outfile+strlen(outfile)-3, ".gz") == 0)
         *(outfile+strlen(outfile)-3) = '\0';

      if(strlen(outfile) > 4 && strcmp(outfile+strlen(outfile)-4, ".fit") == 0)
         strcat(outfile, "s");


      if(strlen(outfile) > 5 &&
	 strncmp(outfile+strlen(outfile)-5, ".FITS", 5) == 0)
	    outfile[strlen(outfile)-5] = '\0';

      else if(strlen(outfile) > 5 &&
	 strncmp(outfile+strlen(outfile)-5, ".fits", 5) == 0)
	    outfile[strlen(outfile)-5] = '\0';

      else if(strlen(outfile) > 4 &&
	 strncmp(outfile+strlen(outfile)-4, ".FIT", 4) == 0)
	    outfile[strlen(outfile)-4] = '\0';

      else if(strlen(outfile) > 4 &&
	 strncmp(outfile+strlen(outfile)-4, ".fit", 4) == 0)
	    outfile[strlen(outfile)-4] = '\0';

      strcat(outfile, ".fits");

      if(iscale < 0)
         sprintf(scale_str, "1.0");
      else
         strcpy(scale_str, tval(iscale));


      /* Try to generate an alternate input header so we can */
      /* use the fast projection                             */

      intan = INTRINSIC;

      strcpy(path, filePath(datadir, infile));

      fitsstat = 0;
      if(fits_open_file(&infptr, path, READONLY, &fitsstat))
         continue;

      fitsstat = 0;
      if(fits_get_image_wcs_keys(infptr, &inheader, &fitsstat))
         FITSerror(infile, fitsstat);

      fitsstat = 0;
      if(fits_close_file(infptr, &fitsstat))
         FITSerror(infile, fitsstat);

      wcsin = wcsinit(inheader);

      if(wcsin == (struct WorldCoor *)NULL)
      {
         strcpy(msg, "Bad WCS in input image");

         printerr(msg);
      }

      if(strcmp(wcsin->ptype, "TAN") != 0
      && strcmp(wcsin->ptype, "SIN") != 0
      && strcmp(wcsin->ptype, "ZEA") != 0
      && strcmp(wcsin->ptype, "STG") != 0
      && strcmp(wcsin->ptype, "ARC") != 0)
      {
         sprintf(cmd, "mGetHdr %s orig.hdr", path);

         if(debug >= 4)
         {
            fprintf(fdebug, "[%s]\n", cmd);
            fflush(fdebug);
         }

         svc_run(cmd);

         strcpy( status, svc_value( "stat" ));

         if(strcmp( status, "ERROR") == 0)
         {
            strcpy(msg, svc_value( "msg" ));

            printerr(msg);
         }

         sprintf(cmd, "mTANHdr orig.hdr altin.hdr");

         if(debug >= 4)
         {
            fprintf(fdebug, "[%s]\n", cmd);
            fflush(fdebug);
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

            if(debug)
            {
               fprintf(fdebug, "   Distorted TAN on input: max error = %-g, allowed error = %-g\n",
	          maxerror, allowedError);
               fflush(fdebug);
            }

            if(maxerror > allowedError)
               intan = FAILED;
         }
      }
      else
         intan = INTRINSIC;

      if(wcs->syswcs != wcsin->syswcs)
      {
          intan = FAILED;
         outtan = FAILED;

	 if(debug)
	 {
	    fprintf(fdebug, "   Can't use distorted TAN when projecting between coordinate systems.\n");
	    fflush(fdebug);
	 }
      }


      /* Now run mProject or mProjectPP (depending */
      /* on what we have to work with)             */

      if(     intan == COMPUTED  && outtan == COMPUTED )
         sprintf(cmd, "mProjectPP -b 1 -i altin.hdr -o altout.hdr -x %s -X %s/%s projected/%s big_region.hdr",
            scale_str, datadir, infile, outfile);

      else if(intan == COMPUTED  && outtan == INTRINSIC)
         sprintf(cmd, "mProjectPP -b 1 -i altin.hdr -x %s -X %s/%s projected/%s big_region.hdr",
            scale_str, datadir, infile, outfile);

      else if(intan == INTRINSIC && outtan == COMPUTED )
         sprintf(cmd, "mProjectPP -b 1 -o altout.hdr -x %s -X %s/%s projected/%s big_region.hdr",
            scale_str, datadir, infile, outfile);

      else if(intan == INTRINSIC && outtan == INTRINSIC)
         sprintf(cmd, "mProjectPP -b 1 -x %s -X %s/%s projected/%s big_region.hdr",
            scale_str, datadir, infile, outfile);

      else
         sprintf(cmd, "mProject -x %s -X %s/%s projected/%s big_region.hdr",
            scale_str, datadir, infile, outfile);

      if(debug >= 4)
      {
         fprintf(fdebug, "[%s]\n", cmd);
         fflush(fdebug);
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
         strcpy(goodFile, infile);

	 if(strlen(goodFile) > 3 && strcmp(goodFile+strlen(goodFile)-3, ".gz") == 0)
	    *(goodFile+strlen(goodFile)-3) = '\0';

	 if(debug >= 3)
	 {
	    fprintf(fdebug, "%s took %s seconds (%3d of %3d)\n", 
	       tval(ifname), svc_value("time"), index, nimages);
	    fflush(fdebug);
	 }
      }

      if(!keepAll && !userRaw)
      {
         strcpy(cmd, filePath(rawdir, infile));
         unlink(cmd);
      }
   }

   baseCount = index - failed - nooverlap;

   time(&currtime);

   if(debug >= 1)
   {
      if(intan == FAILED && outtan == FAILED)
         fprintf(fdebug, "TIME: mProject         %6d (%d successful, %d failed, %d no overlap)\n",
            (int)(currtime - lasttime), baseCount, failed, nooverlap);
      else
         fprintf(fdebug, "TIME: mProjectPP       %6d (%d successful, %d failed, %d no overlap)\n", 
            (int)(currtime - lasttime), baseCount, failed, nooverlap);
      fflush(fdebug);
   }

   lasttime = currtime;

   tclose();

   unlink("altin.hdr");
   unlink("altout.hdr");

   if(baseCount == 0)
      printerr("None of the images had data in this area");

   if(baseCount == 1)
   {
      sprintf(cmd, "mSubimage projected/%s mosaic.fits %.6f %.6f %.6f %.6f", 
         goodFile, rac, decc, fabs(wcs->nxpix * wcs->xinc), fabs(wcs->nypix * wcs->yinc));

      if(debug >= 4)
      {
         fprintf(fdebug, "[%s]\n", cmd);
         fflush(fdebug);
      }

      svc_run(cmd);

      sprintf(cmd, "mImgtbl projected pimages.tbl");

      if(debug >= 4)
      {
         fprintf(fdebug, "[%s]\n", cmd);
         fflush(fdebug);
      }

      svc_run(cmd);

      nimages = atof(svc_value("count"));

      if(nimages <= 0)
         printerr("None of the projected images were good.");
   }


   /*********************************/
   /* Generate the differences list */
   /*********************************/

   if(baseCount > 1)
   {
      sprintf(cmd, "mImgtbl projected pimages.tbl");

      if(debug >= 4)
      {
         fprintf(fdebug, "[%s]\n", cmd);
         fflush(fdebug);
      }

      svc_run(cmd);

      nimages = atof(svc_value("count"));

      if(nimages <= 0)
         printerr("None of the projected images were good.");

      time(&currtime);

      if(debug >= 1)
      {
         fprintf(fdebug, "TIME: mImgtbl(proj)    %6d\n", (int)(currtime - lasttime));
         fflush(fdebug);
      }

      lasttime = currtime;

      sprintf(cmd, "mOverlaps pimages.tbl diffs.tbl");

      if(debug >= 4)
      {
         fprintf(fdebug, "[%s]\n", cmd);
         fflush(fdebug);
      }

      svc_run(cmd);

      strcpy( status, svc_value( "stat" ));

      if(strcmp( status, "ERROR") == 0)
      {
         strcpy( msg, svc_value( "msg" ));

         printerr(msg);
      }

      noverlap = atof(svc_value("count"));

      if(infoMsg)
      {
          printf("[struct stat=\"INFO\", msg=\"Performing background correction analysis (%d overlaps)\"]\n", 
               noverlap);
         fflush(stdout);
      }

      time(&currtime);

      if(debug >= 1)
      {
         fprintf(fdebug, "TIME: mOverlaps        %6d (%d overlaps)\n", (int)(currtime - lasttime), noverlap);
         fflush(fdebug);
      }

      lasttime = currtime;
   }



   /***************************************/ 
   /* Open the difference list table file */
   /***************************************/ 

   if(baseCount > 1)
   {
      ncols = topen("diffs.tbl");

      if(ncols <= 0)
      {
         printf("[struct stat=\"ERROR\", msg=\"Invalid image metadata file: diffs.tbl\"]\n");
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

      fprintf(fout, "|   plus  |  minus  |         a      |        b       |        c       |    crpix1    |    crpix2    |   xmin   |   xmax   |   ymin   |   ymax   |   xcenter   |   ycenter   |    npixel   |      rms       |      boxx      |      boxy      |    boxwidth    |   boxheight    |     boxang     |\n");
      fflush(fout);

      while(1)
      {
         istat = tread();

         if(istat < 0)
            break;

         cntr1 = atoi(tval(icntr1));
         cntr2 = atoi(tval(icntr2));

         ++count;

         if(debug >= 3)
         {
            fprintf(fdebug, "Processing image difference %3d - %3d (%3d of %3d)\n", 
               cntr1+1, cntr2+1, count, noverlap);
            fflush(fdebug);
         }

         strcpy(fname1,   tval(ifname1));
         strcpy(fname2,   tval(ifname2));
         strcpy(diffname, tval(idiffname));

         sprintf(cmd, "mDiff projected/%s projected/%s diffs/%s big_region.hdr", fname1, fname2, diffname);

         if(debug >= 4)
         {
            fprintf(fdebug, "[%s]\n", cmd);
            fflush(fdebug);
         }

         svc_run(cmd);

         strcpy( status, svc_value( "stat" ));

         if(strcmp( status, "ABORT") == 0)
         {
            strcpy( msg, svc_value( "msg" ));

            printf("[struct stat=\"ERROR\", msg=\"%s\"]\n", msg);
            fflush(fdebug);

            exit(1);
         }

         if(strcmp( status, "ERROR"  ) == 0
         || strcmp( status, "WARNING") == 0)
            ++failed;


	 if(levelOnly)
	    sprintf(cmd, "mFitplane -l diffs/%s", diffname);
         else
	    sprintf(cmd, "mFitplane diffs/%s", diffname);

         if(debug >= 4)
         {
            fprintf(fdebug, "[%s]\n", cmd);
            fflush(fdebug);
         }

         svc_run(cmd);

         strcpy( status, svc_value( "stat" ));

         if(strcmp( status, "ABORT") == 0)
         {
            strcpy( msg, svc_value( "msg" ));

            printf("[struct stat=\"ERROR\", msg=\"%s\"]\n", msg);
            fflush(stdout);

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

            fprintf(fout, " %9d %9d %16.5e %16.5e %16.5e %14.2f %14.2f %10d %10d %10d %10d %13.2f %13.2f %13.0f %16.5e %16.1f %16.1f %16.1f %16.1f %16.1f \n",
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
      }

      tclose();

      time(&currtime);

      if(debug >= 1)
      {
         fprintf(fdebug, "TIME: mDiff/mFitplane  %6d (%d diffs,  %d successful, %d failed)\n", 
            (int)(currtime - lasttime), count, count - failed,  failed);

         fflush(fdebug);
      }

      lasttime = currtime;
   }


   /*********************************/
   /* Generate the correction table */
   /*********************************/

   if(baseCount > 1)
   {
      if(levelOnly)
         sprintf(cmd, "mBgModel -i 100000 -l -a pimages.tbl fits.tbl corrections.tbl");
      else
         sprintf(cmd, "mBgModel -i 100000 pimages.tbl fits.tbl corrections.tbl");

      if(debug >= 4)
      {
         fprintf(fdebug, "[%s]\n", cmd);
         fflush(fdebug);
      }

      svc_run(cmd);

      strcpy( status, svc_value( "stat" ));

      if(strcmp( status, "ERROR") == 0)
      {
         strcpy( msg, svc_value( "msg" ));

         printerr(msg);
      }

      time(&currtime);

      if(debug >= 1)
      {
         fprintf(fdebug, "TIME: mBgModel         %6d\n", (int)(currtime - lasttime));
         fflush(fdebug);
      }

      lasttime = currtime;
   }


   /**************************************/ 
   /* Background correct all the images  */
   /**************************************/ 

   if(baseCount > 1)
   {
      if(infoMsg)
      {
         printf("[struct stat=\"INFO\", msg=\"Background correcting %d images\"]\n", 
            nimages);
         fflush(stdout);
      }

      /**************************************************************/
      /* Make sorted copies of the image list and corrections table */
      /**************************************************************/

      sprintf(template, "corrected/IMGTBLXXXXXX");
      strcpy(imgsort, mktemp(template));

      sprintf(cmd, "mTblSort pimages.tbl cntr %s", imgsort);

      if(debug >= 4)
      {
         fprintf(fdebug, "[%s]\n", cmd);
         fflush(fdebug);
      }
    
      svc_run(cmd);

      strcpy(status, svc_value("stat"));
    
      if(strcmp( status, "ABORT") == 0
      || strcmp( status, "ERROR") == 0)
      {
         strcpy( msg, svc_value( "msg" ));
      
         printf("[struct stat=\"ERROR\", msg=\"%s\"]\n", msg);
         fflush(stdout);
      
         exit(1);
      }


      sprintf(template, "corrected/CORTBLXXXXXX");
      strcpy(corrsort, mktemp(template));

      sprintf(cmd, "mTblSort corrections.tbl id %s", corrsort);

      if(debug >= 4)
      {
         fprintf(fdebug, "[%s]\n", cmd);
         fflush(fdebug);
      }
    
      svc_run(cmd);
    
      strcpy(status, svc_value("stat"));
    
      if(strcmp( status, "ABORT") == 0
      || strcmp( status, "ERROR") == 0)
      {
         strcpy( msg, svc_value( "msg" ));
      
         printf("[struct stat=\"ERROR\", msg=\"%s\"]\n", msg);
         fflush(stdout);
      
         exit(1);
      }


      /********************************/ 
      /* Open the image metadata file */
      /********************************/ 

      ncols = topen(imgsort);

      if(ncols <= 0)
      {
           printf ("[struct stat=\"ERROR\", msg=\"Invalid image metadata file: %s\"]\n",
            imgsort);
           exit(1);
      }

      imgs = tsave();

      icntr = tcol( "cntr");
      ifile = tcol( "fname");

      if(debug >= 4)
      {
         fprintf(fdebug, "\nImage metdata table\n");
         fprintf(fdebug, "icntr  = %d\n", icntr);
         fprintf(fdebug, "ifile  = %d\n", ifile);
         fflush(fdebug);
      }

      if(icntr < 0
      || ifile < 0)
      {
         printf ("[struct stat=\"ERROR\", msg=\"Need columns: cntr and fname in image list\"]\n");
         exit(1);
      }


      /***********************************/ 
      /* Open the corrections table file */
      /***********************************/ 

      ncols = topen(corrsort);

      if(ncols <= 0)
      {
           printf ("[struct stat=\"ERROR\", msg=\"Invalid corrections  file: %s\"]\n",
            corrsort);
           exit(1);
      }

      corrs = tsave();

      iidcorr = tcol( "id");
      ia      = tcol( "a");
      ib      = tcol( "b");
      ic      = tcol( "c");

      if(debug >= 4)
      {
         fprintf(fdebug, "\nCorrections table\n");
         fprintf(fdebug, "iidcorr = %d\n", iidcorr);
         fprintf(fdebug, "ia      = %d\n", ia);
         fprintf(fdebug, "ib      = %d\n", ib);
         fprintf(fdebug, "ic      = %d\n", ic);
         fprintf(fdebug, "\n");
         fflush(fdebug);
      }

      if(iidcorr < 0
      || ia      < 0
      || ib      < 0
      || ic      < 0)
      {
         printf ("[struct stat=\"ERROR\", msg=\"Need columns: id,a,b,c in corrections file\"]\n");
         exit(1);
      }


      /***************************************************/ 
      /* Read through the two sorted tables, keeping     */
      /* them matched up.                                */
      /*                                                 */
      /* Then run mBackground to create the corrected    */
      /* image.  If there is an image in the list for    */
      /* which we don't actually have a projected file   */
      /* (can happen if the list was created from the    */
      /* 'raw' set), increment the 'failed' count.       */
      /***************************************************/ 

      count        = 0;
      nocorrection = 0;
      failed       = 0;

      if(nextImg())
      {
         printf ("[struct stat=\"ERROR\", msg=\"No images in list\"]\n");
         fflush(stdout);
         exit(1);
      }

      if(nextCorr())
      {
         printf ("[struct stat=\"ERROR\", msg=\"No corrections in list\"]\n");
         fflush(stdout);
         exit(1);
      }

      while(1)
      {
         if(debug >= 4)
         {
            fprintf(fdebug, "cntr = %d (%s),  idcorr = %d (%s %s %s)\n",
               cntr, corrfile, idcorr, astr, bstr, cstr);
            fflush(fdebug);
         }

         if(cntr == idcorr)
         {
            sprintf(cmd, "mBackground projected/%s corrected/%s %s %s %s", 
               corrfile, corrfile, astr, bstr, cstr);

            if(debug >= 4)
            {
             fprintf(fdebug, "[%s]\n", cmd);
             fflush(fdebug);
            }

            svc_run(cmd);

            if(strcmp( status, "ABORT") == 0)
            {
               strcpy( msg, svc_value( "msg" ));
            
               printf("[struct stat=\"ERROR\", msg=\"%s\"]\n", msg);
               fflush(stdout);
            
               exit(1);
            }

            strcpy( status, svc_value( "stat" ));

            ++count;
            if(strcmp( status, "ERROR") == 0)
               ++failed;

            if(!keepAll)
            {
               sprintf(cmd, "projected/%s", corrfile);
               unlink(cmd);

               strcpy(areafile, cmd);
               areafile[strlen(areafile) - 5] = '\0';
               strcat(areafile, "_area.fits");

               unlink(areafile);
            }

            if(nextImg())
               break;

            if(nextCorr())
               break;
         }

         else if(cntr < idcorr)
         {
            strcpy(astr,"0.0");
            strcpy(bstr,"0.0");
            strcpy(cstr,"0.0");

            sprintf(cmd, "mBackground projected/%s corrected/%s %s %s %s", 
               corrfile, corrfile, astr, bstr, cstr);

            if(debug >= 4)
            {
             fprintf(fdebug, "[%s] MISSING> No correction found\n", cmd);
             fflush(fdebug);
            }

            svc_run(cmd);

            if(strcmp( status, "ABORT") == 0)
            {
               strcpy( msg, svc_value( "msg" ));
            
               printf("[struct stat=\"ERROR\", msg=\"%s\"]\n", msg);
               fflush(stdout);
            
               exit(1);
            }

            strcpy( status, svc_value( "stat" ));

            ++count;
            if(strcmp( status, "ERROR") == 0)
               ++failed;
            else
               ++nocorrection;

            if(!keepAll)
            {
               sprintf(cmd, "projected/%s", corrfile);
               unlink(cmd);

               strcpy(areafile, cmd);
               areafile[strlen(areafile) - 5] = '\0';
               strcat(areafile, "_area.fits");

               unlink(areafile);
            }

            if(nextImg())
               break;
         }

         else if(cntr > idcorr)
         {
            if(debug >= 4)
            {
             fprintf(fdebug, "MISSING> No image found\n");
             fflush(fdebug);
            }

            if(nextCorr())
               break;
         }
      }

      if(!keepAll)
      {
         sprintf(cmd, "projected/%s", corrfile);
         unlink(cmd);

         strcpy(areafile, cmd);
         areafile[strlen(areafile) - 5] = '\0';
         strcat(areafile, "_area.fits");

         unlink(areafile);
      }

      time(&currtime);

      if(debug >= 1)
      {
         fprintf(fdebug, "TIME: mBackground      %6d (%d corrected)\n", (int)(currtime - lasttime), count);
         fflush(fdebug);
      }

      lasttime = currtime;
   }


   /**************************/
   /* Coadd for final mosaic */
   /**************************/

   if(baseCount > 1)
   {
      if(infoMsg)
      {
         printf("[struct stat=\"INFO\", msg=\"Coadding %d images for final mosaic\"]\n", count);
         fflush(stdout);
      }

      sprintf(cmd, "mImgtbl corrected cimages.tbl");

      if(debug >= 4)
      {
         fprintf(fdebug, "[%s]\n", cmd);
         fflush(fdebug);
      }

      svc_run(cmd);

      time(&currtime);

      if(debug >= 1)
      {
         fprintf(fdebug, "TIME: mImgtbl(corr)    %6d\n", (int)(currtime - lasttime));
         fflush(fdebug);
      }

      lasttime = currtime;

      if(ntile*mtile == 1)
      {
         sprintf(cmd, "mAdd -p corrected cimages.tbl region.hdr mosaic.fits");

         if(debug >= 4)
         {
            fprintf(fdebug, "[%s]\n", cmd);
            fflush(fdebug);
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
         for(i=0; i<ntile; ++i)
         {
            for(j=0; j<mtile; ++j)
            {
               sprintf(cmd, "mTileHdr region.hdr tmp/region_%d_%d.hdr %d %d %d %d 100 100",
                              i, j, ntile, mtile, i, j);

               if(debug >= 4)
               {
                  fprintf(fdebug, "[%s]\n", cmd);
                  fflush(fdebug);
               }

               svc_run(cmd);

               strcpy( status, svc_value( "stat" ));

               if(strcmp( status, "ERROR") == 0)
               {
                  strcpy( msg, svc_value( "msg" ));

                  printerr(msg);
               }

               sprintf(cmd, "mSubset cimages.tbl tmp/region_%d_%d.hdr tmp/cimages_%d_%d.tbl",
                  i, j, i, j);

               if(debug >= 4)
               {
                  fprintf(fdebug, "[%s]\n", cmd);
                  fflush(fdebug);
               }

               svc_run(cmd);

               strcpy( status, svc_value( "stat" ));

               if(strcmp( status, "ERROR") == 0)
               {
                  strcpy( msg, svc_value( "msg" ));

                  printerr(msg);
               }

               nmatches = atoi(svc_value("nmatches"));

               if(nmatches > 0)
               {
                  sprintf(cmd, "mAdd -p corrected tmp/cimages_%d_%d.tbl tmp/region_%d_%d.hdr tiles/tile_%d_%d.fits",
                     i, j, i, j, i, j);

                  if(debug >= 4)
                  {
                     fprintf(fdebug, "[%s]\n", cmd);
                     fflush(fdebug);
                  }

                  svc_run(cmd);

                  strcpy( status, svc_value( "stat" ));

                  if(strcmp( status, "ERROR") == 0)
                  {
                     strcpy( msg, svc_value( "msg" ));

                     printerr(msg);
                  }
               }

               if(!keepAll)
               {
                  sprintf(cmd, "tmp/region_%d_%d.hdr", i, j);
                  unlink(cmd);

                  sprintf(cmd, "tmp/cimages_%d_%d.tbl", i, j);
                  unlink(cmd);
               }
            }
         }

         sprintf(cmd, "mImgtbl tiles timages.tbl");

         if(debug >= 4)
         {
            fprintf(fdebug, "[%s]\n", cmd);
            fflush(fdebug);
         }

         svc_run(cmd);

         time(&currtime);

         if(debug >= 1)
         {
            fprintf(fdebug, "TIME: mAdd(tiles)      %6d\n", (int)(currtime - lasttime));
            fflush(fdebug);
         }

         lasttime = currtime;
         
         sprintf(cmd, "mAdd -p tiles timages.tbl region.hdr mosaic.fits");

         if(debug >= 4)
         {
            fprintf(fdebug, "[%s]\n", cmd);
            fflush(fdebug);
         }

         svc_run(cmd);

         strcpy( status, svc_value( "stat" ));

         if(strcmp( status, "ERROR") == 0)
         {
            strcpy( msg, svc_value( "msg" ));

            printerr(msg);
         }
   
         if(!keepAll)
         {
            unlink("timages.tbl");

            sprintf(cmd, "rm -rf tiles/*_area.fits");

            if(debug >= 2)
            {
               fprintf(fdebug, "%s\n", cmd);
               fflush(fdebug);
            }

            system(cmd);
         }
      }

      
      time(&currtime);

      if(debug >= 1)
      {
         fprintf(fdebug, "TIME: mAdd             %6d\n", (int)(currtime - lasttime));
         fflush(fdebug);
      }

      lasttime = currtime;
   }



   /******************************/ 
   /* Save file if so instructed */
   /******************************/ 

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

      time(&currtime);

      if(debug >= 1)
      {
         fprintf(fdebug, "TIME: Copy output      %6d (%s)\n",
            (int)(currtime - lasttime), savefile);
         fflush(fdebug);
      }

      lasttime = currtime;
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

      count = 0;

      while(1)
      {
         istat = tread();

         if(istat < 0)
            break;

         strcpy(infile, filePath("corrected", tval(ifname)));

         if(!keepAll)
         {
            unlink(infile);

            strcpy(areafile, infile);
            areafile[strlen(areafile) - 5] = '\0';
            strcat(areafile, "_area.fits");

            unlink(areafile);

            count += 2;
         }
      }

      tclose();

      if(!keepAll)
      {
         unlink("big_region.hdr");
         unlink("remote_big.tbl");
         unlink("pimages.tbl");
         unlink("cimages.tbl");
         unlink("diffs.tbl");
         unlink("fits.tbl");
         unlink("corrections.tbl");
         unlink(imgsort);
         unlink(corrsort);

         count += 11;

         if(!userRaw)
            rmdir ("raw");

         rmdir ("projected");
         rmdir ("diffs");
         rmdir ("corrected");
      }
   }
   else
   {
      ncols = topen("pimages.tbl");

      ifname = tcol("fname");

      if(ifname < 0)
      {
         strcpy(msg, "Need column 'fname' in input");

         printerr(msg);
      }

      count = 0;

      while(1)
      {
         istat = tread();

         if(istat < 0)
            break;

         strcpy(infile, filePath("projected", tval(ifname)));

         if(!keepAll)
         {
            unlink(infile);

            strcpy(areafile, infile);
            areafile[strlen(areafile) - 5] = '\0';
            strcat(areafile, "_area.fits");

            unlink(areafile);

            count += 2;
         }
      }

      tclose();

      if(!keepAll)
      {
         unlink("big_region.hdr");
         unlink("remote_big.tbl");
         unlink("pimages.tbl");
         unlink(imgsort);
         unlink(corrsort);

         count += 7;

         if(!userRaw)
            rmdir ("raw");

         rmdir ("projected");
         rmdir ("corrected");
      }
   }

   if(!keepAll && shrink != 1.)
   {
      sprintf(cmd, "rm -rf shrunken/*");

      if(debug >= 2)
      {
	 fprintf(fdebug, "%s\n", cmd);
	 fflush(fdebug);
      }

      system(cmd);
   }

   rmdir ("shrunken");

   if(!keepAll)
   {
      sprintf(cmd, "rm -rf corrected");

      if(debug >= 2)
      {
	 fprintf(fdebug, "%s\n", cmd);
	 fflush(fdebug);
      }

      system(cmd);
   }

   rmdir ("corrected");

   time(&currtime);

   if(debug >= 1)
   {
      fprintf(fdebug, "TIME: Delete files     %6d (%d files)\n", (int)(currtime - lasttime), count);
      fflush(fdebug);
   }

   lasttime = currtime;


   /******************************/
   /* Create JPEG of final image */
   /******************************/

   if(infoMsg)
   {
      printf("[struct stat=\"INFO\", msg=\"Creating presentation\"]\n");
      fflush(stdout);
   }

   factor = 1.0;

   if(naxismax < 1500)
   {
      if(showMarker)
	 sprintf(cmd, "mJPEG -ct 1 -mark %.6f %.6f eq J2000 7 red -gray mosaic.fits min max gaussianlog -out mosaic.jpg",
	    rac, decc);
      else
	 sprintf(cmd, "mJPEG -ct 1 -gray mosaic.fits min max gaussianlog -out mosaic.jpg");


      if(debug >= 4)
      {
         fprintf(fdebug, "[%s]\n", cmd);
         fflush(fdebug);
      }

      svc_run(cmd);

      strcpy( status, svc_value( "stat" ));

      if(strcmp( status, "ERROR") == 0)
      {
         strcpy( msg, svc_value( "msg" ));

         printerr(msg);
      }

      time(&currtime);

      if(debug >= 1)
      {
         fprintf(fdebug, "TIME: mJPEG            %6d\n", (int)(currtime - lasttime));
         fflush(fdebug);
      }

      lasttime = currtime;
   }
   else
   {
      factor = (double)naxismax / 1000.;

      sprintf(cmd, "mShrink mosaic.fits mosaic_small.fits %.2f", factor);

      if(debug >= 4)
      {
         fprintf(fdebug, "[%s]\n", cmd);
         fflush(fdebug);
      }

      svc_run(cmd);

      strcpy( status, svc_value( "stat" ));

      if(strcmp( status, "ERROR") == 0)
      {
         strcpy( msg, svc_value( "msg" ));

         printerr(msg);
      }

      time(&currtime);

      if(debug >= 1)
      {
         fprintf(fdebug, "TIME: mShrink          %6d\n", (int)(currtime - lasttime));
         fflush(fdebug);
      }

      lasttime = currtime;

      if(showMarker)
	 sprintf(cmd, "mJPEG -ct 1 -mark %.6f %.6f eq J2000 7 red -gray mosaic_small.fits min max gaussianlog -out mosaic.jpg",
	    rac, decc);
      else
	 sprintf(cmd, "mJPEG -ct 1 -gray mosaic_small.fits min max gaussianlog -out mosaic.jpg");

      if(debug >= 4)
      {
         fprintf(fdebug, "[%s]\n", cmd);
         fflush(fdebug);
      }

      svc_run(cmd);

      strcpy( status, svc_value( "stat" ));

      if(strcmp( status, "ERROR") == 0)
      {
         strcpy( msg, svc_value( "msg" ));

         printerr(msg);
      }

      time(&currtime);

      if(debug >= 1)
      {
         fprintf(fdebug, "TIME: mJPEG            %6d\n", (int)(currtime - lasttime));
         fflush(fdebug);
      }

      lasttime = currtime;
   }


   /**********************************************/ 
   /* Create the index.html file for the results */
   /**********************************************/ 

   ncell = 4;

   if(factor != 1.0)
      ++ncell;

   if(ntile*mtile > 1)
      ++ncell;

   if(strlen(labelText) == 0)
   {
      strcpy(labelText, locText);
      strcpy(locText, "");
   }

   fhtml = fopen("index.html", "w+");

   fprintf(fhtml, "<html>\n");
   fprintf(fhtml, "<head>\n");
   fprintf(fhtml, "<title>%s Mosaic</title>\n", labelText);
   fprintf(fhtml, "<body bgcolor=\"#ffffff\">\n");
   fprintf(fhtml, "<center>\n");
   fprintf(fhtml, "<table cellpadding=\"3\" border=\"1\">\n");

   fprintf(fhtml, "  <tr>\n");

   fprintf(fhtml, "    <td colspan=\"%d\" align=\"center\" bgcolor=\"#669999\">\n", ncell);

   fprintf(fhtml, "<table width=100%% cellpadding=5 border=0><tr>\n");
   fprintf(fhtml, "<td align=left><a href=\"http://us-vo.org\"><img border=0 src=\"http://www.us-vo.org/images/NVO_100pixels.jpg\"></a></td>\n");
   fprintf(fhtml, "<td align=center><font color=\"#ffffff\" size=+3><b>%s</b></font></td>\n", labelText);
   fprintf(fhtml, "<td align=right><a href=\"http://irsa.ipac.caltech.edu\"><img border=0 src=\"http://irsa.ipac.caltech.edu/images/logobig.jpg\" height=50></a></td>\n");
   fprintf(fhtml, "</tr></table>\n");

   fprintf(fhtml, "<table width=100%% cellpadding=5 border=0><tr>\n");

   if(strlen(locText) > 0)
      fprintf(fhtml, "<td align=left><font color=\"#ffff00\">%s</font></td>\n", locText);

   fprintf(fhtml, "<td align=center><font color=\"#ffff00\">(%s)</font></td>\n", locstr);
   fprintf(fhtml, "<td align=center><font color=\"#ffff00\">Size: %s degrees</font></td>\n", radstr);
   fprintf(fhtml, "<td align=right><font color=\"#ffff00\">%s / %s</font></td></tr>\n", survey, band);
   fprintf(fhtml, "</tr></table>\n");

   fprintf(fhtml, "    </td>\n");
   fprintf(fhtml, "  </tr>\n");
   fprintf(fhtml, "  <tr>\n");

   fprintf(fhtml, "    <td colspan=\"%d\" bgcolor=\"#669999\">\n", ncell);

   fprintf(fhtml, "      <center>\n");
   fprintf(fhtml, "        <img src=\"mosaic.jpg\" width=600 alt=\"Region mosaic image\"><br>\n");
   fprintf(fhtml, "      </center>\n");
   fprintf(fhtml, "    </td>\n");
   fprintf(fhtml, "   </tr>\n");
   fprintf(fhtml, "   <tr>\n");
   fprintf(fhtml, "     <td bgcolor=\"#669999\">\n");
   fprintf(fhtml, "      <center>\n");
   fprintf(fhtml, "      <a href=\"mosaic.fits\"><font size=-1>Mosaic&nbsp;in<br>FITS&nbsp;format</a></font>\n");
   fprintf(fhtml, "      </center>\n");
   fprintf(fhtml, "     </td>\n");

   fprintf(fhtml, "     <td bgcolor=\"#669999\">\n");
   fprintf(fhtml, "      <center>\n");
   fprintf(fhtml, "      <a href=\"mosaic_area.fits\"><font size=-1>Coverage&nbsp;map<br>in&nbsp;FITS&nbsp;format</a></font>\n");
   fprintf(fhtml, "      </center>\n");
   fprintf(fhtml, "     </td>\n");

   if(factor != 1.0)
   {
      fprintf(fhtml, "     <td bgcolor=\"#669999\">\n");
      fprintf(fhtml, "      <center>\n");
      fprintf(fhtml, "      <a href=\"mosaic_small.fits\"><font size=-1>%-gx&nbsp;shrunken&nbsp;version&nbsp;of&nbsp;FITS&nbsp;image<br>(used&nbsp;to&nbsp;make&nbsp;above&nbsp;JPEG)</font></a>\n", factor);
      fprintf(fhtml, "      </center>\n");
      fprintf(fhtml, "     </td>\n");
   }

   fprintf(fhtml, "     <td bgcolor=\"#669999\">\n");
   fprintf(fhtml, "      <center>\n");
   fprintf(fhtml, "      <a href=\"rimages.tbl\" target=\"montageinfo\"><font size=-1>List&nbsp;of&nbsp;input<br>images</font></a>\n");
   fprintf(fhtml, "      </center>\n");
   fprintf(fhtml, "     </td>\n");
   fprintf(fhtml, "     <td bgcolor=\"#669999\">\n");
   fprintf(fhtml, "      <center>\n");
   fprintf(fhtml, "      <a href=\"region.hdr\" target=\"montageinfo\"><font size=-1>FITS&nbsp;header&nbsp;from<br>mosaic&nbsp;file</font></a>\n");
   fprintf(fhtml, "      </center>\n");
   fprintf(fhtml, "     </td>\n");

   if(ntile*mtile > 1)
   {
      fprintf(fhtml, "     <td bgcolor=\"#669999\">\n");
      fprintf(fhtml, "      <center>\n");
      fprintf(fhtml, "      <a href=\"tiles\" target=\"montageinfo\"><font size=-1>Tiled subimages of mosaic region</font></a>\n");
      fprintf(fhtml, "      </center>\n");
   }

   fprintf(fhtml, "     </td>\n");
   fprintf(fhtml, "  </tr>\n");
   fprintf(fhtml, "</table><br>\n");
   fprintf(fhtml, "<font color=\"#000000\"><b>Powered by</b> <a href=\"http://montage.ipac.caltech.edu\">\n");
   fprintf(fhtml, "<font color=\"#880000\"><b>Montage</b><font></a></font><p>\n");

   if(strlen(contactText) > 0)
      fprintf(fhtml, "%s\n", contactText);
   
   fprintf(fhtml, "</center>\n");
   fprintf(fhtml, "</body>\n");
   fprintf(fhtml, "</html>\n");

   fclose(fhtml);


   /**************************************/ 
   /* Delete everything if so instructed */
   /**************************************/ 

   if(deleteAll && ntile*mtile == 1)
   {
      sprintf(cmd, "rm -rf %s", workspace);

      if(debug >= 2)
      {
         fprintf(fdebug, "%s\n", cmd);
         fflush(fdebug);
      }

      system(cmd);
   }


   /*************/
   /* Finish up */
   /*************/

   time(&currtime);

   if(debug)
   {
      if(debug == 1)
         fprintf(fdebug, "\n");

      fprintf(fdebug, "Mosaic complete.       %6d (total)\n\n", 
             (int)(currtime - start));
      fflush(fdebug);
   }

   if(infoMsg)
   {
      printf("[struct stat=\"INFO\", msg=\"Mosaic complete (%d sec)\"]\n", 
	     (int)(currtime - start));
      fflush(stdout);
   }

   printf("[struct stat=\"OK\", workspace=\"%s\"]\n", 
      workspace);
   fflush(stdout);

   exit(0);
}


int printerr(char *str)
{
   printf("[struct stat=\"ERROR\", msg=\"%s\"]\n", str);
   fflush(stdout);
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

   sprintf(msg, "Bad FITS file [%s]",
      fname);

   printerr(msg);

   return 0;
}


int nextImg()
{
   int istat;

   trestore(imgs);

   istat = tread();

   if(istat < 0)
      return 1;

   cntr = atoi(tval(icntr));

   strcpy(corrfile, tval(ifile));

   return 0;
}


int nextCorr()
{
   int istat;

   trestore(corrs);

   istat = tread();

   if(istat < 0)
      return 1;

   idcorr = atoi(tval(iidcorr));

   strcpy(astr, tval(ia));
   strcpy(bstr, tval(ib));
   strcpy(cstr, tval(ic));

   return 0;
}
