/*
Version  Developer        Date     Change
-------  ---------------  -------  -----------------------
3.0      John Good         7Nov07  Added support for point source catalogs
2.0      John Good        16Oct07  Added memory map persistant storage option
1.0      John Good        15Feb07  Baseline code
*/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <math.h>
#include <sys/time.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <montage.h>
#include <wcs.h>
#include <coord.h>
#include <mtbl.h>
#include <boundaries.h>

#include <index.h>
#include <mfmalloc.h>

#define NULLMODE       0
#define WCSMODE        1
#define CORNERMODE     2
#define POINTMODE      3

#define MAXRECT    32768
#define MAXSET        32
#define MAXSTR      1024

#define NONE          -1
#define POINT          0
#define CONE           1
#define BOX            2
#define TABLE          3

long listNodeCount;
long nodeCount;

typedef struct vec
{
   double x;
   double y;
   double z;
}
Vec;


typedef struct setStruct
{ 
   char file[MAXSTR];
   char name[MAXSTR];
   int  headbytes;
   int  reclen;
}
Set;


typedef struct setCounts
{ 
   long match;
   int  flag;
   long srcmatch;
}
SetCount;


typedef struct rectStruct
{
   int  setid;
   long catoff;
   Vec  center;
   Vec  corner[4];
   int  datatype;
}
RectInfo;


extern char *optarg;
extern int   optind, opterr;

extern int getopt(int argc, char *const *argv, const char *options);

void      pix2wcs   (struct WorldCoor*, double, double, double*, double*);

double    ra, dec, radius;
double    prevra, prevdec;

double    dtr;

long      nrect;

int       maxlev;
long      nindex, rootid;

Vec       point;

int       nset, subsetSetid;
Set      *set, *setptr;
SetCount *setcount;
RectInfo *rectinfo;

long      srcid;

long      match, nmatch, nomatch;
int       isRegion, isSubset, isMatch;

double    corner_ra[4], corner_dec[4];

static long  prevsrc, lastid, srccount;

FILE     *fsum;
FILE     *fref;

struct Rect *rect;
struct Rect  search_rect;

double    search_ra    [4];
double    search_dec   [4];
Vec       search_corner[4];
Vec       search_center;
double    search_radius;
double    search_radiusDot;
double    padDot, matchDot;

int       search_type, tmp_type;

int       checkFile (char *filename);
int       checkWCS  (struct WorldCoor *wcs, int action);

int       findBoundary();

int       pointInPolygon(Vec *point, Vec *corners);
 
int       Cross    (Vec *a, Vec *b, Vec *c);
double    Dot      (Vec *a, Vec *b);
double    Normalize(Vec *v);
void      Reverse  (Vec *v);

int       storageMode;

char      refHdr[8192];

static char *newMap;

SearchHitCallback overlapCallback(long id, void* arg);

int       errno;

double delta = 0.0001;

int rdebug = 0;

/***********************************************************************/
/*                                                                     */
/* QUICKSEARCH                                                         */
/*                                                                     */
/* Creates a R-Tree of images and searches it to find find images      */
/* overlapping a specific location.  The input image tables are in     */
/* the form output by mImgTbl and are analyzed to construct RA,Dec     */
/* bounding boxes (which is what the R-Tree is based on).              */
/*                                                                     */
/* The program can be used in a two-step process to generate memory    */
/* map files of the R-Tree, etc.  (which can take quite a while)       */
/* and then use these files in subsequent runs to search (which is     */
/* very fast).                                                         */
/*                                                                     */
/* After initialization, the program goes into a loop processing the   */
/* following commands:                                                 */
/*                                                                     */
/*    point <ra> <dec>                                                 */
/*    cone  <ra> <dec> <radius>                                        */
/*    box   <ra1> <dec1> <ra2> <dec2> <ra3> <dec3> <ra4> <dec4>        */
/*                                                                     */
/*       These three define a region of interest on                    */
/*       the sky and are then followed by one or more                  */
/*       of these two commands:                                        */
/*                                                                     */
/*                                                                     */
/*    region <outfile.tbl>             Generates a list of the number  */
/*                                     of images in each set in the    */
/*                                     region of interest.             */
/*                                                                     */
/*    subset <setid> <subset.tbl>      Gets those records from the     */
/*                                     original metadata table for     */
/*                                     the specified image set (setid  */
/*                                     is a column in the imgsets.tbl  */
/*                                     input file) and copies them to  */
/*                                     the output subset tables.  All  */
/*                                     the original columns are        */
/*                                     preserved (and don't have to    */
/*                                     be the same from image set to   */
/*                                     image set).                     */
/*                                                                     */
/*                                                                     */
/*    There are also three commands meant for large-scale comparisons  */
/*    to a user input table of locations:                              */
/*                                                                     */
/*    radius <matchsize>               A radius, in degrees, for       */
/*                                     a match size for the following  */
/*                                     two commands.  This is only     */
/*                                     used when the archive dataset   */
/*                                     is a catalog (for image sets,   */
/*                                     the criterion is that the image */
/*                                     image cover the user data       */
/*                                     location exactly).              */
/*                                                                     */
/*    table <user.tbl> <counts.tbl>    Gives a summary, by image set,  */
/*                                     of the number of sources in the */
/*                                     user tables which were observed */
/*                                     by at least image in that set.  */
/*                                                                     */
/*    matches <user.tbl> <setid>       If you want to know exactly     */
/*       <matches.tbl>                 which sources in the user table */
/*                                     were matched in the above, this */
/*                                     command can be used to get a    */
/*                                     user source table subset.       */
/*                                     (You can then take one of these */
/*                                     and use 'point' and 'subset'    */
/*                                     to get a complete list of       */
/*                                     images).                        */
/*                                                                     */
/*                                                                     */
/* If you are using the memory map file option, you can use the        */
/*                                                                     */
/*    dump <count>                                                     */
/*                                                                     */
/* command.  This will print out the contents of the information       */
/* in the memory map files in a user friendly format. If you are       */
/* working entirely in memory, "dump" will only print out some         */
/* dump information (just the tree, not the memory-order dump).        */
/*                                                                     */
/* Similarly,                                                          */
/*                                                                     */
/*    trace <id>                                                       */
/*                                                                     */
/* will print out the specific leaf node requested and all its         */
/* parents (to the tree root).                                         */
/*                                                                     */
/* You can also quit the program or set the debugging level using:     */
/*                                                                     */
/*    quit                                                             */
/*    debug <level>                                                    */
/*                                                                     */
/***********************************************************************/

int main(int argc, char **argv) 
{
   char   infile   [1024];
   char   tblfile  [1024];
   char   proj       [16];
   char   line     [1024];
   char   filename [1024];
   char   summary  [1024];
   char   outstr   [1024];
   char   setName  [1024];
   char   basefile [1024];
   char   memfile  [1024];
   char   reformat [1024];
   char   codename [1024];

   FILE  *finfo;

   int    reffile;

   int    fdset;
   int    fdrec;

   size_t size;
   size_t sizeset;
   size_t sizerec;

   struct Rect inrect;

   long   i, j, id, nrow, childID;
   long   nhits, nrec, nkey;
   long   dumpcount, dumprect, offset;

   int    ncol, tblmode;
   int    blankRec, stat, csys;
   int    iset, info, ch, dup;
   int    memMapRead, useMemMap, iname, ifile;

   char  *ptr, *key, *val, *end;

   int    ira,  idec;
   int    ira1, idec1;
   int    ira2, idec2;
   int    ira3, idec3;
   int    ira4, idec4;

   int    inl,     ins;
   int    ictype1, ictype2;
   int    icrval1, icrval2;
   int    icrpix1, icrpix2;
   int    icdelt1, icdelt2;
   int    icrota2;

   int    iequinox, iepoch;

   char   ctype1[256];
   char   ctype2[256];

   int    nl, naxis1;
   int    ns, naxis2;

   int    equinox;
   double epoch;

   double crpix1;
   double crpix2;

   double crval1;
   double crval2;

   double cdelt1;
   double cdelt2;

   double crota2;

   double lon, lat;

   double xmin, xmax;
   double ymin, ymax;
   double zmin, zmax;

   double len, pad, pad0, padpt;
   double matchRadius, padMatch, matchDelta;

   struct WorldCoor *wcsimg;

   struct Node* root;

   char   cmd[MAXSTR];

   int    cmdc;
   char  *cmdv[128];

   double tmp;

   struct timeval tp;
   struct timezone tzp;
   double begintime;
   double starttime;
   double loadtime;
   double searchtime;

   gettimeofday(&tp, &tzp);
   starttime = (double)tp.tv_sec + (double)tp.tv_usec/1000000.;
   begintime = starttime;

   search_type = NONE;

   dtr = atan(1.) / 45.;

   pad0  = tan(0.01/3600.*dtr);
   padpt = tan(1.00/3600.*dtr);

   padDot = cos(1.0/3600.*dtr);

   listNodeCount = 0;
   nodeCount = 0;

   matchDelta = 0.;


   /**********************************/
   /* Process command-line arguments */
   /**********************************/

   info       = 0;
   opterr     = 0;
   memMapRead = 0;
   useMemMap  = 0;

   strcpy(basefile, "");

   while ((ch = getopt(argc, argv, "d:mi:o:s:")) != EOF)
   {
      switch (ch)
      {
         case 'm':
            info = 1;
            break;

         case 'd':
            rdebug = debugCheck(optarg);
            break;

         case 'i':
            memMapRead = 1;
            useMemMap  = 1;
            strcpy(basefile, optarg);
            break;

         case 'o':
            strcpy(basefile, optarg);
            useMemMap  = 1;
            break;

         default:
            printf("[struct stat=\"ERROR\", msg=\"Usage: %s [-d level][-m (info)][-o|-i memfile][-m(essages)] catlist\"]\n", argv[0]);
            fflush(stdout);
            exit(0);
            break;
      }
   }

   if((memMapRead && argc < optind) || (!memMapRead && argc <= optind))
   {
      printf("[struct stat=\"ERROR\", msg=\"Usage: %s [-d level][-m (info)][-o|-i memfile][-m(essages)] catlist\"]\n", argv[0]);
      fflush(stdout);
      exit(0);
   }

   strcpy(infile, argv[optind]);

   if(rdebug)
   {
      printf("\n");
      printf("infile   = %s\n", infile);
      fflush(stdout);
   }

   if(checkFile(infile) != 0)
   {
      printf("[struct stat=\"ERROR\", msg=\"Input list file (%s) does not exist\"]\n", infile);
      fflush(stdout);
      exit(1);
   }


   /****************/
   /* DATA LOADING */
   /****************/

   /* If we are using pre-made memory-map files, */
   /* set that up and skip the rest of the data  */
   /* import stuff.                              */

   storageMode = 0;

   if(memMapRead)
   {
      /* Read the info file (text describing counts */
      /* and the R-Tree root node location)         */

      strcpy(memfile, basefile);
      strcat(memfile, ".info");

      finfo = fopen(memfile, "r");

      while(1)
      {
         if(fgets(line, 1024, finfo) == (char *)NULL)
            break;

         key = line;
         end = line + strlen(line);
         
         while(key < end && *key == ' ')
            ++key;

         val = key;

         while(val < end && *val != ' ')
            ++val;

         while(val < end && *val == ' ')
         {
            *val = '\0';
            ++val;
         }

         if(*val == '=')
            ++val;

         while(val < end && *val == ' ')
            ++val;

         ptr = val;

         while(ptr < end && *ptr != ' ')
            ++ptr;

         while(ptr < end && *ptr == ' ')
         {
            *ptr = '\0';
            ++ptr;
         }

         if(strcmp(key, "nset") == 0)
            nset = atoi(val);

         if(strcmp(key, "nrect") == 0)
            nrect = atoll(val);

         if(strcmp(key, "nindex") == 0)
            nindex = atoll(val);

         if(strcmp(key, "rootid") == 0)
            rootid = atoll(val);

         if(strcmp(key, "maxlev") == 0)
            maxlev = atoi(val);
      }

      if(rdebug)
      {
         printf("\nLoading from memory map file\n");

         printf("nset   = %d\n",  nset);
         printf("nrect  = %ld\n", nrect);
         printf("nindex = %ld\n", nindex);
         printf("rootid = %ld\n", rootid);
         printf("maxlev = %d\n",  maxlev);
         printf("\n");
         fflush(stdout);
      }


      /* Attach the image set info memory file */

      strcpy(memfile, basefile);
      strcat(memfile, ".set");

      sizeset = (long)nset * (long)sizeof(Set);
      fdset   = open(memfile, O_RDONLY);

      set = (Set *) mmap(0, sizeset, PROT_READ, MAP_SHARED, fdset, 0);

      if(set == MAP_FAILED)
      {
	 printf("[struct stat=\"ERROR\", msg=\"mmap(%ld) failed for set data\"]\n", 
	    (long)sizeset);
	 fflush(stdout);
	 exit(0);
      }

      if(rdebug)
      {
         printf("'set' memory mapped at %ld, %ld long (%ld structs of size %d)\n",
	    set, sizeset, nset, sizeof(Set));
	 fflush(stdout);
      }

      setcount = (SetCount *)malloc((long)nset * (long)sizeof(SetCount));


      /* Attach the bounding rectangle data memory file */

      strcpy(memfile, basefile);
      strcat(memfile, ".rec");

      sizerec = (long)nrect * (long)sizeof(RectInfo);
      fdrec   = open(memfile, O_RDONLY);

      rectinfo = (RectInfo *) mmap(0, sizerec, PROT_READ, MAP_SHARED, fdrec, 0);

      if(rectinfo == MAP_FAILED)
      {
	 printf("[struct stat=\"ERROR\", msg=\"mmap(%ld) failed for rectinfo data\"]\n", 
	    (long)sizerec);
	 fflush(stdout);
	 exit(0);
      }

      if(rdebug)
      {
         printf("'rectinfo' memory mapped at %ld, %ld long (%ld structs of size %d)\n",
	    rectinfo, sizerec, nrect, sizeof(RectInfo));
	 fflush(stdout);
      }


      /* Attach the image set info memory file */

      strcpy(memfile, basefile);
      strcat(memfile, ".rti");

      size = 2 * (long)nindex * (long)sizeof(struct Node);

      if(mfInit(memfile, size, memMapRead))
      {
         printf("[struct stat=\"ERROR\", msg=\"Cannot open memory file [%s]\"]\n", memfile);
         fflush(stdout);
         exit(1);
      }
      
      if(rdebug)
      {
         printf("RTree 'Node' memory map at %ld, %ld long (%ld structs of size %d)\n",
	    (long)mfMemLoc(), size, nindex, sizeof(struct Node));
	 fflush(stdout);
      }

      storageMode = 1;
      root = (struct Node *)rootid;

      gettimeofday(&tp, &tzp);
      loadtime = (double)tp.tv_sec + (double)tp.tv_usec/1000000.;

      printf("[struct stat=\"OK\", startuptime=\"%.4f\", nset=\"%d\", count=\"%ld\", size=\"%ld\"]\n",
         loadtime-begintime, nset, nrect, size);
      fflush(stdout);

      if(rdebug > 3)
      {
         for(i=0; i<nset; ++i)
         {
            printf("SET> %d %s %s\n", 
               i, set[i].file, set[i].name);
            fflush(stdout);
         }

         for(i=0; i<nrect; ++i)
         {
            printf("RECT> %ld %d %ld %-g %-g %-g\n", 
               i, rectinfo[i].setid, rectinfo[i].catoff, 
               rectinfo[i].center.x, rectinfo[i].center.y, rectinfo[i].center.z);
            fflush(stdout);
         }
      }
   }

   else  /* Not memMapRead */
   {
      /* Read in the image set info and    */
      /* allocate space for image set info */

      ncol = topen(infile);

      if(ncol < 0)
      {
         printf("[struct stat=\"ERROR\", msg=\"Error opening image set list (%s)\"]\n",
            infile);
         fflush(stdout);
         exit(0);
      }

      iname = tcol( "identifier");
      ifile = tcol( "file");

      if(iname < 0 || ifile < 0)
      {
         printf("[struct stat=\"ERROR\", msg=\"Need columns 'identifier' and 'file' in image set list (%s)\"]\n",
            infile);
         fflush(stdout);
         exit(0);
      }

      nset = tlen();

      if(rdebug)
      {
         printf("%s tlen() = %d\n", infile, nset);
         fflush(stdout);
      }

      if(nset < 0)
      {
         while(1)
         {
            stat = tread();

            if(stat < 0)
               break;

            ++nset;
         }

         tseek(0);
      }
         
      if(rdebug)
      {
         printf("nset = %d\n", nset);
         fflush(stdout);
      }

      if(strlen(basefile) > 0)
      {
         strcpy(memfile, basefile);
         strcat(memfile, ".info");

         finfo = fopen(memfile, "w+");
      }


      /* We are just goint to work in memory (no mmap files) */

      if(strlen(basefile) == 0)
      {
         set      = (Set      *)malloc(nset * (long)sizeof(Set));
         setcount = (SetCount *)malloc(nset * sizeof(SetCount));
      }


      /* We need to create and populate the mmap files */
      /* (for use by later instances of the program)   */

      else
      {
         strcpy(memfile, basefile);
         strcat(memfile, ".set");

         sizeset = nset * sizeof(Set);

         fdset   = open(memfile, O_RDWR | O_CREAT | O_TRUNC, 0664);

         lseek(fdset, sizeset-1, SEEK_SET);

         write(fdset, "", 1);

         set = (Set *) mmap(0, sizeset, PROT_READ | PROT_WRITE, MAP_SHARED, fdset, 0);

	 if(rdebug)
	 {
	    printf("'set' memory mapped at %ld, %ld long (%ld structs of size %d)\n",
	       set, sizeset, nset, sizeof(Set));
	    fflush(stdout);
	 }

	 if(rectinfo == MAP_FAILED)
	 {
            printf("[struct stat=\"ERROR\", msg=\"mmap(%ld) failed for set data\"]\n", 
	       (long)sizeset);
            fflush(stdout);
            exit(0);
	 }

         setcount = (SetCount *)malloc(nset * sizeof(SetCount));
      }


      /* Read in the names of all the image  */
      /* metadata tables we are going to use */

      i = 0;

      while(1)
      {
         stat = tread();

         if(stat < 0)
            break;

         strcpy(set[i].file, tval(ifile));
         strcpy(set[i].name, tval(iname));

         ++i;
      }

      tclose();
    

      /* Find the total number of images */
      /* (across all sets)               */

      nrect = 0;

      for(iset=0; iset<nset; ++iset)
      {
         strcpy(tblfile, set[iset].file);

         ncol = topen(tblfile);

         nrec = (long)tlen();

         set[iset].headbytes = tbl_headbytes;
         set[iset].reclen    = tbl_reclen;

         if(rdebug)
         {
            printf("\n%d:\n",           iset);
            printf("%s tlen() = %ld\n", tblfile, nrec);
	    printf("ncol      = %d\n",  ncol);
            printf("headbytes = %d\n",  tbl_headbytes);
            printf("reclen    = %d\n",  tbl_reclen);
            fflush(stdout);
         }

         if(nrec < 0)
         {
            nrec = 0;

            while(1)
            {
               stat = tread();

               if(stat < 0)
                  break;

               ++nrec;
            }
         }

         nrect += nrec;

         if(rdebug)
         {
            printf("nrect -> %ld\n", nrect);
            fflush(stdout);
         }

         tclose();
      }


      /* Attach a memory map file where */
      /* we will store the RTree data   */
      /* or if we are not using files,  */
      /* just malloc memory             */

      if(strlen(basefile) > 0)
      {
         strcpy(memfile, basefile);
         strcat(memfile, ".rti");

	 /* The number of nodes has to be less than the */
	 /* number of rectangles                        */

	 nindex = nrect;

	 size = 2 * (long)nindex * (long)sizeof(struct Node);

         if(mfInit(memfile, size, memMapRead))
         {
            printf("[struct stat=\"ERROR\", msg=\"Cannot open memory file [%s]\"]\n", memfile);
            fflush(stdout);
            exit(1);
         }

	 if(rdebug)
	 {
	    printf("RTree 'Node' memory map at %ld, %ld long (%ld structs of size %d)\n",
	       (long)mfMemLoc(), size, nindex, sizeof(struct Node));
	    fflush(stdout);
	 }
      }


      /* Initialize the R-Tree */

      root = RTreeNewIndex();


      /* Set up space for the rectangle objects */
      /* and ancillary info                     */

      if(strlen(basefile) == 0)
         rectinfo = (RectInfo *)malloc(nrect * (long)sizeof(RectInfo));
      else
      {
         strcpy(memfile, basefile);
         strcat(memfile, ".rec");

         sizerec = (long)nrect * (long)sizeof(RectInfo);

         fdrec   = open(memfile, O_RDWR | O_CREAT | O_TRUNC, 0664);

         lseek(fdrec, sizerec-1, SEEK_SET);

         write(fdrec, "", 1);

         rectinfo = (RectInfo *) mmap(0, sizerec, PROT_READ | PROT_WRITE, MAP_SHARED, fdrec, 0);

	 if(rectinfo == MAP_FAILED)
	 {

            if(errno == EACCES)    strcpy(codename, "EACCES");
            if(errno == EAGAIN)    strcpy(codename, "EAGAIN");
            if(errno == EBADF)     strcpy(codename, "EBADF");
            if(errno == EINVAL)    strcpy(codename, "EINVAL");
            if(errno == EMFILE)    strcpy(codename, "EMFILE");
            if(errno == ENODEV)    strcpy(codename, "ENODEV");
            if(errno == ENOMEM)    strcpy(codename, "ENOMEM");
            if(errno == ENOTSUP)   strcpy(codename, "ENOTSUP");
            if(errno == ENXIO)     strcpy(codename, "ENXIO");
            if(errno == EOVERFLOW) strcpy(codename, "EOVERFLOW");

            printf("[struct stat=\"ERROR\", msg=\"mmap(%ld) failed for rectinfo data: %s %s\"]\n", 
	       (long)sizerec, codename, strerror(errno));
            fflush(stdout);
            exit(0);
	 }

	 if(rdebug)
	 {
	    printf("'rectinfo' memory mapped at %ld, %ld long (%ld structs of size %d)\n",
	       rectinfo, sizerec, nrect, sizeof(RectInfo));
	    fflush(stdout);
	 }
      }


      /* Loop over the input tables,        */
      /* building a complete set of all     */
      /* images (identified by 'id' number) */

      id = 0;

      for(iset=0; iset<nset; ++iset)
      {
         nrec = 0;

         strcpy(tblfile, set[iset].file);

         if(rdebug)
         {
            printf("Opening image/catalog table [%s]\n", tblfile);
            fflush(stdout);
         }

         ncol = topen(tblfile);

         if(ncol < 0)
         {
            printf("[struct stat=\"ERROR\", msg=\"Error opening table %s\"]\n",
               tblfile);
            fflush(stdout);
            exit(0);
         }

         ictype1  = tcol( "ctype1");
         ictype2  = tcol( "ctype2");
         iequinox = tcol( "equinox");
         iepoch   = tcol( "epoch");
         inl      = tcol( "nl");
         ins      = tcol( "ns");
         icrval1  = tcol( "crval1");
         icrval2  = tcol( "crval2");
         icrpix1  = tcol( "crpix1");
         icrpix2  = tcol( "crpix2");
         icdelt1  = tcol( "cdelt1");
         icdelt2  = tcol( "cdelt2");
         icrota2  = tcol( "crota2");
         ira      = tcol( "ra");
         idec     = tcol( "dec");

         if(ins < 0)
            ins = tcol("naxis1");

         if(inl < 0)
            inl = tcol("naxis2");

         ira1     = tcol( "ra1");
         idec1    = tcol( "dec1");
         ira2     = tcol( "ra2");
         idec2    = tcol( "dec2");
         ira3     = tcol( "ra3");
         idec3    = tcol( "dec3");
         ira4     = tcol( "ra4");
         idec4    = tcol( "dec4");

         if(rdebug > 1)
         {
            printf("ira      = %d\n", ira);
            printf("idec     = %d\n", idec);
            printf("ictype1  = %d\n", ictype1);
            printf("ictype2  = %d\n", ictype2);
            printf("iequinox = %d\n", iequinox);
            printf("iepoch   = %d\n", iepoch);
            printf("inl      = %d\n", inl);
            printf("ins      = %d\n", ins);
            printf("icrval1  = %d\n", icrval1);
            printf("icrval2  = %d\n", icrval2);
            printf("icrpix1  = %d\n", icrpix1);
            printf("icrpix2  = %d\n", icrpix2);
            printf("icdelt1  = %d\n", icdelt1);
            printf("icdelt2  = %d\n", icdelt2);
            printf("icrota2  = %d\n", icrota2);
            printf("ira1     = %d\n", ira1);
            printf("idec1    = %d\n", idec1);
            printf("ira2     = %d\n", ira2);
            printf("idec2    = %d\n", idec2);
            printf("ira3     = %d\n", ira3);
            printf("idec3    = %d\n", idec3);
            printf("ira4     = %d\n", ira4);
            printf("idec4    = %d\n", idec4);
            printf("\n");
            fflush(stdout);
         }


         /* Corners, if they exist, take precedence */

         tblmode = NULLMODE;

         if(ira1     >= 0
         && idec1    >= 0
         && ira2     >= 0
         && idec2    >= 0
         && ira3     >= 0
         && idec3    >= 0
         && ira4     >= 0
         && idec4    >= 0)
            tblmode = CORNERMODE;

         else 
         if(ictype1  >= 0
         && ictype2  >= 0
         && inl      >= 0
         && ins      >= 0
         && icrval1  >= 0
         && icrval2  >= 0
         && icrpix1  >= 0
         && icrpix2  >= 0
         && icdelt1  >= 0
         && icdelt2  >= 0
         && icrota2  >= 0)
            tblmode = WCSMODE;

         else
         if(ira      >= 0
         && idec     >= 0)
            tblmode = POINTMODE;

         if(tblmode == NULLMODE)
         {
            printf("[struct stat=\"ERROR\", msg=\"Need either WCS or corner columns or at least point source ra and dec.\"]\n");
            fflush(stdout);
            exit(0);
         }


         /* Read the table file and process each record */

         nrow = 0;

         while(1)
         {
            blankRec = 0;

            stat = tread();

            if(stat < 0)
               break;

            ++nrow;

            if(rdebug > 2)
            {
               printf("\n\n---------------\nREAD image/point %d\n", nrow);
               fflush(stdout);
            }


            /* If we don't have the corners, compute them */
            /* using the WCS library.                     */

            if(tblmode == WCSMODE)
            {
               strcpy(ctype1, tval(ictype1));
               strcpy(ctype2, tval(ictype2));

               equinox = 0;
               epoch   = 0;

               if(iequinox >= 0)
                  equinox = atoi(tval(iequinox));

               if(iepoch >= 0)
                  epoch   = atof(tval(iepoch));

               nl      = atoi(tval(inl));
               ns      = atoi(tval(ins));

               if(strlen(tval(icrval1)) == 0
               || strlen(tval(icrval2)) == 0)
                  blankRec = 1;
                  
               crval1  = atof(tval(icrval1));
               crval2  = atof(tval(icrval2));

               crpix1  = atof(tval(icrpix1));
               crpix2  = atof(tval(icrpix2));

               cdelt1  = atof(tval(icdelt1));
               cdelt2  = atof(tval(icdelt2));

               crota2  = atof(tval(icrota2));

               strcpy(proj, "");
               csys = EQUJ;

               if(strlen(ctype1) > 4)
               strcpy (proj, ctype1+4);  

               if(strncmp(ctype1, "RA",   2) == 0)
                  csys = EQUJ;
               if(strncmp(ctype1, "GLON", 4) == 0)
                  csys = GAL;
               if(strncmp(ctype1, "ELON", 4) == 0)
                  csys = ECLJ;

               if(rdebug > 1)
               {
                  printf("proj      = [%s]\n", proj);
                  printf("csys      = %d\n",   csys);
                  printf("\n");
                  fflush(stdout);
               }


               /* Correct if no epoch / equinox */

               if (epoch == 0) 
                  epoch = 2000.;

               if (equinox == 0) 
               {
                  equinox = 2000;

                  if ((epoch >= 1950.0) && (epoch <= 2000.)) 
                     equinox = (int)epoch;
               }

               if(rdebug > 1)
               {
                  printf("nrow     = %d\n",    nrow);
                  printf("-------------\n");
                  printf("ctype1   = [%s]\n",  ctype1);
                  printf("ctype2   = [%s]\n",  ctype2);

                  printf("equinox  = %d\n",    equinox);
                  printf("epoch    = %-g\n",   epoch);

                  printf("ns       = %d\n",    ns);
                  printf("nl       = %d\n",    nl);

                  printf("crval1   = %-g\n",   crval1);
                  printf("crval2   = %-g\n",   crval2);

                  printf("crpix1   = %-g\n",   crpix1);
                  printf("crpix2   = %-g\n",   crpix2);

                  printf("cdelt1   = %-g\n",   cdelt1);
                  printf("cdelt2   = %-g\n",   cdelt2);

                  printf("crota2   = %-g\n",   crota2);
                  printf("\n");
                  fflush(stdout);
               }

               wcsimg = wcskinit (ns,     nl,
                                  ctype1, ctype2,
                                  crpix1, crpix2,
                                  crval1, crval2, (double *)NULL,
                                  cdelt1, cdelt2,
                                  crota2, equinox, 0.);
               
               checkWCS(wcsimg, 0);

               if(rdebug > 1)
               {
                  printf("WCS set within WCSMODE\n");
                  fflush(stdout);
               }

               if (nowcs (wcsimg)) 
               {
                  printf("[struct stat=\"ERROR\", msg=\"Failed to create wcs structure for record %d.\"]\n", nrow);
                  fflush(stdout);
                  exit(0);
               }

               pix2wcs(wcsimg, -0.5, -0.5, &lon, &lat);
               convertCoordinates (csys, (double)equinox, lon, lat, 
                                   EQUJ, 2000., &corner_ra[0], &corner_dec[0], 0.);


               pix2wcs(wcsimg, wcsimg->nxpix+0.5, -0.5, &lon, &lat);
               convertCoordinates (csys, (double)equinox, lon, lat, 
                                   EQUJ, 2000., &corner_ra[1], &corner_dec[1], 0.);


               pix2wcs(wcsimg, wcsimg->nxpix+0.5, wcsimg->nypix+0.5, &lon, &lat);
               convertCoordinates (csys, (double)equinox, lon, lat, 
                                   EQUJ, 2000., &corner_ra[2], &corner_dec[2], 0.);


               pix2wcs(wcsimg, -0.5, wcsimg->nypix+0.5, &lon, &lat);
               convertCoordinates (csys, (double)equinox, lon, lat, 
                                   EQUJ, 2000., &corner_ra[3], &corner_dec[3], 0.);
            }


            /* If we have corners, determine the centroid and */
            /* bounding radius in the same way                */

            else if(tblmode == CORNERMODE)
            {
               if(strlen(tval(ira1) ) == 0
               || strlen(tval(idec1)) == 0
               || strlen(tval(ira2) ) == 0
               || strlen(tval(idec2)) == 0
               || strlen(tval(ira3) ) == 0
               || strlen(tval(idec3)) == 0
               || strlen(tval(ira4) ) == 0
               || strlen(tval(idec4)) == 0)
                  blankRec = 1;
                  
               corner_ra [0] = atof(tval(ira1));
               corner_dec[0] = atof(tval(idec1));
               corner_ra [1] = atof(tval(ira2));
               corner_dec[1] = atof(tval(idec2));
               corner_ra [2] = atof(tval(ira3));
               corner_dec[2] = atof(tval(idec3));
               corner_ra [3] = atof(tval(ira4));
               corner_dec[3] = atof(tval(idec4));
            }


            /* If we are dealing with point sources, get the */
            /* ra and dec                                    */

            else if(tblmode == POINTMODE)
            {
               if(strlen(tval(ira) ) == 0
               || strlen(tval(idec)) == 0)
                  blankRec = 1;
                  
               ra  = atof(tval(ira));
               dec = atof(tval(idec));
            }

            if(blankRec)
            {
               printf("[struct stat=\"WARNING\", msg=\"Error loading record %d from table %s\"]\n",
                  nrec, tblfile);
               fflush(stdout);

	       ++nrec;
	       continue;
            }


            if(tblmode == WCSMODE || tblmode == CORNERMODE)
	    {
	       /* Compute the x,y,z vectors for the corners */
	       /* While we are at it, compute the 'average' */
	       /* x,y,z (which we will use as the center of */
	       /* the image)                                */

	       xmin =  2.;
	       xmax = -2.;

	       ymin =  2.;
	       ymax = -2.;

	       zmin =  2.;
	       zmax = -2.;

               rectinfo[id].datatype = CORNERMODE;

	       rectinfo[id].center.x = 0.;
	       rectinfo[id].center.y = 0.;
	       rectinfo[id].center.z = 0.;

	       for(i=0; i<4; ++i)
	       {
		  ra  = corner_ra [i] * dtr;
		  dec = corner_dec[i] * dtr;

		  rectinfo[id].corner[i].x = cos(ra) * cos(dec);
		  rectinfo[id].corner[i].y = sin(ra) * cos(dec);
		  rectinfo[id].corner[i].z = sin(dec);

		  rectinfo[id].center.x += rectinfo[id].corner[i].x;
		  rectinfo[id].center.y += rectinfo[id].corner[i].y;
		  rectinfo[id].center.z += rectinfo[id].corner[i].z;

		  if(rectinfo[id].corner[i].x < xmin) xmin = rectinfo[id].corner[i].x;
		  if(rectinfo[id].corner[i].x > xmax) xmax = rectinfo[id].corner[i].x;

		  if(rectinfo[id].corner[i].y < ymin) ymin = rectinfo[id].corner[i].y;
		  if(rectinfo[id].corner[i].y > ymax) ymax = rectinfo[id].corner[i].y;

		  if(rectinfo[id].corner[i].z < zmin) zmin = rectinfo[id].corner[i].z;
		  if(rectinfo[id].corner[i].z > zmax) zmax = rectinfo[id].corner[i].z;
	       }


	       /* Use this center to determine   */
	       /* how much 'bulge' the image has */
	       /* and pad xmin ... zmax by this  */

	       len = sqrt(rectinfo[id].center.x*rectinfo[id].center.x 
			+ rectinfo[id].center.y*rectinfo[id].center.y 
			+ rectinfo[id].center.z*rectinfo[id].center.z);

	       rectinfo[id].center.x = rectinfo[id].center.x/len;
	       rectinfo[id].center.y = rectinfo[id].center.y/len;
	       rectinfo[id].center.z = rectinfo[id].center.z/len;

	       pad = 1. - Dot(&rectinfo[id].corner[0], &rectinfo[id].center);

	       if(pad < pad0)
		  pad = pad0;

	       xmin -= pad;
	       xmax += pad;
	       ymin -= pad;
	       ymax += pad;
	       zmin -= pad;
	       zmax += pad;

	       if(rdebug > 1)
	       {
		  printf("\n");

		  for(i=0; i<4; ++i)
		  {
		     printf("Corner %d:  %11.6f %11.6f  -> %11.8f %11.8f %11.8f\n",
			i, corner_ra[i], corner_dec[i], rectinfo[id].corner[i].x, 
			   rectinfo[id].corner[i].y, rectinfo[id].corner[i].z);
		  }

		  printf("\n");
		  printf("pad = %11.8f\n", pad);
		  printf("\n");

		  printf("x range: %11.8f %11.8f\n", xmin, xmax);
		  printf("y range: %11.8f %11.8f\n", ymin, ymax);
		  printf("z range: %11.8f %11.8f\n", zmin, zmax);
		  printf("\n");

		  fflush(stdout);
	       }
	    }
	    else if(tblmode == POINTMODE)
	    {
	       /* Pad the point by a small amount (~1 arcsec) */

               rectinfo[id].datatype = POINTMODE;

	       rectinfo[id].center.x = cos(ra*dtr) * cos(dec*dtr);
	       rectinfo[id].center.y = sin(ra*dtr) * cos(dec*dtr);
	       rectinfo[id].center.z = sin(dec*dtr);

	       for(i=0; i<4; ++i)
	       {
		  rectinfo[id].corner[i].x = 0.;
		  rectinfo[id].corner[i].y = 0.;
		  rectinfo[id].corner[i].z = 0.;
	       }

	       xmin = rectinfo[id].center.x - padpt;
	       ymin = rectinfo[id].center.y - padpt;
	       zmin = rectinfo[id].center.z - padpt;

	       xmax = rectinfo[id].center.x + padpt;
	       ymax = rectinfo[id].center.y + padpt;
	       zmax = rectinfo[id].center.z + padpt; 

	       if(rdebug > 1)
	       {
		  printf("Point %d:  %11.6f %11.6f  -> %11.8f %11.8f %11.8f\n",
		     id, ra, dec, rectinfo[id].center.x, 
			rectinfo[id].center.y, rectinfo[id].center.z);

		  printf("x range: %11.8f %11.8f\n", xmin, xmax);
		  printf("y range: %11.8f %11.8f\n", ymin, ymax);
		  printf("z range: %11.8f %11.8f\n", zmin, zmax);
		  printf("\n");

		  fflush(stdout);
	       }
	    }


            /* Add this image/point to the R-Tree */

            inrect.boundary[0] = xmin;
            inrect.boundary[1] = ymin;
            inrect.boundary[2] = zmin;
            inrect.boundary[3] = xmax;
            inrect.boundary[4] = ymax;
            inrect.boundary[5] = zmax;

	    if(info)
	    {
	       if(xmax-xmin < pad)
	       {
		  printf("[struct stat=\"INFO\", set=%d, rec=%d, xdiff=%-g]\n", iset, nrec, xmax - xmin);
		  fflush(stdout);
	       }

	       if(ymax-ymin < pad)
	       {
		  printf("[struct stat=\"INFO\", set=%d, rec=%d, ydiff=%-g]\n", iset, nrec, ymax - ymin);
		  fflush(stdout);
	       }

	       if(zmax-zmin < pad)
	       {
		  printf("[struct stat=\"INFO\", set=%d, rec=%d, zdiff=%-g]\n", iset, nrec, zmax - zmin);
		  fflush(stdout);
	       }
	    }

            rectinfo[id].setid  = iset;
            rectinfo[id].catoff = nrec;
	    
	    if(rdebug)
	    {
	       printf("\nrect %d:\n", id);
	       printf("setid       %d\n", rectinfo[id].setid);
	       printf("catoff      %ld\n", rectinfo[id].catoff);

	       printf("center     %13.10f %13.10f %13.10f\n", 
		  rectinfo[id].center.x, rectinfo[id].center.y, rectinfo[id].center.z);

	       for(j=0; j<4; ++j)
		  printf("corner[%d]  %13.10f %13.10f %13.10f\n", 
		     j, rectinfo[id].corner[j].x, rectinfo[id].corner[j].y, rectinfo[id].corner[j].z);

	       printf("datatype    %d\n", rectinfo[id].datatype);
	       fflush(stdout);
	    }

            RTreeInsertRect(&inrect, id+1, &root, 0);

	    if(rectinfo[0].setid != 0)
	    {
	       printf("XXX> memory screwed up here!\n");
	       fflush(stdout);
	       exit(0);
	    }

            ++id;
            ++nrec;
         }

         tclose();

         gettimeofday(&tp, &tzp);
         loadtime = (double)tp.tv_sec + (double)tp.tv_usec/1000000.;

         if(info)
         {
            printf("[struct stat=\"INFO\", time=\"%.4f\", setid=\"%d\", setname=\"%s\", count=\"%d\"]\n",
               loadtime-starttime, iset, set[iset].name, nrow);
            fflush(stdout);
         }

         starttime = loadtime;
      }

      msync((void *)set,      sizeset, MS_SYNC);
      msync((void *)rectinfo, sizerec, MS_SYNC);

      nindex = RTreeGetNodeCount();
      maxlev = RTreeGetMaxLevel();
      rootid = RTreeGetRootID();

      if(strlen(basefile) > 0)
      {
         RTreeConvertToID(root);

         root = (struct Node *)rootid;

         storageMode = 1;
      }

      if(strlen(basefile) > 0)
      {
         fprintf(finfo, "nset   = %d\n", nset);
         fprintf(finfo, "nrect  = %d\n", nrect);
         fprintf(finfo, "nindex = %d\n", nindex);
         fprintf(finfo, "rootid = %d\n", rootid);
         fprintf(finfo, "maxlev = %d\n", maxlev);
         fclose(finfo);
      }

      if(rdebug)
      {
         printf("\nLoad from table files\n");
         printf("nset    = %d\n",  nset);
         printf("nrect   = %ld\n", nrect);
         printf("nindex  = %ld\n", nindex);
         printf("rootid  = %ld\n", rootid);
         printf("maxlev  = %d\n",  maxlev);
         printf("\n");
         fflush(stdout);
      }

      gettimeofday(&tp, &tzp);
      loadtime = (double)tp.tv_sec + (double)tp.tv_usec/1000000.;

      printf("[struct stat=\"OK\", startuptime=\"%.4f\", nset=\"%d\", count=\"%d\", size=%ld]\n",
         loadtime-begintime, nset, id, (long)mfSize());
      fflush(stdout);
   }


   /*********************/
   /* MAIN COMMAND LOOP */
   /*********************/

   if(rdebug)
   {
      printf("\nREADING COMMAND\n");
      fflush(stdout);
   }

   while(1)
   {
      isRegion = 0;
      isSubset = 0;
      isMatch  = 0;

      srcid   = 0;
      nmatch  = 0;
      nomatch = 0;

      for(i=0; i<nset; ++i)
      {
         setcount[i].match    = 0;
         setcount[i].flag     = 0;
         setcount[i].srcmatch = 0;
      }

      if(fgets(line, 1024, stdin) == (char *)NULL)
         break;

      gettimeofday(&tp, &tzp);
      starttime = (double)tp.tv_sec + (double)tp.tv_usec/1000000.;

      cmdc = parsecmd(line, cmdv);
      
      if(cmdc < 1)
      {
         printf("[struct stat=\"ERROR\", msg=\"Null command.\"]\n");
         fflush(stdout);
         continue;
      }

      if(rdebug)
      {
         printf("\n");
         for(i=0; i<cmdc; ++i)
            printf("CMD %d: [%s]\n", i, cmdv[i]);
         printf("\n");
         fflush(stdout);
      }
     
      strcpy(cmd, cmdv[0]);


      /* QUIT command */

      if(strncasecmp(cmd, "quit", 1) == 0
      || strncasecmp(cmd, "exit", 2) == 0)
         break;


      /* TRACE command */

      else if(strncasecmp(cmd, "trace", 2) == 0)
      {
	 printf("[struct stat=\"OK\", command=\"trace\"]\n");
	 fflush(stdout);

	 childID = 0;
	 dumprect  = nrect;

         if(cmdc > 1)
            childID = atoll(cmdv[1]);

	 if(dumpcount < 0)
	    childID = 0;

	 printf("\nTRACE %ld:\n\n", childID);

	 RTreeParentage(root, childID, storageMode);
      }


      /* DUMP command */

      else if(strncasecmp(cmd, "dump", 2) == 0)
      {
	 dumpcount = 0;
	 dumprect  = nrect;

         if(cmdc > 1)
	 {
            dumpcount = atoi(cmdv[1]);
            dumprect  = dumpcount;
	 }

	 if(dumpcount < 0)
	 {
	    dumpcount = 0;
	    dumprect  = nrect;
	 }

	 if(rdebug)
	 {
	    printf("\ndumpcount = %d\n\n");
	    fflush(stdout);
	 }
	

	 printf("RECTINFO:\n\n");

	 for(id=0; id<dumprect; ++id)
	 {
            printf("\nrect %d:\n", id);
            printf("setid       %d\n", rectinfo[id].setid);
            printf("catoff      %ld\n", rectinfo[id].catoff);

            printf("center     %13.10f %13.10f %13.10f\n", 
	       rectinfo[id].center.x, rectinfo[id].center.y, rectinfo[id].center.z);

	    for(j=0; j<4; ++j)
	       printf("corner[%d]  %13.10f %13.10f %13.10f\n", 
		  j, rectinfo[id].corner[j].x, rectinfo[id].corner[j].y, rectinfo[id].corner[j].z);

            printf("datatype    %d\n", rectinfo[id].datatype);
	    fflush(stdout);
         }


	 printf("\n\nNode data @%ld\n", (long)set);
	 printf("Node size: %d\n", sizeof(struct Node));
	 fflush(stdout);

	 if(strlen(basefile) > 0)
	 {
	    printf("\n\nNODES (mem):\n\n");
	    fflush(stdout);

	    RTreeDumpMem(dumpcount);
	 }


	 printf("\n\nNODES (tree):\n\n");
	 fflush(stdout);

	 RTreeDumpTree(root, dumpcount, storageMode, maxlev, 1);

	 printf("[struct stat=\"OK\", command=\"dump\"]\n");
	 fflush(stdout);
      }


      /* ORGANIZE command */

      else if(strncasecmp(cmd, "organize", 2) == 0)
      {
	 if(!useMemMap)
         {
            printf("[struct stat=\"ERROR\", msg=\"Not using memory mapped files\"]\n");
            fflush(stdout);
            continue;
         }

         strcpy(reformat, basefile);
         strcat(reformat, ".ref");

	 reffile = open(reformat, O_RDWR | O_CREAT | O_TRUNC, 0664);

	 lseek(reffile, size-1, SEEK_SET);

	 write(reffile, "", 1);

	 newMap = mmap(0, size, PROT_READ | PROT_WRITE, MAP_SHARED, reffile, 0);

	 if(newMap == MAP_FAILED)
	    return(1);

	 if(rdebug)
	 {
	    printf("DEBUG> newMap = %0lxx\n", (long)newMap);
	    fflush(stdout);
	 }

	 newNodes = (struct Node *)newMap;

	 RTreeReformat(root, maxlev, 0, 1);

	 msync((void *)newMap, size, MS_SYNC);

	 printf("[struct stat=\"OK\", command=\"organize\"]\n");
	 fflush(stdout);
      }


      /* DEBUG command */

      else if(strncasecmp(cmd, "debug", 2) == 0)
      {
         rdebug = atoi(cmdv[1]);

         printf("[struct stat=\"OK\", command=\"debug\"]\n");
	 fflush(stdout);
      }


      /* RADIUS command */

      else if(strncasecmp(cmd, "radius", 2) == 0)
      {
         if(cmdc < 2)
         {
            printf("[struct stat=\"ERROR\", msg=\"Command usage: radius <matchradius>\"]\n");
            fflush(stdout);
            continue;
         }
         
         matchRadius = fabs(atof(cmdv[1]));

         matchDot   = cos(matchRadius * dtr);
	 matchDelta = tan(matchRadius * dtr);

         if(matchDot > padDot)
            matchDot = padDot;

         printf("[struct stat=\"OK\", command=\"radius\", radius=%.8f]\n", matchRadius);
         fflush(stdout);
      }


      /* POINT command */

      else if(strncasecmp(cmd, "point", 2) == 0)
      {
         search_type = POINT;

         if(cmdc < 3)
         {
            printf("[struct stat=\"ERROR\", msg=\"Command usage: point <ra> <dec>\"]\n");
            fflush(stdout);
            continue;
         }
         
         ra  = atof(cmdv[1]);
         dec = atof(cmdv[2]);

         search_center.x = cos(ra*dtr) * cos(dec*dtr);
         search_center.y = sin(ra*dtr) * cos(dec*dtr);
         search_center.z = sin(dec*dtr);

         if(rdebug)
         {
            printf("POINT search: %11.6f %11.6f -> %11.8f %11.8f %11.8f\n\n",
               ra, dec,  search_center.x,  search_center.y,  search_center.z);
            fflush(stdout);
         }

         printf("[struct stat=\"OK\", command=\"point\", ra=%.6f, dec=%.6f]\n", ra, dec);
         fflush(stdout);
      }


      /* CONE command */

      else if(strncasecmp(cmd, "cone", 2) == 0)
      {
         search_type = CONE;

         if(cmdc < 4)
         {
            printf("[struct stat=\"ERROR\", msg=\"Command usage: cone <ra> <dec> <radius>\"]\n");
            fflush(stdout);
            continue;
         }
         
         ra  = atof(cmdv[1]);
         dec = atof(cmdv[2]);

         search_radius = atof(cmdv[3]);

         search_radiusDot = cos(search_radius*dtr);

         search_center.x = cos(ra*dtr) * cos(dec*dtr);
         search_center.y = sin(ra*dtr) * cos(dec*dtr);
         search_center.z = sin(dec*dtr);

         if(rdebug)
         {
            printf("CONE search: %11.6f %11.6f -> %11.8f %11.8f %11.8f\n",
               ra, dec,  search_center.x,  search_center.y,  search_center.z);
            printf("            radius = %11.6f\n\n",
               search_radius);
            fflush(stdout);
         }

         printf("[struct stat=\"OK\", command=\"cone\", ra=%.6f, dec=%.6f, radius=%.6f]\n",
	    ra, dec, search_radius);
         fflush(stdout);
      }


      /* BOX command */

      else if(strncasecmp(cmd, "box", 2) == 0)
      {
         search_type = BOX;

         if(cmdc < 9)
         {
            printf("[struct stat=\"ERROR\", msg=\"Command usage: box <ra1> <dec1> <ra2> <dec2> <ra3> <dec3> <ra4> <dec4>\"]\n");
            fflush(stdout);
            continue;
         }
         
         search_center.x = 0.;
         search_center.y = 0.;
         search_center.z = 0.;

         for(i=0; i<4; ++i)
         {
            ra  = atof(cmdv[2*i+1]);
            dec = atof(cmdv[2*i+2]);

	    search_ra [i] = ra;
	    search_dec[i] = dec;

            search_corner[i].x = cos(ra*dtr) * cos(dec*dtr);
            search_corner[i].y = sin(ra*dtr) * cos(dec*dtr);
            search_corner[i].z = sin(dec*dtr);

            search_center.x += search_corner[i].x;
            search_center.y += search_corner[i].y;
            search_center.z += search_corner[i].z;
         }

         len = search_center.x*search_center.x 
             + search_center.y*search_center.y 
             + search_center.z*search_center.z;

         if(len <= 0.)
         {
            printf("[struct stat=\"ERROR\", msg=\"Invalid corners\n]\n");
            fflush(stdout);
            continue;
         }
         
         len = sqrt(len);

         search_center.x /= len;
         search_center.y /= len;
         search_center.z /= len;

         if(rdebug)
         {
            printf("BOX search:\n");

            for(i=0; i<4; ++i)
            {
               ra  = atof(cmdv[2*i+1]);
               dec = atof(cmdv[2*i+2]);

               printf("  %11.6f %11.6f -> %11.8f %11.8f %11.8f\n",
                  ra, dec,  search_corner[i].x,  search_corner[i].y,  search_corner[i].z);
            }

            printf("  center = %11.8f %11.8f %11.8f\n",
               search_center.x,  search_center.y,  search_center.z);

            fflush(stdout);
         }

         printf("[struct stat=\"OK\", command=\"box\", ra1=%.6f, dec1=%.6f, ra4=%.6f, dec4=%.6f, ra3=%.6f, dec3=%.6f, ra4=%.6f, dec4=%.6f]\n",
	    search_ra[0], search_dec[0], 
	    search_ra[1], search_dec[1], 
	    search_ra[2], search_dec[2], 
	    search_ra[3], search_dec[3]);
         fflush(stdout);
      }


      /* TABLE command */

      else if(strncasecmp(cmd, "table", 2) == 0)
      {
         if(cmdc < 3)
         {
            printf("[struct stat=\"ERROR\", msg=\"Command usage: table <source.tbl> <summary.tbl>\"]\n");
            fflush(stdout);
            continue;
         }
         
         strcpy(filename, cmdv[1]);
         strcpy(summary,  cmdv[2]);

         fsum = fopen(summary, "w+");

         if(fsum == (FILE *)NULL)
         {
            printf("[struct stat=\"ERROR\", msg=\"Cannot open summary output file (%s)\"]\n", summary);
            fflush(stdout);
            continue;
         }

         ncol = topen(filename);

         if(ncol <= 0)
         {
            printf("[struct stat=\"ERROR\", msg=\"Error opening table %s\"]\n",
               filename);
            fflush(stdout);
            continue;
         }
         else
         {
            ira  = tcol("ra");
            idec = tcol("dec");

            if(ira < 0 || idec < 0)
            {
               printf("[struct stat=\"ERROR\", msg=\"Table must have columns 'ra' and 'dec'\"]\n");
               fflush(stdout);
               continue;
            }

            prevsrc = -1;

            tmp_type    = search_type;
            search_type = TABLE;
               
            while(1)
            {
               stat = tread();

               if(stat < 0)
                  break;

               prevra  = ra;
               prevdec = dec;

               ra  = atof(tval(ira));
               dec = atof(tval(idec));

               point.x = cos(ra*dtr) * cos(dec*dtr);
               point.y = sin(ra*dtr) * cos(dec*dtr);
               point.z = sin(dec*dtr);

               if(rdebug > 2)
               {
                  printf("\n------------------\nREAD SOURCE:  srcid = %ld  prevsrc = %d:  ra = %11.6f   dec = %11.6f\n\n",
                     srcid, prevsrc, ra, dec);
                  fflush(stdout);
               }

               search_rect.boundary[0] = point.x - delta - matchDelta;
               search_rect.boundary[1] = point.y - delta - matchDelta;
               search_rect.boundary[2] = point.z - delta - matchDelta;
               search_rect.boundary[3] = point.x + delta + matchDelta;
               search_rect.boundary[4] = point.y + delta + matchDelta;
               search_rect.boundary[5] = point.z + delta + matchDelta;

               nhits = RTreeSearch(root, &search_rect, (SearchHitCallback)overlapCallback, 0, storageMode);

               if(nhits <= 0)
               {
                  ++nomatch;

                  if(rdebug > 1)
                  {
                     printf("Source %ld [%.6f %.6f] not matched by any image box\n\n",
                        srcid, ra, dec);
                     fflush(stdout);
                  }
               }

               ++srcid;
            }

            search_type = tmp_type;
         }

         tclose();

         match = 0;

         for(i=0; i<nset; ++i)
         {
            /* The flag was set for this */
            /* source for image set 'i', */
            /* so increment the source   */
            /* count for that set        */

            if(setcount[i].flag)
            {
               ++setcount[i].srcmatch;

               match = 1;
            }

            else if(rdebug > 2)
            {
               printf("Source %8d  [%11.6f %11.6f] not matched in set %3d\n\n", prevsrc, ra, dec, i);
               fflush(stdout);
            }

            setcount[i].match = 0;
            setcount[i].flag  = 0;
         }

         if(match)
            ++nmatch;
         else
            ++nomatch;

         
         /* Go through the image metadata list again */
         /* printing out the records with the path   */
         /* field removed and the source match       */
         /* data added                               */

         ncol = topen(infile);

         ifile = tcol( "file");

         offset = tbl_rec[ifile].endcol - tbl_rec[ifile].colwd;

         strcpy(outstr, tbl_hdr_string);

         outstr[offset] = '\0';

         fprintf(fsum, "\\fixlen = T\n");
         fprintf(fsum, "%s|%10s|\n", outstr, "count");
         fflush(fsum);

         if(strlen(tbl_typ_string) != 0)
         {
            strcpy(outstr, tbl_typ_string);

            outstr[offset] = '\0';

            fprintf(fsum, "%s|%10s|\n", outstr, " int ");
            fflush(fsum);
         }

         for(i=0; i<nset; ++i)
         {
            tread();

            strcpy(outstr, tbl_rec_string);

            outstr[offset] = '\0';

	    if(setcount[i].srcmatch > 0)
	    {
	       fprintf(fsum, "%s %10d \n", outstr, setcount[i].srcmatch);
	       fflush(fsum);
	    }

            if(info)
            {
               printf("[struct stat=\"INFO\", setid=\"%d\", setname=\"%s\", matchedsrcs=\"%d\"]\n",
                  i, set[i].name, setcount[i].srcmatch);
               fflush(stdout);
            }
         }

         tclose();

         gettimeofday(&tp, &tzp);
         searchtime = (double)tp.tv_sec + (double)tp.tv_usec/1000000.;

         fclose(fsum);

         printf("[struct stat=\"OK\", command=\"table\", table=\"%s\", outfile=\"%s\", time=\"%.4f\", nsrc=\"%ld\", match=\"%ld\", nomatch=\"%ld\"]\n",
            filename, summary, searchtime-starttime, srcid, nmatch, nomatch);
         fflush(stdout);
      }


      /* REGION command */

      else if(strncasecmp(cmd, "region", 2) == 0)
      {
         isRegion =  1;
         prevsrc  = -1;

         if(cmdc < 2)
         {
            printf("[struct stat=\"ERROR\", msg=\"Command usage: region <outfile>\"]\n");
            fflush(stdout);
            continue;
         }
         
         if(search_type == NONE)
         {
            printf("[struct stat=\"ERROR\", msg=\"No search region specified\"]\n");
            fflush(stdout);
            continue;
         }
         
         strcpy(summary, cmdv[1]);
         
         fsum = fopen(summary, "w+");

         if(fsum == (FILE *)NULL)
         {
            printf("[struct stat=\"ERROR\", msg=\"Cannot open summary output file (%s)\"]\n", summary);
            fflush(stdout);
            continue;
         }

         if(rdebug > 2)
         {
            printf("\n------------------\nSINGLE REGION, ALL IMAGE SETS:\n\n");
            fflush(stdout);
         }

         findBoundary();

         nhits = RTreeSearch(root, &search_rect, (SearchHitCallback)overlapCallback, 0, storageMode);

         nmatch = 0;

         ncol = topen(infile);

         ifile = tcol( "file");

         offset = tbl_rec[ifile].endcol - tbl_rec[ifile].colwd;

         strcpy(outstr, tbl_hdr_string);

         outstr[offset] = '\0';

         fprintf(fsum, "\\fixlen = T\n");
         fprintf(fsum, "%s|%10s|\n", outstr, "count");
         fflush(fsum);

         if(strlen(tbl_typ_string) != 0)
         {
            strcpy(outstr, tbl_typ_string);

            outstr[offset] = '\0';

            fprintf(fsum, "%s|%10s|\n", outstr, " int ");
            fflush(fsum);
         }

         for(i=0; i<nset; ++i)
         {
            tread();

            strcpy(outstr, tbl_rec_string);

            outstr[offset] = '\0';

            if(setcount[i].flag)
               ++nmatch;
            
	    if(setcount[i].match > 0)
	    {
	       fprintf(fsum, "%s %10d \n", outstr, setcount[i].match);
	       fflush(fsum);
	    }
         }

         printf("[struct stat=\"OK\", command=\"region\", outfile=\"%s\", count=\"%d\"]\n", 
	    summary, nmatch);
         fflush(stdout);
      }


      /* SUBSET command */

      else if(strncasecmp(cmd, "subset", 2) == 0)
      {
         isSubset =  1;
         prevsrc  = -1;

         if(cmdc < 3)
         {
            printf("[struct stat=\"ERROR\", msg=\"Command usage: subset <setid> <outfile>\"]\n");
            fflush(stdout);
            continue;
         }
         
         if(search_type == NONE)
         {
            printf("[struct stat=\"ERROR\", msg=\"No search region specified\"]\n");
            fflush(stdout);
            continue;
         }
         
         strcpy(setName, cmdv[1]);

         subsetSetid = -1;

         for(i=0; i<nset; ++i)
         {
            if(strcasecmp(set[i].name, setName) == 0)
            {
               subsetSetid = i;
               break;
            }
         }
         
         if(subsetSetid == -1)
         {
            printf("[struct stat=\"ERROR\", msg=\"No such dataset name (%s)\"]\n", cmdv[1]);
            fflush(stdout);
            continue;
         }

         strcpy(summary, cmdv[2]);
         
         fsum = fopen(summary, "w+");

         if(fsum == (FILE *)NULL)
         {
            printf("[struct stat=\"ERROR\", msg=\"Cannot open summary output file (%s)\"]\n", summary);
            fflush(stdout);
            continue;
         }

         strcpy(tblfile, set[subsetSetid].file);

         ncol = topen(tblfile);

	 nkey = tkeycount();

	 for(i=0; i<nkey; ++i)
	    fprintf(fsum, "%s\n", thdrline(i));

         fprintf(fsum, "%s\n", tbl_hdr_string);
         fflush(fsum);

         if(strlen(tbl_typ_string) != 0)
         {
            fprintf(fsum, "%s\n", tbl_typ_string);
            fflush(fsum);
         }

         if(strlen(tbl_uni_string) != 0)
         {
            fprintf(fsum, "%s\n", tbl_uni_string);
            fflush(fsum);
         }

         if(strlen(tbl_nul_string) != 0)
         {
            fprintf(fsum, "%s\n", tbl_nul_string);
            fflush(fsum);
         }

         if(rdebug > 2)
         {
            printf("\n------------------\nSINGLE REGION, SINGLE IMAGE SET:\n\n");
            fflush(stdout);
         }

         findBoundary();

         nhits = RTreeSearch(root, &search_rect, (SearchHitCallback)overlapCallback, 0, storageMode);

         printf("[struct stat=\"OK\", command=\"subset\", dataset=\"%s\", outfile=\"%s\", count=\"%d\"]\n", 
	    setName, summary, setcount[subsetSetid].match);
         fflush(stdout);
         fclose(fsum);
      }


      /* MATCHES command */

      else if(strncasecmp(cmd, "matches", 2) == 0)
      {
         isMatch = 1;

         if(cmdc < 3)
         {
            printf("[struct stat=\"ERROR\", msg=\"Command usage: matches <source.tbl> <setid> <summary.tbl>\"]\n");
            fflush(stdout);
            continue;
         }
         
         strcpy(filename, cmdv[1]);

         strcpy(setName, cmdv[2]);

         subsetSetid = -1;

         for(i=0; i<nset; ++i)
         {
            if(strcasecmp(set[i].name, setName) == 0)
            {
               subsetSetid = i;

               fref = fopen(set[i].file, "r");

               if(fref == (FILE *)NULL)
	       {
		  printf("[struct stat=\"ERROR\", msg=\"No such dataset name (%s)\"]\n", cmdv[2]);
		  fflush(stdout);
		  continue;
	       }

               topen(set[i].file);
               strcpy(refHdr, tbl_hdr_string);
               tclose();
  
               break;
            }
         }
         
         if(subsetSetid == -1)
         {
            printf("[struct stat=\"ERROR\", msg=\"No such dataset name (%s)\"]\n", cmdv[2]);
            fflush(stdout);
            continue;
         }

         strcpy(summary,  cmdv[3]);

         fsum = fopen(summary, "w+");

         if(fsum == (FILE *)NULL)
         {
            printf("[struct stat=\"ERROR\", msg=\"Cannot open summary output file (%s)\"]\n", summary);
            fflush(stdout);
            continue;
         }

         ncol = topen(filename);

         fprintf(fsum, "|%12s%s |%s\n", "matchid", tbl_hdr_string, refHdr+1);
         fflush(fsum);

         if(ncol <= 0)
         {
            printf("[struct stat=\"ERROR\", msg=\"Error opening table %s\"]\n",
               filename);
            fflush(stdout);
            continue;
         }
         else
         {
            ira  = tcol("ra");
            idec = tcol("dec");

            if(ira < 0 || idec < 0)
            {
               printf("[struct stat=\"ERROR\", msg=\"Table must have columns 'ra' and 'dec'\"]\n");
               fflush(stdout);
               continue;
            }

            prevsrc  = -1;
            lastid   = -1;
            srccount =  0;
            srcid    =  0;

            tmp_type    = search_type;
            search_type = TABLE;
               
            while(1)
            {
               stat = tread();

               if(stat < 0)
                  break;

               prevra  = ra;
               prevdec = dec;

               ra  = atof(tval(ira));
               dec = atof(tval(idec));

               point.x = cos(ra*dtr) * cos(dec*dtr);
               point.y = sin(ra*dtr) * cos(dec*dtr);
               point.z = sin(dec*dtr);

               if(rdebug > 2)
               {
                  printf("\n------------------\nREAD SOURCE:  srcid = %ld  prevsrc = %d:  ra = %11.6f   dec = %11.6f\n\n",
                     srcid, prevsrc, ra, dec);
                  fflush(stdout);
               }

               search_rect.boundary[0] = point.x - delta - matchDelta;
               search_rect.boundary[1] = point.y - delta - matchDelta;
               search_rect.boundary[2] = point.z - delta - matchDelta;
               search_rect.boundary[3] = point.x + delta + matchDelta;
               search_rect.boundary[4] = point.y + delta + matchDelta;
               search_rect.boundary[5] = point.z + delta + matchDelta;

               nhits = RTreeSearch(root, &search_rect, (SearchHitCallback)overlapCallback, 0, storageMode);

               if(nhits <= 0)
               {
                  ++nomatch;

                  if(rdebug > 1)
                  {
                     printf("Source %ld [%.6f %.6f] not matched by any image box\n\n",
                        srcid, ra, dec);
                     fflush(stdout);
                  }
               }

               ++srcid;
            }

            search_type = tmp_type;
         }

         tclose();

         fclose(fref);

         match = 0;

         for(i=0; i<nset; ++i)
         {
            /* The flag was set for this */
            /* source for image set 'i', */
            /* so increment the source   */
            /* count for that set        */

            if(setcount[i].flag)
            {
               ++setcount[i].srcmatch;

               match = 1;
            }

            else if(rdebug > 2)
            {
               printf("Source %8d  [%11.6f %11.6f] not matched in set %3d\n\n", prevsrc, ra, dec, i);
               fflush(stdout);
            }

            setcount[i].match = 0;
            setcount[i].flag  = 0;
         }

         if(match)
            ++nmatch;
         else
            ++nomatch;

         gettimeofday(&tp, &tzp);
         searchtime = (double)tp.tv_sec + (double)tp.tv_usec/1000000.;

         printf("[struct stat=\"OK\", command=\"matches\", table=\"%s\", dataset=\"%s\", outfile=\"%s\", time=\"%.4f\", nsrc=\"%ld\", nmatch=\"%ld\"]\n", 
            filename, setName, summary, searchtime-starttime, srcid, srccount);
         fflush(stdout);

         fclose(fsum);
      }


      /* DEFAULT */

      else
      {
         printf("[struct stat=\"ERROR\", command=\"%s\", msg=\"Invalid command.\"]\n", cmd);
         fflush(stdout);
         continue;
      }
   }

   printf("[struct stat=\"OK\", command=\"quit\", msg=\"Quitting\"]\n");
   fflush(stdout);

   exit(0);
}



/***************************************************/
/*                                                 */
/* Depending on the type of region, determine the  */
/* search bounding box.                            */
/*                                                 */
/***************************************************/

int findBoundary()
{
   int    i;
   double ra, dec;
   double R, pad;
   double dx, dy, dz;
   double xmin, xmax;
   double ymin, ymax;
   double zmin, zmax;

   if(search_type == POINT)
   {
      /* The search rectangle is just a tiny */
      /* box centered on the point           */

      search_rect.boundary[0] = search_center.x - delta;
      search_rect.boundary[1] = search_center.y - delta;
      search_rect.boundary[2] = search_center.z - delta;
      search_rect.boundary[3] = search_center.x + delta;
      search_rect.boundary[4] = search_center.y + delta;
      search_rect.boundary[5] = search_center.z + delta;

      return(0);
   }

   else if(search_type == CONE)
   {
      /* Find the maximum extent of the search */

      ra  = atan2(search_center.y, search_center.x)/dtr;
      dec = asin (search_center.z)/dtr;


      /* The "cartesian" radius of the cone */

      R = sin(search_radius*dtr);


      /* This length transformed into dx,dy,dz */

      dx = R;
      dy = R;
      dz = R;

      xmin = search_center.x*cos(search_radius*dtr) - dx;
      xmax = search_center.x*cos(search_radius*dtr) + dx;
      ymin = search_center.y*cos(search_radius*dtr) - dy;
      ymax = search_center.y*cos(search_radius*dtr) + dy;
      zmin = search_center.z*cos(search_radius*dtr) - dz;
      zmax = search_center.z*cos(search_radius*dtr) + dz;

      search_rect.boundary[0] = xmin;
      search_rect.boundary[1] = ymin;
      search_rect.boundary[2] = zmin;
      search_rect.boundary[3] = xmax;
      search_rect.boundary[4] = ymax;
      search_rect.boundary[5] = zmax;
   }

   else if(search_type == BOX)
   {
      xmin =  2.;
      xmax = -2.;

      ymin =  2.;
      ymax = -2.;

      zmin =  2.;
      zmax = -2.;

      for(i=0; i<4; ++i)
      {
         if(search_corner[i].x < xmin) xmin = search_corner[i].x;
         if(search_corner[i].x > xmax) xmax = search_corner[i].x;

         if(search_corner[i].y < ymin) ymin = search_corner[i].y;
         if(search_corner[i].y > ymax) ymax = search_corner[i].y;

         if(search_corner[i].z < zmin) zmin = search_corner[i].z;
         if(search_corner[i].z > zmax) zmax = search_corner[i].z;
      }

      /* Use the center to determine    */
      /* how much 'bulge' the image has */
      /* and pad xmin ... zmax by this  */

      pad = 1. - Dot(&search_corner[0], &search_center);

      xmin -= pad;
      xmax += pad;
      ymin -= pad;
      ymax += pad;
      zmin -= pad;
      zmax += pad;

      search_rect.boundary[0] = xmin;
      search_rect.boundary[1] = ymin;
      search_rect.boundary[2] = zmin;
      search_rect.boundary[3] = xmax;
      search_rect.boundary[4] = ymax;
      search_rect.boundary[5] = zmax;
   }

   if(rdebug)
   {
      printf("\nSearch rectangle (findBoundary()):\n");

      printf("x: %11.8f %11.8f\n", xmin, xmax);
      printf("y: %11.8f %11.8f\n", ymin, ymax);
      printf("z: %11.8f %11.8f\n", zmin, zmax);

      printf("\n");
      fflush(stdout);
   }
}



/********************************************************/
/*                                                      */
/* Callback for RTree library:  Called whenever         */
/* a match is found and populates the following         */
/* summary statistics information:                      */
/*                                                      */
/* setcount[setid].match     -  total number of source  */
/*                              hits for an image set   */
/*                                                      */
/* setcount[setid].flag      -  boolean: this image set */
/*                              has been hit            */
/*                                                      */
/* setcounT[setid].srcmatch  -  number of sources in    */
/*                              the current table that  */
/*                              found matching images   */
/*                              in an image set         */
/*                                                      */
/********************************************************/

SearchHitCallback overlapCallback(long index, void* arg)
{
   long  nrec, id;

   int    i, setid;
   int    interior, datamode;
   Vec    normal, X;
   double l, L, len, dist, distDot;

   char   refRec[8192];
   int    refOffset;

   interior = 0;


   /* All the hits for a given source   */
   /* come together, so if the srcid    */
   /* changes, we are done with this    */
   /* source.                           */
   /*                                   */
   /* We want to determine how many of  */
   /* our sources are covered by one or */
   /* more image in each set.           */

   if(srcid != prevsrc && prevsrc != -1)
   {
      match = 0;

      for(i=0; i<nset; ++i)
      {
         /* The flag was set for this */
         /* source for image set 'i', */
         /* so increment the source   */
         /* count for that set        */

         if(setcount[i].flag)
         {
            ++setcount[i].srcmatch;

            match = 1;
         }

         else if(rdebug > 2)
         {
            printf("<== overlapCallback(): Source %ld [%.6f %.6f] not matched in set %d\n\n",
               prevsrc, prevra, prevdec, i);
            fflush(stdout);
         }


         /* and reset the info in preparation */
         /* for the next source               */

         setcount[i].match = 0;
         setcount[i].flag  = 0;
      }

      if(match)
         ++nmatch;
      else
         ++nomatch;
   }

   prevsrc = srcid;


   /* Check exact coverage */

   id = index - 1;

   setid    = rectinfo[id].setid;
   nrec     = rectinfo[id].catoff;
   datamode = rectinfo[id].datatype;

   if(rdebug > 1)
   {
      printf("overlapCallback(): Source %ld MAY be covered by image %ld (record %ld in set %d)\n",
         srcid, id, nrec, setid);
      fflush(stdout);
   }


   if(datamode == POINTMODE)
   {
      /* POINT DATA                                 */
      /*                                            */
      /* There is different code here for each of   */
      /* the three region types (point, cone, box)  */

      /* Multi-source table input */

      if(search_type == TABLE)
      {
	 distDot = Dot(&point, &rectinfo[id].center);

	 if(distDot > matchDot)
	 {
	    if(rdebug)
	    {
	       printf("  MATCH: distance = %.6f\n", acos(distDot)/dtr);
	       fflush(stdout);
	    }

	    interior = 1;
	 }
      }


      /* A single cone */

      else if(search_type == CONE)
      {
	 /* Check for point in cone */

	 distDot = Dot(&search_center, &rectinfo[id].center);
         
	 if(distDot > search_radiusDot)
	    interior = 1;
      }


      /* A single box */

      else if(search_type == BOX)
      {

	 /* Check for point inside search box */

	 interior = pointInPolygon(&rectinfo[id].center, search_corner);
      }
   }

   else
   {
      /* IMAGE DATA                                 */
      /*                                            */
      /* There is different code here for each of   */
      /* the three region types (point, cone, box)  */

      /* Multi-source table input */

      if(search_type == TABLE)
	 interior = pointInPolygon(&point, rectinfo[id].corner);


      /* A single point-like location */

      else if(search_type == POINT)
	 interior = pointInPolygon(&search_center, rectinfo[id].corner);


      /* A single cone */

      else if(search_type == CONE)
      {
	 /* Check for image center in cone */

	 distDot = Dot(&search_center, &rectinfo[id].center);

	 if(distDot > search_radiusDot)
	    interior = 1;
	 

	 /* Check for image corners in cone */

	 if(!interior)
	 {
	    for(i=0; i<4; ++i)
	    {
	       if(Dot(&search_center, &rectinfo[id].corner[i]) > search_radiusDot)
	       {
		  interior = 1;
		  break;
	       }
	    }
	 }


	 /* Check for cone center in image */

	 if(!interior)
	    interior = pointInPolygon(&search_center, rectinfo[id].corner);


	 /* Check for cone edge point (the one */
	 /* nearest the image center) inside   */
	 /* the image                          */

	 if(!interior)
	 {
	    dist = acos(distDot);
	     
	    L = 2. * sin(dist/2.);

	    l = L/2. - sin(dist/2. - search_radius*dtr);

	    X.x = search_center.x + l/L * (rectinfo[id].center.x - search_center.x);
	    X.y = search_center.y + l/L * (rectinfo[id].center.y - search_center.y);
	    X.z = search_center.z + l/L * (rectinfo[id].center.z - search_center.z);

	    len = sqrt(X.x*X.x + X.y*X.y + X.z*X.z);

	    X.x /= len;
	    X.y /= len;
	    X.z /= len;

	    if(Dot(&search_center, &X) > distDot)
	       interior = 1;
	 }
      }


      /* A single box */

      else if(search_type == BOX)
      {

	 /* Image corners inside search box */

	 for(i=0; i<4; ++i)
	 {
	    ra  = atan2(search_corner[i].y, search_corner[i].x)/dtr;
	    dec = asin (search_corner[i].z)/dtr;

	    interior = pointInPolygon(&search_corner[i], rectinfo[id].corner);

	    if(interior)
	       break;
	 }


	 /* Search region corners inside image */

	 if(!interior)
	 {
	    for(i=0; i<4; ++i)
	    {
	       ra  = atan2(rectinfo[id].corner[i].y, rectinfo[id].corner[i].x)/dtr;
	       dec = asin (rectinfo[id].corner[i].z)/dtr;

	       interior = pointInPolygon(&rectinfo[id].corner[i], search_corner);

	       if(interior)
		  break;
	    }
	 }
      }
   }

   if(!interior)
      return((SearchHitCallback)1);


   /* Log the hit */

   if(isMatch && setid == subsetSetid)
   {
      refOffset = set[setid].headbytes + set[setid].reclen * nrec;

      fseek(fref, refOffset, SEEK_SET);

      fgets(refRec, 8192, fref);

      fprintf(fsum, " %12ld%s  %s",
	 srcid, tbl_rec_string, refRec+1);
      fflush(fsum);

      lastid = srcid;

      ++srccount;
   }

   if(isSubset && setid == subsetSetid)
   {
      tseek(nrec);
      tread();

      fprintf(fsum, "%s\n", tbl_rec_string);
      fflush(fsum);
   }

   if(rdebug)
   {
      printf("overlapCallback(): Source %ld is covered by image %ld (record %ld in set %d)\n",
         srcid, id, nrec, setid);
      fflush(stdout);
   }


   /* We have another hit */
   /* for this image set  */

   ++setcount[setid].match;


   /* This image set has now */
   /* been hit at least once */

   setcount[setid].flag = 1;

   return((SearchHitCallback)1);
}


/*********************************************/
/*                                           */
/* Check whether a point is inside a polygon */
/*                                           */
/*********************************************/

int pointInPolygon(Vec *point, Vec *corners)
{
   int i, inext, interior;
   Vec normal;

   interior = 1;

   for(i=0; i<4; ++i)
   {
      inext = (i+1)%4;

      Cross(&corners[i], &corners[inext], &normal);

      if(Dot(&normal, point) > 0.)
      {
         interior = 0;
         break;
      }
   }

   if(rdebug > 2)
   {
      printf("\npointInPolygon():\n");

      for(i=0; i<4; ++i)
         printf("corner %d) %11.6f %11.6f %11.6f\n", i, corners[i].x, corners[i].y, corners[i].z);

      printf("\nvector)   %11.6f %11.6f %11.6f --> %d\n\n", point->x, point->y, point->z, interior);

      fflush(stdout);
   }

   return(interior);
}


/***************************************************/
/*                                                 */
/* Cross()                                         */
/*                                                 */
/* Vector cross product.                           */
/*                                                 */
/***************************************************/

int Cross(Vec *v1, Vec *v2, Vec *v3)
{
   v3->x =  v1->y*v2->z - v2->y*v1->z;
   v3->y = -v1->x*v2->z + v2->x*v1->z;
   v3->z =  v1->x*v2->y - v2->x*v1->y;

   if(v3->x == 0.
   && v3->y == 0.
   && v3->z == 0.)
      return 0;

   return 1;
}


/***************************************************/
/*                                                 */
/* Dot()                                           */
/*                                                 */
/* Vector dot product.                             */
/*                                                 */
/***************************************************/

double Dot(Vec *a, Vec *b)
{
   int i;
   double sum = 0.0;

   sum = a->x * b->x
       + a->y * b->y
       + a->z * b->z;

   return sum;
}


/***************************************************/
/*                                                 */
/* Normalize()                                     */
/*                                                 */
/* Normalize the vector                            */
/*                                                 */
/***************************************************/

double Normalize(Vec *v)
{
   double len;

   len = 0.;

   len = sqrt(v->x * v->x + v->y * v->y + v->z * v->z);

   if(len == 0.)
      len = 1.;

   v->x = v->x / len;
   v->y = v->y / len;
   v->z = v->z / len;

   return len;
}


/***************************************************/
/*                                                 */
/* Reverse()                                       */
/*                                                 */
/* Reverse the vector                              */
/*                                                 */
/***************************************************/

void Reverse(Vec *v)
{
   v->x = -v->x;
   v->y = -v->y;
   v->z = -v->z;
}
