#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>

#include <mtbl.h>

#define TRUE       1
#define FALSE  !TRUE
#define FOREVER TRUE

struct TBL_REC *tbl_rec;

static int    ncol      = 0;
static int    headbytes = 0;
static int    reclen    = 0;
static int    nrec      = 0;

int           tbl_headbytes  = 0;
int           tbl_reclen     = 0;

char         *tbl_rec_string = (char *)NULL;
char         *tbl_hdr_string = (char *)NULL;
char         *tbl_typ_string = (char *)NULL;
char         *tbl_uni_string = (char *)NULL;
char         *tbl_nul_string = (char *)NULL;

static int    mtbl_linelen = 0;
static int    mtbl_maxline = 0;
static int    mtbl_maxcol  = 0;

int           haveType = 0;
int           haveUnit = 0;
int           haveNull = 0;

static char  *dval;

static char **keystr  = (char **)NULL;
static char **keyword = (char **)NULL;
static char **value   = (char **)NULL;

static int    nhdr      = 0;
static int    nkey      = 0;
static int    nkeyalloc = 0;

static FILE  *tfile = (FILE *)NULL;

static int    tdebug = 0;
static int    tWrite = 0;


void tsetlen(int maxstr)
{
   mtbl_maxline = maxstr;
}


void tsetdebug(int debug)
{
   tdebug = debug;
}


void tWritable(int flag)
{
   tWrite = flag;
}


int topen(char *fname)
{
   int         i, j, k, l, headlen, isblank;
   int         headlent, ncolt, nscan;
   char       *ptr, *kptr, *vptr;
   struct stat buf;
   char       *scan;

   tclear();

   /* Quick scan to determine table */
   /* record width                  */

   if(mtbl_maxline == 0)
   {
      if(tdebug)
      {
     printf("TDEBUG> Checking %s for header width<br>\n", fname);
     fflush(stdout);
      }

      nscan = MTBL_MAXLINE;

      while(1)
      {
     scan = (char *)malloc(nscan * sizeof(char));

     if(scan != (char *)NULL)
        break;

     if(nscan < 1024)
        return(MTBL_MALLOC);
      }

      if(tWrite)
	 tfile = fopen(fname, "r+");
      else
	 tfile = fopen(fname, "r");

      if (tfile == (FILE *)NULL)
     return(MTBL_NOFILE);

      while(1)
      {
     if(fgets(scan, nscan, tfile) == (char *)NULL)
        return (MTBL_RDERR);

     if(tdebug)
     {
        printf("TDEBUG> Scanning [%s]<br>\n", scan);
        fflush(stdout);
     }

     if(scan[0] == '|')
     {
        mtbl_maxline = 2 * strlen(scan);

        if(tdebug)
        {
           printf("TDEBUG> Scan -> max line length = %d<br>\n",
          mtbl_maxline);
           fflush(stdout);
        }

        break;
     }
      }

      if(mtbl_maxline < MTBL_MAXSTR)
      {
     mtbl_maxline = MTBL_MAXSTR;

     if(tdebug)
     {
        printf("TDEBUG> No header found -> max line length defaults to %d<br>\n",
           mtbl_maxline);
        fflush(stdout);
     }
      }

      free(scan);

      rewind(tfile);
      fclose(tfile);
   }

   if(tdebug)
   {
      printf("TDEBUG> Max line length = %d<br>\n", mtbl_maxline);
      fflush(stdout);
   }


   /* Allocate space for working strings */

   if (mtbl_linelen < mtbl_maxline)
   {
      if(tdebug)
      {
     printf("TDEBUG> Malloc %d character strings (tbl_hdr_len, etc.)<br>\n",
        mtbl_maxline);
     fflush(stdout);
      }
      tbl_rec_string = (char *)realloc(tbl_rec_string, mtbl_maxline * sizeof(char));
      tbl_hdr_string = (char *)realloc(tbl_hdr_string, mtbl_maxline * sizeof(char));
      tbl_typ_string = (char *)realloc(tbl_typ_string, mtbl_maxline * sizeof(char));
      tbl_uni_string = (char *)realloc(tbl_uni_string, mtbl_maxline * sizeof(char));
      tbl_nul_string = (char *)realloc(tbl_nul_string, mtbl_maxline * sizeof(char));
      dval           = (char *)realloc(dval,           mtbl_maxline * sizeof(char));
      mtbl_linelen   = mtbl_maxline;
   }

   strcpy(tbl_rec_string, "");
   strcpy(tbl_hdr_string, "");
   strcpy(tbl_typ_string, "");
   strcpy(tbl_uni_string, "");
   strcpy(tbl_nul_string, "");

   strcpy(dval, "");


   if(mtbl_maxcol == 0)
   {
      mtbl_maxcol = MTBL_MAXCOL;

      tbl_rec = (struct TBL_REC *)malloc(mtbl_maxcol * sizeof(struct TBL_REC));

      for(i=0; i<mtbl_maxcol; ++i)
      {
     for(k=0; k<MTBL_MAXSTR; ++k)
     {
        tbl_rec[i].name[k] = '\0';
        tbl_rec[i].type[k] = '\0';
        tbl_rec[i].unit[k] = '\0';
        tbl_rec[i].nuls[k] = '\0';
     }

     tbl_rec[i].endcol = 0;
     tbl_rec[i].colwd  = 0;
      }
   }


   /* OPEN FILE */

   if(tWrite)
      tfile = fopen(fname, "r+");
   else
      tfile = fopen(fname, "r");

   if (tfile == (FILE *)NULL)
      return(MTBL_NOFILE);


   /******************************************************/
   /* READ HEADER, find columns and extract column names */
   /******************************************************/

   headbytes =  0;
   reclen    =  0;
   nrec      = -1;
   nkey      =  0;
   nhdr      =  0;

   haveType = 0;
   haveUnit = 0;
   haveNull = 0;


   /* Parse keyword lines */

   while(FOREVER)
   {
      if(fgets(dval, mtbl_maxline, tfile) == (char *) NULL)
     break;

      if(tdebug)
      {
     printf("TDEBUG> Read keyword header line [%s]<br>\n", dval);
     fflush(stdout);
      }

      reclen     = (int)strlen(dval);
      headbytes += reclen;

      if(dval[strlen(dval) - 1] == '\n')
         dval[strlen(dval) - 1]  = '\0';

      if(dval[strlen(dval) - 1] == '\r')
         dval[strlen(dval) - 1]  = '\0';

      isblank = 1;
      for(i=0; i<strlen(dval); ++i)
      {
     if(dval[i] != ' ' && dval[i] != '\t')
     {
        isblank = 0;
        break;
     }
      }

      if(!isblank && dval[0] != '\\')
     break;

      if(nkeyalloc <= nkey)
      {
     nkeyalloc += MTBL_MAXKEY;

     keystr  = (char **)realloc(keystr,  nkeyalloc * sizeof(char *));
     keyword = (char **)realloc(keyword, nkeyalloc * sizeof(char *));
     value   = (char **)realloc(value,   nkeyalloc * sizeof(char *));

     for(i=nkey; i<nkeyalloc; ++i)
     {
        keystr [i] = (char *)malloc(MTBL_KEYLEN * sizeof(char));
        keyword[i] = (char *)malloc(MTBL_KEYLEN * sizeof(char));
        value  [i] = (char *)malloc(MTBL_KEYLEN * sizeof(char));
     }
      }

      strcpy(keystr[nhdr], dval);
      ++nhdr;

      kptr = dval + 1;
      ptr  = kptr;

      while(*ptr != ' '
     && *ptr != '='
     && *ptr != '\0')
        ++ptr;

      while(*ptr == ' ')
      {
     *ptr = '\0';
     ++ptr;
      }

      if(*ptr != '=')
     continue;

      *ptr = '\0';
      ++ptr;

      while(*ptr == ' ')
         ++ptr;

      vptr = ptr;

      for(i=strlen(vptr)-1; i>=0; --i)
      {
     if(vptr[i] == ' ')
        vptr[i] = '\0';
     else
        break;
      }

      if(strlen(kptr) > 0)
      {
     strcpy(keyword[nkey], kptr);
     strcpy(value  [nkey], vptr);
     ++nkey;
      }
   }

   strcpy(tbl_hdr_string, dval);

   if(dval[0] == '|')
      dval[0] =  ' ';

   headlen = reclen;

   if(tbl_hdr_string[headlen - 1] == '\n')
      tbl_hdr_string[headlen - 1] =  '\0';


   /* Parse the header line for column names and sizes */

   ncol = 0;

   j = 0;

   headlen = (int)strlen(dval);

   for(i=0; i<headlen; ++i)
   {
      if (dval[i] == '\\')
     break;

      if (dval[i] == '\n')
     break;

      else if (dval[i] == '|')
      {
     tbl_rec[ncol].endcol = i;
     tbl_rec[ncol].name[j] = '\0';
     ++ncol;

     if(ncol > mtbl_maxcol)
     {
        mtbl_maxcol += MTBL_MAXCOL;

        tbl_rec = (struct TBL_REC *)
           realloc(tbl_rec, mtbl_maxcol * sizeof(struct TBL_REC));

        for(k=mtbl_maxcol-MTBL_MAXCOL; k<mtbl_maxcol; ++k)
        {
           for(l=0; l<MTBL_MAXSTR; ++l)
           {
          tbl_rec[k].name[l] = '\0';
          tbl_rec[k].type[l] = '\0';
          tbl_rec[k].unit[l] = '\0';
          tbl_rec[k].nuls[l] = '\0';
           }

           tbl_rec[k].endcol = 0;
           tbl_rec[k].colwd  = 0;
        }
     }

     j = 0;

         if(i == 0)
        --ncol;
      }

      else if (dval[i] != ' ' )
      {
     tbl_rec[ncol].name[j] = dval[i];
     ++j;
      }
   }

   tbl_rec[0].colwd = tbl_rec[0].endcol + 1;

   for (i=1; i<ncol; ++i)
    tbl_rec[i].colwd = tbl_rec[i].endcol - tbl_rec[i-1].endcol;


   /* Read any additional header lines */

   while(FOREVER)
   {
      if(fgets(dval, mtbl_maxline, tfile) == (char *) NULL)
     break;

      if(tdebug)
      {
     printf("TDEBUG> Read additional header [%s]<br>\n", dval);
     fflush(stdout);
      }

      if(dval[0] != '|')
     break;

      reclen     = (int)strlen(dval);
      headbytes += reclen;

      if(!haveType)
      {
     if(dval[strlen(dval) - 1] == '\n')
        dval[strlen(dval) - 1]  = '\0';

     if(dval[strlen(dval) - 1] == '\r')
        dval[strlen(dval) - 1]  = '\0';

     haveType = 1;
     strcpy(tbl_typ_string, dval);

     j     = 0;
     ncolt = 0;

     headlent = (int)strlen(dval);

     for(i=0; i<headlent; ++i)
     {
        if (dval[i] == '\\')
           break;

        if (dval[i] == '\n')
           break;

        else if (dval[i] == '|')
        {
           ++ncolt;

           if(ncolt > MTBL_MAXCOL)
          return(MTBL_COLUMN);

           j = 0;

           if(i == 0)
          --ncolt;
        }

        else if (dval[i] != ' ' )
        {
           tbl_rec[ncolt].type[j] = dval[i];
           ++j;
        }
     }
      }

      else if(!haveUnit)
      {
     if(dval[strlen(dval) - 1] == '\n')
        dval[strlen(dval) - 1]  = '\0';

     if(dval[strlen(dval) - 1] == '\r')
        dval[strlen(dval) - 1]  = '\0';

     haveUnit = 1;
     strcpy(tbl_uni_string, dval);

     j     = 0;
     ncolt = 0;

     headlent = (int)strlen(dval);

     for(i=0; i<headlent; ++i)
     {
        if (dval[i] == '\\')
           break;

        if (dval[i] == '\n')
           break;

        else if (dval[i] == '|')
        {
           ++ncolt;

           if(ncolt > MTBL_MAXCOL)
          return(MTBL_COLUMN);

           j = 0;

           if(i == 0)
          --ncolt;
        }

        else if (dval[i] != ' ' )
        {
           tbl_rec[ncolt].unit[j] = dval[i];
           ++j;
        }
     }
      }

      else if(!haveNull)
      {
     if(dval[strlen(dval) - 1] == '\n')
        dval[strlen(dval) - 1]  = '\0';

     if(dval[strlen(dval) - 1] == '\r')
        dval[strlen(dval) - 1]  = '\0';

     haveNull = 1;
     strcpy(tbl_nul_string, dval);

     j     = 0;
     ncolt = 0;

     headlent = (int)strlen(dval);

     for(i=0; i<headlent; ++i)
     {
        if (dval[i] == '\\')
           break;

        if (dval[i] == '\n')
           break;

        else if (dval[i] == '|')
        {
           ++ncolt;

           if(ncolt > MTBL_MAXCOL)
          return(MTBL_COLUMN);

           j = 0;

           if(i == 0)
          --ncolt;
        }

        else if (dval[i] != ' ' )
        {
           tbl_rec[ncolt].nuls[j] = dval[i];
           ++j;
        }
     }
      }
   }


   /* Read the first data line to get record length */

   if(fgets(dval, mtbl_maxline, tfile) != (char *) NULL)
   {
      if(tdebug)
      {
     printf("TDEBUG> Read data line [%s]<br>\n", dval);
     fflush(stdout);
      }

      reclen = (int)strlen(dval);
   }


   if(tdebug)
   {
       printf("TDEBUG> tbl_hdr_string = [%s]<br>\n", tbl_hdr_string);
       printf("TDEBUG> tbl_typ_string = [%s]<br>\n", tbl_typ_string);
       printf("TDEBUG> tbl_uni_string = [%s]<br>\n", tbl_uni_string);
       printf("TDEBUG> tbl_nul_string = [%s]<br>\n", tbl_nul_string);
       printf("TDEBUG> firsrt record  = [%s](%d)<br>\n", dval, reclen);
       fflush(stdout);

       for (i=0; i<ncol; ++i)
       {
          printf("<br>\n");
          printf("TDEBUG> Column %d:<br>\n",    i+1);
          printf("TDEBUG> name   = [%s]<br>\n", tbl_rec[i].name);
          printf("TDEBUG> type   = [%s]<br>\n", tbl_rec[i].type);
          printf("TDEBUG> unit   = [%s]<br>\n", tbl_rec[i].unit);
          printf("TDEBUG> endcol =  %d<br>\n",  tbl_rec[i].endcol);
          printf("TDEBUG> colwd  =  %d<br>\n",  tbl_rec[i].colwd);
          fflush(stdout);
       }
   }


   /* Estimate the number of records in the file */
   /*  (correct for fixed-record table files)    */

   if(stat(fname, &buf) != 0)
      nrec = -1;

   if(reclen > 0)
      nrec = (buf.st_size - headbytes) / reclen;
   else
      nrec = -1;


   /* Reset pointer and exit */

   tseek(0);

   tbl_headbytes = headbytes;
   tbl_reclen    = reclen;

   return(ncol);
}



int tlen()
{
   return(nrec);
}



int tcol(char *name)
{
   int i;
   for(i=0; i<ncol; ++i)
   {
      if(strcmp(tbl_rec[i].name, name) == 0)
     return(i);
   }

   return(-1);
}



char *tinfo(int col)
{
   if(col < ncol)
      return(tbl_rec[col].name);
   else
      return((char *)NULL);
}



int tkeycount()
{
   return(nkey);
}

int thdrcount()
{
   return (nhdr);
}

char * thdrline(int i)
{
   if ((i < nhdr) && (i >=0))
       return (keystr[i]);
   else
       return ((char *)NULL);
}

char *tkeyname(int i)
{
   if ((i<nkey) && (i>=0))
      return(keyword[i]);
   else
      return((char *)NULL);
}



char *tkeyval(int i)
{
   if(i<nkey)
      return(value[i]);
   else
      return((char *)NULL);
}



char *tfindkey(char *key)
{
   int i;

   for(i=0; i<nkey; ++i)
   {
      if(strcmp(key, keyword[i]) == 0)
     return(value[i]);
   }

   return((char *)NULL);
}



int tseek(int recno)
{
   long offset;

   offset = (long)headbytes + (long)recno * (long)reclen;

   fseek(tfile, offset, SEEK_SET);

   return(offset);
}


int tread()
{
   int i, j;

   for(i=0; i<mtbl_maxline; ++i)
      dval[i] = '\0';

   while(FOREVER)
   {
      if(fgets(dval, mtbl_maxline, tfile) == (char *)NULL)
     return(MTBL_RDERR);

      if(tdebug)
      {
     printf("TDEBUG> Read data line [%s]<br>\n", dval);
     fflush(stdout);
      }

      if(dval[0] != '\\' && dval[0] != '|')
     break;
   }

   if(dval[(int)strlen(dval)-1] == '\n')
      dval[(int)strlen(dval)-1] = '\0';

   if(dval[(int)strlen(dval)-1] == '\r')
      dval[(int)strlen(dval)-1] = '\0';

   strcpy(tbl_rec_string, dval);

   dval[tbl_rec[0].endcol] = '\0';
   tbl_rec[0].dptr = dval;

   for(i=1; i<ncol; ++i)
   {
      dval[tbl_rec[i].endcol] = '\0';
      tbl_rec[i].dptr = dval + tbl_rec[i-1].endcol + 1;
   }

   for(i=0; i<ncol; ++i)
   {
      j = tbl_rec[i].endcol;

      while(FOREVER)
      {
     if(dval[j] != ' ' && dval[j] != '\0')
        break;

     if(j == 0)
        break;

     if(i > 0 && j == tbl_rec[i-1].endcol)
        break;

     dval[j] = '\0';
     --j;
      }

      while(FOREVER)
      {
     if(*(tbl_rec[i].dptr) != ' ')
        break;

     if(*(tbl_rec[i].dptr) == '\0')
        break;

     ++(tbl_rec[i].dptr);
      }
   }

   return(MTBL_OK);
}



char *tval(int col)
{
   if(col < ncol)
      return(tbl_rec[col].dptr);
   else
      return((char *)NULL);
}



int tnull(int col)
{
   if(!haveNull)
      return 0;

   if(col < ncol)
   {
      if(strcmp(tbl_rec[col].dptr,
        tbl_rec[col].nuls) == 0)
     return 1;
      else
     return 0;
   }
   else
      return 1;
}



void tclose()
{
   if(tfile != (FILE *)NULL)
      fclose(tfile);
}


int isBlank(char *str)
{
   int i;

   for(i=0; i<(int)strlen(str); ++i)
      if(str[i] != ' ')
     return(0);

   return(1);
}


struct TBL_INFO *tsave()
{
   int i;

   struct TBL_INFO *tbl_info;

   if(tdebug)
   {
      printf("\nTDEBUG> Saving:\n");
      printf("TDEBUG> ncol           = %d\n", ncol);
      printf("TDEBUG> headbytes      = %d\n", headbytes);
      printf("TDEBUG> reclen         = %d\n", reclen);
      printf("TDEBUG> nrec           = %d\n", nrec);
      printf("TDEBUG> haveType       = %d\n", haveType);
      printf("TDEBUG> haveUnit       = %d\n", haveUnit);
      printf("TDEBUG> haveNull       = %d\n", haveNull);
      printf("TDEBUG> nhdr           = %d\n", nhdr);
      printf("TDEBUG> nkey           = %d\n", nkey);
      printf("TDEBUG> mtbl_maxline   = %d\n", mtbl_maxline);
      printf("TDEBUG> tbl_hdr_string = \"%s\"\n", tbl_hdr_string);
      printf("TDEBUG> tbl_typ_string = \"%s\"\n", tbl_typ_string);
      printf("TDEBUG> tbl_uni_string = \"%s\"\n", tbl_uni_string);
      printf("TDEBUG> tbl_nul_string = \"%s\"\n", tbl_nul_string);
      fflush(stdout);
   }

   tbl_info = (struct TBL_INFO *)malloc(sizeof(struct TBL_INFO));

   tbl_info -> ncol         = ncol;
   tbl_info -> headbytes    = headbytes;
   tbl_info -> reclen       = reclen;
   tbl_info -> nrec         = nrec;
   tbl_info -> haveType     = haveType;
   tbl_info -> haveUnit     = haveUnit;
   tbl_info -> haveNull     = haveNull;
   tbl_info -> nhdr         = nhdr;
   tbl_info -> nkey         = nkey;
   tbl_info -> mtbl_maxline = mtbl_maxline;
   tbl_info -> tfile        = tfile;

   tbl_info -> tbl_hdr_string = (char *)malloc(strlen(tbl_hdr_string)+1);
   tbl_info -> tbl_typ_string = (char *)malloc(strlen(tbl_typ_string)+1);
   tbl_info -> tbl_uni_string = (char *)malloc(strlen(tbl_uni_string)+1);
   tbl_info -> tbl_nul_string = (char *)malloc(strlen(tbl_nul_string)+1);

   strcpy(tbl_info -> tbl_hdr_string, tbl_hdr_string);
   strcpy(tbl_info -> tbl_typ_string, tbl_typ_string);
   strcpy(tbl_info -> tbl_uni_string, tbl_uni_string);
   strcpy(tbl_info -> tbl_nul_string, tbl_nul_string);

   tbl_info -> keystr  = (char **)malloc(nhdr * sizeof(char *));

   tbl_info -> keyword = (char **)malloc(nkey * sizeof(char *));
   tbl_info -> value   = (char **)malloc(nkey * sizeof(char *));

   for(i=0; i<tbl_info->nhdr; ++i)
   {
      tbl_info -> keystr[i]  = (char *)malloc(strlen(keystr[i])+1);

      strcpy(tbl_info -> keystr[i],  keystr[i]);
   }

   for(i=0; i<tbl_info->nkey; ++i)
   {
      tbl_info -> keyword[i] = (char *)malloc(strlen(keyword[i])+1);
      tbl_info -> value[i]   = (char *)malloc(strlen(value[i])+1);

      strcpy(tbl_info -> keyword[i], keyword[i]);
      strcpy(tbl_info -> value[i],   value[i]);
   }

   tbl_info -> tbl_rec
      = (struct TBL_REC *)malloc(tbl_info->ncol * sizeof(struct TBL_REC));

   for(i=0; i<tbl_info->ncol; ++i)
   {
      strcpy(tbl_info -> tbl_rec[i].name, tbl_rec[i].name);
      strcpy(tbl_info -> tbl_rec[i].type, tbl_rec[i].type);
      strcpy(tbl_info -> tbl_rec[i].unit, tbl_rec[i].unit);
      strcpy(tbl_info -> tbl_rec[i].nuls, tbl_rec[i].nuls);

      tbl_info -> tbl_rec[i].endcol = tbl_rec[i].endcol;
      tbl_info -> tbl_rec[i].colwd  = tbl_rec[i].colwd;
   }


   return tbl_info;
}


void tclear()
{
   ncol         = 0;
   headbytes    = 0;
   reclen       = 0;
   nrec         = 0;
   haveType     = 0;
   haveUnit     = 0;
   haveNull     = 0;
   nhdr         = 0;
   nkey         = 0;
   mtbl_maxline = 0;
   tfile        = (FILE *)NULL;

   return;
}


void trestore(struct TBL_INFO *tbl_info)
{
   int i;

   if(tdebug)
   {
      printf("\nTDEBUG> Restoring:\n");
      printf("TDEBUG> ncol           = %d\n", tbl_info -> ncol);
      printf("TDEBUG> headbytes      = %d\n", tbl_info -> headbytes);
      printf("TDEBUG> reclen         = %d\n", tbl_info -> reclen);
      printf("TDEBUG> nrec           = %d\n", tbl_info -> nrec);
      printf("TDEBUG> haveType       = %d\n", tbl_info -> haveType);
      printf("TDEBUG> haveUnit       = %d\n", tbl_info -> haveUnit);
      printf("TDEBUG> haveNull       = %d\n", tbl_info -> haveNull);
      printf("TDEBUG> nhdr           = %d\n", tbl_info -> nhdr);
      printf("TDEBUG> nkey           = %d\n", tbl_info -> nkey);
      printf("TDEBUG> mtbl_maxline   = %d\n", tbl_info -> mtbl_maxline);
      printf("TDEBUG> tbl_hdr_string = \"%s\"\n", tbl_info -> tbl_hdr_string);
      printf("TDEBUG> tbl_typ_string = \"%s\"\n", tbl_info -> tbl_typ_string);
      printf("TDEBUG> tbl_uni_string = \"%s\"\n", tbl_info -> tbl_uni_string);
      printf("TDEBUG> tbl_nul_string = \"%s\"\n", tbl_info -> tbl_nul_string);
      fflush(stdout);
   }


   ncol         = tbl_info -> ncol;
   headbytes    = tbl_info -> headbytes;
   reclen       = tbl_info -> reclen;
   nrec         = tbl_info -> nrec;
   haveType     = tbl_info -> haveType;
   haveUnit     = tbl_info -> haveUnit;
   haveNull     = tbl_info -> haveNull;
   nhdr         = tbl_info -> nhdr;
   nkey         = tbl_info -> nkey;
   mtbl_maxline = tbl_info -> mtbl_maxline;
   tfile        = tbl_info -> tfile;

   strcpy(tbl_hdr_string, tbl_info -> tbl_hdr_string);
   strcpy(tbl_typ_string, tbl_info -> tbl_typ_string);
   strcpy(tbl_uni_string, tbl_info -> tbl_uni_string);
   strcpy(tbl_nul_string, tbl_info -> tbl_nul_string);

   for(i=0; i<nhdr; ++i)
      strcpy(keystr[i],  tbl_info -> keystr[i]);

   for(i=0; i<nkey; ++i)
   {
      strcpy(keyword[i], tbl_info -> keyword[i]);
      strcpy(value[i],   tbl_info -> value[i]);
   }

   for(i=0; i<ncol; ++i)
   {
      strcpy(tbl_rec[i].name, tbl_info -> tbl_rec[i].name);
      strcpy(tbl_rec[i].type, tbl_info -> tbl_rec[i].type);
      strcpy(tbl_rec[i].unit, tbl_info -> tbl_rec[i].unit);
      strcpy(tbl_rec[i].nuls, tbl_info -> tbl_rec[i].nuls);

      tbl_rec[i].endcol = tbl_info -> tbl_rec[i].endcol;
      tbl_rec[i].colwd  = tbl_info -> tbl_rec[i].colwd;
   }

   return;
}


void tfree(struct TBL_INFO *tbl_info)
{
   int i;

   free(tbl_info -> tbl_hdr_string);
   free(tbl_info -> tbl_typ_string);
   free(tbl_info -> tbl_uni_string);
   free(tbl_info -> tbl_nul_string);

   for(i=0; i<tbl_info->nhdr; ++i)
      free(tbl_info -> keystr[i]);

   for(i=0; i<tbl_info->nkey; ++i)
   {
      free(tbl_info -> keyword[i]);
      free(tbl_info -> value[i]);
   }

   free(tbl_info -> keystr);
   free(tbl_info -> keyword);
   free(tbl_info -> value);

   free(tbl_info -> tbl_rec);

   free(tbl_info);

   tbl_info = (struct TBL_INFO *)NULL;

   return;
}
