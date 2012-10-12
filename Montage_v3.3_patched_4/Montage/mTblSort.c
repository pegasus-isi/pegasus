/* Module: mTblSort.c

Version  Developer        Date     Change
-------  ---------------  -------  -----------------------
1.1      John Good        25Jun07  Add check for empty table  
                                    (no data records)
1.0      John Good        06Sep06  Baseline code
*/


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mtbl.h>

#define MAXDATA  4096
#define MAXSTR  16384

int    *recno;
double *data;
int     ndata, maxdata;

int     debug = 0;
int     flip  = 1;

void    qksort(int ilo, int ihi);


/*******************************************************************/
/*                                                                 */
/*  mTblSort                                                       */
/*                                                                 */
/*  Output is a sorted copy of the input table.  Only sorts on     */
/*  numeric values.                                                */
/*                                                                 */
/*******************************************************************/

int main(int argc, char **argv)
{
   char    tblname[1024];
   char    outname[1024];
   char    colname[1024];

   char    line[MAXSTR];

   FILE   *fin, *fout;

   int     i, ncols, icol;
   int     foundHdr, irec;


   /* Process command-line arguments */

   if(argc < 4)
   {
      printf("[struct stat=\"ERROR\", msg=\"Usage: %s [-d] in.tbl colname out.tbl\"]\n",
	 argv[0]);
      fflush(stdout);
      exit(0);
   }

   for(i=1; i<argc; ++i)
   {
      if(strcmp(argv[i], "-d") == 0)
	 debug = 1;

      if(strcmp(argv[i], "-r") == 0)
	 flip = -1;
   }

   if(debug)
   {
      ++argv;
      --argc;
   }

   if(flip == -1)
   {
      ++argv;
      --argc;
   }

   if(argc < 4)
   {
      printf("[struct stat=\"ERROR\", msg=\"Usage: %s [-d] in.tbl colname out.tbl\"]\n",
	 argv[0]);
      fflush(stdout);
      exit(0);
   }


   strcpy(tblname, argv[1]);
   strcpy(colname, argv[2]);
   strcpy(outname, argv[3]);


   /* Allocate memory */

   maxdata = MAXDATA;

   data  = (double *)malloc(maxdata*sizeof(double));
   recno = (int    *)malloc(maxdata*sizeof(int));

   if(data  == (double *)NULL
   || recno == (int    *)NULL)
   {
      printf("[struct stat=\"ERROR\", msg=\"Cannot allocate memory for data\"]\n");
      fflush(stdout);
      exit(0);
   }


   /* Copy header info */

   fin = fopen(tblname, "r");

   if(fin == (FILE *)NULL)
   {
      printf("[struct stat=\"ERROR\", msg=\"Cannot open input file\"]\n");
      fflush(stdout);
      exit(0);
   }

   fout = fopen(outname, "w+");

   if(fout == (FILE *)NULL)
   {
      printf("[struct stat=\"ERROR\", msg=\"Cannot open output file\"]\n");
      fflush(stdout);
      exit(0);
   }

   foundHdr = 0;

   while(1)
   {
      fgets(line, MAXSTR, fin);

      while(line[strlen(line) - 1] == '\n'
         || line[strlen(line) - 1] == '\r')
            line[strlen(line) - 1]  = '\0';

      if(foundHdr && line[0] != '|')
	 break;
      
      if(line[0] == '|')
	 foundHdr = 1;

      fprintf(fout, "%s\n", line);
      fflush(fout);
   }

   fclose(fin);


   /* Read through the table file,       */
   /* collecting the values to be sorted */

   ncols = topen(tblname);

   if(ncols <= 0)
   {
      printf("[struct stat=\"ERROR\", msg=\"Table file open failed for [%s]\"]\n",
	 tblname);
      fflush(stdout);
      exit(0);
   }

   if(debug)
   {
      printf("DEBUG> nrec = %d\n", tlen());
      fflush(stdout);
   }

   if(tlen() < 0)
   {
      printf("[struct stat=\"ERROR\", msg=\"Table file does not have fixed-length records\"]\n");
      fflush(stdout);
      exit(0);
   }

   icol = tcol(colname);

   if(icol < 0)
   {
      printf("[struct stat=\"ERROR\", msg=\"Table does not contain column [%s]\"]\n",
	 colname);
      fflush(stdout);
      exit(0);
   }

   ndata = 0;

   while(1)
   {
      if(tread())
	 break;
      
      data [ndata] = atof(tval(icol));
      recno[ndata] = ndata;

      ++ndata;

      if(ndata >= maxdata)
      {
	 maxdata += MAXDATA;

	 data  = (double *)realloc(data,  maxdata*sizeof(double));
	 recno = (int    *)realloc(recno, maxdata*sizeof(int));

	 if(data  == (double *)NULL
	 || recno == (int    *)NULL)
	 {
	    printf("[struct stat=\"ERROR\", msg=\"Cannot allocate memory for data\"]\n");
	    fflush(stdout);
	    exit(0);
	 }
      }
   }

   if(!ndata)
   {
      printf("[struct stat=\"ERROR\", msg=\"Table has no data records\"]\n");
      fflush(stdout);
      exit(0);
   }
   


   /* Sort the data values (reordering the record numbers) */

   if(debug)
   {
      printf("DEBUG> %d data values found\n", ndata);
      fflush(stdout);
   }

   qksort(0, ndata-1);

   if(debug)
   {
      printf("DEBUG> sorting done\n");
      fflush(stdout);
   }


   /* Seek through the table, copying records */
   /* in this new order to the output file    */

   for(i=0; i<ndata; ++i)
   {
      irec = recno[i];

      tseek(irec);
      tread();

      fprintf(fout, "%s\n", tbl_rec_string);
   }

   fflush(fout);
   fclose(fout);

   if(debug)
   {
      printf("DEBUG> copying done\n");
      fflush(stdout);
   }

   printf("[struct stat=\"OK\", count=%d]\n", ndata);
   fflush(stdout);

   exit(0);
}



void qksort(int ilo, int ihi) 
{
   double pivot;        /* pivot value for partitioning array                   */
   int    ulo, uhi;     /* indices at ends of unpartitioned region              */
   int    ieq;          /* least index of array entry with value equal to pivot */
   double tempEntry;    /* temporary entry used for swapping                    */
   int    tempRecno;    /* temporary entry used for swapping                    */

   if (ilo >= ihi) 
      return;
   

   /* Select a pivot value. */

   pivot = data[(ilo + ihi)/2];


   /* Initialize ends of unpartitioned region and     */
   /* least index of entry with value equal to pivot. */

   ieq = ulo = ilo;
   uhi = ihi;


   /* While the unpartitioned region is  */
   /* not empty, try to reduce its size. */

   while (ulo <= uhi) 
   {
      if (flip*data[uhi] > flip*pivot) 
      {
	 /* Here, we can reduce the size of the */
	 /* unpartitioned region and try again. */

	 uhi--;
      }
      else 
      {
	 /* Here, data[uhi] <= pivot, so swap */
	 /* entries at indices ulo and uhi.   */

	 tempEntry  = data[ulo];
	 data[ulo]  = data[uhi];
	 data[uhi]  = tempEntry;

	 tempRecno  = recno[ulo];
	 recno[ulo] = recno[uhi];
	 recno[uhi] = tempRecno;


	 /* After the swap, data[ulo] <= pivot. */

	 if (flip*data[ulo] < flip*pivot) 
	 {
	    /* Swap entries at indices ieq and ulo. */

	    tempEntry  = data[ieq];
	    data[ieq]  = data[ulo];
	    data[ulo]  = tempEntry;

	    tempRecno  = recno[ieq];
	    recno[ieq] = recno[ulo];
	    recno[ulo] = tempRecno;


	    /* After the swap, data[ieq] < pivot, */
	    /* so we need to change ieq.          */

	    ieq++;


	    /* We also need to change ulo, but we also need to do  */
	    /* that when data[ulo] = pivot, so we do it after this */
	    /* if statement.                                       */
	 }
	 

	 /* Once again, we can reduce the size of   */
	 /* the unpartitioned region and try again. */

	 ulo++;
      }
   }


   /* Now, all entries from index ilo to ieq - 1 are less than the pivot */
   /* and all entries from index uhi to ihi + 1 are greater than the     */
   /* pivot.  So we have two regions of the array that can be sorted     */
   /* recursively to put all of the entries in order.                    */

   qksort(ilo, ieq - 1);
   qksort(uhi + 1, ihi);

   return;
}
