#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mtbl.h>

#define MAXSTR 256


main(argc, argv)
int argc;
char **argv;
{
   char   intbl   [MAXSTR];
   char   outxml  [MAXSTR];

   char   objstr  [MAXSTR];

   char   xcolname[MAXSTR];
   char   xlabel  [MAXSTR];

   char   ycolname[MAXSTR];
   char   ylabel  [MAXSTR];

   int    xind, yind;

   double xmin, xmax;
   double ymin, ymax;
   double xval, yval;

   int    i, icol, ncol;
   int    nrow, stat;
   char  *name, *value;

   FILE  *fout;


   /* Process command-line arguments */

   if(argc < 8)
   {
      printf("[struct stat=\"ERROR\", msg=\"Usage:  %s in.tbl out.xml objstr xcolname xlabel ycolname ylabel\"]\n", argv[0]);
      fflush(stdout);
      exit(0);
   }

   strcpy(intbl,    argv[1]);
   strcpy(outxml,   argv[2]);
   strcpy(objstr,   argv[3]);
   strcpy(xcolname, argv[4]);
   strcpy(xlabel,   argv[5]);
   strcpy(ycolname, argv[6]);
   strcpy(ylabel,   argv[7]);


   /* Open table */

   ncol = topen(intbl);
   if(ncol < 0)
   {
      printf("[struct stat=\"ERROR\", msg=\"Error opening table %s\"]\n", 
	 intbl);
      fflush(stdout);
      exit(0);
   }


   /* Find the columns to plot */

   xind = tcol(xcolname);
   yind = tcol(ycolname);

   if(xind < 0)
   {
      printf("[struct stat=\"ERROR\", msg=\"Can't find column %s\"]\n", 
	 xcolname);
      fflush(stdout);
      exit(0);
   }

   if(yind < 0)
   {
      printf("[struct stat=\"ERROR\", msg=\"Can't find column %s\"]\n", 
	 ycolname);
      fflush(stdout);
      exit(0);
   }


   /* Open output XML file */

   fout = fopen(outxml, "w+");

   if(fout < 0)
   {
      printf("[struct stat=\"ERROR\", msg=\"Error opening output XML file %s\"]\n", 
	 outxml);
      fflush(stdout);
      exit(0);
   }


   /* Read the rows and compute x,y ranges */

   nrow = 0;
   while(1)
   {
      stat = tread();

      if(stat < 0)
	 break;

      ++nrow;

      xval = atof(tval(xind));
      yval = atof(tval(yind));

      if(nrow == 1)
      {
	 xmin = xval;
	 xmax = xval;
	 ymin = yval;
	 ymax = yval;
      }

      if(xval < xmin) xmin = xval;
      if(xval > xmax) xmax = xval;
      if(yval < ymin) ymin = yval;
      if(yval > ymax) ymax = yval;
   }


   /* Go back to the beginning */

   tseek(0);


   /* Create the XML header */

   fprintf(fout, "<?xml version=\"1.0\" ?>\n");
   fprintf(fout, "\n");
   fprintf(fout, "<XYPlot>\n");
   fprintf(fout, "\n");
   fprintf(fout, "<plotcols xaxis=\"%s\" yaxis=\"%s\"/>\n", 
                 xcolname, ycolname);
   fprintf(fout, "\n");
   fprintf(fout, "<axis name=\"%s\">\n", xcolname);
   fprintf(fout, "   <axislabel>%s</axislabel>\n", xlabel);
   fprintf(fout, "   <min>%16.8f</min>\n", xmin);
   fprintf(fout, "   <max>%16.8f</max>\n", xmax);
   fprintf(fout, "</axis>\n");
   fprintf(fout, "\n");
   fprintf(fout, "<axis name=\"%s\">\n", ycolname);
   fprintf(fout, "   <axislabel>%s</axislabel>\n", ylabel);
   fprintf(fout, "   <min>%16.8f</min>\n", ymin);
   fprintf(fout, "   <max>%16.8f</max>\n", ymax);
   fprintf(fout, "</axis>\n");
   fprintf(fout, "\n");
   fprintf(fout, "\n");
   fprintf(fout, "<pointset name    = \"%s\"\n", objstr);
   fprintf(fout, "          type    = \"unconnected\"\n");
   fprintf(fout, "          symbol  = \"3\">\n");
   fprintf(fout, "\n");
   fprintf(fout, "\n");
   fprintf(fout, "<asciitable type=\"delim\">\n");
   fprintf(fout, "\n");


   /* Print out the column names */

   for(i=0; i<ncol; ++i)
   {
      name = tinfo(i);
      fprintf(fout, "<column name=\"%s\"/>\n", name);
   }


   /* Print out the data table */

   fprintf(fout, "\n<data>\n");

   while(1)
   {
      stat = tread();

      if(stat < 0)
	 break;

      ++nrow;

      for(i=0; i<ncol-1; ++i)
      {
	 value = tval(i);
	 fprintf(fout, "%s|", value);
      }

      value = tval(i);
      fprintf(fout, "%s\n", value);
   }


   /* Close table */

   fprintf(fout, "</data>\n");
   fprintf(fout, "</asciitable>\n");
   fprintf(fout, "</pointset>\n");
   fprintf(fout, "</XYPlot>\n");

   tclose();
   fclose(fout);

   printf("[struct stat=\"OK\", nrow=%d]\n", nrow);
   fflush(stdout);
   exit(0);
}
