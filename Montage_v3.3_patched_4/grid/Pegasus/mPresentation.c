/* Module: mPresentation.c

Version  Developer        Date     Change
-------  ---------------  -------  -----------------------
1.3      John Good        03Dec03  One more "rimages.tbl" reference
1.2      John Good        03Dec03  Changed the names of some files
				   referenced in output HTML
1.1      John Good        01Dec03  Removed extraneous "http://" strings
1.0      John Good        04Nov03  Baseline code

*/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <strings.h>

#define MAXLEN 20000

int debug = 0;


/*********************************************/
/*                                           */
/* mPresentation -- Generate HTML wrapper    */
/* around final mosaic FITS tiles, thumbnail */
/* and JPEG.                                 */
/*                                           */
/*********************************************/

int main(int argc, char **argv)
{
   int    i, j, nx, ny;
  
   char   urlbase   [MAXLEN];
   char   filebase  [MAXLEN];
   char   locstr    [MAXLEN];
   char   size      [MAXLEN];
   char   band      [MAXLEN];
   char   outfile   [MAXLEN];

   FILE  *fout;


   /* Get the Base name, tiling counts */
   /* and output file name             */

   if(argc < 9)
   {
      printf("[struct stat=\"ERROR\", msg=\"Usage: %s urlbase filebase locstr size band N M output.html\"]\n", argv[0]);
      exit(0);
   }

   strcpy(urlbase,  argv[1]);
   strcpy(filebase, argv[2]);
   strcpy(locstr,   argv[3]);
   strcpy(size,     argv[4]);
   strcpy(band,     argv[5]);

   if(band[0] == 'j') strcpy(band, "J");
   if(band[0] == 'h') strcpy(band, "H");
   if(band[0] == 'k') strcpy(band, "K<sub>s</sub>");

   if(band[0] == 'J') strcpy(band, "J");
   if(band[0] == 'H') strcpy(band, "H");
   if(band[0] == 'K') strcpy(band, "K<sub>s</sub>");

   nx = atoi(argv[6]);
   ny = atoi(argv[7]);

   if(nx <= 0 || ny <=0)
   {
      printf("[struct stat=\"ERROR\", msg=\"Tile counts must be greater than zero in both directions\"]\n");
      exit(0);
   }

   strcpy(outfile, argv[8]);

   fout = fopen(outfile, "w+");

   if(fout == (FILE *)NULL)
   {
      printf("[struct stat=\"ERROR\", msg=\"Cannot open output file [%s]\"]\n", outfile);
      exit(0);
   }


   /* Create the HTML summary */

   fprintf(fout, "<html>\n");
   fprintf(fout, "<head>\n");
   fprintf(fout, "<title>2MASS Mosaic: %s</title>\n", locstr);
   fprintf(fout, "\n");
   fprintf(fout, "<center>\n");
   fprintf(fout, "<table cellpadding=\"5\" border=\"1\">\n");
   fprintf(fout, "  <tr>\n");
   fprintf(fout, "    <td colspan=3 align=\"center\" bgcolor=\"#669999\">\n");
   fprintf(fout, "      <font size=\"+1\" color=\"ffffff\">\n");
   fprintf(fout, "        <b>%s</b> &nbsp;&nbsp; %s degrees &nbsp;&nbsp; 2MASS %s\n", locstr, size, band);
   fprintf(fout, "      </font>\n");
   fprintf(fout, "    </td>\n");
   fprintf(fout, "  </tr>\n");
   fprintf(fout, "  <tr>\n");
   fprintf(fout, "    <td colspan=\"3\" bgcolor=\"#d3d3d3\">\n");
   fprintf(fout, "      <center>\n");
   fprintf(fout, "        <img src=%s/shrunken_%s.jpg width=600 alt=\"Region mosaic image\"><br>\n", 
	   urlbase, filebase);
   fprintf(fout, "      </center>\n");
   fprintf(fout, "    </td>\n");
   fprintf(fout, "   </tr>\n");
   fprintf(fout, "   <tr>\n");
   fprintf(fout, "     <td bgcolor=\"#d3d3d3\">\n");
   fprintf(fout, "      <center>\n");
   fprintf(fout, "      <a href=\"%s/shrunken_%s.fits\">FITS file</a>&nbsp;&nbsp;&nbsp;\n",
	   urlbase, filebase);
   fprintf(fout, "      <a href=\"http://irsa.ipac.caltech.edu/cgi-bin/OasisLink/nph-oasislink?ref=%s/shrunken_%s.fits\">\n", urlbase, filebase);
   fprintf(fout, "\n");
   fprintf(fout, "         <img align=middle src=http://irsa.ipac.caltech.edu/applications/Oasis/images/oasislink.gif alt=\"Send to Oasis\"></a>\n");
   fprintf(fout, "      </center>\n");
   fprintf(fout, "     </td>\n");
   fprintf(fout, "     </td>\n");
   fprintf(fout, "     <td bgcolor=\"#d3d3d3\">\n");
   fprintf(fout, "      <center>\n");
   fprintf(fout, "      <a href=\"%s/template.hdr\">FITS header</a>\n", urlbase);
   fprintf(fout, "      </center>\n");
   fprintf(fout, "     </td>\n");
   fprintf(fout, "     <td bgcolor=\"#d3d3d3\">\n");
   fprintf(fout, "      <center>\n");
   fprintf(fout, "      <a href=\"%s/images.tbl\">Image metadata</a>&nbsp;&nbsp;&nbsp;&nbsp;\n", urlbase);
   fprintf(fout, "      <a href=\"http://irsa.ipac.caltech.edu/cgi-bin/OasisLink/nph-oasislink?ref=%s/images.tbl\">\n", urlbase);
   fprintf(fout, "         <img align=middle src=http://irsa.ipac.caltech.edu/applications/Oasis/images/oasislink.gif alt=\"Send to Oasis\"></a>\n");
   fprintf(fout, "      </center>\n");
   fprintf(fout, "  </tr>\n");
   fprintf(fout, "</table><p>\n");
   fprintf(fout, "\n");

   if(nx > 1 || ny > 1)
   {
      fprintf(fout, "<table border=1 cellpadding=5 bgcolor=\"#e3e3e3\">\n");

      for(j=ny-1; j>=0; --j)
      {
	 fprintf(fout, "   <tr>\n");

	 for(i=0; i<nx; ++i)
	 {
	    fprintf(fout, "   <td><center>\n");
	    fprintf(fout, "      <a href=\"%s/%s_%d_%d.fits\">Tile %d %d<br>\n", 
	       urlbase, filebase, i, j, i, j);
	    fprintf(fout, "      <a href=\"http://irsa.ipac.caltech.edu/cgi-bin/OasisLink/nph-oasislink?ref=%s/%s_%d_%d.fits\">\n", urlbase, filebase, i, j);
	    fprintf(fout, "      <img align=middle src=http://irsa.ipac.caltech.edu/applications/Oasis/images/oasislink.gif alt=\"Send to Oasis\"></a>\n");
	    fprintf(fout, "   </td>\n"); 
	 }

	 fflush(fout); 
      }

      fprintf(fout, "</table>\n");
   }
   else
   {
      fprintf(fout, "<a href=\"%s/%s.fits\">Full-Resolution FITS Mosaic<br>\n", 
	 urlbase, filebase);
      fprintf(fout, "<a href=\"http://irsa.ipac.caltech.edu/cgi-bin/OasisLink/nph-oasislink?ref=%s/%s.fits\">\n", urlbase, filebase);
      fprintf(fout, "<img align=middle src=http://irsa.ipac.caltech.edu/applications/Oasis/images/oasislink.gif alt=\"Send to Oasis\"></a>\n");
   }

   fprintf(fout, "</center>\n");
   fprintf(fout, "\n");
   fprintf(fout, "</body>\n");
   fprintf(fout, "</html>\n");

   fclose(fout);


   printf("[struct stat=\"OK\"]\n");
   fflush(stdout);

   exit(0);
}
