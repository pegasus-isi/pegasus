#include <stdio.h>
#include <math.h>
#include <wcs.h>
#include <two_plane.h>
#include <distort.h>

#define MAXSTR  256

char    intemplate[MAXSTR];
char   outtemplate[MAXSTR];

char    inheader[80000];
char   outheader[80000];

struct WorldCoor *input, *output;
struct TwoPlane   twoplane;

int main(int argc, char **argv)
{

   int    j, offscl, status;

   double xsky, ysky;
   double xpix1, ypix1;
   double xpix2, ypix2;

   strcpy( intemplate, argv[1]);
   strcpy(outtemplate, argv[2]);

   readHeaders();

   status = Initialize_TwoPlane_BothDistort(&twoplane, inheader, outheader);

   for (j=0; j<input->nxpix+1; ++j)
   {
      offscl = plane1_to_plane2_transform(j+0.5, j+0.5,
	                                &xpix1, &ypix1, &twoplane);


      printf("Twoplane lib: (%12.5f,%12.5f) -> (%12.5f,%12.5f)\n",
	 j+0.5, j+0.5, xpix1, ypix1);

      pix2wcs( input, j+0.5, j+0.5, &xsky,   &ysky);
      wcs2pix(output,  xsky,  ysky, &xpix2, &ypix2, &offscl);

      printf("WCS lib:      (%12.5f,%12.5f) -> (%12.5f,%12.5f)  Sky: [%12.5f,%12.5f]\n\n",
	 j+0.5, j+0.5, xpix2, ypix2, xsky, ysky);
   }

   exit(0);
}



readHeaders()
{
   char      line[MAXSTR];
   int       i, j;
   FILE     *fp;


   /* Input header */

   fp = fopen(intemplate, "r");

   strcpy(inheader, "");

   for(j=0; j<1000; ++j)
   {
      if(fgets(line, MAXSTR, fp) == (char *)NULL)
         break;

      if(line[strlen(line)-1] == '\n')
         line[strlen(line)-1]  = '\0';
      
      if(line[strlen(line)-1] == '\r')
	 line[strlen(line)-1]  = '\0';

      stradd(inheader, line);
   }

   input = wcsinit(inheader);

   fclose(fp);


   /* Output header */

   fp = fopen(outtemplate, "r");

   strcpy(outheader, "");

   for(j=0; j<1000; ++j)
   {
      if(fgets(line, MAXSTR, fp) == (char *)NULL)
         break;

      if(line[strlen(line)-1] == '\n')
         line[strlen(line)-1]  = '\0';
      
      if(line[strlen(line)-1] == '\r')
	 line[strlen(line)-1]  = '\0';
      
      stradd(outheader, line);
   }

   output = wcsinit(outheader);

   fclose(fp);

   return;
}


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
