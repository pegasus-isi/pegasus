/* Module: mPix2Coord

Version  Developer        Date     Change
-------  ---------------  -------  -----------------------
1.0      John Good        17Dec08  Baseline code

*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*************************************************************************/
/*                                                                       */
/*  mFixHdr                                                              */
/*                                                                       */
/*  Montage is a set of general reprojection / coordinate-transform /    */
/*  mosaicking programs.  Any number of input images can be merged into  */
/*  an output FITS file.  The attributes of the input are read from the  */
/*  input files; the attributes of the output are read a combination of  */
/*  the command line and a FITS header template file.                    */
/*                                                                       */
/*  This module takes a FITS header block (new newlines) and outputs     */
/*  the kind of multi-line header file Montages uses internally.         */
/*                                                                       */
/*************************************************************************/

int main(int argc, char **argv)
{
   int i, ch, done; 
   char  infile[1024];
   char outfile[1024];
   char line   [128];

   char *ptr;

   FILE *fin;
   FILE *fout;

   if(argc < 3)
   {
      printf("[struct stat=\"ERROR\" msg=\"Usage: mFitHdr infile outfile\"]\n");
      exit(1);
   }

   strcpy(infile,  argv[1]);
   strcpy(outfile, argv[2]);

   fin = fopen(infile, "r");

   if(fin == (FILE *)NULL)
   {
      printf("[struct stat=\"ERROR\" msg=\"File [%s] cannot be read.\"]\n", infile);
      exit(1);
   }

   fout = fopen(outfile, "w+");

   if(fout == (FILE *)NULL)
   {
      printf("[struct stat=\"ERROR\" msg=\"File [%s] cannot be opened for writing.\"]\n", outfile);
      exit(1);
   }

   done = 0;

   while(1)
   {
      for(i=0; i<80; ++i)
      {
	 ch = fgetc(fin);

	 if(ch == EOF)
	 {
	    if(i == 0)
	    {
	       done = 1;
	       break;
	    }

	    printf("[struct stat=\"ERROR\" msg=\"Incomplete header line in [%s] (all must be 80 characters.\"]\n", infile);
	    exit(1);
	 }
      
	 line[i] = (char)ch;
      }

      if(done)
	 break;
      
      line[80] = '\0';

      ptr = line + 79;

      while(ptr > line && (*ptr == ' ' || *ptr == '\0'))
      {
	 *ptr = '\0';
	 --ptr;
      }

      fprintf(fout, "%s\n", line);
      fflush(fout);

      if(strcmp(line, "END") == 0)
	 break;
   }

   fclose(fin);
   fclose(fout);

   exit(0);
}
