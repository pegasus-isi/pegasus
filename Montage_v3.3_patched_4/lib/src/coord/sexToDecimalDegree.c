#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include <math.h>

#define  FALSE   0
#define  TRUE  !FALSE

void getSubstrings();
int checkFormat(char *);


/***********************************************************************/
/*                                                                     */
/*   sexToDegree  Converts the sexgesimal longitude and latitude       */
/*   -----------  to the decimal longitude and latitude.               */
/*                                                                     */
/*    Input :  sexgesimal longitude and latitude                       */
/*                                                                     */
/*             "xxhxxmxx.xs", "xx xx xx.x",  "xx:xx:xx.x",  "xxxxxx"   */
/*             "xxdxxmxx.xs", "xx xx xx.x",  "xx:xx:xx.x",  "xxxxxx"   */
/*                                                                     */
/*    Output:  decimal longtitude and latitude                         */
/*             xxx.xxxx,  xxxx.xxxx                                    */
/*                                                                     */
/*    Return value:                                                    */     
/*             0  both are converted successfully                      */
/*             1  syntax error in longitude                            */
/*             2  syntax error in latitude                             */
/*             3  syntax error in both                                 */
/*                                                                     */
/***********************************************************************/


int sexToDegree(char *cra, char *cdec, double *ra, double *dec)
{
   char    crain[40], cdecin[40], *subst[10], teststr[40], coordin[40];
   char    tmph[40], tmpd[40], tmpm[40], tmps[40];
   char    chr, *p, *end;
   int     ideg, syntax, lonstatus, latstatus, testlen;
   double  scale, hr, deg, min, sec, testval;
   int     i, leng;
   int     isign, nsub, nh, nd, nm, ns;


   strcpy(crain,  cra);
   strcpy(cdecin, cdec);



   /*  RA  */

   ideg = 0;
   hr   = 0;
   min  = 0;
   sec  = 0;
 
   strcpy(tmph , "0");
   strcpy(tmpm , "0");
   strcpy(tmps , "0");


   /*  Determine the Sign on the RA  */

   strcpy(coordin, crain);

   isign = 1;
   leng = strlen(crain);

   i = 0;
   while (crain[i] == ' ')
      i++;

   if (crain[i] == '-')
   {
      isign = -1;
      crain[i] = ' ';
   }
   else if (crain[i] == '+') 
      crain[i] = ' ';


   /*  Find the positions of the 'h' (or 'd'), 'm', and 's'  */

   nd = 0;
   nh = -1;
   nm = -1;
   ns = -1;
   leng = strlen(crain);
   for(i = 0; i<=leng-1; i++)
   {
      chr = crain[i];
      if (chr == 'h'  ||  chr == 'H') 
	 nh = i;
      
      else if (chr == 'd' ||  chr == 'D') 
      {
	 ideg = 1;
	 nh = i;
      }

      if (chr == 'm' || chr == 'M') 
	 nm = i;

      if (chr == 's' || chr == 'S') 
	 ns = i;
   }


   /*  See if the HMS identifiers are out of order  */

   if((nh > 0 && nm > 0 && nh > nm) 
   || (nh > 0 && ns > 0 && nh > ns)
   || (nm > 0 && ns > 0 && nm > ns))
      return 1;


   /*  Extract the Integer Hours, Minutes, and Seconds  */

   if (nh > 0) 
   {
      p = crain;
      strcpy(tmph , p);
      tmph[nh] ='\0';
   }

   if (nm > 0)
   {
      if (nh > 0 && nm-1 > nh) 
      {
	 p = crain+nh+1;
	 strcpy(tmpm , p);
	 tmpm[nm-nh-1] = '\0';
      }
      else
      {
	 p = crain;
	 strcpy(tmpm , p);
	 tmpm[nm] = '\0';
      }
   }

   if (ns > 0)
   {
      if(nm > 0 && ns-1 > nm) 
      {
	 p = crain+nm+1;
	 strcpy(tmps , p);
	 tmps[ns-nm-1] ='\0';
      }
      else if(nh > 0 && ns-1 > nh) 
      {
	 p = crain+nh+1;
	 strcpy(tmps , p);
	 tmps[ns-nh-1] ='\0';
      }
      else
      {
	 p = crain;
	 strcpy(tmps , p);
	 tmps[ns] ='\0';
      }
   }


   /*  If There are No 'hms', Find the Substrings  */

   nsub = 0;

   if (nh == -1 && nm == -1 && ns == -1) 
   {
      getSubstrings(crain, subst, &nsub);
      if (nsub >= 3) 
      {
	 strcpy(tmph , subst[0]);
	 strcpy(tmpm , subst[1]);
	 strcpy(tmps , subst[2]);
      }

      if (nsub == 2) 
      {
	 strcpy(tmpm , subst[0]);
	 strcpy(tmps , subst[1]);
      }

      if (nsub == 1) 
	 strcpy(tmps , subst[0]);
   }

   else if (nm == -1 && ns == -1)
   {
      getSubstrings( (crain+nh+1), subst, &nsub);
      if (nsub == 2) 
      {
	 strcpy(tmpm , subst[0]);
	 strcpy(tmps , subst[1]);
      }

      if (nsub == 1) 
	 strcpy(tmpm , subst[0]);
   }

   else if (ns == -1)
   {
      getSubstrings( (crain+nm+1), subst, &nsub);
      if (nsub == 1) 
      strcpy(tmps , subst[0]);
   }


   /*  If the Number is Too Big, Assume +hhmmss.s  */

   syntax = checkFormat(tmps);

   if (syntax == 1)
   {
      syntax = checkFormat(tmpm);
      syntax = checkFormat(tmph);
   }

   lonstatus = syntax;

   if (syntax == 1)
   {
      sec = atof(tmps);

      if (sec >  60.) 
      {
	 i = (int) (sec/10000.);
	 hr = (double) i;
	 i = (int) ((sec - hr*10000.) / 100.);
	 min = (double) i;
	 sec = sec - hr*10000. - min*100.;
      }
      else
      {
	 hr =  atof(tmph);
	 min = atof(tmpm);
	 sec = atof(tmps);
      }
   } 


   /*  Generate the RA in Decimal Degrees  */

   scale = 15.;
   if (ideg  ==  1) 
      scale = 1.;

   *ra = isign * scale * (hr + (min/60.) + (sec/3600.));


   /* If the user mixes modes (e.g. sexegesimal lon      */
   /* and decimal degree lat), we need to double check   */
   /* supposedly sexegesimal strings here to see if they */
   /* should more reasonably be interpreted as decimal   */
   /* degrees.                                           */

   strcpy(teststr, coordin);

   testlen = strlen(teststr);

   testval = strtod(teststr, &end);

   for(i=0; i<strlen(teststr); ++i)
      if(teststr[i] == '.')
         teststr[i]  = '\0';

   if(end == teststr + testlen && strlen(teststr) < 5)
   {
      *ra = testval;

      lonstatus = 0;
   }


   /*  DEC  */

   deg = 0;
   min = 0;
   sec = 0;
   strcpy(tmpd , "0");
   strcpy(tmpm , "0");
   strcpy(tmps , "0");


   /*  Determine the Sign on the Dec  */

   strcpy(coordin, cdecin);

   isign = 1;
   leng = strlen(cdecin);

   i = 0;
   while (cdecin[i] ==' ')
      i++;

   if (cdecin[i] == '-') 
   {
      isign = -1;
      cdecin[i] = ' ';
   }
   else if (cdecin[i] == '+') 
      cdecin[i] = ' ';


   /*  Find the positions of the 'h' (or 'd'), 'm', and 's'  */

   nd = -1;
   nm = -1;
   ns = -1;
   leng = strlen(cdecin);
   for(i = 0; i<=leng-1; i++)
   {
      chr = cdecin[i];
      if (chr == 'd' ||  chr == 'D') 
	 nd = i;

      if (chr == 'm' || chr == 'M') 
	 nm = i;

      if (chr == 's' || chr == 'S') 
	 ns = i;
   }

   /*  See if the DMS identifiers are out of order  */

   if((nd > 0 && nm > 0 && nd > nm) 
   || (nd > 0 && ns > 0 && nd > ns)
   || (nm > 0 && ns > 0 && nm > ns))
      return 1;


   /*  Extract the Integer Hours, Minutes, and Seconds  */

   if (nd > 0) 
   {
      p = cdecin;
      strcpy(tmpd , p);
      tmpd[nd] ='\0';
   }

   if (nm > 0)
   {
      if (nd > 0 && nm-1 > nd) 
      {
	 p = cdecin+nd+1;
	 strcpy(tmpm , p);
	 tmpm[nm-nd-1] = '\0';
      }
      else
      {
	 p = cdecin;
	 strcpy(tmpm , p);
	 tmpm[nm] = '\0';
      }
   }

   if (ns > 0)
   {
      if(nm > 0 && ns-1 > nm) 
      {
	 p = cdecin+nm+1;
	 strcpy(tmps , p);
	 tmps[ns-nm-1] ='\0';
      }
      else if(nd > 0 && ns-1 > nd) 
      {
	 p = cdecin+nd+1;
	 strcpy(tmps , p);
	 tmps[ns-nd-1] ='\0';
      }
      else
      {
	 p = cdecin;
	 strcpy(tmps , p);
	 tmps[ns] ='\0';
      }
   }


   /*  If There are No 'hms', Find the Substrings  */

   nsub = 0;
   if (nd == -1 && nm == -1 && ns == -1) 
   {
      getSubstrings(cdecin, subst, &nsub);
      if (nsub >= 3) 
      {
	 strcpy(tmpd , subst[0]);
	 strcpy(tmpm , subst[1]);
	 strcpy(tmps , subst[2]);
      }

      if (nsub == 2) 
      {
	 strcpy(tmpm , subst[0]);
	 strcpy(tmps , subst[1]);
      }

      if (nsub == 1) 
	 strcpy(tmps , subst[0]);

   }

   else if (nm == -1 && ns == -1)
   {
      getSubstrings( (cdecin+nd+1), subst, &nsub);

      if (nsub == 2) 
      {
	 strcpy(tmpm , subst[0]);
	 strcpy(tmpm , subst[0]);
	 strcpy(tmps , subst[1]);
      }

      if (nsub == 1) 
	 strcpy(tmpm , subst[0]);
   }

   else if (ns == -1)
   {
      getSubstrings( (cdecin+nm+1), subst, &nsub);

      if (nsub == 1) 
	 strcpy(tmps , subst[0]);
   }



   /*  If the Number is Too Big, Assume +ddmmss.s  */

   syntax = checkFormat(tmps);

   if (syntax ==1)
      syntax = checkFormat(tmpm);

   if (syntax ==1)
      syntax = checkFormat(tmpd);

   latstatus = syntax;

   if (syntax == 1)
   {
      sec = atof(tmps);

      if (sec >  60.) 
      {
	 i = (int) (sec/10000.);
	 deg = (double) i;
	 i = (int) ((sec - deg*10000.) / 100.);
	 min= (double) i;
	 sec = sec - deg*10000. - min*100.;
      }
      else
      {
	 deg = atof(tmpd);
	 min = atof(tmpm);
	 sec = atof(tmps);
      }
   }


   /*  Generate the DEC in Decimal Degrees  */

   *dec = isign * (deg + (min/60.) + (sec/3600.));


   /* If the user mixes modes (e.g. sexegesimal lon      */
   /* and decimal degree lat), we need to double check   */
   /* supposedly sexegesimal strings here to see if they */
   /* should more reasonably be interpreted as decimal   */
   /* degrees.                                           */

   strcpy(teststr, coordin);

   testlen = strlen(teststr);

   testval = strtod(teststr, &end);

   for(i=0; i<strlen(teststr); ++i)
      if(teststr[i] == '.')
         teststr[i]  = '\0';

   if(end == teststr + testlen && strlen(teststr) < 5)
   {
      *dec = testval;

      latstatus = 1;

      if(fabs(*dec) > 90.)
	 latstatus = 0;
   }



   /*  Returned value reflects the syntax of RA and DEC  */

   if (lonstatus == 0 && latstatus == 0)
      return(3);

   else if (lonstatus == 0)
      return(1);

   else if (latstatus == 0)
      return(2);

   else return(0);
}




/***********************************************************************/
/*                                                                     */
/*    checkFormat  Decide whether string is packed ddmmss.s            */
/*    -------------                                                    */
/*                                                                     */
/***********************************************************************/


int checkFormat(char *s)
{
   int len, i;

   len = strlen(s);

   while (s[len] ==' ')
      len--;

   i = 0;
   while (s[i] == ' ' || s[i] =='+')
      i++;

   while (isdigit((int)s[i]) != 0)
      i++;

   if (s[i] == '.')
      i++;

   while (isdigit((int)s[i]) != 0)
      i++;

   if (s[i] =='e' || s[i] =='E')
      i++;

   while (isdigit((int)s[i]) != 0)
      i++;

   if (i == len)
      return(1);
   else
      return(0);
}




/***********************************************************************/
/*                                                                     */
/*    getSubstrings  Finds the substrings in string                    */
/*    -------------                                                    */
/*                                                                     */
/*    A string is divided into substrings by ' '(space) or ':'(colon)  */
/*    If no characters(non_space) between two colons, the substring    */
/*    is a null string.                                                */
/*                                                                     */
/***********************************************************************/


void getSubstrings(char *string, char **subst, int *nsub)
{
   char *p;
   int num;
   int i;

   p = string;
   num = 0;


/* Find the beginning of the first substring */

   i = 0;
   while ( *p ==' ')
      p++;
   *(subst+num) = p;

   while (*p != '\0')
   {
      while (*p != ' ' &&  *p != ':' && *p != '\0') 
        p++;
      if (*p == ' ' || *p == ':')
         {
	 *p = '\0';
	 p++;
	 }
      while (*p == ' ' )/*|| *p == ':')*/
	 p++;
      num++;
      *(subst + num) = p;
   }
   *nsub=num;
}     
