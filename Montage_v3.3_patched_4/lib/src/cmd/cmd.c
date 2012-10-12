#include <stdio.h>
#include <string.h>
#include <ctype.h>

#include <cmd.h>


#define TRUE       1
#define FALSE  !TRUE
#define LIBCMD_DEFWS_CHARS " \t,"


static unsigned char gDefWhiteMap[256] =
    {
        0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
    };

static unsigned char gWhiteMap[256] =
    {
        0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
    };

 
void setwhitespace(const char * const wsin)
{
   const char * w;
   char         c;

   if ( wsin == (char *)NULL )
	memcpy(gWhiteMap, gDefWhiteMap, 256);
   else
   {
	memset(gWhiteMap, 0, 256);
        w = wsin;
        while ((c = *w) != 0)
        {
            gWhiteMap[ (int)((unsigned char) c) ] = 1;
            ++w;
        }
   }
}


int isws(char ch)
{
   return (int) gWhiteMap[ (int)((unsigned char) ch) ];   
}


int parsecmd(char *cmd, char **cmdv)
{
   char   *ptmp;
   int     i, len, cmdc;            
   int     in_quotes;

   /* Scan the command for commas, semicolons, etc. */

   in_quotes = FALSE;
   len = strlen (cmd);

   for (i = 0; i < len; i++)
   {
      if ( isprint( (int)cmd[i] ) )
      {
         if (cmd[i] == '"')
            in_quotes = !in_quotes;    /* toggle quoting on/off          */

         if (in_quotes == FALSE)       /* if this isn't a quoted string  */
         {
	    if(cmd[i] == ';')          /* change semicolon to null       */
	       cmd[i] = '\0';
         }

         if (cmd[i] == '\0')           /* if this is a null */
            break;                     /* stop the scan     */
      }

      else
         cmd[i] = ' ';                 /* change non-printables to blank */
   }


   /* Collect pointers to each argument for this command */

   ptmp = cmd;
   cmdc = 0;

   while ( gWhiteMap[(int)((unsigned char) *ptmp)] )
      ptmp++;                          /* discard spaces */

   while (*ptmp != '\0')               /* while some of string is left */
   {
      if (*ptmp == '"')                /* If this is a quote */
      {
         *ptmp = '\0';                 /* change it to a NULL */

         ptmp++;                       /* move past quote */
         cmdv[cmdc] = ptmp;               /* pointer to this arg  */
         ++cmdc;

         while (*ptmp != '"' && *ptmp != '\0')
            ptmp++;                    /* search for ending quote */

         if (*ptmp == '"')
         {
            *ptmp = '\0';              /* change quote to a NULL */
            ++ptmp;
         }
      }

      else
      {
         cmdv[cmdc] = ptmp;            /* pointer to this arg     */
         ++cmdc;
      }
      
      while ( !gWhiteMap[(int)((unsigned char) *ptmp)] && (*ptmp != 0) )
         ptmp++;                       /* Find the next space */

      if ( gWhiteMap[(int)((unsigned char) *ptmp)] )
         *ptmp++ = 0;                  /* change space to null and advance */

      while ( gWhiteMap[(int)((unsigned char) *ptmp)] )
         ++ptmp;                       /* discard additional spaces */
   }

   return (cmdc);
}


