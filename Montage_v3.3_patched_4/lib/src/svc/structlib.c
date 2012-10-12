#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <signal.h>

#include <svc.h>

#define TRUE    1
#define FALSE  !TRUE
#define FOREVER TRUE

int svc_struct_debug = 0;


SVC *svc_struct(char *instr)
{
   int i, len, inquote, blev;
   char *str, *p, *begin, *end;
   char *sb, *se, *key, *peq, *val;

   SVC *svc;

   if(instr == (char *)NULL)
      return((SVC *)NULL);

   if(instr[0] == '\0')
      return((SVC *)NULL);

   if(svc_struct_debug)
   {
      fprintf(stderr, "\nDEBUG: Input string: \"%s\"\n", instr);
      fflush(stderr);
   }


   /* Allocate initial space for structure */

   svc = (SVC *) malloc(sizeof(SVC));

   svc->count  = 0;
   svc->nalloc = SVC_SVCCNT;

   svc->key = (char **) malloc(svc->nalloc * sizeof(char *));
   svc->val = (char **) malloc(svc->nalloc * sizeof(char *));

   for(i=0; i<svc->nalloc; ++i)
   {
      svc->key[i] = (char *) malloc(SVC_STRLEN * sizeof(char));
      svc->val[i] = (char *) malloc(SVC_STRLEN * sizeof(char));
   }

   if(svc_struct_debug)
   {
      fprintf(stderr, "\nDEBUG: Allocated SVC structure and %d keyword/value pairs\n", 
	 svc->nalloc);
      fflush(stderr);
   }



   /* Strip off header and trailer */

   len = (int)strlen(instr);
   str = (char *) malloc((len + 1) * sizeof(char));
   strcpy(str, instr);

   p = str;
   p = svc_stripblanks(p, len, 0);


   /* Check for (and skip over) structure/array definition */

   if(     !strncmp(p, "[struct ", 8))
      p += 8;
   else if(!strncmp(p, "[array ",  7))
      p += 7;
   else
   {
      if(svc_struct_debug)
      {
	 fprintf(stderr, "\nDEBUG: Invalid structure start\n");
	 fflush(stderr);
      }

      return((SVC *)NULL);
   }
   

   /* Check to see that the structure/array ends properly */

   end = p + strlen(p) - 1;

   if(*end != ']')
   {
      if(svc_struct_debug)
      {
	 fprintf(stderr, "\nDEBUG: Invalid structure end\n");
	 fflush(stderr);
      }

      return((SVC *)NULL);
   }
   
   *end = '\0';


   /* Now step through the key = val (or val, val, ... sets for array) */

   if(svc_struct_debug)
   {
      fprintf(stderr, "\nDEBUG: Looking for elements in: \"%s\"\n", p);
      fflush(stderr);
   }

   blev = 0;
   begin = p;
   end = p;

   len = (int)strlen(p);



   /* Loop over structure elements */

   while(FOREVER)
   {
      /* Search for closing comma */

      inquote = FALSE;
      while(FOREVER)
      {
	 if(!inquote && blev == 0 && *end == ',')
	    break;

	 if(*end == '\0')
	    break;

	 if(end > p + len)
	    break;

	 if(*end == '"' && *(end-1) != '\\')
	    inquote = !inquote;

	 if(!inquote && *end == '[')
	    ++blev;

	 if(!inquote && *end == ']')
	    --blev;
	 
	 ++end;
      }
      if(inquote)
	 return((SVC *)NULL);

      *end = '\0';



      /* Take the key = val expression apart */

      if(svc_struct_debug)
      {
	 fprintf(stderr, "\nDEBUG: Taking apart: \"%s\"\n", begin);
	 fflush(stderr);
      }


      /* Strip off the leading and trailing blanks in key = value */

      sb = begin;
      sb = svc_stripblanks(sb, strlen(sb), 0);

      if(svc_struct_debug)
      {
	 fprintf(stderr, "\nDEBUG: Stripped: \"%s\"\n", sb);
	 fflush(stderr);
      }

      inquote = FALSE;
      key = sb;
      val = sb;


      /* Find '=' (if any) */

      peq = (char *) NULL;

      se = sb + strlen(sb);
      while(FOREVER)
      {
	 if(!inquote && *val == '=')
	 {
	    peq = val;
	    ++val;
	    break;
	 }

	 if(*val == '"' && *(val-1) != '\\')
	    inquote = !inquote;

	 if(val >= se)
	    break;

	 ++val;
      }
      if(inquote)
	 return((SVC *)NULL);


      /* Forget it if this unit is a struct or array */

      if(!strncmp(sb, "[struct ", 8)
      || !strncmp(sb, "[array ",  7))
      {
	 peq = (char *)NULL;

	 if(svc_struct_debug)
	 {
	    fprintf(stderr, "\nDEBUG: struct or array\n");
	    fflush(stderr);
	 }
      }


      /* Reset if there was no quote (i.e., we have array, not struct) */

      if(peq == (char *)NULL)
      {
	 val = sb;

	 if(svc_struct_debug)
	 {
	    fprintf(stderr, "\nDEBUG: array element\n");
	    fflush(stderr);
	 }
      }


      /* Assign the key, val to the return structure */

      if(peq)
      {
	 *peq = '\0';

	 key = svc_stripblanks(key, strlen(key), 1);
	 strcpy(svc->key[svc->count], key);

	 val = svc_stripblanks(val, strlen(val), 1);
	 strcpy(svc->val[svc->count], val);

	 if(svc_struct_debug)
	 {
	    fprintf(stderr, "\nDEBUG: %4d: \"%s\" = \"%s\"\n", 
	       svc->count, key, val);
	    fflush(stderr);
	 }
      }
      else
      {
	 sprintf(svc->key[svc->count], "%-d", svc->count);

	 key = svc_stripblanks(key, strlen(key), 1);
	 strcpy(svc->val[svc->count], key);

	 if(svc_struct_debug)
	 {
	    fprintf(stderr, "\nDEBUG: %4d: \"%s\" = \"%s\"\n", 
	       svc->count, svc->key[svc->count], val);
	    fflush(stderr);
	 }
      }


      /* If necessary, allocate more space for structure */

      ++svc->count;
      if(svc->count >= svc->nalloc)
      {
	 svc->nalloc += SVC_SVCCNT;

	 svc->key = (char **) realloc(svc->key, svc->nalloc * sizeof(char *));
	 svc->val = (char **) realloc(svc->val, svc->nalloc * sizeof(char *));

	 for(i=svc->nalloc - SVC_SVCCNT; i<svc->nalloc; ++i)
	 {
	    svc->key[i] = (char *) malloc(SVC_STRLEN * sizeof(char));
	    svc->val[i] = (char *) malloc(SVC_STRLEN * sizeof(char));
	 }

	 if(svc_struct_debug)
	 {
	    fprintf(stderr, "\nDEBUG: Allocated space for %d more keyword/value pairs\n",
	       SVC_SVCCNT);
	    fflush(stderr);
	 }
      }


      /* Go on to the next substructure */

      begin = end + 1;
      end = begin;

      if(end >= p + len)
	 break;
   }

   free(str);
   return(svc);
}



/* This routine strips leading and trailing white space from   */
/* strings, returning the new starting location for the string */

char *svc_stripblanks(char *ptr, int len, int quotes)
{
   char *begin, *end;


   /* First strip off the trailing white space */

   begin = ptr;
   end = begin + len - 1;
      
   while(FOREVER)
   {
      if(*end == ' '  || *end == '\t'
      || *end == '\r' || *end == '\n')
      {
	 *end = '\0';
	 --end;
      }
      else
	 break;
      
      if(end <= begin)
	 break;
   }


   /* Then move the begin pointer over the leading white space */

   while(FOREVER)
   {
      if(*begin == ' '  || *begin == '\t'
      || *begin == '\r' || *begin == '\n')
      {
	 ++begin;
      }
      else
	 break;

      if(begin >= end)
	 break;
   }


   /* If desired, strip off leading/trailing qoutes */

   if(quotes)
   {
      if(*end == '"')
	 *end = '\0';

      if(*begin == '"')
      {
	 *begin = '\0';
	 ++begin;
      }
   }

   return(begin);
}




int svc_free(SVC *svc)
{
   int i, nalloc, s;

   s = SVC_ERROR;
   if(svc != (SVC *)NULL)
   { 
      nalloc = svc->nalloc;

      for(i=0; i<nalloc; ++i)
      {
         free(svc->key[i]);
         free(svc->val[i]);
      }

      free(svc->key);
      free(svc->val);
      free(svc);
      s = SVC_OK;
   }
   return s;
}




char *svc_val(char *structstr, char *key, char *val)
{
   int  i, len;
   char subkey[SVC_STRLEN], tail[SVC_STRLEN], subval[SVC_STRLEN];

   SVC *sv;

   strcpy(subkey, key);
   len = strlen(subkey);

   for(i=0; i<len; ++i)
   {
      if(subkey[i] == '.' || subkey[i] == '[')
      {
	 subkey[i] = '\0';

         break;
      }
   }

   if(subkey[strlen(subkey) - 1] == ']')
      subkey[strlen(subkey) - 1] = '\0';

   if(i >= len)
      tail[0] = '\0';
   else
      strcpy(tail, subkey + i + 1);
   
   len = strlen(tail);

   if((sv = svc_struct(structstr)) != (SVC *)NULL)
   {
      for(i=0; i<sv->count; ++i)
      {
	 if(strcmp(sv->key[i], subkey) == 0)
	 {
	    if(!len)
	    {
	       strcpy(val, sv->val[i]);
	       svc_free(sv);
	       return(val);
	    }

	    else if(svc_val(sv->val[i], tail, subval))
	    {
	       strcpy(val, subval);
	       svc_free(sv);
	       return(val);
	    }

	    else
	    {
	       svc_free(sv);
	       return((char *) NULL);
	    }


	    break;
	 }
      }

      return((char *) NULL);
   }
   else
      return((char *) NULL);
}
