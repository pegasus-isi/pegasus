#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <ctype.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <signal.h>

#include <svc.h>

FILE *fdopen(int fildes, const char *mode);
int   kill  (pid_t pid, int sig);



/*    CHILD SERVICE related information.  The last three elements are      */
/*    totally optional only the last one is actually used for anything     */
/*    at this time.                                                        */

struct svc_info
{
   int  pid;          /* Process ID of child service                       */
   int  running;      /* Flag denoting whether child is currently running  */

   int  fdin [2];     /* Pipe for redirecting child service stdin          */
   int  fdout[2];     /* Pipe for redirecting child service stdout         */

   FILE *toexec;      /* Stream in parent associated with child stdin      */
   FILE *fromexec;    /* Stream in parent associated with child stdout     */
 
   char *svcname;     /* Registered "name" of service                      */
   char *sigfunc;     /* Registered signal handler for process (not used)  */
   char *quitstr;     /* Registered "quit" string for service              */
};

static struct svc_info **svc_list;

static int   svc_list_count;         /* Number of services in use          */
static int   svc_list_maxalloc = 0;  /* Service slots currently allocated  */
   

static char *svc_return_string;      /* Response from last service command */

static FILE *svc_debug_stream = (FILE *)NULL;     /* Debug message output  */


void set_apputil_debug(int flag)
{
   if(flag)
      svc_debug(stdout);
   else
      svc_debug((FILE *)NULL);
}

/*********************/
/* Turn on debugging */
/*********************/

void svc_debug(FILE *stream)
{
   svc_debug_stream = stream;

   if(svc_debug_stream)
   {
      fprintf(svc_debug_stream,
	 "SVC_DEBUG>  svc_debug(): ON<br>\n");
      fflush(svc_debug_stream);
   }
}



/**********************************************/
/* Allocate/grow space for child service info */
/**********************************************/

int svc_alloc()
{
   int i;

   if(svc_debug_stream)
   {
      fprintf(svc_debug_stream,
	 "SVC_DEBUG>  Entering svc_alloc()<br>\n");
      fflush(svc_debug_stream);
   }

   if(svc_list_maxalloc == 0)
   {
      svc_list_maxalloc = SVC_MAXSVC;

      svc_list = (struct svc_info **)
		    malloc(svc_list_maxalloc * sizeof(struct svc_info *));
   
      if(svc_list == (struct svc_info **)NULL)
	 return(SVC_ERROR);

      for(i=0; i<svc_list_maxalloc; ++i)
      {
	 svc_list[i] = (struct svc_info *) malloc(sizeof(struct svc_info));

	 if(svc_list[i] == (struct svc_info *)NULL)
	    return(SVC_ERROR);

	 svc_list[i]->pid     = 0;
	 svc_list[i]->running = 0;

	 svc_list[i]->svcname = (char *)NULL;
	 svc_list[i]->sigfunc = (char *)NULL;
	 svc_list[i]->quitstr = (char *)NULL;
      }

      svc_list_count = 0;

      if(svc_debug_stream)
      {
	 fprintf(svc_debug_stream,
	    "SVC_DEBUG>  svc_alloc(): allocated %d service slots [@%p]<br>\n", 
	    svc_list_maxalloc, (void *)svc_list);
	 fflush(svc_debug_stream);
      }
   }

   else if(svc_list_count >= svc_list_maxalloc)
   {
      svc_list_maxalloc += SVC_MAXSVC;

      svc_list = (struct svc_info **)
		    realloc((void *)svc_list,
		       svc_list_maxalloc * sizeof(struct svc_info *));

      
      if(svc_list == (struct svc_info **)NULL)
	 return(SVC_ERROR);

      for(i=svc_list_maxalloc-SVC_MAXSVC; i<svc_list_maxalloc; ++i)
      {
	 svc_list[i] = (struct svc_info *) malloc(sizeof(struct svc_info));

	 if(svc_list[i] == (struct svc_info *)NULL)
	    return(SVC_ERROR);

	 svc_list[i]->pid     = 0;
	 svc_list[i]->running = 0;

	 svc_list[i]->svcname = (char *)NULL;
	 svc_list[i]->sigfunc = (char *)NULL;
	 svc_list[i]->quitstr = (char *)NULL;
      }

      if(svc_debug_stream)
      {
	 fprintf(svc_debug_stream,
	    "SVC_DEBUG>  svc_alloc(): reallocated to %d service slots [@%p]<br>\n", 
	    svc_list_maxalloc, (void *)svc_list);
	 fflush(svc_debug_stream);
      }
   }
   return SVC_OK;
}



/****************************************/
/* Check the status of child processes  */
/* (i.e. see if they are still running) */
/****************************************/

void svc_check()
{
   int i, npid;

   if(svc_debug_stream)
   {
      fprintf(svc_debug_stream,
	 "SVC_DEBUG>  Entering svc_check()<br>\n");
      fflush(svc_debug_stream);
   }

   for(i=0; i<svc_list_maxalloc; ++i)
   {
      svc_list[i]->running = 0;

      npid = svc_list[i]->pid;

      if(npid > 0)
      {
	 if (!kill(npid, 0))
	    svc_list[i]->running = 1;
      }
   }
}



/***************************/
/* Fire up a child process */
/***************************/

int svc_init(char *svcstr)
{
   int   i, len, npid, index, cmdc;
   char *path;
   
   static char  *str  = (char  *)NULL;
   static char **cmdv = (char **)NULL;

   static char nullmsg[] 
      = "[struct stat=\"ABORT\", msg=\"No child program executable or program aborted\"]";

   if(svc_debug_stream)
   {
      fprintf(svc_debug_stream,
	 "SVC_DEBUG>  Entering svc_init()<br>\n");
      fflush(svc_debug_stream);
   }

   if(svcstr == (char *)NULL)
   {
      if(svc_debug_stream)
      {
	 fprintf(svc_debug_stream,
	    "SVC_DEBUG>  svc_init(): svcstr is NULL<br>\n");
	 fflush(svc_debug_stream);
      }

      return(SVC_ERROR);
   }

   if(svc_debug_stream)
   {
      fprintf(svc_debug_stream,
	 "SVC_DEBUG>  svc_init(): svcstr = [%s]<br>\n", svcstr);
      fflush(svc_debug_stream);
   }


   /* Check on allocated space */

   if(svc_alloc() == SVC_ERROR)
   {
      if(svc_debug_stream)
      {
	 fprintf(svc_debug_stream,
	    "SVC_DEBUG>  svc_init(): got svc_alloc() error<br>\n");
	 fflush(svc_debug_stream);
      }

      return(SVC_ERROR);
   }


   /* Allocate space for exec() argument */
   /* list and construct it              */

   len = strlen(svcstr)+1;

   if(str)
      free((void *)str);

   str = (char *)malloc(len * sizeof(char));

   if(str == (char *)NULL)
   {
      if(svc_debug_stream)
      {
	 fprintf(svc_debug_stream,
	    "SVC_DEBUG>  svc_init(): string malloc() error<br>\n");
	 fflush(svc_debug_stream);
      }

      return(SVC_ERROR);
   }

   strcpy(str, svcstr);

   if(cmdv)
      free((void *)cmdv);

   cmdv = (char **)malloc(len * sizeof(char *));

   if(cmdv == (char **)NULL)
   {
      if(svc_debug_stream)
      {
	 fprintf(svc_debug_stream,
	    "SVC_DEBUG>  svc_init(): cmdv malloc() error<br>\n");
	 fflush(svc_debug_stream);
      }

      return(SVC_ERROR);
   }

   cmdc = svc_getargs(str, cmdv);
   cmdv[cmdc] = (char *) NULL;

   if(cmdc < 1)
      return(SVC_OK);

   path = cmdv[0];


   /* Find and open slot in the service table */

   index = -1;
   for(i=0; i<svc_list_maxalloc; ++i)
   {
      if(svc_list[i]->pid == 0)
      {
	 index = i;
	 break;
      }
   }

   if(index < 0)
      return(SVC_OK);

   svc_list[index]->svcname = (char *)NULL;
   svc_list[index]->sigfunc = (char *)NULL;
   svc_list[index]->quitstr = (char *)NULL;
   svc_list[index]->running = 1;


   /* Ignore child termination signals */

   signal(SIGCHLD, SIG_IGN);


   /* Set up pipes and start child */

   (void) pipe(svc_list[index]->fdin );
   (void) pipe(svc_list[index]->fdout);

   if((npid=fork()) == 0)                                       /* CHILD */
   {
      close(svc_list[index]->fdin[1] );
      close(svc_list[index]->fdout[0]);

      (void) dup2(svc_list[index]->fdin[0],  0);
      (void) dup2(svc_list[index]->fdout[1], 1);

      execvp(path, cmdv);

      svc_return_string = nullmsg;

      exit(0);
   }
   else                                                         /* PARENT */
   {
      close(svc_list[index]->fdin[0] );
      close(svc_list[index]->fdout[1]);

      svc_list[index]->toexec   = fdopen(svc_list[index]->fdin[1],  "a");
      svc_list[index]->fromexec = fdopen(svc_list[index]->fdout[0], "r");

      svc_list[index]->pid = npid;
      ++svc_list_count;
   }


   /* Return index number */

   if(svc_debug_stream)
   {
      fprintf(svc_debug_stream,
         "SVC_DEBUG>  svc_init(): index = %d<br>\n", index);

      fprintf(svc_debug_stream,
         "SVC_DEBUG>  svc_init(): pid   = %d<br>\n", npid);

      fprintf(svc_debug_stream,
         "SVC_DEBUG>  svc_init(): path  = %s<br>\n", path);

      fprintf(svc_debug_stream,
         "SVC_DEBUG>  svc_init(): fdin  = %d<br>\n", svc_list[index]->fdin[1]);

      fprintf(svc_debug_stream,
         "SVC_DEBUG>  svc_init(): fdout = %d<br>\n", svc_list[index]->fdout[0]);

      fflush(svc_debug_stream);
   }

   if(npid < 0)
   {
      (void) fclose(svc_list[index]->toexec  );
      (void) fclose(svc_list[index]->fromexec);

      (void) close(svc_list[index]->fdin[1] );
      (void) close(svc_list[index]->fdout[0]);

      if(svc_debug_stream)
      {
	 fprintf(svc_debug_stream,
	    "SVC_DEBUG>  svc_init(): fork() failed<br>\n");
	 fflush(svc_debug_stream);


	 fprintf(svc_debug_stream,
	    "SVC_DEBUG>  svc_init(): [%s]<br>\n", strerror(errno));
	 fflush(svc_debug_stream);
      }

      svc_list[index]->pid     = 0;
      svc_list[index]->running = 0;
      
      --svc_list_count;

      return(SVC_ERROR);
   }

   return(index);
}



/*********************************/
/* Register a name, quit command */
/* for a service (OPTIONAL)      */
/*********************************/

int svc_register(int index, char *name, char *sig, char *quit)
{
   if(svc_debug_stream)
   {
      fprintf(svc_debug_stream,
	 "SVC_DEBUG>  Entering svc_register()<br>\n");
      fflush(svc_debug_stream);
   }

   if(svc_list[index]->pid != 0)
   {
      if(svc_list[index]->svcname)
	 free((void *)svc_list[index]->svcname);

      if(svc_list[index]->sigfunc)
	 free((void *)svc_list[index]->sigfunc);

      if(svc_list[index]->quitstr)
	 free((void *)svc_list[index]->quitstr);

      svc_list[index]->svcname = (char *)malloc((strlen(name)+1) * sizeof(char));
      svc_list[index]->sigfunc = (char *)malloc((strlen(sig) +1) * sizeof(char));
      svc_list[index]->quitstr = (char *)malloc((strlen(quit)+1) * sizeof(char));

      if(svc_list[index]->svcname == (char *)NULL)
	 return(SVC_ERROR);

      if(svc_list[index]->sigfunc == (char *)NULL)
	 return(SVC_ERROR);

      if(svc_list[index]->quitstr == (char *)NULL)
	 return(SVC_ERROR);

      strcpy(svc_list[index]->svcname, name);
      strcpy(svc_list[index]->sigfunc, sig);
      strcpy(svc_list[index]->quitstr, quit);

      return(SVC_OK);
   }
   else
      return(SVC_ERROR);
}



/************************/
/* Close down a service */
/************************/

int svc_close(int index)
{
   int nchar;

   if(svc_debug_stream)
   {
      fprintf(svc_debug_stream,
	 "SVC_DEBUG>  Entering svc_close()<br>\n");
      fflush(svc_debug_stream);
   }

   if(index < 0 || index >= svc_list_maxalloc)
   {
      if(svc_debug_stream)
      {
	 fprintf(svc_debug_stream,
	    "SVC_DEBUG>  svc_close():  Illegal index [%d]<br>\n", index);
	 fflush(svc_debug_stream);
      }

      return(SVC_ERROR);
   }

   if(svc_list[index]->pid != 0)
   {
      if(svc_list[index]->quitstr
      && (int)strlen(svc_list[index]->quitstr) > 0)
      {
	 nchar = fprintf(svc_list[index]->toexec, 
			 "%s\n", svc_list[index]->quitstr);
      }

      (void) fflush(svc_list[index]->toexec  );
      (void) fflush(svc_list[index]->fromexec);

      (void) fclose(svc_list[index]->toexec  );
      (void) fclose(svc_list[index]->fromexec);

      (void) close(svc_list[index]->fdin[1] );
      (void) close(svc_list[index]->fdout[0]);

      svc_check();
      if(svc_list[index]->running)
      {
	 (void) kill(svc_list[index]->pid, SIGTERM);

	 if(svc_debug_stream)
	 {
	    fprintf(svc_debug_stream,
	       "SVC_DEBUG>  svc_close(): kill %d (SIGTERM)<br>\n", 
		  svc_list[index]->pid);

	    fflush(svc_debug_stream);
	 }
      }

      svc_list[index]->pid     = 0;
      svc_list[index]->running = 0;
      
      --svc_list_count;
      return(SVC_OK);
   }
   else
      return(SVC_ERROR);
}
      
   

/*******************************/
/* Close down all open service */
/*******************************/

int svc_closeall()
{
   int index;
   int nchar;

   if(svc_debug_stream)
   {
      fprintf(svc_debug_stream,
	 "SVC_DEBUG>  Entering svc_closeall()<br>\n");
      fflush(svc_debug_stream);
   }

   for(index=0; index<svc_list_maxalloc; ++index)
   {
      if(svc_list[index]->pid != 0)
      {
	 if(svc_debug_stream)
	 {
	    fprintf(svc_debug_stream,
	       "SVC_DEBUG>  svc_closeall(): closing %d <br>\n", 
		  svc_list[index]->pid);

	    fflush(svc_debug_stream);
	 }
      
	 if(svc_list[index]->quitstr
	 && (int)strlen(svc_list[index]->quitstr) > 0)
	 {
	    nchar = fprintf(svc_list[index]->toexec, 
		       "%s\n", svc_list[index]->quitstr);
	 }

	 (void) fflush(svc_list[index]->toexec  );
	 (void) fflush(svc_list[index]->fromexec);

	 (void) fclose(svc_list[index]->toexec  );
	 (void) fclose(svc_list[index]->fromexec);

	 (void) close(svc_list[index]->fdin[1] );
	 (void) close(svc_list[index]->fdout[0]);

	 svc_check();
	 if(svc_list[index]->running)
	 {
	    (void) kill(svc_list[index]->pid, SIGTERM);

	    if(svc_debug_stream)
	    {
	       fprintf(svc_debug_stream,
		  "SVC_DEBUG>  svc_closeall(): kill %d (SIGTERM)<br>\n", 
		     svc_list[index]->pid);

	       fflush(svc_debug_stream);
	    }
	 }

	 svc_list[index]->pid = 0;
	 svc_list[index]->running = 0;
	 --svc_list_count;
      }
   }
   return(SVC_OK);
}
   


/***************************************************/
/* Run non-interactive (command-line only) service */
/***************************************************/

int svc_run(svcstr)

char *svcstr;
{
   int  index;
   SVC *sv;

   if(svc_debug_stream)
   {
      fprintf(svc_debug_stream,
	 "SVC_DEBUG>  Entering svc_run()<br>\n");
      fflush(svc_debug_stream);
   }

   if(svcstr == (char *)NULL)
   {
      if(svc_debug_stream)
      {
	 fprintf(svc_debug_stream,
	    "SVC_DEBUG>  svc_run(): svcstr is NULL<br>\n");
	 fflush(svc_debug_stream);
      }

      return(SVC_ERROR);
   }

   if(svc_debug_stream)
   {
      fprintf(svc_debug_stream,
	 "SVC_DEBUG>  svc_run(): svcstr = [%s]<br>\n", svcstr);
      fflush(svc_debug_stream);
   }

   index = svc_init(svcstr);

   if(svc_debug_stream)
   {
      fprintf(svc_debug_stream,
	 "SVC_DEBUG>  svc_run(): index (from svc_init()) = %d<br>\n", index);
      fflush(svc_debug_stream);
   }

   if(index == SVC_ERROR)
      return(SVC_ERROR);

   svc_return_string = svc_receive( index );

   if( ( sv = svc_struct( svc_return_string ) ) == (SVC *) NULL )
   {
      if(svc_debug_stream)
      {
	 fprintf(svc_debug_stream,
	    "SVC_DEBUG>  svc_run(): illegal return structure [%s] (running [%s]).<br>\n",
	    svc_return_string, svcstr);
	 fflush(svc_debug_stream);
      }

      return(SVC_ERROR);
   }

   svc_free( sv );

   if(svc_close(index) == SVC_ERROR)
   {
      if(svc_debug_stream)
      {
	 fprintf(svc_debug_stream,
	    "SVC_DEBUG>  svc_run(): close failure; service probably already exited.<br>\n");
	 fflush(svc_debug_stream);
      }
   }

   return(SVC_OK);
}



/**************************************/
/* Send a command string to a service */
/**************************************/

int svc_send(index, cmd)

int   index;
char *cmd;
{
   if(svc_debug_stream)
   {
      fprintf(svc_debug_stream,
	 "SVC_DEBUG>  Entering svc_send()<br>\n");
      fflush(svc_debug_stream);
   }

   svc_check();

   if(!svc_list[index]->running)
   {
      svc_close(index);

      return(SVC_ERROR);
   }

   if(svc_debug_stream)
   {
      fprintf(svc_debug_stream,
	 "SVC_DEBUG>  svc_send(): Sending   [%s]<br>\n", cmd);
      fflush(svc_debug_stream);
   }

   fprintf(svc_list[index]->toexec, "%s\n", cmd);
   fflush(svc_list[index]->toexec);

   return(SVC_OK);
}



/*************************************/
/* Receive a response from a service */
/*************************************/

char *svc_receive(index)

int index;
{
   char *str;

   static char nullmsg[] 
      = "[struct stat=\"ABORT\", msg=\"No child program executable or program aborted\"]";

   if(svc_debug_stream)
   {
      fprintf(svc_debug_stream,
	 "SVC_DEBUG>  Entering svc_receive()<br>\n");
      fflush(svc_debug_stream);
   }

   str = svc_fgets(index);

   if(str == (char *)NULL)
   {
      svc_check();

      if(!svc_list[index]->running)
	 svc_close(index);

      return(nullmsg);
   }
   else
   {
      svc_check();

      if(!svc_list[index]->running)
      {
	 svc_close(index);
      }

      if(svc_debug_stream)
      {
	 fprintf(svc_debug_stream,
	    "SVC_DEBUG>  svc_receive(): Receiving [%s]<br>\n", str);
	 fflush(svc_debug_stream);
      }

      return(str);
   }
}




/************************************************/
/* Reads a line of unknonw length from a stream */
/************************************************/

char *svc_fgets(int index)
{
   int   i, ch, nalloc;
   FILE *fp;

   static char *str = (char *)NULL;

   if(svc_debug_stream)
   {
      fprintf(svc_debug_stream,
	 "SVC_DEBUG>  Entering svc_fgets()<br>\n");
      fflush(svc_debug_stream);
   }

   fp = svc_list[index]->fromexec;

   if(fp == (FILE *)NULL)
   {
      if(svc_debug_stream)
      {
	 fprintf(svc_debug_stream,
	    "SVC_DEBUG>  svc_fgets(): Invalid FILE pointer<br>\n");
	 fflush(svc_debug_stream);
      }

      return((char *)NULL);
   }


   /* Allocate space for the string */

   if(str)
      free((void *)str);

   str = (char *)malloc(SVC_STRLEN * sizeof(char));
   
   if(str == (char *)NULL)
      return((char *)NULL);

   nalloc = SVC_STRLEN;

   if(svc_debug_stream)
   {
      fprintf(svc_debug_stream,
	 "SVC_DEBUG>  svc_fgets(): Allocate string space [@%p]<br>\n", (void *)str);
      fflush(svc_debug_stream);
   }


   /* Loop to collect characters until newline */

   i = 0;
   while(1)
   {
      ch = fgetc(fp);

      if(ch == EOF)
      {
	 if(svc_debug_stream)
	 {
	    fprintf(svc_debug_stream,
	       "SVC_DEBUG>  svc_fgets(): EOF encountered<br>\n");
	    fflush(svc_debug_stream);
	 }

	 return((char *)NULL);
      }

      if((char)ch == '\n')
      {
	 str[i] = '\0';
	 return(str);
      }
      else
	 str[i] = (char)ch;
	 

      ++i;


      /* Reallocating string space if necessary */

      if(i >= nalloc)
      {
	 nalloc += SVC_STRLEN;
	 str = (char *)realloc((void *)str, nalloc * sizeof(char));

	 if(str == (char *)NULL)
	    return((char *)NULL);
      }
   }
}



/**********************************************************************/
/* Relatively generic interaction routine, where we assume there will */
/* a response to each command and if the response is a invalid        */
/* structure we exit (calls svc_send() and svc_receive() in tandem).  */
/**********************************************************************/

int svc_command(int svc, char *cmdstr)
{
   SVC  *sv;

   if(svc_debug_stream)
   {
      fprintf(svc_debug_stream,
	 "SVC_DEBUG>  Entering svc_command()<br>\n");
      fflush(svc_debug_stream);
   }

   svc_send( svc, cmdstr );
   svc_return_string = svc_receive( svc );

   if( ( sv = svc_struct( svc_return_string ) ) == (SVC *) NULL )
   {
      if(svc_debug_stream)
      {
	 fprintf(svc_debug_stream, "SVC_DEBUG>  svc_command(): ERROR: <br>\n");
	 fprintf(svc_debug_stream, "SVC_DEBUG>  svc_command(): Command: [%s]<br>\n", cmdstr);
	 fprintf(svc_debug_stream, "SVC_DEBUG>  svc_command(): Return:  [%s]<br>\n", svc_return_string);
	 fflush(svc_debug_stream);
      }

      return(SVC_ERROR);
   }
   else
      svc_free( sv );

   return(SVC_OK);
}



/**********************************************************************/
/* Relatively generic routine to extract the structure value          */
/* requested and return a string pointer to it.                       */
/**********************************************************************/

char *svc_value(char *ref)
{
   static char *svc_return_value = (char *)NULL;

   if(svc_debug_stream)
   {
      fprintf(svc_debug_stream,
	 "SVC_DEBUG>  Entering svc_value()<br>\n");
      fflush(svc_debug_stream);
   }

   if(!svc_return_string)
      return((char *)NULL);

   if(ref == (char *)NULL)
      return(svc_return_string);
   
   if(svc_return_value)
      free((void *)svc_return_value);
   
   svc_return_value = (char *)malloc((strlen(svc_return_string)+1) * sizeof(char));

   if( svc_val( svc_return_string, ref, svc_return_value ) )
   {
      if(svc_debug_stream)
      {
	 fprintf(svc_debug_stream, "SVC_DEBUG>  svc_value(): [%s] -> [%s] [@%p]<br>\n", 
	    ref, svc_return_value, (void *)svc_return_value);
	 fflush(svc_debug_stream);
      }

      return( svc_return_value );
   }
   else
      return( (char *) NULL );
}



/********************************************/
/* Utility routine to parse command strings */
/********************************************/

int svc_getargs (char *cmd, char **cmdv)

/* cmd     input argument - command string to be parsed        */
/* cmdv[]  output argument - array of strings (cmd parameters) */

{
   char   *ptmp;
   int     i, j, len, slen, end, cmdc;	    
   int     in_quotes;

   char   *cmdstr;

   if(svc_debug_stream)
   {
      fprintf(svc_debug_stream,
	 "SVC_DEBUG>  Entering svc_getargs()<br>\n");
      fflush(svc_debug_stream);
   }


   /* Scan the command for commas, semicolons, etc. */

   in_quotes = 0;
   len = (int)strlen (cmd);

   cmdstr = (char *)malloc(len);

   for (i = 0; i < len; i++)
   {
      if (isprint ((int)cmd[i]))
      {
	 if ((i == 0 && cmd[i] == '"') || (cmd[i] == '"' && cmd[i-1] != '\\'))

	    in_quotes = !in_quotes;    /* toggle quoting on/off          */

	 if (in_quotes == 0)           /* if this isn't a quoted string  */
	 {
	    if (cmd[i] == ',')	       /* change commas to spaces        */
	       cmd[i] = ' ';

	    if(cmd[i] == ';')          /* change semicolon to null       */
	       cmd[i] = '\0';
	 }

	 if (cmd[i] == '\0')	       /* if this is a null */
	    break;		       /* stop the scan     */
      }

      else
	 cmd[i] = ' ';                 /* change non-printables to blank */
   }


   /* Collect pointers to each argument for this command */

   ptmp = cmd;
   cmdc = 0;

   while (*ptmp == ' ')	               /* discard spaces */
      ptmp++;

   while (*ptmp != '\0')	       /* while some of string is left */
   {
      /* If this is a quote */

      if ((ptmp == cmd && *ptmp == '"') || (*ptmp == '"' && *(ptmp-1) != '\\'))
      {
	 *ptmp = '\0';                 /* change it to a NULL */

	 ptmp++;                       /* move past quote */
	 cmdv[cmdc] = ptmp;	       /* pointer to this arg  */
	 ++cmdc;

	 /* search for ending quote */

	 while (1)
	 {
	    if(*ptmp == '\0')
	       break;

	    else if(*ptmp == '"' && *(ptmp-1) != '\\')
	       break; 
	    
	    else
	       ++ptmp;
	 }

	 if (*ptmp == '"' && *(ptmp-1) != '\\')
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
      
      while ((*ptmp != ' ') && (*ptmp != '\0'))
	 ptmp++;                       /* Find the next space */

      if (*ptmp == ' ')
	 *ptmp++ = '\0';               /* change space to null and advance */

      while (*ptmp == ' ')
	 ptmp++;                       /* discard additional spaces */
   }


   /* Transform backslash escaped */
   /* characters:  \" \t \n \r \\ */

   for(i=0; i<cmdc; ++i)
   {
      end  = 0;
      slen = strlen(cmdv[i]);

      for(j=0; j<slen; ++j)
      {
	 if(j < slen-1 && cmdv[i][j] == '\\')
	 {
	    if(cmdv[i][j+1] == '"')
	    {
	       cmdstr[end] = '"';
	       ++j;
	    }

	    else if(cmdv[i][j+1] == 't')
	    {
	       cmdstr[end] = '\t';
	       ++j;
	    }

	    else if(cmdv[i][j+1] == 'n')
	    {
	       cmdstr[end] = '\n';
	       ++j;
	    }

	    else if(cmdv[i][j+1] == 'r')
	    {
	       cmdstr[end] = '\r';
	       ++j;
	    }

	    else if(cmdv[i][j+1] == '\\')
	    {
	       cmdstr[end] = '\\';
	       ++j;
	    }
	    
	    else 
	       cmdstr[end] = '\\';
	 }

	 else
	    cmdstr[end] = cmdv[i][j];
	 
	 ++end;
      }

      cmdstr[end] = '\0';
      ++end;

      for(j=0; j<end; ++j)
	 cmdv[i][j] = cmdstr[j];
   }

   free (cmdstr);
   return (cmdc);
}



/*********************************************/
/* Set up signal catching so child processes */
/* can be shut down gracefully               */
/*********************************************/

void svc_sigset()
{
   if(svc_debug_stream)
   {
      fprintf(svc_debug_stream,
	 "SVC_DEBUG>  Entering svc_sigset()<br>\n");
      fflush(svc_debug_stream);
   }

   signal(SIGHUP,     svc_sighandler);
   signal(SIGINT,     svc_sighandler);
   signal(SIGQUIT,    svc_sighandler);
   signal(SIGILL,     svc_sighandler);
   signal(SIGTRAP,    svc_sighandler);
   signal(SIGABRT,    svc_sighandler);
   signal(SIGFPE,     svc_sighandler);
   signal(SIGKILL,    svc_sighandler);
   signal(SIGBUS,     svc_sighandler);
   signal(SIGSEGV,    svc_sighandler);
   signal(SIGSYS,     svc_sighandler);
   signal(SIGPIPE,    svc_sighandler);
   signal(SIGALRM,    svc_sighandler);
   signal(SIGTERM,    svc_sighandler);
   signal(SIGUSR1,    svc_sighandler);
   signal(SIGUSR2,    svc_sighandler);
   signal(SIGCHLD,    svc_sighandler);
   signal(SIGWINCH,   svc_sighandler);
   signal(SIGURG,     svc_sighandler);
   signal(SIGSTOP,    svc_sighandler);
   signal(SIGTSTP,    svc_sighandler);
   signal(SIGCONT,    svc_sighandler);
   signal(SIGTTIN,    svc_sighandler);
   signal(SIGTTOU,    svc_sighandler);
   signal(SIGVTALRM,  svc_sighandler);
   signal(SIGPROF,    svc_sighandler);
   signal(SIGXCPU,    svc_sighandler);
   signal(SIGXFSZ,    svc_sighandler);
}



/*******************/
/* Process signals */
/*******************/

void svc_sighandler(int sig)
{
   char msg[80];

   if(svc_debug_stream)
   {
      fprintf(svc_debug_stream,
	 "SVC_DEBUG>  Entering svc_sighandler()<br>\n");
      fflush(svc_debug_stream);
   }

   if(sig == SIGHUP    
   || sig == SIGINT   
   || sig == SIGQUIT
   || sig == SIGILL    
   || sig == SIGTRAP  
   || sig == SIGABRT
   || sig == SIGFPE    
   || sig == SIGKILL   
   || sig == SIGBUS
   || sig == SIGSEGV   
   || sig == SIGSYS   
   || sig == SIGPIPE
   || sig == SIGALRM   
   || sig == SIGTERM  
   || sig == SIGUSR1
   || sig == SIGUSR2   
   || sig == SIGSTOP
   || sig == SIGTSTP   
   || sig == SIGTTIN  
   || sig == SIGTTOU
   || sig == SIGVTALRM 
   || sig == SIGPROF
   || sig == SIGXCPU)
   {
      signal(SIGHUP,     SIG_IGN);
      signal(SIGINT,     SIG_IGN);
      signal(SIGQUIT,    SIG_IGN);
      signal(SIGILL,     SIG_IGN);
      signal(SIGTRAP,    SIG_IGN);
      signal(SIGABRT,    SIG_IGN);
      signal(SIGFPE,     SIG_IGN);
      signal(SIGKILL,    SIG_IGN);
      signal(SIGBUS,     SIG_IGN);
      signal(SIGSEGV,    SIG_IGN);
      signal(SIGSYS,     SIG_IGN);
      signal(SIGPIPE,    SIG_IGN);
      signal(SIGALRM,    SIG_IGN);
      signal(SIGTERM,    SIG_IGN);
      signal(SIGUSR1,    SIG_IGN);
      signal(SIGUSR2,    SIG_IGN);
      signal(SIGSTOP,    SIG_IGN);
      signal(SIGTSTP,    SIG_IGN);
      signal(SIGTTIN,    SIG_IGN);
      signal(SIGTTOU,    SIG_IGN);
      signal(SIGVTALRM,  SIG_IGN);
      signal(SIGPROF,    SIG_IGN);
      signal(SIGXCPU,    SIG_IGN);

           if(sig == SIGHUP   ) strcpy(msg, "SIGHUP:     Hangup (see termio(7I))");
      else if(sig == SIGINT   ) strcpy(msg, "SIGINT:     Interrupt (see termio(7I))");
      else if(sig == SIGQUIT  ) strcpy(msg, "SIGQUIT:    Quit (see termio(7I))");
      else if(sig == SIGILL   ) strcpy(msg, "SIGILL:     Illegal Instruction");
      else if(sig == SIGTRAP  ) strcpy(msg, "SIGTRAP:    Trace/Breakpoint Trap");
      else if(sig == SIGABRT  ) strcpy(msg, "SIGABRT:    Abort");
      else if(sig == SIGFPE   ) strcpy(msg, "SIGFPE:     Arithmetic Exception");
      else if(sig == SIGKILL  ) strcpy(msg, "SIGKILL:    Killed");
      else if(sig == SIGBUS   ) strcpy(msg, "SIGBUS:     Bus Error");
      else if(sig == SIGSEGV  ) strcpy(msg, "SIGSEGV:    Segmentation Fault");
      else if(sig == SIGSYS   ) strcpy(msg, "SIGSYS:     Bad System Call");
      else if(sig == SIGPIPE  ) strcpy(msg, "SIGPIPE:    Broken Pipe");
      else if(sig == SIGALRM  ) strcpy(msg, "SIGALRM:    Alarm Clock");
      else if(sig == SIGTERM  ) strcpy(msg, "SIGTERM:    Terminated");
      else if(sig == SIGUSR1  ) strcpy(msg, "SIGUSR1:    User Signal 1");
      else if(sig == SIGUSR2  ) strcpy(msg, "SIGUSR2:    User Signal 2");
      else if(sig == SIGSTOP  ) strcpy(msg, "SIGSTOP:    Stopped (signal)");
      else if(sig == SIGTSTP  ) strcpy(msg, "SIGTSTP:    Stopped (user)");
      else if(sig == SIGCONT  ) strcpy(msg, "SIGCONT:    Continued");
      else if(sig == SIGTTIN  ) strcpy(msg, "SIGTTIN:    Stopped (tty input)");
      else if(sig == SIGTTOU  ) strcpy(msg, "SIGTTOU:    Stopped (tty output)");
      else if(sig == SIGVTALRM) strcpy(msg, "SIGVTALRM:  Virtual Timer Expired");
      else if(sig == SIGPROF  ) strcpy(msg, "SIGPROF:    Profiling Timer Expired");
      else if(sig == SIGXCPU  ) strcpy(msg, "SIGXCPU:    CPU time limit exceeded");

      if(svc_debug_stream)
      {
	 fprintf(svc_debug_stream,
	    "SVC_DEBUG>  svc_sighandler(): signal = %d  msg = [%s]<br>\n",
	       sig, msg);
	 fflush(svc_debug_stream);
      }

      printf("[struct stat=\"ABORT\", msg=\"%s\"]\n", msg);
      fflush(stdout);

      svc_closeall();

      exit(0);
   }
}

