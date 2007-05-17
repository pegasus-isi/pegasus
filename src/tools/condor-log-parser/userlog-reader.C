#include <stdio.h>
#include <time.h>
#include <assert.h>
#include <string.h>
#include "user_log.c++.h"

int foundbad=0;

int print_detail(FILE *fp, ULogEvent *e)
{
  if(e==NULL) return 1;
  fprintf(fp,"%d.%d.%d ",
      e->cluster, e->proc, e->subproc);
  return 0;
}

int print_terminated_detail(FILE *fp, ULogEvent *e)
{
  TerminatedEvent *te=(TerminatedEvent *) e;
  if(e==NULL) return 1;

  if(te->returnValue==0) {
    } else {
      foundbad++;
      print_detail(fp,e);
      fprintf(fp,"0_TERMINATE_BAD\n");
  }  
  return te->returnValue;
}

int print_script_detail(FILE *fp, ULogEvent *e)
{
  PostScriptTerminatedEvent *te=(PostScriptTerminatedEvent *) e;
  if(e==NULL) return 1;

  if(te->returnValue==0) {
    } else {
      foundbad++;
      print_detail(fp,e);
      fprintf(fp,"0_POSTSCRIPT_BAD\n");
  }  
  return te->returnValue;
}

int print_aborted_detail(FILE *fp, ULogEvent *e)
{
  JobAbortedEvent *te=(JobAbortedEvent *) e;
  if(e==NULL) return 1;
  foundbad++;
  print_detail(fp,e);
  fprintf(fp,"0_ABORT_BAD \n");
  return 0;
}

void print_submit_detail(FILE *fp, ULogEvent *e)
{
   SubmitEvent *se=(SubmitEvent *)e;
   char ptr[125];
   if(e==NULL) return;
   if(se->submitEventUserNotes != NULL) { 
     sscanf(se->submitEventUserNotes,"    pool:%s",ptr);
     print_detail(fp, e);
     fprintf(fp,"%s\n", ptr);
   }
}

int main(int argc, char** argv)
{
  int i;
  bool done = false;
  ReadUserLog *ru=NULL;
  ULogEvent* e = NULL;
  FILE *tfp=NULL;
  
  if(argc != 2) {
    fprintf(stderr,"Usage: condor-log-parser  condor.log\n");
    return 1;
  } 
  tfp=fopen(argv[1],"r");
  if(tfp==NULL) {
    return 0;
  } else fclose(tfp);

  ru=new ReadUserLog(argv[1]);

  while( !done ) {
    
    ULogEventOutcome outcome = ru->readEvent( e );
    const char *eventName = NULL;
    
    switch (outcome) {
      
    case ULOG_NO_EVENT:
    case ULOG_RD_ERROR:
    case ULOG_UNK_ERROR:
      
      done = true;
      break;
      
    case ULOG_OK:
      {
        switch (e->eventNumber) {
          case ULOG_JOB_EVICTED:
          case ULOG_SHADOW_EXCEPTION:
          case ULOG_GLOBUS_SUBMIT_FAILED:
          case ULOG_GLOBUS_RESOURCE_DOWN:
          case ULOG_REMOTE_ERROR:
                print_detail(stdout, e);
                fprintf(stdout,"_BAD_%s\n",ULogEventNumberNames[e->eventNumber]);
                break;
          case ULOG_JOB_ABORTED:
                print_aborted_detail(stdout,e);
                break;
          case ULOG_POST_SCRIPT_TERMINATED:
                {
                int ret=0;
                if(((PostScriptTerminatedEvent *)e)->normal)
                   ret=print_script_detail(stdout,e);
                break;
                }
          case ULOG_JOB_TERMINATED:
                {
                int ret=0;
                if(((TerminatedEvent *)e)->normal)
                   ret=print_terminated_detail(stdout,e);
                }
                break;
          case ULOG_EXECUTABLE_ERROR:
                break;
          case ULOG_SUBMIT:
                print_submit_detail(stdout, e);
                break;
          default:
                break;
        }
      }
      break;
      
    default:
      assert( false );
      break;
    }
  }
  delete ru;

  if(foundbad) return 1;
  return 0;
}
