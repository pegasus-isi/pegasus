#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <memory.h>
#include <errno.h>
#include <sys/wait.h>
#include "mypopen.h"

static const char* RCS_Id =
"$Id$";

PipeCmd*
mypopen( const char* tag, char* argv[], char* envp[] )
/* purpose: fork off a commend and capture its stderr and stdout. 
 * warning: does not use /bin/sh -c internally. 
 * paramtr: tag (IN): some short tag to name the app
 *          argv (IN): the true argv[] vector for execve
 *          envp (IN): the true envp[] vector for execve
 * returns: a structure which contains information about the child process.
 *          it will return NULL on failure. */
{
  pid_t  child;
  int    pfds[2];
  PipeCmd* result = NULL;

  /* create communication with subprocess */
  if ( pipe(pfds) == -1 ) {
    fprintf( stderr, "Error: %s create pipe: %s\n", tag, strerror(errno) );
    return result;
  }

  /* prepare for fork */
  fflush( stdout );
  fflush( stderr );

  /* popen(): spawn child process to execute grid-proxy-info */
  if ( (child=fork()) == (pid_t) -1 ) {
    /* unable to fork */
    fprintf( stderr, "Error: %s fork: %s\n", tag, strerror(errno) );
    return result;
  } else if ( child == 0 ) {
    /* child - redirect stdout and stderr onto communication channel */
    close(pfds[0]);

    if ( dup2( pfds[1], STDOUT_FILENO ) == -1 ) _exit(126);
    if ( dup2( pfds[1], STDERR_FILENO ) == -1 ) _exit(126);
    close(pfds[1]);

    execve( argv[0], argv, envp );
    _exit(127);    /* if you reach this, exec failed */
  } 
    
  /* parent */
  close(pfds[1]);

  /* prepare result */
  if ( (result = (PipeCmd*) malloc( sizeof(PipeCmd) )) != NULL ) {
    result->child = child;
    result->readfd = pfds[0];
  }

  return result;
}

int
mypclose( PipeCmd* po )
/* purpose: free the data structure and all associated resources.
 * paramtr: po (IO): is a valid pipe open structure.
 * returns: process exit status, or -1 for invalid po structure. */
{
  int status = -1;

  /* sanity check */
  if ( po != NULL ) {

    /* close fd early to send SIGPIPE */
    close(po->readfd);

    /* wait for child */
    while ( waitpid( po->child, &status, 0 ) == -1 ) {
      if ( errno == EINTR || errno == EAGAIN ) continue;
      fprintf( stderr, "Error: waiting for child %d: %s\n", 
	       po->child, strerror(errno) );
      status = -1;
    }    

    /* done with memory piece */
    free( (void*) po );
  }
   
  return status;
}

int
pipe_out_cmd( const char* tag, char* argv[], char* envp[], 
	      char* buffer, size_t blen )
/* purpose: fork off a commend and capture its stderr and stdout
 * paramtr: name (IN): some short tag to name the app
 *          argv (IN): the true argv[] vector for execve
 *          envp (IN): the true envp[] vector for execve
 *          buffer (OUT): area to store output into. Will be cleared
 *          blen (IN): length of the area that is usable to us. 
 * returns: -1 for regular failure, exit code from application otherwise
 */
{
  ssize_t rsize, wsize = 0;
  PipeCmd* cmd = mypopen( tag, argv, envp );
  
  /* prepare */
  if ( cmd == NULL ) return -1;
  else memset( buffer, 0, blen );

  /* read result(s) */
  while ( (rsize=read( cmd->readfd, buffer+wsize, blen-wsize )) > 0 && 
	  wsize < blen ) {
    wsize += rsize;
  }

  /* done with it */
  return mypclose(cmd);
}
