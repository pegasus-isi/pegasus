/*
 * This file or a portion of this file is licensed under the terms of
 * the Globus Toolkit Public License, found in file GTPL, or at
 * http://www.globus.org/toolkit/download/license.html. This notice must
 * appear in redistributions of this file, with or without modification.
 *
 * Redistributions of this Software, with or without modification, must
 * reproduce the GTPL in: (1) the Software, or (2) the Documentation or
 * some other similar material which is provided with the Software (if
 * any).
 *
 * Copyright 1999-2004 University of Chicago and The University of
 * Southern California. All rights reserved.
 */
#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <memory.h>
#include <errno.h>
#include <sys/wait.h>
#include <signal.h>

#include "mypopen.h"

int
mysystem( const char* tag, char* argv[], char* envp[],
	  int fd_input, int fd_output, int fd_error )
/* purpose: fork off a command and reconnect its stdio
 * paramtr: tag (IN): Some name to use while logging.
 *          argv (IN): the true argv[] vector for execve
 *          envp (IN): the true envp[] vector for execve
 *          fd_input (IN): if >= 0, connect stdin to this FD
 *          fd_output (IN): if >= 0, connect stdout to this FD
 *          fd_error (IN): if >= 0, connect stderr to this FD
 * returns: >=0: see status in wait(2) and waitpid(2)
 *               special child exit code 126: unable to connect FDs
 *               special child exit code 127: unable to exec command
 *           -1: error setting up
 * globals: modifies errno. 
 */
{
  struct sigaction ignore, saveintr, savequit;
  sigset_t childmask, savemask;
  pid_t child;
  int saverr, status = -1;

  ignore.sa_handler = SIG_IGN;
  sigemptyset( &ignore.sa_mask );
  ignore.sa_flags = 0;
  if ( sigaction( SIGINT, &ignore, &saveintr ) < 0 )
    return status;
  if ( sigaction( SIGQUIT, &ignore, &savequit ) < 0 ) {
    sigaction( SIGINT, &saveintr, NULL );
    return status;
  }

  sigemptyset( &childmask );
  sigaddset( &childmask, SIGCHLD );
  if ( sigprocmask( SIG_BLOCK, &childmask, &savemask ) < 0 ) {
    sigaction( SIGINT, &saveintr, NULL );
    sigaction( SIGQUIT, &savequit, NULL );
    return status;
  }

  if ( (child=fork()) < 0 ) {
    /* no more process table space */
     fprintf( stderr, "Error: %s fork: %s\n", tag, strerror(errno) );
     status = -1; 
  } else if ( child == (pid_t) 0 ) {
    /* connect jobs stdio */
    if ( fd_input >= 0 && fd_input != STDIN_FILENO ) {
      if ( dup2( fd_input, STDIN_FILENO ) ) _exit(126);
    }
    if ( fd_output >= 0 && fd_output != STDOUT_FILENO ) {
      if ( dup2( fd_output, STDOUT_FILENO ) ) _exit(126);
    }
    if ( fd_error >= 0 && fd_error != STDERR_FILENO ) {
      if ( dup2( fd_error, STDERR_FILENO ) ) _exit(126);
    }

    /* undo signal handlers */
    sigaction( SIGINT, &saveintr, NULL );
    sigaction( SIGQUIT, &savequit, NULL );
    sigprocmask( SIG_SETMASK, &savemask, NULL );

    execve( argv[0], argv, envp );
    _exit(127); /* executed in child process */
  } else {
    /* parent */
    while ( waitpid( child, &status, 0 ) < 0 ) {
      if ( errno != EINTR ) {
	saverr = errno;
	fprintf( stderr, "Error: %s waitpid: %s\n", tag, strerror(saverr) );
	errno = saverr;
        status = -1;
        break;
      }
    }

    /* sanity check */
    saverr = errno;
    if ( kill( child, 0 ) == 0 )
      fprintf( stderr, "Warning: %s's job %d is still running!\n", tag, child );
    errno = saverr;
  }

  /* save any errors before anybody overwrites this */
  saverr = errno;

  /* ignore errors on these, too. */
  sigaction( SIGINT, &saveintr, NULL );
  sigaction( SIGQUIT, &savequit, NULL );
  sigprocmask( SIG_SETMASK, &savemask, NULL );

  /* finalize */
  errno = saverr;
  return status;
}

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

extern char** environ;

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
      fprintf(stderr,"xx a wait\n");
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

int 
chop_up_cmd(char *argvstr, char* argv[], int cnt)
{
  int i=0;
  char *tmpstr;
  char *tmpptr;
  char *ptr;

  if ( argvstr==NULL ) return 0;
  /* skip '\n', for some reason there is a \n */
  for ( i=0; i<strlen(argvstr); i++ )
    if ( argvstr[i] == '\n' ) argvstr[i]='\0';

  tmpstr=(char *) malloc(sizeof(char*)*(strlen(argvstr)+1));
  tmpptr=tmpstr;

  i=0;
  if ( (ptr=strtok_r(argvstr," ",&tmpstr)) != NULL ) {
    argv[i]=ptr;
    i++;
  } else {
    free((void*) tmpptr);
    return 0;
  } 

  while (i<cnt && (ptr=strtok_r(NULL," ",&tmpstr))!= NULL)
  {
    argv[i]=ptr;
    i++;
  }
  argv[i]=NULL;

  free(tmpptr);
  return i;
}

int
exec_cmd( char* cmd, char* envp[], char* buffer, size_t blen )
{
  char *argv[125];
  char *argvstr;
  int   result = -1;

  argvstr=strdup(cmd);
  if ( chop_up_cmd(argvstr,argv,125) != 0 )
    result = pipe_out_cmd( "mGenericExec", argv, envp, buffer, blen );

  free((void*) argvstr);
  return result;
}

int
exec_cmd2( char* cmd, char* buffer, size_t blen )
{
  return exec_cmd( cmd, environ, buffer, blen );
}

