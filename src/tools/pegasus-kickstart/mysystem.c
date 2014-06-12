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
#include <sys/types.h>
#include <errno.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <signal.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <fcntl.h>
#include <stdio.h>
#include <libgen.h>
#include <dirent.h>

#include "debug.h"
#include "tools.h"
#include "appinfo.h"
#include "statinfo.h"
#include "mysystem.h"
#include "mysignal.h"
#include "procinfo.h"
#include "access.h"

/* The name of the program (argv[0]) set in pegasus-kickstart.c:main */
char *programname;

/* Find the path to the interposition library */
static int findInterposeLibrary(char *path, int pathsize) {
    // TODO Get arg0 from a global var or something
    char kickstart[BUFSIZ];
    char lib[BUFSIZ];

    // Find the real path of kickstart
    if (realpath(programname, kickstart) < 0) {
        return -1;
    }

    // Find the directory containing kickstart
    char *dir = dirname(kickstart);

    // Check if the library is in the kickstart dir
    strcpy(lib, dir);
    strcat(lib, "/libinterpose.so");
    if (access(lib, R_OK) == 0) {
        strncpy(path, lib, pathsize);
        return 0;
    }

    // Not in the kickstart dir, try the ../lib/pegasus/ dir instead
    dir = dirname(dir);
    strcpy(lib, dir);
    strcat(lib, "/lib/pegasus/libinterpose.so");
    if (access(lib, R_OK) == 0) {
        strncpy(path, lib, pathsize);
        return 0;
    }

    // Not found
    return -1;
}

void processTraceFiles(const char *tempdir, const char *trace_file_prefix, AppInfo* appinfo) {
  FileAccess *accesses = NULL;
  FileAccess *lastacc = NULL;

  DIR *tmp = opendir(tempdir);
  if (tmp == NULL) {
    fprintf(stderr, "Unable to open trace file directory: %s", tempdir);
  } else {
    struct dirent *d;
    for (d = readdir(tmp); d!=NULL; d = readdir(tmp)) {
      if (strstr(d->d_name, trace_file_prefix) == d->d_name) {
        char fullpath[BUFSIZ];
        snprintf(fullpath, BUFSIZ, "%s/%s", tempdir, d->d_name);
        /* Read data from the trace files */
        FILE *trace = fopen(fullpath, "r");
        char buf[BUFSIZ];
        char filename[BUFSIZ];
        size_t size = 0;
        while (1) {
            if (fgets(buf, BUFSIZ, trace) == NULL) {
                break;
            } else {
                /* TODO Filter out duplicates? */
                /* TODO Should we print the start/end sizes? */

                sscanf(buf, "%s %lu", filename, &size);

                /* If the file size is zero, then probably the file was created
                 * or truncated and we should stat it again to get the final
                 * size.
                 * TODO Should this be done only for files opened for write/append?
                 */
                if (size == 0) {
                    struct stat st;
                    if (stat(filename, &st) == 0) {
                        size = st.st_size;
                    }
                }

                FileAccess *acc = (FileAccess *)calloc(sizeof(FileAccess), 1);
                acc->filename = strdup(filename);
                acc->size = size;

                if (accesses == NULL) {
                  accesses = acc;
                  lastacc = acc;
                } else {
                  lastacc->next = acc;
                  lastacc = acc;
                }
            }
        }
        fclose(trace);
        unlink(fullpath);
      }
    }
    closedir(tmp);
  }

  appinfo->accesses = accesses;
}

int mysystem(AppInfo* appinfo, JobInfo* jobinfo, char* envp[])
/* purpose: emulate the system() libc call, but save utilization data. 
 * paramtr: appinfo (IO): shared record of information
 *                        isPrinted (IO): reset isPrinted in child process!
 *                        input (IN): connect to stdin or share
 *                        output (IN): connect to stdout or share
 *                        error (IN): connect to stderr or share
 *          jobinfo (IO): updated record of job-specific information
 *                        argv (IN): assembled commandline
 *                        child (OUT): pid of child process
 *                        status (OUT): also returned as function result
 *                        saverr (OUT): will be set to value of errno
 *                        start (OUT): will be set to startup time
 *                        final (OUT): will be set to finish time after reap
 *                        use (OUT): rusage record from application call
 *          input (IN): connect to stdin or share
 *          output (IN): connect to stdout or share
 *          error (IN): connect to stderr or share
 *          envp (IN): vector with the parent's environment
 * returns:   -1: failure in mysystem processing, check errno
 *           126: connecting child to its new stdout failed
 *           127: execve() call failed
 *          else: status of child
 */
{
  struct sigaction ignore, saveintr, savequit;

  /* sanity checks first */
  if ( ! jobinfo->isValid ) {
    errno = ENOEXEC; /* no executable */
    return -1;
  }

  memset( &ignore, 0, sizeof(ignore) );
  ignore.sa_handler = SIG_IGN;
  sigemptyset( &ignore.sa_mask );
  ignore.sa_flags = 0;
  if ( sigaction( SIGINT, &ignore, &saveintr ) < 0 )
    return -1;
  if ( sigaction( SIGQUIT, &ignore, &savequit ) < 0 )
    return -1;

  /* Prefix for trace files generated by this job */
  char trace_file_prefix[128];
  snprintf(trace_file_prefix, 128, "gs.trace.%d", getpid());

  /* Temp dir where trace files are stored for this job */
  const char *tempdir = getTempDir();
  if (tempdir == NULL) {
      tempdir = "/tmp";
  }

  /* start wall-clock */
  now( &(jobinfo->start) );

  if ( (jobinfo->child=fork()) < 0 ) {
    /* no more process table space */
    jobinfo->status = -1;
  } else if ( jobinfo->child == 0 ) {
    /* child */
    appinfo->isPrinted=1;

    /* If we are using library tracing, try to set the necessary environment variables */
    if (appinfo->enableLibTrace) {

      /* If the interposition library was found, then update the environment */
      char libpath[BUFSIZ];
      if (findInterposeLibrary(libpath, BUFSIZ) == 0) {

        char ld_preload[BUFSIZ];
        snprintf(ld_preload, BUFSIZ, "LD_PRELOAD=%s", libpath);

        char kickstart_prefix[BUFSIZ];
        snprintf(kickstart_prefix, BUFSIZ, "KICKSTART_PREFIX=%s/%s", tempdir, trace_file_prefix);

        /* If the user has LD_PRELOAD already set, then ours will be ignored
         * because it is at the end. That is the correct behavior.
         */

        /* Copy the environment variables to a new array */
        int vars; for (vars=0; envp[vars] != NULL; vars++);
        char **newenvp = (char **)malloc(sizeof(char **)*(vars+3));
        for (vars=0; envp[vars] != NULL; vars++) {
          newenvp[vars] = envp[vars];
        }

        newenvp[vars] = ld_preload;
        newenvp[vars+1] = kickstart_prefix;
        newenvp[vars+2] = NULL;

        envp = newenvp;
      }
    }

    /* connect jobs stdio */
    if ( forcefd( &appinfo->input, STDIN_FILENO ) ) _exit(126);
    if ( forcefd( &appinfo->output, STDOUT_FILENO ) ) _exit(126);
    if ( forcefd( &appinfo->error, STDERR_FILENO ) ) _exit(126);

    /* undo signal handlers */
    sigaction( SIGINT, &saveintr, NULL );
    sigaction( SIGQUIT, &savequit, NULL );

    /* If we are tracing, then hand over control to the proc module */
    if (appinfo->enableTracing) {
      if ( procChild() ) _exit(126);
    }

    execve( jobinfo->argv[0], (char* const*) jobinfo->argv, envp );
    perror("execve");
    _exit(127); /* executed in child process */
  } else {
    /* parent */
    if (appinfo->enableTracing) {
      /* TODO If this returns an error, then we need to untrace all the children and try the wait instead */
      procParentTrace(jobinfo->child, &jobinfo->status, &jobinfo->use, &(jobinfo->children), appinfo->enableSysTrace);
    } else {
      procParentWait(jobinfo->child, &jobinfo->status, &jobinfo->use, &(jobinfo->children));
    }

    /* sanity check */
    if ( kill( jobinfo->child, 0 ) == 0 ) {
      debugmsg( "ERROR: job %d is still running!\n", jobinfo->child );
      if ( ! errno ) errno = EINPROGRESS;
    }
  }

  /* save any errors before anybody overwrites this */
  jobinfo->saverr = errno;

  /* stop wall-clock */
  now( &(jobinfo->finish) );

  /* ignore errors on these, too. */
  sigaction( SIGINT, &saveintr, NULL );
  sigaction( SIGQUIT, &savequit, NULL );

  /* Look for trace files and do something with them */
  processTraceFiles(tempdir, trace_file_prefix, appinfo);

  /* finalize */
  return jobinfo->status;
}

