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
#include <ctype.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdarg.h>

#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <fcntl.h>
#include <grp.h>
#include <pwd.h>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>

#include "getif.h"
#include "utils.h"
#include "useinfo.h"
#include "machine.h"
#include "jobinfo.h"
#include "statinfo.h"
#include "appinfo.h"
#include "error.h"
#include "checksum.h"

#define YAML_SCHEMA_VERSION "3.0"

extern char **environ;

/* Return non-zero if any part of the job failed */
static int any_failure(const AppInfo *run) {
    if (run->status) return 1;
    if (run->setup.status) return 1;
    if (run->prejob.status) return 1;
    if (run->application.status) return 1;
    if (run->postjob.status) return 1;
    if (run->cleanup.status) return 1;
    return 0;
}

/* Extract all of the available job IDs out of the environment and put them in a <jobids> element */
static void printJobIDs(FILE *out) {

    fprintf(out, "  jobids:\n");

    char *condor = getenv("CONDOR_JOBID");
    if (condor) {
        fprintf(out, "    condor: %s\n", condor);
    }

    char *gram = getenv("GLOBUS_GRAM_JOB_CONTACT");
    if (gram) {
        fprintf(out, "    gram: %s\n", gram);
    }

    /* We assume that there is only going to be one LRM job ID */
    char *lrm;

    lrm = getenv("SLURM_JOBID");
    if (lrm) {
        fprintf(out, "    lrmtype: slurm\n");
        fprintf(out, "    lrm: %s\n", lrm);
        goto end;
    }

    lrm = getenv("COBALT_JOBID");
    if (lrm) {
        fprintf(out, "    lrmtype: cobalt\n");
        fprintf(out, "    lrm: %s\n", lrm);
        goto end;
    }

    lrm = getenv("JOB_ID");
    if (lrm) {
        fprintf(out, "    lrmtype: sge\n");
        fprintf(out, "    lrm: %s\n", lrm);
        goto end;
    }

    lrm = getenv("LSB_JOBID");
    if (lrm) {
        fprintf(out, "    lrmtype: lsf\n");
        fprintf(out, "    lrm: %s\n", lrm);
        goto end;
    }

    /* Do PBS last so that a more specific LRM is identified, if possible */
    lrm = getenv("PBS_JOBID");
    if (lrm) {
        fprintf(out, "    lrmtype: pbs\n");
        fprintf(out, "    lrm: %s\n", lrm);
        goto end;
    }

end:
    return;
}

/* callback for qsort(3) on array of strings */
static int comparator(const void* a, const void* b) {
  return strcmp(*((const char**) a), *((const char**)b));
}

static size_t convert2YAML(FILE *out, const AppInfo* run) {
    size_t i;
    struct passwd* user = getpwuid(getuid());
    struct group* group = getgrgid(getgid());

    fprintf(out, "- invocation: True\n"
                 "  version: " YAML_SCHEMA_VERSION "\n");

    /* start */
    fprintf(out, "  start: %s\n", fmtisodate(run->start.tv_sec,
                                             run->start.tv_usec));

    /* duration */
    fprintf(out, "  duration: %.3f\n",
            doubletime(run->finish) - doubletime(run->start));

    /* optional attributes for root element: transformation fqdn */
    if (run->xformation && strlen(run->xformation)) {
        fprintf(out, "  transformation: \"");
        yamlquote(out, run->xformation, strlen(run->xformation));
        fprintf(out, "\"\n");
    }

    /* optional attributes for root element: derivation fqdn */
    if (run->derivation && strlen(run->derivation)) {
        fprintf(out, "  derivation: \"");
        yamlquote(out, run->derivation, strlen(run->derivation));
        fprintf(out, "\"\n");
    }

    /* optional attributes for root element: name of remote site */
    if (run->sitehandle && strlen(run->sitehandle)) {
        fprintf(out, "  resource: \"");
        yamlquote(out, run->sitehandle, strlen(run->sitehandle));
        fprintf(out, "\"\n");
    }

    /* optional attribute for workflow label: name of workflow */
    if (run->wf_label && strlen(run->wf_label)) {
        fprintf(out, "  wf-label: \"");
        yamlquote(out, run->wf_label, strlen(run->wf_label));
        fprintf(out, "\"\n");
    }
    if (run->wf_stamp && strlen(run->wf_stamp)) {
        fprintf(out, "  wf-stamp: \"");
        yamlquote(out, run->wf_stamp, strlen(run->wf_stamp));
        fprintf(out, "\"\n");
    }

    /* optional attributes for root element: host address dotted quad */
    if (isdigit(run->ipv4[0])) {
        struct hostent* h;
        in_addr_t address = inet_addr(run->ipv4);
        fprintf(out, "  interface: %s\n", run->prif);
        fprintf(out, "  hostaddr: %s\n", run->ipv4);
        if ((h = gethostbyaddr((const char*) &address, sizeof(in_addr_t), AF_INET))) {
            fprintf(out, "  hostname: %s\n", h->h_name);
        }
    }

    /* optional attributes for root element: application process id */
    if (run->child != 0) {
        fprintf(out, "  pid: %d\n", run->child);
    }

    /* user info about who ran this thing */
    fprintf(out, "  uid: %d\n", getuid());
    if (user) {
        fprintf(out, "  user: %s\n", user->pw_name);
    }

    /* group info about who ran this thing */
    fprintf(out, "  gid: %d\n", getgid());
    if (group) {
        fprintf(out, "  group: %s\n", group->gr_name);
    }

    /* currently active umask settings */
    fprintf(out, "  umask: 0o0%03o\n", run->umask);

    /* <setup>, <prejob>, <application>, <postjob>, <cleanup> */
    printYAMLJobInfo(out, 2, "setup", &run->setup);
    printYAMLJobInfo(out, 2, "prejob", &run->prejob);
    printYAMLJobInfo(out, 2, "mainjob", &run->application);
    printYAMLJobInfo(out, 2, "postjob", &run->postjob);
    printYAMLJobInfo(out, 2, "cleanup", &run->cleanup);

    /* <jobid> */
    printJobIDs(out);

    /* <cwd> */
    fprintf(out, "  cwd: ");
    if (run->workdir != NULL) {
        fprintf(out, "%s", run->workdir);
    }
    fprintf(out, "\n");

    /* <usage> own resources */
    printYAMLUseInfo(out, 2, "usage", &run->usage);

    if (!run->noHeader) {
        printYAMLMachineInfo(out, 2, "machine", &run->machine);
    }

    fprintf(out, "  files:\n");

    /* We include <data> in the <statcall>s if any job failed, or if the user
     * did not specify -q */
    int includeData = !run->omitData || any_failure(run);
    int useCDATA = run->useCDATA;

    /* User-specified initial and final arbitrary <statcall> records */
    if (run->icount && run->initial) {
        for (i=0; i<run->icount; ++i) {
            printYAMLStatInfo(out, 4, "initial", &run->initial[i], includeData, useCDATA, 1);
        }
    }
    if (run->fcount && run->final) {
        for (i=0; i<run->fcount; ++i) {
            printYAMLStatInfo(out, 4, "final", &run->final[i], includeData, useCDATA, 1);
        }
    }

    /* If yaml blob file exists (for example, created via pegasus-transfer), include it */
    print_pegasus_integrity_yaml_blob(out, run->integritydata.file.name);

    /* Default <statcall> records */
    printYAMLStatInfo(out, 4, "stdin", &run->input, includeData, useCDATA, 1);
    updateStatInfo(&(((AppInfo*) run)->output));
    printYAMLStatInfo(out, 4, "stdout", &run->output, includeData, useCDATA, 1);
    updateStatInfo(&(((AppInfo*) run)->error));
    printYAMLStatInfo(out, 4, "stderr", &run->error, includeData, useCDATA, 1);
    updateStatInfo(&(((AppInfo*) run)->metadata));
    printYAMLStatInfo(out, 4, "metadata", &run->metadata, 1, useCDATA, 0);

    /* If the job failed, or if the user requested the full kickstart record */
    if (any_failure(run) || run->fullInfo) {
        char** tmp;
        int N;

        /* Extra <statcall> records */
        printYAMLStatInfo(out, 4, "kickstart", &run->kickstart, includeData, useCDATA, 1);
        updateStatInfo(&(((AppInfo*) run)->logfile));
        printYAMLStatInfo(out, 4, "logfile", &run->logfile, includeData, useCDATA, 1);

        /* <environment> */
        fprintf(out, "  environment:\n");
        for (N=0; environ[N] != NULL; N++) ;

        /* tmp has pointers to strings, not strings themselves */
        tmp = (char**) malloc((N+1) * sizeof(char*));
        memcpy(tmp, environ, (N+1) * sizeof(char*));
        qsort(tmp, N, sizeof(char*), comparator);

        /* show environment variables sorted */
        for (i=0; tmp[i] != NULL; i++) {
            char *key = tmp[i];
            char *s;
            if (key && (s = strchr(key, '='))) {
                *s = '\0'; /* temporarily cut string here */
                fprintf(out, "    \"");
                yamlquote(out, key, strlen(key));
                fprintf(out, "\": \"");
                yamlquote(out, s+1, strlen(s+1));
                fprintf(out, "\"\n");
                *s = '='; /* reset string to original */
            }
        }
        free((void*) tmp);

        /* <resource>  limits */
        printYAMLLimitInfo(out, 2, &run->limits);

    } /* run->status || run->fullInfo */

    return 0;
}

static char* pattern(char* buffer, size_t size, const char* dir,
                     const char* sep, const char* file) {
    --size;
    buffer[size] = '\0'; /* reliably terminate string */
    strncpy(buffer, dir, size);
    strncat(buffer, sep, size);
    strncat(buffer, file, size);
    return buffer;
}

int initAppInfo(AppInfo* appinfo, int argc, char* const* argv) {
    /* purpose: initialize the data structure with defaults
     * paramtr: appinfo (OUT): initialized memory block
     *          argc (IN): from main()
     *          argv (IN): from main()
     */
    char tempname[BUFSIZ];
    size_t tempsize = BUFSIZ;

    /* find a suitable directory for temporary files */
    const char* tempdir = getTempDir();

    /* reset everything */
    memset(appinfo, 0, sizeof(AppInfo));

    /* init timestamps with defaults */
    now(&appinfo->start);
    appinfo->finish = appinfo->start;

    /* obtain umask */
    appinfo->umask = umask(0);
    umask(appinfo->umask);

    /* obtain system information */
    initMachineInfo(&appinfo->machine);

    /* initialize some data for myself */
    initStatInfoFromName(&appinfo->kickstart, argv[0], O_RDONLY, 0);

    /* default for stdin */
    initStatInfoFromName(&appinfo->input, "/dev/null", O_RDONLY, 0);

    /* default for stdout */
    pattern(tempname, tempsize, tempdir, "/", "ks.out.XXXXXX");
    initStatInfoAsTemp(&appinfo->output, tempname);

    /* default for stderr */
    pattern(tempname, tempsize, tempdir, "/", "ks.err.XXXXXX");
    initStatInfoAsTemp(&appinfo->error, tempname);

    /* default for stdlog */
    initStatInfoFromHandle(&appinfo->logfile, STDOUT_FILENO);

    /* metadata */
    pattern(tempname, tempsize, tempdir, "/", "ks.meta.XXXXXX");
    initStatInfoAsTemp(&appinfo->metadata, tempname);

    /* integrity data */
    pattern(tempname, tempsize, tempdir, "/", "ks.integrity.XXXXXX");
    initStatInfoAsTemp(&appinfo->integritydata, tempname);

    /* original argument vector */
    appinfo->argc = argc;
    appinfo->argv = argv;

    /* where do I run -- guess the primary interface IPv4 dotted quad */
    /* find out where we run at (might stall LATER for some time on DNS) */
    whoami(appinfo->ipv4, sizeof(appinfo->ipv4),
           appinfo->prif, sizeof(appinfo->prif));

    /* record resource limits */
    initLimitInfo(&appinfo->limits);

    /* which process is me */
    appinfo->child = getpid();

    /* Set defaults for job timeouts */
    appinfo->termTimeout = 0;
    appinfo->killTimeout = 5;
    appinfo->currentChild = 0;
    appinfo->nextSignal = SIGTERM;

    return 0;
}

int countProcs(JobInfo *job) {
    int procs = 0;
    ProcInfo *i;
    for (i=job->children; i; i=i->next){
        procs++;
    }
    return procs;
}

int printAppInfo(AppInfo* run) {
    /* purpose: output the given app info onto the given fd
     * paramtr: run (IN): is the collective information about the run
     * returns: the number of characters actually written (as of write() call).
     *          if negative, check with errno for the cause of write failure. */
    int result = -1;

    /* Get the descriptor to write to */
    int fd;
    if (run->logfile.source == IS_FILE) {
        fd = open(run->logfile.file.name, O_WRONLY | O_APPEND | O_CREAT, 0644);
    } else {
        fd = run->logfile.file.descriptor;
    }

    if (fd < 0) {
        printerr("ERROR: Unable to open output file\n");
        return -1;
    }

    /* Create a stream for the file. We use dup so that we can call
     * fclose later regardless of whether fd is stdout/stderr.
     */
    FILE *out = fdopen(dup(fd), "w");
    if (out == NULL) {
        printerr("ERROR: Unable to open output stream\n");
        goto exit;
    }

    /* what about myself? Update stat info on log file */
    updateStatInfo(&run->logfile);

    /* obtain resource usage for xxxx */
    getrusage(RUSAGE_SELF, &run->usage);

    /* FIXME: is this true and necessary? */
    updateLimitInfo(&run->limits);

    /* stop the clock */
    now(&run->finish);

    /* print the invocation record */
    result = convert2YAML(out, run);

    /* make sure the data is completely flushed */
    fflush(out);
    fsync(fileno(out));

    run->isPrinted = 1;

    fclose(out);

exit:
    if (run->logfile.source == IS_FILE) {
        fsync(fd);
        close(fd);
    }

    return result;
}

void deleteAppInfo(AppInfo* runinfo) {
    size_t i;

    deleteLimitInfo(&runinfo->limits);

    deleteStatInfo(&runinfo->input);
    deleteStatInfo(&runinfo->output);
    deleteStatInfo(&runinfo->error);
    deleteStatInfo(&runinfo->logfile);
    deleteStatInfo(&runinfo->kickstart);
    deleteStatInfo(&runinfo->metadata);
    deleteStatInfo(&runinfo->integritydata);

    if (runinfo->icount && runinfo->initial) {
        for (i=0; i<runinfo->icount; ++i) {
            deleteStatInfo(&runinfo->initial[i]);
        }
    }
    if (runinfo->fcount && runinfo->final) {
        for (i=0; i<runinfo->fcount; ++i) {
            deleteStatInfo(&runinfo->final[i]);
        }
    }

    deleteJobInfo(&runinfo->setup);
    deleteJobInfo(&runinfo->prejob);
    deleteJobInfo(&runinfo->application);
    deleteJobInfo(&runinfo->postjob);
    deleteJobInfo(&runinfo->cleanup);

    /* release system information */
    deleteMachineInfo(&runinfo->machine);

    memset(runinfo, 0, sizeof(AppInfo));
}
