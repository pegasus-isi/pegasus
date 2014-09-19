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

#include <sys/wait.h>
#include <unistd.h>
#include <signal.h>
#include <fcntl.h>

#include "getif.h"
#include "utils.h"
#include "useinfo.h"
#include "jobinfo.h"
#include "parse.h"

void initJobInfoFromString(JobInfo* jobinfo, const char* commandline) {
    /* purpose: initialize the data structure with default
     * paramtr: jobinfo (OUT): initialized memory block
     *          commandline (IN): commandline concatenated string to separate
     */
    size_t i;
    char* t;
    int state = 0;
    Node* head = parseCommandLine(commandline, &state);

    /* reset everything */
    memset(jobinfo, 0, sizeof(JobInfo));

    /* only continue in ok state AND if there is anything to do */
    if (state == 32 && head) {
        size_t size, argc = size = 0;
        Node* temp = head;
        while (temp) {
            size += (strlen(temp->data) + 1);
            argc++;
            temp = temp->next;
        }

        /* prepare copy area */
        jobinfo->copy = (char*) malloc(size+argc);
        if (jobinfo->copy == NULL) {
            fprintf(stderr, "malloc: %s\n", strerror(errno));
            return;
        }

        /* prepare argument vector */
        jobinfo->argc = argc;
        jobinfo->argv = (char* const*) calloc(argc+1, sizeof(char*));
        if (jobinfo->argv == NULL) {
            fprintf(stderr, "calloc: %s\n", strerror(errno));
            return;
        }

        /* copy list while updating argument vector and freeing lose arguments */
        t = jobinfo->copy;
        for (i=0; i < argc && (temp=head); ++i) {
            /* append string to copy area */
            size_t len = strlen(temp->data)+1;
            memcpy(t, temp->data, len);
            /* I hate nagging compilers which think they know better */
            memcpy((void*) &jobinfo->argv[i], &t, sizeof(char*));
            t += len;

            /* clear parse list while we are at it */
            head = temp->next;
            free((void*) temp->data);
            free((void*) temp);
        }
    }

    /* free list of (partial) argv */
    if (head) deleteNodes(head);

    /* this is a valid (and initialized) entry */
    if (jobinfo->argc > 0) {
        /* check out path to job */
        char* realpath = findApp(jobinfo->argv[0]);

        if (realpath) {
            /* I hate nagging compilers which think they know better */
            memcpy((void*) &jobinfo->argv[0], &realpath, sizeof(char*));
            jobinfo->isValid = 1;
        } else {
            jobinfo->status = -127;
            jobinfo->saverr = errno;
            jobinfo->isValid = 2;
        }
        /* initialize some data for myself */
        initStatInfoFromName(&jobinfo->executable, jobinfo->argv[0], O_RDONLY, 0);
    }
}

void initJobInfo(JobInfo* jobinfo, int argc, char* const* argv) {
    /* purpose: initialize the data structure with defaults
     * paramtr: jobinfo (OUT): initialized memory block
     *          argc (IN): adjusted argc string (maybe from main())
     *          argv (IN): adjusted argv string to point to executable
     */
    size_t i;
    char* t;
    int state = 0;
    Node* head = parseArgVector(argc, argv, &state);

    /* initialize memory */
    memset(jobinfo, 0, sizeof(JobInfo));

    /* only continue in ok state AND if there is anything to do */
    if (state == 32 && head) {
        size_t size, argc = size = 0;
        Node* temp = head;

        while (temp) {
            size += (strlen(temp->data) + 1);
            argc++;
            temp = temp->next;
        }

        /* prepare copy area */
        jobinfo->copy = (char*) malloc(size+argc);
        if (jobinfo->copy == NULL) {
            fprintf(stderr, "malloc: %s\n", strerror(errno));
            return;
        }

        /* prepare argument vector */
        jobinfo->argc = argc;
        jobinfo->argv = (char* const*) calloc(argc+1, sizeof(char*));
        if (jobinfo->argv == NULL) {
            fprintf(stderr, "calloc: %s\n", strerror(errno));
            return;
        }

        /* copy list while updating argument vector and freeing lose arguments */
        t = jobinfo->copy;
        for (i=0; i < argc && (temp=head); ++i) {
            /* append string to copy area */
            size_t len = strlen(temp->data)+1;
            memcpy(t, temp->data, len);
            /* I hate nagging compilers which think they know better */
            memcpy((void*) &jobinfo->argv[i], &t, sizeof(char*));
            t += len;

            /* clear parse list while we are at it */
            head = temp->next;
            free((void*) temp->data);
            free((void*) temp);
        }
    }

    /* free list of (partial) argv */
    if (head) {
        deleteNodes(head);
    }

    /* this is a valid (and initialized) entry */
    if (jobinfo->argc > 0) {
        /* check out path to job */
        char* realpath = findApp(jobinfo->argv[0]);

        if (realpath) {
            /* I hate nagging compilers which think they know better */
            memcpy((void*) &jobinfo->argv[0], &realpath, sizeof(char*));
            jobinfo->isValid = 1;
        } else {
            jobinfo->status = -127;
            jobinfo->saverr = errno;
            jobinfo->isValid = 2;
        }

        /* initialize some data for myself */
        initStatInfoFromName(&jobinfo->executable, jobinfo->argv[0], O_RDONLY, 0);
    }
}

int printXMLJobInfo(FILE *out, int indent, const char* tag, const JobInfo* job) {
    /* purpose: format the job information into the given stream as XML.
     * paramtr: out (IO): the stream
     *          indent (IN): indentation level
     *          tag (IN): name to use for element tags.
     *          job (IN): job info to print.
     * returns: number of characters put into buffer (buffer length)
     */

    /* sanity check */
    if (!job->isValid) {
        return 0;
    }

    /* start tag with indentation */
    fprintf(out, "%*s<%s start=\"%s\"", indent, "", tag,
            fmtisodate(job->start.tv_sec, job->start.tv_usec));
    fprintf(out, " duration=\"%.3f\"",
            doubletime(job->finish) - doubletime(job->start));

    /* optional attribute: application process id */
    if (job->child != 0) {
        fprintf(out, " pid=\"%d\"", job->child);
    }

    /* finalize open tag of element */
    fprintf(out, ">\n");

    /* <usage> */
    printXMLUseInfo(out, indent+2, "usage", &job->use);

    int status = (int) job->status;

    /* <status>: open tag */
    fprintf(out, "%*s<status raw=\"%d\">", indent+2, "", status);

    /* <status>: cases of completion */
    if (status < 0) {
        /* <failure> */
        fprintf(out, "<failure error=\"%d\">%s%s</failure>", job->saverr,
                job->prefix && job->prefix[0] ? job->prefix : "",
                strerror(job->saverr));
    } else if (WIFEXITED(status)) {
        fprintf(out, "<regular exitcode=\"%d\"/>", WEXITSTATUS(status));
    } else if (WIFSIGNALED(status)) {
        /* result = 128 + WTERMSIG(status); */
        fprintf(out, "<signalled signal=\"%u\"", WTERMSIG(status));
#ifdef WCOREDUMP
        fprintf(out, " corefile=\"%s\"", WCOREDUMP(status) ? "true" : "false");
#endif
        fprintf(out, ">%s</signalled>", sys_siglist[WTERMSIG(status)]);
    } else if (WIFSTOPPED(status)) {
        fprintf(out, "<suspended signal=\"%u\">%s</suspended>", WSTOPSIG(status),
                sys_siglist[WSTOPSIG(status)]);
    } /* FIXME: else? */
    fprintf(out, "</status>\n");

    /* <executable> */
    printXMLStatInfo(out, indent+2, "statcall", NULL, &job->executable, 1);

    /* alternative 1: new-style <argument-vector> */
    fprintf(out, "%*s<argument-vector", indent+2, "");
    if (job->argc == 1) {
        /* empty element */
        fprintf(out, "/>\n");
    } else {
        /* content are the CLI args */
        int i;

        fprintf(out, ">\n");
        for (i=1; i<job->argc; ++i) {
            fprintf(out, "%*s<arg nr=\"%d\">", indent+4, "", i);
            xmlquote(out, job->argv[i], strlen(job->argv[i]));
            fprintf(out, "</arg>\n");
        }

        /* end tag */
        fprintf(out, "%*s</argument-vector>\n", indent+2, "");
    }

    /* <proc>s */
    printXMLProcInfo(out, indent+2, job->children);

    /* finalize close tag of outmost element */
    fprintf(out, "%*s</%s>\n", indent, "", tag);

    return 0;
}

void deleteJobInfo(JobInfo* jobinfo) {
    /* purpose: destructor
     * paramtr: runinfo (IO): valid AppInfo structure to destroy. */
    if (jobinfo == NULL) {
        return;
    }

    if (jobinfo->isValid) {
        if (jobinfo->argv[0] != NULL && jobinfo->argv[0] != jobinfo->copy) {
            free((void*) jobinfo->argv[0]); /* from findApp() allocation */
        }
        deleteStatInfo(&jobinfo->executable);
    }

    if (jobinfo->copy != NULL) {
        free((void*) jobinfo->copy);
        free((void*) jobinfo->argv);
        jobinfo->copy = 0;
    }

    deleteProcInfo(jobinfo->children);
    jobinfo->children = NULL;

    /* final invalidation */
    jobinfo->isValid = 0;
}

