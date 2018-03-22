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
#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <time.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <signal.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <stdlib.h>
#include <limits.h>

#include "error.h"
#include "appinfo.h"
#include "mysystem.h"
#include "mylist.h"
#include "invoke.h"
#include "utils.h"
#include "version.h"
#include "ptrace.h"
#include "monitoring.h"
#include "log.h"

#define show(s) (s ? s : "(undefined)")

/* truly shared globals */
AppInfo appinfo; /* sigh, needs to be global for signal handlers */

/* module local globals */
static volatile sig_atomic_t alarmed = 0;
static volatile sig_atomic_t skip_atexit = 0;

static void on_alarm(int signal) {
    /* If this signal handler is invoked, then we need to do something special */
    alarmed = 1;

    /* If there is no current child, then we may be between jobs or at the
     * end of the job. In that case, set a short alarm to either catch the
     * next job or give kickstart enough time to finish
     */
    if (appinfo.currentChild == 0) {
        alarm(1);
        return;
    }

    if (appinfo.nextSignal == SIGKILL) {
        fprintf(stderr, "Job still running, sending SIGKILL\n");
    } else {
        fprintf(stderr, "Job timed out, sending SIGTERM\n");
    }

    kill(appinfo.currentChild, appinfo.nextSignal);

    /* Next time it will be killed */
    appinfo.nextSignal = SIGKILL;
    alarm(appinfo.killTimeout);
}

/* convert the raw result from wait() into a status code */
static int obtainStatusCode(int raw) {
    int result = 127;

    if (raw < 0) {
        /* nothing to do to result */
    } else if (WIFEXITED(raw)) {
        result = WEXITSTATUS(raw);
    } else if (WIFSIGNALED(raw)) {
        result = 128 + WTERMSIG(raw); 
    } else if (WIFSTOPPED(raw)) {
        /* nothing to do to result */
    }

    return result;
}

/* prepare a side job from environment string. return 1 if there is a job to run */
static int prepareSideJob(JobInfo* scripting, const char* envval) {
    /* no value, no job */
    if (envval == NULL) return 0;

    /* set-up scripting structure (which is part of the appinfo) */
    initJobInfoFromString(scripting, envval);

    /* execute process, if there is any */
    if (scripting->isValid != 1) return 0;

    return 1;
}

/* Initialize the statlist and statlist size in appinfo. */
StatInfo* initStatFromList(mylist_p list, size_t* size) {
    /* paramtr: list (IN): list of filenames
     *          size (OUT): statlist size to be set
     * returns: a vector of initialized statinfo records, or NULL
     */

    if (list->count == 0) {
        return NULL;
    }

    StatInfo* result = (StatInfo*) calloc(sizeof(StatInfo), list->count);
    if (result == NULL) {
        printerr("calloc: %s\n", strerror(errno));
        return NULL;
    }

    size_t i = 0;
    mylist_item_p item = list->head;

    while (item && i < list->count) {
        initStatInfoFromName(result+i, item->pfn, O_RDONLY, 0);
        if (item->lfn != NULL) addLFNToStatInfo(result+i, item->lfn);
        item = item->next;
        ++i;
    }

    *size = list->count;

    return result;
}

/* purpose: small helper for helpMe() function */
static const char* xlate(const StatInfo* info) {
    /* paramtr: info (IN): is a record about a file
     * returns: a pointer to the filename, or a local or static buffer w/ info
     * warning: Returns static buffer pointer
     */
    static char buffer[16];

    switch (info->source) {
        case IS_HANDLE:
            snprintf(buffer, sizeof(buffer), "&%d", info->file.descriptor);
            return buffer;
        case IS_FIFO:
        case IS_TEMP:
        case IS_FILE:
            return show(info->file.name);
        default:
            return "[INVALID]";
    }
}

static void helpMe(const AppInfo* run) {
    /* purpose: print invocation quick help with currently set parameters and
     *          exit with error condition.
     * paramtr: run (IN): constitutes the set of currently set parameters. */
    const char* p = strrchr(run->argv[0], '/');
    if (p) {
        ++p;
    } else {
        p=run->argv[0];
    }

    fprintf(stderr,
            "Usage:\t%s [-i fn] [-o fn] [-e fn] [-l fn] [-n xid] [-N did] \\\n"
            "\t[-w|-W cwd] [-R res] [-s [l=]p] [-S [l=]p] [-X] [-H] [-L lbl -T iso] \\\n" 
            "\t[-B sz] [-F] [-f] (-I fn | app [appflags])\n", p);
    fprintf(stderr,
            " -i fn\tConnects stdin of app to file fn, default is \"%s\".\n", 
            xlate(&run->input));
    fprintf(stderr,
            " -o fn\tConnects stdout of app to file fn, default is \"%s\".\n",
            xlate(&run->output));
    fprintf(stderr,
            " -e fn\tConnects stderr of app to file fn, default is \"%s\".\n", 
            xlate(&run->error));
    fprintf(stderr,
            " -l fn\tProtocols invocation record into file fn, default is \"%s\".\n",
            xlate(&run->logfile));
    fprintf(stderr, 
            " -n xid\tProvides the TR name, default is \"%s\".\n"
            " -N did\tProvides the DV name, default is \"%s\".\n" 
            " -R res\tReflects the resource handle into record, default is \"%s\".\n"
            " -B sz\tResizes the data section size for stdio capture, default is %zu.\n",
            show(run->xformation), show(run->derivation), 
            show(run->sitehandle), data_section_size);
    fprintf(stderr,
            " -L lbl\tReflects the workflow label into record, no default.\n"
            " -T iso\tReflects the workflow time stamp into record, no default.\n"
            " -H\tOmit <?xml ...?> header and <machine> from record. This is used\n"
            "   \tin clustered jobs to supress duplicate information.\n"
            " -I fn\tReads job and args from the file fn, one arg per line.\n"
            " -V\tDisplays the version and exit.\n"
            " -X\tMakes the application executable, no matter what.\n"
            " -w dir\tSets a different working directory dir for jobs.\n" 
            " -W dir\tLike -w, but also creates the directory dir if necessary.\n"
            " -S l=p\tProvides filename pairs to stat after start, multi-option.\n"
            " \tIf the arg is prefixed with '@', it is a list-of-filenames file.\n"
            " -s l=p\tProvides filename pairs to stat before exit, multi-option.\n"
            " \tIf the arg is prefixed with '@', it is a list-of-filenames file.\n"
            " -F\tThis flag does nothing. Kept for historical reasons.\n"
            " -f\tPrint full information including <resource>, <environment> and \n"
            "   \t<statcall>. If the job fails, then -f is implied.\n"
            " -q\tOmit <data> for <statcall> (stdout, stderr) if the job succeeds.\n"
            " -c\tUse CDATA for <data> sections\n");
    fprintf(stderr,
            " -k S\tSend TERM signal to job after S seconds. Default is 0, which means never.\n"
            " -K S\tSend KILL signal to job S seconds after a TERM signal. Default is %d.\n",
            run->killTimeout);
    fprintf(stderr, ""
#ifdef LINUX
#ifdef HAS_PTRACE
            " -t\tEnable resource usage tracing with ptrace\n"
            " -z\tEnable system call interposition to get files and I/O\n"
#endif
            " -Z\tEnable library call interposition to get files and I/O\n"
            " -m I\tEnable an online monitoring with a I-seconds interval between measurements.\n"
            "     \tWorks only with the -Z option.\n"
#endif
            /* NOTE: If you add another flag to kickstart, please update
             * the argument skipping logic in
             * pegasus-cluster/report.c:find_application()
             */
           );

    /* avoid printing of results in exit handler */
    ((AppInfo*) run)->isPrinted = 1;

    /* exit with error condition */
    exit(127);
}

static void finish() {
    if (!skip_atexit) {
        /* log the output here in case of abnormal termination */
        if (!appinfo.isPrinted) { 
            printAppInfo(&appinfo);
        }
        deleteAppInfo(&appinfo);
    }
}

static int readFromFile(const char* fn, char*** argv, int* argc, int* i, int j) {
    size_t newc = 2;
    size_t index = 0;
    char** newv = calloc(sizeof(char*), newc+1);
    if (newv == NULL) {
        printerr("calloc: %s\n", strerror(errno));
        return -1;
    }

    if (expand_arg(fn, &newv, &index, &newc, 0) == 0) {
        /* replace argv with newv */
        *argv = newv;
        *argc = index;
        *i = -1;
        return 0;
    } else {
        /* error parsing */
        return -1;
    }
}

/* Initialize stdout or stderr from commandline arguments */
static void handleOutputStream(StatInfo* stream, const char* temp, int std_fileno) {
    /* paramtr: stream (IO): pointer to the statinfo record for stdout or stderr
     *          temp (IN): command-line argument
     *          std_fileno (IN): STD(OUT|ERR)_FILENO matching to the stream
     */
    if (temp[0] == '-' && temp[1] == '\0') {
        initStatInfoFromHandle(stream, std_fileno);
    } else if (temp[0] == '!') {
        if (temp[1] == '^') {
            initStatInfoFromName(stream, temp+2, O_WRONLY | O_CREAT | O_APPEND, 6);
        } else {
            initStatInfoFromName(stream, temp+1, O_WRONLY | O_CREAT | O_APPEND, 2);
        }
    } else if (temp[0] == '^') {
        if (temp[1] == '!') {
            initStatInfoFromName(stream, temp+2, O_WRONLY | O_CREAT | O_APPEND, 6);
        } else {
            initStatInfoFromName(stream, temp+1, O_WRONLY | O_CREAT, 7);
        }
    } else {
        initStatInfoFromName(stream, temp, O_WRONLY | O_CREAT, 3);
    }
}

static char* noquote(char* s) {
    /* sanity check */
    if (!s) {
        return NULL;
    }
    if (!*s) {
        return s;
    }

    size_t len = strlen(s);

    if ((s[0] == '\'' && s[len-1] == '\'') ||
        (s[0] == '"' && s[len-1] == '"')) {
        char* tmp = calloc(sizeof(char), len);
        if (tmp == NULL) {
            printerr("calloc: %s\n", strerror(errno));
        }
        memcpy(tmp, s+1, len-2);
        return tmp;
    }

    return s;
}

/* If KICKSTART_PREPEND_PATH is in the environment, then add it to PATH */
void set_path() {
    char *prepend_path = getenv("KICKSTART_PREPEND_PATH");
    if (prepend_path == NULL || strlen(prepend_path) == 0) {
        return;
    }

    char *orig_path = getenv("PATH");
    if (orig_path == NULL || strlen(orig_path) == 0) {
        if (setenv("PATH", prepend_path, 1) < 0) {
            printerr("Error setting PATH to KICKSTART_PREPEND_PATH: %s\n", strerror(errno));
            exit(1);
        }
    } else {
        char new_path[PATH_MAX];
        if (snprintf(new_path, PATH_MAX, "%s:%s", prepend_path, orig_path) >= PATH_MAX) {
            printerr("New path from KICKSTART_PREPEND_PATH is larger than PATH_MAX\n");
            exit(1);
        }
        if (setenv("PATH", new_path, 1) < 0) {
            printerr("Error setting PATH with KICKSTART_PREPEND_PATH: %s\n", strerror(errno));
            exit(1);
        }
    }
}

int main(int argc, char* argv[]) {
    log_set_name("pegasus-kickstart");
    log_set_default_level();

    size_t cwd_size = getpagesize();
    int status, result = 0;
    int i, j, keeploop;
    int createDir = 0;
#ifdef LINUX
    int monitoringInterval = 0;
#endif
    char* temp;
    char* end;
    char* workdir = NULL;
    mylist_t initial;
    mylist_t final;

    /* premature init with defaults */
    if (mylist_init(&initial)) return 43;
    if (mylist_init(&final)) return 43;
    if (initAppInfo(&appinfo, argc, argv)) return 43;

    /* Set the default status to 1 */
    appinfo.status = 1;

    /* Set the PATH variable before we copy env into appinfo */
    set_path();
    
    /* Tell the app where to write integritydata */
    setenv("KICKSTART_INTEGRITY_DATA", appinfo.integritydata.file.name, 1);

    /* Tell the app where to write metadata */
    setenv("KICKSTART_METADATA", appinfo.metadata.file.name, 1);

    /* register emergency exit handler */
    if (atexit(finish) == -1) {
        appinfo.application.status = -1;
        appinfo.application.saverr = errno;
        printerr("Unable to register an exit handler\n");
        return 127;
    }

    /* no arguments, print help and exit */
    if (argc == 1) {
        helpMe(&appinfo);
    }

    /*
     * read commandline arguments
     */
    for (keeploop=i=1; i < argc && argv[i][0] == '-' && keeploop; ++i) {
        j = i;
        switch (argv[i][1]) {
            case 'B':
                if (!argv[i][2] && argc <= i+1) {
                    fprintf(stderr, "ERROR: -B argument missing\n");
                    return 127;
                }
                temp = argv[i][2] ? &argv[i][2] : argv[++i];
                end = temp;

                /* The special value 'all' means that we echo all the output */
                if (strcmp(temp, "all") == 0) {
                    data_section_size = ULONG_MAX;
                    break;
                }

                /* Otherwise, we expect a unsigned long value */
                size_t m = strtoul(temp, &end, 0);
                if (m == 0 || *end != '\0') {
                    fprintf(stderr, "ERROR: Invalid -B argument: %s\n", temp);
                    return 127;
                }
                data_section_size = m;

                break;
            case 'e':
                if (!argv[i][2] && argc <= i+1) {
                    fprintf(stderr, "ERROR: -e argument missing\n");
                    return 127;
                }
                if (appinfo.error.source != IS_INVALID) {
                    deleteStatInfo(&appinfo.error);
                }
                temp = (argv[i][2] ? &argv[i][2] : argv[++i]);
                handleOutputStream(&appinfo.error, temp, STDERR_FILENO);
                break;
            case 'h':
            case '?':
                helpMe(&appinfo);
                break; /* unreachable */
            case 'V':
                puts(PEGASUS_VERSION);
                appinfo.isPrinted=1;
                return 0;
            case 'i':
                if (!argv[i][2] && argc <= i+1) {
                    fprintf(stderr, "ERROR: -i argument missing\n");
                    return 127;
                }
                if (appinfo.input.source != IS_INVALID) {
                    deleteStatInfo(&appinfo.input);
                }
                temp = argv[i][2] ? &argv[i][2] : argv[++i];
                if (temp[0] == '-' && temp[1] == '\0') {
                    initStatInfoFromHandle(&appinfo.input, STDIN_FILENO);
                } else {
                    initStatInfoFromName(&appinfo.input, temp, O_RDONLY, 2);
                }
                break;
            case 'H':
                appinfo.noHeader++;
                break;
            case 'f':
                appinfo.fullInfo++;
                break;
            case 'F':
                /* This does nothing now */
                break;
            case 'I':
                /* XXX We expect exactly 1 argument after -I. If we see more,
                 * then it is considered to be an error. This should be fixed
                 * to work in a more sensible fashion.
                 */
                if (argc > i+2) {
                    fprintf(stderr, "ERROR: No arguments allowed after -I fn\n");
                    return 127;
                }

                if (!argv[i][2] && argc <= i+1) {
                    fprintf(stderr, "ERROR: -I argument missing\n");
                    return 127;
                }

                /* invoke application and args from given file */
                temp = argv[i][2] ? &argv[i][2] : argv[++i];
                if (readFromFile(temp, &argv, &argc, &i, j) == -1) {
                    int saverr = errno;
                    fprintf(stderr, "ERROR: While parsing -I %s: %d: %s\n",
                            temp, errno, strerror(saverr));
                    appinfo.application.prefix = strerror(saverr);
                    appinfo.application.status = -1;
                    return 127;
                }
                keeploop = 0;
                break;
            case 'l':
                if (!argv[i][2] && argc <= i+1) {
                    fprintf(stderr, "ERROR: -l argument missing\n");
                    return 127;
                }
                if (appinfo.logfile.source != IS_INVALID) {
                    deleteStatInfo(&appinfo.logfile);
                }
                temp = argv[i][2] ? &argv[i][2] : argv[++i];
                if (temp[0] == '-' && temp[1] == '\0') {
                    initStatInfoFromHandle(&appinfo.logfile, STDOUT_FILENO);
                } else {
                    initStatInfoFromName(&appinfo.logfile, temp, O_WRONLY | O_CREAT | O_APPEND, 2);
                }
                break;
            case 'L':
                if (!argv[i][2] && argc <= i+1) {
                    fprintf(stderr, "ERROR: -L argument missing\n");
                    return 127;
                }
                appinfo.wf_label = noquote(argv[i][2] ? &argv[i][2] : argv[++i]);
                break;
            case 'n':
                if (!argv[i][2] && argc <= i+1) {
                    fprintf(stderr, "ERROR: -n argument missing\n");
                    return 127;
                }
                appinfo.xformation = noquote(argv[i][2] ? &argv[i][2] : argv[++i]);
                break;
            case 'N':
                if (!argv[i][2] && argc <= i+1) {
                    fprintf(stderr, "ERROR: -N argument missing\n");
                    return 127;
                }
                appinfo.derivation = noquote(argv[i][2] ? &argv[i][2] : argv[++i]);
                break;
            case 'o':
                if (!argv[i][2] && argc <= i+1) {
                    fprintf(stderr, "ERROR: -o argument missing\n");
                    return 127;
                }
                if (appinfo.output.source != IS_INVALID) {
                    deleteStatInfo(&appinfo.output);
                }
                temp = (argv[i][2] ? &argv[i][2] : argv[++i]);
                handleOutputStream(&appinfo.output, temp, STDOUT_FILENO);
                break;
            case 'q':
                appinfo.omitData = 1;
                break;
            case 'c':
                appinfo.useCDATA = 1;
                break;
            case 'R':
                if (!argv[i][2] && argc <= i+1) {
                    fprintf(stderr, "ERROR: -R argument missing\n");
                    return 127;
                }
                appinfo.sitehandle = noquote(argv[i][2] ? &argv[i][2] : argv[++i]);
                break;
            case 'S':
                if (!argv[i][2] && argc <= i+1) {
                    fprintf(stderr, "ERROR: -S argument missing\n");
                    return 127;
                }
                temp = argv[i][2] ? &argv[i][2] : argv[++i];
                if (temp[0] == '@') {
                    /* list-of-filenames file */
                    if ((result=mylist_fill(&initial, temp+1))) {
                        fprintf(stderr, "ERROR: initial %s: %d: %s\n",
                                temp+1, result, strerror(result));
                    }
                } else {
                    /* direct filename */
                    if ((result=mylist_add(&initial, temp))) {
                        fprintf(stderr, "ERROR: initial %s: %d: %s\n",
                                temp, result, strerror(result));
                    }
                }
                break;
            case 's':
                if (!argv[i][2] && argc <= i+1) {
                    fprintf(stderr, "ERROR: -s argument missing\n");
                    return 127;
                }
                temp = argv[i][2] ? &argv[i][2] : argv[++i];
                if (temp[0] == '@') {
                    /* list-of-filenames file */
                    if ((result=mylist_fill(&final, temp+1))) {
                        fprintf(stderr, "ERROR: final %s: %d: %s\n",
                                temp+1, result, strerror(result));
                    }
                } else {
                    /* direct filename */
                    if ((result=mylist_add(&final, temp))) {
                        fprintf(stderr, "ERROR: final %s: %d: %s\n",
                                temp, result, strerror(result));
                    }
                }
                break;
            case 'T':
                if (!argv[i][2] && argc <= i+1) {
                    fprintf(stderr, "ERROR: -T argument missing\n");
                    return 127;
                }
                appinfo.wf_stamp = noquote(argv[i][2] ? &argv[i][2] : argv[++i]);
                break;
            case 'w':
                if (!argv[i][2] && argc <= i+1) {
                    fprintf(stderr, "ERROR: -w argument missing\n");
                    return 127;
                }
                workdir = noquote(argv[i][2] ? &argv[i][2] : argv[++i]);
                createDir = 0;
                break;
            case 'W':
                if (!argv[i][2] && argc <= i+1) {
                    fprintf(stderr, "ERROR: -W argument missing\n");
                    return 127;
                }
                workdir = noquote(argv[i][2] ? &argv[i][2] : argv[++i]);
                createDir = 1;
                break;
            case 'X':
                make_application_executable++;
                break;
            case 'k':
                if (!argv[i][2] && argc <= i+1) {
                    fprintf(stderr, "ERROR: -k argument missing\n");
                    return 127;
                }

                temp = argv[i][2] ? &argv[i][2] : argv[++i];
                end = temp;
                int k = strtoul(temp, &end, 0);
                if (k < 0 || *end != '\0') {
                    fprintf(stderr, "ERROR: Invalid -k argument: %s\n", temp);
                    return 127;
                }

                appinfo.termTimeout = k;

                break;
            case 'K':
                if (!argv[i][2] && argc <= i+1) {
                    fprintf(stderr, "ERROR: -K argument missing\n");
                    return 127;
                }

                temp = argv[i][2] ? &argv[i][2] : argv[++i];
                end = temp;
                int K = strtoul(temp, &end, 0);
                if (K < 1 || *end != '\0') {
                    fprintf(stderr, "ERROR: Invalid -K argument: %s\n", temp);
                    return 127;
                }

                appinfo.killTimeout = K;

                break;
                /* NOTE: If you add another flag to kickstart, please update
                 * the argument skipping logic in
                 * pegasus-cluster/report.c:find_application()
                 */
            case '-':
                keeploop = 0;
                break;
#ifdef LINUX
#ifdef HAS_PTRACE
            case 't':
                appinfo.enableTracing++;
                break;
            case 'z':
                appinfo.enableTracing++;
                appinfo.enableSysTrace++;
                break;
#endif
            case 'Z':
                appinfo.enableLibTrace++;
                break;
            case 'm':
                if (!argv[i][2] && argc <= i+1) {
                    fprintf(stderr, "ERROR: -m argument missing\n");
                    return 127;
                }

                temp = argv[i][2] ? &argv[i][2] : argv[++i];
                end = temp;
                monitoringInterval = strtoul(temp, &end, 0);
                if (monitoringInterval < 0 || monitoringInterval > 3600 || *end != '\0') {
                    fprintf(stderr, "ERROR: Invalid -m argument (0 to 3600 seconds): %s\n", temp);
                    return 127;
                }
                setenv("KICKSTART_MON_INTERVAL", temp, 1);

                break;
#endif
            default:
                i -= 1;
                keeploop = 0;
                break;
        }
    }

    if (argc-i <= 0) {
        /* there is no application to run */
        helpMe(&appinfo);
    }

    /* initialize app info and register CLI parameters with it */
    initJobInfo(&appinfo.application, argc-i, argv+i, getenv("KICKSTART_WRAPPER"));

    /* is there really something to run? */
    if (appinfo.application.isValid != 1) {
        printerr("FATAL: Unable to execute the specified binary. It might not exist,"
                 " have the correct permissions, or be of the right type for the"
                 " current system.\n");
        return 127;
    }

    /* make/change into new workdir NOW */
REDIR:
    if (workdir != NULL && chdir(workdir) != 0) {
        /* shall we try to make the directory */
        if (createDir) {
            createDir = 0; /* once only */

            if (mkdir(workdir, 0777) == 0) {
                /* If this causes an infinite loop, your file-system is
                 * seriously whacked out -- run fsck or equivalent. */
                goto REDIR;
            }

            appinfo.application.saverr = errno;
            printerr("Unable to mkdir %s: %d: %s\n",
                     workdir, errno, strerror(errno));
            appinfo.application.prefix = "Unable to mkdir: ";
            appinfo.application.status = -1;
            return 127;
        }

        /* unable to use alternate workdir */
        appinfo.application.saverr = errno;
        printerr("Unable to chdir %s: %d: %s\n",
                 workdir, errno, strerror(errno));
        appinfo.application.prefix = "Unable to chdir: ";
        appinfo.application.status = -1;
        return 127;
    }

    /* record the current working directory */
    appinfo.workdir = calloc(cwd_size, sizeof(char));
    if (appinfo.workdir == NULL) {
        printerr("calloc: %s\n", strerror(errno));
        return 127;
    }

    if (getcwd(appinfo.workdir, cwd_size) == NULL && errno == ERANGE) {
        /* error allocating sufficient space */
        free((void*) appinfo.workdir);
        appinfo.workdir = NULL;
    }

    /* update stdio and logfile *AFTER* we arrived in working directory */
    updateStatInfo(&appinfo.input);
    updateStatInfo(&appinfo.output);
    updateStatInfo(&appinfo.error);
    updateStatInfo(&appinfo.logfile);

    /* stat pre files */
    appinfo.initial = initStatFromList(&initial, &appinfo.icount);
    mylist_done(&initial);

    /* If there is a timeout, then set the alarm and a handler to kill the job */
    if (appinfo.termTimeout > 0) {
        struct sigaction handler;
        handler.sa_handler = on_alarm;
        handler.sa_flags = 0;
        sigemptyset(&handler.sa_mask);
        if (sigaction(SIGALRM, &handler, NULL) < 0) {
            printerr("Unable to set handler for SIGALRM: %s", strerror(errno));
            return 127;
        }

        alarm(appinfo.termTimeout);
    }

    /* Set job attributes in environment for monitoring */
    if (appinfo.wf_label != NULL) {
        if (setenv("PEGASUS_WF_LABEL", appinfo.wf_label, 1)) {
            fprintf(stderr, "ERROR: Couldn't set PEGASUS_WF_LABEL environment variable\n");
            return 127;
        }
    }
    if (appinfo.xformation != NULL) {
        if (setenv("PEGASUS_XFORMATION", appinfo.xformation, 1)) {
            fprintf(stderr, "ERROR: Couldn't set PEGASUS_XFORMATION environment variable\n");
            return 127;
        }
    }
    if (appinfo.derivation != NULL) {
        if (setenv("PEGASUS_TASK_ID", appinfo.derivation, 1)) {
            fprintf(stderr, "ERROR: Couldn't set PEGASUS_TASK_ID environment variable\n");
            return 127;
        }
    }

#ifdef LINUX
    /* If monitoring is enabled, create new PG and start monitoring thread */
    if (monitoringInterval > 0) {
        if (start_monitoring_thread()) {
            printerr("ERROR: Unable to start monitoring thread\n");
            return 127;
        }
    }
#endif

    /* Our own initially: an independent setup job */
    char *SETUP = getenv("KICKSTART_SETUP");
    if (SETUP == NULL) { SETUP = getenv("GRIDSTART_SETUP"); }
    if (prepareSideJob(&appinfo.setup, SETUP)) {
        mysystem(&appinfo, &appinfo.setup);
    }

    /* possible pre job (skipped if timeout happens) */
    if (result == 0 && alarmed == 0) {
        char *PREJOB = getenv("KICKSTART_PREJOB");
        if (PREJOB == NULL) { PREJOB = getenv("GRIDSTART_PREJOB"); }
        if (prepareSideJob(&appinfo.prejob, PREJOB)) {
            /* there is a prejob to be executed */
            status = mysystem(&appinfo, &appinfo.prejob);
            result = obtainStatusCode(status);
        }
    }

    /* start main application (skipped if timeout happens) */
    if (result == 0 && alarmed == 0) {
        status = mysystem(&appinfo, &appinfo.application);
        result = obtainStatusCode(status);
    } else {
        /* actively invalidate main record */
        appinfo.application.isValid = 0;
    }

    /* possible post job (skipped if the timeout happens) */
    if (result == 0 && alarmed == 0) {
        char *POSTJOB = getenv("KICKSTART_POSTJOB");
        if (POSTJOB == NULL) { POSTJOB = getenv("GRIDSTART_POSTJOB"); }
        if (prepareSideJob(&appinfo.postjob, POSTJOB)) {
            status = mysystem(&appinfo, &appinfo.postjob);
            result = obtainStatusCode(status);
        }
    }

    /* Reset alarm here so that the cleanup job does not get killed */
    alarm(0);

    /* An independent clean-up job that runs regardless of main application result or timeout */
    char *CLEANUP = getenv("KICKSTART_CLEANUP");
    if (CLEANUP == NULL) { CLEANUP = getenv("GRIDSTART_CLEANUP"); }
    if (prepareSideJob(&appinfo.cleanup, CLEANUP)) {
        mysystem(&appinfo, &appinfo.cleanup);
    }

    /* stat post files */
    appinfo.final = initStatFromList(&final, &appinfo.fcount);
    mylist_done(&final);

    /* If the timeout occurred, then set the result to SIGALRM */
    if (alarmed) {
        result = SIGALRM;
    }

#ifdef LINUX
    /* If monitoring, stop monitoring thead */
    if (monitoringInterval > 0) {
        if (stop_monitoring_thread()) {
            printerr("WARNING: Unable to stop monitoring thread\n");
        }
    }
#endif

    appinfo.status = result;

    /* append results to log file */
    printAppInfo(&appinfo);

    /* clean up and close FDs */
    skip_atexit = 1;
    deleteAppInfo(&appinfo);

    return result;
}

