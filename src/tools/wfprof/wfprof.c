/*
 * Copyright 2009 University Of Southern California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/* This is a kickstart-like wrapper that collects memory and CPU usage info
 * about a transformation and all of its child processes. Each child (and
 * grandchild) is traced using ptrace. When the child is about to exit the
 * tracing proces (this process) looks it up in the /proc file system and
 * determines: what the maximum virtual memory size was (vmpeak), what the
 * maximum physical memory size was (rsspeak), how much time the process
 * spent in the kernel (stime), how much time the process spent in user
 * mode (utime) and how much wall-clock time elapsed between when the
 * process was launched and when it exited (wtime). The data is written to 
 * stderr (because kickstart writes everything to stdout) and the child is 
 * allowed to exit.
 *
 * NOTE:
 * This wrapper won't work if the transformation requires any executable to 
 * be notified when one of its children stops (i.e. some process needs to
 * wait() for a child to get a SIGSTOP and then deliver a SIGCONT). See also
 * the man page for ptrace().
 */

#include <sys/ptrace.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <sys/user.h> /* struct user_regs_struct */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

/* Check kernel version */
#include <linux/version.h>
#if LINUX_VERSION_CODE < KERNEL_VERSION(2,5,46)
    #error "Linux 2.5.46 or greater is required"
#endif

/* Prior to version 2.5 glibc did not have these */
#if __GLIBC__ < 2 || (__GLIBC__ == 2 && __GLIBC_MINOR__ < 5)
    /* 0x4200-0x4300 are reserved for architecture-independent additions.  */
    #define PTRACE_SETOPTIONS       0x4200
    #define PTRACE_GETEVENTMSG      0x4201
    #define PTRACE_GETSIGINFO       0x4202
    #define PTRACE_SETSIGINFO       0x4203
#endif

/* Prior to version 2.7 glibc did not have these */
#if __GLIBC__ < 2 || (__GLIBC__ == 2 && __GLIBC_MINOR__ < 7)
    /* options set using PTRACE_SETOPTIONS */
    #define PTRACE_O_TRACESYSGOOD   0x00000001
    #define PTRACE_O_TRACEFORK      0x00000002
    #define PTRACE_O_TRACEVFORK     0x00000004
    #define PTRACE_O_TRACECLONE     0x00000008
    #define PTRACE_O_TRACEEXEC      0x00000010
    #define PTRACE_O_TRACEVFORKDONE 0x00000020
    #define PTRACE_O_TRACEEXIT      0x00000040
    #define PTRACE_O_MASK           0x0000007f

    /* Wait extended result codes for the above trace options.  */
    #define PTRACE_EVENT_FORK       1
    #define PTRACE_EVENT_VFORK      2
    #define PTRACE_EVENT_CLONE      3
    #define PTRACE_EVENT_EXEC       4
    #define PTRACE_EVENT_VFORK_DONE 5
    #define PTRACE_EVENT_EXIT       6
#endif

#include "child.h"
#include "syscall.h"

char XFORM[BUFSIZ] = "none";

double get_time() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec + ((double)tv.tv_usec / 1e6);
}

void print_header() {
    fprintf(stderr,
        "xform pid ppid exe lstart lstop tstart tstop vmpeak rsspeak utime stime wtime cutime cstime\n");
}

void print_report(child_t *item) {
    /* Don't report threads */
    if (item->tgid != item->pid)
        return;

    fprintf(stderr, 
        "%s %d %d %s %d %d %lf %lf %d %d %lf %lf %lf %lf %lf\n", 
	XFORM,
        item->pid, 
	item->ppid,
	item->exe, 
	item->lstart,
	item->lstop,
	item->tstart,
	item->tstop,
        item->vmpeak,
        item->rsspeak,
	item->utime,
	item->stime,
	item->tstop - item->tstart,
        item->cutime,
        item->cstime);
}

int main(int argc, char **argv) {
    pid_t cpid;
    child_t *child;
    int i, status, event, lclock;
    char *PEGASUS_HOME;
    char kickstart[BUFSIZ];
    struct user_regs_struct regs;

    /* check for kickstart in local dir */
    sprintf(kickstart, "./kickstart");
    if (access(kickstart, X_OK) < 0) {

        /* check for PEGASUS_HOME env var */
        PEGASUS_HOME = getenv("PEGASUS_HOME");
        if (PEGASUS_HOME == NULL) {
            fprintf(stderr, "Please set PEGASUS_HOME\n");
            exit(1);
	}

        /* check for kickstart in $PEGASUS_HOME/bin */
    	sprintf(kickstart, "%s/bin/kickstart", PEGASUS_HOME);
    	if (access(kickstart, X_OK) < 0) {
    	    fprintf(stderr, "cannot execute kickstart: %s\n", kickstart);
    	    exit(1);
    	}
    }

    /* Get transformation name if possible */
    for (i=0; i<argc; i++) {
        if (strcmp(argv[i], "-n") == 0) {
	    strcpy(XFORM, argv[i+1]);
	    break;
	}
    }

    /* Fork kickstart */
    cpid = fork();
    if (cpid < 0) {
        perror("fork");
        exit(1);
    }
    else if(cpid == 0) {
        if (ptrace(PTRACE_TRACEME, 0, NULL, NULL) < 0) {
            perror("PTRACE_TRACEME");
            exit(1);
        }
	dup2(1, 2); /* redirect stderr to stdout */
	argv[0] = "kickstart";
        execv(kickstart, argv);
        _exit(0);
    }
    else {

        /* initialize logical clock */
	lclock = 0;

        print_header();

        while (1) {
	    /* __WALL is needed so that we can wait on threads too */
	    cpid = waitpid(0, &status, __WALL);

            /* find the child */
	    child = find_child(cpid);

	    /* if not found, then it is new, so add it */
	    if (child == NULL) {
	        child = add_child(cpid);
		child->tstart = get_time();
		child->lstart = lclock++;
                if (ptrace(PTRACE_SETOPTIONS, cpid, NULL, 
                           PTRACE_O_TRACESYSGOOD|PTRACE_O_TRACEEXIT|
			   PTRACE_O_TRACEFORK|PTRACE_O_TRACEVFORK|
			   PTRACE_O_TRACECLONE)) {
		    perror("PTRACE_SETOPTIONS");
                    exit(1);
                }
	    }

            /* child exited */
            if (WIFEXITED(status)) {
		remove_child(cpid);
		if (no_children()) break;
            }

	    /* child was stopped */
	    if (WIFSTOPPED(status)) {

	        /* Because of a special event we wanted to see */
	        if(WSTOPSIG(status) == SIGTRAP) {
                    event = status >> 16;
	            if (event == PTRACE_EVENT_EXIT) {
		        child->tstop = get_time();
		        child->lstop = lclock++;

		        /* fill in exe name */
		        if (read_exeinfo(child) < 0) {
		            perror("read_exeinfo");
			    exit(1);
		        }

		        /* fill in memory info */
		        if (read_meminfo(child) < 0) {
			    perror("read_meminfo");
			    exit(1);
			}

                        /* fill in stat info */
			if (read_statinfo(child) < 0) {
			    perror("read_statinfo");
			    exit(1);
			}

		        /* print stats */
		        print_report(child);
		    }

		    if (ptrace(PTRACE_SYSCALL, cpid, NULL, NULL)) {
		        perror("PTRACE_SYSCALL event");
                         exit(1);
		    }
                } 
		
		/* Because of a system call */
		else if(WSTOPSIG(status) == (SIGTRAP|0x80)) {
		    if (ptrace(PTRACE_GETREGS, cpid, NULL, &regs)) {
		        perror("PTRACE_GETREGS");
			exit(1);
		    }

		    if (child->insyscall) {
			child->sc_rval = SC_RVAL(regs);
			int (*handler)(child_t *c) = syscalls[child->sc_nr].handler;
			if (handler) handler(child);
		        child->insyscall = 0;
		    } else {
		        child->sc_nr = SC_NR(regs);
			child->sc_args[0] = SC_ARG0(regs);
			child->sc_args[1] = SC_ARG1(regs);
			child->sc_args[2] = SC_ARG2(regs);
			child->sc_args[3] = SC_ARG3(regs);
			child->sc_args[4] = SC_ARG4(regs);
			child->sc_args[5] = SC_ARG5(regs);
			child->insyscall = 1;
		    }

                    if (ptrace(PTRACE_SYSCALL, cpid, NULL, NULL)) {
                        perror("PTRACE_SYSCALL syscall");
                        exit(1);
                    }
		} 
		
		/* Because it got a signal */
	        else {
                    /* pass the signal on to the child */
                    if (ptrace(PTRACE_SYSCALL, cpid, 0, WSTOPSIG(status))) {
                        perror("PTRACE_SYSCALL signal");
                        exit(1);
                    }
		}
            } 
        }
    }

    return 0;
}
