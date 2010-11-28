#ifndef WFPROF_CHILD_H
#define WFPROF_CHILD_H

#include <sys/types.h> /* pid_t */

#define MAX_NAME 1024
#define SC_ARGS 6

typedef struct _child_t {
  pid_t pid; 		/* child's process ID */
  pid_t ppid;		/* parent process ID */
  pid_t tgid;		/* thread group ID */
  char exe[MAX_NAME];	/* exe name */
  int lstart;		/* logical clock start time */
  int lstop;		/* logical clock stop time */
  int vmpeak;		/* peak virtual memory usage */
  int rsspeak;		/* peak physical memory usage */
  double utime;		/* time spent in user mode */
  double stime;		/* time spent in kernel mode */
  double cutime;	/* time waited-on children were in user mode */
  double cstime;	/* time waited-on children were in kernel mode */
  double tstart;	/* start time (seconds from epoch) */
  double tstop;		/* stop time (seconds from epoch) */
  int insyscall;	/* in a system call? */
  int sc_nr;		/* system call number */
  long sc_args[SC_ARGS];/* system call arguments */
  long sc_rval;		/* system call return value */

  struct _child_t *next;
  struct _child_t *prev;
} child_t;

child_t *find_child(pid_t pid);
child_t *add_child(pid_t pid);
void remove_child(pid_t pid);
int no_children();

int read_exeinfo(child_t *c);
int read_meminfo(child_t *c);
int read_statinfo(child_t *c);

#endif /* WFPROF_CHILD_H */
