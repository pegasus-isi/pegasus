#ifndef WFPROF_SYSCALL_H
#define WFPROF_SYSCALL_H

#include "ptrace.h"

#ifdef HAS_PTRACE

#include "procinfo.h"

/* Macros to inspect user_regs_struct */
#ifdef __i386__
  #define SC_NR(r)    r.orig_eax
  #define SC_ARG0(r)  r.ebx
  #define SC_ARG1(r)  r.ecx
  #define SC_ARG2(r)  r.edx
  #define SC_ARG3(r)  r.esi
  #define SC_ARG4(r)  r.edi
  #define SC_ARG5(r)  r.ebp
  #define SC_RVAL(r)  r.eax
  #define MAX_SYSCALL 332
#else
  #define SC_NR(r)    r.orig_rax
  #define SC_ARG0(r)  r.rdi
  #define SC_ARG1(r)  r.rsi
  #define SC_ARG2(r)  r.rdx
  #define SC_ARG3(r)  r.rcx
  #define SC_ARG4(r)  r.r8
  #define SC_ARG5(r)  r.r9
  #define SC_RVAL(r)  r.rax
  #define MAX_SYSCALL 294
#endif

struct syscallent {
    const char *name;
    int (*handler)(ProcInfo *c);
};

extern const struct syscallent syscalls[];

int initFileInfo(ProcInfo *c);
int finiFileInfo(ProcInfo *c);

#define handle_none 0

// TODO Implement all of these handlers 
//int handle_open(ProcInfo *c);
//int handle_openat(ProcInfo *c);
//int handle_creat(ProcInfo *c);
//int handle_close(ProcInfo *c);
//int handle_read(ProcInfo *c);
//int handle_write(ProcInfo *c);
//int handle_lseek(ProcInfo *c);
//int handle_dup(ProcInfo *c);
//int handle_dup2(ProcInfo *c);
//int handle_pipe(ProcInfo *c);
//int handle_fcntl(ProcInfo *c); /* only F_DUPFD */
//int handle_readv(ProcInfo *c);
//int handle_writev(ProcInfo *c);
//int handle_pread64(ProcInfo *c);
//int handle_pwrite64(ProcInfo *c);
//int handle_mq_open(ProcInfo *c);
//int handle_sendfile(ProcInfo *c);
//int handle_epoll_create(ProcInfo *c); /* since linux 2.5.44 */
//int handle_signalfd(ProcInfo *c); /* since linux 2.6.22 */
//int handle_eventfd(ProcInfo *c); /* since linux 2.6.22 */
//int handle_timerfd_create(ProcInfo *c); /* since linux 2.6.25 */

#ifdef __i386__
//int handle_socketcall(ProcInfo *c);
//int handle__llseek(ProcInfo *c); /* same as lseek64? */
//int handle_pread(ProcInfo *c);
//int handle_pwrite(ProcInfo *c);
//int handle_fcntl64(ProcInfo *c);
//int handle_sendfile64(ProcInfo *c);
#endif

#ifdef __amd64__
//int handle_socket(ProcInfo *c);
//int handle_accept(ProcInfo *c);
//int handle_socketpair(ProcInfo *c);
#endif

#endif /* HAS_PTRACE */

#endif /* WFPROF_SYSCALL_H */
