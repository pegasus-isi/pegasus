#ifndef WFPROF_SYSCALL_H
#define WFPROF_SYSCALL_H

#include "child.h"

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
#else
  #define SC_NR(r)    r.orig_rax
  #define SC_ARG0(r)  r.rdi
  #define SC_ARG1(r)  r.rsi
  #define SC_ARG2(r)  r.rdx
  #define SC_ARG3(r)  r.rcx
  #define SC_ARG4(r)  r.r8
  #define SC_ARG5(r)  r.r9
  #define SC_RVAL(r)  r.rax
#endif

struct syscallent {
    const char *name;
    int (*handler)(child_t *c);
};

extern const struct syscallent syscalls[];

#define handle_none 0

int handle_open(child_t *c);
int handle_openat(child_t *c);
int handle_creat(child_t *c);
int handle_close(child_t *c);
int handle_read(child_t *c);
int handle_write(child_t *c);
int handle_lseek(child_t *c);
int handle_dup(child_t *c);
int handle_dup2(child_t *c);
int handle_pipe(child_t *c);
int handle_fcntl(child_t *c); /* only F_DUPFD */
int handle_readv(child_t *c);
int handle_writev(child_t *c);
int handle_pread64(child_t *c);
int handle_pwrite64(child_t *c);
int handle_mq_open(child_t *c);
int handle_sendfile(child_t *c);
int handle_epoll_create(child_t *c); /* since linux 2.5.44 */
int handle_signalfd(child_t *c); /* since linux 2.6.22 */
int handle_eventfd(child_t *c); /* since linux 2.6.22 */
int handle_timerfd_create(child_t *c); /* since linux 2.6.25 */

#ifdef __i386__
int handle_socketcall(child_t *c);
int handle__llseek(child_t *c); /* same as lseek64? */
int handle_pread(child_t *c);
int handle_pwrite(child_t *c);
int handle_fcntl64(child_t *c);
int handle_sendfile64(child_t *c);
#endif

#ifdef __amd64__
int handle_socket(child_t *c);
int handle_accept(child_t *c);
int handle_socketpair(child_t *c);
#endif

#endif /* WFPROF_SYSCALL_H */
