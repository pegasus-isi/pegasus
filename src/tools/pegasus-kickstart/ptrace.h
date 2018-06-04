#ifndef _PTRACE_H
#define _PTRACE_H

#ifdef LINUX

#include <linux/version.h>

/* do not enable ptrace on arm */
#ifndef __arm__

/* The ptrace options we need were not all available until 2.5.60 */
#if LINUX_VERSION_CODE >= KERNEL_VERSION(2,5,60)

#include <sys/ptrace.h>
#define HAS_PTRACE

#ifdef __GLIBC__

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

#endif /* glibc */

#endif /* Linux >= version */

#endif /* arm */

#endif /* Linux */


#endif /* _PTRACE_H */
