#include <stdio.h>

#include "syscall.h"

/* On i386 we have to do something special because there
 * is only one system call, socketcall, for sockets.
 */
#ifdef __i386__
# include <linux/net.h> /* for SYS_SOCKET, etc subcalls */
#endif

/* System call handler table */
const struct syscallent syscalls[] = {
#ifdef __i386__
# include "syscall_32.h"
#else
# include "syscall_64.h"
#endif
};


int handle_open(child_t *c) {
    fprintf(stderr, "PID %d: open %ld\n", c->pid, c->sc_rval);
    return 0;
}

int handle_close(child_t *c) {
    fprintf(stderr, "PID %d: close %ld = %ld\n", c->pid, c->sc_args[0], c->sc_rval);
    return 0;
}

int handle_read(child_t *c) {
    fprintf(stderr, "PID %d: read %ld = %ld\n", c->pid, c->sc_args[0], c->sc_rval);
    return 0;
}

int handle_write(child_t *c) {
    fprintf(stderr, "write\n");
    return 0;
}
