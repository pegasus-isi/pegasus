#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <sys/wait.h>
#include <unistd.h>
#include <string.h>

#include "monitoring.h"
#include "log.h"

void usage(char *argv) {
    fprintf(stderr, "Usage: %s [-i interval] application [args...]\n", argv);
    exit(1);
}

int main(int argc, char **argv) {
    log_set_name("pegasus-monitor");
    log_set_default_level();

    /* Check arguments */
    if (argc < 2) {
        usage(argv[0]);
    }

    /* Did they specify the interval? */
    int argoffset = 1;
    if (argv[1][0] == '-') {
        /* Check for common issues */
        if (argv[1][1] != 'i' || argc < 4) {
            usage(argv[0]);
        }
        setenv("KICKSTART_MON_INTERVAL", argv[2], 1);
        argoffset = 3;
    }

    start_monitoring_thread();

    /* Launch application process */
    pid_t pid = fork();
    if (pid < 0) {
        fprintf(stderr, "Error forking process: %s\n", strerror(errno));
        return 1;
    } else if (pid == 0) {
        execvp(argv[argoffset], &argv[argoffset]);
        fprintf(stderr, "execvp: %s\n", strerror(errno));
        _exit(1);
    }

    /* Wait for subprocess */
    int status = 0;
    if (waitpid(pid, &status, 0) < 0) {
        fprintf(stderr, "waitpid: %s\n", strerror(errno));
        return 1;
    }

    /* Terminate monitoring thread */
    stop_monitoring_thread();

    /* Return application status */
    return WEXITSTATUS(status);
}
