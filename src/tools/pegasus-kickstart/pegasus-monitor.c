#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <sys/wait.h>
#include <unistd.h>
#include <string.h>

#include "monitoring.h"
#include "log.h"

int main(int argc, char **argv) {
    log_set_name("pegasus-monitor");
    log_set_default_level();

    /* Check arguments */
    if (argc < 2) {
        fprintf(stderr, "Usage: %s application [args...]\n", argv[0]);
        return 1;
    }

    start_monitoring_thread();

    /* Launch application process */
    pid_t pid = fork();
    if (pid < 0) {
        fprintf(stderr, "Error forking process: %s\n", strerror(errno));
        return 1;
    } else if (pid == 0) {
        execvp(argv[1], &argv[1]);
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
