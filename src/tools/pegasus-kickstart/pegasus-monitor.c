#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <sys/wait.h>
#include <unistd.h>
#include <string.h>
#include <getopt.h>

#include "log.h"

#include "monitoring_helpers.h"
#include "monitoring.h"

#ifdef HAS_CUDA
#include "nvidia_monitoring.h"
#endif

void usage(char *argv) {
#ifdef HAS_CUDA
    fprintf(stderr, "Usage: %s [-g] [-i interval] application [args...]\n", argv);
#else
    fprintf(stderr, "Usage: %s [-i interval] application [args...]\n", argv);
#endif
    exit(1);
}

int main(int argc, char **argv) {
    static int gpu_monitoring = 0;
    log_set_name("pegasus-monitor");
    log_set_default_level();

    int opt;
    int option_index;
#ifdef HAS_CUDA
    static struct option long_options[] = {
        {"gpu", 0, &gpu_monitoring, 1},
        {"interval", 1, 0, 'i'},
        {"help", 0, 0, 'h'},
        {0, 0, 0, 0}
    };
    
    while((opt = getopt_long(argc, argv, ":i:gh", long_options, &option_index)) != -1) { 
        switch(opt) {
            case 0:
                break;
            case 'i':
                setenv("KICKSTART_MON_INTERVAL", optarg, 1);
                break;
            case 'g':
                gpu_monitoring = 1;
                break;
            case 'h': 
                usage(argv[0]);
                break;
            case ':': 
                fprintf(stderr, "Option '%c' needs a value\n", optopt); 
                usage(argv[0]);
                break;
            case '?': 
                fprintf(stderr, "Unknown option: %c\n", optopt);
                usage(argv[0]);
        }
    }
#else
    static struct option long_options[] = {
        {"gpu", 0, &gpu_monitoring, 1},
        {"interval", 1, 0, 'i'},
        {"help", 0, 0, 'h'},
        {0, 0, 0, 0}
    };
    
    while((opt = getopt_long(argc, argv, ":i:h", long_options, &option_index)) != -1) { 
        switch(opt) {
            case 0:
                break;
            case 'i':
                setenv("KICKSTART_MON_INTERVAL", optarg, 1);
                break;
            case 'h': 
                usage(argv[0]);
                break;
            case ':': 
                fprintf(stderr, "Option '%c' needs a value\n", optopt); 
                usage(argv[0]);
                break;
            case '?': 
                fprintf(stderr, "Unknown option: %c\n", optopt);
                usage(argv[0]);
        }
    }
#endif

    /*
    // Check arguments
    if (argc < 2) {
        usage(argv[0]);
    }

    // Did they specify the interval?
    int argoffset = 1;
    if (argv[1][0] == '-') {
        // Check for common issues
        if (argv[1][1] != 'i' || argc < 4) {
            usage(argv[0]);
        }
        setenv("KICKSTART_MON_INTERVAL", argv[2], 1);
        argoffset = 3;
    }*/

    
#ifdef HAS_CUDA
    /* Replace this with gpu_monitoring for amd gpus too */
    if (gpu_monitoring || getenv("PEGASUS_GPUS")) {
        gpu_monitoring = 1;
        start_nvidia_monitoring_thread();
    }
#endif

    start_monitoring_thread();

    /* Launch application process */
    pid_t pid = fork();
    if (pid < 0) {
        fprintf(stderr, "Error forking process: %s\n", strerror(errno));
        return 1;
    } else if (pid == 0) {
        execvp(argv[optind], &argv[optind]);
        fprintf(stderr, "execvp: %s\n", strerror(errno));
        _exit(1);
    }

    /* Wait for subprocess */
    int status = 0;
    if (waitpid(pid, &status, 0) < 0) {
        fprintf(stderr, "waitpid: %s\n", strerror(errno));
        return 1;
    }

#ifdef HAS_CUDA
    if (gpu_monitoring)
        stop_nvidia_monitoring_thread();
#endif

    /* Terminate monitoring thread */
    stop_monitoring_thread();

    /* Return application status */
    return WEXITSTATUS(status);
}
