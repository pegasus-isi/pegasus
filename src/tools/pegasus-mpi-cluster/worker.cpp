#include "strlib.h"
#include "errno.h"
#include "stdlib.h"
#include "unistd.h"
#include "stdio.h"
#include "sys/wait.h"
#include "sys/time.h"
#include "fcntl.h"
#include "mpi.h"

#include "worker.h"
#include "protocol.h"
#include "log.h"
#include "failure.h"
#include "tools.h"

extern char **environ;

Worker::Worker() {
}

Worker::~Worker() {
}

int Worker::run() {
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    
    log_info("Worker %d: Starting...", rank);

    char buf[10000];
    
    std::string outfile;
    std::string errfile;
    
    // Get outfile/errfile
    recv_stdio_paths(outfile, errfile);
    
    // Append rank to outfile/errfile
    char dotrank[10];
    snprintf(dotrank, 10, ".%d", rank);
    outfile += dotrank;
    errfile += dotrank;
    
    log_debug("Worker %d: Using stdout file: %s", rank, outfile.c_str());
    log_debug("Worker %d: Using stderr file: %s", rank, errfile.c_str());
    
    int out = open(outfile.c_str(), O_WRONLY|O_CREAT|O_TRUNC, 0000644);
    if (out < 0) {
        failures("Worker %d: unable to open task stdout", rank);
    }
    
    int err = open(errfile.c_str(), O_WRONLY|O_CREAT|O_TRUNC, 0000644);
    if (err < 0) {
        failures("Worker %d: unable to open task stderr", rank);
    }
    
    double total_runtime = 0.0;

    while (1) {
        std::string name;
        std::string command;
        std::string extra_id;
        
        log_trace("Worker %d: Waiting for request", rank);
        int shutdown = recv_request(name, command, extra_id);
        log_trace("Worker %d: Got request", rank);
        
        if (shutdown) {
            log_trace("Worker %d: Got shutdown message", rank);
            break;
        }
        
        log_debug("Worker %d: Running task %s", rank, name.c_str());

        struct timeval task_start;
        gettimeofday(&task_start, NULL);
            
        // Process arguments
        std::vector<std::string> args;
        split_args(args, command);
        // N + 1 for null-termination
        char **argv = new char*[args.size()+1];
        for (unsigned i=0; i<args.size(); i++) {
            argv[i] = new char[args[i].size()+1];
            strcpy(argv[i], args[i].c_str());
        }
        argv[args.size()] = NULL; // Last one is null
        
        pid_t pid = fork();
        if (pid == 0) {
            
            // Redirect stdout/stderr
            close(STDOUT_FILENO);
            dup2(out, STDOUT_FILENO);
            
            close(STDERR_FILENO);
            dup2(err, STDERR_FILENO);
            
            close(out);
            close(err);
            
            // Exec process
            execve(argv[0], argv, environ);
            fprintf(stderr, "%s: unable to execve command: %s\n", 
                name.c_str(), strerror(errno));
            exit(1);
        }
        
        // Wait for task to complete
        struct rusage usage;
        int exitcode = 1;
        if (wait4(pid, &exitcode, 0, &usage) == -1) {
            log_warn("Failed waiting for task: %s", strerror(errno));
        }
        
        struct timeval task_finish;
        gettimeofday(&task_finish, NULL);

        double task_stime = task_start.tv_sec + (task_start.tv_usec/1000000.0);
        double task_ftime = task_finish.tv_sec + (task_finish.tv_usec/1000000.0);
        double task_runtime = task_ftime - task_stime;

        total_runtime += task_runtime;

        log_debug("Worker %d: Task %s finished with exitcode %d in %f seconds", 
            rank, name.c_str(), exitcode, task_runtime);

        // pegasus cluster output - used for provenance
        char date[32];
        iso2date(task_stime, date, sizeof(date));
        sprintf(buf, "[cluster-task id=%s, start=\"%s\", duration=%.3f, status=%d, app=\"%s\"]\n",
                     extra_id.c_str(), date, task_runtime, exitcode, argv[0]);
        write(out, buf, strlen(buf));
        
        send_response(name, task_stime, task_ftime, exitcode);
    }

    close(out);
    close(err);

    // Send total_runtime
    log_trace("Worker %d: Sending total runtime to master", rank);
    send_total_runtime(total_runtime);
    
    log_info("Worker %d: Exiting...", rank);
    
    return 0;
}
