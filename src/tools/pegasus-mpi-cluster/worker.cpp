#include <errno.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/wait.h>
#include <sys/time.h>
#include <fcntl.h>
#include <signal.h>
#include "mpi.h"

#include "strlib.h"
#include "worker.h"
#include "protocol.h"
#include "log.h"
#include "failure.h"
#include "tools.h"

extern char **environ;

Worker::Worker(const std::string &hostscript) {
    this->hostscript = hostscript;
    this->hostscript_pid = 0;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    get_host_name(hostname);
}

Worker::~Worker() {
}

/**
 * Launch the host script if a) this worker has host rank 0, and 
 * b) the host script is valid 
 */
void Worker::launch_host_script() {
    // Only launch it if it exists
    if (hostscript == "")
        return;
    
    // Only hostrank 0 launches a script
    if (hostrank > 0)
        return;
    
    log_info("Worker %d: Launching host script %s", rank, hostscript.c_str());
    
    pid_t pid = fork();
    if (pid < 0) {
        myfailures("Unable to fork()"); 
    } else if (pid == 0) {
        // Redirect stdout to stderr
        close(STDOUT_FILENO);
        dup2(STDERR_FILENO, STDOUT_FILENO);
        
        // Create a new process group so we can kill it later if
        // it runs longer than the workflow
        if (setpgid(0, 0) < 0) {
            fprintf(stderr, 
                "Unable to set process group for host script: %s\n", 
                strerror(errno));
            exit(1);
        }
        
        // Exec process
        char *argv[2] = {
            (char *)hostscript.c_str(),
            NULL
        };
        execve(hostscript.c_str(), argv, environ);
        fprintf(stderr, "Unable to execve host script: %s\n", strerror(errno));
        exit(1);
    } else {
        hostscript_pid = pid;
    } 
}

/**
 * Check to see if the host script exited. If it did, then log the status.
 * If terminate is true, then send SIGTERM to the host script's process group
 * and block until it exits.
 */
void Worker::check_host_script(bool terminate) {
    // Workers with hostrank > 0 will not have host scripts
    if (hostrank > 0)
        return;
    
    // If there is no pid to wait for then skip the check
    if (hostscript_pid <= 0)
        return;
    
    int options = WNOHANG;
    
    if (terminate) {
        log_warn("Worker %d: Terminating host script with SIGTERM", rank);
        
        if (killpg(hostscript_pid, SIGTERM) < 0) {
            log_error("Worker %d: Error terminating host script process group: %s", 
                rank, strerror(errno));
        }
        
        // If we are killing it we want to wait
        options = 0;
    }
    
    int status = 0;
    pid_t pid = waitpid(hostscript_pid, &status, options);
    
    if (pid < 0) {
        log_error("Worker %d: Error checking host script: %s", rank, strerror(errno));
    } else if (pid > 0) {
        if (WIFEXITED(status)) {
            log_info("Worker %d: Host script exited with status %d (%d)", 
                rank, WEXITSTATUS(status), status);
        } else {
            log_info("Worker %d: Host script exited on signal %d (%d)", 
                rank, WTERMSIG(status), status);
        }
        hostscript_pid = 0;
    }
}

int Worker::run() {
    log_info("Worker %d: Starting...", rank);
    
    // Send worker's hostname
    send_hostname(hostname);
    log_trace("Hostname: %s", hostname.c_str());
    
    // Get worker's host rank
    recv_hostrank(hostrank);
    log_trace("Hostrank: %d", hostrank);
    
    // Get outfile/errfile
    std::string outfile;
    std::string errfile;
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
        myfailures("Worker %d: unable to open task stdout", rank);
    }
    
    int err = open(errfile.c_str(), O_WRONLY|O_CREAT|O_TRUNC, 0000644);
    if (err < 0) {
        myfailures("Worker %d: unable to open task stderr", rank);
    }
    
    launch_host_script();
    
    double total_runtime = 0.0;
    
    while (1) {
        check_host_script(false);
        
        log_trace("Worker %d: Waiting for request", rank);
        std::string name;
        std::string command;
        std::string pegasus_id;
        int shutdown;
        recv_request(name, command, pegasus_id, shutdown);
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
        int status = 0;
        if (wait4(pid, &status, 0, &usage) < 0) {
            log_error("Failed waiting for task: %s", strerror(errno));
        }
        
        struct timeval task_finish;
        gettimeofday(&task_finish, NULL);

        double task_stime = task_start.tv_sec + (task_start.tv_usec/1000000.0);
        double task_ftime = task_finish.tv_sec + (task_finish.tv_usec/1000000.0);
        double task_runtime = task_ftime - task_stime;

        total_runtime += task_runtime;

        if (WIFEXITED(status)) {
            log_debug("Worker %d: Task %s exited with status %d (%d) in %f seconds", 
                rank, name.c_str(), WEXITSTATUS(status), status, task_runtime);
        } else {
            log_debug("Worker %d: Task %s exited on signal %d (%d) in %f seconds", 
                rank, name.c_str(), WTERMSIG(status), status, task_runtime);
        }
        
        // pegasus cluster output - used for provenance
        char summary[BUFSIZ];
        char date[32];
        iso2date(task_stime, date, sizeof(date));
        sprintf(summary, "[cluster-task id=%s, start=\"%s\", duration=%.3f, status=%d, app=\"%s\"]\n",
                     pegasus_id.c_str(), date, task_runtime, status, argv[0]);
        write(out, summary, strlen(summary));
        
        send_response(name, status);
    }

    close(out);
    close(err);
    
    check_host_script(true);
    
    // Send total_runtime
    log_trace("Worker %d: Sending total runtime to master", rank);
    send_total_runtime(total_runtime);
    
    log_info("Worker %d: Exiting...", rank);
    
    return 0;
}
