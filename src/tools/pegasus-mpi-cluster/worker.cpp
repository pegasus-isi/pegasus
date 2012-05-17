#include <errno.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/wait.h>
#include <sys/time.h>
#include <fcntl.h>
#include <signal.h>
#include <mpi.h>

#include "strlib.h"
#include "worker.h"
#include "protocol.h"
#include "log.h"
#include "failure.h"
#include "tools.h"

Worker::Worker(const std::string &host_script, unsigned host_memory) {
    this->host_script = host_script;
    this->host_memory = host_memory;
    this->host_script_pid = 0;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    get_host_name(host_name);
}

Worker::~Worker() {
}

/**
 * Launch the host script if a) this worker has host rank 0, and 
 * b) the host script is valid 
 */
void Worker::launch_host_script() {
    // Only launch it if it exists
    if (host_script == "")
        return;
    
    // Only host_rank 0 launches a script
    if (host_rank > 0)
        return;
    
    log_info("Worker %d: Launching host script %s", rank, host_script.c_str());
    
    pid_t pid = fork();
    if (pid < 0) {
        myfailures("Worker %d: Unable to fork host script", rank); 
    } else if (pid == 0) {
        // Redirect stdout to stderr
        if (dup2(STDERR_FILENO, STDOUT_FILENO) < 0) {
            fprintf(stderr,
                "Unable to redirect host script stdout to stderr: %s\n", 
                strerror(errno));
            exit(1);
        }
        
        // Create a new process group so we can kill it later if
        // it runs longer than the workflow
        if (setpgid(0, 0) < 0) {
            fprintf(stderr, 
                "Unable to set process group in host script: %s\n", 
                strerror(errno));
            exit(1);
        }
        
        // Close any other open descriptors. This will not really close
        // everything, but it is unlikely that we will have more than a 
        // few descriptors open. It depends on MPI.
        for (int i=3; i<32; i++) {
            close(i);
        }
        
        // Exec process
        char *argv[2] = {
            (char *)host_script.c_str(),
            NULL
        };
        execvp(argv[0], argv);
        fprintf(stderr, "Unable to exec host script: %s\n", strerror(errno));
        exit(1);
    } else {
        
        // Also set process group here to avoid potential races. It is 
        // possible that we might try to call killpg() before the child
        // has a chance to run. Calling setpgid() here ensures that the
        // child's process group will always be set before killpg() is 
        // called so that it doesn't fail.
        if (setpgid(pid, pid) < 0) {
            log_error("Unable to set process group for host script: %s", 
                strerror(errno));
        }
        
        host_script_pid = pid;
    } 
}

/**
 * Check to see if the host script exited. If it did, then log the status.
 * If terminate is true, then send SIGTERM to the host script's process group
 * and block until it exits.
 */
void Worker::check_host_script(bool terminate) {
    // Workers with host_rank > 0 will not have host scripts
    if (host_rank > 0)
        return;
    
    // If there is no pid to wait for then skip the check
    if (host_script_pid <= 0)
        return;
    
    int options = WNOHANG;
    
    if (terminate) {
        log_warn("Worker %d: Terminating host script with SIGTERM", rank);
        
        if (killpg(host_script_pid, SIGTERM) < 0) {
            log_error("Worker %d: Error terminating host script process group: %s", 
                rank, strerror(errno));
        }
        
        // If we are killing it we want to wait
        options = 0;
    }
    
    int status = 0;
    pid_t pid = waitpid(host_script_pid, &status, options);
    
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
        host_script_pid = 0;
    }
}

int Worker::run() {
    log_info("Worker %d: Starting...", rank);
    
    // Send worker's hostname
    send_hostname(host_name);
    log_trace("Host name: %s", host_name.c_str());
    
    // Get worker's host rank
    recv_hostrank(host_rank);
    log_trace("Host rank: %d", host_rank);
    
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
        
        // Process arguments
        std::list<std::string> args;
        split_args(args, command);
        
        // Task status is exit code 1 by default
        int status = 256; // 256 is exit code 1
        
        // Get start time
        struct timeval task_start;
        gettimeofday(&task_start, NULL);
        
        pid_t pid = fork();
        if (pid < 0) {
            // Fork failed
            log_error("Unable to fork task %s: %s", 
                name.c_str(), strerror(errno));
        } else if (pid == 0) {
            // In child
            
            // Create argument structure
            unsigned nargs = args.size();
            // N + 1 for null-termination
            char **argv = new char*[nargs+1];
            for (unsigned i=0; i<nargs; i++) {
                std::string arg = args.front();
                args.pop_front();
                argv[i] = new char[arg.size()+1];
                strcpy(argv[i], arg.c_str());
            }
            argv[nargs] = NULL; // Last one is null
            
            // Redirect stdout/stderr
            if (dup2(out, STDOUT_FILENO) < 0) {
                fprintf(stderr, "Error redirecting stdout of task %s: %s\n", 
                    name.c_str(), strerror(errno));
            }
            if (dup2(err, STDERR_FILENO) < 0) {
                fprintf(stderr, "Error redirecting stderr of task %s: %s\n", 
                    name.c_str(), strerror(errno));
            }
            
            // Close any other open descriptors. This will not really close
            // everything, but it is unlikely that we will have more than a 
            // few descriptors open. It depends on MPI.
            for (int i=3; i<32; i++) {
                close(i);
            }
            
            // Exec process
            execvp(argv[0], argv);
            fprintf(stderr, "Unable to exec command for task %s: %s\n", 
                name.c_str(), strerror(errno));
            exit(1);
        } else {
            // Wait for task to complete
            if (waitpid(pid, &status, 0) < 0) {
                log_error("Failed waiting for task: %s", strerror(errno));
            }
        }
        
        // Finish time
        struct timeval task_finish;
        gettimeofday(&task_finish, NULL);
        
        // Elapsed time
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
        std::string app = args.front();
        char summary[BUFSIZ];
        char date[32];
        iso2date(task_stime, date, sizeof(date));
        sprintf(summary, "[cluster-task id=%s, start=\"%s\", duration=%.3f, status=%d, app=\"%s\"]\n",
                     pegasus_id.c_str(), date, task_runtime, status, app.c_str());
        write(out, summary, strlen(summary));
        
        send_response(name, status);
    }

    check_host_script(true);
    
    close(out);
    close(err);
    
    // Send total_runtime
    log_trace("Worker %d: Sending total runtime to master", rank);
    send_total_runtime(total_runtime);
    
    log_info("Worker %d: Exiting...", rank);
    
    return 0;
}
