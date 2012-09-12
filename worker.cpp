#include <errno.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/wait.h>
#include <sys/time.h>
#include <fcntl.h>
#include <signal.h>
#include <math.h>
#include <mpi.h>
#include <sys/resource.h>

#include "strlib.h"
#include "worker.h"
#include "protocol.h"
#include "log.h"
#include "failure.h"
#include "tools.h"

extern char **environ;

static void log_signal(int signo) {
    log_error("Caught signal %d", signo);
}

Pipe::Pipe(std::string forward, int readfd, int writefd) {
    int eq = forward.find("=");
    this->varname = forward.substr(0, eq);
    this->filename = forward.substr(eq + 1);
    this->readfd = readfd;
    this->writefd = writefd;
}

Pipe::~Pipe() {
}

void Pipe::save(char *buf, unsigned n) {
    this->buffer.append(buf, n);
}

const char *Pipe::data() {
    return this->buffer.data();
}

unsigned Pipe::size() {
    return this->buffer.size();
}

void Pipe::close() {
    this->closeread();
    this->closewrite();
}

void Pipe::closeread() {
    if (this->readfd != -1) {
        ::close(this->readfd);
        this->readfd = -1;
    }
}

void Pipe::closewrite() {
    if (this->writefd != -1) {
        ::close(this->writefd);
        this->writefd = -1;
    }
}

Worker::Worker(const std::string &outfile, const std::string &errfile, const std::string &host_script, unsigned int host_memory, unsigned host_cpus, bool strict_limits) {
    this->outfile = outfile;
    this->errfile = errfile;

    this->host_script = host_script;
    if (host_memory == 0) {
        // If host memory is not specified by the user, then get the amount
        // of physical memory on the host and convert it to MB. 1 MB is the 
        // minimum, but that shouldn't ever happen.
        unsigned long bytes = get_host_memory();
        this->host_memory = (unsigned)ceil(bytes / (1024.0*1024.0)); // bytes -> MB
    } else {
        this->host_memory = host_memory;
    }
    if (host_cpus == 0) {
        this->host_cpus = get_host_cpus();
    } else {
        this->host_cpus = host_cpus;
    }
    this->strict_limits = strict_limits;
    this->host_script_pgid = 0;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    get_host_name(host_name);
}

Worker::~Worker() {
}

/**
 * Launch the host script if a) this worker has host rank 0, and 
 * b) the host script is valid 
 */
void Worker::run_host_script() {
    // Only launch it if it exists
    if (host_script == "")
        return;
    
    // Only host_rank 0 launches a script, the others need to wait
    if (host_rank > 0)
        return;
    
    log_info("Worker %d: Launching host script %s", rank, host_script.c_str());
    
    pid_t pid = fork();
    if (pid < 0) {
        myfailures("Worker %d: Unable to fork host script", rank); 
    } else if (pid == 0) {
        // Redirect stdout to stderr
        if (dup2(STDERR_FILENO, STDOUT_FILENO) < 0) {
            log_fatal("Unable to redirect host script stdout to stderr: %s", 
                strerror(errno));
            exit(1);
        }
        
        // Create a new process group so we can kill it later if
        // it runs longer than the workflow
        if (setpgid(0, 0) < 0) {
            log_fatal("Unable to set process group in host script: %s", 
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
        
        // Record the process group id so we can kill it when the workflow finishes
        host_script_pgid = pid;
        
        // Give this process a timeout
        struct sigaction act;
        struct sigaction oact;
        act.sa_handler = log_signal;
        act.sa_flags = SA_NODEFER;
        if (sigaction(SIGALRM, &act, &oact) < 0) {
            myfailures(
                "Worker %d: Unable to set signal handler for SIGALRM", rank);
        }
        alarm(HOST_SCRIPT_TIMEOUT);
        
        // Wait for the host script to exit
        int status; pid_t result = waitpid(pid, &status, 0);
        
        // Reset timer
        alarm(0);
        if (sigaction(SIGALRM, &oact, NULL) < 0) {
            myfailures(
                "Worker %d: Unable to clear signal handler for SIGALRM", rank);
        }
        
        if (result < 0) {
            if (errno == EINTR) {
                // If waitpid was interrupted, then the host script timed out
                // Kill the host script's process group
                killpg(pid, SIGKILL);
                myfailure("Worker %d: Host script timed out after %d seconds", 
                    rank, HOST_SCRIPT_TIMEOUT);
            } else {
                myfailures("Worker %d: Error waiting for host script", rank);
            }
        } else {
            if (WIFEXITED(status)) {
                log_info("Worker %d: Host script exited with status %d (%d)", 
                    rank, WEXITSTATUS(status), status);
            } else {
                log_info("Worker %d: Host script exited on signal %d (%d)", 
                    rank, WTERMSIG(status), status);
            }
            
            if (status != 0) {
                myfailure("Worker %d: Host script failed", rank);
            }
        }
    }
}

/**
 * Send SIGTERM to the host script's process group to shut down any services that
 * it left running.
 */
void Worker::kill_host_script_group() {
    // Workers with host_rank > 0 will not have host scripts
    if (host_rank > 0)
        return;
    
    // Not necessary if there is no pgid to kill
    if (host_script_pgid <= 0)
        return;
    
    log_debug("Worker %d: Terminating host script process group with SIGTERM", rank);
    
    if (killpg(host_script_pgid, SIGTERM) < 0) {
        if (errno != ESRCH) {
            log_warn("Worker %d: Unable to terminate host script process group: %s", 
                rank, strerror(errno));
        }
    } else {
        // There must have been some processes in the process group, give 
        // them a few seconds and then kill them
        if (sleep(HOST_SCRIPT_GRACE_PERIOD) != 0) {
            log_warn("Worker %d: sleep() finished prematurely", rank);
        }
        if (killpg(host_script_pgid, SIGKILL) < 0) {
            if (errno != ESRCH) {
                log_warn("Worker %d: Unable to kill host script process group: %s", 
                    rank, strerror(errno));
            }
        } else {
            // If the call didn't fail, then there were some remaining
            log_warn("Worker %d: Sent SIGKILL to resilient host script processes", rank);
        }
    }
}

int Worker::run() {
    log_debug("Worker %d: Starting...", rank);
    
    // Send worker's hostname
    send_registration(host_name, host_memory, host_cpus);
    log_trace("Worker %d: Host name: %s", rank, host_name.c_str());
    log_trace("Worker %d: Host memory: %u MB", rank, this->host_memory);
    log_trace("Worker %d: Host CPUs: %u", rank, this->host_cpus);
    
    // Get worker's host rank
    recv_hostrank(host_rank);
    log_trace("Worker %d: Host rank: %d", rank, host_rank);
    
    log_debug("Worker %d: Using task stdout file: %s", rank, outfile.c_str());
    log_debug("Worker %d: Using task stderr file: %s", rank, errfile.c_str());
    
    int out = open(outfile.c_str(), O_WRONLY|O_APPEND|O_CREAT, 0000644);
    if (out < 0) {
        myfailures("Worker %d: unable to open task stdout", rank);
    }
    
    int err = open(errfile.c_str(), O_WRONLY|O_APPEND|O_CREAT, 0000644);
    if (err < 0) {
        myfailures("Worker %d: unable to open task stderr", rank);
    }
    
    // If there is a host script, then run it and wait here for all the host scripts to finish
    if ("" != host_script) {
        run_host_script();
        MPI_Barrier(MPI_COMM_WORLD);
    }
    
    while (1) {
        log_trace("Worker %d: Waiting for request", rank);
        
#ifdef SLEEP_IF_NO_REQUEST
        /* On many MPI implementations MPI_Recv uses a busy wait loop. This
         * really wreaks havoc on the load and CPU utilization of the workers
         * when there are no tasks to process or some slots are idle due to 
         * limited resource availability (memory and CPUs). In order to avoid 
         * that we check here to see if there are any requests first, and if 
         * there are not, then we wait for a few millis before checking again 
         * and keep doing that until there is a request waiting. This should 
         * reduce the load/CPU usage on workers significantly. It decreases
         * responsiveness a bit, but it is a fair tradeoff.
         */
        while (!request_waiting()) {
            usleep(NO_MESSAGE_SLEEP_TIME);
        }
#endif
        
        std::string name;
        std::string command;
        std::string pegasus_id;
        unsigned int memory = 0;
        unsigned int cpus = 0;
        std::vector<std::string> forwards;
        int shutdown;
        recv_request(name, command, pegasus_id, memory, cpus, forwards, shutdown);
        log_trace("Worker %d: Got request", rank);
        
        if (shutdown) {
            log_trace("Worker %d: Got shutdown message", rank);
            break;
        }
        
        log_debug("Worker %d: Running task %s", rank, name.c_str());
        
        double task_start = current_time();
        
        // Process arguments
        std::list<std::string> args;
        split_args(args, command);
        
        // Task status is exit code 1 by default
        int status = 256; // 256 is exit code 1
        
        // Create pipes for all of the forwarded files
        std::vector<Pipe *> pipes;
        for (unsigned i=0; i<forwards.size(); i++) {
            int pipefd[2];
            if (pipe(pipefd) < 0) {
                log_error("Unable to create pipe for task %s: %s",
                        name.c_str(), strerror(errno));
                // TODO Handle this error somehow
            }
            Pipe *p = new Pipe(forwards[i], pipefd[0], pipefd[1]);
            log_trace("Pipe: %s = %s", p->varname.c_str(), p->filename.c_str());
            pipes.push_back(p);
        }
        
        pid_t pid = fork();
        if (pid < 0) {
            // Fork failed
            log_error("Unable to fork task %s: %s", 
                name.c_str(), strerror(errno));
        } else if (pid == 0) {
            // In child
            
            // Close the read end of all the pipes
            for (unsigned i=0; i<pipes.size(); i++) {
                pipes[i]->closeread();
            }
            
            // TODO Search path if args[0] does not have '/'
            
            // Create argument structure
            unsigned nargs = args.size();
            char **argp = new char*[nargs+1];
            for (unsigned i=0; i<nargs; i++) {
                std::string arg = args.front();
                asprintf(&argp[i], "%s", arg.c_str());
                args.pop_front();
            }
            argp[nargs] = NULL;
            
            // Create environment structure
            unsigned nenvs = 0;
            while (environ[nenvs]) nenvs++;
            char **envp = new char*[nenvs+pipes.size()+1];
            for (unsigned i=0; i<nenvs; i++) {
                envp[i] = environ[i];
            }
            for (unsigned i=0; i<pipes.size(); i++) {
                Pipe *p = pipes[i];
                asprintf(&envp[nenvs+i], "%s=%d", p->varname.c_str(), p->writefd);
            }
            envp[nenvs+pipes.size()] = NULL;
            
            // Redirect stdout/stderr
            if (dup2(out, STDOUT_FILENO) < 0) {
                log_fatal("Error redirecting stdout of task %s: %s", 
                    name.c_str(), strerror(errno));
                exit(1);
            }
            if (dup2(err, STDERR_FILENO) < 0) {
                log_fatal("Error redirecting stderr of task %s: %s", 
                    name.c_str(), strerror(errno));
                exit(1);
            }
            
            // Set strict resource limits
            if (strict_limits && memory > 0) {
                rlim_t bytes = memory * 1024 * 1024;
                struct rlimit memlimit;
                memlimit.rlim_cur = bytes;
                memlimit.rlim_max = bytes;
                
                // These limits don't always seem to work, so set all of them
                if (setrlimit(RLIMIT_DATA, &memlimit) < 0) {
                    log_error("Unable to set memory limit (RLIMIT_DATA) for task %s: %s",
                        name.c_str(), strerror(errno));
                }
                if (setrlimit(RLIMIT_STACK, &memlimit) < 0) {
                    log_error("Unable to set memory limit (RLIMIT_STACK) for task %s: %s",
                        name.c_str(), strerror(errno));
                }
                if (setrlimit(RLIMIT_RSS, &memlimit) < 0) {
                    log_error("Unable to set memory limit (RLIMIT_RSS) for task %s: %s",
                        name.c_str(), strerror(errno));
                }
                if (setrlimit(RLIMIT_AS, &memlimit) < 0) {
                    log_error("Unable to set memory limit (RLIMIT_AS) for task %s: %s",
                        name.c_str(), strerror(errno));
                }
            }
            
            // Exec process
            execve(argp[0], argp, envp);
            fprintf(stderr, "Unable to exec command for task %s: %s\n", 
                name.c_str(), strerror(errno));
            exit(1);
        } else {
            // Close the write end of all the pipes
            for (unsigned i=0; i<pipes.size(); i++) {
                pipes[i]->closewrite();
            }
            
            // TODO poll() for data on pipes
            
            // Wait for task to complete
            if (waitpid(pid, &status, 0) < 0) {
                log_error("Failed waiting for task: %s", strerror(errno));
            }
        }
        
        // Close all the pipes
        for (unsigned i=0; i<pipes.size(); i++) {
            pipes[i]->close();
        }
        
        double task_finish = current_time();
        
        double task_runtime = task_finish - task_start;
        
        if (WIFEXITED(status)) {
            log_debug("Worker %d: Task %s exited with status %d (%d) in %f seconds", 
                rank, name.c_str(), WEXITSTATUS(status), status, task_runtime);
        } else {
            log_debug("Worker %d: Task %s exited on signal %d (%d) in %f seconds", 
                rank, name.c_str(), WTERMSIG(status), status, task_runtime);
        }
        
        // pegasus cluster output - used for provenance
        
        // If the Pegasus id is missing then don't add it to the message
        std::string id = "";
        if (pegasus_id.size() > 0) {
            id = "id=" + pegasus_id + ", ";
        }
        
        std::string app = args.front();
        
        char date[32];
        iso2date(task_start, date, sizeof(date));
        
        char summary[BUFSIZ];
        sprintf(summary, 
            "[cluster-task %sname=%s, start=\"%s\", duration=%.3f, "
            "status=%d, app=\"%s\", hostname=\"%s\", slot=%d, cpus=%u, memory=%u]\n",
            id.c_str(), name.c_str(), date, task_runtime, status, app.c_str(), 
            host_name.c_str(), rank, cpus, memory);
        write(out, summary, strlen(summary));
        
        send_response(name, status, task_runtime);
    }
    
    kill_host_script_group();
    
    close(out);
    close(err);
    
    log_debug("Worker %d: Exiting...", rank);
    
    return 0;
}
