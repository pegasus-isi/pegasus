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
#include <map>
#include <poll.h>

#include "strlib.h"
#include "worker.h"
#include "protocol.h"
#include "log.h"
#include "failure.h"
#include "tools.h"

using std::string;
using std::map;
using std::vector;
using std::list;

extern char **environ;

static void log_signal(int signo) {
    log_error("Caught signal %d", signo);
}

Pipe::Pipe(string varname, string filename, int readfd, int writefd) {
    this->varname = varname;
    this->filename = filename;
    this->readfd = readfd;
    this->writefd = writefd;
}

Pipe::~Pipe() {
}

int Pipe::read() {
    char buff[BUFSIZ];
    int rc = ::read(readfd, buff, BUFSIZ);
    if (rc > 0) {
        // We got some data, save it to the pipe's data buffer
        this->buffer.append(buff, rc);
    }
    return rc;
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
        if (::close(this->readfd)) {
            log_error("Error closing read end of pipe %s: %s", 
                    varname.c_str(), strerror(errno));
        }
        this->readfd = -1;
    }
}

void Pipe::closewrite() {
    if (this->writefd != -1) {
        if (::close(this->writefd)) {
            log_error("Error closing write end of pipe %s: %s", 
                    varname.c_str(), strerror(errno));
        }
        this->writefd = -1;
    }
}

TaskHandler::TaskHandler(Worker *worker, string &name, string &command, string &id, unsigned memory, unsigned cpus, map<string, string> &forwards) {
    this->worker = worker;
    this->name = name;
    split_args(this->args, command);
    this->id = id;
    this->memory = memory;
    this->cpus = cpus;
    this->forwards = forwards;

    // We allocate this here so that we can be sure to free
    // it in the destructor
    this->fds = new struct pollfd[forwards.size()];
    
    this->start = 0;
    this->finish = 0;
    
    // Task status is exitcode 1 by default
    this->status = 256;
}

TaskHandler::~TaskHandler() {
    // Make sure the pipes are closed before
    // deleting them to prevent descriptor leaks
    // in the case of failures
    for (unsigned i=0; i<pipes.size(); i++) {
        pipes[i]->close();
        delete pipes[i];
    }
    delete [] fds;
}

/** Compute the elapsed runtime of the task */
double TaskHandler::elapsed() {
    if (this->start == 0) {
        return 0.0;
    }
    if (this->finish == 0) {
        return current_time() - this->start;
    }
    return this->finish - this->start;
}

/** 
 * Do all the operations required for the child process after
 * fork() up to and including execve()
 */
void TaskHandler::child_process() {
    
    // Redirect stdout/stderr. We do this first thing so that any
    // of the error messages printed before the execve show up in
    // the task stdout/stderr where they belong. Otherwise, we could
    // end up shipping a lot of error messages to the master process.
    if (dup2(worker->out, STDOUT_FILENO) < 0) {
        log_fatal("Error redirecting stdout of task %s: %s", 
            name.c_str(), strerror(errno));
        exit(1);
    }
    if (dup2(worker->err, STDERR_FILENO) < 0) {
        log_fatal("Error redirecting stderr of task %s: %s", 
            name.c_str(), strerror(errno));
        exit(1);
    }
    
    // Close the read end of all the pipes. This should force a
    // SIGPIPE in the case that the parent process closes the read
    // end of the pipe while we are writing to it.
    for (unsigned i=0; i<pipes.size(); i++) {
        pipes[i]->closeread();
    }
    
    // TODO Search path if args[0] does not have '/'
    
    // Create argument structure
    unsigned nargs = args.size();
    char **argp = new char*[nargs+1];
    for (unsigned i=0; i<nargs; i++) {
        string arg = args.front();
        if (asprintf(&argp[i], "%s", arg.c_str()) == -1) {
            log_fatal("Unable to create arguments: %s", strerror(errno));
            exit(1);
        }
        args.pop_front();
    }
    argp[nargs] = NULL;
    
    // Create environment structure. We need to copy the worker's
    // environment, but also add env variables for the pipes used
    // to forward I/O from the task.
    unsigned nenvs = 0;
    while (environ[nenvs]) nenvs++;
    char **envp = new char*[nenvs+pipes.size()+1];
    for (unsigned i=0; i<nenvs; i++) {
        envp[i] = environ[i];
    }
    for (unsigned i=0; i<pipes.size(); i++) {
        Pipe *p = pipes[i];
        if (asprintf(&envp[nenvs+i], "%s=%d", p->varname.c_str(), p->writefd) == -1) {
            log_fatal("Unable to create environment: %s", strerror(errno));
            exit(1);
        }
    }
    envp[nenvs+pipes.size()] = NULL;
    
    // Set strict resource limits
    if (worker->strict_limits && memory > 0) {
        rlim_t bytes = memory * 1024 * 1024;
        struct rlimit memlimit;
        memlimit.rlim_cur = bytes;
        memlimit.rlim_max = bytes;
        
        // These limits don't always seem to work, so set all of them. In fact,
        // they don't seem to work at all on OS X.
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
}

void TaskHandler::run() {
    log_trace("Running task %s", this->name.c_str());
    
    // Record start time of task
    start = current_time();
    
    // Create pipes for all of the forwarded files
    for (map<string,string>::iterator i = forwards.begin(); i != forwards.end(); i++) {
        string varname = (*i).first;
        string filename = (*i).second;
        int pipefd[2];
        if (pipe(pipefd) < 0) {
            log_error("Unable to create pipe for task %s: %s",
                    name.c_str(), strerror(errno));
            return;
        }
        log_trace("Pipe: %s = %s", varname.c_str(), filename.c_str());
        Pipe *p = new Pipe(varname, filename, pipefd[0], pipefd[1]);
        pipes.push_back(p);
    }
    
    // Fork a child process to execute the task
    pid_t pid = fork();
    if (pid < 0) {
        // Fork failed
        log_error("Unable to fork task %s: %s", name.c_str(), strerror(errno));
        return;
    }
    
    if (pid == 0) {
        child_process();
    }
    
    // Close the write end of all the pipes. I'm not really sure why.
    for (unsigned i=0; i<pipes.size(); i++) {
        pipes[i]->closewrite();
    }
    
    // Create a structure to hold all of the pipes
    // we need to read from
    map<int, Pipe *> reading;
    for (unsigned i=0; i<pipes.size(); i++) {
        reading[pipes[i]->readfd] = pipes[i];
    }
    
    // While there are pipes to read from
    while (reading.size() > 0) {
        
        // Set up inputs for poll()
        int nfds = 0;
        for (map<int, Pipe *>::iterator p=reading.begin(); p!=reading.end(); p++) {
            Pipe *pipe = (*p).second;
            fds[nfds].fd = pipe->readfd;
            fds[nfds].events = POLLIN;
            nfds++;
        }
        
        log_trace("Polling %d pipes", nfds);
        
        int timeout = -1;
        int rc = poll(fds, nfds, timeout);
        if (rc <= 0) {
            // If this happens then we are in trouble. The only thing we
            // can do is log it and break out of the loop. What should happen
            // then is that we close all the pipes, which will force the child
            // to get SIGPIPE and fail.
            log_error("poll() failed for task %s: %s", name.c_str(), 
                      strerror(errno));
            goto after_poll_loop;
        }
        
        // One or more of the file descriptors are readable, find out which ones
        for (int i=0; i<nfds; i++) {
            int revents = fds[i].revents;
            int fd = fds[i].fd;
            
            // This descriptor has no events 
            if (revents == 0) {
                continue;
            }
            
            if (revents & POLLIN) {
                rc = reading[fd]->read();
                if (rc < 0) {
                    // If this happens we have a serious problem and need the
                    // task to fail. Cause the failure by breaking out of the
                    // loop and closing the pipes.
                    log_error("Error reading from pipe %d: %s", 
                              fd, strerror(errno));
                    goto after_poll_loop;
                } else if (rc == 0) {
                    // Pipe was closed, EOF. Stop polling it.
                    log_trace("Pipe %d closed", fd);
                    reading.erase(fd);
                } else {
                    log_trace("Read %d bytes from pipe %d", rc, fd);
                }
            }
            
            if (revents & POLLHUP) {
                log_trace("Hangup on pipe %d", fd);
                // It is important that we don't stop reading the fd here
                // because in the next poll we may get more data if our
                // buffer wasn't big enough to get everything on this read.
            }
            
            if (revents & POLLERR) {
                // I don't know what would cause this. I think possibly it can
                // only happen for hardware devices and not pipes. In case it
                // does happen we will log it here and fail the task.
                log_error("Error on pipe %d", fd);
                goto after_poll_loop;
            }
        }
    }
    
after_poll_loop: 
    // Close the pipes here just in case something happens above 
    // so that we aren't deadlocked waiting for a process that is itself 
    // deadlocked waiting for us to read data off the pipe. Instead, 
    // if we close the pipes here, then the task will get SIGPIPE and we 
    // can wait on it successfully.
    for (unsigned i=0; i<pipes.size(); i++) {
        pipes[i]->close();
    }
    
    // Wait for task to complete
    if (waitpid(pid, &status, 0) < 0) {
        log_error("Failed waiting for task: %s", strerror(errno));
    }
    
    // Record the finish time of the task
    finish = current_time();
    
    write_cluster_task();
    
    double runtime = elapsed();
    
    if (WIFEXITED(status)) {
        log_debug("Task %s exited with status %d (%d) in %f seconds", 
            name.c_str(), WEXITSTATUS(status), status, runtime);
    } else {
        log_debug("Task %s exited on signal %d (%d) in %f seconds", 
            name.c_str(), WTERMSIG(status), status, runtime);
    }
}

void TaskHandler::write_cluster_task() {
    // pegasus cluster output - used for provenance
    
    // If the Pegasus id is missing then don't add it to the message
    string id_string = "";
    if (id.size() > 0) {
        id_string = "id=" + id + ", ";
    }
    
    string app = args.front();
    
    char date[32];
    iso2date(start, date, sizeof(date));
    
    char *summary;
    asprintf(&summary, 
        "[cluster-task %sname=%s, start=\"%s\", duration=%.3f, "
        "status=%d, app=\"%s\", hostname=\"%s\", slot=%d, cpus=%u, memory=%u]\n",
        id_string.c_str(), name.c_str(), date, elapsed(), status, app.c_str(), 
        worker->host_name.c_str(), worker->rank, cpus, memory);
    
    write(worker->out, summary, strlen(summary));
    
    delete summary;
}

Worker::Worker(const string &outfile, const string &errfile, const string &host_script, unsigned int host_memory, unsigned host_cpus, bool strict_limits) {
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
    
    // Send worker's registration message to the master
    RegistrationMessage regmsg(host_name, host_memory, host_cpus);
    send_message(regmsg, 0);
    log_trace("Worker %d: Host name: %s", rank, host_name.c_str());
    log_trace("Worker %d: Host memory: %u MB", rank, this->host_memory);
    log_trace("Worker %d: Host CPUs: %u", rank, this->host_cpus);
    
    // Get worker's host rank
    HostrankMessage *hrmsg = (HostrankMessage *)recv_message();
    host_rank = hrmsg->hostrank;
    delete hrmsg;
    log_trace("Worker %d: Host rank: %d", rank, host_rank);
    
    log_debug("Worker %d: Using task stdout file: %s", rank, outfile.c_str());
    log_debug("Worker %d: Using task stderr file: %s", rank, errfile.c_str());
    
    out = open(outfile.c_str(), O_WRONLY|O_APPEND|O_CREAT, 0000644);
    if (out < 0) {
        myfailures("Worker %d: unable to open task stdout", rank);
    }
    
    err = open(errfile.c_str(), O_WRONLY|O_APPEND|O_CREAT, 0000644);
    if (err < 0) {
        myfailures("Worker %d: unable to open task stderr", rank);
    }
    
    // If there is a host script, then run it and wait here for all the host scripts to finish
    if ("" != host_script) {
        run_host_script();
        MPI_Barrier(MPI_COMM_WORLD);
    }
    
    while (true) {
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
        while (!message_waiting()) {
            usleep(NO_MESSAGE_SLEEP_TIME);
        }
#endif
        
        Message *mesg = recv_message();
        if (mesg->type == SHUTDOWN) {
            log_trace("Worker %d: Got shutdown message", rank);
            delete mesg;
            break;
        } else if (mesg->type == COMMAND) {
            CommandMessage *cmd = (CommandMessage *)mesg;
            
            log_trace("Worker %d: Got task", rank);
            
            TaskHandler *task = new TaskHandler(this, cmd->name, cmd->command, 
                    cmd->id, cmd->memory, cmd->cpus, cmd->forwards);
            
            task->run();
            
            // Send task information back to master
            ResultMessage res(task->name, task->status, task->elapsed());
            send_message(res, 0);
            
            // If the task succeeded, then send the I/O back to the master
            if (task->status == 0) {
                for (unsigned i = 0; i < task->pipes.size(); i++) {
                    Pipe *pipe = task->pipes[i];
                    log_trace("Pipe %s got %d bytes", pipe->varname.c_str(), pipe->size());
                    // TODO Send the data from the pipes back to the master
                }
            }
            
            delete task;
        } else {
            myfailure("Unknown message type: %d", mesg->type);
        }
        
        delete mesg;
    }
    
    kill_host_script_group();
    
    close(out);
    close(err);
    
    log_debug("Worker %d: Exiting...", rank);
    
    return 0;
}
