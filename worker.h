#ifndef WORKER_H
#define WORKER_H

#include <poll.h>
#include <string>
#include <map>
#include <list>
#include <vector>

using std::string;
using std::map;
using std::list;
using std::vector;

// Give the host script 60 seconds to exit
#define HOST_SCRIPT_TIMEOUT 60

// Give processes in the host script's process 
// group 5 seconds after SIGTERM before sending SIGKILL
#define HOST_SCRIPT_GRACE_PERIOD 5

class Pipe {
public:
    string varname;
    string filename;
    int readfd;
    int writefd;
    string buffer;
    
    Pipe(string varname, string filename, int readfd, int writefd);
    ~Pipe();
    void save(char *buf, unsigned n);
    const char *data();
    unsigned size();
    void close();
    void closeread();
    void closewrite();
};

class Worker {
public:
    string outfile;
    string errfile;

    int out;
    int err;
    
    int rank;
    int host_rank;
    
    string host_script;
    pid_t host_script_pgid;
    
    string host_name;
    unsigned int host_memory;
    unsigned int host_cpus;
    
    bool strict_limits;
    
    Worker(const string &outfile, const string &errfile, const string &host_script, unsigned host_memory = 0, unsigned host_cpus = 0, bool strict_limits = false);
    ~Worker();
    int run();
    void run_host_script();
    void kill_host_script_group();
};

class TaskHandler {
public:
    Worker *worker;
    string name;
    string id;
    list<string> args;
    unsigned memory;
    unsigned cpus;
    map<string, string> forwards;
    
    vector<Pipe *> pipes;
    struct pollfd *fds;
    
    double start;
    double finish;
    
    int status;
    
    TaskHandler(Worker *worker, string &name, string &command, string &id, unsigned memory, unsigned cpus, map<string, string> &forwards);
    ~TaskHandler();
    double elapsed();
    void run();
    void child_process();
    void write_cluster_task();
};

#endif /* WORKER_H */
