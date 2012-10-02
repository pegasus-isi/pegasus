#ifndef WORKER_H
#define WORKER_H

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

class Forward {
public: 
    virtual ~Forward() {};
    virtual const char *data() = 0;
    virtual size_t size() = 0;
    virtual string destination() = 0;
};

class PipeForward : public Forward {
private:
    string buffer;

public:
    string filename;
    string varname;
    int readfd;
    int writefd;
    
    PipeForward(string varname, string filename, int readfd, int writefd);
    ~PipeForward();
    int read();
    void append(char *buff, int size);
    void close();
    void closeread();
    void closewrite();
    const char *data();
    size_t size();
    string destination();
};

class FileForward : public Forward {
public:
    string destfile;
    string srcfile;
    char *buff;
    size_t buffsize;
    
    FileForward(const string &srcfile, const string &destfile, char *buff, size_t buffsize);
    ~FileForward();
    const char *data();
    size_t size();
    string destination();
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
    
    vector<Forward *> forwards;
    vector<PipeForward *> pipes;
    map<string, string> pipe_forwards;
    vector<FileForward *> files;
    map<string, string> file_forwards;
    
    double start;
    double finish;
    
    int status;
    
    TaskHandler(Worker *worker, string &name, string &command, string &id, unsigned memory, unsigned cpus, const map<string,string> &pipe_forwards, const map<string,string> &file_forwards);
    ~TaskHandler();
    double elapsed();
    void execute();
private:
    bool succeeded();
    void send_result();
    int run_process();
    void child_process();
    void write_cluster_task();
    void send_io_data();
    int read_file_data();
    void delete_files();
};

#endif /* WORKER_H */
