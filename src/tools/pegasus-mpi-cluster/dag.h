#ifndef DAG_H
#define DAG_H

#include <string>
#include <map>
#include <vector>

using std::string;
using std::map;
using std::vector;

class Task {
public:
    string name;
    string command;
    vector<Task *> children;
    vector<Task *> parents;
    
    // This comes from the pegasus cluster arguments
    string pegasus_id;
    
    bool success;
    bool io_failed;
    int last_exitcode;
    
    unsigned memory;
    unsigned cpus;
    unsigned tries;
    unsigned failures;
    int priority;
    map<string, string> *pipe_forwards;
    map<string, string> *file_forwards;
    
    unsigned submit_seq;
    
    Task(const string &name, const string &command, unsigned memory, unsigned cpus, unsigned tries, int priority, const map<string,string> &pipe_forwards, const map<string,string> &file_forwards);
    ~Task();
    
    bool is_ready();
};

class DAG {
    map<string, Task *> tasks;
    bool lock;
    int dagfd;
    unsigned tries;
    
    void read_dag(const string &filename);
    void read_rescue(const string &filename);
    void add_task(Task *task);
    void add_edge(const string &parent, const string &child);
public:
    typedef map<string, Task *>::iterator iterator;
    
    DAG(const string &dagfile, const string &rescuefile = "", const bool lock = true, unsigned tries = 1);
    ~DAG();
    
    bool has_task(const string &name) const;
    Task *get_task(const string &name) const;
    iterator begin() { return this->tasks.begin(); }
    iterator end() { return this->tasks.end(); }
    unsigned size() { return this->tasks.size(); }
};

#endif /* DAG_H */
