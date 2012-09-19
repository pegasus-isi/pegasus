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
    
    // These come from the Pegasus cluster comments
    string pegasus_id;
    string pegasus_transformation;
    
    bool success;

    bool io_failed;
    
    unsigned memory;
    unsigned cpus;
    unsigned tries;
    unsigned failures;
    int priority;
    map<string, string> forwards;
    
    Task(const string &name, const string &command);
    ~Task();
    
    bool is_ready();
};

class DAG {
    FILE *dag;
    map<string, Task *> tasks;
    bool lock;
    unsigned tries;
    
    void read_dag();
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
};

#endif /* DAG_H */
