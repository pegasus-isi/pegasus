#ifndef DAG_H
#define DAG_H

#include <string>
#include <map>
#include <vector>

class Task {
public:
    std::string name;
    std::string command;
    std::vector<Task *> children;
    std::vector<Task *> parents;

    // These come from the Pegasus cluster comments
    std::string pegasus_id;
    std::string pegasus_transformation;
    
    bool success;
    int failures;
    
    unsigned memory;
    unsigned cpus;
    
    Task(const std::string &name, const std::string &command);
    ~Task();
    
    bool is_ready();
};

class DAG {
    FILE *dag;
    std::map<std::string, Task *> tasks;
    bool lock;
    
    void read_dag();
    void read_rescue(const std::string &filename);
    void add_task(Task *task);
    void add_edge(const std::string &parent, const std::string &child);
public:
    typedef std::map<std::string, Task *>::iterator iterator;

    DAG(const std::string &dagfile, const std::string &rescuefile = "", const bool lock = true);
    ~DAG();

    bool has_task(const std::string &name) const;
    Task *get_task(const std::string &name) const;
    iterator begin() { return this->tasks.begin(); }
    iterator end() { return this->tasks.end(); }
};

#endif /* DAG_H */
