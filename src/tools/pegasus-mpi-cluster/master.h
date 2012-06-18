#ifndef MASTER_H
#define MASTER_H

#include <list>
#include <vector>

#include "engine.h"
#include "dag.h"

class Host {
public:
    std::string host_name;
    unsigned int memory;
    unsigned int cpus;
    
    Host(const std::string &host_name, unsigned int memory, unsigned int cpus) {
        this->host_name = host_name;
        this->memory = memory;
        this->cpus = cpus;
    }
    
    void log_status();
};

class Slot {
public:
    unsigned int rank;
    Host *host;
    
    Slot(unsigned int rank, Host *host) {
        this->rank = rank;
        this->host = host;
    }
};

class TaskPriority {
public:
    bool operator ()(const Task *x, const Task *y){
        return x->priority < y->priority;
    }
};

typedef std::priority_queue<Task *, std::vector<Task *>, TaskPriority> TaskQueue;

typedef std::list<Slot *> SlotList;
typedef std::list<Task *> TaskList;

class Master {
    std::string program;
    std::string dagfile;
    std::string outfile;
    std::string errfile;
    DAG *dag;
    Engine *engine;
    
    std::vector<Slot *> slots;
    std::vector<Host *> hosts;
    SlotList free_slots;
    TaskQueue ready_queue;
    
    int numworkers;
    
    long total_count;
    long success_count;
    long failed_count;
    
    unsigned total_cpus;
    
    bool has_host_script;
    
    void register_workers();
    void schedule_tasks();
    void wait_for_results();
    void process_result();
    void queue_ready_tasks();
    void submit_task(Task *t, int worker);
    void merge_task_stdio(FILE *dest, const std::string &src, const std::string &stream);
public:
    Master(const std::string &program, Engine &engine, DAG &dag, const std::string &dagfile, const std::string &outfile, const std::string &errfile, bool has_host_script = false);
    ~Master();
    int run();
};

#endif /* MASTER_H */
