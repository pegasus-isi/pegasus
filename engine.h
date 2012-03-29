#ifndef ENGINE_H
#define ENGINE_H

#include <queue>
#include <set>
#include "stdio.h"

#include "dag.h"

class Engine {
    DAG *dag;
    std::queue<Task *> ready;
    std::set<Task *> queue;
    FILE *rescue;
    int failures;
    int max_failures;
    int tries;
    
    void queue_ready_task(Task *t);

    void open_rescue(const std::string &rescuefile);
    void close_rescue();
    void write_rescue(Task *task);
    bool has_rescue();
public:
    Engine(DAG &dag, const std::string &rescuefile = "", int max_failures = 0, int tries = 1);
    ~Engine();
    
    bool max_failures_reached();
    void mark_task_finished(Task *t, int exitcode);
    bool has_ready_task();
    Task *next_ready_task();
    bool is_finished();
    bool is_failed();
};

#endif /* ENGINE_H */
