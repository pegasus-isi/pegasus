#include <map>
#include <vector>
#include "string.h"
#include "stdio.h"
#include "unistd.h"

#include "strlib.h"
#include "dag.h"
#include "failure.h"
#include "log.h"
#include "engine.h"

Engine::Engine(DAG &dag, const std::string &rescuefile, int max_failures, int tries) {
    if (max_failures < 0) {
        myfailure("max_failures must be >= 0");
    }
    if (tries < 1) {
        myfailure("tries must be >= 1");
    }
    this->max_failures = max_failures;
    this->tries = tries;
    this->dag = &dag;
    this->rescue = NULL;
    if (!rescuefile.empty()) {
        this->open_rescue(rescuefile);
    }
    
    this->failures = 0;
    
    // Queue all tasks that are ready, but not done
    for (DAG::iterator i=this->dag->begin(); i!=this->dag->end(); i++) {
        Task *t = (*i).second;
        if (t->is_ready() && !t->success) {
            this->queue_ready_task(t);
        }
    }
}

Engine::~Engine() {
    // Close rescue file
    this->close_rescue();
}

void Engine::queue_ready_task(Task *t) {
    this->ready.push(t);
    this->queue.insert(t);
}

void Engine::open_rescue(const std::string &filename) {
    this->rescue = fopen(filename.c_str(), "a");
    if (this->rescue == NULL) {
        myfailure("Unable to open rescue file: %s", filename.c_str());
    }
    
    // Mark done tasks as done in the new rescue file
    for (DAG::iterator i=this->dag->begin(); i!=this->dag->end(); i++) {
        Task *t = (*i).second;
        if (t->success) {
            this->write_rescue(t);
        }
    }
}

bool Engine::has_rescue() {
    return this->rescue != NULL;
}

void Engine::close_rescue() {
    if (this->has_rescue()) {
        fclose(this->rescue);
        this->rescue = NULL;
    }
}

void Engine::write_rescue(Task *task) {
    if (this->has_rescue()) {
        if (fprintf(this->rescue, "\nDONE %s", task->name.c_str()) < 0) {
            log_error("Error writing to rescue file: %s", strerror(errno));
        }
        if (fflush(this->rescue)) {
            log_error("Error flushing rescue file: %s", strerror(errno));
        }
    }
}

void Engine::mark_task_finished(Task *t, int exitcode) {
    
    if (exitcode == 0) {
        // Task succeeded
        t->success = true;
        this->write_rescue(t);
    } else {
        // Task failed
        t->failures += 1;

        //If job can be retried, then re-submit it
        if (t->failures < this->tries) {
            this->queue_ready_task(t);
            return;
        }
        
        // Otherwise count the failure
        this->failures += 1;
    }

    // Remove from the queue
    this->queue.erase(t);
    
    if (max_failures_reached()) {
        // Clear ready queue
        while (this->has_ready_task()) {
            Task *t = this->next_ready_task();
            this->queue.erase(t);
        }
    } else {
        // Release ready children
        for (unsigned i=0; i<t->children.size(); i++) {
            Task *c = t->children[i];
            if (c->is_ready()) {
                this->queue_ready_task(c);
            }
        }
    }
    
    // If we are finished, close rescue
    if (this->is_finished()) {
        this->close_rescue();
    }
}

bool Engine::max_failures_reached() {
    return this->failures >= this->max_failures && this->max_failures != 0;
}

bool Engine::has_ready_task() {
    return !this->ready.empty();
}

Task *Engine::next_ready_task() {
    if (!this->has_ready_task()) {
        myfailure("No ready tasks");
    }
    Task *t = this->ready.front();
    this->ready.pop();
    return t;
}

bool Engine::is_finished() {
    return this->queue.empty();
}

bool Engine::is_failed() {
    bool finished = this->is_finished();
    if (!finished) {
        myfailure("Not finished");
    }
    
    for (DAG::iterator i=this->dag->begin(); i!=this->dag->end(); i++) {
        Task *t = (*i).second;
        if (!t->success) {
            return true;
        }
    }
    
    return false;
}
