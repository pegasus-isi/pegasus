#include <map>
#include <vector>
#include "string.h"
#include "stdio.h"
#include "unistd.h"
#include "fcntl.h"

#include "strlib.h"
#include "dag.h"
#include "failure.h"
#include "log.h"

#define MAX_LINE 16384

Task::Task(const std::string &name, const std::string &command) {
    this->name = name;
    this->command = command;
    this->success = false;
    this->failures = 0;
}

Task::~Task() {
}

bool Task::is_ready() {
    // A task is ready when all its parents are done
    if (this->parents.empty()) {
        return true;
    }
    
    for (unsigned j=0; j<this->parents.size(); j++) {
        Task *p = this->parents[j];
        if (!p->success) {
            return false;
        }
    }
    
    return true;
}
    
void Task::set_extra_id(const std::string &extra_id) {
    this->extra_id = extra_id;
}

void Task::set_extra_transformation(const std::string &extra_transformation) {
    this->extra_transformation = extra_transformation;
}

DAG::DAG(const std::string &dagfile, const std::string &rescuefile, const bool lock) {
    this->lock = lock;
    
    this->dag = fopen(dagfile.c_str(), "r+");
    if (this->dag == NULL) {
        myfailures("Unable to open DAG file: %s", dagfile.c_str());
    }
    
    if (this->lock) {
        log_debug("Locking DAG file...");
        
        int dagfd = fileno(this->dag);
        struct flock exclusive;
        
        exclusive.l_start = 0;
        exclusive.l_len = 0;
        exclusive.l_type = F_WRLCK;
        exclusive.l_whence = SEEK_SET;
        exclusive.l_pid = getpid();
        
        int locked = fcntl(dagfd, F_SETLK, &exclusive);
        if (locked < 0) {
            if (errno == EAGAIN || errno == EACCES) {
                myfailures("DAG file is already locked by another process: %s", dagfile.c_str());
            } else {
                myfailures("Unable to lock DAG file: %s", dagfile.c_str());
            }
        }
    }
    
    this->read_dag();
    
    if (!rescuefile.empty()) {
        this->read_rescue(rescuefile);
    }
}

DAG::~DAG() {
    
    if (this->lock) {
        log_debug("Unlocking DAG file...");
        
        int dagfd = fileno(this->dag);
        struct flock clear;
        
        clear.l_start = 0;
        clear.l_len = 0;
        clear.l_type = F_UNLCK;
        clear.l_whence = SEEK_SET;
        clear.l_pid = getpid();
        
        int locked = fcntl(dagfd, F_SETLK, &clear);
        if (locked < 0) {
            log_error("Error unlocking DAG file: %s", strerror(errno));
        }
    }
    
    fclose(this->dag);
    
    // Delete all tasks
    for (iterator i = this->begin(); i != this->end(); i++) {
        delete (*i).second;
    }
}

bool DAG::has_task(const std::string &name) const {
    return this->tasks.find(name) != this->tasks.end();
}

Task *DAG::get_task(const std::string &name) const {
    if (!this->has_task(name)) {
        return NULL;
    }
    return (*(this->tasks.find(name))).second;
}

void DAG::add_task(Task *task) {
    if (this->has_task(task->name)) {
        myfailure("Duplicate task: %s\n", task->name.c_str());
    }
    this->tasks[task->name] = task;
}

void DAG::add_edge(const std::string &parent, const std::string &child) {
    if (!this->has_task(parent)) {
        myfailure("No such task: %s\n", parent.c_str());
    }
    if (!this->has_task(child)) {
        myfailure("No such task: %s\n", child.c_str());
    }
    
    Task *p = get_task(parent);
    Task *c = get_task(child);
    
    p->children.push_back(c);
    c->parents.push_back(p);
}

void DAG::read_dag() {
    const char *DELIM = " \t\n\r";
    
    std::string extra_id = "";
    std::string extra_transformation = "";
    std::string extra_name = ""; 
    char line[MAX_LINE];
    while (fgets(line, MAX_LINE, this->dag) != NULL) {
        std::string rec(line);
        trim(rec);
        
        // Blank lines
        if (rec.length() == 0) {
            continue;
        }
        
        
        if (rec.find("TASK", 0, 4) == 0) {
            std::vector<std::string> v;
            
            split(v, rec, DELIM, 2);
            
            if (v.size() < 3) {
                myfailure("Invalid TASK record: %s\n", line);
            }
            
            std::string name = v[1];
            std::string cmd = v[2];
                        
            // Check for duplicate tasks
            if (this->has_task(name)) {
                myfailure("Duplicate task: %s", name.c_str());
            }
            
            Task *t = new Task(name, cmd);
            if (extra_id.length() > 0) {
                if (extra_name != name) {
                    myfailure("Name from comment do not match task: %s %s\n",
                            extra_name.c_str(), name.c_str());
                }
                t->set_extra_id(extra_id);
                t->set_extra_transformation(extra_transformation);

                // reset the extra parameters
                extra_id = "";
                extra_transformation = "";
                extra_name = "";
            }
            this->add_task(t);
        } else if (rec.find("EDGE", 0, 4) == 0) {
            
            std::vector<std::string> v;
            
            split(v, rec, DELIM, 2);
            
            if (v.size() < 3) {
                myfailure("Invalid EDGE record: %s\n", line);
            }
            
            std::string parent = v[1];
            std::string child = v[2];
            
            this->add_edge(parent, child);
        } else if (rec.find("#@", 0, 2) == 0) {
            // Pegasus cluster comment - includes extra task information
            std::vector<std::string> v;
            
            split(v, rec, DELIM, 3);
            
            if (v.size() < 4) {
                myfailure("Invalid #@ record: %s\n", line);
            }

            extra_id = v[1];
            extra_transformation = v[2];
            extra_name = v[3];
        } else if (rec[0] == '#') {
            // Comments
        } else {
            myfailure("Invalid DAG record: %s", line);
        }
    }
}

void DAG::read_rescue(const std::string &filename) {
    
    // Check if rescue file exists
    if (access(filename.c_str(), R_OK)) {
        if (errno == ENOENT) {
            // File doesn't exist
            return;
        }
        myfailures("Unable to read rescue file: %s", filename.c_str());
    }
    
    FILE *rescuefile = fopen(filename.c_str(), "r");
    if (rescuefile == NULL) {
        myfailures("Unable to open rescue file: %s", filename.c_str());
    }
    
    const char *DELIM = " \t\n\r";
    char line[MAX_LINE];
    while (fgets(line, MAX_LINE, rescuefile) != NULL) {
        std::string rec(line);
        trim(rec);
        
        // Blank lines
        if (rec.length() == 0) {
            continue;
        }
        
        // Comments
        if (rec[0] == '#') {
            continue;
        }
        
        if (rec.find("DONE", 0, 4) == 0) {
            std::vector<std::string> v;
            
            split(v, rec, DELIM, 1);
            
            if (v.size() < 2) {
                myfailure("Invalid DONE record: %s\n", line);
            }
            
            std::string name = v[1];
            
            if (!this->has_task(name)) {
                myfailure("Unknown task %s in rescue file", name.c_str());
            }
            
            Task *task = this->get_task(name);
            task->success = true;
        } else {
            myfailure("Invalid rescue record: %s", line);
        }
    }
    
    fclose(rescuefile);
}
