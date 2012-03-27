#include <map>
#include <vector>
#include "string.h"
#include "stdio.h"
#include "unistd.h"

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

DAG::DAG(const std::string &dagfile, const std::string &rescuefile) {
    this->read_dag(dagfile);
    if (!rescuefile.empty()) {
        this->read_rescue(rescuefile);
    }
}

DAG::~DAG() {
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
        failure("Duplicate task: %s\n", task->name.c_str());
    }
    this->tasks[task->name] = task;
}

void DAG::add_edge(const std::string &parent, const std::string &child) {
    if (!this->has_task(parent)) {
        failure("No such task: %s\n", parent.c_str());
    }
    if (!this->has_task(child)) {
        failure("No such task: %s\n", child.c_str());
    }
    
    Task *p = get_task(parent);
    Task *c = get_task(child);
    
    p->children.push_back(c);
    c->parents.push_back(p);
}

void DAG::read_dag(const std::string &filename) {
    const char *DELIM = " \t\n\r";
    
    FILE *dagfile = fopen(filename.c_str(), "r");
    if (dagfile == NULL) {
        failures("Unable to open DAG file: %s", filename.c_str());
    }
   
    std::string extra_id = "";
    std::string extra_transformation = "";
    std::string extra_name = ""; 
    char line[MAX_LINE];
    while (fgets(line, MAX_LINE, dagfile) != NULL) {
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
                failure("Invalid TASK record: %s\n", line);
            }
            
            std::string name = v[1];
            std::string cmd = v[2];
                        
            // Check for duplicate tasks
            if (this->has_task(name)) {
                failure("Duplicate task: %s", name.c_str());
            }
            
            Task *t = new Task(name, cmd);
            if (extra_id.length() > 0) {
                if (extra_name != name) {
                    failure("Name from comment do not match task: %s %s\n",
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
                failure("Invalid EDGE record: %s\n", line);
            }
            
            std::string parent = v[1];
            std::string child = v[2];
            
            this->add_edge(parent, child);
        } else if (rec.find("#@", 0, 2) == 0) {
            // Pegasus cluster comment - includes extra task information
            std::vector<std::string> v;
            
            split(v, rec, DELIM, 3);
            
            if (v.size() < 4) {
                failure("Invalid #@ record: %s\n", line);
            }

            extra_id = v[1];
            extra_transformation = v[2];
            extra_name = v[3];
        } else if (rec[0] == '#') {
            // Comments
        } else {
            failure("Invalid DAG record: %s", line);
        }
    }
    
    
    fclose(dagfile);
}

void DAG::read_rescue(const std::string &filename) {
    
    // Check if rescue file exists
    if (access(filename.c_str(), R_OK)) {
        if (errno == ENOENT) {
            // File doesn't exist
            return;
        }
        failures("Unable to read rescue file: %s", filename.c_str());
    }
    
    FILE *rescuefile = fopen(filename.c_str(), "r");
    if (rescuefile == NULL) {
        failures("Unable to open rescue file: %s", filename.c_str());
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
                failure("Invalid DONE record: %s\n", line);
            }
            
            std::string name = v[1];
            
            if (!this->has_task(name)) {
                failure("Unknown task %s in rescue file", name.c_str());
            }
            
            Task *task = this->get_task(name);
            task->success = true;
        } else {
            failure("Invalid rescue record: %s", line);
        }
    }
    
    fclose(rescuefile);
}
