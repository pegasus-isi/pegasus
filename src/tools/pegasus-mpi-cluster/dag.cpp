#include <map>
#include <vector>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <math.h>

#include "strlib.h"
#include "dag.h"
#include "failure.h"
#include "log.h"

using std::string;
using std::vector;
using std::map;
using std::list;

#define MAX_LINE 16384

Task::Task(const string &name, const string &command, unsigned memory, unsigned cpus, unsigned tries, int priority, const map<string,string> &pipe_forwards, const map<string,string> &file_forwards) {
    this->name = name;
    this->command = command;
    this->memory = memory;
    this->cpus = cpus;
    this->tries = tries;
    this->priority = priority;
    this->pipe_forwards = NULL;
    if (pipe_forwards.size() > 0) {
        this->pipe_forwards = new map<string,string>(pipe_forwards);
    }
    this->file_forwards = NULL;
    if (file_forwards.size() > 0) {
        this->file_forwards = new map<string,string>(file_forwards);
    }
    this->io_failed = false;
    this->success = false;
    this->failures = 0;
}

Task::~Task() {
    delete pipe_forwards;
    delete file_forwards;
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

DAG::DAG(const string &dagfile, const string &rescuefile, const bool lock, unsigned tries) {
    this->lock = lock;
    this->tries = tries;
    
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

bool DAG::has_task(const string &name) const {
    return this->tasks.find(name) != this->tasks.end();
}

Task *DAG::get_task(const string &name) const {
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

void DAG::add_edge(const string &parent, const string &child) {
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
    
    string pegasus_id = "";
    string pegasus_transformation = "";
    string pegasus_name = ""; 
    char line[MAX_LINE];
    while (fgets(line, MAX_LINE, this->dag) != NULL) {
        string rec(line);
        trim(rec);
        
        // Blank lines
        if (rec.length() == 0) {
            continue;
        }
        
        
        if (rec.find("TASK", 0, 4) == 0) {
            vector<string> v;
            
            split(v, rec, DELIM, 2);
            
            if (v.size() < 3) {
                myfailure("Invalid TASK record: %s\n", line);
            }
            
            string name = v[1];
            
            // Check for duplicate tasks
            if (this->has_task(name)) {
                myfailure("Duplicate task: %s", name.c_str());
            }
            
            // Default task arguments
            unsigned memory = 0;
            unsigned cpus = 1;
            unsigned tries = this->tries;
            int priority = 0;
            map<string, string> pipe_forwards;
            map<string, string> file_forwards;
            
            // Parse task arguments
            list<string> args;
            split_args(args, v[2]);
            while (true) {
                string arg = args.front();
                if (arg[0] == '-') {
                    if (arg == "-m" || arg == "--request-memory") {
                        args.pop_front();
                        if (args.size() == 0) {
                            myfailure("-m/--request-memory requires N for task %s", 
                                name.c_str());
                        }
                        string smemory = args.front();
                        float fmemory;
                        if (sscanf(smemory.c_str(), "%f", &fmemory) != 1) {
                            myfailure(
                                "Invalid memory requirement '%s' for task %s", 
                                smemory.c_str(), name.c_str());
                        }
                        if (fmemory < 0) {
                            myfailure(
                                "Negative memory requirement not allowed for task %s", 
                                name.c_str());
                        }
                        // We round up to the next integer
                        memory = (unsigned)ceil(fmemory);
                        log_trace("Requested %u MB memory for task %s", 
                            memory, name.c_str());
                    } else if (arg == "-c" || arg == "--request-cpus") {
                        args.pop_front();
                        if (args.size() == 0) {
                            myfailure("-c/--request-cpus requires N for task %s", 
                                name.c_str());
                        }
                        string scpus = args.front();
                        float fcpus;
                        if (sscanf(scpus.c_str(), "%f", &fcpus) != 1) {
                            myfailure(
                                "Invalid CPU requirement '%s' for task %s", 
                                scpus.c_str(), name.c_str());
                        }
                        if (fcpus < 0) {
                            myfailure(
                                "Negative CPU requirement not allowed for task %s", 
                                name.c_str());
                        }
                        // We round up to the next integer
                        cpus = (unsigned)ceil(fcpus);
                        log_trace("Requested %u CPUs for task %s", 
                            cpus, name.c_str());
                    } else if (arg == "-t" || arg == "--tries") {
                        args.pop_front();
                        if (args.size() == 0) {
                            myfailure("-t/--tries requires N for task %s", 
                                name.c_str());
                        }
                        string stries = args.front();
                        int itries;
                        if (sscanf(stries.c_str(), "%d", &itries) != 1) {
                            myfailure("Invalid tries '%s' for task %s", 
                                stries.c_str(), name.c_str());
                        }
                        if (itries < 0) {
                            myfailure("Negative tries not allowed for task %s", 
                                name.c_str());
                        }
                        tries = itries;
                        log_trace("Task %s has %u tries", name.c_str(), tries);
                    } else if (arg == "-p" || arg == "--priority") {
                        args.pop_front();
                        if (args.size() == 0) {
                            myfailure("-p/--priority requires P for task %s", 
                                name.c_str());
                        }
                        string spriority = args.front();
                        if (sscanf(spriority.c_str(), "%d", &priority) != 1) {
                            myfailure("Invalid priority '%s' for task %s", 
                                spriority.c_str(), name.c_str());
                        }
                        log_trace("Task %s has priority %d", 
                            name.c_str(), priority);
                    } else if (arg == "-f" || arg == "--pipe-forward") {
                        args.pop_front();
                        if (args.size() == 0) {
                            myfailure("-f/--pipe-forward requires VAR=PATH for task %s",
                                name.c_str());
                        }
                        string forward = args.front();
                        size_t eq = forward.find("=");
                        if (eq == string::npos) {
                            myfailure("-f/--pipe-forward format should be VAR=PATH for task %s: %s",
                                    name.c_str(), forward.c_str());
                        }
                        string varname = forward.substr(0, eq);
                        string filename = forward.substr(eq + 1);
                        log_trace("Task %s needs data forwarded to %s",
                                name.c_str(), filename.c_str());
                        pipe_forwards[varname] = filename;
                    } else if (arg == "-F" || arg == "--file-forward") {
                        args.pop_front();
                        if (args.size() == 0) {
                            myfailure("-F/--file-forward requires SRC=DEST for task %s",
                                name.c_str());
                        }
                        string forward = args.front();
                        size_t eq = forward.find("=");
                        if (eq == string::npos) {
                            myfailure("-F/--file-forward format should be SRC=DEST for task %s: %s",
                                    name.c_str(), forward.c_str());
                        }
                        string srcfile = forward.substr(0, eq);
                        string destfile = forward.substr(eq + 1);
                        log_trace("Task %s needs data forwarded from %s to %s",
                                name.c_str(), srcfile.c_str(), destfile.c_str());
                        file_forwards[srcfile] = destfile;
                    } else {
                        myfailure("Invalid argument '%s' for task %s", 
                            arg.c_str(), name.c_str());
                    }
                    args.pop_front();
                } else {
                    break;
                }
            }
            
            // Copy all the arguments into a single string
            string command = "";
            while (args.size() > 0) {
                command += args.front();
                args.pop_front();
                if (args.size() > 0) {
                    command += " ";
                }
            }
            
            Task *t = new Task(name, command, memory, cpus, tries, priority, pipe_forwards, file_forwards);
            
            if (pegasus_id.length() > 0) {
                if (pegasus_name != name) {
                    myfailure("Name from Pegasus does not match task: %s != %s\n",
                            pegasus_name.c_str(), name.c_str());
                }
                t->pegasus_id = pegasus_id;
                
                // reset the extra parameters
                pegasus_id = "";
                pegasus_transformation = "";
                pegasus_name = "";
            }
            this->add_task(t);
        } else if (rec.find("EDGE", 0, 4) == 0) {
            
            vector<string> v;
            
            split(v, rec, DELIM, 2);
            
            if (v.size() < 3) {
                myfailure("Invalid EDGE record: %s\n", line);
            }
            
            string parent = v[1];
            string child = v[2];
            
            this->add_edge(parent, child);
        } else if (rec.find("#@", 0, 2) == 0) {
            // Pegasus cluster comment - includes extra task information
            vector<string> v;
            
            split(v, rec, DELIM, 3);
            
            if (v.size() < 4) {
                myfailure("Invalid #@ record: %s\n", line);
            }

            pegasus_id = v[1];
            pegasus_transformation = v[2];
            pegasus_name = v[3];
        } else if (rec[0] == '#') {
            // Comments
        } else {
            myfailure("Invalid DAG record: %s", line);
        }
    }
}

void DAG::read_rescue(const string &filename) {
    
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
        string rec(line);
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
            vector<string> v;
            
            split(v, rec, DELIM, 1);
            
            if (v.size() < 2) {
                myfailure("Invalid DONE record: %s\n", line);
            }
            
            string name = v[1];
            
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
