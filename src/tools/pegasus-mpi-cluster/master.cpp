#include <map>
#include <stdio.h>
#include <unistd.h>
#include <sys/time.h>
#include <mpi.h>

#include "master.h"
#include "failure.h"
#include "protocol.h"
#include "log.h"
#include "tools.h"

void Host::log_status() {
    log_trace("Host %s now has %u MB and %u CPUs free", 
        this->host_name.c_str(), this->memory, this->cpus);
}

Master::Master(const std::string &program, Engine &engine, DAG &dag,
               const std::string &dagfile, const std::string &outfile,
               const std::string &errfile) {
    this->program = program;
    this->dagfile = dagfile;
    this->outfile = outfile;
    this->errfile = errfile;
    this->engine = &engine;
    this->dag = &dag;
    
    total_count = 0;
    success_count = 0;
    failed_count = 0;
    
    total_cpus = 0;
    
    // Determine the number of workers we have
    int numprocs;
    MPI_Comm_size(MPI_COMM_WORLD, &numprocs);
    numworkers = numprocs - 1;
    if (numworkers == 0) {
        myfailure("Need at least 1 worker");
    }
}

Master::~Master() {
    std::vector<Slot *>::iterator s;
    for (s = slots.begin(); s != slots.end(); s++) {
        delete *s;
    }
    
    std::vector<Host *>::iterator h;
    for (h = hosts.begin(); h != hosts.end(); h++) {
        delete *h;
    }
}

void Master::submit_task(Task *task, int rank) {
    log_debug("Submitting task %s to slot %d", task->name.c_str(), rank);
    send_request(task->name, task->command, task->pegasus_id, task->memory, rank);
    
    this->total_count++;
}

void Master::wait_for_results() {
    // This will process all the waiting responses. If there are none 
    // waiting, then it will block until one arrives. If there are 
    // several waiting, then it will process them all and return without 
    // waiting.
    unsigned int tasks = 0;
    while (tasks == 0 || response_waiting()) {
        process_result();
        tasks++;
    }
    log_trace("Processed %u task(s) this cycle", tasks);
}

void Master::process_result() {
    log_trace("Waiting for task to finish");
    
    std::string name;
    int exitcode;
    int rank;
    recv_response(name, exitcode, rank);
    
    // Mark task finished
    if (exitcode == 0) {
        log_debug("Task %s finished with exitcode %d", name.c_str(), exitcode);
        this->success_count++;
    } else {
        log_error("Task %s failed with exitcode %d", name.c_str(), exitcode);
        this->failed_count++;
    }
    Task *task = this->dag->get_task(name);
    this->engine->mark_task_finished(task, exitcode);
    
    // Mark slot idle
    log_trace("Worker %d is idle", rank);
    Slot *slot = slots[rank-1];
    
    // Return resources to host
    Host *host = slot->host;
    host->memory += task->memory;
    host->log_status();
    
    // Mark slot as free
    free_slots.push_back(slot);
}

void Master::merge_task_stdio(FILE *dest, const std::string &srcfile, const std::string &stream) {
    log_trace("Merging %s file: %s", stream.c_str(), srcfile.c_str());
    
    FILE *src = fopen(srcfile.c_str(), "r");
    if (src == NULL) {
        // The file may not exist if the worker didn't run any tasks, just print a warning
        if (errno == ENOENT) {
            log_warn("No %s file: %s", stream.c_str(), srcfile.c_str());
            return;
        } else {
            myfailures("Unable to open task %s file: %s", stream.c_str(), srcfile.c_str());
        }
    }
    
    char buf[BUFSIZ];
    while (1) {
        int r = fread(buf, 1, BUFSIZ, src);
        if (r < 0) {
            myfailures("Error reading source file: %s", srcfile.c_str());
        }
        if (r == 0) {
            break;
        }
        int w = fwrite(buf, 1, r, dest);
        if (w < r) {
            myfailures("Error writing to dest file");
        }
    }
    
    fclose(src);
    
    if (unlink(srcfile.c_str())) {
        myfailures("Unable to delete task %s file: %s", stream.c_str(), srcfile.c_str());
    }
}

/*
 * Register all workers, create hosts, create slots. Assign a host-centric 
 * rank to each of the workers. The worker with the lowest global rank on 
 * each host is given host rank 0, the next lowest is given host rank 1, 
 * and so on. The master is not given a host rank.
 */
void Master::register_workers() {
    typedef std::map<std::string, Host *> HostMap;
    HostMap hostmap;
    
    typedef std::map<int, std::string> HostnameMap;
    HostnameMap hostnames;
    
    // Collect host names from all workers, create host objects
    for (int i=0; i<numworkers; i++) {
        int rank;
        std::string hostname;
        unsigned int memory = 0;
        unsigned int cpus = 0;
        
        recv_registration(rank, hostname, memory, cpus);
        
        hostnames[rank] = hostname;
        
        // If the host is not found, create a new one
        if (hostmap.find(hostname) == hostmap.end()) {
            log_debug("Got new host: name=%s, mem=%u, cpus=%u", hostname.c_str(), memory, cpus);
            Host *newhost = new Host(hostname, memory, cpus);
            hosts.push_back(newhost);
            hostmap[hostname] = newhost;
            
            total_cpus += cpus;
        }
        
        log_debug("Slot %d on host %s", rank, hostname.c_str());
    }
    
    typedef std::map<std::string, int> RankMap;
    RankMap ranks;
    
    // Create slots, assign a host rank to each worker
    for (int rank=1; rank<=numworkers; rank++) {
        std::string hostname = hostnames.find(rank)->second;
        
        // Find host
        Host *host = hostmap.find(hostname)->second;
        
        // Create new slot
        Slot *slot = new Slot(rank, host);
        slots.push_back(slot);
        free_slots.push_back(slot);
        
        // Compute hostrank for this slot
        RankMap::iterator nextrank = ranks.find(hostname);
        int hostrank = 0;
        if (nextrank != ranks.end()) {
            hostrank = nextrank->second;
        }
        ranks[hostname] = hostrank + 1;
        
        send_hostrank(rank, hostrank);
        log_debug("Host rank of worker %d is %d", rank, hostrank);
    }
}

void Master::schedule_tasks() {
    log_debug("Scheduling tasks...");
    
    // Keep scheduling tasks until no matches are found
    bool match = true;
    while (match) {
        match = false;
        
        // Shortcut when there are no more free slots
        if (free_slots.size() == 0) {
            return;
        }
        
        for (SlotList::iterator s = free_slots.begin(); s != free_slots.end(); s++) {
            Slot *slot = *s;
            Host *host = slot->host;
            
            // Shortcut when there are no more ready tasks
            if (ready_tasks.size() == 0) {
                return;
            }
            
            for (TaskList::iterator t = ready_tasks.begin(); t != ready_tasks.end(); t++) {
                Task *task = *t;
                
                // If the task fits, send it
                if (host->memory >= task->memory) {
                    
                    // We found a match
                    log_debug("Matched task %s to slot %d on host %s", 
                        task->name.c_str(), slot->rank, host->host_name.c_str());
                    match = true;
                    
                    // Submit the task
                    host->memory -= task->memory;
                    submit_task(task, slot->rank);
                    host->log_status();
                    
                    // Dequeue task
                    t = ready_tasks.erase(t);
                    
                    // Mark slot as busy
                    s = free_slots.erase(s);
                    
                    // so that the s++ in the loop doesn't skip one
                    s--;
                    
                    // This is to break out of the task loop so that we can 
                    // consider the next slot
                    break;
                }
            }
        }
    }
}

void Master::queue_ready_tasks() {
    while (this->engine->has_ready_task()) {
        Task *task = this->engine->next_ready_task();
        log_debug("Queueing task %s", task->name.c_str());
        ready_tasks.push_back(task);
    }
}

int Master::run() {
    // Start time of workflow
    struct timeval start;
    gettimeofday(&start, NULL);
    
    log_info("Master starting with %d workers", numworkers);
    
    register_workers();
    
    // Send out a unique path workers can use for their out/err files
    pid_t pid = getpid();
    char dotpid[10];
    snprintf(dotpid, 10, ".%d", pid);
    std::string worker_out_path = this->dagfile + dotpid + ".out";
    std::string worker_err_path = this->dagfile + dotpid + ".err";
    send_stdio_paths(worker_out_path, worker_err_path);
    
    // Check to make sure that there is at least one host capable
    // of executing every task
    for (DAG::iterator t = dag->begin(); t != dag->end(); t++){
        Task *task = (*t).second;
        
        // Check all the hosts for one that can run the task
        bool match = false;
        for (unsigned h=0; h<hosts.size(); h++) {
            Host *host = hosts[h];
            if (host->memory >= task->memory) {
                match = true;
                break;
            }
        }
        
        if (!match) {
            // There was no host found that was capable of executing the
            // task, so we must abort
            myfailure("FATAL ERROR: No host is capable of running task %s", 
                task->name.c_str());
        }
    }
    
    // Execute the workflow
    while (!this->engine->is_finished()) {
        queue_ready_tasks();
        schedule_tasks();
        wait_for_results();
    }
    
    log_info("Workflow finished");
    
    // Finish time of workflow
    struct timeval finish;
    gettimeofday(&finish, NULL);
    
    // Tell workers to exit
    log_trace("Sending workers shutdown messages");
    for (int i=1; i<=numworkers; i++) {
        send_shutdown(i);
    }
    
    log_trace("Waiting for workers to finish");
    
    double total_runtime = collect_total_runtimes();
    log_info("Total runtime of tasks: %f", total_runtime);
    
    double stime = start.tv_sec + (start.tv_usec/1000000.0);
    double ftime = finish.tv_sec + (finish.tv_usec/1000000.0);
    double walltime = ftime - stime;
    log_info("Wall time: %lf seconds", walltime);

    // Compute resource utilization
    double master_util = total_runtime / (walltime * (numworkers+1));
    double worker_util = total_runtime / (walltime * numworkers);
    if (total_runtime <= 0) {
        master_util = 0.0;
        worker_util = 0.0;
    }
    log_info("Resource utilization (with master): %lf", master_util);
    log_info("Resource utilization (without master): %lf", worker_util);
    
    // Merge stdout/stderr from all tasks
    log_trace("Merging stdio from workers");
    FILE *outf = stdout;
    if (outfile != "stdout") {
        outf = fopen(this->outfile.c_str(), "w");
        if (outf == NULL) {
            myfailures("Unable to open stdout file: %s\n", this->outfile.c_str());
        }
    }
    FILE *errf = stderr;
    if (errfile != "stderr") {
        errf = fopen(this->errfile.c_str(), "w");
        if (errf == NULL) {
            myfailures("Unable to open stderr file: %s\n", this->outfile.c_str());
        }
    }
    
    // Collect all stdout/stderr
    char dotrank[25];
    for (int i=1; i<=numworkers; i++) {
        sprintf(dotrank, ".%d", i);
        
        std::string toutfile = worker_out_path + dotrank;
        this->merge_task_stdio(outf, toutfile, "stdout");
        
        std::string terrfile = worker_err_path + dotrank;
        this->merge_task_stdio(errf, terrfile, "stderr");
    }
    
    // pegasus cluster output - used for provenance
    char summary[BUFSIZ];
    char stat[10];
    char date[32];
    if (this->engine->is_failed()) {
        strcpy(stat, "failed");
    }
    else {
        strcpy(stat, "ok");
    }
    iso2date(stime, date, sizeof(date));
    sprintf(summary, "[cluster-summary stat=\"%s\" tasks=%ld, succeeded=%ld, failed=%ld, extra=%d,"
                 " start=\"%s\", duration=%.3f, pid=%d, app=\"%s\", runtime=%.3f, slots=%d, cpus=%u,"
                 " utilization=%.3f]\n",
                 stat, 
                 this->total_count,
                 this->success_count, 
                 this->failed_count,
                 0,
                 date,
                 walltime,
                 getpid(),
                 this->program.c_str(),
                 total_runtime,
                 numworkers,
                 total_cpus,
                 worker_util);
    fwrite(summary, 1, strlen(summary), outf);
    
    if (errfile != "stderr") { 
        fclose(errf);
    }
    if (outfile != "stdout") {
        fclose(outf);
    }
    
    if (this->engine->max_failures_reached()) {
        log_error("Max failures reached: DAG prematurely aborted");
    }
    
    if (this->engine->is_failed()) {
        log_error("Workflow failed");
        return 1;
    } else {
        log_info("Workflow suceeded");
        return 0;
    }
}
