#include <map>
#include <stdio.h>
#include <unistd.h>
#include <mpi.h>
#include <signal.h>
#include <math.h>
#include <sys/time.h>

#include "master.h"
#include "failure.h"
#include "protocol.h"
#include "log.h"
#include "tools.h"

static bool ABORT = false;

static void on_signal(int signo) {
    log_error("Caught signal %d", signo);
    ABORT = true;
}

void Host::log_status() {
    log_trace("Host %s now has %u MB, %u CPUs, and %u slots free", 
        this->host_name.c_str(), this->memory, this->cpus, this->slots);
}

Master::Master(const std::string &program, Engine &engine, DAG &dag,
               const std::string &dagfile, const std::string &outfile,
               const std::string &errfile, bool has_host_script,
               double max_wall_time, const std::string &resourcefile) {
    this->program = program;
    this->dagfile = dagfile;
    this->outfile = outfile;
    this->errfile = errfile;
    this->engine = &engine;
    this->dag = &dag;
    this->has_host_script = has_host_script;
    this->max_wall_time = max_wall_time;
    
    this->total_count = 0;
    this->success_count = 0;
    this->failed_count = 0;
    
    this->start_time = 0.0;
    this->finish_time = 0.0;
    this->wall_time = 0.0;
    
    this->total_cpus = 0;
    this->total_runtime = 0.0;

    this->memory_avail = 0;
    this->cpus_avail = 0;
    this->slots_avail = 0;

    // Determine the number of workers we have
    int numprocs;
    MPI_Comm_size(MPI_COMM_WORLD, &numprocs);
    this->numworkers = numprocs - 1;
    if (numworkers == 0) {
        myfailure("Need at least 1 worker");
    }
    
    // Open task stdout
    this->task_stdout = stdout;
    if (outfile != "stdout") {
        this->task_stdout = fopen(this->outfile.c_str(), "w");
        if (this->task_stdout == NULL) {
            myfailures("Unable to open stdout file: %s\n", this->outfile.c_str());
        }
    }
    
    // Open task stderr
    this->task_stderr = stderr;
    if (errfile != "stderr") {
        this->task_stderr = fopen(this->errfile.c_str(), "w");
        if (this->task_stderr == NULL) {
            myfailures("Unable to open stderr file: %s\n", this->outfile.c_str());
        }
    }
    
    // Set the worker stdout/stderr paths
    pid_t pid = getpid();
    char dotpid[10];
    snprintf(dotpid, 10, ".%d.", pid);
    this->worker_out_path = this->dagfile + dotpid + "out";
    this->worker_err_path = this->dagfile + dotpid + "err";

    if (resourcefile == "") {
        this->resource_log = NULL;
    } else {
        this->resource_log = fopen(resourcefile.c_str(), "a");
    }

}

Master::~Master() {
    if (errfile != "stderr") {
        fclose(task_stderr);
    }
    
    if (outfile != "stdout") {
        fclose(task_stdout);
    }
    
    std::vector<Slot *>::iterator s;
    for (s = slots.begin(); s != slots.end(); s++) {
        delete *s;
    }
    
    std::vector<Host *>::iterator h;
    for (h = hosts.begin(); h != hosts.end(); h++) {
        delete *h;
    }

    if (resource_log != NULL) {
        fclose(resource_log);
    }
}

void Master::submit_task(Task *task, int rank) {
    log_debug("Submitting task %s to slot %d", task->name.c_str(), rank);
    send_request(task->name, task->command, task->pegasus_id, task->memory, task->cpus, rank);
    
    this->total_count++;
}

void Master::wait_for_results() {
    // This will process all the waiting responses. If there are none 
    // waiting, then it will block until one arrives. If there are 
    // several waiting, then it will process them all and return without 
    // waiting.
    unsigned int tasks = 0;
    do {
        
        /* In many MPI implementations MPI_Recv uses a busy wait loop. This
         * really wreaks havoc on the load and CPU utilization of the master
         * when there are no tasks to schedule or all slots are busy. In order 
         * to avoid that we check here to see if there are any responses first, 
         * and if there are not, then we wait for a few millis before checking 
         * again and keep doing that until there is a response waiting. This 
         * should reduce the load/CPU usage on master significantly. It decreases
         * responsiveness a bit, but it is a fair tradeoff.
         *
         * Another issue is that if the user specifies a maximum wall time for
         * the workflow, then the master sets a timeout by calling alarm(), which 
         * causes the kernel to send a SIGALRM when the timer expires. Also, 
         * on most PBS systems when the max wall time is reached PBS sends the 
         * process a SIGTERM. We can catch these signals, however, in many MPI 
         * implementations signals do not interrupt blocking message calls such
         * as MPI_Recv. So we cannot be waiting in MPI_Recv when the signal is
         * caught or we cannot respond to it. So we wait in this sleep loop and
         * check the flag that is set by the signal handlers to detect when the
         * master needs to abort the workflow.
         */
        while (!ABORT && !response_waiting()) {
            usleep(NO_MESSAGE_SLEEP_TIME);    
        }
        
        if (ABORT) {
            // If ABORT is true, then we caught a signal and need to 
            // abort the workflow, so return without processing any 
            // more results.
            return;
        }
        
        process_result();
        tasks++;
    } while (response_waiting());
    log_trace("Processed %u task(s) this cycle", tasks);
}

void Master::process_result() {
    log_trace("Waiting for task to finish");
    
    std::string name;
    int exitcode;
    int rank;
    double task_runtime;
    recv_response(name, exitcode, task_runtime, rank);
    
    total_runtime += task_runtime;
    
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
    release_resources(slot->host, task->cpus, task->memory);

    // Mark slot as free
    free_slots.push_back(slot);
}

void Master::allocate_resources(Host *host, unsigned cpus, unsigned memory) {

    memory_avail -= memory;
    cpus_avail -= cpus;
    slots_avail -= 1;

    host->memory -= memory;
    host->cpus -= cpus;
    host->slots -= 1;

    host->log_status();
    log_resources(host->slots, host->cpus, host->memory, host->host_name);
    log_resources(slots_avail, cpus_avail, memory_avail, "*");
}

void Master::release_resources(Host *host, unsigned cpus, unsigned memory) {

    memory_avail += memory;
    cpus_avail += cpus;
    slots_avail += 1;

    host->cpus += cpus;
    host->memory += memory;
    host->slots += 1;

    host->log_status();
    log_resources(host->slots, host->cpus, host->memory, host->host_name);
    log_resources(slots_avail, cpus_avail, memory_avail, "*");
}

void Master::log_resources(unsigned slots, unsigned cpus, unsigned memory, const std::string &hostname) {
    if (resource_log == NULL) {
        return;
    }

    struct timeval ts;

    gettimeofday(&ts, NULL);
    double timestamp = ts.tv_sec + (ts.tv_usec / 1.0e6);

    fprintf(resource_log, "%lf,%u,%u,%u,%s\n", 
            timestamp, slots, cpus, memory, hostname.c_str());
}

void Master::merge_all_task_stdio() {
    log_trace("Merging stdio from workers");
    char dotrank[25];
    for (int i=1; i<=numworkers; i++) {
        sprintf(dotrank, ".%d", i);
        
        std::string task_outfile = worker_out_path + dotrank;
        this->merge_task_stdio(task_stdout, task_outfile, "stdout");
        
        std::string task_errfile = worker_err_path + dotrank;
        this->merge_task_stdio(task_stderr, task_errfile, "stderr");
    }
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

void Master::write_cluster_summary(bool failed) {
    // pegasus cluster output - used for provenance
    char stat[10];
    if (failed) {
        strcpy(stat, "failed");
    } else {
        strcpy(stat, "ok");
    }
    
    char date[32];
    iso2date(start_time, date, sizeof(date));
    
    int extra = 0;
    
    char summary[BUFSIZ];
    sprintf(summary, "[cluster-summary stat=\"%s\" tasks=%ld, succeeded=%ld, failed=%ld, extra=%d,"
                 " start=\"%s\", duration=%.3f, pid=%d, app=\"%s\", runtime=%.3f, slots=%d, cpus=%u]\n",
                 stat, 
                 this->total_count,
                 this->success_count, 
                 this->failed_count,
                 extra,
                 date,
                 wall_time,
                 getpid(),
                 this->program.c_str(),
                 total_runtime,
                 this->numworkers,
                 this->total_cpus);
    
    int len = strlen(summary);
    int w = fwrite(summary, 1, len, task_stdout);
    if (w < len) {
        myfailures("Error writing cluster-summary");
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
        
        if (hostmap.find(hostname) == hostmap.end()) {
            // If the host is not found, create a new one
            log_debug("Got new host: name=%s, mem=%u, cpus=%u", hostname.c_str(), memory, cpus);
            Host *newhost = new Host(hostname, memory, cpus);
            hosts.push_back(newhost);
            hostmap[hostname] = newhost;
            
            total_cpus += cpus;
            cpus_avail += cpus;
            memory_avail += memory;
        } else {
            // Otherwise, increment the number of slots available
            Host *host = hostmap[hostname];
            host->slots += 1;
        }
        
        slots_avail += 1;
        
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
    
    // Log the initial resource availability
    for (std::vector<Host *>::iterator i = hosts.begin(); i!=hosts.end(); i++) {
        Host *host = *i;
        log_resources(host->slots, host->cpus, host->memory, host->host_name);
    }
    log_resources(slots_avail, cpus_avail, memory_avail, "*");
}

void Master::schedule_tasks() {
    log_debug("Scheduling %d tasks on %d slots...", 
        ready_queue.size(), free_slots.size());
    
    int scheduled = 0;
    TaskList deferred_tasks;
    
    while (ready_queue.size() > 0 && free_slots.size() > 0) {
        Task *task = ready_queue.top();
        ready_queue.pop();
        
        log_trace("Scheduling task %s", task->name.c_str());
        
        bool match = false;
        
        for (SlotList::iterator s = free_slots.begin(); s != free_slots.end(); s++) {
            Slot *slot = *s;
            Host *host = slot->host;
            
            // If the task fits, schedule it
            if (host->memory >= task->memory && host->cpus >= task->cpus) {
                
                log_trace("Matched task %s to slot %d on host %s", 
                    task->name.c_str(), slot->rank, host->host_name.c_str());
                
                // Reserve the resources
                allocate_resources(host, task->cpus, task->memory);
                
                submit_task(task, slot->rank);
                
                s = free_slots.erase(s);
                
                // so that the s++ in the loop doesn't skip one
                s--;
                
                match = true;
                scheduled += 1;
                
                // This is to break out of the slot loop so that we can 
                // consider the next task
                break;
            }
        }
        
        if (!match) {
            // If the task could not be scheduled, then we save it 
            // and move on to the next one. It will be requeued later.
            log_trace("No slot found for task %s", task->name.c_str());
            deferred_tasks.push_back(task);
        }
    }
    
    log_debug("Scheduled %d tasks and deferred %d tasks", scheduled, deferred_tasks.size());
    
    // Requeue all the deferred tasks
    for (TaskList::iterator t = deferred_tasks.begin(); t != deferred_tasks.end(); t++) {
        ready_queue.push(*t);
    }
}

void Master::queue_ready_tasks() {
    while (this->engine->has_ready_task()) {
        Task *task = this->engine->next_ready_task();
        log_debug("Queueing task %s", task->name.c_str());
        ready_queue.push(task);
    }
}

int Master::run() {
    log_info("Master starting with %d workers", numworkers);
    
    start_time = current_time();
    
    // Install signal handlers
    struct sigaction signal_action;
    signal_action.sa_handler = on_signal;
    signal_action.sa_flags = SA_NODEFER;
    if (sigaction(SIGALRM, &signal_action, NULL) < 0) {
        myfailures("Unable to set signal handler for SIGALRM");
    }
    if (sigaction(SIGTERM, &signal_action, NULL) < 0) {
        myfailures("Unable to set signal handler for SIGTERM");
    }
    
    // Set alarm to interrupt the master when the walltime is up
    if (this->max_wall_time > 0.0) {    
        log_info("Setting max walltime to %lf minutes", this->max_wall_time);
        alarm((unsigned)ceil(max_wall_time * 60.0));
    }
    
    register_workers();
    
    // Send out a unique path workers can use for their out/err files
    send_stdio_paths(worker_out_path, worker_err_path);
    
    // Check to make sure that there is at least one host capable
    // of executing every task
    for (DAG::iterator t = dag->begin(); t != dag->end(); t++){
        Task *task = (*t).second;
        
        // Check all the hosts for one that can run the task
        bool match = false;
        for (unsigned h=0; h<hosts.size(); h++) {
            Host *host = hosts[h];
            if (host->memory >= task->memory && host->cpus >= task->cpus) {
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
    
    // If there is a host script, wait here for it to run
    if (has_host_script) {
        MPI_Barrier(MPI_COMM_WORLD);
    }
    
    log_info("Starting workflow");
    
    // Keep executing tasks until the workflow is finished or the master
    // needs to abort the workflow due to a signal being caught
    while (!this->engine->is_finished() && !ABORT) {
        queue_ready_tasks();
        schedule_tasks();
        wait_for_results();
    }
    
    if (ABORT) {
        log_error("Aborting workflow");
    } else {
        log_info("Workflow finished");
    }
    
    if (this->engine->max_failures_reached()) {
        log_error("Max failures reached: DAG prematurely aborted");
    }
    
    merge_all_task_stdio();
    
    // This must be done before write_cluster_summary so that the
    // wall time can be recorded in the cluster-summary record
    finish_time = current_time();
    wall_time = finish_time - start_time;
    log_info("Wall time: %lf minutes", wall_time/60.0);
    
    bool failed = ABORT || this->engine->is_failed();
    write_cluster_summary(failed);
    
    // Compute resource utilization
    double master_util = total_runtime / (wall_time * (numworkers+1));
    double worker_util = total_runtime / (wall_time * numworkers);
    if (total_runtime <= 0) {
        master_util = 0.0;
        worker_util = 0.0;
    }
    log_info("Resource utilization (with master): %lf", master_util);
    log_info("Resource utilization (without master): %lf", worker_util);
    
    log_info("Total runtime of tasks: %f", total_runtime);
    
    log_trace("Sending workers shutdown messages");
    for (int i=1; i<=numworkers; i++) {
        send_shutdown(i);
    }
    
    if (ABORT) {
        myfailure("Workflow aborted");
    } else if (failed) {
        log_error("Workflow failed");
        return 1;
    } else {
        log_info("Workflow suceeded");
        return 0;
    }
}
