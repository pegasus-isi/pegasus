#include "stdio.h"
#include "unistd.h"
#include "sys/time.h"
#include "mpi.h"

#include "master.h"
#include "failure.h"
#include "protocol.h"
#include "log.h"
#include "tools.h"

#include <sstream>

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
}

Master::~Master() {
}

void Master::submit_task(Task *task, int worker) {
    log_debug("Submitting task %s to worker %d", task->name.c_str(), worker);
    send_request(task->name, task->command, task->extra_id, worker);
    
    this->total_count++;
}

void Master::wait_for_result() {
    log_trace("Waiting for task to finish");
    
    std::string name;
    int exitcode;
    int worker;
    recv_response(name, exitcode, worker);
    
    // Mark worker idle
    log_trace("Worker %d is idle", worker);
    this->mark_worker_idle(worker);
    
    // Mark task finished
    if (exitcode == 0) {
        log_debug("Task %s finished with exitcode %d", name.c_str(), exitcode);
        this->success_count++;
    } else {
        log_error("Task %s failed with exitcode %d", name.c_str(), exitcode);
        this->failed_count++;
    }
    Task *t = this->dag->get_task(name);
    this->engine->mark_task_finished(t, exitcode);
}

void Master::add_worker(int worker) {
    this->mark_worker_idle(worker);
}

bool Master::has_idle_worker() {
    return !this->idle.empty();
}

int Master::next_idle_worker() {
    if (!this->has_idle_worker()) {
        myfailure("No idle workers");
    }
    int worker = this->idle.front();
    this->idle.pop();
    return worker;
}

void Master::mark_worker_idle(int worker) {
    this->idle.push(worker);
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

int Master::run() {
    // Start time of workflow
    struct timeval start;
    gettimeofday(&start, NULL);

    int numprocs;
    MPI_Comm_size(MPI_COMM_WORLD, &numprocs);
    
    int numworkers = numprocs - 1;
    if (numworkers == 0) {
        myfailure("Need at least 1 worker");
    }
    
    log_info("Master starting with %d workers", numworkers);
    
    // First, send out a unique path workers can use for their out/err files
    pid_t pid = getpid();
    char dotpid[10];
    snprintf(dotpid, 10, ".%d", pid);
    std::string worker_out_path = this->dagfile + dotpid + ".out";
    std::string worker_err_path = this->dagfile + dotpid + ".err";
    send_stdio_paths(worker_out_path, worker_err_path);
    
    // Queue up the workers
    for (int i=1; i<=numworkers; i++) {
        this->add_worker(i);
    }
    
    // While DAG has tasks to run
    while (!this->engine->is_finished()) {
        
        // Submit as many tasks as we can
        while (this->engine->has_ready_task() && this->has_idle_worker()) {
            int worker = this->next_idle_worker();
            Task *task = this->engine->next_ready_task();
            this->submit_task(task, worker);
        }
        
        if (!this->engine->has_ready_task()) {
            log_debug("No ready tasks");
        }
        
        if (!this->has_idle_worker()) {
            log_debug("No idle workers");
        }
        
        this->wait_for_result();
    }
    
    log_info("Workflow finished");
    
    // Finish time of workflow
    struct timeval finish;
    gettimeofday(&finish, NULL);
    
    // Tell workers to exit
    // TODO Change this to MPI_Bcast
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
    if (total_runtime > 0) {
        double master_util = total_runtime / (walltime * numprocs);
        double worker_util = total_runtime / (walltime * numworkers);
        log_info("Resource utilization (with master): %lf", master_util);
        log_info("Resource utilization (without master): %lf", worker_util);
    }

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
    char buf[BUFSIZ];
    char stat[10];
    char date[32];
    if (this->engine->is_failed()) {
        strcpy(stat, "failed");
    }
    else {
        strcpy(stat, "ok");
    }
    iso2date(stime, date, sizeof(date));
    sprintf(buf, "[cluster-summary stat=\"%s\" tasks=%ld, succeeded=%ld, failed=%ld, extra=%d,"
                 " start=\"%s\", duration=%.3f, pid=%d, app=\"%s\"]\n",
                 stat, 
                 this->total_count,
                 this->success_count, 
                 this->failed_count,
                 0,
                 date,
                 walltime * numprocs, /* duration is for all cores */
                 getpid(),
                 this->program.c_str());
    fwrite(buf, 1, strlen(buf), outf);
    
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
