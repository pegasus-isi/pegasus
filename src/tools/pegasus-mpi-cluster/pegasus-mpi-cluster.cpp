/* mpi.h must come before stdio.h for Intel MPI */
#include <mpi.h>
#include <stdio.h>
#include <list>
#include <stdlib.h>
#include <unistd.h>
#include <math.h>

#include "version.h"
#include "dag.h"
#include "engine.h"
#include "master.h"
#include "worker.h"
#include "failure.h"
#include "log.h"
#include "mpicomm.h"
#include "protocol.h"
#include "tools.h"
#include "config.h"

using std::string;
using std::list;
using std::exception;

static char *program = NULL;
static int rank = 0;

void version() {
    if (rank == 0) {
#ifdef PEGASUS_VERSION
        fprintf(stderr, "Version: %s\n", PEGASUS_VERSION);
#endif
        fprintf(stderr, "Compiled: %s %s\n", __DATE__, __TIME__);
#ifdef __VERSION__
        fprintf(stderr, "Compiler: %s\n", __VERSION__);
#endif
        int major, minor;
        MPI_Get_version(&major, &minor);
        fprintf(stderr, "MPI: %d.%d\n", major, minor);
#ifdef MPICH_VERSION
        fprintf(stderr, "MPICH: %s\n", MPICH_VERSION);
#endif
#ifdef MPICH2_VERSION
        fprintf(stderr, "MPICH2: %s\n", MPICH2_VERSION);
#endif
#ifdef MVAPICH2_VERSION
        fprintf(stderr, "MVAPICH2: %s\n", MVAPICH2_VERSION);
#endif
#ifdef OPEN_MPI
        fprintf(stderr, "OpenMPI: %d.%d.%d\n", OMPI_MAJOR_VERSION, OMPI_MINOR_VERSION, OMPI_RELEASE_VERSION);
#endif
    }
}

void usage() {
    if (rank == 0) {
        fprintf(stderr,
            "Usage: %s [options] DAGFILE\n"
            "\n"
            "Options:\n"
            "   -h|--help            Print this message\n"
            "   -V|--version         Print version information\n"
            "   -v|--verbose         Increase logging level\n"
            "   -q|--quiet           Decrease logging level\n"
            "   -o|--stdout PATH     Path to stdout file for tasks\n"
            "   -e|--stderr PATH     Path to stderr file for tasks\n"
            "   -s|--skip-rescue     Ignore existing rescue file (still creates one)\n"
            "   -m|--max-failures N  Stop submitting tasks after N tasks have failed\n"
            "   -t|--tries N         Try tasks N times before marking them failed\n"
            "   -n|--nolock          Do not try to lock DAGFILE\n"
            "   -r|--rescue PATH     Path to rescue log [default: DAGFILE.rescue]\n"
            "   --host-script PATH   Path to script that will be launched on each host\n"
            "   --host-memory N      Amount of memory per host in MB\n"
            "   --host-cpus N        Number of CPUs per host\n"
            "   --strict-limits      Enforce strict task resource limits\n"
            "   --max-wall-time T    Maximum wall time of the job in minutes\n"
            "   --per-task-stdio     Write each task's stdout/stderr to a different file\n"
            "   --jobstate-log       Generate jobstate.log\n"
            "   --monitord-hack      Generate a .dagman.out file to trick monitord\n"
            "   --no-resource-log    Do not generate a log of resource usage\n"
            "   --no-sleep-on-recv   Do not sleep on message receive\n"
            "   --maxfds             Maximum cached file descriptors\n"
            "   --keep-affinity      Keep inherited CPU and memory affinity\n"
            "   --set-affinity       Set CPU affinity for multicore tasks\n",
            program
        );
    }
}

void argerror(const string message) {
    if (rank == 0) {
        fprintf(stderr, "%s\n", message.c_str());
    }
}

int mpidag(int argc, char *argv[], MPICommunicator &comm) {
    rank = comm.rank();
    int numprocs = comm.size();

    list<char *> flags;
    for (int i=1; i<argc; i++) {
        flags.push_back(argv[i]);
    }

    string outfile = "stdout";
    string errfile = "stderr";
    list<string> args;
    int loglevel = LOG_INFO;
    bool skiprescue = false;
    int max_failures = 0;
    int tries = 1;
    bool lock = true;
    string rescuefile = "";
    string host_script = "";
    unsigned host_memory = 0;
    cpu_t host_cpus = 0;
    bool strict_limits = false;
    double max_wall_time = 0.0;
    bool per_task_stdio = false;
    bool jobstate_log = false;
    bool monitord_hack = false;
    bool log_resources = true;
    bool sleep_on_recv = true;
    int maxfds = 0;
    bool clear_affinity = true;
    config.set_affinity = false;

    // Environment variable defaults
    char *env_host_script = getenv("PMC_HOST_SCRIPT");
    if (env_host_script != NULL) {
        host_script = env_host_script;
    }

    char *env_host_memory = getenv("PMC_HOST_MEMORY");
    if (env_host_memory != NULL) {
        if (sscanf(env_host_memory, "%u", &host_memory) != 1) {
            argerror("Invalid value for PMC_HOST_MEMORY");
            return 1;
        }
    }

    char *env_host_cpus = getenv("PMC_HOST_CPUS");
    if (env_host_cpus != NULL) {
        if (sscanf(env_host_cpus, "%" SCNcpu_t, &host_cpus) != 1) {
            argerror("Invalid value for PMC_HOST_CPUS");
            return 1;
        }
    }

    char *env_max_wall_time = getenv("PMC_MAX_WALL_TIME");
    if (env_max_wall_time != NULL) {
        if (sscanf(env_max_wall_time, "%lf", &max_wall_time) != 1) {
            argerror("Invalid value for PMC_MAX_WALL_TIME");
            return 1;
        }
    }

    while (flags.size() > 0) {
        string flag = flags.front();
        if (flag == "-h" || flag == "--help") {
            // This should no longer be reachable because
            // of the code in main()
            usage();
            return 0;
        } else if (flag == "-V" || flag == "--version") {
            version();
            return 0;
        } else if (flag == "-o" || flag == "--stdout") {
            flags.pop_front();
            if (flags.size() == 0) {
                argerror("-o/--stdout requires PATH");
                return 1;
            }
            outfile = flags.front();
        } else if (flag == "-e" || flag == "--stderr") {
            flags.pop_front();
            if (flags.size() == 0) {
                argerror("-e/--stderr requires PATH");
                return 1;
            }
            errfile = flags.front();
        } else if (flag == "-q" || flag == "--quiet") {
            loglevel -= 1;
        } else if (flag == "-v" || flag == "--verbose") {
            loglevel += 1;
        } else if (flag == "-s" || flag == "--skip-rescue") {
            skiprescue = true;
        } else if (flag == "-m" || flag == "--max-failures") {
            flags.pop_front();
            if (flags.size() == 0) {
                argerror("-m/--max-failures requires N");
                return 1;
            }
            string N = flags.front();
            if (!sscanf(N.c_str(), "%d", &max_failures)) {
                argerror("N for -m/--max-failures is invalid");
                return 1;
            }
            if (max_failures < 0) {
                argerror("N for -m/--max-failures must be >= 0");
                return 1;
            }
        } else if (flag == "-t" || flag == "--tries") {
            flags.pop_front();
            if (flags.size() == 0) {
                argerror("-t/--tries requires N");
                return 1;
            }
            string N = flags.front();
            if (!sscanf(N.c_str(), "%d", &tries)) {
                argerror("N for -t/--tries is invalid");
                return 1;
            }
            if (tries < 1) {
                argerror("N for -t/--tries must be >= 1");
                return 1;
            }
        } else if (flag == "-n" || flag == "--nolock") {
            lock = false;
        } else if (flag == "-r" || flag == "--rescue") {
            flags.pop_front();
            if (flags.size() == 0) {
                argerror("-r/--rescue requires PATH");
                return 1;
            }
            rescuefile = flags.front();
        } else if (flag == "--host-script") {
            flags.pop_front();
            if (flags.size() == 0) {
                argerror("--host-script requires PATH");
                return 1;
            }
            host_script = flags.front();
        } else if (flag == "--host-memory") {
            flags.pop_front();
            if (flags.size() == 0) {
                argerror("--host-memory requires N");
                return 1;
            }
            string host_memory_string = flags.front();
            if (sscanf(host_memory_string.c_str(), "%u", &host_memory) != 1) {
                argerror("Invalid value for --host-memory");
                return 1;
            }
            if (host_memory < 1) {
                argerror("--host-memory must be an integer >= 1");
                return 1;
            }
        } else if (flag == "--host-cpus") {
            flags.pop_front();
            if (flags.size() == 0) {
                argerror("--host-cpus requires N");
                return 1;
            }
            string host_cpus_string = flags.front();
            if (sscanf(host_cpus_string.c_str(), "%" SCNcpu_t, &host_cpus) != 1) {
                argerror("Invalid value for --host-cpus");
                return 1;
            }
        } else if (flag == "--strict-limits") {
            strict_limits = true;
        } else if (flag == "--max-wall-time") {
            flags.pop_front();
            if (flags.size() == 0) {
                argerror("--max-wall-time requires T");
                return 1;
            }
            string max_wall_time_string = flags.front();
            if (sscanf(max_wall_time_string.c_str(), "%lf", &max_wall_time) != 1) {
                argerror("Invalid value for --max-wall-time");
                return 1;
            }
        } else if (flag == "--per-task-stdio") {
            per_task_stdio = true;
        } else if (flag == "--jobstate-log") {
            jobstate_log = true;
        } else if (flag == "--monitord-hack") {
            monitord_hack = true;
            per_task_stdio = true;
        } else if (flag == "--no-resource-log") {
            log_resources = false;
        } else if (flag == "--no-sleep-on-recv") {
            sleep_on_recv = false;
        } else if (flag == "--maxfds") {
            flags.pop_front();
            if (flags.size() == 0) {
                argerror("--maxfds requires N");
                return 1;
            }
            string maxfds_string = flags.front();
            if (sscanf(maxfds_string.c_str(), "%d", &maxfds) != 1) {
                argerror("Invalid value for --maxfds");
                return 1;
            }
            if (maxfds < 1) {
                argerror("--maxfds must be at least 1");
                return 1;
            }
        } else if (flag == "--keep-affinity") {
            clear_affinity = false;
        } else if (flag == "--set-affinity") {
            config.set_affinity = true;
        } else if (flag[0] == '-') {
            string message = "Unrecognized argument: ";
            message += flag;
            argerror(message);
            return 1;
        } else {
            args.push_back(flag);
        }
        flags.pop_front();
    }

    if (args.size() == 0) {
        usage();
        return 1;
    }

    if (args.size() > 1) {
        argerror("Invalid argument\n");
        return 1;
    }

    string dagfile = args.front();

    log_set_level(loglevel);

    if (numprocs < 2) {
        fprintf(stderr, "At least one worker process is required\n");
        return 1;
    }

    // You can't specify --host-cpus and --set-affinity because that would
    // break stuff
    if (config.set_affinity && host_cpus > 0) {
        fprintf(stderr, "--host-cpus is not compatible with --set-affinity\n");
        return 1;
    }

    comm.sleep_on_recv = sleep_on_recv;

    version();

    // Unless the user specifies otherwise, we clear the CPU and memory
    // affinity settings on Linux. This is the default because some systems
    // are not configured correctly and bind all the processes to one CPU,
    // and we can't expect users to know that this is happening.
    if (clear_affinity || config.set_affinity) {
        log_debug("Rank %d: Clearing CPU and memory affinity", rank);
        if (clear_cpu_affinity() < 0) {
            log_error("Rank %d: Error clearing CPU affinity: %s",
                      rank, strerror(errno));
            return 1;
        }
        if (clear_memory_affinity() < 0) {
            log_debug("Rank %d: Error clearing memory affinity: %s",
                      rank, strerror(errno));
        }
    }

    // Everything is pretty deterministic up until the processes reach
    // this point. Once we get here the different processes can diverge 
    // in their behavior for many reasons (file systems issues, bad nodes,
    // etc.), so be careful how failures are handled after this point
    // and make sure MPI_Abort is called when something bad happens.

    if (rank == 0) {

        // If no rescue file specified, use default
        if (rescuefile == "") {
            rescuefile = dagfile + ".rescue";
        }

        string oldrescue = rescuefile;
        string newrescue = rescuefile;

        if (skiprescue) {
            // User does not want to read old rescue file
            oldrescue = "";
        }

        log_debug("Using old rescue file: %s", oldrescue.c_str());
        log_debug("Using new rescue file: %s", newrescue.c_str());

        string resource_log;
        if (log_resources) {
            resource_log = dagfile + ".resource";
        }

        bool has_host_script = ("" != host_script);

        DAG dag(dagfile, oldrescue, lock, tries);
        Engine engine(dag, newrescue, max_failures);
        Master master(&comm, program, engine, dag, dagfile, outfile, errfile,
                has_host_script, max_wall_time, resource_log, per_task_stdio,
                maxfds);

        string jobstate_path = dirname(dagfile) + "/jobstate.log";
        JobstateLog jslog(jobstate_path);
        if (jobstate_log) {
            master.add_listener(&jslog);
        }

        DAGManLog dagmanlog(dagfile + ".dagman.out", dagfile);
        if (monitord_hack) {
            master.add_listener(&dagmanlog);
        }

        return master.run();
    } else {

        Worker worker(&comm, dagfile, host_script, host_memory, host_cpus, 
                strict_limits, per_task_stdio);

        return worker.run();
    }
}

void out_of_memory() {
    myfailure("Unable to allocate memory");
}

int main(int argc, char *argv[]) {
    program = argv[0];

    // Shortcut to make 'help' work when running without an MPI context
    if (argc < 2) {
        usage();
        return 1;
    }
    for (int i=1; i<argc; i++) {
        string flag = argv[i];
        if (flag == "-h" || flag == "--help") {
            usage();
            return 0;
        }
    }

    MPICommunicator comm(&argc, &argv);
    try {
        std::set_new_handler(out_of_memory);
        int rc = mpidag(argc, argv, comm);
        return rc;
    } catch (exception &error) {
        // If we catch an execption here, then one of the
        // processes has hit an unsolvable problem and we
        // need to abort the entire workflow.
        fprintf(stderr, "ABORT: %s\n", error.what());
        // ensure that abort() is not eating our errors
        fflush(stdout);
        fflush(stderr);
        sleep(1);
        comm.abort(1);
    }
}

