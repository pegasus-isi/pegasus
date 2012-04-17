#include <list>
#include "stdio.h"
#include "stdlib.h"
#include "mpi.h"
#include "unistd.h"

#include "dag.h"
#include "engine.h"
#include "master.h"
#include "worker.h"
#include "failure.h"
#include "log.h"
#include "protocol.h"

static char *program;
static int rank;

void usage() {
    if (rank == 0) {
        fprintf(stderr,
            "Usage: %s [options] DAGFILE\n"
            "\n"
            "Options:\n"
            "   -h|--help            Print this message\n"
            "   -v|--verbose         Increase logging level\n"
            "   -q|--quiet           Decrease logging level\n"
            "   -L|--logfile PATH    Path to log file\n"
            "   -o|--stdout PATH     Path to stdout file for tasks\n"
            "   -e|--stderr PATH     Path to stderr file for tasks\n"
            "   -s|--skip-rescue     Ignore existing rescue file (still creates one)\n"
            "   -m|--max-failures N  Stop submitting tasks after N tasks have failed\n"
            "   -t|--tries N         Try tasks N times before marking them failed\n",
            program
        );
    }
}

bool file_exists(const std::string &filename) {
    int readok = access(filename.c_str(), R_OK);
    if (readok == 0) {
        return true;
    } else {
        if (errno == ENOENT) {
            // File does not exist
            return false;
        } else {
            // It exists, but we can't access it
            myfailures("Error accessing file %s", filename.c_str());
        }
    }
    
    myfailure("Unreachable");
}

int next_retry_file(std::string &name) {
    std::string base = name;
    int i;
    for (i=0; i<=100; i++) {
        char rbuf[5];
        snprintf(rbuf, 5, ".%03d", i);
        name = base;
        name += rbuf;
        if (!file_exists(name)) {
            break;
        }
    }
    if (i >= 100) {
        myfailure("Too many retry files: %s", name.c_str());
    }
    return i;
}

int mpidag(int argc, char *argv[]) {
    int numprocs;
    program = argv[0];
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &numprocs);
    
    std::list<char *> flags;
    for (int i=1; i<argc; i++) {
        flags.push_back(argv[i]);
    }
    
    std::string outfile = "stdout";
    std::string errfile = "stderr";
    std::string logfile;
    std::list<std::string> args;
    int loglevel = LOG_INFO;
    bool skiprescue = false;
    int max_failures = 0;
    int tries = 1;
    
    while (flags.size() > 0) {
        std::string flag = flags.front();
        if (flag == "-h" || flag == "--help") {
            usage();
            return 0;
        } else if (flag == "-o" || flag == "--stdout") {
            flags.pop_front();
            if (flags.size() == 0) {
                if (rank == 0) {
                    fprintf(stderr, "-o/--stdout requires PATH\n");
                }
                return 1;
            }
            outfile = flags.front();
        } else if (flag == "-e" || flag == "--stderr") {
            flags.pop_front();
            if (flags.size() == 0) {
                if (rank == 0) {
                    fprintf(stderr, "-e/--stderr requires PATH\n");
                }
                return 1;
            }
            errfile = flags.front();
        } else if (flag == "-q" || flag == "--quiet") {
            loglevel -= 1;
        } else if (flag == "-v" || flag == "--verbose") {
            loglevel += 1;
        } else if (flag == "-L" || flag == "--logfile") {
            flags.pop_front();
            if (flags.size() == 0) {
                if (rank == 0) {
                    fprintf(stderr, "-L/--logfile requires PATH\n");
                }
                return 1;
            }
            logfile = flags.front();
        } else if (flag == "-s" || flag == "--skip-rescue") {
            skiprescue = true;
        } else if (flag == "-m" || flag == "--max-failures") {
            flags.pop_front();
            if (flags.size() == 0) {
                if (rank == 0) {
                    fprintf(stderr, "-m/--max-failures requires N\n");
                }
                return 1;
            }
            std::string N = flags.front();
            if (!sscanf(N.c_str(), "%d", &max_failures)) {
                fprintf(stderr, "N for -m/--max-failures is invalid\n");
                return 1;
            }
            if (max_failures < 0) {
                fprintf(stderr, "N for -m/--max-failures must be >= 0\n");
                return 1;
            }
        } else if (flag == "-t" || flag == "--tries") {
            flags.pop_front();
            if (flags.size() == 0) {
                if (rank == 0) {
                    fprintf(stderr, "-t/--tries requires N\n");
                }
                return 1;
            }
            std::string N = flags.front();
            if (!sscanf(N.c_str(), "%d", &tries)) {
                fprintf(stderr, "N for -t/--tries is invalid\n");
                return 1;
            }
            if (tries < 1) {
                fprintf(stderr, "N for -t/--tries must be >= 1\n");
                return 1;
            }
        } else if (flag[0] == '-') {
            if (rank == 0) {
                fprintf(stderr, "Unrecognized argument: %s\n", flag.c_str());
            }
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
        fprintf(stderr, "Invalid argument\n");
        return 1;
    }
    
    std::string dagfile = args.front();
    
    if (numprocs < 2) {
        fprintf(stderr, "At least one worker process is required\n");
        return 1;
    }
    
    // Everything is pretty deterministic up until the processes reach
    // this point. Once we get here the different processes can diverge 
    // in their behavior for many reasons (file systems issues, bad nodes,
    // etc.), so be careful how failures are handled after this point
    // and make sure MPI_Abort is called when something bad happens.
    
    char dotrank[25];
    sprintf(dotrank, ".%d", rank);
    
    FILE *log = NULL;
    log_set_level(loglevel);
    if (logfile.size() > 0) {
        logfile += dotrank;
        log = fopen(logfile.c_str(), "w");
        if (log == NULL) {
            myfailure("Unable to open log file: %s: %s\n", 
                logfile.c_str(), strerror(errno));
        }
        log_set_file(log);
    }
    
    try {
        if (rank == 0) {
            
            // IMPORTANT: The rank 0 process figures out the names
            // of these files so that we don't have 1000 workers all
            // slamming the file system with stat() calls to check if
            // the out/err/rescue files exist. The master will figure
            // it out here, and then broadcast it to the workers when
            // it starts up.
            
            // Determine old and new rescue files
            std::string rescuebase = dagfile;
            rescuebase += ".rescue";
            std::string oldrescue;
            std::string newrescue = rescuebase;
            int next = next_retry_file(newrescue);
            if (next == 0 || skiprescue) {
                // Either there is no old rescue file, or the
                // user doesnt want to read it.
                oldrescue = "";
            } else {
                char rbuf[5];
                snprintf(rbuf, 5, ".%03d", next-1);
                oldrescue = rescuebase;
                oldrescue += rbuf;
            }
            log_debug("Using old rescue file: %s", oldrescue.c_str());
            log_debug("Using new rescue file: %s", newrescue.c_str());
            
            DAG dag(dagfile, oldrescue);
            Engine engine(dag, newrescue, max_failures, tries);
            
            return Master(program, engine, dag, dagfile, outfile, errfile).run();
        } else {
            return Worker().run();
        }
    } catch (...) {
        // Make sure we close the log
        if (log != NULL) {
            fclose(log);
        }
        throw;
    }
}

int main(int argc, char *argv[]) {
    try {
        MPI_Init(&argc, &argv);
        protocol_request_struct();
        protocol_response_struct();
        int rc = mpidag(argc, argv);
        MPI_Finalize();
        return rc;
    } catch (std::exception &error) {
        // If we catch an execption here, then one of the
        // processes has hit an unsolvable problem and we
        // need to abort the entire workflow.
        fprintf(stderr, "ABORT: %s\n", error.what());
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
}
