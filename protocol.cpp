#include "string.h"
#include "mpi.h"

#include "protocol.h"

#define MAX_MESSAGE 16384

static char buf[MAX_MESSAGE];

struct task_request_t task_request;
MPI_Datatype mpi_task_request_t;

struct task_response_t task_response;
MPI_Datatype mpi_task_response_t;

// XXX This protocol is really, really terrible. Make something better.

void protocol_request_struct() {

    int blocklengths[3];
    MPI_Datatype types[3];
    MPI_Aint displacements[3];

    MPI_Aint mpi_addr_struct;
    MPI_Aint mpi_addr_name;
    MPI_Aint mpi_addr_command;
    MPI_Aint mpi_addr_extra_id;

    MPI_Get_address(&task_request, &mpi_addr_struct);
    MPI_Get_address(&task_request.name, &mpi_addr_name);
    MPI_Get_address(&task_request.command, &mpi_addr_command);
    MPI_Get_address(&task_request.extra_id, &mpi_addr_extra_id);

    types[0] = MPI_CHAR;
    types[1] = MPI_CHAR;
    types[2] = MPI_CHAR;

    blocklengths[0]= 50;
    blocklengths[1]= 5000;
    blocklengths[2]= 50; 

    displacements[0]= 0;
    displacements[1]= mpi_addr_command - mpi_addr_struct;
    displacements[2]= mpi_addr_extra_id - mpi_addr_struct;

    MPI_Type_create_struct(3, blocklengths, displacements, types, &mpi_task_request_t);
    MPI_Type_commit(&mpi_task_request_t);
}


void protocol_response_struct() {

    int blocklengths[4];
    MPI_Datatype types[4];
    MPI_Aint displacements[4];

    MPI_Aint mpi_addr_struct;
    MPI_Aint mpi_addr_name;
    MPI_Aint mpi_addr_start_time;
    MPI_Aint mpi_addr_end_time;
    MPI_Aint mpi_addr_exit_code;

    MPI_Get_address(&task_response, &mpi_addr_struct);
    MPI_Get_address(&task_response.name, &mpi_addr_name);
    MPI_Get_address(&task_response.start_time, &mpi_addr_start_time);
    MPI_Get_address(&task_response.end_time, &mpi_addr_end_time);
    MPI_Get_address(&task_response.exit_code, &mpi_addr_exit_code);

    types[0] = MPI_CHAR;
    types[1] = MPI_DOUBLE;
    types[2] = MPI_DOUBLE;
    types[3] = MPI_INT;

    blocklengths[0]= 50;
    blocklengths[1]= 1;
    blocklengths[2]= 1; 
    blocklengths[3]= 1; 

    displacements[0]= 0;
    displacements[1]= mpi_addr_start_time - mpi_addr_struct;
    displacements[2]= mpi_addr_end_time - mpi_addr_struct;
    displacements[3]= mpi_addr_exit_code - mpi_addr_struct;

    MPI_Type_create_struct(4, blocklengths, displacements, types, &mpi_task_response_t);
    MPI_Type_commit(&mpi_task_response_t);
}

void send_stdio_paths(const std::string &outfile, const std::string &errfile) {
    strcpy(buf, outfile.c_str());
    strcpy(buf+outfile.size()+1, errfile.c_str());
    int size = outfile.size()+errfile.size()+2;
    MPI_Bcast(&size, 1, MPI_INT, 0, MPI_COMM_WORLD); // Send message size first
    MPI_Bcast(buf, outfile.size()+errfile.size()+2, MPI_CHAR, 0, MPI_COMM_WORLD); // Then send message
}

void recv_stdio_paths(std::string &outfile, std::string &errfile) {
    int size;
    MPI_Bcast(&size, 1, MPI_INT, 0, MPI_COMM_WORLD); // Get size first
    MPI_Bcast(buf, size, MPI_CHAR, 0, MPI_COMM_WORLD); // Then get message
    outfile = buf;
    errfile = buf+strlen(buf)+1;
}

int send_request(const std::string &name, const std::string &command, const std::string &extra_id, int worker) {
    int rc;
    task_request_t req;
    strcpy(req.name, name.c_str());
    strcpy(req.command, command.c_str());
    strcpy(req.extra_id, extra_id.c_str());
    rc = MPI_Send(&req, 1, mpi_task_request_t, worker, TAG_COMMAND, MPI_COMM_WORLD);
    if (rc != MPI_SUCCESS)
        return 1;
    return 0;
}

int recv_request(std::string &name, std::string &command, std::string &extra_id) {
    int rc;
    MPI_Status status;
    task_request_t req;
    rc = MPI_Recv(&req, 1, mpi_task_request_t, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
    
    if (rc != MPI_SUCCESS) {
        return 1;
    }

    if (status.MPI_TAG == TAG_SHUTDOWN) {
        return 1;
    }
    
    name = req.name;
    command = req.command;
    extra_id = req.extra_id;
    
    return 0;
}

void send_shutdown(int worker) {
    MPI_Send(NULL, 0, mpi_task_request_t, worker, TAG_SHUTDOWN, MPI_COMM_WORLD);
}

void send_response(const std::string &name, double start_time, double end_time, int exitcode) {
    task_response_t tinfo;
    strcpy(tinfo.name, name.c_str());
    tinfo.start_time = start_time;
    tinfo.end_time = end_time;
    tinfo.exit_code = exitcode;
    MPI_Send(&tinfo, 1, mpi_task_response_t, 0, TAG_RESULT, MPI_COMM_WORLD);
}

void recv_response(std::string &name, double &start_time, double &end_time, int &exitcode, int &worker) {
    task_response_t tinfo;
    MPI_Status status;
    MPI_Recv(&tinfo, 1, mpi_task_response_t, MPI_ANY_SOURCE, TAG_RESULT, MPI_COMM_WORLD, &status);
    
    name = tinfo.name;
    start_time = tinfo.start_time;
    end_time = tinfo.end_time;
    exitcode = tinfo.exit_code;
    worker = status.MPI_SOURCE;
}

void send_total_runtime(double total_runtime) {
    MPI_Reduce(&total_runtime, NULL, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
}

double collect_total_runtimes() {
    double ignore = 0.0;
    double total_runtime = 0.0;
    MPI_Reduce(&ignore, &total_runtime, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
    return total_runtime;
}
