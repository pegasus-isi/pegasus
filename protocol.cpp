#include <string.h>
#include <mpi.h>

#include "tools.h"
#include "protocol.h"
#include "failure.h"
#include "log.h"

#define MAX_MESSAGE 16384

static char buf[MAX_MESSAGE];

void send_registration(const std::string &hostname, unsigned int memory, unsigned int cpus) {
    // Send the hostname
    sprintf(buf, "%s %u %u", hostname.c_str(), memory, cpus);
    int size = strlen(buf) + 1;
    MPI_Send(buf, size, MPI_CHAR, 0, TAG_HOSTNAME, MPI_COMM_WORLD);
}

void recv_registration(int &worker, std::string &hostname, unsigned int &memory, unsigned int &cpus) {
    MPI_Status status;
    MPI_Recv(buf, MAX_MESSAGE, MPI_CHAR, MPI_ANY_SOURCE, TAG_HOSTNAME, MPI_COMM_WORLD, &status);
    worker = status.MPI_SOURCE;
    char name[HOST_NAME_MAX];
    hostname = buf;
    sscanf(buf, "%s %u %u", name, &memory, &cpus);
    hostname = name;
}

void recv_hostrank(int &hostrank) {
    MPI_Status status;
    MPI_Recv(&hostrank, 1, MPI_INT, 0, TAG_HOSTRANK, MPI_COMM_WORLD, &status);
}

void send_hostrank(int worker, int hostrank) {
    MPI_Send(&hostrank, 1, MPI_INT, worker, TAG_HOSTRANK, MPI_COMM_WORLD);
}

void send_request(const std::string &name, const std::string &command, const std::string &pegasus_id, unsigned int memory, unsigned int cpus, std::map<std::string, std::string> &forwards, int worker) {
    
    // Pack message
    unsigned size = 0;
    strcpy(buf+size, name.c_str());
    size += name.size() + 1;
    strcpy(buf+size, command.c_str());
    size += command.size() + 1;
    strcpy(buf+size, pegasus_id.c_str());
    size += pegasus_id.size() + 1;
    sprintf(buf+size, "%u", memory);
    size += strlen(buf+size) + 1;
    sprintf(buf+size, "%u", cpus);
    size += strlen(buf+size) + 1;
    
    std::map<std::string, std::string>::iterator i;
    for (i=forwards.begin(); i!=forwards.end(); i++) {
        std::string varname = (*i).first;
        std::string filename = (*i).second;
        log_trace("Sending forward %s => %s", varname.c_str(), filename.c_str());
        sprintf(buf+size, "%s=%s", varname.c_str(), filename.c_str());
        size += strlen(buf+size) + 1;
    }
    
    // Send message
    MPI_Send(buf, size, MPI_CHAR, worker, TAG_COMMAND, MPI_COMM_WORLD);
}

void recv_request(std::string &name, std::string &command, std::string &pegasus_id, unsigned int &memory, unsigned int &cpus, std::vector<std::string> &forwards, int &shutdown) {
    
    // Recv message
    MPI_Status status;
    MPI_Recv(buf, MAX_MESSAGE, MPI_CHAR, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
    
    // If the master wants us to shutdown, we just return here
    shutdown = 0;
    if (status.MPI_TAG == TAG_SHUTDOWN) {
        shutdown = 1;
        return;
    }
    
    int msgsize;
    MPI_Get_count(&status, MPI_CHAR, &msgsize);
    
    // Unpack message
    unsigned size = 0;
    name = buf+size;
    size += name.size() + 1;
    command = buf+size;
    size += command.size() + 1;
    pegasus_id = buf+size;
    size += pegasus_id.size() + 1;
    sscanf(buf+size, "%u", &memory);
    size += strlen(buf+size) + 1;
    sscanf(buf+size, "%u", &cpus);
    size += strlen(buf+size) + 1;
    
    while (size < msgsize) {
        std::string forward = buf+size;
        size += forward.size() + 1;
        forwards.push_back(forward);
        log_trace("Received forward %s", forward.c_str());
    }
}

void send_shutdown(int worker) {
    MPI_Send(NULL, 0, MPI_CHAR, worker, TAG_SHUTDOWN, MPI_COMM_WORLD);
}

void send_response(const std::string &name, int exitcode, double runtime) {
    int size = 0;
    sprintf(buf, "%d", exitcode);
    size += strlen(buf) + 1;
    strcpy(buf+size, name.c_str());
    size += name.size() + 1;
    sprintf(buf+size, "%lf", runtime);
    size += strlen(buf+size) + 1;
    MPI_Send(buf, size, MPI_CHAR, 0, TAG_RESULT, MPI_COMM_WORLD);
}

void recv_response(std::string &name, int &exitcode, double &runtime, int &worker) {
    MPI_Status status;
    MPI_Recv(buf, MAX_MESSAGE, MPI_CHAR, MPI_ANY_SOURCE, TAG_RESULT, MPI_COMM_WORLD, &status);
    
    int size = 0;
    sscanf(buf, "%d", &exitcode);
    size += strlen(buf) + 1;
    name = buf+size;
    size += name.size() + 1;
    sscanf(buf+size, "%lf", &runtime);
    worker = status.MPI_SOURCE;
}

bool response_waiting() {
    int flag;
    MPI_Iprobe(MPI_ANY_SOURCE, TAG_RESULT, MPI_COMM_WORLD, &flag, MPI_STATUS_IGNORE);
    return flag != 0;
}

bool request_waiting() {
    int flag;
    MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &flag, MPI_STATUS_IGNORE);
    return flag != 0;
}

