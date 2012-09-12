#include <string.h>
#include <mpi.h>

#include "tools.h"
#include "protocol.h"
#include "failure.h"
#include "log.h"

#define MAX_MESSAGE 16384

static char buf[MAX_MESSAGE];

void send_registration(const string &hostname, unsigned int memory, unsigned int cpus) {
    // Send the hostname
    sprintf(buf, "%s %u %u", hostname.c_str(), memory, cpus);
    int size = strlen(buf) + 1;
    MPI_Send(buf, size, MPI_CHAR, 0, TAG_HOSTNAME, MPI_COMM_WORLD);
}

void recv_registration(int &worker, string &hostname, unsigned int &memory, unsigned int &cpus) {
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

void send_request(const string &name, const string &command, const string &pegasus_id, unsigned int memory, unsigned int cpus, map<string, string> &forwards, int worker) {
    
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
    
    map<string, string>::iterator i;
    for (i=forwards.begin(); i!=forwards.end(); i++) {
        string varname = (*i).first;
        string filename = (*i).second;
        log_trace("Sending forward %s => %s", varname.c_str(), filename.c_str());
        sprintf(buf+size, "%s=%s", varname.c_str(), filename.c_str());
        size += strlen(buf+size) + 1;
    }
    
    // Send message
    MPI_Send(buf, size, MPI_CHAR, worker, TAG_COMMAND, MPI_COMM_WORLD);
}

void recv_request(string &name, string &command, string &pegasus_id, unsigned int &memory, unsigned int &cpus, map<string, string> &forwards, int &shutdown) {
    
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
        string forward = buf+size;
        size += forward.size() + 1;
        int eq = forward.find("=");
        string varname = forward.substr(0, eq);
        string filename = forward.substr(eq + 1);
        log_trace("Received forward %s = %s", varname.c_str(), filename.c_str());
        forwards[varname] = filename;
    }
}

void send_shutdown(int worker) {
    MPI_Send(NULL, 0, MPI_CHAR, worker, TAG_SHUTDOWN, MPI_COMM_WORLD);
}

void send_response(const string &name, int exitcode, double runtime) {
    int size = 0;
    sprintf(buf, "%d", exitcode);
    size += strlen(buf) + 1;
    strcpy(buf+size, name.c_str());
    size += name.size() + 1;
    sprintf(buf+size, "%lf", runtime);
    size += strlen(buf+size) + 1;
    MPI_Send(buf, size, MPI_CHAR, 0, TAG_RESULT, MPI_COMM_WORLD);
}

void recv_response(string &name, int &exitcode, double &runtime, int &worker) {
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

