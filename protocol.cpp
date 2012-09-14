#include <string.h>
#include <mpi.h>

#include "tools.h"
#include "protocol.h"
#include "failure.h"
#include "log.h"

// XXX This protocol implementation assumes that the source and destination
// XXX both have the same endianness

unsigned long pmc_bytes_sent = 0;
unsigned long pmc_bytes_recvd = 0;

ShutdownMessage::ShutdownMessage() : Message(SHUTDOWN) {
}

int ShutdownMessage::encode(char **buff) {
    // This is an empty message
    *buff = NULL;
    return 0;
}

void ShutdownMessage::decode(char *buff, int size) {
    // Nothing to do for shutdown message
}

CommandMessage::CommandMessage() : Message(COMMAND) {
}

CommandMessage::CommandMessage(const string &name, const string &command, const string &id, unsigned memory, unsigned cpus, const map<string,string> &forwards) : Message(COMMAND) {
    this->name = name;
    this->command = command;
    this->id = id;
    this->memory = memory;
    this->cpus = cpus;
    this->forwards = forwards;
}

int CommandMessage::encode(char **buff) {
    map<string, string>::iterator i;
    
    int size = name.length() + 1 + command.length() + 1 +
        id.length() + 1 + sizeof(memory) + sizeof(cpus);
    for (i=forwards.begin(); i!=forwards.end(); i++) {
        size += (*i).first.length() + 1;
        size += (*i).second.length() + 1;
    }
    
    char *msg = (char *)malloc(size);
    
    int off = 0;
    
    strcpy(msg + off, name.c_str());
    off += name.length() + 1;
    strcpy(msg + off, command.c_str());
    off += command.length() + 1;
    strcpy(msg + off, id.c_str());
    off += id.length() + 1;
    memcpy(msg + off, &memory, sizeof(memory));
    off += sizeof(memory);
    memcpy(msg + off, &cpus, sizeof(cpus));
    off += sizeof(cpus);
    
    for (i=forwards.begin(); i!=forwards.end(); i++) {
        const string *varname = &(*i).first;
        const string *filename = &(*i).second;
        strcpy(msg + off, varname->c_str());
        off += varname->length() + 1;
        strcpy(msg + off, filename->c_str());
        off += filename->length() + 1;
    }
    
    *buff = msg;
    return size;
}

void CommandMessage::decode(char *buff, int size) {
    unsigned off = 0;
    name = buff + off;
    off += name.length() + 1;
    command = buff + off;
    off += command.length() + 1;
    id = buff + off;
    off += id.length() + 1;
    memcpy(&memory, buff + off, sizeof(memory));
    off += sizeof(memory);
    memcpy(&cpus, buff + off, sizeof(cpus));
    off += sizeof(cpus);
    
    while (off < size) {
        string varname = buff + off;
        off += varname.length() + 1;
        string filename = buff + off;
        off += filename.length() + 1;
        forwards[varname] = filename;
    }
}

ResultMessage::ResultMessage() : Message (RESULT) {
}

ResultMessage::ResultMessage(const string &name, int exitcode, double runtime) : Message(RESULT) {
    this->name = name;
    this->exitcode = exitcode;
    this->runtime = runtime;
}

int ResultMessage::encode(char **buff) {
    int size = name.length() + 1 + sizeof(exitcode) + sizeof(runtime);
    char *msg = (char *)malloc(size);
    int off = 0;
    strcpy(msg + off, name.c_str());
    off += name.length() + 1;
    memcpy(msg + off, &exitcode, sizeof(exitcode));
    off += sizeof(exitcode);
    memcpy(msg + off, &runtime, sizeof(runtime));
    off += sizeof(runtime);
    *buff = msg;
    return size;
}

void ResultMessage::decode(char *buff, int size) {
    int off = 0;
    name = buff;
    off += name.length() + 1;
    memcpy(&exitcode, buff + off, sizeof(exitcode));
    off += sizeof(exitcode);
    memcpy(&runtime, buff + off, sizeof(runtime));
    off += sizeof(runtime);
}

RegistrationMessage::RegistrationMessage() : Message(REGISTRATION) {
}

RegistrationMessage::RegistrationMessage(const string &hostname, unsigned memory, unsigned cpus) : Message(REGISTRATION) {
    this->hostname = hostname;
    this->memory = memory;
    this->cpus = cpus;
}

int RegistrationMessage::encode(char **buff) {
    int size = hostname.length() + 1 + sizeof(memory) + sizeof(cpus);
    char *msg = (char *)malloc(size);
    int off = 0;
    strcpy(msg + off, hostname.c_str());
    off += strlen(msg) + 1;
    memcpy(msg + off, &memory, sizeof(memory));
    off += sizeof(memory);
    memcpy(msg + off, &cpus, sizeof(cpus));
    *buff = msg;
    return size;
}

void RegistrationMessage::decode(char *buff, int size) {
    hostname = buff;
    int off = hostname.length() + 1;
    memcpy(&memory, buff + off, sizeof(memory));
    off += sizeof(memory);
    memcpy(&cpus, buff + off, sizeof(cpus));
    off += sizeof(cpus);
}

HostrankMessage::HostrankMessage() : Message(HOSTRANK) {
}

HostrankMessage::HostrankMessage(int hostrank) : Message(HOSTRANK) {
    this->hostrank = hostrank;
}

int HostrankMessage::encode(char **buff) {
    int size = sizeof(hostrank);
    char *msg = (char *)malloc(size);
    memcpy(msg, &hostrank, sizeof(hostrank));
    *buff = msg;
    return size;
}

void HostrankMessage::decode(char *buff, int size) {
    memcpy(&hostrank, buff, sizeof(hostrank));
}

IODataMessage::IODataMessage() : Message(IODATA) {
}

IODataMessage::IODataMessage(const string &task, const string &filename, const char *data, unsigned size) : Message(IODATA) {
    this->task = task;
    this->filename = filename;
    this->data = data;
    this->size = size;
}

int IODataMessage::encode(char **buff) {
    // TODO
    return 0;
}

void IODataMessage::decode(char *buff, int size) {
    // TODO
}

void send_message(Message &message, int dest) {
    char *buff;
    int size = message.encode(&buff);
    int tag = message.type;
    MPI_Send(buff, size, MPI_CHAR, dest, tag, MPI_COMM_WORLD);
    pmc_bytes_sent += size;
    if (buff) free(buff);
}

Message *recv_message() {
    // We probe first in order to get the status, which will tell us
    // the size of the message so that we can allocate an appropriate
    // buffer for it.
    MPI_Status status;
    MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
    
    // Allocate a buffer of the right size
    int size;
    MPI_Get_count(&status, MPI_CHAR, &size);
    char *buff = (char *)malloc(size);
    MPI_Recv(buff, size, MPI_CHAR, MPI_ANY_SOURCE, MPI_ANY_TAG, 
             MPI_COMM_WORLD, &status);
    pmc_bytes_recvd += size;
    
    // Create the right type of message
    Message *message;
    MessageType type = (MessageType)status.MPI_TAG;
    log_trace("Got message %d", type);
    switch(type) {
        case SHUTDOWN:
            message = new ShutdownMessage();
            break;
        case COMMAND:
            message = new CommandMessage();
            break;
        case RESULT:
            message = new ResultMessage();
            break;
        case REGISTRATION:
            log_trace("Got registration message");
            message = new RegistrationMessage();
            break;
        case HOSTRANK:
            message = new HostrankMessage();
            break;
        default:
            myfailure("Unknown message type: %d", type);
    }
    
    message->decode(buff, size);
    message->source = status.MPI_SOURCE;
    
    if (buff) free(buff);
    
    return message;
}

bool message_waiting() {
    int flag;
    MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &flag, MPI_STATUS_IGNORE);
    return flag != 0;
}

