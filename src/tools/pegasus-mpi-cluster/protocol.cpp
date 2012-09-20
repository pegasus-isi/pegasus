#include <string.h>
#include <mpi.h>
#include <stdlib.h>

#include "tools.h"
#include "protocol.h"
#include "failure.h"
#include "log.h"

// XXX This protocol implementation assumes that the source and destination
// XXX both have the same endianness

unsigned long pmc_bytes_sent = 0;
unsigned long pmc_bytes_recvd = 0;

Message::Message(MessageType type) {
    this->type = type;
    this->msg = NULL;
    this->msgsize = 0;
    this->source = 0;
}

Message::Message(MessageType type, char *msg, unsigned msgsize, int source) {
    this->type = type;
    this->msg = msg;
    this->msgsize = msgsize;
    this->source = source;
}

Message::~Message() {
    if (msg) {
        free(msg);
    }
}

ShutdownMessage::ShutdownMessage(char *msg, unsigned msgsize, int source) : Message(SHUTDOWN, msg, msgsize, source) {
}

ShutdownMessage::ShutdownMessage() : Message(SHUTDOWN) {
}

CommandMessage::CommandMessage(char *msg, unsigned msgsize, int source) : Message(COMMAND, msg, msgsize, source) {
    unsigned off = 0;
    name = msg + off;
    off += name.length() + 1;
    command = msg + off;
    off += command.length() + 1;
    id = msg + off;
    off += id.length() + 1;
    memcpy(&memory, msg + off, sizeof(memory));
    off += sizeof(memory);
    memcpy(&cpus, msg + off, sizeof(cpus));
    off += sizeof(cpus);
    
    while (off < msgsize) {
        string varname = msg + off;
        off += varname.length() + 1;
        string filename = msg + off;
        off += filename.length() + 1;
        forwards[varname] = filename;
    }
}

CommandMessage::CommandMessage(const string &name, const string &command, const string &id, unsigned memory, unsigned cpus, const map<string,string> &forwards) : Message(COMMAND) {
    this->name = name;
    this->command = command;
    this->id = id;
    this->memory = memory;
    this->cpus = cpus;
    this->forwards = forwards;
    
    msgsize = name.length() + 1 + command.length() + 1 +
        id.length() + 1 + sizeof(memory) + sizeof(cpus);
    
    map<string,string>::iterator i;
    for (i=this->forwards.begin(); i!=this->forwards.end(); i++) {
        msgsize += (*i).first.length() + 1;
        msgsize += (*i).second.length() + 1;
    }
    
    msg = (char *)malloc(msgsize);
    
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
    
    for (i=this->forwards.begin(); i!=this->forwards.end(); i++) {
        const string *varname = &(*i).first;
        const string *filename = &(*i).second;
        strcpy(msg + off, varname->c_str());
        off += varname->length() + 1;
        strcpy(msg + off, filename->c_str());
        off += filename->length() + 1;
    }
}

ResultMessage::ResultMessage(char *msg, unsigned msgsize, int source, int _dummy_) : Message (RESULT, msg, msgsize, source) {
    int off = 0;
    name = msg;
    off += name.length() + 1;
    memcpy(&exitcode, msg + off, sizeof(exitcode));
    off += sizeof(exitcode);
    memcpy(&runtime, msg + off, sizeof(runtime));
    off += sizeof(runtime);
}

ResultMessage::ResultMessage(const string &name, int exitcode, double runtime) : Message(RESULT) {
    this->name = name;
    this->exitcode = exitcode;
    this->runtime = runtime;

    this->msgsize = name.length() + 1 + sizeof(exitcode) + sizeof(runtime);
    this->msg = (char *)malloc(this->msgsize);
    
    int off = 0;
    strcpy(msg + off, name.c_str());
    off += name.length() + 1;
    memcpy(msg + off, &exitcode, sizeof(exitcode));
    off += sizeof(exitcode);
    memcpy(msg + off, &runtime, sizeof(runtime));
    off += sizeof(runtime);
}

RegistrationMessage::RegistrationMessage(char *msg, unsigned msgsize, int source) : Message(REGISTRATION, msg, msgsize, source) {
    hostname = msg;
    int off = hostname.length() + 1;
    memcpy(&memory, msg + off, sizeof(memory));
    off += sizeof(memory);
    memcpy(&cpus, msg + off, sizeof(cpus));
    off += sizeof(cpus);
}

RegistrationMessage::RegistrationMessage(const string &hostname, unsigned memory, unsigned cpus) : Message(REGISTRATION) {
    this->hostname = hostname;
    this->memory = memory;
    this->cpus = cpus;

    this->msgsize = hostname.length() + 1 + sizeof(memory) + sizeof(cpus);
    this->msg = (char *)malloc(this->msgsize);
    
    int off = 0;
    strcpy(msg + off, hostname.c_str());
    off += strlen(msg) + 1;
    memcpy(msg + off, &memory, sizeof(memory));
    off += sizeof(memory);
    memcpy(msg + off, &cpus, sizeof(cpus));
}

HostrankMessage::HostrankMessage(char *msg, unsigned msgsize, int source) : Message(HOSTRANK, msg, msgsize, source) {
    memcpy(&hostrank, msg, sizeof(hostrank));
}

HostrankMessage::HostrankMessage(int hostrank) : Message(HOSTRANK) {
    this->hostrank = hostrank;
    
    this->msgsize = sizeof(hostrank);
    this->msg = (char *)malloc(this->msgsize);
    
    memcpy(msg, &hostrank, sizeof(hostrank));
}

IODataMessage::IODataMessage(char *msg, unsigned msgsize, int source) : Message(IODATA, msg, msgsize, source) {
    int off = 0;
    task = msg + off;
    off += task.length() + 1;
    filename = msg + off;
    off += filename.length() + 1;
    memcpy(&size, msg + off, sizeof(size));
    off += sizeof(size);
    data = msg + off;
}

IODataMessage::IODataMessage(const string &task, const string &filename, const char *data, unsigned size) : Message(IODATA) {
    this->task = task;
    this->filename = filename;
    this->data = data;
    this->size = size;

    this->msgsize = task.length() + 1 + filename.length() + 1 + sizeof(size) + size;
    this->msg = (char *)malloc(this->msgsize);
    
    int off = 0;
    strcpy(msg + off, task.c_str());
    off += task.length() + 1;
    strcpy(msg + off, filename.c_str());
    off += filename.length() + 1;
    memcpy(msg + off, &size, sizeof(size));
    off += sizeof(size);
    memcpy(msg + off, data, size);
}

void send_message(Message *message, int dest) {
    char *msg = message->msg;
    unsigned msgsize = message->msgsize;
    int tag = message->type;
    MPI_Send(msg, msgsize, MPI_CHAR, dest, tag, MPI_COMM_WORLD);
    pmc_bytes_sent += msgsize;
}

Message *recv_message() {
    // We probe first in order to get the status, which will tell us
    // the size of the message so that we can allocate an appropriate
    // buffer for it.
    MPI_Status status;
    MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
    
    // Allocate a buffer of the right size
    int msgsize;
    MPI_Get_count(&status, MPI_CHAR, &msgsize);
    char *msg = (char *)malloc(msgsize);
    MPI_Recv(msg, msgsize, MPI_CHAR, MPI_ANY_SOURCE, MPI_ANY_TAG, 
            MPI_COMM_WORLD, &status);
    pmc_bytes_recvd += msgsize;
    
    // Create the right type of message
    Message *message = NULL;
    int source = status.MPI_SOURCE;
    MessageType type = (MessageType)status.MPI_TAG;
    switch(type) {
        case SHUTDOWN:
            message = new ShutdownMessage(msg, msgsize, source);
            break;
        case COMMAND:
            message = new CommandMessage(msg, msgsize, source);
            break;
        case RESULT:
            // The extra zero is just for disambiguation
            message = new ResultMessage(msg, msgsize, source, 0);
            break;
        case REGISTRATION:
            message = new RegistrationMessage(msg, msgsize, source);
            break;
        case HOSTRANK:
            message = new HostrankMessage(msg, msgsize, source);
            break;
        case IODATA:
            message = new IODataMessage(msg, msgsize, source);
            break;
        default:
            myfailure("Unknown message type: %d", type);
    }
    
    return message;
}

bool message_waiting() {
    int flag;
    MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &flag, MPI_STATUS_IGNORE);
    return flag != 0;
}

