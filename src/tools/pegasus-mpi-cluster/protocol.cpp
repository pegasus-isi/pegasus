#include <string.h>
#include <stdlib.h>

#include "tools.h"
#include "protocol.h"
#include "failure.h"
#include "log.h"

Message::Message() {
    this->msg = NULL;
    this->msgsize = 0;
    this->source = 0;
}

Message::Message(char *msg, unsigned msgsize, int source) {
    this->msg = msg;
    this->msgsize = msgsize;
    this->source = source;
}

Message::~Message() {
    delete [] msg;
}

ShutdownMessage::ShutdownMessage(char *msg, unsigned msgsize, int source) : Message(msg, msgsize, source) {
}

ShutdownMessage::ShutdownMessage() {
}

CommandMessage::CommandMessage(char *msg, unsigned msgsize, int source) : Message(msg, msgsize, source) {
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
    
    unsigned char npipes;
    memcpy(&npipes, msg + off, sizeof(npipes));
    off += sizeof(npipes);
    
    for (int i = 0; i<npipes; i++) {
        string varname = msg + off;
        off += varname.length() + 1;
        string filename = msg + off;
        off += filename.length() + 1;
        pipe_forwards[varname] = filename;
    }
    
    unsigned char nfiles;
    memcpy(&nfiles, msg + off, sizeof(nfiles));
    off += sizeof(nfiles);
    
    for (int i = 0; i<nfiles; i++) {
        string srcfile = msg + off;
        off += srcfile.length() + 1;
        string destfile = msg + off;
        off += destfile.length() + 1;
        file_forwards[srcfile] = destfile;
    }
}

CommandMessage::CommandMessage(const string &name, const string &command, const string &id, unsigned memory, unsigned cpus, const map<string,string> *pipe_forwards, const map<string,string> *file_forwards) {
    this->name = name;
    this->command = command;
    this->id = id;
    this->memory = memory;
    this->cpus = cpus;
    if (pipe_forwards) this->pipe_forwards = *pipe_forwards;
    if (file_forwards) this->file_forwards = *file_forwards;
    
    unsigned char npipes = this->pipe_forwards.size();
    unsigned char nfiles = this->file_forwards.size();
    
    msgsize = name.length() + 1 + command.length() + 1 +
        id.length() + 1 + sizeof(memory) + sizeof(cpus) + 
        sizeof(npipes) + sizeof(nfiles);
    
    map<string,string>::iterator i;
    for (i=this->pipe_forwards.begin(); i!=this->pipe_forwards.end(); i++) {
        msgsize += i->first.length() + 1;
        msgsize += i->second.length() + 1;
    }
    for (i=this->file_forwards.begin(); i!=this->file_forwards.end(); i++) {
        msgsize += i->first.length() + 1;
        msgsize += i->second.length() + 1;
    }
    
    msg = new char[msgsize];
    
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
    
    memcpy(msg + off, &npipes, sizeof(npipes));
    off += sizeof(npipes);
    
    for (i=this->pipe_forwards.begin(); i!=this->pipe_forwards.end(); i++) {
        const string *varname = &(i->first);
        const string *filename = &(i->second);
        strcpy(msg + off, varname->c_str());
        off += varname->length() + 1;
        strcpy(msg + off, filename->c_str());
        off += filename->length() + 1;
    }
    
    memcpy(msg + off, &nfiles, sizeof(nfiles));
    off += sizeof(nfiles);
    
    for (i=this->file_forwards.begin(); i!=this->file_forwards.end(); i++) {
        const string *srcfile = &(i->first);
        const string *destfile = &(i->second);
        strcpy(msg + off, srcfile->c_str());
        off += srcfile->length() + 1;
        strcpy(msg + off, destfile->c_str());
        off += destfile->length() + 1;
    }
}

ResultMessage::ResultMessage(char *msg, unsigned msgsize, int source, int _dummy_) : Message(msg, msgsize, source) {
    int off = 0;
    name = msg;
    off += name.length() + 1;
    memcpy(&exitcode, msg + off, sizeof(exitcode));
    off += sizeof(exitcode);
    memcpy(&runtime, msg + off, sizeof(runtime));
    //off += sizeof(runtime);
}

ResultMessage::ResultMessage(const string &name, int exitcode, double runtime) {
    this->name = name;
    this->exitcode = exitcode;
    this->runtime = runtime;

    this->msgsize = name.length() + 1 + sizeof(exitcode) + sizeof(runtime);
    this->msg = new char[this->msgsize];
    
    int off = 0;
    strcpy(msg + off, name.c_str());
    off += name.length() + 1;
    memcpy(msg + off, &exitcode, sizeof(exitcode));
    off += sizeof(exitcode);
    memcpy(msg + off, &runtime, sizeof(runtime));
    //off += sizeof(runtime);
}

RegistrationMessage::RegistrationMessage(char *msg, unsigned msgsize, int source) : Message(msg, msgsize, source) {
    hostname = msg;
    int off = hostname.length() + 1;
    memcpy(&memory, msg + off, sizeof(memory));
    off += sizeof(memory);
    memcpy(&cpus, msg + off, sizeof(cpus));
    //off += sizeof(cpus);
}

RegistrationMessage::RegistrationMessage(const string &hostname, unsigned memory, unsigned cpus) {
    this->hostname = hostname;
    this->memory = memory;
    this->cpus = cpus;

    this->msgsize = hostname.length() + 1 + sizeof(memory) + sizeof(cpus);
    this->msg = new char[this->msgsize];
    
    int off = 0;
    strcpy(msg + off, hostname.c_str());
    off += strlen(msg) + 1;
    memcpy(msg + off, &memory, sizeof(memory));
    off += sizeof(memory);
    memcpy(msg + off, &cpus, sizeof(cpus));
}

HostrankMessage::HostrankMessage(char *msg, unsigned msgsize, int source) : Message(msg, msgsize, source) {
    memcpy(&hostrank, msg, sizeof(hostrank));
}

HostrankMessage::HostrankMessage(int hostrank) {
    this->hostrank = hostrank;
    
    this->msgsize = sizeof(hostrank);
    this->msg = new char [this->msgsize];
    
    memcpy(msg, &hostrank, sizeof(hostrank));
}

IODataMessage::IODataMessage(char *msg, unsigned msgsize, int source) : Message(msg, msgsize, source) {
    int off = 0;
    task = msg + off;
    off += task.length() + 1;
    filename = msg + off;
    off += filename.length() + 1;
    memcpy(&size, msg + off, sizeof(size));
    off += sizeof(size);
    data = msg + off;
}

IODataMessage::IODataMessage(const string &task, const string &filename, const char *data, unsigned size) {
    this->task = task;
    this->filename = filename;
    this->data = data;
    this->size = size;

    this->msgsize = task.length() + 1 + filename.length() + 1 + sizeof(size) + size;
    this->msg = new char [this->msgsize];
    
    int off = 0;
    strcpy(msg + off, task.c_str());
    off += task.length() + 1;
    strcpy(msg + off, filename.c_str());
    off += filename.length() + 1;
    memcpy(msg + off, &size, sizeof(size));
    off += sizeof(size);
    memcpy(msg + off, data, size);
}

