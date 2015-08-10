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

    // Get the task name
    name = msg + off;
    off += name.length() + 1;

    // Get the number of arguments
    unsigned nargs;
    memcpy(&nargs, msg + off, sizeof(nargs));
    off += sizeof(nargs);

    // Now retrieve the arguments
    for (unsigned i = 0; i<nargs; i++) {
        string arg = msg + off;
        off += arg.length() + 1;
        this->args.push_back(arg);
    }

    // Get the task ID
    id = msg + off;
    off += id.length() + 1;

    // Get the memory requirement
    memcpy(&memory, msg + off, sizeof(memory));
    off += sizeof(memory);

    // Get the cpu requirement
    memcpy(&cpus, msg + off, sizeof(cpus));
    off += sizeof(cpus);

    // Get the number of bindings
    cpu_t nbindings;
    memcpy(&nbindings, msg + off, sizeof(nbindings));
    off += sizeof(nbindings);

    // Get the bindings
    for (cpu_t i = 0; i<nbindings; i++) {
        cpu_t binding;
        memcpy(&binding, msg + off, sizeof(binding));
        bindings.push_back(binding);
        off += sizeof(binding);
    }

    // Get the number of pipe forwards
    unsigned char npipes;
    memcpy(&npipes, msg + off, sizeof(npipes));
    off += sizeof(npipes);

    // Get the pipe forwards
    for (int i = 0; i<npipes; i++) {
        string varname = msg + off;
        off += varname.length() + 1;
        string filename = msg + off;
        off += filename.length() + 1;
        pipe_forwards[varname] = filename;
    }

    // Get the number of file forwards
    unsigned char nfiles;
    memcpy(&nfiles, msg + off, sizeof(nfiles));
    off += sizeof(nfiles);

    // Get the file forwards
    for (int i = 0; i<nfiles; i++) {
        string srcfile = msg + off;
        off += srcfile.length() + 1;
        string destfile = msg + off;
        off += destfile.length() + 1;
        file_forwards[srcfile] = destfile;
    }
}

CommandMessage::CommandMessage(const string &name, const list<string> &args, const string &id, unsigned memory, cpu_t cpus, const vector<cpu_t> &bindings, const map<string,string> *pipe_forwards, const map<string,string> *file_forwards) {
    this->name = name;
    this->args = args;
    this->id = id;
    this->memory = memory;
    this->cpus = cpus;
    this->bindings = bindings;
    if (pipe_forwards) this->pipe_forwards = *pipe_forwards;
    if (file_forwards) this->file_forwards = *file_forwards;

    // Compute the size of the variable length sections
    unsigned nargs = this->args.size();
    cpu_t nbindings = this->bindings.size();
    unsigned char npipes = this->pipe_forwards.size();
    unsigned char nfiles = this->file_forwards.size();

    // The constant part of the message size
    msgsize = name.length() + 1 +
              sizeof(nargs) +
              id.length() + 1 +
              sizeof(memory) +
              sizeof(cpus) +
              sizeof(nbindings) + (nbindings * sizeof(cpu_t)) +
              sizeof(npipes) +
              sizeof(nfiles);

    // Add the size of the arguments section
    list<string>::iterator l;
    for (l=this->args.begin(); l!=this->args.end(); l++) {
        msgsize += l->length() + 1;
    }

    // Add the size of the pipe forwards section
    map<string,string>::iterator m;
    for (m=this->pipe_forwards.begin(); m!=this->pipe_forwards.end(); m++) {
        msgsize += m->first.length() + 1;
        msgsize += m->second.length() + 1;
    }

    // Add the size of the file forwards section
    for (m=this->file_forwards.begin(); m!=this->file_forwards.end(); m++) {
        msgsize += m->first.length() + 1;
        msgsize += m->second.length() + 1;
    }

    // Now allocate an appropriate-sized buffer
    msg = new char[msgsize];

    // This keeps track of where we are writing to the message buffer
    int off = 0;

    // Add the name to the message
    strcpy(msg + off, name.c_str());
    off += name.length() + 1;

    // Add the arguments section to the message
    memcpy(msg + off, &nargs, sizeof(nargs));
    off += sizeof(nargs);
    for (l=this->args.begin(); l!=this->args.end(); l++) {
        strcpy(msg + off, l->c_str());
        off += l->length() + 1;
    }

    // Add the task ID
    strcpy(msg + off, id.c_str());
    off += id.length() + 1;

    // Add the memory requirement
    memcpy(msg + off, &memory, sizeof(memory));
    off += sizeof(memory);

    // Add the CPU requirement
    memcpy(msg + off, &cpus, sizeof(cpus));
    off += sizeof(cpus);

    // Add the bindings
    memcpy(msg + off, &nbindings, sizeof(nbindings));
    off += sizeof(nbindings);
    for (vector<cpu_t>::iterator i=this->bindings.begin(); i!=this->bindings.end(); i++) {
        cpu_t binding = *i;
        memcpy(msg + off, &binding, sizeof(binding));
        off += sizeof(binding);
    }

    // Add the pipe forwards
    memcpy(msg + off, &npipes, sizeof(npipes));
    off += sizeof(npipes);
    for (m=this->pipe_forwards.begin(); m!=this->pipe_forwards.end(); m++) {
        const string *varname = &(m->first);
        const string *filename = &(m->second);
        strcpy(msg + off, varname->c_str());
        off += varname->length() + 1;
        strcpy(msg + off, filename->c_str());
        off += filename->length() + 1;
    }

    // Add the file forwards
    memcpy(msg + off, &nfiles, sizeof(nfiles));
    off += sizeof(nfiles);
    for (m=this->file_forwards.begin(); m!=this->file_forwards.end(); m++) {
        const string *srcfile = &(m->first);
        const string *destfile = &(m->second);
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
    memcpy(&threads, msg + off, sizeof(threads));
    off += sizeof(threads);
    memcpy(&cores, msg + off, sizeof(cores));
    off += sizeof(cores);
    memcpy(&sockets, msg + off, sizeof(sockets));
    //off += sizeof(sockets);
}

RegistrationMessage::RegistrationMessage(const string &hostname, unsigned memory, cpu_t threads, cpu_t cores, cpu_t sockets) {
    this->hostname = hostname;
    this->memory = memory;
    this->threads = threads;
    this->cores = cores;
    this->sockets = sockets;

    this->msgsize = hostname.length() + 1 + sizeof(memory) + sizeof(threads) + sizeof(cores) + sizeof(sockets);
    this->msg = new char[this->msgsize];

    int off = 0;
    strcpy(msg + off, hostname.c_str());
    off += strlen(msg) + 1;
    memcpy(msg + off, &memory, sizeof(memory));
    off += sizeof(memory);
    memcpy(msg + off, &threads, sizeof(threads));
    off += sizeof(threads);
    memcpy(msg + off, &cores, sizeof(cores));
    off += sizeof(cores);
    memcpy(msg + off, &sockets, sizeof(sockets));
    //off += sizeof(sockets);
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

