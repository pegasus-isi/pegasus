#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <string>
#include <map>

using std::string;
using std::map;

// Time in microseconds to sleep if there is no message waiting
#define NO_MESSAGE_SLEEP_TIME 50000

extern unsigned long pmc_bytes_sent;
extern unsigned long pmc_bytes_recvd;

enum MessageType {
    COMMAND      = 1,
    RESULT       = 2,
    SHUTDOWN     = 3,
    REGISTRATION = 4,
    HOSTRANK     = 5,
    IODATA       = 6
};

class Message {
public:
    int source;
    char *msg;
    unsigned msgsize;
    
    Message();
    Message(char *msg, unsigned msgsize, int source);
    virtual ~Message();
    virtual int tag() const = 0;
};

class ShutdownMessage: public Message {
public:
    ShutdownMessage(char *msg, unsigned msgsize, int source);
    ShutdownMessage();
    virtual int tag() const { return SHUTDOWN; };
};

class CommandMessage: public Message {
public:
    string name;
    string command;
    string id;
    unsigned memory;
    unsigned cpus;
    map<string, string> pipe_forwards;
    map<string, string> file_forwards;
    
    CommandMessage(char *msg, unsigned msgsize, int source);
    CommandMessage(const string &name, const string &command, const string &id, unsigned memory, unsigned cpus, const map<string,string> *pipe_forwards, const map<string,string> *file_forwards);
    virtual int tag() const { return COMMAND; };
};

class ResultMessage: public Message {
public:
    string name;
    int exitcode;
    double runtime;
    
    ResultMessage(char *msg, unsigned msgsize, int source, int _dummy_);
    ResultMessage(const string &name, int exitcode, double runtime);
    virtual int tag() const { return RESULT; };
};

class RegistrationMessage: public Message {
public:
    string hostname;
    unsigned memory;
    unsigned cpus;
    
    RegistrationMessage(char *msg, unsigned msgsize, int source);
    RegistrationMessage(const string &hostname, unsigned memory, unsigned cpus);
    virtual int tag() const { return REGISTRATION; };
};

class HostrankMessage: public Message {
public:
    int hostrank;
    
    HostrankMessage(char *msg, unsigned msgsize, int source);
    HostrankMessage(int hostrank);
    virtual int tag() const { return HOSTRANK; };
};

class IODataMessage: public Message {
public:
    string task;
    string filename;
    const char *data;
    unsigned size;
    
    IODataMessage(char *msg, unsigned msgsize, int source);
    IODataMessage(const string &task, const string &filename, const char *data, unsigned size);
    virtual int tag() const { return IODATA; }
};

void send_message(Message *message, int rank);
Message *recv_message();
bool message_waiting();

#endif /* PROTOCOL_H */
