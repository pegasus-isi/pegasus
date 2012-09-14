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
    MessageType type;
    int source;
    Message(MessageType type) { this->type = type; }
    virtual ~Message() {}
    virtual int encode(char **buff) = 0;
    virtual void decode(char *buff, int size) = 0;
};

class ShutdownMessage: public Message {
public:
    ShutdownMessage();
    int encode(char **buff);
    void decode(char *buff, int size);
};

class CommandMessage: public Message {
public:
    string name;
    string command;
    string id;
    unsigned memory;
    unsigned cpus;
    map<string, string> forwards;
    
    CommandMessage();
    CommandMessage(const string &name, const string &command, const string &id, unsigned memory, unsigned cpus, const map<string,string> &forwards);
    int encode(char **buff);
    void decode(char *buff, int size);
};

class ResultMessage: public Message {
public:
    string name;
    int exitcode;
    double runtime;
    
    ResultMessage();
    ResultMessage(const string &name, int exitcode, double runtime);
    int encode(char **buff);
    void decode(char *buff, int size);
};

class RegistrationMessage: public Message {
public:
    string hostname;
    unsigned memory;
    unsigned cpus;
    
    RegistrationMessage();
    RegistrationMessage(const string &hostname, unsigned memory, unsigned cpus);
    int encode(char **buff);
    void decode(char *buff, int size);
};

class HostrankMessage: public Message {
public:
    int hostrank;
    
    HostrankMessage();
    HostrankMessage(int hostrank);
    int encode(char **buff);
    void decode(char *buff, int size);
};

class IODataMessage: public Message {
public:
    string task;
    string filename;
    const char *data;
    unsigned size;
    
    IODataMessage();
    IODataMessage(const string &task, const string &filename, const char *data, unsigned size);
    int encode(char **buff);
    void decode(char *buff, int size);
};

void send_message(Message &message, int rank);
Message *recv_message();
bool message_waiting();

#endif /* PROTOCOL_H */
