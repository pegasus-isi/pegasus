#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <string>
#include <map>

using std::string;
using std::map;

// Time in microseconds to sleep if there is no message waiting
#define NO_MESSAGE_SLEEP_TIME 50000

#define TAG_COMMAND 1
#define TAG_RESULT 2
#define TAG_SHUTDOWN 3
#define TAG_HOSTNAME 4
#define TAG_HOSTRANK 5

void send_registration(const string &hostname, unsigned int memory, unsigned int cpus);
void recv_registration(int &worker, string &hostname, unsigned int &memory, unsigned int &cpus);
void send_hostrank(int worker, int hostrank);
void recv_hostrank(int &hostrank);
void send_request(const string &name, const string &command, const string &pegasus_id, unsigned int memory, unsigned int cpus, map<string, string> &forwards, int worker);
void send_shutdown(int worker);
void recv_request(string &name, string &command, string &pegasus_id, unsigned int &memory, unsigned int &cpus, map<string, string> &forwards, int &shutdown);
void send_response(const string &name, int exitcode, double runtime);
void recv_response(string &name, int &exitcode, double &runtime, int &worker);
bool response_waiting();
bool request_waiting();

#endif /* PROTOCOL_H */
