#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <string>

#define TAG_COMMAND 1
#define TAG_RESULT 2
#define TAG_SHUTDOWN 3
#define TAG_HOSTNAME 4
#define TAG_HOSTRANK 5

void send_registration(const std::string &hostname, unsigned int memory, unsigned int cpus);
void recv_registration(int &worker, std::string &hostname, unsigned int &memory, unsigned int &cpus);
void send_hostrank(int worker, int hostrank);
void recv_hostrank(int &hostrank);
void send_stdio_paths(const std::string &outfile, const std::string &errfile);
void recv_stdio_paths(std::string &outfile, std::string &errfile);
void send_request(const std::string &name, const std::string &command, const std::string &pegasus_id, unsigned int memory, int worker);
void send_shutdown(int worker);
void recv_request(std::string &name, std::string &command, std::string &pegasus_id, unsigned int &memory, int &shutdown);
void send_response(const std::string &name, int exitcode);
void recv_response(std::string &name, int &exitcode, int &worker);
bool response_waiting();
double collect_total_runtimes();
void send_total_runtime(double total_runtime);

#endif /* PROTOCOL_H */
