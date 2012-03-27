#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <string>

#define TAG_COMMAND 1
#define TAG_RESULT 2
#define TAG_SHUTDOWN 3

struct task_request_t {
    char name[50];
    char command[5000];
    char extra_id[50];
};

struct task_response_t {
    char name[50];
    double start_time;
    double end_time;
    int exit_code;
};

void protocol_request_struct();
void protocol_response_struct();
void send_stdio_paths(const std::string &outfile, const std::string &errfile);
void recv_stdio_paths(std::string &outfile, std::string &errfile);
int send_request(const std::string &name, const std::string &command, const std::string &extra_id, int worker);
void send_shutdown(int worker);
int recv_request(std::string &name, std::string &command, std::string &extra_id);
void send_response(const std::string &name, double start_time, double end_time, int exitcode);
void recv_response(std::string &name, double &start_time, double &end_time, int &exitcode, int &worker);
double collect_total_runtimes();
void send_total_runtime(double total_runtime);

#endif /* PROTOCOL_H */
