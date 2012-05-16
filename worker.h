#ifndef WORKER_H
#define WORKER_H

#include <string>

class Worker {
	int rank;
	int host_rank;
	std::string host_name;
	std::string host_script;
	pid_t host_script_pid;
	unsigned host_memory;
	
	void launch_host_script();
	void check_host_script(bool terminate);
public:
    Worker(const std::string &host_script, unsigned host_memory);
    ~Worker();
    int run();
};

#endif /* WORKER_H */
