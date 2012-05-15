#ifndef WORKER_H
#define WORKER_H

#include <string>

class Worker {
	int rank;
	int hostrank;
	std::string hostname;
	std::string hostscript;
	pid_t hostscript_pid;
	
	void launch_host_script();
	void check_host_script(bool terminate);
public:
    Worker(const std::string &hostscript);
    ~Worker();
    int run();
};

#endif /* WORKER_H */
