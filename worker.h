#ifndef WORKER_H
#define WORKER_H

#include <string>

class Worker {
	int rank;
	int hostrank;
	std::string hostname;
public:
    Worker();
    ~Worker();
    int run();
};

#endif /* WORKER_H */
