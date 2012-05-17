#include <string>
#include <stdio.h>

#include "tools.h"
#include "failure.h"

int main(int argc, char *argv[]) {
	get_host_memory();
	get_host_cpus();
	std::string host_name;
	get_host_name(host_name);
}
