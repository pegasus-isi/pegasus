#ifndef _TOOLS_H
#define _TOOLS_H

#include <string>
#include <stddef.h>
#include <time.h>
#include <unistd.h>
#include <vector>

#ifndef HOST_NAME_MAX
#define HOST_NAME_MAX 255
#endif

typedef unsigned int cpu_t;
#define SCNcpu_t "u"
#define PRIcpu_t "u"

struct cpuinfo {
    cpu_t threads;
    cpu_t cores;
    cpu_t sockets;
};

char * isodate(time_t seconds, char* buffer, size_t size);
char * iso2date(double seconds_wf, char* buffer, size_t size);
double current_time();
void get_host_name(std::string &hostname);
unsigned long get_host_memory();
struct cpuinfo get_host_cpuinfo();
int mkdirs(const char *path);
bool is_executable(const std::string &file);
std::string pathfind(const std::string &file);
int read_file(const std::string &file, char *buf, size_t size);
std::string dirname(const std::string &path);
std::string filename(const std::string &path);
int set_cpu_affinity(std::vector<cpu_t> &bindings);
int clear_cpu_affinity();
int clear_memory_affinity();

#endif /* _TOOLS_H */
