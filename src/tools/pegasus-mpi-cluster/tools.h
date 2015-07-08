#ifndef _TOOLS_H
#define _TOOLS_H

#include <string>
#include <stddef.h>
#include <time.h>
#include <unistd.h>

using std::string;

#ifndef HOST_NAME_MAX
#define HOST_NAME_MAX 255
#endif

struct cpuinfo {
    unsigned int threads;
    unsigned int cores;
    unsigned int sockets;
};

char * isodate(time_t seconds, char* buffer, size_t size);
char * iso2date(double seconds_wf, char* buffer, size_t size);
double current_time();
void get_host_name(string &hostname);
unsigned long get_host_memory();
struct cpuinfo get_host_cpuinfo();
int mkdirs(const char *path);
bool is_executable(const string &file);
string pathfind(const string &file);
int read_file(const string &file, char *buf, size_t size);
string dirname(const string &path);
string filename(const string &path);
int clear_cpu_affinity();
int clear_memory_affinity();

#endif /* _TOOLS_H */
