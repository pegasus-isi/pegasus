#include <string>
#include <fstream>
#include <errno.h>
#include <math.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#ifdef DARWIN
# include <sys/param.h>
# include <sys/sysctl.h>
#endif
#include <sys/time.h>
#include <sys/stat.h>
#include <limits.h>
#include <sstream>
#include <stdlib.h>
#include <libgen.h>
#ifdef LINUX
# include <sched.h>
# ifdef HAS_LIBNUMA
#  include <numaif.h>
# endif
#endif

#include "tools.h"
#include "failure.h"
#include "log.h"

using std::string;
using std::vector;

/* purpose: formats ISO 8601 timestamp into given buffer (simplified)
 * paramtr: seconds (IN): time stamp
 *          buffer (OUT): where to put the results
 *          size (IN): capacity of buffer
 * returns: pointer to start of buffer for convenience. 
 */
char * isodate( time_t seconds, char* buffer, size_t size ) {
    struct tm zulu = *gmtime(&seconds);
    struct tm local = *localtime(&seconds);
    zulu.tm_isdst = local.tm_isdst;
    {
        time_t distance = (seconds - mktime(&zulu)) / 60;
        int hours = distance / 60;
        int minutes = distance < 0 ? -distance % 60 : distance % 60;
        size_t len = strftime( buffer, size, "%Y-%m-%dT%H:%M:%S", &local );
        snprintf( buffer+len, size-len, "%+03d:%02d", hours, minutes );
    }
    return buffer;
}


/* purpose: formats ISO 8601 timestamp into given buffer (simplified)
 * paramtr: seconds_wf (IN): time stamp with fractional seconds (millis)
 *          buffer (OUT): where to put the results
 *          size (IN): capacity of buffer
 * returns: pointer to start of buffer for convenience. 
 */
char * iso2date( double seconds_wf, char* buffer, size_t size ) {
    char millis[8]; 
    double integral, fractional = modf(seconds_wf,&integral); 
    time_t seconds = (time_t) integral; 
    struct tm zulu = *gmtime(&seconds);
    struct tm local = *localtime(&seconds);
    zulu.tm_isdst = local.tm_isdst;
    snprintf( millis, sizeof(millis), "%.3f", fractional ); 
    {
        time_t distance = (seconds - mktime(&zulu)) / 60;
        int hours = distance / 60;
        int minutes = distance < 0 ? -distance % 60 : distance % 60;
        size_t len = strftime( buffer, size, "%Y-%m-%dT%H:%M:%S", &local );
        snprintf( buffer+len, size-len, "%s%+03d:%02d", millis+1, hours, minutes );
    }
    return buffer;
}


/* Get the local host name */
void get_host_name(std::string &hostname) {
    char name[HOST_NAME_MAX];
    if (gethostname(name, HOST_NAME_MAX) < 0) {
        myfailures("Unable to get host name");
    }
    hostname = name;
}


/* Get the current time as the number of seconds since the epoch */
double current_time() {
    struct timeval now;
    gettimeofday(&now, NULL);
    double ts = now.tv_sec + (now.tv_usec/1000000.0);
    return ts;
}


/* Get the total amount of physical memory in bytes */
unsigned long get_host_memory() {
    unsigned long memory;
#ifdef DARWIN
    size_t size = sizeof(memory);
    if (sysctlbyname("hw.memsize", &memory, &size, NULL, 0) < 0) {
        myfailures("Unable to get host physical memory size");
    }
#else
    long pages = sysconf(_SC_PHYS_PAGES);
    if (pages < 0) {
        myfailures("Unable to get number of host physical memory pages");
    }
    long pagesize = sysconf(_SC_PAGE_SIZE);
    if (pagesize < 0) {
        myfailures("Unable to get physical page size");
    }
    memory = pages * pagesize;
#endif
    if (memory == 0) {
        myfailure("Invalid memory size: %lu bytes", memory);
    }
    return memory;
}

struct cpuinfo get_host_cpuinfo() {
    struct cpuinfo c;
    c.threads = 0;
    c.cores = 0;
    c.sockets = 0;
#ifdef __MACH__
    unsigned int temp;
    size_t size = sizeof(temp);
    if (sysctlbyname("hw.logicalcpu", &temp, &size, NULL, 0) < 0) {
        myfailures("Unable to get number of CPUs (logical CPUs)");
    }
    c.threads = temp;
    if (sysctlbyname("hw.physicalcpu", &temp, &size, NULL, 0) < 0) {
        myfailures("Unable to get number of cores (physical CPUs)");
    }
    c.cores = temp;
    if (sysctlbyname("hw.packages", &temp, &size, NULL, 0) < 0) {
        myfailures("Unable to get number of CPU sockets");
    }
    c.threads = temp;
#else
    std::ifstream infile;
    infile.open("/proc/cpuinfo");
    if (!infile.good()) {
        myfailures("Error opening /proc/cpuinfo");
    }

    int last_physical_id = -1;
    bool new_socket = false;
    string rec;
    while (getline(infile, rec)) {
        if (rec.find("processor\t:", 0, 11) == 0) {
            // Each time we encounter a processor field, we increment the
            // number of cpus/threads
            c.threads += 1;
        } else if (rec.find("physical id\t:", 0, 13) == 0) {
            // Each time we encounter a new physical id, we increment the
            // number of sockets
            int new_physical_id;
            if (sscanf(rec.c_str(), "physical id\t: %d", &new_physical_id) != 1) {
                myfailures("Error reading 'physical id' field from /proc/cpuinfo");
            }
            if (new_physical_id > last_physical_id) {
                c.sockets += 1;
                last_physical_id = new_physical_id;
                new_socket = true;
            }
        } else if (rec.find("cpu cores\t:", 0, 11) == 0) {
            // Each time we encounter a new socket, we count the number of
            // cores it has
            if (new_socket) {
                cpu_t cores;
                if (sscanf(rec.c_str(), "cpu cores\t: %" SCNcpu_t, &cores) != 1) {
                    myfailures("Error reading 'cpu cores' field from /proc/cpuinfo");
                }
                c.cores += cores;
                new_socket = false;
            }
        }
    }

    if (infile.bad() || !infile.eof()) {
        myfailures("Error reading /proc/cpuinfo");
    }

    infile.close();
#endif
    if (c.threads == 0 || c.cores == 0 || c.sockets == 0 ||
            c.cores > c.threads || c.sockets > c.cores ||
            c.threads % c.cores > 0 || c.cores % c.sockets > 0) {
        myfailure("Invalid cpuinfo: %" PRIcpu_t " %" PRIcpu_t " %" PRIcpu_t, c.threads, c.cores, c.sockets);
    }
    return c;
}

int mkdirs(const char *path) {
    if (path == NULL || strlen(path) == 0) {
        return 0;
    }
    
    // No need to create cwd
    if (path[0] == '.' && path[1] == '\0') {
        return 0;
    }
    
    if (strlen(path) > PATH_MAX) {
        errno = ENAMETOOLONG;
        return -1;
    }
    
    char mypath[PATH_MAX];
    char *p = mypath;
    
    if (path[0] == '.' && path[1] == '.') {
        if (getcwd(mypath, PATH_MAX) == NULL) {
            return -1;
        }
        
        char *parent = strrchr(mypath, '/');
        if (parent) *parent = '\0';
        
        int off = strlen(mypath);
        strcpy(mypath+off, path+2);
        
        // In this case we don't need to go back to the root
        p = mypath + off; 
    } else if (path[0] == '.' && path[1] == '/') {
        strcpy(mypath, path+2);
    } else {
        strcpy(mypath, path);
    }
    
    while (*p == '/') p++;
    
    int created = 0;
    struct stat st;
    while ((p = strchr(p, '/'))) {
        *p = '\0';
        
        if (stat(mypath, &st)) {
            if (errno == ENOENT) {
                if (mkdir(mypath, 0777)) {
                    return -1;
                }
                created++;
            } else {
                return -1;
            }
        } else {
            // If it exists, make sure it is a dir
            if (S_ISDIR(st.st_mode) == 0) {
                errno = ENOTDIR;
                return -1;
            }
        }
        
        *p = '/';
        while (*p == '/') p++;
    }
    
    // Last element of path
    if (stat(mypath, &st)) {
        if (errno == ENOENT) { 
            if (mkdir(mypath, 0777)) {
                return -1;
            }
            created++;
        } else {
            return -1;
        }
    } else {
        if (S_ISDIR(st.st_mode) == 0) {
            errno = ENOTDIR;
            return -1;
        }
    }
    
    return created;
}

bool is_executable(const string &file) {
    // Invalid path
    if (file.size() == 0) {
        return false;
    }
    
    struct stat st;
    if (stat(file.c_str(), &st) == 0) {
        if (S_ISREG(st.st_mode)) {
            if ((st.st_uid == geteuid() && (S_IXUSR & st.st_mode) == S_IXUSR) ||
                (st.st_gid == getegid() && (S_IXGRP & st.st_mode) == S_IXGRP) ||
                ((S_IXOTH & st.st_mode) == S_IXOTH)) {
                // It is a regular file and we can execute it
                return true;
            }
        }
    }
    
    // In all other cases, the file is not executable
    return false;
}

int read_file(const string &file, char *buf, size_t size) {
    // Invalid path
    if (file.size() == 0) {
        errno = ENOENT;
        return -1;
    }
    
    FILE *f = fopen(file.c_str(), "r");
    if (f == NULL) {
        return -1;
    }
    
    size_t read = fread(buf, 1, size, f);

    if (fclose(f)) {
        return -1;
    }
    
    return read;
}

string pathfind(const string &file) {
    if (file.size() == 0) {
        return file;
    }
    
    // files that have a / should be returned as-is
    if (file.find('/') != string::npos) {
        return file;
    }
    
    // normally we wouldn't allow this
    if (is_executable(file)) {
        return file;
    }
    
    string path;
    char *env = getenv("PATH");
    if (env == NULL) {
#ifdef _PATH_DEFPATH
        path = _PATH_DEFPATH;
#else
        return file;
#endif
    } else {
        /* yes, there is a PATH variable */ 
        path = env;
    }
    
    string element;
    std::istringstream split(path);
    while(std::getline(split, element, ':')) {
        if (element.size() == 0) {
            continue;
        }
        string myfile = element + "/" + file;
        if (is_executable(myfile)) {
            return myfile;
        }
    }
    
    return file;
}

/* Return the directory part of a path */
string dirname(const string &path) {
    char *temp = strdup(path.c_str());
    string result = ::dirname(temp);
    free(temp);
    return result;
}

/* Return the last part of a path */
string filename(const string &path) {
    char *temp = strdup(path.c_str());
    string result = ::basename(temp);
    free(temp);
    return result;
}

/* Set the cpu affinity to values in bindings */
int set_cpu_affinity(vector<cpu_t> &bindings) {
#ifdef LINUX
    struct cpuinfo c = get_host_cpuinfo();
    cpu_set_t *cpuset = CPU_ALLOC(c.threads);
    if (cpuset == NULL) {
        return -1;
    }
    size_t cpusetsize = CPU_ALLOC_SIZE(c.threads);
    CPU_ZERO_S(cpusetsize, cpuset);

    for (vector<cpu_t>::iterator i = bindings.begin(); i != bindings.end(); i++) {
        cpu_t j = *i;
        if (j >= c.threads) {
            CPU_FREE(cpuset);
            errno = ERANGE;
            return -1;
        }
        CPU_SET_S(j, cpusetsize, cpuset);
    }

    int rc = sched_setaffinity(0, cpusetsize, cpuset);
    CPU_FREE(cpuset);
    if (rc < 0) {
        return -1;
    }
#endif
    return 0;
}

int clear_cpu_affinity() {
#ifdef LINUX
    struct cpuinfo c = get_host_cpuinfo();
    cpu_set_t *cpuset = CPU_ALLOC(c.threads);
    if (cpuset == NULL) {
        return -1;
    }
    size_t cpusetsize = CPU_ALLOC_SIZE(c.threads);
    CPU_ZERO_S(cpusetsize, cpuset);

    for (unsigned i=0; i<c.threads; i++) {
        CPU_SET_S(i, cpusetsize, cpuset);
    }

    int rc = sched_setaffinity(0, cpusetsize, cpuset);
    CPU_FREE(cpuset);
    if (rc < 0) {
        return -1;
    }
#endif
    return 0;
}

int clear_memory_affinity() {
#ifdef HAS_LIBNUMA
    int rc = set_mempolicy(MPOL_DEFAULT, NULL, 0);
    if (rc < 0) {
        return -1;
    }
#endif
    return 0;
}
