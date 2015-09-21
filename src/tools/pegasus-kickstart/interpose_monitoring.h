#ifndef KICKSTART_INTERPOSE_MONITORING_H
#define KICKSTART_INTERPOSE_MONITORING_H

#include <sys/types.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdio.h>

/* ONLINE MONITORING RELATED TYPES AND FUNCTIONS */

typedef struct {
    double real_utime;
    double real_stime;
    double real_iowait;
} CpuUtilInfo;

typedef struct {
    unsigned long long vmSize;
    unsigned long long vmRSS;
    unsigned long threads;
} MemUtilInfo;

typedef struct {
    unsigned long long rchar;
    unsigned long long wchar;
    unsigned long syscr;
    unsigned long syscw;
    unsigned long long read_bytes;
    unsigned long long write_bytes;
    unsigned long long cancelled_write_bytes;
} IoUtilInfo;

typedef struct {
    int mpi_rank;
    pthread_t main_thread_id;
} MonitoringThreadInput;

// SOCKET-BASED COMMUNICATION WITH KICKSTART

int prepare_socket(int *sockfd, char *monitoring_socket_host, char* monitoring_socket_port, struct sockaddr_in *serv_addr);

int send_msg_to_kickstart(char *msg, char *host, char *port);

// END SOCKET-BASED COMMUNICATION WITH KICKSTART

/* Dependencies from interpose.c */

extern pthread_mutex_t _interpose_io_mut;
extern IoUtilInfo _interpose_io_util_info;

extern FILE *_interpose_fopen_untraced(const char *path, const char *mode);
extern int _interpose_fclose_untraced(FILE *fp);
extern char *_interpose_fgets_untraced(char *s, int size, FILE *stream);
extern void _interpose_read_exe();

/* End dependencies */

int set_monitoring_params(int mpi_rank, int *interval, char **socket_host, char **socket_port, 
						  char **kickstart_pid, char *hostname, char **job_id);

/* monitoring related functions */
void read_cpu_status(CpuUtilInfo *info);
void read_mem_status(MemUtilInfo *info);
void read_io_status(IoUtilInfo *info);

void _interpose_spawn_monitoring_thread();
void _interpose_stop_monitoring_thread();

void* monitoring_thread_func(void* mpi_rank_void);

#ifdef HAS_PAPI

#define papi_max_events 20

int read_hardware_counters(int eventset, int *shared_nevents, int *shared_events, long long *shared_counters);

#endif /* HAS_PAPI */

#endif /* KICKSTART_INTERPOSE_MONITORING_H */