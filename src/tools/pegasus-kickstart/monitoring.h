#ifndef _PEGASUS_MONITORING_H

#include <pthread.h>
#include "error.h"

typedef struct {
    char* url;
    char* credentials;
    char* kickstart_status;
} MonitoringEndpoint;

typedef struct {
	char* wf_label;
	char* wf_uuid;
	char* dag_job_id;
	char* condor_job_id;
} JobIdInfo;

int
start_status_thread(pthread_t* monitoring_thread, char* socked_desc_buf);
/* purpose: read environment variables, starts a monitoring thread and detaches it
 * paramtr:	monitoring_thread (OUT) a thread struct
 *			kickstart_status  (IN)	an absolute path to a global trace file
 * returns:	0 success
 *			1 failure
 */

void*
monitoring_thread_func(void* monitoring_endpoint_struct);
/* purpose: monitoring thread - periodically reads global trace file (kickstart status file)
 *			and sends each line (i.e. measurement) to external rabbitmq through HTTP
 * paramtr: monitoring_endpoint_struct (IN): a pointer to an instance of MonitoringEndpoint
 *				- it has all the information necessary to connect to rabbitmq
 */

 int prepare_socket(char *kickstart_hostname, char *kickstart_port);

#endif /* _PEGASUS_MONITORING_H */
