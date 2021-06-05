#ifndef _PEGASUS_MONITORING_HELPERS_H
#define _PEGASUS_MONITORING_HELPERS_H

typedef struct {
    char *url;
    char *wf_label;
    char *wf_uuid;
    char *dag_job_id;
    char *condor_job_id;
    char *xformation;
    char *task_id;
    char *site;
    char *hostname;
    int socket;
    int interval;
} MonitoringContext;

int publish_stats(MonitoringContext *ctx, char *doc_buffer);

#endif
