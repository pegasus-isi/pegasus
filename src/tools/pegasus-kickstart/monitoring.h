#ifndef _PEGASUS_MONITORING_H
#define _PEGASUS_MONITORING_H

#include "procfs.h"

typedef struct {
    char *url;
    char *wf_label;
    char *wf_uuid;
    char *dag_job_id;
    char *condor_job_id;
    char *xformation;
    char *task_id;
    int socket;
    int interval;
} MonitoringContext;

int initialize_monitoring_context(MonitoringContext *ctx);
void release_monitoring_context(MonitoringContext* ctx);

int start_monitoring_thread();
int stop_monitoring_thread();

/* This is used by libinterpose for online monitoring */
int send_monitoring_report(MonitoringContext *ctx, ProcStats *stats);

#endif /* _PEGASUS_MONITORING_H */
