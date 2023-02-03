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

typedef struct json_doc {
    int buffer_size;
    size_t buffer_limit;
    char* buffer;
} json_doc;

int initialize_monitoring_context(MonitoringContext *ctx);
void release_monitoring_context(MonitoringContext *ctx);
int send_monitoring_report_json(MonitoringContext *ctx, json_doc doc);
int json_encode_context(MonitoringContext *ctx, char *buf, size_t maxsize);

#endif
