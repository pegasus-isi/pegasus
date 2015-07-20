#include <curl/curl.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <sys/time.h>

#include "monitoring.h"

/* Get the current time in seconds since the epoch */
static double get_time() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec + ((double)tv.tv_usec / 1e6);
}

// a util function for reading env variables by the main kickstart process
// with monitoring endpoint data or set default values
static void initialize_monitoring_endpoint(MonitoringEndpoint* endpoint, char* kickstart_status_path) {
    char* envptr;

    endpoint->kickstart_status = kickstart_status_path;

    envptr = getenv("KICKSTART_MON_ENDPOINT_URL");

    if (envptr != NULL) {
        endpoint->url = (char*) calloc(sizeof(char), strlen(envptr) + 1);
        strcpy(endpoint->url, envptr);
    }

    envptr = getenv("KICKSTART_MON_ENDPOINT_CREDENTIALS");

    if (envptr != NULL) {
        endpoint->credentials = (char*) calloc(sizeof(char), strlen(envptr) + 1);
        strcpy(endpoint->credentials, envptr);
    }
}

static void release_monitoring_endpoint(MonitoringEndpoint* monitoring_endpoint) {
    if(monitoring_endpoint->url != NULL)
        free(monitoring_endpoint->url);

    if(monitoring_endpoint->credentials != NULL)
        free(monitoring_endpoint->credentials);
}

// read information about workflow and job ids
// and put it into a JobIdInfo struct
static void initialize_job_id_info(JobIdInfo *info) {
    char* envptr;

    envptr = getenv("PEGASUS_WF_UUID");

    if (envptr != NULL) {
        info->wf_uuid = (char*) calloc(sizeof(char), strlen(envptr) + 1);
        strcpy(info->wf_uuid, envptr);
    }

    envptr = getenv("PEGASUS_WF_LABEL");

    if (envptr != NULL) {
        info->wf_label = (char*) calloc(sizeof(char), strlen(envptr) + 1);
        strcpy(info->wf_label, envptr);
    }

    envptr = getenv("PEGASUS_DAG_JOB_ID");

    if (envptr != NULL) {
        info->dag_job_id = (char*) calloc(sizeof(char), strlen(envptr) + 1);
        strcpy(info->dag_job_id, envptr);
    }

    envptr = getenv("CONDOR_JOBID");

    if (envptr != NULL) {
        info->condor_job_id = (char*) calloc(sizeof(char), strlen(envptr) + 1);
        strcpy(info->condor_job_id, envptr);
    }
}

static void release_job_id_info(JobIdInfo *job_id_info) {
    if(job_id_info->wf_uuid != NULL)
        free(job_id_info->wf_uuid);

    if(job_id_info->wf_label != NULL)
        free(job_id_info->wf_label);

    if(job_id_info->dag_job_id != NULL)
        free(job_id_info->dag_job_id);

    if(job_id_info->condor_job_id != NULL)
        free(job_id_info->condor_job_id);
}

int start_status_thread(pthread_t* monitoring_thread, char* kickstart_status) {
    int rc = 0;

    rc = pthread_create(monitoring_thread, NULL, monitoring_thread_func, (void*)kickstart_status);
    if (rc) {
        printerr("ERROR: return code from pthread_create() is %d\n", rc);
    }
    else {
        rc = pthread_detach(*monitoring_thread);
        if (rc) {
            printerr("ERROR: return code from pthread_detach() is %d\n", rc);
        }
    }

    return rc;
}

void print_debug_info(MonitoringEndpoint *monitoring_endpoint, JobIdInfo *job_id_info) {
    printerr("[mon-thread] Our monitoring information:\n");
    printerr("[mon-thread] kickstart-status-file: %s\n", monitoring_endpoint->kickstart_status);
    printerr("[mon-thread] url: %s\n", monitoring_endpoint->url);
    printerr("[mon-thread] credentials: %s\n", monitoring_endpoint->credentials);
    printerr("[mon-thread] wf uuid: %s\n", job_id_info->wf_uuid);
    printerr("[mon-thread] wf label: %s\n", job_id_info->wf_label);
    printerr("[mon-thread] dag job id: %s\n", job_id_info->dag_job_id);
    printerr("[mon-thread] condor job id: %s\n", job_id_info->condor_job_id);
}

size_t write_callback(char *ptr, size_t size, size_t nmemb, void *userdata) {
    //    we do nothing for now
    return size * nmemb;
}

// MESSAGES AGGREGATION 
static int msg_counter = 0;
//static int MSG_AGGR_FACTOR = 5;
#define MSG_AGGR_FACTOR 5
static char aggr_msg_buffer[BUFSIZ * MSG_AGGR_FACTOR];
static int aggr_msg_buffer_offset = 0;

// copies msg to aggr_msg_buff and increase aggr_msg_offset
static int aggregate_message(char* msg_buff, char* aggr_msg_buff, int *aggr_msg_offset) {
    int n;

    printerr("[mon-thread] Aggregating message - %s\n", msg_buff);

    n = sprintf(aggr_msg_buff + *aggr_msg_offset, "%s:delim1:", msg_buff);
    if(n < 0) {
        printerr("[mon-thread] Error during aggregating messages\n");
    }
    else {
        *aggr_msg_offset += n;
    }

    return n;
}

static void send_msg_to_mq(char* msg_buff, MonitoringEndpoint *monitoring_endpoint, char* wf_uuid) {
    CURL *curl;
    CURLcode res;
    char *payload = (char*) malloc(sizeof(msg_buff) * sizeof(char) + BUFSIZ);

    // sending this message to rabbitmq
    curl = curl_easy_init();
    if(curl) {
        curl_easy_setopt(curl, CURLOPT_URL, monitoring_endpoint->url);
        curl_easy_setopt(curl, CURLOPT_USERPWD, monitoring_endpoint->credentials);
        curl_easy_setopt(curl, CURLOPT_POST, 1);

        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0);
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0);

        struct curl_slist *http_header = NULL;
        http_header = curl_slist_append(http_header, "Content-Type: application/json");
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, http_header);

        sprintf(payload, "{\"properties\":{},\"routing_key\":\"%s\",\"payload\":\"%s\",\"payload_encoding\":\"string\"}",
            wf_uuid, msg_buff);

        printerr("[mon-thread] Sending aggregated msg payload: %s\n", payload);

        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, payload);

        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);

        /* Perform the request, res will get the return code */
        res = curl_easy_perform(curl);
        /* Check for errors */
        if(res != CURLE_OK) {
            printerr("[mon-thread] an error occured while sending measurement: %s\n",
                curl_easy_strerror(res));
        }

        /* always cleanup */
        curl_easy_cleanup(curl);

        curl_slist_free_all(http_header);
    }
    else {
        printerr("[mon-thread] we couldn't initialize curl\n");
    }

    free(payload);
}

/*
 * Main monitoring thread loop - it periodically read global trace file as sent this info somewhere, e.g. to another file
 * or to an external service.
 * It parses any information from the global monitoring and calculates some mean values.
 */
void* monitoring_thread_func(void* kickstart_status_path) {
    // CURL *curl;
    // CURLcode res;
    int interval;
    char payload[BUFSIZ], enriched_line[BUFSIZ], line[BUFSIZ], *envptr;
    MonitoringEndpoint monitoring_endpoint;
    JobIdInfo job_id_info = { "", "", "", "" };

    envptr = getenv("KICKSTART_MON_INTERVAL");

    if (envptr != NULL) {
        interval = atoi(envptr) + 2;
    }
    else {
        printerr("[mon-thread] Couldn't read KICKSTART_MON_INTERVAL\n");
        pthread_exit(NULL);
        return NULL;
    }

    initialize_monitoring_endpoint(&monitoring_endpoint, (char*)kickstart_status_path);
    initialize_job_id_info(&job_id_info);

    print_debug_info(&monitoring_endpoint, &job_id_info);

    FILE* kickstart_status = fopen(monitoring_endpoint.kickstart_status, "r");

    curl_global_init(CURL_GLOBAL_ALL);

    printerr("[mon-thread] starting monitoring loop...\n");
    while(1) {

        sleep(interval);

        if(kickstart_status == NULL) {
            kickstart_status = fopen(monitoring_endpoint.kickstart_status, "r");
            if(kickstart_status == NULL) {
                printerr("[mon-thread] Couldn't open kickstart_status_path for read - %s\n",
                    strerror(errno));
            }
        }

        if(kickstart_status != NULL) {
            while(fgets(line, BUFSIZ, kickstart_status) != NULL)
            {
                char *pos;

                if( (pos = strchr(line, '\n')) != NULL )
                    *pos = '\0';

                // if( strstr(line, "ts=") != line )
                    // continue;

                sprintf(enriched_line, "%s wf_uuid=%s wf_label=%s dag_job_id=%s condor_job_id=%s",
                    line, job_id_info.wf_uuid, job_id_info.wf_label, job_id_info.dag_job_id,
                    job_id_info.condor_job_id);

                // AGGREGATION
                msg_counter += 1;
                if( aggregate_message(enriched_line, aggr_msg_buffer, &aggr_msg_buffer_offset) > 0 ) {
                    if( msg_counter == MSG_AGGR_FACTOR ) {                        
                        printerr("[mon-thread] Sending aggregated message: %s\n", aggr_msg_buffer);
                        send_msg_to_mq(aggr_msg_buffer, &monitoring_endpoint, job_id_info.wf_uuid);

                        msg_counter = 0;
                        aggr_msg_buffer_offset = 0;
                        memset(aggr_msg_buffer, 0, BUFSIZ * MSG_AGGR_FACTOR);
                    }
                }

                // sending this message to rabbitmq
                // curl = curl_easy_init();
                // if(curl) {
                //     curl_easy_setopt(curl, CURLOPT_URL, monitoring_endpoint.url);
                //     curl_easy_setopt(curl, CURLOPT_USERPWD, monitoring_endpoint.credentials);
                //     curl_easy_setopt(curl, CURLOPT_POST, 1);

                //     curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0);
                //     curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0);

                //     struct curl_slist *http_header = NULL;
                //     http_header = curl_slist_append(http_header, "Content-Type: application/json");
                //     curl_easy_setopt(curl, CURLOPT_HTTPHEADER, http_header);

                //     sprintf(payload, "{\"properties\":{},\"routing_key\":\"%s\",\"payload\":\"%s\",\"payload_encoding\":\"string\"}",
                //         job_id_info.wf_uuid,
                //         enriched_line);

                //     curl_easy_setopt(curl, CURLOPT_POSTFIELDS, payload);

                //     curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);

                //     /* Perform the request, res will get the return code */
                //     res = curl_easy_perform(curl);
                //     /* Check for errors */
                //     if(res != CURLE_OK) {
                //         printerr("[mon-thread] an error occured while sending measurement: %s\n",
                //             curl_easy_strerror(res));
                //     }

                //     /* always cleanup */
                //     curl_easy_cleanup(curl);

                //     curl_slist_free_all(http_header);
                // }
                // else {
                //     printerr("[mon-thread] we couldn't initialize curl\n");
                // }

            }
        }
    }

    if( msg_counter != 0 ) {
        printerr("[mon-thread] Sending last aggregated message: %s\n", aggr_msg_buffer);
        send_msg_to_mq(aggr_msg_buffer, &monitoring_endpoint, job_id_info.wf_uuid);
    }

    release_monitoring_endpoint(&monitoring_endpoint);
    release_job_id_info(&job_id_info);

    // fclose(monitoring_file);
    fclose(kickstart_status);

    pthread_exit(NULL);
    curl_global_cleanup();
}
