#include <curl/curl.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <sys/time.h>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <unistd.h>
#include <errno.h>
#include <sys/types.h>

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

int start_status_thread(pthread_t* monitoring_thread, char* kickstart_socket_port) {
    int rc = 0;

    rc = pthread_create(monitoring_thread, NULL, monitoring_thread_func, (void*)kickstart_socket_port);
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
    // TODO we use socket-based communication for now
    // printerr("[mon-thread] kickstart-status-file: %s\n", monitoring_endpoint->kickstart_status);
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
#define MSG_AGGR_FACTOR 5

// copies msg to aggr_msg_buff and increase aggr_msg_offset
static int aggregate_message(char* msg_buff, char* aggr_msg_buff, int *aggr_msg_offset) {
    int n;

    // printerr("[mon-thread] Aggregating message - %s\n", msg_buff);

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

        // printerr("[mon-thread] Sending aggregated msg payload: %s\n", payload);

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

// SOCKET BASED COMMUNICATION
// open a temporary socket to find out what ephemeral port can be used 
int prepare_socket(char *kickstart_hostname, char *kickstart_port) {
    int listenfd;
    struct sockaddr_in serv_addr;
    socklen_t addr_len;

    if( (listenfd = socket(AF_INET, SOCK_STREAM, 0)) < 0 ) {
      printerr("ERROR[socket]: %s\n", strerror(errno));
      return -1;
    }
  
    memset(&serv_addr, 0, sizeof(serv_addr));
      
    serv_addr.sin_family = AF_INET;    
    serv_addr.sin_addr.s_addr = htonl(INADDR_ANY); 
    serv_addr.sin_port = 0;

    if( bind(listenfd, (const struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0 ) {
      printerr("ERROR[bind]: %s\n", strerror(errno));
      return -1;
    }

    if( listen(listenfd, 1) < 0 ) {
      printerr("ERROR[listen]: %s\n", strerror(errno));
      return -1;
    }
    else {
        memset(&serv_addr, 0, sizeof(serv_addr));
        addr_len = sizeof(serv_addr);

        if( getsockname(listenfd, (struct sockaddr*)&serv_addr, &addr_len) == -1 ) {
            printerr("ERROR[getsockname]: %s\n", strerror(errno));
            return -1;
        }
        else {
            sprintf(kickstart_port, "%d", ntohs(serv_addr.sin_port));

            printerr("Port: %s\n", kickstart_port);

            if( gethostname(kickstart_hostname, BUFSIZ) ) {
                printerr("ERROR[gethostname]: %s\n", strerror(errno));
                return -1;
            }

            printerr("Host: %s\n", kickstart_hostname);
        }

        close(listenfd);
    }

    return 0;
}

// Open a socket on a given port and return its fd
int prepare_monitoring_socket(int *socket_fd, int port) {
    struct sockaddr_in serv_addr;

    if( (*socket_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0 ) {
      printerr("ERROR[socket]: %s\n", strerror(errno));
      return -1;
    }

    memset(&serv_addr, 0, sizeof(serv_addr));
      
    serv_addr.sin_family = AF_INET;    
    serv_addr.sin_addr.s_addr = htonl(INADDR_ANY); 
    serv_addr.sin_port = htons(port);

    if( bind(*socket_fd, (const struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0 ) {
      printerr("ERROR[bind]: %s\n", strerror(errno));
      return -1;
    }

    if( listen(*socket_fd, 1) < 0 ) {
        printerr("ERROR[listen]: %s\n", strerror(errno));
        return -1;
    }

    return 0;
}

// END SOCKET BASED COMMUNICATION


/*
 * Main monitoring thread loop - it periodically read global trace file as sent this info somewhere, e.g. to another file
 * or to an external service.
 * It parses any information from the global monitoring and calculates some mean values.
 */
void* monitoring_thread_func(void* socket_port_buf) {
    int kickstart_socket_port = -1, monitoring_socket = -1, num_bytes, incoming_socket;
    int msg_counter = 0, aggr_msg_buffer_offset = 0;
    struct sockaddr_in client_addr;
    socklen_t client_add_len;
    char line[BUFSIZ], *pos = NULL, enriched_line[BUFSIZ], aggr_msg_buffer[BUFSIZ * MSG_AGGR_FACTOR];
    MonitoringEndpoint monitoring_endpoint;
    JobIdInfo job_id_info;

    initialize_monitoring_endpoint(&monitoring_endpoint, NULL);
    initialize_job_id_info(&job_id_info);

    print_debug_info(&monitoring_endpoint, &job_id_info);

    if( socket_port_buf == NULL ) {
        printerr("[mon-thread] Kickstart socket port is not set\n");
    }
    else {
        kickstart_socket_port = atoi((char*)socket_port_buf);
        printerr("[mon-thread] Socket nr is: %d\n", kickstart_socket_port);

        if( prepare_monitoring_socket(&monitoring_socket, kickstart_socket_port) < 0 ) {
            printerr("[mon-thread] ERROR occured during socket preparation\n");
            monitoring_socket = -1;
        } 
        else {
            printerr("[mon-thread] Monitoring socket prepared\n");
        }
    }

    curl_global_init(CURL_GLOBAL_ALL);

    printerr("[mon-thread] starting monitoring loop...\n");

    while(1) {
        bzero((char *)&client_addr, sizeof(client_addr));
        client_add_len = sizeof(client_addr);

        incoming_socket = accept(monitoring_socket, (struct sockaddr *)&client_addr, &client_add_len);
        if(incoming_socket < 0) {
            printerr("[mon-thread] ERROR[accept]: %s\n", strerror(errno));
        }
        else {
            num_bytes = recv(incoming_socket, line, BUFSIZ, 0);
            if(num_bytes < 0) {
                printerr("[mon-thread] ERROR[recv]: %s\n", strerror(errno));
            }
            else {
                // printerr("[mon-thread] succesfull read from socket - %s\n", line);
                // replace end line with a terminating character
                if( (pos = strchr(line, '\n')) != NULL ) {
                    *pos = '\0';
                }
                // a proper monitoring message should start with a timestamp
                if( strstr(line, "ts=") != line ) {
                    continue;    
                }

                sprintf(enriched_line, "%s wf_uuid=%s wf_label=%s dag_job_id=%s condor_job_id=%s",
                    line, job_id_info.wf_uuid, job_id_info.wf_label, job_id_info.dag_job_id,
                    job_id_info.condor_job_id);

                // AGGREGATION
                msg_counter += 1;
                if( aggregate_message(enriched_line, aggr_msg_buffer, &aggr_msg_buffer_offset) > 0 ) {
                    if( msg_counter == MSG_AGGR_FACTOR ) {                        
                        // printerr("[mon-thread] Sending aggregated message...\n");
                        send_msg_to_mq(aggr_msg_buffer, &monitoring_endpoint, job_id_info.wf_uuid);

                        msg_counter = 0;
                        aggr_msg_buffer_offset = 0;
                        memset(aggr_msg_buffer, 0, BUFSIZ * MSG_AGGR_FACTOR);
                    }
                }                
            }
        }

        close(incoming_socket);
    }

    release_monitoring_endpoint(&monitoring_endpoint);
    release_job_id_info(&job_id_info);
    
    curl_global_cleanup();
    pthread_exit(NULL);
}
