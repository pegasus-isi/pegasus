#include <curl/curl.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>

#include "monitoring.h"

// a util function for reading env variables by the main kickstart process 
// with monitoring endpoint data or set default values
// TODO: default values are for testing only !!!
static void initialize_monitoring_endpoint(MonitoringEndpoint* monitoring_endpoint, char* kickstart_status_path) {
    char url[BUFSIZ], credentials[BUFSIZ], routing_key[BUFSIZ]; 
    char* envptr;

    monitoring_endpoint->kickstart_status = kickstart_status_path;

    envptr = getenv("KICKSTART_MON_ENDPOINT_URL");

    if (envptr != NULL) {
        strcpy(url, envptr);
    }

    monitoring_endpoint->url = url;

    envptr = getenv("KICKSTART_MON_ENDPOINT_CREDENTIALS");

    if (envptr != NULL) {
        strcpy(credentials, envptr);
    }

    monitoring_endpoint->credentials = credentials;

    envptr = getenv("KICKSTART_MON_ENDPOINT_ROUTE_KEY");

    if (envptr != NULL) {
        strcpy(routing_key, envptr);
    }

    monitoring_endpoint->routing_key = routing_key;
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

/*
 * Main monitoring thread loop - it periodically read global trace file as sent this info somewhere, e.g. to another file
 * or to an external service. 
 * It parses any information from the global monitoring and calculates some mean values.
 */
void* monitoring_thread_func(void* kickstart_status_path) {
    CURL *curl;
    CURLcode res;
    int interval = 10;
    char line[BUFSIZ];
    char payload[4096];

    MonitoringEndpoint monitoring_endpoint;

    initialize_monitoring_endpoint(&monitoring_endpoint, (char*)kickstart_status_path);

    printerr("[mon-thread] We are starting a monitoring function\n");
    printerr("[mon-thread] Our monitoring information:\n");
    printerr("[mon-thread] kickstart-status-file: %s\n", monitoring_endpoint.kickstart_status);
    printerr("[mon-thread] url: %s\n", monitoring_endpoint.url);
    printerr("[mon-thread] credentials: %s\n", monitoring_endpoint.credentials);
    printerr("[mon-thread] routing_key: %s\n", monitoring_endpoint.routing_key);

    FILE* kickstart_status = fopen(monitoring_endpoint.kickstart_status, "r");
    if(kickstart_status == NULL) {
        printerr("[kickstart-thread] Couldn't open kickstart_status_path for read - %s\n", strerror(errno));
    }

    curl_global_init(CURL_GLOBAL_ALL);

    // FILE* monitoring_file = fopen("monitoring.log", "a");
    // if(monitoring_file == NULL) {
    //     printerr("[kickstart-thread] Couldn't open monitoring file for append\n");
    //     pthread_exit(NULL);
    //     return NULL;
    // }

    // printerr("[kickstart-thread] We opened the ./monitoring.log to append online monitoring information\n");    

    while(1) {        
        sleep(interval);

        printerr("[kickstart-thread] monitoring loop\n");

        if(kickstart_status == NULL) {
            kickstart_status = fopen(monitoring_endpoint.kickstart_status, "r");
                
            if(kickstart_status == NULL) {
                printerr("[kickstart-thread] Couldn't open kickstart_status_path for read - %s\n", strerror(errno));
            }    
        }

        if(kickstart_status != NULL) {

            while(fgets(line, BUFSIZ, kickstart_status) != NULL)
            {
                // printerr("[kickstart-thread] are writing another line: %s", line);
                // fprintf(monitoring_file, "%s", line);

                // sending this message to rabbitmq
                curl = curl_easy_init();
                if(curl) {
                    curl_easy_setopt(curl, CURLOPT_URL, monitoring_endpoint.url);
                    curl_easy_setopt(curl, CURLOPT_USERPWD, monitoring_endpoint.credentials);
                    curl_easy_setopt(curl, CURLOPT_POST, 1);

                    struct curl_slist *http_header = NULL;
                    http_header = curl_slist_append(http_header, "Content-Type: application/json");
                    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, http_header);

                    sprintf(payload, "{\"properties\":{},\"routing_key\":\"%s\",\"payload\":\"%s\",\"payload_encoding\":\"string\"}",
                        monitoring_endpoint.routing_key,
                        line);

                    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, payload);

                    /* Perform the request, res will get the return code */
                    res = curl_easy_perform(curl);
                    /* Check for errors */
                    if(res != CURLE_OK) {
                        printerr("[kickstart-thread] an error occured while sending measurement: %s\n", curl_easy_strerror(res));
                    }
                    else {
                        printerr("[kickstart-thread] measurement sent\n");
                    }

                    /* always cleanup */
                    curl_easy_cleanup(curl);

                    curl_slist_free_all(http_header);
                }
                else {
                    printerr("[kickstart-thread] we couldn't initialize curl\n");
                }
            }

            // fflush(monitoring_file);
        }

        
    }

    // fclose(monitoring_file);
    fclose(kickstart_status);

    pthread_exit(NULL);
    curl_global_cleanup();
}
