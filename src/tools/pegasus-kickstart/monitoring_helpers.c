#include <curl/curl.h>
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include "error.h"
#include "log.h"
#include "monitoring_helpers.h"


//IMPLEMENT THIS IN THE FUTURE TO PROVIDE MONITORING ENDPOINT TO ALL THREADS
int setup_monitoring_endpoint() {
    return 0;
}

// a util function for reading env variables by the main kickstart process
// with monitoring endpoint data or set default values
int initialize_monitoring_context(MonitoringContext *ctx) {
    char* envptr;

    envptr = getenv("KICKSTART_MON_INTERVAL");
    if (envptr == NULL) {
        error("KICKSTART_MON_INTERVAL not specified\n");
        return -1;
    }
    ctx->interval = atoi(envptr);

    envptr = getenv("KICKSTART_MON_URL");
    if (envptr == NULL) {
        error("KICKSTART_MON_URL not specified\n");
        return -1;
    }
    ctx->url = strdup(envptr);

    envptr = getenv("PEGASUS_WF_UUID");
    if (envptr == NULL) {
        warn("PEGASUS_WF_UUID not specified\n");
        ctx->wf_uuid = NULL;
    } else {
        ctx->wf_uuid = strdup(envptr);
    }

    envptr = getenv("PEGASUS_WF_LABEL");
    if (envptr == NULL) {
        warn("PEGASUS_WF_LABEL not specified\n");
        ctx->wf_label = NULL;
    } else {
        ctx->wf_label = strdup(envptr);
    }

    envptr = getenv("PEGASUS_DAG_JOB_ID");
    if (envptr == NULL) {
        warn("PEGASUS_DAG_JOB_ID not specified\n");
        ctx->dag_job_id = NULL;
    } else {
        ctx->dag_job_id = strdup(envptr);
    }

    envptr = getenv("CONDOR_JOBID");
    if (envptr == NULL) {
        warn("CONDOR_JOBID not specified\n");
        ctx->condor_job_id = NULL;
    } else {
        ctx->condor_job_id = strdup(envptr);
    }

    envptr = getenv("PEGASUS_XFORMATION");
    if (envptr == NULL) {
        warn("PEGASUS_XFORMATION not specified\n");
        ctx->xformation = NULL;
    } else {
        ctx->xformation = strdup(envptr);
    }

    envptr = getenv("PEGASUS_TASK_ID");
    if (envptr == NULL) {
        warn("PEGASUS_TASK_ID not specified\n");
        ctx->task_id = NULL;
    } else {
        ctx->task_id = strdup(envptr);
    }

    envptr = getenv("PEGASUS_SITE");
    if (envptr == NULL) {
        warn("PEGASUS_SITE not specified\n");
        ctx->site = NULL;
    } else {
        ctx->site = strdup(envptr);
    }

    char hostname[BUFSIZ];
    if (gethostname(hostname, BUFSIZ)) {
        printerr("ERROR[gethostname]: %s\n", strerror(errno));
        ctx->hostname = NULL;
    } else {
        ctx->hostname = strdup(hostname);
    }

    return 0;
}

void release_monitoring_context(MonitoringContext* ctx) {
    if (ctx == NULL) return;
    if (ctx->url != NULL) free(ctx->url);
    if (ctx->wf_uuid != NULL) free(ctx->wf_uuid);
    if (ctx->wf_label != NULL) free(ctx->wf_label);
    if (ctx->dag_job_id != NULL) free(ctx->dag_job_id);
    if (ctx->condor_job_id != NULL) free(ctx->condor_job_id);
    if (ctx->xformation != NULL) free(ctx->xformation);
    if (ctx->task_id != NULL) free(ctx->task_id);
    free(ctx);
}

int json_encode_context(MonitoringContext *ctx, char *buf, size_t maxsize) {
    size_t size = snprintf(buf, maxsize,
            "{\"hostname\":\"%s\","
            "\"site\":\"%s\","
            "\"wf_uuid\":\"%s\","
            "\"wf_label\":\"%s\","
            "\"dag_job_id\":\"%s\","
            "\"condor_job_id\":\"%s\","
            "\"xformation\":\"%s\","
            "\"task_id\":\"%s\"}",
            ctx->hostname == NULL ? "" : ctx->hostname,
            ctx->site == NULL ? "" : ctx->site,
            ctx->wf_uuid == NULL ? "" : ctx->wf_uuid,
            ctx->wf_label == NULL ? "" : ctx->wf_label,
            ctx->dag_job_id == NULL ? "" : ctx->dag_job_id,
            ctx->condor_job_id == NULL ? "" : ctx->condor_job_id,
            ctx->xformation == NULL ? "" : ctx->xformation,
            ctx->task_id == NULL ? "" : ctx->task_id
    );

    if (size >= maxsize) {
        error("JSON too large for buffer: %d > %d", size, maxsize);
        return -1;
    }

    return size;
}

static size_t write_callback(char *ptr, size_t size, size_t nmemb, void *userdata) {
    //    we do nothing for now
    return size * nmemb;
}

static void send_http_msg(char *url, char *msg) {
    CURL *curl = curl_easy_init();
    if (curl == NULL) {
        printerr("[mon-thread] Error initializing curl\n");
        return;
    }

    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_POST, 1);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0); /* FIXME Not secure */
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0); /* FIXME Not secure */
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, msg);
    
    struct curl_slist *http_header = NULL;
    http_header = curl_slist_append(http_header, "Content-Type: application/json");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, http_header);

    /* Perform the request, res will get the return code */
    CURLcode res = curl_easy_perform(curl);
    /* Check for errors */
    if (res != CURLE_OK) {
        error("an error occured while sending measurement: %s\n", curl_easy_strerror(res));
    }

    /* always cleanup */
    curl_easy_cleanup(curl);
    curl_slist_free_all(http_header);
}

static size_t base64_encode(const char *data, size_t input_length, char *encoded_data, size_t max_output) {
    /* http://stackoverflow.com/questions/342409/how-do-i-base64-encode-decode-in-c */

    size_t output_length = 4 * ((input_length + 2) / 3);
    if (output_length >= max_output) {
        error("base64 encoding exceeds output buffer: %d >= %d", output_length, max_output);
        return -1;
    }

    char encoding_table[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    for (int i=0, j=0; i < input_length; ) {
        uint32_t octet_a = i < input_length ? (unsigned char)data[i++] : 0;
        uint32_t octet_b = i < input_length ? (unsigned char)data[i++] : 0;
        uint32_t octet_c = i < input_length ? (unsigned char)data[i++] : 0;

        uint32_t triple = (octet_a << 0x10) + (octet_b << 0x08) + octet_c;

        encoded_data[j++] = encoding_table[(triple >> 3 * 6) & 0x3F];
        encoded_data[j++] = encoding_table[(triple >> 2 * 6) & 0x3F];
        encoded_data[j++] = encoding_table[(triple >> 1 * 6) & 0x3F];
        encoded_data[j++] = encoding_table[(triple >> 0 * 6) & 0x3F];
    }

    int mod_table[] = {0, 2, 1};
    for (int i=0; i < mod_table[input_length % 3]; i++) {
        encoded_data[output_length-1-i] = '=';
    }

    encoded_data[output_length] = '\0';

    return output_length;
}

static void send_rabbitmq(MonitoringContext *ctx, json_doc doc) {
    debug("Sending stats to rabbitmq endpoint");
    size_t b64msg_size = 0;

    char routing_key[] = "kickstart.inv.online";

    char *b64msg = (char*) malloc(doc.buffer_size * 2);
    if (b64msg == NULL) {
        error("Failed to allocate memory for base64 encoding");
        return;
    }
    b64msg_size = base64_encode(doc.buffer, strlen(doc.buffer), b64msg, doc.buffer_size * 2);
    if (b64msg_size < 0) {
        error("Unable to base64 encode message");
        free(b64msg);
        return;
    }

    char *payload = (char*) malloc(b64msg_size + 256);
    if (snprintf(payload, (b64msg_size + 256),
        "{\"properties\":{},\"routing_key\":\"%s\",\"payload\":\"%s\",\"payload_encoding\":\"base64\"}",
        routing_key, b64msg) >= (b64msg_size + 256)) {
        error("RabbitMQ payload too large for buffer");
        return;
    }
    
    free(b64msg);

    /* Need to construct a new URL */
    char url[128];
    if (strstr(ctx->url, "rabbitmqs://") == ctx->url) {
        snprintf(url, 128, "https://%s", ctx->url + strlen("rabbitmqs://"));
    } else {
        snprintf(url, 128, "http://%s", ctx->url + strlen("rabbitmq://"));
    }

    send_http_msg(url, payload);
    free(payload);
}


static void send_http(MonitoringContext *ctx, json_doc doc) {
    debug("Sending stats to http endpoint");
    send_http_msg(ctx->url, doc.buffer);
}


static void send_file(MonitoringContext *ctx, json_doc doc) {
    char *path = ctx->url + strlen("file://");
    debug("Writing monitoring data to %s", path);

    FILE *log = fopen(path, "a");
    if (log == NULL) {
        error("Unable to open monitoring log '%s': %s", path, strerror(errno));
        return;
    }

    fprintf(log, "%s\n", doc.buffer);

    fclose(log);
}

int send_monitoring_report_json(MonitoringContext *ctx, json_doc doc) {
    if (strstr(ctx->url, "rabbitmq://") == ctx->url ||
        strstr(ctx->url, "rabbitmqs://") == ctx->url) {
        send_rabbitmq(ctx, doc);
    } else if (strstr(ctx->url, "http://") == ctx->url ||
               strstr(ctx->url, "https://") == ctx->url) {
        send_http(ctx, doc);
    } else if (strstr(ctx->url, "file://") == ctx->url) {
        send_file(ctx, doc);
    } else {
        error("Unknown endpoint URL scheme: %s\n", ctx->url);
    }

    return 0;
}
