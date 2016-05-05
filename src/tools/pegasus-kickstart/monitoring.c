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
#include <pthread.h>
#include <fcntl.h>
#include <poll.h>
#include <sys/timerfd.h>
#include <netdb.h>

#include "error.h"
#include "log.h"
#include "monitoring.h"
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

/* This pipe is used to send a shutdown message from the main thread to the
 * monitoring thread */
static int signal_pipe[2];
static pthread_t monitoring_thread;

// a util function for reading env variables by the main kickstart process
// with monitoring endpoint data or set default values
static int initialize_monitoring_context(MonitoringContext *ctx) {
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

    return 0;
}

static void release_monitoring_context(MonitoringContext* ctx) {
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
        error("an error occured while sending measurement: %s\n",
              curl_easy_strerror(res));
    }

    /* always cleanup */
    curl_easy_cleanup(curl);
    curl_slist_free_all(http_header);
}

static size_t json_encode(MonitoringContext *ctx, ProcStats *stats, char *buf, size_t maxsize) {
    size_t size = snprintf(buf, maxsize,
            "{\"ts\":%lu,"
            "\"wf_uuid\":\"%s\","
            "\"wf_label\":\"%s\","
            "\"dag_job_id\":\"%s\","
            "\"condor_job_id\":\"%s\","
            "\"xformation\":\"%s\","
            "\"task_id\":\"%s\","
            "\"pid\":%d,"
            "\"exe\":\"%s\","
            "\"utime\":%.3f,"
            "\"stime\":%.3f,"
            "\"iowait\":%.3f,"
            "\"vm\":%llu,"
            "\"rss\":%llu,"
            "\"procs\":%d,"
            "\"threads\":%d,"
            "\"bread\":%llu,"
            "\"bwrite\":%llu,"
            "\"rchar\":%llu,"
            "\"wchar\":%llu,"
            "\"syscr\":%lu,"
            "\"syscw\":%lu}",
            stats->ts,
            ctx->wf_uuid == NULL ? "" : ctx->wf_uuid,
            ctx->wf_label == NULL ? "" : ctx->wf_label,
            ctx->dag_job_id == NULL ? "" : ctx->dag_job_id,
            ctx->condor_job_id == NULL ? "" : ctx->condor_job_id,
            ctx->xformation == NULL ? "" : ctx->xformation,
            ctx->task_id == NULL ? "" : ctx->task_id,
            stats->pid,
            stats->exe,
            stats->utime,
            stats->stime,
            stats->iowait,
            stats->vm,
            stats->rss,
            stats->procs,
            stats->threads,
            stats->read_bytes,
            stats->write_bytes,
            stats->rchar,
            stats->wchar,
            stats->syscr,
            stats->syscw);

    if (size >= maxsize) {
        error("JSON too large for buffer: %d > %d", size, maxsize);
        return -1;
    }

    return size;
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

static void send_rabbitmq(MonitoringContext *ctx, ProcStats *stats) {
    debug("Sending stats to rabbitmq endpoint");

    char msg[1024];
    if (json_encode(ctx, stats, msg, 1024) < 0) {
        error("Unable to json encode message");
        return;
    }

    char b64msg[1024];
    if (base64_encode(msg, strlen(msg), b64msg, 1024) < 0) {
        error("Unable to base64 encode message");
        return;
    }

    char payload[1024];
    if (snprintf(payload, 1024,
        "{\"properties\":{},\"routing_key\":\"%s\",\"payload\":\"%s\",\"payload_encoding\":\"base64\"}",
        ctx->wf_uuid, b64msg) >= 1024) {
        error("RabbitMQ payload too large for buffer");
        return;
    }

    /* Need to construct a new URL */
    char url[128];
    if (strstr(ctx->url, "rabbitmqs://") == ctx->url) {
        snprintf(url, 128, "https://%s", ctx->url + strlen("rabbitmqs://"));
    } else {
        snprintf(url, 128, "http://%s", ctx->url + strlen("rabbitmq://"));
    }

    send_http_msg(url, payload);
}

static void send_http(MonitoringContext *ctx, ProcStats *stats) {
    debug("Sending stats to http endpoint");
    char msg[1024];
    if (json_encode(ctx, stats, msg, 1024) < 0) {
        error("Unable to json encode message");
        return;
    }
    send_http_msg(ctx->url, msg);
}

int send_msg_to_kickstart(char *host, char *port, ProcStats *stats) {
    if (host == NULL || port == NULL) {
        return -1;
    }

    struct addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    struct addrinfo *servinfo;
    int gaierr = getaddrinfo(host, port, &hints, &servinfo);
    if (gaierr != 0) {
        printerr("getaddrinfo: %s\n", gai_strerror(gaierr));
        return -1;
    }

    int sockfd;
    struct addrinfo *p;
    for (p = servinfo; p != NULL; p = p->ai_next) {
        sockfd = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
        if (sockfd == -1) {
            printerr("socket: %s\n", strerror(errno));
            continue;
        }

        if (connect(sockfd, p->ai_addr, p->ai_addrlen) == -1) {
            printerr("connect: %s\n", strerror(errno));
            close(sockfd);
            continue;
        }

        // Successful connection
        break;
    }

    freeaddrinfo(servinfo);

    if (p == NULL) {
        printerr("failed to connect to kickstart monitor: no suitable addr\n");
        return -1;
    }

    if (send(sockfd, stats, sizeof(ProcStats), 0) < 0) {
        printerr("Error during msg send: %s\n", strerror(errno));
        return -1;
    }

    close(sockfd);

    return 0;
}

static void send_kickstart(MonitoringContext *ctx, ProcStats *stats) {
    debug("Sending stats to kickstart endpoint");

    /* Get host and port from URL */
    char host[128];
    char port[16];
    /* FIXME We should probably just do this once */
    if (sscanf(ctx->url, "kickstart://%127[^:]:%15[0-9]", host, port) != 2) {
        error("Unable to parse kickstart URL: %s", ctx->url);
        return;
    }

    send_msg_to_kickstart(host, port, stats);
}

static void send_file(MonitoringContext *ctx, ProcStats *stats) {

    char msg[1024];
    if (json_encode(ctx, stats, msg, 1024) < 0) {
        error("Unable to json encode message");
        return;
    }

    char *path = ctx->url + strlen("file://");
    debug("Writing monitoring data to %s", path);

    FILE *log = fopen(path, "a");
    if (log == NULL) {
        error("Unable to open monitoring log '%s': %s", path, strerror(errno));
        return;
    }

    fprintf(log, "%s\n", msg);

    fclose(log);
}

/* Merge the stats we have and send them to the parent monitor */
static int send_report(MonitoringContext *ctx, ProcStatsList *listptr) {
    ProcStats stats;
    procfs_merge_stats_list(listptr, &stats, ctx->interval);

    if (strstr(ctx->url, "rabbitmq://") == ctx->url ||
        strstr(ctx->url, "rabbitmqs://") == ctx->url) {
        send_rabbitmq(ctx, &stats);
    } else if (strstr(ctx->url, "http://") == ctx->url ||
               strstr(ctx->url, "https://") == ctx->url) {
        send_http(ctx, &stats);
    } else if (strstr(ctx->url, "kickstart://") == ctx->url) {
        send_kickstart(ctx, &stats);
    } else if (strstr(ctx->url, "file://") == ctx->url) {
        send_file(ctx, &stats);
    } else {
        error("Unknown endpoint URL scheme: %s\n", ctx->url);
    }

    return 0;
}

/* purpose: find an ephemeral port available on a machine for further socket-based communication;
 *          opens a new socket on an ephemeral port, returns this port number and hostname
 *          where kickstart will listen for monitoring information
 * paramtr: kickstart_hostname (OUT): a pointer where the hostname of the kickstart machine will be stored,
 *          kickstart_port (OUT): a pointer where the available ephemeral port number will be stored
 * returns: 0  success
 *          -1 failure
 */
static int create_ephemeral_endpoint(char *kickstart_hostname, int *kickstart_port) {

    int listenfd = socket(AF_INET, SOCK_STREAM|SOCK_CLOEXEC, 0);
    if (listenfd < 0 ) {
        printerr("ERROR[socket]: %s\n", strerror(errno));
        return -1;
    }

    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = htonl(INADDR_ANY); 
    serv_addr.sin_port = 0;

    if (bind(listenfd, (const struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        printerr("ERROR[bind]: %s\n", strerror(errno));
        goto error;
    }

    if (listen(listenfd, 1) < 0 ) {
        printerr("ERROR[listen]: %s\n", strerror(errno));
        goto error;
    }

    memset(&serv_addr, 0, sizeof(serv_addr));
    socklen_t addr_len = sizeof(serv_addr);
    if (getsockname(listenfd, (struct sockaddr*)&serv_addr, &addr_len) == -1) {
        printerr("ERROR[getsockname]: %s\n", strerror(errno));
        goto error;
    }

    if (gethostname(kickstart_hostname, BUFSIZ)) {
        printerr("ERROR[gethostname]: %s\n", strerror(errno));
        return -1;
    }
    *kickstart_port = ntohs(serv_addr.sin_port);

    return listenfd;

error:
    close(listenfd);
    return -1;
}

static int handle_client(MonitoringContext *ctx, ProcStatsList **list) {
    struct sockaddr_in client_addr;
    socklen_t client_add_len = sizeof(client_addr);
    memset(&client_addr, 0, sizeof(client_addr));
    int incoming_socket = accept(ctx->socket, (struct sockaddr *)&client_addr, &client_add_len);
    if (incoming_socket < 0) {
        printerr("[mon-thread] ERROR[accept]: %s\n", strerror(errno));
        if (errno == EINTR) {
            goto next;
        } else {
            return -1;
        }
    }

    ProcStats stats;
    int sz = recv(incoming_socket, &stats, sizeof(ProcStats), 0);
    if (sz < 0) {
        printerr("[mon-thread] ERROR[recv]: %s\n", strerror(errno));
        goto next;
    }
    if (sz != sizeof(ProcStats)) {
        printerr("Invalid message\n");
        goto next;
    }

    /* Update the state of the process in our list */
    procfs_list_update(list, &stats);

next:
    close(incoming_socket);
    return 0;
}

void* monitoring_thread_func(void* arg) {
    MonitoringContext *ctx = (MonitoringContext *)arg;

    info("Monitoring thread starting...");
    debug("url: %s", ctx->url);
    debug("wf uuid: %s", ctx->wf_uuid);
    debug("wf label: %s", ctx->wf_label);
    debug("dag job id: %s", ctx->dag_job_id);
    debug("condor job id: %s", ctx->condor_job_id);
    debug("xformation: %s", ctx->xformation);
    debug("task id: %s", ctx->task_id);
    debug("process group: %d", getpgid(0));

    curl_global_init(CURL_GLOBAL_ALL);

    /* Create timer for monitoring interval */
    int timer = timerfd_create(CLOCK_MONOTONIC, TFD_CLOEXEC);
    struct itimerspec timercfg;
    timercfg.it_value.tv_sec = ctx->interval;
    timercfg.it_value.tv_nsec = 0;
    timercfg.it_interval.tv_sec = ctx->interval; /* Fire every interval seconds */
    timercfg.it_interval.tv_nsec = 0;
    if (timerfd_settime(timer, 0, &timercfg, NULL) < 0) {
        printerr("Error setting timerfd time: %s\n", strerror(errno));
        pthread_exit(NULL);
    }

    ProcStatsList *list = NULL;

    int signalled = 0;
    while (!signalled) {

        /* Poll signal_pipe, socket, and timer to see which one is readable */
        struct pollfd fds[3];
        fds[0].fd = signal_pipe[0];
        fds[0].events = POLLIN;
        fds[1].fd = ctx->socket;
        fds[1].events = POLLIN;
        fds[2].fd = timer;
        fds[2].events = POLLIN;
        if (poll(fds, 3, -1) <= 0) {
            printerr("Error polling: %s\n", strerror(errno));
            break;
        }

        /* If signal_pipe[0] is readable, then stop the thread. Note that you
         * could theoretically have some clients waiting for accept(). That is
         * not too likely because by the time we are stoping the thread,
         * wait() has returned in the main thread, so there shouldn't be any
         * clients left, but if libinterpose is used, then it will send a
         * last message when it exits, which might not arrive until later.
         */
        if (fds[0].revents & POLLIN) {
            debug("Monitoring thread caught signal");
            signalled = 1;
        }

        /* Got a client connection */
        if (fds[1].revents & POLLIN) {
            trace("Got client connection");
            if (handle_client(ctx, &list) < 0) {
                break;
            }

            if (signalled) {
                /* Must be the last process on libinterpose, send a message */
                send_report(ctx, list);
            }
        }

        /* Timer fired */
        if (fds[2].revents & POLLIN) {
            unsigned long long expirations = 0;
            if (read(timer, &expirations, sizeof(expirations)) < 0) {
                error("timerfd read failed: %s\n", strerror(errno));
            } else if (expirations > 1) {
                warn("timer expired %llu times\n", expirations);
            }

            trace("Timer expired");

            /* Update the list of process stats */
            procfs_read_stats_group(&list);

            /* Send a monitoring message */
            send_report(ctx, list);
        }
    }

    info("Monitoring thread exiting");
    procfs_free_stats_list(list);
    close(timer);
    close(ctx->socket);
    curl_global_cleanup();
    release_monitoring_context(ctx);
    pthread_exit(NULL);
}

int start_monitoring_thread() {
    /* Make sure the calling process is in its own process group */
    setpgid(0, 0);

    /* Set up parameters for the thread */
    MonitoringContext *ctx = calloc(1, sizeof(MonitoringContext));
    if (initialize_monitoring_context(ctx) < 0) {
        return -1;
    }

    /* Find a host and port to use */
    char socket_host[BUFSIZ];
    int socket_port;
    ctx->socket = create_ephemeral_endpoint(socket_host, &socket_port);
    if (ctx->socket < 0) {
        error("Couldn't create endpoint for monitoring\n");
        return -1;
    }

    /* Set the monitoring environment */
    char envvar[128];
    /* TODO If we are not aggregating, don't override this so that children
     * will report to the same destination that we are. XXX Make sure that
     * libinterpose can handle all the endpoint types first.
     */
    snprintf(envvar, 128, "kickstart://%s:%d", socket_host, socket_port);
    setenv("KICKSTART_MON_URL", envvar, 1);

    /* Create a pipe to signal between the main thread and the monitor thread */
    int rc = pipe(signal_pipe);
    if (rc < 0) {
        printerr("ERROR: Unable to create signal pipe: %s\n", strerror(errno));
        return rc;
    }
    rc = fcntl(signal_pipe[0], F_SETFD, FD_CLOEXEC);
    if (rc < 0) {
        printerr("WARNING: Unable to set CLOEXEC on pipe: %s\n", strerror(errno));
    }
    rc = fcntl(signal_pipe[1], F_SETFD, FD_CLOEXEC);
    if (rc < 0) {
        printerr("WARNING: Unable to set CLOEXEC on pipe: %s\n", strerror(errno));
    }

    /* Start and detach the monitoring thread */
    rc = pthread_create(&monitoring_thread, NULL, monitoring_thread_func, (void*)ctx);
    if (rc) {
        printerr("ERROR: return code from pthread_create() is %d: %s\n", rc, strerror(errno));
        return rc;
    }

    return 0;
}

int stop_monitoring_thread() {
    /* Signal the thread to stop */
    char msg = 1;
    int rc = write(signal_pipe[1], &msg, 1);
    if (rc <= 0) {
        printerr("ERROR: Problem signalling monitoring thread: %s\n", strerror(errno));
        return rc;
    }

    /* Wait for the monitoring thread */
    pthread_join(monitoring_thread, NULL);

    /* Close the pipe */
    close(signal_pipe[0]);
    close(signal_pipe[1]);

    return 0;
}

