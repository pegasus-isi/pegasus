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

#include "error.h"
#include "monitoring.h"
#include "procfs.h"

typedef struct {
    char *url;
    char *credentials;
    char *wf_label;
    char *wf_uuid;
    char *dag_job_id;
    char *condor_job_id;
    char *xformation;
    char *task_id;
    int socket;
    int interval;
} MonitoringThreadContext;

/* This pipe is used to send a shutdown message from the main thread to the
 * monitoring thread */
static int signal_pipe[2];
static pthread_t monitoring_thread;

// a util function for reading env variables by the main kickstart process
// with monitoring endpoint data or set default values
static int initialize_monitoring_context(MonitoringThreadContext *ctx) {
    char* envptr;

    envptr = getenv("KICKSTART_MON_ENDPOINT_URL");
    if (envptr == NULL) {
        printerr("ERROR: KICKSTART_MON_ENDPOINT_URL not specified\n");
        return -1;
    }
    ctx->url = strdup(envptr);

    envptr = getenv("KICKSTART_MON_ENDPOINT_CREDENTIALS");
    if (envptr == NULL) {
        printerr("ERROR: KICKSTART_MON_ENDPOINT_CREDENTIALS not specified\n");
        return -1;
    }
    ctx->credentials = strdup(envptr);

    envptr = getenv("PEGASUS_WF_UUID");
    if (envptr == NULL) {
        printerr("ERROR: PEGASUS_WF_UUID not specified\n");
        return -1;
    }
    ctx->wf_uuid = strdup(envptr);

    envptr = getenv("PEGASUS_WF_LABEL");
    if (envptr == NULL) {
        printerr("ERROR: PEGASUS_WF_LABEL not specified\n");
        return -1;
    }
    ctx->wf_label = strdup(envptr);

    envptr = getenv("PEGASUS_DAG_JOB_ID");
    if (envptr == NULL) {
        printerr("ERROR: PEGASUS_DAG_JOB_ID not specified\n");
        return -1;
    }
    ctx->dag_job_id = strdup(envptr);

    envptr = getenv("CONDOR_JOBID");
    if (envptr == NULL) {
        printerr("ERROR: CONDOR_JOBID not specified\n");
        return -1;
    }
    ctx->condor_job_id = strdup(envptr);

    envptr = getenv("PEGASUS_XFORMATION");
    if (envptr == NULL) {
        ctx->xformation = NULL;
    } else {
        ctx->xformation = strdup(envptr);
    }

    envptr = getenv("PEGASUS_TASK_ID");
    if (envptr == NULL) {
        ctx->task_id = NULL;
    } else {
        ctx->task_id = strdup(envptr);
    }

    return 0;
}

static void release_monitoring_context(MonitoringThreadContext* ctx) {
    if (ctx == NULL) return;
    if (ctx->url != NULL) free(ctx->url);
    if (ctx->credentials != NULL) free(ctx->credentials);
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

/* sending this message to rabbitmq */
static void send_msg_to_mq(char *msg_buff, MonitoringThreadContext *ctx) {
    CURL *curl = curl_easy_init();
    if (curl == NULL) {
        printerr("[mon-thread] Error initializing curl\n");
        return;
    }

    curl_easy_setopt(curl, CURLOPT_URL, ctx->url);
    curl_easy_setopt(curl, CURLOPT_USERPWD, ctx->credentials);
    curl_easy_setopt(curl, CURLOPT_POST, 1);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);

    char payload[BUFSIZ];
    if (snprintf(payload, BUFSIZ,
        "{\"properties\":{},\"routing_key\":\"%s\",\"payload\":\"%s\",\"payload_encoding\":\"string\"}",
        ctx->wf_uuid, msg_buff) >= BUFSIZ) {
        printerr("[mon-thread] Message too large for buffer: %lu\n", strlen(msg_buff));
        return;
    }
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, payload);

    struct curl_slist *http_header = NULL;
    http_header = curl_slist_append(http_header, "Content-Type: application/json");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, http_header);

    /* Perform the request, res will get the return code */
    CURLcode res = curl_easy_perform(curl);
    /* Check for errors */
    if (res != CURLE_OK) {
        printerr("[mon-thread] an error occured while sending measurement: %s\n",
            curl_easy_strerror(res));
    }

    /* always cleanup */
    curl_easy_cleanup(curl);
    curl_slist_free_all(http_header);
}

/* purpose: find an ephemeral port available on a machine for further socket-based communication;
 *          opens a new socket on an ephemeral port, returns this port number and hostname
 *          where kickstart will listen for monitoring information
 * paramtr: kickstart_hostname (OUT): a pointer where the hostname of the kickstart machine will be stored,
 *          kickstart_port (OUT): a pointer where the available ephemeral port number will be stored
 * returns:	0  success
 *		    -1 failure
 */
static int create_ephemeral_endpoint(char *kickstart_hostname, int *kickstart_port) {

    int listenfd = socket(AF_INET, SOCK_STREAM, 0);
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

    printerr("Host: %s Port: %d\n", kickstart_hostname, *kickstart_port);

    return listenfd;

error:
    close(listenfd);
    return -1;
}

static int handle_client(MonitoringThreadContext *ctx) {

    /* Accept a network connection and read the message */
    struct sockaddr_in client_addr;
    socklen_t client_add_len = sizeof(client_addr);
    bzero((char *)&client_addr, sizeof(client_addr));
    int incoming_socket = accept(ctx->socket, (struct sockaddr *)&client_addr, &client_add_len);
    if (incoming_socket < 0) {
        printerr("[mon-thread] ERROR[accept]: %s\n", strerror(errno));
        if (errno == EINTR) {
            goto next;
        } else {
            return -1;
        }
    }

    char line[BUFSIZ];
    int num_bytes = recv(incoming_socket, line, BUFSIZ, 0);
    if (num_bytes < 0) {
        printerr("[mon-thread] ERROR[recv]: %s\n", strerror(errno));
        goto next;
    }

    // replace end line with a terminating character
    char *pos;
    if ((pos = strchr(line, '\n')) != NULL) {
        *pos = '\0';
    }

    // a proper monitoring message should start with a timestamp
    if (strstr(line, "ts=") != line) {
        printerr("[mon-thread] ERROR: Message did not start with 'ts=': \n%s\n", line);
        goto next;
    }

    // Add all the extra information
    char enriched_line[BUFSIZ];
    snprintf(enriched_line, BUFSIZ, "%s wf_uuid=%s wf_label=%s dag_job_id=%s condor_job_id=%s xformation=%s task_id=%s",
        line, ctx->wf_uuid, ctx->wf_label, ctx->dag_job_id, ctx->condor_job_id, ctx->xformation, ctx->task_id);

    send_msg_to_mq(enriched_line, ctx);

next:
    close(incoming_socket);
    return 0;
}

static int handle_timeout(MonitoringThreadContext *ctx, ProcStatsList **listptr) {

    /* Update the list of process stats */
    procfs_read_stats_group(listptr);

    ProcStats stats;
    procfs_add_stats_list(*listptr, &stats);

    char msg[BUFSIZ];
    snprintf(msg, BUFSIZ, "wf_uuid=%s wf_label=%s dag_job_id=%s condor_job_id=%s "
            "xformation=%s task_id=%s utime=%.3f stime=%.3f iowait=%.3f "
            "vm=%llu rss=%llu threads=%d bread=%llu bwrite=%llu "
            "rchar=%llu wchar=%llu syscr=%lu syscw=%lu",
            ctx->wf_uuid, ctx->wf_label, ctx->dag_job_id, ctx->condor_job_id,
            ctx->xformation, ctx->task_id,
            stats.utime, stats.stime, stats.iowait, stats.vm, stats.rss,
            stats.threads, stats.read_bytes, stats.write_bytes,
            stats.rchar, stats.wchar, stats.syscr, stats.syscw);

    fprintf(stdout, "%s\n", msg);

    send_msg_to_mq(msg, ctx);

    return 0;
}

/*
 * Main monitoring thread loop - it periodically read global trace file as sent this info somewhere, e.g. to another file
 * or to an external service.
 * It parses any information from the global monitoring and calculates some mean values.
 */
void* monitoring_thread_func(void* arg) {
    MonitoringThreadContext *ctx = (MonitoringThreadContext *)arg;

    printerr("[mon-thread] url: %s\n", ctx->url);
    //printerr("[mon-thread] credentials: %s\n", ctx->credentials);
    printerr("[mon-thread] wf uuid: %s\n", ctx->wf_uuid);
    printerr("[mon-thread] wf label: %s\n", ctx->wf_label);
    printerr("[mon-thread] dag job id: %s\n", ctx->dag_job_id);
    printerr("[mon-thread] condor job id: %s\n", ctx->condor_job_id);
    printerr("[mon-thread] xformation: %s\n", ctx->xformation);
    printerr("[mon-thread] task id: %s\n", ctx->task_id);
    printerr("[mon-thread] process group: %d\n", getpgid(0));

    curl_global_init(CURL_GLOBAL_ALL);

    /* Create timer for monitoring interval */
    int timer = timerfd_create(CLOCK_MONOTONIC, TFD_CLOEXEC);
    struct itimerspec timercfg;
    timercfg.it_value.tv_sec = 0; /* Fire immediately at start */
    timercfg.it_value.tv_nsec = 1;
    timercfg.it_interval.tv_sec = ctx->interval; /* Fire every interval seconds */
    timercfg.it_interval.tv_nsec = 0;
    if (timerfd_settime(timer, 0, &timercfg, NULL) < 0) {
        printerr("[mon-thread] Error setting timerfd time: %s\n", strerror(errno));
        pthread_exit(NULL);
    }

    printerr("[mon-thread] Starting monitoring loop...\n");

    ProcStatsList *stats = NULL;

    while (1) {
        /* Poll signal_pipe, socket, and timer to see which one is readable */
        struct pollfd fds[3];
        fds[0].fd = signal_pipe[0];
        fds[0].events = POLLIN;
        fds[1].fd = ctx->socket;
        fds[1].events = POLLIN;
        fds[2].fd = timer;
        fds[2].events = POLLIN;
        if (poll(fds, 3, -1) <= 0) {
            printerr("[mon-thread] Error polling: %s\n", strerror(errno));
            break;
        }

        /* If signal_pipe[0] is readable, then stop the thread. Note that you
         * could theoretically have some clients waiting for accept(), but that
         * is not possible because by the time we are stoping the thread,
         * wait() has returned in the main thread, so there shouldn't be any
         * clients left.
         */
        if (fds[0].revents & POLLIN) {
            printerr("[mon-thread] Caught signal\n");
            break;
        }

        /* Got a client connection */
        if (fds[1].revents & POLLIN) {
            if (handle_client(ctx) < 0) {
                break;
            }
        }

        /* Timer fired */
        if (fds[2].revents & POLLIN) {
            unsigned long long expirations = 0;
            if (read(timer, &expirations, sizeof(expirations)) < 0) {
                printerr("[mon-thread] timerfd read failed: %s\n", strerror(errno));
            } else if (expirations > 1) {
                printerr("[mon-thread] WARNING: timer expired %llu times\n", expirations);
            }
            if (handle_timeout(ctx, &stats) < 0) {
                break;
            }
        }
    }

    printerr("[mon-thread] Monitoring thread exiting...\n");
    procfs_free_stats_list(stats);
    close(timer);
    close(ctx->socket);
    curl_global_cleanup();
    release_monitoring_context(ctx);
    pthread_exit(NULL);
}

int start_monitoring_thread(int interval) {
    /* Find a host and port to use */
    char socket_host[BUFSIZ];
    int socket_port;
    int socket = create_ephemeral_endpoint(socket_host, &socket_port);
    if (socket < 0) {
        printerr("Couldn't find an endpoint for communication with kickstart\n");
        return -1;
    }

    /* Set the monitoring environment */
    setenv("KICKSTART_MON_HOST", socket_host, 1);
    char envvar[10];
    snprintf(envvar, 10, "%d", socket_port);
    setenv("KICKSTART_MON_PORT", envvar, 1);

    printf("size = %lu\n", sizeof(ProcStats));

    /* Set up parameters for the thread */
    MonitoringThreadContext *ctx = calloc(sizeof(MonitoringThreadContext), 1);
    if (initialize_monitoring_context(ctx) < 0) {
        return -1;
    }
    ctx->socket = socket;
    ctx->interval = interval;

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

