#include "stdio.h"
#include "time.h"
#include "sys/time.h"

#include "log.h"

#define MAX_LOG_MESSAGE 8192

static int loglevel = LOG_INFO;
static FILE *logfile = stdout;

void log_set_level(int level) {
    loglevel = level;
}

int log_get_level() {
    return loglevel;
}

void log_set_file(FILE *log) {
    if (log == NULL) {
        log_warn("Log file being set to NULL");
    }
    logfile = log;
}

FILE *log_get_file() {
    return logfile;
}

static void timestr(char *dest) {
    struct timeval tod;
    gettimeofday(&tod, NULL);
    struct tm *t = localtime(&(tod.tv_sec));
    int ms = (int)(tod.tv_usec/1000.0);
    sprintf(dest, "%02d/%02d/%04d %02d:%02d:%02d.%.3d", 
        t->tm_mon+1, t->tm_mday, t->tm_year+1900, 
        t->tm_hour, t->tm_min, t->tm_sec, ms);
}

void log_message(int level, const char *message, va_list args) {
    // Just in case...
    if (logfile == NULL || ferror(logfile) || ftell(logfile) < 0) {
        logfile = stdout;
    }
    
    if (log_test(level)) {
        char logformat[MAX_LOG_MESSAGE];
        if (logfile == stdout) {
            snprintf(logformat, MAX_LOG_MESSAGE, "%s\n", message);    
        } else {
            // If logging to a file, add the date
            char ts[26];
            timestr(ts);
            snprintf(logformat, MAX_LOG_MESSAGE, "%s: %s\n", ts, message);
        }
        
        // If logging to stdio, send ERROR and FATAL to stderr, others to stdout
        if (logfile == stdout && level <= LOG_ERROR) {
            vfprintf(stderr, logformat, args);
        } else {
            vfprintf(logfile, logformat, args);
        }
    }
}

#define __LOG_MESSAGE(level) \
    va_list args; \
    va_start(args, format); \
    log_message(level, format, args); \
    va_end(args);

void log_fatal(const char *format, ...) {
    __LOG_MESSAGE(LOG_FATAL)
}

void log_error(const char *format, ...) {
    __LOG_MESSAGE(LOG_ERROR)
}

void log_warn(const char *format, ...) {
    __LOG_MESSAGE(LOG_WARN)
}

void log_info(const char *format, ...) {
    __LOG_MESSAGE(LOG_INFO)
}

void log_debug(const char *format, ...) {
    __LOG_MESSAGE(LOG_DEBUG)
}

void log_trace(const char *format, ...) {
    __LOG_MESSAGE(LOG_TRACE)
}

bool log_test(int level) {
    return (level <= loglevel);
}

bool log_fatal() {
    return log_test(LOG_FATAL);
}

bool log_error() {
    return log_test(LOG_ERROR);
}

bool log_warn() {
    return log_test(LOG_WARN);
}

bool log_info() {
    return log_test(LOG_INFO);
}

bool log_debug() {
    return log_test(LOG_DEBUG);
}

bool log_trace() {
    return log_test(LOG_TRACE);
}
