#include <stdarg.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>

#include "log.h"

#define LOG_MAX 512

static LogLevel log_level = LOG_WARN;

static char *log_name = NULL;

static int log_output = STDERR_FILENO;

static const char *log_levels[] = {
    "FATAL",
    "ERROR",
    "WARNING",
    "INFO",
    "DEBUG",
    "TRACE"
};

void log_set_default_level() {
    char *envptr = getenv("KICKSTART_LOG_LEVEL");
    if (envptr == NULL) {
        return;
    }

    for (int i=LOG_FATAL; i<=LOG_TRACE; i++) {
        if (strcasecmp(envptr, log_levels[i]) == 0) {
            log_set_level(i);
            return;
        }
    }

    error("Invalid log level: %s", envptr);
}

void log_set_level(LogLevel level) {
    if (level < LOG_FATAL || level > LOG_TRACE) {
        return;
    }
    log_level = level;
}

LogLevel log_get_level() {
    return log_level;
}

void log_set_name(const char *name) {
    if (log_name != NULL) {
        free(log_name);
    }
    if (name == NULL) {
        log_name = NULL;
    } else {
        log_name = strdup(name);
    }
}

void log_set_output(int fd) {
    log_output = fd;
}

int log_printf(LogLevel level, char *file, int line, const char *format, ...) {
    if (level > log_level || level < LOG_FATAL) {
        return 0;
    }

    time_t ts = time(NULL);
    struct tm *now = localtime(&ts);
    char timestamp[32];
    strftime(timestamp, 32, "%FT%T%z", now);

    char logformat[LOG_MAX];
    if (snprintf(logformat, LOG_MAX, "%s %s[%d] %s:%d %s: %s\n", timestamp,
                 log_name, getpid(), file, line, log_levels[level], format) >= LOG_MAX) {
        warn("truncated log message follows");
        logformat[LOG_MAX-1] = '\n';
    }

    /* Old error messages come with an extra '\n' */
    int len = strlen(logformat);
    if (logformat[len-2] == '\n') {
        logformat[len-1] = '\0';
    }

    va_list args;
    va_start(args, format);
    int chars = vdprintf(log_output, logformat, args);
    va_end(args);

    return chars;
}

