#include <stdarg.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

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

    char logformat[LOG_MAX];
    if (snprintf(logformat, LOG_MAX, "%s[%d] %s %s:%d: %s\n", log_name, getpid(),
                 log_levels[level], file, line, format) >= LOG_MAX) {
        warn("truncated log message follows");
        logformat[LOG_MAX-1] = '\n';
    }

    va_list args;
    va_start(args, format);
    int chars = vdprintf(log_output, logformat, args);
    va_end(args);

    return chars;
}

