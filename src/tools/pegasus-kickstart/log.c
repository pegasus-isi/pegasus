#include <stdarg.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "log.h"

static LogLevel log_level = LOG_INFO;

static char *log_name = NULL;

static int log_output = STDERR_FILENO;

static const char *log_levels[] = {
    "FATAL",
    "ERROR",
    "INFO",
    "DEBUG",
    "TRACE"
};

void set_log_level(LogLevel level) {
    if (level < LOG_FATAL || level > LOG_TRACE) {
        return;
    }
    log_level = level;
}

LogLevel get_log_level() {
    return log_level;
}

void set_log_name(const char *name) {
    if (log_name != NULL) {
        free(log_name);
    }
    if (name == NULL) {
        log_name = NULL;
    } else {
        log_name = strdup(name);
    }
}

int logprintf(LogLevel level, char *file, int line, const char *format, ...) {
    if (level > log_level || level < LOG_FATAL) {
        return 0;
    }

    char logformat[1024];
    if (snprintf(logformat, 1024, "%s[%d] %-5s %s:%d: %s\n", log_name, getpid(), log_levels[level], file, line, format) >= 1024) {
        /* Truncated log message */
    }

    va_list args;
    va_start(args, format);
    int chars = vdprintf(log_output, logformat, args);
    va_end(args);

    return chars;
}

