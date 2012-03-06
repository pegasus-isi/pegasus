#ifndef LOG_H
#define LOG_H

#include "stdio.h"
#include "stdarg.h"

#define LOG_FATAL 0
#define LOG_ERROR 1
#define LOG_WARN 2
#define LOG_INFO 3
#define LOG_DEBUG 4
#define LOG_TRACE 5

void log_set_level(int level);
int log_get_level();

void log_set_file(FILE *log);
FILE *log_get_file();

void log_message(int level, const char *message, va_list args);

void log_fatal(const char *format, ...);
void log_error(const char *format, ...);
void log_warn(const char *format, ...);
void log_info(const char *format, ...);
void log_debug(const char *format, ...);
void log_trace(const char *format, ...);

bool log_test(int level);

bool log_fatal();
bool log_error();
bool log_warn();
bool log_info();
bool log_debug();
bool log_trace();


#endif /* LOG_H */
