#ifndef KICKSTART_LOG
#define KICKSTART_LOG

typedef enum _LogLevel {
    LOG_FATAL,
    LOG_ERROR,
    LOG_WARN,
    LOG_INFO,
    LOG_DEBUG,
    LOG_TRACE
} LogLevel;

void log_set_default_level();
void log_set_level(LogLevel level);
LogLevel log_get_level();
void log_set_name(const char *name);
void log_set_output(int fd);
int log_printf(LogLevel level, char *file, int line, const char *format, ...);
#define fatal(fmt, ...) log_printf(LOG_FATAL, __FILE__, __LINE__, fmt, ##__VA_ARGS__)
#define error(fmt, ...) log_printf(LOG_ERROR, __FILE__, __LINE__, fmt, ##__VA_ARGS__)
#define warn(fmt, ...) log_printf(LOG_WARN, __FILE__, __LINE__, fmt, ##__VA_ARGS__)
#define info(fmt, ...) log_printf(LOG_INFO, __FILE__, __LINE__, fmt, ##__VA_ARGS__)
#define debug(fmt, ...) log_printf(LOG_DEBUG, __FILE__, __LINE__, fmt, ##__VA_ARGS__)
#define trace(fmt, ...) log_printf(LOG_TRACE, __FILE__, __LINE__, fmt, ##__VA_ARGS__)

#endif
