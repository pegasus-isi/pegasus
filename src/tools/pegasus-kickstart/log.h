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

void set_log_level(LogLevel level);
LogLevel get_log_level();
void set_log_name(const char *name);
int logprintf(LogLevel level, char *file, int line, const char *format, ...);
#define fatal(fmt, ...) logprintf(LOG_FATAL, __FILE__, __LINE__, fmt, ##__VA_ARGS__)
#define error(fmt, ...) logprintf(LOG_ERROR, __FILE__, __LINE__, fmt, ##__VA_ARGS__)
#define warn(fmt, ...) logprintf(LOG_WARN, __FILE__, __LINE__, fmt, ##__VA_ARGS__)
#define info(fmt, ...) logprintf(LOG_INFO, __FILE__, __LINE__, fmt, ##__VA_ARGS__)
#define debug(fmt, ...) logprintf(LOG_DEBUG, __FILE__, __LINE__, fmt, ##__VA_ARGS__)
#define trace(fmt, ...) logprintf(LOG_TRACE, __FILE__, __LINE__, fmt, ##__VA_ARGS__)

#endif
