#ifndef KICKSTART_ERROR_H
#define KICKSTART_ERROR_H

#include "log.h"

#define printerr(fmt, ...) \
    log_printf(LOG_ERROR, __FILE__, __LINE__, fmt, ##__VA_ARGS__)

#endif /* KICKSTART_ERROR_H */
