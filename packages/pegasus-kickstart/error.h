#ifndef KICKSTART_ERROR_H
#define KICKSTART_ERROR_H

#include <stdio.h>
#include <unistd.h>

#define printerr(fmt, ...) \
    fprintf(stderr, "kickstart[%d]: %s[%d]: " fmt, \
            getpid(), __FILE__, __LINE__, ##__VA_ARGS__)

#endif /* KICKSTART_ERROR_H */
