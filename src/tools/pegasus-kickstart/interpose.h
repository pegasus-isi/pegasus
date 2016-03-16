#ifndef KICKSTART_INTERPOSE_H
#define KICKSTART_INTERPOSE_H

extern int myerr;

#define printerr(fmt, ...) \
    dprintf(myerr, "libinterpose[%d]: %s[%d]: " fmt, \
            getpid(), __FILE__, __LINE__, ##__VA_ARGS__)

#ifdef DEBUG
#define debug(format, args...) \
    dprintf(myerr, "libinterpose: " format "\n" , ##args)
#else
#define debug(format, args...)
#endif

void _interpose_read_exe(char *exe, int maxsize);

#endif /* KICKSTART_INTERPOSE_H */

