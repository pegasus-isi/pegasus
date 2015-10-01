#ifndef KICKSTART_INTERPOSE_H
#define KICKSTART_INTERPOSE_H

typedef struct {
    unsigned long long rchar;
    unsigned long long wchar;
    unsigned long syscr;
    unsigned long syscw;
    unsigned long long read_bytes;
    unsigned long long write_bytes;
    unsigned long long cancelled_write_bytes;
} IoUtilInfo;

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

FILE *_interpose_fopen_untraced(const char *path, const char *mode);
int _interpose_fclose_untraced(FILE *fp);
char *_interpose_fgets_untraced(char *s, int size, FILE *stream);
void _interpose_read_exe(char *exe, int maxsize);
int _interpose_vfprintf_untraced(FILE *stream, const char *format, va_list ap);
size_t _interpose_fread_untraced(void *ptr, size_t size, size_t nmemb, FILE *stream);
int _interpose_dup_untraced(int fd);

#ifdef HAS_PAPI
#define n_papi_events 8
#endif

#endif /* KICKSTART_INTERPOSE_H */

