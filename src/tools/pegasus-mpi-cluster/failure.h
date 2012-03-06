#ifndef FAILURE_H
#define FAILURE_H

#include <string>
#include <exception>
#include "stdio.h"
#include "string.h"
#include "errno.h"

class Failure : public std::exception {
    std::string *message;
public:
    Failure(const char *message);
    ~Failure() throw ();
    const char* what() const throw();
};

#define MAX_FAILURE_MSG 2048

#define failure(format, args...) \
    do { \
        char msg[MAX_FAILURE_MSG]; \
        snprintf(msg, MAX_FAILURE_MSG, "%s(%d): "format, __FILE__, __LINE__, ##args); \
        throw Failure(msg); \
    } while (0);

#define failures(format, args...) \
    do { \
        char msg[MAX_FAILURE_MSG]; \
        snprintf(msg, MAX_FAILURE_MSG, "%s(%d): "format": %s", __FILE__, __LINE__, ##args, strerror(errno)); \
        throw Failure(msg); \
    } while (0);

#endif /* FAILURE_H */
