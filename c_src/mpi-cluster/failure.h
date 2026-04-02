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

void myfailure(const char *format, ...);

void myfailures(const char *format, ...);

#endif /* FAILURE_H */
