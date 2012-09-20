#include "failure.h"

#define MAX_FAILURE_MSG 2048

static char __msg_buffer[MAX_FAILURE_MSG];

Failure::Failure(const char *message) {
    this->message = new std::string(message);
}

Failure::~Failure() throw () {
    delete this->message;
}

const char* Failure::what() const throw () {
    return this->message->c_str();
}

static char *generate_message(const char *format, va_list args, bool error_message) {
    snprintf(__msg_buffer, MAX_FAILURE_MSG, "%s(%d): ", __FILE__, __LINE__);
    
    int off = strlen(__msg_buffer);
    vsnprintf(__msg_buffer+off, MAX_FAILURE_MSG-off, format, args);
    
    if (error_message) {
        off = strlen(__msg_buffer);
        snprintf(__msg_buffer+off, MAX_FAILURE_MSG-off, ": %s", strerror(errno));
    }
    
    return __msg_buffer;
}

void myfailure(const char *format, ...) {
    va_list args;
    va_start(args, format);
    char *msg = generate_message(format, args, false);
    va_end(args);
    throw Failure(msg);
}

void myfailures(const char *format, ...) {
    va_list args;
    va_start(args, format);
    char *msg = generate_message(format, args, true);
    va_end(args);
    throw Failure(msg);
}

