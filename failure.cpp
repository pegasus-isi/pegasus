#include "failure.h"

Failure::Failure(const char *message) {
    this->message = new std::string(message);
}

Failure::~Failure() throw () {
    delete this->message;
}

const char* Failure::what() const throw () {
    return this->message->c_str();
}
