#ifndef COMM_H
#define COMM_H

#include "protocol.h"

// Time in microseconds to sleep if there is no message waiting
#define NO_MESSAGE_SLEEP_TIME 50000

class Communicator {
public:
    Communicator() {}
    virtual ~Communicator() {}
    virtual void send_message(Message *message, int dest) = 0;
    virtual Message *recv_message() = 0;
    virtual bool message_waiting() = 0;
    virtual void barrier() = 0;
    virtual void abort(int exitcode) = 0;
    virtual int rank() = 0;
    virtual int size() = 0;
    virtual unsigned long sent() = 0;
    virtual unsigned long recvd() = 0;
};

#endif /* COMM_H */

