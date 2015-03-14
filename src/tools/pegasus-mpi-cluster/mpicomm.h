#ifndef MPICOMM_H
#define MPICOMM_H

#include "comm.h"

class MPICommunicator : public Communicator {
private:
    int myrank;
    int mysize;
    unsigned long bytes_sent;
    unsigned long bytes_recvd;
    virtual int wait_for_message(MPI_Status &status, double timeout);
    
public:
    bool sleep_on_recv;
    
    MPICommunicator(int *argc, char ***argv);
    virtual ~MPICommunicator();
    virtual void send_message(Message *message, int dest);
    virtual Message *recv_message(double timeout = 0);
    virtual bool message_waiting();
    virtual void barrier();
    virtual void abort(int exitcode);
    virtual int rank();
    virtual int size();
    virtual unsigned long sent();
    virtual unsigned long recvd();
};

#endif /* MPICOMM_H */

