#include <mpi.h>

#include "mpicomm.h"
#include "protocol.h"
#include "failure.h"

MPICommunicator::MPICommunicator(int *argc, char ***argv) {
    MPI_Init(argc, argv);
    MPI_Errhandler_set(MPI_COMM_WORLD, MPI_ERRORS_ARE_FATAL);
    bytes_sent = 0;
    bytes_recvd = 0;
}

MPICommunicator::~MPICommunicator() {
    MPI_Finalize();
}

void MPICommunicator::send_message(Message *message, int dest) {
    char *msg = message->msg;
    unsigned msgsize = message->msgsize;
    int tag = message->tag();
    MPI_Send(msg, msgsize, MPI_CHAR, dest, tag, MPI_COMM_WORLD);
    bytes_sent += msgsize;
}

Message *MPICommunicator::recv_message() {
    // We probe first in order to get the status, which will tell us
    // the size of the message so that we can allocate an appropriate
    // buffer for it.
    MPI_Status status;
    MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
    
    // Allocate a buffer of the right size
    int msgsize;
    MPI_Get_count(&status, MPI_CHAR, &msgsize);
    char *msg = new char[msgsize];
    MPI_Recv(msg, msgsize, MPI_CHAR, MPI_ANY_SOURCE, MPI_ANY_TAG, 
            MPI_COMM_WORLD, &status);
    bytes_recvd += msgsize;
    
    // Create the right type of message
    Message *message = NULL;
    int source = status.MPI_SOURCE;
    MessageType type = (MessageType)status.MPI_TAG;
    switch(type) {
        case SHUTDOWN:
            message = new ShutdownMessage(msg, msgsize, source);
            break;
        case COMMAND:
            message = new CommandMessage(msg, msgsize, source);
            break;
        case RESULT:
            // The extra zero is just for disambiguation
            message = new ResultMessage(msg, msgsize, source, 0);
            break;
        case REGISTRATION:
            message = new RegistrationMessage(msg, msgsize, source);
            break;
        case HOSTRANK:
            message = new HostrankMessage(msg, msgsize, source);
            break;
        case IODATA:
            message = new IODataMessage(msg, msgsize, source);
            break;
        default:
            myfailure("Unknown message type: %d", type);
    }
    
    return message;
}

bool MPICommunicator::message_waiting() {
    int flag;
    MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &flag, MPI_STATUS_IGNORE);
    return flag != 0;
}

void MPICommunicator::barrier() {
    MPI_Barrier(MPI_COMM_WORLD);
}

void MPICommunicator::abort(int exitcode) {
    MPI_Abort(MPI_COMM_WORLD, exitcode);
}

int MPICommunicator::rank() {
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    return rank;
}

int MPICommunicator::size() {
    int size;
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    return size;
}

unsigned long MPICommunicator::sent() {
    return bytes_sent;
}

unsigned long MPICommunicator::recvd() {
    return bytes_recvd;
}

