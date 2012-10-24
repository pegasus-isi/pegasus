#include <mpi.h>

#include "mpicomm.h"
#include "protocol.h"
#include "failure.h"
#include "tools.h"

MPICommunicator::MPICommunicator(int *argc, char ***argv) {
    MPI_Init(argc, argv);
    MPI_Errhandler_set(MPI_COMM_WORLD, MPI_ERRORS_ARE_FATAL);
    bytes_sent = 0;
    bytes_recvd = 0;
    sleep_on_recv = true;
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

Message *MPICommunicator::recv_message(double timeout) {
    // We wait for the message first in order to get the size
    // so that we can allocate an appropriate buffer.
    int msgsize = wait_for_message(timeout);
    if (msgsize < 0) {
        return NULL;
    }
    
    char *msg = new char[msgsize];
    
    MPI_Status status;
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

int MPICommunicator::wait_for_message(double timeout) {
    /* On many MPI implementations MPI_Probe uses a busy wait loop. This
     * really wreaks havoc on the load and CPU utilization of the workers 
     * when there are no tasks to process or some slots are idle due to 
     * limited resource availability (memory and CPUs), and of the master
     * when there are no tasks to schedule or all slots are busy. In order
     * to avoid that we check here to see if there are any messages first,
     * and if there are not, then we wait for a few millis before checking
     * again and keep doing that until there is a message waiting. This 
     * should reduce the load/CPU usage on the system significantly. It
     * decreases responsiveness a bit, but it is a fair tradeoff.
     */
    MPI_Status status;
    if (sleep_on_recv || timeout > 0) {
        double start = current_time();
        unsigned i = 0;
        while (1) {
            i++;
            
            int message = 0;
            MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &message, &status);
            if (message) {
                // We got the message
                break;
            }
            
            if (timeout > 0) {
                double now = current_time();
                if (now-start >= timeout) {
                   return -1;
                }
            }
            
            // This causes the loop to spin rapidly for 1 second
            useconds_t sleeptime = 10; // Default is 10 usec
            if (i > 1e5) {
                // Increase sleep time to 0.5 sec after 
                // 1e5 x 10 usec = 1 second has elapsed
                // This is only approximate of course
                sleeptime = 5e5;
            }
            
            if (usleep(sleeptime)) {
                // The sleep was interrupted by a signal
                return -1;
            }
        }
    } else {
        // This call blocks, potentially in a busy loop depending on the
        // MPI implementation used
        MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
    }
    
    // Return the size of the waiting message
    int msgsize;
    MPI_Get_count(&status, MPI_CHAR, &msgsize);
    return msgsize;
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

