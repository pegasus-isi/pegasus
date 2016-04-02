#ifndef _PEGASUS_MONITORING_H
#define _PEGASUS_MONITORING_H

#include "procfs.h"

int start_monitoring_thread();
int stop_monitoring_thread();

/* This is used by libinterpose for online monitoring */
int send_msg_to_kickstart(char *host, char *port, ProcStats *stats);

#endif /* _PEGASUS_MONITORING_H */
