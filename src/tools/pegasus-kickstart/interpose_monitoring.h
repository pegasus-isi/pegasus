#ifndef KICKSTART_INTERPOSE_MONITORING_H
#define KICKSTART_INTERPOSE_MONITORING_H

#include "procfs.h"

void interpose_spawn_monitoring_thread();
void interpose_stop_monitoring_thread();
void interpose_send_stats(ProcStats *stats);

#endif /* KICKSTART_INTERPOSE_MONITORING_H */
