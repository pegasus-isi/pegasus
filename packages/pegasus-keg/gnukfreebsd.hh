#ifndef _PEGASUS_LINUX_HH
#define _PEGASUS_LINUX_HH

#include <sys/types.h>

extern 
void 
pegasus_statfs( char* buffer, size_t capacity );

extern 
void
pegasus_loadavg( char* buffer, size_t capacity );

extern
void
pegasus_meminfo( char* buffer, size_t capacity ); 

extern
void
pegasus_cpuinfo( char* buffer, size_t capacity ); 

#endif // _PEGASUS_LINUX_HH
