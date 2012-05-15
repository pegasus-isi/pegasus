#ifndef _TOOLS_H
#define _TOOLS_H

#include <string>
#include <stddef.h>
#include <time.h>
 
#ifndef HOST_NAME_MAX
#define HOST_NAME_MAX 255
#endif

char * isodate(time_t seconds, char* buffer, size_t size);
char * iso2date(double seconds_wf, char* buffer, size_t size);
void get_host_name(std::string &hostname);

#endif /* _TOOLS_H */
