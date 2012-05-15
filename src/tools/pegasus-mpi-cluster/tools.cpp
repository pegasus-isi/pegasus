#include <string>
#include <errno.h>
#include <math.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include "tools.h"
#include "failure.h"


/* purpose: formats ISO 8601 timestamp into given buffer (simplified)
 * paramtr: seconds (IN): time stamp
 *          buffer (OUT): where to put the results
 *          size (IN): capacity of buffer
 * returns: pointer to start of buffer for convenience. 
 */
char * isodate( time_t seconds, char* buffer, size_t size ) {
    struct tm zulu = *gmtime(&seconds);
    struct tm local = *localtime(&seconds);
    zulu.tm_isdst = local.tm_isdst;
    {
        time_t distance = (seconds - mktime(&zulu)) / 60;
        int hours = distance / 60;
        int minutes = distance < 0 ? -distance % 60 : distance % 60;
        size_t len = strftime( buffer, size, "%Y-%m-%dT%H:%M:%S", &local );
        snprintf( buffer+len, size-len, "%+03d:%02d", hours, minutes );
    }
    return buffer;
}


/* purpose: formats ISO 8601 timestamp into given buffer (simplified)
 * paramtr: seconds_wf (IN): time stamp with fractional seconds (millis)
 *          buffer (OUT): where to put the results
 *          size (IN): capacity of buffer
 * returns: pointer to start of buffer for convenience. 
 */
char * iso2date( double seconds_wf, char* buffer, size_t size ) {
    char millis[8]; 
    double integral, fractional = modf(seconds_wf,&integral); 
    time_t seconds = (time_t) integral; 
    struct tm zulu = *gmtime(&seconds);
    struct tm local = *localtime(&seconds);
    zulu.tm_isdst = local.tm_isdst;
    snprintf( millis, sizeof(millis), "%.3f", fractional ); 
    {
        time_t distance = (seconds - mktime(&zulu)) / 60;
        int hours = distance / 60;
        int minutes = distance < 0 ? -distance % 60 : distance % 60;
        size_t len = strftime( buffer, size, "%Y-%m-%dT%H:%M:%S", &local );
        snprintf( buffer+len, size-len, "%s%+03d:%02d", millis+1, hours, minutes );
    }
    return buffer;
}


/* Get the local host name */
void get_host_name(std::string &hostname) {
    char name[HOST_NAME_MAX];
    if (gethostname(name, HOST_NAME_MAX) < 0) {
        myfailures("Unable to get host name");
    }
    hostname = name;
}
