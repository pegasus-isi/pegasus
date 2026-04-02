#include <stdio.h>
#include <stdlib.h>

#include "log.h"
#include "failure.h"

int main(int argc, char *argv[]) {
    FILE *log = fopen("/dev/null", "w");
    log_set_file(log);
    log_set_level(LOG_WARN);
    
    log_trace("NOT OK");
    log_debug("NOT OK");
    log_info("NOT OK");
    log_warn("OK");
    log_error("OK");
    log_fatal("OK");
    
    if (log_trace()) abort();
    if (log_debug()) abort();
    if (log_info()) abort();
    if (log_warn()) {log_warn();}
    if (log_error()) {log_error();}
    if (log_fatal()) {log_fatal();}
    
    fclose(log);
    
    log_info("NOT OK");
    log_warn("OK");
    
    log_set_file(NULL);
    log_warn("OK");

    /* Test the timestamp stuff
    FILE *logf = fopen("/tmp/foo.log","w");
    log_set_file(logf);

    log_warn("OK");
    log_warn("OK");
    log_warn("OK AGAIN");

    fclose(logf);
    */
    
    return 0;
}
