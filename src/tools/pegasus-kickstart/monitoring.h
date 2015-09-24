#ifndef _PEGASUS_MONITORING_H
#define _PEGASUS_MONITORING_H

int start_monitoring_thread(char* socked_desc_buf);
/* purpose: read environment variables, starts a monitoring thread and detaches it
 * paramtr:	monitoring_thread (OUT) a thread struct
 *			kickstart_status  (IN)	an absolute path to a global trace file
 * returns:	0 success
 *			1 failure
 */

int find_ephemeral_endpoint(char *kickstart_hostname, char *kickstart_port);
/* purpose: find an ephemeral port available on a machine for further socket-based communication;
 *          opens a new socket on an ephemeral port, returns this port number and hostname
 *          where kickstart will listen for monitoring information
 * paramtr: kickstart_hostname (OUT): a pointer where the hostname of the kickstart machine will be stored,
 *          kickstart_port (OUT): a pointer where the available ephemeral port number will be stored
 * returns:	0  success
 *		    -1 failure
 */

#endif /* _PEGASUS_MONITORING_H */
