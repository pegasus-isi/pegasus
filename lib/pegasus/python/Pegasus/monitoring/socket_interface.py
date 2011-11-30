#!/usr/bin/env python

"""
This file implements the debug socket interface available in pegasus-monitord.
"""

##
#  Copyright 2007-2011 University Of Southern California
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
##

# Import Python modules
import os
import re
import sys
import time
import socket
import logging
import traceback

# Import other Pegasus modules
from Pegasus.tools import utils
from Pegasus.monitoring.job import Job
from Pegasus.tools import kickstart_parser

# Get logger object (initialized elsewhere)
logger = logging.getLogger()

# Compile our regular expressions

# Used in untaint
re_clean_content = re.compile(r"([^-a-zA-z0-9_\s.,\[\]^\*\?\/\+])")

def server_socket(low, hi, bind_addr="127.0.0.1"):
    """
    purpose: create a local TCP server socket to listen to sitesel requests
    paramtr: low (IN): minimum port from bind range
    paramtr: hi (IN): maximum port from bind range
    paramtr: bind_addr (IN): optional hostaddr_in to bind to , defaults to LOOPBACK
    returns: open socket, or None on error
    """

    # Create socket
    try:
        my_socket = socket.socket(socket.AF_INET,
                                  socket.SOCK_STREAM,
                                  socket.getprotobyname("tcp"))
    except:
        logger.critical("could not create socket!")
        sys.exit(42)

    # Set options
    try:
        my_socket.setsockopt(socket.SOL_SOCKET,
                             socket.SO_REUSEADDR,
                             1)
    except:
        logger.critical("setsockopt SO_REUSEADDR!")
        sys.exit(42)

    # Bind to a free port
    my_port = low
    for my_port in range(low, hi):
        try:
            my_res = my_socket.bind((bind_addr, my_port))
        except:
            # Go to next port
            continue
        else:
            break

    if my_port >= hi:
        logger.critical("no free port to bind to!")
        sys.exit(42)

    # Make server socket non-blocking to not have a race condition
    # when doing select() before accept() on a server socket
    try:
        my_socket.setblocking(0)
    except:
        logger.critical("setblocking!")
        sys.exit(42)

    # Start listener
    try:
        my_socket.listen(socket.SOMAXCONN)
    except:
        logger.critical("listen!")
        sys.exit(42)

    # Return socket
    return my_socket

def untaint(text):
    """
    purpose: do not trust anything we get from the internet
    paramtr: text(IN): text to untaint
    returns: cleaned text, without any "special" characters
    """

    if text is None:
        return None

    my_text = re_clean_content.sub('', str(text))

    return my_text

def sendmsg(client_connection, msg):
    """
    purpose: send all data to socket connection, try several time if necessary
    paramtr: client_connection(IN): socket connection to send data
    paramtr: msg(IN): message to send
    returns: None on error, 1 on success
    """
    my_total_bytes_sent = 0

    while my_total_bytes_sent < len(msg):
        try:
            my_bytes_sent = client_connection.send(msg[my_total_bytes_sent:])
        except:
            logger.error("writing to socket!")
            return None

        my_total_bytes_sent = my_total_bytes_sent + my_bytes_sent

    return 1

def list_workflows(client_conn, wfs, param=""):
    """
    purpose: lists the workflows currently being tracked
    globals: wfs(IN): array of workflows
    """

    for workflow_entry in wfs:
        if workflow_entry.wf is not None:
            my_wf = workflow_entry.wf
            my_line = "%d - %s\r\n" % (my_wf._workflow_start, my_wf._wf_uuid)
        sendmsg(client_conn, my_line)

jumptable = {'list-wf': list_workflows}

def service_request(server, wfs):
    """
    purpose: accept an incoming connection and service its request
    paramtr: server(IN): server socket with a pending connection request
    returns: number of status lines, or None in case of error
    """

    # First, we accept the connection
    try:
        my_conn, my_addr = server.accept()
    except:
        logger.error("accept!")
        return None

    my_count = 0
    logger.info("processing request from %s:%d" % (my_addr[0], my_addr[1]))

    # TODO: Can only handle 1 line up to 1024 bytes long, should fix this later
    # Read line fron socket
    while True:
        try:
            my_buffer = my_conn.recv(1024)
        except socket.error, e:
            if e[0] == 35:
                continue
            else:
                logger.error("recv: %d:%s" % (e[0], e[1]))
                try:
                    # Close socket
                    my_conn.close()
                except:
                    pass
                return None
        else:
            # Received line, leave loop
            break

    if my_buffer == '':
        # Nothing else to read
        try:
            my_conn.close()
        except:
            pass
        return my_count

    # Removed leading/trailing spaces/tabs, trailing \r\n
    my_buffer = my_buffer.strip()
    # Do not trust anything we get from the internet
    my_buffer = untaint(my_buffer)
    
    # Create list of tokens
    my_args = my_buffer.split()
    
    if len(my_args) < 3:
        # Clearly not enough information
        sendmsg(my_conn, "%s 204 No Content\r\n" % (speak))
        try:
            my_conn.close()
        except:
            pass
        return my_count

    # Read information we need
    my_proto = my_args.pop(0).upper()
    my_method = my_args.pop(0)
    my_what = my_args.pop(0)

    if my_proto != speak:
        # Illegal or unknown protocol
        sendmsg(my_conn, "%s 400 Bad request\r\n" % (speak))
    elif my_method.upper() != "GET":
        # Unsupported method
        sendmsg(my_conn, "%s 405 Method not allowed\r\n" % (speak))
    elif not my_what in jumptable:
        # Request item is not supported
        sendmsg(my_conn, "%s 501 Not implemented\r\n" % (speak))
    else:
        # OK, process the command
        sendmsg(my_conn, "%s 200 OK\r\n" % (speak))
        my_count = jumptable[my_what](my_conn, wfs, " ".join(my_args).lower())

    try:
        my_conn.close()
    except:
        pass

    return my_count
	
def check_request(server, wfs, timeout=0):
    """
    purpose: check for a pending service request, and service it
    paramtr: server(IN): server socket
    paramtr: timeout(IN, OPT): timeout in seconds, defaults to 0
    returns: return value of select on server socket
    """
    # Nothing to do if server was not started
    if server is None:
        return

    my_input_list = [server]
    my_input_ready, my_output_ready, my_except_ready = select.select(my_input_list, [], [], timeout)

    if len(my_input_ready) == 1:
        service_request(server, wfs)

    return len(my_input_ready)

