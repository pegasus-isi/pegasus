#!/usr/bin/env python

"""
This is a tool used by pegasus-transfer to do transfers between
Globus Online endpoints
"""

#
#  Copyright 2016 University Of Southern California
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
#

import sys
import os
import logging
import math
import re
import time
import subprocess
import optparse
import json
import signal
from datetime import datetime, timedelta
from difflib import SequenceMatcher
from httplib import EXPECTATION_FAILED
from globusonline.transfer.api_client import api_result

# Use pegasus-config to find our lib path
bin_dir = os.path.normpath(os.path.join(os.path.dirname(sys.argv[0])))
pegasus_config = os.path.join(bin_dir, "pegasus-config") + " --noeoln --python"
lib_dir = subprocess.Popen(pegasus_config, stdout=subprocess.PIPE, shell=True).communicate()[0]
pegasus_config = os.path.join(bin_dir, "pegasus-config") + " --noeoln --python-externals"
lib_ext_dir = subprocess.Popen(pegasus_config, stdout=subprocess.PIPE, shell=True).communicate()[0]

# Insert this directory in our search path
os.sys.path.insert(0, lib_dir)
os.sys.path.insert(0, lib_ext_dir)

try:
    from globusonline.transfer.api_client import TransferAPIClient, Transfer, Delete
    from globusonline.transfer.api_client import x509_proxy
except ImportError, e:
    sys.stderr.write("ERROR: Unable to load GlobusOnline library: %s\n" % e)
    sys.stderr.write("Please see https://github.com/globusonline/transfer-api-client-python\n")
    exit(1)

# --- global variables ----------------------------------------------------------------

prog_dir  = os.path.realpath(os.path.join(os.path.dirname(sys.argv[0])))
prog_base = os.path.split(sys.argv[0])[1]   # Name of this program

logger = logging.getLogger("my_logger")

api = None
task_id = None

# --- functions ----------------------------------------------------------------
            
def setup_logger(debug_flag):
    
    # log to the console
    console = logging.StreamHandler()
    
    # default log level - make logger/console match
    logger.setLevel(logging.INFO)
    console.setLevel(logging.INFO)

    # debug - from command line
    if debug_flag:
        logger.setLevel(logging.DEBUG)
        console.setLevel(logging.DEBUG)

    # formatter
    formatter = logging.Formatter("%(asctime)s %(levelname)7s:  %(message)s")
    console.setFormatter(formatter)
    logger.addHandler(console)
    logger.debug("Logger has been configured")


def prog_sigint_handler(signum, frame):
    logger.warn("Exiting due to signal %d" % (signum))
    if api is not None:
        cancel_task(api, task_id)
    sys.exit(1)


def activate_ep(api, ep):
    """
    Activate a Globus Online endpoint - only auto-activate is allowed for now
    """
    logger.info("Activating " + ep)
    try:
        api.endpoint_autoactivate(ep)
    except Exception, err:
        logger.critical("Unable to activate endpoint: : %s" % (err))
        logger.critical("Please log into Globus Online and activate the endpoint there")
        sys.exit(1)


def wait_for_task(api, task_id, acceptable_faults = None):
    """
    Wait for a task to complete
    """
    logger.info("Waiting for transfer to complete")
    poll_interval = 60
    connection_error_count = 0
    old_details = None
    while True:
        
        # make sure we can recover if our connection gets dropped
        code, reason, data = api.task(task_id, fields="status,nice_status_details,files,faults")
        if code != 200:
            logger.error("Got code %s from the server" %(code))
            connection_error_count += 1
            if connection_error_count > 20:
                raise RuntimeError("Too many http errors from the Globus Online service")
            time.sleep(poll_interval)
            continue
        # reset the error counter
        connection_error_count = 0
            
        status = data["status"]
        details = data["nice_status_details"]
        files = int(data["files"])
        faults = int(data["faults"])
        logger.debug("%s : %s" %(status, data['nice_status_details']))
        
        # standard states
        if status in ["SUCCEEDED"]:
            return True
        if status in ["FAILED"]:
            raise RuntimeError("GlobusOnline reports the transfer failed: %s" %(data['nice_status_details']))
        if status in ["INACTIVE"]:
            raise RuntimeError("GlobusOnline has suspended your transfer: %s" %(data['nice_status_details']))
        
        # too many faults?
        if acceptable_faults is not None and faults > acceptable_faults:
            raise RuntimeError("Too many faults: %d" %(faults))
        
        # did we get a new nice status updates?
        if old_details is None and details is not None:
            logger.info(details)
        elif details is not None and SequenceMatcher(None, details, old_details).ratio() < .5:
            logger.debug("Ratio: %f" %(SequenceMatcher(None, details, old_details)))
            logger.info(details)
        old_details = details
                  
        time.sleep(poll_interval)
        
    return None


def cancel_task(api, task_id):
    """
    Cancel a task - useful when a transfer has too many faults or we catch a signal
    """
    logger.info("Canceling transfer")
    try:
        api.task_cancel(task_id)
    except:
        pass


def mkdir(request):
    """
    mkdir is not supported in the Python API yet, so translate it to a transfer
    """
    
    # global so that we can use it in signal handlers
    global api
    global task_id
    
    # connect to the service
    api = TransferAPIClient(request["globus_username"], cert_file = request["x509_proxy"])

    # make sure we can auto-activate the endpoints
    ep = activate_ep(api, request["endpoint"])
    
    label = None
    if "PEGASUS_WF_UUID" in os.environ and "PEGASUS_DAG_JOB_ID" in os.environ:
        label = os.environ["PEGASUS_WF_UUID"] + " - " + os.environ["PEGASUS_DAG_JOB_ID"] 

    # set up a new transfer
    code, message, data = api.transfer_submission_id()
    submission_id = data["value"]    
    deadline = datetime.utcnow() + timedelta(hours=24)
    t = Transfer(submission_id,
                 request["endpoint"],
                 request["endpoint"], 
                 deadline = deadline,
                 label = label,
                 notify_on_succeeded = False,
                 notify_on_failed = False,
                 notify_on_inactive = False)
    
    for f in request["files"]:
        t.add_item("/dev/null", f + "/.created")
    
    # finalize and submit the transfer
    code, reason, data = api.transfer(t)
    task_id = data["task_id"]
    
    # how many faults will we accept before giving up?
    acceptable_faults = min(100, len(request["files"]) * 3)

    # wait for the task to complete, and see the tasks and
    # endpoint ls change
    try:
        status = wait_for_task(api, task_id, acceptable_faults)
    except Exception, err:
        logger.error(err)
        cancel_task(api, task_id)
        sys.exit(1)
        logger.info("Mkdir complete")


def transfer(request):
    """
    takes a transfer specification parsed from json:
    {
      "globus_username": "rynge",
      "x509_proxy": "/tmp/...",
      "src_endpoint": "rynge#obelix",
      "dst_endpoint": "rynge#workflow",
      "files":[
         {"src":"/etc/hosts","dst":"/tmp/foobar.txt"},
         {"src":"/etc/hosts","dst":"/tmp/foobar-2.txt"}
        ],
    }
    """
    
    # global so that we can use it in signal handlers
    global api
    global task_id
    
    # connect to the service
    api = TransferAPIClient(request["globus_username"], cert_file = request["x509_proxy"])

    # make sure we can auto-activate the endpoints
    src_ep = activate_ep(api, request["src_endpoint"])
    dst_ep = activate_ep(api, request["dst_endpoint"])

    label = None
    if "PEGASUS_WF_UUID" in os.environ and "PEGASUS_DAG_JOB_ID" in os.environ:
        label = os.environ["PEGASUS_WF_UUID"] + " - " + os.environ["PEGASUS_DAG_JOB_ID"] 

    # set up a new transfer
    code, message, data = api.transfer_submission_id()
    submission_id = data["value"]    
    deadline = datetime.utcnow() + timedelta(hours=24)
    t = Transfer(submission_id,
                 request["src_endpoint"],
                 request["dst_endpoint"], 
                 deadline = deadline,
                 label = label,
                 notify_on_succeeded = False,
                 notify_on_failed = False,
                 notify_on_inactive = False)
    
    for pair in request["files"]:
        t.add_item(pair["src"], pair["dst"])
    
    # finalize and submit the transfer
    code, reason, data = api.transfer(t)
    task_id = data["task_id"]
    
    # how many faults will we accept before giving up?
    acceptable_faults = min(100, len(request["files"]) * 3)

    # wait for the task to complete, and see the tasks and
    # endpoint ls change
    try:
        status = wait_for_task(api, task_id, acceptable_faults)
    except Exception, err:
        logger.error(err)
        cancel_task(api, task_id)
        sys.exit(1)
    logger.info("Transfer complete")


def remove(request):
    """
    removes files on a remote Globus Online endpoint - API is not complete, so transfer
    0 byte files instead of actually deleting anything
    """
    
    # global so that we can use it in signal handlers
    global api
    global task_id
    
    # connect to the service
    api = TransferAPIClient(request["globus_username"], cert_file = request["x509_proxy"])

    # make sure we can auto-activate the endpoints
    ep = activate_ep(api, request["endpoint"])

    label = None
    if "PEGASUS_WF_UUID" in os.environ and "PEGASUS_DAG_JOB_ID" in os.environ:
        label = os.environ["PEGASUS_WF_UUID"] + " - " + os.environ["PEGASUS_DAG_JOB_ID"] 

    # set up a new transfer
    code, message, data = api.transfer_submission_id()
    submission_id = data["value"]    
    deadline = datetime.utcnow() + timedelta(hours=24)
    t = Transfer(submission_id,
                 request["endpoint"],
                 request["endpoint"], 
                 deadline = deadline,
                 label = label,
                 notify_on_succeeded = False,
                 notify_on_failed = False,
                 notify_on_inactive = False)
    
    for f in request["files"]:
        t.add_item("/dev/null", f)

    # finalize and submit the transfer
    code, reason, data = api.transfer(t)
    task_id = data["task_id"]
    
    # how many faults will we accept before giving up?
    acceptable_faults = min(100, len(request["files"]) * 3)

    # wait for the task to complete, and see the tasks and
    # endpoint ls change
    try:
        status = wait_for_task(api, task_id, acceptable_faults)
    except Exception, err:
        logger.error(err)
        cancel_task(api, task_id)
        sys.exit(1)
    logger.info("Delete complete")


def main():
    
    # Configure command line option parser
    prog_usage = "usage: %s [options]" % (prog_base)
    parser = optparse.OptionParser(usage=prog_usage)
    
    parser.add_option("--mkdir", action = "store_true", dest = "mkdir",
                      help = "Select mkdir mode")
    parser.add_option("--transfer", action = "store_true", dest = "transfer",
                      help = "Select transfer mode")
    parser.add_option("--remove", action = "store_true", dest = "remove",
                      help = "Select remove mode")
    parser.add_option("--file", action = "store", dest = "file",
                      help = "File containing GO URL pairs to be transferred")
    parser.add_option("-d", "--debug", action = "store_true", dest = "debug",
                      help = "Enables debugging output")
    
    # Parse command line options
    (options, args) = parser.parse_args()
    setup_logger(options.debug)

    if not options.file:
        logger.critical("An input file has to be given with --file")
        sys.exit(1)
    
    # get the json
    with open(options.file) as data_file:    
        data = json.load(data_file)

    # Die nicely when asked to (Ctrl+C, system shutdown)
    signal.signal(signal.SIGINT, prog_sigint_handler)
    signal.signal(signal.SIGTERM, prog_sigint_handler)

    if options.mkdir:
        mkdir(data)
    elif options.transfer:
        transfer(data)
    elif options.remove:
        remove(data)
    else:
        logger.critical("Please specify one of: --mkdir, --transfer, --remove")
        sys.exit(1)


if __name__ == '__main__':
    main()
