#!/usr/bin/env python3

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

import json
import logging
import optparse
import os
import re
import signal
import sys
from datetime import datetime, timedelta

import globus_sdk

# --- global variables ----------------------------------------------------------------

prog_dir = os.path.realpath(os.path.join(os.path.dirname(sys.argv[0])))
prog_base = os.path.split(sys.argv[0])[1]  # Name of this program

logger = logging.getLogger("Pegasus")

client = None
transfer_client = None
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
    if transfer_client is not None:
        cancel_task(transfer_client, task_id)
    sys.exit(1)


def acquire_clients(request):
    # connect to the service
    client = globus_sdk.NativeAppAuthClient(request["client_id"])

    if request["transfer_rt"] is None:
        authorizer = globus_sdk.AccessTokenAuthorizer(request["transfer_at"])
    else:
        authorizer = globus_sdk.RefreshTokenAuthorizer(
            request["transfer_rt"],
            client,
            access_token=request["transfer_at"],
            expires_at=int(request["transfer_at_exp"]),
        )

    transfer_client = globus_sdk.TransferClient(
        authorizer=authorizer, transport_params={"max_retries": 0}
    )
    return client, transfer_client


def activate_endpoint(transfer_client, endpoint):
    """
    Try to Auto-Activate a Globus Online endpoint - only auto-activate is allowed for now
    """
    logger.info("Activating " + endpoint)
    try:
        res = transfer_client.endpoint_autoactivate(endpoint, if_expires_in=3600)
        if res["code"] == "AutoActivationFailed":
            logger.critical('Endpoint "%s" requires manual activation' % endpoint)
            logger.critical(
                "Please log into Globus Online and activate the endpoint there"
            )
            sys.exit(1)
    except globus_sdk.TransferAPIError as e:
        logger.critical('Endpoint "%s" auto-activation ERROR' % endpoint)
        logger.critical(str(e))
        sys.exit(1)


def wait_for_task(transfer_client, task_id, acceptable_faults=None):
    """
    Wait for a task to complete
    """
    logger.info("Waiting for transfer to complete")
    wait_timeout = 60
    loop_counter = 0

    #### SHOULD CHECK FOR ERRORS ####
    while not transfer_client.task_wait(task_id, timeout=wait_timeout):
        loop_counter += 1
        logger.info(
            "Globus transfer task %s is still running (%d seconds)"
            % (task_id, loop_counter * wait_timeout)
        )
        task_errors = transfer_client.task_event_list(
            task_id=task_id, limit=20, query_params={"filter": "is_error:1"}
        )
        for error in task_errors:
            details = re.sub(r"\n|\r", " ", error["description"])
            if re.search(r"System error in mkdir.*File exists", details):
                logger.info("Ignoring mkdir error: " + details)
            else:
                raise RuntimeError(
                    "Error on globus transfer task %s at %s: %s"
                    % (task_id, error["time"], details)
                )

    return None


def cancel_task(transfer_client, task_id):
    """
    Cancel a task - useful when a transfer has errors or we catch a signal
    """
    logger.info("Canceling transfer")
    try:
        transfer_client.cancel_task(task_id)
        logger.info("Globus transfer task %s has been canceled" % task_id)
    except Exception:
        pass


def mkdir(request):
    """
    operation_mkdir doesn't support recursive creation of directories
    """
    # global so that we can use it in signal handlers
    global client
    global transfer_client
    global task_id

    # connect to the service
    client, transfer_client = acquire_clients(request)

    # make sure we can auto-activate the endpoints
    activate_endpoint(transfer_client, request["endpoint"])

    for f in request["files"]:
        base_path = ""
        child_dirs = []

        # find base_path with operation_ls
        base_path = f
        found = False
        while (not found) and base_path != "/":
            found = True
            base_path, dir_name = os.path.split(base_path)
            if dir_name not in ["", "/"]:
                child_dirs.append(dir_name)
            try:
                response = transfer_client.operation_ls(
                    request["endpoint"], path=base_path, query_params={"limit": 2}
                )
            except globus_sdk.TransferAPIError as e:
                logger.warn("Finding existing parent dir for mkdir " + f)
                logger.warn(str(e))
                found = False

        child_dirs.reverse()
        path = base_path
        for child in child_dirs:
            if path[-1] == "/":
                path += child
            else:
                path = path + "/" + child

            try:
                transfer_client.operation_mkdir(request["endpoint"], path)
            except globus_sdk.TransferAPIError as e:
                if e.code == "ExternalError.MkdirFailed.Exists":
                    logger.warn("Directory already exists: " + path)
                else:
                    raise RuntimeError(str(e))
    logger.info("Mkdir complete")


def transfer(request):
    """
    takes a transfer specification parsed from json:
    {
      "client_id":       "globus oauth client id",
      "src_endpoint":    "rynge#obelix",
      "dst_endpoint":    "rynge#workflow",
      "transfer_at":     "secret_auth_token",
      "transfer_rt":     "secret_refresh_auth_token",
      "transfer_at_exp": "secret_auth_token_expiration",
      "recursive":       [True | False],
      "files":[
         {"src":"/etc/hosts","dst":"/tmp/foobar.txt"},
         {"src":"/etc/hosts","dst":"/tmp/foobar-2.txt"}
        ],
    }
    """

    # global so that we can use it in signal handlers
    global client
    global transfer_client
    global task_id

    # connect to the service
    client, transfer_client = acquire_clients(request)

    # make sure we can auto-activate the endpoints
    activate_endpoint(transfer_client, request["src_endpoint"])
    activate_endpoint(transfer_client, request["dst_endpoint"])

    label = None
    if "PEGASUS_WF_UUID" in os.environ and "PEGASUS_DAG_JOB_ID" in os.environ:
        label = os.environ["PEGASUS_WF_UUID"] + " - " + os.environ["PEGASUS_DAG_JOB_ID"]

    # set up a new data transfer
    deadline = datetime.utcnow() + timedelta(hours=24)
    transfer_data = globus_sdk.TransferData(
        transfer_client,
        source_endpoint=request["src_endpoint"],
        destination_endpoint=request["dst_endpoint"],
        label=label,
        deadline=deadline,
        notify_on_succeeded=False,
        notify_on_failed=False,
        notify_on_inactive=False,
    )

    for pair in request["files"]:
        transfer_data.add_item(pair["src"], pair["dst"])

    # finalize and submit the transfer
    transfer_result = transfer_client.submit_transfer(transfer_data)
    task_id = transfer_result["task_id"]

    # how many faults will we accept before giving up?
    # acceptable_faults = min(100, len(request["files"]) * 3)

    # wait for the task to complete, and see the tasks and
    # endpoint ls change
    try:
        wait_for_task(transfer_client, task_id)
    except Exception as err:
        logger.error(err)
        cancel_task(transfer_client, task_id)
        sys.exit(1)
    logger.info("Transfer complete")


def remove(request):
    """
    removes files on a remote Globus Online endpoint - API is not complete, so transfer
    0 byte files instead of actually deleting anything
    """

    # global so that we can use it in signal handlers
    global client
    global transfer_client
    global task_id

    # connect to the service
    client, transfer_client = acquire_clients(request)

    activate_endpoint(transfer_client, request["endpoint"])

    label = None
    if "PEGASUS_WF_UUID" in os.environ and "PEGASUS_DAG_JOB_ID" in os.environ:
        label = os.environ["PEGASUS_WF_UUID"] + " - " + os.environ["PEGASUS_DAG_JOB_ID"]

    # set up a new delete transfer
    deadline = datetime.utcnow() + timedelta(hours=24)
    del_data = globus_sdk.DeleteData(
        transfer_client,
        endpoint=request["endpoint"],
        label=label,
        recursive=request["recursive"],
        deadline=deadline,
        notify_on_succeeded=False,
        notify_on_failed=False,
        notify_on_inactive=False,
    )

    for f in request["files"]:
        del_data.add_item(f)

    # finalize and submit the transfer
    delete_result = transfer_client.submit_delete(del_data)
    task_id = delete_result["task_id"]

    # how many faults will we accept before giving up?
    # acceptable_faults = min(100, len(request["files"]) * 3)

    # wait for the task to complete, and see the tasks and
    # endpoint ls change
    try:
        wait_for_task(transfer_client, task_id)
    except Exception as err:
        logger.error(err)
        cancel_task(transfer_client, task_id)
        sys.exit(1)
    logger.info("Delete complete")


def main():

    # Configure command line option parser
    prog_usage = "usage: %s [options]" % (prog_base)
    parser = optparse.OptionParser(usage=prog_usage)

    parser.add_option(
        "--mkdir", action="store_true", dest="mkdir", help="Select mkdir mode"
    )
    parser.add_option(
        "--transfer", action="store_true", dest="transfer", help="Select transfer mode"
    )
    parser.add_option(
        "--remove", action="store_true", dest="remove", help="Select remove mode"
    )
    parser.add_option(
        "--file",
        action="store",
        dest="file",
        help="File containing GO URL pairs to be transferred",
    )
    parser.add_option(
        "-d",
        "--debug",
        action="store_true",
        dest="debug",
        help="Enables debugging output",
    )

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

    logger.info("Globus SDK Version: %s" % globus_sdk.__version__)

    if options.mkdir:
        mkdir(data)
    elif options.transfer:
        transfer(data)
    elif options.remove:
        remove(data)
    else:
        logger.critical("Please specify one of: --mkdir, --transfer, --remove")
        sys.exit(1)


if __name__ == "__main__":
    main()
