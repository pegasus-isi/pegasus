"""
Class for managing notifications in pegasus-monitord.
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

import logging
import math
import os
import shlex
import signal
import subprocess
import sys
import tempfile
import time

from Pegasus.tools import utils

NOTIFICATION_FILE = "monitord-notifications.log"  # filename for writing the output of notification scripts
WAIT_CHILD_FINISH = 5  # in seconds

logger = logging.getLogger(__name__)


class Notifications:
    """
    This object contains all functions needed for managing
    notifications and starting notification scripts.
    """

    def __init__(
        self,
        notification_file_prefix,
        max_parallel_notifications=10,
        notifications_timeout=0,
    ):
        """
        This function initialized the Notifications class.
        """
        self._active_notifications = []
        self._pending_notifications = []
        self._max_parallel_notifications = max_parallel_notifications
        self._notifications_timeout = notifications_timeout
        self._notifications_fn = os.path.join(
            notification_file_prefix, NOTIFICATION_FILE
        )
        self._notifications_log = None
        self._notifications = {}

        # Open notifications' log file
        try:
            self._notifications_log = open(self._notifications_fn, "a")
        except OSError:
            logger.critical("cannot create notifications' log file... exiting...")
            sys.exit(1)

    def has_pending_notifications(self):
        """
        This function returns True if we have pending notifications.
        """
        return len(self._pending_notifications) > 0

    def has_active_notifications(self):
        """
        This function returns True if we have active notifications.
        """
        return len(self._active_notifications) > 0

    def terminate_notification(self, my_entry):
        """
        This function terminates a notification process, and cleans up its
        output/error files.
        """
        my_p = my_entry["subp"]
        my_pid = my_entry["pid"]
        my_notification = my_entry["notification"]
        my_out_fn = my_entry["out_fn"]
        my_err_fn = my_entry["err_fn"]
        my_entry["out_fd"]
        my_entry["err_fd"]
        my_action = my_entry["action"]
        my_p.poll()
        # If process hasn't finished...
        if my_p.returncode is None:
            # Send SIGTERM first...
            try:
                os.kill(my_pid, signal.SIGTERM)
            except OSError:
                logger.info("error sending SIGTERM to notification script...")
            # Wait for child to finish
            logger.warning(
                "waiting for notification process to finish: %s - %s"
                % (my_notification, my_action)
            )
            time.sleep(WAIT_CHILD_FINISH)
            my_p.poll()
            if my_p.returncode is None:
                # Send SIGKILL now...
                logger.warning(
                    "killing notification process to finish: %s - %s"
                    % (my_notification, my_action)
                )
                try:
                    os.kill(my_pid, signal.SIGKILL)
                except OSError:
                    logger.info("error sending SIGKILL to notification script...")

        # Finally, clean up files...
        try:
            os.unlink(my_out_fn)
            os.unlink(my_err_fn)
        except OSError:
            # No error here...
            pass
        logger.warning(
            "notification terminated: {} - {}".format(my_notification, my_action)
        )

    def service_notifications(self):
        """
        This function services notifications. It chekcs the notifications
        in the active list to see if they have finished. If so, it copies
        the stdout/stderr from these notifications to the
        monitord-notifications.log file. For notifications in the
        pending_notifications list, it starts the notification scripts,
        unless there are already too many notifications running in the
        system.
        """

        # Step 1: Look at existing notifications
        if len(self._active_notifications) > 0:
            logger.info("active notifications: %d" % len(self._active_notifications))
            # We have active notifications, let's check on their statuses
            my_notif_index = 0
            while my_notif_index < len(self._active_notifications):
                my_active_notif = self._active_notifications[my_notif_index]
                # Get subprocess object
                my_active_p = my_active_notif["subp"]
                my_status = my_active_p.poll()
                if my_status is not None:
                    # Process finished notification
                    my_finished_out_fn = my_active_notif["out_fn"]
                    my_finished_err_fn = my_active_notif["err_fn"]
                    my_finished_out_fd = my_active_notif["out_fd"]
                    my_finished_err_fd = my_active_notif["err_fd"]
                    my_finished_notification = my_active_notif["notification"]
                    my_finished_action = my_active_notif["action"]
                    my_finished_notification_params = my_active_notif["params"]
                    # Close out/err files, if not already closed...
                    try:
                        my_finished_out_fd.close()
                    except OSError:
                        logger.warning(
                            "error closing stdout file for notification %s... continuing..."
                            % (my_finished_notification)
                        )
                    try:
                        my_finished_err_fd.close()
                    except OSError:
                        logger.warning(
                            "error closing stderr file for notification %s... continuing..."
                            % (my_finished_notification)
                        )

                    if self._notifications_log is not None:
                        if logger.isEnabledFor(logging.INFO):
                            self._notifications_log.write("%s\n" % ("-" * 80))
                            self._notifications_log.write(
                                "Notification time  : %s\n" % (utils.isodate())
                            )
                            self._notifications_log.write(
                                "Notification event : %s\n" % (my_finished_notification)
                            )
                            self._notifications_log.write(
                                "Notification action: %s\n" % (my_finished_action)
                            )
                            self._notifications_log.write(
                                "Notification status: %s\n" % (my_status)
                            )
                            self._notifications_log.write("\n")
                            self._notifications_log.write("Notification environment\n")
                            for k in my_finished_notification_params:
                                self._notifications_log.write(
                                    "%s : %s\n"
                                    % (k, my_finished_notification_params[k])
                                )
                            self._notifications_log.write("\n")
                            self._notifications_log.write("stdout:\n")
                            try:
                                my_f = open(my_finished_out_fn)
                                for line in my_f:
                                    self._notifications_log.write(line)
                            except OSError:
                                logger.warning(
                                    "error processing notification stdout file: %s. continuing..."
                                    % (my_finished_out_fn)
                                )
                            else:
                                my_f.close()
                            self._notifications_log.write("\n")
                            self._notifications_log.write("stderr:\n")
                            try:
                                my_f = open(my_finished_err_fn)
                                for line in my_f:
                                    self._notifications_log.write(line)
                            except OSError:
                                logger.warning(
                                    "error processing notification stderr file: %s. continuing..."
                                    % (my_finished_err_fn)
                                )
                            else:
                                my_f.close()
                            self._notifications_log.write("\n")
                            self._notifications_log.write("\n")
                        else:
                            # Only log a one-liner so we can debug things later if we need to
                            self._notifications_log.write(
                                "%s - %s - %s - %s\n"
                                % (
                                    utils.isodate(),
                                    my_finished_notification,
                                    my_finished_action,
                                    my_status,
                                )
                            )
                    else:
                        logger.critical(
                            "notifications' output log file not initialized... exiting..."
                        )
                        sys.exit(1)

                    # Now, delete output and error files
                    try:
                        os.unlink(my_finished_out_fn)
                    except OSError:
                        logger.warning(
                            "error deleting notification stdout file: %s. continuing..."
                            % (my_finished_out_fn)
                        )
                    try:
                        os.unlink(my_finished_err_fn)
                    except OSError:
                        logger.warning(
                            "error deleting notification stderr file: %s. continuing..."
                            % (my_finished_err_fn)
                        )

                    # Delete this notification from our list
                    self._active_notifications.pop(my_notif_index)
                else:
                    # Process still going... leave it...
                    my_notif_index = my_notif_index + 1

        # Step 2: Look at our notification queue
        while len(self._pending_notifications) > 0:
            # Ok we have notifications to service...
            # print "pending notifications: %s" % (len(self._pending_notifications))
            logger.info(
                "pending notifications: %s" % (len(self._pending_notifications))
            )

            # Check if we have reached the maximum number of concurrent notifications
            if len(self._active_notifications) > self._max_parallel_notifications:
                # print "reaching maximum number of concurrent notifications... waiting until next cycle..."
                logger.info(
                    "reaching maximum number of concurrent notifications... waiting until next cycle..."
                )
                break

            # Get first notification from the list
            try:
                my_action, my_env = self._pending_notifications.pop(0)
            except IndexError:
                logger.error("error processing notification list... exiting!")
                sys.exit(1)

            # Merge default environment with notification-specific environment
            my_complete_env = os.environ.copy()
            my_complete_env.update(my_env)
            try:
                my_notification = "{} - {}".format(
                    my_env["PEGASUS_JOBID"], my_env["PEGASUS_EVENT"],
                )
            except KeyError:
                logger.warning(
                    "notification missing PEGASUS_JOBID or PEGASUS_EVENT... skipping..."
                )
                continue

            # Split arguments
            my_args = shlex.split(my_action)

            # Create output and error files for the notification script to use
            try:
                my_temp_out = tempfile.mkstemp(
                    prefix="notification-", suffix="-out.log", dir="/tmp"
                )
                my_temp_err = tempfile.mkstemp(
                    prefix="notification-", suffix="-err.log", dir="/tmp"
                )
                os.close(my_temp_out[0])
                os.close(my_temp_err[0])
                my_out_fn = my_temp_out[1]
                my_err_fn = my_temp_err[1]
            except OSError:
                logger.warning(
                    "cannot create temp files for notification: %s... skipping..."
                    % (my_notification)
                )
                continue

            # Open output and error files for the notification script
            try:
                my_f_out = open(my_out_fn, "w")
                my_f_err = open(my_err_fn, "w")
            except OSError:
                logger.warning(
                    "cannot open temp files for notification: %s... skipping..."
                    % (my_notification)
                )
                try:
                    os.unlink(my_out_fn)
                    os.unlink(my_err_fn)
                except OSError:
                    # No error here...
                    pass
                continue

            # Ok, here we go...
            try:
                my_p = subprocess.Popen(
                    my_args, stdout=my_f_out, stderr=my_f_err, env=my_complete_env
                )
            except OSError:
                logger.warning(
                    "cannot start notification executable: %s... skipping..."
                    % (my_notification)
                )
                try:
                    my_f_out.close()
                    my_f_err.close()
                    os.unlink(my_out_fn)
                    os.unlink(my_err_fn)
                except OSError:
                    logger.warning(
                        "found problem cleaning up notification: %s... skipping..."
                        % (my_notification)
                    )
                    continue
                # Clean up ok, just continue
                continue
            except Exception:
                logger.warning(
                    "problem starting notification: %s... skipping..."
                    % (my_notification)
                )
                try:
                    my_f_out.close()
                    my_f_err.close()
                    os.unlink(my_out_fn)
                    os.unlink(my_err_fn)
                except OSError:
                    logger.warning(
                        "found problem cleaning up notification: %s... skipping..."
                        % (my_notification)
                    )
                    continue
                # Clean up ok, just continue
                continue

            # Let's keep everything we need for the future
            my_started_notification = {}
            my_started_notification["pid"] = my_p.pid
            my_started_notification["subp"] = my_p
            my_started_notification["env"] = my_complete_env
            my_started_notification["params"] = my_env
            my_started_notification["args"] = my_args
            my_started_notification["action"] = my_action
            my_started_notification["out_fd"] = my_f_out
            my_started_notification["err_fd"] = my_f_err
            my_started_notification["out_fn"] = my_out_fn
            my_started_notification["err_fn"] = my_err_fn
            my_started_notification["notification"] = my_notification
            my_started_notification["time"] = time.time()

            # Add to the active list, and done!
            self._active_notifications.append(my_started_notification)
            logger.info("started notification for: %s" % (my_notification))

        # Step 3: Check if any notifications ran over the allowed time
        if self._notifications_timeout > 0:
            # Only go through the list if a timeout was specified

            # Get current time
            now = int(math.floor(time.time()))

            # Go through our list
            my_index = 0
            while my_index < len(self._active_notifications):
                my_entry = self._active_notifications[my_index]
                my_exp_time = my_entry["time"] + self._notifications_timeout

                # Check if notification has expired
                if my_exp_time < now:
                    # Notification has expired... kill it...
                    logger.warning("notification expired... terminating it...")
                    self.terminate_notification(my_entry)
                    # Delete this notification from our list
                    self._active_notifications.pop(my_index)
                else:
                    # Notification hasn't expired yet, move to next one...
                    my_index = my_index + 1

    def finish_notifications(self):
        """
        This function flushes all notifications, and closes the
        notifications' log file. It also logs all pending (but not yet
        issued) notifications.
        """
        # Take care of active notifications
        if len(self._active_notifications) > 0:
            for my_entry in self._active_notifications:
                self.terminate_notification(my_entry)

        # Take care of pending notifications
        if len(self._pending_notifications) > 0:
            for my_action, my_env in self._pending_notifications:
                try:
                    my_notification = "{} - {}".format(
                        my_env["PEGASUS_JOBID"], my_env["PEGASUS_EVENT"],
                    )
                except KeyError:
                    logger.warning(
                        "notification missing PEGASUS_JOBID or PEGASUS_EVENT... skipping..."
                    )
                    continue
                logger.warning(
                    "pending notification skipped: %s - %s"
                    % (my_notification, my_action)
                )

        # Close notifications' log file
        if self._notifications_log is not None:
            try:
                self._notifications_log.close()
            except OSError:
                logger.warning("error closing notifications' log file...")
            self._notifications_log = None

    def read_notification_file(self, notify_file, wf_uuid):
        """
        This function reads the notification file, parsing all
        notifications and creating our list of events to track.
        It returns the number of notifications read from the
        notifications' file.
        """
        if notify_file is None:
            return 0

        logger.info("loading notifications from %s" % (notify_file))

        # Open file
        try:
            NOTIFY = open(notify_file)
        except OSError:
            logger.warning(
                "cannot load notification file %s, continuing without notifications"
                % (notify_file)
            )
            return 0

        # Start with empty dictionaries for the three types of notifications
        my_notifications_read = 0
        my_notifications = {"workflow": {}, "job": {}, "invocation": {}}
        # For workflow and job notifications, we have a dict(workflow_id|job_id, dict(cond, [actions]))
        # For invocation notifications, we have a dict(job_id, dict(inv_id, dict(cond, [actions])))

        # Process notifications
        for line in NOTIFY:
            line = line.strip()
            # Skip blank lines
            if len(line) == 0:
                continue
            # Skip comments
            if line.startswith("#"):
                continue

            # Check if we split it in 4 or 5 pieces
            if line.lower().startswith("invocation"):
                # This is an invocation notification, split and get all pieces
                my_entry = line.split(None, 4)
                if len(my_entry) != 5:
                    logger.warning(
                        "cannot parse notification: %s, skipping..." % (line)
                    )
                    continue
                my_type = my_entry[0].lower()
                my_id = my_entry[1]
                try:
                    my_inv = int(my_entry[2])
                except ValueError:
                    logger.warning(
                        "cannot parse notification: %s, skipping..." % (line)
                    )
                    continue
                my_condition = my_entry[3]
                my_action = my_entry[4]
            else:
                # This is a workflow/job notification, split and get all pieces
                my_entry = line.split(None, 3)
                if len(my_entry) != 4:
                    logger.warning(
                        "cannot parse notification: %s, skipping..." % (line)
                    )
                    continue
                my_type = my_entry[0].lower()
                my_id = my_entry[1]
                my_condition = my_entry[2]
                my_action = my_entry[3]

            # Pick the right dictionary, depending on event type
            if my_type == "workflow":
                my_dict = my_notifications["workflow"]
                if my_id != wf_uuid:
                    logger.warning(
                        "workflow notification has id %s, our id is %s, skipping..."
                        % (my_id, wf_uuid)
                    )
                    continue

            elif my_type == "job" or my_type == "daxjob" or my_type == "dagjob":
                my_dict = my_notifications["job"]
            elif my_type == "invocation":
                my_dict = my_notifications["invocation"]
            else:
                logger.warning("unknown notification type: %s, skipping..." % (line))
                continue

            logger.debug("loading notification: %s" % (line))
            my_notifications_read = my_notifications_read + 1

            # Make sure id is in dictionary
            if not my_id in my_dict:
                my_dict[my_id] = {}

            # For invocations, one extra level...
            if my_type == "invocation":
                my_dict = my_dict[my_id]
                if not my_inv in my_dict:
                    my_dict[my_inv] = {}
                # Now add the notification condition, action pair
                if not my_condition in my_dict[my_inv]:
                    # No actions, start with the list
                    my_dict[my_inv][my_condition] = [my_action]
                else:
                    # We already have an action(s), let's add the new one to the list
                    my_dict[my_inv][my_condition].append(my_action)
            else:
                # Now add the notification condition, action pair
                if not my_condition in my_dict[my_id]:
                    my_dict[my_id][my_condition] = [my_action]
                else:
                    my_dict[my_id][my_condition].append(my_action)

        # Save our notifications for later use...
        if wf_uuid in self._notifications:
            logger.debug("reloaded notifications for workflow %s" % (wf_uuid))
        self._notifications[wf_uuid] = my_notifications

        # Close file
        try:
            NOTIFY.close()
        except OSError:
            pass

        # Return number of notifications read
        logger.debug(
            "loaded %d notifications for workflow %s" % (my_notifications_read, wf_uuid)
        )
        return my_notifications_read

    def process_workflow_notifications(self, wf, state):
        """
        This function takes care of processing workflow-level notifications.
        """
        # Check if we have notifications for this workflow
        if not wf._wf_uuid in self._notifications:
            return

        # Get the notifications' dictionary for this workflow id
        wf_notifications = self._notifications[wf._wf_uuid]

        if "workflow" in wf_notifications:
            my_dict = wf_notifications["workflow"]
            if len(my_dict) == 0:
                # No workflow notifications
                return
        else:
            logger.warning("notification structure missing workflow entry...")
            return

        # Our workflow is must be in there...
        if wf._wf_uuid in my_dict:
            my_notifications = my_dict[wf._wf_uuid]
        else:
            logger.warning(
                "notification has mismatching workflow id: %s different from %s"
                % (wf._wf_uuid, str(my_dict))
            )
            return

        # Sanity check the state...
        if state != "start" and state != "end":
            logger.warning("unknown workflow state %s, continuing..." % (state))
            return

        # Now, match the workflow state to the conditions in the notifications...
        for k in my_notifications:
            # Look up the actions for this notification now
            my_actions = my_notifications[k]
            if state == "start":
                if k != "start" and k != "all":
                    continue
                # Change k == 'all' to 'start'
                k = "start"
            if state == "end":
                if k == "on_error":
                    if wf._dagman_exit_code == 0:
                        continue
                elif k == "on_success":
                    if wf._dagman_exit_code != 0:
                        continue
                elif k != "at_end" and k != "all":
                    continue
                if k == "all":
                    k = "at_end"

            # Ok, we have a match!
            for action in my_actions:
                # Create dictionary with needed environment variables
                my_env = {}
                my_env["PEGASUS_EVENT"] = k
                my_env["PEGASUS_EVENT_TIMESTAMP"] = str(wf._current_timestamp)
                my_env["PEGASUS_EVENT_TIMESTAMP_ISO"] = utils.isodate(
                    wf._current_timestamp
                )
                my_env["PEGASUS_SUBMIT_DIR"] = wf._original_submit_dir
                my_env["PEGASUS_STDOUT"] = wf._out_file
                my_env["PEGASUS_JOBID"] = wf._wf_uuid
                my_env["PEGASUS_WFID"] = (
                    (wf._dax_label or "unknown") + "-" + (wf._dax_index or "unknown")
                )
                if state == "end":
                    # Workflow status is already in plain format, no need for conversion
                    my_env["PEGASUS_STATUS"] = str(wf._dagman_exit_code)

                # Done, queue the notification
                self._pending_notifications.append((action, my_env))
                # print "WORKFLOW NOTIFICATION ---> ", action, my_env

    def process_job_notifications(self, wf, state, job, status):
        """
        This function takes care of processing job-level notifications.
        """
        # Check if we have notifications for this workflow
        if not wf._wf_uuid in self._notifications:
            return

        # Get the notifications' dictionary for this workflow id
        wf_notifications = self._notifications[wf._wf_uuid]

        if "job" in wf_notifications:
            my_dict = wf_notifications["job"]
        else:
            logger.warning("notification structure missing job entry...")
            return

        # Check if we have notifications for this job
        if not job._exec_job_id in my_dict:
            return

        my_notifications = my_dict[job._exec_job_id]
        if job._exec_job_id in wf._job_info:
            if wf._job_info[job._exec_job_id][3] is None:
                job_has_post_script = False
            else:
                job_has_post_script = True
        else:
            logger.warning(
                "cannot find job %s in job_info database... skipping notification..."
                % (job._exec_job_id)
            )
            return

        # Now, match the job state to the conditions in the notifications...
        for k in my_notifications:
            # Look up the actions for this notification now
            my_actions = my_notifications[k]
            if state == "EXECUTE":
                if k != "start" and k != "all":
                    continue
                # Change k to "start"
                k = "start"
                my_status = None
            elif state == "JOB_SUCCESS":
                if job_has_post_script:
                    # Wait till postscript...
                    continue
                if k == "start" or k == "on_error":
                    continue
                if k == "all":
                    k = "at_end"
                my_status = "0"
            elif state == "POST_SCRIPT_SUCCESS":
                if k == "start" or k == "on_error":
                    continue
                if k == "all":
                    k = "at_end"
                my_status = "0"
            elif state == "JOB_FAILURE":
                if job_has_post_script:
                    # Wait till postscript...
                    continue
                if k == "start" or k == "on_success":
                    continue
                if k == "all":
                    k = "at_end"
                my_status = status
            elif state == "POST_SCRIPT_FAILURE":
                if k == "start" or k == "on_success":
                    continue
                if k == "all":
                    k = "at_end"
                my_status = status
            else:
                # We are in some other state...
                continue

            my_output = os.path.join(wf._original_submit_dir, job._output_file)
            my_error = os.path.join(wf._original_submit_dir, job._error_file)
            # Use the rotated file names if at the end of the job
            if k != "start":
                my_output = my_output + ".%03d" % (job._job_output_counter)
                my_error = my_error + ".%03d" % (job._job_output_counter)

            # Ok, we have a match!
            for action in my_actions:
                # Create dictionary with needed environment variables
                my_env = {}
                my_env["PEGASUS_EVENT"] = k
                my_env["PEGASUS_EVENT_TIMESTAMP"] = str(wf._current_timestamp)
                my_env["PEGASUS_EVENT_TIMESTAMP_ISO"] = utils.isodate(
                    wf._current_timestamp
                )
                my_env["PEGASUS_SUBMIT_DIR"] = wf._original_submit_dir
                my_env["PEGASUS_JOBID"] = job._exec_job_id
                my_env["PEGASUS_WFID"] = (
                    (wf._dax_label or "unknown") + "-" + (wf._dax_index or "unknown")
                )
                my_env["PEGASUS_STDOUT"] = my_output
                my_env["PEGASUS_STDERR"] = my_error
                if my_status is not None:
                    my_env["PEGASUS_STATUS"] = str(my_status)

                # Done, queue the notification
                self._pending_notifications.append((action, my_env))
                # print "JOB NOTIFICATION ---> ", action, my_env

    def process_invocation_notifications(self, wf, job, task_id, record=None):
        """
        This function takes care of processing invocation-level notifications.
        """
        if record is None:
            record = {}

        # Check if we have notifications for this workflow
        if not wf._wf_uuid in self._notifications:
            return

        # Get the notifications' dictionary for this workflow id
        wf_notifications = self._notifications[wf._wf_uuid]

        if "invocation" in wf_notifications:
            my_dict = wf_notifications["invocation"]
        else:
            logger.warning("notification structure missing invocation entry...")
            return

        # Check if we have notifications for this job
        if not job._exec_job_id in my_dict:
            return

        # Advance to the task dictionary
        my_dict = my_dict[job._exec_job_id]

        # Check if we have notifications for this invocation
        if not task_id in my_dict:
            return

        my_notifications = my_dict[task_id]

        # Now, match the invocation state to the condition in the notification
        for k in my_notifications:
            # Look up the actions for this notification now
            my_actions = my_notifications[k]
            if "raw" in record:
                my_status = record["raw"]
            else:
                my_status = job._main_job_exitcode
            # Convert exitcode to int
            try:
                my_status = int(my_status)
            except ValueError:
                pass
            # Now, compare to the notification condition(s)
            if my_status == 0:
                if k == "on_error":
                    continue
            if my_status != 0:
                if k == "on_success":
                    continue
            if k == "all":
                k = "at_end"

            # Here, we always use the rotated file names as the invocation has already finished...
            my_output = os.path.join(
                wf._original_submit_dir, job._output_file
            ) + ".%03d" % (job._job_output_counter)
            my_error = os.path.join(
                wf._original_submit_dir, job._error_file
            ) + ".%03d" % (job._job_output_counter)

            # Ok, we have a match!
            for action in my_actions:
                # Create dictionary with needed environment variables
                my_env = {}
                my_env["PEGASUS_EVENT"] = k
                my_env["PEGASUS_EVENT_TIMESTAMP"] = str(wf._current_timestamp)
                my_env["PEGASUS_EVENT_TIMESTAMP_ISO"] = utils.isodate(
                    wf._current_timestamp
                )
                my_env["PEGASUS_SUBMIT_DIR"] = wf._original_submit_dir
                my_env["PEGASUS_JOBID"] = job._exec_job_id
                my_env["PEGASUS_INVID"] = str(task_id)
                my_env["PEGASUS_WFID"] = (
                    (wf._dax_label or "unknown") + "-" + (wf._dax_index or "unknown")
                )
                my_env["PEGASUS_STDOUT"] = my_output
                my_env["PEGASUS_STDERR"] = my_error
                if k != "start":
                    # Convert raw exitcode into human-parseable format
                    my_env["PEGASUS_STATUS"] = str(utils.raw_to_regular(my_status))

                # Done, queue the notification
                self._pending_notifications.append((action, my_env))
                # print "INVOCATION NOTIFICATION ---> ", action, my_env

    def remove_notifications(self, wf_uuid):
        """
        This function removes the notifications for workflow wf_uuid
        from our _notifications dictionary.
        """

        # Check if we have notifications for this workflow
        if not wf_uuid in self._notifications:
            return

        logger.debug("deleting notifications for workflow %s..." % (wf_uuid))

        # Delete them from our dictionary
        del self._notifications[wf_uuid]
