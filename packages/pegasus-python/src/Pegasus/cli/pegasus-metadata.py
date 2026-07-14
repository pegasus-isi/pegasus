#!/usr/bin/env python3

"""
Pegasus Metadata is a tool to query metadata collected by Pegasus workflows.

Usage: pegasus-metadata [-h] [-v] [-c] {task,file,workflow} ... submit_dir
"""

##
#  Copyright 2007-2012 University Of Southern California
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

import argparse
import logging
import os
import sys
from pathlib import Path

# PEGASUS_PYTHONPATH is set by the pegasus-python-wrapper script
peg_path = os.environ.get("PEGASUS_PYTHONPATH")
if peg_path:
    for p in reversed(peg_path.split(":")):
        if p not in sys.path:
            sys.path.insert(0, p)

from Pegasus.db import connection
from Pegasus.db.connection import ConnectionError, DBType
from Pegasus.service.monitoring.queries import (
    StampedeDBNotFoundError,
    StampedeWorkflowQueries,
)
from Pegasus.tools import utils


def configure_logging(verbosity=0):
    verbosity = min(3, verbosity)

    log_levels = [logging.ERROR, logging.WARNING, logging.INFO, logging.DEBUG]

    utils.configureLogging(level=log_levels[verbosity])


def get_workflow_uuid(submit_dir):
    bdump_yml = Path(submit_dir) / "braindump.yml"

    if bdump_yml.exists() is False:
        raise ValueError(f"Not a valid workflow submit directory: {submit_dir!r}")

    braindump = utils.slurp_braindb(submit_dir)
    return braindump["root_wf_uuid"], braindump["wf_uuid"]


def get_workflow_uri(submit_dir):
    return connection.url_by_submitdir(submit_dir, DBType.WORKFLOW)


def render_metas(metas, indent=""):
    if not metas:
        print(f"{indent}No metadata found")
        return

    max_key_len = 0

    for meta in metas:
        max_key_len = max(max_key_len, len(meta.key))

    max_key_len += 1

    for meta in metas:
        print(f"{indent}{meta.key.ljust(max_key_len)}: {meta.value}")


def workflow_metadata(recursive=False, submit_dir=".", *args, **kwargs):
    logging.debug("workflow_metadata")

    try:
        root_wf_uuid, wf_uuid = get_workflow_uuid(submit_dir)
        logging.debug(f"Workflow UUID: {wf_uuid}")
        db_uri = get_workflow_uri(submit_dir)

        queries = StampedeWorkflowQueries(db_uri)

        if recursive:
            wfs = queries.get_workflows(
                root_wf_uuid, order="w.root_wf_id, w.parent_wf_id"
            )

            for wf in wfs.records:
                print(f"Workflow {wf.wf_uuid}")
                workflow_metas = queries.get_workflow_meta(wf_uuid).records
                render_metas(workflow_metas, "    ")

        else:
            workflow_metas = queries.get_workflow_meta(wf_uuid).records
            print(f"Workflow {wf_uuid}")
            render_metas(workflow_metas, "    ")

    except ValueError as e:
        logging.error(e)
        sys.exit(1)

    except ConnectionError as e:
        logging.error(e)
        sys.exit(2)

    except StampedeDBNotFoundError as e:
        logging.error(e)
        sys.exit(3)


def task_metadata(abs_task_id=None, submit_dir=".", *args, **kwargs):
    logging.debug("task_metadata")

    if not abs_task_id:
        logging.error("task_id is required")
        sys.exit(1)

    try:
        root_wf_uuid, wf_uuid = get_workflow_uuid(submit_dir)
        logging.debug(f"Workflow UUID: {wf_uuid}")
        db_uri = get_workflow_uri(submit_dir)

        queries = StampedeWorkflowQueries(db_uri)

        logging.debug(f"Get task metadata for abs_task_id {abs_task_id}")
        workflow = queries.get_workflow_tasks(
            wf_uuid, query=f"t.abs_task_id == {abs_task_id!r}"
        )

        if workflow.total_filtered == 0:
            raise ValueError(f"Invalid task_name {abs_task_id!r}")

        task_id = workflow.records[0].task_id
        task_metas = queries.get_task_meta(task_id).records

        print("Task", abs_task_id)
        render_metas(task_metas, "    ")

    except ValueError as e:
        logging.error(e)
        sys.exit(1)

    except ConnectionError as e:
        logging.error(e)
        sys.exit(2)

    except StampedeDBNotFoundError as e:
        logging.error(e)
        sys.exit(3)


def file_metadata(
    file_name=None, list_files=False, trace=False, submit_dir=".", *args, **kwargs
):
    logging.debug("task_metadata")

    if trace and not file_name:
        logging.error("file_name is required when trace is True")
        sys.exit(1)

    if not file_name:
        list_files = True
        logging.info("file_name not provided, will list metadata for all files")

    try:
        root_wf_uuid, wf_uuid = get_workflow_uuid(submit_dir)
        logging.debug(f"Workflow UUID: {wf_uuid}")
        db_uri = get_workflow_uri(submit_dir)

        queries = StampedeWorkflowQueries(db_uri)

        if list_files:
            logging.debug("Get file metadata for all files")
            workflow_files = queries.get_workflow_files(wf_uuid)

            if workflow_files.total_filtered == 0:
                print("No files found")

        else:
            logging.debug(f"Get file metadata for lfn {file_name!r}")

            workflow_files = queries.get_workflow_files(
                wf_uuid, query=f"l.lfn == {file_name!r}"
            )

            if workflow_files.total_filtered == 0:
                raise ValueError(f"Invalid file {file_name!r}")

        if trace:
            wf_file = workflow_files.records.values()[0]
            wf_id = wf_file.extras.wf_id
            task_id = wf_file.extras.task_id

            # Get workflow information
            root_wf = None
            wf = queries.get_workflow(wf_id)

            # If workflow is hierarchical workflow, get root workflow information
            if wf_id != wf.root_wf_id:
                root_wf = queries.get_workflow(wf.root_wf_id)

            if root_wf:
                root_wf_metas = queries.get_workflow_meta(root_wf.wf_id).records
                print(f"Root Workflow {root_wf.wf_uuid}")
                render_metas(root_wf_metas, "    ")
                print()

            wf_metas = queries.get_workflow_meta(wf_id).records
            print(f"Workflow {wf.wf_uuid}")
            render_metas(wf_metas, "    ")
            print()

            task = queries.get_task(task_id)
            task_metas = queries.get_task_meta(task_id).records
            print(f"Task {task.abs_task_id}")
            render_metas(task_metas, "    ")
            print()

        for wf_file in workflow_files.records:
            print(f"File {wf_file.lfn}")
            render_metas(wf_file.meta, "    ")

    except ValueError as e:
        logging.error(e)
        sys.exit(1)

    except ConnectionError as e:
        logging.error(e)
        sys.exit(2)

    except StampedeDBNotFoundError as e:
        logging.error(e)
        sys.exit(3)


def main():
    parent_parser = argparse.ArgumentParser(add_help=False)
    parent_parser.add_argument(
        "-v", "--verbose", default=0, action="count", help="Logging verbosity"
    )
    parent_parser.add_argument(
        "submit_dir", nargs="?", default=".", help="Workflow submit directory"
    )

    parser = argparse.ArgumentParser(description="Pegasus Metadata Query Tool")
    sub_parser = parser.add_subparsers(
        title="Metadata types", description="Types of metadata that can be queried"
    )

    # Workflow Metadata Options
    workflow = sub_parser.add_parser("workflow", parents=[parent_parser])
    workflow.add_argument("-r", "--recursive", default=False, action="store_true")
    workflow.set_defaults(func=workflow_metadata)

    # Task Metadata Options
    task = sub_parser.add_parser("task", parents=[parent_parser])
    task.add_argument("-i", "--task-id", dest="abs_task_id", required=True)
    task.set_defaults(func=task_metadata)

    # File Metadata Options
    file = sub_parser.add_parser("file", parents=[parent_parser])
    file.add_argument("-n", "--file-name")
    file.add_argument(
        "-l", "--list", default=False, action="store_true", dest="list_files"
    )
    file.add_argument("-t", "--trace", default=False, action="store_true")
    file.set_defaults(func=file_metadata)

    args = parser.parse_args(sys.argv[1:])

    configure_logging(args.verbose)
    args.func(**vars(args))


if __name__ == "__main__":
    main()
