import pytest
import os
import logging
import shutil
from tempfile import TemporaryDirectory
from textwrap import dedent

import Pegasus
from Pegasus.client.status import Status


@pytest.fixture(scope="function")
def status():
    return Status()

def test_should_create_Status(status):
    assert status is not None


@pytest.mark.parametrize(
    "pegasus_wf_name_from_bd, submit_dir, expected_dict",
    [
        (
            'sample_1_success',
            './status_sample_files/sample1',
            {
            "totals": {
                "unready": 0,
                "ready": 0,
                "pre": 0,
                "queued": 0,
                "post": 0,
                "succeeded": 0,
                "failed": 0,
                "percent_done": 0.0,
                "total": 0,
            },
            "dags": {
                "root": {
                    "unready": 0,
                    "ready": 0,
                    "pre": 0,
                    "queued": 0,
                    "post": 0,
                    "succeeded": 17,
                    "failed": 0,
                    "percent_done": 100.0,
                    "state": "Success",
                    "total": 17,
                    "dagname": "*sample_1_success.dag",
                }
            }
        }
        ),
        (
            'sample_2_held',
            './status_sample_files/sample2',
            {
            "totals": {
                "unready": 0,
                "ready": 0,
                "pre": 0,
                "queued": 0,
                "post": 0,
                "succeeded": 0,
                "failed": 0,
                "percent_done": 0.0,
                "total": 0,
            },
            "dags": {
                "root": {
                    "unready": 28,
                    "ready": 0,
                    "pre": 0,
                    "queued": 1,
                    "post": 0,
                    "succeeded": 0,
                    "failed": 0,
                    "percent_done": 0.0,
                    "state": "Running",
                    "total": 29,
                    "dagname": "*sample_2_held.dag",
                }
            }
        }
        ),
        (
            'sample_3_failure',
            './status_sample_files/sample3',
            {
            "totals": {
                "unready": 0,
                "ready": 0,
                "pre": 0,
                "queued": 0,
                "post": 0,
                "succeeded": 0,
                "failed": 0,
                "percent_done": 0.0,
                "total": 0,
            },
            "dags": {
                "root": {
                    "unready": 9,
                    "ready": 0,
                    "pre": 0,
                    "queued": 0,
                    "post": 0,
                    "succeeded": 8,
                    "failed": 3,
                    "percent_done": 40.0,
                    "state": "Failure",
                    "total": 20,
                    "dagname": "*sample_3_failure.dag",
                }
            }
        }
        ),
        (
            'sample_4_removed',
            './status_sample_files/sample4',
            {
            "totals": {
                "unready": 0,
                "ready": 0,
                "pre": 0,
                "queued": 0,
                "post": 0,
                "succeeded": 0,
                "failed": 0,
                "percent_done": 0.0,
                "total": 0,
            },
            "dags": {
                "root": {
                    "unready": 21,
                    "ready": 0,
                    "pre": 0,
                    "queued": 4,
                    "post": 0,
                    "succeeded": 4,
                    "failed": 0,
                    "percent_done": 13.79,
                    "state": "Failure",
                    "total": 29,
                    "dagname": "*sample_4_removed.dag",
                }
            }
        }
        ),
        (
           'sample_5_failed_with_no_progress_lines',
            './status_sample_files/sample5',
            {
            "totals": {
                "unready": 0,
                "ready": 0,
                "pre": 0,
                "queued": 0,
                "post": 0,
                "succeeded": 0,
                "failed": 0,
                "percent_done": 0.0,
                "total": 0,
            },
            "dags": {
                "root": {
                    "unready": 0,
                    "ready": 0,
                    "pre": 0,
                    "queued": 0,
                    "post": 0,
                    "succeeded": 0,
                    "failed": 0,
                    "percent_done": 0.0,
                    "state": "Failure",
                    "total": 0,
                    "dagname": "*sample_5_failed_with_no_progress_lines.dag",
                }
            }
        }
        ),
        (
            'dagman_which_does_not_exists should return None',
            'random/submit/directory',
            None
        )
    ]
)
def test_fetch_status_json(mocker, status, pegasus_wf_name_from_bd, submit_dir, expected_dict):
    status.pegasus_wf_name = pegasus_wf_name_from_bd
    mocker.patch("Pegasus.client.status.Status._get_q_values")

    assert status.fetch_status(submit_dir, json=True) == expected_dict
    status._get_q_values.assert_called_once_with(submit_dir)

'''
Will be modified as expected output of wrong dir input will be changed to throw an error
instead of displaying ("No matchin jobs found")

def test_fetch_status_invalid_submit_dir(mocker, status, caplog):
    mocker.patch("Pegasus.client.status.Status._get_q_values",return_value=None)
    mocker.patch("Pegasus.client.status.Status._get_progress",return_value=None)

    submit_dir = "random/submit/directory"

    temp_logger = logging.getLogger("testing_fetch_status")
    with caplog.at_level(logging.DEBUG):
        status.fetch_status(submit_dir)

    assert "No matchin jobs found" in caplog.text

    Pegasus.client.status.Status._get_q_values.assert_called_once_with(submit_dir)
    Pegasus.client.status.Status._get_progress.assert_called_once_with(submit_dir)
'''

def test_get_condor_jobs(mocker,status):
    q_values = [{'JobID':1,'Iwd':'dir1'},{'JobID':2,'Iwd':'dir2'}]
    mocker.patch("Pegasus.client.condor._q", return_value=q_values)
    mocker.patch("shutil.which", return_value="/usr/bin/pegasus-version")

    root_wf_uuid = 'root_wf_uuid'
    expression = r""'pegasus_wf_uuid == "{}"'"".format(root_wf_uuid)
    cmd = ['condor_q','-constraint',expression,'-json']

    assert status._get_condor_jobs(root_wf_uuid) == q_values
    Pegasus.client.condor._q.assert_called_once_with(cmd)
