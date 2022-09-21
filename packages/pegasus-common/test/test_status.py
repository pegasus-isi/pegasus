import os
import time
import pytest
import logging
from textwrap import dedent

import Pegasus
from Pegasus.client.status import Status

directory = os.path.dirname(__file__)

@pytest.fixture(scope="function")
def status():
    return Status()

def test_should_create_Status(status):
    assert status is not None

@pytest.mark.parametrize(
    "pegasus_wf_name_from_bd, samples_dir, expected_dict",
    [
        (
            'sample_1_success',
            'status_sample_files/sample1',
            {
            "totals": {
                "unready": 0,
                "ready": 0,
                "pre": 0,
                "queued": 0,
                "post": 0,
                "succeeded": 17,
                "failed": 0,
                "percent_done": 100.0,
                "total": 17,
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
                    "dagname": "sample_1_success.dag",
                }
            }
        }
        ),
        (
            'sample_2_held',
            'status_sample_files/sample2',
            {
            "totals": {
                "unready": 28,
                "ready": 0,
                "pre": 0,
                "queued": 1,
                "post": 0,
                "succeeded": 0,
                "failed": 0,
                "percent_done": 0.0,
                "total": 29,
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
                    "dagname": "sample_2_held.dag",
                }
            }
        }
        ),
        (
            'sample_3_failure',
            'status_sample_files/sample3',
            {
            "totals": {
                "unready": 9,
                "ready": 0,
                "pre": 0,
                "queued": 0,
                "post": 0,
                "succeeded": 8,
                "failed": 3,
                "percent_done": 40.0,
                "total": 20,
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
                    "dagname": "sample_3_failure.dag",
                }
            }
        }
        ),
        (
            'sample_4_removed',
            'status_sample_files/sample4',
            {
            "totals": {
                "unready": 21,
                "ready": 0,
                "pre": 0,
                "queued": 4,
                "post": 0,
                "succeeded": 4,
                "failed": 0,
                "percent_done": 13.79,
                "total": 29,
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
                    "dagname": "sample_4_removed.dag",
                }
            }
        }
        ),
        (
           'sample_5_failed_with_no_progress_lines',
            'status_sample_files/sample5',
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
                    "dagname": "sample_5_failed_with_no_progress_lines.dag",
                }
            }
        } 
        ),
        (
            'dagman_which_does_not_exists should return None',
            'random/submit/directory',
            None
        ),
        #Hierarchical workflow tests
        (
           'sample1_hr_success',
            'status_sample_files/sample1_hr',
            {
            "totals": {
                "unready": 0,
                "ready": 0,
                "pre": 0,
                "queued": 0,
                "post": 0,
                "succeeded": 33,
                "failed": 0,
                "percent_done": 100.0,
                "total": 33,
            },
            "dags": {
                "root": {
                    "unready": 0,
                    "ready": 0,
                    "pre": 0,
                    "queued": 0,
                    "post": 0,
                    "succeeded": 13,
                    "failed": 0,
                    "percent_done": 100.0,
                    "state": "Success",
                    "total": 13,
                    "dagname": "sample1_hr_success.dag",
                },
                'sleep-wf-0': {
                    'unready': 0,
                    'ready': 0,
                    'pre': 0,
                    'queued': 0,
                    'post': 0,
                    'succeeded': 4,
                    'failed': 0,
                    'percent_done': 100.0,
                    'total': 4,
                    'dagname': 'sleep-wf-0.dag',
                    'state': 'Success'
                },
                'inner': {
                    'unready': 0,
                    'ready': 0,
                    'pre': 0,
                    'queued': 0,
                    'post': 0,
                    'succeeded': 16,
                    'failed': 0,
                    'percent_done': 100.0,
                    'total': 16,
                    'dagname': 'inner.dag',
                    'state': 'Success'
                }
            }
        } 
        ),
        (
           'sample2_hr_failure',
            'status_sample_files/sample2_hr',
            {
            "totals": {
                "unready": 4,
                "ready": 0,
                "pre": 0,
                "queued": 0,
                "post": 0,
                "succeeded": 8,
                "failed": 1,
                "percent_done": 61.54,
                "total": 13,
            },
            "dags": {
                "root": {
                    "unready": 4,
                    "ready": 0,
                    "pre": 0,
                    "queued": 0,
                    "post": 0,
                    "succeeded": 8,
                    "failed": 1,
                    "percent_done": 61.54,
                    "state": "Failure",
                    "total": 13,
                    "dagname": "sample2_hr_failure.dag",
                },
                'inner': {
                    'unready': 0,
                    'ready': 0,
                    'pre': 0,
                    'queued': 0,
                    'post': 0,
                    'succeeded': 0,
                    'failed': 0,
                    'percent_done': 0.0,
                    'total': 0,
                    'dagname': 'inner.dag',
                    'state': 'Failure'
                }
            }
        } 
        )
    ]
)
def test_fetch_status_json(mocker, status, pegasus_wf_name_from_bd, samples_dir, expected_dict):
    status.pegasus_wf_name = pegasus_wf_name_from_bd
    mocker.patch("Pegasus.client.status.Status._get_q_values")
    submit_dir = os.path.join(directory,samples_dir)
    assert status.fetch_status(submit_dir, json=True) == expected_dict
    status._get_q_values.assert_called_once_with(submit_dir)


@pytest.mark.parametrize(
    "pegasus_wf_name_from_bd, samples_dir, expected_output",
    [
        (
            'sample_1_success',
            'status_sample_files/sample1',
            dedent(
                """
                (No matching jobs found in Condor Q)

                UNREADY  READY   PRE   QUEUED   POST   SUCCESS  FAILURE  %DONE  
                   0       0      0      0       0       17        0     100.0  
                Summary: 1 DAG total (Success:1)
                
                """
            )
        ),
        (
            'sample_2_held',
            'status_sample_files/sample2',
            dedent(
                """
                (No matching jobs found in Condor Q)

                UNREADY  READY   PRE   QUEUED   POST   SUCCESS  FAILURE  %DONE  
                   28      0      0      1       0        0        0      0.0   
                Summary: 1 DAG total (Running:1)
                
                """
            )
        ),
        (
            'sample_3_failure',
            'status_sample_files/sample3',
            dedent(
                """
                (No matching jobs found in Condor Q)

                UNREADY  READY   PRE   QUEUED   POST   SUCCESS  FAILURE  %DONE  
                   9       0      0      0       0        8        3      40.0  
                Summary: 1 DAG total (Failure:1)
                
                """
            )
        ),
        (
            'sample_4_removed',
            'status_sample_files/sample4',
            dedent(
                """
                (No matching jobs found in Condor Q)

                UNREADY  READY   PRE   QUEUED   POST   SUCCESS  FAILURE  %DONE  
                   21      0      0      4       0        4        0     13.79  
                Summary: 1 DAG total (Failure:1)
                
                """
            )
        ),
        (
           'sample_5_failed_with_no_progress_lines',
            'status_sample_files/sample5',
            dedent(
                """
                (No matching jobs found in Condor Q)

                UNREADY  READY   PRE   QUEUED   POST   SUCCESS  FAILURE  %DONE  
                   0       0      0      0       0        0        0      0.0   
                Summary: 1 DAG total (Failure:1)
                
                """
            )
        ),
        #Hierarchical Workflow tests
        (
            'sample1_hr_success',
            'status_sample_files/sample1_hr',
            dedent(
                """
                (No matching jobs found in Condor Q)

                UNREADY  READY   PRE   QUEUED   POST   SUCCESS  FAILURE  %DONE  
                   0       0      0      0       0       33        0     100.0  
                Summary: 3 DAGs total (Success:3)
                
                """
            )
        ),
        (
            'sample2_hr_failure',
            'status_sample_files/sample2_hr',
            dedent(
                """
                (No matching jobs found in Condor Q)

                UNREADY  READY   PRE   QUEUED   POST   SUCCESS  FAILURE  %DONE  
                   4       0      0      0       0        8        1     61.54  
                Summary: 2 DAGs total (Failure:2)
                
                """
            )
        )
    ]
)
def test_show_dag_progress(mocker,caplog,status,pegasus_wf_name_from_bd, samples_dir, expected_output):
    status.pegasus_wf_name = pegasus_wf_name_from_bd
    mocker.patch("Pegasus.client.status.Status._get_q_values",return_value=None)
    submit_dir = os.path.join(directory,samples_dir)
    with caplog.at_level(logging.INFO):
        status.fetch_status(submit_dir)
        assert status.progress_string == expected_output
    Pegasus.client.status.Status._get_q_values.assert_called_once_with(submit_dir)

    
@pytest.mark.parametrize(
    "pegasus_wf_name_from_bd, samples_dir, expected_output",
    [
        (
            'sample_1_success',
            'status_sample_files/sample1',
            dedent(
                """
                (No matching jobs found in Condor Q)

                UNREADY READY  PRE  IN_Q  POST  DONE  FAIL %DONE  STATE  DAGNAME                  
                   0      0     0    0     0     17    0   100.0 Success sample_1_success.dag     
                Summary: 1 DAG total (Success:1)
                
                """
            )
        ),
        (
            'sample_2_held',
            'status_sample_files/sample2',
            dedent(
                """
                (No matching jobs found in Condor Q)

                UNREADY READY  PRE  IN_Q  POST  DONE  FAIL %DONE  STATE  DAGNAME                  
                  28      0     0    1     0     0     0    0.0  Running sample_2_held.dag        
                Summary: 1 DAG total (Running:1)
                
                """
            )
        ),
        (
            'sample_3_failure',
            'status_sample_files/sample3',
            dedent(
                """
                (No matching jobs found in Condor Q)

                UNREADY READY  PRE  IN_Q  POST  DONE  FAIL %DONE  STATE  DAGNAME                  
                   9      0     0    0     0     8     3    40.0 Failure sample_3_failure.dag     
                Summary: 1 DAG total (Failure:1)
                
                """
            )
        ),
        #Hierarchical Workflow tests
        (
            'sample1_hr_success',
            'status_sample_files/sample1_hr',
            dedent(
                """
                (No matching jobs found in Condor Q)

                UNREADY READY  PRE  IN_Q  POST  DONE  FAIL %DONE  STATE  DAGNAME                  
                   0      0     0    0     0     13    0   100.0 Success sample1_hr_success.dag   
                   0      0     0    0     0     4     0   100.0 Success   ├─sleep-wf-0.dag       
                   0      0     0    0     0     16    0   100.0 Success   └─inner.dag            
                   0      0     0    0     0     33    0   100.0         TOTALS(33 jobs)          
                Summary: 3 DAGs total (Success:3)
                
                """
            )
        ),
        (
            'sample2_hr_failure',
            'status_sample_files/sample2_hr',
            dedent(
                """
                (No matching jobs found in Condor Q)

                UNREADY READY  PRE  IN_Q  POST  DONE  FAIL %DONE  STATE  DAGNAME                  
                   4      0     0    0     0     8     1   61.54 Failure sample2_hr_failure.dag   
                   0      0     0    0     0     0     0    0.0  Failure   └─inner.dag            
                   4      0     0    0     0     8     1   61.54         TOTALS(13 jobs)          
                Summary: 2 DAGs total (Failure:2)
                
                """
            )
        )
    ]
)
def test_show_dag_progress_long(mocker,caplog,status,pegasus_wf_name_from_bd, samples_dir, expected_output):
    status.pegasus_wf_name = pegasus_wf_name_from_bd
    mocker.patch("Pegasus.client.status.Status._get_q_values",return_value=None)
    submit_dir = os.path.join(directory,samples_dir)
    with caplog.at_level(logging.INFO):
        status.fetch_status(submit_dir,long=True) 
        assert status.progress_string == expected_output
    Pegasus.client.status.Status._get_q_values.assert_called_once_with(submit_dir)


def test_get_condor_jobs(mocker,status):
    q_values = [{'JobID':1,'Iwd':'dir1'},{'JobID':2,'Iwd':'dir2'}]
    mocker.patch("Pegasus.client.condor._q", return_value=q_values)
    mocker.patch("shutil.which", return_value="/usr/bin/pegasus-version")
    root_wf_uuid = 'root_wf_uuid'
    expression = r""'pegasus_wf_uuid == "{}"'"".format(root_wf_uuid)
    cmd = ['condor_q','-constraint',expression,'-json']
    assert status._get_condor_jobs(root_wf_uuid) == q_values
    Pegasus.client.condor._q.assert_called_once_with(cmd)

@pytest.mark.parametrize(
    "condor_q_values, expected_output",
    [
        (
            [
                {
                    'JobStatus':2,
                    'EnteredCurrentStatus':time.time()-500,
                    'pegasus_wf_xformation': 'pegasus::dagman',
                    'Iwd':'root/workflow/submit/directory',
                    'pegasus_wf_dag_job_id':'sample-workflow-0',
                    'pegasus_wf_name':'sample-workflow-0'
                },
                {
                    'JobStatus':2,
                    'EnteredCurrentStatus':time.time()-300,
                    'pegasus_wf_xformation':'pegasus::job',
                    'pegasus_wf_dag_job_id':'job1',
                },
                {
                    'JobStatus':1,
                    'EnteredCurrentStatus':time.time()-200,
                    'pegasus_wf_xformation': 'pegasus::job',
                    'pegasus_wf_dag_job_id':'job2',
                },
                {
                    'JobStatus':1,
                    'EnteredCurrentStatus':time.time()-100,
                    'pegasus_wf_xformation': 'pegasus::job',
                    'pegasus_wf_dag_job_id':'job3',
                }
            ],
            dedent(
                """
                STAT  IN_STATE  JOB                      
                Run     08:20   sample-workflow-0 (root/workflow/submit/directory)
                Run     05:00    ┣━job1                     
                Idle    03:20    ┣━job2                     
                Idle    01:40    ┗━job3                     
                Summary: 4 Condor jobs total (I:2 R:2)
                """
            )
        )
    ]
)
def test_show_condor_jobs(mocker,caplog,status,condor_q_values,expected_output):
    mocker.patch("Pegasus.client.status.Status._get_q_values",return_value=condor_q_values)
    mocker.patch("Pegasus.client.status.Status._get_progress",return_value=None)
    submit_dir = 'submit_dir'
    with caplog.at_level(logging.INFO):
        status.fetch_status(submit_dir) 
        assert status.progress_string == expected_output
    Pegasus.client.status.Status._get_q_values.assert_called_once_with(submit_dir)
    
def test_valid_braindump_dir(mocker,status):
    mocker.patch("Pegasus.client.status.Status._get_condor_jobs",return_value=['job1','job2'])
    submit_dir = os.path.join(directory,'status_sample_files/sample1')
    assert status._get_q_values(submit_dir) == ['job1','job2']
    assert status.root_wf_uuid == "d943d68b-ffc6-4154-8b82-9d8be4dbd683"
    assert status.root_wf_name == "Drug-Combination-Therapy-0"
    
def test_get_braindump_invalid_dir(mocker,status):
    submit_dir = 'some/random/directory'
    with pytest.raises(FileNotFoundError) as err:
        status._get_braindump(submit_dir) == ''
    assert "Unable to load braindump file" in str(err)
