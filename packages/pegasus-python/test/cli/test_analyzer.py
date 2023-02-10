import os
from collections import defaultdict
from textwrap import dedent

import pytest

from Pegasus import analyzer
from Pegasus.db import connection

directory = os.path.dirname(__file__)


@pytest.fixture(scope="function")
def Analyzer():
    return analyzer

def test_should_create_Analyzer(Analyzer):
    assert Analyzer is not None    
    

@pytest.mark.parametrize(
    "expected_output, submit_dir_db, wf_uuid",
    [
        (
            dedent(
                """
                ************************************Summary*************************************

                 Submit Directory   : None
                 Workflow Status    : running
                 Total jobs         :     13 (100.00%)
                 # jobs succeeded   :      9 (69.23%)
                 # jobs failed      :      0 (0.00%)
                 # jobs held        :      0 (0.00%)
                 # jobs unsubmitted :      4 (30.77%)
 
                """
            ),
            "analyzer_samples_dir/hierarchical_wf/hierarchical-workflow-0.stampede.db",
            "14729e62-8451-4081-8935-cad6d3391b84"
        ),
        (
            dedent(
                """
                ************************************Summary*************************************

                 Submit Directory   : None
                 Workflow Status    : success
                 Total jobs         :      5 (100.00%)
                 # jobs succeeded   :      5 (100.00%)
                 # jobs failed      :      0 (0.00%)
                 # jobs held        :      0 (0.00%)
                 # jobs unsubmitted :      0 (0.00%)
 
                """
            ),
            "analyzer_samples_dir/process_wf_success/process-0.stampede.db",
            "130612dc-736f-4b85-8a8d-878d456f7394"
        )
    ]
)
def test_running_and_success_workflows(mocker, capsys, Analyzer, expected_output, submit_dir_db, wf_uuid):
    database_url = "sqlite:////" + os.path.join(directory, submit_dir_db)
    mocker.patch("Pegasus.db.connection.url_by_submitdir", return_value=database_url)
    mocker.patch("Pegasus.db.connection.get_wf_uuid", return_value=wf_uuid)
    Analyzer.analyze_db(None)
    captured = capsys.readouterr()
    captured_output = captured.out.lstrip()
    assert captured_output[captured_output.find("*"):] == expected_output.lstrip()
    

@pytest.mark.parametrize(
    "expected_output, submit_dir_db, wf_uuid",
    [
        (
            dedent(
                """
                ************************************Summary*************************************

                 Submit Directory   : None
                 Workflow Status    : failure
                 Total jobs         :      5 (100.00%)
                 # jobs succeeded   :      1 (20.00%)
                 # jobs failed      :      1 (20.00%)
                 # jobs held        :      0 (0.00%)
                 # jobs unsubmitted :      3 (60.00%)

                ******************************Failed jobs' details******************************

                ==================================ls_ID0000001==================================

                 last state: POST_SCRIPT_FAILED
                       site: condorpool
                submit file: 00/00/ls_ID0000001.sub
                output file: 00/00/ls_ID0000001.out.001
                 error file: 00/00/ls_ID0000001.err.001

                -------------------------------Task #1 - Summary--------------------------------

                site        : condorpool
                hostname    : compute-7.isi.edu
                executable  : /usr/bin/ls
                arguments   : -
                exitcode    : 2
                working dir : /var/lib/condor/execute/dir_3343119

                ------------------Task #1 - ls - ID0000001 - Kickstart stderr-------------------

                 /bin/ls: invalid option -- 'z'
                Try '/bin/ls --help' for more information.
 
                """
            ),
            "analyzer_samples_dir/process_wf_failure/process-0.stampede.db",
            "a42ca854-7c15-41ad-82dc-72fdd191a191"
        )
    ]
)
def test_failure_workflow(mocker, capsys, Analyzer, expected_output, submit_dir_db, wf_uuid):
    database_url = "sqlite:////" + os.path.join(directory, submit_dir_db)
    mocker.patch("Pegasus.db.connection.url_by_submitdir", return_value=database_url)
    mocker.patch("Pegasus.db.connection.get_wf_uuid", return_value=wf_uuid)
    with pytest.raises(Exception) as err:
        Analyzer.analyze_db(None)
    assert "One or more workflows failed" in str(err)
    captured = capsys.readouterr()
    captured_output = captured.out.lstrip()
    assert captured_output[captured_output.find("*"):] == expected_output.lstrip()

    
@pytest.mark.parametrize(
    "expected_output, submit_dir_db, wf_uuid",
    [
        (
            dedent(
                """
                ************************************Summary*************************************

                 Submit Directory   : None
                 Workflow Status    : failure
                 Total jobs         :      5 (100.00%)
                 # jobs succeeded   :      1 (20.00%)
                 # jobs failed      :      1 (20.00%)
                 # jobs held        :      0 (0.00%)
                 # jobs unsubmitted :      3 (60.00%)
 
                """
            ),
            "analyzer_samples_dir/process_wf_failure/process-0.stampede.db",
            "a42ca854-7c15-41ad-82dc-72fdd191a191"
        )
    ]
)
def test_summary_mode(mocker, capsys, Analyzer, expected_output, submit_dir_db, wf_uuid):
    database_url = "sqlite:////" + os.path.join(directory, submit_dir_db)
    mocker.patch("Pegasus.db.connection.url_by_submitdir", return_value=database_url)
    mocker.patch("Pegasus.db.connection.get_wf_uuid", return_value=wf_uuid)
    mocker.patch("Pegasus.analyzer.summary_mode", return_value=True)
    with pytest.raises(Exception) as err:
        Analyzer.analyze_db(None)
    assert "One or more workflows failed" in str(err)
    captured = capsys.readouterr()
    captured_output = captured.out.lstrip()
    assert captured_output[captured_output.find("*"):] == expected_output.lstrip()

    
@pytest.mark.parametrize(
    "expected_output, submit_dir_db, wf_uuid",
    [
        (
            dedent(
                """
                ************************************Summary*************************************

                 Submit Directory   : None
                 Workflow Status    : failure
                 Total jobs         :      5 (100.00%)
                 # jobs succeeded   :      1 (20.00%)
                 # jobs failed      :      1 (20.00%)
                 # jobs held        :      0 (0.00%)
                 # jobs unsubmitted :      3 (60.00%)
                 
                ******************************Failed jobs' details******************************

                ==================================ls_ID0000001==================================

                 last state: POST_SCRIPT_FAILED
                       site: condorpool
                submit file: 00/00/ls_ID0000001.sub
                output file: 00/00/ls_ID0000001.out.001
                 error file: 00/00/ls_ID0000001.err.001

                -------------------------------Task #1 - Summary--------------------------------

                site        : condorpool
                hostname    : compute-7.isi.edu
                executable  : /usr/bin/ls
                arguments   : -
                exitcode    : 2
                working dir : /var/lib/condor/execute/dir_3343119

                ------------------Job stderr file - 00/00/ls_ID0000001.err.001------------------

                2023-02-10 08:41:07: PegasusLite: version 5.1.0dev
                2023-02-10 08:41:07: Executing on host compute-7.isi.edu IP=128.9.46.37

                ########################[Pegasus Lite] Setting up workdir ########################
                2023-02-10 08:41:07: Not creating a new work directory as it is already set to /var/lib/condor/execute/dir_3343119

                ##############[Pegasus Lite] Figuring out the worker package to use ##############
                2023-02-10 08:41:07: The job contained a Pegasus worker package
                tar: write error

                ##################### Checking file integrity for input files #####################

                ######################[Pegasus Lite] Executing the user task ######################
                2023-02-10 08:41:10: User task failed with exitcode 2
                PegasusLite: exitcode 2
                
                """
            ),
            "analyzer_samples_dir/process_wf_failure/process-0.stampede.db",
            "a42ca854-7c15-41ad-82dc-72fdd191a191"
        )
    ]
)
def test_quiet_mode(mocker, capsys, Analyzer, expected_output, submit_dir_db, wf_uuid):
    database_url = "sqlite:////" + os.path.join(directory, submit_dir_db)
    mocker.patch("Pegasus.db.connection.url_by_submitdir", return_value=database_url)
    mocker.patch("Pegasus.db.connection.get_wf_uuid", return_value=wf_uuid)
    mocker.patch("Pegasus.analyzer.quiet_mode", return_value=True)
    with pytest.raises(Exception) as err:
        Analyzer.analyze_db(None)
    assert "One or more workflows failed" in str(err)
    captured = capsys.readouterr()
    captured_output = captured.out.lstrip()
    assert captured_output[captured_output.find("*"):] == expected_output.lstrip()
    
