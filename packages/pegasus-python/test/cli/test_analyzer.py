import os
import glob
import logging
import shutil
import tempfile
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
    
    
def test_with_submit_dir(mocker, capsys, Analyzer):
    submit_dir = os.path.join(directory, "analyzer_samples_dir/process_wf_success")
    mocker.patch.object(Analyzer, "input_dir", submit_dir)
    
    Analyzer.analyze_db(None)
    captured = capsys.readouterr()
    captured_output = captured.out.lstrip()
    expected_output = dedent(
                f"""\
                ************************************Summary*************************************

                 Submit Directory   : {submit_dir}
                 Workflow Status    : success
                 Total jobs         :      5 (100.00%)
                 # jobs succeeded   :      5 (100.00%)
                 # jobs failed      :      0 (0.00%)
                 # jobs held        :      0 (0.00%)
                 # jobs unsubmitted :      0 (0.00%)
 
                """
            )
    assert captured_output[captured_output.find("*"):] == expected_output.lstrip()   
    

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
                working dir : /var/lib/condor/execute/dir_1653659

                ------------------Task #1 - ls - ID0000001 - Kickstart stderr-------------------

                 /bin/ls: invalid option -- 'z'
                Try '/bin/ls --help' for more information.
 
                """
            ),
            "analyzer_samples_dir/process_wf_failure/process-0.stampede.db",
            "432031f4-af13-40a9-aef2-cc1760f3e5f1"
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
            "432031f4-af13-40a9-aef2-cc1760f3e5f1"
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
                working dir : /var/lib/condor/execute/dir_1653659

                ------------------Job stderr file - 00/00/ls_ID0000001.err.001------------------

                2023-02-14 16:46:10: PegasusLite: version 5.1.0dev
                2023-02-14 16:46:10: Executing on host compute-7.isi.edu IP=128.9.46.37

                ########################[Pegasus Lite] Setting up workdir ########################
                2023-02-14 16:46:10: Not creating a new work directory as it is already set to /var/lib/condor/execute/dir_1653659

                ##############[Pegasus Lite] Figuring out the worker package to use ##############
                2023-02-14 16:46:10: The job contained a Pegasus worker package
                tar: write error

                ##################### Checking file integrity for input files #####################

                ######################[Pegasus Lite] Executing the user task ######################
                2023-02-14 16:46:13: User task failed with exitcode 2
                PegasusLite: exitcode 2
                
                """
            ),
            "analyzer_samples_dir/process_wf_failure/process-0.stampede.db",
            "432031f4-af13-40a9-aef2-cc1760f3e5f1"
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
    

def test_use_files(mocker, capsys, Analyzer):
    submit_dir = os.path.join(directory, "analyzer_samples_dir/process_wf_failure")
    mocker.patch.object(Analyzer,"input_dir", submit_dir)
    mocker.patch.object(Analyzer,"use_files", True)
    
    with pytest.raises(Exception) as err:
        Analyzer.analyze_files()
    assert "One or more workflows failed" in str(err)
    captured = capsys.readouterr()
    captured_output = captured.out.lstrip()
    expected_output = dedent(
        f"""\
        ************************************Summary*************************************

         Submit Directory   : {submit_dir}
         Total jobs         :     10 (100.00%)
         # jobs succeeded   :      2 (20.00%)
         # jobs failed      :      3 (30.00%)
         # jobs held        :      0 (0.00%)
         # jobs unsubmitted :      6 (60.00%)
         
        ******************************Failed jobs' details******************************

        ==================================ls_ID0000001==================================

         last state: POST_SCRIPT_FAILURE
               site: submit file not available
        submit file: /home/mzalam/pegasus_master/pegasus/packages/pegasus-python/test/cli/analyzer_samples_dir/process_wf_failure/00/00/ls_ID0000001.sub
        output file: None
         error file: None
         
        """
    )
    assert captured_output == expected_output
    

def test_invoke_monitord(mocker, Analyzer):
    temp_dir = tempfile.mkdtemp('sample_temp_dir')
    dagman_out_file = os.path.join(directory, "analyzer_samples_dir/process_wf_failure/process-0.dag.dagman.out")
    Analyzer.invoke_monitord(dagman_out_file,temp_dir)
    
    assert len(os.listdir(temp_dir)) == 5
    assert len(glob.glob(temp_dir+"/*jobstate.log")) == 1
    assert len(glob.glob(temp_dir+"/*monitord.done")) == 1
    assert len(glob.glob(temp_dir+"/*monitord.started")) == 1
    assert len(glob.glob(temp_dir+"/*monitord.info")) == 1
    assert len(glob.glob(temp_dir+"/monitord.subwf")) == 1


def test_dump_file(mocker, capsys, Analyzer):
    file_path = os.path.join(directory,"analyzer_samples_dir/process_wf_success/braindump.yml")
    Analyzer.dump_file(file_path)
    captured = capsys.readouterr()
    captured_output = captured.out.lstrip()
    assert 'user: "mzalam"' in captured_output
    

def test_traverse_all_subworkflows(mocker, capsys, Analyzer):
    db_url = "sqlite:////home/mzalam/028-dynamic-hierarchy/work/mzalam/pegasus/hierarchical-workflow/run0001/hierarchical-workflow-0.stampede.db"
    #db_version = os.popen("pegasus-version").read().rstrip()
    db_version = '5.0.1'
    submit_dir = os.path.join(directory, "analyzer_samples_dir/hierarchical_wf")
    mocker.patch.object(Analyzer,"input_dir", submit_dir)
    mocker.patch.object(Analyzer,"traverse_all", True)
    Analyzer.analyze_db(None)
    
    captured = capsys.readouterr()
    captured_output = captured.out.lstrip()
    expected_output = dedent(
        f"""\
        Database version: '{db_version}' ({db_url})
        Database version: '{db_version}' ({db_url})
        Database version: '{db_version}' ({db_url})

        ************************************Summary*************************************

         Submit Directory   : {submit_dir}
         Workflow Status    : success
         Total jobs         :     13 (100.00%)
         # jobs succeeded   :     13 (100.00%)
         # jobs failed      :      0 (0.00%)
         # jobs held        :      0 (0.00%)
         # jobs unsubmitted :      0 (0.00%)

        Database version: '{db_version}' ({db_url})
        Database version: '{db_version}' ({db_url})

        ************************************Summary*************************************

         Submit Directory   : {submit_dir}
         Workflow Status    : success
         Total jobs         :     16 (100.00%)
         # jobs succeeded   :     16 (100.00%)
         # jobs failed      :      0 (0.00%)
         # jobs held        :      0 (0.00%)
         # jobs unsubmitted :      0 (0.00%)

        Database version: '{db_version}' ({db_url})
        Database version: '{db_version}' ({db_url})

        ************************************Summary*************************************

         Submit Directory   : {submit_dir}
         Workflow Status    : success
         Total jobs         :      4 (100.00%)
         # jobs succeeded   :      4 (100.00%)
         # jobs failed      :      0 (0.00%)
         # jobs held        :      0 (0.00%)
         # jobs unsubmitted :      0 (0.00%)

        """
    )
    assert captured_output == expected_output
    

def test_debug_mode_no_debug_dir(mocker, capsys, Analyzer):
    submit_dir = os.path.join(directory, "analyzer_samples_dir/process_wf_failure")
    debug_job = os.path.join(submit_dir, "ls_ID0000001.sub")
    mocker.patch.object(Analyzer,"debug_job", debug_job)
    mocker.patch.object(Analyzer,"input_dir", submit_dir)
    
    Analyzer.debug_workflow()
    captured = capsys.readouterr()
    captured_output = captured.out.lstrip()
    assert "finished generating job debug script!" in captured_output


def test_addon(mocker, Analyzer):
    class Options(object):
        def __init__(self, initial_data):
            for key in initial_data:
                setattr(self, key, initial_data[key])
        
    assert Analyzer.addon(Options({"recurse_mode":True,
                                    "quiet_mode" :True,
                                    "summary_mode" :True,
                                    "use_files" :True,
                                    "indent_length" : 2,
                                  })) == "--recurse --quiet --summary --files --indent 3 "
    

def test_debug_mode_with_debug_dir(mocker, capsys, Analyzer):
    submit_dir = os.path.join(directory, "analyzer_samples_dir/process_wf_failure")
    temp_dir = tempfile.mkdtemp('sample_temp_dir')
    debug_job = os.path.join(submit_dir, "ls_ID0000001.sub")
    mocker.patch.object(Analyzer,"debug_job", debug_job)
    mocker.patch.object(Analyzer,"input_dir", submit_dir)
    mocker.patch.object(Analyzer,"debug_dir", temp_dir)
    
    Analyzer.debug_workflow()
    captured = capsys.readouterr()
    captured_output = captured.out.lstrip()
    assert "finished generating job debug script!" in captured_output
    assert "sample_temp_dir" in captured_output
    shutil.rmtree(temp_dir)
    
