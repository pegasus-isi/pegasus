import os
import glob
import tempfile
from textwrap import dedent

import pytest
from Pegasus.db.workflow.stampede_statistics import StampedeStatistics
from Pegasus.analyzer import *

directory = os.path.dirname(__file__)
pegasus_version = '5.0.1' #os.popen("pegasus-version").read()


@pytest.fixture(scope="function")
def AnalyzerDatabase():
    return AnalyzeDB


@pytest.fixture(scope="function")
def AnalyzerFiles():
    return AnalyzeFiles


@pytest.fixture(scope="function")
def AnalyzerDebug():
    return DebugWF


@pytest.fixture(scope="function")
def AnalyzerOptions():
    return Options


@pytest.fixture(scope="function")
def BaseAnalyzer():
    return BaseAnalyze


@pytest.fixture(scope="function")
def Output():
    return AnalyzerOutput


class TestBaseAnalyze:
    
    def test_should_create_BaseAnalyze(self, BaseAnalyzer):
        assert BaseAnalyzer is not None
        
        
    def test_check_wf_start(self, mocker, capsys, BaseAnalyzer):
        mocker.patch("Pegasus.analyzer.BaseAnalyze.backticks")
        BaseAnalyzer.check_for_wf_start(Options(input_dir='temp'),Counts(0,0,0,0,0,0,[],[]))
        captured = capsys.readouterr()
        captured_output = captured.out.lstrip()
        expected_output = "HTCondor DAGMan expects submit directories to be NOT NFS mounted\n Set your submit directory to a directory on the local filesystem OR \n    Set HTCondor configuration CREATE_LOCKS_ON_LOCAL_DISK and ENABLE_USERLOG_LOCKING to True. Check HTCondor documentation for further details."
        assert expected_output in captured_output
        
    def test_addon(self, mocker, BaseAnalyzer):
        opts = Options(
                recurse_mode=True,
                quiet_mode=True,
                summary_mode=True,
                use_files=True,
                indent_length = 2
        )
        
        assert BaseAnalyzer.addon(opts) == "--recurse --quiet --summary --files --indent 3 "   

    def test_parse_submit_file(self, mocker, capsys, BaseAnalyzer):
        submit_file = os.path.join(
                        directory,"analyzer_samples_dir/sample_wf_held/Drug-Combination-Therapy-0.dag.condor.sub"
                    )
        job = Job('job-0','running')
        job.is_subdax = True
        job.sub_file = submit_file
        job.retries = 3
        opts = Options(debug_mode=True,
            input_dir="pegasus/hierarchical-workflow/run0005",
            workflow_base_dir="/home/mzalam/028-dynamic-hierarchy"
            )
        BaseAnalyze.parse_submit_file(job, opts)
        assert job.transfer_input_files == 'rrr'
        

class TestAnalyzerOutput:
    
    def test_should_create_BaseAnalyze(self, Output):
        assert Output is not None
        
    def test_get_failed_workflows(self, Output):
        analyzer_output = Output()
        analyzer_output.workflows={'wf-0':Workflow(wf_status='failure')}
        expected = {'wf-0':Workflow(wf_status='failure')}
        assert analyzer_output.get_failed_workflows()['wf-0'].wf_status == 'failure'
    

class TestAnalyzeDB:
    
    def test_should_create_AnalyzeDB(self, AnalyzerDatabase):
        assert AnalyzerDatabase is not None

        
    def test_simple_wf_success(self, mocker, capsys, AnalyzerDatabase):
        submit_dir = os.path.join(directory, "analyzer_samples_dir/process_wf_success")
        analyze = AnalyzerDatabase(Options(input_dir=submit_dir))
        analyze.analyze_db(None)
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
        assert captured_output[captured_output.find("*") :] == expected_output.lstrip()
        
        
    def test_simple_wf_failure(self, mocker, capsys, AnalyzerDatabase):
        submit_dir = os.path.join(directory, "analyzer_samples_dir/process_wf_failure")
        analyze = AnalyzerDatabase(Options(input_dir=submit_dir))
        
        with pytest.raises(Exception) as err:
            analyze.analyze_db(None)
        assert "One or more workflows failed" in str(err)
        
        captured = capsys.readouterr()
        captured_output = captured.out.lstrip()
        expected_output = dedent(
            f"""\
                ************************************Summary*************************************

                 Submit Directory   : {submit_dir}
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
                hostname    : workflow.isi.edu
                executable  : /usr/bin/ls
                arguments   : -
                exitcode    : 2
                working dir : /home/mzalam/wf/condor/local/execute/dir_148537

                ------------------Task #1 - ls - ID0000001 - Kickstart stderr-------------------

                 /bin/ls: invalid option -- 'z'
                Try '/bin/ls --help' for more information.
 
                """
        )
        assert captured_output[captured_output.find("*") :] == expected_output.lstrip()
    

    def test_sample_wf_held(self, mocker, capsys, AnalyzerDatabase):
        submit_dir = os.path.join(directory, "analyzer_samples_dir/sample_wf_held")
        analyze = AnalyzerDatabase(Options(input_dir=submit_dir))
        
        with pytest.raises(Exception) as err:
            analyze.analyze_db(None)
        assert "One or more workflows failed" in str(err)
        
        captured = capsys.readouterr()
        captured_output = captured.out.lstrip()
        expected_output = dedent(
            f"""\
                ************************************Summary*************************************

                 Submit Directory   : {submit_dir}
                 Workflow Status    : failure
                 Total jobs         :      8 (100.00%)
                 # jobs succeeded   :      2 (25.00%)
                 # jobs failed      :      1 (12.50%)
                 # jobs held        :      1 (12.50%)
                 # jobs unsubmitted :      5 (62.50%)

                *******************************Held jobs' details*******************************

                ==========================chebi_drug_loader_ID0000001===========================

                submit file            : chebi_drug_loader_ID0000001.sub
                last_job_instance_id   : 3
                reason                 :  Transfer output files failure at execution point slot1@workflow.isi.edu while sending files to access point workflow. Details: reading from file /home/mzalam/wf/condor/local/execute/dir_761020/chebi_dict.pickle: (errno 2) No such file or directory

                ******************************Failed jobs' details******************************

                ==========================chebi_drug_loader_ID0000001===========================

                 last state: POST_SCRIPT_FAILED
                       site: condorpool
                submit file: 00/00/chebi_drug_loader_ID0000001.sub
                output file: 00/00/chebi_drug_loader_ID0000001.out.000
                 error file: 00/00/chebi_drug_loader_ID0000001.err.000

                -------------------------------Task #1 - Summary--------------------------------

                site        : condorpool
                hostname    : -
                executable  : /home/mzalam/covid/mzalam/pegasus/Drug-Combination-Therapy/run0013/00/00/chebi_drug_loader_ID0000001.sh
                arguments   : -
                exitcode    : -1
                working dir : /home/mzalam/covid/mzalam/pegasus/Drug-Combination-Therapy/run0013

                ----------Job stderr file - 00/00/chebi_drug_loader_ID0000001.err.000-----------

                2023-03-05 09:11:20: PegasusLite: version 5.0.5
                2023-03-05 09:11:20: Executing on host workflow.isi.edu IP=128.9.46.53

                ########################[Pegasus Lite] Setting up workdir ########################
                2023-03-05 09:11:21: Not creating a new work directory as it is already set to /home/mzalam/wf/condor/local/execute/dir_761020

                ##############[Pegasus Lite] Figuring out the worker package to use ##############
                2023-03-05 09:11:21: The job contained a Pegasus worker package
                tar: write error

                ########[Pegasus Lite] Writing out script to launch user task in container ########
                2023-03-05 09:11:22: Unable to load image from docker_container
                2023-03-05 09:11:22: Last command exited with 1
                PegasusLite: exitcode 1
 
                """
        )
        assert captured_output[captured_output.find("*") :] == expected_output.lstrip()


    def test_hierarchical_wf_success(self, mocker, capsys, AnalyzerDatabase):
        submit_dir = os.path.join(directory, "analyzer_samples_dir/hierarchical_wf_success")
        analyze = AnalyzerDatabase(Options(input_dir=submit_dir))
        analyze.analyze_db(None)
        captured = capsys.readouterr()
        captured_output = captured.out.lstrip()
        expected_output = dedent(
            f"""\
                ************************************Summary*************************************

                 Submit Directory   : {submit_dir}
                 Workflow Status    : running
                 Total jobs         :     13 (100.00%)
                 # jobs succeeded   :      9 (69.23%)
                 # jobs failed      :      0 (0.00%)
                 # jobs held        :      0 (0.00%)
                 # jobs unsubmitted :      4 (30.77%)
  
                """
        )
        assert captured_output[captured_output.find("*") :] == expected_output.lstrip()
        
        
    def test_hierarchical_wf_prescript_failure(self, mocker, capsys, AnalyzerDatabase):
        submit_dir = os.path.join(directory, "analyzer_samples_dir/hierarchical_wf_failure")
        analyze = AnalyzerDatabase(Options(input_dir=submit_dir,quiet_mode=True))
        
        with pytest.raises(Exception) as err:
            analyze.analyze_db(None)
        assert "One or more workflows failed" in str(err)
        
        captured = capsys.readouterr()
        captured_output = captured.out.lstrip()
        expected_output = dedent(
            f"""\
                ************************************Summary*************************************

                 Submit Directory   : {submit_dir}
                 Workflow Status    : failure
                 Total jobs         :     13 (100.00%)
                 # jobs succeeded   :      6 (46.15%)
                 # jobs failed      :      1 (7.69%)
                 # jobs held        :      0 (0.00%)
                 # jobs unsubmitted :      6 (46.15%)

                ******************************Failed jobs' details******************************

                ========================pegasus-plan_diamond_subworkflow========================

                 last state: JOB_FAILURE
                       site: local
                submit file: 00/00/pegasus-plan_diamond_subworkflow.sub
                output file: /home/mzalam/hierar/hierarichal-sample-wf/submit/mzalam/pegasus/hierarchical-workflow/run0003/pegasus-plan_diamond_subworkflow.pre.log.001
                 error file: 00/00/pegasus-plan_diamond_subworkflow.err

                -------------------------------Task #-1 - Summary-------------------------------

                site        : local
                hostname    : -
                executable  : 00/00/pegasus-plan_diamond_subworkflow.pre.sh
                arguments   :  -Dpegasus.log.*=/home/mzalam/hierar/hierarichal-sample-wf/submit/mzalam/pegasus/hierarchical-workflow/run0003/pegasus-plan_diamond_subworkflow.pre.log -Dpegasus.workflow.root.uuid=06031cb1-d2e1-41d8-9e8b-45c9a77e3eb8 --conf inner_diamond_workflow.pegasus.properties --dir /home/mzalam/hierar/hierarichal-sample-wf/submit --relative-dir mzalam/pegasus/hierarchical-workflow/run0003/00/00/./blackdiamond_diamond_subworkflow --relative-submit-dir mzalam/pegasus/hierarchical-workflow/run0003/00/00/./blackdiamond_diamond_subworkflow --basename inner --sites condorpool --cache /home/mzalam/hierar/hierarichal-sample-wf/submit/mzalam/pegasus/hierarchical-workflow/run0003/00/00/pegasus-plan_diamond_subworkflow.cache --output-map /home/mzalam/hierar/hierarichal-sample-wf/submit/mzalam/pegasus/hierarchical-workflow/run0003/00/00/pegasus-plan_diamond_subworkflow.output.map --output-sites local --cleanup none --verbose  --verbose  --verbose  --deferred  /home/mzalam/hierar/hierarichal-sample-wf/inner_diamond_workflow.yml
                exitcode    : 2
                working dir : /home/mzalam/hierar/hierarichal-sample-wf/submit/mzalam/pegasus/hierarchical-workflow/run0003

                """
        )
        assert captured_output[captured_output.find("*") :] == expected_output.lstrip()
   

    def test_prescript_failure_with_print_invocation(self, mocker, capsys, AnalyzerDatabase):
        submit_dir = os.path.join(directory, "analyzer_samples_dir/hierarchical_wf_failure")
        analyze = AnalyzerDatabase(Options(input_dir=submit_dir,
                                           print_invocation=True,
                                           print_pre_script=True))
        
        with pytest.raises(Exception) as err:
            analyze.analyze_db(None)
        assert "One or more workflows failed" in str(err)
        
        captured = capsys.readouterr()
        captured_output = captured.out.lstrip()
        assert "To re-run this job, use:" and "SCRIPT PRE:"in captured_output


    def test_traverse_all_subworkflows(self, mocker, capsys, AnalyzerDatabase):
        submit_dir = os.path.join(directory, "analyzer_samples_dir/hierarchical_wf_success")
        db_version = pegasus_version
        db_url = 'sqlite:////home/mzalam/hierar/hierarichal-sample-wf/submit/mzalam/pegasus/hierarchical-workflow/run0001/hierarchical-workflow-0.stampede.db'
        analyze = AnalyzerDatabase(Options(input_dir=submit_dir, traverse_all=True))
        analyze.analyze_db(None)
        captured = capsys.readouterr()
        captured_output = captured.out.lstrip()
        expected_output = dedent(
            f"""\
                ************************************Summary*************************************

                 Submit Directory   : {submit_dir}
                 Workflow Status    : running
                 Total jobs         :     13 (100.00%)
                 # jobs succeeded   :      9 (69.23%)
                 # jobs failed      :      0 (0.00%)
                 # jobs held        :      0 (0.00%)
                 # jobs unsubmitted :      4 (30.77%)

                Database version: '{db_version}' ({db_url})
                Database version: '{db_version}' ({db_url})

                ************************************Summary*************************************

                 Submit Directory   : {submit_dir}
                 Workflow Status    : success
                 Total jobs         :     15 (100.00%)
                 # jobs succeeded   :     15 (100.00%)
                 # jobs failed      :      0 (0.00%)
                 # jobs held        :      0 (0.00%)
                 # jobs unsubmitted :      0 (0.00%)

                """
        )
        assert captured_output[captured_output.find("*") :] == expected_output.lstrip()
        
        
    def test_analyze_db_workflow_error(self, mocker, capsys, AnalyzerDatabase):
        mocker.patch("Pegasus.analyzer.AnalyzeDB.get_workflow_status",return_value=WORKFLOW_STATUS.FAILURE)
        mock_wf = mocker.MagicMock()
        mock_wf_stats = mocker.MagicMock()
        mock_wf_stats.configure_mock(
            **{
                "get_failing_jobs.return_value": (1,2,3),
            }
        )
        
        analyze = AnalyzerDatabase(Options(summary_mode=True))
        with pytest.raises(Exception) as err:
            analyze.analyze_db_for_wf(mock_wf_stats,'uuid-0','submit_dir-0', mock_wf)
            assert "Workflow failed" in str(err)
            assert 'uuid-0' in str(err) and 'submit_dir-0' in str(err)


    def test_analyze_db_failing_jobs(self, mocker, capsys, AnalyzerDatabase):
        mocker.patch("Pegasus.analyzer.AnalyzeDB.get_job_details")
        mocker.patch("Pegasus.analyzer.AnalyzeDB.print_job_instance")
        mock_wf = mocker.MagicMock()

        class count:
            succeeded = None
            failed = None
        class failing:
            def _asdict():
                return {
                    "job_instance_id":"job-id-0"
                }
        class wf_stats:
            def get_total_jobs_status():
                return 0
            def get_total_succeeded_failed_jobs_status():
                return count
            def get_total_held_jobs():
                return []
            def get_failing_jobs():
                return None, None, [failing]
            def get_workflow_states():
                return
            def get_invocation_info(val):
                return
            def get_job_instance_info(val):
                return ['job-instance-0']
        
        analyze = AnalyzerDatabase(Options(input_dir='random/submit'))
        analyze.analyze_db_for_wf(wf_stats,'uuid-0','submit_dir-0', mock_wf)
        
        AnalyzeDB.print_job_instance.assert_called_once_with('job-id-0','job-instance-0',None)


    def test_json_mode(self, mocker, capsys, AnalyzerDatabase):
        submit_dir = os.path.join(directory, "analyzer_samples_dir/hierarchical_wf_success")
        analyze = AnalyzerDatabase(Options(input_dir=submit_dir, json_mode=True))
        analyzer_ouput = analyze.analyze_db(None)
         
        expected_output = {
              "root_wf_uuid": "48f759a4-165f-47d6-8c32-571649ce311b",
              "submit_directory": submit_dir,
              "workflows": {
                "root": {
                  "wf_uuid": "48f759a4-165f-47d6-8c32-571649ce311b",
                  "dag_file_name": "hierarchical-workflow-0.dag",
                  "submit_hostname": "workflow.isi.edu",
                  "submit_dir": "/home/mzalam/hierar/hierarichal-sample-wf/submit/mzalam/pegasus/hierarchical-workflow/run0001",
                  "user": "mzalam",
                  "planner_version": "5.0.5",
                  "wf_name": "hierarchical-workflow",
                  "wf_status": "running",
                  "parent_wf_name": "-",
                  "parent_wf_uuid": "-",
                  "jobs": {
                    "total": 13,
                    "success": 9,
                    "failed": 0,
                    "held": 0,
                    "unsubmitted": 4,
                    "job_details": {}
                  }
                }
              }
            }

        assert analyzer_ouput.as_dict() == expected_output
        
        
    def test_json_mode_traverse_subworkflows(self, mocker, capsys, AnalyzerDatabase):
        submit_dir = os.path.join(directory, "analyzer_samples_dir/hierarchical_wf_success")
        analyze = AnalyzerDatabase(Options(input_dir=submit_dir, json_mode=True, traverse_all=True))
        analyzer_ouput = analyze.analyze_db(None)
         
        expected_output = {
              "root_wf_uuid": "48f759a4-165f-47d6-8c32-571649ce311b",
              "submit_directory": submit_dir,
              "workflows": {
                "root": {
                  "wf_uuid": "48f759a4-165f-47d6-8c32-571649ce311b",
                  "dag_file_name": "hierarchical-workflow-0.dag",
                  "submit_hostname": "workflow.isi.edu",
                  "submit_dir": "/home/mzalam/hierar/hierarichal-sample-wf/submit/mzalam/pegasus/hierarchical-workflow/run0001",
                  "user": "mzalam",
                  "planner_version": "5.0.5",
                  "wf_name": "hierarchical-workflow",
                  "wf_status": "running",
                  "parent_wf_name": "-",
                  "parent_wf_uuid": "-",
                  "jobs": {
                    "total": 13,
                    "success": 9,
                    "failed": 0,
                    "held": 0,
                    "unsubmitted": 4,
                    "job_details": {}
                  }
                },
                "160be601-bda4-45fb-86cd-9cf7a269bb22": {
                  "wf_uuid": "160be601-bda4-45fb-86cd-9cf7a269bb22",
                  "dag_file_name": "inner.dag",
                  "submit_hostname": "workflow.isi.edu",
                  "submit_dir": "/home/mzalam/hierar/hierarichal-sample-wf/submit/mzalam/pegasus/hierarchical-workflow/run0001/00/00/blackdiamond_diamond_subworkflow.000",
                  "user": "mzalam",
                  "planner_version": "5.0.5",
                  "wf_name": "blackdiamond",
                  "wf_status": "success",
                  "parent_wf_name": "hierarchical-workflow",
                  "parent_wf_uuid": "48f759a4-165f-47d6-8c32-571649ce311b",
                  "jobs": {
                    "total": 15,
                    "success": 15,
                    "failed": 0,
                    "held": 0,
                    "unsubmitted": 0,
                    "job_details": {}
                  }
                }
              }
            }

        assert analyzer_ouput.as_dict() == expected_output
        
        
    def test_print_job_instance(self, mocker, capsys, AnalyzerDatabase):
        mocker.patch("Pegasus.analyzer.AnalyzeDB.print_tasks_info")
        mock_tasks = mocker.MagicMock()
        class job_info:
            subwf_dir='some/random/dir/subwf'
            submit_dir='some/random/'
            job_name='job-0'
            state='running'
            site='isi.edu'
            submit_file='temp.sub'
            stdout_file='temp.out'
            stderr_file='temp.err'

        analyze = AnalyzerDatabase(Options(input_dir='some/random/dir'))
        analyze.print_job_instance('jobid-0',job_info,mock_tasks)
        captured = capsys.readouterr()
        captured_output = captured.out.lstrip()
        expected_output = dedent(
            f"""\
                =====================================job-0======================================

                 last state: running
                       site: isi.edu
                submit file: temp.sub
                output file: temp.out
                 error file: temp.err
                 This job contains sub workflows!
                 Please run the command below for more information:
                 __main__ --indent 1  -d some/random/dir/some/random/dir/subwf --top-dir some/random/dir
 

                """
        )
        assert captured_output == expected_output
        
        
        
class TestAnalyzeFiles:
    
    def test_should_create_AnalyzeFiles(self, AnalyzerFiles):
        assert AnalyzerFiles is not None
        

    def test_simple_wf_success(self, mocker, capsys, AnalyzerFiles):
        submit_dir = os.path.join(directory, "analyzer_samples_dir/process_wf_success")
        analyze = AnalyzerFiles(Options(input_dir=submit_dir))
        analyze.analyze_files()
        captured = capsys.readouterr()
        captured_output = captured.out.lstrip()
        expected_output = dedent(
                f"""\
                    ************************************Summary*************************************

                     Submit Directory   : {directory}/analyzer_samples_dir/process_wf_success
                     Total jobs         :      5 (100.00%)
                     # jobs succeeded   :      5 (100.00%)
                     # jobs failed      :      0 (0.00%)
                     # jobs held        :      0 (0.00%)
                     # jobs unsubmitted :      0 (0.00%)
 
                """
            )
        assert captured_output[captured_output.find("*") :] == expected_output.lstrip()
        

    def test_hierarchical_wf_success(self, mocker, capsys, AnalyzerFiles):
        submit_dir = os.path.join(directory, "analyzer_samples_dir/hierarchical_wf_success")
        analyze = AnalyzerFiles(Options(input_dir=submit_dir))
        analyze.analyze_files()
        captured = capsys.readouterr()
        captured_output = captured.out.lstrip()
        expected_output = dedent(
                f"""\
                    ************************************Summary*************************************

                     Submit Directory   : {directory}/analyzer_samples_dir/hierarchical_wf_success
                     Total jobs         :     14 (100.00%)
                     # jobs succeeded   :     10 (71.43%)
                     # jobs failed      :      0 (0.00%)
                     # jobs held        :      0 (0.00%)
                     # jobs unsubmitted :      3 (21.43%)
                     # jobs unknown     :      1 (7.14%)

                    *****************************Unknown jobs' details******************************

                    ===========================stage_out_local_local_2_0============================

                     last state: POST_SCRIPT_STARTED
                           site: submit file not available
                    submit file: {directory}/analyzer_samples_dir/hierarchical_wf_success/stage_out_local_local_2_0.sub
                    output file: {directory}/analyzer_samples_dir/hierarchical_wf_success/stage_out_local_local_2_0.out
                     error file: {directory}/analyzer_samples_dir/hierarchical_wf_success/stage_out_local_local_2_0.err

                """
                )
        assert captured_output[captured_output.find("*") :] == expected_output.lstrip()    

        
    def test_simple_wf_failure(self, mocker, capsys, AnalyzerFiles):
        submit_dir = os.path.join(directory, "analyzer_samples_dir/process_wf_failure")
        analyze = AnalyzerFiles(Options(input_dir=submit_dir))
        
        with pytest.raises(Exception) as err:
            analyze.analyze_files()
        assert "One or more workflows failed" in str(err)
        
        captured = capsys.readouterr()
        captured_output = captured.out.lstrip()
        expected_output = dedent(
            f"""\
                ************************************Summary*************************************

                 Submit Directory   : {submit_dir}
                 Total jobs         :      5 (100.00%)
                 # jobs succeeded   :      1 (20.00%)
                 # jobs failed      :      1 (20.00%)
                 # jobs held        :      0 (0.00%)
                 # jobs unsubmitted :      3 (60.00%)

                ******************************Failed jobs' details******************************

                ==================================ls_ID0000001==================================

                 last state: POST_SCRIPT_FAILURE
                       site: condorpool
                submit file: {submit_dir}/00/00/ls_ID0000001.sub
                output file: {submit_dir}/ls_ID0000001.out
                 error file: {submit_dir}/ls_ID0000001.err
 
                """
        )
        assert captured_output[captured_output.find("*") :] == expected_output.lstrip()

        
    def test_wf_prescript_failure(self, mocker, capsys, AnalyzerFiles):
        submit_dir = os.path.join(directory, "analyzer_samples_dir/hierarchical_wf_failure")
        analyze = AnalyzerFiles(Options(input_dir=submit_dir,
                                        print_invocation=True,
                                        print_pre_script=True))
        
        analyze.analyze_files()        
        captured = capsys.readouterr()
        captured_output = captured.out.lstrip()
        assert "To re-run this job, use:" in captured_output


    def test_invoke_monitord(self, mocker, AnalyzerFiles):
        temp_dir = tempfile.mkdtemp('sample_temp_dir')
        dagman_out_file = os.path.join(directory, "analyzer_samples_dir/process_wf_failure/process-0.dag.dagman.out")
        analyze = AnalyzerFiles(Options(run_monitord=True))
        analyze.invoke_monitord(dagman_out_file, temp_dir)
    
        assert len(os.listdir(temp_dir)) == 5
        assert len(glob.glob(temp_dir+"/*jobstate.log")) == 1
        assert len(glob.glob(temp_dir+"/*monitord.done")) == 1
        assert len(glob.glob(temp_dir+"/*monitord.started")) == 1
        assert len(glob.glob(temp_dir+"/*monitord.info")) == 1
        assert len(glob.glob(temp_dir+"/monitord.subwf")) == 1

        
    def test_dump_file(mocker, capsys, AnalyzerFiles):
        file_path = os.path.join(directory,"analyzer_samples_dir/process_wf_success/braindump.yml")
        analyze = AnalyzerFiles(Options())
        analyze.dump_file(file_path)
        captured = capsys.readouterr()
        captured_output = captured.out.lstrip()
        assert 'user: "mzalam"' in captured_output


    def test_json_mode(self, mocker, capsys, AnalyzerFiles):
        submit_dir = os.path.join(directory, "analyzer_samples_dir/process_wf_failure")
        analyze = AnalyzerFiles(Options(input_dir=submit_dir, json_mode=True))
        analyzer_ouput = analyze.analyze_files()
         
        expected_output = {
                  "root_wf_uuid": "f84f05fc-a8d0-42b5-bac5-52d6f41a77e3",
                  "submit_directory": "/home/mzalam/processwf/process-workflow/submit/mzalam/pegasus/process/run0001",
                  "workflows": {
                    "root": {
                      "wf_uuid": "f84f05fc-a8d0-42b5-bac5-52d6f41a77e3",
                      "dag_file_name": "process-0.dag",
                      "submit_hostname": "workflow.isi.edu",
                      "submit_dir": "/home/mzalam/processwf/process-workflow/submit/mzalam/pegasus/process/run0001",
                      "user": "mzalam",
                      "planner_version": "5.0.5",
                      "wf_name": "process",
                      "wf_status": "failure",
                      "parent_wf_name": "-",
                      "parent_wf_uuid": "-",
                      "jobs": {
                        "total": 5,
                        "success": 1,
                        "failed": 1,
                        "held": 0,
                        "unsubmitted": 3,
                        "job_details": {
                          "failed_job_details": {
                            "ls_ID0000001": {
                              "job_name": "ls_ID0000001",
                              "state": "POST_SCRIPT_FAILURE",
                              "site": "",
                              "submit_file": "/home/mzalam/pegasus_master/pegasus/packages/pegasus-python/test/cli/analyzer_samples_dir/process_wf_failure/00/00/ls_ID0000001.sub",
                              "stdout_file": "",
                              "stderr_file": "",
                              "executable": "",
                              "argv": "",
                              "tasks": {}
                            }
                          }
                        }
                      }
                    }
                  }
                }
        assert analyzer_ouput.as_dict() == expected_output
        

        
class TestDebugWF:
    
    def test_should_create_AnalyzeDB(self, AnalyzerDebug):
        assert AnalyzerDebug is not None
        
    def test_debug_mode_no_debug_dir(mocker, capsys, AnalyzerDebug):
        submit_dir = os.path.join(directory, "analyzer_samples_dir/process_wf_failure")
        job = os.path.join(submit_dir, "00/00/ls_ID0000001.sub")
        debug = AnalyzerDebug(Options(debug_job=job, input_dir=submit_dir))
        debug.debug_workflow()
        captured = capsys.readouterr()
        captured_output = captured.out.lstrip()
        assert "finished generating job debug script!" in captured_output

    def test_debug_mode_with_debug_dir(mocker, capsys, AnalyzerDebug):
        submit_dir = os.path.join(directory, "analyzer_samples_dir/process_wf_failure")
        job = os.path.join(submit_dir, "00/00/ls_ID0000001.sub")
        temp_dir = tempfile.mkdtemp('sample_temp_dir')
        debug = AnalyzerDebug(Options(debug_job=job, input_dir=submit_dir, debug_dir=temp_dir))
        debug.debug_workflow()
        captured = capsys.readouterr()
        captured_output = captured.out.lstrip()
        assert "finished generating job debug script!" in captured_output

        
