import os
import sys
import tempfile

import pytest

from Pegasus.client.analyzer import *
from Pegasus.db import *

directory = os.path.dirname(__file__)
pegasus_version = "5.0.1"
prog_base = os.path.split(sys.argv[0])[1].replace(".py", "")


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
        mocker.patch("Pegasus.client.analyzer.BaseAnalyze.backticks")
        BaseAnalyzer.check_for_wf_start(
            Options(input_dir="temp"), Counts(0, 0, 0, 0, 0, 0, [], [])
        )
        captured = capsys.readouterr()
        captured_output = captured.out.lstrip()
        expected_output = "*********************** Looks like workflow did not start***********************\n\n"
        assert expected_output in captured_output

    def test_addon(self, mocker, BaseAnalyzer):
        opts = Options(
            recurse_mode=True,
            quiet_mode=True,
            summary_mode=True,
            use_files=True,
            indent_length=2,
        )

        assert (
            BaseAnalyzer.addon(opts)
            == "--recurse --quiet --summary --files --indent 3 "
        )

    def test_parse_submit_file(self, mocker, capsys, BaseAnalyzer):
        submit_file = os.path.join(
            directory,
            "analyzer_samples_dir/sample_wf_held/Drug-Combination-Therapy-0.dag.condor.sub",
        )
        job = Job("job-0", "running")
        job.is_subdax = True
        job.sub_file = submit_file
        job.retries = 3
        opts = Options(
            debug_mode=True,
            input_dir="pegasus/hierarchical-workflow/run0005",
            workflow_base_dir="/home/mzalam/028-dynamic-hierarchy",
        )
        BaseAnalyze.parse_submit_file(job, opts)
        assert job.transfer_input_files == "rrr"

    def test_parse_submit_file_open_error(self, mocker, capsys, BaseAnalyzer):
        submit_file = os.path.join(
            directory,
            "analyzer_samples_dir/sample_wf_held/Drug-Combination-Therapy-0.dag.condor.sub",
        )
        job = Job("job-0", "running")
        job.sub_file = submit_file
        mocker.patch("builtins.open", side_effect=Exception("mocked error"))
        opts = Options(
            debug_mode=True,
            input_dir="pegasus/hierarchical-workflow/run0005",
            workflow_base_dir="/home/mzalam/028-dynamic-hierarchy",
        )
        with pytest.raises(Exception) as err:
            BaseAnalyze.parse_submit_file(job, opts)
        assert f"error opening submit file: {submit_file}" in str(err)

    def test_parse_submit_file_subdag_job(self, mocker, capsys, BaseAnalyzer):
        job = Job("job-0", "running")
        job.is_subdag = True
        opts = Options(
            debug_mode=True,
            input_dir="pegasus/hierarchical-workflow/run0005",
            workflow_base_dir="/home/mzalam/028-dynamic-hierarchy",
        )

        assert BaseAnalyze.parse_submit_file(job, opts) == None

    def test_parse_submit_file_no_retries(self, mocker, capsys, BaseAnalyzer):
        submit_file = os.path.join(
            directory,
            "analyzer_samples_dir/sample_wf_held/Drug-Combination-Therapy-0.dag.condor.sub",
        )
        job = Job("job-0", "running")
        job.is_subdax = True
        job.sub_file = submit_file
        job.retries = None
        opts = Options(
            debug_mode=True,
            input_dir="pegasus/hierarchical-workflow/run0005",
            workflow_base_dir="/home/mzalam/028-dynamic-hierarchy",
        )

        BaseAnalyze.parse_submit_file(job, opts)
        captured = capsys.readouterr()
        captured_output = captured.out.lstrip()
        expected_output = "sub-workflow retry counter not initialized... continuing..."
        assert expected_output in captured_output


class TestAnalyzerOutput:
    @pytest.fixture
    def get_output(self):
        submit_dir = os.path.join(
            directory, "analyzer_samples_dir/hierarchical_wf_failure"
        )
        az = AnalyzeDB(Options(input_dir=submit_dir, traverse_all=True))
        out = az.analyze_db(None)
        return out.workflows

    def test_should_create_BaseAnalyze(self, Output):
        assert Output is not None

    def test_get_failed_workflows(self, Output):
        analyzer_output = Output()
        analyzer_output.workflows = {"wf-0": Workflow(wf_status="failure")}
        expected = {"wf-0": Workflow(wf_status="failure")}
        assert analyzer_output.get_failed_workflows()["wf-0"].wf_status == "failure"

    def test_get_all_jobs(self, Output):
        submit_dir = os.path.join(directory, "analyzer_samples_dir/sample_wf_held")
        az = AnalyzeDB(Options(input_dir=submit_dir))
        out = az.analyze_db(None)
        analyzer_output = Output()
        analyzer_output.workflows = out.workflows

        assert "failed_jobs_details" in analyzer_output.get_all_jobs()
        assert "held_jobs_details" in analyzer_output.get_all_jobs()
        assert len(analyzer_output.get_all_jobs()) == 2

    def test_get_jobs_counts(self, Output, get_output):
        analyzer_output = Output()
        analyzer_output.workflows = get_output
        output_counts = analyzer_output.get_jobs_counts()

        assert output_counts.unsubmitted == 6

    def test_get_failed_jobs(self, Output, get_output):
        analyzer_output = Output()
        analyzer_output.workflows = get_output
        failed_jobs = analyzer_output.get_failed_jobs()

        assert "pegasus-plan_diamond_subworkflow" in failed_jobs

    def test_get_held_jobs(self, Output):
        submit_dir = os.path.join(directory, "analyzer_samples_dir/sample_wf_held")
        az = AnalyzeDB(Options(input_dir=submit_dir))
        out = az.analyze_db(None)
        analyzer_output = Output()
        analyzer_output.workflows = out.workflows
        held_jobs = analyzer_output.get_held_jobs()

        assert "chebi_drug_loader_ID0000001" in held_jobs


class TestAnalyzeDB:
    def test_should_create_AnalyzeDB(self, AnalyzerDatabase):
        assert AnalyzerDatabase is not None

    def test_analyze_db_error(self, mocker, capsys, AnalyzerDatabase):
        mocker.patch(
            "Pegasus.db.connection.url_by_submitdir", side_effect=AnalyzerError
        )
        submit_dir = os.path.join(directory, "analyzer_samples_dir/process_wf_success")
        analyze = AnalyzerDatabase(Options(input_dir=submit_dir))

        with pytest.raises(AnalyzerError) as err:
            analyze.analyze_db(None)
            assert "Unable to connect to database for workflow" in str(err.value)

    def test_analyze_db_no_url(self, mocker, capsys, AnalyzerDatabase):
        mocker.patch("Pegasus.db.connection.url_by_submitdir", return_value=None)
        submit_dir = os.path.join(directory, "analyzer_samples_dir/process_wf_success")
        analyze = AnalyzerDatabase(Options(input_dir=submit_dir))

        with pytest.raises(ValueError) as err:
            analyze.analyze_db(None)
        assert "Database URL is required" in str(err)

    def test_analyze_db_no_wf_id(self, mocker, capsys, AnalyzerDatabase):
        mocker.patch(
            "Pegasus.db.workflow.stampede_statistics.StampedeStatistics.get_workflow_details"
        )
        submit_dir = os.path.join(directory, "analyzer_samples_dir/process_wf_success")

        analyze = AnalyzerDatabase(Options(input_dir=submit_dir))
        with pytest.raises(KeyError) as err:
            analyze.analyze_db(None)
        captured = capsys.readouterr()
        captured_error = captured.err.lstrip()
        expected_error = "Unable to determine the database id for workflow with uuid f00c0056-abd4-4e8e-b793-24eb760dda1f"
        assert expected_error in captured_error

    def test_simple_wf_success(self, mocker, capsys, AnalyzerDatabase):
        submit_dir = os.path.join(directory, "analyzer_samples_dir/process_wf_success")
        analyze = AnalyzerDatabase(Options(input_dir=submit_dir))
        analyzer_ouput = analyze.analyze_db(None)

        expected_output = {
            "root_wf_uuid": "f00c0056-abd4-4e8e-b793-24eb760dda1f",
            "submit_directory": submit_dir,
            "workflows": {
                "root": {
                    "wf_uuid": "f00c0056-abd4-4e8e-b793-24eb760dda1f",
                    "dag_file_name": "process-0.dag",
                    "submit_hostname": "workflow.isi.edu",
                    "submit_dir": "/home/mzalam/processwf/submit/mzalam/pegasus/process/run0001",
                    "user": "mzalam",
                    "planner_version": "5.0.5",
                    "wf_name": "process",
                    "wf_status": "success",
                    "parent_wf_name": "-",
                    "parent_wf_uuid": "-",
                    "jobs": {
                        "total": 5,
                        "success": 5,
                        "failed": 0,
                        "held": 0,
                        "unsubmitted": 0,
                        "job_details": {},
                    },
                }
            },
        }

        assert analyzer_ouput.as_dict() == expected_output

    def test_simple_wf_failure(self, mocker, capsys, AnalyzerDatabase):
        submit_dir = os.path.join(directory, "analyzer_samples_dir/process_wf_failure")
        analyze = AnalyzerDatabase(Options(input_dir=submit_dir))
        analyzer_ouput = analyze.analyze_db(None)

        expected_output = {
            "root_wf_uuid": "f84f05fc-a8d0-42b5-bac5-52d6f41a77e3",
            "submit_directory": submit_dir,
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
                            "failed_jobs_details": {
                                "ls_ID0000001": {
                                    "job_name": "ls_ID0000001",
                                    "state": "POST_SCRIPT_FAILED",
                                    "site": "condorpool",
                                    "hostname": "workflow.isi.edu",
                                    "work_dir": "/home/mzalam/wf/condor/local/execute/dir_148537",
                                    "submit_file": "00/00/ls_ID0000001.sub",
                                    "stdout_file": "00/00/ls_ID0000001.out.001",
                                    "stderr_file": "00/00/ls_ID0000001.err.001",
                                    "executable": "/home/mzalam/processwf/process-workflow/submit/mzalam/pegasus/process/run0001/00/00/ls_ID0000001.sh",
                                    "argv": "-",
                                    "pre_executable": "-",
                                    "pre_argv": "-",
                                    "submit_dir": "/home/mzalam/processwf/process-workflow/submit/mzalam/pegasus/process/run0001",
                                    "subwf_dir": "-",
                                    "stdout_text": "#@ 1 stderr\n /bin/ls: invalid option -- 'z'\nTry '/bin/ls --help' for more information.",
                                    "stderr_text": "2023-03-03 23:11:14: PegasusLite: version 5.0.5\n2023-03-03 23:11:14: Executing on host workflow.isi.edu IP=128.9.46.53\n\n########################[Pegasus Lite] Setting up workdir ########################\n2023-03-03 23:11:14: Not creating a new work directory as it is already set to /home/mzalam/wf/condor/local/execute/dir_148537\n\n##############[Pegasus Lite] Figuring out the worker package to use ##############\n2023-03-03 23:11:14: The job contained a Pegasus worker package\ntar: write error\n\n##################### Checking file integrity for input files #####################\n\n######################[Pegasus Lite] Executing the user task ######################\n2023-03-03 23:11:15: User task failed with exitcode 2\nPegasusLite: exitcode 2",
                                    "tasks": {
                                        "ID0000001": {
                                            "task_submit_seq": 1,
                                            "exitcode": 2,
                                            "executable": "/usr/bin/ls",
                                            "arguments": "-",
                                            "transformation": "ls",
                                            "abs_task_id": "ID0000001",
                                        }
                                    },
                                }
                            }
                        },
                    },
                }
            },
        }

        assert analyzer_ouput.as_dict() == expected_output

    def test_sample_wf_held(self, mocker, capsys, AnalyzerDatabase):
        submit_dir = os.path.join(directory, "analyzer_samples_dir/sample_wf_held")
        analyze = AnalyzerDatabase(Options(input_dir=submit_dir))
        analyze = AnalyzerDatabase(Options(input_dir=submit_dir))
        analyzer_ouput = analyze.analyze_db(None)

        expected_output = {
            "chebi_drug_loader_ID0000001": {
                "submit_file": "chebi_drug_loader_ID0000001.sub",
                "last_job_instance_id": 3,
                "reason": " Transfer output files failure at execution point slot1@workflow.isi.edu while sending files to access point workflow. Details: reading from file /home/mzalam/wf/condor/local/execute/dir_761020/chebi_dict.pickle: (errno 2) No such file or directory",
            }
        }

        assert (
            analyzer_ouput.as_dict()["workflows"]["root"]["jobs"]["job_details"][
                "held_jobs_details"
            ]
            == expected_output
        )

    def test_hierarchical_wf_traverse_all(self, mocker, capsys, AnalyzerDatabase):
        submit_dir = os.path.join(
            directory, "analyzer_samples_dir/hierarchical_wf_success"
        )

        analyze = AnalyzerDatabase(Options(input_dir=submit_dir, traverse_all=True))
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
                        "job_details": {},
                    },
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
                        "job_details": {},
                    },
                },
            },
        }

        assert analyzer_ouput.as_dict() == expected_output

    def test_hierarchical_wf_failure(self, mocker, capsys, AnalyzerDatabase):
        submit_dir = os.path.join(
            directory, "analyzer_samples_dir/hierarchical_wf_failure"
        )
        analyze = AnalyzerDatabase(Options(input_dir=submit_dir))
        analyzer_ouput = analyze.analyze_db(None)
        analyzer_ouput_failed_jobs = analyzer_ouput.as_dict()["workflows"]["root"][
            "jobs"
        ]["job_details"]["failed_jobs_details"]

        expected_argv = " -p 0  -f -l . -Notification never -Debug 3 -Lockfile inner.dag.lock -Dag inner.dag -AllowVersionMismatch  -AutoRescue 1 -DoRescueFrom 0 "
        expected_pre_executable = "00/00/pegasus-plan_diamond_subworkflow.pre.sh"

        assert (
            analyzer_ouput_failed_jobs["pegasus-plan_diamond_subworkflow"]["argv"]
            == expected_argv
        )
        assert (
            analyzer_ouput_failed_jobs["pegasus-plan_diamond_subworkflow"][
                "pre_executable"
            ]
            == expected_pre_executable
        )

    def test_analyze_db_for_wf_workflow_error(self, mocker, capsys, AnalyzerDatabase):
        mocker.patch(
            "Pegasus.client.analyzer.AnalyzeDB.get_workflow_status",
            return_value=WORKFLOW_STATUS.FAILURE,
        )
        mock_wf = mocker.MagicMock()
        mock_wf_stats = mocker.MagicMock()
        mock_wf_stats.configure_mock(
            **{"get_failing_jobs.return_value": (1, 2, 3),}
        )

        analyze = AnalyzerDatabase(Options(summary_mode=True))
        with pytest.raises(Exception) as err:
            analyze.analyze_db_for_wf(mock_wf_stats, "uuid-0", "submit_dir-0", mock_wf)
            assert "Workflow failed" in str(err)
            assert "uuid-0" in str(err) and "submit_dir-0" in str(err)

    def DISABLED_test_analyze_db_for_wf_failing_jobs(
        self, mocker, capsys, AnalyzerDatabase
    ):
        mocker.patch("Pegasus.client.analyzer.AnalyzeDB.get_job_details")
        mock_wf = mocker.MagicMock()

        class count:
            succeeded = None
            failed = None

        class failing:
            def _asdict():
                return {"job_instance_id": "job-id-0"}

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
                return ["job-instance-0"]

        analyze = AnalyzerDatabase(Options(input_dir="random/submit"))
        analyze.analyze_db_for_wf(wf_stats, "uuid-0", "submit_dir-0", mock_wf)

        AnalyzeDB.get_job_details.assert_called_once_with("job-instance-0", None)

    def test_json_mode(self, mocker, capsys, AnalyzerDatabase):
        submit_dir = os.path.join(
            directory, "analyzer_samples_dir/hierarchical_wf_success"
        )
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
                        "job_details": {},
                    },
                }
            },
        }

        assert analyzer_ouput.as_dict() == expected_output

    def test_json_mode_traverse_subworkflows(self, mocker, capsys, AnalyzerDatabase):
        submit_dir = os.path.join(
            directory, "analyzer_samples_dir/hierarchical_wf_success"
        )
        analyze = AnalyzerDatabase(
            Options(input_dir=submit_dir, json_mode=True, traverse_all=True)
        )
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
                        "job_details": {},
                    },
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
                        "job_details": {},
                    },
                },
            },
        }

        assert analyzer_ouput.as_dict() == expected_output


class TestAnalyzeFiles:
    def test_should_create_AnalyzeFiles(self, AnalyzerFiles):
        assert AnalyzerFiles is not None

    def test_simple_wf_success(self, mocker, capsys, AnalyzerFiles):
        submit_dir = os.path.join(directory, "analyzer_samples_dir/process_wf_success")
        analyze = AnalyzerFiles(Options(input_dir=submit_dir))
        analyzer_ouput = analyze.analyze_files()

        expected_output = {
            "root_wf_uuid": "f00c0056-abd4-4e8e-b793-24eb760dda1f",
            "submit_directory": "/home/mzalam/processwf/submit/mzalam/pegasus/process/run0001",
            "workflows": {
                "root": {
                    "wf_uuid": "f00c0056-abd4-4e8e-b793-24eb760dda1f",
                    "dag_file_name": "process-0.dag",
                    "submit_hostname": "workflow.isi.edu",
                    "submit_dir": "/home/mzalam/processwf/submit/mzalam/pegasus/process/run0001",
                    "user": "mzalam",
                    "planner_version": "5.0.5",
                    "wf_name": "process",
                    "wf_status": "success",
                    "parent_wf_name": "-",
                    "parent_wf_uuid": "-",
                    "jobs": {
                        "total": 5,
                        "success": 5,
                        "failed": 0,
                        "held": 0,
                        "unsubmitted": 0,
                        "job_details": {},
                    },
                }
            },
        }

        assert analyzer_ouput.as_dict() == expected_output

    def test_hierarchical_wf_success(self, mocker, capsys, AnalyzerFiles):
        submit_dir = os.path.join(
            directory, "analyzer_samples_dir/hierarchical_wf_success"
        )
        analyze = AnalyzerFiles(Options(input_dir=submit_dir))
        analyzer_ouput = analyze.analyze_files()
        expected_state = "POST_SCRIPT_STARTED"
        out_job_details = analyzer_ouput.as_dict()["workflows"]["root"]["jobs"][
            "job_details"
        ]
        output_state = out_job_details["unknown_jobs_details"][
            "stage_out_local_local_2_0"
        ]["state"]

        assert output_state == expected_state

    def test_simple_wf_failure(self, mocker, capsys, AnalyzerFiles):
        submit_dir = os.path.join(directory, "analyzer_samples_dir/process_wf_failure")
        analyze = AnalyzerFiles(Options(input_dir=submit_dir))
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
                            "failed_jobs_details": {
                                "ls_ID0000001": {
                                    "job_name": "ls_ID0000001",
                                    "state": "POST_SCRIPT_FAILURE",
                                    "site": "condorpool",
                                    "hostname": "workflow.isi.edu",
                                    "work_dir": "/home/mzalam/wf/condor/local/execute/dir_148537",
                                    "submit_file": submit_dir
                                    + "/00/00/ls_ID0000001.sub",
                                    "stdout_file": submit_dir
                                    + "/00/00/ls_ID0000001.out",
                                    "stderr_file": submit_dir
                                    + "/00/00/ls_ID0000001.err",
                                    "executable": "/home/mzalam/processwf/process-workflow/submit/mzalam/pegasus/process/run0001/00/00/ls_ID0000001.sh",
                                    "argv": "",
                                    "pre_executable": "",
                                    "pre_argv": None,
                                    "submit_dir": None,
                                    "subwf_dir": "-",
                                    "stdout_text": "-",
                                    "stderr_text": "/bin/ls: invalid option -- 'z'\nTry '/bin/ls --help' for more information.\n",
                                    "tasks": {
                                        1: {
                                            "task_submit_seq": 1,
                                            "exitcode": 2,
                                            "executable": "/usr/bin/ls",
                                            "arguments": "-",
                                            "transformation": "ls",
                                            "abs_task_id": "ID0000001",
                                        }
                                    },
                                }
                            }
                        },
                    },
                }
            },
        }
        assert analyzer_ouput.as_dict() == expected_output

    @pytest.mark.parametrize(
        "out_dir, expected_output",
        [
            (
                None,
                "User must specify directory for new monitord logs with the --output-dir option, exiting...",
            ),
        ],
    )
    def test_analyze_files_monitord_errors(
        self, mocker, capsys, AnalyzerFiles, out_dir, expected_output
    ):
        submit_dir = os.path.join(directory, "analyzer_samples_dir/process_wf_failure")
        mocker.patch("Pegasus.client.analyzer.AnalyzeFiles.invoke_monitord")
        mocker.patch(
            "Pegasus.client.analyzer.AnalyzeFiles.get_jsdl_filename",
            return_value="jobstate.log",
        )
        mocker.patch("os.access", return_value=False)

        analyze = AnalyzerFiles(
            Options(run_monitord=True, input_dir=submit_dir, output_dir=out_dir)
        )
        with pytest.raises(Exception) as err:
            analyze.analyze_files()
        assert expected_output in str(err)

    def test_hierarchical_wf_failure(self, mocker, capsys, AnalyzerFiles):
        submit_dir = os.path.join(
            directory, "analyzer_samples_dir/hierarchical_wf_failure"
        )
        analyze = AnalyzerFiles(Options(input_dir=submit_dir))
        analyzer_ouput = analyze.analyze_files()
        expected_state = "PRE_SCRIPT_FAILURE"
        out_job_details = analyzer_ouput.as_dict()["workflows"]["root"]["jobs"][
            "job_details"
        ]
        output_state = out_job_details["unknown_jobs_details"][
            "pegasus-plan_diamond_subworkflow"
        ]["state"]

        assert output_state == expected_state

    def DISABLED_test_invoke_monitord(self, mocker, AnalyzerFiles):
        mocker.patch("Pegasus.client.analyzer.BaseAnalyze.backticks")
        temp_dir = tempfile.mkdtemp("sample_temp_dir")
        dagman_out_file = os.path.join(
            directory,
            "analyzer_samples_dir/process_wf_failure/process-0.dag.dagman.out",
        )

        cmd = (
            f"pegasus-monitord -r --no-events --output-dir {temp_dir} {dagman_out_file}"
        )
        analyze = AnalyzerFiles(Options(run_monitord=True))
        analyze.invoke_monitord(dagman_out_file, temp_dir)
        BaseAnalyze.backticks.assert_called_once_with(cmd)

    def DISABLED_test_invoke_monitord_error(self, mocker, AnalyzerFiles):
        analyze = AnalyzerFiles(Options(run_monitord=True))
        mocker.patch(
            "Pegasus.client.analyzer.BaseAnalyze.backticks", side_effect=AnalyzerError
        )
        with pytest.raises(Exception) as err:
            analyze.invoke_monitord("", None)
        assert "could not invoke monitord, exiting..." == str(err.value)
        assert AnalyzerError == err.type

    def test_find_file_error(self, mocker, AnalyzerFiles):
        analyze = AnalyzerFiles(Options())
        with pytest.raises(Exception) as err:
            analyze.find_file("/random/dir", ".txt")
        assert "cannot read directory: /random/dir" == str(err.value)
        assert AnalyzerError == err.type

    def test_find_file_not_found(self, mocker, AnalyzerFiles):
        analyze = AnalyzerFiles(Options())
        submit_dir = os.path.join(directory, "analyzer_samples_dir/process_wf_failure")
        with pytest.raises(Exception) as err:
            analyze.find_file(submit_dir, ".pkl")
        assert f"could not find any .pkl file in {submit_dir}" == str(err.value)
        assert AnalyzerError == err.type

    def test_get_jsdl_filename(self, mocker, AnalyzerFiles):
        input_dir = os.path.join(directory, "analyzer_samples_dir/process_wf_failure")
        analyze = AnalyzerFiles(Options())
        assert "jobstate.log" in analyze.get_jsdl_filename(input_dir)

    def test_get_jsdl_filename_no_braindump(self, mocker, AnalyzerFiles):
        input_dir = os.path.join(directory, "analyzer_samples_dir/process_wf_failure")
        mocker.patch("Pegasus.tools.utils.slurp_braindb", side_effect=AnalyzerError)
        analyze = AnalyzerFiles(Options())

        with pytest.raises(Exception) as err:
            analyze.get_jsdl_filename(input_dir)

        assert "cannot read braindump.txt file... exiting..." == str(err.value)
        assert AnalyzerError == err.type

    def test_get_jsdl_filename_no_wf_uuid(self, mocker, AnalyzerFiles):
        input_dir = os.path.join(directory, "analyzer_samples_dir/process_wf_failure")
        mocker.patch("Pegasus.tools.utils.slurp_braindb", return_value={"job": "a"})
        analyze = AnalyzerFiles(Options())

        with pytest.raises(Exception) as err:
            analyze.get_jsdl_filename(input_dir)

        assert "braindump.txt does not contain wf_uuid... exiting..." == str(err.value)
        assert AnalyzerError == err.type

    def test_parse_dag_file(self, mocker, capsys, AnalyzerFiles):
        dag_file = os.path.join(directory, "analyzer_samples_dir/sample.dag")
        analyze = AnalyzerFiles(Options(input_dir="/random/dir"))
        analyze.parse_dag_file(dag_file)
        captured = capsys.readouterr()
        captured_output = captured.out.lstrip()
        expected_output = [
            "confused parsing dag line: SUBDAG EXTERNAL job1 x y",
            "job appears twice in dag file: job0",
            "confused parsing dag line: JOB job3",
            "job appears twice in dag file: job2",
            "couldn't find job: job4 for PRE SCRIPT line in dag file",
            "confused parsing dag line: SCRIPT PRE",
            "couldn't find job: job5 for VARS line in dag file",
        ]
        for each_line in expected_output:
            assert each_line in captured_output

        jobs = ["job0", "job2", "pegasus-plan-job"]
        for each_job in jobs:
            assert each_job in analyze.jobs

    def test_parse_dag_file_error(self, mocker, capsys, AnalyzerFiles):
        analyze = AnalyzerFiles(Options(input_dir="/random/dir"))
        with pytest.raises(AnalyzerError) as err:
            analyze.parse_dag_file("dag_file")
        assert "could not open dag file dag_file: exiting..." in str(err)

    def test_parse_jobstate_log(self, mocker, capsys, AnalyzerFiles):
        analyze = AnalyzerFiles(Options(input_dir="/random/dir"))
        with pytest.raises(AnalyzerError) as err:
            analyze.parse_jobstate_log("jobstate_file")
        assert "could not open file jobstate_file: exiting..." in str(err)

    def test_add_job(self, mocker, AnalyzerFiles):
        analyze = AnalyzerFiles(Options())
        analyze.jobs = {"job-0": Job("job-0", "running")}

        assert analyze.add_job("job-0") == None

    def test_update_job_state_error(self, mocker, capsys, AnalyzerFiles):
        analyze = AnalyzerFiles(Options())
        analyze.jobs = {"job-0": Job("job-0", "running")}
        analyze.update_job_state("job-1")

        captured = capsys.readouterr()
        captured_error = captured.err.lstrip()
        assert "could not find job job-1" in captured_error

    def test_update_job_condor_info(self, mocker, capsys, AnalyzerFiles):
        analyze = AnalyzerFiles(Options())
        analyze.jobs = {"job-0": Job("job-0", "running")}

        assert analyze.update_job_condor_info("job-0") == None

    def test_update_job_condor_info_error(self, mocker, capsys, AnalyzerFiles):
        analyze = AnalyzerFiles(Options())
        analyze.jobs = {"job-0": Job("job-0", "running")}
        analyze.update_job_condor_info("job-1")

        captured = capsys.readouterr()
        captured_error = captured.err.lstrip()
        assert "could not find job job-1" in captured_error

    def test_dump_file(self, mocker, capsys, AnalyzerFiles):
        file_path = os.path.join(
            directory, "analyzer_samples_dir/process_wf_success/braindump.yml"
        )
        analyze = AnalyzerFiles(Options())
        data = analyze.dump_file(file_path)
        assert 'user: "mzalam"' in data


class TestDebugWF:
    def test_should_create_AnalyzeDB(self, AnalyzerDebug):
        assert AnalyzerDebug is not None

    def test_debug_workflow_no_access(self, mocker, AnalyzerDebug):
        debug = AnalyzerDebug(Options(debug_job="job-0"))

        with pytest.raises(AnalyzerError) as err:
            debug.debug_workflow()
        assert "cannot access job submit file: job-0.sub" in str(err)

    def test_debug_workflow_no_tempdir(self, mocker, AnalyzerDebug):
        submit_dir = os.path.join(directory, "analyzer_samples_dir/process_wf_failure")
        job = os.path.join(submit_dir, "00/00/ls_ID0000001.sub")
        debug = AnalyzerDebug(Options(debug_job=job))
        mocker.patch("tempfile.mkdtemp", side_effect=AnalyzerError)

        with pytest.raises(AnalyzerError) as err:
            debug.debug_workflow()
        assert "could not create temporary directory!" in str(err)

    def test_debug_workflow_create_debug_dir_fail(self, mocker, capsys, AnalyzerDebug):
        submit_dir = os.path.join(directory, "analyzer_samples_dir/process_wf_failure")
        job = os.path.join(submit_dir, "00/00/ls_ID0000001.sub")
        debug = AnalyzerDebug(Options(debug_job=job, debug_dir="a.txt"))
        mocker.patch("os.mkdir", side_effect=AnalyzerError)

        with pytest.raises(AnalyzerError) as err:
            debug.debug_workflow()
        captured = capsys.readouterr()
        captured_error = captured.err.lstrip()
        assert "cannot create debug directory:" in captured_error

    def test_debug_workflow_wf_type(mocker, capsys, AnalyzerDebug):
        submit_dir = os.path.join(directory, "analyzer_samples_dir/process_wf_failure")
        job = os.path.join(submit_dir, "00/00/ls_ID0000001.sub")
        debug = AnalyzerDebug(
            Options(debug_job=job, workflow_type="OTHER", input_dir=submit_dir)
        )

        with pytest.raises(AnalyzerError) as err:
            debug.debug_workflow()
        assert "workflow type OTHER not supported!" in str(err)

    def test_debug_mode_no_debug_dir(mocker, capsys, AnalyzerDebug):
        submit_dir = os.path.join(directory, "analyzer_samples_dir/process_wf_failure")
        job = os.path.join(submit_dir, "00/00/ls_ID0000001.sub")
        debug = AnalyzerDebug(
            Options(debug_job=job, workflow_type="CONDOR", input_dir=submit_dir)
        )
        debug.debug_workflow()

        captured = capsys.readouterr()
        captured_output = captured.out.lstrip()
        assert "finished generating job debug script!" in captured_output

    def test_debug_mode_with_debug_dir(mocker, capsys, AnalyzerDebug):
        submit_dir = os.path.join(directory, "analyzer_samples_dir/process_wf_failure")
        job = os.path.join(submit_dir, "00/00/ls_ID0000001.sub")
        temp_dir = tempfile.mkdtemp("sample_temp_dir")
        debug = AnalyzerDebug(
            Options(debug_job=job, input_dir=submit_dir, debug_dir=temp_dir)
        )
        debug.debug_workflow()

        captured = capsys.readouterr()
        captured_output = captured.out.lstrip()
        assert "finished generating job debug script!" in captured_output
