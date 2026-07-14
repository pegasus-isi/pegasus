import getpass
import os

import pytest
from flask import g


class NoAuthFlaskTestCase:
    @pytest.fixture(autouse=True)
    def init(self, app):
        app.config["AUTHENTICATION"] = "NoAuthentication"
        app.config["PROCESS_SWITCHING"] = False

        self.user = getpass.getuser()

    @staticmethod
    def pre_callable():
        directory = os.path.dirname(__file__)
        db = os.path.join(
            directory, "../../resources/monitoring-db/", "monitoring-rest-api-master.db"
        )
        g.master_db_url = f"sqlite:///{db}"
        g.stampede_db_url = f"sqlite:///{db}"


class TestMasterWorkflowQueries(NoAuthFlaskTestCase):
    def test_get_root_workflows(self, cli):
        rv = cli.get_context(
            f"/api/v1/user/{self.user}/root?pretty-print=true",
            pre_callable=self.pre_callable,
        )

        assert rv.status_code == 200
        assert rv.content_type.lower() == "application/json"

        root_workflows = rv.json

        assert len(root_workflows["records"]) == 5
        assert (
            len(root_workflows["records"]) == root_workflows["_meta"]["records_total"]
        )

        assert (
            root_workflows["_meta"]["records_total"]
            == root_workflows["_meta"]["records_filtered"]
        )
        assert "workflow_state" in root_workflows["records"][0]

    def test_query_with_prefix(self, cli):
        rv = cli.get_context(
            f"/api/v1/user/{self.user}/root?pretty-print=false&query=r.wf_id == 1",
            pre_callable=self.pre_callable,
        )

        assert rv.status_code == 200
        assert rv.content_type.lower() == "application/json"

        root_workflows = rv.json

        assert root_workflows["records"][0]["wf_id"] == 1

    def test_query_without_prefix(self, cli):
        rv = cli.get_context(
            f"/api/v1/user/{self.user}/root?query=submit_hostname like '%.edu'&order=wf_id",
            pre_callable=self.pre_callable,
        )

        assert rv.status_code == 400
        assert rv.content_type.lower() == "application/json"

    def test_complex_query(self, cli):
        rv = cli.get_context(
            f"/api/v1/user/{self.user}/root?query=r.wf_id == 1 or (r.wf_id.like(2) and r.grid_dn is None)&order=%2br.wf_id",
            pre_callable=self.pre_callable,
        )

        assert rv.status_code == 200
        assert rv.content_type.lower() == "application/json"

        root_workflows = rv.json

        assert len(root_workflows["records"]) == 2
        assert root_workflows["records"][0]["wf_id"] == 1
        assert root_workflows["records"][1]["wf_id"] == 2

    def test_complex_query_2(self, cli):
        rv = cli.get_context(
            f"/api/v1/user/{self.user}/root?query=r.wf_id < r.timestamp&order=r.wf_id",
            pre_callable=self.pre_callable,
        )

        assert rv.status_code == 200
        assert rv.content_type.lower() == "application/json"

        root_workflows = rv.json

        assert len(root_workflows["records"]) == 5
        assert root_workflows["records"][0]["wf_id"] == 1
        assert root_workflows["records"][1]["wf_id"] == 2

    def test_ambiguous_query(self, cli):
        rv = cli.get_context(
            f"/api/v1/user/{self.user}/root?query=timestamp > 1000.0&order=wf_id",
            pre_callable=self.pre_callable,
        )

        assert rv.status_code == 400

    def test_order(self, cli):
        rv = cli.get_context(
            f"/api/v1/user/{self.user}/root?order=-r.wf_id",
            pre_callable=self.pre_callable,
        )

        assert rv.status_code == 200
        assert rv.content_type.lower() == "application/json"

        root_workflows = rv.json

        assert root_workflows["records"][0]["wf_id"] == 5

    def test_bad_order(self, cli):
        rv = cli.get_context(
            f"/api/v1/user/{self.user}/root?order=r.wf_i",
            pre_callable=self.pre_callable,
        )

        assert rv.status_code == 400

    def test_start_index(self, cli):
        rv = cli.get_context(
            f"/api/v1/user/{self.user}/root?start-index=1",
            pre_callable=self.pre_callable,
        )

        assert rv.status_code == 200
        assert rv.content_type.lower() == "application/json"

        root_workflows = rv.json

        assert len(root_workflows["records"]) == 4

    def test_bad_start_index(self, cli):
        rv = cli.get_context(
            f"/api/v1/user/{self.user}/root?start-index=AAA",
            pre_callable=self.pre_callable,
        )

        assert rv.status_code == 400

    def test_max_results(self, cli):
        rv = cli.get_context(
            f"/api/v1/user/{self.user}/root?max-results=2",
            pre_callable=self.pre_callable,
        )

        assert rv.status_code == 200
        assert rv.content_type.lower() == "application/json"

        root_workflows = rv.json

        assert len(root_workflows["records"]) == 2

    def test_bad_max_results(self, cli):
        rv = cli.get_context(
            f"/api/v1/user/{self.user}/root?max-results=AAA",
            pre_callable=self.pre_callable,
        )

        assert rv.status_code == 400

    def test_paging(self, cli):
        rv = cli.get_context(
            f"/api/v1/user/{self.user}/root?start-index=1&max-results=2",
            pre_callable=self.pre_callable,
        )

        assert rv.status_code == 200
        assert rv.content_type.lower() == "application/json"

        root_workflows = rv.json

        assert len(root_workflows["records"]) == 2

    def test_get_root_workflow_id(self, cli):
        rv = cli.get_context(
            f"/api/v1/user/{self.user}/root/1", pre_callable=self.pre_callable
        )

        assert rv.status_code == 200
        assert rv.content_type.lower() == "application/json"

        root_workflow = rv.json

        assert root_workflow["wf_id"] == 1

    def test_get_root_workflow_uuid(self, cli):
        rv = cli.get_context(
            f"/api/v1/user/{self.user}/root/7193de8c-a28d-4eca-b576-1b1c3c4f668b",
            pre_callable=self.pre_callable,
        )

        assert rv.status_code == 200
        assert rv.content_type.lower() == "application/json"

        root_workflow = rv.json

        assert root_workflow["wf_uuid"] == "7193de8c-a28d-4eca-b576-1b1c3c4f668b"

    def test_get_missing_root_workflow(self, cli):
        rv = cli.get_context(
            f"/api/v1/user/{self.user}/root/1000000000",
            pre_callable=self.pre_callable,
        )

        assert rv.status_code == 404


class TestStampedeWorkflowQueries(NoAuthFlaskTestCase):
    def test_get_workflows(self, cli):
        rv = cli.get_context(
            f"/api/v1/user/{self.user}/root/1/workflow",
            pre_callable=self.pre_callable,
        )

        assert rv.status_code == 200
        assert rv.content_type.lower() == "application/json"

        workflows = rv.json

        assert len(workflows["records"]) == 1
        assert len(workflows["records"]) == workflows["_meta"]["records_total"]
        assert (
            workflows["_meta"]["records_total"]
            == workflows["_meta"]["records_filtered"]
        )

    def test_get_workflow_uuid(self, cli):
        rv = cli.get_context(
            f"/api/v1/user/{self.user}/root/1/workflow/7193de8c-a28d-4eca-b576-1b1c3c4f668b",
            pre_callable=self.pre_callable,
        )

        assert rv.status_code == 200
        assert rv.content_type.lower() == "application/json"

        workflow = rv.json

        assert workflow["wf_uuid"] == "7193de8c-a28d-4eca-b576-1b1c3c4f668b"

    def test_get_workflow_uuids(self, cli):
        uuid = "7193de8c-a28d-4eca-b576-1b1c3c4f668b"
        rv = cli.get_context(
            f"/api/v1/user/{self.user}/root/{uuid}/workflow/{uuid}",
            pre_callable=self.pre_callable,
        )

        assert rv.status_code == 200
        assert rv.content_type.lower() == "application/json"

        workflow = rv.json

        assert workflow["wf_uuid"] == "7193de8c-a28d-4eca-b576-1b1c3c4f668b"

    def test_get_missing_workflow(self, cli):
        rv = cli.get_context(
            f"/api/v1/user/{self.user}/root/1/workflow/1000000000",
            pre_callable=self.pre_callable,
        )

        assert rv.status_code == 404


class TestStampedeWorkflowMetaQueries(NoAuthFlaskTestCase):
    def test_get_workflow_metas(self, cli):
        rv = cli.get_context(
            f"/api/v1/user/{self.user}/root/1/workflow/1/meta",
            pre_callable=self.pre_callable,
        )

        assert rv.status_code == 200
        assert rv.content_type.lower() == "application/json"

        metas = rv.json

        assert len(metas["records"]) == 1
        assert len(metas["records"]) == metas["_meta"]["records_total"]
        assert metas["_meta"]["records_total"] == metas["_meta"]["records_filtered"]

    def test_get_workflow_meta_query(self, cli):
        rv = cli.get_context(
            f"/api/v1/user/{self.user}/root/1/workflow/1/meta?query=wm.key == 'author'",
            pre_callable=self.pre_callable,
        )

        assert rv.status_code == 200
        assert rv.content_type.lower() == "application/json"

        metas = rv.json

        assert len(metas["records"]) == 1
        assert metas["records"][0]["value"] == "test"

    @pytest.mark.parametrize(
        "clause",
        [
            # Original published exploit chain.
            "wm.__init__.__globals__['__builtins__']['__import__']('os').system('id')",
            # Dunder traversal.
            "wm.__class__ == 1",
            "wm.wf_id.__class__ == 1",
            # Subscripting is no longer supported.
            "wm.wf_id['x'] == 1",
            # Calls other than like/ilike are rejected.
            "wm.metadata.create_all()",
            # Bare-name (constructor) call.
            "r() == 1",
            # __call__ bypass attempt.
            "wm.wf_id.like.__call__(2)",
            "wm.__init__.__globals__['__builtins__']['__import__']('os').system('id')",
        ],
    )
    def test_malicious_queries_rejected(self, cli, clause):
        rv = cli.get_context(
            f"/api/v1/user/{self.user}/root/1/workflow/1/meta?query={clause}",
            pre_callable=self.pre_callable,
        )

        assert rv.status_code == 400
        assert rv.content_type.lower() == "application/json"


class TestStampedeWorkflowFilesQueries(NoAuthFlaskTestCase):
    def test_get_workflow_files(self, cli):
        rv = cli.get_context(
            f"/api/v1/user/{self.user}/root/1/workflow/1/files",
            pre_callable=self.pre_callable,
        )

        assert rv.status_code == 200
        assert rv.content_type.lower() == "application/json"

        files = rv.json

        assert files["records"][0]["lfn"] == "f.b"
        assert len(files["records"][0]["pfns"]) == 4
        assert len(files["records"][0]["meta"]) == 1
        assert len(files["records"]) == 1
        assert len(files["records"]) == files["_meta"]["records_total"]
        assert files["_meta"]["records_total"] == files["_meta"]["records_filtered"]

    def test_get_workflow_files_query(self, cli):
        rv = cli.get_context(
            f"/api/v1/user/{self.user}/root/1/workflow/1/files?query=rm.key == 'sizeeee'",
            pre_callable=self.pre_callable,
        )

        assert rv.status_code == 200
        assert rv.content_type.lower() == "application/json"

        files = rv.json
        assert len(files["records"]) == files["_meta"]["records_filtered"]
        assert files["_meta"]["records_total"] == 1


class TestStampedeWorkflowStateQueries(NoAuthFlaskTestCase):
    def test_get_workflowstates(self, cli):
        rv = cli.get_context(
            f"/api/v1/user/{self.user}/root/1/workflow/1/state",
            pre_callable=self.pre_callable,
        )

        assert rv.status_code == 200
        assert rv.content_type.lower() == "application/json"

        workflow_states = rv.json

        assert len(workflow_states["records"]) == 8
        assert (
            len(workflow_states["records"]) == workflow_states["_meta"]["records_total"]
        )

        assert (
            workflow_states["_meta"]["records_total"]
            == workflow_states["_meta"]["records_filtered"]
        )

    def test_get_workflow(self, cli):
        rv = cli.get_context(
            f"/api/v1/user/{self.user}/root/1/workflow/1",
            pre_callable=self.pre_callable,
        )

        assert rv.status_code == 200
        assert rv.content_type.lower() == "application/json"

        job = rv.json

        assert job["dax_label"], "hello_world"


class TestStampedeJobQueries(NoAuthFlaskTestCase):
    def test_get_workflow_jobs(self, cli):
        rv = cli.get_context(
            f"/api/v1/user/{self.user}/root/1/workflow/1/job",
            pre_callable=self.pre_callable,
        )

        assert rv.status_code == 200
        assert rv.content_type.lower() == "application/json"

        jobs = rv.json

        assert len(jobs["records"]) == 14
        assert len(jobs["records"]) == jobs["_meta"]["records_total"]
        assert jobs["_meta"]["records_total"] == jobs["_meta"]["records_filtered"]

    def test_get_job(self, cli):
        rv = cli.get_context(
            f"/api/v1/user/{self.user}/root/1/workflow/1/job/1",
            pre_callable=self.pre_callable,
        )

        assert rv.status_code == 200
        assert rv.content_type.lower() == "application/json"

        job = rv.json

        assert job["max_retries"] == 3

    def test_get_missing_job(self, cli):
        rv = cli.get_context(
            f"/api/v1/user/{self.user}/root/1/workflow/1/job/0",
            pre_callable=self.pre_callable,
        )

        assert rv.status_code == 404

    def test_get_running_jobs(self, cli):
        rv = cli.get_context(
            f"/api/v1/user/{self.user}/root/1/workflow/1/job/successful",
            pre_callable=self.pre_callable,
        )

        assert rv.status_code == 200
        assert rv.content_type.lower() == "application/json"

        jobs = rv.json

        assert len(jobs["records"]) == 5
        assert len(jobs["records"]) == jobs["_meta"]["records_total"]
        assert jobs["_meta"]["records_total"] == jobs["_meta"]["records_filtered"]
        assert "job_instance" in jobs["records"][0]


class TestStampedeHostQueries(NoAuthFlaskTestCase):
    def test_get_workflow_hosts(self, cli):
        rv = cli.get_context(
            f"/api/v1/user/{self.user}/root/1/workflow/1/host",
            pre_callable=self.pre_callable,
        )

        assert rv.status_code == 200
        assert rv.content_type.lower() == "application/json"

        hosts = rv.json

        assert len(hosts["records"]) == 2
        assert len(hosts["records"]) == hosts["_meta"]["records_total"]
        assert hosts["_meta"]["records_total"] == hosts["_meta"]["records_filtered"]

    def test_get_host(self, cli):
        rv = cli.get_context(
            f"/api/v1/user/{self.user}/root/1/workflow/1/host/1",
            pre_callable=self.pre_callable,
        )

        assert rv.status_code == 200
        assert rv.content_type.lower() == "application/json"

        host = rv.json

        assert host["hostname"] == "isis.isi.edu"

    def test_get_missing_host(self, cli):
        rv = cli.get_context(
            f"/api/v1/user/{self.user}/root/1/workflow/1/host/0",
            pre_callable=self.pre_callable,
        )

        assert rv.status_code == 404


class TestStampedeJobstateQueries(NoAuthFlaskTestCase):
    def test_get_jobstates(self, cli):
        rv = cli.get_context(
            f"/api/v1/user/{self.user}/root/1/workflow/1/job/1/job-instance/3/state",
            pre_callable=self.pre_callable,
        )

        assert rv.status_code == 200
        assert rv.content_type.lower() == "application/json"

        job_states = rv.json

        assert len(job_states["records"]) == 7
        assert len(job_states["records"]) == job_states["_meta"]["records_total"]
        assert (
            job_states["_meta"]["records_total"]
            == job_states["_meta"]["records_filtered"]
        )


class TestStampedeTaskQueries(NoAuthFlaskTestCase):
    def test_get_workflow_tasks(self, cli):
        rv = cli.get_context(
            f"/api/v1/user/{self.user}/root/1/workflow/1/task",
            pre_callable=self.pre_callable,
        )

        assert rv.status_code == 200
        assert rv.content_type.lower() == "application/json"

        tasks = rv.json

        assert len(tasks["records"]) == 2
        assert len(tasks["records"]) == tasks["_meta"]["records_total"]
        assert tasks["_meta"]["records_total"] == tasks["_meta"]["records_filtered"]

    def test_get_job_tasks(self, cli):
        rv = cli.get_context(
            f"/api/v1/user/{self.user}/root/1/workflow/1/job/11/task",
            pre_callable=self.pre_callable,
        )

        assert rv.status_code == 200
        assert rv.content_type.lower() == "application/json"

        tasks = rv.json

        assert len(tasks["records"]) == 1
        assert len(tasks["records"]) == tasks["_meta"]["records_total"]
        assert tasks["_meta"]["records_total"] == tasks["_meta"]["records_filtered"]

    def test_get_task(self, cli):
        rv = cli.get_context(
            f"/api/v1/user/{self.user}/root/1/workflow/1/task/1",
            pre_callable=self.pre_callable,
        )

        assert rv.status_code == 200
        assert rv.content_type.lower() == "application/json"

        task = rv.json

        assert task["task_id"] == 1

    def test_get_missing_task(self, cli):
        rv = cli.get_context(
            f"/api/v1/user/{self.user}/root/1/workflow/1/task/0",
            pre_callable=self.pre_callable,
        )

        assert rv.status_code == 404


class TestStampedeTaskMetaQueries(NoAuthFlaskTestCase):
    def test_get_workflow_metas(self, cli):
        rv = cli.get_context(
            f"/api/v1/user/{self.user}/root/1/workflow/1/task/1/meta",
            pre_callable=self.pre_callable,
        )

        assert rv.status_code == 200
        assert rv.content_type.lower() == "application/json"

        metas = rv.json

        assert len(metas["records"]) == 1
        assert len(metas["records"]) == metas["_meta"]["records_total"]
        assert metas["_meta"]["records_total"] == metas["_meta"]["records_filtered"]

    def test_get_workflow_meta_query(self, cli):
        rv = cli.get_context(
            f"/api/v1/user/{self.user}/root/1/workflow/1/task/1/meta?query=tm.key == 'time'",
            pre_callable=self.pre_callable,
        )

        assert rv.status_code == 200
        assert rv.content_type.lower() == "application/json"

        metas = rv.json

        assert len(metas["records"]) == 1
        assert metas["records"][0]["value"] == "60"


class TestStampedeJobInstanceQueries(NoAuthFlaskTestCase):
    def test_get_job_instances(self, cli):
        rv = cli.get_context(
            f"/api/v1/user/{self.user}/root/1/workflow/1/job/6/job-instance",
            pre_callable=self.pre_callable,
        )

        assert rv.status_code == 200
        assert rv.content_type.lower() == "application/json"

        job_instances = rv.json

        assert len(job_instances["records"]) == 16
        assert len(job_instances["records"]) == job_instances["_meta"]["records_total"]
        assert (
            job_instances["_meta"]["records_total"]
            == job_instances["_meta"]["records_filtered"]
        )

    def test_get_recent_job_instance(self, cli):
        rv = cli.get_context(
            f"/api/v1/user/{self.user}/root/1/workflow/1/job/6/job-instance?recent=true",
            pre_callable=self.pre_callable,
        )

        assert rv.status_code == 200
        assert rv.content_type.lower() == "application/json"

        job_instances = rv.json

        assert len(job_instances["records"]) == 1
        assert job_instances["_meta"]["records_total"] == 16
        assert job_instances["_meta"]["records_filtered"] == 1

    def test_get_job_instance(self, cli):
        rv = cli.get_context(
            f"/api/v1/user/{self.user}/root/1/workflow/1/job/6/job-instance?recent=true",
            pre_callable=self.pre_callable,
        )

        assert rv.status_code == 200
        assert rv.content_type.lower() == "application/json"

        job_instance = rv.json

        assert job_instance["records"][0]["job_instance_id"] == 21

    def test_get_missing_job_instance(self, cli):
        rv = cli.get_context(
            f"/api/v1/user/{self.user}/root/1/workflow/1/job-instance/0",
            pre_callable=self.pre_callable,
        )

        assert rv.status_code == 404


class TestStampedeInvocationQueries(NoAuthFlaskTestCase):
    def test_get_workflow_invocations(self, cli):
        rv = cli.get_context(
            f"/api/v1/user/{self.user}/root/1/workflow/1/invocation",
            pre_callable=self.pre_callable,
        )

        assert rv.status_code == 200
        assert rv.content_type.lower() == "application/json"

        invocations = rv.json

        assert len(invocations["records"]) == 41
        assert len(invocations["records"]) == invocations["_meta"]["records_total"]
        assert (
            invocations["_meta"]["records_total"]
            == invocations["_meta"]["records_filtered"]
        )

    def test_get_job_instance_invocations(self, cli):
        rv = cli.get_context(
            f"/api/v1/user/{self.user}/root/1/workflow/1/job/6/job-instance/21/invocation",
            pre_callable=self.pre_callable,
        )

        assert rv.status_code == 200
        assert rv.content_type.lower() == "application/json"

        invocations = rv.json

        assert len(invocations["records"]) == 2
        assert len(invocations["records"]) == invocations["_meta"]["records_total"]
        assert (
            invocations["_meta"]["records_total"]
            == invocations["_meta"]["records_filtered"]
        )

    def test_get_invocation(self, cli):
        rv = cli.get_context(
            f"/api/v1/user/{self.user}/root/1/workflow/1/invocation/41",
            pre_callable=self.pre_callable,
        )

        assert rv.status_code == 200
        assert rv.content_type.lower() == "application/json"

        invocation = rv.json

        assert invocation["task_submit_seq"] == -2

    def test_get_missing_invocation(self, cli):
        rv = cli.get_context(
            f"/api/v1/user/{self.user}/root/1/workflow/1/invocation/0",
            pre_callable=self.pre_callable,
        )

        assert rv.status_code == 404
