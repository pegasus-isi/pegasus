#  Copyright 2007-2014 University Of Southern California
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

__author__ = "Rajiv Mayani"

import json
import logging
import os
import unittest

from six import StringIO

from flask import g
from Pegasus.service import app
from Pegasus.service.monitoring.resources import *


class JSONResponseMixin(object):
    @staticmethod
    def read_response(response):
        output = StringIO()

        try:
            for line in response.response:
                output.write(line)

            return output.getvalue()
        finally:
            output.close()

    @staticmethod
    def read_json_response(response):
        return json.loads(JSONResponseMixin.read_response(response))


class FlaskTestCase(unittest.TestCase, JSONResponseMixin):
    def setUp(self):
        logging.basicConfig(level=logging.ERROR)
        app.config["TESTING"] = True
        self.client = app.test_client()

        self.get = self.client.get
        self.post = self.client.post
        self.put = self.client.put
        self.delete = self.client.delete

        self.get_context = self.get_with_context
        self.post_context = self.post_with_context
        self.put_context = self.put_with_context
        self.delete_context = self.delete_with_context

    def get_with_context(self, uri, data=None, pre_callable=None):
        return self.request_with_context(uri, "GET", data, pre_callable=pre_callable)

    def post_with_context(self, uri, data=None, pre_callable=None):
        return self.request_with_context(uri, "POST", data, pre_callable=pre_callable)

    def put_with_context(self, uri, data=None, pre_callable=None):
        return self.request_with_context(uri, "PUT", data, pre_callable=pre_callable)

    def delete_with_context(self, uri, data=None, pre_callable=None):
        return self.request_with_context(uri, "DELETE", data, pre_callable=pre_callable)

    def request_with_context(self, uri, method="GET", data=None, pre_callable=None):
        """
        Take a request through Flask's request lifecycle in a test context.

        :param uri: URI to call
        :param method: HTTP verb to call request with
        :param pre_callable: A callable method, invoked after pre_processing functions of Flask are called.
        :param kwargs: data to be passed along wtih request
        :return: Flask Response object
        """
        with app.test_request_context(uri, method=method, data=data):
            try:
                # Set Master DB URL
                if pre_callable is not None:
                    pre_callable()

                # Pre process Request
                rv = app.preprocess_request()

                # Set STAMPEDE DB URL, which was overwritten by pre processing methods.
                if pre_callable is not None:
                    pre_callable()

                if rv is None:
                    # Main Dispatch
                    rv = app.dispatch_request()

            except Exception as e:
                rv = app.handle_user_exception(e)

            response = app.make_response(rv)

            # Post process Request
            response = app.process_response(response)

        return response


class NoAuthFlaskTestCase(FlaskTestCase):
    def setUp(self):
        FlaskTestCase.setUp(self)
        app.config["AUTHENTICATION"] = "NoAuthentication"
        app.config["PROCESS_SWITCHING"] = False

        self.user = os.getenv("USER")

    @staticmethod
    def pre_callable():
        directory = os.path.dirname(__file__)
        db = os.path.join(directory, "monitoring-rest-api-master.db")
        g.master_db_url = "sqlite:///%s" % db
        g.stampede_db_url = "sqlite:///%s" % db


class TestMasterWorkflowQueries(NoAuthFlaskTestCase):
    def test_get_root_workflows(self):
        rv = self.get_context(
            "/api/v1/user/%s/root" % self.user, pre_callable=self.pre_callable
        )

        self.assertEqual(rv.status_code, 200)
        self.assertEqual(rv.content_type.lower(), "application/json")

        root_workflows = self.read_json_response(rv)

        self.assertEqual(len(root_workflows["records"]), 5)
        self.assertEqual(
            len(root_workflows["records"]), root_workflows["_meta"]["records_total"]
        )
        self.assertEqual(
            root_workflows["_meta"]["records_total"],
            root_workflows["_meta"]["records_filtered"],
        )

    def test_query_with_prefix(self):
        rv = self.get_context(
            "/api/v1/user/%s/root?query=r.wf_id == 1" % self.user,
            pre_callable=self.pre_callable,
        )

        self.assertEqual(rv.status_code, 200)
        self.assertEqual(rv.content_type.lower(), "application/json")

        root_workflows = self.read_json_response(rv)

        self.assertEqual(root_workflows["records"][0]["wf_id"], 1)

    def test_query_without_prefix(self):
        rv = self.get_context(
            "/api/v1/user/%s/root?query=submit_hostname like '%%.edu'&order=wf_id"
            % self.user,
            pre_callable=self.pre_callable,
        )

        self.assertEqual(rv.status_code, 400)
        self.assertEqual(rv.content_type.lower(), "application/json")

    def test_complex_query(self):
        rv = self.get_context(
            "/api/v1/user/%s/root?query=r.wf_id == 1 or (r.wf_id == 2 and r.grid_dn is None)&order=%%2br.wf_id"
            % self.user,
            pre_callable=self.pre_callable,
        )

        self.assertEqual(rv.status_code, 200)
        self.assertEqual(rv.content_type.lower(), "application/json")

        root_workflows = self.read_json_response(rv)

        self.assertEqual(len(root_workflows["records"]), 2)
        self.assertEqual(root_workflows["records"][0]["wf_id"], 1)
        self.assertEqual(root_workflows["records"][1]["wf_id"], 2)

    def test_complex_query_2(self):
        rv = self.get_context(
            "/api/v1/user/%s/root?query=r.wf_id < r.timestamp&order=r.wf_id"
            % self.user,
            pre_callable=self.pre_callable,
        )

        self.assertEqual(rv.status_code, 200)
        self.assertEqual(rv.content_type.lower(), "application/json")

        root_workflows = self.read_json_response(rv)

        self.assertEqual(len(root_workflows["records"]), 5)
        self.assertEqual(root_workflows["records"][0]["wf_id"], 1)
        self.assertEqual(root_workflows["records"][1]["wf_id"], 2)

    def test_ambiguous_query(self):
        rv = self.get_context(
            "/api/v1/user/%s/root?query=timestamp > 1000.0&order=wf_id" % self.user,
            pre_callable=self.pre_callable,
        )

        self.assertEqual(rv.status_code, 400)

    def test_order(self):
        rv = self.get_context(
            "/api/v1/user/%s/root?order=-r.wf_id" % self.user,
            pre_callable=self.pre_callable,
        )

        self.assertEqual(rv.status_code, 200)
        self.assertEqual(rv.content_type.lower(), "application/json")

        root_workflows = self.read_json_response(rv)

        self.assertEqual(root_workflows["records"][0]["wf_id"], 5)

    def test_bad_order(self):
        rv = self.get_context(
            "/api/v1/user/%s/root?order=r.wf_i" % self.user,
            pre_callable=self.pre_callable,
        )

        self.assertEqual(rv.status_code, 400)

    def test_start_index(self):
        rv = self.get_context(
            "/api/v1/user/%s/root?start-index=1" % self.user,
            pre_callable=self.pre_callable,
        )

        self.assertEqual(rv.status_code, 200)
        self.assertEqual(rv.content_type.lower(), "application/json")

        root_workflows = self.read_json_response(rv)

        self.assertEqual(len(root_workflows["records"]), 4)

    def test_bad_start_index(self):
        rv = self.get_context(
            "/api/v1/user/%s/root?start-index=AAA" % self.user,
            pre_callable=self.pre_callable,
        )

        self.assertEqual(rv.status_code, 400)

    def test_max_results(self):
        rv = self.get_context(
            "/api/v1/user/%s/root?max-results=2" % self.user,
            pre_callable=self.pre_callable,
        )

        self.assertEqual(rv.status_code, 200)
        self.assertEqual(rv.content_type.lower(), "application/json")

        root_workflows = self.read_json_response(rv)

        self.assertEqual(len(root_workflows["records"]), 2)

    def test_bad_max_results(self):
        rv = self.get_context(
            "/api/v1/user/%s/root?max-results=AAA" % self.user,
            pre_callable=self.pre_callable,
        )

        self.assertEqual(rv.status_code, 400)

    def test_paging(self):
        rv = self.get_context(
            "/api/v1/user/%s/root?start-index=1&max-results=2" % self.user,
            pre_callable=self.pre_callable,
        )

        self.assertEqual(rv.status_code, 200)
        self.assertEqual(rv.content_type.lower(), "application/json")

        root_workflows = self.read_json_response(rv)

        self.assertEqual(len(root_workflows["records"]), 2)

    def test_get_root_workflow_id(self):
        rv = self.get_context(
            "/api/v1/user/%s/root/1" % self.user, pre_callable=self.pre_callable
        )

        self.assertEqual(rv.status_code, 200)
        self.assertEqual(rv.content_type.lower(), "application/json")

        root_workflow = self.read_json_response(rv)

        self.assertTrue(root_workflow["_links"])
        self.assertTrue(root_workflow["wf_id"], 1)

    def test_get_root_workflow_uuid(self):
        rv = self.get_context(
            "/api/v1/user/%s/root/7193de8c-a28d-4eca-b576-1b1c3c4f668b" % self.user,
            pre_callable=self.pre_callable,
        )

        self.assertEqual(rv.status_code, 200)
        self.assertEqual(rv.content_type.lower(), "application/json")

        root_workflow = self.read_json_response(rv)

        self.assertTrue(root_workflow["_links"])
        self.assertTrue(
            root_workflow["wf_uuid"], "7193de8c-a28d-4eca-b576-1b1c3c4f668b"
        )

    def test_get_missing_root_workflow(self):
        rv = self.get_context(
            "/api/v1/user/%s/root/1000000000" % self.user,
            pre_callable=self.pre_callable,
        )

        self.assertEqual(rv.status_code, 404)


class TestStampedeWorkflowQueries(NoAuthFlaskTestCase):
    def test_get_workflows(self):
        rv = self.get_context(
            "/api/v1/user/%s/root/1/workflow" % self.user,
            pre_callable=self.pre_callable,
        )

        self.assertEqual(rv.status_code, 200)
        self.assertEqual(rv.content_type.lower(), "application/json")

        workflows = self.read_json_response(rv)

        self.assertEqual(len(workflows["records"]), 1)
        self.assertEqual(len(workflows["records"]), workflows["_meta"]["records_total"])
        self.assertEqual(
            workflows["_meta"]["records_total"], workflows["_meta"]["records_filtered"]
        )

    def test_get_workflow_uuid(self):
        rv = self.get_context(
            "/api/v1/user/%s/root/1/workflow/7193de8c-a28d-4eca-b576-1b1c3c4f668b"
            % self.user,
            pre_callable=self.pre_callable,
        )

        self.assertEqual(rv.status_code, 200)
        self.assertEqual(rv.content_type.lower(), "application/json")

        workflow = self.read_json_response(rv)

        self.assertTrue(workflow["_links"])
        self.assertTrue(workflow["wf_uuid"], "7193de8c-a28d-4eca-b576-1b1c3c4f668b")

    def test_get_workflow_uuids(self):
        uuid = "7193de8c-a28d-4eca-b576-1b1c3c4f668b"
        rv = self.get_context(
            "/api/v1/user/%s/root/%s/workflow/%s" % (self.user, uuid, uuid),
            pre_callable=self.pre_callable,
        )

        self.assertEqual(rv.status_code, 200)
        self.assertEqual(rv.content_type.lower(), "application/json")

        workflow = self.read_json_response(rv)

        self.assertTrue(workflow["_links"])
        self.assertTrue(workflow["wf_uuid"], "7193de8c-a28d-4eca-b576-1b1c3c4f668b")

    def test_get_missing_workflow(self):
        rv = self.get_context(
            "/api/v1/user/%s/root/1/workflow/1000000000" % self.user,
            pre_callable=self.pre_callable,
        )

        self.assertEqual(rv.status_code, 404)


class TestStampedeWorkflowMetaQueries(NoAuthFlaskTestCase):
    def test_get_workflow_metas(self):
        rv = self.get_context(
            "/api/v1/user/%s/root/1/workflow/1/meta" % self.user,
            pre_callable=self.pre_callable,
        )

        self.assertEqual(rv.status_code, 200)
        self.assertEqual(rv.content_type.lower(), "application/json")

        metas = self.read_json_response(rv)

        self.assertEqual(len(metas["records"]), 1)
        self.assertEqual(len(metas["records"]), metas["_meta"]["records_total"])
        self.assertEqual(
            metas["_meta"]["records_total"], metas["_meta"]["records_filtered"]
        )

    def test_get_workflow_meta_query(self):
        rv = self.get_context(
            "/api/v1/user/%s/root/1/workflow/1/meta?query=wm.key == 'author'"
            % self.user,
            pre_callable=self.pre_callable,
        )

        self.assertEqual(rv.status_code, 200)
        self.assertEqual(rv.content_type.lower(), "application/json")

        metas = self.read_json_response(rv)

        self.assertEqual(len(metas["records"]), 1)
        self.assertTrue(metas["records"][0]["value"], "test")


class TestStampedeWorkflowFilesQueries(NoAuthFlaskTestCase):
    def test_get_workflow_files(self):
        rv = self.get_context(
            "/api/v1/user/%s/root/1/workflow/1/files" % self.user,
            pre_callable=self.pre_callable,
        )

        self.assertEqual(rv.status_code, 200)
        self.assertEqual(rv.content_type.lower(), "application/json")

        files = self.read_json_response(rv)

        self.assertEqual(files["records"][0]["lfn"], "f.b")
        self.assertEqual(len(files["records"][0]["pfns"]), 4)
        self.assertEqual(len(files["records"][0]["meta"]), 1)
        self.assertEqual(len(files["records"]), 1)
        self.assertEqual(len(files["records"]), files["_meta"]["records_total"])
        self.assertEqual(
            files["_meta"]["records_total"], files["_meta"]["records_filtered"]
        )

    def test_get_workflow_files_query(self):
        rv = self.get_context(
            "/api/v1/user/%s/root/1/workflow/1/files?query=rm.key == 'sizeeee'"
            % self.user,
            pre_callable=self.pre_callable,
        )

        self.assertEqual(rv.status_code, 200)
        self.assertEqual(rv.content_type.lower(), "application/json")

        files = self.read_json_response(rv)
        self.assertEqual(len(files["records"]), files["_meta"]["records_filtered"])
        self.assertEqual(files["_meta"]["records_total"], 1)


class TestStampedeWorkflowStateQueries(NoAuthFlaskTestCase):
    def test_get_workflowstates(self):
        rv = self.get_context(
            "/api/v1/user/%s/root/1/workflow/1/state" % self.user,
            pre_callable=self.pre_callable,
        )

        self.assertEqual(rv.status_code, 200)
        self.assertEqual(rv.content_type.lower(), "application/json")

        workflow_states = self.read_json_response(rv)

        self.assertEqual(len(workflow_states["records"]), 8)
        self.assertEqual(
            len(workflow_states["records"]), workflow_states["_meta"]["records_total"]
        )
        self.assertEqual(
            workflow_states["_meta"]["records_total"],
            workflow_states["_meta"]["records_filtered"],
        )

    def test_get_workflow(self):
        rv = self.get_context(
            "/api/v1/user/%s/root/1/workflow/1" % self.user,
            pre_callable=self.pre_callable,
        )

        self.assertEqual(rv.status_code, 200)
        self.assertEqual(rv.content_type.lower(), "application/json")

        job = self.read_json_response(rv)

        self.assertEqual(job["dax_label"], "hello_world")

    def test_get_recent_workflow_state(self):
        rv = self.get_context(
            "/api/v1/user/%s/root/1/workflow/1/state;recent=true" % self.user,
            pre_callable=self.pre_callable,
        )

        self.assertEqual(rv.status_code, 200)
        self.assertEqual(rv.content_type.lower(), "application/json")

        workflow_states = self.read_json_response(rv)

        self.assertEqual(len(workflow_states["records"]), 1)
        self.assertEqual(workflow_states["_meta"]["records_total"], 8)
        self.assertEqual(workflow_states["_meta"]["records_filtered"], 1)


class TestStampedeJobQueries(NoAuthFlaskTestCase):
    def test_get_workflow_jobs(self):
        rv = self.get_context(
            "/api/v1/user/%s/root/1/workflow/1/job" % self.user,
            pre_callable=self.pre_callable,
        )

        self.assertEqual(rv.status_code, 200)
        self.assertEqual(rv.content_type.lower(), "application/json")

        jobs = self.read_json_response(rv)

        self.assertEqual(len(jobs["records"]), 14)
        self.assertEqual(len(jobs["records"]), jobs["_meta"]["records_total"])
        self.assertEqual(
            jobs["_meta"]["records_total"], jobs["_meta"]["records_filtered"]
        )

    def test_get_job(self):
        rv = self.get_context(
            "/api/v1/user/%s/root/1/workflow/1/job/1" % self.user,
            pre_callable=self.pre_callable,
        )

        self.assertEqual(rv.status_code, 200)
        self.assertEqual(rv.content_type.lower(), "application/json")

        job = self.read_json_response(rv)

        self.assertEqual(job["max_retries"], 3)

    def test_get_missing_job(self):
        rv = self.get_context(
            "/api/v1/user/%s/root/1/workflow/1/job/0" % self.user,
            pre_callable=self.pre_callable,
        )

        self.assertEqual(rv.status_code, 404)


class TestStampedeHostQueries(NoAuthFlaskTestCase):
    def test_get_workflow_hosts(self):
        rv = self.get_context(
            "/api/v1/user/%s/root/1/workflow/1/host" % self.user,
            pre_callable=self.pre_callable,
        )

        self.assertEqual(rv.status_code, 200)
        self.assertEqual(rv.content_type.lower(), "application/json")

        hosts = self.read_json_response(rv)

        self.assertEqual(len(hosts["records"]), 2)
        self.assertEqual(len(hosts["records"]), hosts["_meta"]["records_total"])
        self.assertEqual(
            hosts["_meta"]["records_total"], hosts["_meta"]["records_filtered"]
        )

    def test_get_host(self):
        rv = self.get_context(
            "/api/v1/user/%s/root/1/workflow/1/host/1" % self.user,
            pre_callable=self.pre_callable,
        )

        self.assertEqual(rv.status_code, 200)
        self.assertEqual(rv.content_type.lower(), "application/json")

        host = self.read_json_response(rv)

        self.assertEqual(host["hostname"], "isis.isi.edu")

    def test_get_missing_host(self):
        rv = self.get_context(
            "/api/v1/user/%s/root/1/workflow/1/host/0" % self.user,
            pre_callable=self.pre_callable,
        )

        self.assertEqual(rv.status_code, 404)


class TestStampedeJobstateQueries(NoAuthFlaskTestCase):
    def test_get_jobstates(self):
        rv = self.get_context(
            "/api/v1/user/%s/root/1/workflow/1/job/1/job-instance/3/state" % self.user,
            pre_callable=self.pre_callable,
        )

        self.assertEqual(rv.status_code, 200)
        self.assertEqual(rv.content_type.lower(), "application/json")

        job_states = self.read_json_response(rv)

        self.assertEqual(len(job_states["records"]), 7)
        self.assertEqual(
            len(job_states["records"]), job_states["_meta"]["records_total"]
        )
        self.assertEqual(
            job_states["_meta"]["records_total"],
            job_states["_meta"]["records_filtered"],
        )

    def test_get_recent_job_state(self):
        rv = self.get_context(
            "/api/v1/user/%s/root/1/workflow/1/job/1/job-instance/3/state;recent=true"
            % self.user,
            pre_callable=self.pre_callable,
        )

        self.assertEqual(rv.status_code, 200)
        self.assertEqual(rv.content_type.lower(), "application/json")

        job_states = self.read_json_response(rv)

        self.assertEqual(len(job_states["records"]), 1)
        self.assertEqual(job_states["_meta"]["records_total"], 7)
        self.assertEqual(job_states["_meta"]["records_filtered"], 1)


class TestStampedeTaskQueries(NoAuthFlaskTestCase):
    def test_get_workflow_tasks(self):
        rv = self.get_context(
            "/api/v1/user/%s/root/1/workflow/1/task" % self.user,
            pre_callable=self.pre_callable,
        )

        self.assertEqual(rv.status_code, 200)
        self.assertEqual(rv.content_type.lower(), "application/json")

        tasks = self.read_json_response(rv)

        self.assertEqual(len(tasks["records"]), 2)
        self.assertEqual(len(tasks["records"]), tasks["_meta"]["records_total"])
        self.assertEqual(
            tasks["_meta"]["records_total"], tasks["_meta"]["records_filtered"]
        )

    def test_get_job_tasks(self):
        rv = self.get_context(
            "/api/v1/user/%s/root/1/workflow/1/job/11/task" % self.user,
            pre_callable=self.pre_callable,
        )

        self.assertEqual(rv.status_code, 200)
        self.assertEqual(rv.content_type.lower(), "application/json")

        tasks = self.read_json_response(rv)

        self.assertEqual(len(tasks["records"]), 1)
        self.assertEqual(len(tasks["records"]), tasks["_meta"]["records_total"])
        self.assertEqual(
            tasks["_meta"]["records_total"], tasks["_meta"]["records_filtered"]
        )

    def test_get_task(self):
        rv = self.get_context(
            "/api/v1/user/%s/root/1/workflow/1/task/1" % self.user,
            pre_callable=self.pre_callable,
        )

        self.assertEqual(rv.status_code, 200)
        self.assertEqual(rv.content_type.lower(), "application/json")

        task = self.read_json_response(rv)

        self.assertEqual(task["task_id"], 1)

    def test_get_missing_task(self):
        rv = self.get_context(
            "/api/v1/user/%s/root/1/workflow/1/task/0" % self.user,
            pre_callable=self.pre_callable,
        )

        self.assertEqual(rv.status_code, 404)


class TestStampedeTaskMetaQueries(NoAuthFlaskTestCase):
    def test_get_workflow_metas(self):
        rv = self.get_context(
            "/api/v1/user/%s/root/1/workflow/1/task/1/meta" % self.user,
            pre_callable=self.pre_callable,
        )

        self.assertEqual(rv.status_code, 200)
        self.assertEqual(rv.content_type.lower(), "application/json")

        metas = self.read_json_response(rv)

        self.assertEqual(len(metas["records"]), 1)
        self.assertEqual(len(metas["records"]), metas["_meta"]["records_total"])
        self.assertEqual(
            metas["_meta"]["records_total"], metas["_meta"]["records_filtered"]
        )

    def test_get_workflow_meta_query(self):
        rv = self.get_context(
            "/api/v1/user/%s/root/1/workflow/1/task/1/meta?query=tm.key == 'time'"
            % self.user,
            pre_callable=self.pre_callable,
        )

        self.assertEqual(rv.status_code, 200)
        self.assertEqual(rv.content_type.lower(), "application/json")

        metas = self.read_json_response(rv)

        self.assertEqual(len(metas["records"]), 1)
        self.assertTrue(metas["records"][0]["value"], "60")


class TestStampedeJobInstanceQueries(NoAuthFlaskTestCase):
    def test_get_job_instances(self):
        rv = self.get_context(
            "/api/v1/user/%s/root/1/workflow/1/job/6/job-instance" % self.user,
            pre_callable=self.pre_callable,
        )

        self.assertEqual(rv.status_code, 200)
        self.assertEqual(rv.content_type.lower(), "application/json")

        job_instances = self.read_json_response(rv)

        self.assertEqual(len(job_instances["records"]), 16)
        self.assertEqual(
            len(job_instances["records"]), job_instances["_meta"]["records_total"]
        )
        self.assertEqual(
            job_instances["_meta"]["records_total"],
            job_instances["_meta"]["records_filtered"],
        )

    def test_get_recent_job_instance(self):
        rv = self.get_context(
            "/api/v1/user/%s/root/1/workflow/1/job/6/job-instance;recent=true"
            % self.user,
            pre_callable=self.pre_callable,
        )

        self.assertEqual(rv.status_code, 200)
        self.assertEqual(rv.content_type.lower(), "application/json")

        job_instances = self.read_json_response(rv)

        self.assertEqual(len(job_instances["records"]), 1)
        self.assertEqual(job_instances["_meta"]["records_total"], 16)
        self.assertEqual(job_instances["_meta"]["records_filtered"], 1)

    def test_get_job_instance(self):
        rv = self.get_context(
            "/api/v1/user/%s/root/1/workflow/1/job/6/job-instance;recent=true"
            % self.user,
            pre_callable=self.pre_callable,
        )

        self.assertEqual(rv.status_code, 200)
        self.assertEqual(rv.content_type.lower(), "application/json")

        job_instance = self.read_json_response(rv)

        self.assertEqual(job_instance["records"][0]["job_instance_id"], 21)

    def test_get_missing_job_instance(self):
        rv = self.get_context(
            "/api/v1/user/%s/root/1/workflow/1/job-instance/0" % self.user,
            pre_callable=self.pre_callable,
        )

        self.assertEqual(rv.status_code, 404)


class TestStampedeInvocationQueries(NoAuthFlaskTestCase):
    def test_get_workflow_invocations(self):
        rv = self.get_context(
            "/api/v1/user/%s/root/1/workflow/1/invocation" % self.user,
            pre_callable=self.pre_callable,
        )

        self.assertEqual(rv.status_code, 200)
        self.assertEqual(rv.content_type.lower(), "application/json")

        invocations = self.read_json_response(rv)

        self.assertEqual(len(invocations["records"]), 41)
        self.assertEqual(
            len(invocations["records"]), invocations["_meta"]["records_total"]
        )
        self.assertEqual(
            invocations["_meta"]["records_total"],
            invocations["_meta"]["records_filtered"],
        )

    def test_get_job_instance_invocations(self):
        rv = self.get_context(
            "/api/v1/user/%s/root/1/workflow/1/job/6/job-instance/21/invocation"
            % self.user,
            pre_callable=self.pre_callable,
        )

        self.assertEqual(rv.status_code, 200)
        self.assertEqual(rv.content_type.lower(), "application/json")

        invocations = self.read_json_response(rv)

        self.assertEqual(len(invocations["records"]), 2)
        self.assertEqual(
            len(invocations["records"]), invocations["_meta"]["records_total"]
        )
        self.assertEqual(
            invocations["_meta"]["records_total"],
            invocations["_meta"]["records_filtered"],
        )

    def test_get_invocation(self):
        rv = self.get_context(
            "/api/v1/user/%s/root/1/workflow/1/invocation/41" % self.user,
            pre_callable=self.pre_callable,
        )

        self.assertEqual(rv.status_code, 200)
        self.assertEqual(rv.content_type.lower(), "application/json")

        invocation = self.read_json_response(rv)

        self.assertEqual(invocation["task_submit_seq"], -2)

    def test_get_missing_invocation(self):
        rv = self.get_context(
            "/api/v1/user/%s/root/1/workflow/1/invocation/0" % self.user,
            pre_callable=self.pre_callable,
        )

        self.assertEqual(rv.status_code, 404)
