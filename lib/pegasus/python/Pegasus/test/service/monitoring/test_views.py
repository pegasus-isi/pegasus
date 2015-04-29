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

__author__ = 'Rajiv Mayani'

import json
import logging
import unittest
import StringIO

from Pegasus.service import app

from Pegasus.service.monitoring.resources import *


class JSONResponseMixin(object):
    @staticmethod
    def read_response(response):
        output = StringIO.StringIO()

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
        app.config['TESTING'] = True
        self.app = app.test_client()


class NoAuthFlaskTestCase(FlaskTestCase):
    def setUp(self):
        FlaskTestCase.setUp(self)
        app.config['AUTHENTICATION'] = 'NoAuthentication'


class TestMasterWorkflowQueries(NoAuthFlaskTestCase):
    def test_get_root_workflows(self):
        rv = self.app.get('/api/v1/user/mayani/root')

        self.assertEqual(rv.status_code, 200)
        self.assertEqual(rv.content_type.lower(), 'application/json')

        root_workflows = self.read_json_response(rv)

        self.assertEqual(len(root_workflows['records']), 5)
        self.assertEqual(len(root_workflows['records']), root_workflows['_meta']['records_total'])
        self.assertEqual(root_workflows['_meta']['records_total'], root_workflows['_meta']['records_filtered'])

    def test_query_with_prefix(self):
        rv = self.app.get('/api/v1/user/mayani/root?query=r.wf_id = 1')

        self.assertEqual(rv.status_code, 200)
        self.assertEqual(rv.content_type.lower(), 'application/json')

        root_workflows = self.read_json_response(rv)

        self.assertEqual(root_workflows['records'][0]['wf_id'], 1)

    def test_query_without_prefix(self):
        rv = self.app.get("/api/v1/user/mayani/root?query=submit_hostname like '%.edu'&order=wf_id ASC")

        self.assertEqual(rv.status_code, 200)
        self.assertEqual(rv.content_type.lower(), 'application/json')

        root_workflows = self.read_json_response(rv)

        self.assertEqual(root_workflows['records'][0]['wf_id'], 1)

    def test_complex_query(self):
        rv = self.app.get('/api/v1/user/mayani/root?query=r.wf_id = 1 OR (wf_id = 2 AND grid_dn = NULL)&order=wf_id asc')

        self.assertEqual(rv.status_code, 200)
        self.assertEqual(rv.content_type.lower(), 'application/json')

        root_workflows = self.read_json_response(rv)

        self.assertEqual(len(root_workflows['records']), 2)
        self.assertEqual(root_workflows['records'][0]['wf_id'], 1)
        self.assertEqual(root_workflows['records'][1]['wf_id'], 2)

    def test_ambiguous_query(self):
        rv = self.app.get('/api/v1/user/mayani/root?query=timestamp > 1000.0&order=wf_id asc')

        self.assertEqual(rv.status_code, 400)

    def test_order(self):
        rv = self.app.get('/api/v1/user/mayani/root?order=r.wf_id desc')

        self.assertEqual(rv.status_code, 200)
        self.assertEqual(rv.content_type.lower(), 'application/json')

        root_workflows = self.read_json_response(rv)

        self.assertEqual(root_workflows['records'][0]['wf_id'], 5)

    def test_bad_order(self):
        rv = self.app.get('/api/v1/user/mayani/root?order=r.wf_id des')

        self.assertEqual(rv.status_code, 400)

    def test_start_index(self):
        rv = self.app.get('/api/v1/user/mayani/root?start-index=1')

        self.assertEqual(rv.status_code, 200)
        self.assertEqual(rv.content_type.lower(), 'application/json')

        root_workflows = self.read_json_response(rv)

        self.assertEqual(len(root_workflows['records']), 4)

    def test_bad_start_index(self):
        rv = self.app.get('/api/v1/user/mayani/root?start-index=AAA')

        self.assertEqual(rv.status_code, 400)

    def test_max_results(self):
        rv = self.app.get('/api/v1/user/mayani/root?max-results=2')

        self.assertEqual(rv.status_code, 200)
        self.assertEqual(rv.content_type.lower(), 'application/json')

        root_workflows = self.read_json_response(rv)

        self.assertEqual(len(root_workflows['records']), 2)

    def test_bad_max_results(self):
        rv = self.app.get('/api/v1/user/mayani/root?max-results=AAA')

        self.assertEqual(rv.status_code, 400)

    def test_paging(self):
        rv = self.app.get('/api/v1/user/mayani/root?start-index=1&max-results=2')

        self.assertEqual(rv.status_code, 200)
        self.assertEqual(rv.content_type.lower(), 'application/json')

        root_workflows = self.read_json_response(rv)

        self.assertEqual(len(root_workflows['records']), 2)

    def test_get_root_workflow_id(self):
        rv = self.app.get('/api/v1/user/mayani/root/1')

        self.assertEqual(rv.status_code, 200)
        self.assertEqual(rv.content_type.lower(), 'application/json')

        root_workflow = self.read_json_response(rv)

        self.assertTrue(root_workflow['_links'])
        self.assertTrue(root_workflow['wf_id'], 1)

    def test_get_root_workflow_uuid(self):
        rv = self.app.get('/api/v1/user/mayani/root/7193de8c-a28d-4eca-b576-1b1c3c4f668b')

        self.assertEqual(rv.status_code, 200)
        self.assertEqual(rv.content_type.lower(), 'application/json')

        root_workflow = self.read_json_response(rv)

        self.assertTrue(root_workflow['_links'])
        self.assertTrue(root_workflow['wf_uuid'], '7193de8c-a28d-4eca-b576-1b1c3c4f668b')

    def test_get_missing_root_workflow(self):
        rv = self.app.get('/api/v1/user/mayani/root/1000000000')

        self.assertEqual(rv.status_code, 404)
