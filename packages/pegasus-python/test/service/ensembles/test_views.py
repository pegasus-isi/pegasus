import datetime
import json
import os
import pwd

import pytest
from flask import g
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

import Pegasus.db.schema as schema
from Pegasus.db.ensembles import EMError, Triggers, TriggerType
from Pegasus.service.ensembles.views import to_seconds


class NoAuthFlaskTestCase:
    @pytest.fixture(autouse=True)
    def init(self, emapp):
        self.user = pwd.getpwuid(os.getuid()).pw_name

    @staticmethod
    def pre_callable():

        # create in-mem db
        engine = create_engine("sqlite://")

        # create all tables in schema
        schema.Base.metadata.create_all(engine)

        g.session = Session(engine)

        # create an ensemble entry
        g.session.add(
            schema.Ensemble(
                name="test-ensemble",
                created=datetime.datetime.now(),
                updated=datetime.datetime.now(),
                state="ACTIVE",
                max_running=1,
                max_planning=1,
                username=pwd.getpwuid(os.getuid()).pw_name,
            )
        )

        g.session.commit()

        # create a trigger entry
        g.session.add(
            schema.Trigger(
                ensemble_id=1,
                name="test-trigger",
                state="READY",
                workflow=r'{"script":"/wf.py", "args":["arg1"]}',
                _type=TriggerType.CRON.value,
            )
        )

        g.session.commit()


class TestTriggerRoutes(NoAuthFlaskTestCase):
    @pytest.mark.parametrize(
        "ensemble,expected",
        [
            (
                "test-ensemble",
                [
                    {
                        "args": None,
                        "ensemble_id": 1,
                        "id": 1,
                        "name": "test-trigger",
                        "state": "READY",
                        "type": "CRON",
                        "workflow": {"args": ["arg1"], "script": "/wf.py"},
                    }
                ],
            ),
            ("nonexistent-ensemble", []),
        ],
    )
    def test_list_triggers(self, mocker, emapp_client, ensemble, expected):
        # patch so that the in-mem db defined in pre_callable can be used
        # and also to avoid a db version check done in Pegasus.db.connection.connect
        mocker.patch("Pegasus.db.connection.connect")

        rv = emapp_client.get_context(
            f"/ensembles/{ensemble}/triggers",
            pre_callable=self.pre_callable,
        )

        assert rv.status_code == 200
        assert rv.json == expected

    def test_get_route(self, emapp_client):
        pass

    def test_route_create_cron_trigger(self, mocker, emapp_client, emapp):
        # patch so that the in-mem db defined in pre_callable can be used
        # and also to avoid a db version check done in Pegasus.db.connection.connect
        mocker.patch("Pegasus.db.connection.connect")

        request_data = {
            "trigger": "test-trigger2",
            "workflow_script": "/workflow.py",
            "workflow_args": json.dumps([]),
            "interval": "10s",
            "timeout": "20s",
            "type": TriggerType.CRON.value,
        }

        rv = emapp_client.post_context(
            "/ensembles/test-ensemble/triggers/cron",
            pre_callable=self.pre_callable,
            data=request_data,
        )

        assert rv.status_code == 201
        assert rv.json == {"message": "created"}
        assert rv.location == "/ensembles/test-ensemble/triggers/test-trigger2"

        db_entry = (
            g.session.execute(select(schema.Trigger).where(schema.Trigger._id == 2))
            .scalars()
            .one()
        )
        assert Triggers.get_object(db_entry) == {
            "id": 2,
            "ensemble_id": 1,
            "name": "test-trigger2",
            "state": "READY",
            "workflow": {"script": "/workflow.py", "args": []},
            "args": {
                "interval": 10,
                "timeout": 20,
            },
            "type": "CRON",
        }

    @pytest.mark.parametrize(
        "ensemble,request_data,exception_msg,status_code",
        [
            (
                "nonexistent-ensemble",
                {
                    "trigger": "test-trigger2",
                    "workflow_script": "/workflow.py",
                    "workflow_args": json.dumps([]),
                    "interval": "10s",
                    "timeout": "20s",
                    "type": TriggerType.CRON.value,
                },
                "No such ensemble: nonexistent-ensemble",
                404,
            ),
            (
                "test-ensemble",
                {
                    "trigger": "",
                    "workflow_script": "/workflow.py",
                    "workflow_args": json.dumps([]),
                    "interval": "10s",
                    "timeout": "20s",
                    "type": TriggerType.CRON.value,
                },
                "trigger name must be a non-empty string",
                500,
            ),
            (
                "test-ensemble",
                {
                    "trigger": "test-trigger",
                    "workflow_script": "",
                    "workflow_args": json.dumps([]),
                    "interval": "10s",
                    "timeout": "20s",
                    "type": TriggerType.CRON.value,
                },
                "workflow_script name must be a non-empty string",
                500,
            ),
            (
                "test-ensemble",
                {
                    "trigger": "test-trigger",
                    "workflow_script": "workflow.py",
                    "workflow_args": json.dumps([]),
                    "interval": "10s",
                    "timeout": "20s",
                    "type": TriggerType.CRON.value,
                },
                "workflow_script must be given as an absolute path",
                500,
            ),
            (
                "test-ensemble",
                {
                    "trigger": "test-trigger",
                    "workflow_script": "/workflow.py",
                    "workflow_args": "badjson",
                    "interval": "10s",
                    "timeout": "20s",
                    "type": TriggerType.CRON.value,
                },
                "workflow_args must be given as a list serialized to json",
                500,
            ),
            (
                "test-ensemble",
                {
                    "trigger": "test-trigger",
                    "workflow_script": "/workflow.py",
                    "workflow_args": json.dumps({"a": 1}),
                    "interval": "10s",
                    "timeout": "20s",
                    "type": TriggerType.CRON.value,
                },
                "workflow_args must be given as a list serialized to json",
                500,
            ),
            (
                "test-ensemble",
                {
                    "trigger": "test-trigger",
                    "workflow_script": "/workflow.py",
                    "workflow_args": "[]",
                    "interval": "0s",
                    "timeout": "20s",
                    "type": TriggerType.CRON.value,
                },
                "interval must be given as `<int> <s|m|h|d>` and be greater than 0 seconds",
                500,
            ),
            (
                "test-ensemble",
                {
                    "trigger": "test-trigger",
                    "workflow_script": "/workflow.py",
                    "workflow_args": "[]",
                    "interval": "10s",
                    "timeout": "0s",
                    "type": TriggerType.CRON.value,
                },
                "timeout must be given as `<int> <s|m|h|d>` and be greater than 0 seconds",
                500,
            ),
        ],
    )
    def test_route_create_cron_trigger_invalid_request(
        self, mocker, emapp_client, ensemble, request_data, exception_msg, status_code
    ):
        # patch so that the in-mem db defined in pre_callable can be used
        # and also to avoid a db version check done in Pegasus.db.connection.connect
        mocker.patch("Pegasus.db.connection.connect")

        rv = emapp_client.post_context(
            f"/ensembles/{ensemble}/triggers/cron",
            pre_callable=self.pre_callable,
            data=request_data,
        )

        assert rv.status_code == status_code
        assert exception_msg in rv.json["message"]

    def test_route_create_file_pattern_trigger(self, mocker, emapp_client, emapp):
        # patch so that the in-mem db defined in pre_callable can be used
        # and also to avoid a db version check done in Pegasus.db.connection.connect
        mocker.patch("Pegasus.db.connection.connect")

        request_data = {
            "trigger": "test-trigger2",
            "workflow_script": "/workflow.py",
            "workflow_args": json.dumps(["arg1"]),
            "interval": "10s",
            "timeout": "20s",
            "file_patterns": json.dumps(["/*.txt", "/*.csv"]),
            "type": TriggerType.FILE_PATTERN.value,
        }

        rv = emapp_client.post_context(
            "/ensembles/test-ensemble/triggers/file_pattern",
            pre_callable=self.pre_callable,
            data=request_data,
        )

        assert rv.status_code == 201
        assert rv.json == {"message": "created"}
        assert rv.location == "/ensembles/test-ensemble/triggers/test-trigger2"

        db_entry = (
            g.session.execute(select(schema.Trigger).where(schema.Trigger._id == 2))
            .scalars()
            .one()
        )
        assert Triggers.get_object(db_entry) == {
            "id": 2,
            "ensemble_id": 1,
            "name": "test-trigger2",
            "state": "READY",
            "workflow": {"script": "/workflow.py", "args": ["arg1"]},
            "args": {
                "interval": 10,
                "timeout": 20,
                "file_patterns": ["/*.txt", "/*.csv"],
            },
            "type": "FILE_PATTERN",
        }

    @pytest.mark.parametrize(
        "ensemble,request_data,exception_msg,status_code",
        [
            (
                "nonexistent-ensemble",
                {
                    "trigger": "test-trigger2",
                    "workflow_script": "/workflow.py",
                    "workflow_args": json.dumps([]),
                    "interval": "10s",
                    "timeout": "20s",
                    "file_patterns": json.dumps(["/*.txt"]),
                    "type": TriggerType.FILE_PATTERN.value,
                },
                "No such ensemble: nonexistent-ensemble",
                404,
            ),
            (
                "test-ensemble",
                {
                    "trigger": "",
                    "workflow_script": "/workflow.py",
                    "workflow_args": json.dumps([]),
                    "interval": "10s",
                    "timeout": "20s",
                    "file_patterns": json.dumps(["/*.txt"]),
                    "type": TriggerType.FILE_PATTERN.value,
                },
                "trigger name must be a non-empty string",
                500,
            ),
            (
                "test-ensemble",
                {
                    "trigger": "test-trigger",
                    "workflow_script": "",
                    "workflow_args": json.dumps([]),
                    "interval": "10s",
                    "timeout": "20s",
                    "file_patterns": json.dumps(["/*.txt"]),
                    "type": TriggerType.FILE_PATTERN.value,
                },
                "workflow_script name must be a non-empty string",
                500,
            ),
            (
                "test-ensemble",
                {
                    "trigger": "test-trigger",
                    "workflow_script": "workflow.py",
                    "workflow_args": json.dumps([]),
                    "interval": "10s",
                    "timeout": "20s",
                    "file_patterns": json.dumps(["/*.txt"]),
                    "type": TriggerType.FILE_PATTERN.value,
                },
                "workflow_script must be given as an absolute path",
                500,
            ),
            (
                "test-ensemble",
                {
                    "trigger": "test-trigger",
                    "workflow_script": "/workflow.py",
                    "workflow_args": "badjson",
                    "interval": "10s",
                    "timeout": "20s",
                    "file_patterns": json.dumps(["/*.txt"]),
                    "type": TriggerType.FILE_PATTERN.value,
                },
                "workflow_args must be given as a list serialized to json",
                500,
            ),
            (
                "test-ensemble",
                {
                    "trigger": "test-trigger",
                    "workflow_script": "/workflow.py",
                    "workflow_args": json.dumps({"a": 1}),
                    "interval": "10s",
                    "timeout": "20s",
                    "file_patterns": json.dumps(["/*.txt"]),
                    "type": TriggerType.FILE_PATTERN.value,
                },
                "workflow_args must be given as a list serialized to json",
                500,
            ),
            (
                "test-ensemble",
                {
                    "trigger": "test-trigger",
                    "workflow_script": "/workflow.py",
                    "workflow_args": "[]",
                    "interval": "0s",
                    "timeout": "20s",
                    "file_patterns": json.dumps(["/*.txt"]),
                    "type": TriggerType.FILE_PATTERN.value,
                },
                "interval must be given as `<int> <s|m|h|d>` and be greater than 0 seconds",
                500,
            ),
            (
                "test-ensemble",
                {
                    "trigger": "test-trigger",
                    "workflow_script": "/workflow.py",
                    "workflow_args": "[]",
                    "interval": "10s",
                    "timeout": "0s",
                    "file_patterns": json.dumps(["/*.txt"]),
                    "type": TriggerType.FILE_PATTERN.value,
                },
                "timeout must be given as `<int> <s|m|h|d>` and be greater than 0 seconds",
                500,
            ),
            (
                "test-ensemble",
                {
                    "trigger": "test-trigger",
                    "workflow_script": "/workflow.py",
                    "workflow_args": "[]",
                    "interval": "10s",
                    "timeout": "20s",
                    "file_patterns": "abc123",
                    "type": TriggerType.FILE_PATTERN.value,
                },
                "file_patterns must be given as a list serialized to json",
                500,
            ),
            (
                "test-ensemble",
                {
                    "trigger": "test-trigger",
                    "workflow_script": "/workflow.py",
                    "workflow_args": "[]",
                    "interval": "10s",
                    "timeout": "20s",
                    "file_patterns": json.dumps([]),
                    "type": TriggerType.FILE_PATTERN.value,
                },
                "file_patterns must contain at least one file pattern",
                500,
            ),
            (
                "test-ensemble",
                {
                    "trigger": "test-trigger",
                    "workflow_script": "/workflow.py",
                    "workflow_args": "[]",
                    "interval": "10s",
                    "timeout": "20s",
                    "file_patterns": json.dumps(["not_abs_path.txt"]),
                    "type": TriggerType.FILE_PATTERN.value,
                },
                "each file pattern must be given as an absolute path",
                500,
            ),
        ],
    )
    def test_route_create_file_pattern_trigger_invalid_request(
        self, mocker, emapp_client, ensemble, request_data, exception_msg, status_code
    ):
        # patch so that the in-mem db defined in pre_callable can be used
        # and also to avoid a db version check done in Pegasus.db.connection.connect
        mocker.patch("Pegasus.db.connection.connect")

        rv = emapp_client.post_context(
            f"/ensembles/{ensemble}/triggers/file_pattern",
            pre_callable=self.pre_callable,
            data=request_data,
        )

        assert rv.status_code == status_code
        assert exception_msg in rv.json["message"]

    def test_route_delete_trigger(self, mocker, emapp_client):
        # patch so that the in-mem db defined in pre_callable can be used
        # and also to avoid a db version check done in Pegasus.db.connection.connect
        mocker.patch("Pegasus.db.connection.connect")

        rv = emapp_client.delete_context(
            "/ensembles/test-ensemble/triggers/test-trigger",
            pre_callable=self.pre_callable,
        )

        assert rv.status_code == 200
        assert (
            rv.json["message"]
            == "ensemble: test-ensemble, trigger: test-trigger deleted"
        )
        assert g.session.execute(select(schema.Trigger)).scalars().all() == []

    @pytest.mark.parametrize(
        "ensemble,trigger,exception_msg,status_code",
        [
            (
                "nonexistent-ensemble",
                "test-trigger",
                "No such ensemble: nonexistent-ensemble",
                404,
            ),
            (
                "test-ensemble",
                "nonexistent-trigger",
                "No such trigger: nonexistent-trigger assigned to ensemble",
                404,
            ),
        ],
    )
    def test_route_delete_nonexistent_trigger(
        self, mocker, emapp_client, ensemble, trigger, exception_msg, status_code
    ):
        # patch so that the in-mem db defined in pre_callable can be used
        # and also to avoid a db version check done in Pegasus.db.connection.connect
        mocker.patch("Pegasus.db.connection.connect")

        rv = emapp_client.delete_context(
            f"/ensembles/{ensemble}/triggers/{trigger}",
            pre_callable=self.pre_callable,
        )

        assert rv.status_code == status_code
        assert exception_msg in rv.json["message"]


class TestEnsembleRoutes(NoAuthFlaskTestCase):
    @staticmethod
    def add_workflow(state):
        def pre_callable():
            NoAuthFlaskTestCase.pre_callable()
            now = datetime.datetime.now(datetime.timezone.utc)
            g.session.add(
                schema.EnsembleWorkflow(
                    ensemble_id=1,
                    name="wf",
                    basedir="/tmp",
                    created=now,
                    updated=now,
                    state=state,
                    priority=0,
                    plan_command="./plan.sh",
                    submitdir="/tmp/submit",
                )
            )
            g.session.commit()

        return pre_callable

    @staticmethod
    def workflow():
        return g.session.execute(select(schema.EnsembleWorkflow)).scalars().one()

    @pytest.mark.parametrize(
        "state,removed",
        [("READY", 0), ("PLANNING", 0), ("QUEUED", 0), ("RUNNING", 1)],
    )
    def test_route_abort_workflow(self, mocker, emapp_client, state, removed):
        mocker.patch("Pegasus.db.connection.connect")
        remove = mocker.patch(
            "Pegasus.service.ensembles.manager.WorkflowProcessor.remove"
        )

        rv = emapp_client.post_context(
            "/ensembles/test-ensemble/workflows/wf",
            data={"state": "ABORTED"},
            pre_callable=self.add_workflow(state),
        )

        assert rv.status_code == 200
        assert self.workflow().state == "ABORTED"
        assert remove.call_count == removed

    @pytest.mark.parametrize("state", ["SUCCESSFUL", "FAILED", "ABORTED"])
    def test_route_abort_finished_workflow(self, mocker, emapp_client, state):
        mocker.patch("Pegasus.db.connection.connect")

        rv = emapp_client.post_context(
            "/ensembles/test-ensemble/workflows/wf",
            data={"state": "ABORTED"},
            pre_callable=self.add_workflow(state),
        )

        assert f"Cannot abort workflow in {state} state" in rv.json["message"]
        assert self.workflow().state == state

    def test_route_abort_remove_fails(self, mocker, emapp_client):
        mocker.patch("Pegasus.db.connection.connect")
        mocker.patch(
            "Pegasus.service.ensembles.manager.WorkflowProcessor.remove",
            side_effect=EMError("pegasus-remove failed"),
        )

        rv = emapp_client.post_context(
            "/ensembles/test-ensemble/workflows/wf",
            data={"state": "ABORTED"},
            pre_callable=self.add_workflow("RUNNING"),
        )

        assert "pegasus-remove failed" in rv.json["message"]
        assert self.workflow().state == "RUNNING"

    @pytest.mark.parametrize(
        "state,aborted,removed",
        [("RUNNING", ["wf"], 1), ("QUEUED", ["wf"], 0), ("SUCCESSFUL", [], 0)],
    )
    def test_route_pause_abort(self, mocker, emapp_client, state, aborted, removed):
        mocker.patch("Pegasus.db.connection.connect")
        remove = mocker.patch(
            "Pegasus.service.ensembles.manager.WorkflowProcessor.remove"
        )

        rv = emapp_client.post_context(
            "/ensembles/test-ensemble",
            data={"state": "PAUSED", "abort": "true"},
            pre_callable=self.add_workflow(state),
        )

        assert rv.status_code == 200
        assert rv.json["state"] == "PAUSED"
        assert rv.json["aborted"] == aborted
        assert remove.call_count == removed
        expected = "ABORTED" if aborted else state
        assert self.workflow().state == expected
        # triggers are left alone
        assert len(g.session.execute(select(schema.Trigger)).scalars().all()) == 1

    def test_route_abort_requires_pause(self, mocker, emapp_client):
        mocker.patch("Pegasus.db.connection.connect")

        rv = emapp_client.post_context(
            "/ensembles/test-ensemble",
            data={"state": "ACTIVE", "abort": "true"},
            pre_callable=self.add_workflow("RUNNING"),
        )

        assert rv.status_code == 400
        assert "Pause the ensemble" in rv.json["message"]
        assert self.workflow().state == "RUNNING"

    def test_route_pause_without_abort(self, mocker, emapp_client):
        mocker.patch("Pegasus.db.connection.connect")

        rv = emapp_client.post_context(
            "/ensembles/test-ensemble",
            data={"state": "PAUSED"},
            pre_callable=self.add_workflow("RUNNING"),
        )

        assert rv.status_code == 200
        assert "aborted" not in rv.json
        assert self.workflow().state == "RUNNING"

    @pytest.mark.parametrize("state,removed", [("PLANNING", 0), ("RUNNING", 1)])
    def test_route_force_delete_ensemble(self, mocker, emapp_client, state, removed):
        mocker.patch("Pegasus.db.connection.connect")
        remove = mocker.patch(
            "Pegasus.service.ensembles.manager.WorkflowProcessor.remove"
        )

        rv = emapp_client.delete_context(
            "/ensembles/test-ensemble?force=true",
            pre_callable=self.add_workflow(state),
        )

        assert rv.status_code == 200
        assert remove.call_count == removed
        assert g.session.execute(select(schema.Ensemble)).scalars().all() == []

    @pytest.mark.parametrize("state", ["READY", "QUEUED", "SUCCESSFUL", "FAILED"])
    def test_route_delete_ensemble(self, mocker, emapp_client, state):
        mocker.patch("Pegasus.db.connection.connect")

        rv = emapp_client.delete_context(
            "/ensembles/test-ensemble", pre_callable=self.add_workflow(state)
        )

        assert rv.status_code == 200
        assert rv.json["message"] == "ensemble: test-ensemble deleted"
        for table in (schema.Ensemble, schema.EnsembleWorkflow, schema.Trigger):
            assert g.session.execute(select(table)).scalars().all() == []

    @pytest.mark.parametrize("state", ["PLANNING", "RUNNING"])
    def test_route_delete_busy_ensemble(self, mocker, emapp_client, state):
        mocker.patch("Pegasus.db.connection.connect")

        rv = emapp_client.delete_context(
            "/ensembles/test-ensemble", pre_callable=self.add_workflow(state)
        )

        assert rv.status_code == 409
        assert "planning or running workflows: wf" in rv.json["message"]
        assert "--force" in rv.json["message"]
        assert len(g.session.execute(select(schema.Ensemble)).scalars().all()) == 1

    def test_route_delete_nonexistent_ensemble(self, mocker, emapp_client):
        mocker.patch("Pegasus.db.connection.connect")

        rv = emapp_client.delete_context(
            "/ensembles/nope", pre_callable=self.pre_callable
        )

        assert rv.status_code == 404
        assert "No such ensemble: nope" in rv.json["message"]


@pytest.mark.parametrize(
    "input,expected",
    [
        ("1s", 1),
        ("1 s", 1),
        ("10 m", 60 * 10),
        ("10h", 60 * 60 * 10),
        ("1d", 60 * 60 * 24),
        ("2 d", 60 * 60 * 24 * 2),
    ],
)
def test_to_seconds(input, expected):
    assert to_seconds(input) == expected


@pytest.mark.parametrize(
    "bad_input,exception_msg",
    [
        ("", "interval must be given as '<int> <s|m|h|d>'"),
        ("1", "interval must be given as '<int> <s|m|h|d>'"),
        ("1s h", "interval must be given as '<int> <s|m|h|d>'"),
        ("1x", "interval must be given as '<int> <s|m|h|d>'"),
        ("0s", "interval must be greater than 0 seconds"),
        ("0d", "interval must be greater than 0 seconds"),
    ],
)
def test_to_seconds_invalid_input(bad_input, exception_msg):
    with pytest.raises(ValueError) as e:
        to_seconds(bad_input)

    assert exception_msg in str(e)
