import datetime

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import Pegasus.db.schema as schema
from Pegasus.db.ensembles import EMError, Triggers, TriggerType


@pytest.fixture(scope="function")
def session():
    """
    Create in-memory sqlite database with tables setup and return a db session
    object.
    """
    engine = create_engine("sqlite://")

    # create all tables in the schema
    schema.Base.metadata.create_all(engine)

    session = sessionmaker(bind=engine)()

    # create an ensemble entry
    session.add(
        schema.Ensemble(
            name="test-ensemble",
            created=datetime.datetime.now(),
            updated=datetime.datetime.now(),
            state="ACTIVE",
            max_running=1,
            max_planning=1,
            username="test-user",
        )
    )

    session.commit()

    yield session

    # close session, db will be released
    session.close()


class TestTriggers:
    def test_get_trigger(self, session):
        # insert trigger
        t = schema.Trigger(
            _id=1,
            ensemble_id=1,
            name="test-trigger",
            state="STOPPED",
            workflow=r'{"script":"/wf.py", "args":["arg1"]}',
            args=r'{"timeout":100, "interval":20}',
            _type=TriggerType.CRON.value,
        )
        session.add(t)

        triggers = Triggers(session)

        expected = {
            "id": 1,
            "ensemble_id": 1,
            "name": "test-trigger",
            "state": "STOPPED",
            "workflow": {"script": "/wf.py", "args": ["arg1"]},
            "args": {"timeout": 100, "interval": 20},
            "type": "CRON",
        }

        # get trigger and convert to dict for comparison
        result = Triggers.get_object(triggers.get_trigger(1, "test-trigger"))

        assert expected == result

    def test_get_trigger_not_found(self, session):
        with pytest.raises(EMError) as e:
            Triggers(session).get_trigger(1, "test-trigger")

        assert "No such trigger: test-trigger" in str(e)
        assert e.value.status_code == 404

    def test_list_triggers(self, session):
        t1 = schema.Trigger(
            _id=1,
            ensemble_id=1,
            name="test-trigger1",
            state="READY",
            workflow=r'{"script":"/wf.py", "args":["arg1"]}',
            args=r'{"timeout":100, "interval":20}',
            _type=TriggerType.CRON.value,
        )
        session.add(t1)

        t2 = schema.Trigger(
            _id=2,
            ensemble_id=1,
            name="test-trigger2",
            state="READY",
            workflow=r'{"script":"/wf.py", "args":["arg1"]}',
            args=r'{"timeout":100, "interval":20}',
            _type=TriggerType.CRON.value,
        )
        session.add(t2)
        session.commit()

        triggers = Triggers(session)
        result = triggers.list_triggers()
        assert len(result) == 2

    def test_list_triggers_by_ensemble(self, session):
        # add another ensemble to the ensemble table
        session.add(
            schema.Ensemble(
                id=2,
                name="test-ensemble2",
                created=datetime.datetime.now(),
                updated=datetime.datetime.now(),
                state="ACTIVE",
                max_running=1,
                max_planning=1,
                username="test-user",
            )
        )
        session.commit()

        # add a trigger assigned to test-ensemble2
        t = schema.Trigger(
            _id=1,
            ensemble_id=2,
            name="test-trigger1",
            state="READY",
            workflow=r'{"script":"/wf.py", "args":["arg1"]}',
            args=r'{"timeout":100, "interval":20}',
            _type=TriggerType.CRON.value,
        )
        session.add(t)
        session.commit()

        triggers = Triggers(session)
        result = triggers.list_triggers_by_ensemble(
            username="test-user", ensemble="test-ensemble2"
        )

        assert len(result) == 1
        assert Triggers.get_object(result[0]) == {
            "id": 1,
            "ensemble_id": 2,
            "name": "test-trigger1",
            "state": "READY",
            "workflow": {"script": "/wf.py", "args": ["arg1"]},
            "args": {"timeout": 100, "interval": 20},
            "type": "CRON",
        }

        result = triggers.list_triggers_by_ensemble(
            username="test-user", ensemble="doesntexist"
        )
        assert len(result) == 0

    def test_insert_trigger(self, session):
        print(session.query(schema.Ensemble).all())
        triggers = Triggers(session)
        triggers.insert_trigger(
            ensemble_id=1,
            trigger="test-trigger",
            trigger_type=TriggerType.CRON.value,
            workflow_script="/wf.py",
            workflow_args=["arg1"],
            interval=10,
            timeout=20,
        )

        expected = {
            "id": 1,
            "ensemble_id": 1,
            "name": "test-trigger",
            "state": "READY",
            "workflow": {"script": "/wf.py", "args": ["arg1"]},
            "args": {"timeout": 20, "interval": 10},
            "type": "CRON",
        }

        result = Triggers.get_object(
            session.query(schema.Trigger)
            .filter_by(ensemble_id=1, name="test-trigger")
            .one()
        )

        assert expected == result

    def test_update_state(self, session):
        # insert trigger
        t = schema.Trigger(
            _id=1,
            ensemble_id=1,
            name="test-trigger",
            state="READY",
            workflow=r'{"script":"/wf.py", "args":["arg1"]}',
            args=r'{"timeout":100, "interval":20}',
            _type=TriggerType.CRON.value,
        )
        session.add(t)

        triggers = Triggers(session)
        triggers.update_state(ensemble_id=1, trigger_id=1, new_state="RUNNING")

        expected_state = "RUNNING"
        result = session.query(schema.Trigger).filter_by(_id=1).one().state

        assert expected_state == result

    def test_delete_trigger(self, session):
        # insert trigger
        t = schema.Trigger(
            _id=1,
            ensemble_id=1,
            name="test-trigger",
            state="READY",
            workflow=r'{"script":"/wf.py", "args":["arg1"]}',
            args=r'{"timeout":100, "interval":20}',
            _type=TriggerType.CRON.value,
        )
        session.add(t)
        assert len(session.query(schema.Trigger).all()) == 1

        triggers = Triggers(session)

        # delete trigger
        triggers.delete_trigger(ensemble_id=1, trigger="test-trigger")

        assert len(session.query(schema.Trigger).all()) == 0

    def test_get_object(self, session):
        t = schema.Trigger(
            _id=1,
            ensemble_id=1,
            name="test-trigger",
            state="READY",
            workflow=r'{"script":"/wf.py", "args":["arg1"]}',
            args=r'{"timeout":100, "interval":20}',
            _type=TriggerType.CRON.value,
        )

        expected = {
            "id": 1,
            "ensemble_id": 1,
            "name": "test-trigger",
            "state": "READY",
            "workflow": {"script": "/wf.py", "args": ["arg1"]},
            "args": {"timeout": 100, "interval": 20},
            "type": "CRON",
        }

        result = Triggers.get_object(t)

        assert expected == result
