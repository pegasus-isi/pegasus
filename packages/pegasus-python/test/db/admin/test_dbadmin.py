import os
import shutil
import uuid

import pytest

from Pegasus.db import connection
from Pegasus.db.admin.admin_loader import *
from Pegasus.db.admin.admin_loader import get_version
from Pegasus.db.schema import *


def test_create_database():
    dburi = "sqlite://"

    db = connection.connect(dburi, create=True, verbose=False)
    assert get_version(db) == CURRENT_DB_VERSION

    db.execute("DROP TABLE dbversion")
    with pytest.raises(DBAdminError):
        db_verify(db, check=True)
    db.close()

    db = connection.connect(dburi, create=True, verbose=False)
    assert get_version(db) == CURRENT_DB_VERSION

    db.execute("DELETE FROM dbversion")
    db.close()

    db = connection.connect(dburi, create=True, verbose=False)
    assert get_version(db) == CURRENT_DB_VERSION

    db.execute("DROP TABLE rc_pfn")
    with pytest.raises(DBAdminError):
        db_verify(db, check=True)
    db.close()

    db = connection.connect(dburi, create=True, verbose=False)
    assert get_version(db) == CURRENT_DB_VERSION

    db.execute("DROP TABLE rc_pfn")
    db.execute("DROP TABLE master_workflow")
    with pytest.raises(DBAdminError):
        db_verify(db, check=True)
    with pytest.raises(DBAdminError):
        db_verify(db, check=True), "4.3.0"
    db.close()

    db = connection.connect(dburi, create=True, verbose=False)
    assert get_version(db) == CURRENT_DB_VERSION
    db.close()


@pytest.mark.parametrize(
    "input, expected",
    [(None, CURRENT_DB_VERSION), ("", CURRENT_DB_VERSION), ("4.3.0", 1)],
)
def test_parse_pegasus_version(input, expected):
    assert parse_pegasus_version(input) == expected


@pytest.mark.parametrize(
    "input", [0, 1, 4.5, "1.2.3", "a.b.c", "4.3.a", "4.3", "4"],
)
def test_parse_pegasus_version_fail(input):
    with pytest.raises(DBAdminError):
        parse_pegasus_version(input)


def test_version_operations():
    dburi = "sqlite://"
    db = connection.connect(dburi, create=True, verbose=False)

    db_downgrade(db, pegasus_version="4.7.0", verbose=False)
    assert get_version(db) == 8
    with pytest.raises(DBAdminError):
        db_verify(db, check=True)
    RCLFN.__table__._set_parent(metadata)
    db.close()

    db = connection.connect(dburi, create=True, verbose=False)
    assert get_version(db) == CURRENT_DB_VERSION
    db.close()

    dburi2 = "sqlite://"
    db2 = connection.connect(dburi2, create=True, verbose=False)
    db2.close()


def test_partial_database():
    dburi = "sqlite://"
    db = connection.connect(dburi, schema_check=False, create=False, verbose=False)
    RCLFN.__table__.create(db.get_bind(), checkfirst=True)
    RCPFN.__table__.create(db.get_bind(), checkfirst=True)
    RCMeta.__table__.create(db.get_bind(), checkfirst=True)
    with pytest.raises(DBAdminError):
        db_verify(db, check=True)
    db.close()

    db = connection.connect(dburi, create=True, verbose=False)
    assert get_version(db) == CURRENT_DB_VERSION
    db.close()

    db = connection.connect(dburi, schema_check=False, create=False, verbose=False)
    Workflow.__table__.create(db.get_bind(), checkfirst=True)
    DashboardWorkflowstate.__table__.create(db.get_bind(), checkfirst=True)
    Ensemble.__table__.create(db.get_bind(), checkfirst=True)
    EnsembleWorkflow.__table__.create(db.get_bind(), checkfirst=True)
    with pytest.raises(DBAdminError):
        db_verify(db, check=True)
    db.close()

    db = connection.connect(dburi, create=True, verbose=False)
    assert get_version(db) == CURRENT_DB_VERSION
    db.close()

    db = connection.connect(dburi, schema_check=False, create=False, verbose=False)
    Workflow.__table__.create(db.get_bind(), checkfirst=True)
    Workflowstate.__table__.create(db.get_bind(), checkfirst=True)
    Host.__table__.create(db.get_bind(), checkfirst=True)
    Job.__table__.create(db.get_bind(), checkfirst=True)
    JobEdge.__table__.create(db.get_bind(), checkfirst=True)
    JobInstance.__table__.create(db.get_bind(), checkfirst=True)
    Jobstate.__table__.create(db.get_bind(), checkfirst=True)
    Task.__table__.create(db.get_bind(), checkfirst=True)
    TaskEdge.__table__.create(db.get_bind(), checkfirst=True)
    Invocation.__table__.create(db.get_bind(), checkfirst=True)
    with pytest.raises(DBAdminError):
        db_verify(db, check=True)
    db.close()

    db = connection.connect(dburi, create=True, verbose=False)
    assert get_version(db) == CURRENT_DB_VERSION
    db.close()


def test_malformed_db():
    dburi = "sqlite://"
    db = connection.connect(dburi, create=True, verbose=False)
    assert get_version(db) == CURRENT_DB_VERSION
    db.execute("DROP TABLE rc_pfn")
    with pytest.raises(DBAdminError):
        db_verify(db, check=True)
    db.close()

    db = connection.connect(dburi, create=True, verbose=False)
    assert get_version(db) == CURRENT_DB_VERSION
    db.close()


def test_connection_from_properties_file(tmp_path):
    """
    Test whether DB connections are being established from properties loaded from the properties file.
    """
    props_filename = str(uuid.uuid4())
    dburi = "sqlite://"

    with open(props_filename, "w") as f:
        # JDBCRC
        f.write("pegasus.catalog.replica=JDBCRC\n")
        f.write("pegasus.catalog.replica.db.driver=SQLite\n")
        f.write("pegasus.catalog.replica.db.url=jdbc:sqlite::memory:\n")
        # MASTER
        f.write("pegasus.dashboard.output=%s\n" % dburi)
        # WORKFLOW
        f.write("pegasus.monitord.output=%s\n" % dburi)

    db = connection.connect_by_properties(
        props_filename, connection.DBType.JDBCRC, create=True, verbose=False
    )
    assert get_version(db) == CURRENT_DB_VERSION
    db.close()

    db = connection.connect_by_properties(
        props_filename, connection.DBType.MASTER, create=True, verbose=False
    )
    assert get_version(db) == CURRENT_DB_VERSION
    db.close()

    db = connection.connect_by_properties(
        props_filename, connection.DBType.WORKFLOW, create=True, verbose=False
    )
    assert get_version(db) == CURRENT_DB_VERSION
    db.close()


def test_upper_version():
    """
    Test whether DBs created with newer Pegasus version raises an exception.
    """
    dburi = "sqlite://"
    db = connection.connect(dburi, create=True, verbose=False)
    dbversion = DBVersion()
    dbversion.version = CURRENT_DB_VERSION + 1
    dbversion.version_number = CURRENT_DB_VERSION + 1
    dbversion.version_timestamp = (
        datetime.datetime.now() + datetime.timedelta(seconds=3)
    ).strftime("%s")
    db.add(dbversion)
    db.commit()

    with pytest.raises(DBAdminError):
        get_version(db)
    with pytest.raises(DBAdminError):
        db_verify(db, check=True)

    db.close()


@pytest.mark.parametrize("input", ["test-01.db", "test-02.db"])
def test_dbs(input, tmp_path):
    orig_filename = os.path.dirname(os.path.abspath(__file__)) + "/input/" + input
    filename = str(uuid.uuid4())
    shutil.copyfile(orig_filename, filename)
    dburi = "sqlite:///%s" % filename

    db = connection.connect(dburi, create=False, schema_check=False, verbose=False)
    with pytest.raises(DBAdminError):
        db_verify(db, check=True)
    db.close()

    db = connection.connect(dburi, create=True, verbose=False)
    assert get_version(db) == CURRENT_DB_VERSION
    db.close()
