#!/usr/bin/env python
#
#  Copyright 2018-2020 University Of Southern California
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
#

__author__ = "Monte Goode"
__author__ = "Karan Vahi"
__author__ = "Rafael Ferreira da Silva"
__author__ = "Rajiv Mayani"

import logging
import time
import warnings

from sqlalchemy.dialects import mysql, postgresql, sqlite
from sqlalchemy.exc import OperationalError, ProgrammingError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import foreign, mapper, relation
from sqlalchemy.schema import Column, ForeignKey, Index, MetaData, UniqueConstraint
from sqlalchemy.sql.expression import and_
from sqlalchemy.types import (
    BigInteger,
    Boolean,
    DateTime,
    Enum,
    Integer,
    Numeric,
    String,
    Text,
)

from Pegasus.db.ensembles import Ensemble as _Ensemble
from Pegasus.db.ensembles import EnsembleStates
from Pegasus.db.ensembles import EnsembleWorkflow as _EnsembleWorkflow
from Pegasus.db.ensembles import EnsembleWorkflowStates, TriggerStates, TriggerType

__all__ = (
    "DBVersion",
    "Workflow",
    "Workflowstate",
    "WorkflowMeta",
    "Host",
    "Job",
    "JobEdge",
    "JobInstance",
    "Jobstate",
    "Tag",
    "Task",
    "TaskEdge",
    "TaskMeta",
    "Invocation",
    "WorkflowFiles",
    "IntegrityMetrics",
    "MasterWorkflow",
    "MasterWorkflowstate",
    "Ensemble",
    "EnsembleWorkflow",
    "Trigger",
    "RCLFN",
    "RCPFN",
    "RCMeta",
)

log = logging.getLogger(__name__)

# for SQLite
warnings.filterwarnings("ignore", r".*does \*not\* support Decimal*.")

# These are keywords that all tables should have
table_keywords = {"mysql_charset": "utf8mb4", "mysql_engine": "InnoDB"}

KeyInteger = BigInteger()
KeyInteger = KeyInteger.with_variant(postgresql.BIGINT(), "postgresql")
KeyInteger = KeyInteger.with_variant(mysql.BIGINT(), "mysql")
KeyInteger = KeyInteger.with_variant(sqlite.INTEGER(), "sqlite")

TimestampType = Numeric(precision=16, scale=6)
DurationType = Numeric(precision=10, scale=3)


# --------------------------------------------------------------------


class SABase:
    """
    Base class for all the DB mapper objects.
    """

    def _commit(self, session, batch, merge=False):
        if merge:
            session.merge(self)
        else:
            session.add(self)
        if batch:
            return
        session.flush()
        session.commit()

    def commit_to_db(self, session, batch=False):
        """
        Commit the DB object/row to the database.

        @type   session: sqlalchemy.orm.scoping.ScopedSession object
        @param  session: SQLAlch session to commit row to.
        """
        self._commit(session, batch)

    def merge_to_db(self, session, batch=False):
        """
        Merge the DB object/row with an existing row in the database.

        @type   session: sqlalchemy.orm.scoping.ScopedSession object
        @param  session: SQLAlch session to merge row with.

        Using this method pre-supposes that the developer has already
        assigned any primary key information to the object before
        calling.
        """
        self._commit(session, batch, merge=True)

    def __repr__(self):
        retval = "%s:\n" % self.__class__
        for k, v in self.__dict__.items():
            if k == "_sa_instance_state":
                continue
            retval += "  * {} : {}\n".format(k, v)
        return retval


metadata = MetaData()
Base = declarative_base(cls=SABase, metadata=metadata)


# ----------------------------------------------------------------
# Method to verify if tables exist or are according to the schema
# ----------------------------------------------------------------


def get_missing_tables(db):
    tables = (
        DBVersion,
        # WORKFLOW
        Workflow,
        Workflowstate,
        WorkflowMeta,
        WorkflowFiles,
        Host,
        Job,
        JobEdge,
        JobInstance,
        Jobstate,
        Tag,
        Task,
        TaskEdge,
        TaskMeta,
        Invocation,
        IntegrityMetrics,
        # MASTER
        MasterWorkflow,
        MasterWorkflowstate,
        Ensemble,
        EnsembleWorkflow,
        # JDBCRC
        RCLFN,
        RCPFN,
        RCMeta,
    )
    missing_tables = []
    for table in tables:
        if not check_table_exists(db, table):
            missing_tables.append(table.__tablename__)

    return missing_tables


def check_table_exists(engine, table):
    try:
        engine.execute(table.__table__.select().limit(1))
        return True

    except OperationalError as e:
        if (
            "no such table" in str(e).lower()
            or "unknown" in str(e).lower()
            or "no such column" in str(e).lower()
        ):
            return False
        raise
    except ProgrammingError as e:
        if "doesn't exist" in str(e).lower():
            return False
        raise


# ---------------------------------------------
# DB ADMIN
# ---------------------------------------------


class DBVersion(Base):
    """."""

    __tablename__ = "dbversion"
    __table_args__ = (
        {
            "mysql_charset": "utf8mb4",
            "mysql_engine": "InnoDB",
            "sqlite_autoincrement": True,
        },
    )

    id = Column("id", KeyInteger, primary_key=True, autoincrement=True)
    version_number = Column("version_number", Integer, default=5)
    version = Column("version", String(50), nullable=False)
    version_timestamp = Column("version_timestamp", Integer, nullable=False)


# ---------------------------------------------
# STAMPEDE
# ---------------------------------------------


class Workflow(Base):
    """."""

    __tablename__ = "workflow"

    # ==> Information comes from braindump.txt file
    wf_id = Column("wf_id", KeyInteger, primary_key=True)
    wf_uuid = Column("wf_uuid", String(255), nullable=False)
    dag_file_name = Column("dag_file_name", String(255))
    timestamp = Column("timestamp", TimestampType)
    submit_hostname = Column("submit_hostname", String(255))
    submit_dir = Column("submit_dir", Text)
    planner_arguments = Column("planner_arguments", Text)
    user = Column("user", String(255))
    grid_dn = Column("grid_dn", String(255))
    planner_version = Column("planner_version", String(255))
    dax_label = Column("dax_label", String(255))
    dax_version = Column("dax_version", String(255))
    dax_file = Column("dax_file", String(255))
    db_url = Column("db_url", Text)
    parent_wf_id = Column(
        "parent_wf_id", KeyInteger, ForeignKey(wf_id, ondelete="CASCADE"),
    )
    # not marked as FK to not screw up the cascade.
    root_wf_id = Column("root_wf_id", KeyInteger)

    # Relationships
    root_wf = relation(
        lambda: Workflow,
        cascade="all, delete-orphan",
        single_parent=True,
        remote_side=(wf_id,),
    )
    parent_wf = relation(
        lambda: Workflow,
        cascade="all, delete-orphan",
        single_parent=True,
        remote_side=(wf_id,),
    )
    states = relation(
        lambda: Workflowstate,
        backref="workflow",
        cascade="all, delete-orphan",
        passive_deletes=True,
        order_by=lambda: Workflowstate.timestamp,
    )
    jobs = relation(
        lambda: Job,
        backref="workflow",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    job_edges = relation(
        lambda: JobEdge,
        backref="workflow",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    tasks = relation(
        lambda: Task,
        backref="workflow",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    task_edges = relation(
        lambda: TaskEdge,
        backref="workflow",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    invocations = relation(
        lambda: Invocation,
        backref="workflow",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    hosts = relation(
        lambda: Host,
        backref="workflow",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    workflow_files = relation(
        lambda: WorkflowFiles,
        backref="workflow",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    files = relation(
        lambda: RCLFN,
        secondary=lambda: WorkflowFiles.__table__,
        primaryjoin=lambda: Workflow.wf_id == WorkflowFiles.wf_id,
        secondaryjoin=lambda: WorkflowFiles.lfn_id == RCLFN.lfn_id,
    )
    integrity_metrics = relation(
        lambda: IntegrityMetrics,
        backref="workflow",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    tags = relation(
        lambda: Tag,
        backref="workflow",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    meta = relation(
        lambda: WorkflowMeta,
        backref="workflow",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


Workflow.__table_args__ = (
    UniqueConstraint(Workflow.wf_uuid, name="UNIQUE_WF_UUID"),
    table_keywords,
)


class Workflowstate(Base):
    """."""

    __tablename__ = "workflowstate"
    __table_args__ = (table_keywords,)

    # All three columns are marked as primary key to produce the desired
    # effect - ie: it is the combo of the three columns that make a row
    # unique.
    wf_id = Column(
        "wf_id",
        KeyInteger,
        ForeignKey(Workflow.wf_id, ondelete="CASCADE"),
        primary_key=True,
    )
    state = Column(
        "state",
        Enum("WORKFLOW_STARTED", "WORKFLOW_TERMINATED", name="workflow_state"),
        primary_key=True,
    )
    timestamp = Column(
        "timestamp", TimestampType, primary_key=True, default=time.time(),
    )
    restart_count = Column("restart_count", Integer, nullable=False)
    status = Column("status", Integer)
    reason = Column("reason", Text)


Index("workflowstate_timestamp_COL", Workflowstate.timestamp)


class WorkflowMeta(Base):
    """."""

    __tablename__ = "workflow_meta"
    __table_args__ = (table_keywords,)

    wf_id = Column(
        "wf_id",
        KeyInteger,
        ForeignKey(Workflow.wf_id, ondelete="CASCADE"),
        primary_key=True,
    )
    key = Column("key", String(255), primary_key=True)
    value = Column("value", String(255), nullable=False)


# Host definition
# ==> Information from kickstart output file
#
# site_name = <resource, from invocation element>
# hostname = <hostname, from invocation element>
# ip_address = <hostaddr, from invocation element>
# uname = <combined (system, release, machine) from machine element>
# total_ram = <ram_total from machine element>


class Host(Base):
    """."""

    __tablename__ = "host"

    host_id = Column("host_id", KeyInteger, primary_key=True)
    wf_id = Column(
        "wf_id",
        KeyInteger,
        ForeignKey(Workflow.wf_id, ondelete="CASCADE"),
        nullable=False,
    )
    site = Column("site", String(255), nullable=False)
    hostname = Column("hostname", String(255), nullable=False)
    ip = Column("ip", String(255), nullable=False)
    uname = Column("uname", String(255))
    total_memory = Column("total_memory", Integer)


Host.__table_args__ = (
    UniqueConstraint(Host.wf_id, Host.site, Host.hostname, Host.ip, name="UNIQUE_HOST"),
    table_keywords,
)


class Job(Base):
    """."""

    __tablename__ = "job"

    job_id = Column("job_id", KeyInteger, primary_key=True)
    wf_id = Column(
        "wf_id",
        KeyInteger,
        ForeignKey(Workflow.wf_id, ondelete="CASCADE"),
        nullable=False,
    )
    exec_job_id = Column("exec_job_id", String(255), nullable=False)
    submit_file = Column("submit_file", String(255), nullable=False)
    type_desc = Column(
        "type_desc",
        Enum(
            "unknown",
            "compute",
            "stage-in-tx",
            "stage-out-tx",
            "registration",
            "inter-site-tx",
            "create-dir",
            "staged-compute",
            "cleanup",
            "chmod",
            "dax",
            "dag",
            name="job_type_desc",
        ),
        nullable=False,
    )
    clustered = Column("clustered", Boolean, nullable=False)
    max_retries = Column("max_retries", Integer, nullable=False)
    executable = Column("executable", Text, nullable=False)
    argv = Column("argv", Text)
    task_count = Column("task_count", Integer, nullable=False)

    # Relationships
    parents = relation(
        lambda: Job,
        backref="children",
        cascade="all",
        secondary=lambda: JobEdge.__table__,
        primaryjoin=lambda: and_(
            Job.wf_id == JobEdge.wf_id,
            Job.exec_job_id == foreign(JobEdge.child_exec_job_id),
        ),
        secondaryjoin=lambda: and_(
            Job.wf_id == JobEdge.wf_id,
            Job.exec_job_id == foreign(JobEdge.parent_exec_job_id),
        ),
    )
    tasks = relation(
        lambda: Task, backref="job", cascade="all, delete-orphan", passive_deletes=True,
    )
    job_instances = relation(
        lambda: JobInstance,
        backref="job",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


Job.__table_args__ = (
    UniqueConstraint(Job.wf_id, Job.exec_job_id, name="UNIQUE_JOB"),
    table_keywords,
)
Index("job_type_desc_COL", Job.type_desc)
Index("job_exec_job_id_COL", Job.exec_job_id)


class JobEdge(Base):
    """."""

    __tablename__ = "job_edge"
    __table_args__ = (table_keywords,)

    wf_id = Column(
        "wf_id",
        KeyInteger,
        ForeignKey(Workflow.wf_id, ondelete="CASCADE"),
        primary_key=True,
    )
    parent_exec_job_id = Column("parent_exec_job_id", String(255), primary_key=True)
    child_exec_job_id = Column("child_exec_job_id", String(255), primary_key=True)


class JobInstance(Base):
    """."""

    __tablename__ = "job_instance"

    job_instance_id = Column("job_instance_id", KeyInteger, primary_key=True)
    job_id = Column(
        "job_id",
        KeyInteger,
        ForeignKey(Job.job_id, ondelete="CASCADE"),
        nullable=False,
    )
    host_id = Column(
        "host_id", KeyInteger, ForeignKey(Host.host_id, ondelete="SET NULL"),
    )
    job_submit_seq = Column("job_submit_seq", Integer, nullable=False)
    sched_id = Column("sched_id", String(255))
    site = Column("site", String(255))
    user = Column("user", String(255))
    work_dir = Column("work_dir", Text)
    cluster_start = Column("cluster_start", TimestampType)
    cluster_duration = Column("cluster_duration", DurationType)
    local_duration = Column("local_duration", DurationType)
    subwf_id = Column(
        "subwf_id", KeyInteger, ForeignKey(Workflow.wf_id, ondelete="SET NULL"),
    )
    stdout_file = Column("stdout_file", String(255))
    stdout_text = Column("stdout_text", Text)
    stderr_file = Column("stderr_file", String(255))
    stderr_text = Column("stderr_text", Text)
    stdin_file = Column("stdin_file", String(255))
    multiplier_factor = Column("multiplier_factor", Integer, nullable=False, default=1)
    exitcode = Column("exitcode", Integer)

    # Relationships

    # PM-712 don't want merges to happen to invocation table .
    # setting lazy = false leads to a big join query when a job_instance is updated
    # with the postscript status.
    invocations = relation(
        lambda: Invocation,
        backref="job_instance",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    states = relation(
        lambda: Jobstate,
        backref="job_instance",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    sub_workflow = relation(
        lambda: Workflow,
        backref="job_instance",
        cascade="all, delete-orphan",
        single_parent=True,
    )
    host = relation(
        lambda: Host,
        backref="job_instance",
        cascade="all, delete-orphan",
        single_parent=True,
    )
    integrity_metrics = relation(
        lambda: IntegrityMetrics,
        backref="job_instance",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    tag = relation(
        lambda: Tag,
        backref="job_instance",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


JobInstance.__table_args__ = (
    UniqueConstraint(
        JobInstance.job_id, JobInstance.job_submit_seq, name="UNIQUE_JOB_INSTANCE"
    ),
    table_keywords,
)

# Jobstate definition
# ==> Same information that currently goes into jobstate.log file,
#       obtained from dagman.out file
#
# job_id = from Job table (autogenerated)
# state = from dagman.out file (3rd column of jobstate.log file)
# timestamp = from dagman,out file (1st column of jobstate.log file)


class Jobstate(Base):
    """."""

    __tablename__ = "jobstate"
    __table_args__ = (table_keywords,)

    # All four columns are marked as primary key to produce the desired
    # effect - ie: it is the combo of the four columns that make a row
    # unique.
    job_instance_id = Column(
        "job_instance_id",
        KeyInteger,
        ForeignKey(JobInstance.job_instance_id, ondelete="CASCADE"),
        primary_key=True,
    )
    state = Column("state", String(255), primary_key=True)
    timestamp = Column(
        "timestamp", TimestampType, primary_key=True, default=time.time(),
    )
    jobstate_submit_seq = Column(
        "jobstate_submit_seq", Integer, nullable=False, primary_key=True
    )
    reason = Column("reason", Text)


Index("jobstate_jobstate_submit_seq_COL", Jobstate.jobstate_submit_seq)


class Tag(Base):
    """."""

    __tablename__ = "tag"

    tag_id = Column("tag_id", KeyInteger, primary_key=True)
    wf_id = Column(
        "wf_id",
        KeyInteger,
        ForeignKey(Workflow.wf_id, ondelete="CASCADE"),
        nullable=False,
    )
    job_instance_id = Column(
        "job_instance_id",
        KeyInteger,
        ForeignKey(JobInstance.job_instance_id, ondelete="CASCADE"),
        nullable=False,
    )
    name = Column("name", String(255), nullable=False)
    count = Column("count", Integer, nullable=False)


Tag.__table_args__ = (
    UniqueConstraint(Tag.wf_id, Tag.job_instance_id, name="UNIQUE_TAG"),
    table_keywords,
)


class Task(Base):
    """."""

    __tablename__ = "task"

    task_id = Column("task_id", KeyInteger, primary_key=True)
    wf_id = Column(
        "wf_id",
        KeyInteger,
        ForeignKey(Workflow.wf_id, ondelete="CASCADE"),
        nullable=False,
    )
    job_id = Column("job_id", KeyInteger, ForeignKey(Job.job_id, ondelete="SET NULL"),)
    abs_task_id = Column("abs_task_id", String(255), nullable=False)
    transformation = Column("transformation", Text, nullable=False)
    argv = Column("argv", Text)
    type_desc = Column("type_desc", String(255), nullable=False)

    # Relationships
    parents = relation(
        lambda: Task,
        backref="children",
        cascade="all",
        secondary=lambda: TaskEdge.__table__,
        primaryjoin=lambda: and_(
            Task.wf_id == TaskEdge.wf_id,
            Task.abs_task_id == foreign(TaskEdge.child_abs_task_id),
        ),
        secondaryjoin=lambda: and_(
            Task.wf_id == TaskEdge.wf_id,
            Task.abs_task_id == foreign(TaskEdge.parent_abs_task_id),
        ),
    )
    task_files = relation(
        lambda: WorkflowFiles,
        backref="task",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    files = relation(
        lambda: RCLFN,
        secondary=lambda: WorkflowFiles.__table__,
        primaryjoin=lambda: Task.task_id == WorkflowFiles.task_id,
        secondaryjoin=lambda: WorkflowFiles.lfn_id == RCLFN.lfn_id,
    )
    meta = relation(
        lambda: TaskMeta,
        backref="task",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


Task.__table_args__ = (
    UniqueConstraint(Task.wf_id, Task.abs_task_id, name="UNIQUE_TASK"),
    table_keywords,
)
Index("task_abs_task_id_COL", Task.abs_task_id)
Index("task_wf_id_COL", Task.wf_id)


class TaskEdge(Base):
    """."""

    __tablename__ = "task_edge"
    __table_args__ = (table_keywords,)

    wf_id = Column(
        "wf_id",
        KeyInteger,
        ForeignKey(Workflow.wf_id, ondelete="CASCADE"),
        primary_key=True,
    )
    parent_abs_task_id = Column("parent_abs_task_id", String(255), primary_key=True)
    child_abs_task_id = Column("child_abs_task_id", String(255), primary_key=True)


class TaskMeta(Base):
    """."""

    __tablename__ = "task_meta"
    __table_args__ = (table_keywords,)

    task_id = Column(
        "task_id",
        KeyInteger,
        ForeignKey(Task.task_id, ondelete="CASCADE"),
        primary_key=True,
    )
    key = Column("key", String(255), primary_key=True)
    value = Column("value", String(255), nullable=False)


class Invocation(Base):
    """."""

    __tablename__ = "invocation"

    invocation_id = Column("invocation_id", KeyInteger, primary_key=True)
    wf_id = Column(
        "wf_id",
        KeyInteger,
        ForeignKey(Workflow.wf_id, ondelete="CASCADE"),
        nullable=False,
    )
    job_instance_id = Column(
        "job_instance_id",
        KeyInteger,
        ForeignKey(JobInstance.job_instance_id, ondelete="CASCADE"),
        nullable=False,
    )
    task_submit_seq = Column("task_submit_seq", Integer, nullable=False)
    start_time = Column(
        "start_time", TimestampType, nullable=False, default=time.time()
    )
    remote_duration = Column("remote_duration", DurationType, nullable=False)
    remote_cpu_time = Column("remote_cpu_time", DurationType)
    exitcode = Column("exitcode", Integer, nullable=False)
    transformation = Column("transformation", Text, nullable=False)
    executable = Column("executable", Text, nullable=False)
    argv = Column("argv", Text)
    abs_task_id = Column("abs_task_id", String(255))
    maxrss = Column("maxrss", Integer)
    avg_cpu = Column("avg_cpu", Numeric(precision=16, scale=6))


Invocation.__table_args__ = (
    UniqueConstraint(
        Invocation.job_instance_id, Invocation.task_submit_seq, name="UNIQUE_INVOCATION"
    ),
    table_keywords,
)
Index("invoc_abs_task_id_COL", Invocation.abs_task_id)
Index("invoc_wf_id_COL", Invocation.wf_id)


class IntegrityMetrics(Base):
    """."""

    __tablename__ = "integrity"

    integrity_id = Column("integrity_id", KeyInteger, primary_key=True)
    wf_id = Column(
        "wf_id",
        KeyInteger,
        ForeignKey(Workflow.wf_id, ondelete="CASCADE"),
        nullable=False,
    )
    job_instance_id = Column(
        "job_instance_id",
        KeyInteger,
        ForeignKey(JobInstance.job_instance_id, ondelete="CASCADE"),
        nullable=False,
    )
    type = Column(
        "type", Enum("check", "compute", name="integrity_type_desc"), nullable=False
    )
    file_type = Column(
        "file_type", Enum("input", "output", name="integrity_file_type_desc")
    )
    count = Column("count", Integer, nullable=False)
    duration = Column("duration", DurationType, nullable=False)


IntegrityMetrics.__table_args__ = (
    UniqueConstraint(
        IntegrityMetrics.job_instance_id,
        IntegrityMetrics.type,
        IntegrityMetrics.file_type,
        name="UNIQUE_INTEGRITY",
    ),
    table_keywords,
)


# ---------------------------------------------
# JDBCRC
# ---------------------------------------------


class RCLFN(Base):
    """."""

    __tablename__ = "rc_lfn"

    lfn_id = Column("lfn_id", KeyInteger, primary_key=True)
    lfn = Column("lfn", String(245), nullable=False)

    # Relationships
    pfns = relation(
        lambda: RCPFN,
        backref="lfn",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    meta = relation(
        lambda: RCMeta,
        backref="lfn",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


RCLFN.__table_args__ = (
    UniqueConstraint(RCLFN.lfn, name="UNIQUE_LFN"),
    table_keywords,
)


class RCPFN(Base):
    """."""

    __tablename__ = "rc_pfn"

    pfn_id = Column("pfn_id", KeyInteger, primary_key=True)
    lfn_id = Column(
        "lfn_id",
        KeyInteger,
        ForeignKey(RCLFN.lfn_id, ondelete="CASCADE"),
        nullable=False,
    )
    pfn = Column("pfn", String(245), nullable=False)
    site = Column("site", String(245))


RCPFN.__table_args__ = (
    UniqueConstraint(RCPFN.lfn_id, RCPFN.pfn, RCPFN.site, name="UNIQUE_PFN"),
    table_keywords,
)


class RCMeta(Base):
    """."""

    __tablename__ = "rc_meta"
    __table_args__ = (table_keywords,)

    lfn_id = Column(
        "lfn_id",
        KeyInteger,
        ForeignKey(RCLFN.lfn_id, ondelete="CASCADE"),
        primary_key=True,
    )
    key = Column("key", String(245), primary_key=True)
    value = Column("value", String(245), nullable=False)


class WorkflowFiles(Base):
    """."""

    __tablename__ = "workflow_files"
    __table_args__ = (table_keywords,)

    wf_id = Column(
        "wf_id",
        KeyInteger,
        ForeignKey(Workflow.wf_id, ondelete="CASCADE"),
        primary_key=True,
        nullable=False,
    )
    task_id = Column(
        "task_id",
        KeyInteger,
        ForeignKey(Task.task_id, ondelete="CASCADE"),
        primary_key=True,
        nullable=False,
    )
    lfn_id = Column(
        "lfn_id",
        KeyInteger,
        ForeignKey(RCLFN.lfn_id, ondelete="CASCADE"),
        primary_key=True,
        nullable=False,
    )
    file_type = Column("file_type", String(255))

    # Relationships
    lfn = relation(lambda: RCLFN, cascade="all, delete-orphan", single_parent=True)


# ---------------------------------------------
# DASHBOARD
# ---------------------------------------------


class MasterWorkflow(Base):
    """."""

    __tablename__ = "master_workflow"

    # ==> Information comes from braindump.txt file
    wf_id = Column("wf_id", KeyInteger, primary_key=True)
    wf_uuid = Column("wf_uuid", String(255), nullable=False)
    dax_label = Column("dax_label", String(255))
    dax_version = Column("dax_version", String(255))
    dax_file = Column("dax_file", String(255))
    dag_file_name = Column("dag_file_name", String(255))
    timestamp = Column("timestamp", TimestampType)
    submit_hostname = Column("submit_hostname", String(255))
    submit_dir = Column("submit_dir", Text)
    planner_arguments = Column("planner_arguments", Text)
    user = Column("user", String(255))
    grid_dn = Column("grid_dn", String(255))
    planner_version = Column("planner_version", String(255))
    db_url = Column("db_url", Text)
    archived = Column("archived", Boolean, nullable=False, default=0)

    # Relationships
    states = relation(
        lambda: MasterWorkflowstate,
        backref="workflow",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


MasterWorkflow.__table_args__ = (
    UniqueConstraint(MasterWorkflow.wf_uuid, name="UNIQUE_MASTER_WF_UUID"),
    table_keywords,
)


class MasterWorkflowstate(Base):
    """."""

    __tablename__ = "master_workflowstate"
    __table_args__ = (table_keywords,)

    # All three columns are marked as primary key to produce the desired
    # effect - ie: it is the combo of the three columns that make a row
    # unique.
    wf_id = Column(
        "wf_id",
        KeyInteger,
        ForeignKey(MasterWorkflow.wf_id, ondelete="CASCADE"),
        primary_key=True,
    )
    state = Column(
        "state",
        Enum("WORKFLOW_STARTED", "WORKFLOW_TERMINATED", name="master_workflow_state"),
        primary_key=True,
    )
    timestamp = Column("timestamp", TimestampType, primary_key=True)
    restart_count = Column("restart_count", Integer, nullable=False)
    status = Column("status", Integer)
    reason = Column("reason", Text)


# ---------------------------------------------
# ENSEMBLE MANAGER
# ---------------------------------------------


class Ensemble(Base):
    """."""

    __tablename__ = "ensemble"

    id = Column("id", KeyInteger, primary_key=True)
    name = Column("name", String(100), nullable=False)
    created = Column("created", DateTime, nullable=False)
    updated = Column("updated", DateTime, nullable=False)
    state = Column(
        "state", Enum(*EnsembleStates, name="ensemble_state"), nullable=False
    )
    max_running = Column("max_running", Integer, nullable=False)
    max_planning = Column("max_planning", Integer, nullable=False)
    username = Column("username", String(100), nullable=False)


Ensemble.__table_args__ = (
    UniqueConstraint(Ensemble.name, Ensemble.username, name="UNIQUE_ENSEMBLE"),
    table_keywords,
)

# User mapper(..,...) to extend ensembles.Ensemble to the schema.Ensemble table
mapper(
    _Ensemble,
    Ensemble.__table__,
    properties={"workflows": relation(lambda: _EnsembleWorkflow)},
)


class EnsembleWorkflow(Base):
    """."""

    __tablename__ = "ensemble_workflow"

    id = Column("id", KeyInteger, primary_key=True)
    ensemble_id = Column(
        "ensemble_id", KeyInteger, ForeignKey(Ensemble.id), nullable=False
    )
    name = Column("name", String(100), nullable=False)
    basedir = Column("basedir", String(512), nullable=False)
    created = Column("created", DateTime, nullable=False)
    updated = Column("updated", DateTime, nullable=False)
    state = Column(
        "state",
        Enum(*EnsembleWorkflowStates, name="ensemble_wf_state"),
        nullable=False,
    )
    priority = Column("priority", Integer, nullable=False)
    wf_uuid = Column("wf_uuid", String(36))
    submitdir = Column("submitdir", String(512))
    plan_command = Column(
        "plan_command", String(1024), nullable=False, default="./plan.sh"
    )


EnsembleWorkflow.__table_args__ = (
    UniqueConstraint(
        EnsembleWorkflow.ensemble_id,
        EnsembleWorkflow.name,
        name="UNIQUE_ENSEMBLE_WORKFLOW",
    ),
    table_keywords,
)

mapper(
    _EnsembleWorkflow,
    EnsembleWorkflow.__table__,
    properties={"ensemble": relation(_Ensemble)},
)


class Trigger(Base):
    """."""

    __tablename__ = "trigger"
    _id = Column("id", KeyInteger, primary_key=True)
    ensemble_id = Column(
        "ensemble_id", KeyInteger, ForeignKey(Ensemble.id), nullable=False
    )
    name = Column("name", String(100), nullable=False)
    state = Column("state", Enum(*TriggerStates, name="trigger_state"), nullable=False)
    workflow = Column("workflow", Text(), nullable=False)
    args = Column("args", Text())
    _type = Column(
        "type",
        Enum(*[t.value for t in list(TriggerType)], name="trigger_type"),
        nullable=False,
    )


Trigger.__table_args__ = (
    UniqueConstraint(Trigger.ensemble_id, Trigger.name, name="UNIQUE_TRIGGER"),
    table_keywords,
)
