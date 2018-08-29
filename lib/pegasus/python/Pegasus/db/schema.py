#!/usr/bin/env python
#
#  Copyright 2018 University Of Southern California
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

import time
import warnings
import logging

from sqlalchemy import *
from sqlalchemy import orm
from sqlalchemy.exc import *
from sqlalchemy.orm import relation
from sqlalchemy.dialects import postgresql, mysql, sqlite

from Pegasus.db.ensembles import Ensemble, EnsembleStates
from Pegasus.db.ensembles import EnsembleWorkflow, EnsembleWorkflowStates

log = logging.getLogger(__name__)

metadata = MetaData()

# for SQLite
warnings.filterwarnings('ignore', '.*does \*not\* support Decimal*.')

# These are keywords that all tables should have
table_keywords = {}
table_keywords['mysql_charset'] = 'latin1'
table_keywords['mysql_engine'] = 'InnoDB'

KeyInteger = BigInteger()
KeyInteger = KeyInteger.with_variant(postgresql.BIGINT(), 'postgresql')
KeyInteger = KeyInteger.with_variant(mysql.BIGINT(), 'mysql')
KeyInteger = KeyInteger.with_variant(sqlite.INTEGER(), 'sqlite')


# --------------------------------------------------------------------
# Method to verify if tables exist or are according to the schema
# --------------------------------------------------------------------
def get_missing_tables(db):
    tables = [
        db_version,
        # WORKFLOW
        st_workflow,
        st_workflowstate,
        st_workflow_meta,
        st_workflow_files,
        st_host,
        st_job,
        st_job_edge,
        st_job_instance,
        st_jobstate,
        st_tag,
        st_task,
        st_task_edge,
        st_task_meta,
        st_invocation,
        st_integrity,
        # MASTER
        pg_workflow,
        pg_workflowstate,
        pg_ensemble,
        pg_ensemble_workflow,
        # JDBCRC
        rc_sequences,
        rc_lfn,
        rc_pfn,
        rc_meta,
    ]
    missing_tables = []
    for table in tables:
        if not check_table_exists(db, table):
            missing_tables.append(table.name)

    return missing_tables


def check_table_exists(engine, table):
    try:
        engine.execute(table.select().limit(1))
        return True

    except OperationalError as e:
        if "no such table" in str(e).lower() or "unknown" in str(e).lower() \
          or "no such column" in str(e).lower():
            return False
        raise
    except ProgrammingError as e:
        if "doesn't exist" in str(e).lower():
            return False
        raise
# --------------------------------------------------------------------


class SABase(object):
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
        retval = '%s:\n' % self.__class__
        for k,v in self.__dict__.items():
            if k == '_sa_instance_state':
                continue
            retval += '  * %s : %s\n' % (k,v)
        return retval

# Empty classes that will be populated and mapped
# to tables via the SQLAlch mapper.

# ---------------------------------------------
# DB Admin
class DBVersion(SABase):
    pass

# ---------------------------------------------
# STAMPEDE
class Host(SABase):
    pass

class Workflow(SABase):
    pass

class Workflowstate(SABase):
    pass

class WorkflowMeta(SABase):
    pass

class WorkflowFiles(SABase):
    pass

class Job(SABase):
    pass

class JobEdge(SABase):
    pass

class JobInstance(SABase):
    pass

class Jobstate(SABase):
    pass

class Tag(SABase):
    pass

class Task(SABase):
    pass

class TaskEdge(SABase):
    pass

class TaskMeta(SABase):
    pass

class Invocation(SABase):
    pass

class IntegrityMetrics(SABase):
    pass

# ---------------------------------------------
# DASHBOARD
class DashboardWorkflow(SABase):
    pass

class DashboardWorkflowstate(SABase):
    pass

# ---------------------------------------------
# JDBCRC
class Sequences(SABase):
    pass

class RCLFN(SABase):
    pass

class RCPFN(SABase):
    pass

class RCMeta(SABase):
    pass

# ---------------------------------------------
# DB ADMIN
# ---------------------------------------------
db_version = Table('dbversion', metadata,
    Column('id', KeyInteger, primary_key=True, autoincrement=True, nullable=False),
    Column('version_number', INT, default=5),
    Column('version', VARCHAR(50), nullable=False),
    Column('version_timestamp', INT, nullable=False),
    sqlite_autoincrement=True
)

orm.mapper(DBVersion, db_version)


# ---------------------------------------------
# STAMPEDE
# ---------------------------------------------

st_workflow = Table('workflow', metadata,
    # ==> Information comes from braindump.txt file
    Column('wf_id', KeyInteger, primary_key=True, nullable=False),
    Column('wf_uuid', VARCHAR(255), nullable=False),
    Column('dag_file_name', VARCHAR(255), nullable=True),
    Column('timestamp', NUMERIC(precision=16,scale=6), nullable=True),
    Column('submit_hostname', VARCHAR(255), nullable=True),
    Column('submit_dir', TEXT, nullable=True),
    Column('planner_arguments', TEXT, nullable=True),
    Column('user', VARCHAR(255), nullable=True),
    Column('grid_dn', VARCHAR(255), nullable=True),
    Column('planner_version', VARCHAR(255), nullable=True),
    Column('dax_label', VARCHAR(255), nullable=True),
    Column('dax_version', VARCHAR(255), nullable=True),
    Column('dax_file', VARCHAR(255), nullable=True),
    Column('db_url', TEXT, nullable=True),
    Column('parent_wf_id', KeyInteger, ForeignKey("workflow.wf_id", ondelete='CASCADE'), nullable=True),
    # not marked as FK to not screw up the cascade.
    Column('root_wf_id', KeyInteger, nullable=True),
    **table_keywords
)

Index('wf_id_KEY', st_workflow.c.wf_id, unique=True)
Index('wf_uuid_UNIQUE', st_workflow.c.wf_uuid, unique=True)

orm.mapper(Workflow, st_workflow, properties = {
    'child_wf':relation(Workflow, cascade='all, delete-orphan', passive_deletes=True),
    'child_wfs':relation(Workflowstate, backref='st_workflow', cascade='all, delete-orphan', passive_deletes=True),
    'child_host':relation(Host, backref='st_workflow', cascade='all, delete-orphan', passive_deletes=True),
    'child_task':relation(Task, backref='st_workflow', cascade='all, delete-orphan', passive_deletes=True),
    'child_job':relation(Job, backref='st_workflow', cascade='all, delete-orphan', passive_deletes=True),
    'child_invocation':relation(Invocation, backref='st_workflow', cascade='all, delete-orphan', passive_deletes=True),
    'child_task_e':relation(TaskEdge, backref='st_workflow', cascade='all, delete-orphan', passive_deletes=True),
    'child_job_e':relation(JobEdge, backref='st_workflow', cascade='all, delete-orphan', passive_deletes=True),
})


st_workflowstate = Table('workflowstate', metadata,
    # All three columns are marked as primary key to produce the desired
    # effect - ie: it is the combo of the three columns that make a row
    # unique.
    Column('wf_id', KeyInteger, ForeignKey('workflow.wf_id', ondelete='CASCADE'), nullable=False, primary_key=True),
    Column('state', Enum('WORKFLOW_STARTED', 'WORKFLOW_TERMINATED', name='workflow_state'), nullable=False, primary_key=True),
    Column('timestamp', NUMERIC(precision=16,scale=6), nullable=False, primary_key=True, default=time.time()),
    Column('restart_count', INT, nullable=False),
    Column('status', INT, nullable=True),
    Column('reason', TEXT, nullable=True),
    **table_keywords
)

Index('UNIQUE_WORKFLOWSTATE',
    st_workflowstate.c.wf_id,
    st_workflowstate.c.state,
    st_workflowstate.c.timestamp,
    unique=True)

orm.mapper(Workflowstate, st_workflowstate)


st_workflow_meta = Table('workflow_meta', metadata,
    Column('wf_id', KeyInteger, ForeignKey('workflow.wf_id', ondelete='CASCADE'), primary_key=True, nullable=False),
    Column('key', VARCHAR(255), primary_key=True, nullable=False),
    Column('value', VARCHAR(255), nullable=False),
    **table_keywords
)
Index('UNIQUE_WORKFLOW_META', st_workflow_meta.c.wf_id, st_workflow_meta.c.key, st_workflow_meta.c.value, unique=True)

orm.mapper(WorkflowMeta, st_workflow_meta)


# st_host definition
# ==> Information from kickstart output file
#
# site_name = <resource, from invocation element>
# hostname = <hostname, from invocation element>
# ip_address = <hostaddr, from invocation element>
# uname = <combined (system, release, machine) from machine element>
# total_ram = <ram_total from machine element>

st_host = Table('host', metadata,
    Column('host_id', KeyInteger, primary_key=True, nullable=False),
    Column('wf_id', KeyInteger, ForeignKey('workflow.wf_id', ondelete='CASCADE'), nullable=False),
    Column('site', VARCHAR(255), nullable=False),
    Column('hostname', VARCHAR(255), nullable=False),
    Column('ip', VARCHAR(255), nullable=False),
    Column('uname', VARCHAR(255), nullable=True),
    Column('total_memory', Integer, nullable=True),
    **table_keywords
)

Index('UNIQUE_HOST', st_host.c.wf_id, st_host.c.site, st_host.c.hostname, st_host.c.ip, unique=True)

orm.mapper(Host, st_host)


# static job table

st_job = Table('job', metadata,
    Column('job_id', KeyInteger, primary_key=True, nullable=False),
    Column('wf_id', KeyInteger, ForeignKey('workflow.wf_id', ondelete='CASCADE'), nullable=False),
    Column('exec_job_id', VARCHAR(255), nullable=False),
    Column('submit_file', VARCHAR(255), nullable=False),
    Column('type_desc', Enum('unknown',
                             'compute',
                             'stage-in-tx',
                             'stage-out-tx',
                             'registration',
                             'inter-site-tx',
                             'create-dir',
                             'staged-compute',
                             'cleanup',
                             'chmod',
                             'dax',
                             'dag', name='job_type_desc'), nullable=False),
    Column('clustered', BOOLEAN, nullable=False),
    Column('max_retries', INT, nullable=False),
    Column('executable', TEXT, nullable=False),
    Column('argv', TEXT, nullable=True),
    Column('task_count', INT, nullable=False),
    **table_keywords
)

Index('job_id_KEY', st_job.c.job_id, unique=True)
Index('job_type_desc_COL', st_job.c.type_desc)
Index('job_exec_job_id_COL', st_job.c.exec_job_id)
Index('UNIQUE_JOB', st_job.c.wf_id, st_job.c.exec_job_id, unique=True)

orm.mapper(Job, st_job, properties = {
    'child_job_instance':relation(JobInstance, backref='st_job', cascade='all, delete-orphan', passive_deletes=True, lazy=True)
})



st_job_edge = Table('job_edge', metadata,
    Column('wf_id', KeyInteger, ForeignKey('workflow.wf_id', ondelete='CASCADE'), primary_key=True, nullable=False),
    Column('parent_exec_job_id', VARCHAR(255), primary_key=True, nullable=False),
    Column('child_exec_job_id', VARCHAR(255), primary_key=True, nullable=False),
    **table_keywords
)

Index('UNIQUE_JOB_EDGE',
    st_job_edge.c.wf_id,
    st_job_edge.c.parent_exec_job_id,
    st_job_edge.c.child_exec_job_id,
    unique=True)

orm.mapper(JobEdge, st_job_edge)


st_job_instance = Table('job_instance', metadata,
    Column('job_instance_id', KeyInteger, primary_key=True, nullable=False),
    Column('job_id', KeyInteger, ForeignKey('job.job_id', ondelete='CASCADE'), nullable=False),
    Column('host_id', KeyInteger, ForeignKey('host.host_id', ondelete='SET NULL'), nullable=True),
    Column('job_submit_seq', INT, nullable=False),
    Column('sched_id', VARCHAR(255), nullable=True),
    Column('site', VARCHAR(255), nullable=True),
    Column('user', VARCHAR(255), nullable=True),
    Column('work_dir', TEXT, nullable=True),
    Column('cluster_start', NUMERIC(16,6), nullable=True),
    Column('cluster_duration', NUMERIC(10,3), nullable=True),
    Column('local_duration', NUMERIC(10,3), nullable=True),
    Column('subwf_id', KeyInteger, ForeignKey('workflow.wf_id', ondelete='SET NULL'), nullable=True),
    Column('stdout_file', VARCHAR(255), nullable=True),
    Column('stdout_text', TEXT, nullable=True),
    Column('stderr_file', VARCHAR(255), nullable=True),
    Column('stderr_text', TEXT, nullable=True),
    Column('stdin_file', VARCHAR(255), nullable=True),
    Column('multiplier_factor', INT, nullable=False, default=1),
    Column('exitcode', INT, nullable=True),
    **table_keywords
)

Index('job_instance_id_KEY',
    st_job_instance.c.job_instance_id,
    unique=True)
Index('UNIQUE_JOB_INSTANCE',
    st_job_instance.c.job_id,
    st_job_instance.c.job_submit_seq,
    unique=True)

orm.mapper(JobInstance, st_job_instance, properties = {
    #PM-712 don't want merges to happen to invocation table .
    #setting lazy = false leads to a big join query when a job_instance is updated
    #with the postscript status.
    'child_tsk':relation(Invocation, backref='st_job_instance', cascade='all, delete-orphan', passive_deletes=True, lazy=True),
    'child_jst':relation(Jobstate, backref='st_job_instance', cascade='all, delete-orphan', passive_deletes=True, lazy=True),
    'child_integrity':relation(IntegrityMetrics, backref='st_integrity', cascade='all, delete-orphan', passive_deletes=True, lazy=True),
    'child_tag':relation(Tag, backref='st_tag', cascade='all, delete-orphan', passive_deletes=True, lazy=True),
})


# st_jobstate definition
# ==> Same information that currently goes into jobstate.log file,
#       obtained from dagman.out file
#
# job_id = from st_job table (autogenerated)
# state = from dagman.out file (3rd column of jobstate.log file)
# timestamp = from dagman,out file (1st column of jobstate.log file)

st_jobstate = Table('jobstate', metadata,
    # All four columns are marked as primary key to produce the desired
    # effect - ie: it is the combo of the four columns that make a row
    # unique.
    Column('job_instance_id', KeyInteger, ForeignKey('job_instance.job_instance_id', ondelete='CASCADE'), nullable=False, primary_key=True),
    Column('state', VARCHAR(255), nullable=False, primary_key=True),
    Column('timestamp', NUMERIC(precision=16,scale=6), nullable=False, primary_key=True, default=time.time()),
    Column('jobstate_submit_seq', INT, nullable=False, primary_key=True),
    Column('reason', TEXT, nullable=True),
    **table_keywords
)

Index('UNIQUE_JOBSTATE',
    st_jobstate.c.job_instance_id,
    st_jobstate.c.state,
    st_jobstate.c.timestamp,
    st_jobstate.c.jobstate_submit_seq,
    unique=True)

orm.mapper(Jobstate, st_jobstate)


st_tag = Table('tag', metadata,
    Column('tag_id', KeyInteger, primary_key=True, nullable=False),
    Column('wf_id', KeyInteger, ForeignKey('workflow.wf_id', ondelete='CASCADE'), nullable=False),
    Column('job_instance_id', KeyInteger, ForeignKey('job_instance.job_instance_id', ondelete='CASCADE'), nullable=False),
    Column('name', VARCHAR(255), nullable=False),
    Column('count', INT, nullable=False),
    **table_keywords
)

Index('tag_id_KEY', st_tag.c.tag_id, unique=True)
Index('UNIQUE_TAG', st_tag.c.job_instance_id, st_tag.c.wf_id, unique=True)
orm.mapper(Tag, st_tag)


st_task = Table('task', metadata,
    Column('task_id', KeyInteger, primary_key=True, nullable=False),
    Column('job_id', KeyInteger, ForeignKey('job.job_id', ondelete='SET NULL'), nullable=True),
    Column('wf_id', KeyInteger, ForeignKey('workflow.wf_id', ondelete='CASCADE'), nullable=False),
    Column('abs_task_id', VARCHAR(255), nullable=False),
    Column('transformation', TEXT, nullable=False),
    Column('argv', TEXT, nullable=True),
    Column('type_desc', VARCHAR(255), nullable=False),
    **table_keywords
)

Index('task_id_KEY', st_task.c.task_id, unique=True)
Index('task_abs_task_id_COL', st_task.c.abs_task_id)
Index('task_wf_id_COL', st_task.c.wf_id)
Index('UNIQUE_TASK', st_task.c.wf_id, st_task.c.abs_task_id, unique=True)

orm.mapper(Task, st_task, properties = {
    'child_task_meta':relation(TaskMeta, backref='st_task_meta', cascade='all, delete-orphan', passive_deletes=True),
})


st_task_edge = Table('task_edge', metadata,
    Column('wf_id', KeyInteger, ForeignKey('workflow.wf_id', ondelete='CASCADE'), primary_key=True, nullable=False),
    Column('parent_abs_task_id', VARCHAR(255), primary_key=True, nullable=True),
    Column('child_abs_task_id', VARCHAR(255), primary_key=True, nullable=True),
    **table_keywords
)

Index('UNIQUE_TASK_EDGE',
    st_task_edge.c.wf_id,
    st_task_edge.c.parent_abs_task_id,
    st_task_edge.c.child_abs_task_id,
    unique=True)

orm.mapper(TaskEdge, st_task_edge)


st_task_meta = Table('task_meta', metadata,
    Column('task_id', KeyInteger, ForeignKey('task.task_id', ondelete='CASCADE'), primary_key=True, nullable=False),
    Column('key', VARCHAR(255), primary_key=True, nullable=False),
    Column('value', VARCHAR(255), nullable=False),
    **table_keywords
)
Index('UNIQUE_TASK_META', st_task_meta.c.task_id, st_task_meta.c.key, st_task_meta.c.value, unique=True)

orm.mapper(TaskMeta, st_task_meta)


st_invocation = Table('invocation', metadata,
    Column('invocation_id', KeyInteger, primary_key=True, nullable=False),
    Column('job_instance_id', KeyInteger, ForeignKey('job_instance.job_instance_id', ondelete='CASCADE'), nullable=False),
    Column('task_submit_seq', INT, nullable=False),
    Column('start_time', NUMERIC(16,6), nullable=False, default=time.time()),
    Column('remote_duration', NUMERIC(10,3), nullable=False),
    Column('remote_cpu_time', NUMERIC(10,3), nullable=True),
    Column('exitcode', INT, nullable=False),
    Column('transformation', TEXT, nullable=False),
    Column('executable', TEXT, nullable=False),
    Column('argv', TEXT, nullable=True),
    Column('abs_task_id', VARCHAR(255), nullable=True),
    Column('wf_id', KeyInteger, ForeignKey('workflow.wf_id', ondelete='CASCADE'), nullable=False),
    **table_keywords
)

Index('invocation_id_KEY', st_invocation.c.invocation_id, unique=True)
Index('invoc_abs_task_id_COL', st_invocation.c.abs_task_id)
Index('invoc_wf_id_COL', st_invocation.c.wf_id)
Index('UNIQUE_INVOCATION', st_invocation.c.job_instance_id, st_invocation.c.task_submit_seq, unique=True)

orm.mapper(Invocation, st_invocation)


st_workflow_files = Table('workflow_files', metadata,
                          Column('lfn_id', KeyInteger, ForeignKey('rc_lfn.lfn_id', ondelete='CASCADE'), nullable=False, primary_key=True),
                          Column('wf_id', KeyInteger, ForeignKey('workflow.wf_id', ondelete='CASCADE'), nullable=False, primary_key=True),
                          Column('task_id', KeyInteger, ForeignKey('task.task_id', ondelete='CASCADE'), nullable=False, primary_key=True),
                          Column('file_type', VARCHAR(255), nullable=True),
                          **table_keywords
                          )

orm.mapper(WorkflowFiles, st_workflow_files)


st_integrity = Table('integrity', metadata,
                             Column('integrity_id', KeyInteger, primary_key=True, nullable=False),
                             Column('wf_id', KeyInteger, ForeignKey('workflow.wf_id', ondelete='CASCADE'), nullable=False),
                             Column('job_instance_id', KeyInteger, ForeignKey('job_instance.job_instance_id', ondelete='CASCADE'), nullable=False),
                             Column('type', Enum('check', 'compute', name='integrity_type_desc'), nullable=False),
                             Column('file_type', Enum('input', 'output', name='integrity_file_type_desc')),
                             Column('count', INT, nullable=False),
                             Column('duration', NUMERIC(10,3), nullable=False),
                             **table_keywords
                             )

Index('integrity_id_KEY', st_integrity.c.integrity_id, unique=True)
Index('UNIQUE_INTEGRITY', st_integrity.c.job_instance_id, st_integrity.c.type, st_integrity.c.file_type, unique=True)
orm.mapper(IntegrityMetrics, st_integrity)


# ---------------------------------------------
# DASHBOARD
# ---------------------------------------------


pg_workflow = Table('master_workflow', metadata,
    # ==> Information comes from braindump.txt file
    Column('wf_id', KeyInteger, primary_key=True, nullable=False),
    Column('wf_uuid', VARCHAR(255), nullable=False),
    Column('dax_label', VARCHAR(255), nullable=True),
    Column('dax_version', VARCHAR(255), nullable=True),
    Column('dax_file', VARCHAR(255), nullable=True),
    Column('dag_file_name', VARCHAR(255), nullable=True),
    Column('timestamp', NUMERIC(precision=16,scale=6), nullable=True),
    Column('submit_hostname', VARCHAR(255), nullable=True),
    Column('submit_dir', TEXT, nullable=True),
    Column('planner_arguments', TEXT, nullable=True),
    Column('user', VARCHAR(255), nullable=True),
    Column('grid_dn', VARCHAR(255), nullable=True),
    Column('planner_version', VARCHAR(255), nullable=True),
    Column('db_url', TEXT, nullable=True),
    Column('archived', BOOLEAN, nullable=False, default=0),
    **table_keywords
)

Index('UNIQUE_MASTER_WF_UUID', pg_workflow.c.wf_uuid, unique=True)

orm.mapper(DashboardWorkflow, pg_workflow )


pg_workflowstate = Table('master_workflowstate', metadata,
    # All three columns are marked as primary key to produce the desired
    # effect - ie: it is the combo of the three columns that make a row
    # unique.
    Column('wf_id', KeyInteger, ForeignKey('master_workflow.wf_id', ondelete='CASCADE'), nullable=False, primary_key=True),
    Column('state', Enum('WORKFLOW_STARTED', 'WORKFLOW_TERMINATED', name='master_workflow_state'), nullable=False, primary_key=True),
    Column('timestamp', NUMERIC(precision=16,scale=6), nullable=False, primary_key=True),
    Column('restart_count', INT, nullable=False),
    Column('status', INT, nullable=True),
    Column('reason', TEXT, nullable=True),
    **table_keywords
)

Index('UNIQUE_MASTER_WORKFLOWSTATE',
      pg_workflowstate.c.wf_id,
      pg_workflowstate.c.state,
      pg_workflowstate.c.timestamp,
      unique=True)

orm.mapper(DashboardWorkflowstate, pg_workflowstate)


pg_ensemble = Table('ensemble', metadata,
    Column('id', KeyInteger, primary_key=True),
    Column('name', String(100), nullable=False),
    Column('created', DateTime, nullable=False),
    Column('updated', DateTime, nullable=False),
    Column('state', Enum(*EnsembleStates, name='ensemble_state'), nullable=False),
    Column('max_running', Integer, nullable=False),
    Column('max_planning', Integer, nullable=False),
    Column('username', String(100), nullable=False),
    **table_keywords
)

Index('UNIQUE_ENSEMBLE',
      pg_ensemble.c.username,
      pg_ensemble.c.name)

orm.mapper(Ensemble, pg_ensemble)


pg_ensemble_workflow = Table('ensemble_workflow', metadata,
    Column('id', KeyInteger, primary_key=True),
    Column('name', String(100), nullable=False),
    Column('basedir', String(512), nullable=False),
    Column('created', DateTime, nullable=False),
    Column('updated', DateTime, nullable=False),
    Column('state', Enum(*EnsembleWorkflowStates, name='ensemble_wf_state'), nullable=False),
    Column('priority', Integer, nullable=False),
    Column('wf_uuid', String(36)),
    Column('submitdir', String(512)),
    Column('plan_command', String(1024), nullable=False, default="./plan.sh"),
    Column('ensemble_id', KeyInteger, ForeignKey('ensemble.id'), nullable=False),
    **table_keywords
)

Index('UNIQUE_ENSEMBLE_WORKFLOW',
      pg_ensemble_workflow.c.ensemble_id,
      pg_ensemble_workflow.c.name,
      unique=True)

orm.mapper(EnsembleWorkflow, pg_ensemble_workflow, properties = {
    'ensemble':relation(Ensemble, backref='workflows')
})


# ---------------------------------------------
# JDBCRC
# ---------------------------------------------

rc_sequences = Table('sequences', metadata,
    Column('name', VARCHAR(32), nullable=False, primary_key=True),
    Column('currval', BIGINT, nullable=False),
    **table_keywords
)
orm.mapper(Sequences, rc_sequences)


rc_lfn = Table('rc_lfn', metadata,
    Column('lfn_id', KeyInteger, primary_key=True, nullable=False),
    Column('lfn', VARCHAR(245), nullable=False),
    **table_keywords
)

Index('ix_rc_lfn', rc_lfn.c.lfn, unique=True)
orm.mapper(RCLFN, rc_lfn)


rc_pfn = Table('rc_pfn', metadata,
    Column('pfn_id', KeyInteger, primary_key=True, nullable=False),
    Column('lfn_id', KeyInteger, ForeignKey('rc_lfn.lfn_id', ondelete='CASCADE'), nullable=False),
    Column('pfn', VARCHAR(245), nullable=False),
    Column('site', VARCHAR(245)),
    **table_keywords
)

Index('UNIQUE_PFN', rc_pfn.c.lfn_id, rc_pfn.c.pfn, rc_pfn.c.site, unique=True)
orm.mapper(RCPFN, rc_pfn)


rc_meta = Table('rc_meta', metadata,
    Column('lfn_id', KeyInteger, ForeignKey('rc_lfn.lfn_id', ondelete='CASCADE'), primary_key=True, nullable=False),
    Column('key', VARCHAR(245), primary_key=True, nullable=False),
    Column('value', VARCHAR(245), nullable=False),
    **table_keywords
)
Index('rc_meta_unique', rc_meta.c.lfn_id, rc_meta.c.key, unique=True)
orm.mapper(RCMeta, rc_meta)
