__author__ = "Monte Goode"
__author__ = "Karan Vahi"

import time
import warnings
import logging

from sqlalchemy import *
from sqlalchemy import MetaData, orm, exc

from Pegasus.db.schema import SABase, KeyInteger

log = logging.getLogger(__name__)

CURRENT_SCHEMA_VERSION = 4.0

metadata = MetaData()

# These are keywords that all tables should have
table_keywords = {}
table_keywords['mysql_charset'] = 'latin1'
table_keywords['mysql_engine'] = 'InnoDB'

def initializeToDashboardDB(db):
    # For SQLite
    warnings.filterwarnings('ignore', '.*does \*not\* support Decimal*.')

    # This is only required if you want to query using the domain objects
    # instead of the session
    #metadata.bind = db

    # Create all the tables if they don't exist
    metadata.create_all(db)

# Empty classes that will be populated and mapped
# to tables via the SQLAlch mapper.
class DashboardWorkflow(SABase):
    pass

class DashboardWorkflowstate(SABase):
    pass

from Pegasus.service.ensembles import Ensemble, EnsembleStates
from Pegasus.service.ensembles import EnsembleWorkflow, EnsembleWorkflowStates


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
    **table_keywords
)

Index('UNIQUE_MASTER_WF_UUID', pg_workflow.c.wf_uuid, unique=True)

orm.mapper(DashboardWorkflow, pg_workflow )


pg_workflowstate = Table('master_workflowstate', metadata,
    # All three columns are marked as primary key to produce the desired
    # effect - ie: it is the combo of the three columns that make a row
    # unique.
    Column('wf_id', KeyInteger, ForeignKey('master_workflow.wf_id', ondelete='CASCADE'), nullable=False, primary_key=True),
    Column('state', Enum('WORKFLOW_STARTED', 'WORKFLOW_TERMINATED'), nullable=False, primary_key=True),
    Column('timestamp', NUMERIC(precision=16,scale=6), nullable=False, primary_key=True),
    Column('restart_count', INT, nullable=False),
    Column('status', INT, nullable=True),
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
    Column('state', Enum(*EnsembleStates), nullable=False),
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
    Column('state', Enum(*EnsembleWorkflowStates), nullable=False),
    Column('priority', Integer, nullable=False),
    Column('wf_uuid', String(36)),
    Column('submitdir', String(512)),
    Column('ensemble_id', KeyInteger, ForeignKey('ensemble.id'), nullable=False),
    **table_keywords
)

Index('UNIQUE_ENSEMBLE_WORKFLOW',
      pg_ensemble_workflow.c.ensemble_id,
      pg_ensemble_workflow.c.name,
      unique=True)

orm.mapper(EnsembleWorkflow, pg_ensemble_workflow, properties = {
    'ensemble':orm.relation(Ensemble, backref='workflows')
})


