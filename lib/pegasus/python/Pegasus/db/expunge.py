__author__ = "Monte Goode"

import os
import time
import logging

from Pegasus.db.schema import *
from Pegasus.db import connection

log = logging.getLogger(__name__)

"""
Module to expunge a workflow and the associated data from
a stampede schema database in the case of running with the replay
option or a similar situation.

The wf_uuid that is passed into the constructor MUST be the
"top-level" workflow the user wants to delete.  Which is to
say if the wf_uuid is a the child of another workflow, then
only the data associated with that workflow will be deleted.
Any parent or sibling workflows will be left untouched.

Usage::

 from Pegasus.db import expunge

 connString = 'sqlite:///pegasusMontage.db'
 wf_uuid = '1249335e-7692-4751-8da2-efcbb5024429'
 expunge.delete_workflow(connString, wf_uuid)

All children/grand-children/etc information and associated
workflows will be removed.
"""

def delete_workflow(dburi, wf_uuid):
    "Expunge workflow from workflow database"

    log.info('Expunging %s from workflow database', wf_uuid)

    session = connection.connect(dburi, create=True)
    try:
        query = session.query(Workflow).filter(Workflow.wf_uuid == wf_uuid)
        try:
            wf = query.one()
        except orm.exc.NoResultFound, e:
            log.warn('No workflow found with wf_uuid %s - aborting expunge', wf_uuid)
            return

        session.delete(wf)

        log.info('Flushing top-level workflow: %s', wf.wf_uuid)
        i = time.time()
        session.flush()
        session.commit()
        log.info('Flush took: %f seconds', time.time() - i)
    finally:
        session.close()

def delete_dashboard_workflow(dburi, wf_uuid):
    "Expunge workflow from dashboard database"

    log.info('Expunging %s from dashboard database', wf_uuid)

    session = connection.connect(dburi, create=True)
    try:
        query = session.query(DashboardWorkflow).filter(DashboardWorkflow.wf_uuid == wf_uuid)
        try:
            wf = query.one()
        except orm.exc.NoResultFound, e:
            log.warn('No workflow found with wf_uuid %s - aborting expunge', wf_uuid)
            return

        session.delete(wf)

        i = time.time()
        session.flush()
        session.commit()
        log.info('Flush took: %f seconds', time.time() - i)
    finally:
        session.close()

