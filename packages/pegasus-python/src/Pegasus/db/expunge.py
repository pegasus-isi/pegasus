__author__ = "Monte Goode"

import logging
import time

from sqlalchemy import orm

from Pegasus.db import connection
from Pegasus.db.schema import *

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

    log.info("Expunging %s from workflow database", wf_uuid)

    session = connection.connect(dburi, create=True)
    try:
        query = session.query(Workflow).filter(Workflow.wf_uuid == wf_uuid)
        try:
            wf = query.one()
        except orm.exc.NoResultFound as e:
            log.warn("No workflow found with wf_uuid %s - aborting expunge", wf_uuid)
            return

        # PM-1218 gather list of descendant workflows with wf_uuid
        query = session.query(Workflow).filter(Workflow.root_wf_id == wf.wf_id)
        try:
            desc_wfs = query.all()
            for desc_wf in desc_wfs:
                # delete the files from the rc_lfn explicitly as they are
                # not associated with workflow table
                __delete_workflow_files__(session, desc_wf.wf_uuid, desc_wf.wf_id)
        except orm.exc.NoResultFound as e:
            log.warn(
                "No workflow found with root wf_id %s - aborting expunge", wf.wf_id
            )
            return

        session.delete(wf)

        log.info("Flushing top-level workflow: %s", wf.wf_uuid)
        i = time.time()
        session.flush()
        session.commit()
        log.info("Flush took: %f seconds", time.time() - i)
    finally:
        session.close()


def __delete_workflow_files__(session, wf_uuid, wf_id):
    # Expunge all files associated with the workflow from the rc tables
    log.info(
        "Expunging rc files for workflow %s with database id %s from workflow database"
        % (wf_uuid, wf_id)
    )

    query = session.query(RCLFN).filter(
        RCLFN.lfn_id.in_(
            session.query(WorkflowFiles.lfn_id).filter(WorkflowFiles.wf_id == wf_id)
        )
    )
    count = query.delete(synchronize_session=False)
    log.info("Flushing deletes of rc_lfn from workflow: %s", wf_uuid)
    i = time.time()
    session.flush()
    session.commit()
    log.info("Flush took: %f seconds", time.time() - i)
    log.info(
        "Deleted  {} files from rc_file table for workflow {} ".format(count, wf_uuid)
    )


def delete_dashboard_workflow(dburi, wf_uuid):
    "Expunge workflow from dashboard database"

    log.info("Expunging %s from dashboard database", wf_uuid)

    session = connection.connect(dburi, create=True)
    try:
        query = session.query(MasterWorkflow).filter(MasterWorkflow.wf_uuid == wf_uuid)
        try:
            wf = query.one()
        except orm.exc.NoResultFound as e:
            log.warn("No workflow found with wf_uuid %s - aborting expunge", wf_uuid)
            return

        session.delete(wf)

        i = time.time()
        session.flush()
        session.commit()
        log.info("Flush took: %f seconds", time.time() - i)
    finally:
        session.close()
