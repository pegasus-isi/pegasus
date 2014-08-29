import os
import re
import stat
import shutil
from datetime import datetime

from flask import url_for, g
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy import sql
import werkzeug

from Pegasus.db.modules import SQLAlchemyInit
from Pegasus.service.api import *
from Pegasus.service.ensembles.bundle import Bundle

def validate_ensemble_name(name):
    if name is None:
        raise APIError("Specify ensemble name")
    if len(name) >= 100:
        raise APIError("Ensemble name too long: %d" % len(name))
    if ".." in name or re.match(r"\A[a-zA-Z0-9._-]+\Z", name) is None:
        raise APIError("Invalid ensemble name: %s" % name)
    return name

def validate_priority(priority):
    try:
        return int(priority)
    except ValueError:
        raise APIError("Invalid priority: %s" % priority)

class EnsembleBase(object):
    def set_name(self, name):
        self.name = validate_ensemble_name(name)

    def set_created(self):
        self.created = datetime.utcnow()

    def set_updated(self):
        self.updated = datetime.utcnow()

class States(set):
    def __getattr__(self, name):
        if name in self:
            return name
        raise AttributeError

EnsembleStates = States(["ACTIVE","HELD","PAUSED"])
EnsembleWorkflowStates = States(["READY","PLANNING","PLAN_FAILED","QUEUED","RUN_FAILED","RUNNING",
                                 "FAILED","SUCCESSFUL","ABORTED"])

class Ensemble(EnsembleBase):
    def __init__(self, username, name):
        self.username = username
        self.set_name(name)
        self.set_created()
        self.set_updated()
        self.state = EnsembleStates.ACTIVE
        self.set_max_running(1)
        self.set_max_planning(1)

    def set_state(self, state):
        state = state.upper()
        if state not in EnsembleStates:
            raise APIError("Invalid ensemble state: %s" % state)
        self.state = state

    def set_max_running(self, max_running):
        try:
            x = int(max_running)
            if max_running < 1:
                raise APIError("Value for max_running must be >= 1: %s" % max_running)
            self.max_running = x
        except ValueError:
            raise APIError("Invalid value for max_running: %s" % max_running)

    def set_max_planning(self, max_planning):
        try:
            x = int(max_planning)
            if max_planning < 1:
                raise APIError("Value for max_planning must be >= 1: %s" % max_planning)
            self.max_planning = x
        except ValueError:
            raise APIError("Invalid value for max_planning: %s" % max_planning)

    def get_dir(self):
        return os.path.join(g.user.get_userdata_dir(), "ensembles", self.name)

    def get_object(self):
        return {
            "id": self.id,
            "name": self.name,
            "created": self.created,
            "updated": self.updated,
            "state": self.state,
            "max_running": self.max_running,
            "max_planning": self.max_planning,
            "workflows": url_for("route_list_ensemble_workflows", name=self.name, _external=True),
            "href": url_for("route_get_ensemble", name=self.name, _external=True)
        }

class EnsembleWorkflow(EnsembleBase):
    def __init__(self, ensemble_id, name):
        self.ensemble_id = ensemble_id
        self.set_name(name)
        self.set_created()
        self.set_updated()
        self.state = EnsembleWorkflowStates.READY
        self.set_priority(0)
        self.set_wf_uuid(None)
        self.set_submitdir(None)

    def set_state(self, state):
        state = state.upper()
        if state not in EnsembleWorkflowStates:
            raise APIError("Invalid ensemble workflow state: %s" % state)
        self.state = state

    def change_state(self, state):
        # The allowed transitions are:
        #   PLAN_FAILED -> READY
        #   RUN_FAILED -> QUEUED
        #   RUN_FAILED -> READY
        #   FAILED -> QUEUED
        #   FAILED -> READY
        if self.state == EnsembleWorkflowStates.PLAN_FAILED:
            if state != EnsembleWorkflowStates.READY:
                raise APIError("Can only replan workflows in PLAN_FAILED state")
        elif self.state == EnsembleWorkflowStates.RUN_FAILED:
            if state not in (EnsembleWorkflowStates.READY, EnsembleWorkflowStates.QUEUED):
                raise APIError("Can only replan or rerun workflows in RUN_FAILED state")
        elif self.state == EnsembleWorkflowStates.FAILED:
            if state not in (EnsembleWorkflowStates.READY, EnsembleWorkflowStates.QUEUED):
                raise APIError("Can only replan or rerun workflows in FAILED state")
        else:
            raise APIError("Invalid state change: %s -> %s" % (self.state, state))

        self.set_state(state)

    def set_priority(self, priority):
        self.priority = validate_priority(priority)

    def set_wf_uuid(self, wf_uuid):
        if wf_uuid is not None and len(wf_uuid) != 36:
            raise APIError("Invalid wf_uuid")
        self.wf_uuid = wf_uuid

    def set_submitdir(self, submitdir):
        self.submitdir = submitdir

    def get_dir(self):
        ensembledir = self.ensemble.get_dir()
        return os.path.join(ensembledir, "workflows", self.name)

    def get_object(self):
        return {
            "id": self.id,
            "name": self.name,
            "created": self.created,
            "updated": self.updated,
            "state": self.state,
            "priority": self.priority,
            "wf_uuid": self.wf_uuid,
            "href": url_for("route_get_ensemble_workflow", ensemble=self.ensemble.name, workflow=self.name, _external=True)
        }

    def get_detail_object(self):
        def myurl_for(filename):
            return url_for("route_get_ensemble_workflow_file",
                           ensemble=self.ensemble.name, workflow=self.name,
                           filename=filename, _external=True)
        o = self.get_object()
        o["dax"] = myurl_for("dax.xml")
        o["replicas"] = myurl_for("rc.txt")
        o["transformations"] = myurl_for("tc.txt")
        o["sites"] = myurl_for("sites.xml")
        o["conf"] = myurl_for("pegasus.properties")
        o["plan_script"] = myurl_for("plan.sh")
        return o

class Ensembles(SQLAlchemyInit):
    def __init__(self, dburl):
        from Pegasus.db.schema.stampede_dashboard_schema import initializeToDashboardDB
        SQLAlchemyInit.__init__(self, dburl, initializeToDashboardDB)

    def list_ensembles(self, username):
        ensembles = self.session.query(Ensemble).filter(Ensemble.username==username).all()
        return ensembles

    def list_actionable_ensembles(self):
        states = (
            EnsembleWorkflowStates.READY,
            EnsembleWorkflowStates.PLANNING,
            EnsembleWorkflowStates.QUEUED,
            EnsembleWorkflowStates.RUNNING
        )
        stmt = sql.exists().where(Ensemble.id==EnsembleWorkflow.ensemble_id).where(EnsembleWorkflow.state.in_(states))
        return self.session.query(Ensemble).filter(stmt).all()

    def get_ensemble(self, username, name):
        try:
            return self.session.query(Ensemble).filter(Ensemble.username==username, Ensemble.name==name).one()
        except NoResultFound:
            raise APIError("No such ensemble: %s" % name, 404)

    def create_ensemble(self, username, name, max_running, max_planning):
        if self.session.query(Ensemble).filter(Ensemble.username==username, Ensemble.name==name).count() > 0:
            raise APIError("Ensemble %s already exists" % name, 400)

        ensemble = Ensemble(username, name)
        ensemble.set_max_running(max_running)
        ensemble.set_max_planning(max_planning)
        self.session.add(ensemble)
        self.session.flush()
        return ensemble

    def list_ensemble_workflows(self, ensemble_id):
        return self.session.query(EnsembleWorkflow).filter(EnsembleWorkflow.ensemble_id==ensemble_id).all()

    def get_ensemble_workflow(self, ensemble_id, name):
        try:
            return self.session.query(EnsembleWorkflow).filter(EnsembleWorkflow.ensemble_id==ensemble_id, EnsembleWorkflow.name==name).one()
        except NoResultFound:
            raise APIError("No such ensemble workflow: %s" % name, 404)

    def create_ensemble_workflow(self, ensemble_id, name, priority, bundlefile,
            sites, output_site, staging_sites=None, clustering=None,
            force=None, cleanup=None):

        # Verify that the workflow doesn't already exist
        q = self.session.query(EnsembleWorkflow)
        q = q.filter(EnsembleWorkflow.ensemble_id==ensemble_id,
                     EnsembleWorkflow.name==name)
        if q.count() > 0:
            raise APIError("Ensemble workflow %s already exists" % name, 400)

        # Create database record
        w = EnsembleWorkflow(ensemble_id, name)
        w.set_priority(priority)
        self.session.add(w)
        self.session.flush()

        dirname = w.get_dir()

        # If the directory already exists, then we need to remove it
        if os.path.isdir(dirname):
            shutil.rmtree(dirname)

        # Create the workflow directory
        os.makedirs(dirname)

        # Save bundle
        bundlefilename = werkzeug.secure_filename(bundlefile.filename)
        bundlepath = os.path.join(dirname, bundlefilename)
        f = open(bundlepath, "wb")
        try:
            shutil.copyfileobj(bundlefile, f)
        finally:
            f.close()

        # Verify and unpack the bundle
        bundle = Bundle(bundlepath)
        bundle.verify()
        bundle.unpack(dirname)

        properties = bundle.get_properties()

        # TODO Filter out properties that are not allowed and rewrite properties

        # Get path to dax file
        dax = properties["pegasus.dax.file"]

        # Create planning script
        planfile = os.path.join(dirname, "plan.sh")
        f = open(planfile, "w")
        try:
            self.write_planning_script(f, dirname, dax, sites=sites,
                    output_site=output_site, staging_sites=staging_sites,
                    clustering=clustering, force=force, cleanup=cleanup)
        finally:
            f.close()
        os.chmod(planfile, stat.S_IRWXU|stat.S_IRGRP|stat.S_IXGRP|stat.S_IROTH|stat.S_IXOTH)

        return w

    def write_planning_script(self, f, dirname, dax, sites, output_site,
            staging_sites=None, clustering=None, force=False, cleanup=None):

        f.write("#!/bin/bash\n")
        f.write("pegasus-plan \\\n")

        # We need to make sure that the dashboard info is
        # sent to the same database we are using
        f.write("-Dpegasus.dashboard.output=%s \\\n" % self.dburi)

        f.write("--conf pegasus.properties \\\n")
        f.write("--site %s \\\n" % ",".join(sites))
        f.write("--output-site %s \\\n" % output_site)

        if staging_sites is not None and len(staging_sites) > 0:
            pairs = ["%s=%s" % (k,v) for k,v in staging_sites.items()]
            f.write("--staging-site %s \\\n" % ",".join(pairs))

        if clustering is not None and len(clustering) > 0:
            f.write("--cluster %s \\\n" % ",".join(clustering))

        if force:
            f.write("--force \\\n")

        if cleanup is not None:
            f.write("--cleanup %s\\\n" % cleanup)

        f.write("--dir submit \\\n")
        f.write("--dax %s\n" % dax)
        f.write("--input-dir %s\n" % dirname)

        f.write("exit $?")

