import os
import sys
import re
import stat
import shutil
from StringIO import StringIO
from datetime import datetime
from flask import g, url_for, make_response, request, send_file, json

from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy import sql

from Pegasus.service import app, db, catalogs
from Pegasus.service.api import *
from Pegasus.service.command import ClientCommand, CompoundCommand

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

class EnsembleMixin:
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

class Ensemble(db.Model, EnsembleMixin):
    __tablename__ = 'ensemble'
    __table_args__ = (
        db.UniqueConstraint('username', 'name'),
        {'mysql_engine': 'InnoDB'}
    )

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    created = db.Column(db.DateTime, nullable=False)
    updated = db.Column(db.DateTime, nullable=False)
    state = db.Column(db.Enum(*EnsembleStates), nullable=False)
    max_running = db.Column(db.Integer, nullable=False)
    max_planning = db.Column(db.Integer, nullable=False)
    username = db.Column(db.String(100), nullable=False)

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

class EnsembleWorkflow(db.Model, EnsembleMixin):
    __tablename__ = 'ensemble_workflow'
    __table_args__ = (
        db.UniqueConstraint('ensemble_id', 'name'),
        {'mysql_engine': 'InnoDB'}
    )

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    created = db.Column(db.DateTime, nullable=False)
    updated = db.Column(db.DateTime, nullable=False)
    state = db.Column(db.Enum(*EnsembleWorkflowStates), nullable=False)
    priority = db.Column(db.Integer, nullable=False)
    wf_uuid = db.Column(db.String(36))
    submitdir = db.Column(db.String(512))
    ensemble_id = db.Column(db.Integer, db.ForeignKey('ensemble.id'), nullable=False)
    ensemble = db.relationship("Ensemble", backref="workflows")

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

def list_ensembles(username):
    ensembles = Ensemble.query.filter_by(username=username).all()
    return ensembles

def list_actionable_ensembles():
    states = (
        EnsembleWorkflowStates.READY,
        EnsembleWorkflowStates.PLANNING,
        EnsembleWorkflowStates.QUEUED,
        EnsembleWorkflowStates.RUNNING
    )
    stmt = sql.exists().where(Ensemble.id==EnsembleWorkflow.ensemble_id).where(EnsembleWorkflow.state.in_(states))
    return Ensemble.query.filter(stmt).all()

def get_ensemble(username, name):
    try:
        return Ensemble.query.filter_by(username=username, name=name).one()
    except NoResultFound:
        raise APIError("No such ensemble: %s" % name, 404)

def create_ensemble(username, name, max_running, max_planning):
    if Ensemble.query.filter_by(username=username, name=name).count() > 0:
        raise APIError("Ensemble %s already exists" % name, 400)

    ensemble = Ensemble(username, name)
    ensemble.set_max_running(max_running)
    ensemble.set_max_planning(max_planning)
    db.session.add(ensemble)
    db.session.flush()
    return ensemble

def list_ensemble_workflows(ensemble_id):
    return EnsembleWorkflow.query.filter_by(ensemble_id=ensemble_id).all()

def get_ensemble_workflow(ensemble_id, name):
    try:
        return EnsembleWorkflow.query.filter_by(ensemble_id=ensemble_id, name=name).one()
    except NoResultFound:
        raise APIError("No such ensemble workflow: %s" % name, 404)

def create_ensemble_workflow(ensemble_id, name, priority, rc, tc, sc, dax, conf, sites, output_site,
        staging_sites=None, clustering=None, force=False, cleanup=True):

    if EnsembleWorkflow.query.filter_by(ensemble_id=ensemble_id, name=name).count() > 0:
        raise APIError("Ensemble workflow %s already exists" % name, 400)

    # Create database record
    w = EnsembleWorkflow(ensemble_id, name)
    w.set_priority(priority)
    db.session.add(w)
    db.session.flush()

    dirname = w.get_dir()

    # If the directory already exists, then we need to remove it
    if os.path.isdir(dirname):
        shutil.rmtree(dirname)

    # Create the workflow directory
    os.makedirs(dirname)

    # Save catalogs
    rcfile = os.path.join(dirname, "rc.txt")
    shutil.copyfile(rc.get_catalog_file(), rcfile)
    tcfile = os.path.join(dirname, "tc.txt")
    shutil.copyfile(tc.get_catalog_file(), tcfile)
    scfile = os.path.join(dirname, "sites.xml")
    shutil.copyfile(sc.get_catalog_file(), scfile)

    # Save dax
    daxfile = os.path.join(dirname, "dax.xml")
    f = open(daxfile, "wb")
    try:
        shutil.copyfileobj(dax, f)
    finally:
        f.close()

    # Save properties file
    propsfile = os.path.join(dirname, "pegasus.properties")
    f = open(propsfile, "wb")
    try:
        # It is possible that there is no properties file
        # but we still need to create the file
        if conf is not None:
            # TODO Filter out properties that are not allowed
            shutil.copyfileobj(conf, f)

        # We need to make sure that the dashboard info is
        # sent to the same database we are using
        f.write("\npegasus.dashboard.output=%s\n" % app.config["SQLALCHEMY_DATABASE_URI"])
    finally:
        f.close()

    # Create planning script
    filename = os.path.join(dirname, "plan.sh")
    f = open(filename, "w")
    try:
        write_planning_script(f, tcformat=tc.format, rcformat=rc.format, scformat=sc.format,
                sites=sites, output_site=output_site, staging_sites=staging_sites,
                clustering=clustering, force=force, cleanup=cleanup)
    finally:
        f.close()
    os.chmod(filename, stat.S_IRWXU|stat.S_IRGRP|stat.S_IXGRP|stat.S_IROTH|stat.S_IXOTH)

    return w

def write_planning_script(f, tcformat, rcformat, scformat, sites, output_site,
        staging_sites=None, clustering=None, force=False, cleanup=True):

    f.write("#!/bin/bash\n")
    f.write("pegasus-plan \\\n")
    f.write("-Dpegasus.catalog.site=%s \\\n" % scformat)
    f.write("-Dpegasus.catalog.site.file=sites.xml \\\n")
    f.write("-Dpegasus.catalog.transformation=%s \\\n" % tcformat)
    f.write("-Dpegasus.catalog.transformation.file=tc.txt \\\n")
    f.write("-Dpegasus.catalog.replica=%s \\\n" % rcformat)
    f.write("-Dpegasus.catalog.replica.file=rc.txt \\\n")
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

    if not cleanup:
        f.write("--nocleanup \\\n")

    f.write("--dir submit \\\n")
    f.write("--dax dax.xml\n")
    f.write("exit $?")

@app.route("/ensembles", methods=["GET"])
def route_list_ensembles():
    ensembles = list_ensembles(g.username)
    result = [e.get_object() for e in ensembles]
    return json_response(result)

@app.route("/ensembles", methods=["POST"])
def route_create_ensemble():
    name = request.form.get("name", None)
    if name is None:
        raise APIError("Specify ensemble name")

    max_running = request.form.get("max_running", 1)
    max_planning = request.form.get("max_planning", 1)

    create_ensemble(g.username, name, max_running, max_planning)

    db.session.commit()

    return json_created(url_for("route_get_ensemble", name=name, _external=True))

@app.route("/ensembles/<string:name>", methods=["GET"])
def route_get_ensemble(name):
    e = get_ensemble(g.username, name)
    result = e.get_object()
    return json_response(result)

@app.route("/ensembles/<string:name>", methods=["PUT","POST"])
def route_update_ensemble(name):
    e = get_ensemble(g.username, name)

    max_running = request.form.get("max_running", None)
    if max_running is not None:
        e.set_max_running(max_running)

    max_planning = request.form.get("max_planning", None)
    if max_planning is not None:
        e.set_max_planning(max_planning)

    state = request.form.get("state", None)
    if state is not None:
        if state != e.state:
            # TODO Do the necessary state transition
            e.set_state(state)

    e.set_updated()

    db.session.commit()

    return json_response(e.get_object())

@app.route("/ensembles/<string:name>/workflows", methods=["GET"])
def route_list_ensemble_workflows(name):
    e = get_ensemble(g.username, name)
    result = [w.get_object() for w in e.workflows]
    return json_response(result)

@app.route("/ensembles/<string:ensemble>/workflows", methods=["POST"])
def route_create_ensemble_workflow(ensemble):
    e = get_ensemble(g.username, ensemble)

    name = request.form.get("name", None)
    if name is None:
        raise APIError("Specify ensemble workflow name")

    priority = request.form.get("priority", 0)

    site_catalog = request.form.get("site_catalog", None)
    if site_catalog is None:
        raise APIError("Specify site_catalog")

    transformation_catalog = request.form.get("transformation_catalog", None)
    if transformation_catalog is None:
        raise APIError("Specify transformation_catalog")

    replica_catalog = request.form.get("replica_catalog", None)
    if replica_catalog is None:
        raise APIError("Specify replica_catalog")

    sites = request.form.get("sites", None)
    if sites is None:
        raise APIError("Specify sites")
    else:
        sites = [s.strip() for s in sites.split(",")]
        sites = [s for s in sites if len(s) > 0]
    if len(sites) == 0:
        raise APIError("Specify sites")

    output_site = request.form.get("output_site", None)
    if output_site is None:
        raise APIError("Specify output_site")

    cleanup = request.form.get("cleanup", None)
    if cleanup is not None:
        if cleanup.lower() not in ["true","false"]:
            raise APIError("Invalid value for cleanup: %s" % cleanup)
        cleanup = cleanup.lower() == "true"

    force = request.form.get("force", None)
    if force is not None:
        if force.lower() not in ["true","false"]:
            raise APIError("Invalid value for force: %s" % force)
        force = force.lower() == "true"

    clustering = request.form.get("clustering", None)
    if clustering is not None:
        clustering = [s.strip() for s in clustering.split(",")]
        clustering = [s for s in clustering if len(s) > 0]

    staging_sites = request.form.get("staging_sites", None)
    if staging_sites is not None:
        kvs = [s.strip() for s in staging_sites.split(",")]
        kvs = [s for s in kvs if len(s) > 0]
        staging_sites = dict([s.split("=") for s in kvs])

    dax = request.files.get("dax", None)
    if dax is None:
        raise APIError("Specify dax")

    conf = request.files.get("conf", None)

    sc = catalogs.get_catalog("site", g.username, site_catalog)
    tc = catalogs.get_catalog("transformation", g.username, transformation_catalog)
    rc = catalogs.get_catalog("replica", g.username, replica_catalog)

    create_ensemble_workflow(e.id, name, priority, rc, tc, sc, dax, conf,
            sites=sites, output_site=output_site, cleanup=cleanup,
            force=force, clustering=clustering, staging_sites=staging_sites)

    db.session.commit()

    return json_created(url_for("route_get_ensemble_workflow", ensemble=ensemble, workflow=name))

@app.route("/ensembles/<string:ensemble>/workflows/<string:workflow>", methods=["GET"])
def route_get_ensemble_workflow(ensemble, workflow):
    e = get_ensemble(g.username, ensemble)
    w = get_ensemble_workflow(e.id, workflow)
    result = w.get_detail_object()
    return json_response(result)

@app.route("/ensembles/<string:ensemble>/workflows/<string:workflow>", methods=["PUT","POST"])
def route_update_ensemble_workflow(ensemble, workflow):
    e = get_ensemble(g.username, ensemble)
    w = get_ensemble_workflow(e.id, workflow)

    priority = request.form.get("priority", None)
    if priority is not None:
        w.set_priority(priority)

    state = request.form.get("state", None)
    if state is not None:
        w.change_state(state)

    w.set_updated()

    db.session.commit()

    return json_response(w.get_detail_object())

@app.route("/ensembles/<string:ensemble>/workflows/<string:workflow>/<string:filename>", methods=["GET"])
def route_get_ensemble_workflow_file(ensemble, workflow, filename):
    e = get_ensemble(g.username, ensemble)
    w = get_ensemble_workflow(e.id, workflow)
    dirname = w.get_dir()
    mimetype = "text/plain"
    if filename == "sites.xml":
        path = os.path.join(dirname, "sites.xml")
    elif filename == "tc.txt":
        path = os.path.join(dirname, "tc.txt")
    elif filename == "rc.txt":
        path = os.path.join(dirname, "rc.txt")
    elif filename == "dax.xml":
        path = os.path.join(dirname, "dax.xml")
        mimetype = "application/xml"
    elif filename == "pegasus.properties":
        path = os.path.join(dirname, "pegasus.properties")
    elif filename == "plan.sh":
        path = os.path.join(dirname, "plan.sh")
    else:
        raise APIError("Invalid file: %s" % filename)

    return send_file(path, mimetype=mimetype)


def add_ensemble_option(self):
    self.parser.add_option("-e", "--ensemble", action="store", dest="ensemble",
        default=None, help="Ensemble name")

def add_workflow_option(self):
    self.parser.add_option("-w", "--workflow", action="store", dest="workflow",
        default=None, help="Workflow name")

class EnsemblesCommand(ClientCommand):
    description = "List ensembles"
    usage = "Usage: %prog ensembles"

    def run(self):
        response = self.get("/ensembles")
        result = response.json()

        if response.status_code != 200:
            print "ERROR:",result["message"]
            exit(1)

        fmt = "%-20s %-8s %-30s %-30s %14s %12s"
        if len(result) > 0:
            print fmt % ("NAME","STATE","CREATED","UPDATED","MAX PLANNING","MAX RUNNING")
        for r in result:
            print fmt % (r["name"], r["state"], r["created"], r["updated"], r["max_planning"], r["max_running"])

class CreateCommand(ClientCommand):
    description = "Create ensemble"
    usage = "Usage: %prog create [options] -e ENSEMBLE"

    def __init__(self):
        ClientCommand.__init__(self)
        add_ensemble_option(self)
        self.parser.add_option("-P", "--max-planning", action="store", dest="max_planning",
            default=1, type="int", help="Maximum number of workflows being planned at once")
        self.parser.add_option("-R", "--max-running", action="store", dest="max_running",
            default=1, type="int", help="Maximum number of workflows running at once")

    def run(self):
        if self.options.ensemble is None:
            self.parser.error("Specify -e/--ensemble")

        request = {
            "name": self.options.ensemble,
            "max_planning": self.options.max_planning,
            "max_running": self.options.max_running
        }

        response = self.post("/ensembles", data=request)

        if response.status_code != 201:
            result = response.json()
            print "ERROR:", result["message"]
            exit(1)

class SubmitCommand(ClientCommand):
    description = "Submit workflow"
    usage = "Usage: %prog submit [options] -e ENSEMBLE -w WORKFLOW -d DAX -T TC -S SC -R RC -s SITE -o SITE"

    def __init__(self):
        ClientCommand.__init__(self)
        add_ensemble_option(self)
        add_workflow_option(self)
        self.parser.add_option("-d", "--dax", action="store", dest="dax",
            default=None, help="DAX file", metavar="PATH")
        self.parser.add_option("-T", "--transformation-catalog", action="store", dest="transformation_catalog",
            default=None, help="Name of transformation catalog", metavar="NAME")
        self.parser.add_option("-S", "--site-catalog", action="store", dest="site_catalog",
            default=None, help="Name of site catalog", metavar="NAME")
        self.parser.add_option("-R", "--replica-catalog", action="store", dest="replica_catalog",
            default=None, help="Name of replica catalog", metavar="NAME")
        self.parser.add_option("-s", "--site", action="store", dest="sites",
            default=None, help="Execution sites (see pegasus-plan man page)", metavar="SITE[,SITE...]")
        self.parser.add_option("-o", "--output-site", action="store", dest="output_site",
            default=None, help="Output storage site (see pegasus-plan man page)", metavar="SITE")

        self.parser.add_option("-p", "--priority", action="store", dest="priority",
            default=0, help="Workflow priority", metavar="NUMBER")
        self.parser.add_option("-c", "--conf", action="store", dest="conf",
            default=None, help="Configuration file (pegasus properties file)", metavar="PATH")
        self.parser.add_option("--staging-site", action="store", dest="staging_sites",
            default=None, help="Staging sites (see pegasus-plan man page)", metavar="s=ss[,s=ss...]")
        self.parser.add_option("--nocleanup", action="store_false", dest="cleanup",
            default=None, help="Add cleanup jobs (see pegasus-plan man page)")
        self.parser.add_option("-f", "--force", action="store_true", dest="force",
            default=None, help="Skip workflow reduction (see pegasus-plan man page)")
        self.parser.add_option("-C", "--cluster", action="store", dest="clustering",
            default=None, help="Clustering techniques to apply (see pegasus-plan man page)", metavar="STYLE[,STYLE...]")

    def run(self):
        o = self.options
        p = self.parser

        if o.ensemble is None:
            p.error("Specify -e/--ensemble")
        if o.workflow is None:
            p.error("Specify -w/--workflow")
        if o.dax is None:
            p.error("Specify -d/--dax")
        if o.transformation_catalog is None:
            p.error("Specify -T/--transformation-catalog")
        if o.site_catalog is None:
            p.error("Specify -S/--site-catalog")
        if o.replica_catalog is None:
            p.error("Specify -R/--replica-catalog")
        if o.sites is None:
            p.error("Specify -s/--site")
        if o.output_site is None:
            p.error("Specify -o/--output-site")

        data = {
            "name": o.workflow,
            "transformation_catalog": o.transformation_catalog,
            "site_catalog": o.site_catalog,
            "replica_catalog": o.replica_catalog,
            "priority": o.priority,
            "sites": o.sites,
            "output_site": o.output_site
        }

        if o.cleanup is not None:
            data["cleanup"] = o.cleanup

        if o.force is not None:
            data["force"] = o.force

        if o.staging_sites is not None:
            data["staging_sites"] = o.staging_sites

        if o.clustering is not None:
            data["clustering"] = [s.strip() for s in o.clustering.split(",")]

        files = {
            "dax": open(o.dax, "rb")
        }

        if o.conf is not None:
            files["conf"] = open(o.conf, "rb")

        response = self.post("/ensembles/%s/workflows" % o.ensemble, data=data, files=files)

        if response.status_code != 201:
            result = response.json()
            print "ERROR:",response.status_code,result["message"]

class WorkflowsCommand(ClientCommand):
    description = "List workflows in ensemble"
    usage = "Usage: %prog workflows [options] -e ENSEMBLE."

    def __init__(self):
        ClientCommand.__init__(self)
        add_ensemble_option(self)
        self.parser.add_option("-l", "--long", action="store_true", dest="long",
            default=False, help="Show detailed output")

    def run(self):
        if self.options.ensemble is None:
            self.parser.error("Specify -e/--ensemble")
        if len(self.args) > 0:
            self.parser.error("Invalid argument")

        response = self.get("/ensembles/%s/workflows" % self.options.ensemble)

        result = response.json()

        if response.status_code != 200:
            print "ERROR:",response.status_code,result["message"]
            exit(1)

        if len(result) == 0:
            return

        if self.options.long:
            for w in result:
                print "ID:      ",w["id"]
                print "Name:    ",w["name"]
                print "Created: ",w["created"]
                print "Updated: ",w["updated"]
                print "State:   ",w["state"]
                print "Priority:",w["priority"]
                print "UUID:    ",w["wf_uuid"]
                print "URL:     ",w["href"]
                print
        else:
            fmt = "%-20s %-15s %-8s %-30s %-30s"
            print fmt % ("NAME","STATE","PRIORITY","CREATED","UPDATED")
            for w in result:
                print fmt % (w["name"],w["state"],w["priority"],w["created"],w["updated"])

class StateChangeCommand(ClientCommand):
    def __init__(self):
        ClientCommand.__init__(self)
        add_ensemble_option(self)

    def run(self):
        if self.options.ensemble is None:
            self.parser.error("Specify -e/--ensemble")

        response = self.post("/ensembles/%s" % self.options.ensemble, data={"state":self.newstate})
        result = response.json()

        if response.status_code != 200:
            print "ERROR:",result["message"]

        print "State:", result["state"]

class PauseCommand(StateChangeCommand):
    description = "Pause active ensemble"
    usage = "Usage: %prog pause -e ENSEMBLE"
    newstate = EnsembleStates.PAUSED

class ActivateCommand(StateChangeCommand):
    description = "Activate paused or held ensemble"
    usage = "Usage: %prog activate -e ENSEMBLE"
    newstate = EnsembleStates.ACTIVE

class HoldCommand(StateChangeCommand):
    description = "Hold active ensemble"
    usage = "Usage: %prog hold -e ENSEMBLE"
    newstate = EnsembleStates.HELD

class ConfigCommand(ClientCommand):
    description = "Change ensemble configuration"
    usage = "Usage: %prog config [options] -e ENSEMBLE"

    def __init__(self):
        ClientCommand.__init__(self)
        add_ensemble_option(self)
        self.parser.add_option("-P", "--max-planning", action="store", dest="max_planning",
            default=None, type="int", help="Maximum number of workflows being planned at once")
        self.parser.add_option("-R", "--max-running", action="store", dest="max_running",
            default=None, type="int", help="Maximum number of workflows running at once")

    def run(self):
        if self.options.ensemble is None:
            self.parser.error("Specify -e/--ensemble")

        request = {}

        if self.options.max_planning:
            request["max_planning"] = self.options.max_planning
        if self.options.max_running:
            request["max_running"] = self.options.max_running

        if len(request) == 0:
            self.parser.error("Specify --max-planning or --max-running")

        response = self.post("/ensembles/%s" % self.options.ensemble, data=request)

        result = response.json()

        if response.status_code != 200:
            print "ERROR:", result["message"]
            exit(1)

        print "Max Planning:",result["max_planning"]
        print "Max Running:",result["max_running"]

class WorkflowStateChangeCommand(ClientCommand):
    def __init__(self):
        ClientCommand.__init__(self)
        add_ensemble_option(self)
        add_workflow_option(self)

    def run(self):
        if self.options.ensemble is None:
            self.parser.error("Specify -e/--ensemble")
        if self.options.workflow is None:
            self.parser.error("Specify -w/--workflow")

        if len(self.args) > 0:
            self.parser.error("Invalid argument")

        request = {"state": self.newstate}

        response = self.post("/ensembles/%s/workflows/%s" % (self.options.ensemble, self.options.workflow), data=request)

        result = response.json()

        if response.status_code != 200:
            print "ERROR:", result["message"]
            exit(1)

        print "State:", result["state"]

class ReplanCommand(WorkflowStateChangeCommand):
    description = "Replan failed workflow"
    usage = "Usage: %prog replan -e ENSEMBLE -w WORKFLOW"
    newstate = EnsembleWorkflowStates.READY

class RerunCommand(WorkflowStateChangeCommand):
    description = "Rerun failed workflow"
    usage = "Usage: %prog rerun -e ENSEMBLE -w WORKFLOW"
    newstate = EnsembleWorkflowStates.QUEUED

class AbortCommand(WorkflowStateChangeCommand):
    description = "Abort workflow"
    usage = "Usage: %prog abort -e ENSEMBLE -w WORKFLOW"
    newstate = EnsembleWorkflowStates.ABORTED

class PriorityCommand(ClientCommand):
    description = "Update workflow priority"
    usage = "Usage: %prog priority -e ENSEMBLE -w WORKFLOW -p PRIORITY"

    def __init__(self):
        ClientCommand.__init__(self)
        add_ensemble_option(self)
        add_workflow_option(self)
        self.parser.add_option("-p","--priority",action="store",dest="priority",
                default=None,type="int",help="New workflow priority")

    def run(self):
        if self.options.ensemble is None:
            self.parser.error("Specify -e/--ensemble")
        if self.options.workflow is None:
            self.parser.error("Specify -w/--workflow")
        if self.options.priority is None:
            self.parser.error("Specify -p/--priority")

        if len(self.args) > 0:
            self.parser.error("Invalid argument")

        request = {"priority": self.options.priority}

        response = self.post("/ensembles/%s/workflows/%s" % (self.options.ensemble, self.options.workflow), data=request)

        result = response.json()

        if response.status_code != 200:
            print "ERROR:", result["message"]
            exit(1)

        print "Priority:", result["priority"]

class EnsembleCommand(CompoundCommand):
    description = "Client for ensemble management"
    commands = [
        ("ensembles", EnsemblesCommand),
        ("create", CreateCommand),
        ("pause", PauseCommand),
        ("activate", ActivateCommand),
        ("config", ConfigCommand),
        ("submit", SubmitCommand),
        ("workflows", WorkflowsCommand),
        ("replan", ReplanCommand),
        ("rerun", RerunCommand),
        ("priority", PriorityCommand)
    ]

def main():
    "The entry point for pegasus-service-ensemble"
    EnsembleCommand().main()

