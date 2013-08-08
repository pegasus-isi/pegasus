import os
import sys
import re
import shutil
from StringIO import StringIO
from datetime import datetime
from flask import g, url_for, make_response, request, send_file, json

from sqlalchemy.orm.exc import NoResultFound

from pegasus.service import app, db, catalogs
from pegasus.service.api import *
from pegasus.service.command import ClientCommand, CompoundCommand

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

    def set_priority(self, priority):
        self.priority = validate_priority(priority)

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
EnsembleWorkflowStates = States(["READY","PLANNING","QUEUED","RUNNING",
                                 "FAILED","SUCCESSFUL","ABORTED"])

class Ensemble(db.Model, EnsembleMixin):
    __tablename__ = 'ensemble'
    __table_args__ = (
        db.UniqueConstraint('user_id', 'name'),
        {'mysql_engine': 'InnoDB'}
    )

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    created = db.Column(db.DateTime, nullable=False)
    updated = db.Column(db.DateTime, nullable=False)
    state = db.Column(db.Enum(*EnsembleStates), nullable=False)
    priority = db.Column(db.Integer, nullable=False)
    max_running = db.Column(db.Integer, nullable=False)
    max_planning = db.Column(db.Integer, nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'))
    user = db.relationship("User")

    def __init__(self, user_id, name):
        self.user_id = user_id
        self.set_name(name)
        self.set_created()
        self.set_updated()
        self.state = EnsembleStates.ACTIVE
        self.set_priority(0)
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
        return os.path.join(self.user.get_userdata_dir(), "ensembles", self.name)

    def get_object(self):
        return {
            "id": self.id,
            "name": self.name,
            "created": self.created,
            "updated": self.updated,
            "state": self.state,
            "priority": self.priority,
            "max_running": self.max_running,
            "max_planning": self.max_planning,
            "href": url_for("route_get_ensemble", name=self.name, _external=True)
        }

    def get_detail_object(self):
        obj = self.get_object()
        obj["workflows"] = [w.get_object() for w in self.workflows]
        return obj

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
    ensemble_id = db.Column(db.Integer, db.ForeignKey('ensemble.id'))
    ensemble = db.relationship("Ensemble", backref="workflows")

    def __init__(self, ensemble_id, name):
        self.ensemble_id = ensemble_id
        self.set_name(name)
        self.set_created()
        self.set_updated()
        self.state = EnsembleWorkflowStates.READY
        self.set_priority(0)

    def set_state(self, state):
        state = state.upper()
        if state not in EnsembleWorkflowStates:
            raise APIError("Invalid ensemble workflow state: %s" % state)
        self.state = state

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
            "href": url_for("route_get_ensemble_workflow", ensemble=self.ensemble.name, workflow=self.name, _external=True)
        }

    def get_detail_object(self):
        def myurl_for(filename):
            return url_for("route_get_ensemble_workflow_file",
                           ensemble=self.ensemble.name, workflow=self.name,
                           filename=filename, _external=True)
        o = self.get_object()
        o["dax"] = myurl_for("dax")
        o["replicas"] = myurl_for("replicas")
        o["transformations"] = myurl_for("transformations")
        o["sites"] = myurl_for("sites")
        o["properties"] = myurl_for("properties")
        return o

def list_ensembles(user_id):
    ensembles = Ensemble.query.filter_by(user_id=user_id).all()
    return ensembles

def get_ensemble(user_id, name):
    try:
        return Ensemble.query.filter_by(user_id=user_id, name=name).one()
    except NoResultFound:
        raise APIError("No such ensemble: %s" % name, 404)

def create_ensemble(user_id, name, priority, max_running, max_planning):
    ensemble = Ensemble(g.user.id, name)
    ensemble.set_priority(priority)
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

def create_ensemble_workflow(ensemble_id, name, priority, rc, tc, sc, dax, props):
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
        if props is not None:
            shutil.copyfileobj(props, f)
    finally:
        f.close()

@app.route("/ensembles", methods=["GET"])
def route_list_ensembles():
    ensembles = list_ensembles(g.user.id)
    result = [e.get_object() for e in ensembles]
    return json_response(result)

@app.route("/ensembles", methods=["POST"])
def route_create_ensemble():
    name = request.form.get("name", None)
    if name is None:
        raise APIError("Specify ensemble name")

    priority = request.form.get("priority", 0)
    max_running = request.form.get("max_running", 1)
    max_planning = request.form.get("max_planning", 1)

    create_ensemble(g.user.id, name, priority, max_running, max_planning)

    db.session.commit()

    return json_created(url_for("route_get_ensemble", name=name, _external=True))

@app.route("/ensembles/<string:name>", methods=["GET"])
def route_get_ensemble(name):
    e = get_ensemble(g.user.id, name)
    result = e.get_detail_object()
    return json_response(result)

@app.route("/ensembles/<string:name>", methods=["PUT","POST"])
def route_update_ensemble(name):
    e = get_ensemble(g.user.id, name)

    priority = request.form.get("priority", None)
    if priority is not None:
        e.set_priority(priority)

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
    e = get_ensemble(g.user.id, name)
    result = [w.get_object() for w in e.workflows]
    return json_response(result)

@app.route("/ensembles/<string:ensemble>/workflows", methods=["POST"])
def route_create_ensemble_workflow(ensemble):
    e = get_ensemble(g.user.id, ensemble)

    name = request.form.get("name", None)
    if name is None:
        raise APIError("Specify ensemble workflow name")

    priority = request.form.get("priority", 0)

    sites = request.form.get("sites", None)
    if sites is None: raise APIError("Specify sites")

    transformations = request.form.get("transformations", None)
    if transformations is None: raise APIError("Specify transformations")

    replicas = request.form.get("replicas", None)
    if replicas is None: raise APIError("Specify replicas")

    dax = request.files.get("dax", None)
    if dax is None:
        raise APIError("Specify dax")

    props = request.files.get("properties", None)

    sc = catalogs.get_catalog("site", g.user, sites)
    tc = catalogs.get_catalog("transformation", g.user, transformations)
    rc = catalogs.get_catalog("replica", g.user, replicas)

    create_ensemble_workflow(e.id, name, priority, rc, tc, sc, dax, props)

    db.session.commit()

    return json_created(url_for("route_get_ensemble_workflow", ensemble=ensemble, workflow=name))

@app.route("/ensembles/<string:ensemble>/workflows/<string:workflow>", methods=["GET"])
def route_get_ensemble_workflow(ensemble, workflow):
    e = get_ensemble(g.user.id, ensemble)
    w = get_ensemble_workflow(e.id, workflow)
    result = w.get_detail_object()
    return json_response(result)

@app.route("/ensembles/<string:ensemble>/workflows/<string:workflow>", methods=["PUT","POST"])
def route_update_ensemble_workflow(ensemble, workflow):
    e = get_ensemble(g.user.id, ensemble)
    w = get_ensemble_workflow(e.id, workflow)

    priority = request.form.get("priority", None)
    if priority is not None:
        w.set_priority(priority)

    w.set_updated()

    db.session.commit()

    return json_response(w.get_detail_object())

@app.route("/ensembles/<string:ensemble>/workflows/<string:workflow>/<string:filename>", methods=["GET"])
def route_get_ensemble_workflow_file(ensemble, workflow, filename):
    e = get_ensemble(g.user.id, ensemble)
    w = get_ensemble_workflow(e.id, workflow)
    dirname = w.get_dir()
    mimetype = "text/plain"
    if filename == "sites":
        path = os.path.join(dirname, "sites.xml")
    elif filename == "transformations":
        path = os.path.join(dirname, "tc.txt")
    elif filename == "replicas":
        path = os.path.join(dirname, "rc.txt")
    elif filename == "dax":
        path = os.path.join(dirname, "dax.xml")
        mimetype = "application/xml"
    elif filename == "properties":
        path = os.path.join(dirname, "pegasus.properties")
    else:
        raise APIError("Invalid file: %s" % filename)

    return send_file(path, mimetype=mimetype)


#TODO Change --name to --ensemble
def add_name_option(self):
    self.parser.add_option("-n", "--name", action="store", dest="name",
        default=None, help="Ensemble name")

class ListCommand(ClientCommand):
    description = "List ensembles"
    usage = "Usage: %prog list"

    def run(self):
        response = self.get("/ensembles")
        result = response.json()

        if response.status_code != 200:
            print "ERROR:",result["message"]
            exit(1)

        fmt = "%-20s %-8s %-30s %-30s %8s %14s %12s"
        if len(result) > 0:
            print fmt % ("NAME","STATE","CREATED","UPDATED","PRIORITY","MAX PLANNING","MAX RUNNING")
        for r in result:
            print fmt % (r["name"], r["state"], r["created"], r["updated"], r["priority"], r["max_planning"], r["max_running"])

class CreateCommand(ClientCommand):
    description = "Create ensemble"
    usage = "Usage: %prog create ..."

    def __init__(self):
        ClientCommand.__init__(self)
        add_name_option(self)
        self.parser.add_option("-p", "--priority", action="store", dest="priority",
            default=0, type="int", help="Ensemble priority")
        self.parser.add_option("-P", "--max-planning", action="store", dest="max_planning",
            default=1, type="int", help="Maximum number of workflows being planned at once")
        self.parser.add_option("-R", "--max-running", action="store", dest="max_running",
            default=1, type="int", help="Maximum number of workflows running at once")

    def run(self):
        if self.options.name is None:
            parser.error("Specify -n/--name")

        request = {
            "name": self.options.name,
            "priority": self.options.priority,
            "max_planning": self.options.max_planning,
            "max_running": self.options.max_running
        }

        response = self.post("/ensembles", data=request)

        if response.status_code != 201:
            result = response.json()
            print "ERROR:", result["message"]
            exit(1)

class SubmitCommand(ClientCommand):
    description = "Submit ensemble workflow"
    usage = "Usage: %prog submit ..."

    def __init__(self):
        ClientCommand.__init__(self)

    def run(self):
        # TODO Finish submit command
        raise Exception("Not implemented")

class ShowCommand(ClientCommand):
    description = "Show workflows in ensemble"
    usage = "Usage: %prog show ..."

    def run(self):
        # TODO Finish ensemble show command
        raise Exception("Not implemented")

class StateChangeCommand(ClientCommand):
    def __init__(self):
        ClientCommand.__init__(self)
        add_name_option(self)

    def run(self):
        if self.options.name is None:
            self.parser.error("Specify -n/--name")

        response = self.post("/ensembles/%s" % self.options.name, data={"state":self.newstate})
        result = response.json()

        if response.status_code != 200:
            print "ERROR:",result["message"]

        print "State:", result["state"]

class PauseCommand(StateChangeCommand):
    description = "Pause ensemble"
    usage = "Usage: %prog pause -n NAME"
    newstate = EnsembleStates.PAUSED

class ActivateCommand(StateChangeCommand):
    description = "Activate ensemble"
    usage = "Usage: %prog activate -n NAME"
    newstate = EnsembleStates.ACTIVE

class HoldCommand(StateChangeCommand):
    description = "Hold ensemble"
    usage = "Usage: %prog hold -n NAME"
    newstate = EnsembleStates.HELD

class UpdateCommand(ClientCommand):
    description = "Update ensemble"
    usage = "Usage: %prog update ..."

    def __init__(self):
        ClientCommand.__init__(self)
        add_name_option(self)
        self.parser.add_option("-p", "--priority", action="store", dest="priority",
            default=None, type="int", help="Ensemble priority")
        self.parser.add_option("-P", "--max-planning", action="store", dest="max_planning",
            default=None, type="int", help="Maximum number of workflows being planned at once")
        self.parser.add_option("-R", "--max-running", action="store", dest="max_running",
            default=None, type="int", help="Maximum number of workflows running at once")

    def run(self):
        if self.options.name is None:
            self.parser.error("Specify -n/--name")

        request = {}

        if self.options.priority:
            request["priority"] = self.options.priority
        if self.options.max_planning:
            request["max_planning"] = self.options.max_planning
        if self.options.max_running:
            request["max_running"] = self.options.max_running

        if len(request) == 0:
            self.parser.error("Specify --priority, --max-planning or --max-running")

        response = self.post("/ensembles/%s" % self.options.name, data=request)

        result = response.json()

        if response.status_code != 200:
            print "ERROR:", result["message"]
            exit(1)

        print "Name:",result["name"]
        print "State:",result["state"]
        print "Created:",result["created"]
        print "Updated:",result["updated"]
        print "Priority:",result["priority"]
        print "Max Planning:",result["max_planning"]
        print "Max Running:",result["max_running"]

class EnsembleCommand(CompoundCommand):
    description = "Client for ensemble management"
    commands = {
        "list": ListCommand,
        "create": CreateCommand,
        "pause": PauseCommand,
        "activate": ActivateCommand,
        "hold": HoldCommand,
        "update": UpdateCommand,
        "submit": SubmitCommand,
        "show": ShowCommand
    }

def main():
    "The entry point for pegasus-service-ensemble"
    EnsembleCommand().main()

