import os
import logging
import subprocess

from flask import g, url_for, make_response, request, send_file, json

from Pegasus.db import connection
from Pegasus.service.ensembles import emapp, api, auth
from Pegasus.db.ensembles import EMError, Ensembles, EnsembleWorkflowStates

log = logging.getLogger(__name__)

def connect():
    log.debug("Connecting to database")
    g.session = connection.connect(g.master_db_url)

def disconnect():
    if "conn" in g:
        log.debug("Disconnecting from database")
        g.session.close()

@emapp.errorhandler(Exception)
def handle_error(e):
    return api.json_api_error(e)

@emapp.before_request
def setup_request():
    resp = auth.authorize_request()
    if resp: return resp
    connect()

@emapp.teardown_request
def teardown_request(exception):
    disconnect()

@emapp.route("/ensembles", methods=["GET"])
def route_list_ensembles():
    dao = Ensembles(g.session)
    ensembles = dao.list_ensembles(g.user.username)
    result = [e.get_object() for e in ensembles]
    return api.json_response(result)

@emapp.route("/ensembles", methods=["POST"])
def route_create_ensemble():
    name = request.form.get("name", None)
    if name is None:
        raise EMError("Specify ensemble name")

    max_running = request.form.get("max_running", 1)
    max_planning = request.form.get("max_planning", 1)

    dao = Ensembles(g.session)
    dao.create_ensemble(g.user.username, name, max_running, max_planning)
    g.session.commit()

    return api.json_created(url_for("route_get_ensemble", name=name, _external=True))

@emapp.route("/ensembles/<string:name>", methods=["GET"])
def route_get_ensemble(name):
    dao = Ensembles(g.session)
    e = dao.get_ensemble(g.user.username, name)
    result = e.get_object()
    return api.json_response(result)

@emapp.route("/ensembles/<string:name>", methods=["PUT","POST"])
def route_update_ensemble(name):
    dao = Ensembles(g.session)
    e = dao.get_ensemble(g.user.username, name)

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

    g.session.commit()

    return api.json_response(e.get_object())

@emapp.route("/ensembles/<string:name>/workflows", methods=["GET"])
def route_list_ensemble_workflows(name):
    dao = Ensembles(g.session)
    e = dao.get_ensemble(g.user.username, name)
    result = [w.get_object() for w in dao.list_ensemble_workflows(e.id)]
    return api.json_response(result)

@emapp.route("/ensembles/<string:ensemble>/workflows", methods=["POST"])
def route_create_ensemble_workflow(ensemble):
    dao = Ensembles(g.session)
    e = dao.get_ensemble(g.user.username, ensemble)

    name = request.form.get("name", None)
    if name is None:
        raise EMError("Specify ensemble workflow 'name'")

    priority = request.form.get("priority", 0)

    basedir = request.form.get("basedir")
    if basedir is None:
        raise EMError("Specify 'basedir' where plan command should be executed")

    plan_command = request.form.get("plan_command")
    if plan_command is None:
        raise EMError("Specify 'plan_command' that should be executed to plan workflow")

    dao.create_ensemble_workflow(e.id, name, basedir, priority, plan_command)

    g.session.commit()

    return api.json_created(url_for("route_get_ensemble_workflow", ensemble=ensemble, workflow=name))

@emapp.route("/ensembles/<string:ensemble>/workflows/<string:workflow>", methods=["GET"])
def route_get_ensemble_workflow(ensemble, workflow):
    dao = Ensembles(g.session)
    e = dao.get_ensemble(g.user.username, ensemble)
    w = dao.get_ensemble_workflow(e.id, workflow)
    result = w.get_detail_object()
    return api.json_response(result)

@emapp.route("/ensembles/<string:ensemble>/workflows/<string:workflow>", methods=["PUT","POST"])
def route_update_ensemble_workflow(ensemble, workflow):
    dao = Ensembles(g.session)

    e = dao.get_ensemble(g.user.username, ensemble)
    w = dao.get_ensemble_workflow(e.id, workflow)

    priority = request.form.get("priority", None)
    if priority is not None:
        w.set_priority(priority)

    state = request.form.get("state", None)
    if state is not None:
        w.change_state(state)

    w.set_updated()

    g.session.commit()

    return api.json_response(w.get_detail_object())

@emapp.route("/ensembles/<string:ensemble>/workflows/<string:workflow>/analyze", methods=["GET"])
def route_analyze_ensemble_workflow(ensemble, workflow):
    dao = Ensembles(g.session)
    e = dao.get_ensemble(g.user.username, ensemble)
    w = dao.get_ensemble_workflow(e.id, workflow)
    report = "".join(analyze(w))
    resp = make_response(report, 200)
    resp.headers['Content-Type'] = 'text/plain'
    return resp

def analyze(workflow):
    w = workflow

    yield "Workflow state is %s\n" % w.state
    yield "Plan command is: %s\n" % w.plan_command

    logfile = w.get_logfile()
    if os.path.isfile(logfile):
        yield "Workflow log:\n"
        for l in open(w.get_logfile(), "rb"):
            yield "LOG: %s" % l
    else:
        yield "No workflow log available\n"

    if w.submitdir is None or not os.path.isdir(w.submitdir):
        yield "No submit directory available\n"
    else:
        yield "pegasus-analyzer output is:\n"
        p = subprocess.Popen(["pegasus-analyzer", w.submitdir], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        out, err = p.communicate()
        for l in out.split("\n"):
            yield "ANALYZER: %s\n" % l
        rc = p.wait()
        yield "ANALYZER: Exited with code %d\n" % rc

    if w.state == EnsembleWorkflowStates.PLAN_FAILED:
        yield "Planner failure detected\n"
    elif w.state == EnsembleWorkflowStates.RUN_FAILED:
        yield "pegasus-run failure detected\n"
    elif w.state == EnsembleWorkflowStates.FAILED:
        yield "Workflow failure detected\n"

