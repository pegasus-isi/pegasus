import json
import logging
import os
import subprocess

from flask import g, make_response, request, url_for

from Pegasus.db import connection
from Pegasus.db.ensembles import (
    EMError,
    Ensembles,
    EnsembleWorkflowStates,
    Triggers,
    TriggerType,
)
from Pegasus.service.ensembles import api, emapp
from Pegasus.service.lifecycle import authenticate

log = logging.getLogger(__name__)


def connect():
    log.debug("Connecting to database")
    g.master_db_url = g.user.get_master_db_url()
    g.session = connection.connect(
        g.master_db_url, connect_args={"check_same_thread": False}
    )


def disconnect():
    if "conn" in g:
        log.debug("Disconnecting from database")
        g.session.close()


@emapp.errorhandler(Exception)
def handle_error(e):
    return api.json_api_error(e)


emapp.before_request(authenticate)
emapp.before_request(connect)


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


@emapp.route("/ensembles/<string:name>", methods=["PUT", "POST"])
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

    return api.json_created(
        url_for("route_get_ensemble_workflow", ensemble=ensemble, workflow=name)
    )


@emapp.route(
    "/ensembles/<string:ensemble>/workflows/<string:workflow>", methods=["GET"]
)
def route_get_ensemble_workflow(ensemble, workflow):
    dao = Ensembles(g.session)
    e = dao.get_ensemble(g.user.username, ensemble)
    w = dao.get_ensemble_workflow(e.id, workflow)
    result = w.get_detail_object()
    return api.json_response(result)


@emapp.route(
    "/ensembles/<string:ensemble>/workflows/<string:workflow>", methods=["PUT", "POST"]
)
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


@emapp.route(
    "/ensembles/<string:ensemble>/workflows/<string:workflow>/analyze", methods=["GET"]
)
def route_analyze_ensemble_workflow(ensemble, workflow):
    dao = Ensembles(g.session)
    e = dao.get_ensemble(g.user.username, ensemble)
    w = dao.get_ensemble_workflow(e.id, workflow)
    report = "".join(analyze(w))
    resp = make_response(report, 200)
    resp.headers["Content-Type"] = "text/plain"
    return resp


def analyze(workflow):
    w = workflow

    yield "Workflow state is %s\n" % w.state
    yield "Plan command is: %s\n" % w.plan_command

    logfile = w.get_logfile()
    if os.path.isfile(logfile):
        yield "Workflow log:\n"
        for l in open(w.get_logfile(), "rb"):
            yield "LOG: %s" % l.decode()
    else:
        yield "No workflow log available\n"

    if w.submitdir is None or not os.path.isdir(w.submitdir):
        yield "No submit directory available\n"
    else:
        yield "pegasus-analyzer output is:\n"
        p = subprocess.Popen(
            ["pegasus-analyzer", w.submitdir],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )

        out, err = p.communicate()
        out = out.decode()

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


# --- trigger related routes ---------------------------------------------------
@emapp.route("/ensembles/<string:ensemble>/triggers", methods=["GET"])
def route_list_triggers(ensemble):
    dao = Triggers(g.session)
    triggers = dao.list_triggers_by_ensemble(g.user.username, ensemble)
    return api.json_response([Triggers.get_object(t) for t in triggers])


@emapp.route("/ensembles/<string:ensemble>/triggers/<string:trigger>", methods=["GET"])
def route_get_trigger(ensemble, trigger):
    raise NotImplementedError("TODO")


# TODO: checks for correct data should be done here on the backend
# should be just /ensembles/<string:ensemble>/triggers, methods=["POST"]
# possibly look into using jsonschema to validate incoming json requests
"""
# error response format

{
  id: "id", <-- unique id to a request, it has been added as request.uid (use this when logging)
  code: "UNPROCESSABLE_ENTITY", <-- capitalized versions of errors that json schema would return 
  "message": "Err description",
  "errors": [
    {
      code: "MIN_LEN_ERR",
      message": "Err description",
      path: [ field_name ],
    },
    ..
  ],
  "warnings": [ .. ]
}
"""


@emapp.route("/ensembles/<string:ensemble>/triggers/<string:trigger>", methods=["POST"])
def route_create_trigger(ensemble, trigger):
    # verify that ensemble exists for user
    e_dao = Ensembles(g.session)

    # raises EMError code 404 if does not exist
    ensemble_id = e_dao.get_ensemble(g.user.username, ensemble).id

    # create trigger entry in db
    t_dao = Triggers(g.session)

    trigger_type = request.form.get("type")
    kwargs = {
        "ensemble_id": ensemble_id,
        "trigger": trigger,
        "trigger_type": trigger_type,
        "workflow_script": request.form.get("workflow_script"),
        "workflow_args": json.loads(request.form.get("workflow_args")),
    }

    if trigger_type == TriggerType.CRON.value:
        # add cron trigger specific parameters
        kwargs["interval"] = request.form.get("interval")
        kwargs["timeout"] = request.form.get("timeout")
    elif trigger_type == TriggerType.FILE_PATTERN.value:
        # add file pattern specific parameters
        kwargs["interval"] = request.form.get("interval")
        kwargs["timeout"] = request.form.get("timeout")
        kwargs["file_patterns"] = json.loads(request.form.get("file_patterns"))
    else:
        raise NotImplementedError(
            "encountered unsupported trigger type: {}".format(trigger_type)
        )

    t_dao.insert_trigger(**kwargs)

    # TODO: what to return here
    # return ID that was created, in this case trigger name is sufficient
    # probably code 201
    # use Flask response object and a json object representing an id of the entity
    return "hello world from create_trigger!"


@emapp.route(
    "/ensembles/<string:ensemble>/triggers/<string:trigger>", methods=["DELETE"]
)
def route_delete_trigger(ensemble, trigger):
    # verify that ensemble exists for user
    e_dao = Ensembles(g.session)

    # raises EMError code 404 if does not exist
    ensemble_id = e_dao.get_ensemble(g.user.username, ensemble).id

    # update trigger state to be STOPPED so that the TriggerManager can
    # handle it appropriately
    t_dao = Triggers(g.session)

    # make sure get_trigger raises 404 if nothing found
    trigger_id = t_dao.get_trigger(ensemble_id, trigger).id
    t_dao.update_state(ensemble_id, trigger_id)

    # TODO: what to return here
    # return HTTP code that represents that it was successful and that nothing
    # is to returned
    # status code 204, nothing else to return
    return "hello world from delete_trigger!"
