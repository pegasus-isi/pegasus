import json
import logging
import os
import re
import subprocess
from pathlib import Path

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


@emapp.route("/ensembles/<string:ensemble>/triggers/cron", methods=["POST"])
def route_create_cron_trigger(ensemble):
    # verify that ensemble exists for user
    e_dao = Ensembles(g.session)

    # raises EMError code 404 if does not exist
    ensemble_id = e_dao.get_ensemble(g.user.username, ensemble).id

    # validate trigger
    trigger = request.form.get("trigger", type=str)
    if not trigger or len(trigger) == 0:
        raise EMError("trigger name must be a non-empty string")

    # validate workflow_script
    workflow_script = request.form.get("workflow_script", type=str)
    if not workflow_script or len(workflow_script) == 0:
        raise EMError("workflow_script name must be a non-empty string")

    if not Path(workflow_script).is_absolute():
        raise EMError("workflow_script must be given as an absolute path")

    # validate workflow_args
    can_decode = True
    try:
        workflow_args = json.loads(request.form.get("workflow_args"))
    except json.JSONDecodeError:
        can_decode = False

    if not can_decode or not isinstance(workflow_args, list):
        raise EMError("workflow_args must be given as a list serialized to json")

    # validate interval
    try:
        interval = to_seconds(request.form.get("interval", type=str))
    except ValueError:
        raise EMError(
            "interval must be given as `<int> <s|m|h|d>` and be greater than 0 seconds"
        )

    # validate timeout
    try:
        timeout = request.form.get("timeout", type=str, default=None)
        if timeout is not None:
            timeout = to_seconds(timeout)
    except ValueError:
        raise EMError(
            "timeout must be given as `<int> <s|m|h|d>` and be greater than 0 seconds"
        )

    kwargs = {
        "ensemble_id": ensemble_id,
        "trigger": trigger,
        "trigger_type": TriggerType.CRON.value,
        "workflow_script": workflow_script,
        "workflow_args": workflow_args,
        "interval": interval,
        "timeout": timeout,
    }

    # create trigger entry in db
    t_dao = Triggers(g.session)
    t_dao.insert_trigger(**kwargs)

    # return response success
    return api.json_created(
        url_for("route_get_trigger", ensemble=ensemble, trigger=trigger)
    )


@emapp.route("/ensembles/<string:ensemble>/triggers/file_pattern", methods=["POST"])
def route_create_file_pattern_trigger(ensemble):
    # verify that ensemble exists for user
    e_dao = Ensembles(g.session)

    # raises EMError code 404 if does not exist
    ensemble_id = e_dao.get_ensemble(g.user.username, ensemble).id

    # validate trigger
    trigger = request.form.get("trigger", type=str)
    if not trigger or len(trigger) == 0:
        raise EMError("trigger name must be a non-empty string")

    # validate workflow_script
    workflow_script = request.form.get("workflow_script", type=str)
    if not workflow_script or len(workflow_script) == 0:
        raise EMError("workflow_script name must be a non-empty string")

    if not Path(workflow_script).is_absolute():
        raise EMError("workflow_script must be given as an absolute path")

    # validate workflow_args
    can_decode = True
    try:
        workflow_args = json.loads(request.form.get("workflow_args"))
    except json.JSONDecodeError:
        can_decode = False

    if not can_decode or not isinstance(workflow_args, list):
        raise EMError("workflow_args must be given as a list serialized to json")

    # validate interval
    try:
        interval = to_seconds(request.form.get("interval", type=str))
    except ValueError:
        raise EMError(
            "interval must be given as `<int> <s|m|h|d>` and be greater than 0 seconds"
        )

    # validate timeout
    try:
        timeout = request.form.get("timeout", type=str, default=None)
        if timeout is not None:
            timeout = to_seconds(timeout)
    except ValueError:
        raise EMError(
            "timeout must be given as `<int> <s|m|h|d>` and be greater than 0 seconds"
        )

    # validate file_patterns
    can_decode = True
    try:
        file_patterns = json.loads(request.form.get("file_patterns"))
    except json.JSONDecodeError:
        can_decode = False

    if not can_decode or not isinstance(file_patterns, list):
        raise EMError("file_patterns must be given as a list serialized to json")

    if len(file_patterns) < 1:
        raise EMError("file_patterns must contain at least one file pattern")

    for fp in file_patterns:
        if not Path(fp).is_absolute():
            raise EMError(
                "each file pattern must be given as an absolute path (e.g. '/inputs/*.txt"
            )

    kwargs = {
        "ensemble_id": ensemble_id,
        "trigger": trigger,
        "trigger_type": TriggerType.FILE_PATTERN.value,
        "workflow_script": workflow_script,
        "workflow_args": workflow_args,
        "interval": interval,
        "timeout": timeout,
        "file_patterns": file_patterns,
    }

    # create trigger entry in db
    t_dao = Triggers(g.session)
    t_dao.insert_trigger(**kwargs)

    # return response success
    return api.json_created(
        url_for("route_get_trigger", ensemble=ensemble, trigger=trigger)
    )


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
    trigger_id = t_dao.get_trigger(ensemble_id, trigger)._id
    t_dao.update_state(ensemble_id, trigger_id, "STOPPED")

    return api.json_response(
        {
            "message": "ensemble: {}, trigger: {} marked for deletion".format(
                ensemble, trigger
            )
        },
        status_code=202,
    )


def to_seconds(value: str) -> int:
    """Convert time unit given as '<int> <s|m|h|d>` to seconds.
    :param value: input str
    :type value: str
    :raises ValueError: value must be given as '<int> <s|m|h|d>
    :raises ValueError: value must be > 0s
    :return: value given in seconds
    :rtype: int
    """

    value = value.strip()
    pattern = re.compile(r"\d+ *[sSmMhHdD]")
    if not pattern.fullmatch(value):
        raise ValueError(
            "invalid interval: {}, interval must be given as '<int> <s|m|h|d>'".format(
                value
            )
        )

    num = int(value[0 : len(value) - 1])
    unit = value[-1].lower()

    as_seconds = {"s": 1, "m": 60, "h": 60 * 60, "d": 60 * 60 * 24}

    result = as_seconds[unit] * num

    if result <= 0:
        raise ValueError(
            "invalid interval: {}, interval must be greater than 0 seconds".format(
                result
            )
        )

    return result
