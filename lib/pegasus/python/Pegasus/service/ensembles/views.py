import os

from flask import g, url_for, make_response, request, send_file, json

from Pegasus.service.ensembles import emapp, models, api

@emapp.route("/ensembles", methods=["GET"])
def route_list_ensembles():
    db = models.Ensembles(g.master_db_url)
    ensembles = db.list_ensembles(g.user.username)
    result = [e.get_object() for e in ensembles]
    return api.json_response(result)

@emapp.route("/ensembles", methods=["POST"])
def route_create_ensemble():
    name = request.form.get("name", None)
    if name is None:
        raise api.APIError("Specify ensemble name")

    max_running = request.form.get("max_running", 1)
    max_planning = request.form.get("max_planning", 1)

    db = models.Ensembles(g.master_db_url)
    db.create_ensemble(g.user.username, name, max_running, max_planning)
    db.session.commit()

    return api.json_created(url_for("route_get_ensemble", name=name, _external=True))

@emapp.route("/ensembles/<string:name>", methods=["GET"])
def route_get_ensemble(name):
    db = models.Ensembles(g.master_db_url)
    e = db.get_ensemble(g.user.username, name)
    result = e.get_object()
    return api.json_response(result)

@emapp.route("/ensembles/<string:name>", methods=["PUT","POST"])
def route_update_ensemble(name):
    db = models.Ensembles(g.master_db_url)
    e = db.get_ensemble(g.user.username, name)

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

    return api.json_response(e.get_object())

@emapp.route("/ensembles/<string:name>/workflows", methods=["GET"])
def route_list_ensemble_workflows(name):
    db = models.Ensembles(g.master_db_url)
    e = db.get_ensemble(g.user.username, name)
    result = [w.get_object() for w in db.list_ensemble_workflows(e.id)]
    return api.json_response(result)

@emapp.route("/ensembles/<string:ensemble>/workflows", methods=["POST"])
def route_create_ensemble_workflow(ensemble):
    db = models.Ensembles(g.master_db_url)
    e = db.get_ensemble(g.user.username, ensemble)

    name = request.form.get("name", None)
    if name is None:
        raise api.APIError("Specify ensemble workflow 'name'")

    priority = request.form.get("priority", 0)

    basedir = request.form.get("basedir")
    if basedir is None:
        raise api.APIError("Specify 'basedir' where plan command should be executed")

    plan_command = request.form.get("plan_command")
    if plan_command is None:
        raise api.APIError("Specify 'plan_command' that should be executed to plan workflow")

    db = models.Ensembles(g.master_db_url)

    db.create_ensemble_workflow(e.id, name, basedir, priority, plan_command)

    db.session.commit()

    return api.json_created(url_for("route_get_ensemble_workflow", ensemble=ensemble, workflow=name))

@emapp.route("/ensembles/<string:ensemble>/workflows/<string:workflow>", methods=["GET"])
def route_get_ensemble_workflow(ensemble, workflow):
    db = models.Ensembles(g.master_db_url)
    e = db.get_ensemble(g.user.username, ensemble)
    w = db.get_ensemble_workflow(e.id, workflow)
    result = w.get_detail_object()
    return api.json_response(result)

@emapp.route("/ensembles/<string:ensemble>/workflows/<string:workflow>", methods=["PUT","POST"])
def route_update_ensemble_workflow(ensemble, workflow):
    db = models.Ensembles(g.master_db_url)
    e = db.get_ensemble(g.user.username, ensemble)
    w = db.get_ensemble_workflow(e.id, workflow)

    priority = request.form.get("priority", None)
    if priority is not None:
        w.set_priority(priority)

    state = request.form.get("state", None)
    if state is not None:
        w.change_state(state)

    w.set_updated()

    db.session.commit()

    return api.json_response(w.get_detail_object())

@emapp.route("/ensembles/<string:ensemble>/workflows/<string:workflow>/<string:filename>", methods=["GET"])
def route_get_ensemble_workflow_file(ensemble, workflow, filename):
    db = models.Ensembles(g.master_db_url)
    e = db.get_ensemble(g.user.username, ensemble)
    w = db.get_ensemble_workflow(e.id, workflow)
    mimetype = "text/plain"
    path = os.path.join(w.basedir, filename)
    if not os.path.isfile(path):
        raise api.APIError("Invalid file: %s" % filename)

    return send_file(path, mimetype=mimetype)
