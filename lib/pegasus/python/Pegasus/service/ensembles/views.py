import os

from flask import g, url_for, make_response, request, send_file, json

from Pegasus.service import app, api
from Pegasus.service.ensembles import models
from Pegasus.service.ensembles.bundle import BundleException

@app.route("/ensembles", methods=["GET"])
def route_list_ensembles():
    db = models.Ensembles(g.master_db_url)
    ensembles = db.list_ensembles(g.user.username)
    result = [e.get_object() for e in ensembles]
    return api.json_response(result)

@app.route("/ensembles", methods=["POST"])
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

@app.route("/ensembles/<string:name>", methods=["GET"])
def route_get_ensemble(name):
    db = models.Ensembles(g.master_db_url)
    e = db.get_ensemble(g.user.username, name)
    result = e.get_object()
    return api.json_response(result)

@app.route("/ensembles/<string:name>", methods=["PUT","POST"])
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

@app.route("/ensembles/<string:name>/workflows", methods=["GET"])
def route_list_ensemble_workflows(name):
    db = models.Ensembles(g.master_db_url)
    e = db.get_ensemble(g.user.username, name)
    result = [w.get_object() for w in db.list_ensemble_workflows(e.id)]
    return api.json_response(result)

@app.route("/ensembles/<string:ensemble>/workflows", methods=["POST"])
def route_create_ensemble_workflow(ensemble):
    db = models.Ensembles(g.master_db_url)
    e = db.get_ensemble(g.user.username, ensemble)

    name = request.form.get("name", None)
    if name is None:
        raise api.APIError("Specify ensemble workflow name")

    priority = request.form.get("priority", 0)

    sites = request.form.get("sites", None)
    if sites is None:
        raise api.APIError("Specify sites")
    else:
        sites = [s.strip() for s in sites.split(",")]
        sites = [s for s in sites if len(s) > 0]
    if len(sites) == 0:
        raise api.APIError("Specify sites")

    output_site = request.form.get("output_site", None)
    if output_site is None:
        raise api.APIError("Specify output_site")

    cleanup = request.form.get("cleanup", None)
    if cleanup is not None:
        cleanup = cleanup.lower()
        if cleanup not in ["none","leaf","inplace"]:
            raise api.APIError("Invalid value for cleanup: %s" % cleanup)

    force = request.form.get("force", None)
    if force is not None:
        if force.lower() not in ["true","false"]:
            raise api.APIError("Invalid value for force: %s" % force)
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

    bundle = request.files.get("bundle", None)
    if bundle is None:
        raise api.APIError("Specify bundle")

    db = models.Ensembles(g.master_db_url)

    try:
        db.create_ensemble_workflow(e.id, name, priority, bundle, sites=sites,
                output_site=output_site, cleanup=cleanup, force=force,
                clustering=clustering, staging_sites=staging_sites)
    except BundleException, e:
        raise api.APIError(e.message)

    db.session.commit()

    return api.json_created(url_for("route_get_ensemble_workflow", ensemble=ensemble, workflow=name))

@app.route("/ensembles/<string:ensemble>/workflows/<string:workflow>", methods=["GET"])
def route_get_ensemble_workflow(ensemble, workflow):
    db = models.Ensembles(g.master_db_url)
    e = db.get_ensemble(g.user.username, ensemble)
    w = db.get_ensemble_workflow(e.id, workflow)
    result = w.get_detail_object()
    return api.json_response(result)

@app.route("/ensembles/<string:ensemble>/workflows/<string:workflow>", methods=["PUT","POST"])
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

@app.route("/ensembles/<string:ensemble>/workflows/<string:workflow>/<string:filename>", methods=["GET"])
def route_get_ensemble_workflow_file(ensemble, workflow, filename):
    db = models.Ensembles(g.master_db_url)
    e = db.get_ensemble(g.user.username, ensemble)
    w = db.get_ensemble_workflow(e.id, workflow)
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
        raise api.APIError("Invalid file: %s" % filename)

    return send_file(path, mimetype=mimetype)
