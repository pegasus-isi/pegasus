from flask import g, Response, render_template, request, json

from pegasus.service import app, db
from pegasus.service.replicas import models as replicas

def want_json():
    print request.accept_mimetypes
    best = request.accept_mimetypes.best_match(['text/plain', 'text/html', 'application/json'])
    return best == 'application/json'

@app.route("/replicas", methods=["POST"])
def post_replicas():
    mappings = request.get_json()
    if not mappings:
        return "", 401

    for m in mappings:
        print m
        #replicas.create_mapping(g.user.id, m["lfn"], m["pfn"], m["pool"])

    db.session.commit()

    return ""

@app.route("/replicas", methods=["GET"])
def get_replicas():
    mappings = replicas.find_mappings(g.user.id)
    if want_json():
        obj_list = [{"lfn": m.lfn, "pfn": m.pfn, "pool": m.pool} for m in mappings]
        return Response(json.dumps(obj_list), 200, mimetype="application/json")
    else:
        lines = ['%s %s pool="%s"' % (m.lfn, m.pfn, m.pool) for m in mappings]
        return Response("\n".join(lines), 200, mimetype="text/plain")

@app.route("/replicas/<lfn>", methods=["GET"])
def get_lfn(lfn):
    pfns = replicas.find_pfns(g.user.id, lfn)
    if want_json():
        pfn_list = [{"pfn": p.name, "pool": p.pool} for p in pfns]
        lfn_obj = {"lfn": lfn, "pfns": pfn_list}
        return Response(json.dumps(lfn_obj), 200, mimetype="application/json")
    else:
        lines = ['%s %s pool="%s"' % (lfn, p.name, p.pool) for p in pfns]
        return Response("\n".join(lines), 200, mimetype="text/plain")

