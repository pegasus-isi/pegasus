from flask import json, make_response

JSON_HEADERS = {"Content-Type":"application/json"}

def json_api_error(e):
    response = {"message": e.message}
    status_code = 500
    if hasattr(e, "status_code"):
        status_code = e.status_code
    return make_response(json.dumps(response), status_code, JSON_HEADERS)

def json_created(location):
    response = {"message": "created"}
    headers = {}
    headers.update(JSON_HEADERS)
    headers["Location"] = location
    return make_response(json.dumps(response), 201, headers)

def json_response(obj):
    return make_response(json.dumps(obj), 200, JSON_HEADERS)

