import sys
from functools import wraps
import traceback

from flask import json, make_response

from pegasus.service import app

JSON_HEADERS = {"Content-Type":"application/json"}

class APIError(Exception):
    def __init__(self, message, status_code=400):
        Exception.__init__(self, message)
        self.status_code = status_code
        self.cause = sys.exc_info()

@app.errorhandler(APIError)
def json_api_error(e):
    response = {"message": e.message}
    if e.cause is not (None, None, None) and app.config["DEBUG"]:
        response["cause"] = u"".join(traceback.format_exception(*e.cause))
    return make_response(json.dumps(response), e.status_code, JSON_HEADERS)

def json_created(location):
    response = {"message": "created"}
    headers = {}
    headers.update(JSON_HEADERS)
    headers["Location"] = location
    return make_response(json.dumps(response), 201, headers)

def json_response(obj):
    return make_response(json.dumps(obj), 200, JSON_HEADERS)

