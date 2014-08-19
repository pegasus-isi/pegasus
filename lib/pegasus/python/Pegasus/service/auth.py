from flask import request, Response, g

import pam

from Pegasus.service import app

class NoAuthentication(object):
    def authenticate(self, username, password):
        return True

class PAMAuthentication(object):
    def authenticate(self, username, password):
        if not pam.authenticate(username, password):
            return False
        return True

@app.before_request
def perform_basic_auth():
    authclass = app.config["AUTHENTICATION"]
    if authclass in globals():
        Authentication = globals()[authclass]
    else:
        raise Exception("Unknown authentication class: %s" % authclass)

    auth = request.authorization
    if not auth or not Authentication().authenticate(auth.username, auth.password):
        return Response('Basic Auth Required', 401,
                        {'WWW-Authenticate': 'Basic realm="Pegasus Service"'})

    g.username = auth.username

