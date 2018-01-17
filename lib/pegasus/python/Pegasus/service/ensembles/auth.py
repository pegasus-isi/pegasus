import os
import logging

from flask import request, Response, g, abort
import pam

from Pegasus import user
from Pegasus.service.ensembles import emapp

log = logging.getLogger(__name__)

def authenticate(username, password):
    try:
        return pam.authenticate(username, password)
    except Exception as e:
        log.exception(e)
        return False

def basic_auth_response():
    return Response('Invalid Login', 401,
                    {'WWW-Authenticate': 'Basic realm="Pegasus Service"'})

def authorize_request():
    cred = request.authorization

    if not cred:
        log.error("Auth required")
        return basic_auth_response()

    if not authenticate(cred.username, cred.password):
        log.error("Invalid login: %s", cred.username)
        return basic_auth_response()

    try:
        g.user = user.get_user_by_username(cred.username)
    except user.NoSuchUser as e:
        log.error("No such user: %s" % cred.username)
        return basic_auth_response()

    log.info('Authenticated user %s', g.user.username)

    g.username = g.user.username
    g.master_db_url = g.user.get_master_db_url()

