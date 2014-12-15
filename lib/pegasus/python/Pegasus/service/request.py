import os
import pwd
import logging

from flask import request, Response, g, abort
import pam

from Pegasus.service import app

log = logging.getLogger(__name__)

class User(object):
    def __init__(self, uid, gid, username, homedir):
        self.uid = uid
        self.gid = gid
        self.username = username
        self.homedir = homedir

    def get_userdata_dir(self):
        return os.path.join(app.config["STORAGE_DIRECTORY"], self.username)

    def get_master_db(self):
        return os.path.join(self.homedir, ".pegasus", "workflow.db")

    def get_master_db_url(self):
        return "sqlite:///%s" % self.get_master_db()

def get_user_by_uid(uid):
    pw = pwd.getpwuid(uid)
    return User(pw.pw_uid, pw.pw_gid, pw.pw_name, pw.pw_dir)

def get_user_by_username(username):
    pw = pwd.getpwnam(username)
    return User(pw.pw_uid, pw.pw_gid, pw.pw_name, pw.pw_dir)

class BaseAuthentication(object):
    def __init__(self, username, password):
        self.username = username
        self.password = password

    def authenticate(self):
        raise Exception("Not implemented")

    def get_user(self):
        raise Exception("Not implemented")

class NoAuthentication(BaseAuthentication):
    def authenticate(self):
        # Always authenticate the user
        return True

    def get_user(self):
        # Just return info for the user running the service
        return get_user_by_uid(os.getuid())

class PAMAuthentication(BaseAuthentication):
    def authenticate(self):
        try:
            return pam.authenticate(self.username, self.password)
        except Exception, e:
            log.exception(e)
            return False

    def get_user(self):
        try:
            return get_user_by_username(self.username)
        except KeyError:
            raise Exception("Invalid user: %s" % self.username)


def basic_auth_response():
    return Response('Basic Auth Required', 401,
                    {'WWW-Authenticate': 'Basic realm="Pegasus Service"'})


def is_user_valid(username):
    """
        Check if username is a valid UNIX user.
    """
    try:
        pw = pwd.getpwnam(username)
        return User(pw.pw_uid, pw.pw_gid, pw.pw_name, pw.pw_dir)
    except KeyError:
        return False


@app.url_defaults
def add_username(endpoint, values):
    """
        If the endpoint expects a variable username, then set it's value to g.username.
        This is done so as not to provide g.username as a parameter to every call to url_for.
    """
    if 'username' in values or not g.username:
        return

    if app.url_map.is_endpoint_expecting(endpoint, 'username'):
        values['username'] = g.username


@app.url_value_preprocessor
def pull_username(endpoint, values):
    """
        If the requested endpoint contains a value for username variable then extract it and set it in g.username.
    """
    if values and 'username' in values:
        g.username = values['username']


@app.before_request
def before():

    #
    # Authentication
    #

    cred = request.authorization

    if not cred:
        return basic_auth_response()

    authclass = app.config["AUTHENTICATION"]
    if authclass not in globals():
        log.error("Unknown authentication method: %s", authclass)
        return basic_auth_response()

    Authentication = globals()[authclass]
    auth = Authentication(cred.username, cred.password)
    if not auth.authenticate():
        log.error("Invalid login: %s", cred.username)
        return basic_auth_response()

    g.user = auth.get_user()
    log.info('Authenticated user %s', g.user.username)

    # If a username is not specified in the requested URI, then set username to the logged in user?
    if 'username' not in g:
        g.username = g.user.username

    #
    # Authorization
    #

    # Root user is off limits.
    if g.username == 'root':
        log.error('Accessing root user info. is not allowed')
        # If the user has logged in as root, then ask user to login as a regular user.
        # If the non-root logged in user is attempting to access root user's data, then return 403 FORBIDDEN
        if g.user.username == 'root':
            return basic_auth_response()
        else:
            abort(403)

    user_info = g.user

    if g.username != g.user.username:
        # Is user (g.user.username) allowed to view user (g.username) runs?
        # TODO Check user's admin privileges.
        # if g.user.username is not admin
        log.error("User %s is accessing user %s's runs" % (g.user.username, g.username))

        # Is user a valid system user?
        user_info = is_user_valid(g.username)
        if user_info is False:
            log.error('User %s is not a valid user' % g.username)
            abort(400)

    # If required, set uid and gid of handler process
    if os.getuid() != user_info.uid:
        if os.getuid() != 0:
            log.error("Pegasus service must run as root to enable multi-user access")
            return basic_auth_response()

    os.setgid(user_info.gid)
    os.setuid(user_info.uid)

    # Does the user have a Pegasus home directory?
    user_pegasus_dir = os.path.join(os.path.expanduser('~%s' % g.username), '.pegasus')

    if not os.path.isdir(user_pegasus_dir):
        log.info("User's pegasus directory does not exist. Creating one...")
        os.makedirs(user_pegasus_dir, mode=0644)

    # Set master DB URL for the dashboard
    g.master_db_url = user_info.get_master_db_url()

    # TODO Add login page and session for storing authentication status
