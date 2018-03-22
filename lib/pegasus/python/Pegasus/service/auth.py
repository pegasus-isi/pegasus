import logging
import os

import pam
from flask import Response, abort, g, make_response, request, url_for
from Pegasus import user
from Pegasus.service import app

log = logging.getLogger(__name__)


class BaseAuthentication(object):
    def __init__(self, username, password):
        self.username = username
        self.password = password

    def authenticate(self):
        raise Exception("Not implemented")

    def get_user(self):
        raise Exception("Not implemented")


class NoAuthentication(BaseAuthentication):
    def __init__(self, *args):
        # For no auth. username and password is not required
        pass

    def authenticate(self):
        # Always authenticate the user
        return True

    def get_user(self):
        # Just return info for the user running the service
        if 'username' in g:
            return user.get_user_by_username(g.username)
        else:
            return user.get_user_by_uid(os.getuid())


class PAMAuthentication(BaseAuthentication):
    def authenticate(self):
        try:
            return pam.authenticate(self.username, self.password)
        except Exception as e:
            log.exception(e)
            return False

    def get_user(self):
        return user.get_user_by_username(self.username)


def basic_auth_response():
    return Response(
        'Basic Auth Required', 401, {
            'WWW-Authenticate': 'Basic realm="Pegasus Service"'
        }
    )


def is_user_an_admin(username):
    """
        Check if user ia a valid admin user.
    """
    admin_users = app.config['ADMIN_USERS']

    if isinstance(admin_users, str) or isinstance(admin_users, unicode):
        admin_users = admin_users.strip()

    if admin_users is None or admin_users is False or admin_users == '':
        return False
    elif admin_users == '*':
        return True
    elif hasattr(admin_users, '__iter__'):
        return username in admin_users
    else:
        log.error('Invalid configuration: ADMIN_USERS is invalid.')
        abort(500)


@app.url_defaults
def add_username(endpoint, values):
    """
        If the endpoint expects a variable username, then set it's value to g.username.
        This is done so as not to provide g.username as a parameter to every call to url_for.
    """

    #
    # Route does not expects a value for username
    #

    if not app.url_map.is_endpoint_expecting(endpoint, 'username'):
        return

    #
    # Route expects a value for username
    #

    # Value for username has already been provided
    if 'username' in values or ('username' in g and not g.username):
        return

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

    # Static files do not need to be authenticated.
    if (request.script_root + request.path).startswith(
        url_for('static', filename='')
    ):
        return

    #
    # Authentication
    #

    cred = request.authorization
    username = cred.username if cred else None
    password = cred.password if cred else None

    authclass = app.config["AUTHENTICATION"]
    if authclass not in globals():
        log.error("Unknown authentication method: %s", authclass)
        return make_response("Invalid server configuration", 500)

    Authentication = globals()[authclass]
    auth = Authentication(username, password)
    if not auth.authenticate():
        log.error("Invalid login: %s", username)
        return basic_auth_response()

    try:
        g.user = auth.get_user()
    except user.NoSuchUser as e:
        log.error("No such user: %s" % username)
        return basic_auth_response()

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
        if not is_user_an_admin(g.user.username):
            log.error(
                "User %s is accessing user %s's runs" %
                (g.user.username, g.username)
            )
            abort(403)

        # Is user a valid system user?
        try:
            user_info = user.get_user_by_username(g.username)
        except user.NoSuchUser as e:
            log.error('User %s is not a valid user' % g.username)
            abort(400)

    if app.config["PROCESS_SWITCHING"]:
        # If required, set uid and gid of handler process
        if os.getuid() != user_info.uid:
            if os.getuid() != 0:
                log.error(
                    "Pegasus service must run as root to enable process switching"
                )
                return make_response(
                    "Pegasus service must run as root to enable process switching",
                    500
                )

        os.setgid(user_info.gid)
        os.setuid(user_info.uid)

    # Does the user have a Pegasus home directory?
    user_pegasus_dir = user_info.get_pegasus_dir()

    if not os.path.isdir(user_pegasus_dir):
        log.info("User's pegasus directory does not exist. Creating one...")
        try:
            os.makedirs(user_pegasus_dir, mode=0o744)
        except OSError:
            log.info(
                "Invalid Permissions: Could not create user's pegasus directory."
            )
            return make_response(
                "Could not find user's Pegasus directory", 404
            )

    # Set master DB URL for the dashboard
    # For testing master_db_url would be pre-populated, so let's not overwrite it here.
    if 'master_db_url' not in g:
        g.master_db_url = user_info.get_master_db_url()
