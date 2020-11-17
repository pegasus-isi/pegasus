#
# Activate Virtualenv
#

#
# If Pegasus Service is installed in a virtualenv,
# Set VIRTUALENV to point to the virtualenv directory.
# VIRTUALENV = '/var/virtualenv/pegasus-service'
#
VIRTUALENV = None

if VIRTUALENV:
    import os

    activate_this = os.path.join(VIRTUALENV, "bin/activate_this.py")
    execfile(activate_this, dict(__file__=activate_this))


#
# Configure Pegasus Service
#

#
# Authentication method to use (NoAuthentication or PAMAuthentication)
#
AUTHENTICATION = "PAMAuthentication"

#
# Should Pegasus Service change the Process UID/GID?
#
PROCESS_SWITCHING = True

#
# Authorization -
# None, '', False -> User can only access their own data.
# * -> All users are admin users and can access data of any other user.
# {'u1', .., 'un'} OR ['u1', .., 'un'] -> Only users in the set/list are admin users.
#
ADMIN_USERS = None

#
# Configure Logging so Flask log messages end up in Apache logs
#
import sys
import logging

logging.basicConfig(stream=sys.stderr, level=logging.INFO)

#
# Start Pegasus Service
#
from Pegasus.service.server import create_app

app = create_app(env="production")

app.config.from_object(__name__)

application = app
