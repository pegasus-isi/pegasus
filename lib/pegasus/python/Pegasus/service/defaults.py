import os
import tempfile

# SERVER CONFIGURATION

SERVER_HOST = "127.0.0.1"
SERVER_PORT = 5000

# SSL config: path to certificate and private key files
CERTIFICATE = None
PRIVATE_KEY = None

# Max number of processes to fork when handling requests
MAX_PROCESSES = 10

# Enable debugging
DEBUG = False

# The secret key used by Flask to encrypt session keys
SECRET_KEY = os.urandom(24)

# Authentication method to use (NoAuthentication or PAMAuthentication)
AUTHENTICATION = "PAMAuthentication"
PROCESS_SWITCHING = True

# Flask cache configuration
CACHE_TYPE = 'filesystem'
CACHE_DIR = os.path.join(tempfile.gettempdir(), 'pegasus-service')

#
# Authorization -
# None, '', False -> User can only access their own data.
# * -> All users are admin users and can access data of any other user.
# {'u1', .., 'un'} OR ['u1', .., 'un'] -> Only users in the set/list are admin users.
#
ADMIN_USERS = None

# CLIENT CONFIGURATION

# User credentials
USERNAME = ""
PASSWORD = ""

# ENSEMBLE MANAGER CONFIGURATION

# Workflow processing interval in seconds
EM_INTERVAL = 60

# Directory to store data
STORAGE_DIRECTORY = "/var/pegasus"

# Path to Pegasus home directory
#PEGASUS_HOME = "/usr"

# Path to Condor home directory
#CONDOR_HOME = "/usr"
