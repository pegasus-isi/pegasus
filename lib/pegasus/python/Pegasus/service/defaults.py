import os

# SERVER CONFIGURATION

SERVER_HOST = "127.0.0.1"
SERVER_PORT = 5000

# Max number of processes to fork when handling requests
MAX_PROCESSES = 10

# Enable debugging
DEBUG = False

# The secret key used by Flask to encrypt session keys
SECRET_KEY = os.urandom(24)

# Authentication method to use (NoAuthentication or PAMAuthentication)
AUTHENTICATION = "PAMAuthentication"

# Flask cache configuration
CACHE_TYPE = 'simple'



# CLIENT CONFIGURATION

# Service endpoint. This is only required if you install the service
# at a URL other than "http://SERVER_HOST:SERVER_PORT/".
ENDPOINT = None

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

