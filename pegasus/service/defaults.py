import os

# Server configuration
SERVER_HOST = "127.0.0.1"
SERVER_PORT = 5000

DEBUG = False

# The secret key used by Flask to encrypt session keys
SECRET_KEY = os.urandom(24)

# The URI of the database
#SQLALCHEMY_DATABASE_URI = "mysql://pegasus:secret@127.0.0.1:3306/pegasus_service"
SQLALCHEMY_DATABASE_URI = 'sqlite:///%s/.pegasus/workflow.db' % os.getenv('HOME')
#SQLALCHEMY_DATABASE_URI = 'sqlite:////tmp/test.db'

# Set to True to log SQL queries
#SQLALCHEMY_ECHO = False

# Set to change the SQLAlchemy connection pool size
#SQLALCHEMY_POOL_SIZE = 5

# Set to change the connection timeout (in seconds)
#SQLALCHEMY_POOL_TIMEOUT = 10

# Set to change how long connections remain before being recycled (in seconds)
# Default is 2 hours for MySQL
#SQLALCHEMY_POOL_RECYCLE = 2 * 60 * 60

# Client configuration
ENDPOINT = "http://%s:%s/" % (SERVER_HOST, SERVER_PORT)
USERNAME = ""
PASSWORD = ""

# Storage
STORAGE_DIR = os.path.join(os.getenv('HOME'), ".pegasus", "service")

# Ensemble manager
EM_INTERVAL = 120
PEGASUS_HOME = "/usr"

