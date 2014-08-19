import sys
import os
from flask import Flask
from flask.ext.cache import Cache

app = Flask(__name__)

# Load configuration defaults
app.config.from_object("Pegasus.service.defaults")

# Load user configuration
conf = os.path.expanduser("~/.pegasus/service.py")
if os.path.isfile(conf):
    app.config.from_pyfile(conf)
del conf

# Find pegasus home
def get_pegasus_home():
    home = os.getenv("PEGASUS_HOME", None)
    if home is not None:
        if not os.path.isdir(home):
            raise ImportError("Invalid value for PEGASUS_HOME environment variable: %s" % home)
        return home

    home = app.config.get("PEGASUS_HOME", None)
    if home is not None:
        if not os.path.isdir(home):
            raise ImportError("Invalid directory for PEGASUS_HOME in configuration file: %s" % home)
        return home

    return None

def get_userdata_dir(username):
    return os.path.join(app.config["STORAGE_DIR"],
                        "userdata", username)

from flask.ext.sqlalchemy import SQLAlchemy
db = SQLAlchemy(app)
cache = Cache(app)

from Pegasus.service import auth, filters, api, dashboard, catalogs, ensembles

from Pegasus.netlogger.analysis.schema import stampede_dashboard_schema as dash
dash.initializeToDashboardDB(db.engine, db.metadata)

