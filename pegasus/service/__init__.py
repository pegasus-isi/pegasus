import sys
import os
from flask import Flask

app = Flask(__name__)

# Load configuration defaults
app.config.from_object("pegasus.service.defaults")

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

# Find pegasus python library
def get_pegasus_lib_python():
    home = get_pegasus_home()
    if home is not None:
        for lib in ["lib64","lib"]:
            pythonpath = os.path.join(home, lib, "pegasus", "python")
            if os.path.isdir(pythonpath):
                return pythonpath
        raise ImportError("Pegasus Python lib directory not found in PEGASUS_HOME: '%s' " % home)

    # Try the default RPM/DEB locations as a last resort
    shares = ["/usr/lib64/pegasus/python", "/usr/lib/pegasus/python"]
    for share in shares:
        if os.path.isdir(share):
            return share

    raise ImportError("Error getting path for Pegasus Python library: Set PEGASUS_HOME in your environment or configuration file")

# Add pegasus wms python library to pythonpath
try:
    import Pegasus.netlogger
except ImportError:
    sys.path.insert(0, get_pegasus_lib_python())

from flask.ext.sqlalchemy import SQLAlchemy
db = SQLAlchemy(app)

from pegasus.service import auth, filters, api, dashboard, catalogs, ensembles

from Pegasus.netlogger.analysis.schema import stampede_dashboard_schema as dash
dash.initializeToDashboardDB(db.engine, db.metadata)

