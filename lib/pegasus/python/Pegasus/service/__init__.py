import os
from flask import Flask

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
            raise ImportError(
                "Invalid value for PEGASUS_HOME environment variable: %s" %
                home
            )
        return home

    home = app.config.get("PEGASUS_HOME", None)
    if home is not None:
        if not os.path.isdir(home):
            raise ImportError(
                "Invalid directory for PEGASUS_HOME in configuration file: %s"
                % home
            )
        return home

    return None


from flask.ext.cache import Cache
cache = Cache(app)

#
# Flask URL variables support int, float, and path converters.
# Adding support for a boolean converter.
#
from Pegasus.service.base import BooleanConverter
app.url_map.converters['boolean'] = BooleanConverter

#
# Relax trailing slash requirement
#
app.url_map.strict_slashes = False

from Pegasus.service import auth, filters, dashboard, monitoring
