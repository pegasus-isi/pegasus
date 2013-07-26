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

from flask.ext.sqlalchemy import SQLAlchemy
db = SQLAlchemy(app)

from pegasus.service import auth, filters, dashboard, replicas

