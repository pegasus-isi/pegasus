import os

from flask import Flask

emapp = Flask(__name__)

# Load configuration defaults
emapp.config.from_object("Pegasus.service.defaults")

# Load user configuration
conf = os.path.expanduser("~/.pegasus/service.py")
if os.path.isfile(conf):
    emapp.config.from_pyfile(conf)
del conf

from Pegasus.service.ensembles import api, views  # noqa: E402,F401 isort:skip
