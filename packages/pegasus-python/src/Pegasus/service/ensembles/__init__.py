from flask import Flask

from Pegasus.service._config import load_user_config

emapp = Flask(__name__)

# Load configuration defaults
emapp.config.from_object("Pegasus.service.defaults")

emapp.config["AUTHENTICATION"] = "LocalTokenAuthentication"
load_user_config(emapp)

from Pegasus.service.ensembles import api, views  # noqa: E402,F401 isort:skip
