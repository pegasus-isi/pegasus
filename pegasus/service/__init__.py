import os
from flask import Flask

app = Flask(__name__)

def locate_config():
    search_path = [
        os.path.join(os.getcwd(), "service_config.py"),
        os.path.expanduser("~/.pegasus/service_config.py"),
        "/etc/pegasus/service_config.py"
    ]
 
    for f in search_path:
        if os.path.exists(f):
            return f

    raise Exception("Pegasus service config file not found. "
                    "Expected one of:\n\t%s" % "\n\t".join(search_path))

app.config.from_pyfile(locate_config())

from flask.ext.sqlalchemy import SQLAlchemy
db = SQLAlchemy(app)

from pegasus.service import views, models

