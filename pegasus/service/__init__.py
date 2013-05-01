import os
from flask import Flask

app = Flask(__name__)

app.config.from_object("pegasus.service.defaults")

from flask.ext.sqlalchemy import SQLAlchemy
db = SQLAlchemy(app)

from pegasus.service import filters, dashboard

