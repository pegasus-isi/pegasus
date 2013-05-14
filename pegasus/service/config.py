import os

from pegasus.service import app

def load_config(fname):
    app.config.from_pyfile(fname)

def set_debug(debug):
    app.config.update(DEBUG=debug)

def set_dburi(dburi):
    app.config.update(SQLALCHEMY_DATABASE_URI=dburi)

