import os

from pegasus.service import app

def add_options(parser):
    parser.add_option("--config", action="store", dest="config",
            default=None, help="Path to configuration file")
    parser.add_option("--dburi", action="store", dest="dburi",
            default=None, help="SQLAlchemy database URI")
    parser.add_option("-d", "--debug", action="store_true", dest="debug",
            default=None, help="Enable debugging")

def set_options(options):
    if options.config:
        app.config.from_pyfile(options.config)

    if options.debug:
        app.config.update(DEBUG=options.debug)

    if options.dburi:
        app.config.update(SQLALCHEMY_DATABASE_URI=options.dburi)

