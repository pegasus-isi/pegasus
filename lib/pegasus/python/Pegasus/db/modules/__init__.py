"""
Base for analysis modules.
"""
import logging
import warnings

from Pegasus.db import connection

class SQLAlchemyInitWarning(Warning):
    pass

class SQLAlchemyInit(object):
    """Mixin class to provide SQLAlchemy database initialization/mapping.
    Takes a SQLAlchemy connection string and a module function as
    required arguments. 
    """
    def __init__(self, dburi, props=None, db_type=None, **kwarg):
        self.dburi = dburi
        self.session = connection.connect(dburi, create=True, props=props, db_type=db_type)

    def __getattr__(self, name):
        if name == "db":
            warnings.warn("SQLAlchemyInit.db is deprecated. Use session or session.bind instead.", SQLAlchemyInitWarning)
            return self.session.bind
        raise AttributeError

    def disconnect(self):
        self.session.close()

    def close(self):
        self.session.close()


class BaseLoader(object):
    "Base loader class. Has a database session and a log handle."

    def __init__(self, dburi, props=None, db_type=None):
        """Will be overridden by subclasses to take
        parameters specific to their function.
        """
        self.log = logging.getLogger("%s.%s" % (self.__module__, self.__class__.__name__))
        self.dburi = dburi
        self.session = connection.connect(dburi, create=True, props=props, db_type=db_type)

    def process(self, data):
        """Override with logic; 'data' is a dictionary with timestamp,
        event, and other values.
        """
        pass

    def flush(self):
        """Override to flush any intermediate results, e.g. call
        flush() on open file descriptors.
        """
        pass

    def finish(self):
        """Override with logic if subclass needs a cleanup method,
        ie: threading or whatnot
        """
        pass

    def disconnect(self):
        self.session.close()

