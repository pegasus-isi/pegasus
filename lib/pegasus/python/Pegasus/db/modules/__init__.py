"""
Base for analysis modules.
"""
import logging
import warnings

from Pegasus.db import connection

class AnalyzerException(Exception):
    pass

class ProcessException(AnalyzerException):
    pass

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


class BaseAnalyzer(object):
    "Base analysis class. Doesn't do much."

    def __init__(self):
        """Will be overridden by subclasses to take
        parameters specific to their function.
        """
        self.log = logging.getLogger("%s.%s" % (self.__module__, self.__class__.__name__))

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

    def notify(self, data):
        """Called by framework code to notify loader of new data,
        which will result in a call to process(data).
        Do not override this unless you know what you are doing.

        Args:
          - data: NetLogger event dictionary

        Returns:
          - Whatever the loading code returns, in most cases
          the return value should be ignored.

        Exceptions:
          - ProcessException (AnalyzerException): Something went wrong
             during processing
        """
        try:
            return self.process(data)
        except Exception, err:
            raise ProcessException(str(err))

