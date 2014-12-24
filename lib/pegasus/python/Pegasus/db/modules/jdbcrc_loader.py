__author__ = "Rafael Ferreira da Silva"

from Pegasus.db.schema.schema_check import ErrorStrings, SchemaCheck, SchemaVersionError
from Pegasus.db.schema.jdbcrc_schema import *
from Pegasus.db.modules import Analyzer as BaseAnalyzer
from Pegasus.db.modules import SQLAlchemyInit

class Analyzer(BaseAnalyzer, SQLAlchemyInit):
    """Load into the Stampede SQL schema through SQLAlchemy.

    Parameters:
      - connString {string,None*}: SQLAlchemy connection string.
        The general form of this is
          'dialect+driver://username:password@host:port/database'.
        See the SQLAlchemy docs for details.
        For sqlite, use 'sqlite:///foo.db' for a relative path and
        'sqlite:////path/to/foo.db' (four slashes) for an absolute one.
        When using MySQL, the general form will work, but the library
        expects the database to exist (ie: will not issue CREATE DB)
        but will populate an empty DB with tables/indexes/etc.
    """
    def __init__(self, connString=None, **kw):
        """Init object

        @type   connString: string
        @param  connString: SQLAlchemy connection string - REQUIRED
        """
        BaseAnalyzer.__init__(self, **kw)
        if connString is None:
            raise ValueError("connString is required")

        try:
            SQLAlchemyInit.__init__(self, connString, initializeToJDBCRC)
        except exc.OperationalError, e:
            self.log.error('Connection String %s  %s', (connString, ErrorStrings.get_init_error(e)))
            raise RuntimeError
        
        