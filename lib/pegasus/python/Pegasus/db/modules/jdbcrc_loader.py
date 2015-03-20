__author__ = "Rafael Ferreira da Silva"

from Pegasus import db
from Pegasus.db.schema import *
from Pegasus.db.modules import Analyzer as BaseAnalyzer
from Pegasus.db.modules import SQLAlchemyInit

class Analyzer(BaseAnalyzer, SQLAlchemyInit):
    """Load into the JDBCRC SQL schema through SQLAlchemy.

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
        self.session = db.connect(connString)

