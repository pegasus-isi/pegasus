"""
Base for analysis modules.
"""
from logging import DEBUG
import Queue
import re
import sys
import threading
import time
#
from netlogger import util
from netlogger.nllog import DoesLogging, TRACE
from netlogger.nlapi import TS_FIELD, EVENT_FIELD, HASH_FIELD
from netlogger.util import hash_event
from netlogger.analysis import schemacfg

"""
Standard Exceptions for all Analyzers
"""

class AnalyzerException(Exception):
    """Common base class.
    """
    pass

class ConnectionException(AnalyzerException):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class PreprocessException(AnalyzerException):
    pass

class ProcessException(AnalyzerException):
    pass

"""
Database imports
"""

# SQLite
sqlite = None
try:
    # Python 2.5
    import sqlite3 as sqlite
except ImportError:
    try:
        # Python 2.4
        from pysqlite2 import dbapi2 as sqlite
    except ImportError:
        pass

try:
    from sqlalchemy import create_engine, MetaData, orm
except ImportError:
    pass

def dsn_dialect(s):
    """Data source name (dsn) dialect."""
    dialect = ""
    m = re.match(r'(.*?)[:+]', s)
    if m and (len(m.groups()) == 1):
        dialect = m.group(1)
    return dialect.lower()

"""
Database classes
"""

class DBConnectError(Exception): pass

class Connection:
    NAME = None

    def __init__(self, dsn=None, database=None, kw=None):
        """Connect to the database.

        :Parameter:
          dsn - DBMS filename (sqlite) or host (others)
          database - Database inside DBMS, ignored for sqlite
          kw - Additional keywords

        On error, raise DBConnectError
        """
        self.connection = None

class SQLiteConnection(Connection):
    NAME = 'sqlite'

    def __init__(self, dsn=None, database=None, kw=None):
        self.connection = sqlite.connect(dsn, isolation_level="DEFERRED")

"""
User-level name for each connection class, from the
NAME constant in each class.
"""

CONNECTION_CLASSMAP = { }
for clazz in (SQLiteConnection,):
    CONNECTION_CLASSMAP[clazz.NAME] = clazz

"""
Mixin class to provide SQLAlchemy database initialization/mapping.
Takes a SQLAlchemy connection string and a module function as
required arguments.  The initialization function takes the db and 
metadata objects (and optional args) as args, initializes to the 
appropriate schema and sets "self.session" as a class member for
loader classes to interact with the DB with.

See: netlogger.analysis.schema.stampede_schema.initializeToPegasusDB

For an example of what the intialization function needs to do to setup
the schema mappings and the metadata object.  This should be __init__'ed
in the subclass AFTER the Analyzer superclass gets called.

The module netlogger.analysis.modules.stampede_loader shows the use
of this to initialize to a DB.
"""
class SQLAlchemyInit:
    def __init__(self, connString, initFunction, **kwarg):
        if not hasattr(self, '_dbg'):
            # The Analyzer superclass SHOULD have been _init__'ed
            # already but if not, bulletproof this attr.
            self._dbg = False
        self.db = create_engine(connString, echo=self._dbg)
        self.metadata = MetaData()
        dialect_kw = kwarg.get(dsn_dialect(connString), {})
        initFunction(self.db, self.metadata, kw=dialect_kw)
        self.metadata.bind = self.db
        sm = orm.sessionmaker(bind=self.db, autoflush=False, autocommit=False, 
                                expire_on_commit=False)
        self.session = orm.scoped_session(sm)

            
"""
Base classes
"""

class Analyzer(DoesLogging):
    """Base analysis class. Doesn't do much.

    Parameters:
      - add_hash {yes,no,no*}: To each input event, add a new field,
           'nlhash', which is a probabilistically unique (MD5) hash of all
           the other fields in the event.
      - schemata {<file,file..>,None*}: If given, read a simple form of
           schema from file(s). The schema uses INI format with a [section]
           for each event name and <field> = <type-name> describing the type
           of each field for that event.
    """

    FLUSH_SEC = 5 # time to wait before calling flush()

    def __init__(self, add_hash="no", _validate=False,
                 schemata=None):
        """Will be overridden by subclasses to take
        parameters specific to their function.
        """
        DoesLogging.__init__(self)
        self._do_preprocess = False # may get set to True, below
        self.last_flush = time.time()
        self._validate = _validate  
        # Parameter: add_hash
        try:
            self._add_hash = util.as_bool(add_hash)
            self._do_preprocess = True
        except ValueError, err:
            self.log.error("parameter.error",
                           name="add_hash", value=add_hash, msg=err)
            self._add_hash = False
        # Parameter: schemata
        self._schema = None
        if schemata:
            schema_files = [s.strip() for s in schemata.split(',')]
            try:
                p = schemacfg.SchemaParser(files=schema_files)
                self._schema = p.get_schema()
                self._do_preprocess = True
            except (IOError, ValueError),err:
                self.log.error("parameter.error",
                               name="schemata", value=schema_files,
                               msg=err)
            
    def process(self, data):
        """Override with logic; 'data' is a dictionary with timestamp,
        event, and other values.
        """
        pass

    def _preprocess(self, data):
        """Called before data is handed to subclass in order to allow
        standardized massaging / filtering of input data.

        Returns:
          - Result of module's process() function, or None if the
            data was rejected by validation.

        Exceptions:
          - ValueError
        """
        if self._schema:
            self._schema.event(data)
        if self._add_hash:
            data[HASH_FIELD] = hash_event(data)
        return data
    
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
          - PreprocessException (AnalyzerException): Something went wrong
             during preprocessing
          - ProcessException (AnalyzerException): Something went wrong
             during processing
        """
        if self._validate:
            valid = (TS_FIELD in data and EVENT_FIELD in data)
            if not valid:
                raise PreprocessException("Invalid event data in '%s'" %
                                          str(data))
        if self._do_preprocess:
            try:
                data = self._preprocess(data)
            except Exception, err:
                raise PreprocessException(str(err))
        try:
            result = self.process(data)
        except Exception, err:
            raise ProcessException(str(err))
        t = time.time()
        if t  - self.last_flush >= self.FLUSH_SEC:
            if self._dbg:
                self.log.debug("flush")
            self.flush()
            self.last_flush = t
        return result


class BufferedAnalyzer(Analyzer, threading.Thread):
    """Threaded class to process input in a buffered fashion.
    Intended for cases (db inserts, etc) where processing may
    lag behind input from the infomation broker, et al.
    """
    def __init__(self, *args, **kwargs):
        Analyzer.__init__(self, *args, **kwargs)
        threading.Thread.__init__(self)
        self.daemon = True
        self.running = False
        self.finishing = False
        self.queue = Queue.Queue()

    def process(self, data):
        """Get input from controlling process as per usual,
        put the input into the queue and return immediately.
        """
        if not self.running:
            self.running = True
            self.start()
        # if finish() has been called, stop queueing intput
        if not self.finishing:
            self.queue.put(data)

    def run(self):
        """Thread method - pull data FIFO style from the queue
        and pass off to the worker method.
        """
        self.log.info('run.start')
        while self.running:
            if not self.queue.empty():
                row = self.queue.get()
                self.process_buffer(row)
                if sys.version_info >= (2,5):
                    self.queue.task_done()
            else:
                time.sleep(0.1)
        self.log.info('run.end')

    def process_buffer(self, row):
        """Override with logic - this is the worker method
        where the user defines the loading behavior.
        """
        pass

    def finish(self):
        """This is called when processing is finished.  Waits
        for any queued data to be processed, and shuts down
        the processing thread.  See nl_load for an example on
        the appropriate time/place to call.
        """
        self.log.info('finish.begin')
        if not self.finishing:
            self.log.info('finish.finishing queue')
            self.finishing = True
        while not self.queue.empty():
            time.sleep(0.1)
        self.running = False
        if self.isAlive():
            self.join()
        #time.sleep(1)
        self.log.info('finish.end')

class Loader:
    """Abstract class for loading into database-like things.
    """
    def __init__(self, type=None, dsn=None, **kw):
        """Initialize state.        

        :Parameters:
          type - Name for type of database
          dsn - DBMS filename (sqlite) or host (others)
          kw - Additional connection keywords
        """
        # get connection class
        try:
            self.type = type.lower()
        except AttributeError:
            raise ValueError("Database type not a string")
        self.conn_class = CONNECTION_CLASSMAP.get(self.type, None)
        if self.conn_class is None:
            raise NotImplementedError("Unknown DB type '%s'" % type)
        # set server (or file) DSN
        if dsn is None:
            if self.conn_class is sqlite:
                self.dsn = "db.sqlite"
            else:
                self.dsn = "localhost"
        else:
            self.dsn = dsn
        # save connection keywords
        self.conn_kw = kw

    def connect(self):
        """Connect to the database.

        Return new connection (also in self._conn.connection)
        """
        self._conn = self.conn_class(self.dsn, self.conn_kw)
        return self._conn.connection

    def disconnect(self):
        """Disconnect, if connected.
        """
        if self._conn:
            self._conn.connection.close()
            self._conn = None
