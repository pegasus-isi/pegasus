"""
Base for analysis modules.
"""
import Queue
import re
import sys
import threading
import time
import logging
import warnings

from sqlalchemy import create_engine, orm

from Pegasus.db import connection
from Pegasus.netlogger import util
from Pegasus.netlogger.nlapi import TS_FIELD, EVENT_FIELD, HASH_FIELD
from Pegasus.netlogger.util import hash_event

class AnalyzerException(Exception):
    pass

class PreprocessException(AnalyzerException):
    pass

class ProcessException(AnalyzerException):
    pass

class SQLAlchemyInitWarning(Warning):
    pass

"""
Mixin class to provide SQLAlchemy database initialization/mapping.
Takes a SQLAlchemy connection string and a module function as
required arguments. 
"""
class SQLAlchemyInit(object):
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


class Analyzer(object):
    """Base analysis class. Doesn't do much.

    Parameters:
      - add_hash {yes,no,no*}: To each input event, add a new field,
           'nlhash', which is a probabilistically unique (MD5) hash of all
           the other fields in the event.
    """

    FLUSH_SEC = 5 # time to wait before calling flush()

    def __init__(self, add_hash="no", _validate=False):
        """Will be overridden by subclasses to take
        parameters specific to their function.
        """
        self.log = logging.getLogger("%s.%s" % (self.__module__, self.__class__.__name__))
        self._do_preprocess = False # may get set to True, below
        self.last_flush = time.time()
        self._validate = _validate
        # Parameter: add_hash
        try:
            self._add_hash = util.as_bool(add_hash)
            self._do_preprocess = True
        except ValueError, err:
            self.log.exception(err)
            self.log.error("Paramenter error: add_hash = %s", add_hash)
            self._add_hash = False

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
        while self.running:
            if not self.queue.empty():
                row = self.queue.get()
                self.process_buffer(row)
                if sys.version_info >= (2,5):
                    self.queue.task_done()
            else:
                time.sleep(0.1)

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
        if not self.finishing:
            self.finishing = True
        # XXX This can be replaced with queue.join() in Python 2.5
        while not self.queue.empty():
            time.sleep(0.1)
        self.running = False
        if self.isAlive():
            self.join()

