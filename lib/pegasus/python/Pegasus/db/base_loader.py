import time
import logging

from Pegasus.db import connection
from sqlalchemy import exc

class BaseLoader(object):
    "Base loader class. Has a database session and a log handle."

    def __init__(self, dburi, batch=True, props=None, db_type=None, backup=False, flush_every=1000):
        """Will be overridden by subclasses to take
        parameters specific to their function.
        """
        self.log = logging.getLogger("%s.%s" % (self.__module__, self.__class__.__name__))
        self.dburi = dburi

        # PM-898 all props passed should have pegasus prefix stripped off
        # so they are more like connect_args to be used for database
        connect_args = {}
        for key in props.keyset():
            # we don't pass url in connect args
            if key != "url":
                connect_args[key] = props.property(key)

        # make sure timeout is an int
        if "timeout" in connect_args:
            connect_args["timeout"] = int(connect_args["timeout"])

        #self.session = connection.connect(dburi, create=True, props=props, db_type=db_type, backup=backup)
        self.session = connection.connect(dburi, create=True, connect_args=connect_args, db_type=db_type, backup=backup)

        # flags and state for batching
        self._batch = batch
        self._flush_every = flush_every
        self._flush_count = 0
        self._last_flush = time.time()

    def process(self, data):
        """Override with logic; 'data' is a dictionary with timestamp,
        event, and other values.
        """
        pass

    def flush(self):
        "Try to flush the batch"
        self.check_flush()

    def check_flush(self, increment=False):
        """
        Check to see if the batch needs to be flushed based on
        either the number of queued inserts or based on time
        since last flush.
        """
        if not self._batch:
            return

        if increment:
            self._flush_count += 1

        if self._flush_count >= self._flush_every:
            self.log.debug('Flush: flush count')
            self.hard_flush()
            return

        if (time.time() - self._last_flush) > 30:
            self.log.debug('Flush: time based')
            self.hard_flush()

    def hard_flush(self, batch_flush=True, retry=0):
        "Subclasses override this with flushing logic"
        pass

    def reset_flush_state(self):
        "Reset the internal flust state if batching"
        if self._batch:
            self.log.debug('Resetting flush state')
            self._flush_count = 0
            self._last_flush = time.time()

    def finish(self):
        """Override with logic if subclass needs a cleanup method,
        ie: threading or whatnot
        """
        pass

    def disconnect(self):
        self.session.close()

    def check_connection(self, sub=False):
        self.log.trace('Checking connection')
        try:
            self.session.connection().closed
        except exc.OperationalError as e:
            try:
                if not self.session.is_active:
                    self.session.rollback()
                self.log.error('Lost connection - attempting reconnect')
                time.sleep(5)
                self.session.connection().connect()
            except exc.OperationalError as e:
                self.check_connection(sub=True)
            if not sub:
                self.log.warn('Connection re-established')

    def individual_commit(self, event, merge=False):
        """
        @type   merge: boolean
        @param  merge: Set to true if the row should be a merge
                rather than a plain insert.

        This gets called by hard_flush if there is a problem
        with a batch commit to commit each object individually.
        """
        try:
            if merge:
                event.merge_to_db(self.session)
            else:
                event.commit_to_db(self.session)
            self.session.expunge(event)
        except exc.IntegrityError as e:
            self.log.error('Insert failed for event %s : %s', event, e)
            self.session.rollback()

