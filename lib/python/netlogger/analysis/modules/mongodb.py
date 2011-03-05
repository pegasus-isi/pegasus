"""
Load input in to a MongoDB database.

See http://www.mongodb.org/ for details on MongoDB
"""
# Standard library
from datetime import datetime
import re
import sys
import time
# Third-party
import pymongo
from pymongo.connection import Connection
from pymongo.errors import ConnectionFailure
# NetLogger
from netlogger.analysis.modules._base import Analyzer as BaseAnalyzer
from netlogger.analysis.modules._base import ConnectionException
from netlogger import util
from netlogger.nlapi import TS_FIELD, STATUS_FIELD

class Analyzer(BaseAnalyzer):
    """Load into a collection in a MongoDB database.

    Parameters: 

      - host {string,localhost*}:  mongod server host
      - port {integer,27017*}: mongod server port
      - database {string,application*}: Name of database to create/use
      - collection {string,netlogger*}: Name of collection to create/use
      - user {string,anonymous*}: Name of user to authenticate as
      - password {string,None*}: Password to authenticate with
      - indices {field1,field2,...,""*}: Comma-separated list of fields to index.
        Add a caret ('^') before the field name to make the index "unique".
        Example: "ts,event,^_hash".
      - datetime {yes,no,yes*}: If 'yes', convert the timestamp
        in 'ts' into a datetime object. Otherwise, leave it as
        a floating-point number.
      - event_filter {regex,""*}: Regular expression for event name.
        If it doesn't match (from the start of the event name), 
        the event will not be inserted.

    Attributes:

       - connection: MongoDB Connection instance.
       - database: MongoDB Database instance.
       - collection: MongoDB Collection instance.
    """
    def __init__(self, host="localhost", port=27017,
                 database='application', collection='netlogger',
                 indices="", datetime='yes',
                 intvals="", floatvals="", event_filter="",
                 user="", password="", batch=0, perf=None, **kw):
        BaseAnalyzer.__init__(self, _validate=True, **kw)
        # map for converting values
        self._convert = { }
        # mongo database and collection
        self.db_name, self.coll_name = database, collection
        # connect
        try:
            self.connection = pymongo.Connection(host=host, port=port)
        except ConnectionFailure:
            raise ConnectionException("Couldn't connect to DB "
                                      "at %s:%d" % (host, port))
        # create/use database, by retrieving it
        if self._dbg:
            self.log.debug("init.database_name", value=self.db_name)
        self.database = self.connection[self.db_name]
        # if authentication is on, use it
        if user != "":
            success = self.database.authenticate(user, password)
            if not success:
                raise ConnectionException("Could not authenticate to "
                                          "database=%s, collection=%s as user '%s'" % (
                                              self.db_name, self.coll_name, user))
        # create/use collection, by retrieving it
        if self._dbg:
            self.log.debug("init.collection_name", value=self.coll_name)
        self.collection = self.database[self.coll_name]
        # ensure indexes are set
        index_fields = indices.split(",")
        for field in index_fields:
            field = field.strip()
            if not field or field == "^":
                continue
            if self._dbg:
                self.log.debug("init.index", value=field)
            if field[0] == '^':
                unique = True
                field = field[1:]
            else:
                unique = False
            self.collection.ensure_index(field, unique=unique)
        # datetime flag
        self._datetime = util.as_bool(datetime)
        # Add numeric values to conversion map
        if intvals.strip():
            self._convert.update(dict.fromkeys(intvals.split(','),int))
        if floatvals.strip():
            self._convert.update(dict.fromkeys(floatvals.split(','),float))
        # filter, if given
        self._event_re = None
        if event_filter:
            self._event_re = re.compile(event_filter)
        # batch, if requested
        if batch:
            self._batch = int(batch)
            self._curbatch = [ ]
            self._batchlen = 0
        else:
            self._batch = 0

    def fix_key_formats(self, data):
        """Make sure key names are not illegal
        * cannot have a '.' anywhere will replace with '_'
        * cannot have $ as first symbol will remove
        """
        fixed_data = { }
        for key, value in data.items():
            key = key.replace('.', '_')
            if key[0] == '$':
                key = key[1:]
            fixed_data[key] = value
        return fixed_data

    def process(self, data):
        """Insert 'data' into Mongo collection.
        """
        if self._dbg:
            self.log.debug("process_data.start")

        # Apply filter, if there is one
        if self._event_re is not None:
            try:
                m = self._event_re.match(data['event'])
            except KeyError:
                raise ValueError("no 'event' field")
            if m is None:
                if self._dbg:
                    self.log.debug("process_data.end", msg="filtered out")
                return

        # fix keys
        data = self.fix_key_formats(data)

        # try to set status to int
        if STATUS_FIELD in data:
            try:
                data[STATUS_FIELD] = int(data[STATUS_FIELD])
            except ValueError:
                self.log.warn("bad_status", value=data[STATUS_FIELD],
                              msg="not integer")
        # optionally convert timestamp to datetime
        if self._datetime:
            ts = data[TS_FIELD]
            if not isinstance(ts, datetime):
                data[TS_FIELD] = datetime.utcfromtimestamp(ts)

        # convert fields
        for key, func in self._convert.items():
            if key in data:
                try:
                    data[key] = func(data[key])
                except ValueError:
                    self.log.warn("bad_value", value=data[key],
                                  msg="expected " + str(func))
                    del data[key]
        # insert data
        if self._batch > 0:
            self._curbatch.append(data)
            self._batchlen += 1
            if self._batchlen > self._batch:
                self.collection.insert(self._curbatch)
                self._curbatch = [ ]
                self._batchlen = 0
        else:
            self.collection.insert(data)

        if self._dbg:
            self.log.debug("process_data.end", status=0)

    def finish(self):
        if self._batch > 0 and self._batchlen > 0:
            self.collection.insert(self._curbatch)
