"""
Functions for output pegasus-monitord events to various destinations.
"""

##
#  Copyright 2007-2011 University Of Southern California
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
##

import os
import sys
import socket
import logging
import urlparse

from Pegasus.tools import utils
from Pegasus.netlogger import nlapi
from Pegasus.db.workflow_loader import WorkflowLoader
from Pegasus.db.dashboard_loader import DashboardLoader
from Pegasus.db import expunge

log = logging.getLogger(__name__)

# Optional imports, only generate 'warnings' if they fail
bson = None
try:
    import bson
except:
    log.info("cannot import BSON library, 'bson'")

amqp = None
try:
    from amqplib import client_0_8 as amqp
except:
    log.info("cannot import AMQP library")

# Event name-spaces
STAMPEDE_NS = "stampede."
DASHBOARD_NS = "dashboard."


def purge_wf_uuid_from_database(rundir, output_db):
    """
    This function purges a workflow id from the output database.
    """
    # PM-652 do nothing for sqlite
    # DB is already rotated in pegasus-monitord
    if output_db.lower().startswith('sqlite'):
        return

    # Parse the braindump file
    wfparams = utils.slurp_braindb(rundir)

    wf_uuid = wfparams.get("wf_uuid", None)
    if "wf_uuid" is None:
        return

    expunge.delete_workflow(output_db, wf_uuid)

def purge_wf_uuid_from_dashboard_database(rundir, output_db):
    """
    This function purges a workflow id from the output database.
    """

    # Parse the braindump file
    wfparams = utils.slurp_braindb(rundir)

    wf_uuid = wfparams.get("wf_uuid", None)
    if "wf_uuid" is None:
        return

    expunge.delete_dashboard_workflow(output_db, wf_uuid)

class OutputURL:
    """
    Break output URL into named parts for easier handling.
    """
    def __init__(self, url):

        # Fix for Python 2.5 and earlier 2.6, as their urlparse module
        # does not handle these schemes correctly (netloc empty and
        # everything after the scheme in path)
        if (url.startswith("amqp:")
            or url.startswith("mysql:")
            or url.startswith("x-tcp:")
            or url.startswith("sqlite:")):
            self.scheme, rest_url = url.split(":", 1)
            url = "http:" + rest_url

            http_scheme, self.netloc, self.path, self.params, query, frag = urlparse.urlparse(url)
        else:
            # No need to change anything
            self.scheme, self.netloc, self.path, self.params, query, frag = urlparse.urlparse(url)

        host_port = ''
        user_pass = ''

        if '@' in self.netloc:
            user_pass, host_port = self.netloc.split('@', 1)
        else:
            host_port = self.netloc
        if ':' in host_port:
            self.host, portstr = host_port.split(':', 1)
            self.port = int(portstr)
        else:
            self.host = self.netloc
            self.port = None

        if ':' in user_pass:
            self.user, self.password = user_pass.split( ':', 1 )

class EventSink(object):
    """
    Base class for an Event Sink.
    """
    def __init__(self):
        self._log = logging.getLogger("%s.%s" % (self.__module__, self.__class__.__name__))

    def send(self, event, kw):
        """
        Clients call this function to send an event to the sink.
        """
        pass

    def close(self):
        """
        Clients call this function to close the output to this sink.
        """
        pass

    def flush(self):
        "Clients call this to flush events to the sink"
        pass

class DBEventSink(EventSink):
    """
    Write wflow event logs to database via loader
    """
    def __init__(self, dest, db_stats=False, namespace=STAMPEDE_NS, props=None, db_type=None, backup=False, **kw):
        self._namespace=namespace
        #pick the right database loader based on prefix
        if namespace == STAMPEDE_NS:
            self._db = WorkflowLoader(dest, perf=db_stats, batch=True, props=props, db_type=db_type, backup=backup)
        elif namespace == DASHBOARD_NS:
            self._db = DashboardLoader(dest, perf=db_stats, batch=True, props=props, db_type=db_type, backup=backup)
        else:
            raise ValueError("Unknown namespace specified '%s'" % (namespace))

        super(DBEventSink, self).__init__()

    def send(self, event, kw):
        self._log.trace("send.start event=%s", event)
        d = {'event' : self._namespace + event}
        for k, v in kw.iteritems():
            d[k.replace('__','.')] = v
        self._db.process(d)
        self._log.trace("send.end event=%s", event)

    def close(self):
        self._log.trace("close.start")
        self._db.finish()
        self._log.trace("close.end")

    def flush(self):
        self._db.flush()

class FileEventSink(EventSink):
    """
    Write wflow event logs to a file.
    """
    def __init__(self, path, restart=False, encoder=None, **kw):
        super(FileEventSink, self).__init__()
        if restart:
            self._output = open(path, 'w', 1)
        else:
            self._output = open(path, 'a', 1)
        self._encoder = encoder

    def send(self, event, kw):
        self._log.trace("send.start event=%s", event)
        self._output.write(self._encoder(event=event, **kw))
        self._log.trace("send.end event=%s", event)

    def close(self):
        self._log.trace("close.start")
        self._output.close()
        self._log.trace("close.end")

class TCPEventSink(EventSink):
    """
    Write wflow event logs to a host:port.
    """
    def __init__(self, host, port, encoder=None, **kw):
        super(TCPEventSink, self).__init__()
        self._encoder = encoder
        self._sock = socket.socket()
        self._sock.connect((host, port))

    def send(self, event, kw):
        self._log.trace("send.start event=%s", event)
        self._sock.send(self._encoder(event=event, **kw))
        self._log.trace("send.end event=%s", event)

    def close(self):
        self._log.trace("close.start")
        self._sock.close()
        self._log.trace("close.end")

class AMQPEventSink(EventSink):
    """
    Write wflow event logs to an AMQP server.
    """
    EXCH_OPTS = {'type' : 'topic', 'durable' : True, 'auto_delete' : False}
    DEFAULT_AMQP_VIRTUAL_HOST="pegasus"  #should be /

    def __init__(self, host, port, exch=None, encoder=None,
                 userid='guest', password='guest', virtual_host=DEFAULT_AMQP_VIRTUAL_HOST,
                 ssl=False, connect_timeout=None, **kw):
        super(AMQPEventSink, self).__init__()
        self._encoder = encoder

        self._log.info( "Connecting to host: %s:%s virtual host: %s exchange: %s with user: %s ssl: %s" %(host, port, virtual_host, exch, userid, ssl ))
        self._conn = amqp.Connection(host="%s:%s" % (host, port),
                                     userid=userid, password=password,
                                     virtual_host=virtual_host, ssl=ssl,
                                     connect_timeout=connect_timeout, **kw)
        self._channel = self._conn.channel()
        self._exch = exch
        self._channel.exchange_declare(exch, **self.EXCH_OPTS)

    def send(self, event, kw):
        full_event = STAMPEDE_NS + event
        self._log.trace("send.start event=%s", full_event)
        data = self._encoder(event=event, **kw)
        self._channel.basic_publish(amqp.Message(body=data),
                                    exchange=self._exch, routing_key=full_event)
        self._log.trace("send.end event=%s", event)

    def close(self):
        self._log.trace("close.start")
        self._conn.close()
        self._log.trace("close.end")

def bson_encode(event, **kw):
    """
    Adapt bson.dumps() to NetLogger's Log.write() signature.
    """
    kw['event'] = STAMPEDE_NS + event
    return bson.dumps(kw)

def create_wf_event_sink(dest, enc=None, prefix=STAMPEDE_NS, props=None, **kw):
    """
    Create & return subclass of EventSink, chosen by value of 'dest'
    and parameterized by values (if any) in 'kw'.
    """

    if dest is None:
        return None

    url = OutputURL(dest)

    log.info( "Connecting workflow event sink to %s" %dest)

    # Pick an encoder

    def pick_encfn(enc_name, namespace ):
        if enc_name is None or enc_name == 'bp':
            # NetLogger name=value encoding
            encfn = nlapi.Log(level=nlapi.Level.ALL, prefix=namespace)
        elif enc_name == 'bson':
            # BSON
            if bson is None:
                raise Exception("BSON encoding selected, but cannot import bson library")
            encfn = bson_encode
        else:
            raise ValueError("Unknown encoding '%s'" % (enc_name))
        return encfn

    # Branch on scheme
    if url.scheme == '':
        sink = FileEventSink(dest, encoder=pick_encfn(enc, prefix), **kw)
        _type, _name = "file", dest
    elif url.scheme == 'file':
        sink = FileEventSink(url.path, encoder=pick_encfn(enc, prefix), **kw)
        _type, _name = "file", url.path
    elif url.scheme == 'x-tcp':
        if url.port is None:
            url.port = 14380
        sink = TCPEventSink(url.host, url.port, encoder=pick_encfn(enc, prefix), **kw)
        _type, _name = "network", "%s:%s" % (url.host, url.port)
    elif url.scheme == 'amqp':
        # amqp://[USERNAME:PASSWORD@]<hostname>[:port]/[<virtualhost>]/<exchange_name>
        if amqp is None:
            raise Exception("AMQP destination selected, but cannot import AMQP library")
        if url.port is None:
            url.port = 5672 # RabbitMQ default

        # PM-1258 parse exchange and virtual host info
        exchange = None
        virtual_host= None
        path_comp=url.path.split('/')
        if path_comp is not None:
            exchange=path_comp.pop()
        if path_comp is not None:
            virtual_host=path_comp.pop()
            if len(virtual_host) == 0:
                virtual_host = None

        sink = AMQPEventSink(url.host, url.port, virtual_host=virtual_host, exch=exchange,
                             userid = url.user, password=url.password, ssl=False,
                             encoder=pick_encfn(enc,prefix), **kw)
        _type, _name="AMQP", "%s:%s/%s" % (url.host, url.port, url.path)
    else:
        # load the appropriate DBEvent on basis of prefix passed
        sink = DBEventSink(dest, namespace=prefix, props=props, **kw)
        _type, _name = "DB", dest

    log.info("output type=%s namespace=%s name=%s" % (_type, prefix, _name))

    return sink

