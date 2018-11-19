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

import socket
import logging
import urlparse
import traceback
import json
import re

from Pegasus.tools import utils
from Pegasus.tools import properties
from Pegasus.netlogger import nlapi
from Pegasus.db.workflow_loader import WorkflowLoader
from Pegasus.db.dashboard_loader import DashboardLoader
from Pegasus.db import expunge
from Pegasus.db import connection

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
        # Set listing events handled to be kept consistent with dict in workflow loader
        self._acceptedEvents = (
            'stampede.wf.plan',
            'stampede.wf.map.task_job',
            'stampede.static.start',
            'stampede.static.end',
            'stampede.xwf.start',
            'stampede.xwf.end',
            'stampede.xwf.map.subwf_job',
            'stampede.task.info',
            'stampede.task.edge',
            'stampede.job.info',
            'stampede.job.edge',
            'stampede.job_inst.pre.start',
            'stampede.job_inst.pre.term',
            'stampede.job_inst.pre.end',
            'stampede.job_inst.submit.start',
            'stampede.job_inst.submit.end',
            'stampede.job_inst.held.start',
            'stampede.job_inst.held.end',
            'stampede.job_inst.main.start',
            'stampede.job_inst.main.term',
            'stampede.job_inst.main.end',
            'stampede.job_inst.post.start',
            'stampede.job_inst.post.term',
            'stampede.job_inst.post.end',
            'stampede.job_inst.host.info',
            'stampede.job_inst.image.info',
            'stampede.job_inst.abort.info',
            'stampede.job_inst.grid.submit.start',
            'stampede.job_inst.grid.submit.end',
            'stampede.job_inst.globus.submit.start',
            'stampede.job_inst.globus.submit.end',
            'stampede.job_inst.tag',
            'stampede.inv.start',
            'stampede.inv.end',
            'stampede.static.meta.start',
            'stampede.xwf.meta',
            'stampede.task.meta',
            'stampede.rc.meta',
            'stampede.int.metric',
            'stampede.rc.pfn',
            'stampede.wf.map.file',
            'stampede.static.meta.end',
            'stampede.task.monitoring'
        )

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
        for k, v in kw.items():
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
        if self._encoder == json_encode:
            self._output.write('\n')
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
                 ssl=False, props=None, connect_timeout=None, **kw):
        super(AMQPEventSink, self).__init__()
        self._log.info( "Properties received %s", props)
        self._encoder = encoder

        if connect_timeout is None:
            # pick timeout from properties
            connect_timeout = props.property("timeout")
            if connect_timeout:
                connect_timeout = float(connect_timeout)

        self._log.info( "Connecting to host: %s:%s virtual host: %s exchange: %s with user: %s ssl: %s" %(host, port, virtual_host, exch, userid, ssl ))
        self._conn = amqp.Connection(host="%s:%s" % (host, port),
                                     userid=userid, password=password,
                                     virtual_host=virtual_host, ssl=ssl,
                                     connect_timeout=connect_timeout, **kw)
        self._channel = self._conn.channel()
        self._exch = exch
        self._channel.exchange_declare(exch, **self.EXCH_OPTS)
        self._handled_events = set()
        self._handle_all_events = False
        self.configure_filters(props.property("events"))

    def configure_filters(self, events):
        event_regexes = set()

        if events is None:
            # add pre-configured specific events
            event_regexes.add(re.compile(STAMPEDE_NS + "job_inst.tag"))
            event_regexes.add(re.compile(STAMPEDE_NS + "inv.end"))
            event_regexes.add(re.compile(STAMPEDE_NS + "wf.plan"))
        else:
            for exp in events.split(","):
                if exp == "*":
                    # short circuit
                    self._handle_all_events = True
                    self._log.debug("Events Handled: All")
                    return
                else:
                    event_regexes.add(re.compile(exp))

        # go through each regex and match against accepted events once
        for regex in event_regexes:
            # go through each list of accepted events to check match
            for event in self._acceptedEvents:
                if regex.search(event) is not None:
                    self._handled_events.add(event)

        self._log.debug( "Events Handled: %s", self._handled_events)


    def send(self, event, kw):
        full_event = STAMPEDE_NS + event
        if self.ignore(full_event):
            return

        self._log.trace("send.start event=%s", full_event)
        data = self._encoder(event=event, **kw)
        self._channel.basic_publish(amqp.Message(body=data),
                                    exchange=self._exch, routing_key=full_event)
        self._log.trace("send.end event=%s", event)

    def ignore(self, event):
        if self._handle_all_events:
            # we want all events
             return False

        return event not in self._handled_events



    def close(self):
        self._log.trace("close.start")
        self._conn.close()
        self._log.trace("close.end")


class MultiplexEventSink(EventSink):
    """
    Sends events to multiple end points
    """

    def __init__(self, dest, enc, prefix=STAMPEDE_NS,  props=None, **kw):
        super( MultiplexEventSink, self ).__init__()
        self._endpoints = {}
        self._log.info("Multiplexed Event Sink Connection Properties  %s", props)
        for key in props.keyset():
            if key.endswith( ".url" ):
                sink_name= key[0:key.rfind(".url")]

                # remove from our copy sink_name properties if they exist
                endpoint_props = properties.Properties( props.propertyset(sink_name + ".", remove=True ))
                try:
                    self._endpoints[ sink_name ] = create_wf_event_sink(props.property(key), db_type=connection.DBType.WORKFLOW, enc=enc,
                                                                                   prefix=prefix, props=endpoint_props, multiplexed = True, **kw)
                except:
                    self._log.error("[multiplex event sender] Unable to connect to endpoint %s with props %s . Disabling" % (sink_name,endpoint_props))
                    self._log.error(traceback.format_exc())

    def send(self, event, kw):
        remove_endpoints=[]
        for key in self._endpoints:
            sink = self._endpoints[key]
            try:
                sink.send(event, kw)
            except:
                self._log.error(traceback.format_exc())
                self._log.error("[multiplex event sender] error sending event. Disabling endpoint %s" %key )
                self.close_sink( sink )
                remove_endpoints.append( key )

        # remove endpoints that are disabled
        for key in remove_endpoints:
            del self._endpoints[key]

    def close(self):
        for key in self._endpoints:
            self._log.debug("[multiplex event sender] Closing endpoint %s" % key)
            self.close_sink(self._endpoints[key])

    def close_sink(self, sink):
        try:
            sink.close()
        except:
            pass

    def flush(self):
        "Clients call this to flush events to the sink"
        for key in self._endpoints:
            self._log.debug("[multiplex event sender] Flushing endpoint %s" % key)
            self._endpoints[key].flush()

def bson_encode(event, **kw):
    """
    Adapt bson.dumps() to NetLogger's Log.write() signature.
    """
    kw['event'] = STAMPEDE_NS + event
    return bson.dumps(kw)

def json_encode(event, **kw):
    """
    Adapt bson.dumps() to NetLogger's Log.write() signature.
    """
    kw['event'] = STAMPEDE_NS + event
    return json.dumps(kw)

def create_wf_event_sink(dest, db_type, enc=None, prefix=STAMPEDE_NS, props=None, multiplexed = False, **kw):
    """
    Create & return subclass of EventSink, chosen by value of 'dest'
    and parameterized by values (if any) in 'kw'.
    """

    if dest is None:
        return None

    # we only subset the properties and strip off prefix once
    if not multiplexed:
        sink_props = get_workflow_connect_props(props, db_type)
        # we delete from our copy pegasus.catalog.workflow.url as we want with default prefix
        if "url" in sink_props.keyset():
            del sink_props["url"]
            sink_props.property( "default.url", dest )
    else:
        sink_props = props

    # PM-898 are additional URL's to populate specified
    if not multiplexed and multiplex( dest, prefix , props ):
        sink_props.property("default.url", dest)
        # any properties that don't have a . , remap to default.propname
        for key in sink_props.keyset():
            if key.find(".") == -1:
                sink_props.property("default." + key, sink_props.property(key))
                del sink_props[key]

        return MultiplexEventSink(dest, enc, prefix, sink_props, **kw)

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
        elif enc_name == 'json':
            encfn = json_encode
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
                             encoder=pick_encfn(enc,prefix),props=sink_props, **kw)
        _type, _name="AMQP", "%s:%s/%s" % (url.host, url.port, url.path)
    else:
        # load the appropriate DBEvent on basis of prefix passed
        sink = DBEventSink(dest, namespace=prefix, props=sink_props, **kw)
        _type, _name = "DB", dest

    log.info("output type=%s namespace=%s name=%s" % (_type, prefix, _name))

    return sink


def multiplex( dest, prefix, props=None):
    """
    Determines whether we need to multiplex and events to multiple sinks
    :param dest:
    :param props:
    :return:
    """
    if props is None:
        return False

    # we never attempt multiplex on dashboard sink
    if prefix == DASHBOARD_NS:
        return False

    additional_sink_props = props.propertyset("pegasus.catalog.workflow" + ".", False)
    multiplex = False
    for key in additional_sink_props:
        if key == "pegasus.catalog.workflow.url":
            pass
        if key.endswith(".url"):
            multiplex = True
            break
    return multiplex


def get_workflow_connect_props( props, db_type ):
    """
    Returns connection properties for workflow database

    :param props:
    :return:
    """

    if props is None:
        return None

    # first get the default one's with the star notation
    connect_props = properties.Properties(props.propertyset("pegasus.catalog.*.", True))

    prefix = "pegasus.catalog.workflow"
    if db_type == connection.DBType.MASTER:
        prefix = "pegasus.catalog.dashboard"

    # over ride these with workflow specific or dashboard specific props
    addons = props.propertyset( prefix + ".", True)
    for key in addons:
        connect_props.property( key, addons[key])

    return connect_props
