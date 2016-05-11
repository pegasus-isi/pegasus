from influxdb.influxdb08 import InfluxDBClient
from influxdb.influxdb08.client import InfluxDBClientError
import urlparse
import os
import logging
import json

from Pegasus.monitoring import event_output
from Pegasus.tools import amqp

log = logging.getLogger(__name__)

class OnlineMonitord:
    def __init__(self, wf_label, wf_uuid, dburi, child_conn):
        self.wf_label = wf_label
        self.wf_uuid = wf_uuid
        self.wf_name = wf_label + ":" + wf_uuid
        self.event_sink = event_output.create_wf_event_sink(dburi)
        self.child_conn = child_conn
        self.influxdb_url = self.getconf("INFLUXDB_URL")
        self.amqp_url = self.getconf("PEGASUS_AMQP_URL")
        self.influx_client = None

    def getconf(self, name):
        value = os.getenv(name)
        if value is None:
            log.error("'%s' is not set in the environment" % name)
            return None
        return value

    def start(self):
        log.info("Starting online monitoring listener...")
        self.setup_timeseries_db_conn()
        #self.start_web_server()
        self.start_consuming_mq_messages()

    def setup_timeseries_db_conn(self):
        if self.influxdb_url is None:
            log.error("Unable to connect to InfluxDB")
            return

        url = urlparse.urlparse(self.influxdb_url)

        self.influx_client = InfluxDBClient(host=url.hostname, port=url.port,
                                            username=url.username, password=url.password,
                                            ssl=(url.scheme == "https"))

        # Create the database for this workflow
        try:
            self.influx_client.create_database(self.wf_name)
        except InfluxDBClientError, e:
            # Influx returns 409 if the database exists
            if e.code != 409:
                raise

        self.influx_client.switch_database(self.wf_name)
        self.influx_client.switch_user(url.username, url.password)

    def start_web_server(self):
        # This is how you could eventually have kickstart report directly to
        # monitord. This starts a web API, and then writes the address to
        # the monitord.env file, which is picked up by pegasus-submit-job
        # and passed to the environment of condor_submit, where it can be
        # incorporated into the job environment using $ENV() classad exprs.
        from flask import Flask, request
        import socket

        # The endpoint is the workflow UUID
        endpoint = "/" + self.wf_uuid

        app = Flask(__name__)

        @app.route(endpoint, methods=["POST"])
        def post_monitoring_data():
            print request.headers
            print request.json
            return "", 200

        # Get a random port to use
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(('localhost', 0))
        port = sock.getsockname()[1]
        sock.close()

        # Construct URL and write it to env file
        url = "http://%s:%d%s" % (socket.gethostname(), port, endpoint)
        f = open("monitord.env", "w")
        f.write("KICKSTART_MON_URL=%s\n" % url)
        f.close()

        # Start flask server
        app.run(host="0.0.0.0", port=port)

    def start_consuming_mq_messages(self):
        if self.amqp_url is None:
            log.error("Unable to connect to RabbitMQ")
            return

        mq_conn = amqp.connect(self.amqp_url)

        mq_channel = mq_conn.channel()

        # create a queue for the observer workflow uuid
        mq_channel.queue_declare(queue=self.wf_name, auto_delete=True, exclusive=True)

        # bind the queue to the monitoring exchange
        mq_channel.queue_bind(queue=self.wf_name, exchange="monitoring", routing_key=self.wf_uuid)

        message_count = None
        db_is_processing_events = False

        for result in mq_channel.consume(self.wf_name, no_ack=True, inactivity_timeout=60.0):

            # If we got a message, then process it
            if result:
                method_frame, header_frame, body = result
                self.on_message(body)

            # Check to see if monitord sent us a message
            if self.child_conn.poll():
                msg = self.child_conn.recv()
                log.info("Got a message from monitord: '%s'" % msg)
                if msg == "WORKFLOW_ENDED":
                    self.child_conn.send("WAIT")

                    # check how many messages we have in a message broker
                    response = mq_channel.queue_declare(self.wf_name, passive=True)
                    log.info('The queue has {0} more messages'.format(response.method.message_count))
                    message_count = int(response.method.message_count)

                    # check if there is any event to be processed by database
                    db_is_processing_online_monitoring_msgs = getattr(self.event_sink, "is_processing_online_monitoring_msgs", None)
                    if callable(db_is_processing_online_monitoring_msgs):
                        db_is_processing_events = db_is_processing_online_monitoring_msgs()
                    else:
                        db_is_processing_events = False

            # TODO Check the logic here to make sure this actually exits
            if message_count == 0 and (not db_is_processing_events):
                log.info("No more messages to process")
                break

        log.info("Sending OK to monitord")
        self.child_conn.send("OK")
        mq_conn.close()

    def on_message(self, body):
        try:
            message = json.loads(body)
            self.write_influx(message)
            self.write_stampede(message)
        except Exception, e:
            log.exception(e)

    def write_influx(self, message):
        if self.influx_client is None:
            return

        trace_id = "{0}:{1}:{2}".format(message["dag_job_id"], message["condor_job_id"], message["pid"])

        data = {}
        data["time"] = int(message["ts"] * 1000000) # microseconds
        data["read_bytes"] = message["bread"]
        data["write_bytes"] = message["bwrite"]
        data["rchar"] = message["rchar"]
        data["wchar"] = message["wchar"]
        data["syscr"] = message["syscr"]
        data["syscw"] = message["syscw"]
        data["utime"] = message["utime"]
        data["stime"] = message["stime"]
        data["iowait"] = message["iowait"]
        data["vmSize"] = message["vm"]
        data["vmRSS"] = message["rss"]
        data["threads"] = message["threads"]
        data["procs"] = message["procs"]
        data["bsend"] = message["bsend"]
        data["brecv"] = message["brecv"]
        data["totins"] = message.get("totins", None)
        data["fpops"] = message.get("fpops", None)
        data["fpins"] = message.get("fpins", None)
        data["ldins"] = message.get("ldins", None)
        data["srins"] = message.get("srins", None)
        data["l3misses"] = message.get("l3misses", None)
        data["l2misses"] = message.get("l2misses", None)
        data["l1misses"] = message.get("l1misses", None)

        try:
            point = [
                {
                    "name": trace_id,
                    "columns": data.keys(),
                    "points": [data.values()]
                }
            ]
            self.influx_client.write_points(point, time_precision="u")
        except Exception, err:
            log.error("An error occured while sending monitoring measurement to InfluxDB: ")
            log.exception(err)

    def write_stampede(self, message):
        """Sending an event to an event sink (stampede db most probably) about aggregated measurement."""
        if self.event_sink is None:
            return

        event = "job.monitoring"

        # Map monitoring message to stampede db
        value = {}
        value["wf_uuid"] = message["wf_uuid"]
        value["sched_id"] = message["condor_job_id"]
        value["dag_job_id"] = message["dag_job_id"]
        value["hostname"] = message["hostname"] or None
        value["exec_name"] = message["exe"] or None
        value["kickstart_pid"] = message["pid"]
        value["ts"] = message["ts"]
        value["stime"] = message["stime"]
        value["utime"] = message["utime"]
        value["iowait"] = message["iowait"]
        value["vmsize"] = message["vm"]
        value["vmrss"] = message["rss"]
        value["read_bytes"] = message["bread"]
        value["write_bytes"] = message["bwrite"]
        value["syscr"] = message["syscr"]
        value["syscw"] = message["syscw"]
        value["threads"] = message["threads"]
        value["bytes_transferred"] = message["bsend"] + message["brecv"]
        value["transfer_duration"] = None
        value["site"] = message["site"] or None
        value["totins"] = message.get("totins", None)
        value["fpops"] = message.get("fpops", None)
        value["fpins"] = message.get("fpins", None)
        value["ldins"] = message.get("ldins", None)
        value["srins"] = message.get("srins", None)
        value["l3misses"] = message.get("l3misses", None)
        value["l2misses"] = message.get("l2misses", None)
        value["l1misses"] = message.get("l1misses", None)

        try:
            log.debug("Sending record to DB: %s --> %s" % (event, value))
            self.event_sink.send(event, value)
        except Exception, e:
            log.error("Error sending event: %s --> %s" % (event, value))
            log.exception(e)

    def close(self):
        self.event_sink.close()

