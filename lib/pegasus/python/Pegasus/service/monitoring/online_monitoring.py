import pika
from influxdb.influxdb08 import InfluxDBClient
from influxdb.influxdb08.client import InfluxDBClientError
import datetime
import sys
import urlparse
import os
import logging
import ssl
import time
import copy

from Pegasus.monitoring import event_output

log = logging.getLogger(__name__)

class OnlineMonitord:
    def __init__(self, wf_label, wf_uuid, dburi, child_conn):
        log.info("Pegasus wf_label: %s" % wf_label)
        log.info("Pegasus wf_uuid: %s" % wf_uuid)
        log.info("Stampede Database URI: %s" % dburi)

        self.wf_label = wf_label
        self.wf_uuid = wf_uuid
        self.wf_name = wf_label + ":" + wf_uuid
        self.event_sink = event_output.create_wf_event_sink(dburi)
        self.child_conn = child_conn
        self.influxdb_url = self.getconf("INFLUXDB_URL")
        self.rabbitmq_url = self.getconf("KICKSTART_MON_ENDPOINT_URL")
        self.rabbitmq_credentials = self.getconf("KICKSTART_MON_ENDPOINT_CREDENTIALS")

        self.aggregators = dict()

        self.influx_client = None

    def getconf(self, name):
        value = os.getenv(name)
        if value is None:
            log.error("'%s' is not set in the environment" % name)
            return None
        log.info("%s: %s" % (name, value))
        return value

    def start(self):
        self.setup_timeseries_db_conn()
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

    def start_consuming_mq_messages(self):
        if self.rabbitmq_url is None or self.rabbitmq_credentials is None:
            log.error("Unable to connect to RabbitMQ")
            return

        username, password = self.rabbitmq_credentials.split(":")

        rabbit_url = urlparse.urlparse(self.rabbitmq_url)
        virtual_host = rabbit_url.path.split("/")[3]
        exchange_name = rabbit_url.path.split("/")[4]

        parameters = pika.ConnectionParameters(host=rabbit_url.hostname,
                                               port=rabbit_url.port - 10000,
                                               ssl=(rabbit_url.scheme == "https"),
                                               ssl_options={"cert_reqs": ssl.CERT_NONE},
                                               virtual_host=virtual_host,
                                               credentials=pika.PlainCredentials(username, password))
        mq_conn = pika.BlockingConnection(parameters)

        mq_channel = mq_conn.channel()

        # create a queue for the observer workflow uuid
        mq_channel.queue_declare(queue=self.wf_name, auto_delete=True, exclusive=True)

        # bind the queue to the monitoring exchange
        mq_channel.queue_bind(queue=self.wf_name, exchange=exchange_name, routing_key=self.wf_uuid)

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
        """
        An utility function for processing messages regarding online monitoring for a particular workflow.
        :param body: raw body of the message
        """
        # Each message might have more than one line in it
        for msg_body in body.split(":delim1:"):
            msg_body = msg_body.strip()
            if len(msg_body) == 0:
                continue

            if len(msg_body.split(" ")) < 2 or len(msg_body) < 4:
                log.warning("The given measurement line is too short: %s", msg_body)
                continue

            message = MonitoringMessage.parse(msg_body)
            if message is not None:
                self.write_points(message.trace_id, message.metrics(), message.measurements())
                # FIXME Do the aggregation
                #self.handle_aggregation(message)

    def write_points(self, trace_id, columns, point):
        if self.influx_client is not None:
            try:
                data = [
                    {
                        "name": trace_id,
                        "columns": columns,
                        "points": [point]
                    }
                ]
                self.influx_client.write_points(data)
            except Exception, err:
                log.error("An error occured while sending monitoring measurement to InfluxDB: ")
                log.exception(err)

    def handle_aggregation(self, msg):
        """
        This function performs aggregation logic for MPI jobs. It takes measurements from different ranks of an MPI
        job and summaries performance metrics values. As a result it adds a new timeserie - named <dag_job_id>:<condor_job_id> -
        which describes a whole MPI job instead of its internal subprocesses.
        :param msg: a parsed mq message - MonitoringMessage object
        """
        if msg.dag_job_id not in self.aggregators:
            self.aggregators[msg.dag_job_id] = JobAggregator(msg)

        aggregator = self.aggregators[msg.dag_job_id]

        if aggregator.repeated_mpi_rank(msg) and aggregator.past_due_date(msg):
            # 1. check if aggregated measurements are ascending
            aggregator.align_measurements()
            # 2. send the aggregated measurement to a timeseries db and stampede db
            self.write_points(aggregator.trace_id(), aggregator.metrics, aggregator.aggregated_measurements)
            self.emit_measurement_event(aggregator)
            # 3. save this measurement for future comparisons
            aggregator.store_measurements()
            # 4. reset aggregated measurements
            aggregator.reset_measurements()

        aggregator.add(msg)

    def emit_measurement_event(self, aggregator):
        """Sending an event to an event sink (stampede db most probably) about aggregated measurement."""
        if self.event_sink is None:
            return

        kwargs = {}
        kwargs.update(aggregator.message_info())

        for idx, column_name in enumerate(aggregator.metrics):
            attr_value = aggregator.aggregated_measurements[idx]

            if column_name == "time":
                column_name = "ts"

            kwargs[column_name] = attr_value

        event = "job.monitoring"

        try:
            log.info("Sending record to DB: %s --> %s" % (event, kwargs))
            self.event_sink.send(event, kwargs)
        except Exception, e:
            log.error("Error sending event: %s --> %s" % (event, kwargs))
            log.exception(e)

    def close(self):
        self.event_sink.close()

class MonitoringMessage:
    """ Base class for all messages related to online monitoring. """
    def __init__(self, dag_job_id=None, condor_job_id=None, mpi_rank=None, msg=dict()):
        self.dag_job_id = dag_job_id
        self.mpi_rank = mpi_rank
        self.msg = msg
        self.condor_job_id = condor_job_id
        self.trace_id = None
        self.exec_name = 'N/A'
        if "executable" in msg:
            self.exec_name = msg["executable"]

    @staticmethod
    def parse(raw_message):
        """ A factory method. Actual message type is chosen based on the 'event' attribute of the message.
        :param raw_message: A key-value pair string coming from a message broker with a space delimiter after each pair.
        :return: A proper message object or None if 'event' was not identified.
        """
        try:
            message = dict(item.split("=") for item in raw_message.strip().split())

            log.info("Got message: %s", message)

            if "event" not in message:
                log.error("No 'event' attribute in message: %s" % raw_message)
                return None

            if message["event"] == "workflow_trace":
                parsed_msg = WorkflowTraceMessage(message)
            elif message["event"] == "data_transfer":
                parsed_msg = DataTransferMessage(message)
            else:
                log.error("Unknown event type '%s' in message: %s", (message["event"], raw_message))
                return None

            if parsed_msg.trace_id is None:
                log.warning("trace_id not set for message: %s" % raw_message)
                return None

            return parsed_msg
        except ValueError as val_error:
            log.error("Wrong format for message: %s" % raw_message)
            log.exception(val_error)
            return None

    # XXX Not used?
    #def aggregated_trace_id(self):
    #    return "%s:%s" % (self.dag_job_id, self.condor_job_id)

    def metrics(self):
        """ Selects fields from a message which indicates performance metrics and a timestamp."""
        return self.perf_metrics

    def measurements(self):
        """ Gives a single data point, which describes measurement represented by this message."""
        point = []

        for column in self.metrics():
            if column == "time":
                column = "ts"
                point.append(int(self.msg[column]))
            else:
                if column not in self.msg:
                    point.append(float(0))
                else:
                    point.append(float(self.msg[column]))

        return point

    def aggregated_message_info(self, exec_name):
        info = dict()

        for attr in ["wf_uuid", "dag_job_id", "hostname", "condor_job_id", "kickstart_pid"]:
            if attr not in self.msg:
                continue

            attr_value = self.msg[attr]
            column_name = attr
            if attr == "condor_job_id":
                column_name = "sched_id"

            info[column_name] = attr_value

        info["exec_name"] = exec_name

        return info

class WorkflowTraceMessage(MonitoringMessage):
    """ Describes a monitoring message sent from kickstart (libinterpose) about application performance. """
    def __init__(self, message):
        self.perf_metrics = ["time", "utime", "stime", "iowait", "vmSize", "vmRSS", "threads", "read_bytes",
                             "write_bytes", "syscr", "syscw" ] + self.supported_papi_counters().values()

        if self.message_has_required_params(message):
            MonitoringMessage.__init__(self, message["dag_job_id"], message["condor_job_id"], message["mpi_rank"],
                                       message)

            # <dag_job_id>:<hostname>:<condor_jobid>:<mpi_rank>:<kickstart_pid>:<executable>
            self.trace_id = "{0}:{1}:{2}:{3}:{4}:{5}".format(message["dag_job_id"], message["hostname"],
                                                             message["condor_job_id"], message["mpi_rank"],
                                                             message["kickstart_pid"], message["executable"])
            # optional hardware counters info from PAPI
            self.parse_papi_counters(message)

        else:
            MonitoringMessage.__init__(self)
            log.error("Unable to create trace_id")

    def message_has_required_params(self, message):
        required_params = ("ts", "hostname", "executable", "dag_job_id", "condor_job_id", "mpi_rank", "kickstart_pid")
        return all(k in message for k in required_params)

    def parse_papi_counters(self, message):
        """ Changes names of fields related to hardware counters in an incoming message. """
        for key, value in self.supported_papi_counters().iteritems():
            if key in message:
                message[value] = message[key]

    def supported_papi_counters(self):
        return {"PAPI_TOT_INS": "totins",
                "PAPI_LD_INS": "ldins",
                "PAPI_SR_INS": "srins",
                "PAPI_FP_OPS": "fpops",
                "PAPI_FP_INS": "fpins",
                "PAPI_L3_TCM": "l3misses",
                "PAPI_L2_TCM": "l2misses",
                "PAPI_L1_TCM": "l1misses"}

class DataTransferMessage(MonitoringMessage):
    def __init__(self, message):
        self.perf_metrics = ["time", "transfer_duration", "bytes_transferred"]

        if self.message_has_required_params(message):
            MonitoringMessage.__init__(self, message["dag_job_id"], message["condor_job_id"], 0, message)
            self.trace_id = "{0}:{1}:{2}".format(message["dag_job_id"], message["hostname"], message["condor_job_id"])
        else:
            MonitoringMessage.__init__(self)
            print "We couldn't create trace_id"

    def message_has_required_params(self, message):
        required_params = ("ts", "hostname", "dag_job_id", "condor_job_id")
        return all(k in message for k in required_params)


class JobAggregator:

    def __init__(self, msg):
        self.metrics = copy.copy(msg.metrics())

        self.aggregated_measurements = [0] * len(self.metrics)
        self.last_aggregated_data = [0] * len(self.metrics)
        self.retrieved_ranks = dict()

        self.exec_name = msg.exec_name
        self.dag_job_id = msg.dag_job_id
        self.condor_job_id = msg.condor_job_id
        self.msg = msg

    def add(self, msg):
        """add metric values from this mpi rank to create an aggregated measurement for a job"""

        # first we check if we have these metrics (besides the first one which is timestamp)
        if not set(msg.metrics()[1:]).issubset(set(self.metrics)):
            print "Index error in the aggregation logic: ", msg.dag_job_id
            print "This aggregator supports the following metrics:", self.metrics
            print "But we got the following list:", msg.metrics()

            self.metrics += msg.metrics()[1:]
            self.aggregated_measurements += [0] * len(msg.metrics()[1:])
            self.last_aggregated_data += [0] * len(msg.metrics()[1:])

        # update the executable name only if this is mpi rank greater than 0
        if int(msg.mpi_rank) > 0:
            self.exec_name = msg.exec_name

        for i, metric_value in enumerate(msg.measurements()):
            metric = msg.metrics()[i]
            idx = self.metrics.index(metric)

            if metric == 'time':
                if self.aggregated_measurements[idx] < metric_value:
                    self.aggregated_measurements[idx] = metric_value
            else:
                if isinstance(msg, WorkflowTraceMessage):
                    self.aggregated_measurements[idx] += metric_value
                elif isinstance(msg, DataTransferMessage):
                    self.aggregated_measurements[idx] += metric_value + self.last_aggregated_data[idx]

        if "kickstart_pid" not in msg.msg:
            msg.msg["kickstart_pid"] = ""
        if "executable" not in msg.msg:
            msg.msg["executable"] = ""

        rank_id = "{0}:{1}:{2}".format(msg.mpi_rank, msg.msg["kickstart_pid"], msg.msg["executable"])

        self.retrieved_ranks[rank_id] = True

    def repeated_mpi_rank(self, msg):
        if "kickstart_pid" not in msg.msg:
            msg.msg["kickstart_pid"] = ""
        if "executable" not in msg.msg:
            msg.msg["executable"] = ""

        rank_id = "{0}:{1}:{2}".format(msg.mpi_rank, msg.msg["kickstart_pid"], msg.msg["executable"])
        return rank_id in self.retrieved_ranks

    def past_due_date(self, msg):
        idx = self.metrics.index("time")
        idx_2 = msg.metrics().index("time")

        return msg.measurements()[idx_2] - self.aggregated_measurements[idx] > 0

    def reset_measurements(self):
        self.retrieved_ranks = dict()
        self.aggregated_measurements = [0] * len(self.metrics)

    def align_measurements(self):
        for i, metric_value in enumerate(self.last_aggregated_data):
            if self.aggregated_measurements[i] < metric_value:
                self.aggregated_measurements[i] = metric_value

    def store_measurements(self):
        for i, metric_value in enumerate(self.aggregated_measurements):
            self.last_aggregated_data[i] = metric_value

    def trace_id(self):
        return "%s:%s" % (self.dag_job_id, self.condor_job_id)

    def message_info(self):
        info = dict()

        for attr in ["wf_uuid", "dag_job_id", "hostname", "condor_job_id", "kickstart_pid"]:
            if attr not in self.msg.msg:
                continue

            attr_value = self.msg.msg[attr]
            column_name = attr
            if attr == "condor_job_id":
                column_name = "sched_id"

            info[column_name] = attr_value

        info["exec_name"] = self.exec_name

        return info

