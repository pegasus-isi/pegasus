import pika
from influxdb.influxdb08 import InfluxDBClient
import datetime
import sys
import urlparse
import os
import logging
import ssl
import time

from Pegasus.monitoring import event_output as eo

log = logging.getLogger(__name__)

class OnlineMonitord:
    def __init__(self, wf_label, wf_uuid, dburi, child_conn):
        log.info("PEGASUS_WF_UUID: %s" % wf_uuid)
        log.info("PEGASUS_WF_LABEL: %s" % wf_label)
        log.info("INFLUXDB_URL: %s" % os.getenv("INFLUXDB_URL"))
        log.info("KICKSTART_MON_ENDPOINT_URL: %s" % os.getenv("KICKSTART_MON_ENDPOINT_URL"))
        log.info("KICKSTART_MON_ENDPOINT_CREDENTIALS: %s" % os.getenv("KICKSTART_MON_ENDPOINT_CREDENTIALS"))

        self.wf_label = wf_label
        self.wf_uuid = wf_uuid
        self.event_sink = eo.create_wf_event_sink(dburi)

        self.aggregators = dict()

        self.queue_name = wf_label + ":" + wf_uuid

        self.client = None

        self.child_conn = child_conn

    def start(self):
        self.setup_timeseries_db_conn()
        self.start_consuming_mq_messages()

    def setup_timeseries_db_conn(self):
        if os.getenv("INFLUXDB_URL") is None:
            log.error("There is no 'INFLUXDB_URL' set in your environment")
            return

        url = urlparse.urlparse(os.getenv("INFLUXDB_URL"))

        self.client = InfluxDBClient(host=url.hostname, port=url.port,
                                     username=url.username, password=url.password,
                                     ssl=(url.scheme == "https")
                                     )

        # 2. check if influxdb includes a database for storing online monitoring information for the workflow
        #    and create it if it doesn't exist
        if self.queue_name not in [db_info["name"] for db_info in self.client.get_list_database()]:
            self.client.create_database(self.queue_name)

        self.client.switch_database(self.queue_name)
        self.client.switch_user(url.username, url.password)

    def start_consuming_mq_messages(self):
        rabbit_credentials = os.getenv("KICKSTART_MON_ENDPOINT_CREDENTIALS")
        if rabbit_credentials is None:
            log.error("There is no 'KICKSTART_MON_ENDPOINT_CREDENTIALS' in your environment")
            return

        rabbit_url = os.getenv("KICKSTART_MON_ENDPOINT_URL")
        if rabbit_url is None:
            log.error("There is no 'KICKSTART_MON_ENDPOINT_URL' in your environment")
            return

        username, password = rabbit_credentials.split(":")

        rabbit_url = urlparse.urlparse(rabbit_url)
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
        mq_channel.queue_declare(queue=self.queue_name, durable=True)

        # bind the queue to the monitoring exchange
        mq_channel.queue_bind(queue=self.queue_name, exchange=exchange_name, routing_key=self.wf_uuid)

        message_count = None
        db_is_processing_events = False

        # TODO Make this more efficient by getting rid of the Pipe and using channel.consume()
        while True:
            method_frame, header_frame, body = mq_channel.basic_get(self.queue_name, True)

            if method_frame:
                self.on_message(body)
            else:
                # TODO Remove this junk
                time.sleep(1)

            # print "Monitoring process: checking messages from the main process"
            if self.child_conn.poll():
                msg = self.child_conn.recv()
                print "Monitoring process: we got a message from the main process: '", msg, "'"
                if msg == "WORKFLOW_ENDED":
                    self.child_conn.send("WAIT")

                    # check how many messages we have in a message broker
                    # TODO Do something else here to get count
                    response = mq_channel.queue_declare(self.queue_name, passive=True)
                    print 'The queue has {0} more messages'.format(response.method.message_count)
                    message_count = int(response.method.message_count)

                    # check if there is any event to be processed by database
                    db_is_processing_online_monitoring_msgs = getattr(self.event_sink, "is_processing_online_monitoring_msgs", None)
                    if callable(db_is_processing_online_monitoring_msgs):
                        db_is_processing_events = db_is_processing_online_monitoring_msgs()
                    else:
                        db_is_processing_events = False

            if message_count == 0 and (not db_is_processing_events):
                print 'We can stop the online monitoring thread'
                break

        print 'Monitoring process: sending OK to the main process...'
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

            try:
                message = MonitoringMessage.parse(msg_body)

                if message is not None:
                    if self.client is not None:
                        self.client.write_points(InfluxDbMessageFormatter.format_msg(message))

                    self.handle_aggregation(message)

            except ValueError, val_err:
                log.error("An error occured - (probably when parsing a message): ")
                log.exception(val_err)

            except Exception, err:
                log.error("An error occured while sending monitoring measurement: ")
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
            self.send_aggregated_measurement(aggregator)
            # 3. save this measurement for future comparisons
            aggregator.store_measurements()
            # 4. reset aggregated measurements
            aggregator.reset_measurements()

        aggregator.add(msg)

    def send_aggregated_measurement(self, aggregator):
        if self.client is not None:
            try:
                self.client.write_points(
                    InfluxDbMessageFormatter.format(aggregator.trace_id(),
                                                    aggregator.metrics,
                                                    aggregator.aggregated_measurements
                                                    )
                )
            except Exception, err:
                print "An error occured while sending aggregated monitoring measurement: "
                print err

        if self.event_sink is not None:
            self.emit_measurement_event(aggregator)

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
            print "Sending record to DB %s,%s" % (event, kwargs)
            self.event_sink.send(event, kwargs)
        except:
            print "error sending event: %s --> %s" % (event, kwargs)

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

            if "event" not in message:
                print "There is no 'event' attribute in the message"
                return None

            if message["event"] == "workflow_trace":
                parsed_msg = WorkflowTraceMessage(message)
            elif message["event"] == "data_transfer":
                parsed_msg = DataTransferMessage(message)
            else:
                log.error("Unknown event type: %s", message["event"])
                return None

            if parsed_msg.trace_id is None:
                log.warning("message trace_id is not set: %s" % raw_message)
                return None

            return parsed_msg
        except ValueError as val_error:
            log.error("Wrong message format: %s" % raw_message)
            log.exception(val_error)
            return None

    def aggregated_trace_id(self):
        return "%s:%s" % (self.dag_job_id, self.condor_job_id)

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
            print "We couldn't create trace_id"

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


class InfluxDbMessageFormatter:
    @staticmethod
    def format(trace_id, columns, point):
        return [
            {
                "name": trace_id,
                "columns": columns,
                "points": [point]
            }
        ]

    @staticmethod
    def format_msg(trace_msg):
        return InfluxDbMessageFormatter.format(trace_msg.trace_id, trace_msg.metrics(), trace_msg.measurements())

import copy

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

