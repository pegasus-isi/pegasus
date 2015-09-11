import pika
from influxdb.influxdb08 import InfluxDBClient
import datetime
import sys
import urlparse
import os
import logging
import ssl

from Pegasus.monitoring import event_output as eo


class OnlineMonitord:
    def __init__(self, wf_label, wf_uuid, dburi):
        print "[online-monitord] PEGASUS_WF_UUID: %s" % wf_uuid
        print "[online-monitord] PEGASUS_WF_LABEL: %s" % wf_label
        print "[online-monitord] INFLUXDB_URL: %s" % os.getenv("INFLUXDB_URL")
        print "[online-monitord] KICKSTART_MON_ENDPOINT_URL: %s" % os.getenv("KICKSTART_MON_ENDPOINT_URL")
        print "[online-monitord] KICKSTART_MON_ENDPOINT_CREDENTIALS: %s" % os.getenv(
            "KICKSTART_MON_ENDPOINT_CREDENTIALS")

        self.wf_label = wf_label
        self.wf_uuid = wf_uuid
        self.event_sink = eo.create_wf_event_sink(dburi)

        self.aggregators = dict()

        self.mq_conn = None
        self.channel = None
        self.queue_name = wf_label + ":" + wf_uuid

        self.client = None

    def setup_mq_conn(self):
        self.mq_conn = self.initialize_mq_connection()

        if self.mq_conn is not None:
            self.channel = self.prepare_channel(self.mq_conn, self.queue_name, self.wf_uuid)

    def setup_timeseries_db_conn(self):
        if os.getenv("INFLUXDB_URL") is None:
            print "There is no 'INFLUXDB_URL' set in your environment"
            return

        url = urlparse.urlparse(os.getenv("INFLUXDB_URL"))

        self.client = InfluxDBClient(host=url.hostname, port=url.port,
                                     username=url.username, password=url.password,
                                     ssl=(url.scheme == "https")
                                     )

        # 2. check if influxdb includes a database for storing online monitoring information for the workflow
        #    and creates it if it doesn't exist
        # print "Database list: ", client.get_list_database()
        if self.queue_name not in [db_info["name"] for db_info in self.client.get_list_database()]:
            self.client.create_database(self.queue_name)

        self.client.switch_database(self.queue_name)
        self.client.switch_user(url.username, url.password)

    def start_consuming_mq_messages(self):
        if self.channel is None:
            return

        for method_frame, properties, body in self.channel.consume(self.queue_name):
            if method_frame is not None:
                print method_frame.delivery_tag
            # print body
            # print

            self.channel.basic_ack(delivery_tag=method_frame.delivery_tag)

            self.on_message(body)

        self.mq_conn.close()

    def on_message(self, body):
        """
        An utility function for processing messages regarding online monitoring for a particular workflow.
        :param body: raw body of the message
        """

        if len(body.split(" ")) < 2 or len(body) < 4:
            print "The given measurement line is too short"
        else:
            messages = body.split(":delim1:")
            if len(messages) > 1:
                messages = messages[0:-1]

            for msg_body in messages:
                try:
                    message = MonitoringMessage.parse(msg_body)

                    if message is not None:
                        if self.client is not None:
                            self.client.write_points(InfluxDbMessageFormatter.format_msg(message))

                        self.handle_aggregation(message)

                except ValueError, val_err:
                    print "An error occured - (probably when parsing a message): "
                    print val_err

                except Exception, err:
                    print "An error occured while sending monitoring measurement: "
                    print err

    def initialize_mq_connection(self):
        """
        Utility function to create a connection to rabbitmq using information about rabbitmq endpoint and credentials
         stored in environment variables.
        :return: a set up connection for further use
        """
        rabbit_credentials = os.getenv("KICKSTART_MON_ENDPOINT_CREDENTIALS")
        if rabbit_credentials is None:
            print "There is no 'KICKSTART_MON_ENDPOINT_CREDENTIALS' in your environment"
            return None

        username, password = rabbit_credentials.split(":")

        credentials = pika.PlainCredentials(username, password)

        if os.getenv("KICKSTART_MON_ENDPOINT_URL") is None:
            print "There is no 'KICKSTART_MON_ENDPOINT_URL' in your environment"
            return None

        rabbit_url = urlparse.urlparse(os.getenv("KICKSTART_MON_ENDPOINT_URL"))

        parameters = pika.ConnectionParameters(host=rabbit_url.hostname,
                                               port=rabbit_url.port - 10000,
                                               ssl=(rabbit_url.scheme == "https"),
                                               ssl_options={"cert_reqs": ssl.CERT_NONE},
                                               virtual_host=rabbit_url.path.split("/")[3],
                                               credentials=credentials)

        return pika.BlockingConnection(parameters)

    def prepare_channel(self, mq_conn, queue_name, wf_uuid):
        """
        A utility function that registers a queue in rabbitmq for the monitored workflow, and binds a "well-known"
        exchange with the queue.

        :param mq_conn:
        :param queue_name:
        :param wf_uuid:
        :return:
        """
        channel = mq_conn.channel()

        exchange_name = urlparse.urlparse(os.getenv("KICKSTART_MON_ENDPOINT_URL")).path.split("/")[4]
        # import pdb; pdb.set_trace()

        # create a queue for the observer workflow uuid
        channel.queue_declare(
            queue=queue_name,
            durable=True            
        )

        # bind the queue to the monitoring exchange
        channel.queue_bind(
            queue=queue_name,
            exchange=exchange_name,
            routing_key=wf_uuid
        )

        return channel

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

            parsed_msg = None

            if message["event"] == "workflow_trace":
                parsed_msg = WorkflowTraceMessage(message)

            elif message["event"] == "data_transfer":
                parsed_msg = DataTransferMessage(message)

            if parsed_msg.trace_id is None:
                return None

            return parsed_msg
        except ValueError as val_error:
            print "[online-monitord] Wrong message format (", raw_message, ") : ", val_error
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
