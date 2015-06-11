from multiprocessing import Process
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

        self.aggregated_measurements = dict()
        self.last_aggregated_data = dict()
        self.retrieved_messages = dict()

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

        self.channel.basic_consume(self.on_message, self.queue_name)

        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            self.channel.stop_consuming()

        self.mq_conn.close()

    def on_message(self, channel, method_frame, header_frame, body):
        """
        A callback registered in a rabbitmq instance to process messages regarding online monitoring for a particular
        workflow.
        :param channel: a channel from which the message came
        :param method_frame: message number
        :param header_frame:
        :param body: raw body of the message
        :return:
        """
        # if method_frame is not None:
        #     print method_frame.delivery_tag
        # print body
        # print

        if len(body.split(" ")) < 2:
            print "The given measurement line is too short"
            return

        message = MonitoringMessage.parse(body)

        if message is not None:
            if self.client is not None:
                try:
                    self.client.write_points(message.to_influxdb_json())
                except Exception, err:
                    print "An error occured while sending monitoring measurement: "
                    print err

            self.handle_aggregation(message)

        if channel is not None:
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)

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
        dag_job_id = msg.dag_job_id
        mpi_rank = msg.mpi_rank

        # print "Msg metrics: ", msg.metrics()
        # print "Msg measurements: ", msg.measurements()

        if dag_job_id not in self.retrieved_messages:
            self.retrieved_messages[dag_job_id] = dict()
            self.aggregated_measurements[dag_job_id] = [0] * len(msg.measurements())
            self.last_aggregated_data[dag_job_id] = [0] * len(msg.measurements())

        if mpi_rank in self.retrieved_messages[dag_job_id]:
            # 1. check if aggregated measurements are ascending
            for i, metric_value in enumerate(self.last_aggregated_data[dag_job_id]):
                if self.aggregated_measurements[dag_job_id][i] < metric_value:
                    self.aggregated_measurements[dag_job_id][i] = metric_value

            # 2. send the aggregated measurement to a timeseries db and stampede db
            self.send_aggregated_measurement(self.aggregated_measurements[dag_job_id], msg)

            # 3. save this measurement for future comparisons
            for i, metric_value in enumerate(self.aggregated_measurements[dag_job_id]):
                self.last_aggregated_data[dag_job_id][i] = metric_value

            # 4. reset aggregated measurements
            self.retrieved_messages[dag_job_id] = dict()
            self.aggregated_measurements[dag_job_id] = [0] * len(msg.measurements())

        self.retrieved_messages[dag_job_id][mpi_rank] = True
        self.aggregate_measurement(dag_job_id, msg)

    # TODO not sure if this is fully correct
    def aggregate_measurement(self, dag_job_id, msg):
        "add metric values from this mpi rank to create an aggregated measurement for a job"
        # print "We are adding measurements", measurement, "for", dag_job_id
        if isinstance(msg, WorkflowTraceMessage):
            for i, metric_value in enumerate(msg.measurements()):
                # timestamp is set to the latest value
                if i == 0:
                    if self.aggregated_measurements[dag_job_id][i] < metric_value:
                        self.aggregated_measurements[dag_job_id][i] = metric_value
                    # other metrics are aggregated
                else:
                    self.aggregated_measurements[dag_job_id][i] += metric_value

        elif isinstance(msg, DataTransferMessage):
            for i, metric_value in enumerate(msg.measurements()):
                # timestamp is set to the latest value
                if i == 0:
                    if self.aggregated_measurements[dag_job_id][i] < metric_value:
                        self.aggregated_measurements[dag_job_id][i] = metric_value
                else:
                    self.aggregated_measurements[dag_job_id][i] += metric_value + self.last_aggregated_data[dag_job_id][i]


    def send_aggregated_measurement(self, aggregated_measurements, message):
        if self.client is not None:
            try:
                self.client.write_points(
                    MonitoringMessage.get_influxdb_json(message.aggregated_trace_id(),
                        message.metrics(),
                        aggregated_measurements
                    )
                )
            except Exception, err:
                print "An error occured while sending aggregated monitoring measurement: "
                print err

        if self.event_sink is not None:
            self.emit_measurement_event(aggregated_measurements, message)

    def emit_measurement_event(self, metrics_values, parsed_msg):
        """
        Sending an event to an event sink (stampede db most probably) about aggregated measurement.
        :param metrics_values: a list of performance metrics values
        :param parsed_msg: a parsed mq message
        """
        if self.event_sink is None:
            return

        kwargs = {}
        kwargs.update(parsed_msg.aggregated_message_info())

        metrics_names = parsed_msg.metrics()
        for i in range(len(metrics_names)):
            attr_value = metrics_values[i]
            column_name = metrics_names[i]
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
    def __init__(self):
        self.trace_id = None

    def __init__(self, dag_job_id, condor_job_id, mpi_rank, msg):
        self.dag_job_id = dag_job_id
        self.mpi_rank = mpi_rank
        self.msg = msg
        self.condor_job_id = condor_job_id
        self.trace_id = None

    @staticmethod
    def parse(raw_message):
        message = dict(item.split("=") for item in raw_message.strip().split(" "))

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

    def aggregated_trace_id(self):
        return "%s:%s" % (self.dag_job_id, self.condor_job_id)

    def metrics(self):
        return self.perf_metrics

    @staticmethod
    def get_influxdb_json(trace_id, columns, point):
        return [
            {
                "name": trace_id,
                "columns": columns,
                "points": [point]
            }
        ]

    def to_influxdb_json(self):
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

        return MonitoringMessage.get_influxdb_json(self.trace_id, self.metrics(), point)

    def measurements(self):
        return self.to_influxdb_json()[0]["points"][0]

    def aggregated_message_info(self):
        info = dict()

        for attr in ["wf_uuid", "dag_job_id", "hostname", "condor_job_id", "kickstart_pid", "executable"]:
            if attr not in self.msg:
                continue

            attr_value = self.msg[attr]
            column_name = attr
            if attr == "condor_job_id":
                column_name = "sched_id"
            elif attr == "executable":
                column_name = "exec_name"

            info[column_name] = attr_value

        return info



class WorkflowTraceMessage(MonitoringMessage):
    def __init__(self, message):
        self.perf_metrics = ["time", "utime", "stime", "iowait", "vmSize", "vmRSS", "threads", "read_bytes",
                             "write_bytes", "syscr", "syscw"]

        if self.message_has_required_params(message):
            MonitoringMessage.__init__(self, message["dag_job_id"], message["condor_job_id"], message["mpi_rank"],
                                       message)

            # <dag_job_id>:<hostname>:<condor_jobid>:<mpi_rank>:<kickstart_pid>:<executable>
            self.trace_id = "{0}:{1}:{2}:{3}:{4}:{5}".format(message["dag_job_id"], message["hostname"],
                                                             message["condor_job_id"], message["mpi_rank"],
                                                             message["kickstart_pid"], message["executable"])
        else:
            MonitoringMessage.__init__(self)
            print "We couldn't create trace_id"

    def message_has_required_params(self, message):
        required_params = ("hostname", "executable", "dag_job_id", "condor_job_id", "mpi_rank", "kickstart_pid")
        return all(k in message for k in required_params)


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
        required_params = ("hostname", "dag_job_id", "condor_job_id")
        return all(k in message for k in required_params)
