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
    PERF_METRICS = ["time", "utime", "stime", "iowait", "vmSize", "vmRSS", "threads", "read_bytes", "write_bytes",
                    "syscr", "syscw"]

    def __init__(self, wf_label, wf_uuid, dburi):
        print "[online-monitord] PEGASUS_WF_UUID: %s" % wf_uuid
        print "[online-monitord] PEGASUS_WF_LABEL: %s" % wf_label
        print "[online-monitord] INFLUXDB_URL: %s" % os.getenv("INFLUXDB_URL")
        print "[online-monitord] KICKSTART_MON_ENDPOINT_URL: %s" % os.getenv("KICKSTART_MON_ENDPOINT_URL")
        print "[online-monitord] KICKSTART_MON_ENDPOINT_CREDENTIALS: %s" % os.getenv("KICKSTART_MON_ENDPOINT_CREDENTIALS")

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
        A callback registered in a rabbitmq instance to process messages regarding online monitoring for a particalur
        workflow.
        :param channel: a channel from which the message came
        :param method_frame: message number
        :param header_frame:
        :param body: raw body of the message
        :return:
        """
        # print method_frame.delivery_tag
        # print body
        # print

        if len(body.split(" ")) < 2:
            print "The given measurement line is too short"
            return

        message = dict(item.split("=") for item in body.split(" "))

        measurement = self.parse_measurement(message)

        if measurement is not None:
            if self.client is not None:
                self.client.write_points(measurement)

            perf_metrics = measurement[0]["points"][0]
            self.handle_aggregation(perf_metrics, message)

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

    def handle_aggregation(self, measurement, msg):
        """
        This function performs aggregation logic for MPI jobs. It takes measurements from different ranks of an MPI
        job and summaries performance metrics values. As a result it adds a new timeserie - named <dag_job_id> -
        which describes a whole MPI job instead of its internal subprocesses.
        :param measurement: a list of performance metrics values
        :param msg: a parsed mq message - dict
        """
        dag_job_id = msg["dag_job_id"]
        mpi_rank = int(msg["mpi_rank"])

        if dag_job_id not in self.retrieved_messages:
            self.retrieved_messages[dag_job_id] = dict()
            self.aggregated_measurements[dag_job_id] = [0] * len(OnlineMonitord.PERF_METRICS)
            self.last_aggregated_data[dag_job_id] = [0] * len(OnlineMonitord.PERF_METRICS)

        if mpi_rank in self.retrieved_messages[dag_job_id]:
            # 1. check if new measurement has only metrics values greater than the previous ones
            for i, metric_value in enumerate(self.last_aggregated_data[dag_job_id]):
                if self.aggregated_measurements[dag_job_id][i] < metric_value:
                    self.aggregated_measurements[dag_job_id][i] = metric_value

            # 2. send the aggregated measurement to a timeseries db and stampede db
            self.send_aggregated_measurement(msg, self.aggregated_measurements[dag_job_id])

            # 3. save this measurement for future comparisons
            for i, metric_value in enumerate(self.aggregated_measurements[dag_job_id]):
                self.last_aggregated_data[dag_job_id][i] = metric_value

            # 4. reset aggregated measurements
            self.retrieved_messages[dag_job_id] = dict()
            self.aggregated_measurements[dag_job_id] = [0] * len(OnlineMonitord.PERF_METRICS)

        self.retrieved_messages[dag_job_id][mpi_rank] = True
        self.aggregate_measurement(dag_job_id, measurement)

    def aggregate_measurement(self, dag_job_id, measurement):
        "add metric values from this mpi rank to create an aggregated measurement for a job"
        # print "We are adding measurements", measurement, "for", dag_job_id
        for i, metric_value in enumerate(measurement):
            # timestamp is set to the latest value
            if i == 0:
                if self.aggregated_measurements[dag_job_id][i] < metric_value:
                    self.aggregated_measurements[dag_job_id][i] = metric_value
            # other metrics are aggregated
            else:
                self.aggregated_measurements[dag_job_id][i] += metric_value

    def send_aggregated_measurement(self, message, measurement):
        if self.client is not None:
            json_body = [
                {
                    "name": message["dag_job_id"],
                    "columns": OnlineMonitord.PERF_METRICS,
                    "points": [measurement]
                }
            ]
            self.client.write_points(json_body)

        if self.event_sink is not None:
            self.emit_measurement_event(measurement, message)

    def parse_measurement(self, msg):
        """
        This function transforms a single message body taken from rabbitmq with online monitoring information.
        :param measurement_text: it is a string with a simple <key>=<value> format with spaces between subsequent pairs.
        :return: a JSON object in a format supported by InfluxDB
        """
        if not all(k in msg for k in ("hostname", "executable", "kickstart_pid", "mpi_rank")):
            print "We couldn't create trace_id"
            return None
        else:
            exec_id = msg["executable"]  # .split("/")[-1]
            # <dag_job_id>:<hostname>:<condor_jobid>:<mpi_rank>:<kickstart_pid>:<executable>
            trace_id = "{0}:{1}:{2}:{3}:{4}:{5}".format(msg["dag_job_id"], msg["hostname"],
                                                        msg["condor_job_id"], msg["mpi_rank"], msg["kickstart_pid"],
                                                        exec_id)

            point = []
            for column in OnlineMonitord.PERF_METRICS:
                if column == "time":
                    column = "ts"
                    point.append(int(msg[column]))
                else:
                    point.append(float(msg[column]))

            json_body = [
                {
                    "name": trace_id,
                    "columns": OnlineMonitord.PERF_METRICS,
                    "points": [point]
                    # "timestamp": datetime.datetime.fromtimestamp( float(msg["ts"]) ).isoformat('T')
                }
            ]

            return json_body

    def emit_measurement_event(self, metrics_values, parsed_msg):
        """
        Sending an event to an event sink (stampede db most probably) about aggregated measurement.
        :param metrics_values: a list of performance metrics values
        :param parsed_msg: a parsed mq message
        """
        if self.event_sink is None:
            return

        kwargs = {}

        for attr in ["wf_uuid", "dag_job_id", "hostname", "condor_job_id", "kickstart_pid", "executable"]:
            attr_value = parsed_msg[attr]
            column_name = attr
            if attr == "condor_job_id":
                column_name = "sched_id"
            elif attr == "executable":
                column_name = "exec_name"

            kwargs[column_name] = attr_value

        # for metric in enumerate(OnlineMonitord.PERF_METRICS):
        for i in range(len(OnlineMonitord.PERF_METRICS)):
            attr_value = metrics_values[i]
            column_name = OnlineMonitord.PERF_METRICS[i]
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
