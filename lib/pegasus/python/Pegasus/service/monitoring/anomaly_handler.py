import pika
import urlparse
import os
import ssl

from Pegasus.monitoring import event_output as eo


class AnomalyHandler:
    def __init__(self, wf_label, wf_uuid, dburi):
        print "[anomaly-handler] PEGASUS_WF_UUID: %s" % wf_uuid
        print "[anomaly-handler] PEGASUS_WF_LABEL: %s" % wf_label
        print "[anomaly-handler] rabbitmq endpoint: %s" % os.getenv("PEGASUS_ANOMALIES_ENDPOINT_URL")
        print "[anomaly-handler] rabbitmq credentials: %s" % os.getenv("PEGASUS_ANOMALIES_ENDPOINT_CREDENTIALS")

        self.wf_label = wf_label
        self.wf_uuid = wf_uuid
        self.event_sink = eo.create_wf_event_sink(dburi)

        self.mq_conn = None
        self.channel = None
        self.queue_name = "anomalies:" + wf_label + ":" + wf_uuid

    def setup_mq_conn(self):
        self.mq_conn = self.initialize_mq_connection()

        if self.mq_conn is not None:
            self.channel = self.prepare_channel()

    def start_consuming_mq_messages(self):
        if self.channel is None:
            return

        self.channel.basic_consume(self.on_message, self.queue_name, exclusive=True)

        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            self.channel.stop_consuming()

        self.mq_conn.close()

    def on_message(self, channel, method_frame, header_frame, body):
        """
        A callback registered in a rabbitmq instance to process messages regarding anomalies for a workflow.
        :param channel: a channel from which the message came
        :param method_frame: message number
        :param header_frame:
        :param body: raw body of the message
        :return:
        """

        print "[anomaly-handler] on message: ", body

        if len(body.split(" ")) < 2:
            print "The given measurement line is too short"
            return

        anomaly = AnomalyMessage.parse(body)

        if anomaly is not None and self.event_sink is not None:
            print "[anomaly-handler] We are sending an anomaly:", anomaly
            self.emit_anomaly_event(anomaly)
        else:
            print "[anomaly-handler] Either anomaly or event sink is None", anomaly, self.event_sink

        if channel is not None:
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)

    def emit_anomaly_event(self, anomaly):
        """Sending an event to an event sink (stampede db most probably) about an anomaly."""

        # kwargs = {
        #     'time': anomaly.ts,
        #     ''
        # }
        # kwargs.update(aggregator.message_info())
        #
        # for idx, column_name in enumerate(aggregator.metrics):
        #     attr_value = aggregator.aggregated_measurements[idx]
        #
        #     if column_name == "time":
        #         column_name = "ts"
        #
        #     kwargs[column_name] = attr_value

        # event = "job.anomaly_detection"
        #
        # try:
        #     print "Sending anomaly record to DB %s,%s" % (event, kwargs)
        #     self.event_sink.send(event, kwargs)
        # except:
        #     print "error sending event: %s --> %s" % (event, kwargs)

    def close(self):
        self.event_sink.close()

    def initialize_mq_connection(self):
        """
        Utility function to create a connection to rabbitmq using information about rabbitmq endpoint and credentials
         stored in environment variables.
        :return: a set up connection for further use
        """
        mq_credentials = os.getenv("PEGASUS_ANOMALIES_ENDPOINT_CREDENTIALS")
        if mq_credentials is None:
            print "[anomaly-handler] There is no 'PEGASUS_ANOMALIES_ENDPOINT_CREDENTIALS' in your environment"
            return None

        username, password = mq_credentials.split(":")

        credentials = pika.PlainCredentials(username, password)

        if os.getenv("PEGASUS_ANOMALIES_ENDPOINT_URL") is None:
            print "[anomaly-handler] There is no 'PEGASUS_ANOMALIES_ENDPOINT_URL' in your environment"
            return None

        rabbit_url = urlparse.urlparse(os.getenv("PEGASUS_ANOMALIES_ENDPOINT_URL"))

        parameters = pika.ConnectionParameters(host=rabbit_url.hostname,
                                               port=rabbit_url.port - 10000,
                                               ssl=(rabbit_url.scheme == "https"),
                                               ssl_options={"cert_reqs": ssl.CERT_NONE},
                                               virtual_host=rabbit_url.path.split("/")[3],
                                               credentials=credentials)

        return pika.BlockingConnection(parameters)

    def prepare_channel(self):
        """
        A utility function that registers a queue in rabbitmq for the monitored workflow, and binds a "well-known"
        exchange with the queue.

        :param mq_conn:
        :param queue_name:
        :param wf_uuid:
        :return:
        """
        channel = self.mq_conn.channel()

        exchange_name = urlparse.urlparse(os.getenv("PEGASUS_ANOMALIES_ENDPOINT_URL")).path.split("/")[4]

        # create a queue for the observer workflow uuid
        channel.queue_declare(
            queue=self.queue_name,
            durable=True
        )

        # bind the queue to the monitoring exchange
        channel.queue_bind(
            queue=self.queue_name,
            exchange=exchange_name,
            routing_key=self.wf_uuid
        )

        return channel


class AnomalyMessage:
    def __init__(self, ts, wf_uuid, dag_job_id, anomaly_type, message, raw_data):
        self.ts = ts
        self.wf_uuid = wf_uuid
        self.dag_job_id = dag_job_id
        self.anomaly_type = anomaly_type
        self.message = message
        self.raw_data = raw_data  # this should be a dict object

    @staticmethod
    def parse(raw_message):
        """
        Factory method
        """
        required_params = ("ts", "wf_uuid", "dag_job_id", "type", "message")

        anomaly_message = dict(item.split("=") for item in raw_message.strip().split("|"))

        if not all(k in anomaly_message for k in required_params):
            print "[anomaly-handler] We expect anomaly message to include parameters:", required_params
            print "[anomaly-handler] but we got:", raw_message
            return None
        else:
            return AnomalyMessage(anomaly_message["ts"], anomaly_message["wf_uuid"], anomaly_message["dag_job_id"],
                                  anomaly_message["type"], anomaly_message["message"], anomaly_message)

            # def metrics(self):
            #     return self.perf_metrics
            #
            # def measurements(self):
            #     point = []
            #
            #     for column in self.metrics():
            #         if column == "time":
            #             column = "ts"
            #             point.append(int(self.msg[column]))
            #         else:
            #             if column not in self.msg:
            #                 point.append(float(0))
            #             else:
            #                 point.append(float(self.msg[column]))
            #
            #     return point
            #
            # def aggregated_message_info(self, exec_name):
            #     info = dict()
            #
            #     for attr in ["wf_uuid", "dag_job_id", "hostname", "condor_job_id", "kickstart_pid"]:
            #         if attr not in self.msg:
            #             continue
            #
            #         attr_value = self.msg[attr]
            #         column_name = attr
            #         if attr == "condor_job_id":
            #             column_name = "sched_id"
            #
            #         info[column_name] = attr_value
            #
            #     info["exec_name"] = exec_name
            #
            #     return info
