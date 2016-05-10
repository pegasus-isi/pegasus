import pika
import urlparse
import os
import ssl
import json
import urllib
import logging

from Pegasus.monitoring import event_output as eo

log = logging.getLogger(__name__)

class AnomalyHandler:
    def __init__(self, wf_label, wf_uuid, dburi):
        self.rabbitmq_url = self.getconf("PEGASUS_ANOMALIES_URL")
        self.wf_label = wf_label
        self.wf_uuid = wf_uuid
        self.queue_name = "anomalies:" + wf_label + ":" + wf_uuid
        self.event_sink = eo.create_wf_event_sink(dburi)

    def getconf(self, name):
        value = os.getenv(name)
        if value is None:
            log.error("'%s' is not set in the environment" % name)
            return None
        return value

    def start(self):
        log.info("Starting anomaly listener...")
        self.start_consuming_mq_messages()

    def start_consuming_mq_messages(self):
        if self.rabbitmq_url is None:
            log.warning("Unable to connect to RabbitMQ")
            return None

        url = urlparse.urlparse(self.rabbitmq_url)
        virtual_host, exchange_name = url.path.split("/")[3:5]
        virtual_host = urllib.unquote(virtual_host) # Replace %2F with /
        credentials = pika.PlainCredentials(url.username, url.password)

        parameters = pika.ConnectionParameters(host=url.hostname,
                                               port=url.port - 10000,
                                               ssl=(url.scheme == "rabbitmqs"),
                                               ssl_options={"cert_reqs": ssl.CERT_NONE},
                                               virtual_host=virtual_host,
                                               credentials=credentials)

        mq_conn = pika.BlockingConnection(parameters)

        mq_channel = mq_conn.channel()

        # create a queue for the observer workflow uuid
        mq_channel.queue_declare(queue=self.queue_name, auto_delete=True, exclusive=True)

        # bind the queue to the monitoring exchange
        mq_channel.queue_bind(queue=self.queue_name, exchange=exchange_name, routing_key=self.wf_uuid)

        mq_channel.basic_consume(self.on_message, self.queue_name, exclusive=True)

        try:
            mq_channel.start_consuming()
        except KeyboardInterrupt:
            mq_channel.stop_consuming()

        mq_conn.close()

    def on_message(self, channel, method_frame, header_frame, body):
        """
        A callback registered in a rabbitmq instance to process messages regarding anomalies for a workflow.
        :param channel: a channel from which the message came
        :param method_frame: message number
        :param header_frame:
        :param body: raw body of the message
        :return:
        """

        log.info("on message: %s", body)

        if len(body.split(" ")) < 2:
            log.warning("The given anomaly line is too short")
            return

        anomaly = AnomalyMessage.parse(body)

        if anomaly is not None and self.event_sink is not None:
            self.emit_anomaly_event(anomaly)
        else:
            log.warning("Either anomaly or event sink is None: %s %s", anomaly, self.event_sink)

        if channel is not None:
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)

    def emit_anomaly_event(self, anomaly):
        """Sending an event to an event sink (stampede db most probably) about an anomaly."""

        kwargs = {}
        kwargs.update(anomaly.message_info())

        event = "job.anomaly_detection"

        log.info("Sending anomaly record to DB: %s,%s", event, kwargs)
        self.event_sink.send(event, kwargs)

    def close(self):
        self.event_sink.close()


class AnomalyMessage:
    def __init__(self, ts, wf_uuid, anomaly_type, message, raw_data):
        self.ts = ts
        self.wf_uuid = wf_uuid
        self.anomaly_type = anomaly_type
        self.message = message
        self.json = raw_data  # this should be a dict object

        if 'metrics' in raw_data:
            self.json['metrics'] = json.loads(raw_data['metrics'])

    @staticmethod
    def required_params():
        return ["ts", "wf_uuid", "dag_job_id", "anomaly_type", "message"]

    @staticmethod
    def parse(raw_message):
        """
        Factory method
        """
        anomaly_message = dict(item.split("=") for item in raw_message.strip().split("|||"))

        if not all(k in anomaly_message for k in AnomalyMessage.required_params()):
            log.warning("We expect anomaly message to include parameters: %s", AnomalyMessage.required_params())
            log.warning("but we got: %s", raw_message)
            return None
        else:
            return AnomalyMessage(anomaly_message["ts"], anomaly_message["wf_uuid"], anomaly_message["anomaly_type"],
                                  anomaly_message["message"], anomaly_message)

    def message_info(self):
        info = {
            'ts': self.ts,
            'wf_uuid': self.wf_uuid,
            'anomaly_type': self.anomaly_type,
            'message': self.message,
            'json': self.json
        }

        if 'dag_job_id' in self.json:
            info['dag_job_id'] = self.json['dag_job_id']

        if 'metrics' in self.json:
            info['metrics'] = self.json['metrics']

        return info
