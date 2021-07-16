import logging
import ssl
import queue
from threading import Thread

import pika

try:
    # Python 3.0 and later
    pass
except ImportError:
    # Fall back to Python 2's urllib
    pass

try:
    # Python 3.0 and later
    from urllib import parse as urlparse
except ImportError:
    # Fall back to Python 2's urllib
    import urlparse


class AMQP:
    EXCH_OPTS = {"exchange_type": "topic", "durable": True, "auto_delete": False}
    DEFAULT_AMQP_VIRTUAL_HOST = "/"

    def __init__(self, amqp_url):
        self.params = {}
        self._conn = None
        self._msg_queue = queue.Queue()
        self._stopping = False

        self._set_logger()

        url = urlparse.urlparse(amqp_url)
        self.params["host"] = url.hostname
        self._set_port(url)
        self._set_exchange_vhost(url)
        self._set_ssl_options(url)
        self._set_connection_timeout(url)
        self._set_heartbeat(url)

        creds = pika.PlainCredentials(url.username, url.password)
        self._params = pika.ConnectionParameters(
            host=self.params["host"],
            port=self.params["port"],
            ssl_options=self.params["SSLOptions"],
            virtual_host=self.params["virtual_host"],
            credentials=creds,
            blocked_connection_timeout=self.params["connection_timeout"],
            heartbeat=self.params["heartbeat"],
        )  # heartbeat: None -> negotiate heartbeat with the AMQP server

        # initialize worker thread in daemon and start it
        self._worker_thread = Thread(target=self._event_publisher, daemon=True)
        self._worker_thread.start()

    def _set_logger(self, debug_flag=False):
        logger = logging.getLogger("AMQP_Handle")
        # log to the console
        console = logging.StreamHandler()

        # default log level - make logger/console match
        logger.setLevel(logging.INFO)
        console.setLevel(logging.INFO)

        ## debug - from command line
        # if debug_flag:
        #    logger.setLevel(logging.DEBUG)
        #    console.setLevel(logging.DEBUG)

        # formatter
        formatter = logging.Formatter("%(asctime)s %(levelname)7s:  %(message)s")
        console.setFormatter(formatter)
        logger.addHandler(console)

        self._logger = logger

    def _set_port(self, url):
        if url.port is None:
            if url.scheme == "amqps":
                self.params["port"] = 5671  # RabbitMQ default TLS
            else:
                self.params["port"] = 5672  # RabbitMQ default
        else:
            self.params["port"] = url.port

    def _set_exchange_vhost(self, url):
        exchange = None
        virtual_host = None
        path_comp = url.path.split("/")
        if path_comp:
            exchange = path_comp.pop()
        if path_comp:
            virtual_host = path_comp.pop()
            if len(virtual_host) == 0:
                virtual_host = DEFAULT_AMQP_VIRTUAL_HOST
        self.params["virtual_host"] = virtual_host
        self.params["exchange"] = exchange

    def _set_ssl_options(self, url):
        SSLOptions = None
        if url.scheme == "amqps":
            context = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
            SSLOptions = pika.SSLOptions(context)

        self.params["SSLOptions"] = SSLOptions

    def _set_connection_timeout(self, url):
        connection_timeout = None
        query_params = urlparse.parse_qs(url.query)
        if "connection_timeout" in query_params:
            connection_timeout = int(query_params["connection_timeout"][0])
        self.params["connection_timeout"] = connection_timeout

    def _set_heartbeat(self, url):
        heartbeat = None
        query_params = urlparse.parse_qs(url.query)
        if "heartbeat" in query_params:
            heartbeat = int(query_params["heartbeat"][0])
        self.params["heartbeat"] = heartbeat

    def send(self, event, data):
        if not self._worker_thread.is_alive():
            raise Exception("AMQP publisher thread is dead. Cannot send amqp events.")
        self._msg_queue.put((event, data))

    def close(self):
        if self._worker_thread.is_alive():
            self._logger.debug("Waiting for queue to emtpy.")
            self._msg_queue.join()  # wait for queue to empty if worker is alive
        self._stopping = True
        self._logger.debug("Waiting for publisher thread to exit.")
        self._worker_thread.join()
        self._logger.debug("Publisher thread exited.")

    def _event_publisher(self):
        event, data = (None, None)
        reconnect_attempts = 0
        while not self._stopping:
            try:
                self._logger.info(
                    "Connecting to host: %s:%s virtual host: %s exchange: %s with user: %s ssl: %s"
                    % (
                        self._params.host,
                        self._params.port,
                        self._params.virtual_host,
                        self.params["exchange"],
                        self._params.credentials.username,
                        not self._params.ssl_options is None,
                    )
                )

                self._conn = pika.BlockingConnection(self._params)
                self._channel = self._conn.channel()
                self._channel.exchange_declare(
                    self.params["exchange"], **self.EXCH_OPTS
                )

                reconnect_attempts = 0

                while not self._stopping:
                    try:
                        # if variables are initialized we haven't sent them yet.
                        # don't retrieve a new event, send the old one
                        if (event is None) and (data is None):
                            event, data = self._msg_queue.get(timeout=5)
                        self._logger.debug("send.start event=%s", event)
                        self._channel.basic_publish(
                            body=data,
                            exchange=self.params["exchange"],
                            routing_key=event,
                        )
                        self._logger.debug("send.end event=%s", event)
                        # reset vars
                        event, data = (None, None)
                        # mark item as processed
                        self._msg_queue.task_done()
                    except queue.Empty:
                        self._conn.process_data_events()  # keep up with the AMQP heartbeats
                        continue

            # Do not recover if connection was closed by broker
            except pika.exceptions.ConnectionClosedByBroker as err:
                self._logger.error(
                    "Connection to %s:%s was closed by Broker - Not Recovering"
                    % (self._params.host, self._params.port)
                )
                self._logger.error(
                    "Broker closed connection with: %s, stopping..." % err
                )
                self._conn = None
                break
            # Do not recover on channel errors
            except pika.exceptions.AMQPChannelError as err:
                self._logger.error(
                    "Channel error at %s:%s - Not Recovering"
                    % (self._params.host, self._params.port)
                )
                self._logger.error("Channel error: %s, stopping..." % err)
                self._conn = None
                break
            # Recover on all other connection errors if reconnect attempts is less than 5
            except pika.exceptions.AMQPConnectionError:
                reconnect_attempts += 1
                if reconnect_attempts > 5:
                    self._logger.info(
                        "Connection to %s:%s was closed - Not Recovering"
                        % (self._params.host, self._params.port)
                    )
                    break
                else:
                    self._logger.info(
                        "Connection to %s:%s was closed - Will try to recover the connection"
                        % (self._params.host, self._params.port)
                    )
                    time.sleep((2 ** reconnect_attempts) * 10)
                    continue

        if not self._conn is None:
            self._logger.debug("connection - close.start")
            self._conn.close()
            self._logger.debug("connection - close.end")
