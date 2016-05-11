import sys
import threading
import logging
import json

from Pegasus.tools import amqp

log = logging.getLogger(__name__)

class ManifestListener(threading.Thread):
    def __init__(self, url, sliceid, manifest_exchange='testManifestExchange'):
        threading.Thread.__init__(self)
        self.daemon = True
        self.url = url
        self.manifest_exchange = manifest_exchange
        self.sliceid = sliceid
        self.ready = 0
        self.provisioning = 0
        self.status = None

        # Connect
        self.connection = amqp.connect(self.url)
        self.channel = self.connection.channel()

        # Declare the exchange
        self.channel.exchange_declare(exchange=self.manifest_exchange, type='topic')

        # Create an exclusive queue
        self.manifest_queue = self.channel.queue_declare(exclusive=True).method.queue

        # Bind the queue to the exchange for this slice
        self.manifest_routing_key = "adamant.manifest.%s" % self.sliceid
        self.channel.queue_bind(exchange=self.manifest_exchange,
                                queue=self.manifest_queue,
                                routing_key=self.manifest_routing_key)

    def manifest_message(self, ch, method, properties, body):
        #{"response_sliceStatus":"ready",
        # "response_orcaSliceID":"testSlice",
        # "response_numWorkersReady":2}
        man = json.loads(body)
        self.ready = int(man["response_numWorkersReady"])
        self.provisioning = int(man["response_numWorkersProvisioning"])
        self.status = man["response_sliceStatus"]
        log.info("Slice Manifest: sliceStatus = %s, numWorkersReady = %d", self.status, self.current)

    def slots(self):
        return self.ready + self.provisioning

    def run(self):
        self.channel.basic_consume(self.manifest_message,
                                   queue=self.manifest_queue, no_ack=True)
        self.channel.start_consuming()

class RequestPublisher(object):

    def __init__(self, url, sliceid, wf_uuid, request_queue='testRequestQ'):
        self.daemon = True
        self.lock = threading.Lock()
        self.url = url
        self.sliceid = sliceid
        self.wf_uuid = wf_uuid
        self.request_queue = request_queue

        self.connection = amqp.connect(self.url)
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.request_queue)

    def send_modify_request(self, deadline, deadline_diff, util_max, current, required):
        self.lock.acquire()
        req = {
            "requestType": "modifyCompute",
            "req_sliceID": self.sliceid,
            "req_wfuuid": self.wf_uuid,
            "req_numCurrentRes": current,
            "req_deadline": int(deadline),
            "req_deadlineDiff": int(deadline_diff),
            "req_numResReqToMeetDeadline": required,
            "req_numResUtilMax": util_max
        }
        self.channel.basic_publish(exchange='',
                                   routing_key=self.request_queue,
                                   body=json.dumps(req))
        self.lock.release()

    def send_workflow_finished(self):
        self.lock.acquire()
        req = {
            "requestType": "workflowFinished",
            "req_sliceID": self.sliceid,
            "req_wfuuid": self.wf_uuid,
        }
        self.channel.basic_publish(exchange='',
                                   routing_key=self.request_queue,
                                   body=json.dumps(req))
        self.lock.release()

