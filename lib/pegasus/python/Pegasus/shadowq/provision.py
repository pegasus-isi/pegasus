import logging
import time
import threading

log = logging.getLogger(__name__)

class Provisioner(threading.Thread):
    def __init__(self, dag, interval):
        threading.Thread.__init__(self)
        self.setDaemon(True)
        self.dag = dag
        self.interval = interval

    def run(self):
        while True:
            log.info("Provisioning resources...")

            # TODO Run some experiments to see which resource configuration is best

            newdag = self.dag.clone()
            for name in newdag.jobs:
                job = newdag.jobs[name]
                log.info(job)

            # TODO Notify the resource manager how many resources we need

            time.sleep(self.interval)

