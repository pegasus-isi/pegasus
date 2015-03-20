import os
import logging

from Pegasus.command import Command
from Pegasus.service import app

log = logging.getLogger(__name__)


class ServerCommand(Command):
    usage = "%prog [options]"
    description = "Start Pegasus Service"

    def __init__(self):
        Command.__init__(self)
        self.parser.add_option("-H", "--host", dest="host", default=app.config["SERVER_HOST"],
                               help="Network interface on which to listen for requests")
        self.parser.add_option("-p", "--port", dest="port", type='int', default=app.config["SERVER_PORT"],
                               help="Request listener port")
        self.parser.add_option("-d", "--debug", action="store_true", dest="debug", default=None,
                               help="Enable debugging")
        self.parser.add_option("-v", "--verbose", action="count", default=0, dest="verbose",
                               help="Increase logging verbosity, repeatable")

    def run(self):
        if self.options.debug:
            app.config.update(DEBUG=True)

        log_level = logging.DEBUG if self.options.debug else self.options.verbose

        if log_level < 0:
            log_level = logging.ERROR
        elif log_level == 0:
            log_level = logging.WARNING
        elif log_level == 1:
            log_level = logging.INFO
        elif log_level > 1:
            log_level = logging.DEBUG

        logging.basicConfig(level=log_level)
        logging.getLogger().setLevel(log_level)

        cert = app.config.get("CERTIFICATE", None)
        pkey = app.config.get("PRIVATE_KEY", None)
        if cert is not None and pkey is not None:
            ssl_context = (cert, pkey)
        else:
            log.warning("SSL is not configured: Using adhoc certificate")
            ssl_context = 'adhoc'

        if os.getuid() != 0:
            log.warning("Service not running as root: Will not be able to switch users")

        app.run(host=self.options.host,
                port=self.options.port,
                processes=app.config["MAX_PROCESSES"],
                ssl_context=ssl_context)

        log.info("Exiting")


def main():
    ServerCommand().main()
