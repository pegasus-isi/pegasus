import logging
from optparse import OptionParser

from pegasus.service import app
from pegasus.service.command import Command

class ServerCommand(Command):
    usage = "%prog [options]"
    description = "Start Pegasus Service"

    def __init__(self):
        Command.__init__(self)
        self.parser.add_option("-d", "--debug", action="store_true", dest="debug",
                default=None, help="Enable debugging")

    def run(self):
        if self.options.debug:
            app.config.update(DEBUG=True)

        logging.basicConfig(level=logging.INFO)

        app.run(port=app.config["SERVER_PORT"], host=app.config["SERVER_HOST"])

def main():
    ServerCommand().main()

