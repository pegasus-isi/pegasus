import os
import logging
from optparse import OptionParser

from Pegasus.service import app, em
from Pegasus.service.command import Command

log = logging.getLogger("server")

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


        # We only start the ensemble manager if we are not debugging
        # or if we are debugging and Werkzeug is restarting. This
        # prevents us from having two ensemble managers running in
        # the debug case.
        WERKZEUG_RUN_MAIN = os.environ.get('WERKZEUG_RUN_MAIN') == 'true'
        DEBUG = app.config.get("DEBUG", False)
        if (not DEBUG) or WERKZEUG_RUN_MAIN:
            # Make sure the environment is OK for the ensemble manager
            try:
                em.check_environment()
            except em.EMException, e:
                log.warning("%s: Ensemble manager disabled" % e.message)
            else:
                mgr = em.EnsembleManager()
                mgr.start()

        app.run(port=app.config["SERVER_PORT"], host=app.config["SERVER_HOST"])

def main():
    ServerCommand().main()

