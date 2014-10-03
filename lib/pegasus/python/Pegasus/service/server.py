import os
import logging
from optparse import OptionParser

from Pegasus.service import app
from Pegasus.service.command import Command
from Pegasus.service.ensembles import manager

log = logging.getLogger(__name__)

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
                manager.check_environment()
            except manager.EMException, e:
                log.warning("%s: Ensemble manager disabled" % e.message)
            else:
                mgr = manager.EnsembleManager()
                mgrpid = mgr.start()

        cert = app.config.get("CERTIFICATE", None)
        pkey = app.config.get("PRIVATE_KEY", None)
        if cert is not None and pkey is not None:
            ssl_context = (cert, pkey)
        else:
            log.warning("SSL is not configured: passwords will not be encrypted")
            ssl_context = None

        if os.getuid() != 0:
            log.warning("Service not running as root: Will not be able to switch users")

        app.run(port=app.config["SERVER_PORT"],
                host=app.config["SERVER_HOST"],
                processes=app.config["MAX_PROCESSES"],
                ssl_context=ssl_context)

        os.waitpid(mgrpid, 0)

        log.info("Exiting")

def main():
    ServerCommand().main()

