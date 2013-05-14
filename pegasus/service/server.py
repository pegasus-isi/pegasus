import logging
from optparse import OptionParser

from pegasus.service import app, config

def main():
    parser = OptionParser()

    parser.add_option("--config", action="store", dest="config",
            default=None, help="Path to configuration file")
    parser.add_option("--dburi", action="store", dest="dburi",
            default=None, help="SQLAlchemy database URI")
    parser.add_option("-d", "--debug", action="store_true", dest="debug",
            default=None, help="Enable debugging")
    parser.add_option("-H", "--host", action="store", dest="host",
            default="127.0.0.1", help="Server host (default: 127.0.0.1)")
    parser.add_option("-p", "--port", action="store", dest="port",
            default=5000, type="int", help="Server port number (default: %default)")

    (options, args) = parser.parse_args()

    if options.config:
        config.load_config(options.config)

    if options.dburi:
        config.set_dburi(options.dburi)

    if options.debug:
        config.set_debug(True)

    logging.basicConfig(level=logging.INFO)

    app.run(port=options.port, host=options.host)

