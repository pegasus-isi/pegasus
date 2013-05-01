import logging
from optparse import OptionParser

from pegasus.service import app, config

def main():
    parser = OptionParser()

    config.add_options(parser)

    parser.add_option("-H", "--host", action="store", dest="host",
            default="127.0.0.1", help="Server host (default: 127.0.0.1)")
    parser.add_option("-p", "--port", action="store", dest="port",
            default=5000, type="int", help="Server port number (default: %default)")

    (options, args) = parser.parse_args()

    config.set_options(opts)

    logging.basicConfig(level=logging.INFO)

    app.run(port=options.port, host=options.host)

