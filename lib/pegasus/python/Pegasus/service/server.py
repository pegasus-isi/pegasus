import os
import logging
import random
from OpenSSL import crypto, SSL

from Pegasus.command import Command
from Pegasus.service import app

log = logging.getLogger(__name__)

def generate_self_signed_certificate(certfile, pkeyfile):
    "If certfile and pkeyfile don't exist, create a self-signed certificate"

    if os.path.isfile(certfile) and os.path.isfile(pkeyfile):
        return

    log.info("Generating self-signed certificate")

    pkey = crypto.PKey()
    pkey.generate_key(crypto.TYPE_RSA, 2048)

    cert = crypto.X509()

    sub = cert.get_subject()
    sub.C = "US"
    sub.ST = "California"
    sub.L = "Marina Del Rey"
    sub.O = "University of Southern California"
    sub.OU = "Information Sciences Institute"
    sub.CN = "Pegasus Service"

    cert.set_version(1)
    cert.set_serial_number(random.randint(0,2**32))
    cert.gmtime_adj_notBefore(0)
    cert.gmtime_adj_notAfter(10*365*24*60*60) # 10 years
    cert.set_issuer(sub)
    cert.set_pubkey(pkey)
    cert.sign(pkey, 'sha1')

    open(certfile, "w").write(crypto.dump_certificate(crypto.FILETYPE_PEM, cert))
    open(pkeyfile, "w").write(crypto.dump_privatekey(crypto.FILETYPE_PEM, pkey))

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
        if cert is None or pkey is None:
            log.warning("SSL is not configured: Using self-signed certificate")
            cert = os.path.expanduser("~/.pegasus/selfcert.pem")
            pkey = os.path.expanduser("~/.pegasus/selfkey.pem")
            generate_self_signed_certificate(cert, pkey)
        ssl_context = (cert, pkey)

        if os.getuid() != 0:
            log.warning("Service not running as root: Will not be able to switch users")

        app.run(host=self.options.host,
                port=self.options.port,
                processes=app.config["MAX_PROCESSES"],
                ssl_context=ssl_context)

        log.info("Exiting")


def main():
    ServerCommand().main()
