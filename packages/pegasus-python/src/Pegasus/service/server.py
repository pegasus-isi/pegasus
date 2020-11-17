import logging
import os
import random

import click
import flask
from OpenSSL import crypto

from Pegasus.service import cache
from Pegasus.service._encoder import PegasusJsonEncoder
from Pegasus.service.base import BooleanConverter
from Pegasus.service.filters import register_jinja2_filters
from Pegasus.service.lifecycle import register_lifecycle_handlers

log = logging.getLogger(__name__)

# Services
services = ["dashboard", "monitoring"]


def generate_self_signed_certificate(certfile, pkeyfile):
    """
    SSL.
    :param certfile:
    :param pkeyfile:
    :return:
    If certfile and pkeyfile don't exist, create a self-signed certificate
    """

    if os.path.isfile(certfile) and os.path.isfile(pkeyfile):
        return

    logging.info("Generating self-signed certificate")

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
    cert.set_serial_number(random.randint(0, 2 ** 32))
    cert.gmtime_adj_notBefore(0)
    cert.gmtime_adj_notAfter(10 * 365 * 24 * 60 * 60)  # 10 years
    cert.set_issuer(sub)
    cert.set_pubkey(pkey)
    cert.sign(pkey, "sha1")

    open(certfile, "wb").write(crypto.dump_certificate(crypto.FILETYPE_PEM, cert))
    open(pkeyfile, "wb").write(crypto.dump_privatekey(crypto.FILETYPE_PEM, pkey))


def run(host="localhost", port=5000, debug=True, verbose=logging.INFO, **kwargs):
    app = create_app(env=os.getenv("FLASK_ENV", "development"))

    if debug:
        app.config.update(DEBUG=True)
        logging.getLogger().setLevel(logging.DEBUG)

    pegasusdir = os.path.expanduser("~/.pegasus")
    if not os.path.isdir(pegasusdir):
        os.makedirs(pegasusdir, mode=0o744)

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

    app.run(
        host=host, port=port, threaded=True, ssl_context=ssl_context,
    )

    log.info("Exiting")


def _load_user_config(app):
    # Load user configuration
    conf = os.path.expanduser("~/.pegasus/service.py")
    if os.path.isfile(conf):
        app.config.from_pyfile(conf)


def create_app(config=None, env="development"):
    """Configure app."""
    # Environment
    os.environ["FLASK_ENV"] = env

    app = flask.Flask(__name__)

    # Flask Configuration
    app.config.from_object("Pegasus.service.defaults")
    # app.config.from_object("Pegasus.service.config.%sConfig" % env.capitalize())
    _load_user_config(app)
    app.config.update(config or {})

    if "PEGASUS_ENV" in os.environ:
        app.config.from_envvar("PEGASUS_ENV")

    # Initialize Extensions
    cache.init_app(app)
    # db.init_app(app)
    # socketio.init_app(app, json=flask.json)

    configure_app(app)

    # Service Configuration
    for service in services:
        config_method = "configure_%s" % service
        if config_method in globals():
            globals()["configure_%s" % service](app)

    return app


def configure_app(app):
    #
    # Flask URL variables support int, float, and path converters.
    # Adding support for a boolean converter.
    #
    app.url_map.converters["boolean"] = BooleanConverter

    #
    # Relax trailing slash requirement
    #
    app.url_map.strict_slashes = False

    # Attach global JSONEncoder
    app.json_encoder = PegasusJsonEncoder

    # Register lifecycle methods
    register_lifecycle_handlers(app)

    # Register Jinja2 Filters
    register_jinja2_filters(app)

    # Error handlers
    ## register_error_handlers(app)
    ...


def configure_dashboard(app):
    from Pegasus.service.dashboard import blueprint

    app.register_blueprint(blueprint)


def configure_monitoring(app):
    from Pegasus.service.monitoring import monitoring

    app.register_blueprint(monitoring, url_prefix="/api/v1/user/<string:username>")


@click.command(name="pegasus-service")
@click.option(
    "--host",
    default="localhost",
    metavar="<hostname>",
    show_default=True,
    help="Hostname",
)
@click.option(
    "-p",
    "--port",
    type=int,
    default=5000,
    metavar="<port-number>",
    show_default=True,
    help="Port no. on which to listen for requests",
)
@click.option(
    "-d/-nd",
    "--debug/--no-debug",
    default=True,
    metavar="<debug-mode>",
    help="Start server in development mode",
)
@click.option(
    "-v",
    "--verbose",
    default=logging.DEBUG,
    count=True,
    metavar="<verbosity>",
    help="Logging verbosity",
)
def main(host: str, port: int, debug: bool, verbose: int):
    """Run the Pegasus Service server."""
    run(host=host, port=port, debug=debug, verbose=verbose)


if __name__ == "__main__":
    main()
