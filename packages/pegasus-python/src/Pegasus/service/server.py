import logging
import os

import click
import flask
from werkzeug.middleware.dispatcher import DispatcherMiddleware
from werkzeug.serving import run_simple

from Pegasus.service import cache
from Pegasus.service._encoder import PegasusJsonEncoder
from Pegasus.service.base import BooleanConverter
from Pegasus.service.filters import register_jinja2_filters
from Pegasus.service.lifecycle import register_lifecycle_handlers

log = logging.getLogger(__name__)

# Services
services = ["dashboard", "monitoring"]


def run(host="localhost", port=5000, debug=True, verbose=logging.INFO, **kwargs):
    app = create_app(env=os.getenv("FLASK_ENV", "development"))

    if debug:
        app.config.update(DEBUG=True)
        logging.getLogger().setLevel(logging.DEBUG)

    pegasus_service_url_prefix = os.environ.get(
        "PEGASUS_SERVICE_URL_PREFIX", app.config.get("PEGASUS_SERVICE_URL_PREFIX", None)
    )
    if pegasus_service_url_prefix:
        logging.info(
            "Using non-standard URL prefix: {}".format(pegasus_service_url_prefix)
        )
        app.config["APPLICATION_ROOT"] = pegasus_service_url_prefix

    pegasusdir = os.path.expanduser("~/.pegasus")
    if not os.path.isdir(pegasusdir):
        os.makedirs(pegasusdir, mode=0o744)

    cert = app.config.get("CERTIFICATE", None)
    pkey = app.config.get("PRIVATE_KEY", None)
    if cert and pkey:
        ssl_context = (cert, pkey)
    else:
        ssl_context = "adhoc"

    if os.getuid() != 0:
        log.warning("Service not running as root: Will not be able to switch users")

    options = {}
    if app.config.get("MAX_PROCESSES", None):
        options = {
            "threaded": False,
            "processes": app.config["MAX_PROCESSES"],
            "use_reloader": False,
        }
    else:
        options = {
            "threaded": True,
            "use_reloader": True,
        }

    if pegasus_service_url_prefix:
        application = DispatcherMiddleware(
            flask.Flask("dummy_app"), {app.config["APPLICATION_ROOT"]: app}
        )
        run_simple(
            host, port, application, ssl_context=ssl_context, **options,
        )
    else:
        app.run(
            host=host, port=port, ssl_context=ssl_context, **options,
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
