import logging

from Pegasus.command import CompoundCommand, LoggingCommand
from Pegasus.db import connection
from Pegasus.db.admin.admin_loader import *
from Pegasus.db.admin.versions import *

__author__ = "Rafael Ferreira da Silva"


log = logging.getLogger(__name__)


# ------------------------------------------------------
class CreateCommand(LoggingCommand):
    description = "Create Pegasus database."
    usage = "Usage: %prog create [options] [DATABASE_URL]"

    def __init__(self):
        LoggingCommand.__init__(self)
        _add_common_options(self)

    def run(self):
        _set_log_level(self.options.debug)

        dburi = None
        if len(self.args) > 0:
            dburi = self.args[0]

        try:
            _validate_conf_type_options(
                dburi,
                self.options.properties,
                self.options.config_properties,
                self.options.submit_dir,
                self.options.db_type,
            )
            db = _get_connection(
                dburi,
                self.options.properties,
                self.options.config_properties,
                self.options.submit_dir,
                self.options.db_type,
                create=True,
                force=self.options.force,
            )
            db.close()

        except (DBAdminError, connection.ConnectionError) as e:
            log.error(e)
            exit(1)


# ------------------------------------------------------
class UpdateCommand(LoggingCommand):
    description = "Update the database to the latest or a given version."
    usage = "Usage: %prog update [options] [DATABASE_URL]"

    def __init__(self):
        LoggingCommand.__init__(self)
        _add_common_options(self)
        self.parser.add_option(
            "-V",
            "--version",
            action="store",
            type="string",
            dest="pegasus_version",
            default=None,
            help="Pegasus version",
        )
        self.parser.add_option(
            "-a",
            "--all",
            action="store_true",
            dest="all",
            default=False,
            help="Update all databases of completed workflows in MASTER.",
        )

    def run(self):
        _set_log_level(self.options.debug)

        dburi = None
        if len(self.args) > 0:
            dburi = self.args[0]

        try:
            _validate_conf_type_options(
                dburi,
                self.options.properties,
                self.options.config_properties,
                self.options.submit_dir,
                self.options.db_type,
            )
            db = _get_connection(
                dburi,
                self.options.properties,
                self.options.config_properties,
                self.options.submit_dir,
                self.options.db_type,
                pegasus_version=self.options.pegasus_version,
                create=True,
                force=self.options.force,
            )

            if self.options.all:
                all_workflows_db(
                    db,
                    pegasus_version=self.options.pegasus_version,
                    force=self.options.force,
                )

            db.close()

        except (DBAdminError, connection.ConnectionError) as e:
            log.error(e)
            exit(1)


# ------------------------------------------------------
class DowngradeCommand(LoggingCommand):
    description = "Downgrade the database version."
    usage = "Usage: %prog downgrade [options] [DATABASE_URL]"

    def __init__(self):
        LoggingCommand.__init__(self)
        _add_common_options(self)
        self.parser.add_option(
            "-V",
            "--version",
            action="store",
            type="string",
            dest="pegasus_version",
            default=None,
            help="Pegasus version.",
        )
        self.parser.add_option(
            "-a",
            "--all",
            action="store_true",
            dest="all",
            default=False,
            help="Downgrade all databases of completed workflows in MASTER.",
        )

    def run(self):
        _set_log_level(self.options.debug)

        dburi = None
        if len(self.args) > 0:
            dburi = self.args[0]

        try:
            _validate_conf_type_options(
                dburi,
                self.options.properties,
                self.options.config_properties,
                self.options.submit_dir,
                self.options.db_type,
            )
            db = _get_connection(
                dburi,
                self.options.properties,
                self.options.config_properties,
                self.options.submit_dir,
                self.options.db_type,
                pegasus_version=self.options.pegasus_version,
                schema_check=False,
                force=self.options.force,
            )
            db_downgrade(db, self.options.pegasus_version, self.options.force)
            db_verify(db)

            if self.options.all:
                all_workflows_db(
                    db,
                    update=False,
                    pegasus_version=self.options.pegasus_version,
                    schema_check=False,
                    force=self.options.force,
                )

            db.close()

        except (DBAdminError, connection.ConnectionError) as e:
            log.error(e)
            exit(1)


# ------------------------------------------------------
class CheckCommand(LoggingCommand):
    description = "Verify if the database is updated to the latest or a given version."
    usage = "Usage: %prog check [options] [DATABASE_URL]"

    def __init__(self):
        LoggingCommand.__init__(self)
        _add_common_options(self)
        self.parser.add_option(
            "-V",
            "--version",
            action="store",
            type="string",
            dest="pegasus_version",
            default=None,
            help="Pegasus version",
        )
        self.parser.add_option(
            "-e",
            "--version-value",
            action="store_false",
            dest="version_value",
            default=True,
            help="Show actual version values",
        )

    def run(self):
        _set_log_level(self.options.debug)

        dburi = None
        if len(self.args) > 0:
            dburi = self.args[0]

        try:
            _validate_conf_type_options(
                dburi,
                self.options.properties,
                self.options.config_properties,
                self.options.submit_dir,
                self.options.db_type,
            )
            db = _get_connection(
                dburi,
                self.options.properties,
                self.options.config_properties,
                self.options.submit_dir,
                self.options.db_type,
                pegasus_version=self.options.pegasus_version,
                force=self.options.force,
            )
            db.close()

        except (DBAdminError, connection.ConnectionError) as e:
            log.error(e)
            exit(1)


# ------------------------------------------------------
class VersionCommand(LoggingCommand):
    description = "Print the current version of the database."
    usage = "Usage: %prog version [options] [DATABASE_URL]"

    def __init__(self):
        LoggingCommand.__init__(self)
        _add_common_options(self)

    def run(self):
        _set_log_level(self.options.debug)

        dburi = None
        if len(self.args) > 0:
            dburi = self.args[0]

        try:
            _validate_conf_type_options(
                dburi,
                self.options.properties,
                self.options.config_properties,
                self.options.submit_dir,
                self.options.db_type,
            )
            db = _get_connection(
                dburi,
                self.options.properties,
                self.options.config_properties,
                self.options.submit_dir,
                self.options.db_type,
                schema_check=False,
                force=self.options.force,
            )
            db_verify(db)
            db.close()

        except (DBAdminError, connection.ConnectionError) as e:
            log.error(e)
            exit(1)


# ------------------------------------------------------
def _set_log_level(debug):
    if debug:
        logging.getLogger().setLevel(logging.DEBUG)


def _validate_conf_type_options(
    dburi, properties, config_properties, submit_dir, db_type
):
    """Validate DB type parameter
    :param dburi: database URI
    :param config_properties: Pegasus configuration properties file
    :param submit_dir: workflow submit directory
    :param db_type: database type (workflow, master, or jdbcrc)
    """
    if dburi:
        # command-line URI has the highest priority
        return

    if (config_properties or submit_dir) and not db_type:
        log.error("A type should be provided with the property file/submit directory.")
        exit(1)

    if (
        not config_properties
        and not submit_dir
        and not _has_connection_properties(properties)
    ) and db_type:
        log.error(
            "A property file/submit directory should be provided with the type option."
        )
        exit(1)


def _add_common_options(object):
    """ Add command line common options """
    object.parser.add_option(
        "-c",
        "--conf",
        action="store",
        type="string",
        dest="config_properties",
        default=None,
        help="Specify properties file. This overrides all other property files. Should be used with '-t'",
    )
    object.parser.add_option(
        "-s",
        "--submitdir",
        action="store",
        type="string",
        dest="submit_dir",
        default=None,
        help="Specify submit directory. Should be used with '-t'",
    )
    object.parser.add_option(
        "-t",
        "--type",
        action="store",
        type="string",
        dest="db_type",
        default=None,
        help="Type of the database (JDBCRC, MASTER, or WORKFLOW). Should be used with '-c' or '-s'",
    )
    object.parser.add_option(
        "-D",
        "",
        action="append",
        type="string",
        dest="properties",
        default=[],
        help="Commandline overwrite for properties. Must be in the 'prop=val' format",
    )
    object.parser.add_option(
        "-d",
        "--debug",
        action="store_true",
        dest="debug",
        default=None,
        help="Enable debugging",
    )
    object.parser.add_option(
        "-f",
        "--force",
        action="store_true",
        dest="force",
        default=None,
        help="Ignore conflicts or data loss.",
    )


def _get_connection(
    dburi=None,
    cl_properties=None,
    config_properties=None,
    submit_dir=None,
    db_type=None,
    pegasus_version=None,
    schema_check=True,
    create=False,
    force=False,
    print_version=True,
):
    """ Get connection to the database based on the parameters"""
    if dburi:
        return connection.connect(
            dburi,
            pegasus_version=pegasus_version,
            schema_check=schema_check,
            create=create,
            force=force,
            db_type=db_type,
            print_version=print_version,
        )
    elif submit_dir:
        return connection.connect_by_submitdir(
            submit_dir,
            db_type,
            config_properties,
            pegasus_version=pegasus_version,
            schema_check=schema_check,
            create=create,
            force=force,
            cl_properties=cl_properties,
            print_version=print_version,
        )

    elif config_properties or _has_connection_properties(cl_properties):
        return connection.connect_by_properties(
            config_properties,
            db_type,
            cl_properties=cl_properties,
            pegasus_version=pegasus_version,
            schema_check=schema_check,
            create=create,
            force=force,
            print_version=print_version,
        )

    if not db_type:
        dburi = connection._get_master_uri()
        return connection.connect(
            dburi,
            pegasus_version=pegasus_version,
            schema_check=schema_check,
            create=create,
            force=force,
            db_type=db_type,
            print_version=print_version,
        )
    return None


def _has_connection_properties(cl_properties):
    """
    Verify if provided command-line properties contains connection properties.
    :param cl_properties: command-line properties
    """
    for property in cl_properties:
        key = property.split("=")[0]
        if key in connection.CONNECTION_PROPERTIES:
            return True
    return False


# ------------------------------------------------------
class DBAdminCommand(CompoundCommand):
    commands = [
        ("create", CreateCommand),
        ("downgrade", DowngradeCommand),
        ("update", UpdateCommand),
        ("check", CheckCommand),
        ("version", VersionCommand),
    ]
    aliases = {
        "c": "create",
        "d": "downgrade",
        "u": "update",
        "k": "check",
        "v": "version",
    }
    epilog = """The pegasus-db-admin tool should always be followed by a COMMAND listed
        below. To see the available options for each command, please use the -h option
        after the command. For example: pegasus-db-admin update -h"""


def main():
    "The entry point for pegasus-db-admin"
    DBAdminCommand().main()
