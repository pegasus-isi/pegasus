__author__ = "Rafael Ferreira da Silva"

import logging
import sys

from Pegasus.command import Command, CompoundCommand
from Pegasus.db import connection
from Pegasus.db.admin.admin_loader import *
from Pegasus.db.admin.versions import *

class ConsoleHandler(logging.StreamHandler):
    """A handler that logs to console in the sensible way.

    StreamHandler can log to *one of* sys.stdout or sys.stderr.

    It is more sensible to log to sys.stdout by default with only error
    (logging.ERROR and above) messages going to sys.stderr. This is how
    ConsoleHandler behaves.
    """

    def __init__(self):
        logging.StreamHandler.__init__(self)
        self.stream = None # reset it; we are not going to use it anyway

    def emit(self, record):
        if record.levelno >= logging.ERROR:
            self.__emit(record, sys.stderr)
        else:
            self.__emit(record, sys.stdout)

    def __emit(self, record, strm):
        self.stream = strm
        logging.StreamHandler.emit(self, record)

    def flush(self):
        # Workaround a bug in logging module
        # See:
        #   http://bugs.python.org/issue6333
        if self.stream and hasattr(self.stream, 'flush') and not self.stream.closed:
            logging.StreamHandler.flush(self)


consoleHandler = ConsoleHandler()
logging.getLogger().addHandler(consoleHandler)

log = logging.getLogger(__name__)

# ------------------------------------------------------
class CreateCommand(Command):
    description = "Create Pegasus databases."
    usage = "Usage: %prog create [options] [DATABASE_URL]"
    
    def __init__(self):
        Command.__init__(self)
        _add_common_options(self)
                              
    def run(self):
        _set_log_level(self.options.debug)
        
        dburi = None
        if len(self.args) > 0:
            dburi = self.args[0]
        
        try:
            _validate_conf_type_options(self.options.config_properties, self.options.submit_dir, self.options.db_type)
            db = _get_connection(dburi, self.options.config_properties, self.options.submit_dir, self.options.db_type, create=True, force=self.options.force)
            db.close()
            
        except DBAdminError, e:
            log.error(e)
            exit(1)
        except connection.ConnectionError, e:
            log.error(e)
            exit(1)
    
    
# ------------------------------------------------------
class DowngradeCommand(Command):
    description = "Downgrade the database version."
    usage = "Usage: %prog downgrade [options] [DATABASE_URL]"

    def __init__(self):
        Command.__init__(self)
        _add_common_options(self)
        self.parser.add_option("-V","--version",action="store",type="string", 
            dest="pegasus_version",default=None, help = "Pegasus version.")

    def run(self):
        _set_log_level(self.options.debug)
        
        dburi = None
        if len(self.args) > 0:
            dburi = self.args[0]
            
        try:
            _validate_conf_type_options(self.options.config_properties, self.options.submit_dir, self.options.db_type)
            db = _get_connection(dburi, self.options.config_properties, self.options.submit_dir, self.options.db_type, force=self.options.force)
            db_downgrade(db, self.options.pegasus_version, self.options.force)
            version = db_current_version(db, parse=True)
            _print_version(version)
            db.close()
                
        except DBAdminError, e:
            log.error(e)
            exit(1)
        except connection.ConnectionError, e:
            log.error(e)
            exit(1)


# ------------------------------------------------------
class UpdateCommand(Command):
    description = "Update the database to a specific version."
    usage = "Usage: %prog update [options] [DATABASE_URL]"
    
    def __init__(self):
        Command.__init__(self)
        _add_common_options(self)
        self.parser.add_option("-V","--version",action="store",type="string", 
            dest="pegasus_version",default=None, help = "Pegasus version")
    
    def run(self):
        _set_log_level(self.options.debug)
        
        dburi = None
        if len(self.args) > 0:
            dburi = self.args[0]
            
        try:
            _validate_conf_type_options(self.options.config_properties, self.options.submit_dir, self.options.db_type)
            db = _get_connection(dburi, self.options.config_properties, self.options.submit_dir, self.options.db_type, force=self.options.force)
            db_update(db, self.options.pegasus_version, force=self.options.force)
            version = db_current_version(db, parse=True)
            _print_version(version)
            db.close()
            
        except DBAdminError, e:
            if "Non-existent or missing database tables" in str(e):
                try:
                    db = _get_connection(dburi, self.options.config_properties, self.options.submit_dir, self.options.db_type, create=True, force=self.options.force)
                except DBAdminError, e:
                    log.error(e)
                    exit(1)
            else:
                log.error(e)
                exit(1)
        except connection.ConnectionError, e:
            log.error(e)
            exit(1)
  
    
# ------------------------------------------------------
class CheckCommand(Command):
    description = "Verify if the database is updated to the latest or a given version."
    usage = "Usage: %prog check [options] [DATABASE_URL]"

    def __init__(self):
        Command.__init__(self)
        _add_common_options(self)
        self.parser.add_option("-V","--version",action="store",type="string", 
            dest="pegasus_version",default=None, help = "Pegasus version")
        self.parser.add_option("-e", "--version-value", action="store_false", dest="version_value",
                               default=True, help="Show actual version values")
                    
    def run(self):
        _set_log_level(self.options.debug)
        
        dburi = None
        if len(self.args) > 0:
            dburi = self.args[0]

        try:
            _validate_conf_type_options(self.options.config_properties, self.options.submit_dir, self.options.db_type)
            db = _get_connection(dburi, self.options.config_properties, self.options.submit_dir, self.options.db_type, force=self.options.force)
            compatible = db_verify(db, self.options.pegasus_version)
            _print_db_check(db, compatible, self.options.pegasus_version, self.options.version_value)
            db.close()

        except DBAdminError, e:
            log.error(e)
            exit(1)
        except connection.ConnectionError, e:
            log.error(e)
            exit(1)
   
    
# ------------------------------------------------------
class VersionCommand(Command):
    description = "Print the current version of the database."
    usage = "Usage: %prog version [options] [DATABASE_URL]"

    def __init__(self):
        Command.__init__(self)
        _add_common_options(self)
        self.parser.add_option("-e", "--version-value", action="store_false", dest="version_value",
                               default=True, help="Show actual version values.")
        
    def run(self):
        _set_log_level(self.options.debug)
        
        dburi = None
        if len(self.args) > 0:
            dburi = self.args[0]
        
        try:
            _validate_conf_type_options(self.options.config_properties, self.options.submit_dir, self.options.db_type)
            db = _get_connection(dburi, self.options.config_properties, self.options.submit_dir, self.options.db_type, force=self.options.force)
            version = db_current_version(db, self.options.version_value)
            _print_version(version)
            db.close()

        except DBAdminError, e:
            log.error(e)
            exit(1)
        except connection.ConnectionError, e:
            log.error(e)
            exit(1)


# ------------------------------------------------------
def _print_version(data):
    log.info("Your database is compatible with Pegasus version: %s" % data)


def _set_log_level(debug):
    log_level = logging.INFO
    if debug:
        log_level = logging.DEBUG

    logging.getLogger().setLevel(log_level)
    consoleHandler.setLevel(log_level)


def _validate_conf_type_options(config_properties, submit_dir, db_type):
    """ Validate DB type parameter """
    if (config_properties or submit_dir) and not db_type:
        log.error("A type should be provided with the property file/submit directory.")
        exit(1)
    
    if (not config_properties and not submit_dir) and db_type:
        log.error("A property file/submit directory should be provided with the type option.")
        exit(1)


def _add_common_options(object):
    """ Add command line common options """
    object.parser.add_option("-c","--conf",action="store",type="string", 
        dest="config_properties",default=None,
        help = "Specify properties file. This overrides all other property files. Should be used with '-t'")
    object.parser.add_option("-s","--submitdir",action="store",type="string", 
        dest="submit_dir",default=None, help = "Specify submit directory. Should be used with '-t'")
    object.parser.add_option("-t","--type",action="store",type="string", 
        dest="db_type",default=None, help = "Type of the database. Should be used with '-c'")
    object.parser.add_option("-d", "--debug", action="store_true", dest="debug",
        default=None, help="Enable debugging")
    object.parser.add_option("-f","--force",action="store_true",dest="force",
            default=None, help = "Ignore conflicts or data loss.")



def _get_connection(dburi=None, config_properties=None, submit_dir=None, db_type=None, create=False, force=False):
    """ Get connection to the database based on the parameters"""
    if dburi:
        return connection.connect(dburi, create=create, force=force)
    elif config_properties:
        return connection.connect_by_properties(config_properties, db_type, create=create, force=force)
    elif submit_dir:
        return connection.connect_by_submitdir(submit_dir, db_type, config_properties, create=create, force=force)
    
    if not db_type:
        dburi = connection._get_master_uri()
        return connection.connect(dburi, create=create, force=force)
    return None


def _print_db_check(db, compatible, pegasus_version=None, parse=False):
    """ Print result for db_verify """
    version = parse_pegasus_version(pegasus_version)
    friendly_version = version
    if parse:
        if pegasus_version:
            friendly_version = pegasus_version
        else:
            friendly_version = get_compatible_version(version)

    if compatible:
        log.info("Your database is compatible with version %s." % friendly_version)
    else:
        log.error("Your database is NOT compatible with version %s." % friendly_version)
        current_version = db_current_version(db)
        command = "update"
        if current_version > version:
            command = "downgrade"
        if version == CURRENT_DB_VERSION:
            log.error("Use 'pegasus-db-admin %s %s' to %s your database." % (command, db.get_bind().url, command))
        else:
            log.error("Use 'pegasus-db-admin %s %s -V %s' to %s your database." % (command, db.get_bind().url, friendly_version, command))
        exit(1)


# ------------------------------------------------------
class DBAdminCommand(CompoundCommand):
    description = "Database administrator client"
    commands = [
        ("create", CreateCommand),
        ('downgrade', DowngradeCommand),
        ('update', UpdateCommand),
        ('check', CheckCommand),
        ('version', VersionCommand)
    ]
    aliases = {
        "c": "create",
        "d": "downgrade",
        "u": "update",
        "k": "check",
        "v": "version"
    }


def main():
    "The entry point for pegasus-db-admin"
    DBAdminCommand().main()
