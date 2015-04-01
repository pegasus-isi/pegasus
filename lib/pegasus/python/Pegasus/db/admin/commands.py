__author__ = "Rafael Ferreira da Silva"

import logging
import sys

from Pegasus.command import Command, CompoundCommand
from Pegasus.db import connection
from Pegasus.db.admin.admin_loader import *
from Pegasus.db.admin.versions import *

consoleHandler = logging.StreamHandler(sys.stdout)
consoleHandler.setLevel(logging.INFO)
errorHandler = logging.StreamHandler(sys.stderr)
errorHandler.setLevel(logging.ERROR)
logging.getLogger().addHandler(consoleHandler)
logging.getLogger().addHandler(errorHandler)

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
            _validate_conf_type_options(self.options.config_properties, self.options.db_type)
            dburi = db_get_uri(self.options.config_properties, self.options.db_type, dburi)
            connection.connect(dburi, create=True)
            log.info("Pegasus databases were successfully created.")
            
        except RuntimeError:
            exit(1)
    
    
# ------------------------------------------------------
class DowngradeCommand(Command):
    description = "Downgrade the database version."
    usage = "Usage: %prog downgrade [options] [DATABASE_URL]"

    def __init__(self):
        Command.__init__(self)
        _add_common_options(self)
        self.parser.add_option("-f","--force",action="store_true",dest="force",
            default=None, help = "Ignore conflicts or data loss.")
        self.parser.add_option("-V","--version",action="store",type="string", 
            dest="pegasus_version",default=None, help = "Pegasus version.")

    def run(self):
        _set_log_level(self.options.debug)
        
        dburi = None
        if len(self.args) > 0:
            dburi = self.args[0]
            
        try:
            _validate_conf_type_options(self.options.config_properties, self.options.db_type)
            dburi = db_get_uri(self.options.config_properties, self.options.db_type, dburi)
            db = connection.connect(dburi)
            if not self.options.pegasus_version or not db_verify(db, self.options.pegasus_version):
                db_downgrade(db, self.options.pegasus_version, self.options.force)
            
            version = db_current_version(db, parse=True)
            _print_version(version)
                
        except RuntimeError:
            exit(1)


# ------------------------------------------------------
class UpdateCommand(Command):
    description = "Update the database to a specific version."
    usage = "Usage: %prog update [options] [DATABASE_URL]"
    
    def __init__(self):
        Command.__init__(self)
        _add_common_options(self)
        self.parser.add_option("-f","--force",action="store_true",dest="force",
            default=None, help = "Ignore conflicts or data loss")
        self.parser.add_option("-V","--version",action="store",type="string", 
            dest="pegasus_version",default=None, help = "Pegasus version")
    
    def run(self):
        _set_log_level(self.options.debug)
        
        dburi = None
        if len(self.args) > 0:
            dburi = self.args[0]
            
        try:
            _validate_conf_type_options(self.options.config_properties, self.options.db_type)
            dburi = db_get_uri(self.options.config_properties, self.options.db_type, dburi)
            db = connection.connect(dburi)
            if not db_verify(db, self.options.pegasus_version):
                db_update(db, self.options.pegasus_version, self.options.force)
                
            version = db_current_version(db, parse=True)
            _print_version(version)
            
        except RuntimeError:
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
            _validate_conf_type_options(self.options.config_properties, self.options.db_type)
            dburi = db_get_uri(self.options.config_properties, self.options.db_type, dburi)
            db = connection.connect(dburi)
            db_verify(db, self.options.pegasus_version, self.options.version_value, verbose=True)
        except RuntimeError:
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
            _validate_conf_type_options(self.options.config_properties, self.options.db_type)
            dburi = db_get_uri(self.options.config_properties, self.options.db_type, dburi)
            db = connection.connect(dburi)
            version = db_current_version(db, self.options.version_value)
            _print_version(version)
        except RuntimeError:
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


def _validate_conf_type_options(config_properties, type):
    if config_properties and not type:
        log.error("A type should be provided with the property file.")
        raise RuntimeError("A type should be provided with the property file.")
    if not config_properties and type:
        log.error("A property file should be provided with the type option.")
        raise RuntimeError("A property file should be provided with the type option.")


def _add_common_options(object):

    object.parser.add_option("-c","--conf",action="store",type="string", 
        dest="config_properties",default=None,
        help = "Specify properties file. This overrides all other property files. Should be used with '-t'")
    object.parser.add_option("-t","--type",action="store",type="string", 
        dest="db_type",default=None, help = "Type of the database. Should be used with '-c'")
    object.parser.add_option("-d", "--debug", action="store_true", dest="debug",
        default=None, help="Enable debugging")


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
