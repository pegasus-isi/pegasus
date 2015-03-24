__author__ = "Rafael Ferreira da Silva"

import logging

from Pegasus.command import LoggingCommand, CompoundCommand
from Pegasus.db.admin.admin_loader import *
from Pegasus.db.admin.versions import *

log = logging.getLogger(__name__)

# ------------------------------------------------------
def print_version(data):
    log.info("Your database is compatible with Pegasus version: %s" % data)


def set_log_level(debug):
    log_level = logging.INFO
    if debug:
        log_level = logging.DEBUG

    logging.basicConfig(level=log_level)
    logging.getLogger().setLevel(log_level)


# ------------------------------------------------------
class CreateCommand(LoggingCommand):
    description = "Create Pegasus databases."
    usage = "Usage: %prog create [options] [DATABASE_URL]"
    
    def __init__(self):
        Command.__init__(self)
        self.parser.add_option("-d", "--debug", action="store_true", dest="debug",
                               default=None, help="Enable debugging")
            
    def run(self):
        set_log_level(self.options.debug)
        
        dburi = None
        if len(self.args) > 0:
            dburi = self.args[0]
        
        AdminDB(dburi, create=True)
        log.info("Pegasus databases were successfully created.")
    
    
# ------------------------------------------------------
class DowngradeCommand(LoggingCommand):
    description = "Downgrade the database version."
    usage = "Usage: %prog downgrade [options] [DATABASE_URL]"

    def __init__(self):
        Command.__init__(self)
        self.parser.add_option("-c","--conf",action="store",type="string", 
            dest="config_properties",default=None,
            help = "Specify properties file. This overrides all other property files.")
        self.parser.add_option("-t","--type",action="store",type="string", 
            dest="db_type",default=None, help = "Type of the database.")
        self.parser.add_option("-f","--force",action="store_true",dest="force",
            default=None, help = "Ignore conflicts or data loss.")
        self.parser.add_option("-V","--version",action="store",type="string", 
            dest="pegasus_version",default=None, help = "Pegasus version.")
        self.parser.add_option("-d", "--debug", action="store_true", dest="debug",
                               default=None, help="Enable debugging")

    def run(self):
        set_log_level(self.options.debug)
        
        dburi = None
        if len(self.args) > 0:
            dburi = self.args[0]
            
        try:
            adminDB = AdminDB(self.options.config_properties, self.options.db_type, dburi)
            if not self.options.pegasus_version or not adminDB.verify(self.options.pegasus_version):
                adminDB.downgrade(self.options.pegasus_version, self.options.force)
            
            version = adminDB.current_version(parse=True)
            print_version(version)
                
        except RuntimeError:
            exit(1)


# ------------------------------------------------------
class UpdateCommand(LoggingCommand):
    description = "Update the database to a specific version."
    usage = "Usage: %prog update [options] [DATABASE_URL]"
    
    def __init__(self):
        Command.__init__(self)
        self.parser.add_option("-c","--conf",action="store",type="string", 
            dest="config_properties",default=None,
            help = "Specify properties file. This overrides all other property files.")
        self.parser.add_option("-t","--type",action="store",type="string", 
            dest="db_type",default=None, help = "Type of the database")
        self.parser.add_option("-f","--force",action="store_true",dest="force",
            default=None, help = "Ignore conflicts or data loss")
        self.parser.add_option("-V","--version",action="store",type="string", 
            dest="pegasus_version",default=None, help = "Pegasus version")
        self.parser.add_option("-d", "--debug", action="store_true", dest="debug",
                               default=None, help="Enable debugging")
    
    def run(self):
        set_log_level(self.options.debug)
        
        dburi = None
        if len(self.args) > 0:
            dburi = self.args[0]
            
        try:
            adminDB = AdminDB(self.options.config_properties, self.options.db_type, dburi)
            if not adminDB.verify(self.options.pegasus_version, verbose=True):
                adminDB.update(self.options.pegasus_version, self.options.force)
                
            version = adminDB.current_version(parse=True)
            print_version(version)
            
        except RuntimeError:
            exit(1)
    
    
# ------------------------------------------------------
class CheckCommand(LoggingCommand):
    description = "Verify if the database is updated to the latest or a given version."
    usage = "Usage: %prog check [options] [DATABASE_URL]"

    def __init__(self):
        Command.__init__(self)
        self.parser.add_option("-c","--conf",action="store",type="string", 
            dest="config_properties",default=None,
            help = "Specify properties file. This overrides all other property files.")
        self.parser.add_option("-t","--type",action="store",type="string", 
            dest="db_type",default=None, help = "Type of the database")
        self.parser.add_option("-V","--version",action="store",type="string", 
            dest="pegasus_version",default=None, help = "Pegasus version")
        self.parser.add_option("-e", "--version-value", action="store_false", dest="version_value",
                               default=True, help="Show actual version values")
        self.parser.add_option("-d", "--debug", action="store_true", dest="debug",
                               default=None, help="Enable debugging")
                    
    def run(self):
        set_log_level(self.options.debug)
        
        dburi = None
        if len(self.args) > 0:
            dburi = self.args[0]

        try:
            adminDB = AdminDB(self.options.config_properties, self.options.db_type, dburi)
            adminDB.verify(self.options.pegasus_version, self.options.version_value, verbose=True)
        except RuntimeError:
            exit(1)
    
    
# ------------------------------------------------------
class VersionCommand(LoggingCommand):
    description = "Print the current version of the database."
    usage = "Usage: %prog version [options] [DATABASE_URL]"

    def __init__(self):
        Command.__init__(self)
        self.parser.add_option("-c","--conf",action="store",type="string", 
            dest="config_properties",default=None,
            help = "Specify properties file. This overrides all other property files")
        self.parser.add_option("-t","--type",action="store",type="string", 
            dest="db_type",default=None, help = "Type of the database")
        self.parser.add_option("-e", "--version-value", action="store_false", dest="version_value",
                               default=True, help="Show actual version values.")
        self.parser.add_option("-d", "--debug", action="store_true", dest="debug",
                               default=None, help="Enable debugging")
        
    def run(self):
        set_log_level(self.options.debug)
        
        dburi = None
        if len(self.args) > 0:
            dburi = self.args[0]
        
        try:
            adminDB = AdminDB(self.options.config_properties, self.options.db_type, dburi)
            version = adminDB.current_version(self.options.version_value)
            print_version(version)
        except RuntimeError:
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
