__author__ = "Rafael Ferreira da Silva"

import sys
import logging

from Pegasus.command import Command, CompoundCommand
from Pegasus.db.admin.admin_loader import *
from Pegasus.db.admin.versions import *

log = logging.getLogger(__name__)

# ------------------------------------------------------
def print_versions(data):
    sys.stdout.write("Database compatibilities:\n")
    for key in data:
        sys.stdout.write("    %s\t%s\n" % (data[key], key))


def set_log_level(debug):
    if debug:
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO

    logging.basicConfig(level=log_level)
    logging.getLogger().setLevel(log_level)


# ------------------------------------------------------
class CreateCommand(Command):
    description = "Create Pegasus databases."
    usage = "Usage: %prog create [options] [SUBMITDIR]"
    
    def __init__(self):
        Command.__init__(self)
        self.parser.add_option("-c","--conf",action="store",type="string", 
            dest="config_properties",default=None,
            help = "Specify properties file. This overrides all other property files.")
        self.parser.add_option("-u","--url",action="store",type="string", 
            dest="database_url",default=None,
            help = "Database URL.")
        self.parser.add_option("-d", "--debug", action="store_true", dest="debug",
                               default=None, help="Enable debugging")
            
    def run(self):
        set_log_level(self.options.debug)
        
        submit_dir = None
        if len(self.args) > 0:
            submit_dir = self.args[0]
        
        AdminDB(self.options.config_properties, self.options.database_url, submit_dir)
        sys.stdout.write("Pegasus databases were successfully created.\n")
    
    
# ------------------------------------------------------
class DowngradeCommand(Command):
    description = "Downgrade the database version."
    usage = "Usage: %prog downgrade [options] [SUBMITDIR]"

    def __init__(self):
        Command.__init__(self)
        self.parser.add_option("-c","--conf",action="store",type="string", 
            dest="config_properties",default=None,
            help = "Specify properties file. This overrides all other property files.")
        self.parser.add_option("-u","--url",action="store",type="string", 
            dest="database_url",default=None, help = "Database URL.")
        self.parser.add_option("-f","--force",action="store_true",dest="force",
            default=None, help = "Ignore conflicts or data loss.")
        self.parser.add_option("-V","--version",action="store",type="string", 
            dest="pegasus_version",default=None, help = "Pegasus version.")
        self.parser.add_option("-d", "--debug", action="store_true", dest="debug",
                               default=None, help="Enable debugging")

    def run(self):
        set_log_level(self.options.debug)
        
        submit_dir = None
        if len(self.args) > 0:
            submit_dir = self.args[0]
            
        adminDB = AdminDB(self.options.config_properties, self.options.database_url, submit_dir)
        
        if not self.options.pegasus_version or not adminDB.verify(self.options.pegasus_version):
            adminDB.downgrade(self.options.pegasus_version, self.options.force)
        
        data = adminDB.current_version(parse=True, print_friendly=True)
        print_versions(data)
    
    
# ------------------------------------------------------
class UpdateCommand(Command):
    description = "Update the database to a specific version."
    usage = "Usage: %prog update [options] [SUBMITDIR]"
    
    def __init__(self):
        Command.__init__(self)
        self.parser.add_option("-c","--conf",action="store",type="string", 
            dest="config_properties",default=None,
            help = "Specify properties file. This overrides all other property files.")
        self.parser.add_option("-u","--url",action="store",type="string", 
            dest="database_url",default=None, help = "Database URL.")
        self.parser.add_option("-f","--force",action="store_true",dest="force",
            default=None, help = "Ignore conflicts or data loss.")
        self.parser.add_option("-V","--version",action="store",type="string", 
            dest="pegasus_version",default=None, help = "Pegasus version.")
        self.parser.add_option("-d", "--debug", action="store_true", dest="debug",
                               default=None, help="Enable debugging")
    
    def run(self):
        set_log_level(self.options.debug)
        
        submit_dir = None
        if len(self.args) > 0:
            submit_dir = self.args[0]
            
        adminDB = AdminDB(self.options.config_properties, self.options.database_url, submit_dir)
        
        if not adminDB.verify(self.options.pegasus_version):
            adminDB.update(self.options.pegasus_version, self.options.force)
        
        data = adminDB.current_version(parse=True, print_friendly=True)
        print_versions(data)
    
    
# ------------------------------------------------------
class CheckCommand(Command):
    description = "Verify if the database is updated to the latest or a given version."
    usage = "Usage: %prog check [SUBMITDIR]"

    def __init__(self):
        Command.__init__(self)
        self.parser.add_option("-c","--conf",action="store",type="string", 
            dest="config_properties",default=None,
            help = "Specify properties file. This overrides all other property files.")
        self.parser.add_option("-u","--url",action="store",type="string", 
            dest="database_url",default=None, help = "Database URL.")
        self.parser.add_option("-n","--database",action="store",type="string", 
            dest="database_name",default=None, help = "Database Name.")
        self.parser.add_option("-V","--version",action="store",type="string", 
            dest="pegasus_version",default=None, help = "Pegasus version.")
        self.parser.add_option("-d", "--debug", action="store_true", dest="debug",
                               default=None, help="Enable debugging")
                    
    def run(self):
        set_log_level(self.options.debug)
        
        submit_dir = None
        if len(self.args) > 0:
            submit_dir = self.args[0]

        adminDB = AdminDB(self.options.config_properties, self.options.database_url, submit_dir)
        if adminDB.verify(self.options.pegasus_version, self.options.database_name):
            if self.options.pegasus_version:
                sys.stdout.write("Your database is compatible with Pegasus version: %s.\n" % self.options.pegasus_version)
            else:
                sys.stdout.write("Your database is compatible with the current Pegasus version.\n")
        else:
            if self.options.pegasus_version:
                sys.stderr.write("Your database is NOT compatible with Pegasus version: %s\n" % self.options.pegasus_version)
            else:
                sys.stderr.write("Your database is NOT compatible with the current Pegasus version.\n")
            exit(1)
    
    
# ------------------------------------------------------
class VersionCommand(Command):
    description = "Print the current version of the database."
    usage = "Usage: %prog version [SUBMITDIR]"

    def __init__(self):
        Command.__init__(self)
        self.parser.add_option("-c","--conf",action="store",type="string", 
            dest="config_properties",default=None,
            help = "Specify properties file. This overrides all other property files.")
        self.parser.add_option("-u","--url",action="store",type="string", 
            dest="database_url",default=None, help = "Database URL.")
        self.parser.add_option("-n","--database",action="store",type="string", 
            dest="database_name",default=None, help = "Database Name.")
        self.parser.add_option("-e", "--version-value", action="store_false", dest="version_value",
                               default=True, help="Show actual version values.")
        self.parser.add_option("-d", "--debug", action="store_true", dest="debug",
                               default=None, help="Enable debugging")
        
    def run(self):
        set_log_level(self.options.debug)
        
        submit_dir = None
        if len(self.args) > 0:
            submit_dir = self.args[0]
        
        adminDB = AdminDB(self.options.config_properties, self.options.database_url, submit_dir)
        data = adminDB.current_version(self.options.database_name, self.options.version_value, True)
        print_versions(data)


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
