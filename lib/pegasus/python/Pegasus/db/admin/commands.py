__author__ = "Rafael Ferreira da Silva"

import sys
import logging

from Pegasus.service.command import Command, CompoundCommand
from Pegasus.db.admin.admin_loader import *
from Pegasus.db.admin.versions import *

log = logging.getLogger(__name__)

# ------------------------------------------------------
def print_versions(data):
    sys.stdout.write("Your database is compatible with the following Pegasus versions:\n")
    for key in data:
        sys.stdout.write("    %s\t%s\n" % (key, data[key]))


# ------------------------------------------------------
class CreateCommand(Command):
    description = "Create Pegasus databases."
    usage = "Usage: %prog create [options]"
    
    def __init__(self):
        Command.__init__(self)
        self.parser.add_option("-c","--conf",action="store",type="string", 
            dest="config_properties",default=None,
            help = "Specify properties file. This overrides all other property files.")
        self.parser.add_option("-u","--url",action="store",type="string", 
            dest="database_url",default=None,
            help = "Database URL.")
            
    def run(self):
        AdminDB(self.options.config_properties, self.options.database_url)
        sys.stdout.write("Pegasus databases were successfully created.\n")
    
    
# ------------------------------------------------------
class DowngradeCommand(Command):
    description = "Downgrade the database to a specific version."
    usage = "Usage: %prog downgrade [options] [version]"

    def __init__(self):
        Command.__init__(self)
        self.parser.add_option("-c","--conf",action="store",type="string", 
            dest="config_properties",default=None,
            help = "Specify properties file. This overrides all other property files.")
        self.parser.add_option("-u","--url",action="store",type="string", 
            dest="database_url",default=None, help = "Database URL.")
        self.parser.add_option("-n","--database",action="store",type="string", 
            dest="database_name",default=None, help = "Database Name.")
        self.parser.add_option("-f","--force",action="store_true",dest="force",
            default=None, help = "Ignore conflicts or data loss.")

    def run(self):
        pegasus_version = None
        if len(self.args) > 0:
            pegasus_version = self.args[0]
            
        adminDB = AdminDB(self.options.config_properties, self.options.database_url)
        
        if not pegasus_version or not adminDB.verify(pegasus_version, self.options.database_name):
            adminDB.downgrade(pegasus_version, self.options.database_name, self.options.force)
        
        data = adminDB.current_version(self.options.database_name, True)
        print_versions(data)
    
    
# ------------------------------------------------------
class UpdateCommand(Command):
    description = "Update the database to a specific version."
    usage = "Usage: %prog update [options] [version]"
    
    def __init__(self):
        Command.__init__(self)
        self.parser.add_option("-c","--conf",action="store",type="string", 
            dest="config_properties",default=None,
            help = "Specify properties file. This overrides all other property files.")
        self.parser.add_option("-u","--url",action="store",type="string", 
            dest="database_url",default=None, help = "Database URL.")
        self.parser.add_option("-n","--database",action="store",type="string", 
            dest="database_name",default=None, help = "Database Name.")
        self.parser.add_option("-f","--force",action="store_true",dest="force",
            default=None, help = "Ignore conflicts or data loss.")
    
    def run(self):
        pegasus_version = None
        if len(self.args) > 0:
            pegasus_version = self.args[0]
            
        adminDB = AdminDB(self.options.config_properties, self.options.database_url)
        
        if not adminDB.verify(pegasus_version, self.options.database_name):
            adminDB.update(pegasus_version, self.options.database_name, self.options.force)
        
        data = adminDB.current_version(self.options.database_name, True)
        print_versions(data)
    
    
# ------------------------------------------------------
class CheckCommand(Command):
    description = "Verify if the database is updated to the latest or a given version."
    usage = "Usage: %prog check [pegasus_version]"

    def __init__(self):
        Command.__init__(self)
        self.parser.add_option("-c","--conf",action="store",type="string", 
            dest="config_properties",default=None,
            help = "Specify properties file. This overrides all other property files.")
        self.parser.add_option("-u","--url",action="store",type="string", 
            dest="database_url",default=None, help = "Database URL.")
        self.parser.add_option("-n","--database",action="store",type="string", 
            dest="database_name",default=None, help = "Database Name.")
                    
    def run(self):
        pegasus_version = None
        if len(self.args) > 0:
            pegasus_version = self.args[0]

        adminDB = AdminDB(self.options.config_properties, self.options.database_url)
        if adminDB.verify(pegasus_version, self.options.database_name):
            if pegasus_version:
                sys.stdout.write("Your database is compatible with Pegasus version: %s.\n" % pegasus_version)
            else:
                sys.stdout.write("Your database is compatible with the current Pegasus version.\n")
        else:
            if pegasus_version:
                sys.stderr.write("Your database is NOT compatible with Pegasus version: %s\n" % pegasus_version)
            else:
                sys.stderr.write("Your database is NOT compatible with the current Pegasus version.\n")
            exit(1)
    
    
# ------------------------------------------------------
class VersionCommand(Command):
    description = "Print the current version of the database."
    usage = "Usage: %prog version"

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
        
    def run(self):
        adminDB = AdminDB(self.options.config_properties, self.options.database_url)
        data = adminDB.current_version(self.options.database_name, self.options.version_value)
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
