import os
import sys
from optparse import OptionParser
import getpass

from Pegasus.service import schema, migrations, db

from Pegasus.service.command import Command, CompoundCommand

class AdminCommand(Command):
    def __init__(self):
        Command.__init__(self)
        self.parser.add_option("-d", "--debug", action="store_true", dest="debug",
                default=None, help="Enable debugging")

    def main(self, args=None):
        self.parse(args)

        if self.options.debug:
            app.config.update(DEBUG=True)

        self.run()

class CreateCommand(AdminCommand):
    usage = "%prog create"
    description = "Create the database"

    def run(self):
        if len(self.args) > 0:
            self.parser.error("Invalid argument")

        # First we check to see if it is current
        cur = migrations.current_schema()
        if cur is None:
            print "Creating database..."
            migrations.create()
        elif cur < schema.version:
            print "Database schema out of date. Please run migrate."
        elif cur == schema.version:
            print "Database schema up-to-date."
        else:
            print "Database schema is newer than expected. "\
                  "Expected <= %d, got %d." % (schema.version, cur)

class DropCommand(AdminCommand):
    usage = "%prog drop"
    description = "Drop the database"

    def run(self):
        if len(self.args) > 0:
            self.parser.error("Invalid argument")

        sure = raw_input("Are you sure? [y/n] ") == "y"
        if sure:
            print "Dropping database..."
            migrations.drop()

class MigrateCommand(AdminCommand):
    usage = "%prog migrate [version]"
    description = "Update the database schema"

    def run(self):
        if len(self.args) > 1:
            self.parser.error("Invalid argument")
        elif len(self.args) == 1:
            target = int(self.args[0])
        else:
            target = schema.version

        current = migrations.current_schema()
        if current is None:
            print "No database schema. Please run create."
            exit(1)

        if current == target:
            print "Database schema up to date"
            exit(0)

        print "Migrating database schema from v%d to v%d..." %(current, target)
        migrations.migrate(target)

class AdminClient(CompoundCommand):
    description = "Administrative client for Pegasus Service"
    commands = [
        ('create', CreateCommand),
        ('drop', DropCommand),
        ('migrate', MigrateCommand)
    ]

def main():
    AdminClient().main()

