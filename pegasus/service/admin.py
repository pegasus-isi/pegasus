import os
import sys
from optparse import OptionParser
import getpass

from pegasus.service import schema, migrations, db, users

from pegasus.service.command import Command, CompoundCommand

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

class UserAddCommand(AdminCommand):
    usage = "%prog useradd USERNAME EMAIL"
    description = "Add a user"

    def __init__(self):
        AdminCommand.__init__(self)
        self.parser.add_option("-p", "--password", action="store", dest="password",
                default=None, help="Password")

    def run(self):
        if len(self.args) < 2:
            self.parser.error("Specify USERNAME and EMAIL")
        elif len(self.args) > 2:
            self.parser.error("Invalid argument")

        username = self.args[0]
        email = self.args[1]
        password = self.options.password

        try:
            users.create(username, password, email)
            db.session.commit()
        except Exception, e:
            if self.options.debug: raise
            print e
            exit(1)

class UserListCommand(AdminCommand):
    usage = "%prog userlist"
    description = "List all users"

    def run(self):
        if len(self.args) > 0:
            self.parser.error("Invalid argument")

        print "%-20s %-20s" % ("USERNAME", "EMAIL")
        for user in users.all():
            print "%-20s %-20s" % (user.username, user.email)

class PasswdCommand(AdminCommand):
    usage = "%prog passwd USERNAME"
    description = "Change a user's password"

    def run(self):
        if len(self.args) != 1:
            self.parser.error("Invalid argument")

        try:
            users.passwd(self.args[0], None)
            db.session.commit()
        except Exception, e:
            if self.options.debug: raise
            print e
            exit(1)

class UsermodCommand(AdminCommand):
    usage = "%prog usermod USERNAME EMAIL"
    description = "Change a user's email"

    def run(self):
        if len(self.args) < 2:
            self.parser.error("Specify USERNAME and EMAIL")
        elif len(self.args) > 2:
            self.parser.error("Invalid argument")

        username = self.args[0]
        email = self.args[1]

        try:
            users.usermod(username, email)
            db.session.commit()
        except Exception, e:
            if self.options.debug: raise
            print e
            exit(1)

class AdminClient(CompoundCommand):
    description = "Administrative client for Pegasus Service"
    commands = [
        ('create', CreateCommand),
        ('drop', DropCommand),
        ('migrate', MigrateCommand),
        ('userlist', UserListCommand),
        ('useradd', UserAddCommand),
        ('passwd', PasswdCommand),
        ('usermod', UsermodCommand)
    ]

def main():
    AdminClient().main()

