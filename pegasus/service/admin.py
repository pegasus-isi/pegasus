import os
import sys
from optparse import OptionParser

from pegasus.service import models, migrations, config

def parse_args(args, synopsis):
    script = os.path.basename(sys.argv[0])
    parser = OptionParser("%s %s" % (script, synopsis))

    parser.add_option("--config", action="store", dest="config",
            default=None, help="Path to configuration file")
    parser.add_option("--dburi", action="store", dest="dburi",
            default=None, help="SQLAlchemy database URI")
    parser.add_option("-d", "--debug", action="store_true", dest="debug",
            default=None, help="Enable debugging")

    options, args = parser.parse_args(args)

    if options.config:
        config.load_config(options.config)

    if options.dburi:
        config.set_dburi(options.dburi)

    if options.debug:
        config.set_debug(True)

    return options, args, parser

def usage():
    """Print help message"""
    print "Usage: %s COMMAND\n" % os.path.basename(sys.argv[0])
    print "Commands:"
    for name, fn in COMMANDS.items():
        print "    %-10s %s" % (name, fn.__doc__)
    exit(1)

def create(args):
    """Create the database"""
    options, args, parser = parse_args(args, "create")

    if len(args) > 0:
        parser.error("Invalid argument")

    # First we check to see if it is current
    schema = migrations.current_schema()
    if schema is None:
        print "Creating database..."
        migrations.create()
    elif schema < models.version:
        print "Database schema out of date. Please run migrate."
    elif schema == models.version:
        print "Database schema up-to-date."
    else:
        print "Database schema is newer than expected. "\
              "Expected <= %d, got %d." % (models.version, schema)

def drop(args):
    """Drop the database"""
    options, args, parser = parse_args(args, "drop")

    if len(args) > 0:
        print "Invalid argument"
        usage()

    sure = raw_input("Are you sure? [y/n] ") == "y"
    if sure:
        print "Dropping database..."
        migrations.drop()

def migrate(args):
    """Update the database schema"""
    options, args, parser = parse_args(args, "migrate [version]")

    if len(args) > 1:
        parser.error("Invalid argument")
    elif len(args) == 1:
        target = int(args[1])
    else:
        target = models.version

    current = migrations.current_schema()
    if current is None:
        print "No database schema. Please run create."
        exit(1)

    if current == target:
        print "Database schema up to date"
        exit(0)

    print "Migrating database schema from v%d to v%d..." %(current, target)
    migrations.migrate(target)


COMMANDS = {
    'create': create,
    'drop': drop,
    'migrate': migrate
}

def main():
    if len(sys.argv) <= 1:
        usage()

    command = sys.argv[1]
    fn = COMMANDS.get(command, None)

    if not fn:
        print "No such command: %s" % command
        exit(1)

    fn(sys.argv[2:])

