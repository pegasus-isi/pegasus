import os
import sys

from pegasus.service import models, migrations

def usage():
    """Print help message"""
    print "Usage: %s COMMAND\n" % os.path.basename(sys.argv[0])
    print "Commands:"
    for name, fn in COMMANDS.items():
        print "    %-10s %s" % (name, fn.__doc__)
    exit(1)

def create(args):
    """Create the database"""
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
    sure = raw_input("Are you sure? [y/n] ") == "y"
    if sure:
        print "Dropping database..."
        migrations.drop()

def migrate(args):
    """Update the database schema"""
    if len(args) > 2:
        print "Usage: migrate [version]"
        exit(1)
    elif len(args) == 2:
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
    'help': usage,
    'create': create,
    'drop': drop,
    'migrate': migrate
}

def main():
    if len(sys.argv) <= 1:
        usage()
    
    if "-h" in sys.argv or "--help" in sys.argv:
        usage()
    
    command = sys.argv[1]
    fn = COMMANDS.get(command, None)
    
    if not fn:
        print "No such command: %s" % command
        exit(1)
    
    fn(sys.argv[1:])

