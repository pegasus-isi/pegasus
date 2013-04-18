import os
import sys

from pegasus.service import db, models
from pegasus.service.models import Schema


def current_schema():
    schema = db.session.query(Schema).\
                        order_by(Schema.version.desc()).\
                        first()
    return schema

def usage():
    """Print help message"""
    print "Usage: %s COMMAND\n" % os.path.basename(sys.argv[0])
    print "Commands:"
    for name, fn in COMMANDS.items():
        print "    %-10s %s" % (name, fn.__doc__)
    exit(1)

def create():
    """Create the database"""
    print "Creating database..."
    db.create_all()
    schema = current_schema()
    if schema is None:
        db.session.add(Schema(models.version))
        db.session.commit()
    elif schema.version < models.version:
        print "Schema out of date. Please run migrate."

def drop():
    """Drop the database"""
    sure = raw_input("Are you sure? [y/n] ") == "y"
    if sure:
        print "Dropping database..."
        db.drop_all()

def migrate():
    """Update the database schema"""
    schema = current_schema()
    current = schema.version
    latest = models.version
    if current < latest:
        print "Updating database from v%d to v%d..." %(current, latest)
        #TODO Actually update the schema
        print "NOT YET IMPLEMENTED"
        db.session.add(Schema(models.version))
        db.session.commit()
    else:
        print "Schema up to date"

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
    
    fn()

