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
    try:
        # First we check to see if it is current
        schema = current_schema()
        if schema.version < models.version:
            print "Database schema out of date. Please run migrate."
        elif schema.version == models.version:
            print "Database schema up-to-date."
        else:
            print "Database schema is newer than expected. "\
                  "Expected <= %d, got %d." % (models.version, schema.version)
    except Exception, e: 
        # If there was no schema table, then we create the database
        if "no such table: schema" in e.message:
            print "Creating database schema..."
            db.create_all()
            db.session.add(Schema(models.version))
            db.session.commit()
        else:
            raise

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
    elif current == latest:
        print "Schema up to date"
    else:
        print "Database schema is newer than expected. "\
              "Expected <= %d, got %d." % (latest, current)
 

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

