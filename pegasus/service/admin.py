import sys

from pegasus.service import db, models

def usage():
    print "Usage: %s COMMAND\n" % sys.argv[0]
    print """
Commands:
    help    Print this message
    create  Create database
    drop    Drop database
"""
    exit(1)

help = usage

def create():
    print "Creating database..."
    db.create_all()

def drop():
    sure = raw_input("Are you sure? [y/n] ") == "y"
    if sure:
        print "Dropping database..."
        db.drop_all()

def main():
    if len(sys.argv) <= 1:
        usage()
    
    if "-h" in sys.argv or "--help" in sys.argv:
        usage()
    
    command = sys.argv[1]
    fn = globals().get(command, None)
    
    if not fn:
        usage()
    
    fn()

