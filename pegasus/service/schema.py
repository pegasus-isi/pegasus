from datetime import datetime

from pegasus.service import db

#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# IMPORTANT
# This is the current version of the schema. It should be incremented
# each time a change is made to the schema and a migration script
# should be created to go from the old version to the new version.
version = 2
#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

class Schema(db.Model):
    __tablename__ = 'schema'
    __table_args__ = {'mysql_engine':'InnoDB'}

    id = db.Column(db.Integer, primary_key=True)
    version = db.Column(db.Integer)
    timestamp = db.Column(db.DateTime)

    def __init__(self, version):
        self.version = version
        self.timestamp = datetime.utcnow()

    def __repr__(self):
        return '<Schema %d %s>' % (self.version, self.timestamp)

# Need to import all the other schema objects here
from pegasus.service.users import User

