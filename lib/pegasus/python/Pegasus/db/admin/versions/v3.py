__author__ = "Gideon Juve"
__author__ = "Rafael Ferreira da Silva"

DB_VERSION = 3

from Pegasus.db.admin.admin_loader import *
from Pegasus.db.admin.versions.base_version import *

class Version(BaseVersion):

    def __init__(self, connections, database_name=None, verbose=False, debug=False):
        super(Version, self).__init__(connections, database_name, verbose, debug)


    def update(self, force):
        "Add plan_command field to ensemble_workflow table"
        # TODO We might need to check to see if the field already exists first
        if self.connections[self.database_name]:
            db = self.connections[self.database_name].db
            try:
                db.execute("ALTER TABLE ensemble_workflow ADD plan_command VARCHAR(1024) NOT NULL default './plan.sh'")
            except OperationalError, e:
                pass

    def downgrade(self, force):
        "Downgrade is not necessary as plan_command is added with a default that works for old versions"
        pass


    def is_compatible(self):
        if self.connections[self.database_name]:
            db = self.connections[self.database_name].db
            try:
                db.execute("SELECT plan_command FROM ensemble_workflow")
            except Exception, e:
                return False
        return True


