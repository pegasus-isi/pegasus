__author__ = "Gideon Juve"

DB_VERSION = 1.3

from Pegasus.db.admin.admin_loader import *
from Pegasus.db.admin.versions.base_version import *

class Version(BaseVersion):

    def __init__(self, config_properties, verbose=False, debug=False):
        super(Version, self).__init__(config_properties, verbose, debug)
        self.db = DashboardDB(config_properties, verbose, debug)

    def update(self, force):
        "Add plan_command field to ensemble_workflow table"
        # TODO We might need to check to see if the field already exists first
        self.db.execute_update("ALTER TABLE ensemble_workflow ADD plan_command VARCHAR(1024) NOT NULL default './plan.sh'")

    def downgrade(self, force):
        "Downgrade is not necessary as plan_command is added with a default that works for old versions"
        pass

    def is_compatible(self):
        try:
            cur = self.db.get_connection().cursor()
            cur.execute("SELECT plan_command FROM ensemble_workflow")
        except:
            return False
        return True

    def dispose(self):
        self.db.close()

