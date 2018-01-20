__author__ = "Gideon Juve"
__author__ = "Rafael Ferreira da Silva"

DB_VERSION = 3

import logging

from Pegasus.db.admin.admin_loader import *
from Pegasus.db.admin.versions.base_version import *
from sqlalchemy.exc import *

log = logging.getLogger(__name__)


class Version(BaseVersion):
    def __init__(self, connection):
        super(Version, self).__init__(connection)

    def update(self, force=False):
        "Add plan_command field to ensemble_workflow table"
        log.info("Updating to version %s" % DB_VERSION)
        # TODO We might need to check to see if the field already exists first
        try:
            self.db.execute(
                "ALTER TABLE ensemble_workflow ADD plan_command VARCHAR(1024) NOT NULL default './plan.sh'"
            )
        except (OperationalError, ProgrammingError):
            pass
        except Exception as e:
            self.db.rollback()
            log.exception(e)
            raise Exception(e)

    def downgrade(self, force=False):
        "Downgrade is not necessary as plan_command is added with a default that works for old versions"
        pass
