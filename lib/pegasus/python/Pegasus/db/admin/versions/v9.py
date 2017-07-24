import logging

from Pegasus.db.admin.admin_loader import *
from Pegasus.db.admin.versions.base_version import BaseVersion
from Pegasus.db.schema import *
from sqlalchemy.exc import *

DB_VERSION = 9

log = logging.getLogger(__name__)

class Version(BaseVersion):

    def __init__(self, connection):
        super(Version, self).__init__(connection)

    def update(self, force=False):
        """

        :param force:
        :return:
        """
        log.info("Updating to version %s" % DB_VERSION)
        try:
            #log.info("Updating ensemble_workflow...")
            #self.db.execute("ALTER TABLE ensemble_workflow ADD eventconfig String(512)")
            #log.info("Updating ensemble_workflow...")
            #self.db.execute("ALTER TABLE ensemble_workflow ADD eventfile String(512)")
            #log.info("Updating ensemble_workflow...")
            #self.db.execute("ALTER TABLE ensemble_workflow ADD eventdir String(512)")
            #log.info("Updating ensemble_workflow...")
            #self.db.execute("ALTER TABLE ensemble_workflow ADD eventcontent String(512)")
            #log.info("Updating ensemble_workflow...")
            #self.db.execute("ALTER TABLE ensemble_workflow ADD event_cycle INT NOT NULL DEFAULT 1")
            #log.info("Updating ensemble_workflow...")
            #self.db.execute("ALTER TABLE ensemble_workflow ADD event_maxcycle INT NOT NULL DEFAULT 1")
            #log.info("Updating ensemble_workflow...")
            #self.db.execute("ALTER TABLE ensemble_workflow ADD event_timestamp String(512)")

            log.info("Updating ensemble...")
            self.db.execute("ALTER TABLE ensemble ADD eventconfig String(512)")
        except (OperationalError, ProgrammingError):
            pass
        except Exception, e:
            self.db.rollback()
            log.exception(e)
            raise Exception(e)

        self.db.commit()

    def downgrade(self, force=False):
        "Downgrade is not necessary as reason accepts NULL values"
        pass
