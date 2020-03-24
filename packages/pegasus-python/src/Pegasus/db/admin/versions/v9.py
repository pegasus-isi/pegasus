import logging

from sqlalchemy.exc import *

from Pegasus.db.admin.admin_loader import *
from Pegasus.db.admin.versions.base_version import BaseVersion
from Pegasus.db.schema import *

DB_VERSION = 9

log = logging.getLogger(__name__)


class Version(BaseVersion):
    def __init__(self, connection):
        super().__init__(connection)

    def update(self, force=False):
        """

        :param force:
        :return:
        """
        log.info("Updating to version %s" % DB_VERSION)
        try:
            log.debug("Creating integrity_metrics table...")
            self.db.execute(
                "CREATE TABLE integrity_metrics ( integrity_id INTEGER NOT NULL, wf_id INTEGER NOT NULL, job_instance_id INTEGER NOT NULL, type VARCHAR(7) NOT NULL, file_type VARCHAR(6), count INTEGER NOT NULL, duration NUMERIC(10, 3) NOT NULL, PRIMARY KEY (integrity_id), FOREIGN KEY(wf_id) REFERENCES workflow (wf_id) ON DELETE CASCADE, FOREIGN KEY(job_instance_id) REFERENCES job_instance (job_instance_id) ON DELETE CASCADE, CONSTRAINT integrity_type_desc CHECK (type IN ('check', 'compute')), CONSTRAINT integrity_file_type_desc CHECK (file_type IN ('input', 'output')) )"
            )
        except (OperationalError, ProgrammingError):
            pass
        except Exception as e:
            self.db.rollback()
            log.exception(e)
            raise Exception(e)

        self.db.commit()

    def downgrade(self, force=False):
        """
        Downgrade is not necessary as integrity_meta table does not affect the system"
        """
