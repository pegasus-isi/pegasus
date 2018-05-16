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
        log.info('Updating to version %s' % DB_VERSION)
        try:
            log.debug('Creating integrity_meta table...')
            self.db.execute(
                "CREATE TABLE integrity_meta ( integrity_id INTEGER NOT NULL, job_instance_id INTEGER NOT NULL, type VARCHAR(7) NOT NULL, file_type VARCHAR(6) NOT NULL, count INTEGER NOT NULL, duration NUMERIC(10, 3) NOT NULL, PRIMARY KEY (integrity_id, job_instance_id, type, file_type), FOREIGN KEY(job_instance_id) REFERENCES job_instance (job_instance_id) ON DELETE CASCADE, CONSTRAINT integrity_type_desc CHECK (type IN ('check', 'compute')), CONSTRAINT integrity_file_type_desc CHECK (file_type IN ('input', 'output')) )"
            )
            self.db.execute(
                "CREATE UNIQUE INDEX integrity_id_KEY ON integrity_meta (integrity_id)"
            )
        except (OperationalError, ProgrammingError):
            pass
        except Exception as e:
            self.db.rollback()
            log.exception(e)
            raise Exception(e)

        self.db.commit()

    def downgrade(self, force=False):
        "Downgrade is not necessary as integrity_meta table does not affect the system"
        pass
