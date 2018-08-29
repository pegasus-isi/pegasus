import logging

from Pegasus.db.admin.admin_loader import *
from Pegasus.db.admin.versions.base_version import BaseVersion
from Pegasus.db.schema import *
from sqlalchemy.exc import *

DB_VERSION = 10

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
            log.debug('Creating tag table...')
        except (OperationalError, ProgrammingError):
            pass
        except Exception as e:
            self.db.rollback()
            log.exception(e)
            raise Exception(e)

        self.db.commit()

    def downgrade(self, force=False):
        """
        Downgrade is not necessary as tag table does not affect the system"
        """
        pass
