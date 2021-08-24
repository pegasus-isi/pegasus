#!/usr/bin/env python
#
#  Copyright 2017-2021 University Of Southern California
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
import logging

from sqlalchemy.exc import *

from Pegasus.db.admin.admin_loader import *
from Pegasus.db.admin.versions.base_version import BaseVersion
from Pegasus.db.schema import *

DB_VERSION = 10

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
            log.debug("Creating tag table...")
            self.db.execute(
                "CREATE TABLE tag ( tag_id INTEGER NOT NULL, wf_id INTEGER NOT NULL, job_instance_id INTEGER NOT NULL, name VARCHAR(255) NOT NULL, count INTEGER NOT NULL, PRIMARY KEY (tag_id), FOREIGN KEY(wf_id) REFERENCES workflow (wf_id) ON DELETE CASCADE, FOREIGN KEY(job_instance_id) REFERENCES job_instance (job_instance_id) ON DELETE CASCADE )"
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
        Downgrade is not necessary as tag table does not affect the system"
        """
