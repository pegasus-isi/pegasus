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

from Pegasus.db.admin.admin_loader import DBAdminError
from Pegasus.db.admin.versions.base_version import BaseVersion

DB_VERSION = 13

log = logging.getLogger(__name__)


class Version(BaseVersion):
    def __init__(self, connection):
        super().__init__(connection)

    def update(self, force=False):
        """."""
        log.debug("Updating to version %s" % DB_VERSION)
        try:
            self.db.execute("ALTER TABLE invocation ADD COLUMN maxrss INTEGER")
            self.db.execute("ALTER TABLE invocation ADD COLUMN avg_cpu NUMERIC(16, 6)")
        except Exception as e:
            if "uplicate column name" not in str(
                e
            ) and "no such table: invocation" not in str(e):
                self.db.rollback()
                raise DBAdminError(e)

    def downgrade(self, force=False):
        """."""
        log.debug("Downgrading from version %s" % DB_VERSION)
        # no downgrade is necessary
