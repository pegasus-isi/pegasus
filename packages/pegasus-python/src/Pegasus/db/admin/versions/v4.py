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

from Pegasus.db.admin.versions.base_version import BaseVersion

DB_VERSION = 4

log = logging.getLogger(__name__)


class Version(BaseVersion):
    def __init__(self, connection):
        super().__init__(connection)

    def update(self, force=False):
        "Add archived field to master_workflow table"
        log.info("Updating to version %s" % DB_VERSION)
        # TODO We might need to check to see if the field already exists first
        try:
            self.db.execute(
                "ALTER TABLE master_workflow ADD archived BOOLEAN NOT NULL default 0"
            )
        except (OperationalError, ProgrammingError):
            pass
        except Exception as e:
            log.error("Error adding archived field to master_workflow table")
            log.exception(e)
            raise RuntimeError(e)

    def downgrade(self, force=False):
        "Downgrade is not necessary as archived is added with a default that works for old versions"
