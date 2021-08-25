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
__author__ = "Gideon Juve"
__author__ = "Rafael Ferreira da Silva"

DB_VERSION = 3

import logging

from sqlalchemy.exc import *

from Pegasus.db.admin.admin_loader import *
from Pegasus.db.admin.versions.base_version import *

log = logging.getLogger(__name__)


class Version(BaseVersion):
    def __init__(self, connection):
        super().__init__(connection)

    def update(self, force=False):
        "Add plan_command field to ensemble_workflow table"
        log.info("Updating to version %s" % DB_VERSION)
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
