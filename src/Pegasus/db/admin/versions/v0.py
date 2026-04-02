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
__author__ = "Rafael Ferreira da Silva"

DB_VERSION = 0

import logging

from sqlalchemy.exc import *
from sqlalchemy.sql import text

from Pegasus.db.admin.admin_loader import *
from Pegasus.db.admin.versions.base_version import *
from Pegasus.db.schema import *

log = logging.getLogger(__name__)


class Version(BaseVersion):
    def __init__(self, connection):
        super().__init__(connection)

    def update(self, force=False):
        log.info("Updating to version %s" % DB_VERSION)
        query = "ALTER TABLE invocation ADD COLUMN remote_cpu_time NUMERIC(10,3) NULL"
        if self.db.connection().dialect.name != "sqlite":
            query += " AFTER remote_duration"
        self.execute(query)
        self.execute(
            "ALTER TABLE job_instance ADD COLUMN multiplier_factor INT NOT NULL DEFAULT 1"
        )
        self.execute("ALTER TABLE job_instance ADD COLUMN exitcode INT NULL")

        success = ["JOB_SUCCESS", "POST_SCRIPT_SUCCESS"]
        failure = [
            "PRE_SCRIPT_FAILED",
            "SUBMIT_FAILED",
            "JOB_FAILURE",
            "POST_SCRIPT_FAILED",
        ]

        try:
            from sqlalchemy import select

            q = self.db.execute(
                select(JobInstance.job_instance_id).order_by(
                    JobInstance.job_instance_id
                )
            ).fetchall()
            for r in q:
                qq = (
                    select(Jobstate.state)
                    .where(Jobstate.job_instance_id == r.job_instance_id)
                    .order_by(Jobstate.jobstate_submit_seq.desc())
                    .limit(1)
                )
                for rr in self.db.execute(qq).fetchall():
                    if rr.state in success:
                        self.db.execute(
                            text(
                                "UPDATE job_instance SET exitcode = 0 WHERE job_instance_id = %s"
                                % r.job_instance_id
                            )
                        )
                    elif rr.state in failure:
                        self.db.execute(
                            text(
                                "UPDATE job_instance SET exitcode = 256 WHERE job_instance_id = %s"
                                % r.job_instance_id
                            )
                        )
                    else:
                        pass
        except (OperationalError, ProgrammingError):
            pass

        self.db.commit()

    def downgrade(self, force=False):
        """ Downgrade to this version will not be allowed."""

    def execute(self, query):
        try:
            if isinstance(query, str):
                from sqlalchemy.sql import text

                query = text(query)
            self.db.execute(query)
        except (OperationalError, ProgrammingError):
            pass
        except Exception as e:
            self.db.rollback()
            raise DBAdminError(e)
