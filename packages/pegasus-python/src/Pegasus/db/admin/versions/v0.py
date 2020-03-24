__author__ = "Rafael Ferreira da Silva"

DB_VERSION = 0

import logging

from sqlalchemy.exc import *

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
            q = self.db.query(JobInstance.job_instance_id).order_by(
                JobInstance.job_instance_id
            )
            for r in q.all():
                qq = self.db.query(Jobstate.state)
                qq = qq.filter(Jobstate.job_instance_id == r.job_instance_id)
                qq = qq.order_by(Jobstate.jobstate_submit_seq.desc()).limit(1)
                for rr in qq.all():
                    if rr.state in success:
                        self.db.execute(
                            "UPDATE job_instance SET exitcode = 0 WHERE job_instance_id = %s"
                            % r.job_instance_id
                        )
                    elif rr.state in failure:
                        self.db.execute(
                            "UPDATE job_instance SET exitcode = 256 WHERE job_instance_id = %s"
                            % r.job_instance_id
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
            self.db.execute(query)
        except (OperationalError, ProgrammingError):
            pass
        except Exception as e:
            self.db.rollback()
            raise DBAdminError(e)
