import logging
import os
import subprocess

from Pegasus.db.admin.admin_loader import *
from Pegasus.db.admin.versions.base_version import BaseVersion
from Pegasus.db.schema import *

DB_VERSION = 12

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
            self.db.execute("DROP TABLE sequences")
        except Exception:
            pass

        self.db.commit()

        # update charset
        if self.db.get_bind().driver == "mysqldb":
            log.debug("Updating table charsets")
            tables = (
                DBVersion,
                # WORKFLOW
                Workflow,
                Workflowstate,
                WorkflowMeta,
                WorkflowFiles,
                Host,
                Job,
                JobEdge,
                JobInstance,
                Jobstate,
                Tag,
                Task,
                TaskEdge,
                TaskMeta,
                Invocation,
                IntegrityMetrics,
                # MASTER
                DashboardWorkflow,
                DashboardWorkflowstate,
                Ensemble,
                EnsembleWorkflow,
                # JDBCRC
                RCLFN,
                RCPFN,
                RCMeta,
            )
            for table in tables:
                self.db.execute("ALTER TABLE %s CONVERT TO CHARACTER SET utf8mb4" % tbl_name)
            self.db.commit()

        elif self.db.get_bind().driver == "psycopg2":
            url = self.db.get_bind().url
            command = ("psql %s" % url.database
                       if not url.password
                       else "export PGPASSWORD=%s; psql %s" % (url.password, url.database)
                       )
            if url.username:
                command += " -U %s" % url.username
            command += " -c \"UPDATE pg_database SET encoding = pg_char_to_encoding('UTF8') WHERE datname = '%s'\"" % url.database
            log.debug("Executing: %s" % command)
            child = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                shell=True,
                cwd=os.getcwd(),
            )
            out, err = child.communicate()
            if child.returncode != 0:
                raise DBAdminError(err.decode("utf8").strip())

    def downgrade(self, force=False):
        """

        """
        log.info("Downgrading from version %s" % DB_VERSION)
