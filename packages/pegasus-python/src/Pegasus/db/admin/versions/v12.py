import logging
import os
import subprocess

from sqlalchemy import inspect

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
        log.debug("Updating to version %s" % DB_VERSION)
        try:
            self.db.execute("DROP TABLE sequences")
        except Exception:
            pass

        # fixing indexes
        log.debug("Updating indexes")
        self._drop_indexes(
            [
                ("wf_id_KEY", Workflow),
                ("UNIQUE_WORKFLOWSTATE", Workflowstate),
                ("job_id_KEY", Job),
                ("UNIQUE_JOB_EDGE", JobEdge),
                ("job_instance_id_KEY", JobInstance),
                ("UNIQUE_JOBSTATE", Jobstate),
                ("tag_id_KEY", Tag),
                ("task_id_KEY", Task),
                ("UNIQUE_TASK_EDGE", TaskEdge),
                ("invocation_id_KEY", Invocation),
                ("integrity_id_KEY", IntegrityMetrics),
                ("UNIQUE_MASTER_WORKFLOWSTATE", MasterWorkflowstate),
                ("rc_meta_unique", RCMeta),
                ("wf_uuid_UNIQUE", Workflow),
                ("UNIQUE_HOST", Host),
                ("UNIQUE_JOB", Job),
                ("UNIQUE_JOB_INSTANCE", JobInstance),
                ("UNIQUE_TAG", Tag),
                ("UNIQUE_TASK", Task),
                ("UNIQUE_INVOCATION", Invocation),
                ("UNIQUE_INTEGRITY", IntegrityMetrics),
                ("UNIQUE_MASTER_WF_UUID", MasterWorkflow),
                ("UNIQUE_ENSEMBLE_WORKFLOW", EnsembleWorkflow),
                ("ix_rc_lfn", RCLFN),
                ("UNIQUE_PFN", RCPFN),
                ("UNIQUE_ENSEMBLE", Ensemble),
                ("UNIQUE_WORKFLOW_META", WorkflowMeta),
                ("UNIQUE_TASK_META", TaskMeta),
            ]
        )
        if self.db.get_bind().driver == "pysqlite":
            self._drop_indexes(
                [
                    ("job_exec_job_id_COL", None),
                    ("task_abs_task_id_COL", None),
                    ("invoc_abs_task_id_COL", None),
                    ("job_type_desc_COL", None),
                    ("task_wf_id_COL", None),
                    ("invoc_wf_id_COL", None),
                ]
            )
            self._create_indexes(
                [
                    [Workflowstate, Workflowstate.timestamp],
                    [Jobstate, Jobstate.jobstate_submit_seq],
                ]
            )

        # updating unique constraints
        log.debug("Updating unique constraints")
        self._update_unique_constraints()

        # update charset
        if self.db.get_bind().driver == "mysqldb":
            log.debug("Updating table charsets")
            self.db.execute("SET FOREIGN_KEY_CHECKS=0")
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
                MasterWorkflow,
                MasterWorkflowstate,
                Ensemble,
                EnsembleWorkflow,
                # JDBCRC
                RCLFN,
                RCPFN,
                RCMeta,
            )
            for table in tables:
                self.db.execute(
                    "ALTER TABLE %s CONVERT TO CHARACTER SET utf8mb4"
                    % table.__tablename__
                )
            self.db.execute("SET FOREIGN_KEY_CHECKS=1")
            self.db.commit()

        elif self.db.get_bind().driver == "psycopg2":
            url = self.db.get_bind().url
            command = (
                "psql %s" % url.database
                if not url.password
                else "export PGPASSWORD={}; psql {}".format(url.password, url.database)
            )
            if url.username:
                command += " -U %s" % url.username
            command += (
                " -c \"UPDATE pg_database SET encoding = pg_char_to_encoding('UTF8') WHERE datname = '%s'\""
                % url.database
            )
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
        """."""
        log.debug("Downgrading from version %s" % DB_VERSION)

    def _drop_indexes(self, index_list):
        """"."""
        for index in index_list:
            try:
                if self.db.get_bind().driver == "mysqldb":
                    self.db.execute(
                        "DROP INDEX {} ON {}".format(index[0], index[1].__tablename__)
                    )
                else:
                    self.db.execute("DROP INDEX %s" % index[0])
            except (OperationalError, ProgrammingError):
                pass
            except Exception as e:
                self.db.rollback()
                raise DBAdminError(e)
        self.db.commit()

    def _create_indexes(self, index_list):
        """"."""
        for index in index_list:
            try:
                self.db.execute(
                    "CREATE INDEX {}_{}_COL ON {}({})".format(
                        index[0].__tablename__,
                        index[1].name,
                        index[0].__tablename__,
                        index[1].name,
                    )
                )
            except (OperationalError, ProgrammingError):
                pass
            except Exception as e:
                self.db.rollback()
                raise DBAdminError(e)
        self.db.commit()

    def _update_unique_constraints(self):
        """."""
        uc_list = [
            (Workflow, "UNIQUE_WF_UUID", [Workflow.wf_uuid.name]),
            (
                Host,
                "UNIQUE_HOST",
                [Host.wf_id.name, Host.site.name, Host.hostname.name, Host.ip.name,],
            ),
            (Job, "UNIQUE_JOB", [Job.wf_id.name, Job.exec_job_id.name]),
            (
                JobInstance,
                "UNIQUE_JOB_INSTANCE",
                [JobInstance.job_id.name, JobInstance.job_submit_seq.name],
            ),
            (Tag, "UNIQUE_TAG", [Tag.wf_id.name, Tag.job_instance_id.name]),
            (Task, "UNIQUE_TASK", [Task.wf_id.name, Task.abs_task_id.name]),
            (
                Invocation,
                "UNIQUE_INVOCATION",
                [Invocation.job_instance_id.name, Invocation.task_submit_seq.name,],
            ),
            (
                IntegrityMetrics,
                "UNIQUE_INTEGRITY",
                [
                    IntegrityMetrics.job_instance_id.name,
                    IntegrityMetrics.type.name,
                    IntegrityMetrics.file_type.name,
                ],
            ),
            (MasterWorkflow, "UNIQUE_MASTER_WF_UUID", [MasterWorkflow.wf_uuid.name],),
            (
                EnsembleWorkflow,
                "UNIQUE_ENSEMBLE_WORKFLOW",
                [EnsembleWorkflow.ensemble_id.name, EnsembleWorkflow.name.name],
            ),
            (RCLFN, "UNIQUE_LFN", [RCLFN.lfn.name]),
            (
                RCPFN,
                "UNIQUE_PFN",
                [RCPFN.lfn_id.name, RCPFN.pfn.name, RCPFN.site.name],
            ),
            (Ensemble, "UNIQUE_ENSEMBLE", [Ensemble.name.name, Ensemble.username.name]),
        ]
        for uc in uc_list:
            if self.db.get_bind().driver == "pysqlite":
                self._update_sqlite_table(uc[0])
            else:
                try:
                    self.db.execute(
                        "ALTER TABLE {} ADD CONSTRAINT {} UNIQUE ({})".format(
                            uc[0].__tablename__, uc[1], ",".join(uc[2])
                        )
                    )
                except (OperationalError, ProgrammingError):
                    pass
                except Exception as e:
                    self.db.rollback()
                    raise DBAdminError(e)
        self.db.commit()

    def _update_sqlite_table(self, tbl):
        """"."""
        try:
            self.db.execute("PRAGMA foreign_keys=off")
            self.db.execute(
                "ALTER TABLE {} RENAME TO _{}_old".format(
                    tbl.__tablename__, tbl.__tablename__
                )
            )
            tbl.__table__.create(self.db.get_bind(), checkfirst=True)

            cols = ", ".join(inspect(tbl).column_attrs.keys())
            self.db.execute(
                "INSERT INTO {} ({}) SELECT {} FROM _{}_old".format(
                    tbl.__tablename__, cols, cols, tbl.__tablename__
                )
            )
            self.db.execute("DROP TABLE _%s_old" % tbl.__tablename__)
            self.db.execute("PRAGMA foreign_keys=on")
            self.db.commit()
        except (OperationalError, ProgrammingError):
            pass
        except Exception as e:
            self.db.rollback()
            raise DBAdminError(e)
