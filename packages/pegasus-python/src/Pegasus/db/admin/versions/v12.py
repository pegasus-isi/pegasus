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
        log.debug("Updating to version {}".format(DB_VERSION))
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
        log.debug("Downgrading from version {}".format(DB_VERSION))

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
            (
                Workflow,
                "UNIQUE_WF_UUID",
                [Workflow.wf_uuid.name],
                [
                    "CREATE TABLE workflow_new ("
                    "wf_id INTEGER NOT NULL, "
                    "wf_uuid VARCHAR(255) NOT NULL, "
                    "dag_file_name VARCHAR(255), "
                    "timestamp NUMERIC(16, 6), "
                    "submit_hostname VARCHAR(255), "
                    "submit_dir TEXT, "
                    "planner_arguments TEXT, "
                    "user VARCHAR(255), "
                    "grid_dn VARCHAR(255), "
                    "planner_version VARCHAR(255), "
                    "dax_label VARCHAR(255), "
                    "dax_version VARCHAR(255), "
                    "dax_file VARCHAR(255), "
                    "db_url TEXT, "
                    "parent_wf_id INTEGER, "
                    "root_wf_id INTEGER, "
                    "PRIMARY KEY (wf_id), "
                    "FOREIGN KEY(parent_wf_id) REFERENCES workflow (wf_id) ON DELETE CASCADE, "
                    "CONSTRAINT 'UNIQUE_WF_UUID' UNIQUE (wf_uuid) )"
                ],
            ),
            (
                Host,
                "UNIQUE_HOST",
                [Host.wf_id.name, Host.site.name, Host.hostname.name, Host.ip.name],
                [
                    "CREATE TABLE host_new ("
                    "host_id INTEGER NOT NULL, "
                    "wf_id INTEGER NOT NULL, "
                    "site VARCHAR(255) NOT NULL, "
                    "hostname VARCHAR(255) NOT NULL, "
                    "ip VARCHAR(255) NOT NULL, "
                    "uname VARCHAR(255), "
                    "total_memory INTEGER, "
                    "PRIMARY KEY (host_id), "
                    "FOREIGN KEY(wf_id) REFERENCES workflow (wf_id) ON DELETE CASCADE, "
                    "CONSTRAINT 'UNIQUE_HOST' UNIQUE (wf_id, site, hostname, ip) )"
                ],
            ),
            (
                Job,
                "UNIQUE_JOB",
                [Job.wf_id.name, Job.exec_job_id.name],
                [
                    "CREATE TABLE job_new ("
                    "job_id INTEGER NOT NULL, "
                    "wf_id INTEGER NOT NULL, "
                    "exec_job_id VARCHAR(255) NOT NULL, "
                    "submit_file VARCHAR(255) NOT NULL, "
                    "type_desc VARCHAR(14) NOT NULL, "
                    "clustered BOOLEAN NOT NULL, "
                    "max_retries INTEGER NOT NULL, "
                    "executable TEXT NOT NULL, "
                    "argv TEXT, "
                    "task_count INTEGER NOT NULL, "
                    "PRIMARY KEY (job_id), "
                    "FOREIGN KEY(wf_id) REFERENCES workflow (wf_id) ON DELETE CASCADE, "
                    "CONSTRAINT job_type_desc CHECK (type_desc IN ('unknown', 'compute', 'stage-in-tx', 'stage-out-tx', 'registration', 'inter-site-tx', 'create-dir', 'staged-compute', 'cleanup', 'chmod', 'dax', 'dag')), "
                    "CHECK (clustered IN (0, 1)), "
                    "CONSTRAINT 'UNIQUE_JOB' UNIQUE (wf_id, exec_job_id) "
                    ")",
                    "CREATE INDEX 'job_type_desc_COL' ON job (type_desc)",
                    "CREATE INDEX 'job_exec_job_id_COL' ON job (exec_job_id)",
                ],
            ),
            (
                JobInstance,
                "UNIQUE_JOB_INSTANCE",
                [JobInstance.job_id.name, JobInstance.job_submit_seq.name],
                [
                    "CREATE TABLE job_instance_new ("
                    "job_instance_id INTEGER NOT NULL, "
                    "job_id INTEGER NOT NULL, "
                    "host_id INTEGER, "
                    "job_submit_seq INTEGER NOT NULL, "
                    "sched_id VARCHAR(255), "
                    "site VARCHAR(255), "
                    "user VARCHAR(255), "
                    "work_dir TEXT, "
                    "cluster_start NUMERIC(16, 6), "
                    "cluster_duration NUMERIC(10, 3), "
                    "local_duration NUMERIC(10, 3), "
                    "subwf_id INTEGER, "
                    "stdout_file VARCHAR(255), "
                    "stdout_text TEXT, "
                    "stderr_file VARCHAR(255), "
                    "stderr_text TEXT, "
                    "stdin_file VARCHAR(255), "
                    "multiplier_factor INTEGER NOT NULL, "
                    "exitcode INTEGER, "
                    "PRIMARY KEY (job_instance_id), "
                    "FOREIGN KEY(job_id) REFERENCES job (job_id) ON DELETE CASCADE, "
                    "FOREIGN KEY(host_id) REFERENCES host (host_id) ON DELETE SET NULL, "
                    "FOREIGN KEY(subwf_id) REFERENCES workflow (wf_id) ON DELETE SET NULL, "
                    "CONSTRAINT 'UNIQUE_JOB_INSTANCE' UNIQUE (job_id, job_submit_seq) )"
                ],
            ),
            (
                Tag,
                "UNIQUE_TAG",
                [Tag.wf_id.name, Tag.job_instance_id.name],
                [
                    "CREATE TABLE tag_new ("
                    "tag_id INTEGER NOT NULL, "
                    "wf_id INTEGER NOT NULL, "
                    "job_instance_id INTEGER NOT NULL, "
                    "name VARCHAR(255) NOT NULL, "
                    "count INTEGER NOT NULL, "
                    "PRIMARY KEY (tag_id), "
                    "FOREIGN KEY(wf_id) REFERENCES workflow (wf_id) ON DELETE CASCADE, "
                    "FOREIGN KEY(job_instance_id) REFERENCES job_instance (job_instance_id) ON DELETE CASCADE, "
                    "CONSTRAINT 'UNIQUE_TAG' UNIQUE (wf_id, job_instance_id) )"
                ],
            ),
            (
                Task,
                "UNIQUE_TASK",
                [Task.wf_id.name, Task.abs_task_id.name],
                [
                    "CREATE TABLE task_new ("
                    "task_id INTEGER NOT NULL, "
                    "wf_id INTEGER NOT NULL, "
                    "job_id INTEGER, "
                    "abs_task_id VARCHAR(255) NOT NULL, "
                    "transformation TEXT NOT NULL, "
                    "argv TEXT, "
                    "type_desc VARCHAR(255) NOT NULL, "
                    "PRIMARY KEY (task_id), "
                    "FOREIGN KEY(wf_id) REFERENCES workflow (wf_id) ON DELETE CASCADE, "
                    "FOREIGN KEY(job_id) REFERENCES job (job_id) ON DELETE SET NULL, "
                    "CONSTRAINT 'UNIQUE_TASK' UNIQUE (wf_id, abs_task_id))",
                    "CREATE INDEX 'task_abs_task_id_COL' ON task (abs_task_id)",
                    "CREATE INDEX 'task_wf_id_COL' ON task (wf_id)",
                ],
            ),
            (
                Invocation,
                "UNIQUE_INVOCATION",
                [Invocation.job_instance_id.name, Invocation.task_submit_seq.name],
                [
                    "CREATE TABLE invocation_new ("
                    "invocation_id INTEGER NOT NULL, "
                    "wf_id INTEGER NOT NULL, "
                    "job_instance_id INTEGER NOT NULL, "
                    "task_submit_seq INTEGER NOT NULL, "
                    "start_time NUMERIC(16, 6) NOT NULL, "
                    "remote_duration NUMERIC(10, 3) NOT NULL, "
                    "remote_cpu_time NUMERIC(10, 3), "
                    "exitcode INTEGER NOT NULL, "
                    "transformation TEXT NOT NULL, "
                    "executable TEXT NOT NULL, "
                    "argv TEXT, "
                    "abs_task_id VARCHAR(255), "
                    "maxrss INTEGER, "
                    "avg_cpu NUMERIC(16, 6), "
                    "PRIMARY KEY (invocation_id), "
                    "FOREIGN KEY(wf_id) REFERENCES workflow (wf_id) ON DELETE CASCADE, "
                    "FOREIGN KEY(job_instance_id) REFERENCES job_instance (job_instance_id) ON DELETE CASCADE, "
                    "CONSTRAINT 'UNIQUE_INVOCATION' UNIQUE (job_instance_id, task_submit_seq)"
                    ")",
                    "CREATE INDEX 'invoc_wf_id_COL' ON invocation (wf_id)",
                    "CREATE INDEX 'invoc_abs_task_id_COL' ON invocation (abs_task_id)",
                ],
            ),
            (
                IntegrityMetrics,
                "UNIQUE_INTEGRITY",
                [
                    IntegrityMetrics.job_instance_id.name,
                    IntegrityMetrics.type.name,
                    IntegrityMetrics.file_type.name,
                ],
                [
                    "CREATE TABLE integrity_new ("
                    "integrity_id INTEGER NOT NULL, "
                    "wf_id INTEGER NOT NULL, "
                    "job_instance_id INTEGER NOT NULL, "
                    "type VARCHAR(7) NOT NULL, "
                    "file_type VARCHAR(6), "
                    "count INTEGER NOT NULL, "
                    "duration NUMERIC(10, 3) NOT NULL, "
                    "PRIMARY KEY (integrity_id), "
                    "FOREIGN KEY(wf_id) REFERENCES workflow (wf_id) ON DELETE CASCADE, "
                    "FOREIGN KEY(job_instance_id) REFERENCES job_instance (job_instance_id) ON DELETE CASCADE, "
                    "CONSTRAINT integrity_type_desc CHECK (type IN ('check', 'compute')), "
                    "CONSTRAINT integrity_file_type_desc CHECK (file_type IN ('input', 'output')), "
                    "CONSTRAINT 'UNIQUE_INTEGRITY' UNIQUE (job_instance_id, type, file_type) )"
                ],
            ),
            (
                MasterWorkflow,
                "UNIQUE_MASTER_WF_UUID",
                [MasterWorkflow.wf_uuid.name],
                [
                    "CREATE TABLE master_workflow_new ("
                    "wf_id INTEGER NOT NULL, "
                    "wf_uuid VARCHAR(255) NOT NULL, "
                    "dax_label VARCHAR(255), "
                    "dax_version VARCHAR(255), "
                    "dax_file VARCHAR(255), "
                    "dag_file_name VARCHAR(255), "
                    "timestamp NUMERIC(16, 6), "
                    "submit_hostname VARCHAR(255), "
                    "submit_dir TEXT, "
                    "planner_arguments TEXT, "
                    "user VARCHAR(255), "
                    "grid_dn VARCHAR(255), "
                    "planner_version VARCHAR(255), "
                    "db_url TEXT, "
                    "archived BOOLEAN NOT NULL, "
                    "PRIMARY KEY (wf_id), "
                    "CHECK (archived IN (0, 1)), "
                    "CONSTRAINT 'UNIQUE_MASTER_WF_UUID' UNIQUE (wf_uuid))"
                ],
            ),
            (
                EnsembleWorkflow,
                "UNIQUE_ENSEMBLE_WORKFLOW",
                [EnsembleWorkflow.ensemble_id.name, EnsembleWorkflow.name.name],
                [
                    "CREATE TABLE master_workflow_new ("
                    "wf_id INTEGER NOT NULL, "
                    "wf_uuid VARCHAR(255) NOT NULL, "
                    "dax_label VARCHAR(255), "
                    "dax_version VARCHAR(255), "
                    "dax_file VARCHAR(255), "
                    "dag_file_name VARCHAR(255), "
                    "timestamp NUMERIC(16, 6), "
                    "submit_hostname VARCHAR(255), "
                    "submit_dir TEXT, "
                    "planner_arguments TEXT, "
                    "user VARCHAR(255), "
                    "grid_dn VARCHAR(255), "
                    "planner_version VARCHAR(255), "
                    "db_url TEXT, "
                    "archived BOOLEAN NOT NULL, "
                    "PRIMARY KEY (wf_id), "
                    "CHECK (archived IN (0, 1)), "
                    "CONSTRAINT 'UNIQUE_MASTER_WF_UUID' UNIQUE (wf_uuid))"
                ],
            ),
            (
                RCLFN,
                "UNIQUE_LFN",
                [RCLFN.lfn.name],
                [
                    "CREATE TABLE rc_lfn_new ( "
                    "lfn_id INTEGER NOT NULL, "
                    "lfn VARCHAR(245) NOT NULL, "
                    "PRIMARY KEY (lfn_id), "
                    "CONSTRAINT 'UNIQUE_LFN' UNIQUE (lfn))"
                ],
            ),
            (
                RCPFN,
                "UNIQUE_PFN",
                [RCPFN.lfn_id.name, RCPFN.pfn.name, RCPFN.site.name],
                [
                    "CREATE TABLE rc_pfn_new ( "
                    "pfn_id INTEGER NOT NULL, "
                    "lfn_id INTEGER NOT NULL, "
                    "pfn VARCHAR(245) NOT NULL, "
                    "site VARCHAR(245), "
                    "PRIMARY KEY (pfn_id), "
                    "FOREIGN KEY(lfn_id) REFERENCES rc_lfn (lfn_id) ON DELETE CASCADE, "
                    "CONSTRAINT 'UNIQUE_PFN' UNIQUE (lfn_id, pfn, site))"
                ],
            ),
            (
                Ensemble,
                "UNIQUE_ENSEMBLE",
                [Ensemble.name.name, Ensemble.username.name],
                [
                    "CREATE TABLE ensemble_new ("
                    "id INTEGER NOT NULL, "
                    "name VARCHAR(100) NOT NULL, "
                    "created DATETIME NOT NULL, "
                    "updated DATETIME NOT NULL, "
                    "state VARCHAR(6) NOT NULL, "
                    "max_running INTEGER NOT NULL, "
                    "max_planning INTEGER NOT NULL, "
                    "username VARCHAR(100) NOT NULL, "
                    "PRIMARY KEY (id), "
                    "CONSTRAINT ensemble_state CHECK (state IN ('ACTIVE', 'PAUSED', 'HELD')), "
                    "CONSTRAINT 'UNIQUE_ENSEMBLE' UNIQUE (name, username))"
                ],
            ),
        ]
        for uc in uc_list:
            if self.db.get_bind().driver == "pysqlite":
                self._update_sqlite_table(uc[0], uc[3])
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

    def _update_sqlite_table(self, tbl, create_stmt):
        """"."""
        try:
            if {tbl.__tablename__}.issubset(self.db.get_bind().table_names()):
                log.debug("Updating table: {}".format(tbl.__tablename__))
                for stmt in create_stmt:
                    self.db.execute(stmt)

                resultproxy = self.db.execute(
                    "PRAGMA table_info('{}')".format(tbl.__tablename__)
                )
                cols = []
                for rowproxy in resultproxy:
                    for column, value in rowproxy.items():
                        if column == "name":
                            cols.append(value)

                if len(cols) > 0:
                    cols = ", ".join(cols)
                    self.db.execute(
                        "INSERT INTO {} ({}) SELECT {} FROM {}".format(
                            tbl.__tablename__ + "_new", cols, cols, tbl.__tablename__
                        )
                    )

                self.db.execute("DROP TABLE {}".format(tbl.__tablename__))
                self.db.execute(
                    "ALTER TABLE {} RENAME TO {}".format(
                        tbl.__tablename__ + "_new", tbl.__tablename__
                    )
                )
                self.db.commit()

        except (OperationalError, ProgrammingError, Exception) as e:
            if not "no such table" in str(e):
                self.db.rollback()
                raise DBAdminError(e)
