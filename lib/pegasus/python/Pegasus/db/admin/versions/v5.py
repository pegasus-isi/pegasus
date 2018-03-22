import logging
import warnings

from Pegasus.db.admin.versions.base_version import BaseVersion
from Pegasus.db.schema import *
from sqlalchemy import *
from sqlalchemy.exc import *

DB_VERSION = 5

log = logging.getLogger(__name__)


class Version(BaseVersion):
    def __init__(self, connection):
        super(Version, self).__init__(connection)

    def update(self, force=False):
        "Fixes malfunction past migrations and clean the database"
        log.info("Updating to version %s" % DB_VERSION)

        # check and fix foreign key for master_workflowstate table
        with warnings.catch_warnings():
            table_names = self.db.get_bind().table_names()
            tables_to_inquiry = [
                'master_workflowstate', 'master_workflow', 'workflow'
            ]
            if set(tables_to_inquiry).issubset(table_names):
                meta = MetaData()
                warnings.simplefilter("ignore")
                meta.reflect(bind=self.db.get_bind(), only=tables_to_inquiry)
                mw_id = meta.tables['master_workflowstate'].c.wf_id

                # PM-1015: invalid constraint
                if not mw_id.references(
                    meta.tables['master_workflow'].c.wf_id
                ) and mw_id.references(meta.tables['workflow'].c.wf_id):
                    log.info("Updating foreign key constraint.")
                    try:
                        self.db.execute(
                            "DROP INDEX UNIQUE_MASTER_WORKFLOWSTATE"
                        )
                    except Exception as e:
                        pass
                    if self.db.get_bind().driver == "mysqldb":
                        self.db.execute(
                            "RENAME TABLE master_workflowstate TO master_workflowstate_v4"
                        )
                    else:
                        self.db.execute(
                            "ALTER TABLE master_workflowstate RENAME TO master_workflowstate_v4"
                        )
                    pg_workflowstate.create(
                        self.db.get_bind(), checkfirst=True
                    )
                    self.db.execute(
                        "INSERT INTO master_workflowstate(wf_id, state, timestamp, restart_count, status) SELECT m4.wf_id, m4.state, m4.timestamp, m4.restart_count, m4.status FROM master_workflowstate_v4 m4 LEFT JOIN master_workflow mw WHERE m4.wf_id=mw.wf_id"
                    )
                    self.db.commit()
                    self._drop_table("master_workflowstate_v4")

        # clean the database from past migrations
        self._drop_table("rc_lfn_new")
        self._drop_table("rc_lfn_old")

    def downgrade(self, force=False):
        "No need for downgrade"
        pass

    def _drop_table(self, table_name):
        """
        Drop a table.
        :param table_name:
        :return:
        """
        try:
            self.db.execute("DROP TABLE %s" % table_name)
        except Exception as e:
            pass
