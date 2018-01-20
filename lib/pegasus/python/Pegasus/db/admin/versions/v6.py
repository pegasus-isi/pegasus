import logging

from Pegasus.db.admin.admin_loader import *
from Pegasus.db.admin.versions.base_version import BaseVersion
from Pegasus.db.schema import *
from sqlalchemy.exc import *

DB_VERSION = 6

log = logging.getLogger(__name__)


class Version(BaseVersion):
    def __init__(self, connection):
        super(Version, self).__init__(connection)

    def update(self, force=False):
        """

        :param force:
        :return:
        """
        log.info("Updating to version %s" % DB_VERSION)
        # check whether it is safe to do that
        # self.db.execute("DROP TABLE file")

        # check if the migration was interrupted
        interrupted = False
        try:
            self.db.execute("SELECT site FROM rc_lfn_v4 LIMIT 0,1")
            interrupted = True
        except (OperationalError, ProgrammingError):
            pass

        # check if the migration is required
        try:
            if check_table_exists(self.db, rc_lfn) and not interrupted:
                return
        except (OperationalError, ProgrammingError):
            pass
        except Exception as e:
            raise DBAdminError(e)

        # check if previous table exists. If not, the migration should not continue (for new dbs)
        try:
            if not interrupted:
                self.db.execute("SELECT site FROM rc_lfn LIMIT 0,1")
        except (OperationalError, ProgrammingError) as e:
            return
        except Exception as e:
            raise DBAdminError(e)

        # Renaming rc_lfn.id to rc_lfn.lfn_id
        if interrupted:
            log.info("Recovering migration...")
            self._drop_table("rc_pfn")
            self._drop_table("rc_meta")
            self._drop_table("rc_lfn")
        else:
            log.info("Renaming 'rc_lfn' table...")
            if self.db.get_bind().driver == "mysqldb":
                self.db.execute("RENAME TABLE rc_lfn TO rc_lfn_v4")
            else:
                self.db.execute("ALTER TABLE rc_lfn RENAME TO rc_lfn_v4")

        # Create new rc_lfn table and populate it
        log.info("Updating tables...")
        self._create_table(rc_lfn)
        self.db.commit()
        self._create_table(rc_pfn)
        self._create_table(rc_meta)
        self.db.commit()

        try:
            log.info("Updating rc_lfn...")
            self.db.execute(
                "INSERT INTO rc_lfn(lfn) SELECT DISTINCT lfn FROM rc_lfn_v4"
            )
            self.db.commit()
            log.info("Updating rc_pfn...")
            self.db.execute(
                "INSERT INTO rc_pfn(lfn_id, pfn, site) SELECT l.lfn_id, a.pfn, a.site FROM rc_lfn l LEFT JOIN rc_lfn_v4 a ON (l.lfn=a.lfn)"
            )
            self.db.commit()
            log.info("Updating rc_meta...")
            self.db.execute(
                "INSERT INTO rc_meta(lfn_id, key, value) SELECT l.lfn_id, a.name, a.value FROM rc_lfn l LEFT JOIN rc_lfn_v4 b ON (l.lfn=b.lfn) INNER JOIN rc_attr a ON (a.id=b.id)"
            )
            self.db.commit()

        except Exception as e:
            log.info(e)
            self.db.rollback()

        # Drop old tables
        log.info("Cleaning up...")
        self._drop_table("rc_attr")
        self._drop_table("rc_lfn_v4")
        self.db.commit()

    def downgrade(self, force=False):
        """

        :param force:
        :return:
        """
        log.info("Downgrading from version %s" % DB_VERSION)

        log.info("Renaming 'rc_lfn' table...")
        if self.db.get_bind().driver == "mysqldb":
            self.db.execute("RENAME TABLE rc_lfn TO rc_lfn_v5")
        else:
            self.db.execute("ALTER TABLE rc_lfn RENAME TO rc_lfn_v5")
        self.db.commit()

        # Create tables
        log.info("Creating tables...")
        self._drop_index("v4_rc_lfn")
        self._drop_index("v4_rc_attr")
        metadata.remove(rc_lfn)
        v4_rc_lfn = Table(
            'rc_lfn', metadata,
            Column('id', KeyInteger, primary_key=True, nullable=False),
            Column('lfn', VARCHAR(245), nullable=False),
            Column('pfn', VARCHAR(245), nullable=False),
            Column('site', VARCHAR(245)), **table_keywords
        )
        Index(
            'UNIQUE_RC_LFN',
            v4_rc_lfn.c.lfn,
            v4_rc_lfn.c.pfn,
            v4_rc_lfn.c.site,
            unique=True
        )
        Index('v4_rc_lfn', v4_rc_lfn.c.lfn)
        v4_rc_lfn.create(self.db.get_bind(), checkfirst=True)

        v4_rc_attr = Table(
            'rc_attr', metadata,
            Column(
                'id',
                KeyInteger,
                ForeignKey('rc_lfn.id', ondelete='CASCADE'),
                primary_key=True,
                nullable=False
            ), Column('name', VARCHAR(245), primary_key=True, nullable=False),
            Column('value', VARCHAR(245), nullable=False), **table_keywords
        )
        Index('v4_rc_attr', v4_rc_attr.c.name)
        v4_rc_attr.create(self.db.get_bind(), checkfirst=True)
        v4_st_file = Table(
            'file', metadata,
            Column('file_id', KeyInteger, primary_key=True, nullable=False),
            Column(
                'task_id',
                KeyInteger,
                ForeignKey('task.task_id', ondelete='CASCADE'),
                nullable=True
            ), Column('lfn', VARCHAR(255), nullable=True),
            Column('estimated_size', INT, nullable=True),
            Column('md_checksum', VARCHAR(255), nullable=True),
            Column('type', VARCHAR(255), nullable=True), **table_keywords
        )

        Index('file_id_UNIQUE', v4_st_file.c.file_id, unique=True)
        Index('FK_FILE_TASK_ID', st_task.c.task_id, unique=False)
        v4_st_file.create(self.db.get_bind(), checkfirst=True)

        # Migrate entries
        try:
            log.info("Updating rc_lfn...")
            self.db.execute(
                "INSERT INTO rc_lfn(lfn, pfn, site) SELECT l.lfn, a.pfn, a.site FROM (rc_lfn_v5 l INNER JOIN rc_pfn a ON (l.lfn_id=a.lfn_id))"
            )
            self.db.commit()
            log.info("Updating rc_attr...")
            self.db.execute(
                "INSERT INTO rc_attr(id, name, value) SELECT l.id, a.key, a.value FROM rc_lfn l LEFT JOIN rc_lfn_v5 b ON (l.lfn=b.lfn) INNER JOIN rc_meta a ON (a.lfn_id=b.lfn_id)"
            )
            self.db.commit()

        except Exception as e:
            log.info(e)
            self.db.rollback()

        # Drop tables
        log.info("Cleaning up...")
        self._drop_table("rc_pfn")
        self._drop_table("rc_meta")
        self._drop_table("workflow_files")
        self._drop_table("rc_lfn_v5")
        self.db.commit()

    def _create_table(self, table_obj):
        """
        Create a table from its backref.
        :param table_obj:
        :return:
        """
        try:
            table_obj.create(self.db.get_bind(), checkfirst=True)
        except (OperationalError, ProgrammingError) as e:
            pass
        except Exception as e:
            self.db.rollback()
            raise DBAdminError(e)

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

    def _drop_index(self, index_name):
        """
        Drop am index.
        :param index_name:
        :return:
        """
        try:
            self.db.execute("DROP INDEX %s" % index_name)
        except Exception as e:
            pass
