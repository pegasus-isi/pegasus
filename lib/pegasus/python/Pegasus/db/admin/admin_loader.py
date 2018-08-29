#!/usr/bin/env python
#
#  Copyright 2017-2018 University Of Southern California
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
from __future__ import print_function

import datetime
import glob
import logging
import os
import shutil
import subprocess
import sys
import time
import warnings

from Pegasus.db import connection
from Pegasus.db.schema import *
from sqlalchemy import func
from sqlalchemy.orm.exc import *

__author__ = "Rafael Ferreira da Silva"

log = logging.getLogger(__name__)

# -------------------------------------------------------------------
# DB Admin configuration
# -------------------------------------------------------------------
CURRENT_DB_VERSION = 11
DB_MIN_VERSION = 4

COMPATIBILITY = {
    '4.3.0': 1,
    '4.3.1': 1,
    '4.3.2': 1,
    '4.4.0': 2,
    '4.4.1': 2,
    '4.4.2': 2,
    '4.5.0': 4,
    '4.5.1': 4,
    '4.5.2': 4,
    '4.5.3': 4,
    '4.5.4': 5,
    '4.6.0': 6,
    '4.6.1': 6,
    '4.6.2': 6,
    '4.7.0': 8,
    '4.7.3': 8,
    '4.8.0': 8,
    '4.8.1': 8,
    '4.8.2': 8,
    '4.8.3': 8,
    '4.9.0': 11
}


# -------------------------------------------------------------------


class DBAdminError(Exception):
    def __init__(self, message, db=None, db_version=None, given_version=None):
        """
        :param message: Exception message
        :param db: DB session object
        :param dburi: DB URI
        :param db_version: Current DB version (integer)
        :param db_compatible_version: Current DB version (pegasus version)
        :param given_version: Provided pegasus version
        :param pegasus_version: Pegasus DB version (integer)
        :param pegasus_compatible_version: Pegasus DB version (integer)
        """
        super(DBAdminError, self).__init__(message)

        self.db = db
        self.db_version = db_version
        self.given_version = given_version

        if db:
            self.dburi = db.get_bind().url
            if not db_version:
                try:
                    self.db_version = _get_version(db)
                except NoResultFound:
                    pass
            self.db_compatible_version = get_compatible_version(
                self.db_version
            )

            if given_version:
                self.pegasus_version = COMPATIBILITY[given_version]
                self.pegasus_compatible_version = given_version
            else:
                self.pegasus_version = CURRENT_DB_VERSION
                self.pegasus_compatible_version = get_compatible_version(
                    CURRENT_DB_VERSION
                )


def get_compatible_version(version):
    """
    Get a compatible Pegasus version for the database version.
    :param version: version of the database
    :return: the equivalent Pegasus version
    """
    if version == CURRENT_DB_VERSION:
        # find pegasus-version path
        pegasus_version = None
        if 'PATH' in os.environ:
            paths = os.environ.get('PATH').split(os.pathsep)
            for p in paths:
                f = os.path.join(p, 'pegasus-version')
                if os.path.isfile(f):
                    pegasus_version = f
                    break

        if not pegasus_version and 'PEGASUS_HOME' in os.environ:
            f = os.path.join(os.environ.get('PEGASUS_HOME'), 'bin/pegasus-version')
            if os.path.isfile(f):
                pegasus_version = f

        if not pegasus_version:
            f = os.path.join(os.path.dirname(sys.argv[0]), 'pegasus-version')
            if os.path.isfile(f):
                pegasus_version = f

        if pegasus_version:
            out, err = subprocess.Popen(
                pegasus_version,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                shell=True,
                cwd=os.getcwd()
            ).communicate()
            if err:
                raise DBAdminError(err.decode('utf8').strip())
            return out.decode('utf8').strip()

    print_version = None
    previous_version = None

    if version >= CURRENT_DB_VERSION:
        pv = -1
        for ver in COMPATIBILITY:
            if COMPATIBILITY[ver] > pv and ver > previous_version:
                pv = COMPATIBILITY[ver]
                print_version = ver
                previous_version = ver

    for ver in COMPATIBILITY:
        if COMPATIBILITY[ver] == version and ver > previous_version:
            print_version = ver
            previous_version = ver
    return print_version


def get_class(version, db):
    module = "Pegasus.db.admin.versions.v%s" % version
    mod = __import__(module, fromlist=["Version"])
    klass = getattr(mod, "Version")
    return klass(db)


# -------------------------------------------------------------------
def db_create(dburi, engine, db, pegasus_version=None, force=False, verbose=True):
    """
    Create/Update the Pegasus database from the schema.
    :param dburi: URL to the db
    :param engine: DB engine object
    :param db: DB session object
    :param pegasus_version: version of the Pegasus software (e.g., 4.6.0)
    :param force: whether operations should be performed despite conflicts
    :param verbose: whether messages should be printed in the prompt
    """
    table_names = engine.table_names(connection=db)
    db_version.create(engine, checkfirst=True)

    v = -1
    if len(table_names) == 0:
        engine.execute(
            db_version.insert(),
            version=CURRENT_DB_VERSION,
            version_number=int(CURRENT_DB_VERSION),
            version_timestamp=datetime.datetime.now().strftime("%s")
        )
        if verbose:
            print("Created Pegasus database in: %s" % dburi)
    else:
        v = _discover_version(
            db, pegasus_version=pegasus_version, force=force, verbose=False
        )

    try:
        metadata.create_all(engine)
    except OperationalError as e:
        raise DBAdminError(e, db=db, given_version=pegasus_version)
    if verbose and v > 0:
        print("Your database has been updated.")


def db_current_version(db, parse=False, force=False):
    """
    Get the current version of the database.
    :param db: DB session object
    :param parse: whether database version should be presented as a version of the Pegasus software
    :param force: whether operations should be performed despite conflicts
    :return: current version of the database
    """
    try:
        current_version = _get_version(db)
    except NoResultFound:
        current_version = _discover_version(db, force=force)

    if parse:
        current_version = get_compatible_version(current_version)
        if not current_version:
            raise DBAdminError(
                "Your database is not compatible with any Pegasus version.\nRun 'pegasus-db-admin "
                "update %s' to update it to the latest version." %
                db.get_bind().url
            )

    return current_version


def db_verify(db, pegasus_version=None, force=False):
    """
    Verify whether the database is compatible to the specified Pegasus version.
    :param db: DB session object
    :param pegasus_version: version of the Pegasus software (e.g., 4.6.0)
    :param force: whether operations should be performed despite conflicts
    """
    _verify_tables(db)
    version = parse_pegasus_version(pegasus_version)

    try:
        db_version = _get_version(db)

    except NoResultFound:
        db_version = _discover_version(
            db, pegasus_version=pegasus_version, force=force
        )

    if db_version and db_version <= CURRENT_DB_VERSION and not version == db_version:
        raise DBAdminError(
            "Your database is NOT compatible with version %s" %
            get_compatible_version(version),
            db=db,
            given_version=pegasus_version
        )


def db_downgrade(db, pegasus_version=None, force=False, verbose=True):
    """
    Downgrade the database.
    :param db: DB session object
    :param pegasus_version: version of the Pegasus software (e.g., 4.6.0)
    :param force: whether operations should be performed despite conflicts
    :param verbose: whether messages should be printed in the prompt
    """
    if not check_table_exists(db, db_version):
        raise DBAdminError(
            "Unable to determine database version.",
            db=db,
            given_version=pegasus_version
        )

    try:
        current_version = _get_version(db)
    except NoResultFound:
        raise DBAdminError(
            "Unable to determine database version.",
            db=db,
            given_version=pegasus_version
        )

    if pegasus_version:
        version = parse_pegasus_version(pegasus_version)
    else:
        previous_version = ''
        for ver in COMPATIBILITY:
            if COMPATIBILITY[ver] < current_version and ver > previous_version:
                version = COMPATIBILITY[ver]
                previous_version = ver

    if current_version == version:
        log.info("Your database is already downgraded.")
        return
    elif current_version < version:
        raise DBAdminError(
            "Cannot downgrade to a higher version.",
            db=db,
            given_version=pegasus_version
        )
        return

    if version < DB_MIN_VERSION:
        raise DBAdminError(
            "Your database is already downgraded to the minimum version.",
            db=db,
            given_version=pegasus_version
        )

    # backup the database before making changes
    _backup_db(db)

    for i in range(int(current_version), int(version) - 1, -1):

        if i == int(current_version):
            max_range = _get_minor_version(current_version)
        else:
            max_range = _get_max_minor_version(i)

        for j in range(max_range, 0, -1):
            k = get_class("%s-%s" % (i, j), db)
            k.downgrade(force)
            actual_version = float("%s.%s" % (i, j - 1))
            _update_version(db, actual_version)

        if (i > version):
            k = get_class(i, db)
            k.downgrade(force)
            actual_version = float(
                "%s.%s" % (i - 1, _get_max_minor_version(i - 1))
            )
            _update_version(db, actual_version)

        if actual_version == version:
            break

    if verbose:
        print("Your database was successfully downgraded.")


def parse_pegasus_version(pegasus_version=None):
    """
    Get database version associated to the Pegasus version.
    :param pegasus_version: version of the Pegasus software (e.g., 4.6.0)
    :return: database version
    """
    version = None
    if pegasus_version == 0 or pegasus_version:
        for key in COMPATIBILITY:
            if key == pegasus_version:
                return COMPATIBILITY[key]
        if not version:
            raise DBAdminError(
                "Version does not exist: %s." % pegasus_version,
                given_version=pegasus_version
            )

    if not version:
        return CURRENT_DB_VERSION


def all_workflows_db(
        db, update=True, pegasus_version=None, schema_check=True, force=False
):
    """
    Update/Downgrade all completed workflow databases listed in master_workflow table.
    :param db: DB session object
    :param pegasus_version: version of the Pegasus software (e.g., 4.6.0)
    :param schema_check: whether a sanity check of the schema should be performed
    :param force: whether operations should be performed despite conflicts
    """
    # log files
    file_prefix = "%s-dbadmin" % time.strftime("%Y%m%dT%H%M%S")
    f_out = open("%s.out" % file_prefix, 'w')
    f_err = open("%s.err" % file_prefix, 'w')

    data = db.query(
        DashboardWorkflow.db_url, DashboardWorkflowstate.state,
        func.max(DashboardWorkflowstate.timestamp)
    ).join(DashboardWorkflowstate).group_by(DashboardWorkflow.wf_id).all()

    db_urls = []
    for d in data:
        if d[1] == "WORKFLOW_TERMINATED":
            db_urls.append(d[0])
            f_err.write("[ACTIVE] %s\n" % d[0])

    counts = {
        'total': len(data),
        'running': len(data) - len(db_urls),
        'success': 0,
        'failed': 0,
        'unable_to_connect': 0,
    }
    if update:
        msg = ['updating', 'Updated']
    else:
        msg = ['downgrading', 'Downgraded']

    print("")
    print("Verifying and %s workflow databases:" % msg[0])
    i = counts['running']
    for dburi in db_urls:
        log.debug("%s '%s'..." % (msg[0], dburi))
        i += 1
        sys.stdout.write("\r%d/%d" % (i, counts['total']))
        sys.stdout.flush()
        try:
            if update:
                con = connection.connect(
                    dburi,
                    pegasus_version=pegasus_version,
                    schema_check=schema_check,
                    create=True,
                    force=force,
                    verbose=False
                )
            else:
                con = connection.connect(
                    dburi,
                    schema_check=schema_check,
                    create=False,
                    verbose=False
                )
                metadata.clear()
                warnings.simplefilter("ignore")
                metadata.reflect(bind=con.get_bind())
                db_downgrade(
                    con,
                    pegasus_version=pegasus_version,
                    force=force,
                    verbose=False
                )
            con.close()
            f_out.write("[SUCCESS] %s\n" % dburi)
            counts['success'] += 1
        except connection.ConnectionError as e:
            if "unable to open database file" in str(e):
                f_err.write("[UNABLE TO CONNECT] %s\n" % dburi)
                counts['unable_to_connect'] += 1
                log.debug(e)
            else:
                f_err.write("[ERROR] %s\n" % dburi)
                counts['failed'] += 1
                log.debug(e)
        except Exception as e:
            f_err.write("[ERROR] %s\n" % dburi)
            counts['failed'] += 1
            log.debug(e)

    f_out.close()
    f_err.close()

    print("\n\nSummary:")
    print(
        "  Verified/%s: %s/%s" % (msg[1], counts['success'], counts['total'])
    )
    print("  Failed: %s/%s" % (counts['failed'], counts['total']))
    print(
        "  Unable to connect: %s/%s" %
        (counts['unable_to_connect'], counts['total'])
    )
    print(
        "  Unable to update (active workflows): %s/%s" %
        (counts['running'], counts['total'])
    )
    print("\nLog files:")
    print("  %s.out (Succeeded operations)" % file_prefix)
    print("  %s.err (Failed operations)" % file_prefix)


################################################################################
def _get_version(db):
    current_version = None

    try:
        current_version = db.query(DBVersion.version).order_by(
            DBVersion.id.desc()
        ).first()

    except OperationalError as e:
        # update dbversion table
        # Temporary migration. Should be removed in future releases
        try:
            log.info("Updating dbversion...")
            if db.get_bind().driver == "mysqldb":
                db.execute("RENAME TABLE dbversion TO dbversion_v4")
            else:
                db.execute("ALTER TABLE dbversion RENAME TO dbversion_v4")
            db_version.create(db.get_bind(), checkfirst=True)
            db.execute(
                "INSERT INTO dbversion(version_number, version, version_timestamp) SELECT version_number, "
                "version_number, version_timestamp FROM dbversion_v4 ORDER BY id"
            )
            db.execute("DROP TABLE dbversion_v4")
            db.commit()
            current_version = db.query(DBVersion.version).order_by(
                DBVersion.id.desc()
            ).first()

        except (OperationalError, ProgrammingError) as e:
            pass
        except Exception as e:
            db.rollback()
            raise DBAdminError(e, db=db)

    if not current_version:
        log.debug("No version record found on dbversion table.")
        raise NoResultFound()

    _version_sanity_check(db, current_version[0])

    return float(current_version[0])


def _discover_version(db, pegasus_version=None, force=False, verbose=True):
    version = parse_pegasus_version(pegasus_version)

    current_version = 0
    if not force:
        try:
            current_version = _get_version(db)
        except NoResultFound:
            pass

    if current_version == version:
        try:
            _verify_tables(db)
            log.debug("Your database is already updated.")
            return None
        except DBAdminError:
            current_version = 0

    _version_sanity_check(db, current_version)

    if current_version > version:
        raise DBAdminError(
            "Unable to run update. Current database version is newer than specified version '%s'."
            % (pegasus_version),
            db=db,
            given_version=pegasus_version
        )

    _backup_db(db)
    v = 0.0
    for i in range(int(current_version), int(version) + 1):
        if not i == int(current_version):
            k = get_class(i, db)
            k.update(force=force)
        v = float(i)

        # verify minor versions
        max_range = 999
        if i == int(version):
            max_range = _get_minor_version(version)

        for j in range(1, max_range + 1):
            try:
                k = get_class("%s-%s" % (i, j), db)
                k.update(force=force)
                v = float("%s.%s" % (i, j))
            except ImportError:
                break

    if v > current_version:
        _update_version(db, v)
        if verbose:
            print("Your database has been updated.")
    else:
        v = 0
    return v


def _update_version(db, version):
    v = DBVersion()
    v.version = version
    v.version_number = int(version)
    v.version_timestamp = datetime.datetime.now().strftime("%s")
    if db:
        db.add(v)
        db.commit()


def _backup_db(db):
    """
    Create a copy of the database (SQLite), or create a dump of the database into a .sql file (MySQL).
    :param db: DB session object
    """
    url = db.get_bind().url
    # Backup SQLite databases
    if url.drivername == "sqlite":
        db_list = glob.glob(url.database + ".[0-9][0-9][0-9]")
        max_index = -1
        for file in db_list:
            index = int(file[-3:])
            if index > max_index:
                max_index = index
        dest_file = url.database + ".%03d" % (max_index + 1)
        shutil.copy(url.database, dest_file)
        log.debug("Created backup database file at: %s" % dest_file)

    # Backup MySQL databases
    elif url.drivername == "mysql":
        log.info("Backing up MySQL database. This operation may take a while.")
        dest_file = "%s-%s.sql" % (
            url.database, time.strftime('%Y%m%d-%H%M%S')
        )
        # mysqldump command preparation
        command = "mysqldump"
        if url.username:
            command += " -u %s" % url.username
        if url.host:
            command += " -h %s" % url.host
        if url.password:
            command += " --password=%s" % url.password
        command += " %s > %s" % (url.database, dest_file)
        out, err = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True
        ).communicate()
        if err:
            if 'Error 2013' in err:
                err += '\nPlease, refer to the pegasus-db-admin troubleshooting for possible ways to fix this error.'
            raise DBAdminError(err, db=db)
        log.debug("Created backup database file at: %s" % dest_file)


def _verify_tables(db):
    try:
        missing_tables = get_missing_tables(db)
        if len(missing_tables) > 0:
            raise DBAdminError(
                "Missing database tables or tables are not updated:\n    %s\n"
                "Run 'pegasus-db-admin update %s' to create/update your database."
                % (" \n    ".join(missing_tables), db.get_bind().url),
                db=db
            )
    except Exception as e:
        raise DBAdminError(e, db=db)


def _get_minor_version(version):
    return int(str(float(version) - int(version))[2:])


def _get_max_minor_version(version):
    max_version = 0
    for ver in COMPATIBILITY:
        minor_version = _get_minor_version(COMPATIBILITY[ver])
        if int(COMPATIBILITY[ver]) == version and minor_version > max_version:
            max_version = minor_version
    return max_version


def _version_sanity_check(db, version):
    """ Verify whether db version is higher than current version.
    :param db: db connection
    :param version: version to be verified
    """
    if float(version) > CURRENT_DB_VERSION:
        raise DBAdminError(
            "You database was created with a newer Pegasus version. "
            "It will not work properly with the current version."
            "\nPlease, run 'pegasus-db-admin downgrade' with the latest Pegasus to downgrade your "
            "database.",
            db=db,
            db_version=version
        )
