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

import datetime
import logging
import os
import shutil
import subprocess
import sys
import time
import warnings

from sqlalchemy import func
from sqlalchemy.exc import OperationalError, ProgrammingError
from sqlalchemy.orm.exc import NoResultFound

from Pegasus.db import connection
from Pegasus.db.schema import *
from Pegasus.db.schema import check_table_exists, get_missing_tables, metadata

__author__ = "Rafael Ferreira da Silva"

log = logging.getLogger(__name__)

# -------------------------------------------------------------------
# DB Admin configuration
# -------------------------------------------------------------------
CURRENT_DB_VERSION = 14
DB_MIN_VERSION = 8

COMPATIBILITY = {
    "4.3.0": 1,
    "4.3.1": 1,
    "4.3.2": 1,
    "4.4.0": 2,
    "4.4.1": 2,
    "4.4.2": 2,
    "4.5.0": 4,
    "4.5.1": 4,
    "4.5.2": 4,
    "4.5.3": 4,
    "4.5.4": 5,
    "4.6.0": 6,
    "4.6.1": 6,
    "4.6.2": 6,
    "4.7.0": 8,
    "4.7.3": 8,
    "4.8.0": 8,
    "4.8.1": 8,
    "4.8.2": 8,
    "4.8.3": 8,
    "4.9.0": 11,
    "4.9.1": 11,
    "4.9.2": 11,
    "4.9.3": 11,
    "5.0.0": 14,
    "5.0.1": 14,
}


# -------------------------------------------------------------------


class DBAdminError(Exception):
    def __init__(self, message, db=None, db_version=None, given_version=None):
        """
        :param message: Exception message
        :param db: DB session object
        :param db_version: Current DB version (integer)
        :param given_version: Provided pegasus version
        """
        super().__init__(message)

        self.db = db
        self.db_version = db_version
        self.given_version = given_version

        if db:
            self.dburi = db.get_bind().url
            if not db_version:
                try:
                    self.db_version = get_version(db)
                except (NoResultFound, ProgrammingError):
                    pass
            self.db_compatible_version = get_compatible_version(self.db_version)

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
        pegasus_version = None

        # PM-1596 - Python codes now get PEGASUS_HOME set correctly
        if "PEGASUS_HOME" in os.environ:
            f = os.path.join(os.environ.get("PEGASUS_HOME"), "bin/pegasus-version")
            if os.path.isfile(f):
                pegasus_version = f

        if not pegasus_version:
            f = os.path.join(os.path.dirname(sys.argv[0]), "pegasus-version")
            if os.path.isfile(f):
                pegasus_version = f

        if pegasus_version:
            child = subprocess.Popen(
                pegasus_version,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                shell=True,
                cwd=os.getcwd(),
            )
            out, err = child.communicate()
            if child.returncode != 0:
                raise DBAdminError(err.decode("utf8").strip())
            return out.decode("utf8").strip()

    print_version = None
    previous_version = None

    if version is not None and int(version) >= CURRENT_DB_VERSION:
        pv = -1
        for ver in COMPATIBILITY:
            if COMPATIBILITY[ver] > pv and (
                previous_version is None or ver > previous_version
            ):
                pv = COMPATIBILITY[ver]
                print_version = ver
                previous_version = ver

    for ver in COMPATIBILITY:
        if COMPATIBILITY[ver] == version and (
            previous_version is None or ver > previous_version
        ):
            print_version = ver
            previous_version = ver
    return print_version


def get_class(version, db):
    """
    Get a database version class for update/downgrade.
    :param version: version of the database
    :param db: DB session object
    :return: a database version class
    """
    module = f"Pegasus.db.admin.versions.v{version}"
    mod = __import__(module, fromlist=["Version"])
    klass = getattr(mod, "Version")
    return klass(db)


# -------------------------------------------------------------------
def db_create(
    dburi,
    engine,
    db,
    pegasus_version=None,
    force=False,
    verbose=True,
    print_version=True,
):
    """
    Create/Update the Pegasus database from the schema.
    :param dburi: URL to the db
    :param engine: DB engine object
    :param db: DB session object
    :param pegasus_version: version of the Pegasus software (e.g., 5.0.1)
    :param force: whether operations should be performed despite conflicts
    :param verbose: whether messages should be printed in the prompt
    :param print_version: whether to print the database version
    """
    table_names = engine.table_names()
    db_version = DBVersion.__table__
    db_version.create(engine, checkfirst=True)

    v = 0
    if len(table_names) == 0:
        engine.execute(
            db_version.insert(),
            version=CURRENT_DB_VERSION,
            version_number=int(CURRENT_DB_VERSION),
            version_timestamp=datetime.datetime.now().strftime("%s"),
        )
        if verbose:
            print("Pegasus database was successfully created.")
    else:
        v = _discover_version(
            db, pegasus_version=pegasus_version, force=force, verbose=False
        )

    try:
        metadata.create_all(engine, checkfirst=True)
    except OperationalError as e:
        raise DBAdminError(e, db=db, given_version=pegasus_version)

    if verbose and v > 0:
        print(f"Database has been updated: {dburi}")

    print_db_version(print_version, v if v > 0 else CURRENT_DB_VERSION, db, parse=True)


def db_verify(db, check=False, pegasus_version=None, force=False, print_version=True):
    """
    Verify whether the database is compatible to the specified Pegasus version.
    :param db: DB session object
    :param check: whether to check database compatibility
    :param pegasus_version: version of the Pegasus software (e.g., 5.0.1)
    :param force: whether operations should be performed despite conflicts
    :param print_version: whether to print the database version
    """
    _has_version_table(db, pegasus_version)
    db_version = get_version(db)

    if check:
        _verify_tables(db, db_version)
        version = parse_pegasus_version(pegasus_version)

        if (
            db_version
            and db_version <= CURRENT_DB_VERSION
            and not version == db_version
        ):
            raise DBAdminError(
                "Database is NOT compatible with version: {} ({})".format(
                    get_compatible_version(version), db.get_bind().url
                ),
                db=db,
                db_version=db_version,
                given_version=pegasus_version,
            )


def db_downgrade(db, pegasus_version=None, force=False, verbose=True):
    """
    Downgrade the database.
    :param db: DB session object
    :param pegasus_version: version of the Pegasus software (e.g., 4.6.0)
    :param force: whether operations should be performed despite conflicts
    :param verbose: whether messages should be printed in the prompt
    """
    _has_version_table(db, pegasus_version)
    try:
        current_version = get_version(db, sanity_check=False)
    except NoResultFound:
        raise DBAdminError(
            "Unable to determine database version.",
            db=db,
            given_version=pegasus_version,
        )

    if pegasus_version:
        version = parse_pegasus_version(pegasus_version)
    else:
        previous_version = ""
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
            db_version=current_version,
            given_version=pegasus_version,
        )
    elif version < DB_MIN_VERSION:
        raise DBAdminError(
            "Cannot downgrade database to a version below Pegasus %s."
            % get_compatible_version(DB_MIN_VERSION),
            db=db,
            db_version=current_version,
            given_version=pegasus_version,
        )

    # backup the database before making changes
    _backup_db(db)

    for i in range(int(current_version), int(version) - 1, -1):

        if i == int(current_version):
            max_range = _get_minor_version(current_version)
        else:
            max_range = _get_max_minor_version(i)

        for j in range(max_range, 0, -1):
            k = get_class(f"{i}-{j}", db)
            k.downgrade(force)
            actual_version = float(f"{i}.{j - 1}")
            _update_version(db, actual_version)

        if i > version:
            k = get_class(i, db)
            k.downgrade(force)
            actual_version = float(f"{i - 1}.{_get_max_minor_version(i - 1)}")
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
    if pegasus_version == 0 or pegasus_version:
        if pegasus_version in COMPATIBILITY:
            return COMPATIBILITY[pegasus_version]
        raise DBAdminError(
            f"Version does not exist: {pegasus_version}.",
            given_version=pegasus_version,
        )
    return CURRENT_DB_VERSION


def all_workflows_db(
    db, update=True, pegasus_version=None, schema_check=True, force=False
):
    """
    Update/Downgrade all completed workflow databases listed in master_workflow table.
    :param db: DB session object
    :param pegasus_version: version of the Pegasus software (e.g., 5.0.1)
    :param schema_check: whether a sanity check of the schema should be performed
    :param force: whether operations should be performed despite conflicts
    """
    # log files
    file_prefix = "%s-dbadmin" % time.strftime("%Y%m%dT%H%M%S")
    f_out = open("%s.out" % file_prefix, "w")
    f_err = open("%s.err" % file_prefix, "w")

    data = (
        db.query(
            MasterWorkflow.db_url,
            MasterWorkflowstate.state,
            func.max(MasterWorkflowstate.timestamp),
        )
        .join(MasterWorkflowstate)
        .group_by(MasterWorkflow.wf_id)
        .all()
    )

    db_urls = []
    for d in data:
        if d[1] == "WORKFLOW_TERMINATED":
            db_urls.append(d[0])
            f_err.write("[ACTIVE] %s\n" % d[0])

    counts = {
        "total": len(data),
        "running": len(data) - len(db_urls),
        "success": 0,
        "failed": 0,
        "unable_to_connect": 0,
    }
    if update:
        msg = ["updating", "Updated"]
    else:
        msg = ["downgrading", "Downgraded"]

    print("")
    print("Verifying and %s workflow databases:" % msg[0])
    i = counts["running"]
    for dburi in db_urls:
        log.debug(f"{msg[0]} '{dburi}'...")
        i += 1
        sys.stdout.write("\r%d/%d" % (i, counts["total"]))
        sys.stdout.flush()
        try:
            if update:
                con = connection.connect(
                    dburi,
                    pegasus_version=pegasus_version,
                    schema_check=schema_check,
                    create=True,
                    force=force,
                    verbose=False,
                )
            else:
                con = connection.connect(
                    dburi, schema_check=schema_check, create=False, verbose=False
                )
                metadata.clear()
                warnings.simplefilter("ignore")
                metadata.reflect(bind=con.get_bind())
                db_downgrade(
                    con, pegasus_version=pegasus_version, force=force, verbose=False
                )
            con.close()
            f_out.write("[SUCCESS] %s\n" % dburi)
            counts["success"] += 1
        except connection.ConnectionError as e:
            if "unable to open database file" in str(e):
                f_err.write("[UNABLE TO CONNECT] %s\n" % dburi)
                counts["unable_to_connect"] += 1
                log.debug(e)
            else:
                f_err.write("[ERROR] %s\n" % dburi)
                counts["failed"] += 1
                log.debug(e)
        except Exception as e:
            f_err.write("[ERROR] %s\n" % dburi)
            counts["failed"] += 1
            log.debug(e)

    f_out.close()
    f_err.close()

    print("\n\nSummary:")
    print("  Verified/{}: {}/{}".format(msg[1], counts["success"], counts["total"]))
    print("  Failed: {}/{}".format(counts["failed"], counts["total"]))
    print(
        "  Unable to connect: {}/{}".format(
            counts["unable_to_connect"], counts["total"]
        )
    )
    print(
        "  Unable to update (active workflows): %s/%s"
        % (counts["running"], counts["total"])
    )
    print("\nLog files:")
    print("  %s.out (Succeeded operations)" % file_prefix)
    print("  %s.err (Failed operations)" % file_prefix)


def print_db_version(print_version, db_version, db, parse=True):
    """
    Print the database version
    :param print_version: whether to print the database version
    :param db_version: Current DB version (integer)
    :param db: DB session object
    :param parse: whether database version should be presented as a version of the Pegasus software
    """
    if print_version:
        current_version = _db_current_version(db_version, db, parse=True)
        print(f"Database version: '{current_version}' ({db.get_bind().url})")


def get_version(db, sanity_check=True):
    """
    Get the DB version.
    :param db: DB session object
    :param sanity_check:
    :return: the DB current version (integer) or -1 if not found
    """
    try:
        current_version = (
            db.query(DBVersion.version).order_by(DBVersion.id.desc()).first()
        )
        if not current_version:
            log.debug("No version record found on dbversion table.")
            return -1

        if sanity_check:
            _version_sanity_check(db, current_version[0])

        return float(current_version[0])

    except OperationalError:
        return -1


################################################################################


def _db_current_version(current_version, db, parse=False):
    """
    Get the current version of the database.
    :param current_version:
    :param db: DB session object
    :param parse: whether database version should be presented as a version of the Pegasus software
    :return: current version of the database
    """
    if parse:
        current_version = get_compatible_version(current_version)
        if not current_version:
            raise DBAdminError(
                "Your database is not compatible with any Pegasus version.\nRun 'pegasus-db-admin "
                "update {}' to update it to the latest version.".format(
                    db.get_bind().url
                )
            )

    return current_version


def _discover_version(db, pegasus_version=None, force=False, verbose=True):
    """
    Discover Pegasus database version and update it if not the latest.
    :param db: DB session object
    :param pegasus_version: version of the Pegasus software (e.g., 5.0.1)
    :param force: whether operations should be performed despite conflicts
    :param verbose: whether messages should be printed in the prompt
    :return: database version
    """
    version = parse_pegasus_version(pegasus_version)

    current_version = 0
    if not force:
        current_version = get_version(db)

    if current_version == version:
        try:
            _verify_tables(db, current_version)
            log.debug(f"Database is already updated: {db.get_bind().url}")
            return 0
        except DBAdminError:
            current_version = 0

    _version_sanity_check(db, current_version)

    if current_version > version:
        raise DBAdminError(
            "Unable to run update. Current database version is newer than specified version '{}'.".format(
                pegasus_version
            ),
            db=db,
            db_version=current_version,
            given_version=pegasus_version,
        )

    # update database
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
                k = get_class(f"{i}-{j}", db)
                k.update(force=force)
                v = float(f"{i}.{j}")
            except ImportError:
                break

    if v > current_version:
        _update_version(db, v)
        if verbose:
            print(f"Database has been updated: {db.get_bind().url}")
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
    # Backup SQLite database
    if url.drivername == "sqlite" and str(url).lower() != "sqlite://":
        dest_file = "{}-{}".format(url.database, time.strftime("%Y%m%d-%H%M%S"))
        shutil.copy(url.database, dest_file)
        log.debug(f"Created backup database file at: {dest_file}")

    elif url.drivername == "mysql" or url.drivername == "postgresql":
        # Backup MySQL database
        if url.drivername == "mysql":
            log.info("Backing up MySQL database. This operation may take a while.")
            # mysqldump command preparation
            command = (
                "mysqldump"
                if not url.password
                else f"export MYSQL_PWD={url.password}; mysqldump"
            )
            if url.username:
                command += f" -u {url.username}"

        # Backup PostgreSQL database
        elif url.drivername == "postgresql":
            log.info("Backing up PostgreSQL database. This operation may take a while.")
            # pg_dump command preparation
            command = (
                "pg_dump"
                if not url.password
                else f"export PGPASSWORD={url.password}; pg_dump"
            )
            if url.username:
                command += f" -U {url.username}"

        if url.host:
            command += f" -h {url.host}"

        dest_file = "{}-{}.sql".format(url.database, time.strftime("%Y%m%d-%H%M%S"))
        command += f" {url.database} > {dest_file}"
        child = subprocess.Popen(
            command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True
        )
        out, err = child.communicate()
        if child.returncode != 0:
            if "Error 2013" in err:
                err += "\nPlease, refer to the pegasus-db-admin troubleshooting for possible ways to fix this error."
            raise DBAdminError(err, db=db)
        log.debug(f"Created backup database file at: {dest_file}")


def _verify_tables(db, db_version=None):
    try:
        missing_tables = get_missing_tables(db)
    except Exception as e:
        raise DBAdminError(e, db_version=db_version, db=db)

    if len(missing_tables) > 0:
        raise DBAdminError(
            "Missing database tables or tables are not updated:\n    %s\n"
            "Run 'pegasus-db-admin update %s' to create/update your database."
            % (" \n    ".join(missing_tables), db.get_bind().url),
            db=db,
            db_version=db_version,
        )


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
    """
    Verify whether db version is higher than current version.
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
            db_version=version,
        )


def _has_version_table(db, pegasus_version=None):
    """
    Verify whether version table exists.
    :param db: db connection
    :param pegasus_version: version of the Pegasus software (e.g., 5.0.1)
    """
    if not check_table_exists(db, DBVersion):
        raise DBAdminError(
            "Unable to determine database version.",
            db=db,
            db_version=CURRENT_DB_VERSION,
            given_version=pegasus_version,
        )
