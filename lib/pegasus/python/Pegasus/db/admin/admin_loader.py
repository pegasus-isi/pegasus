__author__ = "Rafael Ferreira da Silva"

import logging
import datetime
import glob
import shutil

from Pegasus.db.schema import *
from sqlalchemy.orm.exc import *

log = logging.getLogger(__name__)

#-------------------------------------------------------------------
# DB Admin configuration
#-------------------------------------------------------------------
CURRENT_DB_VERSION = 5

COMPATIBILITY = {
    '4.3.0': 1, '4.3.1': 1, '4.3.2': 1,
    '4.4.0': 2, '4.4.1': 2, '4.4.2': 2,
    '4.5.0': 5, '4.6.0panorama': 6,
}
#-------------------------------------------------------------------

class DBAdminError(Exception):
    pass


def get_compatible_version(version):
    print_version = None
    previous_version = None

    if version > CURRENT_DB_VERSION:
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


#-------------------------------------------------------------------
def db_create(dburi, engine, db, pegasus_version=None, force=False):
    """ Create/Update the Pegasus database from the schema """
    table_names = engine.table_names()
    db_version.create(engine, checkfirst=True)

    v = -1
    if len(table_names) == 0:
        engine.execute(db_version.insert(), version_number=CURRENT_DB_VERSION, 
                version_timestamp=datetime.datetime.now().strftime("%s"))
        print "Created Pegasus database in: %s" % dburi
    else:
        v = _discover_version(db, pegasus_version=pegasus_version, force=force, verbose=False)
    
    try:
        metadata.create_all(engine)
    except OperationalError, e:
        raise DBAdminError(e)
    if v > 0:
        print "Your database has been updated."
            

def db_current_version(db, parse=False, force=False):
    """ Get the current version of the database."""
    _verify_tables(db)
    current_version = None

    try:
        current_version = _get_version(db)
    except NoResultFound:
        current_version = _discover_version(db, force=force)

    if parse:
        current_version = get_compatible_version(current_version)
        if not current_version:
            raise DBAdminError("Your database is not compatible with any Pegasus version.\nRun 'pegasus-db-admin update %s' to update it to the latest version." % db.get_bind().url)

    return current_version


def db_verify(db, pegasus_version=None, force=False):
    """ Verify whether the database is compatible to the specified 
        Pegasus version."""
    _verify_tables(db)
    version = parse_pegasus_version(pegasus_version)

    compatible = False
    try:
        compatible = _check_version(db, version)
    except NoResultFound:
        _discover_version(db, pegasus_version=pegasus_version, force=force)
        compatible = _check_version(db, version)
    
    if not compatible:
        raise DBAdminError("Your database is NOT compatible with version %s" % get_compatible_version(version))
    
    return compatible


# def db_downgrade(db, pegasus_version=None, force=False):
#     """ Downgrade the database. """
#     _verify_tables(db)
#
#     current_version = db_current_version(db, force=force)
#     if pegasus_version:
#         version = parse_pegasus_version(pegasus_version)
#     else:
#         version = current_version - 1
#
#     if current_version == version:
#         log.debug("Your database is already downgraded.")
#         return
#
#     if version == 0:
#         print "Your database is already downgraded to the minimum version."
#         return
#
#     previous_version = 'Z'
#     for ver in COMPATIBILITY:
#         if COMPATIBILITY[ver] <= version and ver < previous_version:
#             version = COMPATIBILITY[ver]
#             previous_version = ver
#             break
#
#     if current_version < version:
#         raise DBAdminError("Unable to run downgrade. Current database version is older than specified version '%s'." % (pegasus_version))
#
#     _backup_db(db)
#     for i in range(current_version, version, -1):
#         k = get_class(i, db)
#         k.downgrade(force)
#         _update_version(db, i - 1)
#     print "Your database was successfully downgraded."


def parse_pegasus_version(pegasus_version=None):
    version = None
    if pegasus_version == 0 or pegasus_version:
        for key in COMPATIBILITY:
            if key == pegasus_version:
                version = COMPATIBILITY[key]
                break
        if not version:
            raise DBAdminError("Version does not exist: %s." % pegasus_version)

    if not version:
        version = CURRENT_DB_VERSION
    return version


################################################################################
def _get_version(db):
    current_version = db.query(DBVersion.version_number).order_by(
        DBVersion.id.desc()).first()
    if not current_version:
        log.debug("No version record found on dbversion table.")
        raise NoResultFound()
    return current_version[0]


def _discover_version(db, pegasus_version=None, force=False, verbose=True):
    version = parse_pegasus_version(pegasus_version)

    current_version = -1
    if not force:
        try:
            current_version = _get_version(db)
        except NoResultFound:
            pass
    
    if current_version == version or current_version > CURRENT_DB_VERSION:
        try:
            _verify_tables(db)
            log.debug("Your database is already updated.")
            return None
        except DBAdminError:
            current_version = -1
    
    if current_version > version:
        raise DBAdminError("Unable to run update. Current database version is newer than specified version '%s'." % (pegasus_version))
    
    _backup_db(db)
    v = -1
    for i in range(current_version + 1, version + 1):
        k = get_class(i, db)
        k.update(force=force)
        v = i
        
    if v > current_version:
        _update_version(db, i)
        if verbose:
            print "Your database has been updated."
    return v


def _check_version(db, version):
    db_version = _get_version(db)
    if db_version and db_version <= CURRENT_DB_VERSION and not version == db_version:
        return False
    return True


def _update_version(db, version):
    v = DBVersion()
    v.version_number = version
    v.version_timestamp = datetime.datetime.now().strftime("%s")
    if db:
        db.add(v)
        db.commit()


def _backup_db(db):
    url = db.get_bind().url
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
        pass


def _verify_tables(db):
    try:
        missing_tables = get_missing_tables(db)
        if len(missing_tables) > 0:
            raise DBAdminError("Missing database tables or tables are not updated:\n    %s\nRun 'pegasus-db-admin update %s' to create/update your database."
                % (" \n    ".join(missing_tables), db.get_bind().url))
    except Exception, e:
        raise DBAdminError(e)
