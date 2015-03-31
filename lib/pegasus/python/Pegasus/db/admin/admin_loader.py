__author__ = "Rafael Ferreira da Silva"

import logging
import datetime
import os

from Pegasus.db.schema import *
from Pegasus.tools import properties
from sqlalchemy.orm.exc import *
from urlparse import urlparse

log = logging.getLogger(__name__)

#-------------------------------------------------------------------
# DB Admin configuration
#-------------------------------------------------------------------
CURRENT_DB_VERSION = 4

COMPATIBILITY = {
    '4.3.0': 1, '4.3.1': 1, '4.3.2': 1,
    '4.4.0': 2, '4.4.1': 2, '4.4.2': 2,
    '4.5.0': 4
}
#-------------------------------------------------------------------

def get_compatible_version(version):
    print_version = None
    previous_version = None
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
def db_get_uri(config_properties=None, db_type=None, dburi=None):
    """ Database connection """
    if not dburi:
        if config_properties and db_type:
            props = properties.Properties()
            props.new(config_file=config_properties)
            
            if db_type.upper() == "JDBCRC":
                dburi = _get_jdbcrc_uri(props)
            elif db_type.upper() == "MASTER":
                dburi = _get_master_uri(props)
            elif db_type.upper() == "WORKFLOW":
                dburi = _get_workflow_uri(props)
            else:
                log.error("Invalid database type '%s'." % db_type)
                raise RuntimeError("Invalid database type '%s'." % db_type)

        else:
            dburi = _get_master_uri()

    if dburi:
        log.debug("Using database: %s" % dburi)
        return dburi
    
    log.error("Unable to find a database URI to connect.")
    raise RuntimeError("Unable to find a database URI to connect.")


def db_create(dburi, engine):
    """ Create the Pegasus database from the schema """
    try:
        metadata.create_all(engine)
        data = engine.execute("SELECT * FROM dbversion").fetchone()
        if not data:
            engine.execute(db_version.insert(), version_number=CURRENT_DB_VERSION, 
                version_timestamp=datetime.datetime.now().strftime("%s"))
            log.info("Created Pegasus database in: %s" % dburi)
            
    except OperationalError, e:
        if "mysql" in dburi and "unknown database" in str(e).lower():
            log.error("MySQL database should be previously created.")
            raise RuntimeError(e)
    

def db_current_version(db, parse=False):
    """ Get the current version of the database."""
    _verify_tables(db)
    current_version = None

    try:
        current_version = _get_version(db)
    except NoResultFound:
        current_version = _discover_version(db)

    if parse:
        current_version = get_compatible_version(current_version)
        if not current_version:
            log.error("Your database is not compatible with any Pegasus version.")
            log.error("Use 'pegasus-db-admin check' to verify its compatibility.")
            raise RuntimeError("Your database is not compatible with any Pegasus version.")

    return current_version


def db_verify(db, pegasus_version=None, parse=False, verbose=False):
    """ Verify whether the database is compatible to the specified 
        Pegasus version."""
    _verify_tables(db)
    version = _parse_pegasus_version(pegasus_version)       

    compatible = False
    try:
        compatible = _check_version(db, version)
    except NoResultFound:
        _discover_version(db)
        compatible = _check_version(db, version)

    if verbose:
        friendly_version = version
        if parse:
            if pegasus_version:
                friendly_version = pegasus_version
            else:
                friendly_version = get_compatible_version(version)

        if compatible:
            log.info("Your database is compatible with version %s." % friendly_version)
        else:
            log.error("Your database is NOT compatible with version %s." % friendly_version)
            current_version = db_current_version(db)
            command = "update"
            if current_version > version:
                command = "downgrade"
            if version == CURRENT_DB_VERSION:
                log.error("Use 'pegasus-db-admin %s' to %s your database." % (command, command))
            else:
                log.error("Use 'pegasus-db-admin %s -V %s' to %s your database." % (command, friendly_version, command))
            raise RuntimeError("Your database is NOT compatible with version %s." % friendly_version)

    return compatible


def db_update(db, pegasus_version=None, force=False):
    """ Update the database. """
    _verify_tables(db)

    current_version = db_current_version(db)
    version = _parse_pegasus_version(pegasus_version)

    if current_version > version:
        log.error("Unable to run update. Current database version is newer than specified version '%s'." % (pegasus_version))
        raise RuntimeError("Unable to run update. Current database version is newer than specified version '%s'." % (pegasus_version))

    for i in range(current_version + 1, version + 1):
        k = get_class(i, db)
        k.update(force)
        _update_version(db, i)
    log.info("Your database was successfully updated.")


def db_downgrade(db, pegasus_version=None, force=False):
    """ Downgrade the database. """
    _verify_tables(db)

    current_version = db_current_version(db)
    if pegasus_version:
        version = _parse_pegasus_version(pegasus_version)
    else:
        version = current_version - 1

    if version == 0:
        log.info("Your database is already downgraded to the minimum version.")
        return

    previous_version = 'Z'
    for ver in COMPATIBILITY:
        if COMPATIBILITY[ver] <= version and ver < previous_version:
            version = COMPATIBILITY[ver]
            previous_version = ver
            break

    if current_version < version:
        log.error("Unable to run downgrade. Current database version is older than specified version '%s'." % (pegasus_version))
        raise RuntimeError("Unable to run downgrade. Current database version is older than specified version '%s'." % (pegasus_version))

    for i in range(current_version, version, -1):
        k = get_class(i, db)
        k.downgrade(force)
        _update_version(db, i - 1)
    log.info("Your database was successfully downgraded.")


################################################################################

def _get_jdbcrc_uri(props=None):
    """ Get JDBCRC URI """
    if props:
        replica_catalog = props.property('pegasus.catalog.replica')
        if not replica_catalog:
            log.error("'pegasus.catalog.replica' property not set.")
            raise RuntimeError("'pegasus.catalog.replica' property not set.")
        
        if replica_catalog != "JDBCRC":
            return None

        rc_info = {
            "driver" : props.property('pegasus.catalog.replica.db.driver'),
            "url" : props.property('pegasus.catalog.replica.db.url'),
            "user" : props.property('pegasus.catalog.replica.db.user'),
            "password" : props.property('pegasus.catalog.replica.db.password'),
        }

        url = rc_info["url"]
        if not url:
            log.error("'pegasus.catalog.replica.db.url' property not set.")
            raise RuntimeError("'pegasus.catalog.replica.db.url' property not set.")
        url = url.replace("jdbc:", "")
        o = urlparse(url)
        host = o.netloc
        database = o.path.replace("/", "")

        driver = rc_info["driver"]
        if not driver:
            log.error("'pegasus.catalog.replica.db.driver' property not set.")
            raise RuntimeError("'pegasus.catalog.replica.db.driver' property not set.")
        
        if driver.lower() == "mysql":
            return "mysql://" + rc_info["user"] + ":" + rc_info["password"] + "@" + host + "/" + database

        if driver.lower() == "sqlite":
            connString = os.path.join(host, "workflow.db")
            return "sqlite:///" + connString

        if driver.lower() == "postgresql":
            return "postgresql://" + rc_info["user"] + ":" + rc_info["password"] + "@" + host + "/" + database

        log.error("Invalid JDBCRC driver: %s" % rc_info["driver"])
    return None
    
    
def _get_master_uri(props=None):
    """ Get MASTER URI """
    if props:
        dburi = props.property('pegasus.catalog.master.url')
        if dburi:
            dburi = dburi.replace("jdbc:", "")
            return dburi
        dburi = props.property('pegasus.dashboard.output')
        if dburi:
            dburi = dburi.replace("jdbc:", "")
            return dburi

    homedir = os.getenv("HOME", None)
    dburi = os.path.join(homedir, ".pegasus", "workflow.db")
    pegasusDir = os.path.dirname(dburi)
    if not os.path.exists(pegasusDir):
        os.mkdir(pegasusDir)
    return "sqlite:///" + dburi
    
    
def _get_workflow_uri(props=None):
    """ Get WORKFLOW URI """
    if props:
        dburi = props.property('pegasus.catalog.workflow.url')
        if dburi:
            dburi = dburi.replace("jdbc:", "")
            return dburi
        dburi = props.property('pegasus.monitord.output')
        if dburi:
            dburi = dburi.replace("jdbc:", "")
            return dburi
    return None


def _get_version(db):
    current_version = db.query(DBVersion.version_number).order_by(
        DBVersion.id.desc()).first()
    if not current_version:
        raise NoResultFound()
    return current_version[0]


def _discover_version(db):
    version = 0
    for i in range(1, CURRENT_DB_VERSION + 1):
        k = get_class(i, db)
        k.update()
        version = i

    if version > 0:
        _update_version(db, version)
    return version


def _parse_pegasus_version(pegasus_version):
    version = None
    if pegasus_version:
        for key in COMPATIBILITY:
            if key == pegasus_version:
                version = COMPATIBILITY[key]
                break
        if not version:
            log.error("Version does not exist: %s." % pegasus_version)
            raise RuntimeError("Version does not exist: %s." % pegasus_version)

    if not version:
        version = CURRENT_DB_VERSION
    return version


def _check_version(db, version):
    db_version = _get_version(db)
    if db_version and not version == db_version:
        return False
    return True


def _update_version(db, version):
    v = DBVersion()
    v.version_number = version
    v.version_timestamp = datetime.datetime.now().strftime("%s")
    if db:
        db.add(v)
        db.commit()

def _verify_tables(db):
    try:
        db.execute("SELECT * FROM dbversion")
    except:
        log.error("Non-existent or missing database tables.")
        log.error("Run 'pegasus-db-admin create' to create the missing tables.")
        raise RuntimeError("Non-existent or missing database tables.")
