__author__ = "Rafael Ferreira da Silva"

import logging
import collections
import datetime
import os

from Pegasus.db import connection
from Pegasus.db.schema import *
from Pegasus.tools import properties
from sqlalchemy.orm.exc import *
from urlparse import urlparse

log = logging.getLogger(__name__)

#-------------------------------------------------------------------
# DB Admin configuration
#-------------------------------------------------------------------
CURRENT_DB_VERSION = 4

COMPATIBILITY = collections.OrderedDict([
    ('4.3.0', 1), ('4.3.1', 1), ('4.3.2', 1),
    ('4.4.0', 2), ('4.4.1', 2), ('4.4.2', 2),
    ('4.5.0', 4)
])
#-------------------------------------------------------------------


def get_class(version, connection):
    module = "Pegasus.db.admin.versions.v%s" % version
    mod = __import__(module, fromlist=["Version"])
    klass = getattr(mod, "Version")
    return klass(connection)


#-------------------------------------------------------------------
class AdminDB(object):

    def __init__(self, config_properties=None, db_type=None, dburi=None, create=False):
        """ Database connection """
        if not dburi:
            if config_properties and db_type:
                dburi = {
                    "JDBCRC": self._get_jdbcrc_uri(config_properties),
                    "MASTER": self._get_master_uri(config_properties),
                    "WORKFLOW": self._get_workflow_uri(config_properties),
                }.get(db_type.upper(), "invalid")
                if dburi == "invalid":
                    log.error("Invalid database type '%s'." % db_type)
                    raise RuntimeError("Invalid database type '%s'." % db_type)
                
            else:
                dburi = self._get_workflow_uri()
                
        if dburi:
            log.debug("Using database: %s" % dburi)
            self.db = connection.connect(dburi, create=create)
        else:
            log.error("Unable to find a database URI to connect.")
            raise RuntimeError("Unable to find a database URI to connect.")
    
    
    def _get_jdbcrc_uri(self, config_properties):
        """ Get JDBCRC URI """
        props = properties.Properties()
        props.new(config_file=config_properties)
        replica_catalog = props.property('pegasus.catalog.replica')
        if replica_catalog != "JDBCRC":
            return None
    
        rc_info = {
            "driver" : props.property('pegasus.catalog.replica.db.driver'),
            "url" : props.property('pegasus.catalog.replica.db.url'),
            "user" : props.property('pegasus.catalog.replica.db.user'),
            "password" : props.property('pegasus.catalog.replica.db.password'),
        }

        url = rc_info["url"]
        url = url.replace("jdbc:", "")
        o = urlparse(url)
        host = o.netloc
        database = o.path.replace("/", "")

        if rc_info["driver"].lower() == "mysql":
            return "mysql://" + rc_info["user"] + ":" + rc_info["password"] + "@" + host + "/" + database

        if rc_info["driver"].lower() == "sqlite":
            connString = os.path.join(host, "workflow.db")
            return "sqlite:///" + connString

        if rc_info["driver"].lower() == "postgresql":
            return "postgresql://" + rc_info["user"] + ":" + rc_info["password"] + "@" + host + "/" + database
        
        log.error("Invalid JDBCRC driver: %s" % rc_info["driver"])
        return None
    
    
    def _get_master_uri(self, config_properties):
        """ Get MASTER URI """
        props = properties.Properties()
        props.new(config_file=config_properties)
        return props.property('pegasus.monitord.output')
    
    
    def _get_workflow_uri(self, config_properties=None):
        """ Get WORKFLOW URI """
        if config_properties:
            props = properties.Properties()
            props.new(config_file=config_properties)
            dburi = props.property('pegasus.dashboard.output')
            if dburi:
                return dburi
    
        homedir = os.getenv("HOME", None)
        dburi = os.path.join(homedir, ".pegasus", "workflow.db")
        pegasusDir = os.path.dirname(dburi)
        if not os.path.exists(pegasusDir):
            os.mkdir(pegasusDir)
        return "sqlite:///" + dburi


################################################################################

    def current_version(self, parse=False):
        """ Get the current version of the database."""
        self._verify_tables()
        current_version = None
        
        try:
            current_version = self._get_version()
        except NoResultFound:
            current_version = self._discover_version()
                        
        if parse:
            parsed = False
            for ver in COMPATIBILITY:
                if COMPATIBILITY[ver] == current_version:
                    current_version = ver
                    parsed = True
            if not parsed:
                log.error("Your database is not compatible with any Pegasus version.")
                log.error("Use 'pegasus-db-admin check' to verify its compatibility.")
                raise RuntimeError("Your database is not compatible with any Pegasus version.")
        
        return current_version
    

    def verify(self, pegasus_version=None, parse=False, verbose=False):
        """ Verify whether the database is compatible to the specified 
            Pegasus version."""
        self._verify_tables()
        version = self._parse_pegasus_version(pegasus_version)       
        
        compatible = False
        try:
            compatible = self._check_version(version)
        except NoResultFound:
            self._discover_version()
            compatible = self._check_version(version)
        
        if verbose:
            friendly_version = version
            if parse:
                if pegasus_version:
                    friendly_version = pegasus_version
                else:
                    for ver in COMPATIBILITY:
                        if COMPATIBILITY[ver] == version:
                            friendly_version = ver

            if compatible:
                log.info("Your database is compatible with version %s." % friendly_version)
            else:
                log.error("Your database is NOT compatible with version %s." % friendly_version)
                current_version = self.current_version()
                command = "update"
                if current_version > version:
                    command = "downgrade"
                if version == CURRENT_DB_VERSION:
                    log.error("Use 'pegasus-db-admin %s' to %s your database." % (command, command))
                else:
                    log.error("Use 'pegasus-db-admin %s -V %s' to %s your database." % (command, friendly_version, command))
                raise RuntimeError("Your database is NOT compatible with version %s." % friendly_version)
        
        return compatible


    def update(self, pegasus_version=None, force=False):
        """ Update the database. """
        self._verify_tables()
        
        current_version = self.current_version()
        version = self._parse_pegasus_version(pegasus_version)
        
        if current_version > version:
            log.error("Unable to run update. Current database version is newer than specified version '%s'." % (pegasus_version))
            raise RuntimeError("Unable to run update. Current database version is newer than specified version '%s'." % (pegasus_version))
        
        for i in range(current_version + 1, version + 1):
            k = get_class(i, self.db)
            k.update(force)
            self._update_version(i)
        log.info("Your database was successfully updated.")
                    
                    
    def downgrade(self, pegasus_version=None, force=False):
        """ Downgrade the database. """
        self._verify_tables()
        
        current_version = self.current_version()
        version = self._parse_pegasus_version(pegasus_version)
        
        if version == CURRENT_DB_VERSION:
            version = version - 1
            for ver in COMPATIBILITY:
                if COMPATIBILITY[ver] < CURRENT_DB_VERSION:
                    version = COMPATIBILITY[ver]
        
        if current_version < version:
            log.error("Unable to run downgrade. Current database version is older than specified version '%s'." % (pegasus_version))
            raise RuntimeError("Unable to run downgrade. Current database version is older than specified version '%s'." % (pegasus_version))
        
        for i in range(current_version, version, -1):
            k = get_class(i, self.db)
            k.downgrade(force)
            self._update_version(i - 1)
        log.info("Your database was successfully downgraded.")


################################################################################

    def _get_version(self):
        current_version = self.db.query(DBVersion.version_number).order_by(
            DBVersion.id.desc()).first()
        if not current_version:
            raise NoResultFound()
        return current_version[0]


    def _discover_version(self):
        version = 0
        for i in range(1, CURRENT_DB_VERSION + 1):
            k = get_class(i, self.db)
            if k.is_compatible():
                version = i
        
        if version > 0:
            self._update_version(version)
        return version


    def _parse_pegasus_version(self, pegasus_version):
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


    def _check_version(self, version):
        db_version = self._get_version()
        if db_version and not version == db_version:
            return False
        return True
    
       
    def _update_version(self, version):
        v = DBVersion()
        v.version_number = version
        v.version_timestamp = datetime.datetime.now().strftime("%s")    
        if self.db:
            self.db.add(v)
            self.db.commit()
            
    def _verify_tables(self):
        try:
            self.db.execute("SELECT * FROM dbversion")
        except:
            log.error("Non-existent or missing database tables.")
            log.error("Run 'pegasus-db-admin create' to create the missing tables.")
            raise RuntimeError("Non-existent or missing database tables.")