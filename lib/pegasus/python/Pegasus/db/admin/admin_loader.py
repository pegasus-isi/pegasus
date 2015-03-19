__author__ = "Rafael Ferreira da Silva"

import collections
import datetime
import fnmatch
import os
import time

from Pegasus.db.modules import stampede_loader
from Pegasus.db.modules import stampede_dashboard_loader
from Pegasus.db.modules import jdbcrc_loader
from Pegasus.db.schema.pegasus_schema import *
from Pegasus.tools import properties
from sqlalchemy.orm.exc import *
from urlparse import urlparse

#-------------------------------------------------------------------
# DB Admin configuration
#-------------------------------------------------------------------
CURRENT_DB_VERSION = 3

COMPATIBILITY = collections.OrderedDict([
    ('4.3.0', 1), ('4.3.1', 1), ('4.3.2', 1),
    ('4.4.0', 2), ('4.4.1', 2), ('4.4.2', 2),
    ('4.5.0', 3)
])
#-------------------------------------------------------------------


def get_class(version, connections, database_name=None, verbose=False, debug=False):
    module = "Pegasus.db.admin.versions.v%s" % version
    mod = __import__(module, fromlist=["Version"])
    klass = getattr(mod, "Version")
    return klass(connections, database_name, verbose, debug)


#-------------------------------------------------------------------
class AdminDB(object):

    def __init__(self, config_properties, database_url, verbose=False, debug=False):
        self.config_properties = config_properties
        self.database_url = database_url
        self._vbs = verbose
        self._dbg = debug
        
        # configure database objects
        self.connections = {
            'JDBCRC': self._connect_jdbcrc(),
            'DASHBOARD': self._connect_dashboard(),
            'STAMPEDE': self._connect_stampede(),
        }
        

    def _connect_jdbcrc(self):
        """ Connect to the JDBCRC database """
        props = properties.Properties()
        props.new(config_file=self.config_properties)
        replica_catalog = props.property('pegasus.catalog.replica')
        if replica_catalog != "JDBCRC":
            return None
        
        connString = None
        if self.database_url:
           connString = self.database_url
        else :
            rc_info = self._get_rc_info(self.config_properties)
            url = rc_info["url"]
            url = url.replace("jdbc:", "")
            o = urlparse(url)
            host = o.netloc
            database = o.path.replace("/", "")

            if rc_info["driver"].lower() == "mysql":
                connString = "mysql://" + rc_info["user"] + ":" + rc_info["password"] + "@" + host + "/" + database

            if rc_info["driver"].lower() == "sqlite":
                connString = os.path.join(host, "workflow.db")
                connString = "sqlite:///" + connString

            if rc_info["driver"].lower() == "postgresql":
                connString = "postgresql://" + rc_info["user"] + ":" + rc_info["password"] + "@" + host + "/" + database
        
        if connString:
            return jdbcrc_loader.Analyzer(connString)
        
        return None
    

    def _connect_dashboard(self):
        """ Connect to the Dashboard database """
        connString = None
        if self.database_url:
           connString = self.database_url
        else:
            homedir = os.getenv("HOME", None)
            connString = os.path.join(homedir, ".pegasus", "workflow.db")
            pegasusDir = os.path.dirname(connString)
            if not os.path.exists(pegasusDir):
                os.mkdir(pegasusDir)
            connString = "sqlite:///" + connString
        
        if connString:
            return stampede_dashboard_loader.Analyzer(connString)
        
        return None
            

    def _connect_stampede(self):
        """ Connect to the Stampede database """
        connString = None
        if self.database_url:
            connString = self.database_url
        else:
            # TODO connection to Stampede database
            pass
           
        if connString:
            return stampede_loader.Analyzer(connString)
        
        return None
           
        
    def _get_rc_info(self, config_properties):
        props = properties.Properties()
        props.new(config_file=config_properties)
        rc_info = {
            "driver" : props.property('pegasus.catalog.replica.db.driver'),
            "url" : props.property('pegasus.catalog.replica.db.url'),
            "user" : props.property('pegasus.catalog.replica.db.user'),
            "password" : props.property('pegasus.catalog.replica.db.password'),
        }
        return rc_info


    def verify(self, pegasus_version=None, database_name=None):
        """ Verify whether the database is compatible to the specified 
            Pegasus version."""
        version = self._parse_pegasus_version(pegasus_version)       
        
        if database_name:
            try:
                return self._check_version(database_name.upper(), version)
            except NoResultFound, e:
                self._discover_version(database_name.upper())
                return self._check_version(database_name.upper(), version)
        else:
            for db_name in self.connections:
                try:
                    if not self._check_version(db_name, version):
                        return False
                except NoResultFound, e:
                    self._discover_version(db_name)
                    if not self._check_version(db_name, version):
                        return False
        return True


    def current_version(self, database_name, parse=False):
        """ Get the current version of the database."""
        current_version = {}
        
        if database_name:
            try:
                current_version[database_name.upper()] = self._get_version(database_name.upper())
            except NoResultFound, e:
                    current_version[database_name.upper()] = self._discover_version(database_name.upper())
        else:
            for db_name in self.connections:
                try:
                   current_version[db_name] = self._get_version(db_name) 
                except NoResultFound, e:
                    current_version[db_name] = self._discover_version(db_name)
                    
        if parse:
            for key in current_version:
                for ver in COMPATIBILITY:
                    if COMPATIBILITY[ver] == current_version[key]:
                        current_version[key] = ver
        
        return current_version


    def update(self, pegasus_version=None, database_name=None, force=False):
        """ Update the database. """
        
        current_version = self.current_version(database_name)
        version = self._parse_pegasus_version(pegasus_version)
        
        for db_name in current_version:
            cv = current_version[db_name]
            if cv < version:
                for i in range(cv + 1, version + 1):
                    k = get_class(i, self.connections, db_name, self._vbs, self._dbg)
                    k.update(force)
                    self._update_version(i, db_name)
                    
                    
    def downgrade(self, pegasus_version=None, database_name=None, force=False):
        """ Downgrade the database. """
        
        current_version = self.current_version(database_name)
        version = self._parse_pegasus_version(pegasus_version)
        if version == CURRENT_DB_VERSION:
            version = version - 1
        
        for db_name in current_version:
            cv = current_version[db_name]
            if cv > version:
                for i in range(cv, version, -1):
                    k = get_class(i, self.connections, db_name, self._vbs, self._dbg)
                    k.downgrade(force)
                    self._update_version(i - 1, db_name)


    def _parse_pegasus_version(self, pegasus_version):
        version = None
        if pegasus_version:
            for key in COMPATIBILITY:
                if key == pegasus_version:
                    version = COMPATIBILITY[key]
                    break
            if not version:
                raise ValueError("Version does not exist: %s." % pegasus_version)
        if not version:
            version = CURRENT_DB_VERSION
        return version


    def _check_version(self, db_name, version):
        if not version == self._get_version(db_name):
            return False
        return True
    
    
    def _discover_version(self, db_name):
        version = 0
        for i in range(1, CURRENT_DB_VERSION + 1):
            k = get_class(i, self.connections, db_name, self._vbs, self._dbg)
            if k.is_compatible():
                version = i
        
        if version > 0:
            self._update_version(version, db_name)
        
        return version
    
    
    def _update_version(self, version, db_name):
        v = DBVersion()
        v.version_number = version
        v.version_timestamp = datetime.datetime.now().strftime("%s")    
        db = self.connections[db_name]
        if db:
            db.session.add(v)
            db.session.commit()
    

    def _get_version(self, db_name):
        db = self.connections[db_name]
        if db:
            current_version = db.session.query(DBVersion.version_number).order_by(
                DBVersion.id.desc()).first()
            if not current_version:
                raise NoResultFound()
            return current_version[0]
        # TODO fix this return once connection to stampede database is fixed.
        return CURRENT_DB_VERSION


    def get_connections(self):
        return self.connections
    
