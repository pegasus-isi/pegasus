__author__ = "Rafael Ferreira da Silva"

import os
import time
import sqlite3 as lite

from Pegasus.db.modules import stampede_dashboard_loader
from Pegasus.db.modules import jdbcrc_loader
from Pegasus.tools import properties
from urlparse import urlparse

MIN_VERSION = 1.0
CURRENT_DB_VERSION = 1.2

def get_class(version, config_properties, verbose=False, debug=False):
    version = "v%s" % version
    version = version.replace(".", "-")
    module = "Pegasus.db.admin.versions.%s" % version
    mod = __import__(module, fromlist=["Version"])
    klass = getattr(mod, "Version")
    return klass(config_properties, verbose, debug)

#-------------------------------------------------------------------
class DashboardDB(object):

    def __init__(self, config_properties, verbose=False, debug=False):

        self.config_properties = config_properties
        self._vbs = verbose
        self._dbg = debug
        
        homedir = os.getenv("HOME", None)
        self.dashConnString = os.path.join(homedir, "pegasus", "workflow.db")
        self.connection = None
        
        try:
            self.connection = lite.connect(self.dashConnString)
            cur = self.connection.cursor()
            cur.execute("SELECT name FROM sqlite_master "
                "WHERE type='table' AND name='db_admin'")
            data = cur.fetchone()
            if data == None:
                self.create_admin_table()
            
        except lite.Error, e:
            raise RuntimeError(e)
    
    def get_connection(self):
        return self.connection

    def current_version(self):
        cur = self.connection.cursor()
        cur.execute("SELECT version_number, version_timestamp "
            "FROM db_admin ORDER BY id DESC")
        data = cur.fetchone()
        return data
        
    def execute_update(self, query):
        try:
            cur = self.connection.cursor()
            cur.execute(query)
            self.connection.commit()
        except lite.Error, e:
            self.connection.rollback()
            raise RuntimeError(e)
    
    def update_version(self, version):
        try:
            cur = self.connection.cursor()
            ts = time.time()
            cur.execute("INSERT INTO db_admin(version_number, version_timestamp) "
                "VALUES(%.2f, %d)" % (version, ts))
            self.connection.commit()
        except lite.Error, e:
            self.connection.rollback()
            raise RuntimeError(e)
    
    def create_admin_table(self):
        try:
            cur = self.connection.cursor()
            cur.execute("CREATE TABLE db_admin ("
                "id                 INTEGER PRIMARY KEY AUTOINCREMENT,"
                "version_number     NUMERIC(2,1) NOT NULL, "
                "version_timestamp  NUMERIC(16,6) NOT NULL, "
                "CONSTRAINT sk_db_admin UNIQUE (version_number, version_timestamp)"
                ")")
                
            version = MIN_VERSION
            temp = MIN_VERSION
            while (temp < CURRENT_DB_VERSION):
                temp += 0.1
                v = get_class(temp, self.config_properties, self._vbs, self._dbg)
                if v.is_compatible():
                    version = temp
                            
            ts = time.time()
            cur.execute("INSERT INTO db_admin(version_number, version_timestamp) "
                "VALUES(%.2f, %d)" % (version, ts))
            self.connection.commit()
            
        except lite.Error, e:
            self.connection.rollback()
            raise RuntimeError(e)
        
    def create_tables(self):
        stampede_dashboard_loader.Analyzer("sqlite:///" + self.dashConnString)
    
    def close(self):
        if self.connection:
            self.connection.close()

    def history(self):
        cur = self.connection.cursor()
        cur.execute("SELECT version_number, version_timestamp "
            "FROM db_admin ORDER BY id DESC")
        data = cur.fetchall()
        return data
    
#-------------------------------------------------------------------
class JDBCRC(object):
    
    def __init__(self, config_properties, verbose=False, debug=False):
        
        self._vbs = verbose
        self._dbg = debug
        self.connection = None
        
        rc_info = self.get_rc_info(config_properties)
        url = rc_info["url"]
        url = url.replace("jdbc:", "")
        o = urlparse(url)
        host = o.netloc
        database = o.path.replace("/", "")
        
        if rc_info["driver"].lower() == "mysql":
            db = __import__('MySQLdb')
            self.connection = db.connect(host, rc_info["user"], rc_info["password"], database);
            self.RCConnString = "mysql://" + rc_info["user"] + ":" + rc_info["password"] + "@" + host + "/" + database
            
        elif rc_info["driver"].lower() == "sqlite":
            db = __import__('sqlite3')
            connString = os.path.join(host, "workflow.db")
            self.connection = db.connect(connString);
            self.RCConnString = "sqlite://" + connString
            
        else:
            db = __import__('psycopg2')
            self.connection = db.connect(database, rc_info["user"]);
            self.RCConnString = "postgresql://" + rc_info["user"] + ":" + rc_info["password"] + "@" + host + "/" + database
            
    def get_rc_info(self, config_properties):
        props = properties.Properties()
        props.new(config_file=config_properties)
        rc_info = {
            "driver" : props.property('pegasus.catalog.replica.db.driver'),
            "url" : props.property('pegasus.catalog.replica.db.url'),
            "user" : props.property('pegasus.catalog.replica.db.user'),
            "password" : props.property('pegasus.catalog.replica.db.password'),
        }
        return rc_info
            
    def execute_update(self, query):
        try:
            cur = self.connection.cursor()
            cur.execute(query)
            self.connection.commit()
        except lite.Error, e:
            self.connection.rollback()
            raise RuntimeError(e)
        
    def get_connection(self):
        return self.connection
    
    def create_tables(self):
        jdbcrc_loader.Analyzer(self.RCConnString)
        
    def close(self):
        if self.connection:
            self.connection.close()
            