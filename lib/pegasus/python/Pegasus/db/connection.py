import imp
import logging
import getpass
import os

from Pegasus.tools import properties
from Pegasus.tools import utils
from sqlalchemy import create_engine, orm, event, exc
from sqlalchemy.engine import Engine
from sqlite3 import Connection as SQLite3Connection
from urlparse import urlparse

from Pegasus import user as users

__all__ = ['connect']

log = logging.getLogger(__name__)

#-------------------------------------------------------------------
class ConnectionError(Exception):
    pass


class DBType:
    JDBCRC = "JDBCRC"
    MASTER = "MASTER"
    WORKFLOW = "WORKFLOW"


def connect(dburi, echo=False, schema_check=True, create=False, pegasus_version=None, force=False):
    """ Connect to the provided URL database."""
    dburi = _parse_jdbc_uri(dburi)
    _validate(dburi)
    
    try:
        log.info("Attempting to connect to: %s" % dburi)
        engine = create_engine(dburi, echo=echo, pool_recycle=True)
        engine.connect()
        log.info("Connected successfully established.")

    except exc.OperationalError, e:
        if "mysql" in dburi and "unknown database" in str(e).lower():
            raise ConnectionError("MySQL database should be previously created: %s" % e.message)
        raise ConnectionError(e)
    
    except Exception, e:
        raise ConnectionError(e)

    Session = orm.sessionmaker(bind=engine, autoflush=False, autocommit=False,
                               expire_on_commit=False)
    db = orm.scoped_session(Session)

    # Database creation
    if create:
        try:
            from Pegasus.db.admin.admin_loader import db_create
            db_create(dburi, engine, db, pegasus_version=pegasus_version, force=force)

        except exc.OperationalError, e:
            raise ConnectionError(e)

    if schema_check:
        from Pegasus.db.admin.admin_loader import db_verify
        db_verify(db, pegasus_version=pegasus_version, force=force)

    return db


def connect_by_submitdir(submit_dir, db_type, config_properties=None, echo=False, schema_check=True, create=False, pegasus_version=None, force=False):
    """ Connect to the database from submit directory and database type """
    dburi = url_by_submitdir(submit_dir, db_type, config_properties)
    return connect(dburi, echo, schema_check, create=create, pegasus_version=pegasus_version, force=force)

    
def connect_by_properties(config_properties, db_type, echo=False, schema_check=True, create=False, pegasus_version=None, force=False):
    """ Connect to the database from properties file and database type """
    dburi = url_by_properties(config_properties, db_type)
    return connect(dburi, echo, schema_check, create=create, pegasus_version=pegasus_version, force=force)


def url_by_submitdir(submit_dir, db_type, config_properties=None, top_dir=None):
    """ Get URL from the submit directory """
    if not submit_dir:
        raise ConnectionError("A submit directory should be provided with the type parameter.")
    if not db_type:
        raise ConnectionError("A type should be provided with the property file.")

    # From the submit dir, we need the wf_uuid
    # Getting values from the submit_dir braindump file
    top_level_wf_params = utils.slurp_braindb(submit_dir)
    
    # Return if we cannot parse the braindump.txt file
    if not top_level_wf_params:
        raise ConnectionError("File 'braindump.txt' not found in %s" % (submit_dir))
    
    # Load the top-level braindump now if top_dir is not None
    if top_dir is not None:
        # Getting values from the top_dir braindump file
        top_level_wf_params = utils.slurp_braindb(top_dir)

        # Return if we cannot parse the braindump.txt file
        if not top_level_wf_params:
            raise ConnectionError("File 'braindump.txt' not found in %s" % (top_dir))
    
    # Get the location of the properties file from braindump
    top_level_prop_file = None
    
    # Get properties tag from braindump
    if "properties" in top_level_wf_params:
        top_level_prop_file = top_level_wf_params["properties"]
        # Create the full path by using the submit_dir key from braindump
        if "submit_dir" in top_level_wf_params:
            top_level_prop_file = os.path.join(top_level_wf_params["submit_dir"], top_level_prop_file)
            
    return url_by_properties(config_properties, db_type, submit_dir, rundir_properties=top_level_prop_file)

    
def url_by_properties(config_properties, db_type, submit_dir=None, rundir_properties=None):
    """ Get URL from the property file """
    # Validate parameters
    if not db_type:
        raise ConnectionError("A type should be provided with the property file.")
    
    # Parse, and process properties
    props = properties.Properties()
    props.new(config_file=config_properties, rundir_propfile=rundir_properties)
    
    dburi = None
    if db_type.upper() == DBType.JDBCRC:
        dburi = _get_jdbcrc_uri(props)
    elif db_type.upper() == DBType.MASTER:
        dburi = _get_master_uri(props)
    elif db_type.upper() == DBType.WORKFLOW:
        dburi = _get_workflow_uri(props, submit_dir)
    else:
        raise ConnectionError("Invalid database type '%s'." % db_type)

    if dburi:
        log.debug("Using database: %s" % dburi)
        return dburi
    
    raise ConnectionError("Unable to find a database URI to connect.")


def get_wf_uuid(submit_dir):
    # From the submit dir, we need the wf_uuid
    # Getting values from the submit_dir braindump file
    top_level_wf_params = utils.slurp_braindb(submit_dir)
    
    # Return if we cannot parse the braindump.txt file
    if not top_level_wf_params:
        logger.error("Unable to process braindump.txt in %s" % (submit_dir))
        return None
    
    # Get wf_uuid for this workflow
    wf_uuid = None
    if (top_level_wf_params.has_key('wf_uuid')):
        wf_uuid = top_level_wf_params['wf_uuid']
    else:
        logger.error("workflow id cannot be found in the braindump.txt ")
        return None
    
    return wf_uuid
    

def connect_to_master_db(user=None):
    "Connect to 'user's master database"

    if user is None:
        user = getpass.getuser()

    u = users.get_user_by_username(user)

    dburi = u.get_master_db_url()

    return connect(dburi)


#-------------------------------------------------------------------

# This turns on foreign keys for SQLite3 connections
@event.listens_for(Engine, "connect")
def _set_sqlite_pragma(conn, record):
    if isinstance(conn, SQLite3Connection):
        log.debug("Turning on foreign keys")
        cursor = conn.cursor()
        cursor.execute("PRAGMA foreign_keys=ON;")
        cursor.close()


def _get_jdbcrc_uri(props=None):
    """ Get JDBCRC URI from properties """
    if props:
        replica_catalog = props.property('pegasus.catalog.replica')
        if not replica_catalog:
            raise ConnectionError("'pegasus.catalog.replica' property not set.")
        
        if replica_catalog.upper() != DBType.JDBCRC:
            return None

        rc_info = {
            "driver" : props.property('pegasus.catalog.replica.db.driver'),
            "url" : props.property('pegasus.catalog.replica.db.url'),
            "user" : props.property('pegasus.catalog.replica.db.user'),
            "password" : props.property('pegasus.catalog.replica.db.password'),
        }

        url = rc_info["url"]
        if not url:
            raise ConnectionError("'pegasus.catalog.replica.db.url' property not set.")
        url = _parse_jdbc_uri(url)
        o = urlparse(url)
        host = o.netloc
        database = o.path.replace("/", "")

        driver = rc_info["driver"]
        if not driver:
            raise ConnectionError("'pegasus.catalog.replica.db.driver' property not set.")
        
        if driver.lower() == "mysql":
            return "mysql://" + rc_info["user"] + ":" + rc_info["password"] + "@" + host + "/" + database

        if driver.lower() == "sqlite":
            if "sqlite:" in url:
                return url
            connString = os.path.join(host, "workflow.db")
            return "sqlite:///" + connString

        if driver.lower() == "postgresql":
            return "postgresql://" + rc_info["user"] + ":" + rc_info["password"] + "@" + host + "/" + database

        log.debug("Invalid JDBCRC driver: %s" % rc_info["driver"])
    return None
    
    
def _get_master_uri(props=None):
    """ Get MASTER URI """
    if props:
        dburi = props.property('pegasus.catalog.master.url')
        if dburi:
            return dburi
        dburi = props.property('pegasus.dashboard.output')
        if dburi:
            return dburi

    homedir = os.getenv("HOME", None)
    if homedir == None:
        raise ConnectionError("Environment variable HOME not defined, set pegasus.dashboard.output property to point to the Dashboard database.")
    
    dir = os.path.join( homedir, ".pegasus" );
    
    # check for writability and create directory if required
    if not os.path.isdir(dir):
        try:
            os.mkdir(dir)
        except OSError:
            raise ConnectionError("Unable to create directory: %s" % dir)
    elif not os.access(dir, os.W_OK):
        log.warning("Unable to write to directory: %s" % dir)
        return None
    
    #directory exists, touch the file and set permissions
    filename =  os.path.join(dir, "workflow.db")
    if not os.access(filename, os.F_OK):
        try:
            # touch the file
            open(filename, 'w').close()
            os.chmod(filename, 0600)
        except Exception, e:
            log.warning("unable to initialize MASTER db %s." % filename)
            log.exception(e)
            return None
    elif not os.access( filename, os.W_OK ):
        log.warning("No read access for file: %s" % filename)
        return None

    return "sqlite:///" + filename
    
    
def _get_workflow_uri(props=None, submit_dir=None):
    """ Get WORKFLOW URI """
    if props:
        dburi = props.property('pegasus.catalog.workflow.url')
        if dburi:
            return dburi
        dburi = props.property('pegasus.monitord.output')
        if dburi:
            return dburi
        
    if submit_dir:
        # From the submit dir, we need the wf_uuid
        # Getting values from the submit_dir braindump file
        top_level_wf_params = utils.slurp_braindb(submit_dir)

        # The default case is a .stampede.db file with the dag name as base
        dag_file_name = ""
        if (top_level_wf_params.has_key('dag')):
            dag_file_name = top_level_wf_params['dag']
        else:
            raise ConnectionError("DAG file name cannot be found in the braindump.txt.")

        # Create the sqllite db url
        dag_file_name = os.path.basename(dag_file_name)
        output_db_file = (submit_dir) + "/" + dag_file_name[:dag_file_name.find(".dag")] + ".stampede.db"
        dburi = "sqlite:///" + output_db_file
        return dburi
    
    return None


def _parse_jdbc_uri(dburi):
    if dburi:
        if dburi.startswith("jdbc:"):
            dburi = dburi.replace("jdbc:", "")
            if dburi.startswith("sqlite:"):
                dburi = dburi.replace("sqlite:", "sqlite:///")
    return dburi


def _validate(dburi):
    try:
        if dburi:
            if dburi.startswith("postgresql:"):
                imp.find_module('psycopg2')
            if dburi.startswith("mysql:"):
                imp.find_module('MySQLdb')
            
    except ImportError, e:
        raise ConnectionError("Missing Python module: %s" % e)
    
