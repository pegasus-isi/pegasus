"""
Code to handle various aspects of transitioning to a new verson of the 
Stampede schema.
"""
__rcsid__ = "$Id: schema_check.py 29297 2012-01-12 00:24:17Z mgoode $"
__author__ = "Monte Goode"

import exceptions

from netlogger.analysis.modules._base import SQLAlchemyInit, dsn_dialect
from netlogger.analysis.schema.stampede_schema import *
from netlogger.nllog import DoesLogging

class SchemaVersionError(Exception): 
    """
    Custom exception.  Will be raised in the loader/etc if the schema
    is out of date so the calling program can catch and handle it.
    """
    pass

class ConnHandle(SQLAlchemyInit, DoesLogging):
    """
    Stand-alone connection class that returns a SQLAlchemy session.
    """
    def __init__(self, connString=None, mysql_engine=None, **kw):
        DoesLogging.__init__(self)
        if connString is None:
            raise ValueError("connString is required")
        _kw = { }
        dialect = dsn_dialect(connString)
        _kw[dialect] = { }
        if dialect == 'mysql':
            if mysql_engine is not None:
                _kw[dialect]['mysql_engine'] = mysql_engine
        try:
            SQLAlchemyInit.__init__(self, connString, initializeToPegasusDB, **_kw)
        except exceptions.OperationalError, e:
            self.log.error('init', msg='%s' % ErrorStrings.get_init_error(e))
            raise RuntimeError
        pass
        
    def get_session(self):
        return self.session
        

class ErrorStrings:
    """
    Parses SQLAlchemy OperationalErrors to generate error strings.
    Currently just handles case of when a user with limited permissions
    might hit a wall when running 3.2 code on an existing 3.1 DB.
    """
    # Actions
    create_failure = 'CREATE command denied to user'
    # Tables
    schema_info = 'schema_info'
    
    @staticmethod
    def get_init_error(e):
        action_error = e.message.split("'")[0].split('"')[1].strip()
        table_error  = e.message.split("'")[-2]
        
        er = ''
        
        if action_error == ErrorStrings.create_failure and \
            table_error == ErrorStrings.schema_info:
            er = 'The schema_info table does not exist: '
            er += 'user does not have CREATE TABLE permissions - '
            er += 'database schema needs to be upgraded to version %s, ' % CURRENT_SCHEMA_VERSION
            er += 'database admin will need to run upgrade tool'
            return er
        else:
            er = 'Unknown error raised during database init: %s' % e
            
        return er
        
class SchemaCheck(DoesLogging):
    """
    This handles checking the schema, setting the proper version_number
    if not already set, returning the current version for things like the
    API and methods to manually upgrade an older existing DB to the new
    schema.
    
    The check_schema() method should be called by any code that 
    creates/initializes a database (like the loader) so it can scan 
    schema and set the correct version number.
    """
    def __init__(self, session):
        DoesLogging.__init__(self)
        self.session = session
        self.log.info('init')
        pass
        
    def _get_current_version(self):
        q = self.session.query(cast(func.max(SchemaInfo.version_number), Float))
        if not q.one()[0]:
            return q.one()[0]
        else:
            return float(q.one()[0])
        
    def _version_check(self, version_number):
        self.log.info('check_schema', msg='Current version set to: %s' % version_number)
        if float(version_number) == CURRENT_SCHEMA_VERSION:
            self.log.info('check_schema', msg='Schema up to date')
            return True
        elif float(version_number) < CURRENT_SCHEMA_VERSION:
            self.log.error('check_schema', \
                msg='Schema version %s out of date - database admin will need to run upgrade tool' % float(version_number))
            return False
            
    def check_schema(self):
        """
        Checks the schema to determine the version, sets the information 
        in the schema_info table, and outputs an error message if the 
        schema is out of date.  Returns True or False so calling apps
        (like the loader) can handle appropriately with execptions, etc.
        """
        self.log.info('check_schema.start')
        
        version_number = self._get_current_version()
        if not version_number:
            self.log.info('check_schema', msg='No version_number set in schema_info')
        else:
            return self._version_check(version_number)
        
        self.log.info('check_schema', msg='Determining schema version.')
        
        #
        # Checks for version 3.2
        #
        
        m_factor_check = exitcode_check = remote_cpu_check = False
        
        # Check job_instance table
        q = self.session.query(JobInstance).limit(1)
        if hasattr(q.column_descriptions[0]['expr'], 'multiplier_factor'):
            m_factor_check = True
        if hasattr(q.column_descriptions[0]['expr'], 'exitcode'):
            exitcode_check = True
        
        # Check invocation
        q = self.session.query(Invocation).limit(1)
        if hasattr(q.column_descriptions[0]['expr'], 'remote_cpu_time'):
            remote_cpu_check = True
        
        s_info = SchemaInfo()
        
        if not m_factor_check and not exitcode_check and not remote_cpu_check:
            self.log.info('check_schema', msg='Setting schema to version 3.1')
            s_info.version_number = 3.1
        elif m_factor_check and exitcode_check and remote_cpu_check:
            s_info.version_number = 3.2
            self.log.info('check_schema', msg='Setting schema to version 3.2')
        else:
            self.log.error('check_schema', msg='Error in determining database schema')
            raise RuntimeError
        
        s_info.commit_to_db(self.session)
        
        #
        # End version 3.2 code
        #
        
        return self._version_check(self._get_current_version())
        
    def check_version(self):
        """
        Check version in the schema_info table.  Called for things
        like the stats api.  Assumes 3.1 if no version set.
        """
        version_number = self._get_current_version()
        if not version_number:
            # presume 3.1
            return 3.1
        else:
            return version_number
            
    def upgrade_to_3_2(self):
        """
        Called by the "upgrade tool" - upgrades a populated 3.1 DB to
        3.2.  This is not necessary for querying the DB, but must be
        done to load data - even if the events are '3.1 style.'
        """
        self.log.info('upgrade_to_3_2', msg='Upgrading to schema version 3.2')
        if self._get_current_version() == 3.2:
            self.log.warn('upgrade_to_3_2', msg='Schema version already 3.2 - skipping')
            return
        
        r_c_t = 'ALTER TABLE invocation ADD COLUMN remote_cpu_time NUMERIC(10,3) NOT NULL'
        if self.session.connection().dialect.name != 'sqlite':
            r_c_t += ' AFTER remote_duration'
        m_fac = 'ALTER TABLE job_instance ADD COLUMN multiplier_factor INT NULL DEFAULT 1'
        e_cod  = 'ALTER TABLE job_instance ADD COLUMN exitcode INT NULL'
        
        self.session.execute(r_c_t)
        self.session.execute(m_fac)
        self.session.execute(e_cod)
        
        s_info = SchemaInfo()
        s_info.version_number = 3.2
        s_info.commit_to_db(self.session)
        pass

        
def main():
    pass
    
if __name__ == '__main__':
    main()