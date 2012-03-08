"""
Code to handle various aspects of transitioning to a new verson of the 
Stampede schema.
"""
__rcsid__ = "$Id: schema_check.py 30802 2012-03-07 17:01:34Z mgoode $"
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
    might hit a wall when running 4.0 code on an existing 3.1 DB.
    """
    # Actions
    create_failure = 'CREATE command denied to user'
    # Tables
    schema_info = 'schema_info'
    
    @staticmethod
    def get_init_error(e):
        
        action_error = table_error = None
        
        try:
            action_error = e.args[0].split("'")[0].split('"')[1].strip()
            table_error  = e.args[0].split("'")[-2]
        except IndexError:
            # specific parse didn't work, so pass the original
            # exception through.
            pass
        
        er = ''
        
        if action_error == ErrorStrings.create_failure and \
            table_error == ErrorStrings.schema_info:
            er = 'The schema_info table does not exist: '
            er += 'user does not have CREATE TABLE permissions - '
            er += 'database schema needs to be upgraded to version %s, ' % CURRENT_SCHEMA_VERSION
            er += 'database admin will need to run upgrade tool'
            return er
        else:
            er = 'Error raised during database init: %s' % e.args[0]
            
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
        
        self._table_map = {}
        pass
        
    def _get_current_version(self):
        q = self.session.query(cast(func.max(SchemaInfo.version_number), Float))
        if not q.one()[0]:
            return q.one()[0]
        else:
            return round(q.one()[0],1)
        
    def _version_check(self, version_number):
        self.log.info('check_schema', msg='Current version set to: %s' % version_number)
        if float(version_number) == CURRENT_SCHEMA_VERSION:
            self.log.info('check_schema', msg='Schema up to date')
            return True
        elif float(version_number) < CURRENT_SCHEMA_VERSION:
            self.log.error('check_schema', \
                msg='Schema version %s found - expecting %s - database admin will need to run upgrade tool' % \
                    (float(version_number), CURRENT_SCHEMA_VERSION))
            return False
            
    def _table_scan(self, sql, table, idx):
        if not self._table_map.has_key(table):
            self._table_map[table] = {}
        for row in self.session.execute(sql).fetchall():
            self._table_map[table][row[idx]] = True
            
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
        elif version_number == 3.2:
            self.log.info('check_schema', msg='Schema set to 3.2 deveopment version - resetting to release version')
        else:
            return self._version_check(version_number)
        
        self.log.info('check_schema', msg='Determining schema version')
        
        table_scan = ['job_instance', 'invocation']
        
        # Due to how the SQLAlchemy mapper works, I need to look at these with
        # raw SQL calls to the DBM and not use the mapper objects.
        for t in table_scan:
            if self.session.connection().dialect.name == 'sqlite':
                self._table_scan('PRAGMA table_info(%s)' % t, t, 1)
            elif self.session.connection().dialect.name == 'mysql':
                self._table_scan('desc %s' % t, t, 0)
            else:
                self.log.error('check_schema', msg='Dialect %s not available for scanning' \
                    % self.session.connection().dialect.name )
        
        #
        # Checks for version 4.0
        #
        
        m_factor_check = exitcode_check = remote_cpu_check = False
        
        # Check job_instance table
        if self._table_map['job_instance'].has_key('multiplier_factor'):
            m_factor_check = True
        if self._table_map['job_instance'].has_key('exitcode'):
            exitcode_check = True
        
        # Check invocation
        if self._table_map['invocation'].has_key('remote_cpu_time'):
            remote_cpu_check = True
        
        s_info = SchemaInfo()
        
        if not m_factor_check and not exitcode_check and not remote_cpu_check:
            self.log.info('check_schema', msg='Setting schema to version 3.1')
            s_info.version_number = 3.1
        elif m_factor_check and exitcode_check and remote_cpu_check:
            s_info.version_number = 4.0
            self.log.info('check_schema', msg='Setting schema to version 4.0')
        else:
            self.log.error('check_schema', msg='Error in determining database schema')
            raise RuntimeError
        
        s_info.commit_to_db(self.session)
        
        #
        # End version 4.0 code
        #
        
        self._table_map = {}
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
            
    def upgrade_to_4_0(self):
        """
        Called by the "upgrade tool" - upgrades a populated 3.1 DB to
        4.0.
        """
        self.log.info('upgrade_to_4_0', msg='Upgrading to schema version 4.0')
        if self._get_current_version() >= 4.0:
            self.log.warn('upgrade_to_4_0', msg='Schema version already 4.0 - skipping upgrade')
            return
        
        # Alter tables
        r_c_t = 'ALTER TABLE invocation ADD COLUMN remote_cpu_time NUMERIC(10,3) NULL'
        if self.session.connection().dialect.name != 'sqlite':
            r_c_t += ' AFTER remote_duration'
        m_fac = 'ALTER TABLE job_instance ADD COLUMN multiplier_factor INT NOT NULL DEFAULT 1'
        e_cod  = 'ALTER TABLE job_instance ADD COLUMN exitcode INT NULL'
        
        self.session.execute(r_c_t)
        self.session.execute(m_fac)
        self.session.execute(e_cod)
        
        # Seed new columns with data derived from existing 3.1 data
        
        success = ['JOB_SUCCESS', 'POST_SCRIPT_SUCCESS']
        failure = ['PRE_SCRIPT_FAILED', 'SUBMIT_FAILED', 'JOB_FAILURE', 'POST_SCRIPT_FAILED']
        
        q = self.session.query(JobInstance.job_instance_id).order_by(JobInstance.job_instance_id)
        for r in q.all():
            qq = self.session.query(Jobstate.state)
            qq = qq.filter(Jobstate.job_instance_id == r.job_instance_id)
            qq = qq.order_by(Jobstate.jobstate_submit_seq.desc()).limit(1)
            for rr in qq.all():
                if rr.state in success:
                    self.session.execute('UPDATE job_instance set exitcode = 0 where job_instance_id = %s' \
                        % r.job_instance_id )
                elif rr.state in failure:
                    self.session.execute('UPDATE job_instance set exitcode = 256 where job_instance_id = %s' \
                    % r.job_instance_id)
                else:
                    pass
        
        s_info = SchemaInfo()
        s_info.version_number = 4.0
        s_info.commit_to_db(self.session)
        pass
        
    def upgrade(self):
        """
        Public wrapper around the version-specific upgrade methods.
        """
        self.check_schema()
        self.upgrade_to_4_0()
        pass

        
def main():
    pass
    
if __name__ == '__main__':
    main()