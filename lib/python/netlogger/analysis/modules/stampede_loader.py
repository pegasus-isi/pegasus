"""
Load input into the Stampede DB schema via a SQLAlchemy interface.  This
is an nl_load module which MUST be invoked with the command-line pair
connString='SQLAlchemy connection string'.  Example:

nl_parse bp pegasus.db | nl_load stampede_loader connString='sqlite:///pegasusTest.db'

The connection string must be of a format that is accepted as the first arg
of the SQLAlchemy create_engine() function.  The database indicated by the 
conection string will be create and populated with tables and indexes
if it does not exist.  If it does exist, it will merely be connected to
and the SQLAlchemy object mappings will be initialized.

This module does not produce any output other than loading the BP data into
the Stampede DB.

See http://www.sqlalchemy.org/ for details on SQLAlchemy
"""
__rcsid__ = "$Id: stampede_loader.py 26919 2010-12-16 23:04:40Z mgoode $"
__author__ = "Monte Goode"

from netlogger.analysis.schema.stampede_schema import *
from netlogger.analysis.modules._base import Analyzer as BaseAnalyzer
from netlogger.analysis.modules._base import SQLAlchemyInit, dsn_dialect
from netlogger import util
import sys
import time

class Analyzer(BaseAnalyzer, SQLAlchemyInit):
    """Load into the Stampede SQL schema through SQLAlchemy.
    
    Parameters:
      - connString {string,None*}: SQLAlchemy connection string.
        The general form of this is 
          'dialect+driver://username:password@host:port/database'.
        See the SQLAlchemy docs for details.
        For sqlite, use 'sqlite:///foo.db' for a relative path and
        'sqlite:////path/to/foo.db' (four slashes) for an absolute one.
        When using MySQL, the general form will work, but the library
        expects the database to exist (ie: will not issue CREATE DB)
        but will populate an empty DB with tables/indexes/etc.
     - mysql_engine {string,None*}: For MySQL, the storage engine. Accepted
       values include 'InnoDB' and 'MyISAM'. See SQLAlchemy/MySQL documentation
       for more details. Ignored if connString does not start with 'mysql'. 
    """
    def __init__(self, connString=None, perf='no', batch='no', mysql_engine=None, **kw):
        """Init object

        @type   connString: string
        @param  connString: SQLAlchemy connection string - REQUIRED
        """
        BaseAnalyzer.__init__(self, **kw)
        _kw = { }
        if connString is None:
            raise ValueError("connString is required")
        dialect = dsn_dialect(connString)
        _kw[dialect] = { }
        if dialect == 'mysql':
            # mySQL-specific options
            if mysql_engine is not None:
                _kw[dialect]['mysql_engine'] = mysql_engine
        # This mixin adds a class member "self.session" after initialization.
        # This is the session handler that the code logic uses for queries
        # and other DB interaction.  The arg "initializeToPegasusDB" is
        # a function from the stampede_schema module.
        SQLAlchemyInit.__init__(self, connString, initializeToPegasusDB, **_kw)

        self.log.info('init.start')

        # "Case" dict to map events to handler methods
        self.eventMap = {
            'stampede.edge': self.edge_static,
            'stampede.host': self.host,
            'stampede.job.mainjob.end': self.job,
            'stampede.job.mainjob.start': self.job,
            'stampede.job.postscript.end': self.noop,
            'stampede.job.postscript.start': self.noop,
            'stampede.job.prescript.end': self.noop,
            'stampede.job.prescript.start': self.job,
            'stampede.job.state': self.jobstate,
            'stampede.task.mainjob': self.task,
            'stampede.task.postscript': self.task,
            'stampede.task.prescript': self.task,
            'stampede.workflow.end': self.workflowstate,
            'stampede.workflow.plan': self.workflow,
            'stampede.workflow.start': self.workflowstate
        }
        
        # Dicts for caching FK lookups
        self.wf_id_cache = {}
        self.job_id_cache = {}
        self.host_cache = {}
        self.hosts_written_cache = {}
        self._wf_pks = []
        
        # undocumented performance option
        self._perf = util.as_bool(perf)
        if self._perf:
            self._insert_time, self._insert_num = 0, 0
            self._start_time = time.time()
        
        # flags and state for batching
        self._batch = util.as_bool(batch)
        self._flush_every = 10000
        self._flush_count = 0
        self._last_flush = time.time()
        # caches for batched events
        self._raw_edges = []
        self._job_update_events = []
        self._jobstate_events = []
        self._host_events = []
        self._host_to_job = []
        self._task_events = []
        self._wfstate_events = []
        
        self.log.info('init.end', msg='Batching: %s' % self._batch)
        
    def process(self, linedata):
        """
        @type   linedata: dict
        @param  linedata: One line of BP data dict-ified.
        
        Get the BP dict from the controlling process and dispatch
        to the appropriate method per-event.
        """
        self.log.debug('process', msg=linedata)
        try:
            if self._perf:
                t = time.time()
                self.eventMap[linedata['event']](linedata)
                self._insert_time += (time.time() - t)
                self._insert_num += 1
            else:
                self.eventMap[linedata['event']](linedata)
        except KeyError:
            # Raised if self.eventMap does not have an entry for
            # the passed in event.
            self.log.error('process', 
                msg='no handler for event type "%s" defined' % linedata['event'])
        except exceptions.IntegrityError, e:
            # This is raised when an attempted insert violates the
            # schema (unique indexes, etc).
            self.log.error('process',
                msg='Insert failed for event "%s" : %s' % (linedata['event'], e))
            self.session.rollback()
            
        self.check_flush()
        
    def linedataToObject(self, linedata, o):
        """
        @type   linedata: dict
        @param  linedata: One line of BP data dict-ified.
        @type   o: instance of mapper class from stampede_schema module.
        @param  o: Passed in by the appropriate event handler method.
        
        Takes the dict of BP linedata, assigns contents to the class o
        as attributes, and does any global type massaging like 
        transforming dict strings to numeric types.
        """
        for k,v in linedata.items():
            if k == 'level':
                continue
            if k == 'wf.id':
                k = 'wf_uuid'
            if k == 'condor.id':
                k = 'condor_id'
            if k == 'job.id':
                k = 'job_submit_seq'
            if k == 'task.id':
                k = 'task_submit_seq'
            if k == 'js.id':
                k = 'jobstate_submit_seq'
            if k == 'parent.wf.id':
                k = 'parent_workflow_id'
            if k == 'arguments':
                if v != None:
                    v = v.replace("'", "\\'")
            
            try:
                exec("o.%s = '%s'" % (k,v))
            except:
                self.log.error('linedataToObject', 
                    msg='unable to process attribute %s with values: %s' % (k,v))
        
        # global type re-assignments
        if hasattr(o, 'ts'):
            # make all timestamp values floats
            o.ts = float(o.ts)
        if hasattr(o, 'start_time') and o.start_time != None:
            o.start_time = float(o.start_time)
        if hasattr(o, 'cluster_start_time') and o.cluster_start_time != None:
            o.cluster_start_time = float(o.cluster_start_time)
        if hasattr(o, 'cluster_duration') and o.cluster_duration != None:
            o.cluster_duration = float(o.cluster_duration)
        if hasattr(o, 'duration') and o.duration != None:
            o.duration = float(o.duration)
        if hasattr(o, 'restart_count') and o.restart_count != None:
            o.restart_count = int(o.restart_count)
        return o
        
    #############################################
    # Methods to handle batching/flushing
    #############################################
    
    def reset_flush_state(self):
        """
        Reset the internal flust state if batching.
        """
        if self._batch:
            self.log.debug('reset_flush_state', msg='Resetting flush state')
            self._flush_count = 0
            self._last_flush = time.time()
            
    def check_flush(self):
        """
        Check to see if the batch needs to be flushed based on
        either the number of queued inserts or based on time 
        since last flush.
        """
        if not self._batch:
            return
        
        if self._flush_count >= self._flush_every:
            self.hard_flush()
            self.log.debug('reset_flush_state', msg='Flush: flush count')
            return
        else:
            self._flush_count += 1
            
        if (time.time() - self._last_flush) > 30:
            self.hard_flush()
            self.log.debug('reset_flush_state', msg='Flush: time based')
            
    def hard_flush(self, batch_flush=True):
        """
        @type   batch_flush: boolean
        @param  batch_flush: Defaults to true.  Is set to false
            when the batch commit hits and integrity error.
            
        Process queued inserts and flush/commit to the database.
        If the commit fails due to an integrity error, then method
        re-calls itself with setting batch_flush to False which 
        causes each insert/object to be committed individually
        so all the "good" inserts can succeed.  This will increase
        the processing time of the batch with the bad data in it.
        """
        if not self._batch:
            return
        self.log.debug('hard_flush.begin', batching=batch_flush)
        
        # leaving all of these distinct rather than putting
        # everyting in two lists for the moment
        for re in self._raw_edges:
            if batch_flush:
                self.session.add(re)
            else:
                self.individual_commit(re)
        
        for js in self._jobstate_events:
            if batch_flush:
                self.session.add(js)
            else:
                self.individual_commit(js)
        
        for tsk in self._task_events:
            if batch_flush:
                self.session.add(tsk)
            else:
                self.individual_commit(tsk)
        
        for wfs in self._wfstate_events:
            if batch_flush:
                self.session.add(wfs)
            else:
                self.individual_commit(wfs)
        
        for j in self._job_update_events:
            if batch_flush:
                self.session.merge(j)
            else:
                self.individual_commit(j, merge=True)
        
        for h in self._host_events:
            if batch_flush:
                self.session.add(h)
            else:
                self.individual_commit(h)
                
        try:
            self.session.commit()
        except exceptions.IntegrityError, e:
            self.log.error('batch_flush', 
                msg='Integrity error on batch flush: %s - batch will need to be committed per-event which will take longer' % e)
            self.session.rollback()
            self.hard_flush(batch_flush=False)
        
        self._raw_edges = []
        self._jobstate_events = []
        self._task_events = []
        self._wfstate_events = []
        self._job_update_events = []
        self._host_events = []
        
        for htj in self._host_to_job:
            self.mapHostToJob(htj)
        self._host_to_job = []
        
        self.session.commit()
        self.reset_flush_state()
        self.log.debug('hard_flush.end', batching=batch_flush)
        
    def individual_commit(self, event, merge=False):
        """
        @type   merge: boolean
        @param  merge: Set to true if the row should be a merge
                rather than a plain insert.
        
        This gets called by hard_flush if there is a problem
        with a batch commit to commit each object individually.
        """
        try:
            if merge:
                event.merge_to_db(self.session)
            else:
                event.commit_to_db(self.session)
        except exceptions.IntegrityError, e:
            self.log.error('individual_commit', msg='Insert failed for event %s : %s' % (event,e))
            self.session.rollback()
            
        
    #############################################
    # Methods to handle the various insert events
    #############################################
    def workflow(self, linedata):
        """
        @type   linedata: dict
        @param  linedata: One line of BP data dict-ified.
        
        Handles a workflow insert event.
        """
        wf = self.linedataToObject(linedata, Workflow())
        wf.timestamp = wf.ts
        if wf.parent_workflow_id == 'None':
            wf.parent_workflow_id = None
        else:
            wf.parent_workflow_id = self.wf_uuidToId(wf.parent_workflow_id)
        del wf.event, wf.ts
        self.log.debug('workflow', msg=wf)
        # workflow inserts must be explicitly written to db
        if self._batch:
            wf.commit_to_db(self.session)
        else:
            wf.commit_to_db(self.session)
        self._wf_pks.append(wf.wf_id)
        pass
        
    def workflowstate(self, linedata):
        """
        @type   linedata: dict
        @param  linedata: One line of BP data dict-ified.
        
        Handles a workflowstate insert event.
        """
        wfs = self.linedataToObject(linedata, Workflowstate())
        wfs.state = wfs.event[wfs.event.rfind('.')+1:]
        wfs.wf_id = self.wf_uuidToId(wfs.wf_uuid)
        wfs.timestamp = wfs.ts
        # XXX: this test is a band aid while people transition to
        # the newer log style.
        if hasattr(wfs, 'restart_count'):
            if wfs.restart_count > 0:
                wfs.state += ' restart=%s' % wfs.restart_count
                del wfs.restart_count
        else:
            self.log.warn('workflowstate', 
                msg='Workflow state event lacks restart_count attribute.  Reprocess log with updated monitord.')
        del wfs.event, wfs.ts
        self.log.debug('workflowstate', msg=wfs)
        if self._batch:
            self._wfstate_events.append(wfs)
        else:
            wfs.commit_to_db(self.session)
        
        if wfs.state.startswith('end'):
            self.flushCaches(wfs)
        pass
        
    def job(self, linedata):
        """
        @type   linedata: dict
        @param  linedata: One line of BP data dict-ified.
        
        Handles a job insert event.
        """
        job = self.linedataToObject(linedata, Job())
        
        # get wf_id
        job.wf_id = self.wf_uuidToId(job.wf_uuid)
        if job.wf_id == None:
            er = 'No wf_id associated with wf_uuid %s - can not insert job %s' \
                % (job.wf_uuid, job)
            self.log.error('job', msg=er)
            return
            
        if job.name.startswith('merge_'):
            job.clustered = True
        else:
            job.clustered = False
        
        del job.ts, job.event
        
        self.log.debug('job', msg=job)
        
        # See if this is the initial entry or an update
        if not self.job_id_cache.has_key((job.wf_id, job.job_submit_seq)):
            jcheck = self.session.query(Job).filter(Job.wf_id == job.wf_id).filter(Job.job_submit_seq == job.job_submit_seq).first()
            if not jcheck:
                # A job entry does not exist so insert
                # job inserts must be explicitly flushed to db
                # whether batching or not
                job.commit_to_db(self.session)
                self.job_id_cache[(job.wf_id, job.job_submit_seq)] = job.job_id
                self.log.debug('job', msg='Inserting new jobid: %s' % job.job_id)
            else:
                # A job entry EXISTS but not cached probably due to 
                # an interrupted run.  Bulletproofing.  Raises a warning
                # as this is a non-optimal state.
                self.job_id_cache[(jcheck.wf_id, jcheck.job_submit_seq)] = jcheck.job_id
                job.job_id = jcheck.job_id
                if self._batch:
                    self._job_update_events.append(job)
                else:
                    job.merge_to_db(self.session)
                self.log.warn('job', msg='Updating non-cached job: %s' % job)
        else:
            jid = self.jobIdFromUnique(job)
            self.log.debug('job', msg='Updating jobid: %s' % jid)
            job.job_id = jid # set PK from cache for merge
            if self._batch:
                self._job_update_events.append(job)
            else:
                job.merge_to_db(self.session)
        pass
    
    def jobstate(self, linedata):
        """
        @type   linedata: dict
        @param  linedata: One line of BP data dict-ified.
        
        Handles a jobstate insert event.
        """
        js = self.linedataToObject(linedata, Jobstate())
        js.timestamp = js.ts

        js.job_id = self.jobIdFromUnique(js)
        if js.job_id == None:
            self.log.error('jobstate',
                msg='Could not determine job_id for jobstate: %s' % js)
            return
        del js.name, js.wf_uuid, js.job_submit_seq, js.ts, js.event
        
        if self._batch:
            self._jobstate_events.append(js)
            return
        
        self.log.debug('jobstate', msg=js)
        js.commit_to_db(self.session)
        pass
        
    def task(self, linedata):
        """
        @type   linedata: dict
        @param  linedata: One line of BP data dict-ified.
        
        Handles a task insert event.
        """
        tsk = self.linedataToObject(linedata, Task())

        if tsk.duration == None:
            tsk.duration = 0
            self.log.warn('task',
                msg='Task event lacked duration - setting to zero')
                
        if tsk.executable == None:
            tsk.executable = ''
            self.log.warn('task',
                msg='Task event lacked executable - setting to empty string')
        
        tsk.job_id = self.jobIdFromUnique(tsk)
        
        if tsk.job_id == None:
            self.log.error('task',
                msg='Could not determine job_id for task: %s' % tsk)
            return
        del tsk.wf_uuid, tsk.name, tsk.job_submit_seq, tsk.ts
        
        if self._batch:
            self._task_events.append(tsk)
            return
        
        self.log.debug('task', msg=tsk)
        tsk.commit_to_db(self.session)
        pass
        
    def host(self, linedata):
        """
        @type   linedata: dict
        @param  linedata: One line of BP data dict-ified.
        
        Handles a host insert event.
        """
        host = self.linedataToObject(linedata, Host())
        del host.ts, host.event, host.name
        
        self.log.debug('host', msg=host)
        
        if self._batch:
            if not self.hosts_written_cache.has_key((host.site_name,host.hostname,host.ip_address)):
                self._host_events.append(host)
                self.hosts_written_cache[(host.site_name,host.hostname,host.ip_address)] = True
            self._host_to_job.append(host)
            return
        try:
            # host info needs to be written to the db
            host.commit_to_db(self.session)
        except exceptions.IntegrityError, e:
            # In this case, catch the duplicate insert exception
            # and ignore - we are bound to see duplicate host events
            # during a load.
            self.session.rollback()
        
        self.mapHostToJob(host)
        pass
        
    def edge_static(self, linedata):
        """
        @type   linedata: dict
        @param  linedata: One line of BP data dict-ified.
        
        Handles a static edge insert event.
        """
        edge = self.linedataToObject(linedata, EdgeStatic())
        del edge.ts, edge.event
        
        self.log.debug('edge_static', msg=edge)
        if self._batch:
            self._raw_edges.append(edge)
        else:
            edge.commit_to_db(self.session)
        pass
        
    def noop(self, linedata):
        """
        @type   linedata: dict
        @param  linedata: One line of BP data dict-ified.
        
        A NOOP method for events that are being ignored.
        """
        self.log.debug('noop', msg=linedata)
        pass

    ####################################
    # DB helper/lookup/caching functions
    ####################################
    def wf_uuidToId(self, wf_uuid):
        """
        @type   wf_uuid: string
        @param  wf_uuid: wf_uuid string from BP logs
        
        Attempts to retrieve a workflow wf_id PK/FK from cache.  If
        not in cache, retrieve from st_workflow table in DB and cache.  
        Cuts down on DB queries during insert processing.
        """
        if not self.wf_id_cache.has_key(wf_uuid):
            query = self.session.query(Workflow).filter(Workflow.wf_uuid == wf_uuid)
            try:
                self.wf_id_cache[wf_uuid] = query.one().wf_id
            except orm.exc.MultipleResultsFound, e:
                self.log.error('wf_uuidToId', 
                    msg='Multiple wf_id results for wf_uuid %s : %s' % (wf_uuid, e))
                return None
            except orm.exc.NoResultFound, e:
                self.log.error('wf_uuidToId',
                    msg='No wf_id results for wf_uuid %s : %s' % (wf_uuid, e))
                return None
                pass
            
        return self.wf_id_cache[wf_uuid]
        
    def jobIdFromUnique(self, o):
        """
        @type   o: class instance
        @param  o: Mapper object containing wf_uuid, condor_id and job name.
        
        Attempts to retrieve a job job_id PK/FK from cache.  If not in
        cache, retrieve from st_job table by the three unique columns and 
        cache.  Cuts down on DB queries, especially when inserting jobstate
        events, during insert processing.
        """
        wf_id = self.wf_uuidToId(o.wf_uuid)
        uniqueIdIdx = (wf_id, o.job_submit_seq)
        if not self.job_id_cache.has_key(uniqueIdIdx):
            query = self.session.query(Job).filter(Job.wf_id == wf_id).filter(Job.job_submit_seq == o.job_submit_seq)
            try:
                self.job_id_cache[uniqueIdIdx] = query.one().job_id
            except orm.exc.MultipleResultsFound, e:
                self.log.error('jobIdFromUnique',
                    msg='Multple job_id results for tuple %s : %s' % (uniqueIdIdx, e))
                return None
            except orm.exc.NoResultFound, e:
                self.log.error('jobIdFromUnique',
                    msg='No job_id results for tuple %s : %s' % (uniqueIdIdx, e))
                return None
                    
            
        return self.job_id_cache[uniqueIdIdx]
        
    def mapHostToJob(self, host):
        """
        @type   host: class instance of stampede_schema.Host
        @param  host: Host object with info from a host event in the log
        
        A single job may have multiple (redundant) host events.  This
        checks the cache to see if a job had already had its host_id,
        and if not, do the proper update and note it in the cache.
        """
        self.log.debug('mapHostToJob', msg=host)
        if not self.host_cache.has_key((host.wf_uuid, host.job_submit_seq)):
            if not host.host_id:
                try:
                    host.host_id = self.session.query(Host.host_id).filter(Host.site_name == host.site_name).filter(Host.hostname == host.hostname).filter(Host.ip_address == host.ip_address).one().host_id
                except orm.exc.MultipleResultsFound, e:
                    self.log.error('mapHostToJob',
                        msg='Multiple host_id results for host: %s' % host)
            wf_id = self.wf_uuidToId(host.wf_uuid)
            job = self.session.query(Job).filter(Job.wf_id == wf_id).filter(Job.job_submit_seq == host.job_submit_seq).one()
            job.host_id = host.host_id
            job.merge_to_db(self.session, batch=self._batch)
            self.host_cache[(host.wf_uuid, host.job_submit_seq)] = True
        
        pass
        
    def mapEdges(self):
        """
        A post-processing step to build the edge table after all the
        other data has been written out.
        """
        s = time.time()
        wf_map = {}
        for k,v in self.wf_id_cache.items():
            wf_map[v] = k
        query = self.session.query(Job.job_id, Job.name, Job.wf_id).filter(Job.wf_id.in_(wf_map.keys())).order_by(Job.job_id)
        for job in query.all():
            self.log.debug('mapEdges', msg='Finding edges for job %s (job_id: %s)' % (job.name, job.job_id))
            edgeq = self.session.query(EdgeStatic.parent).filter(EdgeStatic.wf_uuid == wf_map[job.wf_id]).filter(EdgeStatic.child == job.name)
            for parent in edgeq.all():
                parentName = parent[0]
                parentq = self.session.query(Job.job_id).filter(Job.name == parentName).filter(Job.wf_id == job.wf_id)
                for parentid in parentq.all():
                    edge = Edge()
                    edge.parent_id = parentid[0]
                    edge.child_id = job.job_id
                    edge.commit_to_db(self.session, batch=self._batch)
        if self._batch:
            self.session.commit()
        e = time.time()
        self.log.info('mapEdges', msg='Edges took: %d sec' % (e-s))
        pass
        
    def flushCaches(self, wfs):
        """
        @type   wfs: class instance of stampede_schema.Workflowstate
        @param  wfs: Workflow state object from an end event.
        
        Flushes information from the lookup caches after a workflow.end
        event has been recieved.
        """
        self.log.debug('flushCaches', msg='Flushing caches for: %s' % wfs)
        for k,v in self.wf_id_cache.items():
            if k == wfs.wf_uuid:
                pass
                #del self.wf_id_cache[k]
                
        for k,v in self.job_id_cache.items():
            if k[0] == wfs.wf_id:
                del self.job_id_cache[k]
        
        for k,v in self.host_cache.items():
            if k[0] == wfs.wf_uuid:
                del self.host_cache[k]
        
        pass
        
    ################
    # Cleanup, etc
    ################
        
    def finish(self):
        BaseAnalyzer.finish(self)
        if self._batch:
            self.log.info('finish', msg='Executing final flush')
            self.hard_flush()
        self.mapEdges()
        if self._perf:
            run_time = time.time() - self._start_time
            self.log.info("performance", insert_time=self._insert_time,
                          insert_num=self._insert_num, 
                          total_time=run_time, 
                          run_time_delta=run_time - self._insert_time,
                          mean_time=self._insert_time / self._insert_num)

def main():
    if os.path.exists('pegasusTest.db'):
        os.unlink('pegasusTest.db')
    loader = Analyzer('sqlite:///pegasusTest.db')
    f = open('pegasus.db.log', 'rU')
    for line in f.readlines():
        rowdict = {}
        for l in line.strip().split(' '):
            k,v = l.split('=')
            rowdict[k] = v
        loader.process(rowdict)
    pass
    
if __name__ == '__main__':
    main()
