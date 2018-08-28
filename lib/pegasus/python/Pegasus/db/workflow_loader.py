__author__ = "Monte Goode"
__author__ = "Karan Vahi"

from Pegasus.db.schema import *
from Pegasus.db.base_loader import BaseLoader
from Pegasus.netlogger import util
from sqlalchemy import exc
import time

class WorkflowLoader(BaseLoader):
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
    """

    MAX_RETRIES = 10 # maximum number of retries in case of operational errors that arise because of database locked/connection dropped

    def __init__(self, connString, perf=False, batch=False, props=None, db_type=None, backup=False):
        """Init object

        @type   connString: string
        @param  connString: SQLAlchemy connection string - REQUIRED
        """
        super(WorkflowLoader, self).__init__(connString, batch=batch, props=props, db_type=db_type, backup=backup,
                                             flush_every=1000)

        # "Case" dict to map events to handler methods
        self.eventMap = {
            'stampede.wf.plan' : self.workflow,
            'stampede.wf.map.task_job' : self.task_map,
            'stampede.static.start' : self.noop, # good
            'stampede.static.end' : self.static_end,
            'stampede.xwf.start' : self.workflowstate,
            'stampede.xwf.end' : self.workflowstate,
            'stampede.xwf.map.subwf_job' : self.subwf_map,
            'stampede.task.info' : self.task,
            'stampede.task.edge' : self.task_edge,
            'stampede.job.info' : self.job,
            'stampede.job.edge' : self.job_edge,
            'stampede.job_inst.pre.start' : self.job_instance,
            'stampede.job_inst.pre.term' : self.jobstate,
            'stampede.job_inst.pre.end' : self.jobstate,
            'stampede.job_inst.submit.start' : self.job_instance,
            'stampede.job_inst.submit.end' : self.jobstate,
            'stampede.job_inst.held.start' : self.jobstate,
            'stampede.job_inst.held.end' : self.jobstate,
            'stampede.job_inst.main.start' : self.jobstate,
            'stampede.job_inst.main.term' : self.jobstate,
            'stampede.job_inst.main.end' : self.job_instance,
            'stampede.job_inst.post.start' : self.jobstate,
            'stampede.job_inst.post.term' : self.jobstate,
            'stampede.job_inst.post.end' : self.job_instance,
            'stampede.job_inst.host.info' : self.host,
            'stampede.job_inst.image.info' : self.jobstate,
            'stampede.job_inst.abort.info' : self.jobstate,
            'stampede.job_inst.grid.submit.start' : self.noop, # good
            'stampede.job_inst.grid.submit.end' : self.jobstate,
            'stampede.job_inst.globus.submit.start' : self.noop, # good
            'stampede.job_inst.globus.submit.end' : self.jobstate,
            'stampede.job_inst.tag' : self.tag,
            'stampede.inv.start' : self.noop, # good
            'stampede.inv.end' : self.invocation,
            'stampede.static.meta.start': self.static_meta_start,
            'stampede.xwf.meta' : self.workflow_meta,
            'stampede.task.meta' : self.task_meta,
            'stampede.rc.meta'   : self.rc_meta,
            'stampede.int.metric'  : self.int_metric,
            'stampede.rc.pfn'    : self.rc_pfn,
            'stampede.wf.map.file' : self.wf_task_file_map,
            'stampede.static.meta.end': self.noop,
            'stampede.task.monitoring': self.noop,
        }

        # Dicts for caching FK lookups
        self.wf_id_cache = {}
        self.root_wf_id_cache = {}
        self.task_id_cache = {} #for task metadata population
        self.lfn_id_cache  = {} #for file metadata population
        self.job_id_cache = {}
        self.job_instance_id_cache = {}
        self.host_cache = {}
        self.hosts_written_cache = None

        # undocumented performance option
        self._perf = perf
        if self._perf:
            self._insert_time, self._insert_num = 0, 0
            self._start_time = time.time()

        # caches for batched events
        self._batch_cache = {
            'batch_events' : [],
            'update_events' : [],
            'host_map_events' : []
        }
        self._task_map_flush = {}
        self._task_edge_flush = {}

    def process(self, linedata):
        """
        @type   linedata: dict
        @param  linedata: One line of BP data dict-ified.

        Get the BP dict from the controlling process and dispatch
        to the appropriate method per-event.
        """
        self.log.trace("Process: %s", linedata)

        for retry in range( 1, self.MAX_RETRIES + 1):
            if not self._batch:
                self.check_connection()

            try:
                if self._perf:
                    t = time.time()
                    self.eventMap[linedata['event']](linedata)
                    self._insert_time += (time.time() - t)
                    self._insert_num += 1
                else:
                    self.eventMap[linedata['event']](linedata)

            except KeyError:
                if linedata['event'].startswith('stampede.job_inst.'):
                    self.log.warning('Corner case jobstate event: "%s"', linedata['event'])
                    self.jobstate(linedata)
                else:
                    self.log.error('No handler for event type "%s" defined', linedata['event'])
            except exc.IntegrityError as e:
                # This is raised when an attempted insert violates the
                # schema (unique indexes, etc).
                self.log.exception(e)
                self.log.error('Insert failed for event "%s"', linedata['event'])
                self.session.rollback()
            except exc.OperationalError as e:
                self.log.error('Connection seemingly lost - attempting to refresh. Retry %s' %retry)
                self.session.rollback()
                self.check_connection()
                #PM-1013 retry only in case of operational errors
                continue
            # the current attempt was successful or there was integrity error/key error. exit the loop
            break
        else:
            #loop finished after all retries have been made
            self.log.error( 'Maximum number of retries reached for stampede_loader.process() method %s' %retry)
            raise RuntimeError( 'Maximum number of retries reached for stampede_loader.process() method %s' %retry)

        self.check_flush(increment=True)

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

            # undot
            attr = k.replace('.', '_')

            attr_remap = {
                # workflow
                'xwf_id': 'wf_uuid',
                'parent_xwf_id': 'parent_wf_id',
                # task.info
                'task_id': 'abs_task_id',
                # task.edge
                'child_task_id': 'child_abs_task_id',
                'parent_task_id': 'parent_abs_task_id',
                # job.info
                'job_id': 'exec_job_id',
                # job.edge
                'child_job_id': 'child_exec_job_id',
                'parent_job_id': 'parent_exec_job_id',
                # xwf.start/end (none)
                # job_inst.submit.start/job_inst.submit.start/etc
                'job_inst_id': 'job_submit_seq',
                'js_id': 'jobstate_submit_seq',
                'cluster_dur': 'cluster_duration',
                'local_dur': 'local_duration',
                # inv.end
                'inv_id': 'task_submit_seq',
                'dur': 'remote_duration',
            }

            # remap attr names
            if attr in attr_remap:
                attr = attr_remap[attr]

            # sanitize argv input
            if attr == 'argv':
                if v != None:
                    v = v.replace("\\", "\\\\" )
                    v = v.replace("'", "\\'")

            try:
                setattr(o, attr, v)
            except:
                self.log.error('Unable to process attribute %s with values: %s', k, v)

        # global type re-assignments
        if hasattr(o, 'ts'):
            # make all timestamp values floats
            o.ts = float(o.ts)
        if hasattr(o, 'start_time') and o.start_time != None:
            o.start_time = float(o.start_time)
        if hasattr(o, 'cluster_start_time') and o.cluster_start_time != None:
            o.cluster_start_time = float(o.cluster_start_time)
        if hasattr(o, 'duration') and o.duration != None:
            o.duration = float(o.duration)
        if hasattr(o, 'restart_count') and o.restart_count != None:
            o.restart_count = int(o.restart_count)
        return o

    #############################################
    # Methods to handle batching/flushing
    #############################################

    def hard_flush(self, batch_flush=True, retry=0):
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

        self.log.debug('Hard flush: batch_flush=%s', batch_flush)

        if retry == self.MAX_RETRIES + 1:
            #PM-1013 see if max retries is reached
            self.log.error( 'Maximum number of retries reached for workflow_loader.hard_flush() method %s' %self.MAX_RETRIES )
            raise RuntimeError( 'Maximum number of retries reached for workflow_loader.hard_flush() method %s' %self.MAX_RETRIES )

        retry = retry + 1
        self.check_connection()

        if self._perf:
            s = time.time()

        end_event = []

        self.log.debug('Batch event sizes: batch_event_size=%s update_event_size=%s',
                len(self._batch_cache['batch_events']),
                len(self._batch_cache['update_events']))

        for event in self._batch_cache['batch_events']:
            if event.event == 'stampede.xwf.end':
                end_event.append(event)
            if batch_flush:
                self.session.add(event)
            else:
                self.individual_commit(event)

        for event in self._batch_cache['update_events']:
            if batch_flush:
                self.session.merge(event)
            else:
                self.individual_commit(event, merge=True)

        try:
            self.session.commit()
        except exc.IntegrityError as e:
            self.log.exception(e)
            self.log.error('Integrity error on batch flush: batch will need to be committed per-event which will take longer')
            self.session.rollback()
            self.hard_flush(batch_flush=False, retry=retry)
        except exc.OperationalError as e:
            self.log.exception(e)
            self.log.error('Connection problem during commit in hard_flush(): reattempting batch. Retry %s' %retry)
            self.session.rollback()
            self.hard_flush(retry=retry)

        for host in self._batch_cache['host_map_events']:
            self.map_host_to_job_instance(host)

        for ee in end_event:
            self.purgeCaches(ee)
        end_event = []

        # Clear all data structures here.
        for k in self._batch_cache.keys():
            self._batch_cache[k] = []

        try:
            # commit the map host to job events . no retries for this.
            self.session.commit()
        except exc.IntegrityError as e:
            self.log.exception(e)
            self.log.error('Integrity error on host_map_events in hard_flush()')
            self.session.rollback()
        except exc.OperationalError as e:
            self.log.exception(e)
            self.log.error('Connection problem on host_map_events during commit in hard_flush()')
            self.session.rollback()

        self.reset_flush_state()
        self.log.debug('Hard flush end')

        if self._perf:
            self.log.debug('Hard flush duration: %s', (time.time() - s))

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
        self.log.trace("Workflow: %s", wf)

        wf.timestamp = wf.ts
        wf.planner_arguments = wf.argv

        is_root = True
        if wf.root_xwf_id != wf.wf_uuid:
            is_root = False
            wf.root_wf_id = self.wf_uuid_to_id(wf.root_xwf_id)

        if wf.parent_wf_id is not None:
            wf.parent_wf_id = self.wf_uuid_to_id(wf.parent_wf_id)

        # workflow inserts must be explicitly written to db whether
        # batching or not
        wf.commit_to_db(self.session)
        if is_root:
            wf.root_wf_id = self.wf_uuid_to_id(wf.root_xwf_id)
            wf.commit_to_db(self.session)
        if wf.root_wf_id == None:
            self.log.warn('Count not determine root_wf_id for event %s', wf)

    def workflowstate(self, linedata):
        """
        @type   linedata: dict
        @param  linedata: One line of BP data dict-ified.

        Handles a workflowstate insert event.
        """
        wfs = self.linedataToObject(linedata, Workflowstate())
        self.log.trace('workflowstate: %s', wfs)

        state = {
            'stampede.xwf.start': 'WORKFLOW_STARTED',
            'stampede.xwf.end': 'WORKFLOW_TERMINATED'
        }

        wfs.wf_id = self.wf_uuid_to_id(wfs.wf_uuid)
        wfs.timestamp = wfs.ts
        wfs.state = state[wfs.event]

        if self._batch:
            self._batch_cache['batch_events'].append(wfs)
        else:
            wfs.commit_to_db(self.session)
            if wfs.event == 'stampede.xwf.end':
                self.purgeCaches(wfs)

    def workflow_meta(self, linedata):
        """

        :param linedata:  dictionary of netlogger BP data dict-ified
        :return:

        Handles a workflow metadata insert event.
        """
        wf_meta = self.linedataToObject( linedata, WorkflowMeta() )
        wf_meta.wf_id = self.wf_uuid_to_id( wf_meta.wf_uuid )

        self.log.trace('workflowmeta: %s', wf_meta)

        if self._batch:
            self._batch_cache['batch_events'].append(wf_meta)
        else:
            wf_meta.commit_to_db(self.session)

    def job(self, linedata):
        """
        @type   linedata: dict
        @param  linedata: One line of BP data dict-ified.

        Handles a static job insert event.
        """
        job = self.linedataToObject(linedata, Job())
        job.wf_id = self.wf_uuid_to_id(job.wf_uuid)
        job.clustered = util.as_bool(job.clustered)
        self.log.trace('job: %s', job)

        if self._batch:
            self._batch_cache['batch_events'].append(job)
        else:
            job.commit_to_db(self.session)

    def job_edge(self, linedata):
        """
        @type   linedata: dict
        @param  linedata: One line of BP data dict-ified.

        Handles a static job edge insert event.
        """
        je = self.linedataToObject(linedata, JobEdge())
        je.wf_id = self.wf_uuid_to_id(je.wf_uuid)
        self.log.trace('job_edge: %s', je)

        if self._batch:
            self._batch_cache['batch_events'].append(je)
        else:
            je.commit_to_db(self.session)

    def job_instance(self, linedata):
        """
        @type   linedata: dict
        @param  linedata: One line of BP data dict-ified.

        Handles a job instance insert event.
        """
        job_instance = self.linedataToObject(linedata, JobInstance())
        self.log.trace('job_instance: %s', job_instance)

        job_instance.wf_id = self.wf_uuid_to_id(job_instance.wf_uuid)
        if job_instance.wf_id == None:
            self.log.error('No wf_id associated with wf_uuid %s - can not insert job instance %s', job_instance.wf_uuid, job_instance)
            return

        job_instance.job_id = self.get_job_id(job_instance.wf_id, job_instance.exec_job_id)
        if not job_instance.job_id:
            self.log.error('Could not determine job_id for job_instance: %s', job_instance)
            return

        if job_instance.event == 'stampede.job_inst.submit.start' or \
            job_instance.event == 'stampede.job_inst.pre.start':

            iid = self.get_job_instance_id(job_instance, quiet=True)

            if not iid:
                # explicit insert
                job_instance.commit_to_db(self.session)
                # seed the cache
                noop = self.get_job_instance_id(job_instance)

            if job_instance.event == 'stampede.job_inst.pre.start':
                self.jobstate(linedata)
            return

        if job_instance.event == 'stampede.job_inst.main.end' or \
            job_instance.event == 'stampede.job_inst.post.end':

            job_instance.job_instance_id = self.get_job_instance_id(job_instance)

            if self._batch:
                self._batch_cache['update_events'].append(job_instance)
            else:
                job_instance.merge_to_db(self.session)
            self.jobstate(linedata)

    def jobstate(self, linedata):
        """
        @type   linedata: dict
        @param  linedata: One line of BP data dict-ified.

        Handles a jobstate insert event.
        """
        js = self.linedataToObject(linedata, Jobstate())
        self.log.trace('jobstate: %s', js)

        states = {
            # array maps to status [-1, 0]
            'stampede.job_inst.pre.start' : ['PRE_SCRIPT_STARTED', 'PRE_SCRIPT_STARTED'], # statusless
            'stampede.job_inst.pre.term' : ['PRE_SCRIPT_TERMINATED', 'PRE_SCRIPT_TERMINATED'], # s-less
            'stampede.job_inst.pre.end' : ['PRE_SCRIPT_FAILED', 'PRE_SCRIPT_SUCCESS'],
            'stampede.job_inst.submit.end' : ['SUBMIT_FAILED', 'SUBMIT'],
            'stampede.job_inst.main.start' : ['EXECUTE', 'EXECUTE'], # s-less
            'stampede.job_inst.main.term' : ['JOB_EVICTED', 'JOB_TERMINATED'],
            'stampede.job_inst.main.end' : ['JOB_FAILURE', 'JOB_SUCCESS'],
            'stampede.job_inst.post.start' : ['POST_SCRIPT_STARTED', 'POST_SCRIPT_STARTED'], # s-less
            'stampede.job_inst.post.term' : ['POST_SCRIPT_TERMINATED', 'POST_SCRIPT_TERMINATED'], # s-less
            'stampede.job_inst.post.end' : ['POST_SCRIPT_FAILED', 'POST_SCRIPT_SUCCESS'],
            'stampede.job_inst.held.start' : ['JOB_HELD', 'JOB_HELD'], # s-less
            'stampede.job_inst.held.end' : ['JOB_RELEASED', 'JOB_RELEASED'], # s-less
            'stampede.job_inst.image.info' : ['IMAGE_SIZE', 'IMAGE_SIZE'], # s-less
            'stampede.job_inst.abort.info' : ['JOB_ABORTED', 'JOB_ABORTED'], # s-less
            'stampede.job_inst.grid.submit.end' : ['GRID_SUBMIT_FAILED', 'GRID_SUBMIT'],
            'stampede.job_inst.globus.submit.end' : ['GLOBUS_SUBMIT_FAILED', 'GLOBUS_SUBMIT'],

        }

        if js.event not in states:
            # corner case event
            js.state = js.event.split('.')[2].upper()
        else:
            # doctor status-less events to simplify code
            if not hasattr(js, 'status'): js.status = 0
            js.state = states[js.event][int(js.status)+1]


        js.job_instance_id = self.get_job_instance_id(js)
        if not js.job_instance_id:
            self.log.error('No job_instance_id for event: %s -%s', linedata, js)
            return

        js.timestamp = js.ts

        if self._batch:
            self._batch_cache['batch_events'].append(js)
        else:
            js.commit_to_db(self.session)

    def invocation(self, linedata):
        """
        @type   linedata: dict
        @param  linedata: One line of BP data dict-ified.

        Handles a invocation insert event.
        """
        invocation = self.linedataToObject(linedata, Invocation())
        self.log.trace('invocation: %s', invocation)

        invocation.wf_id = self.wf_uuid_to_id(invocation.wf_uuid)

        invocation.job_instance_id = self.get_job_instance_id(invocation)
        if invocation.job_instance_id == None:
            self.log.error('Could not determine job_instance_id for invocation: %s', invocation)
            return

        if self._batch:
            self._batch_cache['batch_events'].append(invocation)
        else:
            invocation.commit_to_db(self.session)

    def task(self, linedata):
        """
        @type   linedata: dict
        @param  linedata: One line of BP data dict-ified.

        Handles a task insert event
        """
        task = self.linedataToObject(linedata, Task())
        self.log.trace('task: %s', task)
        task.wf_id = self.wf_uuid_to_id(task.wf_uuid)

        if self._batch:
            self._batch_cache['batch_events'].append(task)
        else:
            task.commit_to_db(self.session)

    def task_edge(self, linedata):
        """
        @type   linedata: dict
        @param  linedata: One line of BP data dict-ified.

        Handles a task edge insert event
        """
        if linedata['xwf.id'] not in self._task_edge_flush:
            if self._batch:
                self.hard_flush()
            self._task_edge_flush[linedata['xwf.id']] = True

        te = self.linedataToObject(linedata, TaskEdge())
        self.log.trace('task_event: %s', te)
        te.wf_id = self.wf_uuid_to_id(te.wf_uuid)

        if self._batch:
            self._batch_cache['batch_events'].append(te)
        else:
            te.commit_to_db(self.session)

    def task_map(self, linedata):
        """
        @type   linedata: dict
        @param  linedata: One line of DB data dict-ified

        Handles a task.map event.  Updates a Task table row
        to include the proper job_id event.
        """
        # Flush previous events to ensure that all the batched
        # Job table entries are written.
        if linedata['xwf.id'] not in self._task_map_flush:
            if self._batch:
                self.hard_flush()
            self._task_map_flush[linedata['xwf.id']] = True

        wf_id = self.wf_uuid_to_id(linedata['xwf.id'])
        job_id = self.get_job_id(wf_id, linedata['job.id'])

        if not job_id:
            self.log.error('Could not determine job_id for task map: %s', linedata)
            return

        try:
            task = self.session.query(Task).filter(Task.wf_id == wf_id).filter(Task.abs_task_id == linedata['task.id']).one()
            task.job_id = job_id
        except orm.exc.MultipleResultsFound as e:
            self.log.error('Multiple task results: cant map task: %s ', linedata)
            return
        except orm.exc.NoResultFound as e:
            self.log.error('No task found: cant map task: %s ', linedata)
            return

        if self._batch:
            # next flush will catch this - no cache
            pass
        else:
            self.session.commit()

    def task_meta(self, linedata):
        """

        :param linedata:  dictionary of netlogger BP data dict-ified
        :return:

        Handles a task metadata insert event.
        """
        task_meta = self.linedataToObject( linedata, TaskMeta() )
        task_meta.wf_id = self.wf_uuid_to_id( task_meta.wf_uuid )
        task_meta.task_id = self.get_task_id( task_meta.wf_id , task_meta.abs_task_id)

        self.log.trace('task_meta: %s', task_meta)

        if self._batch:
            self._batch_cache['batch_events'].append(task_meta)
        else:
            task_meta.commit_to_db(self.session)

    def rc_meta(self, linedata):
        """

        :param linedata:  dictionary of netlogger BP data dict-ified
        :return:

        Handles a rc metadata insert event.
        """
        rc_meta = self.linedataToObject( linedata, RCMeta() )
        lfn = rc_meta.lfn_id
        rc_meta.lfn = lfn
        rc_meta.wf_id = self.wf_uuid_to_id( rc_meta.wf_uuid )
        rc_meta.lfn_id = self.get_lfn_id( rc_meta.wf_id, lfn )

        self.log.trace('rc_meta: %s', rc_meta)

        #we have to do the merge individually to prevent integrity constraint
        #errors that happen if we put them in the batch cache update_events
        rc_meta.merge_to_db(self.session)


    def int_metric(self, linedata):
        """
        @type   linedata: dict
        @param  linedata: One line of BP data dict-ified.

        Handles a integrity metric event
        """
        int_meta = self.linedataToObject(linedata, IntegrityMetrics())
        self.log.trace('int_meta: %s', int_meta)

        int_meta.wf_id = self.wf_uuid_to_id(int_meta.wf_uuid)

        int_meta.job_instance_id = self.get_job_instance_id(int_meta)
        if int_meta.job_instance_id == None:
            self.log.error('Could not determine job_instance_id for int_meta: %s', int_meta)
            return

        if self._batch:
            self._batch_cache['batch_events'].append(int_meta)
        else:
            int_meta.commit_to_db(self.session)

    def tag(self, linedata):
        """
        @type   linedata: dict
        @param  linedata: One line of BP data dict-ified.

        Handles a job_instance tag event
        """
        tag = self.linedataToObject(linedata, Tag())
        self.log.trace('job_inst.tag: %s', tag)

        tag.wf_id = self.wf_uuid_to_id(tag.wf_uuid)

        tag.job_instance_id = self.get_job_instance_id(tag)
        if tag.job_instance_id == None:
            self.log.error('Could not determine job_instance_id for tag: %s', tag)
            return

        if self._batch:
            self._batch_cache['batch_events'].append(tag)
        else:
            tag.commit_to_db(self.session)

    def rc_pfn(self, linedata):
        """

        :param linedata:  dictionary of netlogger BP data dict-ified
        :return:

        Handles a rc pfn insert event that populates the pfn and the site attribute.
        """
        rc_pfn = self.linedataToObject( linedata, RCPFN() )
        lfn = rc_pfn.lfn_id
        rc_pfn.lfn = lfn
        rc_pfn.wf_id = self.wf_uuid_to_id( rc_pfn.wf_uuid )
        rc_pfn.lfn_id = self.get_lfn_id( rc_pfn.wf_id, lfn )

        self.log.trace('rc_pfn: %s', rc_pfn)

        if self._batch:
            self._batch_cache['batch_events'].append(rc_pfn)
        else:
            rc_pfn.commit_to_db(self.session)

    def wf_task_file_map(self , linedata ):
        """
        Handles the event that associates workflow, task and a file lfn .
        Populates to wf_files

        :param linedata:
        :return:
        """

        wf_files = self.linedataToObject( linedata, WorkflowFiles() )
        lfn = wf_files.lfn_id
        wf_files.lfn = lfn
        wf_files.wf_id   = self.wf_uuid_to_id( wf_files.wf_uuid )
        wf_files.task_id = self.get_task_id( wf_files.wf_id, wf_files.abs_task_id )
        wf_files.lfn_id  = self.get_lfn_id( wf_files.wf_id, lfn )

        self.log.trace('wf_files: %s', wf_files)

        if self._batch:
            self._batch_cache['batch_events'].append(wf_files)
        else:
            wf_files.commit_to_db(self.session)

    def subwf_map(self, linedata):
        """
        @type   linedata: dict
        @param  linedata: One line of BP data dict-ified.

        Handles a subworkflow job map event.
        """
        self.log.trace('subwf_map: %s', linedata)

        wf_id = self.wf_uuid_to_id(linedata['xwf.id'])
        subwf_id = self.wf_uuid_to_id(linedata['subwf.id'])
        job_id = self.get_job_id(wf_id, linedata['job.id'])

        try:
            job_inst = self.session.query(JobInstance).filter(JobInstance.job_id == job_id).filter(JobInstance.job_submit_seq == linedata['job_inst.id']).one()
            job_inst.subwf_id = subwf_id
        except orm.exc.MultipleResultsFound as e:
            self.log.error('Multiple job instance results: cant map subwf: %s ', linedata)
            return
        except orm.exc.NoResultFound as e:
            self.log.error('No job instance found: cant map subwf: %s ', linedata)
            return

        if self._batch:
            # next flush will catch this - no cache
            pass
        else:
            self.session.commit()

    def host(self, linedata):
        """
        @type   linedata: dict
        @param  linedata: One line of BP data dict-ified.

        Handles a host insert event.
        """
        host = self.linedataToObject(linedata, Host())

        self.log.trace('host: %s', host)

        if self.hosts_written_cache == None:
            self.hosts_written_cache = {}
            query = self.session.query(Host)
            for row in query.all():
                self.hosts_written_cache[(row.wf_id,row.site,row.hostname,row.ip)] = True

        host.wf_id = self.wf_uuid_to_root_id(host.wf_uuid)

        # handle inserts into the host table
        if (host.wf_id,host.site,host.hostname,host.ip) not in self.hosts_written_cache:
            if self._batch:
                self._batch_cache['batch_events'].append(host)
            else:
                host.commit_to_db(self.session)
            self.hosts_written_cache[(host.wf_id,host.site,host.hostname,host.ip)] = True

        # handle mappings
        if self._batch:
            self._batch_cache['host_map_events'].append(host)
        else:
            self.map_host_to_job_instance(host)

    def static_end(self, linedata):
        """
        @type   linedata: dict
        @param  linedata: One line of BP data dict-ified.

        This forces a flush after all of the static events
        have been processed.
        """
        self.log.trace('static_end: %s', linedata)
        if self._batch:
            self.hard_flush()

    def static_meta_start(self, linedata):
        """
        @type   linedata: dict
        @param  linedata: One line of BP data dict-ified.

        This forces a flush of all the abstract related events,
        so that the task id's can be retrieved for metadata population
        of tasks.
        """
        self.log.trace('static_meta_start: %s', linedata)
        if self._batch:
            self.hard_flush()

    def noop(self, linedata):
        """
        @type   linedata: dict
        @param  linedata: One line of BP data dict-ified.

        A NOOP method for events that are being ignored.
        """
        self.log.trace('noop: %s', linedata)

    ####################################
    # DB helper/lookup/caching functions
    ####################################
    def wf_uuid_to_id(self, wf_uuid):
        """
        @type   wf_uuid: string
        @param  wf_uuid: wf_uuid string from BP logs

        Attempts to retrieve a workflow wf_id PK/FK from cache.  If
        not in cache, retrieve from st_workflow table in DB and cache.
        Cuts down on DB queries during insert processing.
        """
        if wf_uuid not in self.wf_id_cache:
            query = self.session.query(Workflow).filter(Workflow.wf_uuid == wf_uuid)
            try:
                self.wf_id_cache[wf_uuid] = query.one().wf_id
            except orm.exc.MultipleResultsFound as e:
                self.log.error('Multiple wf_id results for wf_uuid %s : %s', wf_uuid, e)
                return None
            except orm.exc.NoResultFound as e:
                self.log.error('No wf_id results for wf_uuid %s : %s', wf_uuid, e)
                return None

        return self.wf_id_cache[wf_uuid]

    def wf_uuid_to_root_id(self, wf_uuid):
        """
        @type   wf_uuid: string
        @param  wf_uuid: wf_uuid string from BP logs

        Attempts to retrieve a root workflow wf_id PK/FK from cache.  If
        not in cache, retrieve from st_workflow table in DB and cache.
        Cuts down on DB queries during insert processing.
        """
        if wf_uuid not in self.root_wf_id_cache:
            query = self.session.query(Workflow).filter(Workflow.wf_uuid == wf_uuid)
            try:
                self.root_wf_id_cache[wf_uuid] = query.one().root_wf_id
            except orm.exc.MultipleResultsFound as e:
                self.log.error('Multiple wf_id results for wf_uuid %s : %s', wf_uuid, e)
                return None
            except orm.exc.NoResultFound as e:
                self.log.error('No wf_id results for wf_uuid %s : %s', wf_uuid, e)
                return None

        return self.root_wf_id_cache[wf_uuid]


    def get_task_id(self, wf_id, task_dax_id):
        """
        @type   wf_id: int
        @param  wf_id: A workflow id from the workflow table.
        @type   task_dax_id: string
        @param  task_dax_id: The ID for the task in the DAX

        Gets and caches task_id for task_meta inserts
        """
        if (wf_id, task_dax_id) not in self.task_id_cache:
            query = self.session.query(Task.task_id).filter(Task.wf_id == wf_id).filter(Task.abs_task_id == task_dax_id)
            try:
                self.task_id_cache[((wf_id, task_dax_id))] = query.one().task_id
            except orm.exc.MultipleResultsFound as e:
                self.log.error('Multiple results found for wf_uuid/task_dax_id: %s/%s', wf_id, task_dax_id)
                return None
            except orm.exc.NoResultFound as e:
                self.log.error('No results found for wf_uuid/task_dax_id: %s/%s', wf_id, task_dax_id)
                return None

        return self.task_id_cache[(wf_id, task_dax_id)]

    def get_lfn_id(self, wf_id, lfn):
        """
        @type   wf_id: int
        @param  wf_id: A workflow id from the workflow table.
        @type   lfn: string
        @param  lfn: The logical filename for the file

        Gets and caches lfn_id for rc_meta, rc_lfn, rc_pfn and wf_files inserts
        """
        if (wf_id, lfn) not in self.lfn_id_cache:
            id =  self.__get_lfn_id_from_database__(wf_id, lfn )

            if id is None:
                # we do an explicit insert to populate the RCLFN table
                file = RCLFN()
                file.lfn = lfn
                # explicit insert
                file.commit_to_db(self.session)
                # retrieve from the database and set the cache
                id = self.__get_lfn_id_from_database__(wf_id, lfn)

                # if ID is still None then definitely an an error
                if id is None:
                    self.log.error('No results found for wf_uuid/lfn: %s/%s', wf_id, lfn)
                    return None

            self.lfn_id_cache[(wf_id, lfn)] = id

        return self.lfn_id_cache[(wf_id, lfn)]

    def __get_lfn_id_from_database__(self, wf_id, lfn):
        """
        @type   wf_id: int
        @param  wf_id: A workflow id from the workflow table.
        @type   lfn: string
        @param  lfn: The logical filename for the file

        Retrieves the LFN explicitly by querying the database.
        """

        query = self.session.query(RCLFN.lfn_id).filter( RCLFN.lfn == lfn)
        lfn_id = None
        try:
            lfn_id = query.one().lfn_id
        except orm.exc.MultipleResultsFound as e:
            self.log.error('Multiple results found for wf_uuid/lfn: %s/%s', wf_id, lfn)
            return None
        except orm.exc.NoResultFound as e:
            return None

        return lfn_id


    def get_job_id(self, wf_id, exec_id):
        """
        @type   wf_id: int
        @param  wf_id: A workflow id from the workflow table.
        @type   exec_id: string
        @param  exec_id: The exec_job_id for a given job.

        Gets and caches job_id for job_instance inserts and static
        table updating.
        """
        if (wf_id, exec_id) not in self.job_id_cache:
            query = self.session.query(Job.job_id).filter(Job.wf_id == wf_id).filter(Job.exec_job_id == exec_id)
            try:
                self.job_id_cache[((wf_id, exec_id))] = query.one().job_id
            except orm.exc.MultipleResultsFound as e:
                self.log.error('Multiple results found for wf_uuid/exec_job_id: %s/%s', wf_id, exec_id)
                return None
            except orm.exc.NoResultFound as e:
                self.log.error('No results found for wf_uuid/exec_job_id: %s/%s', wf_id, exec_id)
                return None

        return self.job_id_cache[((wf_id, exec_id))]


    def get_job_instance_id(self, o, quiet=False):
        """
        @type   o: class instance
        @param  o: Mapper object containing wf_uuid and exec_job_id.

        Attempts to retrieve a job job_instance_id PK/FK from cache.  If not in
        cache, retrieve from st_job table.
        """
        wf_id = self.wf_uuid_to_id(o.wf_uuid)
        cached_job_id = self.get_job_id(wf_id, o.exec_job_id)
        uniqueIdIdx = (cached_job_id, o.job_submit_seq)
        if uniqueIdIdx not in self.job_instance_id_cache:
            query = self.session.query(JobInstance).filter(JobInstance.job_id == cached_job_id).filter(JobInstance.job_submit_seq == o.job_submit_seq)
            try:
                self.job_instance_id_cache[uniqueIdIdx] = query.one().job_instance_id
            except orm.exc.MultipleResultsFound as e:
                if not quiet:
                    self.log.error('Multple job_instance_id results for tuple %s : %s', uniqueIdIdx, e)
                return None
            except orm.exc.NoResultFound as e:
                if not quiet:
                    self.log.error('No job_instance_id results for tuple %s : %s', uniqueIdIdx, e)
                return None

        return self.job_instance_id_cache[uniqueIdIdx]

    def map_host_to_job_instance(self, host):
        """
        @type   host: class instance of stampede_schema.Host
        @param  host: Host object with info from a host event in the log

        A single job may have multiple (redundant) host events.  This
        checks the cache to see if a job had already had its host_id,
        and if not, do the proper update and note it in the cache.
        """
        self.log.trace('map_host_to_job_instance: %s', host)

        wf_id = self.wf_uuid_to_id(host.wf_uuid)
        cached_job_id = self.get_job_id(wf_id, host.exec_job_id)

        if (cached_job_id, host.job_submit_seq) not in self.host_cache:
            if not host.host_id:
                try:
                    host.host_id = self.session.query(Host.host_id).filter(Host.wf_id == host.wf_id).filter(Host.site == host.site).filter(Host.hostname == host.hostname).filter(Host.ip == host.ip).one().host_id
                except orm.exc.MultipleResultsFound as e:
                    self.log.error('Multiple host_id results for host: %s', host)
            job_instance = self.session.query(JobInstance).filter(JobInstance.job_id == cached_job_id).filter(JobInstance.job_submit_seq == host.job_submit_seq).one()
            job_instance.host_id = host.host_id
            job_instance.merge_to_db(self.session, batch=self._batch)
            self.host_cache[(cached_job_id, host.job_submit_seq)] = True

    def purgeCaches(self, wfs):
        """
        @type   wfs: class instance of stampede_schema.Workflowstate
        @param  wfs: Workflow state object from an end event.

        Purges information from the lookup caches after a workflow.end
        event has been recieved.
        """
        self.log.debug('Purging caches for: %s', wfs.wf_uuid)

        for k,v in self.wf_id_cache.items():
            if k == wfs.wf_uuid:
                del self.wf_id_cache[k]

        for k,v in self.root_wf_id_cache.items():
            if k == wfs.wf_uuid:
                del self.root_wf_id_cache[k]

        for k,v in self.job_instance_id_cache.items():
            if k[0] == wfs.wf_id:
                del self.job_instance_id_cache[k]

        for k,v in self.host_cache.items():
            if k[0] == wfs.wf_uuid:
                del self.host_cache[k]

        for k,v in self.task_id_cache.items():
            if k[0] == wfs.wf_id:
                del self.task_id_cache[k]

        for k,v in self.lfn_id_cache.items():
            if k[0] == wfs.wf_id:
                del self.lfn_id_cache[k]

        for k,v in self.job_id_cache.items():
            if k[0] == wfs.wf_id:
                del self.job_id_cache[k]

        if wfs.wf_uuid in self._task_map_flush:
            del self._task_map_flush[wfs.wf_uuid]


    ################
    # Cleanup, etc
    ################

    def finish(self):
        if self._batch:
            self.log.info('Executing final flush')
            self.hard_flush()
        self.disconnect()
        if self._perf:
            run_time = time.time() - self._start_time
            self.log.info("Loader performance: insert_time=%s, insert_num=%s, "
                          "total_time=%s, run_time_delta=%s, mean_time=%s",
                          self._insert_time, self._insert_num, run_time,
                          run_time - self._insert_time,
                          self._insert_time / self._insert_num)

