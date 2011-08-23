"""
SQLAlchemy interface to the Stampede backend.

Named sql_alchemy to avoid import errors with the library proper.
"""

__rcsid__ = "$Id: sql_alchemy.py 27235 2011-02-24 16:10:26Z mgoode $"
__author__ = "Monte Goode MMGoode@lbl.gov"

import calendar
import datetime
import time

from netlogger.analysis.schema.stampede_schema import initializeToPegasusDB, func, orm, \
    Workflow as WorkflowTable, Workflowstate as WorkflowstateTable, Job as JobTable, \
    Jobstate as JobstateTable, Host as HostTable, Task as TaskTable, Edge as EdgeTable
from netlogger.analysis.modules._base import SQLAlchemyInit
from netlogger.analysis.workflow._base import Workflow as BaseWorkflow, \
    Job as BaseJob, Host as BaseHost, Task as BaseTask, Jobstate as BaseJobstate, \
    Discovery as BaseDiscovery, Workflowstate as BaseWorkflowstate
    
from sqlalchemy import or_
    
debug = False

def as_string(s):
    if s is None:
        return None
    else:
        return s.encode('ascii')
        
def as_integer(i):
    if i is None:
        return None
    else:
        return int(i)
        
def as_float(f):
    if f is None:
        return None
    else:
        return float(f)
    
class Workflow(BaseWorkflow, SQLAlchemyInit):
    """
    Top level workflow class that exposes information about
    a specific workflow and the associated jobs/etc.
    
    Usage::
    
     w = Workflow()
     w.initialize('unique_wf_uuid')
     print w.timestamp, w.dax_label
    
    etc
    """
    def __init__(self, connString=None):
        BaseWorkflow.__init__(self)
        if connString is None:
            raise ValueError("connString is required")
        SQLAlchemyInit.__init__(self, connString, initializeToPegasusDB)
        
        # These attrs come straight from the workflow table
        self._wf_id = None
        self._wf_uuid = None
        self._dax_label = None
        self._timestamp = None
        self._submit_hostname = None
        self._submit_dir = None
        self._planner_arguments = None
        self._user = None
        self._grid_dn = None
        self._planner_version = None
        self._parent_workflow_id = None
        self._parent_wf_uuid = None
        
        # State information
        self._startstate = []
        self._endstate = []
        
        # Job information
        self._jobs = []
        self._jobtypes_executed = {}
        
        # Sub-workflow information
        self._sub_wf_uuid = None
        
        # A debug flag that can be manually switched to turn off
        # generation of job edges.  Just used in development
        # to quiet down the amount of output generated.
        # ie:
        # w = Workflow()
        # w._edges = False
        # w.initialize(wf_uuid)
        self._edges = True
    
    def initialize(self, wf_id):
        """
        This method is the initialization method that accepts
        the unique wf_uuid and triggers the subclass specific
        queries and calculations that pulls the workflow 
        information from the back end.
        
        The wf_id is represented in the .bp logs as "wf.id".
        
        @type   wf_uuid: string
        @param  wf_uuid: the unique wf_uuid as defined by Pegasus
        """
        self._wf_uuid = wf_id
        query = self.session.query(WorkflowTable).filter(WorkflowTable.wf_uuid == self._wf_uuid)
        try:
            wf = query.one()
            self._wf_id = wf.wf_id
            self._dax_label = as_string(wf.dax_label)
            self._timestamp = wf.timestamp
            self._submit_hostname = as_string(wf.submit_hostname)
            self._submit_dir = as_string(wf.submit_dir)
            self._planner_arguments = as_string(wf.planner_arguments)
            self._user = as_string(wf.user)
            self._grid_dn = as_string(wf.grid_dn)
            self._planner_version = as_string(wf.planner_version)
            self._parent_workflow_id = wf.parent_workflow_id
        except orm.exc.MultipleResultsFound, e:
            self.log.error('initialize', 
            msg='Multiple wf_id results for wf_uuid %s : %s' % (wf_uuid, e))
            return
        pass
        
    def _check_states(self):
        if not self._startstate:
            state_cache = []
            query = self.session.query(WorkflowstateTable.state, WorkflowstateTable.timestamp).filter(WorkflowstateTable.wf_id == self._wf_id).order_by(WorkflowstateTable.timestamp)
            for row in query.all():
                wfs = Workflowstate()
                wfs.initialize(row.state, row.timestamp)
                # seed with the first start event
                if wfs.state == 'start':
                    self._startstate.append(wfs)
                    state_cache.append(wfs)
                    continue
                if wfs.state.startswith('start'):
                    if state_cache[-1].state.startswith('start'):
                        self.log.warn('_check_states', 
                            msg='Workflow state missing end event - padding list.')
                        self._endstate.append(None)
                    self._startstate.append(wfs)
                    state_cache.append(wfs)
                elif wfs.state.startswith('end'):
                    if state_cache[-1].state.startswith('end'):
                        self.log.warn('_check_states', 
                            msg='Workflow state missing start event - padding list.')
                        self._startstate.append(None)
                    self._endstate.append(wfs)
                    state_cache.append(wfs)
                else:
                    self.log.error('_check_states', msg='Bad state attribute:' % wfs.state)
        pass

    @property
    def wf_uuid(self):
        """
        Return the wf_uuid for this workflow.
        
        @rtype:     string
        @return:    The wf_uuid of the current workflow
        """
        return self._wf_uuid

    @property
    def dax_label(self):
        """
        Return dax_label from storage backend.
        
        @rtype:     string
        @return:    The dax_label of the current workflow.
        """
        return self._dax_label

    @property
    def timestamp(self):
        """
        Return timestamp from storage backend.
        
        @rtype:     python datetime obj (utc)
        @return:    The workflow timestamp
        """
        return datetime.datetime.utcfromtimestamp(self._timestamp)

    @property
    def submit_hostname(self):
        """
        Return submit_hostname from storage backend.
        
        @rtype:     string
        @return:    The workflow submit host
        """
        return self._submit_hostname

    @property
    def submit_dir(self):
        """
        Return submid_dir from storage backend.
        
        @rtype:     string
        @return:    The workflow submit directory
        """
        return self._submit_dir

    @property
    def planner_arguments(self):
        """
        Return planner_arguments from storage backend.
        
        @rtype:     string
        @return:    The workflow planner arguments
        """
        return self._planner_arguments

    @property
    def user(self):
        """
        Return user from storage backend.
        
        @rtype:     string
        @return:    The workflow user
        """
        return self._user

    @property
    def grid_dn(self):
        """
        Return grid_dn from storage backend.
        
        @rtype:     string
        @return:    The grid DN of the workflow
        """
        return self._grid_dn

    @property
    def planner_version(self):
        """
        Return planner_version from storage backend.
        
        @rtype:     string
        @return:    The planner version of the workflow
        """
        return self._planner_version

    @property
    def parent_wf_uuid(self):
        """
        Takes the parent_workflow_id column from the current 
        Workflow which is a reference to a Primary Key
        and return the string wf_uuid of the workflow it 
        references, otherwise, return None.
        
        @rtype:     string
        @return:    The parent wf_uuid if it exists
        """
        if self._parent_workflow_id is not None:
            if self._parent_wf_uuid is None:
                query = self.session.query(WorkflowTable.wf_uuid).filter(WorkflowTable.wf_id == self._parent_workflow_id)
                try:
                    self._parent_wf_uuid = query.one().wf_uuid
                except orm.exc.MultipleResultsFound, e:
                    self.log.error('parent_wf_uuid', 
                    msg='Multiple wf_uuid results for parent_workflow_id %s : %s' % (self._parent_workflow_id, e))
                    return
        return self._parent_wf_uuid
        
    @property
    def sub_wf_uuids(self):
        """
        Returns a list of the wf_uuids of any sub-workflows associated
        with the current workflow object.  Returned in the order in 
        which they are entered in the workflow table.  If no sub-workflows
        are found, return an empty list.
        
        @rtype:     List of strings
        @return:    The wf_uuids of any sub-workflows.
        """
        if self._sub_wf_uuid == None:
            self._sub_wf_uuid = []
            query = self.session.query(WorkflowTable.wf_uuid).filter(WorkflowTable.parent_workflow_id == self._wf_id).order_by(WorkflowTable.wf_id)
            for row in query.all():
                self._sub_wf_uuid.append(row.wf_uuid)
        return self._sub_wf_uuid
        
    @property
    def start_events(self):
        """
        Return a list of Workflowstate object instances representing
        the re/start events.  The list should ordered by timestamp.

        In the event that there are no logged workflow states an empty
        list should be returned.
        
        In the case that there is a dropped event (ie: no matching end
        event to a start event or vice versa), the missing event will
        be padded as a None.

        @rtype:     List of Workflowstate object instances (or None)
        @return:    Returns a list with workflow start events.
        """
        self._check_states()
        return self._startstate
        
    @property
    def end_events(self):
        """
        Return a list of Workflowstate object instances representing
        the end events.  The list should ordered by timestamp.

        In the event that there are no logged workflow states an empty
        list should be returned.
        
        In the case that there is a dropped event (ie: no matching end
        event to a start event or vice versa), the missing event will
        be padded as a None.

        @rtype:     List of Workflowstate object instances
        @return:    Returns a list with workflow end events.
        """
        self._check_states()
        return self._endstate

    @property
    def is_running(self):
        """
        Derived boolean flag indicating if the workflow
        is currently running.  Derived in a backend-appropriate
        way.
        
        @rtype:     boolean
        @return:    Indicates if the workflow is running.
        """
        self._check_states()
        if ((len(self._startstate) - len(self._endstate)) > 0):
            return True
        else:
            return False
        pass
        
    @property
    def is_restarted(self):
        """
        Derived boolean flag indicating if this workflow has
        been retstarted.  Derived in a backend-appropriate
        way.
        
        @rtype:     boolean
        @return:    Indicates if the workflow has been restarted.
        """
        self._check_states()
        if len(self._startstate) > 1:
            return True
        else:
            return False
            
    @property
    def restart_count(self):
        """
        Returns an integer reflecting restart count.  Derived in 
        a backend-appropriate way.
        
        @rtype:     integer
        @return:    Number of workflow restarts.
        """
        self._check_states()
        return len(self._startstate) - 1
        
    @property
    def total_time(self):
        """
        Returns the total runtime of the workflow.  This is defined
        as the delta between either the first start event and last end
        event, or if the job is still running, between the first
        start event and current epoch UTC time.
        
        @rtype:     python datetime.timedelta object or None
        @return:    The total time of the workflow.
        """
        self._check_states()
        if len(self._startstate) == 0:
            return None
        if self._endstate[-1] == None or self._startstate[0] == None:
            return None
        if len(self._startstate) == len(self._endstate):
            return self._endstate[-1].timestamp - self._startstate[0].timestamp
        else:
            return datetime.datetime.utcfromtimestamp(time.time()) - self._startstate[0].timestamp
        
    @property
    def jobs(self):
        """
        Returns a list of the jobs associated with this workflow
        object.  This property is a prime candidate for lazy eval
        as there is no need to query the backend for this information
        if the user is only looking at superficial information about
        the workflow - ie: if it is running, how long it took, etc.
        
        @rtype:     list of Job objects
        @return:    List of job objects associated with current wf
        """
        if self._jobs:
            query = self.session.query(func.max(JobTable.job_id))
            row = query.first()
            if self._jobs[-1]._job_id != row[0]:
                self._jobs = []
        
        if not self._jobs:
            query = self.session.query(JobTable.job_id).filter(JobTable.wf_id == self._wf_id).order_by(JobTable.job_submit_seq)
            for row in query.all():
                j = Job(self.session)
                j._sql_initialize(row[0], find_edges=self._edges)
                self._jobs.append(j)
                if debug:
                    break
        return self._jobs
            
    @property
    def total_jobs_executed(self):
        """
        Return the number of jobs that were executed as an
        integer value.
        
        @rtype:     integer
        @return:    Number of jobs executed
        """
        return len(self.jobs)

    @property
    def successful_jobs(self):
        """
        Return the number of jobs that executed successfully
        as an integer value.
        
        @rtype:     integer
        @return:    Number of sucessfully executed jobs
        """
        # XXX: note - this is not fully accurate at the moment.
        # Mostly here to improve later and act as example code.
        # This can only give a count at the moment but the value
        # is subject to change.  Logic in the job objects will 
        # need to be tightened up later when it is possible.
        success = 0
        for j in self.jobs:
            if j.is_success:
                success += 1
        return success

    @property
    def failed_jobs(self):
        """
        Return the number of jobs that failed as an integer
        value.
        
        @rtype:     integer
        @return:    Number of failed jobs
        """
        # XXX: note - this is not fully accurate at the moment.
        # Mostly here to improve later and act as example code.
        # This can only give a count at the moment but the value
        # is subject to change.  Logic in the job objects will 
        # need to be tightened up later when it is possible.
        failures = 0
        for j in self.jobs:
            if j.is_failure:
                failures += 1
        return failures

    @property
    def restarted_jobs(self):
        """
        Return the number of jobs that were restarted.
        
        @rtype:     integer
        @return:    Number of restarted jobs
        """
        restarts = 0
        for j in self.jobs:
            if j.is_restart:
                restarts += 1
        return restarts
        
    @property
    def submitted_jobs(self):
        """
        Return the number of jobs that were submitted.

        @rtype:     integer
        @return:    Number of submitted jobs
        """
        submits = 0
        for j in self.jobs:
            if j.submit_time:
                submits += 1
        return submits
        
    @property
    def jobtypes_executed(self):
        """
        Returns a dictionary of the various jobtypes that
        are executed in the current workflow and a count of
        how many of each type.
        
        Example: {'create dir': 1, 'compute': 105}
        
        @rtype:     dict - string keys, integer values
        @return:    A dictionary of a count of the jobtypes
                    that were executed in the current workflow.
        """
        for j in self.jobs:
            if not self._jobtypes_executed.has_key(j.jobtype):
                self._jobtypes_executed[j.jobtype] = 1
            else:
                self._jobtypes_executed[j.jobtype] += 1
                
        return self._jobtypes_executed
        
class Workflowstate(BaseWorkflowstate):
    """
    Class to expose information about a specific
    workflow event.  This is a simple class to expose state
    and timestamp information as class attributes.

    Usage::

     ws = Workflowstate()
     ws.initialize(state, timestamp)
     print ws.state

    etc
    """
    _indent = 2
    def __init__(self):
        BaseWorkflowstate.__init__(self)
        self._state = None
        self._timestamp = None

    def initialize(self, state, timestamp):
        """
        This method is the initialization method that accepts
        the state and timestamp of a given workflow state
        event.

        @type   state: string
        @param  state: the jobstate entry as defined by Pegasus.
        @type   timestamp: float
        @param  timestamp: the epoch timestamp as reported by Pegasus.
        """
        self._state = as_string(state)
        self._timestamp = timestamp

    @property
    def state(self):
        """
        Return the current workflowstate state.  Might be
        none if there is no state information logged yet.

        @rtype:     string or None
        @return:    Return current job state
        """
        return self._state

    @property
    def timestamp(self):
        """
        Return the timestamp of the current workflow state.  Might be
        none if there is no state information logged yet.

        @rtype:     python datetime obj (utc) or None
        @return:    Return timestamp of current job state
        """
        if self._timestamp:
            return datetime.datetime.utcfromtimestamp(self._timestamp)
        else:
            return None


class Job(BaseJob):
    """
    Class to retrieve and expose information about a 
    specific job.  This class is intended to be instantiated
    inside a Workflow() object and not as a stand-alone
    instance.
    
    Usage::
    
     j = Job()
     j._sql_initialize(job_id) (PK from the DB)
     print j.name
    
    etc
    """
    def __init__(self, db_session):
        BaseJob.__init__(self)
        self.session = db_session
        
        self._job_id = None
        self._wf_id = None
        self._job_submit_seq = None
        self._name = None
        self._host_id = None
        self._condor_id = None
        self._jobtype = None
        self._clustered = False
        self._site_name = None
        self._remote_user = None
        self._remote_working_dir = None
        self._cluster_start_time = None
        self._cluster_duration = None
        
        # nested objects
        self._host = None
        self._tasks = []
        
        # derived state values
        self._is_restart = None
        self._is_success = None
        self._is_failure = None
        
        # cached values
        self._submit_time = None
        self._current_js_ss = None
        self._jobstates = []
        self._parent_edge_ids = []
        self._child_edge_ids = []
        self._parent_edges = []
        self._child_edges = []
    
    def _sql_initialize(self, job_id, find_edges=True):
        """
        This private method is the initialization method that accepts
        the sql DB job_id primary key from the job table to use
        for initialization of the job object.
        
        @type   job_id: integer
        @param  job_id: The job_id primary key from the sql Jobs table.
        """
        self._job_id = job_id
        
        query = self.session.query(JobTable).filter(JobTable.job_id == self._job_id)
        
        try:
            j = query.one()
            self._job_id = j.job_id
            self._wf_id = j.wf_id
            self._job_submit_seq = as_integer(j.job_submit_seq)
            self._name = as_string(j.name)
            self._host_id = j.host_id
            self._condor_id = as_string(j.condor_id)
            self._jobtype = as_string(j.jobtype)
            self._clustered = j.clustered
            self._site_name = as_string(j.site_name)
            self._remote_user = as_string(j.remote_user)
            self._remote_working_dir = as_string(j.remote_working_dir)
            self._cluster_start_time = j.cluster_start_time
            self._cluster_duration = as_float(j.cluster_duration)
        except orm.exc.MultipleResultsFound, e:
            # this probably will NEVER happen and if it does
            # then there is something screwy with the jobs table
            self.log.error('_sql_initialize', 
            msg='Multiple job results for job_id: %s : %s' % (self._job_id, e))
            return
            
        if find_edges:
            self._find_edge_id()
                
    def _find_edge_id(self):
        """
        Private method to get the job_ids of the parent and child edge
        jobs.
        """
        query = self.session.query(EdgeTable.parent_id).filter(EdgeTable.child_id == self._job_id)
        for row in query.all():
            self._parent_edge_ids.append(row[0])
        query = self.session.query(EdgeTable.child_id).filter(EdgeTable.parent_id == self._job_id)
        for row in query.all():
            self._child_edge_ids.append(row[0])
        pass
    
    def _get_current_js_ss(self):
        """
        This private method hits the jobstate table to get the most
        recent jobstate_submit_seq for a given job.  This is used by
        both the properties that return jobstate information.
        """
        max_id = self.session.query(func.max(JobstateTable.jobstate_submit_seq).label('max_id')).filter(JobstateTable.job_id == self._job_id)
        row = max_id.first()
        if row:
            self._current_js_ss = row[0]

    @property
    def job_submit_seq(self):
        """
        Return job_submit_seq of current job (an input arg).
        
        @rtype:     integer
        @return:    Return job_submit_seq of current job
        """
        return self._job_submit_seq

    @property
    def name(self):
        """
        Return the job name from the storage backend.
        
        @rtype:     string
        @return:    Return job name
        """
        return self._name

    @property
    def host(self):
        """
        Return job host information from storage backend.
        
        @rtype:     Host object instance
        @return:    Return a host object with host info for
                    current job.
        """
        if self._host is None:
            self._host = Host(self.session)
            if self._host_id is not None:
                self._host._sql_initialize(self._host_id)
            
        return self._host
            

    @property
    def condor_id(self):
        """
        Return the condor_id from the storage backend.
        
        @rtype:     string (looks like a float however)
        @return:    Return job condor_id
        """
        return self._condor_id

    @property
    def jobtype(self):
        """
        Return jobtype from the storage backend.
        
        @rtype:     string
        @return:    Return jobtype
        """
        return self._jobtype

    @property
    def clustered(self):
        """
        Return the clustered boolean flag from the storage
        backend.  This may need to be derived depending on 
        how the backend implementation does/not store this 
        value.
        
        @rtype:     boolean
        @return:    Return True or False depending on if the
                    job is clustered or not.
        """
        return self._clustered

    @property
    def site_name(self):
        """
        Return the site name from the storage backend.
        
        @rtype:     string
        @return:    Return site_name for current job
        """
        return self._site_name

    @property
    def remote_user(self):
        """
        Return the remote use of the current job from
        the storage backend.
        
        @rtype:     string
        @return:    Return remote_user for current job.
        """
        return self._remote_user
    
    @property
    def remote_working_dir(self):
        """
        Return the remote working directory of the current
        job from the storage backend.
        
        @rtype:     string
        @return:
        """
        return self._remote_working_dir

    @property
    def cluster_start_time(self):
        """
        Return the job cluster start time as a python
        datetime object (utc) if it exists or None
        if it does not.  Not all jobs will have this
        value.
        
        @rtype:     python datetime obj (utc) or None
        @return:    Return job cluster start time.
        """
        if self._cluster_start_time is not None:
            return datetime.datetime.utcfromtimestamp(self._cluster_start_time)
        else:
            return None

    @property
    def cluster_duration(self):
        """
        Return the job cluster duration from the 
        storage backend as a float or None if this value
        is not assocaited with the current job.  Not all j
        will have this value.
        
        @rtype:     float (from db)
        @return:    Return cluster duration.
        """
        return self._cluster_duration

    @property
    def tasks(self):
        """
        Returns a list of the tasks associated with this job
        object.  This property is a prime candidate for lazy eval
        as there is no need to query the backend for this information
        if the user is only looking at superficial information about
        the job - ie: its current state, name, etc.
        
        @rtype:     list of Task objects
        @return:    List of task objects associated with current job
        """
        if not self._tasks:
            query = self.session.query(TaskTable.task_id).filter(TaskTable.job_id == self._job_id).order_by(TaskTable.task_id)
            for row in query.all():
                t = Task(self.session)
                t._sql_initialize(row[0])
                self._tasks.append(t)
                if debug:
                    break
        return self._tasks

    @property
    def is_restart(self):
        """
        Return a boolean flag indicating whether or not this
        curent job is a "restart".  This value will be derived
        from backend information as appropriate.
        
        @rtype:     boolean
        @return:    Return True or False if the job is a restart
                    or not.
        """
        if self._is_restart is None:
            query = self.session.query(JobTable.job_id).filter(JobTable.wf_id == self._wf_id).filter(JobTable.name == self._name).filter(JobTable.job_submit_seq < self._job_submit_seq)
            if query.count() > 1:
                self._is_restart = True
            else:
                self._is_restart = False
            
        return self._is_restart
    
            
    @property
    def is_success(self):
        """
        Return a boolean flag indicating whether or not this
        curent job was successful.  This value will be derived
        from backend information as appropriate.

        @rtype:     boolean
        @return:    Return True or False if the job is a restart
        """
        # XXX: note - this is not fully accurate at the moment.
        # Mostly here to improve later and act as example code.
        if self.is_failure:
            return False
        scount = self.session.query(JobstateTable.state).filter(JobstateTable.state.like('%_SUCCESS')).filter(JobstateTable.job_id == self._job_id).count()
        if scount > 0:
            return True
        return False
        
    @property
    def is_failure(self):
        """
        Return a boolean flag indicating whether or not this
        curent job has failed.  This value will be derived
        from backend information as appropriate.

        @rtype:     boolean
        @return:    Return True or False if the job is a restart
        """
        # XXX: note - this is not fully accurate at the moment.
        # Mostly here to improve later and act as example code.
        fcount = self.session.query(JobstateTable.state).filter(or_(JobstateTable.state.like('%_FAILURE'),JobstateTable.state.like('%_FAILED'))).filter(JobstateTable.job_id == self._job_id).count()
        if fcount > 0:
            return True
        return False

    @property
    def current_state(self):
        """
        Return the current state of this job.  This property 
        pretty much requires lazy evaluation every access rather
        than attribute caching.  A single job moves through
        multiple jobstates and this property should return
        the current state of the running job when accessed.
        
        In the event that there is not yet a jobstate logged for
        this job, the Jobstate instance will have its properties
        "state" and "timestamp" set to None.
        
        @rtype:     Jobstate object instance
        @return:    Returns the current state and timestamp
        """
        self._get_current_js_ss()
        js = Jobstate(self.session)
        if self._current_js_ss:
            js._sql_initialize(self._job_id, self._current_js_ss)
        return js
        
    @property
    def all_jobstates(self):
        """
        Return a list of Jobstate object instances representing
        the states that the job has moved through.  The list should
        ordered by the order of the different jobstate submissions.

        In the event that there are no logged jobstates an empty
        list should be returned.

        This property may do light weight attribute caching, but
        the current jobstate should still be lazily evaluated 
        and the list updated if need be.

        @rtype:     List of Jobstate object instances
        @return:    Returns a list with all the jobstates this job 
                    has moved through.
        """
        self._get_current_js_ss()
        if self._current_js_ss == None:
            # no current submit_seq so do nothing
            pass
        elif not self._jobstates or self._jobstates[-1]._jss != self._current_js_ss:
            # either initialize or refresh if new states have been logged.
            self._jobstates = []
            query = self.session.query(JobstateTable.jobstate_submit_seq).filter(JobstateTable.job_id == self._job_id).order_by(JobstateTable.jobstate_submit_seq)
            results = query.all()
            for row in results:
                js = Jobstate(self.session)
                js._sql_initialize(self._job_id, row.jobstate_submit_seq)
                self._jobstates.append(js)
                if debug:
                    break
        
        return self._jobstates
            
    @property
    def submit_time(self):
        """
        Return the timestamp of when this job was submitted.

        @rtype:     python datetime obj (utc) or None
        @return:    Return the submit time of this job
        """
        if self._submit_time is None:
            query = self.session.query(JobstateTable.timestamp).filter(JobstateTable.job_id == self._job_id).filter(JobstateTable.state == 'SUBMIT')
            state = query.first()
            if state is None:
                return None # a rare state of unlikely timing
            else:
                self._submit_time = state.timestamp
        
        return datetime.datetime.utcfromtimestamp(self._submit_time)
        
    @property
    def elapsed_time(self):
        """
        Return the elapsed time of this job.  Calculated as
        the delta between the submit time and the current/last
        jobstate timestamp.

        @rtype:     python datetime.timedelta object or None
        @return:    Return the elapsed time of this job
        """
        if self.submit_time is not None:
            return self.current_state.timestamp - self.submit_time
        else:
            return None
            
    @property
    def edge_parents(self):
        """
        Return a list of job objects for the parent job edges for
        this current job object.  The job objects returned by this
        property will NOT contain additional edge information (ie: this
        method will return an empty list) to avoid a recursive situation.
        
        @rtype:     list containing Job objects
        @return:    Return the parent edge Job objects.
        """
        if self._parent_edge_ids and not self._parent_edges:
            for p_id in self._parent_edge_ids:
                j = Job(self.session)
                j._sql_initialize(p_id, find_edges=False)
                self._parent_edges.append(j)
                if debug:
                    break
        return self._parent_edges
            
    @property
    def edge_children(self):
        """
        Return a list of job objects for the child job edges for
        this current job object.  The job objects returned by this
        property will NOT contain additional edge information (ie: this
        method will return an empty list) to avoid a recursive situation.

        @rtype:     list containing Job objects
        @return:    Return the child edge Job objects.
        """
        if self._child_edge_ids and not self._child_edges:
            for c_id in self._child_edge_ids:
                j = Job(self.session)
                j._sql_initialize(c_id, find_edges=False)
                self._child_edges.append(j)
                if debug:
                    break
        return self._child_edges
        
class Jobstate(BaseJobstate):
    """
    A small class that returns jobstate information.  Intended
    to be instantiated by a call to job.current_state.  Is
    not cached so multiple calls will return the latest information.

    Usage::
    
     js = Jobstate()
     js.initialize('unique_wf_id', 3)
     print js.state, js.timestamp
     
    etc.
    """
    _indent = 3
    def __init__(self, db_session):
        BaseJobstate.__init__(self)
        self.session = db_session
        
        self._job_id = None
        self._jss = None
        self._state = None
        self._timestamp = None
            
    def _sql_initialize(self, job_id, jss):
        """
        This is the private initialization method that accepts
        the job_id primary key from the relational implementation.

        @type   host_id: integer
        @param  host_id: The host_id public key from the sql
                task table.
        """
        self._job_id = job_id
        self._jss = jss
        query = self.session.query(JobstateTable).filter(JobstateTable.job_id == self._job_id).filter(JobstateTable.jobstate_submit_seq == self._jss)
        row = query.first()
        if row:
            self._state = as_string(row.state)
            self._timestamp = row.timestamp

    @property
    def state(self):
        """
        Return the current jobstate state.  Might be
        none if there is no state information logged yet.

        @rtype:     string or None
        @return:    Return current job state
        """
        return self._state

    @property
    def timestamp(self):
        """
        Return the timestampe of the current job state.  Might be
        none if there is no state information logged yet.

        @rtype:     python datetime obj (utc) or None
        @return:    Return timestamp of current job state
        """
        if self._timestamp:
            return datetime.datetime.utcfromtimestamp(self._timestamp)
        else:
            return None



class Host(BaseHost):
    """
    A straightforward class that contains host information
    about a job.  This is intended to be instantiated inside 
    a Job() object and not as a standalone instance.
    
    Usage::
    
     h = Host()
     h._sql_initialize(task_id) (PK from the db)
     print h.site_name, h.hostname
    
    etc.
    """
    def __init__(self, db_session):
        BaseHost.__init__(self)
        self.session = db_session
        
        self._host_id = None
        self._site_name = None
        self._hostname = None
        self._ip_address = None
        self._uname = None
        self._total_ram = None
    
    def _sql_initialize(self, host_id):
        """
        This is the private initialization method that accepts
        the host_id primary key from the relational implementation.
        
        @type   host_id: integer
        @param  host_id: The host_id public key from the sql
                task table.
        """

        self._host_id = host_id
        
        query = self.session.query(HostTable).filter(HostTable.host_id == self._host_id)
        
        try:
            h = query.one()
            self._site_name = as_string(h.site_name)
            self._hostname = as_string(h.hostname)
            self._ip_address = as_string(h.ip_address)
            self._uname = as_string(h.uname)
            self._total_ram = as_integer(h.total_ram)
        except orm.exc.MultipleResultsFound, e:
            self.log.error('_sql_initialize', 
            msg='Multiple host results for host_id %s : %s' % (self._host_id, e))
            return
        
        pass

    @property
    def site_name(self):
        """
        Return the site name associated with this host.
        Might be None if a host has not been associated
        with a particular job at time of calling.
        
        @rtype:     string or None
        @return:    Return host site name or None
        """
        return self._site_name

    @property
    def hostname(self):
        """
        Return the host name associated with this host.
        Might be None if a host has not been associated
        with a particular job at time of calling.
        
        @rtype:     string or None
        @return:    Return hostname or None
        """
        return self._hostname

    @property
    def ip_address(self):
        """
        Return the ip address associated with this host.
        Might be None if a host has not been associated
        with a particular job at time of calling.
        
        @rtype:     string or None
        @return:    Return host ip address or None
        """
        return self._ip_address

    @property
    def uname(self):
        """
        Return the uname information of this host machine.
        Might be None if a host has not been associated
        with a particular job at time of calling.
        
        @rtype:     string or None
        @return:    Return host uname or None.
        """
        return self._uname
    
    @property
    def total_ram(self):
        """
        Return the total ram of this host machine.
        Might be None if a host has not been associated
        with a particular job at time of calling.
        
        @rtype:     integer or None
        @return:    Return host RAM or None
        """
        return self._total_ram


class Task(BaseTask):
    """
    Class to expose information about a task associated
    with a particular job.  This is intended to be
    instantiated inside of a Job() object and not as a 
    stand alone instance.
    
    Usage::
    
     t = Task()
     t._sql_initialize(task_id) (PK from the db)
     print t.start_time, t.duration
    
    etc
    """
    def __init__(self, db_session):
        BaseTask.__init__(self)
        self.session = db_session
        
        self._task_id = None
        self._job_id = None
        self._task_submit_seq = None
        self._start_time = None
        self._duration = None
        self._exitcode = None
        self._transformation = None
        self._executable = None
        self._arguments = None
    
    def _sql_initialize(self, task_id):
        """
        This is the private initialization method that accepts
        the unique task_id public key from the relational 
        implementation.
        
        @type   task_id: integer
        @param  task_id: The task_id PK from the relational
                implemntation.
        """
        self._task_id = task_id
        
        query = self.session.query(TaskTable).filter(TaskTable.task_id == self._task_id)
        
        try:
            tsk = query.one()
            self._job_id = tsk.job_id
            self._task_submit_seq = tsk.task_submit_seq
            self._start_time = tsk.start_time
            self._duration = as_float(tsk.duration)
            self._exitcode = as_integer(tsk.exitcode)
            self._transformation = as_string(tsk.transformation)
            self._executable = as_string(tsk.executable)
            self._arguments = as_string(tsk.arguments)
        except orm.exc.MultipleResultsFound, e:
            self.log.error('_sql_initialize', 
            msg='Multiple task results for task_id %s : %s' % (self._task_id, e))
            return
            
    @property
    def task_submit_seq(self):
        """
        Return the task submit sequence number from the
        storage backend as an integer.

        @rtype:     int
        @return:    submit sequence number
        """
        return self._task_submit_seq

    @property
    def start_time(self):
        """
        Return start time of this task from the storage
        backend as a python datetime object (utc).
        
        @rtype:     python datetime obj (utc)
        @return:    Return task start time
        """
        if self._start_time is not None:
            return datetime.datetime.utcfromtimestamp(self._start_time)
        else:
            return None

    @property
    def duration(self):
        """
        Return duration of this task from the storage
        backend as a float.
        
        @rtype:     float (from db)
        @return:    Return the duration of this task
        """
        return self._duration

    @property
    def exitcode(self):
        """
        Return the exitcode of this task from the storage
        backend as an integer.
        
        @rtype:     integer
        @return:    Return the task exitcode
        """
        return self._exitcode

    @property
    def transformation(self):
        """
        Return the transformation type of this task from
        the storage backend.
        
        @rtype:     string
        @return:    Return task transformation
        """
        return self._transformation

    @property
    def executable(self):
        """
        Return the executable invoked by this task from
        the storage backend.
        
        @rtype:     string
        @return:    Return the task executable
        """
        return self._executable

    @property
    def arguments(self):
        """
        Return the task args from the storage backend.
        
        @rtype:     string
        @return:    Return the task arguments
        """
        return self._arguments

# Discovery class
        
class Discovery(BaseDiscovery, SQLAlchemyInit):
    """
    Class to facilitate pulling the wf_uuid strings
    from the back end to feed to the Workflow() objects.
    
    Usage::
    
     db_conn = 'sqlite:///pegasusMontage.db'
     d = Discovery(db_conn)
     for wf_uuid in d.fetch_all():
         w = Workflow(db_conn)
         w.initialize(wf_uuid)
    
    etc
    """
    def __init__(self, connString):
        """
        Initialization method.  The manditory argument connectionInfo
        will be defined as appropriate in a subclass (string, dict, etc),
        and connect to the appropriate back end.
        """
        BaseDiscovery.__init__(self)
        SQLAlchemyInit.__init__(self, connString, initializeToPegasusDB)
        pass

    def fetch_all(self):
        """
        Void method that will return a list of workflow uuids
        from the back end.

        @rtype:     list
        @return:    Returns a list of wf_uuid strings.
        """
        
        query = self.session.query(WorkflowTable.wf_uuid).order_by(WorkflowTable.wf_id)
        ids = []
        for row in query.all():
            ids.append(as_string(row.wf_uuid))
        return ids

    def time_threshold(self, startTime=None, endTime=None):
        """
        Method to return a list of wf_uuid strings that
        were submitted within a certain timeframe.  The startTime
        arg should represent the time floor and endTime the time
        ceiling.  If both args are supplied return workflows
        that were exectued between the two.  If only one
        arg is supplied, use that one as the appropriate
        floor or ceiling.
        
        This is based on the workflowstate start event occurring
        after the startTime and/or before the endTime.

        @type       startTime: python datetime obj (utc)
        @param      startTime: The time "floor" to bound query.
        @type       endTime: python datetime obj (utc)
        @param      endTime: The time "ceiling" to bound query.
        @rtype:     list
        @return:    Returns a list of wf_uuid strings.
        """
        if not startTime and not endTime:
            return []
            
        retvals = []
        
        querystring = "self.session.query(Workflowstate.wf_id).filter(Workflowstate.state == 'start')"
        orderstring = '.order_by(Workflowstate.wf_id)'
        filterstring = ''
        
        if startTime:
            filterstring += ".filter(Workflowstate.timestamp > %s)" % calendar.timegm(startTime.timetuple())
        if endTime:
            filterstring += ".filter(Workflowstate.timestamp < %s)" % calendar.timegm(endTime.timetuple())
        
        fullquery = querystring + filterstring + orderstring
        query = eval(fullquery)
        
        wf_ids = []
        for row in query.all():
            wf_ids.append(row.wf_id)
        
        query = self.session.query(WorkflowTable.wf_uuid).filter(WorkflowTable.wf_id.in_(wf_ids))
        for row in query.all():
            retvals.append(as_string(row.wf_uuid))
        return retvals
        

### XXX: remove this testing code
if __name__ == '__main__':
    import os, calendar
    from netlogger.analysis.workflow.api_test import test_workflow_types
    os.chdir('/Users/monte/Desktop/Pegasus')
    debug = True
    
    db_conn = 'sqlite:///pegasusMontage.db'
    #db_conn = 'sqlite:///diamond.db'
    
    d = Discovery(db_conn)

    for uuid in d.fetch_all():
        w = Workflow(db_conn)
        #w._edges = False
        w.initialize(uuid)
        print w
        #test_workflow_types(w)
    pass
