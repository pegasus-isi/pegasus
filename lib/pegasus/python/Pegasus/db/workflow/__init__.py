"""
Base classes that define the interface that the back-end specific
retrieval classes must expose.  Each read-only property includes
inline epydoc describing what is to be returned and what type
(string, int, an object) each property must return when called.

How a subclass caches attributes or performs lazy evaluation is
implementation-specific and not defined here.  The only conformity
necessary is that the proper property returns the correct data as
the correct type.

Any class attributes or methods that are specific to the subclass
implementations MUST start with an underscore:

ie: self._wf_uuid = None or def _calculateValue(self, job):

This signals that the attr or method is subclass-specific and
also allows the inherited __repr__() method to ignore it.
"""

__author__ = "Monte Goode MMGoode@lbl.gov"

import logging

class WorkflowBase(object):
    # indent level for pretty printing = override in subclasses
    # if you want different indent levels for your various
    # objects.
    _indent = 1
    def __init__(self):
        self.log = logging.getLogger("%s.%s" % (self.__module__, self.__class__.__name__))

    def __repr__(self):
        spacer = '  '
        retval = '%s:' % self.__class__
        if self._indent > 1:
            retval = '\n%s+++ %s:' % (spacer * self._indent, self.__class__)
        for i in dir(self):
            if i.startswith('_') or i == 'initialize' or i == 'db' \
                or i == 'metadata' or i == 'session' or i == 'log':
                continue
            try:
                retval += '\n%s* %s : %s' % (spacer * self._indent, i, eval('self.%s' % i))
            except NotImplementedError as e:
                retval += '\n%s* %s : WARNING: %s' % (spacer * self._indent, i,e)
        return retval

class Workflow(WorkflowBase):
    """
    Top level workflow class that exposes information about
    a specific workflow and the associated jobs/etc.

    Usage::

     w = Workflow()
     w.initialize('unique_wf_uuid')
     print w.timestamp, w.dax_label

    etc
    """
    def __init__(self):
        WorkflowBase.__init__(self)

    def initialize(self, wf_id):
        """
        This method is the initialization method that accepts
        the unique wf_uuid and triggers the subclass specific
        queries and calculations that pulls the workflow
        information from the back end.

        The wf_id is represented in the .bp logs as "wf.id".

        @type   wf_id: string
        @param  wf_id: the unique wf_uuid as defined by Pegasus
        """
        raise NotImplementedError('initialize not yet implemented')

    @property
    def wf_uuid(self):
        """
        Return the wf_uuid for this workflow.

        @rtype:     string
        @return:    The wf_uuid of the current workflow
        """
        raise NotImplementedError('wf_uuid not yet implemented')

    @property
    def dax_label(self):
        """
        Return dax_label from storage backend.

        @rtype:     string
        @return:    The dax_label of the current workflow.
        """
        raise NotImplementedError('dax_label not yet implemented')

    @property
    def timestamp(self):
        """
        Return timestamp from storage backend.

        @rtype:     python datetime obj (utc)
        @return:    The workflow timestamp
        """
        raise NotImplementedError('timestamp not yet implemented')

    @property
    def submit_hostname(self):
        """
        Return submit_hostname from storage backend.

        @rtype:     string
        @return:    The workflow submit host
        """
        raise NotImplementedError('submit_hostname not yet implemented')

    @property
    def submit_dir(self):
        """
        Return submid_dir from storage backend.

        @rtype:     string
        @return:    The workflow submit directory
        """
        raise NotImplementedError('submit_dir not yet implemented')

    @property
    def planner_arguments(self):
        """
        Return planner_arguments from storage backend.

        @rtype:     string
        @return:    The workflow planner arguments
        """
        raise NotImplementedError('planner_arguments not yet implemented')

    @property
    def user(self):
        """
        Return user from storage backend.

        @rtype:     string
        @return:    The workflow user
        """
        raise NotImplementedError('user not yet implemented')

    @property
    def grid_dn(self):
        """
        Return grid_dn from storage backend.

        @rtype:     string
        @return:    The grid DN of the workflow
        """
        raise NotImplementedError('grid_dn not yet implemented')

    @property
    def planner_version(self):
        """
        Return planner_version from storage backend.

        @rtype:     string
        @return:    The planner version of the workflow
        """
        raise NotImplementedError('planner_version not yet implemented')

    @property
    def parent_wf_uuid(self):
        """
        Return parent_wf_uuid from storage backend.

        @rtype:     string
        @return:    The parent wf_uuid if it exists
        """
        raise NotImplementedError('parent_wf_uuid not yet implemented')

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
        raise NotImplementedError('sub_wf_uuids not yet implemented')

    @property
    def start_events(self):
        """
        Return a list of Workflowstate object instances representing
        the re/start events.  The list should ordered by timestamp.

        In the event that there are no logged workflow states an empty
        list should be returned.

        In the case that there is a dropped event (ie: no matching end
        event to a start event or vice versa), the missing event will
        be padded as a None.  This is an error situation.

        @rtype:     List of Workflowstate object instances (or None)
        @return:    Returns a list with workflow start events.
        """
        raise NotImplementedError('start_events not yet implemented')

    @property
    def end_events(self):
        """
        Return a list of Workflowstate object instances representing
        the end events.  The list should ordered by timestamp.

        In the event that there are no logged workflow states an empty
        list should be returned.

        In the case that there is a dropped event (ie: no matching end
        event to a start event or vice versa), the missing event will
        be padded as a None.  This is an error situation.

        @rtype:     List of Workflowstate object instances (or None)
        @return:    Returns a list with workflow end events.
        """
        raise NotImplementedError('end_events not yet implemented')

    @property
    def is_running(self):
        """
        Derived boolean flag indicating if the workflow
        is currently running.  Derived in a backend-appropriate
        way.

        @rtype:     boolean
        @return:    Indicates if the workflow is running.
        """
        raise NotImplementedError('is_running not yet implemented')

    @property
    def is_restarted(self):
        """
        Derived boolean flag indicating if this workflow has
        been retstarted.  Derived in a backend-appropriate
        way.

        @rtype:     boolean
        @return:    Indicates if the workflow has been restarted.
        """
        raise NotImplementedError('is_restarted not yet implemented')

    @property
    def restart_count(self):
        """
        Returns an integer reflecting restart count.  Derived in
        a backend-appropriate way.

        @rtype:     integer
        @return:    Number of workflow restarts.
        """
        raise NotImplementedError('restart_count not yet implemented')

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
        raise NotImplementedError('total_time not yet implemented')

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
        raise NotImplementedError('jobs not yet implemented')

    @property
    def total_jobs_executed(self):
        """
        Return the number of jobs that were executed as an
        integer value.

        @rtype:     integer
        @return:    Number of jobs executed
        """
        raise NotImplementedError('total_jobs_executed not yet implemented')

    @property
    def successful_jobs(self):
        """
        Return the number of jobs that executed successfully
        as an integer value.

        @rtype:     integer
        @return:    Number of sucessfully executed jobs
        """
        raise NotImplementedError('successful_jobs not yet implemented')

    @property
    def failed_jobs(self):
        """
        Return the number of jobs that failed as an integer
        value.

        @rtype:     integer
        @return:    Number of failed jobs
        """
        raise NotImplementedError('failed_jobs not yet implemented')

    @property
    def restarted_jobs(self):
        """
        Return the number of jobs that were restarted.

        @rtype:     integer
        @return:    Number of restarted jobs
        """
        raise NotImplementedError('restarted_jobs not yet implemented')

    @property
    def submitted_jobs(self):
        """
        Return the number of jobs that were submitted.

        @rtype:     integer
        @return:    Number of submitted jobs
        """
        raise NotImplementedError('submitted_jobs not yet implemented')

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
        raise NotImplementedError('jobtypes_executed not yet implemented')

class Workflowstate(WorkflowBase):
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
        WorkflowBase.__init__(self)

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
        raise NotImplementedError('initialize not yet implemented')

    @property
    def state(self):
        """
        Return the current workflowstate state.  Might be
        none if there is no state information logged yet.

        @rtype:     string or None
        @return:    Return current job state
        """
        raise NotImplementedError('state not implemented yet')

    @property
    def timestamp(self):
        """
        Return the timestamp of the current workflow state.  Might be
        none if there is no state information logged yet.

        @rtype:     python datetime obj (utc) or None
        @return:    Return timestamp of current job state
        """
        raise NotImplementedError('timestamp not implemented yet')


class Job(WorkflowBase):
    """
    Class to retrieve and expose information about a
    specific job.  This class is intended to be instantiated
    inside a Workflow() object and not as a stand-alone
    instance.

    Usage::

     j = Job()
     j.initialize('unique_wf_uuid', 3)
     print j.name

    etc
    """
    _indent = 2
    def __init__(self):
        WorkflowBase.__init__(self)

    def initialize(self, wf_id, job_id):
        """
        This method is the initialization method that accepts
        the unique wf_uuid and and job_submit_seq that triggers
        the subclass specific queries and calculations that
        pulls job information from the back end.

        The wf_id is represented in the .bp logs as "wf.id".
        The job_id is represented as "job.id".

        @type   wf_id: string
        @param  wf_id: the unique wf_uuid as defined by Pegasus
        @type   job_id: integer
        @param  job_id: the sequence number as defined
                by tailstatd
        """
        raise NotImplementedError('initialize not yet implemented')

    @property
    def job_submit_seq(self):
        """
        Return job_submit_seq of current job (an input arg).

        @rtype:     integer
        @return:    Return job_submit_seq of current job
        """
        raise NotImplementedError('job_submit_seq not yet implemented')

    @property
    def name(self):
        """
        Return the job name from the storage backend.

        @rtype:     string
        @return:    Return job name
        """
        raise NotImplementedError('name not yet implemented')

    @property
    def host(self):
        """
        Return job host information from storage backend.

        @rtype:     Host object instance
        @return:    Return a host object with host info for
                    current job.
        """
        raise NotImplementedError('host not yet implemented')

    @property
    def condor_id(self):
        """
        Return the condor_id from the storage backend.

        @rtype:     string (looks like a float however)
        @return:    Return job condor_id
        """
        raise NotImplementedError('condor_id not yet implemented')

    @property
    def jobtype(self):
        """
        Return jobtype from the storage backend.

        @rtype:     string
        @return:    Return jobtype
        """
        raise NotImplementedError('jobtype not yet implemented')

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
        raise NotImplementedError('clustered not yet implemented')

    @property
    def site_name(self):
        """
        Return the site name from the storage backend.

        @rtype:     string
        @return:    Return site_name for current job
        """
        raise NotImplementedError('site_name not yet implemented')

    @property
    def remote_user(self):
        """
        Return the remote use of the current job from
        the storage backend.

        @rtype:     string
        @return:    Return remote_user for current job.
        """
        raise NotImplementedError('remote_user not yet implemented')

    @property
    def remote_working_dir(self):
        """
        Return the remote working directory of the current
        job from the storage backend.

        @rtype:     string
        @return:
        """
        raise NotImplementedError('remote_working_dir not yet implemented')

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
        raise NotImplementedError('cluster_start_time not yet implemented')

    @property
    def cluster_duration(self):
        """
        Return the job cluster duration from the
        storage backend as a float or None if this value
        is not assocaited with the current job.  Not all j
        will have this value.

        @rtype:     float (from db)
        @return:
        """
        raise NotImplementedError('cluster_duration not yet implemented')

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
        raise NotImplementedError('tasks not yet implemented')

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
        raise NotImplementedError('is_restart not yet implemented')

    @property
    def is_success(self):
        """
        Return a boolean flag indicating whether or not this
        curent job was successful.  This value will be derived
        from backend information as appropriate.

        @rtype:     boolean
        @return:    Return True or False if the job is a restart
        """
        raise NotImplementedError('is_success not yet implemented')

    @property
    def is_failure(self):
        """
        Return a boolean flag indicating whether or not this
        curent job has failed.  This value will be derived
        from backend information as appropriate.

        @rtype:     boolean
        @return:    Return True or False if the job is a restart
        """
        raise NotImplementedError('is_failure not yet implemented')

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
        @return:    Return the current/last jobstate event.
        """
        raise NotImplementedError('current_state not yet implemented')

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
        raise NotImplementedError('all_jobstates not yet implemented')

    @property
    def submit_time(self):
        """
        Return the timestamp of when this job was submitted.

        @rtype:     python datetime obj (utc) or None
        @return:    Return the submit time of this job
        """
        raise NotImplementedError('submit_time not yet implemented')

    @property
    def elapsed_time(self):
        """
        Return the elapsed time of this job.  Calculated as
        the delta between the submit time and the current/last
        jobstate timestamp.

        @rtype:     python datetime.timedelta or None
        @return:    Return the elapsed time of this job
        """
        raise NotImplementedError('elapsed_time not yet implemented')

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
        raise NotImplementedError('edge_parents not yet implemented')

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
        raise NotImplementedError('edge_children not yet implemented')


class Jobstate(WorkflowBase):
    """
    A small class that returns jobstate information.  Intended
    to be instantiated by a call to job.current_state.  Is
    not cached so multiple calls will return the latest information.

    Usage::

     js = Jobstate()
     js.initialize('unique_wf_id', 3)
     print js.state, js.timestamp

    etc
    """
    _indent = 3
    def __init__(self):
        WorkflowBase.__init__(self)

    def initialize(self, wf_id, job_id):
        """
        This method is the initialization method that accepts
        the unique wf_uuid and and job_submit_seq that triggers
        the subclass specific queries and calculations that
        pulls host information from the back end.

        The wf_id is represented in the .bp logs as "wf.id".
        The job_id is represented as "job.id".

        @type   wf_id: string
        @param  wf_id: the unique wf_uuid as defined by Pegasus
        @type   job_id: integer
        @param  job_id: the sequence number as defined
                by tailstatd
        """
        raise NotImplementedError('initialize not yet implemented')

    @property
    def state(self):
        """
        Return the current jobstate state.  Might be
        none if there is no state information logged yet.

        @rtype:     string or None
        @return:    Return current job state
        """
        raise NotImplementedError('state not implemented yet')

    @property
    def timestamp(self):
        """
        Return the timestampe of the current job state.  Might be
        none if there is no state information logged yet.

        @rtype:     python datetime obj (utc) or None
        @return:    Return timestamp of current job state
        """
        raise NotImplementedError('timestamp not implemented yet')


class Host(WorkflowBase):
    """
    A straightforward class that contains host information
    about a job.  This is intended to be instantiated inside
    a Job() object and not as a standalone instance.

    Usage::

     h = Host()
     h.initialize('unique_wf_uuid', 3)
     print h.site_name, h.hostname

    etc.
    """
    _indent = 3
    def __init__(self):
        WorkflowBase.__init__(self)

    def initialize(self, wf_id, job_id):
        """
        This method is the initialization method that accepts
        the unique wf_uuid and and job_submit_seq that triggers
        the subclass specific queries and calculations that
        pulls host information from the back end.

        The wf_id is represented in the .bp logs as "wf.id".
        The job_id is represented as "job.id".

        @type   wf_id: string
        @param  wf_id: the unique wf_uuid as defined by Pegasus
        @type   job_id: integer
        @param  job_id: the sequence number as defined
                by tailstatd
        """
        raise NotImplementedError('initialize not yet implemented')

    @property
    def site_name(self):
        """
        Return the site name associated with this host.
        Might be None if a host has not been associated
        with a particular job at time of calling.

        @rtype:     string or None
        @return:    Return host site name or None
        """
        raise NotImplementedError('site_name not yet implemented')

    @property
    def hostname(self):
        """
        Return the host name associated with this host.
        Might be None if a host has not been associated
        with a particular job at time of calling.

        @rtype:     string or None
        @return:    Return hostname or None
        """
        raise NotImplementedError('hostname not yet implemented')

    @property
    def ip_address(self):
        """
        Return the ip address associated with this host.
        Might be None if a host has not been associated
        with a particular job at time of calling.

        @rtype:     string or None
        @return:    Return host ip address or None
        """
        raise NotImplementedError('ip_address not yet implemented')

    @property
    def uname(self):
        """
        Return the uname information of this host machine.
        Might be None if a host has not been associated
        with a particular job at time of calling.

        @rtype:     string or None
        @return:    Return host uname or None.
        """
        raise NotImplementedError('uname not yet implemented')

    @property
    def total_ram(self):
        """
        Return the total ram of this host machine.
        Might be None if a host has not been associated
        with a particular job at time of calling.

        @rtype:     integer or None
        @return:    Return host RAM or None
        """
        raise NotImplementedError('total_ram not yet implemented')


class Task(WorkflowBase):
    """
    Class to expose information about a task associated
    with a particular job.  This is intended to be
    instantiated inside of a Job() object and not as a
    stand alone instance.

    Usage::

     t = Task()
     t.initialize('unique_wf_uuid', 3, 1)
     print t.start_time, t.duration

    etc
    """
    _indent = 3
    def __init__(self):
        WorkflowBase.__init__(self)

    def initialize(self, wf_id, job_id, task_id):
        """
        This method is the initialization method that accepts
        the unique wf_uuid, job_submit_seq and task_submit_seq
        that triggers the subclass specific queries and
        calculations that pulls task information from the back end.

        The wf_id is represented in the .bp logs as "wf.id".
        The job_id is represented as "job.id".
        The task_id is represented as "task.id".

        @type   wf_id: string
        @param  wf_id: the unique wf_uuid as defined by Pegasus
        @type   job_id: integer
        @param  job_id: the sequence number as defined
                by tailstatd
        @type   task_id: integer
        @param  task_id: the sequence number as defined
                by tailstatd
        """
        raise NotImplementedError('initialize not yet implemented')

    @property
    def task_submit_seq(self):
        """
        Return the task submit sequence number from the
        storage backend as an integer.

        @rtype:     int
        @return:    submit sequence number
        """
        raise NotImplementedError('task_submit_seq not yet implemented')

    @property
    def start_time(self):
        """
        Return start time of this task from the storage
        backend as a python datetime object (utc).

        @rtype:     python datetime obj (utc)
        @return:    Return task start time
        """
        raise NotImplementedError('start_time not yet implemented')

    @property
    def duration(self):
        """
        Return duration of this task from the storage
        backend as a float.

        @rtype:     float (from db)
        @return:    Return the duration of this task
        """
        raise NotImplementedError('duration not yet implemented')

    @property
    def exitcode(self):
        """
        Return the exitcode of this task from the storage
        backend as an integer.

        @rtype:     integer
        @return:    Return the task exitcode
        """
        raise NotImplementedError('exitcode not yet implemented')

    @property
    def transformation(self):
        """
        Return the transformation type of this task from
        the storage backend.

        @rtype:     string
        @return:    Return task transformation
        """
        raise NotImplementedError('transformation not yet implemented')

    @property
    def executable(self):
        """
        Return the executable invoked by this task from
        the storage backend.

        @rtype:     string
        @return:    Return the task executable
        """
        raise NotImplementedError('executable not yet implemented')

    @property
    def arguments(self):
        """
        Return the task args from the storage backend.

        @rtype:     string
        @return:    Return the task arguments
        """
        raise NotImplementedError('arguments not yet implemented')

# Base class for discovery/wf_uuid querying.

class Discovery(object):
    def __init__(self, connectionInfo=None):
        """
        Initialization method.  The manditory argument connectionInfo
        will be defined as appropriate in a subclass (string, dict, etc),
        and connect to the appropriate back end.
        """
        pass

    def fetch_all(self):
        """
        Void method that will return a list of workflow uuids
        from the back end.

        @rtype:     list
        @return:    Returns a list of wf_uuid strings.
        """
        raise NotImplementedError('fetch_all not yet implemented')

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
        raise NotImplementedError('time_threshold not yet implemented')
