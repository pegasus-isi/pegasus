"""
Test module to check that a specific implementation of the workflow retrieval
API is returning the correct types.  Program should import test_workflow_types()
function and pass it an initialized Workflow instance.

Usage:

from netlogger.analysis.workflow.sql_alchemy import Workflow
from netlogger.analysis.workflow.api_test import test_workflow_types

w = Workflow('sqlite:///pegasusMontage.db')
w.initialize('5117013a-f7f1-4bc5-a2f8-517186599fad')
test_workflow_types(w)

Any properties that are not returning the correct type will be listed along
with the return value and the incorrect type that is being returned, otherwise
the section will be marked as passed.

Sample output including errors:

** Checking workflow object:
   dax_label montage <type 'unicode'>

** Checking w.jobtypes_executed:
   Passed

** Checking w.jobs for instance type:
   Passed
   
Etc.

"""

__rcsid__ = "$Id: api_test.py 26758 2010-11-10 19:09:16Z mgoode $"
__author__ = "Monte Goode MMGoode@lbl.gov"

from netlogger.analysis.workflow._base import Job as JobBase, Host as HostBase,\
    Task as TaskBase, Jobstate as JobstateBase, Workflowstate as WorkflowstateBase

import datetime

# basic type checks
string = type('string')
integer = type(1)
floating = type(1.0)
timestamp = type(datetime.datetime.utcfromtimestamp(100))
boolean = type(True)
nonetype = type(None)
delta = type(datetime.timedelta(seconds=2 - 1))
dictionary = type({})
array = type([])

# object type dicts
workflow_types = {
    'wf_uuid' : (string),
    'dax_label' : (string, nonetype),
    'timestamp' : (timestamp, nonetype),
    'submit_hostname' : (string, nonetype),
    'submit_dir' : (string, nonetype),
    'planner_arguments' : (string, nonetype),
    'user' : (string, nonetype),
    'grid_dn' : (string, nonetype),
    'parent_wf_uuid' : (string, nonetype),
    'is_running' : (boolean),
    'is_restarted' : (boolean),
    'restart_count' : (integer),
    'total_time' : (delta, nonetype),
    'jobs' : (array),
    'total_jobs_executed' : (integer),
    'successful_jobs' : (integer),
    'failed_jobs' : (integer),
    'restarted_jobs' : (integer),
    'submitted_jobs' : (integer),
    'jobtypes_executed' : (dictionary)
}

workflowstate_types = {
    'state' : (string, nonetype),
    'timestamp' : (timestamp, nonetype)
}

job_types = {
    'job_submit_seq' : (integer),
    'name' : (string),
    'host' : (HostBase),
    'condor_id' : (string, nonetype),
    'jobtype' : (string),
    'clustered' : (boolean),
    'site_name' : (string, nonetype),
    'remote_user' : (string, nonetype),
    'remote_working_dir' : (string, nonetype),
    'cluster_start_time' : (timestamp, nonetype),
    'cluster_duration' : (floating, nonetype),
    'tasks' : (array),
    'is_restart' : (boolean),
    'is_success' : (boolean),
    'is_failure' : (boolean),
    'current_state' : (JobstateBase),
    'all_jobstates' : (array),
    'submit_time' : (timestamp, nonetype),
    'elapsed_time' : (delta, nonetype),
    'edge_parents' : (array),
    'edge_children' : (array)
}

host_types = {
    'site_name' : (string, nonetype),
    'hostname' : (string, nonetype),
    'ip_address' : (string, nonetype),
    'uname' : (string, nonetype),
    'total_ram' : (integer, nonetype)
}

jobstate_types = {
    'state' : (string, nonetype),
    'timestamp' : (timestamp, nonetype)
}

task_types = {
    'task_submit_seq' : (integer),
    'start_time' : (timestamp),
    'duration' : (floating),
    'exitcode' : (integer),
    'transformation' : (string),
    'executable' : (string),
    'arguments' : (string, nonetype)
}

def test_workflow_types(w):
    """
    Tests an instance of a workflow object and it's children to ensure
    that the correct return types are being returned.  Used for testing
    differing backend implementations.
    
    This version just dumps output to the console for a quick visual check.
    """
    # workflow
    passed = True
    
    print '** Checking workflow object:'
    for k,v in workflow_types.items():
        testval = eval('w.%s' % k)
        if not isinstance(testval, v):
            print '  ', k, testval, type(testval)
            passed = False
            
    if passed:
        print '   Passed'
    else:
        passed = True
    
    print '\n** Checking w.jobtypes_executed:'
    for k,v in w.jobtypes_executed.items():
        if not isinstance(k, string):
            print '   jobtype key problem:', k, type(k)
            passed = False
        if not isinstance(v, integer):
            print '   jobtype value problem', v, type(v)
            passed = False
            
    if passed:
        print '   Passed'
    else:
        passed = True
        
    print '\n** Checking workflow state:'
    ws = w.start_events[0]
    for k,v in workflowstate_types.items():
        testval = eval('ws.%s' % k)
        if not isinstance(testval, v):
            print '  ', k, testval, type(testval)
            passed = False
            
    if passed:
        print '   Passed'
    else:
        passed = True
    
    print '\n** Checking w.jobs for instance type:'
    if w.jobs:
        job = w.jobs[0]
        if not isinstance(job, JobBase):
            print '   Job instance type failed:', job
            passed = False
            
        if passed:
            print '   Passed'
        else:
            passed = True
    
        # job
        print '\n** Checking Job properties:'
        for k,v in job_types.items():
            testval = eval('job.%s' % k)
            if not isinstance(testval, v):
                print '  ', k, testval, type(testval)
                passed = False
                
        if passed:
            print '   Passed'
        else:
            passed = True
            
        print '\n** Checking Host properties:'
        for k,v in host_types.items():
            testval = eval('job.host.%s' % k)
            if not isinstance(testval, v):
                print '  ', k, testval, type(testval)
                passed = False
                
        if passed:
            print '   Passed'
        else:
            passed = True
            
        print '\n** Checking Jobstate properties:'
        for k,v in jobstate_types.items():
            testval = eval('job.current_state.%s' % k)
            if not isinstance(testval, v):
                print '  ', k, testval, type(testval)
                passed = False
                
        if passed:
            print '   Passed'
        else:
            passed = True
                
        if job.tasks:
            print '\n** Checking Task properties:'
            task = job.tasks[0]
            for k,v in task_types.items():
                testval = eval('task.%s' % k)
                if not isinstance(testval, v):
                    print '  ', k, testval, type(testval)
                    passed = False
            if passed:
                print '   Passed'
        else:
            print 'WARNING: no valid Task object to test!'
            
        if job.edge_children:
            print '\n** Checking child edges'
            job = job.edge_children[0]
            if not isinstance(job, JobBase):
                print '   Job instance type failed:', job
                passed = False

            if passed:
                print '   Passed'
            else:
                passed = True
                
        if job.edge_parents:
            print '\n** Checking parent edges'
            job = job.edge_parents[0]
            if not isinstance(job, JobBase):
                print '   Job instance type failed:', job
                passed = False

            if passed:
                print '   Passed'
            else:
                passed = True
    else:
        print 'WARNING: no valid Job object to test!'
    pass
    
    
def test_workflow_types_list(w):
    """
    Like test_workflow_types, but returns a list of problems.  More
    useful for unittests.
    """

    messages = []

    for k,v in workflow_types.items():
        testval = eval('w.%s' % k)
        if not isinstance(testval, v):
            messages.append('Workflow property %s returned %s (%s)' \
                % (k, type(testval), testval))

    for k,v in w.jobtypes_executed.items():
        if not isinstance(k, string):
            messages.append('Workflow.jobtypes_executed jobtype key problem: %s (%s)' % (type(k), k))
        if not isinstance(v, integer):
            messages.append('Workflow.jobtypes_executed jobtype value problem: %s (%s)' % (type(v), v))
            
    ws = w.start_events[0]
    for k,v in workflowstate_types.items():
        testval = eval('ws.%s' % k)
        if not isinstance(testval, v):
            messages.append('Workflowstate property %s returned %s (%s)' % (k, type(testval), testval))
    
    if w.jobs:
        job = w.jobs[0]
        if not isinstance(job, JobBase):
            messages.append('Workflow.job instance type failed: %s' % job.__class__)
        
        for k,v in job_types.items():
            testval = eval('job.%s' % k)
            if not isinstance(testval, v):
                messages.append('Job property %s returned %s (%s)' % (k, type(testval), testval))
        
        for k,v in host_types.items():
            testval = eval('job.host.%s' % k)
            if not isinstance(testval, v):
                messages.append('Host property %s returned %s (%s)' % (k, type(testval), testval))
        
        for k,v in jobstate_types.items():
            testval = eval('job.current_state.%s' % k)
            if not isinstance(testval, v):
                messages.append('Jobstate property %s returned %s (%s)' % (k, type(testval), testval))

        if job.tasks:
            task = job.tasks[0]
            for k,v in task_types.items():
                testval = eval('task.%s' % k)
                if not isinstance(testval, v):
                    messages.append('Task property %s returned %s (%s)' % (k, type(testval), testval))
        else:
            messages.append('WARNING: no valid Task object to test!')
        
        if job.edge_parents:
            job = job.edge_parents[0]
            if not isinstance(job, JobBase):
                messages.append('Workflow.job.edge_parents instance type failed: %s' % job.__class__)
                
        if job.edge_children:
            job = job.edge_children[0]
            if not isinstance(job, JobBase):
                messages.append('Workflow.job.edge_children instance type failed: %s' % job.__class__)
    else:
        messages.append('WARNING: no valid Job object to test!')
        
    return messages
    
def create_reference_dump(w):
    """
    Create a reference dump of a workflow object.  Can be used for comparing
    output in tests or across implementations.
    
    Could just print the top level object, but this explicitly iterates 
    through all of the nested lists.
    """
    print 'Workflow:\n'
    wt = workflow_types.keys()
    wt.sort()
    for k in wt:
        if k in ['jobs']:
            continue
        print k, eval('w.%s\n' % k)
        
    print '\nJobs:\n'
    jt = job_types.keys()
    jt.sort()
    for j in w.jobs:
        for k in jt:
            if k in ['tasks', 'all_jobstates']:
                continue
            print k, eval('j.%s' % k)
        
        print '\n  Tasks:'
        for t in j.tasks:
            print '  ', t
        print '\n  Jobstates:'
        for js in j.all_jobstates:
            print '  ', js
        print '\n============\n'
    
def main():
    pass
    
if __name__ == '__main__':
    main()