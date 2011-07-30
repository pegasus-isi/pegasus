"""
Embedded REST server for dashboard

Requires 'web.py' framework.
"""
import json
import web
import re
import time
import uuid

import sqlalchemy as sa
from netlogger.analysis.workflow import stampede_statistics as sstat

DB_URL = 'sqlite:///sample.db'
g_engine =  sa.create_engine(DB_URL)

def json_header():
    "Add JSON header."
    web.header('Content-Type', 'text/json') 

def returns_json(fn):
    """Decorator to add headers and convert
       Python dict to JSON string before returning it.
    """
    def new(self, *args, **kwarg):
        json_header()
        s = json.dumps(fn(self, *args, **kwarg))
        web.header('Content-Length', str(len(s)))
        return s
    return new

def find_workflows(engine, dburl, label=None, submit_host=None, user=None,
                   grid_dn=None):
    # Get list of workflows
    conn = engine.connect()
    qstr = "select wf_id, wf_uuid, dax_label from workflow" # XXX: ignore filters
    result = conn.execute(qstr)
    wf_list, workflows = [ ], { }
    # Build workflow summary info for each workflow
    for wf_id, wf_uuid, label in result:
        stats = sstat.StampedeStatistics(dburl, True)
        stats.set_job_filter('nonsub')
        stats.initialize(wf_uuid)
        ttl = stats.get_total_tasks_status()
        succ = stats.get_total_succeeded_tasks_status()
        fail = stats.get_total_failed_tasks_status()
        retry = stats.get_total_tasks_retries()
        parent_wf_id = None
        datum = {
            'id': wf_uuid,
            'name' : label,
            'pid': parent_wf_id,
            'running' : True,
            'restarted' : stats.get_workflow_retries(),
            'wallclock' : float(stats.get_submit_side_job_wall_time()),
            'jobs': {
                'total' : ttl, 'successful': succ , 'failed': fail,
                'restarted': retry, 'queued': ttl - retry - succ - fail },
            'subwf' : [ ],
            'children' : [ ] }
        wf_list.append(datum)
        workflows[wf_id] = datum
    # Put list of children in all parents
    for wf_id in workflows:
        parent = datum['pid']
        if workflows.has_key(parent):
            workflows[parent]['children'].append(wf_id)
    # Recursively nest workflows in a list
    wf_list = [ ]
    while workflows:
        for wf_id, datum in workflows.items():
            n = len(datum['children'])
            # If all children are filled in (including zero of them),
            # then add this workflow to its parent or at the top-level. 
            if n == len(datum['subwf']):
                if datum['pid']:
                    # Add to parent
                    workflows['subwf'].append(datum)
                else:
                    # Add to top-level list
                    wf_list.append(datum)
                # Delete workflow from working set
                del workflows[wf_id]
    # Return the (nested) list of workflows
    #print("@@ wf_list="+str(wf_list))
    return wf_list

def get_max_ts(engine):
    result = 0.0
    conn = engine.connect()
    try:
        qstr = "select max(timestamp) as 'ts' from jobstate"        
        result = float(conn.execute(qstr).first()[0])
    except sa.exc.SQLAlchemyError, err:
        pass #XXX: should do *something*
    return result

class Index(object):
    @returns_json
    def GET(self, name=''):
        return {'title': 'Options',
                'menu' : [{'name':'workflow list',
                           'desc':'filtered list of workflows',
                           'path':'/workflows/'},
                          ]}

class Workflows(object):
    @returns_json
    def GET(self, n=None):
        fltr = web.input() #XXX: mostly ignored    
        workflow_data = find_workflows(g_engine, DB_URL)
        return {'title':'workflow list',
                'filter':str(fltr),
                'data': {
                    'wf':workflow_data,
                    'max_timestamp' : get_max_ts(g_engine),
                    }
                }
    
urls = (
    '/', 'Index',
    '/workflows/', 'Workflows',
    '/workflow/(.*)', 'Workflow')

if __name__ == "__main__":
    app = web.application(urls, globals())
    app.run()
