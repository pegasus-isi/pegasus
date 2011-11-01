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

#DB_URL = 'sqlite:///sample.db'
DB_URL = 'sqlite:////Users/dang/Logs/Stampede/run0005/Stampede-Test-0.stampede.db'
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
        print(json.dumps(fn(self, *args, **kwarg)))
        return json.dumps(fn(self, *args, **kwarg))
        #web.header('Content-Length', '{0:d}'.format(len(s)))
        #return s
    return new

def find_workflows(engine, dburl, label=None, submit_host=None, user=None,
                   grid_dn=None):
    # Get list of workflows
    conn = engine.connect()
    qstr = "select wf_id, root_wf_id, wf_uuid, dax_label from workflow" # XXX: ignore filters
    result = conn.execute(qstr)
    workflows = { }
    # Build workflow summary info for each workflow
    for wf_id, root_wf_id, wf_uuid, label in result:
        do_expand = wf_id == root_wf_id # only expand root workflows
        stats = sstat.StampedeStatistics(connString=dburl, expand_workflow=do_expand)
        stats.initialize(wf_uuid)
        stats.set_job_filter('all')
        ttl = stats.get_total_tasks_status()
        succ = stats.get_total_succeeded_tasks_status()
        fail = stats.get_total_failed_tasks_status()
        retry = stats.get_total_tasks_retries()
        try:
            wallclock = float(stats.get_submit_side_job_wall_time()),
        except TypeError:
            wallclock = 0
        datum = {
            'id': wf_uuid,
            'name' : label,
            'pid': root_wf_id,
            'running' : True,
            'restarted' : stats.get_workflow_retries(),
            'wallclock' : wallclock,
            'jobs': {
                'total' : ttl, 'successful': succ , 'failed': fail,
                'restarted': retry, 'queued': ttl - retry - succ - fail },
            'subwf' : [ ] }
        workflows[wf_id] = datum
        #stats.close()
    # Nest children under parents
    delete_later = [ ]
    for wfid, datum in workflows.iteritems():
        parent_id = datum['pid']
        if parent_id == wfid:
            pass # root, nothing to do
        else:
            workflows[parent_id]['subwf'].append(datum)
            delete_later.append(wfid)
    for wfid in delete_later:
        del workflows[wfid]
    return workflows.values()

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

class Home(object):
    def GET(self):
        lines = file("dash.html").readlines()
        return '\n'.join(lines)

class Workflows(object):
    @returns_json
    def GET(self, n=None):
        fltr = web.input() #XXX: mostly ignored    
        workflow_data = find_workflows(g_engine, DB_URL)
        result = {'title':'workflow list',
                'filter': "NULL",
                'data': {
                    'wf':workflow_data,
                    'max_timestamp' : get_max_ts(g_engine),
                    }
                }
        return result

urls = (
    '/', 'Index',
    '/workflows/.*', 'Workflows',
    '/workflow/(.*)', 'Workflow')

if __name__ == "__main__":
    app = web.application(urls, globals())
    app.run()
