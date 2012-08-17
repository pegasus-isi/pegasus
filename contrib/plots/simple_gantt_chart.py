#!/usr/bin/env python
# This script generates a gantt chart of a workflow given the stampede
# database connection information and a workflow UUID.
#
# The gantt chart comes out in PDF format, at a size that is suitable
# for printing.
#
# This script is designed for very large workflows containing many
# thousands of tasks. As such, it does not contain any details about
# the tasks.
import sys
import os
import urlparse
import MySQLdb
import matplotlib as mpl

mpl.use('Agg')

import matplotlib.pyplot as plot

def fail(err):
    sys.stderr.write(err)
    sys.stderr.write('\n')
    sys.exit(1)

if len(sys.argv) != 3:
    print "Usage: %s dburl wf_uuid" % sys.argv[0]
    print
    print "Example: %s mysql://user:pass@shock.usc.edu:3306/monitord_400 645a6ea1-69d0-4c0e-8e28-debdd8b7515b" % sys.argv[0]
    sys.exit(1)

dburl = sys.argv[1]
wf_uuid = sys.argv[2]

if not dburl.startswith('mysql://'):
    fail("only mysql is supported at this time")

url = urlparse.urlparse(dburl, 'mysql')
user = url.username or fail("Database username required")
passwd = url.password or ""
host = url.hostname or fail("Database hostname required")
port = url.port or 3306
dbname = url.path[1:]

db = MySQLdb.connect(host=host, port=port, user=user, passwd=passwd, db=dbname)

cur = db.cursor()

cur.execute("SELECT wf_id, dax_label from workflow WHERE wf_uuid='%s'" % (wf_uuid))

wf = cur.fetchone()

if wf is None:
    fail("No such workflow: %s" % wf_uuid)

wf_id, name = wf

print name

cur.execute("""
select i.start_time-(
    select min(i1.start_time) 
    from invocation i1 
    where i1.wf_id=%s and i1.abs_task_id is not NULL), i.remote_duration 
from invocation i 
where i.wf_id=%s and i.abs_task_id is not NULL""", [wf_id, wf_id])

data = []

xlim = 0
ylim = 0
for r in cur:
    start, duration = r
    start = float(start)
    duration = float(duration)
    data.append((start,duration))
    xlim = max(xlim, start+duration)
    ylim += 1

cur.close()

db.close()

if len(data) == 0:
    fail("No jobs found for workflow %s" % wf_uuid)

data.sort()

left = [d[0] for d in data] # left is start
width = [d[1] for d in data] # width is duration

plot.figure(figsize=(8, 6))

axes = plot.axes()
axes.get_xaxis().tick_bottom()
axes.get_yaxis().tick_left()

plot.title(name)
plot.xlabel('Time (seconds)')
plot.ylabel('Task')

plot.ylim(0,ylim+(ylim*0.1))
plot.xlim(0,xlim+(xlim*0.1))

plot.barh(range(ylim), width, height=1, left=left, edgecolor='blue')

plot.savefig('%s.pdf' % name)

