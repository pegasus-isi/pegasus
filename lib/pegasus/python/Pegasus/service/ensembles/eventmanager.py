import os
import sys
import subprocess
import logging
import time
import threading
import datetime
from sqlalchemy.orm.exc import NoResultFound
import json
import subprocess


from Pegasus import user
from Pegasus.db import connection
from Pegasus.service import app
from Pegasus.db.ensembles import Ensembles, EnsembleStates, EnsembleWorkflowStates, EMError
from Pegasus.db.schema import DashboardWorkflow, DashboardWorkflowstate
from datetime import datetime

log = logging.getLogger(__name__)

class EnsembleEventManager(threading.Thread):
    def __init__(self, interval=None):
        threading.Thread.__init__(self)
        self.daemon = True
        self.timestamp = None
        self.script = None
        self.pegasus_args = None
        self.cycle = 0
        self.daxdir = None
        if interval is None:
            interval = float(app.config["EV_INTERVAL"])
        self.interval = interval

    def run(self):
        print "Event-Manager starting"
        log.info("Event-Manager starting")
        # self.timestamp = subprocess.check_output(["date"])
        # self.timestamp = self.timestamp.strip("\n")
        self.timestamp = {}
        self.script = {}
        self.pegasus_args = {}
        self.cycle = {}
        self.daxdir = {}
        self.loop_forever()

    def loop_forever(self):
        while True:
            u = user.get_user_by_uid(os.getuid())
            session = connection.connect(u.get_master_db_url())
            try:
                dao = Ensembles(session)
                self.loop_once(dao,u)
            finally:
                session.close()
            time.sleep(self.interval)

    def loop_once(self, dao, u):
        actionable = dao.list_ensembles(u.username)
        if len(actionable) == 0:
            return

        log.info("Processing ensembles events")
        for e in actionable:
            log.info("Processing ensemble events %s", e.name)
            print "Processing ensemble events ",e.name
            if not e.name in self.timestamp.keys() and not e.name in self.cycle.keys():
                self.timestamp[e.name] = subprocess.check_output(["date"])
                self.timestamp[e.name] = self.timestamp[e.name].strip("\n")
                self.cycle[e.name] = 1
            if self.has_events(e):
                try:
                    print "Executing script to generate new dax"
                    subprocess.call([self.script[e.name],"workflow_dax"+str(self.cycle[e.name])+".dax"])
                    os.chdir(str(self.daxdir[e.name]))
                    os.system("pegasus-em submit "+str(e.name)+"."+str(self.cycle[e.name])+" "+str(self.pegasus_args[e.name])+" "+str(self.daxdir[e.name])+"/"+"workflow_dax"+str(self.cycle[e.name])+".dax")
                    self.cycle[e.name]+=1
                    self.timestamp[e.name] = subprocess.check_output(["date"])
                    self.timestamp[e.name] = self.timestamp[e.name].strip("\n")
                except Exception as e:
                    print e
            # eventconfig = e.get_eventconfig()


    def has_events(self,e):
        eventconfig = e.get_eventconfig()
        #print eventconfig
        #print os.path.exists(eventconfig)
        if not eventconfig is None:
            if not os.path.isfile(eventconfig):
                print eventconfig
                print "Path specified for the event config does not point to any file"
                return False
            with open(eventconfig) as data_file:
                data = json.load(data_file)
            if not "event-type" in data.keys():
                print "Specify event-type: file | hdfs | file-dir | hdfs-dir"
                return False

            if not "event-cycle" in data.keys():
                print "event-cycle is necessary in event-config file"
                return False

            if data["event-type"]=="file":
                if not "event-file" in data.keys():
                    print "event-type is file but event-file is not specified"
                    return False
                if not os.path.exists(data["event-file"]):
                    return False

                if not "pegasus-args" in data.keys():
                    print "pegasus-args not provided"
                    return False

                if not "event-script" in data.keys():
                    print "event-script not provided"
                    return False

                if not "event-dax-dir" in data.keys():
                    print "event-dax-dir not provided"
                    return False

                if not os.path.exists(data["event-dax-dir"]):
                    print "Path to event-dax-dir does not exist"
                    return False

                self.daxdir[e.name] = data["event-dax-dir"]

                if not os.path.exists(data["event-script"]):
                    print "event-script path does not exist"
                    return False

                self.script[e.name] = data["event-script"]


                if not "event-content" in data.keys():
                    return True

                data["event-cycle"] = int(data["event-cycle"])
                self.pegasus_args[e.name] = data["pegasus-args"]
                # greater_than_cycle = [i for i in data["event-cycle"] if i>=workflow.get_event_cycle()]
                # idx_of_event_content = data["event-cycle"].index(greater_than_cycle[0])
                # workflow.set_event_maxcycle(greater_than_cycle[len(greater_than_cycle)-1])
                #self.maxcycle = greater_than_cycle[len(greater_than_cycle)-1]
                #print "index",idx_of_event_content

                statinfo = os.stat(data["event-file"])

                timestamp = time.ctime(statinfo.st_mtime)
                file_timestamp = datetime.strptime(timestamp,"%a %b %d %H:%M:%S %Y")

                timestamp = self.timestamp[e.name]
                event_timestamp = datetime.strptime(timestamp,"%a %b %d %H:%M:%S %Z %Y")

                if file_timestamp<event_timestamp:
                    return False

                for content in data["event-content"]:
                    print "content",content
                    if not content in open(data["event-file"]).read():
                        return False

            if data["event-type"]=="file-dir":
                if not "event-dir" in data.keys():
                    print "event-type is file-dir but event-dir is not specified"
                    return False
                if not os.path.exists(data["event-dir"]):
                    return False
                if not "event-size" in data.keys():
                    print "event-size is necessary"
                    return False

                if not "pegasus-args" in data.keys():
                    print "pegasus-args not provided"
                    return False

                if not "event-script" in data.keys():
                    print "event-script not provided"
                    return False

                if not "event-dax-dir" in data.keys():
                    print "event-dax-dir not provided"
                    return False

                if not os.path.exists(data["event-dax-dir"]):
                    print "Path to event-dax-dir does not exist"
                    return False

                self.daxdir[e.name] = data["event-dax-dir"]

                if not os.path.exists(data["event-script"]):
                    print "event-script path does not exists"
                    return False

                self.script[e.name] = data["event-script"]

                if not "event-content" in data.keys():
                    return True

                if data["event-content"]=="*":

                    if not "event-numfiles" in data.keys():
                        print "event-numfiles is necessary"
                        return False

                    data["event-cycle"] = int(data["event-cycle"])
                    self.pegasus_args[e.name] = data["pegasus-args"]

                    event_timestamp = self.timestamp[e.name]
                    # print "Workflow timestamp",workflow_timestamp
                    if event_timestamp is None:
                        print "Workflow timestamp not set yet"
                        return False
                        #workflow_timestamp = os.system("echo date")

                        # print "Afterwards",workflow_timestamp,workflow.get_event_timestamp()
                    #num_files = os.system("find '"+data["event-dir"]+"' -size +"+str(data["event-size"])+"c -newermt '"+str(workflow_timestamp)+"' -exec /bin/echo {} \; | wc -l")
                    files = subprocess.check_output(["find", data["event-dir"], "-size", str(data["event-size"])+"c", "-newermt", str(event_timestamp)])
                    num_files = files.count("\n")-1
                    print num_files, data["event-dir"],data["event-size"],event_timestamp,data["event-numfiles"]
                    if (num_files) < data["event-numfiles"]:
                        return False


                else:
                    data["event-cycle"] = int(data["event-cycle"])
                    self.pegasus_args[e.name] = data["pegasus-args"]
                    # greater_than_cycle = [i for i in data["event-cycle"] if i>=workflow.get_event_cycle()]
                    # idx_of_event_content = data["event-cycle"].index(greater_than_cycle[0])
                    # workflow.set_event_maxcycle(greater_than_cycle[len(greater_than_cycle)-1])
                    #self.maxcycle = greater_than_cycle[len(greater_than_cycle)-1]
                    # print "index",idx_of_event_content
                    # idx = workflow.get_event_cycle()
                    for content in data["event-content"]:
                        # print "content",content, it, idx, int(data["event-size"][idx-1][it])
                        if not os.path.exists(data["event-dir"]+"/"+content):
                            return False

                        statinfo = os.stat(data["event-dir"]+"/"+content)
                        filesize = int(statinfo.st_size)

                        timestamp = time.ctime(statinfo.st_mtime)
                        file_timestamp = datetime.strptime(timestamp,"%a %b %d %H:%M:%S %Y")

                        timestamp = self.timestamp[e.name]
                        event_timestamp = datetime.strptime(timestamp,"%a %b %d %H:%M:%S %Z %Y")

                        if filesize < int(data["event-size"]) or file_timestamp<event_timestamp:
                            return False



            if  data["event-type"]=="hdfs-dir":
                if not "event-dir" in data.keys():
                    print "event-type is hdfs-dir but event-dir is not specified"
                    return False
                try:
                    subprocess.check_output(["hdfs","dfs","-test","-e",data["event-dir"]])
                except:
                    print "Check if HDFS is running or if the event-dir exists in HDFS"
                    return False

                if not "event-size" in data.keys():
                    print "event-size is necessary"
                    return False

                if not "pegasus-args" in data.keys():
                    print "pegasus-args not provided"
                    return False

                if not "event-script" in data.keys():
                    print "event-script not provided"
                    return False

                if not "event-dax-dir" in data.keys():
                    print "event-dax-dir not provided"
                    return False

                if not os.path.exists(data["event-dax-dir"]):
                    print "Path to event-dax-dir does not exist"
                    return False

                self.daxdir[e.name] = data["event-dax-dir"]

                if not os.path.exists(data["event-script"]):
                    print "event-script path does not exists"
                    return False

                self.script[e.name] = data["event-script"]

                if not "event-content" in data.keys():
                    return True

                if data["event-content"]=="*":

                    data["event-cycle"] = int(data["event-cycle"])
                    self.pegasus_args[e.name] = data["pegasus-args"]

                    if not "event-numfiles" in data.keys():
                        print "event-numfiles is necessary"
                        return False


                    # greater_than_cycle = [i for i in data["event-cycle"] if i>=workflow.get_event_cycle()]
                    # workflow.set_event_maxcycle(greater_than_cycle[len(greater_than_cycle)-1])

                    #os.system("date +%x_%H:%M:%S:%N")
                    event_timestamp = self.timestamp[e.name]
                    print "Workflow timestamp",event_timestamp
                    if event_timestamp == None:
                        print "Workflow timestamp not set yet"
                        return False
                        #workflow_timestamp = os.system("echo date")

                        # print "Afterwards",workflow_timestamp,workflow.get_event_timestamp()
                    #num_files = os.system("find '"+data["event-dir"]+"' -size +"+str(data["event-size"])+"c -newermt '"+str(workflow_timestamp)+"' -exec /bin/echo {} \; | wc -l")
                    ps = subprocess.Popen(["hdfs", "dfs", "-ls", data["event-dir"]], stdout=subprocess.PIPE)
                    dates = subprocess.check_output(["awk","{print $6}"],stdin=ps.stdout)
                    ps = subprocess.Popen(["hdfs", "dfs", "-ls", data["event-dir"]], stdout=subprocess.PIPE)
                    times = subprocess.check_output(["awk","{print $7}"],stdin=ps.stdout)
                    ps = subprocess.Popen(["hdfs", "dfs", "-ls", data["event-dir"]], stdout=subprocess.PIPE)
                    sizes = subprocess.check_output(["awk","{print $5}"],stdin=ps.stdout)

                    dates = dates.split("\n")
                    times = times.split("\n")
                    sizes = sizes.split("\n")
                    dates = dates[1:len(dates)-1]
                    times = times[1:len(times)-1]
                    sizes = sizes[1:len(sizes)-1]
                    event_time = datetime.strptime(event_timestamp,"%a %b %d %H:%M:%S %Z %Y")
                    print dates,times,sizes,event_time
                    num_files = 0
                    for i in xrange(len(times)):
                        t1 = datetime.strptime(dates[i]+" "+times[i],"%Y-%m-%d %H:%M")
                        if t1>=event_time and float(sizes[i])>float(data["event-size"]):
                            num_files += 1


                    #num_files = files.count("\n")-1
                    print dates,times,sizes
                    print num_files, data["event-dir"],data["event-size"],event_timestamp,data["event-numfiles"],event_time
                    if (num_files) < data["event-numfiles"]:
                        return False


                else:
                    data["event-cycle"] = int(data["event-cycle"])
                    self.pegasus_args[e.name] = data["pegasus-args"]
                    # greater_than_cycle = [i for i in data["event-cycle"] if i>=workflow.get_event_cycle()]
                    # idx_of_event_content = data["event-cycle"].index(greater_than_cycle[0])
                    # workflow.set_event_maxcycle(greater_than_cycle[len(greater_than_cycle)-1])
                    #self.maxcycle = greater_than_cycle[len(greater_than_cycle)-1]
                    # print "index",idx_of_event_content
                    # it = 0
                    # idx = workflow.get_event_cycle()
                    data["event-dir"] = data["event-dir"].strip('/')
                    if len(data["event-dir"])>0:
                        data["event-dir"] += '/'
                    event_timestamp = self.timestamp[e.name]
                    #print "content",data["event-content"]
                    for content in data["event-content"]:
                        # print "content",content, it, idx, int(data["event-size"][idx-1][it])
                        try:
                            # subprocess.check_output(["hdfs", "dfs", "-test", "-f", data["event-dir"]+"/"+content])
                            #print "Before", "/"+data["event-dir"]+content
                            exists = os.system("hdfs dfs -test -e /"+data["event-dir"]+content)
                            print "Exists", exists
                            if not exists is 0:
                                return False
                        except:
                            return False


                        print "/"+data["event-dir"]+content
                        ps = subprocess.Popen(["hdfs", "dfs", "-ls", "/"+data["event-dir"]+content], stdout=subprocess.PIPE)
                        dates = subprocess.check_output(["awk", "{print $6}"], stdin=ps.stdout)
                        ps = subprocess.Popen(["hdfs", "dfs", "-ls", "/"+data["event-dir"]+content], stdout=subprocess.PIPE)
                        times = subprocess.check_output(["awk", "{print $7}"], stdin=ps.stdout)
                        ps = subprocess.Popen(["hdfs", "dfs", "-ls", "/"+data["event-dir"]+content], stdout=subprocess.PIPE)
                        sizes = subprocess.check_output(["awk", "{print $5}"], stdin=ps.stdout)

                        dates = dates.split("\n")
                        times = times.split("\n")
                        sizes = sizes.split("\n")
                        dates = dates[0:len(dates) - 1]
                        times = times[0:len(times) - 1]
                        sizes = sizes[0:len(sizes) - 1]

                        if(len(dates)<0 or len(times)<0 or len(sizes)<0):
                            return False

                        event_time = datetime.strptime(event_timestamp, "%a %b %d %H:%M:%S %Z %Y")
                        for i in xrange(len(times)):
                            t1 = datetime.strptime(dates[i] + " " + times[i], "%Y-%m-%d %H:%M")
                            print t1,event_time
                            if (t1 < event_time) or (float(sizes[i]) < float(data["event-size"])):
                                return False


        return True
