from Pegasus.service.command import ClientCommand, CompoundCommand
from Pegasus.service.ensembles.models import EnsembleStates, EnsembleWorkflowStates

def add_ensemble_option(self):
    self.parser.add_option("-e", "--ensemble", action="store", dest="ensemble",
        default=None, help="Ensemble name")

def add_workflow_option(self):
    self.parser.add_option("-w", "--workflow", action="store", dest="workflow",
        default=None, help="Workflow name")

class EnsemblesCommand(ClientCommand):
    description = "List ensembles"
    usage = "Usage: %prog ensembles"

    def run(self):
        response = self.get("/ensembles")
        result = response.json()

        if response.status_code != 200:
            print "ERROR:",result["message"]
            exit(1)

        fmt = "%-20s %-8s %-30s %-30s %14s %12s"
        if len(result) > 0:
            print fmt % ("NAME","STATE","CREATED","UPDATED","MAX PLANNING","MAX RUNNING")
        for r in result:
            print fmt % (r["name"], r["state"], r["created"], r["updated"], r["max_planning"], r["max_running"])

class CreateCommand(ClientCommand):
    description = "Create ensemble"
    usage = "Usage: %prog create [options] -e ENSEMBLE"

    def __init__(self):
        ClientCommand.__init__(self)
        add_ensemble_option(self)
        self.parser.add_option("-P", "--max-planning", action="store", dest="max_planning",
            default=1, type="int", help="Maximum number of workflows being planned at once")
        self.parser.add_option("-R", "--max-running", action="store", dest="max_running",
            default=1, type="int", help="Maximum number of workflows running at once")

    def run(self):
        if self.options.ensemble is None:
            self.parser.error("Specify -e/--ensemble")

        request = {
            "name": self.options.ensemble,
            "max_planning": self.options.max_planning,
            "max_running": self.options.max_running
        }

        response = self.post("/ensembles", data=request)

        if response.status_code != 201:
            result = response.json()
            print "ERROR:", result["message"]
            exit(1)

class SubmitCommand(ClientCommand):
    description = "Submit workflow"
    usage = "Usage: %prog submit [options] -e ENSEMBLE -w WORKFLOW -d DAX -T TC -S SC -R RC -s SITE -o SITE"

    def __init__(self):
        ClientCommand.__init__(self)
        add_ensemble_option(self)
        add_workflow_option(self)
        self.parser.add_option("-d", "--dax", action="store", dest="dax",
            default=None, help="DAX file", metavar="PATH")
        self.parser.add_option("-T", "--transformation-catalog", action="store", dest="transformation_catalog",
            default=None, help="Name of transformation catalog", metavar="NAME")
        self.parser.add_option("-S", "--site-catalog", action="store", dest="site_catalog",
            default=None, help="Name of site catalog", metavar="NAME")
        self.parser.add_option("-R", "--replica-catalog", action="store", dest="replica_catalog",
            default=None, help="Name of replica catalog", metavar="NAME")
        self.parser.add_option("-s", "--site", action="store", dest="sites",
            default=None, help="Execution sites (see pegasus-plan man page)", metavar="SITE[,SITE...]")
        self.parser.add_option("-o", "--output-site", action="store", dest="output_site",
            default=None, help="Output storage site (see pegasus-plan man page)", metavar="SITE")

        self.parser.add_option("-p", "--priority", action="store", dest="priority",
            default=0, help="Workflow priority", metavar="NUMBER")
        self.parser.add_option("-c", "--conf", action="store", dest="conf",
            default=None, help="Configuration file (pegasus properties file)", metavar="PATH")
        self.parser.add_option("--staging-site", action="store", dest="staging_sites",
            default=None, help="Staging sites (see pegasus-plan man page)", metavar="s=ss[,s=ss...]")
        self.parser.add_option("--nocleanup", action="store_false", dest="cleanup",
            default=None, help="Add cleanup jobs (see pegasus-plan man page)")
        self.parser.add_option("-f", "--force", action="store_true", dest="force",
            default=None, help="Skip workflow reduction (see pegasus-plan man page)")
        self.parser.add_option("-C", "--cluster", action="store", dest="clustering",
            default=None, help="Clustering techniques to apply (see pegasus-plan man page)", metavar="STYLE[,STYLE...]")

    def run(self):
        o = self.options
        p = self.parser

        if o.ensemble is None:
            p.error("Specify -e/--ensemble")
        if o.workflow is None:
            p.error("Specify -w/--workflow")
        if o.dax is None:
            p.error("Specify -d/--dax")
        if o.transformation_catalog is None:
            p.error("Specify -T/--transformation-catalog")
        if o.site_catalog is None:
            p.error("Specify -S/--site-catalog")
        if o.replica_catalog is None:
            p.error("Specify -R/--replica-catalog")
        if o.sites is None:
            p.error("Specify -s/--site")
        if o.output_site is None:
            p.error("Specify -o/--output-site")

        data = {
            "name": o.workflow,
            "transformation_catalog": o.transformation_catalog,
            "site_catalog": o.site_catalog,
            "replica_catalog": o.replica_catalog,
            "priority": o.priority,
            "sites": o.sites,
            "output_site": o.output_site
        }

        if o.cleanup is not None:
            data["cleanup"] = o.cleanup

        if o.force is not None:
            data["force"] = o.force

        if o.staging_sites is not None:
            data["staging_sites"] = o.staging_sites

        if o.clustering is not None:
            data["clustering"] = [s.strip() for s in o.clustering.split(",")]

        files = {
            "dax": open(o.dax, "rb")
        }

        if o.conf is not None:
            files["conf"] = open(o.conf, "rb")

        response = self.post("/ensembles/%s/workflows" % o.ensemble, data=data, files=files)

        if response.status_code != 201:
            result = response.json()
            print "ERROR:",response.status_code,result["message"]

class WorkflowsCommand(ClientCommand):
    description = "List workflows in ensemble"
    usage = "Usage: %prog workflows [options] -e ENSEMBLE."

    def __init__(self):
        ClientCommand.__init__(self)
        add_ensemble_option(self)
        self.parser.add_option("-l", "--long", action="store_true", dest="long",
            default=False, help="Show detailed output")

    def run(self):
        if self.options.ensemble is None:
            self.parser.error("Specify -e/--ensemble")
        if len(self.args) > 0:
            self.parser.error("Invalid argument")

        response = self.get("/ensembles/%s/workflows" % self.options.ensemble)

        result = response.json()

        if response.status_code != 200:
            print "ERROR:",response.status_code,result["message"]
            exit(1)

        if len(result) == 0:
            return

        if self.options.long:
            for w in result:
                print "ID:      ",w["id"]
                print "Name:    ",w["name"]
                print "Created: ",w["created"]
                print "Updated: ",w["updated"]
                print "State:   ",w["state"]
                print "Priority:",w["priority"]
                print "UUID:    ",w["wf_uuid"]
                print "URL:     ",w["href"]
                print
        else:
            fmt = "%-20s %-15s %-8s %-30s %-30s"
            print fmt % ("NAME","STATE","PRIORITY","CREATED","UPDATED")
            for w in result:
                print fmt % (w["name"],w["state"],w["priority"],w["created"],w["updated"])

class StateChangeCommand(ClientCommand):
    def __init__(self):
        ClientCommand.__init__(self)
        add_ensemble_option(self)

    def run(self):
        if self.options.ensemble is None:
            self.parser.error("Specify -e/--ensemble")

        response = self.post("/ensembles/%s" % self.options.ensemble, data={"state":self.newstate})
        result = response.json()

        if response.status_code != 200:
            print "ERROR:",result["message"]

        print "State:", result["state"]

class PauseCommand(StateChangeCommand):
    description = "Pause active ensemble"
    usage = "Usage: %prog pause -e ENSEMBLE"
    newstate = EnsembleStates.PAUSED

class ActivateCommand(StateChangeCommand):
    description = "Activate paused or held ensemble"
    usage = "Usage: %prog activate -e ENSEMBLE"
    newstate = EnsembleStates.ACTIVE

class HoldCommand(StateChangeCommand):
    description = "Hold active ensemble"
    usage = "Usage: %prog hold -e ENSEMBLE"
    newstate = EnsembleStates.HELD

class ConfigCommand(ClientCommand):
    description = "Change ensemble configuration"
    usage = "Usage: %prog config [options] -e ENSEMBLE"

    def __init__(self):
        ClientCommand.__init__(self)
        add_ensemble_option(self)
        self.parser.add_option("-P", "--max-planning", action="store", dest="max_planning",
            default=None, type="int", help="Maximum number of workflows being planned at once")
        self.parser.add_option("-R", "--max-running", action="store", dest="max_running",
            default=None, type="int", help="Maximum number of workflows running at once")

    def run(self):
        if self.options.ensemble is None:
            self.parser.error("Specify -e/--ensemble")

        request = {}

        if self.options.max_planning:
            request["max_planning"] = self.options.max_planning
        if self.options.max_running:
            request["max_running"] = self.options.max_running

        if len(request) == 0:
            self.parser.error("Specify --max-planning or --max-running")

        response = self.post("/ensembles/%s" % self.options.ensemble, data=request)

        result = response.json()

        if response.status_code != 200:
            print "ERROR:", result["message"]
            exit(1)

        print "Max Planning:",result["max_planning"]
        print "Max Running:",result["max_running"]

class WorkflowStateChangeCommand(ClientCommand):
    def __init__(self):
        ClientCommand.__init__(self)
        add_ensemble_option(self)
        add_workflow_option(self)

    def run(self):
        if self.options.ensemble is None:
            self.parser.error("Specify -e/--ensemble")
        if self.options.workflow is None:
            self.parser.error("Specify -w/--workflow")

        if len(self.args) > 0:
            self.parser.error("Invalid argument")

        request = {"state": self.newstate}

        response = self.post("/ensembles/%s/workflows/%s" % (self.options.ensemble, self.options.workflow), data=request)

        result = response.json()

        if response.status_code != 200:
            print "ERROR:", result["message"]
            exit(1)

        print "State:", result["state"]

class ReplanCommand(WorkflowStateChangeCommand):
    description = "Replan failed workflow"
    usage = "Usage: %prog replan -e ENSEMBLE -w WORKFLOW"
    newstate = EnsembleWorkflowStates.READY

class RerunCommand(WorkflowStateChangeCommand):
    description = "Rerun failed workflow"
    usage = "Usage: %prog rerun -e ENSEMBLE -w WORKFLOW"
    newstate = EnsembleWorkflowStates.QUEUED

class AbortCommand(WorkflowStateChangeCommand):
    description = "Abort workflow"
    usage = "Usage: %prog abort -e ENSEMBLE -w WORKFLOW"
    newstate = EnsembleWorkflowStates.ABORTED

class PriorityCommand(ClientCommand):
    description = "Update workflow priority"
    usage = "Usage: %prog priority -e ENSEMBLE -w WORKFLOW -p PRIORITY"

    def __init__(self):
        ClientCommand.__init__(self)
        add_ensemble_option(self)
        add_workflow_option(self)
        self.parser.add_option("-p","--priority",action="store",dest="priority",
                default=None,type="int",help="New workflow priority")

    def run(self):
        if self.options.ensemble is None:
            self.parser.error("Specify -e/--ensemble")
        if self.options.workflow is None:
            self.parser.error("Specify -w/--workflow")
        if self.options.priority is None:
            self.parser.error("Specify -p/--priority")

        if len(self.args) > 0:
            self.parser.error("Invalid argument")

        request = {"priority": self.options.priority}

        response = self.post("/ensembles/%s/workflows/%s" % (self.options.ensemble, self.options.workflow), data=request)

        result = response.json()

        if response.status_code != 200:
            print "ERROR:", result["message"]
            exit(1)

        print "Priority:", result["priority"]

class EnsembleCommand(CompoundCommand):
    description = "Client for ensemble management"
    commands = [
        ("ensembles", EnsemblesCommand),
        ("create", CreateCommand),
        ("pause", PauseCommand),
        ("activate", ActivateCommand),
        ("config", ConfigCommand),
        ("submit", SubmitCommand),
        ("workflows", WorkflowsCommand),
        ("replan", ReplanCommand),
        ("rerun", RerunCommand),
        ("priority", PriorityCommand)
    ]

def main():
    "The entry point for pegasus-service-ensemble"
    EnsembleCommand().main()

