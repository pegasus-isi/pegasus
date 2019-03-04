from __future__ import print_function
import sys
import os
import pwd
import shutil, errno
from jinja2 import Environment, FileSystemLoader


class TutorialEnv:
    LOCAL_MACHINE = ("Local Machine Condor Pool", "submit-host")
    USC_HPCC_CLUSTER = ("USC HPCC Cluster", "usc-hpcc")
    OSG_FROM_ISI = ("OSG from ISI submit node", "osg")
    XSEDE_BOSCO = ("XSEDE, with Bosco", "xsede-bosco")
    BLUEWATERS_GLITE = ("Bluewaters, with Glite", "bw-glite")
    TACC_WRANGLER = ("TACC Wrangler with Glite", "wrangler-glite")
    OLCF_TITAN = ("OLCF TITAN with Glite", "titan-glite")


class TutorialExample:
    PROCESS = ("Process", "process")
    PIPELINE = ("Pipeline", "pipeline")
    SPLIT = ("Split", "split")
    MERGE = ("Merge", "merge")
    EPA = ("EPA (requires R)", "r-epa")
    DIAMOND = ("Diamond", "diamond")
    CONTAINER = ("Population Modeling using Containers", "population")
    MPI = ("MPI Hello World", "mpi-hw")


def choice(question, options, default):
    "Ask the user to choose from a short list of named options"
    while True:
        sys.stdout.write("%s (%s) [%s]: " % (question, "/".join(options), default))
        answer = sys.stdin.readline().strip()
        if len(answer) == 0:
            return default
        for opt in options:
            if answer == opt:
                return answer


def yesno(question, default="y"):
    "Ask the user a yes/no question"
    while True:
        sys.stdout.write("%s (y/n) [%s]: " % (question, default))
        answer = sys.stdin.readline().strip().lower()
        if len(answer) == 0:
            answer = default
        if answer == 'y':
            return True
        elif answer == 'n':
            return False


def query(question, default=None):
    "Ask the user a question and return the response"
    while True:
        if default:
            sys.stdout.write("%s [%s]: " % (question, default))
        else:
            sys.stdout.write("%s: " % question)
        answer = sys.stdin.readline().strip().replace(' ', '_')
        if answer == "":
            if default:
                return default
        else:
            return answer


def optionlist(question, options, default=0):
    "Ask the user to choose from a list of options"
    for i, option in enumerate(options):
        print("%d: %s" % (i + 1, option[0]))
    while True:
        sys.stdout.write("%s (1-%d) [%d]: " % (question, len(options), default + 1))
        answer = sys.stdin.readline().strip()
        if len(answer) == 0:
            return options[default][1]
        try:
            optno = int(answer)
            if optno > 0 and optno <= len(options):
                return options[optno - 1][1]
        except:
            pass


class Workflow(object):
    def __init__(self, workflowdir, sharedir):
        self.jinja = Environment(loader=FileSystemLoader(sharedir), trim_blocks=True)
        self.name = os.path.basename(workflowdir)
        self.workflowdir = workflowdir
        self.sharedir = sharedir
        self.properties = {}
        self.home = os.environ["HOME"]
        self.user = pwd.getpwuid(os.getuid())[0]
        self.generate_tutorial = False
        self.tutorial_setup = None
        self.compute_queue = "default"
        self.project = "MYPROJ123"
        sysname, _, _, _, machine = os.uname()
        if sysname == 'Darwin':
            self.os = "MACOSX"
        else:
            # Probably Linux
            self.os = sysname.upper()
        self.arch = machine

    def copy_template(self, template, dest, mode=0o644):
        "Copy template to dest in workflowdir with mode"
        path = os.path.join(self.workflowdir, dest)
        t = self.jinja.get_template(template)
        t.stream(**self.__dict__).dump(path)
        os.chmod(path, mode)

    def copy_dir(self, src, dest):
        #self.mkdir(dest)
        if not src.startswith("/"):
            src = os.path.join(self.sharedir,src)
        try:
            dest = os.path.join(self.workflowdir, dest)
            shutil.copytree(src, dest)
        except OSError as exc:  # python >2.5
            if exc.errno == errno.ENOTDIR:
                shutil.copy(src, dest)
            else:
                raise

    def mkdir(self, path):
        "Make relative directory in workflowdir"
        path = os.path.join(self.workflowdir, path)
        if not os.path.exists(path):
            os.makedirs(path)

    def configure(self):
        # The tutorial is a special case
        if yesno("Do you want to generate a tutorial workflow?", "n"):
            self.config = "tutorial"
            self.daxgen = "tutorial"
            self.generate_tutorial = True

            # determine the environment to setup tutorial for
            self.tutorial_setup = optionlist("What environment is tutorial to be setup for?", [
                TutorialEnv.LOCAL_MACHINE,
                TutorialEnv.USC_HPCC_CLUSTER,
                TutorialEnv.OSG_FROM_ISI,
                TutorialEnv.XSEDE_BOSCO,
                TutorialEnv.BLUEWATERS_GLITE,
                TutorialEnv.TACC_WRANGLER,
                TutorialEnv.OLCF_TITAN
            ])

            # figure out what example options to provide
            examples = [
                TutorialExample.PROCESS,
                TutorialExample.PIPELINE,
                TutorialExample.SPLIT,
                TutorialExample.MERGE,
                TutorialExample.EPA,
                TutorialExample.CONTAINER,
            ]
            if self.tutorial_setup != "osg":
                examples.append(TutorialExample.DIAMOND)

            if self.tutorial_setup in ["bw-glite", "wrangler-glite", "titan-glite"]:
                examples.append(TutorialExample.MPI)
                self.project = query("What project your jobs should run under. For example on TACC there are like : TG-DDM160003 ?")

            self.tutorial = optionlist("What tutorial workflow do you want?", examples)

            self.setup_tutorial()
            return

        # Determine which DAX generator API to use
        self.daxgen = choice("What DAX generator API do you want to use?", ["python", "perl", "java", "r"], "python")

        # Determine what kind of site catalog we need to generate
        self.config = optionlist("What does your computing infrastructure look like?", [
            ("Local Machine Condor Pool", "condorpool"),
            ("Remote Cluster using Globus GRAM", "globus"),
            ("Remote Cluster using CREAMCE", "creamce"),
            ("Local PBS Cluster with Glite", "glite"),
            ("Remote PBS Cluster with BOSCO and SSH", "bosco")
        ])

        # Find out some information about the site
        self.sitename = query("What do you want to call your compute site?", "compute")
        self.os = choice("What OS does your compute site have?", ["LINUX", "MACOSX"], self.os)
        self.arch = choice("What architecture does your compute site have?", ["x86_64", "x86"], self.arch)

    def setup_tutorial(self):
        """
        Set up tutorial for pre-defined computing environments
        :return:
        """

        if self.tutorial_setup is None:
            self.tutorial_setup = "submit-host"

        if self.tutorial_setup == "submit-host":
            self.sitename = "condorpool"
        elif self.tutorial_setup == "usc-hpcc":
            self.sitename = "usc-hpcc"
            self.config = "glite"
            self.compute_queue = "quick"
            # for running the whole workflow as mpi job
            self.properties["pegasus.job.aggregator"] = "mpiexec"
        elif self.tutorial_setup == "osg":
            self.sitename = "osg"
            self.os = "linux"
            if not yesno("Do you want to use Condor file transfers", "y"):
                self.staging_site = "isi_workflow"
        elif self.tutorial_setup == "xsede-bosco":
            self.sitename = "condorpool"
        elif self.tutorial_setup == "bw-glite":
            self.sitename = "bluewaters"
            self.config = "glite"
            self.compute_queue = "normal"
        elif self.tutorial_setup == "wrangler-glite":
            self.sitename = "wrangler"
            self.config = "glite"
            self.compute_queue = "normal"
        elif self.tutorial_setup == "titan-glite":
            self.sitename = "titan"
            self.config = "glite"
            self.compute_queue = "titan"
        return

    def generate(self):
        os.makedirs(self.workflowdir)
        if self.tutorial != "population":
            self.mkdir("input")
        self.mkdir("output")

        if self.generate_tutorial:
            self.copy_template("%s/tc.txt" % self.tutorial, "tc.txt")
            if self.tutorial == "r-epa":
                self.copy_template("%s/daxgen.R" % self.tutorial, "daxgen.R")
            else:
                self.copy_template("%s/daxgen.py" % self.tutorial, "daxgen.py")

            if self.tutorial == "diamond":

                # Executables used by the diamond workflow
                self.mkdir("bin")
                self.copy_template("diamond/transformation.py", "bin/preprocess", mode=0o755)
                self.copy_template("diamond/transformation.py", "bin/findrange", mode=0o755)
                self.copy_template("diamond/transformation.py", "bin/analyze", mode=0o755)

                # Diamond input file
                self.copy_template("diamond/f.a", "input/f.a")
            elif self.tutorial == "split":
                # Split workflow input file
                self.mkdir("bin")
                self.copy_template("split/pegasus.html", "input/pegasus.html")
            elif self.tutorial == "r-epa":
                # Executables used by the R-EPA workflow
                self.mkdir("bin")
                self.copy_template("r-epa/epa-wrapper.sh", "bin/epa-wrapper.sh", mode=0o755)
                self.copy_template("r-epa/setupvar.R", "bin/setupvar.R", mode=0o755)
                self.copy_template("r-epa/weighted.average.R", "bin/weighted.average.R", mode=0o755)
                self.copy_template("r-epa/cumulative.percentiles.R", "bin/cumulative.percentiles.R", mode=0o755)
            elif self.tutorial == "population":
                self.copy_template("%s/Dockerfile" % self.tutorial, "Dockerfile")
                self.copy_template("%s/Singularity" % self.tutorial, "Singularity")
                self.copy_template("%s/tc.txt.containers" % self.tutorial, "tc.txt.containers")
                self.copy_dir("%s/scripts" % self.tutorial, "scripts")
                self.copy_dir("%s/data" % self.tutorial, "input")
                # copy the mpi wrapper, c code and mpi
            elif self.tutorial == "mpi-hw":
                # copy the mpi wrapper, c code and mpi example
                # Executables used by the mpi-hw workflow
                self.mkdir("bin")
                self.copy_template("%s/pegasus-mpi-hw.c" % self.tutorial, "pegasus-mpi-hw.c")
                self.copy_template("%s/Makefile" % self.tutorial, "Makefile")
                self.copy_template("%s/mpi-hello-world-wrapper" % self.tutorial, "bin/mpi-hello-world-wrapper",
                                   mode=0o755)
                self.copy_template("split/pegasus.html", "input/f.in")
        else:
            self.copy_template("tc.txt", "tc.txt")
            if self.daxgen == "python":
                self.copy_template("daxgen/daxgen.py", "daxgen.py")
            elif self.daxgen == "perl":
                self.copy_template("daxgen/daxgen.pl", "daxgen.pl")
            elif self.daxgen == "java":
                self.copy_template("daxgen/DAXGen.java", "DAXGen.java")
            elif self.daxgen == "r":
                self.copy_template("daxgen/daxgen.R", "daxgen.R")
            else:
                assert False

        self.copy_template("sites.xml", "sites.xml")
        self.copy_template("plan_dax.sh", "plan_dax.sh", mode=0o755)
        self.copy_template("plan_cluster_dax.sh", "plan_cluster_dax.sh", mode=0o755)
        self.copy_template("generate_dax.sh", "generate_dax.sh", mode=0o755)
        self.copy_template("README.md", "README.md")
        self.copy_template("rc.txt", "rc.txt")
        self.copy_template("pegasus.properties", "pegasus.properties")

        if self.tutorial_setup == "wrangler-glite":
            self.copy_template("pmc-wrapper.wrangler", "bin/pmc-wrapper", mode=0o755)
        elif self.tutorial_setup == "titan-glite":
            self.copy_template("pmc-wrapper.titan", "bin/pmc-wrapper", mode=0o755)

        if self.generate_tutorial:
            sys.stdout.write("Pegasus Tutorial setup for example workflow - %s for execution on %s in directory %s\n"
                             % (self.tutorial, self.tutorial_setup, self.workflowdir))


def usage():
    print("Usage: %s WORKFLOW_DIR" % sys.argv[0])


def main(pegasus_share_dir):
    if len(sys.argv) != 2:
        usage()
        exit(1)

    if "-h" in sys.argv:
        usage()
        exit(1)

    workflowdir = sys.argv[1]
    if os.path.exists(workflowdir):
        print("ERROR: WORKFLOW_DIR '%s' already exists" % workflowdir)
        exit(1)

    workflowdir = os.path.abspath(workflowdir)
    sharedir = os.path.join(pegasus_share_dir, "init")
    w = Workflow(workflowdir, sharedir)
    w.configure()
    w.generate()
