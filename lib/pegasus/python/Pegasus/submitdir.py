import os
import glob
import tarfile
import shutil
import logging
import getpass
from optparse import OptionParser


from Pegasus import db, user
from Pegasus.tools import utils
from Pegasus.service.command import Command, CompoundCommand
from Pegasus.db.schema.pegasus_schema import *

log = logging.getLogger(__name__)

class SubmitDirException(Exception): pass

class Database:
    def __init__(self, connString):
        self.session = db.connect(connString)

    def commit(self):
        self.session.flush()
        self.session.commit()

    def close(self):
        self.session.close()

    def get_master_workflow(self, wf_uuid):
        q = self.session.query(DashboardWorkflow)
        q = q.filter(DashboardWorkflow.wf_uuid == wf_uuid)
        return q.first()

    def get_ensemble_workflow(self, wf_uuid):
        q = self.session.query(EnsembleWorkflow)
        q = q.filter(EnsembleWorkflow.wf_uuid == wf_uuid)
        return q.first()

    def delete_master_workflow(self, wf_uuid):
        w = self.get_master_workflow(wf_uuid)
        if w is None:
            return

        # Delete any ensemble workflows
        q = self.session.query(EnsembleWorkflow)
        q = q.filter(EnsembleWorkflow.wf_uuid == wf_uuid)
        q.delete()

        # Delete the workflow
        q = self.session.query(DashboardWorkflow)
        q = q.filter(DashboardWorkflow.wf_id == w.wf_id)
        q.delete()

    def get_workflow(self, wf_uuid):
        q = self.session.query(Workflow)
        q = q.filter(Workflow.wf_uuid == wf_uuid)
        return q.first()

    def update_submit_dirs(self, root_wf_id, src, dest):
        q = self.session.query(Workflow)
        q = q.filter(Workflow.root_wf_id == root_wf_id)
        for wf in q.all():
            log.info("Old submit dir: %s" % wf.submit_dir)
            wf.submit_dir = wf.submit_dir.replace(src, dest)
            log.info("New submit dir: %s" % wf.submit_dir)

def extract(submitdir):
    if not os.path.isdir(submitdir):
        raise SubmitDirException("Invalid submit dir: %s" % submitdir)

    submitdir = os.path.abspath(submitdir)

    # Locate braindump file
    braindump = os.path.join(submitdir, "braindump.txt")
    if not os.path.isfile(braindump):
        raise SubmitDirException("Not a submit directory: braindump.txt missing")

    # Locate archive file
    archname = os.path.join(submitdir, "archive.tar.gz")
    if not os.path.isfile(archname):
        raise SubmitDirException("Submit dir not archived")

    tar = tarfile.open(archname, "r:gz")
    tar.extractall(path=submitdir)
    tar.close()

    os.remove(archname)

def archive(submitdir):
    if not os.path.isdir(submitdir):
        raise SubmitDirException("Invalid submit dir: %s" % submitdir)

    submitdir = os.path.abspath(submitdir)

    exclude = set()

    # Locate and exclude braindump file
    braindump = os.path.join(submitdir, "braindump.txt")
    if not os.path.isfile(braindump):
        raise SubmitDirException("Not a submit directory: braindump.txt missing")
    exclude.add(braindump)

    # Ignore monitord files. This is needed so that tools like pegasus-statistics
    # will consider the workflow to be complete
    for name in ["monitord.started", "monitord.done", "monitord.log"]:
        exclude.add(os.path.join(submitdir, name))

    # Exclude stampede db
    for db in glob.glob(os.path.join(submitdir, "*.stampede.db")):
        exclude.add(db)

    # Exclude properties file
    for prop in glob.glob(os.path.join(submitdir, "pegasus.*.properties")):
        exclude.add(prop)

    # Locate and exclude archive file
    archname = os.path.join(submitdir, "archive.tar.gz")
    if os.path.exists(archname):
        raise SubmitDirException("Submit dir already archived")
    exclude.add(archname)

    # Visit all the files in the submit dir that we want to archive
    def visit(submitdir):
        for name in os.listdir(submitdir):
            path = os.path.join(submitdir, name)

            if path not in exclude:
                yield name, path

    # Archive the files
    tar = tarfile.open(name=archname, mode="w:gz")
    for name, path in visit(submitdir):
        tar.add(name=path, arcname=name)
    tar.close()

    # Remove the files and directories
    # We do this here, instead of doing it in the loop above
    # because we want to make sure there are no errors in creating
    # the archive before we start removing files
    for name, path in visit(submitdir):
        if os.path.isfile(path) or os.path.islink(path):
            os.remove(path)
        else:
            shutil.rmtree(path)

def move(submitdir, dest):
    submitdir = os.path.abspath(submitdir)
    dest = os.path.abspath(dest)

    if not os.path.isdir(submitdir):
        raise SubmitDirException("Invalid submit dir: %s" % submitdir)

    if os.path.isfile(dest):
        raise SubmitDirException("Destination is a file: %s" % dest)

    if os.path.isdir(dest):
        if os.path.exists(os.path.join(dest, "braindump.txt")):
            raise SubmitDirException("Destination is a submit dir: %s" % dest)
        dest = os.path.join(dest, os.path.basename(submitdir))

    bd = os.path.join(submitdir, "braindump.txt")
    if not os.path.isfile(bd):
        raise SubmitDirException("Not a submit dir (no braindump.txt): %s" % submitdir)

    # Read the braindump file
    items = utils.read_braindump(bd)
    wf_uuid = items["wf_uuid"]
    root_wf_uuid = items["root_wf_uuid"]

    # Verify that we aren't trying to move a subworkflow
    if wf_uuid != root_wf_uuid:
        raise SubmitDirException("Subworkflows cannot be moved independent of the root workflow")

    # Confirm that the username matches
    wf_user = items["user"]
    os_user = getpass.getuser()
    if wf_user != os_user:
        raise SubmitDirException("Workflow username from braindump does not match current username: %s != %s" % (wf_user, os_user))

    # Find the master db
    u = user.get_user_by_username(wf_user)
    mdb_file = u.get_master_db()
    if not os.path.isfile(mdb_file):
        raise SubmitDirException("Master database does not exist: %s" % mdb_file)

    # Connect to master database
    mdb_url = u.get_master_db_url()
    mdb = Database(mdb_url)

    # Get the workflow record from the master db
    db_url = None
    wf = mdb.get_master_workflow(wf_uuid)
    if wf is None:
        # No mdb record found to update
        log.warning("No master db record found for this workflow: %s" % wf_uuid)

        # Try looking in the workflow directory
        pegasus_wf_name = items["pegasus_wf_name"]
        db_file = os.path.join(submitdir, pegasus_wf_name + ".stampede.db")
        if os.path.isfile(db_file):
            log.info("Found workflow db in submit dir: %s" % db_file)
            db_url = "sqlite:///%s" % db_file
        else:
            # TODO Try looking at the pegasus properties configuration
            log.warning("Unable to locate workflow database")
            db_url = None
    else:
        # We found an mdb record, so we need to update it

        # Save the master db's pointer
        db_url = wf.db_url

        # Update the master db's db_url
        # Note that this will only update the URL if it is an sqlite file
        # located in the submitdir
        log.info("Old master db_url: %s" % wf.db_url)
        wf.db_url = db_url.replace(submitdir, dest)
        log.info("New master db_url: %s" % wf.db_url)

        # Change the master db's submit_dir
        log.info("Old master submit_dir: %s" % wf.submit_dir)
        wf.submit_dir = dest
        log.info("New master submit_dir: %s" % wf.submit_dir)

    # Update the ensemble record if one exists
    ew = mdb.get_ensemble_workflow(wf_uuid)
    if ew is not None:
        log.info("Old ensemble submit dir: %s", ew.submitdir)
        ew.submitdir = dest
        log.info("New ensemble submit dir: %s", ew.submitdir)

    # Update the workflow database if we found one
    if db_url is not None:
        db = Database(db_url)
        root_wf = db.get_workflow(wf_uuid)
        db.update_submit_dirs(root_wf.wf_id, submitdir, dest)
        db.commit()
        db.close()

    # Move all the files
    shutil.move(submitdir, dest)

    # Set new paths in the braindump file
    items["submit_dir"] = dest
    items["basedir"] = os.path.dirname(dest)
    utils.write_braindump(os.path.join(dest, "braindump.txt"), items)

    # TODO We might want to update all of the absolute paths in the condor submit files
    # if we plan on moving workflows that could be resubmitted in the future

    # TODO We might want to update the braindump files for subworkflows

    # Update master database
    mdb.commit()
    mdb.close()

def delete(submitdir):
    submitdir = os.path.abspath(submitdir)

    if not os.path.isdir(submitdir):
        raise SubmitDirException("Invalid submit dir: %s" % submitdir)

    bd = os.path.join(submitdir, "braindump.txt")
    if not os.path.isfile(bd):
        raise SubmitDirException("Not a submit dir (no braindump.txt): %s" % submitdir)

    # Read the braindump file
    items = utils.read_braindump(bd)
    wf_uuid = items["wf_uuid"]
    root_wf_uuid = items["root_wf_uuid"]

    # Verify that we aren't trying to move a subworkflow
    if wf_uuid != root_wf_uuid:
        raise SubmitDirException("Subworkflows cannot be deleted independent of the root workflow")

    # Confirm that the username matches
    wf_user = items["user"]
    os_user = getpass.getuser()
    if wf_user != os_user:
        raise SubmitDirException("Workflow username from braindump does not match current username: %s != %s" % (wf_user, os_user))

    # Find the master db
    u = user.get_user_by_username(wf_user)
    mdb_file = u.get_master_db()
    if not os.path.isfile(mdb_file):
        raise SubmitDirException("Master database does not exist: %s" % mdb_file)

    # Confirm that they want to delete the workflow
    while True:
        sys.stdout.write("Are you sure you want to delete this workflow? This operation cannot be undone. [y/n]: ")
        answer = raw_input().strip().lower()
        if answer == "y":
            break
        if answer == "n":
            return

    # Connect to master database
    mdb_url = u.get_master_db_url()
    mdb = Database(mdb_url)

    # TODO We might want to delete all of the records from the workflow db
    # if they are not using an sqlite db that is in the submit dir

    # Delete the workflow
    mdb.delete_master_workflow(wf_uuid)

    # Remove all the files
    shutil.rmtree(submitdir)

    # Update master db
    mdb.commit()
    mdb.close()

class ExtractCommand(Command):
    description = "Extract (uncompress) submit directory"
    usage = "Usage: %prog extract SUBMITDIR"

    def run(self):
        if len(self.args) != 1:
            self.parser.error("Specify SUBMITDIR")

        extract(self.args[0])

class ArchiveCommand(Command):
    description = "Archive (compress) submit directory"
    usage = "Usage: %prog archive SUBMITDIR"

    def run(self):
        if len(self.args) != 1:
            self.parser.error("Specify SUBMITDIR")

        archive(self.args[0])

class MoveCommand(Command):
    description = "Move a submit directory"
    usage = "Usage: %prog move SUBMITDIR DEST"

    def run(self):
        if len(self.args) != 2:
            self.parser.error("Specify SUBMITDIR and DEST")

        move(self.args[0], self.args[1])

class DeleteCommand(Command):
    description = "Delete a submit directory and the associated DB entries"
    usage = "Usage: %prog delete SUBMITDIR"

    def run(self):
        if len(self.args) != 1:
            self.parser.error("Specify SUBMITDIR")

        delete(self.args[0])

class SubmitDirCommand(CompoundCommand):
    description = "Manages submit directories"
    commands = [
        ("archive", ArchiveCommand),
        ("extract", ExtractCommand),
        ("move", MoveCommand),
        ("delete", DeleteCommand)
    ]
    aliases = {
        "ar": "archive",
        "ex": "extract",
        "mv": "move",
        "rm": "delete"
    }

def main():
    "The entry point for pegasus-submitdir"
    logging.basicConfig(level=logging.DEBUG)
    SubmitDirCommand().main()

