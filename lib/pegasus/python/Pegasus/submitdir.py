import os
import glob
import tarfile
import shutil
import logging
from optparse import OptionParser


from Pegasus.db import connection
from Pegasus.tools import utils
from Pegasus.command import Command, CompoundCommand
from Pegasus.db.schema import *

log = logging.getLogger(__name__)

class SubmitDirException(Exception): pass

class MasterDatabase:
    def __init__(self, session):
        self.session = session

    def get_master_workflow(self, wf_uuid):
        q = self.session.query(DashboardWorkflow)
        q = q.filter(DashboardWorkflow.wf_uuid == wf_uuid)
        wf = q.first()
        if wf is None:
            log.warning("No master db record found for workflow: %s" % wf_uuid)
        return wf

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

class WorkflowDatabase(object):
    def __init__(self, session):
        self.session = session

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

class SubmitDir(object):
    def __init__(self, submitdir):
        if not os.path.isdir(submitdir):
            raise SubmitDirException("Invalid submit dir: %s" % submitdir)
        self.submitdir = os.path.abspath(submitdir)

        # Locate braindump file
        self.braindump_file = os.path.join(self.submitdir, "braindump.txt")
        if not os.path.isfile(self.braindump_file):
            raise SubmitDirException("Not a submit directory: braindump.txt missing")

        # Read the braindump file
        self.braindump = utils.read_braindump(self.braindump_file)

        # Read some attributes from braindump file
        self.wf_uuid = self.braindump["wf_uuid"]
        self.root_wf_uuid = self.braindump["root_wf_uuid"]
        self.user = self.braindump["user"]

    def is_subworkflow(self):
        "Check to see if this workflow is a subworkflow"
        return self.wf_uuid != self.root_wf_uuid

    def extract(self):
        "Extract files from an archived submit dir"

        # Locate archive file
        archname = os.path.join(self.submitdir, "archive.tar.gz")
        if not os.path.isfile(archname):
            raise SubmitDirException("Submit dir not archived")

        # Update record in master db
        mdbsession = connection.connect_to_master_db(self.user)
        mdb = MasterDatabase(mdbsession)
        wf = mdb.get_master_workflow(self.wf_uuid)
        if wf is not None:
            wf.archived = False

        # Untar the files
        tar = tarfile.open(archname, "r:gz")
        tar.extractall(path=self.submitdir)
        tar.close()

        # Remove the tar file
        os.remove(archname)

        # Commit the workflow changes
        mdbsession.commit()
        mdbsession.close()

    def archive(self):
        "Archive a submit dir by adding files to a compressed archive"

        # Update record in master db
        mdbsession = connection.connect_to_master_db(self.user)
        mdb = MasterDatabase(mdbsession)
        wf = mdb.get_master_workflow(self.wf_uuid)
        if wf is not None:
            wf.archived = True

        # The set of files to exclude from the archive
        exclude = set()

        # Exclude braindump file
        exclude.add(self.braindump_file)

        # Locate and exclude archive file
        archname = os.path.join(self.submitdir, "archive.tar.gz")
        if os.path.exists(archname):
            raise SubmitDirException("Submit dir already archived")
        exclude.add(archname)

        # Ignore monitord files. This is needed so that tools like pegasus-statistics
        # will consider the workflow to be complete
        for name in ["monitord.started", "monitord.done", "monitord.log"]:
            exclude.add(os.path.join(self.submitdir, name))

        # Exclude stampede db
        for db in glob.glob(os.path.join(self.submitdir, "*.stampede.db")):
            exclude.add(db)

        # Exclude properties file
        for prop in glob.glob(os.path.join(self.submitdir, "pegasus.*.properties")):
            exclude.add(prop)

        # Visit all the files in the submit dir that we want to archive
        def visit(dirpath):
            for name in os.listdir(dirpath):
                filepath = os.path.join(dirpath, name)

                if filepath not in exclude:
                    yield name, filepath

        # Archive the files
        tar = tarfile.open(name=archname, mode="w:gz")
        for name, path in visit(self.submitdir):
            tar.add(name=path, arcname=name)
        tar.close()

        # Remove the files and directories
        # We do this here, instead of doing it in the loop above
        # because we want to make sure there are no errors in creating
        # the archive before we start removing files
        for name, path in visit(self.submitdir):
            if os.path.isfile(path) or os.path.islink(path):
                os.remove(path)
            else:
                shutil.rmtree(path)

        # Commit the workflow changes
        mdbsession.commit()
        mdbsession.close()

    def move(self, dest):
        "Move this submit directory to dest"

        dest = os.path.abspath(dest)

        if os.path.isfile(dest):
            raise SubmitDirException("Destination is a file: %s" % dest)

        if os.path.isdir(dest):
            if os.path.exists(os.path.join(dest, "braindump.txt")):
                raise SubmitDirException("Destination is a submit dir: %s" % dest)
            dest = os.path.join(dest, os.path.basename(self.submitdir))

        # Verify that we aren't trying to move a subworkflow
        if self.is_subworkflow():
            raise SubmitDirException("Subworkflows cannot be moved independent of the root workflow")

        # Connect to master database
        mdbsession = connection.connect_to_master_db(self.user)
        mdb = MasterDatabase(mdbsession)

        # Get the workflow record from the master db
        db_url = None
        wf = mdb.get_master_workflow(self.wf_uuid)
        if wf is None:
            # No master db record

            # Try looking in the workflow directory for workflow db
            pegasus_wf_name = self.braindump["pegasus_wf_name"]
            db_file = os.path.join(self.submitdir, pegasus_wf_name + ".stampede.db")
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
            wf.db_url = db_url.replace(self.submitdir, dest)
            log.info("New master db_url: %s" % wf.db_url)

            # Change the master db's submit_dir
            log.info("Old master submit_dir: %s" % wf.submit_dir)
            wf.submit_dir = dest
            log.info("New master submit_dir: %s" % wf.submit_dir)

        # Update the ensemble record if one exists
        ew = mdb.get_ensemble_workflow(self.wf_uuid)
        if ew is not None:
            log.info("Old ensemble submit dir: %s", ew.submitdir)
            ew.submitdir = dest
            log.info("New ensemble submit dir: %s", ew.submitdir)

        # Update the workflow database if we found one
        if db_url is not None:
            dbsession = connection.connect(db_url)
            db = WorkflowDatabase(dbsession)
            root_wf = db.get_workflow(self.wf_uuid)
            db.update_submit_dirs(root_wf.wf_id, self.submitdir, dest)
            dbsession.commit()
            dbsession.close()

        # Move all the files
        shutil.move(self.submitdir, dest)

        # Set new paths in the braindump file
        self.braindump["submit_dir"] = dest
        self.braindump["basedir"] = os.path.dirname(dest)
        utils.write_braindump(os.path.join(dest, "braindump.txt"), self.braindump)

        # TODO We might want to update all of the absolute paths in the condor submit files
        # if we plan on moving workflows that could be resubmitted in the future

        # TODO We might want to update the braindump files for subworkflows

        # Update master database
        mdbsession.commit()
        mdbsession.close()

        # Finally, update object
        self.submitdir = dest

    def delete(self):
        "Delete this submit dir and its entry in the master db"

        # Verify that we aren't trying to move a subworkflow
        if self.is_subworkflow():
            raise SubmitDirException("Subworkflows cannot be deleted independent of the root workflow")

        # Confirm that they want to delete the workflow
        while True:
            sys.stdout.write("Are you sure you want to delete this workflow? This operation cannot be undone. [y/n]: ")
            answer = raw_input().strip().lower()
            if answer == "y":
                break
            if answer == "n":
                return

        # Connect to master database
        mdbsession = connection.connect_to_master_db(self.user)
        mdb = MasterDatabase(mdbsession)

        # TODO We might want to delete all of the records from the workflow db
        # if they are not using an sqlite db that is in the submit dir

        # Delete the workflow
        mdb.delete_master_workflow(self.wf_uuid)

        # Remove all the files
        shutil.rmtree(self.submitdir)

        # Update master db
        mdbsession.commit()
        mdbsession.close()

class ExtractCommand(Command):
    description = "Extract (uncompress) submit directory"
    usage = "Usage: %prog extract SUBMITDIR"

    def run(self):
        if len(self.args) != 1:
            self.parser.error("Specify SUBMITDIR")

        SubmitDir(self.args[0]).extract()

class ArchiveCommand(Command):
    description = "Archive (compress) submit directory"
    usage = "Usage: %prog archive SUBMITDIR"

    def run(self):
        if len(self.args) != 1:
            self.parser.error("Specify SUBMITDIR")

        SubmitDir(self.args[0]).archive()

class MoveCommand(Command):
    description = "Move a submit directory"
    usage = "Usage: %prog move SUBMITDIR DEST"

    def run(self):
        if len(self.args) != 2:
            self.parser.error("Specify SUBMITDIR and DEST")

        SubmitDir(self.args[0]).move(self.args[1])

class DeleteCommand(Command):
    description = "Delete a submit directory and the associated DB entries"
    usage = "Usage: %prog delete SUBMITDIR"

    def run(self):
        if len(self.args) != 1:
            self.parser.error("Specify SUBMITDIR")

        SubmitDir(self.args[0]).delete()

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
    logging.basicConfig(level=logging.INFO)
    SubmitDirCommand().main()

