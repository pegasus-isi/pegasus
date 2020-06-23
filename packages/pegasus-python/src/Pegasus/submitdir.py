"""."""

import glob
import logging
import os
import shutil
import tarfile
from os.path import expanduser

from Pegasus.command import CompoundCommand, LoggingCommand
from Pegasus.db import connection
from Pegasus.db.schema import (
    EnsembleWorkflow,
    MasterWorkflow,
    MasterWorkflowstate,
    Workflow,
    Workflowstate,
)
from Pegasus.tools import utils

log = logging.getLogger(__name__)


class SubmitDirException(Exception):
    pass


class MasterDatabase:
    def __init__(self, session):
        self.session = session

    def get_master_workflow(self, wf_uuid, submit_dir=None):
        q = self.session.query(MasterWorkflow)
        q = q.filter(MasterWorkflow.wf_uuid == wf_uuid)

        if submit_dir:
            q = q.filter(MasterWorkflow.submit_dir == submit_dir)

        wf = q.first()
        return wf

    def get_master_workflow_for_submitdir(self, submitdir):
        q = self.session.query(MasterWorkflow)
        q = q.filter(MasterWorkflow.submit_dir == submitdir)
        return q.all()

    def get_ensemble_workflow(self, wf_uuid):
        q = self.session.query(EnsembleWorkflow)
        q = q.filter(EnsembleWorkflow.wf_uuid == wf_uuid)
        return q.first()

    def delete_master_workflow(self, wf_uuid, submit_dir=None):
        w = self.get_master_workflow(wf_uuid, submit_dir=submit_dir)
        if w is None:
            return

        # Delete any ensemble workflows
        q = self.session.query(EnsembleWorkflow)
        q = q.filter(EnsembleWorkflow.wf_uuid == wf_uuid)
        q.delete()

        # Delete the workflow
        q = self.session.query(MasterWorkflow)
        q = q.filter(MasterWorkflow.wf_id == w.wf_id)
        q.delete()


class WorkflowDatabase:
    def __init__(self, session):
        self.session = session

    def delete_workflow(self, wf_uuid):
        q = self.session.query(Workflow)
        q = q.filter(Workflow.wf_uuid == wf_uuid)
        w = q.first()

        # If not found, do nothing
        if w is None:
            log.warning("Workflow not found in workflow DB: %s" % wf_uuid)
            return

        # Delete it
        self.session.delete(w)

    def get_workflow(self, wf_uuid):
        q = self.session.query(Workflow)
        q = q.filter(Workflow.wf_uuid == wf_uuid)
        return q.first()

    def get_workflow_states(self, wf_id):
        q = self.session.query(Workflowstate)
        q = q.filter(Workflowstate.wf_id == wf_id)
        return q.all()

    def update_submit_dirs(self, root_wf_id, src, dest):
        q = self.session.query(Workflow)
        q = q.filter(Workflow.root_wf_id == root_wf_id)
        for wf in q.all():
            log.info("Old submit dir: %s" % wf.submit_dir)
            wf.submit_dir = wf.submit_dir.replace(src, dest)
            log.info("New submit dir: %s" % wf.submit_dir)


class SubmitDir:
    def __init__(self, submitdir, raise_err=True):
        self.submitdir = os.path.abspath(submitdir)
        self.submitdir_exists = True

        if not os.path.isdir(submitdir):
            self.submitdir_exists = False

            if raise_err is False:
                return

            raise SubmitDirException("Invalid submit dir: %s" % submitdir)

        self.braindump_file = os.path.join(self.submitdir, "braindump.yml")
        if not os.path.isfile(self.braindump_file):
            self.braindump_file = os.path.join(self.submitdir, "braindump.txt")

        # Read the braindump file
        self.braindump = utils.slurp_braindb(os.path.join(self.submitdir))

        # Read some attributes from braindump file
        self.wf_uuid = self.braindump["wf_uuid"]
        self.root_wf_uuid = self.braindump["root_wf_uuid"]
        self.user = self.braindump["user"]

        self.archname = os.path.join(self.submitdir, "archive.tar.gz")

    def is_subworkflow(self):
        "Check to see if this workflow is a subworkflow"
        return self.wf_uuid != self.root_wf_uuid

    def is_archived(self):
        "A submit dir is archived if the archive file exists"
        return os.path.isfile(self.archname)

    def extract(self):
        "Extract files from an archived submit dir"

        # Locate archive file
        if not self.is_archived():
            raise SubmitDirException("Submit dir not archived")

        # Update record in master db
        mdbsession = connection.connect_by_submitdir(
            self.submitdir, connection.DBType.MASTER
        )
        mdb = MasterDatabase(mdbsession)
        wf = mdb.get_master_workflow(self.wf_uuid)
        if wf is not None:
            wf.archived = False

        # Untar the files
        tar = tarfile.open(self.archname, "r:gz")
        tar.extractall(path=self.submitdir)
        tar.close()

        # Remove the tar file
        os.remove(self.archname)

        # Commit the workflow changes
        mdbsession.commit()
        mdbsession.close()

    def archive(self):
        "Archive a submit dir by adding files to a compressed archive"

        # Update record in master db
        mdbsession = connection.connect_by_submitdir(
            self.submitdir, connection.DBType.MASTER
        )
        mdb = MasterDatabase(mdbsession)
        wf = mdb.get_master_workflow(self.wf_uuid)
        if wf is not None:
            wf.archived = True

        # The set of files to exclude from the archive
        exclude = set()

        # Exclude braindump file
        exclude.add(self.braindump_file)

        # We use a temporary file so that we can determine if the archive step
        # completed successfully later
        tmparchname = os.path.join(self.submitdir, "archive.tmp.tar.gz")

        # We use a lock file to determine if cleanup is complete
        lockfile = os.path.join(self.submitdir, "archive.cleanup.lock")

        # If a previous archive was partially completed, then remove the
        # temporary file that was created
        if os.path.exists(tmparchname):
            os.unlink(tmparchname)

        # Exclude the temporary archive name so we don't add it to itself
        exclude.add(tmparchname)

        # We don't want the lock file to be saved, if it exists
        exclude.add(lockfile)

        # Also exclude the final archive name in case they try to run it again
        exclude.add(self.archname)

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

        if self.is_archived() and not os.path.exists(lockfile):
            raise SubmitDirException("Submit directory already archived")

        if not self.is_archived():
            # Archive the files
            print("Creating archive...")
            tar = tarfile.open(name=tmparchname, mode="w:gz")
            for name, path in visit(self.submitdir):
                tar.add(name=path, arcname=name)
            tar.close()

            # This "commits" the archive step
            os.rename(tmparchname, self.archname)

        # Touch lockfile
        open(lockfile, "w").close()

        # Remove the files and directories
        # We do this here, instead of doing it in the loop above
        # because we want to make sure there are no errors in creating
        # the archive before we start removing files
        print("Removing files...")
        for name, path in visit(self.submitdir):
            if os.path.isfile(path) or os.path.islink(path):
                os.remove(path)
            else:
                shutil.rmtree(path)

        # This "commits" the file removal
        os.unlink(lockfile)

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
            raise SubmitDirException(
                "Subworkflows cannot be moved independent of the root workflow"
            )

        # Connect to master database
        mdbsession = connection.connect_by_submitdir(
            self.submitdir, connection.DBType.MASTER
        )
        mdb = MasterDatabase(mdbsession)

        # Get the workflow record from the master db
        db_url = None
        wf = mdb.get_master_workflow(self.wf_uuid)
        if wf is None:
            db_url = connection.url_by_submitdir(
                self.submitdir, connection.DBType.WORKFLOW
            )
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

        # Note that we do not need to update the properties file even though it
        # might contain DB URLs because it cannot contain a DB URL with the submit
        # dir in it.

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
            raise SubmitDirException(
                "Subworkflows cannot be deleted independent of the root workflow"
            )

        # Confirm that they want to delete the workflow
        while True:
            try:
                input = raw_input
            except NameError:
                pass
            answer = (
                input(
                    "Are you sure you want to delete this workflow? This operation cannot be undone. [y/n]: "
                )
                .strip()
                .lower()
            )
            if answer == "y":
                break
            if answer == "n":
                return

        # Connect to master database
        mdbsession = connection.connect_by_submitdir(
            self.submitdir, connection.DBType.MASTER
        )
        mdb = MasterDatabase(mdbsession)

        # Delete all of the records from the workflow db if they are not using
        # an sqlite db that is in the submit dir.
        db_url = connection.url_by_submitdir(self.submitdir, connection.DBType.WORKFLOW)
        if self.submitdir not in db_url:
            dbsession = connection.connect(db_url)
            db = WorkflowDatabase(dbsession)
            db.delete_workflow(self.wf_uuid)
            dbsession.commit()
            dbsession.close()

        # Delete the workflow
        mdb.delete_master_workflow(self.wf_uuid)

        # Remove all the files
        shutil.rmtree(self.submitdir)

        # Update master db
        mdbsession.commit()
        mdbsession.close()

    def attach(self):
        "Add a workflow to the master db"

        # Verify that we aren't trying to attach a subworkflow
        if self.is_subworkflow():
            raise SubmitDirException(
                "Subworkflows cannot be attached independent of the root workflow"
            )

        # Connect to master database
        mdbsession = connection.connect_by_submitdir(
            self.submitdir, connection.DBType.MASTER
        )
        mdb = MasterDatabase(mdbsession)

        # Check to see if it already exists and just update it
        wf = mdb.get_master_workflow(self.wf_uuid)
        if wf is not None:
            print("Workflow is already in master db")
            old_submit_dir = wf.submit_dir
            if old_submit_dir != self.submitdir:
                print("Updating path...")
                wf.submit_dir = self.submitdir
                wf.db_url = connection.url_by_submitdir(
                    self.submitdir, connection.DBType.WORKFLOW
                )
                mdbsession.commit()
            mdbsession.close()
            return

        # Connect to workflow db
        db_url = connection.url_by_submitdir(self.submitdir, connection.DBType.WORKFLOW)
        dbsession = connection.connect(db_url)
        db = WorkflowDatabase(dbsession)

        # Get workflow record
        wf = db.get_workflow(self.wf_uuid)
        if wf is None:
            print("No database record for that workflow exists")
            return

        # Update the workflow record
        wf.submit_dir = self.submitdir
        wf.db_url = db_url

        # Insert workflow record into master db
        mwf = MasterWorkflow()
        mwf.wf_uuid = wf.wf_uuid
        mwf.dax_label = wf.dax_label
        mwf.dax_version = wf.dax_version
        mwf.dax_file = wf.dax_file
        mwf.dag_file_name = wf.dag_file_name
        mwf.timestamp = wf.timestamp
        mwf.submit_hostname = wf.submit_hostname
        mwf.submit_dir = self.submitdir
        mwf.planner_arguments = wf.planner_arguments
        mwf.user = wf.user
        mwf.grid_dn = wf.grid_dn
        mwf.planner_version = wf.planner_version
        mwf.db_url = wf.db_url
        mwf.archived = self.is_archived()
        mdbsession.add(mwf)
        mdbsession.flush()  # We should have the new wf_id after this

        # Query states from workflow database
        states = db.get_workflow_states(wf.wf_id)

        # Insert states into master db
        for s in states:
            ms = MasterWorkflowstate()
            ms.wf_id = mwf.wf_id
            ms.state = s.state
            ms.timestamp = s.timestamp
            ms.restart_count = s.restart_count
            ms.status = s.status
            mdbsession.add(ms)
        mdbsession.flush()

        dbsession.commit()
        dbsession.close()

        mdbsession.commit()
        mdbsession.close()

    def detach(self, wf_uuid=None):
        "Remove any master db entries for the given root workflow"
        if self.submitdir_exists:
            # Verify that we aren't trying to detach a subworkflow
            if self.is_subworkflow():
                raise SubmitDirException(
                    "Subworkflows cannot be detached independent of the root workflow"
                )

            # Connect to master database
            mdbsession = connection.connect_by_submitdir(
                self.submitdir, connection.DBType.MASTER
            )
            mdb = MasterDatabase(mdbsession)

            # Check to see if it even exists
            wf = mdb.get_master_workflow(self.wf_uuid)
            if wf is None:
                print("Workflow is not in master DB")
            else:
                # Delete the workflow (this will delete the master_workflowstate entries as well)
                mdb.delete_master_workflow(self.wf_uuid)

            # Update the master db
            mdbsession.commit()
            mdbsession.close()

        else:
            # Connect to master database
            home = expanduser("~")
            mdbsession = connection.connect(
                "sqlite:///%s/.pegasus/workflow.db" % home,
                db_type=connection.DBType.MASTER,
            )
            mdb = MasterDatabase(mdbsession)

            try:
                if wf_uuid is None:
                    wfs = mdb.get_master_workflow_for_submitdir(self.submitdir)
                    if wfs:
                        msg = (
                            "Invalid submit dir: %s, Specify --wf-uuid <WF_UUID> to detach\n"
                            % self.submitdir
                        )
                        msg += (
                            "\tWorkflow UUID, DAX Label, Submit Hostname, Submit Dir.\n"
                        )
                        for wf in wfs:
                            msg += "\t{}, {}, {}, {}\n".format(
                                wf.wf_uuid,
                                wf.dax_label,
                                wf.submit_hostname,
                                wf.submit_dir,
                            )
                        raise SubmitDirException(msg)

                    else:
                        raise SubmitDirException(
                            "Invalid submit dir: %s" % self.submitdir
                        )

                else:
                    # Delete
                    mdb.delete_master_workflow(wf_uuid, submit_dir=self.submitdir)

                    # Update the master db
                    mdbsession.commit()

            finally:
                mdbsession.close()


class ExtractCommand(LoggingCommand):
    description = "Extract (uncompress) submit directory"
    usage = "Usage: %prog extract SUBMITDIR"

    def run(self):
        if len(self.args) != 1:
            self.parser.error("Specify SUBMITDIR")

        SubmitDir(self.args[0]).extract()


class ArchiveCommand(LoggingCommand):
    description = "Archive (compress) submit directory"
    usage = "Usage: %prog archive SUBMITDIR"

    def run(self):
        if len(self.args) != 1:
            self.parser.error("Specify SUBMITDIR")

        SubmitDir(self.args[0]).archive()


class MoveCommand(LoggingCommand):
    description = "Move a submit directory"
    usage = "Usage: %prog move SUBMITDIR DEST"

    def run(self):
        if len(self.args) != 2:
            self.parser.error("Specify SUBMITDIR and DEST")

        SubmitDir(self.args[0]).move(self.args[1])


class DeleteCommand(LoggingCommand):
    description = "Delete a submit directory and the associated DB entries"
    usage = "Usage: %prog delete SUBMITDIR"

    def run(self):
        if len(self.args) != 1:
            self.parser.error("Specify SUBMITDIR")

        SubmitDir(self.args[0]).delete()


class AttachCommand(LoggingCommand):
    description = "Attach a submit dir to the master db (dashboard)"
    usage = "Usage: %prog attach SUBMITDIR"

    def run(self):
        if len(self.args) != 1:
            self.parser.error("Specify SUBMITDIR")

        SubmitDir(self.args[0]).attach()


class DetachCommand(LoggingCommand):
    description = "Detach a submit dir from the master db (dashboard)"
    usage = "Usage: %prog detach SUBMITDIR"

    def __init__(self):
        LoggingCommand.__init__(self)
        self.parser.add_option(
            "-i",
            "--wf-uuid",
            dest="wf_uuid",
            help="Specify wf_uuid of the workflow to be detached.",
        )

    def run(self):
        if len(self.args) != 1:
            self.parser.error("Specify SUBMITDIR")

        wf_uuid = self.options.wf_uuid
        SubmitDir(self.args[0], raise_err=False).detach(wf_uuid=wf_uuid)


class SubmitDirCommand(CompoundCommand):
    description = "Manages submit directories"
    commands = [
        ("archive", ArchiveCommand),
        ("extract", ExtractCommand),
        ("move", MoveCommand),
        ("delete", DeleteCommand),
        ("attach", AttachCommand),
        ("detach", DetachCommand),
    ]
    aliases = {
        "ar": "archive",
        "ex": "extract",
        "mv": "move",
        "rm": "delete",
        "at": "attach",
        "dt": "detach",
    }


def main():
    "The entry point for pegasus-submitdir"
    SubmitDirCommand().main()
