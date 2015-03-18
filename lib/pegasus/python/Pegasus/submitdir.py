#!/usr/bin/env python

import os
import glob
import tarfile
import shutil
from optparse import OptionParser

from Pegasus.service.command import Command, CompoundCommand

class SubmitDirException(Exception): pass

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

class SubmitDirCommand(CompoundCommand):
    description = "Manages submit directories"
    commands = [
        ("archive", ArchiveCommand),
        ("extract", ExtractCommand)
    ]
    aliases = {
        "ar": "archive",
        "ex": "extract"
    }

def main():
    "The entry point for pegasus-submitdir"
    SubmitDirCommand().main()

