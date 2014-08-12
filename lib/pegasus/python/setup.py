import os
import sys
import subprocess
from setuptools import setup, find_packages

srcdir = os.path.dirname(__file__)
homedir = os.path.abspath(os.path.join(srcdir, "../../.."))

# Utility function to read the pegasus Version.in file
def readversion():
    return subprocess.Popen("%s/release-tools/getversion" % homedir,
                stdout=subprocess.PIPE, shell=True).communicate()[0].strip()

# Utility function to read the README file.
def read(fname):
    return open(os.path.join(srcdir, fname)).read()

def find_package_data(dirname):
    def find_paths(dirname):
        items = []
        for fname in os.listdir(dirname):
            path = os.path.join(dirname, fname)
            if os.path.isdir(path):
                items += find_paths(path)
            elif not path.endswith(".py") and not path.endswith(".pyc"):
                items.append(path)
        return items
    items = find_paths(dirname)
    return [os.path.relpath(path, dirname) for path in items]

setup(
    name = "pegasus-wms",
    version = readversion(),
    author = "Pegasus Team",
    author_email = "pegasus@isi.edu",
    description = "Pegasus Workflow Management System Python API",
    long_description = read("README"),
    license = "Apache2",
    url = "http://pegasus.isi.edu",
    keywords = ["scientific workflows"],
    classifiers = [
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Operating System :: Unix",
        "Programming Language :: Python",
        "Topic :: Scientific/Engineering",
        "Topic :: Utilities",
        "License :: OSI Approved :: Apache Software License",
    ],
    packages = find_packages(exclude=["Pegasus.test","Pegasus.service.test"]),
    package_data = {"Pegasus.service" : find_package_data("Pegasus/service") },
    include_package_data = True,
    zip_safe = False,
    test_suite = "Pegasus.test"
)

