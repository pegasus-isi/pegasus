import os
import sys
import subprocess
from setuptools import setup, find_packages

srcdir = os.path.dirname(__file__)
homedir = os.path.abspath(os.path.join(srcdir, "../../.."))

# Utility function to read the pegasus Version.in file
def readversion():
    return subprocess.check_output("%s/release-tools/getversion" % homedir).strip()

# Utility function to read the README file.
def read(fname):
    return open(os.path.join(srcdir, fname)).read()

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
    packages = find_packages(exclude=["Pegasus.test"]),
    install_requires = [
        "SQLAlchemy"
    ]
)

