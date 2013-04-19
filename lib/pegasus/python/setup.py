import os
import sys
from setuptools import setup, find_packages

srcdir = os.path.dirname(__file__)

# Utility function to read the pegasus Version.in file
def readversion():
    version = os.path.join(srcdir, "../../../src/edu/isi/pegasus/common/util/Version.in")

    def getnum(l):
        s = l.index("=") + 1
        e = l.index(";")
        return l[s:e].strip()
 
    f = open(version, "r")
    for l in f:
        if "int MAJOR" in l:
            MAJOR = getnum(l)
        elif "int MINOR" in l:
            MINOR = getnum(l)
        elif "int PLEVEL" in l:
            PLEVEL = getnum(l)
    f.close()

    return "%s.%s.%s" % (MAJOR, MINOR, PLEVEL)

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

