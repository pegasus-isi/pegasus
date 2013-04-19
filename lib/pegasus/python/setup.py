import os
import sys
from setuptools import setup, find_packages

# Utility function to read the README file.
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name = "pegasus-wms",
    version = "4.3.0",
    author = "Gideon Juve",
    author_email = "gideon@isi.edu",
    description = "Pegasus Workflow Management System Python API",
    long_description = read("README"),
    license = "Apache2",
    url = "http://pegasus.isi.edu",
    classifiers = [
        "Development Status :: 3 - Alpha",
        "Topic :: Utilities",
        "License :: OSI Approved :: Apache Software License",
    ],
    packages = ["Pegasus"],
    install_requires = [
        "SQLAlchemy"
    ]
)

