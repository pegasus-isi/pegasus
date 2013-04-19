import os
import sys
from setuptools import setup, find_packages

# Utility function to read the README file.
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name = "pegasus-wms",
    version = "4.3",
    author = "Pegasus Team",
    author_email = "pegasus-support@isi.edu",
    description = "Pegasus",
    long_description = "",
    license = "Apache2",
    url = "https://github.com/pegasus-isi/pegasus",
    classifiers = [
        "Development Status :: 3 - Alpha",
        "Topic :: Utilities",
        "License :: OSI Approved :: Apache Software License",
    ],
    packages = find_packages(),
    #package_data = {"" : ["templates/*", "static/*"] },
    #include_package_data = True,
    zip_safe = False,
    install_requires = [
        "SQLAlchemy",
        "boto",
        "pysqlite"
    ]
)

