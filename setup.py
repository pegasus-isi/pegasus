import os
import sys
from setuptools import setup, find_packages

# Utility function to read the README file.
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name = "pegasus-service",
    version = "0.1",
    author = "Pegasus Team",
    author_email = "pegasus-support@isi.edu",
    description = "Pegasus as a Service",
    long_description = read("README.md"),
    license = "Apache2",
    url = "https://github.com/pegasus-isi/pegasus-service",
    classifiers = [
        "Development Status :: 3 - Alpha",
        "Topic :: Utilities",
        "License :: OSI Approved :: Apache Software License",
    ],
    packages = find_packages(),
    package_data = {"" : ["templates/*", "static/*"] },
    include_package_data = True,
    zip_safe = False,
    entry_points = {
        'console_scripts': [
            'pegasus-service-server = pegasus.service.server:main',
            'pegasus-service-admin = pegasus.service.admin:main',
        ]
    },
    install_requires = [
        "Flask",
        "Flask-SQLAlchemy",
        "MySQL-python",
        "WTForms",
        "requests"
    ]
)

